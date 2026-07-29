"""Gemini spectra from the CADC archive.

Retrieval chain (the part that is easy to get wrong):

    ivoa.ObsCore.access_url  ->  XML VOTable (a DataLink *listing*, not data)
        -> row with semantics '#this'  ->  the actual FITS (maybe .bz2 / tar)

Gemini products are served through several reduction pipelines and instrument
formats, so a single decoder cannot assume one layout. ``read_spectrum`` tries
every plausible encoding (binary table, WCS image axis, GHOST-style AWAV-TAB
pairing) and keeps whichever yields the best S/N.

Gemini spectra are in detector electrons rather than a calibrated flux density,
so they are stored with the astropy count unit (``u.ct``), the same convention
used by the 6dFGS service for its raw-count spectra.
"""

import bz2
import gzip
import io
import logging
import re
import tarfile
from datetime import date, timezone

import numpy as np
import requests

import astropy.units as u
from astropy.io import fits
from astropy.time import Time
from specutils import Spectrum1D

import pyvo
from pyvo.dal.adhoc import DatalinkResults

from django.conf import settings

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from tom_dataproducts.processors.data_serializers import SpectrumSerializer
from custom_code.data_services.forms import GeminiSpectraQueryForm
from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT


logger = logging.getLogger(__name__)

GEMINI_ARCHIVE_URL = 'https://archive.gemini.edu/'
CADC_TAP_URL = 'https://ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/argus'

# CADC keeps a small second Gemini set under its own collection name.
GEMINI_COLLECTIONS = ('GEMINI', 'GEMINICADC')

# Cap the number of ObsCore rows, and the number we will actually download.
# Every kept row costs one DataLink round trip plus one FITS download.
MAX_OBSCORE_ROWS = 200
MAX_SPECTRA = 40

# Spectrum download can be much slower than a catalogue call.
GEMINI_FITS_TIMEOUT = (
    DATA_SERVICE_HTTP_TIMEOUT[0],
    max(DATA_SERVICE_HTTP_TIMEOUT[1], 300),
)


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _gemini_cert():
    """Client certificate for proprietary data, if the deployment has one."""
    return getattr(settings, 'GEMINI_CADC_CERT', None) or None


# --------------------------------------------------------------------------
# generic spectrum decoding
# --------------------------------------------------------------------------
# Collections share no format, so try every encoding and keep the best S/N.
WAVE_KEYS = ('WAVELENGTH', 'WAVE', 'LAMBDA', 'AWAV', 'SPEC_WAVE')
LOGWAVE_KEYS = ('LOGLAM', 'LOG_LAMBDA', 'LOGWAVE')
FLUX_KEYS = ('FLUX', 'SPECTRUM', 'COUNTS', 'DATA', 'FLAM', 'F_LAMBDA')
ERR_KEYS = ('ERROR', 'ERR', 'SIGMA', 'FLUX_ERROR', 'IVAR', 'VARIANCE', 'VAR', 'FLUXERR')

_SKIP_EXTNAMES = ('DQ', 'VAR', 'ERR', 'AWAV', 'MASK')


def _decompress(blob):
    if blob[:3] == b'BZh':
        return bz2.decompress(blob)
    if blob[:2] == b'\x1f\x8b':
        return gzip.decompress(blob)
    return blob


def _untar(blob):
    """Return the first FITS member of a tar blob, or None if it is not a tar."""
    if blob[257:262] != b'ustar':
        return None
    with tarfile.open(fileobj=io.BytesIO(blob)) as archive:
        for member in archive.getmembers():
            if not member.isfile():
                continue
            extracted = _decompress(archive.extractfile(member).read())
            if extracted[:6] == b'SIMPLE':
                return extracted
    return None


def _to_nm(lam, unit):
    """Convert a wavelength array to nm, guessing from magnitude if unit is absent."""
    unit = (unit or '').lower()
    if unit.startswith('ang') or unit == 'a':
        return lam / 10.0
    if unit.startswith('nm'):
        return lam
    if unit in ('um', 'micron', 'microns'):
        return lam * 1000.0
    if unit == 'm':
        return lam * 1e9
    median = np.nanmedian(lam)
    if median > 1e4:
        return lam / 10.0
    if median < 10:
        return lam * 1000.0
    return lam


def _snr(flux, err):
    """Median S/N, from the error array when usable and from pixel scatter otherwise."""
    finite = np.isfinite(flux)
    if err is not None:
        usable = finite & np.isfinite(err) & (err > 0)
        if usable.sum() > 10:
            return float(np.nanmedian(np.abs(flux[usable] / err[usable])))
    if finite.sum() < 10:
        return 0.0
    diff = np.diff(flux[finite])
    noise = 1.4826 * np.nanmedian(np.abs(diff - np.nanmedian(diff))) / np.sqrt(2)
    return float(np.nanmedian(np.abs(flux[finite])) / noise) if noise > 0 else 0.0


def _wcs_axis(header, npix):
    """Build a wavelength axis from CRVAL1/CD1_1, honouring the log-wavelength CTYPEs."""
    crval = header.get('CRVAL1')
    cdelt = header.get('CD1_1', header.get('CDELT1'))
    if crval is None or cdelt in (None, 0):
        return None, None
    if crval == 0 and cdelt == 1:
        return None, None  # placeholder WCS, not a real wavelength solution
    pixels = np.arange(npix) + 1.0
    crpix = header.get('CRPIX1', 1.0)
    ctype = (header.get('CTYPE1') or '').upper()
    if 'LOG' in ctype and 'AWAV' in ctype:
        lam = crval * np.exp(cdelt * (pixels - crpix) / crval)  # FITS WCS Paper III -LOG
    elif ctype.startswith('LOG'):
        lam = 10.0 ** (crval + (pixels - crpix) * cdelt)
    else:
        lam = crval + (pixels - crpix) * cdelt
    return lam, (header.get('CUNIT1') or '').strip() or None


def _keep_best(best, lam, flux, err, note):
    """Validate one candidate spectrum and keep it only if it beats the incumbent S/N."""
    if lam is None or flux is None:
        return best
    lam = np.asarray(lam, float).ravel()
    flux = np.asarray(flux, float).ravel()
    if lam.size != flux.size or lam.size < 50:
        return best
    err = np.asarray(err, float).ravel() if err is not None else None
    if err is not None and err.size != flux.size:
        err = None
    finite = np.isfinite(lam) & np.isfinite(flux)
    if finite.sum() < 50:
        return best
    lam, flux = lam[finite], flux[finite]
    err = err[finite] if err is not None else None
    order = np.argsort(lam)
    lam, flux = lam[order], flux[order]
    err = err[order] if err is not None else None
    snr = _snr(flux, err)
    if best is None or snr > best[4]:
        return (lam, flux, err, note, snr)
    return best


def _read_bintable_spectra(hdul, best):
    """(a) binary tables: WAVELENGTH/FLUX columns, or SDSS-style loglam."""
    for hdu in hdul:
        if not isinstance(hdu, fits.BinTableHDU) or hdu.data is None:
            continue
        try:
            names = {c.upper(): c for c in hdu.columns.names}
            units = {c.name.upper(): (c.unit or '') for c in hdu.columns}
            wave_key = next((names[k] for k in WAVE_KEYS if k in names), None)
            logwave_key = next((names[k] for k in LOGWAVE_KEYS if k in names), None)
            flux_key = next((names[k] for k in FLUX_KEYS if k in names), None)
            err_key = next((names[k] for k in ERR_KEYS if k in names), None)
            if flux_key is None or (wave_key is None and logwave_key is None):
                continue

            flux = np.asarray(hdu.data[flux_key], float)
            if wave_key:
                lam = _to_nm(np.asarray(hdu.data[wave_key], float), units.get(wave_key.upper()))
            else:
                lam = _to_nm(10.0 ** np.asarray(hdu.data[logwave_key], float), 'angstrom')

            err = np.asarray(hdu.data[err_key], float) if err_key else None
            if err is not None and err_key.upper() == 'IVAR':
                with np.errstate(all='ignore'):
                    err = 1.0 / np.sqrt(np.where(err > 0, err, np.nan))
            elif err is not None and err_key.upper() in ('VAR', 'VARIANCE'):
                err = np.sqrt(np.abs(err))

            if flux.ndim == 2:  # x1d layout: one spectrum per row
                for row in range(flux.shape[0]):
                    row_err = err[row] if (err is not None and err.ndim == 2) else None
                    best = _keep_best(best, lam[row], flux[row], row_err, f'bintable row {row}')
            else:
                best = _keep_best(best, lam, flux, err, 'bintable')
        except Exception:
            logger.debug('Gemini: unreadable bintable HDU; skipping.', exc_info=True)
    return best


def _read_image_spectra(hdul, best):
    """(b) image extensions, including the GHOST-style AWAV/EXTVER pairing."""
    try:
        awav = {
            hdu.header.get('EXTVER'): hdu.data
            for hdu in hdul
            if hdu.header.get('EXTNAME') == 'AWAV' and hdu.data is not None
        }
        variance = {
            hdu.header.get('EXTVER'): hdu.data
            for hdu in hdul
            if hdu.header.get('EXTNAME') in ('VAR', 'ERR') and hdu.data is not None
        }
        for hdu in hdul:
            data = getattr(hdu, 'data', None)
            if data is None or isinstance(hdu, fits.BinTableHDU) or data.ndim not in (1, 2):
                continue
            extname = hdu.header.get('EXTNAME')
            extver = hdu.header.get('EXTVER')
            if extname in _SKIP_EXTNAMES:
                continue

            data = np.asarray(data, float)
            err = np.sqrt(np.abs(np.asarray(variance[extver], float))) if extver in variance else None

            if extver in awav:  # AWAV-TAB: wavelength lives in its own extension
                lam = _to_nm(np.asarray(awav[extver], float), hdu.header.get('CUNIT1'))
                best = _keep_best(
                    best,
                    lam.ravel(),
                    data.ravel(),
                    err.ravel() if err is not None else None,
                    f'AWAV-TAB {extname},{extver}',
                )
                continue

            lam, unit = _wcs_axis(hdu.header, data.shape[-1])
            if lam is None:
                continue
            lam = _to_nm(lam, unit)
            if data.ndim == 1:
                best = _keep_best(best, lam, data, err, f'WCS 1D {extname},{extver}')
            else:
                best = _keep_best(
                    best, lam, np.nanmedian(data, axis=0), None, f'WCS 2D collapsed {extname},{extver}'
                )
    except Exception:
        logger.debug('Gemini: unreadable image HDU; skipping.', exc_info=True)
    return best


def read_spectrum(blob):
    """Decode a downloaded product into ``(lam_nm, flux, err, note, snr)``, or None."""
    blob = _decompress(blob)
    inner = _untar(blob)
    if inner:
        blob = inner
    if blob[:6] != b'SIMPLE':
        return None
    with fits.open(io.BytesIO(blob)) as hdul:
        best = _read_bintable_spectra(hdul, None)
        best = _read_image_spectra(hdul, best)
    return best


def describe_blob(blob):
    """Say what a downloaded blob actually is, so failures are never silent."""
    try:
        raw = blob
        blob = _decompress(blob)
        untarred = _untar(blob)
        prefix = 'tar>' if untarred else ('compressed>' if blob is not raw else '')
        blob = untarred or blob
        if blob[:6] != b'SIMPLE':
            head = blob[:60].decode('utf8', 'replace').strip().replace('\n', ' ')
            return f'{prefix}not FITS ({len(blob)}B): {head[:52]!r}'
        with fits.open(io.BytesIO(blob)) as hdul:
            parts = []
            for hdu in hdul:
                data = getattr(hdu, 'data', None)
                shape = '' if data is None else str(getattr(data, 'shape', ''))
                parts.append(f"{hdu.header.get('EXTNAME') or hdu.__class__.__name__}{shape}")
        return f'{prefix}FITS but undecodable: ' + ', '.join(parts[:7])
    except Exception as exc:
        return f'unreadable: {type(exc).__name__}'


# --------------------------------------------------------------------------
# observation time
# --------------------------------------------------------------------------
def mjd_from_row(row):
    """ObsCore ``t_min`` is frequently NULL/masked at CADC, so this may return None."""
    try:
        value = row['t_min']
    except Exception:
        return None, None
    try:
        if value is not None and not np.ma.is_masked(value) and np.isfinite(float(value)):
            return float(value), 'ObsCore t_min'
    except (TypeError, ValueError):
        pass
    return None, None


def mjd_from_fits(blob):
    """Fall back to the FITS header. GHOST has no MJD-OBS but carries DATE-OBS + UTSTART."""
    try:
        blob = _decompress(blob)
        blob = _untar(blob) or blob
        with fits.open(io.BytesIO(blob)) as hdul:
            headers = [hdu.header for hdu in hdul]

        for header in headers:
            for key in ('MJD-OBS', 'MJDOBS', 'MJD'):
                if key in header:
                    try:
                        return float(header[key]), f'header {key}'
                    except (TypeError, ValueError):
                        pass

        for header in headers:
            date_obs = header.get('DATE-OBS')
            if not date_obs:
                continue
            time_obs = header.get('UTSTART') or header.get('TIME-OBS') or header.get('UT')
            iso = f'{str(date_obs)[:10]}T{time_obs}' if time_obs else f'{str(date_obs)[:10]}T00:00:00'
            try:
                mjd = float(Time(iso, format='isot', scale='utc').mjd)
                return mjd, ('DATE-OBS+UTSTART' if time_obs else 'DATE-OBS only')
            except Exception:
                pass

        for header in headers:  # last resort: barycentric JD
            if 'BJD' in header:
                try:
                    return float(header['BJD']) - 2400000.5, 'BJD (barycentric)'
                except (TypeError, ValueError):
                    pass
    except Exception:
        logger.debug('Gemini: could not read an observation time from FITS headers.', exc_info=True)
    return None, 'unavailable'


# --------------------------------------------------------------------------
# DataLink
# --------------------------------------------------------------------------
SEMANTICS_PREFERENCE = ['#this', '#calibrated', '#derivation', '#progenitor', '#auxiliary']


def _field(record, key):
    try:
        value = record.get(key)
        return (value.decode() if isinstance(value, bytes) else str(value or '')).lower()
    except Exception:
        return ''


def _record_url(record):
    for getter in (lambda: record.access_url, lambda: record.get('access_url')):
        try:
            value = getter()
        except Exception:
            continue
        if value is None:
            continue
        value = value.decode() if isinstance(value, bytes) else str(value)
        if value.startswith('http'):
            return value
    return None


def datalink_urls(access_url):
    """Read the DataLink XML and yield real download URLs, best first.

    Deliberately does not use ``dl.bysemantics()``: that fetches the IVOA
    vocabulary over the network and 403s from many hosts.
    """
    try:
        datalink = DatalinkResults.from_result_url(access_url)
    except Exception:
        logger.debug('Gemini: DataLink lookup failed for %s; trying the raw URL.', access_url)
        yield access_url, 'raw'
        return

    rows = []
    for record in datalink:
        if _field(record, 'error_message'):
            continue
        url = _record_url(record)
        if not url:
            continue
        semantics = _field(record, 'semantics')
        content_type = _field(record, 'content_type')
        rank = SEMANTICS_PREFERENCE.index(semantics) if semantics in SEMANTICS_PREFERENCE else len(SEMANTICS_PREFERENCE)
        if 'fits' in content_type:
            rank -= 0.5
        if any(x in content_type for x in ('png', 'jpeg', 'html', 'text')):
            rank += 10
        rows.append((rank, url, semantics or '?'))

    for _, url, semantics in sorted(rows, key=lambda item: item[0]):
        yield url, semantics


# --------------------------------------------------------------------------
# ObsCore query
# --------------------------------------------------------------------------
def _build_cadc_query(ra, dec, radius_arcsec, max_rows=MAX_OBSCORE_ROWS):
    """Cone search over every Gemini instrument; the collection filter is the only cut."""
    radius_deg = radius_arcsec / 3600.0
    where = [
        "dataproduct_type = 'spectrum'",
        'calib_level >= 2',
        f"INTERSECTS(s_region, CIRCLE('ICRS', {ra}, {dec}, {radius_deg})) = 1",
        f"obs_release_date < '{date.today().isoformat()}'",
        'obs_collection IN (%s)' % ', '.join(f"'{c}'" for c in GEMINI_COLLECTIONS),
    ]
    return (
        f'SELECT TOP {max_rows} obs_collection, obs_id, target_name, facility_name, '
        f'instrument_name, t_min, em_min, em_max, access_url '
        f"FROM ivoa.ObsCore WHERE {' AND '.join(where)} ORDER BY obs_collection, t_min"
    )


_PIPELINE_SUFFIX = re.compile(r'-(DRAGONS|CALIBRATED)$')


def _dedupe_pipeline_products(rows):
    """Drop duplicate products of the same exposure, preferring the DRAGONS reduction.

    The same exposure is frequently served twice, once as ``*_CALIBRATED`` and once
    as ``*_DRAGONS``; downloading both wastes a round trip and produces two
    near-identical spectra for one epoch.
    """
    seen = {}
    for index, row in enumerate(rows):
        obs_id = str(row['obs_id'])
        base = _PIPELINE_SUFFIX.sub('', obs_id)
        rank = 0 if obs_id.endswith('DRAGONS') else 1
        if base not in seen or rank < seen[base][0]:
            seen[base] = (rank, index)
    keep = sorted(value[1] for value in seen.values())
    if len(keep) == len(rows):
        return rows
    logger.debug('Gemini: dedup %s -> %s rows (same exposure served by two pipelines).', len(rows), len(keep))
    return rows[keep]


class GeminiSpectraDataService(DataService):
    name = 'GeminiSpectra'
    verbose_name = 'Gemini Spectra'
    update_on_daily_refresh = False
    info_url = GEMINI_ARCHIVE_URL
    service_notes = (
        'Query public Gemini spectra by coordinates from the CADC ObsCore TAP service. '
        'Flux is detector electrons, stored in counts.'
    )

    @classmethod
    def get_form_class(cls):
        return GeminiSpectraQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or 10.0,
            'include_spectroscopy': bool(parameters.get('include_spectroscopy', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or 10.0

        obs_table = None

        if ra is None or dec is None:
            self.query_results = {'spectroscopy_data': None, 'source_location': None}
            return self.query_results

        try:
            tap = pyvo.dal.TAPService(CADC_TAP_URL)
            result = tap.run_sync(
                _build_cadc_query(ra, dec, radius_arcsec),
                maxrec=MAX_OBSCORE_ROWS,
            )
            table = result.to_table()
            if len(table) > 0:
                obs_table = _dedupe_pipeline_products(table)
            else:
                logger.debug('Gemini/CADC returned no spectrum for RA=%s Dec=%s', ra, dec)
        except Exception as exc:
            logger.debug('Gemini/CADC TAP error %s', exc)

        self.query_results = {
            'spectroscopy_data': obs_table if obs_table is not None and len(obs_table) else None,
            'source_location': GEMINI_ARCHIVE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        ra = data.get('ra')
        dec = data.get('dec')
        spectroscopy_data = data.get('spectroscopy_data')
        if ra is None or dec is None or spectroscopy_data is None:
            return []

        datums = self._build_spectroscopy_datums(spectroscopy_data)
        if not datums:
            return []

        return [{
            'name': None,
            'ra': ra,
            'dec': dec,
            'aliases': [None],
            'reduced_datums': {'spectroscopy': datums},
            'source_location': data.get('source_location'),
        }]

    def create_target_from_query(self, target_result, **kwargs):
        return Target(
            name=target_result['name'],
            type='SIDEREAL',
            ra=target_result.get('ra'),
            dec=target_result.get('dec'),
            epoch=2000.0,
        )

    def create_aliases_from_query(self, alias_results, **kwargs):
        return [TargetName(name=alias) for alias in alias_results]

    def create_reduced_datums_from_query(self, target, data=None, data_type=None, **kwargs):
        if data_type != 'spectroscopy' or not data:
            return
        source_location = kwargs.get('source_location') or self.info_url
        for datum in data:
            ReducedDatum.objects.get_or_create(
                target=target,
                data_type='spectroscopy',
                timestamp=datum['timestamp'],
                value=datum['value'],
                defaults={
                    'source_name': self.name,
                    'source_location': source_location,
                },
            )

    def to_reduced_datums(self, target, data_results=None, **kwargs):
        if not data_results:
            return
        for data_type, data in data_results.items():
            self.create_reduced_datums_from_query(
                target,
                data=data,
                data_type=data_type,
                source_location=self.query_results.get('source_location') or self.info_url,
            )

    def _build_spectroscopy_datums(self, obs_table):
        output = []
        for row in obs_table:
            if len(output) >= MAX_SPECTRA:
                logger.info('Gemini: reached the %s spectrum cap; ignoring the rest.', MAX_SPECTRA)
                break
            datum = self._datum_from_obscore_row(row)
            if datum:
                output.append(datum)
        return output

    def _datum_from_obscore_row(self, row):
        """Follow one ObsCore row through DataLink and decode it, or return None.

        Never raises: a product that is missing, proprietary, or in a format the
        decoder does not recognise is logged and skipped so it cannot discard the
        rest of the batch.
        """
        instrument = str(row['instrument_name']).strip()
        obs_id = str(row['obs_id']).strip()
        why = 'no DataLink rows'

        try:
            links = datalink_urls(row['access_url'])
        except Exception as exc:
            logger.warning('Gemini: DataLink failed for obs_id=%s: %s', obs_id, exc)
            return None

        for url, _semantics in links:
            try:
                kwargs = {'timeout': GEMINI_FITS_TIMEOUT}
                cert = _gemini_cert()
                if cert:
                    kwargs['cert'] = cert
                response = requests.get(url, **kwargs)
                if not response.ok:
                    why = f'HTTP {response.status_code}'
                    continue
                spectrum_data = read_spectrum(response.content)
            except Exception as exc:
                why = f'{type(exc).__name__}: {str(exc)[:60]}'
                continue

            if spectrum_data is None:
                why = describe_blob(response.content)
                continue

            lam_nm, flux, err, note, snr = spectrum_data
            mjd, mjd_source = mjd_from_row(row)
            if mjd is None:
                mjd, mjd_source = mjd_from_fits(response.content)
            if mjd is None:
                logger.warning('Gemini: no observation time for obs_id=%s; skipping.', obs_id)
                return None

            try:
                # Electrons, so store as counts, matching the 6dFGS convention.
                # Wavelengths are decoded in nm and converted to the Angstrom
                # spectral axis used by the other BHTOM spectroscopy services.
                spectrum = Spectrum1D(
                    flux=flux * u.ct,
                    spectral_axis=(lam_nm * 10.0) * u.AA,
                )
                serialized = SpectrumSerializer().serialize(spectrum)
            except Exception as exc:
                logger.warning('Gemini: could not serialize obs_id=%s: %s', obs_id, exc)
                return None

            serialized.update({
                'filter': f'Gemini-{instrument}' if instrument else 'Gemini',
                'source_id': obs_id,
                'spectrum_type': 'Gemini_spectrum',
                'instrument': instrument,
                'snr': round(snr, 2),
                'extraction': note,
            })
            logger.debug(
                'Gemini: OK %s / %s MJD %.4f (%s) %.1f-%.1f nm S/N=%.1f',
                instrument, obs_id, mjd, mjd_source, lam_nm.min(), lam_nm.max(), snr,
            )
            return {
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': serialized,
            }

        logger.info('Gemini: no usable spectrum for obs_id=%s (%s): %s', obs_id, instrument, why)
        return None
