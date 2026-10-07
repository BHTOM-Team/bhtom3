"""OzDES DR2 spectra (AGN monitoring, transients, host galaxies) from AAO Data Central.

OzDES (Lidman et al. 2020, MNRAS 496, 19) used 2dF/AAOmega on the AAT over six seasons
(2013-2019) to monitor ~770 AGN for reverberation mapping, classify DES transients and measure
redshifts of their hosts and other galaxies in the 10 DES deep fields (~27 deg^2). DR2 has
~38,600 objects and, unlike most redshift surveys, every individual exposure as well as the
coadd: some AGN have more than 60 exposures over six years.

Data Central's SSA service returns the spectra within a cone; each spectrum's DataLink points to
a 5000-pixel FITS file on a common linear grid (3733-8921 A) in counts per Angstrom. The spectra
are not flux calibrated, so, like 2dFGRS and 6dFGS, they are stored in counts.

All spectra of the nearest OzDES object are used. The exposures of one night (usually three
40-min exposures) are averaged into one spectrum per night, or kept individually on request,
and the coadd of all exposures is stored as its own spectrum. The object's types, transient
classification, redshift and its quality come from the OzDES redshift catalogue.
"""

import concurrent.futures
import io
import logging
import time
import warnings
from datetime import timezone

import astropy.units as u
import numpy as np
import requests
from astropy.io import fits
from astropy.time import Time
from specutils import Spectrum1D

from tom_dataproducts.models import ReducedDatum
from tom_dataproducts.processors.data_serializers import SpectrumSerializer
from tom_dataservices.dataservices import DataService
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import OzDESQueryForm
from custom_code.data_services.gs2df_dataservice import GS2DF_SSA_URL, _text, _to_float, _votable_rows
from custom_code.data_services.service_utils import resolve_query_coordinates

logger = logging.getLogger(__name__)

OZDES_PAGE_URL = 'https://datacentral.org.au/services/ssa/'
OZDES_TAP_URL = 'https://datacentral.org.au/vo/tap/sync'
OZDES_COLLECTION = 'ozdes_dr2'
OZDES_DEFAULT_RADIUS_ARCSEC = 2.0
OZDES_DOWNLOAD_WORKERS = 4
# Data Central occasionally answers 5xx under parallel load; such requests are retried.
OZDES_RETRIES = 3
OZDES_RETRY_DELAY_SECONDS = 1.0
# Data Central's SSA search can take well over the generic 10 s data-service timeout.
OZDES_HTTP_TIMEOUT = 60
# Exposures closer than this (days) belong to one night.
OZDES_NIGHT_GAP_DAYS = 0.5
OZDES_EPOCH_MODES = ('nightly', 'exposures', 'coadd')

OZDES_ACKNOWLEDGEMENT = (
    'This research uses spectra from the Australian Dark Energy Survey (OzDES; Yuan et al. 2015, '
    'MNRAS 452, 3047; Childress et al. 2017, MNRAS 472, 273; Lidman et al. 2020, MNRAS 496, 19), '
    'obtained with the 2dF/AAOmega spectrograph on the Anglo-Australian Telescope and retrieved '
    'from AAO Data Central.'
)


def _ozdes_alias(name):
    return f'OzDES_{name}'


def _catalogue_entry(name):
    """OzDES redshift-catalogue row of the object (types, redshift, quality, classification)."""
    safe_name = name.replace("'", "''")
    response = requests.post(OZDES_TAP_URL, data={
        'REQUEST': 'doQuery',
        'LANG': 'ADQL',
        'QUERY': (
            'SELECT Object_types, z, qop, Transient_type, rmag, Comment '
            f"FROM ozdes_dr2.RedshiftCatalogue WHERE OzDES_ID = '{safe_name}'"
        ),
    }, timeout=OZDES_HTTP_TIMEOUT)
    response.raise_for_status()
    rows = _votable_rows(response.content)
    if not rows:
        return {}
    row = rows[0]
    transient_type = _text(row.get('Transient_type'))
    quality = _to_float(row.get('qop'))
    return {
        'object_types': [part for part in _text(row.get('Object_types')).split(',') if part],
        'catalogue_redshift': _to_float(row.get('z')),
        'redshift_quality': int(quality) if quality is not None else None,
        'transient_type': transient_type if transient_type and transient_type != 'None' else None,
        'rmag': _to_float(row.get('rmag')),
        'comment': _text(row.get('Comment')) or None,
    }


def _get(url, params=None):
    """GET with retries on connection errors and 5xx responses."""
    for attempt in range(OZDES_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=OZDES_HTTP_TIMEOUT)
            if response.status_code < 500:
                response.raise_for_status()
                return response
            error = requests.HTTPError(f'{response.status_code} Server Error for url: {url}', response=response)
        except (requests.ConnectionError, requests.Timeout) as exc:
            error = exc
        if attempt < OZDES_RETRIES - 1:
            time.sleep(OZDES_RETRY_DELAY_SECONDS * (attempt + 1))
    raise error


def _search_spectra(ra, dec, radius_arcsec):
    """SSA rows of the OzDES object nearest to the position (all of its spectra)."""
    response = _get(GS2DF_SSA_URL, params={
        'REQUEST': 'queryData',
        'POS': f'{ra},{dec}',
        'SIZE': radius_arcsec / 3600.0,
        'COLLECTION': OZDES_COLLECTION,
    })
    rows = [row for row in _votable_rows(response.content) if _text(row.get('target_name'))]
    if not rows:
        return None, []
    # 'score' is the separation from the search position in arcsec.
    nearest = min(rows, key=lambda row: _to_float(row.get('score')) or float('inf'))
    name = _text(nearest['target_name'])
    return name, [row for row in rows if _text(row.get('target_name')) == name]


def _fetch_spectrum(row):
    """(row, wavelength [A], counts per A, MJD) of one SSA row, or None."""
    links = _get(_text(row['access_url']))
    file_url = next(
        (_text(link['access_url']) for link in _votable_rows(links.content)
         if _text(link.get('semantics')) == '#this' and _text(link.get('access_url'))),
        None,
    )
    if not file_url:
        return None
    response = _get(file_url)
    with fits.open(io.BytesIO(response.content)) as hdul:
        header = hdul[0].header
        counts = np.asarray(hdul[0].data, dtype=float).ravel()
    pixels = np.arange(1, counts.size + 1)
    wavelength = header['CRVAL1'] + (pixels - header['CRPIX1']) * header['CDELT1']
    mjd = _to_float(header.get('TMID')) or _to_float(row.get('t_midpoint'))
    if mjd is None or not np.isfinite(counts).any():
        return None
    return row, wavelength, counts, mjd


def _nightly_groups(exposures):
    """Exposures (sorted by MJD) split into nights."""
    nights = []
    for exposure in sorted(exposures, key=lambda item: item[3]):
        if nights and exposure[3] - nights[-1][-1][3] < OZDES_NIGHT_GAP_DAYS:
            nights[-1].append(exposure)
        else:
            nights.append([exposure])
    return nights


def _average(exposures):
    """Mean spectrum of exposures on one wavelength grid; None if the grids differ."""
    wavelength = exposures[0][1]
    if any(item[1].shape != wavelength.shape or not np.allclose(item[1], wavelength) for item in exposures[1:]):
        return None
    # Pixels masked (NaN) in every exposure stay NaN and are dropped later.
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', RuntimeWarning)
        counts = np.nanmean(np.vstack([item[2] for item in exposures]), axis=0)
    return wavelength, counts


class OzDESDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return OZDES_DEFAULT_RADIUS_ARCSEC

    name = 'OzDES'
    verbose_name = 'OzDES DR2'
    # DR2 is the final release.
    update_on_daily_refresh = False
    info_url = OZDES_PAGE_URL
    acknowledgement = OZDES_ACKNOWLEDGEMENT
    service_notes = (
        'Query OzDES DR2 spectra (AAT 2dF/AAOmega, 2013-2019, DES deep fields; AGN monitoring, '
        'transients and their hosts) by coordinates from AAO Data Central. All spectra of the nearest '
        'OzDES object within 2 arcsec are imported in counts (not flux calibrated): one averaged '
        'spectrum per night by default, plus the coadd, with the OzDES object types, transient '
        'classification and redshift.'
    )

    @classmethod
    def get_form_class(cls):
        return OzDESQueryForm

    @classmethod
    def get_acknowledgement(cls):
        return cls.acknowledgement

    def build_query_parameters(self, parameters, **kwargs):
        target_name, ra, dec = resolve_query_coordinates(parameters)
        epochs = parameters.get('epochs') or 'nightly'
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or OZDES_DEFAULT_RADIUS_ARCSEC,
            'epochs': epochs if epochs in OZDES_EPOCH_MODES else 'nightly',
            'include_spectroscopy': bool(parameters.get('include_spectroscopy', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or OZDES_DEFAULT_RADIUS_ARCSEC
        epochs = query_parameters.get('epochs') or 'nightly'

        name, catalogue, spectra = None, {}, []
        if ra is not None and dec is not None and query_parameters.get('include_spectroscopy', True):
            try:
                name, rows = _search_spectra(ra, dec, radius_arcsec)
                if epochs == 'coadd':
                    rows = [row for row in rows if _text(row.get('dataproduct_subtype')) == 'combined']
                with concurrent.futures.ThreadPoolExecutor(max_workers=OZDES_DOWNLOAD_WORKERS) as pool:
                    futures = [pool.submit(_fetch_spectrum, row) for row in rows]
                    for future in futures:
                        try:
                            spectrum = future.result()
                        except Exception as exc:
                            logger.warning('OzDES: could not read a spectrum of %s: %s', name, exc)
                            continue
                        if spectrum is not None:
                            spectra.append(spectrum)
                if name:
                    try:
                        catalogue = _catalogue_entry(name)
                    except Exception as exc:
                        logger.debug('OzDES: no catalogue entry for %s: %s', name, exc)
                else:
                    logger.debug('OzDES returned no spectrum for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('OzDES query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                name, spectra = None, []

        self.query_results = {
            'name': name,
            'catalogue': catalogue,
            'spectra': spectra,
            'epochs': epochs,
            'source_location': OZDES_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        name = data.get('name')
        if data.get('ra') is None or data.get('dec') is None or not name:
            return []
        datums = self._build_spectroscopy_datums(
            data.get('spectra') or [], name, data.get('catalogue') or {}, data.get('epochs') or 'nightly',
        )
        if not datums:
            return []
        alias = _ozdes_alias(name)
        return [{
            'name': alias,
            'ra': data['ra'],
            'dec': data['dec'],
            'aliases': [alias],
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
                source_location=(getattr(self, 'query_results', {}) or {}).get('source_location') or self.info_url,
            )

    def _build_spectroscopy_datums(self, spectra, name, catalogue, epochs):
        coadds = [item for item in spectra if _text(item[0].get('dataproduct_subtype')) == 'combined']
        exposures = [item for item in spectra if _text(item[0].get('dataproduct_subtype')) != 'combined']

        # (kind, wavelength, counts, MJD, number of exposures)
        entries = []
        for _row, wavelength, counts, mjd in coadds:
            entries.append(('coadd', wavelength, counts, mjd, None))
        if epochs == 'exposures':
            entries.extend(('exposure', w, c, mjd, 1) for _row, w, c, mjd in exposures)
        elif epochs == 'nightly':
            for night in _nightly_groups(exposures):
                averaged = _average(night)
                if averaged is None:
                    entries.extend(('exposure', w, c, mjd, 1) for _row, w, c, mjd in night)
                else:
                    entries.append(('night', averaged[0], averaged[1], float(np.mean([item[3] for item in night])), len(night)))

        redshift = next((_to_float(item[0].get('redshift')) for item in spectra if _to_float(item[0].get('redshift')) is not None), None)
        output = []
        serializer = SpectrumSerializer()
        for kind, wavelength, counts, mjd, n_exposures in entries:
            good = np.isfinite(counts)
            if good.sum() < 10:
                continue
            serialized = serializer.serialize(Spectrum1D(flux=counts[good] * u.ct, spectral_axis=wavelength[good] * u.AA))
            serialized.update({
                'filter': 'OzDES' if kind != 'coadd' else 'OzDES(coadd)',
                'source_id': name,
                'spectrum_type': 'OzDES_coadd' if kind == 'coadd' else 'OzDES_spectrum',
                'epoch_type': kind,
                'n_exposures': n_exposures,
                'flux_calibration': 'none (counts per Angstrom)',
                'redshift': catalogue.get('catalogue_redshift', redshift),
                'redshift_quality': catalogue.get('redshift_quality'),
                'object_types': catalogue.get('object_types'),
                'transient_type': catalogue.get('transient_type'),
                'rmag': catalogue.get('rmag'),
                'comment': catalogue.get('comment'),
                'data_release': 'OzDES DR2',
            })
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': serialized,
            })
        return output
