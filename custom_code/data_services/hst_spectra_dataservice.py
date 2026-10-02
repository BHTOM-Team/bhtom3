"""Public HST UV/optical spectra (COS and STIS) from MAST.

MAST is searched by position for public HST science spectra taken with COS (FUV/NUV) and STIS
(CCD, FUV-MAMA, NUV-MAMA). One calibrated 1D spectrum is kept per dataset: the COS association
product (x1dsum, all exposures of a visit and setting combined) and the STIS x1d (MAMA) or sx1
(CCD). Individual COS exposures, target acquisitions, HASP coadds and the pre-1997 FOS/GHRS
data are not used.

Pixels are kept when their flux is finite, the error positive and they carry no serious data
quality flag (COS: DQ_WGT > 0; STIS: DQ & SDQFLAGS == 0). Segments and echelle orders are merged
in wavelength order and, so that a spectrum stays a manageable size in the database, averaged
into at most HST_MAX_POINTS equal-pixel-count bins; the full-resolution file is on MAST.
Fluxes are erg s^-1 cm^-2 A^-1 and the timestamp is the mid-exposure time.
"""

import concurrent.futures
import io
import logging
from datetime import timezone

import astropy.units as u
import numpy as np
import requests
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.time import Time
from specutils import Spectrum1D

from tom_dataproducts.models import ReducedDatum
from tom_dataproducts.processors.data_serializers import SpectrumSerializer
from tom_dataservices.dataservices import DataService
from tom_targets.models import Target

from custom_code.data_services.forms import HSTSpectraQueryForm
from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT, resolve_query_coordinates

logger = logging.getLogger(__name__)

HST_SPECTRA_PAGE_URL = 'https://mast.stsci.edu/search/ui/#/hst'
MAST_DOWNLOAD_URL = 'https://mast.stsci.edu/api/v0.1/Download/file'
HST_SPECTRA_INSTRUMENTS = ['COS/FUV', 'COS/NUV', 'STIS/CCD', 'STIS/FUV-MAMA', 'STIS/NUV-MAMA']
HST_SPECTRA_PRODUCTS = ('X1DSUM', 'X1D', 'SX1')
HST_DEFAULT_RADIUS_ARCSEC = 5.0
HST_MAX_SPECTRA = 30
HST_MAX_POINTS = 4000
HST_DOWNLOAD_WORKERS = 6

HST_SPECTRA_ACKNOWLEDGEMENT = (
    'Based on observations made with the NASA/ESA Hubble Space Telescope, obtained from the '
    'Mikulski Archive for Space Telescopes (MAST) at the Space Telescope Science Institute, which '
    'is operated by the Association of Universities for Research in Astronomy, Inc., under NASA '
    'contract NAS 5-26555.'
)


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _text(value):
    if value is None or value is np.ma.masked:
        return ''
    return str(value).strip()


def _spectrum_uris(observations):
    """(obs_id, data URI, start MJD) of one 1D spectrum per COS/STIS dataset."""
    from astroquery.mast import Observations

    found = {}
    needs_product_list = []
    for row in observations:
        obs_id = _text(row['obs_id'])
        instrument = _text(row['instrument_name'])
        data_url = _text(row['dataURL'])
        start = _to_float(row['t_min'])
        if data_url.endswith('_x1dsum.fits') or (instrument.startswith('STIS') and data_url.endswith(('_x1d.fits', '_sx1.fits'))):
            found[obs_id] = (obs_id, data_url, start)
        elif instrument.startswith('STIS'):
            # STIS MAMA datasets often have no preview product; their x1d is in the product list.
            needs_product_list.append(row)
        # COS rows pointing at a single exposure (x1d) or an acquisition are covered by x1dsum.

    if needs_product_list:
        from astropy.table import vstack

        products = Observations.get_product_list(vstack(needs_product_list))
        starts = {_text(row['obsid']): _to_float(row['t_min']) for row in needs_product_list}
        for product in products:
            if _text(product['productSubGroupDescription']) not in HST_SPECTRA_PRODUCTS:
                continue
            obs_id = _text(product['obs_id'])
            if obs_id and obs_id not in found:
                found[obs_id] = (obs_id, _text(product['dataURI']), starts.get(_text(product['obsID'])))
    return list(found.values())


def _bin_spectrum(wavelength, flux, max_points=HST_MAX_POINTS):
    order = np.argsort(wavelength)
    wavelength, flux = wavelength[order], flux[order]
    if wavelength.size <= max_points:
        return wavelength, flux
    edges = np.linspace(0, wavelength.size, max_points + 1).astype(int)
    counts = np.diff(edges)
    return (np.add.reduceat(wavelength, edges[:-1]) / counts,
            np.add.reduceat(flux, edges[:-1]) / counts)


def _read_spectrum(content):
    """(wavelength [A], flux [erg/s/cm2/A], header info) from a COS x1dsum or STIS x1d/sx1 file."""
    with fits.open(io.BytesIO(content)) as hdul:
        primary, header, data = hdul[0].header, hdul[1].header, hdul[1].data
        names = data.columns.names
        serious = int(header.get('SDQFLAGS', 0) or 0)
        wavelengths, fluxes = [], []
        for row in data:
            wavelength = np.asarray(row['WAVELENGTH'], dtype=float)
            flux = np.asarray(row['FLUX'], dtype=float)
            error = np.asarray(row['ERROR'], dtype=float)
            good = np.isfinite(wavelength) & np.isfinite(flux) & np.isfinite(error) & (error > 0) & (wavelength > 0)
            if 'DQ_WGT' in names:
                good &= np.asarray(row['DQ_WGT'], dtype=float) > 0
            elif 'DQ' in names:
                good &= (np.asarray(row['DQ'], dtype=np.int64) & serious) == 0
            wavelengths.append(wavelength[good])
            fluxes.append(flux[good])
        info = {
            'instrument': _text(primary.get('INSTRUME')),
            'detector': _text(primary.get('DETECTOR')),
            'grating': _text(primary.get('OPT_ELEM')),
            'cenwave': primary.get('CENWAVE'),
            'aperture': _text(primary.get('APERTURE')),
            'target_name': _text(primary.get('TARGNAME')),
            'proposal_id': _text(primary.get('PROPOSID')),
            'expstart': _to_float(header.get('EXPSTART')),
            'expend': _to_float(header.get('EXPEND')),
            'exptime': _to_float(header.get('EXPTIME')),
        }
    wavelength = np.concatenate(wavelengths) if wavelengths else np.array([])
    flux = np.concatenate(fluxes) if fluxes else np.array([])
    return wavelength, flux, info


def _download_spectrum(item):
    obs_id, uri, start = item
    response = requests.get(MAST_DOWNLOAD_URL, params={'uri': uri}, timeout=DATA_SERVICE_HTTP_TIMEOUT)
    response.raise_for_status()
    wavelength, flux, info = _read_spectrum(response.content)
    if wavelength.size < 10:
        return None
    n_pixels = int(wavelength.size)
    wavelength, flux = _bin_spectrum(wavelength, flux)
    if info['expstart'] is not None and info['expend'] is not None:
        mjd = (info['expstart'] + info['expend']) / 2.0
    else:
        mjd = info['expstart'] or start
    if mjd is None:
        return None

    serialized = SpectrumSerializer().serialize(Spectrum1D(
        flux=flux * (u.erg / (u.s * u.cm ** 2 * u.AA)),
        spectral_axis=wavelength * u.AA,
    ))
    label = f"HST-{info['instrument']}({info['grating']})" if info['grating'] else f"HST-{info['instrument']}"
    serialized.update({
        'filter': label,
        'source_id': obs_id,
        'spectrum_type': 'HST_spectrum',
        'instrument': info['instrument'],
        'detector': info['detector'],
        'grating': info['grating'],
        'cenwave': info['cenwave'],
        'aperture': info['aperture'],
        'exptime': info['exptime'],
        'proposal_id': info['proposal_id'],
        'hst_target_name': info['target_name'],
        'n_pixels': n_pixels,
        'binned': n_pixels > HST_MAX_POINTS,
        'mast_uri': uri,
    })
    return {
        'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
        'value': serialized,
    }


class HSTSpectraDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return HST_DEFAULT_RADIUS_ARCSEC

    name = 'HSTSpectra'
    verbose_name = 'HST spectra (COS/STIS, MAST)'
    # Archival spectra; a daily refresh would re-download every file for little gain.
    update_on_daily_refresh = False
    info_url = HST_SPECTRA_PAGE_URL
    acknowledgement = HST_SPECTRA_ACKNOWLEDGEMENT
    service_notes = (
        'Query MAST by coordinates for public HST COS and STIS spectra. One calibrated 1D spectrum '
        'per dataset (COS x1dsum, STIS x1d/sx1), the 30 most recent by default, with flagged pixels '
        'removed and averaged to at most 4000 points (full resolution on MAST). Fluxes in '
        'erg/s/cm2/A.'
    )

    @classmethod
    def get_form_class(cls):
        return HSTSpectraQueryForm

    @classmethod
    def get_acknowledgement(cls):
        return cls.acknowledgement

    def build_query_parameters(self, parameters, **kwargs):
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or self.get_finding_chart_radius_arcsec(),
            'max_spectra': parameters.get('max_spectra') or HST_MAX_SPECTRA,
            'include_spectroscopy': bool(parameters.get('include_spectroscopy', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or self.get_finding_chart_radius_arcsec()
        max_spectra = int(_to_float(query_parameters.get('max_spectra')) or HST_MAX_SPECTRA)

        spectra = []
        if ra is not None and dec is not None and query_parameters.get('include_spectroscopy', True):
            try:
                from astroquery.mast import Observations

                observations = Observations.query_criteria(
                    coordinates=SkyCoord(ra, dec, unit='deg'),
                    radius=radius_arcsec * u.arcsec,
                    obs_collection='HST',
                    dataproduct_type='spectrum',
                    intentType='science',
                    dataRights='PUBLIC',
                    instrument_name=HST_SPECTRA_INSTRUMENTS,
                )
                items = sorted(_spectrum_uris(observations), key=lambda item: item[2] or 0, reverse=True)
                if len(items) > max_spectra:
                    logger.info('HSTSpectra: %s spectra found; keeping the %s most recent.', len(items), max_spectra)
                # Datasets with no usable pixels (e.g. failed visits) do not count towards the cap,
                # so fetch in batches until enough spectra are read or the list runs out.
                with concurrent.futures.ThreadPoolExecutor(max_workers=HST_DOWNLOAD_WORKERS) as pool:
                    while items and len(spectra) < max_spectra:
                        batch, items = items[:max_spectra - len(spectra)], items[max_spectra - len(spectra):]
                        for item, future in [(item, pool.submit(_download_spectrum, item)) for item in batch]:
                            try:
                                datum = future.result()
                            except Exception as exc:
                                logger.warning('HSTSpectra: could not read %s: %s', item[0], exc)
                                continue
                            if datum:
                                spectra.append(datum)
            except Exception as exc:
                logger.warning('HSTSpectra query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                spectra = []

        self.query_results = {
            'spectroscopy_data': spectra,
            'source_location': HST_SPECTRA_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        if data.get('ra') is None or data.get('dec') is None or not data.get('spectroscopy_data'):
            return []
        return [{
            'name': None,
            'ra': data['ra'],
            'dec': data['dec'],
            'aliases': [],
            'reduced_datums': {'spectroscopy': data['spectroscopy_data']},
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
        return []

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
