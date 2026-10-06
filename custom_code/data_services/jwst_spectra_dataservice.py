"""Public JWST NIRSpec and MIRI spectra from MAST.

MAST is searched by position for public, level-3 (combined) JWST spectroscopic observations
with NIRSpec (fixed slit, MOS, IFU) and MIRI (LRS slit, MRS). Each level-3 product has a 1D
spectrum, <obs_id>_x1d.fits; for the IFUs it is the pipeline's extraction from the cube in an
aperture around the source position, which suits point sources (SNe, AGN nuclei, stars) but
not extended objects. Time-series (TSO) and slitless (WFSS, SOSS) data are not used.

The per-band products of one observation (e.g. the 12 MIRI MRS sub-bands, or NIRSpec
G235M + G395M) are merged into one spectrum per program, observation, source and instrument.
Per-observation (-o) products are used; the cross-observation (-c) combinations are used only
when no per-observation product exists. Pixels are kept when wavelength, flux and error are
finite, the error positive and the DO_NOT_USE bit unset; MRS spectra use the residual-fringe
corrected flux when the file has one. Fluxes are converted from Jy to erg s^-1 cm^-2 A^-1 and
wavelengths from um to A; merged spectra above JWST_MAX_POINTS pixels are averaged into that
many equal-pixel-count bins (full resolution on MAST). The timestamp is the mean mid-exposure
time of the merged products.
"""

import concurrent.futures
import io
import logging
import re
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

from custom_code.data_services.forms import JWSTSpectraQueryForm
from custom_code.data_services.hst_spectra_dataservice import MAST_DOWNLOAD_URL, _bin_spectrum, _text, _to_float
from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT, resolve_query_coordinates

logger = logging.getLogger(__name__)

JWST_SPECTRA_PAGE_URL = 'https://mast.stsci.edu/search/ui/#/jwst'
JWST_SPECTRA_INSTRUMENTS = ['NIRSPEC/SLIT', 'NIRSPEC/MSA', 'NIRSPEC/IFU', 'MIRI/SLIT', 'MIRI/IFU']
JWST_DEFAULT_RADIUS_ARCSEC = 3.0
JWST_MAX_SPECTRA = 30
JWST_MAX_POINTS = 8000
JWST_DOWNLOAD_WORKERS = 6
JWST_DO_NOT_USE = 1
SPEED_OF_LIGHT_AA = 2.99792458e18  # A/s
# Level-3 obs_id up to the instrument: jw01328-o015_t014_miri, jw04522-o001_t001-s000000001_nirspec.
JWST_GROUP_RE = re.compile(r'^(jw\d{5}-([oc])[^_]+_.*?_(?:nirspec|miri))_', re.IGNORECASE)

JWST_SPECTRA_ACKNOWLEDGEMENT = (
    'This work is based on observations made with the NASA/ESA/CSA James Webb Space Telescope. '
    'The data were obtained from the Mikulski Archive for Space Telescopes at the Space Telescope '
    'Science Institute, which is operated by the Association of Universities for Research in '
    'Astronomy, Inc., under NASA contract NAS 5-03127 for JWST.'
)

FLAMBDA = u.erg / (u.s * u.cm ** 2 * u.AA)


def _spectrum_groups(observations):
    """{group key: [(obs_id, start MJD, instrument)]} of the level-3 products to merge."""
    groups = {}
    for row in observations:
        obs_id = _text(row['obs_id'])
        match = JWST_GROUP_RE.match(obs_id)
        if match:
            groups.setdefault(match.group(1).lower(), []).append(
                (obs_id, _to_float(row['t_min']), _text(row['instrument_name'])),
            )

    def signature(key):
        # Program, target/source and instrument, without the observation or candidate number.
        return re.sub(r'^(jw\d{5})-[oc][^_]+_', r'\1-_', key)

    per_observation = {signature(key) for key in groups if JWST_GROUP_RE.match(key + '_').group(2) == 'o'}
    return {
        key: members for key, members in groups.items()
        if JWST_GROUP_RE.match(key + '_').group(2) == 'o' or signature(key) not in per_observation
    }


def _read_x1d(content):
    """(wavelength [A], flux [erg/s/cm2/A], header info) from a JWST level-3 x1d file."""
    wavelengths, fluxes = [], []
    with fits.open(io.BytesIO(content)) as hdul:
        primary = hdul[0].header
        for hdu in hdul:
            if hdu.name != 'EXTRACT1D' or hdu.data is None:
                continue
            data, names = hdu.data, hdu.data.columns.names
            wavelength = np.asarray(data['WAVELENGTH'], dtype=float) * 1e4
            flux = np.asarray(data['FLUX'], dtype=float)
            if 'RF_FLUX' in names:
                rf_flux = np.asarray(data['RF_FLUX'], dtype=float)
                if np.isfinite(rf_flux).any() and np.nanmax(np.abs(rf_flux)) > 0:
                    flux = rf_flux
            error = np.asarray(data['FLUX_ERROR'], dtype=float)
            good = np.isfinite(wavelength) & (wavelength > 0) & np.isfinite(flux) & np.isfinite(error) & (error > 0)
            if 'DQ' in names:
                good &= (np.asarray(data['DQ'], dtype=np.int64) & JWST_DO_NOT_USE) == 0
            wavelengths.append(wavelength[good])
            # F_lambda = F_nu c / lambda^2, with F_nu in Jy = 1e-23 erg/s/cm2/Hz.
            fluxes.append(flux[good] * 1e-23 * SPEED_OF_LIGHT_AA / wavelength[good] ** 2)
        info = {
            'instrument': _text(primary.get('INSTRUME')),
            'exp_type': _text(primary.get('EXP_TYPE')),
            'setting': '-'.join(part for part in (
                _text(primary.get('GRATING')) if primary.get('GRATING') not in (None, 'N/A') else '',
                _text(primary.get('FILTER')) if primary.get('FILTER') not in (None, 'N/A') else '',
                f"ch{_text(primary.get('CHANNEL'))}" if primary.get('CHANNEL') else '',
                _text(primary.get('BAND')).lower() if primary.get('BAND') else '',
            ) if part),
            'program': _text(primary.get('PROGRAM')),
            'target_name': _text(primary.get('TARGPROP')),
            'expmid': _to_float(primary.get('EXPMID')),
            'effexptm': _to_float(primary.get('EFFEXPTM')),
        }
    wavelength = np.concatenate(wavelengths) if wavelengths else np.array([])
    flux = np.concatenate(fluxes) if fluxes else np.array([])
    return wavelength, flux, info


def _download_x1d(obs_id):
    uri = f'mast:JWST/product/{obs_id}_x1d.fits'
    response = requests.get(MAST_DOWNLOAD_URL, params={'uri': uri}, timeout=DATA_SERVICE_HTTP_TIMEOUT)
    response.raise_for_status()
    return uri, _read_x1d(response.content)


def _merge_group(key, members, pool):
    """One spectroscopy datum from the x1d products of one observation, or None."""
    parts = []
    for (obs_id, start, instrument), future in [(member, pool.submit(_download_x1d, member[0])) for member in members]:
        try:
            uri, (wavelength, flux, info) = future.result()
        except Exception as exc:
            logger.warning('JWSTSpectra: could not read %s: %s', obs_id, exc)
            continue
        if wavelength.size >= 10:
            parts.append((obs_id, start, instrument, uri, wavelength, flux, info))
    if not parts:
        return None

    wavelength = np.concatenate([part[4] for part in parts])
    flux = np.concatenate([part[5] for part in parts])
    n_pixels = int(wavelength.size)
    wavelength, flux = _bin_spectrum(wavelength, flux, JWST_MAX_POINTS)
    mjds = [part[6]['expmid'] or part[1] for part in parts]
    mjds = [mjd for mjd in mjds if mjd is not None]
    if not mjds:
        return None

    info = parts[0][6]
    instrument = info['instrument'] or parts[0][2].split('/')[0]
    mode = parts[0][2].split('/')[-1] if '/' in parts[0][2] else ''
    settings = sorted({part[6]['setting'] for part in parts if part[6]['setting']})
    serialized = SpectrumSerializer().serialize(Spectrum1D(flux=flux * FLAMBDA, spectral_axis=wavelength * u.AA))
    serialized.update({
        'filter': f'JWST-{instrument}({mode})' if mode else f'JWST-{instrument}',
        'source_id': key,
        'spectrum_type': 'JWST_spectrum',
        'instrument': instrument,
        'mode': mode,
        'exp_type': info['exp_type'],
        'settings': settings,
        'n_products': len(parts),
        'program': info['program'],
        'jwst_target_name': info['target_name'],
        'exptime': sum(part[6]['effexptm'] or 0.0 for part in parts) or None,
        'extraction': 'IFU cube aperture extraction (point-source)' if mode == 'IFU' else 'slit',
        'n_pixels': n_pixels,
        'binned': n_pixels > JWST_MAX_POINTS,
        'mast_uris': [part[3] for part in parts],
    })
    return {
        'timestamp': Time(float(np.mean(mjds)), format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
        'value': serialized,
    }


class JWSTSpectraDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return JWST_DEFAULT_RADIUS_ARCSEC

    name = 'JWSTSpectra'
    verbose_name = 'JWST spectra (NIRSpec/MIRI, MAST)'
    # Archival spectra; a daily refresh would re-download every file for little gain.
    update_on_daily_refresh = False
    info_url = JWST_SPECTRA_PAGE_URL
    acknowledgement = JWST_SPECTRA_ACKNOWLEDGEMENT
    service_notes = (
        'Query MAST by coordinates for public JWST NIRSpec (fixed slit, MOS, IFU) and MIRI (LRS, MRS) '
        'spectra. The level-3 1D spectra of each observation are merged into one spectrum, the 30 '
        'most recent by default, with flagged pixels removed and averaged to at most 8000 points '
        '(full resolution on MAST). IFU spectra are the pipeline point-source extraction. Fluxes in '
        'erg/s/cm2/A.'
    )

    @classmethod
    def get_form_class(cls):
        return JWSTSpectraQueryForm

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
            'max_spectra': parameters.get('max_spectra') or JWST_MAX_SPECTRA,
            'include_spectroscopy': bool(parameters.get('include_spectroscopy', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or self.get_finding_chart_radius_arcsec()
        max_spectra = int(_to_float(query_parameters.get('max_spectra')) or JWST_MAX_SPECTRA)

        spectra = []
        if ra is not None and dec is not None and query_parameters.get('include_spectroscopy', True):
            try:
                from astroquery.mast import Observations

                observations = Observations.query_criteria(
                    coordinates=SkyCoord(ra, dec, unit='deg'),
                    radius=radius_arcsec * u.arcsec,
                    obs_collection='JWST',
                    dataproduct_type=['spectrum', 'cube'],
                    calib_level=3,
                    intentType='science',
                    dataRights='PUBLIC',
                    instrument_name=JWST_SPECTRA_INSTRUMENTS,
                )
                groups = _spectrum_groups(observations)
                ordered = sorted(groups, key=lambda key: max(m[1] or 0 for m in groups[key]), reverse=True)
                if len(ordered) > max_spectra:
                    logger.info('JWSTSpectra: %s observations found; keeping the %s most recent.', len(ordered), max_spectra)
                # Observations with no usable pixels do not count towards the cap.
                with concurrent.futures.ThreadPoolExecutor(max_workers=JWST_DOWNLOAD_WORKERS) as pool:
                    for key in ordered:
                        if len(spectra) >= max_spectra:
                            break
                        datum = _merge_group(key, groups[key], pool)
                        if datum:
                            spectra.append(datum)
            except Exception as exc:
                logger.warning('JWSTSpectra query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                spectra = []

        self.query_results = {
            'spectroscopy_data': spectra,
            'source_location': JWST_SPECTRA_PAGE_URL,
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
