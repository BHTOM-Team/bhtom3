import logging
import math
import threading
import time
from datetime import timezone

import astropy.units as u
import numpy as np
import requests
from astropy.time import Time
from specutils import Spectrum1D

from tom_dataproducts.models import ReducedDatum
from tom_dataproducts.processors.data_serializers import SpectrumSerializer
from tom_dataservices.dataservices import DataService
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import SCATQueryForm
from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT

logger = logging.getLogger(__name__)

SCAT_PAGE_URL = 'https://joysankar-astro.github.io/SCATv1/'
SCAT_API_URL = f'{SCAT_PAGE_URL}api/v1/'
SCAT_CATALOG_TTL = 3600
SCAT_DEFAULT_RADIUS_ARCSEC = 10.0

_catalog_cache = {'fetched_at': 0.0, 'objects': None}
_catalog_lock = threading.Lock()


def _to_float(value):
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(converted) else converted


def _fetch_catalog(base_url=SCAT_API_URL):
    """Return the SCAT catalog objects, cached: catalog.json is ~670 KB and static."""
    with _catalog_lock:
        if _catalog_cache['objects'] is not None and time.time() - _catalog_cache['fetched_at'] < SCAT_CATALOG_TTL:
            return _catalog_cache['objects']
        response = requests.get(f'{base_url}catalog.json', timeout=DATA_SERVICE_HTTP_TIMEOUT)
        response.raise_for_status()
        _catalog_cache['objects'] = response.json().get('objects') or []
        _catalog_cache['fetched_at'] = time.time()
        return _catalog_cache['objects']


def _separation_arcsec(ra1, dec1, ra2, dec2):
    """Angular separation in arcsec (haversine), all inputs in degrees."""
    r1, d1, r2, d2 = map(math.radians, (ra1, dec1, ra2, dec2))
    a = math.sin((d2 - d1) / 2) ** 2 + math.cos(d1) * math.cos(d2) * math.sin((r2 - r1) / 2) ** 2
    return math.degrees(2 * math.asin(min(1.0, math.sqrt(a)))) * 3600


def _cone_search(objects, ra, dec, radius_arcsec):
    """Objects with at least one spectrum inside the cone, nearest first.

    SCAT only stores a position per spectrum (OBJRA/OBJDEC), so each result keeps just the
    spectra that fall inside the cone.
    """
    hits = []
    for obj in objects:
        matched = []
        for spectrum in obj.get('spectra') or []:
            sp_ra, sp_dec = _to_float(spectrum.get('ra')), _to_float(spectrum.get('dec'))
            if sp_ra is None or sp_dec is None:
                continue
            separation = _separation_arcsec(ra, dec, sp_ra, sp_dec)
            if separation <= radius_arcsec:
                matched.append({**spectrum, 'sep_arcsec': round(separation, 3)})
        if matched:
            matched.sort(key=lambda spectrum: spectrum['sep_arcsec'])
            hits.append({**obj, 'spectra': matched, 'sep_arcsec': matched[0]['sep_arcsec']})
    return sorted(hits, key=lambda obj: obj['sep_arcsec'])


def _parse_spectrum_text(text):
    """Parse a SCAT .ascii file into (header dict, wavelength, flux) with NaN rows removed.

    Header lines are '# KEY = value // comment'; data columns are lbda [A], flux, error.
    """
    header = {}
    wavelength, flux = [], []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith('#'):
            body = line[1:].split('//', 1)[0]
            if '=' in body:
                key, _, value = body.partition('=')
                header[key.strip()] = value.strip()
            continue
        columns = line.split()
        if len(columns) < 2:
            continue
        try:
            lbda, value = float(columns[0]), float(columns[1])
        except ValueError:
            continue
        if math.isnan(lbda) or math.isnan(value):
            continue
        wavelength.append(lbda)
        flux.append(value)
    return header, np.asarray(wavelength), np.asarray(flux)


class SCATDataService(DataService):
    name = 'SCAT'
    verbose_name = 'SCAT'
    update_on_daily_refresh = False
    info_url = SCAT_PAGE_URL
    service_notes = 'Query SCAT DR1 transient spectra by coordinates. Spectra only; no aliases are added.'

    @classmethod
    def get_form_class(cls):
        return SCATQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or SCAT_DEFAULT_RADIUS_ARCSEC,
            'include_spectroscopy': bool(parameters.get('include_spectroscopy', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or SCAT_DEFAULT_RADIUS_ARCSEC

        matches = []
        if ra is None or dec is None or not query_parameters.get('include_spectroscopy', True):
            self.query_results = {'matches': matches, 'source_location': SCAT_PAGE_URL, 'ra': ra, 'dec': dec}
            return self.query_results

        try:
            matches = _cone_search(_fetch_catalog(), ra, dec, radius_arcsec)
        except Exception as exc:
            logger.warning('SCAT catalog query failed for RA=%s Dec=%s: %s', ra, dec, exc)
        if not matches:
            logger.debug('SCAT returned no spectrum for RA=%s Dec=%s', ra, dec)

        self.query_results = {
            'matches': matches,
            'source_location': SCAT_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        ra = data.get('ra')
        dec = data.get('dec')
        results = []
        for obj in data.get('matches') or []:
            datums = self._build_spectroscopy_datums(obj)
            if not datums:
                continue
            results.append({
                'name': obj['name'],
                'ra': ra,
                'dec': dec,
                # Spectra only: SCAT names must not be added to BHTOM targets as aliases.
                'aliases': [],
                'reduced_datums': {'spectroscopy': datums},
                'source_location': data.get('source_location'),
            })
        return results

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
                source_location=self.query_results.get('source_location') or self.info_url,
            )

    def _build_spectroscopy_datums(self, obj):
        output = []
        for spectrum in obj.get('spectra') or []:
            datum = self._datum_from_spectrum(obj, spectrum)
            if datum:
                output.append(datum)
        return output

    def _datum_from_spectrum(self, obj, spectrum):
        """Download one SCAT spectrum and build a datum, or return None.

        Never raises: a spectrum that is missing or malformed is logged and skipped so it
        cannot discard the rest of the batch.
        """
        try:
            response = requests.get(f"{SCAT_API_URL}{spectrum['url']}", timeout=DATA_SERVICE_HTTP_TIMEOUT)
            response.raise_for_status()
            header, wavelength, flux = _parse_spectrum_text(response.text)
            mjd = _to_float(spectrum.get('mjd')) or _to_float(header.get('MJD-OBS'))
            if mjd is None or wavelength.size == 0:
                logger.warning('SCAT: no usable spectrum in %s; skipping.', spectrum.get('file'))
                return None
            # Flux columns are in units of FLUXNORM erg/s/cm2/A, which differs between files.
            flux_scale = _to_float(header.get('FLUXNORM')) or 1.0
            spec = Spectrum1D(
                flux=flux * flux_scale * u.erg / u.s / u.cm**2 / u.AA,
                spectral_axis=wavelength * u.AA,
            )
            serialized = SpectrumSerializer().serialize(spec)
            serialized.update({
                'filter': 'SCAT',
                'source_id': str(obj['name']),
                'spectrum_type': 'SCAT_spectrum',
                'rest_frame': header.get('REDSHIFT_CORRECTED', '').lower() == 'true',
                'redshift': obj.get('redshift'),
                'sptype': obj.get('sptype'),
                'subtype': obj.get('subtype'),
                'phase': spectrum.get('phase'),
                'quality': spectrum.get('quality'),
            })
            return {
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': serialized,
            }
        except Exception as exc:
            logger.warning('SCAT: failed to process %s: %s', spectrum.get('file'), exc)
            return None
