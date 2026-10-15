"""ATCA Calibrator Database: cm-wavelength flux-density light curves of ATCA calibrators.

The Australia Telescope Compact Array has measured its calibrators (mostly bright, compact
quasars and blazars, about 1,000 sources, nearly all south of Dec +30) since the 1990s in the
16 cm, 4 cm, 15 mm, 7 mm and 3 mm bands. For every observation the database stores a model of
the flux-density spectrum across the band: polynomial coefficients in log10(frequency / GHz)
giving log10(S / Jy), and the scatter of the data about that model.

The nearest calibrator to the target is found with the database's position search, and all of
its measurements are read in one request. Like the ATCA web interface, each band's model is
evaluated at the band's reference frequencies (2.1; 5.5 and 9.0; 17; 33; 93 GHz). The fit
scatter (floored at 1% of the flux density) is used as the uncertainty. Flux densities are
stored in mJy as radio data, like the ALMA service, and plotted on the radio axis.
"""

import logging
import math
import time
from datetime import timezone

import requests
from astropy.coordinates import Angle
from astropy.time import Time
import astropy.units as u

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target

from custom_code.data_services.forms import ATCAQueryForm
from custom_code.data_services.service_utils import resolve_query_coordinates, upsert_reduced_datums

logger = logging.getLogger(__name__)

ATCA_PAGE_URL = 'https://www.narrabri.atnf.csiro.au/calibrators/'
ATCA_API_URL = 'https://www.narrabri.atnf.csiro.au/cgi-bin/Calibrators/new/caldb_v3.pl'
ATCA_DEFAULT_RADIUS_ARCSEC = 5.0
ATCA_HTTP_TIMEOUT = 60
ATCA_RETRIES = 3
ATCA_FLUX_UNIT = 'mJy'
# Reference frequencies (GHz) at which each band's model is evaluated, as on the ATCA web pages.
ATCA_BAND_FREQUENCIES_GHZ = {
    '16cm': (2.1,),
    '4cm': (5.5, 9.0),
    '15mm': (17.0,),
    '7mm': (33.0,),
    '3mm': (93.0,),
}
ATCA_MIN_RELATIVE_ERROR = 0.01

ATCA_ACKNOWLEDGEMENT = (
    'This research uses flux densities from the ATCA Calibrator Database. The Australia Telescope '
    'Compact Array is part of the Australia Telescope National Facility '
    '(https://ror.org/05qajvd42), which is funded by the Australian Government for operation as a '
    'National Facility managed by CSIRO. We acknowledge the Gomeroi people as the Traditional '
    'Owners of the Observatory site.'
)


def _to_float(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _api(**data):
    """POST to the calibrator database; retried, because it answers 502 now and then."""
    error = None
    for attempt in range(ATCA_RETRIES):
        try:
            response = requests.post(ATCA_API_URL, data=data, timeout=ATCA_HTTP_TIMEOUT)
            if response.status_code < 500:
                response.raise_for_status()
                return response.json()
            error = requests.HTTPError(f'HTTP {response.status_code} from the ATCA calibrator database')
        except (requests.ConnectionError, requests.Timeout) as exc:
            error = exc
        time.sleep(1.0 + attempt)
    raise error


def _nearest_calibrator(ra, dec, radius_arcsec):
    """(name, separation in arcsec) of the calibrator nearest the position, or (None, None)."""
    position = (
        Angle(ra, u.deg).to_string(unit=u.hourangle, sep=':', precision=2) + ','
        + Angle(dec, u.deg).to_string(unit=u.deg, sep=':', precision=1, alwayssign=True)
    )
    matches = _api(action='search', position=position, radius=radius_arcsec / 3600.0).get('matches') or []
    best = None
    for match in matches:
        # angular_distance is in turns.
        distance = _to_float(match.get('angular_distance'))
        if match.get('name') and distance is not None:
            separation = distance * 360.0 * 3600.0
            if separation <= radius_arcsec and (best is None or separation < best[1]):
                best = (str(match['name']), separation)
    return best or (None, None)


def model_flux_density_jy(coefficients, frequency_ghz):
    """Flux density (Jy) of an ATCA band model at a frequency; coefficients end with 'log' when
    the polynomial is in log10(frequency / GHz) and gives log10(S)."""
    if not coefficients:
        return None
    is_log = str(coefficients[-1]).strip() == 'log'
    terms = [_to_float(c) for c in (coefficients[:-1] if is_log else coefficients)]
    if not terms or any(term is None for term in terms):
        return None
    x = math.log10(frequency_ghz) if is_log else frequency_ghz
    value = sum(term * x ** power for power, term in enumerate(terms))
    if is_log:
        if value > 10:  # nonsense model
            return None
        value = 10 ** value
    return value if value > 0 else None


class ATCADataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return ATCA_DEFAULT_RADIUS_ARCSEC

    name = 'ATCA'
    verbose_name = 'ATCA Calibrator Database (cm)'
    update_on_daily_refresh = True
    info_url = ATCA_PAGE_URL
    acknowledgement = ATCA_ACKNOWLEDGEMENT
    upsert_identity_keys = ('filter', 'atca_project')
    service_notes = (
        'Query the ATCA Calibrator Database by coordinates. If an ATCA calibrator lies within '
        '5 arcsec, all of its measurements (1990s onwards) are imported as flux densities at 2.1, 5.5, '
        '9.0, 17, 33 and 93 GHz, evaluated from the per-epoch band models (mJy, radio axis of the '
        'photometry plot). Radio data only; no aliases are added.'
    )

    @classmethod
    def get_form_class(cls):
        return ATCAQueryForm

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
            'include_radio': bool(parameters.get('include_radio', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or self.get_finding_chart_radius_arcsec()

        calibrator, separation, measurements = None, None, []
        if ra is not None and dec is not None and query_parameters.get('include_radio', True):
            try:
                calibrator, separation = _nearest_calibrator(ra, dec, radius_arcsec)
                if calibrator:
                    measurements = _api(action='source_all_details', source=calibrator).get('measurements') or []
                else:
                    logger.debug('No ATCA calibrator near RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('ATCA calibrator database query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                calibrator, measurements = None, []

        self.query_results = {
            'calibrator': calibrator,
            'separation_arcsec': separation,
            'measurements': measurements,
            'source_location': f'{ATCA_PAGE_URL}?source={calibrator}' if calibrator else ATCA_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        calibrator = data.get('calibrator')
        if data.get('ra') is None or data.get('dec') is None or not calibrator:
            return []
        datums = self._build_radio_datums(data.get('measurements') or [], calibrator, data.get('separation_arcsec'))
        if not datums:
            return []
        return [{
            'name': calibrator,
            'ra': data['ra'],
            'dec': data['dec'],
            'aliases': [],
            'reduced_datums': {'radio': datums},
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
        if data_type != 'radio' or not data:
            return 0
        created, _updated = upsert_reduced_datums(
            target=target,
            data_type='radio',
            source_name=self.name,
            source_location=kwargs.get('source_location') or self.info_url,
            datums=data,
            identity_keys=self.upsert_identity_keys,
        )
        return created

    def to_reduced_datums(self, target, data_results=None, **kwargs):
        if not data_results:
            return
        for data_type, data in data_results.items():
            self.create_reduced_datums_from_query(
                target,
                data=data,
                data_type=data_type,
                source_location=(getattr(self, 'query_results', {}) or {}).get('source_location'),
            )

    def _build_radio_datums(self, measurements, calibrator, separation):
        output = []
        for measurement in measurements:
            band = str(measurement.get('frequency_band') or '').strip()
            frequencies = ATCA_BAND_FREQUENCIES_GHZ.get(band)
            mjd = _to_float(measurement.get('epoch_start'))
            if not frequencies or mjd is None:
                continue
            timestamp = Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc)
            for fit in measurement.get('fluxdensities') or []:
                coefficients = fit.get('fluxdensity_fit_coeff')
                scatter_jy = _to_float(fit.get('fluxdensity_fit_scatter')) or 0.0
                for frequency in frequencies:
                    flux_jy = model_flux_density_jy(coefficients, frequency)
                    if flux_jy is None:
                        continue
                    error_jy = max(scatter_jy, ATCA_MIN_RELATIVE_ERROR * flux_jy)
                    output.append({
                        'timestamp': timestamp,
                        'value': {
                            'filter': f'ATCA({frequency:g}GHz)',
                            'flux': round(flux_jy * 1000.0, 3),
                            'error': round(error_jy * 1000.0, 3),
                            'flux_unit': ATCA_FLUX_UNIT,
                            'frequency_ghz': frequency,
                            'facility': 'ATCA',
                            'atca_band': band,
                            'atca_project': str(measurement.get('project_code') or '').strip() or None,
                            'atca_array': str(measurement.get('array') or '').split(' ')[0] or None,
                            'atca_calibrator': calibrator,
                            'match_separation_arcsec': round(separation, 3) if separation is not None else None,
                        },
                    })
        return output
