import logging
from datetime import timedelta, timezone
from urllib.parse import urlencode

from astropy.time import Time
from django.conf import settings
from django.utils import timezone as django_timezone
import requests

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import FRAMQueryForm
from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT


logger = logging.getLogger(__name__)

FRAM_ARCHIVE_URL = 'http://fram.fzu.cz/archive/'
FRAM_PHOTOMETRY_SEARCH_URL = f'{FRAM_ARCHIVE_URL}search/photometry/'
FRAM_PHOTOMETRY_MJD_URL = f'{FRAM_ARCHIVE_URL}photometry/mjd'
FRAM_QUERY_CADENCE_DAYS = 3
FRAM_FIRST_QUERY_NIGHT = '19000101'


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _night_string(moment):
    return moment.strftime('%Y%m%d')


def _fram_alias(target_name):
    target_name = str(target_name or '').strip()
    return f'FRAM_{target_name}' if target_name else ''


def _build_fram_download_url(ra, dec, radius_arcsec, night1, night2):
    sr_degrees = float(radius_arcsec) / 3600.0
    params = {
        'night1': night1,
        'night2': night2,
        'coords': f'{ra} {dec}',
        'name': 'degrees',
        'ra': ra,
        'dec': dec,
        'sr': sr_degrees,
    }
    return f'{FRAM_PHOTOMETRY_MJD_URL}?{urlencode(params)}'


def _parse_mjd_photometry(text):
    rows = []
    for line in str(text or '').splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        parts = line.split()
        if len(parts) < 4:
            continue
        mjd = _to_float(parts[0])
        magnitude = _to_float(parts[1])
        error = _to_float(parts[2])
        filter_name = str(parts[3] or '').strip()
        if mjd is None or magnitude is None or error is None or not filter_name:
            continue
        rows.append({
            'mjd': mjd,
            'magnitude': magnitude,
            'error': error,
            'filter': filter_name,
        })
    return rows


class FRAMDataService(DataService):
    name = 'FRAM'
    verbose_name = 'FRAM'
    update_on_daily_refresh = True
    info_url = FRAM_PHOTOMETRY_SEARCH_URL
    service_notes = (
        'Query FRAM Archive photometry by coordinates and ingest cleaned FRAM light curves. '
        'The archive is maintained by the FRAM team at FZU, Institute of Physics of the Czech Academy of Sciences. '
        'When using these data, cite the FRAM Archive and the relevant FRAM instrument papers, e.g. '
        'Pierre Auger Collaboration, "The FRAM robotic telescope for atmospheric monitoring at the Pierre Auger Observatory" '
        '(arXiv:2101.11602), and acknowledge the FRAM/FZU archive.'
    )

    @classmethod
    def get_form_class(cls):
        return FRAMQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_name, ra, dec = resolve_query_coordinates(parameters)
        radius_arcsec = _to_float(parameters.get('radius_arcsec')) or 3.0
        now = django_timezone.now()
        night2 = parameters.get('night2') or _night_string(now)
        night1 = parameters.get('night1')

        target_id = parameters.get('target_id')
        force = bool(parameters.get('force'))
        if not night1:
            has_existing_fram_data = False
            if target_id and not force:
                has_existing_fram_data = ReducedDatum.objects.filter(
                    target_id=target_id,
                    source_name=self.name,
                    data_type='photometry',
                ).exists()
            if has_existing_fram_data:
                night1 = _night_string(now - timedelta(days=FRAM_QUERY_CADENCE_DAYS))
            else:
                night1 = FRAM_FIRST_QUERY_NIGHT

        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': radius_arcsec,
            'include_photometry': bool(parameters.get('include_photometry', True)),
            'night1': str(night1),
            'night2': str(night2),
            'site': 'all',
            'ccd': 'all',
            'serial': 'all',
            'filter': 'all',
        }
        if target_id:
            self.query_parameters['target_id'] = target_id
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or 3.0
        night1 = str(query_parameters.get('night1') or FRAM_FIRST_QUERY_NIGHT)
        night2 = str(query_parameters.get('night2') or _night_string(django_timezone.now()))

        if ra is None or dec is None or not query_parameters.get('include_photometry', True):
            self.query_results = {'photometry_rows': [], 'source_location': None, 'ra': ra, 'dec': dec}
            return self.query_results

        source_location = _build_fram_download_url(ra, dec, radius_arcsec, night1, night2)
        username = getattr(settings, 'FRAM_ARCHIVE_USERNAME', 'guest')
        password = getattr(settings, 'FRAM_ARCHIVE_PASSWORD', 'framarchive')
        response = requests.get(
            source_location,
            auth=(username, password),
            timeout=getattr(settings, 'FRAM_ARCHIVE_TIMEOUT', DATA_SERVICE_HTTP_TIMEOUT),
        )
        response.raise_for_status()
        rows = _parse_mjd_photometry(response.text)

        self.query_results = {
            'photometry_rows': rows,
            'source_location': source_location,
            'ra': ra,
            'dec': dec,
            'night1': night1,
            'night2': night2,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        rows = data.get('photometry_rows') or []
        ra = data.get('ra')
        dec = data.get('dec')
        if ra is None or dec is None or not rows:
            return []

        target_name = str(query_parameters.get('target_name') or '').strip()
        alias = _fram_alias(target_name)
        aliases = []
        if alias:
            aliases.append({
                'name': alias,
                'url': data.get('source_location') or self.info_url,
                'source_name': self.name,
            })

        return [{
            'name': None,
            'ra': ra,
            'dec': dec,
            'aliases': aliases,
            'reduced_datums': {'photometry': self._build_photometry_datums(rows)},
            'source_location': data.get('source_location') or self.info_url,
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
        aliases = []
        for alias in alias_results:
            alias_name = alias.get('name') if isinstance(alias, dict) else alias
            alias_name = str(alias_name or '').strip()
            if alias_name:
                aliases.append(TargetName(name=alias_name))
        return aliases

    def create_reduced_datums_from_query(self, target, data=None, data_type=None, **kwargs):
        if data_type != 'photometry' or not data:
            return
        source_location = kwargs.get('source_location') or self.info_url
        for datum in data:
            ReducedDatum.objects.get_or_create(
                target=target,
                data_type='photometry',
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

    def _build_photometry_datums(self, rows):
        output = []
        for row in rows:
            output.append({
                'timestamp': Time(row['mjd'], format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {
                    'filter': f"FRAM({row['filter']})",
                    'magnitude': row['magnitude'],
                    'error': row['error'],
                    'facility': 'FRAM',
                    'archive': 'FRAM Archive',
                },
            })
        return output
