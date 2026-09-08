import logging
import math
import re
from datetime import datetime, timezone

import requests
from astropy.coordinates import SkyCoord
from astropy.time import Time
import astropy.units as u

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import OGLEEWSQueryForm
from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT


logger = logging.getLogger(__name__)

OGLE_BASE_URL = 'https://www.astrouw.edu.pl/ogle'
OGLE_EWS_INFO_URL = f'{OGLE_BASE_URL}/ogle4/ews'
OGLE_FTP_BASE_URL = 'https://ftp.astrouw.edu.pl/ogle'

OGLE_ARCHIVE_SOURCES = (
    {
        'key': 'ogle2_bulge',
        'kind': 'ogle2_bulge',
        'catalog_url': f'{OGLE_FTP_BASE_URL}/ogle2/microlensing/gb/table2.dat',
        'photometry_base_url': f'{OGLE_FTP_BASE_URL}/ogle2/microlensing/gb/phot/',
        'info_url': f'{OGLE_FTP_BASE_URL}/ogle2/microlensing/gb/',
    },
    {
        'key': 'ogle3_bulge_a',
        'kind': 'ogle3_bulge',
        'catalog_url': f'{OGLE_FTP_BASE_URL}/ogle3/blg_tau/catalogA_basic.table',
        'photometry_base_url': f'{OGLE_FTP_BASE_URL}/ogle3/blg_tau/PHOT/CLASSA-OPTIMIZED/',
        'photometry_prefix': 'phot',
        'info_url': f'{OGLE_FTP_BASE_URL}/ogle3/blg_tau/',
    },
    {
        'key': 'ogle3_bulge_b',
        'kind': 'ogle3_bulge',
        'catalog_url': f'{OGLE_FTP_BASE_URL}/ogle3/blg_tau/catalogB.table',
        'photometry_base_url': f'{OGLE_FTP_BASE_URL}/ogle3/blg_tau/PHOT/CLASSB/',
        'photometry_prefix': 'star',
        'info_url': f'{OGLE_FTP_BASE_URL}/ogle3/blg_tau/',
    },
    {
        'key': 'ogle4_bulge',
        'kind': 'ogle4_bulge',
        'catalog_url': f'{OGLE_FTP_BASE_URL}/ogle4/microlensing_maps/table3.dat',
        'photometry_base_url': f'{OGLE_FTP_BASE_URL}/ogle4/microlensing_maps/phot/',
        'info_url': f'{OGLE_FTP_BASE_URL}/ogle4/microlensing_maps/',
    },
    {
        'key': 'ogle4_disk',
        'kind': 'ogle4_disk',
        'catalog_url': f'{OGLE_FTP_BASE_URL}/ogle4/galactic_disk_microlensing/table_B1.txt',
        'photometry_base_url': f'{OGLE_FTP_BASE_URL}/ogle4/galactic_disk_microlensing/data/',
        'info_url': f'{OGLE_FTP_BASE_URL}/ogle4/galactic_disk_microlensing/',
    },
    {
        'key': 'ogle4_disk_candidates',
        'kind': 'ogle4_disk_candidates',
        'catalog_url': f'{OGLE_FTP_BASE_URL}/ogle4/galactic_disk_microlensing/table_B2.txt',
        'photometry_base_url': f'{OGLE_FTP_BASE_URL}/ogle4/galactic_disk_microlensing/data_c/',
        'info_url': f'{OGLE_FTP_BASE_URL}/ogle4/galactic_disk_microlensing/',
    },
    {
        'key': 'ogle_lmc',
        'kind': 'magellanic',
        'catalog_url': f'{OGLE_FTP_BASE_URL}/ogle4/LMC_OPTICAL_DEPTH/table5.txt',
        'photometry_base_url': f'{OGLE_FTP_BASE_URL}/ogle4/LMC_OPTICAL_DEPTH/phot/',
        'info_url': f'{OGLE_FTP_BASE_URL}/ogle4/LMC_OPTICAL_DEPTH/',
    },
    {
        'key': 'ogle_smc',
        'kind': 'magellanic',
        'catalog_url': f'{OGLE_FTP_BASE_URL}/ogle4/SMC_OPTICAL_DEPTH/table4.txt',
        'photometry_base_url': f'{OGLE_FTP_BASE_URL}/ogle4/SMC_OPTICAL_DEPTH/phot/',
        'info_url': f'{OGLE_FTP_BASE_URL}/ogle4/SMC_OPTICAL_DEPTH/',
    },
)

OGLE_LEGACY_MAGELLANIC_EVENTS = (
    {
        'name': 'LMC-01', 'ra_text': '05:16:53.26', 'dec_text': '-69:16:30.1',
        'aliases': ['OGLE-1999-LMC-01'],
        'source_location': f'{OGLE_FTP_BASE_URL}/ogle2/lmc_tau/',
        'photometry': [
            {
                'url': f'{OGLE_FTP_BASE_URL}/ogle2/lmc_tau/eventOGLE-LMC-01_ogle2_ogle3.dat',
                'band': 'I', 'hjd_offset': 2450000.0,
            },
            {
                'url': f'{OGLE_FTP_BASE_URL}/ogle2/lmc_tau/eventOGLE-LMC-01_ogle2_V.dat',
                'band': 'V', 'hjd_offset': 2450000.0,
            },
        ],
    },
    {
        'name': 'LMC-02', 'ra_text': '05:30:48.00', 'dec_text': '-69:54:33.6',
        'aliases': [],
        'source_location': f'{OGLE_FTP_BASE_URL}/ogle2/lmc_tau/',
        'photometry': [
            {
                'url': f'{OGLE_FTP_BASE_URL}/ogle2/lmc_tau/eventOGLE-LMC-02_ogle2_ogle3.dat',
                'band': 'I', 'hjd_offset': 2450000.0,
            },
            {
                'url': f'{OGLE_FTP_BASE_URL}/ogle2/lmc_tau/eventOGLE-LMC-02_ogle2_V.dat',
                'band': 'V', 'hjd_offset': 2450000.0,
            },
        ],
    },
    {
        'name': 'LMC-03', 'ra_text': '05:07:03.63', 'dec_text': '-71:17:06.3',
        'aliases': ['OGLE-2007-LMC-01'],
        'source_location': f'{OGLE_FTP_BASE_URL}/ogle3/lmc_tau/',
        'photometry': [
            {
                'url': f'{OGLE_FTP_BASE_URL}/ogle3/lmc_tau/eventOGLE-LMC-03.ogle3.I.dat',
                'band': 'I', 'hjd_offset': 2450000.0,
            },
            {
                'url': f'{OGLE_FTP_BASE_URL}/ogle3/lmc_tau/eventOGLE-LMC-03.ogle3.V.dat',
                'band': 'V', 'hjd_offset': 2450000.0,
            },
        ],
    },
    {
        'name': 'SMC-01', 'ra_text': '00:56:45.89', 'dec_text': '-72:37:19.8',
        'aliases': [],
        'source_location': f'{OGLE_FTP_BASE_URL}/ogle2/smc_tau/',
        'photometry': [
            {'url': f'{OGLE_FTP_BASE_URL}/ogle2/smc_tau/ogle-smc-01_Iband.dat', 'band': 'I', 'hjd_offset': 2450000.0},
            {'url': f'{OGLE_FTP_BASE_URL}/ogle2/smc_tau/ogle3.I.dat', 'band': 'I', 'hjd_offset': 2450000.0},
            {'url': f'{OGLE_FTP_BASE_URL}/ogle2/smc_tau/ogle3.V.dat', 'band': 'V', 'hjd_offset': 2450000.0},
        ],
    },
    {
        'name': 'LMC-20', 'ra_text': '05:12:57.62', 'dec_text': '-69:25:21.0',
        'aliases': [],
        'source_location': f'{OGLE_FTP_BASE_URL}/ogle4/LMC_FFP_PBH/',
        'photometry': [{
            'url': f'{OGLE_FTP_BASE_URL}/ogle4/LMC_FFP_PBH/phot/OGLE-LMC-20.dat',
            'band': 'I',
        }],
    },
)


def _to_float(value):
    try:
        number = float(value)
        if math.isnan(number) or not math.isfinite(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def _normalize_target_name(value):
    name = str(value or '').strip().upper()
    if name.startswith('OGLE-'):
        name = name[5:]
    elif name.startswith('OGLE '):
        name = name[5:]
    return name


def _prefixed_target_name(value):
    normalized_name = _normalize_target_name(value)
    if not normalized_name:
        return ''
    if normalized_name.startswith('OGLE3-ULENS-'):
        return normalized_name
    return f'OGLE-{normalized_name}'


def _ra_to_decimal(ra_value):
    hours, minutes, seconds = [float(part) for part in str(ra_value).split(':')]
    return 15.0 * (hours + minutes / 60.0 + seconds / 3600.0)


def _dec_to_decimal(dec_value):
    degrees, arcminutes, arcseconds = [float(part) for part in str(dec_value).split(':')]
    if degrees < 0:
        return degrees - arcminutes / 60.0 - arcseconds / 3600.0
    return degrees + arcminutes / 60.0 + arcseconds / 3600.0


def _ogle_version_for_year(year):
    if year < 2001:
        return 'ogle2'
    if year < 2010:
        return 'ogle3'
    return 'ogle4'


def _ogle_years(current_year=None):
    if current_year is None:
        current_year = datetime.now(timezone.utc).year
    years = [1998, 1999, 2000]
    years.extend(range(2002, 2010))
    years.extend(range(2011, current_year + 2))
    return years


def _lenses_url(year):
    version = _ogle_version_for_year(year)
    return f'{OGLE_BASE_URL}/{version}/ews/{year}/lenses.par'


def _ogle_phot_url(name):
    normalized_name = _normalize_target_name(name)
    year_text, field, number = normalized_name.split('-', 2)
    year = int(year_text)
    version = _ogle_version_for_year(year)
    return f'{OGLE_BASE_URL}/{version}/ews/{year}/{field.lower()}-{number}/phot.dat'


def _ogle_event_url(name):
    normalized_name = _normalize_target_name(name)
    if not normalized_name:
        return OGLE_EWS_INFO_URL
    year_text, field, number = normalized_name.split('-', 2)
    year = int(year_text)
    if year >= 2026:
        return f'{OGLE_BASE_URL}/ogle4/ews/{year}/{field.lower()}-{number}/'
    return f'{OGLE_EWS_INFO_URL}/{normalized_name}.html'


def _year_from_target_name(name):
    normalized_name = _normalize_target_name(name)
    if not normalized_name:
        return None
    transient_match = re.match(r'(?i)^(?:GAIA|ASASSN-?)(\d{2})', normalized_name)
    if transient_match:
        return 2000 + int(transient_match.group(1))
    year_text = normalized_name.split('-', 1)[0]
    try:
        return int(year_text)
    except (TypeError, ValueError):
        return None


def _is_archive_target_name(name):
    normalized_name = _normalize_target_name(name)
    return bool(re.match(
        r'(?i)^(?:OGLE3-ULENS-\d+|BUL_SC\d+\.\d+|GD\d+\.\d+\.\d+|(?:LMC|SMC)-\d+)$',
        normalized_name,
    ))


def _parse_lenses_rows(text):
    rows = []
    headers = None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#'):
            continue
        parts = line.split()
        if len(parts) < 5:
            continue
        if headers is None and parts[0].lower() == 'name':
            headers = parts
            continue
        if headers:
            row = dict(zip(headers, parts))
            name = row.get('name')
            field = row.get('field')
            starno = row.get('starno')
            ra_text = row.get('RA(J2000)') or row.get('ra')
            dec_text = row.get('Dec(J2000)') or row.get('dec')
        else:
            name, field, starno, ra_text, dec_text = parts[:5]

        ra = _to_float(ra_text)
        dec = _to_float(dec_text)
        if ra is None or dec is None:
            try:
                ra = _ra_to_decimal(ra_text)
                dec = _dec_to_decimal(dec_text)
            except (TypeError, ValueError):
                continue

        normalized_name = _normalize_target_name(name)
        rows.append({
            'name': normalized_name,
            'field': field,
            'starno': starno,
            'ra_text': ra_text,
            'dec_text': dec_text,
            'ra': ra,
            'dec': dec,
        })
    return rows


def _parse_photometry_rows(text, band='I', hjd_offset=0.0):
    rows = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#'):
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        hjd = _to_float(parts[0])
        mag = _to_float(parts[1])
        magerr = _to_float(parts[2])
        if hjd is None or mag is None or magerr is None:
            continue
        rows.append({'hjd': hjd + hjd_offset, 'mag': mag, 'magerr': magerr, 'band': band})
    return rows


def _archive_alias(value):
    alias = str(value or '').strip()
    if not alias or alias in {'-', 'X'}:
        return ''
    if alias.upper().startswith(('GAIA', 'ASASSN-', 'ASAS-SN')):
        return alias
    return _prefixed_target_name(alias)


def _parse_archive_catalog_rows(text, source):
    rows = []
    kind = source['kind']
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#'):
            continue
        parts = line.split()
        try:
            if kind == 'ogle2_bulge':
                if len(parts) < 5 or not parts[0].startswith('BUL_SC'):
                    continue
                field, starno, crossmatch, ra_text, dec_text = parts[:5]
                raw_name = f'{field}.{starno}'
                aliases = [_archive_alias(crossmatch)]
                field_number = field.removeprefix('BUL_SC')
                photometry = [{
                    'url': f"{source['photometry_base_url']}{field_number}.{starno}.dat",
                    'band': 'I',
                    'hjd_offset': 2450000.0,
                }]
            elif kind == 'ogle3_bulge':
                if len(parts) < 6:
                    continue
                number, ra_text, dec_text, field, starno, crossmatch = parts[:6]
                raw_name = f'OGLE3-ULENS-{int(number):04d}'
                aliases = [_archive_alias(item) for item in crossmatch.split(',')]
                photometry = [{
                    'url': (
                        f"{source['photometry_base_url']}"
                        f"{source['photometry_prefix']}{field}.I.{starno}.dat"
                    ),
                    'band': 'I',
                    'hjd_offset': 2450000.0,
                }]
            elif kind in {'ogle4_bulge', 'ogle4_disk'}:
                if len(parts) < 7:
                    continue
                raw_name, ra_text, dec_text = parts[0], parts[3], parts[4]
                aliases = [_archive_alias(parts[-1])]
                photometry = [{'url': f"{source['photometry_base_url']}{raw_name}.dat", 'band': 'I'}]
            elif kind == 'ogle4_disk_candidates':
                if len(parts) < 5:
                    continue
                raw_name, ra_text, dec_text = parts[0], parts[1], parts[2]
                aliases = [
                    item for item in parts[7:]
                    if item.upper().startswith(('GAIA', 'ASASSN-', 'ASAS-SN'))
                ]
                photometry = [{'url': f"{source['photometry_base_url']}{raw_name}.dat", 'band': 'I'}]
            elif kind == 'magellanic':
                if len(parts) < 3:
                    continue
                official_name, ra_text, dec_text = parts[:3]
                raw_name = _normalize_target_name(official_name)
                aliases = []
                photometry = [{'url': f"{source['photometry_base_url']}{official_name}.dat", 'band': 'I'}]
                event_number = int(raw_name.rsplit('-', 1)[-1])
                if raw_name.startswith('LMC-') and 3 <= event_number <= 6:
                    photometry.append({
                        'url': (
                            f'{OGLE_FTP_BASE_URL}/ogle3/lmc_tau/'
                            f'eventOGLE-LMC-{event_number:02d}.ogle3.V.dat'
                        ),
                        'band': 'V',
                        'hjd_offset': 2450000.0,
                    })
            else:
                continue

            ra = _ra_to_decimal(ra_text)
            dec = _dec_to_decimal(dec_text)
        except (TypeError, ValueError):
            continue

        rows.append({
            'name': raw_name,
            'ra_text': ra_text,
            'dec_text': dec_text,
            'ra': ra,
            'dec': dec,
            'aliases': [alias for alias in aliases if alias],
            'photometry': photometry,
            'source_location': source['info_url'],
            'catalog_source': source['key'],
        })
    return rows


def _legacy_magellanic_rows():
    rows = []
    for event in OGLE_LEGACY_MAGELLANIC_EVENTS:
        row = dict(event)
        row.update({
            'ra': _ra_to_decimal(event['ra_text']),
            'dec': _dec_to_decimal(event['dec_text']),
            'catalog_source': 'ogle_legacy_magellanic',
        })
        rows.append(row)
    return rows


class OGLEEWSDataService(DataService):
    name = 'OGLEEWS'
    verbose_name = 'OGLE EWS'
    update_on_daily_refresh = True
    info_url = OGLE_EWS_INFO_URL
    service_notes = (
        'Query OGLE EWS and published OGLE-II/III/IV bulge, Galactic-disk, LMC, and SMC microlensing catalogues '
        'by event name or cone search, and ingest available OGLE I/V photometry.'
    )

    @classmethod
    def get_form_class(cls):
        return OGLEEWSQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or 5.0,
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        target_name = _normalize_target_name(query_parameters.get('target_name'))
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or 5.0

        alert_rows = self._fetch_alert_rows(target_name=target_name)
        archive_rows = self._fetch_archive_catalog_rows()
        all_rows = alert_rows + archive_rows
        matching_rows = []
        if target_name:
            matching_rows = self._find_by_name(all_rows, target_name)

        if not matching_rows and ra is not None and dec is not None:
            matching_rows = self._find_by_cone(all_rows, ra, dec, radius_arcsec)

        photometry_by_name = {}
        photometry_urls = {}
        page_urls = {}
        if query_parameters.get('include_photometry', True):
            for row in matching_rows:
                name = row.get('name')
                if not name:
                    continue
                photometry_specs = row.get('photometry') or [{
                    'url': _ogle_phot_url(name),
                    'band': 'I',
                }]
                photometry_urls[name] = [spec['url'] for spec in photometry_specs]
                page_urls[name] = row.get('source_location') or _ogle_event_url(name)
                photometry_rows = []
                for spec in photometry_specs:
                    try:
                        photometry_rows.extend(self._fetch_photometry_rows(
                            spec['url'],
                            band=spec.get('band', 'I'),
                            hjd_offset=spec.get('hjd_offset', 0.0),
                        ))
                    except Exception as exc:
                        logger.warning(
                            'OGLE %s-band photometry unavailable for %s: %s',
                            spec.get('band', 'I'), name, exc,
                        )
                photometry_by_name[name] = photometry_rows

        self.query_results = {
            'alerts': matching_rows,
            'photometry_by_name': photometry_by_name,
            'photometry_urls': photometry_urls,
            'page_urls': page_urls,
            'source_location': next(iter(page_urls.values()), self.info_url),
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        alerts = data.get('alerts') or []
        if not alerts:
            return []

        target_results = []
        photometry_by_name = data.get('photometry_by_name') or {}
        page_urls = data.get('page_urls') or {}
        for alert in alerts:
            raw_name = alert.get('name')
            normalized_raw_name = _normalize_target_name(raw_name)
            name = _prefixed_target_name(raw_name)
            ra = _to_float(alert.get('ra'))
            dec = _to_float(alert.get('dec'))
            if not name or ra is None or dec is None:
                continue

            aliases = [name]
            aliases.extend(alias for alias in alert.get('aliases', []) if alias and alias not in aliases)
            target_result = {
                'name': name,
                'ra': ra,
                'dec': dec,
                'aliases': aliases,
                'source_location': page_urls.get(normalized_raw_name) or self.info_url,
            }
            photometry_rows = photometry_by_name.get(normalized_raw_name)
            if photometry_rows is not None:
                target_result['reduced_datums'] = {
                    'photometry': self._build_photometry_datums(photometry_rows),
                }
            target_results.append(target_result)

        return target_results

    def create_target_from_query(self, target_result, **kwargs):
        return Target(
            name=target_result['name'],
            type='SIDEREAL',
            ra=target_result.get('ra'),
            dec=target_result.get('dec'),
            epoch=target_result.get('epoch', 2000.0),
        )

    def create_aliases_from_query(self, alias_results, **kwargs):
        aliases = []
        seen = set()
        for alias in alias_results:
            if not alias or alias in seen:
                continue
            aliases.append(TargetName(name=alias))
            seen.add(alias)
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

    def _fetch_alert_rows(self, target_name=''):
        if _is_archive_target_name(target_name):
            return []
        rows = []
        year = _year_from_target_name(target_name)
        years = [year] if year else _ogle_years()
        for year in years:
            response = requests.get(_lenses_url(year), timeout=DATA_SERVICE_HTTP_TIMEOUT)
            if response.status_code == 404:
                continue
            response.raise_for_status()
            rows.extend(_parse_lenses_rows(response.text))
        return rows

    def _fetch_archive_catalog_rows(self):
        rows = _legacy_magellanic_rows()
        for source in OGLE_ARCHIVE_SOURCES:
            try:
                response = requests.get(source['catalog_url'], timeout=DATA_SERVICE_HTTP_TIMEOUT)
                if response.status_code == 404:
                    continue
                response.raise_for_status()
                rows.extend(_parse_archive_catalog_rows(response.text, source))
            except Exception as exc:
                logger.warning('OGLE archive catalogue %s unavailable: %s', source['key'], exc)
        return rows

    def _fetch_photometry_rows(self, photometry_url, band='I', hjd_offset=0.0):
        response = requests.get(photometry_url, timeout=DATA_SERVICE_HTTP_TIMEOUT)
        response.raise_for_status()
        return _parse_photometry_rows(response.text, band=band, hjd_offset=hjd_offset)

    def _find_by_name(self, alert_rows, target_name):
        def searchable_names(row):
            return [row.get('name')] + list(row.get('aliases') or [])

        exact_matches = [
            row for row in alert_rows
            if any(_normalize_target_name(name) == target_name for name in searchable_names(row))
        ]
        if exact_matches:
            return exact_matches
        return [
            row for row in alert_rows
            if any(target_name in _normalize_target_name(name) for name in searchable_names(row))
        ]

    def _find_by_cone(self, alert_rows, ra, dec, radius_arcsec):
        center = SkyCoord(ra=ra * u.deg, dec=dec * u.deg)
        matching_rows = []
        for row in alert_rows:
            row_ra = _to_float(row.get('ra'))
            row_dec = _to_float(row.get('dec'))
            if row_ra is None or row_dec is None:
                continue
            candidate = SkyCoord(ra=row_ra * u.deg, dec=row_dec * u.deg)
            if center.separation(candidate) <= radius_arcsec * u.arcsec:
                matching_rows.append(row)
        return matching_rows

    def _build_photometry_datums(self, rows):
        output = []
        for row in rows:
            hjd = _to_float(row.get('hjd'))
            mag = _to_float(row.get('mag'))
            magerr = _to_float(row.get('magerr'))
            if hjd is None or mag is None or magerr is None or magerr > 9:
                continue
            mjd = hjd - 2400000.5
            band = str(row.get('band') or 'I').strip().upper()
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {'filter': f'OGLE({band})', 'magnitude': mag, 'error': magerr},
            })
        return output
