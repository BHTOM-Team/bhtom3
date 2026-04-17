import csv
import io
import logging
import math
import re
import tarfile
from datetime import timezone
from urllib.parse import urljoin

import pandas as pd
import requests
from astropy.coordinates import SkyCoord
from astropy.time import Time
import astropy.units as u

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import KMTQueryForm


logger = logging.getLogger(__name__)

KMT_BASE_URL = 'https://kmtnet.kasi.re.kr/ulens/event/'
KMT_CATALOG_URL = 'https://raw.githubusercontent.com/mauritzwicker/queryKMTmicrolensing/main/kmt_fullEvents.csv'
KMT_NAME_RE = re.compile(r'^(?:KMT-)?(?P<year>\d{4})-BLG-(?P<number>\d{1,5})$', re.IGNORECASE)
KMT_SITE_MAP = {'KMTC': 'CTIO', 'KMTA': 'SSO', 'KMTS': 'SAAO'}
REQUEST_TIMEOUT = 45


def _to_float(value):
    try:
        number = float(value)
        if math.isnan(number) or not math.isfinite(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def _normalize_event_name(value):
    event_name = str(value or '').strip().upper().replace('_', '-')
    event_name = re.sub(r'\s+', '', event_name)
    match = KMT_NAME_RE.match(event_name)
    if not match:
        return event_name
    return f"KMT-{match.group('year')}-BLG-{int(match.group('number')):04d}"


def _parse_year(value):
    match = KMT_NAME_RE.match(str(value or '').strip())
    return int(match.group('year')) if match else None


def _event_id(event_name):
    normalized = _normalize_event_name(event_name)
    parts = normalized.split('-')
    if len(parts) != 4:
        return None
    return f'{parts[0][0]}{parts[2][0]}{parts[1][-2:]}{parts[3]}'


def _event_page_url(event_name):
    normalized = _normalize_event_name(event_name)
    year = _parse_year(normalized)
    if year is None:
        return None
    return f'{KMT_BASE_URL}{year}/view.php?event={normalized}'


def _event_tar_url(event_name):
    normalized = _normalize_event_name(event_name)
    year = _parse_year(normalized)
    event_id = _event_id(normalized)
    if year is None or not event_id:
        return None
    return f'{KMT_BASE_URL}{year}/data/{event_id}/pysis/pysis.tar.gz'


def _read_pysis_table(fileobj):
    header = None
    lines = []
    for raw_line in fileobj.read().decode('utf-8', errors='replace').splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith('#'):
            if header is None:
                header = line[1:].strip().split()
            continue
        lines.append(raw_line)
    if not header or not lines:
        return pd.DataFrame()
    return pd.read_csv(io.StringIO('\n'.join(lines)), delim_whitespace=True, names=header)


class KMTDataService(DataService):
    name = 'KMT'
    verbose_name = 'KMT'
    update_on_daily_refresh = False
    info_url = KMT_BASE_URL
    service_notes = 'Query KMTNet microlensing events by KMT name or cone search and ingest KMT I-band photometry.'

    @classmethod
    def get_form_class(cls):
        return KMTQueryForm

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
        target_name = _normalize_event_name(query_parameters.get('target_name'))
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or 5.0

        catalog_rows = self._fetch_catalog_rows()
        matches = []
        if target_name:
            matches = self._find_by_name(catalog_rows, target_name)

        if not matches and ra is not None and dec is not None:
            best_match = self._find_by_cone(catalog_rows, ra, dec, radius_arcsec)
            if best_match is not None:
                matches = [best_match]

        photometry_by_name = {}
        page_urls = {}
        tar_urls = {}
        if query_parameters.get('include_photometry', True):
            for row in matches:
                event_name = _normalize_event_name(row.get('Event'))
                if not event_name:
                    continue
                page_urls[event_name] = _event_page_url(event_name)
                tar_urls[event_name] = _event_tar_url(event_name)
                try:
                    photometry_by_name[event_name] = self._fetch_photometry_rows(event_name)
                except Exception as exc:
                    logger.warning('KMT photometry unavailable for %s: %s', event_name, exc)

        self.query_results = {
            'events': matches,
            'photometry_by_name': photometry_by_name,
            'page_urls': page_urls,
            'tar_urls': tar_urls,
            'source_location': next(iter(page_urls.values()), self.info_url),
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        matches = data.get('events') or []
        if not matches:
            return []

        photometry_by_name = data.get('photometry_by_name') or {}
        page_urls = data.get('page_urls') or {}
        target_results = []
        for row in matches:
            event_name = _normalize_event_name(row.get('Event'))
            ra = _to_float(row.get('ra_deg') or row.get('RA_deg') or row.get('ra'))
            dec = _to_float(row.get('dec_deg') or row.get('Dec_deg') or row.get('dec'))
            if not event_name or ra is None or dec is None:
                continue
            result = {
                'name': event_name,
                'ra': ra,
                'dec': dec,
                'aliases': [event_name],
                'source_location': page_urls.get(event_name) or self.info_url,
            }
            if event_name in photometry_by_name:
                result['reduced_datums'] = {'photometry': self._build_photometry_datums(photometry_by_name[event_name])}
            target_results.append(result)
        return target_results

    def create_target_from_query(self, target_result, **kwargs):
        return Target(
            name=target_result['name'],
            type='SIDEREAL',
            ra=target_result.get('ra'),
            dec=target_result.get('dec'),
            epoch=2000.0,
        )

    def create_aliases_from_query(self, alias_results, **kwargs):
        return [TargetName(name=str(alias).strip()) for alias in alias_results if str(alias).strip()]

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

    def _fetch_catalog_rows(self):
        response = requests.get(KMT_CATALOG_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        reader = csv.DictReader(io.StringIO(response.text))
        rows = []
        for row in reader:
            normalized = dict(row)
            normalized['Event'] = _normalize_event_name(row.get('Event'))
            rows.append(normalized)
        return rows

    def _find_by_name(self, rows, target_name):
        search_name = _normalize_event_name(target_name)
        if not search_name:
            return []
        exact_matches = []
        prefix_matches = []
        contains_matches = []
        for row in rows:
            event_name = _normalize_event_name(row.get('Event'))
            if not event_name:
                continue
            if event_name == search_name:
                exact_matches.append(row)
                continue
            if event_name.startswith(search_name):
                prefix_matches.append(row)
                continue
            if search_name in event_name:
                contains_matches.append(row)
        return exact_matches or prefix_matches or contains_matches

    def _find_by_cone(self, rows, ra_deg, dec_deg, radius_arcsec):
        center = SkyCoord(ra=ra_deg * u.deg, dec=dec_deg * u.deg)
        best_row = None
        best_sep = None
        for row in rows:
            row_ra = _to_float(row.get('ra_deg') or row.get('RA_deg'))
            row_dec = _to_float(row.get('dec_deg') or row.get('Dec_deg'))
            if row_ra is None or row_dec is None:
                continue
            candidate = SkyCoord(ra=row_ra * u.deg, dec=row_dec * u.deg)
            separation = center.separation(candidate).arcsecond
            if separation <= radius_arcsec and (best_sep is None or separation < best_sep):
                best_row = row
                best_sep = separation
        return best_row

    def _fetch_photometry_rows(self, event_name):
        tar_url = _event_tar_url(event_name)
        if not tar_url:
            raise ValueError(f'Could not determine KMT tarball URL for {event_name}')
        response = requests.get(tar_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        rows = []
        with tarfile.open(fileobj=io.BytesIO(response.content), mode='r:gz') as tar:
            for member in tar.getmembers():
                member_name = member.name.rsplit('/', 1)[-1]
                if not member_name.endswith('.pysis'):
                    continue
                if '_I.' not in member_name:
                    continue
                fileobj = tar.extractfile(member)
                if fileobj is None:
                    continue
                df = _read_pysis_table(fileobj)
                if df.empty or 'HJD' not in df.columns or 'mag' not in df.columns or 'mag_err' not in df.columns:
                    continue
                site_code = member_name.split('_', 1)[0]
                site_label = KMT_SITE_MAP.get(site_code, site_code)
                facility = f'{site_label}_{site_code}'
                df = df[df['mag_err'] > 0].copy()
                for _, row in df.iterrows():
                    hjd = _to_float(row.get('HJD'))
                    magnitude = _to_float(row.get('mag'))
                    error = _to_float(row.get('mag_err'))
                    if hjd is None or magnitude is None or error is None:
                        continue
                    rows.append({
                        'hjd': hjd + 2450000.0,
                        'magnitude': magnitude,
                        'error': error,
                        'facility': facility,
                        'filter': 'KMT(I)',
                    })
        return rows

    def _build_photometry_datums(self, rows):
        output = []
        for row in rows:
            hjd = _to_float(row.get('hjd'))
            magnitude = _to_float(row.get('magnitude'))
            error = _to_float(row.get('error'))
            filter_name = str(row.get('filter') or '').strip()
            facility = str(row.get('facility') or '').strip()
            if hjd is None or magnitude is None or error is None or not filter_name:
                continue
            value = {'filter': filter_name, 'magnitude': magnitude, 'error': error}
            if facility:
                value['facility'] = facility
            output.append({
                'timestamp': Time(hjd, format='jd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': value,
            })
        return output
