import csv
import gzip
import io
import logging
import math
import re
from datetime import timezone
from html import unescape
from urllib.parse import urljoin

import requests
from astropy.coordinates import SkyCoord
from astropy.time import Time
import astropy.units as u

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import MOAQueryForm


logger = logging.getLogger(__name__)

MOA_ARCHIVE_BASE_URL = 'https://moaprime.massey.ac.nz/moaarchive'
MOA_ARCHIVE_EVENT_BASE_URL = f'{MOA_ARCHIVE_BASE_URL}/event/'
MOA_ARCHIVE_PHOT_BASE_URL = f'{MOA_ARCHIVE_EVENT_BASE_URL}phot/'
MOA_ALERT_BASE_URL = 'https://moaprime.massey.ac.nz/alerts'
MOA_ALERT_DISPLAY_BASE_URL = f'{MOA_ALERT_BASE_URL}/display/'
MOA_CATALOG_URL = (
    'https://raw.githubusercontent.com/mauritzwicker/queryMOAmicrolensing/main/moa_fullEvents.csv'
)
EVENT_NAME_RE = re.compile(r'^(?:MOA-)?(?P<year>\d{4})-BLG-(?P<number>\d{1,4})$', re.IGNORECASE)

REQUEST_TIMEOUT = 45
REQUEST_VERIFY = False


requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]


def _to_float(value):
    try:
        number = float(value)
        if math.isnan(number) or not math.isfinite(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def _clean_html_text(value):
    text = re.sub(r'<sub>\s*([^<]+?)\s*</sub>', r'\1', str(value or ''), flags=re.IGNORECASE)
    text = re.sub(r'<sup>\s*([^<]+?)\s*</sup>', r'^\1', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def _parse_year(value):
    match = EVENT_NAME_RE.match(str(value or '').strip())
    return int(match.group('year')) if match else None


def _normalize_event_name(value):
    event_name = str(value or '').strip().upper().replace('_', '-')
    event_name = re.sub(r'\s+', '', event_name)
    match = EVENT_NAME_RE.match(event_name)
    if not match:
        return event_name
    return f"MOA-{match.group('year')}-BLG-{int(match.group('number')):04d}"


def _event_suffix(normalized_name, width=None):
    match = EVENT_NAME_RE.match(_normalize_event_name(normalized_name))
    if not match:
        return _normalize_event_name(normalized_name).removeprefix('MOA-')
    number = int(match.group('number'))
    width = width or len(match.group('number'))
    return f"{match.group('year')}-BLG-{number:0{width}d}"


def _event_suffix_candidates(value):
    normalized_name = _normalize_event_name(value)
    year = _parse_year(normalized_name)
    if year is None:
        suffix = normalized_name.removeprefix('MOA-')
        return [suffix] if suffix else []
    widths = (3, 4) if year < 2025 else (4, 3)
    candidates = []
    for width in widths:
        suffix = _event_suffix(normalized_name, width=width)
        if suffix not in candidates:
            candidates.append(suffix)
    return candidates


def _parse_event_page(html_text):
    result = {'metadata': {}, 'micro': {}, 'calibration_equation': None, 'phot_href': None}

    metadata_match = re.search(r'<div id="metadata".*?</div>', html_text, flags=re.IGNORECASE | re.DOTALL)
    if metadata_match:
        for row_html in re.findall(r'<tr.*?>.*?</tr>', metadata_match.group(0), flags=re.IGNORECASE | re.DOTALL):
            cells = re.findall(r'<td.*?>(.*?)</td>', row_html, flags=re.IGNORECASE | re.DOTALL)
            if len(cells) >= 2:
                key = _clean_html_text(cells[0]).rstrip(':')
                value = _clean_html_text(cells[1])
                if key:
                    result['metadata'][key] = value

    micro_match = re.search(r'<div id="micro".*?</div>', html_text, flags=re.IGNORECASE | re.DOTALL)
    if micro_match:
        for row_html in re.findall(r'<tr.*?>.*?</tr>', micro_match.group(0), flags=re.IGNORECASE | re.DOTALL):
            cells = [_clean_html_text(cell) for cell in re.findall(r'<td.*?>(.*?)</td>', row_html, flags=re.IGNORECASE | re.DOTALL)]
            cells = [cell for cell in cells if cell]
            if len(cells) >= 3 and cells[1] == '=':
                key = cells[0].replace(' ', '')
                value = cells[2]
                result['micro'][key] = value

    calib_match = re.search(
        r'<div id="calib".*?<p>(.*?)(?:</p>|</div>|</html>)',
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if calib_match:
        result['calibration_equation'] = _clean_html_text(calib_match.group(1))

    phot_match = re.search(
        r'<a[^>]+href="([^"]*phot/[^"]*)"[^>]*>\s*Photometry data file',
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if phot_match:
        result['phot_href'] = phot_match.group(1)

    return result


def _extract_calibration(equation):
    cleaned = _clean_html_text(equation).replace('log 10', 'log10')
    band_token = ''
    band_match = re.match(r'^([A-Za-z]+)\s*=', cleaned)
    if band_match:
        band_token = band_match.group(1).upper()

    match = re.search(
        r'=\s*([+-]?\d+(?:\.\d+)?)\s*-\s*2\.5\s*log10?\s*\(\s*(?:delta|Δ)?\s*flux\s*\+\s*([+-]?\d+(?:\.\d+)?)\s*\)',
        cleaned,
        flags=re.IGNORECASE,
    )
    if not match:
        raise ValueError(f'Unsupported MOA calibration equation: {equation}')

    zeropoint = float(match.group(1))
    reference_flux = float(match.group(2))

    if band_token == 'I':
        band = 'Red'
    elif band_token in {'B', 'V'}:
        band = 'Blue'
    else:
        band = band_token.title() if band_token else 'Unknown'

    return {
        'equation': cleaned,
        'zeropoint': zeropoint,
        'reference_flux': reference_flux,
        'band': band,
    }


def _parse_photometry_rows(text):
    rows = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#'):
            continue
        parts = line.split()
        if len(parts) not in (4, 8):
            continue
        jd = _to_float(parts[0])
        dflux = _to_float(parts[1])
        dflux_err = _to_float(parts[2])
        if jd is None or dflux is None or dflux_err is None:
            continue
        # Some MOA files contain placeholder rows with JD=0 that cannot be
        # converted to real timestamps; skip them.
        if jd <= 2400000.0:
            continue
        rows.append({
            'jd': jd,
            'mjd': jd - 2400000.5,
            'dflux': dflux,
            'dflux_err': dflux_err,
        })
    return rows


class MOADataService(DataService):
    name = 'MOA'
    verbose_name = 'MOA'
    update_on_daily_refresh = False
    info_url = MOA_ARCHIVE_BASE_URL
    service_notes = (
        'Query MOA microlensing events by MOA name or cone search, and ingest calibrated MOA lightcurve photometry.'
    )

    @classmethod
    def get_form_class(cls):
        return MOAQueryForm

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
        event_pages_by_name = {}
        photometry_urls = {}
        page_urls = {}

        if query_parameters.get('include_photometry', True):
            for row in matches:
                normalized_name = _normalize_event_name(row.get('Event'))
                if not normalized_name:
                    continue
                try:
                    event_page = self._fetch_event_page(normalized_name)
                except Exception as exc:
                    logger.warning('MOA event-page lookup failed for %s: %s', normalized_name, exc)
                    continue
                event_pages_by_name[normalized_name] = event_page
                photometry_urls[normalized_name] = event_page.get('phot_url')
                page_urls[normalized_name] = event_page.get('page_url')
                try:
                    photometry_by_name[normalized_name] = self._fetch_calibrated_photometry(event_page)
                except Exception as exc:
                    logger.warning('MOA photometry unavailable for %s: %s', normalized_name, exc)

        self.query_results = {
            'events': matches,
            'event_pages_by_name': event_pages_by_name,
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
            bare_name = event_name.removeprefix('MOA-')
            target_result = {
                'name': event_name,
                'ra': ra,
                'dec': dec,
                'aliases': [event_name],
                'source_location': page_urls.get(event_name) or self.info_url,
            }
            if event_name in photometry_by_name:
                target_result['reduced_datums'] = {
                    'photometry': self._build_photometry_datums(photometry_by_name[event_name]),
                }
            target_results.append(target_result)
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
        aliases = []
        for alias in alias_results:
            alias_name = str(alias or '').strip()
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

    def _request(self, url):
        response = requests.get(url, timeout=REQUEST_TIMEOUT, verify=REQUEST_VERIFY)
        response.raise_for_status()
        return response

    def _fetch_catalog_rows(self):
        response = self._request(MOA_CATALOG_URL)
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
        search_bare = search_name.removeprefix('MOA-')

        exact_matches = []
        prefix_matches = []
        contains_matches = []
        for row in rows:
            event_name = _normalize_event_name(row.get('Event'))
            if not event_name:
                continue
            bare_name = event_name.removeprefix('MOA-')
            if event_name == search_name or bare_name == search_name or bare_name == search_bare:
                exact_matches.append(row)
                continue
            if event_name.startswith(search_name) or bare_name.startswith(search_bare):
                prefix_matches.append(row)
                continue
            if search_bare in bare_name or search_name in event_name:
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

    def _fetch_event_page(self, event_name):
        normalized_name = _normalize_event_name(event_name)
        last_error = None
        for suffix in _event_suffix_candidates(normalized_name):
            archive_url = urljoin(MOA_ARCHIVE_EVENT_BASE_URL, suffix)
            try:
                response = requests.get(archive_url, timeout=REQUEST_TIMEOUT, verify=REQUEST_VERIFY)
                if response.status_code == 404:
                    continue
                response.raise_for_status()
                parsed = _parse_event_page(response.text)
                phot_url = urljoin(archive_url, parsed.get('phot_href') or f'phot/{suffix}')
                parsed.update({
                    'event_name': normalized_name,
                    'page_url': archive_url,
                    'phot_url': phot_url,
                })
                return parsed
            except Exception as exc:
                last_error = exc

        alert_url = urljoin(MOA_ALERT_DISPLAY_BASE_URL, normalized_name)
        try:
            response = requests.get(alert_url, timeout=REQUEST_TIMEOUT, verify=REQUEST_VERIFY)
            if response.status_code != 404:
                response.raise_for_status()
                parsed = _parse_event_page(response.text)
                phot_url = urljoin(alert_url, parsed.get('phot_href') or '')
                parsed.update({
                    'event_name': normalized_name,
                    'page_url': alert_url,
                    'phot_url': phot_url or None,
                })
                return parsed
        except Exception as exc:
            last_error = exc

        raise requests.HTTPError(f'Unable to resolve a MOA page for {normalized_name}: {last_error}')

    def _fetch_calibrated_photometry(self, event_page):
        phot_url = event_page.get('phot_url')
        equation = event_page.get('calibration_equation')
        if not phot_url or not equation:
            raise ValueError(f'Missing MOA photometry URL or calibration equation for {event_page.get("event_name")}')

        response = self._request(phot_url)
        raw_bytes = response.content
        try:
            if raw_bytes[:2] == b'\x1f\x8b':
                phot_text = gzip.decompress(raw_bytes).decode('utf-8')
            else:
                phot_text = raw_bytes.decode('utf-8')
        except Exception:
            phot_text = response.text

        raw_rows = _parse_photometry_rows(phot_text)
        calibration = _extract_calibration(equation)
        band = calibration['band']
        reference_flux = calibration['reference_flux']
        zeropoint = calibration['zeropoint']
        event_name = str(event_page.get('event_name') or '').strip()

        if reference_flux == 0.0 and zeropoint == 0.0:
            logger.warning(
                'MOA data exists for %s but no flux calibration is provided.',
                event_name or 'unknown event',
            )
            return []

        calibrated_rows = []
        for row in raw_rows:
            flux = row['dflux'] + reference_flux
            if flux <= 0:
                continue
            magnitude = zeropoint - 2.5 * math.log10(flux)
            magnitude_error = (2.5 / math.log(10.0)) * (row['dflux_err'] / flux) if row['dflux_err'] >= 0 else None
            if magnitude_error is not None and magnitude_error > 1.0:
                continue
            calibrated_rows.append({
                'jd': row['jd'],
                'mjd': row['mjd'],
                'magnitude': magnitude,
                'error': magnitude_error,
                'filter': f'MOA({band})',
            })
        return calibrated_rows

    def _build_photometry_datums(self, rows):
        output = []
        for row in rows:
            mjd = _to_float(row.get('mjd'))
            magnitude = _to_float(row.get('magnitude'))
            magnitude_error = _to_float(row.get('error'))
            filter_name = str(row.get('filter') or '').strip()
            if mjd is None or magnitude is None or not filter_name:
                continue
            value = {
                'filter': filter_name,
                'magnitude': magnitude,
            }
            if magnitude_error is not None and magnitude_error > 0:
                value['error'] = magnitude_error
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': value,
            })
        return output
