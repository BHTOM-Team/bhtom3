import logging
import math
import re
from datetime import timezone
from html import unescape
from urllib.parse import quote_plus, urljoin

import requests
from astropy.coordinates import SkyCoord
from astropy.time import Time
import astropy.units as u

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import OGLEOCVSQueryForm
from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT


logger = logging.getLogger(__name__)

OGLE_OCVS_BASE_URL = 'https://ogledb.astrouw.edu.pl/~ogle/OCVS/'


def _to_float(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _normalize_target_name(value):
    name = str(value or '').strip().upper().replace('_', '-')
    name = re.sub(r'\s+', '-', name)
    if name and not name.startswith('OGLE-'):
        name = f'OGLE-{name}'
    return name


def _object_url(name):
    normalized_name = _normalize_target_name(name)
    return f'{OGLE_OCVS_BASE_URL}?{quote_plus(normalized_name)}' if normalized_name else OGLE_OCVS_BASE_URL


def _coordinates_url(name):
    normalized_name = _normalize_target_name(name)
    return f'{OGLE_OCVS_BASE_URL}radec.php?{quote_plus(normalized_name)}&decimal=1'


def _nearby_url(ra, dec, radius_arcsec):
    coordinates = SkyCoord(ra=float(ra) * u.deg, dec=float(dec) * u.deg)
    ra_text = coordinates.ra.to_string(unit=u.hour, sep=':', precision=6, pad=True)
    dec_text = coordinates.dec.to_string(unit=u.deg, sep=':', precision=5, pad=True, alwayssign=True)
    return (
        f'{OGLE_OCVS_BASE_URL}nearby.php?ra={quote_plus(ra_text)}'
        f'&dec={quote_plus(dec_text)}&arcsec={float(radius_arcsec):g}'
    )


def _parse_coordinate_rows(text):
    rows = []
    for raw_line in str(text or '').splitlines():
        parts = raw_line.strip().split()
        if len(parts) < 3:
            continue
        name = _normalize_target_name(parts[0])
        ra = _to_float(parts[1])
        dec = _to_float(parts[2])
        if name and ra is not None and dec is not None:
            rows.append({'name': name, 'ra': ra, 'dec': dec})
    return rows


def _parse_nearby_names(text):
    names = []
    for raw_line in str(text or '').splitlines():
        parts = raw_line.strip().split()
        if not parts:
            continue
        name = _normalize_target_name(parts[0])
        if name and name not in names:
            names.append(name)
    return names


def _parse_photometry_rows(text):
    rows = []
    for raw_line in str(text or '').splitlines():
        parts = raw_line.strip().split()
        if len(parts) < 3 or raw_line.lstrip().startswith('#'):
            continue
        reduced_jd = _to_float(parts[0])
        magnitude = _to_float(parts[1])
        error = _to_float(parts[2])
        if reduced_jd is None or magnitude is None or error is None:
            continue
        rows.append({'hjd': reduced_jd + 2450000.0, 'mag': magnitude, 'magerr': error})
    return rows


def _extract_page_links(html_text, object_name, page_url):
    links = {'I': None, 'V': None, 'cvs': None}
    normalized_name = _normalize_target_name(object_name)
    for href in re.findall(r'<a\b[^>]*\bhref\s*=\s*["\']([^"\']+)["\']', str(html_text or ''), re.IGNORECASE):
        href = unescape(href.strip())
        absolute_url = urljoin(page_url, href)
        if re.search(r'\.\./CVS/o\.php\?', href, re.IGNORECASE):
            links['cvs'] = absolute_url
        elif href.endswith(f'{normalized_name}.dat'):
            if '/data/I/' in absolute_url:
                links['I'] = absolute_url
            elif '/data/V/' in absolute_url:
                links['V'] = absolute_url
    return links


class OGLEOCVSDataService(DataService):
    name = 'OGLEOCVS'
    verbose_name = 'OGLE OCVS'
    update_on_daily_refresh = False
    info_url = OGLE_OCVS_BASE_URL
    service_notes = (
        'Query the OGLE Collection of Variable Stars by object name or cone search and ingest I/V photometry.'
    )

    @classmethod
    def get_form_class(cls):
        return OGLEOCVSQueryForm

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

        matches = []
        if target_name:
            try:
                matches = self._find_by_name(target_name)
            except requests.RequestException:
                if ra is None or dec is None:
                    raise
                logger.info('OGLE OCVS name lookup failed for %s; trying coordinate lookup.', target_name)
        if not matches and ra is not None and dec is not None:
            matches = self._find_by_cone(ra, dec, radius_arcsec)

        photometry_by_name = {}
        page_urls = {}
        if query_parameters.get('include_photometry', True):
            for match in matches:
                name = match['name']
                page_urls[name] = _object_url(name)
                try:
                    photometry_by_name[name] = self._fetch_object_photometry(name)
                except Exception as exc:
                    logger.warning('OGLE OCVS photometry unavailable for %s: %s', name, exc)

        self.query_results = {
            'objects': matches,
            'photometry_by_name': photometry_by_name,
            'page_urls': page_urls,
            'source_location': next(iter(page_urls.values()), self.info_url),
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        results = []
        for match in data.get('objects') or []:
            name = _normalize_target_name(match.get('name'))
            ra = _to_float(match.get('ra'))
            dec = _to_float(match.get('dec'))
            if not name or ra is None or dec is None:
                continue
            result = {
                'name': name,
                'ra': ra,
                'dec': dec,
                'aliases': [name],
                'source_location': data.get('page_urls', {}).get(name) or _object_url(name),
            }
            rows_by_filter = data.get('photometry_by_name', {}).get(name)
            if rows_by_filter is not None:
                result['reduced_datums'] = {'photometry': self._build_photometry_datums(rows_by_filter)}
            results.append(result)
        return results

    def create_target_from_query(self, target_result, **kwargs):
        return Target(
            name=target_result['name'], type='SIDEREAL', ra=target_result.get('ra'),
            dec=target_result.get('dec'), epoch=2000.0,
        )

    def create_aliases_from_query(self, alias_results, **kwargs):
        return [TargetName(name=name) for name in dict.fromkeys(alias_results) if name]

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
                defaults={'source_name': self.name, 'source_location': source_location},
            )

    def to_reduced_datums(self, target, data_results=None, **kwargs):
        if data_results:
            for data_type, data in data_results.items():
                self.create_reduced_datums_from_query(
                    target, data=data, data_type=data_type,
                    source_location=self.query_results.get('source_location') or self.info_url,
                )

    def _request_text(self, url):
        response = requests.get(url, timeout=DATA_SERVICE_HTTP_TIMEOUT)
        response.raise_for_status()
        return response.text

    def _find_by_name(self, target_name):
        if not target_name:
            return []
        return _parse_coordinate_rows(self._request_text(_coordinates_url(target_name)))

    def _find_by_cone(self, ra, dec, radius_arcsec):
        names = _parse_nearby_names(self._request_text(_nearby_url(ra, dec, radius_arcsec)))
        matches = []
        for name in names:
            matches.extend(self._find_by_name(name))
        return matches

    def _fetch_object_photometry(self, object_name):
        page_url = _object_url(object_name)
        page_html = self._request_text(page_url)
        links = _extract_page_links(page_html, object_name, page_url)
        rows_by_filter = {'I': [], 'V': []}
        for band in ('I', 'V'):
            if links[band]:
                try:
                    rows_by_filter[band].extend(_parse_photometry_rows(self._request_text(links[band])))
                except requests.RequestException as exc:
                    logger.warning('OGLE OCVS %s-band download failed for %s: %s', band, object_name, exc)

        if links['cvs']:
            try:
                cvs_html = self._request_text(links['cvs'])
                cvs_links = _extract_page_links(cvs_html, object_name, links['cvs'])
                for band in ('I', 'V'):
                    if not cvs_links[band]:
                        continue
                    try:
                        rows_by_filter[band].extend(_parse_photometry_rows(self._request_text(cvs_links[band])))
                    except requests.RequestException as exc:
                        logger.warning(
                            'OGLE OCVS legacy %s-band download failed for %s: %s', band, object_name, exc,
                        )
            except requests.RequestException as exc:
                logger.warning('OGLE OCVS legacy page unavailable for %s: %s', object_name, exc)
        return rows_by_filter

    @staticmethod
    def _build_photometry_datums(rows_by_filter):
        datums = []
        for band in ('I', 'V'):
            for row in rows_by_filter.get(band) or []:
                hjd = _to_float(row.get('hjd'))
                mag = _to_float(row.get('mag'))
                magerr = _to_float(row.get('magerr'))
                if hjd is None or mag is None or magerr is None or magerr > 9:
                    continue
                mjd = hjd - 2400000.5
                datums.append({
                    'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                    'value': {'filter': f'OGLE({band})', 'magnitude': mag, 'error': magerr},
                })
        return datums
