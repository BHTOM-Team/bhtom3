import logging
import math
from datetime import datetime, timezone

import requests
from astropy.coordinates import SkyCoord
from astropy.time import Time
import astropy.units as u

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import OGLEEWSQueryForm


logger = logging.getLogger(__name__)

OGLE_BASE_URL = 'https://www.astrouw.edu.pl/ogle'
OGLE_EWS_INFO_URL = f'{OGLE_BASE_URL}/ogle4/ews'


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


def _ogle_years():
    current_year = datetime.now(timezone.utc).year
    years = [1998, 1999, 2000]
    years.extend(range(2002, 2010))
    years.extend(range(2011, current_year + 1))
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


def _year_from_target_name(name):
    normalized_name = _normalize_target_name(name)
    if not normalized_name:
        return None
    year_text = normalized_name.split('-', 1)[0]
    try:
        return int(year_text)
    except (TypeError, ValueError):
        return None


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


def _parse_photometry_rows(text):
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
        rows.append({'hjd': hjd, 'mag': mag, 'magerr': magerr})
    return rows


class OGLEEWSDataService(DataService):
    name = 'OGLEEWS'
    verbose_name = 'OGLE EWS'
    update_on_daily_refresh = True
    info_url = OGLE_EWS_INFO_URL
    service_notes = 'Query OGLE Early Warning System by event name or cone search and ingest OGLE I-band photometry.'

    @classmethod
    def get_form_class(cls):
        return OGLEEWSQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        self.query_parameters = {
            'target_name': (parameters.get('target_name') or '').strip(),
            'ra': parameters.get('ra'),
            'dec': parameters.get('dec'),
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
        matching_rows = []
        if target_name:
            matching_rows = self._find_by_name(alert_rows, target_name)

        if not matching_rows and ra is not None and dec is not None:
            matching_rows = self._find_by_cone(alert_rows, ra, dec, radius_arcsec)

        photometry_by_name = {}
        photometry_urls = {}
        if query_parameters.get('include_photometry', True):
            for row in matching_rows:
                name = row.get('name')
                if not name:
                    continue
                phot_url = _ogle_phot_url(name)
                photometry_urls[name] = phot_url
                try:
                    photometry_by_name[name] = self._fetch_photometry_rows(phot_url)
                except Exception as exc:
                    logger.warning('OGLE EWS photometry unavailable for %s: %s', name, exc)

        self.query_results = {
            'alerts': matching_rows,
            'photometry_by_name': photometry_by_name,
            'photometry_urls': photometry_urls,
            'source_location': next(iter(photometry_urls.values()), self.info_url),
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
        photometry_urls = data.get('photometry_urls') or {}
        for alert in alerts:
            raw_name = alert.get('name')
            normalized_raw_name = _normalize_target_name(raw_name)
            name = _prefixed_target_name(raw_name)
            ra = _to_float(alert.get('ra'))
            dec = _to_float(alert.get('dec'))
            if not name or ra is None or dec is None:
                continue

            target_result = {
                'name': name,
                'ra': ra,
                'dec': dec,
                'aliases': [name, normalized_raw_name],
                'source_location': photometry_urls.get(normalized_raw_name) or self.info_url,
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
        rows = []
        year = _year_from_target_name(target_name)
        years = [year] if year else _ogle_years()
        for year in years:
            response = requests.get(_lenses_url(year), timeout=30)
            if response.status_code == 404:
                continue
            response.raise_for_status()
            rows.extend(_parse_lenses_rows(response.text))
        return rows

    def _fetch_photometry_rows(self, photometry_url):
        response = requests.get(photometry_url, timeout=30)
        response.raise_for_status()
        return _parse_photometry_rows(response.text)

    def _find_by_name(self, alert_rows, target_name):
        exact_matches = [row for row in alert_rows if _normalize_target_name(row.get('name')) == target_name]
        if exact_matches:
            return exact_matches
        return [row for row in alert_rows if target_name in _normalize_target_name(row.get('name'))]

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
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {'filter': 'OGLE(I)', 'magnitude': mag, 'error': magerr},
            })
        return output
