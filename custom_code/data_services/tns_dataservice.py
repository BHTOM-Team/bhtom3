"""TNS adapter with bounded requests and photometry import support."""

import json
import math
import re
import time
from datetime import datetime, timezone as dt_timezone
from urllib.parse import quote

import requests
from astropy.time import Time
from tom_dataproducts.models import ReducedDatum
from tom_dataservices.data_services.tns import TNSDataService as BaseTNSDataService
from tom_targets.models import Target

from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT


def _to_float(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _nested_name(value):
    if isinstance(value, dict):
        return value.get('name') or value.get('group_name') or value.get('value')
    return value


def _first_float(mapping, *keys):
    for key in keys:
        value = _to_float(mapping.get(key))
        if value is not None:
            return value
    return None


def _iter_photometry(value):
    if isinstance(value, list):
        yield from value
    elif isinstance(value, dict):
        for key in ('photometry', 'photometry_group', 'items', 'data'):
            nested = value.get(key)
            if isinstance(nested, list):
                yield from nested
                return
        if any(key in value for key in ('jd', 'mjd', 'flux', 'limflux')):
            yield value


def _canonical_survey(photo, raw_filter):
    candidates = (
        _nested_name(photo.get('source_group')),
        _nested_name(photo.get('reporting_group')),
        photo.get('survey'),
        raw_filter,
    )
    joined = ' '.join(str(value) for value in candidates if value).upper()
    if 'GOTO' in joined:
        return 'GOTO'
    if 'ATLAS' in joined:
        return 'ATLAS'
    if 'ASAS-SN' in joined or 'ASASSN' in joined:
        return 'ASASSN'
    if 'ZTF' in joined:
        return 'ZTF'
    return None


def _tns_filter(photo):
    raw_filter = _nested_name(photo.get('filters') or photo.get('filter')) or 'unknown'
    survey = _canonical_survey(photo, raw_filter)
    band = str(raw_filter).strip()
    for suffix in ('GOTO', 'ATLAS', 'ASAS-SN', 'ASASSN', 'ZTF', 'Sloan'):
        band = re.sub(rf'(?i)(?:[-_ ]?{re.escape(suffix)})', '', band)
    band = band.strip('-_ ') or 'unknown'
    cousins_match = re.fullmatch(r'(?i)([RI])[-_ ]?Cousins', band)
    if cousins_match:
        band = f'{cousins_match.group(1).upper()}Cousins'
    band_aliases = {'cyan': 'c', 'orange': 'o', 'clear': 'C', 'l': 'L'}
    band = band_aliases.get(band.lower(), band)
    inner_filter = f'{survey}-{band}' if survey else band
    return f'TNS({inner_filter})', survey or 'TNS', str(raw_filter)


def _tns_timestamp(photo):
    jd = _to_float(photo.get('jd'))
    if jd is not None:
        return Time(jd, format='jd').to_datetime(timezone=dt_timezone.utc), jd
    mjd = _to_float(photo.get('mjd'))
    if mjd is not None:
        return Time(mjd, format='mjd').to_datetime(timezone=dt_timezone.utc), mjd + 2400000.5
    observed = photo.get('obsdate') or photo.get('observed_at') or photo.get('date')
    if observed:
        parsed = Time(observed).to_datetime(timezone=dt_timezone.utc)
        return parsed, Time(parsed).jd
    return None, None


def _parse_tns_photometry(target_data):
    rows = []
    for photo in _iter_photometry(target_data.get('photometry')):
        if not isinstance(photo, dict):
            continue
        timestamp, jd = _tns_timestamp(photo)
        if timestamp is None:
            continue

        unit = _nested_name(photo.get('flux_unit')) or photo.get('unit')
        if unit and 'mag' not in str(unit).lower():
            continue
        magnitude = _to_float(photo.get('flux') or photo.get('magnitude'))
        limit = _to_float(photo.get('limflux') or photo.get('limiting_flux'))
        if magnitude is None and limit is None:
            continue

        filter_name, survey, raw_filter = _tns_filter(photo)
        value = {
            'filter': filter_name,
            'observer': 'TNS',
            'facility': 'TNS',
            'survey': survey,
            'tns_filter': raw_filter,
            'tns_observer': photo.get('observer'),
            'tns_photometry_id': photo.get('id') or photo.get('photometry_id'),
            'julian_date': jd,
            'mjd': jd - 2400000.5 if jd is not None else None,
            'flux_unit': unit,
            'tns_telescope': _nested_name(photo.get('telescope')),
            'tns_instrument': _nested_name(photo.get('instrument')),
            'exposure_time': photo.get('exptime') or photo.get('exposure_time'),
            'comments': photo.get('remarks') or photo.get('comments'),
            'source_group': _nested_name(photo.get('source_group')),
        }
        if magnitude is not None:
            value['magnitude'] = magnitude
            error = _to_float(photo.get('fluxerr') or photo.get('flux_error') or photo.get('magnitude_error'))
            if error is not None and error >= 0:
                value['error'] = error
        else:
            value.update({'limit': limit, 'error': -1, 'upper_limit': True})
        rows.append({'timestamp': timestamp, 'value': value})
    return rows


def _classification(target_data):
    return _nested_name(target_data.get('object_type')) or _nested_name(target_data.get('type'))


def _discovery_date(target_data):
    raw_value = (
        target_data.get('discoverydate')
        or target_data.get('discovery_date')
        or target_data.get('discovered_at')
    )
    if isinstance(raw_value, datetime):
        parsed = raw_value
    elif raw_value not in (None, ''):
        try:
            parsed = datetime.fromisoformat(str(raw_value).strip().replace('Z', '+00:00'))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt_timezone.utc)
    return parsed


def _target_description(target_data):
    classification = str(_classification(target_data) or 'unknown').strip() or 'unknown'
    description = f'TNS target, classification {classification}'
    redshift = _to_float(target_data.get('redshift'))
    if redshift is not None:
        description += f', redshift {redshift:g}'
    return f'{description}.'


class TNSDataService(BaseTNSDataService):
    name = 'TNS'
    update_on_daily_refresh = True

    def build_query_parameters(self, parameters, **kwargs):
        adapted = dict(parameters)
        self._query_deadline_monotonic = adapted.get('_query_deadline_monotonic')
        target_name = adapted.get('target_name')
        if target_name:
            adapted['target_name'] = re.sub(r'^(?:SN|AT)\s*', '', str(target_name), flags=re.IGNORECASE)
        if 'radius_arcsec' in adapted:
            adapted.setdefault('radius', adapted.pop('radius_arcsec'))
            adapted.setdefault('units', 'arcsec')
        return super().build_query_parameters(adapted, **kwargs)

    def query_service(self, data, **kwargs):
        timeout = DATA_SERVICE_HTTP_TIMEOUT
        deadline = getattr(self, '_query_deadline_monotonic', None)
        if deadline is not None:
            remaining = max(0.1, float(deadline) - time.monotonic())
            timeout = (
                min(float(DATA_SERVICE_HTTP_TIMEOUT[0]), remaining),
                min(float(DATA_SERVICE_HTTP_TIMEOUT[1]), remaining),
            )

        # TOM Toolkit requests the full object after its search request. Enable TNS
        # photometry on that object call while retaining the upstream request flow.
        request_data = dict(data)
        if str(kwargs.get('url') or '').rstrip('/').endswith('/object'):
            try:
                body = json.loads(request_data.get('data') or '{}')
            except (TypeError, ValueError):
                body = {}
            body['photometry'] = '1'
            body['spectra'] = '0'
            request_data['data'] = json.dumps(body)

        response = requests.post(
            kwargs['url'],
            data=request_data,
            headers=self.build_headers(),
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
        self.query_results = payload.get('data') or []
        return self.query_results

    def query_targets(self, query_parameters):
        targets = super().query_targets(query_parameters)
        for target in targets:
            if not isinstance(target, dict):
                continue
            objname = str(target.get('objname') or '').strip()
            prefix = target.get('name_prefix') or target.get('prefix') or ''
            target['name'] = f'{prefix} {objname}'.strip()
            target['source'] = self.name
            target['classification'] = _classification(target)
            target['redshift'] = _to_float(target.get('redshift'))
            target['discovery_date'] = _discovery_date(target)
            target['ra'] = _first_float(target, 'radeg', 'ra_deg', 'ra')
            target['dec'] = _first_float(target, 'decdeg', 'dec_deg', 'dec')
            target['source_location'] = f'https://www.wis-tns.org/object/{quote(objname)}'
            target['reduced_datums'] = {'photometry': _parse_tns_photometry(target)}
        return targets

    def create_target_from_query(self, target_result, **kwargs):
        target = Target(
            name=target_result.get('name'),
            type=Target.SIDEREAL,
            ra=_first_float(target_result, 'ra', 'radeg', 'ra_deg'),
            dec=_first_float(target_result, 'dec', 'decdeg', 'dec_deg'),
            epoch=2000.0,
        )
        target.description = _target_description(target_result)
        target.importance = 9.99
        target.cadence = 1.0
        target.discovery_date = _discovery_date(target_result)
        redshift = _to_float(target_result.get('redshift'))
        if redshift is not None:
            target.redshift = redshift
        return target

    def create_reduced_datums_from_query(self, target, data=None, data_type=None, **kwargs):
        """Persist TNS photometry when TOM's interactive DataService import calls us."""
        if data_type != 'photometry' or not data:
            return
        source_location = kwargs.get('source_location') or ''
        for datum in data:
            ReducedDatum.objects.get_or_create(
                target=target,
                data_type='photometry',
                source_name=self.name,
                timestamp=datum['timestamp'],
                value=datum['value'],
                defaults={'source_location': source_location},
            )
