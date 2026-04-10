import logging
import math
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import astropy.units as u
import requests
from astropy.coordinates import SkyCoord
from bs4 import BeautifulSoup

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import ExoClockQueryForm


logger = logging.getLogger(__name__)

EXOCLOCK_BASE_URL = 'https://www.exoclock.space'
EXOCLOCK_PLANETS_JSON_URL = f'{EXOCLOCK_BASE_URL}/database/planets_json'
EXOCLOCK_PLANET_URL = f'{EXOCLOCK_BASE_URL}/database/planets'


def _to_float(value):
    try:
        number = float(value)
        if math.isnan(number) or not math.isfinite(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def _normalize_name(value: str) -> str:
    return re.sub(r'[^0-9a-z]+', '', str(value or '').strip().lower())


def _parse_oc_minutes(value: str):
    text = ' '.join(str(value or '').replace('&plusmn;', '±').split())
    if not text:
        return None, None
    match = re.search(r'([+-]?\d+(?:\.\d+)?)\s*±\s*(\d+(?:\.\d+)?)', text)
    if match:
        return _to_float(match.group(1)), _to_float(match.group(2))
    single = re.search(r'([+-]?\d+(?:\.\d+)?)', text)
    if single:
        return _to_float(single.group(1)), None
    return None, None


def _planet_page_url(planet_name: str) -> str:
    return f'{EXOCLOCK_PLANET_URL}/{planet_name}'


def _extract_date(text: str) -> str:
    match = re.search(r'(\d{4}-\d{2}-\d{2})', str(text or ''))
    return match.group(1) if match else ''


def _text_lines(cell) -> List[str]:
    if cell is None:
        return []
    return [line.strip() for line in cell.stripped_strings if line and line.strip()]


def _build_timing_value(section_name: str, date_text: str, oc_text: str, metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    oc_minutes, oc_uncertainty_minutes = _parse_oc_minutes(oc_text)
    if oc_minutes is None:
        return None

    value = {
        'date': date_text,
        'oc_minutes': oc_minutes,
        'category': section_name,
    }
    if oc_uncertainty_minutes is not None:
        value['oc_uncertainty_minutes'] = oc_uncertainty_minutes
    value.update({key: val for key, val in metadata.items() if val not in (None, '', [])})
    return value


class ExoClockDataService(DataService):
    name = 'ExoClock'
    verbose_name = 'ExoClock'
    update_on_daily_refresh = True
    info_url = EXOCLOCK_PLANETS_JSON_URL
    service_notes = 'Query ExoClock by planet name or cone search and ingest transit ephemerides plus public O-C timing rows.'

    @classmethod
    def get_form_class(cls):
        return ExoClockQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        target_names = parameters.get('target_names') or []
        if isinstance(target_names, str):
            target_names = [target_names]
        target_name = (parameters.get('target_name') or '').strip()
        if target_name and target_name not in target_names:
            target_names = [target_name] + list(target_names)

        self.query_parameters = {
            'target_name': target_name,
            'target_names': [name for name in target_names if str(name).strip()],
            'ra': _to_float(parameters.get('ra')),
            'dec': _to_float(parameters.get('dec')),
            'radius_arcsec': _to_float(parameters.get('radius_arcsec')) or 30.0,
            'include_timing_data': bool(parameters.get('include_timing_data', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        response = requests.get(EXOCLOCK_PLANETS_JSON_URL, timeout=60)
        response.raise_for_status()
        catalog = response.json()
        self.query_results = {
            'catalog': catalog,
            'source_location': EXOCLOCK_PLANETS_JSON_URL,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        catalog = data.get('catalog') or {}

        match = self._match_catalog_entry(
            catalog,
            target_names=query_parameters.get('target_names') or [],
            ra=query_parameters.get('ra'),
            dec=query_parameters.get('dec'),
            radius_arcsec=query_parameters.get('radius_arcsec') or 30.0,
        )
        if not match:
            return []

        planet_key, planet_data = match
        source_location = _planet_page_url(planet_key)
        result = self._build_target_result(planet_key, planet_data, source_location)

        if query_parameters.get('include_timing_data', True):
            try:
                result['reduced_datums'] = {
                    'transit_timing': self._fetch_timing_datums(planet_key, source_location),
                }
            except Exception as exc:
                logger.warning('ExoClock timing rows unavailable for %s: %s', planet_key, exc)

        return [result]

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
            alias_name = alias.get('name') if isinstance(alias, dict) else alias
            alias_name = str(alias_name or '').strip()
            if not alias_name or alias_name in seen:
                continue
            aliases.append(TargetName(name=alias_name))
            seen.add(alias_name)
        return aliases

    def create_reduced_datums_from_query(self, target, data=None, data_type=None, **kwargs):
        if data_type != 'transit_timing' or not data:
            return
        source_location = kwargs.get('source_location') or self.info_url
        for datum in data:
            ReducedDatum.objects.get_or_create(
                target=target,
                data_type='transit_timing',
                timestamp=datum['timestamp'],
                value=datum['value'],
                defaults={
                    'source_name': self.name,
                    'source_location': datum.get('source_location') or source_location,
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

    def _match_catalog_entry(self, catalog, target_names: Iterable[str], ra: float, dec: float, radius_arcsec: float):
        normalized_names = {_normalize_name(name) for name in target_names if str(name).strip()}

        if normalized_names:
            for key, planet_data in catalog.items():
                candidates = {
                    _normalize_name(key),
                    _normalize_name(planet_data.get('name')),
                    _normalize_name(planet_data.get('star')),
                }
                if normalized_names & candidates:
                    return key, planet_data

        if ra is None or dec is None:
            return None

        center = SkyCoord(ra=ra * u.deg, dec=dec * u.deg)
        best_match = None
        best_sep = None
        for key, planet_data in catalog.items():
            planet_ra = planet_data.get('ra_j2000')
            planet_dec = planet_data.get('dec_j2000')
            if not planet_ra or not planet_dec:
                continue
            try:
                candidate = SkyCoord(planet_ra, planet_dec, unit=(u.hourangle, u.deg))
            except Exception:
                continue
            separation = center.separation(candidate).arcsecond
            if separation <= radius_arcsec and (best_sep is None or separation < best_sep):
                best_match = (key, planet_data)
                best_sep = separation
        return best_match

    def _build_target_result(self, planet_key: str, planet_data: Dict[str, Any], source_location: str):
        coord = SkyCoord(planet_data['ra_j2000'], planet_data['dec_j2000'], unit=(u.hourangle, u.deg))
        aliases = [
            {'name': planet_data.get('name') or planet_key, 'url': source_location, 'source_name': self.name},
        ]
        host_name = str(planet_data.get('star') or '').strip()
        if host_name:
            aliases.append({'name': host_name, 'url': source_location, 'source_name': self.name})

        return {
            'name': planet_data.get('name') or planet_key,
            'ra': coord.ra.degree,
            'dec': coord.dec.degree,
            'aliases': aliases,
            'source_location': source_location,
            'target_updates': {
                'ra': coord.ra.degree,
                'dec': coord.dec.degree,
                'epoch': 2000.0,
            },
            'transit_ephemeris_updates': {
                'source_name': self.name,
                'source_url': source_location,
                'planet_name': planet_data.get('name') or planet_key,
                'host_name': host_name,
                'priority': str(planet_data.get('priority') or '').strip(),
                'current_oc_min': _to_float(planet_data.get('current_oc_min')),
                't0_bjd_tdb': _to_float(planet_data.get('t0_bjd_tdb')),
                't0_unc': _to_float(planet_data.get('t0_unc')),
                'period_days': _to_float(planet_data.get('period_days')),
                'period_unc': _to_float(planet_data.get('period_unc')),
                'duration_hours': _to_float(planet_data.get('duration_hours')),
                'depth_r_mmag': _to_float(planet_data.get('depth_r_mmag')),
                'v_mag': _to_float(planet_data.get('v_mag')),
                'r_mag': _to_float(planet_data.get('r_mag')),
                'gaia_g_mag': _to_float(planet_data.get('gaia_g_mag')),
                'min_telescope_inches': _to_float(planet_data.get('min_telescope_inches')),
                'total_observations': planet_data.get('total_observations'),
                'recent_observations': planet_data.get('total_observations_recent'),
                'payload': planet_data,
            },
        }

    def _fetch_timing_datums(self, planet_key: str, source_location: str):
        response = requests.get(source_location, timeout=60)
        response.raise_for_status()
        return self._parse_timing_datums(response.text, source_location)

    def _parse_timing_datums(self, html: str, source_location: str):
        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.find_all('table')
        if len(tables) < 4:
            return []

        rows = tables[3].find_all('tr')
        datums = []
        section_name = ''
        section_headers = []
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if not cells:
                continue

            row_text = ' '.join(' '.join(cell.get_text(' ', strip=True).split()) for cell in cells)
            if row_text in {'ExoClock Observations', 'Space Observations', 'ETD Observations', 'Literature Mid-times'}:
                section_name = row_text
                section_headers = []
                continue

            if not section_name:
                continue

            if len(cells) >= 3 and 'Observation Date' in row_text and 'O-C' in row_text:
                section_headers = [' '.join(cell.get_text(' ', strip=True).split()) for cell in cells]
                continue

            datum = self._parse_timing_row(section_name, cells, source_location)
            if datum:
                datums.append(datum)

        return datums

    def _parse_timing_row(self, section_name: str, cells, source_location: str):
        if section_name == 'ExoClock Observations':
            return self._parse_exoclock_row(cells, source_location)
        if section_name == 'Space Observations':
            return self._parse_space_row(cells, source_location)
        if section_name == 'ETD Observations':
            return self._parse_etd_row(cells, source_location)
        if section_name == 'Literature Mid-times':
            return self._parse_literature_row(cells, source_location)
        return None

    def _parse_exoclock_row(self, cells, source_location: str):
        if len(cells) < 5:
            return None
        planet_date_lines = _text_lines(cells[0])
        observer_lines = _text_lines(cells[1])
        instrument_lines = _text_lines(cells[2])
        date_text = _extract_date(' '.join(planet_date_lines))
        if not date_text:
            return None
        detail_href = cells[4].find('a')
        detail_url = f'{EXOCLOCK_BASE_URL}{detail_href["href"]}' if detail_href and detail_href.get('href') else source_location

        metadata = {
            'observer': observer_lines[0] if observer_lines else '',
            'observatory': observer_lines[1] if len(observer_lines) > 1 else '',
            'telescope': instrument_lines[0] if instrument_lines else '',
            'instrument': instrument_lines[1] if len(instrument_lines) > 1 else '',
            'detail_url': detail_url,
            'has_data_download': False,
        }
        value = _build_timing_value('exoclock', date_text, cells[3].get_text(' ', strip=True), metadata)
        return self._datum_from_value(date_text, value, detail_url)

    def _parse_space_row(self, cells, source_location: str):
        if len(cells) < 4:
            return None
        planet_date_lines = _text_lines(cells[0])
        date_text = _extract_date(' '.join(planet_date_lines))
        if not date_text:
            return None
        detail_href = cells[3].find('a')
        detail_url = f'{EXOCLOCK_BASE_URL}{detail_href["href"]}' if detail_href and detail_href.get('href') else source_location
        metadata = {
            'mission': ' '.join(_text_lines(cells[1])),
            'detail_url': detail_url,
            'has_data_download': 'Get data' in cells[3].get_text(' ', strip=True),
        }
        value = _build_timing_value('space', date_text, cells[2].get_text(' ', strip=True), metadata)
        return self._datum_from_value(date_text, value, detail_url)

    def _parse_etd_row(self, cells, source_location: str):
        if len(cells) < 5:
            return None
        planet_date_lines = _text_lines(cells[0])
        date_text = _extract_date(' '.join(planet_date_lines))
        if not date_text:
            return None
        tresca_link = cells[2].find('a')
        detail_href = cells[4].find('a')
        detail_url = f'{EXOCLOCK_BASE_URL}{detail_href["href"]}' if detail_href and detail_href.get('href') else source_location
        metadata = {
            'observers': ', '.join(_text_lines(cells[1])),
            'tresca_id': ' '.join(_text_lines(cells[2])),
            'tresca_url': tresca_link.get('href', '') if tresca_link else '',
            'detail_url': detail_url,
            'has_data_download': 'Get data' in cells[4].get_text(' ', strip=True),
        }
        value = _build_timing_value('etd', date_text, cells[3].get_text(' ', strip=True), metadata)
        return self._datum_from_value(date_text, value, detail_url)

    def _parse_literature_row(self, cells, source_location: str):
        if len(cells) < 4:
            return None
        date_text = _extract_date(' '.join(_text_lines(cells[0])))
        if not date_text:
            return None
        detail_href = cells[3].find('a')
        detail_url = f'{EXOCLOCK_BASE_URL}{detail_href["href"]}' if detail_href and detail_href.get('href') else source_location
        metadata = {
            'reference': ' '.join(_text_lines(cells[1])),
            'detail_url': detail_url,
            'has_data_download': False,
        }
        value = _build_timing_value('literature', date_text, cells[2].get_text(' ', strip=True), metadata)
        return self._datum_from_value(date_text, value, detail_url)

    def _datum_from_value(self, date_text: str, value: Optional[Dict[str, Any]], source_location: str):
        if not value or not date_text:
            return None
        timestamp = datetime.fromisoformat(date_text).replace(tzinfo=timezone.utc)
        return {
            'timestamp': timestamp,
            'value': value,
            'source_location': source_location,
        }
