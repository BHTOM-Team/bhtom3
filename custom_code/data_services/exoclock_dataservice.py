import math
import re
from datetime import timezone as datetime_timezone
from typing import Any, Dict, Iterable

import astropy.units as u
import requests
from astropy.coordinates import SkyCoord
from astropy.time import Time
from django.utils import timezone

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import ExoClockQueryForm
from custom_code.sun_separation import compute_sun_separation


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


def _planet_page_url(planet_name: str) -> str:
    return f'{EXOCLOCK_PLANET_URL}/{planet_name}'


class ExoClockDataService(DataService):
    name = 'ExoClock'
    verbose_name = 'ExoClock'
    update_on_daily_refresh = True
    info_url = EXOCLOCK_PLANETS_JSON_URL
    service_notes = 'Query ExoClock by planet name or cone search and ingest transit ephemerides.'

    @classmethod
    def get_form_class(cls):
        return ExoClockQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_names = parameters.get('target_names') or []
        if isinstance(target_names, str):
            target_names = [target_names]
        target_name, ra, dec = resolve_query_coordinates(parameters)
        if target_name and target_name not in target_names:
            target_names = [target_name] + list(target_names)

        self.query_parameters = {
            'target_name': target_name,
            'target_names': [name for name in target_names if str(name).strip()],
            'ra': _to_float(ra),
            'dec': _to_float(dec),
            'radius_arcsec': _to_float(parameters.get('radius_arcsec')) or 30.0,
            'magnitude_limit': _to_float(parameters.get('magnitude_limit')),
            'eclipse_depth_min': _to_float(parameters.get('eclipse_depth_min')),
            'declination_min': _to_float(parameters.get('declination_min')),
            'declination_max': _to_float(parameters.get('declination_max')),
            'sun_distance_min': _to_float(parameters.get('sun_distance_min')),
            'transit_within_days': _to_float(parameters.get('transit_within_days')),
            'compute_from_date': self._coerce_time(parameters.get('compute_from_date')),
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

        if self._uses_advanced_filters(query_parameters):
            results = []
            for planet_key, planet_data in catalog.items():
                if not self._catalog_entry_matches(query_parameters, planet_key, planet_data):
                    continue
                results.append(
                    self._build_target_result(
                        planet_key,
                        planet_data,
                        _planet_page_url(planet_key),
                        query_parameters=query_parameters,
                    )
                )
            return results

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
        return [self._build_target_result(planet_key, planet_data, source_location, query_parameters=query_parameters)]

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

    def _catalog_entry_matches(self, query_parameters, planet_key: str, planet_data: Dict[str, Any]) -> bool:
        if not self._matches_name_filter(planet_key, planet_data, query_parameters.get('target_names') or []):
            return False
        if not self._matches_coordinate_filter(
            planet_data,
            ra=query_parameters.get('ra'),
            dec=query_parameters.get('dec'),
            radius_arcsec=query_parameters.get('radius_arcsec') or 30.0,
        ):
            return False

        coord = self._entry_coord(planet_data)
        if coord is None:
            return False

        v_mag = _to_float(planet_data.get('v_mag'))
        magnitude_limit = query_parameters.get('magnitude_limit')
        if magnitude_limit is not None and (v_mag is None or v_mag > magnitude_limit):
            return False

        eclipse_depth = _to_float(planet_data.get('depth_r_mmag'))
        eclipse_depth_min = query_parameters.get('eclipse_depth_min')
        if eclipse_depth_min is not None and (eclipse_depth is None or eclipse_depth < eclipse_depth_min):
            return False

        declination_min = query_parameters.get('declination_min')
        declination_max = query_parameters.get('declination_max')
        if declination_min is not None and coord.dec.degree < declination_min:
            return False
        if declination_max is not None and coord.dec.degree > declination_max:
            return False

        compute_from = query_parameters.get('compute_from_date') or self._coerce_time(timezone.now())

        sun_distance_min = query_parameters.get('sun_distance_min')
        if sun_distance_min is not None:
            sun_distance = compute_sun_separation(coord.ra.degree, coord.dec.degree, time_to_compute=compute_from)
            if sun_distance < sun_distance_min:
                return False

        transit_within_days = query_parameters.get('transit_within_days')
        if transit_within_days is not None:
            next_transit = self._compute_next_transit(planet_data, compute_from)
            if next_transit is None:
                return False
            if next_transit['days_until'] < 0 or next_transit['days_until'] > transit_within_days:
                return False

        return True

    def _build_target_result(
        self,
        planet_key: str,
        planet_data: Dict[str, Any],
        source_location: str,
        query_parameters: Dict[str, Any] = None,
    ):
        coord = SkyCoord(planet_data['ra_j2000'], planet_data['dec_j2000'], unit=(u.hourangle, u.deg))
        query_parameters = query_parameters or {}
        compute_from = query_parameters.get('compute_from_date') or self._coerce_time(timezone.now())
        next_transit = self._compute_next_transit(planet_data, compute_from)
        sun_distance = None
        if query_parameters.get('sun_distance_min') is not None or query_parameters.get('compute_from_date') is not None:
            sun_distance = compute_sun_separation(coord.ra.degree, coord.dec.degree, time_to_compute=compute_from)
        aliases = []
        host_name = str(planet_data.get('star') or '').strip()
        if host_name:
            aliases.append({'name': host_name, 'url': source_location, 'source_name': self.name})

        return {
            'name': planet_data.get('name') or planet_key,
            'ra': coord.ra.degree,
            'dec': coord.dec.degree,
            'aliases': aliases,
            'source_location': source_location,
            'next_transit_utc': next_transit['utc'].strftime('%Y-%m-%d %H:%M:%S') if next_transit else '',
            'next_transit_in_days': round(next_transit['days_until'], 4) if next_transit else '',
            'sun_distance_deg': round(sun_distance, 1) if sun_distance is not None else '',
            'transit_source_name': self.name,
            'transit_source_url': source_location,
            'transit_planet_name': planet_data.get('name') or planet_key,
            'transit_host_name': host_name,
            'transit_t0_bjd_tdb': _to_float(planet_data.get('t0_bjd_tdb')),
            'transit_t0_unc': _to_float(planet_data.get('t0_unc')),
            'transit_period_days': _to_float(planet_data.get('period_days')),
            'transit_period_unc': _to_float(planet_data.get('period_unc')),
            'transit_duration_hours': _to_float(planet_data.get('duration_hours')),
            'transit_depth_r_mmag': _to_float(planet_data.get('depth_r_mmag')),
            'transit_v_mag': _to_float(planet_data.get('v_mag')),
            'transit_r_mag': _to_float(planet_data.get('r_mag')),
            'transit_gaia_g_mag': _to_float(planet_data.get('gaia_g_mag')),
            'target_updates': {
                'ra': coord.ra.degree,
                'dec': coord.dec.degree,
                'epoch': 2000.0,
                'classification': 'Planetary Transit',
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

    @staticmethod
    def _coerce_time(value):
        if value in (None, ''):
            return None
        if isinstance(value, Time):
            return value
        if hasattr(value, 'tzinfo') and value.tzinfo is None:
            value = timezone.make_aware(value, timezone=datetime_timezone.utc)
        return Time(value)

    @staticmethod
    def _uses_advanced_filters(query_parameters):
        return any(
            query_parameters.get(key) is not None
            for key in (
                'magnitude_limit',
                'eclipse_depth_min',
                'declination_min',
                'declination_max',
                'sun_distance_min',
                'transit_within_days',
                'compute_from_date',
            )
        )

    @staticmethod
    def _matches_name_filter(planet_key: str, planet_data: Dict[str, Any], target_names: Iterable[str]) -> bool:
        normalized_names = {_normalize_name(name) for name in target_names if str(name).strip()}
        if not normalized_names:
            return True
        candidates = {
            _normalize_name(planet_key),
            _normalize_name(planet_data.get('name')),
            _normalize_name(planet_data.get('star')),
        }
        return bool(normalized_names & candidates)

    @staticmethod
    def _entry_coord(planet_data: Dict[str, Any]):
        planet_ra = planet_data.get('ra_j2000')
        planet_dec = planet_data.get('dec_j2000')
        if not planet_ra or not planet_dec:
            return None
        try:
            return SkyCoord(planet_ra, planet_dec, unit=(u.hourangle, u.deg))
        except Exception:
            return None

    def _matches_coordinate_filter(self, planet_data: Dict[str, Any], ra: float, dec: float, radius_arcsec: float) -> bool:
        if ra is None or dec is None:
            return True
        coord = self._entry_coord(planet_data)
        if coord is None:
            return False
        center = SkyCoord(ra=ra * u.deg, dec=dec * u.deg)
        return center.separation(coord).arcsecond <= radius_arcsec

    @staticmethod
    def _compute_next_transit(planet_data: Dict[str, Any], current_time: Time):
        t0_bjd_tdb = _to_float(planet_data.get('t0_bjd_tdb'))
        period_days = _to_float(planet_data.get('period_days'))
        if t0_bjd_tdb is None or period_days in (None, 0):
            return None

        current_bjd_tdb = current_time.tdb.jd
        epochs_since_t0 = (current_bjd_tdb - t0_bjd_tdb) / period_days
        next_epoch = math.ceil(epochs_since_t0)
        next_bjd_tdb = t0_bjd_tdb + next_epoch * period_days
        next_time = Time(next_bjd_tdb, format='jd', scale='tdb')
        return {
            'time': next_time,
            'utc': next_time.utc.to_datetime(timezone=datetime_timezone.utc),
            'days_until': next_time.utc.jd - current_time.utc.jd,
        }
