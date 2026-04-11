import logging
import math
import re

import requests
from astropy.coordinates import SkyCoord
import astropy.units as u
from tom_catalogs.harvester import AbstractHarvester


logger = logging.getLogger(__name__)

EXOCLOCK_PLANETS_JSON_URL = 'https://www.exoclock.space/database/planets_json'
EXOCLOCK_PLANET_URL = 'https://www.exoclock.space/database/planets'


def _to_float(value):
    try:
        number = float(value)
        if math.isnan(number) or not math.isfinite(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def _normalize_name(value):
    return re.sub(r'[^0-9a-z]+', '', str(value or '').strip().lower())


def _fetch_catalog():
    response = requests.get(EXOCLOCK_PLANETS_JSON_URL, timeout=60)
    response.raise_for_status()
    return response.json()


def _candidate_names(key, planet_data):
    return {
        _normalize_name(key),
        _normalize_name(planet_data.get('name')),
        _normalize_name(planet_data.get('star')),
    }


def _match_by_name(catalog, term):
    normalized = _normalize_name(term)
    if not normalized:
        return None

    exact_matches = []
    prefix_matches = []
    contains_matches = []
    for key, planet_data in catalog.items():
        names = _candidate_names(key, planet_data)
        if normalized in names:
            exact_matches.append((key, planet_data))
            continue
        if any(name.startswith(normalized) for name in names if name):
            prefix_matches.append((key, planet_data))
            continue
        if any(normalized in name for name in names if name):
            contains_matches.append((key, planet_data))
    matches = exact_matches or prefix_matches or contains_matches
    return matches[0] if matches else None


def _match_by_cone(catalog, ra_deg, dec_deg, radius_arcsec):
    center = SkyCoord(ra=ra_deg * u.deg, dec=dec_deg * u.deg)
    best_match = None
    best_sep = None
    for key, planet_data in catalog.items():
        ra_text = planet_data.get('ra_j2000')
        dec_text = planet_data.get('dec_j2000')
        if not ra_text or not dec_text:
            continue
        try:
            candidate = SkyCoord(ra_text, dec_text, unit=(u.hourangle, u.deg))
        except Exception:
            continue
        separation = center.separation(candidate).arcsecond
        if separation <= radius_arcsec and (best_sep is None or separation < best_sep):
            best_match = (key, planet_data)
            best_sep = separation
    return best_match


def _match_term(catalog, term):
    search_term = str(term or '').strip()
    if not search_term:
        return None

    by_name = _match_by_name(catalog, search_term)
    if by_name:
        return by_name

    parts = re.split(r'[\s,]+', search_term)
    if len(parts) == 3:
        try:
            ra_deg = float(parts[0])
            dec_deg = float(parts[1])
            radius_arcsec = float(parts[2])
        except (TypeError, ValueError):
            return None
        if radius_arcsec > 0:
            return _match_by_cone(catalog, ra_deg, dec_deg, radius_arcsec)
    return None


def get(term):
    catalog = _fetch_catalog()
    match = _match_term(catalog, term)
    if not match:
        return {}
    key, planet_data = match
    result = dict(planet_data)
    result['_catalog_key'] = key
    return result


def get_all(term):
    result = get(term)
    return [result] if result else []


class ExoClockHarvester(AbstractHarvester):
    name = 'ExoClock'

    def query(self, term):
        try:
            self.catalog_data = get(term)
        except Exception as exc:
            logger.warning('ExoClock query failed for term "%s": %s', term, exc)
            self.catalog_data = {}
        return self.catalog_data

    def to_target(self):
        target = super().to_target()
        if not self.catalog_data:
            return target

        ra_text = self.catalog_data.get('ra_j2000')
        dec_text = self.catalog_data.get('dec_j2000')
        coords = SkyCoord(ra_text, dec_text, unit=(u.hourangle, u.deg)) if ra_text and dec_text else None
        planet_name = str(self.catalog_data.get('name') or self.catalog_data.get('_catalog_key') or '').strip()
        host_name = str(self.catalog_data.get('star') or '').strip()

        target.name = planet_name or 'ExoClock'
        target.type = 'SIDEREAL'
        target.ra = coords.ra.degree if coords else None
        target.dec = coords.dec.degree if coords else None
        target.epoch = 2000.0
        target.classification = 'Planetary Transit'
        target.description = f'ExoClock transit target{f" around {host_name}" if host_name else ""}'
        source_url = f'{EXOCLOCK_PLANET_URL}/{planet_name}' if planet_name else EXOCLOCK_PLANETS_JSON_URL
        target.extra_aliases = []
        if host_name:
            target.extra_aliases.append({
                'name': host_name,
                'url': source_url,
                'source_name': self.name,
            })
        target.transit_source_name = self.name
        target.transit_source_url = source_url
        target.transit_planet_name = planet_name
        target.transit_host_name = host_name
        target.transit_t0_bjd_tdb = _to_float(self.catalog_data.get('t0_bjd_tdb'))
        target.transit_t0_unc = _to_float(self.catalog_data.get('t0_unc'))
        target.transit_period_days = _to_float(self.catalog_data.get('period_days'))
        target.transit_period_unc = _to_float(self.catalog_data.get('period_unc'))
        target.transit_duration_hours = _to_float(self.catalog_data.get('duration_hours'))
        target.transit_depth_r_mmag = _to_float(self.catalog_data.get('depth_r_mmag'))
        target.transit_v_mag = _to_float(self.catalog_data.get('v_mag'))
        target.transit_r_mag = _to_float(self.catalog_data.get('r_mag'))
        target.transit_gaia_g_mag = _to_float(self.catalog_data.get('gaia_g_mag'))
        target.transit_priority = str(self.catalog_data.get('priority') or '').strip()
        return target
