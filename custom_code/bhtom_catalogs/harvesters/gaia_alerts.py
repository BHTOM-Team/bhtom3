import csv
import io
import logging
import re

import requests
from astropy.coordinates import Angle, SkyCoord

from tom_catalogs.harvester import AbstractHarvester


logger = logging.getLogger(__name__)

GAIA_ALERTS_CSV_URL = 'https://gsaweb.ast.cam.ac.uk/alerts/alerts.csv'


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fetch_alert_rows():
    response = requests.get(GAIA_ALERTS_CSV_URL, timeout=30)
    response.raise_for_status()
    reader = csv.DictReader(io.StringIO(response.text))
    rows = []
    for row in reader:
        normalized = {}
        for key, value in row.items():
            if key is None:
                continue
            normalized[key.strip()] = value.strip() if isinstance(value, str) else value
        if normalized:
            rows.append(normalized)
    return rows


def _find_by_name(rows, name):
    term = str(name).strip().lower()
    if not term:
        return None
    for row in rows:
        candidate = str(row.get('#Name', '')).strip().lower()
        if candidate == term:
            return row
    return None


def _cone_search(rows, ra_deg, dec_deg, radius_arcsec):
    center = SkyCoord(ra=ra_deg, dec=dec_deg, unit='deg')
    radius = Angle(radius_arcsec, unit='arcsec')
    best_row = None
    best_sep = None
    for row in rows:
        ra = _to_float(row.get('RaDeg'))
        dec = _to_float(row.get('DecDeg'))
        if ra is None or dec is None:
            continue
        try:
            candidate = SkyCoord(ra=ra, dec=dec, unit='deg')
        except Exception:
            continue
        separation = center.separation(candidate)
        if separation <= radius and (best_sep is None or separation < best_sep):
            best_row = row
            best_sep = separation
    return best_row


def get(term):
    rows = _fetch_alert_rows()

    # Primary mode for catalog search: Gaia Alerts name, e.g. "Gaia21eeo".
    by_name = _find_by_name(rows, term)
    if by_name:
        return by_name

    # Optional mode: "ra dec radius_arcsec"
    parts = re.split(r'[\s,]+', str(term).strip())
    if len(parts) == 3:
        try:
            ra = float(parts[0])
            dec = float(parts[1])
            radius_arcsec = float(parts[2])
            if radius_arcsec > 0:
                return _cone_search(rows, ra, dec, radius_arcsec) or {}
        except (TypeError, ValueError):
            pass

    return {}


class GaiaAlertsHarvester(AbstractHarvester):
    name = 'Gaia Alerts'

    def query(self, term):
        try:
            self.catalog_data = get(term)
        except Exception as exc:
            logger.warning('Gaia Alerts query failed for term "%s": %s', term, exc)
            self.catalog_data = {}
        return self.catalog_data

    def to_target(self):
        target = super().to_target()
        alert_name = str(self.catalog_data.get('#Name', '')).strip()
        target.name = alert_name or 'GaiaAlerts'
        target.type = 'SIDEREAL'
        target.ra = _to_float(self.catalog_data.get('RaDeg'))
        target.dec = _to_float(self.catalog_data.get('DecDeg'))
        target.description = str(self.catalog_data.get('Comment', '')).strip()
        return target
