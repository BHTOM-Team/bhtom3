"""ALeRCE ZTF object search, by ZTF object id (e.g. ZTF24aaipblm) or by coordinates."""

import logging
import math
import re
from urllib.parse import quote

import requests
from tom_catalogs.harvester import AbstractHarvester

logger = logging.getLogger(__name__)

ALERCE_API_URL = 'https://api.alerce.online/ztf/v1/objects'
ALERCE_PAGE = 'https://alerce.online/'
ALERCE_TIMEOUT = 20
ALERCE_MAX_RESULTS = 50
# ZTF object ids: 'ZTF', two-digit year, seven lowercase letters.
ZTF_OID_RE = re.compile(r'^ztf(\d{2})([a-z]{7})$', re.IGNORECASE)
# Data service whose aliases and finding-chart circle the ZTF id belongs to.
ALIAS_SOURCE_NAME = 'Alerce'


def _to_float(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def ztf_oid(term):
    """Canonical ZTF object id (ZTF24aaipblm) if the term is one, else None."""
    match = ZTF_OID_RE.match(str(term or '').strip())
    return f'ZTF{match.group(1)}{match.group(2).lower()}' if match else None


def object_url(oid):
    return f"{ALERCE_PAGE}object/{quote(str(oid), safe='')}"


def get_by_oid(oid):
    response = requests.get(f"{ALERCE_API_URL}/{quote(oid, safe='')}", timeout=ALERCE_TIMEOUT,
                            headers={'accept': 'application/json'})
    if response.status_code == 404:
        return None
    response.raise_for_status()
    data = response.json()
    return data if data.get('oid') else None


def cone_search(ra, dec, radius_arcsec):
    """ALeRCE objects within the radius, nearest first."""
    response = requests.get(ALERCE_API_URL, timeout=ALERCE_TIMEOUT, headers={'accept': 'application/json'}, params={
        'ra': ra, 'dec': dec, 'radius': radius_arcsec, 'page': 1, 'page_size': ALERCE_MAX_RESULTS,
    })
    response.raise_for_status()
    items = [item for item in response.json().get('items') or [] if item.get('oid')]

    def separation(item):
        item_ra, item_dec = _to_float(item.get('meanra')), _to_float(item.get('meandec'))
        if item_ra is None or item_dec is None:
            return float('inf')
        dra = ((item_ra - ra + 180.0) % 360.0 - 180.0) * math.cos(math.radians(dec))
        return math.hypot(dra, item_dec - dec) * 3600.0

    for item in items:
        item['separation_arcsec'] = separation(item)
    return sorted(items, key=lambda item: (item['separation_arcsec'], -(_to_float(item.get('ndet')) or 0)))


def get_all(term='', ra=None, dec=None, radius_arcsec=3.0):
    """A ZTF id is looked up directly; otherwise a cone search is run when coordinates are given."""
    oid = ztf_oid(term)
    if oid:
        found = get_by_oid(oid)
        return [found] if found else []
    if str(term or '').strip():
        return []
    ra, dec = _to_float(ra), _to_float(dec)
    if ra is None or dec is None:
        return []
    return cone_search(ra, dec, _to_float(radius_arcsec) or 3.0)


def summary(row):
    parts = []
    if row.get('ndet') is not None:
        parts.append(f"{row['ndet']} detections")
    separation = _to_float(row.get('separation_arcsec'))
    if separation is not None:
        parts.append(f'{separation:.2f}″ from position')
    return ', '.join(parts)


class AlerceHarvester(AbstractHarvester):
    name = 'ALeRCE'

    def query(self, term):
        try:
            matches = get_all(term)
        except Exception as exc:
            logger.warning('ALeRCE query failed for term "%s": %s', term, exc)
            matches = []
        self.catalog_data = matches[0] if matches else {}
        return self.catalog_data

    def to_target(self):
        target = super().to_target()
        oid = str(self.catalog_data.get('oid') or '').strip()
        target.name = oid or 'ALeRCE'
        target.type = 'SIDEREAL'
        target.ra = _to_float(self.catalog_data.get('meanra'))
        target.dec = _to_float(self.catalog_data.get('meandec'))
        target.epoch = 2000.0
        target.extra_aliases = [{'name': oid, 'url': object_url(oid), 'source_name': ALIAS_SOURCE_NAME}] if oid else []
        return target
