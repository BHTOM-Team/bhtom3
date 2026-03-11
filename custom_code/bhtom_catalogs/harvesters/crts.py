import logging
from io import StringIO

import pandas as pd
import requests
from astropy.time import Time
from tom_catalogs.harvester import AbstractHarvester


logger = logging.getLogger(__name__)

CRTS_QUERY_URL = 'http://nunuku.caltech.edu/cgi-bin/getcssconedb_release_img.cgi'
CRTS_REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _crts_alias(ra, dec):
    return f'CRTS+J{ra}_{dec}'


def _build_crts_url(ra, dec, radius_arcmin):
    return (
        f'{CRTS_QUERY_URL}?RADec={ra}+{dec}'
        f'&Rad={radius_arcmin}&DB=photcat&OUT=web&SHORT=short'
    )


def get(term):
    parts = str(term).strip().replace(',', ' ').split()
    if len(parts) != 3:
        return {}
    try:
        ra = float(parts[0])
        dec = float(parts[1])
        radius_arcsec = float(parts[2])
    except (TypeError, ValueError):
        return {}
    if radius_arcsec <= 0:
        return {}

    radius_arcmin = radius_arcsec / 60.0
    query_url = _build_crts_url(ra, dec, radius_arcmin)
    response = requests.get(query_url, timeout=30, headers=CRTS_REQUEST_HEADERS)
    response.raise_for_status()

    try:
        tables = pd.read_html(StringIO(response.text), match='Photometry of Objs')
    except ValueError:
        return {}
    if not tables or tables[0].empty:
        return {}

    first = tables[0].iloc[0].to_dict()
    discovered = ''
    mjd = _to_float(first.get('MJD'))
    if mjd is not None:
        try:
            discovered = Time(mjd, format='mjd').to_datetime().date().isoformat()
        except Exception:
            discovered = ''

    return {
        'name': _crts_alias(ra, dec),
        'ra': ra,
        'dec': dec,
        'disc': discovered,
    }


class CRTSHarvester(AbstractHarvester):
    name = 'CRTS'

    def query(self, term):
        try:
            self.catalog_data = get(term)
        except Exception as exc:
            logger.warning('CRTS query failed for term "%s": %s', term, exc)
            self.catalog_data = {}
        return self.catalog_data

    def to_target(self):
        target = super().to_target()
        target.name = self.catalog_data.get('name') or 'CRTS'
        target.type = 'SIDEREAL'
        target.ra = self.catalog_data.get('ra')
        target.dec = self.catalog_data.get('dec')
        return target
