import logging
import math
from decimal import Decimal, InvalidOperation

import requests
from astropy.time import Time

from tom_catalogs.harvester import AbstractHarvester


logger = logging.getLogger(__name__)

FINK_API_URL = 'https://api.lsst.fink-portal.org'


def _normalize_lsst_id(term):
    term_str = str(term).strip()
    if term_str.upper().startswith('LSST_'):
        term_str = term_str.split('_', 1)[1].strip()

    # Handle values like "313761043604045880.0" or scientific notation.
    try:
        parsed = Decimal(term_str)
        if parsed == parsed.to_integral_value():
            return str(int(parsed))
    except (InvalidOperation, ValueError):
        pass

    return term_str


def _parse_rows(response_json):
    if isinstance(response_json, list):
        return response_json
    if isinstance(response_json, dict):
        return response_json.get('data', [])
    return []


def _first_present(row, keys):
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def get(term):
    dia_object_id = _normalize_lsst_id(term)
    if not dia_object_id.isdigit():
        return {}

    def _query(endpoint, payload):
        response = requests.post(f'{FINK_API_URL}{endpoint}', json=payload, timeout=20)
        response.raise_for_status()
        return _parse_rows(response.json())

    object_rows = _query('/api/v1/objects', {'diaObjectId': dia_object_id, 'output-format': 'json'})
    if not object_rows:
        object_rows = _query('/api/v1/objects', {'objectId': dia_object_id, 'output-format': 'json'})

    source_rows = _query('/api/v1/sources', {'diaObjectId': dia_object_id, 'output-format': 'json'})
    if not source_rows:
        source_rows = _query('/api/v1/sources', {'objectId': dia_object_id, 'output-format': 'json'})

    if not object_rows and not source_rows:
        return {}

    first_row = object_rows[0] if object_rows else source_rows[0]
    lsst_id = str(_first_present(first_row, ('r:diaObjectId', 'diaObjectId', 'objectId')) or dia_object_id)
    ra = _first_present(first_row, ('i:ra', 'r:ra', 'ra'))
    dec = _first_present(first_row, ('i:dec', 'r:dec', 'dec'))

    if (ra is None or dec is None) and source_rows:
        source_first = source_rows[0]
        ra = _first_present(source_first, ('i:ra', 'r:ra', 'ra'))
        dec = _first_present(source_first, ('i:dec', 'r:dec', 'dec'))
        source_id = _first_present(source_first, ('r:diaObjectId', 'diaObjectId', 'objectId'))
        if source_id is not None:
            lsst_id = str(source_id)

    if ra is None or dec is None:
        return {}

    try:
        ra_value = float(ra)
        dec_value = float(dec)
    except (TypeError, ValueError):
        return {}

    mjd_values = []
    for row in source_rows:
        value = _first_present(row, ('r:midpointMjdTai', 'midpointMjdTai', 'mjd'))
        try:
            mjd = float(value)
            if math.isfinite(mjd):
                mjd_values.append(mjd)
        except (TypeError, ValueError):
            continue

    disc = ''
    if mjd_values:
        try:
            disc = Time(min(mjd_values), format='mjd').to_datetime().date().isoformat()
        except Exception:
            disc = ''

    return {
        'lsst_id': lsst_id,
        'ra': ra_value,
        'dec': dec_value,
        'disc': disc,
    }


class LSSTHarvester(AbstractHarvester):
    name = 'LSST'

    def query(self, term):
        try:
            self.catalog_data = get(term)
        except Exception as exc:
            logger.warning('LSST query failed for term "%s": %s', term, exc)
            self.catalog_data = {}
        return self.catalog_data

    def to_target(self):
        target = super().to_target()
        lsst_id = str(self.catalog_data.get('lsst_id', '')).strip()
        target.name = f'LSST_{lsst_id}' if lsst_id else 'LSST'
        target.type = 'SIDEREAL'
        target.ra = self.catalog_data.get('ra')
        target.dec = self.catalog_data.get('dec')
        target.epoch = 2000.0
        return target
