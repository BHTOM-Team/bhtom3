import logging
import math
import re

from astropy.coordinates import Angle, SkyCoord
from astroquery.gaia import Gaia
from tom_catalogs.harvester import AbstractHarvester


logger = logging.getLogger(__name__)


def _row_to_dict(row):
    data = {}
    for key in row.colnames:
        value = row[key]
        if hasattr(value, 'mask') and bool(value.mask):
            data[key] = None
        else:
            data[key] = value.item() if hasattr(value, 'item') else value
    return data


def _to_float(value):
    try:
        converted = float(value)
        if math.isnan(converted):
            return None
        return converted
    except (TypeError, ValueError):
        return None


def _build_box_prefilter(ra_deg, dec_deg, radius_deg):
    cos_dec = abs(math.cos(math.radians(dec_deg)))
    cos_dec = max(cos_dec, 1e-6)
    ra_half_width = min(180.0, radius_deg / cos_dec)
    dec_min = max(-90.0, dec_deg - radius_deg)
    dec_max = min(90.0, dec_deg + radius_deg)

    ra_min = (ra_deg - ra_half_width) % 360.0
    ra_max = (ra_deg + ra_half_width) % 360.0
    if ra_half_width >= 180.0:
        ra_clause = '1 = 1'
    elif ra_min <= ra_max:
        ra_clause = f'ra BETWEEN {ra_min} AND {ra_max}'
    else:
        ra_clause = f'(ra >= {ra_min} OR ra <= {ra_max})'

    return f'({ra_clause}) AND dec BETWEEN {dec_min} AND {dec_max}'


def search_term_in_gaia(term):
    term_str = str(term).strip()
    if not term_str.isdigit():
        logger.info('Gaia DR3 source_id must be numeric, got: %s', term)
        return {}

    try:
        query = (
            'SELECT TOP 1 source_id, ra, dec, parallax, pmra, pmdec, has_xp_sampled '
            'FROM gaiadr3.gaia_source '
            f'WHERE source_id = {term_str}'
        )
        job = Gaia.launch_job(query)
        result = job.get_results()
    except Exception as exc:
        logger.error('Error while querying Gaia DR3 for %s: %s', term, exc)
        return {}

    if len(result) == 0:
        return {}
    return _row_to_dict(result[0])


def cone_search(coordinates, radius):
    try:
        ra_deg = float(coordinates.ra.deg)
        dec_deg = float(coordinates.dec.deg)
        radius_deg = float(radius.deg)
        box_prefilter = _build_box_prefilter(ra_deg, dec_deg, radius_deg)
        query = (
            'SELECT TOP 1 source_id, ra, dec, parallax, pmra, pmdec, has_xp_sampled, '
            f'       DISTANCE(POINT({ra_deg}, {dec_deg}), POINT(ra, dec)) AS dist '
            'FROM gaiadr3.gaia_source '
            f'WHERE {box_prefilter} '
            f'  AND DISTANCE(POINT({ra_deg}, {dec_deg}), POINT(ra, dec)) <= {radius_deg} '
            'ORDER BY dist ASC'
        )
        result = Gaia.launch_job(query).get_results()
        if len(result) == 0:
            return {}
        return _row_to_dict(result[0])
    except Exception as exc:
        logger.error('Error when running Gaia DR3 cone search: %s', exc)
        return {}


def get(term):
    term_str = str(term).strip()
    catalog_data = search_term_in_gaia(term_str)
    if catalog_data:
        return catalog_data

    # Optional cone input: "ra dec radius_arcsec"
    parts = re.split(r'[\s,]+', term_str)
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

    coordinates = SkyCoord(ra=ra, dec=dec, unit='deg')
    return cone_search(coordinates, Angle(radius_arcsec, unit='arcsec'))


class GaiaDR3Harvester(AbstractHarvester):
    name = 'Gaia DR3'

    def query(self, term):
        self.catalog_data = get(term)
        return self.catalog_data

    def to_target(self):
        target = super().to_target()
        source_id = self.catalog_data.get('SOURCE_ID', self.catalog_data.get('source_id'))
        target.name = f'GaiaDR3_{source_id}' if source_id else 'GaiaDR3'
        target.type = 'SIDEREAL'
        target.ra = _to_float(self.catalog_data.get('ra'))
        target.dec = _to_float(self.catalog_data.get('dec'))
        target.parallax = _to_float(self.catalog_data.get('parallax'))
        target.pm_ra = _to_float(self.catalog_data.get('pmra'))
        target.pm_dec = _to_float(self.catalog_data.get('pmdec'))
        return target
