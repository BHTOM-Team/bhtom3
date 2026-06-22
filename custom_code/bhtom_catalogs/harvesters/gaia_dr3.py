import logging
import math
import re

from astropy.coordinates import Angle, SkyCoord
from tom_catalogs.harvester import AbstractHarvester


logger = logging.getLogger(__name__)
PREFERRED_GAIA_VARIABILITY_CLASSIFIER = 'n_transits:5+'


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


def _build_source_query(where_clause, extra_columns=''):
    columns = (
        'g.source_id, g.ra, g.dec, g.parallax, g.pmra, g.pmdec, g.has_xp_sampled, '
        'vcr.best_class_name AS gaia_variability_type'
    )
    if extra_columns:
        columns = f'{columns}, {extra_columns}'
    return (
        f'SELECT TOP 1 {columns} '
        'FROM gaiadr3.gaia_source AS g '
        'LEFT OUTER JOIN gaiadr3.vari_classifier_result AS vcr '
        f"ON g.source_id = vcr.source_id AND vcr.classifier_name = '{PREFERRED_GAIA_VARIABILITY_CLASSIFIER}' "
        f'WHERE {where_clause}'
    )


def _build_variability_query(source_ids):
    normalized_ids = [str(source_id).strip() for source_id in source_ids if str(source_id).strip().isdigit()]
    if not normalized_ids:
        return None
    return (
        'SELECT source_id, best_class_name, classifier_name '
        'FROM gaiadr3.vari_classifier_result '
        f"WHERE source_id IN ({', '.join(normalized_ids)}) "
        'ORDER BY source_id ASC'
    )


def _select_variability_by_source(rows):
    selected = {}
    for row in rows:
        source_id = str(row.get('source_id') or row.get('SOURCE_ID') or '').strip()
        best_class_name = row.get('best_class_name') or row.get('BEST_CLASS_NAME')
        classifier_name = row.get('classifier_name') or row.get('CLASSIFIER_NAME')
        if not source_id or best_class_name in (None, ''):
            continue
        current = selected.get(source_id)
        if current is None or classifier_name == PREFERRED_GAIA_VARIABILITY_CLASSIFIER:
            selected[source_id] = str(best_class_name).strip()
    return selected


def _enrich_missing_variability_types(rows):
    missing_source_ids = [
        str(row.get('source_id') or row.get('SOURCE_ID') or '').strip()
        for row in rows
        if row.get('gaia_variability_type') in (None, '')
    ]
    query = _build_variability_query(missing_source_ids)
    if not query:
        return rows

    try:
        from astroquery.gaia import Gaia
        result = Gaia.launch_job(query).get_results()
    except Exception as exc:
        logger.warning('Error when backfilling Gaia DR3 variability class: %s', exc)
        return rows

    variability_map = _select_variability_by_source([_row_to_dict(row) for row in result])
    for row in rows:
        if row.get('gaia_variability_type') not in (None, ''):
            continue
        source_id = str(row.get('source_id') or row.get('SOURCE_ID') or '').strip()
        variability_type = variability_map.get(source_id)
        if variability_type:
            row['gaia_variability_type'] = variability_type
    return rows


def search_term_in_gaia(term):
    term_str = str(term).strip()
    if not term_str.isdigit():
        logger.info('Gaia DR3 source_id must be numeric, got: %s', term)
        return {}

    try:
        from astroquery.gaia import Gaia
        query = _build_source_query(f'g.source_id = {term_str}')
        job = Gaia.launch_job(query)
        result = job.get_results()
    except Exception as exc:
        logger.error('Error while querying Gaia DR3 for %s: %s', term, exc)
        return {}

    if len(result) == 0:
        return {}
    return _enrich_missing_variability_types([_row_to_dict(result[0])])[0]


def cone_search(coordinates, radius):
    try:
        ra_deg = float(coordinates.ra.deg)
        dec_deg = float(coordinates.dec.deg)
        radius_deg = float(radius.deg)
        box_prefilter = _build_box_prefilter(ra_deg, dec_deg, radius_deg)
        query = (
            _build_source_query(
                f'{box_prefilter} '
                f'AND DISTANCE(POINT({ra_deg}, {dec_deg}), POINT(g.ra, g.dec)) <= {radius_deg}',
                f'DISTANCE(POINT({ra_deg}, {dec_deg}), POINT(g.ra, g.dec)) AS dist',
            )
            + ' ORDER BY dist ASC'
        )
        from astroquery.gaia import Gaia
        result = Gaia.launch_job(query).get_results()
        if len(result) == 0:
            return {}
        return _enrich_missing_variability_types([_row_to_dict(result[0])])[0]
    except Exception as exc:
        logger.error('Error when running Gaia DR3 cone search: %s', exc)
        return {}


def cone_search_all(coordinates, radius, limit=100):
    try:
        ra_deg = float(coordinates.ra.deg)
        dec_deg = float(coordinates.dec.deg)
        radius_deg = float(radius.deg)
        box_prefilter = _build_box_prefilter(ra_deg, dec_deg, radius_deg)
        query = (
            f'SELECT TOP {int(limit)} '
            'g.source_id, g.ra, g.dec, g.parallax, g.pmra, g.pmdec, g.has_xp_sampled, '
            'vcr.best_class_name AS gaia_variability_type, '
            f'DISTANCE(POINT({ra_deg}, {dec_deg}), POINT(g.ra, g.dec)) AS dist '
            'FROM gaiadr3.gaia_source AS g '
            'LEFT OUTER JOIN gaiadr3.vari_classifier_result AS vcr '
            "ON g.source_id = vcr.source_id AND vcr.classifier_name = 'n_transits:5+' "
            f'WHERE {box_prefilter} '
            f'  AND DISTANCE(POINT({ra_deg}, {dec_deg}), POINT(g.ra, g.dec)) <= {radius_deg} '
            'ORDER BY dist ASC'
        )
        from astroquery.gaia import Gaia
        result = Gaia.launch_job(query).get_results()
        return _enrich_missing_variability_types([_row_to_dict(row) for row in result])
    except Exception as exc:
        logger.error('Error when running Gaia DR3 multi cone search: %s', exc)
        return []


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


def get_all(term):
    term_str = str(term).strip()
    catalog_data = search_term_in_gaia(term_str)
    if catalog_data:
        return [catalog_data]

    parts = re.split(r'[\s,]+', term_str)
    if len(parts) != 3:
        return []
    try:
        ra = float(parts[0])
        dec = float(parts[1])
        radius_arcsec = float(parts[2])
    except (TypeError, ValueError):
        return []
    if radius_arcsec <= 0:
        return []

    coordinates = SkyCoord(ra=ra, dec=dec, unit='deg')
    return cone_search_all(coordinates, Angle(radius_arcsec, unit='arcsec'))


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
        target.epoch = 2000.0
        target.parallax = _to_float(self.catalog_data.get('parallax'))
        target.pm_ra = _to_float(self.catalog_data.get('pmra'))
        target.pm_dec = _to_float(self.catalog_data.get('pmdec'))
        target.gaia_variability_type = self.catalog_data.get(
            'GAIA_VARIABILITY_TYPE', self.catalog_data.get('gaia_variability_type')
        )
        return target
