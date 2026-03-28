import logging
from typing import Any, Dict, List

from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.table import Row, Table
from astroquery.simbad import Simbad

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import SimbadQueryForm


logger = logging.getLogger(__name__)

SIMBAD_BASE_URL = 'https://simbad.cds.unistra.fr/simbad/sim-coo'


def _decode_identifier(value):
    if hasattr(value, 'item'):
        value = value.item()
    if isinstance(value, bytes):
        value = value.decode('utf-8')
    return str(value).strip()


def _clean_number(value):
    if value is None:
        return None
    mask = getattr(value, 'mask', None)
    if mask is not None and bool(mask):
        return None
    try:
        converted = value.item() if hasattr(value, 'item') else value
        return float(converted)
    except (TypeError, ValueError):
        return None


def _main_id_from_row(row: Row) -> str:
    value = row['main_id'] if 'main_id' in row.colnames else ''
    return _decode_identifier(value)


def _simbad_url(ra: float, dec: float) -> str:
    if ra is None or dec is None:
        return ''
    return (
        f'{SIMBAD_BASE_URL}?Coord={ra}+{dec}'
        f'&Radius=3&Radius.unit=arcsec&submit=submit+query'
    )


def _pick_best_row(table: Table, query_ra: float, query_dec: float) -> Row:
    if len(table) == 1:
        return table[0]

    center = SkyCoord(query_ra, query_dec, unit='deg')
    best_row = table[0]
    best_sep = None
    for row in table:
        ra = _clean_number(row['ra']) if 'ra' in row.colnames else None
        dec = _clean_number(row['dec']) if 'dec' in row.colnames else None
        if ra is None or dec is None:
            continue
        sep = center.separation(SkyCoord(ra, dec, unit='deg')).arcsecond
        if best_sep is None or sep < best_sep:
            best_sep = sep
            best_row = row
    return best_row


class SimbadDataService(DataService):
    name = 'Simbad'
    verbose_name = 'Simbad'
    update_on_daily_refresh = False
    info_url = 'https://simbad.cds.unistra.fr/simbad/'
    service_notes = 'Query SIMBAD by RA/Dec using a fixed 3 arcsec cone search.'

    @classmethod
    def get_form_class(cls):
        return SimbadQueryForm

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.simbad = Simbad()
        self.simbad.add_votable_fields('propermotions', 'parallax')

    def build_query_parameters(self, parameters, **kwargs):
        self.query_parameters = {
            'ra': parameters.get('ra'),
            'dec': parameters.get('dec'),
            'radius_arcsec': 3.0,
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = float(query_parameters['ra'])
        dec = float(query_parameters['dec'])
        coord = SkyCoord(ra, dec, unit='deg')
        self.query_results = self.simbad.query_region(coord, radius=3.0 * u.arcsec)
        return self.query_results

    def query_targets(self, query_parameters, **kwargs) -> List[Dict[str, Any]]:
        target_table: Table = self.query_service(query_parameters, **kwargs)
        if target_table is None or len(target_table) == 0:
            return []

        row = _pick_best_row(target_table, float(query_parameters['ra']), float(query_parameters['dec']))
        main_id = _main_id_from_row(row)
        ra = _clean_number(row['ra']) if 'ra' in row.colnames else float(query_parameters['ra'])
        dec = _clean_number(row['dec']) if 'dec' in row.colnames else float(query_parameters['dec'])
        pmra = _clean_number(row['pmra']) if 'pmra' in row.colnames else None
        pmdec = _clean_number(row['pmdec']) if 'pmdec' in row.colnames else None
        parallax = _clean_number(row['plx_value']) if 'plx_value' in row.colnames else None

        if ra is None:
            ra = float(query_parameters['ra'])
        if dec is None:
            dec = float(query_parameters['dec'])

        result = {
            'name': main_id.replace(' ', '') if main_id else 'SIMBAD',
            'ra': ra,
            'dec': dec,
            'pmra': pmra,
            'pmdec': pmdec,
            'plx_value': parallax,
            'main_id': main_id,
            'aliases': [{'name': main_id, 'url': _simbad_url(ra, dec), 'source_name': self.name}] if main_id else [],
            'target_updates': {
                'ra': ra,
                'dec': dec,
                'pm_ra': pmra,
                'pm_dec': pmdec,
                'parallax': parallax,
            },
        }
        logger.info('SIMBAD query result mapped to %s', result)
        return [result]

    def create_target_from_query(self, target_result: Dict[str, Any], **kwargs) -> Target:
        return Target(
            name=target_result['name'],
            type='SIDEREAL',
            ra=target_result['ra'],
            dec=target_result['dec'],
            pm_ra=target_result.get('pmra'),
            pm_dec=target_result.get('pmdec'),
            parallax=target_result.get('plx_value'),
        )

    def create_aliases_from_query(self, alias_results, **kwargs) -> List[TargetName]:
        aliases = []
        for alias in alias_results:
            if isinstance(alias, dict):
                aliases.append(TargetName(name=alias.get('name', '')))
            else:
                aliases.append(TargetName(name=alias))
        return aliases
