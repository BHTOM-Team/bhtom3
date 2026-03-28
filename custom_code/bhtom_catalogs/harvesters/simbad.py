import logging

from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.table import Row, Table
from astroquery.simbad import Simbad
from tom_catalogs.harvester import AbstractHarvester


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


class SimbadHarvester(AbstractHarvester):
    name = 'Simbad'

    def __init__(self, *args, **kwargs):
        self.simbad = Simbad()
        self.simbad.add_votable_fields('propermotions', 'parallax')
        self.query_ra = None
        self.query_dec = None

    def query(self, term='', ra=None, dec=None, radius_arcsec=None):
        self.catalog_data = None
        if ra is None or dec is None:
            return
        self.query_ra = float(ra)
        self.query_dec = float(dec)
        coord = SkyCoord(self.query_ra, self.query_dec, unit='deg')
        self.catalog_data = self.simbad.query_region(coord, radius=3.0 * u.arcsec)

    def to_target(self):
        target = super().to_target()
        row = _pick_best_row(self.catalog_data, self.query_ra, self.query_dec)
        main_id = _main_id_from_row(row)
        target.type = 'SIDEREAL'
        target.ra = _clean_number(row['ra']) if 'ra' in row.colnames else self.query_ra
        target.dec = _clean_number(row['dec']) if 'dec' in row.colnames else self.query_dec
        target.pm_ra = _clean_number(row['pmra']) if 'pmra' in row.colnames else None
        target.pm_dec = _clean_number(row['pmdec']) if 'pmdec' in row.colnames else None
        target.parallax = _clean_number(row['plx_value']) if 'plx_value' in row.colnames else None
        target.name = main_id.replace(' ', '') if main_id else 'SIMBAD'
        target.extra_aliases = [{'name': main_id, 'url': _simbad_url(target.ra, target.dec), 'source_name': self.name}] if main_id else []
        logger.info(
            'SIMBAD harvester mapped row with columns=%s main_id=%s ra=%s dec=%s',
            list(row.colnames), main_id, target.ra, target.dec
        )
        return target
