from django import template
from django.conf import settings
from decimal import Decimal, InvalidOperation, ROUND_DOWN


register = template.Library()


def _guess_alias_source(alias_name, url=''):
    value = str(alias_name or '').strip()
    url_value = str(url or '').strip().lower()
    upper = value.upper()

    if 'simbad' in url_value:
        return 'Simbad'
    if upper.startswith('GAIADR3_'):
        return 'GaiaDR3'
    if upper.startswith('GAIA'):
        return 'GaiaAlerts'
    if upper.startswith('LSST_'):
        return 'LSST'
    if upper.startswith('ASASSN_'):
        return 'ASASSN'
    if upper.startswith('ALLWISE'):
        return 'AllWISE'
    if upper.startswith('NEOWISE'):
        return 'NeoWISE'
    if upper.startswith('2MASS_'):
        return '2MASS'
    if upper.startswith('PS1_'):
        return 'PS1'
    if upper.startswith('SWIFT'):
        return 'SwiftUVOT'
    if upper.startswith('GALEX'):
        return 'Galex'
    if upper.startswith('6DFGS'):
        return '6dFGS'
    if upper.startswith('DESI'):
        return 'DESI'
    if upper.startswith('CRTS'):
        return 'CRTS'
    return 'Other'


def _simbad_coordinate_url(target):
    if target.ra is None or target.dec is None:
        return ''
    return (
        f'https://simbad.cds.unistra.fr/simbad/sim-coo?Coord={target.ra}+{target.dec}'
        f'&Radius=3&Radius.unit=arcsec&submit=submit+query'
    )


@register.filter
def truncate_decimals(value, places=4):
    if value in (None, ''):
        return ''
    try:
        places = int(places)
        quantizer = Decimal('1').scaleb(-places)
        truncated = Decimal(str(value)).quantize(quantizer, rounding=ROUND_DOWN)
        return f'{truncated:.{places}f}'
    except (InvalidOperation, TypeError, ValueError):
        return value


@register.inclusion_tag('tom_targets/partials/target_data.html')
def bhtom_target_data(target):
    extras = {
        k['name']: target.extra_fields.get(k['name'])
        for k in settings.EXTRA_FIELDS
        if not k.get('hidden')
    }
    extras = {key: value for key, value in extras.items() if value not in (None, '')}
    tags = {
        key: value
        for key, value in target.tags.items()
        if key not in {'parallax_error', 'pm_ra_error', 'pm_dec_error'}
    }
    astrometry_rows = [
        {
            'label': 'Parallax (mas)',
            'value': target.parallax,
            'error': getattr(target, 'parallax_error', None),
            'error_label': 'Parallax error (mas)',
        },
        {
            'label': 'Proper Motion RA (mas/yr)',
            'value': target.pm_ra,
            'error': getattr(target, 'pm_ra_error', None),
            'error_label': 'Proper Motion RA error (mas/yr)',
        },
        {
            'label': 'Proper Motion Dec (mas/yr)',
            'value': target.pm_dec,
            'error': getattr(target, 'pm_dec_error', None),
            'error_label': 'Proper Motion Dec error (mas/yr)',
        },
    ]
    astrometry_rows = [row for row in astrometry_rows if row['value'] is not None]
    other_names = []
    for alias in target.aliases.all().select_related('alias_info'):
        alias_info = getattr(alias, 'alias_info', None)
        url = getattr(alias_info, 'url', '')
        source_name = getattr(alias_info, 'source_name', '') or _guess_alias_source(alias.name, url)
        if source_name == 'Simbad':
            url = _simbad_coordinate_url(target)
        other_names.append({
            'source_name': source_name,
            'name': alias.name,
            'url': url,
        })
    other_names.sort(key=lambda row: (row['source_name'].lower(), row['name'].lower()))
    try:
        transit_ephemeris = target.transit_ephemeris
    except Exception:
        transit_ephemeris = None
    return {
        'target': target,
        'astrometry_rows': astrometry_rows,
        'extras': extras,
        'tags': tags,
        'target_other_names': other_names,
        'transit_ephemeris': transit_ephemeris,
    }
