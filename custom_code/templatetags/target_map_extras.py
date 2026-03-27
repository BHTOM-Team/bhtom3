from datetime import datetime, timezone
import json

from astroplan import moon_illumination
from astropy.coordinates import get_body
from astropy.time import Time
from django import template

from tom_targets.models import Target
from custom_code.sun_separation import get_live_target_values

register = template.Library()


@register.inclusion_tag('tom_targets/partials/aladin_skymap.html')
def custom_target_distribution(targets):
    """
    Aladin skymap payload with live non-sidereal coordinates.
    Sidereal targets use stored coordinates.
    """
    now = Time(datetime.now(timezone.utc), scale="utc")
    target_list = []
    for target in targets:
        live = get_live_target_values(target, time_to_compute=now)
        ra = live.get('ra')
        dec = live.get('dec')
        if ra is None or dec is None:
            continue
        target_list.append({
            'name': target.name,
            'ra': ra,
            'dec': dec,
            'is_non_sidereal': target.type == Target.NON_SIDEREAL,
        })

    moon_pos = get_body('moon', now)
    moon_illum = moon_illumination(now)
    sun_pos = get_body('sun', now)

    return {
        'targets': json.dumps(target_list),
        'moon_ra': moon_pos.ra.deg,
        'moon_dec': moon_pos.dec.deg,
        'moon_illumination': moon_illum,
        'sun_ra': sun_pos.ra.deg,
        'sun_dec': sun_pos.dec.deg,
    }
