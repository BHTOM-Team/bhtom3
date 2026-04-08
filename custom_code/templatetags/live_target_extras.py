from astropy.time import Time
from django import template

from custom_code.sun_separation import get_live_target_values

register = template.Library()


@register.simple_tag
def live_target_values(
    target,
    calculation_time=None,
    observer_lat_deg=None,
    observer_lon_deg=None,
    observer_elevation_m=None,
):
    time_to_compute = None
    if calculation_time:
        time_to_compute = Time(calculation_time, scale='utc')
    return get_live_target_values(
        target,
        time_to_compute=time_to_compute,
        observer_lat_deg=observer_lat_deg,
        observer_lon_deg=observer_lon_deg,
        observer_elevation_m=observer_elevation_m,
    )
