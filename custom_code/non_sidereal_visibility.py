import math

from astroplan import Observer, time_grid_from_range
from astropy import units
from astropy.coordinates import SkyCoord, get_sun
from astropy.time import Time

from tom_observations import facility
from tom_targets.models import Target

from custom_code.sun_separation import _resolve_target_coordinates_now


def get_non_sidereal_visibility(target, start_time, end_time, interval, airmass_limit, observation_facility=None):
    """
    Calculate site-by-site airmass curves for a non-sidereal target.

    This mirrors TOM Toolkit's sidereal visibility planner, but resolves
    topocentric RA/Dec for each time sample and observing site.
    """
    if target.type != Target.NON_SIDEREAL:
        return {}

    if end_time < start_time:
        raise Exception('Start must be before end')

    if airmass_limit is None:
        airmass_limit = 10

    if observation_facility is None:
        facilities = facility.get_service_classes()
    else:
        facilities = [observation_facility]

    start = Time(start_time)
    end = Time(end_time)
    time_range = time_grid_from_range(time_range=[start, end], time_resolution=interval * units.minute)

    visibility = {}
    for observing_facility in facilities:
        observing_facility_class = facility.get_service_class(observing_facility)
        sites = observing_facility_class().get_observing_sites() or {}
        for site, site_details in sites.items():
            latitude = site_details.get('latitude')
            longitude = site_details.get('longitude')
            elevation = site_details.get('elevation', 0)
            if latitude in (None, '') or longitude in (None, ''):
                continue

            observer = Observer(
                longitude=float(longitude) * units.deg,
                latitude=float(latitude) * units.deg,
                elevation=float(elevation) * units.m,
            )

            times = []
            airmasses = []
            for tt in time_range:
                coords = _resolve_target_coordinates_now(
                    target,
                    time_to_compute=tt,
                    observer_lat_deg=latitude,
                    observer_lon_deg=longitude,
                    observer_elevation_m=elevation,
                )
                if coords is None:
                    times.append(tt.to_datetime())
                    airmasses.append(None)
                    continue

                ra, dec = coords
                body = SkyCoord(ra=ra * units.deg, dec=dec * units.deg, frame='icrs')
                obj_altaz = observer.altaz(tt, body)
                sun_alt = observer.altaz(tt, get_sun(tt)).alt

                secz = float(obj_altaz.secz)
                if (
                    not math.isfinite(secz) or
                    secz >= airmass_limit or
                    secz <= 1.0 or
                    float(sun_alt.deg) > -18.0
                ):
                    secz = None

                times.append(tt.to_datetime())
                airmasses.append(secz)

            visibility[f'({observing_facility}) {site}'] = (times, airmasses)

    return visibility
