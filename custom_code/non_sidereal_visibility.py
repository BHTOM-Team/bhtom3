import math

import numpy as np
from astroplan import Observer, time_grid_from_range
from astropy import units
from astropy.coordinates import EarthLocation, SkyCoord, get_body, get_sun
from astropy.time import Time

from tom_observations import facility
from tom_targets.models import Target

from custom_code.sun_separation import (
    _build_elements_from_target,
    _ecliptic_to_equatorial_j2000,
    _heliocentric_ecliptic_xyz,
)


def _geocentric_icrs_xyz_series(target, time_range):
    elements = _build_elements_from_target(target)
    if elements is None:
        return None

    obj_xyz_eq = []
    valid_mask = []
    for tt in time_range:
        obj_xyz_ecl = _heliocentric_ecliptic_xyz(elements, tt)
        if obj_xyz_ecl is None:
            obj_xyz_eq.append((np.nan, np.nan, np.nan))
            valid_mask.append(False)
            continue
        obj_xyz_eq.append(_ecliptic_to_equatorial_j2000(*obj_xyz_ecl))
        valid_mask.append(True)

    obj_xyz_eq = np.asarray(obj_xyz_eq, dtype=float)

    earth_bary = get_body("earth", time_range).cartesian
    sun_bary = get_body("sun", time_range).cartesian
    earth_helio = earth_bary - sun_bary
    ex = earth_helio.x.to(units.au).value
    ey = earth_helio.y.to(units.au).value
    ez = earth_helio.z.to(units.au).value

    gx = obj_xyz_eq[:, 0] - ex
    gy = obj_xyz_eq[:, 1] - ey
    gz = obj_xyz_eq[:, 2] - ez

    return gx, gy, gz, np.asarray(valid_mask, dtype=bool)


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
    geocentric_xyz = _geocentric_icrs_xyz_series(target, time_range)
    if geocentric_xyz is None:
        return {}
    gx, gy, gz, valid_mask = geocentric_xyz
    sun_coords = get_sun(time_range)

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

            location = EarthLocation(
                lon=float(longitude) * units.deg,
                lat=float(latitude) * units.deg,
                height=float(elevation) * units.m,
            )
            observer = Observer(
                location=location,
            )
            observer_gcrs, _ = location.get_gcrs_posvel(time_range)
            topox = gx - observer_gcrs.x.to(units.au).value
            topoy = gy - observer_gcrs.y.to(units.au).value
            topoz = gz - observer_gcrs.z.to(units.au).value

            body = SkyCoord(
                x=topox * units.au,
                y=topoy * units.au,
                z=topoz * units.au,
                representation_type='cartesian',
                frame='icrs',
            )
            obj_altaz = observer.altaz(time_range, body)
            sun_alt = observer.altaz(time_range, sun_coords).alt.deg

            times = []
            airmasses = []
            for idx, tt in enumerate(time_range):
                secz = float(obj_altaz.secz[idx])
                if (
                    not valid_mask[idx] or
                    not math.isfinite(secz) or
                    secz >= airmass_limit or
                    secz <= 1.0 or
                    float(sun_alt[idx]) > -18.0
                ):
                    secz = None

                times.append(tt.to_datetime())
                airmasses.append(secz)

            visibility[f'({observing_facility}) {site}'] = (times, airmasses)

    return visibility
