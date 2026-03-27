import logging
from datetime import datetime, timezone
import math
from typing import Optional
import urllib.error
import urllib.request

from astropy import units as u
from astropy.coordinates import AltAz, EarthLocation, get_sun
from astropy.time import Time
from django.core.cache import cache
from skyfield.api import EarthSatellite, load, wgs84


logger = logging.getLogger(__name__)

WARSAW_LAT_DEG = 52.2297
WARSAW_LON_DEG = 21.0122
WARSAW_ELEVATION_M = 100.0

_TLE_CACHE_TIMEOUT_SECONDS = 6 * 3600
_TS = load.timescale()


def _coerce_utc_datetime(value: Optional[datetime]) -> datetime:
    instant = value or datetime.now(timezone.utc)
    if instant.tzinfo is None:
        return instant.replace(tzinfo=timezone.utc)
    return instant.astimezone(timezone.utc)


def fetch_tle_by_norad_id(norad_id: int):
    url = f"https://celestrak.org/NORAD/elements/gp.php?CATNR={norad_id}&FORMAT=TLE"
    with urllib.request.urlopen(url, timeout=20) as response:
        text = response.read().decode("utf-8").strip()

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) < 3:
        raise ValueError(f"Unexpected TLE response for NORAD {norad_id}.")

    return lines[0], lines[1], lines[2]


def get_tle(norad_id: int):
    cache_key = f"geosat_tle_{norad_id}"
    cached = cache.get(cache_key)
    if cached and len(cached) == 3:
        return cached

    try:
        tle = fetch_tle_by_norad_id(norad_id)
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        logger.warning("Could not fetch TLE for NORAD %s: %s", norad_id, exc)
        if cached and len(cached) == 3:
            return cached
        return None

    cache.set(cache_key, tle, timeout=_TLE_CACHE_TIMEOUT_SECONDS)
    return tle


def geosat_alt_az(
    norad_id: int,
    observer_lat_deg: float = WARSAW_LAT_DEG,
    observer_lon_deg: float = WARSAW_LON_DEG,
    observer_elevation_m: float = WARSAW_ELEVATION_M,
    when_utc: Optional[datetime] = None,
):
    tle = get_tle(norad_id)
    if tle is None:
        return None

    name, line1, line2 = tle
    instant = _coerce_utc_datetime(when_utc)

    satellite = EarthSatellite(line1, line2, name, _TS)
    observer = wgs84.latlon(observer_lat_deg, observer_lon_deg, elevation_m=observer_elevation_m)
    skyfield_time = _TS.from_datetime(instant)
    topocentric = (satellite - observer).at(skyfield_time)
    alt, az, distance = topocentric.altaz()
    ra, dec, _ = topocentric.radec()
    lst_hours = observer.lst_hours_at(skyfield_time)
    hour_angle_hours = (lst_hours - ra.hours) % 24.0

    location = EarthLocation(
        lat=observer_lat_deg * u.deg,
        lon=observer_lon_deg * u.deg,
        height=observer_elevation_m * u.m,
    )
    obs_time = Time(instant)
    sun_altaz = get_sun(obs_time).transform_to(AltAz(obstime=obs_time, location=location))
    sat_alt_rad = math.radians(float(alt.degrees))
    sat_az_rad = math.radians(float(az.degrees))
    sun_alt_rad = math.radians(float(sun_altaz.alt.deg))
    sun_az_rad = math.radians(float(sun_altaz.az.deg))
    cos_elong = (
        math.sin(sat_alt_rad) * math.sin(sun_alt_rad)
        + math.cos(sat_alt_rad) * math.cos(sun_alt_rad) * math.cos(sat_az_rad - sun_az_rad)
    )
    cos_elong = max(-1.0, min(1.0, cos_elong))
    solar_elongation_deg = math.degrees(math.acos(cos_elong))
    phase_angle_deg = max(0.0, min(180.0, 180.0 - solar_elongation_deg))

    phase_factor = 0.5 * (1.0 + math.cos(math.radians(phase_angle_deg)))
    phase_factor = max(phase_factor, 1e-3)
    reference_range_km = 40000.0
    base_vmag = 11.0
    range_term = 5.0 * math.log10(max(float(distance.km), 1.0) / reference_range_km)
    phase_term = -2.5 * math.log10(phase_factor)
    estimated_vmag = base_vmag + range_term + phase_term

    return {
        "tle_name": name,
        "alt_deg": float(alt.degrees),
        "az_deg": float(az.degrees),
        "ra_icrf_hours": float(ra.hours),
        "dec_deg": float(dec.degrees),
        "hour_angle_hours": float(hour_angle_hours),
        "distance_km": float(distance.km),
        "solar_elongation_deg": float(solar_elongation_deg),
        "phase_angle_deg": float(phase_angle_deg),
        "estimated_vmag": float(estimated_vmag),
        "computed_at_utc": instant,
    }


def _altaz_to_enu_vector(alt_deg, az_deg):
    alt_rad = math.radians(alt_deg)
    az_rad = math.radians(az_deg)
    x_east = math.cos(alt_rad) * math.sin(az_rad)
    y_north = math.cos(alt_rad) * math.cos(az_rad)
    z_up = math.sin(alt_rad)
    return (x_east, y_north, z_up)


def _normalize_vector(vec):
    x, y, z = vec
    norm = math.sqrt(x * x + y * y + z * z)
    if norm == 0.0:
        return (0.0, 0.0, 0.0)
    return (x / norm, y / norm, z / norm)


def _cross(a, b):
    return (
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    )


def _enu_vector_to_altaz(vec):
    x_east, y_north, z_up = _normalize_vector(vec)
    alt_rad = math.asin(z_up)
    az_rad = math.atan2(x_east, y_north)
    az_deg = math.degrees(az_rad) % 360.0
    alt_deg = math.degrees(alt_rad)
    return alt_deg, az_deg


def sun_visibility_curve(
    observer_lat_deg: float = WARSAW_LAT_DEG,
    observer_lon_deg: float = WARSAW_LON_DEG,
    observer_elevation_m: float = WARSAW_ELEVATION_M,
    when_utc: Optional[datetime] = None,
    num_points: int = 361,
):
    instant = _coerce_utc_datetime(when_utc)

    location = EarthLocation(
        lat=observer_lat_deg * u.deg,
        lon=observer_lon_deg * u.deg,
        height=observer_elevation_m * u.m,
    )
    obs_time = Time(instant)
    sun_altaz = get_sun(obs_time).transform_to(AltAz(obstime=obs_time, location=location))
    sun_alt_deg = float(sun_altaz.alt.deg)
    sun_az_deg = float(sun_altaz.az.deg)

    sun_vec = _normalize_vector(_altaz_to_enu_vector(sun_alt_deg, sun_az_deg))
    ref = (0.0, 0.0, 1.0)
    u_vec = _cross(sun_vec, ref)
    u_norm = math.sqrt(u_vec[0] * u_vec[0] + u_vec[1] * u_vec[1] + u_vec[2] * u_vec[2])
    if u_norm < 1e-8:
        ref = (1.0, 0.0, 0.0)
        u_vec = _cross(sun_vec, ref)
    u_vec = _normalize_vector(u_vec)
    w_vec = _normalize_vector(_cross(sun_vec, u_vec))

    points = []
    previous_az = None
    for i in range(num_points):
        t = 2.0 * math.pi * (i / (num_points - 1))
        vec = (
            u_vec[0] * math.cos(t) + w_vec[0] * math.sin(t),
            u_vec[1] * math.cos(t) + w_vec[1] * math.sin(t),
            u_vec[2] * math.cos(t) + w_vec[2] * math.sin(t),
        )
        alt_deg, az_deg = _enu_vector_to_altaz(vec)

        if previous_az is not None and abs(az_deg - previous_az) > 180.0:
            points.append({"az_deg": None, "alt_deg": None})
        points.append({"az_deg": az_deg, "alt_deg": alt_deg})
        previous_az = az_deg

    return {
        "sun_alt_deg": sun_alt_deg,
        "sun_az_deg": sun_az_deg,
        "computed_at_utc": instant,
        "curve_points": points,
    }


def _equatorial_vector_to_ra_dec(vec):
    x, y, z = _normalize_vector(vec)
    dec_deg = math.degrees(math.asin(z))
    ra_deg = math.degrees(math.atan2(y, x)) % 360.0
    return ra_deg, dec_deg


def sun_visibility_curve_ha_dec(
    observer_lat_deg: float = WARSAW_LAT_DEG,
    observer_lon_deg: float = WARSAW_LON_DEG,
    observer_elevation_m: float = WARSAW_ELEVATION_M,
    when_utc: Optional[datetime] = None,
    num_points: int = 361,
):
    instant = _coerce_utc_datetime(when_utc)

    location = EarthLocation(
        lat=observer_lat_deg * u.deg,
        lon=observer_lon_deg * u.deg,
        height=observer_elevation_m * u.m,
    )
    obs_time = Time(instant)
    lst_hours = float(obs_time.sidereal_time("apparent", longitude=location.lon).hour)

    sun_icrs = get_sun(obs_time).icrs
    sun_ra_deg = float(sun_icrs.ra.deg)
    sun_dec_deg = float(sun_icrs.dec.deg)
    sun_ra_rad = math.radians(sun_ra_deg)
    sun_dec_rad = math.radians(sun_dec_deg)
    sun_vec = _normalize_vector((
        math.cos(sun_dec_rad) * math.cos(sun_ra_rad),
        math.cos(sun_dec_rad) * math.sin(sun_ra_rad),
        math.sin(sun_dec_rad),
    ))

    ref = (0.0, 0.0, 1.0)
    u_vec = _cross(sun_vec, ref)
    u_norm = math.sqrt(u_vec[0] * u_vec[0] + u_vec[1] * u_vec[1] + u_vec[2] * u_vec[2])
    if u_norm < 1e-8:
        ref = (1.0, 0.0, 0.0)
        u_vec = _cross(sun_vec, ref)
    u_vec = _normalize_vector(u_vec)
    w_vec = _normalize_vector(_cross(sun_vec, u_vec))

    points = []
    previous_ha = None
    for i in range(num_points):
        t = 2.0 * math.pi * (i / (num_points - 1))
        vec = (
            u_vec[0] * math.cos(t) + w_vec[0] * math.sin(t),
            u_vec[1] * math.cos(t) + w_vec[1] * math.sin(t),
            u_vec[2] * math.cos(t) + w_vec[2] * math.sin(t),
        )
        ra_deg, dec_deg = _equatorial_vector_to_ra_dec(vec)
        ha_hours = (lst_hours - (ra_deg / 15.0)) % 24.0

        if previous_ha is not None and abs(ha_hours - previous_ha) > 12.0:
            points.append({"ha_hours": None, "dec_deg": None})
        points.append({"ha_hours": ha_hours, "dec_deg": dec_deg})
        previous_ha = ha_hours

    sun_ha_hours = (lst_hours - (sun_ra_deg / 15.0)) % 24.0
    return {
        "sun_ha_hours": sun_ha_hours,
        "sun_dec_deg": sun_dec_deg,
        "computed_at_utc": instant,
        "curve_points": points,
    }
