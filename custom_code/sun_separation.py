from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import math
from typing import Optional

from astropy import units as u
from astropy.coordinates import SkyCoord, get_body
from astropy.time import Time
from numpy import around

from tom_targets.models import Target

logger = logging.getLogger(__name__)

# Gaussian gravitational constant squared in AU^3/day^2 (solar GM units).
MU_SUN_AU3_PER_DAY2 = 0.01720209895 ** 2
# Mean obliquity of the ecliptic (J2000), degrees.
OBLIQUITY_DEG = 23.4392911


@dataclass
class OrbitalElements:
    a_au: float
    e: float
    i_rad: float
    omega_rad: float
    node_rad: float
    mean_anomaly0_rad: Optional[float] = None
    epoch_mjd: Optional[float] = None
    tp_mjd: Optional[float] = None
    mean_motion_rad_per_day: Optional[float] = None


def _to_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_angle_rad(angle):
    return angle % (2.0 * math.pi)


def _solve_kepler_elliptic(m_rad, e):
    e_anom = m_rad
    for _ in range(25):
        f = e_anom - e * math.sin(e_anom) - m_rad
        fp = 1.0 - e * math.cos(e_anom)
        if abs(fp) < 1e-12:
            break
        step = f / fp
        e_anom -= step
        if abs(step) < 1e-12:
            break
    return e_anom


def _solve_kepler_hyperbolic(m_h, e):
    h_anom = math.asinh(m_h / max(e, 1.0000001))
    for _ in range(35):
        f = e * math.sinh(h_anom) - h_anom - m_h
        fp = e * math.cosh(h_anom) - 1.0
        if abs(fp) < 1e-12:
            break
        step = f / fp
        h_anom -= step
        if abs(step) < 1e-12:
            break
    return h_anom


def _build_elements_from_target(target) -> Optional[OrbitalElements]:
    scheme = target.scheme or ""
    e = _to_float(target.eccentricity)
    i = _to_float(target.inclination)
    omega = _to_float(target.arg_of_perihelion)
    node = _to_float(target.lng_asc_node)
    if any(v is None for v in (e, i, omega, node)):
        return None

    mean_anomaly = _to_float(target.mean_anomaly)
    common = {
        "e": e,
        "i_rad": math.radians(i),
        "omega_rad": math.radians(omega),
        "node_rad": math.radians(node),
        "epoch_mjd": _to_float(target.epoch_of_elements),
        "mean_anomaly0_rad": (None if mean_anomaly is None else math.radians(mean_anomaly)),
    }

    if scheme in ("MPC_MINOR_PLANET", "JPL_MAJOR_PLANET"):
        a = _to_float(target.semimajor_axis)
        if a is None:
            return None
        mean_motion = _to_float(target.mean_daily_motion)
        mean_motion_rad_per_day = (
            math.radians(mean_motion) if mean_motion is not None else None
        )
        return OrbitalElements(
            a_au=a,
            mean_motion_rad_per_day=mean_motion_rad_per_day,
            tp_mjd=None,
            **common,
        )

    if scheme == "MPC_COMET":
        q = _to_float(target.perihdist)
        if q is None:
            return None
        if abs(1.0 - e) < 1e-8:
            # Parabolic case needs Barker equation; skip safely for now.
            return None
        a = q / (1.0 - e)
        return OrbitalElements(
            a_au=a,
            mean_motion_rad_per_day=None,
            tp_mjd=_to_float(target.epoch_of_perihelion),
            **common,
        )

    return None


def _mean_motion_rad_per_day(elements: OrbitalElements):
    if elements.mean_motion_rad_per_day is not None:
        return elements.mean_motion_rad_per_day
    a = elements.a_au
    if a > 0:
        return math.sqrt(MU_SUN_AU3_PER_DAY2 / (a ** 3))
    return math.sqrt(MU_SUN_AU3_PER_DAY2 / ((-a) ** 3))


def _true_anomaly_and_radius(elements: OrbitalElements, tt: Time):
    e = elements.e
    a = elements.a_au
    n = _mean_motion_rad_per_day(elements)
    t_mjd = tt.mjd

    if elements.tp_mjd is not None:
        dt_days = t_mjd - elements.tp_mjd
        m_raw = n * dt_days
    else:
        if elements.epoch_mjd is None or elements.mean_anomaly0_rad is None:
            return None
        dt_days = t_mjd - elements.epoch_mjd
        m_raw = elements.mean_anomaly0_rad + n * dt_days

    if e < 1.0:
        m = _normalize_angle_rad(m_raw)
        e_anom = _solve_kepler_elliptic(m, e)
        cos_e = math.cos(e_anom)
        sin_e = math.sin(e_anom)
        r = a * (1.0 - e * cos_e)
        factor = math.sqrt(max(0.0, 1.0 - e * e))
        nu = math.atan2(factor * sin_e, cos_e - e)
        return nu, r

    if e > 1.0:
        m_h = m_raw
        h_anom = _solve_kepler_hyperbolic(m_h, e)
        cosh_h = math.cosh(h_anom)
        r = a * (1.0 - e * cosh_h)
        r = abs(r)
        num = math.sqrt(e + 1.0) * math.sinh(h_anom / 2.0)
        den = math.sqrt(e - 1.0) * math.cosh(h_anom / 2.0)
        nu = 2.0 * math.atan2(num, den)
        return nu, r

    return None


def _heliocentric_ecliptic_xyz(elements: OrbitalElements, tt: Time):
    nu_r = _true_anomaly_and_radius(elements, tt)
    if nu_r is None:
        return None
    nu, r = nu_r

    cos_o = math.cos(elements.node_rad)
    sin_o = math.sin(elements.node_rad)
    cos_i = math.cos(elements.i_rad)
    sin_i = math.sin(elements.i_rad)
    cos_wv = math.cos(elements.omega_rad + nu)
    sin_wv = math.sin(elements.omega_rad + nu)

    x = r * (cos_o * cos_wv - sin_o * sin_wv * cos_i)
    y = r * (sin_o * cos_wv + cos_o * sin_wv * cos_i)
    z = r * (sin_wv * sin_i)
    return x, y, z


def _ecliptic_to_equatorial_j2000(x, y, z):
    eps = math.radians(OBLIQUITY_DEG)
    cos_e = math.cos(eps)
    sin_e = math.sin(eps)
    x_eq = x
    y_eq = y * cos_e - z * sin_e
    z_eq = y * sin_e + z * cos_e
    return x_eq, y_eq, z_eq


def _non_sidereal_ra_dec_now(target, tt: Time):
    elements = _build_elements_from_target(target)
    if elements is None:
        return None
    obj_xyz_ecl = _heliocentric_ecliptic_xyz(elements, tt)
    if obj_xyz_ecl is None:
        return None

    obj_x, obj_y, obj_z = _ecliptic_to_equatorial_j2000(*obj_xyz_ecl)

    earth_bary = get_body("earth", tt).cartesian
    sun_bary = get_body("sun", tt).cartesian
    earth_helio = earth_bary - sun_bary
    ex = earth_helio.x.to(u.au).value
    ey = earth_helio.y.to(u.au).value
    ez = earth_helio.z.to(u.au).value

    gx = obj_x - ex
    gy = obj_y - ey
    gz = obj_z - ez

    geocentric = SkyCoord(
        x=gx * u.au,
        y=gy * u.au,
        z=gz * u.au,
        representation_type="cartesian",
        frame="icrs",
    )
    spherical = geocentric.spherical
    return float(spherical.lon.deg), float(spherical.lat.deg)


def compute_sun_separation(ra, dec, time_to_compute: Optional[Time] = None) -> float:
    """
    Compute Sun-target angular separation in degrees for current UTC time.
    Rounds to integer degrees to match historical bhtom2/cpcsv2 behavior.
    """
    tt = time_to_compute or Time(datetime.now(timezone.utc))
    sun_pos = get_body("sun", tt)
    obj_pos = SkyCoord(ra=ra * u.deg, dec=dec * u.deg, frame="icrs")
    obj_in_sun_frame = obj_pos.transform_to(sun_pos.frame)
    return float(around(sun_pos.separation(obj_in_sun_frame).deg, 0))


def _resolve_target_coordinates_now(target, time_to_compute: Optional[Time] = None):
    tt = time_to_compute or Time(datetime.now(timezone.utc))

    if target.type != Target.NON_SIDEREAL:
        if target.ra is None or target.dec is None:
            return None
        return float(target.ra), float(target.dec)

    try:
        coords = _non_sidereal_ra_dec_now(target, tt)
        if coords is not None:
            return coords
    except Exception:
        logger.exception(
            "Local non-sidereal ephemeris propagation failed for target %s.",
            target.name,
        )

    # Fallback to stored static coordinates if available.
    if target.ra is not None and target.dec is not None:
        logger.warning(
            "Could not resolve local ephemeris coordinates for non-sidereal target %s; "
            "falling back to stored RA/Dec.",
            target.name,
        )
        return float(target.ra), float(target.dec)

    logger.warning(
        "Could not resolve coordinates for non-sidereal target %s; skipping sun separation refresh.",
        target.name,
    )
    return None


def get_live_target_values(target, time_to_compute: Optional[Time] = None):
    """
    Return display-ready coordinates and sun separation.

    For non-sidereal targets, coordinates are propagated locally from stored
    orbital elements for "now".
    For sidereal targets, stored RA/Dec and stored sun_separation are returned.
    """
    if target.type != Target.NON_SIDEREAL:
        return {
            "ra": target.ra,
            "dec": target.dec,
            "sun_separation": target.sun_separation,
            "computed_at_utc": None,
        }

    tt = time_to_compute or Time(datetime.now(timezone.utc))
    coordinates = _resolve_target_coordinates_now(target, time_to_compute=tt)
    if coordinates is None:
        return {
            "ra": target.ra,
            "dec": target.dec,
            "sun_separation": target.sun_separation,
            "computed_at_utc": None,
        }

    ra, dec = coordinates
    return {
        "ra": ra,
        "dec": dec,
        "sun_separation": compute_sun_separation(ra, dec, time_to_compute=tt),
        "computed_at_utc": tt.to_datetime(timezone=timezone.utc),
    }


def refresh_target_sun_separation(target_id: int) -> None:
    target = Target.objects.filter(pk=target_id).first()
    if target is None:
        return

    coordinates = _resolve_target_coordinates_now(target)
    if coordinates is None:
        return
    ra, dec = coordinates

    sun_separation = compute_sun_separation(ra, dec)
    Target.objects.filter(pk=target_id).update(sun_separation=sun_separation)
