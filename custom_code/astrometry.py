from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time
from tom_targets.models import Target


def can_compute_current_coordinates(target: Target) -> bool:
    parallax = getattr(target, 'parallax', None)
    parallax_error = getattr(target, 'parallax_error', None)
    pm_ra = getattr(target, 'pm_ra', None)
    pm_dec = getattr(target, 'pm_dec', None)

    if target.ra is None or target.dec is None:
        return False
    if parallax in (None, '') or parallax_error in (None, ''):
        return False
    if pm_ra in (None, '') or pm_dec in (None, ''):
        return False

    try:
        parallax = float(parallax)
        parallax_error = float(parallax_error)
        float(pm_ra)
        float(pm_dec)
    except (TypeError, ValueError):
        return False

    if parallax <= 0.0 or parallax_error <= 0.0:
        return False

    return (parallax / parallax_error) > 2.0


def compute_current_coordinates(target: Target, now: datetime | Time | None = None) -> Dict[str, Any]:
    if not can_compute_current_coordinates(target):
        raise ValueError('Target does not have sufficient Gaia astrometry to compute current coordinates.')

    epoch = getattr(target, 'epoch', None)
    try:
        epoch_value = 2000.0 if epoch in (None, '') else float(epoch)
    except (TypeError, ValueError):
        epoch_value = 2000.0
    reference_time = Time(epoch_value, format='jyear')
    current_time = now if isinstance(now, Time) else Time(now or datetime.now(timezone.utc), scale='utc')

    source = SkyCoord(
        ra=float(target.ra) * u.deg,
        dec=float(target.dec) * u.deg,
        pm_ra_cosdec=float(target.pm_ra) * u.mas / u.yr,
        pm_dec=float(target.pm_dec) * u.mas / u.yr,
        distance=(1000.0 / float(target.parallax)) * u.pc,
        obstime=reference_time,
        frame='icrs',
    )
    propagated = source.apply_space_motion(new_obstime=current_time)

    return {
        'ra_deg': float(propagated.ra.deg),
        'dec_deg': float(propagated.dec.deg),
        'ra_hms': propagated.ra.to_string(unit=u.hourangle, sep=':', precision=2, pad=True),
        'dec_dms': propagated.dec.to_string(unit=u.deg, sep=':', precision=2, alwayssign=True, pad=True),
        'computed_at_utc': current_time.to_datetime(timezone=timezone.utc),
        'reference_epoch': epoch_value,
    }
