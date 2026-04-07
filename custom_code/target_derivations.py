from __future__ import annotations

import logging
from typing import Dict

import astropy.units as u
from astropy.coordinates import FK5, SkyCoord, get_constellation
from astropy.time import Time
from numpy import around
from tom_targets.models import Target


logger = logging.getLogger(__name__)


def _sidereal_coordinate_frame(epoch) -> FK5 | str:
    if epoch in (None, ''):
        return 'icrs'

    try:
        return FK5(equinox=Time(float(epoch), format='jyear'))
    except (TypeError, ValueError):
        logger.warning('Invalid target epoch for derived coordinate calculation: %s', epoch)
        return 'icrs'


def derive_sidereal_target_fields(target: Target) -> Dict[str, object]:
    if getattr(target, 'type', None) != Target.SIDEREAL:
        return {}

    if target.ra is None or target.dec is None:
        return {}

    coords = SkyCoord(
        ra=float(target.ra) * u.deg,
        dec=float(target.dec) * u.deg,
        frame=_sidereal_coordinate_frame(getattr(target, 'epoch', None)),
    )
    galactic = coords.galactic

    return {
        'galactic_lng': float(around(galactic.l.degree, 6)),
        'galactic_lat': float(around(galactic.b.degree, 6)),
        'constellation': get_constellation(coords, short_name=False),
    }


def refresh_target_derived_fields(target_id: int) -> None:
    try:
        target = Target.objects.get(pk=target_id)
    except Target.DoesNotExist:
        return

    updates = derive_sidereal_target_fields(target)
    if not updates:
        return

    Target.objects.filter(pk=target_id).update(**updates)
