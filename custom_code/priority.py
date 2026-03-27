import math
from datetime import datetime, timezone

from astropy.time import Time
from numpy import around

from tom_targets.models import Target


def _to_float_or(value, default):
    try:
        val = float(value)
        if math.isfinite(val):
            return val
    except (TypeError, ValueError):
        pass
    return default


def _compute_priority(dt, importance, cadence):
    if cadence == 0:
        return 0.0
    return float(around((dt / cadence) * importance, 1))


def compute_target_priority_values(target: Target):
    imp = _to_float_or(getattr(target, 'importance', 0.0), 1.0)
    cadence = _to_float_or(getattr(target, 'cadence', 0.0), 1.0)
    mjd_last = _to_float_or(getattr(target, 'mjd_last', 0.0), 0.0)

    mjd_now = Time(datetime.now(timezone.utc)).mjd
    try:
        dt = float(mjd_now - mjd_last)
    except (TypeError, ValueError):
        dt = 10.0

    priority = _compute_priority(dt, imp, cadence)
    cadence_priority = _compute_priority(dt, imp, cadence)
    return priority, cadence_priority


def refresh_target_priority(target_id: int) -> None:
    target = Target.objects.filter(pk=target_id).only('mjd_last', 'importance', 'cadence').first()
    if target is None:
        return

    priority, cadence_priority = compute_target_priority_values(target)
    Target.objects.filter(pk=target_id).update(
        priority=priority,
        cadence_priority=cadence_priority,
    )
