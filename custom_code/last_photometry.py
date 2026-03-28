import math
from datetime import timezone
from typing import Tuple

from astropy.time import Time

from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target


I_FILTER_TOKENS = ("i(", "(I)", "(zi)", "(i)")
R_FILTER_TOKENS = ("r(", "(R)", "(zr)", "(r)")
G_FILTER_TOKENS = ("g(", "(zg)", "(g)")
V_FILTER_TOKENS = ("V(", "(V)")
B_FILTER_TOKENS = ("B(", "(B)")
U_FILTER_TOKENS = ("U(", "(U)")
G_REF_FILTER_TOKENS = ("G(", "(G)", "g(Gaia)")
IGNORE_FILTERS = {"WISE(W1)", "WISE(W2)", "GALEX(NUV)", "GALEX(FUV)", "LAT(>100MeV)", "LAT(>800MeV)"}
IGNORE_FILTER_PREFIXES = ("UVOT(UVW", "UVOT(UVM")


def _is_finite_number(value) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _extract_mag_and_filter(datum: ReducedDatum) -> Tuple[float, str]:
    value = datum.value
    if isinstance(value, dict):
        mag = value.get("magnitude")
        if mag is None:
            mag = value.get("mag")
        datum_filter = value.get("filter") or ""
    else:
        mag = value
        datum_filter = ""
    return mag, str(datum_filter or "")


def _extract_mjd(datum: ReducedDatum):
    if _is_finite_number(getattr(datum, "mjd", None)):
        return float(datum.mjd)
    timestamp = getattr(datum, "timestamp", None)
    if not timestamp:
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return float(Time(timestamp, scale="utc").mjd)


def _should_ignore_filter(datum_filter: str) -> bool:
    normalized_filter = str(datum_filter or "").strip()
    return normalized_filter in IGNORE_FILTERS or normalized_filter.startswith(IGNORE_FILTER_PREFIXES)


def compute_last_photometry_values(target_id: int) -> Tuple[float, float, str]:
    datums = ReducedDatum.objects.filter(target_id=target_id, data_type="photometry").order_by("timestamp")
    if not datums.exists():
        return 99.0, 0.0, ""

    last_mjd = 0.0
    last_mag = 100.0
    last_filter = ""

    mean_i = 0.0
    mean_r = 0.0
    mean_g = 0.0
    mean_v = 0.0
    mean_b = 0.0
    mean_u = 0.0
    n_i = 0
    n_r = 0
    n_g = 0
    n_v = 0
    n_b = 0
    n_u = 0

    for datum in datums:
        mag, datum_filter = _extract_mag_and_filter(datum)
        mjd = _extract_mjd(datum)
        value_ok = _is_finite_number(mag)
        mjd_ok = _is_finite_number(mjd)

        if value_ok and mjd_ok and not _should_ignore_filter(datum_filter) and float(mjd) > last_mjd:
            last_mjd = float(mjd)
            last_mag = float(mag)
            last_filter = datum_filter

        if not value_ok:
            continue

        if any(token in datum_filter for token in I_FILTER_TOKENS):
            mean_i += float(mag)
            n_i += 1
        if any(token in datum_filter for token in R_FILTER_TOKENS):
            mean_r += float(mag)
            n_r += 1
        if any(token in datum_filter for token in G_FILTER_TOKENS):
            mean_g += float(mag)
            n_g += 1
        if any(token in datum_filter for token in V_FILTER_TOKENS):
            mean_v += float(mag)
            n_v += 1
        if any(token in datum_filter for token in B_FILTER_TOKENS):
            mean_b += float(mag)
            n_b += 1
        if any(token in datum_filter for token in U_FILTER_TOKENS):
            mean_u += float(mag)
            n_u += 1

    if n_i:
        mean_i /= n_i
    if n_r:
        mean_r /= n_r
    if n_g:
        mean_g /= n_g
    if n_v:
        mean_v /= n_v
    if n_b:
        mean_b /= n_b
    if n_u:
        mean_u /= n_u

    return_mag = last_mag
    approxsign = last_filter

    if any(token in last_filter for token in G_REF_FILTER_TOKENS) or any(
        token in last_filter for token in R_FILTER_TOKENS
    ):
        return_mag = last_mag
        approxsign = "Gaia/r"
    else:
        if mean_r != 0 and mean_v != 0 and any(token in last_filter for token in V_FILTER_TOKENS):
            return_mag = last_mag - (mean_v - mean_r)
            approxsign = "~G"
        if mean_g != 0 and mean_r != 0 and any(token in last_filter for token in G_FILTER_TOKENS):
            return_mag = last_mag - (mean_g - mean_r)
            approxsign = "~G"
        if mean_i != 0 and mean_r != 0 and any(token in last_filter for token in I_FILTER_TOKENS):
            return_mag = last_mag - (mean_i - mean_r)
            approxsign = "~G"
        if mean_g != 0 and mean_i != 0 and any(token in last_filter for token in G_FILTER_TOKENS):
            return_mag = last_mag - (mean_g - mean_i) / 2.0
            approxsign = "~G"
        if mean_b != 0 and mean_r != 0 and any(token in last_filter for token in B_FILTER_TOKENS):
            return_mag = last_mag - (mean_b - mean_r)
            approxsign = "~G"
        if mean_u != 0 and mean_g != 0 and any(token in last_filter for token in U_FILTER_TOKENS):
            return_mag = last_mag - (mean_u - mean_g)
            approxsign = "~G"
        if mean_u != 0 and mean_r != 0 and any(token in last_filter for token in U_FILTER_TOKENS):
            return_mag = last_mag - (mean_u - mean_r)
            approxsign = "~G"

    return round(float(return_mag), 1), round(float(last_mjd), 8), approxsign


def refresh_target_last_photometry(target_id: int) -> None:
    mag_last, mjd_last, filter_last = compute_last_photometry_values(target_id)
    Target.objects.filter(pk=target_id).update(
        mag_last=mag_last,
        mjd_last=mjd_last,
        filter_last=(filter_last or "")[:20],
    )
