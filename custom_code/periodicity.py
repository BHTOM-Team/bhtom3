"""Lomb-Scargle period search on one dataset, with a separate zero-point per band.

A dataset is one filter, but its telescopes rarely agree exactly: different calibrations,
apertures and colour terms leave constant offsets between them. Concatenated, those offsets
look like variability and the periodogram locks onto *when each telescope observed* instead
of the source. Points are therefore grouped into bands (e.g. one per telescope); each band
has its error-weighted mean subtracted before a single periodogram is computed over all
points, so the bands share one period and phase.

Other datasets (other filters, Fermi-LAT fluxes) are not searched together: the periodicity
page folds and fits them at the period found here.
"""
import numpy as np
from astropy.timeseries import LombScargle


MIN_POINTS_TOTAL = 5
MIN_POINTS_PER_BAND = 3
SAMPLES_PER_PEAK = 5
MAX_FREQUENCIES = 5_000_000
MAX_RETURNED_POINTS = 15000


class PeriodSearchError(ValueError):
    """Invalid input for a period search; the message is safe to show to users."""


def _fill_errors(errors):
    """Replace missing/non-positive errors by the band's median valid error; None if it has none."""
    valid = np.isfinite(errors) & (errors > 0)
    if not valid.any():
        return None
    filled = errors.copy()
    filled[~valid] = np.median(errors[valid])
    return filled


def _downsample_keeping_peaks(periods, power, max_points):
    """Thin the periodogram for the browser, keeping the highest point of every block."""
    n = len(power)
    if n <= max_points:
        return periods, power
    block = -(-n // max_points)
    idx = np.array([start + int(np.argmax(power[start:start + block])) for start in range(0, n, block)])
    return periods[idx], power[idx]


def search_period(times, values, errors=None, bands=None, min_period=0.1, max_period=1000.0):
    """Lomb-Scargle periodogram of one dataset.

    times, values, errors and bands are parallel sequences (errors and bands optional); points
    in the same band share a zero-point. Returns a JSON-serialisable dict; raises
    PeriodSearchError on bad input.
    """
    try:
        times = np.asarray(times, dtype=float)
        values = np.asarray(values, dtype=float)
        errors = np.full(len(times), np.nan) if errors is None else np.asarray(errors, dtype=float)
    except (TypeError, ValueError) as exc:
        raise PeriodSearchError(f'Non-numeric input: {exc}') from exc
    bands = np.array(['all'] * len(times) if bands is None else [str(b) for b in bands], dtype=object)

    if not (len(times) == len(values) == len(errors) == len(bands)):
        raise PeriodSearchError('times, values, errors and bands must have the same length')
    if min_period <= 0 or min_period >= max_period:
        raise PeriodSearchError('min_period must be positive and less than max_period')

    finite = np.isfinite(times) & np.isfinite(values)
    times, values, errors, bands = times[finite], values[finite], errors[finite], bands[finite]

    band_names = sorted(set(bands))
    excluded = [name for name in band_names if np.count_nonzero(bands == name) < MIN_POINTS_PER_BAND]
    keep = ~np.isin(bands, excluded)
    times, values, errors, bands = times[keep], values[keep], errors[keep], bands[keep]
    band_names = [name for name in band_names if name not in excluded]

    n = len(times)
    if n < MIN_POINTS_TOTAL:
        raise PeriodSearchError(
            f'Need at least {MIN_POINTS_TOTAL} data points in bands with >= {MIN_POINTS_PER_BAND} points (got {n})'
        )

    t_ref = float(times.min())
    t = times - t_ref
    baseline = float(t.max())
    if baseline <= 0:
        raise PeriodSearchError('All data points have the same time; cannot search for a period')

    # Periods longer than twice the baseline are unconstrained.
    max_period = max(min(max_period, 2.0 * baseline), min_period * 2)
    min_frequency, max_frequency = 1.0 / max_period, 1.0 / min_period
    n_frequencies = (max_frequency - min_frequency) * SAMPLES_PER_PEAK * baseline
    if n_frequencies > MAX_FREQUENCIES:
        raise PeriodSearchError(
            f'Search grid too large (~{n_frequencies:,.0f} frequencies); increase min period or shorten the time range'
        )

    centred = np.empty(n)
    dy = np.empty(n)
    for name in band_names:
        member = bands == name
        y = values[member]
        band_errors = _fill_errors(errors[member])
        if band_errors is None:
            # A band without errors is weighted by its own scatter.
            scatter = float(np.std(y))
            band_errors = np.full(len(y), scatter if scatter > 0 else 1.0)
        centred[member] = y - np.average(y, weights=band_errors ** -2)
        dy[member] = band_errors

    ls = LombScargle(t, centred, dy)
    frequency = ls.autofrequency(
        samples_per_peak=SAMPLES_PER_PEAK,
        minimum_frequency=min_frequency,
        maximum_frequency=max_frequency,
    )
    power = np.nan_to_num(ls.power(frequency))

    fap_levels = [None, None, None]
    try:
        fap_levels = [float(level) for level in ls.false_alarm_level(
            [0.1, 0.01, 0.001],
            minimum_frequency=min_frequency,
            maximum_frequency=max_frequency,
            samples_per_peak=SAMPLES_PER_PEAK,
        )]
    except Exception:
        pass

    best_frequency = float(frequency[int(np.argmax(power))])
    periods, power = _downsample_keeping_peaks(1.0 / frequency, power, MAX_RETURNED_POINTS)

    return {
        'periods': periods.tolist(),
        'powers': power.tolist(),
        'best_period': 1.0 / best_frequency,
        'fap_10': fap_levels[0],
        'fap_1': fap_levels[1],
        'fap_01': fap_levels[2],
        't_ref': t_ref,
        'n_points': n,
        'bands': [{'name': name, 'n_points': int(np.count_nonzero(bands == name))} for name in band_names],
        'excluded_bands': excluded,
    }
