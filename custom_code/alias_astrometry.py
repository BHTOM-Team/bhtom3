"""Matched positions and angular offsets for target aliases.

Positions are in degrees. Errors are 1-sigma tangent-plane errors in arcseconds
(RA already includes cos(dec)); the target's stored coordinates are the fixed
reference. Photometric errors and search radii are never positional errors.
"""
import math


ARCSEC_PER_RADIAN = 180.0 * 3600.0 / math.pi


def _number(value):
    if isinstance(value, bool) or getattr(value, 'mask', False):
        return None
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) else None


def matched_position(ra, dec, ra_error_arcsec=None, dec_error_arcsec=None, ra_dec_corr=None):
    ra, dec = _number(ra), _number(dec)
    if ra is None or dec is None or not 0 <= ra <= 360 or not -90 <= dec <= 90:
        return {}
    position = {'ra': ra % 360, 'dec': dec}
    for key, value in (('ra_error_arcsec', ra_error_arcsec), ('dec_error_arcsec', dec_error_arcsec)):
        error = _number(value)
        if error is not None and error >= 0:
            position[key] = error
    correlation = _number(ra_dec_corr)
    if correlation is not None and -1 <= correlation <= 1:
        position['ra_dec_corr'] = correlation
    return position


def clean_matched_position(value):
    if not isinstance(value, dict):
        return {}
    position = matched_position(**{key: value.get(key) for key in (
        'ra', 'dec', 'ra_error_arcsec', 'dec_error_arcsec', 'ra_dec_corr',
    )})
    if position and value.get('error_kind') == 'scatter':
        position['error_kind'] = 'scatter'
    return position


def _separation_and_direction(ra1, dec1, ra2, dec2):
    """Great-circle distance and unit tangent at the first position towards the second."""
    delta_ra = math.radians((ra2 - ra1 + 180) % 360 - 180)
    dec1, dec2 = math.radians(dec1), math.radians(dec2)
    east = math.cos(dec2) * math.sin(delta_ra)
    north = math.cos(dec1) * math.sin(dec2) - math.sin(dec1) * math.cos(dec2) * math.cos(delta_ra)
    norm = math.hypot(east, north)
    dot = math.sin(dec1) * math.sin(dec2) + math.cos(dec1) * math.cos(dec2) * math.cos(delta_ra)
    separation = math.atan2(norm, dot) * ARCSEC_PER_RADIAN
    return separation, east / norm if norm else 0.0, north / norm if norm else 0.0


def position_from_error_ellipse(ra, dec, major_mas, minor_mas, angle_deg):
    major, minor, angle = _number(major_mas), _number(minor_mas), _number(angle_deg)
    if major is None or minor is None or angle is None or major < 0 or minor < 0:
        return matched_position(ra, dec)
    sine, cosine = math.sin(math.radians(angle)), math.cos(math.radians(angle))
    major, minor = major / 1000, minor / 1000
    var_e = (major * sine)**2 + (minor * cosine)**2
    var_n = (major * cosine)**2 + (minor * sine)**2
    covariance = (major**2 - minor**2) * sine * cosine
    correlation = covariance / math.sqrt(var_e * var_n) if var_e * var_n else 0.0
    return matched_position(ra, dec, math.sqrt(var_e), math.sqrt(var_n), max(-1.0, min(1.0, correlation)))


def position_from_samples(samples):
    """Spherical centroid with sample scatter, not error on the mean.

    Callers should deduplicate multiband copies of the same positional observation.
    Repeated identical catalog coordinates do not establish a positional error.
    """
    positions = [p for ra, dec in samples if (p := matched_position(ra, dec))]
    if not positions:
        return {}
    vectors = [(
        math.cos(math.radians(p['dec'])) * math.cos(math.radians(p['ra'])),
        math.cos(math.radians(p['dec'])) * math.sin(math.radians(p['ra'])),
        math.sin(math.radians(p['dec'])),
    ) for p in positions]
    x, y, z = (math.fsum(v[i] for v in vectors) for i in range(3))
    if math.sqrt(x*x + y*y + z*z) < 1e-12:
        return {}
    ra = math.degrees(math.atan2(y, x)) % 360
    dec = math.degrees(math.atan2(z, math.hypot(x, y)))
    position = matched_position(ra, dec)
    if len({(p['ra'], p['dec']) for p in positions}) < 2:
        return position
    offsets = [_separation_and_direction(ra, dec, p['ra'], p['dec']) for p in positions]
    east = [distance * e for distance, e, _n in offsets]
    north = [distance * n for distance, _e, n in offsets]
    mean_e, mean_n = math.fsum(east) / len(east), math.fsum(north) / len(north)
    var_e = math.fsum((e - mean_e)**2 for e in east) / (len(east) - 1)
    var_n = math.fsum((n - mean_n)**2 for n in north) / (len(north) - 1)
    covariance = math.fsum((e - mean_e) * (n - mean_n) for e, n in zip(east, north)) / (len(east) - 1)
    position.update(ra_error_arcsec=math.sqrt(var_e), dec_error_arcsec=math.sqrt(var_n), error_kind='scatter')
    if var_e > 0 and var_n > 0:
        position['ra_dec_corr'] = max(-1.0, min(1.0, covariance / math.sqrt(var_e * var_n)))
    return position


def alias_offset(target_ra, target_dec, position):
    reference = matched_position(target_ra, target_dec)
    position = clean_matched_position(position)
    if not reference or not position:
        return {'offset_arcsec': None, 'offset_error_arcsec': None, 'offset_description': 'Matched coordinates unavailable.'}
    distance, east, north = _separation_and_direction(position['ra'], position['dec'], reference['ra'], reference['dec'])
    error = None
    description = 'Angular separation from the stored target coordinates; positional uncertainty unavailable.'
    if 'ra_error_arcsec' in position and 'dec_error_arcsec' in position:
        ra_error, dec_error = position['ra_error_arcsec'], position['dec_error_arcsec']
        if distance < 1e-8:
            # There is no radial direction at coincidence; show the total positional scale.
            variance = ra_error**2 + dec_error**2
        else:
            covariance = position.get('ra_dec_corr', 0.0) * ra_error * dec_error
            variance = (east * ra_error)**2 + (north * dec_error)**2 + 2 * east * north * covariance
        error = math.sqrt(max(0.0, variance))
        kind = '1σ scatter of measured positions' if position.get('error_kind') == 'scatter' else 'propagated 1σ catalog position error'
        description = f'Angular separation from the stored target coordinates; {kind}. Target coordinates treated as fixed.'
        if distance < 1e-8:
            description += ' At zero offset the error is the quadrature sum of the RA and Dec errors.'
    precision = max(3, min(8, 1 - math.floor(math.log10(error)))) if error else 3
    return {'offset_arcsec': distance, 'offset_error_arcsec': error,
            'offset_description': description, 'offset_precision': precision}
