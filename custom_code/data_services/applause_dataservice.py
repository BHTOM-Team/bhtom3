"""APPLAUSE DR4: historical photographic-plate photometry from German and associated archives.

APPLAUSE (Archives of Photographic PLates for Astronomical USE; Enke et al. 2024, DR4) holds
digitised plates of Hamburg, Bamberg (including its northern and southern sky patrols), Potsdam,
Tartu, Tautenburg and others, from about 1900 to the 1980s. Every scan was processed with source
extraction and photometric calibration against Gaia EDR3. The DR4 TAP service is queried by
position: applause_dr4.source_calib holds every calibrated detection, applause_dr4.exposure the
time of each exposure and applause_dr4.plate the emulsion and filter. A light curve is one point
per plate.

The magnitude stored is natmag, the calibrated magnitude in the plate's natural photometric
system, which DR4 defines as natmag = G_RP + C (G_BP - G_RP) with the plate's colour term C. The
BP and RP magnitudes DR4 also lists are natmag transformed with the star's catalogue colour, so
they are not independent measurements and are not stored. Plates are grouped into passband
classes by their colour term: photovisual-like (C < 1.2), blue photographic (1.2 <= C < 2.3) and
ultraviolet-sensitive (C >= 2.3).

Only detections within the calibrated magnitude range (phot_range_flags = 0) of plates with a
single exposure are used (a multi-exposure plate does not say which exposure a detection belongs
to); a plate scanned more than once contributes its best-measured detection. Bright stars
(V < ~9-10) are mostly saturated on the plates and have no calibrated magnitude.
"""

import io
import logging
import math
import warnings
from datetime import timezone

import numpy as np
import requests
from astropy.io.votable import parse_single_table
from astropy.time import Time
from erfa import ErfaWarning

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target

from custom_code.data_services.forms import APPLAUSEQueryForm
from custom_code.data_services.service_utils import resolve_query_coordinates, upsert_reduced_datums

logger = logging.getLogger(__name__)

APPLAUSE_PAGE_URL = 'https://www.plate-archive.org/'
APPLAUSE_TAP_URL = 'https://www.plate-archive.org/tap/sync'
APPLAUSE_DEFAULT_RADIUS_ARCSEC = 5.0
APPLAUSE_TAP_TIMEOUT = 120
APPLAUSE_MAX_MAG_ERROR = 0.5
# Probability (from the DR4 artefact classifier) that the detection is a real source.
APPLAUSE_MIN_MODEL_PREDICTION = 0.5
# Passband classes by plate colour term C (natmag = RP + C (BP - RP)).
APPLAUSE_PASSBANDS = ((1.2, 'pv'), (2.3, 'pg'), (math.inf, 'UV'))

APPLAUSE_ACKNOWLEDGEMENT = (
    'This research has made use of APPLAUSE (Archives of Photographic PLates for Astronomical USE), '
    'DR4, https://www.plate-archive.org. Funding for APPLAUSE has been provided by the Deutsche '
    'Forschungsgemeinschaft (DFG). The APPLAUSE archives comprise plate collections of Hamburg '
    'Observatory, Dr. Remeis Observatory Bamberg, Leibniz-Institut fuer Astrophysik Potsdam (AIP), '
    'Tartu Observatory, Thueringer Landessternwarte Tautenburg and further partners.'
)


def _to_float(value):
    if value is None or value is np.ma.masked:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _text(value):
    if value is None or value is np.ma.masked:
        return ''
    if isinstance(value, bytes):
        value = value.decode('utf-8', 'replace')
    return str(value).strip()


def _passband(color_term):
    for limit, label in APPLAUSE_PASSBANDS:
        if color_term < limit:
            return label
    return APPLAUSE_PASSBANDS[-1][1]


def _query_detections(ra, dec, radius_arcsec):
    """Calibrated detections within the radius, with exposure time and plate metadata."""
    # The alias 'e' is rejected by the DR4 ADQL translator, hence 'ex'.
    query = f"""
        SELECT s.source_id, s.plate_id, s.scan_id, s.solution_num, s.natmag, s.natmag_error,
               s.color_term, s.phot_range_flags, s.model_prediction, s.gaiaedr3_id, s.gaiaedr3_dist,
               DISTANCE(POINT('ICRS', s.ra_icrs, s.dec_icrs), POINT('ICRS', {ra}, {dec})) * 3600 AS separation_arcsec,
               ex.exposure_id, ex.jd_mid, ex.exptime, p.archive_id, p.emulsion, p.filter
        FROM applause_dr4.source_calib AS s
        JOIN applause_dr4.plate AS p ON p.plate_id = s.plate_id
        LEFT JOIN applause_dr4.exposure AS ex ON ex.plate_id = s.plate_id
        WHERE 1 = CONTAINS(POINT('ICRS', s.ra_icrs, s.dec_icrs), CIRCLE('ICRS', {ra}, {dec}, {radius_arcsec / 3600.0}))
    """
    response = requests.post(
        APPLAUSE_TAP_URL, data={'REQUEST': 'doQuery', 'LANG': 'ADQL', 'QUERY': query}, timeout=APPLAUSE_TAP_TIMEOUT,
    )
    response.raise_for_status()
    table = parse_single_table(io.BytesIO(response.content), verify='ignore').to_table(use_names_over_ids=True)
    return [{name: row[name] for name in table.colnames} for row in table]


def _select_points(rows):
    """One detection per single-exposure plate: calibrated, in range, nearest the target, best error."""
    exposures_per_plate = {}
    for row in rows:
        plate = _text(row.get('plate_id'))
        exposures_per_plate.setdefault(plate, set()).add(_text(row.get('exposure_id')))

    best = {}
    for row in rows:
        plate = _text(row.get('plate_id'))
        if not plate or len(exposures_per_plate[plate]) != 1:
            continue
        magnitude, error = _to_float(row.get('natmag')), _to_float(row.get('natmag_error'))
        color_term, jd = _to_float(row.get('color_term')), _to_float(row.get('jd_mid'))
        prediction = _to_float(row.get('model_prediction'))
        if None in (magnitude, error, color_term, jd) or not 0 < error <= APPLAUSE_MAX_MAG_ERROR:
            continue
        if _to_float(row.get('phot_range_flags')) != 0:
            continue
        if prediction is not None and prediction < APPLAUSE_MIN_MODEL_PREDICTION:
            continue
        key = (_to_float(row.get('separation_arcsec')) or 0.0, error)
        if plate not in best or key < best[plate][0]:
            best[plate] = (key, row)
    return [row for _key, row in best.values()]


class APPLAUSEDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return APPLAUSE_DEFAULT_RADIUS_ARCSEC

    name = 'APPLAUSE'
    verbose_name = 'APPLAUSE DR4 (photographic plates)'
    # DR4 is a fixed release.
    update_on_daily_refresh = False
    info_url = APPLAUSE_PAGE_URL
    acknowledgement = APPLAUSE_ACKNOWLEDGEMENT
    upsert_identity_keys = ('filter', 'applause_plate_id')
    service_notes = (
        'Query APPLAUSE DR4 (digitised Hamburg, Bamberg, Potsdam, Tartu, Tautenburg ... plates, '
        '~1900-1980s) by coordinates. One calibrated magnitude per single-exposure plate within '
        '5 arcsec, in the plate natural system tied to Gaia EDR3, grouped as photovisual (pv), blue '
        '(pg) or UV-sensitive (UV) plates. Typical errors 0.1-0.4 mag; bright stars are saturated.'
    )

    @classmethod
    def get_form_class(cls):
        return APPLAUSEQueryForm

    @classmethod
    def get_acknowledgement(cls):
        return cls.acknowledgement

    def build_query_parameters(self, parameters, **kwargs):
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or self.get_finding_chart_radius_arcsec(),
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or self.get_finding_chart_radius_arcsec()

        points = []
        if ra is not None and dec is not None and query_parameters.get('include_photometry', True):
            try:
                points = _select_points(_query_detections(ra, dec, radius_arcsec))
                if not points:
                    logger.debug('APPLAUSE returned no calibrated detection for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('APPLAUSE query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                points = []

        self.query_results = {
            'points': points,
            'source_location': APPLAUSE_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        if data.get('ra') is None or data.get('dec') is None or not data.get('points'):
            return []
        datums = self._build_photometry_datums(data['points'])
        if not datums:
            return []
        return [{
            'name': None,
            'ra': data['ra'],
            'dec': data['dec'],
            'aliases': [],
            'reduced_datums': {'photometry': datums},
            'source_location': data.get('source_location'),
        }]

    def create_target_from_query(self, target_result, **kwargs):
        return Target(
            name=target_result['name'],
            type='SIDEREAL',
            ra=target_result.get('ra'),
            dec=target_result.get('dec'),
            epoch=2000.0,
        )

    def create_aliases_from_query(self, alias_results, **kwargs):
        return []

    def create_reduced_datums_from_query(self, target, data=None, data_type=None, **kwargs):
        if data_type != 'photometry' or not data:
            return 0
        created, _updated = upsert_reduced_datums(
            target=target,
            data_type='photometry',
            source_name=self.name,
            source_location=kwargs.get('source_location') or self.info_url,
            datums=data,
            identity_keys=self.upsert_identity_keys,
        )
        return created

    def to_reduced_datums(self, target, data_results=None, **kwargs):
        if not data_results:
            return
        for data_type, data in data_results.items():
            self.create_reduced_datums_from_query(
                target,
                data=data,
                data_type=data_type,
                source_location=(getattr(self, 'query_results', {}) or {}).get('source_location'),
            )

    def _build_photometry_datums(self, points):
        output = []
        for row in points:
            jd = _to_float(row['jd_mid'])
            color_term = _to_float(row['color_term'])
            with warnings.catch_warnings():
                # UTC is undefined before 1960; ERFA warns ("dubious year") for most plate dates.
                warnings.simplefilter('ignore', ErfaWarning)
                timestamp = Time(jd, format='jd', scale='utc').to_datetime(timezone=timezone.utc)
            output.append({
                'timestamp': timestamp,
                'value': {
                    'filter': f'APPLAUSE({_passband(color_term)})',
                    'magnitude': round(_to_float(row['natmag']), 3),
                    'error': round(_to_float(row['natmag_error']), 3),
                    'mjd': round(jd - 2400000.5, 5),
                    'color_term': round(color_term, 3),
                    'emulsion': _text(row.get('emulsion')) or None,
                    'plate_filter': _text(row.get('filter')) or None,
                    'exposure_time_s': _to_float(row.get('exptime')),
                    'applause_plate_id': _text(row.get('plate_id')),
                    'applause_archive_id': _text(row.get('archive_id')),
                    'match_separation_arcsec': round(_to_float(row.get('separation_arcsec')) or 0.0, 3),
                },
            })
        return output
