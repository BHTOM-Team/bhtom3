"""SuperCOSMOS Sky Survey (SSS): photographic B_J, R and I photometry from the 1950s-1990s.

SuperCOSMOS (Hambly et al. 2001, MNRAS 326, 1279/1295/1315) digitised the UK Schmidt, ESO-R,
POSS-I and POSS-II survey plates over the whole sky. The SuperCOSMOS Science Archive (WFAU,
Edinburgh) is queried through its public TAP service: SSA.Detection holds every detection on
every plate, and SSA.Plate gives each plate's mid-exposure MJD and survey. A source therefore
has one point per plate, usually 2-4 (B_J, R at two epochs ~40 years apart, I) and more where
survey fields overlap.

Parent (undeblended) detections are dropped in favour of their deblended children, the nearest
detection per plate is kept, and detections with quality bits other than the informational ones
and the conservative 'near a very bright star' bit are dropped (Hambly et al. 2001, Paper II). The magnitude follows the detection's image
class (galaxy calibration for class 1, stellar otherwise). The archive gives no per-detection
error; photographic photometry is good to ~0.3 mag, which is stored as a nominal error. Plates
saturate for stars brighter than ~11-12 mag, where the magnitudes are unreliable.
"""

import io
import logging
import math
from datetime import timezone

import numpy as np
import requests
from astropy.io.votable import parse_single_table
from astropy.time import Time

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target

from custom_code.data_services.forms import SuperCOSMOSQueryForm
from custom_code.data_services.service_utils import (
    DATA_SERVICE_HTTP_TIMEOUT,
    resolve_query_coordinates,
    upsert_reduced_datums,
)

logger = logging.getLogger(__name__)

SSS_PAGE_URL = 'http://ssa.roe.ac.uk/'
SSS_TAP_URL = 'http://tap.roe.ac.uk/ssa/sync'
SSS_DEFAULT_RADIUS_ARCSEC = 3.0
# Quality bits that are allowed (Hambly et al. 2001, Paper II, Table 2): 0, 1 and 4 are informational;
# 10 marks images near a very bright star, flagged conservatively for possibly spurious detections,
# which does not apply to a known target. Any other bit (fragmented, large or wedge-affected image,
# boundary contact, ...) rejects the detection.
SSS_ALLOWED_QUALITY_BITS = 1 | 2 | 16 | 1024
SSS_NOMINAL_ERROR = 0.3
SSS_GALAXY_CLASS = 1
# SSA.Plate.surveyID -> (band, plate survey).
SSS_SURVEYS = {
    1: ('Bj', 'UKST J'),
    2: ('R', 'UKST R'),
    3: ('I', 'UKST I'),
    4: ('R', 'ESO R'),
    5: ('R', 'POSS-I E'),
    6: ('Bj', 'POSS-II J'),
    7: ('R', 'POSS-II F'),
    8: ('I', 'POSS-II N'),
    9: ('R', 'POSS-I E'),
}

SSS_ACKNOWLEDGEMENT = (
    'This research uses data from the SuperCOSMOS Sky Survey (Hambly et al. 2001, MNRAS 326, '
    '1279), retrieved from the SuperCOSMOS Science Archive at the Wide Field Astronomy Unit, '
    'Institute for Astronomy, University of Edinburgh.'
)


def _to_float(value):
    if value is None or value is np.ma.masked:
        return None
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def _to_int(value):
    converted = _to_float(value)
    return int(converted) if converted is not None else None


def _tap_query(adql):
    response = requests.post(
        SSS_TAP_URL,
        data={'REQUEST': 'doQuery', 'LANG': 'ADQL', 'FORMAT': 'votable', 'QUERY': adql},
        timeout=DATA_SERVICE_HTTP_TIMEOUT,
    )
    response.raise_for_status()
    table = parse_single_table(io.BytesIO(response.content), verify='ignore').to_table(use_names_over_ids=True)
    return [{name: row[name] for name in table.colnames} for row in table]


def _angular_separation_arcsec(ra1, dec1, ra2, dec2):
    ra1, dec1, ra2, dec2 = map(math.radians, (ra1, dec1, ra2, dec2))
    cos_sep = (math.sin(dec1) * math.sin(dec2)
               + math.cos(dec1) * math.cos(dec2) * math.cos(ra1 - ra2))
    return math.degrees(math.acos(min(1.0, max(-1.0, cos_sep)))) * 3600.0


def _fetch_detections(ra, dec, radius_arcsec):
    """One detection per plate (nearest deblended detection within the radius)."""
    # The archive's SQL Server backend has no ADQL geometry; select a box, then cut on separation.
    half = radius_arcsec / 3600.0
    half_ra = half / max(math.cos(math.radians(dec)), 1e-6)
    rows = _tap_query(f"""
        SELECT d.objID, d.ra, d.dec, d.sMag, d.gMag, d.class, d.quality, d.blend, d.plateID,
               p.mjd, p.surveyID, p.plateNum
        FROM SSA.Detection AS d JOIN SSA.Plate AS p ON d.plateID = p.plateID
        WHERE d.dec BETWEEN {dec - half} AND {dec + half}
          AND d.ra BETWEEN {ra - half_ra} AND {ra + half_ra}
    """)
    nearest_per_plate = {}
    for row in rows:
        det_ra, det_dec = _to_float(row.get('ra')), _to_float(row.get('dec'))
        plate_id = _to_int(row.get('plateID'))
        if det_ra is None or det_dec is None or plate_id is None:
            continue
        # A negative blend flag marks an undeblended parent; its children carry the photometry.
        if (_to_int(row.get('blend')) or 0) < 0:
            continue
        separation = _angular_separation_arcsec(ra, dec, det_ra, det_dec)
        if separation > radius_arcsec:
            continue
        current = nearest_per_plate.get(plate_id)
        if current is None or separation < current['separation_arcsec']:
            nearest_per_plate[plate_id] = {**row, 'separation_arcsec': separation}
    return list(nearest_per_plate.values())


class SuperCOSMOSDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return SSS_DEFAULT_RADIUS_ARCSEC

    name = 'SuperCOSMOS'
    verbose_name = 'SuperCOSMOS Sky Survey (photographic plates)'
    # The plate archive is complete; nothing new appears between refreshes.
    update_on_daily_refresh = False
    info_url = SSS_PAGE_URL
    acknowledgement = SSS_ACKNOWLEDGEMENT
    # One SuperCOSMOS detection per plate.
    upsert_identity_keys = ('filter', 'sss_plate_id')
    service_notes = (
        'Query the SuperCOSMOS Science Archive by coordinates for photographic B_J, R and I '
        'photometry from the UKST, ESO-R, POSS-I and POSS-II plates (1950s-1990s): one point per '
        'plate within 3 arcsec, deblended detections without image-quality defects only, with a '
        'nominal 0.3 mag error. Stars brighter than ~11-12 mag are saturated on the plates.'
    )

    @classmethod
    def get_form_class(cls):
        return SuperCOSMOSQueryForm

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

        detections = []
        if ra is not None and dec is not None and query_parameters.get('include_photometry', True):
            try:
                detections = _fetch_detections(ra, dec, radius_arcsec)
                if not detections:
                    logger.debug('SuperCOSMOS returned no detection for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('SuperCOSMOS query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                detections = []

        self.query_results = {
            'detections': detections,
            'source_location': SSS_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        if data.get('ra') is None or data.get('dec') is None:
            return []
        datums = self._build_photometry_datums(data.get('detections') or [])
        if not datums:
            return []
        return [{
            'name': f"SSS_{datums[0]['value']['sss_object_id']}",
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

    def _build_photometry_datums(self, detections):
        output = []
        for row in detections:
            survey = SSS_SURVEYS.get(_to_int(row.get('surveyID')))
            mjd = _to_float(row.get('mjd'))
            quality = _to_int(row.get('quality'))
            image_class = _to_int(row.get('class'))
            if survey is None or mjd is None or mjd <= 0 or quality is None or quality & ~SSS_ALLOWED_QUALITY_BITS:
                continue
            magnitude = _to_float(row.get('gMag') if image_class == SSS_GALAXY_CLASS else row.get('sMag'))
            if magnitude is None or not (0 < magnitude < 30):
                continue
            band, plate_survey = survey
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {
                    'filter': f'SSS({band})',
                    'magnitude': round(magnitude, 3),
                    'error': SSS_NOMINAL_ERROR,
                    'error_nominal': True,
                    'mjd': mjd,
                    'mag_system': 'Vega',
                    'plate_survey': plate_survey,
                    'sss_plate_id': _to_int(row.get('plateID')),
                    'sss_plate_number': _to_int(row.get('plateNum')),
                    'sss_object_id': _to_int(row.get('objID')),
                    'image_class': image_class,
                    'quality': quality,
                    'match_separation_arcsec': round(row['separation_arcsec'], 3),
                },
            })
        return output
