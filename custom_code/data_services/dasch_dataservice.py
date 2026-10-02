"""DASCH DR7 century-long photographic light curves (Harvard plates, ~1890-1990).

DASCH DR7 (Digital Access to a Sky Century @ Harvard; Grindlay et al. 2012) holds ~23.6 billion
calibrated magnitudes of ~252 million sources from the Harvard plate collection, referenced
to APASS DR8 (Johnson B). The public Starglass web API (no key) is used:
- POST querycat {refcat, ra_deg, dec_deg, radius_arcsec} -> sources (box search) with
  ref_number and gsc_bin_index;
- POST lightcurve {refcat, ref_number, gsc_bin_index} -> one row per plate.
Both return a JSON list of CSV lines (first line = header).
See https://dasch.cfa.harvard.edu/dr7/web-apis/ and the daschlab package.

Detections are rows with magcal_magdep (the preferred calibrated magnitude) and use
magcal_local_rms as the error, as daschlab plots them. Following daschlab's
apply_standard_rejections(), points with AFLAGS HIGH_BACKGROUND, LARGE_ISO_RMS,
LARGE_LOCAL_SMOOTH_RMS, CLOSE_TO_LIMITING or BIN_DRAD_UNKNOWN are rejected, plus
UNCERTAIN_DATE so no point is plotted at a wrong epoch. Non-detections are stored as upper
limits at limiting_mag_local with error -1 (the photometry upper-limit convention).

date_jd is the HJD midpoint of the exposure; it is stored as given (the JD/HJD difference of
<= 8.3 min is below DASCH's typical timing accuracy).
"""

import csv
import io
import logging
import math
from datetime import timezone

import requests
from astropy.time import Time

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target

from custom_code.data_services.forms import DASCHQueryForm
from custom_code.data_services.service_utils import (
    resolve_query_coordinates,
    upsert_reduced_datums,
)

logger = logging.getLogger(__name__)

DASCH_PAGE_URL = 'https://dasch.cfa.harvard.edu/dr7/'
DASCH_API_URL = 'https://api.starglass.cfa.harvard.edu/public/dasch/dr7/'
DASCH_REFCAT = 'apass'
DASCH_FILTER = 'DASCH(B)'
DASCH_RELEASE = 'DASCH DR7'
DASCH_DEFAULT_RADIUS_ARCSEC = 5.0
DASCH_HTTP_TIMEOUT = (10, 300)

# AFLAGS bits (0-based) as defined in daschlab.photometry.AFlags.
AFLAG_HIGH_BACKGROUND = 1 << 6
AFLAG_UNCERTAIN_DATE = 1 << 9
AFLAG_LARGE_ISO_RMS = 1 << 11
AFLAG_LARGE_LOCAL_SMOOTH_RMS = 1 << 12
AFLAG_CLOSE_TO_LIMITING = 1 << 13
AFLAG_BIN_DRAD_UNKNOWN = 1 << 15
DASCH_REJECT_AFLAGS = (
    AFLAG_HIGH_BACKGROUND | AFLAG_LARGE_ISO_RMS | AFLAG_LARGE_LOCAL_SMOOTH_RMS
    | AFLAG_CLOSE_TO_LIMITING | AFLAG_BIN_DRAD_UNKNOWN | AFLAG_UNCERTAIN_DATE
)

# DASCH's suggested acknowledgement (https://dasch.cfa.harvard.edu/citing/); cite Grindlay et
# al. 2012 (2012IAUS..285...29G).
DASCH_ACKNOWLEDGEMENT = (
    'This work has made use of data provided by Digital Access to a Sky Century @ Harvard '
    '(DASCH), which has been partially supported by NSF grants AST-0407380, AST-0909073, and '
    'AST-1313370. Work on DASCH Data Release 7 received support from the Smithsonian American '
    "Women's History Initiative Pool. DASCH: Grindlay et al. 2012, IAUS 285, 29."
)


def _to_float(value):
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def _to_int(value, default=0):
    converted = _to_float(value)
    return int(converted) if converted is not None else default


def _post_csv(endpoint, payload):
    """POST to a DASCH endpoint; the response is a JSON list of CSV lines."""
    response = requests.post(DASCH_API_URL + endpoint, json=payload, timeout=DASCH_HTTP_TIMEOUT)
    response.raise_for_status()
    lines = response.json()
    if not isinstance(lines, list) or not lines:
        return []
    return list(csv.DictReader(io.StringIO('\n'.join(lines))))


def _angular_separation_arcsec(ra1, dec1, ra2, dec2):
    ra1, dec1, ra2, dec2 = map(math.radians, (ra1, dec1, ra2, dec2))
    cos_sep = (math.sin(dec1) * math.sin(dec2)
               + math.cos(dec1) * math.cos(dec2) * math.cos(ra1 - ra2))
    return math.degrees(math.acos(min(1.0, max(-1.0, cos_sep)))) * 3600.0


def _find_nearest_source(ra, dec, radius_arcsec):
    """Nearest DASCH reference-catalogue source within radius (querycat is a box search)."""
    best = None
    for row in _post_csv('querycat', {
        'refcat': DASCH_REFCAT, 'ra_deg': ra, 'dec_deg': dec, 'radius_arcsec': radius_arcsec,
    }):
        src_ra, src_dec = _to_float(row.get('ra_deg')), _to_float(row.get('dec_deg'))
        ref_number, bin_index = _to_float(row.get('ref_number')), _to_float(row.get('gsc_bin_index'))
        if None in (src_ra, src_dec, ref_number, bin_index):
            continue
        separation = _angular_separation_arcsec(ra, dec, src_ra, src_dec)
        if separation <= radius_arcsec and (best is None or separation < best['separation_arcsec']):
            best = {
                'ref_text': (row.get('ref_text') or '').strip(),
                'ref_number': int(row['ref_number']),
                'gsc_bin_index': int(row['gsc_bin_index']),
                'stdmag': _to_float(row.get('stdmag')),
                'separation_arcsec': separation,
            }
    return best


class DASCHDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return DASCH_DEFAULT_RADIUS_ARCSEC

    name = 'DASCH'
    verbose_name = 'DASCH DR7 (Harvard plates 1890-1990)'
    # DR7 is the final DASCH release; nothing new appears between refreshes.
    update_on_daily_refresh = False
    info_url = DASCH_PAGE_URL
    acknowledgement = DASCH_ACKNOWLEDGEMENT
    upsert_identity_keys = ('filter', 'dasch_plate')
    service_notes = (
        'Query DASCH DR7 (Harvard photographic plates, ~1890-1990, calibrated to APASS B) through '
        'the public Starglass API by coordinates. The nearest DASCH source within 5 arcsec is '
        'used; detections passing daschlab\'s standard AFLAGS rejections (and with certain dates) '
        'are imported, and non-detections are stored as upper limits. Photometry only; no aliases '
        'are added.'
    )

    @classmethod
    def get_form_class(cls):
        return DASCHQueryForm

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

        match, rows = None, []
        if ra is not None and dec is not None and query_parameters.get('include_photometry', True):
            try:
                match = _find_nearest_source(ra, dec, radius_arcsec)
                if match:
                    rows = _post_csv('lightcurve', {
                        'refcat': DASCH_REFCAT,
                        'ref_number': match['ref_number'],
                        'gsc_bin_index': match['gsc_bin_index'],
                    })
                else:
                    logger.debug('DASCH returned no source for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('DASCH query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                match, rows = None, []

        self.query_results = {
            'match': match,
            'rows': rows,
            'source_location': DASCH_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        match = data.get('match')
        if data.get('ra') is None or data.get('dec') is None or not match:
            return []

        datums = self._build_photometry_datums(data.get('rows') or [], match)
        if not datums:
            return []

        return [{
            'name': match['ref_text'] or f"DASCH_{match['ref_number']}",
            'ra': data['ra'],
            'dec': data['dec'],
            # Photometry only: APASS/DASCH reference names are not names anyone searches for.
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

    def _build_photometry_datums(self, rows, match):
        output = []
        for row in rows:
            jd = _to_float(row.get('date_jd'))
            if jd is None or jd <= 2400000.0:
                continue
            series = (row.get('series') or '').strip()
            plate = f"{series}{_to_int(row.get('plate_number'))}:{_to_int(row.get('mosaic_number'))}:{_to_int(row.get('solution_number'))}"
            base = {
                'filter': DASCH_FILTER,
                'dasch_plate': plate,
                'hjd': jd,
                'time_accuracy_days': _to_float(row.get('time_accuracy_days')),
                'dasch_ref': match['ref_text'],
                'data_release': DASCH_RELEASE,
                'match_separation_arcsec': round(match['separation_arcsec'], 3),
            }
            magnitude = _to_float(row.get('magcal_magdep'))
            if magnitude is not None:
                error = _to_float(row.get('magcal_local_rms'))
                aflags = _to_int(row.get('aflags'))
                if error is None or error <= 0 or aflags & DASCH_REJECT_AFLAGS:
                    continue
                value = {**base, 'magnitude': magnitude, 'error': error, 'aflags': aflags,
                         'bflags': _to_int(row.get('bflags')), 'reject_flag': _to_int(row.get('reject_flag'))}
            else:
                limit = _to_float(row.get('limiting_mag_local'))
                if limit is None or limit <= 0:
                    continue
                value = {**base, 'magnitude': limit, 'error': -1.0, 'upper_limit': True}
            output.append({
                'timestamp': Time(jd, format='jd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': value,
            })
        return output
