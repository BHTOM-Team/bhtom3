"""XMM-Newton EPIC X-ray fluxes from the per-observation source detections in the XMM-Newton
Science Archive (the detections behind the 5XMM-DR15 serendipitous source catalogue;
Webb et al. 2020, A&A 641, A136 for 4XMM).

``xsa.v_epic_source`` holds one row per EPIC detection per observation, with the EPIC
0.2-12 keV flux in erg/cm^2/s. It has no catalogue-wide source id (``src_num`` only counts
within one observation), so the nearest detection to the target in each public observation
is used; observation dates come from ``xsa.v_public_observations`` (midpoint of the
exposure). Only detections are listed, so there are no upper limits.

EP_FLAG is a 12-character string whose k-th character (left to right) is catalogue flag k;
the 12th is unused. Following the catalogue's SUM_FLAG definition, flags 1, 2, 3 and 9 are
warnings (1), flags 7, 8 and 10 possibly spurious (2) and flag 11 failed visual screening
(3, or 4 together with 7/8/10). Detections with SUM_FLAG <= XMMEPIC_MAX_SUM_FLAG are kept:
"possibly spurious" matters for faint field sources, while for a source at the target's
position it mostly marks bright detections on warm pixels or bad CCD areas.
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

from custom_code.data_services.forms import XMMEPICQueryForm
from custom_code.data_services.service_utils import (
    DATA_SERVICE_HTTP_TIMEOUT,
    resolve_query_coordinates,
    upsert_reduced_datums,
)

logger = logging.getLogger(__name__)

XMMEPIC_PAGE_URL = 'https://nxsa.esac.esa.int/nxsa-web/'
XMMEPIC_TAP_URL = 'https://nxsa.esac.esa.int/tap-server/tap/sync'
XMMEPIC_FILTER = 'EPIC(0.2-12keV)'
XMMEPIC_FLUX_UNIT = 'erg/cm2/s'
# EPIC positions are good to ~1.5" (1 sigma); the PSF is ~6" FWHM.
XMMEPIC_DEFAULT_RADIUS_ARCSEC = 5.0
XMMEPIC_MAX_SUM_FLAG = 2

XMMEPIC_ACKNOWLEDGEMENT = (
    'This research uses XMM-Newton EPIC source detections from the XMM-Newton Science '
    'Archive, as compiled for the XMM-Newton Serendipitous Source Catalogue (5XMM-DR15; '
    'Webb et al. 2020, A&A 641, A136 for 4XMM), prepared by the XMM-Newton Survey Science '
    'Centre. XMM-Newton is an ESA science mission with instruments and contributions directly '
    'funded by ESA Member States and NASA.'
)


def _to_float(value):
    if value is None or value is np.ma.masked:
        return None
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def _text(value):
    if value is None or value is np.ma.masked:
        return ''
    if isinstance(value, bytes):
        value = value.decode('utf-8', 'replace')
    return str(value).strip()


def _sum_flag(ep_flag):
    """Catalogue SUM_FLAG (0-4) from an EP_FLAG string; None if the string is unusable."""
    flags = _text(ep_flag).upper()
    if len(flags) < 11 or set(flags) - {'T', 'F', '-'}:
        return None
    is_set = {k: flags[k - 1] == 'T' for k in range(1, 12)}
    spurious = any(is_set[k] for k in (7, 8, 10))
    if is_set[11]:
        return 4 if spurious else 3
    if spurious:
        return 2
    if any(is_set[k] for k in (1, 2, 3, 9)):
        return 1
    return 0


def _build_xmmepic_query(ra, dec, radius_arcsec):
    return f"""
    SELECT s.observation_id, s.src_num, s.ra, s.dec, s.radec_err, s.ep_tot_flux,
           s.ep_tot_flux_err, s.ep_det_ml, s.ep_extent, s.ep_flag, o.start_utc, o.end_utc,
           DISTANCE(POINT('ICRS', s.ra, s.dec), POINT('ICRS', {ra}, {dec})) * 3600 AS dist_arcsec
    FROM xsa.v_epic_source AS s
    JOIN xsa.v_public_observations AS o ON s.observation_oid = o.observation_oid
    WHERE 1 = CONTAINS(POINT('ICRS', s.ra, s.dec), CIRCLE('ICRS', {ra}, {dec}, {radius_arcsec / 3600.0}))
    ORDER BY dist_arcsec
    """


def _tap_query(adql):
    """Run a synchronous ADQL query on the XMM-Newton Science Archive; rows as dicts."""
    response = requests.post(
        XMMEPIC_TAP_URL,
        data={'REQUEST': 'doQuery', 'LANG': 'ADQL', 'QUERY': adql},
        timeout=DATA_SERVICE_HTTP_TIMEOUT,
    )
    response.raise_for_status()
    table = parse_single_table(io.BytesIO(response.content), verify='ignore').to_table(use_names_over_ids=True)
    return [{name: row[name] for name in table.colnames} for row in table]


def _nearest_per_observation(rows):
    """The detection nearest the target in each observation (others are neighbours)."""
    nearest = {}
    for row in rows:
        observation_id = _text(row.get('observation_id'))
        distance = _to_float(row.get('dist_arcsec'))
        if not observation_id or distance is None:
            continue
        if observation_id not in nearest or distance < _to_float(nearest[observation_id]['dist_arcsec']):
            nearest[observation_id] = row
    return list(nearest.values())


def _observation_midpoint(row):
    try:
        t_start = Time(_text(row.get('start_utc')), format='isot', scale='utc')
    except ValueError:
        return None
    try:
        t_end = Time(_text(row.get('end_utc')), format='isot', scale='utc')
    except ValueError:
        return t_start
    return t_start + (t_end - t_start) / 2


class XMMEPICDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return XMMEPIC_DEFAULT_RADIUS_ARCSEC

    name = 'XMMEPIC'
    verbose_name = 'XMM-Newton EPIC (X-ray)'
    # New public XMM observations appear continuously as proprietary periods end.
    update_on_daily_refresh = True
    info_url = XMMEPIC_PAGE_URL
    acknowledgement = XMMEPIC_ACKNOWLEDGEMENT
    upsert_identity_keys = ('filter', 'observation_id')
    service_notes = (
        'Query XMM-Newton EPIC source detections (the per-observation detections behind '
        '5XMM-DR15) in the XMM-Newton Science Archive by coordinates. In each public observation '
        'the detection nearest the target is used; its 0.2-12 keV flux in erg/cm^2/s is plotted '
        'on the high-energy plot. Detections failing visual screening (SUM_FLAG 3-4) are '
        'rejected. No upper limits; no aliases are added.'
    )

    @classmethod
    def get_form_class(cls):
        return XMMEPICQueryForm

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
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or self.get_finding_chart_radius_arcsec()

        detections = []
        if ra is not None and dec is not None:
            try:
                detections = _nearest_per_observation(_tap_query(_build_xmmepic_query(ra, dec, radius_arcsec)))
                if not detections:
                    logger.debug('XMM EPIC returned no detection for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('XMM EPIC query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                detections = []

        self.query_results = {
            'detections': detections,
            'source_location': XMMEPIC_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        if data.get('ra') is None or data.get('dec') is None or not data.get('detections'):
            return []

        datums = self._build_highenergy_datums(data['detections'])
        if not datums:
            return []

        return [{
            'name': f"XMMEPIC_J{data['ra']:.5f}{data['dec']:+.5f}",
            'ra': data['ra'],
            'dec': data['dec'],
            # High-energy data only: no catalogue-wide source name exists for these detections.
            'aliases': [],
            'reduced_datums': {'highenergy': datums},
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
        if data_type != 'highenergy' or not data:
            return 0
        created, _updated = upsert_reduced_datums(
            target=target,
            data_type='highenergy',
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

    def _build_highenergy_datums(self, detections):
        output = []
        for row in detections:
            flux = _to_float(row.get('ep_tot_flux'))
            error = _to_float(row.get('ep_tot_flux_err'))
            sum_flag = _sum_flag(row.get('ep_flag'))
            midpoint = _observation_midpoint(row)
            if flux is None or error is None or flux <= 0 or error <= 0 or midpoint is None:
                continue
            if sum_flag is None or sum_flag > XMMEPIC_MAX_SUM_FLAG:
                continue
            output.append({
                'timestamp': midpoint.to_datetime(timezone=timezone.utc),
                'value': {
                    'filter': XMMEPIC_FILTER,
                    'flux': flux,
                    'error': error,
                    'flux_unit': XMMEPIC_FLUX_UNIT,
                    'mjd': float(midpoint.mjd),
                    'observation_id': _text(row.get('observation_id')),
                    'src_num': int(_to_float(row.get('src_num')) or 0),
                    'det_ml': _to_float(row.get('ep_det_ml')),
                    'extent_arcsec': _to_float(row.get('ep_extent')),
                    'ep_flag': _text(row.get('ep_flag')),
                    'sum_flag': sum_flag,
                    'match_separation_arcsec': round(_to_float(row.get('dist_arcsec')) or 0.0, 4),
                    'facility': 'XMM-Newton EPIC',
                    'observer': 'XMM-Newton',
                },
            })
        return output
