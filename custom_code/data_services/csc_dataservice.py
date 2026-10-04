"""Chandra Source Catalog (CSC 2.1) per-observation X-ray fluxes.

CSC 2.1 (Evans et al. 2024, ApJS 274, 22) lists every source detected in public Chandra ACIS and
HRC imaging observations to the end of 2021, with fluxes measured in each observation. It is
queried through the CXC TAP service in three steps: the nearest master source (csc21.master_source),
its stacked-detection links (master_stack_assoc, stack_observation_assoc), and its per-observation
detections (csc21.observation_source). The service has no ADQL geometry, so the cone is a box cut
by separation here.

Each observation gives one point: the aperture-corrected ACIS broad-band (0.5-7 keV) energy flux,
or the HRC wide-band (~0.1-10 keV) flux for HRC observations, in erg/cm2/s, with the error taken
as half the 68% confidence interval. Saturated detections (fluxes far too low) and detections on
an ACIS readout streak are dropped, as are observations where the flux was not measured. CSC 2.1
gives no per-observation upper limits, so non-detections are absent.
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

from custom_code.data_services.forms import CSCQueryForm
from custom_code.data_services.service_utils import (
    DATA_SERVICE_HTTP_TIMEOUT,
    resolve_query_coordinates,
    upsert_reduced_datums,
)

logger = logging.getLogger(__name__)

CSC_PAGE_URL = 'https://cxc.cfa.harvard.edu/csc/'
CSC_TAP_URL = 'https://cda.cfa.harvard.edu/csc21tap/sync'
CSC_RELEASE = 'CSC 2.1'
CSC_DEFAULT_RADIUS_ARCSEC = 3.0
CSC_FLUX_UNIT = 'erg/cm2/s'
# (flux column suffix, BHTOM filter, band) per instrument.
CSC_BANDS = {
    'ACIS': ('b', 'ACIS(0.5-7keV)', '0.5-7 keV'),
    'HRC': ('w', 'HRC(0.1-10keV)', '~0.1-10 keV'),
}

CSC_ACKNOWLEDGEMENT = (
    'This research has made use of data obtained from the Chandra Source Catalog (Evans et al. '
    '2024, ApJS 274, 22), provided by the Chandra X-ray Center (CXC).'
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


def _text(value):
    if value is None or value is np.ma.masked:
        return ''
    if isinstance(value, bytes):
        value = value.decode('utf-8', 'replace')
    return str(value).strip()


def _truthy(value):
    if value is None or value is np.ma.masked:
        return False
    return _text(value).lower() in ('true', '1', 't')


def _tap_query(adql):
    response = requests.post(
        CSC_TAP_URL,
        data={'REQUEST': 'doQuery', 'LANG': 'ADQL', 'QUERY': adql},
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


def _nearest_master_source(ra, dec, radius_arcsec):
    half = radius_arcsec / 3600.0
    half_ra = half / max(math.cos(math.radians(dec)), 1e-6)
    rows = _tap_query(f"""
        SELECT name, ra, dec, err_ellipse_r0, var_flag, var_inter_prob_b
        FROM csc21.master_source
        WHERE dec BETWEEN {dec - half} AND {dec + half} AND ra BETWEEN {ra - half_ra} AND {ra + half_ra}
    """)
    candidates = []
    for row in rows:
        src_ra, src_dec = _to_float(row.get('ra')), _to_float(row.get('dec'))
        if src_ra is None or src_dec is None or not _text(row.get('name')):
            continue
        separation = _angular_separation_arcsec(ra, dec, src_ra, src_dec)
        if separation <= radius_arcsec:
            candidates.append((separation, row))
    if not candidates:
        return None
    separation, row = min(candidates, key=lambda item: item[0])
    return {
        'name': _text(row['name']),
        'ra': _to_float(row['ra']),
        'dec': _to_float(row['dec']),
        'separation_arcsec': separation,
        'var_flag': _truthy(row.get('var_flag')),
        'var_inter_prob_b': _to_float(row.get('var_inter_prob_b')),
    }


def _observation_detections(name):
    """Per-observation detections of one master source."""
    safe_name = name.replace("'", "''")
    stacks = _tap_query(
        f"SELECT detect_stack_id, region_id FROM csc21.master_stack_assoc WHERE name = '{safe_name}'"
    )
    if not stacks:
        return []
    # The parser rejects multi-condition joins, so each step is its own query.
    stack_terms = ' OR '.join(
        f"(detect_stack_id = '{_text(row['detect_stack_id'])}' AND region_id = {_to_int(row['region_id'])})"
        for row in stacks
    )
    links = _tap_query(f"SELECT obsid, obi, region_id FROM csc21.stack_observation_assoc WHERE {stack_terms}")
    if not links:
        return []
    keys = sorted({(_to_int(row['obsid']), _to_int(row['obi']), _to_int(row['region_id'])) for row in links})
    obs_terms = ' OR '.join(f'(obsid = {o} AND obi = {i} AND region_id = {r})' for o, i, r in keys)
    return _tap_query(f"""
        SELECT obsid, obi, instrument, gti_mjd_obs, gti_elapse, livetime, theta,
               flux_aper_b, flux_aper_lolim_b, flux_aper_hilim_b,
               flux_aper_w, flux_aper_lolim_w, flux_aper_hilim_w,
               flux_significance_b, sat_src_flag, streak_src_flag
        FROM csc21.observation_source WHERE {obs_terms}
    """)


class CSCDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return CSC_DEFAULT_RADIUS_ARCSEC

    name = 'CSC'
    verbose_name = 'Chandra Source Catalog 2.1 (X-ray)'
    # CSC 2.1 is a fixed release.
    update_on_daily_refresh = False
    info_url = CSC_PAGE_URL
    acknowledgement = CSC_ACKNOWLEDGEMENT
    # One CSC detection per Chandra observation interval.
    upsert_identity_keys = ('filter', 'chandra_obsid')
    service_notes = (
        'Query the Chandra Source Catalog 2.1 by coordinates. The nearest CSC source within 3 arcsec '
        'is used; its per-observation ACIS 0.5-7 keV (or HRC wide-band) fluxes in erg/cm2/s are '
        'imported, one point per Chandra observation, dropping saturated and readout-streak '
        'detections. Detections only (no upper limits). Plotted on the high-energy plot.'
    )

    @classmethod
    def get_form_class(cls):
        return CSCQueryForm

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

        match, detections = None, []
        if ra is not None and dec is not None:
            try:
                match = _nearest_master_source(ra, dec, radius_arcsec)
                if match:
                    detections = _observation_detections(match['name'])
                else:
                    logger.debug('CSC returned no source for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('CSC query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                match, detections = None, []

        self.query_results = {
            'match': match,
            'detections': detections,
            'source_location': CSC_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        match = data.get('match')
        if data.get('ra') is None or data.get('dec') is None or not match:
            return []
        datums = self._build_highenergy_datums(data.get('detections') or [], match)
        if not datums:
            return []
        return [{
            'name': match['name'].replace(' ', '_'),
            'ra': data['ra'],
            'dec': data['dec'],
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

    def _build_highenergy_datums(self, detections, match):
        best = {}
        for row in detections:
            band = CSC_BANDS.get(_text(row.get('instrument')).upper())
            mjd = _to_float(row.get('gti_mjd_obs'))
            if band is None or mjd is None:
                continue
            if _truthy(row.get('sat_src_flag')) or _truthy(row.get('streak_src_flag')):
                continue
            suffix, filter_name, energy_band = band
            flux = _to_float(row.get(f'flux_aper_{suffix}'))
            low = _to_float(row.get(f'flux_aper_lolim_{suffix}'))
            high = _to_float(row.get(f'flux_aper_hilim_{suffix}'))
            if flux is None or flux <= 0 or low is None or high is None or high <= low:
                continue
            # Points are placed at mid-observation; gti_mjd_obs is the start.
            elapse_days = (_to_float(row.get('gti_elapse')) or 0.0) / 86400.0
            obsid, obi = _to_int(row.get('obsid')), _to_int(row.get('obi'))
            significance = _to_float(row.get('flux_significance_b')) or 0.0
            key = (filter_name, obsid, obi)
            # An observation can hold two detections linked to the same source (e.g. far off-axis);
            # keep the most significant one.
            if key in best and best[key][0] >= significance:
                continue
            best[key] = (significance, {
                'timestamp': Time(mjd + elapse_days / 2.0, format='mjd', scale='tt').utc.to_datetime(timezone=timezone.utc),
                'value': {
                    'filter': filter_name,
                    'flux': flux,
                    'error': (high - low) / 2.0,
                    'flux_lolim': low,
                    'flux_hilim': high,
                    'flux_unit': CSC_FLUX_UNIT,
                    'energy_band': energy_band,
                    'facility': f"Chandra/{_text(row.get('instrument')).upper()}",
                    'chandra_obsid': f'{obsid}.{obi}',
                    'livetime_s': _to_float(row.get('livetime')),
                    'off_axis_arcmin': _to_float(row.get('theta')),
                    'flux_significance': _to_float(row.get('flux_significance_b')),
                    'csc_name': match['name'],
                    'csc_var_flag': match['var_flag'],
                    'match_separation_arcsec': round(match['separation_arcsec'], 3),
                    'data_release': CSC_RELEASE,
                },
            })
        return [datum for _significance, datum in best.values()]
