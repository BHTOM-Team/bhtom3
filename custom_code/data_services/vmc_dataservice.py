"""VISTA Magellanic Clouds survey (VMC) near-infrared light curves from the ESO catalogue archive.

VMC (Cioni et al. 2011, A&A 527, A116; ESO programme 179.B-2003) imaged the LMC, SMC,
Magellanic Bridge and Stream in Y, J and Ks with VISTA/VIRCAM, with typically ~12 Ks and a
few Y/J epochs per field between 2009 and 2022. Data release 7 (the final release, full
survey area) is queried through ESO's catalogue TAP service: ``vmc_dr7_ksjy_V6`` (band-merged
source catalogue) gives the source id, and ``vmc_dr7_mPhot{Y,J,Ks}_V6`` its per-epoch 2"
aperture-corrected magnitudes (VISTA Vega system).

Only primary sources (``PRIMARY_SOURCE = 1``) are matched; seam duplicates carry no epochs.
The ``*PPERRBITS`` column of the per-epoch tables holds one value per observation frame (never
0) rather than quality bits, so it is not used; epochs are kept when the error is in
(0, VMC_MAX_MAG_ERROR].
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

from custom_code.data_services.forms import VMCQueryForm
from custom_code.data_services.service_utils import (
    DATA_SERVICE_HTTP_TIMEOUT,
    resolve_query_coordinates,
    upsert_reduced_datums,
)

logger = logging.getLogger(__name__)

VMC_PAGE_URL = 'https://archive.eso.org/scienceportal/home?data_collection=VMC'
VMC_TAP_URL = 'https://archive.eso.org/tap_cat/sync'
VMC_SOURCES_TABLE = 'vmc_dr7_ksjy_V6'
# Band -> (multi-epoch table, column prefix, BHTOM filter name).
VMC_BANDS = {
    'Y': ('vmc_dr7_mPhotY_V6', 'Y', 'VMC(Y)'),
    'J': ('vmc_dr7_mPhotJ_V6', 'J', 'VMC(J)'),
    'Ks': ('vmc_dr7_mPhotKs_V6', 'KS', 'VMC(Ks)'),
}
VMC_RELEASE = 'VMC DR7'
VMC_DEFAULT_RADIUS_ARCSEC = 1.0
VMC_MAX_MAG_ERROR = 0.3

VMC_ACKNOWLEDGEMENT = (
    'This research uses data from the VISTA Magellanic Cloud survey (VMC; Cioni et al. 2011, '
    'A&A 527, A116), based on observations made with ESO telescopes at the La Silla Paranal '
    'Observatory under ESO programme 179.B-2003, retrieved from the ESO Science Archive.'
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
    """Run a synchronous ADQL query on ESO's catalogue TAP service; rows as dicts."""
    response = requests.post(
        VMC_TAP_URL,
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


def _find_nearest_source(ra, dec, radius_arcsec):
    # ESO's TAP service has no ADQL DISTANCE(); the separation is computed here.
    rows = _tap_query(f"""
        SELECT SOURCEID, RA2000, DEC2000
        FROM {VMC_SOURCES_TABLE}
        WHERE PRIMARY_SOURCE = 1
          AND CONTAINS(POINT('ICRS', RA2000, DEC2000), CIRCLE('ICRS', {ra}, {dec}, {radius_arcsec / 3600.0})) = 1
    """)
    candidates = []
    for row in rows:
        src_ra, src_dec = _to_float(row.get('RA2000')), _to_float(row.get('DEC2000'))
        if _to_int(row.get('SOURCEID')) is None or src_ra is None or src_dec is None:
            continue
        candidates.append((_angular_separation_arcsec(ra, dec, src_ra, src_dec), row, src_ra, src_dec))
    if not candidates:
        return None
    separation, nearest, src_ra, src_dec = min(candidates, key=lambda item: item[0])
    return {
        'sourceid': _to_int(nearest['SOURCEID']),
        'ra': src_ra,
        'dec': src_dec,
        'separation_arcsec': separation,
    }


def _fetch_light_curve(sourceid):
    """Per-epoch rows of every band, tagged with the band name."""
    rows = []
    for band, (table, prefix, _filter_name) in VMC_BANDS.items():
        for row in _tap_query(f"""
            SELECT PHOT_ID, MJD, {prefix}MAG AS mag, {prefix}ERR AS err
            FROM {table}
            WHERE SOURCEID = {int(sourceid)}
        """):
            row['band'] = band
            rows.append(row)
    return rows


class VMCDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return VMC_DEFAULT_RADIUS_ARCSEC

    name = 'VMC'
    verbose_name = 'VISTA Magellanic Clouds (VMC, near-IR)'
    # VMC DR7 is the final release; nothing new appears between refreshes.
    update_on_daily_refresh = False
    info_url = VMC_PAGE_URL
    acknowledgement = VMC_ACKNOWLEDGEMENT
    # PHOT_ID is VMC's unique id for one source detection on one epoch.
    upsert_identity_keys = ('filter', 'vmc_phot_id')
    service_notes = (
        'Query the VISTA Magellanic Clouds survey (VMC DR7: LMC, SMC, Bridge and Stream) in the '
        'ESO catalogue archive by coordinates. The nearest primary source within 1 arcsec is '
        'used; Y/J/Ks epochs with errors <= 0.3 mag are imported (2" aperture-corrected VISTA '
        'Vega magnitudes). Photometry only; no aliases are added.'
    )

    @classmethod
    def get_form_class(cls):
        return VMCQueryForm

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

        match, light_curve = None, []
        if ra is not None and dec is not None and query_parameters.get('include_photometry', True):
            try:
                match = _find_nearest_source(ra, dec, radius_arcsec)
                if match:
                    light_curve = _fetch_light_curve(match['sourceid'])
                else:
                    logger.debug('VMC returned no source for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('VMC query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                match, light_curve = None, []

        self.query_results = {
            'match': match,
            'light_curve': light_curve,
            'source_location': VMC_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        match = data.get('match')
        if data.get('ra') is None or data.get('dec') is None or not match:
            return []

        datums = self._build_photometry_datums(data.get('light_curve') or [], match)
        if not datums:
            return []

        return [{
            'name': f"VMC_{match['sourceid']}",
            'ra': data['ra'],
            'dec': data['dec'],
            # Photometry only: VMC source ids are not names anyone searches for.
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
            band = VMC_BANDS.get(row.get('band'))
            mjd = _to_float(row.get('MJD'))
            mag = _to_float(row.get('mag'))
            error = _to_float(row.get('err'))
            if band is None or mjd is None or mag is None or error is None:
                continue
            if not (0 < error <= VMC_MAX_MAG_ERROR):
                continue
            value = {
                'filter': band[2],
                'magnitude': mag,
                'error': error,
                'mjd': mjd,
                'mag_system': 'Vega',
                'vmc_phot_id': _to_int(row.get('PHOT_ID')),
                'vmc_sourceid': match['sourceid'],
                'data_release': VMC_RELEASE,
            }
            if match.get('separation_arcsec') is not None:
                value['match_separation_arcsec'] = round(match['separation_arcsec'], 4)
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': value,
            })
        return output
