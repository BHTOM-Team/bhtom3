"""VVV/VVVX near-infrared light curves from VIRAC2 in the ESO catalogue archive.

VIRAC2 (the VVV Infrared Astrometric Catalogue, version 2; Smith et al. 2025, MNRAS 536,
3707) holds per-epoch VISTA Z/Y/J/H/Ks photometry for ~560 deg^2 of the southern Galactic
bulge and disc from the VVV and VVVX ESO public surveys, typically hundreds of Ks epochs
per star from 2010 on. It is queried through ESO's catalogue TAP service:
``VVVX_VIRAC_V2_SOURCES`` (positions, ``duplicate`` flag) gives the source id, and
``VVVX_VIRAC_V2_LC`` its time series. Magnitudes are VISTA (Vega) system.

The Galactic plane is crowded (~100 VIRAC2 sources in a 14" cone in the bulge), so the
default radius is small and only the nearest non-duplicate source is used. Epochs are kept
when phot_flag == 0, the detection is not shared with another source (ambiguous_match == 0)
and the error is in (0, VIRAC2_MAX_MAG_ERROR].
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

from custom_code.data_services.forms import VIRAC2QueryForm
from custom_code.data_services.service_utils import (
    DATA_SERVICE_HTTP_TIMEOUT,
    resolve_query_coordinates,
    upsert_reduced_datums,
)

logger = logging.getLogger(__name__)

VIRAC2_PAGE_URL = 'https://archive.eso.org/scienceportal/home?data_collection=VVVX'
VIRAC2_TAP_URL = 'https://archive.eso.org/tap_cat/sync'
VIRAC2_SOURCES_TABLE = 'VVVX_VIRAC_V2_SOURCES'
VIRAC2_LC_TABLE = 'VVVX_VIRAC_V2_LC'
VIRAC2_RELEASE = 'VIRAC2'
VIRAC2_DEFAULT_RADIUS_ARCSEC = 1.0
VIRAC2_MAX_MAG_ERROR = 0.5
VIRAC2_FILTERS = {
    'Z': 'VIRAC2(Z)',
    'Y': 'VIRAC2(Y)',
    'J': 'VIRAC2(J)',
    'H': 'VIRAC2(H)',
    'Ks': 'VIRAC2(Ks)',
}

VIRAC2_ACKNOWLEDGEMENT = (
    'This research uses VIRAC2 (Smith et al. 2025, MNRAS 536, 3707), based on data products '
    'from observations made with ESO telescopes at the La Silla Paranal Observatory under ESO '
    'programmes 179.B-2002 (VVV) and 198.B-2004 (VVVX), retrieved from the ESO Science Archive.'
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


def _tap_query(adql):
    """Run a synchronous ADQL query on ESO's catalogue TAP service; rows as dicts."""
    response = requests.post(
        VIRAC2_TAP_URL,
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
        SELECT sourceid, ra, de, duplicate, phot_ks_mean_mag
        FROM {VIRAC2_SOURCES_TABLE}
        WHERE CONTAINS(POINT('ICRS', ra, de), CIRCLE('ICRS', {ra}, {dec}, {radius_arcsec / 3600.0})) = 1
    """)
    candidates = []
    for row in rows:
        src_ra, src_dec = _to_float(row.get('ra')), _to_float(row.get('de'))
        if _to_int(row.get('sourceid')) is None or src_ra is None or src_dec is None:
            continue
        if _to_int(row.get('duplicate')) not in (0, None):
            continue
        candidates.append((_angular_separation_arcsec(ra, dec, src_ra, src_dec), row, src_ra, src_dec))
    if not candidates:
        return None
    separation, nearest, src_ra, src_dec = min(candidates, key=lambda item: item[0])
    return {
        'sourceid': _to_int(nearest['sourceid']),
        'ra': src_ra,
        'dec': src_dec,
        'ks_mean_mag': _to_float(nearest.get('phot_ks_mean_mag')),
        'separation_arcsec': separation,
    }


def _fetch_light_curve(sourceid):
    return _tap_query(f"""
        SELECT mjdobs, filter, mag, emag, phot_flag, ambiguous_match, objtype, catid
        FROM {VIRAC2_LC_TABLE}
        WHERE sourceid = {int(sourceid)}
    """)


class VIRAC2DataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return VIRAC2_DEFAULT_RADIUS_ARCSEC

    name = 'VIRAC2'
    verbose_name = 'VVV/VVVX VIRAC2 (near-IR)'
    # VIRAC2 is a fixed release; nothing new appears between refreshes.
    update_on_daily_refresh = False
    info_url = VIRAC2_PAGE_URL
    acknowledgement = VIRAC2_ACKNOWLEDGEMENT
    # One VIRAC2 epoch per source, filter and detector catalogue (catid).
    upsert_identity_keys = ('filter', 'catid')
    service_notes = (
        'Query VIRAC2 (VVV/VVVX near-infrared light curves of the southern Galactic bulge and '
        'disc) in the ESO catalogue archive by coordinates. The nearest non-duplicate source '
        'within 1 arcsec is used; Z/Y/J/H/Ks epochs with clean photometry flags, no shared '
        'detection and errors <= 0.5 mag are imported (VISTA Vega magnitudes). Photometry only; '
        'no aliases are added.'
    )

    @classmethod
    def get_form_class(cls):
        return VIRAC2QueryForm

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
                    logger.debug('VIRAC2 returned no source for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('VIRAC2 query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                match, light_curve = None, []

        self.query_results = {
            'match': match,
            'light_curve': light_curve,
            'source_location': VIRAC2_PAGE_URL,
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
            'name': f"VIRAC2_{match['sourceid']}",
            'ra': data['ra'],
            'dec': data['dec'],
            # Photometry only: VIRAC2 source ids are not names anyone searches for.
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
            filter_name = VIRAC2_FILTERS.get(_text(row.get('filter')))
            mjd = _to_float(row.get('mjdobs'))
            mag = _to_float(row.get('mag'))
            error = _to_float(row.get('emag'))
            if filter_name is None or mjd is None or mag is None or error is None:
                continue
            if _to_int(row.get('phot_flag')) != 0 or _to_int(row.get('ambiguous_match')) != 0:
                continue
            if not (0 < error <= VIRAC2_MAX_MAG_ERROR):
                continue
            value = {
                'filter': filter_name,
                'magnitude': mag,
                'error': error,
                'mjd': mjd,
                'mag_system': 'Vega',
                'catid': _to_int(row.get('catid')),
                'objtype': _to_int(row.get('objtype')),
                'virac2_sourceid': match['sourceid'],
                'data_release': VIRAC2_RELEASE,
            }
            if match.get('separation_arcsec') is not None:
                value['match_separation_arcsec'] = round(match['separation_arcsec'], 4)
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': value,
            })
        return output
