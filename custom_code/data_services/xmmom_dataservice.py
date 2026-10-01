"""XMM-Newton Optical Monitor photometry from the XMM-OM Serendipitous UV Source Survey (SUSS).

SUSS (Page et al. 2012, MNRAS 426, 903; current release 6.2 in the XMM-Newton Science
Archive) lists every OM detection per XMM observation, with AB magnitudes in whichever
of UVW2, UVM2, UVW1, U, B and V were used. Rows of one source share ``srcnum``, so the
rows of the nearest source form a multi-band light curve with one point per observation
and filter. Each point is a mean over the observation (hours), dated at its midpoint.

Each filter has a 13-bit quality flag. Points are kept when their only flags are in
XMMOM_ALLOWED_QUALITY: 32 (inside the central background enhancement, flagged whether or
not it matters) and 256 (point source within an extended source, i.e. nuclei of
galaxies). Everything else (bad pixel, readout streak, smoke ring, diffraction spike,
Mod-8/coincidence loss, near a bright source or edge, mixed exposure, Jupiter patch, too
bright) is rejected.

Catalogue errors are statistical only and reach ~0.001 mag for bright sources, so the
stored error is raised to at least XMMOM_MIN_MAG_ERROR; the catalogue value is kept as
``stat_error``.
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

from custom_code.data_services.forms import XMMOMQueryForm
from custom_code.data_services.service_utils import (
    DATA_SERVICE_HTTP_TIMEOUT,
    resolve_query_coordinates,
    upsert_reduced_datums,
)

logger = logging.getLogger(__name__)

XMMOM_PAGE_URL = 'https://www.cosmos.esa.int/web/xmm-newton/om-catalogue'
XMMOM_TAP_URL = 'https://nxsa.esac.esa.int/tap-server/tap/sync'
XMMOM_TABLE = 'xsa.v_om_source_cat'
XMMOM_RELEASE = 'XMM-SUSS 6.2'
# OM PSF is ~2" FWHM and SUSS positions agree to a fraction of an arcsec between observations.
XMMOM_DEFAULT_RADIUS_ARCSEC = 3.0

# SUSS column prefix -> BHTOM filter name.
XMMOM_FILTERS = {
    'uvw2': 'XMM-OM(UVW2)',
    'uvm2': 'XMM-OM(UVM2)',
    'uvw1': 'XMM-OM(UVW1)',
    'u': 'XMM-OM(U)',
    'b': 'XMM-OM(B)',
    'v': 'XMM-OM(V)',
}
XMMOM_ALLOWED_QUALITY = 32 | 256
XMMOM_MIN_MAG_ERROR = 0.02
XMMOM_MAX_MAG_ERROR = 1.0
XMMOM_MIN_MAG = 0.0
XMMOM_MAX_MAG = 30.0

XMMOM_ACKNOWLEDGEMENT = (
    'This research uses the XMM-Newton Optical Monitor Serendipitous Ultraviolet Source '
    'Survey catalogue (XMM-SUSS 6.2; Page et al. 2012, MNRAS 426, 903) from the '
    'XMM-Newton Science Archive. XMM-Newton is an ESA science mission with instruments and '
    'contributions directly funded by ESA Member States and NASA.'
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


def _build_xmmom_query(ra, dec, radius_arcsec):
    columns = ['srcnum', 'iauname', 'obsid', 'ra', 'dec', 'date_obs', 'date_end']
    for prefix in XMMOM_FILTERS:
        columns += [f'{prefix}_ab_mag', f'{prefix}_ab_mag_err', f'{prefix}_quality_flag']
    return f"""
    SELECT {', '.join(columns)},
           DISTANCE(POINT('ICRS', ra, dec), POINT('ICRS', {ra}, {dec})) * 3600 AS dist_arcsec
    FROM {XMMOM_TABLE}
    WHERE 1 = CONTAINS(POINT('ICRS', ra, dec), CIRCLE('ICRS', {ra}, {dec}, {radius_arcsec / 3600.0}))
    ORDER BY dist_arcsec
    """


def _tap_query(adql):
    """Run a synchronous ADQL query on the XMM-Newton Science Archive; rows as dicts."""
    response = requests.post(
        XMMOM_TAP_URL,
        data={'REQUEST': 'doQuery', 'LANG': 'ADQL', 'QUERY': adql},
        timeout=DATA_SERVICE_HTTP_TIMEOUT,
    )
    response.raise_for_status()
    table = parse_single_table(io.BytesIO(response.content), verify='ignore').to_table(use_names_over_ids=True)
    return [{name: row[name] for name in table.colnames} for row in table]


def _select_nearest_source(rows):
    """All detections (one per XMM observation) of the source nearest the query position."""
    rows = [row for row in rows if _to_int(row.get('srcnum')) is not None]
    if not rows:
        return []
    nearest = min(rows, key=lambda row: _to_float(row.get('dist_arcsec')) or 0.0)
    srcnum = _to_int(nearest['srcnum'])
    return [row for row in rows if _to_int(row['srcnum']) == srcnum]


def _observation_midpoint(row):
    """Midpoint of the XMM observation as an astropy Time, or None."""
    start, end = _text(row.get('date_obs')), _text(row.get('date_end'))
    try:
        t_start = Time(start, format='isot', scale='utc')
    except ValueError:
        return None
    try:
        t_end = Time(end, format='isot', scale='utc')
    except ValueError:
        return t_start
    return t_start + (t_end - t_start) / 2


class XMMOMDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return XMMOM_DEFAULT_RADIUS_ARCSEC

    name = 'XMMOM'
    verbose_name = 'XMM-OM SUSS (UV/optical)'
    # SUSS is a periodic catalogue release; nothing new appears between refreshes.
    update_on_daily_refresh = False
    info_url = XMMOM_PAGE_URL
    acknowledgement = XMMOM_ACKNOWLEDGEMENT
    upsert_identity_keys = ('filter', 'obsid')
    service_notes = (
        'Query the XMM-OM Serendipitous UV Source Survey (SUSS 6.2) in the XMM-Newton Science '
        'Archive by coordinates. All XMM observations of the nearest OM source are imported as '
        'UVW2/UVM2/UVW1/U/B/V AB magnitudes, one point per observation and filter. Points with '
        'quality flags other than central enhancement or extended host are rejected. Photometry '
        'only; no aliases are added.'
    )

    @classmethod
    def get_form_class(cls):
        return XMMOMQueryForm

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

        source_rows = []
        if ra is not None and dec is not None and query_parameters.get('include_photometry', True):
            try:
                source_rows = _select_nearest_source(_tap_query(_build_xmmom_query(ra, dec, radius_arcsec)))
                if not source_rows:
                    logger.debug('XMM-OM SUSS returned no source for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('XMM-OM SUSS query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                source_rows = []

        self.query_results = {
            'source_rows': source_rows,
            'source_location': XMMOM_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        source_rows = data.get('source_rows') or []
        if data.get('ra') is None or data.get('dec') is None or not source_rows:
            return []

        datums = self._build_photometry_datums(source_rows)
        if not datums:
            return []

        iauname = _text(source_rows[0].get('iauname'))
        return [{
            'name': iauname.replace(' ', '_') if iauname else f"XMMOM_{_to_int(source_rows[0]['srcnum'])}",
            'ra': data['ra'],
            'dec': data['dec'],
            # Photometry only: SUSS names are not names anyone searches for.
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

    def _build_photometry_datums(self, source_rows):
        output = []
        for row in source_rows:
            midpoint = _observation_midpoint(row)
            if midpoint is None:
                continue
            separation = _to_float(row.get('dist_arcsec'))
            for prefix, filter_name in XMMOM_FILTERS.items():
                mag = _to_float(row.get(f'{prefix}_ab_mag'))
                error = _to_float(row.get(f'{prefix}_ab_mag_err'))
                quality = _to_int(row.get(f'{prefix}_quality_flag'))
                if mag is None or error is None or quality is None:
                    continue
                if quality & ~XMMOM_ALLOWED_QUALITY:
                    continue
                if not (XMMOM_MIN_MAG < mag < XMMOM_MAX_MAG and 0 < error <= XMMOM_MAX_MAG_ERROR):
                    continue
                value = {
                    'filter': filter_name,
                    'magnitude': mag,
                    'error': max(error, XMMOM_MIN_MAG_ERROR),
                    'stat_error': error,
                    'mjd': float(midpoint.mjd),
                    'obsid': _text(row.get('obsid')),
                    'xmmom_srcnum': _to_int(row.get('srcnum')),
                    'quality_flag': quality,
                    'mag_system': 'AB',
                    'data_release': XMMOM_RELEASE,
                }
                if separation is not None:
                    value['match_separation_arcsec'] = round(separation, 4)
                output.append({
                    'timestamp': midpoint.to_datetime(timezone=timezone.utc),
                    'value': value,
                })
        return output
