"""Bochum Galactic Disk Survey (BGDS) DR2 light curves from the GAVO Data Center TAP service.

BGDS monitored a 6 degree wide stripe along the Galactic plane from the Robotic Bochum
Twin Telescope (RoBoTT, Cerro Armazones) between 2010 and 2019, nightly in Sloan r and i
with intermittent Johnson UBV, Sloan z and narrowband visits. Each point is the mean of
nine 10 s exposures.

``bgds2.lc_all`` holds one row per object, band and survey field, with the whole light
curve in the ``mjds``/``mags``/``mag_errs`` array columns, so a single cone query returns
everything. A star in overlapping fields has up to four rows per band. Rows of the nearest
object are grouped by its Gaia DR3 source_id (every DR2 row carries one), falling back to
a small positional match when the id is missing. MJDs are topocentric UTC.
"""

import csv
import logging
import math
from datetime import timezone
from io import StringIO

import requests
from astropy.time import Time

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target

from custom_code.data_services.forms import BGDSQueryForm
from custom_code.data_services.service_utils import (
    DATA_SERVICE_HTTP_TIMEOUT,
    resolve_query_coordinates,
    upsert_reduced_datums,
)

logger = logging.getLogger(__name__)

BGDS_PAGE_URL = 'https://dc.g-vo.org/browse/bgds/l2'
BGDS_TAP_URL = 'https://dc.g-vo.org/tap/sync'
BGDS_RELEASE = 'BGDS DR2'
# The Galactic plane is crowded; a wider cone mostly adds neighbours.
BGDS_DEFAULT_RADIUS_ARCSEC = 2.0
# Without a Gaia id, rows of other bands/fields within this distance of the nearest row
# are the same star (r and i centroids of one star differ by ~0.2").
BGDS_SAME_SOURCE_ARCSEC = 1.0

# BGDS band_name -> BHTOM filter name. Rows in any other band are dropped.
BGDS_FILTERS = {
    "SDSS r'": 'BGDS(r)',
    "SDSS i'": 'BGDS(i)',
    "SDSS z'": 'BGDS(z)',
    'Johnson U': 'BGDS(U)',
    'Johnson B': 'BGDS(B)',
    'Johnson V': 'BGDS(V)',
    'Astrodon Halpha': 'BGDS(Halpha)',
    'Astrodon NB': 'BGDS(NB)',
    'Astrodon OIII': 'BGDS(OIII)',
    'Astrodon SII': 'BGDS(SII)',
}
BGDS_MIN_MAG = 0.0
BGDS_MAX_MAG = 30.0
BGDS_MAX_MAG_ERROR = 2.5

BGDS_ACKNOWLEDGEMENT = (
    'This research uses data from the Bochum Galactic Disk Survey DR2 '
    '(2026AN....34770087B), obtained from the GAVO Data Center '
    '(doi:10.21938/dChYyzuCGA00rsfzfQ8v:Q).'
)


def _to_float(value):
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def _parse_array(value):
    """Parse a TAP CSV array cell like '[1.0, 2.0]' into floats (None for bad entries)."""
    text = str(value or '').strip().strip('[]')
    if not text:
        return []
    return [_to_float(item.strip()) for item in text.split(',')]


def _build_bgds_query(ra, dec, radius_arcsec):
    return f"""
    SELECT obs_id, band_name, ra, dec, field, gdr3_id, mjds, mags, mag_errs,
           DISTANCE(POINT(ra, dec), POINT({ra}, {dec})) * 3600 AS dist_arcsec
    FROM bgds2.lc_all
    WHERE 1 = CONTAINS(POINT(ra, dec), CIRCLE({ra}, {dec}, {radius_arcsec / 3600.0}))
    ORDER BY dist_arcsec
    """


def _tap_query(adql):
    """Run a synchronous ADQL query on the GAVO DC and return rows as dicts of strings."""
    response = requests.post(
        BGDS_TAP_URL,
        data={'REQUEST': 'doQuery', 'LANG': 'ADQL', 'FORMAT': 'csv', 'QUERY': adql},
        timeout=DATA_SERVICE_HTTP_TIMEOUT,
    )
    response.raise_for_status()
    text = response.text
    # TAP errors come back as a VOTable with QUERY_STATUS=ERROR instead of CSV.
    if text.lstrip().startswith('<'):
        raise RuntimeError(f'BGDS TAP query failed: {text[:300]}')
    return list(csv.DictReader(StringIO(text)))


def _angular_separation_arcsec(ra1, dec1, ra2, dec2):
    ra1, dec1, ra2, dec2 = map(math.radians, (ra1, dec1, ra2, dec2))
    cos_sep = (math.sin(dec1) * math.sin(dec2)
               + math.cos(dec1) * math.cos(dec2) * math.cos(ra1 - ra2))
    return math.degrees(math.acos(min(1.0, max(-1.0, cos_sep)))) * 3600.0


def _select_nearest_source(rows):
    """Rows (all bands and fields) of the object nearest the query position."""
    rows = [row for row in rows if _to_float(row.get('ra')) is not None and _to_float(row.get('dec')) is not None]
    if not rows:
        return []
    nearest = min(rows, key=lambda row: _to_float(row.get('dist_arcsec')) or 0.0)
    gaia_id = str(nearest.get('gdr3_id') or '').strip()
    if gaia_id:
        return [row for row in rows if str(row.get('gdr3_id') or '').strip() == gaia_id]
    ra0, dec0 = _to_float(nearest['ra']), _to_float(nearest['dec'])
    return [
        row for row in rows
        if _angular_separation_arcsec(ra0, dec0, _to_float(row['ra']), _to_float(row['dec'])) <= BGDS_SAME_SOURCE_ARCSEC
    ]


class BGDSDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return BGDS_DEFAULT_RADIUS_ARCSEC

    name = 'BGDS'
    verbose_name = 'Bochum Galactic Disk Survey DR2'
    # DR2 is a frozen release; nothing new appears between refreshes.
    update_on_daily_refresh = False
    info_url = BGDS_PAGE_URL
    acknowledgement = BGDS_ACKNOWLEDGEMENT
    # Overlapping fields give separate light curves of one star; obs_id keeps them apart.
    upsert_identity_keys = ('obs_id', 'filter')
    service_notes = (
        'Query Bochum Galactic Disk Survey DR2 light curves (Galactic plane, 2010-2019; '
        'r and i nightly, occasional UBV, z and narrowbands) from the GAVO Data Center by '
        'coordinates. All bands and overlapping fields of the nearest object are imported. '
        'Photometry only; no aliases are added.'
    )

    @classmethod
    def get_form_class(cls):
        return BGDSQueryForm

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
                source_rows = _select_nearest_source(_tap_query(_build_bgds_query(ra, dec, radius_arcsec)))
                if not source_rows:
                    logger.debug('BGDS returned no object for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('BGDS query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                source_rows = []

        self.query_results = {
            'source_rows': source_rows,
            'source_location': BGDS_PAGE_URL,
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

        gaia_id = str(source_rows[0].get('gdr3_id') or '').strip()
        return [{
            'name': f'BGDS_{gaia_id}' if gaia_id else str(source_rows[0].get('obs_id')),
            'ra': data['ra'],
            'dec': data['dec'],
            # Photometry only: BGDS obs_ids are not names anyone searches for.
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
            filter_name = BGDS_FILTERS.get(str(row.get('band_name') or '').strip())
            if filter_name is None:
                continue
            mjds = _parse_array(row.get('mjds'))
            mags = _parse_array(row.get('mags'))
            errors = _parse_array(row.get('mag_errs'))
            if not (len(mjds) == len(mags) == len(errors)):
                logger.warning('BGDS %s has mismatched array lengths; skipped', row.get('obs_id'))
                continue
            separation = _to_float(row.get('dist_arcsec'))
            for mjd, mag, error in zip(mjds, mags, errors):
                if mjd is None or mag is None or error is None:
                    continue
                if not (BGDS_MIN_MAG < mag < BGDS_MAX_MAG and 0 < error <= BGDS_MAX_MAG_ERROR):
                    continue
                value = {
                    'filter': filter_name,
                    'magnitude': mag,
                    'error': error,
                    'mjd': mjd,
                    'obs_id': str(row.get('obs_id') or ''),
                    'field': str(row.get('field') or ''),
                    'gaia_dr3_id': str(row.get('gdr3_id') or ''),
                    'data_release': BGDS_RELEASE,
                }
                if separation is not None:
                    value['match_separation_arcsec'] = round(separation, 4)
                output.append({
                    'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                    'value': value,
                })
        return output
