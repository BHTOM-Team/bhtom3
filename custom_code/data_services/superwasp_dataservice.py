"""SuperWASP DR1 unphased photometry from the NASA Exoplanet Archive.

The original public archive (https://wasp.cerit-sc.cz/) offers a useful coordinate
search and a documented CSV link, but that CSV contains only rounded, SYSREM-corrected
magnitudes and a camera number.  The NASA mirror exposes the same DR1 rows through
stable per-source IPAC tables and retains MAG2, TAMMAG2, IMAGEID, CCD coordinates and
quality flags.  Consequently this adapter uses NASA TAP for source discovery and the
NASA table file for photometry.  No rendered HTML is scraped.

The archive column named HJD is HJD_UTC.  ``ReducedDatum.timestamp`` cannot encode a
heliocentric time scale, so it receives the UTC datetime with the same Julian-day
number solely for BHTOM plotting/querying.  Every row retains ``original_hjd_utc`` and
``time_standard='HJD_UTC'``; it must not be interpreted as BJD_TDB.  The helper
``hjd_utc_to_bjd_tdb`` provides the documented HJD -> BJD conversion path for timing
work and leaves the archived number untouched.
"""

import ast
import logging
import math
import re
from datetime import timezone
from urllib.parse import quote

import numpy as np
import requests
from astropy import units as u
from astropy.constants import c
from astropy.coordinates import SkyCoord, get_body_barycentric
from astropy.table import Table
from astropy.time import Time
from django.conf import settings
from django.utils import timezone as django_timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import SuperWASPQueryForm
from custom_code.data_services.service_utils import (
    DATA_SERVICE_HTTP_TIMEOUT,
    resolve_query_coordinates,
    upsert_reduced_datums,
)


logger = logging.getLogger(__name__)

SUPERWASP_INFO_URL = 'https://exoplanetarchive.ipac.caltech.edu/docs/SuperWASPMission.html'
SUPERWASP_TAP_URL = 'https://exoplanetarchive.ipac.caltech.edu/TAP/sync'
SUPERWASP_DATA_ROOT = 'https://exoplanetarchive.ipac.caltech.edu/data/ETSS/SuperWASP/TBL/DR1'
SUPERWASP_ORIGINAL_ARCHIVE_URL = 'https://wasp.cerit-sc.cz/form'
SUPERWASP_DOI = '10.26133/NEA9'
SUPERWASP_RELEASE = 'WASP DR1'

CORRECTED_FILTER = 'WASP/SuperWASP (TAMMAG2)'
DEFAULT_MATCH_RADIUS_ARCSEC = 5.0
WASP_ID_RE = re.compile(r'^1SWASP\s+J\d{6}(?:\.\d+)?[+-]\d{6}(?:\.\d+)?$', re.I)
TILE_RE = re.compile(r'^tile\d{6}$')
RETRYABLE_HTTP_STATUSES = (429, 500, 502, 503, 504)

SUPERWASP_ACKNOWLEDGEMENT = (
    'This paper makes use of data from the first public release of the WASP data '
    '(Butters et al. 2010) as provided by the WASP consortium and services at the '
    'NASA Exoplanet Archive, which is operated by the California Institute of '
    'Technology, under contract with the National Aeronautics and Space Administration '
    'under the Exoplanet Exploration Program. DOI 10.26133/NEA9.'
)
# Kept as the per-row provenance key value.  Download builders should call the
# service-level get_acknowledgement() method instead of reading datum internals.
ATTRIBUTION = SUPERWASP_ACKNOWLEDGEMENT


class SuperWASPMatchError(ValueError):
    """Base class for a source-selection failure."""


class SuperWASPAmbiguousMatchError(SuperWASPMatchError):
    """Raised when a cone contains more than one DR1 source."""


def _to_float(value):
    try:
        if value is None or np.ma.is_masked(value):
            return None
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _to_int(value):
    value = _to_float(value)
    return int(value) if value is not None else None


def _to_text(value):
    if value is None or np.ma.is_masked(value):
        return ''
    if isinstance(value, bytes):
        value = value.decode('utf-8', 'replace')
    return str(value).strip()


def _normalise_wasp_id(value):
    value = ' '.join(str(value or '').strip().split())
    return value if WASP_ID_RE.match(value) else ''


def _angular_separation_arcsec(ra1, dec1, ra2, dec2):
    return float(SkyCoord(ra1, dec1, unit='deg').separation(SkyCoord(ra2, dec2, unit='deg')).arcsec)


def _metadata_query(ra=None, dec=None, radius_arcsec=None, wasp_id=''):
    columns = 'sourceid,ra,dec,tile,npts,hjd_ref,hjdstart,hjdstop,obsstart,obsstop'
    if wasp_id:
        escaped = wasp_id.replace("'", "''")
        return f"select top 2 {columns} from superwasptimeseries where sourceid='{escaped}'"

    # CONTAINS currently fails for this legacy table because the Exoplanet Archive's
    # spatial-index expression references a retired HTM20 column.  A small indexed
    # RA/Dec bounding box plus an exact SkyCoord separation is deterministic and TAP,
    # not HTML scraping.  Account for RA convergence and wrap at 0/360 degrees.
    radius_deg = float(radius_arcsec) / 3600.0
    dec_min = max(-90.0, float(dec) - radius_deg)
    dec_max = min(90.0, float(dec) + radius_deg)
    cos_dec = max(abs(math.cos(math.radians(float(dec)))), 1e-6)
    ra_half_width = min(180.0, radius_deg / cos_dec)
    ra_min = float(ra) - ra_half_width
    ra_max = float(ra) + ra_half_width
    if ra_min < 0:
        ra_clause = f'(ra>={ra_min + 360.0:.12f} or ra<={ra_max:.12f})'
    elif ra_max >= 360:
        ra_clause = f'(ra>={ra_min:.12f} or ra<={ra_max - 360.0:.12f})'
    else:
        ra_clause = f'ra between {ra_min:.12f} and {ra_max:.12f}'
    return (
        f'select {columns} from superwasptimeseries '
        f'where {ra_clause} and dec between {dec_min:.12f} and {dec_max:.12f}'
    )


def _lightcurve_url(source_id, tile):
    if not TILE_RE.match(str(tile or '')):
        raise ValueError(f'Unexpected SuperWASP tile value: {tile!r}')
    source_id = _normalise_wasp_id(source_id)
    if not source_id:
        raise ValueError('Unexpected SuperWASP source ID in TAP response.')
    filename = source_id.replace(' ', '_', 1) + '_lc.tbl'
    return f'{SUPERWASP_DATA_ROOT}/{tile}/{quote(filename)}'


def _parse_ipac_header(text):
    """Return scalar backslash-header values from one NASA IPAC table."""
    header = {}
    for line in (text or '').splitlines():
        if line.startswith('|'):
            break
        match = re.match(r'^\\([A-Z0-9_]+)\s*=\s*(.*?)\s*$', line)
        if not match:
            continue
        key, raw = match.groups()
        try:
            value = ast.literal_eval(raw)
        except (SyntaxError, ValueError):
            value = raw.strip().strip("'")
        header[key] = value
    return header


def hjd_utc_to_datetime(hjd_utc):
    """Map an archived HJD_UTC number to BHTOM's timezone-aware timestamp field.

    This preserves numeric ordering and precision but does not change the heliocentric
    correction or relabel the value as an ordinary geocentric JD.
    """
    return Time(float(hjd_utc), format='jd', scale='utc').to_datetime(timezone=timezone.utc)


def hjd_utc_to_bjd_tdb(hjd_utc, ra_deg, dec_deg):
    """Convert HJD_UTC to BJD_TDB without altering the archived HJD value.

    The observatory-to-Earth term cancels between HJD and BJD.  The remaining terms
    are UTC -> TDB and the Sun-to-barycentre projection along the target direction.
    This is the appropriate conversion path for pulsation timing; callers should keep
    ``original_hjd_utc`` as the authoritative input and record the ephemeris version.
    """
    hjd_as_utc = Time(float(hjd_utc), format='jd', scale='utc')
    direction = SkyCoord(float(ra_deg), float(dec_deg), unit='deg').icrs.cartesian
    sun = get_body_barycentric('sun', hjd_as_utc.tdb)
    sun_projection_days = (
        (sun.x * direction.x + sun.y * direction.y + sun.z * direction.z).to(u.m)
        / c
    ).to_value(u.day)
    return float(hjd_utc) + float(hjd_as_utc.tdb.jd - hjd_as_utc.utc.jd) + sun_projection_days


def parse_superwasp_ipac(text, *, metadata, source_url, retrieved_at=None):
    """Parse an unphased DR1 IPAC table into BHTOM ReducedDatum dictionaries."""
    header = _parse_ipac_header(text)
    required_header = {'OBJNAME', 'JD_REF'}
    if not required_header.issubset(header):
        raise ValueError(f'SuperWASP table is missing headers: {sorted(required_header - set(header))}')

    source_id = _normalise_wasp_id(header.get('OBJNAME'))
    expected_id = _normalise_wasp_id(metadata.get('sourceid'))
    if not source_id or (expected_id and source_id != expected_id):
        raise ValueError(f'SuperWASP table object {source_id!r} does not match TAP object {expected_id!r}.')

    table = Table.read((text or '').splitlines(), format='ascii.ipac')
    required_columns = {
        'TMID', 'FLUX2', 'FLUX2_ERR', 'TAMFLUX2', 'TAMFLUX2_ERR',
        'IMAGEID', 'CCDX', 'CCDY', 'FLAG', 'HJD', 'MAG2', 'MAG2_ERR',
        'TAMMAG2', 'TAMMAG2_ERR',
    }
    if not required_columns.issubset(table.colnames):
        raise ValueError(f'SuperWASP table is missing columns: {sorted(required_columns - set(table.colnames))}')
    header_row_count = header.get('NUMRECORDS', header.get('NPTS'))
    if header_row_count is None:
        raise ValueError('SuperWASP table is missing its NUMRECORDS header.')
    if int(header_row_count) != len(table):
        raise ValueError(
            f'SuperWASP NUMRECORDS={header_row_count} but the table contains {len(table)} rows.'
        )

    retrieved_at = retrieved_at or django_timezone.now()
    retrieval_date = retrieved_at.astimezone(timezone.utc).isoformat()
    separation = _to_float(metadata.get('separation_arcsec'))
    jd_ref = _to_float(header.get('JD_REF'))
    output = []

    for row in table:
        hjd = _to_float(row['HJD'])
        tmid = _to_int(row['TMID'])
        image_id = _to_text(row['IMAGEID'])
        if hjd is None or tmid is None or not image_id:
            continue
        computed_hjd = jd_ref + tmid / 86400.0 if jd_ref is not None else None
        if computed_hjd is not None and abs(hjd - computed_hjd) > 2e-6:
            raise ValueError(
                f'SuperWASP HJD/header mismatch for IMAGEID={image_id}: '
                f'file={hjd}, JD_REF+TMID={computed_hjd}'
            )

        common = {
            'wasp_id': source_id,
            'wasp_ra_deg': _to_float(metadata.get('ra')),
            'wasp_dec_deg': _to_float(metadata.get('dec')),
            'observation_key': f'{SUPERWASP_RELEASE}:{source_id}:{tmid}:{image_id}',
            'original_hjd_utc': hjd,
            'time_standard': 'HJD_UTC',
            'timestamp_mapping': 'HJD_UTC numeric value mapped to UTC datetime; not BJD_TDB',
            'hjd_reference_utc': jd_ref,
            'tmid_seconds': tmid,
            'image_id': image_id,
            'camera_id': image_id[:3] if len(image_id) >= 3 else '',
            'ccd_x_sixteenth_pixel': _to_int(row['CCDX']),
            'ccd_y_sixteenth_pixel': _to_int(row['CCDY']),
            'quality_flag': _to_int(row['FLAG']),
            'flux2_microvega': _to_float(row['FLUX2']),
            'flux2_error_microvega': _to_float(row['FLUX2_ERR']),
            'tamflux2_microvega': _to_float(row['TAMFLUX2']),
            'tamflux2_error_microvega': _to_float(row['TAMFLUX2_ERR']),
            'archive_mag2': _to_float(row['MAG2']),
            'archive_mag2_error': _to_float(row['MAG2_ERR']),
            'archive_tammag2': _to_float(row['TAMMAG2']),
            'archive_tammag2_error': _to_float(row['TAMMAG2_ERR']),
            'data_release': SUPERWASP_RELEASE,
            'source_archive': 'NASA Exoplanet Archive SuperWASP mirror',
            'original_archive': SUPERWASP_ORIGINAL_ARCHIVE_URL,
            'source_url': source_url,
            'retrieved_at': retrieval_date,
            'doi': SUPERWASP_DOI,
            'attribution': ATTRIBUTION,
        }
        if separation is not None:
            common['match_separation_arcsec'] = round(separation, 6)

        timestamp = hjd_utc_to_datetime(hjd)
        corrected_mag = common['archive_tammag2']
        if corrected_mag is not None:
            corrected = {
                **common,
                'filter': CORRECTED_FILTER,
                'wasp_series': 'TAMMAG2',
                'magnitude': corrected_mag,
            }
            if common['archive_tammag2_error'] is not None:
                corrected['error'] = common['archive_tammag2_error']
            output.append({'timestamp': timestamp, 'value': corrected})

    return output, header


class SuperWASPDataService(DataService):
    name = 'SuperWASP'
    verbose_name = 'SuperWASP (WASP DR1)'
    update_on_daily_refresh = True
    info_url = SUPERWASP_INFO_URL
    acknowledgement = SUPERWASP_ACKNOWLEDGEMENT
    acknowledgement_url = SUPERWASP_INFO_URL
    acknowledgement_doi = SUPERWASP_DOI
    # The scheduled bulk-ingestion path uses this stable key rather than comparing the
    # whole provenance dictionary (whose retrieval timestamp can change).
    upsert_identity_keys = ('observation_key', 'filter')
    service_notes = (
        'Discover one SuperWASP DR1 source by coordinates and ingest its individual '
        'unphased systematics-corrected TAMMAG2 measurements from the NASA Exoplanet '
        'Archive. Original MAG2 values remain in each point\'s provenance metadata.'
    )

    @classmethod
    def get_form_class(cls):
        return SuperWASPQueryForm

    @classmethod
    def get_acknowledgement(cls):
        """Return text suitable for appending to a data export's acknowledgements."""
        return cls.acknowledgement

    def _get_http_session(self):
        session = getattr(self, '_superwasp_http_session', None)
        if session is None:
            retries = int(getattr(settings, 'SUPERWASP_HTTP_RETRIES', 3))
            retry = Retry(
                total=retries,
                connect=retries,
                read=retries,
                status=retries,
                backoff_factor=float(getattr(settings, 'SUPERWASP_HTTP_RETRY_BACKOFF', 1.0)),
                status_forcelist=RETRYABLE_HTTP_STATUSES,
                allowed_methods=frozenset({'GET'}),
                respect_retry_after_header=True,
                raise_on_status=False,
            )
            session = requests.Session()
            session.headers.update({'User-Agent': 'BHTOM3 SuperWASP data service'})
            session.mount('https://', HTTPAdapter(max_retries=retry))
            self._superwasp_http_session = session
        return session

    def _target_wasp_id(self, target_id):
        if not target_id:
            return ''
        try:
            target = Target.objects.get(pk=target_id)
        except Target.DoesNotExist:
            return ''
        for value in [target.name, *target.aliases.values_list('name', flat=True)]:
            wasp_id = _normalise_wasp_id(value)
            if wasp_id:
                return wasp_id
        return ''

    def build_query_parameters(self, parameters, **kwargs):
        target_name, ra, dec = resolve_query_coordinates(parameters)
        default_radius = float(getattr(settings, 'SUPERWASP_MATCH_RADIUS_ARCSEC', DEFAULT_MATCH_RADIUS_ARCSEC))
        wasp_id = _normalise_wasp_id(parameters.get('wasp_id'))
        if not wasp_id:
            wasp_id = self._target_wasp_id(parameters.get('target_id'))
        self.query_parameters = {
            'target_name': target_name,
            'target_id': parameters.get('target_id'),
            'wasp_id': wasp_id,
            'ra': _to_float(ra),
            'dec': _to_float(dec),
            'radius_arcsec': _to_float(parameters.get('radius_arcsec')) or default_radius,
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def _fetch_metadata(self, *, ra=None, dec=None, radius_arcsec=None, wasp_id=''):
        query = _metadata_query(ra=ra, dec=dec, radius_arcsec=radius_arcsec, wasp_id=wasp_id)
        response = self._get_http_session().get(
            SUPERWASP_TAP_URL,
            params={'query': query, 'format': 'json'},
            timeout=DATA_SERVICE_HTTP_TIMEOUT,
        )
        response.raise_for_status()
        rows = response.json()
        if wasp_id:
            return rows
        matches = []
        for row in rows:
            row_ra, row_dec = _to_float(row.get('ra')), _to_float(row.get('dec'))
            if row_ra is None or row_dec is None:
                continue
            separation = _angular_separation_arcsec(ra, dec, row_ra, row_dec)
            if separation <= radius_arcsec + 1e-9:
                matches.append({**row, 'separation_arcsec': separation})
        return sorted(matches, key=lambda row: row['separation_arcsec'])

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius = _to_float(query_parameters.get('radius_arcsec')) or DEFAULT_MATCH_RADIUS_ARCSEC
        wasp_id = _normalise_wasp_id(query_parameters.get('wasp_id'))
        if not wasp_id and (ra is None or dec is None):
            self.query_results = {'status': 'missing_coordinates', 'rows': [], 'source_location': self.info_url}
            return self.query_results

        matches = self._fetch_metadata(ra=ra, dec=dec, radius_arcsec=radius, wasp_id=wasp_id)
        if not matches:
            logger.info(
                'SuperWASP: no DR1 source matched %s within %s arcsec.',
                wasp_id or f'RA={ra} Dec={dec}',
                radius,
            )
            self.query_results = {'status': 'no_match', 'rows': [], 'source_location': self.info_url}
            return self.query_results
        if len(matches) > 1:
            summary = ', '.join(
                f'{row.get("sourceid")} ({row.get("separation_arcsec"):.3f} arcsec)'
                for row in matches[:10]
            )
            raise SuperWASPAmbiguousMatchError(
                f'SuperWASP coordinate match is ambiguous within {radius:g} arcsec: {summary}. '
                'Supply an exact SuperWASP source ID.'
            )

        match = dict(matches[0])
        if ra is not None and dec is not None and match.get('separation_arcsec') is None:
            match['separation_arcsec'] = _angular_separation_arcsec(
                ra, dec, float(match['ra']), float(match['dec'])
            )
        source_url = _lightcurve_url(match['sourceid'], match['tile'])
        datums = []
        header = {}
        if query_parameters.get('include_photometry', True):
            response = self._get_http_session().get(source_url, timeout=DATA_SERVICE_HTTP_TIMEOUT)
            response.raise_for_status()
            datums, header = parse_superwasp_ipac(
                response.text,
                metadata=match,
                source_url=source_url,
                retrieved_at=django_timezone.now(),
            )
        self.query_results = {
            'status': 'matched',
            'match': match,
            'datums': datums,
            'header': header,
            'source_location': source_url,
            'ra': ra,
            'dec': dec,
        }
        logger.info(
            'SuperWASP: matched %s at %.3f arcsec; downloaded %s archive rows (%s BHTOM series points).',
            match['sourceid'],
            match.get('separation_arcsec') or 0.0,
            header.get('NUMRECORDS', header.get('NPTS', 0)),
            len(datums),
        )
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        match = data.get('match')
        if not match:
            return []
        source_id = match['sourceid']
        source_url = data['source_location']
        return [{
            'name': source_id,
            'ra': _to_float(match.get('ra')),
            'dec': _to_float(match.get('dec')),
            'aliases': [{
                'name': source_id,
                'url': source_url,
                'source_name': 'SuperWASP / NASA Exoplanet Archive',
            }],
            'match_separation_arcsec': match.get('separation_arcsec'),
            'reduced_datums': {'photometry': data.get('datums') or []},
            'source_location': source_url,
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
        aliases = []
        for alias in alias_results:
            name = alias.get('name') if isinstance(alias, dict) else alias
            if name:
                aliases.append(TargetName(name=name))
        return aliases

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
