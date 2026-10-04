"""KELT (Kilodegree Extremely Little Telescope) light curves from the NASA Exoplanet Archive.

KELT-North and KELT-South (Pepper et al. 2007, 2012) monitored ~70% of the sky from 2005 to
~2014 for bright stars (V ~ 8-11, usable to ~14) with 23"/pixel cameras, typically ~5,000-9,000
points per field over several years. The archive holds the KELT DR1 light curves (Oelkers et
al. 2018): the kelttimeseries table lists each source (field, position, KELT magnitude), and each
light curve is an IPAC table (BJD_TDB, MAG, MAG_ERR) in a raw and a TFA-detrended version.

Two quirks of the archive shape this module:
1. File paths are not derivable from the source id (e.g. .../KELT2/005/074/28/KELT_N04_lc_000001
   _V01_east_tfa_lc.tbl, with directories assigned per file). The only mapping is the archive's
   bulk wget scripts (KELT_wget.tar.gz, ~68 MB), so they are downloaded once and reduced to a
   compact index (~11 MB, KELT_CACHE_DIR) of the directory of every east light curve (~30 s).
2. Every source id is an 'east' id. The telescope's west-orientation light curves of the same star
   are separate files with their own, unlinked numbers, so only east light curves are used; they
   normally hold most of a star's points.

The nearest KELT source within the radius is used, together with its light curves from other
overlapping fields (same star within KELT_SAME_STAR_ARCSEC). Points with errors in (0, 1] mag
are kept; times are barycentric (BJD_TDB).

Magnitudes. KELT DR1 magnitudes are instrumental, with an arbitrary zero point per telescope:
compared with SIMBAD V for ~100 stars in 7 fields, KELT - V is +3.79 to +3.94 for KELT-North
fields and +2.22 to +2.39 for KELT-South fields (star-to-star scatter 0.1-0.3 mag, mostly colour).
The stored magnitude is shifted by the per-telescope median (KELT_V_OFFSETS) so KELT sits near V on
the plot; the instrumental value is kept as 'instrumental_magnitude'. Relative variability is
unaffected; the absolute level is good to ~0.2-0.3 mag.
"""

import io
import logging
import math
import os
import re
import shutil
import tarfile
import tempfile
import threading
from datetime import timezone

import numpy as np
import requests
from astropy.table import Table
from astropy.time import Time
from django.conf import settings

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target

from custom_code.data_services.forms import KELTQueryForm
from custom_code.data_services.service_utils import (
    DATA_SERVICE_HTTP_TIMEOUT,
    resolve_query_coordinates,
    upsert_reduced_datums,
)

logger = logging.getLogger(__name__)

KELT_PAGE_URL = 'https://exoplanetarchive.ipac.caltech.edu/docs/KELT.html'
KELT_TAP_URL = 'https://exoplanetarchive.ipac.caltech.edu/TAP/sync'
KELT_DATA_ROOT = 'https://exoplanetarchive.ipac.caltech.edu/data/ETSS//KELT2'
KELT_WGET_URL = 'https://exoplanetarchive.ipac.caltech.edu/bulk_data_download/KELT_wget.tar.gz'
KELT_INDEX_FILE = 'kelt_east_index-v1.npz'
KELT_DEFAULT_RADIUS_ARCSEC = 10.0
KELT_SAME_STAR_ARCSEC = 2.0
KELT_MAX_MAG_ERROR = 1.0
KELT_FILTER = 'KELT'
# Median KELT instrumental magnitude minus SIMBAD V, per telescope (N: fields N02/N04/N08/N12, S: S05/S13/S36).
KELT_V_OFFSETS = {'N': 3.83, 'S': 2.38}
KELT_HTTP_TIMEOUT = (10, 600)
KELT_SOURCE_RE = re.compile(r'^KELT_([NS])(\d{2})_lc_(\d{6})_(V\d+)_east$')
_WGET_RE = re.compile(rb'ETSS//KELT2/(\d{3})/(\d{3})/(\d{2})/KELT_([NS])(\d{2})_lc_(\d{6})_V\d+_east_(raw|tfa)_lc\.tbl')

KELT_ACKNOWLEDGEMENT = (
    'This research has made use of the KELT light curves (Oelkers et al. 2018, AJ 155, 39) and the '
    'NASA Exoplanet Archive, which is operated by the California Institute of Technology, under '
    'contract with the National Aeronautics and Space Administration under the Exoplanet '
    'Exploration Program.'
)

_index_lock = threading.Lock()
_index_cache = {}


def _to_float(value):
    if value is None or value is np.ma.masked:
        return None
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def _index_key(hemisphere, field, number, proc_type):
    """One int64 per (field, source number, raw/tfa); hemisphere S fields are offset by 100."""
    field_code = (0 if hemisphere in ('N', b'N') else 100) + int(field)
    return (field_code * 1_000_000 + int(number)) * 2 + (1 if proc_type in ('tfa', b'tfa') else 0)


def _cache_dir():
    path = getattr(settings, 'KELT_CACHE_DIR', None) or os.path.join(tempfile.gettempdir(), 'bhtom3-kelt-cache')
    os.makedirs(path, exist_ok=True)
    return path


def _build_index(path):
    response = requests.get(KELT_WGET_URL, timeout=KELT_HTTP_TIMEOUT)
    response.raise_for_status()
    keys, dirs = [], []
    with tarfile.open(fileobj=io.BytesIO(response.content), mode='r:gz') as archive:
        for member in archive:
            if not member.isfile():
                continue
            for match in _WGET_RE.finditer(archive.extractfile(member).read()):
                d1, d2, d3, hemisphere, field, number, proc_type = match.groups()
                keys.append(_index_key(hemisphere, field, number, proc_type))
                dirs.append(int(d1) * 100000 + int(d2) * 100 + int(d3))
    keys = np.asarray(keys, dtype=np.int64)
    dirs = np.asarray(dirs, dtype=np.int32)
    order = np.argsort(keys)
    tmp_path = path + f'.tmp{os.getpid()}.npz'
    np.savez_compressed(tmp_path, key=keys[order], dir=dirs[order])
    os.replace(tmp_path, path)


def _load_index():
    with _index_lock:
        if not _index_cache:
            path = os.path.join(_cache_dir(), KELT_INDEX_FILE)
            if not os.path.exists(path):
                _build_index(path)
            with np.load(path) as loaded:
                _index_cache['key'] = loaded['key']
                _index_cache['dir'] = loaded['dir']
        return _index_cache['key'], _index_cache['dir']


def _lightcurve_url(source_id, proc_type):
    match = KELT_SOURCE_RE.match(source_id)
    if not match:
        return None
    hemisphere, field, number, version = match.groups()
    keys, dirs = _load_index()
    key = _index_key(hemisphere, field, number, proc_type)
    i = int(np.searchsorted(keys, key))
    if i >= len(keys) or keys[i] != key:
        return None
    d = int(dirs[i])
    directory = f'{d // 100000:03d}/{(d // 100) % 1000:03d}/{d % 100:02d}'
    return f'{KELT_DATA_ROOT}/{directory}/{source_id}_{proc_type}_lc.tbl'


def _angular_separation_arcsec(ra1, dec1, ra2, dec2):
    ra1, dec1, ra2, dec2 = map(math.radians, (ra1, dec1, ra2, dec2))
    cos_sep = (math.sin(dec1) * math.sin(dec2)
               + math.cos(dec1) * math.cos(dec2) * math.cos(ra1 - ra2))
    return math.degrees(math.acos(min(1.0, max(-1.0, cos_sep)))) * 3600.0


def _find_sources(ra, dec, radius_arcsec, proc_type):
    """KELT source ids (one per field) of the star nearest the position, with their separation."""
    half = radius_arcsec / 3600.0
    half_ra = min(180.0, half / max(abs(math.cos(math.radians(dec))), 1e-6))
    ra_min, ra_max = ra - half_ra, ra + half_ra
    if ra_min < 0:
        ra_clause = f'(ra >= {ra_min + 360:.8f} or ra <= {ra_max:.8f})'
    elif ra_max >= 360:
        ra_clause = f'(ra >= {ra_min:.8f} or ra <= {ra_max - 360:.8f})'
    else:
        ra_clause = f'ra between {ra_min:.8f} and {ra_max:.8f}'
    query = (
        'select kelt_sourceid, kelt_field, ra, dec, kelt_mag, npts from kelttimeseries '
        f"where {ra_clause} and dec between {dec - half:.8f} and {dec + half:.8f} "
        f"and kelt_orientation = 'east' and proc_type = '{proc_type}'"
    )
    response = requests.get(KELT_TAP_URL, params={'query': query, 'format': 'csv'}, timeout=DATA_SERVICE_HTTP_TIMEOUT)
    response.raise_for_status()
    rows = Table.read(response.text, format='ascii.csv') if response.text.strip().count('\n') else []
    candidates = []
    for row in rows:
        src_ra, src_dec = _to_float(row['ra']), _to_float(row['dec'])
        if src_ra is None or src_dec is None:
            continue
        separation = _angular_separation_arcsec(ra, dec, src_ra, src_dec)
        if separation <= radius_arcsec:
            candidates.append((separation, str(row['kelt_sourceid']).strip(), src_ra, src_dec, _to_float(row['kelt_mag'])))
    if not candidates:
        return []
    nearest = min(candidates)
    same_star = [c for c in candidates
                 if _angular_separation_arcsec(nearest[2], nearest[3], c[2], c[3]) <= KELT_SAME_STAR_ARCSEC]
    return sorted({c[1]: c for c in same_star}.values())


def _read_lightcurve(text):
    rows = []
    for line in text.splitlines():
        if not line.strip() or line[0] in '\\|':
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        try:
            rows.append((float(parts[0]), float(parts[1]), float(parts[2])))
        except ValueError:
            continue
    return rows


class KELTDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return KELT_DEFAULT_RADIUS_ARCSEC

    name = 'KELT'
    verbose_name = 'KELT (2005-2014)'
    # KELT DR1 is a fixed release.
    update_on_daily_refresh = False
    info_url = KELT_PAGE_URL
    acknowledgement = KELT_ACKNOWLEDGEMENT
    # One KELT measurement per field and time.
    upsert_identity_keys = ('filter', 'kelt_field')
    service_notes = (
        'Query KELT DR1 light curves (bright stars, 2005-2014) from the NASA Exoplanet Archive by '
        'coordinates. The nearest KELT source within 10 arcsec is used, with its light curves from '
        'overlapping fields; TFA-detrended by default (raw optional), east orientation only, errors '
        '<= 1 mag. Instrumental magnitudes are shifted to ~V per telescope (+-0.3 mag). The first '
        'query builds an 11 MB file index (~30 s). Times are BJD_TDB.'
    )

    @classmethod
    def get_form_class(cls):
        return KELTQueryForm

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
            'proc_type': parameters.get('proc_type') or 'tfa',
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or self.get_finding_chart_radius_arcsec()
        proc_type = 'raw' if query_parameters.get('proc_type') == 'raw' else 'tfa'

        sources, lightcurves = [], []
        if ra is not None and dec is not None and query_parameters.get('include_photometry', True):
            try:
                sources = _find_sources(ra, dec, radius_arcsec, proc_type)
                for separation, source_id, _ra, _dec, kelt_mag in sources:
                    url = _lightcurve_url(source_id, proc_type)
                    if not url:
                        logger.info('KELT: no %s light-curve file indexed for %s', proc_type, source_id)
                        continue
                    response = requests.get(url, timeout=DATA_SERVICE_HTTP_TIMEOUT)
                    response.raise_for_status()
                    lightcurves.append({
                        'source_id': source_id,
                        'separation_arcsec': separation,
                        'kelt_mag': kelt_mag,
                        'url': url,
                        'rows': _read_lightcurve(response.text),
                    })
                if not sources:
                    logger.debug('KELT returned no source for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('KELT query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                lightcurves = []

        self.query_results = {
            'lightcurves': lightcurves,
            'proc_type': proc_type,
            'source_location': lightcurves[0]['url'] if lightcurves else KELT_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        lightcurves = data.get('lightcurves') or []
        if data.get('ra') is None or data.get('dec') is None or not lightcurves:
            return []
        datums = self._build_photometry_datums(lightcurves, data['proc_type'])
        if not datums:
            return []
        return [{
            'name': lightcurves[0]['source_id'],
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

    def _build_photometry_datums(self, lightcurves, proc_type):
        output = []
        for lightcurve in lightcurves:
            field = lightcurve['source_id'].split('_')[1]
            offset = KELT_V_OFFSETS[field[0]]
            for bjd, magnitude, error in lightcurve['rows']:
                if not (math.isfinite(bjd) and math.isfinite(magnitude) and math.isfinite(error)):
                    continue
                if not (0 < error <= KELT_MAX_MAG_ERROR):
                    continue
                output.append({
                    'timestamp': Time(bjd - 2400000.5, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                    'value': {
                        'filter': KELT_FILTER,
                        'magnitude': round(magnitude - offset, 5),
                        'error': error,
                        'instrumental_magnitude': magnitude,
                        'zero_point_offset': offset,
                        'kelt_field': field,
                        'kelt_source_id': lightcurve['source_id'],
                        'proc_type': proc_type,
                        'time_scale': 'BJD_TDB',
                        'match_separation_arcsec': round(lightcurve['separation_arcsec'], 2),
                    },
                })
        return output
