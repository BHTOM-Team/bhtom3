"""unTimely (unWISE time-domain) W1/W2 light curves.

unTimely (Meisner, Caselden, Schlafly & Kiwy 2023, AJ 165, 36) extracts sources from
time-resolved unWISE coadds: one coadd per ~6-monthly WISE/NEOWISE sky pass, ~16 epochs
from 2010 to 2020, about 1.3 mag deeper than the NEOWISE single-exposure tables.

There is no query API. The catalogue is a set of gzipped FITS files, one per unWISE tile,
band and epoch, at NERSC (the same files the unTimely Catalog Explorer reads,
https://github.com/fkiwy/unTimely_Catalog_explorer). This service follows the explorer:
1. the catalogue index (~25 MB; tile centres, epochs, file names) is downloaded once and
   cached on disk in a reduced form (UNTIMELY_CACHE_DIR);
2. the tile containing the target farthest from its edges is chosen (2048-pixel box,
   2.75"/pixel, tangent projection about the tile centre);
3. that tile's W1 and W2 epoch files (~34 files, ~100 MB) are downloaded in parallel and
   only the nearest detection to the target in each file is kept.

Fluxes are Vega nanomaggies; magnitude = 22.5 - 2.5 log10(flux). Detections are kept when
they lie within the search radius, have flags_unwise == 0 (no bright-star artefacts) and
flux S/N >= UNTIMELY_MIN_SNR.
"""

import concurrent.futures
import gzip
import io
import logging
import math
import os
import tempfile
import threading
from datetime import timezone

import numpy as np
import requests
from astropy.io import fits
from astropy.time import Time
from django.conf import settings

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target

from custom_code.data_services.forms import UnTimelyQueryForm
from custom_code.data_services.service_utils import (
    resolve_query_coordinates,
    upsert_reduced_datums,
)

logger = logging.getLogger(__name__)

UNTIMELY_PAGE_URL = 'https://irsa.ipac.caltech.edu/data/WISE/unWISE/overview.html'
UNTIMELY_BASE_URL = 'https://portal.nersc.gov/project/cosmo/data/unwise/neo7/untimely-catalog/'
UNTIMELY_INDEX_FILE = 'untimely_index-neo7.fits'
UNTIMELY_PIXEL_SCALE_ARCSEC = 2.75
# Box used by the unTimely Catalog Explorer to decide whether a tile contains the target.
UNTIMELY_TILE_BOX_PIXELS = 2048
UNTIMELY_DEFAULT_RADIUS_ARCSEC = 3.0
UNTIMELY_MIN_SNR = 3.0
UNTIMELY_ZEROPOINT = 22.5
UNTIMELY_DOWNLOAD_WORKERS = 8
UNTIMELY_HTTP_TIMEOUT = (10, 300)
UNTIMELY_FILTERS = {1: 'unTimely(W1)', 2: 'unTimely(W2)'}

UNTIMELY_ACKNOWLEDGEMENT = (
    'This research uses the unWISE Time-Domain Catalog (unTimely; Meisner, Caselden, Schlafly '
    '& Kiwy 2023, AJ 165, 36; doi:10.26131/IRSA580), based on data from WISE and NEOWISE.'
)

_index_lock = threading.Lock()
_index_cache = {}


def _open_fits_bytes(content):
    """astropy only auto-decompresses gzip for file names, not in-memory buffers."""
    if content[:2] == b'\x1f\x8b':
        content = gzip.decompress(content)
    return fits.open(io.BytesIO(content))


def _cache_dir():
    path = getattr(settings, 'UNTIMELY_CACHE_DIR', None) or os.path.join(
        tempfile.gettempdir(), 'bhtom3-untimely-cache'
    )
    os.makedirs(path, exist_ok=True)
    return path


def _load_index():
    """Compact catalogue index: one row per tile (id, centre) plus, per file, its tile, band
    and epoch. File names are not stored: they are always
    '<tile[:3]>/<tile>/<tile>_w<band>_e<epoch:03d>.cat.fits.gz' (checked for all 616,806)."""
    with _index_lock:
        if _index_cache:
            return _index_cache
        reduced_path = os.path.join(_cache_dir(), 'untimely_index-neo7-v2.npz')
        if not os.path.exists(reduced_path):
            response = requests.get(UNTIMELY_BASE_URL + UNTIMELY_INDEX_FILE + '.gz', timeout=UNTIMELY_HTTP_TIMEOUT)
            response.raise_for_status()
            with _open_fits_bytes(response.content) as hdul:
                data = hdul[1].data
                coadd_ids = np.char.strip(np.asarray(data['COADD_ID']).astype('S8'))
                tiles, first_row, file_tile = np.unique(coadd_ids, return_index=True, return_inverse=True)
                np.savez_compressed(
                    reduced_path + '.tmp.npz',
                    tile_id=tiles,
                    tile_ra=np.asarray(data['RA'], dtype=float)[first_row],
                    tile_dec=np.asarray(data['DEC'], dtype=float)[first_row],
                    file_tile=file_tile.astype(np.int32),
                    file_band=np.asarray(data['BAND'], dtype=np.int8),
                    file_epoch=np.asarray(data['EPOCH'], dtype=np.int16),
                )
            os.replace(reduced_path + '.tmp.npz', reduced_path)
        with np.load(reduced_path) as loaded:
            _index_cache.update({key: loaded[key] for key in loaded.files})
        return _index_cache


def _tile_margin(tile_ra, tile_dec, ra, dec):
    """Product of distances (pixels) from the target to the nearest tile edges, or None if
    the target is outside the tile box (same projection as the unTimely Catalog Explorer)."""
    if abs(tile_dec - dec) > 1.0:
        return None
    ra_r, dec_r, ra0, dec0 = map(math.radians, (ra, dec, tile_ra, tile_dec))
    cos_c = math.sin(dec0) * math.sin(dec_r) + math.cos(dec0) * math.cos(dec_r) * math.cos(ra_r - ra0)
    if cos_c <= 0:
        return None
    scale = 3600.0 / UNTIMELY_PIXEL_SCALE_ARCSEC
    x = -math.degrees(math.cos(dec_r) * math.sin(ra_r - ra0) / cos_c) * scale
    y = math.degrees(
        (math.cos(dec0) * math.sin(dec_r) - math.sin(dec0) * math.cos(dec_r) * math.cos(ra_r - ra0)) / cos_c
    ) * scale
    half = UNTIMELY_TILE_BOX_PIXELS / 2.0
    x_margin, y_margin = half - abs(x), half - abs(y)
    if x_margin < 0 or y_margin < 0:
        return None
    return x_margin * y_margin


def _tile_files(ra, dec):
    """(coadd_id, [(band, epoch, url), ...]) for the tile best containing the target."""
    index = _load_index()
    best_tile, best_margin = None, None
    for tile in np.flatnonzero(np.abs(index['tile_dec'] - dec) <= 1.0):
        margin = _tile_margin(float(index['tile_ra'][tile]), float(index['tile_dec'][tile]), ra, dec)
        if margin is not None and (best_margin is None or margin > best_margin):
            best_tile, best_margin = int(tile), margin
    if best_tile is None:
        return None, []
    tile_id = index['tile_id'][best_tile].decode()
    files = []
    for i in np.flatnonzero(index['file_tile'] == best_tile):
        band, epoch = int(index['file_band'][i]), int(index['file_epoch'][i])
        files.append((band, epoch, f'{UNTIMELY_BASE_URL}{tile_id[:3]}/{tile_id}/{tile_id}_w{band}_e{epoch:03d}.cat.fits.gz'))
    return tile_id, files


def _angular_separation_arcsec(ra1, dec1, ra2, dec2):
    """Vectorised great-circle separation in arcsec."""
    ra1, dec1, ra2, dec2 = map(np.radians, (ra1, dec1, ra2, dec2))
    cos_sep = (np.sin(dec1) * np.sin(dec2) + np.cos(dec1) * np.cos(dec2) * np.cos(ra1 - ra2))
    return np.degrees(np.arccos(np.clip(cos_sep, -1.0, 1.0))) * 3600.0


def _nearest_detection(url, ra, dec, radius_arcsec, session):
    """Nearest detection to the target in one epoch file, as a dict, or None."""
    response = session.get(url, timeout=UNTIMELY_HTTP_TIMEOUT)
    response.raise_for_status()
    with _open_fits_bytes(response.content) as hdul:
        data = hdul[1].data
        # Cheap box prefilter before exact separations.
        box = radius_arcsec / 3600.0
        cos_dec = max(math.cos(math.radians(dec)), 1e-6)
        near = np.flatnonzero((np.abs(data['dec'] - dec) <= box) & (np.abs(data['ra'] - ra) * cos_dec <= box))
        if near.size == 0:
            return None
        separations = _angular_separation_arcsec(ra, dec, data['ra'][near], data['dec'][near])
        best = int(np.argmin(separations))
        if separations[best] > radius_arcsec:
            return None
        row = data[near[best]]
        return {
            'flux': float(row['flux']),
            'dflux': float(row['dflux']),
            'mjd': float(row['MJDMEAN']),
            'qf': float(row['qf']),
            'fracflux': float(row['fracflux']),
            'flags_unwise': int(row['flags_unwise']),
            'flags_info': int(row['flags_info']),
            'primary': int(row['primary']),
            'separation_arcsec': float(separations[best]),
        }


class UnTimelyDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return UNTIMELY_DEFAULT_RADIUS_ARCSEC

    name = 'unTimely'
    verbose_name = 'unTimely (unWISE time-domain W1/W2)'
    # The catalogue is a fixed release (2010-2020); nothing new appears between refreshes.
    update_on_daily_refresh = False
    info_url = UNTIMELY_PAGE_URL
    acknowledgement = UNTIMELY_ACKNOWLEDGEMENT
    upsert_identity_keys = ('filter', 'untimely_epoch')
    service_notes = (
        'Query the unWISE time-domain catalogue (unTimely, ~16 six-monthly W1/W2 epochs 2010-2020, '
        'deeper than NEOWISE single exposures) by coordinates. The target\'s unWISE tile is read '
        'from the public NERSC files (~100 MB, ~30 s); the nearest detection within 3 arcsec in each '
        'epoch with no unWISE bright-star flags and S/N >= 3 is imported as Vega magnitudes. '
        'Photometry only; no aliases are added.'
    )

    @classmethod
    def get_form_class(cls):
        return UnTimelyQueryForm

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
        ra = query_parameters.get('ra')
        dec = query_parameters.get('dec')
        radius_arcsec = float(query_parameters.get('radius_arcsec') or self.get_finding_chart_radius_arcsec())

        tile, detections = None, []
        if ra is not None and dec is not None and query_parameters.get('include_photometry', True):
            ra, dec = float(ra), float(dec)
            try:
                tile, files = _tile_files(ra, dec)
                session = requests.Session()
                adapter = requests.adapters.HTTPAdapter(pool_maxsize=UNTIMELY_DOWNLOAD_WORKERS)
                session.mount('https://', adapter)
                with concurrent.futures.ThreadPoolExecutor(UNTIMELY_DOWNLOAD_WORKERS) as pool:
                    futures = {
                        pool.submit(_nearest_detection, url, ra, dec, radius_arcsec, session): (band, epoch)
                        for band, epoch, url in files
                    }
                    for future in concurrent.futures.as_completed(futures):
                        band, epoch = futures[future]
                        detection = future.result()
                        if detection:
                            detections.append({**detection, 'band': band, 'epoch': epoch})
                if not files:
                    logger.debug('unTimely has no tile for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('unTimely query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                tile, detections = None, []

        self.query_results = {
            'tile': tile,
            'detections': detections,
            'source_location': UNTIMELY_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        if data.get('ra') is None or data.get('dec') is None or not data.get('detections'):
            return []

        datums = self._build_photometry_datums(data['detections'], data['tile'])
        if not datums:
            return []

        return [{
            'name': f"unTimely_J{data['ra']:.5f}{data['dec']:+.5f}",
            'ra': data['ra'],
            'dec': data['dec'],
            # Photometry only: unTimely detections have no catalogue-wide source name.
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

    def _build_photometry_datums(self, detections, tile):
        output = []
        for det in detections:
            flux, dflux, mjd = det['flux'], det['dflux'], det['mjd']
            if not (math.isfinite(flux) and math.isfinite(dflux) and math.isfinite(mjd)):
                continue
            if flux <= 0 or dflux <= 0 or flux / dflux < UNTIMELY_MIN_SNR or det['flags_unwise'] != 0:
                continue
            filter_name = UNTIMELY_FILTERS.get(det['band'])
            if filter_name is None:
                continue
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {
                    'filter': filter_name,
                    'magnitude': UNTIMELY_ZEROPOINT - 2.5 * math.log10(flux),
                    'error': 2.5 / math.log(10) * dflux / flux,
                    'mag_system': 'Vega',
                    'flux_nmgy': flux,
                    'flux_error_nmgy': dflux,
                    'mjd': mjd,
                    'untimely_epoch': det['epoch'],
                    'unwise_tile': tile,
                    'qf': det['qf'],
                    'fracflux': det['fracflux'],
                    'flags_info': det['flags_info'],
                    'primary': det['primary'],
                    'match_separation_arcsec': round(det['separation_arcsec'], 4),
                },
            })
        return output
