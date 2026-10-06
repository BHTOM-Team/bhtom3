"""HETDEX Public Source Catalog 2 (HPSC2) 1D spectra from the HETDEX Public Data Release 1.

HETDEX (Mentuch Cooper et al. 2026, ApJS 284, 67) is a blind IFU survey with VIRUS on the
Hobby-Eberly Telescope (2017-2024) covering 87 deg^2 in dex-spring (~13h +51), dex-fall
(~1.5h 0), COSMOS, NEP, GOODS-N and SSA22, at 3470-5540 A (air), R ~ 800, 2 A per pixel. HPSC2
has ~1.1 million sources (Lya emitters, [O II] emitters, stars, AGN, low-z galaxies); a source
observed in several shots has one row, and one PSF-extracted, dust-corrected spectrum, per shot.

All spectra are in one 8.7 GB FITS file (hetdex_sc2_spec_v1.5.fits) whose SPEC and SPEC_ERR
images are stored wavelength-major: one spectrum is 1036 scattered 4-byte values. They are read
with multi-range HTTP requests, so only ~8 kB is transferred per spectrum. Finding a source's row
needs the file's INFO table (225 MB); its positions, source ids and shot ids are downloaded once
and cached in HETDEX_CACHE_DIR together with the observation MJD of every shot (from the PDR1
ifu-index.fits, 80 MB). The first query therefore takes a few minutes; later ones a few seconds.
"""

import io
import logging
import math
import os
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import astropy.units as u
import numpy as np
import requests
from astropy.io import fits
from astropy.time import Time
from django.conf import settings
from specutils import Spectrum1D

from tom_dataproducts.models import ReducedDatum
from tom_dataproducts.processors.data_serializers import SpectrumSerializer
from tom_dataservices.dataservices import DataService
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import HETDEXQueryForm
from custom_code.data_services.service_utils import resolve_query_coordinates

logger = logging.getLogger(__name__)

HETDEX_PAGE_URL = 'https://hetdex.org/data-results/'
HETDEX_PDR1_URL = 'https://web.corral.tacc.utexas.edu/hetdex/HETDEX/pdr/pdr1/'
HETDEX_SPEC_URL = HETDEX_PDR1_URL + 'hetdex_source_catalog_2/hetdex_sc2_spec_v1.5.fits'
HETDEX_IFU_INDEX_URL = HETDEX_PDR1_URL + 'ifu-index.fits'
HETDEX_CATALOG = 'HPSC2 v1.5'
HETDEX_INDEX_FILE = 'hetdex_hpsc2_v1.5_index-v1.npz'
HETDEX_DEFAULT_RADIUS_ARCSEC = 3.0
# Rows from different shots this close to the matched row are the same source (positions ~0.5").
HETDEX_SAME_SOURCE_ARCSEC = 1.0
HETDEX_HTTP_TIMEOUT = 120
HETDEX_FLUX_SCALE = 1e-17  # SPEC/SPEC_ERR are in 1e-17 erg/s/cm2/A
# Byte ranges per request (the Range header stays under ~6 kB) and requests in flight.
HETDEX_RANGES_PER_REQUEST = 250
HETDEX_PARALLEL_REQUESTS = 4
HETDEX_DOWNLOAD_CHUNK = 8 * 1024 * 1024
HETDEX_INFO_COLUMNS = ('RA', 'DEC', 'source_id', 'shotid')

HETDEX_ACKNOWLEDGEMENT = (
    'This research uses data from the Hobby-Eberly Telescope Dark Energy Experiment (HETDEX) Public '
    'Data Release 1 and the HETDEX Public Source Catalog 2 (Mentuch Cooper et al. 2026, ApJS 284, '
    '67). HETDEX is led by the University of Texas at Austin McDonald Observatory and Department of '
    'Astronomy with participation from the Ludwig-Maximilians-Universitat Munchen, '
    'Max-Planck-Institut fur Extraterrestrische Physik (MPE), Leibniz-Institut fur Astrophysik '
    'Potsdam (AIP), Texas A&M University, The Pennsylvania State University, Institut fur '
    'Astrophysik Gottingen, The University of Oxford, Max-Planck-Institut fur Astrophysik (MPA), '
    'The University of Tokyo, and Missouri University of Science and Technology. Observations for '
    'HETDEX were obtained with the Hobby-Eberly Telescope (HET), a joint project of the University '
    'of Texas at Austin, the Pennsylvania State University, Ludwig-Maximilians-Universitat Munchen, '
    'and Georg-August-Universitat Gottingen. The HET is named in honor of its principal '
    'benefactors, William P. Hobby and Robert E. Eberly. The authors acknowledge the Texas Advanced '
    'Computing Center (TACC) at The University of Texas at Austin for providing high performance '
    'computing, visualization, and storage resources.'
)

_index_lock = threading.Lock()
_index_cache = {}

FLAMBDA = u.erg / (u.s * u.cm ** 2 * u.AA)
_FITS_DTYPES = {'K': '>i8', 'J': '>i4', 'I': '>i2', 'E': '>f4', 'D': '>f8', 'L': 'S1', 'B': 'u1'}


def _to_float(value):
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) and converted != -999.0 else None


def _text(value):
    if isinstance(value, bytes):
        value = value.decode('ascii', 'replace')
    value = str(value).strip()
    return None if value in ('', 'n/a') else value


def _cache_dir():
    path = getattr(settings, 'HETDEX_CACHE_DIR', None) or os.path.join(tempfile.gettempdir(), 'bhtom3-hetdex-cache')
    os.makedirs(path, exist_ok=True)
    return path


def _get_range(url, start, stop):
    response = requests.get(url, headers={'Range': f'bytes={start}-{stop - 1}'}, timeout=HETDEX_HTTP_TIMEOUT)
    response.raise_for_status()
    if response.status_code != 206:
        raise ValueError(f'{url} ignored the byte range')
    return response.content


def _read_hdu_layout(url, count):
    """(header, data offset, padded data size) of the first `count` HDUs of a remote FITS file."""
    layout, offset = [], 0
    for _ in range(count):
        raw = b''
        while True:
            raw += _get_range(url, offset + len(raw), offset + len(raw) + 2880 * 4)
            cards = [raw[i:i + 80] for i in range(0, len(raw) - len(raw) % 80, 80)]
            ends = [i for i, card in enumerate(cards) if card.rstrip() == b'END']
            if ends:
                break
        header_size = int(math.ceil((ends[0] + 1) * 80 / 2880.0)) * 2880
        header = fits.Header.fromstring(raw[:header_size].decode('ascii'))
        naxis = header.get('NAXIS', 0)
        size = abs(header.get('BITPIX', 8)) // 8 * math.prod(header[f'NAXIS{k}'] for k in range(1, naxis + 1)) if naxis else 0
        size += header.get('PCOUNT', 0)
        padded = int(math.ceil(size / 2880.0)) * 2880
        layout.append((header, offset + header_size, padded))
        offset += header_size + padded
    return layout


def _table_dtype(header):
    """numpy dtype of one row of a FITS binary table (scalar and character columns only)."""
    fields = []
    for k in range(1, header['TFIELDS'] + 1):
        form = header[f'TFORM{k}'].strip()
        repeat, code = int(form[:-1] or 1), form[-1]
        fields.append((header[f'TTYPE{k}'], f'S{repeat}' if code == 'A' else _FITS_DTYPES[code]))
    dtype = np.dtype(fields)
    if dtype.itemsize != header['NAXIS1']:
        raise ValueError(f'unexpected INFO row size {dtype.itemsize} != {header["NAXIS1"]}')
    return dtype


def _build_index(path):
    """Cache the INFO table's positions and ids, the file layout and the MJD of every shot."""
    layout = _read_hdu_layout(HETDEX_SPEC_URL, 5)
    hdus = {header.get('EXTNAME'): (header, offset) for header, offset, _ in layout}
    info_header, info_offset = hdus['INFO']
    dtype = _table_dtype(info_header)
    nrows = info_header['NAXIS2']

    columns = {name: [] for name in HETDEX_INFO_COLUMNS}
    rows_per_chunk = HETDEX_DOWNLOAD_CHUNK // dtype.itemsize
    with requests.get(
        HETDEX_SPEC_URL, stream=True, timeout=HETDEX_HTTP_TIMEOUT,
        headers={'Range': f'bytes={info_offset}-{info_offset + nrows * dtype.itemsize - 1}'},
    ) as response:
        response.raise_for_status()
        pending = b''
        for block in response.iter_content(chunk_size=rows_per_chunk * dtype.itemsize):
            pending += block
            usable = len(pending) - len(pending) % dtype.itemsize
            rows = np.frombuffer(pending[:usable], dtype=dtype)
            for name in HETDEX_INFO_COLUMNS:
                columns[name].append(rows[name].copy())
            pending = pending[usable:]
    columns = {name: np.concatenate(values) for name, values in columns.items()}
    if len(columns['RA']) != nrows:
        raise ValueError(f'HETDEX INFO download incomplete: {len(columns["RA"])} of {nrows} rows')

    response = requests.get(HETDEX_IFU_INDEX_URL, timeout=HETDEX_HTTP_TIMEOUT * 5)
    response.raise_for_status()
    with fits.open(io.BytesIO(response.content), memmap=False) as hdul:
        shots = np.asarray(hdul[1].data['shotid'], dtype=np.int64)
        mjds = np.asarray(hdul[1].data['mjd'], dtype=np.float64)
    shot_ids, first = np.unique(shots, return_index=True)

    wave_header, wave_offset = hdus['WAVELENGTH']
    wavelength = np.frombuffer(_get_range(HETDEX_SPEC_URL, wave_offset, wave_offset + 4 * wave_header['NAXIS1']), '>f4')

    tmp_path = path + f'.tmp{os.getpid()}.npz'
    np.savez_compressed(
        tmp_path,
        ra=columns['RA'].astype(np.float64),
        dec=columns['DEC'].astype(np.float64),
        source_id=columns['source_id'].astype(np.int64),
        shotid=columns['shotid'].astype(np.int64),
        shot_ids=shot_ids,
        shot_mjd=mjds[first],
        wavelength=wavelength.astype(np.float64),
        info_offset=info_offset,
        spec_offset=hdus['SPEC'][1],
        err_offset=hdus['SPEC_ERR'][1],
        nrows=nrows,
        info_ttype=np.array([dtype.names[k] for k in range(len(dtype.names))]),
        info_tform=np.array([info_header[f'TFORM{k}'].strip() for k in range(1, info_header['TFIELDS'] + 1)]),
    )
    os.replace(tmp_path, path)


def _load_index():
    with _index_lock:
        if not _index_cache:
            path = os.path.join(_cache_dir(), HETDEX_INDEX_FILE)
            if not os.path.exists(path):
                _build_index(path)
            with np.load(path) as loaded:
                _index_cache.update({key: loaded[key] for key in loaded.files})
            fields = [
                (name, f'S{form[:-1] or 1}' if form[-1] == 'A' else _FITS_DTYPES[form[-1]])
                for name, form in zip(_index_cache['info_ttype'], _index_cache['info_tform'])
            ]
            _index_cache['info_dtype'] = np.dtype(fields)
        return _index_cache


def _multirange(url, ranges):
    """Bytes of each (start, stop) range, keyed by start, using multipart range requests."""
    def fetch(batch):
        header = 'bytes=' + ','.join(f'{start}-{stop - 1}' for start, stop in batch)
        response = requests.get(url, headers={'Range': header}, timeout=HETDEX_HTTP_TIMEOUT)
        response.raise_for_status()
        content_type = response.headers.get('Content-Type', '')
        if len(batch) == 1 and response.status_code == 206 and 'multipart' not in content_type:
            return {batch[0][0]: response.content}
        if 'boundary=' not in content_type:
            raise ValueError(f'{url} did not return a multipart range response')
        boundary = b'--' + content_type.split('boundary=')[1].strip().strip('"').encode()
        parts = {}
        for part in response.content.split(boundary):
            head, sep, body = part.partition(b'\r\n\r\n')
            if not sep:
                continue
            for line in head.split(b'\r\n'):
                if line.lower().startswith(b'content-range:'):
                    start, stop = line.split(b' ')[-1].split(b'/')[0].split(b'-')
                    parts[int(start)] = body[:int(stop) - int(start) + 1]
        return parts

    batches = [ranges[i:i + HETDEX_RANGES_PER_REQUEST] for i in range(0, len(ranges), HETDEX_RANGES_PER_REQUEST)]
    result = {}
    with ThreadPoolExecutor(max_workers=HETDEX_PARALLEL_REQUESTS) as pool:
        for parts in pool.map(fetch, batches):
            result.update(parts)
    missing = [start for start, _ in ranges if start not in result]
    if missing:
        raise ValueError(f'HETDEX range response is missing {len(missing)} of {len(ranges)} ranges')
    return result


def _fetch_observations(index, rows):
    """INFO record, flux and error arrays (in 1e-17 erg/s/cm2/A) of the given INFO rows."""
    dtype, nrows = index['info_dtype'], int(index['nrows'])
    nwave = len(index['wavelength'])
    info_offset, spec_offset, err_offset = (int(index[key]) for key in ('info_offset', 'spec_offset', 'err_offset'))
    ranges = []
    for row in rows:
        start = info_offset + int(row) * dtype.itemsize
        ranges.append((start, start + dtype.itemsize))
        for base in (spec_offset, err_offset):
            ranges.extend((base + (w * nrows + int(row)) * 4, base + (w * nrows + int(row)) * 4 + 4) for w in range(nwave))
    parts = _multirange(HETDEX_SPEC_URL, ranges)

    observations = []
    for row in rows:
        start = info_offset + int(row) * dtype.itemsize
        record = np.frombuffer(parts[start], dtype=dtype)[0]
        arrays = []
        for base in (spec_offset, err_offset):
            arrays.append(np.frombuffer(b''.join(
                parts[base + (w * nrows + int(row)) * 4] for w in range(nwave)
            ), '>f4').astype(np.float64))
        observations.append((record, arrays[0], arrays[1]))
    return observations


def _rows_within(index, ra, dec, radius_arcsec):
    """INFO rows within the radius of the position, and their separations in arcsec."""
    radius = radius_arcsec / 3600.0
    near = np.flatnonzero(np.abs(index['dec'] - dec) <= radius)
    if near.size == 0:
        return near, np.empty(0)
    dra = (index['ra'][near] - ra + 180.0) % 360.0 - 180.0
    near = near[np.abs(dra) * math.cos(math.radians(dec)) <= radius]
    d1, d2, a1, a2 = map(np.radians, (dec, index['dec'][near], ra, index['ra'][near]))
    separation = np.degrees(2 * np.arcsin(np.sqrt(
        np.sin((d2 - d1) / 2) ** 2 + np.cos(d1) * np.cos(d2) * np.sin((a2 - a1) / 2) ** 2
    ))) * 3600.0
    inside = separation <= radius_arcsec
    return near[inside], separation[inside]


def _nearest_source_rows(index, ra, dec, radius_arcsec):
    """INFO rows (one per shot) of the HPSC2 source nearest the position, and its separation.

    HPSC2 gives each observation of a source its own source_id, so the observations from other
    shots are the rows within HETDEX_SAME_SOURCE_ARCSEC of the nearest row, nearest one per shot.
    """
    rows, separation = _rows_within(index, ra, dec, radius_arcsec)
    if rows.size == 0:
        return [], None
    best = rows[np.argmin(separation)]
    group, group_separation = _rows_within(index, index['ra'][best], index['dec'][best], HETDEX_SAME_SOURCE_ARCSEC)
    per_shot = {}
    for row, sep in sorted(zip(group, group_separation), key=lambda item: item[1]):
        per_shot.setdefault(int(index['shotid'][row]), row)
    return [per_shot[shot] for shot in sorted(per_shot)], float(separation.min())


def _shot_mjd(index, shotid):
    """MJD of the observation from the IFU index, or 0h UT of the shot's date (YYYYMMDDNNN)."""
    i = int(np.searchsorted(index['shot_ids'], shotid))
    if i < len(index['shot_ids']) and index['shot_ids'][i] == shotid and np.isfinite(index['shot_mjd'][i]):
        return float(index['shot_mjd'][i])
    date = datetime.strptime(str(shotid)[:8], '%Y%m%d').replace(tzinfo=timezone.utc)
    return float(Time(date).mjd)


class HETDEXDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return HETDEX_DEFAULT_RADIUS_ARCSEC

    name = 'HETDEX'
    verbose_name = 'HETDEX (HPSC2 spectra)'
    update_on_daily_refresh = False
    info_url = HETDEX_PAGE_URL
    acknowledgement = HETDEX_ACKNOWLEDGEMENT
    service_notes = (
        'Query HETDEX Public Source Catalog 2 spectra (3470-5540 A, R~800, 2017-2024; 87 deg^2 in '
        'dex-spring, dex-fall, COSMOS, NEP, GOODS-N and SSA22) by coordinates. The nearest HETDEX '
        'source within 3 arcsec is used and its spectrum from every shot is imported, with the '
        'HETDEX redshift and source type; the HETDEX name is added as an alias. The first query '
        'builds a local index (~5 min); later queries take a few seconds.'
    )

    @classmethod
    def get_form_class(cls):
        return HETDEXQueryForm

    @classmethod
    def get_acknowledgement(cls):
        return cls.acknowledgement

    def build_query_parameters(self, parameters, **kwargs):
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or HETDEX_DEFAULT_RADIUS_ARCSEC,
            'include_spectroscopy': bool(parameters.get('include_spectroscopy', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or HETDEX_DEFAULT_RADIUS_ARCSEC

        observations, separation, wavelength, mjds = [], None, None, []
        if ra is not None and dec is not None and query_parameters.get('include_spectroscopy', True):
            try:
                index = _load_index()
                rows, separation = _nearest_source_rows(index, ra, dec, radius_arcsec)
                if rows:
                    observations = _fetch_observations(index, rows)
                    wavelength = index['wavelength']
                    mjds = [_shot_mjd(index, int(index['shotid'][row])) for row in rows]
                else:
                    logger.debug('HETDEX has no source near RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('HETDEX query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                observations, separation = [], None

        self.query_results = {
            'observations': observations,
            'mjds': mjds,
            'wavelength': wavelength,
            'separation_arcsec': separation,
            'source_location': HETDEX_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        observations = data.get('observations') or []
        if data.get('ra') is None or data.get('dec') is None or not observations:
            return []
        datums = self._build_spectroscopy_datums(
            observations, data['mjds'], data['wavelength'], data.get('separation_arcsec'),
        )
        if not datums:
            return []
        name = _text(observations[0][0]['source_name'])
        aliases = [name] if name else []
        return [{
            'name': name or f"HETDEX_{int(observations[0][0]['source_id'])}",
            'ra': data['ra'],
            'dec': data['dec'],
            'aliases': aliases,
            'reduced_datums': {'spectroscopy': datums},
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
        return [TargetName(name=alias) for alias in alias_results]

    def create_reduced_datums_from_query(self, target, data=None, data_type=None, **kwargs):
        if data_type != 'spectroscopy' or not data:
            return
        source_location = kwargs.get('source_location') or self.info_url
        for datum in data:
            ReducedDatum.objects.get_or_create(
                target=target,
                data_type='spectroscopy',
                timestamp=datum['timestamp'],
                value=datum['value'],
                defaults={
                    'source_name': self.name,
                    'source_location': source_location,
                },
            )

    def to_reduced_datums(self, target, data_results=None, **kwargs):
        if not data_results:
            return
        for data_type, data in data_results.items():
            self.create_reduced_datums_from_query(
                target,
                data=data,
                data_type=data_type,
                source_location=(getattr(self, 'query_results', {}) or {}).get('source_location') or self.info_url,
            )

    def _build_spectroscopy_datums(self, observations, mjds, wavelength, separation):
        output = []
        serializer = SpectrumSerializer()
        for (record, flux, error), mjd in zip(observations, mjds):
            keep = np.isfinite(flux) & np.isfinite(error) & (error > 0)
            if keep.sum() < 10:
                continue
            serialized = serializer.serialize(Spectrum1D(
                flux=flux[keep] * HETDEX_FLUX_SCALE * FLAMBDA,
                spectral_axis=wavelength[keep] * u.AA,
            ))
            serialized.update({
                'flux_error': [float(value) for value in error[keep] * HETDEX_FLUX_SCALE],
                'filter': 'HETDEX',
                'source_id': _text(record['source_name']),
                'hetdex_source_id': int(record['source_id']),
                'detectid': int(record['detectid']),
                'shotid': int(record['shotid']),
                'ifuslot': _text(record['ifuslot']),
                'field': _text(record['field']),
                'spectrum_type': 'HETDEX_spectrum',
                'source_type': _text(record['source_type']),
                'redshift': _to_float(record['z_hetdex']),
                'redshift_source': _text(record['z_hetdex_src']),
                'redshift_confidence': _to_float(record['z_hetdex_conf']),
                'gmag': _to_float(record['gmag']),
                'av_corrected': _to_float(record['Av']),
                'wavelength_frame': 'air',
                'catalog': HETDEX_CATALOG,
                'match_separation_arcsec': round(separation, 3) if separation is not None else None,
            })
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': serialized,
            })
        return output
