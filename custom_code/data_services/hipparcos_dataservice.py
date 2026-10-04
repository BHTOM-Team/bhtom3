"""Hipparcos/Tycho photometry from the ESA mission catalogues (VizieR I/239).

Scope
-----
For Hipparcos stars this service ingests the Hipparcos Epoch Photometry Annex: the individual
Hp transits (typically 60-150 per star, 1989-1993) as filter 'Hp'. CDS keeps the annex
as one gzip member per star in I/239/epophot/hep.gz, with byte offsets in hep.gz.idx; the index
(~2 MB) is downloaded once and cached (HIPPARCOS_CACHE_DIR) and each star is then one ~1 kB
HTTP range request. Transits flagged with bits 3, 4, 5, 7 or 8 of the quality flag (high
background, interfering object, FAST quality flag, Sun-pointing mode, FAST/NDAC discrepancy)
are dropped; this reproduces the annex's own count of photometrically accepted transits (Nh)
exactly for 144 of 150 randomly chosen stars. Times are barycentric JD (TT).

The Tycho mean BT and VT magnitudes (J1991.25) are kept as before; the catalogue's mean Hp is
not stored, since the transits carry it.

Band warning
------------
Hp is NOT V. It is a very broad unfiltered passband set by the S20 image
dissector response, lambda_eff ~ 520 nm with FWHM ~ 230 nm (Bessell 2000).
Converting Hp -> V needs a colour term, so the filters are stored under their
own names ('Hp', 'BT', 'VT') and must not be stacked with V-band data raw.
"""

import gzip
import logging
import os
import tempfile
import threading
import time
from datetime import timezone

import numpy as np
import pyvo
import requests
from astropy.time import Time
from django.conf import settings

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import HipparcosQueryForm


logger = logging.getLogger(__name__)

VIZIER_TAP_URL = 'https://tapvizier.cds.unistra.fr/TAPVizieR/tap'
HIPPARCOS_PAGE_URL = 'https://vizier.cds.unistra.fr/viz-bin/VizieR-3?-source=I/239/hip_main'

HIP_MAIN = 'I/239/hip_main'
TYC_MAIN = 'I/239/tyc_main'

# The catalogue epoch, J1991.25 = JD 2448349.0625 (TT). Every mean magnitude is
# a mission average over 1989-1993 conventionally attributed to this epoch.
J1991_25_MJD = 48348.5625

# CDS TAP is intermittently flaky under load. Two distinct transient failures show
# up, and both clear on a retry: an explicit "service too busy", and a bogus
# "unresolved identifiers" that is really VizieR's ADQL validator being unable to
# run ("Unable to check the ADQL query"). The latter reads like a query bug but is
# not one -- the identical query succeeds seconds later.
TAP_MAX_ATTEMPTS = 4
TAP_RETRY_SLEEP = 6.0
TAP_TRANSIENT_ERRORS = ('too busy', 'unable to check the adql query', 'no connection available')

HEP_URL = 'https://cdsarc.cds.unistra.fr/ftp/I/239/epophot/hep.gz'
HEP_INDEX_URL = HEP_URL + '.idx'
HEP_HTTP_TIMEOUT = (10, 120)
# Quality-flag bits that reject a transit: 3 very high background, 4 possible interfering object,
# 5 FAST quality flag, 7 Sun-pointing mode, 8 FAST/NDAC discrepancy. Bits 0/1 (one consortium
# only) and 6 are kept; this reproduces the annex's accepted-transit count (Nh).
HEP_REJECT_FLAGS = (1 << 3) | (1 << 4) | (1 << 5) | (1 << 7) | (1 << 8)
BJD2440000_TO_MJD = 39999.5  # (JD - 2440000) + 2440000 - 2400000.5

_hep_index_lock = threading.Lock()
_hep_index = {}


def _to_float(value):
    """Astropy tables hand back masked values for absent measurements."""
    try:
        if value is None or value is np.ma.masked or np.ma.is_masked(value):
            return None
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if np.isfinite(value) else None


def _to_text(value):
    try:
        if value is None or value is np.ma.masked or np.ma.is_masked(value):
            return ''
        if isinstance(value, bytes):
            value = value.decode('utf8', 'replace')
    except (TypeError, ValueError):
        return ''
    text = str(value).strip()
    return '' if text in ('--', 'nan', 'None') else text


def _hip_alias(hip):
    return f'HIP_{hip}'


def _tyc_alias(tyc):
    return f'TYC_{"-".join(str(tyc).split())}'


def _hip_source_location(hip):
    return f'https://vizier.cds.unistra.fr/viz-bin/VizieR-4?-source={HIP_MAIN}&HIP={hip}'


def _run_tap(tap, query, maxrec=10):
    """Run one ADQL query, retrying while CDS reports the service is busy."""
    last_error = None
    for attempt in range(TAP_MAX_ATTEMPTS):
        try:
            return tap.run_sync(query, maxrec=maxrec).to_table()
        except Exception as exc:
            last_error = exc
            message = str(exc).lower()
            transient = any(marker in message for marker in TAP_TRANSIENT_ERRORS)
            if transient and attempt < TAP_MAX_ATTEMPTS - 1:
                logger.debug('Hipparcos: transient VizieR TAP error, retrying: %s', str(exc)[:120])
                time.sleep(TAP_RETRY_SLEEP)
                continue
            raise
    raise last_error


def _cone_query(table, columns, ra, dec, radius_deg):
    """Cone search on the proper-motion-corrected J2000 positions.

    The catalogue's own RAICRS/DEICRS are at epoch J1991.25. Hipparcos stars are
    nearby and many have large proper motions, so matching a J2000 BHTOM target
    against J1991.25 positions would miss exactly the high-proper-motion stars.
    VizieR's computed "_RA.icrs"/"_DE.icrs" columns are J2000 with proper motion
    applied, which is what BHTOM target coordinates are.
    """
    selected = ', '.join(f'"{c}"' for c in columns)
    return (
        f'SELECT TOP 20 {selected} FROM "{table}" '
        f'WHERE 1=CONTAINS(POINT(\'ICRS\', "_RA.icrs", "_DE.icrs"), '
        f"CIRCLE('ICRS', {ra}, {dec}, {radius_deg}))"
    )


def _angular_separation_arcsec(ra1, dec1, ra2, dec2):
    ra1, dec1, ra2, dec2 = np.radians([ra1, dec1, ra2, dec2])
    sep = np.arccos(
        np.clip(
            np.sin(dec1) * np.sin(dec2) + np.cos(dec1) * np.cos(dec2) * np.cos(ra1 - ra2),
            -1.0,
            1.0,
        )
    )
    return float(np.degrees(sep) * 3600.0)


def _nearest_row(table, ra, dec):
    """VizieR TAP rejects an aliased ORDER BY, so the closest match is picked here."""
    best = None
    best_sep = None
    for row in table:
        row_ra = _to_float(row['_RA_icrs'])
        row_dec = _to_float(row['_DE_icrs'])
        if row_ra is None or row_dec is None:
            continue
        sep = _angular_separation_arcsec(ra, dec, row_ra, row_dec)
        if best_sep is None or sep < best_sep:
            best, best_sep = row, sep
    return best, best_sep


def _hep_cache_dir():
    path = getattr(settings, 'HIPPARCOS_CACHE_DIR', None) or os.path.join(tempfile.gettempdir(), 'bhtom3-hipparcos-cache')
    os.makedirs(path, exist_ok=True)
    return path


def _load_hep_index():
    """(sorted HIP numbers, 1-based byte offsets) of the epoch photometry file."""
    with _hep_index_lock:
        if _hep_index:
            return _hep_index['hip'], _hep_index['offset']
        path = os.path.join(_hep_cache_dir(), 'hep.gz.idx')
        if not os.path.exists(path):
            response = requests.get(HEP_INDEX_URL, timeout=HEP_HTTP_TIMEOUT)
            response.raise_for_status()
            with open(path + '.tmp', 'w') as handle:
                handle.write(response.text)
            os.replace(path + '.tmp', path)
        hips, offsets = [], []
        with open(path) as handle:
            for line in handle:
                hip, _, offset = line.strip().partition('=')
                if hip and offset:
                    hips.append(int(hip))
                    offsets.append(int(offset))
        _hep_index['hip'] = np.asarray(hips)
        _hep_index['offset'] = np.asarray(offsets)
        return _hep_index['hip'], _hep_index['offset']


def _fetch_epoch_photometry(hip):
    """Accepted Hp transits of one star as [(bjd_minus_2440000, hp, error, flag)], or []."""
    hips, offsets = _load_hep_index()
    i = int(np.searchsorted(hips, hip))
    if i >= len(hips) or hips[i] != hip:
        return []
    start = offsets[i] - 1
    headers = {'Range': f'bytes={start}-{offsets[i + 1] - 2}' if i + 1 < len(offsets) else f'bytes={start}-'}
    response = requests.get(HEP_URL, headers=headers, timeout=HEP_HTTP_TIMEOUT)
    response.raise_for_status()
    lines = gzip.decompress(response.content).decode('ascii', 'replace').splitlines()
    transits = []
    for line in lines[1:]:  # line 0 is the star's header record
        if not line.strip():
            break
        try:
            epoch, hp, error, flag = float(line[0:10]), float(line[11:18]), float(line[19:24]), int(line[25:28])
        except ValueError:
            continue
        if flag & HEP_REJECT_FLAGS or error <= 0:
            continue
        transits.append((epoch, hp, error, flag))
    return transits


HIP_COLUMNS = (
    'HIP', '_RA.icrs', '_DE.icrs', 'Hpmag', 'e_Hpmag',
    'BTmag', 'e_BTmag', 'VTmag', 'e_VTmag',
)
TYC_COLUMNS = (
    'TYC', 'HIP', '_RA.icrs', '_DE.icrs', 'BTmag', 'e_BTmag', 'VTmag', 'e_VTmag',
)


class HipparcosDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return 5.0

    name = 'Hipparcos'
    verbose_name = 'Hipparcos/Tycho'
    update_on_daily_refresh = False
    info_url = HIPPARCOS_PAGE_URL
    service_notes = (
        'Query Hipparcos/Tycho (VizieR I/239) by coordinates. Ingests the Hipparcos Epoch '
        'Photometry Annex (individual Hp transits, 1989-1993) and the Tycho mean BT/VT at J1991.25. '
        'Hp is a broad unfiltered band, not Johnson V, and needs a colour term to convert.'
    )

    @classmethod
    def get_form_class(cls):
        return HipparcosQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or 5.0,
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or 5.0

        if ra is None or dec is None:
            self.query_results = {'hip_row': None, 'tyc_row': None, 'source_location': None}
            return self.query_results

        radius_deg = radius_arcsec / 3600.0
        hip_row = tyc_row = None
        hip_sep = tyc_sep = None

        try:
            tap = pyvo.dal.TAPService(VIZIER_TAP_URL)

            try:
                hip_table = _run_tap(tap, _cone_query(HIP_MAIN, HIP_COLUMNS, ra, dec, radius_deg), maxrec=20)
                hip_row, hip_sep = _nearest_row(hip_table, ra, dec)
            except Exception as exc:
                logger.debug('Hipparcos hip_main query failed for RA=%s Dec=%s: %s', ra, dec, exc)

            try:
                tyc_table = _run_tap(tap, _cone_query(TYC_MAIN, TYC_COLUMNS, ra, dec, radius_deg), maxrec=20)
                tyc_row, tyc_sep = _nearest_row(tyc_table, ra, dec)
            except Exception as exc:
                logger.debug('Hipparcos tyc_main query failed for RA=%s Dec=%s: %s', ra, dec, exc)

            if hip_row is None and tyc_row is None:
                logger.debug('Hipparcos/Tycho returned no match for RA=%s Dec=%s', ra, dec)
        except Exception as exc:
            logger.debug('Hipparcos VizieR TAP error %s', exc)

        hip_id = int(_to_float(hip_row['HIP'])) if hip_row is not None else None
        epochs = []
        if hip_id:
            try:
                epochs = _fetch_epoch_photometry(hip_id)
            except Exception as exc:
                logger.warning('Hipparcos epoch photometry failed for HIP %s: %s', hip_id, exc)
        self.query_results = {
            'epochs': epochs,
            'hip_row': hip_row,
            'tyc_row': tyc_row,
            'hip_sep_arcsec': hip_sep,
            'tyc_sep_arcsec': tyc_sep,
            'source_location': _hip_source_location(hip_id) if hip_id else HIPPARCOS_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        ra = data.get('ra')
        dec = data.get('dec')
        hip_row = data.get('hip_row')
        tyc_row = data.get('tyc_row')
        if ra is None or dec is None or (hip_row is None and tyc_row is None):
            return []

        aliases = []
        name = None
        if hip_row is not None:
            hip_id = int(_to_float(hip_row['HIP']))
            name = _hip_alias(hip_id)
            aliases.append(name)
        if tyc_row is not None:
            tyc_text = _to_text(tyc_row['TYC'])
            if tyc_text:
                tyc_name = _tyc_alias(tyc_text)
                aliases.append(tyc_name)
                if name is None:
                    name = tyc_name

        datums = self._build_photometry_datums(hip_row, tyc_row, data.get('epochs') or [])
        if not datums:
            return []

        return [{
            'name': name,
            'ra': ra,
            'dec': dec,
            'aliases': aliases,
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
        return [TargetName(name=alias) for alias in alias_results]

    def create_reduced_datums_from_query(self, target, data=None, data_type=None, **kwargs):
        if data_type != 'photometry' or not data:
            return
        source_location = kwargs.get('source_location') or self.info_url
        for datum in data:
            ReducedDatum.objects.get_or_create(
                target=target,
                data_type='photometry',
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
                source_location=self.query_results.get('source_location') or self.info_url,
            )

    def _build_photometry_datums(self, hip_row, tyc_row, epochs=()):
        """Hp epoch transits plus the Tycho mean BT/VT at J1991.25.

        hip_main repeats the Tycho BT/VT for stars that have both, so it is preferred
        and tyc_main only fills in for Tycho-only stars.
        """
        timestamp = Time(J1991_25_MJD, format='mjd', scale='utc').to_datetime(timezone=timezone.utc)
        output = []

        def add(filter_name, magnitude, error):
            if magnitude is None:
                return
            output.append({
                'timestamp': timestamp,
                'value': {'filter': filter_name, 'magnitude': magnitude, 'error': error},
            })

        for epoch, hp, error, flag in epochs:
            output.append({
                'timestamp': Time(epoch + BJD2440000_TO_MJD, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {
                    'filter': 'Hp',
                    'magnitude': hp,
                    'error': error,
                    'transit_flag': flag,
                    'time_scale': 'BJD_TT',
                },
            })

        if hip_row is not None:
            # Bright stars (Vega, say) can have BT/VT masked in hip_main while their
            # own Tycho entry carries them. Only fall back when tyc_main names the
            # same HIP, so a close neighbour can never be blended in.
            tyc_fallback = tyc_row if self._same_star(hip_row, tyc_row) else None
            for band in ('BT', 'VT'):
                magnitude = _to_float(hip_row[f'{band}mag'])
                error = _to_float(hip_row[f'e_{band}mag'])
                if magnitude is None and tyc_fallback is not None:
                    magnitude = _to_float(tyc_fallback[f'{band}mag'])
                    error = _to_float(tyc_fallback[f'e_{band}mag'])
                add(band, magnitude, error)
        elif tyc_row is not None:
            add('BT', _to_float(tyc_row['BTmag']), _to_float(tyc_row['e_BTmag']))
            add('VT', _to_float(tyc_row['VTmag']), _to_float(tyc_row['e_VTmag']))

        return output

    @staticmethod
    def _same_star(hip_row, tyc_row):
        if hip_row is None or tyc_row is None:
            return False
        hip_id = _to_float(hip_row['HIP'])
        tyc_hip_id = _to_float(tyc_row['HIP'])
        return hip_id is not None and tyc_hip_id is not None and int(hip_id) == int(tyc_hip_id)
