"""AAVSO photometry DataService.

Ingests time-series photometry from the AAVSO International Database (AID) via the public
delimited API (https://vsx.aavso.org/index.php?view=api.delim). No credentials required.

The API is keyed by a star identifier (``ident``) and has no coordinate query, so we resolve
a target's RA/Dec to the nearest VSX star name via the VizieR B/vsx mirror (whose ``Name``
column is exactly what ``ident`` accepts), then fall back to the target's own name/aliases.
We try each candidate identifier until one returns observations, then ingest them.

Notes on the AID data:
* Time is Julian Date (``JD``); we convert to a UTC timestamp (JD = MJD + 2400000.5).
* ``uncert`` is blank for visual observations -- those are stored without an error (rather
  than error 0, which would be mistaken for an upper limit).
* ``fainterThan == 1`` marks a non-detection -- stored as an UPPER LIMIT with ``error = -1``
  (bhtom3 plots error <= 0 as a limit), reusing the same convention as the ATLAS service.
* Bands (V, B, I, R, CV, Vis., ...) become ``AAVSO(<band>)`` filters.

Loading: the full history is pulled by default, in bounded JD chunks. When a target exists
each chunk is inserted immediately, so a well-observed star (tens of thousands of points) is
loaded durably -- a timeout mid-load leaves earlier chunks persisted and the next run's
incremental ``fromjd`` (latest stored point) resumes where it stopped. Dedup is on
``(timestamp, value)`` within each chunk's time span.
"""

import json
import logging
from collections import Counter
from datetime import timezone as dt_timezone

import requests
from astropy.time import Time

from django.conf import settings

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import AAVSOQueryForm
from custom_code.data_services.service_utils import (
    DATA_SERVICE_HTTP_TIMEOUT,
    DATA_SERVICE_READ_TIMEOUT,
    resolve_query_coordinates,
)


logger = logging.getLogger(__name__)

AAVSO_API_URL = 'https://vsx.aavso.org/index.php'
AAVSO_INFO_URL = 'https://www.aavso.org/'
AAVSO_DELIMITER = '@@@'
# VizieR mirror of the AAVSO Variable Star Index, used to resolve coordinates -> VSX star name.
_VSX_VIZIER_CATALOG = 'B/vsx/vsx'
_DEFAULT_MATCH_RADIUS = 5.0  # arcsec
# JD floor for a full-history first fetch (JD 2415020 ~= 1900-01-01, covering essentially all
# AAVSO observations); incremental fetches use the latest stored point instead.
_DEFAULT_FROM_JD = 2415020.0
# JD = MJD + this constant.
_MJD_TO_JD = 2400000.5
_REQUEST_HEADERS = {'User-Agent': 'bhtom3 AAVSO dataservice', 'Accept': 'text/plain'}


def _chunk_days():
    """Width (in JD days) of each fetch window. Well-observed stars return ~100k points/year
    (e.g. SS Cyg), so the full history is pulled in bounded chunks rather than one huge request.
    One year keeps each request/insert manageable; incremental runs after the first are tiny."""
    return float(getattr(settings, 'AAVSO_CHUNK_DAYS', 365.0))


def _now_jd():
    return float(Time.now().jd)


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _reduced_datum_identity(timestamp, value):
    return (timestamp, json.dumps(value, sort_keys=True, separators=(',', ':'), default=str))


def _ingest_photometry(target, rows):
    """Insert one chunk's photometry, deduping against existing points in the chunk's time span.

    Dedup uses a timestamp *range* query (not a huge ``timestamp IN (...)`` list), so it stays
    cheap even for chunks with tens of thousands of points. Returns the number of new rows.
    """
    if not rows:
        return 0
    times = [r['timestamp'] for r in rows]
    lo, hi = min(times), max(times)
    existing = {
        _reduced_datum_identity(ts, val)
        for ts, val in ReducedDatum.objects.filter(
            target=target,
            source_name=AAVSODataService.name,
            data_type='photometry',
            timestamp__gte=lo,
            timestamp__lte=hi,
        ).values_list('timestamp', 'value')
    }
    seen = set()
    new_rows = []
    for r in rows:
        key = _reduced_datum_identity(r['timestamp'], r['value'])
        if key in existing or key in seen:
            continue
        seen.add(key)
        new_rows.append(ReducedDatum(
            target=target,
            data_type='photometry',
            source_name=AAVSODataService.name,
            source_location=AAVSO_INFO_URL,
            timestamp=r['timestamp'],
            value=r['value'],
        ))
    if new_rows:
        ReducedDatum.objects.bulk_create(new_rows, batch_size=500)
    return len(new_rows)


def _band_label(band):
    band = str(band or '').strip().rstrip('.')  # 'Vis.' -> 'Vis'
    return f'AAVSO({band})' if band else 'AAVSO(Vis)'


def _resolve_vsx_names(ra, dec, radius_arcsec):
    """Resolve coordinates to nearby VSX star name(s) via the VizieR B/vsx mirror, nearest first.

    api.delim is keyed by star name, and the VizieR B/vsx 'Name' column is that name. Returns an
    empty list on any failure so the caller can fall back to name-based lookup.
    """
    try:
        from astroquery.vizier import Vizier
        import astropy.units as u
        from astropy.coordinates import SkyCoord
    except Exception:
        return []
    try:
        vizier = Vizier(catalog=_VSX_VIZIER_CATALOG, columns=['Name', 'RAJ2000', 'DEJ2000'])
        vizier.ROW_LIMIT = 50
        vizier.TIMEOUT = int(DATA_SERVICE_READ_TIMEOUT)
        center = SkyCoord(ra, dec, unit='deg')
        result = vizier.query_region(center, radius=radius_arcsec * u.arcsec)
    except Exception as exc:
        logger.info('AAVSO: VizieR B/vsx resolution failed for %.5f,%.5f: %s', ra, dec, exc)
        return []
    if not result or 'Name' not in result[0].colnames:
        return []
    table = result[0]
    try:
        coords = SkyCoord(table['RAJ2000'], table['DEJ2000'], unit='deg')
        seps = center.separation(coords).arcsec
        pairs = sorted((float(s), str(n).strip()) for s, n in zip(seps, table['Name']))
    except Exception:
        pairs = [(0.0, str(n).strip()) for n in table['Name']]
    return [name for _, name in pairs if name]


class AAVSODataService(DataService):
    name = 'AAVSO'
    verbose_name = 'AAVSO Photometry'
    update_on_daily_refresh = True
    info_url = AAVSO_INFO_URL
    base_url = AAVSO_API_URL
    service_notes = (
        'Query the AAVSO International Database by star name for time-series photometry. '
        'Resolves the target by its name and aliases (no VSX lookup).'
    )

    @classmethod
    def get_form_class(cls):
        return AAVSOQueryForm

    # -------------------------------------------------------- query params
    def _target_names(self, target_name, target_id):
        """The target's own name and aliases, as fallback AAVSO identifiers."""
        names = []
        explicit = str(target_name or '').strip()
        if explicit:
            names.append(explicit)
        if target_id:
            try:
                target = Target.objects.get(id=target_id)
                names.append(target.name)
                names.extend(TargetName.objects.filter(target_id=target_id).values_list('name', flat=True))
            except Target.DoesNotExist:
                logger.info('AAVSO: target id=%s not found while building idents.', target_id)
        return [n for n in names if n]

    def _incremental_from_jd(self, target_id):
        """Only fetch observations newer than the latest AAVSO point already stored."""
        if not target_id:
            return _DEFAULT_FROM_JD
        latest = (
            ReducedDatum.objects
            .filter(target_id=target_id, source_name=self.name, data_type='photometry')
            .order_by('-timestamp')
            .values_list('timestamp', flat=True)
            .first()
        )
        if not latest:
            return _DEFAULT_FROM_JD
        try:
            return float(Time(latest, scale='utc').mjd) + _MJD_TO_JD
        except Exception:
            return _DEFAULT_FROM_JD

    def build_query_parameters(self, parameters, **kwargs):
        target_name, ra, dec = resolve_query_coordinates(parameters)
        target_id = parameters.get('target_id')
        radius = _to_float(parameters.get('radius_arcsec')) or _DEFAULT_MATCH_RADIUS

        # Resolve coordinates -> nearest VSX star name(s) via the VizieR B/vsx mirror.
        vsx_names = []
        ra_f, dec_f = _to_float(ra), _to_float(dec)
        if ra_f is not None and dec_f is not None:
            vsx_names = _resolve_vsx_names(ra_f, dec_f, radius)

        # Candidate identifiers: VSX-resolved names first (coordinate match), then the
        # target's own name/aliases as a fallback.
        idents = list(vsx_names)
        for name in self._target_names(target_name, target_id):
            if name not in idents:
                idents.append(name)

        from_jd = _to_float(parameters.get('fromjd'))
        if from_jd is None:
            from_jd = self._incremental_from_jd(target_id)

        self.query_parameters = {
            'idents': idents,
            'vsx_name': vsx_names[0] if vsx_names else None,
            'target_id': target_id,
            'fromjd': from_jd,
            'tojd': _to_float(parameters.get('tojd')),
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    # --------------------------------------------------------- remote query
    def query_service(self, query_parameters, **kwargs):
        """Single-window fetch for the first identifier that returns data (abstract contract)."""
        idents = query_parameters.get('idents') or []
        vsx_name = query_parameters.get('vsx_name')
        from_jd = query_parameters.get('fromjd') or _DEFAULT_FROM_JD
        to_jd = query_parameters.get('tojd')
        for ident in idents:
            rows, star_name = self._fetch_photometry(ident, from_jd, to_jd)
            if rows:
                self.query_results = {'rows': rows, 'star_name': star_name, 'ident': ident, 'vsx_name': vsx_name}
                return self.query_results
        self.query_results = {'rows': [], 'star_name': None, 'ident': None, 'vsx_name': vsx_name}
        return self.query_results

    def _fetch_chunked(self, ident, from_jd, to_jd, target):
        """Fetch [from_jd, to_jd] for one identifier in bounded JD chunks.

        When ``target`` is set, each chunk is inserted immediately (durable and resumable: a
        timeout mid-load leaves earlier chunks persisted, and the next run's incremental
        ``fromjd`` continues where it stopped). Otherwise rows are accumulated and returned.
        Returns (added, star_name, accumulated_or_None, saw_any).
        """
        chunk = _chunk_days()
        added = 0
        star_names = []
        accumulate = target is None
        accumulated = [] if accumulate else None
        saw_any = False

        lo = from_jd
        while lo < to_jd:
            hi = min(lo + chunk, to_jd)
            rows, star_name = self._fetch_photometry(ident, lo, hi)
            if rows:
                saw_any = True
                if star_name:
                    star_names.append(star_name)
                if accumulate:
                    accumulated.extend(rows)
                else:
                    added += _ingest_photometry(target, rows)
            lo = hi

        star = Counter(star_names).most_common(1)[0][0] if star_names else None
        return added, star, accumulated, saw_any

    def _fetch_photometry(self, ident, from_jd, to_jd):
        params = {
            'view': 'api.delim',
            'ident': ident,
            'fromjd': from_jd,
            'delimiter': AAVSO_DELIMITER,
        }
        if to_jd is not None:
            params['tojd'] = to_jd
        resp = requests.get(AAVSO_API_URL, params=params, headers=_REQUEST_HEADERS, timeout=DATA_SERVICE_HTTP_TIMEOUT)
        resp.raise_for_status()
        return self._parse_delim(resp.text)

    def _parse_delim(self, text):
        """Parse the @@@-delimited AID response into datum dicts; return (rows, star_name)."""
        if not text or not text.strip():
            return [], None
        lines = text.strip().splitlines()
        header = lines[0].split(AAVSO_DELIMITER)
        idx = {name.strip(): i for i, name in enumerate(header)}
        if not {'JD', 'mag', 'band'}.issubset(idx):
            logger.warning('AAVSO response missing expected columns; got %s', list(idx))
            return [], None

        def cell(parts, col):
            i = idx.get(col)
            return parts[i].strip() if (i is not None and i < len(parts)) else ''

        rows = []
        star_names = []
        for line in lines[1:]:
            parts = line.split(AAVSO_DELIMITER)
            jd = _to_float(cell(parts, 'JD'))
            mag = _to_float(cell(parts, 'mag'))
            if jd is None or mag is None:
                continue
            timestamp = Time(jd, format='jd', scale='utc').to_datetime(timezone=dt_timezone.utc)
            value = {'filter': _band_label(cell(parts, 'band')), 'magnitude': mag}

            if cell(parts, 'fainterThan') == '1':
                # Non-detection: fainter-than measurement -> upper limit.
                value['error'] = -1.0
            else:
                uncert = _to_float(cell(parts, 'uncert'))
                if uncert is not None and uncert > 0:
                    value['error'] = uncert
                # visual obs have no uncertainty -> leave 'error' unset (not a limit)

            rows.append({'timestamp': timestamp, 'value': value})
            name = cell(parts, 'starName')
            if name:
                star_names.append(name)

        # Use the most common starName as the AAVSO alias (observer entries vary in case/format).
        star_name = Counter(star_names).most_common(1)[0][0] if star_names else None
        return rows, star_name

    # --------------------------------------------------------- target shape
    def query_targets(self, query_parameters, **kwargs):
        if not query_parameters.get('include_photometry', True):
            return []
        idents = query_parameters.get('idents') or []
        if not idents:
            return []

        vsx_name = query_parameters.get('vsx_name')
        from_jd = query_parameters.get('fromjd') or _DEFAULT_FROM_JD
        to_jd = query_parameters.get('tojd') or _now_jd()

        target = None
        target_id = query_parameters.get('target_id')
        if target_id:
            try:
                target = Target.objects.get(id=target_id)
            except Target.DoesNotExist:
                target = None

        for ident in idents:
            added, star_name, accumulated, saw_any = self._fetch_chunked(ident, from_jd, to_jd, target)
            if not saw_any:
                continue  # this identifier had no AAVSO data; try the next candidate

            # Prefer the coordinate-resolved VSX name as the alias; fall back to observed starName.
            alias = vsx_name or star_name
            result = {'source_location': AAVSO_INFO_URL}
            if alias:
                result['name'] = alias
                result['aliases'] = [alias]
            if accumulated is not None:
                # No target to insert into (ad-hoc query): hand rows to the framework.
                result['reduced_datums'] = {'photometry': accumulated}
            else:
                logger.info('AAVSO: ingested %s new points for target id=%s (ident=%s).', added, target_id, ident)
            return [result]

        return []

    def create_target_from_query(self, target_result, **kwargs):
        return Target(name=target_result.get('name'), type='SIDEREAL', epoch=2000.0)

    def create_aliases_from_query(self, alias_results, **kwargs):
        return [TargetName(name=alias) for alias in alias_results if alias]
