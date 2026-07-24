"""ATLAS forced-photometry DataService (asynchronous, two-phase).

The ATLAS Forced Photometry Server (https://fallingstar-data.com/forcedphot/) is an
*asynchronous queue*: you POST a job, then poll a task URL until it finishes, then
download a whitespace-delimited result table. ATLAS jobs routinely sit in the queue for
several minutes, so this integration is deliberately **decoupled** into two phases rather
than blocking a worker on a poll loop (a single dataservice run is a forked subprocess
that is force-killed at ``DATA_SERVICE_JOB_TIMEOUT`` = 300s):

* **Phase 1 -- submit** (``query_service``, runs in the normal dataservice task): resolves
  coordinates, submits the job, and records an :class:`ATLASForcedPhotJob` row. It ingests
  nothing and returns immediately. A pending job for the same target short-circuits the
  submit so repeat refreshes do not pile up duplicate jobs.
* **Phase 2 -- poll & ingest** (:func:`poll_pending_atlas_jobs`, called every
  ``ATLAS_POLL_INTERVAL_SECONDS`` -- default 5 min -- from db_worker's scheduled loop):
  does a *single* non-blocking check per pending job; when a job has finished it downloads
  the result, cleans the ATLAS AB magnitudes, and bulk-inserts the total photometry.

Efficiency notes: incremental fetch (only request epochs newer than the latest stored
ATLAS point), one authentication per worker (module-cached token, inherited across the
fork), total (reduced-image) photometry read straight from ATLAS's own m/dm columns with
the legacy magnitude/error and outlier cleaning, non-detections ingested as upper limits at
the 5-sigma depth (error = -1), and dedup on ``(timestamp, value)``
so incremental-boundary overlap never duplicates rows.
"""

import json
import logging
from datetime import timezone as dt_timezone
from io import StringIO

import numpy as np
import pandas as pd
import requests
from astropy.time import Time

from django.conf import settings
from django.utils import timezone as dj_timezone

from tom_dataservices.dataservices import DataService, NotConfiguredError, QueryServiceError
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import ATLASQueryForm
from custom_code.data_services.service_utils import (
    DATA_SERVICE_HTTP_TIMEOUT,
    resolve_query_coordinates,
)


logger = logging.getLogger(__name__)

ATLAS_BASE_URL = 'https://fallingstar-data.com/forcedphot'
ATLAS_INFO_URL = 'https://fallingstar-data.com/forcedphot/'

# ATLAS forced photometry only exists in the survey era; MJD floor for a full-history first query.
_DEFAULT_MJD_FLOOR = 55000.0

# We request TOTAL photometry (use_reduced=True -> reduced images, not difference images), so the
# reported AB magnitude column m is the source's total brightness. ATLAS defines m = -2.5*log10(uJy)
# + 23.9, so reading m/dm directly matches its own per-exposure calibration. Cleaning thresholds are
# ported from the legacy bhtom2 ATLAS broker. Negative-flux points carry m < 0 and the range drops them.
_MAG_BRIGHT_LIMIT = 5.0    # drop points brighter than this (and negative-flux "magnitudes")
_MAG_FAINT_LIMIT = 22.0    # drop points fainter than this
_MAX_MAG_ERR = 1.0         # drop points with magnitude error >= this
_OUTLIER_Z_SCORE = 5.0     # drop |z| >= 5 outliers; keeps transients only a few rms above baseline

# Process-lived token cache so a worker authenticates at most once per (base_url, username).
_TOKEN_CACHE = {}


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _atlas_tuning():
    """Runtime knobs, all overridable via settings but usable with sane defaults."""
    return {
        'mjd_floor': float(getattr(settings, 'ATLAS_MJD_FLOOR', _DEFAULT_MJD_FLOOR)),
        'job_max_age': float(getattr(settings, 'ATLAS_JOB_MAX_AGE_SECONDS', 86400.0)),
        'poll_batch': int(getattr(settings, 'ATLAS_POLL_BATCH', 50)),
    }


def _atlas_alias(ra, dec):
    return f'ATLAS+J{ra:.5f}{dec:+.5f}'


def _filter_label(code):
    # Match the project-wide SURVEY(filter) convention used by the plot colour maps
    # (custom_dataproduct_extras.PHOTOMETRY_COLOR_MAP): ATLAS(o) / ATLAS(c).
    code = str(code).strip().lower()
    return {'o': 'ATLAS(o)', 'c': 'ATLAS(c)'}.get(code, f'ATLAS({code})')


def _authed_session(token):
    session = requests.Session()
    session.headers.update({'Authorization': f'Token {token}', 'Accept': 'application/json'})
    return session


# --------------------------------------------------------------------- parsing
def _mjd_to_datetime(mjd):
    return Time(float(mjd), format='mjd', scale='utc').to_datetime(timezone=dt_timezone.utc)


def _clean_atlas_table(df):
    """Turn an ATLAS result table into total-photometry datum dicts.

    Detections use the m/dm columns with the legacy bhtom2 cleaning (5 < m <= 22, dm < 1,
    then |z| >= 5 outlier rejection so a few bad points do not distort the light curve while
    genuine transients only a few rms above a faint baseline are preserved).

    Non-detections -- ATLAS reports these with negative flux, i.e. m < 0 -- are ingested as
    UPPER LIMITS at the 5-sigma limiting magnitude (mag5sig column) with error = -1, which is
    how bhtom3 flags a limit (plotting treats error <= 0 as an upper limit).
    """
    required = {'MJD', 'm', 'dm', 'F'}
    if not required.issubset(df.columns):
        logger.warning('ATLAS result missing expected columns; got %s', list(df.columns))
        return []

    work = df.copy()
    for col in ('MJD', 'm', 'dm'):
        work[col] = pd.to_numeric(work[col], errors='coerce')
    work = work[np.isfinite(work['MJD'])]
    if work.empty:
        return []

    output = []

    # --- detections: valid magnitude range + error cut, then outlier rejection ---
    det = work[(work['m'] > _MAG_BRIGHT_LIMIT) & (work['m'] <= _MAG_FAINT_LIMIT) & (work['dm'] < _MAX_MAG_ERR)]
    std = det['m'].std()
    if len(det) > 2 and std and np.isfinite(std) and std > 0:
        z = np.abs((det['m'] - det['m'].mean()) / std)
        det = det[z < _OUTLIER_Z_SCORE]
    for row in det.itertuples(index=False):
        output.append({
            'timestamp': _mjd_to_datetime(row.MJD),
            'value': {'filter': _filter_label(row.F), 'magnitude': float(row.m), 'error': float(row.dm)},
        })

    # --- upper limits: negative-flux non-detections plotted at the 5-sigma depth, error = -1 ---
    if 'mag5sig' in work.columns:
        work['mag5sig'] = pd.to_numeric(work['mag5sig'], errors='coerce')
        lim = work[
            (work['m'] <= 0)
            & work['mag5sig'].notna()
            & (work['mag5sig'] > _MAG_BRIGHT_LIMIT)
            & (work['mag5sig'] <= _MAG_FAINT_LIMIT)
        ]
        for row in lim.itertuples(index=False):
            output.append({
                'timestamp': _mjd_to_datetime(row.MJD),
                'value': {'filter': _filter_label(row.F), 'magnitude': float(row.mag5sig), 'error': -1.0},
            })

    return output


def _parse_result_text(text):
    if not text or not text.strip():
        return []
    # The header line is prefixed with '###'; strip it so the columns parse cleanly.
    df = pd.read_csv(StringIO(text.replace('###', '')), sep=r'\s+')
    df.columns = [str(col).strip().lstrip('#') for col in df.columns]
    return _clean_atlas_table(df)


# ------------------------------------------------------------------- ingestion
def _reduced_datum_identity(timestamp, value):
    return (timestamp, json.dumps(value, sort_keys=True, separators=(',', ':'), default=str))


def _ingest_photometry(target, rows):
    """Bulk-insert photometry rows, deduping on (timestamp, value). Returns count added."""
    if not rows:
        return 0
    timestamps = [row['timestamp'] for row in rows]
    existing_keys = {
        _reduced_datum_identity(ts, val)
        for ts, val in ReducedDatum.objects.filter(
            target=target,
            source_name=ATLASDataService.name,
            data_type='photometry',
            timestamp__in=timestamps,
        ).values_list('timestamp', 'value')
    }
    seen = set()
    new_rows = []
    for row in rows:
        key = _reduced_datum_identity(row['timestamp'], row['value'])
        if key in existing_keys or key in seen:
            continue
        seen.add(key)
        new_rows.append(ReducedDatum(
            target=target,
            data_type='photometry',
            source_name=ATLASDataService.name,
            source_location=ATLAS_INFO_URL,
            timestamp=row['timestamp'],
            value=row['value'],
        ))
    if new_rows:
        ReducedDatum.objects.bulk_create(new_rows, batch_size=500)
    return len(new_rows)


# ----------------------------------------------------------- phase 2: poller
def poll_pending_atlas_jobs(limit=None):
    """Check pending ATLAS jobs once each; ingest photometry for any that have finished.

    Designed to be called on a fixed cadence (every ATLAS_POLL_INTERVAL_SECONDS) from the
    db_worker scheduled loop. Each job costs at most one HTTP GET (plus one download when it
    is ready), so a tick never blocks the worker on the ATLAS queue.
    """
    from custom_code.models import ATLASForcedPhotJob

    tuning = _atlas_tuning()
    limit = limit or tuning['poll_batch']
    summary = {'checked': 0, 'completed': 0, 'ingested': 0, 'failed': 0}

    jobs = list(
        ATLASForcedPhotJob.objects
        .filter(status=ATLASForcedPhotJob.STATUS_PENDING)
        .select_related('target')
        .order_by('submitted_at')[:limit]
    )
    if not jobs:
        return summary

    try:
        token = ATLASDataService._resolve_token()
    except NotConfiguredError:
        logger.warning('ATLAS poller: no credentials configured; leaving %s job(s) pending.', len(jobs))
        return summary

    session = _authed_session(token)
    try:
        for job in jobs:
            summary['checked'] += 1
            now = dj_timezone.now()

            # Give up on jobs that have been queued far too long.
            if (now - job.submitted_at).total_seconds() > tuning['job_max_age']:
                job.status = ATLASForcedPhotJob.STATUS_FAILED
                job.error = 'Timed out waiting for the ATLAS queue.'
                job.finished_at = now
                job.last_checked_at = now
                job.save(update_fields=['status', 'error', 'finished_at', 'last_checked_at'])
                summary['failed'] += 1
                continue

            job.attempts += 1
            job.last_checked_at = now
            try:
                resp = session.get(job.task_url, timeout=DATA_SERVICE_HTTP_TIMEOUT)
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                job.error = f'Poll failed: {exc}'
                job.save(update_fields=['attempts', 'last_checked_at', 'error'])
                logger.warning('ATLAS poller: task check failed for job id=%s: %s', job.id, exc)
                continue

            if not data.get('finishtimestamp'):
                # Still queued/running; try again next tick.
                job.save(update_fields=['attempts', 'last_checked_at'])
                continue

            result_url = data.get('result_url')
            job.result_url = result_url
            if not result_url:
                # Job finished with no data in the requested window.
                job.status = ATLASForcedPhotJob.STATUS_DONE
                job.finished_at = now
                job.datapoints_added = 0
                job.save()
                summary['completed'] += 1
                continue

            try:
                result = session.get(result_url, timeout=DATA_SERVICE_HTTP_TIMEOUT)
                result.raise_for_status()
                rows = _parse_result_text(result.text)
                added = _ingest_photometry(job.target, rows)
            except Exception as exc:
                job.error = f'Result download/ingest failed: {exc}'
                job.save(update_fields=['attempts', 'last_checked_at', 'result_url', 'error'])
                logger.exception('ATLAS poller: ingest failed for job id=%s', job.id)
                continue

            _ensure_alias(job.target, job.target.ra, job.target.dec)
            job.status = ATLASForcedPhotJob.STATUS_DONE
            job.finished_at = now
            job.datapoints_added = added
            job.error = None
            job.save()
            summary['completed'] += 1
            summary['ingested'] += added
    finally:
        session.close()

    return summary


def _ensure_alias(target, ra, dec):
    ra = _to_float(ra)
    dec = _to_float(dec)
    if ra is None or dec is None:
        return
    name = _atlas_alias(ra, dec)
    if str(target.name or '').strip().casefold() == name.casefold():
        return
    existing = TargetName.objects.filter(name=name).first()
    if existing is None:
        try:
            TargetName.objects.create(target=target, name=name)
        except Exception:
            logger.debug('ATLAS poller: could not create alias %s for target %s.', name, target.id, exc_info=True)


class ATLASDataService(DataService):
    name = 'ATLAS'
    verbose_name = 'ATLAS Forced Photometry'
    update_on_daily_refresh = True
    info_url = ATLAS_INFO_URL
    base_url = ATLAS_BASE_URL
    service_notes = (
        'Query the ATLAS Forced Photometry Server by coordinates for o/c-band photometry. '
        'Runs asynchronously: a job is submitted now and the light curve is ingested a few '
        'minutes later once the ATLAS queue finishes. Requires an ATLAS account '
        '(api_key/token, or username+password) in DATA_SERVICES["ATLAS"].'
    )

    @classmethod
    def get_form_class(cls):
        return ATLASQueryForm

    # ------------------------------------------------------------------ auth
    @classmethod
    def _resolve_token(cls):
        """Return an ATLAS API token, authenticating with username/password if needed.

        Raises NotConfiguredError when no usable credentials are configured.
        """
        config = cls.configuration()  # may raise NotConfiguredError
        token = str(config.get('api_key') or config.get('token') or '').strip()
        if token:
            return token

        username = str(config.get('username') or '').strip()
        password = str(config.get('password') or '').strip()
        if not (username and password):
            raise NotConfiguredError(
                'ATLAS requires either "api_key"/"token" or "username"+"password" '
                'in DATA_SERVICES["ATLAS"].'
            )

        cache_key = (ATLAS_BASE_URL, username)
        cached = _TOKEN_CACHE.get(cache_key)
        if cached:
            return cached

        resp = requests.post(
            f'{ATLAS_BASE_URL}/api-token-auth/',
            data={'username': username, 'password': password},
            timeout=DATA_SERVICE_HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        token = str(resp.json().get('token') or '').strip()
        if not token:
            raise NotConfiguredError('ATLAS authentication did not return a token; check credentials.')
        _TOKEN_CACHE[cache_key] = token
        return token

    # -------------------------------------------------------- query params
    def _incremental_mjd_min(self, target_id):
        """Only fetch epochs newer than the latest ATLAS point already stored."""
        floor = _atlas_tuning()['mjd_floor']
        if not target_id:
            return floor
        latest = (
            ReducedDatum.objects
            .filter(target_id=target_id, source_name=self.name, data_type='photometry')
            .order_by('-timestamp')
            .values_list('timestamp', flat=True)
            .first()
        )
        if not latest:
            return floor
        try:
            return float(Time(latest, scale='utc').mjd)
        except Exception:
            logger.debug('ATLAS: could not convert latest timestamp %r to MJD; using floor.', latest)
            return floor

    def build_query_parameters(self, parameters, **kwargs):
        target_name, ra, dec = resolve_query_coordinates(parameters)

        mjd_min = _to_float(parameters.get('mjd_min'))
        if mjd_min is None:
            mjd_min = self._incremental_mjd_min(parameters.get('target_id'))

        self.query_parameters = {
            'target_name': target_name,
            'target_id': parameters.get('target_id'),
            'ra': ra,
            'dec': dec,
            'mjd_min': mjd_min,
            'mjd_max': _to_float(parameters.get('mjd_max')),
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        # Resolve the token in the parent process so the forked query subprocess inherits it.
        try:
            self.query_parameters['token'] = self._resolve_token()
        except NotConfiguredError:
            self.query_parameters['token'] = None
        return self.query_parameters

    # ------------------------------------------------- phase 1: submit job
    def query_service(self, query_parameters, **kwargs):
        """Submit an ATLAS forced-photometry job and record it for later polling.

        Returns immediately without ingesting; :func:`poll_pending_atlas_jobs` picks the
        job up once the ATLAS queue has finished it.
        """
        from custom_code.models import ATLASForcedPhotJob

        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        empty = {'submitted': False, 'ra': ra, 'dec': dec}
        if ra is None or dec is None or not query_parameters.get('include_photometry', True):
            self.query_results = empty
            return self.query_results

        target_id = query_parameters.get('target_id')
        if target_id and ATLASForcedPhotJob.objects.filter(
            target_id=target_id, status=ATLASForcedPhotJob.STATUS_PENDING
        ).exists():
            logger.info('ATLAS: a pending job already exists for target %s; skipping submit.', target_id)
            self.query_results = empty
            return self.query_results

        token = query_parameters.get('token') or self._resolve_token()
        if not token:
            raise NotConfiguredError('ATLAS token unavailable; configure DATA_SERVICES["ATLAS"].')

        session = _authed_session(token)
        try:
            task_url = self._submit_job(session, ra, dec, query_parameters)
        finally:
            session.close()

        if task_url is None:
            # Throttled by ATLAS (HTTP 429); no job recorded, so the next refresh retries.
            self.query_results = empty
            return self.query_results

        if target_id:
            ATLASForcedPhotJob.objects.create(
                target_id=target_id,
                task_url=task_url,
                mjd_min=_to_float(query_parameters.get('mjd_min')),
                mjd_max=_to_float(query_parameters.get('mjd_max')),
            )
            logger.info('ATLAS: submitted job for target %s (%s); awaiting queue.', target_id, task_url)

        self.query_results = {'submitted': True, 'task_url': task_url, 'ra': ra, 'dec': dec}
        return self.query_results

    def _submit_job(self, session, ra, dec, query_parameters):
        # use_reduced=True -> total photometry from reduced images (not difference images).
        payload = {'ra': ra, 'dec': dec, 'send_email': False, 'use_reduced': True}
        mjd_min = _to_float(query_parameters.get('mjd_min'))
        if mjd_min is not None:
            payload['mjd_min'] = mjd_min
        mjd_max = _to_float(query_parameters.get('mjd_max'))
        if mjd_max is not None:
            payload['mjd_max'] = mjd_max

        resp = session.post(f'{ATLAS_BASE_URL}/queue/', data=payload, timeout=DATA_SERVICE_HTTP_TIMEOUT)
        if resp.status_code == 429:
            # Rate-limited: expected during bulk refreshes. Skip gracefully; retried next cycle.
            detail = ''
            try:
                detail = resp.json().get('detail', '')
            except ValueError:
                pass
            logger.info('ATLAS: forced-photometry queue is throttling (HTTP 429). %s', detail)
            return None
        resp.raise_for_status()
        task_url = resp.json().get('url')
        if not task_url:
            raise QueryServiceError('ATLAS did not return a task URL for the submitted job.')
        return task_url

    def query_targets(self, query_parameters, **kwargs):
        # Async service: submit the job now, ingest later via the poller. No synchronous data.
        self.query_service(query_parameters, **kwargs)
        return []

    # ----------------------------------- UI "create target from query" path
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
