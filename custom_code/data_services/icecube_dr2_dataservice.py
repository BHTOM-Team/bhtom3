"""IceCube IceTracks-DR2 track-like neutrino candidates (April 2008 - May 2022).

The 14 season files (~220 MB of text on Harvard Dataverse, doi:10.7910/DVN/MMIIZA) ship with
BHTOM as one compressed table built by `manage.py build_icecube_dr2`, so a query is a local cone
search with no network access.

An event is matched when the target lies within `n_sigma` times the event's own angular error
(AngErr, IceCube's per-event 1-sigma estimate, floored at 0.2 deg) and within `max_radius_deg`.
The sample is dominated by atmospheric muons and neutrinos: a handful of matches per target is
the expected background, not evidence of a neutrino source.

IceCube asks that this release not be combined with other IceCube data releases.
"""
import functools
import logging
import os
from datetime import timezone

import numpy as np
from astropy.time import Time

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import IceCubeDR2QueryForm
from custom_code.data_services.service_utils import upsert_reduced_datums


logger = logging.getLogger(__name__)

DATAVERSE_DOI = 'doi:10.7910/DVN/MMIIZA'
ICECUBE_DR2_PAGE = f'https://dataverse.harvard.edu/dataset.xhtml?persistentId={DATAVERSE_DOI}'
ICECUBE_DR2_DATA_PATH = os.path.join(os.path.dirname(__file__), 'data', 'icecube_dr2_tracks.npz')

NEUTRINO_DATA_TYPE = 'neutrino'
ICECUBE_DR2_FILTER = 'IceCube(DR2 tracks)'

# Integer encoding of the packed table, at exactly the precision IceCube publishes.
MJD_QUANTUM = 1e-8  # days
SCALES = {'log10_energy': 100, 'ang_err': 100, 'ra': 1000, 'dec': 1000}

DEFAULT_N_SIGMA = 1.0
DEFAULT_MAX_RADIUS_DEG = 3.0

# Association strength per event: log10 of IceCube's spatial signal-over-background weight,
# S/B = [exp(-r^2 / 2 sigma^2) / (2 pi sigma^2)] / B(dec), with sigma = AngErr and B the
# release's own background density (fraction of events per steradian) at the event's Dec.
# It rewards both a small offset and a small AngErr. Every matched event has S/B >> 1, so it
# ranks events against each other; it is not a detection significance.
SB_HIGH = 4.0  # ~ AngErr <= 0.7 deg with the target well inside the error circle; below is 'low'
SIN_DEC_BINS = 100


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@functools.lru_cache(maxsize=1)
def load_icecube_dr2_events(path=ICECUBE_DR2_DATA_PATH):
    """Unpack the event table once per process (~50 MB in memory)."""
    with np.load(path) as packed:
        mjd_ticks = np.concatenate([packed['mjd_start'], packed['mjd_delta'].astype(np.int64)]).cumsum()
        counts, _ = np.histogram(np.sin(np.radians(packed['dec'] / SCALES['dec'])),
                                 bins=SIN_DEC_BINS, range=(-1.0, 1.0))
        bin_solid_angle = 2 * np.pi * (2.0 / SIN_DEC_BINS)
        return {
            'background_density': counts / counts.sum() / bin_solid_angle,
            'mjd': mjd_ticks * MJD_QUANTUM,
            'run': packed['run'],
            'event': packed['event'],
            'subevent': packed['subevent'],
            'log10_energy': packed['log10_energy'],
            'ang_err': packed['ang_err'],
            'ra': packed['ra'],
            'dec': packed['dec'],
            'season': packed['season'],
            'seasons': packed['seasons'].tolist(),
            'dataverse_version': str(packed['dataverse_version']),
        }


def _angular_separation_deg(ra1, dec1, ra2, dec2):
    """Haversine separation in degrees; stable at the sub-degree scales that matter here."""
    ra1, dec1, ra2, dec2 = (np.radians(x) for x in (ra1, dec1, ra2, dec2))
    a = np.sin((dec2 - dec1) / 2) ** 2 + np.cos(dec1) * np.cos(dec2) * np.sin((ra2 - ra1) / 2) ** 2
    return np.degrees(2 * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0))))


def association_log10_sb(separation_deg, ang_err_deg, background_density):
    sigma = np.radians(ang_err_deg)
    offset = np.radians(separation_deg)
    signal = np.exp(-offset ** 2 / (2 * sigma ** 2)) / (2 * np.pi * sigma ** 2)
    return np.log10(signal / background_density)


def association_level(log10_sb):
    if log10_sb is None:
        return None
    return 'high' if log10_sb >= SB_HIGH else 'low'


def icecube_dr2_cone_search(ra, dec, n_sigma=DEFAULT_N_SIGMA, max_radius_deg=DEFAULT_MAX_RADIUS_DEG,
                            events=None):
    """Events whose n_sigma error circle contains (ra, dec), nearest first."""
    events = events or load_icecube_dr2_events()
    dec_scale = SCALES['dec']
    # Integer Dec band first: cheap over all ~1.6M rows, leaves a few thousand for the trig.
    band = np.flatnonzero(np.abs(events['dec'] - dec * dec_scale) <= max_radius_deg * dec_scale)
    event_ra = events['ra'][band] / SCALES['ra']
    event_dec = events['dec'][band] / dec_scale
    separation = _angular_separation_deg(ra, dec, event_ra, event_dec)
    ang_err = events['ang_err'][band] / SCALES['ang_err']
    keep = (separation <= n_sigma * ang_err) & (separation <= max_radius_deg)

    rows = band[keep]
    order = np.argsort(separation[keep])
    rows, separation, ang_err = rows[order], separation[keep][order], ang_err[keep][order]
    sin_dec_bin = min(int((np.sin(np.radians(dec)) + 1.0) / 2.0 * SIN_DEC_BINS), SIN_DEC_BINS - 1)
    log10_sb = association_log10_sb(separation, ang_err, events['background_density'][sin_dec_bin])
    return [
        {
            'mjd': float(events['mjd'][row]),
            'run': int(events['run'][row]),
            'event': int(events['event'][row]),
            'subevent': int(events['subevent'][row]),
            'log10_energy_gev': float(events['log10_energy'][row]) / SCALES['log10_energy'],
            'ang_err_deg': float(err),
            'ra': float(events['ra'][row]) / SCALES['ra'],
            'dec': float(events['dec'][row]) / dec_scale,
            'separation_deg': round(float(sep), 3),
            'season': events['seasons'][events['season'][row]],
            # The high/low band is derived from this when plotting, so a threshold change
            # needs no re-query.
            'log10_sb': round(float(sb), 2),
        }
        for row, sep, err, sb in zip(rows, separation, ang_err, log10_sb)
    ]


class IceCubeDR2DataService(DataService):
    name = 'IceCube'
    verbose_name = 'IceCube DR2 (neutrino tracks)'
    # The release is static and bundled; it never needs a daily re-query.
    update_on_daily_refresh = False
    # An event is identified by IceCube's own IDs, so re-queries fill new keys in place
    # instead of storing a second copy of every event.
    upsert_identity_keys = ('run', 'event', 'subevent')
    info_url = ICECUBE_DR2_PAGE
    service_notes = (
        'Local cone search of the bundled IceCube IceTracks-DR2 track events (2008-2022). An event '
        'matches when the target lies inside n x its angular error. Matches are mostly atmospheric '
        'background; do not combine with other IceCube releases.'
    )

    @classmethod
    def get_form_class(cls):
        return IceCubeDR2QueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_name, ra, dec = resolve_query_coordinates(parameters)
        n_sigma = _to_float(parameters.get('n_sigma'))
        max_radius_deg = _to_float(parameters.get('max_radius_deg'))
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'n_sigma': n_sigma if n_sigma is not None else DEFAULT_N_SIGMA,
            'max_radius_deg': max_radius_deg if max_radius_deg is not None else DEFAULT_MAX_RADIUS_DEG,
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        matches = []
        if ra is not None and dec is not None:
            try:
                matches = icecube_dr2_cone_search(
                    ra, dec,
                    n_sigma=_to_float(query_parameters.get('n_sigma')) or DEFAULT_N_SIGMA,
                    max_radius_deg=_to_float(query_parameters.get('max_radius_deg')) or DEFAULT_MAX_RADIUS_DEG,
                )
            except OSError as exc:
                logger.warning('IceCube DR2: event table unavailable (%s); run manage.py build_icecube_dr2.', exc)
        self.query_results = {'matches': matches, 'ra': ra, 'dec': dec, 'source_location': ICECUBE_DR2_PAGE}
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        if data.get('ra') is None or data.get('dec') is None or not data['matches']:
            return []
        return [{
            'name': None,
            'ra': data['ra'],
            'dec': data['dec'],
            'aliases': [],
            'reduced_datums': {NEUTRINO_DATA_TYPE: self._build_neutrino_datums(data['matches'])},
            'source_location': data['source_location'],
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
        if data_type != NEUTRINO_DATA_TYPE or not data:
            return
        upsert_reduced_datums(
            target, NEUTRINO_DATA_TYPE, self.name,
            kwargs.get('source_location') or self.info_url, data,
            identity_keys=('run', 'event', 'subevent'),
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

    @staticmethod
    def _build_neutrino_datums(matches):
        output = []
        for match in matches:
            value = {key: val for key, val in match.items() if key != 'mjd'}
            value['filter'] = ICECUBE_DR2_FILTER
            value['facility'] = 'IceCube'
            output.append({
                'timestamp': Time(match['mjd'], format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': value,
            })
        return output
