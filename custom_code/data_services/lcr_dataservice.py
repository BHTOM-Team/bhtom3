"""Fermi-LAT Light Curve Repository (LCR) gamma-ray light curves.

The LCR (Abdollahi et al. 2023, ApJS 265, 31) holds continually updated, flux-calibrated
0.1-100 GeV light curves at 3-day ('daily'), weekly and monthly cadence for the variable
sources of the 4FGL catalogue. Unlike FAVA (relative flux changes), these are calibrated
fluxes with upper limits, so they are plotted on the physical-flux axis of the high-energy
plot.

Matching: LAT positions are uncertain by arcminutes, so a target near a 4FGL position is not
evidence that it is the gamma-ray emitter (and bright sources can even sit outside their own
95% ellipse). Instead the target must be the 4FGL-DR4 associated counterpart: within
LCR_COUNTERPART_RADIUS_ARCSEC of the counterpart's position, with association probability
(Bayesian or likelihood-ratio, whichever is higher) >= LCR_MIN_ASSOC_PROB. Unassociated 4FGL
sources are never matched automatically; a manual query can opt in with allow_unassociated,
which falls back to the nearest unassociated LCR source within radius_arcmin.

The 4FGL-DR4 counterparts come from HEASARC (fermilpsc, ~0.8 MB) and the LCR source list
from the LCR web API; both are cached in memory for a day. A DR4 name missing from the LCR
list (renamed sources) is mapped to the LCR source within LCR_RENAME_RADIUS_ARCMIN.

Access is the public JSON API used by the LCR web pages (queryDB.php):
- lightCurveData with flux_type=energy returns energy flux in MeV cm^-2 s^-1 (checked
  against the 4FGL Energy_Flux100 of 3C 279), converted here to erg cm^-2 s^-1. Each array
  entry starts with the bin centre in Fermi MET; 'ts' is the test statistic, not time.
- flux_error gives [MET, lower, upper] bounds; the stored error is half their difference.
  Bins with fit_convergence != 0 are dropped. Upper limits (TS < 4) are stored with
  error -1, the upper-limit convention used for photometry.
"""

import io
import logging
import math
import threading
import time
from datetime import timezone
from urllib.parse import quote

import numpy as np
import requests
from astropy.io.votable import parse_single_table
from astropy.time import Time

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target

from custom_code.data_services.forms import LCRQueryForm
from custom_code.data_services.service_utils import (
    DATA_SERVICE_HTTP_TIMEOUT,
    resolve_query_coordinates,
    upsert_reduced_datums,
)

logger = logging.getLogger(__name__)

LCR_PAGE_URL = 'https://fermi.gsfc.nasa.gov/ssc/data/access/lat/LightCurveRepository/'
LCR_QUERY_URL = f'{LCR_PAGE_URL}queryDB.php'
# Parameters the LCR web pages themselves send for the 4FGL source list.
LCR_SOURCE_LIST_PARAMS = {'typeOfRequest': 'SourceList', 'catalog': '4FGL', 'magicWord': '130427A'}
LCR_SOURCE_LIST_TTL_S = 24 * 3600
LCR_CADENCES = {'daily': '3-day', 'weekly': 'weekly', 'monthly': 'monthly'}
LCR_DEFAULT_CADENCE = 'weekly'
# Counterpart positions are optical/radio; real counterparts sit well under 1" from targets.
LCR_COUNTERPART_RADIUS_ARCSEC = 3.0
LCR_MIN_ASSOC_PROB = 0.8
LCR_RENAME_RADIUS_ARCMIN = 1.0
# Only for the manual allow_unassociated fallback: nearest unassociated LCR source.
LCR_DEFAULT_RADIUS_ARCMIN = 3.0
FGL_COUNTERPART_TAP_URL = 'https://heasarc.gsfc.nasa.gov/xamin/vo/tap/sync'
FGL_COUNTERPART_QUERY = (
    'SELECT name, ra, dec, assoc_name, assoc_ra, assoc_dec, assoc_prob_bay, assoc_prob_lr '
    'FROM fermilpsc'
)
LCR_FLUX_UNIT = 'erg/cm2/s'
MEV_TO_ERG = 1.602176634e-6
FERMI_MET_EPOCH_MJD = 51910.0

LCR_ACKNOWLEDGEMENT = (
    'This research uses data from the Fermi-LAT Light Curve Repository (Abdollahi et al. '
    '2023, ApJS 265, 31), provided by the Fermi Science Support Center.'
)

_source_list_cache = {'loaded_at': 0.0, 'sources': []}
_source_list_lock = threading.Lock()
_counterpart_cache = {'loaded_at': 0.0, 'counterparts': []}
_counterpart_lock = threading.Lock()


def _to_float(value):
    if value is np.ma.masked:
        return None
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def _lcr_filter(cadence):
    return f'LAT-LCR({LCR_CADENCES.get(cadence, cadence)})'


def _source_list():
    """4FGL names and positions from the LCR, cached in memory for a day."""
    with _source_list_lock:
        if _source_list_cache['sources'] and time.time() - _source_list_cache['loaded_at'] < LCR_SOURCE_LIST_TTL_S:
            return _source_list_cache['sources']
        response = requests.get(LCR_QUERY_URL, params=LCR_SOURCE_LIST_PARAMS, timeout=DATA_SERVICE_HTTP_TIMEOUT)
        response.raise_for_status()
        sources = []
        for row in response.json():
            ra, dec = _to_float(row.get('RAJ2000')), _to_float(row.get('DEJ2000'))
            name = str(row.get('Source_Name') or '').strip()
            if name and ra is not None and dec is not None:
                sources.append({'name': name, 'ra': ra, 'dec': dec, 'assoc': str(row.get('ASSOC1') or '').strip()})
        _source_list_cache.update(loaded_at=time.time(), sources=sources)
        return sources


def _angular_separation_deg(ra1, dec1, ra2, dec2):
    ra1, dec1, ra2, dec2 = map(math.radians, (ra1, dec1, ra2, dec2))
    cos_sep = (math.sin(dec1) * math.sin(dec2)
               + math.cos(dec1) * math.cos(dec2) * math.cos(ra1 - ra2))
    return math.degrees(math.acos(min(1.0, max(-1.0, cos_sep))))


def _counterparts():
    """4FGL-DR4 sources with an associated counterpart position, cached in memory for a day."""
    with _counterpart_lock:
        if _counterpart_cache['counterparts'] and time.time() - _counterpart_cache['loaded_at'] < LCR_SOURCE_LIST_TTL_S:
            return _counterpart_cache['counterparts']
        response = requests.post(
            FGL_COUNTERPART_TAP_URL,
            data={'REQUEST': 'doQuery', 'LANG': 'ADQL', 'QUERY': FGL_COUNTERPART_QUERY, 'MAXREC': 20000},
            timeout=DATA_SERVICE_HTTP_TIMEOUT,
        )
        response.raise_for_status()
        table = parse_single_table(io.BytesIO(response.content), verify='ignore').to_table(use_names_over_ids=True)
        counterparts = []
        for row in table:
            assoc_ra, assoc_dec = _to_float(row['assoc_ra']), _to_float(row['assoc_dec'])
            if assoc_ra is None or assoc_dec is None:
                continue
            probabilities = [p for p in (_to_float(row['assoc_prob_bay']), _to_float(row['assoc_prob_lr'])) if p is not None]
            counterparts.append({
                'name': str(row['name']).strip(),
                'ra': _to_float(row['ra']),
                'dec': _to_float(row['dec']),
                'assoc': str(row['assoc_name']).strip(),
                'assoc_ra': assoc_ra,
                'assoc_dec': assoc_dec,
                'assoc_prob': max(probabilities) if probabilities else None,
            })
        _counterpart_cache.update(loaded_at=time.time(), counterparts=counterparts)
        return counterparts


def _nearest(items, ra, dec, radius_deg, ra_key='ra', dec_key='dec'):
    """(item, separation_deg) of the nearest item within radius_deg, or (None, None)."""
    best, best_separation = None, None
    for item in items:
        if item[ra_key] is None or item[dec_key] is None or abs(item[dec_key] - dec) > radius_deg:
            continue
        separation = _angular_separation_deg(ra, dec, item[ra_key], item[dec_key])
        if separation <= radius_deg and (best_separation is None or separation < best_separation):
            best, best_separation = item, separation
    return best, best_separation


def _lcr_source_for(fgl):
    """The LCR source for a 4FGL-DR4 source: same name, or the renamed one at its position."""
    sources = _source_list()
    by_name = next((s for s in sources if s['name'] == fgl['name']), None)
    if by_name:
        return by_name
    if fgl['ra'] is None or fgl['dec'] is None:
        return None
    renamed, _ = _nearest(sources, fgl['ra'], fgl['dec'], LCR_RENAME_RADIUS_ARCMIN / 60.0)
    return renamed


def _match_counterpart(ra, dec):
    """LCR source whose 4FGL-DR4 counterpart is the target, as a match dict, or None."""
    counterpart, separation = _nearest(
        _counterparts(), ra, dec, LCR_COUNTERPART_RADIUS_ARCSEC / 3600.0, ra_key='assoc_ra', dec_key='assoc_dec',
    )
    if counterpart is None:
        return None
    if counterpart['assoc_prob'] is None or counterpart['assoc_prob'] < LCR_MIN_ASSOC_PROB:
        logger.info('4FGL counterpart %s of %s has association probability %s; not used',
                    counterpart['assoc'], counterpart['name'], counterpart['assoc_prob'])
        return None
    source = _lcr_source_for(counterpart)
    if source is None:
        return None
    return {
        'name': source['name'],
        'assoc': counterpart['assoc'],
        'assoc_prob': counterpart['assoc_prob'],
        'match_method': 'counterpart',
        'counterpart_separation_arcsec': separation * 3600.0,
        'separation_arcmin': _angular_separation_deg(ra, dec, source['ra'], source['dec']) * 60.0,
    }


def _match_unassociated(ra, dec, radius_arcmin):
    """Manual fallback: nearest LCR source with no 4FGL-DR4 counterpart within radius_arcmin."""
    associated = {c['name'] for c in _counterparts()}
    candidates = [s for s in _source_list() if s['name'] not in associated]
    source, separation = _nearest(candidates, ra, dec, radius_arcmin / 60.0)
    if source is None:
        return None
    return {
        'name': source['name'],
        'assoc': '',
        'assoc_prob': None,
        'match_method': 'unassociated position',
        'counterpart_separation_arcsec': None,
        'separation_arcmin': separation * 60.0,
    }


def _fetch_light_curve(source_name, cadence):
    response = requests.get(
        LCR_QUERY_URL,
        params={
            'typeOfRequest': 'lightCurveData',
            'source_name': source_name,
            'cadence': cadence,
            'flux_type': 'energy',
            'index_type': 'fixed',
            'ts_min': 4,
        },
        timeout=DATA_SERVICE_HTTP_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def _met_to_datetime(met):
    return Time(FERMI_MET_EPOCH_MJD + met / 86400.0, format='mjd', scale='utc').to_datetime(timezone=timezone.utc)


class LCRDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return LCR_DEFAULT_RADIUS_ARCMIN * 60.0

    name = 'FermiLCR'
    verbose_name = 'Fermi-LAT Light Curve Repository'
    # The LCR is continually updated as new LAT data arrive.
    update_on_daily_refresh = True
    info_url = LCR_PAGE_URL
    acknowledgement = LCR_ACKNOWLEDGEMENT
    upsert_identity_keys = ('filter', 'upper_limit')
    service_notes = (
        'Query the Fermi-LAT Light Curve Repository by coordinates. A 4FGL source is used only '
        'when the target is its associated counterpart (within 3 arcsec of the 4FGL-DR4 '
        'counterpart position, association probability >= 0.8); unassociated 4FGL sources are '
        'matched only on request. Its calibrated 0.1-100 GeV energy-flux light curve (weekly by '
        'default, or 3-day / monthly) is converted to erg/cm^2/s and plotted on the high-energy '
        'plot, with upper limits for bins with TS < 4. No aliases are added.'
    )

    @classmethod
    def get_form_class(cls):
        return LCRQueryForm

    @classmethod
    def get_acknowledgement(cls):
        return cls.acknowledgement

    def build_query_parameters(self, parameters, **kwargs):
        target_name, ra, dec = resolve_query_coordinates(parameters)
        cadence = parameters.get('cadence') or LCR_DEFAULT_CADENCE
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcmin': parameters.get('radius_arcmin') or LCR_DEFAULT_RADIUS_ARCMIN,
            'cadence': cadence if cadence in LCR_CADENCES else LCR_DEFAULT_CADENCE,
            # Automatic (daily/target-creation) queries never set this.
            'allow_unassociated': bool(parameters.get('allow_unassociated', False)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcmin = _to_float(query_parameters.get('radius_arcmin')) or LCR_DEFAULT_RADIUS_ARCMIN
        cadence = query_parameters.get('cadence') or LCR_DEFAULT_CADENCE

        match, light_curve = None, None
        if ra is not None and dec is not None:
            try:
                match = _match_counterpart(ra, dec)
                if match is None and query_parameters.get('allow_unassociated'):
                    match = _match_unassociated(ra, dec, radius_arcmin)
                if match:
                    light_curve = _fetch_light_curve(match['name'], cadence)
            except Exception as exc:
                logger.warning('Fermi LCR query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                match, light_curve = None, None

        self.query_results = {
            'match': match,
            'light_curve': light_curve,
            'cadence': cadence,
            'source_location': (
                f"{LCR_PAGE_URL}source.php?source_name={quote(match['name'])}" if match else LCR_PAGE_URL
            ),
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        match = data.get('match')
        if data.get('ra') is None or data.get('dec') is None or not match:
            return []

        datums = self._build_highenergy_datums(data.get('light_curve'), match, data.get('cadence'))
        if not datums:
            return []

        return [{
            'name': match['name'].replace(' ', '_'),
            'ra': data['ra'],
            'dec': data['dec'],
            # High-energy data only: 4FGL names are added by other services if wanted.
            'aliases': [],
            'reduced_datums': {'highenergy': datums},
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
        if data_type != 'highenergy' or not data:
            return 0
        created, _updated = upsert_reduced_datums(
            target=target,
            data_type='highenergy',
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

    def _build_highenergy_datums(self, light_curve, match, cadence):
        if not isinstance(light_curve, dict):
            return []

        def by_met(key):
            return {row[0]: row[1:] for row in light_curve.get(key) or [] if isinstance(row, list) and len(row) >= 2}

        bad_fit = {met for met, values in by_met('fit_convergence').items() if values[0] != 0}
        test_statistic = {met: values[0] for met, values in by_met('ts').items()}
        photon_index = {met: values[0] for met, values in by_met('photon_index').items()}
        errors = by_met('flux_error')
        base = {
            'filter': _lcr_filter(cadence),
            'flux_unit': LCR_FLUX_UNIT,
            'energy_band': '0.1-100 GeV',
            'cadence': cadence,
            'fgl_name': match['name'],
            'fgl_assoc': match.get('assoc') or '',
            'fgl_assoc_prob': match.get('assoc_prob'),
            'match_method': match['match_method'],
            'counterpart_separation_arcsec': (
                round(match['counterpart_separation_arcsec'], 3)
                if match.get('counterpart_separation_arcsec') is not None else None
            ),
            'match_separation_arcmin': round(match['separation_arcmin'], 3),
            'facility': 'Fermi-LAT',
            'observer': 'LCR',
        }
        output = []
        for met, values in by_met('flux').items():
            flux_mev = _to_float(values[0])
            bounds = errors.get(met) or []
            low, high = (_to_float(bounds[0]), _to_float(bounds[1])) if len(bounds) >= 2 else (None, None)
            if met in bad_fit or flux_mev is None or flux_mev <= 0 or low is None or high is None or high <= low:
                continue
            output.append({
                'timestamp': _met_to_datetime(met),
                'value': {
                    **base,
                    'flux': flux_mev * MEV_TO_ERG,
                    'error': (high - low) / 2.0 * MEV_TO_ERG,
                    'ts': _to_float(test_statistic.get(met)),
                    'photon_index': _to_float(photon_index.get(met)),
                    'met': met,
                    'upper_limit': False,
                },
            })
        for met, values in by_met('flux_upper_limits').items():
            limit_mev = _to_float(values[0])
            if met in bad_fit or limit_mev is None or limit_mev <= 0:
                continue
            output.append({
                'timestamp': _met_to_datetime(met),
                'value': {
                    **base,
                    'flux': limit_mev * MEV_TO_ERG,
                    'error': -1.0,
                    'ts': _to_float(test_statistic.get(met)),
                    'met': met,
                    'upper_limit': True,
                },
            })
        return output
