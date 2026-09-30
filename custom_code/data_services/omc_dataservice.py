"""INTEGRAL Optical Monitoring Camera (OMC) V-band light curves from the CAB OMC archive.

OMC has monitored the optical counterparts of INTEGRAL's high-energy targets (X-ray
binaries, AGN, CVs, ...) and serendipitous variables in its field of view since 2002,
in Johnson V down to V ~ 16-17. The archive exposes an IVOA SSA cone search whose rows
link to per-source light curves as VOTables (Time in MJD, Mag, MagErr, Problems).

One star often has several OMC light curves: the same exposures measured in different
OMC sub-windows, with magnitudes differing by a few 0.01 mag. Importing all of them would
double-count every epoch, so only the longest light curve of the nearest source is used.

PROBLEMS is a bit register (centroid off, bad centroid, anomalous PSF, low flux, bad
pixels, extended source, ...); see https://sdc.cab.inta-csic.es/omc/help/documentation.jsp.
Points are imported when their only problems are in OMC_ALLOWED_PROBLEMS.
"""

import io
import logging
import math
from datetime import timezone

import numpy as np
import requests
from astropy.io.votable import parse_single_table
from astropy.time import Time

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target

from custom_code.data_services.forms import OMCQueryForm
from custom_code.data_services.service_utils import (
    DATA_SERVICE_HTTP_TIMEOUT,
    resolve_query_coordinates,
    upsert_reduced_datums,
)

logger = logging.getLogger(__name__)

OMC_PAGE_URL = 'https://sdc.cab.inta-csic.es/omc/'
OMC_SSA_URL = 'https://sdc.cab.inta-csic.es/omc/jsp/ssap.jsp'
OMC_FILTER = 'OMC(V)'
# OMC pixels are 17.5"; catalogued positions of the monitored sources are much better.
OMC_DEFAULT_RADIUS_ARCSEC = 10.0
# Light curves this much further than the nearest one are other stars, not duplicates.
OMC_SAME_SOURCE_ARCSEC = 3.0
# PROBLEMS bits that leave the photometry usable: 16 anomalous PSF shape (set on most
# points, which agree with unflagged ones), 256/512 bad pixel in the 5x5/3x3 rim but not
# the centre. Any other bit (centroid, low flux, sky, extended, unknown mag, ...) rejects.
OMC_ALLOWED_PROBLEMS = 16 | 256 | 512
OMC_MIN_MAG = 0.0
OMC_MAX_MAG = 30.0
OMC_MAX_MAG_ERROR = 1.0

OMC_ACKNOWLEDGEMENT = (
    'This research uses data from the INTEGRAL Optical Monitoring Camera (OMC; '
    'Mas-Hesse et al. 2003, A&A 411, L261), processed by the OMC team at the Centro de '
    'Astrobiologia (CAB, INTA-CSIC) and retrieved from the OMC archive.'
)


def _to_float(value):
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def _text(value):
    if isinstance(value, bytes):
        value = value.decode('utf-8', 'replace')
    return str(value or '').strip()


def _angular_separation_arcsec(ra1, dec1, ra2, dec2):
    ra1, dec1, ra2, dec2 = map(math.radians, (ra1, dec1, ra2, dec2))
    cos_sep = (math.sin(dec1) * math.sin(dec2)
               + math.cos(dec1) * math.cos(dec2) * math.cos(ra1 - ra2))
    return math.degrees(math.acos(min(1.0, max(-1.0, cos_sep)))) * 3600.0


def _get_votable(url, params=None):
    response = requests.get(url, params=params, timeout=DATA_SERVICE_HTTP_TIMEOUT)
    response.raise_for_status()
    table = parse_single_table(io.BytesIO(response.content), verify='ignore')
    # The SSA response has short FIELD IDs (acref, nsamples); the names are the documented ones.
    return table.to_table(use_names_over_ids=True)


def _search_light_curves(ra, dec, radius_arcsec):
    """SSA cone search; returns light-curve descriptors sorted by distance."""
    rows = _get_votable(OMC_SSA_URL, params={
        'REQUEST': 'queryData',
        'POS': f'{ra},{dec}',
        'SIZE': radius_arcsec / 3600.0,
    })
    found = []
    for row in rows:
        coords = row['Coordinates']
        # Parsed as a 2-element array; fall back to an 'ra dec' string just in case.
        coords = _text(coords).split() if isinstance(coords, (str, bytes)) else list(np.ravel(coords))
        lc_ra = _to_float(coords[0]) if len(coords) == 2 else None
        lc_dec = _to_float(coords[1]) if len(coords) == 2 else None
        url = _text(row['AccessReference'])
        if lc_ra is None or lc_dec is None or not url:
            continue
        found.append({
            'url': url.replace('http://', 'https://', 1),
            'title': _text(row['Title']),
            'target_name': _text(row['TargetName']),
            'ra': lc_ra,
            'dec': lc_dec,
            'n_samples': int(_to_float(row['NumberOfSamples']) or 0),
            'separation_arcsec': _angular_separation_arcsec(ra, dec, lc_ra, lc_dec),
        })
    return sorted(found, key=lambda item: item['separation_arcsec'])


def _select_light_curve(candidates):
    """Longest light curve among those of the nearest source (duplicates of one star)."""
    if not candidates:
        return None
    nearest = candidates[0]['separation_arcsec']
    same_source = [c for c in candidates if c['separation_arcsec'] <= nearest + OMC_SAME_SOURCE_ARCSEC]
    return max(same_source, key=lambda c: c['n_samples'])


def _omc_id(title):
    """'OMC Light curve. OMCID: 2678000054, type 0004' -> '2678000054'."""
    marker = 'OMCID:'
    if marker not in title:
        return ''
    return title.split(marker, 1)[1].split(',', 1)[0].strip()


class OMCDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return OMC_DEFAULT_RADIUS_ARCSEC

    name = 'OMC'
    verbose_name = 'INTEGRAL OMC (V band)'
    # OMC keeps observing while INTEGRAL archive processing continues; not worth a daily poll.
    update_on_daily_refresh = False
    info_url = OMC_PAGE_URL
    acknowledgement = OMC_ACKNOWLEDGEMENT
    upsert_identity_keys = ('filter', 'omc_id')
    service_notes = (
        'Query INTEGRAL Optical Monitoring Camera V-band light curves (2002 onwards, V < ~16-17) '
        'from the CAB OMC archive by coordinates. The longest light curve of the nearest OMC '
        'source is imported, keeping points that are unflagged or flagged only for an anomalous '
        'PSF shape or a bad pixel outside the source centre. Photometry only; no aliases are added.'
    )

    @classmethod
    def get_form_class(cls):
        return OMCQueryForm

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

        match = None
        lc_table = None
        if ra is not None and dec is not None and query_parameters.get('include_photometry', True):
            try:
                match = _select_light_curve(_search_light_curves(ra, dec, radius_arcsec))
                if match:
                    lc_table = _get_votable(match['url'])
                else:
                    logger.debug('OMC returned no light curve for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('OMC query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                match, lc_table = None, None

        self.query_results = {
            'match': match,
            'lc_table': lc_table,
            'source_location': (match or {}).get('url') or OMC_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        match = data.get('match')
        lc_table = data.get('lc_table')
        if data.get('ra') is None or data.get('dec') is None or not match or lc_table is None:
            return []

        datums = self._build_photometry_datums(lc_table, match)
        if not datums:
            return []

        omc_id = _omc_id(match['title'])
        return [{
            'name': f'OMC_{omc_id}' if omc_id else (match['target_name'] or 'OMC'),
            'ra': data['ra'],
            'dec': data['dec'],
            # Photometry only: OMC ids are not names anyone searches for.
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

    def _build_photometry_datums(self, lc_table, match):
        columns = set(lc_table.colnames)
        if not {'Time', 'Mag', 'MagErr', 'Problems'} <= columns:
            logger.warning('OMC light curve %s lacks expected columns: %s', match.get('url'), sorted(columns))
            return []

        mjds = np.ma.filled(np.ma.asarray(lc_table['Time'], dtype=float), np.nan)
        mags = np.ma.filled(np.ma.asarray(lc_table['Mag'], dtype=float), np.nan)
        errors = np.ma.filled(np.ma.asarray(lc_table['MagErr'], dtype=float), np.nan)
        # A missing flag is not a clean flag: -1 has every bit set, so it is rejected.
        problems = np.ma.filled(np.ma.asarray(lc_table['Problems'], dtype=np.int64), -1)
        good = (
            np.isfinite(mjds) & np.isfinite(mags) & np.isfinite(errors)
            & ((problems & ~OMC_ALLOWED_PROBLEMS) == 0)
            & (mags > OMC_MIN_MAG) & (mags < OMC_MAX_MAG)
            & (errors > 0) & (errors <= OMC_MAX_MAG_ERROR)
        )

        omc_id = _omc_id(match['title'])
        output = []
        for mjd, mag, error, flags in zip(mjds[good], mags[good], errors[good], problems[good]):
            mjd = float(mjd)
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {
                    'filter': OMC_FILTER,
                    'magnitude': float(mag),
                    'error': float(error),
                    'mjd': mjd,
                    'omc_id': omc_id,
                    'omc_problems': int(flags),
                    'match_separation_arcsec': round(match['separation_arcsec'], 4),
                },
            })
        return output
