"""NOIRLab Source Catalog DR2 per-exposure photometry from Astro Data Lab.

NSC DR2 re-measured every public DECam, Mosaic3 and 90Prime exposure up to ~2019, so
it is the public route to single-epoch Dark Energy Survey photometry: the DES DR2
tables themselves only hold coadded magnitudes. ``nsc_dr2.meas`` rows are not tagged
with a proposal ID, so the light curve mixes DES with every other public program on
the same instruments; it is labelled NSC rather than DES for that reason.

Magnitudes are NSC's calibrated MAG_AUTO. Only measurements with a known passband, a
physical magnitude, an error in (0, NSC_MAX_MAG_ERROR] and no saturation/truncation
SExtractor flag are imported.
"""

import logging
import math
from datetime import timezone
from io import StringIO
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
import requests
from astropy.time import Time

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target

from custom_code.data_services.forms import NSCQueryForm
from custom_code.data_services.service_utils import (
    DATA_SERVICE_HTTP_TIMEOUT,
    resolve_query_coordinates,
    upsert_reduced_datums,
)

logger = logging.getLogger(__name__)

NSC_PAGE_URL = 'https://datalab.noirlab.edu/data/nsc'
NSC_QUERY_URL = 'https://datalab.noirlab.edu/query/query'
NSC_ANON_TOKEN = 'anonymous.0.0.anon_access'
NSC_RELEASE = 'NSC DR2'
# NSC objects are PSF-sized detections in crowded deep fields; a wider cone picks neighbours.
NSC_DEFAULT_RADIUS_ARCSEC = 1.5

# NSC passband -> BHTOM filter name. Rows in any other passband are dropped.
NSC_FILTERS = {
    'u': 'NSC(u)',
    'g': 'NSC(g)',
    'r': 'NSC(r)',
    'i': 'NSC(i)',
    'z': 'NSC(z)',
    'Y': 'NSC(Y)',
    'VR': 'NSC(VR)',
}
# NSC writes 99.99 for failed photometry; anything outside this range is not a measurement.
NSC_MIN_MAG = 0.0
NSC_MAX_MAG = 30.0
NSC_MAX_MAG_ERROR = 2.5
# SExtractor FLAGS bits 1 (neighbours) and 2 (deblended) are harmless; 4 and above mean
# saturated, truncated at the CCD edge or corrupted aperture/isophotal data.
NSC_MAX_SEXTRACTOR_FLAGS = 3

NSC_ACKNOWLEDGEMENT = (
    'This research uses services or data provided by the Astro Data Lab, which is part of '
    'the Community Science and Data Center (CSDC) Program of NSF NOIRLab, and the NOIRLab '
    'Source Catalog DR2 (Nidever et al. 2021, AJ 161, 192).'
)


def _to_float(value):
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def _build_nsc_object_query(ra, dec, radius_arcsec):
    return f"""
    SELECT O.id, O.ra, O.dec, O.ndet,
           q3c_dist(O.ra, O.dec, {ra}, {dec}) * 3600 AS dist_arcsec
    FROM nsc_dr2.object AS O
    WHERE q3c_radial_query(O.ra, O.dec, {ra}, {dec}, {radius_arcsec / 3600.0})
    ORDER BY dist_arcsec
    """


def _build_nsc_photometry_query(object_id):
    escaped = str(object_id).replace("'", "''")
    return f"""
    SELECT M.measid, M.mjd, M.filter, M.mag_auto, M.magerr_auto, M.flags, M.exposure,
           E.instrument
    FROM nsc_dr2.meas AS M
    JOIN nsc_dr2.exposure AS E ON E.exposure = M.exposure
    WHERE M.objectid = '{escaped}'
    ORDER BY M.mjd
    """


def _datalab_query(sql):
    """Run an anonymous Astro Data Lab SQL query and return the result as a DataFrame."""
    url = f'{NSC_QUERY_URL}?sql={quote_plus(sql)}&ofmt=csv&out=None&async=False&drop=False&&profile=default'
    response = requests.get(
        url,
        headers={
            'Content-Type': 'text/ascii',
            'X-DL-TimeoutRequest': '300',
            'X-DL-AuthToken': NSC_ANON_TOKEN,
        },
        timeout=DATA_SERVICE_HTTP_TIMEOUT,
    )
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text), dtype={'id': str, 'measid': str, 'exposure': str})


def _good_measurements(photometry):
    """Rows in a known passband with a physical magnitude, 0 < error <= 2.5 and clean flags."""
    if photometry is None or photometry.empty:
        return photometry
    numeric = photometry[['mjd', 'mag_auto', 'magerr_auto', 'flags']].apply(pd.to_numeric, errors='coerce')
    finite = np.isfinite(numeric).all(axis=1)
    known_filter = photometry['filter'].astype(str).str.strip().isin(NSC_FILTERS)
    mag_ok = (numeric['mag_auto'] > NSC_MIN_MAG) & (numeric['mag_auto'] < NSC_MAX_MAG)
    err_ok = (numeric['magerr_auto'] > 0) & (numeric['magerr_auto'] <= NSC_MAX_MAG_ERROR)
    flags_ok = numeric['flags'] <= NSC_MAX_SEXTRACTOR_FLAGS
    return photometry[finite & known_filter & mag_ok & err_ok & flags_ok]


class NSCDataService(DataService):
    name = 'NSC'
    verbose_name = 'NOIRLab Source Catalog DR2 (DECam/DES)'
    # DR2 is a frozen release; nothing new appears between refreshes.
    update_on_daily_refresh = False
    info_url = NSC_PAGE_URL
    acknowledgement = NSC_ACKNOWLEDGEMENT
    upsert_identity_keys = ('measid', 'filter')
    service_notes = (
        'Query NOIRLab Source Catalog DR2 single-exposure photometry (DECam incl. DES, '
        'Mosaic3, 90Prime) from Astro Data Lab by coordinates. The nearest NSC object is '
        'used; points with magnitude errors outside (0, 2.5] mag or saturation/truncation '
        'flags are rejected. Photometry only; no aliases are added.'
    )

    @classmethod
    def get_form_class(cls):
        return NSCQueryForm

    @classmethod
    def get_acknowledgement(cls):
        return cls.acknowledgement

    def build_query_parameters(self, parameters, **kwargs):
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or NSC_DEFAULT_RADIUS_ARCSEC,
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or NSC_DEFAULT_RADIUS_ARCSEC

        match = None
        lc_data = None
        if ra is not None and dec is not None and query_parameters.get('include_photometry', True):
            try:
                objects = _datalab_query(_build_nsc_object_query(ra, dec, radius_arcsec))
                if len(objects) > 0:
                    # Only the nearest NSC object is the target; its neighbours are other sources.
                    nearest = objects.iloc[0]
                    match = {
                        'id': str(nearest['id']),
                        'ra': _to_float(nearest['ra']),
                        'dec': _to_float(nearest['dec']),
                        'separation_arcsec': _to_float(nearest['dist_arcsec']),
                    }
                    lc_data = _datalab_query(_build_nsc_photometry_query(match['id']))
                else:
                    logger.debug('NSC returned no object for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('NSC query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                match, lc_data = None, None

        self.query_results = {
            'match': match,
            'lc_data': lc_data,
            'source_location': NSC_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        match = data.get('match')
        lc_data = data.get('lc_data')
        if data.get('ra') is None or data.get('dec') is None or not match or lc_data is None or lc_data.empty:
            return []

        datums = self._build_photometry_datums(lc_data, match)
        if not datums:
            return []

        return [{
            'name': f"NSC_{match['id']}",
            'ra': data['ra'],
            'dec': data['dec'],
            # Photometry only: NSC object ids are not names anyone searches for.
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

    def _build_photometry_datums(self, lc_data, match):
        output = []
        for _, row in _good_measurements(lc_data).iterrows():
            mjd = float(row['mjd'])
            value = {
                'filter': NSC_FILTERS[str(row['filter']).strip()],
                'magnitude': float(row['mag_auto']),
                'error': float(row['magerr_auto']),
                'measid': str(row['measid']),
                'exposure': str(row['exposure']),
                'instrument': str(row.get('instrument') or ''),
                'flags': int(row['flags']),
                'mjd': mjd,
                'nsc_object_id': match['id'],
                'data_release': NSC_RELEASE,
            }
            if match.get('separation_arcsec') is not None:
                value['match_separation_arcsec'] = round(match['separation_arcsec'], 4)
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': value,
            })
        return output
