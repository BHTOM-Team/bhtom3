import logging
import math
from datetime import timezone
from io import StringIO
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
import requests
from astropy.time import Time

from tom_dataproducts.models import ReducedDatum
from tom_dataservices.dataservices import DataService
from tom_targets.models import Target

from custom_code.data_services.forms import PGIRQueryForm
from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT

logger = logging.getLogger(__name__)

PGIR_PAGE_URL = 'https://datalab.noirlab.edu/data/pgir'
PGIR_QUERY_URL = 'https://datalab.noirlab.edu/query/query'
PGIR_ANON_TOKEN = 'anonymous.0.0.anon_access'
PGIR_DEFAULT_RADIUS_ARCSEC = 5.0
# Same cut as the Data Lab PGIR DR1 notebook: both error columns must give SNR above this.
PGIR_MIN_SNR = 10.0
PGIR_FILTER = 'PGIR(J)'


def _to_float(value):
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def _build_pgir_source_query(ra, dec, radius_arcsec):
    return f"""
    SELECT S.pts_key, S.tmcra, S.tmcdec,
           q3c_dist(S.tmcra, S.tmcdec, {ra}, {dec}) * 3600 AS dist_arcsec
    FROM pgir_dr1.sources AS S
    WHERE q3c_radial_query(S.tmcra, S.tmcdec, {ra}, {dec}, {radius_arcsec / 3600.0})
    ORDER BY dist_arcsec
    """


def _build_pgir_photometry_query(pts_key):
    return f"""
    SELECT P.obsjd, P.magpsf, P.magpsferr, P.magpsfstaterr, P.flags
    FROM pgir_dr1.photometry AS P
    WHERE P.pts_key = {int(pts_key)}
    ORDER BY P.obsjd
    """


def _datalab_query(sql):
    """Run an anonymous Astro Data Lab SQL query and return the result as a DataFrame."""
    url = (
        f'{PGIR_QUERY_URL}?sql={quote_plus(sql)}&ofmt=csv&out=None&async=False&drop=False&&profile=default'
    )
    response = requests.get(
        url,
        headers={
            'Content-Type': 'text/ascii',
            'X-DL-TimeoutRequest': '300',
            'X-DL-AuthToken': PGIR_ANON_TOKEN,
        },
        timeout=DATA_SERVICE_HTTP_TIMEOUT,
    )
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))


def _good_detections(photometry):
    """Rows with a finite magnitude and both error estimates positive with SNR >= PGIR_MIN_SNR."""
    columns = photometry[['obsjd', 'magpsf', 'magpsferr', 'magpsfstaterr']].apply(pd.to_numeric, errors='coerce')
    finite = np.isfinite(columns).all(axis=1)
    positive_err = (columns['magpsferr'] > 0) & (columns['magpsfstaterr'] > 0)
    with np.errstate(divide='ignore', invalid='ignore'):
        snr_ok = (1 / columns['magpsferr'] >= PGIR_MIN_SNR) & (1 / columns['magpsfstaterr'] >= PGIR_MIN_SNR)
    return photometry[finite & positive_err & snr_ok]


class PGIRDataService(DataService):
    name = 'PGIR'
    verbose_name = 'PGIR'
    update_on_daily_refresh = False
    info_url = PGIR_PAGE_URL
    service_notes = (
        'Query Palomar Gattini-IR DR1 J-band light curves from Astro Data Lab by coordinates. '
        'Photometry only; no aliases are added.'
    )

    @classmethod
    def get_form_class(cls):
        return PGIRQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or PGIR_DEFAULT_RADIUS_ARCSEC,
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or PGIR_DEFAULT_RADIUS_ARCSEC

        pts_key = None
        lc_data = None
        if ra is not None and dec is not None and query_parameters.get('include_photometry', True):
            try:
                sources = _datalab_query(_build_pgir_source_query(ra, dec, radius_arcsec))
                if len(sources) > 0:
                    # Only the nearest 2MASS source is the target; its neighbours are blends.
                    pts_key = int(sources['pts_key'].iloc[0])
                    lc_data = _datalab_query(_build_pgir_photometry_query(pts_key))
                else:
                    logger.debug('PGIR returned no source for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('PGIR query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                pts_key, lc_data = None, None

        self.query_results = {
            'pts_key': pts_key,
            'lc_data': lc_data,
            'source_location': PGIR_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        lc_data = data.get('lc_data')
        if data.get('ra') is None or data.get('dec') is None or lc_data is None or lc_data.empty:
            return []

        datums = self._build_photometry_datums(lc_data)
        if not datums:
            return []

        return [{
            'name': f"PGIR_{data['pts_key']}",
            'ra': data['ra'],
            'dec': data['dec'],
            # Photometry only: PGIR/2MASS ids must not be added to BHTOM targets as aliases.
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

    def _build_photometry_datums(self, lc_data):
        output = []
        for _, row in _good_detections(lc_data).iterrows():
            output.append({
                'timestamp': Time(float(row['obsjd']), format='jd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {
                    'filter': PGIR_FILTER,
                    'magnitude': float(row['magpsf']),
                    'error': float(row['magpsferr']),
                    'flags': int(row['flags']),
                },
            })
        return output
