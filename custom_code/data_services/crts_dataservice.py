import logging
from io import StringIO

import pandas as pd
import requests
from astropy.time import Time
from datetime import timezone

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import CRTSQueryForm


logger = logging.getLogger(__name__)

CRTS_QUERY_URL = 'http://nunuku.caltech.edu/cgi-bin/getcssconedb_release_img.cgi'
CRTS_REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _crts_alias(ra, dec):
    return f'CRTS+J{ra}_{dec}'


def _build_crts_url(ra, dec, radius_arcmin):
    return (
        f'{CRTS_QUERY_URL}?RADec={ra}+{dec}'
        f'&Rad={radius_arcmin}&DB=photcat&OUT=web&SHORT=short'
    )


class CRTSDataService(DataService):
    name = 'CRTS'
    verbose_name = 'CRTS'
    update_on_daily_refresh = False
    info_url = CRTS_QUERY_URL
    service_notes = 'Query CRTS by coordinates and ingest Catalina photometry.'

    @classmethod
    def get_form_class(cls):
        return CRTSQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        self.query_parameters = {
            'ra': parameters.get('ra'),
            'dec': parameters.get('dec'),
            'radius_arcmin': parameters.get('radius_arcmin') or 0.1,
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcmin = _to_float(query_parameters.get('radius_arcmin')) or 0.1
        if ra is None or dec is None:
            self.query_results = {'match': None, 'photometry_rows': [], 'source_location': None}
            return self.query_results

        source_location = _build_crts_url(ra, dec, radius_arcmin)
        response = requests.get(source_location, timeout=30, headers=CRTS_REQUEST_HEADERS)
        response.raise_for_status()

        match_row = None
        photometry_rows = []
        try:
            tables = pd.read_html(StringIO(response.text), match='Photometry of Objs')
            if tables:
                df = tables[0]
                rows = df.to_dict(orient='records')
                if rows:
                    match_row = rows[0]
                    if query_parameters.get('include_photometry', True):
                        photometry_rows = rows
        except ValueError:
            logger.debug('CRTS returned no photometry for RA=%s Dec=%s', ra, dec)

        self.query_results = {
            'match': match_row,
            'photometry_rows': photometry_rows,
            'source_location': source_location,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        ra = data.get('ra')
        dec = data.get('dec')
        photometry_rows = data.get('photometry_rows') or []
        if ra is None or dec is None or not data.get('match'):
            return []

        alias = _crts_alias(ra, dec)
        return [{
            'name': alias,
            'ra': ra,
            'dec': dec,
            'aliases': [alias],
            'reduced_datums': {'photometry': self._build_photometry_datums(photometry_rows)},
            'source_location': data.get('source_location'),
        }]

    def create_target_from_query(self, target_result, **kwargs):
        return Target(
            name=target_result['name'],
            type='SIDEREAL',
            ra=target_result.get('ra'),
            dec=target_result.get('dec'),
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

    def _build_photometry_datums(self, rows):
        output = []
        for row in rows:
            mjd = _to_float(row.get('MJD'))
            mag = _to_float(row.get('Mag'))
            magerr = _to_float(row.get('Magerr'))
            if mjd is None or mag is None or magerr is None:
                continue
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {'filter': 'CRTS(CL)', 'magnitude': mag, 'error': magerr},
            })
        return output
