import logging

import requests
from astropy.time import Time
from datetime import timezone

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import SwiftUVOTQueryForm


logger = logging.getLogger(__name__)

SWIFTUVOT_START_URL = 'http://193.0.88.218:8892/api/start'
SWIFTUVOT_RESULT_URL = 'http://193.0.88.218:8892/api/result'

def _swift_alias(ra, dec):
    return f'SWIFT+J{ra}_{dec}'


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

class SwiftUVOTDataService(DataService):
    name = 'SwiftUVOT'
    verbose_name = 'SwiftUVOT'
    update_on_daily_refresh = True
    info_url = SWIFTUVOT_START_URL
    service_notes = 'Query Swift UVOT by coordinates through in house Swift service.'

    @classmethod
    def get_form_class(cls):
        return SwiftUVOTQueryForm

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
        swiift_data = None
        if ra is None or dec is None:
            self.query_results = {'photometry_data': [], 'source_location': None}
            return self.query_results

        try:
            src_data = {"ra": ra, "dec": dec}
            requests.post(SWIFTUVOT_START_URL, json=src_data)
            swift_response = requests.get(SWIFTUVOT_RESULT_URL, params=src_data)
            swiift_data = swift_response.json()

            if len(swiift_data) == 0:
                logger.debug('Swift UVOT returned no photometry for RA=%s Dec=%s', ra, dec)

        except Exception as e:
            logger.debug('SkyMapper error %s', e)
        
        self.query_results = {
            'photometry_data': swiift_data,
            'source_location': SWIFTUVOT_RESULT_URL,
            'ra': ra,
            'dec': dec,
        }

        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        ra = data.get('ra')
        dec = data.get('dec')
        photometry_data = data.get('photometry_data')
        if ra is None or dec is None or photometry_data is None:
            return []

        alias = _swift_alias(ra,dec)
        return [{
            'name': alias,
            'ra': ra,
            'dec': dec,
            'aliases': [alias],
            'reduced_datums': {'photometry': self._build_photometry_datums(photometry_data)},
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

    def _build_photometry_datums(self, rows):
        output = []
        for row in rows:
            mjd = _to_float(row["obs_time"])
            mag = _to_float(row["mag"])
            magerr = _to_float(row["mag_err"])
            fil = row["filter"]
            band = f"UVOT({fil})"
            if mjd is None or mag is None or magerr is None:
                continue
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {'filter': band, 'magnitude': mag, 'error': magerr},
            })
        return output
