import logging

from astropy.time import Time
from datetime import timezone

import pandas as pd
from io import StringIO
import requests

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import ZTFQueryForm


logger = logging.getLogger(__name__)

ALERCE_PAGE = "https://alerce.online/"

def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def _getAlerceObjcet(ra,dec,rad):
  url=f"https://api.alerce.online/ztf/v1/objects/?ra={ra}&dec={dec}&radius={rad}&page=1&page_size=20&count=true"
  headers = {"accept": "application/json"}
  response = requests.get(url, headers=headers)
  return response.json()

def _getAlerceLightCurve(oid):
  url=f"https://api.alerce.online/ztf/v1/objects/{oid}/lightcurve"
  headers = {"accept": "application/json"}
  response = requests.get(url, headers=headers)
  return response.json()

def _get_filter(value):
    mapping = {
        1: "zg",
        2: "zr",
        3: "zi"
    }
    return mapping.get(value)

class AlerceDataService(DataService):
    name = 'Alerce'
    verbose_name = 'Alerce'
    update_on_daily_refresh = True
    info_url = ALERCE_PAGE
    service_notes = 'Query ZTF by coordinates and ingest ZTF photometry through Alerce.'

    @classmethod
    def get_form_class(cls):
        return ZTFQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or 1.1,
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or 1.1
        if ra is None or dec is None:
            self.query_results = {'lc_data': [], 'source_location': None}
            return self.query_results

        lc_data = None
        source_location = "https://alerce.online/"
        try:
            objcet_data = _getAlerceObjcet(ra,dec,radius_arcsec)
            if objcet_data['total']>0:
                oid = objcet_data['items'][0]['oid']
                ztf_data = _getAlerceLightCurve(oid)
                lc_data = ztf_data['detections']
                source_location = f"https://alerce.online/object/{oid}"
            else:
                logger.debug('Alerce returned no data for RA=%s Dec=%s', ra, dec)
        except ValueError:
            logger.debug('Alerce returned error for RA=%s Dec=%s', ra, dec)

        self.query_results = {
            'lc_data': lc_data,
            'source_location': source_location,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        ra = data.get('ra')
        dec = data.get('dec')
        lc_data = data.get('lc_data')
        if ra is None or dec is None or lc_data is None:
            return []

        return [{
            'name': None,
            'ra': ra,
            'dec': dec,
            'aliases': [None],
            'reduced_datums': {'photometry': self._build_photometry_datums(lc_data)},
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

    def _build_photometry_datums(self, lc_data):
        output = []
        for datum in lc_data:
            mag = datum['magpsf_corr']
            mag_err = datum['sigmapsf_corr']
            if mag is None or mag_err is None or not mag or not mag_err:
                continue
            mjd = datum['mjd']
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {'filter': f"ZTF({_get_filter(datum['fid'])})", 'magnitude': mag, 'error': mag_err},
                })
        return output
