import logging

from astropy.time import Time
from datetime import timezone

from specutils import Spectrum1D
import numpy as np

import astropy.units as u
from urllib.parse import quote_plus
import requests
import pandas as pd
from io import StringIO

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from tom_dataproducts.processors.data_serializers import SpectrumSerializer
from custom_code.data_services.forms import DESIQueryForm



logger = logging.getLogger(__name__)

DESI_PAGE_URL = 'https://data.desi.lbl.gov/doc/releases/dr1/'


def _desi_source_location(id):
    return f"https://www.legacysurvey.org/viewer/desi-spectrum/dr1/targetid{id}"
def _build_desi_query(ra,dec,rad):
    return quote_plus(f"""
    SELECT *
    FROM desi_dr1.zpix
    WHERE q3c_radial_query(
    mean_fiber_ra, mean_fiber_dec,
    {ra}, {dec},
    {rad/(60*60)} );
    """)


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class DESIDataService(DataService):
    name = 'DESI'
    verbose_name = 'DESI'
    info_url = DESI_PAGE_URL
    service_notes = 'Query DESI spectra by coordinates.'

    @classmethod
    def get_form_class(cls):
        return DESIQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        self.query_parameters = {
            'ra': parameters.get('ra'),
            'dec': parameters.get('dec'),
            'radius_arcsec': parameters.get('radius_arcsec') or 5.0,
            'include_spectroscopy': bool(parameters.get('include_spectroscopy', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or 5.0

        cat_name = None
        spec_id = None
        time = None
        spectra_data = None
        

        if ra is None or dec is None:
            self.query_results = {'spectroscopy_data': [], 'source_location': None}
            return self.query_results

        try:
            query = _build_desi_query(ra,dec,radius_arcsec)
            qfmt = 'csv'
            async_=False 
            drop=False 
            profile='default'
            ANON_TOKEN	= 'anonymous.0.0.anon_access'
            svc_url = "https://datalab.noirlab.edu/query"
            out=None
            datalab_headers = {'Content-Type': 'text/ascii',
                   'X-DL-TimeoutRequest': str(300),
                   'X-DL-AuthToken': ANON_TOKEN} 
            datalab_url = '%s/query?sql=%s&ofmt=%s&out=%s&async=%s&drop=%s&&profile=%s' % (svc_url, query, qfmt, out, async_, drop,profile)
            datalab_response = requests.get(datalab_url, headers=datalab_headers, stream=True)
            datalab_table = pd.read_csv(StringIO(datalab_response.text))

            if len(datalab_table) == 0:
                logger.info('DESI returned no spectrum for RA=%s Dec=%s', ra, dec)
            else:
                cat_name = datalab_table['desiname'][0]
                spec_id = datalab_table['targetid'][0]
                time = datalab_table['mean_mjd'][0]
                specids = [int(spec_id)]
                find_url = "https://astrosparcl.datalab.noirlab.edu/api/find/?limit=500"
                find_payload = {
                        "outfields": ["sparcl_id", "specid"], 
                        "search": [
                            ["specid"] + specids]}
                find_response = requests.post(find_url, json=find_payload).json()
                sparcl_ids = [item['sparcl_id'] for item in find_response if item.get('_dr') == 'DESI-DR1']
                retrieve_params = {
                    "include": "specid,flux,wavelength", 
                    "format": "json" 
                        }
                retrieve_url = "https://astrosparcl.datalab.noirlab.edu/api/spectras/"
                retrieve_response = requests.post(
                    retrieve_url, 
                    params=retrieve_params, 
                    json=sparcl_ids).json()
                spectra_data = retrieve_response[1]

        except Exception as e:
            logger.info('DESI error', e)
        
        self.query_results = {
            'cat_name':cat_name or None,
            'spec_id':spec_id or None,
            'time':time or None,
            'spectroscopy_data': spectra_data or None,
            'source_location':_desi_source_location(spec_id),
            'ra': ra,
            'dec': dec,
        }

        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        cat_name = data.get('cat_name')
        ra = data.get('ra')
        dec = data.get('dec')
        time = data.get('time')
        spectroscopy_data = data.get('spectroscopy_data')
        if ra is None or dec is None or spectroscopy_data is None:
            return []


        return [{
            'name': cat_name,
            'ra': ra,
            'dec': dec,
            'aliases': [cat_name],
            'reduced_datums': {'spectroscopy': self._build_spectroscopy_datums(spectroscopy_data,time,cat_name)},
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
        if data_type != 'spectroscopy' or not data:
            return
        source_location = kwargs.get('source_location') or self.info_url
        for datum in data:
            ReducedDatum.objects.get_or_create(
                target=target,
                data_type='spectroscopy',
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

    def _build_spectroscopy_datums(self, spec_data, time,cat_name):
        output = []
        serializer = SpectrumSerializer()
        wlength = np.array(spec_data['wavelength'])
        flx = np.array(spec_data['flux'])*1e-17
        spectrum = Spectrum1D(
                flux=flx * u.erg / u.s / u.cm**2 / u.AA,
                spectral_axis=wlength * u.AA,
            )
        serialized = serializer.serialize(spectrum)
        serialized.update({
                'filter': 'DESI-DR1',
                'source_id': str(cat_name),
                'spectrum_type': 'DESI_DR1_spectrum',
            })
        output.append({
                'timestamp': Time(time, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': serialized,
            })

        return output
