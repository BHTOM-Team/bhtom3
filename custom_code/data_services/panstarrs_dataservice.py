import logging

from astropy.time import Time
from datetime import timezone
import numpy as np
import requests
from io import StringIO
import pandas as pd

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import PanSTARRSQueryForm


logger = logging.getLogger(__name__)

PS1_QUERY_URL = 'https://catalogs.mast.stsci.edu/panstarrs'


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def _ps1_cat_url(id):
    return f"{PS1_QUERY_URL}/detections.html?objID={id}"


def _ps1_alias(id):
    return f'PS1_{id}'

def _get_ps1_filter_name(filter_id):
    filter_map = {
        1: 'g',
        2: 'r',
        3: 'i',
        4: 'z',
        5: 'y'
    }
    return filter_map.get(filter_id, None)


class PanSTARRSDataService(DataService):
    name = 'PS1'
    verbose_name = 'PS1'
    update_on_daily_refresh = False
    info_url = PS1_QUERY_URL
    service_notes = 'Query Pan-STARRS by coordinates and ingest Pan-STARRS photometry.'

    @classmethod
    def get_form_class(cls):
        return PanSTARRSQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        self.query_parameters = {
            'ra': parameters.get('ra'),
            'dec': parameters.get('dec'),
            'radius_arcsec': parameters.get('radius_arcsec') or 5.0,
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or 5.0
        if ra is None or dec is None:
            self.query_results = {'lc_data': [], 'source_location': None}
            return self.query_results

        ps1_id = None
        lc_data = None
        source_location = None
        try:
            ps1_response = requests.get(f"https://catalogs.mast.stsci.edu/api/v0.1/panstarrs/dr2/detection.csv?ra={ra}&dec={dec}&radius={radius_arcsec/3600.0}")
            if ps1_response.text.strip():
                lc_data = pd.read_csv(StringIO(ps1_response.text))
                ps1_id = lc_data['objID'][0]
                source_location = _ps1_cat_url(ps1_id)
            else:
                logger.info('PanSTARRS returned no data for RA=%s Dec=%s', ra, dec)
        except ValueError:
            logger.info('PanSTARRS returned error for RA=%s Dec=%s', ra, dec)

        self.query_results = {
            'ps1_id':ps1_id,
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
        if ra is None or dec is None or len(lc_data) < 1:
            return []

        alias = _ps1_alias(data.get('ps1_id'))
        lc_data = lc_data[lc_data['psfFlux']>0]
        lc_data = lc_data[lc_data['psfFluxErr']>0]
        return [{
            'name': alias,
            'ra': ra,
            'dec': dec,
            'aliases': [alias],
            'reduced_datums': {'photometry': self._build_photometry_datums(lc_data)},
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

    def _build_photometry_datums(self, lc_data):
        output = []
        for _, row in lc_data.iterrows():
            mjd = row['obsTime']
            psfFlux = row['psfFlux']
            psfFluxErr = row['psfFluxErr']
            filterNo = row['filterID']
            filter = f"PS1({_get_ps1_filter_name(filterNo)})"
            if mjd is None or psfFlux is None or psfFluxErr is None or filterNo is None:
                continue
            snr = psfFlux/psfFluxErr
            if (snr) > 3:
                mag = -2.5 * np.log10(psfFlux / 3631)
                magerr = 1.0857 * (psfFluxErr / psfFlux)
                output.append({
                    'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                    'value': {'filter': filter, 'magnitude': mag, 'error': magerr},
                })
            else:
                mag = -2.5 * np.log10((psfFlux) * snr / 3631)
                magerr = -1
                output.append({
                    'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                    'value': {'filter': filter, 'magnitude': mag, 'error': magerr},
                })

        return output
