import logging
from datetime import timezone

import numpy as np
import requests
from astropy.time import Time

from tom_dataproducts.models import ReducedDatum
from tom_dataservices.dataservices import DataService
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import FAVAQueryForm
from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT


logger = logging.getLogger(__name__)

FAVA_API_URL = "https://fermi.gsfc.nasa.gov/ssc/data/access/lat/FAVA/queryDB_Lightcurve.php"
FAVA_PAGE = "https://fermi.gsfc.nasa.gov/ssc/data/access/lat/FAVA/"


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class FAVADataService(DataService):
    name = 'FAVA'
    verbose_name = 'FAVA (Fermi-LAT)'
    update_on_daily_refresh = True
    info_url = FAVA_PAGE
    service_notes = 'Query FAVA (Fermi All-sky Variability Analysis) by coordinates and ingest high-energy light curves.'

    @classmethod
    def get_form_class(cls):
        return FAVAQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        if ra is None or dec is None:
            self.query_results = {'lc_data': None, 'source_location': None}
            return self.query_results

        lc_data = None
        source_location = f"{FAVA_PAGE}LightCurve.php?ra={ra}&dec={dec}"
        try:
            response = requests.get(
                FAVA_API_URL,
                params={'ra': ra, 'dec': dec},
                timeout=DATA_SERVICE_HTTP_TIMEOUT,
            )
            data = response.json()
            if data:
                lc_data = data
            else:
                logger.debug('FAVA returned no data for RA=%s Dec=%s', ra, dec)
        except (ValueError, requests.RequestException) as e:
            logger.debug('FAVA returned error for RA=%s Dec=%s: %s', ra, dec, e)

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
        if ra is None or dec is None or not lc_data:
            return []

        return [{
            'name': None,
            'ra': ra,
            'dec': dec,
            'aliases': [],
            'reduced_datums': {'highenergy': self._build_highenergy_datums(lc_data, ra, dec)},
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
        if data_type != 'highenergy' or not data:
            return
        source_location = kwargs.get('source_location') or self.info_url
        for datum in data:
            ReducedDatum.objects.get_or_create(
                target=target,
                data_type='highenergy',
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

    def _build_highenergy_datums(self, lc_data, ra, dec):
        output = []
        source_url = f"{FAVA_PAGE}LightCurve.php?ra={ra}&dec={dec}"

        for lc_point in lc_data:
            met = _to_float(lc_point.get('time'))
            nev = _to_float(lc_point.get('nev'))
            avnev = _to_float(lc_point.get('avnev'))
            if met is None or nev is None or avnev is None or avnev == 0:
                continue

            mjd = 51910.0 + met / 86400.0
            timestamp = Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc)
            flux = (nev - avnev) / avnev
            flux_err = np.sqrt(nev) / avnev

            output.append({
                'timestamp': timestamp,
                'value': {
                    'filter': 'LAT(>100MeV)',
                    'flux': flux,
                    'error': flux_err,
                    'facility': 'FERMI-LAT',
                    'observer': 'FERMI-LAT',
                },
            })

        for lc_point in lc_data:
            met = _to_float(lc_point.get('time'))
            he_nev = _to_float(lc_point.get('he_nev'))
            he_avnev = _to_float(lc_point.get('he_avnev'))
            if met is None or he_nev is None or he_avnev is None or he_avnev == 0:
                continue

            mjd = 51910.0 + met / 86400.0
            timestamp = Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc)
            flux = (he_nev - he_avnev) / he_avnev
            flux_err = np.sqrt(he_nev) / he_avnev

            output.append({
                'timestamp': timestamp,
                'value': {
                    'filter': 'LAT(>800MeV)',
                    'flux': flux,
                    'error': flux_err,
                    'facility': 'FERMI-LAT',
                    'observer': 'FERMI-LAT',
                },
            })

        return output
