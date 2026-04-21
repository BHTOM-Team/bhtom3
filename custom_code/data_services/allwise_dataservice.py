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

from custom_code.data_services.forms import WISEQueryForm
from custom_code.data_services.wise_alias_utils import fetch_allwise_alias


logger = logging.getLogger(__name__)

WISE_QUERY_URL = 'https://irsa.ipac.caltech.edu/cgi-bin/Gator/nph-scan?submit=Select&projshort=WISE'


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _wise_target_name(ra, dec):
    return f'WISE+J{ra}_{dec}'


def _fallback_alias(ra, dec):
    return f'AllWISE+J{ra}_{dec}'


def _build_wise_query(ra,dec,rad):
    ra = str(ra)
    dec = str(dec)
    rad = str(rad)
    return "https://irsa.ipac.caltech.edu/cgi-bin/Gator/nph-query?catalog=allwise_p3as_mep&spatial=cone&radius="+rad+ \
                "&radunits=arcsec&objstr=" + ra + "+" + dec + "&outfmt=1&selcols=ra,dec,mjd,w1mpro_ep," \
                                                                      "w1sigmpro_ep,w2mpro_ep,w2sigmpro_ep"



class AllWISEDataService(DataService):
    name = 'AllWISE'
    verbose_name = 'AllWISE'
    update_on_daily_refresh = False
    info_url = WISE_QUERY_URL
    service_notes = 'Query AllWISE by coordinates and ingest AllWISE photometry.'

    @classmethod
    def get_form_class(cls):
        return WISEQueryForm

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
        if ra is None or dec is None:
            self.query_results = {'lc_data': [], 'alias': None, 'source_location': None}
            return self.query_results

        lc_data = None
        alias = None
        source_location = "https://irsa.ipac.caltech.edu/cgi-bin/Gator/nph-scan?submit=Select&projshort=WISE"
        try:
            wise_response = requests.get(_build_wise_query(ra,dec,radius_arcsec))
            if wise_response.text.strip():
                res_tab = wise_response.text.split("null|\n", 1)[1]
                lc_data = pd.read_csv(
                StringIO(res_tab),
                header=None,
                names=[
                    'ra', 'dec', 'clon', 'clat', 'mjd', 'w1mpro', 'w1sigmpro',
                    'w2mpro', 'w2sigmpro', 'dist', 'angle'
                ],
                sep=r'\s+'
            )
                alias = fetch_allwise_alias(ra, dec, radius_arcsec)
            else:
                logger.debug('ALLWISE returned no data for RA=%s Dec=%s', ra, dec)
        except (IndexError, ValueError, requests.RequestException):
            logger.debug('ALLWISE returned error for RA=%s Dec=%s', ra, dec)

        self.query_results = {
            'lc_data': lc_data,
            'alias': alias,
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
        if ra is None or dec is None or lc_data is None or len(lc_data) < 1:
            return []

        target_name = _wise_target_name(ra, dec)
        alias_name = data.get('alias') or _fallback_alias(ra, dec)

        return [{
            'name': target_name,
            'ra': ra,
            'dec': dec,
            'aliases': [{'name': alias_name, 'source_name': self.name}],
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
        aliases = []
        for alias in alias_results:
            alias_name = alias.get('name') if isinstance(alias, dict) else alias
            alias_name = str(alias_name or '').strip()
            if alias_name:
                aliases.append(TargetName(name=alias_name))
        return aliases

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
            if not np.isnan(row.w1mpro) and not np.isnan(row.w1sigmpro):
                output.append({
                    'timestamp': Time(row.mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                    'value': {'filter': "WISE(W1)", 'magnitude': row.w1mpro, 'error': row.w1sigmpro},
                })
            if not np.isnan(row.w2mpro) and not np.isnan(row.w2sigmpro):
                output.append({
                    'timestamp': Time(row.mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                    'value': {'filter': "WISE(W2)", 'magnitude': row.w2mpro, 'error': row.w2sigmpro},
                })

        return output
