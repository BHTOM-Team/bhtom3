import logging
from datetime import timezone
from io import StringIO

import numpy as np
import pandas as pd
import requests
from astropy.time import Time

from tom_dataproducts.models import ReducedDatum
from tom_dataservices.dataservices import DataService
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import WISEQueryForm


logger = logging.getLogger(__name__)

TWOMASS_QUERY_URL = 'https://irsa.ipac.caltech.edu/cgi-bin/Gator/nph-scan?submit=Select&projshort=2MASS'


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _twomass_alias(designation):
    return f'2MASS_{designation}'


def _build_twomass_query(ra, dec, radius_arcsec):
    return (
        'https://irsa.ipac.caltech.edu/cgi-bin/Gator/nph-query?catalog=fp_psc'
        f'&spatial=cone&radius={radius_arcsec}&radunits=arcsec&objstr={ra}+{dec}'
        '&outfmt=1&selcols=ra,dec,designation,j_m,j_cmsig,h_m,h_cmsig,k_m,k_cmsig'
    )


class TwoMASSDataService(DataService):
    name = '2MASS'
    verbose_name = '2MASS'
    update_on_daily_refresh = False
    info_url = TWOMASS_QUERY_URL
    service_notes = 'Query 2MASS point sources by coordinates and ingest J/H/K photometry.'

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
            'radius_arcsec': parameters.get('radius_arcsec') or 3.0,
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or 3.0
        if ra is None or dec is None:
            self.query_results = {'lc_data': None, 'designation': None, 'source_location': None}
            return self.query_results

        lc_data = None
        designation = None
        source_location = _build_twomass_query(ra, dec, radius_arcsec)
        try:
            response = requests.get(source_location)
            if response.text.strip():
                response_table = response.text.split('null|\n', 1)[1]
                lc_data = pd.read_csv(
                    StringIO(response_table),
                    header=None,
                    names=[
                        'ra', 'dec', 'clon', 'clat', 'designation', 'j_m', 'j_cmsig',
                        'h_m', 'h_cmsig', 'k_m', 'k_cmsig', 'dist', 'angle',
                    ],
                    sep=r'\s+',
                )
                if len(lc_data) > 0:
                    designation = str(lc_data.iloc[0]['designation']).strip()
            else:
                logger.debug('2MASS returned no data for RA=%s Dec=%s', ra, dec)
        except (IndexError, ValueError, requests.RequestException):
            logger.debug('2MASS returned unparsable data for RA=%s Dec=%s', ra, dec)

        self.query_results = {
            'lc_data': lc_data,
            'designation': designation,
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
        designation = data.get('designation')
        if ra is None or dec is None or lc_data is None or len(lc_data) < 1 or not designation:
            return []

        alias = _twomass_alias(designation)
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
        timestamp = Time('2000-01-01T00:00:00', format='isot', scale='utc').to_datetime(timezone=timezone.utc)
        for _, row in lc_data.iterrows():
            if not np.isnan(row.j_m) and not np.isnan(row.j_cmsig):
                output.append({
                    'timestamp': timestamp,
                    'value': {'filter': '2MASS(J)', 'magnitude': row.j_m, 'error': row.j_cmsig},
                })
            if not np.isnan(row.h_m) and not np.isnan(row.h_cmsig):
                output.append({
                    'timestamp': timestamp,
                    'value': {'filter': '2MASS(H)', 'magnitude': row.h_m, 'error': row.h_cmsig},
                })
            if not np.isnan(row.k_m) and not np.isnan(row.k_cmsig):
                output.append({
                    'timestamp': timestamp,
                    'value': {'filter': '2MASS(K)', 'magnitude': row.k_m, 'error': row.k_cmsig},
                })
        return output
