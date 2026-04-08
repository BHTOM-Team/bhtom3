import logging

from astropy.time import Time
from datetime import timezone
import numpy as np

from astroquery.ipac.irsa import Irsa
from astropy import coordinates
import astropy.units as u

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import PTFQueryForm


logger = logging.getLogger(__name__)

PTF_PAGE = 'https://www.ptf.caltech.edu/'


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ptf_alias(id):
    return f'PTF_{id}'



class PTFDataService(DataService):
    name = 'PTF'
    verbose_name = 'PTF'
    update_on_daily_refresh = False
    info_url = PTF_PAGE
    service_notes = 'Query PTF by coordinates and ingest PTF photometry.'

    @classmethod
    def get_form_class(cls):
        return PTFQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        self.query_parameters = {
            'ra': parameters.get('ra'),
            'dec': parameters.get('dec'),
            'radius_arcsec': parameters.get('radius_arcsec') or 3.0,
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or 3.0
        if ra is None or dec is None:
            self.query_results = {'lc_data': [], 'source_location': None}
            return self.query_results

        ptf_id = None
        lc_data = None
        source_location = None
        try:
            coord = coordinates.SkyCoord(ra=ra, dec=dec, unit=(u.deg, u.deg), frame='icrs')
            ptf_table = Irsa.query_region(coord, catalog="ptf_lightcurves", spatial="Cone",radius=radius_arcsec * u.arcsec)
            ptf_df = ptf_table.to_pandas()
            ptf_df = ptf_df[ptf_df['mag_autocorr'] >= 0]
            if len(ptf_df)>0:
                lc_data = ptf_df
                ptf_id = ptf_df['oid'][0]
                source_location = "irsa.ipac.caltech.edu/cgi-bin/Gator/nph-scan"
            else:
                logger.debug('PTF returned no data for RA=%s Dec=%s', ra, dec)
        except ValueError:
            logger.debug('PTF returned error for RA=%s Dec=%s', ra, dec)

        self.query_results = {
            'ptf_id':ptf_id,
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

        alias = _ptf_alias(data.get('ptf_id'))
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
        for _, datum in lc_data.iterrows():
            if not np.isnan(datum.mag_autocorr) and not np.isnan(datum.mag_auto) and datum.fid == 1 and datum.goodflag == 1:
                output.append({
                    'timestamp': Time(datum.hmjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                    'value': {'filter': "PTF(g)", 'magnitude': datum.mag_autocorr, 'error': datum.magerr_auto},
                })
            elif not np.isnan(datum.mag_autocorr) and not np.isnan(datum.mag_auto) and datum.fid == 2 and datum.goodflag == 1:
                output.append({
                    'timestamp': Time(datum.hmjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                    'value': {'filter': "PTF(R)", 'magnitude': datum.mag_autocorr, 'error': datum.magerr_auto},
                })
            else:
                continue
        return output
