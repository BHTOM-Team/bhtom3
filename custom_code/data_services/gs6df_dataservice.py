import logging

from astropy.time import Time
from datetime import timezone

from astropy.io import fits
from specutils import Spectrum1D
import numpy as np

from astroquery.vizier import Vizier
import astropy.units as u
import astropy.coordinates as coord

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from tom_dataproducts.processors.data_serializers import SpectrumSerializer
from custom_code.data_services.forms import GS6dFQueryForm



logger = logging.getLogger(__name__)

GS6DF_PAGE_URL = 'http://www-wfau.roe.ac.uk/6dFGS/'

def _gs6df_alias(obj_id):
    return f'6dFGS_{obj_id}'

def _gs6df_source_location(name,spec):
    return f"http://www-wfau.roe.ac.uk/6dFGS/cgi-bin/show.cgi?release=dr3&targetname={name}&specid={spec}"


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class Gs6dfDataService(DataService):
    name = '6dFGS'
    verbose_name = '6dFGS'
    info_url = GS6DF_PAGE_URL
    service_notes = 'Query 6dFGS spectra by coordinates from Vizier.'

    @classmethod
    def get_form_class(cls):
        return GS6dFQueryForm

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
        fits_table = None
        

        if ra is None or dec is None:
            self.query_results = {'spectroscopy_data': [], 'source_location': None}
            return self.query_results

        try:
            vizier = Vizier()
            viz_result = vizier.query_region(
                coord.SkyCoord(ra=ra, dec=dec,
                unit=(u.deg, u.deg),frame='icrs'),
                radius=radius_arcsec*u.arcsec,
                catalog=["VII/259/spectra"])
            if len(viz_result)>0:
                viz_table = viz_result[0].to_pandas()

                if len(viz_table) == 0:
                    logger.info('6dFGS returned no spectrum for RA=%s Dec=%s', ra, dec)
                else:
                    cat_name = viz_table['6dFGS'][0]
                    spec_id = viz_table['SpecID'][0]
                    time = (viz_table['MJD.V'][0]+viz_table['MJD.R'][0])/2.0
                    ra_id = cat_name[1:3]
                    fits_url = f"http://www-wfau.roe.ac.uk/6dFGS/dr3_fits/fits/{ra_id}/{cat_name}.fits"
                    try:
                        fits_table = fits.open(fits_url)
                    except Exception as fitse:
                        logger.info('6dFGS error', fitse)
            else:
                logger.info('6dFGS returned no spectrum for RA=%s Dec=%s', ra, dec)

        except Exception as e:
            logger.info('6dFGS error', e)
        
        self.query_results = {
            'cat_name':cat_name or None,
            'spec_id':spec_id or None,
            'time':time or None,
            'spectroscopy_data': fits_table or None,
            'source_location':_gs6df_source_location(cat_name,spec_id),
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

        alias = _gs6df_alias(cat_name)

        return [{
            'name': alias,
            'ra': ra,
            'dec': dec,
            'aliases': [alias],
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

    def _build_spectroscopy_datums(self, hdul, time,cat_name):
        output = []
        serializer = SpectrumSerializer()
        spec_data = hdul["SPECTRUM VR"].data
        filter = spec_data[3]>3950.0
        wlength = spec_data[3][filter]
        cts = spec_data[0][filter]
        cts_er = spec_data[1][filter]
        cts_er = np.sqrt(cts_er)
        spectrum = Spectrum1D(
                flux=cts * u.ct,
                spectral_axis=wlength * u.AA,
            )
        serialized = serializer.serialize(spectrum)
        serialized.update({
                'filter': '6dFGS',
                'source_id': str(cat_name),
                'spectrum_type': '6dFGS_VR_spectrum',
            })
        output.append({
                'timestamp': Time(time, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': serialized,
            })

        return output
