import logging

from astropy.time import Time
from datetime import timezone

from astropy.io import fits
from specutils import Spectrum1D

import requests

import astropy.units as u

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from tom_dataproducts.processors.data_serializers import SpectrumSerializer
from custom_code.data_services.forms import GS6dFQueryForm
from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT



logger = logging.getLogger(__name__)

LAMOST_PAGE_URL = 'https://www.lamost.org/dr11/v2.0/'

def _lamost_alias(obj_id):
    return f'LAMOST_{obj_id}'


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class LAMOSTDataService(DataService):
    name = 'LAMOST'
    verbose_name = 'LAMOST'
    update_on_daily_refresh = False
    info_url = LAMOST_PAGE_URL
    service_notes = 'Query LAMOST spectra by LAMOST DR11 v2.0 API.'

    @classmethod
    def get_form_class(cls):
        return GS6dFQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or 2.0,
            'include_spectroscopy': bool(parameters.get('include_spectroscopy', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or 2.0

        lamost_info = None
        

        if ra is None or dec is None:
            self.query_results = {'spectroscopy_data': [], 'source_location': None}
            return self.query_results

        try:
            lamostURL = f"https://www.lamost.org/openapi/dr11/v2.0/get_unique_id_and_related_obsids?ra={ra}&dec={dec}&radius={radius_arcsec/(60*60)}"
            lamostData = requests.get(lamostURL, timeout=DATA_SERVICE_HTTP_TIMEOUT).json()
            
            if len(lamostData)>0:
                lamost_info = lamostData
            else:
                logger.debug('LAMOST returned no spectrum for RA=%s Dec=%s', ra, dec)

        except Exception as e:
            logger.debug('LAMOST error %s', e)
        
        self.query_results = {
            'spectroscopy_data': lamost_info or None,
            'source_location':LAMOST_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }

        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        ra = data.get('ra')
        dec = data.get('dec')
        spectroscopy_data = data.get('spectroscopy_data')
        if ra is None or dec is None or spectroscopy_data is None:
            return []

        alias = _lamost_alias(spectroscopy_data['uid'])

        return [{
            'name': alias,
            'ra': ra,
            'dec': dec,
            'aliases': [alias],
            'reduced_datums': {'spectroscopy': self._build_spectroscopy_datums(spectroscopy_data)},
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

    def _build_spectroscopy_datums(self, data_spec):
        output = []

        if len(data_spec['obsid-low'])>0:
            for idnum in data_spec['obsid-low']:
                fits_url = f'https://www.lamost.org/openapi/dr11/v2.0/lrs/spectrum/fits?obsid={idnum}'
                dat = fits.open(fits_url)
                time = dat[0].header['MJD']
                specdata = dat[1].data
                flux = specdata['FLUX'][0]
                wl = specdata['WAVELENGTH'][0]
                serializer = SpectrumSerializer()
                spectrum = Spectrum1D(
                                flux=flux * u.erg / u.s / u.cm**2 / u.AA,
                                spectral_axis=wl * u.AA,)
                serialized = serializer.serialize(spectrum)
                serialized.update({
                'filter': 'LAMOST',
                'source_id': str(dat[0].header['DESIG']),
                'spectrum_type': 'LAMOST_LRS_spectrum',
                 })
                output.append({
                'timestamp': Time(time, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': serialized,
                })

        if len(data_spec['obsid-medium'])>0:
            for idnum in data_spec['obsid-medium']:
                fits_url = f'https://www.lamost.org/openapi/dr11/v2.0/mrs/spectrum/fits?obsid={idnum}'
                dat = fits.open(fits_url)
                time = dat[0].header['MJD']
                specdata = dat[1].data
                flux = specdata['FLUX'][0]
                wl = specdata['WAVELENGTH'][0]
                serializer = SpectrumSerializer()
                spectrum = Spectrum1D(
                                flux=flux * u.erg / u.s / u.cm**2 / u.AA,
                                spectral_axis=wl * u.AA,)
                serialized = serializer.serialize(spectrum)
                serialized.update({
                'filter': 'LAMOST',
                'source_id': str(dat[0].header['DESIG']),
                'spectrum_type': 'LAMOST_LRS_spectrum',
                 })
                output.append({
                'timestamp': Time(time, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': serialized,
                })

        return output
