import logging

from astropy.time import Time
from datetime import timezone

from specutils import Spectrum1D
import astropy.units as u
from astropy.table import MaskedColumn, Column

import pyvo
import requests
from astropy.io import fits
from astropy.table import Table
from io import BytesIO
import numpy as np

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from tom_dataproducts.processors.data_serializers import SpectrumSerializer
from custom_code.data_services.forms import ESOSpectraQueryForm



logger = logging.getLogger(__name__)

ESO_SCIENCE_PORTAL_URL = 'https://archive.eso.org/scienceportal/home'

def _build_eso_query(ra,dec,rad_arcsec):
    rad_deg = rad_arcsec/3600.0
    return f"""
    SELECT *
    FROM ivoa.ObsCore
    WHERE dataproduct_type = 'spectrum'
    AND dataproduct_subtype = 'flux-calibrated'
    AND 1=CONTAINS(
        POINT('ICRS', s_ra, s_dec),
        CIRCLE('ICRS', {ra}, {dec}, {rad_deg})
    )
    """

def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class ESOSpectraDataService(DataService):
    name = 'ESO'
    verbose_name = 'ESO'
    update_on_daily_refresh = False
    info_url = ESO_SCIENCE_PORTAL_URL
    service_notes = 'Query all archived ESO spectra by coordinates from ESO tap.'

    @classmethod
    def get_form_class(cls):
        return ESOSpectraQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or 5.0,
            'include_spectroscopy': bool(parameters.get('include_spectroscopy', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or 5.0

        fits_table = None

        if ra is None or dec is None:
            self.query_results = {'spectroscopy_data': [], 'source_location': None}
            return self.query_results

        try:
            eso_tap = pyvo.dal.TAPService("https://archive.eso.org/tap_obs")
            eso_result = eso_tap.search(_build_eso_query(ra,dec,radius_arcsec))
            eso_table = eso_result.to_table()

            if len(eso_table)>0:
                fits_table = eso_table
            else:
                logger.debug('ESO Tap returned no spectrum for RA=%s Dec=%s', ra, dec)

        except Exception as e:
            logger.debug('ESO Spectra Tap error %s', e)
        
        self.query_results = {
            'spectroscopy_data': fits_table or None,
            'source_location':ESO_SCIENCE_PORTAL_URL,
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


        return [{
            'name': None,
            'ra': ra,
            'dec': dec,
            'aliases': [None],
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

    def _build_spectroscopy_datums(self, spec_table):
        output = []
        for tab in spec_table:
            try:
                serializer = SpectrumSerializer()
                spec_res = requests.get(f"https://dataportal.eso.org/dataPortal/file/{tab['dp_id']}")
                spec_hdul = fits.open(BytesIO(spec_res.content))
                time_mjd = spec_hdul[0].header['MJD-OBS']
                target_id = spec_hdul[0].header['HIERARCH ESO OBS ID']
                tel = spec_hdul[0].header['TELESCOP']
                instr = spec_hdul[0].header['INSTRUME']
                spec_data = Table.read(BytesIO(spec_res.content), format="fits")
                if isinstance(spec_data['WAVE'], Column):
                    spec_wave = spec_data['WAVE'].data[0]
                else:
                    spec_wave = spec_data['WAVE'][0].data
                if isinstance(spec_data['FLUX'], MaskedColumn):
                    spec_flux = spec_data['FLUX'][0].data
                else:
                    spec_flux = spec_data['FLUX'].data[0]

                mask = (~np.isnan(spec_flux)) & (spec_flux > 0)
                spec_wave_pos = spec_wave[mask]
                spec_flux_pos = spec_flux[mask]
                spectrum = Spectrum1D(
                            flux=spec_flux_pos * spec_data['FLUX'].unit,
                            spectral_axis=spec_wave_pos * spec_data['WAVE'].unit,)
                serialized = serializer.serialize(spectrum)
                serialized.update({
                        'filter': f'{tel}-{instr}',
                        'source_id': str(target_id),
                        'spectrum_type': 'ESO_spectrum',})

                output.append({
                    'timestamp': Time(time_mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                    'value': serialized,
                    })
            except Exception as e:
                continue

        return output
