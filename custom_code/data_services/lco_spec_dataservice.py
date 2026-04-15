import logging
from astropy.time import Time
from datetime import timezone
from datetime import datetime
from specutils import Spectrum1D
import numpy as np
import astropy.units as u
import requests
from astropy.io import fits

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName
from tom_dataproducts.processors.data_serializers import SpectrumSerializer
from custom_code.data_services.forms import LCOSpectraQueryForm



logger = logging.getLogger(__name__)

LCO_SPECTRA_ARCHIVE = 'https://archive.lco.global/'

lco_telescopes = ["COJ", "CPT", "TFN", "LSC", "ELP", "OGG", "TLV", "NGQ"]

def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class LCOSpectraDataService(DataService):
    name = 'LCOSpectra'
    verbose_name = 'LCOSpectra'
    update_on_daily_refresh = False
    info_url = LCO_SPECTRA_ARCHIVE
    service_notes = 'Query LCO spectra by coordinates through API.'

    @classmethod
    def get_form_class(cls):
        return LCOSpectraQueryForm

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

        spectra_data = None
        

        if ra is None or dec is None:
            self.query_results = {'spectroscopy_data': [], 'source_location': None}
            return self.query_results

        try:
            limit = 200
            reduction_level = 91
            now_utc = datetime.now(timezone.utc)
            today_str = now_utc.date().strftime("%Y-%m-%d")
            lco_res = requests.get(f"https://archive-api.lco.global/frames/?start=2014-01-01&end={today_str}&covers=POINT({ra} {dec})&public=true&exclude_calibrations=true&limit={limit}&configuration_type=SPECTRUM&reduction_level={reduction_level}").json()
            lco_filtered_res = [obj for obj in lco_res['results']if any(telescope.lower() in obj['site_id'].lower() for telescope in lco_telescopes)]
            if len(lco_filtered_res)>0:
                spectra_data = lco_filtered_res
            else:
                logger.debug('LCO returned no spectrum for RA=%s Dec=%s', ra, dec)

        except Exception as e:
            logger.debug('LCO Spectra error %s', e)
        
        self.query_results = {
            'spectroscopy_data': spectra_data,
            'source_location':LCO_SPECTRA_ARCHIVE,
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

    def _build_spectroscopy_datums(self, spec_data):
        output = []
        for spec_info in spec_data:
            try:
                serializer = SpectrumSerializer()
                basename = spec_info['basename']

                if "e91-1d" in basename:
                    spec_hdul = fits.open(spec_info['url'])
                    spec_time = spec_info['observation_date']
                    site_id = spec_info['site_id']
                    spec_id = spec_info['id']
                    sp_data = spec_hdul[1].data
                    valid = (~np.isnan(sp_data['flux'])) & (~np.isnan(sp_data['wavelength']))
                    clean_flux = np.array(sp_data['flux'][valid])
                    clean_wavelength = np.array(sp_data['wavelength'][valid])
                    spectrum = Spectrum1D(
                        flux=clean_flux * u.erg / u.s / u.cm**2 / u.AA,
                        spectral_axis=clean_wavelength * u.AA,)
                    serialized = serializer.serialize(spectrum)
                    serialized.update({
                    'filter': f'LCO-{site_id}',
                    'source_id': str(spec_id),
                    'spectrum_type': 'LCO_spectrum',})
                    output.append({
                    'timestamp': Time(spec_time, format='isot', scale='utc').to_datetime(timezone=timezone.utc),
                    'value': serialized,})
                else:
                    spec_id = spec_info['id']
                    resp_rel_frames = requests.get(f"https://archive-api.lco.global/frames/{spec_id}/related/").json()
                    for rel_frame in resp_rel_frames:
                        if "e91-1d" in rel_frame['basename']:
                            spec_hdul = fits.open(rel_frame['url'])
                            spec_time = rel_frame['observation_date']
                            site_id = rel_frame['site_id']
                            spec_id = rel_frame['id']
                            sp_data = spec_hdul[1].data
                            valid = (~np.isnan(sp_data['flux'])) & (~np.isnan(sp_data['wavelength']))
                            clean_flux = np.array(sp_data['flux'][valid])
                            clean_wavelength = np.array(sp_data['wavelength'][valid])
                            spectrum = Spectrum1D(
                                flux=clean_flux * u.erg / u.s / u.cm**2 / u.AA,
                                spectral_axis=clean_wavelength * u.AA,)
                            serialized = serializer.serialize(spectrum)
                            serialized.update({
                            'filter': f'LCO-{site_id}',
                            'source_id': str(spec_id),
                            'spectrum_type': 'LCO_spectrum',})
                            output.append({
                            'timestamp': Time(spec_time, format='isot', scale='utc').to_datetime(timezone=timezone.utc),
                            'value': serialized,})

            except Exception as e:
                print(e)
                continue

        return output
