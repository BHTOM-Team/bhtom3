import logging
from astropy.time import Time
from datetime import timezone
from datetime import datetime
from specutils import Spectrum1D
import numpy as np
import astropy.units as u
import requests
import tarfile
from io import BytesIO
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

def _giveWaveFlux(file):
    hdul = fits.open(file)

    data = hdul[0].data
    header = hdul[0].header

    flux = data[0][0]

    crval1 = header['CRVAL1']
    cd1_1 = header['CD1_1']
    crpix1 = header['CRPIX1']
    naxis1 = header['NAXIS1']
    pixels = np.arange(naxis1)

    wavelength = crval1 + (pixels + 1 - crpix1) * cd1_1

    mask = flux > 0

    clean_flux = flux[mask]
    clean_wavelength = wavelength[mask]

    return clean_wavelength,clean_flux*1e-20


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

        spectra_data = None
        

        if ra is None or dec is None:
            self.query_results = {'spectroscopy_data': [], 'source_location': None}
            return self.query_results

        try:
            limit = 200
            reduction_level = 90
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
                spec_gz_url = spec_info['url']
                spec_id = spec_info['id']
                spec_time = spec_info['observation_date']
                site_id = spec_info['site_id']
                gz_resp = requests.get(spec_gz_url)
                gz_resp.raise_for_status()
                gz_file = BytesIO(gz_resp.content)
                with tarfile.open(fileobj=gz_file, mode="r:gz") as tar:
                    spec_ex_name = next(name for name in tar.getnames() if "merge" in name.lower() and "_ex.fits" in name.lower())
                    spec_ex_file = tar.extractfile(spec_ex_name)
                    wave,flux = _giveWaveFlux(spec_ex_file)
                    spectrum = Spectrum1D(
                        flux=flux * u.erg / u.s / u.cm**2 / u.AA,
                        spectral_axis=wave * u.AA,)
                    serialized = serializer.serialize(spectrum)
                    serialized.update({
                    'filter': f'LCO-{site_id}',
                    'source_id': str(spec_id),
                    'spectrum_type': 'LCO_spectrum',})
                    output.append({
                    'timestamp': Time(spec_time, format='isot', scale='utc').to_datetime(timezone=timezone.utc),
                    'value': serialized,})
            except Exception as e:
                continue

        return output
