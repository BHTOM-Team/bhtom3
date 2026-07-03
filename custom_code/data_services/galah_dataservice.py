import logging

from astropy.time import Time
from datetime import timezone
from django.db import IntegrityError, transaction

from astropy.io import fits
from specutils import Spectrum1D
import numpy as np
from astropy.coordinates import SkyCoord
from pyvo.dal import SSAService
import astropy.units as u

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from tom_dataproducts.processors.data_serializers import SpectrumSerializer
from custom_code.data_services.forms import GS6dFQueryForm



logger = logging.getLogger(__name__)

GALAH_PAGE_URL = 'https://www.galah-survey.org/dr4'


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def _read_ext(hdul, ext):
    flux = np.asarray(hdul[ext].data, float)
    h = hdul[ext].header
    n = flux.size
    crval, cdelt, crpix = h["CRVAL1"], h["CDELT1"], h.get("CRPIX1", 1)
    wave = crval + (np.arange(n) - (crpix - 1)) * cdelt   # Angstroms
    return wave, flux

class Gs6dfDataService(DataService):
    name = 'GALAH'
    verbose_name = 'GALAH'
    update_on_daily_refresh = False
    info_url = GALAH_PAGE_URL
    service_notes = 'Query GALAH spectra by coordinates through VO.'

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

        cat_name = None
        fits_table = None
        

        if ra is None or dec is None:
            self.query_results = {'spectroscopy_data': [], 'source_location': None}
            return self.query_results

        try:
            service = SSAService("https://datacentral.org.au/vo/ssa/query")
            diameter = (2 * radius_arcsec)/3600.0
            pos = SkyCoord(ra, dec, unit="deg")
            COLLECTION = "galah_dr3" 
            dataResults = service.search(
                        pos=pos,
                        SIZE=diameter,
                        format="fits",
                        COLLECTION=COLLECTION)
            dataTab = dataResults.to_table()
            if len(dataTab)>0:
                dataTab = dataTab[dataTab['dataproduct_subtype']=='combined']
                if len(dataTab)>0:
                    cat_name = dataTab['target_name'][0]
                    fits_table = dataTab

                else:
                    logger.debug('GALAH returned no spectrum for RA=%s Dec=%s', ra, dec)
            else:
                logger.debug('GALAH returned no spectrum for RA=%s Dec=%s', ra, dec)

        except Exception as e:
            logger.debug('GALAH error %s', e)
        
        self.query_results = {
            'cat_name':cat_name or None,
            'spectroscopy_data': fits_table or None,
            'source_location': GALAH_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }

        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        cat_name = data.get('cat_name')
        ra = data.get('ra')
        dec = data.get('dec')
        spectroscopy_data = data.get('spectroscopy_data')
        if ra is None or dec is None or spectroscopy_data is None:
            return []

        return [{
            'name': cat_name,
            'ra': ra,
            'dec': dec,
            'aliases': [cat_name],
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
        if data_type != 'photometry' or not data:
            return
        source_location = kwargs.get('source_location') or self.info_url

        for datum in data:
            try:
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
            except IntegrityError:
                # Another process inserted it concurrently; retry with get
                try:
                    ReducedDatum.objects.get(
                        target=target,
                        data_type='photometry',
                        timestamp=datum['timestamp'],
                        value=datum['value'],
                    )
                except ReducedDatum.DoesNotExist:
                    # Rare case: still doesn't exist, retry in a transaction
                    with transaction.atomic():
                        ReducedDatum.objects.create(
                            target=target,
                            data_type='photometry',
                            timestamp=datum['timestamp'],
                            value=datum['value'],
                            source_name=self.name,
                            source_location=source_location,
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
        for tab in spec_data:
            hdul = fits.open(tab['full_data_url'])
            w_norm, f_norm = _read_ext(hdul, 4)
            time = tab['t_midpoint']
            serializer = SpectrumSerializer()
            spectrum = Spectrum1D(
                flux=f_norm * u.ct,
                spectral_axis=w_norm * u.AA,
            )
            serialized = serializer.serialize(spectrum)
            serialized.update({
                'filter': 'GALAH',
                'source_id': str(tab['target_name']),
                'spectrum_type': f'GALAH_{tab['band_name']}_spectrum',
                })
            output.append({
                'timestamp': Time(time, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': serialized,
            })

        return output
