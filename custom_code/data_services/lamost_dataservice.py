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
from custom_code.data_services.forms import LAMOSTQueryForm
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


def _extract_spectrum(dat):
    """From an open LAMOST FITS, return (flux, wavelength, arm) for the usable coadd spectrum.

    Both LRS (EXTNAME 'COADD') and MRS (EXTNAME 'COADD_B'/'COADD_R') store the final spectrum
    in a coadd HDU, but the HDU index varies and some files have a header-only / None-data
    coadd. We scan for the first coadd HDU that actually carries FLUX + WAVELENGTH data, and
    fall back to the first usable exposure HDU if no coadd is populated. Returns (None, None,
    None) if nothing usable is found.
    """
    usable = []
    for hdu in dat:
        cols = getattr(hdu, 'columns', None)
        names = list(cols.names) if cols is not None else []
        if 'FLUX' not in names or 'WAVELENGTH' not in names:
            continue
        rec = hdu.data
        if rec is None:
            continue
        try:
            flux = rec['FLUX'][0]
            wl = rec['WAVELENGTH'][0]
        except (IndexError, KeyError, TypeError):
            continue
        if flux is None or wl is None or len(flux) == 0 or len(wl) == 0:
            continue
        extname = str(hdu.header.get('EXTNAME', '') or '')
        usable.append((extname, flux, wl))

    coadds = [item for item in usable if item[0].upper().startswith('COADD')]
    chosen = coadds or usable
    if not chosen:
        return None, None, None
    extname, flux, wl = chosen[0]
    return flux, wl, (extname or None)


class LAMOSTDataService(DataService):
    name = 'LAMOST'
    verbose_name = 'LAMOST'
    update_on_daily_refresh = False
    info_url = LAMOST_PAGE_URL
    service_notes = 'Query LAMOST spectra by LAMOST DR11 v2.0 API.'

    @classmethod
    def get_form_class(cls):
        return LAMOSTQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or 2.5,
            'include_spectroscopy': bool(parameters.get('include_spectroscopy', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or 2.5

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
        for idnum in (data_spec.get('obsid-low') or []):
            url = f'https://www.lamost.org/openapi/dr11/v2.0/lrs/spectrum/fits?obsid={idnum}'
            datum = self._datum_from_fits(url, 'LAMOST_LRS_spectrum', idnum)
            if datum:
                output.append(datum)
        for idnum in (data_spec.get('obsid-medium') or []):
            url = f'https://www.lamost.org/openapi/dr11/v2.0/mrs/spectrum/fits?obsid={idnum}'
            datum = self._datum_from_fits(url, 'LAMOST_MRS_spectrum', idnum)
            if datum:
                output.append(datum)
        return output

    def _datum_from_fits(self, fits_url, spectrum_type, obsid):
        """Download one LAMOST spectrum and build a datum, or return None.

        Never raises: a spectrum that is missing, malformed, or has a None-data coadd HDU is
        logged and skipped so it cannot discard the rest of the batch.
        """
        try:
            with fits.open(fits_url) as dat:
                mjd = dat[0].header.get('MJD')
                desig = str(dat[0].header.get('DESIG', ''))
                flux, wl, arm = _extract_spectrum(dat)
            if mjd is None or flux is None or wl is None:
                logger.warning('LAMOST: no usable spectrum for obsid=%s (%s); skipping.', obsid, spectrum_type)
                return None
            spectrum = Spectrum1D(
                flux=flux * u.erg / u.s / u.cm**2 / u.AA,
                spectral_axis=wl * u.AA,
            )
            serialized = SpectrumSerializer().serialize(spectrum)
            serialized.update({
                'filter': 'LAMOST',
                'source_id': desig,
                'spectrum_type': spectrum_type,
            })
            if arm:
                serialized['arm'] = arm
            return {
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': serialized,
            }
        except Exception as exc:
            logger.warning('LAMOST: failed to process obsid=%s (%s): %s', obsid, spectrum_type, exc)
            return None
