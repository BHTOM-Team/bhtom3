import math
import logging

from astropy import units as u
from astropy.time import Time
from astroquery.sdss import SDSS
from datetime import timezone
from specutils import Spectrum1D
from astropy.io import fits
import pandas as pd
import numpy as np

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_dataproducts.processors.data_serializers import SpectrumSerializer
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import SDSSQueryForm

logger = logging.getLogger(__name__)


def _to_float(value):
    try:
        converted = float(value)
        if math.isnan(converted):
            return None
        return converted
    except (TypeError, ValueError):
        return None

def _build_sdss_photometry_query(ra,dec,rad):
    return f"""
    SELECT p.objID,p.mjd,p.u,p.g,p.r,p.i,p.z,p.err_u,p.err_g,p.err_r,p.err_i,p.err_z
    FROM dbo.fGetNearbyObjEq({ra}, {dec}, {rad/60.0}) AS n
    JOIN PhotoObjAll AS p ON p.objID = n.objID
    ORDER BY n.distance, p.mjd
    """

def _build_sdss_spectroscopy_query(ra,dec,rad):
    return f"""
    SELECT
    a.mjd,
    a.sas_url,
    a.apogee_id,
    dbo.fDistanceEq(a.ra, a.dec, {ra}, {dec}) AS distance
    FROM AllSpec AS a
    WHERE
    a.ra BETWEEN {ra}-0.1 AND {ra}+0.1
    AND a.dec BETWEEN {dec}-0.1 AND {dec}+0.1
    AND dbo.fDistanceEq(a.ra, a.dec, {ra}, {dec}) <= {rad/60.0}
    ORDER BY distance, a.mjd
    """

def _build_sdss_page_url(id):
    return f"https://skyserver.sdss.org/dr19/VisualTools/explore/summary?id={id}"

class SDSSDataService(DataService):
    name = 'SDSS'
    verbose_name = 'SDSS'
    info_url = 'https://skyserver.sdss.org/dr19/VisualTools/navi'
    service_notes = 'Query SDSS by cone search'

    @classmethod
    def get_form_class(cls):
        return SDSSQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        self.query_parameters = {
            'ra': parameters.get('ra'),
            'dec': parameters.get('dec'),
            'radius_arcsec': parameters.get('radius_arcsec') or 10.0,
            'include_photometry': bool(parameters.get('include_photometry', True)),
            'include_spectroscopy': bool(parameters.get('include_spectroscopy', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = query_parameters.get('ra')
        dec = query_parameters.get('dec')
        radius_arcsec = float(query_parameters.get('radius_arcsec') or 10.0)

        source_origin = None
        sdss_phot_df = None
        sdss_spec_df = None
        sdss_id = None

        if ra is None and dec is None:
            self.query_results = {'spectroscopy_data': [], 'photometry_data': [], 'source_location': None}
            return self.query_results


        if query_parameters.get('include_photometry', True):
            sdss_query = _build_sdss_photometry_query(ra,dec,radius_arcsec)
            sdss_response = SDSS.query_sql(sdss_query,data_release=19)
            if sdss_response:
                sdss_phot_df = sdss_response.to_pandas()
                sdss_id = sdss_phot_df['objID'][0]
                source_origin = _build_sdss_page_url(sdss_id)
            else:
                logger.info('SDSS returned no photometry for RA=%s Dec=%s', ra, dec)

        if query_parameters.get('include_spectroscopy', True):
            sdss_spec_query = _build_sdss_spectroscopy_query(ra,dec,radius_arcsec)
            sdss_spec_response = SDSS.query_sql(sdss_spec_query, data_release=19)
            if sdss_spec_response:
                sdss_spec_df = sdss_spec_response.to_pandas()
                sdss_spec_df = sdss_spec_df[sdss_spec_df["mjd"]>0]
                sdss_spec_df= sdss_spec_df.drop_duplicates(subset=["mjd"], keep="first")
            else:
                logger.info('SDSS returned no spectroscopy for RA=%s Dec=%s', ra, dec)

        self.query_results = {
            'sdss_id':sdss_id,
            'photometry_data': sdss_phot_df,
            'spectroscopy_data': sdss_spec_df,
            'source_origin': source_origin,
            'photometry_origin': source_origin,
            'spectroscopy_origin': source_origin,
            'ra':ra,
            'dec':dec
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        photometry_origin = data.get('photometry_origin')
        sdss_id = data.get('sdss_id')
        if photometry_origin is None or sdss_id is None:
            return []

        target_result = {
            'name': f'SDSS_{sdss_id}',
            'ra': _to_float(data.get('ra')),
            'dec': _to_float(data.get('dec')),
            'aliases': [f'SDSS_{sdss_id}'],
            'reduced_datums': {
                'photometry': self._build_photometry_datums(data.get('photometry_data')),
                'spectroscopy': self._build_spectroscopy_datums(sdss_id,data.get('spectroscopy_data')),
            },
        }
        return [target_result]

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
        if data_type not in {'photometry', 'spectroscopy'} or not data:
            return
        source_location = kwargs.get('source_location') or self.info_url
        for datum in data:
            ReducedDatum.objects.get_or_create(
                target=target,
                data_type=data_type,
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

    def _build_photometry_datums(self, phot_Data):
        output = []
        for _, datum in phot_Data.iterrows():
            mjd = _to_float(datum['mjd'])
            timestamp = Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc)
            if _to_float(datum['u']) is not None or _to_float(datum['err_u']) is not None:
                 output.append({
                        'timestamp': timestamp,
                        'value': {'filter': f"SDSS(u)", 
                                  'magnitude': _to_float(datum['u']), 
                                  'error': _to_float(datum['err_u'])
                                  },
                    })
            if _to_float(datum['g']) is not None or _to_float(datum['err_g']) is not None:
                 output.append({
                        'timestamp': timestamp,
                        'value': {'filter': f"SDSS(g)", 
                                  'magnitude': _to_float(datum['g']), 
                                  'error': _to_float(datum['err_g'])
                                  },
                    })
                 
            if _to_float(datum['r']) is not None or _to_float(datum['err_r']) is not None:
                 output.append({
                        'timestamp': timestamp,
                        'value': {'filter': f"SDSS(r)", 
                                  'magnitude': _to_float(datum['r']), 
                                  'error': _to_float(datum['err_r'])
                                  },
                    })
            
            if _to_float(datum['i']) is not None or _to_float(datum['err_i']) is not None:
                 output.append({
                        'timestamp': timestamp,
                        'value': {'filter': f"SDSS(i)", 
                                  'magnitude': _to_float(datum['i']), 
                                  'error': _to_float(datum['err_i'])
                                  },
                    })
                 
            if _to_float(datum['z']) is not None or _to_float(datum['err_z']) is not None:
                 output.append({
                        'timestamp': timestamp,
                        'value': {'filter': f"SDSS(z)", 
                                  'magnitude': _to_float(datum['z']), 
                                  'error': _to_float(datum['err_z'])
                                  },
                    })

        return output


    def _build_spectroscopy_datums(self, sdss_id, spec_Data):
        output = []
        for _, datum in spec_Data.iterrows():
            serializer = SpectrumSerializer()
            mjd = _to_float(datum['mjd'])
            sas_url = datum['sas_url']
            ap_id = datum['apogee_id']
            fits_table = fits.open(sas_url)

            if not pd.isna(ap_id):
                try:
                    # it is apogee spectra
                    flux = fits_table[1].data.flatten()
                    wave = fits_table[4].data.flatten()
                    idx = np.argsort(wave)
                    wave_sorted = np.array(wave[idx])
                    flux_sorted = np.array(flux[idx])*1e-17
                    spectrum = Spectrum1D(
                        flux=flux_sorted * u.erg / u.s / u.cm**2 / u.AA,
                        spectral_axis=wave_sorted * u.AA,)
                    serialized = serializer.serialize(spectrum)
                    serialized.update({
                        'filter': 'APOGEE',
                        'source_id': str(sdss_id),
                        'spectrum_type': 'SDSS_APOGEE_spectrum',})
                    output.append({
                        'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                        'value': serialized,})
                except Exception as e:
                    logger.info('SDSS apogee spectra error', e)
            else:
                try:
                    # it is optical spectra
                    flux = fits_table[1].data['FLUX']
                    wave = 10**fits_table[1].data['LOGLAM']
                    flux = np.array(flux)*1e-17
                    wave = np.array(wave)
                    spectrum = Spectrum1D(
                            flux=flux * u.erg / u.s / u.cm**2 / u.AA,
                            spectral_axis=wave * u.AA,)
                    serialized = serializer.serialize(spectrum)
                    serialized.update({
                            'filter': 'SDSS',
                            'source_id': str(sdss_id),
                            'spectrum_type': 'SDSS_OPTICAL_spectrum',})
                    output.append({
                            'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                            'value': serialized,})
                except Exception as e:
                    logger.info('SDSS optical spectra error', e)


        return output
