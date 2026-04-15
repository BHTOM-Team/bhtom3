import logging
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO

import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time
from datetime import timezone

import threading

import gPhoton
from astroquery.mast import Catalogs

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import GalexQueryForm


logger = logging.getLogger(__name__)

GALEX_WEBPAGE = "https://galex.stsci.edu/GR6/"

aperture_radius = 10 / (60 * 60)
bkg_in_aperture_radius = 11 / (60 * 60)
bkg_out_aperture_radius = 20 / (60 * 60)


def galex_aperture_timeout(band, ra, dec, radius, annulus, timeout=900):
    result = {"data": None}

    def worker():
        try:
            with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
                result["data"] = gPhoton.gAperture(
                    band=band,
                    skypos=[ra, dec],
                    radius=radius,
                    annulus=annulus
                )
        except Exception:
            result["data"] = None

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    thread.join(timeout)

    if thread.is_alive():
        logger.warning("gPhoton timeout after %s seconds", timeout)
        return None

    return result["data"]

def _galex_source_location(obj_id):
    return f'https://galex.stsci.edu/GR6/?page=explore&photo=true&objid={obj_id}'

def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

class GalexDataService(DataService):
    name = 'Galex'
    verbose_name = 'Galex'
    update_on_daily_refresh = False
    info_url = GALEX_WEBPAGE
    service_notes = 'Query Galex by coordinates through gPhoton.'

    @classmethod
    def get_form_class(cls):
        return GalexQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or 5.0,
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or 5.0
        if ra is None or dec is None:
            self.query_results = {'photometry_data': [], 'source_location': None}
            return self.query_results

        try:
            galex_catalog_data = Catalogs.query_region(
                coordinates=SkyCoord(ra=ra, dec=dec, unit='deg'),
                catalog="Galex",
                radius=(radius_arcsec / 3600.0) * u.deg,
            )
            galex_data = galex_catalog_data.to_pandas()
            obj_id = None
            IAU_name = None

            if len(galex_data) == 0:
                logger.debug('Galex (astroquery) returned no photometry for RA=%s Dec=%s', ra, dec)
            else:
                obj_id = galex_data['objID'][0]
                IAU_name = galex_data['IAUName'][0]

        except Exception as e:
            logger.debug('Astroquery error %s', e)
        
        self.query_results = {
            'obj_id':obj_id or None,
            'IAU_name':IAU_name or None,
            'source_location':_galex_source_location(obj_id),
            'ra': ra,
            'dec': dec,
        }

        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        IAU_name = data.get('IAU_name')
        ra = data.get('ra')
        dec = data.get('dec')
        if ra is None or dec is None or IAU_name is None:
            return []

        return [{
            'name': IAU_name,
            'ra': ra,
            'dec': dec,
            'aliases': [IAU_name],
            'reduced_datums': {'photometry': self._build_photometry_datums(ra,dec)},
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

    def _build_photometry_datums(self,ra,dec):
        output = []
        data_nuv = None
        data_fuv = None
        try:
            data_nuv = galex_aperture_timeout("NUV",ra,dec,aperture_radius,[bkg_in_aperture_radius, bkg_out_aperture_radius])
            data_fuv = galex_aperture_timeout("FUV",ra,dec,aperture_radius,[bkg_in_aperture_radius, bkg_out_aperture_radius])
                    
        except Exception as e:
            logger.debug('gPhoton error %s', e)

        if data_nuv is None or data_fuv is None:
            return output
        else:
            if len(data_nuv["mag"]) == 0 and len(data_fuv["mag"]) == 0:
                return output

        for loc in range(len(data_nuv["mag"])):
            band = "GALEX(NUV)"
            mag = _to_float(data_nuv["mag"][loc])
            magerr = _to_float(data_nuv["mag_err_1"][loc])
            mjd = Time(
                        (data_nuv["t_mean"][loc] + 315964800.0),
                        format="unix",
                        scale="utc",
                    ).mjd
            
            if mjd is None or mag is None or magerr is None:
                continue
            
            if magerr < 3.0:
                output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {'filter': band, 'magnitude': mag, 'error': magerr},
            })
        
        for loc in range(len(data_fuv["mag"])):
            band = "GALEX(FUV)"
            mag = _to_float(data_fuv["mag"][loc])
            magerr = _to_float(data_fuv["mag_err_1"][loc])
            mjd = Time(
                        (data_fuv["t_mean"][loc] + 315964800.0),
                        format="unix",
                        scale="utc",
                    ).mjd
            
            if mjd is None or mag is None or magerr is None:
                continue
            
            if magerr < 3.0:
                output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {'filter': band, 'magnitude': mag, 'error': magerr},
            })

        return output
