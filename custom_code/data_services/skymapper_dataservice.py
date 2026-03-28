import logging
import math

from astropy.time import Time
from datetime import timezone

import pyvo

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import SkyMapperQueryForm


logger = logging.getLogger(__name__)

SKYMAPPER_TAP_URL = 'https://api.skymapper.nci.org.au/public/tap/'

def _skymapper_alias(obj_id):
    return f'SkyMapper_{obj_id}'

def _skymapper_source_location(obj_id):
    return f'https://skymapper.anu.edu.au/object-viewer/dr4/{obj_id}/'

def _to_float(value):
    # Astropy masked values convert to NaN with a warning; treat them as missing.
    if getattr(value, 'mask', False):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number

def _build_skymapper_tap_query(ra, dec, radius_arcsec):
    return f"""
            SELECT
            p.mag_psf,p.e_mag_psf,p.filter,p.object_id,
            i.date
            FROM dr4.photometry AS p
            JOIN dr4.images AS i
            ON p.image_id = i.image_id
            WHERE 1 = CONTAINS(
                POINT('ICRS', p.ra_img, p.decl_img),
                CIRCLE('ICRS', {ra}, {dec}, {radius_arcsec}/3600.)
            )
            ORDER BY i.date
            """

class SkyMapperDataService(DataService):
    name = 'SkyMapper'
    verbose_name = 'SkyMapper'
    update_on_daily_refresh = False
    info_url = SKYMAPPER_TAP_URL
    service_notes = 'Query SkyMapper by coordinates through TAP service.'

    @classmethod
    def get_form_class(cls):
        return SkyMapperQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        self.query_parameters = {
            'ra': parameters.get('ra'),
            'dec': parameters.get('dec'),
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
            skytap = pyvo.dal.TAPService(SKYMAPPER_TAP_URL)
            source_tap_query = _build_skymapper_tap_query(ra, dec, radius_arcsec)
            tap_response = skytap.search(source_tap_query)
            tap_response = tap_response.to_table()

            obj_id = None

            if len(tap_response) == 0:
                logger.debug('SkyMapper returned no photometry for RA=%s Dec=%s', ra, dec)
            else:
                obj_id = tap_response['object_id'][0]

        except Exception as e:
            logger.debug('SkyMapper error %s', e)
        
        self.query_results = {
            'obj_id':obj_id,
            'photometry_data': tap_response or None,
            'source_location':_skymapper_source_location(obj_id),
            'ra': ra,
            'dec': dec,
        }

        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        obj_id = data.get('obj_id')
        ra = data.get('ra')
        dec = data.get('dec')
        photometry_data = data.get('photometry_data')
        if ra is None or dec is None or photometry_data is None:
            return []

        alias = _skymapper_alias(obj_id)
        return [{
            'name': alias,
            'ra': ra,
            'dec': dec,
            'aliases': [alias],
            'reduced_datums': {'photometry': self._build_photometry_datums(photometry_data)},
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

    def _build_photometry_datums(self, rows):
        output = []
        for row in rows:
            mjd = _to_float(row["date"])
            mag = _to_float(row["mag_psf"])
            magerr = _to_float(row["e_mag_psf"])
            fil = row["filter"]
            band = f"SkyMapper({fil})"
            if mjd is None or mag is None or magerr is None:
                continue
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {'filter': band, 'magnitude': mag, 'error': magerr},
            })
        return output
