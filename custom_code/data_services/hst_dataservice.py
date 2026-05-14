import logging

from astropy.time import Time
from datetime import timezone

from astroquery.esa.hubble import ESAHubble

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import HSTQueryForm


logger = logging.getLogger(__name__)

HUBBLE_PAGE = "https://hst.esac.esa.int/ehst/#/pages/hcv-explorer"

def _build_hcv_query(ra,dec,rad_arcsec):
    rad_degree = rad_arcsec/3600.0
    return f"SELECT * FROM hcv.hcv \
            WHERE 1=CONTAINS( \
            POINT('ICRS', ra, dec), \
            CIRCLE('ICRS', {ra}, {dec}, {rad_degree}))"

def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class HSTDataService(DataService):
    name = 'Hubble'
    verbose_name = 'Hubble'
    update_on_daily_refresh = True
    info_url = HUBBLE_PAGE
    service_notes = 'Query Hubble Catalog of Variables (HCV) by ra and dec through astroquery.'

    @classmethod
    def get_form_class(cls):
        return HSTQueryForm

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
            self.query_results = {'lc_data': [], 'source_location': None}
            return self.query_results

        lc_data = None
        try:
            esahubble = ESAHubble()
            result = esahubble.query_tap(query=_build_hcv_query(ra,dec,radius_arcsec))

            if len(result)<=0:
                logger.debug('HST returned no data for RA=%s Dec=%s', ra, dec)
            else:
                lc_data = result
                source_location = HUBBLE_PAGE

        except ValueError:
            logger.debug('HST returned error for RA=%s Dec=%s', ra, dec)

        self.query_results = {
            'lc_data': lc_data,
            'source_location': source_location,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        ra = data.get('ra')
        dec = data.get('dec')
        lc_data = data.get('lc_data')
        if ra is None or dec is None or lc_data is None:
            return []

        return [{
            'name': None,
            'ra': ra,
            'dec': dec,
            'aliases': [None],
            'reduced_datums': {'photometry': self._build_photometry_datums(lc_data)},
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

    def _build_photometry_datums(self, lc_data):
        output = []
        for datum in lc_data:
            mjd = datum['lightcurve_d']
            mag = datum['lightcurve_cm']
            mag_err = datum['lightcurve_e']
            filt = datum['filter']
            if mag is None or mag_err is None or not mag or not mag_err or mag_err>3.0:
                continue
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {'filter': f"HST({filt})", 'magnitude': mag, 'error': mag_err},
                })
        return output
