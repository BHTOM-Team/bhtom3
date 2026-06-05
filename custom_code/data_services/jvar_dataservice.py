import logging

from astropy.time import Time
from datetime import timezone

import pyvo as vo
from astropy.coordinates import SkyCoord

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import JVARQueryForm


logger = logging.getLogger(__name__)

JVAR_PAGE = "https://archive.cefca.es/catalogues/jvar-dr1"


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class JVARDataService(DataService):
    name = 'JVAR'
    verbose_name = 'JVAR'
    update_on_daily_refresh = True
    info_url = JVAR_PAGE
    service_notes = 'Query JVAR catalog by coordinates (doi:10.1051/0004-6361/202557049).'

    @classmethod
    def get_form_class(cls):
        return JVARQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or 3.0,
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or 3.0
        if ra is None or dec is None:
            self.query_results = {'lc_data': [], 'source_location': None}
            return self.query_results

        lc_data = None
        try:
            url = "https://archive.cefca.es/catalogues/vo/cone/jvar-dr1/JVAR.LIGHT_CURVES"
            scs = vo.dal.SCSService(url)
            res = scs.search(pos=SkyCoord(ra, dec, unit="deg"),radius=radius_arcsec/3600)
            jvar_tab = res.to_table()
            if len(jvar_tab)>0:
                lc_data = jvar_tab
            else:
                logger.debug('JVAR returned no data for RA=%s Dec=%s', ra, dec)
        except ValueError:
            logger.debug('JVAR returned error for RA=%s Dec=%s', ra, dec)

        self.query_results = {
            'lc_data': lc_data,
            'source_location': JVAR_PAGE,
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
        for filter_data in lc_data:
            filter_name = filter_data['FILTER']
            flags = filter_data['FLAGS'].data
            mag = filter_data['MAG'].data
            magerr = filter_data['MAG_ERR'].data
            mjd = filter_data['MJD'].data
            flag_crop = flags<1
            mag_good = mag[flag_crop]
            magerr_good = magerr[flag_crop]
            mjd_good = mjd[flag_crop]
            for loop in range(len(mag_good)):
                if magerr_good[loop]>2.0:
                    continue
                output.append({
                    'timestamp': Time(mjd_good[loop], format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                    'value': {'filter': f"JVAR({filter_name})", 'magnitude': mag_good[loop], 'error': magerr_good[loop]},
                    })
        return output
