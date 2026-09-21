import logging

from astropy.time import Time
from datetime import timezone

import pandas as pd
from io import StringIO
import requests
from urllib.parse import urlencode

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import ZTFQueryForm
from custom_code.data_services.service_utils import (
    DATA_SERVICE_HTTP_TIMEOUT,
    add_origin_coordinates,
    upsert_reduced_datums,
)


logger = logging.getLogger(__name__)

ZTF_PAGE = "https://irsa.ipac.caltech.edu/Missions/ztf.html"
ZTF_LIGHTCURVE_API = "https://irsa.ipac.caltech.edu/cgi-bin/ZTF/nph_light_curves"
ZTF_ALIAS_SOURCE = "ZTF Data Release"


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_ztf_api_url(ra,dec,rad_arcsec):
    rad = rad_arcsec * 0.000278
    query = urlencode({
        'POS': f'CIRCLE {ra} {dec} {rad}',
        'BAD_CATFLAGS_MASK': 32768,
        'FORMAT': 'CSV',
    })
    return f"{ZTF_LIGHTCURVE_API}?{query}"


def _ztf_object_url(oid):
    """Return a human-readable query for one object in the active public release."""
    query = urlencode({
        'ID': str(oid),
        'BAD_CATFLAGS_MASK': 32768,
        'FORMAT': 'HTML',
    })
    return f"{ZTF_LIGHTCURVE_API}?{query}"


def _ztf_object_ids(lc_data):
    if lc_data is None or 'oid' not in lc_data.columns:
        return []

    object_ids = []
    for value in lc_data['oid']:
        if pd.isna(value):
            continue
        # pandas can coerce an integer identifier to a float when nulls are present.
        if isinstance(value, float) and value.is_integer():
            value = int(value)
        oid = str(value).strip()
        if oid and oid not in object_ids:
            object_ids.append(oid)
    return object_ids


class ZTFDataService(DataService):
    name = 'ZTF'
    verbose_name = 'ZTF'
    update_on_daily_refresh = True
    info_url = ZTF_PAGE
    # Photometry values carry origin_ra/origin_dec; see upsert_reduced_datums.
    stores_origin_coordinates = True
    service_notes = 'Query ZTF by coordinates and ingest ZTF photometry.'

    @classmethod
    def get_form_class(cls):
        return ZTFQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or 1.1,
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or 1.1
        if ra is None or dec is None:
            self.query_results = {'lc_data': [], 'source_location': None}
            return self.query_results

        lc_data = None
        source_location = None
        try:
            query_url = _build_ztf_api_url(ra,dec,radius_arcsec)
            ztf_res = requests.get(
                query_url,
                timeout=DATA_SERVICE_HTTP_TIMEOUT,
            )
            ztf_df = pd.read_csv(StringIO(ztf_res.text))
            if len(ztf_df)>0:
                lc_data = ztf_df
                source_location = query_url
            else:
                logger.debug('ZTF returned no data for RA=%s Dec=%s', ra, dec)
        except ValueError:
            logger.debug('ZTF returned error for RA=%s Dec=%s', ra, dec)

        self.query_results = {
            'lc_data': lc_data,
            'source_location': source_location or ZTF_PAGE,
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

        aliases = [
            {
                'name': oid,
                'url': _ztf_object_url(oid),
                'source_name': ZTF_ALIAS_SOURCE,
            }
            for oid in _ztf_object_ids(lc_data)
        ]

        return [{
            'name': None,
            'ra': ra,
            'dec': dec,
            'aliases': aliases,
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
        aliases = []
        for alias in alias_results:
            alias_name = alias.get('name') if isinstance(alias, dict) else alias
            alias_name = str(alias_name or '').strip()
            if alias_name:
                aliases.append(TargetName(name=alias_name))
        return aliases

    def create_reduced_datums_from_query(self, target, data=None, data_type=None, **kwargs):
        if data_type != 'photometry' or not data:
            return
        source_location = kwargs.get('source_location') or self.info_url
        # upsert (rather than get_or_create) so points stored before origin_ra/origin_dec
        # existed get the position filled in instead of being duplicated.
        upsert_reduced_datums(
            target=target,
            data_type='photometry',
            source_name=self.name,
            source_location=source_location,
            datums=data,
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
        for _, datum in lc_data.iterrows():
            if datum.magerr>2.0:
                continue
            value = {'filter': f"ZTF({datum.filtercode})", 'magnitude': datum.mag, 'error': datum.magerr}
            add_origin_coordinates(value, getattr(datum, 'ra', None), getattr(datum, 'dec', None))
            output.append({
                'timestamp': Time(datum.mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': value,
                })
        return output
