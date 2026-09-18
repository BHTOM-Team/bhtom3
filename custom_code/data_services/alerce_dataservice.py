import logging
import math

from astropy.time import Time
from datetime import timezone

import requests

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import ZTFQueryForm
from custom_code.data_services.service_utils import (
    DATA_SERVICE_HTTP_TIMEOUT,
    add_difference_photometry,
    add_origin_coordinates,
    upsert_reduced_datums,
)


logger = logging.getLogger(__name__)

ALERCE_PAGE = "https://alerce.online/"

def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def _getAlerceObjcet(ra,dec,rad):
  url=f"https://api.alerce.online/ztf/v1/objects/?ra={ra}&dec={dec}&radius={rad}&page=1&page_size=20&count=true"
  headers = {"accept": "application/json"}
  response = requests.get(url, headers=headers, timeout=DATA_SERVICE_HTTP_TIMEOUT)
  return response.json()

def _getAlerceLightCurve(oid):
  url=f"https://api.alerce.online/ztf/v1/objects/{oid}/lightcurve"
  headers = {"accept": "application/json"}
  response = requests.get(url, headers=headers, timeout=DATA_SERVICE_HTTP_TIMEOUT)
  return response.json()

def _objects_nearest_first(items, ra, dec):

    def sort_key(item):
        meanra = _to_float(item.get('meanra'))
        meandec = _to_float(item.get('meandec'))
        if meanra is None or meandec is None:
            separation = float('inf')
        else:
            dra = ((meanra - ra + 180.0) % 360.0 - 180.0) * math.cos(math.radians(dec))
            separation = math.hypot(dra, meandec - dec)
        return separation, -(_to_float(item.get('ndet')) or 0)

    return sorted((item for item in items if item.get('oid')), key=sort_key)


def _merged_detections(objects):
    """Detections of every object within the search radius, one per exposure.

    The same alert (candid) appears under each object id it was associated with, and ZTF also
    issues duplicate alerts for a single exposure (same mjd and band, different candid, values
    within ~1e-3 mag). Keeping the lowest candid per exposure is deterministic, so repeated
    refreshes always keep the same alert.
    """
    by_exposure = {}
    for item in objects:
        oid = item['oid']
        try:
            object_detections = _getAlerceLightCurve(oid).get('detections') or []
        except (requests.RequestException, ValueError) as exc:
            logger.warning('Alerce light curve for %s failed: %s', oid, exc)
            continue
        for detection in object_detections:
            exposure = (detection.get('mjd'), detection.get('fid'))
            kept = by_exposure.get(exposure)
            if kept is None or _candid_order(detection) < _candid_order(kept):
                by_exposure[exposure] = detection
    return sorted(by_exposure.values(), key=lambda detection: detection.get('mjd') or 0)


def _candid_order(detection):
    candid = detection.get('candid')
    try:
        return (0, int(candid))
    except (TypeError, ValueError):
        return (1, str(candid))


def _get_filter(value):
    mapping = {
        1: "zg",
        2: "zr",
        3: "zi"
    }
    return mapping.get(value)

class AlerceDataService(DataService):
    name = 'Alerce'
    verbose_name = 'Alerce'
    update_on_daily_refresh = True
    info_url = ALERCE_PAGE
    # Photometry values carry origin_ra/origin_dec; see upsert_reduced_datums.
    stores_origin_coordinates = True
    service_notes = 'Query ZTF by coordinates and ingest ZTF photometry through Alerce.'

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
        source_location = "https://alerce.online/"
        try:
            objcet_data = _getAlerceObjcet(ra,dec,radius_arcsec)
            objects = _objects_nearest_first(objcet_data.get('items') or [], ra, dec)
            if objects:
                lc_data = _merged_detections(objects)
                source_location = f"https://alerce.online/object/{objects[0]['oid']}"
            else:
                logger.debug('Alerce returned no data for RA=%s Dec=%s', ra, dec)
        except ValueError:
            logger.debug('Alerce returned error for RA=%s Dec=%s', ra, dec)

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
        for datum in lc_data:
            value = {'filter': f"ZTF({_get_filter(datum['fid'])})"}

            mag = datum.get('magpsf_corr')
            mag_err = datum.get('sigmapsf_corr')
            if mag and mag_err and mag_err <= 2.0:
                value['magnitude'] = mag
                value['error'] = mag_err

            diff_mag = datum.get('magpsf')
            diff_err = datum.get('sigmapsf')
            if diff_mag and diff_err and diff_err <= 2.0:
                add_difference_photometry(value, diff_mag, diff_err, datum.get('isdiffpos', 1))

            if 'magnitude' not in value and 'diff_magnitude' not in value:
                continue

            add_origin_coordinates(value, datum.get('ra'), datum.get('dec'))
            output.append({
                'timestamp': Time(datum['mjd'], format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': value,
                })
        return output
