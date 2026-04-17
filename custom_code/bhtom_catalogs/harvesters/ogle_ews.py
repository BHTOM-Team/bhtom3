import logging

from tom_catalogs.harvester import AbstractHarvester

from custom_code.data_services.ogle_ews_dataservice import (
    OGLE_EWS_INFO_URL,
    OGLEEWSDataService,
    _ogle_event_url,
    _normalize_target_name,
    _prefixed_target_name,
    _to_float,
)


logger = logging.getLogger(__name__)


def get(term):
    service = OGLEEWSDataService()
    query_parameters = service.build_query_parameters({'target_name': term, 'include_photometry': False})
    results = service.query_service(query_parameters)
    alerts = results.get('alerts') or []
    return alerts[0] if alerts else {}


def get_all(term):
    service = OGLEEWSDataService()
    query_parameters = service.build_query_parameters({'target_name': term, 'include_photometry': False})
    results = service.query_service(query_parameters)
    return results.get('alerts') or []


class OGLEEWSHarvester(AbstractHarvester):
    name = 'OGLE EWS'

    def query(self, term):
        try:
            self.catalog_data = get(term)
        except Exception as exc:
            logger.warning('OGLE EWS query failed for term "%s": %s', term, exc)
            self.catalog_data = {}
        return self.catalog_data

    def to_target(self):
        target = super().to_target()
        raw_name = self.catalog_data.get('name')
        normalized_name = _normalize_target_name(raw_name)
        target.name = _prefixed_target_name(raw_name) or 'OGLE-EWS'
        target.type = 'SIDEREAL'
        target.ra = _to_float(self.catalog_data.get('ra'))
        target.dec = _to_float(self.catalog_data.get('dec'))
        target.epoch = 2000.0
        target.description = f'OGLE Early Warning System event {normalized_name}' if normalized_name else 'OGLE Early Warning System event'
        return target

    @staticmethod
    def source_url(match):
        raw_name = match.get('name')
        normalized_name = _normalize_target_name(raw_name)
        if not normalized_name:
            return OGLE_EWS_INFO_URL
        return _ogle_event_url(normalized_name)
