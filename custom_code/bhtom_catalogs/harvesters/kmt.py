import logging

from tom_catalogs.harvester import AbstractHarvester

from custom_code.data_services.kmt_dataservice import KMTDataService, _normalize_event_name, _to_float, _event_page_url


logger = logging.getLogger(__name__)


def get(term):
    service = KMTDataService()
    query_parameters = service.build_query_parameters({'target_name': term, 'include_photometry': False})
    results = service.query_service(query_parameters)
    events = results.get('events') or []
    return events[0] if events else {}


def get_all(term):
    service = KMTDataService()
    query_parameters = service.build_query_parameters({'target_name': term, 'include_photometry': False})
    results = service.query_service(query_parameters)
    return results.get('events') or []


class KMTHarvester(AbstractHarvester):
    name = 'KMT'

    def query(self, term):
        try:
            self.catalog_data = get(term)
        except Exception as exc:
            logger.warning('KMT query failed for term "%s": %s', term, exc)
            self.catalog_data = {}
        return self.catalog_data

    def to_target(self):
        target = super().to_target()
        event_name = _normalize_event_name(self.catalog_data.get('Event'))
        target.name = event_name or 'KMT'
        target.type = 'SIDEREAL'
        target.ra = _to_float(self.catalog_data.get('ra_deg') or self.catalog_data.get('RA_deg'))
        target.dec = _to_float(self.catalog_data.get('dec_deg') or self.catalog_data.get('Dec_deg'))
        target.epoch = 2000.0
        target.description = f'KMTNet microlensing event {event_name}' if event_name else 'KMTNet microlensing event'
        return target

    @staticmethod
    def source_url(match):
        event_name = _normalize_event_name(match.get('Event'))
        return _event_page_url(event_name) or KMTDataService.info_url
