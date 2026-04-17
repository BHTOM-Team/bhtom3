import logging

from tom_catalogs.harvester import AbstractHarvester

from custom_code.data_services.moa_dataservice import MOADataService, _normalize_event_name, _to_float


logger = logging.getLogger(__name__)


def get(term):
    service = MOADataService()
    query_parameters = service.build_query_parameters({'target_name': term, 'include_photometry': False})
    results = service.query_service(query_parameters)
    events = results.get('events') or []
    return events[0] if events else {}


def get_all(term):
    service = MOADataService()
    query_parameters = service.build_query_parameters({'target_name': term, 'include_photometry': False})
    results = service.query_service(query_parameters)
    return results.get('events') or []


class MOAHarvester(AbstractHarvester):
    name = 'MOA'

    def query(self, term):
        try:
            self.catalog_data = get(term)
        except Exception as exc:
            logger.warning('MOA query failed for term "%s": %s', term, exc)
            self.catalog_data = {}
        return self.catalog_data

    def to_target(self):
        target = super().to_target()
        event_name = _normalize_event_name(self.catalog_data.get('Event'))
        target.name = event_name or 'MOA'
        target.type = 'SIDEREAL'
        target.ra = _to_float(self.catalog_data.get('ra_deg') or self.catalog_data.get('RA_deg'))
        target.dec = _to_float(self.catalog_data.get('dec_deg') or self.catalog_data.get('Dec_deg'))
        target.epoch = 2000.0
        target.description = f'MOA microlensing event {event_name}' if event_name else 'MOA microlensing event'
        return target

    @staticmethod
    def source_url(match):
        event_name = _normalize_event_name(match.get('Event'))
        if not event_name:
            return MOADataService.info_url
        return f'{MOADataService.info_url}/event/{event_name.removeprefix("MOA-")}'
