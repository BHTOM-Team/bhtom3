import logging

from tom_catalogs.harvester import AbstractHarvester

from custom_code.data_services.ogle_ocvs_dataservice import (
    OGLE_OCVS_BASE_URL,
    OGLEOCVSDataService,
    _normalize_target_name,
    _object_url,
    _to_float,
)


logger = logging.getLogger(__name__)


def get(term):
    matches = get_all(term)
    return matches[0] if matches else {}


def get_all(term):
    service = OGLEOCVSDataService()
    parameters = service.build_query_parameters({'target_name': term, 'include_photometry': False})
    return service.query_service(parameters).get('objects') or []


class OGLEOCVSHarvester(AbstractHarvester):
    name = 'OGLE OCVS'

    def query(self, term):
        try:
            self.catalog_data = get(term)
        except Exception as exc:
            logger.warning('OGLE OCVS query failed for term "%s": %s', term, exc)
            self.catalog_data = {}
        return self.catalog_data

    def to_target(self):
        target = super().to_target()
        name = _normalize_target_name(self.catalog_data.get('name'))
        target.name = name or 'OGLE-OCVS'
        target.type = 'SIDEREAL'
        target.ra = _to_float(self.catalog_data.get('ra'))
        target.dec = _to_float(self.catalog_data.get('dec'))
        target.epoch = 2000.0
        target.classification = 'Variable star-other'
        target.description = 'OGLE variable star'
        return target

    @staticmethod
    def source_url(match):
        name = _normalize_target_name(match.get('name'))
        return _object_url(name) if name else OGLE_OCVS_BASE_URL
