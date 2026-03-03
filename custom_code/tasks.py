import logging
import re
from concurrent.futures import ThreadPoolExecutor

from django.db import close_old_connections

from tom_dataservices.dataservices import get_data_service_classes
from tom_targets.models import Target


logger = logging.getLogger(__name__)

_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix='dataservices')
_DEFAULT_SERVICE_NAMES = ('Gaia DR3 DataService', 'LSST DataService')


def enqueue_target_dataservices_update(target_id):
    _EXECUTOR.submit(update_target_dataservices_for_target, target_id)


def update_target_dataservices_for_target(target_id):
    close_old_connections()
    try:
        target = Target.objects.get(pk=target_id)
    except Target.DoesNotExist:
        logger.warning('Target %s not found for data service update.', target_id)
        return

    service_classes = get_data_service_classes()
    for service_name in _DEFAULT_SERVICE_NAMES:
        clazz = service_classes.get(service_name)
        if clazz is None:
            logger.info('Data service "%s" not installed; skipping.', service_name)
            continue
        _run_service_for_target(target, service_name, clazz)


def _run_service_for_target(target, service_name, service_class):
    service = service_class()
    query_parameters = {
        'data_service': service_name,
        'ra': target.ra,
        'dec': target.dec,
        'include_photometry': True,
    }
    if service_name == 'Gaia DR3 DataService':
        query_parameters['radius_arcsec'] = 1.0
        source_id = _extract_id_from_names(target.names, r'(?i)gaia\s*dr3[_\s-]*(\d+)')
        if source_id:
            query_parameters['source_id'] = source_id
    elif service_name == 'LSST DataService':
        query_parameters['radius_arcsec'] = 5.0
        dia_object_id = _extract_id_from_names(target.names, r'(?i)lsst[_\s-]*(\d+)')
        if dia_object_id:
            query_parameters['dia_object_id'] = dia_object_id

    try:
        built_parameters = service.build_query_parameters(query_parameters)
        target_results = service.query_targets(built_parameters)
    except Exception as exc:
        logger.warning('Data service "%s" failed for target %s: %s', service_name, target.name, exc)
        return

    if not target_results:
        logger.info('Data service "%s": no matching results for target %s.', service_name, target.name)
        return

    aliases_added = 0
    for result in target_results:
        for alias in result.get('aliases', []):
            _, created = target.aliases.get_or_create(name=str(alias))
            if created:
                aliases_added += 1
        reduced_datums = result.get('reduced_datums')
        if reduced_datums:
            service.to_reduced_datums(target, reduced_datums)

    logger.info('Data service "%s" update finished for target %s (aliases added: %s).',
                service_name, target.name, aliases_added)


def _extract_id_from_names(names, pattern):
    for name in names:
        value = str(name).strip()
        if value.isdigit():
            return value
        match = re.search(pattern, value)
        if match:
            return match.group(1)
    return None
