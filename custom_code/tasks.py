import logging
import re

from django.apps import apps
from django.conf import settings
from django.db import close_old_connections
from django.utils.module_loading import import_string
from django_tasks import task

from tom_targets.models import Target


logger = logging.getLogger(__name__)


def _get_data_service_classes():
    """
    Compatibility wrapper for TOM Toolkit versions with different
    tom_dataservices API surfaces.
    """
    try:
        from tom_dataservices.dataservices import get_data_service_classes
        return get_data_service_classes()
    except Exception:
        pass

    try:
        from tom_dataservices.dataservices import get_data_services
        return get_data_services()
    except Exception:
        pass

    data_service_choices = {}
    for app in apps.get_app_configs():
        try:
            data_services = app.data_services()
        except Exception:
            continue
        for data_service in data_services or []:
            try:
                clazz = import_string(data_service['class'])
                data_service_choices[clazz.name] = clazz
            except Exception:
                continue
    return data_service_choices


def enqueue_target_dataservices_update(target_id):
    update_target_dataservices_for_target.enqueue(target_id)


@task
def update_target_dataservices_for_target(target_id):
    close_old_connections()
    try:
        target = Target.objects.get(pk=target_id)
    except Target.DoesNotExist:
        logger.warning('Target %s not found for data service update.', target_id)
        return

    service_classes = _get_data_service_classes()
    service_names = getattr(settings, 'AUTO_QUERY_DATA_SERVICE_NAMES', None)
    if service_names:
        selected_names = tuple(service_names)
    else:
        selected_names = tuple(sorted(service_classes.keys()))

    for service_name in selected_names:
        clazz = service_classes.get(service_name)
        if clazz is None:
            logger.info('Data service "%s" not installed; skipping.', service_name)
            continue
        _run_service_for_target(target, service_name, clazz)


def _run_service_for_target(target, service_name, service_class):
    service = service_class()
    query_parameters = _build_query_parameters_for_service(target, service_name, service)

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


def _build_query_parameters_for_service(target, service_name, service):
    form_fields = {}
    try:
        form_class = service.get_form_class()
        form_fields = getattr(form_class, 'base_fields', {}) or {}
    except Exception:
        pass

    query_parameters = {'data_service': service_name}
    if 'ra' in form_fields:
        query_parameters['ra'] = target.ra
    if 'dec' in form_fields:
        query_parameters['dec'] = target.dec
    if 'include_photometry' in form_fields:
        query_parameters['include_photometry'] = True
    if 'include_spectroscopy' in form_fields:
        query_parameters['include_spectroscopy'] = True

    if 'radius_arcsec' in form_fields:
        # Conservative default; service-specific forms may override.
        query_parameters['radius_arcsec'] = 5.0

    if 'source_id' in form_fields:
        query_parameters['radius_arcsec'] = 1.0
        source_id = _extract_id_from_target(target, r'(?i)gaia\s*dr3[_\s-]*(\d+)')
        if source_id:
            query_parameters['source_id'] = source_id

    if 'dia_object_id' in form_fields:
        query_parameters['radius_arcsec'] = 5.0
        dia_object_id = _extract_id_from_target(target, r'(?i)lsst[_\s-]*(\d+)')
        if dia_object_id:
            query_parameters['dia_object_id'] = dia_object_id

    if 'alert_name' in form_fields:
        # Prefer explicit Gaia Alerts name; fallback to cone-search on target coordinates.
        query_parameters['radius_arcsec'] = max(float(query_parameters.get('radius_arcsec', 5.0)), 30.0)
        alert_name = _extract_gaia_alerts_name(target)
        if alert_name:
            query_parameters['alert_name'] = alert_name

    return query_parameters


def _iter_target_names(target):
    yield str(target.name).strip()
    for name in target.names:
        value = str(name).strip()
        if value:
            yield value


def _extract_id_from_target(target, pattern):
    for value in _iter_target_names(target):
        if value.isdigit():
            return value
        match = re.search(pattern, value)
        if match:
            return match.group(1)
    return None


def _extract_gaia_alerts_name(target):
    for value in _iter_target_names(target):
        if re.match(r'(?i)^gaia\d+[a-z]+$', value):
            return value
    return None
