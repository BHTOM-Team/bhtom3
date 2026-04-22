import logging
import re

from django.apps import apps
from django.conf import settings
from django.db import IntegrityError
from django.db import close_old_connections
from django.utils.module_loading import import_string
from django_tasks import task

from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName
from custom_code.last_photometry import refresh_target_last_photometry
from custom_code.models import TargetAliasInfo, TransitEphemeris
from custom_code.priority import refresh_target_priority
from custom_code.sun_separation import refresh_target_sun_separation


logger = logging.getLogger(__name__)


def _normalize_alias_result(alias):
    if isinstance(alias, dict):
        name = str(alias.get('name') or '').strip()
        url = str(alias.get('url') or '').strip()
        source_name = str(alias.get('source_name') or '').strip()
        return {'name': name, 'url': url, 'source_name': source_name}
    name = str(alias or '').strip()
    return {'name': name, 'url': '', 'source_name': ''}


def _resolve_alias_url(alias_data, result, service):
    explicit_url = str(alias_data.get('url') or '').strip()
    if explicit_url:
        return explicit_url

    result_url = str((result or {}).get('source_location') or '').strip()
    if result_url:
        return result_url

    query_results = getattr(service, 'query_results', None) or {}
    query_url = str(query_results.get('source_location') or '').strip()
    if query_url:
        return query_url

    return str(getattr(service, 'info_url', '') or '').strip()


def _count_returned_reduced_datums(reduced_datums):
    if not reduced_datums:
        return 0
    total = 0
    for data in reduced_datums.values():
        try:
            total += len(data)
        except TypeError:
            continue
    return total


def _get_or_create_target_alias(target, alias_name):
    alias_name = str(alias_name or '').strip()
    if not alias_name:
        return None, False
    if alias_name.casefold() == str(target.name or '').strip().casefold():
        logger.info('Skipping alias "%s" for target %s because it matches the primary target name.', alias_name, target.name)
        return None, False

    alias_obj = TargetName.objects.filter(name=alias_name).first()
    if alias_obj is not None:
        if alias_obj.target_id != target.id:
            logger.warning(
                'Skipping alias "%s" for target %s because it already belongs to target %s.',
                alias_name,
                target.name,
                alias_obj.target_id,
            )
            return None, False
        return alias_obj, False

    try:
        return TargetName.objects.create(target=target, name=alias_name), True
    except IntegrityError:
        alias_obj = TargetName.objects.filter(name=alias_name).first()
        if alias_obj is None:
            raise
        if alias_obj.target_id != target.id:
            logger.warning(
                'Skipping alias "%s" for target %s because it was created concurrently for target %s.',
                alias_name,
                target.name,
                alias_obj.target_id,
            )
            return None, False
        return alias_obj, False


def _cleanup_moa_aliases(target, result, service_name):
    if service_name != 'MOA':
        return

    aliases = result.get('aliases') or []
    canonical_names = {
        str(alias.get('name') if isinstance(alias, dict) else alias).strip()
        for alias in aliases
        if str(alias.get('name') if isinstance(alias, dict) else alias).strip()
    }
    canonical_names = {name for name in canonical_names if name.upper().startswith('MOA-')}
    if not canonical_names:
        return

    alias_url = str((result or {}).get('source_location') or '').strip()
    for canonical_name in canonical_names:
        bare_name = canonical_name[4:]

        canonical_alias = target.aliases.filter(name=canonical_name).first()
        if canonical_alias is not None and alias_url:
            TargetAliasInfo.objects.update_or_create(
                target_name=canonical_alias,
                defaults={'url': alias_url, 'source_name': service_name},
            )

        stale_alias = target.aliases.filter(name=bare_name).first()
        if stale_alias is not None:
            stale_alias.delete()


def _cleanup_ogle_aliases(target, result, service_name):
    if service_name != 'OGLEEWS':
        return

    aliases = result.get('aliases') or []
    canonical_names = {
        str(alias.get('name') if isinstance(alias, dict) else alias).strip()
        for alias in aliases
        if str(alias.get('name') if isinstance(alias, dict) else alias).strip()
    }
    canonical_names = {name for name in canonical_names if name.upper().startswith('OGLE-')}
    if not canonical_names:
        return

    alias_url = str((result or {}).get('source_location') or '').strip()
    for canonical_name in canonical_names:
        bare_name = canonical_name[5:]

        canonical_alias = target.aliases.filter(name=canonical_name).first()
        if canonical_alias is not None and alias_url:
            TargetAliasInfo.objects.update_or_create(
                target_name=canonical_alias,
                defaults={'url': alias_url, 'source_name': service_name},
            )

        stale_alias = target.aliases.filter(name=bare_name).first()
        if stale_alias is not None:
            stale_alias.delete()


def _cleanup_wise_aliases(target, result, service_name):
    if service_name not in {'AllWISE', 'NeoWISE'}:
        return

    current_alias_names = {
        str(alias.get('name') if isinstance(alias, dict) else alias).strip()
        for alias in (result or {}).get('aliases') or []
        if str(alias.get('name') if isinstance(alias, dict) else alias).strip()
    }

    stale_patterns = (
        r'(?i)^allwise\+j',
        r'(?i)^neowise\+j',
    )
    for alias in list(target.aliases.all()):
        alias_name = str(alias.name or '').strip()
        is_stale_generated = any(re.match(pattern, alias_name) for pattern in stale_patterns)
        is_stale_literal = alias_name.upper() == 'WISE'
        if (is_stale_generated or is_stale_literal) and alias_name not in current_alias_names:
            alias.delete()

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


def enqueue_target_dataservices_update(target_id, include_create_only=True, force_all_services=False):
    update_target_dataservices_for_target.enqueue(target_id, include_create_only, force_all_services)


def run_target_dataservices_for_target(target_id, include_create_only=True, force_all_services=False):
    close_old_connections()
    try:
        target = Target.objects.get(pk=target_id)
    except Target.DoesNotExist:
        logger.warning('Target %s not found for data service update.', target_id)
        return

    service_classes = _get_data_service_classes()
    service_names = getattr(settings, 'AUTO_QUERY_DATA_SERVICE_NAMES', None)
    if service_names and not force_all_services:
        selected_names = tuple(service_names)
    else:
        selected_names = tuple(sorted(service_classes.keys()))

    for service_name in selected_names:
        clazz = service_classes.get(service_name)
        if clazz is None:
            logger.info('Data service "%s" not installed; skipping.', service_name)
            continue
        if not _service_enabled_for_run(clazz, include_create_only=include_create_only):
            logger.info(
                'Data service "%s" is configured for target-create only; '
                'skipping in recurring refresh mode.',
                service_name,
            )
            continue
        _run_service_for_target(target, service_name, clazz, force_all_services=force_all_services)

    # Ensure summary fields are refreshed once after all DataServices finish,
    # even when all inserted rows were duplicates (get_or_create(created=False)).
    try:
        refresh_target_last_photometry(target.id)
    except Exception as exc:
        logger.warning(
            'Could not refresh last photometry fields for target %s at task end: %s',
            target.name,
            exc,
        )
    try:
        refresh_target_priority(target.id)
    except Exception as exc:
        logger.warning(
            'Could not refresh priority for target %s at task end: %s',
            target.name,
            exc,
        )
    try:
        refresh_target_sun_separation(target.id)
    except Exception as exc:
        logger.warning(
            'Could not refresh sun separation for target %s at task end: %s',
            target.name,
            exc,
        )


@task
def update_target_dataservices_for_target(target_id, include_create_only=True, force_all_services=False):
    run_target_dataservices_for_target(
        target_id,
        include_create_only=include_create_only,
        force_all_services=force_all_services,
    )


def _run_service_for_target(target, service_name, service_class, force_all_services=False):
    service = service_class()
    query_parameters = _build_query_parameters_for_service(
        target,
        service_name,
        service,
        force=force_all_services,
    )

    try:
        built_parameters = service.build_query_parameters(query_parameters)
        target_results = service.query_targets(built_parameters)
    except Exception as exc:
        logger.warning('Data service "%s" failed for target %s: %s', service_name, target.name, exc)
        return

    if not target_results:
        logger.info(
            'Data service "%s" finished for target %s: no match (results=0, aliases_found=0, aliases_added=0, alias_urls_updated=0, datapoints_returned=0, datapoints_added=0).',
            service_name,
            target.name,
        )
        return

    aliases_added = 0
    aliases_found = 0
    alias_urls_updated = 0
    datapoints_returned = 0
    datapoints_added = 0
    for result in target_results:
        target_updates = result.get('target_updates') or {}
        if target_updates:
            Target.objects.filter(pk=target.pk).update(**target_updates)
            for field_name, value in target_updates.items():
                setattr(target, field_name, value)
        transit_ephemeris_updates = result.get('transit_ephemeris_updates') or {}
        if transit_ephemeris_updates:
            TransitEphemeris.objects.update_or_create(target=target, defaults=transit_ephemeris_updates)
        for alias in result.get('aliases', []):
            alias_data = _normalize_alias_result(alias)
            if not alias_data['name']:
                continue
            aliases_found += 1
            alias_obj, created = _get_or_create_target_alias(target, alias_data['name'])
            if alias_obj is None:
                continue
            alias_url = _resolve_alias_url(alias_data, result, service)
            source_name = alias_data['source_name'] or service_name
            if alias_url or source_name:
                TargetAliasInfo.objects.update_or_create(
                    target_name=alias_obj,
                    defaults={'url': alias_url, 'source_name': source_name},
                )
                alias_urls_updated += 1
            if created:
                aliases_added += 1
        _cleanup_ogle_aliases(target, result, service_name)
        _cleanup_moa_aliases(target, result, service_name)
        _cleanup_wise_aliases(target, result, service_name)
        reduced_datums = result.get('reduced_datums')
        if reduced_datums:
            datapoints_returned += _count_returned_reduced_datums(reduced_datums)
            before_count = ReducedDatum.objects.filter(target=target, source_name=service_name).count()
            service.to_reduced_datums(target, reduced_datums)
            after_count = ReducedDatum.objects.filter(target=target, source_name=service_name).count()
            datapoints_added += max(0, after_count - before_count)
            try:
                refresh_target_last_photometry(target.id)
            except Exception as exc:
                logger.warning(
                    'Could not refresh last photometry fields for target %s after service "%s": %s',
                    target.name,
                    service_name,
                    exc,
                )

    logger.info(
        'Data service "%s" finished for target %s: success (results=%s, aliases_found=%s, aliases_added=%s, alias_urls_updated=%s, datapoints_returned=%s, datapoints_added=%s).',
        service_name,
        target.name,
        len(target_results),
        aliases_found,
        aliases_added,
        alias_urls_updated,
        datapoints_returned,
        datapoints_added,
    )


def _service_enabled_for_run(service_class, include_create_only=True):
    if include_create_only:
        return True
    return bool(getattr(service_class, 'update_on_daily_refresh', True))


def _build_query_parameters_for_service(target, service_name, service, force=False):
    form_fields = {}
    try:
        form_class = service.get_form_class()
        form_fields = getattr(form_class, 'base_fields', {}) or {}
    except Exception:
        pass

    query_parameters = {'data_service': service_name}
    query_parameters['target_id'] = target.id
    query_parameters['force'] = bool(force)
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

    if service_name == 'Simbad':
        query_parameters['radius_arcsec'] = 3.0

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

    if 'target_name' in form_fields and service_name == 'OGLEEWS':
        ogle_ews_name = _extract_ogle_ews_name(target)
        if ogle_ews_name:
            query_parameters['target_name'] = ogle_ews_name
    elif 'target_name' in form_fields and service_name == 'KMT':
        kmt_name = _extract_kmt_name(target)
        if kmt_name:
            query_parameters['target_name'] = kmt_name
    elif 'target_name' in form_fields and service_name == 'ExoClock':
        query_parameters['radius_arcsec'] = max(float(query_parameters.get('radius_arcsec', 30.0)), 30.0)
        query_parameters['target_name'] = target.name
        query_parameters['target_names'] = list(dict.fromkeys(_iter_target_names(target)))
    elif service_name == 'ExoClock':
        query_parameters['radius_arcsec'] = max(float(query_parameters.get('radius_arcsec', 30.0)), 30.0)
        query_parameters['target_names'] = list(dict.fromkeys(_iter_target_names(target)))

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


def _extract_ogle_ews_name(target):
    for value in _iter_target_names(target):
        match = re.match(r'(?i)^(?:OGLE[-\s]?)?(\d{4}-[A-Z]{3}-\d{4})$', value.strip())
        if match:
            return match.group(1).upper()
    return None


def _extract_kmt_name(target):
    for value in _iter_target_names(target):
        match = re.match(r'(?i)^(?:KMT[-\s]?)?(\d{4}-BLG-\d{1,5})$', value.strip())
        if match:
            return f'KMT-{match.group(1).upper()}'
    return None
