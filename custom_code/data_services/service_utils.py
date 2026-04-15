import logging

from django.db.models import Q
from tom_targets.models import Target


logger = logging.getLogger(__name__)

TARGET_NAME_HELP_TEXT = 'Optional. Search the data service by the catalog name.'


def normalize_target_name(value):
    return str(value or '').strip()


def resolve_target_by_name(target_name):
    target_name = normalize_target_name(target_name)
    if not target_name:
        return None

    target = (
        Target.objects
        .filter(Q(name__iexact=target_name) | Q(aliases__name__iexact=target_name))
        .distinct()
        .first()
    )
    if target is None:
        logger.info('Could not resolve target name "%s" to a local BHTOM target.', target_name)
    return target


def resolve_query_coordinates(parameters):
    target_name = normalize_target_name(parameters.get('target_name'))
    ra = parameters.get('ra')
    dec = parameters.get('dec')

    if target_name and (ra in (None, '') or dec in (None, '')):
        target = resolve_target_by_name(target_name)
        if target is not None:
            if ra in (None, ''):
                ra = target.ra
            if dec in (None, ''):
                dec = target.dec

    return target_name, ra, dec
