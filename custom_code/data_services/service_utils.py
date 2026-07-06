import logging
import socket

from django.conf import settings
from django.db.models import Q
from tom_targets.models import Target


logger = logging.getLogger(__name__)

TARGET_NAME_HELP_TEXT = 'Optional. Search the data service by the catalog name.'
DATA_SERVICE_CONNECT_TIMEOUT = getattr(settings, 'DATA_SERVICE_CONNECT_TIMEOUT', 10)
DATA_SERVICE_READ_TIMEOUT = getattr(settings, 'DATA_SERVICE_READ_TIMEOUT', 60)
DATA_SERVICE_HTTP_TIMEOUT = (DATA_SERVICE_CONNECT_TIMEOUT, DATA_SERVICE_READ_TIMEOUT)


def configure_data_service_timeouts():
    timeout = DATA_SERVICE_READ_TIMEOUT
    socket.setdefaulttimeout(timeout)

    astroquery_modules = (
        ('astroquery.gaia', 'Gaia'),
        ('astroquery.vizier', 'Vizier'),
        ('astroquery.simbad', 'Simbad'),
        ('astroquery.mast', 'Catalogs'),
        ('astroquery.ipac.irsa', 'Irsa'),
        ('astroquery.sdss', 'SDSS'),
        ('astroquery.esa.hubble', 'ESAHubble'),
    )
    for module_name, attr_name in astroquery_modules:
        try:
            module = __import__(module_name, fromlist=[attr_name, 'conf'])
            conf = getattr(module, 'conf', None)
            if conf is not None and hasattr(conf, 'timeout'):
                conf.timeout = timeout
            service = getattr(module, attr_name, None)
            if service is not None and hasattr(service, 'TIMEOUT'):
                service.TIMEOUT = timeout
        except Exception:
            logger.debug('Could not configure timeout for %s.%s', module_name, attr_name, exc_info=True)


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
