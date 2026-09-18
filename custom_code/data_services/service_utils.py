import logging
import re
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


def compact_target_name(value):
    return re.sub(r'[^a-z0-9]', '', normalize_target_name(value).casefold())


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
        compact_name = compact_target_name(target_name)
        if compact_name:
            for candidate in Target.objects.prefetch_related('aliases').all():
                candidate_names = [candidate.name]
                candidate_names.extend(alias.name for alias in candidate.aliases.all())
                if any(compact_target_name(name) == compact_name for name in candidate_names):
                    target = candidate
                    break
    if target is None:
        logger.debug('Could not resolve target name "%s" to a local BHTOM target.', target_name)
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


ORIGIN_RA_KEY = 'origin_ra'
ORIGIN_DEC_KEY = 'origin_dec'

# Per-epoch sky position is rounded before storage: 1e-7 deg is 0.36 mas, far finer than the
# ~50-100 mas astrometric precision of the surveys we ingest, and it keeps the value JSON small.
ORIGIN_COORD_PRECISION = 7


def _coerce_coordinate(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if value != value or value in (float('inf'), float('-inf')):  # NaN / inf
        return None
    return value


def add_origin_coordinates(value, ra, dec):

    ra = _coerce_coordinate(ra)
    dec = _coerce_coordinate(dec)
    if ra is None or dec is None:
        return value
    if not (0.0 <= ra <= 360.0) or not (-90.0 <= dec <= 90.0):
        return value
    value[ORIGIN_RA_KEY] = round(ra, ORIGIN_COORD_PRECISION)
    value[ORIGIN_DEC_KEY] = round(dec, ORIGIN_COORD_PRECISION)
    return value


def _identity(value, identity_keys):
    return tuple(value.get(key) for key in identity_keys)


def upsert_reduced_datums(target, data_type, source_name, source_location, datums,
                          identity_keys=('filter', 'magnitude')):
    """Create datums, upgrading rows that were ingested before a value key existed.

    ReducedDatum uniqueness is (target, data_type, timestamp, value), and `value` is compared as
    a whole dict. So simply adding a key like origin_ra to a service's output would make every
    previously stored point look new and silently double the light curve. Instead we match on
    timestamp plus a few identifying keys, and fill in only the keys the stored row is missing.

    Uses bulk operations: ZTF alone can return thousands of points per target.
    Returns (created_count, updated_count).
    """
    from tom_dataproducts.models import ReducedDatum

    datums = [
        datum for datum in (datums or [])
        if datum.get('timestamp') is not None and isinstance(datum.get('value'), dict)
    ]
    if not datums:
        return 0, 0

    existing_by_key = {}
    for existing in ReducedDatum.objects.filter(
        target=target,
        data_type=data_type,
        timestamp__in={datum['timestamp'] for datum in datums},
    ):
        if not isinstance(existing.value, dict):
            continue
        key = (existing.timestamp, _identity(existing.value, identity_keys))
        existing_by_key.setdefault(key, existing)

    to_create = []
    to_update = {}
    for datum in datums:
        value = datum['value']
        key = (datum['timestamp'], _identity(value, identity_keys))
        existing = existing_by_key.get(key)
        if existing is None:
            new_row = ReducedDatum(
                target=target,
                data_type=data_type,
                timestamp=datum['timestamp'],
                value=value,
                source_name=source_name,
                source_location=source_location,
            )
            to_create.append(new_row)
            # A repeated point later in the same batch must match this row, not create another.
            existing_by_key[key] = new_row
            continue

        missing = {k: v for k, v in value.items() if k not in existing.value}
        if not missing:
            continue
        existing.value = {**existing.value, **missing}
        if existing.pk is not None:
            to_update[existing.pk] = existing

    if to_create:
        ReducedDatum.objects.bulk_create(to_create, batch_size=500)
    if to_update:
        ReducedDatum.objects.bulk_update(list(to_update.values()), ['value'], batch_size=500)

    return len(to_create), len(to_update)
