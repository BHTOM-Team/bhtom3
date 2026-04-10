import logging

from django.apps import apps
from django.contrib.auth import get_user_model
from django.contrib.auth.signals import user_logged_in
from django.core.exceptions import ObjectDoesNotExist
from django.db.backends.signals import connection_created
from django.db.models.signals import post_delete, post_save, pre_delete, pre_save
from django.dispatch import receiver

from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target

from custom_code.last_photometry import refresh_target_last_photometry
from custom_code.priority import refresh_target_priority

logger = logging.getLogger(__name__)


def _import_session_utils_module():
    """Import TOM session utility module across TOM Toolkit variants."""
    try:
        from tom_common import session_utils as session_utils_module
        return session_utils_module
    except Exception:
        pass
    try:
        from tom_common.session import _utils as session_utils_module
        return session_utils_module
    except Exception:
        return None


@receiver(user_logged_in, dispatch_uid='custom_code.safe_set_cipher_on_user_logged_in')
def safe_set_cipher_on_user_logged_in(sender, request, user, **kwargs):
    """Set TOM cipher key on login only when plaintext password is available."""
    session_utils_module = _import_session_utils_module()
    if session_utils_module is None:
        logger.debug('No tom_common session utility module found; skipping cipher key setup.')
        return

    password = None
    if request is not None:
        post_data = getattr(request, 'POST', None)
        if post_data is not None:
            password = post_data.get('password')

    if not password:
        # Some auth flows (SSO, REMOTE_USER, tests) have no plaintext password in POST.
        logger.warning(
            'Login for user %s has no plaintext password in request; skipping TOM cipher key setup.',
            getattr(user, 'username', '<unknown>'),
        )
        return

    try:
        encryption_key = session_utils_module.create_cipher_encryption_key(user, password)
        session_utils_module.save_key_to_session_store(encryption_key, request.session)
    except Exception as exc:
        logger.error(
            'Could not initialize TOM cipher key for user %s: %s',
            getattr(user, 'username', '<unknown>'),
            exc,
        )


@receiver(pre_save, sender=get_user_model(), dispatch_uid='custom_code.safe_user_updated_on_user_pre_save')
def safe_user_updated_on_user_pre_save(sender, **kwargs):
    """
    Re-encrypt TOM encrypted profile fields only when a real raw password is available.

    Django may update password hashes during login, which changes user.password but sets
    user._password to None. That is not a real password change and must not trigger
    re-encryption.
    """
    user = kwargs.get('instance')
    if not user or getattr(user, 'is_anonymous', False) or getattr(user, 'username', '') == 'AnonymousUser':
        return

    try:
        old_hashed_password = sender.objects.get(id=user.id).password
    except sender.DoesNotExist:
        old_hashed_password = None

    new_hashed_password = user.password
    if new_hashed_password == old_hashed_password:
        return

    session_utils_module = _import_session_utils_module()
    if session_utils_module is None:
        logger.debug('No tom_common session utility module found; skipping re-encryption.')
        return

    raw_password = getattr(user, '_password', None)
    if not raw_password:
        logger.warning(
            'Password hash changed for user %s but raw password is unavailable; '
            'skipping TOM re-encryption (likely hash upgrade on login).',
            getattr(user, 'username', '<unknown>'),
        )
        return

    try:
        session_utils_module.reencrypt_data(user)
    except Exception as exc:
        logger.error(
            'Could not re-encrypt TOM data for user %s after password update: %s',
            getattr(user, 'username', '<unknown>'),
            exc,
        )


@receiver(post_save, sender=ReducedDatum, dispatch_uid='custom_code.update_target_last_photometry_on_save')
def update_target_last_photometry_on_save(sender, instance, created, **kwargs):
    if not created or instance.data_type != 'photometry' or instance.target_id is None:
        return

    refresh_target_last_photometry(instance.target_id)
    refresh_target_priority(instance.target_id)


@receiver(post_delete, sender=ReducedDatum, dispatch_uid='custom_code.update_target_last_photometry_on_delete')
def update_target_last_photometry_on_delete(sender, instance, **kwargs):
    if instance.data_type != 'photometry' or instance.target_id is None:
        return

    refresh_target_last_photometry(instance.target_id)
    refresh_target_priority(instance.target_id)


@receiver(post_save, sender=Target, dispatch_uid='custom_code.update_target_priority_on_target_save')
def update_target_priority_on_target_save(sender, instance, **kwargs):
    if instance is None or instance.pk is None:
        return
    refresh_target_priority(instance.pk)


def _raw_delete_related_rows(app_label, model_name, **filters):
    """
    Delete dependent rows before target deletion.

    Django's collector should cascade these relations, but some TOM models end up
    with deferred foreign keys on SQLite. Removing the child rows explicitly keeps
    target deletion from failing at commit time.
    """
    try:
        model = apps.get_model(app_label, model_name)
    except LookupError:
        return

    queryset = model.objects.filter(**filters)
    if not queryset.exists():
        return

    raw_delete = getattr(queryset, '_raw_delete', None)
    if raw_delete is not None:
        raw_delete(queryset.db)
        return

    queryset.delete()


@receiver(pre_delete, sender=Target, dispatch_uid='custom_code.cleanup_target_relations_on_target_delete')
def cleanup_target_relations_on_target_delete(sender, instance, **kwargs):
    if instance is None or instance.pk is None:
        return

    target_id = instance.pk
    _raw_delete_related_rows('tom_dataproducts', 'ReducedDatum', target_id=target_id)
    _raw_delete_related_rows('tom_dataproducts', 'DataProduct', target_id=target_id)
    _raw_delete_related_rows('tom_observations', 'ObservationRecord', target_id=target_id)
    _raw_delete_related_rows('tom_targets', 'TargetExtra', target_id=target_id)
    _raw_delete_related_rows('tom_targets', 'PersistentShare', target_id=target_id)

    try:
        instance.aliases.all().delete()
    except ObjectDoesNotExist:
        pass

    try:
        transit_ephemeris = instance.transit_ephemeris
    except ObjectDoesNotExist:
        transit_ephemeris = None
    if transit_ephemeris is not None:
        transit_ephemeris.delete()


@receiver(connection_created, dispatch_uid='custom_code.sqlite_pragmas')
def configure_sqlite_pragmas(sender, connection, **kwargs):
    if connection.vendor != 'sqlite':
        return
    with connection.cursor() as cursor:
        cursor.execute('PRAGMA journal_mode=WAL;')
        cursor.execute('PRAGMA busy_timeout=30000;')
