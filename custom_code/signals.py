import logging

from django.contrib.auth.signals import user_logged_in
from django.dispatch import receiver


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
