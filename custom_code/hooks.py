import logging

from django.conf import settings
from django.db import transaction

from tom_common.hooks import target_post_save as default_target_post_save

from custom_code.target_derivations import refresh_target_derived_fields

logger = logging.getLogger(__name__)


def target_post_save(target, created):
    """
    Runs default target hook behavior and then asynchronously updates
    configured DataServices for newly-created targets.
    """
    default_target_post_save(target=target, created=created)

    try:
        refresh_target_derived_fields(target.id)
    except Exception as exc:
        logger.warning('Could not refresh derived target fields for target %s: %s', target.id, exc)

    if not created:
        return

    transaction.on_commit(lambda: _enqueue_safely(target.id))


def _enqueue_safely(target_id):
    try:
        from custom_code.tasks import enqueue_target_dataservices_update
        if getattr(settings, 'AUTO_QUERY_DATA_SERVICES_ON_TARGET_CREATE', True):
            enqueue_target_dataservices_update(target_id)
    except Exception as exc:
        logger.warning('Could not enqueue target-create background updates for target %s: %s', target_id, exc)
