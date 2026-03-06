import logging

from django.conf import settings
from django.db import transaction

from tom_common.hooks import target_post_save as default_target_post_save

logger = logging.getLogger(__name__)


def target_post_save(target, created):
    """
    Runs default target hook behavior and then asynchronously updates photometry
    via configured DataServices for newly-created targets.
    """
    default_target_post_save(target=target, created=created)

    if not created:
        return

    if not getattr(settings, 'AUTO_QUERY_DATA_SERVICES_ON_TARGET_CREATE', True):
        return

    transaction.on_commit(lambda: _enqueue_safely(target.id))


def _enqueue_safely(target_id):
    try:
        from custom_code.tasks import enqueue_target_dataservices_update
        enqueue_target_dataservices_update(target_id)
    except Exception as exc:
        logger.warning('Could not enqueue data service update for target %s: %s', target_id, exc)
