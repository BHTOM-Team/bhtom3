import logging

from django.core.management.base import BaseCommand, CommandError
from requests.exceptions import RequestException
from tom_alerts import alerts
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Refresh each target only from the alert brokers that supplied its data.'

    def add_arguments(self, parser):
        parser.add_argument('--target_id', type=int)

    def handle(self, *args, **options):
        broker_classes = alerts.get_service_classes()
        records = ReducedDatum.objects.filter(source_name__in=broker_classes)
        target_id = options.get('target_id')
        if target_id is not None:
            if not Target.objects.filter(pk=target_id).exists():
                raise CommandError('Invalid target id provided')
            records = records.filter(target_id=target_id)

        # The upstream updater uses sources from the whole database for every
        # target, causing requests with unrelated target names as broker IDs.
        sources_by_target = {}
        for pk, source in records.order_by().values_list('target_id', 'source_name').distinct():
            sources_by_target.setdefault(pk, []).append(source)

        brokers = {}
        failures = []
        for target in Target.objects.filter(pk__in=sources_by_target).iterator():
            for source in sources_by_target[target.pk]:
                if source not in brokers:
                    brokers[source] = broker_classes[source]()
                try:
                    brokers[source].process_reduced_data(target)
                except RequestException as exc:
                    logger.warning(
                        'Broker %s refresh failed for target id=%s name=%s: %s',
                        source, target.pk, target.name, exc,
                    )
                    status = getattr(exc.response, 'status_code', None)
                    reason = f'HTTP {status}' if status is not None else type(exc).__name__
                    failures.append(f'{source} for target {target.pk} ({target.name}): {reason}')

        if failures:
            return 'Update completed with errors: ' + '; '.join(failures)
        return 'Update completed successfully'
