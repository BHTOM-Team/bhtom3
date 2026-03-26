from django.core.management.base import BaseCommand
from tom_targets.models import Target

from custom_code.tasks import enqueue_target_dataservices_update, run_target_dataservices_for_target


class Command(BaseCommand):
    help = (
        "Run DataServices refresh for targets above an importance threshold. "
        "The per-target task also refreshes last photometry and sun separation."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--importance-gt",
            type=float,
            default=0.0,
            help="Refresh only targets with importance greater than this value (default: 0).",
        )
        parser.add_argument(
            "--enqueue",
            action="store_true",
            help="Enqueue background jobs instead of running synchronously in this command.",
        )

    def handle(self, *args, **options):
        threshold = float(options["importance_gt"])
        enqueue = bool(options["enqueue"])

        queryset = Target.objects.filter(importance__gt=threshold).order_by("pk")
        target_ids = list(queryset.values_list("pk", flat=True))
        total = len(target_ids)

        if total == 0:
            self.stdout.write(self.style.WARNING("No targets matched the importance threshold."))
            return

        processed = 0
        for target_id in target_ids:
            if enqueue:
                enqueue_target_dataservices_update(target_id, include_create_only=False)
            else:
                run_target_dataservices_for_target(target_id, include_create_only=False)
            processed += 1

        mode = "enqueued" if enqueue else "processed"
        self.stdout.write(
            self.style.SUCCESS(
                f"{mode.capitalize()} {processed} targets with importance > {threshold}."
            )
        )
