import time

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from custom_code.tasks import enqueue_observation_status_update


class Command(BaseCommand):
    help = "Enqueue observation status updates at a fixed interval for db_worker to process."

    def add_arguments(self, parser):
        parser.add_argument(
            "--interval",
            type=int,
            default=getattr(settings, "OBSERVATION_STATUS_UPDATE_INTERVAL_SECONDS", 180),
            help="Seconds between enqueued status updates (default: 180).",
        )
        parser.add_argument(
            "--run-once",
            action="store_true",
            help="Enqueue one status update and exit.",
        )

    def handle(self, *args, **options):
        interval = max(1, int(options["interval"]))
        run_once = bool(options["run_once"])

        while True:
            enqueue_observation_status_update()
            self.stdout.write(
                self.style.SUCCESS(
                    f"Enqueued observation status update at {timezone.now().isoformat()}."
                )
            )

            if run_once:
                return

            time.sleep(interval)
