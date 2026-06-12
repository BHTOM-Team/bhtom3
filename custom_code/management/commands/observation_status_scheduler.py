import time

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from custom_code.tasks import run_observation_status_update


class Command(BaseCommand):
    help = "Refresh observation statuses at a fixed interval."

    def add_arguments(self, parser):
        parser.add_argument(
            "--interval",
            type=int,
            default=getattr(settings, "OBSERVATION_STATUS_UPDATE_INTERVAL_SECONDS", 180),
            help="Seconds between status updates (default: 180).",
        )
        parser.add_argument(
            "--run-once",
            action="store_true",
            help="Run one status update and exit.",
        )

    def handle(self, *args, **options):
        interval = max(1, int(options["interval"]))
        run_once = bool(options["run_once"])

        while True:
            result = run_observation_status_update()
            self.stdout.write(
                self.style.SUCCESS(
                    f"Finished observation status update at {timezone.now().isoformat()}: {result}"
                )
            )

            if run_once:
                return

            time.sleep(interval)
