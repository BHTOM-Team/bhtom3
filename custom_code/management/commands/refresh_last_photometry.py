from django.core.management.base import BaseCommand
from tom_targets.models import Target

from custom_code.last_photometry import refresh_target_last_photometry


class Command(BaseCommand):
    help = "Recompute mag_last/mjd_last/filter_last from photometry ReducedDatum."

    def add_arguments(self, parser):
        parser.add_argument("--target-id", type=int, help="Refresh only one target id.")

    def handle(self, *args, **options):
        target_id = options.get("target_id")
        if target_id:
            refresh_target_last_photometry(target_id)
            self.stdout.write(self.style.SUCCESS(f"Refreshed target {target_id}"))
            return

        count = 0
        for pk in Target.objects.values_list("pk", flat=True).iterator():
            refresh_target_last_photometry(pk)
            count += 1
        self.stdout.write(self.style.SUCCESS(f"Refreshed {count} targets"))
