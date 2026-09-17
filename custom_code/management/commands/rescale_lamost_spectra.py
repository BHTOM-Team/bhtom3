from django.core.management.base import BaseCommand
from tom_dataproducts.models import ReducedDatum

from custom_code.data_services.lamost_dataservice import (
    LAMOST_FLUX_SCALE,
    LAMOST_UNSCALED_FLUX_THRESHOLD,
    needs_lamost_flux_rescale,
)


class Command(BaseCommand):
    help = (
        "Rescale LAMOST spectroscopy ReducedDatums that were ingested with the raw FITS FLUX "
        "values labelled as erg/s/cm2/Angstrom. Multiplies flux by 1e-17 so they share a flux "
        "axis with SDSS/DESI/ESO spectra instead of swamping them."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report what would change without writing.",
        )
        parser.add_argument(
            "--target-id",
            type=int,
            help="Only rescale datums for one target id.",
        )

    def handle(self, *args, **options):
        dry_run = options.get("dry_run")
        target_id = options.get("target_id")

        queryset = ReducedDatum.objects.filter(source_name="LAMOST", data_type="spectroscopy")
        if target_id:
            queryset = queryset.filter(target_id=target_id)

        rescaled = 0
        skipped = 0
        for datum in queryset.iterator():
            value = datum.value
            if not needs_lamost_flux_rescale(value):
                skipped += 1
                continue
            value = dict(value)
            value["flux"] = [
                flux * LAMOST_FLUX_SCALE if isinstance(flux, (int, float)) else flux
                for flux in value["flux"]
            ]
            rescaled += 1
            peak = max((f for f in value["flux"] if isinstance(f, (int, float))), default=0.0)
            self.stdout.write(
                f"datum {datum.pk} (target {datum.target_id}, {value.get('spectrum_type')}): "
                f"peak flux now {peak:.4g}"
            )
            if not dry_run:
                datum.value = value
                datum.save(update_fields=["value"])

        verb = "Would rescale" if dry_run else "Rescaled"
        self.stdout.write(
            self.style.SUCCESS(f"{verb} {rescaled} LAMOST spectra; left {skipped} unchanged "
                               f"(already below {LAMOST_UNSCALED_FLUX_THRESHOLD:g}).")
        )
