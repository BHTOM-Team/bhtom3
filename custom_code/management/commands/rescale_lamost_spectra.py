import math

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

    def _scaled_twin(self, datum, scaled_flux):
        """An already-correct copy of this spectrum, stored separately because its value differed.

        Happens when a worker running pre-fix code ingested the raw spectrum and a later run with
        current code added the scaled one alongside it.
        """
        siblings = ReducedDatum.objects.filter(
            target_id=datum.target_id,
            source_name="LAMOST",
            data_type="spectroscopy",
            timestamp=datum.timestamp,
        ).exclude(pk=datum.pk)
        for sibling in siblings:
            value = sibling.value if isinstance(sibling.value, dict) else {}
            if needs_lamost_flux_rescale(value):
                continue
            if any(value.get(key) != datum.value.get(key) for key in ("spectrum_type", "source_id", "arm")):
                continue
            if value.get("wavelength") != datum.value.get("wavelength"):
                continue
            flux = value.get("flux") or []
            if len(flux) != len(scaled_flux):
                continue
            if all(
                (a is None and b is None)
                or (isinstance(a, (int, float)) and isinstance(b, (int, float)) and math.isclose(a, b, rel_tol=1e-5, abs_tol=1e-30))
                for a, b in zip(flux, scaled_flux)
            ):
                return sibling
        return None

    def handle(self, *args, **options):
        dry_run = options.get("dry_run")
        target_id = options.get("target_id")

        queryset = ReducedDatum.objects.filter(source_name="LAMOST", data_type="spectroscopy")
        if target_id:
            queryset = queryset.filter(target_id=target_id)

        rescaled = 0
        removed = 0
        skipped = 0
        for datum in list(queryset):
            value = datum.value
            if not needs_lamost_flux_rescale(value):
                skipped += 1
                continue
            value = dict(value)
            value["flux"] = [
                flux * LAMOST_FLUX_SCALE if isinstance(flux, (int, float)) else flux
                for flux in value["flux"]
            ]

            twin = self._scaled_twin(datum, value["flux"])
            if twin is not None:
                removed += 1
                self.stdout.write(
                    f"datum {datum.pk} (target {datum.target_id}, {value.get('spectrum_type')}): "
                    f"raw duplicate of correctly scaled datum {twin.pk}; removing"
                )
                if not dry_run:
                    datum.delete()
                continue

            rescaled += 1
            peak = max((f for f in value["flux"] if isinstance(f, (int, float))), default=0.0)
            self.stdout.write(
                f"datum {datum.pk} (target {datum.target_id}, {value.get('spectrum_type')}): "
                f"peak flux now {peak:.4g}"
            )
            if not dry_run:
                datum.value = value
                datum.save(update_fields=["value"])

        prefix = "Would " if dry_run else ""
        self.stdout.write(
            self.style.SUCCESS(f"{prefix}{'rescale' if dry_run else 'Rescaled'} {rescaled} LAMOST spectra, "
                               f"{prefix.lower()}{'remove' if dry_run else 'removed'} {removed} raw duplicates; "
                               f"left {skipped} unchanged (already below {LAMOST_UNSCALED_FLUX_THRESHOLD:g}).")
        )
