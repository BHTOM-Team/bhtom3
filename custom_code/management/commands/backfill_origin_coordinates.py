from django.core.management.base import BaseCommand
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target

from custom_code.data_services.alerce_dataservice import AlerceDataService
from custom_code.data_services.lsst_dataservice import LSSTDataService
from custom_code.data_services.ztf_dataservice import ZTFDataService

# Only services that know how to store origin_ra/origin_dec are worth re-querying.
BACKFILL_SERVICES = {
    'ZTF': ZTFDataService,
    'Alerce': AlerceDataService,
    'LSST': LSSTDataService,
}


class Command(BaseCommand):
    help = (
        "Re-query ZTF, Alerce and LSST for targets that already hold their photometry, so points "
        "ingested before origin_ra/origin_dec existed pick up a per-epoch sky position. "
        "Existing rows are updated in place; no duplicate photometry is created."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--service",
            action="append",
            choices=sorted(BACKFILL_SERVICES),
            help="Limit to one service (repeatable). Default: all supported services.",
        )
        parser.add_argument("--target-id", type=int, help="Backfill only one target id.")
        parser.add_argument("--limit", type=int, help="Stop after this many targets.")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report how many datums still lack a position, without querying anything.",
        )

    def _targets_missing_positions(self, service_name, target_id):
        """Targets holding datums from this service that have no origin_ra yet."""
        queryset = ReducedDatum.objects.filter(
            source_name=service_name,
            data_type='photometry',
        ).exclude(value__has_key='origin_ra')
        if target_id:
            queryset = queryset.filter(target_id=target_id)
        return queryset.values_list('target_id', flat=True).distinct()

    def handle(self, *args, **options):
        service_names = options.get("service") or sorted(BACKFILL_SERVICES)
        target_id = options.get("target_id")
        limit = options.get("limit")
        dry_run = options.get("dry_run")

        for service_name in service_names:
            target_ids = list(self._targets_missing_positions(service_name, target_id))
            if limit:
                target_ids = target_ids[:limit]

            if dry_run:
                pending = ReducedDatum.objects.filter(
                    source_name=service_name, data_type='photometry',
                ).exclude(value__has_key='origin_ra')
                if target_id:
                    pending = pending.filter(target_id=target_id)
                self.stdout.write(
                    f"{service_name}: {pending.count()} datums without a position "
                    f"across {len(target_ids)} targets."
                )
                continue

            service_class = BACKFILL_SERVICES[service_name]
            updated_targets = 0
            failed = 0
            for pk in target_ids:
                target = Target.objects.filter(pk=pk).first()
                if target is None or target.ra is None or target.dec is None:
                    continue
                try:
                    service = service_class()
                    # Let each service apply its own defaults (e.g. LSST's 2" cone vs ZTF's 1.1").
                    results = service.query_targets(service.build_query_parameters({
                        'target_name': target.name,
                        'ra': target.ra,
                        'dec': target.dec,
                        'include_photometry': True,
                    }))
                    if not results:
                        continue
                    service.to_reduced_datums(target, results[0].get('reduced_datums'))
                    updated_targets += 1
                    self.stdout.write(f"  {service_name}: refreshed {target.name} (id {pk})")
                except Exception as exc:  # one unreachable target must not stop the backfill
                    failed += 1
                    self.stderr.write(f"  {service_name}: failed for target {pk}: {exc}")

            self.stdout.write(self.style.SUCCESS(
                f"{service_name}: refreshed {updated_targets} targets ({failed} failed)."
            ))
