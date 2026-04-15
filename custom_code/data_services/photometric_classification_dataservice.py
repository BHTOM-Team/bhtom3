import logging

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target

from custom_code.data_services.forms import PhotometricClassificationQueryForm
from custom_code.photometry_classification.service import classify_target_coordinates


logger = logging.getLogger(__name__)


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class PhotometricClassificationDataService(DataService):
    name = 'PhotometricClassification'
    verbose_name = 'Photometric Classification'
    update_on_daily_refresh = False
    info_url = ''
    service_notes = 'Classify a target from archival Gaia/2MASS/WISE photometry and store the result in phot_class.'

    @classmethod
    def get_form_class(cls):
        return PhotometricClassificationQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_id': parameters.get('target_id'),
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'force': bool(parameters.get('force', False)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        target_id = query_parameters.get('target_id')
        force = bool(query_parameters.get('force', False))
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        if ra is None or dec is None:
            logger.debug(
                'PhotometricClassification skipped: missing coordinates (target_id=%s, ra=%s, dec=%s).',
                target_id,
                query_parameters.get('ra'),
                query_parameters.get('dec'),
            )
            self.query_results = []
            return []

        logger.debug(
            'Running PhotometricClassification (target_id=%s, ra=%s, dec=%s, force=%s).',
            target_id,
            ra,
            dec,
            force,
        )

        if target_id and not force:
            try:
                target = Target.objects.get(pk=int(target_id))
            except (Target.DoesNotExist, TypeError, ValueError):
                target = None
            if target is not None and getattr(target, 'phot_classification_done', False):
                logger.debug('PhotometricClassification already done for target %s; skipping.', target.name)
                self.query_results = []
                return []

        phot_class = classify_target_coordinates(ra, dec)
        if phot_class in (None, '', '--', '-'):
            logger.debug(
                'PhotometricClassification produced no usable result (target_id=%s, ra=%s, dec=%s, phot_class=%s).',
                target_id,
                ra,
                dec,
                phot_class,
            )
        else:
            logger.debug(
                'PhotometricClassification result (target_id=%s, ra=%s, dec=%s): %s',
                target_id,
                ra,
                dec,
                phot_class,
            )
        self.query_results = [{
            'name': 'Photometric Classification',
            'ra': ra,
            'dec': dec,
            'target_updates': {
                'phot_class': phot_class,
                'phot_classification_done': True,
            },
        }]
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        return self.query_service(query_parameters, **kwargs)

    def create_target_from_query(self, target_result, **kwargs):
        return Target(
            name=target_result['name'],
            type='SIDEREAL',
            ra=target_result.get('ra'),
            dec=target_result.get('dec'),
            epoch=2000.0,
        )

    def create_aliases_from_query(self, alias_results, **kwargs):
        return []

    def to_reduced_datums(self, target, data_results=None, **kwargs):
        return
