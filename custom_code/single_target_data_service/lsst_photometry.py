import logging
import math
import re
from decimal import Decimal, InvalidOperation

import requests
from astropy.time import Time
from django import forms
from django.conf import settings
from django.utils import timezone

import tom_dataproducts.single_target_data_service.single_target_data_service as stds
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target


logger = logging.getLogger(__name__)

DEFAULT_FINK_API_URL = 'https://api.lsst.fink-portal.org'


class LSSTPhotometryQueryForm(stds.BaseSingleTargetDataServiceQueryForm):
    search_radius_arcsec = forms.FloatField(
        required=False,
        initial=5.0,
        min_value=0.1,
        label='Cone search radius (arcsec)',
    )

    def layout(self):
        return 'search_radius_arcsec'


def _first_present(row, keys):
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _safe_float(value):
    try:
        number = float(value)
        if math.isnan(number) or not math.isfinite(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def _normalize_lsst_id(term):
    term_str = str(term).strip()
    if term_str.upper().startswith('LSST_'):
        term_str = term_str.split('_', 1)[1].strip()
    try:
        parsed = Decimal(term_str)
        if parsed == parsed.to_integral_value():
            return str(int(parsed))
    except (InvalidOperation, ValueError):
        pass
    return term_str


class LSSTPhotometryService(stds.BaseSingleTargetDataService):
    name = 'LSST Photometry'
    info_url = 'https://api.fink-portal.org'
    data_service_type = 'Catalog Search'
    service_notes = 'Fetches LSST/Fink photometry and stores it as ReducedDatums.'

    def __init__(self):
        super().__init__()
        self.success_message = 'LSST photometry query completed.'

    def get_form(self):
        return LSSTPhotometryQueryForm

    def query_service(self, query_parameters):
        target_id = query_parameters.get('target_id')
        if not target_id:
            raise stds.SingleTargetDataServiceException('target_id is required')

        try:
            target = Target.objects.get(pk=target_id)
        except Target.DoesNotExist as exc:
            raise stds.SingleTargetDataServiceException(f'Target {target_id} does not exist') from exc

        radius_arcsec = _safe_float(query_parameters.get('search_radius_arcsec')) or 5.0
        source_id = self._resolve_lsst_id(target, radius_arcsec)
        if not source_id:
            self.success_message = (
                f'No LSST/Fink object found near target coordinates '
                f'(RA={target.ra}, Dec={target.dec}).'
            )
            return True

        points = self._fetch_source_points(source_id)
        if not points:
            self.success_message = f'No LSST photometry points returned for {source_id}.'
            return True

        created = self._store_reduced_datums(target, points)
        self.success_message = (
            f'LSST photometry query completed for {source_id}. '
            f'Created {created} new ReducedDatum points.'
        )
        return True

    def validate_form(self, query_parameters):
        return

    def get_success_message(self):
        return self.success_message

    def get_data_product_type(self):
        return 'photometry'

    def _api_url(self):
        return settings.SINGLE_TARGET_DATA_SERVICES.get('LSST_PHOTOMETRY', {}).get('url', DEFAULT_FINK_API_URL)

    def _post_json(self, endpoint, payload):
        response = requests.post(f'{self._api_url()}{endpoint}', json=payload, timeout=25)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get('data', [])
        return []

    @staticmethod
    def _extract_lsst_id_from_names(target):
        for name in target.names:
            value = str(name).strip()
            match = re.search(r'(?i)lsst[_\s-]*([0-9eE\+\-\.]{8,})', value)
            if match:
                normalized = _normalize_lsst_id(match.group(1))
                if normalized.isdigit():
                    return normalized
            # fallback: alias exactly numeric (used in some older DBs for diaObjectId)
            normalized = _normalize_lsst_id(value)
            if normalized.isdigit() and len(normalized) >= 8:
                return normalized
        return None

    def _resolve_lsst_id(self, target, radius_arcsec):
        if target.ra is None or target.dec is None:
            # If no coordinates are available, fallback to name/alias parsing only.
            return self._extract_lsst_id_from_names(target)

        # Some Fink deployments interpret cone radius differently. Try several values.
        radius_candidates = [
            float(radius_arcsec),
            max(float(radius_arcsec) * 3.0, 15.0),
            float(radius_arcsec) / 3600.0,
        ]
        tried_radii = []
        for radius in radius_candidates:
            tried_radii.append(radius)
            rows = self._post_json(
                '/api/v1/conesearch',
                {
                    'ra': float(target.ra),
                    'dec': float(target.dec),
                    'radius': radius,
                    'output-format': 'json',
                },
            )
            if not rows:
                continue

            first_id = _first_present(rows[0], ('r:diaObjectId', 'diaObjectId', 'objectId', 'i:objectId'))
            if first_id is None:
                continue
            normalized = _normalize_lsst_id(first_id)
            if not normalized.isdigit():
                continue

            target.aliases.get_or_create(name=f'LSST_{normalized}')
            return normalized

        # If cone search fails, keep a fallback to existing aliases/names.
        lsst_id = self._extract_lsst_id_from_names(target)
        if lsst_id:
            return lsst_id

        logger.warning(
            'LSST diaObjectId cone search failed for target=%s ra=%s dec=%s radii=%s',
            target.name, target.ra, target.dec, tried_radii
        )
        return None

    def _fetch_source_points(self, lsst_id):
        rows = self._post_json(
            '/api/v1/sources',
            {
                'diaObjectId': str(lsst_id),
                'columns': 'r:diaObjectId,r:midpointMjdTai,r:psfFlux,r:psfFluxErr,r:band',
                'output-format': 'json',
            },
        )
        if not rows:
            rows = self._post_json(
                '/api/v1/sources',
                {
                    'objectId': str(lsst_id),
                    'columns': 'r:diaObjectId,r:midpointMjdTai,r:psfFlux,r:psfFluxErr,r:band',
                    'output-format': 'json',
                },
            )
        return rows

    def _store_reduced_datums(self, target, rows):
        created = 0
        for row in rows:
            mjd = _safe_float(_first_present(row, ('r:midpointMjdTai', 'midpointMjdTai', 'mjd')))
            flux = _safe_float(_first_present(row, ('r:psfFlux', 'psfFlux', 'flux')))
            flux_err = _safe_float(_first_present(row, ('r:psfFluxErr', 'psfFluxErr', 'fluxErr')))
            band = _first_present(row, ('r:band', 'band')) or 'unknown'

            if mjd is None or flux is None or flux_err is None:
                continue
            if flux <= 0 or flux_err <= 0:
                continue

            # Fink flux is in nJy in this endpoint, matching your prior implementation.
            magnitude = -2.5 * math.log10((flux * 1e-9) / 3631.0)
            mag_error = 1.0857 * (flux_err / flux)
            if not math.isfinite(magnitude) or not math.isfinite(mag_error) or mag_error >= 1.5:
                continue

            timestamp = Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc)
            value = {
                'filter': f'LSST({band})',
                'magnitude': magnitude,
                'error': mag_error,
            }
            _, was_created = ReducedDatum.objects.get_or_create(
                target=target,
                data_type='photometry',
                timestamp=timestamp,
                value=value,
                defaults={
                    'source_name': self.name,
                    'source_location': f'{self._api_url()}/api/v1/sources',
                },
            )
            if was_created:
                created += 1

        logger.info('LSST photometry: created %s points for target %s', created, target.name)
        return created
