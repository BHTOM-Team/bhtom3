import logging
import math
import re

from astroquery.gaia import Gaia
from django import forms
from django.utils import timezone
from astropy.time import Time

import tom_dataproducts.single_target_data_service.single_target_data_service as stds
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target
from custom_code.bhtom_catalogs.harvesters.gaia_dr3 import GaiaDR3Harvester


logger = logging.getLogger(__name__)


def gaia_time_to_mjd(gaia_time):
    # Gaia DR3 epoch photometry time columns are offset by 55197 days.
    return float(gaia_time) + 55197.0


def mag_error(flux_over_error):
    return 1.0 / (float(flux_over_error) * 2.5 / math.log(10.0))


def _to_float(value):
    try:
        converted = float(value)
        if math.isnan(converted):
            return None
        return converted
    except (TypeError, ValueError):
        return None


class GaiaDR3PhotometryQueryForm(stds.BaseSingleTargetDataServiceQueryForm):
    search_radius_arcsec = forms.FloatField(
        required=False,
        initial=1.0,
        min_value=0.05,
        label='Cone search radius (arcsec)',
    )

    def layout(self):
        return 'search_radius_arcsec'


class GaiaDR3PhotometryService(stds.BaseSingleTargetDataService):
    name = 'Gaia DR3 Photometry'
    info_url = 'https://gea.esac.esa.int/archive/'
    data_service_type = 'Catalog Search'
    service_notes = 'Downloads Gaia DR3 epoch photometry (G/BP/RP) and stores it as ReducedDatums.'

    def __init__(self):
        super().__init__()
        self.success_message = 'Gaia DR3 photometry query completed.'

    def get_form(self):
        return GaiaDR3PhotometryQueryForm

    def query_service(self, query_parameters):
        target_id = query_parameters.get('target_id')
        if not target_id:
            raise stds.SingleTargetDataServiceException('target_id is required')

        try:
            target = Target.objects.get(pk=target_id)
        except Target.DoesNotExist as exc:
            raise stds.SingleTargetDataServiceException(f'Target {target_id} does not exist') from exc

        radius_arcsec = _to_float(query_parameters.get('search_radius_arcsec')) or 1.0
        source_id = self._resolve_source_id(target, radius_arcsec)
        if not source_id:
            self.success_message = (
                f'No Gaia DR3 object found near target coordinates '
                f'(RA={target.ra}, Dec={target.dec}) within {radius_arcsec} arcsec.'
            )
            return True

        lightcurve = self.download_dr3_lightcurve(source_id)
        if lightcurve is None or len(lightcurve) == 0:
            self.success_message = f'No Gaia DR3 epoch photometry returned for source {source_id}.'
            return True

        created = self._store_reduced_datums(target, lightcurve)
        self.success_message = (
            f'Gaia DR3 query completed for source {source_id}. '
            f'Created {created} new ReducedDatum photometry points.'
        )
        return True

    def validate_form(self, query_parameters):
        return

    def get_success_message(self):
        return self.success_message

    def get_data_product_type(self):
        return 'photometry'

    @staticmethod
    def download_dr3_lightcurve(source_id):
        datalink = Gaia.load_data(
            ids=[str(source_id)],
            data_release='Gaia DR3',
            retrieval_type='EPOCH_PHOTOMETRY',
            data_structure='INDIVIDUAL',
            verbose=False,
            output_file=None,
            format='votable',
        )
        keys = sorted(datalink.keys())
        if not keys:
            return None
        return datalink[keys[0]][0].to_table().to_pandas()

    @staticmethod
    def _extract_source_id(target):
        # Look for Gaia DR3 identifier in target primary name and aliases.
        for name in target.names:
            value = str(name).strip()
            if value.isdigit():
                return value
            match = re.search(r'(?i)gaia\s*dr3[_\s-]*(\d{8,})', value)
            if match:
                return match.group(1)
        return None

    def _resolve_source_id(self, target, radius_arcsec):
        # Prefer cone search around current coordinates.
        if target.ra is not None and target.dec is not None:
            try:
                harvester = GaiaDR3Harvester()
                harvester.query(f'{target.ra} {target.dec} {radius_arcsec}')
                source_id = harvester.catalog_data.get('SOURCE_ID', harvester.catalog_data.get('source_id'))
                if source_id:
                    target.aliases.get_or_create(name=f'GaiaDR3_{source_id}')
                    return str(source_id)
            except Exception as exc:
                logger.warning('Gaia DR3 cone-search source_id resolution failed for %s: %s', target.name, exc)

        # Fallback to existing names/aliases.
        source_id = self._extract_source_id(target)
        if source_id:
            return source_id

        if target.ra is None or target.dec is None:
            return None
        try:
            # Retry with wider radius in case target coordinates are slightly offset.
            harvester = GaiaDR3Harvester()
            harvester.query(f'{target.ra} {target.dec} {max(radius_arcsec * 5.0, 2.0)}')
            source_id = harvester.catalog_data.get('SOURCE_ID', harvester.catalog_data.get('source_id'))
            if source_id:
                target.aliases.get_or_create(name=f'GaiaDR3_{source_id}')
                return str(source_id)
        except Exception as exc:
            logger.warning('Gaia DR3 cone-search source_id resolution failed for %s: %s', target.name, exc)
        return None

    def _store_reduced_datums(self, target, lightcurve):
        created = 0
        band_specs = [
            ('G', 'g_transit_time', 'g_transit_mag', 'g_transit_flux_over_error'),
            ('BP', 'bp_obs_time', 'bp_mag', 'bp_flux_over_error'),
            ('RP', 'rp_obs_time', 'rp_mag', 'rp_flux_over_error'),
        ]

        for _, row in lightcurve.iterrows():
            for band, time_col, mag_col, flux_err_col in band_specs:
                if self._is_missing(row.get(time_col)) or self._is_missing(row.get(mag_col)):
                    continue
                if self._is_missing(row.get(flux_err_col)) or float(row.get(flux_err_col)) <= 0:
                    continue

                try:
                    mjd = gaia_time_to_mjd(row.get(time_col))
                    error = mag_error(row.get(flux_err_col))
                    magnitude = float(row.get(mag_col))
                except (TypeError, ValueError, ZeroDivisionError):
                    continue

                timestamp = Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc)
                value = {
                    'filter': band,
                    'magnitude': magnitude,
                    'error': error,
                }
                _, was_created = ReducedDatum.objects.get_or_create(
                    target=target,
                    data_type='photometry',
                    timestamp=timestamp,
                    value=value,
                    defaults={
                        'source_name': self.name,
                        'source_location': self.info_url,
                    }
                )
                if was_created:
                    created += 1

        logger.info('Gaia DR3 photometry: created %s points for target %s', created, target.name)
        return created

    @staticmethod
    def _is_missing(value):
        if value is None:
            return True
        try:
            return bool(math.isnan(float(value)))
        except (TypeError, ValueError):
            return False
