import gzip
import json
import requests
import time
from io import BytesIO
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from astropy.time import Time
from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.table import Table
from datetime import timezone
from django import forms
from django.core.cache import cache
from django.core.files.uploadedfile import SimpleUploadedFile
from django.contrib.auth import get_user_model
from django.http import QueryDict
from django.test.client import RequestFactory
from django.test import TestCase
from django.urls import reverse
from guardian.shortcuts import assign_perm
import numpy as np
from rest_framework.authtoken.models import Token
from tom_catalogs.harvester import MissingDataException
from tom_dataproducts.models import DataProduct, ReducedDatum
from tom_observations.models import ObservationRecord
from tom_targets.models import Target, TargetName

from bhtom3.bhtom_observations.facilities.lco import (
    AccountLCOSettings,
    BhtomLCOImagingObservationForm,
    BhtomLCOMonitoringObservationForm,
    LCOFacility,
    calculate_lco_etc_exposure_time,
    resolve_lco_bhtom2_observatory_oname,
)
from custom_code.data_services.ogle_ews_dataservice import (
    OGLEEWSDataService,
    _dec_to_decimal,
    _normalize_target_name,
    _ogle_event_url,
    _ogle_phot_url,
    _ogle_years,
    _parse_lenses_rows,
    _parse_photometry_rows,
    _ra_to_decimal,
)
from custom_code.data_services.moa_dataservice import (
    MOADataService,
    _event_suffix_candidates,
    _extract_calibration,
    _normalize_event_name as _normalize_moa_event_name,
    _parse_photometry_rows as _parse_moa_photometry_rows,
    _parse_event_page,
)
from custom_code.astrometry import can_compute_current_coordinates, compute_current_coordinates
from custom_code.data_services.allwise_dataservice import AllWISEDataService
from custom_code.data_services.asassn_dataservice import ASASSNDataService, _normalize_transient_name
from custom_code.data_services.exoclock_dataservice import ExoClockDataService
from custom_code.data_services.gaia_alerts_dataservice import GaiaAlertsDataService
from custom_code.data_services.gaia_dr3_dataservice import GaiaDR3DataService
from custom_code.data_services.fram_dataservice import FRAMDataService, _parse_mjd_photometry
from custom_code.data_services.galah_dataservice import GALAHDataService
from custom_code.data_services.kmt_dataservice import (
    KMTDataService,
    _event_id as _kmt_event_id,
    _normalize_event_name as _normalize_kmt_event_name,
    _normalize_hjd as _normalize_kmt_hjd,
)
from custom_code.data_services.lamost_dataservice import LAMOSTDataService
from custom_code.data_services.neowise_dataservice import NeoWISEDataService
from custom_code.data_services.twomass_dataservice import TwoMASSDataService
from custom_code.bhtom_catalogs.harvesters.simbad import target_from_result
from custom_code.bhtom_catalogs.harvesters.crts import CRTSHarvester
from custom_code.bhtom_catalogs.harvesters.gaia_alerts import GaiaAlertsHarvester
from custom_code.bhtom_catalogs.harvesters import gaia_dr3 as gaia_dr3_harvester
from custom_code.bhtom_catalogs.harvesters.gaia_dr3 import GaiaDR3Harvester
from custom_code.bhtom_catalogs.harvesters.jplhorizons import JPLHorizonsHarvester
from custom_code.bhtom_catalogs.harvesters.exoclock import ExoClockHarvester
from custom_code.bhtom_catalogs.harvesters.kmt import KMTHarvester
from custom_code.bhtom_catalogs.harvesters.lsst import LSSTHarvester
from custom_code.bhtom_catalogs.harvesters.moa import MOAHarvester
from custom_code.bhtom_catalogs.harvesters.ogle_ews import OGLEEWSHarvester
from custom_code.data_services.forms import (
    ExoClockQueryForm,
    GaiaDR3QueryForm,
    GALAHQueryForm,
    GS6dFQueryForm,
    LAMOSTQueryForm,
    SimbadQueryForm,
    WISEQueryForm,
)
from custom_code.data_services.service_utils import resolve_query_coordinates
from custom_code.bhtom2_uploads import has_successful_bhtom2_upload, is_supported_fits_filename, normalize_fits_upload
from custom_code.forms import (
    ALL_DATA_SERVICES_VALUE,
    BhtomCatalogQueryForm,
    BhtomUserCreationForm,
    BhtomUserUpdateForm,
    BhtomNonSiderealTargetCreateForm,
    NonSiderealTargetVisibilityForm,
    BhtomPlanetaryTransitTargetCreateForm,
    BhtomPlanetaryTransitTargetUpdateForm,
    BhtomSiderealTargetCreateForm,
    BhtomSiderealTargetUpdateForm,
    BhtomTargetNamesFormset,
)
from custom_code.models import (
    BhtomUserProfile,
    Facility,
    FacilityAccount,
    FacilityAccountMembership,
    FacilityProposal,
    FacilityProposalMembership,
    GeoTarget,
    TransitEphemeris,
    UserBhtom2UploadPreference,
)
from custom_code.facility_proposals import (
    get_accessible_proposals,
    get_proposal_choices_for_user,
    sync_remote_proposals_for_account,
)
from custom_code.orcid import canonicalize_orcid, unique_orcid_username, validate_orcid
from custom_code.non_sidereal_visibility import get_non_sidereal_visibility
from custom_code.signals import cleanup_target_relations_on_target_delete
from custom_code.templatetags.custom_observation_extras import (
    _target_plan_layout,
    _target_plan_plot_data,
    nonsidereal_target_plan,
)
from custom_code.templatetags.custom_target_extras import bhtom_target_data, non_sidereal_aladin
from custom_code.templatetags.custom_target_extras import truncate_decimals
from custom_code.tasks import _build_query_parameters_for_service, _run_service_for_target
from custom_code.tasks import _get_or_create_target_alias, run_observation_status_update
from custom_code.sun_separation import get_live_target_values
from custom_code.target_derivations import derive_sidereal_target_fields
from custom_code.views import (
    BhtomCatalogQueryView,
    BhtomCreateTargetFromQueryView,
    _annotate_results_with_existing_targets,
    _serialize_query_parameters,
    BhtomTargetCreateView,
    BhtomTargetUpdateView,
    EXOCLOCK_RECOMMENDED_OBSERVING_STRATEGY,
    ProposalAwareObservationCreateView,
)


def _minimal_lco_instruments():
    return {
        '0M4-SCICAM-SBIG': {
            'type': 'IMAGE',
            'class': '0m4',
            'name': '0.4 meter SBIG',
            'optical_elements': {
                'filters': [
                    {'name': 'SDSS-gp', 'code': 'gp', 'schedulable': True, 'default': False},
                ],
            },
            'modes': {
                'readout': {
                    'modes': [
                        {'name': '1x1 binning', 'code': '1x1'},
                    ],
                },
                'guiding': {
                    'modes': [
                        {'name': 'On', 'code': 'ON'},
                        {'name': 'Off', 'code': 'OFF'},
                    ],
                },
            },
            'configuration_types': {
                'EXPOSE': {'name': 'Expose', 'code': 'EXPOSE', 'schedulable': True},
            },
            'default_configuration_type': 'EXPOSE',
        },
        '0M4-SCICAM-QHY600': {
            'type': 'IMAGE',
            'class': '0m4',
            'name': '0m4 SCICAM QHY600',
            'optical_elements': {
                'filters': [
                    {'name': 'SDSS-gp', 'code': 'gp', 'schedulable': True, 'default': False},
                ],
            },
            'modes': {
                'readout': {
                    'modes': [
                        {'name': 'QHY600 Central 30x30 arcmin', 'code': 'qhy600_central_30x30'},
                        {'name': 'QHY600 Full Frame Readout', 'code': 'qhy600_full_frame'},
                    ],
                },
                'guiding': {
                    'modes': [
                        {'name': 'On', 'code': 'ON'},
                        {'name': 'Off', 'code': 'OFF'},
                    ],
                },
            },
            'configuration_types': {
                'EXPOSE': {'name': 'Expose', 'code': 'EXPOSE', 'schedulable': True},
            },
            'default_configuration_type': 'EXPOSE',
        },
        '1M0-SCICAM-SINISTRO': {
            'type': 'IMAGE',
            'class': '1m0',
            'name': '1.0 meter Sinistro',
            'optical_elements': {
                'filters': [
                    {'name': 'SDSS-gp', 'code': 'gp', 'schedulable': True, 'default': False},
                ],
            },
            'modes': {
                'readout': {
                    'modes': [
                        {'name': '1M Sinistro Central 2k 2x2 binned', 'code': 'sinistro_central_2k_2x2'},
                        {'name': '1M Sinistro Full Frame', 'code': 'sinistro_full_frame'},
                    ],
                },
                'guiding': {
                    'modes': [
                        {'name': 'On', 'code': 'ON'},
                        {'name': 'Off', 'code': 'OFF'},
                    ],
                },
            },
            'configuration_types': {
                'EXPOSE': {'name': 'Expose', 'code': 'EXPOSE', 'schedulable': True},
            },
            'default_configuration_type': 'EXPOSE',
        },
    }


def _bhtom2_lco_observatory_payload():
    return {
        'data': [
            {
                'name': 'LCOGT Teide Obs. 40-cm (file code: tfn)',
                'cameras': [
                    {'prefix': 'LCOGT-Teide-40cm_SBIG6303'},
                    {'prefix': 'LCOGT-Teide-40cm_QHY600M'},
                ],
            },
            {
                'name': 'LCOGT Teide Obs. 1-m (file code: tfn)',
                'cameras': [
                    {'prefix': 'LCOGT-Teide-1m_4K'},
                ],
            },
            {
                'name': 'LCOGT Siding Spring 2-m (file code: coj)',
                'cameras': [
                    {'prefix': 'LCOGT-SS-2m_Spectral'},
                    {'prefix': 'LCOGT-SS-2m_Muscat'},
                ],
            },
            {
                'name': 'LCOGT CTIO 40-cm (file code: lsc)',
                'cameras': [
                    {'prefix': 'LCOGT-CTIO-40cm_QHY600M'},
                    {'prefix': 'LCOGT-CTIO-40cm_SBIG6303'},
                ],
            },
        ],
    }


class OGLEEWSDataServiceTests(TestCase):
    def test_coordinate_helpers_match_bhtom2_behavior(self):
        self.assertAlmostEqual(_ra_to_decimal('17:49:39.07'), 267.4127916666667)
        self.assertAlmostEqual(_dec_to_decimal('-30:27:08.4'), -30.452333333333332)

    def test_parse_lenses_rows_supports_headerless_content(self):
        rows = _parse_lenses_rows(
            '\n'.join([
                '2011-BLG-0001 BLG500.01 129412 17:54:00.05 -29:06:06.0',
                '2011-BLG-0002 BLG500.08 102027 17:54:44.93 -28:54:13.5',
            ])
        )

        self.assertEqual(rows[0]['name'], '2011-BLG-0001')
        self.assertEqual(rows[0]['field'], 'BLG500.01')
        self.assertAlmostEqual(rows[0]['ra'], 268.5002083333333)
        self.assertAlmostEqual(rows[0]['dec'], -29.101666666666667)

    def test_query_targets_by_name_includes_photometry(self):
        service = OGLEEWSDataService()
        alert_rows = [{
            'name': '2011-BLG-0001',
            'field': 'BLG500.01',
            'starno': '129412',
            'ra_text': '17:54:00.05',
            'dec_text': '-29:06:06.0',
            'ra': 268.5002083333333,
            'dec': -29.101666666666667,
        }]
        photometry_rows = [
            {'hjd': 2455260.85336, 'mag': 17.131, 'magerr': 0.015},
            {'hjd': 2455260.90029, 'mag': 17.130, 'magerr': 9.999},
        ]

        with patch.object(service, '_fetch_alert_rows', return_value=alert_rows), patch.object(
            service,
            '_fetch_photometry_rows',
            return_value=photometry_rows,
        ):
            results = service.query_targets({
                'target_name': 'OGLE-2011-BLG-0001',
                'include_photometry': True,
            })

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], 'OGLE-2011-BLG-0001')
        self.assertEqual(results[0]['aliases'], ['OGLE-2011-BLG-0001'])
        self.assertEqual(
            results[0]['source_location'],
            'https://www.astrouw.edu.pl/ogle/ogle4/ews/2011-BLG-0001.html',
        )
        photometry = results[0]['reduced_datums']['photometry']
        self.assertEqual(len(photometry), 1)
        self.assertEqual(photometry[0]['value']['filter'], 'OGLE(I)')
        self.assertEqual(photometry[0]['value']['magnitude'], 17.131)

    def test_ogle_helpers_normalize_names_and_build_urls(self):
        self.assertEqual(_normalize_target_name('OGLE-2011-BLG-0001'), '2011-BLG-0001')
        self.assertEqual(
            _ogle_phot_url('OGLE-2011-BLG-0001'),
            'https://www.astrouw.edu.pl/ogle/ogle4/ews/2011/blg-0001/phot.dat',
        )
        self.assertEqual(
            _ogle_event_url('OGLE-2011-BLG-0001'),
            'https://www.astrouw.edu.pl/ogle/ogle4/ews/2011-BLG-0001.html',
        )
        self.assertEqual(
            _parse_photometry_rows('2455260.85336 17.131 0.015 5.94 1033.0\n')[0],
            {'hjd': 2455260.85336, 'mag': 17.131, 'magerr': 0.015},
        )

    def test_ogle_2026_helpers_use_new_ews_layout(self):
        self.assertEqual(
            _ogle_phot_url('OGLE-2026-BLG-0001'),
            'https://www.astrouw.edu.pl/ogle/ogle4/ews/2026/blg-0001/phot.dat',
        )
        self.assertEqual(
            _ogle_event_url('OGLE-2026-BLG-0001'),
            'https://www.astrouw.edu.pl/ogle/ogle4/ews/2026/blg-0001/',
        )

    def test_ogle_years_probes_one_year_ahead(self):
        self.assertIn(2026, _ogle_years(current_year=2025))


class ExoClockDataServiceTests(TestCase):
    def test_build_query_parameters_serializes_compute_from_date(self):
        service = ExoClockDataService()

        params = service.build_query_parameters({
            'compute_from_date': '2026-04-21T12:34:56',
            'transit_within_days': '1',
        })

        self.assertEqual(params['compute_from_date'], '2026-04-21T12:34:56')

    def test_query_targets_by_name_returns_ephemeris_fields(self):
        service = ExoClockDataService()
        catalog = {
            'WASP-12b': {
                'name': 'WASP-12b',
                'star': 'WASP-12',
                'priority': 'low',
                'current_oc_min': -5.37,
                'ra_j2000': '06:30:32.7966',
                'dec_j2000': '+29:40:20.266',
                'v_mag': 11.57,
                'r_mag': 11.288,
                'gaia_g_mag': 11.5,
                'depth_r_mmag': 17.81,
                'duration_hours': 3.0,
                't0_bjd_tdb': 2457368.4973,
                't0_unc': 5.9e-05,
                'period_days': 1.091418859,
                'period_unc': 3.9e-08,
                'min_telescope_inches': 5.0,
                'total_observations': 532,
                'total_observations_recent': 26,
            }
        }
        with patch.object(service, 'query_service', return_value={'catalog': catalog, 'source_location': service.info_url}):
            results = service.query_targets({
                'target_name': 'WASP-12b',
                'target_names': ['WASP-12b', 'WASP-12'],
                'radius_arcsec': 30.0,
            })

        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result['name'], 'WASP-12b')
        self.assertEqual(result['aliases'][0]['name'], 'WASP-12')
        self.assertEqual(result['transit_ephemeris_updates']['period_days'], 1.091418859)
        self.assertEqual(result['transit_ephemeris_updates']['recent_observations'], 26)
        self.assertNotIn('reduced_datums', result)
        self.assertEqual(result['transit_source_name'], 'ExoClock')
        self.assertEqual(result['transit_planet_name'], 'WASP-12b')
        self.assertEqual(result['transit_host_name'], 'WASP-12')
        self.assertEqual(result['transit_period_days'], 1.091418859)
        self.assertEqual(result['target_updates']['classification'], 'Planetary Transit')

    def test_query_targets_by_coordinates_selects_nearest_planet(self):
        service = ExoClockDataService()
        catalog = {
            'WASP-12b': {
                'name': 'WASP-12b',
                'star': 'WASP-12',
                'ra_j2000': '06:30:32.7966',
                'dec_j2000': '+29:40:20.266',
            },
            'WASP-13b': {
                'name': 'WASP-13b',
                'star': 'WASP-13',
                'ra_j2000': '09:20:24.7030',
                'dec_j2000': '+33:52:56.598',
            },
        }

        with patch.object(service, 'query_service', return_value={'catalog': catalog, 'source_location': service.info_url}):
            results = service.query_targets({
                'target_names': [],
                'ra': 97.63665,
                'dec': 29.672296,
                'radius_arcsec': 60.0,
            })

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], 'WASP-12b')

    def test_query_targets_with_advanced_filters_returns_matching_transits(self):
        service = ExoClockDataService()
        catalog = {
            'NearTransit': {
                'name': 'NearTransit',
                'star': 'NearStar',
                'ra_j2000': '06:30:32.7966',
                'dec_j2000': '+29:40:20.266',
                'v_mag': 11.0,
                'depth_r_mmag': 20.0,
                't0_bjd_tdb': 2461137.5,
                'period_days': 2.0,
            },
            'TooLateTransit': {
                'name': 'TooLateTransit',
                'star': 'LateStar',
                'ra_j2000': '06:31:00.0000',
                'dec_j2000': '+30:00:00.000',
                'v_mag': 10.0,
                'depth_r_mmag': 21.0,
                't0_bjd_tdb': 2461139.5,
                'period_days': 5.0,
            },
            'TooFaint': {
                'name': 'TooFaint',
                'star': 'FaintStar',
                'ra_j2000': '06:32:00.0000',
                'dec_j2000': '+31:00:00.000',
                'v_mag': 14.5,
                'depth_r_mmag': 30.0,
                't0_bjd_tdb': 2461137.4,
                'period_days': 2.0,
            },
        }

        with patch.object(service, 'query_service', return_value={'catalog': catalog, 'source_location': service.info_url}), \
             patch('custom_code.data_services.exoclock_dataservice.compute_sun_separation', return_value=120.0):
            results = service.query_targets({
                'target_names': [],
                'ra': None,
                'dec': None,
                'radius_arcsec': 30.0,
                'magnitude_limit': 12.0,
                'eclipse_depth_min': 15.0,
                'declination_min': 20.0,
                'declination_max': 40.0,
                'sun_distance_min': 90.0,
                'compute_from_date': Time('2026-04-21T00:00:00', scale='utc'),
                'transit_within_days': 3.0,
            })

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], 'NearTransit')
        self.assertEqual(results[0]['sun_distance_deg'], 120.0)
        self.assertGreater(float(results[0]['next_transit_in_days']), 0.0)

    def test_exoclock_form_accepts_advanced_filters_without_name_or_coords(self):
        form = ExoClockQueryForm(data={'transit_within_days': '2.5'})

        self.assertTrue(form.is_valid(), form.errors)

    def test_exoclock_form_requires_range_when_compute_date_given(self):
        form = ExoClockQueryForm(data={'compute_from_date': '2026-04-21T12:00'})

        self.assertFalse(form.is_valid())
        self.assertIn('transit_within_days', form.errors)

    def test_exoclock_form_sets_default_advanced_time_filters(self):
        form = ExoClockQueryForm()

        self.assertIsNotNone(form.initial['compute_from_date'])
        self.assertEqual(form.initial['transit_within_days'], 1.0)


class DataServiceQuerySerializationTests(TestCase):
    def test_serialize_query_parameters_converts_datetime_to_iso_string(self):
        serialized = _serialize_query_parameters({
            'data_service': 'ExoClock',
            'compute_from_date': datetime(2026, 4, 21, 12, 34, 56),
            'transit_within_days': 1.0,
            'query_save': False,
            'query_name': '',
        })

        self.assertEqual(serialized['compute_from_date'], '2026-04-21T12:34:56')

    def test_annotate_results_marks_existing_targets(self):
        target = Target.objects.create(name='WASP-12b', type=Target.SIDEREAL, ra=1.0, dec=2.0)
        results = [{'id': 0, 'name': 'WASP-12b'}, {'id': 1, 'name': 'WASP-13b'}]

        annotated = _annotate_results_with_existing_targets(results)

        self.assertEqual(annotated[0]['existing_target_pk'], target.pk)
        self.assertIn(f'/targets/{target.pk}/', annotated[0]['existing_target_url'])
        self.assertNotIn('existing_target_pk', annotated[1])


class ObservationStatusTaskTests(TestCase):
    def test_run_observation_status_update_passes_target_none(self):
        facility_instance = Mock()
        facility_instance.update_all_observation_statuses.return_value = []

        with patch('custom_code.tasks.facility.get_service_classes', return_value=['Swift']), \
             patch('custom_code.tasks.facility.get_service_class', return_value=Mock(return_value=facility_instance)):
            result = run_observation_status_update()

        facility_instance.set_user.assert_called_once_with(None)
        facility_instance.update_all_observation_statuses.assert_called_once_with(target=None)
        self.assertEqual(result, {'Swift': []})

    def test_run_observation_status_update_continues_after_facility_error(self):
        failing_facility = Mock()
        failing_facility.update_all_observation_statuses.side_effect = RuntimeError('portal down')
        working_facility = Mock()
        working_facility.update_all_observation_statuses.return_value = []

        def get_service_class(name):
            return Mock(return_value=failing_facility if name == 'Broken' else working_facility)

        with patch('custom_code.tasks.facility.get_service_classes', return_value=['Broken', 'LCO']), \
             patch('custom_code.tasks.facility.get_service_class', side_effect=get_service_class):
            result = run_observation_status_update()

        failing_facility.set_user.assert_called_once_with(None)
        working_facility.set_user.assert_called_once_with(None)
        failing_facility.update_all_observation_statuses.assert_called_once_with(target=None)
        working_facility.update_all_observation_statuses.assert_called_once_with(target=None)
        self.assertEqual(result['Broken'], ['portal down'])
        self.assertEqual(result['LCO'], [])


class MOADataServiceTests(TestCase):
    def test_helpers_normalize_names_and_parse_calibration(self):
        self.assertEqual(_normalize_moa_event_name('2019-BLG-397'), 'MOA-2019-BLG-0397')
        self.assertEqual(_normalize_moa_event_name('MOA-2019-BLG-0397'), 'MOA-2019-BLG-0397')
        self.assertEqual(_event_suffix_candidates('MOA-2019-BLG-0397'), ['2019-BLG-397', '2019-BLG-0397'])

        calibration = _extract_calibration('I = 27.6026 - 2.5 log10(Delta Flux + 0.0000)')
        self.assertEqual(calibration['band'], 'Red')
        self.assertEqual(calibration['reference_flux'], 0.0)
        self.assertEqual(calibration['zeropoint'], 27.6026)

        zero_calibration = _extract_calibration('I = 0.0000 - 2.5 log10(Δ Flux + 0.0000)')
        self.assertEqual(zero_calibration['band'], 'Red')
        self.assertEqual(zero_calibration['reference_flux'], 0.0)
        self.assertEqual(zero_calibration['zeropoint'], 0.0)

    def test_parse_photometry_rows_skips_comments_and_converts_jd_to_mjd(self):
        rows = _parse_moa_photometry_rows(
            '#\n'
            '# RUN B39558\n'
            '2453658.854530 -192.7525 1155.2324 B300-gb5-R-1 5.528922 3745.0595 0.981619 100.00\n'
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['jd'], 2453658.85453)
        self.assertAlmostEqual(rows[0]['mjd'], 53658.35453)
        self.assertEqual(rows[0]['dflux'], -192.7525)

    def test_parse_photometry_rows_skips_invalid_zero_jd_rows(self):
        rows = _parse_moa_photometry_rows(
            '0.000000 501.5265 340.0248 B4480-gb5-R-1 3.054723 3091.8946 0.886509 0.00\n'
            '2453658.854530 -192.7525 1155.2324 B300-gb5-R-1 5.528922 3745.0595 0.981619 100.00\n'
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['jd'], 2453658.85453)

    def test_parse_event_page_extracts_metadata_calibration_and_photometry_link(self):
        parsed = _parse_event_page(
            '''
            <div id="metadata">
              <table><tr><td>RA:</td><td align="right">+18:10:53.91</td></tr>
              <tr><td>Dec:</td><td align="right">-25:44:46.00</td></tr></table>
            </div>
            <div id="micro">
              <table><tr><td>t<sub>E</sub></td><td>=</td><td align="right">62.94</td></tr></table>
            </div>
            <div id="external"><a href="phot/2019-BLG-397">Photometry data file (gzipped file)</a></div>
            <div id="calib"><p>I = 27.6026 - 2.5 log<sub>10</sub>(&Delta; Flux + 0.0000)</p></div>
            '''
        )

        self.assertEqual(parsed['metadata']['RA'], '+18:10:53.91')
        self.assertEqual(parsed['metadata']['Dec'], '-25:44:46.00')
        self.assertEqual(parsed['micro']['tE'], '62.94')
        self.assertEqual(parsed['phot_href'], 'phot/2019-BLG-397')
        self.assertEqual(parsed['calibration_equation'], 'I = 27.6026 - 2.5 log10(Δ Flux + 0.0000)')

    def test_parse_event_page_accepts_malformed_calibration_block(self):
        parsed = _parse_event_page(
            '''
            <div id="external"><a href="phot/2003-BLG-008">Photometry data file (gzipped file)</a></div>
            <div id="calib">
            <h4>Calibration</h4>
            <p>I = 0.0000 - 2.5 log<sub>10</sub>(&Delta; Flux + 0.0000)
            </html>
            '''
        )

        self.assertEqual(parsed['phot_href'], 'phot/2003-BLG-008')
        self.assertEqual(parsed['calibration_equation'], 'I = 0.0000 - 2.5 log10(Δ Flux + 0.0000)')

    def test_query_targets_by_name_includes_calibrated_photometry(self):
        service = MOADataService()
        catalog_rows = [{
            'Event': 'MOA-2019-BLG-0397',
            'ra_deg': '272.724625',
            'dec_deg': '-25.7461111111',
        }]
        event_page = {
            'event_name': 'MOA-2019-BLG-0397',
            'page_url': 'https://moaprime.massey.ac.nz/moaarchive/event/2019-BLG-397',
            'phot_url': 'https://moaprime.massey.ac.nz/moaarchive/event/phot/2019-BLG-397',
            'calibration_equation': 'I = 27.6026 - 2.5 log10(Delta Flux + 1000.0)',
        }
        calibrated_rows = [
            {'jd': 2458796.19, 'magnitude': 20.1026, 'error': 0.0109, 'filter': 'MOA(Red)'},
        ]

        with patch.object(service, '_fetch_catalog_rows', return_value=catalog_rows), patch.object(
            service, '_fetch_event_page', return_value=event_page
        ), patch.object(service, '_fetch_calibrated_photometry', return_value=calibrated_rows):
            results = service.query_targets({'target_name': '2019-BLG-397', 'include_photometry': True})

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], 'MOA-2019-BLG-0397')
        self.assertEqual(results[0]['aliases'], ['MOA-2019-BLG-0397'])
        photometry = results[0]['reduced_datums']['photometry']
        self.assertEqual(len(photometry), 1)
        self.assertEqual(
            results[0]['source_location'],
            'https://moaprime.massey.ac.nz/moaarchive/event/2019-BLG-397',
        )
        self.assertEqual(photometry[0]['value']['filter'], 'MOA(Red)')
        self.assertAlmostEqual(photometry[0]['value']['magnitude'], 20.1026)

    def test_fetch_calibrated_photometry_warns_when_flux_calibration_missing(self):
        service = MOADataService()
        event_page = {
            'event_name': 'MOA-2003-BLG-0008',
            'phot_url': 'https://example.invalid/moa.dat.gz',
            'calibration_equation': 'I = 0.0000 - 2.5 log10(Δ Flux + 0.0000)',
        }
        response = Mock()
        response.content = b'2452909.10 123.0 4.0 frame\n'
        response.text = '2452909.10 123.0 4.0 frame\n'

        with patch.object(service, '_request', return_value=response), patch(
            'custom_code.data_services.moa_dataservice.logger.warning'
        ) as mocked_warning:
            rows = service._fetch_calibrated_photometry(event_page)

        self.assertEqual(rows, [])
        mocked_warning.assert_called_with(
            'MOA data exists for %s but no flux calibration is provided.',
            'MOA-2003-BLG-0008',
        )

    def test_fetch_calibrated_photometry_drops_points_with_mag_errors_above_one_mag(self):
        service = MOADataService()
        event_page = {
            'event_name': 'MOA-2013-BLG-0008',
            'phot_url': 'https://example.invalid/moa.dat.gz',
            'calibration_equation': 'I = 27.6026 - 2.5 log10(Δ Flux + 1000.0)',
        }
        response = Mock()
        response.content = (
            b'2453658.854530 1000.0 100.0 frame\n'
            b'2453658.903580 10.0 100.0 frame\n'
        )
        response.text = response.content.decode('utf-8')

        with patch.object(service, '_request', return_value=response):
            rows = service._fetch_calibrated_photometry(event_page)

        self.assertEqual(len(rows), 1)
        self.assertLessEqual(rows[0]['error'], 1.0)


class DataServicePersistenceTests(TestCase):
    def test_run_service_for_target_persists_transit_ephemeris(self):
        target = Target.objects.create(
            name='WASP-12b',
            type=Target.SIDEREAL,
            ra=1.0,
            dec=2.0,
            epoch=2000.0,
            description='Original description from create form',
        )

        class StubService:
            name = 'ExoClock'

            @classmethod
            def get_form_class(cls):
                return ExoClockDataService.get_form_class()

            def build_query_parameters(self, parameters, **kwargs):
                return parameters

            def query_targets(self, query_parameters, **kwargs):
                return [{
                    'target_updates': {
                        'ra': 97.63665,
                        'dec': 29.672296,
                        'epoch': 2000.0,
                        'classification': 'Planetary Transit',
                    },
                    'aliases': [{'name': 'WASP-12', 'url': 'https://www.exoclock.space/database/planets/WASP-12b'}],
                    'transit_ephemeris_updates': {
                        'source_name': 'ExoClock',
                        'source_url': 'https://www.exoclock.space/database/planets/WASP-12b',
                        'planet_name': 'WASP-12b',
                        'host_name': 'WASP-12',
                        'priority': 'low',
                        'period_days': 1.091418859,
                        'duration_hours': 3.0,
                        'depth_r_mmag': 17.81,
                        't0_bjd_tdb': 2457368.4973,
                        'recent_observations': 26,
                        'payload': {'name': 'WASP-12b'},
                    },
                }]

        _run_service_for_target(target, 'ExoClock', StubService, force_all_services=False)

        target.refresh_from_db()
        self.assertEqual(target.ra, 97.63665)
        self.assertEqual(target.classification, 'Planetary Transit')
        self.assertEqual(target.description, 'Original description from create form')
        self.assertTrue(target.aliases.filter(name='WASP-12').exists())
        ephemeris = TransitEphemeris.objects.get(target=target)
        self.assertEqual(ephemeris.planet_name, 'WASP-12b')
        self.assertEqual(ephemeris.host_name, 'WASP-12')
        self.assertEqual(ephemeris.recent_observations, 26)

    def test_run_service_for_target_persists_gaia_astrometry_fields(self):
        target = Target.objects.create(
            name='GaiaDR3_123',
            type=Target.SIDEREAL,
            ra=1.0,
            dec=2.0,
            epoch=2000.0,
        )

        class StubService:
            name = 'GaiaDR3'

            @classmethod
            def get_form_class(cls):
                return GaiaDR3DataService.get_form_class()

            def build_query_parameters(self, parameters, **kwargs):
                return parameters

            def query_targets(self, query_parameters, **kwargs):
                return [{
                    'target_updates': {
                        'pm_ra': 4.5,
                        'pm_dec': -6.7,
                        'parallax': 1.2,
                        'pm_ra_error': 0.11,
                        'pm_dec_error': 0.22,
                        'parallax_error': 0.33,
                        'gaia_variability_type': 'RR',
                    },
                }]

        _run_service_for_target(target, 'GaiaDR3', StubService, force_all_services=False)

        target.refresh_from_db()
        self.assertEqual(target.pm_ra, 4.5)
        self.assertEqual(target.pm_dec, -6.7)
        self.assertEqual(target.parallax, 1.2)
        self.assertEqual(target.pm_ra_error, 0.11)
        self.assertEqual(target.pm_dec_error, 0.22)
        self.assertEqual(target.parallax_error, 0.33)
        self.assertEqual(target.gaia_variability_type, 'RR')

    def test_run_service_for_target_skips_alias_owned_by_another_target(self):
        owner = Target.objects.create(
            name='OwnerTarget',
            type=Target.SIDEREAL,
            ra=10.0,
            dec=20.0,
            epoch=2000.0,
        )
        TargetName.objects.create(target=owner, name='SharedAlias')
        target = Target.objects.create(
            name='OtherTarget',
            type=Target.SIDEREAL,
            ra=30.0,
            dec=40.0,
            epoch=2000.0,
        )

        class StubService:
            name = 'ExoClock'

            @classmethod
            def get_form_class(cls):
                return ExoClockDataService.get_form_class()

            def build_query_parameters(self, parameters, **kwargs):
                return parameters

            def query_targets(self, query_parameters, **kwargs):
                return [{
                    'aliases': [{'name': 'SharedAlias', 'url': 'https://example.invalid/shared'}],
                }]

        _run_service_for_target(target, 'ExoClock', StubService, force_all_services=False)

        self.assertFalse(target.aliases.filter(name='SharedAlias').exists())
        self.assertEqual(TargetName.objects.get(name='SharedAlias').target_id, owner.id)

    def test_run_service_for_target_logs_timeout_and_allows_next_service(self):
        target = Target.objects.create(
            name='TimeoutTarget',
            type=Target.SIDEREAL,
            ra=30.0,
            dec=40.0,
            epoch=2000.0,
        )

        class HangingService:
            name = 'Hanging'

            @classmethod
            def get_form_class(cls):
                return ExoClockDataService.get_form_class()

            def build_query_parameters(self, parameters, **kwargs):
                return parameters

            def query_targets(self, query_parameters, **kwargs):
                time.sleep(5)
                return [{'aliases': ['ShouldNotPersist']}]

        class NextService:
            name = 'Next'

            @classmethod
            def get_form_class(cls):
                return ExoClockDataService.get_form_class()

            def build_query_parameters(self, parameters, **kwargs):
                return parameters

            def query_targets(self, query_parameters, **kwargs):
                return [{'aliases': ['RecoveredAlias']}]

        with self.settings(DATA_SERVICE_JOB_TIMEOUT=1):
            started_at = time.monotonic()
            _run_service_for_target(target, 'Hanging', HangingService, force_all_services=False)
            elapsed = time.monotonic() - started_at
            _run_service_for_target(target, 'Next', NextService, force_all_services=False)

        self.assertLess(elapsed, 3)
        self.assertFalse(target.aliases.filter(name='ShouldNotPersist').exists())
        self.assertTrue(target.aliases.filter(name='RecoveredAlias').exists())

    def test_run_service_for_target_logs_exception_and_allows_next_service(self):
        target = Target.objects.create(
            name='ExceptionTarget',
            type=Target.SIDEREAL,
            ra=30.0,
            dec=40.0,
            epoch=2000.0,
        )

        class TimeoutService:
            name = 'Timeout'

            @classmethod
            def get_form_class(cls):
                return ExoClockDataService.get_form_class()

            def build_query_parameters(self, parameters, **kwargs):
                return parameters

            def query_targets(self, query_parameters, **kwargs):
                raise requests.Timeout('simulated timeout')

        class NextService:
            name = 'Next'

            @classmethod
            def get_form_class(cls):
                return ExoClockDataService.get_form_class()

            def build_query_parameters(self, parameters, **kwargs):
                return parameters

            def query_targets(self, query_parameters, **kwargs):
                return [{'aliases': ['AfterTimeoutAlias']}]

        _run_service_for_target(target, 'Timeout', TimeoutService, force_all_services=False)
        _run_service_for_target(target, 'Next', NextService, force_all_services=False)

        self.assertTrue(target.aliases.filter(name='AfterTimeoutAlias').exists())

    def test_db_worker_recovers_stale_running_tasks(self):
        from custom_code.management.commands.db_worker import ScheduledStatusWorker

        worker = ScheduledStatusWorker(
            queue_names=['default'],
            interval=1,
            batch=False,
            backend_name='default',
            startup_delay=False,
            status_interval=0,
            dataservices_interval=0,
            dataservices_importance_gt=0,
            configure_signal_handlers=False,
        )
        worker.stale_running_after = 7200

        stale_queryset = Mock()
        stale_queryset.values_list.return_value = ['task-1']
        stale_queryset.filter.return_value = stale_queryset
        update_queryset = Mock()
        update_queryset.update.return_value = 1
        manager = Mock()
        manager.filter.side_effect = [stale_queryset, update_queryset]

        with patch('custom_code.management.commands.db_worker.DBTaskResult') as db_task_result:
            db_task_result.objects = manager
            worker.recover_stale_running_tasks()

        stale_filter_kwargs = manager.filter.call_args_list[0].kwargs
        update_kwargs = manager.filter.call_args_list[1].kwargs
        update_queryset.update.assert_called_once_with(status='NEW', started_at=None)
        self.assertEqual(stale_filter_kwargs['status'], 'RUNNING')
        self.assertEqual(stale_filter_kwargs['finished_at__isnull'], True)
        self.assertEqual(update_kwargs['id__in'], ['task-1'])

    def test_build_query_parameters_for_exoclock_uses_cone_search_radius(self):
        target = Target.objects.create(name='Gaia DR3 123', type=Target.SIDEREAL, ra=97.63665, dec=29.672296, epoch=2000.0)

        params = _build_query_parameters_for_service(target, 'ExoClock', ExoClockDataService())

        self.assertEqual(params['target_name'], 'Gaia DR3 123')
        self.assertEqual(params['ra'], 97.63665)
        self.assertEqual(params['dec'], 29.672296)
        self.assertEqual(params['radius_arcsec'], 30.0)

    def test_build_query_parameters_for_asassn_includes_target_names(self):
        target = Target.objects.create(name='AT2025abc', type=Target.SIDEREAL, ra=97.63665, dec=29.672296, epoch=2000.0)
        target.aliases.create(name='ASASSN-25ab')

        params = _build_query_parameters_for_service(target, 'ASASSN', ASASSNDataService())

        self.assertEqual(params['target_name'], 'AT2025abc')
        self.assertEqual(params['target_names'], ['AT2025abc', 'ASASSN-25ab'])
        self.assertEqual(params['ra'], 97.63665)
        self.assertEqual(params['dec'], 29.672296)

    def test_transit_ephemeris_computes_next_transit_from_bjd_tdb(self):
        target = Target.objects.create(name='TestTransit', type=Target.SIDEREAL, ra=1.0, dec=2.0, epoch=2000.0)
        now = Time('2026-04-10T12:00:00', scale='utc')
        ephemeris = TransitEphemeris.objects.create(
            target=target,
            planet_name='TestTransit',
            t0_bjd_tdb=now.tdb.jd - 0.25,
            period_days=1.0,
            duration_hours=2.0,
        )

        with patch('custom_code.models.Time.now', return_value=now):
            next_transit = ephemeris.next_transit_time()
            hours_until = ephemeris.hours_until_next_transit()
            window = ephemeris.next_transit_window_display()

        self.assertIsNotNone(next_transit)
        self.assertIsNotNone(hours_until)
        self.assertIsNotNone(window)
        self.assertAlmostEqual(hours_until, 18.0, places=1)
        self.assertEqual(next_transit.date().isoformat(), '2026-04-11')
        self.assertAlmostEqual(window['ingress']['hours'], 17.0, places=1)
        self.assertAlmostEqual(window['egress']['hours'], 19.0, places=1)


class DataServiceCoordinateFormTests(TestCase):
    def test_coordinate_form_accepts_decimal_degrees(self):
        form = SimbadQueryForm(data={'ra': '267.4127916666667', 'dec': '-30.452333333333332'})

        self.assertTrue(form.is_valid(), form.errors)
        self.assertAlmostEqual(form.cleaned_data['ra'], 267.4127916666667)
        self.assertAlmostEqual(form.cleaned_data['dec'], -30.452333333333332)

    def test_coordinate_form_accepts_sexagesimal_values(self):
        form = SimbadQueryForm(data={'ra': '17:49:39.07', 'dec': '-30:27:08.4'})

        self.assertTrue(form.is_valid(), form.errors)
        self.assertAlmostEqual(form.cleaned_data['ra'], 267.4127916666667)
        self.assertAlmostEqual(form.cleaned_data['dec'], -30.452333333333332)

    def test_coordinate_form_accepts_space_separated_sexagesimal_ra(self):
        form = SimbadQueryForm(data={'ra': '17 49 39.07', 'dec': '-30 27 08.4'})

        self.assertTrue(form.is_valid(), form.errors)
        self.assertAlmostEqual(form.cleaned_data['ra'], 267.4127916666667)
        self.assertAlmostEqual(form.cleaned_data['dec'], -30.452333333333332)

    def test_coordinate_form_rejects_invalid_value(self):
        form = SimbadQueryForm(data={'ra': 'not-a-coordinate', 'dec': '-30:27:08.4'})

        self.assertFalse(form.is_valid())
        self.assertIn('ra', form.errors)

    def test_coordinate_form_accepts_target_name_only(self):
        form = WISEQueryForm(data={'target_name': 'Gaia24abc'})

        self.assertTrue(form.is_valid(), form.errors)

    def test_gs6df_form_uses_arcsec_radius_field(self):
        form = GS6dFQueryForm(data={'target_name': 'Gaia24abc'})

        self.assertIn('radius_arcsec', form.fields)
        self.assertNotIn('radius_arcmin', form.fields)
        self.assertTrue(form.is_valid(), form.errors)
        self.assertEqual(form.cleaned_data['radius_arcsec'], 5.0)

    def test_lamost_and_galah_forms_default_to_two_point_five_arcsec_radius(self):
        for form_class in (LAMOSTQueryForm, GALAHQueryForm):
            with self.subTest(form_class=form_class.__name__):
                form = form_class(data={'target_name': 'Gaia24abc'})

                self.assertTrue(form.is_valid(), form.errors)
                self.assertEqual(form.cleaned_data['radius_arcsec'], 2.5)

    def test_gaia_form_accepts_target_name_only(self):
        form = GaiaDR3QueryForm(data={'target_name': 'GaiaDR3_123'})

        self.assertTrue(form.is_valid(), form.errors)


class CatalogQueryCoordinateFormTests(TestCase):
    def test_catalog_query_accepts_decimal_degrees(self):
        form = BhtomCatalogQueryForm(data={
            'service': 'Simbad',
            'ra': '21.4001011',
            'dec': '34.1517361',
        })

        self.assertTrue(form.is_valid(), form.errors)
        self.assertAlmostEqual(form.cleaned_data['ra'], 21.4001011)
        self.assertAlmostEqual(form.cleaned_data['dec'], 34.1517361)

    def test_catalog_query_accepts_sexagesimal_coordinates(self):
        form = BhtomCatalogQueryForm(data={
            'service': 'Simbad',
            'ra': '01:25:36.024',
            'dec': '+34:09:06.25',
        })

        self.assertTrue(form.is_valid(), form.errors)
        self.assertAlmostEqual(form.cleaned_data['ra'], 21.4001, places=4)
        self.assertAlmostEqual(form.cleaned_data['dec'], 34.1517, places=4)


class DataServiceTargetNameResolutionTests(TestCase):
    def test_resolve_query_coordinates_uses_primary_target_name(self):
        Target.objects.create(name='TargetA', type=Target.SIDEREAL, ra=12.3, dec=-45.6, epoch=2000.0)

        target_name, ra, dec = resolve_query_coordinates({'target_name': 'TargetA'})

        self.assertEqual(target_name, 'TargetA')
        self.assertEqual(ra, 12.3)
        self.assertEqual(dec, -45.6)

    def test_resolve_query_coordinates_uses_alias(self):
        target = Target.objects.create(name='TargetB', type=Target.SIDEREAL, ra=98.7, dec=6.5, epoch=2000.0)
        target.aliases.create(name='AliasB')

        target_name, ra, dec = resolve_query_coordinates({'target_name': 'AliasB'})

        self.assertEqual(target_name, 'AliasB')
        self.assertEqual(ra, 98.7)
        self.assertEqual(dec, 6.5)


class TwoMASSDataServiceTests(TestCase):
    def test_query_targets_returns_jhk_photometry(self):
        service = TwoMASSDataService()
        raw_response = '\n'.join([
            'intro',
            'null|',
            '12.345 -45.678 0 0 12345678+1234567 13.1 0.02 12.7 0.03 12.4 0.04 0.1 0.0',
        ])

        with patch('custom_code.data_services.twomass_dataservice.requests.get', return_value=Mock(text=raw_response)):
            results = service.query_targets({
                'ra': 12.345,
                'dec': -45.678,
                'radius_arcsec': 3.0,
                'include_photometry': True,
            })

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], '2MASS_12345678+1234567')
        self.assertEqual(results[0]['aliases'], ['2MASS_12345678+1234567'])
        photometry = results[0]['reduced_datums']['photometry']
        self.assertEqual([datum['value']['filter'] for datum in photometry], ['2MASS(J)', '2MASS(H)', '2MASS(K)'])
        self.assertEqual(photometry[0]['value']['magnitude'], 13.1)
        self.assertEqual(photometry[1]['value']['error'], 0.03)


class GaiaDR3DataServiceTests(TestCase):
    def test_query_targets_maps_astrometry_and_errors(self):
        service = GaiaDR3DataService()

        with patch.object(service, 'query_service', return_value={
            'source': {
                'source_id': '123',
                'ra': 12.3,
                'dec': -45.6,
                'pmra': 4.5,
                'pmdec': -6.7,
                'parallax': 1.2,
                'pmra_error': 0.11,
                'pmdec_error': 0.22,
                'parallax_error': 0.33,
                'gaia_variability_type': 'RR',
            },
            'photometry_rows': [],
            'spectroscopy_rows': [],
        }):
            results = service.query_targets({'source_id': '123'})

        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result['parallax'], 1.2)
        self.assertEqual(result['pmra'], 4.5)
        self.assertEqual(result['pmdec'], -6.7)
        self.assertEqual(result['target_updates']['parallax'], 1.2)
        self.assertEqual(result['target_updates']['pm_ra'], 4.5)
        self.assertEqual(result['target_updates']['pm_dec'], -6.7)
        self.assertEqual(result['target_updates']['parallax_error'], 0.33)
        self.assertEqual(result['target_updates']['pm_ra_error'], 0.11)
        self.assertEqual(result['target_updates']['pm_dec_error'], 0.22)
        self.assertEqual(result['gaia_variability_type'], 'RR')
        self.assertEqual(result['target_updates']['gaia_variability_type'], 'RR')

    def test_query_service_backfills_variability_type_when_preferred_classifier_missing(self):
        service = GaiaDR3DataService()
        source_row = {
            'source_id': '123',
            'ra': 12.3,
            'dec': -45.6,
            'pmra': 4.5,
            'pmdec': -6.7,
            'parallax': 1.2,
            'gaia_variability_type': None,
        }

        with patch.object(service, '_query_source_esa', return_value=source_row.copy()), \
             patch.object(service, '_query_variability_esa', return_value=[
                 {'source_id': '123', 'best_class_name': 'EA', 'classifier_name': 'general'},
             ]):
            result = service.query_service({'source_id': '123', 'include_photometry': False, 'include_spectroscopy': False})

        self.assertEqual(result['source']['gaia_variability_type'], 'EA')


class AliasHandlingTests(TestCase):
    def test_alias_formset_allows_alias_matching_primary_target_name(self):
        target = Target.objects.create(
            name='GaiaDR3_2929359977275703552',
            type=Target.SIDEREAL,
            ra=12.3,
            dec=-45.6,
            epoch=2000.0,
        )
        alias = TargetName.objects.create(target=target, name='GaiaDR3_2929359977275703552')

        formset = BhtomTargetNamesFormset(
            data={
                'aliases-TOTAL_FORMS': '1',
                'aliases-INITIAL_FORMS': '1',
                'aliases-MIN_NUM_FORMS': '0',
                'aliases-MAX_NUM_FORMS': '1000',
                'aliases-0-id': str(alias.id),
                'aliases-0-target': str(target.id),
                'aliases-0-name': 'GaiaDR3_2929359977275703552',
                'aliases-0-url': 'https://gea.esac.esa.int/archive/',
                'aliases-0-source_name': 'GaiaDR3',
            },
            instance=target,
            prefix='aliases',
        )

        self.assertTrue(formset.is_valid(), formset.errors)

    def test_get_or_create_target_alias_skips_primary_target_name(self):
        target = Target.objects.create(
            name='GaiaDR3_2929359977275703552',
            type=Target.SIDEREAL,
            ra=12.3,
            dec=-45.6,
            epoch=2000.0,
        )

        alias_obj, created = _get_or_create_target_alias(target, 'GaiaDR3_2929359977275703552')

        self.assertIsNone(alias_obj)
        self.assertFalse(created)
        self.assertFalse(target.aliases.filter(name='GaiaDR3_2929359977275703552').exists())


class GaiaAlertsDataServiceTests(TestCase):
    def test_query_targets_returns_linked_alias_for_alert_page(self):
        service = GaiaAlertsDataService()
        alert_rows = [{
            '#Name': 'Gaia26abc',
            'RaDeg': '12.3',
            'DecDeg': '-45.6',
        }]
        photometry_rows = [{'jd': 2460000.5, 'mag': '18.2'}]

        with patch.object(service, '_fetch_alerts_rows', return_value=alert_rows), patch.object(
            service, '_fetch_lightcurve_rows', return_value=photometry_rows
        ):
            results = service.query_targets({'alert_name': 'Gaia26abc', 'include_photometry': True})

        self.assertEqual(len(results), 1)
        self.assertEqual(
            results[0]['aliases'],
            [{
                'name': 'Gaia26abc',
                'url': 'https://gsaweb.ast.cam.ac.uk/alerts/alert/Gaia26abc',
                'source_name': 'GaiaAlerts',
            }],
        )
        self.assertIn('photometry', results[0]['reduced_datums'])

    def test_run_service_for_target_stores_gaia_alert_alias_link(self):
        target = Target.objects.create(name='Gaia26abc-target', type='SIDEREAL', ra=12.3, dec=-45.6, epoch=2000.0)

        class FakeGaiaAlertsService:
            info_url = 'https://gsaweb.ast.cam.ac.uk/alerts'

            def build_query_parameters(self, parameters):
                return parameters

            def query_targets(self, built_parameters):
                return [{
                    'aliases': [{
                        'name': 'Gaia26abc',
                        'url': 'https://gsaweb.ast.cam.ac.uk/alerts/alert/Gaia26abc',
                        'source_name': 'GaiaAlerts',
                    }],
                    'source_location': 'https://gsaweb.ast.cam.ac.uk/alerts/alert/Gaia26abc/lightcurve.csv',
                }]

            def to_reduced_datums(self, target, reduced_datums):
                return None

        _run_service_for_target(target, 'GaiaAlerts', FakeGaiaAlertsService)

        alias = target.aliases.select_related('alias_info').get(name='Gaia26abc')
        self.assertEqual(alias.alias_info.source_name, 'GaiaAlerts')
        self.assertEqual(alias.alias_info.url, 'https://gsaweb.ast.cam.ac.uk/alerts/alert/Gaia26abc')


class ASASSNDataServiceTests(TestCase):
    def test_normalize_transient_name_accepts_common_asassn_variants(self):
        expected = '25ab'
        self.assertEqual(_normalize_transient_name('ASASSN-25ab'), expected)
        self.assertEqual(_normalize_transient_name('ASAS-SN 25ab'), expected)
        self.assertEqual(_normalize_transient_name('asas sn 25 ab'), expected)
        self.assertEqual(_normalize_transient_name('25 ab'), expected)
        self.assertEqual(_normalize_transient_name('2025ab'), expected)

    def test_build_query_parameters_preserves_target_names(self):
        service = ASASSNDataService()

        params = service.build_query_parameters({
            'target_name': 'Primary',
            'target_names': ['Primary', 'ASASSN-17cf'],
            'ra': 12.3,
            'dec': -45.6,
            'include_photometry': False,
        })

        self.assertEqual(params['target_names'], ['Primary', 'ASASSN-17cf'])
        self.assertEqual(params['radius_arcsec'], 7.0)

    def test_query_targets_handles_missing_lightcurve_tables(self):
        service = ASASSNDataService()

        with patch.object(service, 'query_service', return_value={
            'asassn_id': '123',
            'lc_filtered': None,
            'lc_limits': None,
            'source_location': 'https://example.invalid/asassn/123',
            'ra': 12.3,
            'dec': -45.6,
        }):
            results = service.query_targets({'ra': 12.3, 'dec': -45.6})

        self.assertEqual(results, [])

    def test_query_targets_resolves_transient_name_when_coordinates_missing(self):
        service = ASASSNDataService()
        transient_row = {
            'name': 'ASASSN-25ab',
            'asassn_name': 'ASASSN-25ab',
            'ra': 12.3,
            'dec': -45.6,
            'source_location': 'https://example.invalid/asassn/transients.html',
        }

        client = Mock()
        client.cone_search.return_value = __import__('pandas').DataFrame()

        with patch('custom_code.data_services.asassn_dataservice._fetch_transient_rows', return_value=[transient_row]), patch(
            'custom_code.data_services.asassn_dataservice.SkyPatrolClient',
            return_value=client,
        ):
            results = service.query_targets({'target_name': 'asas sn 25 ab', 'include_photometry': False})

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], 'ASASSN-25ab')
        self.assertEqual(results[0]['aliases'], ['ASASSN-25ab'])
        self.assertEqual(results[0]['ra'], 12.3)
        self.assertEqual(results[0]['dec'], -45.6)

    def test_query_targets_resolves_transient_from_target_names_aliases(self):
        service = ASASSNDataService()
        transient_row = {
            'name': 'ASASSN-25ab',
            'asassn_name': 'ASASSN-25ab',
            'lookup_aliases': ['ASASSN-25ab', 'AT2025abc'],
            'ra': 12.3,
            'dec': -45.6,
            'source_location': 'https://example.invalid/asassn/transients.html',
        }

        client = Mock()
        client.cone_search.return_value = __import__('pandas').DataFrame()

        with patch('custom_code.data_services.asassn_dataservice._fetch_transient_rows', return_value=[transient_row]), patch(
            'custom_code.data_services.asassn_dataservice.SkyPatrolClient',
            return_value=client,
        ):
            results = service.query_targets({
                'target_name': 'Unrelated primary name',
                'target_names': ['Unrelated primary name', 'AT2025abc'],
                'ra': 12.3,
                'dec': -45.6,
                'include_photometry': False,
            })

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], 'ASASSN-25ab')
        self.assertEqual(results[0]['aliases'], ['ASASSN-25ab'])

    def test_query_targets_combines_catalog_id_and_transient_name(self):
        service = ASASSNDataService()
        transient_row = {
            'name': 'ASASSN-17cf',
            'asassn_name': 'ASASSN-17cf',
            'lookup_aliases': ['ASASSN-17cf'],
            'ra': 12.3,
            'dec': -45.6,
            'source_location': 'https://example.invalid/asassn/transients.html',
        }

        client = Mock()
        client.cone_search.return_value = __import__('pandas').DataFrame([
            {'asas_sn_id': 661428703026, 'ra_deg': 12.3, 'dec_deg': -45.6},
        ])

        with patch('custom_code.data_services.asassn_dataservice._fetch_transient_rows', return_value=[transient_row]), patch(
            'custom_code.data_services.asassn_dataservice.SkyPatrolClient',
            return_value=client,
        ):
            results = service.query_targets({
                'target_name': 'ASASSN-17cf',
                'ra': 12.3,
                'dec': -45.6,
                'include_photometry': False,
            })

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], 'ASASSN-17cf')
        self.assertEqual(results[0]['aliases'], ['661428703026/ASASSN-17cf'])

    def test_query_targets_does_not_emit_other_id_as_asassn_alias(self):
        service = ASASSNDataService()
        transient_row = {
            'name': '',
            'asassn_name': '',
            'lookup_aliases': ['ATLAS25abc'],
            'ra': 12.3,
            'dec': -45.6,
            'source_location': 'https://example.invalid/asassn/transients.html',
        }

        client = Mock()
        client.cone_search.return_value = __import__('pandas').DataFrame()

        with patch('custom_code.data_services.asassn_dataservice._fetch_transient_rows', return_value=[transient_row]), patch(
            'custom_code.data_services.asassn_dataservice.SkyPatrolClient',
            return_value=client,
        ):
            results = service.query_targets({
                'target_name': 'ATLAS25abc',
                'ra': 12.3,
                'dec': -45.6,
                'include_photometry': False,
            })

        self.assertEqual(results, [])


class SpectroscopicDataServiceRadiusTests(TestCase):
    @patch('custom_code.data_services.lamost_dataservice.requests.get')
    def test_lamost_query_uses_two_point_five_arcsec_radius_in_degrees(self, mock_get):
        mock_get.return_value.json.return_value = []

        LAMOSTDataService().query_service({'ra': 12.3, 'dec': -45.6})

        url = mock_get.call_args.args[0]
        self.assertIn('radius=0.0006944444444444445', url)

    def test_lamost_build_query_parameters_defaults_to_two_point_five_arcsec_radius(self):
        params = LAMOSTDataService().build_query_parameters({
            'target_name': 'Gaia24abc',
            'ra': 12.3,
            'dec': -45.6,
        })

        self.assertEqual(params['radius_arcsec'], 2.5)

    @patch('custom_code.data_services.galah_dataservice.SSAService')
    def test_galah_query_sends_two_point_five_arcsec_radius_as_diameter(self, mock_service_class):
        mock_service = mock_service_class.return_value
        mock_service.search.return_value.to_table.return_value = []

        GALAHDataService().query_service({'ra': 12.3, 'dec': -45.6})

        self.assertEqual(mock_service.search.call_args.kwargs['SIZE'], 5.0 / 3600.0)

    def test_galah_build_query_parameters_defaults_to_two_point_five_arcsec_radius(self):
        params = GALAHDataService().build_query_parameters({
            'target_name': 'Gaia24abc',
            'ra': 12.3,
            'dec': -45.6,
        })

        self.assertEqual(params['radius_arcsec'], 2.5)


class WISEDataServiceTests(TestCase):
    def test_allwise_uses_allwise_catalog_alias(self):
        service = AllWISEDataService()
        lc_data = __import__('pandas').DataFrame([
            {'mjd': 58000.0, 'w1mpro': 12.3, 'w1sigmpro': 0.1, 'w2mpro': 11.9, 'w2sigmpro': 0.1},
        ])

        with patch.object(service, 'query_service', return_value={
            'lc_data': lc_data,
            'alias': 'WISEA J123456.78+123456.7',
            'source_location': service.info_url,
            'ra': 12.3,
            'dec': -45.6,
        }):
            results = service.query_targets({'ra': 12.3, 'dec': -45.6})

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], 'WISE+J12.3_-45.6')
        self.assertEqual(
            results[0]['aliases'],
            [{'name': 'WISEA J123456.78+123456.7', 'source_name': 'AllWISE'}],
        )

    def test_allwise_falls_back_to_generated_alias(self):
        service = AllWISEDataService()
        lc_data = __import__('pandas').DataFrame([
            {'mjd': 58000.0, 'w1mpro': 12.3, 'w1sigmpro': 0.1, 'w2mpro': 11.9, 'w2sigmpro': 0.1},
        ])

        with patch.object(service, 'query_service', return_value={
            'lc_data': lc_data,
            'alias': None,
            'source_location': service.info_url,
            'ra': 12.3,
            'dec': -45.6,
        }):
            results = service.query_targets({'ra': 12.3, 'dec': -45.6})

        self.assertEqual(results[0]['aliases'], [{'name': 'AllWISE+J12.3_-45.6', 'source_name': 'AllWISE'}])

    def test_neowise_uses_allwise_catalog_alias(self):
        service = NeoWISEDataService()
        lc_data = __import__('pandas').DataFrame([
            {'mjd': 59000.0, 'w1mpro': 12.3, 'w1sigmpro': 0.1, 'w2mpro': 11.9, 'w2sigmpro': 0.1},
        ])

        with patch.object(service, 'query_service', return_value={
            'lc_data': lc_data,
            'alias': 'WISEA J123456.78+123456.7',
            'source_location': service.info_url,
            'ra': 12.3,
            'dec': -45.6,
        }):
            results = service.query_targets({'ra': 12.3, 'dec': -45.6})

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], 'WISE+J12.3_-45.6')
        self.assertEqual(
            results[0]['aliases'],
            [{'name': 'WISEA J123456.78+123456.7', 'source_name': 'AllWISE'}],
        )

    def test_neowise_falls_back_to_generated_alias(self):
        service = NeoWISEDataService()
        lc_data = __import__('pandas').DataFrame([
            {'mjd': 59000.0, 'w1mpro': 12.3, 'w1sigmpro': 0.1, 'w2mpro': 11.9, 'w2sigmpro': 0.1},
        ])

        with patch.object(service, 'query_service', return_value={
            'lc_data': lc_data,
            'alias': None,
            'source_location': service.info_url,
            'ra': 12.3,
            'dec': -45.6,
        }):
            results = service.query_targets({'ra': 12.3, 'dec': -45.6})

        self.assertEqual(results[0]['aliases'], [{'name': 'NeoWISE+J12.3_-45.6', 'source_name': 'NeoWISE'}])

    def test_run_service_for_target_replaces_literal_wise_alias(self):
        target = Target.objects.create(name='Gaia24abc', type='SIDEREAL', ra=12.3, dec=-45.6, epoch=2000.0)
        target.aliases.create(name='WISE')

        class FakeAllWISEService:
            def build_query_parameters(self, parameters):
                return parameters

            def query_targets(self, built_parameters):
                return [{
                    'aliases': [{'name': 'WISEA J123456.78+123456.7', 'source_name': 'AllWISE'}],
                    'source_location': 'https://example.invalid/wise',
                }]

            def to_reduced_datums(self, target, reduced_datums):
                return None

        _run_service_for_target(target, 'AllWISE', FakeAllWISEService)

        self.assertFalse(target.aliases.filter(name='WISE').exists())
        self.assertTrue(target.aliases.filter(name='WISEA J123456.78+123456.7').exists())


class KMTDataServiceTests(TestCase):
    def test_helpers_normalize_name_and_event_id(self):
        self.assertEqual(_normalize_kmt_event_name('2017-BLG-2573'), 'KMT-2017-BLG-2573')
        self.assertEqual(_normalize_kmt_event_name('KMT-2017-BLG-2573'), 'KMT-2017-BLG-2573')
        self.assertEqual(_kmt_event_id('KMT-2017-BLG-2573'), 'KB172573')

    def test_helper_normalizes_both_legacy_and_full_hjd_formats(self):
        self.assertEqual(_normalize_kmt_hjd(7837.28797), 2457837.28797)
        self.assertEqual(_normalize_kmt_hjd(2461083.25762), 2461083.25762)

    def test_query_targets_by_name_includes_photometry(self):
        service = KMTDataService()
        catalog_rows = [{
            'Event': 'KMT-2017-BLG-2573',
            'ra_deg': '266.54918',
            'dec_deg': '-25.62171',
        }]
        photometry_rows = [{
            'hjd': 2457870.11524,
            'magnitude': 19.31,
            'error': 0.03,
            'facility': 'CTIO_KMTC',
            'filter': 'KMT(I)',
        }]

        with patch.object(service, '_fetch_catalog_rows', return_value=catalog_rows), patch.object(
            service, '_fetch_photometry_rows', return_value=photometry_rows
        ):
            results = service.query_targets({'target_name': 'KMT-2017-BLG-2573', 'include_photometry': True})

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], 'KMT-2017-BLG-2573')
        self.assertEqual(results[0]['aliases'], ['KMT-2017-BLG-2573'])
        self.assertIn('photometry', results[0]['reduced_datums'])


class SimbadHarvesterTests(TestCase):
    def test_target_from_result_sets_j2000_epoch(self):
        target = target_from_result({
            'main_id': 'TYC 9194-662-1',
            'ra': 126.57054084958001,
            'dec': -67.90756133863,
            'pmra': None,
            'pmdec': None,
            'plx_value': None,
        })

        self.assertEqual(target.epoch, 2000.0)

    def test_target_from_result_does_not_create_alias_matching_normalized_target_name(self):
        target = target_from_result({
            'main_id': 'TYC 9194-662-1',
            'ra': 126.57054084958001,
            'dec': -67.90756133863,
            'pmra': None,
            'pmdec': None,
            'plx_value': None,
        })

        self.assertEqual(target.name, 'TYC_9194-662-1')
        self.assertEqual(target.extra_aliases, [])

    def test_other_sidereal_harvesters_set_j2000_epoch(self):
        crts = CRTSHarvester()
        crts.catalog_data = {'name': 'CRTS_J1', 'ra': 12.3, 'dec': -45.6}

        gaia_alerts = GaiaAlertsHarvester()
        gaia_alerts.catalog_data = {'#Name': 'Gaia26abc', 'RaDeg': '12.3', 'DecDeg': '-45.6', 'Comment': 'x'}

        gaia_dr3 = GaiaDR3Harvester()
        gaia_dr3.catalog_data = {'source_id': '123', 'ra': 12.3, 'dec': -45.6, 'parallax': None, 'pmra': None, 'pmdec': None}

        lsst = LSSTHarvester()
        lsst.catalog_data = {'lsst_id': '456', 'ra': 12.3, 'dec': -45.6}

        moa = MOAHarvester()
        moa.catalog_data = {'Event': 'MOA-2019-BLG-0397', 'ra_deg': '12.3', 'dec_deg': '-45.6'}

        kmt = KMTHarvester()
        kmt.catalog_data = {'Event': 'KMT-2017-BLG-2573', 'ra_deg': '12.3', 'dec_deg': '-45.6'}

        self.assertEqual(crts.to_target().epoch, 2000.0)
        self.assertEqual(gaia_alerts.to_target().epoch, 2000.0)
        self.assertEqual(gaia_dr3.to_target().epoch, 2000.0)
        self.assertEqual(lsst.to_target().epoch, 2000.0)
        self.assertEqual(moa.to_target().epoch, 2000.0)
        self.assertEqual(kmt.to_target().epoch, 2000.0)

    def test_gaia_dr3_harvester_maps_variability_class(self):
        gaia_dr3 = GaiaDR3Harvester()
        gaia_dr3.catalog_data = {
            'source_id': '123',
            'ra': 12.3,
            'dec': -45.6,
            'parallax': 1.2,
            'pmra': 4.5,
            'pmdec': -6.7,
            'gaia_variability_type': 'RR',
        }

        target = gaia_dr3.to_target()

        self.assertEqual(target.gaia_variability_type, 'RR')

    def test_gaia_dr3_variability_backfill_prefers_fallback_classifier(self):
        variability = gaia_dr3_harvester._select_variability_by_source([
            {'source_id': '123', 'best_class_name': 'EA', 'classifier_name': 'general'},
            {'source_id': '123', 'best_class_name': 'RR', 'classifier_name': 'n_transits:5+'},
            {'source_id': '456', 'best_class_name': 'CEP', 'classifier_name': 'general'},
        ])

        self.assertEqual(variability['123'], 'RR')
        self.assertEqual(variability['456'], 'CEP')

    def test_exoclock_harvester_maps_target_and_host_alias(self):
        exoclock = ExoClockHarvester()
        exoclock.catalog_data = {
            'name': 'WASP-12b',
            'star': 'WASP-12',
            'ra_j2000': '06:30:32.7966',
            'dec_j2000': '+29:40:20.266',
            't0_bjd_tdb': 2457368.4973,
            'period_days': 1.091418859,
        }

        target = exoclock.to_target()

        self.assertEqual(target.name, 'WASP-12b')
        self.assertEqual(target.type, 'SIDEREAL')
        self.assertEqual(target.epoch, 2000.0)
        self.assertEqual(target.classification, 'Planetary Transit')
        self.assertAlmostEqual(target.ra, 97.6366525)
        self.assertAlmostEqual(target.dec, 29.672296111111113)
        self.assertEqual(target.extra_aliases[0]['name'], 'WASP-12')
        self.assertEqual(target.extra_aliases[0]['source_name'], 'ExoClock')
        self.assertEqual(target.transit_source_name, 'ExoClock')
        self.assertEqual(target.transit_planet_name, 'WASP-12b')
        self.assertEqual(target.transit_host_name, 'WASP-12')
        self.assertEqual(target.transit_t0_bjd_tdb, 2457368.4973)
        self.assertEqual(target.transit_period_days, 1.091418859)


class CatalogServiceRegistrationTests(TestCase):
    def test_exoclock_is_listed_in_catalog_services(self):
        from tom_catalogs.harvester import get_service_classes

        self.assertIn('ExoClock', get_service_classes())

    def test_ogle_ews_is_listed_in_catalog_services(self):
        from tom_catalogs.harvester import get_service_classes

        self.assertIn('OGLE EWS', get_service_classes())

    def test_moa_is_listed_in_catalog_services(self):
        from tom_catalogs.harvester import get_service_classes

        self.assertIn('MOA', get_service_classes())

    def test_kmt_is_listed_in_catalog_services(self):
        from tom_catalogs.harvester import get_service_classes

        self.assertIn('KMT', get_service_classes())

    def test_twomass_is_not_listed_in_catalog_services(self):
        from tom_catalogs.harvester import get_service_classes

        self.assertNotIn('2MASS', get_service_classes())

    def test_twomass_is_listed_in_data_services(self):
        from custom_code.tasks import _get_data_service_classes

        self.assertIn('2MASS', _get_data_service_classes())

    def test_moa_is_listed_in_data_services(self):
        from custom_code.tasks import _get_data_service_classes

        self.assertIn('MOA', _get_data_service_classes())

    def test_kmt_is_listed_in_data_services(self):
        from custom_code.tasks import _get_data_service_classes

        self.assertIn('KMT', _get_data_service_classes())


class DataServiceSelectorViewTests(TestCase):
    def test_dataservices_create_without_service_renders_selector_page(self):
        user = get_user_model().objects.create_user(username='dataservices-selector', password='secret')
        self.client.force_login(user)

        response = self.client.get(reverse('dataservices:create'))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Select a data service')
        self.assertContains(response, 'All Data Services')
        self.assertContains(response, 'Saved Queries')

    def test_dataservices_create_exoclock_renders_advanced_filter_fields(self):
        user = get_user_model().objects.create_user(username='dataservices-exoclock-form', password='secret')
        self.client.force_login(user)

        response = self.client.get(reverse('dataservices:create'), {'data_service': 'ExoClock'})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Other Criteria')
        self.assertContains(response, 'Magnitude limit')
        self.assertContains(response, 'Eclipse depth min')
        self.assertContains(response, 'Compute from date')
        self.assertContains(response, 'Next transit within')

    def test_exoclock_catalog_query_redirect_prefills_transit_fields(self):
        exoclock = ExoClockHarvester()
        exoclock.catalog_data = {
            'name': 'WASP-12b',
            'star': 'WASP-12',
            'priority': 'low',
            'ra_j2000': '06:30:32.7966',
            'dec_j2000': '+29:40:20.266',
            't0_bjd_tdb': 2457368.4973,
            't0_unc': 5.9e-05,
            'period_days': 1.091418859,
            'period_unc': 3.9e-08,
            'duration_hours': 3.0,
            'depth_r_mmag': 17.81,
            'v_mag': 11.57,
            'r_mag': 11.288,
            'gaia_g_mag': 11.5,
        }
        target = exoclock.to_target()

        request = RequestFactory().get(reverse('tom_catalogs:query'))
        request.user = get_user_model().objects.create_user(username='catalog-exoclock', password='secret')

        view = BhtomCatalogQueryView()
        view.request = request
        view.target = target

        location = view.get_success_url()

        self.assertIn('classification=Planetary+Transit', location)
        self.assertIn('source_name=ExoClock', location)
        self.assertIn('source_url=https%3A%2F%2Fwww.exoclock.space%2Fdatabase%2Fplanets%2FWASP-12b', location)
        self.assertIn('planet_name=WASP-12b', location)
        self.assertIn('host_name=WASP-12', location)
        self.assertIn('priority=low', location)
        self.assertIn('t0_bjd_tdb=2457368.4973', location)
        self.assertIn('period_days=1.091418859', location)
        self.assertIn('recommended_observing_strategy=', location)

    def test_catalog_query_renders_single_match_results_without_second_lookup(self):
        request = RequestFactory().post(
            reverse('tom_catalogs:query'),
            data={'service': 'Simbad', 'term': 'Target 1'},
        )
        request.user = get_user_model().objects.create_user(username='catalog-single-match', password='secret')
        request.session = {}

        built_target = Target(name='Target_1', type='SIDEREAL', ra=12.3, dec=-45.6, epoch=2000.0)
        match = {'main_id': 'Target 1', 'ra': 12.3, 'dec': -45.6}

        with patch('custom_code.views._get_catalog_matches', return_value=[match]) as get_matches, patch(
            'custom_code.views._build_catalog_target_from_match',
            return_value=built_target,
        ) as build_target, patch(
            'custom_code.forms.BhtomCatalogQueryForm.get_target',
            side_effect=AssertionError('form.get_target should not be called when a single match is already available'),
        ):
            response = BhtomCatalogQueryView.as_view()(request)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Create')
        self.assertContains(response, 'Target_1')
        get_matches.assert_called_once()
        build_target.assert_called_once_with('Simbad', match)

    def test_catalog_query_returns_object_not_found_without_repeat_lookup(self):
        request = RequestFactory().post(
            reverse('tom_catalogs:query'),
            data={'service': 'Simbad', 'term': 'Missing Target'},
        )
        request.user = get_user_model().objects.create_user(username='catalog-missing-target', password='secret')
        request.session = {}

        with patch('custom_code.views._get_catalog_matches', return_value=[]) as get_matches, patch(
            'custom_code.forms.BhtomCatalogQueryForm.get_target',
            side_effect=AssertionError('form.get_target should not be called when no catalog matches are already known'),
        ) as get_target:
            response = BhtomCatalogQueryView.as_view()(request)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Object not found')
        get_matches.assert_called_once()
        get_target.assert_not_called()

    def test_catalog_all_services_includes_tns_single_result_lookups(self):
        request = RequestFactory().post(
            reverse('tom_catalogs:query'),
            data={'service': ALL_DATA_SERVICES_VALUE, 'term': 'SN 2026abc'},
        )
        request.user = get_user_model().objects.create_user(username='catalog-all-tns', password='secret')
        request.session = {}

        class FakeTNSHarvester:
            name = 'TNS'

            def query(self, term):
                self.term = term

            def to_target(self):
                return Target(name='SN 2026abc', type='SIDEREAL', ra=12.3, dec=-45.6, epoch=2000.0)

        with patch('custom_code.views.get_service_classes', return_value={'TNS': FakeTNSHarvester}), patch(
            'custom_code.views._get_catalog_matches',
            return_value=[],
        ):
            response = BhtomCatalogQueryView.as_view()(request)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'SN 2026abc')
        self.assertContains(response, 'TNS')
        self.assertContains(response, 'Create')

    def test_catalog_all_services_skips_irrelevant_harvesters_for_tns_like_terms(self):
        request = RequestFactory().post(
            reverse('tom_catalogs:query'),
            data={'service': ALL_DATA_SERVICES_VALUE, 'term': 'SN 2026abc'},
        )
        request.user = get_user_model().objects.create_user(username='catalog-all-skip-jpl', password='secret')
        request.session = {}

        class FakeTNSHarvester:
            name = 'TNS'

            def query(self, term):
                self.term = term

            def to_target(self):
                return Target(name='SN 2026abc', type='SIDEREAL', ra=12.3, dec=-45.6, epoch=2000.0)

        class FakeExoClockHarvester:
            name = 'ExoClock'

            def query(self, term):
                self.term = term

            def to_target(self):
                return Target(name='WASP-12b', type='SIDEREAL', ra=100.0, dec=20.0, epoch=2000.0)

        class FakeJPLHarvester:
            name = 'JPL Horizons'

            def query(self, term):
                self.term = term

            def to_target(self):
                return Target(name='Ceres', type='NON_SIDEREAL', ra=None, dec=None, epoch=2000.0)

        class FakeNEDHarvester:
            name = 'NED'

            def query(self, term):
                raise AssertionError('NED should not run in the selected All Services catalog subset')

            def to_target(self):
                raise AssertionError('NED should not run in the selected All Services catalog subset')

        with patch(
            'custom_code.views.get_service_classes',
            return_value={
                'TNS': FakeTNSHarvester,
                'ExoClock': FakeExoClockHarvester,
                'JPL Horizons': FakeJPLHarvester,
                'NED': FakeNEDHarvester,
            },
        ), patch('custom_code.views._get_catalog_matches', return_value=[]):
            response = BhtomCatalogQueryView.as_view()(request)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'SN 2026abc')
        self.assertContains(response, 'TNS')
        self.assertNotContains(response, 'JPL Horizons')
        self.assertNotContains(response, 'ExoClock')
        self.assertNotContains(response, 'NED')

    def test_catalog_all_services_keeps_exoclock_for_exoplanet_like_terms(self):
        request = RequestFactory().post(
            reverse('tom_catalogs:query'),
            data={'service': ALL_DATA_SERVICES_VALUE, 'term': 'WASP-12b'},
        )
        request.user = get_user_model().objects.create_user(username='catalog-all-exoclock', password='secret')
        request.session = {}

        class FakeExoClockHarvester:
            name = 'ExoClock'

            def query(self, term):
                self.term = term

            def to_target(self):
                return Target(name='WASP-12b', type='SIDEREAL', ra=100.0, dec=20.0, epoch=2000.0)

        with patch(
            'custom_code.views.get_service_classes',
            return_value={'ExoClock': FakeExoClockHarvester},
        ), patch('custom_code.views._get_catalog_matches', return_value=[]):
            response = BhtomCatalogQueryView.as_view()(request)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'WASP-12b')
        self.assertContains(response, 'ExoClock')

    def test_catalog_all_services_keeps_jpl_for_numbered_comet_terms(self):
        request = RequestFactory().post(
            reverse('tom_catalogs:query'),
            data={'service': ALL_DATA_SERVICES_VALUE, 'term': '80P'},
        )
        request.user = get_user_model().objects.create_user(username='catalog-all-jpl-comet', password='secret')
        request.session = {}

        class FakeJPLHarvester:
            name = 'JPL Horizons'

            def query(self, term):
                self.term = term

            def to_target(self):
                return Target(name='80P/Peters-Hartley', type='NON_SIDEREAL', ra=None, dec=None, epoch=2000.0)

        with patch(
            'custom_code.views.get_service_classes',
            return_value={'JPL Horizons': FakeJPLHarvester},
        ), patch('custom_code.views._get_catalog_matches', return_value=[]):
            response = BhtomCatalogQueryView.as_view()(request)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, '80P/Peters-Hartley')
        self.assertContains(response, 'JPL Horizons')

    def test_ogle_ews_harvester_maps_target_name_and_coordinates(self):
        harvester = OGLEEWSHarvester()
        harvester.catalog_data = {
            'name': '2011-BLG-001',
            'field': 'BLG',
            'ra': 270.123456,
            'dec': -28.654321,
        }

        target = harvester.to_target()

        self.assertEqual(target.name, 'OGLE-2011-BLG-001')
        self.assertEqual(target.type, 'SIDEREAL')
        self.assertEqual(target.epoch, 2000.0)
        self.assertAlmostEqual(target.ra, 270.123456)
        self.assertAlmostEqual(target.dec, -28.654321)

    def test_jpl_horizons_harvester_resolves_numbered_comet_ambiguity(self):
        table = Table({
            'targetname': ['80P/Peters-Hartley'],
            'M': [1.0],
            'w': [2.0],
            'Omega': [3.0],
            'incl': [4.0],
            'n': [5.0],
            'a': [6.0],
            'e': [0.1],
            'datetime_jd': [2460000.5],
            'Tp_jd': [2460100.5],
            'q': [7.0],
            'P': [8.0],
        })
        ambiguity = ValueError(
            'Ambiguous target name; provide unique id:\n'
            '    Record #  Epoch-yr  >MATCH DESIG<  Primary Desig  Name\n'
            '    --------  --------  -------------  -------------  -------------------------\n'
            '    90000852    1846    80P            80P             Peters-Hartley\n'
            '    90000855    2017    80P            80P             Peters-Hartley\n'
        )
        calls = []

        class FakeHorizons:
            def __init__(self, id, id_type=None, location=None, epochs=None):
                self.id = id
                self.id_type = id_type
                calls.append((id, id_type))

            def elements(self):
                if self.id == '90000855':
                    return table
                raise ambiguity

        harvester = JPLHorizonsHarvester()
        with patch('custom_code.bhtom_catalogs.harvesters.jplhorizons.Horizons', FakeHorizons):
            harvester.query('80P')

        self.assertIn(('80P', None), calls)
        self.assertIn(('90000855', None), calls)
        target = harvester.to_target()
        self.assertEqual(target.name, '80P/Peters-Hartley')
        self.assertEqual(target.type, 'NON_SIDEREAL')

    def test_moa_harvester_maps_target_name_and_coordinates(self):
        harvester = MOAHarvester()
        harvester.catalog_data = {
            'Event': 'MOA-2019-BLG-0397',
            'ra_deg': '272.724625',
            'dec_deg': '-25.7461111111',
        }

        target = harvester.to_target()

        self.assertEqual(target.name, 'MOA-2019-BLG-0397')
        self.assertEqual(target.type, 'SIDEREAL')
        self.assertEqual(target.epoch, 2000.0)
        self.assertAlmostEqual(target.ra, 272.724625)
        self.assertAlmostEqual(target.dec, -25.7461111111)

    def test_kmt_harvester_maps_target_name_and_coordinates(self):
        harvester = KMTHarvester()
        harvester.catalog_data = {
            'Event': 'KMT-2017-BLG-2573',
            'ra_deg': '266.54918',
            'dec_deg': '-25.62171',
        }

        target = harvester.to_target()

        self.assertEqual(target.name, 'KMT-2017-BLG-2573')
        self.assertEqual(target.type, 'SIDEREAL')
        self.assertEqual(target.epoch, 2000.0)
        self.assertAlmostEqual(target.ra, 266.54918)
        self.assertAlmostEqual(target.dec, -25.62171)


class TargetCreateFormVisibilityTests(TestCase):
    def test_sidereal_create_form_hides_derived_and_plot_fields(self):
        form = BhtomSiderealTargetCreateForm()

        self.assertIn('classification', form.fields)
        self.assertIn('parallax', form.fields)
        self.assertIn('parallax_error', form.fields)
        self.assertIn('pm_ra_error', form.fields)
        self.assertIn('pm_dec_error', form.fields)
        self.assertIn('gaia_variability_type', form.fields)
        self.assertIn('source_name', form.fields)
        self.assertIn('planet_name', form.fields)
        self.assertIn('t0_bjd_tdb', form.fields)
        self.assertIn('period_days', form.fields)
        self.assertNotIn('distance', form.fields)
        self.assertNotIn('distance_err', form.fields)
        self.assertNotIn('sun_separation', form.fields)
        self.assertNotIn('cadence_priority', form.fields)
        self.assertNotIn('priority', form.fields)
        self.assertNotIn('galactic_lng', form.fields)
        self.assertNotIn('galactic_lat', form.fields)
        self.assertNotIn('constellation', form.fields)
        self.assertNotIn('phot_class', form.fields)
        self.assertNotIn('phot_classification_done', form.fields)
        self.assertNotIn('mjd_last', form.fields)
        self.assertNotIn('mag_last', form.fields)
        self.assertNotIn('filter_last', form.fields)
        self.assertNotIn('photometry_plot', form.fields)
        self.assertNotIn('photometry_plot_obs', form.fields)
        self.assertNotIn('photometry_icon_plot', form.fields)
        self.assertNotIn('spectroscopy_plot', form.fields)
        self.assertNotIn('plot_created', form.fields)

    def test_sidereal_create_form_accepts_decimal_degrees(self):
        form = BhtomSiderealTargetCreateForm(data={
            'name': 'DecimalDegreeTarget',
            'type': Target.SIDEREAL,
            'ra': '21.4001011',
            'dec': '34.1517361',
            'permissions': Target.Permissions.PUBLIC,
            'importance': 0,
            'cadence': 0,
            'recommended_observing_strategy': 'Observe nightly.',
        })

        self.assertTrue(form.is_valid(), form.errors)
        self.assertAlmostEqual(form.cleaned_data['ra'], 21.4001011)
        self.assertAlmostEqual(form.cleaned_data['dec'], 34.1517361)

    def test_sidereal_create_form_accepts_sexagesimal_coordinates(self):
        form = BhtomSiderealTargetCreateForm(data={
            'name': 'SexagesimalTarget',
            'type': Target.SIDEREAL,
            'ra': '01:25:36.024',
            'dec': '+34:09:06.25',
            'permissions': Target.Permissions.PUBLIC,
            'importance': 0,
            'cadence': 0,
            'recommended_observing_strategy': 'Observe nightly.',
        })

        self.assertTrue(form.is_valid(), form.errors)
        self.assertAlmostEqual(form.cleaned_data['ra'], 21.4001, places=4)
        self.assertAlmostEqual(form.cleaned_data['dec'], 34.1517, places=4)

    def test_planetary_transit_create_form_includes_transit_ephemeris_fields(self):
        form = BhtomPlanetaryTransitTargetCreateForm()

        self.assertIn('source_name', form.fields)
        self.assertIn('source_url', form.fields)
        self.assertIn('planet_name', form.fields)
        self.assertIn('host_name', form.fields)
        self.assertIn('priority', form.fields)
        self.assertIn('t0_bjd_tdb', form.fields)
        self.assertIn('t0_unc', form.fields)
        self.assertIn('period_days', form.fields)
        self.assertIn('period_unc', form.fields)
        self.assertIn('duration_hours', form.fields)
        self.assertIn('depth_r_mmag', form.fields)
        self.assertIn('v_mag', form.fields)
        self.assertIn('r_mag', form.fields)
        self.assertIn('gaia_g_mag', form.fields)

    def test_sidereal_form_reports_hidden_private_coordinate_match(self):
        creator = get_user_model().objects.create_user(username='private-coordinate-checker', password='secret')
        Target.objects.create(
            name='HiddenTarget',
            type=Target.SIDEREAL,
            ra=12.3,
            dec=-45.6,
            epoch=2000.0,
            permissions=Target.Permissions.PRIVATE,
        )

        form = BhtomSiderealTargetCreateForm(data={
            'name': 'VisibleAttempt',
            'type': Target.SIDEREAL,
            'ra': 12.3,
            'dec': -45.6,
            'permissions': Target.Permissions.PUBLIC,
            'importance': 0,
            'cadence': 0,
            'recommended_observing_strategy': 'Observe nightly.',
        })
        form.user = creator

        self.assertFalse(form.is_valid())
        self.assertIn(
            'A target already exists at these coordinates, but it remains private.',
            form.non_field_errors(),
        )

    def test_sidereal_form_allows_private_coordinate_match_when_user_has_access(self):
        creator = get_user_model().objects.create_user(username='private-coordinate-member', password='secret')
        hidden_target = Target.objects.create(
            name='SharedHiddenTarget',
            type=Target.SIDEREAL,
            ra=12.3,
            dec=-45.6,
            epoch=2000.0,
            permissions=Target.Permissions.PRIVATE,
        )
        assign_perm('tom_targets.view_target', creator, hidden_target)

        form = BhtomSiderealTargetCreateForm(data={
            'name': 'VisibleAttempt',
            'type': Target.SIDEREAL,
            'ra': 12.3,
            'dec': -45.6,
            'permissions': Target.Permissions.PUBLIC,
            'importance': 0,
            'cadence': 0,
            'recommended_observing_strategy': 'Observe nightly.',
        })
        form.user = creator

        self.assertTrue(form.is_valid(), form.errors)

    def test_sidereal_update_form_includes_transit_ephemeris_fields(self):
        target = Target.objects.create(name='WASP-12b', type=Target.SIDEREAL, ra=1.0, dec=2.0, epoch=2000.0)
        target.parallax_error = 0.33
        target.gaia_variability_type = 'RR'
        target.save(update_fields=['parallax_error', 'gaia_variability_type'])
        form = BhtomSiderealTargetUpdateForm(instance=target)

        self.assertIn('classification', form.fields)
        self.assertIn('parallax', form.fields)
        self.assertIn('source_name', form.fields)
        self.assertIn('planet_name', form.fields)
        self.assertIn('priority', form.fields)
        self.assertEqual(float(form['parallax_error'].value()), 0.33)
        self.assertEqual(form['gaia_variability_type'].value(), 'RR')

    def test_target_alias_formset_rejects_duplicate_names_before_save(self):
        target = Target.objects.create(name='WASP-12b', type=Target.SIDEREAL, ra=1.0, dec=2.0, epoch=2000.0)
        prefix = BhtomTargetNamesFormset(instance=target).prefix
        formset = BhtomTargetNamesFormset(
            data={
                f'{prefix}-TOTAL_FORMS': '2',
                f'{prefix}-INITIAL_FORMS': '0',
                f'{prefix}-MIN_NUM_FORMS': '0',
                f'{prefix}-MAX_NUM_FORMS': '1000',
                f'{prefix}-0-name': 'WASP-12',
                f'{prefix}-1-name': 'WASP-12',
            },
            instance=target,
        )

        self.assertFalse(formset.is_valid())
        self.assertTrue(formset.non_form_errors())


class TargetDeleteCleanupTests(TestCase):
    def test_target_delete_cleans_related_rows_before_parent_delete(self):
        target = Mock(pk=123)
        target.aliases = Mock()
        target.aliases.all.return_value.delete = Mock()
        target.transit_ephemeris = Mock()

        queryset_mocks = []
        model_mocks = []
        for _ in range(5):
            queryset = Mock()
            queryset.exists.return_value = True
            queryset.db = 'default'
            queryset._raw_delete = Mock()
            model = Mock()
            model.objects.filter.return_value = queryset
            queryset_mocks.append(queryset)
            model_mocks.append(model)

        with patch('custom_code.signals.apps.get_model', side_effect=model_mocks) as mocked_get_model:
            cleanup_target_relations_on_target_delete(sender=Target, instance=target)

        self.assertEqual(mocked_get_model.call_count, 5)
        for queryset in queryset_mocks:
            queryset._raw_delete.assert_called_once_with('default')

        target.aliases.all.return_value.delete.assert_called_once()
        target.transit_ephemeris.delete.assert_called_once()


class TargetDetailDataTests(TestCase):
    def test_truncate_decimals_truncates_without_rounding(self):
        self.assertEqual(truncate_decimals(2.162599706281586, 4), '2.1625')
        self.assertEqual(truncate_decimals(-9.795577453601858, 4), '-9.7955')
        self.assertEqual(truncate_decimals(0.014048762619495392, 4), '0.0140')

    def test_target_data_includes_gaia_astrometry_rows_with_errors(self):
        target = Target.objects.create(
            name='GaiaDR3_123',
            type=Target.SIDEREAL,
            ra=12.3,
            dec=-45.6,
            epoch=2000.0,
            parallax=1.2,
            pm_ra=4.5,
            pm_dec=-6.7,
            gaia_variability_type='RR',
        )
        target.parallax_error = 0.33
        target.pm_ra_error = 0.11
        target.pm_dec_error = 0.22
        target.save(update_fields=['parallax_error', 'pm_ra_error', 'pm_dec_error'])

        context = bhtom_target_data({'request': Mock(), 'current_coords': None}, target)

        self.assertEqual(context['astrometry_rows'][0]['label'], 'Parallax (mas)')
        self.assertEqual(context['astrometry_rows'][0]['value'], 1.2)
        self.assertEqual(context['astrometry_rows'][0]['error'], 0.33)
        self.assertEqual(context['astrometry_rows'][1]['error'], 0.11)
        self.assertEqual(context['astrometry_rows'][2]['error'], 0.22)
        self.assertEqual(context['astrometry_rows'][3]['label'], 'Variability class')
        self.assertEqual(context['astrometry_rows'][3]['value'], 'RR')

    def test_target_data_omits_gaia_astrometry_block_when_all_values_missing(self):
        target = Target.objects.create(
            name='GaiaDR3_456',
            type=Target.SIDEREAL,
            ra=12.3,
            dec=-45.6,
            epoch=2000.0,
        )

        context = bhtom_target_data({'request': Mock(), 'current_coords': None}, target)

        self.assertEqual(context['astrometry_rows'], [])

    def test_target_data_omits_gaia_astrometry_block_when_values_are_empty_strings(self):
        target = Target.objects.create(
            name='GaiaDR3_789',
            type=Target.SIDEREAL,
            ra=12.3,
            dec=-45.6,
            epoch=2000.0,
            gaia_variability_type='',
        )

        context = bhtom_target_data({'request': Mock(), 'current_coords': None}, target)

        self.assertEqual(context['astrometry_rows'], [])

    def test_target_data_exposes_current_coordinate_button_only_for_significant_gaia_astrometry(self):
        eligible_target = Target.objects.create(
            name='GaiaDR3_eligible',
            type=Target.SIDEREAL,
            ra=12.3,
            dec=-45.6,
            epoch=2000.0,
            parallax=1.2,
            pm_ra=4.5,
            pm_dec=-6.7,
        )
        eligible_target.parallax_error = 0.33
        eligible_target.save(update_fields=['parallax_error'])

        ineligible_target = Target.objects.create(
            name='GaiaDR3_ineligible',
            type=Target.SIDEREAL,
            ra=12.3,
            dec=-45.6,
            epoch=2000.0,
            parallax=1.2,
            pm_ra=4.5,
            pm_dec=-6.7,
        )
        ineligible_target.parallax_error = 0.8
        ineligible_target.save(update_fields=['parallax_error'])

        eligible_context = bhtom_target_data({'request': Mock(), 'current_coords': None}, eligible_target)
        ineligible_context = bhtom_target_data({'request': Mock(), 'current_coords': None}, ineligible_target)

        self.assertTrue(eligible_context['show_current_coords_button'])
        self.assertFalse(ineligible_context['show_current_coords_button'])

    def test_compute_current_coordinates_propagates_target_to_requested_time(self):
        target = Target(
            name='GaiaDR3_motion',
            type=Target.SIDEREAL,
            ra=120.0,
            dec=22.0,
            epoch=2000.0,
            parallax=10.0,
            pm_ra=100.0,
            pm_dec=-50.0,
        )
        target.parallax_error = 2.0

        result = compute_current_coordinates(target, now=Time('2026-04-22T12:00:00', scale='utc'))

        self.assertNotEqual(round(result['ra_deg'], 6), 120.0)
        self.assertNotEqual(round(result['dec_deg'], 6), 22.0)
        self.assertEqual(result['computed_at_utc'].date().isoformat(), '2026-04-22')

    def test_target_detail_view_renders_current_coordinate_message(self):
        target = Target.objects.create(
            name='GaiaDR3_view',
            type=Target.SIDEREAL,
            ra=120.0,
            dec=22.0,
            epoch=2000.0,
            parallax=10.0,
            pm_ra=100.0,
            pm_dec=-50.0,
        )
        target.parallax_error = 2.0
        target.save(update_fields=['parallax_error'])

        response = self.client.get(reverse('targets:detail', kwargs={'pk': target.pk}), {'compute_current_coords': '1'})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Compute current Ra/Dec')
        self.assertContains(response, 'Current coordinates at')

    def test_non_sidereal_target_detail_uses_selected_observer_and_time_for_live_coordinates(self):
        target = Target.objects.create(
            name='MinorPlanetView',
            type=Target.NON_SIDEREAL,
            scheme='MPC_MINOR_PLANET',
            semimajor_axis=2.35,
            eccentricity=0.17,
            inclination=8.4,
            arg_of_perihelion=132.5,
            lng_asc_node=76.2,
            mean_anomaly=48.1,
            epoch_of_elements=61000.0,
            mean_daily_motion=0.274,
        )

        with patch('custom_code.templatetags.live_target_extras.get_live_target_values', return_value={
            'ra': 123.456,
            'dec': -12.345,
            'sun_separation': 101.0,
            'altitude_deg': 45.0,
            'computed_at_utc': datetime(2026, 4, 8, 12, 34, 56, tzinfo=timezone.utc),
        }) as live_mock:
            response = self.client.get(
                reverse('targets:detail', kwargs={'pk': target.pk}),
                {
                    'observer': 'ostrowik',
                    'time_utc': '2026-04-08T12:34:56',
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Generated (UT): 2026-04-08 12:34:56')
        self.assertContains(response, 'Observer:')
        self.assertContains(response, 'Ostrowik (52.087981, 21.41614, 120.0 m)')
        self.assertContains(response, 'name="time_utc"')
        self.assertContains(response, 'name="observer"')
        live_mock.assert_called_once()
        _, kwargs = live_mock.call_args
        self.assertEqual(kwargs['observer_lat_deg'], 52.087981)
        self.assertEqual(kwargs['observer_lon_deg'], 21.41614)
        self.assertEqual(kwargs['observer_elevation_m'], 120.0)
        self.assertEqual(kwargs['time_to_compute'].to_datetime(timezone=timezone.utc).isoformat(), '2026-04-08T12:34:56+00:00')


class GaiaCurrentCoordinateComputationTests(TestCase):
    def test_can_compute_current_coordinates_requires_good_parallax_signal_to_noise(self):
        target = Target(
            name='GaiaDR3_threshold',
            type=Target.SIDEREAL,
            ra=12.3,
            dec=-45.6,
            epoch=2000.0,
            parallax=1.2,
            pm_ra=4.5,
            pm_dec=-6.7,
        )
        target.parallax_error = 0.6
        self.assertFalse(can_compute_current_coordinates(target))

        target.parallax_error = 0.5
        self.assertTrue(can_compute_current_coordinates(target))


class NonSiderealVisibilityTests(TestCase):
    def test_non_sidereal_visibility_form_accepts_datetime_range_and_airmass(self):
        form = NonSiderealTargetVisibilityForm(data={
            'start_time': '2026-04-08T00:00:00',
            'end_time': '2026-04-09T00:00:00',
            'airmass': '2.5',
        })

        self.assertTrue(form.is_valid(), form.errors)

    def test_non_sidereal_visibility_uses_registered_sites(self):
        target = Target(
            name='MinorPlanetVisibility',
            type=Target.NON_SIDEREAL,
        )

        class FakeFacility:
            def get_observing_sites(self):
                return {
                    'Warsaw': {
                        'latitude': 52.2297,
                        'longitude': 21.0122,
                        'elevation': 100.0,
                    }
                }

        with patch('custom_code.non_sidereal_visibility.facility.get_service_classes', return_value={'FakeFacility': FakeFacility}), \
             patch('custom_code.non_sidereal_visibility.facility.get_service_class', return_value=FakeFacility), \
             patch('custom_code.non_sidereal_visibility._resolve_target_coordinates_now', return_value=(120.0, 22.0)) as resolve_mock, \
             patch('custom_code.non_sidereal_visibility.get_sun', return_value=SkyCoord(ra=0 * u.deg, dec=-90 * u.deg, frame='icrs')):
            visibility = get_non_sidereal_visibility(
                target,
                datetime(2026, 4, 8, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 8, 1, 0, tzinfo=timezone.utc),
                30,
                5.0,
            )

        self.assertIn('(FakeFacility) Warsaw', visibility)
        times, airmasses = visibility['(FakeFacility) Warsaw']
        self.assertEqual(len(times), len(airmasses))
        self.assertTrue(any(value is not None for value in airmasses))
        _, kwargs = resolve_mock.call_args
        self.assertEqual(kwargs['observer_lat_deg'], 52.2297)
        self.assertEqual(kwargs['observer_lon_deg'], 21.0122)
        self.assertEqual(kwargs['observer_elevation_m'], 100.0)

    def test_nonsidereal_target_plan_renders_plot_with_visibility_data(self):
        target = Target.objects.create(
            name='MinorPlanetPlan',
            type=Target.NON_SIDEREAL,
        )
        request = RequestFactory().get(
            reverse('targets:detail', kwargs={'pk': target.pk}),
            {'tab': 'observe'},
        )

        with patch('custom_code.templatetags.custom_observation_extras.get_non_sidereal_visibility', return_value={
            '(FakeFacility) Warsaw': (
                [
                    datetime(2026, 4, 8, 0, 0, tzinfo=timezone.utc),
                    datetime(2026, 4, 8, 1, 0, tzinfo=timezone.utc),
                ],
                [1.5, 2.0],
            )
        }):
            context = nonsidereal_target_plan({'request': request, 'object': target})

        self.assertEqual(context['target'], target)
        self.assertIn('plotly', context['visibility_graph'])
        self.assertIn('(FakeFacility) Warsaw', context['visibility_graph'])
        self.assertEqual(context['form']['airmass'].value(), '2.5')

    def test_target_plan_plot_caps_airmass_at_three(self):
        visibility_data = {
            '(LCO) Siding Spring': (
                [
                    datetime(2026, 6, 16, 22, 0, tzinfo=timezone.utc),
                    datetime(2026, 6, 16, 23, 0, tzinfo=timezone.utc),
                    datetime(2026, 6, 17, 0, 0, tzinfo=timezone.utc),
                ],
                [1.4, 3.0, 4.2],
            )
        }

        plot_data = _target_plan_plot_data(visibility_data)
        layout = _target_plan_layout(600, 400, None)

        self.assertEqual(list(plot_data[0].y), [1.4, 3.0, None])
        self.assertEqual(tuple(layout.yaxis.range), (3.0, 1))

    def test_non_sidereal_aladin_requires_specific_observer(self):
        target = Target(name='MinorPlanetChart', type=Target.NON_SIDEREAL)

        hidden = non_sidereal_aladin({
            'detail_generated_utc': datetime(2026, 4, 8, 0, 0, tzinfo=timezone.utc),
            'detail_observer': {'key': 'unspecified'},
        }, target)
        self.assertFalse(hidden['render_chart'])

        with patch('custom_code.templatetags.custom_target_extras.get_live_target_values', return_value={
            'ra': 123.4,
            'dec': -12.3,
        }):
            shown = non_sidereal_aladin({
                'detail_generated_utc': datetime(2026, 4, 8, 0, 0, tzinfo=timezone.utc),
                'detail_observer': {
                    'key': 'ostrowik',
                    'lat_deg': 52.087981,
                    'lon_deg': 21.41614,
                    'elevation_m': 120.0,
                },
            }, target)

        self.assertTrue(shown['render_chart'])
        self.assertEqual(shown['chart_ra'], 123.4)
        self.assertEqual(shown['chart_dec'], -12.3)


class PlanetaryTransitTargetCreateTests(TestCase):
    def test_create_view_uses_planetary_transit_form_for_classification(self):
        request = RequestFactory().get(reverse('targets:create'), {'classification': 'Planetary Transit', 'type': 'SIDEREAL'})
        view = BhtomTargetCreateView()
        view.request = request
        view.initial = {}
        view.get_target_type = Mock(return_value=Target.SIDEREAL)

        self.assertIs(view.get_form_class(), BhtomPlanetaryTransitTargetCreateForm)

    def test_form_valid_persists_transit_ephemeris(self):
        request = RequestFactory().post(reverse('targets:create'))
        user = get_user_model().objects.create_user(username='transit-user', password='secret')
        request.user = user

        target = Target.objects.create(
            name='WASP-12b',
            type=Target.SIDEREAL,
            ra=97.6366525,
            dec=29.672296111111113,
            epoch=2000.0,
            classification='Planetary Transit',
        )

        class DummyForm:
            instance = target
            cleaned_data = {
                'source_name': 'ExoClock',
                'source_url': 'https://www.exoclock.space/database/planets/WASP-12b',
                'planet_name': 'WASP-12b',
                'host_name': 'WASP-12',
                'priority': 'A',
                't0_bjd_tdb': 2457368.4973,
                't0_unc': 0.0041,
                'period_days': 1.091418859,
                'period_unc': 0.0000012,
                'duration_hours': 2.93,
                'depth_r_mmag': 17.81,
                'v_mag': 11.3,
                'r_mag': 11.1,
                'gaia_g_mag': 11.25,
                'recommended_observing_strategy': 'Observe around the predicted transit window.',
            }

            def save(self):
                return target

            def get_transit_ephemeris_defaults(self):
                return {
                    'source_name': self.cleaned_data['source_name'],
                    'source_url': self.cleaned_data['source_url'],
                    'planet_name': self.cleaned_data['planet_name'],
                    'host_name': self.cleaned_data['host_name'],
                    'priority': self.cleaned_data['priority'],
                    't0_bjd_tdb': self.cleaned_data['t0_bjd_tdb'],
                    't0_unc': self.cleaned_data['t0_unc'],
                    'period_days': self.cleaned_data['period_days'],
                    'period_unc': self.cleaned_data['period_unc'],
                    'duration_hours': self.cleaned_data['duration_hours'],
                    'depth_r_mmag': self.cleaned_data['depth_r_mmag'],
                    'v_mag': self.cleaned_data['v_mag'],
                    'r_mag': self.cleaned_data['r_mag'],
                    'gaia_g_mag': self.cleaned_data['gaia_g_mag'],
                }

        extra_formset = Mock()
        extra_formset.is_valid.return_value = True
        extra_formset.save.return_value = None
        names_formset = Mock()
        names_formset.is_valid.return_value = True
        names_formset.save.return_value = None

        view = BhtomTargetCreateView()
        view.request = request

        with patch('custom_code.views.TargetExtraFormset', return_value=extra_formset), \
             patch('custom_code.views.BhtomTargetNamesFormset', return_value=names_formset), \
             patch('custom_code.views.Comment.objects.create'), \
             patch('custom_code.views.get_current_site', return_value=Mock()), \
             patch('custom_code.views.run_hook'), \
             patch.object(BhtomTargetCreateView, 'get_success_url', return_value='/targets/1/'):
            response = view.form_valid(DummyForm())

        self.assertEqual(response.status_code, 302)
        ephemeris = TransitEphemeris.objects.get(target=target)
        self.assertEqual(ephemeris.source_name, 'ExoClock')
        self.assertEqual(ephemeris.planet_name, 'WASP-12b')
        self.assertEqual(ephemeris.host_name, 'WASP-12')
        self.assertAlmostEqual(ephemeris.period_days, 1.091418859)

    def test_form_valid_persists_gaia_astrometry_error_fields(self):
        request = RequestFactory().post(reverse('targets:create'))
        user = get_user_model().objects.create_user(username='gaia-astrometry-create', password='secret')
        request.user = user

        target = Target.objects.create(
            name='GaiaDR3_123',
            type=Target.SIDEREAL,
            ra=12.3,
            dec=-45.6,
            epoch=2000.0,
            parallax=1.2,
        )

        class DummyForm:
            instance = target
            cleaned_data = {
                'classification': '',
                'recommended_observing_strategy': 'Observe with cadence.',
                'parallax_error': 0.33,
                'pm_ra_error': 0.11,
                'pm_dec_error': 0.22,
            }

            def save(self):
                target.parallax_error = self.cleaned_data['parallax_error']
                target.pm_ra_error = self.cleaned_data['pm_ra_error']
                target.pm_dec_error = self.cleaned_data['pm_dec_error']
                target.save(update_fields=['parallax_error', 'pm_ra_error', 'pm_dec_error'])
                return target

        extra_formset = Mock()
        extra_formset.is_valid.return_value = True
        extra_formset.save.return_value = None
        names_formset = Mock()
        names_formset.is_valid.return_value = True
        names_formset.save.return_value = None

        view = BhtomTargetCreateView()
        view.request = request

        with patch('custom_code.views.TargetExtraFormset', return_value=extra_formset), \
             patch('custom_code.views.BhtomTargetNamesFormset', return_value=names_formset), \
             patch('custom_code.views.Comment.objects.create'), \
             patch('custom_code.views.get_current_site', return_value=Mock()), \
             patch('custom_code.views.run_hook'), \
             patch.object(BhtomTargetCreateView, 'get_success_url', return_value='/targets/1/'):
            response = view.form_valid(DummyForm())

        self.assertEqual(response.status_code, 302)
        target.refresh_from_db()
        self.assertEqual(target.parallax_error, 0.33)
        self.assertEqual(target.pm_ra_error, 0.11)
        self.assertEqual(target.pm_dec_error, 0.22)

    def test_create_view_prefills_transit_fields_from_exoclock_payload(self):
        request = RequestFactory().get(
            reverse('targets:create'),
            {
                'type': 'SIDEREAL',
                'classification': 'Planetary Transit',
                'source_name': 'ExoClock',
                'source_url': 'https://www.exoclock.space/database/planets/WASP-12b',
                'planet_name': 'WASP-12b',
                'host_name': 'WASP-12',
                't0_bjd_tdb': '2457368.4973',
                't0_unc': '0.0041',
                'period_days': '1.091418859',
                'period_unc': '0.0000012',
                'duration_hours': '2.93',
                'depth_r_mmag': '17.81',
                'v_mag': '11.3',
                'r_mag': '11.1',
                'gaia_g_mag': '11.25',
            },
        )
        user = get_user_model().objects.create_user(username='transit-prefill', password='secret')
        request.user = user

        view = BhtomTargetCreateView()
        view.request = request
        view.object = None
        view.initial = {}
        view.get_target_type = Mock(return_value=Target.SIDEREAL)

        form = view.get_form()

        self.assertEqual(form.fields['source_name'].initial, 'ExoClock')
        self.assertEqual(form.fields['source_url'].initial, 'https://www.exoclock.space/database/planets/WASP-12b')
        self.assertEqual(form.fields['planet_name'].initial, 'WASP-12b')
        self.assertEqual(form.fields['host_name'].initial, 'WASP-12')
        self.assertEqual(form.fields['t0_bjd_tdb'].initial, 2457368.4973)
        self.assertEqual(form.fields['period_days'].initial, 1.091418859)
        self.assertEqual(form.fields['duration_hours'].initial, 2.93)
        self.assertEqual(form.fields['depth_r_mmag'].initial, 17.81)

    def test_create_view_prefills_gaia_astrometry_fields_from_query_string(self):
        request = RequestFactory().get(
            reverse('targets:create'),
            {
                'type': 'SIDEREAL',
                'parallax': '1.2',
                'parallax_error': '0.33',
                'pm_ra': '4.5',
                'pm_ra_error': '0.11',
                'pm_dec': '-6.7',
                'pm_dec_error': '0.22',
                'gaia_variability_type': 'RR',
            },
        )
        user = get_user_model().objects.create_user(username='gaia-prefill', password='secret')
        request.user = user

        view = BhtomTargetCreateView()
        view.request = request
        view.object = None
        view.initial = {}
        view.get_target_type = Mock(return_value=Target.SIDEREAL)

        form = view.get_form()

        self.assertEqual(form['parallax'].value(), '1.2')
        self.assertEqual(form['pm_ra'].value(), '4.5')
        self.assertEqual(form['pm_dec'].value(), '-6.7')
        self.assertEqual(form['parallax_error'].value(), '0.33')
        self.assertEqual(form['pm_ra_error'].value(), '0.11')
        self.assertEqual(form['pm_dec_error'].value(), '0.22')
        self.assertEqual(form['gaia_variability_type'].value(), 'RR')

    def test_create_view_prefills_recommended_strategy_from_query_string(self):
        request = RequestFactory().get(
            reverse('targets:create'),
            {
                'type': 'SIDEREAL',
                'classification': 'Planetary Transit',
                'recommended_observing_strategy': EXOCLOCK_RECOMMENDED_OBSERVING_STRATEGY,
            },
        )
        user = get_user_model().objects.create_user(username='transit-strategy-prefill', password='secret')
        request.user = user

        view = BhtomTargetCreateView()
        view.request = request
        view.object = None
        view.initial = {}
        view.get_target_type = Mock(return_value=Target.SIDEREAL)

        form = view.get_form()

        self.assertEqual(
            form.fields['recommended_observing_strategy'].initial,
            EXOCLOCK_RECOMMENDED_OBSERVING_STRATEGY,
        )

    def test_create_target_from_query_redirects_with_transit_query_params(self):
        cache_key = 'result_0'
        cache_payload = {
            'name': 'WASP-12b',
            'ra': 97.6366525,
            'dec': 29.672296111111113,
            'epoch': 2000.0,
            'transit_source_name': 'ExoClock',
            'transit_source_url': 'https://www.exoclock.space/database/planets/WASP-12b',
            'transit_planet_name': 'WASP-12b',
            'transit_host_name': 'WASP-12',
            'transit_t0_bjd_tdb': 2457368.4973,
            'transit_t0_unc': 0.0041,
            'transit_period_days': 1.091418859,
            'transit_period_unc': 0.0000012,
            'transit_duration_hours': 2.93,
            'transit_depth_r_mmag': 17.81,
            'transit_v_mag': 11.3,
            'transit_r_mag': 11.1,
            'transit_gaia_g_mag': 11.25,
        }
        cache.set(cache_key, cache_payload, 3600)

        request = RequestFactory().post(
            reverse('dataservices:create-target'),
            data={
                'query_id': '17',
                'data_service': 'ExoClock',
                'selected_results': ['0'],
            },
        )
        user = get_user_model().objects.create_user(username='transit-create-query', password='secret')
        request.user = user

        class StubService:
            def to_target(self, cached_result):
                self.cached_result = cached_result
                return Target.objects.create(
                    name='WASP-12b',
                    type=Target.SIDEREAL,
                    ra=97.6366525,
                    dec=29.672296111111113,
                    epoch=2000.0,
                ), None, None

        with patch('custom_code.views.get_data_service_class', return_value=StubService):
            response = BhtomCreateTargetFromQueryView.as_view()(request)

        self.assertEqual(response.status_code, 302)
        location = response['Location']
        self.assertIn('classification=Planetary+Transit', location)
        self.assertIn('source_name=ExoClock', location)
        self.assertIn('planet_name=WASP-12b', location)
        self.assertIn('t0_bjd_tdb=2457368.4973', location)
        self.assertIn('period_days=1.091418859', location)

    def test_create_target_from_query_redirects_with_gaia_astrometry_params(self):
        cache_key = 'result_1'
        cache_payload = {
            'name': 'GaiaDR3_123',
            'ra': 12.3,
            'dec': -45.6,
            'parallax': 1.2,
            'pmra': 4.5,
            'pmdec': -6.7,
            'parallax_error': 0.33,
            'pm_ra_error': 0.11,
            'pm_dec_error': 0.22,
            'gaia_variability_type': 'RR',
        }
        cache.set(cache_key, cache_payload, 3600)

        request = RequestFactory().post(
            reverse('dataservices:create-target'),
            data={
                'query_id': '17',
                'data_service': 'GaiaDR3',
                'selected_results': ['1'],
            },
        )
        user = get_user_model().objects.create_user(username='gaia-create-query', password='secret')
        request.user = user

        class StubService:
            def to_target(self, cached_result):
                self.cached_result = cached_result
                return Target.objects.create(
                    name='GaiaDR3_123',
                    type=Target.SIDEREAL,
                    ra=12.3,
                    dec=-45.6,
                    epoch=2000.0,
                    parallax=1.2,
                    gaia_variability_type='RR',
                ), None, None

        with patch('custom_code.views.get_data_service_class', return_value=StubService):
            response = BhtomCreateTargetFromQueryView.as_view()(request)

        self.assertEqual(response.status_code, 302)
        location = response['Location']
        self.assertIn('parallax=1.2', location)
        self.assertIn('pm_ra=4.5', location)
        self.assertIn('pm_dec=-6.7', location)
        self.assertIn('parallax_error=0.33', location)
        self.assertIn('pm_ra_error=0.11', location)
        self.assertIn('pm_dec_error=0.22', location)
        self.assertIn('gaia_variability_type=RR', location)

    def test_create_context_hides_empty_groups_field(self):
        request = RequestFactory().get(reverse('targets:create'), {'type': 'SIDEREAL'})
        user = get_user_model().objects.create_user(username='target-create-layout', password='secret')
        request.user = user

        view = BhtomTargetCreateView()
        view.request = request
        view.object = None
        view.initial = {}
        view.get_target_type = Mock(return_value=Target.SIDEREAL)

        form = view.get_form()
        context = view.get_context_data(form=form)

        self.assertFalse(context['show_groups_field'])
        self.assertIsNotNone(context['permissions_field'])
        self.assertEqual(form.fields['permissions'].initial, 'PUBLIC')

    def test_form_valid_with_invalid_inline_formsets_renders_without_querying_broken_transaction(self):
        request = RequestFactory().post(
            reverse('targets:create'),
            data={
                'type': 'SIDEREAL',
                'name': 'Gaia24abc',
                'ra': 12.3,
                'dec': -45.6,
                'epoch': 2000.0,
                'classification': '',
                'recommended_observing_strategy': 'Observe with cadence.',
                'permissions': 'PUBLIC',
                'importance': '1.0',
                'cadence': '1.0',
            },
        )
        user = get_user_model().objects.create_user(username='target-create-invalid-inline', password='secret')
        request.user = user

        view = BhtomTargetCreateView()
        view.request = request
        view.args = ()
        view.kwargs = {}
        view.object = None
        view.initial = {}
        view.get_target_type = Mock(return_value=Target.SIDEREAL)

        form = view.get_form()
        self.assertTrue(form.is_valid(), form.errors)

        extra_formset = Mock()
        extra_formset.is_valid.return_value = False
        extra_formset.errors = [{'key': ['Duplicate tag']}]
        extra_formset.non_form_errors.return_value = ['Extra formset invalid']

        names_formset = Mock()
        names_formset.is_valid.return_value = False
        names_formset.errors = [{'name': ['Duplicate alias']}]
        names_formset.non_form_errors.return_value = ['Names formset invalid']

        with patch('custom_code.views.TargetExtraFormset', return_value=extra_formset), \
             patch('custom_code.views.BhtomTargetNamesFormset', return_value=names_formset):
            response = view.form_valid(form)

        self.assertEqual(response.status_code, 200)
        self.assertNotIn('object', response.context_data)
        self.assertFalse(response.context_data['show_groups_field'])
        self.assertIn('form', response.context_data)

    def test_transit_form_normalizes_blank_text_fields_to_empty_strings(self):
        form = BhtomPlanetaryTransitTargetCreateForm(
            data={
                'type': 'SIDEREAL',
                'name': 'WASP-12b',
                'ra': 97.6366525,
                'dec': 29.672296111111113,
                'epoch': 2000.0,
                'classification': 'Planetary Transit',
                'recommended_observing_strategy': 'Observe around the predicted transit window.',
                'permissions': 'PUBLIC',
                'importance': '1.0',
                'cadence': '1.0',
                'source_name': '',
                'source_url': '',
                'planet_name': '',
                'host_name': '',
                'priority': '',
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        defaults = form.get_transit_ephemeris_defaults()
        self.assertEqual(defaults['source_name'], '')
        self.assertEqual(defaults['source_url'], '')
        self.assertEqual(defaults['planet_name'], '')
        self.assertEqual(defaults['host_name'], '')
        self.assertEqual(defaults['priority'], '')


class PlanetaryTransitTargetUpdateTests(TestCase):
    def test_update_form_includes_and_prefills_transit_fields(self):
        target = Target.objects.create(
            name='WASP-12b',
            type=Target.SIDEREAL,
            ra=97.6366525,
            dec=29.672296111111113,
            epoch=2000.0,
            classification='Planetary Transit',
        )
        TransitEphemeris.objects.create(
            target=target,
            source_name='ExoClock',
            source_url='https://www.exoclock.space/database/planets/WASP-12b',
            planet_name='WASP-12b',
            host_name='WASP-12',
            priority='A',
            period_days=1.091418859,
        )

        form = BhtomPlanetaryTransitTargetUpdateForm(instance=target)

        self.assertIn('source_name', form.fields)
        self.assertIn('planet_name', form.fields)
        self.assertEqual(form.fields['source_name'].initial, 'ExoClock')
        self.assertEqual(form.fields['planet_name'].initial, 'WASP-12b')
        self.assertEqual(form.fields['host_name'].initial, 'WASP-12')

    def test_update_view_persists_transit_ephemeris_changes(self):
        request = RequestFactory().post(reverse('targets:update', kwargs={'pk': 1}))
        user = get_user_model().objects.create_user(username='transit-user-update', password='secret')
        request.user = user

        target = Target.objects.create(
            name='WASP-12b',
            type=Target.SIDEREAL,
            ra=97.6366525,
            dec=29.672296111111113,
            epoch=2000.0,
            classification='Planetary Transit',
        )
        TransitEphemeris.objects.create(
            target=target,
            source_name='ExoClock',
            source_url='https://www.exoclock.space/database/planets/WASP-12b',
            planet_name='WASP-12b',
            host_name='WASP-12',
            priority='A',
            period_days=1.091418859,
        )

        class DummyForm:
            cleaned_data = {
                'source_name': 'ExoClock',
                'source_url': 'https://www.exoclock.space/database/planets/WASP-12b',
                'planet_name': 'WASP-12b',
                'host_name': 'WASP-12',
                'priority': 'B',
                't0_bjd_tdb': 2457368.4973,
                't0_unc': 0.0041,
                'period_days': 1.091418860,
                'period_unc': 0.0000013,
                'duration_hours': 3.1,
                'depth_r_mmag': 18.2,
                'v_mag': 11.2,
                'r_mag': 11.0,
                'gaia_g_mag': 11.1,
            }

            def save(self):
                return target

        extra_formset = Mock()
        extra_formset.is_valid.return_value = True
        extra_formset.save.return_value = None
        names_formset = Mock()
        names_formset.is_valid.return_value = True
        names_formset.save.return_value = None

        view = BhtomTargetUpdateView()
        view.request = request
        view.object = target

        with patch('custom_code.views.TargetExtraFormset', return_value=extra_formset), \
             patch('custom_code.views.BhtomTargetNamesFormset', return_value=names_formset), \
             patch('custom_code.views.run_hook'), \
             patch.object(BhtomTargetUpdateView, 'get_success_url', return_value='/targets/1/'):
            response = view.form_valid(DummyForm())

        self.assertEqual(response.status_code, 302)
        ephemeris = TransitEphemeris.objects.get(target=target)
        self.assertEqual(ephemeris.priority, 'B')
        self.assertAlmostEqual(ephemeris.current_oc_min, -4.2)
        self.assertAlmostEqual(ephemeris.period_days, 1.09141886)

    def test_non_sidereal_create_form_hides_internal_and_plot_fields(self):
        form = BhtomNonSiderealTargetCreateForm()

        self.assertNotIn('constellation', form.fields)
        self.assertNotIn('phot_class', form.fields)
        self.assertNotIn('phot_classification_done', form.fields)
        self.assertNotIn('mjd_last', form.fields)
        self.assertNotIn('mag_last', form.fields)
        self.assertNotIn('filter_last', form.fields)
        self.assertNotIn('photometry_plot', form.fields)
        self.assertNotIn('photometry_plot_obs', form.fields)
        self.assertNotIn('photometry_icon_plot', form.fields)
        self.assertNotIn('spectroscopy_plot', form.fields)
        self.assertNotIn('plot_created', form.fields)
        self.assertNotIn('galactic_lng', form.fields)
        self.assertNotIn('galactic_lat', form.fields)


class TargetDerivedFieldsTests(TestCase):
    def test_sidereal_derivations_fill_galactic_coordinates_and_constellation(self):
        target = Target(name='Gaia26abc', type=Target.SIDEREAL, ra=267.4127916666667, dec=-30.452333333333332, epoch=2000.0)

        updates = derive_sidereal_target_fields(target)

        self.assertAlmostEqual(updates['galactic_lng'], 359.155193, places=6)
        self.assertAlmostEqual(updates['galactic_lat'], -1.533473, places=6)
        self.assertEqual(updates['constellation'], 'Scorpius')

    def test_non_sidereal_derivations_do_not_produce_sidereal_fields(self):
        target = Target(name='CometX', type=Target.NON_SIDEREAL)

        self.assertEqual(derive_sidereal_target_fields(target), {})

    def test_non_sidereal_live_values_depend_on_observer_location(self):
        target = Target(
            name='MinorPlanetX',
            type=Target.NON_SIDEREAL,
            scheme='MPC_MINOR_PLANET',
            semimajor_axis=2.35,
            eccentricity=0.17,
            inclination=8.4,
            arg_of_perihelion=132.5,
            lng_asc_node=76.2,
            mean_anomaly=48.1,
            epoch_of_elements=61000.0,
            mean_daily_motion=0.274,
        )

        calculation_time = Time('2026-04-08T00:00:00', scale='utc')
        warsaw = get_live_target_values(
            target,
            time_to_compute=calculation_time,
            observer_lat_deg=52.2297,
            observer_lon_deg=21.0122,
            observer_elevation_m=100.0,
        )
        lasilla = get_live_target_values(
            target,
            time_to_compute=calculation_time,
            observer_lat_deg=-29.2567,
            observer_lon_deg=-70.7346,
            observer_elevation_m=2400.0,
        )

        self.assertIsNotNone(warsaw['ra'])
        self.assertIsNotNone(warsaw['dec'])
        self.assertIsNotNone(lasilla['ra'])
        self.assertIsNotNone(lasilla['dec'])
        self.assertNotEqual(round(warsaw['ra'], 6), round(lasilla['ra'], 6))
        self.assertNotEqual(round(warsaw['dec'], 6), round(lasilla['dec'], 6))

    def test_non_sidereal_live_values_accept_python_datetime(self):
        target = Target(
            name='MinorPlanetY',
            type=Target.NON_SIDEREAL,
            scheme='MPC_MINOR_PLANET',
            semimajor_axis=2.35,
            eccentricity=0.17,
            inclination=8.4,
            arg_of_perihelion=132.5,
            lng_asc_node=76.2,
            mean_anomaly=48.1,
            epoch_of_elements=61000.0,
            mean_daily_motion=0.274,
        )

        live = get_live_target_values(
            target,
            time_to_compute=Time('2026-04-08T00:00:00', scale='utc').to_datetime(timezone=timezone.utc),
            observer_lat_deg=52.2297,
            observer_lon_deg=21.0122,
            observer_elevation_m=100.0,
        )

        self.assertIsNotNone(live['ra'])
        self.assertIsNotNone(live['dec'])

    def test_non_sidereal_live_values_ignore_blank_observer_coordinates(self):
        target = Target(
            name='MinorPlanetZ',
            type=Target.NON_SIDEREAL,
            scheme='MPC_MINOR_PLANET',
            semimajor_axis=2.35,
            eccentricity=0.17,
            inclination=8.4,
            arg_of_perihelion=132.5,
            lng_asc_node=76.2,
            mean_anomaly=48.1,
            epoch_of_elements=61000.0,
            mean_daily_motion=0.274,
        )

        live = get_live_target_values(
            target,
            time_to_compute=Time('2026-04-08T00:00:00', scale='utc'),
            observer_lat_deg='',
            observer_lon_deg='',
            observer_elevation_m='',
        )

        self.assertIsNotNone(live['ra'])
        self.assertIsNotNone(live['dec'])

    def test_sidereal_live_values_compute_altitude_for_selected_observer_and_time(self):
        target = Target(
            name='SiderealTarget',
            type=Target.SIDEREAL,
            ra=120.0,
            dec=22.0,
            sun_separation=95.0,
        )

        live = get_live_target_values(
            target,
            time_to_compute=Time('2026-04-08T00:00:00', scale='utc'),
            observer_lat_deg=52.2297,
            observer_lon_deg=21.0122,
            observer_elevation_m=100.0,
        )

        self.assertEqual(live['ra'], 120.0)
        self.assertEqual(live['dec'], 22.0)
        self.assertIsNotNone(live['altitude_deg'])


class TargetListViewTests(TestCase):
    def test_generic_target_search_redirects_name_queries_to_target_list_filter(self):
        response = self.client.get(reverse('targets-generic-search'), {'q': 'Gaia21abc'})

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, f"{reverse('targets:list')}?name=Gaia21abc")

    def test_generic_target_search_redirects_coordinate_queries_to_cone_search(self):
        response = self.client.get(reverse('targets-generic-search'), {'q': '12.34,-45.67'})

        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            response.url,
            f"{reverse('targets:list')}?cone_search=12.34000000%2C-45.67000000%2C0.0008333333",
        )

    def test_main_target_list_displays_geotom_style_time_and_observer_controls(self):
        user = get_user_model().objects.create_user(username='tester', password='pass')
        self.client.force_login(user)

        response = self.client.get('/targets/')

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Generated (UT):')
        self.assertContains(response, 'Observer:')
        self.assertContains(response, 'Not Specified')
        self.assertContains(response, 'Visible Now')
        self.assertContains(response, 'name="min_alt"')

    def test_authenticated_empty_cone_search_shows_empty_filter_message(self):
        user = get_user_model().objects.create_user(username='cone-tester', password='pass')
        self.client.force_login(user)

        response = self.client.get('/targets/', {'cone_search': '12.34,-45.67,0.1'})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'No targets match those filters.')
        self.assertNotContains(response, 'login</a> to view targets')

    def test_visible_now_does_not_activate_for_unspecified_observer(self):
        user = get_user_model().objects.create_user(username='tester2', password='pass')
        self.client.force_login(user)

        response = self.client.get('/targets/', {'observer': 'unspecified', 'visible_only': '1'})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Not Specified')
        self.assertContains(response, 'Visible Now')
        self.assertNotContains(response, 'btn btn-info">Visible Now')

    def test_target_list_remembers_observer_from_previous_visit(self):
        user = get_user_model().objects.create_user(username='tester3', password='pass')
        self.client.force_login(user)

        first_response = self.client.get('/targets/', {'observer': 'ostrowik'})
        second_response = self.client.get('/targets/')

        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 200)
        self.assertContains(second_response, 'Ostrowik (52.087981, 21.41614, 120.0 m)')

    def test_target_detail_uses_time_saved_from_target_list(self):
        user = get_user_model().objects.create_user(username='tester4', password='pass')
        self.client.force_login(user)
        target = Target.objects.create(name='MinorPlanetSharedTime', type=Target.NON_SIDEREAL)

        self.client.get('/targets/', {'time_utc': '2026-04-21T12:34:56'})
        response = self.client.get(reverse('targets:detail', kwargs={'pk': target.pk}))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, '2026-04-21 12:34:56')

    def test_target_list_sorting_applies_to_filtered_queryset(self):
        user = get_user_model().objects.create_user(username='tester5', password='pass')
        self.client.force_login(user)
        targets = [
            Target.objects.create(name='SortedAlpha', type=Target.SIDEREAL, priority=1),
            Target.objects.create(name='SortedBeta', type=Target.SIDEREAL, priority=2),
            Target.objects.create(name='SortedGamma', type=Target.SIDEREAL, priority=3),
            Target.objects.create(name='UnmatchedTarget', type=Target.SIDEREAL, priority=99),
        ]
        for target in targets:
            assign_perm('tom_targets.view_target', user, target)

        response = self.client.get('/targets/', {'name': 'Sorted', 'sort': 'name', 'direction': 'desc'})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [target.name for target in response.context['object_list']],
            ['SortedGamma', 'SortedBeta', 'SortedAlpha'],
        )


class GeoTomViewTests(TestCase):
    def setUp(self):
        self.target = GeoTarget.objects.create(
            norad_id=12345,
            name='TEST-SAT',
            tle_name='TEST-SAT',
            tle_line1='1 12345U 98067A   26112.50000000  .00000000  00000-0  00000-0 0  9991',
            tle_line2='2 12345   0.0164  90.0000 0001000   0.0000 180.0000  1.00270000    05',
        )

    def test_geotom_list_defaults_to_live_mode(self):
        response = self.client.get(reverse('geotom-list'))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'id="geotom-generated-time" class="geotom-generated-time" data-live-mode="1"')
        self.assertContains(response, 'id="geotom-filter-time-utc" type="hidden" name="time_utc" value=""')

    def test_geotom_list_uses_fixed_mode_for_custom_time(self):
        response = self.client.get(reverse('geotom-list'), {'time_utc': '2026-04-21T12:34:56'})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'data-live-mode="0"')
        self.assertContains(response, 'name="time_utc" value="2026-04-21T12:34:56"')

    def test_geotom_live_data_returns_rows_and_map_payload(self):
        sat_payload = {
            'tle_name': 'TEST-SAT',
            'alt_deg': 12.3456,
            'az_deg': 234.5678,
            'ra_icrf_hours': 5.5,
            'dec_deg': -12.25,
            'hour_angle_hours': 1.25,
            'distance_km': 41000.0,
            'solar_elongation_deg': 120.0,
            'phase_angle_deg': 60.0,
            'estimated_vmag': 10.12,
            'computed_at_utc': None,
        }
        sun_payload = {
            'sun_alt_deg': -20.0,
            'sun_az_deg': 180.0,
            'curve_points': [{'az_deg': 180.0, 'alt_deg': 0.0}],
        }

        with patch('custom_code.views.geosat_alt_az_from_tle', return_value=sat_payload), \
             patch('custom_code.views.sun_visibility_curve', return_value=sun_payload):
            response = self.client.get(reverse('geotom-live-data'))

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload['live_mode'])
        self.assertEqual(payload['rows'][0]['target_id'], self.target.pk)
        self.assertEqual(payload['rows'][0]['hour_angle_sex'], '01:15:00')
        self.assertEqual(payload['rows'][0]['ra_icrf_sex'], '05:30:00')
        self.assertEqual(payload['rows'][0]['dec_sex'], '-12:15:00')
        self.assertAlmostEqual(payload['targets'][0]['alt_deg'], 12.3456)


class TokenAuthAndProfileTests(TestCase):
    def test_orcid_id_is_canonicalized_and_validated(self):
        self.assertEqual(canonicalize_orcid('https://orcid.org/0000000218250097'), '0000-0002-1825-0097')
        self.assertEqual(validate_orcid('0000000218250097'), '0000-0002-1825-0097')

    def test_orcid_username_generation_uses_name_and_collision_suffix(self):
        get_user_model().objects.create_user(username='lukasz.o.neil.smith')

        username = unique_orcid_username('Lukasz', "O'Neil-Smith", '0000-0002-1825-0097')

        self.assertEqual(username, 'lukasz.o.neil.smith2')

    def test_orcid_username_generation_falls_back_to_orcid_id(self):
        username = unique_orcid_username('', '', '0000-0002-1825-0097')

        self.assertEqual(username, 'orcid.0000-0002-1825-0097')

    def test_api_token_auth_returns_token_for_valid_credentials(self):
        user = get_user_model().objects.create_user(username='token-user', password='secret-pass')

        response = self.client.post(
            reverse('api-token-auth'),
            data=json.dumps({'username': user.username, 'password': 'secret-pass'}),
            content_type='application/json',
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn('token', payload)
        self.assertTrue(Token.objects.filter(user=user, key=payload['token']).exists())

    def test_user_profile_redirects_to_current_user_update_page(self):
        user = get_user_model().objects.create_user(username='profile-user', password='secret')
        self.client.force_login(user)

        response = self.client.get(reverse('user-profile'))

        self.assertRedirects(response, reverse('user-update', kwargs={'pk': user.pk}))

    def test_user_update_page_displays_copy_token_button(self):
        user = get_user_model().objects.create_user(username='profile-button-user', password='secret')
        self.client.force_login(user)

        response = self.client.get(reverse('user-update', kwargs={'pk': user.pk}))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Copy Token to Clipboard')
        self.assertContains(response, 'id="user-token"')
        token = Token.objects.get(user=user)
        self.assertContains(response, token.key)

    def test_user_update_page_displays_orcid_profile_status(self):
        user = get_user_model().objects.create_user(username='profile-orcid-user', password='secret')
        BhtomUserProfile.objects.update_or_create(
            user=user,
            defaults={
                'orcid_id': '0000-0002-1825-0097',
                'orcid_verified': True,
                'orcid_source': BhtomUserProfile.OrcidSource.OAUTH,
            },
        )
        self.client.force_login(user)

        response = self.client.get(reverse('user-update', kwargs={'pk': user.pk}))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, '0000-0002-1825-0097')
        self.assertContains(response, 'verified')

    def test_user_create_post_persists_user_and_redirects(self):
        admin = get_user_model().objects.create_superuser('profile-admin', 'admin@example.com', 'secret')
        self.client.force_login(admin)

        response = self.client.post(
            reverse('user-create'),
            data={
                'username': 'created-user',
                'first_name': 'Created',
                'last_name': 'User',
                'email': 'created@example.com',
                'about': 'I need a BHTOM account for target follow-up work.',
                'password1': 'create-user-secret-123',
                'password2': 'create-user-secret-123',
            },
        )

        self.assertRedirects(response, reverse('user-list'))
        created_user = get_user_model().objects.get(username='created-user')
        self.assertEqual(created_user.first_name, 'Created')
        self.assertEqual(created_user.last_name, 'User')
        self.assertEqual(created_user.email, 'created@example.com')
        self.assertTrue(created_user.check_password('create-user-secret-123'))

    def test_classic_user_create_stores_manual_orcid_unverified(self):
        form = BhtomUserCreationForm(
            data={
                'username': 'classic-orcid-user',
                'first_name': 'Classic',
                'last_name': 'Orcid',
                'email': 'classic-orcid@example.com',
                'password1': 'create-user-secret-123',
                'password2': 'create-user-secret-123',
                'orcid_id': '0000000218250097',
                'affiliation': 'Warsaw University Observatory',
                'about': 'Researcher profile.',
            }
        )

        self.assertTrue(form.is_valid(), form.errors.as_json())
        user = form.save()
        profile = user.bhtom_profile
        self.assertEqual(profile.orcid_id, '0000-0002-1825-0097')
        self.assertFalse(profile.orcid_verified)
        self.assertEqual(profile.orcid_source, BhtomUserProfile.OrcidSource.MANUAL)
        self.assertEqual(profile.affiliation, 'Warsaw University Observatory')
        self.assertEqual(profile.about, 'Researcher profile.')

    def test_duplicate_manual_orcid_is_rejected(self):
        existing = get_user_model().objects.create_user(username='existing-orcid')
        BhtomUserProfile.objects.update_or_create(user=existing, defaults={'orcid_id': '0000-0002-1825-0097'})

        form = BhtomUserCreationForm(
            data={
                'username': 'duplicate-orcid',
                'first_name': 'Duplicate',
                'last_name': 'Orcid',
                'email': 'duplicate-orcid@example.com',
                'password1': 'create-user-secret-123',
                'password2': 'create-user-secret-123',
                'orcid_id': '0000-0002-1825-0097',
            }
        )

        self.assertFalse(form.is_valid())
        self.assertIn('orcid_id', form.errors)

    def test_user_update_post_persists_profile_changes_without_password_reset(self):
        user = get_user_model().objects.create_user(
            username='profile-edit-user',
            password='existing-secret',
            first_name='Before',
            email='before@example.com',
        )
        self.client.force_login(user)

        response = self.client.post(
            reverse('user-update', kwargs={'pk': user.pk}),
            data={
                'username': 'profile-edit-user',
                'first_name': 'After',
                'last_name': 'Editor',
                'email': 'after@example.com',
                'password1': '',
                'password2': '',
                'orcid_id': '0000-0002-1825-0097',
                'affiliation': 'After Institute',
                'about': 'Updated about text.',
            },
        )

        self.assertRedirects(response, reverse('user-update', kwargs={'pk': user.pk}))
        user.refresh_from_db()
        self.assertEqual(user.first_name, 'After')
        self.assertEqual(user.last_name, 'Editor')
        self.assertEqual(user.email, 'after@example.com')
        self.assertTrue(user.check_password('existing-secret'))
        profile = user.bhtom_profile
        self.assertEqual(profile.orcid_id, '0000-0002-1825-0097')
        self.assertFalse(profile.orcid_verified)
        self.assertEqual(profile.orcid_source, BhtomUserProfile.OrcidSource.MANUAL)
        self.assertEqual(profile.affiliation, 'After Institute')
        self.assertEqual(profile.about, 'Updated about text.')


class TargetDownloadPhotometryApiTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username='phot-api-user', password='secret')
        self.token = Token.objects.create(user=self.user)
        self.target = Target.objects.create(name='Gaia26xyz', type='SIDEREAL', ra=12.3, dec=-45.6, epoch=2000.0)
        self.url = reverse('targets-download-photometry-api')

    def test_download_photometry_requires_token_auth(self):
        response = self.client.post(
            self.url,
            data=json.dumps({'name': self.target.name}),
            content_type='application/json',
        )

        self.assertEqual(response.status_code, 401)

    def test_download_photometry_returns_bhtom2_style_semicolon_file(self):
        ReducedDatum.objects.create(
            target=self.target,
            data_type='photometry',
            timestamp=datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
            value={
                'magnitude': 17.2,
                'error': 0.13,
                'facility': 'OGLE',
                'filter': 'OGLE(I)',
                'observer': 'survey',
            },
            source_name='OGLE',
        )
        ReducedDatum.objects.create(
            target=self.target,
            data_type='photometry',
            timestamp=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
            value={
                'limit': 18.7,
                'error': -1.0,
                'telescope': 'LCO',
                'filter': 'LCO(r)',
                'observer': 'bot',
            },
            source_name='LCO',
        )
        ReducedDatum.objects.create(
            target=self.target,
            data_type='spectroscopy',
            timestamp=datetime(2024, 1, 3, 12, 0, tzinfo=timezone.utc),
            value={'flux': [1, 2, 3]},
            source_name='Spec',
        )

        response = self.client.post(
            self.url,
            data=json.dumps({'name': self.target.name}),
            content_type='application/json',
            HTTP_AUTHORIZATION=f'Token {self.token.key}',
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response['Content-Type'].startswith('text/csv'))
        self.assertIn('target_Gaia26xyz_photometry.csv', response['Content-Disposition'])

        lines = response.content.decode('utf-8').splitlines()
        self.assertEqual(lines[0], 'MJD;Magnitude;Error;Facility;Filter;Observer')
        self.assertEqual(len(lines), 3)

        first_row = lines[1].split(';')
        second_row = lines[2].split(';')
        self.assertEqual(first_row[3:], ['LCO', 'LCO(r)', 'bot'])
        self.assertEqual(second_row[3:], ['OGLE', 'OGLE(I)', 'survey'])
        self.assertLess(float(first_row[0]), float(second_row[0]))


class LCOFacilityAccountRoutingTests(TestCase):
    def setUp(self):
        cache.delete('LCO_instruments')
        self.user = get_user_model().objects.create_user(username='lco-user', password='secret')
        self.target = Target.objects.create(
            name='LCO Target',
            type=Target.SIDEREAL,
            ra=12.3,
            dec=-45.6,
            epoch=2000.0,
        )
        self.facility, _ = Facility.objects.get_or_create(
            code='LCO',
            defaults={'name': 'Las Cumbres Observatory'},
        )
        self.account = FacilityAccount.objects.create(
            facility=self.facility,
            label='Account A',
            created_by=self.user,
            account_data={'portal_url': 'https://observe.lco.global'},
            credentials={'api_key': 'account-api-key'},
        )
        FacilityAccountMembership.objects.create(
            account=self.account,
            user=self.user,
            role=FacilityAccountMembership.Role.OWNER,
        )
        self.proposal = FacilityProposal.objects.create(
            account=self.account,
            external_id='LCO2026A-001',
            title='Proposal A',
        )
        FacilityProposalMembership.objects.create(
            proposal=self.proposal,
            user=self.user,
            role=FacilityProposalMembership.Role.OWNER,
        )

    def test_account_settings_return_account_api_key(self):
        settings = AccountLCOSettings(account=self.account)

        self.assertEqual(settings.get_setting('api_key'), 'account-api-key')
        self.assertEqual(settings.get_setting('portal_url'), 'https://observe.lco.global')

    @patch('bhtom3.bhtom_observations.facilities.lco.BaseLCOFacility.get_observation_status', autospec=True)
    def test_update_status_uses_proposal_account_credentials(self, mock_get_observation_status):
        def fake_get_observation_status(facility_instance, observation_id):
            self.assertEqual(observation_id, '4205507')
            self.assertEqual(
                facility_instance.facility_settings.get_setting('api_key'),
                'account-api-key',
            )
            return {
                'state': 'COMPLETED',
                'scheduled_start': datetime(2026, 6, 1, 1, 0, tzinfo=timezone.utc),
                'scheduled_end': datetime(2026, 6, 1, 2, 0, tzinfo=timezone.utc),
            }

        mock_get_observation_status.side_effect = fake_get_observation_status
        record = ObservationRecord.objects.create(
            target=self.target,
            user=self.user,
            facility='LCO',
            parameters={'proposal': str(self.proposal.pk)},
            observation_id='4205507',
            status='PENDING',
        )

        LCOFacility().update_observation_status('4205507')

        record.refresh_from_db()
        self.assertEqual(record.status, 'COMPLETED')
        self.assertEqual(mock_get_observation_status.call_count, 1)

    @patch('bhtom3.bhtom_observations.facilities.lco.BaseLCOFacility.get_observation_status', autospec=True)
    def test_update_status_marks_missing_remote_lco_request_canceled(self, mock_get_observation_status):
        response = Mock(status_code=404)
        http_error = requests.HTTPError('404 Client Error', response=response)
        mock_get_observation_status.side_effect = http_error
        record = ObservationRecord.objects.create(
            target=self.target,
            user=self.user,
            facility='LCO',
            parameters={'proposal': str(self.proposal.pk)},
            observation_id='4200393',
            status='PENDING',
        )

        LCOFacility().update_observation_status('4200393')

        record.refresh_from_db()
        self.assertEqual(record.status, 'CANCELED')
        self.assertIsNone(record.scheduled_start)
        self.assertIsNone(record.scheduled_end)

    @patch('bhtom3.bhtom_observations.facilities.lco.requests.post')
    def test_resolve_lco_bhtom2_observatory_oname_uses_bhtom2_prefixes(self, mock_post):
        cache.delete('lco_bhtom2_onames_v1')
        mock_post.return_value = Mock(
            raise_for_status=Mock(),
            content=b'{}',
            json=Mock(return_value=_bhtom2_lco_observatory_payload()),
        )

        qhy_oname = resolve_lco_bhtom2_observatory_oname(
            {'basename': 'tfn0m410-sq01-20260601-0001-e91'},
            'auto-token',
        )
        spectral_oname = resolve_lco_bhtom2_observatory_oname(
            {'basename': 'coj2m002-fs01-20260601-0001-e91'},
            'auto-token',
        )
        sbig_oname = resolve_lco_bhtom2_observatory_oname(
            {'basename': 'lsc0m409-kb98-20260601-0001-e91'},
            'auto-token',
        )

        self.assertEqual(qhy_oname, 'LCOGT-Teide-40cm_QHY600M')
        self.assertEqual(spectral_oname, 'LCOGT-SS-2m_Spectral')
        self.assertEqual(sbig_oname, 'LCOGT-CTIO-40cm_SBIG6303')
        mock_post.assert_called_once()

    @patch('bhtom3.bhtom_observations.facilities.lco.requests.post')
    @patch('custom_code.bhtom2_uploads.requests.post')
    @patch('bhtom3.bhtom_observations.facilities.lco.run_data_processor', return_value=ReducedDatum.objects.none())
    @patch('bhtom3.bhtom_observations.facilities.lco.run_hook')
    @patch('bhtom3.bhtom_observations.facilities.lco.requests.get')
    @patch('bhtom3.bhtom_observations.facilities.lco.BaseLCOFacility.get_observation_status', autospec=True)
    def test_update_status_downloads_completed_lco_frames_and_forwards_to_bhtom2(
        self,
        mock_get_observation_status,
        mock_requests_get,
        mock_run_hook,
        mock_run_data_processor,
        mock_upload_post,
        mock_bhtom2_observatory_post,
    ):
        cache.delete('lco_bhtom2_onames_v1')
        mock_get_observation_status.return_value = {
            'state': 'COMPLETED',
            'scheduled_start': datetime(2026, 6, 1, 1, 0, tzinfo=timezone.utc),
            'scheduled_end': datetime(2026, 6, 1, 2, 0, tzinfo=timezone.utc),
        }
        mock_requests_get.side_effect = [
            Mock(
                raise_for_status=Mock(),
                json=Mock(return_value={
                    'results': [{
                        'id': 123456,
                        'basename': 'tfn0m410-sq01-20260601-0001-e91',
                        'extension': '.fits.fz',
                        'url': 'https://archive-api.lco.global/frames/123456/file',
                        'request_id': 4205507,
                        'observation_id': 4205507,
                        'reduction_level': 91,
                    }],
                    'next': None,
                }),
            ),
            Mock(
                raise_for_status=Mock(),
                content=_build_test_fits_bytes(),
            ),
        ]
        mock_bhtom2_observatory_post.return_value = Mock(
            raise_for_status=Mock(),
            content=b'{}',
            json=Mock(return_value=_bhtom2_lco_observatory_payload()),
        )
        mock_upload_post.return_value = Mock(status_code=201, json=Mock(return_value={'ok': True}), text='created')
        record = ObservationRecord.objects.create(
            target=self.target,
            user=self.user,
            facility='LCO',
            parameters={'proposal': str(self.proposal.pk)},
            observation_id='4205507',
            status='PENDING',
        )

        with self.settings(BHTOM2_API_TOKEN='auto-token', BHTOM2_UPLOAD_SERVICE_URL='http://upload.example/api/upload'):
            LCOFacility().update_observation_status('4205507')

        record.refresh_from_db()
        self.assertEqual(record.status, 'COMPLETED')
        dataproduct = DataProduct.objects.get(observation_record=record, product_id='123456')
        self.assertEqual(dataproduct.data_product_type, 'fits_file')
        self.assertTrue(has_successful_bhtom2_upload(dataproduct))
        self.assertEqual(dataproduct.get_file_name(), 'tfn0m410-sq01-20260601-0001-e91.fits')
        self.assertEqual(mock_upload_post.call_args.kwargs['headers']['Authorization'], 'Token auto-token')
        self.assertEqual(
            mock_upload_post.call_args.kwargs['data']['observatory'],
            'LCOGT-Teide-40cm_QHY600M',
        )
        self.assertEqual(mock_requests_get.call_args_list[0].kwargs['headers']['Authorization'], 'Token account-api-key')
        self.assertEqual(mock_requests_get.call_args_list[0].kwargs['params']['request_id'], '4205507')
        self.assertEqual(mock_requests_get.call_args_list[0].kwargs['params']['reduction_level'], 91)
        self.assertEqual(mock_requests_get.call_args_list[0].kwargs['params']['public'], 'false')
        self.assertEqual(mock_bhtom2_observatory_post.call_count, 1)
        mock_run_hook.assert_called()
        mock_run_data_processor.assert_called_once()

    @patch('custom_code.bhtom2_uploads.requests.post')
    @patch('bhtom3.bhtom_observations.facilities.lco.requests.get')
    @patch('bhtom3.bhtom_observations.facilities.lco.BaseLCOFacility.get_observation_status', autospec=True)
    def test_update_status_skips_redownload_for_already_forwarded_lco_frame(
        self,
        mock_get_observation_status,
        mock_requests_get,
        mock_post,
    ):
        mock_get_observation_status.return_value = {
            'state': 'COMPLETED',
            'scheduled_start': datetime(2026, 6, 1, 1, 0, tzinfo=timezone.utc),
            'scheduled_end': datetime(2026, 6, 1, 2, 0, tzinfo=timezone.utc),
        }
        mock_requests_get.return_value = Mock(
            raise_for_status=Mock(),
            json=Mock(return_value={
                'results': [{
                    'id': 123456,
                    'basename': 'tfn0m410-sq01-20260601-0001-e91',
                    'extension': '.fits',
                    'url': 'https://archive-api.lco.global/frames/123456/file',
                    'request_id': 4205507,
                    'observation_id': 4205507,
                    'reduction_level': 91,
                }],
                'next': None,
            }),
        )
        record = ObservationRecord.objects.create(
            target=self.target,
            user=self.user,
            facility='LCO',
            parameters={'proposal': str(self.proposal.pk)},
            observation_id='4205507',
            status='PENDING',
        )
        dataproduct = DataProduct.objects.create(
            target=self.target,
            observation_record=record,
            product_id='123456',
            data=SimpleUploadedFile('existing.fits', _build_test_fits_bytes()),
            data_product_type='fits_file',
        )
        extra_data = json.loads(dataproduct.extra_data or '{}') if dataproduct.extra_data else {}
        extra_data['bhtom2_fits_upload'] = {'status': 'uploaded'}
        dataproduct.extra_data = json.dumps(extra_data)
        dataproduct.save(update_fields=['extra_data'])

        with self.settings(BHTOM2_API_TOKEN='auto-token', BHTOM2_UPLOAD_SERVICE_URL='http://upload.example/api/upload'):
            LCOFacility().update_observation_status('4205507')

        dataproduct.refresh_from_db()
        self.assertTrue(has_successful_bhtom2_upload(dataproduct))
        self.assertEqual(mock_requests_get.call_count, 1)
        mock_post.assert_not_called()

    @patch('bhtom3.bhtom_observations.facilities.lco.BaseLCOFacility.submit_observation', autospec=True)
    def test_submit_uses_imported_lco_proposal_id(self, mock_submit_observation):
        self.proposal.external_id = '24'
        self.proposal.remote_payload = {'id': 24}
        self.proposal.save()
        mock_submit_observation.return_value = ['4205507']

        result = LCOFacility().submit_observation({
            'name': 'BHTOM Gaia26abc 20260602',
            'proposal': str(self.proposal.pk),
            'requests': [],
        })

        self.assertEqual(result, ['4205507'])
        submitted_payload = mock_submit_observation.call_args.args[1]
        self.assertEqual(submitted_payload['proposal'], '24')

    def test_submit_rejects_unresolved_numeric_lco_proposal(self):
        with self.assertRaisesMessage(Exception, 'LCO proposal 999999 is not available in BHTOM'):
            LCOFacility().submit_observation({
                'name': 'BHTOM Gaia26abc 20260602',
                'proposal': '999999',
                'requests': [],
            })

    @patch('bhtom3.bhtom_observations.facilities.lco.BaseLCOFacility.submit_observation', autospec=True)
    def test_submit_accepts_numeric_lco_identifier(self, mock_submit_observation):
        self.proposal.external_id = '26'
        self.proposal.remote_payload = {'id': 26}
        self.proposal.save()
        mock_submit_observation.return_value = ['4205507']

        result = LCOFacility().submit_observation({
            'name': 'BHTOM Gaia26abc 20260602',
            'proposal': str(self.proposal.pk),
            'requests': [],
        })

        self.assertEqual(result, ['4205507'])
        submitted_payload = mock_submit_observation.call_args.args[1]
        self.assertEqual(submitted_payload['proposal'], '26')

    @patch('bhtom3.bhtom_observations.facilities.lco.BhtomLCOFormMixin._get_instruments')
    @patch('bhtom3.bhtom_observations.facilities.lco.make_request')
    def test_lco_cadence_expansion_uses_selected_proposal_account(self, mock_make_request, mock_get_instruments):
        mock_get_instruments.return_value = _minimal_lco_instruments()
        self.proposal.external_id = '26'
        self.proposal.save()
        mock_make_request.return_value.json.return_value = {'requests': [{'id': 'expanded'}]}
        form = BhtomLCOImagingObservationForm(initial={
            'request_user_id': self.user.pk,
            'target_id': self.target.pk,
            'facility': 'LCO',
        })
        form.cleaned_data = {
            'proposal': str(self.proposal.pk),
            'start': '2026-06-02T12:00:00+00:00',
            'end': '2026-06-03T12:00:00+00:00',
            'period': 24.0,
            'jitter': 1.0,
        }

        result = form._expand_cadence_request({
            'name': 'BHTOM Gaia26abc 20260602',
            'proposal': str(self.proposal.pk),
            'requests': [{'windows': [{'start': 'x', 'end': 'y'}]}],
        })

        self.assertEqual(result, {'requests': [{'id': 'expanded'}]})
        call_kwargs = mock_make_request.call_args.kwargs
        self.assertEqual(call_kwargs['json']['proposal'], '26')
        self.assertEqual(call_kwargs['headers']['Authorization'], 'Token account-api-key')
        self.assertEqual(call_kwargs['json']['requests'][0]['windows'], [])
        self.assertEqual(call_kwargs['json']['requests'][0]['cadence']['period'], 24.0)

    @patch('bhtom3.bhtom_observations.facilities.lco.BhtomLCOFormMixin._get_instruments')
    def test_lco_form_uses_local_proposal_choices(self, mock_get_instruments):
        mock_get_instruments.return_value = _minimal_lco_instruments()

        form = BhtomLCOImagingObservationForm(initial={
            'request_user_id': self.user.pk,
            'target_id': self.target.pk,
            'facility': 'LCO',
        })

        proposal_values = [value for value, label in form.fields['proposal'].choices]
        self.assertIn(str(self.proposal.pk), proposal_values)

    @patch('custom_code.facility_proposals.requests.get')
    def test_lco_remote_resync_preserves_import_shared_users(self, mock_get):
        shared_user = get_user_model().objects.create_user(username='fraser.gillan', password='secret')
        self.facility.supports_remote_proposal_sync = True
        self.facility.save(update_fields=['supports_remote_proposal_sync'])
        mock_get.return_value = Mock(
            raise_for_status=Mock(),
            json=Mock(return_value={
                'proposals': [{
                    'id': 'SUPA2026A-017',
                    'title': 'Astrophysical transients (continuation)',
                    'current': True,
                }],
            }),
        )

        sync_remote_proposals_for_account(self.account, owner=self.user, shared_users=[shared_user])
        imported_proposal = FacilityProposal.objects.get(account=self.account, external_id='SUPA2026A-017')
        self.assertTrue(imported_proposal.memberships.filter(user=shared_user).exists())
        shared_account_membership = self.account.memberships.get(user=shared_user)
        self.assertEqual(shared_account_membership.role, FacilityAccountMembership.Role.VIEWER)
        self.assertFalse(shared_account_membership.can_view_credentials)

        sync_remote_proposals_for_account(self.account)

        self.assertTrue(imported_proposal.memberships.filter(user=shared_user).exists())
        self.assertIn(imported_proposal, get_accessible_proposals(shared_user, 'LCO'))
        proposal_choice_values = [value for value, _label in get_proposal_choices_for_user(shared_user, 'LCO')]
        self.assertIn(str(imported_proposal.pk), proposal_choice_values)

    @patch('bhtom3.bhtom_observations.facilities.lco.BhtomLCOFormMixin._get_instruments')
    def test_lco_form_builds_etc_context_from_recent_filter_photometry(self, mock_get_instruments):
        mock_get_instruments.return_value = _minimal_lco_instruments()
        self.target.mag_last = 18.7
        self.target.save(update_fields=['mag_last'])
        ReducedDatum.objects.create(
            target=self.target,
            data_type='photometry',
            timestamp=datetime(2026, 6, 1, 1, 0, tzinfo=timezone.utc),
            value={'magnitude': 19.4, 'filter': 'ZTF(g)'},
        )
        ReducedDatum.objects.create(
            target=self.target,
            data_type='photometry',
            timestamp=datetime(2026, 6, 1, 2, 0, tzinfo=timezone.utc),
            value={'magnitude': 18.9, 'filter': 'LCO(r)'},
        )

        form = BhtomLCOImagingObservationForm(initial={
            'request_user_id': self.user.pk,
            'target_id': self.target.pk,
            'facility': 'LCO',
        })

        gp_row = next(row for row in form.lco_etc_context['rows_by_class']['0m4'] if row['filter_code'] == 'gp')
        self.assertEqual(gp_row['magnitude'], 19.4)
        self.assertEqual(gp_row['source'], 'recent')
        self.assertGreater(gp_row['exposure_time'], 0)
        self.assertIn('lco-etc-widget', str(form.helper.layout))

    def test_lco_etc_calculator_scales_with_telescope_aperture(self):
        exposure_0m4 = calculate_lco_etc_exposure_time('0m4', 'gp', 19.4, signal_to_noise=100.0)
        exposure_1m0 = calculate_lco_etc_exposure_time('1m0', 'gp', 19.4, signal_to_noise=100.0)
        exposure_2m0 = calculate_lco_etc_exposure_time('2m0', 'gp', 19.4, signal_to_noise=100.0)

        self.assertIsNotNone(exposure_0m4)
        self.assertIsNotNone(exposure_1m0)
        self.assertIsNotNone(exposure_2m0)
        self.assertGreater(exposure_0m4, exposure_1m0)
        self.assertGreater(exposure_1m0, exposure_2m0)

    @patch('bhtom3.bhtom_observations.facilities.lco.requests.get')
    def test_lco_instrument_loading_uses_timeout_and_fallback(self, mock_get):
        mock_get.side_effect = TimeoutError('slow LCO instruments')
        form = BhtomLCOImagingObservationForm(initial={
            'request_user_id': self.user.pk,
            'target_id': self.target.pk,
            'facility': 'LCO',
        })

        instruments = form._get_instruments()

        self.assertIn('No Instrument Found', instruments)
        self.assertEqual(mock_get.call_args.kwargs['timeout'], 8)

    @patch('bhtom3.bhtom_observations.facilities.lco.BhtomLCOFormMixin._get_instruments')
    def test_lco_monitoring_form_prefills_week_window_and_target_cadence(self, mock_get_instruments):
        mock_get_instruments.return_value = _minimal_lco_instruments()
        self.target.cadence = 2.5
        self.target.save(update_fields=['cadence'])
        fixed_start = datetime(2026, 6, 2, 7, 15, 30, tzinfo=timezone.utc)

        form = BhtomLCOMonitoringObservationForm(initial={
            'request_user_id': self.user.pk,
            'target_id': self.target.pk,
            'facility': 'LCO',
            'start': fixed_start,
        })

        self.assertEqual(form.initial['end'], fixed_start + timedelta(days=7))
        self.assertEqual(form.initial['period'], 2.5)
        self.assertEqual(form.fields['period'].help_text, 'days')
        self.assertIn('6 means a 12-hour request window', form.fields['monitoring_dither_hours'].help_text)
        self.assertEqual(form.fields['c_1_min_lunar_distance'].initial, 30)
        self.assertIsInstance(form.fields['start'], forms.DateTimeField)
        self.assertIn('monitoring_frames_gp', form.fields)
        self.assertIn('MONITORING', LCOFacility.observation_forms)

    @patch('bhtom3.bhtom_observations.facilities.lco.BhtomLCOFormMixin._get_instruments')
    def test_lco_monitoring_accepts_immutable_post_data(self, mock_get_instruments):
        mock_get_instruments.return_value = _minimal_lco_instruments()
        data = QueryDict('', mutable=True)
        data.update({
            'request_user_id': str(self.user.pk),
            'target_id': str(self.target.pk),
            'facility': 'LCO',
            'observation_type': 'MONITORING',
            'name': 'BHTOM LCO Target 20260602',
        })
        data._mutable = False

        form = BhtomLCOMonitoringObservationForm(data=data, initial={
            'request_user_id': self.user.pk,
            'target_id': self.target.pk,
            'facility': 'LCO',
        })

        self.assertIn('monitoring_dither_hours', form.fields)

    @patch('bhtom3.bhtom_observations.facilities.lco.BhtomLCOFormMixin._get_instruments')
    def test_lco_monitoring_validate_skips_remote_validation_when_start_is_missing(self, mock_get_instruments):
        mock_get_instruments.return_value = _minimal_lco_instruments()
        form = BhtomLCOMonitoringObservationForm(data={
            'request_user_id': str(self.user.pk),
            'target_id': str(self.target.pk),
            'facility': 'LCO',
            'observation_type': 'MONITORING',
            'name': 'BHTOM LCO Target 20260602',
            'proposal': str(self.proposal.pk),
            'ipp_value': '1.05',
            'observation_mode': 'NORMAL',
            'end': '2026-06-09T12:00:00+00:00',
            'period': '2',
            'monitoring_dither_hours': '1.5',
            'c_1_instrument_type': '0M4-SCICAM-SBIG',
            'c_1_configuration_type': 'EXPOSE',
            'c_1_max_airmass': '1.6',
            'monitoring_frames_gp': '1',
            'monitoring_exp_gp': '86',
        }, initial={
            'request_user_id': self.user.pk,
            'target_id': self.target.pk,
            'facility': 'LCO',
        })

        self.assertFalse(form.is_valid())
        self.assertIn('start', form.errors)

    @patch('bhtom3.bhtom_observations.facilities.lco.BhtomLCOMonitoringObservationForm.validate_at_facility')
    @patch('bhtom3.bhtom_observations.facilities.lco.BhtomLCOFormMixin._get_instruments')
    def test_lco_monitoring_accepts_datetime_local_start_and_end(self, mock_get_instruments, mock_validate):
        mock_get_instruments.return_value = _minimal_lco_instruments()
        form = BhtomLCOMonitoringObservationForm(data={
            'request_user_id': str(self.user.pk),
            'target_id': str(self.target.pk),
            'facility': 'LCO',
            'observation_type': 'MONITORING',
            'name': 'BHTOM LCO Target 20260602',
            'proposal': str(self.proposal.pk),
            'ipp_value': '1.05',
            'observation_mode': 'NORMAL',
            'start': '2026-06-02T12:00',
            'end': '2026-06-09T12:00',
            'period': '2',
            'monitoring_dither_hours': '1.5',
            'c_1_instrument_type': '0M4-SCICAM-SBIG',
            'c_1_configuration_type': 'EXPOSE',
            'c_1_max_airmass': '1.6',
            'monitoring_frames_gp': '1',
            'monitoring_exp_gp': '86',
        }, initial={
            'request_user_id': self.user.pk,
            'target_id': self.target.pk,
            'facility': 'LCO',
        })

        self.assertTrue(form.is_valid(), form.errors)
        self.assertEqual(form.cleaned_data['start'], '2026-06-02T12:00:00+00:00')

    @patch('bhtom3.bhtom_observations.facilities.lco.BhtomLCOFormMixin._get_instruments')
    def test_lco_monitoring_payload_uses_selected_filter_frames_as_repeated_windows(self, mock_get_instruments):
        mock_get_instruments.return_value = _minimal_lco_instruments()
        form = BhtomLCOMonitoringObservationForm(initial={
            'request_user_id': self.user.pk,
            'target_id': self.target.pk,
            'facility': 'LCO',
        })
        form.cleaned_data = {
            'name': 'BHTOM LCO Target 20260602',
            'proposal': str(self.proposal.pk),
            'ipp_value': 1.05,
            'observation_mode': 'NORMAL',
            'optimization_type': 'TIME',
            'configuration_repeats': 1,
            'target_id': self.target.pk,
            'start': '2026-06-02T12:00:00+00:00',
            'end': '2026-06-09T12:00:00+00:00',
            'period': 2.0,
            'monitoring_dither_hours': 1.5,
            'jitter': 0.0,
            'c_1_instrument_type': '0M4-SCICAM-SBIG',
            'c_1_configuration_type': 'EXPOSE',
            'c_1_max_airmass': 1.6,
            'c_1_min_lunar_distance': 30,
            'c_1_max_lunar_phase': None,
            'dither_pattern': '',
            'dither_point_spacing': None,
            'monitoring_frames_gp': 2,
            'monitoring_exp_gp': 86.0,
        }
        for filter_code in BhtomLCOMonitoringObservationForm.monitoring_filter_codes:
            form.cleaned_data.setdefault(f'monitoring_frames_{filter_code}', 0)
            form.cleaned_data.setdefault(f'monitoring_exp_{filter_code}', None)

        result = form.observation_payload()

        self.assertEqual(result['operator'], 'MANY')
        self.assertEqual(len(result['requests']), 4)
        self.assertEqual(
            [request['windows'][0] for request in result['requests']],
            [
                {'start': '2026-06-02T10:30:00+00:00', 'end': '2026-06-02T13:30:00+00:00'},
                {'start': '2026-06-04T10:30:00+00:00', 'end': '2026-06-04T13:30:00+00:00'},
                {'start': '2026-06-06T10:30:00+00:00', 'end': '2026-06-06T13:30:00+00:00'},
                {'start': '2026-06-08T10:30:00+00:00', 'end': '2026-06-08T13:30:00+00:00'},
            ],
        )
        request = result['requests'][0]
        self.assertEqual(len(request['configurations']), 1)
        configuration = request['configurations'][0]
        self.assertEqual(configuration['constraints']['max_airmass'], 1.6)
        self.assertEqual(configuration['constraints']['min_lunar_distance'], 30)
        self.assertEqual(configuration['instrument_configs'], [{
            'exposure_count': 2,
            'exposure_time': 86.0,
            'mode': '1x1',
            'optical_elements': {'filter': 'gp'},
        }])

    @patch('bhtom3.bhtom_observations.facilities.lco.BhtomLCOFormMixin._get_instruments')
    def test_lco_monitoring_payload_preserves_selected_qhy600_readout_mode(self, mock_get_instruments):
        mock_get_instruments.return_value = _minimal_lco_instruments()
        form = BhtomLCOMonitoringObservationForm(initial={
            'request_user_id': self.user.pk,
            'target_id': self.target.pk,
            'facility': 'LCO',
            'c_1_instrument_type': '0M4-SCICAM-QHY600',
        })
        self.assertEqual(form.fields['c_1_ic_1_readout_mode'].initial, 'qhy600_central_30x30')
        self.assertIn(('qhy600_full_frame', 'QHY600 Full Frame Readout'), form.fields['c_1_ic_1_readout_mode'].choices)
        self.assertNotIn(('sinistro_full_frame', '1M Sinistro Full Frame'), form.fields['c_1_ic_1_readout_mode'].choices)
        form.cleaned_data = {
            'name': 'BHTOM LCO Target 20260602',
            'proposal': str(self.proposal.pk),
            'ipp_value': 1.05,
            'observation_mode': 'NORMAL',
            'optimization_type': 'TIME',
            'configuration_repeats': 1,
            'target_id': self.target.pk,
            'start': '2026-06-02T12:00:00+00:00',
            'end': '2026-06-02T12:00:00+00:00',
            'period': 1.0,
            'monitoring_dither_hours': 1.5,
            'jitter': 0.0,
            'c_1_instrument_type': '0M4-SCICAM-QHY600',
            'c_1_configuration_type': 'EXPOSE',
            'c_1_max_airmass': 1.6,
            'c_1_min_lunar_distance': 30,
            'c_1_ic_1_readout_mode': 'qhy600_full_frame',
            'monitoring_frames_gp': 1,
            'monitoring_exp_gp': 86.0,
        }
        for filter_code in BhtomLCOMonitoringObservationForm.monitoring_filter_codes:
            form.cleaned_data.setdefault(f'monitoring_frames_{filter_code}', 0)
            form.cleaned_data.setdefault(f'monitoring_exp_{filter_code}', None)

        configuration = form.observation_payload()['requests'][0]['configurations'][0]

        self.assertEqual(configuration['instrument_type'], '0M4-SCICAM-QHY600')
        self.assertEqual(configuration['instrument_configs'][0]['mode'], 'qhy600_full_frame')

    @patch('bhtom3.bhtom_observations.facilities.lco.BhtomLCOFormMixin._get_instruments')
    def test_lco_monitoring_form_offers_sinistro_readout_modes(self, mock_get_instruments):
        mock_get_instruments.return_value = _minimal_lco_instruments()
        form = BhtomLCOMonitoringObservationForm(initial={
            'request_user_id': self.user.pk,
            'target_id': self.target.pk,
            'facility': 'LCO',
            'c_1_instrument_type': '1M0-SCICAM-SINISTRO',
        })

        self.assertEqual(form.fields['c_1_ic_1_readout_mode'].initial, 'sinistro_central_2k_2x2')
        self.assertIn(('sinistro_full_frame', '1M Sinistro Full Frame'), form.fields['c_1_ic_1_readout_mode'].choices)
        self.assertNotIn(('qhy600_full_frame', 'QHY600 Full Frame Readout'), form.fields['c_1_ic_1_readout_mode'].choices)

    @patch('bhtom3.bhtom_observations.facilities.lco.BhtomLCOFormMixin._get_instruments')
    def test_lco_monitoring_readout_fallback_filters_global_modes_by_instrument(self, mock_get_instruments):
        instruments = _minimal_lco_instruments()
        instruments['1M0-SCICAM-NOMODES'] = {
            'type': 'IMAGE',
            'class': '1m0',
            'name': '1.0 meter Sinistro',
            'optical_elements': {
                'filters': [
                    {'name': 'SDSS-gp', 'code': 'gp', 'schedulable': True, 'default': False},
                ],
            },
            'modes': {
                'guiding': {
                    'modes': [
                        {'name': 'On', 'code': 'ON'},
                        {'name': 'Off', 'code': 'OFF'},
                    ],
                },
            },
            'configuration_types': {
                'EXPOSE': {'name': 'Expose', 'code': 'EXPOSE', 'schedulable': True},
            },
            'default_configuration_type': 'EXPOSE',
        }
        mock_get_instruments.return_value = instruments

        form = BhtomLCOMonitoringObservationForm(initial={
            'request_user_id': self.user.pk,
            'target_id': self.target.pk,
            'facility': 'LCO',
            'c_1_instrument_type': '1M0-SCICAM-NOMODES',
        })

        self.assertIn(('sinistro_full_frame', '1M Sinistro Full Frame'), form.fields['c_1_ic_1_readout_mode'].choices)
        self.assertNotIn(('qhy600_full_frame', 'QHY600 Full Frame Readout'), form.fields['c_1_ic_1_readout_mode'].choices)

    @patch('bhtom3.bhtom_observations.facilities.lco.BhtomLCOFormMixin._get_instruments')
    def test_lco_monitoring_readout_replaces_wrong_modes_for_sinistro_instrument(self, mock_get_instruments):
        instruments = _minimal_lco_instruments()
        instruments['1M0-SCICAM-WRONGMODES'] = {
            'type': 'IMAGE',
            'class': '1m0',
            'name': '1.0 meter Sinistro',
            'optical_elements': {
                'filters': [
                    {'name': 'SDSS-gp', 'code': 'gp', 'schedulable': True, 'default': False},
                ],
            },
            'modes': {
                'readout': {
                    'modes': [
                        {'name': 'QHY600 Central 30x30 arcmin', 'code': 'qhy600_central_30x30'},
                        {'name': 'QHY600 Full Frame Readout', 'code': 'qhy600_full_frame'},
                    ],
                },
                'guiding': {
                    'modes': [
                        {'name': 'On', 'code': 'ON'},
                        {'name': 'Off', 'code': 'OFF'},
                    ],
                },
            },
            'configuration_types': {
                'EXPOSE': {'name': 'Expose', 'code': 'EXPOSE', 'schedulable': True},
            },
            'default_configuration_type': 'EXPOSE',
        }
        mock_get_instruments.return_value = instruments

        form = BhtomLCOMonitoringObservationForm(initial={
            'request_user_id': self.user.pk,
            'target_id': self.target.pk,
            'facility': 'LCO',
            'c_1_instrument_type': '1M0-SCICAM-WRONGMODES',
        })

        self.assertEqual(form.fields['c_1_ic_1_readout_mode'].choices, [
            ('sinistro_central_2k_2x2', '1M Sinistro Central 2k 2x2 binned'),
            ('sinistro_full_frame', '1M Sinistro Full Frame'),
        ])

    @patch('bhtom3.bhtom_observations.facilities.lco.BhtomLCOFormMixin._get_instruments')
    def test_lco_monitoring_dither_is_half_window_in_hours(self, mock_get_instruments):
        mock_get_instruments.return_value = _minimal_lco_instruments()
        form = BhtomLCOMonitoringObservationForm(initial={
            'request_user_id': self.user.pk,
            'target_id': self.target.pk,
            'facility': 'LCO',
        })
        form.cleaned_data = {
            'name': 'BHTOM LCO Target 20260602',
            'proposal': str(self.proposal.pk),
            'ipp_value': 1.05,
            'observation_mode': 'NORMAL',
            'optimization_type': 'TIME',
            'configuration_repeats': 1,
            'target_id': self.target.pk,
            'start': '2026-06-13T06:09:00+00:00',
            'end': '2026-06-15T06:09:00+00:00',
            'period': 2.0,
            'monitoring_dither_hours': 6.0,
            'c_1_instrument_type': '0M4-SCICAM-SBIG',
            'c_1_configuration_type': 'EXPOSE',
            'c_1_max_airmass': 1.6,
            'c_1_min_lunar_distance': 30,
            'monitoring_frames_gp': 1,
            'monitoring_exp_gp': 86.0,
        }
        for filter_code in BhtomLCOMonitoringObservationForm.monitoring_filter_codes:
            form.cleaned_data.setdefault(f'monitoring_frames_{filter_code}', 0)
            form.cleaned_data.setdefault(f'monitoring_exp_{filter_code}', None)

        request = form.observation_payload()['requests'][0]

        self.assertEqual(request['windows'][0], {
            'start': '2026-06-13T00:09:00+00:00',
            'end': '2026-06-13T12:09:00+00:00',
        })

    @patch('bhtom3.bhtom_observations.facilities.lco.BhtomLCOFormMixin._get_instruments')
    def test_lco_monitoring_validation_message_includes_schedule_summary(self, mock_get_instruments):
        mock_get_instruments.return_value = _minimal_lco_instruments()
        form = BhtomLCOMonitoringObservationForm(initial={
            'request_user_id': self.user.pk,
            'target_id': self.target.pk,
            'facility': 'LCO',
        })
        form.validation_message = 'This observation is valid.'
        form.cleaned_data = {
            'name': 'BHTOM LCO Target 20260602',
            'proposal': str(self.proposal.pk),
            'ipp_value': 1.05,
            'observation_mode': 'NORMAL',
            'optimization_type': 'TIME',
            'configuration_repeats': 1,
            'target_id': self.target.pk,
            'start': '2026-06-02T12:00:00+00:00',
            'end': '2026-06-09T12:00:00+00:00',
            'period': 2.0,
            'monitoring_dither_hours': 1.5,
            'c_1_instrument_type': '0M4-SCICAM-SBIG',
            'c_1_configuration_type': 'EXPOSE',
            'c_1_max_airmass': 1.6,
            'c_1_min_lunar_distance': 30,
            'monitoring_frames_gp': 2,
            'monitoring_exp_gp': 86.0,
        }
        for filter_code in BhtomLCOMonitoringObservationForm.monitoring_filter_codes:
            form.cleaned_data.setdefault(f'monitoring_frames_{filter_code}', 0)
            form.cleaned_data.setdefault(f'monitoring_exp_{filter_code}', None)

        message = form.get_validation_message()

        self.assertIn(
            'Requested schedule: 4 window(s), cadence 2 day(s), dither +/- 1.5 hour(s), full window 3 hour(s).',
            message,
        )
        self.assertIn('2026-06-02 10:30-13:30 UTC', message)
        self.assertIn('2026-06-08 10:30-13:30 UTC', message)


class LCOObservationCreateInitialTests(TestCase):
    def test_lco_initial_prefills_name_and_utc_window(self):
        user = get_user_model().objects.create_user(username='lco-initial-user', password='secret')
        target = Target.objects.create(
            name='Gaia26abc',
            type=Target.SIDEREAL,
            ra=12.3,
            dec=-45.6,
            epoch=2000.0,
        )
        fixed_now = datetime(2026, 6, 2, 7, 15, 30, tzinfo=timezone.utc)

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                if tz is None:
                    return fixed_now.replace(tzinfo=None)
                return fixed_now.astimezone(tz)

        request = RequestFactory().get(f'/observations/LCO/create/?target_id={target.pk}')
        request.user = user
        view = ProposalAwareObservationCreateView()
        view.request = request
        view.kwargs = {'facility': 'LCO'}

        with patch('custom_code.views.datetime', FixedDateTime):
            initial = view.get_initial()

        self.assertEqual(initial['name'], 'BHTOM Gaia26abc 20260602')
        self.assertEqual(initial['start'], fixed_now)
        self.assertEqual(initial['end'], fixed_now + timedelta(hours=24))


def _build_test_fits_bytes():
    handle = BytesIO()
    fits.PrimaryHDU(np.arange(16).reshape((4, 4))).writeto(handle)
    return handle.getvalue()


class Bhtom2FitsUploadTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username='fits-user', password='secret')
        self.target = Target.objects.create(
            name='Gaia26fits',
            type=Target.SIDEREAL,
            ra=1.23,
            dec=4.56,
            epoch=2000.0,
        )
        assign_perm('tom_targets.view_target', self.user, self.target)
        self.client.force_login(self.user)

    def test_supported_fits_extensions_and_gzip_normalization(self):
        self.assertTrue(is_supported_fits_filename('image.fits'))
        self.assertTrue(is_supported_fits_filename('image.fit'))
        self.assertTrue(is_supported_fits_filename('image.fts'))
        self.assertTrue(is_supported_fits_filename('image.fits.gz'))
        self.assertTrue(is_supported_fits_filename('image.fits.fz'))

        gz_payload = gzip.compress(_build_test_fits_bytes())
        normalized_file, metadata = normalize_fits_upload(
            SimpleUploadedFile('image.fits.gz', gz_payload, content_type='application/gzip')
        )

        self.assertEqual(normalized_file.name, 'image.fits')
        self.assertEqual(metadata['recognized_format'], 'fits.gz')
        self.assertEqual(metadata['decompression_method'], 'gzip')
        with fits.open(normalized_file) as hdul:
            self.assertEqual(hdul[0].data.shape, (4, 4))

    def test_fz_style_header_normalization_preserves_funpack_like_structure(self):
        primary = fits.Header()
        primary['ORIGIN'] = 'LCO'
        primary['TELESCOP'] = '1m0-04'

        header = fits.Header()
        header['XTENSION'] = 'IMAGE'
        header['BITPIX'] = 16
        header['NAXIS'] = 2
        header['NAXIS1'] = 4
        header['NAXIS2'] = 4
        header['PCOUNT'] = 0
        header['GCOUNT'] = 1
        header['ZIMAGE'] = True
        header['ZCMPTYPE'] = 'RICE_1'
        header['ZNAXIS'] = 2
        header['ZNAXIS1'] = 4
        header['ZNAXIS2'] = 4
        header['EXTNAME'] = 'SCI'
        header['FILTER'] = 'rp'
        header['DATE-OBS'] = '2026-06-03T01:02:03'

        payload = io.BytesIO()
        fits.HDUList([
            fits.PrimaryHDU(header=primary),
            fits.ImageHDU(data=np.ones((4, 4), dtype=np.int16), header=header),
            fits.BinTableHDU.from_columns([
                fits.Column(name='X', format='E', array=np.array([1.0, 2.0], dtype=np.float32)),
            ], name='CAT'),
            fits.ImageHDU(data=np.zeros((4, 4), dtype=np.int16), name='BPM'),
            fits.ImageHDU(data=np.full((4, 4), 3, dtype=np.int16), name='ERR'),
        ]).writeto(payload)

        normalized_file, metadata = normalize_fits_upload(
            SimpleUploadedFile('image.fits.fz', payload.getvalue(), content_type='application/fits')
        )

        self.assertEqual(normalized_file.name, 'image.fits')
        self.assertEqual(metadata['recognized_format'], 'fits.fz')
        with fits.open(normalized_file) as hdul:
            self.assertEqual(len(hdul), 4)
            self.assertEqual(hdul[0].name, 'SCI')
            self.assertEqual(hdul[1].name, 'CAT')
            self.assertEqual(hdul[2].name, 'BPM')
            self.assertEqual(hdul[3].name, 'ERR')
            self.assertEqual(hdul[0].header['ORIGIN'], 'LCO')
            self.assertEqual(hdul[0].header['TELESCOP'], '1m0-04')
            self.assertEqual(hdul[0].header['FILTER'], 'rp')
            self.assertEqual(hdul[0].header['DATE-OBS'], '2026-06-03T01:02:03')
            self.assertNotIn('XTENSION', hdul[0].header)
            self.assertNotIn('ZIMAGE', hdul[0].header)
            self.assertNotIn('ZNAXIS', hdul[0].header)
            self.assertNotIn('ZDITHER0', hdul[0].header)

    @patch('custom_code.bhtom2_uploads.shutil.which', return_value='/opt/homebrew/bin/funpack')
    @patch('custom_code.bhtom2_uploads.subprocess.run')
    def test_fz_normalization_reads_funpack_output_file(self, mock_run, _mock_which):
        output_payload = BytesIO()
        fits.HDUList([
            fits.PrimaryHDU(data=np.ones((4, 4), dtype=np.float32), header=fits.Header({'EXTNAME': 'SCI'})),
            fits.BinTableHDU.from_columns([
                fits.Column(name='X', format='E', array=np.array([1.0], dtype=np.float32)),
            ], name='CAT'),
            fits.ImageHDU(data=np.zeros((4, 4), dtype=np.int16), name='BPM'),
            fits.ImageHDU(data=np.full((4, 4), 3, dtype=np.int16), name='ERR'),
        ]).writeto(output_payload, checksum=True)

        def _mock_funpack(cmd, capture_output, check):
            self.assertEqual(cmd[1], '-O')
            with open(cmd[2], 'wb') as output_handle:
                output_handle.write(output_payload.getvalue())
            return Mock(stdout=b'', stderr=b'')

        mock_run.side_effect = _mock_funpack

        normalized_file, metadata = normalize_fits_upload(
            SimpleUploadedFile('image.fits.fz', b'fake-compressed-payload', content_type='application/fits')
        )

        self.assertEqual(metadata['decompression_method'], 'funpack')
        with fits.open(normalized_file) as hdul:
            self.assertEqual(len(hdul), 4)
            self.assertEqual(hdul[0].name, 'SCI')
            self.assertEqual(hdul[1].name, 'CAT')
            self.assertEqual(hdul[2].name, 'BPM')
            self.assertEqual(hdul[3].name, 'ERR')

    @patch('custom_code.bhtom2_uploads.shutil.which', return_value='/opt/homebrew/bin/funpack')
    @patch('custom_code.bhtom2_uploads.subprocess.run')
    def test_fz_normalization_for_bhtom2_upload_flattens_to_primary_image(self, mock_run, _mock_which):
        output_payload = BytesIO()
        fits.HDUList([
            fits.PrimaryHDU(),
            fits.ImageHDU(
                data=np.ones((4, 4), dtype=np.float32),
                header=fits.Header({'EXTNAME': 'SCI', 'FILTER': 'B', 'DATE-OBS': '2026-06-05T01:02:03'}),
            ),
            fits.BinTableHDU.from_columns([
                fits.Column(name='X', format='E', array=np.array([1.0], dtype=np.float32)),
            ], name='CAT'),
            fits.ImageHDU(data=np.zeros((4, 4), dtype=np.int16), name='BPM'),
            fits.ImageHDU(data=np.full((4, 4), 3, dtype=np.int16), name='ERR'),
        ]).writeto(output_payload, checksum=True)

        def _mock_funpack(cmd, capture_output, check):
            with open(cmd[2], 'wb') as output_handle:
                output_handle.write(output_payload.getvalue())
            return Mock(stdout=b'', stderr=b'')

        mock_run.side_effect = _mock_funpack

        normalized_file, metadata = normalize_fits_upload(
            SimpleUploadedFile('image.fits.fz', b'fake-compressed-payload', content_type='application/fits'),
            preserve_extensions=False,
        )

        self.assertEqual(metadata['decompression_method'], 'funpack')
        self.assertFalse(metadata['preserve_extensions'])
        with fits.open(normalized_file) as hdul:
            self.assertEqual(len(hdul), 1)
            self.assertEqual(hdul[0].data.shape, (4, 4))
            self.assertEqual(hdul[0].header['FILTER'], 'B')
            self.assertEqual(hdul[0].header['DATE-OBS'], '2026-06-05T01:02:03')
            self.assertNotIn('XTENSION', hdul[0].header)
            self.assertNotIn('EXTNAME', hdul[0].header)

    def test_plain_multi_extension_fits_for_bhtom2_upload_flattens_to_primary_image(self):
        payload = BytesIO()
        fits.HDUList([
            fits.PrimaryHDU(),
            fits.ImageHDU(
                data=np.ones((4, 4), dtype=np.float32),
                header=fits.Header({'EXTNAME': 'SCI', 'FILTER': 'V', 'DATE-OBS': '2026-06-05T02:03:04'}),
            ),
            fits.BinTableHDU.from_columns([
                fits.Column(name='X', format='E', array=np.array([1.0], dtype=np.float32)),
            ], name='CAT'),
            fits.ImageHDU(data=np.zeros((4, 4), dtype=np.int16), name='BPM'),
            fits.ImageHDU(data=np.full((4, 4), 3, dtype=np.int16), name='ERR'),
        ]).writeto(payload, checksum=True)

        normalized_file, metadata = normalize_fits_upload(
            SimpleUploadedFile('image.fits', payload.getvalue(), content_type='application/fits'),
            preserve_extensions=False,
        )

        self.assertEqual(metadata['recognized_format'], 'plain fits')
        self.assertEqual(metadata['decompression_method'], 'none')
        self.assertFalse(metadata['preserve_extensions'])
        with fits.open(normalized_file) as hdul:
            self.assertEqual(len(hdul), 1)
            self.assertEqual(hdul[0].data.shape, (4, 4))
            self.assertEqual(hdul[0].header['FILTER'], 'V')
            self.assertEqual(hdul[0].header['DATE-OBS'], '2026-06-05T02:03:04')
            self.assertNotIn('XTENSION', hdul[0].header)
            self.assertNotIn('EXTNAME', hdul[0].header)

    @patch('custom_code.bhtom2_uploads.requests.post')
    @patch('custom_code.views.run_data_processor', return_value=ReducedDatum.objects.none())
    @patch('custom_code.views.run_hook')
    def test_manage_data_fits_upload_forwards_to_bhtom2_and_saves_preferences(
        self,
        mock_run_hook,
        mock_run_data_processor,
        mock_post,
    ):
        mock_post.return_value = Mock(status_code=201, json=Mock(return_value={'ok': True}), text='created')

        response = self.client.post(
            reverse('dataproduct-upload'),
            data={
                'target': self.target.pk,
                'data_product_type': 'fits_file',
                'bhtom2_upload_token': 'token-123',
                'bhtom2_upload_oname': 'OBS-01',
                'bhtom2_upload_filter': 'GaiaSP/any',
                'referrer': reverse('targets:detail', kwargs={'pk': self.target.pk}),
                'files': SimpleUploadedFile('managed.fits.gz', gzip.compress(_build_test_fits_bytes())),
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        preference = UserBhtom2UploadPreference.objects.get(user=self.user)
        self.assertEqual(preference.token, 'token-123')
        self.assertEqual(preference.oname, 'OBS-01')
        self.assertEqual(preference.calibration_filter, 'GaiaSP/any')

        dataproduct = DataProduct.objects.get(target=self.target)
        self.assertTrue(has_successful_bhtom2_upload(dataproduct))
        self.assertEqual(mock_post.call_args.kwargs['data']['observatory'], 'OBS-01')
        upload_tuple = mock_post.call_args.kwargs['files']['file_0']
        self.assertEqual(upload_tuple[0], 'managed.fits')

    @patch('custom_code.bhtom2_uploads.requests.post')
    @patch('custom_code.views.run_hook')
    def test_observation_save_forwards_fits_using_saved_profile_preferences(self, mock_run_hook, mock_post):
        mock_post.return_value = Mock(status_code=201, json=Mock(return_value={'ok': True}), text='created')
        preference = UserBhtom2UploadPreference.objects.create(
            user=self.user,
            token='token-xyz',
            oname='OBS-77',
            calibration_filter='GaiaSP/any',
        )
        observation = ObservationRecord.objects.create(
            target=self.target,
            user=self.user,
            facility='LCO',
            parameters={},
            observation_id='obs-123',
            status='COMPLETED',
        )
        dataproduct = DataProduct.objects.create(
            target=self.target,
            observation_record=observation,
            product_id='product-1',
            data=SimpleUploadedFile('obs-product.fits', _build_test_fits_bytes()),
            data_product_type='',
        )

        with patch('custom_code.views.get_service_class') as mock_get_service_class:
            service = Mock()
            service.save_data_products.return_value = [dataproduct]
            mock_get_service_class.return_value = service
            response = self.client.post(
                reverse('observation-dataproduct-save', kwargs={'pk': observation.pk}),
                data={'facility': 'LCO', 'products': ['product-1']},
                follow=True,
            )

        self.assertEqual(response.status_code, 200)
        dataproduct.refresh_from_db()
        preference.refresh_from_db()
        self.assertEqual(dataproduct.data_product_type, 'fits_file')
        self.assertTrue(has_successful_bhtom2_upload(dataproduct))
        self.assertEqual(mock_post.call_args.kwargs['headers']['Authorization'], 'Token token-xyz')


class FRAMDataServiceTests(TestCase):
    def test_parse_mjd_photometry_skips_comments_and_invalid_rows(self):
        rows = _parse_mjd_photometry(
            '# MJD Mag Magerr Filter\n'
            '60000.125 15.2 0.03 R\n'
            'bad 15.4 0.04 V\n'
            '60001.5 15.1 0.02 I extra\n'
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['mjd'], 60000.125)
        self.assertEqual(rows[0]['filter'], 'R')
        self.assertEqual(rows[1]['filter'], 'I')

    @patch('custom_code.data_services.fram_dataservice.django_timezone.now')
    def test_build_query_parameters_uses_full_range_before_first_ingest(self, mock_now):
        mock_now.return_value = datetime(2026, 6, 16, 12, 0, tzinfo=timezone.utc)
        target = Target.objects.create(name='Gaia26abc', type='SIDEREAL', ra=12.3, dec=-45.6)

        parameters = FRAMDataService().build_query_parameters({
            'target_id': target.id,
            'target_name': target.name,
            'ra': target.ra,
            'dec': target.dec,
        })

        self.assertEqual(parameters['radius_arcsec'], 3.0)
        self.assertEqual(parameters['night1'], '19000101')
        self.assertEqual(parameters['night2'], '20260616')
        self.assertEqual(parameters['site'], 'all')
        self.assertEqual(parameters['ccd'], 'all')

    def test_scheduler_parameters_include_target_name_for_alias(self):
        target = Target.objects.create(name='Gaia26abc', type='SIDEREAL', ra=12.3, dec=-45.6)

        parameters = _build_query_parameters_for_service(target, 'FRAM', FRAMDataService())

        self.assertEqual(parameters['target_name'], 'Gaia26abc')
        self.assertEqual(parameters['radius_arcsec'], 3.0)

    @patch('custom_code.data_services.fram_dataservice.django_timezone.now')
    def test_build_query_parameters_uses_three_day_range_after_existing_ingest(self, mock_now):
        mock_now.return_value = datetime(2026, 6, 16, 12, 0, tzinfo=timezone.utc)
        target = Target.objects.create(name='Gaia26abc', type='SIDEREAL', ra=12.3, dec=-45.6)
        ReducedDatum.objects.create(
            target=target,
            data_type='photometry',
            source_name='FRAM',
            source_location='http://fram.fzu.cz/archive/photometry/mjd',
            timestamp=datetime(2026, 6, 12, 0, 0, tzinfo=timezone.utc),
            value={'filter': 'FRAM(R)', 'magnitude': 15.2, 'error': 0.03},
        )

        parameters = FRAMDataService().build_query_parameters({
            'target_id': target.id,
            'target_name': target.name,
            'ra': target.ra,
            'dec': target.dec,
        })

        self.assertEqual(parameters['night1'], '20260613')
        self.assertEqual(parameters['night2'], '20260616')

    @patch.object(FRAMDataService, 'query_service')
    def test_query_targets_adds_alias_only_when_photometry_exists(self, mock_query_service):
        mock_query_service.return_value = {
            'ra': 12.3,
            'dec': -45.6,
            'source_location': 'http://fram.fzu.cz/archive/photometry/lc?ra=12.3&dec=-45.6',
            'download_location': 'http://fram.fzu.cz/archive/photometry/mjd?ra=12.3&dec=-45.6',
            'photometry_rows': [{'mjd': 60000.0, 'magnitude': 15.2, 'error': 0.03, 'filter': 'R'}],
        }

        results = FRAMDataService().query_targets({
            'target_name': 'Gaia26abc',
            'ra': 12.3,
            'dec': -45.6,
            'include_photometry': True,
        })

        self.assertEqual(results[0]['aliases'][0]['name'], 'FRAM_Gaia26abc')
        self.assertEqual(results[0]['aliases'][0]['source_name'], 'FRAM')
        self.assertIn('/archive/photometry/lc?', results[0]['aliases'][0]['url'])
        self.assertIn('/archive/photometry/lc?', results[0]['source_location'])
        self.assertEqual(results[0]['reduced_datums']['photometry'][0]['value']['filter'], 'FRAM(R)')

        mock_query_service.return_value['photometry_rows'] = []
        self.assertEqual(FRAMDataService().query_targets({'ra': 12.3, 'dec': -45.6}), [])
