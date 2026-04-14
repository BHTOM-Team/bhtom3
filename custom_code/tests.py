import json
from unittest.mock import Mock, patch

from astropy.time import Time
from datetime import timezone
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.cache import cache
from django.contrib.auth import get_user_model
from django.test.client import RequestFactory
from django.test import TestCase, override_settings
from django.urls import reverse
from tom_targets.models import Target

from custom_code.data_services.ogle_ews_dataservice import (
    OGLEEWSDataService,
    _dec_to_decimal,
    _normalize_target_name,
    _ogle_phot_url,
    _parse_lenses_rows,
    _parse_photometry_rows,
    _ra_to_decimal,
)
from custom_code.data_services.exoclock_dataservice import ExoClockDataService
from custom_code.data_services.gaia_dr3_dataservice import GaiaDR3DataService
from custom_code.bhtom_catalogs.harvesters.simbad import target_from_result
from custom_code.bhtom_catalogs.harvesters.crts import CRTSHarvester
from custom_code.bhtom_catalogs.harvesters.gaia_alerts import GaiaAlertsHarvester
from custom_code.bhtom_catalogs.harvesters.gaia_dr3 import GaiaDR3Harvester
from custom_code.bhtom_catalogs.harvesters.exoclock import ExoClockHarvester
from custom_code.bhtom_catalogs.harvesters.lsst import LSSTHarvester
from custom_code.data_services.forms import SimbadQueryForm
from custom_code.forms import (
    BhtomNonSiderealTargetCreateForm,
    BhtomPlanetaryTransitTargetCreateForm,
    BhtomPlanetaryTransitTargetUpdateForm,
    BhtomSiderealTargetCreateForm,
    BhtomSiderealTargetUpdateForm,
    BhtomTargetNamesFormset,
)
from custom_code.models import TransitEphemeris
from custom_code.signals import cleanup_target_relations_on_target_delete
from custom_code.templatetags.custom_target_extras import bhtom_target_data
from custom_code.templatetags.custom_target_extras import truncate_decimals
from custom_code.tasks import _build_query_parameters_for_service, _run_service_for_target
from custom_code.sun_separation import get_live_target_values
from custom_code.target_derivations import derive_sidereal_target_fields
from custom_code.views import (
    BhtomCatalogQueryView,
    BhtomCreateTargetFromQueryView,
    BhtomTargetCreateView,
    BhtomTargetUpdateView,
    EXOCLOCK_RECOMMENDED_OBSERVING_STRATEGY,
)


@override_settings(
    BHTOM2_API_BASE_URL='https://bh-tom2.example',
    BHTOM2_API_TOKEN='secret-api-token',
    BHTOM2_UPLOAD_SERVICE_URL='https://uploadsvc.example',
)
class PublicUploadViewTests(TestCase):
    def test_target_search_filters_results(self):
        with patch('custom_code.views._public_upload_target_choices', return_value=[
            {'label': 'Gaia24abc', 'value': 'Gaia24abc', 'search': 'gaia24abc'},
            {'label': 'AT2026xyz', 'value': 'AT2026xyz', 'search': 'at2026xyz'},
        ]):
            response = self.client.get(reverse('public-upload-targets'), {'q': 'gaia'})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {'results': [{'label': 'Gaia24abc', 'value': 'Gaia24abc', 'search': 'gaia24abc'}]},
        )

    def test_public_upload_posts_selected_single_fits_file(self):
        upload_response = Mock(status_code=201)

        with patch('custom_code.views._public_upload_target_choices', return_value=[
            {'label': 'Gaia24abc', 'value': 'Gaia24abc', 'search': 'gaia24abc'},
        ]), patch('custom_code.views._public_upload_observer_choices', return_value=[
            {'label': 'Jane Doe (jdoe)', 'value': 'jdoe', 'search': 'jdoe jane doe'},
        ]), patch('custom_code.views._public_upload_observatory_choices', return_value=[
            {'label': 'OGLE Warsaw (OGLE)', 'value': 'OGLE', 'search': 'ogle warsaw'},
        ]), patch('custom_code.views.requests.post', return_value=upload_response) as mocked_post:
            response = self.client.post(
                reverse('public-upload'),
                data={
                    'target': 'Gaia24abc',
                    'observer': 'jdoe',
                    'token': 'user-upload-token',
                    'observatory': 'OGLE',
                    'calibration_filter': 'GaiaSP/any',
                    'comment': 'test upload',
                    'fits_file': SimpleUploadedFile('example.fits', b'SIMPLE  = T', content_type='application/fits'),
                },
                follow=True,
            )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'FITS upload sent to BHTOM2 for target Gaia24abc.')

        self.assertEqual(mocked_post.call_count, 1)
        _, kwargs = mocked_post.call_args
        self.assertEqual(kwargs['data']['target'], 'Gaia24abc')
        self.assertEqual(kwargs['data']['observatory'], 'OGLE')
        self.assertEqual(kwargs['data']['observers'], 'jdoe')
        self.assertEqual(kwargs['data']['filter'], 'GaiaSP/any')
        self.assertEqual(kwargs['data']['comment'], 'test upload')
        self.assertEqual(kwargs['headers']['Authorization'], 'Token user-upload-token')
        self.assertIn('file_0', kwargs['files'])

    def test_public_upload_rejects_free_text_values_not_from_reference_lists(self):
        with patch('custom_code.views._public_upload_target_choices', return_value=[]), patch(
            'custom_code.views._public_upload_observer_choices',
            return_value=[],
        ), patch('custom_code.views._public_upload_observatory_choices', return_value=[]):
            response = self.client.post(
                reverse('public-upload'),
                data={
                    'target': 'Unknown target',
                    'observer': 'unknown',
                    'token': 'user-upload-token',
                    'observatory': 'unknown',
                    'calibration_filter': 'GaiaSP/any',
                    'comment': '',
                    'fits_file': SimpleUploadedFile('example.fits', b'SIMPLE  = T', content_type='application/fits'),
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Select a target from the BHTOM2 list.')
        self.assertContains(response, 'Select an observer from the BHTOM2 list.')
        self.assertContains(response, 'Select an observatory from the BHTOM2 list.')


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
        self.assertEqual(results[0]['name'], '2011-BLG-0001')
        self.assertEqual(results[0]['aliases'], ['2011-BLG-0001'])
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
            _parse_photometry_rows('2455260.85336 17.131 0.015 5.94 1033.0\n')[0],
            {'hjd': 2455260.85336, 'mag': 17.131, 'magerr': 0.015},
        )


class ExoClockDataServiceTests(TestCase):
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

    def test_build_query_parameters_for_exoclock_uses_cone_search_radius(self):
        target = Target.objects.create(name='Gaia DR3 123', type=Target.SIDEREAL, ra=97.63665, dec=29.672296, epoch=2000.0)

        params = _build_query_parameters_for_service(target, 'ExoClock', ExoClockDataService())

        self.assertEqual(params['target_name'], 'Gaia DR3 123')
        self.assertEqual(params['ra'], 97.63665)
        self.assertEqual(params['dec'], 29.672296)
        self.assertEqual(params['radius_arcsec'], 30.0)

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

    def test_coordinate_form_rejects_invalid_value(self):
        form = SimbadQueryForm(data={'ra': 'not-a-coordinate', 'dec': '-30:27:08.4'})

        self.assertFalse(form.is_valid())
        self.assertIn('ra', form.errors)


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

        self.assertEqual(crts.to_target().epoch, 2000.0)
        self.assertEqual(gaia_alerts.to_target().epoch, 2000.0)
        self.assertEqual(gaia_dr3.to_target().epoch, 2000.0)
        self.assertEqual(lsst.to_target().epoch, 2000.0)

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


class TargetCreateFormVisibilityTests(TestCase):
    def test_sidereal_create_form_hides_derived_and_plot_fields(self):
        form = BhtomSiderealTargetCreateForm()

        self.assertIn('classification', form.fields)
        self.assertIn('parallax', form.fields)
        self.assertIn('parallax_error', form.fields)
        self.assertIn('pm_ra_error', form.fields)
        self.assertIn('pm_dec_error', form.fields)
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

    def test_sidereal_update_form_includes_transit_ephemeris_fields(self):
        target = Target.objects.create(name='WASP-12b', type=Target.SIDEREAL, ra=1.0, dec=2.0, epoch=2000.0)
        target.parallax_error = 0.33
        target.save(update_fields=['parallax_error'])
        form = BhtomSiderealTargetUpdateForm(instance=target)

        self.assertIn('classification', form.fields)
        self.assertIn('parallax', form.fields)
        self.assertIn('source_name', form.fields)
        self.assertIn('planet_name', form.fields)
        self.assertIn('priority', form.fields)
        self.assertEqual(float(form['parallax_error'].value()), 0.33)

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
        )
        target.parallax_error = 0.33
        target.pm_ra_error = 0.11
        target.pm_dec_error = 0.22
        target.save(update_fields=['parallax_error', 'pm_ra_error', 'pm_dec_error'])

        context = bhtom_target_data(target)

        self.assertEqual(context['astrometry_rows'][0]['label'], 'Parallax (mas)')
        self.assertEqual(context['astrometry_rows'][0]['value'], 1.2)
        self.assertEqual(context['astrometry_rows'][0]['error'], 0.33)
        self.assertEqual(context['astrometry_rows'][1]['error'], 0.11)
        self.assertEqual(context['astrometry_rows'][2]['error'], 0.22)

    def test_target_data_omits_gaia_astrometry_block_when_all_values_missing(self):
        target = Target.objects.create(
            name='GaiaDR3_456',
            type=Target.SIDEREAL,
            ra=12.3,
            dec=-45.6,
            epoch=2000.0,
        )

        context = bhtom_target_data(target)

        self.assertEqual(context['astrometry_rows'], [])


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
                    pm_ra=4.5,
                    pm_dec=-6.7,
                    parallax=1.2,
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
