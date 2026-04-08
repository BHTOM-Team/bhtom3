from unittest.mock import Mock, patch

from astropy.time import Time
from datetime import timezone
from django.core.files.uploadedfile import SimpleUploadedFile
from django.contrib.auth import get_user_model
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
from custom_code.data_services.forms import SimbadQueryForm
from custom_code.forms import (
    BhtomNonSiderealTargetCreateForm,
    BhtomSiderealTargetCreateForm,
)
from custom_code.sun_separation import get_live_target_values
from custom_code.target_derivations import derive_sidereal_target_fields


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


class TargetCreateFormVisibilityTests(TestCase):
    def test_sidereal_create_form_hides_derived_and_plot_fields(self):
        form = BhtomSiderealTargetCreateForm()

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
