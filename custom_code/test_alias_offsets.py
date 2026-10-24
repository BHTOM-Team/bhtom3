from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from django.conf import settings
from django.template import Context, Engine
from django.test import SimpleTestCase, TestCase, override_settings
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.alias_astrometry import matched_position
from custom_code.models import TargetAliasInfo
from custom_code.tasks import _run_service_for_target
from custom_code.templatetags.custom_target_extras import _target_other_names


class AliasOffsetDetailTests(TestCase):
    def setUp(self):
        self.target = Target.objects.create(name='Offset reference', type='SIDEREAL', ra=0, dec=0)

    def _alias(self, name='Matched object', source='GaiaDR3', position=None):
        alias = TargetName.objects.create(target=self.target, name=name)
        return TargetAliasInfo.objects.create(
            target_name=alias, source_name=source, url='https://example.org/object',
            matched_position=position or {},
        )

    def _render(self, rows):
        # Exercise the actual Other Names markup without unrelated target widgets.
        source = (Path(settings.BASE_DIR) / 'templates/tom_targets/partials/target_data.html').read_text()
        markup = source[source.index('{% if target_other_names %}'):]
        return Engine().from_string(markup).render(Context({'target_other_names': rows}))

    def test_catalog_offset_and_error_are_shown_after_linked_name(self):
        self._alias(position=matched_position(1 / 3600, 0, 0.2, 0.4))
        rows = _target_other_names(self.target)
        self.assertAlmostEqual(rows[0]['offset_arcsec'], 1)
        self.assertAlmostEqual(rows[0]['offset_error_arcsec'], 0.2)
        rendered = self._render(rows)
        self.assertIn('[1.000 ± 0.200 arcsec]', rendered)
        self.assertIn('https://example.org/object', rendered)
        self.assertLess(rendered.index('Matched object</a>'), rendered.index('[1.000'))
        with self.assertNumQueries(0):
            self.assertIs(_target_other_names(self.target), rows)

    def test_zero_offset_and_small_error_are_visible(self):
        self._alias(position=matched_position(0, 0, 0.00003, 0.00004))
        rendered = self._render(_target_other_names(self.target))
        self.assertIn('[0.000000 ± 0.000050 arcsec]', rendered)

    def test_unknown_position_and_unknown_error_are_distinct(self):
        self._alias(name='No position')
        self._alias(name='No error', source='Simbad', position=matched_position(1 / 3600, 0))
        rendered = self._render(_target_other_names(self.target))
        self.assertIn('[offset unavailable]', rendered)
        self.assertIn('[1.000 arcsec; uncertainty unavailable]', rendered)

    def test_legacy_photometry_uses_position_scatter_and_deduplicates_bands(self):
        self._alias(source='Alerce')
        for day, ra in ((1, 1 / 3600), (2, 3 / 3600)):
            for band in ('g', 'r'):
                ReducedDatum.objects.create(
                    target=self.target, source_name='Alerce', data_type='photometry',
                    timestamp=datetime(2026, 1, day, tzinfo=timezone.utc),
                    value={'filter': band, 'magnitude': 18, 'origin_ra': ra, 'origin_dec': 0},
                )
        row = _target_other_names(self.target)[0]
        self.assertAlmostEqual(row['offset_arcsec'], 2)
        self.assertAlmostEqual(row['offset_error_arcsec'], 2**0.5)
        self.assertIn('scatter', row['offset_description'])

    def test_source_wide_fallback_is_not_assigned_to_multiple_objects(self):
        self._alias(name='Object 1', source='Alerce')
        self._alias(name='Object 2', source='Alerce')
        ReducedDatum.objects.create(
            target=self.target, source_name='Alerce', data_type='photometry',
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            value={'origin_ra': 1 / 3600, 'origin_dec': 0},
        )
        self.assertTrue(all(row['offset_arcsec'] is None for row in _target_other_names(self.target)))

    @override_settings(DATA_SERVICE_JOB_TIMEOUT=0)
    def test_ingestion_persists_alias_specific_match_and_keeps_it_on_refresh(self):
        class Service:
            info_url = 'https://example.org/object'

            def build_query_parameters(self, parameters):
                return parameters

            def query_targets(self, parameters):
                return [{
                    'ra': 0, 'dec': 0,  # Query coordinates must never become a match.
                    'matched_position': matched_position(2 / 3600, 0),
                    'aliases': [{'name': 'Catalog match', 'matched_position': matched_position(1 / 3600, 0, 0.2, 0.4)}],
                }]

        with patch('custom_code.data_services.service_utils.configure_data_service_timeouts'):
            _run_service_for_target(self.target, 'TestCatalog', Service)
        info = TargetAliasInfo.objects.get(target_name__name='Catalog match')
        self.assertAlmostEqual(info.matched_position['ra'], 1 / 3600)
        self.assertEqual(info.matched_position['ra_error_arcsec'], 0.2)
        with patch.object(Service, 'query_targets', return_value=[{'aliases': ['Catalog match']}]), patch(
            'custom_code.data_services.service_utils.configure_data_service_timeouts',
        ):
            _run_service_for_target(self.target, 'TestCatalog', Service)
        info.refresh_from_db()
        self.assertEqual(info.matched_position['ra_error_arcsec'], 0.2)


class AliasMatchServiceTests(SimpleTestCase):
    def test_allwise_retains_the_named_catalog_objects_position_and_covariance(self):
        from custom_code.data_services.wise_alias_utils import fetch_allwise_match
        response = Mock(text='intro\nnull|\n12.345 -45.678 0 0 00492280-4540408 0.2 0.4 -0.1 0.1 0.0\n')
        with patch('custom_code.data_services.wise_alias_utils.requests.get', return_value=response):
            match = fetch_allwise_match(12, -45, 6)
        self.assertEqual(match['name'], 'WISEA J00492280-4540408')
        self.assertEqual(match['matched_position']['ra'], 12.345)
        self.assertEqual(match['matched_position']['dec'], -45.678)
        self.assertAlmostEqual(match['matched_position']['ra_dec_corr'], -0.125)

    def test_gaia_converts_position_errors_from_mas(self):
        from custom_code.data_services.gaia_dr3_dataservice import GaiaDR3DataService
        service = GaiaDR3DataService()
        with patch.object(service, 'query_service', return_value={
            'source': {'source_id': '123', 'ra': 12, 'dec': 34, 'ra_error': 0.2, 'dec_error': 0.4, 'ra_dec_corr': 0.5},
        }):
            result = service.query_targets({})[0]
        self.assertEqual(result['matched_position'], matched_position(12, 34, 0.0002, 0.0004, 0.5))

    def test_simbad_retains_error_ellipse_for_the_actual_match(self):
        from astropy.table import Table
        from custom_code.data_services.simbad_dataservice import SimbadDataService
        service = object.__new__(SimbadDataService)
        table = Table(rows=[('Object', 12, 34, 400, 200, 0)],
                      names=('main_id', 'ra', 'dec', 'coo_err_maj', 'coo_err_min', 'coo_err_angle'))
        with patch.object(service, 'query_service', return_value=table):
            result = service.query_targets({'ra': 11, 'dec': 33})[0]
        self.assertEqual(result['matched_position']['ra'], 12)
        self.assertAlmostEqual(result['matched_position']['ra_error_arcsec'], 0.2)
        self.assertAlmostEqual(result['matched_position']['dec_error_arcsec'], 0.4)

    def test_twomass_uses_matched_coordinates_instead_of_query_center(self):
        from custom_code.data_services.twomass_dataservice import TwoMASSDataService
        response = Mock(text='intro\nnull|\n12.345 -45.678 0 0 12345678+1234567 13.1 0.02 12.7 0.03 12.4 0.04 0.1 0.0\n')
        with patch('custom_code.data_services.twomass_dataservice.requests.get', return_value=response):
            result = TwoMASSDataService().query_targets({'ra': 12.346, 'dec': -45.678})[0]
        self.assertEqual(result['matched_position'], matched_position(12.345, -45.678))
