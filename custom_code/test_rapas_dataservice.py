from datetime import datetime, timezone
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from custom_code.data_services.rapas_dataservice import (
    RAPASDataService,
    _fetch_records,
    _rapas_description,
    _sheet_measurements,
    _spreadsheet_id,
    _timestamp,
    _to_float,
    _to_mjd,
)


class RAPASDataServiceTests(SimpleTestCase):
    def setUp(self):
        self.record = {
            'name': 'SN 2026fvx',
            'sheet_name': 'SN 2026fvx',
            'year': 2026,
            'ra': 183.7419128,
            'dec': 63.787784,
            'metadata': {'nature': 'SN Ia', 'host_galaxy': 'NGC4205'},
            'measurements': [{
                'timestamp': datetime(2026, 3, 20, 22, tzinfo=timezone.utc),
                'value': {
                    'filter': 'RAPAS(G)',
                    'magnitude': 15.95,
                    'error': 0.14,
                    'measurement_id': '2026:SN 2026fvx:23:G',
                },
            }],
        }

    def test_google_sheet_id_and_localized_numbers(self):
        url = 'https://docs.google.com/spreadsheets/d/abc_123/edit?gid=42'
        self.assertEqual(_spreadsheet_id(url), 'abc_123')
        self.assertEqual(_to_float('61 184,94'), 61184.94)
        self.assertEqual(_to_float('0'), 0.0)

    def test_mjd_accepts_decimal_and_thousands_separator_variants(self):
        for value in ('61184.94', '61184,94', '61,184.94', '61.184,94', '61 184,94'):
            self.assertEqual(_to_mjd(value), 61184.94)
        self.assertEqual(_to_mjd('61,184'), 61184.0)

    def test_timestamp_uses_only_mjd_not_the_timezone_ambiguous_date_columns(self):
        timestamp, mjd = _timestamp(['01/01/1999', '01:02:03', '61,184.5'])
        self.assertEqual(mjd, 61184.5)
        self.assertEqual(timestamp, datetime(2026, 5, 24, 12, 0, tzinfo=timezone.utc))
        self.assertEqual(_timestamp(['20/03/2026', '22:00:00', '']), (None, None))

    def test_combined_query_uses_cached_workbook_without_network(self):
        backend = Mock()
        backend.get.side_effect = lambda key: (
            [self.record] if not key.endswith('-refreshed-at') else None
        )
        with patch(
            'custom_code.data_services.rapas_dataservice._configured_spreadsheets',
            return_value=[{'year': 2026, 'url': 'https://docs.google.com/spreadsheets/d/abc/edit'}],
        ), patch(
            'custom_code.data_services.rapas_dataservice._rapas_cache_backend',
            return_value=backend,
        ), patch(
            'custom_code.data_services.rapas_dataservice.requests.get'
        ) as request:
            records = _fetch_records(cache_only=True)

        self.assertEqual(records, [self.record])
        request.assert_not_called()

    def test_first_use_caches_pre_2026_workbook_without_expiry(self):
        backend = Mock()
        backend.get.return_value = None
        backend.add.return_value = True
        response = Mock(content=b'older workbook')
        response.raise_for_status.return_value = None
        with patch(
            'custom_code.data_services.rapas_dataservice._configured_spreadsheets',
            return_value=[{
                'label': 'pre-2026',
                'year': None,
                'url': 'https://docs.google.com/spreadsheets/d/older/edit',
            }],
        ), patch(
            'custom_code.data_services.rapas_dataservice._rapas_cache_backend',
            return_value=backend,
        ), patch(
            'custom_code.data_services.rapas_dataservice.cache',
            backend,
        ), patch(
            'custom_code.data_services.rapas_dataservice.requests.get',
            return_value=response,
        ), patch(
            'custom_code.data_services.rapas_dataservice.parse_rapas_workbook',
            return_value=[self.record],
        ) as parse_workbook:
            records = _fetch_records(cache_only=False)

        self.assertEqual(records, [self.record])
        parse_workbook.assert_called_once_with(
            b'older workbook',
            None,
            source_label='pre-2026',
        )
        self.assertTrue(any(
            cache_call.args[1] == [self.record] and cache_call.kwargs == {'timeout': None}
            for cache_call in backend.set.call_args_list
        ))

    def test_exact_name_match_returns_private_alias_and_labeled_photometry(self):
        service = RAPASDataService()
        parameters = service.build_query_parameters({
            'target_name': 'Different primary name',
            'target_names': ['Different primary name', 'SN 2026fvx'],
            'ra': self.record['ra'],
            'dec': self.record['dec'],
            'include_photometry': True,
        })
        with patch('custom_code.data_services.rapas_dataservice._fetch_records', return_value=[self.record]):
            results = service.query_targets(parameters)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['aliases'], [{'name': 'SN 2026fvx', 'source_name': 'RAPAS'}])
        self.assertEqual(results[0]['source_location'], '')
        value = results[0]['reduced_datums']['photometry'][0]['value']
        self.assertEqual(value['filter'], 'RAPAS(G)')
        self.assertEqual(value['nature'], 'SN Ia')
        self.assertNotIn('url', results[0]['aliases'][0])
        self.assertEqual(results[0]['description'], 'RAPAS target, nature SN Ia, host galaxy NGC4205')

        target = service.create_target_from_query(results[0])
        self.assertEqual(target.ra, self.record['ra'])
        self.assertEqual(target.dec, self.record['dec'])
        self.assertEqual(target.epoch, 2000.0)
        self.assertEqual(target.description, 'RAPAS target, nature SN Ia, host galaxy NGC4205')

    def test_rapas_coordinates_fall_back_to_measurement_columns(self):
        record = dict(self.record, ra=None, dec=None)
        record['measurements'] = [{
            'timestamp': datetime(2026, 3, 20, 22, tzinfo=timezone.utc),
            'value': {
                'filter': 'RAPAS(G)',
                'magnitude': 15.95,
                'ra': self.record['ra'],
                'dec': self.record['dec'],
            },
        }]
        service = RAPASDataService()
        parameters = service.build_query_parameters({
            'target_name': 'SN2026fvx',
            'target_names': ['SN2026fvx'],
        })
        with patch('custom_code.data_services.rapas_dataservice._fetch_records', return_value=[record]):
            result = service.query_targets(parameters)[0]

        self.assertEqual(result['ra'], self.record['ra'])
        self.assertEqual(result['dec'], self.record['dec'])

    def test_rapas_description_includes_available_metadata_and_respects_model_limit(self):
        description = _rapas_description({
            'nature': 'SN Ia',
            'redshift': 0.0123,
            'host_galaxy': 'NGC4205',
            'discovery_magnitude': 16.2,
            'alert_date': '17/03/2026',
            'rapas_status': 'active',
            'alert_comment': 'A' * 300,
        })

        self.assertTrue(description.startswith('RAPAS target, nature SN Ia, redshift 0.0123'))
        self.assertLessEqual(len(description), 200)

    def test_coordinate_match_is_used_when_names_do_not_match(self):
        service = RAPASDataService()
        parameters = service.build_query_parameters({
            'target_name': 'Unrelated target name',
            'target_names': ['Unrelated target name'],
            'ra': self.record['ra'] + 0.0001,
            'dec': self.record['dec'],
            'radius_arcsec': 5.0,
        })
        with patch('custom_code.data_services.rapas_dataservice._fetch_records', return_value=[self.record]):
            results = service.query_targets(parameters)
        self.assertEqual([result['name'] for result in results], ['SN 2026fvx'])

    def test_general_query_accepts_compact_and_partial_names(self):
        second_record = dict(self.record, name='AT 2026abc', sheet_name='AT 2026abc')
        records = [self.record, second_record]

        for query in ('SN2026fvx', '2026fvx', '26fvx', 'fvx'):
            service = RAPASDataService()
            parameters = service.build_query_parameters({
                'target_name': query,
                'target_names': [query],
                'ra': 0.0,
                'dec': 0.0,
            })
            with patch('custom_code.data_services.rapas_dataservice._fetch_records', return_value=records):
                results = service.query_targets(parameters)
            self.assertEqual([result['name'] for result in results], ['SN 2026fvx'])

        service = RAPASDataService()
        parameters = service.build_query_parameters({
            'target_name': '2026',
            'target_names': ['2026'],
            'ra': 0.0,
            'dec': 0.0,
        })
        with patch('custom_code.data_services.rapas_dataservice._fetch_records', return_value=records):
            results = service.query_targets(parameters)
        self.assertEqual([result['name'] for result in results], ['SN 2026fvx', 'AT 2026abc'])

    def test_scheduled_target_refresh_does_not_use_broad_partial_matching(self):
        service = RAPASDataService()
        parameters = service.build_query_parameters({
            'target_id': 123,
            'target_name': '2026',
            'target_names': ['2026'],
            'ra': 0.0,
            'dec': 0.0,
        })
        with patch('custom_code.data_services.rapas_dataservice._fetch_records', return_value=[self.record]):
            results = service.query_targets(parameters)
        self.assertEqual(results, [])

    def test_scheduled_target_refresh_ignores_even_an_exact_name(self):
        service = RAPASDataService()
        parameters = service.build_query_parameters({
            'target_id': 123,
            'target_name': 'SN2026fvx',
            'target_names': ['SN2026fvx'],
            'ra': 0.0,
            'dec': 0.0,
        })
        with patch('custom_code.data_services.rapas_dataservice._fetch_records', return_value=[self.record]):
            results = service.query_targets(parameters)
        self.assertEqual(results, [])

    def test_scheduled_target_refresh_matches_by_coordinates_and_returns_rapas_name(self):
        service = RAPASDataService()
        parameters = service.build_query_parameters({
            'target_id': 123,
            'target_name': 'Completely different BHTOM name',
            'target_names': ['Completely different BHTOM name'],
            'ra': self.record['ra'] + 0.0001,
            'dec': self.record['dec'],
            'radius_arcsec': 5.0,
        })
        with patch('custom_code.data_services.rapas_dataservice._fetch_records', return_value=[self.record]):
            results = service.query_targets(parameters)
        self.assertEqual([result['name'] for result in results], ['SN 2026fvx'])
        self.assertEqual(results[0]['aliases'], [{'name': 'SN 2026fvx', 'source_name': 'RAPAS'}])

    def test_measurement_rows_create_all_three_rapas_bands(self):
        rows = [[], ['Filtre A / G', '', '', '', '', 'Filtre A / G', '', 'Filtre B / Gbp', '', 'Filtre C / Grp']]
        rows.append(['Date(JJ/MM/AAAA)', 'UTC(HH:MM:SS)', 'MJD'])
        rows.append([
            '20/03/2026', '22:00:00', '61 119.91667', 183.74, 63.79,
            15.95, 0.14, 16.19, 0.07, 15.85, 0.17, 0.34, 19.0,
            'Jean-Louis Dumont', 'PrismV11', '', '', '',
        ])
        measurements = _sheet_measurements(rows, {}, 2026, 'SN 2026fvx', 'SN 2026fvx')
        self.assertEqual(
            [measurement['value']['filter'] for measurement in measurements],
            ['RAPAS(G)', 'RAPAS(GBP)', 'RAPAS(GRP)'],
        )
        self.assertEqual(measurements[0]['value']['observer'], 'Jean-Louis Dumont')
        self.assertEqual(measurements[0]['value']['upper_limit_g'], 19.0)

    def test_pre_2026_measurement_uses_observation_year_and_source_namespace(self):
        rows = [[], ['Filtre A / G', '', '', '', '', 'Filtre A / G']]
        rows.append(['Date(JJ/MM/AAAA)', 'UTC(HH:MM:SS)', 'MJD'])
        rows.append(['not authoritative', 'unknown timezone', '60,500.25', 1.0, 2.0, 17.3, 0.1])

        measurements = _sheet_measurements(
            rows,
            {},
            None,
            'SN2024example',
            'SN2024example',
            source_label='pre-2026',
        )

        value = measurements[0]['value']
        self.assertEqual(value['mjd'], 60500.25)
        self.assertEqual(value['rapas_year'], 2024)
        self.assertTrue(value['measurement_id'].startswith('pre-2026:SN2024example:60500.25000000:G:'))
