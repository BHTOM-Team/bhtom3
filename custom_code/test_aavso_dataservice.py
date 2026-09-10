from unittest.mock import Mock, patch

import requests
from django.test import SimpleTestCase
from django.urls import reverse

from custom_code.data_services.aavso_dataservice import (
    AAVSO_API_URL,
    AAVSODataService,
    _AAVSOIdentifierUnavailable,
    _REQUEST_HEADERS,
    _RETRYABLE_HTTP_STATUSES,
    _aavso_identifier_variants,
    _aavso_object_url,
    _ingest_photometry,
    _transient_discovery_from_jd,
)
from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT


class AAVSODataServiceTests(SimpleTestCase):
    def test_transient_query_starts_at_discovery_year(self):
        self.assertEqual(
            _transient_discovery_from_jd(['SN2026fvx']),
            2461041.5,
        )

    def test_transient_identifier_variants_include_aavso_canonical_spelling(self):
        self.assertEqual(
            _aavso_identifier_variants(['SN2026fvx']),
            ['SN2026fvx', 'SN 2026fvx', '2026fvx'],
        )

    def test_object_url_uses_human_vsx_detail_page_when_oid_is_known(self):
        self.assertEqual(
            _aavso_object_url('SN 2026fvx', '10875707'),
            'https://vsx.aavso.org/index.php?view=detail.top&oid=10875707',
        )

    def test_object_url_falls_back_to_object_specific_api_page(self):
        self.assertEqual(
            _aavso_object_url('SN 2026fvx'),
            'https://vsx.aavso.org/index.php?view=api.object&ident=SN+2026fvx',
        )

    def test_http_session_retries_transient_405_responses(self):
        service = AAVSODataService()

        session = service._get_http_session()
        adapter = session.get_adapter(AAVSO_API_URL)
        retry = adapter.max_retries

        self.assertEqual(retry.total, 3)
        self.assertIn(405, retry.status_forcelist)
        self.assertEqual(retry.allowed_methods, frozenset({'GET'}))
        self.assertFalse(retry.raise_on_status)
        self.assertEqual(session.headers['User-Agent'], _REQUEST_HEADERS['User-Agent'])

    def test_fetch_photometry_uses_persistent_session(self):
        response = Mock()
        response.text = (
            'JD@@@mag@@@uncert@@@band@@@by@@@comCode@@@compStar1@@@compStar2@@@charts@@@comment@@@'
            'transformed@@@airmass@@@val@@@cmag@@@kmag@@@starName@@@obsAffil@@@mtype@@@adsRef@@@'
            'digitizer@@@credit@@@obsID@@@fainterThan@@@obsType@@@software@@@obsName@@@obsCountry\n'
            '2461294.5@@@14.2@@@0.1@@@V@@@TEST@@@B@@@123@@@125@@@X123@@@Clear sky@@@1@@@1.2@@@Z@@@'
            '12.3@@@12.5@@@Test Star@@@AAVSO@@@STD@@@2024A&A...1A@@@Scanner@@@AAVSO@@@123456@@@0@@@'
            'CCD@@@AstroImageJ@@@Test, Observer@@@PL\n'
        )
        session = Mock()
        session.get.return_value = response
        service = AAVSODataService()
        service._aavso_http_session = session

        rows, star_name = service._fetch_photometry('Test Star', 2461294.0, 2461295.0)

        session.get.assert_called_once_with(
            AAVSO_API_URL,
            params={
                'view': 'api.delim',
                'ident': 'Test Star',
                'fromjd': 2461294.0,
                'delimiter': '@@@',
                'tojd': 2461295.0,
            },
            timeout=DATA_SERVICE_HTTP_TIMEOUT,
        )
        response.raise_for_status.assert_called_once_with()
        self.assertEqual(star_name, 'Test Star')
        value = rows[0]['value']
        self.assertEqual(value['filter'], 'AAVSO(V)')
        self.assertEqual(value['magnitude'], 14.2)
        self.assertEqual(value['error'], 0.1)
        self.assertEqual(value['observer'], 'Test, Observer')
        self.assertEqual(value['observer_name'], 'Test, Observer')
        self.assertEqual(value['observer_code'], 'TEST')
        self.assertEqual(value['observer_country'], 'PL')
        self.assertEqual(value['aavso_observation_id'], '123456')
        self.assertEqual(value['observation_type'], 'CCD')
        self.assertEqual(value['software'], 'AstroImageJ')
        self.assertEqual(value['comment'], 'Clear sky')
        self.assertEqual(value['airmass'], 1.2)
        self.assertIs(value['transformed'], True)

    def test_ingest_enriches_matching_legacy_row_instead_of_duplicating_it(self):
        service = AAVSODataService()
        rows, _star_name = service._parse_delim(
            'JD@@@mag@@@uncert@@@band@@@by@@@starName@@@obsID@@@fainterThan@@@obsName\n'
            '2461294.5@@@14.2@@@0.1@@@V@@@TEST@@@Test Star@@@123456@@@0@@@Test Observer\n'
        )
        existing = Mock(
            pk=7,
            timestamp=rows[0]['timestamp'],
            value={'filter': 'AAVSO(V)', 'magnitude': 14.2, 'error': 0.1},
            source_location='https://www.aavso.org/',
        )

        with patch(
            'custom_code.data_services.aavso_dataservice.ReducedDatum.objects.filter',
            return_value=[existing],
        ), patch(
            'custom_code.data_services.aavso_dataservice.ReducedDatum.objects.bulk_create'
        ) as bulk_create, patch(
            'custom_code.data_services.aavso_dataservice.ReducedDatum.objects.bulk_update'
        ) as bulk_update:
            added = _ingest_photometry(Mock(), rows)

        self.assertEqual(added, 0)
        bulk_create.assert_not_called()
        bulk_update.assert_called_once()
        self.assertEqual(existing.value['aavso_observation_id'], '123456')
        self.assertEqual(existing.value['observer_name'], 'Test Observer')

    def test_persistent_405_marks_only_that_identifier_unavailable_after_retries(self):
        response = Mock(status_code=405)
        response.raise_for_status.side_effect = requests.HTTPError(
            '405 Client Error', response=response
        )
        session = Mock()
        session.get.return_value = response
        service = AAVSODataService()
        service._aavso_http_session = session

        with self.assertRaises(_AAVSOIdentifierUnavailable):
            service._fetch_photometry('Unknown Alias', 2415020.0, 2415385.0)

    def test_query_service_skips_405_identifier_and_tries_next_alias(self):
        service = AAVSODataService()
        row = {'timestamp': Mock(), 'value': {'filter': 'AAVSO(V)', 'magnitude': 12.3}}
        with patch.object(
            service,
            '_fetch_photometry',
            side_effect=[_AAVSOIdentifierUnavailable('bad'), ([row], 'Good Star')],
        ) as fetch:
            result = service.query_service({
                'idents': ['Bad Alias', 'Good Star'],
                'fromjd': 2460000.0,
                'tojd': 2460001.0,
            })

        self.assertEqual(result['ident'], 'Good Star')
        self.assertEqual(result['rows'], [row])
        self.assertEqual(fetch.call_count, 2)

    def test_query_service_reports_405_when_every_identifier_is_rejected(self):
        service = AAVSODataService()
        response = Mock(status_code=405)
        unavailable = _AAVSOIdentifierUnavailable('rejected', response=response)
        with patch.object(service, '_fetch_photometry', side_effect=unavailable):
            with self.assertRaises(_AAVSOIdentifierUnavailable) as raised:
                service.query_service({
                    'idents': ['SN2026fvx', 'SN 2026fvx'],
                    'fromjd': 2461041.5,
                    'tojd': 2461294.5,
                })

        self.assertEqual(raised.exception.response.status_code, 405)

    def test_non_405_http_error_is_not_hidden(self):
        response = Mock(status_code=500)
        error = requests.HTTPError('500 Server Error', response=response)
        response.raise_for_status.side_effect = error
        session = Mock()
        session.get.return_value = response
        service = AAVSODataService()
        service._aavso_http_session = session

        with self.assertRaises(requests.HTTPError) as raised:
            service._fetch_photometry('Test Star', 2415020.0, 2415385.0)

        self.assertIs(raised.exception, error)

    def test_aavso_measurement_detail_url(self):
        self.assertEqual(reverse('aavso-measurement-detail', args=(42,)), '/dataproducts/aavso/42/')

    def test_forced_refresh_starts_at_oldest_stored_observation_for_metadata_backfill(self):
        service = AAVSODataService()
        with patch(
            'custom_code.data_services.aavso_dataservice.resolve_query_coordinates',
            return_value=('Test Star', None, None),
        ), patch.object(service, '_target_names', return_value=['Test Star']), patch.object(
            service, '_historical_from_jd', return_value=2450000.0
        ) as historical_from_jd, patch.object(
            service, '_incremental_from_jd', return_value=2460000.0
        ) as incremental_from_jd:
            parameters = service.build_query_parameters({
                'target_id': 7,
                'target_name': 'Test Star',
                'force': True,
            })

        self.assertEqual(parameters['fromjd'], 2450000.0)
        historical_from_jd.assert_called_once_with(7)
        incremental_from_jd.assert_not_called()

    def test_transient_name_skips_slow_vsx_coordinate_lookup(self):
        service = AAVSODataService()
        with patch(
            'custom_code.data_services.aavso_dataservice.resolve_query_coordinates',
            return_value=('SN2026fvx', 183.74221, 63.78789),
        ), patch.object(service, '_target_names', return_value=['SN2026fvx']), patch(
            'custom_code.data_services.aavso_dataservice._resolve_vsx_names'
        ) as resolve_vsx:
            parameters = service.build_query_parameters({'target_name': 'SN2026fvx'})

        resolve_vsx.assert_not_called()
        self.assertEqual(parameters['idents'], ['SN2026fvx', 'SN 2026fvx', '2026fvx'])

    def test_expired_deadline_before_first_aavso_request_reports_timeout(self):
        service = AAVSODataService()
        with patch('custom_code.data_services.aavso_dataservice.time.monotonic', return_value=10.0):
            with self.assertRaises(requests.Timeout):
                service._fetch_chunked('SN2026fvx', 2461000.0, 2461001.0, None, deadline_monotonic=9.0)

    def test_query_target_has_coordinates_and_direct_vsx_link(self):
        service = AAVSODataService()
        row = {'timestamp': Mock(), 'value': {'filter': 'AAVSO(V)', 'magnitude': 12.3}}
        with patch.object(
            service, '_fetch_chunked', return_value=(0, 'SN 2026fvx', [row], True)
        ) as fetch_chunked, patch.object(service, '_fetch_vsx_object', return_value={
            'name': 'SN 2026fvx',
            'oid': '10875707',
            'ra': 183.74221,
            'dec': 63.78789,
        }):
            results = service.query_targets({
                'idents': ['SN 2026fvx'],
                'ra': 183.741913,
                'dec': 63.787784,
                'fromjd': 2461290.0,
                'tojd': 2461291.0,
                'include_photometry': True,
            })

        self.assertEqual(results[0]['ra'], 183.741913)
        self.assertEqual(results[0]['dec'], 63.787784)
        self.assertIsNone(fetch_chunked.call_args.kwargs['deadline_monotonic'])
        self.assertEqual(
            results[0]['source_location'],
            'https://vsx.aavso.org/index.php?view=detail.top&oid=10875707',
        )

    def test_retryable_statuses_include_server_and_rate_limit_errors(self):
        self.assertTrue({405, 429, 500, 502, 503, 504}.issubset(_RETRYABLE_HTTP_STATUSES))
