from unittest.mock import Mock

from django.test import SimpleTestCase

from custom_code.data_services.aavso_dataservice import (
    AAVSO_API_URL,
    AAVSODataService,
    _REQUEST_HEADERS,
    _RETRYABLE_HTTP_STATUSES,
)
from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT


class AAVSODataServiceTests(SimpleTestCase):
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
            'JD@@@mag@@@uncert@@@band@@@starName@@@fainterThan\n'
            '2461294.5@@@14.2@@@0.1@@@V@@@Test Star@@@0\n'
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
        self.assertEqual(rows[0]['value'], {
            'filter': 'AAVSO(V)',
            'magnitude': 14.2,
            'error': 0.1,
        })

    def test_retryable_statuses_include_server_and_rate_limit_errors(self):
        self.assertTrue({405, 429, 500, 502, 503, 504}.issubset(_RETRYABLE_HTTP_STATUSES))
