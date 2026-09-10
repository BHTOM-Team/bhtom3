import json
from unittest.mock import Mock, patch

from django.test import SimpleTestCase
from tom_dataservices.data_services.tns import TNSDataService as BaseTNSDataService

from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT
from custom_code.data_services.tns_dataservice import TNSDataService


class TNSDataServiceTests(SimpleTestCase):
    def test_build_query_parameters_removes_sn_prefix(self):
        service = TNSDataService()

        parameters = service.build_query_parameters({
            'target_name': 'SN2026fvx',
            'ra': 183.741913,
            'dec': 63.787784,
            'radius_arcsec': 5.0,
        })
        request_data = json.loads(parameters['data'])

        self.assertEqual(request_data['name'], '2026fvx')
        self.assertEqual(request_data['radius'], 5.0)
        self.assertEqual(request_data['units'], 'arcsec')

    @patch('custom_code.data_services.tns_dataservice.requests.post')
    def test_query_service_has_http_timeout(self, post):
        response = Mock()
        response.json.return_value = {'data': []}
        post.return_value = response
        service = TNSDataService()

        self.assertEqual(service.query_service({'data': '{}'}, url='https://example.invalid'), [])

        post.assert_called_once_with(
            'https://example.invalid',
            data={'data': '{}'},
            headers=service.build_headers(),
            timeout=DATA_SERVICE_HTTP_TIMEOUT,
        )
        response.raise_for_status.assert_called_once_with()

    def test_query_targets_replaces_non_numeric_display_coordinates(self):
        service = TNSDataService()
        upstream_result = {
            'name_prefix': 'SN',
            'objname': '2026fvx',
            'ra': 'null',
            'dec': '',
            'radeg': '183.742210',
            'decdeg': '63.787890',
        }
        with patch.object(
            BaseTNSDataService,
            'query_targets',
            return_value=[upstream_result],
        ):
            result = service.query_targets({})[0]

        self.assertEqual(result['ra'], 183.74221)
        self.assertEqual(result['dec'], 63.78789)
