from unittest.mock import Mock, call, patch

from django.test import SimpleTestCase

from custom_code.bhtom_catalogs.harvesters.lsst import get
from custom_code.data_services.lsst_dataservice import LSSTDataService


class LSSTDataServiceTests(SimpleTestCase):
    def test_empty_dia_object_query_does_not_retry_with_invalid_object_id_field(self):
        service = LSSTDataService()

        with patch.object(service, '_post', return_value=[]) as post:
            results = service.query_service({
                'dia_object_id': '396895411240977',
                'ra': None,
                'dec': None,
                'include_photometry': False,
            })

        self.assertEqual(results, {'objects': [], 'sources': []})
        post.assert_called_once_with(
            '/api/v1/objects',
            {'diaObjectId': '396895411240977', 'output-format': 'json'},
        )

    def test_object_and_source_queries_only_use_dia_object_id(self):
        service = LSSTDataService()

        with patch.object(service, '_post', return_value=[]) as post:
            service.query_service({
                'dia_object_id': '396895411240977',
                'ra': None,
                'dec': None,
                'include_photometry': True,
            })

        self.assertEqual(post.call_count, 2)
        for request_call in post.call_args_list:
            payload = request_call.args[1]
            self.assertEqual(payload['diaObjectId'], '396895411240977')
            self.assertNotIn('objectId', payload)


class LSSTHarvesterTests(SimpleTestCase):
    @patch('custom_code.bhtom_catalogs.harvesters.lsst.requests.post')
    def test_empty_query_does_not_fall_back_to_invalid_object_id_field(self, post):
        response = Mock()
        response.json.return_value = []
        post.return_value = response

        self.assertEqual(get('396895411240977'), {})

        self.assertEqual(post.call_count, 2)
        self.assertEqual(
            [request_call.kwargs['json'] for request_call in post.call_args_list],
            [
                {'diaObjectId': '396895411240977', 'output-format': 'json'},
                {'diaObjectId': '396895411240977', 'output-format': 'json'},
            ],
        )
        response.raise_for_status.assert_has_calls([call(), call()])
