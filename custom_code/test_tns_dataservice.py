import json
from unittest.mock import Mock, patch

from django.test import SimpleTestCase
from tom_dataservices.data_services.tns import TNSDataService as BaseTNSDataService

from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT
from custom_code.data_services.tns_dataservice import TNSDataService, _parse_tns_photometry


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

    def test_query_targets_normalizes_metadata_and_photometry(self):
        service = TNSDataService()
        upstream_result = {
            'name_prefix': 'SN',
            'objname': '2026fvx',
            'ra': 'null',
            'dec': '',
            'radeg': '183.742210',
            'decdeg': '63.787890',
            'object_type': {'name': 'SN Ia'},
            'redshift': '0.01234',
            'discoverydate': '2026-03-17 19:41:12',
            'photometry': [
                {
                    'id': 17,
                    'jd': 2461293.5,
                    'flux': 18.42,
                    'fluxerr': 0.08,
                    'flux_unit': {'name': 'mag'},
                    'filters': {'name': 'L-GOTO'},
                    'source_group': {'group_name': 'GOTO', 'name': 'GOTO'},
                    'observer': 'Example Observer',
                },
            ],
        }
        with patch.object(
            BaseTNSDataService,
            'query_targets',
            return_value=[upstream_result],
        ):
            result = service.query_targets({})[0]

        self.assertEqual(result['ra'], 183.74221)
        self.assertEqual(result['dec'], 63.78789)
        self.assertEqual(result['classification'], 'SN Ia')
        self.assertEqual(result['source_location'], 'https://www.wis-tns.org/object/2026fvx')
        datum = result['reduced_datums']['photometry'][0]['value']
        self.assertEqual(datum['filter'], 'TNS(GOTO-L)')
        self.assertEqual(datum['magnitude'], 18.42)
        self.assertEqual(datum['observer'], 'TNS')
        self.assertEqual(datum['facility'], 'TNS')
        self.assertEqual(datum['tns_observer'], 'Example Observer')

        target = service.create_target_from_query(result)
        self.assertEqual(target.epoch, 2000.0)
        self.assertEqual(target.description, 'TNS target, classification SN Ia, redshift 0.01234.')
        self.assertEqual(target.redshift, 0.01234)
        self.assertEqual(target.importance, 9.99)
        self.assertEqual(target.cadence, 1.0)
        self.assertEqual(target.discovery_date.isoformat(), '2026-03-17T19:41:12+00:00')

    def test_create_target_uses_unknown_classification_and_omits_unknown_redshift(self):
        service = TNSDataService()

        target = service.create_target_from_query({
            'name': 'AT 2026abc',
            'ra': 12.3,
            'dec': -45.6,
            'redshift': None,
        })

        self.assertEqual(target.description, 'TNS target, classification unknown.')
        self.assertIsNone(target.discovery_date)

    def test_parse_tns_photometry_normalizes_survey_filters_and_limits(self):
        rows = _parse_tns_photometry({
            'photometry': [
                {'jd': 2461293.5, 'flux': 18.4, 'flux_unit': {'name': 'mag'},
                 'filters': {'name': 'g-Sloan'}, 'source_group': {'name': 'ASAS-SN'}},
                {'jd': 2461294.5, 'flux': 19.1, 'flux_unit': {'name': 'mag'},
                 'filters': {'name': 'cyan-ATLAS'}, 'source_group': {'name': 'ATLAS'}},
                {'jd': 2461295.5, 'limflux': 20.2, 'flux_unit': {'name': 'mag'},
                 'filters': {'name': 'r-ZTF'}, 'source_group': {'name': 'ZTF'}},
            ],
        })

        self.assertEqual(
            [row['value']['filter'] for row in rows],
            ['TNS(ASASSN-g)', 'TNS(ATLAS-c)', 'TNS(ZTF-r)'],
        )
        self.assertEqual(rows[-1]['value']['limit'], 20.2)
        self.assertTrue(rows[-1]['value']['upper_limit'])

    def test_parse_tns_photometry_does_not_repeat_an_unknown_survey_name(self):
        rows = _parse_tns_photometry({
            'photometry': [{
                'jd': 2461293.5,
                'flux': 16.0,
                'flux_unit': {'name': 'mag'},
                'filters': {'name': 'R-Cousins'},
            }],
        })

        self.assertEqual(rows[0]['value']['filter'], 'TNS(RCousins)')

    @patch('custom_code.data_services.tns_dataservice.requests.post')
    def test_object_request_enables_photometry(self, post):
        response = Mock()
        response.json.return_value = {'data': {}}
        post.return_value = response
        service = TNSDataService()

        service.query_service(
            {'api_key': 'secret', 'data': json.dumps({'objname': '2026fvx', 'photometry': '0'})},
            url='https://www.wis-tns.org/api/get/object',
        )

        sent = json.loads(post.call_args.kwargs['data']['data'])
        self.assertEqual(sent['photometry'], '1')
        self.assertEqual(sent['spectra'], '0')
