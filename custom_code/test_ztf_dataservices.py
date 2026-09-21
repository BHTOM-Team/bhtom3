from unittest.mock import patch

import pandas as pd
from django.test import SimpleTestCase

from custom_code.data_services.alerce_dataservice import (
    AlerceDataService,
    _alerce_object_url,
)
from custom_code.data_services.ztf_dataservice import (
    ZTFDataService,
    _ztf_object_url,
)


class ZTFDataServiceAliasTests(SimpleTestCase):
    def test_query_targets_returns_each_data_release_object_as_linked_alias(self):
        service = ZTFDataService()
        lightcurve = pd.DataFrame([
            {'oid': 686103400067717, 'mjd': 58204.5, 'mag': 17.7, 'magerr': 0.03, 'filtercode': 'zg'},
            {'oid': 686103400067717, 'mjd': 58205.5, 'mag': 17.6, 'magerr': 0.03, 'filtercode': 'zg'},
            {'oid': 686103400067718, 'mjd': 58206.5, 'mag': 17.8, 'magerr': 0.04, 'filtercode': 'zr'},
        ])

        with patch.object(service, 'query_service', return_value={
            'ra': 298.0,
            'dec': 29.8,
            'lc_data': lightcurve,
            'source_location': 'https://irsa.example/query',
        }):
            results = service.query_targets({'ra': 298.0, 'dec': 29.8})

        self.assertEqual(results[0]['aliases'], [
            {
                'name': '686103400067717',
                'url': _ztf_object_url('686103400067717'),
                'source_name': 'ZTF Data Release',
            },
            {
                'name': '686103400067718',
                'url': _ztf_object_url('686103400067718'),
                'source_name': 'ZTF Data Release',
            },
        ])


class AlerceDataServiceAliasTests(SimpleTestCase):
    def test_query_service_and_targets_return_linked_alerce_object_alias(self):
        service = AlerceDataService()
        oid = 'ZTF19aailpwl'
        detections = [{
            'mjd': 58600.0,
            'magpsf_corr': 18.2,
            'sigmapsf_corr': 0.1,
            'fid': 1,
        }]

        with patch('custom_code.data_services.alerce_dataservice._getAlerceObjcet', return_value={
            'total': 1,
            'items': [{'oid': oid}],
        }), patch(
            'custom_code.data_services.alerce_dataservice._getAlerceLightCurve',
            return_value={'detections': detections},
        ):
            results = service.query_targets({'ra': 298.0, 'dec': 29.8, 'radius_arcsec': 1.1})

        self.assertEqual(results[0]['aliases'], [{
            'name': oid,
            'url': _alerce_object_url(oid),
            'source_name': 'Alerce',
        }])
        self.assertEqual(results[0]['source_location'], _alerce_object_url(oid))
