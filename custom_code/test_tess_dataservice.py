from unittest.mock import Mock, patch

from astropy.table import Table
from django.test import SimpleTestCase

from custom_code.data_services.tess_dataservice import TESSDataService


class TESSDataServiceTests(SimpleTestCase):
    source_ra = 42.213025
    source_dec = 62.2162555556

    def test_query_finds_tess_spoc_hlsp_lightcurves(self):
        observations = [{
            's_ra': 42.2130295833856,
            's_dec': 62.2162651566059,
            'target_name': '423996113',
            'provenance_name': 'TESS-SPOC',
            'obs_id': 'hlsp_tess-spoc_tess_phot_0000000423996113-s0018_tess_v1_tp',
            'sequence_number': 18,
            't_exptime': 1800.0,
        }]
        service = TESSDataService()

        with patch(
            'astroquery.mast.Observations.query_criteria', return_value=observations
        ) as query_criteria, patch.object(
            service, '_collect_lightcurve_products', return_value=['sector18_lc.fits']
        ) as collect:
            result = service.query_service({
                'ra': self.source_ra,
                'dec': self.source_dec,
                'radius_arcsec': 21.0,
                'max_sectors': 12,
                'flux_type': 'sap',
            })

        query_kwargs = query_criteria.call_args.kwargs
        self.assertEqual(query_kwargs['obs_collection'], ['TESS', 'HLSP'])
        self.assertEqual(query_kwargs['provenance_name'], ['SPOC', 'TESS-SPOC'])
        self.assertEqual(query_kwargs['dataproduct_type'], 'timeseries')
        self.assertEqual(result['ticid'], 423996113)
        self.assertEqual(result['products'], ['sector18_lc.fits'])
        self.assertEqual(collect.call_args.args[1], observations)

    def test_product_lookup_recognizes_hlsp_lightcurve_filename(self):
        products = Table({
            'productFilename': [
                'hlsp_tess-spoc_tess_phot_0000000423996113-s0018_tess_v1_lc.fits',
                'hlsp_tess-spoc_tess_phot_0000000423996113-s0018_tess_v1_tp.fits',
            ],
            'productSubGroupDescription': ['--', '--'],
            'productType': ['SCIENCE', 'SCIENCE'],
        })
        manifest = Table({
            'Local Path': ['/tmp/sector18_lc.fits'],
        })

        class FakeObservations:
            get_product_list = staticmethod(lambda _observation: products)

            @staticmethod
            def filter_products(product_list, **filters):
                keep = [
                    all(str(row[name]) == expected for name, expected in filters.items())
                    for row in product_list
                ]
                return product_list[keep]

            download_products = Mock(return_value=manifest)

        paths = TESSDataService()._collect_lightcurve_products(
            FakeObservations, [{'obs_id': 'sector-18'}]
        )

        self.assertEqual(paths, ['/tmp/sector18_lc.fits'])
        downloaded = FakeObservations.download_products.call_args.args[0]
        self.assertEqual(list(downloaded['productFilename']), [
            'hlsp_tess-spoc_tess_phot_0000000423996113-s0018_tess_v1_lc.fits'
        ])

    def test_selection_still_accepts_native_spoc_provenance(self):
        observation = {
            's_ra': self.source_ra,
            's_dec': self.source_dec,
            'target_name': '423996113',
            'provenance_name': 'SPOC',
            'obs_id': 'tess-s0018-0000000423996113-0120-s_lc',
            'sequence_number': 18,
            't_exptime': 120.0,
        }

        ticid, selected = TESSDataService()._select_observations(
            [observation], self.source_ra, self.source_dec, max_sectors=12
        )

        self.assertEqual(ticid, 423996113)
        self.assertEqual(selected, [observation])
