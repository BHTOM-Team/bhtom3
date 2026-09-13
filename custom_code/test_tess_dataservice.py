from unittest.mock import Mock, patch

from astropy.table import Table
from django.test import SimpleTestCase

from custom_code.data_services.tess_dataservice import TESSDataService, _nearest_tic_id


class TESSDataServiceTests(SimpleTestCase):
    source_ra = 42.213025
    source_dec = 62.2162555556

    def test_query_finds_tess_spoc_hlsp_lightcurves(self):
        tic_rows = Table({
            'ID': ['423996113'],
            'ra': [42.2130295833856],
            'dec': [62.2162651566059],
        })
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
            'astroquery.mast.Catalogs.query_region', return_value=tic_rows
        ), patch(
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
        self.assertEqual(query_kwargs['target_name'], '423996113')
        self.assertNotIn('coordinates', query_kwargs)
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

    def test_nearest_tic_is_selected_before_observation_query(self):
        catalog_rows = Table({
            'ID': ['647292123', '423996113'],
            'ra': [42.2140616303557, 42.2130295833856],
            'dec': [62.2157837767896, 62.2162651566059],
        })

        self.assertEqual(
            _nearest_tic_id(catalog_rows, self.source_ra, self.source_dec),
            423996113,
        )

    def test_bulk_product_lookup_downloads_all_selected_lightcurves_once(self):
        selected = Table({
            'obsid': [38474783, 166328851],
            'obs_id': ['sector-18', 'sector-58'],
        })
        products = Table({
            'productFilename': [
                'target-s0018_lc.fits',
                'target-s0018_tp.fits',
                'target-s0058_lc.fits',
                'target-s0058_tp.fits',
            ],
            'productSubGroupDescription': ['--', '--', '--', '--'],
            'productType': ['SCIENCE', 'SCIENCE', 'SCIENCE', 'SCIENCE'],
        })
        manifest = Table({
            'Local Path': ['/tmp/target-s0018_lc.fits', '/tmp/target-s0058_lc.fits'],
        })

        class FakeObservations:
            get_product_list = Mock(return_value=products)

            @staticmethod
            def filter_products(product_list, **filters):
                keep = [
                    all(str(row[name]) == expected for name, expected in filters.items())
                    for row in product_list
                ]
                return product_list[keep]

            download_products = Mock(return_value=manifest)

        paths = TESSDataService()._collect_lightcurve_products(FakeObservations, selected)

        self.assertEqual(paths, ['/tmp/target-s0018_lc.fits', '/tmp/target-s0058_lc.fits'])
        FakeObservations.get_product_list.assert_called_once_with(selected)
        downloaded = FakeObservations.download_products.call_args.args[0]
        self.assertEqual(list(downloaded['productFilename']), [
            'target-s0018_lc.fits',
            'target-s0058_lc.fits',
        ])
