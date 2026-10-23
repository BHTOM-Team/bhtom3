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


class TESSMagnitudeFilterTests(SimpleTestCase):
    def _lightcurve(self, flux, flux_err):
        import os
        import tempfile

        import numpy as np
        from astropy.io import fits

        n = len(flux)
        table = fits.BinTableHDU.from_columns([
            fits.Column(name='TIME', format='D', array=np.arange(n, dtype=float) + 1500.0),
            fits.Column(name='SAP_FLUX', format='E', unit='e-/s', array=np.array(flux, dtype=float)),
            fits.Column(name='SAP_FLUX_ERR', format='E', unit='e-/s', array=np.array(flux_err, dtype=float)),
            fits.Column(name='QUALITY', format='J', array=np.zeros(n, dtype=int)),
        ], name='LIGHTCURVE')
        primary = fits.PrimaryHDU()
        primary.header['SECTOR'] = 17
        tmpdir = tempfile.mkdtemp()
        self.addCleanup(__import__('shutil').rmtree, tmpdir, True)
        path = os.path.join(tmpdir, 'sector17_lc.fits')
        fits.HDUList([primary, table]).writeto(path)
        return path

    def test_negative_magnitudes_are_dropped(self):
        path = self._lightcurve(flux=[1000.0, 2.0e8, 1100.0], flux_err=[5.0, 5.0, 5.0])

        datums = TESSDataService()._build_photometry_datums([path])

        mags = [d['value']['magnitude'] for d in datums]
        self.assertEqual(len(mags), 2)
        self.assertTrue(all(m >= 0 for m in mags))

    def test_errors_above_three_mag_are_dropped(self):
        path = self._lightcurve(flux=[1000.0, 1050.0, 1100.0], flux_err=[5.0, 4000.0, 5.0])

        datums = TESSDataService()._build_photometry_datums([path])

        errors = [d['value']['error'] for d in datums]
        self.assertEqual(len(errors), 2)
        self.assertTrue(all(e <= 3 for e in errors))

    def test_valid_points_are_kept(self):
        path = self._lightcurve(flux=[1000.0, 1050.0, 1100.0], flux_err=[5.0, 5.0, 5.0])

        datums = TESSDataService()._build_photometry_datums([path])

        self.assertEqual(len(datums), 3)

    def test_zero_magnitude_is_dropped(self):
        zero_mag_flux = 10 ** (20.44 / 2.5)
        path = self._lightcurve(flux=[1000.0, zero_mag_flux, 1100.0], flux_err=[5.0, 5.0, 5.0])

        datums = TESSDataService()._build_photometry_datums([path])

        self.assertEqual(len(datums), 2)
        self.assertTrue(all(d['value']['magnitude'] > 0 for d in datums))

    def test_zero_error_is_dropped(self):
        path = self._lightcurve(flux=[1000.0, 1050.0, 1100.0], flux_err=[5.0, 0.0, 5.0])

        datums = TESSDataService()._build_photometry_datums([path])

        self.assertEqual(len(datums), 2)
        self.assertTrue(all(d['value']['error'] > 0 for d in datums))

    def test_negative_error_is_dropped(self):
        path = self._lightcurve(flux=[1000.0, 1050.0, 1100.0], flux_err=[5.0, -5.0, 5.0])

        datums = TESSDataService()._build_photometry_datums([path])

        self.assertEqual(len(datums), 2)
        self.assertTrue(all(d['value']['error'] > 0 for d in datums))

    def test_error_that_rounds_to_zero_is_dropped(self):
        path = self._lightcurve(flux=[1000.0, 1050.0, 1100.0], flux_err=[5.0, 1e-4, 5.0])

        datums = TESSDataService()._build_photometry_datums([path])

        self.assertEqual(len(datums), 2)
        self.assertTrue(all(d['value']['error'] > 0 for d in datums))


class TESSQLPFallbackTests(SimpleTestCase):
    source_ra = 80.11716179217501
    source_dec = 20.485366805848887
    tic_rows = Table({'ID': ['6240885'], 'ra': [80.11716179217501], 'dec': [20.485366805848887]})

    def _qlp_observation(self, sector):
        return {
            's_ra': self.source_ra,
            's_dec': self.source_dec,
            'target_name': '6240885',
            'provenance_name': 'QLP',
            'obs_id': f'hlsp_qlp_tess_ffi_s{sector:04d}-0000000006240885_tess_v01_llc',
            'sequence_number': sector,
            't_exptime': 600.0,
        }

    def _query(self, responses):
        service = TESSDataService()
        with patch(
            'astroquery.mast.Catalogs.query_region', return_value=self.tic_rows
        ), patch(
            'astroquery.mast.Observations.query_criteria', side_effect=responses
        ) as query_criteria, patch.object(
            service, '_collect_lightcurve_products', return_value=['sector43_llc.fits']
        ):
            result = service.query_service({'ra': self.source_ra, 'dec': self.source_dec})
        return result, [call.kwargs['provenance_name'] for call in query_criteria.call_args_list]

    def test_qlp_is_queried_only_when_there_is_no_spoc_lightcurve(self):
        result, provenances = self._query([[], [self._qlp_observation(43)]])

        self.assertEqual(provenances, [['SPOC', 'TESS-SPOC'], ['QLP']])
        self.assertEqual(result['pipeline'], 'QLP')
        self.assertEqual(result['products'], ['sector43_llc.fits'])

    def test_star_with_spoc_lightcurve_never_queries_qlp(self):
        spoc = dict(self._qlp_observation(43), provenance_name='TESS-SPOC', t_exptime=200.0)
        result, provenances = self._query([[spoc]])

        self.assertEqual(provenances, [['SPOC', 'TESS-SPOC']])
        self.assertEqual(result['pipeline'], 'SPOC')

    def test_qlp_lightcurve_filename_is_recognised(self):
        products = Table({
            'productFilename': ['hlsp_qlp_tess_ffi_s0043-0000000006240885_tess_v01_llc.fits'],
            'productSubGroupDescription': ['--'],
            'productType': ['SCIENCE'],
        })

        class FakeObservations:
            @staticmethod
            def filter_products(product_list, **filters):
                keep = [all(str(row[k]) == v for k, v in filters.items()) for row in product_list]
                return product_list[keep]

        found = TESSDataService._filter_lightcurve_products(FakeObservations, products)
        self.assertEqual(len(found), 1)


class TESSQLPConversionTests(SimpleTestCase):
    def _qlp_lightcurve(self, column, flux, flux_err, quality=None, tessmag=11.8513):
        import os
        import shutil

        import numpy as np
        from astropy.io import fits

        n = len(flux)
        table = fits.BinTableHDU.from_columns([
            fits.Column(name='TIME', format='D', array=np.arange(n, dtype=float) + 2474.5),
            fits.Column(name='SAP_FLUX', format='E', array=np.linspace(1.02, 0.88, n)),  # raw ramp, unused
            fits.Column(name=column, format='E', array=np.array(flux, dtype=float)),
            fits.Column(name=f'{column}_ERR', format='E', array=np.array(flux_err, dtype=float)),
            fits.Column(name='QUALITY', format='J', array=np.array(quality or [0] * n, dtype=int)),
        ], name='LIGHTCURVE')
        primary = fits.PrimaryHDU()
        primary.header['SECTOR'] = 43
        if tessmag is not None:
            primary.header['TESSMAG'] = tessmag
        tmpdir = __import__('tempfile').mkdtemp()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        path = os.path.join(tmpdir, 'sector43_llc.fits')
        fits.HDUList([primary, table]).writeto(path)
        return path

    def test_detrended_flux_is_anchored_to_the_tic_magnitude(self):
        path = self._qlp_lightcurve('KSPSAP_FLUX', [1.0, 1.01, 0.99], [0.002, 0.002, 0.002])

        datums = TESSDataService()._build_photometry_datums([path], pipeline='QLP')

        self.assertEqual([d['value']['filter'] for d in datums], ['TESS(QLP)'] * 3)
        self.assertEqual([d['value']['magnitude'] for d in datums], [11.8513, 11.84050, 11.86221])
        self.assertAlmostEqual(datums[0]['value']['error'], 0.002171, places=6)

    def test_newer_det_flux_column_and_quality_flags(self):
        path = self._qlp_lightcurve('DET_FLUX', [1.0, 1.0, 1.0], [0.005, 0.005, 0.005], quality=[0, 4096, 0])

        datums = TESSDataService()._build_photometry_datums([path], pipeline='QLP')

        self.assertEqual(len(datums), 2)

    def test_file_without_tic_magnitude_is_skipped(self):
        path = self._qlp_lightcurve('KSPSAP_FLUX', [1.0, 1.0], [0.002, 0.002], tessmag=None)

        self.assertEqual(TESSDataService()._build_photometry_datums([path], pipeline='QLP'), [])
