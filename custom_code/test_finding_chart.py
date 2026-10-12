from django.test import SimpleTestCase, TestCase

from custom_code.finding_chart import fibre_overlays, survey_overlays


class FindingChartOverlayTests(SimpleTestCase):
    def test_uses_other_names_sources_once_each(self):
        other_names = [
            {'source_name': 'AllWISE', 'name': 'WISE J1927+6533'},
            {'source_name': 'AllWISE', 'name': 'another WISE alias'},
            {'source_name': 'ZTF', 'name': 'ZTF19abc'},
            {'source_name': 'Simbad', 'name': 'RX J1927.3+6533'},
            {'source_name': 'TNS', 'name': 'AT2024abc'},
        ]

        overlays = survey_overlays(other_names, {'FAVA'})

        self.assertEqual([item['name'] for item in overlays], [
            'AllWISE (6″ search radius)',
            'FAVA (12° LAT 95% PSF)',
            'ZTF (1.1″ search radius)',
        ])
        self.assertEqual(len({item['color'] for item in overlays}), 3)

    def test_newer_services_have_search_radius_circles(self):
        overlays = survey_overlays([], {'DECaPS', 'JWSTSpectra', 'Kepler', 'RXTEASM'})

        self.assertEqual([item['name'] for item in overlays], [
            'DECaPS (1.5″ search radius)',
            'JWSTSpectra (3″ search radius)',
            'Kepler (4″ search radius)',
            'RXTEASM (60″ search radius)',
        ])

    def test_no_sources_means_no_circle_layers(self):
        self.assertEqual(survey_overlays([{'source_name': 'Simbad'}]), [])


class FibreOverlayTests(SimpleTestCase):
    def test_one_layer_per_instrument_with_circles_at_fibre_positions(self):
        fibres = [
            {'instrument': 'SDSS', 'fiber_ra': 29.990148, 'fiber_dec': 0.5529173, 'fiber_diameter_arcsec': 3.0},
            {'instrument': 'BOSS', 'fiber_ra': 29.990175, 'fiber_dec': 0.5529163, 'fiber_diameter_arcsec': 2.0},
            {'instrument': 'BOSS', 'fiber_ra': 29.990175, 'fiber_dec': 0.5529163, 'fiber_diameter_arcsec': 2.0},
            {'instrument': 'BOSS', 'fiber_ra': 29.990161, 'fiber_dec': 0.552903, 'fiber_diameter_arcsec': 2.0},
            {'instrument': 'DESI', 'fiber_ra': 29.9902243, 'fiber_dec': 0.5528716, 'fiber_diameter_arcsec': 1.5},
            {'instrument': 'BOSS', 'fiber_ra': None, 'fiber_dec': 0.55, 'fiber_diameter_arcsec': 2.0},
        ]

        overlays = fibre_overlays(fibres, start_index=3)

        self.assertEqual([item['name'] for item in overlays], [
            'BOSS fibre (2\u2033 diameter, 3 spectra)',
            'DESI fibre (1.5\u2033 diameter, 1 spectrum)',
            'SDSS fibre (3\u2033 diameter, 1 spectrum)',
        ])
        boss = overlays[0]
        self.assertEqual(boss['radius_arcsec'], 1.0)
        # Repeat spectra through the same position share a circle.
        self.assertEqual(len(boss['circles']), 2)
        self.assertEqual(boss['circles'][0], {'ra': 29.990175, 'dec': 0.5529163, 'radius_arcsec': 1.0})

    def test_no_fibres_means_no_layers(self):
        self.assertEqual(fibre_overlays([]), [])


class SpectrumFibreBackfillTests(TestCase):
    def test_requery_adds_fibre_keys_without_duplicating_the_spectrum(self):
        from datetime import datetime, timezone

        from tom_dataproducts.models import ReducedDatum
        from tom_targets.models import Target

        from custom_code.data_services.desi_dataservice import DESIDataService

        target = Target.objects.create(name='fibre-test', type='SIDEREAL', ra=29.99, dec=0.553, epoch=2000)
        timestamp = datetime(2021, 10, 10, tzinfo=timezone.utc)
        old_value = {'flux': [1.0, 2.0], 'wavelength': [4000.0, 4001.0], 'spectrum_type': 'DESI_DR1_spectrum',
                     'filter': 'DESI-DR1', 'source_id': 'DESI J015957.65+003310.3'}
        ReducedDatum.objects.create(target=target, data_type='spectroscopy', timestamp=timestamp,
                                    value=old_value, source_name='DESI')
        new_value = {**old_value, 'instrument': 'DESI', 'fiber_ra': 29.9902243, 'fiber_dec': 0.5528716,
                     'fiber_diameter_arcsec': 1.5}

        DESIDataService().create_reduced_datums_from_query(
            target, data=[{'timestamp': timestamp, 'value': new_value}], data_type='spectroscopy')

        rows = ReducedDatum.objects.filter(target=target, data_type='spectroscopy')
        self.assertEqual(rows.count(), 1)
        self.assertEqual(rows.get().value['fiber_diameter_arcsec'], 1.5)

        from custom_code.templatetags.custom_target_extras import survey_finderchart

        overlays = survey_finderchart({}, target)['survey_overlays']
        fibre = [item for item in overlays if 'circles' in item]
        self.assertEqual([item['name'] for item in fibre], ['DESI fibre (1.5\u2033 diameter, 1 spectrum)'])
        self.assertEqual(fibre[0]['circles'], [{'ra': 29.9902243, 'dec': 0.5528716, 'radius_arcsec': 0.75}])

