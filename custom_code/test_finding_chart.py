from django.test import SimpleTestCase

from custom_code.finding_chart import survey_overlays


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
