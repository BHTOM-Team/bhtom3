from unittest import TestCase
from urllib.parse import urljoin

from custom_code.facility_urls import normalize_facility_url


class FacilityURLTests(TestCase):
    def test_unwraps_reported_lco_portal(self):
        value = '[https://observe.lco.global](https://observe.lco.global)'
        self.assertEqual(
            urljoin(normalize_facility_url(value), '/api/instruments/'),
            'https://observe.lco.global/api/instruments/',
        )

    def test_uses_link_destination_rather_than_label(self):
        self.assertEqual(
            normalize_facility_url('  [LCO Archive](https://archive-api.lco.global/)  '),
            'https://archive-api.lco.global/',
        )

    def test_preserves_custom_portal_and_path(self):
        for value in ('https://custom.example/portal/', 'http://localhost:8000'):
            with self.subTest(value=value):
                self.assertEqual(normalize_facility_url(value), value)

    def test_blank_values_use_default(self):
        for value in (None, '', '   '):
            with self.subTest(value=value):
                self.assertEqual(normalize_facility_url(value, 'https://observe.lco.global'),
                                 'https://observe.lco.global')
