from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase

from custom_code.finding_chart import survey_overlays


class DatumRows:
    def __init__(self, rows):
        self.rows = rows

    def filter(self, **criteria):
        rows = self.rows
        if 'source_name__in' in criteria:
            rows = [row for row in rows if row.source_name in criteria['source_name__in']]
        if 'source_name' in criteria:
            rows = [row for row in rows if row.source_name == criteria['source_name']]
        return DatumRows(rows)

    def only(self, *fields):
        return self

    def iterator(self):
        return iter(self.rows)


class FindingChartOverlayTests(SimpleTestCase):
    def test_only_plotted_services_get_independent_radii(self):
        datums = {
            'photometry': DatumRows([
                SimpleNamespace(source_name='AllWISE', value={'filter': 'WISE(W1)', 'magnitude': 14.2}),
                SimpleNamespace(source_name='ZTF', value={'filter': 'ZTF(g)'}),
                SimpleNamespace(source_name='TNS', value={'filter': 'TNS(g)', 'magnitude': 17.0}),
            ]),
            'highenergy': DatumRows([
                SimpleNamespace(source_name='FAVA', value={'filter': 'LAT(>100MeV)', 'flux': 0.3}),
            ]),
        }
        request = SimpleNamespace(GET={})

        def visible(_target, data_type, _request):
            return datums[data_type]

        with patch('custom_code.finding_chart._visible_datums', side_effect=visible):
            overlays = survey_overlays(SimpleNamespace(), request)

        self.assertEqual([item['name'] for item in overlays], [
            'AllWISE (6″ search radius)', 'FAVA (12° LAT 95% PSF)',
        ])
        self.assertEqual([item['radius_arcsec'] for item in overlays], [6.0, 43200.0])
        self.assertNotEqual(overlays[0]['color'], overlays[1]['color'])

    def test_difference_view_excludes_services_without_difference_points(self):
        datums = {
            'photometry': DatumRows([
                SimpleNamespace(source_name='AllWISE', value={'filter': 'WISE(W1)', 'magnitude': 14.2}),
            ]),
            'highenergy': DatumRows([]),
        }
        with patch('custom_code.finding_chart._visible_datums', side_effect=lambda _t, kind, _r: datums[kind]):
            overlays = survey_overlays(SimpleNamespace(), SimpleNamespace(GET={'phot': 'diff'}))
        self.assertEqual(overlays, [])
