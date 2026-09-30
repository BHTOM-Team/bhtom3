from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from django.test import TestCase, override_settings
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target

from custom_code.finding_chart import survey_overlays


@override_settings(TARGET_PERMISSIONS_ONLY=True)
class FindingChartOverlayTests(TestCase):
    def setUp(self):
        self.target = Target.objects.create(
            name='Finding chart target', type=Target.SIDEREAL,
            ra=291.0, dec=65.0, epoch=2000.0,
        )

    def add_datum(self, source, value, data_type='photometry'):
        ReducedDatum.objects.create(
            target=self.target, data_type=data_type, source_name=source,
            timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc), value=value,
        )

    def test_only_services_with_plotted_measurements_get_overlays(self):
        self.add_datum('AllWISE', {'filter': 'WISE(W1)', 'magnitude': 14.2})
        self.add_datum('ZTF', {'filter': 'ZTF(g)', 'magnitude': None})
        self.add_datum('TNS', {'filter': 'TNS(g)', 'magnitude': 17.0})
        self.add_datum('SuperWASP', {'filter': 'WASP/SuperWASP (MAG2)', 'magnitude': 14.5,
                                     'wasp_series': 'MAG2'})
        self.add_datum('SuperWASP', {'filter': 'WASP/SuperWASP (TAMMAG2)', 'magnitude': 14.5})
        self.add_datum('FAVA', {'filter': 'LAT(>100MeV)', 'flux': 0.3}, data_type='highenergy')

        # The chart should ask SQL for source names, not hydrate every light-curve row.
        with patch.object(ReducedDatum, '__init__', side_effect=AssertionError('loaded a photometry row')):
            overlays = survey_overlays(self.target, SimpleNamespace(GET={}))

        self.assertEqual([item['name'] for item in overlays], [
            'AllWISE (6″ search radius)',
            'FAVA (12° LAT 95% PSF)',
            'SuperWASP (5″ search radius)',
        ])
        self.assertEqual(len({item['color'] for item in overlays}), 3)

    def test_difference_view_excludes_services_without_difference_points(self):
        self.add_datum('AllWISE', {'filter': 'WISE(W1)', 'magnitude': 14.2})
        self.add_datum('ZTF', {'filter': 'ZTF(g)', 'diff_magnitude': 18.2})

        overlays = survey_overlays(self.target, SimpleNamespace(GET={'phot': 'diff'}))

        self.assertEqual([item['name'] for item in overlays], ['ZTF (1.1″ search radius)'])
