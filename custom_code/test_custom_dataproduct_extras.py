from datetime import datetime, timezone
from types import SimpleNamespace

from django.contrib.auth.models import User
from django.template import Context, Template
from django.template.library import InclusionNode
from django.template.loader import get_template
from django.test import SimpleTestCase, TestCase, override_settings
from guardian.shortcuts import assign_perm
from plotly import graph_objects as go
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target

from custom_code.templatetags.custom_dataproduct_extras import (
    ALERCE_SPECIAL_COLOR_MAP,
    HIGHENERGY_COLOR_MAP,
    HIGHENERGY_LIMITS_COLOR_MAP,
    PHOTOMETRY_COLOR_MAP,
    PHOTOMETRY_LIMITS_COLOR_MAP,
    _spectrum_source_label,
    _spectrum_time_traces,
    get_photometry_data,
)


@override_settings(TARGET_PERMISSIONS_ONLY=True, DATA_SHARING={})
class PhotometryDataTableTests(TestCase):
    def setUp(self):
        self.target = Target.objects.create(name='mixed-photometry', type='SIDEREAL', ra=1, dec=2)
        self.user = User.objects.create_user(username='photometry-reader')
        self.context = {'request': SimpleNamespace(user=self.user)}

    def datum(self, value, day=1):
        # Include legacy payloads even when current model validation would reject them.
        return ReducedDatum.objects.bulk_create([ReducedDatum(
            target=self.target, data_type='photometry', source_name='test',
            timestamp=datetime(2026, 1, day, tzinfo=timezone.utc), value=value,
        )])[0]

    def test_mixed_records_keep_magnitudes_and_limits_in_timestamp_order(self):
        detection = self.datum({'magnitude': 18.2, 'error': 0.1, 'filter': 'r', 'telescope': 'test'})
        limit = self.datum({'limit': 20.5, 'filter': 'g'}, day=3)
        zero = self.datum({'magnitude': 0, 'magnitude_error': 0.2, 'limit': None}, day=2)
        for value in (
            {'diff_magnitude': 19.1, 'diff_error': 0.2, 'diff_sign': 1, 'filter': 'r'},
            {'flux': 12.3}, {}, [], 'bad payload',
            {'magnitude': None}, {'magnitude': 'bad'}, {'magnitude': 'NaN'},
            {'magnitude': 'Infinity'},
        ):
            self.datum(value)

        result = get_photometry_data(self.context, self.target, target_share=True)

        self.assertEqual([row['id'] for row in result['data']], [limit.pk, zero.pk, detection.pk])
        self.assertEqual([row['magnitude'] for row in result['data']], [20.5, 0, 18.2])
        self.assertEqual([row['limit'] for row in result['data']], [True, False, False])
        self.assertEqual(result['data'][1]['error'], 0.2)
        self.assertEqual(result['data'][2]['error'], 0.1)
        self.assertEqual(result['data'][2]['telescope'], 'test')
        self.assertEqual(result['data'][2]['filter'], 'r')
        self.assertEqual(result['data'][2]['source'], 'test')
        self.assertTrue(result['target_share'])
        self.assertEqual(result['target_data_share_form'].initial['target'], self.target)
        self.assertEqual(result['target_data_share_form'].initial['submitter'], self.user)
        self.assertTrue(result['target_data_share_form'].fields['data_type'].widget.is_hidden)
        self.assertIn(('download', 'download'), result['sharing_destinations'])
        self.assertEqual(ReducedDatum.objects.filter(target=self.target).count(), 12)
        rendered = get_template(
            'tom_dataproducts/partials/photometry_datalist_for_target.html'
        ).render(result)
        for datum in (detection, limit, zero):
            self.assertIn(f'id="phot-row-{datum.pk}"', rendered)

    @override_settings(TARGET_PERMISSIONS_ONLY=False)
    def test_only_permitted_measurements_are_returned(self):
        visible = self.datum({'magnitude': 18.2})
        self.datum({'magnitude': 19.3}, day=2)
        assign_perm('tom_dataproducts.view_reduceddatum', self.user, visible)

        result = get_photometry_data(self.context, self.target)

        self.assertEqual([row['id'] for row in result['data']], [visible.pk])

    def test_inclusion_tag_renders_difference_only_target(self):
        difference = self.datum({'diff_magnitude': 19.1, 'diff_error': 0.2, 'filter': 'r'})
        rendered = Template(
            '{% load dataproduct_extras custom_dataproduct_extras %}'
            '{% get_photometry_data target %}'
        ).render(Context({**self.context, 'target': self.target}))

        self.assertIn('No Photometry Data.', rendered)
        self.assertNotIn(f'id="phot-row-{difference.pk}"', rendered)

    def test_target_detail_template_uses_safe_table_tag(self):
        template = get_template('tom_targets/target_detail.html').template
        tags = template.nodelist.get_nodes_by_type(InclusionNode)
        table_tags = [node for node in tags if node.func.__name__ == 'get_photometry_data']

        self.assertEqual(len(table_tags), 1)
        self.assertIs(table_tags[0].func, get_photometry_data)


class PlotlyMarkerStyleTests(SimpleTestCase):
    def test_all_configured_marker_symbols_are_valid_plotly_symbols(self):
        style_maps = (
            PHOTOMETRY_COLOR_MAP,
            PHOTOMETRY_LIMITS_COLOR_MAP,
            ALERCE_SPECIAL_COLOR_MAP,
            HIGHENERGY_COLOR_MAP,
            HIGHENERGY_LIMITS_COLOR_MAP,
        )

        for style_map in style_maps:
            for filter_name, (_, symbol, _) in style_map.items():
                with self.subTest(filter_name=filter_name, symbol=symbol):
                    marker = go.scatter.Marker(symbol=symbol)
                    self.assertEqual(marker.symbol, symbol)


class SpectrumTimeTraceTests(SimpleTestCase):
    def test_groups_timestamps_by_spectroscopy_source(self):
        first = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
        second = datetime(2026, 2, 3, 4, 5, tzinfo=timezone.utc)
        other = datetime(2026, 3, 4, 5, 6, tzinfo=timezone.utc)
        datums = [
            SimpleNamespace(value={'filter': 'LAMOST'}, source_name='archive', timestamp=first),
            SimpleNamespace(value={'filter': 'GALAH'}, source_name='archive', timestamp=other),
            SimpleNamespace(value={'filter': 'LAMOST'}, source_name='archive', timestamp=second),
        ]

        traces = _spectrum_time_traces(datums)

        self.assertEqual([trace.name for trace in traces], ['LAMOST (Spec)', 'GALAH (Spec)'])
        self.assertEqual(list(traces[0].x), [first, first, None, second, second, None])
        self.assertEqual(list(traces[0].y), [0.85, 0.99, None, 0.85, 0.99, None])
        self.assertEqual(traces[0].yaxis, 'y2')
        self.assertEqual(traces[0].mode, 'lines')
        self.assertEqual(traces[0].line.width, 1)
        self.assertEqual(traces[0].line.dash, 'dot')

    def test_uses_source_name_when_spectrum_filter_is_missing(self):
        datum = SimpleNamespace(value={}, source_name='Imported spectra', timestamp=None)

        self.assertEqual(_spectrum_source_label(datum), 'Imported spectra')
        self.assertEqual(_spectrum_time_traces([datum]), [])


class SpectroscopyUnitTests(TestCase):
    def test_normalised_spectra_get_their_own_axis_and_bad_units_are_skipped(self):
        import json

        from tom_dataproducts.models import ReducedDatum
        from tom_targets.models import Target

        from custom_code.templatetags.custom_dataproduct_extras import custom_spectroscopy_for_target

        target = Target.objects.create(name='spectrum-units', type='SIDEREAL', ra=1.0, dec=2.0, epoch=2000)
        base = {'wavelength': [8500.0, 8501.0], 'wavelength_units': 'Angstrom'}
        for day, flux_units, source in (
            (1, '', 'RAVE'),                                   # continuum-normalised
            (2, 'erg / (Angstrom cm2 s)', 'SDSS'),             # calibrated flux density
            (3, 'ct', '2dFGRS'),                               # counts
            (4, 'm', 'Broken'),                                # not convertible: skipped
        ):
            ReducedDatum.objects.create(
                target=target, data_type='spectroscopy', source_name=source,
                timestamp=datetime(2020, 1, day, tzinfo=timezone.utc),
                value={**base, 'flux': [0.9, 1.0], 'flux_units': flux_units},
            )

        spectra = json.loads(custom_spectroscopy_for_target({}, target)['spectra_data'])

        self.assertEqual([spectrum['unit'] for spectrum in spectra],
                         ['normalised', 'erg / (Angstrom s cm2)', 'ct'])
        self.assertEqual(spectra[0]['flux'], [0.9, 1.0])
