from datetime import datetime, timezone
from types import SimpleNamespace

from django.test import SimpleTestCase
from plotly import graph_objects as go

from custom_code.templatetags.custom_dataproduct_extras import (
    ALERCE_SPECIAL_COLOR_MAP,
    HIGHENERGY_COLOR_MAP,
    HIGHENERGY_LIMITS_COLOR_MAP,
    PHOTOMETRY_COLOR_MAP,
    PHOTOMETRY_LIMITS_COLOR_MAP,
    _spectrum_source_label,
    _spectrum_time_traces,
)


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
