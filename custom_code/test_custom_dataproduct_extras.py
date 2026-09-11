from datetime import datetime, timezone
from types import SimpleNamespace

from django.test import SimpleTestCase

from custom_code.templatetags.custom_dataproduct_extras import (
    _spectrum_source_label,
    _spectrum_time_traces,
)


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
        self.assertEqual(list(traces[0].y), [0.92, 0.99, None, 0.92, 0.99, None])
        self.assertEqual(traces[0].yaxis, 'y2')
        self.assertEqual(traces[0].mode, 'lines')
        self.assertEqual(traces[0].line.dash, 'dash')

    def test_uses_source_name_when_spectrum_filter_is_missing(self):
        datum = SimpleNamespace(value={}, source_name='Imported spectra', timestamp=None)

        self.assertEqual(_spectrum_source_label(datum), 'Imported spectra')
        self.assertEqual(_spectrum_time_traces([datum]), [])
