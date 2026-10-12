from datetime import datetime, timezone
from types import SimpleNamespace

from django.test import SimpleTestCase, TestCase
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

