import json
from datetime import datetime, timedelta, timezone

import numpy as np
from astropy.time import Time
from astropy.timeseries import LombScargle
from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase
from django.urls import reverse
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target

from custom_code.periodicity import PeriodSearchError, search_period


MJD_EPOCH = datetime(1858, 11, 17, tzinfo=timezone.utc)


def _offset_light_curve(seed=42, period=3.7):
    """One star seen by telescopes A and B whose zero-points differ by 1.2 mag, in different seasons."""
    rng = np.random.default_rng(seed)
    t_a = np.sort(rng.uniform(58000, 58700, 150))
    t_a = t_a[((t_a - 58000) % 365) < 240]
    t_b = np.sort(rng.uniform(58400, 59100, 150))
    t_b = t_b[((t_b - 58000) % 365) < 240]
    m_a = 15.0 + 0.3 * np.sin(2 * np.pi * t_a / period) + rng.normal(0, 0.03, len(t_a))
    m_b = 13.8 + 0.3 * np.sin(2 * np.pi * t_b / period) + rng.normal(0, 0.03, len(t_b))
    times = np.r_[t_a, t_b]
    return times, np.r_[m_a, m_b], np.full(len(times), 0.03), ['A'] * len(t_a) + ['B'] * len(t_b)


class SearchPeriodTests(SimpleTestCase):
    def test_per_telescope_offsets_recover_period(self):
        times, values, errors, bands = _offset_light_curve()

        result = search_period(times, values, errors, bands)

        self.assertAlmostEqual(result['best_period'], 3.7, delta=0.01)
        self.assertEqual(result['bands'], [
            {'name': 'A', 'n_points': bands.count('A')},
            {'name': 'B', 'n_points': bands.count('B')},
        ])
        self.assertIsNotNone(result['fap_1'])

        # One shared zero-point is what used to fail: the offset dominates the periodogram.
        merged = search_period(times, values, errors)
        self.assertGreater(abs(merged['best_period'] - 3.7), 1.0)

    def test_single_band_matches_plain_lomb_scargle(self):
        rng = np.random.default_rng(3)
        times = np.sort(rng.uniform(0, 300, 80))
        values = 12 + 0.1 * np.sin(2 * np.pi * times / 5.3) + rng.normal(0, 0.02, 80)
        errors = np.full(80, 0.02)

        result = search_period(times, values, errors, min_period=0.5, max_period=100)

        frequency, power = LombScargle(times, values, errors).autopower(
            minimum_frequency=1 / 100, maximum_frequency=1 / 0.5, samples_per_peak=5,
        )
        self.assertAlmostEqual(result['best_period'], 1 / frequency[np.argmax(power)], places=9)

    def test_missing_errors_and_sparse_bands_are_handled(self):
        times, values, errors, bands = _offset_light_curve()
        errors = errors.astype(object)
        errors[::3] = None
        errors[np.array(bands) == 'B'] = None  # a telescope without any errors
        times = np.r_[times, 58100.0, 58200.0]
        values = np.r_[values, 9.0, 9.1]
        errors = np.r_[errors, None, None]
        bands = bands + ['sparse', 'sparse']

        result = search_period(times.tolist(), values.tolist(), errors.tolist(), bands)

        self.assertEqual(result['excluded_bands'], ['sparse'])
        self.assertEqual([band['name'] for band in result['bands']], ['A', 'B'])
        self.assertAlmostEqual(result['best_period'], 3.7, delta=0.01)

    def test_invalid_input_raises(self):
        with self.assertRaises(PeriodSearchError):
            search_period([1, 2, 3, 4, 5], [1, 2, 3, 4])
        with self.assertRaises(PeriodSearchError):
            search_period(range(10), range(10), min_period=5, max_period=1)
        with self.assertRaises(PeriodSearchError):
            search_period([1.0] * 10, range(10))


class TargetPeriodicityViewTests(TestCase):
    def setUp(self):
        user = get_user_model().objects.create_user(username='periodicity-user', password='secret')
        self.client.force_login(user)
        self.target = Target.objects.create(name='Periodic Target', type='SIDEREAL', ra=10.0, dec=20.0, epoch=2000.0)
        self.page_url = reverse('target-periodicity', kwargs={'pk': self.target.pk})
        self.data_url = reverse('target-periodicity-data', kwargs={'pk': self.target.pk})
        self.compute_url = reverse('target-periodicity-compute', kwargs={'pk': self.target.pk})

    def _datum(self, data_type, day, value):
        return ReducedDatum.objects.create(
            target=self.target,
            data_type=data_type,
            timestamp=datetime(2024, 1, day, 12, 0, tzinfo=timezone.utc),
            value=value,
            source_name='test',
        )

    def _store_mixed_datasets(self):
        self._datum('photometry', 1, {'magnitude': 15.1, 'error': 0.02, 'filter': 'V', 'telescope': 'T1'})
        self._datum('photometry', 2, {'magnitude': 15.3, 'error': 0.02, 'filter': 'V', 'telescope': 'T2'})
        self._datum('photometry', 3, {'limit': 18.0, 'filter': 'V', 'telescope': 'T1'})
        self._datum('photometry', 4, {'magnitude': 14.0, 'filter': ''})
        self._datum('photometry', 5, {'magnitude': 14.2})
        self._datum('highenergy', 6, {'flux': 0.4, 'error': 0.1, 'filter': 'LAT(>100MeV)', 'facility': 'FERMI-LAT'})
        self._datum('highenergy', 7, {'flux': -1, 'error': 0, 'filter': 'LAT(>100MeV)', 'facility': 'FERMI-LAT'})

    def _store_offset_light_curve(self):
        """Store _offset_light_curve() as filter V, with its bands as telescopes."""
        times, values, errors, bands = _offset_light_curve()
        ReducedDatum.objects.bulk_create([
            ReducedDatum(
                target=self.target,
                data_type='photometry',
                timestamp=MJD_EPOCH + timedelta(days=float(t)),
                value={'magnitude': float(v), 'error': float(e), 'filter': 'V', 'telescope': b},
                source_name='test',
            )
            for t, v, e, b in zip(times, values, errors, bands)
        ])
        return times, np.array(bands)

    def _post(self, payload, url=None):
        return self.client.post(url or self.compute_url, data=json.dumps(payload), content_type='application/json')

    def test_page_lists_datasets_without_loading_points(self):
        self._store_mixed_datasets()

        response = self.client.get(self.page_url)

        self.assertEqual(response.status_code, 200)
        datasets = response.context['periodicity_datasets']
        self.assertEqual(
            [(d['id'], d['kind'], d['n_points']) for d in datasets],
            [('photometry:Unknown', 'mag', 2), ('photometry:V', 'mag', 2), ('highenergy:LAT(>100MeV)', 'flux', 1)],
        )
        self.assertAlmostEqual(datasets[1]['mjd_min'], Time(datetime(2024, 1, 1, 12, tzinfo=timezone.utc)).mjd, places=6)
        self.assertAlmostEqual(datasets[1]['mjd_max'], Time(datetime(2024, 1, 2, 12, tzinfo=timezone.utc)).mjd, places=6)
        self.assertNotContains(response, '"val"')

    def test_data_endpoint_returns_one_dataset(self):
        self._store_mixed_datasets()
        self._datum('highenergy', 8, {'flux': 0.2, 'error': 0, 'filter': 'LAT(>100MeV)', 'facility': 'FERMI-LAT'})

        response = self.client.get(self.data_url, {'dataset': 'highenergy:LAT(>100MeV)'})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['kind'], 'flux')
        self.assertEqual(payload['telescopes'], {
            'FERMI-LAT': {'mjd': [Time(datetime(2024, 1, 6, 12, tzinfo=timezone.utc)).mjd], 'val': [0.4], 'err': [0.1]},
        })

        unknown = self.client.get(self.data_url, {'dataset': 'photometry:Unknown'}).json()
        self.assertEqual(sorted(unknown['telescopes']['test']['val']), [14.0, 14.2])

    def test_data_endpoint_errors_are_json(self):
        self._store_mixed_datasets()
        cases = [
            (self.data_url, {'dataset': 'photometry:R'}, 404),
            (self.data_url, {'dataset': 'bogus'}, 400),
            (reverse('target-periodicity-data', kwargs={'pk': 999999}), {'dataset': 'photometry:V'}, 404),
        ]
        for url, params, status in cases:
            response = self.client.get(url, params)
            self.assertEqual(response.status_code, status, params)
            self.assertIn('error', response.json())

    def test_compute_searches_only_the_requested_dataset(self):
        times, _ = self._store_offset_light_curve()
        self._datum('highenergy', 1, {'flux': 0.4, 'error': 0.1, 'filter': 'LAT(>100MeV)'})

        response = self._post({
            'dataset': 'photometry:V', 'telescopes': ['A', 'B'], 'split_telescopes': True,
            'min_period': 0.5, 'max_period': 100,
        })

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertAlmostEqual(payload['best_period'], 3.7, delta=0.01)
        self.assertEqual([band['name'] for band in payload['bands']], ['V · A', 'V · B'])
        self.assertEqual(payload['n_points'], len(times))

    def test_compute_applies_telescope_and_time_range_selection(self):
        times, telescopes = self._store_offset_light_curve()
        mjd_max = 58550.5

        response = self._post({
            'dataset': 'photometry:V', 'telescopes': ['A'], 'mjd_min': None, 'mjd_max': mjd_max,
            'min_period': 0.5, 'max_period': 100,
        })

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual([band['name'] for band in payload['bands']], ['V'])
        self.assertEqual(payload['n_points'], int(np.count_nonzero((telescopes == 'A') & (times <= mjd_max))))

    def test_compute_errors_are_json(self):
        self._store_offset_light_curve()
        other_target = reverse('target-periodicity-compute', kwargs={'pk': 999999})
        cases = [
            ({'dataset': 'photometry:R'}, self.compute_url, 404),
            ({'dataset': 'photometry:V'}, other_target, 404),
            ({'dataset': 'nonsense'}, self.compute_url, 400),
            ({'dataset': 'photometry:V', 'min_period': 5, 'max_period': 1}, self.compute_url, 400),
            ({'dataset': 'photometry:V', 'telescopes': 'A'}, self.compute_url, 400),
        ]
        for payload, url, status in cases:
            response = self._post(payload, url)
            self.assertEqual(response.status_code, status, payload)
            self.assertIn('error', response.json())

        response = self.client.post(self.compute_url, data='not json', content_type='application/json')
        self.assertEqual(response.status_code, 400)
        self.assertIn('error', response.json())
