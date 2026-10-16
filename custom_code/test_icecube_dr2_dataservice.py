import os
import tempfile

import numpy as np
from django.test import SimpleTestCase

from custom_code.data_services.icecube_dr2_dataservice import (
    ICECUBE_DR2_DATA_PATH,
    ICECUBE_DR2_FILTER,
    IceCubeDR2DataService,
    association_level,
    association_log10_sb,
    icecube_dr2_cone_search,
    load_icecube_dr2_events,
)


def _write_table(path, rows):
    """Pack (mjd, ra, dec, ang_err, log10_energy) rows the way build_icecube_dr2 does."""
    rows = sorted(rows)
    mjd_ticks = np.rint(np.array([r[0] for r in rows]) / 1e-8).astype(np.int64)
    np.savez_compressed(
        path,
        mjd_start=mjd_ticks[:1],
        mjd_delta=np.diff(mjd_ticks).astype(np.uint32),
        run=np.arange(len(rows), dtype=np.uint32) + 100000,
        event=np.arange(len(rows), dtype=np.uint32),
        subevent=np.zeros(len(rows), dtype=np.uint8),
        log10_energy=np.array([round(r[4] * 100) for r in rows], dtype=np.uint16),
        ang_err=np.array([round(r[3] * 100) for r in rows], dtype=np.uint16),
        ra=np.array([round(r[1] * 1000) for r in rows], dtype=np.uint32),
        dec=np.array([round(r[2] * 1000) for r in rows], dtype=np.int32),
        season=np.zeros(len(rows), dtype=np.uint8),
        seasons=np.array(['IC86_I']),
        dataverse_version=np.array('3.1'),
    )


class IceCubeDR2ConeSearchTests(SimpleTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, 'events.npz')
        _write_table(self.path, [
            (55700.12345678, 10.000, 20.000, 0.50, 3.25),  # 0.3 deg away, inside its 0.5 deg error
            (55701.00000000, 10.000, 20.900, 0.50, 4.10),  # 0.6 deg away, outside 1 x AngErr
            (55702.00000000, 10.000, 22.000, 5.00, 2.80),  # 1.7 deg away, error huge but past max radius
            (55703.00000000, 359.900, 19.700, 0.40, 3.00),  # across RA = 0 from the target
        ])
        self.events = load_icecube_dr2_events.__wrapped__(self.path)

    def tearDown(self):
        self.tmp.cleanup()

    def test_matches_events_whose_error_circle_contains_the_target(self):
        matches = icecube_dr2_cone_search(10.0, 20.3, max_radius_deg=1.0, events=self.events)
        self.assertEqual([m['separation_deg'] for m in matches], [0.3])
        self.assertAlmostEqual(matches[0]['mjd'], 55700.12345678, places=8)
        self.assertEqual(matches[0]['log10_energy_gev'], 3.25)
        self.assertEqual(matches[0]['season'], 'IC86_I')

    def test_n_sigma_widens_the_match(self):
        matches = icecube_dr2_cone_search(10.0, 20.3, n_sigma=2.0, max_radius_deg=1.0, events=self.events)
        self.assertEqual([m['separation_deg'] for m in matches], [0.3, 0.6])

    def test_wraps_right_ascension(self):
        matches = icecube_dr2_cone_search(0.1, 19.7, events=self.events)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]['ra'], 359.9)

    def test_datums_carry_event_identity_and_utc_time(self):
        matches = icecube_dr2_cone_search(10.0, 20.3, max_radius_deg=1.0, events=self.events)
        datum = IceCubeDR2DataService._build_neutrino_datums(matches)[0]
        self.assertEqual(datum['value']['filter'], ICECUBE_DR2_FILTER)
        self.assertEqual((datum['value']['run'], datum['value']['event']), (100000, 0))
        self.assertNotIn('mjd', datum['value'])
        self.assertEqual(datum['timestamp'].isoformat()[:19], '2011-05-19T02:57:46')

    def test_service_upserts_on_icecube_event_ids(self):
        self.assertEqual(IceCubeDR2DataService.upsert_identity_keys, ('run', 'event', 'subevent'))


class IceCubeDR2BundledTableTests(SimpleTestCase):
    def test_bundled_table_covers_the_full_release(self):
        events = load_icecube_dr2_events(ICECUBE_DR2_DATA_PATH)
        self.assertEqual(len(events['seasons']), 14)
        self.assertGreater(len(events['mjd']), 1_600_000)
        self.assertLess(events['mjd'].min(), 54600)  # April 2008
        self.assertGreater(events['mjd'].max(), 59700)  # May 2022


class IceCubeDR2AssociationTests(SimpleTestCase):
    uniform_background = 1 / (4 * np.pi)

    def level(self, separation_deg, ang_err_deg):
        return association_level(float(association_log10_sb(separation_deg, ang_err_deg, self.uniform_background)))

    def test_well_localised_event_on_target_is_high(self):
        self.assertEqual(self.level(0.1, 0.3), 'high')

    def test_degree_scale_error_is_low(self):
        self.assertEqual(self.level(0.5, 1.0), 'low')

    def test_poorly_localised_or_distant_event_is_low(self):
        self.assertEqual(self.level(2.5, 3.0), 'low')
        self.assertEqual(self.level(1.0, 10.0), 'low')

    def test_cone_search_reports_association(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = os.path.join(tmp.name, 'events.npz')
        _write_table(path, [(55700.0, 10.0, 20.0, 0.30, 3.0), (55701.0, 10.0, 21.5, 3.00, 2.5)])
        matches = icecube_dr2_cone_search(10.0, 20.1, events=load_icecube_dr2_events.__wrapped__(path))
        # A two-event table has no realistic background density, so check the ranking only.
        self.assertGreater(matches[0]['log10_sb'], matches[1]['log10_sb'])
        self.assertEqual(association_level(matches[1]['log10_sb']), 'low')
        self.assertNotIn('association', matches[0])
