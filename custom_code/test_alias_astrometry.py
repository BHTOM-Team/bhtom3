import math
import unittest

from custom_code.alias_astrometry import (
    alias_offset, clean_matched_position, matched_position,
    position_from_error_ellipse, position_from_samples,
)


class AliasAstrometryTests(unittest.TestCase):
    def test_ra_offset_includes_declination_and_ra_wraparound(self):
        result = alias_offset(359.999, 60, matched_position(0.001, 60))
        self.assertAlmostEqual(result['offset_arcsec'], 3.6, places=6)

    def test_separation_matches_astropy_at_poles_and_across_sky(self):
        from astropy.coordinates import SkyCoord
        for ra1, dec1, ra2, dec2 in (
            (359.999, 89.9999, 0.001, 89.9998),
            (10, -89.999, 270, -89.998), (0, 0, 180, 0),
            (13.7, 23.9, 234, -76), (0, 90, 130, 90),
        ):
            with self.subTest(coordinates=(ra1, dec1, ra2, dec2)):
                expected = SkyCoord(ra1, dec1, unit='deg').separation(SkyCoord(ra2, dec2, unit='deg')).arcsec
                self.assertAlmostEqual(alias_offset(ra1, dec1, matched_position(ra2, dec2))['offset_arcsec'], expected, places=7)

    def test_uncertainty_is_projected_along_separation(self):
        position = matched_position(0, 0, 0.2, 0.4)
        self.assertAlmostEqual(alias_offset(1 / 3600, 0, position)['offset_error_arcsec'], 0.2)
        self.assertAlmostEqual(alias_offset(0, 1 / 3600, position)['offset_error_arcsec'], 0.4)

    def test_coordinate_covariance_is_included(self):
        position = matched_position(0, 0, 0.2, 0.2, 0.5)
        self.assertAlmostEqual(alias_offset(1 / 3600, 1 / 3600, position)['offset_error_arcsec'], math.sqrt(0.06), places=7)

    def test_zero_offset_keeps_error(self):
        result = alias_offset(12, 34, matched_position(12, 34, 0.3, 0.4))
        self.assertEqual(result['offset_arcsec'], 0)
        self.assertAlmostEqual(result['offset_error_arcsec'], 0.5)

    def test_missing_and_invalid_positions_never_become_zero_offsets(self):
        for position in ({}, None, {'ra': 'nan', 'dec': 0}, {'ra': 0, 'dec': 91}, {'ra': True, 'dec': 0}):
            self.assertIsNone(alias_offset(0, 0, position)['offset_arcsec'])
        self.assertIsNone(alias_offset(None, 0, matched_position(0, 0))['offset_arcsec'])

    def test_missing_errors_do_not_use_photometric_error_or_match_radius(self):
        position = {'ra': 0, 'dec': 0, 'error': 0.01, 'radius_arcsec': 3, 'ra_error_arcsec': -1}
        self.assertIsNone(alias_offset(1, 1, position)['offset_error_arcsec'])

    def test_spherical_mean_wraps_and_scatter_is_not_error_on_mean(self):
        position = position_from_samples([(359.999, 60), (0.001, 60)])
        self.assertTrue(position['ra'] < 1e-8 or position['ra'] > 360 - 1e-8)
        self.assertAlmostEqual(position['ra_error_arcsec'], 3.6 / math.sqrt(2), places=6)
        self.assertEqual(position['error_kind'], 'scatter')
        self.assertEqual(clean_matched_position(position), position)

    def test_single_or_identical_positions_have_unknown_uncertainty(self):
        for samples in ([(12, 34)], [(12, 34)] * 10):
            position = position_from_samples(samples)
            self.assertNotIn('ra_error_arcsec', position)
        self.assertEqual(position_from_samples([(None, 0), (0, float('inf'))]), {})

    def test_error_ellipse_units_and_orientation(self):
        position = position_from_error_ellipse(0, 0, 400, 200, 0)
        self.assertAlmostEqual(alias_offset(0, 1 / 3600, position)['offset_error_arcsec'], 0.4)
        self.assertAlmostEqual(alias_offset(1 / 3600, 0, position)['offset_error_arcsec'], 0.2)
        diagonal = position_from_error_ellipse(0, 0, 400, 200, 45)
        self.assertAlmostEqual(alias_offset(1 / 3600, 1 / 3600, diagonal)['offset_error_arcsec'], 0.4)


if __name__ == '__main__':
    unittest.main()
