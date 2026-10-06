"""DECaPS DR1 (DECam Plane Survey) per-exposure grizY photometry from Astro Data Lab.

DECaPS (Schlafly et al. 2018, ApJS 234, 39) imaged ~1000 deg^2 of the southern Galactic plane
with DECam in 2016-2017 with crowded-field PSF photometry, typically 10-30 detections per star
in grizY. Its DR1 measurement table (decaps_dr1.meas, 20 billion rows) is the per-epoch data;
DR2 published only an object catalogue.

Two quirks of the Data Lab tables:
1. meas.obj_id does not match object.obj_id, so measurements are found by a q3c cone search
   on the meas table and grouped by their own obj_id; the group nearest the target is used.
2. ~1/3 of rows have no zero point (uncalibrated exposures), hence no magnitude; they are skipped.

A measurement is kept when it is a DECaPS 'good' detection (no bad Community Pipeline flag at
the central pixel: flags == 1, i.e. 2**0; and qf >= 0.85, at least 85% of the PSF on good
pixels), has a calibrated magnitude and an error in (0, DECAPS_MAX_MAG_ERROR]. Magnitudes are AB;
the DR1 -> Scolnic et al. (2015) offsets recommended by Data Lab are added per band.
"""

import logging
import math
from datetime import timezone

import numpy as np
import pandas as pd
from astropy.time import Time

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target

from custom_code.data_services.forms import DECaPSQueryForm
from custom_code.data_services.nsc_dataservice import _datalab_query
from custom_code.data_services.service_utils import resolve_query_coordinates, upsert_reduced_datums

logger = logging.getLogger(__name__)

DECAPS_PAGE_URL = 'https://datalab.noirlab.edu/data/decaps'
DECAPS_RELEASE = 'DECaPS DR1'
# DECaPS PSF photometry in the crowded plane; ~1" seeing.
DECAPS_DEFAULT_RADIUS_ARCSEC = 1.5
DECAPS_MAX_MAG_ERROR = 0.5
DECAPS_MIN_QF = 0.85
# flags holds 2**(Community Pipeline data-quality code); code 0 (good pixel) -> 1.
DECAPS_GOOD_FLAGS = 1
# Offsets to add to DR1 magnitudes for the Scolnic et al. (2015) calibration (Data Lab DECaPS page).
DECAPS_SCOLNIC_OFFSETS = {'g': 0.020, 'r': 0.033, 'i': 0.024, 'z': 0.028, 'Y': 0.011}
DECAPS_FILTERS = {band: f'DECaPS({band})' for band in DECAPS_SCOLNIC_OFFSETS}

DECAPS_ACKNOWLEDGEMENT = (
    'This research uses data from the DECam Plane Survey (DECaPS; Schlafly et al. 2018, ApJS 234, '
    '39), obtained with the Dark Energy Camera (DECam), which was constructed by the Dark Energy '
    'Survey (DES) collaboration, and services provided by the Astro Data Lab at NSF NOIRLab.'
)


def _to_float(value):
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def _fetch_measurements(ra, dec, radius_arcsec):
    """Measurements of the DECaPS star nearest the position, with its separation in arcsec."""
    rows = _datalab_query(f"""
        SELECT decapsid, obj_id, ra, dec, mjd_obs, filterid, mag, mag_err, flags, qf, chip_id,
               q3c_dist(ra, dec, {ra}, {dec}) * 3600 AS dist_arcsec
        FROM decaps_dr1.meas
        WHERE q3c_radial_query(ra, dec, {ra}, {dec}, {radius_arcsec / 3600.0})
    """)
    if rows is None or rows.empty:
        return None, None
    rows['obj_id'] = rows['obj_id'].astype(str)
    # Each star's detections share a meas obj_id; pick the star whose mean position is nearest.
    nearest = rows.groupby('obj_id')['dist_arcsec'].median().idxmin()
    star = rows[rows['obj_id'] == nearest]
    return star, float(star['dist_arcsec'].median())


class DECaPSDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return DECAPS_DEFAULT_RADIUS_ARCSEC

    name = 'DECaPS'
    verbose_name = 'DECaPS DR1 (Galactic plane, DECam)'
    # DR1 is a fixed release.
    update_on_daily_refresh = False
    info_url = DECAPS_PAGE_URL
    acknowledgement = DECAPS_ACKNOWLEDGEMENT
    upsert_identity_keys = ('filter', 'decaps_id')
    service_notes = (
        'Query DECaPS DR1 (southern Galactic plane, DECam, 2016-2017) per-exposure grizY photometry '
        'from Astro Data Lab by coordinates. The nearest DECaPS star within 1.5 arcsec is used; only '
        'good, calibrated detections (clean pixel, >= 85% of PSF on good pixels, error <= 0.5 mag) are '
        'imported, in AB magnitudes on the Scolnic et al. (2015) calibration.'
    )

    @classmethod
    def get_form_class(cls):
        return DECaPSQueryForm

    @classmethod
    def get_acknowledgement(cls):
        return cls.acknowledgement

    def build_query_parameters(self, parameters, **kwargs):
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or self.get_finding_chart_radius_arcsec(),
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or self.get_finding_chart_radius_arcsec()

        measurements, separation = None, None
        if ra is not None and dec is not None and query_parameters.get('include_photometry', True):
            try:
                measurements, separation = _fetch_measurements(ra, dec, radius_arcsec)
                if measurements is None:
                    logger.debug('DECaPS returned no detection for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('DECaPS query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                measurements, separation = None, None

        self.query_results = {
            'measurements': measurements,
            'separation_arcsec': separation,
            'source_location': DECAPS_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        measurements = data.get('measurements')
        if data.get('ra') is None or data.get('dec') is None or measurements is None:
            return []
        datums = self._build_photometry_datums(measurements, data.get('separation_arcsec'))
        if not datums:
            return []
        return [{
            'name': f"DECaPS_{measurements['obj_id'].iloc[0]}",
            'ra': data['ra'],
            'dec': data['dec'],
            'aliases': [],
            'reduced_datums': {'photometry': datums},
            'source_location': data.get('source_location'),
        }]

    def create_target_from_query(self, target_result, **kwargs):
        return Target(
            name=target_result['name'],
            type='SIDEREAL',
            ra=target_result.get('ra'),
            dec=target_result.get('dec'),
            epoch=2000.0,
        )

    def create_aliases_from_query(self, alias_results, **kwargs):
        return []

    def create_reduced_datums_from_query(self, target, data=None, data_type=None, **kwargs):
        if data_type != 'photometry' or not data:
            return 0
        created, _updated = upsert_reduced_datums(
            target=target,
            data_type='photometry',
            source_name=self.name,
            source_location=kwargs.get('source_location') or self.info_url,
            datums=data,
            identity_keys=self.upsert_identity_keys,
        )
        return created

    def to_reduced_datums(self, target, data_results=None, **kwargs):
        if not data_results:
            return
        for data_type, data in data_results.items():
            self.create_reduced_datums_from_query(
                target,
                data=data,
                data_type=data_type,
                source_location=(getattr(self, 'query_results', {}) or {}).get('source_location'),
            )

    def _build_photometry_datums(self, measurements, separation):
        numeric = measurements[['mjd_obs', 'mag', 'mag_err', 'flags', 'qf']].apply(pd.to_numeric, errors='coerce')
        band = measurements['filterid'].astype(str).str.strip()
        good = (
            np.isfinite(numeric).all(axis=1)
            & band.isin(DECAPS_FILTERS)
            & (numeric['flags'] == DECAPS_GOOD_FLAGS)
            & (numeric['qf'] >= DECAPS_MIN_QF)
            & (numeric['mag_err'] > 0) & (numeric['mag_err'] <= DECAPS_MAX_MAG_ERROR)
            & (numeric['mag'] > 0) & (numeric['mag'] < 30)
        )
        output = []
        for index in measurements.index[good]:
            filter_band = band[index]
            mjd = float(numeric.at[index, 'mjd_obs'])
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {
                    'filter': DECAPS_FILTERS[filter_band],
                    'magnitude': round(float(numeric.at[index, 'mag']) + DECAPS_SCOLNIC_OFFSETS[filter_band], 4),
                    'error': round(float(numeric.at[index, 'mag_err']), 4),
                    'mjd': mjd,
                    'mag_system': 'AB',
                    'decaps_id': str(measurements.at[index, 'decapsid']),
                    'decaps_obj_id': str(measurements.at[index, 'obj_id']),
                    'data_release': DECAPS_RELEASE,
                    'match_separation_arcsec': round(separation, 3) if separation is not None else None,
                },
            })
        return output
