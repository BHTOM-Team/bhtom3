"""Kepler (2009-2013) long-cadence light curves from MAST, converted to Kepler magnitudes.

The original Kepler mission observed ~200,000 stars in one 115 deg^2 field in Cygnus-Lyra for
17 quarters at 29.4 min cadence. MAST holds one SPOC light-curve file per target and quarter
(kplr<KIC>-<time>_llc.fits). This module is shared with the K2 service (k2_dataservice), which
uses the same detector, pipeline and file format.

Conversion. The pre-launch handbook zero point (Kp = 12 for 1.74e5 e-/s) makes stars ~0.2-0.3 mag
too bright against their catalogue Kp, so the zero point was measured instead:
    ZP = Kp(catalogue) + 2.5 log10(median quality-0 flux)
over 25 Kepler and 21 K2 stars (Kp 11-16): PDCSAP 25.45 (scatter 0.05) for Kepler and 25.32
(0.07) for K2; SAP 25.27 / 25.26 (0.1). PDCSAP is the default: it corrects for crowding and for
the fraction of starlight outside the aperture, and removes most quarter-to-quarter jumps, but
also part of slow intrinsic trends; SAP keeps those, with the systematics.

Every quality-0 cadence is stored as one magnitude (no binning). Times are BKJD = BJD_TDB -
2454833, so MJD = BKJD + 54832.5, barycentric TDB as for TESS. Only the nearest Kepler target
within the radius is used, since apertures (4"/pixel) are blended for close neighbours.
"""

import logging
import os
import tempfile
from datetime import timezone

import astropy.units as u
import numpy as np
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.time import Time
from django.conf import settings

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import KeplerQueryForm
from custom_code.data_services.service_utils import upsert_reduced_datums
from custom_code.data_services.tess_dataservice import pick_flux_column

logger = logging.getLogger(__name__)

BKJD_TO_MJD = 54832.5  # 2454833 - 2400000.5
MAG_ERR_FACTOR = 1.0857362  # 2.5/ln(10)
# Rows per upsert call: the existing-row lookup is one timestamp IN (...) query.
UPSERT_CHUNK = 5000


def _to_float(value):
    try:
        if value is None or np.ma.is_masked(value):
            return None
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if np.isfinite(value) else None


def _mast_download_dir():
    configured = getattr(settings, 'TESS_MAST_CACHE_DIR', None)
    path = configured or os.path.join(tempfile.gettempdir(), 'bhtom_tess_mast')
    os.makedirs(path, exist_ok=True)
    return path


class KeplerMissionDataService(DataService):
    """Shared Kepler/K2 behaviour; subclasses set the mission constants below."""

    mission = 'Kepler'
    obs_collection = 'Kepler'
    target_prefix = 'kplr'
    long_cadence_marker = '_lc'
    filter_name = 'Kepler(Kp)'
    alias_prefix = 'KIC'
    zero_points = {'pdcsap': 25.45, 'sap': 25.27}
    segment_header = 'QUARTER'
    default_radius_arcsec = 4.0

    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return cls.default_radius_arcsec

    name = 'Kepler'
    verbose_name = 'Kepler (2009-2013)'
    update_on_daily_refresh = False
    info_url = 'https://archive.stsci.edu/missions-and-data/kepler'
    acknowledgement = (
        'This paper includes data collected by the Kepler mission and obtained from the MAST data '
        'archive at the Space Telescope Science Institute (STScI). Funding for the Kepler mission '
        'is provided by the NASA Science Mission Directorate. STScI is operated by the Association '
        'of Universities for Research in Astronomy, Inc., under NASA contract NAS 5-26555.'
    )
    upsert_identity_keys = ('filter',)
    service_notes = (
        'Query public Kepler long-cadence (29.4 min) light curves from MAST by coordinates. The '
        'nearest Kepler target within 4 arcsec is used; every good cadence of every quarter is '
        'converted to Kepler magnitude (PDCSAP by default, Kp = 25.45 - 2.5 log10 flux, calibrated '
        'against catalogue Kp to ~0.05 mag). Times are barycentric TDB.'
    )

    @classmethod
    def get_form_class(cls):
        return KeplerQueryForm

    @classmethod
    def get_acknowledgement(cls):
        return cls.acknowledgement

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or self.get_finding_chart_radius_arcsec(),
            'flux_type': parameters.get('flux_type') or 'pdcsap',
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def _nearest_target(self, observations, ra, dec):
        """target_name of the nearest mission target with a long-cadence light curve."""
        position = SkyCoord(ra, dec, unit='deg')
        best = None
        for row in observations:
            name = str(row['target_name']).strip()
            obs_id = str(row['obs_id'])
            if not name.startswith(self.target_prefix) or self.long_cadence_marker not in obs_id:
                continue
            row_ra, row_dec = _to_float(row['s_ra']), _to_float(row['s_dec'])
            if row_ra is None or row_dec is None:
                continue
            separation = SkyCoord(row_ra, row_dec, unit='deg').separation(position).arcsec
            if best is None or separation < best[0]:
                best = (separation, name)
        return best

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or self.get_finding_chart_radius_arcsec()
        flux_type = query_parameters.get('flux_type') or 'pdcsap'

        paths, target, separation = [], None, None
        if ra is not None and dec is not None and query_parameters.get('include_photometry', True):
            try:
                from astroquery.mast import Observations

                observations = Observations.query_criteria(
                    coordinates=SkyCoord(ra, dec, unit='deg'),
                    radius=radius_arcsec * u.arcsec,
                    obs_collection=self.obs_collection,
                    dataproduct_type='timeseries',
                )
                nearest = self._nearest_target(observations, ra, dec)
                if nearest:
                    separation, target = nearest
                    selected = observations[[
                        str(row['target_name']).strip() == target and self.long_cadence_marker in str(row['obs_id'])
                        for row in observations
                    ]]
                    products = Observations.filter_products(
                        Observations.get_product_list(selected), productSubGroupDescription='LLC',
                    )
                    # Keep the mission's own files, not community (HLSP) reprocessings.
                    products = products[[str(f).startswith(self.target_prefix) for f in products['productFilename']]]
                    if len(products):
                        manifest = Observations.download_products(products, cache=True, download_dir=_mast_download_dir())
                        paths = [str(path) for path in manifest['Local Path']]
                else:
                    logger.debug('%s returned no light curve for RA=%s Dec=%s', self.mission, ra, dec)
            except Exception as exc:
                logger.warning('%s MAST query failed for RA=%s Dec=%s: %s', self.mission, ra, dec, exc)
                paths, target = [], None

        self.query_results = {
            'paths': paths,
            'target': target,
            'separation_arcsec': separation,
            'flux_type': flux_type,
            'source_location': self.info_url,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def _alias(self, target):
        return f'{self.alias_prefix}_{int(target[len(self.target_prefix):])}'

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        target = data.get('target')
        if data.get('ra') is None or data.get('dec') is None or not target or not data.get('paths'):
            return []
        datums = []
        for path in sorted(data['paths']):
            try:
                datums.extend(self._datums_from_lightcurve(path, data['flux_type'], data.get('separation_arcsec')))
            except Exception as exc:
                logger.warning('%s: failed to process %s: %s', self.mission, os.path.basename(path), exc)
        if not datums:
            return []
        alias = self._alias(target)
        return [{
            'name': alias,
            'ra': data['ra'],
            'dec': data['dec'],
            'aliases': [alias],
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
        return [TargetName(name=alias) for alias in alias_results]

    def create_reduced_datums_from_query(self, target, data=None, data_type=None, **kwargs):
        if data_type != 'photometry' or not data:
            return 0
        created = 0
        for start in range(0, len(data), UPSERT_CHUNK):
            chunk_created, _updated = upsert_reduced_datums(
                target=target,
                data_type='photometry',
                source_name=self.name,
                source_location=kwargs.get('source_location') or self.info_url,
                datums=data[start:start + UPSERT_CHUNK],
                identity_keys=self.upsert_identity_keys,
            )
            created += chunk_created
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

    def _datums_from_lightcurve(self, path, flux_type, separation):
        """One magnitude per quality-0 cadence of one quarter/campaign file."""
        with fits.open(path, memmap=False) as hdul:
            primary = hdul[0].header
            header = hdul['LIGHTCURVE'].header
            data = hdul['LIGHTCURVE'].data
            segment = primary.get(self.segment_header)
            column, why = pick_flux_column(data.columns, header, prefer=flux_type)
            if column is None:
                logger.warning('%s: %s %s has no e-/s flux column (%s)', self.mission, self.segment_header, segment, why)
                return []
            time = np.asarray(data['TIME'], float)  # BKJD
            flux = np.asarray(data[column], float)
            err_column = f'{column}_ERR'
            flux_err = np.asarray(data[err_column], float) if err_column in data.columns.names else np.full_like(flux, np.nan)
            quality = np.asarray(data['SAP_QUALITY'], int) if 'SAP_QUALITY' in data.columns.names else np.zeros_like(flux, int)
        zero_point = self.zero_points['sap' if column == 'SAP_FLUX' else 'pdcsap']
        good = np.isfinite(time) & np.isfinite(flux) & (flux > 0) & (quality == 0)
        output = []
        for t, f, e in zip(time[good], flux[good], flux_err[good]):
            magnitude = zero_point - 2.5 * np.log10(f)
            error = MAG_ERR_FACTOR * e / f if np.isfinite(e) and e > 0 else None
            if error is None or error > 3:
                continue
            output.append({
                'timestamp': Time(float(t) + BKJD_TO_MJD, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {
                    'filter': self.filter_name,
                    'magnitude': round(float(magnitude), 5),
                    'error': round(float(error), 6),
                    'flux_type': column,
                    'zero_point': zero_point,
                    'segment': segment,
                    'time_scale': 'BJD_TDB',
                },
            })
        return output


class KeplerDataService(KeplerMissionDataService):
    pass
