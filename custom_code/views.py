from io import StringIO
import json
import logging
import re
import requests
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from uuid import uuid4

from astroquery.jplhorizons import Horizons
from astroquery.mpc import MPC
from django.contrib import messages
from django.conf import settings
from django.contrib.auth import logout
from django.contrib.auth.models import Group
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.sites.shortcuts import get_current_site
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.http import HttpResponseRedirect, JsonResponse
from django.shortcuts import redirect
from django.shortcuts import resolve_url
from django.shortcuts import render
from django.views.generic import FormView, ListView, RedirectView, TemplateView
from django.views import View
from django.urls import reverse, reverse_lazy
from django.db.models import Q
from django.db import transaction
from django_comments.models import Comment

from tom_common.hints import add_hint
from tom_common.hooks import run_hook
from tom_catalogs.harvester import MissingDataException
from tom_targets.forms import TargetExtraFormset
from tom_targets.models import Target
from tom_targets.views import TargetCreateView, TargetDetailView, TargetListView, TargetUpdateView

from custom_code.filters import BhtomTargetFilterSet
from custom_code.forms import (
    BhtomCatalogQueryForm,
    BhtomNonSiderealTargetCreateForm,
    BhtomSiderealTargetCreateForm,
    BhtomTargetNamesFormset,
    GeoTomAddSatForm,
    PublicUploadAccessForm,
    PublicFitsUploadForm,
)
from custom_code.models import GeoTarget
from custom_code.geosat import (
    altaz_to_hadec_point,
    convert_altaz_curve_to_hadec,
    geosat_alt_az,
    geosat_alt_az_from_tle,
    sun_visibility_curve,
)
from custom_code.data_services.geosat_dataservice import GeoSatDataService
from custom_code.tasks import enqueue_target_dataservices_update
from custom_code.bhtom_catalogs.harvesters import gaia_alerts as gaia_alerts_harvester
from custom_code.bhtom_catalogs.harvesters import gaia_dr3 as gaia_dr3_harvester
from custom_code.bhtom_catalogs.harvesters import simbad as simbad_harvester
from tom_dataproducts.views import DataProductUploadView


logger = logging.getLogger(__name__)
CATALOG_RESULTS_SESSION_KEY = 'catalog_query_results'
CATALOG_FORM_SESSION_KEY = 'catalog_query_form_data'
PUBLIC_UPLOAD_CACHE_TIMEOUT = 24 * 60 * 60
PUBLIC_UPLOAD_PAGE_SIZE = 200
PUBLIC_UPLOAD_SESSION_KEY = 'public_upload_access_granted'


def _bhtom2_api_configured():
    return bool(getattr(settings, 'BHTOM2_API_BASE_URL', '').strip() and getattr(settings, 'BHTOM2_API_TOKEN', '').strip())


def _public_upload_password_enabled():
    return bool(getattr(settings, 'PUBLIC_UPLOAD_PASSWORD', ''))


def _public_upload_has_access(request):
    if not _public_upload_password_enabled():
        return True
    return bool(request.session.get(PUBLIC_UPLOAD_SESSION_KEY))


def _bhtom2_api_headers(token=''):
    auth_token = str(token or getattr(settings, 'BHTOM2_API_TOKEN', '')).strip()
    return {
        'Authorization': f'Token {auth_token}',
        'Content-Type': 'application/json',
    }


def _bhtom2_api_url(path):
    return f"{settings.BHTOM2_API_BASE_URL.rstrip('/')}/{path.lstrip('/')}"


def _bhtom2_response_records(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []

    for key in ('results', 'data', 'items', 'rows', 'observatories', 'targets', 'users'):
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return []


def _load_bhtom2_catalog(cache_key, endpoint, normalizer, token=''):
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    if not _bhtom2_api_configured():
        raise RuntimeError('BHTOM2 API is not configured.')

    catalog = []
    page = 1
    seen_pages = set()
    timeout = getattr(settings, 'BHTOM2_API_TIMEOUT', 30)

    while page not in seen_pages:
        seen_pages.add(page)
        response = requests.post(
            _bhtom2_api_url(endpoint),
            json={'page': page},
            headers=_bhtom2_api_headers(token=token),
            timeout=timeout,
        )
        response.raise_for_status()
        rows = _bhtom2_response_records(response.json())
        if not rows:
            break
        for row in rows:
            normalized = normalizer(row)
            if isinstance(normalized, (list, tuple)):
                catalog.extend(normalized)
            else:
                catalog.append(normalized)
        if len(rows) < PUBLIC_UPLOAD_PAGE_SIZE:
            break
        page += 1

    catalog = [row for row in catalog if row]
    if cache_key == 'public_upload_observatories':
        deduped_catalog = []
        seen_values = set()
        for row in catalog:
            value_key = row['value'].lower()
            if value_key in seen_values:
                continue
            seen_values.add(value_key)
            deduped_catalog.append(row)
        catalog = deduped_catalog
    cache.set(cache_key, catalog, PUBLIC_UPLOAD_CACHE_TIMEOUT)
    return catalog


def _normalize_public_upload_target(row):
    name = str(row.get('name') or '').strip()
    if not name:
        return None
    return {
        'label': name,
        'value': name,
        'search': name.lower(),
    }


def _normalize_public_upload_observer(row):
    username = str(row.get('username') or '').strip()
    if not username:
        return None
    first_name = str(row.get('first_name') or '').strip()
    last_name = str(row.get('last_name') or '').strip()
    full_name = ' '.join(part for part in (first_name, last_name) if part).strip()
    if full_name:
        label = f'{full_name} ({username})'
    else:
        label = username
    search_terms = [username, full_name, str(row.get('email') or '').strip()]
    return {
        'label': label,
        'value': username,
        'search': ' '.join(term.lower() for term in search_terms if term),
    }


def _public_upload_observatory_name(row):
    return str(
        row.get('obsName')
        or row.get('observatory')
        or row.get('observatory_name')
        or row.get('name')
        or ''
    ).strip()


def _public_upload_camera_entries(row):
    camera_entries = []
    for key in ('cameras', 'camera_list', 'cameraMatrix', 'camera_matrix'):
        value = row.get(key)
        if isinstance(value, list):
            camera_entries.extend(item for item in value if isinstance(item, dict))

    direct_camera = {
        'oname': row.get('oname') or row.get('prefix'),
        'camera': row.get('camera') or row.get('camera_name') or row.get('cameraName'),
    }
    if direct_camera['oname'] or direct_camera['camera']:
        camera_entries.append(direct_camera)

    return camera_entries


def _normalize_public_upload_observatory(row):
    observatory_name = _public_upload_observatory_name(row)
    camera_entries = _public_upload_camera_entries(row)
    normalized = []

    if not camera_entries:
        oname = str(row.get('oname') or row.get('prefix') or '').strip()
        if not oname:
            return None
        label = f'{observatory_name} ({oname})' if observatory_name else oname
        return {
            'label': label,
            'value': oname,
            'search': ' '.join(term.lower() for term in (observatory_name, oname) if term),
        }

    for camera in camera_entries:
        oname = str(camera.get('oname') or camera.get('prefix') or '').strip()
        camera_name = str(camera.get('camera') or camera.get('camera_name') or camera.get('name') or '').strip()
        if not oname:
            continue
        label_name = observatory_name or camera_name
        label = f'{label_name} ({oname})' if label_name else oname
        search_terms = [observatory_name, camera_name, oname]
        normalized.append({
            'label': label,
            'value': oname,
            'search': ' '.join(term.lower() for term in search_terms if term),
        })

    return normalized or None


def _public_upload_target_choices():
    return _load_bhtom2_catalog(
        'public_upload_targets',
        'targets/getTargetList/',
        _normalize_public_upload_target,
    )


def _public_upload_observer_choices():
    return _load_bhtom2_catalog(
        'public_upload_observers',
        'common/api/users/',
        _normalize_public_upload_observer,
    )


def _public_upload_observatory_choices():
    return _load_bhtom2_catalog(
        'public_upload_observatories',
        'observatory/getObservatoryList/',
        _normalize_public_upload_observatory,
    )


def _filter_public_upload_choices(choices, query):
    needle = str(query or '').strip().lower()
    if not needle:
        return choices[:20]
    return [choice for choice in choices if needle in choice['search']][:20]


def _normalize_public_upload_input(value):
    return str(value or '').strip()


def _match_public_upload_choice(choices, submitted_value):
    needle = str(submitted_value or '').strip().lower()
    if not needle:
        return None
    for choice in choices:
        if choice['value'].lower() == needle:
            return choice['value']
        if choice['label'].lower() == needle:
            return choice['value']
    return None


class BhtomPallasBaseMixin:
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update({
            'bhtom_pallas_active_tab': getattr(self, 'bhtom_pallas_active_tab', 'overview'),
        })
        return context


class BhtomPallasView(BhtomPallasBaseMixin, TemplateView):
    template_name = 'tom_common/bhtom_pallas_overview.html'
    bhtom_pallas_active_tab = 'overview'


class BhtomPallasEphemerisView(BhtomPallasBaseMixin, TemplateView):
    template_name = 'tom_common/bhtom_pallas_ephemeris.html'
    bhtom_pallas_active_tab = 'ephemeris'
    FULL_OBSERVER_QUANTITIES = ','.join(str(index) for index in range(1, 44))
    DEFAULT_VISIBLE_FIELD_IDS = [
        'datetime',
        'ra',
        'dec',
        'visual_mag',
        'airmass',
        'heliocentric_distance',
        'geocentric_distance',
        'phase_angle',
    ]
    FIELD_DEFINITIONS = {
        'datetime': 'epoch (str, `Date__(UT)__HR:MN:SC.fff`)',
        'ra': 'target RA (float, deg, `DEC_(XXX)`)',
        'dec': 'target DEC (float, deg, `DEC_(XXX)`)',
        'ra_app': 'target apparent RA (float, deg, `R.A._(a-app)`)',
        'dec_app': 'target apparent DEC (float, deg, `DEC_(a-app)`) ',
        'visual_mag': 'V magnitude (float, mag, `APmag`); comet Total magnitude (float, mag, `T-mag`); comet Nucleus magnitude (float, mag, `N-mag`)',
        'ra_rate': 'target rate RA (float, arcsec/hr, `RA*cosD`)',
        'dec_rate': 'target DEC rate (float, arcsec/hr, `d(DEC)/dt`)',
        'azimuth': 'Azimuth (float, deg, EoN, `Azi_(a-app)`)',
        'elevation': 'Elevation (float, deg, `Elev_(a-app)`)',
        'azimuth_rate': 'Azimuth rate (float, arcsec/minute, `dAZ*cosE`)',
        'elevation_rate': 'Elevation rate (float, arcsec/minute, `d(ELV)/dt`)',
        'sat_x': 'satellite X position (arcsec, `X_(sat-prim)`)',
        'sat_y': 'satellite Y position (arcsec, `Y_(sat-prim)`)',
        'sat_pang': 'satellite position angle (deg, `SatPANG`)',
        'sidereal_time': 'local apparent sidereal time (str, `L_Ap_Sid_Time`)',
        'airmass': 'target optical airmass (float, `a-mass`)',
        'extinction': 'V-mag extinction (float, mag, `mag_ex`)',
        'illumination': 'fraction of illumination (float, percent, `Illu%`)',
        'illumination_defect': 'defect of illumination (float, arcsec, `Dec_illu`)',
        'sat_sep': 'target-primary angular separation (float, arcsec, `ang-sep`)',
        'sat_vis': 'target-primary visibility (str, `v`)',
        'angular_width': 'angular width of target (float, arcsec, `Ang-diam`)',
        'observer_sub_lon': 'apparent planetodetic longitude (float, deg, `ObsSub-LON`)',
        'observer_sub_lat': 'apparent planetodetic latitude (float, deg, `ObsSub-LAT`)',
        'subsolar_lon': 'subsolar planetodetic longitude (float, deg, `SunSub-LON`)',
        'subsolar_lat': 'subsolar planetodetic latitude (float, deg, `SunSub-LAT`)',
        'subsolar_angle': 'target sub-solar point position angle (float, deg, `SN.ang`)',
        'subsolar_distance': 'target sub-solar point position angle distance (float, arcsec, `SN.dist`)',
        'north_pole_angle': "target's North Pole position angle (float, deg, `NP.ang`)",
        'north_pole_distance': "target's North Pole position angle distance (float, arcsec, `NP.dist`)",
        'heliocentric_ecl_lon': 'heliocentric ecliptic longitude (float, deg, `hEcl-Lon`)',
        'heliocentric_ecl_lat': 'heliocentric ecliptic latitude (float, deg, `hEcl-Lat`)',
        'observer_ecl_lon': 'observer-centric ecliptic longitude (float, deg, `ObsEcLon`)',
        'observer_ecl_lat': 'observer-centric ecliptic latitude (float, deg, `ObsEcLat`)',
        'heliocentric_distance': 'heliocentric distance (float, au, `r`)',
        'heliocentric_radial_rate': 'heliocentric radial rate (float, km/s, `rdot`)',
        'geocentric_distance': 'distance from observer (float, au, `delta`)',
        'geocentric_radial_rate': 'observer-centric radial rate (float, km/s, `deldot`)',
        'lighttime': 'one-way light time (float, min, `1-way_LT`)',
        'velocity_sun': 'target center velocity wrt Sun (float, km/s, `VmagSn`)',
        'velocity_observer': 'target center velocity wrt Observer (float, km/s, `VmagOb`)',
        'elongation': 'solar elongation (float, deg, `S-O-T`)',
        'elongation_flag': 'apparent position relative to Sun (str, `/r`)',
        'phase_angle': 'solar phase angle (float, deg, `S-T-O`)',
        'lunar_elongation': 'apparent lunar elongation angle wrt target (float, deg, `T-O-M`)',
        'lunar_illumination': 'lunar illumination percentage (float, percent, `MN_Illu%`)',
        'interfering_body_elong': 'apparent interfering body elongation angle wrt target (float, deg, `T-O-I`)',
        'interfering_body_illum': 'interfering body illumination percentage (float, percent, `IB_Illu%`)',
        'satellite_phase_angle': 'observer-primary-target angle (float, deg, `O-P-T`)',
        'orbital_plane_angle': 'orbital plane angle (float, deg, `PlAng`)',
        'sun_target_pa': '-Sun vector PA (float, deg, EoN, `PsAng`)',
        'velocity_pa': '-velocity vector PA (float, deg, EoN, `PsAMV`)',
        'constellation': 'constellation ID containing target (str, `Cnst`)',
        'tdb_minus_ut': 'difference between TDB and UT (float, seconds, `TDB-UT`)',
        'north_pole_ra': "target's North Pole RA (float, deg, `N.Pole-RA`)",
        'north_pole_dec': "target's North Pole DEC (float, deg, `N.Pole-DC`)",
        'galactic_longitude': 'galactic longitude (float, deg, `GlxLon`)',
        'galactic_latitude': 'galactic latitude (float, deg, `GlxLat`)',
        'solar_time': 'local apparent solar time (str, `L_Ap_SOL_Time`)',
        'earth_lighttime': 'observer lighttime from center of Earth (float, minutes, `399_ins_LT`)',
        'ra_3sigma': '3 sigma positional uncertainty in RA (float, arcsec, `RA_3sigma`)',
        'dec_3sigma': '3 sigma positional uncertainty in DEC (float, arcsec, `DEC_3sigma`)',
        'smaa_3sigma': '3 sigma positional uncertainty ellipse semi-major axis (float, arcsec, `SMAA_3sig`)',
        'smia_3sigma': '3 sigma positional uncertainty ellipse semi-minor axis (float, arcsec, `SMIA_3sig`)',
        'theta_3sigma': 'position uncertainty ellipse position angle (float, deg, `Theta`)',
        'area_3sigma': '3 sigma positional uncertainty ellipse area (float, arcsec^2, `Area_3sig`)',
        'rss_3sigma': '3 sigma positional uncertainty ellipse root-sum-square (float, arcsec, `POS_3sigma`)',
        'range_3sigma': '3 sigma range uncertainty (float, km, `RNG_3sigma`)',
        'range_rate_3sigma': '3 sigma range rate uncertainty (float, km/second, `RNGRT_3sigma`)',
        'sband_3sigma': '3 sigma Doppler radar uncertainties at S-band (float, Hertz, `DOP_S_3sig`)',
        'xband_3sigma': '3 sigma Doppler radar uncertainties at X-band (float, Hertz, `DOP_X_3sig`)',
        'doppdelay_3sigma': '3 sigma Doppler radar round-trip delay uncertainty (float, second, `RT_delay_3sig`)',
        'true_anomaly': 'True Anomaly (float, deg, `Tru_Anom`)',
        'hour_angle': 'local apparent hour angle (float, hour, `L_Ap_Hour_Ang`)',
        'true_phase_angle': 'true phase angle (float, deg, `phi`)',
        'pab_lon': 'phase angle bisector longitude (float, deg, `PAB-LON`)',
        'pab_lat': 'phase angle bisector latitude (float, deg, `PAB-LAT`)',
    }
    FIELD_CHOICES = [
        {'id': 'datetime', 'label': 'Datetime', 'column': 'datetime_str', 'quantity': None, 'default': True},
        {'id': 'ra', 'label': 'RA', 'column': 'RA', 'quantity': '1', 'default': True},
        {'id': 'dec', 'label': 'DEC', 'column': 'DEC', 'quantity': '1', 'default': True},
        {'id': 'ra_app', 'label': 'Apparent RA', 'column': 'RA_app', 'quantity': 'ALL', 'default': False},
        {'id': 'dec_app', 'label': 'Apparent DEC', 'column': 'DEC_app', 'quantity': 'ALL', 'default': False},
        {'id': 'visual_mag', 'label': 'Visual mag. & surface brightness', 'quantity': '9', 'default': True},
        {'id': 'ra_rate', 'label': 'RA rate', 'column': 'RA_rate', 'quantity': '3', 'default': False},
        {'id': 'dec_rate', 'label': 'DEC rate', 'column': 'DEC_rate', 'quantity': '3', 'default': False},
        {'id': 'azimuth', 'label': 'Azimuth', 'column': 'AZ', 'quantity': 'ALL', 'default': False},
        {'id': 'elevation', 'label': 'Elevation', 'column': 'EL', 'quantity': 'ALL', 'default': False},
        {'id': 'azimuth_rate', 'label': 'Azimuth rate', 'column': 'AZ_rate', 'quantity': 'ALL', 'default': False},
        {'id': 'elevation_rate', 'label': 'Elevation rate', 'column': 'EL_rate', 'quantity': 'ALL', 'default': False},
        {'id': 'sat_x', 'label': 'Satellite X', 'column': 'sat_X', 'quantity': 'ALL', 'default': False},
        {'id': 'sat_y', 'label': 'Satellite Y', 'column': 'sat_Y', 'quantity': 'ALL', 'default': False},
        {'id': 'sat_pang', 'label': 'Satellite P.A.', 'column': 'sat_PANG', 'quantity': 'ALL', 'default': False},
        {'id': 'sidereal_time', 'label': 'Sidereal time', 'column': 'siderealtime', 'quantity': '7', 'default': False},
        {'id': 'airmass', 'label': 'Airmass', 'column': 'airmass', 'quantity': '8', 'default': True},
        {'id': 'extinction', 'label': 'V-mag extinction', 'column': 'magextinct', 'quantity': '8', 'default': False},
        {'id': 'illumination', 'label': 'Illumination', 'column': 'illumination', 'quantity': 'ALL', 'default': False},
        {'id': 'illumination_defect', 'label': 'Illumination defect', 'column': 'illum_defect', 'quantity': 'ALL', 'default': False},
        {'id': 'sat_sep', 'label': 'Target-primary separation', 'column': 'sat_sep', 'quantity': 'ALL', 'default': False},
        {'id': 'sat_vis', 'label': 'Target-primary visibility', 'column': 'sat_vis', 'quantity': 'ALL', 'default': False},
        {'id': 'angular_width', 'label': 'Angular width', 'column': 'ang_width', 'quantity': 'ALL', 'default': False},
        {'id': 'observer_sub_lon', 'label': 'Observer sub-longitude', 'column': 'PDObsLon', 'quantity': 'ALL', 'default': False},
        {'id': 'observer_sub_lat', 'label': 'Observer sub-latitude', 'column': 'PDObsLat', 'quantity': 'ALL', 'default': False},
        {'id': 'subsolar_lon', 'label': 'Subsolar longitude', 'column': 'PDSunLon', 'quantity': 'ALL', 'default': False},
        {'id': 'subsolar_lat', 'label': 'Subsolar latitude', 'column': 'PDSunLat', 'quantity': 'ALL', 'default': False},
        {'id': 'subsolar_angle', 'label': 'Subsolar angle', 'column': 'SubSol_ang', 'quantity': 'ALL', 'default': False},
        {'id': 'subsolar_distance', 'label': 'Subsolar distance', 'column': 'SubSol_dist', 'quantity': 'ALL', 'default': False},
        {'id': 'north_pole_angle', 'label': 'North pole angle', 'column': 'NPole_ang', 'quantity': 'ALL', 'default': False},
        {'id': 'north_pole_distance', 'label': 'North pole distance', 'column': 'NPole_dist', 'quantity': 'ALL', 'default': False},
        {'id': 'heliocentric_ecl_lon', 'label': 'Heliocentric ecl. lon', 'column': 'EclLon', 'quantity': 'ALL', 'default': False},
        {'id': 'heliocentric_ecl_lat', 'label': 'Heliocentric ecl. lat', 'column': 'EclLat', 'quantity': 'ALL', 'default': False},
        {'id': 'observer_ecl_lon', 'label': 'Observer ecl. lon', 'column': 'ObsEclLon', 'quantity': 'ALL', 'default': False},
        {'id': 'observer_ecl_lat', 'label': 'Observer ecl. lat', 'column': 'ObsEclLat', 'quantity': 'ALL', 'default': False},
        {'id': 'heliocentric_distance', 'label': 'Heliocentric distance', 'column': 'r', 'quantity': '19', 'default': True},
        {'id': 'heliocentric_radial_rate', 'label': 'Heliocentric radial rate', 'column': 'r_rate', 'quantity': '19', 'default': False},
        {'id': 'geocentric_distance', 'label': 'Geocentric distance', 'column': 'delta', 'quantity': '20', 'default': True},
        {'id': 'geocentric_radial_rate', 'label': 'Geocentric radial rate', 'column': 'delta_rate', 'quantity': '20', 'default': False},
        {'id': 'lighttime', 'label': 'One-way light time', 'column': 'lighttime', 'quantity': '20', 'default': False},
        {'id': 'velocity_sun', 'label': 'Velocity wrt Sun', 'column': 'vel_sun', 'quantity': 'ALL', 'default': False},
        {'id': 'velocity_observer', 'label': 'Velocity wrt observer', 'column': 'vel_obs', 'quantity': 'ALL', 'default': False},
        {'id': 'elongation', 'label': 'Elongation', 'column': 'elong', 'quantity': '23', 'default': False},
        {'id': 'elongation_flag', 'label': 'Elongation flag', 'column': 'elongFlag', 'quantity': '23', 'default': False},
        {'id': 'phase_angle', 'label': 'Phase angle', 'column': 'alpha', 'quantity': '24', 'default': True},
        {'id': 'lunar_elongation', 'label': 'Lunar elongation', 'column': 'lunar_elong', 'quantity': 'ALL', 'default': False},
        {'id': 'lunar_illumination', 'label': 'Lunar illumination', 'column': 'lunar_illum', 'quantity': 'ALL', 'default': False},
        {'id': 'interfering_body_elong', 'label': 'Interfering-body elong.', 'column': 'IB_elong', 'quantity': 'ALL', 'default': False},
        {'id': 'interfering_body_illum', 'label': 'Interfering-body illum.', 'column': 'IB_illum', 'quantity': 'ALL', 'default': False},
        {'id': 'satellite_phase_angle', 'label': 'Observer-primary-target angle', 'column': 'sat_alpha', 'quantity': 'ALL', 'default': False},
        {'id': 'orbital_plane_angle', 'label': 'Orbital plane angle', 'column': 'OrbPlaneAng', 'quantity': 'ALL', 'default': False},
        {'id': 'sun_target_pa', 'label': 'Sun vector P.A.', 'column': 'sunTargetPA', 'quantity': 'ALL', 'default': False},
        {'id': 'velocity_pa', 'label': 'Velocity vector P.A.', 'column': 'velocityPA', 'quantity': 'ALL', 'default': False},
        {'id': 'constellation', 'label': 'Constellation', 'column': 'constellation', 'quantity': 'ALL', 'default': False},
        {'id': 'tdb_minus_ut', 'label': 'TDB-UT', 'column': 'TDB-UT', 'quantity': 'ALL', 'default': False},
        {'id': 'north_pole_ra', 'label': 'North pole RA', 'column': 'NPole_RA', 'quantity': 'ALL', 'default': False},
        {'id': 'north_pole_dec', 'label': 'North pole DEC', 'column': 'NPole_DEC', 'quantity': 'ALL', 'default': False},
        {'id': 'galactic_longitude', 'label': 'Galactic longitude', 'column': 'GlxLon', 'quantity': '33', 'default': False},
        {'id': 'galactic_latitude', 'label': 'Galactic latitude', 'column': 'GlxLat', 'quantity': '33', 'default': False},
        {'id': 'solar_time', 'label': 'Solar time', 'column': 'solartime', 'quantity': 'ALL', 'default': False},
        {'id': 'earth_lighttime', 'label': 'Earth light time', 'column': 'earth_lighttime', 'quantity': 'ALL', 'default': False},
        {'id': 'ra_3sigma', 'label': 'RA 3-sigma', 'column': 'RA_3sigma', 'quantity': 'ALL', 'default': False},
        {'id': 'dec_3sigma', 'label': 'DEC 3-sigma', 'column': 'DEC_3sigma', 'quantity': 'ALL', 'default': False},
        {'id': 'smaa_3sigma', 'label': 'SMAA 3-sigma', 'column': 'SMAA_3sigma', 'quantity': 'ALL', 'default': False},
        {'id': 'smia_3sigma', 'label': 'SMIA 3-sigma', 'column': 'SMIA_3sigma', 'quantity': 'ALL', 'default': False},
        {'id': 'theta_3sigma', 'label': 'Theta 3-sigma', 'column': 'Theta_3sigma', 'quantity': 'ALL', 'default': False},
        {'id': 'area_3sigma', 'label': 'Area 3-sigma', 'column': 'Area_3sigma', 'quantity': 'ALL', 'default': False},
        {'id': 'rss_3sigma', 'label': 'RSS 3-sigma', 'column': 'RSS_3sigma', 'quantity': 'ALL', 'default': False},
        {'id': 'range_3sigma', 'label': 'Range 3-sigma', 'column': 'r_3sigma', 'quantity': 'ALL', 'default': False},
        {'id': 'range_rate_3sigma', 'label': 'Range-rate 3-sigma', 'column': 'r_rate_3sigma', 'quantity': 'ALL', 'default': False},
        {'id': 'sband_3sigma', 'label': 'S-band 3-sigma', 'column': 'SBand_3sigma', 'quantity': 'ALL', 'default': False},
        {'id': 'xband_3sigma', 'label': 'X-band 3-sigma', 'column': 'XBand_3sigma', 'quantity': 'ALL', 'default': False},
        {'id': 'doppdelay_3sigma', 'label': 'Doppler delay 3-sigma', 'column': 'DoppDelay_3sigma', 'quantity': 'ALL', 'default': False},
        {'id': 'true_anomaly', 'label': 'True anomaly', 'column': 'true_anom', 'quantity': 'ALL', 'default': False},
        {'id': 'hour_angle', 'label': 'Hour angle', 'column': 'hour_angle', 'quantity': 'ALL', 'default': False},
        {'id': 'true_phase_angle', 'label': 'True phase angle', 'column': 'alpha_true', 'quantity': 'ALL', 'default': False},
        {'id': 'pab_lon', 'label': 'PAB longitude', 'column': 'PABLon', 'quantity': 'ALL', 'default': False},
        {'id': 'pab_lat', 'label': 'PAB latitude', 'column': 'PABLat', 'quantity': 'ALL', 'default': False},
    ]
    STEP_UNIT_CHOICES = [
        {'value': 'm', 'label': 'minutes'},
        {'value': 'h', 'label': 'hours'},
        {'value': 'd', 'label': 'days'},
    ]
    OBSERVATORY_GROUPS = [
        {
            'label': 'ATLAS',
            'choices': [
                {'code': 'T08', 'label': 'ATLAS Haleakala', 'display': 'T08 — ATLAS Haleakala'},
                {'code': 'T05', 'label': 'ATLAS Mauna Loa', 'display': 'T05 — ATLAS Mauna Loa'},
                {'code': 'M22', 'label': 'ATLAS Sutherland', 'display': 'M22 — ATLAS Sutherland'},
                {'code': 'W68', 'label': 'ATLAS Rio Hurtado', 'display': 'W68 — ATLAS Rio Hurtado'},
                {'code': 'R17', 'label': 'ATLAS Tenerife', 'display': 'R17 — ATLAS Tenerife'},
            ],
        },
        {
            'label': 'ZTF',
            'choices': [
                {'code': 'I41', 'label': 'ZTF, Palomar', 'display': 'I41 — ZTF, Palomar'},
            ],
        },
        {
            'label': 'LCO',
            'choices': [
                {'code': 'F65', 'label': 'LCO Haleakala, Faulkes Telescope North', 'display': 'F65 — LCO Haleakala, Faulkes Telescope North'},
                {'code': 'T04', 'label': 'LCO Haleakala, Clamshell #1', 'display': 'T04 — LCO Haleakala, Clamshell #1'},
                {'code': 'T03', 'label': 'LCO Haleakala, Clamshell #2', 'display': 'T03 — LCO Haleakala, Clamshell #2'},
                {'code': 'V37', 'label': 'LCO McDonald, 1m A', 'display': 'V37 — LCO McDonald, 1m A'},
                {'code': 'V39', 'label': 'LCO McDonald, 1m B', 'display': 'V39 — LCO McDonald, 1m B'},
                {'code': 'V38', 'label': 'LCO McDonald, Aqawan A #1', 'display': 'V38 — LCO McDonald, Aqawan A #1'},
                {'code': 'V45', 'label': 'LCO McDonald, Aqawan B #1', 'display': 'V45 — LCO McDonald, Aqawan B #1'},
                {'code': 'V47', 'label': 'LCO McDonald, Aqawan B #2', 'display': 'V47 — LCO McDonald, Aqawan B #2'},
                {'code': 'W85', 'label': 'LCO Cerro Tololo, 1m A', 'display': 'W85 — LCO Cerro Tololo, 1m A'},
                {'code': 'W86', 'label': 'LCO Cerro Tololo, 1m B', 'display': 'W86 — LCO Cerro Tololo, 1m B'},
                {'code': 'W87', 'label': 'LCO Cerro Tololo, 1m C', 'display': 'W87 — LCO Cerro Tololo, 1m C'},
                {'code': 'W89', 'label': 'LCO Cerro Tololo, Aqawan A #1', 'display': 'W89 — LCO Cerro Tololo, Aqawan A #1'},
                {'code': 'W79', 'label': 'LCO Cerro Tololo, Aqawan B #1', 'display': 'W79 — LCO Cerro Tololo, Aqawan B #1'},
                {'code': 'K91', 'label': 'LCO Sutherland, 1m A', 'display': 'K91 — LCO Sutherland, 1m A'},
                {'code': 'K92', 'label': 'LCO Sutherland, 1m B', 'display': 'K92 — LCO Sutherland, 1m B'},
                {'code': 'K93', 'label': 'LCO Sutherland, 1m C', 'display': 'K93 — LCO Sutherland, 1m C'},
                {'code': 'L09', 'label': 'LCO Sutherland, Aqawan A #1', 'display': 'L09 — LCO Sutherland, Aqawan A #1'},
                {'code': 'Q58', 'label': 'LCO Siding Spring, Clamshell #1', 'display': 'Q58 — LCO Siding Spring, Clamshell #1'},
                {'code': 'Q59', 'label': 'LCO Siding Spring, Clamshell #2', 'display': 'Q59 — LCO Siding Spring, Clamshell #2'},
                {'code': 'Q63', 'label': 'LCO Siding Spring, 1m A', 'display': 'Q63 — LCO Siding Spring, 1m A'},
                {'code': 'Q64', 'label': 'LCO Siding Spring, 1m B', 'display': 'Q64 — LCO Siding Spring, 1m B'},
                {'code': 'E10', 'label': 'LCO Siding Spring, Faulkes Telescope South', 'display': 'E10 — LCO Siding Spring, Faulkes Telescope South'},
                {'code': 'Z31', 'label': 'LCO Tenerife, 1m A', 'display': 'Z31 — LCO Tenerife, 1m A'},
                {'code': 'Z24', 'label': 'LCO Tenerife, 1m B', 'display': 'Z24 — LCO Tenerife, 1m B'},
                {'code': 'Z21', 'label': 'LCO Tenerife, Aqawan A #1', 'display': 'Z21 — LCO Tenerife, Aqawan A #1'},
                {'code': 'Z17', 'label': 'LCO Tenerife, Aqawan A #2', 'display': 'Z17 — LCO Tenerife, Aqawan A #2'},
            ],
        },
        {
            'label': 'Other',
            'choices': [
                {'code': '060', 'label': 'Warsaw-Ostrowik', 'display': '060 — Warsaw-Ostrowik'},
                {'code': '950', 'label': 'La Palma', 'display': '950 — La Palma'},
            ],
        },
    ]

    @classmethod
    def _dropdown_location_label(cls, code):
        for group in cls.OBSERVATORY_GROUPS:
            for choice in group['choices']:
                if choice['code'].lower() == str(code).lower():
                    return choice['label']
        return None

    @classmethod
    def _resolve_location_label(cls, code):
        code = str(code).strip()
        dropdown_label = cls._dropdown_location_label(code)
        if dropdown_label:
            return dropdown_label

        try:
            location = MPC.get_observatory_location(code)
        except Exception as exc:
            logger.warning('Could not resolve MPC observatory code %s: %s', code, exc)
            return 'Custom / unresolved code'

        if location is None:
            return 'Custom / unresolved code'

        # astroquery return shapes may differ across versions; prefer an explicit name field when present.
        if isinstance(location, tuple):
            if len(location) >= 4 and location[3]:
                return str(location[3])
            return 'Custom / unresolved code'

        if isinstance(location, dict):
            for key in ('name', 'observatory_name', 'observatory'):
                value = location.get(key)
                if value:
                    return str(value)

        for attr in ('name', 'observatory_name', 'observatory'):
            value = getattr(location, attr, None)
            if value:
                return str(value)

        if hasattr(location, 'colnames'):
            for key in ('name', 'observatory_name', 'observatory'):
                if key in location.colnames and len(location):
                    value = location[key][0]
                    if value:
                        return str(value)

        return 'Custom / unresolved code'

    @classmethod
    def _selected_field_ids(cls, request):
        selected = set(request.GET.getlist('fields'))
        if {'vmag', 'apmag', 'tmag', 'nmag'} & selected:
            selected.discard('vmag')
            selected.discard('apmag')
            selected.discard('tmag')
            selected.discard('nmag')
            selected.add('visual_mag')
        if not selected:
            selected = {field['id'] for field in cls.FIELD_CHOICES if field.get('default')}
        return [field['id'] for field in cls.FIELD_CHOICES if field['id'] in selected]

    @classmethod
    def _selected_fields(cls, field_ids):
        return [field for field in cls.FIELD_CHOICES if field['id'] in field_ids]

    @classmethod
    def _quantities_for_fields(cls, fields):
        quantities = []
        for field in fields:
            quantity = field.get('quantity')
            if quantity == 'ALL':
                return cls.FULL_OBSERVER_QUANTITIES
            if quantity and quantity not in quantities:
                quantities.append(quantity)
        if not quantities:
            quantities.append('1')
        return ','.join(quantities)

    @classmethod
    def _quantity_definitions(cls):
        return [
            {
                'id': field['id'],
                'label': field['label'],
                'definition': cls.FIELD_DEFINITIONS.get(field['id'], ''),
            }
            for field in cls.FIELD_CHOICES
            if cls.FIELD_DEFINITIONS.get(field['id'], '')
        ]

    @staticmethod
    def _field_columns(field):
        if field.get('columns'):
            return list(field['columns'])
        return [field['column']]

    @classmethod
    def _magnitude_active_fields(cls, table_columns):
        if 'V' in table_columns:
            return [{'id': 'apmag', 'label': 'APmag', 'resolved_column': 'V'}]
        active_fields = []
        if 'Tmag' in table_columns:
            active_fields.append({'id': 'tmag', 'label': 'T-mag', 'resolved_column': 'Tmag'})
        if 'Nmag' in table_columns:
            active_fields.append({'id': 'nmag', 'label': 'N-mag', 'resolved_column': 'Nmag'})
        return active_fields

    @classmethod
    def _resolve_active_fields(cls, selected_fields, table):
        table_columns = set(getattr(table, 'colnames', []) or [])
        active_fields = []
        for field in selected_fields:
            if field['id'] == 'visual_mag':
                active_fields.extend(cls._magnitude_active_fields(table_columns))
                continue
            matched_column = None
            for column_name in cls._field_columns(field):
                if column_name in table_columns:
                    matched_column = column_name
                    break
            if matched_column is not None:
                field_copy = dict(field)
                field_copy['resolved_column'] = matched_column
                active_fields.append(field_copy)
        return active_fields

    @staticmethod
    def _cell_value(row, field):
        column_name = field.get('resolved_column') or field.get('column')
        if not column_name:
            return ''
        try:
            value = row[column_name]
        except Exception:
            return ''
        if value is None:
            return ''
        if column_name == 'datetime_str':
            return str(value)
        return value

    @staticmethod
    def _parse_utc_datetime_input(raw_value, field_label):
        raw_value = str(raw_value or '').strip()
        if not raw_value:
            return None, ''

        normalized = raw_value
        if normalized.endswith('Z'):
            normalized = normalized[:-1] + '+00:00'
        if 'T' in normalized and '+' not in normalized[10:] and normalized.count('-') <= 2:
            normalized = normalized.replace('T', ' ')

        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(f'Invalid {field_label}. Use a valid UTC date-time such as 2026-04-04T12:00:00.') from exc

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)

        return parsed, raw_value

    @classmethod
    def _parse_time_span(cls, start_input, stop_input, step_number_input, step_unit_input):
        start_time, start_raw = cls._parse_utc_datetime_input(start_input, 'start time')
        stop_time, stop_raw = cls._parse_utc_datetime_input(stop_input, 'stop time')
        step_number_raw = str(step_number_input or '').strip()
        step_unit_raw = str(step_unit_input or '').strip().lower()

        now_utc = datetime.now(timezone.utc)
        if start_time is None:
            start_time = now_utc - timedelta(days=7)
            start_raw = ''
        if stop_time is None:
            stop_time = now_utc
            stop_raw = ''

        if stop_time <= start_time:
            raise ValueError('Invalid time span. Stop time must be later than start time.')

        if not step_number_raw:
            step_number_raw = '1'
        if not step_unit_raw:
            step_unit_raw = 'h'

        if not step_number_raw.isdigit() or int(step_number_raw) <= 0:
            raise ValueError('Invalid step size. Enter a positive whole number.')
        if step_unit_raw not in {choice['value'] for choice in cls.STEP_UNIT_CHOICES}:
            raise ValueError('Invalid step size unit. Choose minutes, hours, or days.')

        step_value = f'{int(step_number_raw)}{step_unit_raw}'

        return {
            'start_time': start_time,
            'stop_time': stop_time,
            'step_size': step_value,
            'start_input': start_raw,
            'stop_input': stop_raw,
            'step_number_input': str(int(step_number_raw)),
            'step_unit_input': step_unit_raw,
            'start_used': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'stop_used': stop_time.strftime('%Y-%m-%d %H:%M:%S'),
            'step_used': step_value,
        }

    @staticmethod
    def _default_time_inputs():
        now_utc = datetime.now(timezone.utc).replace(microsecond=0)
        return {
            'start_time_input': (now_utc - timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%S'),
            'stop_time_input': now_utc.strftime('%Y-%m-%dT%H:%M:%S'),
            'step_size_number_input': '1',
            'step_size_unit_input': 'h',
        }

    @staticmethod
    def _is_comet_like_identifier(query):
        normalized = query.strip().upper()
        return bool(
            re.match(r'^\d+[PD]$', normalized) or
            re.match(r'^\d+[PD]/', normalized) or
            re.match(r'^[PCDXA]/', normalized)
        )

    @classmethod
    def _target_attempts(cls, query):
        normalized = ' '.join(str(query).strip().split())
        normalized = re.sub(r'\s*/\s*', '/', normalized)
        normalized_without_parens = re.sub(r'\s*\([^)]*\)\s*$', '', normalized).strip()
        attempts = []

        def add(identifier, id_type):
            candidate = str(identifier).strip()
            key = (candidate, id_type)
            if candidate and key not in {(item['id'], item['id_type']) for item in attempts}:
                attempts.append({'id': candidate, 'id_type': id_type})

        add(normalized, None)
        add(normalized, 'smallbody')
        if normalized_without_parens != normalized:
            add(normalized_without_parens, None)
            add(normalized_without_parens, 'smallbody')

        numbered_name = re.match(r'^(\d+)\s+(.+)$', normalized)
        if numbered_name:
            number_part = numbered_name.group(1)
            name_part = numbered_name.group(2).strip()
            add(number_part, 'smallbody')
            add(number_part, None)
            add(name_part, 'asteroid_name')
            add(name_part, 'name')

        if normalized.isdigit():
            add(normalized, 'smallbody')
            add(normalized, 'designation')

        if cls._is_comet_like_identifier(normalized):
            add(normalized_without_parens, 'designation')
            add(normalized, 'designation')
            add(normalized, 'comet_name')

            numbered_comet = re.match(r'^(\d+[PD])(?:/(.+))?$', normalized_without_parens, re.IGNORECASE)
            if numbered_comet:
                designation_part = numbered_comet.group(1)
                comet_name_part = (numbered_comet.group(2) or '').strip()
                add(designation_part, 'designation')
                add(designation_part, 'smallbody')
                add(designation_part, None)
                if comet_name_part:
                    add(comet_name_part, 'comet_name')
                    add(comet_name_part, 'name')

            designation_comet = re.match(r'^([PCDXA]/[^()]+?)(?:\s*\(([^)]+)\))?$', normalized, re.IGNORECASE)
            if designation_comet:
                designation_part = designation_comet.group(1).strip()
                comet_name_part = (designation_comet.group(2) or '').strip()
                add(designation_part, 'designation')
                add(designation_part, 'smallbody')
                if comet_name_part:
                    add(comet_name_part, 'comet_name')
                    add(comet_name_part, 'name')

        if re.match(r'^[A-Za-z][A-Za-z0-9 .()_-]*$', normalized):
            add(normalized, 'comet_name')
            add(normalized, 'asteroid_name')
            add(normalized, 'name')

        return attempts

    @staticmethod
    def _is_ambiguous_horizons_error(message):
        lowered = message.lower()
        return any(token in lowered for token in (
            'ambiguous',
            'multiple matches',
            'matches more than one',
            'matching bodies',
            'multiple major-bodies match',
        ))

    @classmethod
    def _parse_horizons_ambiguity_matches(cls, message):
        matches = []
        seen = set()
        for line in str(message).splitlines():
            match = re.match(r'^\s*(\d+)\s+(.*\S)\s*$', line)
            if not match:
                continue
            record_id = match.group(1)
            description = match.group(2).strip()
            if record_id in seen:
                continue
            seen.add(record_id)
            matches.append({
                'record_id': record_id,
                'label': f'{record_id} - {description}',
            })
        return matches

    @classmethod
    def _query_horizons_ephemerides(cls, target_query, location, epochs, quantities, target_record=''):
        errors = []
        ambiguous_error = None
        target_record = str(target_record or '').strip()

        if target_record:
            return Horizons(
                id=target_record,
                id_type=None,
                location=location,
                epochs=epochs,
            ).ephemerides(quantities=quantities)

        for attempt in cls._target_attempts(target_query):
            try:
                table = Horizons(
                    id=attempt['id'],
                    id_type=attempt['id_type'],
                    location=location,
                    epochs=epochs,
                ).ephemerides(quantities=quantities)
                return table
            except Exception as exc:
                message = str(exc).strip() or exc.__class__.__name__
                errors.append(f"{attempt['id']} [{attempt['id_type'] or 'default'}]: {message}")
                if cls._is_ambiguous_horizons_error(message) and ambiguous_error is None:
                    ambiguous_error = message

        if ambiguous_error:
            matches = cls._parse_horizons_ambiguity_matches(ambiguous_error)
            raise ValueError(json.dumps({
                'kind': 'ambiguity',
                'message': (
                    f'Object identifier "{target_query}" matches multiple JPL Horizons targets. '
                    'Select one of the returned records below.'
                ),
                'matches': matches,
            }))

        raise ValueError(
            f'JPL Horizons could not resolve "{target_query}" as a unique small-body identifier.'
        )

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        target_query = (self.request.GET.get('target') or '').strip()
        target_record = (self.request.GET.get('target_record') or '').strip()
        location_query = (self.request.GET.get('location') or '').strip()
        location_preset = (self.request.GET.get('location_preset') or '').strip()
        start_time_input = (self.request.GET.get('start_time') or '').strip()
        stop_time_input = (self.request.GET.get('stop_time') or '').strip()
        step_size_number_input = (self.request.GET.get('step_size_number') or '').strip()
        step_size_unit_input = (self.request.GET.get('step_size_unit') or '').strip()
        default_time_inputs = self._default_time_inputs()
        resolved_location = location_query or location_preset or '500'
        selected_field_ids = self._selected_field_ids(self.request)
        selected_fields = self._selected_fields(selected_field_ids)
        context.update({
            'target_query': target_query,
            'target_record': target_record,
            'location_query': location_query,
            'location_preset': location_preset,
            'start_time_input': start_time_input or default_time_inputs['start_time_input'],
            'stop_time_input': stop_time_input or default_time_inputs['stop_time_input'],
            'step_size_number_input': step_size_number_input or default_time_inputs['step_size_number_input'],
            'step_size_unit_input': step_size_unit_input or default_time_inputs['step_size_unit_input'],
            'start_time_used': '',
            'stop_time_used': '',
            'step_size_used': '',
            'resolved_location': resolved_location,
            'resolved_location_label': '',
            'field_choices': self.FIELD_CHOICES,
            'default_field_choices': [field for field in self.FIELD_CHOICES if field['id'] in self.DEFAULT_VISIBLE_FIELD_IDS],
            'additional_field_choices': [field for field in self.FIELD_CHOICES if field['id'] not in self.DEFAULT_VISIBLE_FIELD_IDS],
            'quantity_definitions': self._quantity_definitions(),
            'step_unit_choices': self.STEP_UNIT_CHOICES,
            'selected_field_ids': selected_field_ids,
            'selected_fields': selected_fields,
            'observatory_groups': self.OBSERVATORY_GROUPS,
            'ambiguity_matches': [],
            'ephemeris_rows': [],
            'ephemeris_error': '',
            'ephemeris_generated_at': None,
            'resolved_target_name': '',
        })

        if not target_query and not target_record:
            return context

        try:
            time_span = self._parse_time_span(
                start_time_input,
                stop_time_input,
                step_size_number_input,
                step_size_unit_input,
            )
        except ValueError as exc:
            context['ephemeris_error'] = str(exc)
            return context

        context['start_time_input'] = time_span['start_input']
        context['stop_time_input'] = time_span['stop_input']
        context['step_size_number_input'] = time_span['step_number_input']
        context['step_size_unit_input'] = time_span['step_unit_input']
        context['start_time_used'] = time_span['start_used']
        context['stop_time_used'] = time_span['stop_used']
        context['step_size_used'] = time_span['step_used']
        epochs = {
            'start': time_span['start_time'].strftime('%Y-%m-%d %H:%M:%S'),
            'stop': time_span['stop_time'].strftime('%Y-%m-%d %H:%M:%S'),
            'step': time_span['step_size'],
        }

        try:
            generated_at = datetime.now(timezone.utc)
            quantities = self._quantities_for_fields(selected_fields)
            table = self._query_horizons_ephemerides(
                target_query=target_query,
                location=resolved_location,
                epochs=epochs,
                quantities=quantities,
                target_record=target_record,
            )
            active_fields = self._resolve_active_fields(selected_fields, table)
            context['selected_fields'] = active_fields
            context['ephemeris_rows'] = []
            for row in table[:25]:
                cells = []
                for field in active_fields:
                    cells.append(self._cell_value(row, field))
                context['ephemeris_rows'].append({'cells': cells})
            context['ephemeris_generated_at'] = generated_at
            context['resolved_location_label'] = self._resolve_location_label(resolved_location)
            if len(table) and 'targetname' in table.colnames:
                context['resolved_target_name'] = str(table[0]['targetname'])
            if not context['ephemeris_rows']:
                context['ephemeris_error'] = f'No ephemeris results were returned for "{target_query}" at location "{resolved_location}".'
        except Exception as exc:
            logger.warning(
                'BHTOM-PALLAS Horizons lookup failed for target %s at location %s: %s',
                target_record or target_query,
                resolved_location,
                exc,
            )
            message = str(exc).strip()
            if message.startswith('{'):
                try:
                    payload = json.loads(message)
                except ValueError:
                    payload = {}
                if payload.get('kind') == 'ambiguity':
                    context['ephemeris_error'] = payload.get('message') or 'Multiple JPL Horizons matches were returned.'
                    context['ambiguity_matches'] = payload.get('matches') or []
                else:
                    context['ephemeris_error'] = message
            elif message:
                context['ephemeris_error'] = message
            else:
                lookup_target = target_record or target_query
                context['ephemeris_error'] = (
                    f'Could not retrieve JPL Horizons ephemeris for "{lookup_target}" '
                    f'using location "{resolved_location}". Check that the observatory/location code is valid.'
                )

        return context


def _refresh_geotarget_from_service(target, service):
    payload = service.query_by_norad_id(target.norad_id)
    object_type, is_debris = service.classify_object_type(payload['name'], payload.get('object_type', ''))
    GeoTarget.objects.filter(pk=target.pk).update(
        name=payload['name'],
        intldes=payload.get('intldes', target.intldes),
        source=payload.get('source', target.source or 'manual'),
        object_type=object_type,
        is_debris=is_debris,
        tle_name=payload['tle_name'],
        tle_line1=payload['tle_line1'],
        tle_line2=payload['tle_line2'],
        epoch_jd=payload['epoch_jd'],
        inclination_deg=payload['inclination_deg'],
        eccentricity=payload['eccentricity'],
        raan_deg=payload['raan_deg'],
        arg_perigee_deg=payload['arg_perigee_deg'],
        mean_anomaly_deg=payload['mean_anomaly_deg'],
        mean_motion_rev_per_day=payload['mean_motion_rev_per_day'],
        bstar=payload['bstar'],
        modified=datetime.now(timezone.utc),
    )


def _parse_alias_payload(payload):
    if not payload:
        return []
    try:
        alias_rows = json.loads(payload)
    except (TypeError, ValueError):
        return []
    if not isinstance(alias_rows, list):
        return []

    cleaned = []
    for row in alias_rows:
        if isinstance(row, str):
            value = row.strip()
            if value:
                cleaned.append({'name': value, 'url': ''})
            continue
        if not isinstance(row, dict):
            continue
        name = str(row.get('name') or '').strip()
        url = str(row.get('url') or '').strip()
        source_name = str(row.get('source_name') or '').strip()
        if name:
            cleaned.append({'name': name, 'url': url, 'source_name': source_name})
    return cleaned


def _guess_alias_source(alias_name, url=''):
    value = str(alias_name or '').strip()
    url_value = str(url or '').strip().lower()
    upper = value.upper()

    if 'simbad' in url_value:
        return 'Simbad'
    if upper.startswith('GAIADR3_'):
        return 'GaiaDR3'
    if upper.startswith('GAIA'):
        return 'GaiaAlerts'
    if upper.startswith('LSST_'):
        return 'LSST'
    if upper.startswith('ASASSN_'):
        return 'ASASSN'
    if upper.startswith('ALLWISE'):
        return 'AllWISE'
    if upper.startswith('NEOWISE'):
        return 'NeoWISE'
    if upper.startswith('PS1_'):
        return 'PS1'
    if upper.startswith('SWIFT'):
        return 'SwiftUVOT'
    if upper.startswith('GALEX'):
        return 'Galex'
    if upper.startswith('6DFGS'):
        return '6dFGS'
    if upper.startswith('DESI'):
        return 'DESI'
    if upper.startswith('CRTS'):
        return 'CRTS'
    return 'Other'


def _build_recommended_observing_strategy_comment(user, strategy):
    full_name = user.get_full_name().strip() or user.get_username()
    username = user.get_username()
    return (
        f'Created by: {full_name} ({username})\n'
        f'Recommended observing strategy: {strategy.strip()}'
    )


def _build_gaia_alerts_catalog_target(row):
    target = Target()
    target.name = str(row.get('#Name') or row.get('Name') or '').strip() or 'GaiaAlerts'
    target.type = 'SIDEREAL'
    target.ra = gaia_alerts_harvester._to_float(row.get('RaDeg'))
    target.dec = gaia_alerts_harvester._to_float(row.get('DecDeg'))
    target.description = str(row.get('Comment') or '').strip()
    return target


def _get_catalog_matches(service_name, cleaned_data):
    term = (cleaned_data.get('term') or '').strip()
    if service_name == 'Gaia Alerts':
        return gaia_alerts_harvester.get_all(term)
    if service_name == 'Gaia DR3':
        return gaia_dr3_harvester.get_all(term)
    if service_name == 'Simbad':
        return simbad_harvester.get_all(
            cleaned_data.get('ra'),
            cleaned_data.get('dec'),
            3.0,
            cleaned_data.get('term') or '',
        )
    return []


def _build_catalog_target_from_match(service_name, match):
    if service_name == 'Gaia Alerts':
        return _build_gaia_alerts_catalog_target(match)
    if service_name == 'Gaia DR3':
        harvester = gaia_dr3_harvester.GaiaDR3Harvester()
        harvester.catalog_data = match
        return harvester.to_target()
    if service_name == 'Simbad':
        return simbad_harvester.target_from_result(match)
    raise ValueError(f'Unsupported catalog multi-match service: {service_name}')


def _build_catalog_result_row(service_name, index, match):
    target = _build_catalog_target_from_match(service_name, match)
    if service_name == 'Gaia Alerts':
        view_url = f'https://gsaweb.ast.cam.ac.uk/alerts/alert/{target.name}' if target.name else gaia_alerts_harvester.GAIA_ALERTS_CSV_URL
        summary = str(match.get('Comment') or '').strip()
    elif service_name == 'Simbad':
        view_url = simbad_harvester._simbad_url(target.ra, target.dec)
        summary = str(match.get('main_id') or '').strip()
    else:
        view_url = ''
        summary = str(match.get('source_id') or match.get('SOURCE_ID') or '').strip()

    return {
        'id': index,
        'name': target.name,
        'ra': target.ra,
        'dec': target.dec,
        'summary': summary,
        'url': view_url,
    }


def _hours_to_hms(hours_value):
    if hours_value is None:
        return "-"
    value = float(hours_value) % 24.0
    h = int(value)
    minutes_total = (value - h) * 60.0
    m = int(minutes_total)
    s = (minutes_total - m) * 60.0
    return f"{h:02d}:{m:02d}:{s:05.2f}"


def _hours_to_hms_astro(hours_value):
    if hours_value is None:
        return "-"
    value = float(hours_value) % 24.0
    h = int(value)
    minutes_total = (value - h) * 60.0
    m = int(minutes_total)
    s = (minutes_total - m) * 60.0
    return f"{h:02d}:{m:02d}:{s:05.2f}"


def _deg_to_dms(deg_value):
    if deg_value is None:
        return "-"
    value = float(deg_value)
    sign = "+" if value >= 0 else "-"
    abs_value = abs(value)
    d = int(abs_value)
    minutes_total = (abs_value - d) * 60.0
    m = int(minutes_total)
    s = (minutes_total - m) * 60.0
    return f"{sign}{d:02d}:{m:02d}:{s:05.2f}"


class Bhtom2TargetListView(TargetListView):
    """
    Target list override matching the non-paginated bhtom2-style table page.
    """

    paginate_by = 20
    ordering = ['-priority', '-created']
    filterset_class = BhtomTargetFilterSet

    def get_paginate_by(self, queryset):
        # HTMXTableViewMixin requires a paginator in context. Use a single page
        # sized to all rows so the bhtom2-style list remains effectively unpaginated.
        try:
            size = queryset.count()
        except (AttributeError, TypeError):
            # django-tables2 may pass TableQuerysetData instead of a QuerySet.
            try:
                size = len(queryset)
            except TypeError:
                size = queryset.data.count() if hasattr(queryset, 'data') else 1
        return max(size, 1)

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)

        object_list = context.get('object_list', [])
        try:
            context['target_count'] = object_list.count()
        except (AttributeError, TypeError):
            context['target_count'] = len(object_list)

        if hasattr(self, 'filterset') and self.filterset and self.filterset.data:
            params = [(k, v) for k, v in self.filterset.data.lists() if any(item != '' for item in v)]
            sorted_params = sorted(params, key=lambda item: item[0])
            context['query_string'] = urlencode(sorted_params, doseq=True)
        else:
            context['query_string'] = self.request.META.get('QUERY_STRING', '')

        return context


class BhtomTargetCreateView(TargetCreateView):
    def get_form_class(self):
        target_type = self.get_target_type()
        self.initial['type'] = target_type
        if target_type == Target.SIDEREAL:
            return BhtomSiderealTargetCreateForm
        return BhtomNonSiderealTargetCreateForm

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        alias_payload = _parse_alias_payload(self.request.GET.get('alias_payload'))
        if self.request.method == 'POST':
            context['names_form'] = BhtomTargetNamesFormset(self.request.POST, instance=getattr(self, 'object', None))
        elif alias_payload:
            context['names_form'] = BhtomTargetNamesFormset(initial=alias_payload)
        else:
            context['names_form'] = BhtomTargetNamesFormset(
                initial=[{'name': new_name} for new_name in self.request.GET.get('names', '').split(',') if new_name]
            )
        return context

    def get_form(self, *args, **kwargs):
        form = super().get_form(*args, **kwargs)
        if self.request.user.is_superuser:
            form.fields['groups'].queryset = Group.objects.all()
        else:
            form.fields['groups'].queryset = self.request.user.groups.all()
        return form

    @transaction.atomic
    def form_valid(self, form):
        self.object = form.save()
        extra = TargetExtraFormset(self.request.POST, instance=self.object)
        names = BhtomTargetNamesFormset(self.request.POST, instance=self.object)
        if extra.is_valid() and names.is_valid():
            extra.save()
            names.save()
            Comment.objects.create(
                content_object=self.object,
                site=get_current_site(self.request),
                user=self.request.user,
                user_name=self.request.user.get_full_name().strip() or self.request.user.get_username(),
                user_email=self.request.user.email or '',
                comment=_build_recommended_observing_strategy_comment(
                    self.request.user,
                    form.cleaned_data['recommended_observing_strategy'],
                ),
            )
            run_hook('target_post_save', target=self.object, created=True)
            return redirect(self.get_success_url())
        form.add_error(None, extra.errors)
        form.add_error(None, extra.non_form_errors())
        form.add_error(None, names.errors)
        form.add_error(None, names.non_form_errors())
        transaction.set_rollback(True)
        return super().form_invalid(form)


class BhtomTargetUpdateView(TargetUpdateView):
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        if self.request.method == 'POST':
            context['names_form'] = BhtomTargetNamesFormset(self.request.POST, instance=self.object)
        else:
            context['names_form'] = BhtomTargetNamesFormset(instance=self.object)
        return context

    @transaction.atomic
    def form_valid(self, form):
        self.object = form.save()
        extra = TargetExtraFormset(self.request.POST, instance=self.object)
        names = BhtomTargetNamesFormset(self.request.POST, instance=self.object)
        if extra.is_valid() and names.is_valid():
            extra.save()
            names.save()
            return redirect(self.get_success_url())
        form.add_error(None, extra.errors)
        form.add_error(None, extra.non_form_errors())
        form.add_error(None, names.errors)
        form.add_error(None, names.non_form_errors())
        transaction.set_rollback(True)
        return super().form_invalid(form)


class BhtomTargetDetailView(TargetDetailView):
    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        target = self.get_object()
        other_names = []
        for alias in target.aliases.all().select_related('alias_info'):
            alias_info = getattr(alias, 'alias_info', None)
            url = getattr(alias_info, 'url', '')
            other_names.append({
                'source_name': getattr(alias_info, 'source_name', '') or _guess_alias_source(alias.name, url),
                'name': alias.name,
                'url': url,
            })
        other_names.sort(key=lambda row: (row['source_name'].lower(), row['name'].lower()))
        context['target_other_names'] = other_names
        return context


class Bhtom2DataProductUploadView(DataProductUploadView):
    def form_valid(self, form):
        dp_type = form.cleaned_data['data_product_type']
        if dp_type != 'fits_file':
            return super().form_valid(form)

        target = form.cleaned_data['target']
        if not target:
            observation_record = form.cleaned_data['observation_record']
            target = observation_record.target

        upload_service_url = getattr(settings, 'BHTOM2_UPLOAD_SERVICE_URL', '').rstrip('/')
        if not upload_service_url:
            messages.error(self.request, 'BHTOM2 upload service URL is not configured.')
            return redirect(form.cleaned_data.get('referrer', '/'))

        bhtom2_target_name = (self.request.POST.get('bhtom2_target_name') or target.name).strip()
        observatory_oname = (self.request.POST.get('observatory_oname') or '').strip()
        bhtom2_user_id = (self.request.POST.get('bhtom2_user_id') or '').strip()
        bhtom2_token = (self.request.POST.get('bhtom2_token') or '').strip()
        calibration_filter = (self.request.POST.get('calibration_filter') or 'GaiaSP/any').strip()
        dry_run = self.request.POST.get('bhtom2_dry_run') == 'on'
        comment = (self.request.POST.get('bhtom2_comment') or '').strip()
        fits_file = self.request.FILES.get('files')

        if not bhtom2_target_name:
            messages.error(self.request, 'Target is required for FITS upload.')
            return redirect(form.cleaned_data.get('referrer', '/'))
        if not fits_file:
            messages.error(self.request, 'Choose a FITS file to upload.')
            return redirect(form.cleaned_data.get('referrer', '/'))
        if not observatory_oname:
            messages.error(self.request, 'Observatory/Camera ONAME is required for FITS upload.')
            return redirect(form.cleaned_data.get('referrer', '/'))
        if not bhtom2_user_id:
            messages.error(self.request, 'BHTOM2 user ID is required for FITS upload.')
            return redirect(form.cleaned_data.get('referrer', '/'))
        if not bhtom2_token:
            messages.error(self.request, 'BHTOM2 token is required for FITS upload.')
            return redirect(form.cleaned_data.get('referrer', '/'))

        post_data = {
            'target': bhtom2_target_name,
            'data_product_type': 'fits_file',
            'observatory': observatory_oname,
            'filter': calibration_filter,
            'comment': comment,
            'dry_run': dry_run,
            'no_plot': False,
        }
        headers = {
            'Authorization': f'Token {bhtom2_token}',
            'Correlation-ID': str(uuid4()),
        }
        files = {'file_0': (fits_file.name, fits_file, fits_file.content_type or 'application/octet-stream')}

        try:
            response = requests.post(
                f'{upload_service_url}/upload/',
                data=post_data,
                files=files,
                headers=headers,
                timeout=120,
            )
        except requests.RequestException as exc:
            logger.exception('BHTOM2 FITS upload failed for target %s', target.pk)
            messages.error(self.request, f'Unable to reach the BHTOM2 upload service: {exc}')
            return redirect(form.cleaned_data.get('referrer', '/'))

        if response.status_code == 201:
            messages.success(
                self.request,
                f'FITS upload sent to BHTOM2 for target {target.name} using user ID {bhtom2_user_id}.'
            )
            return redirect(form.cleaned_data.get('referrer', '/'))

        try:
            payload = response.json()
        except ValueError:
            payload = {}

        error_message = payload.get('detail') or payload.get('non_field_errors') or response.text or 'Unknown error.'
        if isinstance(error_message, list):
            error_message = '; '.join(str(item) for item in error_message)
        messages.error(self.request, f'BHTOM2 upload rejected the FITS file: {error_message}')
        return redirect(form.cleaned_data.get('referrer', '/'))


class PublicUploadChoicesView(View):
    catalog_loader = None

    def get(self, request, *args, **kwargs):
        if not _public_upload_has_access(request):
            return JsonResponse({'error': 'Public upload password required.'}, status=403)
        if self.catalog_loader is None:
            return JsonResponse({'error': 'Unsupported catalog.'}, status=404)
        try:
            choices = self.catalog_loader()
        except requests.RequestException as exc:
            logger.exception('Unable to load public upload choices')
            return JsonResponse({'error': f'Unable to read BHTOM2 data: {exc}'}, status=502)
        except RuntimeError as exc:
            return JsonResponse({'error': str(exc)}, status=503)

        return JsonResponse({'results': _filter_public_upload_choices(choices, request.GET.get('q'))})


class PublicUploadTargetsView(PublicUploadChoicesView):
    catalog_loader = staticmethod(_public_upload_target_choices)


class PublicUploadObserversView(PublicUploadChoicesView):
    catalog_loader = staticmethod(_public_upload_observer_choices)


class PublicUploadObservatoriesView(PublicUploadChoicesView):
    catalog_loader = staticmethod(_public_upload_observatory_choices)


class PublicUploadView(FormView):
    form_class = PublicFitsUploadForm
    template_name = 'public_upload.html'
    success_url = reverse_lazy('public-upload')

    def get(self, request, *args, **kwargs):
        if _public_upload_has_access(request):
            return super().get(request, *args, **kwargs)
        access_form = PublicUploadAccessForm()
        return render(request, self.template_name, self.get_context_data(form=self.get_form(), access_form=access_form))

    def post(self, request, *args, **kwargs):
        if _public_upload_has_access(request):
            return super().post(request, *args, **kwargs)

        access_form = PublicUploadAccessForm(request.POST)
        if access_form.is_valid():
            expected_password = getattr(settings, 'PUBLIC_UPLOAD_PASSWORD', '')
            if access_form.cleaned_data['password'] == expected_password:
                request.session[PUBLIC_UPLOAD_SESSION_KEY] = True
                messages.success(request, 'Public upload unlocked for this session.')
                return redirect('public-upload')
            access_form.add_error('password', 'Incorrect password.')
        return render(request, self.template_name, self.get_context_data(form=self.get_form(), access_form=access_form))

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update({
            'public_upload_targets_url': reverse('public-upload-targets'),
            'public_upload_observers_url': reverse('public-upload-observers'),
            'public_upload_observatories_url': reverse('public-upload-observatories'),
            'bhtom2_api_ready': _bhtom2_api_configured(),
            'public_upload_requires_password': _public_upload_password_enabled(),
            'public_upload_has_access': _public_upload_has_access(self.request),
            'access_form': kwargs.get('access_form') or PublicUploadAccessForm(),
        })
        return context

    def form_valid(self, form):
        upload_service_url = getattr(settings, 'BHTOM2_UPLOAD_SERVICE_URL', '').rstrip('/')
        if not upload_service_url:
            form.add_error(None, 'BHTOM2 upload service URL is not configured.')
            return self.form_invalid(form)

        target = _normalize_public_upload_input(form.cleaned_data['target'])
        observer = _normalize_public_upload_input(form.cleaned_data['observer'])
        observatory = _normalize_public_upload_input(form.cleaned_data['observatory'])
        if not target:
            form.add_error('target', 'Target is required.')
        if not observer:
            form.add_error('observer', 'Observer is required.')
        if not observatory:
            form.add_error('observatory', 'Observatory ONAME is required.')
        if form.errors:
            return self.form_invalid(form)

        fits_file = form.cleaned_data['fits_file']
        post_data = {
            'target': target,
            'data_product_type': 'fits_file',
            'observatory': observatory,
            'observers': observer,
            'filter': form.cleaned_data['calibration_filter'],
            'comment': form.cleaned_data['comment'],
            'no_plot': False,
        }
        headers = {
            'Authorization': f"Token {form.cleaned_data['token'].strip()}",
            'Correlation-ID': str(uuid4()),
        }
        files = {'file_0': (fits_file.name, fits_file, fits_file.content_type or 'application/octet-stream')}

        try:
            response = requests.post(
                f'{upload_service_url}/upload/',
                data=post_data,
                files=files,
                headers=headers,
                timeout=120,
            )
        except requests.RequestException as exc:
            logger.exception('Public FITS upload failed for target %s', target)
            form.add_error(None, f'Unable to reach the BHTOM2 upload service: {exc}')
            return self.form_invalid(form)

        if response.status_code == 201:
            messages.success(self.request, f'FITS upload sent to BHTOM2 for target {target}.')
            return super().form_valid(form)

        try:
            payload = response.json()
        except ValueError:
            payload = {}

        error_message = payload.get('detail') or payload.get('non_field_errors') or response.text or 'Unknown error.'
        if isinstance(error_message, list):
            error_message = '; '.join(str(item) for item in error_message)
        form.add_error(None, f'BHTOM2 upload rejected the FITS file: {error_message}')
        return self.form_invalid(form)


class BhtomCatalogQueryView(FormView):
    form_class = BhtomCatalogQueryForm
    template_name = 'tom_catalogs/query_form.html'

    def _render_catalog_results(self, form, matches):
        service_name = form.cleaned_data.get('service')
        self.request.session[CATALOG_RESULTS_SESSION_KEY] = matches
        self.request.session[CATALOG_FORM_SESSION_KEY] = {
            'service': service_name,
            'term': (form.cleaned_data.get('term') or '').strip(),
        }
        context = self.get_context_data(form=form)
        context.update({
            'data_service': service_name,
            'query': (form.cleaned_data.get('term') or '').strip(),
            'results': [_build_catalog_result_row(service_name, index, row) for index, row in enumerate(matches)],
        })
        return render(self.request, 'tom_catalogs/query_result.html', context)

    def form_valid(self, form):
        matches = _get_catalog_matches(form.cleaned_data.get('service'), form.cleaned_data)
        if len(matches) > 1:
            return self._render_catalog_results(form, matches)

        try:
            self.target = form.get_target()
        except MissingDataException:
            error_target = 'ra' if form.cleaned_data.get('service') == 'Simbad' else 'term'
            form.add_error(error_target, ValidationError('Object not found'))
            return self.form_invalid(form)
        return super().form_valid(form)

    def get_success_url(self):
        target_params = self.target.as_dict()
        target_params['names'] = ','.join(
            alias['name'] for alias in getattr(self.target, 'extra_aliases', []) if alias.get('name')
        )
        alias_payload = BhtomCatalogQueryForm.serialize_alias_payload(self.target)
        if alias_payload:
            target_params['alias_payload'] = alias_payload
        return reverse('targets:create') + '?' + urlencode(target_params)


class BhtomCatalogSelectResultView(LoginRequiredMixin, View):
    @staticmethod
    def _build_create_url(service_name, row):
        target = _build_catalog_target_from_match(service_name, row)
        return reverse('targets:create') + '?' + urlencode(target.as_dict())

    def post(self, request, *args, **kwargs):
        stored_results = request.session.get(CATALOG_RESULTS_SESSION_KEY) or []
        stored_form_data = request.session.get(CATALOG_FORM_SESSION_KEY) or {}
        selected_result = request.POST.get('selected_result')
        service_name = stored_form_data.get('service', '')

        if not stored_results:
            messages.error(request, 'Catalog query results expired. Run the catalog query again.')
            return redirect(reverse('tom_catalogs:query'))
        if selected_result in (None, ''):
            messages.warning(request, 'Please select one result.')
            context = {
                'data_service': service_name,
                'query': stored_form_data.get('term', ''),
                'results': [
                    _build_catalog_result_row(service_name, index, row) for index, row in enumerate(stored_results)
                ],
            }
            return render(request, 'tom_catalogs/query_result.html', context)

        try:
            row = stored_results[int(selected_result)]
        except (TypeError, ValueError, IndexError):
            messages.error(request, 'Selected result is invalid. Run the catalog query again.')
            return redirect(reverse('tom_catalogs:query'))

        request.session.pop(CATALOG_RESULTS_SESSION_KEY, None)
        request.session.pop(CATALOG_FORM_SESSION_KEY, None)
        return redirect(self._build_create_url(service_name, row))


class GeoTomTargetListView(ListView):
    model = GeoTarget
    template_name = 'tom_targets/geotom_target_list.html'
    context_object_name = 'object_list'
    paginate_by = 500
    OBSERVER_PRESETS = {
        'warsaw': {'name': 'Warsaw', 'lat_deg': 52.2297, 'lon_deg': 21.0122, 'elevation_m': 100.0},
        'ostrowik': {'name': 'Ostrowik', 'lat_deg': 52.087981, 'lon_deg': 21.41614, 'elevation_m': 120.0},
        'bialkow': {'name': 'Bialkow', 'lat_deg': 51.47425, 'lon_deg': 16.657822, 'elevation_m': 130.0},
        'bolecina': {'name': 'Bolecina', 'lat_deg': 49.819827, 'lon_deg': 19.370521, 'elevation_m': 398.0},
        'moletai': {'name': 'Moletai', 'lat_deg': 55.3189, 'lon_deg': 25.5633, 'elevation_m': 200.0},
        'piwnice': {'name': 'Piwnice', 'lat_deg': 53.09546, 'lon_deg': 18.56406, 'elevation_m': 87.0},
        'lasilla': {'name': 'La Silla', 'lat_deg': -29.2567, 'lon_deg': -70.7346, 'elevation_m': 2400.0},
    }

    @staticmethod
    def _parse_float(value, default=None):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _parse_utc_datetime(value):
        if not value:
            return None
        normalized = value.strip()
        if not normalized:
            return None
        if normalized.endswith('Z'):
            normalized = normalized[:-1] + '+00:00'
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _resolve_calculation_time(self):
        time_raw = (self.request.GET.get('time_utc') or '').strip()
        calculation_time_utc = self._parse_utc_datetime(time_raw)
        if calculation_time_utc is not None:
            return calculation_time_utc, calculation_time_utc.strftime('%Y-%m-%dT%H:%M:%S'), ''
        if time_raw:
            return (
                datetime.now(timezone.utc),
                time_raw,
                'Custom UTC time is invalid. Use a valid UTC date and time.',
            )
        now_utc = datetime.now(timezone.utc)
        return now_utc, now_utc.strftime('%Y-%m-%dT%H:%M:%S'), ''

    def _resolve_observer(self):
        observer_key = (self.request.GET.get('observer') or 'warsaw').strip().lower()
        lat_raw = (self.request.GET.get('lat') or '').strip()
        lon_raw = (self.request.GET.get('lon') or '').strip()
        elev_raw = (self.request.GET.get('elev') or '').strip()

        if observer_key == 'custom':
            lat = self._parse_float(lat_raw)
            lon = self._parse_float(lon_raw)
            elev = self._parse_float(elev_raw, default=100.0)
            valid = (
                lat is not None and lon is not None and
                -90.0 <= lat <= 90.0 and
                -180.0 <= lon <= 180.0
            )
            if valid:
                return {
                    'key': 'custom',
                    'name': 'Custom',
                    'lat_deg': lat,
                    'lon_deg': lon,
                    'elevation_m': elev,
                    'input_lat': lat_raw,
                    'input_lon': lon_raw,
                    'input_elev': elev_raw or '100',
                    'error': '',
                }
            fallback = self.OBSERVER_PRESETS['warsaw']
            return {
                'key': 'warsaw',
                'name': fallback['name'],
                'lat_deg': fallback['lat_deg'],
                'lon_deg': fallback['lon_deg'],
                'elevation_m': fallback['elevation_m'],
                'input_lat': lat_raw,
                'input_lon': lon_raw,
                'input_elev': elev_raw,
                'error': 'Custom observer requires valid latitude (-90..90) and longitude (-180..180).',
            }

        preset = self.OBSERVER_PRESETS.get(observer_key, self.OBSERVER_PRESETS['warsaw'])
        return {
            'key': observer_key if observer_key in self.OBSERVER_PRESETS else 'warsaw',
            'name': preset['name'],
            'lat_deg': preset['lat_deg'],
            'lon_deg': preset['lon_deg'],
            'elevation_m': preset['elevation_m'],
            'input_lat': lat_raw,
            'input_lon': lon_raw,
            'input_elev': elev_raw,
            'error': '',
        }

    def get_queryset(self):
        queryset = super().get_queryset()
        name = (self.request.GET.get('name') or '').strip()
        norad = (self.request.GET.get('norad_id') or '').strip()
        object_class = (self.request.GET.get('object_class') or 'all').strip().lower()

        if name:
            queryset = queryset.filter(Q(name__icontains=name) | Q(tle_name__icontains=name))
        if norad:
            try:
                queryset = queryset.filter(norad_id=int(norad))
            except ValueError:
                queryset = queryset.none()
        if object_class == 'debris':
            queryset = queryset.filter(is_debris=True)
        elif object_class == 'satellite':
            queryset = queryset.filter(is_debris=False)

        return queryset.order_by('name')

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        observer = self._resolve_observer()
        calculation_time_utc, calculation_time_input, calculation_time_error = self._resolve_calculation_time()
        visible_only = str(self.request.GET.get('visible_only', '')).lower() in ('1', 'true', 'yes', 'on')

        object_list = context.get('object_list', [])
        map_targets = []
        geotom_rows = []
        for target in object_list:
            row = {"target": target}
            sat = geosat_alt_az_from_tle(
                tle_name=target.tle_name or target.name,
                tle_line1=target.tle_line1,
                tle_line2=target.tle_line2,
                observer_lat_deg=observer['lat_deg'],
                observer_lon_deg=observer['lon_deg'],
                observer_elevation_m=observer['elevation_m'],
                when_utc=calculation_time_utc,
            )
            if sat is None:
                row.update({
                    "alt_deg": None,
                    "az_deg": None,
                    "hour_angle_hours": None,
                    "ra_icrf_hours": None,
                    "dec_deg": None,
                    "estimated_vmag": None,
                    "hour_angle_sex": "-",
                    "ra_icrf_sex": "-",
                    "dec_sex": "-",
                })
                if not visible_only:
                    geotom_rows.append(row)
                continue

            is_visible = sat["alt_deg"] > 0 and sat["solar_elongation_deg"] >= 90.0
            row.update({
                "alt_deg": sat["alt_deg"],
                "az_deg": sat["az_deg"],
                "hour_angle_hours": sat["hour_angle_hours"],
                "ra_icrf_hours": sat["ra_icrf_hours"],
                "dec_deg": sat["dec_deg"],
                "solar_elongation_deg": sat["solar_elongation_deg"],
                "is_visible": is_visible,
                "estimated_vmag": sat["estimated_vmag"],
                "hour_angle_sex": _hours_to_hms(sat["hour_angle_hours"]),
                "ra_icrf_sex": _hours_to_hms_astro(sat["ra_icrf_hours"]),
                "dec_sex": _deg_to_dms(sat["dec_deg"]),
            })
            if visible_only and not is_visible:
                continue
            geotom_rows.append(row)
            plot_ha_hours, plot_dec_deg = altaz_to_hadec_point(
                sat['alt_deg'],
                sat['az_deg'],
                observer['lat_deg'],
            )

            map_targets.append({
                'target_id': target.pk,
                'target_name': target.name,
                'norad_id': target.norad_id,
                'is_debris': bool(target.is_debris),
                'tle_name': sat['tle_name'],
                'alt_deg': sat['alt_deg'],
                'az_deg': sat['az_deg'],
                'hour_angle_hours': plot_ha_hours,
                'dec_deg': plot_dec_deg,
                'solar_elongation_deg': sat['solar_elongation_deg'],
                'distance_km': sat['distance_km'],
                'estimated_vmag': sat['estimated_vmag'],
            })

        context['geotom_targets_json'] = json.dumps(map_targets)
        sun_curve_altaz = sun_visibility_curve(
            observer_lat_deg=observer['lat_deg'],
            observer_lon_deg=observer['lon_deg'],
            observer_elevation_m=observer['elevation_m'],
            when_utc=calculation_time_utc,
        )
        context['geotom_visibility_curve_altaz_json'] = json.dumps(sun_curve_altaz['curve_points'])
        sun_hadec = altaz_to_hadec_point(
            sun_curve_altaz['sun_alt_deg'],
            sun_curve_altaz['sun_az_deg'],
            observer['lat_deg'],
        )
        context['geotom_visibility_curve_hadec_json'] = json.dumps(
            convert_altaz_curve_to_hadec(
                sun_curve_altaz['curve_points'],
                observer_lat_deg=observer['lat_deg'],
            )
        )
        context['geotom_sun_altaz_json'] = json.dumps({
            'az_deg': sun_curve_altaz['sun_az_deg'],
            'alt_deg': sun_curve_altaz['sun_alt_deg'],
        })
        context['geotom_sun_hadec_json'] = json.dumps({
            'ha_hours': sun_hadec[0],
            'dec_deg': sun_hadec[1],
        })
        context['geotom_rows'] = geotom_rows
        paginator = context.get('paginator')
        if visible_only:
            context['target_count'] = len(geotom_rows)
        else:
            context['target_count'] = paginator.count if paginator else len(object_list)
        context['geotom_generated_utc'] = calculation_time_utc
        context['geotom_generated_utc_input'] = calculation_time_input
        context['geotom_time_error'] = calculation_time_error
        context['filter_values'] = {
            'name': (self.request.GET.get('name') or '').strip(),
            'norad_id': (self.request.GET.get('norad_id') or '').strip(),
            'object_class': (self.request.GET.get('object_class') or 'all').strip().lower(),
            'visible_only': visible_only,
        }
        context['geotom_observer'] = observer
        context['geotom_observer_presets'] = [
            {'key': key, 'name': value['name']}
            for key, value in self.OBSERVER_PRESETS.items()
        ]
        return context


class GeoTomAddSatView(LoginRequiredMixin, FormView):
    template_name = 'tom_targets/geotom_add_sat.html'
    form_class = GeoTomAddSatForm
    success_url = reverse_lazy('geotom-list')

    def form_valid(self, form):
        norad_id = form.cleaned_data['norad_id']
        service = GeoSatDataService()
        try:
            payload = service.query_by_norad_id(norad_id)
        except Exception as exc:
            form.add_error('norad_id', f'Could not fetch satellite metadata for NORAD {norad_id}: {exc}')
            return self.form_invalid(form)

        defaults = {
            'name': payload['name'],
            'intldes': payload.get('intldes', ''),
            'source': payload.get('source', 'manual'),
            'object_type': payload.get('object_type', 'SATELLITE') or 'SATELLITE',
            'is_debris': payload.get('is_debris', False),
            'tle_name': payload['tle_name'],
            'tle_line1': payload['tle_line1'],
            'tle_line2': payload['tle_line2'],
            'epoch_jd': payload['epoch_jd'],
            'inclination_deg': payload['inclination_deg'],
            'eccentricity': payload['eccentricity'],
            'raan_deg': payload['raan_deg'],
            'arg_perigee_deg': payload['arg_perigee_deg'],
            'mean_anomaly_deg': payload['mean_anomaly_deg'],
            'mean_motion_rev_per_day': payload['mean_motion_rev_per_day'],
            'bstar': payload['bstar'],
        }
        geotarget, created = GeoTarget.objects.update_or_create(norad_id=norad_id, defaults=defaults)
        if created:
            messages.success(self.request, f'Added object {geotarget.name} (NORAD {norad_id}).')
        else:
            messages.success(self.request, f'Updated object {geotarget.name} (NORAD {norad_id}).')
        return super().form_valid(form)


class GeoTomRefreshTleView(LoginRequiredMixin, View):
    def post(self, request, *args, **kwargs):
        service = GeoSatDataService()
        updated = 0
        failed = 0
        for target in GeoTarget.objects.all().iterator():
            try:
                _refresh_geotarget_from_service(target, service)
                updated += 1
            except Exception:
                failed += 1

        if failed:
            messages.warning(request, f'Refreshed TLE for {updated} satellites, {failed} failed.')
        else:
            messages.success(request, f'Refreshed TLE for {updated} satellites.')
        return HttpResponseRedirect(reverse_lazy('geotom-list'))


class GeoTomRefreshSingleTleView(LoginRequiredMixin, View):
    def post(self, request, *args, **kwargs):
        pk = kwargs.get('pk')
        target = GeoTarget.objects.filter(pk=pk).first()
        if target is None:
            messages.warning(request, 'Satellite not found.')
            return HttpResponseRedirect(reverse_lazy('geotom-list'))

        service = GeoSatDataService()
        try:
            _refresh_geotarget_from_service(target, service)
        except Exception as exc:
            messages.warning(request, f'Could not refresh TLE for {target.name} (NORAD {target.norad_id}): {exc}')
        else:
            messages.success(request, f'Refreshed TLE for {target.name} (NORAD {target.norad_id}).')
        return HttpResponseRedirect(reverse_lazy('geotom-list'))


class GeoTomDeleteSatView(LoginRequiredMixin, View):
    def post(self, request, *args, **kwargs):
        pk = kwargs.get('pk')
        target = GeoTarget.objects.filter(pk=pk).first()
        if target is None:
            messages.warning(request, 'Satellite not found.')
            return HttpResponseRedirect(reverse_lazy('geotom-list'))

        label = f'{target.name} (NORAD {target.norad_id})'
        target.delete()
        messages.success(request, f'Deleted satellite {label}.')
        return HttpResponseRedirect(reverse_lazy('geotom-list'))


class LegacyLogoutView(View):
    """
    Compatibility logout endpoint that accepts both GET and POST.
    """

    def get(self, request, *args, **kwargs):
        return self._logout_and_redirect(request)

    def post(self, request, *args, **kwargs):
        return self._logout_and_redirect(request)

    def _logout_and_redirect(self, request):
        logout(request)
        return HttpResponseRedirect(resolve_url(getattr(settings, 'LOGOUT_REDIRECT_URL', '/')))


class UpdateReducedDataAndDataServicesView(LoginRequiredMixin, RedirectView):
    """
    Override for TOM's "update reduced data" endpoint.
    Runs standard broker update flow, then enqueues DataService updates.
    """

    def get(self, request, *args, **kwargs):
        query_params = request.GET.copy()
        target_id = query_params.pop('target_id', None)
        query_params.pop('force_all_dataservices', None)
        force_all_dataservices = str(request.GET.get('force_all_dataservices', '')).lower() in ('1', 'true', 'yes')
        out = StringIO()

        if target_id:
            if isinstance(target_id, list):
                target_id = target_id[-1]
            self._run_update_reduced_data(out=out, target_id=target_id)
            self._enqueue_dataservices_for_target(target_id, force_all_services=force_all_dataservices)
        else:
            self._run_update_reduced_data(out=out)
            self._enqueue_dataservices_for_all_targets(force_all_services=force_all_dataservices)

        if out.getvalue():
            messages.info(request, out.getvalue())
        if force_all_dataservices:
            add_hint(
                request,
                'Forced DataServices refresh was enqueued in the background.',
            )
        else:
            add_hint(
                request,
                'DataServices updates were enqueued in the background. Refresh photometry in a moment if needed.',
            )
        redirect_url = self.get_redirect_url(*args, **kwargs)
        encoded_query = urlencode(query_params)
        if encoded_query:
            redirect_url = f'{redirect_url}?{encoded_query}'
        return HttpResponseRedirect(redirect_url)

    def get_redirect_url(self, *args, **kwargs):
        return self.request.META.get('HTTP_REFERER', '/')

    def _run_update_reduced_data(self, out, target_id=None):
        try:
            if target_id:
                call_command('updatereduceddata', target_id=target_id, stdout=out)
            else:
                call_command('updatereduceddata', stdout=out)
        except Exception as exc:
            logger.exception('Reduced data update failed (target_id=%s): %s', target_id, exc)
            messages.warning(
                self.request,
                f'Broker reduced-data update failed ({exc}). DataServices refresh was still enqueued.',
            )

    def _enqueue_dataservices_for_target(self, target_id, force_all_services=False):
        try:
            enqueue_target_dataservices_update(int(target_id), force_all_services=force_all_services)
        except Exception as exc:
            logger.warning('Could not enqueue DataServices for target %s: %s', target_id, exc)

    def _enqueue_dataservices_for_all_targets(self, force_all_services=False):
        for pk in Target.objects.values_list('pk', flat=True).iterator():
            try:
                enqueue_target_dataservices_update(pk, force_all_services=force_all_services)
            except Exception as exc:
                logger.warning('Could not enqueue DataServices for target %s: %s', pk, exc)
