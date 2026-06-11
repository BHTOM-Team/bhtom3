import json
import logging
import math
import re
import requests
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from datetime import datetime, timedelta, timezone
from io import StringIO
from urllib.parse import urlencode

import numpy as np
from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time
from astropy.timeseries import LombScargle
from astroquery.jplhorizons import Horizons
from astroquery.mpc import MPC
from django.contrib import messages
from django.conf import settings
from django.contrib.auth import logout
from django.contrib.auth import update_session_auth_hash
from django.contrib.auth.models import Group
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.auth.views import redirect_to_login
from django.contrib.sites.shortcuts import get_current_site
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.core.serializers.json import DjangoJSONEncoder
from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
from django.shortcuts import redirect
from django.shortcuts import get_object_or_404
from django.shortcuts import resolve_url
from django.shortcuts import render
from django.views.generic import FormView, ListView, RedirectView, TemplateView
from django.views import View
from django.utils import timezone as django_timezone
from django.utils.decorators import method_decorator
from django.urls import reverse, reverse_lazy
from django.db.models import Q
from django.db import transaction
from django.views.decorators.csrf import csrf_exempt
from django_comments.models import Comment
from guardian.shortcuts import assign_perm
from rest_framework.authtoken.models import Token

from tom_common.hints import add_hint
from tom_common.hooks import run_hook
from tom_catalogs.harvester import MissingDataException, get_service_classes
from tom_dataproducts.data_processor import run_data_processor
from tom_dataproducts.exceptions import InvalidFileFormatException
from tom_dataproducts.forms import AddProductToGroupForm
from tom_dataproducts.models import DataProduct
from tom_dataproducts.models import ReducedDatum
from tom_targets.forms import TargetExtraFormset
from tom_targets.models import Target
from tom_targets.views import TargetCreateView, TargetDetailView, TargetListView, TargetUpdateView
from tom_dataservices.dataservices import get_data_service_class, get_data_service_classes
from tom_dataservices.models import DataServiceQuery
from tom_dataservices.views import (
    CreateTargetFromQueryView,
    DataServiceQueryCreateView,
    DataServiceQueryUpdateView,
    RunQueryView,
)
from tom_common.views import UserCreateView as TomCommonUserCreateView
from tom_common.views import UserUpdateView as TomCommonUserUpdateView
from tom_observations.views import ObservationCreateView as TomObservationCreateView
from tom_observations.views import ObservationRecordDetailView as TomObservationRecordDetailView
from tom_observations.facilities.lco import LCOSettings
from tom_observations.facility import get_service_class
from tom_observations.models import ObservationRecord

from custom_code.filters import BhtomTargetFilterSet
from custom_code.bhtom2_uploads import (
    ensure_fits_dataproduct_type,
    forward_dataproduct_to_bhtom2,
    has_successful_bhtom2_upload,
)
from custom_code.astrometry import can_compute_current_coordinates, compute_current_coordinates
from custom_code.forms import (
    ALL_DATA_SERVICES_LABEL,
    ALL_DATA_SERVICES_VALUE,
    BhtomDataProductUploadForm,
    BhtomCatalogQueryForm,
    BhtomNonSiderealTargetCreateForm,
    BhtomPlanetaryTransitTargetCreateForm,
    BhtomPlanetaryTransitTargetUpdateForm,
    BhtomSiderealTargetCreateForm,
    BhtomSiderealTargetUpdateForm,
    BhtomTargetNamesFormset,
    BhtomUserCreationForm,
    BhtomUserUpdateForm,
    GeoTomAddSatForm,
)
from custom_code.proposal_forms import (
    DirectFacilityProposalForm,
    FacilityAccountForm,
    FacilityProposalForm,
    LCOProposalImportForm,
)
from custom_code.facility_proposals import (
    ensure_default_facilities,
    get_current_proposals_for_user,
    get_or_create_hidden_account,
    sync_remote_proposals_for_account,
    get_accessible_accounts,
    get_accessible_facilities,
    get_accessible_proposals,
    get_account_for_user,
    get_facility_by_code,
    get_first_account_for_user,
    get_manageable_account_for_user,
    get_manageable_accounts,
    get_manageable_proposals,
    get_manageable_proposal_for_user,
    get_proposal_choices_for_user,
    sync_memberships_for_account,
    sync_memberships_for_proposal,
)
from custom_code.models import Facility, GeoTarget, TransitEphemeris
from custom_code.models import UserBhtom2UploadPreference
from custom_code.data_services.forms import AllDataServicesQueryForm
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
from custom_code.bhtom_catalogs.harvesters import ogle_ews as ogle_ews_harvester
from custom_code.bhtom_catalogs.harvesters import simbad as simbad_harvester
from custom_code.sun_separation import get_live_target_values


logger = logging.getLogger(__name__)
CATALOG_RESULTS_SESSION_KEY = 'catalog_query_results'
CATALOG_FORM_SESSION_KEY = 'catalog_query_form_data'
TARGET_LIST_OBSERVER_SESSION_KEY = 'target_list_observer'
TARGET_LIST_TIME_SESSION_KEY = 'target_list_time'
LIST_OBSERVER_PRESETS = {
    'warsaw': {'name': 'Warsaw', 'lat_deg': 52.2297, 'lon_deg': 21.0122, 'elevation_m': 100.0},
    'ostrowik': {'name': 'Ostrowik', 'lat_deg': 52.087981, 'lon_deg': 21.41614, 'elevation_m': 120.0},
    'bialkow': {'name': 'Bialkow', 'lat_deg': 51.47425, 'lon_deg': 16.657822, 'elevation_m': 130.0},
    'bolecina': {'name': 'Bolecina', 'lat_deg': 49.819827, 'lon_deg': 19.370521, 'elevation_m': 398.0},
    'moletai': {'name': 'Moletai', 'lat_deg': 55.3189, 'lon_deg': 25.5633, 'elevation_m': 200.0},
    'piwnice': {'name': 'Piwnice', 'lat_deg': 53.09546, 'lon_deg': 18.56406, 'elevation_m': 87.0},
    'lasilla': {'name': 'La Silla', 'lat_deg': -29.2567, 'lon_deg': -70.7346, 'elevation_m': 2400.0},
}
GENERIC_TARGET_SEARCH_RADIUS_ARCSEC = 3.0
ALL_CATALOG_QUERY_SERVICE_NAMES = (
    'ExoClock',
    'Gaia Alerts',
    'Gaia DR3',
    'JPL Horizons',
    'OGLE EWS',
    'Simbad',
    'TNS',
)
ALL_CATALOG_QUERY_TIMEOUT_SECONDS = 12.0


def _normalize_json_safe_value(value):
    if isinstance(value, Time):
        return value.utc.datetime.replace(tzinfo=None, microsecond=0).isoformat()
    if isinstance(value, dict):
        return {key: _normalize_json_safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_json_safe_value(item) for item in value]
    return value


def _serialize_query_parameters(cleaned_data):
    normalized = _normalize_json_safe_value(dict(cleaned_data))
    return json.loads(json.dumps(normalized, cls=DjangoJSONEncoder))


def _find_existing_target_by_name(name):
    normalized = str(name or '').strip()
    if not normalized:
        return None
    return (
        Target.objects.filter(Q(name__iexact=normalized) | Q(aliases__name__iexact=normalized))
        .distinct()
        .first()
    )


def _annotate_results_with_existing_targets(results):
    for result in results:
        target = _find_existing_target_by_name(result.get('name'))
        if target is None:
            continue
        result['existing_target_pk'] = target.pk
        result['existing_target_url'] = reverse('targets:detail', kwargs={'pk': target.pk})
    return results


def _summarize_target_query_result(result):
    preferred_keys = (
        'main_id',
        'source_id',
        'designation',
        'field',
        'classification',
        'comment',
        'type',
        'gaia_variability_type',
    )
    parts = []
    for key in preferred_keys:
        value = result.get(key)
        if value in (None, '', [], {}):
            continue
        parts.append(f'{key}: {value}')
        if len(parts) == 2:
            break
    return ' | '.join(parts)


def _build_data_service_result_row(result, data_service_name, query_id=''):
    row = dict(result)
    row['service'] = data_service_name
    row['summary'] = _summarize_target_query_result(row)
    row['url'] = str(row.get('source_location') or '').strip()
    row['query_id'] = str(query_id or '')
    return row


def _cache_query_result(result_id, payload):
    cache.set(f'result_{result_id}', payload, 3600)


def _save_bhtom2_upload_preference(user, token, oname, calibration_filter):
    if user is None or not getattr(user, 'is_authenticated', False):
        return None
    preference, _ = UserBhtom2UploadPreference.objects.get_or_create(user=user)
    preference.token = str(token or '').strip()
    preference.oname = str(oname or '').strip()
    preference.calibration_filter = str(calibration_filter or 'GaiaSP/any').strip() or 'GaiaSP/any'
    preference.save()
    return preference


def _get_bhtom2_upload_preference(user):
    if user is None or not getattr(user, 'is_authenticated', False):
        return None
    return getattr(user, 'bhtom2_upload_preference', None)


def _build_bhtom2_comment(user, source_label):
    if user is None or not getattr(user, 'is_authenticated', False):
        return source_label
    full_name = user.get_full_name().strip()
    username = user.get_username()
    if full_name:
        return f'{source_label} by {full_name} ({username})'
    return f'{source_label} by {username}'


def _upload_dataproduct_to_bhtom2(dataproduct, *, user, token, oname, calibration_filter, comment):
    return forward_dataproduct_to_bhtom2(
        dataproduct,
        token=token,
        observatory=oname,
        calibration_filter=calibration_filter,
        comment=comment,
        user_id=getattr(user, 'pk', None),
    )


def _run_single_data_service_query(data_service_name, parameters, *, query_id='', cache_prefix='query'):
    service = get_data_service_class(data_service_name)()
    query_parameters = service.build_query_parameters(parameters)
    raw_results = service.query_targets(query_parameters) or []
    rows = []
    for index, result in enumerate(raw_results):
        result_id = f'{cache_prefix}_{data_service_name}_{index}'
        cached_result = dict(result)
        cached_result['id'] = result_id
        _cache_query_result(result_id, cached_result)
        row = _build_data_service_result_row(cached_result, data_service_name, query_id=query_id)
        rows.append(row)
    return _annotate_results_with_existing_targets(rows)


def _run_all_data_services_query(parameters, *, query_id='', cache_prefix='all'):
    rows = []
    feedback = []
    for service_name in sorted(get_data_service_classes().keys()):
        try:
            rows.extend(
                _run_single_data_service_query(
                    service_name,
                    parameters,
                    query_id=query_id,
                    cache_prefix=f'{cache_prefix}_{service_name}',
                )
            )
        except Exception as exc:
            logger.warning('All-data-services query failed for %s: %s', service_name, exc)
            feedback.append(f'{service_name}: query failed')
    rows.sort(key=lambda row: (str(row.get('name') or ''), str(row.get('service') or '')))
    return rows, feedback


def _run_single_catalog_service_query(service_name, cleaned_data):
    matches = _get_catalog_matches(service_name, cleaned_data)
    if matches:
        return [_build_catalog_result_row(service_name, index, row) for index, row in enumerate(matches)]

    term = (cleaned_data.get('term') or '').strip()
    if not term and service_name != 'Simbad':
        return []

    service_class = get_service_classes()[service_name]
    service = service_class()
    if service_name == 'Simbad':
        service.query(
            term,
            ra=cleaned_data.get('ra'),
            dec=cleaned_data.get('dec'),
            radius_arcsec=3.0,
        )
    else:
        service.query(term)
    return [_build_catalog_single_result_row(service_name, service.to_target(), term)]


def _get_all_catalog_query_service_names():
    installed_service_names = set(get_service_classes().keys())
    return [service_name for service_name in ALL_CATALOG_QUERY_SERVICE_NAMES if service_name in installed_service_names]


def _catalog_query_services_for_input(cleaned_data):
    service_names = _get_all_catalog_query_service_names()
    term = str(cleaned_data.get('term') or '').strip()
    if not term:
        return service_names

    lowered_term = term.lower()
    is_tns_like = bool(re.match(r'^(sn|at)\s*\d{4}[a-z]+$', lowered_term)) or bool(re.match(r'^\d{4}[a-z]+$', lowered_term))
    is_exoplanet_like = bool(re.search(r'(?:^|[-_ ])(?:[bcdefghij])$', lowered_term)) or any(
        lowered_term.startswith(prefix)
        for prefix in ('wasp-', 'hat-', 'toi-', 'kelt-', 'xo-', 'tres-', 'corot-', 'kepler-', 'hd ')
    )
    is_solar_system_like = bool(re.match(r'^\(?\d+\)?\s*\w*$', term)) or bool(
        re.match(r'^(c/|p/|\d{4}\s+[a-z]{1,2}\d*)', lowered_term)
    )

    filtered_service_names = []
    for service_name in service_names:
        if service_name == 'TNS' and not is_tns_like:
            continue
        if service_name == 'ExoClock' and not is_exoplanet_like:
            continue
        if service_name == 'JPL Horizons' and not is_solar_system_like:
            continue
        filtered_service_names.append(service_name)
    return filtered_service_names or service_names


def _run_all_catalog_services_query(cleaned_data):
    rows = []
    feedback = []
    service_names = _catalog_query_services_for_input(cleaned_data)
    if not service_names:
        return rows, feedback

    executor = ThreadPoolExecutor(max_workers=len(service_names))
    future_map = {
        executor.submit(_run_single_catalog_service_query, service_name, cleaned_data): service_name
        for service_name in service_names
    }
    timed_out = False
    try:
        for future in as_completed(future_map, timeout=ALL_CATALOG_QUERY_TIMEOUT_SECONDS):
            service_name = future_map[future]
            try:
                rows.extend(future.result())
            except MissingDataException:
                continue
            except Exception as exc:
                logger.warning('All-catalog-services query failed for %s: %s', service_name, exc)
                feedback.append(f'{service_name}: query failed')
    except FuturesTimeoutError:
        timed_out = True
    finally:
        if timed_out:
            feedback.append('Some services timed out')
        for future, service_name in future_map.items():
            if future.done():
                continue
            future.cancel()
            logger.warning('All-catalog-services query timed out for %s', service_name)
            feedback.append(f'{service_name}: timed out')
        executor.shutdown(wait=False, cancel_futures=True)
    rows.sort(key=lambda row: (str(row.get('name') or ''), str(row.get('service') or '')))
    return rows, feedback
EXOCLOCK_RECOMMENDED_OBSERVING_STRATEGY = (
    'Follow the Exoclock emphemeris and observe in one or more bands to cover the entire transit, '
    'with ingres and egres well determined. Adjust the exposure time accordingly to the brightness '
    'of the star and the depth of the transit.'
)


def _set_groups_field_visibility(form, queryset):
    form.fields['groups'].queryset = queryset
    form.show_groups_field = queryset.exists()
    return form


def _add_inline_formset_errors(form, formset, summary_message):
    if any(errors for errors in formset.errors):
        form.add_error(None, summary_message)
    for error in formset.non_form_errors():
        form.add_error(None, error)

def _parse_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_generic_target_search_coordinates(search_term):
    parts = [part.strip() for part in str(search_term or '').split(',')]
    if len(parts) != 2 or not all(parts):
        return None

    ra_text, dec_text = parts
    coordinate_attempts = (
        {'unit': (u.deg, u.deg)},
        {'unit': (u.hourangle, u.deg)},
    )
    for kwargs in coordinate_attempts:
        try:
            coord = SkyCoord(ra_text, dec_text, frame='icrs', **kwargs)
        except (TypeError, ValueError):
            continue
        return coord.ra.deg, coord.dec.deg
    return None


def _is_finite_number(value):
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _extract_photometry_export_mjd(datum):
    if _is_finite_number(getattr(datum, 'mjd', None)):
        return float(datum.mjd)

    timestamp = getattr(datum, 'timestamp', None)
    if not timestamp:
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return float(Time(timestamp, scale='utc').mjd)


def _build_photometry_export_rows(target):
    photometry_type = settings.DATA_PRODUCT_TYPES['photometry'][0]
    rows = []

    for datum in ReducedDatum.objects.filter(target=target, data_type=photometry_type).order_by('timestamp', 'id'):
        mjd = _extract_photometry_export_mjd(datum)
        if not _is_finite_number(mjd):
            continue

        magnitude = None
        error = None
        facility = getattr(datum, 'facility', None) or ''
        filter_name = getattr(datum, 'filter', None) or ''
        observer = getattr(datum, 'observer', None) or ''

        if isinstance(datum.value, dict):
            magnitude = datum.value.get('magnitude')
            if magnitude is None:
                magnitude = datum.value.get('mag')
            if magnitude is None:
                magnitude = datum.value.get('limit')
            error = datum.value.get('error', datum.value.get('magnitude_error'))
            facility = facility or datum.value.get('facility') or datum.value.get('telescope') or datum.source_name or ''
            filter_name = filter_name or datum.value.get('filter') or ''
            observer = observer or datum.value.get('observer') or ''
        else:
            magnitude = datum.value
            facility = facility or datum.source_name or ''

        if not _is_finite_number(magnitude):
            continue
        magnitude = float(magnitude)

        if error is not None and _is_finite_number(error):
            error = float(error)
        elif error in ('', None):
            error = None
        else:
            continue

        rows.append([float(mjd), magnitude, error, facility, filter_name, observer])

    rows.sort(key=lambda row: row[0])
    return rows


def _authenticate_api_token_user(request):
    auth_header = (request.META.get('HTTP_AUTHORIZATION') or '').strip()
    if not auth_header or not auth_header.lower().startswith('token '):
        return None

    token_key = auth_header.split(None, 1)[1].strip() if ' ' in auth_header else ''
    if not token_key:
        return None

    try:
        token = Token.objects.select_related('user').get(key=token_key)
    except Token.DoesNotExist:
        return None
    return token.user if token.user.is_active else None


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


def _resolve_list_calculation_time(request):
    saved = request.session.get(TARGET_LIST_TIME_SESSION_KEY, {}) if hasattr(request, 'session') else {}
    if 'time_utc' in request.GET:
        time_raw = (request.GET.get('time_utc') or '').strip()
    elif request.GET:
        time_raw = ''
    else:
        time_raw = (saved.get('time_utc') or '').strip()
    calculation_time_utc = _parse_utc_datetime(time_raw)
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


def _store_list_calculation_time(request, calculation_time_input):
    if not hasattr(request, 'session'):
        return
    request.session[TARGET_LIST_TIME_SESSION_KEY] = {
        'time_utc': calculation_time_input or '',
    }


def _format_geotom_row(target, sat):
    row = {"target": target}
    if sat is None:
        row.update({
            "alt_deg": None,
            "az_deg": None,
            "hour_angle_hours": None,
            "ra_icrf_hours": None,
            "dec_deg": None,
            "estimated_vmag": None,
            "solar_elongation_deg": None,
            "is_visible": False,
            "hour_angle_sex": "-",
            "ra_icrf_sex": "-",
            "dec_sex": "-",
        })
        return row

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
    return row


def _build_geotom_payload(object_list, observer, calculation_time_utc, visible_only=False):
    map_targets = []
    geotom_rows = []
    for target in object_list:
        sat = geosat_alt_az_from_tle(
            tle_name=target.tle_name or target.name,
            tle_line1=target.tle_line1,
            tle_line2=target.tle_line2,
            observer_lat_deg=observer['lat_deg'],
            observer_lon_deg=observer['lon_deg'],
            observer_elevation_m=observer['elevation_m'],
            when_utc=calculation_time_utc,
        )
        row = _format_geotom_row(target, sat)
        if visible_only and not row['is_visible']:
            continue
        geotom_rows.append(row)

        if sat is None:
            continue

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

    sun_curve_altaz = sun_visibility_curve(
        observer_lat_deg=observer['lat_deg'],
        observer_lon_deg=observer['lon_deg'],
        observer_elevation_m=observer['elevation_m'],
        when_utc=calculation_time_utc,
    )
    sun_hadec = altaz_to_hadec_point(
        sun_curve_altaz['sun_alt_deg'],
        sun_curve_altaz['sun_az_deg'],
        observer['lat_deg'],
    )

    return {
        'rows': geotom_rows,
        'targets': map_targets,
        'visibility_curve_altaz': sun_curve_altaz['curve_points'],
        'visibility_curve_hadec': convert_altaz_curve_to_hadec(
            sun_curve_altaz['curve_points'],
            observer_lat_deg=observer['lat_deg'],
        ),
        'sun_altaz': {
            'az_deg': sun_curve_altaz['sun_az_deg'],
            'alt_deg': sun_curve_altaz['sun_alt_deg'],
        },
        'sun_hadec': {
            'ha_hours': sun_hadec[0],
            'dec_deg': sun_hadec[1],
        },
    }


def _resolve_list_observer(request, observer_presets=None, default_key='warsaw', include_unspecified=False):
    observer_presets = observer_presets or LIST_OBSERVER_PRESETS
    saved = request.session.get(TARGET_LIST_OBSERVER_SESSION_KEY, {}) if hasattr(request, 'session') else {}
    observer_key = (request.GET.get('observer') if 'observer' in request.GET else saved.get('observer', default_key) or default_key).strip().lower()
    lat_raw = (request.GET.get('lat') if 'lat' in request.GET else saved.get('lat', '')).strip()
    lon_raw = (request.GET.get('lon') if 'lon' in request.GET else saved.get('lon', '')).strip()
    elev_raw = (request.GET.get('elev') if 'elev' in request.GET else saved.get('elev', '')).strip()

    if include_unspecified and observer_key == 'unspecified':
        return {
            'key': 'unspecified',
            'name': 'Not Specified',
            'lat_deg': 0.0,
            'lon_deg': 0.0,
            'elevation_m': 0.0,
            'input_lat': '',
            'input_lon': '',
            'input_elev': '',
            'visibility_enabled': False,
            'error': '',
        }

    if observer_key == 'custom':
        lat = _parse_float(lat_raw)
        lon = _parse_float(lon_raw)
        elev = _parse_float(elev_raw, default=100.0)
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
                'visibility_enabled': True,
                'error': '',
            }
        fallback = observer_presets.get(default_key, observer_presets['warsaw'])
        return {
            'key': default_key if default_key in observer_presets else 'warsaw',
            'name': fallback['name'],
            'lat_deg': fallback['lat_deg'],
            'lon_deg': fallback['lon_deg'],
            'elevation_m': fallback['elevation_m'],
            'input_lat': lat_raw,
            'input_lon': lon_raw,
            'input_elev': elev_raw,
            'visibility_enabled': True,
            'error': 'Custom observer requires valid latitude (-90..90) and longitude (-180..180).',
        }

    preset = observer_presets.get(observer_key, observer_presets.get(default_key, observer_presets['warsaw']))
    return {
        'key': observer_key if observer_key in observer_presets else (default_key if default_key in observer_presets else 'warsaw'),
        'name': preset['name'],
        'lat_deg': preset['lat_deg'],
        'lon_deg': preset['lon_deg'],
        'elevation_m': preset['elevation_m'],
        'input_lat': lat_raw,
        'input_lon': lon_raw,
        'input_elev': elev_raw,
        'visibility_enabled': True,
        'error': '',
    }


def _store_list_observer(request, observer):
    if not hasattr(request, 'session'):
        return
    request.session[TARGET_LIST_OBSERVER_SESSION_KEY] = {
        'observer': observer.get('key', ''),
        'lat': observer.get('input_lat', ''),
        'lon': observer.get('input_lon', ''),
        'elev': observer.get('input_elev', ''),
    }


class BhtomPallasBaseMixin:
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update({
            'bhtom_pallas_active_tab': getattr(self, 'bhtom_pallas_active_tab', 'overview'),
        })
        return context


class BhtomPallasView(BhtomPallasBaseMixin, TemplateView):
    template_name = 'tom_common/bhtom_pallas_overview.html'
    bhtom_pallas_active_tab = 'landing'


class BhtomPallasVisibleView(BhtomPallasBaseMixin, TemplateView):
    template_name = 'tom_common/bhtom_pallas_visible.html'
    bhtom_pallas_active_tab = 'visible'


class BhtomPallasPhotometryView(BhtomPallasBaseMixin, TemplateView):
    template_name = 'tom_common/bhtom_pallas_photometry.html'
    bhtom_pallas_active_tab = 'photometry'


class BhtomPallasAView(BhtomPallasBaseMixin, TemplateView):
    template_name = 'tom_common/bhtom_pallas_a.html'
    bhtom_pallas_active_tab = 'a'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        target_input = (self.request.GET.get('target') or '').strip()
        epoch_input = (self.request.GET.get('epoch') or '').strip()
        deltara_input = (self.request.GET.get('deltara') or '').strip()
        deltadec_input = (self.request.GET.get('deltadec') or '').strip()
        max_trailing_input = (self.request.GET.get('max_trailing') or '').strip()
        calculation_requested = any([deltara_input, deltadec_input, max_trailing_input])

        context.update({
            'target_input': target_input,
            'epoch_input': epoch_input,
            'deltara_input': deltara_input,
            'deltadec_input': deltadec_input,
            'max_trailing_input': max_trailing_input,
            'calculation_requested': calculation_requested,
            'observation_planning_error': '',
            'sky_motion_per_min': None,
            'sky_motion_per_sec': None,
            'exposure_time': None,
            'show_import_summary': bool(epoch_input and deltara_input and deltadec_input),
        })

        if not calculation_requested:
            return context

        deltara = _parse_float(deltara_input)
        deltadec = _parse_float(deltadec_input)
        max_trailing = _parse_float(max_trailing_input)

        if not all(_is_finite_number(value) for value in [deltara, deltadec, max_trailing]):
            context['observation_planning_error'] = 'Enter numeric values for dRA*cosD, d(DEC)/dt, and maximum allowed trailing.'
            return context

        if max_trailing <= 0:
            context['observation_planning_error'] = 'Maximum allowed trailing must be greater than zero.'
            return context

        deltara_per_min = deltara / 60
        deltadec_per_min = deltadec / 60
        sky_motion_per_min = (deltara_per_min**2 + deltadec_per_min**2)**0.5
        sky_motion_per_sec = sky_motion_per_min / 60

        if sky_motion_per_sec <= 0:
            context['observation_planning_error'] = 'Sky motion is zero, so a maximum exposure time cannot be computed.'
            return context

        exposure_time = max_trailing / sky_motion_per_sec
        context.update({
            'sky_motion_per_min': sky_motion_per_min,
            'sky_motion_per_sec': sky_motion_per_sec,
            'exposure_time': exposure_time,
        })
        return context


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
            'observation_planning_available': False,
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
            if quantities != self.FULL_OBSERVER_QUANTITIES:
                quantity_set = [item for item in quantities.split(',') if item]
                if '3' not in quantity_set:
                    quantity_set.append('3')
                quantities = ','.join(quantity_set)
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
            handoff_target = ''
            if len(table) and 'targetname' in table.colnames:
                handoff_target = str(table[0]['targetname'])
            if not handoff_target:
                handoff_target = target_query
            row_handoff_available = (
                'RA_rate' in table.colnames and
                'DEC_rate' in table.colnames and
                'datetime_str' in table.colnames
            )
            for row in table[:25]:
                cells = []
                for field in active_fields:
                    cells.append(self._cell_value(row, field))
                planning_url = ''
                if row_handoff_available:
                    planning_url = '{}?{}'.format(
                        reverse('bhtom-pallas-a'),
                        urlencode({
                            'target': handoff_target,
                            'epoch': str(row['datetime_str']),
                            'deltara': str(row['RA_rate']),
                            'deltadec': str(row['DEC_rate']),
                        }),
                    )
                context['ephemeris_rows'].append({
                    'cells': cells,
                    'planning_url': planning_url,
                })
            context['ephemeris_generated_at'] = generated_at
            context['resolved_location_label'] = self._resolve_location_label(resolved_location)
            if len(table) and 'targetname' in table.colnames:
                context['resolved_target_name'] = str(table[0]['targetname'])
            context['observation_planning_available'] = row_handoff_available
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


def _dedupe_alias_rows(rows):
    deduped = []
    seen = set()
    for row in rows:
        if isinstance(row, dict):
            name = str(row.get('name') or '').strip()
            key = name.casefold()
            if not name or key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        elif isinstance(row, str):
            name = row.strip()
            key = name.casefold()
            if not name or key in seen:
                continue
            seen.add(key)
            deduped.append({'name': name})
    return deduped


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


def _get_transit_ephemeris_defaults(form):
    getter = getattr(form, 'get_transit_ephemeris_defaults', None)
    if not callable(getter):
        return None
    return getter()


def _is_planetary_transit_classification(form):
    classification = str(getattr(form, 'cleaned_data', {}).get('classification') or '').strip()
    if not classification:
        classification = str(getattr(getattr(form, 'instance', None), 'classification', '') or '').strip()
    if not classification:
        classification = str(getattr(getattr(form, 'object', None), 'classification', '') or '').strip()
    return classification == 'Planetary Transit'


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
    if service_name == 'OGLE EWS':
        return ogle_ews_harvester.get_all(term)
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
    if service_name == 'OGLE EWS':
        harvester = ogle_ews_harvester.OGLEEWSHarvester()
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
    elif service_name == 'OGLE EWS':
        view_url = ogle_ews_harvester.OGLEEWSHarvester.source_url(match)
        summary = str(match.get('field') or '').strip()
    elif service_name == 'Simbad':
        view_url = simbad_harvester._simbad_url(target.ra, target.dec)
        summary = str(match.get('main_id') or '').strip()
    else:
        view_url = ''
        summary = str(match.get('source_id') or match.get('SOURCE_ID') or '').strip()

    row = {
        'id': index,
        'service': service_name,
        'name': target.name,
        'ra': target.ra,
        'dec': target.dec,
        'summary': summary,
        'url': view_url,
        'create_url': BhtomCatalogSelectResultView._build_create_url(service_name, match),
    }
    return _annotate_results_with_existing_targets([row])[0]


def _build_catalog_single_result_row(service_name, target, query_term=''):
    row = {
        'id': 0,
        'service': service_name,
        'name': target.name,
        'ra': getattr(target, 'ra', None),
        'dec': getattr(target, 'dec', None),
        'summary': str(query_term or '').strip(),
        'url': '',
        'create_url': reverse('targets:create') + '?' + urlencode(_catalog_target_params(target)),
    }
    aliases = getattr(target, 'extra_aliases', None) or []
    for alias in aliases:
        alias_url = str(alias.get('url') or '').strip() if isinstance(alias, dict) else ''
        if alias_url:
            row['url'] = alias_url
            break
    return _annotate_results_with_existing_targets([row])[0]


def _add_transit_target_params(target_params, target):
    transit_mapping = (
        ('source_name', 'transit_source_name'),
        ('source_url', 'transit_source_url'),
        ('planet_name', 'transit_planet_name'),
        ('host_name', 'transit_host_name'),
        ('priority', 'transit_priority'),
        ('t0_bjd_tdb', 'transit_t0_bjd_tdb'),
        ('t0_unc', 'transit_t0_unc'),
        ('period_days', 'transit_period_days'),
        ('period_unc', 'transit_period_unc'),
        ('duration_hours', 'transit_duration_hours'),
        ('depth_r_mmag', 'transit_depth_r_mmag'),
        ('v_mag', 'transit_v_mag'),
        ('r_mag', 'transit_r_mag'),
        ('gaia_g_mag', 'transit_gaia_g_mag'),
    )
    has_transit_payload = False
    for param_name, attr_name in transit_mapping:
        value = getattr(target, attr_name, None)
        if value not in (None, ''):
            target_params[param_name] = value
            has_transit_payload = True
    if has_transit_payload:
        target_params['classification'] = 'Planetary Transit'
        if getattr(target, 'transit_source_name', '') == 'ExoClock':
            target_params['recommended_observing_strategy'] = EXOCLOCK_RECOMMENDED_OBSERVING_STRATEGY
    return target_params


def _catalog_target_params(target):
    target_params = _add_transit_target_params(target.as_dict(), target)
    target_params['names'] = ','.join(
        alias['name'] for alias in getattr(target, 'extra_aliases', []) if alias.get('name')
    )
    alias_payload = BhtomCatalogQueryForm.serialize_alias_payload(target)
    if alias_payload:
        target_params['alias_payload'] = alias_payload
    return target_params


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

    OBSERVER_PRESETS = LIST_OBSERVER_PRESETS
    paginate_by = 20
    ordering = ['-priority', '-created']
    filterset_class = BhtomTargetFilterSet

    @staticmethod
    def _resolve_min_visible_altitude(request):
        raw_value = (request.GET.get('min_alt') or '').strip()
        if not raw_value:
            return 30.0, '30'
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            return 30.0, raw_value
        return value, raw_value

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
        calculation_time_utc, calculation_time_input, calculation_time_error = _resolve_list_calculation_time(self.request)
        observer = _resolve_list_observer(
            self.request,
            observer_presets=self.OBSERVER_PRESETS,
            default_key='unspecified',
            include_unspecified=True,
        )
        _store_list_observer(self.request, observer)
        _store_list_calculation_time(self.request, calculation_time_input)
        visible_only = str(self.request.GET.get('visible_only', '')).lower() in ('1', 'true', 'yes', 'on')
        min_visible_altitude, min_visible_altitude_input = self._resolve_min_visible_altitude(self.request)

        object_list = context.get('object_list', [])
        try:
            base_target_count = object_list.count()
        except (AttributeError, TypeError):
            base_target_count = len(object_list)

        if visible_only and observer.get('visibility_enabled', True):
            visible_targets = []
            for target in object_list:
                live = get_live_target_values(
                    target,
                    time_to_compute=calculation_time_utc,
                    observer_lat_deg=observer['lat_deg'],
                    observer_lon_deg=observer['lon_deg'],
                    observer_elevation_m=observer['elevation_m'],
                )
                altitude_deg = live.get('altitude_deg')
                if altitude_deg is not None and altitude_deg >= min_visible_altitude:
                    visible_targets.append(target)
            object_list = visible_targets
            context['object_list'] = object_list
            paginator = context.get('paginator')
            if paginator is not None:
                paginator.count = len(object_list)

        if visible_only and observer.get('visibility_enabled', True):
            context['target_count'] = len(object_list)
        else:
            context['target_count'] = base_target_count

        if hasattr(self, 'filterset') and self.filterset and self.filterset.data:
            params = [(k, v) for k, v in self.filterset.data.lists() if any(item != '' for item in v)]
            sorted_params = sorted(params, key=lambda item: item[0])
            context['query_string'] = urlencode(sorted_params, doseq=True)
        else:
            context['query_string'] = self.request.META.get('QUERY_STRING', '')

        context['list_filter_hidden_params'] = [
            (key, value)
            for key, values in self.request.GET.lists()
            if key not in {'observer', 'lat', 'lon', 'elev', 'time_utc'}
            for value in values
            if value != ''
        ]
        context['list_generated_utc'] = calculation_time_utc
        context['list_generated_utc_input'] = calculation_time_input
        context['list_time_error'] = calculation_time_error
        context['list_observer'] = observer
        context['list_visible_only'] = visible_only
        context['list_visible_active'] = bool(visible_only and observer.get('visibility_enabled', True))
        context['list_visibility_enabled'] = observer.get('visibility_enabled', True)
        context['list_min_altitude'] = min_visible_altitude
        context['list_min_altitude_input'] = min_visible_altitude_input or '30'
        context['list_observer_presets'] = (
            [{'key': 'unspecified', 'name': 'Not Specified'}] +
            [
            {'key': key, 'name': value['name']}
            for key, value in self.OBSERVER_PRESETS.items()
            ]
        )
        return context


class GenericTargetSearchRedirectView(View):
    def get(self, request, *args, **kwargs):
        search_term = str(request.GET.get('q') or '').strip()
        fallback_url = request.GET.get('next') or resolve_url('home')

        if not search_term:
            messages.error(request, 'Provide a target name or RA,Dec coordinates.')
            return redirect(fallback_url)

        coordinates = _parse_generic_target_search_coordinates(search_term)
        if coordinates is not None:
            ra_deg, dec_deg = coordinates
            query = {
                'cone_search': f'{ra_deg:.8f},{dec_deg:.8f},{GENERIC_TARGET_SEARCH_RADIUS_ARCSEC / 3600.0:.10f}',
            }
        else:
            query = {'name': search_term}

        return redirect(f"{reverse('targets:list')}?{urlencode(query)}")


class BhtomTargetCreateView(TargetCreateView):
    def _is_planetary_transit_target(self):
        classification = (
            self.request.POST.get('classification')
            or self.request.GET.get('classification')
            or self.initial.get('classification')
            or ''
        ).strip()
        return classification == 'Planetary Transit'

    def get_form_class(self):
        target_type = self.get_target_type()
        self.initial['type'] = target_type
        if target_type == Target.SIDEREAL:
            if self._is_planetary_transit_target():
                return BhtomPlanetaryTransitTargetCreateForm
            return BhtomSiderealTargetCreateForm
        return BhtomNonSiderealTargetCreateForm

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        form = context.get('form')
        transit_char_field_names = getattr(form, 'transit_char_field_names', ())
        transit_field_names = getattr(form, 'transit_field_names', ())
        context['transit_char_field_names'] = transit_char_field_names
        context['transit_field_names'] = transit_field_names
        context['transit_char_fields'] = [form[name] for name in transit_char_field_names if form and name in form.fields]
        context['transit_fields'] = [form[name] for name in transit_field_names if form and name in form.fields]
        context['permissions_field'] = form['permissions'] if form and 'permissions' in form.fields else None
        groups_field = form['groups'] if form and 'groups' in form.fields else None
        context['groups_field'] = groups_field
        context['show_groups_field'] = bool(groups_field and getattr(form, 'show_groups_field', False))
        alias_payload = _dedupe_alias_rows(_parse_alias_payload(self.request.GET.get('alias_payload')))
        if self.request.method == 'POST':
            context['names_form'] = BhtomTargetNamesFormset(self.request.POST, instance=getattr(self, 'object', None))
        elif alias_payload:
            context['names_form'] = BhtomTargetNamesFormset(initial=alias_payload)
        else:
            names = _dedupe_alias_rows([{'name': new_name} for new_name in self.request.GET.get('names', '').split(',') if new_name])
            context['names_form'] = BhtomTargetNamesFormset(
                initial=names
            )
        return context

    def get_form(self, *args, **kwargs):
        form = super().get_form(*args, **kwargs)
        form.user = self.request.user
        if self.request.user.is_superuser:
            _set_groups_field_visibility(form, Group.objects.all())
        else:
            _set_groups_field_visibility(form, self.request.user.groups.all())
        if 'permissions' in form.fields and not form.is_bound:
            form.fields['permissions'].initial = 'PUBLIC'
        if 'recommended_observing_strategy' in form.fields:
            value = self.request.GET.get('recommended_observing_strategy')
            if value not in (None, ''):
                form.fields['recommended_observing_strategy'].initial = value
        for field_name in getattr(form, 'transit_char_field_names', ()):
            if field_name in form.fields:
                value = self.request.GET.get(field_name)
                if value not in (None, ''):
                    form.fields[field_name].initial = form.fields[field_name].to_python(value)
        for field_name in getattr(form, 'transit_field_names', ()):
            if field_name in form.fields:
                value = self.request.GET.get(field_name)
                if value not in (None, ''):
                    form.fields[field_name].initial = form.fields[field_name].to_python(value)
        return form

    @transaction.atomic
    def form_valid(self, form):
        self.object = form.save()
        extra = TargetExtraFormset(self.request.POST, instance=self.object)
        names = BhtomTargetNamesFormset(self.request.POST, instance=self.object)
        transit_ephemeris = _get_transit_ephemeris_defaults(form) if _is_planetary_transit_classification(form) else None
        has_transit_ephemeris = transit_ephemeris is not None and (
            any(value not in (None, '') for value in transit_ephemeris.values())
            or hasattr(self.object, 'transit_ephemeris')
        )

        if extra.is_valid() and names.is_valid():
            extra.save()
            names.save()
            if has_transit_ephemeris:
                TransitEphemeris.objects.update_or_create(target=self.object, defaults=transit_ephemeris)
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
        _add_inline_formset_errors(form, extra, 'Please correct the tag errors below.')
        _add_inline_formset_errors(form, names, 'Please correct the alias errors below.')
        transaction.set_rollback(True)
        self.object = None
        form.instance.pk = None
        form.instance._state.adding = True
        return super().form_invalid(form)


class BhtomTargetUpdateView(TargetUpdateView):
    def get_form_class(self):
        if self.object.type == Target.SIDEREAL:
            return BhtomSiderealTargetUpdateForm
        return super().get_form_class()

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        form = context.get('form')
        transit_char_field_names = getattr(form, 'transit_char_field_names', ())
        transit_field_names = getattr(form, 'transit_field_names', ())
        context['transit_char_field_names'] = transit_char_field_names
        context['transit_field_names'] = transit_field_names
        context['transit_char_fields'] = [form[name] for name in transit_char_field_names if form and name in form.fields]
        context['transit_fields'] = [form[name] for name in transit_field_names if form and name in form.fields]
        context['permissions_field'] = form['permissions'] if form and 'permissions' in form.fields else None
        groups_field = form['groups'] if form and 'groups' in form.fields else None
        context['groups_field'] = groups_field
        context['show_groups_field'] = bool(groups_field and getattr(form, 'show_groups_field', False))
        if self.request.method == 'POST':
            context['names_form'] = BhtomTargetNamesFormset(self.request.POST, instance=self.object)
        else:
            context['names_form'] = BhtomTargetNamesFormset(instance=self.object)
        return context

    def get_form(self, *args, **kwargs):
        form = super().get_form(*args, **kwargs)
        form.user = self.request.user
        if self.request.user.is_superuser:
            _set_groups_field_visibility(form, Group.objects.all())
        else:
            _set_groups_field_visibility(form, self.request.user.groups.all())
        return form

    @transaction.atomic
    def form_valid(self, form):
        self.object = form.save()
        extra = TargetExtraFormset(self.request.POST, instance=self.object)
        names = BhtomTargetNamesFormset(self.request.POST, instance=self.object)
        if extra.is_valid() and names.is_valid():
            extra.save()
            names.save()
            transit_ephemeris = _get_transit_ephemeris_defaults(form) if _is_planetary_transit_classification(form) else None
            if transit_ephemeris is not None and (
                any(value not in (None, '') for value in transit_ephemeris.values())
                or hasattr(self.object, 'transit_ephemeris')
            ):
                TransitEphemeris.objects.update_or_create(target=self.object, defaults=transit_ephemeris)
            return redirect(self.get_success_url())
        _add_inline_formset_errors(form, extra, 'Please correct the tag errors below.')
        _add_inline_formset_errors(form, names, 'Please correct the alias errors below.')
        transaction.set_rollback(True)
        return super().form_invalid(form)


class BhtomTargetDetailView(TargetDetailView):
    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        target = self.get_object()
        if target.type == Target.NON_SIDEREAL:
            calculation_time_utc, calculation_time_input, calculation_time_error = _resolve_list_calculation_time(self.request)
            observer = _resolve_list_observer(
                self.request,
                observer_presets=Bhtom2TargetListView.OBSERVER_PRESETS,
                default_key='unspecified',
                include_unspecified=True,
            )
            _store_list_observer(self.request, observer)
            _store_list_calculation_time(self.request, calculation_time_input)
            context['detail_generated_utc'] = calculation_time_utc
            context['detail_generated_utc_input'] = calculation_time_input
            context['detail_time_error'] = calculation_time_error
            context['detail_observer'] = observer
            context['detail_observer_presets'] = (
                [{'key': 'unspecified', 'name': 'Not Specified'}] +
                [
                    {'key': key, 'name': preset['name']}
                    for key, preset in Bhtom2TargetListView.OBSERVER_PRESETS.items()
                ]
            )
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
        if self.request.GET.get('compute_current_coords') == '1' and can_compute_current_coordinates(target):
            context['current_coords'] = compute_current_coordinates(target)
        return context


class BhtomCatalogQueryView(FormView):
    form_class = BhtomCatalogQueryForm
    template_name = 'tom_catalogs/query_form.html'

    def _render_catalog_results(self, form, matches):
        service_name = form.cleaned_data.get('service')
        context = self.get_context_data(form=form)
        context.update({
            'data_service': service_name,
            'query': (form.cleaned_data.get('term') or '').strip(),
            'results': [_build_catalog_result_row(service_name, index, row) for index, row in enumerate(matches)],
        })
        return render(self.request, 'tom_catalogs/query_result.html', context)

    def form_valid(self, form):
        service_name = form.cleaned_data.get('service')
        if service_name == ALL_DATA_SERVICES_VALUE:
            rows, feedback = _run_all_catalog_services_query(form.cleaned_data)
            if not rows:
                form.add_error('term', ValidationError('Object not found'))
                return self.form_invalid(form)
            context = self.get_context_data(form=form)
            context.update({
                'data_service': ALL_DATA_SERVICES_LABEL,
                'query': (form.cleaned_data.get('term') or '').strip(),
                'results': rows,
                'query_feedback': ' | '.join(feedback),
            })
            return render(self.request, 'tom_catalogs/query_result.html', context)

        matches = _get_catalog_matches(service_name, form.cleaned_data)
        if matches:
            return self._render_catalog_results(form, matches)

        if service_name in {'Gaia Alerts', 'Gaia DR3', 'OGLE EWS', 'Simbad'}:
            error_target = 'ra' if service_name == 'Simbad' else 'term'
            form.add_error(error_target, ValidationError('Object not found'))
            return self.form_invalid(form)

        try:
            self.target = form.get_target()
        except MissingDataException:
            error_target = 'ra' if form.cleaned_data.get('service') == 'Simbad' else 'term'
            form.add_error(error_target, ValidationError('Object not found'))
            return self.form_invalid(form)
        context = self.get_context_data(form=form)
        context.update({
            'data_service': service_name,
            'query': (form.cleaned_data.get('term') or '').strip(),
            'results': [_build_catalog_single_result_row(service_name, self.target, form.cleaned_data.get('term') or '')],
        })
        return render(self.request, 'tom_catalogs/query_result.html', context)

    def get_success_url(self):
        return reverse('targets:create') + '?' + urlencode(_catalog_target_params(self.target))


class BhtomCatalogSelectResultView(LoginRequiredMixin, View):
    @staticmethod
    def _build_create_url(service_name, row):
        target = _build_catalog_target_from_match(service_name, row)
        return reverse('targets:create') + '?' + urlencode(_catalog_target_params(target))

    def post(self, request, *args, **kwargs):
        stored_results = request.session.get(CATALOG_RESULTS_SESSION_KEY) or []
        stored_form_data = request.session.get(CATALOG_FORM_SESSION_KEY) or {}
        selected_result = request.POST.get('selected_result')
        service_name = stored_form_data.get('service', '')

        if not stored_results:
            messages.error(request, 'Catalog query results expired. Run the catalog query again.')
        return redirect(reverse('tom_catalogs:query'))


class BhtomCreateTargetFromQueryView(CreateTargetFromQueryView):
    @staticmethod
    def _build_create_url(target, cached_result):
        target_params = target.as_dict()
        for target_key, cache_keys in (
            ('pm_ra', ('pm_ra', 'pmra')),
            ('pm_dec', ('pm_dec', 'pmdec')),
        ):
            value = getattr(target, target_key, None)
            if value in (None, ''):
                for cache_key in cache_keys:
                    value = cached_result.get(cache_key)
                    if value not in (None, ''):
                        break
            if value not in (None, ''):
                target_params[target_key] = value
        parallax = getattr(target, 'parallax', None)
        if parallax in (None, ''):
            parallax = cached_result.get('parallax')
        if parallax not in (None, ''):
            target_params['parallax'] = parallax
        gaia_variability_type = getattr(target, 'gaia_variability_type', None)
        if gaia_variability_type in (None, ''):
            gaia_variability_type = cached_result.get('gaia_variability_type')
        if gaia_variability_type not in (None, ''):
            target_params['gaia_variability_type'] = gaia_variability_type
        for key in ('parallax_error', 'pm_ra_error', 'pm_dec_error'):
            value = cached_result.get(key)
            if value not in (None, ''):
                target_params[key] = value
        target_params['names'] = ','.join(
            alias['name'] for alias in getattr(target, 'extra_aliases', []) if alias.get('name')
        )
        has_transit_payload = any(
            cached_result.get(key) not in (None, '')
            for key in (
                'transit_source_name',
                'transit_source_url',
                'transit_planet_name',
                'transit_host_name',
                'transit_t0_bjd_tdb',
                'transit_period_days',
            )
        )
        if has_transit_payload:
            target_params['classification'] = 'Planetary Transit'
        target_params['source_name'] = cached_result.get('transit_source_name') or target_params.get('source_name') or ''
        target_params['source_url'] = cached_result.get('transit_source_url') or target_params.get('source_url') or ''
        target_params['planet_name'] = cached_result.get('transit_planet_name') or target_params.get('planet_name') or ''
        target_params['host_name'] = cached_result.get('transit_host_name') or target_params.get('host_name') or ''
        for key, value in [
            ('t0_bjd_tdb', cached_result.get('transit_t0_bjd_tdb')),
            ('t0_unc', cached_result.get('transit_t0_unc')),
            ('period_days', cached_result.get('transit_period_days')),
            ('period_unc', cached_result.get('transit_period_unc')),
            ('duration_hours', cached_result.get('transit_duration_hours')),
            ('depth_r_mmag', cached_result.get('transit_depth_r_mmag')),
            ('v_mag', cached_result.get('transit_v_mag')),
            ('r_mag', cached_result.get('transit_r_mag')),
            ('gaia_g_mag', cached_result.get('transit_gaia_g_mag')),
        ]:
            if value not in (None, ''):
                target_params[key] = value
        alias_payload = BhtomCatalogQueryForm.serialize_alias_payload(target)
        if alias_payload:
            target_params['alias_payload'] = alias_payload
        return reverse('targets:create') + '?' + urlencode(target_params)

    def post(self, request, *args, **kwargs):
        query_id = request.POST.get('query_id')
        data_service_name = request.POST.get('data_service')
        results = request.POST.getlist('selected_results')
        if not results:
            messages.warning(request, 'Please select at least one result from which to create a target.')
            if query_id:
                return redirect(reverse('dataservices:run_saved', kwargs={'pk': query_id}))
            return redirect(reverse('dataservices:run'))

        data_service_class = get_data_service_class(data_service_name)()
        selected_result_id = results[0]
        cached_result = cache.get(f'result_{selected_result_id}')
        if not cached_result:
            messages.error(request, 'Could not create targets. Try re-running the query again.')
            if query_id:
                return redirect(reverse('dataservices:run_saved', kwargs={'pk': query_id}))
            return redirect(reverse('dataservices:run'))

        try:
            target, _, _ = data_service_class.to_target(cached_result)
        except MissingDataException:
            messages.error(request, 'Could not create targets. Try re-running the query again.')
            if query_id:
                return redirect(reverse('dataservices:run_saved', kwargs={'pk': query_id}))
            return redirect(reverse('dataservices:run'))

        return HttpResponseRedirect(self._build_create_url(target, cached_result))
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


class BhtomDataServiceQueryCreateView(DataServiceQueryCreateView):
    template_name = 'tom_dataservices/query_form.html'
    success_url = reverse_lazy('dataservices:run')

    def get_form_class(self):
        data_service_name = self.get_data_service_name()
        if not data_service_name:
            return None
        if data_service_name == ALL_DATA_SERVICES_VALUE:
            return AllDataServicesQueryForm
        return super().get_form_class()

    def get(self, request, *args, **kwargs):
        if not self.get_data_service_name():
            return render(request, self.template_name, self.get_context_data(form=None))
        return super().get(request, *args, **kwargs)

    def form_valid(self, form):
        serialized_parameters = _serialize_query_parameters(form.cleaned_data)
        if serialized_parameters['query_save']:
            DataServiceQuery.objects.create(
                name=serialized_parameters['query_name'],
                data_service=serialized_parameters['data_service'],
                parameters=serialized_parameters,
            )
        self.request.session['query_parameters'] = serialized_parameters
        return redirect(self.success_url)

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs) if self.get_data_service_name() else {}
        installed_services = get_data_service_classes()
        selected_service = self.get_data_service_name()
        context['installed_services'] = installed_services
        context['selected_service'] = selected_service
        context['service_class'] = installed_services.get(selected_service) if selected_service and selected_service != ALL_DATA_SERVICES_VALUE else None
        context['all_data_services_value'] = ALL_DATA_SERVICES_VALUE
        context['all_data_services_label'] = ALL_DATA_SERVICES_LABEL
        context['is_all_data_services'] = selected_service == ALL_DATA_SERVICES_VALUE
        return context


class BhtomDataServiceQueryUpdateView(DataServiceQueryUpdateView):
    template_name = 'tom_dataservices/query_form.html'
    success_url = reverse_lazy('dataservices:run')

    def get_form_class(self):
        if self.object.data_service == ALL_DATA_SERVICES_VALUE:
            return AllDataServicesQueryForm
        return super().get_form_class()

    def form_valid(self, form):
        serialized_parameters = _serialize_query_parameters(form.cleaned_data)
        if serialized_parameters['query_save']:
            self.object.name = serialized_parameters['query_name']
            self.object.data_service = serialized_parameters['data_service']
            self.object.parameters = serialized_parameters
            self.object.save()
        self.request.session['query_parameters'] = serialized_parameters
        return redirect(self.success_url)

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        installed_services = get_data_service_classes()
        selected_service = self.object.data_service
        context['installed_services'] = installed_services
        context['selected_service'] = selected_service
        context['service_class'] = installed_services.get(selected_service) if selected_service and selected_service != ALL_DATA_SERVICES_VALUE else None
        context['all_data_services_value'] = ALL_DATA_SERVICES_VALUE
        context['all_data_services_label'] = ALL_DATA_SERVICES_LABEL
        context['is_all_data_services'] = selected_service == ALL_DATA_SERVICES_VALUE
        return context


class BhtomRunQueryView(RunQueryView):
    def _get_query_source(self):
        if self.kwargs.get('pk'):
            query = get_object_or_404(DataServiceQuery, pk=self.kwargs['pk'])
            return query, dict(query.parameters or {})
        return None, dict(self.request.session.get('query_parameters') or {})

    def get(self, request, *args, **kwargs):
        query, parameters = self._get_query_source()
        if parameters.get('data_service') == ALL_DATA_SERVICES_VALUE:
            rows, feedback = _run_all_data_services_query(
                parameters,
                query_id=getattr(query, 'id', ''),
                cache_prefix='dataservices_all',
            )
            context = {
                'data_service': ALL_DATA_SERVICES_LABEL,
                'query': parameters.get('target_name') or '',
                'results': rows,
                'query_object': query,
                'query_feedback': ' | '.join(feedback),
            }
            return render(request, 'tom_dataservices/query_result.html', context)
        return super().get(request, *args, **kwargs)

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        query = context.get('query')
        query_id = getattr(query, 'id', '') if query is not None else ''
        data_service_name = context.get('data_service')
        context['results'] = [
            _build_data_service_result_row(result, data_service_name, query_id=query_id)
            for result in context.get('results', [])
        ]
        context['results'] = _annotate_results_with_existing_targets(context.get('results', []))
        return context


class GeoTomTargetListView(ListView):
    model = GeoTarget
    template_name = 'tom_targets/geotom_target_list.html'
    context_object_name = 'object_list'
    paginate_by = 500
    OBSERVER_PRESETS = LIST_OBSERVER_PRESETS

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
        observer = _resolve_list_observer(self.request, observer_presets=self.OBSERVER_PRESETS)
        calculation_time_utc, calculation_time_input, calculation_time_error = _resolve_list_calculation_time(self.request)
        visible_only = str(self.request.GET.get('visible_only', '')).lower() in ('1', 'true', 'yes', 'on')

        object_list = context.get('object_list', [])
        payload = _build_geotom_payload(object_list, observer, calculation_time_utc, visible_only=visible_only)
        context['geotom_targets_json'] = json.dumps(payload['targets'])
        context['geotom_visibility_curve_altaz_json'] = json.dumps(payload['visibility_curve_altaz'])
        context['geotom_visibility_curve_hadec_json'] = json.dumps(payload['visibility_curve_hadec'])
        context['geotom_sun_altaz_json'] = json.dumps(payload['sun_altaz'])
        context['geotom_sun_hadec_json'] = json.dumps(payload['sun_hadec'])
        context['geotom_rows'] = payload['rows']
        paginator = context.get('paginator')
        if visible_only:
            context['target_count'] = len(payload['rows'])
        else:
            context['target_count'] = paginator.count if paginator else len(object_list)
        context['geotom_generated_utc'] = calculation_time_utc
        context['geotom_generated_utc_input'] = calculation_time_input
        context['geotom_time_error'] = calculation_time_error
        context['geotom_live_mode'] = not (self.request.GET.get('time_utc') or '').strip()
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
        context['geotom_live_update_url'] = reverse('geotom-live-data')
        return context


class GeoTomLiveDataView(View):
    OBSERVER_PRESETS = LIST_OBSERVER_PRESETS

    def get(self, request, *args, **kwargs):
        observer = _resolve_list_observer(request, observer_presets=self.OBSERVER_PRESETS)
        calculation_time_utc, _, calculation_time_error = _resolve_list_calculation_time(request)
        visible_only = str(request.GET.get('visible_only', '')).lower() in ('1', 'true', 'yes', 'on')

        queryset = GeoTarget.objects.all().order_by('name')
        name = (request.GET.get('name') or '').strip()
        norad = (request.GET.get('norad_id') or '').strip()
        object_class = (request.GET.get('object_class') or 'all').strip().lower()

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

        payload = _build_geotom_payload(queryset[:500], observer, calculation_time_utc, visible_only=visible_only)
        rows = []
        for row in payload['rows']:
            rows.append({
                'target_id': row['target'].pk,
                'alt_deg': row['alt_deg'],
                'az_deg': row['az_deg'],
                'hour_angle_sex': row['hour_angle_sex'],
                'ra_icrf_sex': row['ra_icrf_sex'],
                'dec_sex': row['dec_sex'],
                'estimated_vmag': row['estimated_vmag'],
            })

        return JsonResponse({
            'generated_utc': calculation_time_utc.strftime('%Y-%m-%dT%H:%M:%S'),
            'generated_utc_display': calculation_time_utc.strftime('%Y-%m-%d %H:%M:%S'),
            'live_mode': not (request.GET.get('time_utc') or '').strip(),
            'time_error': calculation_time_error,
            'rows': rows,
            'targets': payload['targets'],
            'visibility_curve_altaz': payload['visibility_curve_altaz'],
            'visibility_curve_hadec': payload['visibility_curve_hadec'],
            'sun_altaz': payload['sun_altaz'],
            'sun_hadec': payload['sun_hadec'],
        })


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


@method_decorator(csrf_exempt, name='dispatch')
class TargetDownloadPhotometryDataApiView(View):
    """
    BHTOM2-compatible API endpoint to download target photometry as semicolon-separated text.
    """

    http_method_names = ['post']

    def post(self, request, *args, **kwargs):
        user = _authenticate_api_token_user(request)
        if user is None:
            return JsonResponse({'detail': 'Authentication credentials were not provided.'}, status=401)

        try:
            payload = json.loads(request.body.decode('utf-8') or '{}')
        except (AttributeError, UnicodeDecodeError, json.JSONDecodeError):
            return JsonResponse({'Error': 'Something went wrong'}, status=400)

        target_name = payload.get('name')
        if not isinstance(target_name, str) or not target_name.strip():
            return JsonResponse({'Error': 'Something went wrong'}, status=400)

        try:
            target = Target.objects.get(name=target_name.strip())
        except Target.DoesNotExist:
            return JsonResponse({'Error': f'Target "{target_name.strip()}" not found'}, status=404)

        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="target_{target.name}_photometry.csv"'

        writer = csv.writer(response, delimiter=';')
        writer.writerow(['MJD', 'Magnitude', 'Error', 'Facility', 'Filter', 'Observer'])
        writer.writerows(_build_photometry_export_rows(target))
        return response


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


class ProposalAwareLCOSettings(LCOSettings):
    def __init__(self, account=None):
        super().__init__(facility_name='LCO')
        self.account = account

    def get_setting(self, key):
        if self.account:
            if key == 'portal_url':
                return self.account.account_data.get('portal_url', super().get_setting(key))
            if key == 'archive_url':
                return self.account.account_data.get('archive_url', super().get_setting(key))
            if key == 'api_key':
                return self.account.credentials.get('api_key', '')
        return super().get_setting(key)


class ProposalListView(LoginRequiredMixin, TemplateView):
    template_name = 'tom_common/proposal_list.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        ensure_default_facilities()
        facilities = []
        for facility in Facility.objects.filter(is_active=True).order_by('name'):
            manageable_proposal_ids = set(get_manageable_proposals(self.request.user, facility.code).values_list('pk', flat=True))
            proposals = list(get_accessible_proposals(self.request.user, facility.code).order_by('title', 'external_id'))
            facilities.append({
                'facility': facility,
                'proposals': proposals,
                'can_import': facility.code == 'LCO',
                'can_add_proposal': facility.code != 'LCO',
                'manageable_proposal_ids': manageable_proposal_ids,
            })
        context['facility_sections'] = facilities
        return context


class FacilityAccountCreateView(LoginRequiredMixin, FormView):
    template_name = 'tom_common/proposal_form.html'

    def dispatch(self, request, *args, **kwargs):
        ensure_default_facilities()
        self.facility = get_object_or_404(Facility.objects.filter(is_active=True), code=kwargs['facility_code'])
        return super().dispatch(request, *args, **kwargs)

    def get_form_class(self):
        return FacilityAccountForm

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs.update({'facility': self.facility, 'user': self.request.user})
        return kwargs

    def form_valid(self, form):
        account = form.save()
        sync_memberships_for_account(account, self.request.user, form.cleaned_data['shared_users'])
        messages.success(self.request, f'{self.facility.name} account saved.')
        return redirect('proposal-list')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['page_title'] = f'Add {self.facility.name} Account'
        context['submit_label'] = 'Save account'
        context['cancel_url'] = reverse('proposal-list')
        return context


class FacilityAccountUpdateView(LoginRequiredMixin, FormView):
    template_name = 'tom_common/proposal_form.html'

    def dispatch(self, request, *args, **kwargs):
        self.account = get_object_or_404(get_manageable_accounts(request.user), pk=kwargs['pk'])
        self.facility = self.account.facility
        return super().dispatch(request, *args, **kwargs)

    def get_form_class(self):
        return FacilityAccountForm

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs.update({'facility': self.facility, 'user': self.request.user, 'account': self.account})
        return kwargs

    def form_valid(self, form):
        account = form.save()
        sync_memberships_for_account(account, self.request.user, form.cleaned_data['shared_users'])
        messages.success(self.request, f'{self.facility.name} account updated.')
        return redirect('proposal-list')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['page_title'] = f'Edit {self.facility.name} Account'
        context['submit_label'] = 'Update account'
        context['cancel_url'] = reverse('proposal-list')
        return context


class FacilityAccountDeleteView(LoginRequiredMixin, TemplateView):
    template_name = 'tom_common/proposal_confirm_delete.html'

    def dispatch(self, request, *args, **kwargs):
        self.account = get_object_or_404(get_manageable_accounts(request.user), pk=kwargs['pk'])
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        label = str(self.account)
        self.account.delete()
        messages.success(request, f'{label} deleted.')
        return redirect('proposal-list')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['page_title'] = 'Delete Account'
        context['object_label'] = str(self.account)
        context['cancel_url'] = reverse('proposal-list')
        return context


class FacilityAccountSyncProposalsView(LoginRequiredMixin, View):
    def post(self, request, *args, **kwargs):
        account = get_object_or_404(get_manageable_accounts(request.user), pk=kwargs['pk'])
        try:
            result = sync_remote_proposals_for_account(account)
        except ValueError as exc:
            messages.error(request, str(exc))
        except requests.RequestException as exc:
            account.sync_status = account.SyncStatus.ERROR
            account.last_synced_at = django_timezone.now()
            account.last_sync_error = str(exc)
            account.save(update_fields=['sync_status', 'last_synced_at', 'last_sync_error', 'modified'])
            messages.error(request, f'Proposal sync failed: {exc}')
        else:
            messages.success(
                request,
                'Imported {imported_count}, updated {updated_count}, active {active_count} proposals.'.format(**result),
            )
        return redirect('proposal-list')


class FacilityProposalCreateView(LoginRequiredMixin, FormView):
    template_name = 'tom_common/proposal_form.html'

    def dispatch(self, request, *args, **kwargs):
        ensure_default_facilities()
        self.facility = get_object_or_404(Facility.objects.filter(is_active=True), code=kwargs['facility_code'])
        return super().dispatch(request, *args, **kwargs)

    def get_form_class(self):
        return DirectFacilityProposalForm

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs.update({'facility': self.facility, 'user': self.request.user})
        return kwargs

    def form_valid(self, form):
        proposal = form.save()
        sync_memberships_for_proposal(proposal, self.request.user, form.cleaned_data['shared_users'])
        messages.success(self.request, f'{self.facility.name} proposal saved.')
        return redirect('proposal-list')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['page_title'] = f'Add {self.facility.name} Proposal'
        context['submit_label'] = 'Save proposal'
        context['cancel_url'] = reverse('proposal-list')
        return context


class FacilityProposalUpdateView(LoginRequiredMixin, FormView):
    template_name = 'tom_common/proposal_form.html'

    def dispatch(self, request, *args, **kwargs):
        self.proposal = get_object_or_404(get_manageable_proposals(request.user), pk=kwargs['pk'])
        self.account = self.proposal.account
        return super().dispatch(request, *args, **kwargs)

    def get_form_class(self):
        return DirectFacilityProposalForm

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs.update({'facility': self.account.facility, 'user': self.request.user, 'proposal': self.proposal})
        return kwargs

    def form_valid(self, form):
        proposal = form.save()
        sync_memberships_for_proposal(proposal, self.request.user, form.cleaned_data['shared_users'])
        messages.success(self.request, f'Proposal updated for {self.account.label}.')
        return redirect('proposal-list')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['page_title'] = f'Edit Proposal for {self.account.label}'
        context['submit_label'] = 'Update proposal'
        context['cancel_url'] = reverse('proposal-list')
        return context


class LCOProposalImportView(LoginRequiredMixin, FormView):
    template_name = 'tom_common/proposal_form.html'

    def dispatch(self, request, *args, **kwargs):
        ensure_default_facilities()
        self.facility = get_object_or_404(Facility.objects.filter(is_active=True), code='LCO')
        return super().dispatch(request, *args, **kwargs)

    def get_form_class(self):
        return LCOProposalImportForm

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs.update({'user': self.request.user, 'initial': {'facility': self.facility}})
        return kwargs

    def form_valid(self, form):
        result = form.save()
        messages.success(
            self.request,
            'Imported {imported_count}, updated {updated_count}, active {active_count} proposals.'.format(**result),
        )
        return redirect('proposal-list')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['page_title'] = 'Import LCO Proposals'
        context['submit_label'] = 'Import proposals'
        context['cancel_url'] = reverse('proposal-list')
        return context


class FacilityProposalDeleteView(LoginRequiredMixin, TemplateView):
    template_name = 'tom_common/proposal_confirm_delete.html'

    def dispatch(self, request, *args, **kwargs):
        self.proposal = get_object_or_404(get_manageable_proposals(request.user), pk=kwargs['pk'])
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        label = str(self.proposal)
        self.proposal.delete()
        messages.success(request, f'{label} deleted.')
        return redirect('proposal-list')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['page_title'] = 'Delete Proposal'
        context['object_label'] = str(self.proposal)
        context['cancel_url'] = reverse('proposal-list')
        return context


class ProposalAwareObservationCreateView(TomObservationCreateView):
    def get_initial(self):
        initial = super().get_initial()
        initial['request_user_id'] = self.request.user.pk
        if self.get_facility() == 'LCO':
            target = self.get_target()
            start = datetime.now(timezone.utc).replace(microsecond=0)
            end = start + timedelta(hours=24)
            initial.setdefault('name', f'BHTOM {target.name} {start:%Y%m%d}')
            initial.setdefault('start', start)
            initial.setdefault('end', end)
        return initial

    def _get_lco_facility_settings(self):
        account = get_first_account_for_user(self.request.user, 'LCO')
        return ProposalAwareLCOSettings(account=account)

    def _configure_observation_form(self, form):
        if self.get_facility() == 'LCO' and 'proposal' in form.fields:
            dynamic_choices = get_proposal_choices_for_user(self.request.user, 'LCO', include_account_label=True)
            if dynamic_choices:
                form.fields['proposal'].choices = dynamic_choices
        return form

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        if self.get_facility() == 'LCO':
            kwargs['facility_settings'] = self._get_lco_facility_settings()
        return kwargs

    def get_context_data(self, **kwargs):
        context = super(TomObservationCreateView, self).get_context_data(**kwargs)
        observation_type_choices = []
        initial = self.get_initial()
        bound_form = kwargs.get('form')
        active_observation_type = self.request.POST.get('observation_type')
        for observation_type, observation_form_class in self.get_facility_class().observation_forms.items():
            if bound_form is not None and observation_type == active_observation_type:
                observation_form = bound_form
            else:
                form_data = {**initial, **{'observation_type': observation_type}}
                if observation_type == active_observation_type:
                    form_data.update(**self.request.POST.dict())
                observation_form_class = type(
                    f'Composite{observation_type}Form',
                    (self.get_cadence_strategy_form(), observation_form_class),
                    {},
                )
                form_kwargs = {'initial': form_data}
                if self.get_facility() == 'LCO':
                    form_kwargs['facility_settings'] = self._get_lco_facility_settings()
                observation_form = observation_form_class(**form_kwargs)
            if not settings.TARGET_PERMISSIONS_ONLY and 'groups' in observation_form.fields:
                observation_form.fields['groups'].queryset = self.request.user.groups.all()
            observation_form.helper.form_action = reverse('tom_observations:create', kwargs=self.kwargs)
            self._configure_observation_form(observation_form)
            observation_type_choices.append((observation_type, observation_form))
        context['observation_type_choices'] = observation_type_choices
        context['active'] = active_observation_type
        context['target'] = Target.objects.get(pk=self.get_target_id())
        context.update(self.get_facility_class()().get_facility_context_data())
        return context

    def get_form(self):
        form = super().get_form()
        return self._configure_observation_form(form)

    def form_valid(self, form):
        try:
            return super().form_valid(form)
        except ValidationError as exc:
            form.add_error(None, exc)
            return self.form_invalid(form)

    def post(self, request, *args, **kwargs):
        form_class = self.get_form_class()
        form_kwargs = self.get_form_kwargs()
        form = form_class(**form_kwargs)
        form = self._configure_observation_form(form)

        try:
            form_is_valid = form.is_valid()
        except ValidationError as exc:
            form.add_error(None, exc)
            return self.form_invalid(form)

        if form_is_valid:
            if 'validate' in request.POST:
                return self.form_validation_valid(form)
            return self.form_valid(form)
        return self.form_invalid(form)


class BhtomObservationRecordDetailView(TomObservationRecordDetailView):
    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        context['group_form'] = context.get('form') or AddProductToGroupForm()
        return context


class BhtomObservationProcessView(LoginRequiredMixin, View):
    def post(self, request, *args, **kwargs):
        observation_record = get_object_or_404(ObservationRecord, pk=kwargs['pk'])
        if observation_record.facility != 'LCO':
            messages.warning(request, 'Force process is only available for LCO observations.')
            return redirect(reverse('tom_observations:detail', kwargs={'pk': observation_record.id}))
        if str(observation_record.status or '').strip() != 'COMPLETED':
            messages.warning(request, 'Observation processing is available after the LCO request is completed.')
            return redirect(reverse('tom_observations:detail', kwargs={'pk': observation_record.id}))

        facility = get_service_class(observation_record.facility)()
        facility.set_user(request.user)
        try:
            result = facility.process_completed_observation(observation_record)
        except Exception as exc:
            logger.warning('Forced LCO processing failed for observation %s: %s', observation_record.observation_id, exc)
            messages.warning(request, f'Force process failed: {exc}')
            return redirect(reverse('tom_observations:detail', kwargs={'pk': observation_record.id}))

        messages.success(
            request,
            'Force process finished for observation {0}: frames={1}, created={2}, forwarded={3}, already_forwarded={4}.'.format(
                observation_record.observation_id,
                result.get('frames_seen', 0),
                result.get('created', 0),
                result.get('forwarded', 0),
                result.get('already_forwarded', 0),
            ),
        )
        return redirect(reverse('tom_observations:detail', kwargs={'pk': observation_record.id}))

class UserProfileRedirectView(View):
    def get(self, request, *args, **kwargs):
        if not request.user.is_authenticated:
            return redirect_to_login(request.get_full_path())
        return redirect('user-update', pk=request.user.pk)


class UserCreateWithFixedFormView(TomCommonUserCreateView):
    form_class = BhtomUserCreationForm

    def form_valid(self, form):
        self.object = form.save()
        group, _ = Group.objects.get_or_create(name='Public')
        group.user_set.add(self.object)
        messages.success(self.request, 'User created')
        return HttpResponseRedirect(self.get_success_url())

    def form_invalid(self, form):
        logger.warning('User create form invalid: %s', form.errors.as_json())
        messages.error(self.request, 'User form could not be saved. Check the highlighted fields.')
        return super().form_invalid(form)


class BhtomDataProductUploadView(LoginRequiredMixin, FormView):
    form_class = BhtomDataProductUploadForm

    def get_form(self, form_class=None):
        form = super().get_form(form_class)
        if not settings.TARGET_PERMISSIONS_ONLY:
            if self.request.user.is_superuser:
                form.fields['groups'].queryset = Group.objects.all()
            else:
                form.fields['groups'].queryset = self.request.user.groups.all()
        return form

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs['user'] = self.request.user
        return kwargs

    def form_valid(self, form):
        target = form.cleaned_data['target']
        if not target:
            observation_record = form.cleaned_data['observation_record']
            target = observation_record.target
        else:
            observation_record = None

        dp_type = form.cleaned_data['data_product_type']
        data_product_files = self.request.FILES.getlist('files')
        successful_uploads = []
        bhtom2_successes = []
        bhtom2_failures = []

        if dp_type == 'fits_file':
            _save_bhtom2_upload_preference(
                self.request.user,
                form.cleaned_data.get('bhtom2_upload_token'),
                form.cleaned_data.get('bhtom2_upload_oname'),
                form.cleaned_data.get('bhtom2_upload_filter'),
            )

        for uploaded_file in data_product_files:
            dp = DataProduct(
                target=target,
                observation_record=observation_record,
                data=uploaded_file,
                product_id=None,
                data_product_type=dp_type,
            )
            dp.save()
            try:
                run_hook('data_product_post_upload', dp)
                reduced_data = run_data_processor(dp)
                if not settings.TARGET_PERMISSIONS_ONLY:
                    for group in form.cleaned_data['groups']:
                        assign_perm('tom_dataproducts.view_dataproduct', group, dp)
                        assign_perm('tom_dataproducts.delete_dataproduct', group, dp)
                        assign_perm('tom_dataproducts.view_reduceddatum', group, reduced_data)
                successful_uploads.append(str(dp))
            except InvalidFileFormatException as exc:
                ReducedDatum.objects.filter(data_product=dp).delete()
                dp.delete()
                messages.error(self.request, f'File format invalid for file {dp} -- error was {exc}')
                continue
            except Exception:
                ReducedDatum.objects.filter(data_product=dp).delete()
                dp.delete()
                messages.error(self.request, f'There was a problem processing your file: {dp}')
                continue

            if dp_type != 'fits_file':
                continue

            try:
                _upload_dataproduct_to_bhtom2(
                    dp,
                    user=self.request.user,
                    token=form.cleaned_data.get('bhtom2_upload_token'),
                    oname=form.cleaned_data.get('bhtom2_upload_oname'),
                    calibration_filter=form.cleaned_data.get('bhtom2_upload_filter') or 'GaiaSP/any',
                    comment=_build_bhtom2_comment(self.request.user, 'Uploaded from BHTOM3 Manage Data'),
                )
                bhtom2_successes.append(dp.get_file_name())
            except Exception as exc:
                logger.warning('BHTOM2 FITS upload failed for dataproduct %s: %s', dp.pk, exc)
                bhtom2_failures.append(f'{dp.get_file_name()}: {exc}')

        if successful_uploads:
            messages.success(
                self.request,
                'Successfully uploaded: {0}'.format('\n'.join([p for p in successful_uploads]))
            )
        if bhtom2_successes:
            messages.success(
                self.request,
                'Sent to BHTOM2: {0}'.format(', '.join(bhtom2_successes))
            )
        for failure in bhtom2_failures:
            messages.warning(self.request, f'Local FITS upload succeeded but BHTOM2 forwarding failed: {failure}')

        return redirect(form.cleaned_data.get('referrer', '/'))

    def form_invalid(self, form):
        messages.error(self.request, 'There was a problem uploading your file: {}'.format(form.errors.as_json()))
        return redirect(self.request.POST.get('referrer', '/'))


class BhtomDataProductSaveView(LoginRequiredMixin, View):
    def post(self, request, *args, **kwargs):
        service_class = get_service_class(request.POST['facility'])
        observation_record = ObservationRecord.objects.get(pk=kwargs['pk'])
        products = request.POST.getlist('products')
        if not products:
            messages.warning(request, 'No products were saved, please select at least one dataproduct')
            return redirect(reverse('tom_observations:detail', kwargs={'pk': observation_record.id}))

        if products[0] == 'ALL':
            total_saved_products = service_class().save_data_products(observation_record)
            messages.success(request, 'Saved all available data products')
        else:
            total_saved_products = []
            for product in products:
                saved_products = service_class().save_data_products(observation_record, product)
                total_saved_products += saved_products
                run_hook('data_product_post_save', saved_products)
                messages.success(
                    request,
                    'Successfully saved: {0}'.format('\n'.join([str(p) for p in saved_products]))
                )
            run_hook('multiple_data_products_post_save', total_saved_products)

        preference = _get_bhtom2_upload_preference(request.user)
        fits_candidates = []
        for dataproduct in total_saved_products:
            if ensure_fits_dataproduct_type(dataproduct):
                fits_candidates.append(dataproduct)

        if fits_candidates and (
            preference is None or not preference.token.strip() or not preference.oname.strip()
        ):
            messages.warning(
                request,
                'Saved FITS products locally, but BHTOM2 forwarding is skipped until your BHTOM2 token and ONAME are set.'
            )
            return redirect(reverse('tom_observations:detail', kwargs={'pk': observation_record.id}))

        uploaded_names = []
        skipped_names = []
        failed_uploads = []
        for dataproduct in fits_candidates:
            if has_successful_bhtom2_upload(dataproduct):
                skipped_names.append(dataproduct.get_file_name())
                continue
            try:
                _upload_dataproduct_to_bhtom2(
                    dataproduct,
                    user=request.user,
                    token=preference.token,
                    oname=preference.oname,
                    calibration_filter=preference.calibration_filter or 'GaiaSP/any',
                    comment=_build_bhtom2_comment(
                        request.user,
                        f'Uploaded from BHTOM3 observation {observation_record.observation_id}',
                    ),
                )
                uploaded_names.append(dataproduct.get_file_name())
            except Exception as exc:
                logger.warning('BHTOM2 forwarding failed for dataproduct %s: %s', dataproduct.pk, exc)
                failed_uploads.append(f'{dataproduct.get_file_name()}: {exc}')

        if uploaded_names:
            messages.success(request, 'Forwarded FITS products to BHTOM2: {0}'.format(', '.join(uploaded_names)))
        if skipped_names:
            messages.info(request, 'Already forwarded to BHTOM2: {0}'.format(', '.join(skipped_names)))
        for failure in failed_uploads:
            messages.warning(request, f'FITS product saved locally but BHTOM2 forwarding failed: {failure}')

        return redirect(reverse('tom_observations:detail', kwargs={'pk': observation_record.id}))


class UserUpdateWithTokenView(TomCommonUserUpdateView):
    form_class = BhtomUserUpdateForm

    def form_valid(self, form):
        self.object = form.save()
        if self.object == self.request.user:
            update_session_auth_hash(self.request, self.object)
        messages.success(self.request, 'Profile updated')
        return HttpResponseRedirect(self.get_success_url())

    def form_invalid(self, form):
        logger.warning(
            'User update form invalid for user_id=%s target_id=%s: %s',
            getattr(self.request.user, 'pk', None),
            self.kwargs.get('pk'),
            form.errors.as_json(),
        )
        messages.error(self.request, 'User form could not be saved. Check the highlighted fields.')
        return super().form_invalid(form)

    def get_form(self, form_class=None):
        form = super().get_form(form_class)
        if not self.request.user.is_superuser:
            form.fields.pop('groups', None)
        return form

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        if getattr(self, 'object', None) is not None:
            token, _ = Token.objects.get_or_create(user=self.object)
            context['user_token'] = token.key
        return context


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


class TargetPeriodicityView(LoginRequiredMixin, TemplateView):
    template_name = 'custom_code/target_periodicity.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        target = get_object_or_404(Target, pk=self.kwargs['pk'])

        try:
            photometry_type = settings.DATA_PRODUCT_TYPES['photometry'][0]
        except (AttributeError, KeyError):
            photometry_type = 'photometry'

        if settings.TARGET_PERMISSIONS_ONLY:
            datums = ReducedDatum.objects.filter(target=target, data_type=photometry_type)
        else:
            from guardian.shortcuts import get_objects_for_user
            datums = get_objects_for_user(
                self.request.user,
                'tom_dataproducts.view_reduceddatum',
                klass=ReducedDatum.objects.filter(target=target, data_type=photometry_type),
            )
        datums = datums.order_by('timestamp')

        # Build JSON structure: {filter_name: {telescope_name: [{mjd, mag, err, t}]}}
        series = {}
        for datum in datums:
            mag = datum.value.get('magnitude')
            if mag is None:
                continue  # skip upper limits
            err = datum.value.get('error') or datum.value.get('magnitude_error')
            filter_name = str(datum.value.get('filter') or 'Unknown').strip() or 'Unknown'
            telescope = str(
                datum.value.get('telescope') or datum.value.get('facility') or datum.source_name or 'Unknown'
            ).strip() or 'Unknown'
            try:
                mag = float(mag)
                err = float(err) if err is not None else None
                mjd = float(Time(datum.timestamp).mjd)
            except (TypeError, ValueError):
                continue

            series.setdefault(filter_name, {})
            series[filter_name].setdefault(telescope, [])
            series[filter_name][telescope].append({
                'mjd': round(mjd, 6),
                'mag': round(mag, 4),
                'err': round(err, 4) if err is not None else None,
                't': datum.timestamp.strftime('%Y-%m-%d %H:%M'),
            })

        context['target'] = target
        context['photometry_json'] = json.dumps(series)
        return context


class TargetPeriodicityComputeView(LoginRequiredMixin, View):
    def post(self, request, pk, *args, **kwargs):
        from scipy.optimize import curve_fit

        try:
            body = json.loads(request.body)
            times_mjd = np.array(body['times_mjd'], dtype=float)
            magnitudes = np.array(body['magnitudes'], dtype=float)
            raw_errors = body.get('errors') or []
            errors = np.array(raw_errors, dtype=float) if raw_errors else np.zeros(len(times_mjd))
            min_period = max(float(body.get('min_period', 0.1)), 1e-4)
            max_period = float(body.get('max_period', 1000.0))
        except (KeyError, ValueError, json.JSONDecodeError) as exc:
            return JsonResponse({'error': f'Invalid request: {exc}'}, status=400)

        n = len(times_mjd)
        if n < 5:
            return JsonResponse({'error': f'Need at least 5 data points (got {n})'}, status=400)
        if min_period >= max_period:
            return JsonResponse({'error': 'min_period must be less than max_period'}, status=400)

        # Sort by time
        sort_idx = np.argsort(times_mjd)
        times_mjd = times_mjd[sort_idx]
        magnitudes = magnitudes[sort_idx]
        errors = errors[sort_idx]

        # Only use errors if all positive
        use_errors = errors if (len(errors) == n and np.all(errors > 0)) else None

        # Cap max_period to avoid unconstrained long periods
        time_baseline = float(times_mjd[-1] - times_mjd[0])
        if time_baseline > 0:
            max_period = min(max_period, 2.0 * time_baseline)
        max_period = max(max_period, min_period * 2)

        try:
            ls = LombScargle(times_mjd, magnitudes, use_errors)
            frequency, power = ls.autopower(
                minimum_frequency=1.0 / max_period,
                maximum_frequency=1.0 / min_period,
                samples_per_peak=5,
            )
        except Exception as exc:
            logger.error('LSP computation failed for target pk=%s: %s', pk, exc)
            return JsonResponse({'error': f'LSP computation failed: {exc}'}, status=500)

        periods = 1.0 / frequency
        # Downsample to keep response manageable
        if len(periods) > 15000:
            step = len(periods) // 15000
            periods = periods[::step]
            power = power[::step]

        best_idx = int(np.argmax(power))
        best_period = float(periods[best_idx])

        fap_10 = fap_1 = fap_01 = None
        try:
            fap_levels = ls.false_alarm_level([0.1, 0.01, 0.001])
            fap_10 = float(fap_levels[0])
            fap_1 = float(fap_levels[1])
            fap_01 = float(fap_levels[2])
        except Exception:
            pass

        fit_result = None
        try:
            def sinusoid(t, amplitude, phase, offset):
                return offset + amplitude * np.sin(2 * np.pi / best_period * t + phase)

            p0 = [float(np.std(magnitudes)), 0.0, float(np.mean(magnitudes))]
            popt, _ = curve_fit(sinusoid, times_mjd, magnitudes, p0=p0, maxfev=10000)

            t_fine = np.linspace(times_mjd[0], times_mjd[-1], 600)
            mags_fine = sinusoid(t_fine, *popt)

            fit_result = {
                'times_fine': t_fine.tolist(),
                'mags_fine': mags_fine.tolist(),
                'amplitude': float(popt[0]),
                'phase': float(popt[1]),
                'offset': float(popt[2]),
            }
        except Exception as exc:
            logger.warning('Sinusoidal fit failed for target pk=%s: %s', pk, exc)

        return JsonResponse({
            'periods': periods.tolist(),
            'powers': power.tolist(),
            'best_period': best_period,
            'fap_10': fap_10,
            'fap_1': fap_1,
            'fap_01': fap_01,
            'fit': fit_result,
        })
