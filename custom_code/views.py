from io import StringIO
import json
import logging
import requests
from datetime import datetime, timezone
from urllib.parse import urlencode
from uuid import uuid4

from django.contrib import messages
from django.conf import settings
from django.contrib.auth import logout
from django.contrib.auth.models import Group
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.sites.shortcuts import get_current_site
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.http import HttpResponseRedirect
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


class BhtomPallasView(TemplateView):
    template_name = 'tom_common/bhtom_pallas.html'


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
