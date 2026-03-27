from io import StringIO
import json
import logging
from datetime import datetime, timezone
from urllib.parse import urlencode

from django.contrib import messages
from django.conf import settings
from django.contrib.auth import logout
from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.management import call_command
from django.http import HttpResponseRedirect
from django.shortcuts import resolve_url
from django.views.generic import FormView, ListView, RedirectView
from django.views import View
from django.urls import reverse_lazy
from django.db.models import Q

from tom_common.hints import add_hint
from tom_targets.views import TargetListView
from tom_targets.models import Target

from custom_code.filters import BhtomTargetFilterSet
from custom_code.forms import GeoTomAddSatForm
from custom_code.geosat import (
    geosat_alt_az,
    sun_visibility_curve,
    sun_visibility_curve_ha_dec,
)
from custom_code.models import GeoTarget
from custom_code.data_services.geosat_dataservice import GeoSatDataService
from custom_code.tasks import enqueue_target_dataservices_update


logger = logging.getLogger(__name__)


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
        'piwnice': {'name': 'Piwnice', 'lat_deg': 53.09546, 'lon_deg': 18.56406, 'elevation_m': 87.0},
        'lasilla': {'name': 'La Silla', 'lat_deg': -29.2567, 'lon_deg': -70.7346, 'elevation_m': 2400.0},
    }

    @staticmethod
    def _parse_float(value, default=None):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

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

        if name:
            queryset = queryset.filter(Q(name__icontains=name) | Q(tle_name__icontains=name))
        if norad:
            try:
                queryset = queryset.filter(norad_id=int(norad))
            except ValueError:
                queryset = queryset.none()

        return queryset.order_by('name')

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        observer = self._resolve_observer()
        calculation_time_utc = datetime.now(timezone.utc)

        object_list = context.get('object_list', [])
        map_targets = []
        geotom_rows = []
        for target in object_list:
            row = {"target": target}
            sat = geosat_alt_az(
                norad_id=target.norad_id,
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
                geotom_rows.append(row)
                continue

            row.update({
                "alt_deg": sat["alt_deg"],
                "az_deg": sat["az_deg"],
                "hour_angle_hours": sat["hour_angle_hours"],
                "ra_icrf_hours": sat["ra_icrf_hours"],
                "dec_deg": sat["dec_deg"],
                "estimated_vmag": sat["estimated_vmag"],
                "hour_angle_sex": _hours_to_hms(sat["hour_angle_hours"]),
                "ra_icrf_sex": _hours_to_hms_astro(sat["ra_icrf_hours"]),
                "dec_sex": _deg_to_dms(sat["dec_deg"]),
            })
            geotom_rows.append(row)

            map_targets.append({
                'target_id': target.pk,
                'target_name': target.name,
                'norad_id': target.norad_id,
                'tle_name': sat['tle_name'],
                'alt_deg': sat['alt_deg'],
                'az_deg': sat['az_deg'],
                'hour_angle_hours': sat['hour_angle_hours'],
                'dec_deg': sat['dec_deg'],
                'distance_km': sat['distance_km'],
            })

        context['geotom_targets_json'] = json.dumps(map_targets)
        sun_curve_altaz = sun_visibility_curve(
            observer_lat_deg=observer['lat_deg'],
            observer_lon_deg=observer['lon_deg'],
            observer_elevation_m=observer['elevation_m'],
            when_utc=calculation_time_utc,
        )
        sun_curve = sun_visibility_curve_ha_dec(
            observer_lat_deg=observer['lat_deg'],
            observer_lon_deg=observer['lon_deg'],
            observer_elevation_m=observer['elevation_m'],
            when_utc=calculation_time_utc,
        )
        context['geotom_visibility_curve_altaz_json'] = json.dumps(sun_curve_altaz['curve_points'])
        context['geotom_visibility_curve_hadec_json'] = json.dumps(sun_curve['curve_points'])
        context['geotom_sun_altaz_json'] = json.dumps({
            'az_deg': sun_curve_altaz['sun_az_deg'],
            'alt_deg': sun_curve_altaz['sun_alt_deg'],
        })
        context['geotom_sun_hadec_json'] = json.dumps({
            'ha_hours': sun_curve['sun_ha_hours'],
            'dec_deg': sun_curve['sun_dec_deg'],
        })
        context['geotom_rows'] = geotom_rows
        paginator = context.get('paginator')
        context['target_count'] = paginator.count if paginator else len(object_list)
        context['geotom_generated_utc'] = calculation_time_utc
        context['filter_values'] = {
            'name': (self.request.GET.get('name') or '').strip(),
            'norad_id': (self.request.GET.get('norad_id') or '').strip(),
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
            messages.success(self.request, f'Added satellite {geotarget.name} (NORAD {norad_id}).')
        else:
            messages.success(self.request, f'Updated satellite {geotarget.name} (NORAD {norad_id}).')
        return super().form_valid(form)


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
        return HttpResponseRedirect(f'{self.get_redirect_url(*args, **kwargs)}?{urlencode(query_params)}')

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
