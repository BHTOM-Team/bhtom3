from io import StringIO
import logging
from urllib.parse import urlencode

from django.contrib import messages
from django.conf import settings
from django.contrib.auth import logout
from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.management import call_command
from django.http import HttpResponseRedirect
from django.shortcuts import resolve_url
from django.views.generic import RedirectView
from django.views import View

from tom_common.hints import add_hint
from tom_targets.views import TargetListView
from tom_targets.models import Target

from custom_code.filters import BhtomTargetFilterSet
from custom_code.tasks import enqueue_target_dataservices_update


logger = logging.getLogger(__name__)


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
