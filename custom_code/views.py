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
from tom_targets.models import Target

from custom_code.tasks import enqueue_target_dataservices_update


logger = logging.getLogger(__name__)


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
        out = StringIO()

        if target_id:
            if isinstance(target_id, list):
                target_id = target_id[-1]
            call_command('updatereduceddata', target_id=target_id, stdout=out)
            self._enqueue_dataservices_for_target(target_id)
        else:
            call_command('updatereduceddata', stdout=out)
            self._enqueue_dataservices_for_all_targets()

        messages.info(request, out.getvalue())
        add_hint(
            request,
            'DataServices updates were enqueued in the background. Refresh photometry in a moment if needed.',
        )
        return HttpResponseRedirect(f'{self.get_redirect_url(*args, **kwargs)}?{urlencode(query_params)}')

    def get_redirect_url(self, *args, **kwargs):
        return self.request.META.get('HTTP_REFERER', '/')

    def _enqueue_dataservices_for_target(self, target_id):
        try:
            enqueue_target_dataservices_update(int(target_id))
        except Exception as exc:
            logger.warning('Could not enqueue DataServices for target %s: %s', target_id, exc)

    def _enqueue_dataservices_for_all_targets(self):
        for pk in Target.objects.values_list('pk', flat=True).iterator():
            try:
                enqueue_target_dataservices_update(pk)
            except Exception as exc:
                logger.warning('Could not enqueue DataServices for target %s: %s', pk, exc)
