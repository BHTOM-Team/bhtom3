from django import template
from django.core.cache import cache
from plotly import offline
from plotly import graph_objs as go
from tom_observations.facility import get_service_classes

from custom_code.forms import NonSiderealTargetVisibilityForm
from custom_code.facility_proposals import get_current_proposals_for_user
from custom_code.non_sidereal_visibility import get_non_sidereal_visibility
from tom_observations.utils import get_sidereal_visibility

register = template.Library()
NON_SIDEREAL_PLAN_INTERVAL_MINUTES = 30


@register.inclusion_tag('tom_observations/partials/observing_buttons.html', takes_context=True)
def proposal_observing_buttons(context, target):
    request = context['request']
    facilities = get_service_classes()
    if not getattr(request.user, 'is_authenticated', False):
        return {'target': target, 'facilities': {}}

    allowed_codes = set(get_current_proposals_for_user(request.user).values_list('account__facility__code', flat=True))
    visible = {name: clazz for name, clazz in facilities.items() if name in allowed_codes}
    return {'target': target, 'facilities': visible}


@register.inclusion_tag('tom_targets/partials/target_plan.html', takes_context=True)
def sidereal_target_plan(
    context,
    fast_render=False,
    width=600,
    height=400,
    background=None,
    label_color=None,
    grid=True,
):
    request = context['request']
    has_query_values = any(request.GET.get(key) for key in ('start_time', 'end_time', 'airmass'))
    if has_query_values:
        form = NonSiderealTargetVisibilityForm(data={
            'start_time': request.GET.get('start_time', ''),
            'end_time': request.GET.get('end_time', ''),
            'airmass': request.GET.get('airmass', '2.5'),
        })
    else:
        form = NonSiderealTargetVisibilityForm()

    visibility_graph = ''
    if (has_query_values or fast_render) and form.is_valid():
        start_time = form.cleaned_data['start_time']
        end_time = form.cleaned_data['end_time']
        airmass_limit = form.cleaned_data['airmass']
        cache_key = (
            f'sidereal-plan:{context["object"].pk}:'
            f'{start_time.strftime("%Y%m%d%H%M")}:'
            f'{end_time.strftime("%Y%m%d%H%M")}:'
            f'{airmass_limit}'
        )
        visibility_graph = cache.get(cache_key, '')
        if not visibility_graph:
            visibility_data = get_sidereal_visibility(context['object'], start_time, end_time, 10, airmass_limit)
            plot_data = [
                go.Scatter(x=data[0], y=data[1], mode='lines', name=site)
                for site, data in visibility_data.items()
            ]
            layout = go.Layout(
                yaxis=dict(autorange='reversed'),
                width=width,
                height=height,
                paper_bgcolor=background,
                plot_bgcolor=background,
            )
            layout.legend.font.color = label_color
            fig = go.Figure(data=plot_data, layout=layout)
            fig.update_yaxes(
                title='Airmass',
                showgrid=grid,
                color=label_color,
                showline=True,
                linecolor=label_color,
                mirror=True,
            )
            fig.update_xaxes(
                title='Date',
                showgrid=grid,
                color=label_color,
                showline=True,
                linecolor=label_color,
                mirror=True,
            )
            visibility_graph = offline.plot(fig, output_type='div', show_link=False)
            cache.set(cache_key, visibility_graph, 300)

    return {
        'form': form,
        'target': context['object'],
        'visibility_graph': visibility_graph,
    }


@register.inclusion_tag('tom_targets/partials/target_plan.html', takes_context=True)
def nonsidereal_target_plan(
    context,
    fast_render=False,
    width=600,
    height=400,
    background=None,
    label_color=None,
    grid=True,
):
    request = context['request']
    has_query_values = any(request.GET.get(key) for key in ('start_time', 'end_time', 'airmass'))
    if has_query_values:
        plan_form = NonSiderealTargetVisibilityForm(data={
            'start_time': request.GET.get('start_time', ''),
            'end_time': request.GET.get('end_time', ''),
            'airmass': request.GET.get('airmass', '2.5'),
        })
    else:
        plan_form = NonSiderealTargetVisibilityForm()
    visibility_graph = ''
    if has_query_values and plan_form.is_valid():
        start_time = plan_form.cleaned_data['start_time']
        end_time = plan_form.cleaned_data['end_time']
        airmass_limit = plan_form.cleaned_data['airmass']
        cache_key = (
            f'nonsidereal-plan:{context["object"].pk}:'
            f'{start_time.strftime("%Y%m%d%H%M")}:'
            f'{end_time.strftime("%Y%m%d%H%M")}:'
            f'{airmass_limit}'
        )
        visibility_graph = cache.get(cache_key, '')
        if not visibility_graph:
            visibility_data = get_non_sidereal_visibility(
                context['object'],
                start_time,
                end_time,
                NON_SIDEREAL_PLAN_INTERVAL_MINUTES,
                airmass_limit,
            )
            plot_data = [
                go.Scatter(x=data[0], y=data[1], mode='lines', name=site)
                for site, data in visibility_data.items()
            ]
            layout = go.Layout(
                yaxis=dict(autorange='reversed'),
                width=width,
                height=height,
                paper_bgcolor=background,
                plot_bgcolor=background,
            )
            layout.legend.font.color = label_color
            fig = go.Figure(data=plot_data, layout=layout)
            fig.update_yaxes(
                title='Airmass',
                showgrid=grid,
                color=label_color,
                showline=True,
                linecolor=label_color,
                mirror=True,
            )
            fig.update_xaxes(
                title='Date',
                showgrid=grid,
                color=label_color,
                showline=True,
                linecolor=label_color,
                mirror=True,
            )
            visibility_graph = offline.plot(fig, output_type='div', show_link=False)
            cache.set(cache_key, visibility_graph, 300)

    return {
        'form': plan_form,
        'target': context['object'],
        'visibility_graph': visibility_graph,
    }
