from datetime import datetime, timedelta

from django import template
from django.core.cache import cache
from plotly import offline
from plotly import graph_objs as go

from custom_code.forms import NonSiderealTargetVisibilityForm
from custom_code.non_sidereal_visibility import get_non_sidereal_visibility

register = template.Library()


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
    default_start = datetime.utcnow().replace(second=0, microsecond=0)
    default_end = default_start + timedelta(days=1)
    form_data = {
        'start_time': request.GET.get('start_time', default_start.strftime('%Y-%m-%dT%H:%M:%S')),
        'end_time': request.GET.get('end_time', default_end.strftime('%Y-%m-%dT%H:%M:%S')),
        'airmass': request.GET.get('airmass', '2.5'),
    }
    plan_form = NonSiderealTargetVisibilityForm(data=form_data)
    visibility_graph = ''
    should_render = (
        request.GET.get('tab') == 'observe' or
        any(request.GET.get(key) for key in ('start_time', 'end_time', 'airmass'))
    )
    if should_render and plan_form.is_valid():
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
                10,
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
