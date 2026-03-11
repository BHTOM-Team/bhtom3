from astropy.time import Time
from django import template
from django.conf import settings
from guardian.shortcuts import get_objects_for_user
from plotly import offline
import plotly.graph_objs as go
import numpy as np

from tom_dataproducts.models import ReducedDatum
from tom_dataproducts.processors.data_serializers import SpectrumSerializer


register = template.Library()


# Color map to be used in all plots.
PHOTOMETRY_COLOR_MAP = {
    'GSA(G)': ['black', 'hexagon', 8],
    'ZTF(zg)': ['green', 'x', 6],
    'ZTF(zi)': ['#800000', 'x', 6],
    'ZTF(zr)': ['red', 'x', 6],
    'ZTF(g)': ['green', 'x', 6],
    'ZTF(i)': ['#800000', 'x', 6],
    'ZTF(r)': ['red', 'x', 6],
    'WISE(W1)': ['#FFCC00', 'x', 3],
    'WISE(W2)': ['blue', 'x', 3],
    'CRTS(CL)': ['#FF1493', 'diamond', 4],
    'LINEAR(CL)': ['teal', 'diamond', 4],
    'SDSS(r)': ['red', 'square', 5],
    'SDSS(i)': ['#800000', 'square', 5],
    'SDSS(u)': ['#40E0D0', 'square', 5],
    'SDSS(z)': ['#ff0074', 'square', 5],
    'SDSS(g)': ['green', 'square', 5],
    'DECAPS(r)': ['red', 'star-square', 5],
    'DECAPS(i)': ['#800000', 'star-square', 5],
    'DECAPS(u)': ['#40E0D0', 'star-square', 5],
    'DECAPS(z)': ['#ff0074', 'star-square', 5],
    'DECAPS(g)': ['green', 'star-square', 5],
    'PS1(r)': ['red', 'star-open', 5],
    'PS1(i)': ['#800000', "star-open", 5],
    'PS1(z)': ['#ff0074', "star-open", 5],
    'PS1(g)': ['green', "star-open", 5],
    'GaiaDR3(RP)': ['#ff8A8A', 'circle', 4],
    'GaiaDR3(BP)': ['#8A8Aff', 'circle', 4],
    'GaiaDR3(G)': ['black', 'circle', 4],
    'RP(GaiaDR3)': ['#ff8A8A', '21', 4],
    'BP(GaiaDR3)': ['#8A8Aff', '21', 4],
    'G(GaiaDR3)': ['black', '21', 4],
    'I(GaiaSP)': ['#6c1414', '21', 4],
    'g(GaiaSP)': ['green', '21', 4],
    'R(GaiaSP)': ['#d82727', '21', 4],
    'V(GaiaSP)': ['darkgreen', '21', 4],
    'B(GaiaSP)': ['#000034', '21', 4],
    'z(GaiaSP)': ['#ff0074', '21', 4],
    'u(GaiaSP)': ['#40E0D0', '21', 4],
    'r(GaiaSP)': ['red', '21', 4],
    'U(GaiaSP)': ['#5ac6bc', '21', 4],
    'i(GaiaSP)': ['#800000', '21', 4],
    'ASASSN(g)': ['green', 'cross-thin', 2],
    'ASASSN(V)': ['darkgreen', 'cross-thin', 2],
    'OGLE(I)': ['#800080', 'diamond', 4],
    'ATLAS(c)': ['#1f7e7d', 'circle', 2],
    'ATLAS(o)': ['#f88f1e', 'circle', 2],
    'KMTNET(I)': ['#8c4646', 'diamond-tall', 2],
    '2MASS(J)': ['#1f77b4', 'circle', 2],
    '2MASS(H)': ['#ff7f0e', 'circle', 2],
    '2MASS(K)': ['#2ca02c', 'circle', 2],
    '(J)2MASS': ['#1f77b4', 'circle', 2],
    '(H)2MASS': ['#ff7f0e', 'circle', 2],
    '(K)2MASS': ['#2ca02c', 'circle', 2],
    'PTF(g)': ['green', 'diamond', 5],
    'PTF(R)': ['#800000', 'diamond', 5],
    'uvv': ['#90ee90', 'circle', 4],
    'ubb': ['#add8e6', 'circle', 4],
    'uuu': ['#e6e6fa', 'circle', 4],
    'uw2': ['#2A013D', 'circle', 4],
    'um2': ['#4D023E', 'circle', 4],
    'uw1': ['#3D011A', 'circle', 4],
    'GALEX(NUV)': ['#6A0DAD', 'star-square', 6],
    'GALEX(FUV)': ['#4169E1', 'star-square', 6],
    'UVOT(V)': ['#90ee90', 'circle', 4],
    'UVOT(B)': ['#add8e6', 'circle', 4],
    'UVOT(U)': ['#e6e6fa', 'circle', 4],
    'UVOT(UVW2)': ['#2A013D', 'circle', 4],
    'UVOT(UVM2)': ['#4D023E', 'circle', 4],
    'UVOT(UVW1)': ['#3D011A', 'circle', 4],
    'SkyMapper(u)': ['#40E0D0', 'triangle-up-open', 5],
    'SkyMapper(g)': ['green', 'triangle-up-open', 5],
    'SkyMapper(r)': ['red', 'triangle-up-open', 5],
    'SkyMapper(i)': ['#800000', 'triangle-up-open', 5],
    'SkyMapper(z)': ['#ff0074', 'triangle-up-open', 5],
    'SkyMapper(v)': ['darkgreen', 'triangle-up-open', 5],
    'LSST(u)': ['#40E0D0', 'pentagon-open', 5],
    'LSST(g)': ['green', 'pentagon-open', 5],
    'LSST(r)': ['red', 'pentagon-open', 5],
    'LSST(i)': ['#800000', 'pentagon-open', 5],
    'LSST(z)': ['#ff0074', 'pentagon-open', 5],
    'LSST(y)': ['#DAA520', 'pentagon-open', 5],
}

# Color map for limits (non-detections).
PHOTOMETRY_LIMITS_COLOR_MAP = {
    'GSA(G)': ['black', 'arrow-down-open', 8],
    'ZTF(zg)': ['green', 'arrow-down-open', 6],
    'ZTF(zi)': ['#800000', 'arrow-down-open', 6],
    'ZTF(zr)': ['red', 'arrow-down-open', 6],
    'ZTF(g)': ['green', 'arrow-down-open', 6],
    'ZTF(i)': ['#800000', 'arrow-down-open', 6],
    'ZTF(r)': ['red', 'arrow-down-open', 6],
    'WISE(W1)': ['#FFCC00', 'arrow-down-open', 3],
    'WISE(W2)': ['blue', 'arrow-down-open', 3],
    'CRTS(CL)': ['#FF1493', 'arrow-down-open', 4],
    'LINEAR(CL)': ['teal', 'arrow-down-open', 4],
    'SDSS(r)': ['red', 'arrow-down-open', 5],
    'SDSS(i)': ['#800000', 'arrow-down-open', 5],
    'SDSS(u)': ['#40E0D0', 'arrow-down-open', 5],
    'SDSS(z)': ['#ff0074', 'arrow-down-open', 5],
    'SDSS(g)': ['green', 'arrow-down-open', 5],
    'DECAPS(r)': ['red', 'arrow-down-open', 5],
    'DECAPS(i)': ['#800000', 'arrow-down-open', 5],
    'DECAPS(u)': ['#40E0D0', 'arrow-down-open', 5],
    'DECAPS(z)': ['#ff0074', 'arrow-down-open', 5],
    'DECAPS(g)': ['green', 'arrow-down-open', 5],
    'PS1(r)': ['red', 'arrow-down-open', 5],
    'PS1(i)': ['#800000', "v", 5],
    'PS1(z)': ['#ff0074', "v", 5],
    'PS1(g)': ['green', "v", 5],
    'RP(Gaia DR3)': ['#ff8A8A', 'arrow-down-open', 4],
    'BP(Gaia DR3)': ['#8A8Aff', 'arrow-down-open', 4],
    'G(Gaia DR3)': ['black', 'arrow-down-open', 4],
    'RP(GaiaDR3)': ['#ff8A8A', 'arrow-down-open', 4],
    'BP(GaiaDR3)': ['#8A8Aff', 'arrow-down-open', 4],
    'G(GaiaDR3)': ['black', 'arrow-down-open', 4],
    'I(GaiaSP)': ['#6c1414', 'arrow-down-open', 4],
    'g(GaiaSP)': ['green', 'arrow-down-open', 4],
    'R(GaiaSP)': ['#d82727', 'arrow-down-open', 4],
    'V(GaiaSP)': ['darkgreen', 'arrow-down-open', 4],
    'B(GaiaSP)': ['#000034', 'arrow-down-open', 4],
    'z(GaiaSP)': ['#ff0074', 'arrow-down-open', 4],
    'u(GaiaSP)': ['#40E0D0', 'arrow-down-open', 4],
    'r(GaiaSP)': ['red', 'arrow-down-open', 4],
    'U(GaiaSP)': ['#5ac6bc', 'arrow-down-open', 4],
    'i(GaiaSP)': ['#800000', 'arrow-down-open', 4],
    'ASASSN(g)': ['green', 'arrow-down-open', 2],
    'ASASSN(V)': ['darkgreen', 'arrow-down-open', 2],
    'OGLE(I)': ['#800080', 'arrow-down-open', 4],
    'ATLAS(c)': ['#1f7e7d', 'arrow-down-open', 2],
    'ATLAS(o)': ['#f88f1e', 'arrow-down-open', 2],
    'KMTNET(I)': ['#8c4646', 'arrow-down-open', 2],
    '2MASS(J)': ['#1f77b4', 'arrow-down-open', 2],
    '2MASS(H)': ['#ff7f0e', 'arrow-down-open', 2],
    '2MASS(K)': ['#2ca02c', 'arrow-down-open', 2],
    'PTF(g)': ['green', 'arrow-down-open', 5],
    'PTF(R)': ['#800000', 'arrow-down-open', 5],
    'SkyMapper(u)': ['#40E0D0', 'arrow-down-open', 5],
    'SkyMapper(g)': ['green', 'arrow-down-open', 5],
    'SkyMapper(r)': ['red', 'arrow-down-open', 5],
    'SkyMapper(i)': ['#800000', 'arrow-down-open', 5],
    'SkyMapper(z)': ['#ff0074', 'arrow-down-open', 5],
    'SkyMapper(V)': ['darkgreen', 'arrow-down-open', 5],
}


@register.inclusion_tag('tom_dataproducts/partials/photometry_for_target.html', takes_context=True)
def custom_photometry_for_target(context, target, width=1000, height=600, background=None, label_color=None, grid=True):
    try:
        photometry_data_type = settings.DATA_PRODUCT_TYPES['photometry'][0]
    except (AttributeError, KeyError):
        photometry_data_type = 'photometry'

    photometry_data = {}
    limits_data = {}
    if settings.TARGET_PERMISSIONS_ONLY:
        datums = ReducedDatum.objects.filter(target=target, data_type=photometry_data_type)
    else:
        datums = get_objects_for_user(
            context['request'].user,
            'tom_dataproducts.view_reduceddatum',
            klass=ReducedDatum.objects.filter(target=target, data_type=photometry_data_type),
        )

    magnitude_min = -100.0
    magnitude_max = 100.0
    skip_filters = {
        'G(GAIA_ALERTS)', 'SDSSDR(u)', 'SDSSDR(g)', 'SDSSDR(r)', 'SDSSDR(i)', 'SDSS(z)',
        'SDSS_DR14(u)', 'SDSS_DR14(g)', 'SDSS_DR14(r)', 'SDSS_DR14(i)', 'SDSS_DR14(z)',
    }

    for datum in datums:
        filter_name = str(datum.value.get('filter', '')).strip()
        if not filter_name or filter_name in skip_filters:
            continue

        value = datum.value.get('magnitude')
        if value is None:
            value = datum.value.get('limit')
        error = datum.value.get('error', datum.value.get('magnitude_error'))
        try:
            value = float(value) if value is not None else None
            error = float(error) if error is not None else None
        except (TypeError, ValueError):
            continue
        if value is None:
            continue

        facility = datum.value.get('telescope') or datum.value.get('facility') or datum.source_name or ''
        observer = datum.value.get('observer') or ''
        link = f"/dataproducts/data/{datum.data_product_id}/" if datum.data_product_id else ''
        custom = f"{facility}, {observer}".strip(', ')

        is_limit = (datum.value.get('limit') is not None) or (error is not None and error <= 0)
        target_bucket = limits_data if is_limit else photometry_data
        target_bucket.setdefault(filter_name, {})
        target_bucket[filter_name].setdefault('time', []).append(datum.timestamp)
        target_bucket[filter_name].setdefault('magnitude', []).append(np.around(value, 3))
        target_bucket[filter_name].setdefault('error', []).append(np.around(error if error is not None else 0.0, 3))
        target_bucket[filter_name].setdefault('customdata', []).append(custom)
        target_bucket[filter_name].setdefault('link', []).append(link)

        if not is_limit and error is not None:
            magnitude_min = max(magnitude_min, value + error)
            magnitude_max = min(magnitude_max, value - error)

    plot_data = []
    mjds_to_plot = {}
    for filter_name, filter_values in photometry_data.items():
        if filter_values.get('magnitude'):
            mjds_to_plot[filter_name] = Time(filter_values['time'], format='datetime').mjd

    for filter_name, filter_values in photometry_data.items():
        if not filter_values.get('magnitude'):
            continue
        plot_data.append(
            go.Scatter(
                x=filter_values['time'],
                y=filter_values['magnitude'],
                mode='markers',
                opacity=0.75,
                marker=dict(
                    color=PHOTOMETRY_COLOR_MAP.get(filter_name, ['gray', 'circle', 4])[0],
                    symbol=PHOTOMETRY_COLOR_MAP.get(filter_name, ['gray', 'circle', 4])[1],
                    size=1.2 * PHOTOMETRY_COLOR_MAP.get(filter_name, ['gray', 'circle', 4])[2],
                ),
                name=filter_name,
                error_y=dict(type='data', array=filter_values['error'], visible=True, thickness=0.5),
                text=mjds_to_plot[filter_name],
                customdata=list(zip(filter_values['customdata'], filter_values['link'])),
                hovertemplate='%{x|%Y/%m/%d %H:%M:%S.%L}<br>MJD= %{text:.6f}'
                              '<br>mag= %{y:.3f}&#177;%{error_y.array:.3f}'
                              '<br>%{customdata[0]}<br>%{customdata[1]}',
            )
        )

    limit_mjds_to_plot = {}
    for filter_name, filter_values in limits_data.items():
        if filter_values.get('magnitude'):
            limit_mjds_to_plot[filter_name] = Time(filter_values['time'], format='datetime').mjd

    for filter_name, filter_values in limits_data.items():
        if not filter_values.get('magnitude'):
            continue
        plot_data.append(
            go.Scatter(
                x=filter_values['time'],
                y=filter_values['magnitude'],
                mode='markers',
                visible='legendonly',
                opacity=0.5,
                marker=dict(
                    color=PHOTOMETRY_LIMITS_COLOR_MAP.get(filter_name, ['gray', 'arrow-down-open', 4])[0],
                    symbol=PHOTOMETRY_LIMITS_COLOR_MAP.get(filter_name, ['gray', 'arrow-down-open', 4])[1],
                    size=1.2 * PHOTOMETRY_LIMITS_COLOR_MAP.get(filter_name, ['gray', 'arrow-down-open', 4])[2],
                ),
                name=f'{filter_name}-LIMIT',
                text=limit_mjds_to_plot[filter_name],
                customdata=list(zip(filter_values['customdata'], filter_values['link'])),
                hovertemplate='%{x|%Y/%m/%d %H:%M:%S.%L}<br>MJD = %{text:.6f}'
                              '<br>limit mag = %{y:.3f}'
                              '<br>%{customdata[0]}<br>%{customdata[1]}',
            )
        )

    fig = go.Figure(
        data=plot_data,
        layout=go.Layout(
            height=height,
            width=width,
            paper_bgcolor=background,
            plot_bgcolor=background,
        ),
    )

    fig.update_layout(
        showlegend=True,
        margin=dict(t=40, r=20, b=40, l=80),
        xaxis=dict(
            autorange=True,
            title='date',
            showgrid=grid,
            color=label_color,
            showline=True,
            linecolor=label_color,
            mirror=True,
        ),
        yaxis=dict(
            autorange=False,
            range=[np.ceil(magnitude_min), np.floor(magnitude_max)],
            title='magnitude',
            showgrid=grid,
            color=label_color,
            showline=True,
            linecolor=label_color,
            mirror=True,
            zeroline=False,
        ),
        legend=dict(
            yanchor='top',
            y=-0.15,
            xanchor='left',
            x=0.0,
            orientation='h',
            font=dict(color=label_color),
        ),
        clickmode='event+select',
    )

    request = context.get('request')
    return {'target': target, 'plot': offline.plot(fig, output_type='div', show_link=False), 'request': request}


@register.inclusion_tag('tom_dataproducts/partials/spectroscopy_for_target.html', takes_context=True)
def custom_spectroscopy_for_target(context, target, dataproduct=None):
    try:
        spectroscopy_data_type = settings.DATA_PRODUCT_TYPES['spectroscopy'][0]
    except (AttributeError, KeyError):
        spectroscopy_data_type = 'spectroscopy'

    datums = ReducedDatum.objects.filter(target=target, data_type=spectroscopy_data_type)
    if dataproduct:
        datums = datums.filter(data_product=dataproduct)

    if not settings.TARGET_PERMISSIONS_ONLY:
        datums = get_objects_for_user(
            context['request'].user,
            'tom_dataproducts.view_reduceddatum',
            klass=datums,
        )

    serializer = SpectrumSerializer()
    plot_data = []
    for datum in datums.order_by('timestamp'):
        try:
            spectrum = serializer.deserialize(datum.value)
        except Exception:
            continue
        label = datum.value.get('filter') or datum.value.get('spectrum_type') or datum.timestamp.strftime('%Y%m%d-%H:%M:%S')
        plot_data.append(
            go.Scatter(
                x=spectrum.wavelength.value,
                y=spectrum.flux.value,
                name=label,
                hovertemplate='lambda=%{x:.2f}<br>flux=%{y:.4e}<extra>%{fullData.name}</extra>',
            )
        )

    figure = go.Figure(
        data=plot_data,
        layout=go.Layout(
            height=600,
            width=1000,
            xaxis=dict(title='Wavelength'),
            yaxis=dict(title='Flux density', tickformat='.2e'),
        ),
    )
    request = context.get('request')
    return {'target': target, 'plot': offline.plot(figure, output_type='div', show_link=False), 'request': request}
