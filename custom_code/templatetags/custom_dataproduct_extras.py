from astropy.time import Time
from django import template
from django.conf import settings
from django.contrib.auth.models import Group
from django.shortcuts import reverse
from guardian.shortcuts import get_objects_for_user
from plotly import offline
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import numpy as np
import astropy.units as u
import json

from tom_dataproducts.models import ReducedDatum
from tom_dataproducts.processors.data_serializers import SpectrumSerializer
from tom_observations.models import ObservationRecord
from tom_targets.models import Target

from custom_code.forms import BhtomDataProductUploadForm


register = template.Library()


@register.inclusion_tag('tom_dataproducts/partials/upload_dataproduct.html', takes_context=True)
def upload_dataproduct(context, obj):
    user = context['user']
    initial = {}
    if isinstance(obj, Target):
        initial['target'] = obj
        initial['referrer'] = reverse('tom_targets:detail', args=(obj.id,))
    elif isinstance(obj, ObservationRecord):
        initial['observation_record'] = obj
        initial['referrer'] = reverse('tom_observations:detail', args=(obj.id,))
    form = BhtomDataProductUploadForm(initial=initial, user=user)
    if not settings.TARGET_PERMISSIONS_ONLY:
        if user.is_superuser:
            form.fields['groups'].queryset = Group.objects.all()
        else:
            form.fields['groups'].queryset = user.groups.all()
    return {'data_product_form': form}


# Color map to be used in all plots.
PHOTOMETRY_COLOR_MAP = {
    # The legacy label is retained only so already-ingested corrected rows are not grey.
    'WASP/SuperWASP': ['#7b2cbf', 'circle', 3],
    'WASP/SuperWASP (TAMMAG2)': ['#7b2cbf', 'circle', 3],
    'GSA(G)': ['black', 'hexagon', 8],
    'RAPAS(G)': ['black', 'diamond-open', 6],
    'RAPAS(GBP)': ['#315efb', 'diamond-open', 6],
    'RAPAS(GRP)': ['#d62728', 'diamond-open', 6],
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
    'PS1(y)': ['#DAA520', "star-open", 5],
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
    'MOA(Red)': ['#ff5a8a', 'diamond-wide', 4],
    'MOA(Blue)': ['#3b82f6', 'diamond-wide', 4],
    'ATLAS(c)': ['#1f7e7d', 'circle', 4],
    'ATLAS(o)': ['#f88f1e', 'circle', 4],
    'TNS(GOTO-L)': ['#9467bd', 'circle-open', 5],
    'TNS(ASASSN-g)': ['green', 'circle-open', 5],
    'TNS(ASASSN-V)': ['darkgreen', 'circle-open', 5],
    'TNS(ATLAS-c)': ['#1f7e7d', 'circle-open', 5],
    'TNS(ATLAS-o)': ['#f88f1e', 'circle-open', 5],
    'TNS(ZTF-g)': ['#2ca02c', 'circle-open', 5],
    'TNS(ZTF-r)': ['#d62728', 'circle-open', 5],
    'TNS(ZTF-i)': ['#800000', 'circle-open', 5],
    'AAVSO(U)': ['#8000ff', 'circle-open', 5],
    'AAVSO(B)': ['blue', 'circle-open', 5],
    'AAVSO(V)': ['green', 'circle-open', 5],
    'AAVSO(R)': ['red', 'circle-open', 5],
    'AAVSO(I)': ['#800000', 'circle-open', 5],
    'AAVSO(Vis)': ["#49B6FF", 'circle-open', 5],
    'AAVSO(CV)': ['darkgreen', 'circle-open', 5],
    'AAVSO(CR)': ['#c04000', 'circle-open', 5],
    'AAVSO(TG)': ['#2ca02c', 'circle-open', 5],
    'AAVSO(TB)': ['#1f77b4', 'circle-open', 5],
    'AAVSO(TR)': ['#d62728', 'circle-open', 5],
    'KMTNET(I)': ['#8c4646', 'diamond-tall', 2],
    '2MASS(J)': ['#1f77b4', 'circle', 2],
    '2MASS(H)': ['#ff7f0e', 'circle', 2],
    '2MASS(K)': ['#2ca02c', 'circle', 2],
    '(J)2MASS': ['#1f77b4', 'circle', 2],
    '(H)2MASS': ['#ff7f0e', 'circle', 2],
    '(K)2MASS': ['#2ca02c', 'circle', 2],
    'PGIR(J)': ['#0b7285', 'diamond', 4],
    'NSC(u)': ['#6a3d9a', 'pentagon-open', 5],
    'NSC(g)': ['green', 'pentagon-open', 5],
    'NSC(r)': ['red', 'pentagon-open', 5],
    'NSC(i)': ['#800000', 'pentagon-open', 5],
    'NSC(z)': ['#ff0074', 'pentagon-open', 5],
    'NSC(Y)': ['#DAA520', 'pentagon-open', 5],
    'NSC(VR)': ['#17becf', 'pentagon-open', 5],
    'BGDS(r)': ['#d62728', 'star-open', 5],
    'BGDS(i)': ['#8c1c13', 'star-open', 5],
    'BGDS(z)': ['#ff0074', 'star-open', 5],
    'BGDS(U)': ['#6a3d9a', 'star-open', 5],
    'BGDS(B)': ['#1f77b4', 'star-open', 5],
    'BGDS(V)': ['#2ca02c', 'star-open', 5],
    'BGDS(Halpha)': ['#e377c2', 'star-triangle-up-open', 5],
    'BGDS(NB)': ['#7f7f7f', 'star-triangle-up-open', 5],
    'BGDS(OIII)': ['#17becf', 'star-triangle-up-open', 5],
    'BGDS(SII)': ['#bcbd22', 'star-triangle-up-open', 5],
    'OMC(V)': ['#006d2c', 'hexagram-open', 5],
    'DASCH(B)': ['#6b4f2a', 'square-open', 4],
    'SSS(Bj)': ['#1d4ed8', 'pentagon-open', 7],
    'SSS(R)': ['#b91c1c', 'pentagon-open', 7],
    'SSS(I)': ['#7f1d1d', 'pentagon', 7],
    'unTimely(W1)': ['#e6a800', 'hourglass', 6],
    'unTimely(W2)': ['#1f3a93', 'hourglass', 6],
    'VIRAC2(Z)': ['#17becf', 'triangle-left-open', 5],
    'VIRAC2(Y)': ['#bcbd22', 'triangle-left-open', 5],
    'VIRAC2(J)': ['#8c564b', 'triangle-left', 5],
    'VIRAC2(H)': ['#e377c2', 'triangle-left', 5],
    'VIRAC2(Ks)': ['#7f0000', 'triangle-left', 5],
    'VMC(Y)': ['#bcbd22', 'triangle-right-open', 5],
    'VMC(J)': ['#8c564b', 'triangle-right', 5],
    'VMC(Ks)': ['#7f0000', 'triangle-right', 5],
    'XMM-OM(UVW2)': ['#4b0082', 'triangle-down', 6],
    'XMM-OM(UVM2)': ['#7b2cbf', 'triangle-down', 6],
    'XMM-OM(UVW1)': ['#c77dff', 'triangle-down', 6],
    'XMM-OM(U)': ['#3a0ca3', 'triangle-down-open', 6],
    'XMM-OM(B)': ['#1f77b4', 'triangle-down-open', 6],
    'XMM-OM(V)': ['#2ca02c', 'triangle-down-open', 6],
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
    'HST(ACS_F814W)': ['#b5a300', 'hexagram', 5],
    'HST(ACS_F606W)': ['#5c0011', 'hexagram', 5],
    'JVAR(J0395)': ['#6a00ff', 'pentagon', 4],
    'JVAR(GSDSS)': ['#0088ff', 'pentagon', 4],
    'JVAR(J0515)': ['#00c853', 'pentagon', 4],
    'JVAR(RSDSS)': ['#ffb300', 'pentagon', 4],
    'JVAR(J0660)': ['#ff6d00', 'pentagon', 4],
    'JVAR(ISDSS)': ['#c62828', 'pentagon', 4],
    'JVAR(J0861)': ['#7b1fa2', 'pentagon', 4],
    'FRAM(U)': ['#5ac6bc', 'hexagon', 4],
    'FRAM(B)': ['#1d4ed8', 'hexagon', 4],
    'FRAM(V)': ['darkgreen', 'hexagon', 4],
    'FRAM(R)': ['red', 'hexagon', 4],
    'FRAM(I)': ['#800000', 'hexagon', 4],
    'FRAM(z)': ['#ff0074', 'hexagon', 4],
    'FRAM(D)': ['#6b7280', 'hexagon', 4],
    'FRAM(DF)': ['#374151', 'hexagon', 4],
    'FRAM(N)': ['#111827', 'hexagon', 4],
    'FRAM(UNK)': ['#9ca3af', 'hexagon', 4],
    'FRAM(unknown)': ['#9ca3af', 'hexagon', 4],
    # TESS: 600-1000 nm, so a deep red. A sector is ~15000 unbinned cadences, so
    # the marker is deliberately tiny -- anything larger renders as a solid bar.
    'TESS(T)': ['#a4133c', 'circle', 1],
    # Hipparcos/Tycho: one mean point per band at J1991.25, so these are large and
    # share the bowtie shape as a family. Hp is coloured violet rather than green
    # on purpose: it is a broad unfiltered band, not V, and must not read as VT.
    'Hp': ['#7048e8', 'bowtie', 9],
    'BT': ['#3b5bdb', 'bowtie', 9],
    'VT': ['#2f9e44', 'bowtie', 9],
}


def _photometry_trace_visibility(filter_name):
    return True

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
    'PS1(i)': ['#800000', 'arrow-down-open', 5],
    'PS1(z)': ['#ff0074', 'arrow-down-open', 5],
    'PS1(g)': ['green', 'arrow-down-open', 5],
    'PS1(y)': ['#DAA520', 'arrow-down-open', 5],
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
    'MOA(Red)': ['#ff5a8a', 'arrow-down-open', 4],
    'MOA(Blue)': ['#3b82f6', 'arrow-down-open', 4],
    'ATLAS(c)': ['#1f7e7d', 'arrow-down-open', 4],
    'ATLAS(o)': ['#f88f1e', 'arrow-down-open', 4],
    'TNS(GOTO-L)': ['#9467bd', 'arrow-down-open', 5],
    'TNS(ASASSN-g)': ['green', 'arrow-down-open', 5],
    'TNS(ASASSN-V)': ['darkgreen', 'arrow-down-open', 5],
    'TNS(ATLAS-c)': ['#1f7e7d', 'arrow-down-open', 5],
    'TNS(ATLAS-o)': ['#f88f1e', 'arrow-down-open', 5],
    'TNS(ZTF-g)': ['#2ca02c', 'arrow-down-open', 5],
    'TNS(ZTF-r)': ['#d62728', 'arrow-down-open', 5],
    'TNS(ZTF-i)': ['#800000', 'arrow-down-open', 5],
    'AAVSO(U)': ['#8000ff', 'arrow-down-open', 5],
    'AAVSO(B)': ['blue', 'arrow-down-open', 5],
    'AAVSO(V)': ['green', 'arrow-down-open', 5],
    'AAVSO(R)': ['red', 'arrow-down-open', 5],
    'AAVSO(I)': ['#800000', 'arrow-down-open', 5],
    'AAVSO(Vis)': ['#888888', 'arrow-down-open', 5],
    'AAVSO(CV)': ['darkgreen', 'arrow-down-open', 5],
    'AAVSO(CR)': ['#c04000', 'arrow-down-open', 5],
    'AAVSO(TG)': ['#2ca02c', 'arrow-down-open', 5],
    'AAVSO(TB)': ['#1f77b4', 'arrow-down-open', 5],
    'AAVSO(TR)': ['#d62728', 'arrow-down-open', 5],
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
    'HST(ACS_F814W)': ['#b5a300', 'arrow-down-open', 5],
    'HST(ACS_F606W)': ['#5c0011', 'arrow-down-open', 5],
    'FRAM(U)': ['#5ac6bc', 'arrow-down-open', 4],
    'FRAM(B)': ['#1d4ed8', 'arrow-down-open', 4],
    'FRAM(V)': ['darkgreen', 'arrow-down-open', 4],
    'FRAM(R)': ['red', 'arrow-down-open', 4],
    'FRAM(I)': ['#800000', 'arrow-down-open', 4],
    'FRAM(z)': ['#ff0074', 'arrow-down-open', 4],
    'FRAM(D)': ['#6b7280', 'arrow-down-open', 4],
    'FRAM(DF)': ['#374151', 'arrow-down-open', 4],
    'FRAM(N)': ['#111827', 'arrow-down-open', 4],
    'FRAM(UNK)': ['#9ca3af', 'arrow-down-open', 4],
    'FRAM(unknown)': ['#9ca3af', 'arrow-down-open', 4],
}

ALERCE_SPECIAL_COLOR_MAP = {
    'ZTF(zg)': ['green', 'cross', 6],
    'ZTF(zi)': ['#800000', 'cross', 6],
    'ZTF(zr)': ['red', 'cross', 6],
    'ZTF(g)': ['green', 'cross', 6],
    'ZTF(i)': ['#800000', 'cross', 6],
    'ZTF(r)': ['red', 'cross', 6],
}


# Radio flux densities (data_type 'radio') share the photometry plot on a right-hand log axis in mJy.
RADIO_FLUX_UNIT_TO_MJY = {'mJy': 1.0, 'Jy': 1000.0, 'uJy': 1e-3}

# ALMA bands, from Band 1 (~40 GHz, blue) to Band 10 (~870 GHz, purple).
RADIO_COLOR_MAP = {
    'ALMA(B1)': ['#1e3a8a', 'star', 6],
    'ALMA(B2)': ['#2563eb', 'star', 6],
    'ALMA(B3)': ['#0891b2', 'star', 6],
    'ALMA(B4)': ['#059669', 'star', 6],
    'ALMA(B5)': ['#65a30d', 'star', 6],
    'ALMA(B6)': ['#ca8a04', 'star', 6],
    'ALMA(B7)': ['#ea580c', 'star', 6],
    'ALMA(B8)': ['#dc2626', 'star', 6],
    'ALMA(B9)': ['#be185d', 'star', 6],
    'ALMA(B10)': ['#7e22ce', 'star', 6],
}


def _power_of_ten_ticks(values):
    """(tickvals, ticktext) for a log axis labelled as 10^n, adding 2x10^n and 5x10^n when the data
    span less than ~1.5 decades so a narrow range still gets labels."""
    positive = [v for v in values if v is not None and np.isfinite(v) and v > 0]
    if not positive:
        return None, None
    low, high = np.log10(min(positive)), np.log10(max(positive))
    mantissas = (1, 2, 5) if high - low < 1.5 else (1,)
    tickvals, ticktext = [], []
    for exponent in range(int(np.floor(low)) - 1, int(np.ceil(high)) + 2):
        for mantissa in mantissas:
            tickvals.append(mantissa * 10.0 ** exponent)
            ticktext.append(f'10<sup>{exponent}</sup>' if mantissa == 1 else f'{mantissa}×10<sup>{exponent}</sup>')
    return tickvals, ticktext


def _radio_traces(datums):
    """Radio flux-density traces on the photometry plot's right-hand log axis (y3), in mJy.
    Upper limits use error <= 0, as in photometry."""
    detections = {}
    limits = {}
    for datum in datums:
        value = datum.value if isinstance(datum.value, dict) else {}
        filter_name = str(value.get('filter') or '').strip()
        scale = RADIO_FLUX_UNIT_TO_MJY.get(str(value.get('flux_unit') or 'mJy'))
        try:
            flux = float(value.get('flux'))
            error = float(value['error']) if value.get('error') is not None else None
        except (TypeError, ValueError):
            continue
        if not filter_name or scale is None or not np.isfinite(flux) or flux <= 0:
            continue
        try:
            frequency = float(value.get('frequency_ghz'))
        except (TypeError, ValueError):
            frequency = None
        facility = value.get('facility') or datum.source_name or ''
        is_limit = error is not None and error <= 0
        bucket = (limits if is_limit else detections).setdefault(
            filter_name, {'time': [], 'flux': [], 'error': [], 'customdata': []})
        bucket['time'].append(datum.timestamp)
        bucket['flux'].append(flux * scale)
        bucket['error'].append(error * scale if error is not None and error > 0 else 0.0)
        bucket['customdata'].append((f'{facility}, {frequency:g} GHz' if frequency else facility, ''))

    traces = []
    for filter_name, values in detections.items():
        color, symbol, size = RADIO_COLOR_MAP.get(filter_name, ['gray', 'star', 6])
        traces.append(go.Scatter(
            x=values['time'],
            y=values['flux'],
            yaxis='y3',
            mode='markers',
            opacity=0.75,
            marker=dict(color=color, symbol=symbol, size=1.2 * size),
            name=filter_name,
            error_y=dict(type='data', array=values['error'], visible=True, thickness=0.5, width=0),
            text=Time(values['time'], format='datetime').mjd,
            customdata=values['customdata'],
            hovertemplate='%{x|%Y/%m/%d %H:%M:%S.%L}<br>MJD= %{text:.6f}'
                          '<br>flux density= %{y:.4g}&#177;%{error_y.array:.2g} mJy'
                          '<br>%{customdata[0]}',
        ))
    for filter_name, values in limits.items():
        color, _symbol, size = RADIO_COLOR_MAP.get(filter_name, ['gray', 'star', 6])
        traces.append(go.Scatter(
            x=values['time'],
            y=values['flux'],
            yaxis='y3',
            mode='markers',
            visible='legendonly',
            opacity=0.5,
            marker=dict(color=color, symbol='arrow-down-open', size=1.2 * size),
            name=f'{filter_name}-LIMIT',
            text=Time(values['time'], format='datetime').mjd,
            customdata=values['customdata'],
            hovertemplate='%{x|%Y/%m/%d %H:%M:%S.%L}<br>MJD = %{text:.6f}'
                          '<br>limit flux density = %{y:.4g} mJy'
                          '<br>%{customdata[0]}',
        ))
    return traces


NEGATIVE_DIFFERENCE_SUFFIX = ' (neg. diff)'


def _photometry_trace_style(color_map, filter_name):
    """[color, symbol, size] for a trace; negative-difference traces reuse their filter's colour
    with an open marker so they stay visually tied to, but distinct from, the positive ones."""
    base_name = filter_name
    is_negative = filter_name.endswith(NEGATIVE_DIFFERENCE_SUFFIX)
    if is_negative:
        base_name = filter_name[:-len(NEGATIVE_DIFFERENCE_SUFFIX)]
    color, symbol, size = color_map.get(base_name, ['gray', 'circle', 4])
    if is_negative:
        symbol = symbol if symbol.endswith('-open') else f'{symbol}-open'
    return [color, symbol, size]


def _negative_difference_hover(filter_name):
    if filter_name.endswith(NEGATIVE_DIFFERENCE_SUFFIX):
        return '<br>negative difference: fainter than the reference image'
    return ''


def _negative_difference_limit(diff_magnitude, diff_error, sigma=3.0):
    """3-sigma upper limit for a negative difference flux (fainter than the reference image).

    The stored magnitude is that of |difference flux|, which says nothing about how bright the
    source could be; the limit comes from the flux error instead: sigma_flux = |f| * err / 1.0857.
    """
    try:
        diff_magnitude = float(diff_magnitude)
        diff_error = float(diff_error)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(diff_magnitude) or not np.isfinite(diff_error) or diff_error <= 0:
        return None
    return diff_magnitude - 2.5 * np.log10(sigma * diff_error / 1.0857)


def _spectrum_source_label(datum):
    """Return the source label used by both photometry and spectroscopy plots."""
    value = datum.value if isinstance(datum.value, dict) else {}
    return str(value.get('filter') or datum.source_name or 'Spectrum').strip()


def _spectrum_time_traces(datums):
    """Build fixed-height timeline markers, grouped into one legend item per source."""
    timestamps_by_source = {}
    for datum in datums:
        if datum.timestamp is None:
            continue
        source = _spectrum_source_label(datum)
        timestamps_by_source.setdefault(source, []).append(datum.timestamp)

    traces = []
    for source, timestamps in timestamps_by_source.items():
        x_values = []
        y_values = []
        for timestamp in timestamps:
            # None separates the individual short line segments in a single trace.
            x_values.extend((timestamp, timestamp, None))
            y_values.extend((0.85, 0.99, None))

        traces.append(go.Scatter(
            x=x_values,
            y=y_values,
            yaxis='y2',
            mode='lines',
            connectgaps=False,
            line=dict(width=1, dash='dot'),
            name=f'{source} (Spec)',
            hovertemplate='Spectrum: %{fullData.name}<br>'
                          '%{x|%Y/%m/%d %H:%M:%S.%L}<extra></extra>',
        ))
    return traces


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

    request = context.get('request')
    diff_mode = bool(request is not None and getattr(request, 'GET', {}).get('phot') == 'diff')
    has_difference = False

    magnitude_min = -100.0
    magnitude_max = 100.0
    skip_filters = {
        'G(GAIA_ALERTS)', 'SDSSDR(u)', 'SDSSDR(g)', 'SDSSDR(r)', 'SDSSDR(i)', 'SDSS(z)',
        'SDSS_DR14(u)', 'SDSS_DR14(g)', 'SDSS_DR14(r)', 'SDSS_DR14(i)', 'SDSS_DR14(z)',
    }

    for datum in datums:
        filter_name = str(datum.value.get('filter', '')).strip()
        # Old imports may contain a separate MAG2 trace. New imports retain MAG2 only
        # as provenance on TAMMAG2 rows, and legacy raw traces should no longer plot.
        if datum.source_name == 'SuperWASP' and (
            datum.value.get('wasp_series') == 'MAG2'
            or filter_name in {'WASP/SuperWASP (MAG2)', 'WASP/SuperWASP (MAG2 raw)'}
        ):
            continue
        if datum.source_name == 'TNS' and filter_name and not filter_name.startswith('TNS('):
            survey = str(datum.value.get('survey') or '').upper()
            if survey in {'GOTO', 'ASASSN', 'ATLAS', 'ZTF'}:
                inner_filter = filter_name
            else:
                raw_filter = str(datum.value.get('tns_filter') or filter_name).strip()
                compact_filter = raw_filter.replace('-', '').replace('_', '').replace(' ', '').lower()
                if compact_filter == 'rcousins':
                    inner_filter = 'RCousins'
                elif compact_filter == 'icousins':
                    inner_filter = 'ICousins'
                else:
                    inner_filter = raw_filter
            filter_name = f'TNS({inner_filter})'
        if not filter_name or filter_name in skip_filters:
            continue

        if datum.value.get('diff_magnitude') is not None:
            has_difference = True

        negative_difference = False
        if diff_mode:
            value = datum.value.get('diff_magnitude')
            error = datum.value.get('diff_error')
            try:
                negative_difference = float(datum.value.get('diff_sign', 1)) < 0
            except (TypeError, ValueError):
                negative_difference = False
            if negative_difference:
                # A significant negative difference is a detection (source fainter than the
                # reference image), plotted at the magnitude of |difference flux| as ALeRCE does,
                # in its own trace so it is not read as a brightening.
                filter_name = f'{filter_name}{NEGATIVE_DIFFERENCE_SUFFIX}'
        else:
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
        if datum.source_name == 'RAPAS':
            link = reverse('rapas-measurement-detail', args=(datum.id,))
        elif datum.source_name == 'AAVSO':
            link = reverse('aavso-measurement-detail', args=(datum.id,))
        else:
            link = f"/dataproducts/data/{datum.data_product_id}/" if datum.data_product_id else ''
        if datum.source_name == 'TNS':
            custom = 'TNS'
        elif datum.source_name == 'AAVSO' and observer:
            custom = f'AAVSO<br>Observer: {observer}'
        else:
            custom = f"{facility}, {observer}".strip(', ')

        if diff_mode:
            is_limit = error is not None and error <= 0
            target_bucket = limits_data if is_limit else photometry_data
        else:
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
        trace_opacity = 0.75 if filter_name.startswith('MOA(') else 0.75

        # plotting ALERCE data with different markers
        custom_vals = np.array(filter_values['customdata'])
        alerce_mask = np.array([
            str(v).startswith('Alerce')
            for v in custom_vals ])
        normal_mask = ~alerce_mask

        plot_data.append(
            go.Scatter(
                x=np.array(filter_values['time'])[normal_mask],
                y=np.array(filter_values['magnitude'])[normal_mask],
                mode='markers',
                opacity=trace_opacity,
                marker=dict(
                        color=_photometry_trace_style(PHOTOMETRY_COLOR_MAP, filter_name)[0],
                        symbol=_photometry_trace_style(PHOTOMETRY_COLOR_MAP, filter_name)[1],
                        size=1.2 * _photometry_trace_style(PHOTOMETRY_COLOR_MAP, filter_name)[2],
                    ),
                name=filter_name,
                visible=_photometry_trace_visibility(filter_name),
                error_y=dict(
                    type='data',
                    array=np.array(filter_values['error'])[normal_mask],
                    visible=True,
                    thickness=0.5,
                    width=0
                ),
                text=np.array(mjds_to_plot[filter_name])[normal_mask],
                customdata=np.array(
                    list(zip(filter_values['customdata'],
                            filter_values['link']))
                )[normal_mask],
                hovertemplate='%{x|%Y/%m/%d %H:%M:%S.%L}<br>'
                            'MJD= %{text:.6f}'
                            '<br>mag= %{y:.3f}&#177;%{error_y.array:.3f}'
                            + _negative_difference_hover(filter_name) +
                            '<br>%{customdata[0]}',
            )   
        )

        plot_data.append(
            go.Scatter(
                x=np.array(filter_values['time'])[alerce_mask],
                y=np.array(filter_values['magnitude'])[alerce_mask],
                mode='markers',
                opacity=trace_opacity,
                marker=dict(
                        color=_photometry_trace_style(ALERCE_SPECIAL_COLOR_MAP, filter_name)[0],
                        symbol=_photometry_trace_style(ALERCE_SPECIAL_COLOR_MAP, filter_name)[1],
                        size=1.2 * _photometry_trace_style(ALERCE_SPECIAL_COLOR_MAP, filter_name)[2],
                    ),
                name=filter_name,
                visible=_photometry_trace_visibility(filter_name),
                error_y=dict(
                    type='data',
                    array=np.array(filter_values['error'])[alerce_mask],
                    visible=True,
                    thickness=0.5,
                    width=0
                ),
                text=np.array(mjds_to_plot[filter_name])[alerce_mask],
                customdata=np.array(
                    list(zip(filter_values['customdata'],
                            filter_values['link']))
                )[alerce_mask],
                hovertemplate='%{x|%Y/%m/%d %H:%M:%S.%L}<br>'
                            'MJD= %{text:.6f}'
                            '<br>mag= %{y:.3f}&#177;%{error_y.array:.3f}'
                            + _negative_difference_hover(filter_name) +
                            '<br>%{customdata[0]}',
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
                              '<br>%{customdata[0]}',
            )
        )

    try:
        radio_data_type = settings.DATA_PRODUCT_TYPES['radio'][0]
    except (AttributeError, KeyError):
        radio_data_type = 'radio'
    radio_datums = ReducedDatum.objects.filter(target=target, data_type=radio_data_type)
    if not settings.TARGET_PERMISSIONS_ONLY:
        radio_datums = get_objects_for_user(
            context['request'].user,
            'tom_dataproducts.view_reduceddatum',
            klass=radio_datums,
        )
    # Radio flux densities belong with apparent photometry, not with difference-image photometry.
    radio_traces = [] if diff_mode else _radio_traces(radio_datums)
    plot_data.extend(radio_traces)
    has_radio = bool(radio_traces)
    has_magnitudes = bool(photometry_data or limits_data)

    # Legend in name order: detections first, then upper limits; spectra are appended last.
    plot_data.sort(key=lambda trace: (
        str(trace.name or '').endswith('-LIMIT'),
        str(trace.name or '').casefold(),
    ))

    try:
        spectroscopy_data_type = settings.DATA_PRODUCT_TYPES['spectroscopy'][0]
    except (AttributeError, KeyError):
        spectroscopy_data_type = 'spectroscopy'

    spectroscopy_datums = ReducedDatum.objects.filter(
        target=target,
        data_type=spectroscopy_data_type,
    ).order_by('timestamp')
    if not settings.TARGET_PERMISSIONS_ONLY:
        spectroscopy_datums = get_objects_for_user(
            context['request'].user,
            'tom_dataproducts.view_reduceddatum',
            klass=spectroscopy_datums,
        )
    plot_data.extend(_spectrum_time_traces(spectroscopy_datums))
    # A wrapped horizontal legend otherwise gives every entry the width of the longest name, leaving
    # few columns and empty space; one legend group per trace lets each entry keep its own width.
    for index, trace in enumerate(plot_data):
        trace.legendgroup = str(index)

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
        margin=dict(t=40, r=100 if has_radio else 20, b=40, l=80),
        xaxis=dict(
            autorange=True,
            title='date',
            showgrid=grid,
            color=label_color,
            showline=True,
            linecolor=label_color,
            mirror=not has_radio,
        ),
        yaxis=dict(
            autorange=False,
            range=[np.ceil(magnitude_min), np.floor(magnitude_max)],
            title='difference magnitude' if diff_mode else 'magnitude',
            showgrid=grid,
            color=label_color,
            showline=True,
            linecolor=label_color,
            mirror=not has_radio,
            zeroline=False,
            # A radio-only target has no magnitudes; leave just the flux-density axis.
            visible=has_magnitudes or not has_radio,
        ),
        yaxis2=dict(
            overlaying='y',
            side='right',
            range=[0, 1],
            fixedrange=True,
            visible=False,
        ),
        legend=dict(
            yanchor='top',
            y=-0.15,
            xanchor='left',
            x=0.0,
            orientation='h',
            font=dict(color=label_color),
            traceorder='grouped',
            groupclick='toggleitem',
            tracegroupgap=0,
        ),
        clickmode='event',
    )
    if has_radio:
        radio_values = [
            value + sign * error
            for trace in radio_traces
            for value, error in zip(trace.y, (trace.error_y.array if trace.error_y.array is not None else [0.0] * len(trace.y)))
            for sign in (-1, 1)
        ]
        tickvals, ticktext = _power_of_ten_ticks(radio_values)
        fig.update_layout(
            yaxis3=dict(
                type='log', autorange=True, overlaying='y', side='right',
                title='flux density (mJy)', showgrid=False, color=label_color,
                showline=True, linecolor=label_color, zeroline=False,
                tickmode='array' if tickvals else 'auto', tickvals=tickvals, ticktext=ticktext,
            ),
        )

    he_result = _build_highenergy_plot(context, target, width=width, height=height,
                                       background=background, label_color=label_color, grid=grid)
    return {
        'target': target,
        'plot': offline.plot(fig, output_type='div', show_link=False),
        'highenergy_plot': he_result,
        'request': request,
        'phot_mode': 'diff' if diff_mode else 'apparent',
        'has_difference': has_difference,
    }

HIGHENERGY_COLOR_MAP = {
    'LAT(>100MeV)': ['#e63946', 'circle', 3],
    'LAT(>800MeV)': ['#457b9d', 'diamond', 3],
    'XRT(0.3-10keV)': ['#ff7f0e', 'square', 4],
    'EPIC(0.2-12keV)': ['#2ca02c', 'diamond', 5],
    'ASM(1.5-12keV)': ['#8c564b', 'triangle-up', 4],
    'ACIS(0.5-7keV)': ['#d62728', 'star', 6],
    'HRC(0.1-10keV)': ['#e377c2', 'star-open', 6],
    'LAT-LCR(3-day)': ['#9467bd', 'circle-open', 4],
    'LAT-LCR(weekly)': ['#9467bd', 'circle', 4],
    'LAT-LCR(monthly)': ['#5b2a86', 'circle', 5],
}

HIGHENERGY_LIMITS_COLOR_MAP = {
    'LAT(>100MeV)': ['#e63946', 'arrow-down-open', 3],
    'LAT(>800MeV)': ['#457b9d', 'arrow-down-open', 3],
    'XRT(0.3-10keV)': ['#ff7f0e', 'arrow-down-open', 4],
    'ASM(1.5-12keV)': ['#8c564b', 'arrow-down-open', 4],
    'LAT-LCR(3-day)': ['#9467bd', 'arrow-down-open', 4],
    'LAT-LCR(weekly)': ['#9467bd', 'arrow-down-open', 4],
    'LAT-LCR(monthly)': ['#5b2a86', 'arrow-down-open', 5],
}

HIGHENERGY_FILTER_ORDER = [
    'LAT(>800MeV)', 'LAT(>100MeV)',
    'LAT-LCR(3-day)', 'LAT-LCR(weekly)', 'LAT-LCR(monthly)',
    'XRT(0.3-10keV)', 'EPIC(0.2-12keV)', 'ACIS(0.5-7keV)', 'HRC(0.1-10keV)', 'ASM(1.5-12keV)',
]


def _build_highenergy_plot(context, target, width=1000, height=600, background=None, label_color=None, grid=True):
    try:
        highenergy_data_type = settings.DATA_PRODUCT_TYPES['highenergy'][0]
    except (AttributeError, KeyError):
        highenergy_data_type = 'highenergy'

    detection_data = {}
    limits_data = {}
    # Filters with physical fluxes (value has 'flux_unit', e.g. Swift-XRT in erg/cm2/s) go on the
    # right-hand log axis; the rest (FAVA relative flux) stay on the left axis.
    flux_axis_filters = set()
    if settings.TARGET_PERMISSIONS_ONLY:
        datums = ReducedDatum.objects.filter(target=target, data_type=highenergy_data_type)
    else:
        datums = get_objects_for_user(
            context['request'].user,
            'tom_dataproducts.view_reduceddatum',
            klass=ReducedDatum.objects.filter(target=target, data_type=highenergy_data_type),
        )

    if not datums.exists():
        return None

    for datum in datums:
        filter_name = str(datum.value.get('filter', '')).strip()
        if not filter_name:
            continue

        value = datum.value.get('flux')
        error = datum.value.get('error')
        try:
            value = float(value) if value is not None else None
            error = float(error) if error is not None else None
        except (TypeError, ValueError):
            continue
        if value is None:
            continue

        facility = datum.value.get('facility') or datum.source_name or ''
        observer = datum.value.get('observer') or ''
        custom = f"{facility}, {observer}".strip(', ')

        # Upper limits: FAVA stores flux == -1; others use a non-positive error (-1, as in photometry).
        is_limit = (value == -1) or (error is not None and error <= 0)
        target_bucket = limits_data if is_limit else detection_data

        if datum.value.get('flux_unit'):
            flux_axis_filters.add(filter_name)
        else:
            # Relative fluxes are O(1); physical fluxes (~1e-11) must not be rounded to zero.
            value = np.around(value, 6)
            error = np.around(error, 6) if error is not None else None

        target_bucket.setdefault(filter_name, {})
        target_bucket[filter_name].setdefault('time', []).append(datum.timestamp)
        target_bucket[filter_name].setdefault('flux', []).append(value)
        target_bucket[filter_name].setdefault('error', []).append(error if error is not None else 0.0)
        target_bucket[filter_name].setdefault('customdata', []).append(custom)

    plot_data = []

    def _sorted_filters(data_dict):
        return sorted(data_dict.keys(), key=lambda f: HIGHENERGY_FILTER_ORDER.index(f) if f in HIGHENERGY_FILTER_ORDER else 999)

    mjds_to_plot = {}
    for filter_name, fv in detection_data.items():
        if fv.get('flux'):
            mjds_to_plot[filter_name] = Time(fv['time'], format='datetime').mjd

    for filter_name in _sorted_filters(detection_data):
        filter_values = detection_data[filter_name]
        if not filter_values.get('flux'):
            continue
        style = HIGHENERGY_COLOR_MAP.get(filter_name, ['gray', 'circle', 3])
        on_flux_axis = filter_name in flux_axis_filters
        plot_data.append(
            go.Scatter(
                x=filter_values['time'],
                y=filter_values['flux'],
                yaxis='y2' if on_flux_axis else 'y',
                mode='markers',
                opacity=0.75,
                marker=dict(color=style[0], symbol=style[1], size=1.2 * style[2]),
                name=filter_name,
                error_y=dict(type='data', array=filter_values['error'], visible=True, thickness=0.5, width=0),
                text=mjds_to_plot[filter_name],
                customdata=list(zip(filter_values['customdata'])),
                hovertemplate=(
                    '%{x|%Y/%m/%d %H:%M:%S.%L}<br>MJD= %{text:.6f}'
                    + ('<br>flux= %{y:.3e}&#177;%{error_y.array:.2e} erg/cm²/s' if on_flux_axis
                       else '<br>rel. flux= %{y:.6f}&#177;%{error_y.array:.6f}')
                    + '<br>%{customdata[0]}'
                ),
            )
        )

    limit_mjds = {}
    for filter_name, fv in limits_data.items():
        if fv.get('flux'):
            limit_mjds[filter_name] = Time(fv['time'], format='datetime').mjd

    for filter_name in _sorted_filters(limits_data):
        filter_values = limits_data[filter_name]
        if not filter_values.get('flux'):
            continue
        style = HIGHENERGY_LIMITS_COLOR_MAP.get(filter_name, ['gray', 'arrow-down-open', 3])
        on_flux_axis = filter_name in flux_axis_filters
        plot_data.append(
            go.Scatter(
                x=filter_values['time'],
                y=filter_values['flux'],
                yaxis='y2' if on_flux_axis else 'y',
                mode='markers',
                visible='legendonly',
                opacity=0.5,
                marker=dict(color=style[0], symbol=style[1], size=1.2 * style[2]),
                name=f'{filter_name}-LIMIT',
                text=limit_mjds[filter_name],
                customdata=list(zip(filter_values['customdata'])),
                hovertemplate=(
                    '%{x|%Y/%m/%d %H:%M:%S.%L}<br>MJD= %{text:.6f}'
                    + ('<br>limit flux= %{y:.3e} erg/cm²/s' if on_flux_axis
                       else '<br>limit rel. flux= %{y:.6f}')
                    + '<br>%{customdata[0]}'
                ),
            )
        )

    fig = go.Figure(
        data=plot_data,
        layout=go.Layout(height=height, width=width, paper_bgcolor=background, plot_bgcolor=background),
    )

    has_flux_axis = bool(flux_axis_filters)
    has_relative_axis = any(
        name not in flux_axis_filters for name in list(detection_data) + list(limits_data)
    )
    fig.update_layout(
        showlegend=True,
        margin=dict(t=40, r=100 if has_flux_axis else 20, b=40, l=80),
        xaxis=dict(autorange=True, title='date', showgrid=grid, color=label_color,
                   showline=True, linecolor=label_color, mirror=not has_flux_axis),
        yaxis=dict(autorange=True, title='relative flux', showgrid=grid, color=label_color,
                   showline=True, linecolor=label_color, mirror=not has_flux_axis, zeroline=True,
                   visible=has_relative_axis or not has_flux_axis),
        legend=dict(yanchor='top', y=-0.15, xanchor='left', x=0.0, orientation='h',
                    font=dict(color=label_color)),
        clickmode='event+select',
    )
    if has_flux_axis:
        fig.update_layout(
            yaxis2=dict(
                type='log', autorange=True, overlaying='y', side='right',
                title='flux (erg cm<sup>-2</sup> s<sup>-1</sup>)',
                # Narrow log ranges otherwise label only the decade and show bare digits for the rest.
                tickformat='.0e', dtick='D2', showgrid=False, color=label_color,
                showline=True, linecolor=label_color,
            ),
        )

    return offline.plot(fig, output_type='div', show_link=False)


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
    #flux_count_data = []
    #flux_other_data = []

    spectra_json = []

    for datum in datums.order_by('timestamp'):
        try:
            spectrum = serializer.deserialize(datum.value)
        except Exception:
            continue

        label = f"{_spectrum_source_label(datum)} {datum.timestamp.strftime('%Y-%m-%d %H:%M')}"

        # Separate by flux units
        if str(spectrum.flux.unit) == 'ct' or str(spectrum.flux.unit) == u.ct:
            #flux_count_data.append(
            #    go.Scatter(
            #        x=spectrum.wavelength.value,
            #        y=spectrum.flux.value,
            #        name=label + " (counts)",
            #        hovertemplate='lambda=%{x:.2f}<br>flux=%{y:.4e}<extra>%{fullData.name}</extra>',
            #        yaxis='y2'
            #    )
            #)
            wavelength = spectrum.wavelength.value.tolist()
            flux = spectrum.flux.value.tolist()
            unit_str = str(spectrum.flux.unit)
            spectra_json.append({
            "wavelength": wavelength,
            "flux": flux,
            "unit": unit_str,
            "label": label
            })
        else:
            #flux_other_data.append(
            #    go.Scatter(
            #        x=spectrum.wavelength.value,
            #        y=spectrum.flux.to(u.erg / (u.cm**2 * u.s * u.AA)).value,
            #        name=label,
            #        hovertemplate='lambda=%{x:.2f}<br>flux=%{y:.4e}<extra>%{fullData.name}</extra>',
            #    )
            #)
            wavelength = spectrum.wavelength.value.tolist()
            flux = spectrum.flux.to(u.erg / (u.cm**2 * u.s * u.AA)).value.tolist()
            unit_str = str(u.erg / (u.cm**2 * u.s * u.AA))
            spectra_json.append({
            "wavelength": wavelength,
            "flux": flux,
            "unit": unit_str,
            "label": label
            })

    # Use subplots with secondary y-axis
    #figure = make_subplots(specs=[[{"secondary_y": True}]])
    #for trace in flux_other_data:
    #    figure.add_trace(trace, secondary_y=False)
    #for trace in flux_count_data:
    #    figure.add_trace(trace, secondary_y=True)

    #figure.update_layout(
    #    height=600,
    #    width=1000,
    #    xaxis=dict(
    #        title="Wavelength (Å)",
    #        showgrid=True,
    #        gridcolor="rgba(200,200,200,0.3)",
    #        zeroline=False,
    #        exponentformat="none",
    #        tickformat=".0f"
    #    ),
    #    yaxis=dict(
    #        title="Flux density",
    #        tickformat=".2e",
    #    ),
    #    yaxis2=dict(
    #        title="Flux (counts)",
    #        overlaying='y',
    #        side='right',
    #        showgrid=False,
    #    ),
    #    showlegend=True,
    #    margin=dict(t=40, r=80, b=40, l=80),
    #    legend=dict(
    #        yanchor='top',
    #        y=-0.15,
    #        xanchor='left',
    #        x=0.0,
    #        orientation='h',
    #    ),
    #)

    request = context.get('request')
    return {
        'target': target,
#        'plot': offline.plot(figure, output_type='div', show_link=False),
        'spectra_data': json.dumps(spectra_json),
        'request': request
    }


# Per-epoch positions currently come from ZTF (IRSA), Alerce and LSST (Fink); other services will follow as
# they learn to store origin_ra/origin_dec, and this tag picks them up with no change here.
ASTROMETRY_DEFAULT_STYLE = ['gray', 'circle', 6]


def _astrometry_series_label(source_name, filter_name):
    """'ZTF(zg)' for ZTF itself, 'Alerce ZTF(zg)' for a broker republishing the same filter."""
    source_name = str(source_name or '').strip()
    filter_name = str(filter_name or '').strip()
    if not source_name:
        return filter_name or 'unknown'
    if not filter_name:
        return source_name
    if filter_name.upper().startswith(f'{source_name.upper()}('):
        return filter_name
    return f'{source_name} {filter_name}'


def _astrometry_style(source_name, filter_name):
    if str(source_name or '').strip().lower() == 'alerce':
        return ALERCE_SPECIAL_COLOR_MAP.get(filter_name, ASTROMETRY_DEFAULT_STYLE)
    return PHOTOMETRY_COLOR_MAP.get(filter_name, ASTROMETRY_DEFAULT_STYLE)


def _astrometry_mag_label(value):
    """Hover text for a position: the apparent magnitude, else the difference magnitude.

    Upper limits (apparent error <= 0, or a negative difference flux) are shown as '<limit'.
    """
    def number(key):
        try:
            number = float(value.get(key))
        except (TypeError, ValueError):
            return None
        return number if np.isfinite(number) else None

    magnitude = number('magnitude')
    if magnitude is None:
        magnitude = number('limit')
    if magnitude is not None:
        error = number('error')
        is_limit = value.get('limit') is not None or (error is not None and error <= 0)
        return f'<{magnitude:.3f}' if is_limit else f'{magnitude:.3f}'

    diff_magnitude = number('diff_magnitude')
    if diff_magnitude is not None:
        sign = number('diff_sign')
        if sign is not None and sign < 0:
            limit = _negative_difference_limit(diff_magnitude, number('diff_error'))
            return f'<{limit:.3f} (diff)' if limit is not None else 'n/a'
        return f'{diff_magnitude:.3f} (diff)'
    return 'n/a'


@register.inclusion_tag('tom_dataproducts/partials/astrometry_for_target.html', takes_context=True)
def astrometry_for_target(context, target):
    """Per-epoch sky positions for a target, as JSON for the client-side astrometry plot.

    Reads origin_ra/origin_dec off photometry datums. Services that do not store them yet simply
    contribute nothing, so the tab grows as more services are updated.
    """
    try:
        photometry_data_type = settings.DATA_PRODUCT_TYPES['photometry'][0]
    except (AttributeError, KeyError):
        photometry_data_type = 'photometry'

    datums = ReducedDatum.objects.filter(target=target, data_type=photometry_data_type)
    if not settings.TARGET_PERMISSIONS_ONLY:
        datums = get_objects_for_user(
            context['request'].user,
            'tom_dataproducts.view_reduceddatum',
            klass=datums,
        )

    series = {}
    for datum in datums.order_by('timestamp'):
        value = datum.value if isinstance(datum.value, dict) else {}
        ra = value.get('origin_ra')
        dec = value.get('origin_dec')
        if not isinstance(ra, (int, float)) or not isinstance(dec, (int, float)):
            continue
        if isinstance(ra, bool) or isinstance(dec, bool):
            continue
        if datum.timestamp is None:
            continue

        filter_name = str(value.get('filter', '')).strip()
        label = _astrometry_series_label(datum.source_name, filter_name)
        if label not in series:
            color, symbol, size = _astrometry_style(datum.source_name, filter_name)
            series[label] = {
                'label': label,
                'source': str(datum.source_name or ''),
                'color': color,
                'symbol': symbol,
                'size': size,
                'ra': [],
                'dec': [],
                'mjd': [],
                'time': [],
                'magnitude': [],
                'mag_label': [],
            }

        magnitude = value.get('magnitude')
        try:
            magnitude = float(magnitude) if magnitude is not None else None
        except (TypeError, ValueError):
            magnitude = None

        entry = series[label]
        entry['ra'].append(float(ra))
        entry['dec'].append(float(dec))
        entry['mjd'].append(float(Time(datum.timestamp, format='datetime').mjd))
        entry['time'].append(datum.timestamp.isoformat())
        entry['magnitude'].append(magnitude)
        entry['mag_label'].append(_astrometry_mag_label(value))

    series_list = sorted(series.values(), key=lambda item: item['label'])
    point_count = sum(len(item['ra']) for item in series_list)

    astrometry_data = {
        'series': series_list,
        # The target's catalog position is the reference for the arcsec-offset view.
        'target_ra': float(target.ra) if target.ra is not None else None,
        'target_dec': float(target.dec) if target.dec is not None else None,
        'target_name': str(target.name or ''),
    }

    return {
        'target': target,
        'astrometry_data': json.dumps(astrometry_data),
        'point_count': point_count,
        'series_count': len(series_list),
        'request': context.get('request'),
    }


@register.inclusion_tag('tom_dataproducts/partials/recent_photometry.html')
def recent_photometry(target, limit=1):
    """Most recent photometric points for a target; overrides tom_dataproducts' tag of the same name.

    Upstream indexes value['magnitude'] and raises KeyError on difference-only rows (e.g. an Alerce
    detection with no corrected apparent magnitude), which breaks the whole target page. Rows with
    neither a magnitude nor a limit are skipped instead, still returning up to `limit` points.
    """
    data = []
    photometry = ReducedDatum.objects.filter(data_type='photometry', target=target).order_by('-timestamp')
    for reduced_datum in photometry.iterator():
        value = reduced_datum.value if isinstance(reduced_datum.value, dict) else {}
        if 'limit' in value:
            data.append({'timestamp': reduced_datum.timestamp, 'magnitude': value['limit'], 'limit': True})
        elif 'magnitude' in value:
            data.append({'timestamp': reduced_datum.timestamp, 'magnitude': value['magnitude'], 'limit': False})
        else:
            continue
        if len(data) >= limit:
            break
    return {'data': data}
