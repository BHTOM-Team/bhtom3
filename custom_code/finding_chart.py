"""Survey radii for the target finding chart."""

from importlib import import_module
import math

from django.conf import settings
from guardian.shortcuts import get_objects_for_user
from tom_dataproducts.models import ReducedDatum


# Only services with a meaningful, single angular footprint belong here. TNS
# combines measurements from several telescopes, so it has no one survey radius.
SERVICE_CLASSES = {
    'AAVSO': ('aavso', 'AAVSODataService'),
    'Alerce': ('alerce', 'AlerceDataService'),
    'AllWISE': ('allwise', 'AllWISEDataService'),
    'ASASSN': ('asassn', 'ASASSNDataService'),
    'ATLAS': ('atlas', 'ATLASDataService'),
    'CRTS': ('crts', 'CRTSDataService'),
    'FAVA': ('fava', 'FAVADataService'),
    'FRAM': ('fram', 'FRAMDataService'),
    'GaiaAlerts': ('gaia_alerts', 'GaiaAlertsDataService'),
    'GaiaDR3': ('gaia_dr3', 'GaiaDR3DataService'),
    'Galex': ('galex', 'GalexDataService'),
    'Hipparcos': ('hipparcos', 'HipparcosDataService'),
    'Hubble': ('hst', 'HSTDataService'),
    'JVAR': ('jvar', 'JVARDataService'),
    'KMT': ('kmt', 'KMTDataService'),
    'LSST': ('lsst', 'LSSTDataService'),
    'MOA': ('moa', 'MOADataService'),
    'NeoWISE': ('neowise', 'NeoWISEDataService'),
    'NSC': ('nsc', 'NSCDataService'),
    'OGLEEWS': ('ogle_ews', 'OGLEEWSDataService'),
    'OGLEOCVS': ('ogle_ocvs', 'OGLEOCVSDataService'),
    'PS1': ('panstarrs', 'PanSTARRSDataService'),
    'PGIR': ('pgir', 'PGIRDataService'),
    'PTF': ('ptf', 'PTFDataService'),
    'RAPAS': ('rapas', 'RAPASDataService'),
    'SDSS': ('sdss', 'SDSSDataService'),
    'SkyMapper': ('skymapper', 'SkyMapperDataService'),
    'SuperWASP': ('superwasp', 'SuperWASPDataService'),
    'SwiftUVOT': ('swiftuvot', 'SwiftUVOTDataService'),
    'TESS': ('tess', 'TESSDataService'),
    '2MASS': ('twomass', 'TwoMASSDataService'),
    'ZTF': ('ztf', 'ZTFDataService'),
}

SKIPPED_FILTERS = {
    'G(GAIA_ALERTS)', 'SDSSDR(u)', 'SDSSDR(g)', 'SDSSDR(r)',
    'SDSSDR(i)', 'SDSS(z)', 'SDSS_DR14(u)', 'SDSS_DR14(g)',
    'SDSS_DR14(r)', 'SDSS_DR14(i)', 'SDSS_DR14(z)',
}


def _visible_datums(target, data_type, request):
    datums = ReducedDatum.objects.filter(target=target, data_type=data_type)
    if settings.TARGET_PERMISSIONS_ONLY:
        return datums
    if request is None:
        return datums.none()
    return get_objects_for_user(
        request.user, 'tom_dataproducts.view_reduceddatum', klass=datums,
    )


def _has_plotted_value(datum, difference_mode):
    value = datum.value if isinstance(datum.value, dict) else {}
    filter_name = str(value.get('filter') or '').strip()
    if not filter_name or filter_name in SKIPPED_FILTERS:
        return False
    if datum.source_name == 'SuperWASP' and (
        value.get('wasp_series') == 'MAG2'
        or filter_name in {'WASP/SuperWASP (MAG2)', 'WASP/SuperWASP (MAG2 raw)'}
    ):
        return False
    plotted = (value.get('diff_magnitude') if difference_mode else
               value.get('magnitude', value.get('limit')))
    if plotted is None and not difference_mode:
        plotted = value.get('limit')
    try:
        return plotted is not None and math.isfinite(float(plotted))
    except (TypeError, ValueError, OverflowError):
        return False


def survey_overlays(target, request):
    """Return radii only for services with points in the target's plotted data."""
    difference_mode = request.GET.get('phot') == 'diff' if request else False
    sources = set()
    photometry_type = settings.DATA_PRODUCT_TYPES.get('photometry', ('photometry',))[0]
    highenergy_type = settings.DATA_PRODUCT_TYPES.get('highenergy', ('highenergy',))[0]
    photometry = _visible_datums(target, photometry_type, request)
    for datum in photometry.filter(source_name__in=SERVICE_CLASSES).only('source_name', 'value').iterator():
        if datum.source_name not in sources and _has_plotted_value(datum, difference_mode):
            sources.add(datum.source_name)
    # FAVA is the high-energy panel that appears below the photometry plot.
    highenergy = _visible_datums(target, highenergy_type, request)
    for datum in highenergy.filter(source_name='FAVA').only('source_name', 'value').iterator():
        value = datum.value if isinstance(datum.value, dict) else {}
        try:
            if value.get('filter') and math.isfinite(float(value.get('flux'))):
                sources.add('FAVA')
                break
        except (TypeError, ValueError, OverflowError):
            pass

    overlays = []
    for index, source in enumerate(sorted(sources, key=str.casefold)):
        module_name, class_name = SERVICE_CLASSES[source]
        service = getattr(import_module(f'custom_code.data_services.{module_name}_dataservice'), class_name)
        radius = float(service.get_finding_chart_radius_arcsec())
        if not math.isfinite(radius) or radius <= 0:
            continue
        radius_label = f'{radius / 3600:g}°' if radius >= 3600 else f'{radius:g}″'
        detail = (' LAT 95% PSF' if source == 'FAVA' else
                  ' pixel' if source in {'ATLAS', 'TESS'} else ' search radius')
        overlays.append({
            'name': f'{source} ({radius_label}{detail})',
            'radius_arcsec': radius,
            'color': f'hsl({(index * 137.508 + 35) % 360:.0f}, 90%, 60%)',
        })
    return overlays
