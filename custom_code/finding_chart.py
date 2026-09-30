"""Survey radii for the target finding chart."""

from importlib import import_module
import math

from django.conf import settings
from django.db.models import Q
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


def survey_overlays(target, request):
    """Return radii only for services with points in the target's plotted data."""
    difference_mode = request.GET.get('phot') == 'diff' if request else False
    sources = set()
    photometry_type = settings.DATA_PRODUCT_TYPES.get('photometry', ('photometry',))[0]
    highenergy_type = settings.DATA_PRODUCT_TYPES.get('highenergy', ('highenergy',))[0]
    photometry = _visible_datums(target, photometry_type, request)
    if difference_mode:
        measurement = Q(value__diff_magnitude__isnull=False) & ~Q(value__diff_magnitude=None)
    else:
        measurement = (
            (Q(value__magnitude__isnull=False) & ~Q(value__magnitude=None))
            | (Q(value__limit__isnull=False) & ~Q(value__limit=None))
        )
    # Keep the light-curve rows in the database; the chart only needs service names.
    sources.update(
        photometry.filter(
            measurement,
            source_name__in=SERVICE_CLASSES,
            value__filter__isnull=False,
        )
        .filter(
            ~Q(source_name='SuperWASP')
            | Q(value__wasp_series__isnull=True)
            | ~Q(value__wasp_series='MAG2')
        )
        .exclude(value__filter=None)
        .exclude(value__filter='')
        .exclude(value__filter__in=SKIPPED_FILTERS)
        .exclude(value__filter__in=(
            'WASP/SuperWASP (MAG2)', 'WASP/SuperWASP (MAG2 raw)',
        ))
        .order_by()
        .values_list('source_name', flat=True)
        .distinct()
    )
    # FAVA is the high-energy panel that appears below the photometry plot.
    highenergy = _visible_datums(target, highenergy_type, request)
    if highenergy.filter(
        source_name='FAVA', value__filter__isnull=False, value__flux__isnull=False,
    ).exclude(value__filter=None).exclude(value__filter='').exclude(value__flux=None).exists():
        sources.add('FAVA')

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
