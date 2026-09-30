"""Survey radii for the target finding chart."""

from importlib import import_module
import math

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

def survey_overlays(other_names, additional_sources=()):
    """Build circles from the sources already displayed under Other Names."""
    sources = {row.get('source_name') for row in other_names}
    sources.update(additional_sources)
    sources.intersection_update(SERVICE_CLASSES)
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
