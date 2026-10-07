"""Survey radii for the target finding chart."""

from importlib import import_module
import math

# Only services with a meaningful, single angular footprint belong here. TNS
# combines measurements from several telescopes, so it has no one survey radius.
SERVICE_CLASSES = {
    'AAVSO': ('aavso', 'AAVSODataService'),
    'Alerce': ('alerce', 'AlerceDataService'),
    'AllWISE': ('allwise', 'AllWISEDataService'),
    'ALMA': ('alma', 'ALMADataService'),
    'ASASSN': ('asassn', 'ASASSNDataService'),
    'ATLAS': ('atlas', 'ATLASDataService'),
    'BGDS': ('bgds', 'BGDSDataService'),
    'CRTS': ('crts', 'CRTSDataService'),
    'CSC': ('csc', 'CSCDataService'),
    'DASCH': ('dasch', 'DASCHDataService'),
    'DECaPS': ('decaps', 'DECaPSDataService'),
    'FAVA': ('fava', 'FAVADataService'),
    'FermiLCR': ('lcr', 'LCRDataService'),
    'FRAM': ('fram', 'FRAMDataService'),
    'GaiaAlerts': ('gaia_alerts', 'GaiaAlertsDataService'),
    'GaiaDR3': ('gaia_dr3', 'GaiaDR3DataService'),
    'Galex': ('galex', 'GalexDataService'),
    'HETDEX': ('hetdex', 'HETDEXDataService'),
    'Hipparcos': ('hipparcos', 'HipparcosDataService'),
    'HSTSpectra': ('hst_spectra', 'HSTSpectraDataService'),
    'Hubble': ('hst', 'HSTDataService'),
    'JVAR': ('jvar', 'JVARDataService'),
    'JWSTSpectra': ('jwst_spectra', 'JWSTSpectraDataService'),
    'K2': ('k2', 'K2DataService'),
    'KELT': ('kelt', 'KELTDataService'),
    'Kepler': ('kepler', 'KeplerDataService'),
    'KMT': ('kmt', 'KMTDataService'),
    'LSST': ('lsst', 'LSSTDataService'),
    'LSXPS': ('lsxps', 'LSXPSDataService'),
    'MOA': ('moa', 'MOADataService'),
    'NeoWISE': ('neowise', 'NeoWISEDataService'),
    'NSC': ('nsc', 'NSCDataService'),
    'OGLEEWS': ('ogle_ews', 'OGLEEWSDataService'),
    'OGLEOCVS': ('ogle_ocvs', 'OGLEOCVSDataService'),
    'OMC': ('omc', 'OMCDataService'),
    'PS1': ('panstarrs', 'PanSTARRSDataService'),
    'PGIR': ('pgir', 'PGIRDataService'),
    'PTF': ('ptf', 'PTFDataService'),
    'RAPAS': ('rapas', 'RAPASDataService'),
    'RXTEASM': ('rxte_asm', 'RXTEASMDataService'),
    'SDSS': ('sdss', 'SDSSDataService'),
    'SkyMapper': ('skymapper', 'SkyMapperDataService'),
    'SuperCOSMOS': ('supercosmos', 'SuperCOSMOSDataService'),
    'SuperWASP': ('superwasp', 'SuperWASPDataService'),
    'SwiftUVOT': ('swiftuvot', 'SwiftUVOTDataService'),
    'TESS': ('tess', 'TESSDataService'),
    '2dFGRS': ('gs2df', 'Gs2dfDataService'),
    '2MASS': ('twomass', 'TwoMASSDataService'),
    'unTimely': ('untimely', 'UnTimelyDataService'),
    'VIRAC2': ('virac2', 'VIRAC2DataService'),
    'VMC': ('vmc', 'VMCDataService'),
    'WiggleZ': ('wigglez', 'WiggleZDataService'),
    'XMMEPIC': ('xmmepic', 'XMMEPICDataService'),
    'XMMOM': ('xmmom', 'XMMOMDataService'),
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
