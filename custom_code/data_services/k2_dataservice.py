"""K2 (2014-2018) long-cadence light curves from MAST, converted to Kepler magnitudes.

K2 re-used the Kepler spacecraft after its second reaction-wheel failure, observing ~20 fields
along the ecliptic for ~80 days each (Campaigns 0-19), including many galaxies, AGN, young
clusters and Solar System fields. The files (ktwo<EPIC>-cNN_llc.fits) have the Kepler format, so
the Kepler service's code is reused with K2 constants. The zero point was measured against EPIC
Kp the same way (PDCSAP 25.32, scatter 0.07; SAP 25.26). K2 SAP flux is dominated by the ~6 h
thruster-firing sawtooth, so PDCSAP is the default here too.
"""

from custom_code.data_services.forms import K2QueryForm
from custom_code.data_services.kepler_dataservice import KeplerMissionDataService


class K2DataService(KeplerMissionDataService):
    mission = 'K2'
    obs_collection = 'K2'
    target_prefix = 'ktwo'
    long_cadence_marker = '_lc'
    filter_name = 'K2(Kp)'
    alias_prefix = 'EPIC'
    zero_points = {'pdcsap': 25.32, 'sap': 25.26}
    segment_header = 'CAMPAIGN'
    default_radius_arcsec = 4.0

    name = 'K2'
    verbose_name = 'K2 (2014-2018)'
    info_url = 'https://archive.stsci.edu/missions-and-data/k2'
    acknowledgement = (
        'This paper includes data collected by the K2 mission and obtained from the MAST data '
        'archive at the Space Telescope Science Institute (STScI). Funding for the K2 mission is '
        'provided by the NASA Science Mission Directorate. STScI is operated by the Association of '
        'Universities for Research in Astronomy, Inc., under NASA contract NAS 5-26555.'
    )
    service_notes = (
        'Query public K2 long-cadence (29.4 min) light curves from MAST by coordinates. The nearest '
        'EPIC target within 4 arcsec is used; every good cadence of every campaign is converted to '
        'Kepler magnitude (PDCSAP by default, Kp = 25.32 - 2.5 log10 flux, calibrated against EPIC '
        'Kp to ~0.07 mag). Times are barycentric TDB.'
    )

    @classmethod
    def get_form_class(cls):
        return K2QueryForm
