"""Swift-XRT X-ray light curves from the Living Swift-XRT Point Source catalogue (LSXPS).

LSXPS (Evans et al. 2023, MNRAS 518, 174) is built at the UK Swift Science Data Centre and
updated as Swift observes, so it is the place to get per-observation XRT light curves of
any catalogued source. Access is through the ``swifttools`` package: a cone search finds
the nearest LSXPS source, its details give the energy conversion factor, and its light
curve is fetched binned per observation in MJD.

Count rates (0.3-10 keV) are converted to observed (absorbed) flux in erg/cm^2/s with the
source's PowECFO, the conversion factor of its mean power-law spectrum; LSXPS's own
PowFlux is exactly rate x PowECFO. Spectral changes between observations are not followed.

Detections are stored as high-energy points on the flux axis of the high-energy plot.
Observations without a detection are stored as LSXPS's upper limits with error -1 (the same
upper-limit convention as photometry) and ``upper_limit: True``.
"""

import logging
import math
from datetime import timezone
from urllib.parse import quote

from astropy.time import Time

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target

from custom_code.data_services.forms import LSXPSQueryForm
from custom_code.data_services.service_utils import (
    resolve_query_coordinates,
    upsert_reduced_datums,
)

logger = logging.getLogger(__name__)

LSXPS_PAGE_URL = 'https://www.swift.ac.uk/LSXPS/'
LSXPS_FILTER = 'XRT(0.3-10keV)'
LSXPS_FLUX_UNIT = 'erg/cm2/s'
# XRT 90% position errors are a few arcsec; LSXPS sources are rarely closer than this.
LSXPS_DEFAULT_RADIUS_ARCSEC = 10.0
# DetFlag: 0 good, 1 reasonable, 2 poor. Poor detections are often spurious.
LSXPS_MAX_DET_FLAG = 1

LSXPS_ACKNOWLEDGEMENT = (
    'This work made use of data supplied by the UK Swift Science Data Centre at the '
    'University of Leicester, from the Living Swift-XRT Point Source catalogue '
    '(LSXPS; Evans et al. 2023, MNRAS 518, 174).'
)


def _to_float(value):
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def _find_nearest_source(ra, dec, radius_arcsec):
    """Nearest LSXPS source within the cone as a dict, or None."""
    from swifttools.ukssdc.query.SXPS import SXPSQuery

    query = SXPSQuery(cat='LSXPS', table='sources', silent=True)
    query.addConeSearch(position=f'{ra} {dec}', radius=radius_arcsec, units='arcsec')
    query.submit()
    results = query.results
    if results is None or len(results) == 0:
        return None
    nearest = results.sort_values('_r').iloc[0]
    return {
        'id': int(nearest['LSXPS_ID']),
        'name': str(nearest['IAUName']).strip(),
        'separation_arcsec': _to_float(nearest['_r']),
        'det_flag': int(nearest['DetFlag']),
    }


def _fetch_light_curve(source_id):
    """(source details, light curve dict) for one LSXPS source id."""
    import swifttools.ukssdc.data.SXPS as uds

    details = uds.getSourceDetails(sourceID=source_id, cat='LSXPS')
    light_curve = uds.getLightCurves(
        sourceID=source_id,
        cat='LSXPS',
        bands=['Total'],
        binning='obsid',
        timeFormat='MJD',
        returnData=True,
        saveData=False,
    )
    return details, light_curve


class LSXPSDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return LSXPS_DEFAULT_RADIUS_ARCSEC

    name = 'LSXPS'
    verbose_name = 'Swift-XRT LSXPS (X-ray)'
    # LSXPS is updated as Swift observes, so new points can appear between refreshes.
    update_on_daily_refresh = True
    info_url = LSXPS_PAGE_URL
    acknowledgement = LSXPS_ACKNOWLEDGEMENT
    upsert_identity_keys = ('filter', 'upper_limit')
    service_notes = (
        'Query the Living Swift-XRT Point Source catalogue (LSXPS) at the UK Swift Science Data '
        'Centre by coordinates. The nearest LSXPS source (detection flag good or reasonable) is '
        'used; its 0.3-10 keV per-observation light curve is converted to observed flux in '
        'erg/cm^2/s and plotted on the high-energy plot, with LSXPS upper limits for '
        'non-detections. No aliases are added.'
    )

    @classmethod
    def get_form_class(cls):
        return LSXPSQueryForm

    @classmethod
    def get_acknowledgement(cls):
        return cls.acknowledgement

    def build_query_parameters(self, parameters, **kwargs):
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or self.get_finding_chart_radius_arcsec(),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or self.get_finding_chart_radius_arcsec()

        match, details, light_curve = None, None, None
        if ra is not None and dec is not None:
            try:
                match = _find_nearest_source(ra, dec, radius_arcsec)
                if match and match['det_flag'] > LSXPS_MAX_DET_FLAG:
                    logger.debug('LSXPS source %s has a poor detection flag; skipped', match['name'])
                    match = None
                if match:
                    details, light_curve = _fetch_light_curve(match['id'])
            except Exception as exc:
                logger.warning('LSXPS query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                match, details, light_curve = None, None, None

        self.query_results = {
            'match': match,
            'details': details,
            'light_curve': light_curve,
            # Source pages are addressed by IAU name, e.g. /LSXPS/LSXPS%20J170249.3-484722.
            'source_location': (
                f"{LSXPS_PAGE_URL}{quote(match['name'])}" if match else LSXPS_PAGE_URL
            ),
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        match = data.get('match')
        if data.get('ra') is None or data.get('dec') is None or not match:
            return []

        datums = self._build_highenergy_datums(data.get('details'), data.get('light_curve'), match)
        if not datums:
            return []

        return [{
            'name': match['name'].replace(' ', '_'),
            'ra': data['ra'],
            'dec': data['dec'],
            # High-energy data only: LSXPS names are not names anyone searches for.
            'aliases': [],
            'reduced_datums': {'highenergy': datums},
            'source_location': data.get('source_location'),
        }]

    def create_target_from_query(self, target_result, **kwargs):
        return Target(
            name=target_result['name'],
            type='SIDEREAL',
            ra=target_result.get('ra'),
            dec=target_result.get('dec'),
            epoch=2000.0,
        )

    def create_aliases_from_query(self, alias_results, **kwargs):
        return []

    def create_reduced_datums_from_query(self, target, data=None, data_type=None, **kwargs):
        if data_type != 'highenergy' or not data:
            return 0
        created, _updated = upsert_reduced_datums(
            target=target,
            data_type='highenergy',
            source_name=self.name,
            source_location=kwargs.get('source_location') or self.info_url,
            datums=data,
            identity_keys=self.upsert_identity_keys,
        )
        return created

    def to_reduced_datums(self, target, data_results=None, **kwargs):
        if not data_results:
            return
        for data_type, data in data_results.items():
            self.create_reduced_datums_from_query(
                target,
                data=data,
                data_type=data_type,
                source_location=(getattr(self, 'query_results', {}) or {}).get('source_location'),
            )

    def _build_highenergy_datums(self, details, light_curve, match):
        ecf = _to_float((details or {}).get('PowECFO'))
        if not ecf or ecf <= 0 or not isinstance(light_curve, dict):
            logger.warning('LSXPS source %s has no usable conversion factor or light curve', match.get('name'))
            return []

        base = {
            'filter': LSXPS_FILTER,
            'flux_unit': LSXPS_FLUX_UNIT,
            'ecf': ecf,
            'lsxps_id': match['id'],
            'match_separation_arcsec': round(match['separation_arcsec'] or 0.0, 4),
            'facility': 'Swift-XRT',
            'observer': 'LSXPS',
        }
        output = []

        rates = light_curve.get('Total_rates')
        if rates is not None:
            for _, row in rates.iterrows():
                mjd, rate = _to_float(row.get('Time')), _to_float(row.get('Rate'))
                pos, neg = _to_float(row.get('RatePos')), _to_float(row.get('RateNeg'))
                if mjd is None or rate is None or rate <= 0 or pos is None or neg is None:
                    continue
                rate_err = (abs(pos) + abs(neg)) / 2.0
                if rate_err <= 0:
                    continue
                output.append({
                    'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                    'value': {
                        **base,
                        'flux': rate * ecf,
                        'error': rate_err * ecf,
                        'rate': rate,
                        'rate_error': rate_err,
                        'exposure_s': _to_float(row.get('Exposure')),
                        'mjd': mjd,
                        'upper_limit': False,
                    },
                })

        limits = light_curve.get('Total_UL')
        if limits is not None:
            for _, row in limits.iterrows():
                mjd, upper = _to_float(row.get('Time')), _to_float(row.get('UpperLimit'))
                if mjd is None or upper is None or upper <= 0:
                    continue
                output.append({
                    'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                    'value': {
                        **base,
                        'flux': upper * ecf,
                        'error': -1.0,
                        'rate': upper,
                        'exposure_s': _to_float(row.get('Exposure')),
                        'mjd': mjd,
                        'upper_limit': True,
                    },
                })
        return output
