"""ALMA Calibrator Source Catalogue: mm/sub-mm flux-density light curves of ALMA calibrators.

The catalogue holds the flux densities ALMA measures for its calibrators (mostly bright
quasars and blazars, ~3000+ sources, 2011 onwards, ALMA Bands 1-10, ~35-950 GHz) and grows as
calibrator monitoring continues. It is queried through the catalogue web application's JSON
search endpoint (the one behind https://almascience.eso.org/sc/): a cone search over the ALMA
catalogue with an open date range returns every measurement of every source in the cone.

Only the nearest calibrator is used, and only measurements flagged valid with a positive flux
density and uncertainty. Flux densities are stored in mJy as radio data (data_type 'radio'),
one filter per ALMA band; the exact observing frequency is kept with each point.
"""

import logging
import math
import re
from datetime import datetime, timezone

import requests

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target

from custom_code.data_services.forms import ALMAQueryForm
from custom_code.data_services.service_utils import (
    DATA_SERVICE_HTTP_TIMEOUT,
    resolve_query_coordinates,
    upsert_reduced_datums,
)

logger = logging.getLogger(__name__)

ALMA_PAGE_URL = 'https://almascience.eso.org/alma-data/calibrator-catalogue'
ALMA_SEARCH_URL = 'https://almascience.eso.org/sc/search'
# Catalogue ids in the search form: 5 = ALMA (41 = VLBI positions, 81 = total-power off positions).
ALMA_CATALOGUE_ID = '5'
ALMA_DEFAULT_RADIUS_ARCSEC = 5.0
ALMA_FLUX_UNIT = 'mJy'

ALMA_ACKNOWLEDGEMENT = (
    'This research uses flux densities from the ALMA Calibrator Source Catalogue. ALMA is a '
    'partnership of ESO (representing its member states), NSF (USA) and NINS (Japan), together '
    'with NRC (Canada), NSTC and ASIAA (Taiwan), and KASI (Republic of Korea), in cooperation '
    'with the Republic of Chile. The Joint ALMA Observatory is operated by ESO, AUI/NRAO and NAOJ.'
)


def _to_float(value):
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def _band_filter(band_name):
    """'ALMA-Band 3' -> 'ALMA(B3)'."""
    match = re.search(r'Band\s*(\d+)', str(band_name or ''))
    return f'ALMA(B{int(match.group(1))})' if match else None


def _parse_date(value):
    try:
        return datetime.strptime(str(value).strip(), '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def _search_measurements(ra, dec, radius_arcsec):
    """All ALMA-catalogue measurements of every calibrator within the cone."""
    response = requests.post(
        ALMA_SEARCH_URL,
        data={
            'internalVersion': 'false',
            'readOnlyVersion': 'true',
            'ra': f'{ra:.7f}',
            'dec': f'{dec:.7f}',
            'radius': f'{radius_arcsec / 3600.0:.7f}',  # degrees
            'selectedCatalogues': ALMA_CATALOGUE_ID,
            # Without a date range the service returns only the latest measurement per band.
            'dateObservedFrom': '1990-01-01',
            'dateObservedTo': '2100-01-01',
        },
        timeout=DATA_SERVICE_HTTP_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get('status') != 'ok':
        raise ValueError(f"ALMA catalogue search failed: {payload.get('errorFields')}")
    return payload.get('data') or []


def _nearest_source_measurements(measurements):
    """(source info, measurements) for the calibrator nearest the search position."""
    by_source = {}
    for row in measurements:
        source_id = row.get('sourceId')
        separation = _to_float(row.get('separationInDegrees'))
        if source_id is None or separation is None:
            continue
        entry = by_source.setdefault(source_id, {'separation_deg': separation, 'rows': [], 'row': row})
        entry['rows'].append(row)
    if not by_source:
        return None, []
    source_id, entry = min(by_source.items(), key=lambda item: item[1]['separation_deg'])
    row = entry['row']
    names = [str(n.get('name')).strip() for n in (row.get('names') or []) if n.get('name')]
    return {
        'source_id': source_id,
        'name': names[0] if names else f'ALMA_{source_id}',
        'ra': _to_float(row.get('sourceRaDeg')),
        'dec': _to_float(row.get('sourceDecDeg')),
        'separation_arcsec': entry['separation_deg'] * 3600.0,
    }, entry['rows']


class ALMADataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return ALMA_DEFAULT_RADIUS_ARCSEC

    name = 'ALMA'
    verbose_name = 'ALMA Calibrator Catalogue (mm/sub-mm)'
    # Calibrators are re-measured continually.
    update_on_daily_refresh = True
    info_url = ALMA_PAGE_URL
    acknowledgement = ALMA_ACKNOWLEDGEMENT
    # The catalogue's own id for one flux-density measurement.
    upsert_identity_keys = ('filter', 'alma_measurement_id')
    service_notes = (
        'Query the ALMA Calibrator Source Catalogue by coordinates. The nearest calibrator within '
        '5 arcsec is used; all of its valid ALMA flux-density measurements (Bands 1-10, 2011 '
        'onwards) are imported in mJy and plotted on the right-hand axis of the photometry plot. '
        'Radio data only; no aliases are added.'
    )

    @classmethod
    def get_form_class(cls):
        return ALMAQueryForm

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
            'include_radio': bool(parameters.get('include_radio', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or self.get_finding_chart_radius_arcsec()

        match, measurements = None, []
        if ra is not None and dec is not None and query_parameters.get('include_radio', True):
            try:
                match, measurements = _nearest_source_measurements(_search_measurements(ra, dec, radius_arcsec))
                if not match:
                    logger.debug('ALMA calibrator catalogue returned no source for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('ALMA calibrator catalogue query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                match, measurements = None, []

        self.query_results = {
            'match': match,
            'measurements': measurements,
            'source_location': ALMA_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        match = data.get('match')
        if data.get('ra') is None or data.get('dec') is None or not match:
            return []

        datums = self._build_radio_datums(data.get('measurements') or [], match)
        if not datums:
            return []

        return [{
            'name': match['name'],
            'ra': data['ra'],
            'dec': data['dec'],
            'aliases': [],
            'reduced_datums': {'radio': datums},
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
        if data_type != 'radio' or not data:
            return 0
        created, _updated = upsert_reduced_datums(
            target=target,
            data_type='radio',
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

    def _build_radio_datums(self, rows, match):
        output = []
        for row in rows:
            if row.get('valid') is not True:
                continue
            filter_name = _band_filter(row.get('bandName'))
            timestamp = _parse_date(row.get('dateObserved'))
            flux_jy = _to_float(row.get('flux'))
            error_jy = _to_float(row.get('fluxUncertainty'))
            frequency_hz = _to_float(row.get('frequency'))
            if filter_name is None or timestamp is None or flux_jy is None or error_jy is None:
                continue
            if flux_jy <= 0 or error_jy <= 0:
                continue
            value = {
                'filter': filter_name,
                'flux': flux_jy * 1000.0,
                'error': error_jy * 1000.0,
                'flux_unit': ALMA_FLUX_UNIT,
                'frequency_ghz': round(frequency_hz / 1e9, 4) if frequency_hz else None,
                'facility': 'ALMA',
                'alma_measurement_id': row.get('id'),
                'alma_source_name': match['name'],
                'match_separation_arcsec': round(match['separation_arcsec'], 4),
            }
            output.append({'timestamp': timestamp, 'value': value})
        return output
