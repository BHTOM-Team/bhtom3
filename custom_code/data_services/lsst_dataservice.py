import math
import re
from decimal import Decimal, InvalidOperation

import requests
from astropy.time import Time
from datetime import timezone

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import LSSTQueryForm


FINK_API_URL = 'https://api.lsst.fink-portal.org'


def _to_float(value):
    try:
        number = float(value)
        if math.isnan(number) or not math.isfinite(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def _normalize_lsst_id(term):
    term_str = str(term).strip()
    if term_str.upper().startswith('LSST_'):
        term_str = term_str.split('_', 1)[1].strip()
    try:
        parsed = Decimal(term_str)
        if parsed == parsed.to_integral_value():
            return str(int(parsed))
    except (InvalidOperation, ValueError):
        pass
    return term_str


def _first_present(row, keys):
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _parse_rows(response_json):
    if isinstance(response_json, list):
        return response_json
    if isinstance(response_json, dict):
        return response_json.get('data', [])
    return []


class LSSTDataService(DataService):
    name = 'LSST'
    verbose_name = 'LSST'
    info_url = 'https://api.fink-portal.org'
    service_notes = 'Query LSST (Fink) by diaObjectId or cone search, with optional photometry.'

    @classmethod
    def get_form_class(cls):
        return LSSTQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        dia_id = (parameters.get('dia_object_id') or '').strip()
        if dia_id:
            match = re.search(r'([0-9eE\+\-\.]+)', dia_id)
            dia_id = _normalize_lsst_id(match.group(1) if match else dia_id)
        self.query_parameters = {
            'dia_object_id': dia_id,
            'ra': parameters.get('ra'),
            'dec': parameters.get('dec'),
            'radius_arcsec': parameters.get('radius_arcsec') or 5.0,
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        dia_id = query_parameters.get('dia_object_id')
        ra = query_parameters.get('ra')
        dec = query_parameters.get('dec')
        radius_arcsec = float(query_parameters.get('radius_arcsec') or 5.0)

        object_rows = []
        source_rows = []

        if dia_id and str(dia_id).isdigit():
            object_rows = self._post('/api/v1/objects', {'diaObjectId': str(dia_id), 'output-format': 'json'})
            if not object_rows:
                object_rows = self._post('/api/v1/objects', {'objectId': str(dia_id), 'output-format': 'json'})
            if query_parameters.get('include_photometry', True):
                source_rows = self._post(
                    '/api/v1/sources',
                    {
                        'diaObjectId': str(dia_id),
                        'columns': 'r:diaObjectId,r:midpointMjdTai,r:psfFlux,r:psfFluxErr,r:band',
                        'output-format': 'json',
                    }
                )
                if not source_rows:
                    source_rows = self._post(
                        '/api/v1/sources',
                        {
                            'objectId': str(dia_id),
                            'columns': 'r:diaObjectId,r:midpointMjdTai,r:psfFlux,r:psfFluxErr,r:band',
                            'output-format': 'json',
                        }
                    )

        if not object_rows and ra is not None and dec is not None:
            cone_rows = self._post(
                '/api/v1/conesearch',
                {
                    'ra': float(ra),
                    'dec': float(dec),
                    'radius': float(radius_arcsec),
                    'output-format': 'json',
                }
            )
            if cone_rows:
                first = cone_rows[0]
                resolved_id = _first_present(first, ('r:diaObjectId', 'diaObjectId', 'objectId', 'i:objectId'))
                if resolved_id is not None:
                    resolved_id = _normalize_lsst_id(resolved_id)
                    object_rows = self._post(
                        '/api/v1/objects', {'diaObjectId': str(resolved_id), 'output-format': 'json'}
                    )
                    if not object_rows:
                        object_rows = cone_rows
                    if query_parameters.get('include_photometry', True):
                        source_rows = self._post(
                            '/api/v1/sources',
                            {
                                'diaObjectId': str(resolved_id),
                                'columns': 'r:diaObjectId,r:midpointMjdTai,r:psfFlux,r:psfFluxErr,r:band',
                                'output-format': 'json',
                            }
                        )

        self.query_results = {'objects': object_rows, 'sources': source_rows}
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        objects = data.get('objects') or []
        sources = data.get('sources') or []
        if not objects and not sources:
            return []

        first = objects[0] if objects else sources[0]
        lsst_id = _normalize_lsst_id(_first_present(first, ('r:diaObjectId', 'diaObjectId', 'objectId', 'i:objectId')))
        ra = _to_float(_first_present(first, ('i:ra', 'r:ra', 'ra')))
        dec = _to_float(_first_present(first, ('i:dec', 'r:dec', 'dec')))

        if (ra is None or dec is None) and sources:
            s0 = sources[0]
            ra = _to_float(_first_present(s0, ('i:ra', 'r:ra', 'ra')))
            dec = _to_float(_first_present(s0, ('i:dec', 'r:dec', 'dec')))

        if lsst_id is None or not str(lsst_id).isdigit() or ra is None or dec is None:
            return []

        target_result = {
            'name': f'LSST_{lsst_id}',
            'ra': ra,
            'dec': dec,
            'aliases': [f'LSST_{lsst_id}'],
            'reduced_datums': {'photometry': self._build_photometry_datums(sources)},
        }
        return [target_result]

    def create_target_from_query(self, target_result, **kwargs):
        return Target(
            name=target_result['name'],
            type='SIDEREAL',
            ra=target_result.get('ra'),
            dec=target_result.get('dec'),
        )

    def create_aliases_from_query(self, alias_results, **kwargs):
        return [TargetName(name=alias) for alias in alias_results]

    def create_reduced_datums_from_query(self, target, data=None, data_type=None, **kwargs):
        if data_type != 'photometry' or not data:
            return
        for datum in data:
            ReducedDatum.objects.get_or_create(
                target=target,
                data_type='photometry',
                timestamp=datum['timestamp'],
                value=datum['value'],
                defaults={
                    'source_name': self.name,
                    'source_location': f'{FINK_API_URL}/api/v1/sources',
                },
            )

    def _post(self, endpoint, payload):
        response = requests.post(f'{FINK_API_URL}{endpoint}', json=payload, timeout=25)
        response.raise_for_status()
        return _parse_rows(response.json())

    def _build_photometry_datums(self, rows):
        output = []
        for row in rows:
            mjd = _to_float(_first_present(row, ('r:midpointMjdTai', 'midpointMjdTai', 'mjd')))
            flux = _to_float(_first_present(row, ('r:psfFlux', 'psfFlux', 'flux')))
            flux_err = _to_float(_first_present(row, ('r:psfFluxErr', 'psfFluxErr', 'fluxErr')))
            band = _first_present(row, ('r:band', 'band')) or 'unknown'
            if mjd is None or flux is None or flux_err is None or flux <= 0 or flux_err <= 0:
                continue

            mag = -2.5 * math.log10((flux * 1e-9) / 3631.0)
            mag_err = 1.0857 * (flux_err / flux)
            if not math.isfinite(mag) or not math.isfinite(mag_err) or mag_err >= 1.5:
                continue

            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {'filter': f'LSST({band})', 'magnitude': mag, 'error': mag_err},
            })
        return output
