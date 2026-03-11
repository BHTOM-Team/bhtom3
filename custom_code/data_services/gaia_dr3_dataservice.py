import math
import re
import logging

from astropy.time import Time
from astroquery.gaia import Gaia
from django.utils import timezone
import pyvo

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import GaiaDR3QueryForm

logger = logging.getLogger(__name__)

AIP_TAP_URL = 'https://gaia.aip.de/tap'


def _to_float(value):
    try:
        converted = float(value)
        if math.isnan(converted):
            return None
        return converted
    except (TypeError, ValueError):
        return None


def _value_to_python(value):
    mask = getattr(value, 'mask', None)
    if mask is not None:
        if hasattr(mask, 'all'):
            if mask.all():
                return None
        elif bool(mask):
            return None

    if hasattr(value, 'filled') and hasattr(mask, 'all'):
        filled = value.filled(None)
        if hasattr(filled, 'tolist'):
            return filled.tolist()
        return filled

    if hasattr(value, 'tolist'):
        return value.tolist()
    return value.item() if hasattr(value, 'item') else value


def _row_to_dict(row):
    return {key: _value_to_python(row[key]) for key in row.colnames}


def _gaia_time_to_mjd(gaia_time):
    return float(gaia_time) + 55197.0


def _mag_error(flux_over_error):
    return 1.0 / (float(flux_over_error) * 2.5 / math.log(10.0))


def _ensure_sequence(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    if hasattr(value, 'tolist'):
        converted = value.tolist()
        return converted if isinstance(converted, list) else [converted]
    return [value]


def _build_box_prefilter(ra_deg, dec_deg, radius_deg):
    cos_dec = abs(math.cos(math.radians(dec_deg)))
    # Prevent exploding RA span near the poles.
    cos_dec = max(cos_dec, 1e-6)
    ra_half_width = min(180.0, radius_deg / cos_dec)
    dec_min = max(-90.0, dec_deg - radius_deg)
    dec_max = min(90.0, dec_deg + radius_deg)

    ra_min = (ra_deg - ra_half_width) % 360.0
    ra_max = (ra_deg + ra_half_width) % 360.0
    if ra_half_width >= 180.0:
        ra_clause = '1 = 1'
    elif ra_min <= ra_max:
        ra_clause = f'ra BETWEEN {ra_min} AND {ra_max}'
    else:
        ra_clause = f'(ra >= {ra_min} OR ra <= {ra_max})'

    return f'({ra_clause}) AND dec BETWEEN {dec_min} AND {dec_max}'


def _build_source_query(where_clause, extra_columns=''):
    columns = 'source_id, ra, dec, pmra, pmdec, parallax'
    if extra_columns:
        columns = f'{columns}, {extra_columns}'
    return f'SELECT TOP 1 {columns} FROM gaiadr3.gaia_source WHERE {where_clause}'


def _build_aip_epoch_query(source_id):
    return (
        'SELECT source_id, transit_id, g_transit_time, g_transit_mag, g_transit_flux_over_error, '
        'bp_obs_time, bp_mag, bp_flux_over_error, '
        'rp_obs_time, rp_mag, rp_flux_over_error '
        'FROM gaiadr3.epoch_photometry '
        f'WHERE source_id = {int(source_id)}'
    )


class GaiaDR3DataService(DataService):
    name = 'GaiaDR3'
    verbose_name = 'GaiaDR3'
    info_url = 'https://gea.esac.esa.int/archive/'
    service_notes = 'Query Gaia DR3 by source_id or cone search, with optional epoch photometry.'

    @classmethod
    def get_form_class(cls):
        return GaiaDR3QueryForm

    def build_query_parameters(self, parameters, **kwargs):
        source_id = (parameters.get('source_id') or '').strip()
        if source_id:
            match = re.search(r'(\d+)', source_id)
            source_id = match.group(1) if match else source_id
        self.query_parameters = {
            'source_id': source_id,
            'ra': parameters.get('ra'),
            'dec': parameters.get('dec'),
            'radius_arcsec': parameters.get('radius_arcsec') or 1.0,
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        source_row = None
        source_id = query_parameters.get('source_id')
        ra = query_parameters.get('ra')
        dec = query_parameters.get('dec')
        radius_arcsec = float(query_parameters.get('radius_arcsec') or 1.0)
        source_origin = None

        if source_id and str(source_id).isdigit():
            query = _build_source_query(f'source_id = {source_id}')
            source_row = self._query_source_esa(query)
            source_origin = 'esa' if source_row else None
            if source_row is None:
                source_row = self._query_source_aip(query)
                source_origin = 'aip' if source_row else None

        if source_row is None and ra is not None and dec is not None:
            ra_deg = float(ra)
            dec_deg = float(dec)
            radius_deg = radius_arcsec / 3600.0
            box_prefilter = _build_box_prefilter(ra_deg, dec_deg, radius_deg)
            query = (
                _build_source_query(
                    f'{box_prefilter} '
                    f' AND DISTANCE(POINT({ra_deg}, {dec_deg}), POINT(ra, dec)) <= {radius_deg}',
                    f'DISTANCE(POINT({ra_deg}, {dec_deg}), POINT(ra, dec)) AS dist',
                )
                + ' ORDER BY dist ASC'
            )
            source_row = self._query_source_esa(query)
            source_origin = 'esa' if source_row else None
            if source_row is None:
                source_row = self._query_source_aip(query)
                source_origin = 'aip' if source_row else None

        phot_rows = []
        if source_row and query_parameters.get('include_photometry', True):
            source_id = source_row.get('SOURCE_ID', source_row.get('source_id'))
            phot_rows = self._fetch_epoch_photometry_esa(source_id)
            if phot_rows:
                source_origin = source_origin or 'esa'
            else:
                phot_rows = self._fetch_epoch_photometry_aip(source_id)
                if phot_rows:
                    source_origin = 'aip'

        self.query_results = {
            'source': source_row,
            'photometry_rows': phot_rows,
            'source_origin': source_origin,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        source = data.get('source')
        if not source:
            return []

        source_id = source.get('SOURCE_ID', source.get('source_id'))
        target_result = {
            'name': f'GaiaDR3_{source_id}',
            'ra': _to_float(source.get('ra')),
            'dec': _to_float(source.get('dec')),
            'pmra': _to_float(source.get('pmra')),
            'pmdec': _to_float(source.get('pmdec')),
            'parallax': _to_float(source.get('parallax')),
            'aliases': [f'GaiaDR3_{source_id}'],
            'reduced_datums': {'photometry': self._build_photometry_datums(data.get('photometry_rows', []))},
        }
        return [target_result]

    def create_target_from_query(self, target_result, **kwargs):
        return Target(
            name=target_result['name'],
            type='SIDEREAL',
            ra=target_result.get('ra'),
            dec=target_result.get('dec'),
            pm_ra=target_result.get('pmra'),
            pm_dec=target_result.get('pmdec'),
            parallax=target_result.get('parallax'),
        )

    def create_aliases_from_query(self, alias_results, **kwargs):
        return [TargetName(name=alias) for alias in alias_results]

    def create_reduced_datums_from_query(self, target, data=None, data_type=None, **kwargs):
        if data_type != 'photometry' or not data:
            return
        source_location = kwargs.get('source_location') or self.info_url
        for datum in data:
            ReducedDatum.objects.get_or_create(
                target=target,
                data_type='photometry',
                timestamp=datum['timestamp'],
                value=datum['value'],
                defaults={
                    'source_name': self.name,
                    'source_location': source_location,
                },
            )

    def to_reduced_datums(self, target, data_results=None, **kwargs):
        if not data_results:
            return
        source_origin = self.query_results.get('source_origin')
        source_location = self.info_url if source_origin != 'aip' else AIP_TAP_URL
        for data_type, data in data_results.items():
            self.create_reduced_datums_from_query(
                target,
                data=data,
                data_type=data_type,
                source_location=source_location,
            )

    def _query_source_esa(self, query):
        try:
            result = Gaia.launch_job(query).get_results()
            if len(result) > 0:
                return _row_to_dict(result[0])
        except Exception as exc:
            logger.warning('Gaia DR3 ESA source query failed: %s', exc)
        return None

    def _query_source_aip(self, query):
        try:
            result = pyvo.dal.TAPService(AIP_TAP_URL).run_sync(query, language='ADQL').to_table()
            if len(result) > 0:
                return _row_to_dict(result[0])
        except Exception as exc:
            logger.warning('Gaia DR3 AIP source query failed: %s', exc)
        return None

    def _fetch_epoch_photometry_esa(self, source_id):
        try:
            datalink = Gaia.load_data(
                ids=[str(source_id)],
                data_release='Gaia DR3',
                retrieval_type='EPOCH_PHOTOMETRY',
                data_structure='INDIVIDUAL',
                verbose=False,
                format='votable',
            )
            keys = sorted(datalink.keys())
            if keys:
                return datalink[keys[0]][0].to_table().to_pandas().to_dict(orient='records')
        except Exception as exc:
            logger.warning('Gaia DR3 ESA epoch photometry unavailable for source %s: %s', source_id, exc)
        return []

    def _fetch_epoch_photometry_aip(self, source_id):
        try:
            query = _build_aip_epoch_query(source_id)
            result = pyvo.dal.TAPService(AIP_TAP_URL).run_sync(query, language='ADQL').to_table()
            if len(result) > 0:
                return [_row_to_dict(result[0])]
        except Exception as exc:
            logger.warning('Gaia DR3 AIP epoch photometry unavailable for source %s: %s', source_id, exc)
        return []

    def _build_photometry_datums(self, rows):
        output = []
        band_specs = [
            ('GaiaDR3(G)', 'g_transit_time', 'g_transit_mag', 'g_transit_flux_over_error'),
            ('GaiaDR3(BP)', 'bp_obs_time', 'bp_mag', 'bp_flux_over_error'),
            ('GaiaDR3(RP)', 'rp_obs_time', 'rp_mag', 'rp_flux_over_error'),
        ]
        for row in rows:
            for band, time_col, mag_col, flux_col in band_specs:
                times = _ensure_sequence(row.get(time_col))
                mags = _ensure_sequence(row.get(mag_col))
                ferrs = _ensure_sequence(row.get(flux_col))
                for t_val, mag_val, ferr_val in zip(times, mags, ferrs):
                    t_float = _to_float(t_val)
                    mag = _to_float(mag_val)
                    ferr = _to_float(ferr_val)
                    if t_float is None or mag is None or ferr is None or ferr <= 0:
                        continue
                    try:
                        mjd = _gaia_time_to_mjd(t_float)
                        err = _mag_error(ferr)
                        timestamp = Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc)
                    except Exception:
                        continue
                    output.append({
                        'timestamp': timestamp,
                        'value': {'filter': band, 'magnitude': mag, 'error': err},
                    })
        return output
