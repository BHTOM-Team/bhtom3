import math
import re
import logging

from astropy.time import Time
from astroquery.gaia import Gaia
from django.utils import timezone

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import GaiaDR3QueryForm

logger = logging.getLogger(__name__)


def _to_float(value):
    try:
        converted = float(value)
        if math.isnan(converted):
            return None
        return converted
    except (TypeError, ValueError):
        return None


def _row_to_dict(row):
    data = {}
    for key in row.colnames:
        value = row[key]
        if hasattr(value, 'mask') and bool(value.mask):
            data[key] = None
        else:
            data[key] = value.item() if hasattr(value, 'item') else value
    return data


def _gaia_time_to_mjd(gaia_time):
    return float(gaia_time) + 55197.0


def _mag_error(flux_over_error):
    return 1.0 / (float(flux_over_error) * 2.5 / math.log(10.0))


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

        if source_id and str(source_id).isdigit():
            query = (
                'SELECT TOP 1 source_id, ra, dec, pmra, pmdec, parallax '
                'FROM gaiadr3.gaia_source '
                f'WHERE source_id = {source_id}'
            )
            result = Gaia.launch_job(query).get_results()
            if len(result) > 0:
                source_row = _row_to_dict(result[0])

        if source_row is None and ra is not None and dec is not None:
            ra_deg = float(ra)
            dec_deg = float(dec)
            radius_deg = radius_arcsec / 3600.0
            box_prefilter = _build_box_prefilter(ra_deg, dec_deg, radius_deg)
            query = (
                'SELECT TOP 1 source_id, ra, dec, pmra, pmdec, parallax, '
                f'       DISTANCE(POINT({ra_deg}, {dec_deg}), POINT(ra, dec)) AS dist '
                'FROM gaiadr3.gaia_source '
                f'WHERE {box_prefilter} '
                f'  AND DISTANCE(POINT({ra_deg}, {dec_deg}), POINT(ra, dec)) <= {radius_deg} '
                'ORDER BY dist ASC'
            )
            result = Gaia.launch_job(query).get_results()
            if len(result) > 0:
                source_row = _row_to_dict(result[0])

        phot_rows = []
        if source_row and query_parameters.get('include_photometry', True):
            source_id = source_row.get('SOURCE_ID', source_row.get('source_id'))
            try:
                datalink = Gaia.load_data(
                    ids=[str(source_id)],
                    data_release='Gaia DR3',
                    retrieval_type='EPOCH_PHOTOMETRY',
                    data_structure='INDIVIDUAL',
                    verbose=False,
                    output_file=None,
                    format='votable',
                )
                keys = sorted(datalink.keys())
                if keys:
                    phot_rows = datalink[keys[0]][0].to_table().to_pandas().to_dict(orient='records')
            except Exception as exc:
                # Gaia's auxiliary data server is occasionally unavailable.
                # Keep the query usable by returning source metadata without photometry.
                logger.warning('Gaia DR3 epoch photometry unavailable for source %s: %s', source_id, exc)

        self.query_results = {'source': source_row, 'photometry_rows': phot_rows}
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
        for datum in data:
            ReducedDatum.objects.get_or_create(
                target=target,
                data_type='photometry',
                timestamp=datum['timestamp'],
                value=datum['value'],
                defaults={
                    'source_name': self.name,
                    'source_location': self.info_url,
                },
            )

    def _build_photometry_datums(self, rows):
        output = []
        band_specs = [
            ('G', 'g_transit_time', 'g_transit_mag', 'g_transit_flux_over_error'),
            ('BP', 'bp_obs_time', 'bp_mag', 'bp_flux_over_error'),
            ('RP', 'rp_obs_time', 'rp_mag', 'rp_flux_over_error'),
        ]
        for row in rows:
            for band, time_col, mag_col, flux_col in band_specs:
                t_val = _to_float(row.get(time_col))
                mag = _to_float(row.get(mag_col))
                ferr = _to_float(row.get(flux_col))
                if t_val is None or mag is None or ferr is None or ferr <= 0:
                    continue
                try:
                    mjd = _gaia_time_to_mjd(t_val)
                    err = _mag_error(ferr)
                except (TypeError, ValueError, ZeroDivisionError):
                    continue
                output.append({
                    'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                    'value': {'filter': band, 'magnitude': mag, 'error': err},
                })
        return output
