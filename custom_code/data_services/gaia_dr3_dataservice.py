import math
import re
import logging
from datetime import datetime

from astropy import units as u
from astropy.time import Time
from astroquery.gaia import Gaia
from datetime import timezone
import pyvo
from specutils import Spectrum1D

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_dataproducts.processors.data_serializers import SpectrumSerializer
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import GaiaDR3QueryForm

logger = logging.getLogger(__name__)

AIP_TAP_URL = 'https://gaia.aip.de/tap'
GAIA_DR3_RELEASE_TIMESTAMP = datetime(2022, 6, 13, tzinfo=timezone.utc)
GAIA_XP_WAVELENGTH_NM = [336.0 + (2.0 * idx) for idx in range(343)]


def _to_float(value):
    try:
        converted = float(value)
        if math.isnan(converted):
            return None
        return converted
    except (TypeError, ValueError):
        return None


def _to_text(value):
    if value is None:
        return None
    if hasattr(value, 'item'):
        value = value.item()
    if isinstance(value, bytes):
        value = value.decode('utf-8', errors='ignore')
    text = str(value).strip()
    return text or None


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


def _build_box_prefilter(ra_deg, dec_deg, radius_deg, column_prefix=''):
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
        ra_clause = f'{column_prefix}ra BETWEEN {ra_min} AND {ra_max}'
    else:
        ra_clause = f'({column_prefix}ra >= {ra_min} OR {column_prefix}ra <= {ra_max})'

    return f'({ra_clause}) AND {column_prefix}dec BETWEEN {dec_min} AND {dec_max}'


def _build_source_query(where_clause, extra_columns=''):
    columns = (
        'g.source_id, g.ra, g.dec, g.pmra, g.pmdec, g.parallax, '
        'g.pmra_error, g.pmdec_error, g.parallax_error, g.has_xp_sampled, '
        'vcr.best_class_name AS gaia_variability_type'
    )
    if extra_columns:
        columns = f'{columns}, {extra_columns}'
    return (
        f'SELECT TOP 1 {columns} '
        'FROM gaiadr3.gaia_source AS g '
        'LEFT OUTER JOIN gaiadr3.vari_classifier_result AS vcr '
        "ON g.source_id = vcr.source_id AND vcr.classifier_name = 'n_transits:5+' "
        f'WHERE {where_clause}'
    )


def _build_aip_epoch_query(source_id):
    return (
        'SELECT source_id, transit_id, g_transit_time, g_transit_mag, g_transit_flux_over_error, '
        'bp_obs_time, bp_mag, bp_flux_over_error, '
        'rp_obs_time, rp_mag, rp_flux_over_error '
        'FROM gaiadr3.epoch_photometry '
        f'WHERE source_id = {int(source_id)}'
    )


def _build_aip_xp_query(source_id):
    return (
        'SELECT source_id, flux, flux_error '
        'FROM gaiadr3.xp_sampled_mean_spectrum '
        f'WHERE source_id = {int(source_id)}'
    )


def _boolify(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in {'1', 'true', 't', 'yes', 'y'}
    return bool(value)


def _normalize_spectrum_table(table):
    if len(table) == 0:
        return None

    first_row = _row_to_dict(table[0])
    row_keys = {key.lower(): key for key in first_row.keys()}
    if {'wavelength', 'flux'} <= set(row_keys):
        wavelengths = []
        fluxes = []
        flux_errors = []
        for row in table:
            row_data = _row_to_dict(row)
            wavelength = _to_float(row_data.get(row_keys['wavelength']))
            flux = _to_float(row_data.get(row_keys['flux']))
            flux_error = _to_float(row_data.get(row_keys.get('flux_error')))
            if wavelength is None or flux is None:
                continue
            wavelengths.append(wavelength)
            fluxes.append(flux)
            flux_errors.append(flux_error)
        return {
            'wavelength': wavelengths,
            'wavelength_units': 'nm',
            'flux': fluxes,
            'flux_units': 'W / (nm m2)',
            'flux_error': flux_errors,
            'flux_error_units': 'W / (nm m2)',
        }

    flux = _ensure_sequence(first_row.get(row_keys.get('flux', 'flux')))
    flux_error = _ensure_sequence(first_row.get(row_keys.get('flux_error', 'flux_error')))
    if flux:
        return {
            'wavelength': list(GAIA_XP_WAVELENGTH_NM),
            'wavelength_units': 'nm',
            'flux': flux,
            'flux_units': 'W / (nm m2)',
            'flux_error': flux_error,
            'flux_error_units': 'W / (nm m2)',
        }
    return None


class GaiaDR3DataService(DataService):
    name = 'GaiaDR3'
    verbose_name = 'GaiaDR3'
    update_on_daily_refresh = False
    info_url = 'https://gea.esac.esa.int/archive/'
    service_notes = 'Query Gaia DR3 by source_id or cone search, with optional epoch photometry.'

    @classmethod
    def get_form_class(cls):
        return GaiaDR3QueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        source_id = (parameters.get('source_id') or '').strip()
        if source_id:
            match = re.search(r'(\d+)', source_id)
            source_id = match.group(1) if match else source_id
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'source_id': source_id,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or 1.0,
            'include_photometry': bool(parameters.get('include_photometry', True)),
            'include_spectroscopy': bool(parameters.get('include_spectroscopy', True)),
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
            query = _build_source_query(f'g.source_id = {source_id}')
            source_row = self._query_source_esa(query)
            source_origin = 'esa' if source_row else None
            if source_row is None:
                source_row = self._query_source_aip(query)
                source_origin = 'aip' if source_row else None

        if source_row is None and ra is not None and dec is not None:
            ra_deg = float(ra)
            dec_deg = float(dec)
            radius_deg = radius_arcsec / 3600.0
            box_prefilter = _build_box_prefilter(ra_deg, dec_deg, radius_deg, column_prefix='g.')
            query = (
                _build_source_query(
                    f'{box_prefilter} '
                    f' AND DISTANCE(POINT({ra_deg}, {dec_deg}), POINT(g.ra, g.dec)) <= {radius_deg}',
                    f'DISTANCE(POINT({ra_deg}, {dec_deg}), POINT(g.ra, g.dec)) AS dist',
                )
                + ' ORDER BY dist ASC'
            )
            source_row = self._query_source_esa(query)
            source_origin = 'esa' if source_row else None
            if source_row is None:
                source_row = self._query_source_aip(query)
                source_origin = 'aip' if source_row else None

        phot_rows = []
        spectra = []
        phot_origin = None
        spectrum_origin = None
        if source_row and query_parameters.get('include_photometry', True):
            source_id = source_row.get('SOURCE_ID', source_row.get('source_id'))
            phot_rows = self._fetch_epoch_photometry_esa(source_id)
            if phot_rows:
                phot_origin = 'esa'
            else:
                phot_rows = self._fetch_epoch_photometry_aip(source_id)
                if phot_rows:
                    phot_origin = 'aip'

        if source_row and query_parameters.get('include_spectroscopy', True):
            source_id = source_row.get('SOURCE_ID', source_row.get('source_id'))
            if _boolify(source_row.get('has_xp_sampled', source_row.get('HAS_XP_SAMPLED'))):
                spectra = self._fetch_xp_spectrum_esa(source_id)
                if spectra:
                    spectrum_origin = 'esa'
                else:
                    spectra = self._fetch_xp_spectrum_aip(source_id)
                    if spectra:
                        spectrum_origin = 'aip'

        self.query_results = {
            'source': source_row,
            'photometry_rows': phot_rows,
            'spectroscopy_rows': spectra,
            'source_origin': source_origin,
            'photometry_origin': phot_origin or source_origin,
            'spectroscopy_origin': spectrum_origin,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        source = data.get('source')
        if not source:
            return []

        source_id = source.get('SOURCE_ID', source.get('source_id'))
        variability_type = _to_text(source.get('gaia_variability_type', source.get('GAIA_VARIABILITY_TYPE')))
        target_result = {
            'name': f'GaiaDR3_{source_id}',
            'ra': _to_float(source.get('ra')),
            'dec': _to_float(source.get('dec')),
            'pmra': _to_float(source.get('pmra')),
            'pmdec': _to_float(source.get('pmdec')),
            'parallax': _to_float(source.get('parallax')),
            'pm_ra_error': _to_float(source.get('pmra_error')),
            'pm_dec_error': _to_float(source.get('pmdec_error')),
            'parallax_error': _to_float(source.get('parallax_error')),
            'gaia_variability_type': variability_type,
            'aliases': [f'GaiaDR3_{source_id}'],
            'target_updates': {
                key: value
                for key, value in {
                    'ra': _to_float(source.get('ra')),
                    'dec': _to_float(source.get('dec')),
                    'epoch': 2000.0,
                    'pm_ra': _to_float(source.get('pmra')),
                    'pm_dec': _to_float(source.get('pmdec')),
                    'parallax': _to_float(source.get('parallax')),
                    'pm_ra_error': _to_float(source.get('pmra_error')),
                    'pm_dec_error': _to_float(source.get('pmdec_error')),
                    'parallax_error': _to_float(source.get('parallax_error')),
                    'gaia_variability_type': variability_type,
                }.items()
                if value is not None
            },
            'reduced_datums': {
                'photometry': self._build_photometry_datums(data.get('photometry_rows', [])),
                'spectroscopy': self._build_spectroscopy_datums(source_id, data.get('spectroscopy_rows', [])),
            },
        }
        return [target_result]

    def create_target_from_query(self, target_result, **kwargs):
        return Target(
            name=target_result['name'],
            type='SIDEREAL',
            ra=target_result.get('ra'),
            dec=target_result.get('dec'),
            epoch=2000.0,
            pm_ra=target_result.get('pmra'),
            pm_dec=target_result.get('pmdec'),
            parallax=target_result.get('parallax'),
            gaia_variability_type=target_result.get('gaia_variability_type'),
        )

    def create_aliases_from_query(self, alias_results, **kwargs):
        return [TargetName(name=alias) for alias in alias_results]

    def create_reduced_datums_from_query(self, target, data=None, data_type=None, **kwargs):
        if data_type not in {'photometry', 'spectroscopy'} or not data:
            return
        source_location = kwargs.get('source_location') or self.info_url
        for datum in data:
            ReducedDatum.objects.get_or_create(
                target=target,
                data_type=data_type,
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
        for data_type, data in data_results.items():
            if data_type == 'spectroscopy':
                origin = self.query_results.get('spectroscopy_origin')
            else:
                origin = self.query_results.get('photometry_origin') or self.query_results.get('source_origin')
            source_location = self.info_url if origin != 'aip' else AIP_TAP_URL
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

    def _fetch_xp_spectrum_esa(self, source_id):
        try:
            datalink = Gaia.load_data(
                ids=[str(source_id)],
                data_release='Gaia DR3',
                retrieval_type='XP_SAMPLED',
                data_structure='INDIVIDUAL',
                verbose=False,
                format='votable',
            )
            for key in sorted(datalink.keys()):
                for table in datalink[key]:
                    normalized = _normalize_spectrum_table(
                        table.to_table() if hasattr(table, 'to_table') else table
                    )
                    if normalized and normalized.get('flux'):
                        return [normalized]
        except Exception as exc:
            logger.warning('Gaia DR3 ESA XP spectrum unavailable for source %s: %s', source_id, exc)
        return []

    def _fetch_xp_spectrum_aip(self, source_id):
        try:
            query = _build_aip_xp_query(source_id)
            result = pyvo.dal.TAPService(AIP_TAP_URL).run_sync(query, language='ADQL').to_table()
            normalized = _normalize_spectrum_table(result)
            if normalized and normalized.get('flux'):
                return [normalized]
        except Exception as exc:
            logger.warning('Gaia DR3 AIP XP spectrum unavailable for source %s: %s', source_id, exc)
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

    def _build_spectroscopy_datums(self, source_id, rows):
        output = []
        serializer = SpectrumSerializer()
        for row in rows:
            wavelengths = [_to_float(val) for val in _ensure_sequence(row.get('wavelength'))]
            fluxes = [_to_float(val) for val in _ensure_sequence(row.get('flux'))]
            flux_errors = [_to_float(val) for val in _ensure_sequence(row.get('flux_error'))]
            clean_triplets = []
            for index, (wavelength, flux) in enumerate(zip(wavelengths, fluxes)):
                if wavelength is None or flux is None:
                    continue
                flux_error = flux_errors[index] if index < len(flux_errors) else None
                clean_triplets.append((wavelength, flux, flux_error))
            if not clean_triplets:
                continue

            clean_wavelengths = [item[0] for item in clean_triplets]
            clean_fluxes = [item[1] for item in clean_triplets]
            clean_flux_errors = [item[2] for item in clean_triplets]
            spectrum = Spectrum1D(
                flux=clean_fluxes * u.Unit(row.get('flux_units') or 'W / (nm m2)'),
                spectral_axis=clean_wavelengths * u.Unit(row.get('wavelength_units') or 'nm'),
            )
            serialized = serializer.serialize(spectrum)
            serialized.update({
                'filter': 'GaiaDR3(XP)',
                'flux_error': clean_flux_errors,
                'flux_error_units': row.get('flux_error_units') or row.get('flux_units') or 'W / (nm m2)',
                'source_id': str(source_id),
                'spectrum_type': 'xp_sampled_mean_spectrum',
            })
            output.append({
                'timestamp': GAIA_DR3_RELEASE_TIMESTAMP,
                'value': serialized,
            })
        return output
