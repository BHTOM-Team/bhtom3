import logging
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import re
from django.core.cache import cache
from django.db import IntegrityError, transaction

try:
    from pyasassn.client import SkyPatrolClient
except ImportError:
    SkyPatrolClient = None

import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time
from datetime import timezone
import numpy as np
import pandas as pd
import requests

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import ASASSNQueryForm
from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT


logger = logging.getLogger(__name__)

ASASSN_QUERY_URL = 'http://asas-sn.ifa.hawaii.edu/skypatrol'
ASASSN_TRANSIENTS_URL = 'https://www.astronomy.ohio-state.edu/asassn/transients.html'
ASASSN_TRANSIENTS_CACHE_KEY = 'asassn_transient_rows'
ASASSN_TRANSIENTS_CACHE_TIMEOUT = 3600


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _asassn_alias(id):
    return f'ASASSN_{id}'


def _clean_text(value):
    value = str(value or '').strip()
    if value.lower() in ('nan', 'none', '---', '-------'):
        return ''
    return re.sub(r'\s+', ' ', value)


def _normalize_transient_name(value):
    value = str(value or '').strip().lower()
    value = value.replace('\u2010', '-').replace('\u2011', '-').replace('\u2012', '-')
    value = value.replace('\u2013', '-').replace('\u2014', '-').replace('\u2212', '-')
    compact = re.sub(r'[^a-z0-9]', '', value)
    if compact.startswith('asassn'):
        compact = compact[6:]
    if re.match(r'^20\d{2}[a-z]+$', compact):
        compact = compact[2:]
    return compact


def _normalize_generic_name(value):
    return re.sub(r'[^a-z0-9]', '', str(value or '').strip().lower())


def _canonical_transient_name(value):
    suffix = _normalize_transient_name(value)
    if not suffix:
        return ''
    return f'ASASSN-{suffix[:2]}{suffix[2:]}'


def _split_other_ids(value):
    return [
        _clean_text(alias)
        for alias in re.split(r'[(),;]\s*|\s{2,}', str(value or ''))
        if _clean_text(alias)
    ]


def _flatten_column_name(column):
    if isinstance(column, tuple):
        parts = [_clean_text(part) for part in column if _clean_text(part)]
        return ' '.join(parts)
    return _clean_text(column)


def _parse_transient_rows(html):
    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return []
    if not tables:
        return []

    table = tables[0]
    table.columns = [_flatten_column_name(column) for column in table.columns]
    rows = []
    for _, row in table.iterrows():
        asassn_name = _clean_text(row.get('ASAS-SN ID') or row.get('ASAS-SN') or row.get('ASASSN'))
        other_ids = _clean_text(row.get('Other IDs') or row.get('Other'))
        aliases = [name for name in (_canonical_transient_name(asassn_name), *_split_other_ids(other_ids)) if name]
        ra_text = _clean_text(row.get('RA'))
        dec_text = _clean_text(row.get('Dec'))
        if not asassn_name and not other_ids:
            continue
        try:
            coord = SkyCoord(ra_text, dec_text, unit=(u.hourangle, u.deg))
        except Exception:
            continue

        rows.append({
            'name': aliases[0] if aliases else '',
            'asassn_name': asassn_name,
            'other_ids': other_ids,
            'aliases': aliases,
            'ra': coord.ra.deg,
            'dec': coord.dec.deg,
            'discovery': _clean_text(row.get('Discovery (UT)') or row.get('Discovery')),
            'magnitude': _clean_text(row.get('V/g (mag)') or row.get('V/g')),
            'spectroscopic_class': _clean_text(row.get('Spectroscopic Class')),
            'comments': _clean_text(row.get('Comments')),
            'source_location': ASASSN_TRANSIENTS_URL,
        })
    return rows


def _fetch_transient_rows():
    cached_rows = cache.get(ASASSN_TRANSIENTS_CACHE_KEY)
    if cached_rows is not None:
        return cached_rows
    response = requests.get(ASASSN_TRANSIENTS_URL, timeout=DATA_SERVICE_HTTP_TIMEOUT)
    response.raise_for_status()
    rows = _parse_transient_rows(response.text)
    cache.set(ASASSN_TRANSIENTS_CACHE_KEY, rows, ASASSN_TRANSIENTS_CACHE_TIMEOUT)
    return rows


def _candidate_target_names(query_parameters):
    names = []
    for value in query_parameters.get('target_names') or []:
        value = str(value or '').strip()
        if value:
            names.append(value)
    target_name = str(query_parameters.get('target_name') or '').strip()
    if target_name:
        names.append(target_name)
    return list(dict.fromkeys(names))


def _find_transient_by_name(rows, target_name):
    term = str(target_name or '').strip()
    normalized_term = _normalize_transient_name(term)
    generic_term = _normalize_generic_name(term)
    if not normalized_term and not generic_term:
        return None

    for row in rows:
        if normalized_term and _normalize_transient_name(row.get('asassn_name')) == normalized_term:
            return row
        if normalized_term and _normalize_transient_name(row.get('name')) == normalized_term:
            return row

    if generic_term:
        for row in rows:
            for alias in row.get('aliases') or _split_other_ids(row.get('other_ids')):
                if _normalize_generic_name(alias) == generic_term:
                    return row
    return None


class ASASSNDataService(DataService):
    name = 'ASASSN'
    verbose_name = 'ASASSN'
    update_on_daily_refresh = True
    info_url = ASASSN_QUERY_URL
    service_notes = 'Query ASASSN by coordinates and ingest ASASSN photometry.'

    @classmethod
    def get_form_class(cls):
        return ASASSNQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or 5.0,
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        target_names = _candidate_target_names(query_parameters)
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or 5.0
        transient = None
        transient_source_location = None
        if target_names:
            try:
                transient_rows = _fetch_transient_rows()
                for target_name in target_names:
                    transient = _find_transient_by_name(transient_rows, target_name)
                    if transient:
                        break
            except Exception as exc:
                logger.warning('ASAS-SN transient lookup failed for "%s": %s', ', '.join(target_names), exc)
                transient = None
            if transient:
                ra = transient.get('ra') if ra is None else ra
                dec = transient.get('dec') if dec is None else dec
                transient_source_location = transient.get('source_location') or ASASSN_TRANSIENTS_URL

        if ra is None or dec is None:
            self.query_results = {
                'lc_limits': None,
                'lc_filtered': [],
                'source_location': None,
                'transient': None,
            }
            return self.query_results

        asassn_id = None
        lc_limits = None
        lc_filtered = None
        source_location = transient_source_location
        try:
            if SkyPatrolClient is None:
                logger.warning('ASAS-SN Sky Patrol client is unavailable; install pyasassn/skypatrol to ingest photometry.')
                raise ValueError
            with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
                client = SkyPatrolClient()
                query = client.cone_search(
                    ra_deg=ra,
                    dec_deg=dec,
                    radius=radius_arcsec / 3600.0,
                    catalog='master_list',
                )
            if query.empty:
                logger.debug('ASASSN returned no spectrum for RA=%s Dec=%s', ra, dec)
            else:
                t = SkyCoord(ra=ra, dec=dec, unit='deg')
                separations = t.separation(SkyCoord(ra=query['ra_deg']*u.degree, dec=query['dec_deg']*u.degree, unit=(u.deg, u.deg)))
                min_index = np.argmin(separations)
                asassn_id = query.iloc[min_index]['asas_sn_id']
                source_location = f"http://asas-sn.ifa.hawaii.edu/skypatrol/objects/{asassn_id}"
                if query_parameters.get('include_photometry', True):
                    with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
                        lcs = client.cone_search(
                            ra_deg=ra,
                            dec_deg=dec,
                            radius=radius_arcsec / 3600.0,
                            download=True,
                            threads=8,
                        )
                    lc = lcs[asassn_id]
                    lc_filtered = lc.data[(lc.data['mag_err'] < 0.5) & (lc.data['mag'] <= 99) & (lc.data['mag_err'] > 0)]
                    lc_limits = lc.data[(lc.data['mag_err'] < 0) & (lc.data['mag'] <= 99)]
        except ValueError:
            logger.debug('ASASSN returned error for RA=%s Dec=%s', ra, dec)

        self.query_results = {
            'asassn_id':asassn_id,
            'lc_filtered': lc_filtered,
            'lc_limits': lc_limits,
            'source_location': source_location,
            'ra': ra,
            'dec': dec,
            'transient': transient,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        ra = data.get('ra')
        dec = data.get('dec')
        lc_limits = data.get('lc_limits')
        lc_filtered = data.get('lc_filtered')
        filtered_count = len(lc_filtered) if lc_filtered is not None else 0
        limits_count = len(lc_limits) if lc_limits is not None else 0
        transient = data.get('transient')
        if ra is None or dec is None or ((filtered_count + limits_count) < 1 and not transient):
            return []

        alias = _asassn_alias(data.get('asassn_id')) if data.get('asassn_id') else None
        transient_name = transient.get('name') if transient else None
        aliases = list(transient.get('aliases') or [transient_name]) if transient else []
        if alias:
            aliases.append(alias)
        aliases = list(dict.fromkeys(name for name in aliases if name))
        name = transient_name or alias
        return [{
            'name': name,
            'ra': ra,
            'dec': dec,
            'aliases': aliases,
            'reduced_datums': {'photometry': self._build_photometry_datums(lc_filtered,lc_limits)},
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
        return [TargetName(name=alias) for alias in alias_results]

        
    def create_reduced_datums_from_query(self, target, data=None, data_type=None, **kwargs):
        if data_type != 'photometry' or not data:
            return
        source_location = kwargs.get('source_location') or self.info_url

        for datum in data:
            try:
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
            except IntegrityError:
                # Another process inserted it concurrently; retry with get
                try:
                    ReducedDatum.objects.get(
                        target=target,
                        data_type='photometry',
                        timestamp=datum['timestamp'],
                        value=datum['value'],
                    )
                except ReducedDatum.DoesNotExist:
                    # Rare case: still doesn't exist, retry in a transaction
                    with transaction.atomic():
                        ReducedDatum.objects.create(
                            target=target,
                            data_type='photometry',
                            timestamp=datum['timestamp'],
                            value=datum['value'],
                            source_name=self.name,
                            source_location=source_location,
                        )

    def to_reduced_datums(self, target, data_results=None, **kwargs):
        if not data_results:
            return
        for data_type, data in data_results.items():
            self.create_reduced_datums_from_query(
                target,
                data=data,
                data_type=data_type,
                source_location=self.query_results.get('source_location') or self.info_url,
            )

    def _build_photometry_datums(self, lc_filtered,lc_limits):
        output = []
        if lc_filtered is not None:
            for _, datum in lc_filtered.iterrows():
                try:
                    mjd = _to_float(datum.jd - 2400000.5)
                    mag = _to_float(datum.mag)
                    magerr = _to_float(datum.mag_err)
                    filter = "ASASSN(" + datum.phot_filter + ")"
                    if mjd is None or mag is None or magerr is None:
                        continue
                    output.append({
                        'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                        'value': {'filter': filter, 'magnitude': mag, 'error': magerr},
                    })
                except TypeError:
                    continue

        if lc_limits is not None:
            for _, datum in lc_limits.iterrows():
                try:
                    mjd = _to_float(datum.jd - 2400000.5)
                    mag = _to_float(datum.limit)
                    magerr = _to_float(-1.0)
                    filter = "ASASSN(" + datum.phot_filter + ")"
                    if mjd is None or mag is None or magerr is None:
                        continue
                    output.append({
                        'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                        'value': {'filter': filter, 'magnitude': mag, 'error': magerr},
                    })
                except TypeError:
                    continue

        return output
