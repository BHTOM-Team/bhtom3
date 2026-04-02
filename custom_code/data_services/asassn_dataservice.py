import logging
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from django.db import IntegrityError, transaction

from pyasassn.client import SkyPatrolClient

import astropy.units as u
from astropy.coordinates import SkyCoord
from astropy.time import Time
from datetime import timezone
import numpy as np

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import ASASSNQueryForm


logger = logging.getLogger(__name__)

ASASSN_QUERY_URL = 'http://asas-sn.ifa.hawaii.edu/skypatrol'


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _asassn_alias(id):
    return f'ASASSN_{id}'


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
        self.query_parameters = {
            'ra': parameters.get('ra'),
            'dec': parameters.get('dec'),
            'radius_arcsec': parameters.get('radius_arcsec') or 5.0,
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or 5.0
        if ra is None or dec is None:
            self.query_results = {'lc_limits': None, 'lc_filtered': [], 'source_location': None}
            return self.query_results

        asassn_id = None
        lc_limits = None
        lc_filtered = None
        source_location = None
        try:
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
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        ra = data.get('ra')
        dec = data.get('dec')
        lc_limits = data.get('lc_limits')
        lc_filtered = data.get('lc_filtered')
        if ra is None or dec is None or (len(lc_filtered)+len(lc_limits)) < 1:
            return []

        alias = _asassn_alias(data.get('asassn_id'))
        return [{
            'name': alias,
            'ra': ra,
            'dec': dec,
            'aliases': [alias],
            'reduced_datums': {'photometry': self._build_photometry_datums(lc_filtered,lc_limits)},
            'source_location': data.get('source_location'),
        }]

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
