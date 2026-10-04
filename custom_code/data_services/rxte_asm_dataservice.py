"""RXTE All-Sky Monitor (ASM) 1.5-12 keV X-ray light curves, 1996-2011, from HEASARC.

The ASM (Levine et al. 1996, ApJ 469, L33) scanned the sky in ~90 s dwells for the whole RXTE
mission. HEASARC keeps the definitive single-dwell light curves of the ~590 sources the ASM
monitored (xteasmlong table; one FITS file per source, xa_<name>_d1.lc, ~10-20 MB). The target
is matched to the nearest ASM source within the radius, its file is downloaded, and the dwells
(one rate per dwell and camera) are averaged per day with inverse-variance weights, as in MIT's
1-day light curves.

Rates (ASM counts/s, 1.5-12 keV) are converted to energy flux with the Crab: the Crab gives
~75 ASM counts/s (median 75.5 over the mission), and a Crab-like spectrum (photon index 2.1,
normalisation 9.7 ph/cm2/s/keV at 1 keV) has 2.8e-8 erg/cm2/s in 1.5-12 keV, so
1 count/s ~ 3.7e-10 erg/cm2/s. This is exact only for Crab-like spectra. Days below
ASM_MIN_SNR become ASM_MIN_SNR-sigma upper limits (error -1), as for the other X-ray services.
"""

import io
import logging
import math
import re
from datetime import timezone

import numpy as np
import requests
from astropy.io import fits
from astropy.io.votable import parse_single_table
from astropy.time import Time

from tom_dataservices.dataservices import DataService
from tom_targets.models import Target

from custom_code.data_services.forms import RXTEASMQueryForm
from custom_code.data_services.service_utils import (
    DATA_SERVICE_HTTP_TIMEOUT,
    resolve_query_coordinates,
    upsert_reduced_datums,
)

logger = logging.getLogger(__name__)

ASM_PAGE_URL = 'https://heasarc.gsfc.nasa.gov/docs/xte/asm_products.html'
ASM_TAP_URL = 'https://heasarc.gsfc.nasa.gov/xamin/vo/tap/sync'
ASM_LC_DIR = 'https://heasarc.gsfc.nasa.gov/FTP/xte/data/archive/ASMProducts/definitive_1dwell/lightcurves/'
ASM_FILTER = 'ASM(1.5-12keV)'
ASM_FLUX_UNIT = 'erg/cm2/s'
# xteasmlong positions are given to 0.01 deg; ASM sources are bright and sparse.
ASM_DEFAULT_RADIUS_ARCSEC = 60.0
ASM_MIN_SNR = 3.0
ASM_CRAB_RATE = 75.0  # ASM counts/s for the Crab (1.5-12 keV)
_CRAB_NORM, _CRAB_INDEX, _KEV_TO_ERG = 9.7, 2.1, 1.602177e-9
# Energy flux of a Crab-like power law between 1.5 and 12 keV, in erg/cm2/s (~2.8e-8).
ASM_CRAB_FLUX = _CRAB_NORM * (12.0 ** (2 - _CRAB_INDEX) - 1.5 ** (2 - _CRAB_INDEX)) / (2 - _CRAB_INDEX) * _KEV_TO_ERG
ASM_COUNTS_TO_FLUX = ASM_CRAB_FLUX / ASM_CRAB_RATE
ASM_HTTP_TIMEOUT = (DATA_SERVICE_HTTP_TIMEOUT[0], max(DATA_SERVICE_HTTP_TIMEOUT[1], 300))

ASM_ACKNOWLEDGEMENT = (
    'This research uses results provided by the ASM/RXTE teams at MIT and at the RXTE SOF and GOF '
    'at NASA\'s GSFC, obtained from the High Energy Astrophysics Science Archive Research Center '
    '(HEASARC).'
)


def _to_float(value):
    if value is None or value is np.ma.masked:
        return None
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def _angular_separation_arcsec(ra1, dec1, ra2, dec2):
    ra1, dec1, ra2, dec2 = map(math.radians, (ra1, dec1, ra2, dec2))
    cos_sep = (math.sin(dec1) * math.sin(dec2)
               + math.cos(dec1) * math.cos(dec2) * math.cos(ra1 - ra2))
    return math.degrees(math.acos(min(1.0, max(-1.0, cos_sep)))) * 3600.0


def _nearest_asm_source(ra, dec, radius_arcsec):
    response = requests.post(ASM_TAP_URL, data={
        'REQUEST': 'doQuery',
        'LANG': 'ADQL',
        'QUERY': (
            "SELECT source, ra, dec FROM xteasmlong WHERE CONTAINS(POINT('ICRS', ra, dec), "
            f"CIRCLE('ICRS', {ra}, {dec}, {radius_arcsec / 3600.0})) = 1"
        ),
    }, timeout=DATA_SERVICE_HTTP_TIMEOUT)
    response.raise_for_status()
    table = parse_single_table(io.BytesIO(response.content), verify='ignore').to_table(use_names_over_ids=True)
    candidates = []
    for row in table:
        src_ra, src_dec = _to_float(row['ra']), _to_float(row['dec'])
        if src_ra is None or src_dec is None:
            continue
        candidates.append((_angular_separation_arcsec(ra, dec, src_ra, src_dec), str(row['source']).strip()))
    if not candidates:
        return None
    separation, name = min(candidates)
    return {'name': name, 'separation_arcsec': separation}


def _light_curve_url(source_name):
    """File names are the source names with spaces removed but not always lower case."""
    listing = requests.get(ASM_LC_DIR, timeout=DATA_SERVICE_HTTP_TIMEOUT)
    listing.raise_for_status()
    wanted = source_name.replace(' ', '').lower()
    for file_name in re.findall(r'href="(xa_([^"]+)_d1\.lc)"', listing.text):
        if file_name[1].lower() == wanted:
            return ASM_LC_DIR + file_name[0]
    return None


def _daily_averages(content):
    """[(mjd, rate, error, n_dwells)] per day from a single-dwell ASM light curve."""
    with fits.open(io.BytesIO(content)) as hdul:
        header, data = hdul[1].header, hdul[1].data
        mjd_ref = float(header.get('MJDREFI', 0)) + float(header.get('MJDREFF', 0.0))
        mjd = np.asarray(data['TIME'], dtype=float) + mjd_ref
        rate = np.asarray(data['RATE'], dtype=float)
        error = np.asarray(data['ERROR'], dtype=float)
    good = np.isfinite(mjd) & np.isfinite(rate) & np.isfinite(error) & (error > 0)
    mjd, rate, error = mjd[good], rate[good], error[good]
    if not mjd.size:
        return []
    day = np.floor(mjd).astype(np.int64)
    weight = 1.0 / error ** 2
    days, index, counts = np.unique(day, return_inverse=True, return_counts=True)
    sum_w = np.bincount(index, weights=weight)
    mean_rate = np.bincount(index, weights=weight * rate) / sum_w
    mean_mjd = np.bincount(index, weights=mjd) / counts
    return list(zip(mean_mjd, mean_rate, 1.0 / np.sqrt(sum_w), counts))


class RXTEASMDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return ASM_DEFAULT_RADIUS_ARCSEC

    name = 'RXTEASM'
    verbose_name = 'RXTE/ASM (1.5-12 keV)'
    # RXTE ended in January 2012; the archive is final.
    update_on_daily_refresh = False
    info_url = ASM_PAGE_URL
    acknowledgement = ASM_ACKNOWLEDGEMENT
    # One point (detection or upper limit) per day.
    upsert_identity_keys = ('filter', 'upper_limit')
    service_notes = (
        'Query the RXTE All-Sky Monitor light curves (1.5-12 keV, 1996-2011) at HEASARC by '
        'coordinates. The nearest of the ~590 ASM sources within 60 arcsec is used; its dwells are '
        'averaged per day and converted to erg/cm2/s with the Crab (1 count/s ~ 3.7e-10 erg/cm2/s), '
        'and days below 3 sigma become upper limits. Plotted on the high-energy plot.'
    )

    @classmethod
    def get_form_class(cls):
        return RXTEASMQueryForm

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

        match, days, source_location = None, [], ASM_PAGE_URL
        if ra is not None and dec is not None:
            try:
                match = _nearest_asm_source(ra, dec, radius_arcsec)
                url = _light_curve_url(match['name']) if match else None
                if url:
                    response = requests.get(url, timeout=ASM_HTTP_TIMEOUT)
                    response.raise_for_status()
                    days = _daily_averages(response.content)
                    source_location = url
                elif match:
                    logger.info('RXTE/ASM: no light-curve file for %s', match['name'])
                    match = None
            except Exception as exc:
                logger.warning('RXTE/ASM query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                match, days = None, []

        self.query_results = {
            'match': match,
            'days': days,
            'source_location': source_location,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        match = data.get('match')
        if data.get('ra') is None or data.get('dec') is None or not match:
            return []
        datums = self._build_highenergy_datums(data.get('days') or [], match)
        if not datums:
            return []
        return [{
            'name': f"ASM_{match['name'].replace(' ', '')}",
            'ra': data['ra'],
            'dec': data['dec'],
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

    def _build_highenergy_datums(self, days, match):
        output = []
        for mjd, rate, error, n_dwells in days:
            value = {
                'filter': ASM_FILTER,
                'flux_unit': ASM_FLUX_UNIT,
                'facility': 'RXTE/ASM',
                'energy_band': '1.5-12 keV',
                'rate': round(float(rate), 4),
                'rate_error': round(float(error), 4),
                'n_dwells': int(n_dwells),
                'asm_source': match['name'],
                'match_separation_arcsec': round(match['separation_arcsec'], 1),
            }
            if rate >= ASM_MIN_SNR * error:
                value.update({
                    'flux': float(rate) * ASM_COUNTS_TO_FLUX,
                    'error': float(error) * ASM_COUNTS_TO_FLUX,
                    'upper_limit': False,
                })
            else:
                value.update({
                    'flux': (max(float(rate), 0.0) + ASM_MIN_SNR * float(error)) * ASM_COUNTS_TO_FLUX,
                    'error': -1.0,
                    'upper_limit': True,
                })
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='tt').utc.to_datetime(timezone=timezone.utc),
                'value': value,
            })
        return output
