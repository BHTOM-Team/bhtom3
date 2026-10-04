"""WiggleZ Dark Energy Survey galaxy spectra from AAO Data Central.

WiggleZ (Drinkwater et al. 2010, MNRAS 401, 1429; final release Drinkwater et al. 2018, MNRAS
474, 4151) took ~225,000 spectra of UV-selected emission-line galaxies at 0.2 < z < 1.0 with
AAOmega on the AAT (2006-2011), ~3700-8900 A at ~1 A per pixel. Like 2dFGRS, the final release
is served by AAO Data Central: the SSA service returns the spectra within a cone and each
spectrum's DataLink points to a FITS file. The redshift quality Q (1-5; Q >= 3 reliable) comes
from the WiggleZ catalogue.

All spectra of the nearest WiggleZ galaxy are kept at full resolution (no smoothing or binning);
only non-finite and zero-padded pixels are dropped. Fluxes are converted from the file's
1e-16 erg/s/cm2/A to erg/s/cm2/A, but WiggleZ spectra are only relatively calibrated: their shape and lines
are reliable, their absolute level is not.
"""

import logging
import re
from datetime import timezone

import astropy.units as u
import numpy as np
import requests
from astropy.time import Time
from specutils import Spectrum1D

from tom_dataproducts.models import ReducedDatum
from tom_dataproducts.processors.data_serializers import SpectrumSerializer
from tom_dataservices.dataservices import DataService
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import WiggleZQueryForm
from custom_code.data_services.gs2df_dataservice import (
    _fetch_spectrum,
    _search_spectra,
    _text,
    _to_float,
    _votable_rows,
)
from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT, resolve_query_coordinates

logger = logging.getLogger(__name__)

WIGGLEZ_PAGE_URL = 'https://datacentral.org.au/services/ssa/'
WIGGLEZ_TAP_URL = 'https://datacentral.org.au/vo/tap/sync'
WIGGLEZ_COLLECTION = 'wigglez_final'
WIGGLEZ_DEFAULT_RADIUS_ARCSEC = 5.0

WIGGLEZ_ACKNOWLEDGEMENT = (
    'This research uses spectra from the WiggleZ Dark Energy Survey (Drinkwater et al. 2010, '
    'MNRAS 401, 1429; Drinkwater et al. 2018, MNRAS 474, 4151), obtained from AAO Data Central.'
)


def _wigglez_alias(name):
    return f'WiggleZ_{name}'


def _redshift_quality(name):
    """Redshift quality Q (1-5) from the WiggleZ catalogue, or None."""
    safe_name = name.replace("'", "''")
    response = requests.post(WIGGLEZ_TAP_URL, data={
        'REQUEST': 'doQuery',
        'LANG': 'ADQL',
        'QUERY': f"SELECT Q FROM wigglez_final.WiggleZCat WHERE WiggleZ_Name = '{safe_name}'",
    }, timeout=DATA_SERVICE_HTTP_TIMEOUT)
    response.raise_for_status()
    rows = _votable_rows(response.content)
    quality = _to_float(rows[0].get('Q')) if rows else None
    return int(quality) if quality is not None else None


FLAMBDA = u.erg / (u.s * u.cm ** 2 * u.AA)


def _flux_scale(bunit):
    """Factor converting the file's flux values to erg/s/cm2/A. BUNIT is written as
    '1e-16 erg / (A cm2 s)', where astropy would read 'A' as ampere, so it is read as Angstrom."""
    try:
        return u.Unit(re.sub(r'\bA\b', 'Angstrom', bunit)).to(FLAMBDA)
    except (TypeError, ValueError, u.UnitsError):
        return 1e-16


class WiggleZDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return WIGGLEZ_DEFAULT_RADIUS_ARCSEC

    name = 'WiggleZ'
    verbose_name = 'WiggleZ'
    update_on_daily_refresh = False
    info_url = WIGGLEZ_PAGE_URL
    acknowledgement = WIGGLEZ_ACKNOWLEDGEMENT
    service_notes = (
        'Query WiggleZ Dark Energy Survey galaxy spectra by coordinates from AAO Data Central (SSA). '
        'All spectra of the nearest WiggleZ galaxy within 5 arcsec are imported at full resolution, '
        'with the redshift and its quality, and the WiggleZ name is added as an alias. Spectra are '
        'only relatively flux calibrated.'
    )

    @classmethod
    def get_form_class(cls):
        return WiggleZQueryForm

    @classmethod
    def get_acknowledgement(cls):
        return cls.acknowledgement

    def build_query_parameters(self, parameters, **kwargs):
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or WIGGLEZ_DEFAULT_RADIUS_ARCSEC,
            'include_spectroscopy': bool(parameters.get('include_spectroscopy', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or WIGGLEZ_DEFAULT_RADIUS_ARCSEC

        name, quality, spectra = None, None, []
        if ra is not None and dec is not None and query_parameters.get('include_spectroscopy', True):
            try:
                name, rows = _search_spectra(ra, dec, radius_arcsec, collection=WIGGLEZ_COLLECTION)
                for row in rows:
                    try:
                        spectrum = _fetch_spectrum(_text(row['access_url']))
                    except Exception as exc:
                        logger.warning('WiggleZ: could not read a spectrum of %s: %s', name, exc)
                        continue
                    if spectrum is not None and spectrum[0].size:
                        spectra.append((row, spectrum))
                if name:
                    try:
                        quality = _redshift_quality(name)
                    except Exception as exc:
                        logger.debug('WiggleZ: no redshift quality for %s: %s', name, exc)
                else:
                    logger.debug('WiggleZ returned no spectrum for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('WiggleZ query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                name, spectra = None, []

        self.query_results = {
            'name': name,
            'redshift_quality': quality,
            'spectra': spectra,
            'source_location': WIGGLEZ_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        name = data.get('name')
        if data.get('ra') is None or data.get('dec') is None or not name:
            return []
        datums = self._build_spectroscopy_datums(data.get('spectra') or [], name, data.get('redshift_quality'))
        if not datums:
            return []
        alias = _wigglez_alias(name)
        return [{
            'name': alias,
            'ra': data['ra'],
            'dec': data['dec'],
            'aliases': [alias],
            'reduced_datums': {'spectroscopy': datums},
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
        if data_type != 'spectroscopy' or not data:
            return
        source_location = kwargs.get('source_location') or self.info_url
        for datum in data:
            ReducedDatum.objects.get_or_create(
                target=target,
                data_type='spectroscopy',
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
            self.create_reduced_datums_from_query(
                target,
                data=data,
                data_type=data_type,
                source_location=(getattr(self, 'query_results', {}) or {}).get('source_location') or self.info_url,
            )

    def _build_spectroscopy_datums(self, spectra, name, quality):
        output = []
        serializer = SpectrumSerializer()
        for row, (wavelength, flux, bunit) in spectra:
            mjd = _to_float(row.get('t_midpoint'))
            if mjd is None:
                continue
            # Zero-valued pixels are padding at the ends of the wavelength range, not data.
            keep = flux != 0
            if keep.sum() < 10:
                continue
            serialized = serializer.serialize(Spectrum1D(
                flux=flux[keep] * _flux_scale(bunit) * FLAMBDA,
                spectral_axis=wavelength[keep] * u.AA,
            ))
            serialized.update({
                'filter': 'WiggleZ',
                'source_id': name,
                'spectrum_type': 'WiggleZ_spectrum',
                'flux_calibration': 'relative (absolute level unreliable)',
                'redshift': _to_float(row.get('redshift')),
                'redshift_quality': quality,
                'snr': _to_float(row.get('em_snr')),
                'match_separation_arcsec': _to_float(row.get('score')),
            })
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': serialized,
            })
        return output
