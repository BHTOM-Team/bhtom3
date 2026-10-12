"""RAVE DR6 spectra (Ca II triplet region, 8410-8795 A) from the RAVE TAP service.

RAVE, the RAdial Velocity Experiment (Steinmetz et al. 2020, AJ 160, 82 and 83), observed about
450,000 bright southern stars (9 < I < 12) with the 6dF multi-fibre spectrograph on the UK
Schmidt Telescope (2003-2013), at R ~ 7500 around the Ca II triplet; many stars have repeat
visits. The RAVE TAP service is queried by position: each observation of a star within the
radius is returned with its spectrum file and the SPARV heliocentric radial velocity. Derived
stellar parameters (Teff, log g, [M/H]) are deliberately not stored.

The spectra are continuum-normalised by the RAVE pipeline (no flux scale); they are stored as
normalised flux with their error spectrum and plotted on the normalised-flux axis. Each spectrum
also records its 6dF fibre (6.7" across on the sky) for the finding chart.
"""

import io
import logging
import math
from datetime import datetime, timedelta, timezone

import astropy.units as u
import numpy as np
import requests
from astropy.io import fits
from astropy.io.votable import parse_single_table
from specutils import Spectrum1D

from tom_dataproducts.processors.data_serializers import SpectrumSerializer
from tom_dataservices.dataservices import DataService
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import RAVEQueryForm
from custom_code.data_services.service_utils import (
    DATA_SERVICE_HTTP_TIMEOUT,
    resolve_query_coordinates,
    upsert_reduced_datums,
)

logger = logging.getLogger(__name__)

RAVE_PAGE_URL = 'https://www.rave-survey.org/'
RAVE_TAP_URL = 'https://www.rave-survey.org/tap/sync'
RAVE_DEFAULT_RADIUS_ARCSEC = 3.0
RAVE_TAP_TIMEOUT = 60
# 6dF fibres on the UK Schmidt Telescope subtend 6.7 arcsec.
RAVE_FIBER_DIAMETER_ARCSEC = 6.7

RAVE_ACKNOWLEDGEMENT = (
    'Funding for RAVE has been provided by: the Leibniz-Institut fuer Astrophysik Potsdam (AIP); '
    'the Australian Astronomical Observatory; the Australian National University; the Australian '
    'Research Council; the French National Research Agency (Programme National Cosmology et '
    'Galaxies (PNCG) of CNRS/INSU with INP and IN2P3, co-funded by CEA and CNES); the German '
    'Research Foundation (SPP 1177 and SFB 881); the European Research Council (ERC-StG 240271 '
    'Galactica); the Istituto Nazionale di Astrofisica at Padova; The Johns Hopkins University; '
    'the National Science Foundation of the USA (AST-0908326); the W. M. Keck foundation; the '
    'Macquarie University; the Netherlands Research School for Astronomy; the Natural Sciences and '
    'Engineering Research Council of Canada; the Slovenian Research Agency (research core funding '
    'No. P1-0188); the Swiss National Science Foundation; the Science & Technology Facilities '
    'Council of the UK; Opticon; Strasbourg Observatory; and the Universities of Basel, Groningen, '
    'Heidelberg and Sydney. This work uses RAVE DR6 (Steinmetz et al. 2020, AJ 160, 82; AJ 160, 83).'
)


def _to_float(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _text(value):
    if value is None or value is np.ma.masked:
        return ''
    if isinstance(value, bytes):
        value = value.decode('utf-8', 'replace')
    return str(value).strip()


def _tap_rows(query):
    response = requests.post(
        RAVE_TAP_URL, data={'REQUEST': 'doQuery', 'LANG': 'ADQL', 'QUERY': query}, timeout=RAVE_TAP_TIMEOUT,
    )
    response.raise_for_status()
    table = parse_single_table(io.BytesIO(response.content), verify='ignore').to_table(use_names_over_ids=True)
    return [{name: row[name] for name in table.colnames} for row in table]


def _search_observations(ra, dec, radius_arcsec):
    """All RAVE observations within the radius, with spectrum file and radial velocity."""
    return _tap_rows(f"""
        SELECT o.rave_obs_id, o.ra_input, o.dec_input, o.obsdate, o.ut_start, o.ut_end, o.exposure_time,
               o.fibernumber, s.spectrum_fits, p.raveid, p.hrv_sparv, p.hrv_error_sparv,
               p.correlation_coeff_sparv, p.snr_med_sparv,
               c.flag1_class,
               DISTANCE(POINT('ICRS', o.ra_input, o.dec_input), POINT('ICRS', {ra}, {dec})) * 3600 AS separation_arcsec
        FROM ravedr6.dr6_obsdata AS o
        JOIN ravedr6.dr6_spectra AS s ON s.rave_obs_id = o.rave_obs_id
        LEFT JOIN ravedr6.dr6_sparv AS p ON p.rave_obs_id = o.rave_obs_id
        LEFT JOIN ravedr6.dr6_classification AS c ON c.rave_obs_id = o.rave_obs_id
        WHERE 1 = CONTAINS(POINT('ICRS', o.ra_input, o.dec_input), CIRCLE('ICRS', {ra}, {dec}, {radius_arcsec / 3600.0}))
    """)


def _nearest_star(rows):
    """Observations of the star nearest the position (stars are identified by their RAVE ID)."""
    if not rows:
        return []
    nearest = min(rows, key=lambda row: _to_float(row.get('separation_arcsec')) or float('inf'))
    star = _text(nearest.get('raveid'))
    if not star:
        return [nearest]
    return [row for row in rows if _text(row.get('raveid')) == star]


def _mid_time(row):
    """Mid-exposure UTC time from obsdate (YYYYMMDD) and the UT start/end of the exposure."""
    date = datetime.strptime(str(int(row['obsdate'])), '%Y%m%d').replace(tzinfo=timezone.utc)

    def at(value):
        parts = _text(value).split(':')
        if len(parts) != 3:
            return None
        hours, minutes, seconds = int(parts[0]), int(parts[1]), float(parts[2])
        return date + timedelta(hours=hours, minutes=minutes, seconds=seconds)

    start, end = at(row.get('ut_start')), at(row.get('ut_end'))
    if start is None:
        return date
    if end is None:
        exposure = _to_float(row.get('exposure_time')) or 0.0
        return start + timedelta(seconds=exposure / 2.0)
    if end < start:  # exposure crossed 0h UT
        end += timedelta(days=1)
    return start + (end - start) / 2


def _fetch_spectrum(url):
    """(wavelength [A], normalised flux, error) from a RAVE spectrum FITS file."""
    response = requests.get(url, timeout=DATA_SERVICE_HTTP_TIMEOUT)
    response.raise_for_status()
    with fits.open(io.BytesIO(response.content)) as hdul:
        header = hdul['SPECTRUM'].header
        flux = np.asarray(hdul['SPECTRUM'].data, dtype=float)
        error = np.asarray(hdul['ERROR'].data, dtype=float)
    pixels = np.arange(1, flux.size + 1)
    wavelength = header['CRVAL1'] + (pixels - header.get('CRPIX1', 1)) * header['CDELT1']
    return wavelength, flux, error


def _rounded(value, digits):
    return round(value, digits) if value is not None else None


class RAVEDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return RAVE_DEFAULT_RADIUS_ARCSEC

    name = 'RAVE'
    verbose_name = 'RAVE DR6'
    # DR6 is the final release.
    update_on_daily_refresh = False
    info_url = RAVE_PAGE_URL
    acknowledgement = RAVE_ACKNOWLEDGEMENT
    service_notes = (
        'Query RAVE DR6 (6dF/UKST, 2003-2013, bright southern stars) by coordinates. Every '
        'observation of the nearest RAVE star within 3 arcsec is imported: the continuum-normalised '
        'Ca II triplet spectrum (8410-8795 A, R~7500) with its error and the heliocentric radial '
        'velocity; the RAVE ID is added as an alias.'
    )

    @classmethod
    def get_form_class(cls):
        return RAVEQueryForm

    @classmethod
    def get_acknowledgement(cls):
        return cls.acknowledgement

    def build_query_parameters(self, parameters, **kwargs):
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or RAVE_DEFAULT_RADIUS_ARCSEC,
            'include_spectroscopy': bool(parameters.get('include_spectroscopy', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or RAVE_DEFAULT_RADIUS_ARCSEC

        observations, spectra = [], []
        if ra is not None and dec is not None and query_parameters.get('include_spectroscopy', True):
            try:
                observations = _nearest_star(_search_observations(ra, dec, radius_arcsec))
                for row in observations:
                    url = _text(row.get('spectrum_fits'))
                    if not url:
                        continue
                    try:
                        spectra.append((row, _fetch_spectrum(url)))
                    except Exception as exc:
                        logger.warning('RAVE: could not read %s: %s', url, exc)
                if not observations:
                    logger.debug('RAVE returned no observation for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('RAVE query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                observations, spectra = [], []

        self.query_results = {
            'raveid': _text(observations[0].get('raveid')) if observations else None,
            'spectra': spectra,
            'source_location': RAVE_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        if data.get('ra') is None or data.get('dec') is None or not data.get('spectra'):
            return []
        datums = self._build_spectroscopy_datums(data['spectra'])
        if not datums:
            return []
        raveid = data.get('raveid')
        aliases = [f'RAVE {raveid}'] if raveid else []
        return [{
            'name': f'RAVE_{raveid}' if raveid else None,
            'ra': data['ra'],
            'dec': data['dec'],
            'aliases': aliases,
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
        upsert_reduced_datums(
            target=target, data_type='spectroscopy', source_name=self.name,
            source_location=kwargs.get('source_location') or self.info_url,
            datums=data, identity_keys=('spectrum_type', 'rave_obs_id'),
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

    def _build_spectroscopy_datums(self, spectra):
        output = []
        serializer = SpectrumSerializer()
        for row, (wavelength, flux, error) in spectra:
            good = np.isfinite(wavelength) & np.isfinite(flux) & (flux > 0)
            if good.sum() < 10:
                continue
            serialized = serializer.serialize(Spectrum1D(
                flux=flux[good] * u.dimensionless_unscaled,
                spectral_axis=wavelength[good] * u.AA,
            ))
            serialized.update({
                'flux_error': [round(float(value), 5) for value in error[good]],
                'flux_normalisation': 'continuum-normalised (RAVE pipeline)',
                'filter': 'RAVE',
                'source_id': _text(row.get('raveid')),
                'rave_obs_id': _text(row.get('rave_obs_id')),
                'spectrum_type': 'RAVE_spectrum',
                'hrv_kms': _rounded(_to_float(row.get('hrv_sparv')), 2),
                'hrv_error_kms': _rounded(_to_float(row.get('hrv_error_sparv')), 2),
                'snr': _rounded(_to_float(row.get('snr_med_sparv')), 1),
                'classification_flag': _text(row.get('flag1_class')) or None,
                'exposure_time_s': _to_float(row.get('exposure_time')),
                'match_separation_arcsec': _rounded(_to_float(row.get('separation_arcsec')), 3),
                'instrument': 'RAVE (6dF)',
                'fiber_ra': _to_float(row.get('ra_input')),
                'fiber_dec': _to_float(row.get('dec_input')),
                'fiber_diameter_arcsec': RAVE_FIBER_DIAMETER_ARCSEC,
                'fiberid': int(row['fibernumber']) if _to_float(row.get('fibernumber')) is not None else None,
                'data_release': 'RAVE DR6',
            })
            output.append({'timestamp': _mid_time(row), 'value': serialized})
        return output
