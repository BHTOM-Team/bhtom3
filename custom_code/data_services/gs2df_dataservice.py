"""2dF Galaxy Redshift Survey (2dFGRS) spectra from AAO Data Central.

2dFGRS (Colless et al. 2001, MNRAS 328, 1039; final release Colless et al. 2003,
arXiv:astro-ph/0306581) took ~245,000 optical spectra (~3600-8000 A, 2dF on the AAT, 1997-2002)
of bJ < 19.45 galaxies in two strips near the Galactic poles plus random fields. The final data
release is served by AAO Data Central: its SSA service returns the spectra within a cone (with
separation, mid-exposure MJD, redshift and S/N), and each spectrum's DataLink points to a
1024-pixel FITS file on a linear wavelength scale.

All spectra of the nearest 2dFGRS object are kept. They are not flux calibrated (counts/s), so,
like 6dFGS, they are stored in counts and plotted on the counts axis.
"""

import io
import logging
from datetime import timezone

import astropy.units as u
import numpy as np
import requests
from astropy.io import fits
from astropy.io.votable import parse_single_table
from astropy.time import Time
from specutils import Spectrum1D

from tom_dataproducts.models import ReducedDatum
from tom_dataproducts.processors.data_serializers import SpectrumSerializer
from tom_dataservices.dataservices import DataService
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import GS2dFQueryForm
from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT, resolve_query_coordinates

logger = logging.getLogger(__name__)

GS2DF_PAGE_URL = 'https://datacentral.org.au/services/ssa/'
GS2DF_SSA_URL = 'https://datacentral.org.au/vo/ssa/query'
GS2DF_COLLECTION = '2dfgrs_fdr'
GS2DF_DEFAULT_RADIUS_ARCSEC = 5.0

GS2DF_ACKNOWLEDGEMENT = (
    'This research uses spectra from the 2dF Galaxy Redshift Survey (Colless et al. 2001, MNRAS '
    '328, 1039; Colless et al. 2003, arXiv:astro-ph/0306581), obtained from AAO Data Central.'
)


def _gs2df_alias(name):
    return f'2dFGRS_{name}'


def _to_float(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if np.isfinite(value) else None


def _text(value):
    if value is None or value is np.ma.masked:
        return ''
    if isinstance(value, bytes):
        value = value.decode('utf-8', 'replace')
    return str(value).strip()


def _votable_rows(content):
    table = parse_single_table(io.BytesIO(content), verify='ignore').to_table(use_names_over_ids=True)
    return [{name: row[name] for name in table.colnames} for row in table]


def _search_spectra(ra, dec, radius_arcsec):
    """SSA rows of the 2dFGRS object nearest to the position (all of its spectra)."""
    response = requests.get(GS2DF_SSA_URL, params={
        'REQUEST': 'queryData',
        'POS': f'{ra},{dec}',
        'SIZE': radius_arcsec / 3600.0,
        'COLLECTION': GS2DF_COLLECTION,
    }, timeout=DATA_SERVICE_HTTP_TIMEOUT)
    response.raise_for_status()
    rows = [row for row in _votable_rows(response.content) if _text(row.get('target_name'))]
    if not rows:
        return None, []
    # 'score' is the separation from the search position in arcsec.
    nearest = min(rows, key=lambda row: _to_float(row.get('score')) or float('inf'))
    name = _text(nearest['target_name'])
    return name, [row for row in rows if _text(row.get('target_name')) == name]


def _fetch_spectrum(access_url):
    """(wavelength [A], counts/s) from the FITS file behind a spectrum's DataLink."""
    links = requests.get(access_url, timeout=DATA_SERVICE_HTTP_TIMEOUT)
    links.raise_for_status()
    file_url = next(
        (_text(link['access_url']) for link in _votable_rows(links.content)
         if _text(link.get('semantics')) == '#this' and _text(link.get('access_url'))),
        None,
    )
    if not file_url:
        return None
    response = requests.get(file_url, timeout=DATA_SERVICE_HTTP_TIMEOUT)
    response.raise_for_status()
    with fits.open(io.BytesIO(response.content)) as hdul:
        header = hdul[0].header
        counts = np.asarray(hdul[0].data, dtype=float).ravel()
    pixels = np.arange(1, counts.size + 1)
    wavelength = header['CRVAL1'] + (pixels - header['CRPIX1']) * header['CDELT1']
    good = np.isfinite(counts)
    return wavelength[good], counts[good]


class Gs2dfDataService(DataService):
    @classmethod
    def get_finding_chart_radius_arcsec(cls):
        """Default coordinate-match radius shown on the finding chart."""
        return GS2DF_DEFAULT_RADIUS_ARCSEC

    name = '2dFGRS'
    verbose_name = '2dFGRS'
    update_on_daily_refresh = False
    info_url = GS2DF_PAGE_URL
    acknowledgement = GS2DF_ACKNOWLEDGEMENT
    service_notes = (
        'Query 2dF Galaxy Redshift Survey spectra by coordinates from AAO Data Central (SSA). All '
        'spectra of the nearest 2dFGRS object within 5 arcsec are imported, in counts (not flux '
        'calibrated), and its 2dFGRS name is added as an alias.'
    )

    @classmethod
    def get_form_class(cls):
        return GS2dFQueryForm

    @classmethod
    def get_acknowledgement(cls):
        return cls.acknowledgement

    def build_query_parameters(self, parameters, **kwargs):
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or GS2DF_DEFAULT_RADIUS_ARCSEC,
            'include_spectroscopy': bool(parameters.get('include_spectroscopy', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or GS2DF_DEFAULT_RADIUS_ARCSEC

        name, spectra = None, []
        if ra is not None and dec is not None and query_parameters.get('include_spectroscopy', True):
            try:
                name, rows = _search_spectra(ra, dec, radius_arcsec)
                for row in rows:
                    try:
                        spectrum = _fetch_spectrum(_text(row['access_url']))
                    except Exception as exc:
                        logger.warning('2dFGRS: could not read a spectrum of %s: %s', name, exc)
                        continue
                    if spectrum is not None and spectrum[0].size:
                        spectra.append((row, spectrum))
                if not rows:
                    logger.debug('2dFGRS returned no spectrum for RA=%s Dec=%s', ra, dec)
            except Exception as exc:
                logger.warning('2dFGRS query failed for RA=%s Dec=%s: %s', ra, dec, exc)
                name, spectra = None, []

        self.query_results = {
            'name': name,
            'spectra': spectra,
            'source_location': GS2DF_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        name = data.get('name')
        if data.get('ra') is None or data.get('dec') is None or not name:
            return []
        datums = self._build_spectroscopy_datums(data.get('spectra') or [], name)
        if not datums:
            return []
        alias = _gs2df_alias(name)
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

    def _build_spectroscopy_datums(self, spectra, name):
        output = []
        serializer = SpectrumSerializer()
        for row, (wavelength, counts) in spectra:
            mjd = _to_float(row.get('t_midpoint'))
            if mjd is None:
                continue
            serialized = serializer.serialize(Spectrum1D(flux=counts * u.ct, spectral_axis=wavelength * u.AA))
            serialized.update({
                'filter': '2dFGRS',
                'source_id': name,
                'spectrum_type': '2dFGRS_spectrum',
                'redshift': _to_float(row.get('redshift')),
                'snr': _to_float(row.get('em_snr')),
                'match_separation_arcsec': _to_float(row.get('score')),
            })
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': serialized,
            })
        return output
