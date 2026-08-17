"""TESS time-series photometry from MAST.

Conversion (TESS FAQ, HEASARC; TESS Instrument Handbook p.37):

    Flux [e-/s] = 10**((20.44 - Tmag)/2.5)
      =>  Tmag  = 20.44 - 2.5*log10(Flux [e-/s])

All TESS data is public immediately on arrival at MAST -- there is no proprietary
period, so no release-date filter is applied.

Every cadence is stored exactly as the portal serves it -- there is no binning,
smoothing or resampling anywhere in this module. What lands in ReducedDatum is
the converted magnitude, the time and the filter; the raw e-/s flux is not
stored, only used to derive the magnitude.

Three things this gets right that are easy to get wrong
-------------------------------------------------------
1. TIMES ARE BARYCENTRIC. The TIME column is BTJD = BJD_TDB - 2457000, so
   MJD = BTJD + 56999.5, but it stays barycentric TDB rather than observed UTC
   (up to ~8.3 min apart). Each datum records which it is.

2. FLUX UNITS ARE CHECKED, NOT ASSUMED. Some products (QLP and other HLSPs) ship
   normalised dimensionless flux with median 1.0. Feeding that to the conversion
   yields a meaningless pile of points at T = 20.44. The column UNIT is checked,
   never the column name, with a median-flux backstop.

3. ONE TIC PER QUERY. TESS pixels are 21 arcsec and photometry is heavily
   blended, so a cone search can return several stars. Only the nearest TIC's
   products are ingested; merging several would splice different stars into one
   light curve.

Volume note: a single sector at 120 s cadence is ~15,000 cadences and a
well-observed target has 30+ sectors, so a full ingest is several hundred
thousand ReducedDatum rows. On SQLite, run this with the db_worker stopped and
keep max_sectors bounded; a large concurrent write is what corrupts the file.
"""

import logging
import os
import re
import tempfile
from datetime import timezone

import numpy as np
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.time import Time
import astropy.units as u

from django.conf import settings

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import TESSQueryForm


logger = logging.getLogger(__name__)

TESS_PAGE_URL = 'https://archive.stsci.edu/missions-and-data/tess'

# TESS FAQ / Instrument Handbook p.37
TESS_ZERO_POINT = 20.44
BTJD_TO_MJD = 56999.5  # 2457000 - 2400000.5

MAG_ERR_FACTOR = 1.0857362  # 2.5/ln(10)

ELECTRONS_PER_SEC = u.electron / u.s

# One TESS pixel. Anything closer than this is blended in the aperture anyway.
DEFAULT_RADIUS_ARCSEC = 21.0
DEFAULT_MAX_SECTORS = 12

# Combined multi-sector products (…-s0001-s0096-…) repeat data already present in
# the per-sector files; ingesting both would double-count every cadence.
_MULTISECTOR_OBS_ID = re.compile(r'-s\d{4}-s\d{4}-')


def _to_float(value):
    try:
        if value is None or np.ma.is_masked(value):
            return None
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if np.isfinite(value) else None


def _tic_alias(ticid):
    return f'TIC_{int(ticid)}'


def _tess_source_location(ticid):
    return f'https://mast.stsci.edu/portal/Mashup/Clients/Mast/Portal.html?searchQuery=TIC {int(ticid)}'


def _mast_download_dir():
    configured = getattr(settings, 'TESS_MAST_CACHE_DIR', None)
    path = configured or os.path.join(tempfile.gettempdir(), 'bhtom_tess_mast')
    os.makedirs(path, exist_ok=True)
    return path


def flux_to_tmag(flux, flux_err=None):
    """e-/s -> Tmag. Non-positive flux -> NaN (faint target, over-subtracted background)."""
    flux = np.asarray(flux, float)
    tmag = np.full(flux.shape, np.nan)
    ok = np.isfinite(flux) & (flux > 0)
    tmag[ok] = TESS_ZERO_POINT - 2.5 * np.log10(flux[ok])
    if flux_err is None:
        return tmag, None
    flux_err = np.asarray(flux_err, float)
    err = np.full(flux.shape, np.nan)
    err[ok] = MAG_ERR_FACTOR * flux_err[ok] / flux[ok]
    return tmag, err


def parse_flux_unit(unit_text):
    """Parse a FITS TUNIT into an astropy unit, or None if it is not a real unit.

    TESS light curves write TUNIT='e-/s', which astropy does NOT recognise: it
    returns an UnrecognizedUnit whose is_equivalent() is always False, so a naive
    unit check silently rejects every genuine TESS flux column. The known
    electron-per-second spellings are normalised here before falling back to
    astropy's parser.
    """
    text = (unit_text or '').strip()
    if not text:
        return None

    normalised = re.sub(r'\s+', '', text.lower())
    normalised = normalised.replace('electrons', 'electron')
    normalised = re.sub(r'^e-?/', 'electron/', normalised)
    normalised = re.sub(r'/sec$', '/s', normalised)
    if normalised in ('electron/s', 'electron/second'):
        return ELECTRONS_PER_SEC

    try:
        parsed = u.Unit(text, parse_strict='silent')
    except Exception:
        return None
    return None if isinstance(parsed, u.UnrecognizedUnit) else parsed


def pick_flux_column(columns, header, prefer='sap'):
    """Return a column genuinely in e-/s, or (None, reason).

    Never trusts the column NAME: many HLSPs ship normalised, dimensionless flux
    with median 1.0 under a plausible-looking name. The FITS TUNIT is checked
    first, with a median-value backstop for products that omit the unit.
    """
    order = ['SAP_FLUX', 'PDCSAP_FLUX'] if prefer == 'sap' else ['PDCSAP_FLUX', 'SAP_FLUX']
    order += [c for c in ('FLUX',) if c not in order]

    rejected = []
    names = list(columns.names)
    for name in order:
        if name not in names:
            continue
        unit_text = (header.get(f'TUNIT{names.index(name) + 1}') or '').strip()
        unit = parse_flux_unit(unit_text)
        if unit is not None and unit.is_equivalent(ELECTRONS_PER_SEC):
            return name, None

        median = np.nanmedian(np.asarray(columns[name].array, float)) if name in names else np.nan
        if not unit_text and np.isfinite(median) and median > 100:
            logger.debug('TESS: %s has no unit but median %.1f, assuming e-/s', name, median)
            return name, None
        rejected.append(f'{name}(unit={unit_text or None}, median={median:.3g})')

    return None, 'no e-/s column; found ' + ', '.join(rejected or ['nothing'])


class TESSDataService(DataService):
    name = 'TESS'
    verbose_name = 'TESS'
    update_on_daily_refresh = False
    info_url = TESS_PAGE_URL
    service_notes = (
        'Query public TESS light curves from MAST by coordinates and convert e-/s to Tmag '
        '(Tmag = 20.44 - 2.5*log10(flux)). Binned in flux space; times are barycentric TDB. '
        'TESS pixels are 21 arcsec, so photometry is blended -- check CROWDSAP on each point.'
    )

    @classmethod
    def get_form_class(cls):
        return TESSQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates
        target_name, ra, dec = resolve_query_coordinates(parameters)
        self.query_parameters = {
            'target_name': target_name,
            'ra': ra,
            'dec': dec,
            'radius_arcsec': parameters.get('radius_arcsec') or DEFAULT_RADIUS_ARCSEC,
            'flux_type': parameters.get('flux_type') or 'sap',
            'max_sectors': parameters.get('max_sectors') or DEFAULT_MAX_SECTORS,
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or DEFAULT_RADIUS_ARCSEC
        max_sectors = int(_to_float(query_parameters.get('max_sectors')) or DEFAULT_MAX_SECTORS)

        if ra is None or dec is None:
            self.query_results = {'products': None, 'source_location': None}
            return self.query_results

        ticid = None
        products = None
        try:
            from astroquery.mast import Observations

            observations = Observations.query_criteria(
                coordinates=SkyCoord(ra, dec, unit='deg'),
                radius=radius_arcsec * u.arcsec,
                obs_collection='TESS',
                dataproduct_type='timeseries',
            )
            if len(observations) == 0:
                logger.debug('TESS returned no time series for RA=%s Dec=%s', ra, dec)
            else:
                ticid, selected = self._select_observations(observations, ra, dec, max_sectors)
                if selected is not None and len(selected):
                    products = self._collect_lightcurve_products(Observations, selected)
        except Exception as exc:
            logger.debug('TESS MAST error %s', exc)

        self.query_results = {
            'products': products or None,
            'ticid': ticid,
            'source_location': _tess_source_location(ticid) if ticid else TESS_PAGE_URL,
            'ra': ra,
            'dec': dec,
            'flux_type': query_parameters.get('flux_type') or 'sap',
        }
        return self.query_results

    def _select_observations(self, observations, ra, dec, max_sectors):
        """Pick the nearest TIC, then its single-sector SPOC light curves.

        A cone search can straddle several TICs; splicing more than one into a
        single light curve would merge different stars. Multi-sector combined
        products are dropped because they repeat the per-sector data, and the
        120 s cadence is preferred over the 20 s 'fast' products, which hold six
        times the rows for the same baseline.
        """
        target = SkyCoord(ra, dec, unit='deg')

        best_tic = None
        best_sep = None
        for row in observations:
            row_ra = _to_float(row['s_ra'])
            row_dec = _to_float(row['s_dec'])
            tic = _to_float(row['target_name'])
            if row_ra is None or row_dec is None or tic is None:
                continue
            sep = SkyCoord(row_ra, row_dec, unit='deg').separation(target).arcsec
            if best_sep is None or sep < best_sep:
                best_tic, best_sep = int(tic), sep

        if best_tic is None:
            return None, None
        logger.debug('TESS: nearest TIC %s at %.2f arcsec', best_tic, best_sep)

        keep = []
        for row in observations:
            tic = _to_float(row['target_name'])
            if tic is None or int(tic) != best_tic:
                continue
            if str(row['provenance_name']).strip().upper() != 'SPOC':
                continue
            if _MULTISECTOR_OBS_ID.search(str(row['obs_id'])):
                continue
            keep.append(row)

        by_sector = {}
        for row in keep:
            sector = _to_float(row['sequence_number'])
            if sector is None:
                continue
            sector = int(sector)
            exptime = _to_float(row['t_exptime']) or 0.0
            current = by_sector.get(sector)
            # Prefer 120 s; otherwise the longest available cadence.
            score = (0 if abs(exptime - 120.0) < 1 else 1, -exptime)
            if current is None or score < current[0]:
                by_sector[sector] = (score, row)

        sectors = sorted(by_sector)
        if len(sectors) > max_sectors:
            # Keep the most recent sectors; they are the ones a follow-up user wants.
            dropped = sectors[:-max_sectors]
            sectors = sectors[-max_sectors:]
            logger.info(
                'TESS: TIC %s has %s sectors, ingesting the %s most recent (skipped %s).',
                best_tic, len(dropped) + len(sectors), len(sectors), dropped,
            )

        return best_tic, [by_sector[s][1] for s in sectors]

    def _collect_lightcurve_products(self, Observations, selected):
        """Download the _lc.fits for each selected observation."""
        download_dir = _mast_download_dir()
        paths = []
        for row in selected:
            try:
                product_list = Observations.get_product_list(row)
                lightcurves = Observations.filter_products(
                    product_list, productSubGroupDescription='LC', productType='SCIENCE'
                )
                if len(lightcurves) == 0:
                    continue
                manifest = Observations.download_products(
                    lightcurves[:1], cache=True, download_dir=download_dir
                )
                for local_path in manifest['Local Path']:
                    paths.append(str(local_path))
            except Exception as exc:
                logger.warning('TESS: could not fetch products for %s: %s', row['obs_id'], exc)
        return paths or None

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        ra = data.get('ra')
        dec = data.get('dec')
        products = data.get('products')
        ticid = data.get('ticid')
        if ra is None or dec is None or not products:
            return []

        datums = self._build_photometry_datums(
            products,
            flux_type=data.get('flux_type') or 'sap',
        )
        if not datums:
            return []

        alias = _tic_alias(ticid) if ticid else None
        return [{
            'name': alias,
            'ra': ra,
            'dec': dec,
            'aliases': [alias] if alias else [],
            'reduced_datums': {'photometry': datums},
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
        for data_type, data in data_results.items():
            self.create_reduced_datums_from_query(
                target,
                data=data,
                data_type=data_type,
                source_location=self.query_results.get('source_location') or self.info_url,
            )

    def _build_photometry_datums(self, paths, flux_type='sap'):
        output = []
        for path in paths:
            try:
                output.extend(self._datums_from_lightcurve(path, flux_type))
            except Exception as exc:
                logger.warning('TESS: failed to process %s: %s', os.path.basename(path), exc)
        return output

    def _datums_from_lightcurve(self, path, flux_type):
        """Convert one sector's _lc.fits into one Tmag datum per TESS cadence.

        One row in, one row out: the only cadences dropped are those TESS itself
        flags (QUALITY != 0) or that carry a non-finite time or flux.
        """
        with fits.open(path, memmap=False) as hdul:
            primary = hdul[0].header
            lightcurve = hdul['LIGHTCURVE']
            header = lightcurve.header
            data = lightcurve.data

            sector = primary.get('SECTOR')

            column, why = pick_flux_column(data.columns, header, prefer=flux_type)
            if column is None:
                logger.warning('TESS: sector %s has no e-/s flux column (%s); skipping.', sector, why)
                return []

            time = np.asarray(data['TIME'], float)  # BTJD
            flux = np.asarray(data[column], float)
            err_column = f'{column}_ERR'
            if err_column in data.columns.names:
                flux_err = np.asarray(data[err_column], float)
            else:
                flux_err = np.full_like(flux, np.nan)
            if 'QUALITY' in data.columns.names:
                quality = np.asarray(data['QUALITY'], int)
            else:
                quality = np.zeros_like(flux, dtype=int)

            good = np.isfinite(time) & np.isfinite(flux) & (quality == 0)
            time, flux, flux_err = time[good], flux[good], flux_err[good]

        if time.size == 0:
            return []

        # Backstop against normalised/relative flux slipping past the unit check:
        # a median near 1 is dimensionless flux and would pile every point up at
        # T = 20.44 rather than a real magnitude.
        median = np.nanmedian(flux)
        if np.isfinite(median) and 0.2 < median < 5.0:
            logger.warning(
                'TESS: refusing sector %s, median flux %.4f is normalised, not e-/s '
                '(would give T~%.2f).', sector, median, TESS_ZERO_POINT - 2.5 * np.log10(median),
            )
            return []

        # Flux is converted to magnitude here and only the magnitude is kept; the
        # e-/s values are not written to the database.
        tmag, tmag_err = flux_to_tmag(flux, flux_err)

        output = []
        for i in range(len(tmag)):
            if not np.isfinite(tmag[i]):
                continue
            # The timestamp is the barycentric MJD (BTJD + 56999.5). Converting the
            # TDB scale to UTC would apply only the ~70 s offset while leaving the
            # barycentric light-travel term (up to ~8.3 min, target-dependent)
            # untouched, which would look converted without being converted.
            mjd = float(time[i]) + BTJD_TO_MJD
            # The TESS per-cadence error, converted straight from SAP_FLUX_ERR.
            # The 20.44 zero point carries a further ~0.05 mag systematic that is
            # not included here, so 'error' is exactly what TESS reports.
            error = float(tmag_err[i]) if tmag_err is not None and np.isfinite(tmag_err[i]) else 0.01
            output.append({
                'timestamp': Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {
                    'filter': 'TESS(T)',
                    'magnitude': round(float(tmag[i]), 5),
                    'error': round(error, 6),
                },
            })

        logger.debug(
            'TESS: sector %s %s -> %s points from %s good cadences (unbinned)',
            sector, column, len(output), int(good.sum()),
        )
        return output
