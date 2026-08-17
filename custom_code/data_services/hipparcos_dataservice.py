"""Hipparcos/Tycho photometry from the ESA mission catalogues (VizieR I/239).

Scope
-----
This service ingests the *mean* mission photometry -- Hp, BT and VT -- not epoch
light curves. The Hipparcos Epoch Photometry Annex (the ~100-200 individual
transits per star) is no longer publicly retrievable: it is absent from VizieR
(catalogue I/239 exposes 15 tables, none of them epoch photometry, and the CDS
file manifest lists no epoch data file), and ESA's original
``rssd.esa.int/hipparcos_scripts`` service has been retired. What is ingested
here are three real calibrated measurements per star, each the mission mean over
1989-1993, which is still worth having: it extends a target's light curve back
three decades before ZTF/ATLAS/ASAS-SN.

The Variability Annex (periodic and unsolved variables) is attached to the Hp
datum, so a target still carries its Hipparcos variability type, period and
amplitude even though the underlying light curve cannot be downloaded.

Band warning
------------
Hp is NOT V. It is a very broad unfiltered passband set by the S20 image
dissector response, lambda_eff ~ 520 nm with FWHM ~ 230 nm (Bessell 2000).
Converting Hp -> V needs a colour term, so the filters are stored under their
own names ('Hp', 'BT', 'VT') and must not be stacked with V-band data raw.
"""

import logging
import time
from datetime import timezone

import numpy as np
import pyvo
from astropy.time import Time

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import HipparcosQueryForm


logger = logging.getLogger(__name__)

VIZIER_TAP_URL = 'https://tapvizier.cds.unistra.fr/TAPVizieR/tap'
HIPPARCOS_PAGE_URL = 'https://vizier.cds.unistra.fr/viz-bin/VizieR-3?-source=I/239/hip_main'

HIP_MAIN = 'I/239/hip_main'
TYC_MAIN = 'I/239/tyc_main'
HIP_VA_1 = 'I/239/hip_va_1'  # Variability Annex: periodic variables
HIP_VA_2 = 'I/239/hip_va_2'  # Variability Annex: unsolved variables

# The catalogue epoch, J1991.25 = JD 2448349.0625 (TT). Every mean magnitude is
# a mission average over 1989-1993 conventionally attributed to this epoch.
J1991_25_MJD = 48348.5625

# CDS TAP is intermittently flaky under load. Two distinct transient failures show
# up, and both clear on a retry: an explicit "service too busy", and a bogus
# "unresolved identifiers" that is really VizieR's ADQL validator being unable to
# run ("Unable to check the ADQL query"). The latter reads like a query bug but is
# not one -- the identical query succeeds seconds later.
TAP_MAX_ATTEMPTS = 4
TAP_RETRY_SLEEP = 6.0
TAP_TRANSIENT_ERRORS = ('too busy', 'unable to check the adql query', 'no connection available')


def _to_float(value):
    """Astropy tables hand back masked values for absent measurements."""
    try:
        if value is None or value is np.ma.masked or np.ma.is_masked(value):
            return None
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if np.isfinite(value) else None


def _to_text(value):
    try:
        if value is None or value is np.ma.masked or np.ma.is_masked(value):
            return ''
        if isinstance(value, bytes):
            value = value.decode('utf8', 'replace')
    except (TypeError, ValueError):
        return ''
    text = str(value).strip()
    return '' if text in ('--', 'nan', 'None') else text


def _hip_alias(hip):
    return f'HIP_{hip}'


def _tyc_alias(tyc):
    return f'TYC_{"-".join(str(tyc).split())}'


def _hip_source_location(hip):
    return f'https://vizier.cds.unistra.fr/viz-bin/VizieR-4?-source={HIP_MAIN}&HIP={hip}'


def _run_tap(tap, query, maxrec=10):
    """Run one ADQL query, retrying while CDS reports the service is busy."""
    last_error = None
    for attempt in range(TAP_MAX_ATTEMPTS):
        try:
            return tap.run_sync(query, maxrec=maxrec).to_table()
        except Exception as exc:
            last_error = exc
            message = str(exc).lower()
            transient = any(marker in message for marker in TAP_TRANSIENT_ERRORS)
            if transient and attempt < TAP_MAX_ATTEMPTS - 1:
                logger.debug('Hipparcos: transient VizieR TAP error, retrying: %s', str(exc)[:120])
                time.sleep(TAP_RETRY_SLEEP)
                continue
            raise
    raise last_error


def _cone_query(table, columns, ra, dec, radius_deg):
    """Cone search on the proper-motion-corrected J2000 positions.

    The catalogue's own RAICRS/DEICRS are at epoch J1991.25. Hipparcos stars are
    nearby and many have large proper motions, so matching a J2000 BHTOM target
    against J1991.25 positions would miss exactly the high-proper-motion stars.
    VizieR's computed "_RA.icrs"/"_DE.icrs" columns are J2000 with proper motion
    applied, which is what BHTOM target coordinates are.
    """
    selected = ', '.join(f'"{c}"' for c in columns)
    return (
        f'SELECT TOP 20 {selected} FROM "{table}" '
        f'WHERE 1=CONTAINS(POINT(\'ICRS\', "_RA.icrs", "_DE.icrs"), '
        f"CIRCLE('ICRS', {ra}, {dec}, {radius_deg}))"
    )


def _angular_separation_arcsec(ra1, dec1, ra2, dec2):
    ra1, dec1, ra2, dec2 = np.radians([ra1, dec1, ra2, dec2])
    sep = np.arccos(
        np.clip(
            np.sin(dec1) * np.sin(dec2) + np.cos(dec1) * np.cos(dec2) * np.cos(ra1 - ra2),
            -1.0,
            1.0,
        )
    )
    return float(np.degrees(sep) * 3600.0)


def _nearest_row(table, ra, dec):
    """VizieR TAP rejects an aliased ORDER BY, so the closest match is picked here."""
    best = None
    best_sep = None
    for row in table:
        row_ra = _to_float(row['_RA_icrs'])
        row_dec = _to_float(row['_DE_icrs'])
        if row_ra is None or row_dec is None:
            continue
        sep = _angular_separation_arcsec(ra, dec, row_ra, row_dec)
        if best_sep is None or sep < best_sep:
            best, best_sep = row, sep
    return best, best_sep


HIP_COLUMNS = (
    'HIP', '_RA.icrs', '_DE.icrs', 'Hpmag', 'e_Hpmag', 'Hpscat', 'o_Hpmag',
    'BTmag', 'e_BTmag', 'VTmag', 'e_VTmag', 'B-V', 'Vmag',
    'HvarType', 'Period', 'morePhoto', 'Hpmax', 'HPmin',
)
TYC_COLUMNS = (
    'TYC', 'HIP', '_RA.icrs', '_DE.icrs', 'BTmag', 'e_BTmag',
    'VTmag', 'e_VTmag', 'VTscat', 'Nphoto', 'morePhoto',
)
# 'VarType' is listed in TAP_SCHEMA but is not actually resolvable on the VizieR
# TAP endpoint, so the GCVS-style type comes from VarName/SpType instead.
VA_COLUMNS = ('HIP', 'HvarType', 'VarName', 'SpType', 'Period', 'maxMag', 'minMag', 'Band')

HVAR_TYPE_LABELS = {
    'C': 'constant',
    'D': 'duplicity-induced',
    'M': 'micro-variable',
    'P': 'periodic',
    'R': 'revised colour index',
    'U': 'unsolved',
}


class HipparcosDataService(DataService):
    name = 'Hipparcos'
    verbose_name = 'Hipparcos/Tycho'
    update_on_daily_refresh = False
    info_url = HIPPARCOS_PAGE_URL
    service_notes = (
        'Query Hipparcos/Tycho (VizieR I/239) mean mission photometry by coordinates. '
        'Ingests Hp, BT and VT at epoch J1991.25 plus Variability Annex metadata. '
        'Hp is a broad unfiltered band, not Johnson V, and needs a colour term to convert.'
    )

    @classmethod
    def get_form_class(cls):
        return HipparcosQueryForm

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
        ra = _to_float(query_parameters.get('ra'))
        dec = _to_float(query_parameters.get('dec'))
        radius_arcsec = _to_float(query_parameters.get('radius_arcsec')) or 5.0

        if ra is None or dec is None:
            self.query_results = {'hip_row': None, 'tyc_row': None, 'source_location': None}
            return self.query_results

        radius_deg = radius_arcsec / 3600.0
        hip_row = tyc_row = variability = None
        hip_sep = tyc_sep = None

        try:
            tap = pyvo.dal.TAPService(VIZIER_TAP_URL)

            try:
                hip_table = _run_tap(tap, _cone_query(HIP_MAIN, HIP_COLUMNS, ra, dec, radius_deg), maxrec=20)
                hip_row, hip_sep = _nearest_row(hip_table, ra, dec)
            except Exception as exc:
                logger.debug('Hipparcos hip_main query failed for RA=%s Dec=%s: %s', ra, dec, exc)

            try:
                tyc_table = _run_tap(tap, _cone_query(TYC_MAIN, TYC_COLUMNS, ra, dec, radius_deg), maxrec=20)
                tyc_row, tyc_sep = _nearest_row(tyc_table, ra, dec)
            except Exception as exc:
                logger.debug('Hipparcos tyc_main query failed for RA=%s Dec=%s: %s', ra, dec, exc)

            hip_id = _to_float(hip_row['HIP']) if hip_row is not None else None
            if hip_id is not None:
                variability = self._query_variability(tap, int(hip_id))

            if hip_row is None and tyc_row is None:
                logger.debug('Hipparcos/Tycho returned no match for RA=%s Dec=%s', ra, dec)
        except Exception as exc:
            logger.debug('Hipparcos VizieR TAP error %s', exc)

        hip_id = int(_to_float(hip_row['HIP'])) if hip_row is not None else None
        self.query_results = {
            'hip_row': hip_row,
            'tyc_row': tyc_row,
            'hip_sep_arcsec': hip_sep,
            'tyc_sep_arcsec': tyc_sep,
            'variability': variability,
            'source_location': _hip_source_location(hip_id) if hip_id else HIPPARCOS_PAGE_URL,
            'ra': ra,
            'dec': dec,
        }
        return self.query_results

    def _query_variability(self, tap, hip):
        """Look the star up in both Variability Annex tables; periodic wins if both hit."""
        for table, solved in ((HIP_VA_1, True), (HIP_VA_2, False)):
            columns = ', '.join(f'"{c}"' for c in VA_COLUMNS)
            try:
                result = _run_tap(tap, f'SELECT TOP 1 {columns} FROM "{table}" WHERE "HIP" = {hip}', maxrec=1)
            except Exception as exc:
                logger.debug('Hipparcos %s lookup failed for HIP %s: %s', table, hip, exc)
                continue
            if len(result):
                row = result[0]
                return {
                    'annex': 'periodic' if solved else 'unsolved',
                    'var_name': _to_text(row['VarName']),
                    'spectral_type': _to_text(row['SpType']),
                    'hvar_type': _to_text(row['HvarType']),
                    'period': _to_float(row['Period']),
                    'mag_max': _to_float(row['maxMag']),
                    'mag_min': _to_float(row['minMag']),
                    'band': _to_text(row['Band']),
                }
        return None

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        ra = data.get('ra')
        dec = data.get('dec')
        hip_row = data.get('hip_row')
        tyc_row = data.get('tyc_row')
        if ra is None or dec is None or (hip_row is None and tyc_row is None):
            return []

        aliases = []
        name = None
        if hip_row is not None:
            hip_id = int(_to_float(hip_row['HIP']))
            name = _hip_alias(hip_id)
            aliases.append(name)
        if tyc_row is not None:
            tyc_text = _to_text(tyc_row['TYC'])
            if tyc_text:
                tyc_name = _tyc_alias(tyc_text)
                aliases.append(tyc_name)
                if name is None:
                    name = tyc_name

        datums = self._build_photometry_datums(hip_row, tyc_row, data.get('variability'))
        if not datums:
            return []

        return [{
            'name': name,
            'ra': ra,
            'dec': dec,
            'aliases': aliases,
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

    def _build_photometry_datums(self, hip_row, tyc_row, variability):
        """Three mean magnitudes at J1991.25: Hp from Hipparcos, BT/VT from Tycho.

        hip_main repeats the Tycho BT/VT for stars that have both, so it is preferred
        and tyc_main only fills in for Tycho-only stars.
        """
        timestamp = Time(J1991_25_MJD, format='mjd', scale='utc').to_datetime(timezone=timezone.utc)
        output = []

        def add(filter_name, magnitude, error, extras=None):
            if magnitude is None:
                return
            value = {'filter': filter_name, 'magnitude': magnitude, 'error': error}
            if extras:
                value.update(extras)
            output.append({'timestamp': timestamp, 'value': value})

        if hip_row is not None:
            add('Hp', _to_float(hip_row['Hpmag']), _to_float(hip_row['e_Hpmag']),
                self._hp_extras(hip_row, variability))
            # Bright stars (Vega, say) can have BT/VT masked in hip_main while their
            # own Tycho entry carries them. Only fall back when tyc_main names the
            # same HIP, so a close neighbour can never be blended in.
            tyc_fallback = tyc_row if self._same_star(hip_row, tyc_row) else None
            for band in ('BT', 'VT'):
                magnitude = _to_float(hip_row[f'{band}mag'])
                error = _to_float(hip_row[f'e_{band}mag'])
                if magnitude is None and tyc_fallback is not None:
                    magnitude = _to_float(tyc_fallback[f'{band}mag'])
                    error = _to_float(tyc_fallback[f'e_{band}mag'])
                add(band, magnitude, error)
        elif tyc_row is not None:
            add('BT', _to_float(tyc_row['BTmag']), _to_float(tyc_row['e_BTmag']))
            add('VT', _to_float(tyc_row['VTmag']), _to_float(tyc_row['e_VTmag']))

        return output

    @staticmethod
    def _same_star(hip_row, tyc_row):
        if hip_row is None or tyc_row is None:
            return False
        hip_id = _to_float(hip_row['HIP'])
        tyc_hip_id = _to_float(tyc_row['HIP'])
        return hip_id is not None and tyc_hip_id is not None and int(hip_id) == int(tyc_hip_id)

    def _hp_extras(self, hip_row, variability):
        """Variability metadata, carried on the Hp point since the Annex light curve is gone."""
        hvar = _to_text(hip_row['HvarType'])
        extras = {
            'n_observations': _to_float(hip_row['o_Hpmag']),
            'scatter': _to_float(hip_row['Hpscat']),
            'mag_max': _to_float(hip_row['Hpmax']),
            'mag_min': _to_float(hip_row['HPmin']),
        }
        if hvar:
            extras['variability_type'] = HVAR_TYPE_LABELS.get(hvar, hvar)
        period = _to_float(hip_row['Period'])
        if period is not None:
            extras['period'] = period
        # 'A'/'B'/'C' means the Epoch Photometry Annex holds a light curve for this
        # star. It is recorded so the information is not lost, but the Annex itself
        # is no longer downloadable from VizieR or ESA.
        more_photo = _to_text(hip_row['morePhoto'])
        if more_photo:
            extras['epoch_photometry_annex'] = more_photo

        if variability:
            if variability.get('var_name'):
                extras['var_name'] = variability['var_name']
            if variability.get('period') is not None:
                extras['period'] = variability['period']
            if variability.get('spectral_type'):
                extras['spectral_type'] = variability['spectral_type']
            extras['variability_annex'] = variability['annex']

        return {k: v for k, v in extras.items() if v is not None}
