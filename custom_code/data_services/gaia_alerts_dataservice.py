import csv
import io
import math
import logging

import requests
from astropy.coordinates import SkyCoord
from astropy.time import Time
from datetime import timezone

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import GaiaAlertsQueryForm


logger = logging.getLogger(__name__)

GAIA_ALERTS_BASE_URL = 'https://gsaweb.ast.cam.ac.uk'


def _to_float(value):
    try:
        number = float(value)
        if math.isnan(number) or not math.isfinite(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def _to_rows(csv_text):
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = []
    for row in reader:
        normalized = {}
        for key, value in row.items():
            if key is None:
                continue
            normalized[key.strip()] = value.strip() if isinstance(value, str) else value
        if normalized:
            rows.append(normalized)
    return rows


def _gaia_alerts_error(mag):
    # Same piecewise approximation used by the historical Gaia Alerts broker.
    if mag <= 13.5:
        exponent = 0.2 * 13.5 - 5.2
    elif mag <= 17.0:
        exponent = 0.2 * mag - 5.2
    else:
        exponent = 0.26 * mag - 6.26
    return 10 ** exponent


class GaiaAlertsDataService(DataService):
    name = 'GaiaAlerts'
    verbose_name = 'GaiaAlerts'
    info_url = f'{GAIA_ALERTS_BASE_URL}/alerts'
    service_notes = 'Query Gaia Alerts by alert name or cone search, with optional lightcurve photometry.'

    @classmethod
    def get_form_class(cls):
        return GaiaAlertsQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        self.query_parameters = {
            'alert_name': (parameters.get('alert_name') or '').strip(),
            'ra': parameters.get('ra'),
            'dec': parameters.get('dec'),
            'radius_arcsec': parameters.get('radius_arcsec') or 5.0,
            'include_photometry': bool(parameters.get('include_photometry', True)),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        alert_name = (query_parameters.get('alert_name') or '').strip()
        ra = query_parameters.get('ra')
        dec = query_parameters.get('dec')
        radius_arcsec = float(query_parameters.get('radius_arcsec') or 5.0)

        alerts_rows = self._fetch_alerts_rows()
        alert_row = None

        if alert_name:
            alert_row = self._find_by_name(alerts_rows, alert_name)

        if alert_row is None and ra is not None and dec is not None:
            alert_row = self._find_by_cone(alerts_rows, float(ra), float(dec), radius_arcsec)

        phot_rows = []
        lightcurve_url = None
        if alert_row and query_parameters.get('include_photometry', True):
            selected_name = alert_row.get('#Name')
            if selected_name:
                lightcurve_url = f'{GAIA_ALERTS_BASE_URL}/alerts/alert/{selected_name}/lightcurve.csv'
                try:
                    phot_rows = self._fetch_lightcurve_rows(lightcurve_url)
                except Exception as exc:
                    logger.warning('Gaia Alerts lightcurve unavailable for %s: %s', selected_name, exc)

        self.query_results = {'alert': alert_row, 'lightcurve_rows': phot_rows, 'lightcurve_url': lightcurve_url}
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        data = self.query_service(query_parameters, **kwargs)
        alert = data.get('alert')
        if not alert:
            return []

        alert_name = alert.get('#Name')
        ra = _to_float(alert.get('RaDeg'))
        dec = _to_float(alert.get('DecDeg'))
        if not alert_name or ra is None or dec is None:
            return []

        target_result = {
            'name': alert_name,
            'ra': ra,
            'dec': dec,
            'aliases': [alert_name],
            'reduced_datums': {
                'photometry': self._build_photometry_datums(data.get('lightcurve_rows', [])),
            },
            'source_location': data.get('lightcurve_url'),
        }
        return [target_result]

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
        # Pass source URL through to create_reduced_datums_from_query for better provenance.
        if not data_results:
            return
        for data_type, data in data_results.items():
            self.create_reduced_datums_from_query(
                target,
                data=data,
                data_type=data_type,
                source_location=self.query_results.get('lightcurve_url') or self.info_url,
            )

    def _fetch_alerts_rows(self):
        response = requests.get(f'{GAIA_ALERTS_BASE_URL}/alerts/alerts.csv', timeout=30)
        response.raise_for_status()
        return _to_rows(response.text)

    def _fetch_lightcurve_rows(self, lightcurve_url):
        response = requests.get(lightcurve_url, timeout=30)
        response.raise_for_status()
        rows = []
        for raw_line in response.text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith('#'):
                continue
            parts = [part.strip() for part in line.split(',')]
            if len(parts) < 3:
                continue
            # Gaia Alerts lightcurve format: "date,jd,mag".
            rows.append({'jd': parts[1], 'mag': parts[2]})
        return rows

    def _find_by_name(self, rows, alert_name):
        alert_name_lower = alert_name.lower()
        for row in rows:
            name = (row.get('#Name') or '').strip()
            if name.lower() == alert_name_lower:
                return row
        return None

    def _find_by_cone(self, rows, ra_deg, dec_deg, radius_arcsec):
        center = SkyCoord(ra_deg, dec_deg, unit='deg')
        best_row = None
        best_sep_arcsec = None
        for row in rows:
            ra = _to_float(row.get('RaDeg'))
            dec = _to_float(row.get('DecDeg'))
            if ra is None or dec is None:
                continue
            try:
                candidate = SkyCoord(ra, dec, unit='deg')
            except Exception:
                continue
            sep_arcsec = center.separation(candidate).arcsecond
            if sep_arcsec <= radius_arcsec and (best_sep_arcsec is None or sep_arcsec < best_sep_arcsec):
                best_row = row
                best_sep_arcsec = sep_arcsec
        return best_row

    def _build_photometry_datums(self, rows):
        output = []
        for row in rows:
            jd = _to_float(row.get('jd') or row.get('JD(TCB)'))
            mag_text = (row.get('mag') or row.get('averagemag') or '').strip()
            if jd is None or not mag_text:
                continue
            if mag_text.lower() in ('untrusted', 'null', 'nan'):
                continue
            mag = _to_float(mag_text)
            if mag is None:
                continue

            output.append({
                'timestamp': Time(jd, format='jd', scale='utc').to_datetime(timezone=timezone.utc),
                'value': {'filter': 'GSA(G)', 'magnitude': mag, 'error': _gaia_alerts_error(mag)},
            })
        return output
