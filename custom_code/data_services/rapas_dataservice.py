import hashlib
import io
import logging
import math
import re
import time
import unicodedata
import zipfile
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
from xml.etree import ElementTree

import requests
from astropy.coordinates import SkyCoord
from astropy.time import Time
from django.conf import settings
from django.core.cache import cache, caches

from tom_dataservices.dataservices import DataService
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target, TargetName

from custom_code.data_services.forms import RAPASQueryForm
from custom_code.data_services.service_utils import DATA_SERVICE_HTTP_TIMEOUT


logger = logging.getLogger(__name__)

_MAIN_NS = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
_REL_NS = 'http://schemas.openxmlformats.org/package/2006/relationships'
_OFFICE_REL_NS = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
_RAPAS_BANDS = {5: 'G', 7: 'GBP', 9: 'GRP'}


def _clean_text(value):
    return '' if value is None else str(value).strip()


def _normalized_label(value):
    text = unicodedata.normalize('NFKD', _clean_text(value))
    return ''.join(char for char in text if not unicodedata.combining(char)).casefold()


def _normalized_name(value):
    return re.sub(r'\s+', ' ', _clean_text(value)).casefold()


def _compact_name(value):
    return re.sub(r'[^a-z0-9]', '', _normalized_name(value))


def _record_names(record):
    return {
        _normalized_name(record.get('name')),
        _normalized_name(record.get('sheet_name')),
    }


def _record_matches_partial_name(record, query):
    compact_query = _compact_name(query)
    if not compact_query:
        return False
    return any(compact_query in _compact_name(name) for name in _record_names(record))


def _record_coordinate(record, coordinate):
    value = _to_float(record.get(coordinate))
    if value is not None:
        return value
    for measurement in record.get('measurements') or []:
        measurement_value = measurement.get('value') if isinstance(measurement, dict) else None
        if isinstance(measurement_value, dict):
            value = _to_float(measurement_value.get(coordinate))
            if value is not None:
                return value
    return None


def _rapas_description(metadata):
    metadata = metadata if isinstance(metadata, dict) else {}
    fields = (
        ('nature', 'nature'),
        ('redshift', 'redshift'),
        ('host_galaxy', 'host galaxy'),
        ('discovery_magnitude', 'discovery magnitude'),
        ('alert_date', 'alert date'),
        ('rapas_status', 'status'),
        ('alert_end_date', 'alert end date'),
        ('alert_comment', 'comment'),
    )
    details = [
        f'{label} {_clean_text(metadata.get(key))}'
        for key, label in fields
        if _clean_text(metadata.get(key))
    ]
    description = 'RAPAS target'
    if details:
        description += f', {", ".join(details)}'
    return description[:200]


def _to_float(value):
    if isinstance(value, (int, float)):
        number = float(value)
    else:
        text = _clean_text(value).replace('\u00a0', '').replace(' ', '').replace(',', '.')
        if not text:
            return None
        try:
            number = float(text)
        except (TypeError, ValueError):
            return None
    return number if math.isfinite(number) else None


def _to_mjd(value):
    """Parse an MJD with either decimal convention and optional thousands grouping."""
    if isinstance(value, (int, float)):
        candidates = [float(value)]
    else:
        text = _clean_text(value).replace('\u00a0', '').replace(' ', '').replace("'", '')
        if not text:
            return None
        candidates = []
        if ',' in text and '.' in text:
            if text.rfind('.') > text.rfind(','):
                normalized = text.replace(',', '')
            else:
                normalized = text.replace('.', '').replace(',', '.')
            try:
                candidates.append(float(normalized))
            except ValueError:
                return None
        elif ',' in text:
            for normalized in (text.replace(',', '.'), text.replace(',', '')):
                try:
                    candidates.append(float(normalized))
                except ValueError:
                    continue
        else:
            try:
                candidates.append(float(text))
            except ValueError:
                return None

    # Reject implausible values before passing them to astropy. This also resolves
    # ambiguous strings such as "61,119" in favour of MJD 61119, not MJD 61.119.
    return next((number for number in candidates if math.isfinite(number) and 20000 <= number <= 100000), None)


def _excel_date_text(value):
    number = _to_float(value)
    if number is not None and 20000 <= number <= 80000:
        return (datetime(1899, 12, 30) + timedelta(days=number)).strftime('%d/%m/%Y')
    return _clean_text(value)


def _coordinates(ra_value, dec_value):
    ra = _to_float(ra_value)
    dec = _to_float(dec_value)
    if ra is not None and dec is not None:
        return ra, dec
    try:
        coordinate = SkyCoord(_clean_text(ra_value), _clean_text(dec_value))
        return float(coordinate.ra.deg), float(coordinate.dec.deg)
    except Exception:
        return ra, dec


def _column_index(cell_reference):
    letters = re.match(r'[A-Z]+', cell_reference or '')
    if not letters:
        return None
    index = 0
    for char in letters.group(0):
        index = index * 26 + ord(char) - ord('A') + 1
    return index - 1


def _spreadsheet_id(url):
    match = re.search(r'/spreadsheets/d/([^/]+)', _clean_text(url))
    if not match:
        raise ValueError('RAPAS spreadsheet URL must contain /spreadsheets/d/<id>.')
    return match.group(1)


def _download_url(url):
    return f'https://docs.google.com/spreadsheets/d/{_spreadsheet_id(url)}/export?format=xlsx'


def _shared_strings(archive):
    if 'xl/sharedStrings.xml' not in archive.namelist():
        return []
    root = ElementTree.fromstring(archive.read('xl/sharedStrings.xml'))
    return [
        ''.join(node.text or '' for node in item.iter(f'{{{_MAIN_NS}}}t'))
        for item in root.findall(f'{{{_MAIN_NS}}}si')
    ]


def _relationship_targets(archive, path):
    if path not in archive.namelist():
        return {}
    root = ElementTree.fromstring(archive.read(path))
    return {
        relationship.attrib['Id']: relationship.attrib.get('Target', '')
        for relationship in root.findall(f'{{{_REL_NS}}}Relationship')
    }


def _worksheet_rows(archive, worksheet_path, shared_strings):
    root = ElementTree.fromstring(archive.read(worksheet_path))
    rows = []
    for row_node in root.findall(f'.//{{{_MAIN_NS}}}sheetData/{{{_MAIN_NS}}}row'):
        row_number = int(row_node.attrib.get('r', len(rows) + 1))
        while len(rows) < row_number:
            rows.append([])
        values = {}
        for cell in row_node.findall(f'{{{_MAIN_NS}}}c'):
            column = _column_index(cell.attrib.get('r'))
            if column is None or column > 40:
                continue
            cell_type = cell.attrib.get('t')
            value_node = cell.find(f'{{{_MAIN_NS}}}v')
            value = value_node.text if value_node is not None else ''
            if cell_type == 's' and value:
                try:
                    value = shared_strings[int(value)]
                except (IndexError, ValueError):
                    value = ''
            elif cell_type == 'inlineStr':
                value = ''.join(node.text or '' for node in cell.iter(f'{{{_MAIN_NS}}}t'))
            elif cell_type == 'b':
                value = value == '1'
            elif cell_type not in ('str', 'e') and value:
                try:
                    value = float(value)
                except ValueError:
                    pass
            values[column] = value
        if values:
            width = max(values) + 1
            rows[row_number - 1] = [values.get(column, '') for column in range(width)]
    return rows, root


def _worksheet_hyperlinks(archive, worksheet_path, root):
    rels_path = worksheet_path.rsplit('/', 1)
    rels_path = f'{rels_path[0]}/_rels/{rels_path[1]}.rels'
    targets = _relationship_targets(archive, rels_path)
    hyperlinks = {}
    for node in root.findall(f'.//{{{_MAIN_NS}}}hyperlink'):
        relationship_id = node.attrib.get(f'{{{_OFFICE_REL_NS}}}id')
        target = targets.get(relationship_id, '')
        reference = node.attrib.get('ref', '')
        if target and reference:
            hyperlinks[reference] = target
    return hyperlinks


def _cell(row, column):
    return row[column] if column < len(row) else ''


def _metadata_from_rows(rows):
    metadata = {}
    for row in rows[:18]:
        key = _normalized_label(_cell(row, 0))
        if key:
            metadata[key] = _cell(row, 1)
    return metadata


def _metadata_value(metadata, *labels):
    for label in labels:
        value = metadata.get(_normalized_label(label))
        if _clean_text(value):
            return value
    return ''


def _find_measurement_header(rows):
    for index, row in enumerate(rows):
        first = _normalized_label(_cell(row, 0))
        third = _normalized_label(_cell(row, 2))
        if first.startswith('date') and third == 'mjd':
            return index
    return None


def _band_columns(rows, header_index):
    bands = dict(_RAPAS_BANDS)
    for row in rows[max(0, header_index - 3):header_index + 1]:
        for column in (5, 7, 9):
            label = _clean_text(_cell(row, column))
            if '/' not in label:
                continue
            candidate = re.sub(r'[^A-Za-z]', '', label.rsplit('/', 1)[-1]).upper()
            if candidate in {'G', 'GBP', 'GRP'}:
                bands[column] = candidate
    return bands


def _timestamp(row):
    # The workbook's displayed date/time has no reliable timezone. MJD is the
    # authoritative instant and is therefore the only accepted timestamp source.
    mjd = _to_mjd(_cell(row, 2))
    if mjd is not None:
        try:
            return Time(mjd, format='mjd', scale='utc').to_datetime(timezone=timezone.utc), mjd
        except Exception:
            pass
    return None, None


def _sheet_measurements(rows, hyperlinks, year, sheet_name, alert_name, source_label=None):
    header_index = _find_measurement_header(rows)
    if header_index is None:
        return []
    band_columns = _band_columns(rows, header_index)
    measurements = []
    for row_index, row in enumerate(rows[header_index + 1:], start=header_index + 2):
        timestamp, mjd = _timestamp(row)
        if timestamp is None:
            continue
        for magnitude_column, band in band_columns.items():
            magnitude = _to_float(_cell(row, magnitude_column))
            if magnitude is None:
                continue
            error = _to_float(_cell(row, magnitude_column + 1))
            cell_reference = f'R{row_index}'
            spectrum_url = _clean_text(_cell(row, 17)) or hyperlinks.get(cell_reference, '')
            if spectrum_url and urlparse(spectrum_url).scheme not in {'http', 'https'}:
                spectrum_url = ''
            observer = _clean_text(_cell(row, 13))
            comment = _clean_text(_cell(row, 14))
            contributor_key = hashlib.sha1(
                f'{_normalized_name(observer)}|{_normalized_name(comment)}'.encode()
            ).hexdigest()[:12]
            observation_year = timestamp.year
            measurement_namespace = source_label or year or observation_year
            measurement_id = f'{measurement_namespace}:{sheet_name}:{mjd:.8f}:{band}:{contributor_key}'
            value = {
                'filter': f'RAPAS({band})',
                'magnitude': magnitude,
                'error': error,
                'measurement_id': measurement_id,
                'rapas_name': alert_name,
                'rapas_year': observation_year,
                'rapas_sheet': sheet_name,
                'rapas_row': row_index,
                'mjd': mjd,
                'ra': _to_float(_cell(row, 3)),
                'dec': _to_float(_cell(row, 4)),
                'color_index_bp_rp': _to_float(_cell(row, 11)),
                'upper_limit_g': _to_float(_cell(row, 12)),
                'observer': observer,
                'comment': comment,
                'field_area_sq_deg': _clean_text(_cell(row, 15)),
                'additional_info': _clean_text(_cell(row, 16)),
                'spectrum_url': spectrum_url,
            }
            measurements.append({'timestamp': timestamp, 'value': value})
    return measurements


def parse_rapas_workbook(content, year, source_label=None):
    records = []
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        shared_strings = _shared_strings(archive)
        workbook = ElementTree.fromstring(archive.read('xl/workbook.xml'))
        relationships = _relationship_targets(archive, 'xl/_rels/workbook.xml.rels')
        for sheet in workbook.findall(f'.//{{{_MAIN_NS}}}sheet'):
            sheet_name = sheet.attrib.get('name', '')
            if _normalized_label(sheet_name) in {'mode demploi', 'modele'}:
                continue
            relationship_id = sheet.attrib.get(f'{{{_OFFICE_REL_NS}}}id')
            target = relationships.get(relationship_id, '')
            if not target:
                continue
            worksheet_path = target.lstrip('/')
            if not worksheet_path.startswith('xl/'):
                worksheet_path = f'xl/{worksheet_path}'
            if worksheet_path not in archive.namelist():
                continue
            rows, root = _worksheet_rows(archive, worksheet_path, shared_strings)
            metadata = _metadata_from_rows(rows)
            alert_name = _clean_text(_metadata_value(metadata, "Nom de l'alerte", 'Nom alerte')) or sheet_name
            ra, dec = _coordinates(
                _metadata_value(metadata, 'RA (deg dec)', 'RA'),
                _metadata_value(metadata, 'Dec (deg dec)', 'Dec'),
            )
            if not alert_name:
                continue
            hyperlinks = _worksheet_hyperlinks(archive, worksheet_path, root)
            measurements = _sheet_measurements(
                rows,
                hyperlinks,
                year,
                sheet_name,
                alert_name,
                source_label=source_label,
            )
            if not measurements:
                continue
            records.append({
                'name': alert_name,
                'sheet_name': sheet_name,
                'year': year,
                'ra': ra,
                'dec': dec,
                'metadata': {
                    'alert_date': _excel_date_text(_metadata_value(metadata, "Date de l'alerte")),
                    'host_galaxy': _clean_text(_metadata_value(metadata, 'galaxie hote')),
                    'nature': _clean_text(_metadata_value(metadata, 'Nature')),
                    'discovery_magnitude': _to_float(_metadata_value(metadata, 'magnitude decouverte')),
                    'redshift': _to_float(_metadata_value(metadata, 'redshift z')),
                    'alert_comment': _clean_text(_metadata_value(metadata, 'Commentaire')),
                    'rapas_status': _clean_text(_metadata_value(metadata, 'Statut AstroCOLIBRI/RAPAS')),
                    'alert_end_date': _excel_date_text(_metadata_value(metadata, "date de fin de l'alerte")),
                    'rapas_restricted': _to_float(_metadata_value(metadata, 'RAPAS restricted')),
                },
                'measurements': measurements,
            })
    return records


def _configured_spreadsheets():
    configured = getattr(settings, 'RAPAS_SPREADSHEETS', ())
    output = []
    for item in configured:
        if isinstance(item, str):
            output.append({'label': None, 'year': None, 'url': item})
        else:
            output.append({
                'label': item.get('label'),
                'year': item.get('year'),
                'url': item.get('url'),
            })
    return [item for item in output if item.get('url')]


def _rapas_cache_backend():
    try:
        return caches['rapas']
    except Exception:
        return cache


def _fetch_records(cache_only=False):
    """Return parsed records, retaining a stale copy so web queries never need Google Sheets."""
    all_records = []
    refresh_seconds = int(getattr(settings, 'RAPAS_REFRESH_SECONDS', 86400))
    backend = _rapas_cache_backend()
    for spreadsheet in _configured_spreadsheets():
        url = spreadsheet['url']
        year = spreadsheet.get('year')
        source_label = spreadsheet.get('label') or year
        cache_identity = f'{source_label}|{url}'
        cache_key = f'rapas-workbook-{hashlib.sha256(cache_identity.encode()).hexdigest()}'
        refreshed_key = f'{cache_key}-refreshed-at'
        lock_key = f'{cache_key}-refresh-lock'
        records = backend.get(cache_key)
        refreshed_at = _to_float(backend.get(refreshed_key))
        if records is None and backend is not cache:
            # Preserve a warm cache created by releases that used Django's default backend.
            records = cache.get(cache_key)
            if records is not None:
                backend.set(cache_key, records, timeout=None)
                backend.set(refreshed_key, time.time(), timeout=None)
                refreshed_at = time.time()
        is_fresh = refreshed_at is not None and time.time() - refreshed_at < refresh_seconds

        # The combined web query is deliberately cache-only. The daily target refresh (or a
        # direct RAPAS query) owns network refreshes and leaves the last good copy available.
        if records is not None and (cache_only or is_fresh):
            all_records.extend(records)
            continue
        if cache_only:
            logger.warning('RAPAS cache is not populated for spreadsheet %s.', source_label)
            warmup_key = f'{cache_key}-warmup-requested'
            if backend.add(warmup_key, True, timeout=300):
                try:
                    from custom_code.tasks import refresh_rapas_workbook_cache
                    refresh_rapas_workbook_cache.enqueue()
                except Exception:
                    backend.delete(warmup_key)
                    logger.warning('Could not enqueue RAPAS cache warmup.', exc_info=True)
            continue

        acquired_lock = backend.add(lock_key, True, timeout=120)
        if not acquired_lock and records is not None:
            all_records.extend(records)
            continue
        try:
            response = requests.get(_download_url(url), timeout=DATA_SERVICE_HTTP_TIMEOUT)
            response.raise_for_status()
            refreshed_records = parse_rapas_workbook(
                response.content,
                year,
                source_label=source_label,
            )
            if refreshed_records:
                records = refreshed_records
                # No expiry: retain stale data until a successful daily refresh replaces it.
                backend.set(cache_key, records, timeout=None)
                backend.set(refreshed_key, time.time(), timeout=None)
        except Exception:
            if records is None:
                raise
            logger.warning(
                'RAPAS refresh failed for spreadsheet %s; using the last cached workbook.',
                source_label,
                exc_info=True,
            )
        finally:
            if acquired_lock:
                backend.delete(lock_key)
        if records is not None:
            all_records.extend(records)
    return all_records


class RAPASDataService(DataService):
    name = 'RAPAS'
    verbose_name = 'RAPAS'
    update_on_daily_refresh = True
    info_url = ''
    service_notes = 'Match BHTOM targets to RAPAS names or coordinates and import RAPAS photometry.'

    @classmethod
    def get_form_class(cls):
        return RAPASQueryForm

    def build_query_parameters(self, parameters, **kwargs):
        from custom_code.data_services.service_utils import resolve_query_coordinates

        target_name, ra, dec = resolve_query_coordinates(parameters)
        target_names = parameters.get('target_names') or ([target_name] if target_name else [])
        self.query_parameters = {
            'target_id': parameters.get('target_id'),
            'target_name': target_name,
            'target_names': [_clean_text(name) for name in target_names if _clean_text(name)],
            'ra': _to_float(ra),
            'dec': _to_float(dec),
            'radius_arcsec': _to_float(parameters.get('radius_arcsec')) or 5.0,
            'include_photometry': bool(parameters.get('include_photometry', True)),
            'cache_only': bool(parameters.get('_all_data_services_query')),
        }
        return self.query_parameters

    def query_service(self, query_parameters, **kwargs):
        records = _fetch_records(cache_only=query_parameters.get('cache_only', False))
        names = {_normalized_name(name) for name in query_parameters.get('target_names') or []}
        is_target_refresh = bool(query_parameters.get('target_id'))
        matches = []

        # Interactive/general queries support abbreviated and substring searches,
        # e.g. SN2026fvx, 2026fvx, 26fvx, fvx, or 2026. Target refreshes deliberately
        # ignore names and link a BHTOM target to RAPAS by coordinates only.
        if not is_target_refresh:
            matches = [
                record for record in records
                if any(_record_matches_partial_name(record, name) for name in names)
            ]

        if not matches and query_parameters.get('ra') is not None and query_parameters.get('dec') is not None:
            center = SkyCoord(query_parameters['ra'], query_parameters['dec'], unit='deg')
            radius_arcsec = query_parameters.get('radius_arcsec') or 5.0
            candidates = []
            for record in records:
                record_ra = _record_coordinate(record, 'ra')
                record_dec = _record_coordinate(record, 'dec')
                if record_ra is None or record_dec is None:
                    continue
                separation = center.separation(SkyCoord(record_ra, record_dec, unit='deg')).arcsecond
                if separation <= radius_arcsec:
                    candidates.append((separation, record))
            if candidates:
                matches = [min(candidates, key=lambda candidate: candidate[0])[1]]

        self.query_results = {'matches': matches}
        return self.query_results

    def query_targets(self, query_parameters, **kwargs):
        matches = self.query_service(query_parameters, **kwargs).get('matches') or []
        results = []
        for match in matches:
            metadata = match.get('metadata') or {}
            result = {
                'name': match['name'],
                'ra': _record_coordinate(match, 'ra'),
                'dec': _record_coordinate(match, 'dec'),
                'aliases': [{'name': match['name'], 'source_name': self.name}],
                'source_location': '',
                'rapas_metadata': metadata,
                'description': _rapas_description(metadata),
            }
            if query_parameters.get('include_photometry', True):
                datums = []
                for datum in match.get('measurements') or []:
                    value = dict(datum['value'])
                    value.update({key: val for key, val in metadata.items() if val not in (None, '')})
                    datums.append({'timestamp': datum['timestamp'], 'value': value})
                result['reduced_datums'] = {'photometry': datums}
            results.append(result)
        return results

    def create_target_from_query(self, target_result, **kwargs):
        target = Target(
            name=target_result['name'],
            type='SIDEREAL',
            ra=_to_float(target_result.get('ra')),
            dec=_to_float(target_result.get('dec')),
            epoch=2000.0,
        )
        target.description = target_result.get('description') or _rapas_description(
            target_result.get('rapas_metadata')
        )
        return target

    def create_aliases_from_query(self, alias_results, **kwargs):
        return [
            TargetName(name=alias.get('name') if isinstance(alias, dict) else alias)
            for alias in alias_results
            if (alias.get('name') if isinstance(alias, dict) else alias)
        ]

    def create_reduced_datums_from_query(self, target, data=None, data_type=None, **kwargs):
        if data_type != 'photometry' or not data:
            return
        for datum in data:
            ReducedDatum.objects.get_or_create(
                target=target,
                data_type='photometry',
                source_name=self.name,
                timestamp=datum['timestamp'],
                value=datum['value'],
                defaults={'source_location': ''},
            )
