import json
import logging
import math
import re
from datetime import datetime, timedelta, timezone
from html import escape
from urllib.parse import urljoin, urlparse

import requests
from dateutil.parser import parse
from crispy_forms.layout import Div, HTML, Layout
from django import forms
from django.conf import settings
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.core.files.uploadedfile import SimpleUploadedFile
from tom_observations.facilities.lco import (
    LCOFacility as BaseLCOFacility,
    LCOSettings,
    LCOImagingObservationForm,
    LCOMuscatImagingObservationForm,
    LCOPhotometricSequenceForm,
    LCOSpectroscopyObservationForm,
    LCOSpectroscopicSequenceForm,
)
from tom_observations.facilities.ocs import make_request
from tom_observations.models import ObservationRecord
from tom_dataproducts.models import DataProduct, ReducedDatum
from tom_targets.models import Target

from custom_code.bhtom2_uploads import (
    forward_dataproduct_to_bhtom2,
    has_successful_bhtom2_upload,
    load_extra_data_dict,
    normalize_fits_upload,
    save_extra_data_dict,
)
from custom_code.facility_proposals import get_proposal_by_pk, get_proposal_choices_for_user


logger = logging.getLogger(__name__)
LCO_ARCHIVE_API_URL = 'https://archive-api.lco.global'
LCO_BHTOM2_API_BASE_URL = 'https://bh-tom2.astrouw.edu.pl'
LCO_BHTOM2_OBSERVATORY_LIST_PATH = '/observatory/getObservatoryList/'
LCO_BHTOM2_ONAMES_CACHE_KEY = 'lco_bhtom2_onames_v1'
LCO_BHTOM2_ONAMES_CACHE_SECONDS = 86400
LCO_BHTOM2_AUTOMATED_FILTER = 'GaiaSP/any'
LCO_ETC_TELESCOPE_CLASS_CHOICES = [('0m4', '0.4 m'), ('1m0', '1 m'), ('2m0', '2 m')]
LCO_ETC_FILTER_ORDER = ['U', 'B', 'V', 'R', 'I', 'up', 'gp', 'rp', 'ip', 'zs', 'Y']
LCO_ETC_FILTER_LABELS = {
    'U': 'Bessell-U',
    'B': 'Bessell-B',
    'V': 'Bessell-V',
    'R': 'Bessell-R',
    'I': 'Bessell-I',
    'up': 'SDSS-up',
    'gp': 'SDSS-gp',
    'rp': 'SDSS-rp',
    'ip': 'SDSS-ip',
    'zs': 'Pan-STARRS-zs',
    'Y': 'Pan-STARRS-Y',
}
LCO_ETC_FILTER_INDEX = {'U': 0, 'B': 1, 'V': 2, 'R': 3, 'I': 4, 'up': 5, 'gp': 6, 'rp': 7, 'ip': 8, 'zs': 9, 'Y': 10}
LCO_ETC_TELESCOPE_INDEX = {'0m4': 0, '1m0': 1, '2m0': 3}
LCO_ETC_PIXEL_SCALE = [0.57, 0.389, 0.73, 0.304, 0.27]
LCO_ETC_RON = [14.0, 8.0, 3.0, 11.0, 14.5]
LCO_ETC_DARK = [0.02, 0.002, 0.04, 0.002, 0.005]
LCO_ETC_EXTINCTION = [0.54, 0.23, 0.12, 0.09, 0.04, 0.59, 0.14, 0.08, 0.06, 0.04, 0.03]
LCO_ETC_ZEROPOINT = [
    [18.0, 20.3, 20.7, 21.2, 20.3, 16.11, 21.4, 21.5, 20.75, 19.4, 17.8],
    [21.4, 23.5, 23.5, 23.8, 23.2, 22.45, 24.3, 23.8, 23.5, 22.2, 20.3],
    [0.0, 21.4, 21.4, 21.2, 20.3, 17.5, 21.8, 21.2, 20.1, 18.4, 0.0],
    [21.3, 24.4, 24.6, 24.9, 24.1, 21.4, 25.4, 25.25, 24.75, 23.75, 21.6],
    [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 25.4, 25.2, 24.5, 24.3, 0.0],
]
LCO_ETC_SKY_BRIGHTNESS = [
    [23.0, 22.5, 21.6, 20.6, 19.8, 23.5, 22.0, 21.1, 20.6, 20.2, 19.4],
    [20.0, 20.5, 20.3, 20.0, 18.8, 21.0, 20.3, 20.2, 19.7, 19.2, 18.0],
    [17.0, 17.8, 17.5, 17.4, 17.0, 18.0, 17.6, 17.5, 17.5, 16.8, 16.5],
]
LCO_ETC_DEFAULT_FILTERS_BY_CLASS = {
    '0m4': ['U', 'B', 'V', 'R', 'I', 'up', 'gp', 'rp', 'ip', 'zs'],
    '1m0': ['U', 'B', 'V', 'R', 'I', 'up', 'gp', 'rp', 'ip', 'zs', 'Y'],
    '2m0': ['U', 'B', 'V', 'R', 'I', 'up', 'gp', 'rp', 'ip', 'zs', 'Y'],
}
LCO_ETC_FILTER_ALIASES = {
    'U': ('bessellu', 'johnsonu', 'lcou', 'uvotu', 'u'),
    'B': ('bessellb', 'johnsonb', 'lcob', 'b'),
    'V': ('bessellv', 'johnsonv', 'lcov', 'v'),
    'R': ('bessellr', 'johnsonr', 'lcor', 'r'),
    'I': ('besselli', 'johnsoni', 'lcoi', 'i'),
    'up': ('up', 'sdssu', 'psu', 'skymapperu', 'ztfu', 'uprime'),
    'gp': ('gp', 'sdssg', 'psg', 'ztfg', 'skymapperg', 'gprime', 'gaiag', 'g'),
    'rp': ('rp', 'sdssr', 'psr', 'ztfr', 'skymapperr', 'rprime'),
    'ip': ('ip', 'sdssi', 'psi', 'ztfi', 'skymapperi', 'iprime'),
    'zs': ('zs', 'psz', 'panstarrsz', 'skymapperz', 'zprime', 'z'),
    'Y': ('panstarrsy', 'psy', 'y'),
}
LCO_CAMERA_CODE_TO_INSTRUMENT_KIND = {
    'sq': 'qhy600m',
    'kb': 'sbig6303',
    'fs': 'spectral',
    'ep': 'muscat',
}
LCO_PREFIX_SUFFIX_TO_INSTRUMENT_KIND = {
    'qhy600m': 'qhy600m',
    'sbig6303': 'sbig6303',
    '4k': 'sinistro_4k',
    'spectral': 'spectral',
    'muscat': 'muscat',
}


def _lco_etc_normalize_filter_name(filter_name):
    return re.sub(r'[^a-z0-9]+', '', str(filter_name or '').strip().lower())


def _lco_etc_match_filter_code(filter_name):
    normalized = _lco_etc_normalize_filter_name(filter_name)
    if not normalized:
        return None
    for filter_code in ('up', 'gp', 'rp', 'ip', 'zs', 'Y', 'U', 'B', 'V', 'R', 'I'):
        aliases = LCO_ETC_FILTER_ALIASES[filter_code]
        if any(normalized == alias or normalized.endswith(alias) or alias in normalized for alias in aliases):
            return filter_code
    return None


def _lco_etc_extract_mag_and_filter(datum):
    value = datum.value
    if isinstance(value, dict):
        magnitude = value.get('magnitude', value.get('mag'))
        datum_filter = value.get('filter') or ''
    else:
        magnitude = value
        datum_filter = ''
    try:
        magnitude = float(magnitude)
    except (TypeError, ValueError):
        return None, ''
    if not math.isfinite(magnitude):
        return None, ''
    return magnitude, str(datum_filter or '')


def _lco_etc_latest_magnitudes_by_filter(target_id):
    magnitudes = {}
    datums = ReducedDatum.objects.filter(target_id=target_id, data_type='photometry').order_by('-timestamp', '-id')
    for datum in datums:
        magnitude, datum_filter = _lco_etc_extract_mag_and_filter(datum)
        if magnitude is None:
            continue
        filter_code = _lco_etc_match_filter_code(datum_filter)
        if filter_code and filter_code not in magnitudes:
            magnitudes[filter_code] = round(magnitude, 2)
    return magnitudes


def calculate_lco_etc_exposure_time(telescope_class, filter_code, magnitude, signal_to_noise=100.0, moon_phase=1, airmass=1.3):
    try:
        magnitude = float(magnitude)
        signal_to_noise = float(signal_to_noise)
        airmass = float(airmass)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(magnitude) or not math.isfinite(signal_to_noise) or not math.isfinite(airmass):
        return None
    if signal_to_noise <= 0:
        return None

    telescope_index = LCO_ETC_TELESCOPE_INDEX.get(str(telescope_class or '').strip())
    filter_index = LCO_ETC_FILTER_INDEX.get(filter_code)
    if telescope_index is None or filter_index is None:
        return None

    zeropoint = LCO_ETC_ZEROPOINT[telescope_index][filter_index]
    if zeropoint <= 0:
        return None

    moon_phase_index = max(0, min(2, int(moon_phase)))
    aperture_diameter_arcsec = 3.0
    pixel_scale = LCO_ETC_PIXEL_SCALE[telescope_index]
    readout_noise = LCO_ETC_RON[telescope_index]
    dark_current = LCO_ETC_DARK[telescope_index]
    sky_mag = LCO_ETC_SKY_BRIGHTNESS[moon_phase_index][filter_index]
    extinction = LCO_ETC_EXTINCTION[filter_index]
    aperture_area_arcsec2 = math.pi * aperture_diameter_arcsec * aperture_diameter_arcsec / 4.0
    pixel_count = aperture_area_arcsec2 / (pixel_scale * pixel_scale)
    airmass_correction = (airmass - 1.0) * extinction

    exposure_time = 1.0
    for _step in range(200000):
        mag_at_airmass = magnitude + airmass_correction
        object_electrons_per_sec = 10.0 ** (-0.4 * (mag_at_airmass - zeropoint))
        background_electrons_per_sec_arcsec2 = 10.0 ** (-0.4 * (sky_mag - zeropoint))
        background_electrons_per_sec = background_electrons_per_sec_arcsec2 * aperture_area_arcsec2
        dark_electrons_per_sec = pixel_count * dark_current
        read_noise_electrons = pixel_count * readout_noise * readout_noise

        object_electrons = object_electrons_per_sec * exposure_time
        background_electrons = background_electrons_per_sec * exposure_time
        dark_electrons = dark_electrons_per_sec * exposure_time
        signal_noise = object_electrons / math.sqrt(object_electrons + background_electrons + dark_electrons + read_noise_electrons)
        if signal_noise >= signal_to_noise:
            return int(round(exposure_time))
        exposure_time += 1.0

    return None


def _bhtom2_api_base_url():
    configured = str(getattr(settings, 'BHTOM2_API_BASE_URL', '') or '').strip()
    if configured:
        return configured.rstrip('/')

    upload_service_url = str(getattr(settings, 'BHTOM2_UPLOAD_SERVICE_URL', '') or '').strip()
    if upload_service_url:
        parsed = urlparse(upload_service_url)
        if parsed.scheme and parsed.netloc:
            host = parsed.netloc
            if host.startswith('uploadsvc2.'):
                return f'{parsed.scheme}://{host[len("uploadsvc2.") :]}'
    return LCO_BHTOM2_API_BASE_URL


def _bhtom2_api_timeout():
    try:
        return max(1, int(getattr(settings, 'BHTOM2_API_TIMEOUT', 30)))
    except (TypeError, ValueError):
        return 30


def _normalize_lco_instrument_kind(prefix):
    suffix = str(prefix or '').strip().rsplit('_', 1)[-1].lower()
    return LCO_PREFIX_SUFFIX_TO_INSTRUMENT_KIND.get(suffix, '')


def _extract_site_code_from_observatory_name(name):
    match = re.search(r'\(file code:\s*([a-z0-9]+)\)', str(name or '').lower())
    return match.group(1) if match else ''


def _extract_telescope_class_from_observatory_name(name):
    lower_name = str(name or '').lower()
    if '40-cm' in lower_name or '40cm' in lower_name:
        return '0m4'
    if '1-m' in lower_name or '1m' in lower_name:
        return '1m0'
    if '2-m' in lower_name or '2m' in lower_name:
        return '2m0'
    return ''


def _build_lco_bhtom2_oname_entries(observatories):
    entries = []
    seen = set()
    for observatory in observatories or []:
        observatory_name = str(observatory.get('name') or '').strip()
        if 'LCOGT' not in observatory_name:
            continue
        site_code = _extract_site_code_from_observatory_name(observatory_name)
        telescope_class = _extract_telescope_class_from_observatory_name(observatory_name)
        if not site_code or not telescope_class:
            continue
        for camera in observatory.get('cameras') or []:
            oname = str(camera.get('prefix') or '').strip()
            instrument_kind = _normalize_lco_instrument_kind(oname)
            if not oname or not instrument_kind:
                continue
            key = (site_code, telescope_class, instrument_kind)
            if key in seen:
                continue
            seen.add(key)
            entries.append({
                'site_code': site_code,
                'telescope_class': telescope_class,
                'instrument_kind': instrument_kind,
                'oname': oname,
            })
    return entries


def _fetch_lco_bhtom2_oname_entries(token):
    cached_entries = cache.get(LCO_BHTOM2_ONAMES_CACHE_KEY)
    if cached_entries:
        return cached_entries

    response = requests.post(
        urljoin(f'{_bhtom2_api_base_url()}/', LCO_BHTOM2_OBSERVATORY_LIST_PATH.lstrip('/')),
        json={},
        headers={
            'Accept': 'application/json',
            'Authorization': f'Token {token}',
        },
        timeout=_bhtom2_api_timeout(),
    )
    response.raise_for_status()
    payload = response.json() if response.content else {}
    entries = _build_lco_bhtom2_oname_entries(payload.get('data') or [])
    cache.set(LCO_BHTOM2_ONAMES_CACHE_KEY, entries, LCO_BHTOM2_ONAMES_CACHE_SECONDS)
    return entries


def _normalize_frame_identity_text(value):
    return re.sub(r'[^a-z0-9]+', ' ', str(value or '').strip().lower())


def _infer_lco_instrument_kind(frame, telescope_class, camera_code, basename):
    if camera_code:
        instrument_kind = LCO_CAMERA_CODE_TO_INSTRUMENT_KIND.get(camera_code.lower())
        if instrument_kind:
            return instrument_kind

    if telescope_class == '1m0':
        return 'sinistro_4k'

    candidates = [basename]
    for _key, value in (frame or {}).items():
        if isinstance(value, str):
            candidates.append(value)
    normalized = ' '.join(_normalize_frame_identity_text(candidate) for candidate in candidates)
    for needle, instrument_kind in (
        ('muscat', 'muscat'),
        ('spectral', 'spectral'),
        ('qhy600m', 'qhy600m'),
        ('sbig6303', 'sbig6303'),
        ('sinistro', 'sinistro_4k'),
    ):
        if needle in normalized:
            return instrument_kind
    return ''


def resolve_lco_bhtom2_observatory_oname(frame, token):
    basename = str(frame.get('basename') or '').strip()
    fallback_filename = str(frame.get('filename') or '').strip()
    frame_identity = basename or fallback_filename
    normalized_identity = frame_identity.lower().replace('_', '-')
    match = re.match(r'^(?P<site>[a-z]{3})(?P<telescope>0m4|1m0|2m0)\d+(?:-(?P<camera>[a-z]{2})\d+)?', normalized_identity)
    if not match:
        raise ValueError(f'Could not parse LCO frame identity from "{frame_identity}".')

    site_code = match.group('site')
    telescope_class = match.group('telescope')
    instrument_kind = _infer_lco_instrument_kind(frame, telescope_class, match.group('camera') or '', frame_identity)
    if not instrument_kind:
        raise ValueError(f'Could not infer LCO instrument kind from "{frame_identity}".')

    entries = _fetch_lco_bhtom2_oname_entries(token)
    key = (site_code, telescope_class, instrument_kind)
    for entry in entries:
        if (entry['site_code'], entry['telescope_class'], entry['instrument_kind']) == key:
            return entry['oname']

    raise ValueError(f'No BHTOM2 LCO ONAME matches site={site_code} telescope={telescope_class} instrument={instrument_kind}.')


class AccountLCOSettings(LCOSettings):
    def __init__(self, account=None):
        super().__init__(facility_name='LCO')
        self.account = account

    def get_setting(self, key):
        if self.account:
            if key == 'portal_url':
                return self.account.account_data.get('portal_url', super().get_setting(key))
            if key == 'archive_url':
                return self.account.account_data.get('archive_url', super().get_setting(key))
            if key == 'api_key':
                return self.account.credentials.get('api_key', '')
        return super().get_setting(key)


class BhtomLCOFormMixin:
    def _proposal_for_payload(self, payload):
        proposal_value = payload.get('proposal') or self.cleaned_data.get('proposal')
        return get_proposal_by_pk(proposal_value, facility_code='LCO')

    def _proposal_external_identifier(self, proposal):
        external_id = str(proposal.external_id or '').strip()
        if external_id:
            return external_id
        raise ValidationError(f'LCO proposal "{proposal}" has no remote LCO proposal id. Re-sync LCO proposals and try again.')

    def _facility_settings_for_payload(self, payload, proposal=None):
        proposal = proposal or self._proposal_for_payload(payload)
        if proposal:
            return AccountLCOSettings(account=proposal.account)
        return self.facility_settings

    def _payload_with_external_proposal(self, payload, proposal=None):
        proposal = proposal or self._proposal_for_payload(payload)
        if not proposal:
            return payload
        payload = dict(payload)
        payload['proposal'] = self._proposal_external_identifier(proposal)
        return payload

    def proposal_choices(self):
        user_id = self.initial.get('request_user_id') or self.data.get('request_user_id')
        choices = get_proposal_choices_for_user(user_id, 'LCO', include_account_label=True)
        return choices or [(0, 'No proposals found')]

    def _get_instruments(self):
        cache_key = f'{self.facility_settings.facility_name}_instruments'
        cached_instruments = cache.get(cache_key)
        if cached_instruments:
            return cached_instruments

        timeout = getattr(settings, 'LCO_INSTRUMENTS_TIMEOUT_SECONDS', 8)
        cache_seconds = getattr(settings, 'LCO_INSTRUMENTS_CACHE_SECONDS', 86400)
        try:
            response = requests.get(
                urljoin(self.facility_settings.get_setting('portal_url'), '/api/instruments/'),
                headers={'Authorization': f'Token {self.facility_settings.get_setting("api_key")}'},
                timeout=timeout,
            )
            response.raise_for_status()
            cached_instruments = {key: value for key, value in response.json().items()}
        except Exception as exc:
            logger.warning('Could not load LCO instruments within %ss: %s', timeout, exc)
            cached_instruments = self.facility_settings.default_instrument_config

        cache.set(cache_key, cached_instruments, cache_seconds)
        return cached_instruments

    def _expand_cadence_request(self, payload):
        proposal = self._proposal_for_payload(payload)
        facility_settings = self._facility_settings_for_payload(payload, proposal=proposal)
        payload = self._payload_with_external_proposal(payload, proposal=proposal)
        payload['requests'][0]['cadence'] = {
            'start': self.cleaned_data['start'],
            'end': self.cleaned_data['end'],
            'period': self.cleaned_data['period'],
            'jitter': self.cleaned_data['jitter'],
        }
        payload['requests'][0]['windows'] = []

        response = make_request(
            'POST',
            urljoin(facility_settings.get_setting('portal_url'), '/api/requestgroups/cadence/'),
            json=payload,
            headers={'Authorization': f'Token {facility_settings.get_setting("api_key")}'},
        )
        return response.json()


class BhtomLCOImagingObservationForm(BhtomLCOFormMixin, LCOImagingObservationForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.lco_etc_context = self._build_lco_etc_context()
        self._insert_lco_etc_layout()

    def _build_lco_etc_context(self):
        target_id = self.initial.get('target_id') or self.data.get('target_id')
        fallback_mag = 18.0
        target = None
        if target_id:
            try:
                target = Target.objects.get(pk=target_id)
            except Target.DoesNotExist:
                target = None
        if target and target.mag_last is not None:
            try:
                fallback_mag = float(target.mag_last)
            except (TypeError, ValueError):
                fallback_mag = 18.0
        if not math.isfinite(fallback_mag):
            fallback_mag = 18.0

        latest_magnitudes = _lco_etc_latest_magnitudes_by_filter(target_id) if target_id else {}
        filters_by_class = self._lco_etc_filters_by_class()
        rows_by_class = {}
        for telescope_class, _label in LCO_ETC_TELESCOPE_CLASS_CHOICES:
            rows = []
            for filter_code in filters_by_class.get(telescope_class, []):
                magnitude = latest_magnitudes.get(filter_code, round(fallback_mag, 2))
                exposure_time = calculate_lco_etc_exposure_time(telescope_class, filter_code, magnitude, signal_to_noise=100.0)
                rows.append({
                    'filter_code': filter_code,
                    'filter_label': LCO_ETC_FILTER_LABELS.get(filter_code, filter_code),
                    'magnitude': magnitude,
                    'exposure_time': exposure_time,
                    'supported': exposure_time is not None,
                    'source': 'recent' if filter_code in latest_magnitudes else 'fallback',
                })
            rows_by_class[telescope_class] = rows

        instrument_to_class = {}
        instruments_by_class = {class_code: [] for class_code, _ in LCO_ETC_TELESCOPE_CLASS_CHOICES}
        for instrument_code, instrument in self.get_instruments().items():
            class_code = str(instrument.get('class') or '').strip()
            if class_code in instruments_by_class:
                instrument_to_class[instrument_code] = class_code
                instruments_by_class[class_code].append(instrument_code)

        return {
            'telescope_choices': [{'value': value, 'label': label} for value, label in LCO_ETC_TELESCOPE_CLASS_CHOICES],
            'rows_by_class': rows_by_class,
            'instrument_to_class': instrument_to_class,
            'instruments_by_class': instruments_by_class,
            'selected_telescope_class': '0m4',
            'selected_filter': self._bound_first_filter_code(),
            'selected_instrument': self.data.get('c_1_instrument_type') or self.initial.get('c_1_instrument_type') or '',
        }

    def _bound_first_filter_code(self):
        selected_filter = self.data.get('c_1_ic_1_filter') or self.initial.get('c_1_ic_1_filter') or self.initial.get('filter') or ''
        return str(selected_filter or '').strip()

    def _lco_etc_filters_by_class(self):
        filters_by_class = {class_code: [] for class_code, _ in LCO_ETC_TELESCOPE_CLASS_CHOICES}
        for instrument in self.get_instruments().values():
            class_code = str(instrument.get('class') or '').strip()
            if class_code not in filters_by_class:
                continue
            for filter_entry in instrument.get('optical_elements', {}).get('filters', []):
                if not filter_entry.get('schedulable'):
                    continue
                filter_code = str(filter_entry.get('code') or '').strip()
                if filter_code and filter_code not in filters_by_class[class_code]:
                    filters_by_class[class_code].append(filter_code)
        for class_code, fallback_filters in LCO_ETC_DEFAULT_FILTERS_BY_CLASS.items():
            if not filters_by_class[class_code]:
                filters_by_class[class_code] = list(fallback_filters)
            filters_by_class[class_code] = sorted(filters_by_class[class_code], key=lambda code: LCO_ETC_FILTER_ORDER.index(code) if code in LCO_ETC_FILTER_ORDER else 999)
        return filters_by_class

    def _insert_lco_etc_layout(self):
        context_json = json.dumps(self.lco_etc_context)
        html = f"""
<div class="card mt-3">
  <div class="card-body">
    <h5 class="card-title">LCO Exposure Time Suggestions</h5>
    <p class="text-muted small mb-3">
      Based on the public LCO exposure time calculator logic for imaging, with initial values from the most recent target photometry.
      Default guess: 0.4 m, S/N = 100, airmass = 1.3, half moon.
    </p>
    <div id="lco-etc-widget" data-context='{context_json}'></div>
  </div>
</div>
<script>
(function() {{
  const root = document.getElementById('lco-etc-widget');
  if (!root) return;
  let context;
  try {{
    context = JSON.parse(root.dataset.context || '{{}}');
  }} catch (_err) {{
    return;
  }}

  const FILTER_INDEX = {json.dumps(LCO_ETC_FILTER_INDEX)};
  const TELESCOPE_INDEX = {json.dumps(LCO_ETC_TELESCOPE_INDEX)};
  const PIXEL_SCALE = {json.dumps(LCO_ETC_PIXEL_SCALE)};
  const RON = {json.dumps(LCO_ETC_RON)};
  const DARK = {json.dumps(LCO_ETC_DARK)};
  const ZEROPOINT = {json.dumps(LCO_ETC_ZEROPOINT)};
  const SKY = {json.dumps(LCO_ETC_SKY_BRIGHTNESS)};
  const EXT = {json.dumps(LCO_ETC_EXTINCTION)};
  const filterField = document.getElementById('id_c_1_ic_1_filter');
  const exposureField = document.getElementById('id_c_1_ic_1_exposure_time');
  const instrumentField = document.getElementById('id_c_1_instrument_type');

  function radialIntegrateGauss(radius, sigma) {{
    return 1 - Math.exp(-1 * (radius * radius) / (2 * sigma * sigma));
  }}

  function calculateExposureTime(telescopeClass, filterCode, magnitude, signalToNoise) {{
    const telescopeIndex = TELESCOPE_INDEX[telescopeClass];
    const filterIndex = FILTER_INDEX[filterCode];
    if (telescopeIndex === undefined || filterIndex === undefined) return null;
    const zeropoint = ZEROPOINT[telescopeIndex][filterIndex];
    if (!zeropoint || !Number.isFinite(zeropoint)) return null;
    const mag = Number(magnitude);
    const snr = Number(signalToNoise);
    if (!Number.isFinite(mag) || !Number.isFinite(snr) || snr <= 0) return null;

    const apertureDiameter = 3.0;
    const pixelScale = PIXEL_SCALE[telescopeIndex];
    const readoutNoise = RON[telescopeIndex];
    const darkCurrent = DARK[telescopeIndex];
    const skyMag = SKY[1][filterIndex];
    const extinction = EXT[filterIndex];
    const apertureArea = Math.PI * apertureDiameter * apertureDiameter / 4.0;
    const pixelCount = apertureArea / (pixelScale * pixelScale);
    const airmassCorrection = (1.3 - 1.0) * extinction;
    const sigma = 2.0 / 2.354;
    let exposureTime = 1;

    for (let step = 0; step < 200000; step += 1) {{
      const magAtAirmass = mag + airmassCorrection;
      const objectElectronsPerSec = Math.pow(10, -0.4 * (magAtAirmass - zeropoint));
      const backgroundElectronsPerSecArcsec2 = Math.pow(10, -0.4 * (skyMag - zeropoint));
      const backgroundElectronsPerSec = backgroundElectronsPerSecArcsec2 * apertureArea;
      const darkElectronsPerSec = pixelCount * darkCurrent;
      const readNoiseElectrons = pixelCount * readoutNoise * readoutNoise;
      const objectElectrons = objectElectronsPerSec * exposureTime;
      const backgroundElectrons = backgroundElectronsPerSec * exposureTime;
      const darkElectrons = darkElectronsPerSec * exposureTime;
      const currentSnr = objectElectrons / Math.sqrt(objectElectrons + backgroundElectrons + darkElectrons + readNoiseElectrons);
      radialIntegrateGauss(pixelScale / 2.0, sigma);
      if (currentSnr >= snr) return Math.round(exposureTime);
      exposureTime += 1;
    }}
    return null;
  }}

  root.innerHTML = `
    <div class="form-row align-items-end mb-3">
      <div class="col-md-3">
        <label for="lco-etc-telescope-class">Telescope</label>
        <select id="lco-etc-telescope-class" class="form-control">
          ${{context.telescope_choices.map((choice) => `<option value="${{choice.value}}">${{choice.label}}</option>`).join('')}}
        </select>
      </div>
      <div class="col-md-3">
        <label for="lco-etc-snr">S/N</label>
        <input id="lco-etc-snr" class="form-control" type="number" min="1" step="1" value="100">
      </div>
      <div class="col-md-3">
        <button id="lco-etc-recompute" type="button" class="btn btn-outline-primary">Recompute</button>
      </div>
    </div>
    <div class="table-responsive">
      <table class="table table-sm table-striped">
        <thead>
          <tr>
            <th>Filter</th>
            <th>Magnitude</th>
            <th>Exposure Time [s]</th>
          </tr>
        </thead>
        <tbody id="lco-etc-table-body"></tbody>
      </table>
    </div>
    <p class="small text-muted mb-0">Changing the first configuration filter updates the first exposure-time field from the table.</p>
  `;

  const telescopeSelect = document.getElementById('lco-etc-telescope-class');
  const snrInput = document.getElementById('lco-etc-snr');
  const tableBody = document.getElementById('lco-etc-table-body');
  const recomputeButton = document.getElementById('lco-etc-recompute');

  function currentRows() {{
    return context.rows_by_class[telescopeSelect.value] || [];
  }}

  function syncInstrumentToTelescopeClass() {{
    if (!instrumentField) return;
    const instrumentCodes = context.instruments_by_class[telescopeSelect.value] || [];
    const currentClass = context.instrument_to_class[instrumentField.value || ''];
    if (instrumentCodes.length && currentClass !== telescopeSelect.value) {{
      instrumentField.value = instrumentCodes[0];
    }}
  }}

  function syncTelescopeClassFromInstrument() {{
    if (!instrumentField || !instrumentField.value) return;
    const classCode = context.instrument_to_class[instrumentField.value];
    if (classCode) telescopeSelect.value = classCode;
  }}

  function syncExposureFieldFromSelectedFilter() {{
    if (!filterField || !exposureField) return;
    const selectedFilter = filterField.value;
    const row = currentRows().find((item) => item.filter_code === selectedFilter);
    if (row && row.exposure_time !== null && row.exposure_time !== undefined) {{
      exposureField.value = row.exposure_time;
    }}
  }}

  function renderRows() {{
    const rows = currentRows();
    const snrValue = Number(snrInput.value);
    tableBody.innerHTML = rows.map((row, index) => {{
      const magnitude = Number(row.magnitude);
      const exposure = calculateExposureTime(telescopeSelect.value, row.filter_code, magnitude, snrValue);
      context.rows_by_class[telescopeSelect.value][index].exposure_time = exposure;
      return `
        <tr data-filter-code="${{row.filter_code}}">
          <td>${{row.filter_label}}</td>
          <td><input type="number" class="form-control form-control-sm lco-etc-mag" step="0.01" value="${{Number.isFinite(magnitude) ? magnitude : ''}}" data-index="${{index}}"></td>
          <td class="lco-etc-exp">${{exposure === null || exposure === undefined ? 'n/a' : exposure}}</td>
        </tr>
      `;
    }}).join('');

    tableBody.querySelectorAll('.lco-etc-mag').forEach((input) => {{
      input.addEventListener('input', (event) => {{
        const index = Number(event.target.dataset.index);
        const value = Number(event.target.value);
        if (Number.isFinite(value)) {{
          context.rows_by_class[telescopeSelect.value][index].magnitude = value;
        }}
      }});
    }});

    syncExposureFieldFromSelectedFilter();
  }}

  telescopeSelect.value = context.selected_telescope_class || '0m4';
  syncTelescopeClassFromInstrument();
  syncInstrumentToTelescopeClass();
  renderRows();

  recomputeButton.addEventListener('click', renderRows);
  telescopeSelect.addEventListener('change', () => {{
    syncInstrumentToTelescopeClass();
    renderRows();
  }});
  if (instrumentField) {{
    instrumentField.addEventListener('change', () => {{
      syncTelescopeClassFromInstrument();
      renderRows();
    }});
  }}
  if (filterField) {{
    filterField.addEventListener('change', syncExposureFieldFromSelectedFilter);
    if (context.selected_filter && filterField.value !== context.selected_filter) {{
      filterField.value = context.selected_filter;
    }}
  }}
}})();
</script>
"""
        insert_index = max(len(getattr(self.helper.layout, 'fields', [])) - 1, 0)
        self.helper.layout.insert(insert_index, HTML(html))


class BhtomLCOMonitoringObservationForm(BhtomLCOImagingObservationForm):
    monitoring_filter_codes = LCO_ETC_FILTER_ORDER

    def __init__(self, *args, **kwargs):
        if kwargs.get('data') is not None and hasattr(kwargs['data'], 'copy'):
            kwargs['data'] = kwargs['data'].copy()
        if not kwargs.get('data'):
            initial = dict(kwargs.get('initial') or {})
            start = initial.get('start') or datetime.now(timezone.utc).replace(microsecond=0)
            if isinstance(start, str):
                try:
                    start = datetime.fromisoformat(start)
                except ValueError:
                    start = datetime.now(timezone.utc).replace(microsecond=0)
            initial['start'] = start
            initial['end'] = start + timedelta(days=7)
            target = self._target_from_initial(initial)
            if target and getattr(target, 'cadence', None) is not None:
                initial.setdefault('period', target.cadence)
            else:
                initial.setdefault('period', 1)
            initial.setdefault('jitter', 0)
            initial.setdefault('observation_mode', 'NORMAL')
            initial.setdefault('optimization_type', 'TIME')
            initial.setdefault('configuration_repeats', 1)
            kwargs['initial'] = initial
        super().__init__(*args, **kwargs)
        self._configure_monitoring_fields()
        self._add_monitoring_filter_fields()
        self.helper.layout = self._monitoring_layout()

    def _target_from_initial(self, initial):
        target_id = initial.get('target_id') or self.data.get('target_id')
        if not target_id:
            return None
        try:
            return Target.objects.get(pk=target_id)
        except Target.DoesNotExist:
            return None

    def _configure_monitoring_fields(self):
        self.fields['monitoring_dither_hours'] = forms.FloatField(
            label='Dither (+/- hours)',
            help_text='Hours before and after each cadence center. 6 means a 12-hour request window.',
            initial=1,
            min_value=0,
            required=True,
        )
        datetime_input_formats = [
            '%Y-%m-%dT%H:%M',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
        ]
        for field_name in ('start', 'end'):
            self.fields[field_name] = forms.DateTimeField(
                input_formats=datetime_input_formats,
                widget=forms.DateTimeInput(attrs={'type': 'datetime-local'}, format='%Y-%m-%dT%H:%M'),
                required=True,
            )
        self.fields['period'].label = 'Cadence'
        self.fields['period'].help_text = 'days'
        self.fields['period'].required = True
        self.fields['period'].min_value = 0.01
        self.fields['jitter'].widget = forms.HiddenInput()
        self.fields['jitter'].required = False
        self.fields['optimization_type'].widget = forms.HiddenInput()
        self.fields['optimization_type'].required = False
        self.fields['configuration_repeats'].widget = forms.HiddenInput()
        self.fields['configuration_repeats'].required = False
        self.fields['c_1_configuration_type'].initial = 'EXPOSE'
        self.fields['c_1_configuration_type'].widget = forms.HiddenInput()
        self.fields['c_1_configuration_type'].required = False
        self.fields['c_1_max_airmass'].required = True
        self.fields['c_1_min_lunar_distance'].required = False
        self.fields['c_1_min_lunar_distance'].initial = 30
        self.fields['c_1_max_lunar_phase'].widget = forms.HiddenInput()
        self._configure_monitoring_readout_field()
        for field_name in (
            'dither_pattern', 'dither_num_points', 'dither_point_spacing', 'dither_line_spacing',
            'dither_orientation', 'dither_num_rows', 'dither_num_columns', 'dither_center',
            'mosaic_pattern', 'mosaic_num_points',
            'mosaic_point_overlap', 'mosaic_line_overlap', 'mosaic_orientation', 'mosaic_num_rows',
            'mosaic_num_columns', 'mosaic_center',
        ):
            if field_name in self.fields:
                self.fields[field_name].widget = forms.HiddenInput()
                self.fields[field_name].required = False

    def _monitoring_selected_instrument_type(self):
        field_name = 'c_1_instrument_type'
        value = self.data.get(field_name) or self.initial.get(field_name) or self.fields[field_name].initial
        if value:
            return value
        choices = list(self.fields[field_name].choices)
        return choices[0][0] if choices else ''

    def _readout_mode_choices_for_instrument(self, instrument_type):
        instrument = self.get_instruments().get(instrument_type, {})
        modes = instrument.get('modes', {}).get('readout', {}).get('modes', [])
        if modes:
            return sorted(
                [(mode['code'], mode.get('name') or mode['code']) for mode in modes],
                key=lambda mode_choice: mode_choice[1],
            )
        return list(self.mode_choices('readout'))

    def _monitoring_readout_context(self):
        readout_by_instrument = {}
        default_by_instrument = {}
        for instrument_type in self.get_instruments().keys():
            choices = self._readout_mode_choices_for_instrument(instrument_type)
            readout_by_instrument[instrument_type] = [
                {'value': value, 'label': label}
                for value, label in choices
            ]
            default_by_instrument[instrument_type] = self._default_readout_mode_for_instrument(instrument_type, choices)
        return {
            'readout_by_instrument': readout_by_instrument,
            'default_by_instrument': default_by_instrument,
        }

    def _default_readout_mode_for_instrument(self, instrument_type, choices):
        if not choices:
            return ''
        instrument = self.get_instruments().get(instrument_type, {})
        instrument_label = f'{instrument_type} {instrument.get("name", "")}'.lower()
        preferred_terms = ('qhy600', 'sinistro') if 'qhy600' in instrument_label else ('sinistro',)
        for term in preferred_terms:
            matching = [
                code for code, label in choices
                if term in f'{code} {label}'.lower() and 'central' in f'{code} {label}'.lower()
            ]
            if matching:
                return matching[0]
        return choices[0][0]

    def _configure_monitoring_readout_field(self):
        field_name = 'c_1_ic_1_readout_mode'
        instrument_type = self._monitoring_selected_instrument_type()
        choices = self._readout_mode_choices_for_instrument(instrument_type)
        selected = self.data.get(field_name) or self.initial.get(field_name)
        if selected and selected not in [choice[0] for choice in choices]:
            selected = ''
        self.fields[field_name] = forms.ChoiceField(
            choices=choices,
            initial=selected or self._default_readout_mode_for_instrument(instrument_type, choices),
            required=True,
            label='Readout Mode',
        )

    def _add_monitoring_filter_fields(self):
        rows = self.lco_etc_context.get('rows_by_class', {}).get('0m4', [])
        exposures = {row['filter_code']: row.get('exposure_time') for row in rows}
        magnitudes = {row['filter_code']: row.get('magnitude') for row in rows}
        for filter_code in self.monitoring_filter_codes:
            filter_label = LCO_ETC_FILTER_LABELS.get(filter_code, filter_code)
            self.fields[f'monitoring_mag_{filter_code}'] = forms.FloatField(
                label='',
                initial=magnitudes.get(filter_code, 18.0),
                required=False,
                widget=forms.NumberInput(attrs={
                    'step': '0.01',
                    'class': 'form-control form-control-sm monitoring-mag',
                    'aria-label': f'{filter_label} magnitude',
                }),
            )
            self.fields[f'monitoring_exp_{filter_code}'] = forms.FloatField(
                label='',
                initial=exposures.get(filter_code) or '',
                min_value=0.1,
                required=False,
                widget=forms.NumberInput(attrs={
                    'step': '0.1',
                    'class': 'form-control form-control-sm monitoring-exp',
                    'aria-label': f'{filter_label} exposure time',
                }),
            )
            self.fields[f'monitoring_frames_{filter_code}'] = forms.IntegerField(
                label='',
                initial=0,
                min_value=0,
                required=False,
                widget=forms.NumberInput(attrs={
                    'step': '1',
                    'class': 'form-control form-control-sm monitoring-frames',
                    'aria-label': f'{filter_label} number of frames',
                }),
            )

    def _monitoring_filter_table_html(self):
        def field_value(field_name):
            if self.is_bound:
                values = self.data.getlist(field_name) if hasattr(self.data, 'getlist') else [self.data.get(field_name)]
                return '' if not values else values[-1]
            return self.fields[field_name].initial

        def number_input(field_name, css_class, step, aria_label):
            value = field_value(field_name)
            value_attr = '' if value is None else f' value="{escape(str(value))}"'
            return (
                f'<input type="number" name="{escape(field_name)}" id="id_{escape(field_name)}"'
                f' class="form-control form-control-sm {escape(css_class)}"'
                f' step="{escape(str(step))}" aria-label="{escape(aria_label)}"{value_attr}>'
            )

        rows = []
        for filter_code in self.monitoring_filter_codes:
            label = LCO_ETC_FILTER_LABELS.get(filter_code, filter_code)
            mag_field = f'monitoring_mag_{filter_code}'
            exp_field = f'monitoring_exp_{filter_code}'
            frames_field = f'monitoring_frames_{filter_code}'
            rows.append(f"""
              <tr data-filter-code="{filter_code}">
                <td>{escape(label)}</td>
                <td>{number_input(mag_field, 'monitoring-mag', '0.01', f'{label} magnitude')}</td>
                <td>{number_input(exp_field, 'monitoring-exp', '0.1', f'{label} exposure time')}</td>
                <td>{number_input(frames_field, 'monitoring-frames', '1', f'{label} number of frames')}</td>
              </tr>
            """)
        return f"""
        <div class="table-responsive mt-3">
          <table class="table table-sm table-striped lco-monitoring-table">
            <thead>
              <tr>
                <th>Filter</th>
                <th>Magnitude</th>
                <th>Exposure Time [s]</th>
                <th>Number of Frames</th>
              </tr>
            </thead>
            <tbody>{''.join(rows)}</tbody>
          </table>
        </div>
        """

    def _monitoring_etc_script(self):
        context_json = json.dumps(self.lco_etc_context)
        readout_context_json = json.dumps(self._monitoring_readout_context())
        return f"""
<script>
(function() {{
  const context = {context_json};
  const readoutContext = {readout_context_json};
  const FILTER_INDEX = {json.dumps(LCO_ETC_FILTER_INDEX)};
  const TELESCOPE_INDEX = {json.dumps(LCO_ETC_TELESCOPE_INDEX)};
  const PIXEL_SCALE = {json.dumps(LCO_ETC_PIXEL_SCALE)};
  const RON = {json.dumps(LCO_ETC_RON)};
  const DARK = {json.dumps(LCO_ETC_DARK)};
  const ZEROPOINT = {json.dumps(LCO_ETC_ZEROPOINT)};
  const SKY = {json.dumps(LCO_ETC_SKY_BRIGHTNESS)};
  const EXT = {json.dumps(LCO_ETC_EXTINCTION)};
  const telescopeSelect = document.getElementById('lco-monitoring-telescope-class');
  const snrInput = document.getElementById('lco-monitoring-snr');
  const instrumentField = document.getElementById('id_c_1_instrument_type');
  const readoutField = document.getElementById('id_c_1_ic_1_readout_mode');
  const recomputeButton = document.getElementById('lco-monitoring-recompute');

  function calculateExposureTime(telescopeClass, filterCode, magnitude, signalToNoise) {{
    const telescopeIndex = TELESCOPE_INDEX[telescopeClass];
    const filterIndex = FILTER_INDEX[filterCode];
    if (telescopeIndex === undefined || filterIndex === undefined) return null;
    const zeropoint = ZEROPOINT[telescopeIndex][filterIndex];
    if (!zeropoint || !Number.isFinite(zeropoint)) return null;
    const mag = Number(magnitude);
    const snr = Number(signalToNoise);
    if (!Number.isFinite(mag) || !Number.isFinite(snr) || snr <= 0) return null;
    const apertureArea = Math.PI * 3.0 * 3.0 / 4.0;
    const pixelScale = PIXEL_SCALE[telescopeIndex];
    const pixelCount = apertureArea / (pixelScale * pixelScale);
    const skyMag = SKY[1][filterIndex];
    const airmassCorrection = (1.3 - 1.0) * EXT[filterIndex];
    for (let exposureTime = 1; exposureTime < 200000; exposureTime += 1) {{
      const objectRate = Math.pow(10, -0.4 * ((mag + airmassCorrection) - zeropoint));
      const backgroundRate = Math.pow(10, -0.4 * (skyMag - zeropoint)) * apertureArea;
      const readNoise = pixelCount * RON[telescopeIndex] * RON[telescopeIndex];
      const objectElectrons = objectRate * exposureTime;
      const noise = Math.sqrt(objectElectrons + backgroundRate * exposureTime + pixelCount * DARK[telescopeIndex] * exposureTime + readNoise);
      if (objectElectrons / noise >= snr) return Math.round(exposureTime);
    }}
    return null;
  }}

  function syncInstrumentToTelescopeClass() {{
    if (!instrumentField || !telescopeSelect) return;
    const instrumentCodes = context.instruments_by_class[telescopeSelect.value] || [];
    const currentClass = context.instrument_to_class[instrumentField.value || ''];
    if (instrumentCodes.length && currentClass !== telescopeSelect.value) instrumentField.value = instrumentCodes[0];
  }}

  function syncTelescopeFromInstrument() {{
    if (!instrumentField || !instrumentField.value || !telescopeSelect) return;
    const classCode = context.instrument_to_class[instrumentField.value];
    if (classCode) telescopeSelect.value = classCode;
  }}

  function syncReadoutFromInstrument() {{
    if (!instrumentField || !readoutField) return;
    const instrumentType = instrumentField.value || '';
    const choices = readoutContext.readout_by_instrument[instrumentType] || [];
    const defaultValue = readoutContext.default_by_instrument[instrumentType] || (choices[0] && choices[0].value) || '';
    const currentValue = readoutField.value || defaultValue;
    readoutField.innerHTML = choices.map((choice) => {{
      const option = document.createElement('option');
      option.value = choice.value;
      option.textContent = choice.label;
      if (choice.value === currentValue || (!choices.some((item) => item.value === currentValue) && choice.value === defaultValue)) {{
        option.selected = true;
      }}
      return option.outerHTML;
    }}).join('');
  }}

  function recompute() {{
    document.querySelectorAll('.lco-monitoring-table tbody tr').forEach((row) => {{
      const filterCode = row.dataset.filterCode;
      const magInput = row.querySelector('.monitoring-mag');
      const expInput = row.querySelector('.monitoring-exp');
      const exposure = calculateExposureTime(telescopeSelect.value, filterCode, magInput.value, snrInput.value);
      if (expInput && exposure !== null) expInput.value = exposure;
    }});
  }}

  syncTelescopeFromInstrument();
  syncInstrumentToTelescopeClass();
  syncReadoutFromInstrument();
  if (recomputeButton) recomputeButton.addEventListener('click', recompute);
  if (telescopeSelect) telescopeSelect.addEventListener('change', () => {{ syncInstrumentToTelescopeClass(); syncReadoutFromInstrument(); recompute(); }});
  if (instrumentField) instrumentField.addEventListener('change', () => {{ syncTelescopeFromInstrument(); syncReadoutFromInstrument(); }});
}})();
</script>
"""

    def _monitoring_layout(self):
        telescope_options = ''.join(
            f'<option value="{choice["value"]}">{choice["label"]}</option>'
            for choice in self.lco_etc_context.get('telescope_choices', [])
        )
        calculator_html = f"""
        <div class="card mt-3">
          <div class="card-body">
            <h5 class="card-title">LCO Exposure Time Suggestions</h5>
            <p class="text-muted small mb-3">
              Based on the public LCO exposure time calculator logic for imaging, with initial values from the most recent target photometry.
              Default guess: 0.4 m, S/N = 100, airmass = 1.3, half moon.
            </p>
            <div class="form-row align-items-end mb-3">
              <div class="col-md-3">
                <label for="lco-monitoring-telescope-class">Telescope</label>
                <select id="lco-monitoring-telescope-class" class="form-control">{telescope_options}</select>
              </div>
              <div class="col-md-3">
                <label for="lco-monitoring-snr">S/N</label>
                <input id="lco-monitoring-snr" class="form-control" type="number" min="1" step="1" value="100">
              </div>
              <div class="col-md-3">
                <button id="lco-monitoring-recompute" type="button" class="btn btn-outline-primary">Recompute</button>
              </div>
            </div>
            {self._monitoring_filter_table_html()}
          </div>
        </div>
        {self._monitoring_etc_script()}
        """
        return Layout(
            self.common_layout,
            Div(
                Div('name', css_class='col'),
                Div('proposal', css_class='col'),
                css_class='form-row',
            ),
            Div(
                Div('observation_mode', css_class='col'),
                Div('ipp_value', css_class='col'),
                css_class='form-row',
            ),
            Div('optimization_type', 'configuration_repeats', 'jitter', 'c_1_configuration_type', 'c_1_max_lunar_phase'),
            Div(
                Div('c_1_max_airmass', css_class='col'),
                Div('c_1_min_lunar_distance', css_class='col'),
                css_class='form-row',
            ),
            Div(
                Div('start', css_class='col'),
                Div('end', css_class='col'),
                css_class='form-row',
            ),
            Div(
                Div('period', css_class='col'),
                Div('monitoring_dither_hours', css_class='col'),
                css_class='form-row',
            ),
            Div(
                Div('c_1_instrument_type', css_class='col'),
                Div('c_1_ic_1_readout_mode', css_class='col'),
                css_class='form-row',
            ),
            HTML(calculator_html),
            self.button_layout(),
        )

    def clean_period(self):
        return self.cleaned_data['period']

    def clean(self):
        cleaned_data = super().clean()
        has_selected_filter = any(
            (cleaned_data.get(f'monitoring_frames_{filter_code}') or 0) > 0
            for filter_code in self.monitoring_filter_codes
        )
        if not has_selected_filter:
            raise ValidationError('Select at least one filter by setting Number of Frames greater than 0.')
        return cleaned_data

    def _build_instrument_configs(self, instrument_type, configuration_id):
        instrument_configs = []
        readout_field = self.fields.get('c_1_ic_1_readout_mode')
        readout_mode = self.cleaned_data.get('c_1_ic_1_readout_mode') or (readout_field.initial if readout_field else None)
        for filter_code in self.monitoring_filter_codes:
            exposure_count = self.cleaned_data.get(f'monitoring_frames_{filter_code}') or 0
            exposure_time = self.cleaned_data.get(f'monitoring_exp_{filter_code}')
            if exposure_count <= 0:
                continue
            if not exposure_time:
                raise ValidationError(f'Exposure time is required for {LCO_ETC_FILTER_LABELS.get(filter_code, filter_code)}.')
            instrument_configs.append({
                'exposure_count': exposure_count,
                'exposure_time': exposure_time,
                'mode': readout_mode,
                'optical_elements': {'filter': filter_code},
            })
        return instrument_configs

    def _monitoring_datetime(self, value):
        parsed = parse(value) if isinstance(value, str) else value
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    def validate_at_facility(self):
        if self._errors:
            return
        required_fields = (
            'name', 'proposal', 'ipp_value', 'observation_mode', 'target_id', 'start', 'end', 'period',
            'monitoring_dither_hours', 'c_1_instrument_type', 'c_1_configuration_type', 'c_1_max_airmass',
            'c_1_ic_1_readout_mode',
        )
        if any(field_name not in self.cleaned_data for field_name in required_fields):
            return
        super().validate_at_facility()

    def _build_monitoring_requests(self, configuration):
        try:
            start = self._monitoring_datetime(self.cleaned_data['start'])
            end = self._monitoring_datetime(self.cleaned_data['end'])
            cadence_delta = timedelta(days=float(self.cleaned_data['period']))
            dither_delta = timedelta(hours=float(self.cleaned_data['monitoring_dither_hours']))
        except KeyError as exc:
            raise ValidationError(f'Missing required Monitoring field: {exc.args[0]}') from exc
        requests = []
        center = start
        while center <= end:
            requests.append({
                'optimization_type': self.cleaned_data['optimization_type'],
                'configuration_repeats': self.cleaned_data['configuration_repeats'],
                'configurations': [configuration],
                'windows': [{
                    'start': (center - dither_delta).isoformat(),
                    'end': (center + dither_delta).isoformat(),
                }],
                'location': self._build_location(),
            })
            center += cadence_delta
        return requests

    def _monitoring_schedule_summary(self):
        requests = self._build_monitoring_requests(self._build_configuration(1))
        cadence_days = float(self.cleaned_data['period'])
        dither_hours = float(self.cleaned_data['monitoring_dither_hours'])
        windows = []
        for request in requests[:5]:
            window = request['windows'][0]
            start = self._monitoring_datetime(window['start'])
            end = self._monitoring_datetime(window['end'])
            windows.append(f'{start:%Y-%m-%d %H:%M}-{end:%H:%M} UTC')
        suffix = '; ...' if len(requests) > len(windows) else ''
        return (
            f'Requested schedule: {len(requests)} window(s), cadence {cadence_days:g} day(s), '
            f'dither +/- {dither_hours:g} hour(s), full window {2 * dither_hours:g} hour(s). '
            f'Windows: {"; ".join(windows)}{suffix}'
        )

    def get_validation_message(self):
        message = str(getattr(self, 'validation_message', '') or 'This observation is valid.')
        try:
            schedule_summary = self._monitoring_schedule_summary()
        except Exception as exc:
            logger.warning('Could not build LCO Monitoring schedule summary: %s', exc)
            return message
        return f'{message} {schedule_summary}'

    def observation_payload(self):
        configuration = self._build_configuration(1)
        if not configuration:
            raise ValidationError('Select at least one filter by setting Number of Frames greater than 0.')
        return {
            'name': self.cleaned_data['name'],
            'proposal': self.cleaned_data['proposal'],
            'ipp_value': self.cleaned_data['ipp_value'],
            'operator': 'MANY',
            'observation_type': self.cleaned_data['observation_mode'],
            'requests': self._build_monitoring_requests(configuration),
        }


class BhtomLCOMuscatImagingObservationForm(BhtomLCOFormMixin, LCOMuscatImagingObservationForm):
    pass


class BhtomLCOSpectroscopyObservationForm(BhtomLCOFormMixin, LCOSpectroscopyObservationForm):
    pass


class BhtomLCOPhotometricSequenceForm(BhtomLCOFormMixin, LCOPhotometricSequenceForm):
    pass


class BhtomLCOSpectroscopicSequenceForm(BhtomLCOFormMixin, LCOSpectroscopicSequenceForm):
    pass


class LCOFacility(BaseLCOFacility):
    observation_forms = {
        'IMAGING': BhtomLCOImagingObservationForm,
        'MONITORING': BhtomLCOMonitoringObservationForm,
        'MUSCAT_IMAGING': BhtomLCOMuscatImagingObservationForm,
        'SPECTRA': BhtomLCOSpectroscopyObservationForm,
        'PHOTOMETRIC_SEQUENCE': BhtomLCOPhotometricSequenceForm,
        'SPECTROSCOPIC_SEQUENCE': BhtomLCOSpectroscopicSequenceForm,
    }

    def _missing_remote_status_payload(self):
        return {
            'state': 'CANCELED',
            'scheduled_start': None,
            'scheduled_end': None,
        }

    def _proposal_external_identifier(self, proposal):
        external_id = str(proposal.external_id or '').strip()
        if external_id:
            return external_id
        raise ValidationError(f'LCO proposal "{proposal}" has no remote LCO proposal id. Re-sync LCO proposals and try again.')

    def _proposal_account_facility(self, observation_payload):
        proposal_value = observation_payload.get('proposal') or observation_payload.get('params', {}).get('proposal')
        proposal = get_proposal_by_pk(proposal_value, facility_code='LCO')
        if proposal:
            return proposal, BaseLCOFacility(facility_settings=AccountLCOSettings(account=proposal.account))
        if proposal_value and str(proposal_value).strip().isdigit():
            raise ValidationError(f'LCO proposal {proposal_value} is not available in BHTOM. Re-sync LCO proposals and try again.')
        return None, BaseLCOFacility()

    def _record_account_facility(self, record):
        proposal = get_proposal_by_pk((record.parameters or {}).get('proposal'), facility_code='LCO')
        if proposal:
            return proposal, BaseLCOFacility(facility_settings=AccountLCOSettings(account=proposal.account))
        return None, BaseLCOFacility()

    def submit_observation(self, observation_payload):
        proposal, facility = self._proposal_account_facility(observation_payload)
        payload = dict(observation_payload)
        if proposal:
            payload['proposal'] = self._proposal_external_identifier(proposal)
        return facility.submit_observation(payload)

    def validate_observation(self, observation_payload):
        proposal, facility = self._proposal_account_facility(observation_payload)
        payload = dict(observation_payload)
        if proposal:
            payload['proposal'] = self._proposal_external_identifier(proposal)
        return facility.validate_observation(payload)

    def cancel_observation(self, observation_id):
        record = ObservationRecord.objects.filter(observation_id=observation_id, facility=self.name).order_by('-created').first()
        if record is None:
            return super().cancel_observation(observation_id)
        _, facility = self._record_account_facility(record)
        return facility.cancel_observation(observation_id)

    def update_observation_status(self, observation_id):
        records = ObservationRecord.objects.filter(observation_id=observation_id, facility=self.name)
        if not records:
            raise Exception('No records exist for that observation id')

        for record in records:
            _, facility = self._record_account_facility(record)
            try:
                status = facility.get_observation_status(observation_id)
            except requests.HTTPError as exc:
                response = getattr(exc, 'response', None)
                if getattr(response, 'status_code', None) == 404:
                    logger.warning(
                        'LCO observation %s was not found in the remote portal; marking local record as canceled.',
                        observation_id,
                    )
                    status = self._missing_remote_status_payload()
                else:
                    raise
            previous_status = record.status
            record.status = status['state']
            record.scheduled_start = status['scheduled_start']
            record.scheduled_end = status['scheduled_end']
            record.save()
            logger.info(
                'Updated LCO observation status observation_id=%s record_id=%s target_id=%s previous_status=%s new_status=%s scheduled_start=%s scheduled_end=%s',
                record.observation_id,
                record.pk,
                record.target_id,
                previous_status,
                record.status,
                record.scheduled_start,
                record.scheduled_end,
            )
            if record.status == 'COMPLETED':
                try:
                    result = self._sync_completed_lco_dataproducts(record, facility.facility_settings)
                    logger.info(
                        'Automatic LCO processing finished for observation %s: %s',
                        record.observation_id,
                        result,
                    )
                except Exception as exc:
                    logger.warning(
                        'Automatic LCO data sync failed for observation %s: %s',
                        record.observation_id,
                        exc,
                    )

    def process_completed_observation(self, record):
        if record.facility != self.name:
            raise ValueError(f'Observation {record.pk} is not an {self.name} observation.')
        if str(record.status or '').strip() != 'COMPLETED':
            raise ValueError(f'Observation {record.observation_id} is not completed yet.')

        _, facility = self._record_account_facility(record)
        result = self._sync_completed_lco_dataproducts(record, facility.facility_settings, force=True)
        logger.info('Manual LCO processing finished for observation %s: %s', record.observation_id, result)
        return result

    def _archive_api_url(self, path):
        root_url = str(getattr(settings, 'LCO_ARCHIVE_API_URL', LCO_ARCHIVE_API_URL) or LCO_ARCHIVE_API_URL).rstrip('/')
        return f'{root_url}/{str(path).lstrip("/")}'

    def _archive_timeout(self):
        try:
            return max(1, int(getattr(settings, 'LCO_ARCHIVE_TIMEOUT_SECONDS', 30)))
        except (TypeError, ValueError):
            return 30

    def _archive_headers(self, api_key):
        return {'Authorization': f'Token {api_key}'}

    def _iter_completed_archive_frames(self, observation_id, api_key):
        next_url = self._archive_api_url('/frames/')
        params = {
            'request_id': observation_id,
            'reduction_level': 91,
            'configuration_type': 'EXPOSE',
            'public': 'false',
            'limit': 100,
        }
        while next_url:
            response = requests.get(
                next_url,
                params=params,
                headers=self._archive_headers(api_key),
                timeout=self._archive_timeout(),
            )
            response.raise_for_status()
            payload = response.json()
            for frame in payload.get('results') or []:
                yield frame
            next_url = payload.get('next')
            params = None

    def _frame_filename(self, frame):
        basename = str(frame.get('basename') or '').strip()
        extension = str(frame.get('extension') or '').strip()
        if basename and extension:
            return f'{basename}{extension}'
        return basename or f'lco-frame-{frame.get("id")}.fits'

    def _normalized_frame_filename(self, frame):
        filename = self._frame_filename(frame)
        lower_name = filename.lower()
        if lower_name.endswith('.fits.fz') or lower_name.endswith('.fits.gz'):
            return filename[:-3]
        if lower_name.endswith('.fz') or lower_name.endswith('.gz'):
            return f'{filename[:-3]}.fits'
        if lower_name.endswith('.fit') or lower_name.endswith('.fts') or lower_name.endswith('.ftt') or lower_name.endswith('.ftsc'):
            return filename
        if lower_name.endswith('.fits'):
            return filename
        return f'{filename}.fits'

    def _create_lco_dataproduct(self, record, frame, api_key, *, force=False):
        frame_id = str(frame.get('id') or '').strip()
        if not frame_id:
            raise ValueError(f'Missing LCO archive frame id for observation {record.observation_id}.')

        existing = DataProduct.objects.filter(observation_record=record, product_id=frame_id).order_by('-created').first()
        created_new = existing is None
        if existing is not None and not force:
            return existing, False

        download_url = str(frame.get('url') or '').strip()
        if not download_url:
            raise ValueError(f'Missing download url for LCO archive frame {frame_id}.')

        download_response = requests.get(
            download_url,
            timeout=self._archive_timeout(),
        )
        download_response.raise_for_status()
        logger.info(
            'Downloaded LCO frame frame_id=%s observation_id=%s source_name=%s bytes=%s',
            frame_id,
            record.observation_id,
            self._frame_filename(frame),
            len(download_response.content),
        )

        uploaded_file = SimpleUploadedFile(
            self._frame_filename(frame),
            download_response.content,
            content_type='application/fits',
        )
        normalized_file, normalization_metadata = normalize_fits_upload(uploaded_file)
        normalized_file.name = self._normalized_frame_filename(frame)
        normalized_file.seek(0)
        logger.info(
            'Normalized LCO frame frame_id=%s observation_id=%s normalized_name=%s metadata=%s',
            frame_id,
            record.observation_id,
            normalized_file.name,
            normalization_metadata,
        )

        dataproduct = existing or DataProduct(
            target=record.target,
            observation_record=record,
            product_id=frame_id,
            data_product_type='fits_file',
        )
        dataproduct.target = record.target
        dataproduct.observation_record = record
        dataproduct.product_id = frame_id
        dataproduct.data_product_type = 'fits_file'
        dataproduct.data.save(normalized_file.name, normalized_file, save=False)
        dataproduct.save()
        logger.info(
            'Saved LCO dataproduct frame_id=%s observation_id=%s dataproduct_id=%s stored_name=%s',
            frame_id,
            record.observation_id,
            dataproduct.pk,
            dataproduct.get_file_name(),
        )

        metadata = load_extra_data_dict(dataproduct)
        metadata['lco_archive_frame'] = {
            'frame_id': frame_id,
            'request_id': str(frame.get('request_id') or record.observation_id),
            'observation_id': str(frame.get('observation_id') or ''),
            'basename': str(frame.get('basename') or ''),
            'filename': normalized_file.name,
            'reduction_level': frame.get('reduction_level'),
            'normalization': normalization_metadata,
        }
        save_extra_data_dict(dataproduct, metadata)

        return dataproduct, created_new

    def _sync_completed_lco_dataproducts(self, record, facility_settings, *, force=False):
        archive_api_key = str(facility_settings.get_setting('api_key') or '').strip()
        if not archive_api_key:
            logger.warning('Skipping LCO archive sync for observation %s because no LCO API key is configured.', record.observation_id)
            return {'frames_seen': 0, 'created': 0, 'forwarded': 0, 'already_forwarded': 0}

        bhtom2_token = str(getattr(settings, 'BHTOM2_API_TOKEN', '') or '').strip()
        if not bhtom2_token:
            logger.warning('Skipping automatic BHTOM2 forwarding for observation %s because BHTOM2_API_TOKEN is empty.', record.observation_id)
            return {'frames_seen': 0, 'created': 0, 'forwarded': 0, 'already_forwarded': 0}

        logger.info('Starting LCO archive sync for observation %s.', record.observation_id)
        result = {
            'frames_seen': 0,
            'created': 0,
            'forwarded': 0,
            'already_forwarded': 0,
            'refreshed': 0,
        }
        for frame in self._iter_completed_archive_frames(record.observation_id, archive_api_key):
            result['frames_seen'] += 1
            logger.info(
                'Processing LCO frame frame_id=%s observation_id=%s filename=%s reduction_level=%s',
                frame.get('id'),
                record.observation_id,
                self._frame_filename(frame),
                frame.get('reduction_level'),
            )
            dataproduct, created_new = self._create_lco_dataproduct(record, frame, archive_api_key, force=force)
            if created_new:
                result['created'] += 1
            elif force:
                result['refreshed'] += 1
            if not force and not created_new and has_successful_bhtom2_upload(dataproduct):
                result['already_forwarded'] += 1
                logger.info(
                    'Skipping already-forwarded LCO dataproduct frame_id=%s observation_id=%s dataproduct_id=%s',
                    frame.get('id'),
                    record.observation_id,
                    dataproduct.pk,
                )
                continue
            observatory_oname = resolve_lco_bhtom2_observatory_oname(frame, bhtom2_token)
            forward_dataproduct_to_bhtom2(
                dataproduct,
                token=bhtom2_token,
                observatory=observatory_oname,
                calibration_filter=LCO_BHTOM2_AUTOMATED_FILTER,
                comment=f'Uploaded automatically from BHTOM3 LCO observation {record.observation_id}',
                user_id=record.user_id,
            )
            result['forwarded'] += 1
            logger.info(
                'Forwarded LCO dataproduct frame_id=%s observation_id=%s dataproduct_id=%s upload_name=%s observatory=%s',
                frame.get('id'),
                record.observation_id,
                dataproduct.pk,
                dataproduct.get_file_name(),
                observatory_oname,
            )
        logger.info('Finished LCO archive sync for observation %s: %s', record.observation_id, result)
        return result
