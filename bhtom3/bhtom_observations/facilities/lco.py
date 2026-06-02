import logging
from urllib.parse import urljoin

import requests
from django.conf import settings
from django.core.cache import cache
from django.core.exceptions import ValidationError
from tom_observations.facilities.lco import (
    LCOFacility as BaseLCOFacility,
    LCOSettings,
    LCOImagingObservationForm,
    LCOMuscatImagingObservationForm,
    LCOPhotometricSequenceForm,
    LCOSpectroscopyObservationForm,
    LCOSpectroscopicSequenceForm,
)
from tom_observations.models import ObservationRecord

from custom_code.facility_proposals import get_proposal_by_pk, get_proposal_choices_for_user


logger = logging.getLogger(__name__)


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


class BhtomLCOImagingObservationForm(BhtomLCOFormMixin, LCOImagingObservationForm):
    pass


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
        'MUSCAT_IMAGING': BhtomLCOMuscatImagingObservationForm,
        'SPECTRA': BhtomLCOSpectroscopyObservationForm,
        'PHOTOMETRIC_SEQUENCE': BhtomLCOPhotometricSequenceForm,
        'SPECTROSCOPIC_SEQUENCE': BhtomLCOSpectroscopicSequenceForm,
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
            status = facility.get_observation_status(observation_id)
            record.status = status['state']
            record.scheduled_start = status['scheduled_start']
            record.scheduled_end = status['scheduled_end']
            record.save()
