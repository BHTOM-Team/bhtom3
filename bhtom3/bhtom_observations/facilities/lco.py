from django.core.exceptions import ValidationError
from tom_observations.facilities.lco import LCOFacility as BaseLCOFacility, LCOSettings
from tom_observations.models import ObservationRecord

from custom_code.facility_proposals import get_proposal_by_pk


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


class LCOFacility(BaseLCOFacility):
    def _proposal_external_identifier(self, proposal):
        remote_payload = proposal.remote_payload or {}
        for key in ('proposal', 'code', 'id'):
            value = str(remote_payload.get(key) or '').strip()
            if value:
                return value
        return str(proposal.external_id or '').strip()

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
