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
    def _proposal_account_facility(self, observation_payload):
        proposal = get_proposal_by_pk(observation_payload.get('proposal') or observation_payload.get('params', {}).get('proposal'), facility_code='LCO')
        if proposal:
            return proposal, BaseLCOFacility(facility_settings=AccountLCOSettings(account=proposal.account))
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
            payload['proposal'] = proposal.external_id
        return facility.submit_observation(payload)

    def validate_observation(self, observation_payload):
        proposal, facility = self._proposal_account_facility(observation_payload)
        payload = dict(observation_payload)
        if proposal:
            payload['proposal'] = proposal.external_id
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
