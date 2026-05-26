from tom_observations.facilities.lco import LCOFacility as BaseLCOFacility, LCOSettings

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
