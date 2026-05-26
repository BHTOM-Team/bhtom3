from django import forms

from custom_code.facility_proposals import get_manageable_users, get_or_create_hidden_account, sync_remote_proposals_for_account
from custom_code.models import FacilityAccount, FacilityProposal


def _build_dynamic_field(field_spec):
    field_type = field_spec.get('type', 'string')
    label = field_spec.get('label') or field_spec.get('name', '').replace('_', ' ').title()
    required = bool(field_spec.get('required'))
    help_text = field_spec.get('help_text', '')
    initial = field_spec.get('initial')

    if field_type == 'secret':
        field = forms.CharField(
            label=label,
            required=required,
            help_text=help_text,
            initial=initial,
            widget=forms.PasswordInput(render_value=True),
        )
    elif field_type == 'boolean':
        field = forms.BooleanField(label=label, required=False, help_text=help_text, initial=bool(initial))
    else:
        field = forms.CharField(label=label, required=required, help_text=help_text, initial=initial)
    return field


class FacilityAccountForm(forms.Form):
    label = forms.CharField(max_length=128)
    shared_users = forms.ModelMultipleChoiceField(queryset=None, required=False)

    def __init__(self, *args, facility, user, account=None, **kwargs):
        self.facility = facility
        self.user = user
        self.account = account
        super().__init__(*args, **kwargs)
        self.fields['shared_users'].queryset = get_manageable_users(exclude_user=user)
        self._schema_fields = []

        for field_spec in facility.account_schema.get('fields', []):
            field_name = f"schema__{field_spec['name']}"
            self.fields[field_name] = _build_dynamic_field(field_spec)
            self._schema_fields.append((field_name, field_spec))

        if account:
            self.fields['label'].initial = account.label
            shared_ids = account.memberships.exclude(user=user).values_list('user_id', flat=True)
            self.fields['shared_users'].initial = list(shared_ids)
            for field_name, field_spec in self._schema_fields:
                value = account.credentials.get(field_spec['name']) if field_spec.get('type') == 'secret' else account.account_data.get(field_spec['name'])
                if value not in (None, ''):
                    self.fields[field_name].initial = value

    def save(self):
        account = self.account or FacilityAccount(facility=self.facility, created_by=self.user)
        account.label = self.cleaned_data['label']
        account.account_data = {}
        account.credentials = {}
        for field_name, field_spec in self._schema_fields:
            value = self.cleaned_data.get(field_name)
            if value in (None, ''):
                continue
            if field_spec.get('type') == 'secret':
                account.credentials[field_spec['name']] = value
            else:
                account.account_data[field_spec['name']] = value
        account.save()
        return account


class FacilityProposalForm(forms.Form):
    external_id = forms.CharField(max_length=128, label='Proposal ID')
    title = forms.CharField(max_length=255, required=False)
    is_active = forms.BooleanField(required=False, initial=True)
    shared_users = forms.ModelMultipleChoiceField(queryset=None, required=False)

    def __init__(self, *args, account, user, proposal=None, **kwargs):
        self.account = account
        self.user = user
        self.proposal = proposal
        super().__init__(*args, **kwargs)
        self.fields['shared_users'].queryset = get_manageable_users(exclude_user=user)
        self._schema_fields = []

        for field_spec in account.facility.proposal_schema.get('fields', []):
            if field_spec['name'] == 'proposal_id':
                continue
            field_name = f"details__{field_spec['name']}"
            self.fields[field_name] = _build_dynamic_field(field_spec)
            self._schema_fields.append((field_name, field_spec))

        if proposal:
            self.fields['external_id'].initial = proposal.external_id
            self.fields['title'].initial = proposal.title
            self.fields['is_active'].initial = proposal.is_active
            shared_ids = proposal.memberships.exclude(user=user).values_list('user_id', flat=True)
            self.fields['shared_users'].initial = list(shared_ids)
            for field_name, field_spec in self._schema_fields:
                value = proposal.details.get(field_spec['name'])
                if value not in (None, ''):
                    self.fields[field_name].initial = value

    def save(self):
        proposal = self.proposal or FacilityProposal(account=self.account)
        proposal.external_id = self.cleaned_data['external_id']
        proposal.title = self.cleaned_data.get('title', '')
        proposal.is_active = self.cleaned_data.get('is_active', True)
        proposal.details = {}
        for field_name, field_spec in self._schema_fields:
            value = self.cleaned_data.get(field_name)
            if value in (None, ''):
                continue
            proposal.details[field_spec['name']] = value
        proposal.save()
        return proposal


class DirectFacilityProposalForm(forms.Form):
    external_id = forms.CharField(max_length=128, label='Proposal ID')
    title = forms.CharField(max_length=255, required=False)
    is_active = forms.BooleanField(required=False, initial=True)
    shared_users = forms.ModelMultipleChoiceField(queryset=None, required=False)

    def __init__(self, *args, facility, user, proposal=None, **kwargs):
        self.facility = facility
        self.user = user
        self.proposal = proposal
        super().__init__(*args, **kwargs)
        self.fields['shared_users'].queryset = get_manageable_users(exclude_user=user)
        self._schema_fields = []

        for field_spec in facility.proposal_schema.get('fields', []):
            if field_spec['name'] == 'proposal_id':
                continue
            field_name = f"details__{field_spec['name']}"
            self.fields[field_name] = _build_dynamic_field(field_spec)
            self._schema_fields.append((field_name, field_spec))

        if proposal:
            self.fields['external_id'].initial = proposal.external_id
            self.fields['title'].initial = proposal.title
            self.fields['is_active'].initial = proposal.is_active
            shared_ids = proposal.memberships.exclude(user=user).values_list('user_id', flat=True)
            self.fields['shared_users'].initial = list(shared_ids)
            for field_name, field_spec in self._schema_fields:
                value = proposal.details.get(field_spec['name'])
                if value not in (None, ''):
                    self.fields[field_name].initial = value

    def save(self):
        if self.proposal:
            proposal = self.proposal
        else:
            account = get_or_create_hidden_account(
                facility=self.facility,
                owner=self.user,
                credentials={},
                account_data={},
                label=f'{self.facility.code} proposal {self.cleaned_data["external_id"]}',
            )
            proposal = FacilityProposal(account=account)

        proposal.external_id = self.cleaned_data['external_id']
        proposal.title = self.cleaned_data.get('title', '')
        proposal.is_active = self.cleaned_data.get('is_active', True)
        proposal.details = {}
        for field_name, field_spec in self._schema_fields:
            value = self.cleaned_data.get(field_name)
            if value in (None, ''):
                continue
            proposal.details[field_spec['name']] = value
        proposal.save()
        return proposal


class LCOProposalImportForm(forms.Form):
    api_key = forms.CharField(widget=forms.PasswordInput(render_value=True), label='API key')
    shared_users = forms.ModelMultipleChoiceField(queryset=None, required=False)

    def __init__(self, *args, user, **kwargs):
        self.user = user
        super().__init__(*args, **kwargs)
        self.fields['shared_users'].queryset = get_manageable_users(exclude_user=user)

    def save(self):
        facility = self.initial['facility']
        account = get_or_create_hidden_account(
            facility=facility,
            owner=self.user,
            credentials={'api_key': self.cleaned_data['api_key']},
            account_data={},
            label=f'LCO API key for {self.user.username}',
        )
        return sync_remote_proposals_for_account(
            account,
            owner=self.user,
            shared_users=list(self.cleaned_data['shared_users']),
        )
