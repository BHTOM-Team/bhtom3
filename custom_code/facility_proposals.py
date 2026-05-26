from django.contrib.auth import get_user_model
from django.db.models import Q
from django.utils import timezone
import requests
from dateutil.parser import parse as parse_datetime

from custom_code.models import (
    Facility,
    FacilityAccount,
    FacilityAccountMembership,
    FacilityProposal,
    FacilityProposalMembership,
)


DEFAULT_FACILITY_DEFINITIONS = [
    {
        'code': 'LCO',
        'name': 'Las Cumbres Observatory',
        'supports_remote_proposal_sync': True,
        'account_schema': {
            'fields': [
                {'name': 'api_key', 'label': 'API key', 'type': 'secret', 'required': True},
            ],
        },
        'proposal_schema': {
            'fields': [],
            'source': 'remote_sync',
        },
    },
    {
        'code': 'LT',
        'name': 'Liverpool Telescope',
        'supports_remote_proposal_sync': False,
        'account_schema': {
            'fields': [
                {'name': 'username', 'label': 'Username', 'type': 'string', 'required': True},
                {'name': 'password', 'label': 'Password', 'type': 'secret', 'required': True},
                {'name': 'host', 'label': 'Host', 'type': 'string', 'required': False},
                {'name': 'port', 'label': 'Port', 'type': 'string', 'required': False},
            ],
        },
        'proposal_schema': {
            'fields': [
                {'name': 'proposal_id', 'label': 'Proposal ID', 'type': 'string', 'required': True},
            ],
            'source': 'manual',
        },
    },
    {
        'code': 'REM',
        'name': 'Rapid Eye Mount',
        'supports_remote_proposal_sync': False,
        'account_schema': {'fields': []},
        'proposal_schema': {
            'fields': [
                {'name': 'proposal_id', 'label': 'Proposal ID', 'type': 'string', 'required': True},
                {'name': 'notification_email', 'label': 'Notification email', 'type': 'string', 'required': True},
                {'name': 'pi_name', 'label': 'PI name', 'type': 'string', 'required': False},
                {'name': 'description', 'label': 'Description', 'type': 'string', 'required': False},
            ],
            'source': 'manual',
        },
    },
    {
        'code': 'SUHORA',
        'name': 'Suhora Observatory',
        'supports_remote_proposal_sync': False,
        'account_schema': {'fields': []},
        'proposal_schema': {
            'fields': [
                {'name': 'proposal_id', 'label': 'Proposal ID', 'type': 'string', 'required': True},
                {'name': 'notification_email', 'label': 'Notification email', 'type': 'string', 'required': True},
            ],
            'source': 'manual',
        },
    },
    {
        'code': 'BOLECINA',
        'name': 'Bolecina Observatory',
        'supports_remote_proposal_sync': False,
        'account_schema': {'fields': []},
        'proposal_schema': {
            'fields': [
                {'name': 'proposal_id', 'label': 'Proposal ID', 'type': 'string', 'required': True},
                {'name': 'notification_email', 'label': 'Notification email', 'type': 'string', 'required': True},
            ],
            'source': 'manual',
        },
    },
    {
        'code': 'LESEDI',
        'name': 'Lesedi Telescope',
        'supports_remote_proposal_sync': False,
        'account_schema': {'fields': []},
        'proposal_schema': {
            'fields': [
                {'name': 'proposal_id', 'label': 'Proposal ID', 'type': 'string', 'required': True},
                {'name': 'notification_email', 'label': 'Notification email', 'type': 'string', 'required': True},
            ],
            'source': 'manual',
        },
    },
    {
        'code': 'GEM',
        'name': 'Gemini',
        'supports_remote_proposal_sync': False,
        'account_schema': {
            'fields': [
                {'name': 'api_key_gs', 'label': 'GS API key', 'type': 'secret', 'required': False},
                {'name': 'api_key_gn', 'label': 'GN API key', 'type': 'secret', 'required': False},
                {'name': 'user_email', 'label': 'User email', 'type': 'string', 'required': False},
            ],
        },
        'proposal_schema': {
            'fields': [
                {'name': 'program_id', 'label': 'Program ID', 'type': 'string', 'required': True},
                {'name': 'mode', 'label': 'Mode', 'type': 'string', 'required': False},
            ],
            'source': 'manual',
        },
    },
    {
        'code': 'SWIFT',
        'name': 'Swift',
        'supports_remote_proposal_sync': False,
        'account_schema': {
            'fields': [
                {'name': 'username', 'label': 'Username', 'type': 'string', 'required': True},
                {'name': 'shared_secret', 'label': 'Shared secret', 'type': 'secret', 'required': True},
            ],
        },
        'proposal_schema': {'fields': [], 'source': 'none'},
    },
]


def ensure_default_facilities():
    for payload in DEFAULT_FACILITY_DEFINITIONS:
        Facility.objects.update_or_create(code=payload['code'], defaults=payload)


def _parse_remote_datetime(value):
    if not value:
        return None
    try:
        parsed = parse_datetime(str(value))
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        return timezone.make_aware(parsed, timezone.utc)
    return parsed.astimezone(timezone.utc)


def _extract_proposal_window(remote_payload):
    valid_from = None
    valid_until = None
    for key in ('start', 'starts', 'start_date', 'start_time', 'active_from', 'valid_from'):
        valid_from = valid_from or _parse_remote_datetime(remote_payload.get(key))
    for key in ('end', 'ends', 'end_date', 'end_time', 'active_until', 'expires', 'valid_until'):
        valid_until = valid_until or _parse_remote_datetime(remote_payload.get(key))
    return valid_from, valid_until


def _coerce_user(user_or_id):
    if getattr(user_or_id, 'is_authenticated', False):
        return user_or_id
    if user_or_id in (None, ''):
        return None
    user_model = get_user_model()
    try:
        return user_model.objects.get(pk=int(user_or_id))
    except (ValueError, TypeError, user_model.DoesNotExist):
        return None


def get_manageable_users(exclude_user=None):
    queryset = get_user_model().objects.filter(is_active=True).order_by('username')
    if exclude_user is not None and getattr(exclude_user, 'pk', None):
        queryset = queryset.exclude(pk=exclude_user.pk)
    return queryset


def get_accessible_facilities(user):
    user = _coerce_user(user)
    if not user:
        return Facility.objects.none()
    if user.is_superuser:
        return Facility.objects.filter(is_active=True)
    return Facility.objects.filter(
        is_active=True,
    ).filter(
        Q(accounts__memberships__user=user) |
        Q(accounts__proposals__memberships__user=user)
    ).distinct()


def get_accessible_accounts(user, facility_code=None):
    user = _coerce_user(user)
    if not user:
        return FacilityAccount.objects.none()
    queryset = FacilityAccount.objects.select_related('facility', 'created_by').prefetch_related(
        'memberships__user',
        'proposals__memberships__user',
    ).filter(is_active=True)
    if facility_code:
        queryset = queryset.filter(facility__code=facility_code)
    if user.is_superuser:
        return queryset
    return queryset.filter(
        Q(memberships__user=user) |
        Q(proposals__memberships__user=user)
    ).distinct()


def get_manageable_accounts(user, facility_code=None):
    user = _coerce_user(user)
    queryset = get_accessible_accounts(user, facility_code=facility_code)
    if not user or user.is_superuser:
        return queryset
    return queryset.filter(
        memberships__user=user,
        memberships__role__in=[
            FacilityAccountMembership.Role.OWNER,
            FacilityAccountMembership.Role.EDITOR,
        ],
    ).distinct()


def get_accessible_proposals(user, facility_code=None):
    user = _coerce_user(user)
    if not user:
        return FacilityProposal.objects.none()
    queryset = FacilityProposal.objects.select_related(
        'account',
        'account__facility',
        'account__created_by',
    ).prefetch_related('memberships__user').filter(is_active=True)
    if facility_code:
        queryset = queryset.filter(account__facility__code=facility_code)
    if user.is_superuser:
        return queryset
    return queryset.filter(
        Q(account__memberships__user=user) |
        Q(memberships__user=user)
    ).distinct()


def get_manageable_proposals(user, facility_code=None):
    user = _coerce_user(user)
    queryset = get_accessible_proposals(user, facility_code=facility_code)
    if not user or user.is_superuser:
        return queryset
    return queryset.filter(
        Q(memberships__user=user, memberships__role__in=[
            FacilityProposalMembership.Role.OWNER,
            FacilityProposalMembership.Role.EDITOR,
        ]) |
        Q(account__memberships__user=user, account__memberships__role__in=[
            FacilityAccountMembership.Role.OWNER,
            FacilityAccountMembership.Role.EDITOR,
        ])
    ).distinct()


def get_first_account_for_user(user, facility_code):
    return get_accessible_accounts(user, facility_code=facility_code).order_by('label', 'pk').first()


def get_account_for_user(user, account_id):
    return get_accessible_accounts(user).filter(pk=account_id).first()


def get_manageable_account_for_user(user, account_id):
    return get_manageable_accounts(user).filter(pk=account_id).first()


def get_proposal_for_user(user, proposal_id, facility_code=None):
    queryset = get_accessible_proposals(user, facility_code=facility_code)
    return queryset.filter(pk=proposal_id).first()


def get_manageable_proposal_for_user(user, proposal_id, facility_code=None):
    queryset = get_manageable_proposals(user, facility_code=facility_code)
    return queryset.filter(pk=proposal_id).first()


def get_proposal_by_pk(proposal_pk, facility_code=None):
    queryset = FacilityProposal.objects.select_related('account', 'account__facility').filter(is_active=True)
    if facility_code:
        queryset = queryset.filter(account__facility__code=facility_code)
    try:
        return queryset.get(pk=int(proposal_pk))
    except (ValueError, TypeError, FacilityProposal.DoesNotExist):
        return None


def get_facility_by_code(facility_code):
    return Facility.objects.filter(code=facility_code, is_active=True).first()


def get_proposal_choices_for_user(user, facility_code, include_account_label=False):
    choices = []
    for proposal in get_accessible_proposals(user, facility_code=facility_code).order_by(
        'account__label', 'title', 'external_id'
    ):
        label_bits = [proposal.title or proposal.external_id]
        if proposal.title and proposal.external_id and proposal.title != proposal.external_id:
            label_bits.append(proposal.external_id)
        label = ' | '.join(label_bits)
        if include_account_label:
            label = f'{proposal.account.label}: {label}'
        choices.append((str(proposal.pk), label))
    return choices


def get_account_members(account):
    return account.memberships.select_related('user').order_by('user__username')


def get_proposal_members(proposal):
    return proposal.memberships.select_related('user').order_by('user__username')


def sync_memberships_for_account(account, owner, shared_users):
    keep_ids = {owner.pk}
    FacilityAccountMembership.objects.update_or_create(
        account=account,
        user=owner,
        defaults={
            'role': FacilityAccountMembership.Role.OWNER,
            'can_view_credentials': True,
            'created_by': owner,
        },
    )
    for user in shared_users:
        keep_ids.add(user.pk)
        FacilityAccountMembership.objects.update_or_create(
            account=account,
            user=user,
            defaults={
                'role': FacilityAccountMembership.Role.VIEWER,
                'can_view_credentials': False,
                'created_by': owner,
            },
        )
    account.memberships.exclude(user_id__in=keep_ids).delete()


def sync_memberships_for_proposal(proposal, owner, shared_users):
    keep_ids = {owner.pk}
    FacilityProposalMembership.objects.update_or_create(
        proposal=proposal,
        user=owner,
        defaults={
            'role': FacilityProposalMembership.Role.OWNER,
            'can_submit_observations': True,
            'created_by': owner,
        },
    )
    for user in shared_users:
        keep_ids.add(user.pk)
        FacilityProposalMembership.objects.update_or_create(
            proposal=proposal,
            user=user,
            defaults={
                'role': FacilityProposalMembership.Role.USER,
                'can_submit_observations': True,
                'created_by': owner,
            },
        )
    proposal.memberships.exclude(user_id__in=keep_ids).delete()


def copy_account_memberships_to_proposal(account, proposal):
    keep_ids = set()
    for membership in account.memberships.select_related('user'):
        keep_ids.add(membership.user_id)
        role = FacilityProposalMembership.Role.USER
        if membership.role == FacilityAccountMembership.Role.OWNER:
            role = FacilityProposalMembership.Role.OWNER
        elif membership.role == FacilityAccountMembership.Role.EDITOR:
            role = FacilityProposalMembership.Role.EDITOR
        FacilityProposalMembership.objects.update_or_create(
            proposal=proposal,
            user=membership.user,
            defaults={
                'role': role,
                'can_submit_observations': True,
                'created_by': membership.created_by or account.created_by,
            },
        )
    proposal.memberships.exclude(user_id__in=keep_ids).delete()


def sync_remote_proposals_for_account(account):
    if not account.facility.supports_remote_proposal_sync:
        raise ValueError(f'{account.facility.code} does not support remote proposal sync.')

    if account.facility.code != 'LCO':
        raise ValueError(f'{account.facility.code} remote sync is not implemented.')

    api_key = account.credentials.get('api_key', '')
    if not api_key:
        raise ValueError('API key is required before syncing proposals.')

    portal_url = account.account_data.get('portal_url') or 'https://observe.lco.global'
    archive_url = account.account_data.get('archive_url') or 'https://archive-api.lco.global/'
    account.account_data.setdefault('portal_url', portal_url)
    account.account_data.setdefault('archive_url', archive_url)

    response = requests.get(
        f'{portal_url.rstrip("/")}/api/profile/',
        headers={'Authorization': f'Token {api_key}'},
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    remote_proposals = payload.get('proposals') or []

    seen_external_ids = set()
    imported_count = 0
    updated_count = 0
    active_count = 0

    for remote_proposal in remote_proposals:
        external_id = str(remote_proposal.get('id') or '').strip()
        if not external_id:
            continue
        seen_external_ids.add(external_id)
        title = str(remote_proposal.get('title') or external_id)
        valid_from, valid_until = _extract_proposal_window(remote_proposal)
        defaults = {
            'title': title,
            'details': {},
            'remote_payload': remote_proposal,
            'is_active': bool(remote_proposal.get('current', True)),
            'valid_from': valid_from,
            'valid_until': valid_until,
        }
        proposal, created = FacilityProposal.objects.update_or_create(
            account=account,
            external_id=external_id,
            defaults=defaults,
        )
        copy_account_memberships_to_proposal(account, proposal)
        imported_count += int(created)
        updated_count += int(not created)
        active_count += int(proposal.is_active)

    if seen_external_ids:
        account.proposals.exclude(external_id__in=seen_external_ids).update(is_active=False)

    account.sync_status = FacilityAccount.SyncStatus.OK
    account.last_synced_at = timezone.now()
    account.last_sync_error = ''
    account.save(update_fields=['account_data', 'sync_status', 'last_synced_at', 'last_sync_error', 'modified'])
    return {
        'imported_count': imported_count,
        'updated_count': updated_count,
        'active_count': active_count,
        'total_remote_count': len(remote_proposals),
    }
