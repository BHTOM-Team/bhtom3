from django.contrib.auth import get_user_model
from django.db.models import Q

from custom_code.models import (
    Facility,
    FacilityAccount,
    FacilityAccountMembership,
    FacilityProposal,
    FacilityProposalMembership,
)


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
