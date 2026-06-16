import logging

import requests
from django.conf import settings
from django.contrib.auth.models import Group
from django.core.mail import mail_admins, send_mail
from django.db import transaction
from django.shortcuts import redirect
from django.urls import reverse
from django.utils import timezone

from allauth.socialaccount.adapter import DefaultSocialAccountAdapter
try:
    from allauth.core.exceptions import ImmediateHttpResponse
except ImportError:
    from allauth.exceptions import ImmediateHttpResponse

from custom_code.models import BhtomUserProfile
from custom_code.orcid import (
    build_orcid_about,
    canonicalize_orcid,
    orcid_public_url,
    profile_has_orcid_note,
    unique_orcid_username,
)


logger = logging.getLogger(__name__)


def _first_value(*values):
    for value in values:
        if isinstance(value, dict):
            value = value.get('value')
        if value not in (None, '', [], {}):
            return value
    return ''


def _extract_person_value(person, *paths):
    for path in paths:
        if isinstance(path, str):
            path = (path,)
        current = person
        for part in path:
            if isinstance(part, int) and isinstance(current, list):
                current = current[part] if len(current) > part else None
            elif isinstance(current, dict):
                current = current.get(part)
            else:
                current = None
                break
        value = _first_value(current)
        if value:
            return value
    return ''


def _extract_affiliation(activities):
    candidates = []
    for section_name in ('employments', 'educations'):
        section = (activities or {}).get(section_name, {})
        for group in section.get('affiliation-group', []) or []:
            summaries = group.get('summaries') or []
            for wrapped in summaries:
                summary = wrapped.get('employment-summary') or wrapped.get('education-summary') or wrapped
                organization = summary.get('organization') or {}
                name = organization.get('name') or ''
                if not name:
                    continue
                end_date = summary.get('end-date')
                start_date = summary.get('start-date') or {}
                candidates.append(
                    {
                        'name': name,
                        'current': end_date in (None, {}, ''),
                        'section': section_name,
                        'start_year': int(_first_value(start_date.get('year')) or 0),
                    }
                )
    if not candidates:
        return ''
    candidates.sort(
        key=lambda item: (
            0 if item['current'] else 1,
            0 if item['section'] == 'employments' else 1,
            -item['start_year'],
        )
    )
    return candidates[0]['name']


def fetch_public_orcid_profile(orcid_id):
    timeout = getattr(settings, 'ORCID_PUBLIC_API_TIMEOUT', 6)
    base_domain = getattr(settings, 'ORCID_BASE_DOMAIN', 'orcid.org')
    api_domain = 'pub.sandbox.orcid.org' if base_domain == 'sandbox.orcid.org' else 'pub.orcid.org'
    url = f'https://{api_domain}/v3.0/{canonicalize_orcid(orcid_id)}/record'
    try:
        response = requests.get(url, headers={'Accept': 'application/json'}, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.info('ORCID public profile lookup failed for %s: %s', canonicalize_orcid(orcid_id), exc)
        return {}

    payload = response.json()
    person = payload.get('person') or {}
    return {
        'first_name': _extract_person_value(person, ('name', 'given-names')),
        'last_name': _extract_person_value(person, ('name', 'family-name')),
        'biography': _extract_person_value(person, ('biography', 'content')),
        'email': _extract_person_value(person, ('emails', 'email', 0, 'email')),
        'affiliation': _extract_affiliation(payload.get('activities-summary') or {}),
    }


def _orcid_from_sociallogin(sociallogin):
    uid = getattr(sociallogin.account, 'uid', '') or ''
    extra_data = getattr(sociallogin.account, 'extra_data', {}) or {}
    return canonicalize_orcid(
        uid
        or extra_data.get('orcid')
        or extra_data.get('orcid-identifier', {}).get('path')
        or extra_data.get('orcid-identifier', {}).get('uri', '').rstrip('/').split('/')[-1]
    )


def _profile_data_from_sociallogin(sociallogin, orcid_id):
    extra_data = getattr(sociallogin.account, 'extra_data', {}) or {}
    data = {
        'first_name': _first_value(extra_data.get('given_name'), extra_data.get('given-names')),
        'last_name': _first_value(extra_data.get('family_name'), extra_data.get('family-name')),
        'email': _first_value(extra_data.get('email')),
        'biography': '',
        'affiliation': '',
    }
    public_data = fetch_public_orcid_profile(orcid_id)
    for key, value in public_data.items():
        if value and not data.get(key):
            data[key] = value
    return data


def _notify_admins_for_orcid_user(request, user, profile):
    if not getattr(settings, 'ORCID_SEND_ADMIN_NOTIFICATION', True):
        return

    recipients = getattr(settings, 'ORCID_ADMIN_NOTIFY_EMAILS', [])
    if isinstance(recipients, str):
        recipients = [email.strip() for email in recipients.split(',') if email.strip()]

    admin_url = ''
    if request is not None:
        admin_url = request.build_absolute_uri(reverse('admin:auth_user_change', args=[user.pk]))

    body = '\n'.join(
        [
            f'username: {user.username}',
            f'first name: {user.first_name}',
            f'last name: {user.last_name}',
            f'email: {user.email}',
            f'affiliation: {profile.affiliation}',
            f'ORCID iD: {profile.orcid_id}',
            f'ORCID URL: {profile.orcid_public_url}',
            f'created: {user.date_joined.isoformat()}',
            f'ORCID verified: {profile.orcid_verified}',
            f'admin user page: {admin_url}',
        ]
    )
    subject = 'New BHTOM3 account created via ORCID'
    from_email = getattr(settings, 'DEFAULT_FROM_EMAIL', None) or getattr(settings, 'EMAIL_HOST_USER', None)
    if recipients:
        send_mail(subject, body, from_email, recipients, fail_silently=True)
    else:
        mail_admins(subject, body, fail_silently=True)


class BhtomOrcidSocialAccountAdapter(DefaultSocialAccountAdapter):
    def is_open_for_signup(self, request, sociallogin):
        return getattr(settings, 'ORCID_ENABLED', True) and super().is_open_for_signup(request, sociallogin)

    def populate_user(self, request, sociallogin, data):
        user = super().populate_user(request, sociallogin, data)
        if sociallogin.account.provider != 'orcid':
            return user

        orcid_id = _orcid_from_sociallogin(sociallogin)
        profile_data = _profile_data_from_sociallogin(sociallogin, orcid_id)
        user.first_name = profile_data.get('first_name') or user.first_name
        user.last_name = profile_data.get('last_name') or user.last_name
        user.email = profile_data.get('email') or user.email
        user.username = unique_orcid_username(user.first_name, user.last_name, orcid_id)
        return user

    def pre_social_login(self, request, sociallogin):
        if sociallogin.account.provider != 'orcid' or sociallogin.is_existing:
            return

        orcid_id = _orcid_from_sociallogin(sociallogin)
        if not orcid_id:
            return

        profile = BhtomUserProfile.objects.filter(orcid_id=orcid_id).select_related('user').first()
        if profile is None:
            return

        with transaction.atomic():
            profile.orcid_verified = True
            profile.orcid_source = BhtomUserProfile.OrcidSource.OAUTH
            profile.orcid_linked_at = timezone.now()
            profile.orcid_public_url = orcid_public_url(orcid_id)
            if not profile_has_orcid_note(profile):
                note = build_orcid_about(orcid_id)
                profile.about = f'{profile.about.strip()}\n\n{note}'.strip()
            profile.save()
            sociallogin.connect(request, profile.user)

        raise ImmediateHttpResponse(redirect(getattr(settings, 'LOGIN_REDIRECT_URL', '/')))

    def save_user(self, request, sociallogin, form=None):
        is_orcid = sociallogin.account.provider == 'orcid'
        with transaction.atomic():
            user = super().save_user(request, sociallogin, form)
            if not is_orcid:
                return user

            orcid_id = _orcid_from_sociallogin(sociallogin)
            profile_data = _profile_data_from_sociallogin(sociallogin, orcid_id)
            profile, _ = BhtomUserProfile.objects.get_or_create(user=user)
            profile.orcid_id = orcid_id
            profile.orcid_verified = True
            profile.orcid_linked_at = timezone.now()
            profile.orcid_source = BhtomUserProfile.OrcidSource.OAUTH
            profile.orcid_public_url = orcid_public_url(orcid_id)
            profile.affiliation = profile_data.get('affiliation') or profile.affiliation
            profile.about = build_orcid_about(orcid_id, profile_data.get('biography'))
            profile.save()

            group, _ = Group.objects.get_or_create(name='Public')
            group.user_set.add(user)
            _notify_admins_for_orcid_user(request, user, profile)
            return user
