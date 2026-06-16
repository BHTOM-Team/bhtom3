import re
import unicodedata

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.utils.translation import gettext_lazy as _


ORCID_CANONICAL_RE = re.compile(r'^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$')
ORCID_NOTE_PREFIX = 'ORCID-authenticated account.'


def canonicalize_orcid(orcid_id):
    value = str(orcid_id or '').strip()
    if not value:
        return ''
    value = value.removeprefix('https://orcid.org/').removeprefix('http://orcid.org/')
    value = value.replace(' ', '').upper()
    if '-' not in value and len(value) == 16:
        value = f'{value[:4]}-{value[4:8]}-{value[8:12]}-{value[12:]}'
    return value


def validate_orcid(orcid_id):
    value = canonicalize_orcid(orcid_id)
    if not ORCID_CANONICAL_RE.match(value):
        raise ValidationError(_('Enter an ORCID iD in the form 0000-0000-0000-0000.'))

    total = 0
    for char in value.replace('-', '')[:-1]:
        total = (total + int(char)) * 2
    remainder = total % 11
    result = (12 - remainder) % 11
    check_digit = 'X' if result == 10 else str(result)
    if value[-1] != check_digit:
        raise ValidationError(_('Enter a valid ORCID iD checksum.'))
    return value


def orcid_public_url(orcid_id):
    value = canonicalize_orcid(orcid_id)
    return f'https://orcid.org/{value}' if value else ''


def _username_part(value):
    normalized = unicodedata.normalize('NFKD', str(value or ''))
    ascii_value = normalized.encode('ascii', 'ignore').decode('ascii').lower()
    ascii_value = re.sub(r'[\s\'-]+', '.', ascii_value)
    ascii_value = re.sub(r'[^a-z0-9.]+', '', ascii_value)
    ascii_value = re.sub(r'\.+', '.', ascii_value).strip('.')
    return ascii_value


def unique_orcid_username(first_name='', last_name='', orcid_id=''):
    if first_name and last_name:
        base = f'{_username_part(first_name)}.{_username_part(last_name)}'.strip('.')
    else:
        base = f'orcid.{canonicalize_orcid(orcid_id).lower()}'
    base = re.sub(r'[^a-z0-9.-]+', '', base).strip('.') or 'orcid.user'

    User = get_user_model()
    candidate = base
    suffix = 2
    while User.objects.filter(username__iexact=candidate).exists():
        candidate = f'{base}{suffix}'
        suffix += 1
    return candidate


def build_orcid_about(orcid_id, biography=''):
    note = f'{ORCID_NOTE_PREFIX}\nORCID iD: {orcid_public_url(orcid_id)}'
    biography = str(biography or '').strip()
    return f'{biography}\n\n{note}' if biography else note


def profile_has_orcid_note(profile):
    about = getattr(profile, 'about', '') or ''
    return ORCID_NOTE_PREFIX in about or orcid_public_url(getattr(profile, 'orcid_id', '')) in about
