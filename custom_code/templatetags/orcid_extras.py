from django import template
from django.conf import settings
from django.urls import NoReverseMatch, reverse


register = template.Library()


@register.simple_tag
def orcid_login_url(process='login'):
    if not getattr(settings, 'ORCID_ENABLED', True):
        return ''
    try:
        url = reverse('orcid_login')
    except NoReverseMatch:
        url = '/accounts/social/orcid/login/'
    return f'{url}?process={process}' if process else url
