from django import template

from custom_code.sun_separation import get_live_target_values

register = template.Library()


@register.simple_tag
def live_target_values(target):
    return get_live_target_values(target)
