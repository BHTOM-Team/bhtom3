from django.contrib import admin

from custom_code.models import GeoTarget


@admin.register(GeoTarget)
class GeoTargetAdmin(admin.ModelAdmin):
    list_display = ("name", "norad_id", "inclination_deg", "mean_motion_rev_per_day", "modified")
    search_fields = ("name", "norad_id", "tle_name")
