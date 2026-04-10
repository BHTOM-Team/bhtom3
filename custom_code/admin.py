from django.contrib import admin

from custom_code.models import GeoTarget, TransitEphemeris


@admin.register(GeoTarget)
class GeoTargetAdmin(admin.ModelAdmin):
    list_display = ("name", "norad_id", "object_type", "is_debris", "source", "inclination_deg", "mean_motion_rev_per_day", "modified")
    search_fields = ("name", "norad_id", "tle_name", "intldes")
    list_filter = ("object_type", "is_debris", "source")


@admin.register(TransitEphemeris)
class TransitEphemerisAdmin(admin.ModelAdmin):
    list_display = (
        "target",
        "planet_name",
        "priority",
        "period_days",
        "duration_hours",
        "depth_r_mmag",
        "modified",
    )
    search_fields = ("target__name", "planet_name", "host_name", "source_name")
    list_filter = ("priority", "source_name")
