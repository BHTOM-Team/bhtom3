from django.contrib import admin

from custom_code.models import (
    Facility,
    FacilityAccount,
    FacilityAccountMembership,
    FacilityProposal,
    FacilityProposalMembership,
    GeoTarget,
    TransitEphemeris,
)


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


@admin.register(Facility)
class FacilityAdmin(admin.ModelAdmin):
    list_display = ('code', 'name', 'supports_remote_proposal_sync', 'is_active', 'modified')
    search_fields = ('code', 'name')
    list_filter = ('supports_remote_proposal_sync', 'is_active')


@admin.register(FacilityAccount)
class FacilityAccountAdmin(admin.ModelAdmin):
    list_display = ('label', 'facility', 'created_by', 'sync_status', 'is_active', 'last_synced_at', 'modified')
    search_fields = ('label', 'facility__code', 'facility__name', 'created_by__username')
    list_filter = ('facility', 'sync_status', 'is_active')
    autocomplete_fields = ('facility', 'created_by')


@admin.register(FacilityAccountMembership)
class FacilityAccountMembershipAdmin(admin.ModelAdmin):
    list_display = ('account', 'user', 'role', 'can_view_credentials', 'modified')
    search_fields = ('account__label', 'user__username', 'user__email')
    list_filter = ('role', 'can_view_credentials', 'account__facility')
    autocomplete_fields = ('account', 'user', 'created_by')


@admin.register(FacilityProposal)
class FacilityProposalAdmin(admin.ModelAdmin):
    list_display = ('external_id', 'title', 'account', 'is_active', 'valid_until', 'modified')
    search_fields = ('external_id', 'title', 'account__label', 'account__facility__code')
    list_filter = ('is_active', 'account__facility')
    autocomplete_fields = ('account',)


@admin.register(FacilityProposalMembership)
class FacilityProposalMembershipAdmin(admin.ModelAdmin):
    list_display = ('proposal', 'user', 'role', 'can_submit_observations', 'modified')
    search_fields = ('proposal__external_id', 'proposal__title', 'user__username', 'user__email')
    list_filter = ('role', 'can_submit_observations', 'proposal__account__facility')
    autocomplete_fields = ('proposal', 'user', 'created_by')
