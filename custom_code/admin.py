from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from django.contrib.auth.models import User

from custom_code.models import (
    BhtomUserProfile,
    Facility,
    FacilityAccount,
    FacilityAccountMembership,
    FacilityProposal,
    FacilityProposalMembership,
    GeoTarget,
    TransitEphemeris,
)


class BhtomUserProfileInline(admin.StackedInline):
    model = BhtomUserProfile
    can_delete = False
    extra = 0
    fields = (
        'affiliation',
        'about',
        'orcid_id',
        'orcid_verified',
        'orcid_linked_at',
        'orcid_public_url',
        'orcid_source',
    )
    readonly_fields = ('orcid_public_url', 'orcid_linked_at')


class BhtomUserAdmin(UserAdmin):
    inlines = UserAdmin.inlines + (BhtomUserProfileInline,)
    search_fields = UserAdmin.search_fields + ('bhtom_profile__orcid_id',)


try:
    admin.site.unregister(User)
except admin.sites.NotRegistered:
    pass
admin.site.register(User, BhtomUserAdmin)


@admin.register(BhtomUserProfile)
class BhtomUserProfileAdmin(admin.ModelAdmin):
    list_display = ('user', 'orcid_id', 'orcid_verified', 'orcid_source', 'affiliation', 'modified')
    search_fields = ('user__username', 'user__email', 'user__first_name', 'user__last_name', 'orcid_id')
    list_filter = ('orcid_verified', 'orcid_source')
    autocomplete_fields = ('user',)
    readonly_fields = ('orcid_public_url', 'created', 'modified')


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
