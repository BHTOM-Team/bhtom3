import math
from datetime import timedelta
from django.db import models
from datetime import datetime, timezone

from dateutil.parser import parse

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models, transaction
from django.db.models import Q
from django.forms.models import model_to_dict
from django.urls import reverse
from astropy.time import Time

from tom_targets.base_models import BaseTarget
from custom_code.orcid import canonicalize_orcid, orcid_public_url, validate_orcid


class BhtomTarget(BaseTarget):
    """
    A target with fields defined by a user.
    """

    class Meta:
        verbose_name = "target"
        permissions = (
            ('view_target', 'View Target'),
            ('add_target', 'Add Target'),
            ('change_target', 'Change Target'),
            ('delete_target', 'Delete Target'),
        )

    def photometry_plot_path(self, filename):
        return '/photometry/{0}'.format(filename)

    def photometry_plot_obs_path(self, filename):
        return '/photometry/obs_{0}'.format(filename)

    def photometry_icon_plot_path(self, filename):
        return '/photometryIcon/{0}'.format(filename)

    def spectroscopy_plot_path(self, filename):
        return '/spectroscopy/{0}'.format(filename)


    classification = models.CharField(
        max_length=50, null=True, blank=True, verbose_name='classification', choices=settings.CLASSIFICATION_TYPES,
        help_text='Classification of the object (e.g. variable star, microlensing event)', db_index=True
    )
    discovery_date = models.DateTimeField(
        verbose_name='discovery date', help_text='Date of the discovery, YYYY-MM-DDTHH:MM:SS, or leave blank',
        null=True, blank=True
    )
    mjd_last = models.FloatField(
        verbose_name='mjd last', null=True, default=0, blank=True
    )
    mag_last = models.FloatField(
        verbose_name='mag last', null=True, blank=True, default=100, db_index=True
    )
    importance = models.FloatField(
        verbose_name='importance',
        help_text='Target importance as an integer 0-10 (10 is the highest)',
        default=0,
        db_index=True
    )
    cadence = models.FloatField(
        verbose_name='cadence',
        help_text='Requested cadence (0-100 days)',
        default=0
    )
    priority = models.FloatField(
        verbose_name='priority', null=True, blank=True, default=0, db_index=True
    )
    sun_separation = models.FloatField(
        verbose_name='sun separation', null=True, blank=True, db_index=True
    )
    constellation = models.CharField(max_length=50,
                                     verbose_name='constellation', null=True, blank=True
                                     )
    phot_class = models.CharField(max_length=50,
                                  verbose_name='phot class', null=True, blank=True
                                  )
    phot_classification_done = models.BooleanField(
        default=False,
        db_index=True,
        verbose_name='photometric classification done',
    )
    photometry_plot = models.FileField(upload_to=photometry_plot_path, null=True, blank=True, default=None)
    photometry_plot_obs = models.FileField(upload_to=photometry_plot_obs_path, null=True, blank=True, default=None)
    photometry_icon_plot = models.FileField(upload_to=photometry_icon_plot_path, null=True, blank=True, default=None)
    spectroscopy_plot = models.FileField(upload_to=spectroscopy_plot_path, null=True, blank=True, default=None)
    plot_created = models.DateTimeField(verbose_name='plot creation date', null=True, blank=True)
    filter_last = models.CharField(max_length=20, verbose_name='last filter', null=True, blank=True, default='')
    cadence_priority = models.FloatField(verbose_name='cadence priority', null=True, blank=True, default=0)
    description = models.CharField(max_length=200, verbose_name='description', null=True, blank=True)
    parallax_error = models.FloatField(
        verbose_name='parallax error', null=True, blank=True,
        help_text='Parallax uncertainty, in milliarcseconds.'
    )
    pm_ra_error = models.FloatField(
        verbose_name='proper motion RA error', null=True, blank=True,
        help_text='Proper Motion RA uncertainty, in milliarcsec/year.'
    )
    pm_dec_error = models.FloatField(
        verbose_name='proper motion Dec error', null=True, blank=True,
        help_text='Proper Motion Dec uncertainty, in milliarcsec/year.'
    )
    gaia_variability_type = models.CharField(
        max_length=64,
        verbose_name='Gaia variability type',
        null=True,
        blank=True,
        help_text='Gaia DR3 variability class from vari_classifier_result.best_class_name.',
    )

    def get_classification_type_display(self):
        for key, display in settings.CLASSIFICATION_TYPES:
            if key == self.classification:
                return display
        return "Unknown"  # Default to "Unknown" if not found


class GeoTarget(models.Model):
    norad_id = models.PositiveIntegerField(unique=True, db_index=True)
    name = models.CharField(max_length=128, db_index=True)
    intldes = models.CharField(max_length=32, blank=True, default="", db_index=True)
    source = models.CharField(max_length=32, blank=True, default="manual", db_index=True)
    object_type = models.CharField(max_length=32, blank=True, default="", db_index=True)
    is_debris = models.BooleanField(default=False, db_index=True)
    tle_name = models.CharField(max_length=128, blank=True, default="")
    tle_line1 = models.CharField(max_length=128)
    tle_line2 = models.CharField(max_length=128)
    epoch_jd = models.FloatField(null=True, blank=True)
    inclination_deg = models.FloatField(null=True, blank=True)
    eccentricity = models.FloatField(null=True, blank=True)
    raan_deg = models.FloatField(null=True, blank=True)
    arg_perigee_deg = models.FloatField(null=True, blank=True)
    mean_anomaly_deg = models.FloatField(null=True, blank=True)
    mean_motion_rev_per_day = models.FloatField(null=True, blank=True)
    bstar = models.FloatField(null=True, blank=True)
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ("name",)

    def __str__(self):
        return f"{self.name} ({self.norad_id})"


class TargetAliasInfo(models.Model):
    target_name = models.OneToOneField('tom_targets.TargetName', on_delete=models.CASCADE, related_name='alias_info')
    source_name = models.CharField(max_length=100, blank=True, default='')
    url = models.URLField(max_length=500, blank=True, default='')
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'target alias info'
        verbose_name_plural = 'target alias info'

    def __str__(self):
        return f'{self.target_name.name}'


class TransitEphemeris(models.Model):
    target = models.OneToOneField('custom_code.BhtomTarget', on_delete=models.CASCADE, related_name='transit_ephemeris')
    source_name = models.CharField(max_length=100, blank=True, default='')
    source_url = models.URLField(max_length=500, blank=True, default='')
    planet_name = models.CharField(max_length=100, blank=True, default='')
    host_name = models.CharField(max_length=100, blank=True, default='')
    priority = models.CharField(max_length=32, blank=True, default='', db_index=True)
    current_oc_min = models.FloatField(null=True, blank=True)
    t0_bjd_tdb = models.FloatField(null=True, blank=True)
    t0_unc = models.FloatField(null=True, blank=True)
    period_days = models.FloatField(null=True, blank=True)
    period_unc = models.FloatField(null=True, blank=True)
    duration_hours = models.FloatField(null=True, blank=True)
    depth_r_mmag = models.FloatField(null=True, blank=True)
    v_mag = models.FloatField(null=True, blank=True)
    r_mag = models.FloatField(null=True, blank=True)
    gaia_g_mag = models.FloatField(null=True, blank=True)
    min_telescope_inches = models.FloatField(null=True, blank=True)
    total_observations = models.PositiveIntegerField(null=True, blank=True)
    recent_observations = models.PositiveIntegerField(null=True, blank=True)
    payload = models.JSONField(blank=True, default=dict)
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'transit ephemeris'
        verbose_name_plural = 'transit ephemerides'

    def __str__(self):
        label = self.planet_name or self.target.name
        return f'{label} transit ephemeris'

    def next_transit_time(self, now=None):
        if self.t0_bjd_tdb is None or self.period_days in (None, 0):
            return None

        current_time = now or Time.now()
        current_bjd_tdb = current_time.tdb.jd if hasattr(current_time, 'tdb') else Time(current_time, scale='utc').tdb.jd
        epochs_since_t0 = (current_bjd_tdb - float(self.t0_bjd_tdb)) / float(self.period_days)
        next_epoch = math.ceil(epochs_since_t0)
        next_bjd_tdb = float(self.t0_bjd_tdb) + next_epoch * float(self.period_days)
        return Time(next_bjd_tdb, format='jd', scale='tdb').utc.to_datetime(timezone=timezone.utc)

    def hours_until_next_transit(self, now=None):
        next_transit = self.next_transit_time(now=now)
        if next_transit is None:
            return None

        current_time = now or Time.now()
        current_dt = current_time.to_datetime(timezone=timezone.utc) if hasattr(current_time, 'to_datetime') else current_time
        delta = next_transit - current_dt
        return delta.total_seconds() / 3600.0

    def next_transit_display(self, now=None):
        next_transit = self.next_transit_time(now=now)
        hours = self.hours_until_next_transit(now=now)
        if next_transit is None or hours is None:
            return None
        return {
            'utc': next_transit,
            'hours': hours,
        }

    def next_transit_window_display(self, now=None):
        next_transit = self.next_transit_time(now=now)
        if next_transit is None:
            return None

        if self.duration_hours in (None, 0):
            return {
                'transit': self.next_transit_display(now=now),
                'ingress': None,
                'egress': None,
            }

        half_duration = timedelta(hours=float(self.duration_hours) / 2.0)
        ingress = next_transit - half_duration
        egress = next_transit + half_duration
        current_time = now or Time.now()
        current_dt = current_time.to_datetime(timezone=timezone.utc) if hasattr(current_time, 'to_datetime') else current_time
        return {
            'transit': {
                'utc': next_transit,
                'hours': (next_transit - current_dt).total_seconds() / 3600.0,
            },
            'ingress': {
                'utc': ingress,
                'hours': (ingress - current_dt).total_seconds() / 3600.0,
            },
            'egress': {
                'utc': egress,
                'hours': (egress - current_dt).total_seconds() / 3600.0,
            },
        }


class Facility(models.Model):
    """
    Declarative description of a facility and the shapes of its account/proposal fields.
    """

    code = models.CharField(max_length=32, unique=True, db_index=True)
    name = models.CharField(max_length=128)
    description = models.TextField(blank=True, default='')
    account_schema = models.JSONField(blank=True, default=dict)
    proposal_schema = models.JSONField(blank=True, default=dict)
    supports_remote_proposal_sync = models.BooleanField(default=False, db_index=True)
    is_active = models.BooleanField(default=True, db_index=True)
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ('name',)

    def __str__(self):
        return self.name


class FacilityAccount(models.Model):
    """
    Shared facility access bundle, such as an LCO API key or REM login/email pair.
    """

    class SyncStatus(models.TextChoices):
        NOT_SYNCED = 'not_synced', 'Not synced'
        OK = 'ok', 'OK'
        ERROR = 'error', 'Error'

    facility = models.ForeignKey(Facility, on_delete=models.CASCADE, related_name='accounts')
    label = models.CharField(max_length=128)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name='facility_accounts_created',
    )
    account_data = models.JSONField(blank=True, default=dict)
    credentials = models.JSONField(blank=True, default=dict)
    is_active = models.BooleanField(default=True, db_index=True)
    sync_status = models.CharField(
        max_length=16,
        choices=SyncStatus.choices,
        default=SyncStatus.NOT_SYNCED,
        db_index=True,
    )
    last_synced_at = models.DateTimeField(null=True, blank=True)
    last_sync_error = models.TextField(blank=True, default='')
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)
    users = models.ManyToManyField(
        settings.AUTH_USER_MODEL,
        through='FacilityAccountMembership',
        through_fields=('account', 'user'),
        related_name='shared_facility_accounts',
    )

    class Meta:
        ordering = ('facility__name', 'label')
        unique_together = (('facility', 'label'),)

    def __str__(self):
        return f'{self.facility.code}: {self.label}'


class FacilityAccountMembership(models.Model):
    class Role(models.TextChoices):
        OWNER = 'owner', 'Owner'
        EDITOR = 'editor', 'Editor'
        VIEWER = 'viewer', 'Viewer'

    account = models.ForeignKey(FacilityAccount, on_delete=models.CASCADE, related_name='memberships')
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='facility_account_memberships',
    )
    role = models.CharField(max_length=16, choices=Role.choices, default=Role.OWNER)
    can_view_credentials = models.BooleanField(default=False)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name='facility_account_memberships_created',
        null=True,
        blank=True,
    )
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = (('account', 'user'),)

    def __str__(self):
        return f'{self.account} -> {self.user}'


class FacilityProposal(models.Model):
    """
    A proposal/project visible inside one facility account.
    """

    account = models.ForeignKey(FacilityAccount, on_delete=models.CASCADE, related_name='proposals')
    external_id = models.CharField(max_length=128)
    title = models.CharField(max_length=255, blank=True, default='')
    details = models.JSONField(blank=True, default=dict)
    remote_payload = models.JSONField(blank=True, default=dict)
    is_active = models.BooleanField(default=True, db_index=True)
    valid_from = models.DateTimeField(null=True, blank=True)
    valid_until = models.DateTimeField(null=True, blank=True)
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)
    users = models.ManyToManyField(
        settings.AUTH_USER_MODEL,
        through='FacilityProposalMembership',
        through_fields=('proposal', 'user'),
        related_name='shared_facility_proposals',
    )

    class Meta:
        ordering = ('account__facility__name', 'title', 'external_id')
        unique_together = (('account', 'external_id'),)

    def __str__(self):
        return self.title or f'{self.account.facility.code} {self.external_id}'


class FacilityProposalMembership(models.Model):
    class Role(models.TextChoices):
        OWNER = 'owner', 'Owner'
        EDITOR = 'editor', 'Editor'
        USER = 'user', 'User'

    proposal = models.ForeignKey(FacilityProposal, on_delete=models.CASCADE, related_name='memberships')
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='facility_proposal_memberships',
    )
    role = models.CharField(max_length=16, choices=Role.choices, default=Role.USER)
    can_submit_observations = models.BooleanField(default=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name='facility_proposal_memberships_created',
        null=True,
        blank=True,
    )
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = (('proposal', 'user'),)

    def __str__(self):
        return f'{self.proposal} -> {self.user}'


class UserBhtom2UploadPreference(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='bhtom2_upload_preference',
    )
    token = models.CharField(max_length=255, blank=True, default='')
    oname = models.CharField(max_length=255, blank=True, default='')
    calibration_filter = models.CharField(max_length=64, blank=True, default='GaiaSP/any')
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'BHTOM2 upload preference'
        verbose_name_plural = 'BHTOM2 upload preferences'

    def __str__(self):
        return f'BHTOM2 upload preference for {self.user}'


class BhtomUserProfile(models.Model):
    class OrcidSource(models.TextChoices):
        OAUTH = 'oauth', 'OAuth'
        MANUAL = 'manual', 'Manual'

    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name='bhtom_profile',
    )
    affiliation = models.CharField(max_length=255, blank=True, default='')
    about = models.TextField(blank=True, default='')
    orcid_id = models.CharField(max_length=19, null=True, blank=True, db_index=True)
    orcid_verified = models.BooleanField(default=False)
    orcid_linked_at = models.DateTimeField(null=True, blank=True)
    orcid_public_url = models.URLField(blank=True, default='')
    orcid_source = models.CharField(max_length=16, choices=OrcidSource.choices, null=True, blank=True)
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'BHTOM user profile'
        verbose_name_plural = 'BHTOM user profiles'
        constraints = [
            models.UniqueConstraint(
                fields=['orcid_id'],
                condition=Q(orcid_id__isnull=False) & ~Q(orcid_id=''),
                name='unique_nonempty_bhtom_orcid_id',
            ),
        ]

    def clean(self):
        super().clean()
        if self.orcid_id:
            self.orcid_id = validate_orcid(self.orcid_id)
            self.orcid_public_url = orcid_public_url(self.orcid_id)

    def save(self, *args, **kwargs):
        if self.orcid_id:
            self.orcid_id = canonicalize_orcid(self.orcid_id)
            self.orcid_public_url = orcid_public_url(self.orcid_id)
        else:
            self.orcid_id = None
            self.orcid_public_url = ''
            self.orcid_verified = False
            self.orcid_source = None
            self.orcid_linked_at = None
        super().save(*args, **kwargs)

    def __str__(self):
        return f'BHTOM profile for {self.user}'


class ATLASForcedPhotJob(models.Model):
    """A submitted-but-not-yet-retrieved ATLAS Forced Photometry Server job.

    The ATLAS server is an asynchronous queue, so the DataService submits a job and
    records it here; a periodic poller (in db_worker) later checks each pending job and
    ingests the photometry once the job finishes. See atlas_dataservice.py.
    """
    STATUS_PENDING = 'pending'
    STATUS_DONE = 'done'
    STATUS_FAILED = 'failed'
    STATUS_CHOICES = (
        (STATUS_PENDING, 'Pending'),
        (STATUS_DONE, 'Done'),
        (STATUS_FAILED, 'Failed'),
    )

    target = models.ForeignKey(
        'custom_code.BhtomTarget', on_delete=models.CASCADE, related_name='atlas_forced_phot_jobs'
    )
    task_url = models.URLField(max_length=500)
    result_url = models.URLField(max_length=500, null=True, blank=True)
    mjd_min = models.FloatField(null=True, blank=True)
    mjd_max = models.FloatField(null=True, blank=True)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_PENDING, db_index=True)
    attempts = models.PositiveIntegerField(default=0)
    datapoints_added = models.IntegerField(default=0)
    error = models.TextField(null=True, blank=True)
    submitted_at = models.DateTimeField(auto_now_add=True)
    last_checked_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        verbose_name = 'ATLAS forced photometry job'
        indexes = [
            models.Index(fields=['status', 'submitted_at']),
        ]

    def __str__(self):
        return f'ATLAS job target={self.target_id} status={self.status}'
