import math
from datetime import timedelta
from django.db import models
from datetime import datetime, timezone

from dateutil.parser import parse

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models, transaction
from django.forms.models import model_to_dict
from django.urls import reverse
from astropy.time import Time

from tom_targets.base_models import BaseTarget


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
