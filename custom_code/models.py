from django.db import models
from datetime import datetime

from dateutil.parser import parse

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models, transaction
from django.forms.models import model_to_dict
from django.urls import reverse

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
    photometry_plot = models.FileField(upload_to=photometry_plot_path, null=True, blank=True, default=None)
    photometry_plot_obs = models.FileField(upload_to=photometry_plot_obs_path, null=True, blank=True, default=None)
    photometry_icon_plot = models.FileField(upload_to=photometry_icon_plot_path, null=True, blank=True, default=None)
    spectroscopy_plot = models.FileField(upload_to=spectroscopy_plot_path, null=True, blank=True, default=None)
    plot_created = models.DateTimeField(verbose_name='plot creation date', null=True, blank=True)
    filter_last = models.CharField(max_length=20, verbose_name='last filter', null=True, blank=True, default='')
    cadence_priority = models.FloatField(verbose_name='cadence priority', null=True, blank=True, default=0)
    description = models.CharField(max_length=200, verbose_name='description', null=True, blank=True)

    def get_classification_type_display(self):
        for key, display in settings.CLASSIFICATION_TYPES:
            if key == self.classification:
                return display
        return "Unknown"  # Default to "Unknown" if not found

