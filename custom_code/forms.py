import json
from datetime import datetime, timedelta

from django import forms
from django.contrib.auth import password_validation
from django.contrib.auth.models import Group, User
from django.core.exceptions import ValidationError
from django.forms import BaseInlineFormSet, inlineformset_factory
from tom_catalogs.harvester import get_service_classes
from tom_targets.forms import NonSiderealTargetCreateForm, SiderealTargetCreateForm
from tom_targets.models import Target, TargetName

from custom_code.models import TargetAliasInfo, TransitEphemeris


CREATE_FORM_HIDDEN_FIELDS = (
    'constellation',
    'phot_class',
    'phot_classification_done',
    'mjd_last',
    'mag_last',
    'filter_last',
    'photometry_plot',
    'photometry_plot_obs',
    'photometry_icon_plot',
    'spectroscopy_plot',
    'plot_created',
)

SIDEREAL_CREATE_FORM_HIDDEN_FIELDS = CREATE_FORM_HIDDEN_FIELDS + (
    'galactic_lng',
    'galactic_lat',
)


class GeoTomAddSatForm(forms.Form):
    norad_id = forms.IntegerField(min_value=1, label="NORAD ID")


class BhtomUserBaseForm(forms.ModelForm):
    email = forms.EmailField(required=True)
    groups = forms.ModelMultipleChoiceField(
        Group.objects.all().exclude(name='Public'),
        required=False,
        widget=forms.CheckboxSelectMultiple,
    )

    class Meta:
        model = User
        fields = ('username', 'first_name', 'last_name', 'email', 'groups')

    def clean_username(self):
        username = (self.cleaned_data.get('username') or '').strip()
        duplicates = User.objects.filter(username__iexact=username)
        if self.instance.pk:
            duplicates = duplicates.exclude(pk=self.instance.pk)
        if duplicates.exists():
            raise forms.ValidationError('A user with that username already exists.')
        return username


class BhtomUserCreationForm(BhtomUserBaseForm):
    password1 = forms.CharField(
        label='Password',
        strip=False,
        widget=forms.PasswordInput(attrs={'autocomplete': 'new-password'}),
        help_text=password_validation.password_validators_help_text_html(),
    )
    password2 = forms.CharField(
        label='Password confirmation',
        strip=False,
        widget=forms.PasswordInput(attrs={'autocomplete': 'new-password'}),
        help_text='Enter the same password as before, for verification.',
    )

    def clean(self):
        cleaned_data = super().clean()
        password1 = cleaned_data.get('password1')
        password2 = cleaned_data.get('password2')
        if password1 != password2:
            self.add_error('password2', 'The two password fields did not match.')
        if password2:
            try:
                password_validation.validate_password(password2, self.instance)
            except ValidationError as error:
                self.add_error('password2', error)
        return cleaned_data

    def save(self, commit=True):
        user = super().save(commit=False)
        user.set_password(self.cleaned_data['password1'])
        if commit:
            user.save()
            self.save_m2m()
        return user


class BhtomUserUpdateForm(BhtomUserBaseForm):
    password1 = forms.CharField(
        label='Password',
        strip=False,
        required=False,
        widget=forms.PasswordInput(attrs={'autocomplete': 'new-password'}),
        help_text='Leave blank to keep the current password.',
    )
    password2 = forms.CharField(
        label='Password confirmation',
        strip=False,
        required=False,
        widget=forms.PasswordInput(attrs={'autocomplete': 'new-password'}),
        help_text='Repeat the new password only if you want to change it.',
    )

    def clean(self):
        cleaned_data = super().clean()
        password1 = cleaned_data.get('password1')
        password2 = cleaned_data.get('password2')
        if password1 or password2:
            if password1 != password2:
                self.add_error('password2', 'The two password fields did not match.')
            if password2:
                try:
                    password_validation.validate_password(password2, self.instance)
                except ValidationError as error:
                    self.add_error('password2', error)
        return cleaned_data

    def save(self, commit=True):
        user = super().save(commit=False)
        if self.cleaned_data.get('password1'):
            user.set_password(self.cleaned_data['password1'])
        if commit:
            user.save()
            self.save_m2m()
        return user


class TargetAliasForm(forms.ModelForm):
    url = forms.URLField(required=False, label='Alias URL')
    source_name = forms.CharField(required=False, widget=forms.HiddenInput())

    class Meta:
        model = TargetName
        fields = ('name',)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        alias_info = getattr(self.instance, 'alias_info', None)
        if alias_info:
            if alias_info.url:
                self.fields['url'].initial = alias_info.url
            if alias_info.source_name:
                self.fields['source_name'].initial = alias_info.source_name

    def save(self, commit=True):
        alias = super().save(commit=commit)
        if not commit:
            return alias

        url = (self.cleaned_data.get('url') or '').strip()
        source_name = (self.cleaned_data.get('source_name') or '').strip()
        if url or source_name:
            TargetAliasInfo.objects.update_or_create(
                target_name=alias,
                defaults={'url': url, 'source_name': source_name},
            )
        else:
            TargetAliasInfo.objects.filter(target_name=alias).delete()
        return alias


class TargetAliasInlineFormSet(BaseInlineFormSet):
    def clean(self):
        super().clean()

        seen_names = set()
        target_name = str(getattr(self.instance, 'name', '') or '').strip().casefold()
        for form in self.forms:
            if self.can_delete and self._should_delete_form(form):
                continue
            if not hasattr(form, 'cleaned_data'):
                continue

            name = str(form.cleaned_data.get('name') or '').strip()
            if not name:
                continue

            normalized_name = name.casefold()
            if normalized_name == target_name:
                # Some data services return the canonical target name as an alias.
                # Treat that as a no-op instead of blocking unrelated target edits.
                continue
            if normalized_name in seen_names:
                raise ValidationError(f'Alias "{name}" is duplicated.')
            seen_names.add(normalized_name)

            duplicates = TargetName.objects.filter(name__iexact=name)
            if self.instance.pk:
                duplicates = duplicates.exclude(target=self.instance)
            if duplicates.exists():
                raise ValidationError(f'Alias "{name}" already exists on another target.')


BhtomTargetNamesFormset = inlineformset_factory(
    Target,
    TargetName,
    form=TargetAliasForm,
    formset=TargetAliasInlineFormSet,
    fields=('name',),
    validate_min=False,
    can_delete=True,
    extra=3,
)


class BhtomCatalogQueryForm(forms.Form):
    service = forms.ChoiceField(choices=lambda: [(key, key) for key in get_service_classes().keys()])
    term = forms.CharField(required=False, label='Object name or identifier')
    ra = forms.FloatField(required=False, label='RA (deg)')
    dec = forms.FloatField(required=False, label='Dec (deg)')
    radius_arcsec = forms.FloatField(required=False, min_value=0.1, initial=3.0, label='Search radius (arcsec)')

    def clean(self):
        cleaned = super().clean()
        service = cleaned.get('service')
        term = (cleaned.get('term') or '').strip()
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        if service == 'Simbad':
            if not term and not has_coords:
                raise forms.ValidationError('Provide SIMBAD object name or RA+Dec.')
            cleaned['radius_arcsec'] = 3.0
        elif not term:
            raise forms.ValidationError('Provide a search term.')
        return cleaned

    def get_target(self):
        service_class = get_service_classes()[self.cleaned_data['service']]
        service = service_class()
        if self.cleaned_data['service'] == 'Simbad':
            service.query(
                self.cleaned_data.get('term') or '',
                ra=self.cleaned_data.get('ra'),
                dec=self.cleaned_data.get('dec'),
                radius_arcsec=3.0,
            )
        else:
            service.query(self.cleaned_data['term'])
        return service.to_target()

    @staticmethod
    def serialize_alias_payload(target):
        aliases = getattr(target, 'extra_aliases', None) or []
        return json.dumps(aliases) if aliases else ''


class PlanetaryTransitTargetFormMixin(forms.Form):
    transit_char_field_names = (
        'source_name',
        'source_url',
        'planet_name',
        'host_name',
        'priority',
    )
    transit_field_names = (
        't0_bjd_tdb',
        't0_unc',
        'period_days',
        'period_unc',
        'duration_hours',
        'depth_r_mmag',
        'v_mag',
        'r_mag',
        'gaia_g_mag',
    )

    source_name = forms.CharField(required=False, label='Transit source name')
    source_url = forms.URLField(required=False, label='Transit source URL')
    planet_name = forms.CharField(required=False, label='Planet name')
    host_name = forms.CharField(required=False, label='Host star name')
    priority = forms.CharField(required=False, label='ExoClock priority')
    t0_bjd_tdb = forms.FloatField(required=False, label='T0 (BJD_TDB)')
    t0_unc = forms.FloatField(required=False, label='T0 uncertainty (d)')
    period_days = forms.FloatField(required=False, label='Period (d)')
    period_unc = forms.FloatField(required=False, label='Period uncertainty (d)')
    duration_hours = forms.FloatField(required=False, label='Duration (h)')
    depth_r_mmag = forms.FloatField(required=False, label='Depth (mmag)')
    v_mag = forms.FloatField(required=False, label='V mag')
    r_mag = forms.FloatField(required=False, label='R mag')
    gaia_g_mag = forms.FloatField(required=False, label='Gaia G mag')

    def _set_transit_initials(self):
        try:
            transit_ephemeris = self.instance.transit_ephemeris
        except TransitEphemeris.DoesNotExist:
            return

        for field_name in self.transit_field_names:
            value = getattr(transit_ephemeris, field_name, None)
            if value not in (None, ''):
                self.fields[field_name].initial = value

    def get_transit_ephemeris_defaults(self):
        defaults = {}
        for field_name in self.transit_char_field_names:
            defaults[field_name] = (self.cleaned_data.get(field_name) or '').strip()
        for field_name in self.transit_field_names:
            defaults[field_name] = self.cleaned_data.get(field_name)
        return defaults

class GaiaAstrometryFormMixin(forms.Form):
    parallax_error = forms.FloatField(required=False, widget=forms.HiddenInput())
    pm_ra_error = forms.FloatField(required=False, widget=forms.HiddenInput())
    pm_dec_error = forms.FloatField(required=False, widget=forms.HiddenInput())
    gaia_variability_type = forms.CharField(required=False, widget=forms.HiddenInput())


class BhtomSiderealTargetCreateForm(GaiaAstrometryFormMixin, PlanetaryTransitTargetFormMixin, SiderealTargetCreateForm):
    classification = forms.ChoiceField(
        choices=Target._meta.get_field('classification').choices,
        required=False,
        label='Classification',
    )
    recommended_observing_strategy = forms.CharField(
        label='Recommended observing strategy',
        min_length=4,
        widget=forms.Textarea(attrs={'rows': 3}),
        help_text='This will be saved as the first comment on the created target.',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name in SIDEREAL_CREATE_FORM_HIDDEN_FIELDS:
            self.fields.pop(field_name, None)
        for field_name in ('distance', 'distance_err', 'sun_separation', 'cadence_priority'):
            self.fields.pop(field_name, None)
        if not getattr(self.instance, 'pk', None):
            self.fields.pop('priority', None)
        self._set_transit_initials()

    class Meta(SiderealTargetCreateForm.Meta):
        fields = tuple(dict.fromkeys(
            tuple(getattr(SiderealTargetCreateForm.Meta, 'fields', ()))
            + ('parallax', 'parallax_error', 'pm_ra_error', 'pm_dec_error', 'gaia_variability_type')
        ))


class BhtomNonSiderealTargetCreateForm(NonSiderealTargetCreateForm):
    recommended_observing_strategy = forms.CharField(
        label='Recommended observing strategy',
        min_length=4,
        widget=forms.Textarea(attrs={'rows': 3}),
        help_text='This will be saved as the first comment on the created target.',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name in CREATE_FORM_HIDDEN_FIELDS:
            self.fields.pop(field_name, None)


class NonSiderealTargetVisibilityForm(forms.Form):
    start_time = forms.DateTimeField(
        required=True,
        label='Start Time',
        input_formats=['%Y-%m-%dT%H:%M', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S'],
        widget=forms.DateTimeInput(
            format='%Y-%m-%dT%H:%M:%S',
            attrs={'type': 'datetime-local', 'step': '1'},
        ),
    )
    end_time = forms.DateTimeField(
        required=True,
        label='End Time',
        input_formats=['%Y-%m-%dT%H:%M', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S'],
        widget=forms.DateTimeInput(
            format='%Y-%m-%dT%H:%M:%S',
            attrs={'type': 'datetime-local', 'step': '1'},
        ),
    )
    airmass = forms.DecimalField(required=False, label='Maximum Airmass', initial=2.5)

    def __init__(self, *args, **kwargs):
        initial = kwargs.setdefault('initial', {})
        now_utc = datetime.utcnow().replace(microsecond=0)
        initial.setdefault('start_time', now_utc)
        initial.setdefault('end_time', now_utc + timedelta(days=1))
        initial.setdefault('airmass', 2.5)
        super().__init__(*args, **kwargs)

    def clean(self):
        cleaned_data = super().clean()
        start_time = cleaned_data.get('start_time')
        end_time = cleaned_data.get('end_time')
        if start_time and end_time and end_time < start_time:
            raise forms.ValidationError('Start time must be before end time')
        return cleaned_data


class BhtomSiderealTargetUpdateForm(BhtomSiderealTargetCreateForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._set_transit_initials()


class BhtomPlanetaryTransitTargetCreateForm(BhtomSiderealTargetCreateForm):
    pass


class BhtomPlanetaryTransitTargetUpdateForm(BhtomSiderealTargetUpdateForm):
    pass
