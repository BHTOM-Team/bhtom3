import json

from django import forms
from django.forms import inlineformset_factory

from tom_catalogs.harvester import get_service_classes
from tom_targets.forms import NonSiderealTargetCreateForm, SiderealTargetCreateForm
from tom_targets.models import Target, TargetName

from custom_code.models import TargetAliasInfo


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


BhtomTargetNamesFormset = inlineformset_factory(
    Target,
    TargetName,
    form=TargetAliasForm,
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


class BhtomSiderealTargetCreateForm(SiderealTargetCreateForm):
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


PUBLIC_UPLOAD_FILTER_CHOICES = [
    ('2MASS/J', '2MASS/J'),
    ('2MASS/H', '2MASS/H'),
    ('2MASS/K', '2MASS/K'),
    ('2MASS/any', '2MASS/any'),
    ('GaiaSP/u', 'GaiaSP/u'),
    ('GaiaSP/g', 'GaiaSP/g'),
    ('GaiaSP/r', 'GaiaSP/r'),
    ('GaiaSP/i', 'GaiaSP/i'),
    ('GaiaSP/z', 'GaiaSP/z'),
    ('GaiaSP/U', 'GaiaSP/U'),
    ('GaiaSP/B', 'GaiaSP/B'),
    ('GaiaSP/V', 'GaiaSP/V'),
    ('GaiaSP/R', 'GaiaSP/R'),
    ('GaiaSP/I', 'GaiaSP/I'),
    ('GaiaSP/any', 'GaiaSP/any'),
    ('GaiaSP/ugriz', 'GaiaSP/ugriz'),
    ('GaiaSP/UBVRI', 'GaiaSP/UBVRI'),
    ('GaiaDR3/any', 'GaiaDR3/any'),
    ('GaiaDR3/G', 'GaiaDR3/G'),
    ('GaiaDR3/GBP', 'GaiaDR3/GBP'),
    ('GaiaDR3/GRP', 'GaiaDR3/GRP'),
]


class PublicFitsUploadForm(forms.Form):
    target = forms.CharField(max_length=255)
    observer = forms.CharField(max_length=255)
    token = forms.CharField(widget=forms.PasswordInput(render_value=True))
    observatory = forms.CharField(max_length=255, label='Observatory')
    calibration_filter = forms.ChoiceField(
        label='Filter',
        choices=PUBLIC_UPLOAD_FILTER_CHOICES,
        initial='GaiaSP/any',
    )
    comment = forms.CharField(required=False, widget=forms.Textarea(attrs={'rows': 3}))
    fits_file = forms.FileField(label='FITS file')

    def clean_fits_file(self):
        fits_file = self.cleaned_data['fits_file']
        filename = (fits_file.name or '').lower()
        if not filename.endswith(('.fits', '.fit', '.fts', '.fz')):
            raise forms.ValidationError('Upload a FITS file.')
        return fits_file


class PublicUploadAccessForm(forms.Form):
    password = forms.CharField(widget=forms.PasswordInput(), strip=False)
