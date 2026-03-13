from django import forms

from tom_dataservices.forms import BaseQueryForm


class GaiaDR3QueryForm(BaseQueryForm):
    source_id = forms.CharField(required=False, label='Gaia DR3 source_id')
    ra = forms.FloatField(required=False, label='RA (deg)')
    dec = forms.FloatField(required=False, label='Dec (deg)')
    radius_arcsec = forms.FloatField(required=False, initial=1.0, min_value=0.05, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include epoch photometry')
    include_spectroscopy = forms.BooleanField(required=False, initial=True, label='Include BP/RP XP spectra')

    def clean(self):
        cleaned = super().clean()
        has_id = bool((cleaned.get('source_id') or '').strip())
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        if not has_id and not has_coords:
            raise forms.ValidationError('Provide Gaia source_id or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 1.0
        return cleaned


class LSSTQueryForm(BaseQueryForm):
    dia_object_id = forms.CharField(required=False, label='LSST diaObjectId')
    ra = forms.FloatField(required=False, label='RA (deg)')
    dec = forms.FloatField(required=False, label='Dec (deg)')
    radius_arcsec = forms.FloatField(required=False, initial=5.0, min_value=0.1, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        has_id = bool((cleaned.get('dia_object_id') or '').strip())
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        if not has_id and not has_coords:
            raise forms.ValidationError('Provide LSST diaObjectId or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned


class GaiaAlertsQueryForm(BaseQueryForm):
    alert_name = forms.CharField(required=False, label='Gaia Alerts name')
    ra = forms.FloatField(required=False, label='RA (deg)')
    dec = forms.FloatField(required=False, label='Dec (deg)')
    radius_arcsec = forms.FloatField(required=False, initial=5.0, min_value=0.1, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include lightcurve photometry')

    def clean(self):
        cleaned = super().clean()
        has_name = bool((cleaned.get('alert_name') or '').strip())
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        if not has_name and not has_coords:
            raise forms.ValidationError('Provide Gaia Alerts name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned


class CRTSQueryForm(BaseQueryForm):
    ra = forms.FloatField(required=False, label='RA (deg)')
    dec = forms.FloatField(required=False, label='Dec (deg)')
    radius_arcmin = forms.FloatField(required=False, initial=0.1, min_value=0.01, label='Search radius (arcmin)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        if not has_coords:
            raise forms.ValidationError('Provide RA+Dec.')
        if cleaned.get('radius_arcmin') is None:
            cleaned['radius_arcmin'] = 0.1
        return cleaned
    
class SkyMapperQueryForm(BaseQueryForm):
    ra = forms.FloatField(required=False, label='RA (deg)')
    dec = forms.FloatField(required=False, label='Dec (deg)')
    radius_arcmin = forms.FloatField(required=False, initial=5.0, min_value=0.01, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        if not has_coords:
            raise forms.ValidationError('Provide RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned


class SwiftUVOTQueryForm(BaseQueryForm):
    ra = forms.FloatField(required=False, label='RA (deg)')
    dec = forms.FloatField(required=False, label='Dec (deg)')
    radius_arcmin = forms.FloatField(required=False, initial=5.0, min_value=0.01, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        if not has_coords:
            raise forms.ValidationError('Provide RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned
    
class GalexQueryForm(BaseQueryForm):
    ra = forms.FloatField(required=False, label='RA (deg)')
    dec = forms.FloatField(required=False, label='Dec (deg)')
    radius_arcmin = forms.FloatField(required=False, initial=5.0, min_value=0.01, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        if not has_coords:
            raise forms.ValidationError('Provide RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned
