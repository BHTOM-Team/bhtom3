from django import forms
from astropy.coordinates import Angle
import astropy.units as u

from tom_dataservices.forms import BaseQueryForm


COORDINATE_HELP_TEXT = 'Accepts decimal degrees or sexagesimal, e.g. 267.4128 or 17:49:39.07 / -30:27:08.4.'


class CoordinateField(forms.FloatField):
    def __init__(self, *args, coordinate_type='dec', **kwargs):
        self.coordinate_type = coordinate_type
        kwargs.setdefault('widget', forms.TextInput())
        super().__init__(*args, **kwargs)

    def to_python(self, value):
        if value in self.empty_values:
            return None
        if isinstance(value, (int, float)):
            return super().to_python(value)

        text = str(value).strip()
        if not text:
            return None

        try:
            return super().to_python(text)
        except forms.ValidationError:
            pass

        try:
            if self.coordinate_type == 'ra':
                if any(token in text.lower() for token in ('h', 'm', 's', ':')):
                    return Angle(text, unit=u.hourangle).degree
                return Angle(float(text), unit=u.deg).degree
            return Angle(text, unit=u.deg).degree
        except Exception as exc:
            raise forms.ValidationError(
                f'Enter a valid {self.coordinate_type.upper()} in decimal degrees or sexagesimal format.'
            ) from exc


def ra_field():
    return CoordinateField(required=False, coordinate_type='ra', label='RA', help_text=COORDINATE_HELP_TEXT)


def dec_field():
    return CoordinateField(required=False, coordinate_type='dec', label='Dec', help_text=COORDINATE_HELP_TEXT)


class GaiaDR3QueryForm(BaseQueryForm):
    source_id = forms.CharField(required=False, label='Gaia DR3 source_id')
    ra = ra_field()
    dec = dec_field()
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
    ra = ra_field()
    dec = dec_field()
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
    alert_name = forms.CharField(
        required=False,
        label='Gaia Alerts name',
        help_text='You can enter either the full name, e.g. Gaia24amo, or just 24amo.',
    )
    ra = ra_field()
    dec = dec_field()
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


class OGLEEWSQueryForm(BaseQueryForm):
    target_name = forms.CharField(
        required=False,
        label='OGLE EWS name',
        help_text='You can enter 2011-BLG-0001 or OGLE-2011-BLG-0001.',
    )
    ra = ra_field()
    dec = dec_field()
    radius_arcsec = forms.FloatField(required=False, initial=5.0, min_value=0.1, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include lightcurve photometry')

    def clean(self):
        cleaned = super().clean()
        has_name = bool((cleaned.get('target_name') or '').strip())
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        if not has_name and not has_coords:
            raise forms.ValidationError('Provide OGLE EWS name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned


class ExoClockQueryForm(BaseQueryForm):
    target_name = forms.CharField(required=False, label='ExoClock planet name')
    ra = ra_field()
    dec = dec_field()
    radius_arcsec = forms.FloatField(required=False, initial=30.0, min_value=0.1, label='Search radius (arcsec)')

    def clean(self):
        cleaned = super().clean()
        has_name = bool((cleaned.get('target_name') or '').strip())
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        if not has_name and not has_coords:
            raise forms.ValidationError('Provide ExoClock target name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 30.0
        return cleaned


class CRTSQueryForm(BaseQueryForm):
    ra = ra_field()
    dec = dec_field()
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
    ra = ra_field()
    dec = dec_field()
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
    ra = ra_field()
    dec = dec_field()
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
    ra = ra_field()
    dec = dec_field()
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

class GS6dFQueryForm(BaseQueryForm):
    ra = ra_field()
    dec = dec_field()
    radius_arcmin = forms.FloatField(required=False, initial=5.0, min_value=0.01, label='Search radius (arcsec)')
    include_spectroscopy = forms.BooleanField(required=False, initial=True, label='Include spectroscopy')

    def clean(self):
        cleaned = super().clean()
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        if not has_coords:
            raise forms.ValidationError('Provide RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned
    
class DESIQueryForm(BaseQueryForm):
    ra = ra_field()
    dec = dec_field()
    radius_arcmin = forms.FloatField(required=False, initial=5.0, min_value=0.01, label='Search radius (arcsec)')
    include_spectroscopy = forms.BooleanField(required=False, initial=True, label='Include spectroscopy')

    def clean(self):
        cleaned = super().clean()
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        if not has_coords:
            raise forms.ValidationError('Provide RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned
    
class ASASSNQueryForm(BaseQueryForm):
    ra = ra_field()
    dec = dec_field()
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
    
class PanSTARRSQueryForm(BaseQueryForm):
    ra = ra_field()
    dec = dec_field()
    radius_arcmin = forms.FloatField(required=False, initial=2.0, min_value=0.01, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        if not has_coords:
            raise forms.ValidationError('Provide RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 2.0
        return cleaned

class WISEQueryForm(BaseQueryForm):
    ra = ra_field()
    dec = dec_field()
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


class PhotometricClassificationQueryForm(BaseQueryForm):
    ra = ra_field()
    dec = dec_field()

    def clean(self):
        cleaned = super().clean()
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        if not has_coords:
            raise forms.ValidationError('Provide RA+Dec.')
        return cleaned


class SimbadQueryForm(BaseQueryForm):
    ra = ra_field()
    dec = dec_field()
    radius_arcsec = forms.FloatField(required=False, initial=3.0, min_value=0.1, label='Search radius (arcsec)')

    def clean(self):
        cleaned = super().clean()
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        if not has_coords:
            raise forms.ValidationError('Provide RA+Dec.')
        cleaned['radius_arcsec'] = 3.0
        return cleaned

class SDSSQueryForm(BaseQueryForm):
    ra = ra_field()
    dec = dec_field()
    radius_arcsec = forms.FloatField(required=False, initial=10.0, min_value=0.05, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')
    include_spectroscopy = forms.BooleanField(required=False, initial=True, label='Include spectra')

    def clean(self):
        cleaned = super().clean()
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        if not has_coords:
            raise forms.ValidationError('Provide Gaia source_id or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 10.0
        return cleaned

class PTFQueryForm(BaseQueryForm):
    ra = ra_field()
    dec = dec_field()
    radius_arcmin = forms.FloatField(required=False, initial=3.0, min_value=0.01, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        if not has_coords:
            raise forms.ValidationError('Provide RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 3.0
        return cleaned

class LCOSpectraQueryForm(BaseQueryForm):
    ra = ra_field()
    dec = dec_field()
    radius_arcmin = forms.FloatField(required=False, initial=5.0, min_value=0.01, label='Search radius (arcsec)')
    include_spectroscopy = forms.BooleanField(required=False, initial=True, label='Include spectroscopy')

    def clean(self):
        cleaned = super().clean()
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        if not has_coords:
            raise forms.ValidationError('Provide RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned

class ZTFQueryForm(BaseQueryForm):
    ra = ra_field()
    dec = dec_field()
    radius_arcmin = forms.FloatField(required=False, initial=1.1, min_value=0.01, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        if not has_coords:
            raise forms.ValidationError('Provide RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 1.1
        return cleaned
