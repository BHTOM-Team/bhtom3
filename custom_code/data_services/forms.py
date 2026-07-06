from datetime import timezone as datetime_timezone

from django import forms
from django.utils import timezone

from tom_dataservices.forms import BaseQueryForm
from custom_code.coordinate_fields import COORDINATE_HELP_TEXT, CoordinateField, dec_field, ra_field
from custom_code.data_services.service_utils import TARGET_NAME_HELP_TEXT


def target_name_field(label='Target name'):
    return forms.CharField(required=False, label=label, help_text=TARGET_NAME_HELP_TEXT)


def has_target_name(cleaned):
    return bool((cleaned.get('target_name') or '').strip())


def has_coords(cleaned):
    return cleaned.get('ra') is not None and cleaned.get('dec') is not None


class AllDataServicesQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()
    radius_arcsec = forms.FloatField(required=False, initial=3.0, min_value=0.1, label='Search radius (arcsec)')

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 3.0
        return cleaned


class GaiaDR3QueryForm(BaseQueryForm):
    target_name = target_name_field()
    source_id = forms.CharField(required=False, label='Gaia DR3 source_id')
    ra = ra_field()
    dec = dec_field()
    radius_arcsec = forms.FloatField(required=False, initial=1.0, min_value=0.05, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include epoch photometry')
    include_spectroscopy = forms.BooleanField(required=False, initial=True, label='Include BP/RP XP spectra')

    def clean(self):
        cleaned = super().clean()
        has_id = bool((cleaned.get('source_id') or '').strip())
        if not has_id and not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name, Gaia source_id or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 1.0
        return cleaned


class LSSTQueryForm(BaseQueryForm):
    target_name = target_name_field()
    dia_object_id = forms.CharField(required=False, label='LSST diaObjectId')
    ra = ra_field()
    dec = dec_field()
    radius_arcsec = forms.FloatField(required=False, initial=2.0, min_value=0.1, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        has_id = bool((cleaned.get('dia_object_id') or '').strip())
        if not has_id and not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name, LSST diaObjectId or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned


class GaiaAlertsQueryForm(BaseQueryForm):
    target_name = target_name_field()
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
        if not has_name and not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name, Gaia Alerts name or RA+Dec.')
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


class MOAQueryForm(BaseQueryForm):
    target_name = forms.CharField(
        required=False,
        label='MOA event name',
        help_text='You can enter 2019-BLG-397, MOA-2019-BLG-397, or use RA+Dec.',
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
            raise forms.ValidationError('Provide MOA event name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned


class KMTQueryForm(BaseQueryForm):
    target_name = forms.CharField(
        required=False,
        label='KMT event name',
        help_text='You can enter KMT-2017-BLG-2573 or use RA+Dec.',
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
            raise forms.ValidationError('Provide KMT event name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned


class ExoClockQueryForm(BaseQueryForm):
    target_name = forms.CharField(required=False, label='ExoClock planet name')
    ra = ra_field()
    dec = dec_field()
    radius_arcsec = forms.FloatField(required=False, initial=30.0, min_value=0.1, label='Search radius (arcsec)')
    magnitude_limit = forms.FloatField(
        required=False,
        label='Magnitude limit',
        help_text='Maximum host-star V magnitude.',
    )
    eclipse_depth_min = forms.FloatField(
        required=False,
        label='Eclipse depth min (mmag)',
        help_text='Minimum transit depth in millimagnitudes.',
    )
    declination_min = forms.FloatField(required=False, min_value=-90.0, max_value=90.0, label='Declination min (deg)')
    declination_max = forms.FloatField(required=False, min_value=-90.0, max_value=90.0, label='Declination max (deg)')
    sun_distance_min = forms.FloatField(
        required=False,
        initial=90.0,
        min_value=0.0,
        max_value=180.0,
        label='Sun distance min (deg)',
        help_text='Minimum Sun-target separation at the compute time.',
    )
    compute_from_date = forms.DateTimeField(
        required=False,
        label='Compute from date (UTC)',
        widget=forms.DateTimeInput(attrs={'type': 'datetime-local', 'step': 1}),
        input_formats=['%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M'],
    )
    transit_within_days = forms.FloatField(
        required=False,
        initial=1.0,
        min_value=0.0,
        label='Next transit within (days)',
        help_text='Return only planets whose next transit occurs within this many days from the compute time.',
    )

    def __init__(self, *args, **kwargs):
        initial = kwargs.setdefault('initial', {})
        now_utc = timezone.now().astimezone(datetime_timezone.utc).replace(tzinfo=None, microsecond=0)
        initial.setdefault('compute_from_date', now_utc)
        initial.setdefault('sun_distance_min', 90.0)
        initial.setdefault('transit_within_days', 1.0)
        super().__init__(*args, **kwargs)

    def clean(self):
        cleaned = super().clean()
        has_name = bool((cleaned.get('target_name') or '').strip())
        has_coords = cleaned.get('ra') is not None and cleaned.get('dec') is not None
        has_advanced = any(
            cleaned.get(field_name) not in (None, '')
            for field_name in (
                'magnitude_limit',
                'eclipse_depth_min',
                'declination_min',
                'declination_max',
                'sun_distance_min',
                'compute_from_date',
                'transit_within_days',
            )
        )
        if not has_name and not has_coords and not has_advanced:
            raise forms.ValidationError('Provide ExoClock target name, RA+Dec, or at least one advanced filter.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 30.0
        if (
            cleaned.get('declination_min') is not None
            and cleaned.get('declination_max') is not None
            and cleaned['declination_min'] > cleaned['declination_max']
        ):
            self.add_error('declination_max', 'Declination max must be greater than or equal to declination min.')
        if cleaned.get('compute_from_date') and cleaned.get('transit_within_days') is None:
            self.add_error('transit_within_days', 'Provide a day range when using Compute from date.')
        return cleaned


class CRTSQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()
    radius_arcmin = forms.FloatField(required=False, initial=0.1, min_value=0.01, label='Search radius (arcmin)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        if cleaned.get('radius_arcmin') is None:
            cleaned['radius_arcmin'] = 0.1
        return cleaned
    
class SkyMapperQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()
    radius_arcmin = forms.FloatField(required=False, initial=5.0, min_value=0.01, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned


class SwiftUVOTQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()
    radius_arcmin = forms.FloatField(required=False, initial=5.0, min_value=0.01, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned
    
class GalexQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()
    radius_arcmin = forms.FloatField(required=False, initial=5.0, min_value=0.01, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned

class GS6dFQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()
    radius_arcmin = forms.FloatField(required=False, initial=5.0, min_value=0.01, label='Search radius (arcsec)')
    include_spectroscopy = forms.BooleanField(required=False, initial=True, label='Include spectroscopy')

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned
    
class DESIQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()
    radius_arcmin = forms.FloatField(required=False, initial=5.0, min_value=0.01, label='Search radius (arcsec)')
    include_spectroscopy = forms.BooleanField(required=False, initial=True, label='Include spectroscopy')

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned
    
class ASASSNQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()
    radius_arcsec = forms.FloatField(required=False, initial=7.0, min_value=0.01, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 7.0
        return cleaned
    
class PanSTARRSQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()
    radius_arcmin = forms.FloatField(required=False, initial=2.0, min_value=0.01, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 2.0
        return cleaned

class WISEQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()
    radius_arcmin = forms.FloatField(required=False, initial=5.0, min_value=0.01, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned


class PhotometricClassificationQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        return cleaned


class SimbadQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()
    radius_arcsec = forms.FloatField(required=False, initial=3.0, min_value=0.1, label='Search radius (arcsec)')

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        cleaned['radius_arcsec'] = 3.0
        return cleaned

class SDSSQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()
    radius_arcsec = forms.FloatField(required=False, initial=10.0, min_value=0.05, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')
    include_spectroscopy = forms.BooleanField(required=False, initial=True, label='Include spectra')

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 10.0
        return cleaned

class PTFQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()
    radius_arcmin = forms.FloatField(required=False, initial=3.0, min_value=0.01, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 3.0
        return cleaned

class LCOSpectraQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()
    radius_arcmin = forms.FloatField(required=False, initial=5.0, min_value=0.01, label='Search radius (arcsec)')
    include_spectroscopy = forms.BooleanField(required=False, initial=True, label='Include spectroscopy')

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned

class ZTFQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()
    radius_arcmin = forms.FloatField(required=False, initial=1.1, min_value=0.01, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 1.1
        return cleaned

class ESOSpectraQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()
    radius_arcmin = forms.FloatField(required=False, initial=5.0, min_value=0.01, label='Search radius (arcsec)')
    include_spectroscopy = forms.BooleanField(required=False, initial=True, label='Include spectroscopy')

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 5.0
        return cleaned

class HSTQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()
    radius_arcmin = forms.FloatField(required=False, initial=5.0, min_value=0.01, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 1.1
        return cleaned

class JVARQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()
    radius_arcsec = forms.FloatField(required=False, initial=3.0, min_value=0.01, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 3.0
        return cleaned


class FAVAQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        return cleaned


class FRAMQueryForm(BaseQueryForm):
    target_name = target_name_field()
    ra = ra_field()
    dec = dec_field()
    radius_arcsec = forms.FloatField(required=False, initial=3.0, min_value=0.1, max_value=300.0, label='Search radius (arcsec)')
    include_photometry = forms.BooleanField(required=False, initial=True, label='Include photometry')
    night1 = forms.CharField(required=False, label='Not before', help_text='YYYYMMDD. Leave blank for the full first-ingest range.')
    night2 = forms.CharField(required=False, label='Not after', help_text='YYYYMMDD. Leave blank for today.')

    def clean(self):
        cleaned = super().clean()
        if not has_target_name(cleaned) and not has_coords(cleaned):
            raise forms.ValidationError('Provide target name or RA+Dec.')
        if cleaned.get('radius_arcsec') is None:
            cleaned['radius_arcsec'] = 3.0
        for field_name in ('night1', 'night2'):
            value = (cleaned.get(field_name) or '').strip()
            if value and (len(value) != 8 or not value.isdigit()):
                self.add_error(field_name, 'Use YYYYMMDD.')
            cleaned[field_name] = value
        return cleaned
