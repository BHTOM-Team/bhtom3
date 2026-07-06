from django import forms
from astropy.coordinates import Angle
import astropy.units as u


COORDINATE_HELP_TEXT = (
    'Accepts decimal degrees or sexagesimal. Bare numeric RA is decimal degrees, not decimal hours; '
    'use sexagesimal for hour notation, e.g. 267.4128 or 17:49:39.07 / -30:27:08.4.'
)


class CoordinateField(forms.FloatField):
    """
    Coordinate field that stores decimal degrees.

    Bare numeric values are always interpreted as decimal degrees. Non-numeric
    RA values are interpreted as sexagesimal hours; Dec values are interpreted
    as sexagesimal degrees.
    """

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
                return Angle(text, unit=u.hourangle).degree
            return Angle(text, unit=u.deg).degree
        except Exception as exc:
            raise forms.ValidationError(
                f'Enter a valid {self.coordinate_type.upper()} in decimal degrees or sexagesimal format.'
            ) from exc


def ra_field(*, required=False, label='RA', help_text=COORDINATE_HELP_TEXT):
    return CoordinateField(required=required, coordinate_type='ra', label=label, help_text=help_text)


def dec_field(*, required=False, label='Dec', help_text=COORDINATE_HELP_TEXT):
    return CoordinateField(required=required, coordinate_type='dec', label=label, help_text=help_text)
