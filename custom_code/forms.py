from django import forms


class GeoTomAddSatForm(forms.Form):
    norad_id = forms.IntegerField(min_value=1, label="NORAD ID")
