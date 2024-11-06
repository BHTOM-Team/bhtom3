from datetime import datetime, timedelta
from django import forms
from crispy_forms.layout import Column, Div, HTML, Layout, Row, MultiWidgetField, Fieldset

from tom_observations.facility import BaseRoboticObservationFacility, BaseRoboticObservationForm
from tom_observations.widgets import FilterField
from tom_observations.cadence import CadenceForm
from tom_targets.models import Target


SUCCESSFUL_OBSERVING_STATES = ['COMPLETED']
FAILED_OBSERVING_STATES = ['WINDOW_EXPIRED', 'CANCELED', 'FAILURE_LIMIT_REACHED', 'NOT_ATTEMPTED']
TERMINAL_OBSERVING_STATES = SUCCESSFUL_OBSERVING_STATES + FAILED_OBSERVING_STATES

valid_instruments = ['ROS2']
valid_filters = [['griz+J','griz+J'],['griz+H','griz+H'],['griz+Ks','griz+Ks']] #griz are always used in REM + infrared filter
exposure_times = {}


class REMPhotometricSequenceForm(BaseRoboticObservationForm):
    name = forms.CharField()
    start = forms.CharField(widget=forms.TextInput(attrs={'type': 'date'}))
    end = forms.CharField(required=False, widget=forms.TextInput(attrs={'type': 'date'}))
    observation_id = forms.CharField(required=False)
    observation_params = forms.CharField(required=False, widget=forms.Textarea(attrs={'type': 'json'}))

    exposure_time = forms.FloatField(initial=100) # in sec
    exposure_count = forms.IntegerField(initial=1)
    cadence_in_days = forms.FloatField(initial=1)  # in days
    filters = forms.ChoiceField(required=True, label='Filters', choices=valid_filters)

    def __init__(self, *args, **kwargs):
        # Set default values for 'start', 'end', and 'name' in initial_data
        initial_data = kwargs.get('initial', {})
        current_date = datetime.now().strftime('%Y-%m-%d')
        next_day = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        
        initial_data.setdefault('start', current_date)
        initial_data.setdefault('end', next_day)
        kwargs['initial'] = initial_data
        
        super().__init__(*args, **kwargs)

        target = Target.objects.get(id=self.initial.get('target_id'))
        initial_data.setdefault('name', f'BHTOM_REM_{target.name}')
        kwargs['initial'] = initial_data

        # Precompute exposure time for each filter option
        mag = target.mag_last
        instrument = "ROS2"
        for filter_option, _ in valid_filters:
            exposure_times[filter_option] = self.exposure_time_calculator(
                mag=mag, filter_name=filter_option, instrument=instrument
            )
        
        # Set initial exposure time based on the first filter choice
        first_filter = self.fields['filters'].initial or valid_filters[0][0]
        initial_data.setdefault('exposure_time', exposure_times.get(first_filter))
        kwargs['initial'] = initial_data
        super().__init__(*args, **kwargs)

    def clean(self):
        cleaned_data = super().clean()
        selected_filter = cleaned_data.get('filters')
        if selected_filter:
            # Set the computed exposure_time directly in the form field : TODO does not work
            self.fields['exposure_time'].initial = exposure_times.get(selected_filter)
        return cleaned_data

    def layout(self):
        # Display a table of filters and exposure times
        filter_rows = "".join(f"<tr><td>{filter_option}</td><td>{exposure_times.get(filter_option)}</td></tr>" for filter_option, _ in valid_filters)

        return Div(
            Div('name', 'observation_id'),
            Div(
                Div('start', css_class='col'),
                Div('end', css_class='col'),
                css_class='form-row'
            ),
            Div('filters'),
            HTML(f"<h6>Suggested exposure times</h6><table><tr><th>Filter</th><th>Exposure Time</th></tr>{filter_rows}</table>"),
            Div('exposure_time'),
            Div('exposure_count'),
            Div('cadence_in_days'),
            Div('observation_params')
        )
    
    #     #TODO: add S/N parameter (default = 100?)
    def exposure_time_calculator(self, mag, filter_name, instrument):
        if instrument not in valid_instruments:
            return -1
        if filter_name in [item for sublist in valid_filters for item in sublist]:
            pass
        else:
            return -1

        # Define a base exposure time for each filter
        filter_base_exposure_times = {
            'griz+J': 100,   # Example base exposure time for griz+J filter
            'griz+H': 120,   # Example base exposure time for griz+H filter
            'griz+Ks': 140   # Example base exposure time for griz+Ks filter
        }

        # Get the base exposure time for the selected filter
        base_exposure_time = filter_base_exposure_times.get(filter_name, 100)  # Default to 100 if not found
        adjusted_exposure_time = base_exposure_time * (10**((mag-14)/2.5))
        return adjusted_exposure_time


class REM(BaseRoboticObservationFacility):
    name = 'REM'
    SITES = {
        'REM': {
            'sitecode': 'REM',
            'latitude': -29.26,
            'longitude': -70.73,
            'elevation': 2400
        }
    }
    observation_forms = {
        'PHOTOMETRIC_SEQUENCE': REMPhotometricSequenceForm,
    }

    def data_products(self, observation_id, product_id=None):
       return []

    def get_form(self, observation_type):
        return self.observation_forms['PHOTOMETRIC_SEQUENCE']

    def get_observation_status(self, observation_id):
        return ['IN_PROGRESS']

    def get_observation_url(self, observation_id):
        return ''

    def get_observing_sites(self):
        return self.SITES

    def get_terminal_observing_states(self):
        return TERMINAL_OBSERVING_STATES

    def submit_observation(self, observation_payload):
        print(observation_payload)
        return []

    def validate_observation(self, observation_payload):
        pass

# # generate text of the email
#     def generate_email_text(params...):


