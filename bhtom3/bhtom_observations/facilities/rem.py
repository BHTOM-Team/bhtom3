from datetime import datetime, timedelta
from django import forms
from crispy_forms.layout import Column, Div, HTML, Layout, Row, MultiWidgetField, Fieldset

from tom_observations.facility import BaseRoboticObservationFacility, BaseRoboticObservationForm
from tom_observations.widgets import FilterField
from tom_observations.cadence import CadenceForm
from tom_targets.models import Target

from custom_code.models import BhtomTarget


SUCCESSFUL_OBSERVING_STATES = ['COMPLETED']
FAILED_OBSERVING_STATES = ['WINDOW_EXPIRED', 'CANCELED', 'FAILURE_LIMIT_REACHED', 'NOT_ATTEMPTED']
TERMINAL_OBSERVING_STATES = SUCCESSFUL_OBSERVING_STATES + FAILED_OBSERVING_STATES

valid_instruments = ['ROS2']
valid_filters = [['griz+JHK','griz+JHK'],['griz+J','griz+J'],['griz+H','griz+H'],['griz+Ks','griz+Ks']] #griz are always used in REM + infrared filter
#z_IRCam, J_IRCam, H_IRCam, K_IRCam,
        # H2_IRCam, JH_IRCam, JKI_RCam, HK_IRCam, JHK_IRCam, KH2_IRCam
#exposure_times = {}
#mag_init = 99.

class REMPhotometricSequenceForm(BaseRoboticObservationForm):
#    name = forms.CharField()
    start = forms.CharField(widget=forms.TextInput(attrs={'type': 'date'}))
    end = forms.CharField(required=False, widget=forms.TextInput(attrs={'type': 'date'}))
    #ADD PROPOSAL ID/PI
#    observation_id = forms.CharField(required=False)
#    observation_params = forms.CharField(required=False, widget=forms.Textarea(attrs={'type': 'json'}))

    exposure_time = forms.FloatField(label="Exposure time Opt [s]",initial=100,help_text="in sec per optical exposure") # in sec
    exposure_count = forms.IntegerField(initial=1, help_text="number of optical exposures per visit") # number of exposures per visit

    exposure_time_ir = forms.FloatField(label="Exposure time IR [s]",initial=20,help_text="in sec per IR exposure") # in sec
    exposure_count_ir = forms.FloatField(label="Number of NDIT in IR",initial=5,help_text="number of dithers in IR per exposure") 
   
    cadence = forms.FloatField(initial=1,help_text="days until next visit")  # in days to next visit
    filter = forms.ChoiceField(required=True, label='Filters', choices=valid_filters)

    mag_init=99.
    exposure_times = {}

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
        # initial_data.setdefault('name', f'BHTOM_REM_{target.name}')
        # kwargs['initial'] = initial_data

        # Precompute exposure time for each filter option
        self.mag_init = target.mag_last

        self.exposure_times = {}

        instrument = "ROS2"
        for filter_option, _ in valid_filters:
            self.exposure_times[filter_option] = self.exposure_time_calculator(
                mag=self.mag_init, filter_name=filter_option, instrument=instrument
            )
        
        # Set initial exposure time based on the first filter choice
        first_filter = self.fields['filter'].initial or valid_filters[0][0]
        initial_data.setdefault('exposure_time', self.exposure_times.get(first_filter))
        kwargs['initial'] = initial_data
        super().__init__(*args, **kwargs)

    def clean(self):
        cleaned_data = super().clean()
        selected_filter = cleaned_data.get('filter')
        if selected_filter:
            # Set the computed exposure_time directly in the form field : TODO does not work
            self.fields['exposure_time'].initial = self.exposure_times.get(selected_filter)
        return cleaned_data

    def layout(self):
        # Display a table of filters and exposure times
        filter_rows = "".join(f"<tr><td>{filter_option}</td><td>{self.exposure_times.get(filter_option)}</td></tr>" for filter_option, _ in valid_filters)
        mag = self.mag_init
        return Div(
#            Div('name'),
            Div(
                Div('start', css_class='col'),
                Div('end', css_class='col'),
                css_class='form-row'
            ),
            Div('filter'),
            HTML(f"<h6><i>Suggested exposure times for mag={mag}</i></h6><small><table><tr><th>Filter</th><th>Exposure Time</th></tr>{filter_rows}</table></small>"),
            Div('exposure_time'),
            Div('exposure_count'),
            Div('exposure_time_ir'),
            Div('exposure_count_ir'),
            Div('cadence'),
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

    def validate_observation(self, observation_payload):
        pass

    def submit_observation(self, observation_payload):
        #print(observation_payload)
        # Retrieve target information using the target_id
        target_id = observation_payload['target_id']
        target = BhtomTarget.objects.get(id=target_id)

        # Extract target details
        # removing spaces in target name (REM requirement)
        target_name = target.name.replace(" ", "_")  # or use .replace(" ", "")
        ra = target.ra
        dec = target.dec

        template = """
        [STARTREMOB]
        # Target data

        [TARGET]

        # no spaces are allowed in name
        TargetName: {target_name}

        # RA degrees.dddd, J2000
        RA: {ra}

        # DEC degrees.dddd, J2000
        DEC: {dec}

        # Equinox year.dd (this parameter is optional, else is 2000.0)
        Equinox: 2000.0

        # Optical camera data
        [ROSS]

        # 1 if optical data are desidered, else 0
        OptFlag: 1

        # seconds, total requested time must be less than 1 hour
        Exptime: {exptime}

        # Camera focus (optional)
        OptFocus: 0

        # CCD sensitivity (optional)
        # Sensitivity options:
        # CCDslowsens, CCDslowhigh, CCDfastsens, CCDfasthigh, CCDultrasens, CCDultrahigh
        OptSensitivity: CCDslowsens

        # number of exposures
        OptNInt: {expcount}


        # Infrared camera data

        [REMIR]

        # 1 if infrared data are desidered, else 0
        IRFlag: 1

        # detector integration time, seconds
        DIT: {exptime_ir}

        # filter
        IRFilter: {ir_filter}

        # number of exposure DITx5 long
        IRNInt: {expcount_ir}

        # Available filter are: z_IRCam, J_IRCam, H_IRCam, K_IRCam,
        # H2_IRCam, JH_IRCam, JKI_RCam, HK_IRCam, JHK_IRCam, KH2_IRCam


        # PI data

        [PI]

        # PI name, no spaces are allowed
        PIName: Mariusz Gromadzki

        # PI institute, no spaces are allowed
        PIInst: Warsaw

        # PI e-mail
        PIEmail: mgromadzki@astrouw.edu.pl



        # Observation data and access permission

        [DATA]

        # your proposal Id
        PropId: 14004

        # Password for OBS activation
        PassWd: Obs4All

        # Minimum airmass (this item is optional)
        MinAirmass: 0.0

        # Maximum airmass (this item is optional)
        MaxAirmass: 2.0

        # Minimum Julian Date (this item is optional, 0 means no constraints)
        MinJD: {start_jd}

        # Maximum Julian Date (this item is optional, 0 means no constraints)
        MaxJD: {end_jd}

        # Strict starting Julian Date (this item is optional, 0 means no constraints)
        StrictJD: 0.

        # Maximum Moon fraction (this item is optional)
        MaxMoonFraction: 1.0

        # Periodical target? (this item is optional, 0 means no periodicity)
        PeriodicalTarget: 1

        # Period (this item is optional, days)
        Period: {cadence}

        # Priority (this item is optional, 0 is the maximum priority, then 1, 2, etc.)
        Priority: 1



        # Jitter data (optional)

        [JITTER]

        # Number of jittered OBs to create
        JitteredOBs: 5

        # Min jittering radius (arcmin)
        MinJitteringRadius: 0.1

        # Max jittering radius (arcmin)
        MaxJitteringRadius: 2.0

        [ENDREMOB]
        """

        # Get start and end dates from observation_payload
        start_date_str = observation_payload['params']['start']
        end_date_str = observation_payload['params']['end']

        # Convert to Julian Dates
        start_jd = self.date_to_julian_date(start_date_str)
        end_jd = self.date_to_julian_date(end_date_str)

        selected_filter = observation_payload['params']['filter']
        # Parse the selected filter to get the infrared part
        filter_parts = selected_filter.split('+')
        if len(filter_parts) > 1:
            filter_ir = f"{filter_parts[1]}_IRCam"
        else:
            # Handle case if there is no "+" in the filter (optional)
            filter_ir = "JHK_IRCam"  # default value

        # Format the template
        filled_template = template.format(
            target_name=target_name,
            ra=ra,
            dec=dec,
            cadence = observation_payload['params']['cadence'],
            exptime = observation_payload['params']['exposure_time'],
            exptime_ir = observation_payload['params']['exposure_time_ir'],
            expcount = observation_payload['params']['exposure_count'],
            expcount_ir = observation_payload['params']['exposure_count_ir'],
            start_jd=start_jd,
            end_jd=end_jd,
            ir_filter=filter_ir

        )

        # Now, the filled_template contains the complete formatted text
        print(filled_template)

        return []



    def date_to_julian_date(self,date_str):
        """
        Convert a date string in 'YYYY-MM-DD' format to Julian Date.
        """
        # Parse the date string into a datetime object
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        
        # Calculate Julian Date
        julian_date = dt.toordinal() + 1721424.5 + (dt.hour + dt.minute / 60 + dt.second / 3600) / 24
        return julian_date

