from datetime import datetime, timedelta
import logging
import time

from lxml import etree
from suds import Client

from django import forms
from django.conf import settings

from astropy.coordinates import SkyCoord
from astropy import units as u

from crispy_forms.layout import Layout, Div, HTML
from crispy_forms.bootstrap import PrependedAppendedText, PrependedText

from tom_observations.facility import BaseRoboticObservationForm, BaseRoboticObservationFacility
from tom_targets.models import Target

#from tom_lt import __version__

import math

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

try:
    LT_SETTINGS = settings.FACILITIES['LT']
except (AttributeError, KeyError):
    LT_SETTINGS = {
        'proposalIDs': (('proposal ID1', ''), ('proposal ID2', '')),
        'username': '',
        'password': '',
        'LT_HOST': '',
        'LT_PORT': '',
        'DEBUG': False,
    }


LT_XML_NS = 'http://www.rtml.org/v3.1a'
LT_XSI_NS = 'http://www.w3.org/2001/XMLSchema-instance'
LT_SCHEMA_LOCATION = 'http://www.rtml.org/v3.1a http://telescope.livjm.ac.uk/rtml/RTML-nightly.xsd'

valid_gratings = ["spbluarm", "spredarm"]
mag_init=99.
#exposure_times = {}

class LTObservationForm(BaseRoboticObservationForm):
    project = forms.ChoiceField(choices=LT_SETTINGS['proposalIDs'], label='Proposal')

    startdate = forms.CharField(label='Start Date',
                                widget=forms.TextInput(attrs={'type': 'date'}))
    starttime = forms.CharField(label='Time',
                                widget=forms.TextInput(attrs={'type': 'time'}),
                                initial='12:00')
    enddate = forms.CharField(label='End Date',
                              widget=forms.TextInput(attrs={'type': 'date'}))
    endtime = forms.CharField(label='Time',
                              widget=forms.TextInput(attrs={'type': 'time'}),
                              initial='12:00')

    max_airmass = forms.FloatField(min_value=1, max_value=3, initial=2,
                                   label='Constraints',
                                   widget=forms.NumberInput(attrs={'step': '0.1'}))
    max_seeing = forms.FloatField(min_value=0.5, max_value=5, initial=1.2,
                                  widget=forms.NumberInput(attrs={'step': '0.1'}),
                                  label='')
    max_skybri = forms.FloatField(min_value=0, max_value=10, initial=1,
                                  widget=forms.NumberInput(attrs={'step': '0.5'}),
                                  label='Sky Brightness Maximum')
    photometric = forms.ChoiceField(choices=[('clear', 'Yes'), ('light', 'No')], initial='light')

    def __init__(self, *args, **kwargs):
        # Set default values for 'start', 'end', and 'name' in initial_data
        initial_data = kwargs.get('initial', {})
        current_date = datetime.now().strftime('%Y-%m-%d')
        next_day = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        
        initial_data.setdefault('startdate', current_date)
        initial_data.setdefault('enddate', next_day)

        kwargs['initial'] = initial_data
        
        super().__init__(*args, **kwargs)

        target = Target.objects.get(id=self.initial.get('target_id'))

        # storing mag of the target to be used by other forms
        mag_init = target.mag_last

        kwargs['initial'] = initial_data
        super().__init__(*args, **kwargs)

        self.helper.layout = Layout(
            self.common_layout,
            self.layout(),
            self.extra_layout(),
            self.button_layout(),
            self.version_layout(),
        )

    def is_valid(self):
        super().is_valid()
        errors = LTFacility.validate_observation(self, self.observation_payload())
        if errors:
            self.add_error(None, errors)
        return not errors

    def layout(self):
        return Div(
            Div(
                Div(
                    'project',
                    css_class='form-row'
                ),
                Div(
                    'startdate', 'starttime',
                    css_class='form-row'
                ),
                Div(
                    'enddate', 'endtime',
                    css_class='form-row'
                ),
                css_class='col-md-16'
            ),
            Div(
                Div(
                    PrependedText('max_airmass', 'Airmass <'),
                    PrependedAppendedText('max_seeing', 'Seeing <', 'arcsec'),
                    PrependedAppendedText('max_skybri', 'Dark + ', 'mag/arcsec\xB2'),
                    'photometric',
                    css_class='col-md-16'
                ),
                css_class='form-row'
            ),
            HTML('<hr width="85%"><h4>Instrument Config</h4>'),
            css_class='form-row'
        )

    def version_layout(self):
        return Div(HTML('<hr>'
                        '<em><a href="http://telescope.livjm.ac.uk" target="_blank">Liverpool Telescope</a>'
                        ' Facility module v{{version}}</em>'
                        ))

    def extra_layout(self):
        return Div()

    def _build_prolog(self):
        namespaces = {
            'xsi': LT_XSI_NS,
        }
        schemaLocation = etree.QName(LT_XSI_NS, 'schemaLocation')
        uid = format(str(int(time.time())))
        return etree.Element('RTML', {schemaLocation: LT_SCHEMA_LOCATION}, xmlns=LT_XML_NS,
                             mode='request', uid=uid, version='3.1a', nsmap=namespaces)

    def _build_project(self, payload):
        project = etree.Element('Project', ProjectID=self.cleaned_data['project'])
        contact = etree.SubElement(project, 'Contact')
        etree.SubElement(contact, 'Username').text = LT_SETTINGS['username']
        etree.SubElement(contact, 'Name').text = ''
        payload.append(project)

    def _build_constraints(self):
        airmass_const = etree.Element('AirmassConstraint', maximum=str(self.cleaned_data['max_airmass']))

        sky_const = etree.Element('SkyConstraint')
        etree.SubElement(sky_const, 'Flux').text = str(self.cleaned_data['max_skybri'])
        etree.SubElement(sky_const, 'Units').text = 'magnitudes/square-arcsecond'

        seeing_const = etree.Element('SeeingConstraint',
                                     maximum=(str(self.cleaned_data['max_seeing'])),
                                     units='arcseconds')

        photom_const = etree.Element('ExtinctionConstraint')
        etree.SubElement(photom_const, 'Clouds').text = self.cleaned_data['photometric']

        date_const = etree.Element('DateTimeConstraint', type='include')
        start = self.cleaned_data['startdate'] + 'T' + self.cleaned_data['starttime'] + ':00+00:00'
        end = self.cleaned_data['enddate'] + 'T' + self.cleaned_data['endtime'] + ':00+00:00'
        etree.SubElement(date_const, 'DateTimeStart', system='UT', value=start)
        etree.SubElement(date_const, 'DateTimeEnd', system='UT', value=end)

        return [airmass_const, sky_const, seeing_const, photom_const, date_const]

    def _build_target(self):
        target_to_observe = Target.objects.get(pk=self.cleaned_data['target_id'])

        target = etree.Element('Target', name=target_to_observe.name)
        c = SkyCoord(ra=target_to_observe.ra*u.degree, dec=target_to_observe.dec*u.degree)
        coordinates = etree.SubElement(target, 'Coordinates')
        ra = etree.SubElement(coordinates, 'RightAscension')
        etree.SubElement(ra, 'Hours').text = str(int(c.ra.hms.h))
        etree.SubElement(ra, 'Minutes').text = str(int(c.ra.hms.m))
        etree.SubElement(ra, 'Seconds').text = str(c.ra.hms.s)

        dec = etree.SubElement(coordinates, 'Declination')
        sign = '+' if c.dec.signed_dms.sign == 1.0 else '-'
        etree.SubElement(dec, 'Degrees').text = sign + str(int(c.dec.signed_dms.d))
        etree.SubElement(dec, 'Arcminutes').text = str(int(c.dec.signed_dms.m))
        etree.SubElement(dec, 'Arcseconds').text = str(c.dec.signed_dms.s)
        etree.SubElement(coordinates, 'Equinox').text = str(target_to_observe.epoch)
        return target

    def observation_payload(self):
        payload = self._build_prolog()
        self._build_project(payload)
        self._build_inst_schedule(payload)
        return etree.tostring(payload, encoding="unicode")


class LT_IOO_ObservationForm(LTObservationForm):
    binning = forms.ChoiceField(
        choices=[('1x1', '1x1'), ('2x2', '2x2')],
        initial=('2x2', '2x2'),
        help_text='2x2 binning is usual, giving 0.3 arcsec/pixel, \
                   faster readout and lower readout noise. 1x1 binning should \
                   only be selected if specifically required.')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filters = ('U',
                        'R',
                        'G',
                        'I',
                        'Z',
                        'B',
                        'V',
                        'Halpha6566',
                        'Halpha6634',
                        'Halpha6705',
                        'Halpha6755',
                        'Halpha6822')

        for filter in self.filters:
            if filter == self.filters[0]:
                self.fields['exp_time_' + filter] = forms.FloatField(min_value=0,
                                                                     initial=120,
                                                                     label='Integration Time')
                self.fields['exp_count_' + filter] = forms.IntegerField(min_value=0,
                                                                        initial=0,
                                                                        label='No. of integrations')
            else:
                self.fields['exp_time_' + filter] = forms.FloatField(min_value=0,
                                                                     initial=120,
                                                                     label='')
                self.fields['exp_count_' + filter] = forms.IntegerField(min_value=0,
                                                                        initial=0,
                                                                        label='')

    def extra_layout(self):
        return Div(
            Div(
                Div(HTML('<br><h5>Sloan</h5>'), css_class='form_row'),
                Div(
                    Div(PrependedAppendedText('exp_time_U', 'u\'', 's'),
                        PrependedAppendedText('exp_time_G', 'g\'', 's'),
                        PrependedAppendedText('exp_time_R', 'r\'', 's'),
                        PrependedAppendedText('exp_time_I', 'i\'', 's'),
                        PrependedAppendedText('exp_time_Z', 'z\'', 's'),
                        css_class='col-md-6', ),

                    Div('exp_count_U',
                        'exp_count_G',
                        'exp_count_R',
                        'exp_count_I',
                        'exp_count_Z',
                        css_class='col-md-4'),
                    css_class='form-row'
                ),
                Div(HTML('<br><h5>Bessell</h5>'), css_class='form_row'),
                Div(
                    Div(PrependedAppendedText('exp_time_B', 'B', 's'),
                        PrependedAppendedText('exp_time_V', 'V', 's'),
                        css_class='col-md-6', ),

                    Div('exp_count_B',
                        'exp_count_V',
                        css_class='col-md-4'),
                    css_class='form-row'
                ),
                Div(HTML('<br><h5>H-alpha</h5>'), css_class='form_row'),

                Div(
                    Div(PrependedAppendedText('exp_time_Halpha6566', '6566', 's'),
                        PrependedAppendedText('exp_time_Halpha6634', '6634', 's'),
                        PrependedAppendedText('exp_time_Halpha6705', '6705', 's'),
                        PrependedAppendedText('exp_time_Halpha6755', '6755', 's'),
                        PrependedAppendedText('exp_time_Halpha6822', '6822', 's'),
                        css_class='col-md-6', ),

                    Div('exp_count_Halpha6566',
                        'exp_count_Halpha6634',
                        'exp_count_Halpha6705',
                        'exp_count_Halpha6755',
                        'exp_count_Halpha6822',
                        css_class='col-md-4'),
                    css_class='form-row'
                    ),
                css_class='col-md-10'
            ),
            Div(css_class='col-md-1'),
            Div('binning', css_class='col-md-6'),
            css_class='form-row'
        )

    def _build_inst_schedule(self, payload):

        for filter in self.filters:
            if self.cleaned_data['exp_count_' + filter] != 0:
                payload.append(self._build_schedule(filter))

    def _build_schedule(self, filter):
        exp_time = self.cleaned_data['exp_time_' + filter]
        exp_count = self.cleaned_data['exp_count_' + filter]

        schedule = etree.Element('Schedule')
        device = etree.SubElement(schedule, 'Device', name="IO:O", type="camera")
        etree.SubElement(device, 'SpectralRegion').text = 'optical'
        setup = etree.SubElement(device, 'Setup')
        etree.SubElement(setup, 'Filter', type=filter)
        detector = etree.SubElement(setup, 'Detector')
        binning = etree.SubElement(detector, 'Binning')
        etree.SubElement(binning, 'X', units='pixels').text = self.cleaned_data['binning'].split('x')[0]
        etree.SubElement(binning, 'Y', units='pixels').text = self.cleaned_data['binning'].split('x')[1]
        exposure = etree.SubElement(schedule, 'Exposure', count=str(exp_count))
        etree.SubElement(exposure, 'Value', units='seconds').text = str(exp_time)
        schedule.append(self._build_target())
        for const in self._build_constraints():
            schedule.append(const)
        return schedule


class LT_IOI_ObservationForm(LTObservationForm):
    exp_time = forms.FloatField(min_value=0, initial=120, label='Integration time',
                                widget=forms.NumberInput(attrs={'step': '0.1'}))
    exp_count = forms.IntegerField(min_value=1, initial=5, label='No. of integrations',
                                   help_text='The Liverpool Telescope will automatically \
                                   create a dither pattern between exposures.')

    def extra_layout(self):
        return Div(
            Div(
                Div(
                    Div(PrependedAppendedText('exp_time', 'H', 's'), css_class='col-md-6'),
                    Div('exp_count', css_class='col-md-4'),
                    css_class='form-row'
                ),
                css_class='col-md-10'
            ),
            Div(css_class='col-md-5'),

            css_class='form-row'
        )

    def _build_inst_schedule(self, payload):
        exp_time = self.cleaned_data['exp_time']
        exp_count = self.cleaned_data['exp_count']

        schedule = etree.Element('Schedule')
        device = etree.SubElement(schedule, 'Device', name="IO:I", type="camera")
        etree.SubElement(device, 'SpectralRegion').text = 'infrared'
        setup = etree.SubElement(device, 'Setup')
        etree.SubElement(setup, 'Filter', type='H')
        detector = etree.SubElement(setup, 'Detector')
        binning = etree.SubElement(detector, 'Binning')
        etree.SubElement(binning, 'X', units='pixels').text = '1'
        etree.SubElement(binning, 'Y', units='pixels').text = '1'
        exposure = etree.SubElement(schedule, 'Exposure', count=str(exp_count))
        etree.SubElement(exposure, 'Value', units='seconds').text = str(exp_time)
        schedule.append(self._build_target())
        for const in self._build_constraints():
            schedule.append(const)
        payload.append(schedule)


class LT_SPRAT_ObservationForm(LTObservationForm):
    exp_time = forms.FloatField(min_value=0, initial=120, label='Integration time',
                                widget=forms.NumberInput(attrs={'step': '0.1'}))
    exp_count = forms.IntegerField(min_value=1, initial=1, label='No. of integrations')

    grating = forms.ChoiceField(choices=[('red', 'Red'), ('blue', 'Blue')], initial='red')
    
    exposure_times = {}

    mag_init = 99.9
    def __init__(self, *args, **kwargs):
        initial_data = kwargs.get('initial', {})
        print(f"init in sprat: {initial_data}")
        kwargs['initial'] = initial_data
                
        target = Target.objects.get(id=initial_data.get('target_id'))
        self.mag_init = target.mag_last

        print(f"init mag in sprat: {self.mag_init}")

        self.exposure_times = {}

        instrument = "sprat"
        for grating_option in valid_gratings:
            self.exposure_times[grating_option] = calculate_exposure_time(None, None, None, None, None, None, instrument, "spratslit", grating_option, 10, self.mag_init, 1.2)
            #calculate_exposure_time(None, None, None, None, None, None, spmag=self.mag_init, sparm=grating_option, spinstrum=instrument, spslit="spratslit",
            #    spsnr = 50, spseeing = 1.2)
        
        # Set initial exposure time based on the first filter choice
        first_filter = valid_gratings[0][0]
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
    
    def extra_layout(self):
        # Display a table of gratings and exposure times
        grating_rows = "".join(f"<tr><td>{grating_option}</td><td>{self.exposure_times.get(grating_option):.2f}</td></tr>" for grating_option in valid_gratings)
        mag = self.mag_init

        return Div(
                    Div(
                        Div(
                            Div(PrependedAppendedText('exp_time', 'SPRAT', 's'), css_class='col-md-6'),
                            Div('exp_count', css_class='col-md-6'),
                            css_class='form-row'
                        ),
                        css_class='col-md-10'
                    ),
                    Div('grating', css_class='col-md-10'),
                    HTML(f"<h6><i>Suggested exposure times for mag={mag} and S/N=10</i></h6><small><br><table><tr><th>Grating</th><th>Exposure Time</th></tr>{grating_rows}</table></small>"),
                    css_class='form-row'
                )

    def _build_inst_schedule(self, payload):
        exp_time = self.cleaned_data['exp_time']
        exp_count = self.cleaned_data['exp_count']
        grating = self.cleaned_data['grating']

        schedule = etree.Element('Schedule')
        device = etree.SubElement(schedule, 'Device', name="Sprat", type="spectrograph")
        etree.SubElement(device, 'SpectralRegion').text = 'optical'
        setup = etree.SubElement(device, 'Setup')
        etree.SubElement(setup, 'Grating', name=grating)
        detector = etree.SubElement(setup, 'Detector')
        binning = etree.SubElement(detector, 'Binning')
        etree.SubElement(binning, 'X', units='pixels').text = '1'
        etree.SubElement(binning, 'Y', units='pixels').text = '1'
        exposure = etree.SubElement(schedule, 'Exposure', count=str(exp_count))
        etree.SubElement(exposure, 'Value', units='seconds').text = str(exp_time)
        schedule.append(self._build_target())
        for const in self._build_constraints():
            schedule.append(const)
        payload.append(schedule)


class LT_FRODO_ObservationForm(LTObservationForm):
    exp_time_blue = forms.FloatField(min_value=0, initial=120, label='Integration time',
                                     widget=forms.NumberInput(attrs={'step': '0.1'}))
    exp_count_blue = forms.IntegerField(min_value=0, initial=1, label='No. of integrations')
    res_blue = forms.ChoiceField(choices=[('high', 'High'), ('low', 'Low')], initial='low', label='Resolution')

    exp_time_red = forms.FloatField(min_value=0, initial=120, label='',
                                    widget=forms.NumberInput(attrs={'step': '0.1'}))
    exp_count_red = forms.IntegerField(min_value=0, initial=1, label='')
    res_red = forms.ChoiceField(choices=[('high', 'High'), ('low', 'Low')], initial='low', label='')

    def extra_layout(self):

        return Div(
                    Div(PrependedAppendedText('exp_time_blue', 'Blue Arm', 's'),
                        PrependedAppendedText('exp_time_red', 'Red Arm', 's'),
                        css_class='col-md-6'),
                    Div('exp_count_blue', 'exp_count_red', css_class='col-md-4'),
                    Div('res_blue', 'res_red', css_class='col-md-2'),
                    css_class='form-row'
        )

    def _build_inst_schedule(self, payload):
        payload.append(self._build_schedule('FrodoSpec-Blue',
                                            str(self.cleaned_data['res_blue']),
                                            str(self.cleaned_data['exp_count_blue']),
                                            str(self.cleaned_data['exp_time_blue'])))
        payload.append(self._build_schedule('FrodoSpec-Red',
                                            str(self.cleaned_data['res_red']),
                                            str(self.cleaned_data['exp_count_red']),
                                            str(self.cleaned_data['exp_time_red'])))

    def _build_schedule(self, device, grating, exp_count, exp_time):
        schedule = etree.Element('Schedule')
        device = etree.SubElement(schedule, 'Device', name=device, type="spectrograph")
        etree.SubElement(device, 'SpectralRegion').text = 'optical'
        setup = etree.SubElement(device, 'Setup')
        etree.SubElement(setup, 'Grating', name=grating)
        exposure = etree.SubElement(schedule, 'Exposure', count=exp_count)
        etree.SubElement(exposure, 'Value', units='seconds').text = exp_time
        schedule.append(self._build_target())
        for const in self._build_constraints():
            schedule.append(const)
        return schedule


class LTFacility(BaseRoboticObservationFacility):
    name = 'LT'
    observation_types = [('IOO', 'IO:O'), ('IOI', 'IO:I'), ('SPRAT', 'SPRAT'), ('FRODO', 'FRODOSpec')]

    # observation_forms should be a dictionary
    #  * it's .items() method is called in views.py::ObservationCreateView.get_context_data()
    #  * the keys are the observation_types; values are the ObservationForm classes

    # TODO: this (required) addition seems redudant to the get_form() method below.
    # TODO: see how get_form() is used and if it's still required
    observation_forms = {
        'SPRAT': LT_SPRAT_ObservationForm,
        'FRODO': LT_FRODO_ObservationForm,
        'IOO': LT_IOO_ObservationForm,
        'IOI': LT_IOI_ObservationForm,
    }

    SITES = {
            'La Palma': {
                'sitecode': 'orm',  # TODO: what does this mean? and document it.
                'latitude': 28.762,
                'longitude': -17.872,
                'elevation': 2363}
            }

    def get_form(self, observation_type):
        """
        """
        try:
            return self.observation_forms[observation_type]
        except KeyError:
            return self.observation_forms['IOO']
        # This is the original implementation of this method below.
        # I've rewritten it to use the observation_forms dictionary above.
        #
        # if observation_type == 'IOO':
        #     return LT_IOO_ObservationForm
        # elif observation_type == 'IOI':
        #     return LT_IOI_ObservationForm
        # elif observation_type == 'SPRAT':
        #     return LT_SPRAT_ObservationForm
        # elif observation_type == 'FRODO':
        #     return LT_FRODO_ObservationForm
        # else:
        #     return LT_IOO_ObservationForm

    def get_facility_context_data(self, **kwargs):
        """Provide Facility-specific data to context for ObservationCreateView's template

        This method is called by ObservationCreateView.get_context_data() and returns a
        dictionary of context data to be added to the View's context
        """
        facility_context_data = super().get_facility_context_data(**kwargs)
        new_context_data = {
            'version': 0.5#__version__,  # from tom_tl/__init__.py
        }

        facility_context_data.update(new_context_data)
        return facility_context_data

    def submit_observation(self, observation_payload):
        if (LT_SETTINGS['DEBUG']):
            payload = etree.fromstring(observation_payload)
            f = open("created.rtml", "w")
            f.write(etree.tostring(payload, encoding="unicode", pretty_print=True))
            f.close()
            return [0]
        else:
            headers = {
                'Username': LT_SETTINGS['username'],
                'Password': LT_SETTINGS['password']
            }
            url = '{0}://{1}:{2}/node_agent2/node_agent?wsdl'.format('http', LT_SETTINGS['LT_HOST'],
                                                                     LT_SETTINGS['LT_PORT'])
            client = Client(url=url, headers=headers)
            # Send payload, and receive response string, removing the encoding tag which causes issue with lxml parsing
            response = client.service.handle_rtml(observation_payload).replace('encoding="ISO-8859-1"', '')
            response_rtml = etree.fromstring(response)
            mode = response_rtml.get('mode')
            if mode == 'reject':
                self.dump_request_response(observation_payload, response_rtml)
            obs_id = response_rtml.get('uid')
            return [obs_id]

    def cancel_observation(self, observation_id):
        form = self.get_form()()
        payload = form._build_prolog()
        payload.append(form._build_project())

    def validate_observation(self, observation_payload):
        if (LT_SETTINGS['DEBUG']):
            return []
        else:
            headers = {
                'Username': LT_SETTINGS['username'],
                'Password': LT_SETTINGS['password']
            }
            url = '{0}://{1}:{2}/node_agent2/node_agent?wsdl'.format('http',
                                                                     LT_SETTINGS['LT_HOST'],
                                                                     LT_SETTINGS['LT_PORT'])
            client = Client(url=url, headers=headers)
            validate_payload = etree.fromstring(observation_payload)
            # Change the payload to an inquiry mode document to test connectivity.
            validate_payload.set('mode', 'inquiry')
            # Send payload, and receive response string, removing the encoding tag which causes issue with lxml parsing
            try:
                response = client.service.handle_rtml(validate_payload).replace('encoding="ISO-8859-1"', '')
            except Exception as e:
                return [f'Error with connection to Liverpool Telescope: {e}',
                        'This could be due to incorrect credentials, or IP / Port settings',
                        'Occassionally, this could be due to the rebooting of systems at the Telescope Site',
                        'Please retry at another time.',
                        'If the problem persists please contact ltsupport_astronomer@ljmu.ac.uk']

            response_rtml = etree.fromstring(response)
            if response_rtml.get('mode') == 'offer':
                return []
            elif response_rtml.get('mode') == 'reject':
                return ['Error with RTML submission to Liverpool Telescope',
                        'This can occassionally happen due to systems rebooting at the Telescope Site',
                        'Please retry at another time.',
                        'If the problem persists please contact ltsupport_astronomer@ljmu.ac.uk']

    def get_observation_url(self, observation_id):
        return ''

    def get_terminal_observing_states(self):
        return ['IN_PROGRESS', 'COMPLETED']

    def get_observing_sites(self):
        return self.SITES

    def get_observation_status(self, observation_id):
        return

    def data_products(self, observation_id, product_id=None):
        return []
    


def calculate_exposure_time(instrum, binn, filt, snr, mag, seeing, spinstrum=None, spslit=None, sparm=None, spsnr=None, spmag=None, spseeing=None):
    # Define instrument characteristics for imaging
    instrum_data = {
        "ioo": {"pixscale": 0.15, "darkcurrent": 0, "readnoise": 10},
        "ioi": {"pixscale": 0.18, "darkcurrent": 0, "readnoise": 17},
        "rise": {"pixscale": 0.54, "darkcurrent": 0, "readnoise": 10},
        "ringo": {"pixscale": 0.48, "darkcurrent": 0, "readnoise": 17}
    }

    # Define filter characteristics
    filter_data = {
        "fsu": {"zp": 22.17, "skybr": 21.0, "skyoff": 1.5},
        "fbb": {"zp": 24.90, "skybr": 22.3, "skyoff": 1.5},
        "fbv": {"zp": 24.96, "skybr": 21.4, "skyoff": 1.5},
        "fsg": {"zp": 25.14, "skybr": 21.7, "skyoff": 1.0},
        "fsr": {"zp": 25.39, "skybr": 20.4, "skyoff": 1.0},
        "fsi": {"zp": 25.06, "skybr": 19.3, "skyoff": 1.0},
        "fsz": {"zp": 24.52, "skybr": 18.3, "skyoff": 0.5},
        "fjj": {"zp": 24.50, "skybr": 16.6, "skyoff": 0.0},
        "fhh": {"zp": 24.00, "skybr": 12.5, "skyoff": 0.0},
        "frise": {"zp": 25.20, "skybr": 20.4, "skyoff": 1.0},
        "frise720": {"zp": 23.40, "skybr": 19.3, "skyoff": 1.0},
        "fringr": {"zp": 21, "skybr": 19.3, "skyoff": 1.0},
        "fringg": {"zp": 21.8, "skybr": 20.4, "skyoff": 1.0},
        "fringb": {"zp": 23, "skybr": 22.3, "skyoff": 1.5}
    }

    # Define binning values
    bin_values = {"two": 2, "one": 1}

    # Imaging calculation
    if instrum in instrum_data and binn in bin_values and filt in filter_data:
        pixscale = instrum_data[instrum]["pixscale"]
        darkcurrent = instrum_data[instrum]["darkcurrent"]
        readnoise = instrum_data[instrum]["readnoise"]
        bin_factor = bin_values[binn]
        zp = filter_data[filt]["zp"]
        skybr = filter_data[filt]["skybr"]
        skyoff = filter_data[filt]["skyoff"]

        # Calculate exposure time for photometry
        exposure_time = 0.1
#        seeing = 0.5 + (i / 2)
#        skymag = skybr - (skyoff * j)
        j=1.0
        skymag = skybr - skyoff*j
        ###
        areaofdisk = (seeing * 2) ** 2
        numberofpixels = areaofdisk / (pixscale * pixscale * bin_factor * bin_factor)
        starphotons = 10 ** ((zp - mag) / 2.5)
        skyphotons = 10 ** ((zp - skymag) / 2.5) * areaofdisk
        a = starphotons ** 2
        b = -snr ** 2 * (starphotons + skyphotons + darkcurrent)
        c = -snr ** 2 * numberofpixels * readnoise ** 2
        texpaa = (-b + math.sqrt(b * b - 4 * a * c)) / (2 * a)
        texpbb = (-b - math.sqrt(b * b - 4 * a * c)) / (2 * a)
        exposure_time = max(texpaa, texpbb)

        # Check for exposure time limits
        if exposure_time < 1.0:
            exposure_time = 1
        if exposure_time > 10800 or (starphotons * exposure_time / numberofpixels) > 10000:
            exposure_time = -1

        return exposure_time

    # Spectroscopy calculation
    elif spinstrum and spslit and sparm and spmag and spsnr:
        spectrometer_data = {
            "frodo": {"sppixscale": 0.82, "spdarkcurrent": 0, "spreadnoise": 10},
            "sprat": {"sppixscale": 0.48, "spdarkcurrent": 0, "spreadnoise": 9}
        }

        slit_data = {
            "spratslit": 2,
            "ifu": 10
        }

        arm_data = {
            "frredarmv": {"spzp": 16.0, "spskybr": 20.8, "spskyoff": 1.0, "spres": 5300, "wvpixscale": 0.8, "refwav": 7000},
            "frbluarmv": {"spzp": 14.5, "spskybr": 22.8, "spskyoff": 1.5, "spres": 5500, "wvpixscale": 0.35, "refwav": 4500},
            "frredarm": {"spzp": 15.60, "spskybr": 20.8, "spskyoff": 1.0, "spres": 2200, "wvpixscale": 1.9, "refwav": 7000},
            "frbluarm": {"spzp": 14.70, "spskybr": 22.8, "spskyoff": 1.5, "spres": 2600, "wvpixscale": 0.60, "refwav": 4500},
            "spredarm": {"spzp": 17.7, "spskybr": 20.8, "spskyoff": 1.0, "spres": 350, "wvpixscale": 9.0, "refwav": 7000},
            "spbluarm": {"spzp": 17.2, "spskybr": 22.8, "spskyoff": 1.5, "spres": 350, "wvpixscale": 3.0, "refwav": 4500}
        }

        sppixscale = spectrometer_data[spinstrum]["sppixscale"]
        spdarkcurrent = spectrometer_data[spinstrum]["spdarkcurrent"]
        spreadnoise = spectrometer_data[spinstrum]["spreadnoise"]
        slitwd = slit_data[spslit]
        spzp = arm_data[sparm]["spzp"]
        spskybr = arm_data[sparm]["spskybr"]
        spskyoff = arm_data[sparm]["spskyoff"]
        spres = arm_data[sparm]["spres"]
        wvpixscale = arm_data[sparm]["wvpixscale"]
        refwav = arm_data[sparm]["refwav"]

        # Calculate exposure times for spectroscopy
        exposure_time = 0.1
        sj = 1.0
#        spseeing = 0.5 + (si / 2)
        spskymag = spskybr - (spskyoff * sj)
        spatialarcs = (slitwd * spseeing * 2)
        spatialarea = spatialarcs / (sppixscale * sppixscale)
        specpixsc = refwav / (spres * wvpixscale)
        spnumberofpixels = spatialarea * specpixsc
        spstarphotons = 10 ** ((spzp - spmag) / 2.5) * (refwav / spres)
        spskyphotons = 10 ** ((spzp - spskymag) / 2.5) * (refwav / spres) * spatialarcs
        spa = spstarphotons ** 2
        spb = -spsnr ** 2 * (spstarphotons + spskyphotons + spdarkcurrent)
        spc = -spsnr ** 2 * spnumberofpixels * spreadnoise ** 2
        sptexpaa = (-spb + math.sqrt(spb * spb - 4 * spa * spc)) / (2 * spa)
        sptexpbb = (-spb - math.sqrt(spb * spb - 4 * spa * spc)) / (2 * spa)
        exposure_time = max(sptexpaa, sptexpbb)

        # Check for exposure time limits
        if exposure_time < 1.0:
            exposure_time = 1
        if exposure_time > 10800 or (spstarphotons * exposure_time / spnumberofpixels) > 10000:
            print(f"SPEC limit hit: {(spstarphotons * exposure_time / spnumberofpixels)}")
            print(f"{exposure_time}")
            exposure_time = -1

        return exposure_time

# # Example usage:
# # For imaging
# imaging_times = calculate_exposure_time("ioo", "two", "fsu", 50, 15, 1.0)

# # For spectroscopy
# spectroscopy_times = calculate_exposure_time(None, None, None, None, None, None, "sprat", "spratslit", "spbluarm", 50, 14.5, 1.0)

# print(f"Imaging Exposure Times: {imaging_times:.2f} s")
# print(f"Spectroscopy Exposure Times: {spectroscopy_times:.2f} s")
