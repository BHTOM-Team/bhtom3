import logging
from urllib.parse import urljoin

import requests
from django.conf import settings
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.core.files.uploadedfile import SimpleUploadedFile
from tom_observations.facilities.lco import (
    LCOFacility as BaseLCOFacility,
    LCOSettings,
    LCOImagingObservationForm,
    LCOMuscatImagingObservationForm,
    LCOPhotometricSequenceForm,
    LCOSpectroscopyObservationForm,
    LCOSpectroscopicSequenceForm,
)
from tom_observations.facilities.ocs import make_request
from tom_observations.models import ObservationRecord
from tom_dataproducts.models import DataProduct

from custom_code.bhtom2_uploads import (
    forward_dataproduct_to_bhtom2,
    has_successful_bhtom2_upload,
    load_extra_data_dict,
    normalize_fits_upload,
    save_extra_data_dict,
)
from custom_code.facility_proposals import get_proposal_by_pk, get_proposal_choices_for_user


logger = logging.getLogger(__name__)
LCO_ARCHIVE_API_URL = 'https://archive-api.lco.global'
LCO_BHTOM2_AUTOMATED_OBSERVATORY = 'LCOGT-Teide-40cm_QHY600M'
LCO_BHTOM2_AUTOMATED_FILTER = 'GaiaSP/any'


class AccountLCOSettings(LCOSettings):
    def __init__(self, account=None):
        super().__init__(facility_name='LCO')
        self.account = account

    def get_setting(self, key):
        if self.account:
            if key == 'portal_url':
                return self.account.account_data.get('portal_url', super().get_setting(key))
            if key == 'archive_url':
                return self.account.account_data.get('archive_url', super().get_setting(key))
            if key == 'api_key':
                return self.account.credentials.get('api_key', '')
        return super().get_setting(key)


class BhtomLCOFormMixin:
    def _proposal_for_payload(self, payload):
        proposal_value = payload.get('proposal') or self.cleaned_data.get('proposal')
        return get_proposal_by_pk(proposal_value, facility_code='LCO')

    def _proposal_external_identifier(self, proposal):
        external_id = str(proposal.external_id or '').strip()
        if external_id:
            return external_id
        raise ValidationError(f'LCO proposal "{proposal}" has no remote LCO proposal id. Re-sync LCO proposals and try again.')

    def _facility_settings_for_payload(self, payload, proposal=None):
        proposal = proposal or self._proposal_for_payload(payload)
        if proposal:
            return AccountLCOSettings(account=proposal.account)
        return self.facility_settings

    def _payload_with_external_proposal(self, payload, proposal=None):
        proposal = proposal or self._proposal_for_payload(payload)
        if not proposal:
            return payload
        payload = dict(payload)
        payload['proposal'] = self._proposal_external_identifier(proposal)
        return payload

    def proposal_choices(self):
        user_id = self.initial.get('request_user_id') or self.data.get('request_user_id')
        choices = get_proposal_choices_for_user(user_id, 'LCO', include_account_label=True)
        return choices or [(0, 'No proposals found')]

    def _get_instruments(self):
        cache_key = f'{self.facility_settings.facility_name}_instruments'
        cached_instruments = cache.get(cache_key)
        if cached_instruments:
            return cached_instruments

        timeout = getattr(settings, 'LCO_INSTRUMENTS_TIMEOUT_SECONDS', 8)
        cache_seconds = getattr(settings, 'LCO_INSTRUMENTS_CACHE_SECONDS', 86400)
        try:
            response = requests.get(
                urljoin(self.facility_settings.get_setting('portal_url'), '/api/instruments/'),
                headers={'Authorization': f'Token {self.facility_settings.get_setting("api_key")}'},
                timeout=timeout,
            )
            response.raise_for_status()
            cached_instruments = {key: value for key, value in response.json().items()}
        except Exception as exc:
            logger.warning('Could not load LCO instruments within %ss: %s', timeout, exc)
            cached_instruments = self.facility_settings.default_instrument_config

        cache.set(cache_key, cached_instruments, cache_seconds)
        return cached_instruments

    def _expand_cadence_request(self, payload):
        proposal = self._proposal_for_payload(payload)
        facility_settings = self._facility_settings_for_payload(payload, proposal=proposal)
        payload = self._payload_with_external_proposal(payload, proposal=proposal)
        payload['requests'][0]['cadence'] = {
            'start': self.cleaned_data['start'],
            'end': self.cleaned_data['end'],
            'period': self.cleaned_data['period'],
            'jitter': self.cleaned_data['jitter'],
        }
        payload['requests'][0]['windows'] = []

        response = make_request(
            'POST',
            urljoin(facility_settings.get_setting('portal_url'), '/api/requestgroups/cadence/'),
            json=payload,
            headers={'Authorization': f'Token {facility_settings.get_setting("api_key")}'},
        )
        return response.json()


class BhtomLCOImagingObservationForm(BhtomLCOFormMixin, LCOImagingObservationForm):
    pass


class BhtomLCOMuscatImagingObservationForm(BhtomLCOFormMixin, LCOMuscatImagingObservationForm):
    pass


class BhtomLCOSpectroscopyObservationForm(BhtomLCOFormMixin, LCOSpectroscopyObservationForm):
    pass


class BhtomLCOPhotometricSequenceForm(BhtomLCOFormMixin, LCOPhotometricSequenceForm):
    pass


class BhtomLCOSpectroscopicSequenceForm(BhtomLCOFormMixin, LCOSpectroscopicSequenceForm):
    pass


class LCOFacility(BaseLCOFacility):
    observation_forms = {
        'IMAGING': BhtomLCOImagingObservationForm,
        'MUSCAT_IMAGING': BhtomLCOMuscatImagingObservationForm,
        'SPECTRA': BhtomLCOSpectroscopyObservationForm,
        'PHOTOMETRIC_SEQUENCE': BhtomLCOPhotometricSequenceForm,
        'SPECTROSCOPIC_SEQUENCE': BhtomLCOSpectroscopicSequenceForm,
    }

    def _missing_remote_status_payload(self):
        return {
            'state': 'CANCELED',
            'scheduled_start': None,
            'scheduled_end': None,
        }

    def _proposal_external_identifier(self, proposal):
        external_id = str(proposal.external_id or '').strip()
        if external_id:
            return external_id
        raise ValidationError(f'LCO proposal "{proposal}" has no remote LCO proposal id. Re-sync LCO proposals and try again.')

    def _proposal_account_facility(self, observation_payload):
        proposal_value = observation_payload.get('proposal') or observation_payload.get('params', {}).get('proposal')
        proposal = get_proposal_by_pk(proposal_value, facility_code='LCO')
        if proposal:
            return proposal, BaseLCOFacility(facility_settings=AccountLCOSettings(account=proposal.account))
        if proposal_value and str(proposal_value).strip().isdigit():
            raise ValidationError(f'LCO proposal {proposal_value} is not available in BHTOM. Re-sync LCO proposals and try again.')
        return None, BaseLCOFacility()

    def _record_account_facility(self, record):
        proposal = get_proposal_by_pk((record.parameters or {}).get('proposal'), facility_code='LCO')
        if proposal:
            return proposal, BaseLCOFacility(facility_settings=AccountLCOSettings(account=proposal.account))
        return None, BaseLCOFacility()

    def submit_observation(self, observation_payload):
        proposal, facility = self._proposal_account_facility(observation_payload)
        payload = dict(observation_payload)
        if proposal:
            payload['proposal'] = self._proposal_external_identifier(proposal)
        return facility.submit_observation(payload)

    def validate_observation(self, observation_payload):
        proposal, facility = self._proposal_account_facility(observation_payload)
        payload = dict(observation_payload)
        if proposal:
            payload['proposal'] = self._proposal_external_identifier(proposal)
        return facility.validate_observation(payload)

    def cancel_observation(self, observation_id):
        record = ObservationRecord.objects.filter(observation_id=observation_id, facility=self.name).order_by('-created').first()
        if record is None:
            return super().cancel_observation(observation_id)
        _, facility = self._record_account_facility(record)
        return facility.cancel_observation(observation_id)

    def update_observation_status(self, observation_id):
        records = ObservationRecord.objects.filter(observation_id=observation_id, facility=self.name)
        if not records:
            raise Exception('No records exist for that observation id')

        for record in records:
            _, facility = self._record_account_facility(record)
            try:
                status = facility.get_observation_status(observation_id)
            except requests.HTTPError as exc:
                response = getattr(exc, 'response', None)
                if getattr(response, 'status_code', None) == 404:
                    logger.warning(
                        'LCO observation %s was not found in the remote portal; marking local record as canceled.',
                        observation_id,
                    )
                    status = self._missing_remote_status_payload()
                else:
                    raise
            record.status = status['state']
            record.scheduled_start = status['scheduled_start']
            record.scheduled_end = status['scheduled_end']
            record.save()
            if record.status == 'COMPLETED':
                try:
                    result = self._sync_completed_lco_dataproducts(record, facility.facility_settings)
                    logger.info(
                        'Automatic LCO processing finished for observation %s: %s',
                        record.observation_id,
                        result,
                    )
                except Exception as exc:
                    logger.warning(
                        'Automatic LCO data sync failed for observation %s: %s',
                        record.observation_id,
                        exc,
                    )

    def process_completed_observation(self, record):
        if record.facility != self.name:
            raise ValueError(f'Observation {record.pk} is not an {self.name} observation.')
        if str(record.status or '').strip() != 'COMPLETED':
            raise ValueError(f'Observation {record.observation_id} is not completed yet.')

        _, facility = self._record_account_facility(record)
        result = self._sync_completed_lco_dataproducts(record, facility.facility_settings, force=True)
        logger.info('Manual LCO processing finished for observation %s: %s', record.observation_id, result)
        return result

    def _archive_api_url(self, path):
        root_url = str(getattr(settings, 'LCO_ARCHIVE_API_URL', LCO_ARCHIVE_API_URL) or LCO_ARCHIVE_API_URL).rstrip('/')
        return f'{root_url}/{str(path).lstrip("/")}'

    def _archive_timeout(self):
        try:
            return max(1, int(getattr(settings, 'LCO_ARCHIVE_TIMEOUT_SECONDS', 30)))
        except (TypeError, ValueError):
            return 30

    def _archive_headers(self, api_key):
        return {'Authorization': f'Token {api_key}'}

    def _iter_completed_archive_frames(self, observation_id, api_key):
        next_url = self._archive_api_url('/frames/')
        params = {
            'request_id': observation_id,
            'reduction_level': 91,
            'configuration_type': 'EXPOSE',
            'public': 'false',
            'limit': 100,
        }
        while next_url:
            response = requests.get(
                next_url,
                params=params,
                headers=self._archive_headers(api_key),
                timeout=self._archive_timeout(),
            )
            response.raise_for_status()
            payload = response.json()
            for frame in payload.get('results') or []:
                yield frame
            next_url = payload.get('next')
            params = None

    def _frame_filename(self, frame):
        basename = str(frame.get('basename') or '').strip()
        extension = str(frame.get('extension') or '').strip()
        if basename and extension:
            return f'{basename}{extension}'
        return basename or f'lco-frame-{frame.get("id")}.fits'

    def _normalized_frame_filename(self, frame):
        filename = self._frame_filename(frame)
        lower_name = filename.lower()
        if lower_name.endswith('.fits.fz') or lower_name.endswith('.fits.gz'):
            return filename[:-3]
        if lower_name.endswith('.fz') or lower_name.endswith('.gz'):
            return f'{filename[:-3]}.fits'
        if lower_name.endswith('.fit') or lower_name.endswith('.fts') or lower_name.endswith('.ftt') or lower_name.endswith('.ftsc'):
            return filename
        if lower_name.endswith('.fits'):
            return filename
        return f'{filename}.fits'

    def _create_lco_dataproduct(self, record, frame, api_key, *, force=False):
        frame_id = str(frame.get('id') or '').strip()
        if not frame_id:
            raise ValueError(f'Missing LCO archive frame id for observation {record.observation_id}.')

        existing = DataProduct.objects.filter(observation_record=record, product_id=frame_id).order_by('-created').first()
        created_new = existing is None
        if existing is not None and not force:
            return existing, False

        download_url = str(frame.get('url') or '').strip()
        if not download_url:
            raise ValueError(f'Missing download url for LCO archive frame {frame_id}.')

        download_response = requests.get(
            download_url,
            timeout=self._archive_timeout(),
        )
        download_response.raise_for_status()
        logger.info(
            'Downloaded LCO frame frame_id=%s observation_id=%s source_name=%s bytes=%s',
            frame_id,
            record.observation_id,
            self._frame_filename(frame),
            len(download_response.content),
        )

        uploaded_file = SimpleUploadedFile(
            self._frame_filename(frame),
            download_response.content,
            content_type='application/fits',
        )
        normalized_file, normalization_metadata = normalize_fits_upload(uploaded_file)
        normalized_file.name = self._normalized_frame_filename(frame)
        normalized_file.seek(0)
        logger.info(
            'Normalized LCO frame frame_id=%s observation_id=%s normalized_name=%s metadata=%s',
            frame_id,
            record.observation_id,
            normalized_file.name,
            normalization_metadata,
        )

        dataproduct = existing or DataProduct(
            target=record.target,
            observation_record=record,
            product_id=frame_id,
            data_product_type='fits_file',
        )
        dataproduct.target = record.target
        dataproduct.observation_record = record
        dataproduct.product_id = frame_id
        dataproduct.data_product_type = 'fits_file'
        dataproduct.data.save(normalized_file.name, normalized_file, save=False)
        dataproduct.save()
        logger.info(
            'Saved LCO dataproduct frame_id=%s observation_id=%s dataproduct_id=%s stored_name=%s',
            frame_id,
            record.observation_id,
            dataproduct.pk,
            dataproduct.get_file_name(),
        )

        metadata = load_extra_data_dict(dataproduct)
        metadata['lco_archive_frame'] = {
            'frame_id': frame_id,
            'request_id': str(frame.get('request_id') or record.observation_id),
            'observation_id': str(frame.get('observation_id') or ''),
            'basename': str(frame.get('basename') or ''),
            'filename': normalized_file.name,
            'reduction_level': frame.get('reduction_level'),
            'normalization': normalization_metadata,
        }
        save_extra_data_dict(dataproduct, metadata)

        return dataproduct, created_new

    def _sync_completed_lco_dataproducts(self, record, facility_settings, *, force=False):
        archive_api_key = str(facility_settings.get_setting('api_key') or '').strip()
        if not archive_api_key:
            logger.warning('Skipping LCO archive sync for observation %s because no LCO API key is configured.', record.observation_id)
            return {'frames_seen': 0, 'created': 0, 'forwarded': 0, 'already_forwarded': 0}

        bhtom2_token = str(getattr(settings, 'BHTOM2_API_TOKEN', '') or '').strip()
        if not bhtom2_token:
            logger.warning('Skipping automatic BHTOM2 forwarding for observation %s because BHTOM2_API_TOKEN is empty.', record.observation_id)
            return {'frames_seen': 0, 'created': 0, 'forwarded': 0, 'already_forwarded': 0}

        logger.info('Starting LCO archive sync for observation %s.', record.observation_id)
        result = {
            'frames_seen': 0,
            'created': 0,
            'forwarded': 0,
            'already_forwarded': 0,
            'refreshed': 0,
        }
        for frame in self._iter_completed_archive_frames(record.observation_id, archive_api_key):
            result['frames_seen'] += 1
            logger.info(
                'Processing LCO frame frame_id=%s observation_id=%s filename=%s reduction_level=%s',
                frame.get('id'),
                record.observation_id,
                self._frame_filename(frame),
                frame.get('reduction_level'),
            )
            dataproduct, created_new = self._create_lco_dataproduct(record, frame, archive_api_key, force=force)
            if created_new:
                result['created'] += 1
            elif force:
                result['refreshed'] += 1
            if not force and not created_new and has_successful_bhtom2_upload(dataproduct):
                result['already_forwarded'] += 1
                logger.info(
                    'Skipping already-forwarded LCO dataproduct frame_id=%s observation_id=%s dataproduct_id=%s',
                    frame.get('id'),
                    record.observation_id,
                    dataproduct.pk,
                )
                continue
            forward_dataproduct_to_bhtom2(
                dataproduct,
                token=bhtom2_token,
                observatory=LCO_BHTOM2_AUTOMATED_OBSERVATORY,
                calibration_filter=LCO_BHTOM2_AUTOMATED_FILTER,
                comment=f'Uploaded automatically from BHTOM3 LCO observation {record.observation_id}',
                user_id=record.user_id,
            )
            result['forwarded'] += 1
            logger.info(
                'Forwarded LCO dataproduct frame_id=%s observation_id=%s dataproduct_id=%s upload_name=%s',
                frame.get('id'),
                record.observation_id,
                dataproduct.pk,
                dataproduct.get_file_name(),
            )
        logger.info('Finished LCO archive sync for observation %s: %s', record.observation_id, result)
        return result
