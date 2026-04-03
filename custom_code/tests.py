from unittest.mock import Mock, patch

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, override_settings
from django.urls import reverse


@override_settings(
    BHTOM2_API_BASE_URL='https://bh-tom2.example',
    BHTOM2_API_TOKEN='secret-api-token',
    BHTOM2_UPLOAD_SERVICE_URL='https://uploadsvc.example',
)
class PublicUploadViewTests(TestCase):
    def test_target_search_filters_results(self):
        with patch('custom_code.views._public_upload_target_choices', return_value=[
            {'label': 'Gaia24abc', 'value': 'Gaia24abc', 'search': 'gaia24abc'},
            {'label': 'AT2026xyz', 'value': 'AT2026xyz', 'search': 'at2026xyz'},
        ]):
            response = self.client.get(reverse('public-upload-targets'), {'q': 'gaia'})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {'results': [{'label': 'Gaia24abc', 'value': 'Gaia24abc', 'search': 'gaia24abc'}]},
        )

    def test_public_upload_posts_selected_single_fits_file(self):
        upload_response = Mock(status_code=201)

        with patch('custom_code.views._public_upload_target_choices', return_value=[
            {'label': 'Gaia24abc', 'value': 'Gaia24abc', 'search': 'gaia24abc'},
        ]), patch('custom_code.views._public_upload_observer_choices', return_value=[
            {'label': 'Jane Doe (jdoe)', 'value': 'jdoe', 'search': 'jdoe jane doe'},
        ]), patch('custom_code.views._public_upload_observatory_choices', return_value=[
            {'label': 'OGLE Warsaw (OGLE)', 'value': 'OGLE', 'search': 'ogle warsaw'},
        ]), patch('custom_code.views.requests.post', return_value=upload_response) as mocked_post:
            response = self.client.post(
                reverse('public-upload'),
                data={
                    'target': 'Gaia24abc',
                    'observer': 'jdoe',
                    'token': 'user-upload-token',
                    'observatory': 'OGLE',
                    'calibration_filter': 'GaiaSP/any',
                    'comment': 'test upload',
                    'fits_file': SimpleUploadedFile('example.fits', b'SIMPLE  = T', content_type='application/fits'),
                },
                follow=True,
            )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'FITS upload sent to BHTOM2 for target Gaia24abc.')

        self.assertEqual(mocked_post.call_count, 1)
        _, kwargs = mocked_post.call_args
        self.assertEqual(kwargs['data']['target'], 'Gaia24abc')
        self.assertEqual(kwargs['data']['observatory'], 'OGLE')
        self.assertEqual(kwargs['data']['observers'], 'jdoe')
        self.assertEqual(kwargs['data']['filter'], 'GaiaSP/any')
        self.assertEqual(kwargs['data']['comment'], 'test upload')
        self.assertEqual(kwargs['headers']['Authorization'], 'Token user-upload-token')
        self.assertIn('file_0', kwargs['files'])

    def test_public_upload_rejects_free_text_values_not_from_reference_lists(self):
        with patch('custom_code.views._public_upload_target_choices', return_value=[]), patch(
            'custom_code.views._public_upload_observer_choices',
            return_value=[],
        ), patch('custom_code.views._public_upload_observatory_choices', return_value=[]):
            response = self.client.post(
                reverse('public-upload'),
                data={
                    'target': 'Unknown target',
                    'observer': 'unknown',
                    'token': 'user-upload-token',
                    'observatory': 'unknown',
                    'calibration_filter': 'GaiaSP/any',
                    'comment': '',
                    'fits_file': SimpleUploadedFile('example.fits', b'SIMPLE  = T', content_type='application/fits'),
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Select a target from the BHTOM2 list.')
        self.assertContains(response, 'Select an observer from the BHTOM2 list.')
        self.assertContains(response, 'Select an observatory from the BHTOM2 list.')
