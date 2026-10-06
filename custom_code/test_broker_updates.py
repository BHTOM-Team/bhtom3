from datetime import datetime, timedelta, timezone
from io import StringIO
from unittest.mock import Mock, patch

from django.contrib.auth import get_user_model
from django.core.management import call_command, CommandError
from django.test import TestCase
from django.urls import reverse
from requests import HTTPError, Response
from tom_dataproducts.models import ReducedDatum
from tom_targets.models import Target


class BrokerUpdateTests(TestCase):
    def setUp(self):
        self.datum_count = 0
        self.alerce = Mock()
        self.gaia = Mock()
        broker_patch = patch(
            'custom_code.management.commands.update_broker_data.alerts.get_service_classes',
            return_value={
                'ALeRCE': Mock(return_value=self.alerce),
                'Gaia': Mock(return_value=self.gaia),
            },
        )
        broker_patch.start()
        self.addCleanup(broker_patch.stop)
        self.ztf_target = Target.objects.create(name='ZTF20aawaorg', type='SIDEREAL')
        self.other_target = Target.objects.create(name='ASASSN-25cd', type='SIDEREAL')
        self.add_datum(self.ztf_target, 'ALeRCE')
        self.add_datum(self.other_target, 'FRAM')

    def add_datum(self, target, source):
        self.datum_count += 1
        return ReducedDatum.objects.create(
            target=target, source_name=source, data_type='photometry',
            timestamp=datetime(2026, 10, 6, tzinfo=timezone.utc) + timedelta(days=self.datum_count),
            value={'magnitude': 15.0, 'filter': 'g'},
        )

    def test_target_without_broker_data_does_not_query_unrelated_brokers(self):
        output = call_command('update_broker_data', target_id=self.other_target.pk, stdout=StringIO())

        self.assertEqual(output, 'Update completed successfully')
        self.alerce.process_reduced_data.assert_not_called()
        self.gaia.process_reduced_data.assert_not_called()

    def test_single_target_refreshes_only_its_own_broker(self):
        self.add_datum(self.other_target, 'Gaia')

        call_command('update_broker_data', target_id=self.ztf_target.pk, stdout=StringIO())

        self.alerce.process_reduced_data.assert_called_once_with(self.ztf_target)
        self.gaia.process_reduced_data.assert_not_called()

    def test_bulk_refresh_calls_each_target_broker_pair_once(self):
        self.add_datum(self.ztf_target, 'ALeRCE')
        self.add_datum(self.other_target, 'Gaia')

        call_command('update_broker_data', stdout=StringIO())

        self.alerce.process_reduced_data.assert_called_once_with(self.ztf_target)
        self.gaia.process_reduced_data.assert_called_once_with(self.other_target)

    def test_failed_broker_reports_target_and_http_status_and_continues(self):
        self.add_datum(self.other_target, 'Gaia')
        response = Response()
        response.status_code = 503
        self.alerce.process_reduced_data.side_effect = HTTPError(response=response)

        output = call_command('update_broker_data', stdout=StringIO())

        self.assertIn(f'ALeRCE for target {self.ztf_target.pk} (ZTF20aawaorg): HTTP 503', output)
        self.gaia.process_reduced_data.assert_called_once_with(self.other_target)

    def test_invalid_target_is_reported(self):
        with self.assertRaisesMessage(CommandError, 'Invalid target id provided'):
            call_command('update_broker_data', target_id=999999, stdout=StringIO())

    @patch('custom_code.views.enqueue_target_dataservices_update')
    def test_refresh_view_uses_scoped_brokers_and_still_enqueues_dataservices(self, enqueue):
        user = get_user_model().objects.create_user(username='broker-refresh-user')
        self.client.force_login(user)

        response = self.client.get(
            reverse('update-reduced-data-services'), {'target_id': self.other_target.pk},
        )

        self.assertEqual(response.status_code, 302)
        self.alerce.process_reduced_data.assert_not_called()
        enqueue.assert_called_once_with(self.other_target.pk, force_all_services=False)
