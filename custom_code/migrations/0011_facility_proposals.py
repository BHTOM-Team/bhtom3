from django.conf import settings
from django.db import migrations, models


def seed_facilities(apps, schema_editor):
    Facility = apps.get_model('custom_code', 'Facility')
    facilities = [
        {
            'code': 'LCO',
            'name': 'Las Cumbres Observatory',
            'supports_remote_proposal_sync': True,
            'account_schema': {
                'fields': [
                    {'name': 'api_key', 'label': 'API key', 'type': 'secret', 'required': True},
                ],
            },
            'proposal_schema': {
                'fields': [],
                'source': 'remote_sync',
            },
        },
        {
            'code': 'LT',
            'name': 'Liverpool Telescope',
            'supports_remote_proposal_sync': False,
            'account_schema': {
                'fields': [
                    {'name': 'username', 'label': 'Username', 'type': 'string', 'required': True},
                    {'name': 'password', 'label': 'Password', 'type': 'secret', 'required': True},
                    {'name': 'host', 'label': 'Host', 'type': 'string', 'required': False},
                    {'name': 'port', 'label': 'Port', 'type': 'string', 'required': False},
                ],
            },
            'proposal_schema': {
                'fields': [
                    {'name': 'proposal_id', 'label': 'Proposal ID', 'type': 'string', 'required': True},
                ],
                'source': 'manual',
            },
        },
        {
            'code': 'REM',
            'name': 'Rapid Eye Mount',
            'supports_remote_proposal_sync': False,
            'account_schema': {
                'fields': [
                    {'name': 'notification_email', 'label': 'Notification email', 'type': 'string', 'required': False},
                ],
            },
            'proposal_schema': {
                'fields': [
                    {'name': 'proposal_id', 'label': 'Proposal ID', 'type': 'string', 'required': True},
                    {'name': 'pi_name', 'label': 'PI name', 'type': 'string', 'required': False},
                    {'name': 'description', 'label': 'Description', 'type': 'string', 'required': False},
                ],
                'source': 'manual',
            },
        },
        {
            'code': 'SUHORA',
            'name': 'Suhora Observatory',
            'supports_remote_proposal_sync': False,
            'account_schema': {'fields': [{'name': 'notification_email', 'label': 'Notification email', 'type': 'string', 'required': False}]},
            'proposal_schema': {'fields': [{'name': 'proposal_id', 'label': 'Proposal ID', 'type': 'string', 'required': True}], 'source': 'manual'},
        },
        {
            'code': 'BOLECINA',
            'name': 'Bolecina Observatory',
            'supports_remote_proposal_sync': False,
            'account_schema': {'fields': [{'name': 'notification_email', 'label': 'Notification email', 'type': 'string', 'required': False}]},
            'proposal_schema': {'fields': [{'name': 'proposal_id', 'label': 'Proposal ID', 'type': 'string', 'required': True}], 'source': 'manual'},
        },
        {
            'code': 'LESEDI',
            'name': 'Lesedi Telescope',
            'supports_remote_proposal_sync': False,
            'account_schema': {'fields': [{'name': 'notification_email', 'label': 'Notification email', 'type': 'string', 'required': False}]},
            'proposal_schema': {'fields': [{'name': 'proposal_id', 'label': 'Proposal ID', 'type': 'string', 'required': True}], 'source': 'manual'},
        },
        {
            'code': 'GEM',
            'name': 'Gemini',
            'supports_remote_proposal_sync': False,
            'account_schema': {
                'fields': [
                    {'name': 'api_key_gs', 'label': 'GS API key', 'type': 'secret', 'required': False},
                    {'name': 'api_key_gn', 'label': 'GN API key', 'type': 'secret', 'required': False},
                    {'name': 'user_email', 'label': 'User email', 'type': 'string', 'required': False},
                ],
            },
            'proposal_schema': {
                'fields': [
                    {'name': 'program_id', 'label': 'Program ID', 'type': 'string', 'required': True},
                    {'name': 'mode', 'label': 'Mode', 'type': 'string', 'required': False},
                ],
                'source': 'manual',
            },
        },
        {
            'code': 'SWIFT',
            'name': 'Swift',
            'supports_remote_proposal_sync': False,
            'account_schema': {
                'fields': [
                    {'name': 'username', 'label': 'Username', 'type': 'string', 'required': True},
                    {'name': 'shared_secret', 'label': 'Shared secret', 'type': 'secret', 'required': True},
                ],
            },
            'proposal_schema': {'fields': [], 'source': 'none'},
        },
    ]
    for payload in facilities:
        Facility.objects.update_or_create(code=payload['code'], defaults=payload)


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('custom_code', '0010_bhtomtarget_gaia_variability_type'),
    ]

    operations = [
        migrations.CreateModel(
            name='Facility',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('code', models.CharField(db_index=True, max_length=32, unique=True)),
                ('name', models.CharField(max_length=128)),
                ('description', models.TextField(blank=True, default='')),
                ('account_schema', models.JSONField(blank=True, default=dict)),
                ('proposal_schema', models.JSONField(blank=True, default=dict)),
                ('supports_remote_proposal_sync', models.BooleanField(db_index=True, default=False)),
                ('is_active', models.BooleanField(db_index=True, default=True)),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('modified', models.DateTimeField(auto_now=True)),
            ],
            options={'ordering': ('name',)},
        ),
        migrations.CreateModel(
            name='FacilityAccount',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('label', models.CharField(max_length=128)),
                ('account_data', models.JSONField(blank=True, default=dict)),
                ('credentials', models.JSONField(blank=True, default=dict)),
                ('is_active', models.BooleanField(db_index=True, default=True)),
                ('sync_status', models.CharField(choices=[('not_synced', 'Not synced'), ('ok', 'OK'), ('error', 'Error')], db_index=True, default='not_synced', max_length=16)),
                ('last_synced_at', models.DateTimeField(blank=True, null=True)),
                ('last_sync_error', models.TextField(blank=True, default='')),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('modified', models.DateTimeField(auto_now=True)),
                ('created_by', models.ForeignKey(on_delete=models.deletion.PROTECT, related_name='facility_accounts_created', to=settings.AUTH_USER_MODEL)),
                ('facility', models.ForeignKey(on_delete=models.deletion.CASCADE, related_name='accounts', to='custom_code.facility')),
            ],
            options={'ordering': ('facility__name', 'label')},
        ),
        migrations.CreateModel(
            name='FacilityProposal',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('external_id', models.CharField(max_length=128)),
                ('title', models.CharField(blank=True, default='', max_length=255)),
                ('details', models.JSONField(blank=True, default=dict)),
                ('remote_payload', models.JSONField(blank=True, default=dict)),
                ('is_active', models.BooleanField(db_index=True, default=True)),
                ('valid_from', models.DateTimeField(blank=True, null=True)),
                ('valid_until', models.DateTimeField(blank=True, null=True)),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('modified', models.DateTimeField(auto_now=True)),
                ('account', models.ForeignKey(on_delete=models.deletion.CASCADE, related_name='proposals', to='custom_code.facilityaccount')),
            ],
            options={'ordering': ('account__facility__name', 'title', 'external_id')},
        ),
        migrations.CreateModel(
            name='FacilityProposalMembership',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('role', models.CharField(choices=[('owner', 'Owner'), ('editor', 'Editor'), ('user', 'User')], default='user', max_length=16)),
                ('can_submit_observations', models.BooleanField(default=True)),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('modified', models.DateTimeField(auto_now=True)),
                ('created_by', models.ForeignKey(blank=True, null=True, on_delete=models.deletion.PROTECT, related_name='facility_proposal_memberships_created', to=settings.AUTH_USER_MODEL)),
                ('proposal', models.ForeignKey(on_delete=models.deletion.CASCADE, related_name='memberships', to='custom_code.facilityproposal')),
                ('user', models.ForeignKey(on_delete=models.deletion.CASCADE, related_name='facility_proposal_memberships', to=settings.AUTH_USER_MODEL)),
            ],
        ),
        migrations.CreateModel(
            name='FacilityAccountMembership',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('role', models.CharField(choices=[('owner', 'Owner'), ('editor', 'Editor'), ('viewer', 'Viewer')], default='owner', max_length=16)),
                ('can_view_credentials', models.BooleanField(default=False)),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('modified', models.DateTimeField(auto_now=True)),
                ('account', models.ForeignKey(on_delete=models.deletion.CASCADE, related_name='memberships', to='custom_code.facilityaccount')),
                ('created_by', models.ForeignKey(blank=True, null=True, on_delete=models.deletion.PROTECT, related_name='facility_account_memberships_created', to=settings.AUTH_USER_MODEL)),
                ('user', models.ForeignKey(on_delete=models.deletion.CASCADE, related_name='facility_account_memberships', to=settings.AUTH_USER_MODEL)),
            ],
        ),
        migrations.AddField(
            model_name='facilityproposal',
            name='users',
            field=models.ManyToManyField(related_name='shared_facility_proposals', through='custom_code.FacilityProposalMembership', through_fields=('proposal', 'user'), to=settings.AUTH_USER_MODEL),
        ),
        migrations.AddField(
            model_name='facilityaccount',
            name='users',
            field=models.ManyToManyField(related_name='shared_facility_accounts', through='custom_code.FacilityAccountMembership', through_fields=('account', 'user'), to=settings.AUTH_USER_MODEL),
        ),
        migrations.AlterUniqueTogether(
            name='facilityproposal',
            unique_together={('account', 'external_id')},
        ),
        migrations.AlterUniqueTogether(
            name='facilityproposalmembership',
            unique_together={('proposal', 'user')},
        ),
        migrations.AlterUniqueTogether(
            name='facilityaccount',
            unique_together={('facility', 'label')},
        ),
        migrations.AlterUniqueTogether(
            name='facilityaccountmembership',
            unique_together={('account', 'user')},
        ),
        migrations.RunPython(seed_facilities, migrations.RunPython.noop),
    ]
