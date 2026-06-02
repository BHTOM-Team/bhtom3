from django.db import migrations


FACILITY_CODES = ('REM', 'SUHORA', 'BOLECINA', 'LESEDI')


def move_notification_email_to_proposal(apps, schema_editor):
    Facility = apps.get_model('custom_code', 'Facility')
    FacilityAccount = apps.get_model('custom_code', 'FacilityAccount')
    FacilityProposal = apps.get_model('custom_code', 'FacilityProposal')

    for facility in Facility.objects.filter(code__in=FACILITY_CODES):
        facility.account_schema = {'fields': []}
        proposal_fields = [{'name': 'proposal_id', 'label': 'Proposal ID', 'type': 'string', 'required': True}]
        if facility.code == 'REM':
            proposal_fields.extend([
                {'name': 'notification_email', 'label': 'Notification email', 'type': 'string', 'required': True},
                {'name': 'pi_name', 'label': 'PI name', 'type': 'string', 'required': False},
                {'name': 'description', 'label': 'Description', 'type': 'string', 'required': False},
            ])
        else:
            proposal_fields.append(
                {'name': 'notification_email', 'label': 'Notification email', 'type': 'string', 'required': True},
            )
        facility.proposal_schema = {'fields': proposal_fields, 'source': 'manual'}
        facility.save(update_fields=['account_schema', 'proposal_schema'])

    for account in FacilityAccount.objects.filter(facility__code__in=FACILITY_CODES):
        notification_email = (account.account_data or {}).get('notification_email')
        if notification_email:
            for proposal in FacilityProposal.objects.filter(account=account):
                details = proposal.details or {}
                if not details.get('notification_email'):
                    details['notification_email'] = notification_email
                    proposal.details = details
                    proposal.save(update_fields=['details'])
        account_data = dict(account.account_data or {})
        if 'notification_email' in account_data:
            account_data.pop('notification_email', None)
            account.account_data = account_data
            account.save(update_fields=['account_data'])


class Migration(migrations.Migration):

    dependencies = [
        ('custom_code', '0011_facility_proposals'),
    ]

    operations = [
        migrations.RunPython(move_notification_email_to_proposal, migrations.RunPython.noop),
    ]
