# Generated for BHTOM3 ORCID profile support.

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django.db.models.query_utils


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('custom_code', '0002_geotarget'),
    ]

    operations = [
        migrations.CreateModel(
            name='BhtomUserProfile',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('affiliation', models.CharField(blank=True, default='', max_length=255)),
                ('about', models.TextField(blank=True, default='')),
                ('orcid_id', models.CharField(blank=True, db_index=True, max_length=19, null=True)),
                ('orcid_verified', models.BooleanField(default=False)),
                ('orcid_linked_at', models.DateTimeField(blank=True, null=True)),
                ('orcid_public_url', models.URLField(blank=True, default='')),
                (
                    'orcid_source',
                    models.CharField(
                        blank=True,
                        choices=[('oauth', 'OAuth'), ('manual', 'Manual')],
                        max_length=16,
                        null=True,
                    ),
                ),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('modified', models.DateTimeField(auto_now=True)),
                (
                    'user',
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name='bhtom_profile',
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                'verbose_name': 'BHTOM user profile',
                'verbose_name_plural': 'BHTOM user profiles',
            },
        ),
        migrations.AddConstraint(
            model_name='bhtomuserprofile',
            constraint=models.UniqueConstraint(
                condition=django.db.models.query_utils.Q(
                    ('orcid_id__isnull', False),
                    django.db.models.query_utils.Q(('orcid_id', ''), _negated=True),
                ),
                fields=('orcid_id',),
                name='unique_nonempty_bhtom_orcid_id',
            ),
        ),
    ]
