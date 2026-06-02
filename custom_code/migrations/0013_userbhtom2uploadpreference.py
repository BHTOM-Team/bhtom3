from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('custom_code', '0012_move_notification_email_to_proposal'),
    ]

    operations = [
        migrations.CreateModel(
            name='UserBhtom2UploadPreference',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('token', models.CharField(blank=True, default='', max_length=255)),
                ('oname', models.CharField(blank=True, default='', max_length=255)),
                ('calibration_filter', models.CharField(blank=True, default='GaiaSP/any', max_length=64)),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('modified', models.DateTimeField(auto_now=True)),
                ('user', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='bhtom2_upload_preference', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'verbose_name': 'BHTOM2 upload preference',
                'verbose_name_plural': 'BHTOM2 upload preferences',
            },
        ),
    ]
