from django.db import migrations, models


def copy_gaia_uncertainties_from_targetextra(apps, schema_editor):
    BhtomTarget = apps.get_model('custom_code', 'BhtomTarget')
    TargetExtra = apps.get_model('tom_targets', 'TargetExtra')

    field_map = {
        'parallax_error': 'parallax_error',
        'pm_ra_error': 'pm_ra_error',
        'pm_dec_error': 'pm_dec_error',
    }

    for extra in TargetExtra.objects.filter(key__in=field_map):
        field_name = field_map.get(extra.key)
        if not field_name:
            continue
        try:
            value = float(extra.value)
        except (TypeError, ValueError):
            continue
        BhtomTarget.objects.filter(pk=extra.target_id).update(**{field_name: value})


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ('custom_code', '0008_transitephemeris'),
    ]

    operations = [
        migrations.AddField(
            model_name='bhtomtarget',
            name='parallax_error',
            field=models.FloatField(blank=True, help_text='Parallax uncertainty, in milliarcseconds.', null=True, verbose_name='parallax error'),
        ),
        migrations.AddField(
            model_name='bhtomtarget',
            name='pm_dec_error',
            field=models.FloatField(blank=True, help_text='Proper Motion Dec uncertainty, in milliarcsec/year.', null=True, verbose_name='proper motion Dec error'),
        ),
        migrations.AddField(
            model_name='bhtomtarget',
            name='pm_ra_error',
            field=models.FloatField(blank=True, help_text='Proper Motion RA uncertainty, in milliarcsec/year.', null=True, verbose_name='proper motion RA error'),
        ),
        migrations.RunPython(copy_gaia_uncertainties_from_targetextra, noop_reverse),
    ]
