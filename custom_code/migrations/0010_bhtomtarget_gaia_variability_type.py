from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('custom_code', '0009_bhtomtarget_gaia_uncertainties'),
    ]

    operations = [
        migrations.AddField(
            model_name='bhtomtarget',
            name='gaia_variability_type',
            field=models.CharField(
                blank=True,
                help_text='Gaia DR3 variability class from vari_classifier_result.best_class_name.',
                max_length=64,
                null=True,
                verbose_name='Gaia variability type',
            ),
        ),
    ]
