from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("custom_code", "0002_geotarget"),
    ]

    operations = [
        migrations.AddField(
            model_name="geotarget",
            name="intldes",
            field=models.CharField(blank=True, db_index=True, default="", max_length=32),
        ),
        migrations.AddField(
            model_name="geotarget",
            name="source",
            field=models.CharField(blank=True, db_index=True, default="manual", max_length=32),
        ),
        migrations.AddField(
            model_name="geotarget",
            name="object_type",
            field=models.CharField(blank=True, db_index=True, default="", max_length=32),
        ),
        migrations.AddField(
            model_name="geotarget",
            name="is_debris",
            field=models.BooleanField(db_index=True, default=False),
        ),
    ]
