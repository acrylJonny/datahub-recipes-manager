# Generated by Django 5.2.1 on 2025-06-20 15:53

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("metadata_manager", "0013_remove_glossarynode_relationships_data"),
    ]

    operations = [
        migrations.AddField(
            model_name="domain",
            name="ownership_data",
            field=models.JSONField(blank=True, null=True),
        ),
    ]
