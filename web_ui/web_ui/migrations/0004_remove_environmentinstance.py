# Generated by Django on 2025-01-XX XX:XX

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("web_ui", "0003_add_connection_model"),
    ]

    operations = [
        migrations.DeleteModel(
            name="EnvironmentInstance",
        ),
    ] 