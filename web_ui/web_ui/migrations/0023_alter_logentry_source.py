# Generated by Django 5.2 on 2025-05-07 15:47

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("web_ui", "0022_gitsecrets_alter_logentry_source"),
    ]

    operations = [
        migrations.AlterField(
            model_name="logentry",
            name="source",
            field=models.TextField(default="application"),
        ),
    ]
