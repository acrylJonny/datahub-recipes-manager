# Generated by Django 5.2 on 2025-05-12 19:41

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("web_ui", "0027_recipetemplate_executor_id"),
    ]

    operations = [
        migrations.AddField(
            model_name="recipetemplate",
            name="cron_schedule",
            field=models.CharField(default="0 0 * * *", max_length=100),
        ),
        migrations.AddField(
            model_name="recipetemplate",
            name="timezone",
            field=models.CharField(default="Etc/UTC", max_length=50),
        ),
    ]
