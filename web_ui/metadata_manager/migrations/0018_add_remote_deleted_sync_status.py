# Generated by Django 5.2.1 on 2025-06-23 10:14

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("metadata_manager", "0017_add_dataset_info_to_data_contracts"),
    ]

    operations = [
        migrations.AlterField(
            model_name="assertion",
            name="sync_status",
            field=models.CharField(
                choices=[
                    ("NOT_SYNCED", "Not Synced"),
                    ("SYNCED", "Synced"),
                    ("LOCAL_ONLY", "Local Only"),
                    ("REMOTE_ONLY", "Remote Only"),
                    ("MODIFIED", "Modified"),
                    ("PENDING_PUSH", "Pending Push"),
                    ("REMOTE_DELETED", "Remote Deleted"),
                ],
                default="NOT_SYNCED",
                max_length=20,
            ),
        ),
        migrations.AlterField(
            model_name="datacontract",
            name="sync_status",
            field=models.CharField(
                choices=[
                    ("NOT_SYNCED", "Not Synced"),
                    ("SYNCED", "Synced"),
                    ("LOCAL_ONLY", "Local Only"),
                    ("REMOTE_ONLY", "Remote Only"),
                    ("MODIFIED", "Modified"),
                    ("PENDING_PUSH", "Pending Push"),
                    ("REMOTE_DELETED", "Remote Deleted"),
                ],
                default="NOT_SYNCED",
                max_length=20,
            ),
        ),
        migrations.AlterField(
            model_name="dataproduct",
            name="sync_status",
            field=models.CharField(
                choices=[
                    ("NOT_SYNCED", "Not Synced"),
                    ("SYNCED", "Synced"),
                    ("LOCAL_ONLY", "Local Only"),
                    ("REMOTE_ONLY", "Remote Only"),
                    ("MODIFIED", "Modified"),
                    ("PENDING_PUSH", "Pending Push"),
                    ("REMOTE_DELETED", "Remote Deleted"),
                ],
                default="NOT_SYNCED",
                max_length=20,
            ),
        ),
        migrations.AlterField(
            model_name="domain",
            name="sync_status",
            field=models.CharField(
                choices=[
                    ("NOT_SYNCED", "Not Synced"),
                    ("SYNCED", "Synced"),
                    ("LOCAL_ONLY", "Local Only"),
                    ("REMOTE_ONLY", "Remote Only"),
                    ("MODIFIED", "Modified"),
                    ("PENDING_PUSH", "Pending Push"),
                    ("REMOTE_DELETED", "Remote Deleted"),
                ],
                default="NOT_SYNCED",
                max_length=20,
            ),
        ),
        migrations.AlterField(
            model_name="glossarynode",
            name="sync_status",
            field=models.CharField(
                choices=[
                    ("NOT_SYNCED", "Not Synced"),
                    ("SYNCED", "Synced"),
                    ("LOCAL_ONLY", "Local Only"),
                    ("REMOTE_ONLY", "Remote Only"),
                    ("MODIFIED", "Modified"),
                    ("PENDING_PUSH", "Pending Push"),
                    ("REMOTE_DELETED", "Remote Deleted"),
                ],
                default="NOT_SYNCED",
                max_length=20,
            ),
        ),
        migrations.AlterField(
            model_name="glossaryterm",
            name="sync_status",
            field=models.CharField(
                choices=[
                    ("NOT_SYNCED", "Not Synced"),
                    ("SYNCED", "Synced"),
                    ("LOCAL_ONLY", "Local Only"),
                    ("REMOTE_ONLY", "Remote Only"),
                    ("MODIFIED", "Modified"),
                    ("PENDING_PUSH", "Pending Push"),
                    ("REMOTE_DELETED", "Remote Deleted"),
                ],
                default="NOT_SYNCED",
                max_length=20,
            ),
        ),
        migrations.AlterField(
            model_name="structuredproperty",
            name="sync_status",
            field=models.CharField(
                choices=[
                    ("NOT_SYNCED", "Not Synced"),
                    ("SYNCED", "Synced"),
                    ("LOCAL_ONLY", "Local Only"),
                    ("REMOTE_ONLY", "Remote Only"),
                    ("MODIFIED", "Modified"),
                    ("PENDING_PUSH", "Pending Push"),
                    ("REMOTE_DELETED", "Remote Deleted"),
                ],
                default="NOT_SYNCED",
                max_length=20,
            ),
        ),
        migrations.AlterField(
            model_name="tag",
            name="sync_status",
            field=models.CharField(
                choices=[
                    ("NOT_SYNCED", "Not Synced"),
                    ("SYNCED", "Synced"),
                    ("LOCAL_ONLY", "Local Only"),
                    ("REMOTE_ONLY", "Remote Only"),
                    ("MODIFIED", "Modified"),
                    ("PENDING_PUSH", "Pending Push"),
                    ("REMOTE_DELETED", "Remote Deleted"),
                ],
                default="NOT_SYNCED",
                max_length=20,
            ),
        ),
        migrations.AlterField(
            model_name="test",
            name="sync_status",
            field=models.CharField(
                choices=[
                    ("NOT_SYNCED", "Not Synced"),
                    ("SYNCED", "Synced"),
                    ("LOCAL_ONLY", "Local Only"),
                    ("REMOTE_ONLY", "Remote Only"),
                    ("MODIFIED", "Modified"),
                    ("PENDING_PUSH", "Pending Push"),
                    ("REMOTE_DELETED", "Remote Deleted"),
                ],
                default="NOT_SYNCED",
                max_length=20,
            ),
        ),
    ]
