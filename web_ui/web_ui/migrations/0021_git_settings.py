# Generated manually for Git integration

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("web_ui", "0013_environment_envvarsinstance_environment_and_more"),
    ]

    operations = [
        migrations.RenameModel(
            old_name="GitHubSettings",
            new_name="GitSettings",
        ),
        migrations.AddField(
            model_name="gitsettings",
            name="provider_type",
            field=models.CharField(
                choices=[
                    ("github", "GitHub"),
                    ("azure_devops", "Azure DevOps"),
                    ("gitlab", "GitLab"),
                    ("bitbucket", "Bitbucket"),
                    ("other", "Other Git Provider"),
                ],
                default="github",
                help_text="Git provider type",
                max_length=50,
            ),
        ),
        migrations.AddField(
            model_name="gitsettings",
            name="base_url",
            field=models.URLField(
                blank=True,
                help_text="Base API URL (leave empty for GitHub.com, required for Azure DevOps or self-hosted instances)",
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name="gitsettings",
            name="token",
            field=models.CharField(help_text="Personal Access Token", max_length=255),
        ),
        migrations.AlterField(
            model_name="gitsettings",
            name="username",
            field=models.CharField(
                help_text="Username, organization, or project name", max_length=100
            ),
        ),
        migrations.AlterField(
            model_name="gitsettings",
            name="repository",
            field=models.CharField(help_text="Repository name", max_length=100),
        ),
        migrations.AlterField(
            model_name="gitsettings",
            name="current_branch",
            field=models.CharField(
                default="main",
                help_text="Current branch for Git operations",
                max_length=255,
            ),
        ),
        migrations.AlterModelOptions(
            name="gitsettings",
            options={
                "verbose_name": "Git Settings",
                "verbose_name_plural": "Git Settings",
            },
        ),
    ]
