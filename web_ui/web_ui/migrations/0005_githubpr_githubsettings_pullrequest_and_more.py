# Generated by Django 5.2 on 2025-04-25 15:51

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("web_ui", "0004_recipesecret"),
    ]

    operations = [
        migrations.CreateModel(
            name="GitHubPR",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("recipe_id", models.CharField(max_length=255)),
                ("pr_url", models.URLField()),
                ("pr_number", models.IntegerField()),
                (
                    "pr_status",
                    models.CharField(
                        choices=[
                            ("open", "Open"),
                            ("merged", "Merged"),
                            ("closed", "Closed"),
                            ("pending", "Pending"),
                        ],
                        default="open",
                        max_length=50,
                    ),
                ),
                ("branch_name", models.CharField(max_length=255)),
                ("title", models.CharField(max_length=255)),
                ("description", models.TextField(blank=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "GitHub Pull Request",
                "verbose_name_plural": "GitHub Pull Requests",
                "ordering": ["-created_at"],
            },
        ),
        migrations.CreateModel(
            name="GitHubSettings",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "token",
                    models.CharField(
                        help_text="GitHub Personal Access Token", max_length=255
                    ),
                ),
                (
                    "username",
                    models.CharField(
                        help_text="GitHub username or organization", max_length=100
                    ),
                ),
                (
                    "repository",
                    models.CharField(
                        help_text="GitHub repository name", max_length=100
                    ),
                ),
                ("enabled", models.BooleanField(default=False)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "GitHub Settings",
                "verbose_name_plural": "GitHub Settings",
            },
        ),
        migrations.CreateModel(
            name="PullRequest",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("recipe_id", models.CharField(max_length=255)),
                ("pr_number", models.IntegerField()),
                ("pr_url", models.URLField()),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("open", "Open"),
                            ("closed", "Closed"),
                            ("merged", "Merged"),
                            ("draft", "Draft"),
                        ],
                        default="open",
                        max_length=10,
                    ),
                ),
                ("title", models.CharField(max_length=255)),
                ("description", models.TextField(blank=True, null=True)),
                ("branch_name", models.CharField(max_length=255)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
        ),
        migrations.AlterModelOptions(
            name="recipesecret",
            options={
                "verbose_name": "Recipe Secret",
                "verbose_name_plural": "Recipe Secrets",
            },
        ),
        migrations.RemoveField(
            model_name="recipesecret",
            name="created_at",
        ),
        migrations.RemoveField(
            model_name="recipesecret",
            name="value",
        ),
        migrations.AddField(
            model_name="recipesecret",
            name="encrypted_value",
            field=models.TextField(blank=True, null=True),
        ),
    ]
