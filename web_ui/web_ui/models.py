from django.db import models
import json
from django.utils import timezone
import re
import base64
import requests
import logging
from pathlib import Path
import yaml
import os
from datetime import datetime

logger = logging.getLogger(__name__)


def replace_env_vars_with_values(content, env_vars):
    """Replace environment variables in content with their values."""
    if not content or not env_vars:
        return content

    # Convert content to string if it's a dict
    if isinstance(content, dict):
        content = json.dumps(content)

    # Replace each environment variable
    for key, var_info in env_vars.items():
        if "value" in var_info:
            # Escape special characters in the value
            value = str(var_info["value"]).replace("\\", "\\\\").replace("$", "\\$")
            # Replace ${VAR} or $VAR with the value
            content = re.sub(r"\$\{?" + re.escape(key) + r"\}?", value, content)

    # Try to parse back to dict if it was originally a dict
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return content


# Define recipe types
RECIPE_TYPES = [
    ("bigquery", "Google BigQuery"),
    ("file", "File"),
    ("s3", "Amazon S3"),
    ("snowflake", "Snowflake"),
    ("postgres", "PostgreSQL"),
    ("mysql", "MySQL"),
    ("mssql", "Microsoft SQL Server"),
    ("oracle", "Oracle"),
    ("dbt", "dbt"),
    ("other", "Other"),
]


# Custom YAML dumper class to ensure proper indentation of lists
class CustomYamlDumper(yaml.SafeDumper):
    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)

    def represent_sequence(self, tag, sequence, flow_style=None):
        # Force flow style to be False for all sequences (lists)
        return super().represent_sequence(tag, sequence, flow_style=False)


class Settings(models.Model):
    """Settings model for storing application configuration."""

    key = models.CharField(max_length=255, unique=True)
    value = models.TextField(blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name_plural = "Settings"

    def __str__(self):
        return self.key


class Environment(models.Model):
    """Model for storing environments (dev, test, prod, etc.)."""

    name = models.CharField(max_length=50, unique=True)
    description = models.TextField(blank=True, null=True)
    is_default = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name_plural = "Environments"
        ordering = ["name"]

    def __str__(self):
        return self.name

    def save(self, *args, **kwargs):
        """Override save to ensure only one default environment."""
        if self.is_default:
            # Set all other environments to not be default
            type(self).objects.filter(is_default=True).exclude(pk=self.pk).update(
                is_default=False
            )
        super().save(*args, **kwargs)

    @classmethod
    def get_default(cls):
        """Get the default environment, or create one if none exists."""
        default = cls.objects.filter(is_default=True).first()
        if not default:
            # Check if 'prod' exists
            prod = cls.objects.filter(name__iexact="prod").first()
            if prod:
                prod.is_default = True
                prod.save()
                return prod

            # Check if any environment exists
            first = cls.objects.first()
            if first:
                first.is_default = True
                first.save()
                return first

            # Create a new default 'prod' environment
            default = cls.objects.create(
                name="prod", description="Production Environment", is_default=True
            )
        return default


class LogEntry(models.Model):
    """Model for storing application logs."""

    LEVEL_CHOICES = (
        ("DEBUG", "Debug"),
        ("INFO", "Info"),
        ("WARNING", "Warning"),
        ("ERROR", "Error"),
        ("CRITICAL", "Critical"),
    )

    timestamp = models.DateTimeField(default=timezone.now)
    level = models.CharField(max_length=10, choices=LEVEL_CHOICES, default="INFO")
    source = models.TextField(default="application")
    message = models.TextField()
    details = models.TextField(blank=True, null=True)

    class Meta:
        verbose_name_plural = "Log Entries"
        ordering = ["-timestamp"]

    def __str__(self):
        return f"[{self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}] {self.level}: {self.message}"

    @classmethod
    def log(cls, level, message, source="application", details=None):
        """Create a new log entry."""
        return cls.objects.create(
            level=level, message=message, source=source, details=details
        )

    @classmethod
    def debug(cls, message, source="application", details=None):
        return cls.log("DEBUG", message, source, details)

    @classmethod
    def info(cls, message, source="application", details=None):
        return cls.log("INFO", message, source, details)

    @classmethod
    def warning(cls, message, source="application", details=None):
        return cls.log("WARNING", message, source, details)

    @classmethod
    def error(cls, message, source="application", details=None):
        return cls.log("ERROR", message, source, details)

    @classmethod
    def critical(cls, message, source="application", details=None):
        return cls.log("CRITICAL", message, source, details)


class AppSettings:
    """Singleton class for managing application settings."""

    @classmethod
    def get(cls, key, default=None):
        """Get a setting value by key."""
        try:
            setting = Settings.objects.get(key=key)
            return setting.value
        except Settings.DoesNotExist:
            return default

    @classmethod
    def set(cls, key, value):
        """Set a setting value by key."""
        setting, created = Settings.objects.update_or_create(
            key=key, defaults={"value": value}
        )
        return setting

    @classmethod
    def get_all(cls):
        """Get all settings as a dictionary."""
        settings = {}
        for setting in Settings.objects.all():
            settings[setting.key] = setting.value
        return settings

    @classmethod
    def get_bool(cls, key, default=False):
        """Get a boolean setting value."""
        value = cls.get(key, default)
        if isinstance(value, str):
            return value.lower() in ("true", "t", "yes", "y", "1")
        return bool(value)

    @classmethod
    def get_int(cls, key, default=0):
        """Get an integer setting value."""
        value = cls.get(key, default)
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    @classmethod
    def get_json(cls, key, default=None):
        """Get a JSON setting value."""
        if default is None:
            default = {}
        value = cls.get(key, None)
        if not value:
            return default
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return default

    @classmethod
    def set_json(cls, key, value):
        """Set a JSON setting value."""
        return cls.set(key, json.dumps(value))


class RecipeTemplate(models.Model):
    """Model for storing reusable recipe templates."""

    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    recipe_type = models.CharField(max_length=50)
    content = models.TextField()  # JSON or YAML content
    is_favorite = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    tags = models.CharField(
        max_length=255, blank=True, null=True
    )  # Comma-separated tags
    deployed = models.BooleanField(default=False)
    deployed_at = models.DateTimeField(null=True, blank=True)
    datahub_urn = models.CharField(
        max_length=255, null=True, blank=True
    )  # Store the DataHub URN when deployed
    executor_id = models.CharField(
        max_length=255, default="default"
    )  # Store the executor ID for DataHub
    cron_schedule = models.CharField(
        max_length=100, default="0 0 * * *"
    )  # Default to daily at midnight
    timezone = models.CharField(
        max_length=50, default="Etc/UTC"
    )  # Default to UTC timezone

    def __str__(self):
        return self.name

    def get_content(self):
        """Get the recipe content as a Python object."""
        try:
            if self.content.strip().startswith("{"):
                content = json.loads(self.content)
            else:
                import yaml

                content = yaml.safe_load(self.content)

            # Add schedule information if not already present
            if "schedule" not in content:
                content["schedule"] = {
                    "cron": self.cron_schedule,
                    "timezone": self.timezone,
                }

            return content
        except Exception:
            return None

    def get_tags_list(self):
        """Get the tags as a list."""
        if not self.tags:
            return []
        return [tag.strip() for tag in self.tags.split(",")]

    def set_tags_list(self, tags_list):
        """Set the tags from a list."""
        if not tags_list:
            self.tags = ""
        else:
            self.tags = ",".join(tags_list)

    def get_recipe_id(self):
        """Extract the ID portion from the DataHub URN."""
        if not self.datahub_urn:
            return None

        # URN format: urn:li:dataHubIngestionSource:<id>
        parts = self.datahub_urn.split(":")
        if len(parts) >= 4:
            return parts[3]
        return None

    def export_to_yaml(self, base_dir=None):
        """Export the recipe template to a YAML file."""
        if not base_dir:
            base_dir = Path(__file__).parent.parent.parent / "recipes" / "templates"

        # Create directory if it doesn't exist
        base_dir = Path(base_dir)
        base_dir.mkdir(parents=True, exist_ok=True)

        # Create YAML content
        yaml_content = {
            "name": self.name,
            "description": self.description,
            "recipe_type": self.recipe_type,
            "content": self.content,
            "schedule": {"cron": self.cron_schedule, "timezone": self.timezone},
        }

        # Add tags if present
        if self.tags:
            yaml_content["tags"] = self.tags

        # Write to file
        file_path = base_dir / f"{self.recipe_type.lower()}.yml"
        with open(file_path, "w") as f:
            yaml.dump(yaml_content, f, default_flow_style=False, sort_keys=False)

        return file_path


class RecipeManager:
    """Helper class for recipe-specific operations."""

    @classmethod
    def get_default_schedule(cls):
        """Get the default schedule cron expression."""
        return AppSettings.get("default_schedule", "0 0 * * *")

    @classmethod
    def set_default_schedule(cls, schedule):
        """Set the default schedule cron expression."""
        return AppSettings.set("default_schedule", schedule)

    @classmethod
    def get_templates_directory(cls):
        """Get the templates directory."""
        return AppSettings.get("recipe_dir", "")

    @classmethod
    def set_templates_directory(cls, directory):
        """Set the templates directory."""
        return AppSettings.set("recipe_dir", directory)

    @classmethod
    def get_auto_enable(cls):
        """Check if recipes should be auto-enabled."""
        return AppSettings.get_bool("auto_enable_recipes", False)

    @classmethod
    def set_auto_enable(cls, enabled):
        """Set whether recipes should be auto-enabled."""
        return AppSettings.set("auto_enable_recipes", "true" if enabled else "false")


class RecipeSecret(models.Model):
    """Model for storing recipe environment variables as secrets."""

    recipe_id = models.CharField(max_length=255)
    variable_name = models.CharField(max_length=255)
    value = models.TextField(blank=True, null=True)
    encrypted_value = models.TextField(blank=True, null=True)
    is_secret = models.BooleanField(default=False)

    class Meta:
        unique_together = ("recipe_id", "variable_name")
        verbose_name = "Recipe Secret"
        verbose_name_plural = "Recipe Secrets"

    def __str__(self):
        return f"{self.recipe_id} - {self.variable_name}"


class PolicyTemplate(models.Model):
    """Model for storing policy templates with staging/deployed status."""

    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    policy_type = models.CharField(max_length=50)
    state = models.CharField(max_length=20, default="ACTIVE")
    content = models.TextField()  # JSON content
    resources = models.TextField(blank=True, null=True)  # JSON array
    privileges = models.TextField(blank=True, null=True)  # JSON array
    actors = models.TextField(blank=True, null=True)  # JSON array
    deployed = models.BooleanField(default=False)
    deployed_at = models.DateTimeField(null=True, blank=True)
    datahub_urn = models.CharField(
        max_length=255, null=True, blank=True
    )  # Store the DataHub URN when deployed
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    def get_resources(self):
        """Get the resources as a Python object."""
        try:
            return json.loads(self.resources or "[]")
        except Exception:
            return []

    def set_resources(self, resources):
        """Set the resources from a Python object."""
        self.resources = json.dumps(resources or [])

    def get_privileges(self):
        """Get the privileges as a Python object."""
        try:
            return json.loads(self.privileges or "[]")
        except Exception:
            return []

    def set_privileges(self, privileges):
        """Set the privileges from a Python object."""
        self.privileges = json.dumps(privileges or [])

    def get_actors(self):
        """Get the actors as a Python object."""
        try:
            return json.loads(self.actors or "[]")
        except Exception:
            return []

    def set_actors(self, actors):
        """Set the actors from a Python object."""
        self.actors = json.dumps(actors or [])

    def get_policy_id(self):
        """Extract the ID portion from the DataHub URN."""
        if not self.datahub_urn:
            return None

        # URN format: urn:li:policy:<id>
        parts = self.datahub_urn.split(":")
        if len(parts) >= 4:
            return parts[3]
        return None


class PullRequest(models.Model):
    """Model to track GitHub Pull Requests for recipes"""

    PR_STATUS_CHOICES = [
        ("open", "Open"),
        ("closed", "Closed"),
        ("merged", "Merged"),
        ("draft", "Draft"),
    ]

    recipe_id = models.CharField(max_length=255)  # Changed from ForeignKey to CharField
    pr_number = models.IntegerField()
    pr_url = models.URLField()
    status = models.CharField(max_length=10, choices=PR_STATUS_CHOICES, default="open")
    title = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    branch_name = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"PR #{self.pr_number}: {self.title}"


class RecipeInstance(models.Model):
    """Model for storing the combination of a recipe template and an environment variables instance."""

    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    template = models.ForeignKey(RecipeTemplate, on_delete=models.CASCADE)
    env_vars_instance = models.ForeignKey(
        "EnvVarsInstance",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="recipes",
    )
    environment = models.ForeignKey(
        Environment,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="recipe_instances",
    )
    datahub_urn = models.CharField(
        max_length=255, null=True, blank=True
    )  # Store the DataHub URN when deployed
    deployed = models.BooleanField(default=False)
    deployed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    cron_schedule = models.CharField(
        max_length=100, default="0 0 * * *"
    )  # Default to daily at midnight
    timezone = models.CharField(max_length=50, default="UTC")
    debug_mode = models.BooleanField(default=False)

    def __str__(self):
        return f"{self.name} ({self.template.name})"

    @property
    def recipe_type(self):
        """Get the recipe type from the associated template."""
        return self.template.recipe_type if self.template else None

    @property
    def datahub_id(self):
        """Get the DataHub ID for this recipe instance."""
        return self.get_recipe_id()

    @datahub_id.setter
    def datahub_id(self, value):
        """Set the DataHub ID by updating the datahub_urn field."""
        if value:
            self.datahub_urn = f"urn:li:dataHubIngestionSource:{value}"
        else:
            self.datahub_urn = None

    def get_recipe_id(self):
        """Extract the ID portion from the DataHub URN."""
        if not self.datahub_urn:
            return None

        # URN format: urn:li:dataHubIngestionSource:<id>
        parts = self.datahub_urn.split(":")
        if len(parts) >= 4:
            return parts[3]
        return None

    def get_recipe_dict(self):
        """Get the recipe configuration as a dictionary."""
        try:
            # Get the template content
            template_content = self.template.get_content()
            if not template_content:
                return None

            # If we have environment variables, apply them
            if self.env_vars_instance:
                env_vars = self.env_vars_instance.get_variables_dict()
                if env_vars:
                    # Replace environment variables in the template
                    template_content = replace_env_vars_with_values(
                        template_content, env_vars
                    )

            # Ensure the recipe has the correct structure
            if isinstance(template_content, dict):
                if "source" not in template_content:
                    # If the template content is not properly structured, wrap it
                    template_content = {"source": template_content}
            else:
                # If template_content is not a dict, create a proper structure
                template_content = {
                    "source": {
                        "type": self.template.recipe_type,
                        "config": template_content
                        if isinstance(template_content, dict)
                        else {},
                    }
                }

            return template_content
        except Exception as e:
            logger.error(f"Error getting recipe dict for instance {self.id}: {str(e)}")
            return None

    def get_combined_content(self):
        """Get the template content with environment variables applied."""
        template_content = self.template.get_content()

        if not template_content or not self.env_vars_instance:
            return template_content

        try:
            env_vars = self.env_vars_instance.get_variables_dict()
            return replace_env_vars_with_values(template_content, env_vars)
        except Exception:
            logger.error(
                f"Error applying environment variables to template for instance {self.id}"
            )
            return template_content

    def export_to_yaml(self, base_dir=None):
        """Export the recipe instance to a YAML file."""
        if not base_dir:
            base_dir = Path(__file__).parent.parent.parent / "recipes" / "instances"

        # Create directory if it doesn't exist
        base_dir = Path(base_dir)
        base_dir.mkdir(parents=True, exist_ok=True)

        # Get the recipe content
        recipe_content = self.get_combined_content()
        if not recipe_content:
            raise ValueError("Unable to generate recipe content")

        # Create YAML content
        yaml_content = {
            "name": self.name,
            "description": self.description,
            "recipe_type": self.recipe_type,
            "recipe": recipe_content,
        }

        # Add environment variables if present
        if self.env_vars_instance:
            yaml_content["env_vars"] = self.env_vars_instance.get_variables_dict()

        # Write to file
        file_path = base_dir / f"{self.name.lower().replace(' ', '_')}.yml"
        with open(file_path, "w") as f:
            yaml.dump(yaml_content, f, default_flow_style=False, sort_keys=False)

        return file_path


class EnvVarsTemplate(models.Model):
    """Template for environment variables to be used in recipes."""

    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    tags = models.TextField(blank=True, null=True)
    recipe_type = models.CharField(max_length=50, choices=RECIPE_TYPES)
    variables = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    DATA_TYPES = [
        ("text", "Text"),
        ("number", "Number"),
        ("boolean", "Boolean"),
        ("json", "JSON"),
    ]

    def __str__(self):
        return self.name

    def get_variables_dict(self):
        """Return the variables as a python dictionary."""
        if not self.variables:
            return {}
        return json.loads(self.variables)

    def set_variables_dict(self, variables_dict):
        """Set the variables from a python dictionary."""
        self.variables = json.dumps(variables_dict)

    def get_tags_list(self):
        """Return the tags as a list."""
        if not self.tags:
            return []
        return [tag.strip() for tag in self.tags.split(",")]

    def set_tags_list(self, tags_list):
        """Set the tags from a list."""
        self.tags = ",".join(tags_list)

    def get_display_variables(self):
        """Return variables formatted for display in a template."""
        variables = self.get_variables_dict()
        result = []

        for key, details in variables.items():
            result.append(
                {
                    "key": key,
                    "description": details.get("description", ""),
                    "required": details.get("required", False),
                    "is_secret": details.get("is_secret", False),
                    "data_type": details.get("data_type", "text"),
                    "default_value": details.get("default_value", ""),
                }
            )

        return result


class EnvVarsInstance(models.Model):
    """Model for storing actual instances of environment variable configurations."""

    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    template = models.ForeignKey(
        EnvVarsTemplate, on_delete=models.SET_NULL, null=True, blank=True
    )
    environment = models.ForeignKey(
        Environment,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="env_vars_instances",
    )
    recipe_id = models.CharField(
        max_length=255, null=True, blank=True
    )  # Optional link to a recipe
    recipe_type = models.CharField(max_length=50, choices=RECIPE_TYPES)
    variables = models.TextField()  # JSON content with actual values, format: {"KEY": {"value": "actual_value", "isSecret": true/false}}
    deployed = models.BooleanField(default=False)
    deployed_at = models.DateTimeField(null=True, blank=True)
    datahub_secrets_created = models.BooleanField(
        default=False
    )  # Track if secrets have been created in DataHub
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.name} ({self.recipe_id or 'No recipe'})"

    def get_variables_dict(self):
        """Get the environment variables as a dictionary."""
        try:
            return json.loads(self.variables)
        except Exception:
            return {}

    def set_variables_dict(self, variables_dict):
        """Set the environment variables from a dictionary."""
        self.variables = json.dumps(variables_dict)

    def get_secret_variables(self):
        """Get only the variables marked as secrets."""
        variables = self.get_variables_dict()
        return {k: v for k, v in variables.items() if v.get("isSecret", False)}

    @property
    def has_secret_variables(self):
        """Check if this instance has any secret variables."""
        variables = self.get_variables_dict()
        return any(v.get("isSecret", False) for k, v in variables.items())

    def validate_all_variables(self):
        """Validate all variable values against their defined data types in the template."""
        if not self.template:
            return True

        template_vars = self.template.get_variables_dict()
        instance_vars = self.get_variables_dict()

        for key, template_def in template_vars.items():
            # Skip if not required and not provided
            if not template_def.get("required", False) and (
                key not in instance_vars or not instance_vars[key].get("value")
            ):
                continue

            # Check if required key is missing
            if template_def.get("required", False) and (
                key not in instance_vars or not instance_vars[key].get("value")
            ):
                return False

            # Validate type if value exists
            if key in instance_vars and "value" in instance_vars[key]:
                value = instance_vars[key]["value"]
                if not self.template.validate_value_for_type(key, value):
                    return False

        return True

    def get_typed_value(self, key):
        """Get the value converted to its proper data type based on the template."""
        variables = self.get_variables_dict()
        if key not in variables or "value" not in variables[key]:
            return None

        value = variables[key]["value"]

        if not self.template:
            return value

        template_vars = self.template.get_variables_dict()
        if key not in template_vars:
            return value

        data_type = template_vars[key].get("data_type", "text")

        try:
            if data_type == "text":
                return str(value)
            elif data_type == "number":
                return float(value)
            elif data_type == "boolean":
                val_lower = str(value).lower()
                return val_lower in ("true", "yes", "1")
            elif data_type == "json":
                if isinstance(value, str):
                    return json.loads(value)
                return value
            return value
        except Exception:
            return value


class GitHubPR(models.Model):
    """Model for storing GitHub pull request information."""

    recipe_id = models.CharField(max_length=255)
    pr_url = models.URLField()
    pr_number = models.IntegerField()
    pr_status = models.CharField(
        max_length=50,
        choices=[
            ("open", "Open"),
            ("merged", "Merged"),
            ("closed", "Closed"),
            ("pending", "Pending"),
        ],
        default="open",
    )
    branch_name = models.CharField(max_length=255)
    title = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "GitHub Pull Request"
        verbose_name_plural = "GitHub Pull Requests"
        ordering = ["-created_at"]

    def __str__(self):
        return f"PR #{self.pr_number} - {self.title}"

    def get_status_display_color(self):
        """Get a color code for the status."""
        colors = {
            "open": "primary",
            "merged": "success",
            "closed": "danger",
            "pending": "warning",
        }
        return colors.get(self.pr_status, "secondary")


class GitSettings(models.Model):
    """Model to store Git integration settings for multiple providers"""

    PROVIDER_CHOICES = [
        ("github", "GitHub"),
        ("azure_devops", "Azure DevOps"),
        ("gitlab", "GitLab"),
        ("bitbucket", "Bitbucket"),
        ("other", "Other Git Provider"),
    ]

    provider_type = models.CharField(
        max_length=50,
        choices=PROVIDER_CHOICES,
        default="github",
        help_text="Git provider type",
    )
    base_url = models.URLField(
        blank=True,
        null=True,
        help_text="Base API URL (leave empty for GitHub.com, required for Azure DevOps or self-hosted instances)",
    )
    token = models.CharField(max_length=255, help_text="Personal Access Token")
    username = models.CharField(
        max_length=100, help_text="Username, organization, or project name"
    )
    repository = models.CharField(max_length=100, help_text="Repository name")
    current_branch = models.CharField(
        max_length=255, default="main", help_text="Current branch for Git operations"
    )
    enabled = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Git Settings"
        verbose_name_plural = "Git Settings"

    def __str__(self):
        if self.provider_type == "github":
            return f"GitHub: {self.username}/{self.repository}"
        elif self.provider_type == "azure_devops":
            return f"Azure DevOps: {self.username}/{self.repository}"
        else:
            return f"{self.provider_type.title()}: {self.username}/{self.repository}"

    @classmethod
    def get_instance(cls):
        """Get or create settings instance (singleton pattern)"""
        settings = cls.objects.first()
        if not settings:
            settings = cls.objects.create(
                token="", username="", repository="", enabled=False
            )
        return settings

    @classmethod
    def get_token(cls):
        """Get the Git token from settings"""
        settings = cls.get_instance()
        return settings.token if settings else ""

    @classmethod
    def get_username(cls):
        """Get the Git username from settings"""
        settings = cls.get_instance()
        return settings.username if settings else ""

    @classmethod
    def get_repository(cls):
        """Get the Git repository from settings"""
        settings = cls.get_instance()
        return settings.repository if settings else ""

    @classmethod
    def get_provider_type(cls):
        """Get the Git provider type from settings"""
        settings = cls.get_instance()
        return settings.provider_type if settings else "github"

    @classmethod
    def get_base_url(cls):
        """Get the Git base URL from settings"""
        settings = cls.get_instance()
        return settings.base_url if settings else ""

    @classmethod
    def set_token(cls, token):
        """Set the Git token in settings"""
        settings = cls.get_instance()
        settings.token = token
        settings.save()

    @classmethod
    def set_username(cls, username):
        """Set the Git username in settings"""
        settings = cls.get_instance()
        settings.username = username
        settings.save()

    @classmethod
    def set_repository(cls, repository):
        """Set the Git repository in settings"""
        settings = cls.get_instance()
        settings.repository = repository
        settings.save()

    @classmethod
    def is_configured(cls):
        """Check if Git settings are properly configured"""
        settings = cls.get_instance()
        return settings.enabled and bool(
            settings.token and settings.username and settings.repository
        )

    @classmethod
    def get_branches(cls):
        """Fetch all branches from Git repository."""
        if not cls.is_configured():
            return []

        provider = cls.get_provider_type()
        cls.get_instance()
        logger = logging.getLogger(__name__)

        try:
            if provider == "github":
                # GitHub branches API
                branches_url = cls.get_api_url("/branches")
                logger.info(f"Fetching branches from: {branches_url}")

                try:
                    response = cls._make_request("GET", branches_url)
                    branches = [branch["name"] for branch in response.json()]
                    logger.info(f"Successfully fetched {len(branches)} branches")
                    return branches
                except Exception as e:
                    logger.error(f"Error in GitHub branches API call: {str(e)}")
                    if isinstance(e, requests.exceptions.HTTPError) and hasattr(
                        e, "response"
                    ):
                        logger.error(
                            f"Response status: {e.response.status_code}, Text: {e.response.text}"
                        )
                    return []

            elif provider == "azure_devops":
                # Azure DevOps branches API
                branches_url = cls.get_api_url("/refs")
                # Add filter for branches only
                if "?" in branches_url:
                    branches_url += "&filter=heads/"
                else:
                    branches_url += "?filter=heads/"

                logger.info(f"Fetching branches from: {branches_url}")
                response = cls._make_request("GET", branches_url)
                # Format differs from GitHub
                branches_data = response.json().get("value", [])
                return [
                    branch["name"].replace("refs/heads/", "")
                    for branch in branches_data
                ]

            elif provider == "gitlab":
                # GitLab branches API
                branches_url = cls.get_api_url("/repository/branches")
                logger.info(f"Fetching branches from: {branches_url}")
                response = cls._make_request("GET", branches_url)
                return [branch["name"] for branch in response.json()]

            elif provider == "bitbucket":
                # Bitbucket branches API
                if "bitbucket.org" in cls.get_api_url():
                    # Bitbucket Cloud
                    branches_url = cls.get_api_url("/refs/branches")
                else:
                    # Bitbucket Server
                    branches_url = cls.get_api_url("/branches")

                logger.info(f"Fetching branches from: {branches_url}")
                response = cls._make_request("GET", branches_url)
                # Format differs between Cloud and Server
                if "values" in response.json():
                    # Bitbucket Cloud
                    return [branch["name"] for branch in response.json()["values"]]
                else:
                    # Bitbucket Server
                    return [branch["displayId"] for branch in response.json()]
            else:
                logger.error(f"Unsupported Git provider: {provider}")
                return []

        except Exception as e:
            logger.error(f"Error fetching branches: {str(e)}")
            # Include traceback for easier debugging
            import traceback

            logger.error(f"Traceback: {traceback.format_exc()}")
            return []


class GitIntegration:
    """Helper class for Git operations with multiple providers."""

    @classmethod
    def is_configured(cls):
        """Check if Git integration is configured."""
        settings = GitSettings.get_instance()
        return settings and settings.is_configured()

    @classmethod
    def get_api_url(cls, endpoint=""):
        """Get the Git API URL for the configured repository."""
        settings = GitSettings.get_instance()
        if not settings:
            return None

        provider = settings.provider_type
        base_url = settings.base_url

        # Construct URL based on provider type
        if provider == "github":
            # GitHub API
            if base_url:
                # GitHub Enterprise or custom URL
                api_base = f"{base_url.rstrip('/')}/repos/{settings.username}/{settings.repository}"
            else:
                # GitHub.com
                api_base = f"https://api.github.com/repos/{settings.username}/{settings.repository}"
        elif provider == "azure_devops":
            # Azure DevOps API
            org_project = settings.username.split("/")
            if len(org_project) != 2:
                logger.error(
                    f"Invalid Azure DevOps username format: {settings.username}. Expected: organization/project"
                )
                return None

            org, project = org_project
            if base_url:
                # Custom Azure DevOps URL
                api_base = f"{base_url.rstrip('/')}/{org}/{project}/_apis/git/repositories/{settings.repository}"
            else:
                # Default Azure DevOps URL
                api_base = f"https://dev.azure.com/{org}/{project}/_apis/git/repositories/{settings.repository}"
        elif provider == "gitlab":
            # GitLab API
            if base_url:
                # Self-hosted GitLab
                api_base = f"{base_url.rstrip('/')}/api/v4/projects/{settings.username}%2F{settings.repository}"
            else:
                # GitLab.com
                api_base = f"https://gitlab.com/api/v4/projects/{settings.username}%2F{settings.repository}"
        elif provider == "bitbucket":
            # Bitbucket API
            if base_url:
                # Self-hosted Bitbucket
                api_base = f"{base_url.rstrip('/')}/rest/api/1.0/projects/{settings.username}/repos/{settings.repository}"
            else:
                # Bitbucket.org
                api_base = f"https://api.bitbucket.org/2.0/repositories/{settings.username}/{settings.repository}"
        else:
            # Custom/Other Git provider - use base_url as is
            if not base_url:
                logger.error(f"Base URL is required for provider type: {provider}")
                return None
            api_base = f"{base_url.rstrip('/')}"

        return f"{api_base}{endpoint}" if endpoint else api_base

    @classmethod
    def _make_request(cls, method, url, **kwargs):
        """Make a request to the Git provider API."""
        settings = GitSettings.get_instance()
        if not settings or not settings.token:
            raise ValueError("Git token not configured")

        headers = {}
        provider = settings.provider_type

        # Set authorization headers based on provider
        if provider == "github":
            headers = {
                "Authorization": f"token {settings.token}",
                "Accept": "application/vnd.github.v3+json",
            }
        elif provider == "azure_devops":
            # Azure DevOps uses Basic Auth with PAT
            auth_token = base64.b64encode(f":{settings.token}".encode()).decode()
            headers = {
                "Authorization": f"Basic {auth_token}",
                "Content-Type": "application/json",
            }
            # Add api-version for Azure DevOps
            if "?" in url:
                url = f"{url}&api-version=6.0"
            else:
                url = f"{url}?api-version=6.0"
        elif provider == "gitlab":
            headers = {
                "Private-Token": settings.token,
                "Content-Type": "application/json",
            }
        elif provider == "bitbucket":
            if "bitbucket.org" in url:
                # Bitbucket Cloud uses OAuth or App passwords
                auth_token = base64.b64encode(
                    f"{settings.username}:{settings.token}".encode()
                ).decode()
                headers = {
                    "Authorization": f"Basic {auth_token}",
                    "Content-Type": "application/json",
                }
            else:
                # Bitbucket Server uses different auth
                headers = {
                    "Authorization": f"Bearer {settings.token}",
                    "Content-Type": "application/json",
                }
        else:
            # Default to token in Authorization header
            headers = {
                "Authorization": f"token {settings.token}",
                "Content-Type": "application/json",
            }

        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))

        response = requests.request(method, url, headers=headers, **kwargs)
        response.raise_for_status()
        return response

    @classmethod
    def get_branches(cls):
        """Fetch all branches from Git repository."""
        settings = GitSettings.get_instance()
        if not cls.is_configured():
            logger.warning("Git integration not configured, cannot fetch branches")
            return []

        provider = settings.provider_type

        try:
            if provider == "github":
                # GitHub branches API
                branches_url = cls.get_api_url("/branches")
                logger.info(f"Fetching branches from: {branches_url}")

                try:
                    response = cls._make_request("GET", branches_url)
                    branches = [branch["name"] for branch in response.json()]
                    logger.info(f"Successfully fetched {len(branches)} branches")
                    return branches
                except Exception as e:
                    logger.error(f"Error in GitHub branches API call: {str(e)}")
                    if isinstance(e, requests.exceptions.HTTPError) and hasattr(
                        e, "response"
                    ):
                        logger.error(
                            f"Response status: {e.response.status_code}, Text: {e.response.text}"
                        )
                    return []

            elif provider == "azure_devops":
                # Azure DevOps branches API
                branches_url = cls.get_api_url("/refs")
                # Add filter for branches only
                if "?" in branches_url:
                    branches_url += "&filter=heads/"
                else:
                    branches_url += "?filter=heads/"

                logger.info(f"Fetching branches from: {branches_url}")
                response = cls._make_request("GET", branches_url)
                # Format differs from GitHub
                branches_data = response.json().get("value", [])
                return [
                    branch["name"].replace("refs/heads/", "")
                    for branch in branches_data
                ]

            elif provider == "gitlab":
                # GitLab branches API
                branches_url = cls.get_api_url("/repository/branches")
                logger.info(f"Fetching branches from: {branches_url}")
                response = cls._make_request("GET", branches_url)
                return [branch["name"] for branch in response.json()]

            elif provider == "bitbucket":
                # Bitbucket branches API
                if "bitbucket.org" in cls.get_api_url():
                    # Bitbucket Cloud
                    branches_url = cls.get_api_url("/refs/branches")
                else:
                    # Bitbucket Server
                    branches_url = cls.get_api_url("/branches")

                logger.info(f"Fetching branches from: {branches_url}")
                response = cls._make_request("GET", branches_url)
                # Format differs between Cloud and Server
                if "values" in response.json():
                    # Bitbucket Cloud
                    return [branch["name"] for branch in response.json()["values"]]
                else:
                    # Bitbucket Server
                    return [branch["displayId"] for branch in response.json()]
            else:
                logger.error(f"Unsupported Git provider: {provider}")
                return []

        except Exception as e:
            logger.error(f"Error fetching branches: {str(e)}")
            # Include traceback for easier debugging
            import traceback

            logger.error(f"Traceback: {traceback.format_exc()}")
            return []

    @classmethod
    def stage_changes(cls, instance_or_template, commit_message=None):
        """Stage changes on the current branch without creating a PR."""
        if not cls.is_configured():
            logger.error("Git integration not configured")
            return {"success": False, "error": "Git integration not configured"}

        try:
            # Create directories if they don't exist
            base_dir = Path(__file__).parent.parent.parent
            recipes_dir = base_dir / "recipes"
            templates_dir = recipes_dir / "templates"
            instances_dir = recipes_dir / "instances"
            params_dir = base_dir / "params"
            params_envs_dir = params_dir / "environments"
            params_instances_dir = params_dir / "instances"
            policies_dir = base_dir / "policies"
            metadata_tests_dir = base_dir / "metadata_tests"

            # Create all necessary directories
            for dir_path in [
                recipes_dir,
                templates_dir,
                instances_dir,
                params_dir,
                params_envs_dir,
                params_instances_dir,
                policies_dir,
                metadata_tests_dir,
            ]:
                dir_path.mkdir(parents=True, exist_ok=True)

            # Export to YAML/JSON based on object type
            if isinstance(instance_or_template, RecipeInstance):
                # Recipe instance
                instance = instance_or_template

                # Get environment name from instance or default to 'dev'
                env_name = "dev"
                if instance.environment:
                    env_name = instance.environment.name.lower()

                # Create environment directories if they don't exist
                templates_env_dir = templates_dir / env_name
                templates_env_dir.mkdir(exist_ok=True)

                instances_env_dir = instances_dir / env_name
                instances_env_dir.mkdir(exist_ok=True)

                # 1. Export template to templates directory
                if instance.template:
                    logger.info(
                        f"Exporting template for recipe instance: {instance.name}"
                    )
                    recipe_type = instance.template.recipe_type.lower()
                    template_file_path = templates_env_dir / f"{recipe_type}.yml"

                    # Get template content
                    template_content = instance.template.get_content()

                    # Export the template
                    with open(template_file_path, "w") as f:
                        yaml.dump(
                            template_content,
                            f,
                            Dumper=CustomYamlDumper,
                            default_flow_style=False,
                            sort_keys=False,
                            indent=2,
                        )

                # 2. Export environment variables to instances directory
                if instance.env_vars_instance:
                    logger.info(
                        f"Exporting environment variables for recipe instance: {instance.name}"
                    )
                    recipe_type = instance.recipe_type.lower()
                    env_vars_file_path = instances_env_dir / f"{recipe_type}.yml"

                    # Format variables for YAML file
                    variables = instance.env_vars_instance.get_variables_dict()

                    # Create YAML content with proper spacing and indentation
                    env_vars_content = {
                        "name": instance.env_vars_instance.name,
                        "description": instance.env_vars_instance.description
                        or f"Environment variables for {instance.recipe_type}",
                        "recipe_type": instance.recipe_type,
                        "parameters": {},
                    }

                    # Add all non-secret values to parameters with proper indentation
                    secret_refs = []
                    for var_name, var_info in variables.items():
                        if var_info.get("isSecret", False):
                            secret_refs.append(var_name)
                        else:
                            env_vars_content["parameters"][var_name] = var_info.get(
                                "value", ""
                            )

                    # Add secret references if there are any, with proper indentation
                    if secret_refs:
                        env_vars_content["secret_references"] = secret_refs

                    # Add template reference if available
                    if instance.env_vars_instance.template:
                        env_vars_content["template"] = (
                            instance.env_vars_instance.template.name
                        )

                    # Export the environment variables
                    with open(env_vars_file_path, "w") as f:
                        yaml.dump(
                            env_vars_content,
                            f,
                            Dumper=CustomYamlDumper,
                            default_flow_style=False,
                            sort_keys=False,
                            indent=2,
                        )

                # 3. Create the linking file
                logger.info(
                    f"Creating linking file for recipe instance: {instance.name}"
                )

                # Create params/instances directory structure if it doesn't exist
                params_dir = base_dir / "params"
                params_instances_dir = params_dir / "instances"
                params_env_dir = params_instances_dir / env_name
                params_env_dir.mkdir(parents=True, exist_ok=True)

                # Create YAML content formatted for the linking file
                linking_content = {
                    "name": instance.name,
                    "description": instance.description,
                }

                # Add references to template and env vars
                if instance.template:
                    linking_content["template"] = {
                        "name": instance.template.name,
                        "id": instance.template.id,
                        "path": f"recipes/templates/{instance.template.recipe_type.lower()}.yml",
                    }

                if instance.env_vars_instance:
                    linking_content["env_vars_instance"] = {
                        "name": instance.env_vars_instance.name,
                        "id": instance.env_vars_instance.id,
                        "path": f"recipes/instances/{env_name}/{instance.recipe_type.lower()}.yml",
                    }

                # Add deployment status information
                if instance.deployed:
                    linking_content["deployment"] = {
                        "deployed": True,
                        "deployed_at": instance.deployed_at.isoformat()
                        if instance.deployed_at
                        else None,
                        "datahub_urn": instance.datahub_urn,
                    }

                # Create file with clean name (no spaces, lowercase)
                instance_name = instance.name.replace(" ", "-").lower()
                linking_file_path = params_env_dir / f"{instance_name}.yml"

                # Write the linking file
                with open(linking_file_path, "w") as f:
                    yaml.dump(
                        linking_content,
                        f,
                        Dumper=CustomYamlDumper,
                        default_flow_style=False,
                        sort_keys=False,
                        indent=2,
                    )

                # Use the linking file path as the main file path
                file_path = linking_file_path

                pr_title = f"Update recipe instance: {instance.name}"

            elif isinstance(instance_or_template, RecipeTemplate):
                logger.info(f"Exporting recipe template: {instance_or_template.name}")

                # Get recipe type as name
                recipe_type = instance_or_template.recipe_type.lower()

                # Create file with recipe type as name
                file_path = templates_dir / f"{recipe_type}.yml"

                # Get content as dict
                content = instance_or_template.get_content()

                # Export the template
                with open(file_path, "w") as f:
                    yaml.dump(content, f, default_flow_style=False)

                pr_title = f"Update recipe template: {instance_or_template.name}"

            elif isinstance(instance_or_template, Policy):
                logger.info(f"Exporting policy: {instance_or_template.name}")

                # Get environment (default to 'prod' if not specified)
                if instance_or_template.environment:
                    environment = instance_or_template.environment.name.lower()
                else:
                    environment = Environment.get_default().name.lower()

                # Create environment directory if it doesn't exist
                env_policies_dir = policies_dir / environment
                env_policies_dir.mkdir(exist_ok=True)

                # Create file with clean name (no spaces, lowercase)
                policy_name = instance_or_template.name.replace(" ", "_").lower()
                file_path = env_policies_dir / f"{policy_name}.json"

                # Get policy data
                policy_data = instance_or_template.to_dict()

                # Export the policy as JSON
                try:
                    with open(file_path, "w") as f:
                        json.dump(policy_data, f, indent=2)
                    logger.info(f"Policy exported successfully to {file_path}")
                except Exception as e:
                    logger.error(f"Error exporting policy to JSON: {str(e)}")
                    return {
                        "success": False,
                        "error": f"Failed to export policy to JSON: {str(e)}",
                    }

                pr_title = f"Update policy: {instance_or_template.name}"

            elif isinstance(instance_or_template, EnvVarsTemplate):
                logger.info(
                    f"Exporting environment variables template: {instance_or_template.name}"
                )

                # Get environment (default to 'dev' if not specified)
                env_name = "dev"

                # Create file with recipe type as name
                template_name = instance_or_template.recipe_type.lower()
                file_path = templates_dir / f"{template_name}.yml"

                # Format the template variables for the YAML file
                template_vars = instance_or_template.get_variables_dict()
                content = {
                    "name": instance_or_template.name,
                    "description": instance_or_template.description
                    or f"Template for {instance_or_template.recipe_type}",
                    "recipe_type": instance_or_template.recipe_type,
                    "source": {"type": instance_or_template.recipe_type, "config": {}},
                }

                # Add variables as environment variable references in the config
                for var_name, var_info in template_vars.items():
                    content["source"]["config"][
                        var_info.get("key", var_name.lower())
                    ] = f"${{{var_name}}}"

                # Export the template
                with open(file_path, "w") as f:
                    yaml.dump(content, f, default_flow_style=False)

                # Create the variable definitions YAML in params/environments
                params_env_dir = params_envs_dir / env_name
                params_env_dir.mkdir(exist_ok=True)

                # Create variables definition file
                vars_file_path = params_env_dir / f"{template_name}_vars.yml"
                vars_content = {
                    "name": f"{instance_or_template.name} Variables",
                    "description": f"Environment variables for {instance_or_template.name}",
                    "recipe_type": instance_or_template.recipe_type,
                    "variables": {},
                }

                # Add variable definitions
                for var_name, var_info in template_vars.items():
                    vars_content["variables"][var_name] = {
                        "description": var_info.get("description", ""),
                        "required": var_info.get("required", False),
                        "is_secret": var_info.get("is_secret", False),
                        "data_type": var_info.get("data_type", "text"),
                        "default_value": var_info.get("default_value", ""),
                    }

                # Write the variables definition file
                with open(vars_file_path, "w") as f:
                    yaml.dump(vars_content, f, default_flow_style=False)

                pr_title = f"Update environment variables template: {instance_or_template.name}"

            elif isinstance(instance_or_template, EnvVarsInstance):
                logger.info(
                    f"Exporting environment variables instance: {instance_or_template.name}"
                )

                # Get environment (default to 'dev' if not specified)
                env_name = "dev"
                if instance_or_template.environment:
                    env_name = instance_or_template.environment.name.lower()

                # Create environment directory if it doesn't exist
                instances_env_dir = instances_dir / env_name
                instances_env_dir.mkdir(exist_ok=True)

                # Get source file name based on recipe type
                file_name = f"{instance_or_template.recipe_type.lower()}.yml"
                file_path = instances_env_dir / file_name

                # Format variables for YAML file
                variables = instance_or_template.get_variables_dict()

                # Create YAML content with proper spacing and indentation
                content = {
                    "name": instance_or_template.name,
                    "description": instance_or_template.description
                    or f"Environment variables for {instance_or_template.recipe_type}",
                    "recipe_type": instance_or_template.recipe_type,
                    "parameters": {},
                }

                # Add all non-secret values to parameters with proper indentation
                secret_refs = []
                for var_name, var_info in variables.items():
                    if var_info.get("isSecret", False):
                        secret_refs.append(var_name)
                    else:
                        content["parameters"][var_name] = var_info.get("value", "")

                # Add secret references if there are any, with proper indentation
                if secret_refs:
                    # Use a list directly instead of adding to the dictionary
                    # to preserve proper YAML formatting
                    content["secret_references"] = secret_refs

                # Add template reference if available
                if instance_or_template.template:
                    content["template"] = instance_or_template.template.name

                # Export the instance with proper YAML formatting
                with open(file_path, "w") as f:
                    # Use our custom dumper for proper list indentation
                    yaml.dump(
                        content,
                        f,
                        Dumper=CustomYamlDumper,
                        default_flow_style=False,
                        sort_keys=False,
                        indent=2,
                        width=70,
                    )

                pr_title = f"Update environment variables instance: {instance_or_template.name}"

            # Handle metadata tests (identified by having id, name, definition, and environment attributes)
            elif (
                hasattr(instance_or_template, "id")
                and hasattr(instance_or_template, "name")
                and hasattr(instance_or_template, "definition")
                and hasattr(instance_or_template, "environment")
            ):
                logger.info(f"Exporting metadata test: {instance_or_template.name}")

                # Get environment (default to 'prod' if not specified)
                if instance_or_template.environment:
                    environment = instance_or_template.environment.name.lower()
                else:
                    environment = Environment.get_default().name.lower()

                # Create environment directory if it doesn't exist
                env_tests_dir = metadata_tests_dir / environment
                env_tests_dir.mkdir(exist_ok=True)

                # Generate a safe file name from the test name
                import re

                safe_name = re.sub(
                    r"[^a-zA-Z0-9_-]", "_", instance_or_template.name.lower()
                )
                file_path = env_tests_dir / f"{safe_name}.yaml"

                # Get test content
                if hasattr(instance_or_template, "to_yaml"):
                    content = instance_or_template.to_yaml()
                else:
                    content = instance_or_template.definition

                # Export the test content
                with open(file_path, "w") as f:
                    f.write(content)

                pr_title = f"Add/update metadata test: {instance_or_template.name}"

            # Handle assertions (identified by having to_dict method and assertion_type)
            elif (
                hasattr(instance_or_template, "to_dict")
                and hasattr(instance_or_template, "assertion_type")
                and hasattr(instance_or_template, "environment")
            ):
                logger.info(f"Exporting assertion: {instance_or_template.name}")

                # Get environment
                environment = instance_or_template.environment

                # Create metadata-manager directory structure
                metadata_manager_dir = base_dir / "metadata-manager" / environment / "assertions"
                metadata_manager_dir.mkdir(parents=True, exist_ok=True)

                # Get assertion data which includes the unique filename
                assertion_data = instance_or_template.to_dict()
                
                # Determine operation and type for filename
                operation = assertion_data.get("operation", "create")
                assertion_type = assertion_data.get("assertion_type", "UNKNOWN")
                
                # Use the filename from assertion data to ensure uniqueness and prevent overwrites
                filename = assertion_data.get("filename")
                if not filename:
                    # Fallback to ID-based naming if filename not present
                    filename = f"{operation}_{assertion_type}_{instance_or_template.id}_{instance_or_template.name.lower().replace(' ', '_')}.json"
                
                file_path = metadata_manager_dir / filename

                # Export the assertion as JSON
                import json
                with open(file_path, "w") as f:
                    json.dump(assertion_data, f, indent=2)

                pr_title = f"Add assertion '{instance_or_template.name}' for {environment} environment"

            # Handle metadata tests (identified by having id, name, definition, and environment attributes)
            elif (
                hasattr(instance_or_template, "id")
                and hasattr(instance_or_template, "name")
                and hasattr(instance_or_template, "definition")
                and hasattr(instance_or_template, "environment")
            ):
                logger.info(f"Exporting metadata test: {instance_or_template.name}")

                # Get environment (default to 'prod' if not specified)
                if instance_or_template.environment:
                    environment = instance_or_template.environment.name.lower()
                else:
                    environment = Environment.get_default().name.lower()

                # Create environment directory if it doesn't exist
                env_tests_dir = metadata_tests_dir / environment
                env_tests_dir.mkdir(exist_ok=True)

                # Generate a safe file name from the test name
                import re

                safe_name = re.sub(
                    r"[^a-zA-Z0-9_-]", "_", instance_or_template.name.lower()
                )
                file_path = env_tests_dir / f"{safe_name}.yaml"

                # Get test content
                if hasattr(instance_or_template, "to_yaml"):
                    content = instance_or_template.to_yaml()
                else:
                    content = instance_or_template.definition

                # Export the test content
                with open(file_path, "w") as f:
                    f.write(content)

                pr_title = f"Add/update metadata test: {instance_or_template.name}"

            else:
                err_msg = f"Invalid object type: {type(instance_or_template)}"
                logger.error(err_msg)
                return {"success": False, "error": err_msg}

            if not commit_message:
                commit_message = pr_title

            # Get current branch from settings
            settings = GitSettings.get_instance()
            current_branch = settings.current_branch
            provider = settings.provider_type

            if not current_branch:
                err_msg = "No current branch selected in Git settings"
                logger.error(err_msg)
                return {"success": False, "error": err_msg}

            # Read file content
            with open(file_path, "r") as f:
                content = f.read()

            logger.info(f"Uploading file to Git provider: {file_path}")

            # Convert the file path to be relative to the repository root
            repo_path = file_path.relative_to(base_dir)
            logger.info(f"Repository path: {repo_path}")

            # Provider-specific file upload logic
            if provider == "github":
                # GitHub file API
                content_url = cls.get_api_url(f"/contents/{repo_path}")
                logger.info(f"GitHub API URL: {content_url}")

                data = {
                    "message": commit_message,
                    "content": base64.b64encode(content.encode()).decode(),
                    "branch": current_branch,
                }

                # Check if file exists
                try:
                    logger.info(f"Checking if file exists: {content_url}")
                    response = cls._make_request(
                        "GET", content_url, params={"ref": current_branch}
                    )
                    data["sha"] = response.json().get("sha")
                    logger.info(f"File exists, updating with SHA: {data['sha']}")
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code != 404:
                        logger.error(
                            f"Error checking file existence: {e.response.status_code} {e.response.text}"
                        )
                        return {
                            "success": False,
                            "error": f"Error checking file existence: {e.response.status_code} {e.response.text}",
                        }
                    logger.info("File does not exist, creating new file")

                try:
                    response = cls._make_request("PUT", content_url, json=data)
                    logger.info(f"File uploaded successfully: {response.status_code}")
                except requests.exceptions.HTTPError as e:
                    logger.error(
                        f"Error uploading file: {e.response.status_code} {e.response.text}"
                    )
                    return {
                        "success": False,
                        "error": f"Error uploading file: {e.response.status_code} {e.response.text}",
                    }

            elif provider == "azure_devops":
                # Azure DevOps uses a different API for file operations
                repo_id = None

                # First, get repository ID
                repo_url = cls.get_api_url()
                repo_response = cls._make_request("GET", repo_url)
                repo_id = repo_response.json().get("id")

                if not repo_id:
                    logger.error("Could not find repository ID")
                    return {"success": False, "error": "Could not find repository ID"}

                # Azure DevOps uses a different API structure
                org_project = settings.username.split("/")
                if len(org_project) != 2:
                    logger.error(
                        f"Invalid Azure DevOps username format: {settings.username}"
                    )
                    return {
                        "success": False,
                        "error": "Invalid Azure DevOps username format",
                    }

                org, project = org_project

                # For Azure DevOps, first check if file exists to get its objectId
                item_url = cls.get_api_url(
                    f"/items?path={repo_path}&versionDescriptor.version={current_branch}"
                )
                file_exists = True
                file_object_id = None

                try:
                    item_response = cls._make_request("GET", item_url)
                    file_object_id = item_response.json().get("objectId")
                except requests.exceptions.HTTPError:
                    file_exists = False

                # Create push data
                push_url = cls.get_api_url("/pushes")

                push_data = {
                    "refUpdates": [
                        {
                            "name": f"refs/heads/{current_branch}",
                            "oldObjectId": file_object_id
                            if file_exists
                            else "0000000000000000000000000000000000000000",
                        }
                    ],
                    "commits": [
                        {
                            "comment": commit_message,
                            "changes": [
                                {
                                    "changeType": "edit" if file_exists else "add",
                                    "item": {"path": str(repo_path)},
                                    "newContent": {
                                        "content": content,
                                        "contentType": "rawtext",
                                    },
                                }
                            ],
                        }
                    ],
                }

                try:
                    response = cls._make_request("POST", push_url, json=push_data)
                    logger.info("File pushed successfully to Azure DevOps")
                except requests.exceptions.HTTPError as e:
                    logger.error(f"Error pushing file to Azure DevOps: {e}")
                    return {
                        "success": False,
                        "error": f"Error pushing file to Azure DevOps: {e}",
                    }

            elif provider == "gitlab":
                # GitLab file API
                file_path_encoded = str(repo_path).replace("/", "%2F")
                content_url = cls.get_api_url(f"/repository/files/{file_path_encoded}")

                data = {
                    "branch": current_branch,
                    "content": content,
                    "commit_message": commit_message,
                }

                # Check if file exists
                try:
                    check_url = f"{content_url}?ref={current_branch}"
                    cls._make_request("GET", check_url)
                    # File exists, use PUT to update
                    method = "PUT"
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 404:
                        # File doesn't exist, use POST to create
                        method = "POST"
                    else:
                        logger.error(f"Error checking file existence: {e}")
                        return {
                            "success": False,
                            "error": f"Error checking file existence: {e}",
                        }

                try:
                    response = cls._make_request(method, content_url, json=data)
                    logger.info("File uploaded successfully to GitLab")
                except requests.exceptions.HTTPError as e:
                    logger.error(f"Error uploading file to GitLab: {e}")
                    return {
                        "success": False,
                        "error": f"Error uploading file to GitLab: {e}",
                    }

            elif provider == "bitbucket":
                # Bitbucket file API differs between Cloud and Server
                if "bitbucket.org" in cls.get_api_url():
                    # Bitbucket Cloud
                    content_url = cls.get_api_url(f"/src/{current_branch}/{repo_path}")

                    # Bitbucket Cloud uses form data
                    files = {"content": (str(repo_path), content)}
                    data = {"message": commit_message}

                    try:
                        response = cls._make_request(
                            "POST", content_url, files=files, data=data
                        )
                        logger.info("File uploaded successfully to Bitbucket Cloud")
                    except requests.exceptions.HTTPError as e:
                        logger.error(f"Error uploading file to Bitbucket Cloud: {e}")
                        return {
                            "success": False,
                            "error": f"Error uploading file to Bitbucket Cloud: {e}",
                        }

                else:
                    # Bitbucket Server
                    content_url = cls.get_api_url(f"/browse/{repo_path}")

                    # First check if file exists
                    try:
                        cls._make_request("GET", content_url)
                        file_exists = True
                    except requests.exceptions.HTTPError:
                        file_exists = False

                    # Bitbucket Server API for file operations
                    branch_url = cls.get_api_url(f"/branches/{current_branch}")
                    branch_response = cls._make_request("GET", branch_url)
                    latest_commit = branch_response.json().get("latestCommit")

                    if not latest_commit:
                        logger.error("Could not get latest commit for branch")
                        return {
                            "success": False,
                            "error": "Could not get latest commit for branch",
                        }

                    # Create commit data
                    commit_url = cls.get_api_url("/commits")

                    commit_data = {
                        "message": commit_message,
                        "parents": [latest_commit],
                        "branch": current_branch,
                        "files": [
                            {
                                "path": str(repo_path),
                                "content": content,
                                "operation": "MODIFY" if file_exists else "ADD",
                            }
                        ],
                    }

                    try:
                        response = cls._make_request(
                            "POST", commit_url, json=commit_data
                        )
                        logger.info("File uploaded successfully to Bitbucket Server")
                    except requests.exceptions.HTTPError as e:
                        logger.error(f"Error uploading file to Bitbucket Server: {e}")
                        return {
                            "success": False,
                            "error": f"Error uploading file to Bitbucket Server: {e}",
                        }
            else:
                logger.error(f"Unsupported Git provider: {provider}")
                return {
                    "success": False,
                    "error": f"Unsupported Git provider: {provider}",
                }

            return {
                "success": True,
                "branch": current_branch,
                "file_path": str(file_path),
            }

        except Exception as e:
            error_msg = f"Error staging changes: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {"success": False, "error": error_msg}

    @classmethod
    def create_pr_from_staged_changes(cls, title=None, description=None, base=None):
        """Create a PR from staged changes on the current branch."""
        if not cls.is_configured():
            logger.error("Git integration not configured")
            return None

        try:
            # Get current branch from settings
            settings = GitSettings.get_instance()
            current_branch = settings.current_branch
            provider = settings.provider_type

            if not current_branch:
                logger.error("No current branch selected in Git settings")
                return None

            # Get default branch based on provider
            if provider == "github":
                # GitHub API
                api_url = cls.get_api_url()
                response = cls._make_request("GET", api_url)
                default_branch = response.json().get("default_branch", "main")

                # Create PR
                pr_url = cls.get_api_url("/pulls")
                data = {
                    "title": title or f"Update from branch: {current_branch}",
                    "body": description or f"Changes from branch: {current_branch}",
                    "head": current_branch,
                    "base": base or default_branch,
                }

                response = cls._make_request("POST", pr_url, json=data)
                pr_data = response.json()

                pr_number = pr_data.get("number")
                html_url = pr_data.get("html_url")

                # Create a record of the PR
                GitHubPR.objects.create(
                    recipe_id="multiple",  # Generic ID for multi-resource PRs
                    pr_url=html_url,
                    pr_number=pr_number,
                    branch_name=current_branch,
                    title=title or f"Update from branch: {current_branch}",
                    description=description or "",
                    pr_status="open",
                )

                return {"success": True, "pr_number": pr_number, "pr_url": html_url}

            elif provider == "azure_devops":
                # Azure DevOps API
                org_project = settings.username.split("/")
                if len(org_project) != 2:
                    logger.error(
                        f"Invalid Azure DevOps username format: {settings.username}"
                    )
                    return None

                org, project = org_project

                # First get repo ID
                repo_url = cls.get_api_url()
                repo_response = cls._make_request("GET", repo_url)
                repo_id = repo_response.json().get("id")

                if not repo_id:
                    logger.error("Could not find repository ID")
                    return None

                # Get default branch
                repo_info = repo_response.json()
                default_branch = repo_info.get(
                    "defaultBranch", "refs/heads/main"
                ).replace("refs/heads/", "")

                # Azure DevOps uses a different endpoint for PRs
                if settings.base_url:
                    # Custom Azure DevOps URL
                    pr_url = f"{settings.base_url.rstrip('/')}/{org}/{project}/_apis/git/repositories/{repo_id}/pullrequests"
                else:
                    # Default Azure DevOps URL
                    pr_url = f"https://dev.azure.com/{org}/{project}/_apis/git/repositories/{repo_id}/pullrequests"

                # Create PR data
                pr_data = {
                    "sourceRefName": f"refs/heads/{current_branch}",
                    "targetRefName": f"refs/heads/{base or default_branch}",
                    "title": title or f"Update from branch: {current_branch}",
                    "description": description
                    or f"Changes from branch: {current_branch}",
                }

                # Add API version
                if "?" in pr_url:
                    pr_url = f"{pr_url}&api-version=6.0"
                else:
                    pr_url = f"{pr_url}?api-version=6.0"

                response = cls._make_request("POST", pr_url, json=pr_data)
                result = response.json()

                pr_id = result.get("pullRequestId")
                web_url = result.get("url")

                # Create a record of the PR
                GitHubPR.objects.create(
                    recipe_id="multiple",  # Generic ID for multi-resource PRs
                    pr_url=web_url,
                    pr_number=pr_id,
                    branch_name=current_branch,
                    title=title or f"Update from branch: {current_branch}",
                    description=description or "",
                    pr_status="open",
                )

                return {"success": True, "pr_number": pr_id, "pr_url": web_url}

            elif provider == "gitlab":
                # GitLab API
                repo_url = cls.get_api_url()
                repo_response = cls._make_request("GET", repo_url)
                default_branch = repo_response.json().get("default_branch", "main")

                # Create PR (called Merge Request in GitLab)
                mr_url = cls.get_api_url("/merge_requests")

                mr_data = {
                    "source_branch": current_branch,
                    "target_branch": base or default_branch,
                    "title": title or f"Update from branch: {current_branch}",
                    "description": description
                    or f"Changes from branch: {current_branch}",
                }

                response = cls._make_request("POST", mr_url, json=mr_data)
                result = response.json()

                mr_id = result.get("iid")  # GitLab uses 'iid' for user-facing IDs
                web_url = result.get("web_url")

                # Create a record of the PR
                GitHubPR.objects.create(
                    recipe_id="multiple",  # Generic ID for multi-resource PRs
                    pr_url=web_url,
                    pr_number=mr_id,
                    branch_name=current_branch,
                    title=title or f"Update from branch: {current_branch}",
                    description=description or "",
                    pr_status="open",
                )

                return {"success": True, "pr_number": mr_id, "pr_url": web_url}

            elif provider == "bitbucket":
                # Bitbucket API differs between Cloud and Server
                if "bitbucket.org" in cls.get_api_url():
                    # Bitbucket Cloud
                    # Get default branch
                    repo_url = cls.get_api_url()
                    repo_response = cls._make_request("GET", repo_url)
                    default_branch = (
                        repo_response.json().get("mainbranch", {}).get("name", "main")
                    )

                    # Create PR
                    pr_url = cls.get_api_url("/pullrequests")

                    pr_data = {
                        "title": title or f"Update from branch: {current_branch}",
                        "description": description
                        or f"Changes from branch: {current_branch}",
                        "source": {"branch": {"name": current_branch}},
                        "destination": {"branch": {"name": base or default_branch}},
                        "close_source_branch": False,
                    }

                    response = cls._make_request("POST", pr_url, json=pr_data)
                    result = response.json()

                    pr_id = result.get("id")
                    web_url = result.get("links", {}).get("html", {}).get("href")

                else:
                    # Bitbucket Server
                    # Get default branch
                    branches_url = cls.get_api_url("/branches/default")
                    branches_response = cls._make_request("GET", branches_url)
                    default_branch = branches_response.json().get("displayId", "main")

                    # Create PR
                    pr_url = cls.get_api_url("/pull-requests")

                    pr_data = {
                        "title": title or f"Update from branch: {current_branch}",
                        "description": description
                        or f"Changes from branch: {current_branch}",
                        "fromRef": {"id": f"refs/heads/{current_branch}"},
                        "toRef": {"id": f"refs/heads/{base or default_branch}"},
                    }

                    response = cls._make_request("POST", pr_url, json=pr_data)
                    result = response.json()

                    pr_id = result.get("id")
                    web_url = result.get("links", {}).get("self", [{}])[0].get("href")

                # Create a record of the PR (common for both Cloud and Server)
                GitHubPR.objects.create(
                    recipe_id="multiple",  # Generic ID for multi-resource PRs
                    pr_url=web_url,
                    pr_number=pr_id,
                    branch_name=current_branch,
                    title=title or f"Update from branch: {current_branch}",
                    description=description or "",
                    pr_status="open",
                )

                return {"success": True, "pr_number": pr_id, "pr_url": web_url}

            else:
                logger.error(f"Unsupported Git provider for PR creation: {provider}")
                return None

        except Exception as e:
            error_msg = f"Error creating PR: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None

    @classmethod
    def push_to_git(cls, instance_or_template, commit_message=None):
        """Stage changes to Git and create related GitHub secrets for environment variables."""
        if not cls.is_configured():
            logger.error("Git integration not configured")
            return {"success": False, "error": "Git integration not configured"}

        # Only stage changes, don't create PR
        result = cls.stage_changes(instance_or_template, commit_message)
        if not result:
            return {"success": False, "error": "Failed to stage changes"}

        # Create GitHub secrets for environment variables instance
        if (
            isinstance(result, dict)
            and result.get("success")
            and isinstance(instance_or_template, EnvVarsInstance)
        ):
            try:
                from web_ui.services.github_service import GitHubService

                github_service = GitHubService()

                # Get environment name
                environment = None
                if instance_or_template.environment:
                    environment = instance_or_template.environment.name

                # Process secrets
                variables = instance_or_template.get_variables_dict()
                secret_result = github_service.create_secrets_from_env_vars(
                    variables, environment
                )

                # Log results
                if secret_result.get("created", 0) > 0:
                    logger.info(f"Created {secret_result['created']} GitHub secrets")

                if secret_result.get("failed", 0) > 0:
                    logger.warning(
                        f"Failed to create {secret_result['failed']} GitHub secrets"
                    )

            except Exception as e:
                logger.error(f"Error creating GitHub secrets: {str(e)}")
                # Don't fail the whole operation if secret creation fails

        # If result is already a dictionary with success field, return as is
        if isinstance(result, dict) and "success" in result:
            return result

        # Determine file path for different types of objects
        file_path = ""
        settings = GitSettings.get_instance()
        current_branch = settings.current_branch or "main"

        if isinstance(instance_or_template, EnvVarsInstance):
            # Get environment name (default to 'dev' if none)
            env_name = "dev"
            if instance_or_template.environment:
                env_name = instance_or_template.environment.name.lower()

            # Construct file path for EnvVarsInstance - use recipes/instances directory
            # and use the recipe type (e.g., mysql.yml) as the file name
            file_name = f"{instance_or_template.recipe_type.lower()}.yml"
            file_path = f"recipes/instances/{env_name}/{file_name}"

        elif isinstance(instance_or_template, RecipeTemplate):
            # Construct file path for RecipeTemplate
            recipe_type = instance_or_template.recipe_type.lower()
            file_path = f"recipes/templates/{recipe_type}.yml"

        elif isinstance(instance_or_template, RecipeInstance):
            # Get environment name (default to 'dev' if none)
            env_name = "dev"
            if instance_or_template.environment:
                env_name = instance_or_template.environment.name.lower()

            # Construct file path for RecipeInstance linking file
            instance_name = instance_or_template.name.replace(" ", "-").lower()
            file_path = f"params/instances/{env_name}/{instance_name}.yml"

        elif isinstance(instance_or_template, EnvVarsTemplate):
            # Get environment name (default to 'dev' if not specified)
            env_name = "dev"

            # Construct file path for EnvVarsTemplate
            template_name = instance_or_template.recipe_type.lower()
            file_path = f"recipes/templates/{template_name}.yml"

        elif isinstance(instance_or_template, Policy):
            # Get environment (default to 'prod' if not specified)
            if instance_or_template.environment:
                environment = instance_or_template.environment.name.lower()
            else:
                environment = Environment.get_default().name.lower()

            # Construct file path for Policy
            policy_name = instance_or_template.name.replace(" ", "_").lower()
            file_path = f"policies/{environment}/{policy_name}.json"

        # Handle metadata tests (identified by having id, name, definition, and environment attributes)
        elif (
            hasattr(instance_or_template, "id")
            and hasattr(instance_or_template, "name")
            and hasattr(instance_or_template, "definition")
            and hasattr(instance_or_template, "environment")
        ):
            # Get environment (default to 'prod' if not specified)
            if instance_or_template.environment:
                environment = instance_or_template.environment.name.lower()
            else:
                environment = Environment.get_default().name.lower()

            # Generate a safe file name from the test name
            import re

            safe_name = re.sub(
                r"[^a-zA-Z0-9_-]", "_", instance_or_template.name.lower()
            )

            # Construct file path for Metadata Test
            file_path = f"metadata_tests/{environment}/{safe_name}.yaml"

        # Otherwise, wrap the result in a success response
        return {"success": True, "branch": current_branch, "file_path": file_path}

    @classmethod
    def revert_staged_file(cls, file_path, branch=None):
        """
        Revert/delete a staged file from the repository

        Args:
            file_path: Path to the file to revert
            branch: Optional branch name (uses current branch if not specified)

        Returns:
            True if successful, False otherwise
        """
        settings = GitSettings.get_instance()
        if not settings or not cls.is_configured():
            return False

        from web_ui.services.git_service import GitService

        # Initialize Git service
        git_service = GitService()

        # Use branch from settings if not specified
        if not branch:
            branch = settings.current_branch or "main"

        # Revert the file
        return git_service.revert_staged_file(file_path, branch)

    @classmethod
    def create_branch(cls, branch_name, base_branch="main"):
        """Create a new branch in Git repository."""
        if not cls.is_configured():
            logger.error("Git integration not configured")
            return False

        try:
            # Get settings
            settings = GitSettings.get_instance()
            provider = settings.provider_type

            # First, get the reference to the base branch
            if provider == "github":
                # GitHub API to get base branch reference
                base_ref_url = cls.get_api_url(f"/git/refs/heads/{base_branch}")
                response = cls._make_request("GET", base_ref_url)
                base_ref = response.json()
                base_sha = base_ref.get("object", {}).get("sha")

                if not base_sha:
                    logger.error(f"Could not find SHA for base branch {base_branch}")
                    return False

                # Create new reference
                create_url = cls.get_api_url("/git/refs")
                data = {"ref": f"refs/heads/{branch_name}", "sha": base_sha}

                response = cls._make_request("POST", create_url, json=data)
                return response.status_code in [201, 200]

            elif provider == "azure_devops":
                # For Azure DevOps, we'd use a different API
                # Get repository ID first
                repo_url = cls.get_api_url()
                repo_response = cls._make_request("GET", repo_url)
                repo_id = repo_response.json().get("id")

                if not repo_id:
                    logger.error("Could not find repository ID")
                    return False

                # Get the base branch information
                base_branch_url = cls.get_api_url(f"/refs/heads/{base_branch}")
                if "?" in base_branch_url:
                    base_branch_url += "&api-version=6.0"
                else:
                    base_branch_url += "?api-version=6.0"

                response = cls._make_request("GET", base_branch_url)
                base_ref = response.json().get("value", [])

                if not base_ref or len(base_ref) == 0:
                    logger.error(f"Could not find base branch {base_branch}")
                    return False

                base_sha = base_ref[0].get("objectId")

                # Create new branch
                create_url = cls.get_api_url("/refs")
                if "?" in create_url:
                    create_url += "&api-version=6.0"
                else:
                    create_url += "?api-version=6.0"

                data = {
                    "name": f"refs/heads/{branch_name}",
                    "newObjectId": base_sha,
                    "oldObjectId": "0000000000000000000000000000000000000000",
                }

                response = cls._make_request("POST", create_url, json=data)
                return response.status_code in [201, 200]

            elif provider == "gitlab":
                # GitLab API
                create_url = cls.get_api_url("/repository/branches")
                data = {"branch": branch_name, "ref": base_branch}

                response = cls._make_request("POST", create_url, json=data)
                return response.status_code in [201, 200]

            elif provider == "bitbucket":
                # Bitbucket API
                if "bitbucket.org" in cls.get_api_url():
                    # Bitbucket Cloud
                    create_url = cls.get_api_url("/refs/branches")
                    data = {"name": branch_name, "target": {"hash": base_branch}}
                else:
                    # Bitbucket Server
                    create_url = cls.get_api_url("/branches")
                    data = {"name": branch_name, "startPoint": base_branch}

                response = cls._make_request("POST", create_url, json=data)
                return response.status_code in [201, 200]

            else:
                logger.error(f"Unsupported Git provider: {provider}")
                return False

        except Exception as e:
            logger.error(f"Error creating branch: {str(e)}", exc_info=True)
            return False


class Policy(models.Model):
    """Model for storing DataHub policies."""

    id = models.CharField(max_length=255, primary_key=True)
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    type = models.CharField(
        max_length=50, choices=[("METADATA", "Metadata"), ("PLATFORM", "Platform")]
    )
    state = models.CharField(
        max_length=50, choices=[("ACTIVE", "Active"), ("INACTIVE", "Inactive")]
    )
    resources = models.TextField()
    privileges = models.TextField()
    actors = models.TextField()
    environment = models.ForeignKey(
        Environment,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="policies",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name_plural = "Policies"

    def __str__(self):
        return self.name

    @property
    def resources_json(self):
        return self.resources if self.resources else "[]"

    @property
    def privileges_json(self):
        return self.privileges if self.privileges else "[]"

    @property
    def actors_json(self):
        return self.actors if self.actors else "{}"

    def to_dict(self):
        """Convert policy to a dictionary suitable for JSON/YAML export."""
        try:
            # Safely parse resources JSON, using empty list as fallback
            try:
                resources = (
                    json.loads(self.resources)
                    if self.resources and self.resources.strip()
                    else []
                )
            except (json.JSONDecodeError, TypeError):
                resources = []

            # Safely parse privileges JSON, using empty list as fallback
            try:
                privileges = (
                    json.loads(self.privileges)
                    if self.privileges and self.privileges.strip()
                    else []
                )
            except (json.JSONDecodeError, TypeError):
                privileges = []

            # Safely parse actors JSON, using empty dict as fallback
            try:
                actors = (
                    json.loads(self.actors)
                    if self.actors and self.actors.strip()
                    else {}
                )
            except (json.JSONDecodeError, TypeError):
                actors = {}

            return {
                "policy": {
                    "id": self.id,
                    "name": self.name,
                    "description": self.description or "",
                    "type": self.type,
                    "state": self.state,
                    "resources": resources,
                    "privileges": privileges,
                    "actors": actors,
                },
                "metadata": {
                    "exported_at": datetime.now().isoformat(),
                    "exported_by": "datahub_recipes_manager",
                },
            }
        except Exception as e:
            logger.error(f"Error converting policy to dict: {str(e)}")
            # Return a minimal valid structure if there's an error
            return {
                "policy": {
                    "id": self.id,
                    "name": self.name,
                    "description": self.description or "",
                    "type": self.type,
                    "state": self.state,
                    "resources": [],
                    "privileges": [],
                    "actors": {},
                },
                "metadata": {
                    "exported_at": datetime.now().isoformat(),
                    "exported_by": "datahub_recipes_manager",
                    "error": "Error parsing some fields",
                },
            }

    def to_yaml(self, path=None):
        """
        Export policy to YAML format.

        Args:
            path: Optional path to save the YAML file

        Returns:
            Path to the saved file or the YAML string if path is None
        """
        import yaml

        data = self.to_dict()
        yaml_content = yaml.dump(data, default_flow_style=False, sort_keys=False)

        if path:
            # Ensure directory exists
            os.makedirs(os.path.dirname(path), exist_ok=True)

            # Write to file
            with open(path, "w") as f:
                f.write(yaml_content)
            return path

        return yaml_content


class EnvironmentInstance(models.Model):
    """Model representing a set of environment variables for a specific deployment"""

    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    template = models.ForeignKey(
        EnvVarsTemplate, on_delete=models.PROTECT, related_name="instances"
    )
    recipe_type = models.CharField(
        max_length=50,
        choices=(
            ("postgres", "PostgreSQL"),
            ("mysql", "MySQL"),
            ("mssql", "Microsoft SQL Server"),
            ("snowflake", "Snowflake"),
            ("bigquery", "BigQuery"),
            ("redshift", "Redshift"),
            ("databricks", "Databricks"),
        ),
    )
    tenant = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    def to_dict(self):
        """Convert environment instance to a dictionary suitable for JSON/YAML export."""
        env_vars = {}
        for var in self.variables.all():
            env_vars[var.key] = {
                "value": var.value,
                "description": var.description or "",
                "is_secret": var.is_secret,
                "is_required": var.is_required,
            }

        return {
            "environment_instance": {
                "name": self.name,
                "description": self.description or "",
                "recipe_type": self.recipe_type,
                "template": self.template.name,
                "tenant": self.tenant or "",
                "variables": env_vars,
            },
            "metadata": {
                "exported_at": datetime.now().isoformat(),
                "exported_by": "datahub_recipes_manager",
            },
        }

    def to_yaml(self, path=None):
        """
        Export environment instance to YAML format.

        Args:
            path: Optional path to save the YAML file

        Returns:
            Path to the saved file or the YAML string if path is None
        """
        import yaml

        data = self.to_dict()
        yaml_content = yaml.dump(data, default_flow_style=False, sort_keys=False)

        if path:
            # Ensure directory exists
            os.makedirs(os.path.dirname(path), exist_ok=True)

            # Write to file
            with open(path, "w") as f:
                f.write(yaml_content)
            return path

        return yaml_content


class GitSecrets(models.Model):
    """Model for storing GitHub repository secrets."""

    name = models.CharField(max_length=255)
    environment = models.CharField(max_length=255, blank=True, default="")
    description = models.TextField(blank=True, null=True)
    # We don't store the actual value for security reasons
    is_configured = models.BooleanField(default=True)
    last_checked = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "GitHub Secret"
        verbose_name_plural = "GitHub Secrets"
        ordering = ["environment", "name"]
        unique_together = ("name", "environment")

    def __str__(self):
        if self.environment:
            return f"{self.name} ({self.environment})"
        return self.name
