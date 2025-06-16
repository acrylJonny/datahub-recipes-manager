from django import forms
import json

from .models import (
    RecipeTemplate,
    EnvVarsTemplate,
    EnvVarsInstance,
    GitSettings,
    Environment,
)

# Define friendly names for common recipe types
FRIENDLY_NAMES = {
    "bigquery": "Google BigQuery",
    "mysql": "MySQL",
    "postgres": "PostgreSQL",
    "mssql": "Microsoft SQL Server",
    "snowflake": "Snowflake",
    "redshift": "Amazon Redshift",
    "kafka": "Apache Kafka",
    "hive": "Apache Hive",
    "glue": "AWS Glue",
    "s3": "Amazon S3",
    "athena": "Amazon Athena",
    "looker": "Looker",
    "tableau": "Tableau",
    "powerbi": "Microsoft Power BI",
    "dbt": "dbt",
    "airflow": "Apache Airflow",
    "elasticsearch": "Elasticsearch",
    "mongodb": "MongoDB",
    "neo4j": "Neo4j",
    "oracle": "Oracle Database",
    "databricks": "Databricks",
    "clickhouse": "ClickHouse",
    "druid": "Apache Druid",
    "superset": "Apache Superset",
    "trino": "Trino",
    "presto": "Presto SQL",
    "dynamodb": "Amazon DynamoDB",
    "metabase": "Metabase",
    "nifi": "Apache NiFi",
    "pulsar": "Apache Pulsar",
    "cassandra": "Apache Cassandra",
    "delta_lake": "Delta Lake",
    "feast": "Feast",
    "fivetran": "Fivetran",
    "dremio": "Dremio",
    "iceberg": "Apache Iceberg",
    "datahub": "DataHub",
    "ldap": "LDAP",
    "okta": "Okta",
    "mode": "Mode Analytics",
    "azure_ad": "Azure Active Directory",
    "salesforce": "Salesforce",
    "dagster": "Dagster",
    "prefect": "Prefect",
    "sagemaker": "AWS SageMaker",
    "mlflow": "MLflow",
}


class RecipeForm(forms.Form):
    """Form for creating or editing a recipe."""

    recipe_id = forms.CharField(
        label="Recipe ID",
        max_length=255,
        required=True,
        widget=forms.TextInput(attrs={"class": "form-control"}),
    )
    recipe_name = forms.CharField(
        label="Recipe Name",
        max_length=255,
        required=True,
        widget=forms.TextInput(attrs={"class": "form-control"}),
    )
    recipe_type = forms.CharField(
        label="Recipe Type",
        max_length=50,
        required=True,
        widget=forms.TextInput(attrs={"class": "form-control"}),
    )
    description = forms.CharField(
        label="Description",
        required=False,
        widget=forms.Textarea(attrs={"class": "form-control", "rows": 3}),
    )
    schedule_cron = forms.CharField(
        label="Schedule (Cron Expression)",
        max_length=100,
        required=False,
        widget=forms.TextInput(attrs={"class": "form-control"}),
    )
    schedule_timezone = forms.CharField(
        label="Timezone",
        max_length=50,
        required=False,
        widget=forms.TextInput(attrs={"class": "form-control", "placeholder": "UTC"}),
    )
    recipe_content = forms.CharField(
        label="Recipe Content (YAML/JSON)",
        required=True,
        widget=forms.Textarea(attrs={"class": "form-control", "rows": 15}),
    )
    replace_env_vars = forms.BooleanField(
        label="Replace environment variables in recipe",
        required=False,
        initial=False,
        widget=forms.CheckboxInput(attrs={"class": "form-check-input"}),
        help_text="If checked, environment variable placeholders will be replaced with their values in the recipe.",
    )


class RecipeImportForm(forms.Form):
    """Form for importing a recipe from a file."""

    recipe_file = forms.FileField(
        label="Recipe File (YAML/JSON)",
        required=True,
        widget=forms.FileInput(attrs={"class": "form-control"}),
    )


class PolicyForm(forms.Form):
    """Form for creating or editing a policy."""

    policy_id = forms.CharField(required=False, max_length=255)
    policy_name = forms.CharField(required=True, max_length=255)
    policy_type = forms.ChoiceField(
        required=True, choices=[("METADATA", "Metadata"), ("PLATFORM", "Platform")]
    )
    policy_state = forms.ChoiceField(
        required=True, choices=[("ACTIVE", "Active"), ("INACTIVE", "Inactive")]
    )
    description = forms.CharField(required=False, widget=forms.Textarea)
    policy_resources = forms.CharField(required=True, widget=forms.Textarea)
    policy_privileges = forms.CharField(required=True, widget=forms.Textarea)
    policy_actors = forms.CharField(required=True, widget=forms.Textarea)

    def clean_policy_resources(self):
        """Validate the policy resources field."""
        data = self.cleaned_data["policy_resources"]
        try:
            resources = json.loads(data)
            if not isinstance(resources, list):
                raise forms.ValidationError("Resources must be a JSON array")
            return json.dumps(resources)
        except json.JSONDecodeError:
            raise forms.ValidationError("Invalid JSON format for resources")

    def clean_policy_privileges(self):
        """Validate the policy privileges field."""
        data = self.cleaned_data["policy_privileges"]
        try:
            privileges = json.loads(data)
            if not isinstance(privileges, list):
                raise forms.ValidationError("Privileges must be a JSON array")
            return json.dumps(privileges)
        except json.JSONDecodeError:
            raise forms.ValidationError("Invalid JSON format for privileges")

    def clean_policy_actors(self):
        """Validate the policy actors field."""
        data = self.cleaned_data["policy_actors"]
        try:
            actors = json.loads(data)
            # Actors can be an array or an object depending on the policy
            return json.dumps(actors)
        except json.JSONDecodeError:
            raise forms.ValidationError("Invalid JSON format for actors")


class PolicyImportForm(forms.Form):
    """Form for importing a policy from a file."""

    policy_file = forms.FileField(
        label="Policy File (JSON)",
        required=True,
        widget=forms.FileInput(attrs={"class": "form-control"}),
    )


class RecipeTemplateForm(forms.Form):
    """Form for creating or editing a recipe template."""

    name = forms.CharField(
        label="Template Name",
        max_length=255,
        required=True,
        widget=forms.TextInput(attrs={"class": "form-control"}),
    )
    description = forms.CharField(
        label="Description",
        required=False,
        widget=forms.Textarea(attrs={"class": "form-control", "rows": 3}),
    )
    recipe_type = forms.ChoiceField(
        label="Recipe Type",
        required=True,
        widget=forms.Select(attrs={"class": "form-select select2-enable"}),
    )
    recipe_type_other = forms.CharField(
        label="Custom Recipe Type",
        required=False,
        widget=forms.TextInput(
            attrs={
                "class": "form-control mt-2",
                "id": "id_recipe_type_other",
                "placeholder": "Enter custom recipe type",
                "style": "display:none;",
            }
        ),
    )
    tags = forms.CharField(
        label="Tags",
        max_length=255,
        required=False,
        widget=forms.TextInput(
            attrs={"class": "form-control", "placeholder": "comma,separated,tags"}
        ),
        help_text="Enter comma-separated tags to categorize this template",
    )
    content = forms.CharField(
        label="Recipe Content (YAML/JSON)",
        required=True,
        widget=forms.Textarea(attrs={"class": "form-control", "rows": 15}),
    )
    executor_id = forms.CharField(
        label="Executor ID",
        max_length=255,
        required=False,
        initial="default",
        widget=forms.TextInput(
            attrs={"class": "form-control", "placeholder": "default"}
        ),
        help_text="The executor ID to use when deploying this recipe to DataHub",
    )
    cron_schedule = forms.CharField(
        label="Schedule (Cron Expression)",
        max_length=100,
        required=False,
        initial="0 0 * * *",
        widget=forms.TextInput(
            attrs={"class": "form-control", "placeholder": "0 0 * * *"}
        ),
        help_text="Default: Daily at midnight (0 0 * * *)",
    )
    timezone = forms.ChoiceField(
        label="Timezone",
        required=False,
        initial="Etc/UTC",
        widget=forms.Select(attrs={"class": "form-select select2-enable"}),
        choices=[
            ("Pacific/Midway", "Pacific/Midway"),
            ("Pacific/Pago_Pago", "Pacific/Pago_Pago"),
            ("Pacific/Honolulu", "Pacific/Honolulu"),
            ("America/Juneau", "America/Juneau"),
            ("America/Los_Angeles", "America/Los_Angeles"),
            ("America/Tijuana", "America/Tijuana"),
            ("America/Denver", "America/Denver"),
            ("America/Phoenix", "America/Phoenix"),
            ("America/Chihuahua", "America/Chihuahua"),
            ("America/Mazatlan", "America/Mazatlan"),
            ("America/Chicago", "America/Chicago"),
            ("America/Regina", "America/Regina"),
            ("America/Mexico_City", "America/Mexico_City"),
            ("America/Monterrey", "America/Monterrey"),
            ("America/Guatemala", "America/Guatemala"),
            ("America/New_York", "America/New_York"),
            ("America/Indiana/Indianapolis", "America/Indiana/Indianapolis"),
            ("America/Bogota", "America/Bogota"),
            ("America/Lima", "America/Lima"),
            ("America/Halifax", "America/Halifax"),
            ("America/Caracas", "America/Caracas"),
            ("America/La_Paz", "America/La_Paz"),
            ("America/Santiago", "America/Santiago"),
            ("America/St_Johns", "America/St_Johns"),
            ("America/Sao_Paulo", "America/Sao_Paulo"),
            ("America/Argentina/Buenos_Aires", "America/Argentina/Buenos_Aires"),
            ("America/Guyana", "America/Guyana"),
            ("America/Godthab", "America/Godthab"),
            ("Atlantic/South_Georgia", "Atlantic/South_Georgia"),
            ("Atlantic/Azores", "Atlantic/Azores"),
            ("Atlantic/Cape_Verde", "Atlantic/Cape_Verde"),
            ("Europe/Dublin", "Europe/Dublin"),
            ("Europe/London", "Europe/London"),
            ("Europe/Lisbon", "Europe/Lisbon"),
            ("Africa/Casablanca", "Africa/Casablanca"),
            ("Africa/Monrovia", "Africa/Monrovia"),
            ("Etc/UTC", "Etc/UTC"),
            ("Europe/Belgrade", "Europe/Belgrade"),
            ("Europe/Bratislava", "Europe/Bratislava"),
            ("Europe/Budapest", "Europe/Budapest"),
            ("Europe/Ljubljana", "Europe/Ljubljana"),
            ("Europe/Prague", "Europe/Prague"),
            ("Europe/Sarajevo", "Europe/Sarajevo"),
            ("Europe/Skopje", "Europe/Skopje"),
            ("Europe/Warsaw", "Europe/Warsaw"),
            ("Europe/Zagreb", "Europe/Zagreb"),
            ("Europe/Brussels", "Europe/Brussels"),
            ("Europe/Copenhagen", "Europe/Copenhagen"),
            ("Europe/Madrid", "Europe/Madrid"),
            ("Europe/Paris", "Europe/Paris"),
            ("Europe/Amsterdam", "Europe/Amsterdam"),
            ("Europe/Berlin", "Europe/Berlin"),
            ("Europe/Rome", "Europe/Rome"),
            ("Europe/Stockholm", "Europe/Stockholm"),
            ("Europe/Vienna", "Europe/Vienna"),
            ("Africa/Algiers", "Africa/Algiers"),
            ("Europe/Bucharest", "Europe/Bucharest"),
            ("Africa/Cairo", "Africa/Cairo"),
            ("Europe/Helsinki", "Europe/Helsinki"),
            ("Europe/Kiev", "Europe/Kiev"),
            ("Europe/Riga", "Europe/Riga"),
            ("Europe/Sofia", "Europe/Sofia"),
            ("Europe/Tallinn", "Europe/Tallinn"),
            ("Europe/Vilnius", "Europe/Vilnius"),
            ("Europe/Athens", "Europe/Athens"),
            ("Europe/Istanbul", "Europe/Istanbul"),
            ("Europe/Minsk", "Europe/Minsk"),
            ("Asia/Jerusalem", "Asia/Jerusalem"),
            ("Africa/Harare", "Africa/Johannesburg"),
            ("Europe/Moscow", "Europe/Moscow"),
            ("Asia/Kuwait", "Asia/Kuwait"),
            ("Asia/Riyadh", "Asia/Riyadh"),
            ("Africa/Nairobi", "Africa/Nairobi"),
            ("Asia/Baghdad", "Asia/Baghdad"),
            ("Asia/Tehran", "Asia/Tehran"),
            ("Asia/Muscat", "Asia/Muscat"),
            ("Asia/Baku", "Asia/Baku"),
            ("Asia/Tbilisi", "Asia/Tbilisi"),
            ("Asia/Yerevan", "Asia/Yerevan"),
            ("Asia/Kabul", "Asia/Kabul"),
            ("Asia/Yekaterinburg", "Asia/Yekaterinburg"),
            ("Asia/Karachi", "Asia/Karachi"),
            ("Asia/Tashkent", "Asia/Tashkent"),
            ("Asia/Kolkata", "Asia/Kolkata"),
            ("Asia/Kathmandu", "Asia/Kathmandu"),
            ("Asia/Dhaka", "Asia/Dhaka"),
            ("Asia/Colombo", "Asia/Colombo"),
            ("Asia/Almaty", "Asia/Almaty"),
            ("Asia/Novosibirsk", "Asia/Novosibirsk"),
            ("Asia/Rangoon", "Asia/Rangoon"),
            ("Asia/Bangkok", "Asia/Bangkok"),
            ("Asia/Jakarta", "Asia/Jakarta"),
            ("Asia/Krasnoyarsk", "Asia/Krasnoyarsk"),
            ("Asia/Shanghai", "Asia/Shanghai"),
            ("Asia/Chongqing", "Asia/Chongqing"),
            ("Asia/Hong_Kong", "Asia/Hong_Kong"),
            ("Asia/Urumqi", "Asia/Urumqi"),
            ("Asia/Kuala_Lumpur", "Asia/Kuala_Lumpur"),
            ("Asia/Singapore", "Asia/Singapore"),
            ("Asia/Taipei", "Asia/Taipei"),
            ("Australia/Perth", "Australia/Perth"),
            ("Asia/Irkutsk", "Asia/Irkutsk"),
            ("Asia/Ulaanbaatar", "Asia/Ulaanbaatar"),
            ("Asia/Seoul", "Asia/Seoul"),
            ("Asia/Tokyo", "Asia/Tokyo"),
            ("Asia/Yakutsk", "Asia/Yakutsk"),
            ("Australia/Darwin", "Australia/Darwin"),
            ("Australia/Adelaide", "Australia/Adelaide"),
            ("Australia/Melbourne", "Australia/Melbourne"),
            ("Australia/Sydney", "Australia/Sydney"),
            ("Australia/Brisbane", "Australia/Brisbane"),
            ("Australia/Hobart", "Australia/Hobart"),
            ("Asia/Vladivostok", "Asia/Vladivostok"),
            ("Pacific/Guam", "Pacific/Guam"),
            ("Pacific/Port_Moresby", "Pacific/Port_Moresby"),
            ("Asia/Magadan", "Asia/Magadan"),
            ("Pacific/Noumea", "Pacific/Noumea"),
            ("Pacific/Fiji", "Pacific/Fiji"),
            ("Asia/Kamchatka", "Asia/Kamchatka"),
            ("Pacific/Majuro", "Pacific/Majuro"),
            ("Pacific/Auckland", "Pacific/Auckland"),
            ("Pacific/Tongatapu", "Pacific/Tongatapu"),
            ("Pacific/Fakaofo", "Pacific/Fakaofo"),
            ("Pacific/Apia", "Pacific/Apia"),
        ],
        help_text="Select the timezone for the cron schedule",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Get source types from directories
        source_types = []
        try:
            import os
            import sys

            # Look for datahub ingestion source directories
            datahub_path = None
            for path in sys.path:
                if os.path.exists(os.path.join(path, "datahub", "ingestion", "source")):
                    datahub_path = os.path.join(path, "datahub", "ingestion", "source")
                    break

            if datahub_path:
                # Get list of directories in source folder that aren't private
                for item in os.listdir(datahub_path):
                    if not item.startswith("_") and os.path.isdir(
                        os.path.join(datahub_path, item)
                    ):
                        # Use friendly name if available, otherwise convert to title case
                        if item in FRIENDLY_NAMES:
                            display_name = FRIENDLY_NAMES[item]
                        else:
                            display_name = " ".join(
                                word.capitalize() for word in item.split("_")
                            )
                        source_types.append((item, display_name))
        except Exception:
            # Fallback to basic list if there's an error
            pass

        # Add existing RECIPE_TYPES from models.py
        from .models import RECIPE_TYPES

        # Combine and sort by display name
        all_types = set(source_types + list(RECIPE_TYPES))
        all_types = sorted(all_types, key=lambda x: x[1].lower())

        # Ensure 'other' is included
        if not any(t[0] == "other" for t in all_types):
            all_types.append(("other", "Other"))

        # Update choices for recipe_type
        self.fields["recipe_type"].choices = all_types

    def clean(self):
        cleaned_data = super().clean()
        recipe_type = cleaned_data.get("recipe_type")
        recipe_type_other = cleaned_data.get("recipe_type_other")

        # If 'other' is selected, use the custom type value
        if recipe_type == "other" and recipe_type_other:
            cleaned_data["recipe_type"] = recipe_type_other

        return cleaned_data


class RecipeTemplateImportForm(forms.Form):
    """Form for importing a recipe template from a file."""

    template_file = forms.FileField(
        label="Recipe Template File (YAML/JSON)",
        required=True,
        widget=forms.FileInput(attrs={"class": "form-control"}),
    )
    tags = forms.CharField(
        label="Tags",
        max_length=255,
        required=False,
        widget=forms.TextInput(
            attrs={"class": "form-control", "placeholder": "comma,separated,tags"}
        ),
        help_text="Enter comma-separated tags to categorize this template",
    )


class RecipeDeployForm(forms.Form):
    """Form for deploying a recipe template to DataHub."""

    recipe_id = forms.CharField(
        label="Recipe ID",
        max_length=255,
        required=True,
        widget=forms.TextInput(attrs={"class": "form-control"}),
    )
    recipe_name = forms.CharField(
        label="Recipe Name",
        max_length=255,
        required=True,
        widget=forms.TextInput(attrs={"class": "form-control"}),
    )
    schedule_cron = forms.CharField(
        label="Schedule (Cron Expression)",
        max_length=100,
        required=False,
        widget=forms.TextInput(
            attrs={"class": "form-control", "placeholder": "0 0 * * *"}
        ),
    )
    schedule_timezone = forms.CharField(
        label="Timezone",
        max_length=50,
        required=False,
        widget=forms.TextInput(attrs={"class": "form-control", "placeholder": "UTC"}),
    )
    environment_variables = forms.CharField(
        label="Environment Variables",
        required=False,
        widget=forms.HiddenInput(attrs={"id": "env_vars_json"}),
    )
    description = forms.CharField(
        label="Description",
        required=False,
        widget=forms.Textarea(attrs={"class": "form-control", "rows": 3}),
    )


class EnvVarsTemplateForm(forms.ModelForm):
    """Form for creating or editing an environment variables template."""

    variables = forms.CharField(
        label="Environment Variables",
        required=True,
        widget=forms.HiddenInput(attrs={"id": "env_vars_template_json"}),
    )
    recipe_type = forms.ChoiceField(
        label="Recipe Type",
        required=True,
        widget=forms.Select(attrs={"class": "form-select select2-enable"}),
    )
    recipe_type_other = forms.CharField(
        label="Custom Recipe Type",
        required=False,
        widget=forms.TextInput(
            attrs={
                "class": "form-control mt-2",
                "id": "id_recipe_type_other",
                "placeholder": "Enter custom recipe type",
                "style": "display:none;",
            }
        ),
    )
    tags = forms.CharField(
        label="Tags",
        max_length=255,
        required=False,
        widget=forms.TextInput(
            attrs={"class": "form-control", "placeholder": "comma,separated,tags"}
        ),
        help_text="Enter comma-separated tags to categorize this template",
    )

    class Meta:
        model = EnvVarsTemplate
        fields = ["name", "description", "recipe_type", "tags", "variables"]
        widgets = {
            "name": forms.TextInput(attrs={"class": "form-control"}),
            "description": forms.Textarea(attrs={"class": "form-control", "rows": 3}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Get source types from directories
        source_types = []
        try:
            import os
            import sys

            # Look for datahub ingestion source directories
            datahub_path = None
            for path in sys.path:
                if os.path.exists(os.path.join(path, "datahub", "ingestion", "source")):
                    datahub_path = os.path.join(path, "datahub", "ingestion", "source")
                    break

            if datahub_path:
                # Get list of directories in source folder that aren't private
                for item in os.listdir(datahub_path):
                    if not item.startswith("_") and os.path.isdir(
                        os.path.join(datahub_path, item)
                    ):
                        # Use friendly name if available, otherwise convert to title case
                        if item in FRIENDLY_NAMES:
                            display_name = FRIENDLY_NAMES[item]
                        else:
                            display_name = " ".join(
                                word.capitalize() for word in item.split("_")
                            )
                        source_types.append((item, display_name))
        except Exception:
            # Fallback to basic list if there's an error
            pass

        # Add existing RECIPE_TYPES from models.py
        from .models import RECIPE_TYPES

        # Combine and sort by display name
        all_types = set(source_types + list(RECIPE_TYPES))
        all_types = sorted(all_types, key=lambda x: x[1].lower())

        # Ensure 'other' is included
        if not any(t[0] == "other" for t in all_types):
            all_types.append(("other", "Other"))

        # Update choices for recipe_type
        self.fields["recipe_type"].choices = all_types

    def clean(self):
        cleaned_data = super().clean()
        recipe_type = cleaned_data.get("recipe_type")
        recipe_type_other = cleaned_data.get("recipe_type_other")

        # If 'other' is selected, use the custom type
        if recipe_type == "other" and recipe_type_other:
            cleaned_data["recipe_type"] = recipe_type_other

        return cleaned_data


class EnvVarsInstanceForm(forms.Form):
    """Form for creating or editing an environment variables instance."""

    name = forms.CharField(
        label="Instance Name",
        max_length=255,
        required=True,
        widget=forms.TextInput(attrs={"class": "form-control"}),
    )
    description = forms.CharField(
        label="Description",
        required=False,
        widget=forms.Textarea(attrs={"class": "form-control", "rows": 3}),
    )
    template = forms.ModelChoiceField(
        label="Template",
        required=False,
        queryset=None,  # Set in __init__
        widget=forms.Select(attrs={"class": "form-control"}),
        help_text="Select an optional template to base this instance on",
    )
    recipe_type = forms.ChoiceField(
        label="Recipe Type",
        required=True,
        widget=forms.Select(attrs={"class": "form-select select2-enable"}),
    )
    recipe_type_other = forms.CharField(
        label="Custom Recipe Type",
        required=False,
        widget=forms.TextInput(
            attrs={
                "class": "form-control mt-2",
                "id": "id_recipe_type_other",
                "placeholder": "Enter custom recipe type",
                "style": "display:none;",
            }
        ),
    )
    variables = forms.CharField(
        label="Environment Variables",
        required=True,
        widget=forms.HiddenInput(attrs={"id": "env_vars_instance_json"}),
    )
    environment = forms.ModelChoiceField(
        queryset=Environment.objects.all(),
        required=False,
        widget=forms.Select(attrs={"class": "form-select select2-enable"}),
    )

    def __init__(self, *args, **kwargs):
        from .models import EnvVarsTemplate

        super().__init__(*args, **kwargs)
        self.fields["template"].queryset = EnvVarsTemplate.objects.all().order_by(
            "name"
        )

        # Set initial environment to the default if it exists
        default_env = Environment.get_default()
        if default_env:
            self.fields["environment"].initial = default_env.id

        # Get source types from directories
        source_types = []
        try:
            import os
            import sys

            # Look for datahub ingestion source directories
            datahub_path = None
            for path in sys.path:
                if os.path.exists(os.path.join(path, "datahub", "ingestion", "source")):
                    datahub_path = os.path.join(path, "datahub", "ingestion", "source")
                    break

            if datahub_path:
                # Get list of directories in source folder that aren't private
                for item in os.listdir(datahub_path):
                    if not item.startswith("_") and os.path.isdir(
                        os.path.join(datahub_path, item)
                    ):
                        # Use friendly name if available, otherwise convert to title case
                        if item in FRIENDLY_NAMES:
                            display_name = FRIENDLY_NAMES[item]
                        else:
                            display_name = " ".join(
                                word.capitalize() for word in item.split("_")
                            )
                        source_types.append((item, display_name))
        except Exception:
            # Fallback to basic list if there's an error
            pass

        # Add existing RECIPE_TYPES from models.py
        from .models import RECIPE_TYPES

        # Combine and sort by display name
        all_types = set(source_types + list(RECIPE_TYPES))
        all_types = sorted(all_types, key=lambda x: x[1].lower())

        # Ensure 'other' is included
        if not any(t[0] == "other" for t in all_types):
            all_types.append(("other", "Other"))

        # Update choices for recipe_type
        self.fields["recipe_type"].choices = all_types

    def clean(self):
        cleaned_data = super().clean()
        recipe_type = cleaned_data.get("recipe_type")
        recipe_type_other = cleaned_data.get("recipe_type_other")

        # If 'other' is selected, use the custom type
        if recipe_type == "other" and recipe_type_other:
            cleaned_data["recipe_type"] = recipe_type_other

        return cleaned_data


class RecipeInstanceForm(forms.Form):
    """Form for creating or editing a recipe instance."""

    name = forms.CharField(
        label="Name",
        max_length=255,
        required=True,
        widget=forms.TextInput(attrs={"class": "form-control"}),
    )
    description = forms.CharField(
        label="Description",
        required=False,
        widget=forms.Textarea(attrs={"class": "form-control", "rows": 3}),
    )
    template = forms.ModelChoiceField(
        label="Recipe Template",
        queryset=RecipeTemplate.objects.all().order_by("name"),
        widget=forms.Select(attrs={"class": "form-select select2-enable"}),
    )
    env_vars_instance = forms.ModelChoiceField(
        label="Environment Variables Instance",
        required=False,
        queryset=EnvVarsInstance.objects.all().order_by("name"),
        widget=forms.Select(attrs={"class": "form-select select2-enable"}),
    )
    environment = forms.ModelChoiceField(
        queryset=Environment.objects.all(),
        required=False,
        widget=forms.Select(attrs={"class": "form-select select2-enable"}),
    )
    cron_schedule = forms.CharField(
        label="Cron Schedule",
        required=False,
        initial="0 0 * * *",
        widget=forms.TextInput(
            attrs={"class": "form-control", "placeholder": "0 0 * * *"}
        ),
    )
    timezone = forms.CharField(
        label="Timezone",
        required=False,
        initial="UTC",
        widget=forms.TextInput(attrs={"class": "form-control", "placeholder": "UTC"}),
    )
    debug_mode = forms.BooleanField(
        label="Debug Mode",
        required=False,
        initial=False,
        widget=forms.CheckboxInput(attrs={"class": "form-check-input"}),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Set initial environment to the default if it exists
        default_env = Environment.get_default()
        if default_env:
            self.fields["environment"].initial = default_env.id

        # Get initial template_id if it exists
        initial = kwargs.get("initial", {})
        template_id = initial.get("template", None)

        # Filter env_vars_instance choices based on template recipe_type if provided
        if template_id:
            try:
                template = RecipeTemplate.objects.get(id=template_id)
                recipe_type = template.recipe_type

                # Filter env vars instances by recipe type
                filtered_instances = EnvVarsInstance.objects.filter(
                    recipe_type=recipe_type
                ).order_by("name")
                self.fields["env_vars_instance"].queryset = filtered_instances

                # Add a note to help text
                self.fields[
                    "env_vars_instance"
                ].help_text = f"Showing only environment variable instances for {recipe_type} recipe type"
            except RecipeTemplate.DoesNotExist:
                pass


class GitSettingsForm(forms.ModelForm):
    """Form for Git integration settings."""

    class Meta:
        model = GitSettings
        fields = [
            "provider_type",
            "base_url",
            "username",
            "repository",
            "token",
            "enabled",
        ]
        widgets = {
            "token": forms.PasswordInput(render_value=True),
            "username": forms.TextInput(
                attrs={
                    "class": "form-control",
                    "placeholder": "Enter username or organization/project",
                }
            ),
            "repository": forms.TextInput(
                attrs={"class": "form-control", "placeholder": "Repository name"}
            ),
            "base_url": forms.URLInput(
                attrs={
                    "class": "form-control",
                    "placeholder": "https://api.github.com (GitHub), https://dev.azure.com (Azure DevOps), etc.",
                }
            ),
        }
        help_texts = {
            "token": "Personal Access Token with repository/code access permissions",
            "username": "For GitHub: username or organization. For Azure DevOps: organization/project",
            "repository": "The repository name without the full URL",
            "base_url": "Leave empty for GitHub.com. For Azure DevOps or self-hosted instances, enter the base API URL",
            "enabled": "Enable Git integration",
            "provider_type": "Select your Git provider",
        }
