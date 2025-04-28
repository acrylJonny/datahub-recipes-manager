from django.db import models
import json
from django.utils import timezone
import time
import re
import base64
import requests
import logging
from django.contrib.auth.models import User
import uuid
from pathlib import Path
import yaml

logger = logging.getLogger(__name__)

# Define recipe types
RECIPE_TYPES = [
    ('kafka', 'Kafka'),
    ('file', 'File'),
    ('s3', 'S3'),
    ('snowflake', 'Snowflake'),
    ('bigquery', 'BigQuery'),
    ('postgres', 'PostgreSQL'),
    ('mysql', 'MySQL'),
    ('mssql', 'Microsoft SQL Server'),
    ('oracle', 'Oracle'),
    ('other', 'Other')
]

class Settings(models.Model):
    """Settings model for storing application configuration."""
    key = models.CharField(max_length=255, unique=True)
    value = models.TextField(blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name_plural = "Settings"

    def __str__(self):
        return self.key

class LogEntry(models.Model):
    """Model for storing application logs."""
    LEVEL_CHOICES = (
        ('DEBUG', 'Debug'),
        ('INFO', 'Info'),
        ('WARNING', 'Warning'),
        ('ERROR', 'Error'),
        ('CRITICAL', 'Critical'),
    )
    
    timestamp = models.DateTimeField(default=timezone.now)
    level = models.CharField(max_length=10, choices=LEVEL_CHOICES, default='INFO')
    source = models.CharField(max_length=50, default='application')
    message = models.TextField()
    details = models.TextField(blank=True, null=True)
    
    class Meta:
        verbose_name_plural = "Log Entries"
        ordering = ['-timestamp']
        
    def __str__(self):
        return f"[{self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}] {self.level}: {self.message}"
    
    @classmethod
    def log(cls, level, message, source='application', details=None):
        """Create a new log entry."""
        return cls.objects.create(
            level=level,
            message=message,
            source=source,
            details=details
        )
    
    @classmethod
    def debug(cls, message, source='application', details=None):
        return cls.log('DEBUG', message, source, details)
    
    @classmethod
    def info(cls, message, source='application', details=None):
        return cls.log('INFO', message, source, details)
    
    @classmethod
    def warning(cls, message, source='application', details=None):
        return cls.log('WARNING', message, source, details)
    
    @classmethod
    def error(cls, message, source='application', details=None):
        return cls.log('ERROR', message, source, details)
    
    @classmethod
    def critical(cls, message, source='application', details=None):
        return cls.log('CRITICAL', message, source, details)

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
            key=key,
            defaults={'value': value}
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
            return value.lower() in ('true', 't', 'yes', 'y', '1')
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
    tags = models.CharField(max_length=255, blank=True, null=True)  # Comma-separated tags
    deployed = models.BooleanField(default=False)
    deployed_at = models.DateTimeField(null=True, blank=True)
    datahub_urn = models.CharField(max_length=255, null=True, blank=True)  # Store the DataHub URN when deployed
    
    def __str__(self):
        return self.name
    
    def get_content(self):
        """Get the recipe content as a Python object."""
        try:
            if self.content.strip().startswith('{'):
                return json.loads(self.content)
            else:
                import yaml
                return yaml.safe_load(self.content)
        except Exception:
            return None
    
    def get_tags_list(self):
        """Get the tags as a list."""
        if not self.tags:
            return []
        return [tag.strip() for tag in self.tags.split(',')]
    
    def set_tags_list(self, tags_list):
        """Set the tags from a list."""
        if not tags_list:
            self.tags = ''
        else:
            self.tags = ','.join(tags_list)
    
    def get_recipe_id(self):
        """Extract the ID portion from the DataHub URN."""
        if not self.datahub_urn:
            return None
        
        # URN format: urn:li:dataHubIngestionSource:<id>
        parts = self.datahub_urn.split(':')
        if len(parts) >= 4:
            return parts[3]
        return None

    def export_to_yaml(self, base_dir=None):
        """Export the recipe template to a YAML file."""
        if not base_dir:
            base_dir = Path(__file__).parent.parent.parent / 'recipes' / 'templates'
        
        # Create directory if it doesn't exist
        base_dir = Path(base_dir)
        base_dir.mkdir(parents=True, exist_ok=True)
        
        # Create YAML content
        yaml_content = {
            'name': self.name,
            'description': self.description,
            'recipe_type': self.recipe_type,
            'content': self.content
        }
        
        # Add tags if present
        if self.tags:
            yaml_content['tags'] = self.tags
        
        # Write to file
        file_path = base_dir / f"{self.recipe_type.lower()}.yml"
        with open(file_path, 'w') as f:
            yaml.dump(yaml_content, f, default_flow_style=False, sort_keys=False)
        
        return file_path

class RecipeManager:
    """Helper class for recipe-specific operations."""
    
    @classmethod
    def get_default_schedule(cls):
        """Get the default schedule cron expression."""
        return AppSettings.get('default_schedule', '0 0 * * *')
    
    @classmethod
    def set_default_schedule(cls, schedule):
        """Set the default schedule cron expression."""
        return AppSettings.set('default_schedule', schedule)
    
    @classmethod
    def get_templates_directory(cls):
        """Get the templates directory."""
        return AppSettings.get('recipe_dir', '')
    
    @classmethod
    def set_templates_directory(cls, directory):
        """Set the templates directory."""
        return AppSettings.set('recipe_dir', directory)
    
    @classmethod
    def get_auto_enable(cls):
        """Check if recipes should be auto-enabled."""
        return AppSettings.get_bool('auto_enable_recipes', False)
    
    @classmethod
    def set_auto_enable(cls, enabled):
        """Set whether recipes should be auto-enabled."""
        return AppSettings.set('auto_enable_recipes', 'true' if enabled else 'false')

class RecipeSecret(models.Model):
    """Model for storing recipe environment variables as secrets."""
    recipe_id = models.CharField(max_length=255)
    variable_name = models.CharField(max_length=255)
    value = models.TextField(blank=True, null=True)
    encrypted_value = models.TextField(blank=True, null=True)
    is_secret = models.BooleanField(default=False)
    
    class Meta:
        unique_together = ('recipe_id', 'variable_name')
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
    datahub_urn = models.CharField(max_length=255, null=True, blank=True)  # Store the DataHub URN when deployed
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return self.name
    
    def get_resources(self):
        """Get the resources as a Python object."""
        try:
            return json.loads(self.resources or '[]')
        except Exception:
            return []
    
    def set_resources(self, resources):
        """Set the resources from a Python object."""
        self.resources = json.dumps(resources or [])
    
    def get_privileges(self):
        """Get the privileges as a Python object."""
        try:
            return json.loads(self.privileges or '[]')
        except Exception:
            return []
    
    def set_privileges(self, privileges):
        """Set the privileges from a Python object."""
        self.privileges = json.dumps(privileges or [])
    
    def get_actors(self):
        """Get the actors as a Python object."""
        try:
            return json.loads(self.actors or '[]')
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
        parts = self.datahub_urn.split(':')
        if len(parts) >= 4:
            return parts[3]
        return None

class PullRequest(models.Model):
    """Model to track GitHub Pull Requests for recipes"""
    
    PR_STATUS_CHOICES = [
        ('open', 'Open'),
        ('closed', 'Closed'),
        ('merged', 'Merged'),
        ('draft', 'Draft'),
    ]
    
    recipe_id = models.CharField(max_length=255)  # Changed from ForeignKey to CharField
    pr_number = models.IntegerField()
    pr_url = models.URLField()
    status = models.CharField(max_length=10, choices=PR_STATUS_CHOICES, default='open')
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
    env_vars_instance = models.ForeignKey('EnvVarsInstance', on_delete=models.SET_NULL, null=True, blank=True)
    datahub_urn = models.CharField(max_length=255, null=True, blank=True)  # Store the DataHub URN when deployed
    deployed = models.BooleanField(default=False)
    deployed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"{self.name} ({self.template.name})"
    
    def get_recipe_id(self):
        """Extract the ID portion from the DataHub URN."""
        if not self.datahub_urn:
            return None
        
        # URN format: urn:li:dataHubIngestionSource:<id>
        parts = self.datahub_urn.split(':')
        if len(parts) >= 4:
            return parts[3]
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
            logger.error(f"Error applying environment variables to template for instance {self.id}")
            return template_content

    def export_to_yaml(self, base_dir=None):
        """Export the recipe instance to a YAML file."""
        if not base_dir:
            base_dir = Path(__file__).parent.parent.parent / 'recipes' / 'instances'
        
        # Create directory if it doesn't exist
        base_dir = Path(base_dir)
        base_dir.mkdir(parents=True, exist_ok=True)
        
        # Get the recipe content
        recipe_content = self.get_combined_content()
        if not recipe_content:
            raise ValueError("Unable to generate recipe content")
        
        # Create YAML content
        yaml_content = {
            'name': self.name,
            'description': self.description,
            'recipe_type': self.template.recipe_type,
            'recipe': recipe_content
        }
        
        # Add environment variables if present
        if self.env_vars_instance:
            yaml_content['env_vars'] = self.env_vars_instance.get_variables_dict()
        
        # Write to file
        file_path = base_dir / f"{self.name.lower().replace(' ', '_')}.yml"
        with open(file_path, 'w') as f:
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
        ('text', 'Text'),
        ('number', 'Number'),
        ('boolean', 'Boolean'),
        ('json', 'JSON')
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
        return [tag.strip() for tag in self.tags.split(',')]
    
    def set_tags_list(self, tags_list):
        """Set the tags from a list."""
        self.tags = ','.join(tags_list)
        
    def get_display_variables(self):
        """Return variables formatted for display in a template."""
        variables = self.get_variables_dict()
        result = []
        
        for key, details in variables.items():
            result.append({
                'key': key,
                'description': details.get('description', ''),
                'required': details.get('required', False),
                'is_secret': details.get('is_secret', False),
                'data_type': details.get('data_type', 'text'),
                'default_value': details.get('default_value', '')
            })
            
        return result

class EnvVarsInstance(models.Model):
    """Model for storing actual instances of environment variable configurations."""
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    template = models.ForeignKey(EnvVarsTemplate, on_delete=models.SET_NULL, null=True, blank=True)
    recipe_id = models.CharField(max_length=255, null=True, blank=True)  # Optional link to a recipe
    recipe_type = models.CharField(max_length=50, choices=RECIPE_TYPES)
    variables = models.TextField()  # JSON content with actual values, format: {"KEY": {"value": "actual_value", "isSecret": true/false}}
    deployed = models.BooleanField(default=False)
    deployed_at = models.DateTimeField(null=True, blank=True)
    datahub_secrets_created = models.BooleanField(default=False)  # Track if secrets have been created in DataHub
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
        return {k: v for k, v in variables.items() if v.get('isSecret', False)}
    
    @property
    def has_secret_variables(self):
        """Check if this instance has any secret variables."""
        variables = self.get_variables_dict()
        return any(v.get('isSecret', False) for k, v in variables.items())
        
    def validate_all_variables(self):
        """Validate all variable values against their defined data types in the template."""
        if not self.template:
            return True
            
        template_vars = self.template.get_variables_dict()
        instance_vars = self.get_variables_dict()
        
        for key, template_def in template_vars.items():
            # Skip if not required and not provided
            if not template_def.get('required', False) and (key not in instance_vars or not instance_vars[key].get('value')):
                continue
                
            # Check if required key is missing
            if template_def.get('required', False) and (key not in instance_vars or not instance_vars[key].get('value')):
                return False
                
            # Validate type if value exists
            if key in instance_vars and 'value' in instance_vars[key]:
                value = instance_vars[key]['value']
                if not self.template.validate_value_for_type(key, value):
                    return False
                    
        return True
        
    def get_typed_value(self, key):
        """Get the value converted to its proper data type based on the template."""
        variables = self.get_variables_dict()
        if key not in variables or 'value' not in variables[key]:
            return None
            
        value = variables[key]['value']
        
        if not self.template:
            return value
            
        template_vars = self.template.get_variables_dict()
        if key not in template_vars:
            return value
            
        data_type = template_vars[key].get('data_type', 'text')
        
        try:
            if data_type == 'text':
                return str(value)
            elif data_type == 'number':
                return float(value)
            elif data_type == 'boolean':
                val_lower = str(value).lower()
                return val_lower in ('true', 'yes', '1')
            elif data_type == 'json':
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
            ('open', 'Open'),
            ('merged', 'Merged'),
            ('closed', 'Closed'),
            ('pending', 'Pending')
        ],
        default='open'
    )
    branch_name = models.CharField(max_length=255)
    title = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "GitHub Pull Request"
        verbose_name_plural = "GitHub Pull Requests"
        ordering = ['-created_at']

    def __str__(self):
        return f"PR #{self.pr_number} - {self.title}"

    def get_status_display_color(self):
        """Get a color code for the status."""
        colors = {
            'open': 'primary',
            'merged': 'success',
            'closed': 'danger',
            'pending': 'warning'
        }
        return colors.get(self.pr_status, 'secondary')

class GitHubSettings(models.Model):
    """Model to store GitHub integration settings"""
    
    token = models.CharField(max_length=255, help_text="GitHub Personal Access Token")
    username = models.CharField(max_length=100, help_text="GitHub username or organization")
    repository = models.CharField(max_length=100, help_text="GitHub repository name")
    enabled = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        verbose_name = "GitHub Settings"
        verbose_name_plural = "GitHub Settings"
    
    def __str__(self):
        return f"GitHub: {self.username}/{self.repository}"
    
    @classmethod
    def get_instance(cls):
        """Get or create settings instance (singleton pattern)"""
        settings = cls.objects.first()
        if not settings:
            settings = cls.objects.create(
                token="",
                username="",
                repository="",
                enabled=False
            )
        return settings
    
    @classmethod
    def get_token(cls):
        """Get the GitHub token from settings"""
        settings = cls.get_instance()
        return settings.token if settings else ""
    
    @classmethod
    def get_username(cls):
        """Get the GitHub username from settings"""
        settings = cls.get_instance()
        return settings.username if settings else ""
    
    @classmethod
    def get_repository(cls):
        """Get the GitHub repository from settings"""
        settings = cls.get_instance()
        return settings.repository if settings else ""
    
    @classmethod
    def set_token(cls, token):
        """Set the GitHub token in settings"""
        settings = cls.get_instance()
        settings.token = token
        settings.save()
    
    @classmethod
    def set_username(cls, username):
        """Set the GitHub username in settings"""
        settings = cls.get_instance()
        settings.username = username
        settings.save()
    
    @classmethod
    def set_repository(cls, repository):
        """Set the GitHub repository in settings"""
        settings = cls.get_instance()
        settings.repository = repository
        settings.save()
    
    @classmethod
    def is_configured(cls):
        """Check if GitHub settings are properly configured"""
        settings = cls.get_instance()
        return settings.enabled and bool(settings.token and settings.username and settings.repository)

class GitHubIntegration:
    """Helper class for GitHub operations."""
    
    # Rate limiting constants
    RATE_LIMIT_WINDOW = 60  # seconds
    MAX_REQUESTS = 30  # per window
    
    @classmethod
    def is_configured(cls):
        """Check if GitHub integration is configured."""
        return bool(GitHubSettings.get_token() and GitHubSettings.get_repository())
    
    @classmethod
    def get_repo_url(cls):
        """Get the repository URL."""
        username = GitHubSettings.get_username()
        repo = GitHubSettings.get_repository()
        if username and repo:
            return f"https://github.com/{username}/{repo}"
        return None
    
    @classmethod
    def get_api_url(cls, endpoint=""):
        """Get GitHub API URL for the configured repository."""
        username = GitHubSettings.get_username()
        repo = GitHubSettings.get_repository()
        if username and repo:
            return f"https://api.github.com/repos/{username}/{repo}{endpoint}"
        return None
    
    @classmethod
    def _make_request(cls, method, url, headers=None, json=None, retries=3):
        """Make a request to GitHub API with rate limiting and retries."""
        if not headers:
            headers = {
                'Authorization': f'token {GitHubSettings.get_token()}',
                'Accept': 'application/vnd.github.v3+json'
            }
        
        for attempt in range(retries):
            try:
                response = requests.request(method, url, headers=headers, json=json)
                
                # Check rate limits
                remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
                reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
                
                if remaining <= 0:
                    wait_time = reset_time - time.time()
                    if wait_time > 0:
                        logger.warning(f"Rate limit reached. Waiting {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                if attempt == retries - 1:
                    raise
                logger.warning(f"Request failed (attempt {attempt + 1}/{retries}): {str(e)}")
                time.sleep(2 ** attempt)  # Exponential backoff
                
        return None
    
    @classmethod
    def get_active_prs(cls, recipe_id=None):
        """Get active pull requests, optionally filtered by recipe_id."""
        queryset = GitHubPR.objects.filter(pr_status__in=['open', 'pending'])
        if recipe_id:
            queryset = queryset.filter(recipe_id=recipe_id)
        return queryset
    
    @classmethod
    def create_pull_request(cls, recipe_id, recipe_name, recipe_content):
        """Create a GitHub pull request for a recipe."""
        if not cls.is_configured():
            logger.error("GitHub integration not configured")
            return None
        
        try:
            # Get default branch
            api_url = cls.get_api_url()
            response = cls._make_request('GET', api_url)
            default_branch = response.json().get('default_branch', 'main')
            
            # Create branch
            branch_name = f"recipe-{recipe_id}-{int(time.time())}"
            ref_url = cls.get_api_url(f"/git/refs/heads/{default_branch}")
            response = cls._make_request('GET', ref_url)
            sha = response.json().get('object', {}).get('sha')
            
            # Create new branch
            create_ref_url = cls.get_api_url("/git/refs")
            data = {
                'ref': f'refs/heads/{branch_name}',
                'sha': sha
            }
            cls._make_request('POST', create_ref_url, json=data)
            
            # Create/update recipe file
            recipe_path = f"recipes/{recipe_id}.yml"
            content_url = cls.get_api_url(f"/contents/{recipe_path}")
            
            try:
                response = cls._make_request('GET', content_url)
                file_sha = response.json().get('sha')
            except requests.exceptions.HTTPError as e:
                if e.response.status_code != 404:
                    raise
                file_sha = None
            
            # Upload file
            data = {
                'message': f"Update recipe: {recipe_name}",
                'content': base64.b64encode(recipe_content.encode()).decode(),
                'branch': branch_name
            }
            if file_sha:
                data['sha'] = file_sha
                
            cls._make_request('PUT', content_url, json=data)
            
            # Create PR
            pr_url = cls.get_api_url("/pulls")
            data = {
                'title': f"Update recipe: {recipe_name}",
                'body': f"This PR updates the recipe '{recipe_name}' (ID: {recipe_id}).",
                'head': branch_name,
                'base': default_branch
            }
            response = cls._make_request('POST', pr_url, json=data)
            pr_data = response.json()
            
            # Create PR record
            pr = GitHubPR.objects.create(
                recipe_id=recipe_id,
                pr_url=pr_data.get('html_url'),
                pr_number=pr_data.get('number'),
                pr_status='open',
                branch_name=branch_name,
                title=f"Update recipe: {recipe_name}",
                description=f"This PR updates the recipe '{recipe_name}' (ID: {recipe_id})."
            )
            return pr
            
        except Exception as e:
            error_msg = f"Error creating PR: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None
    
    @classmethod
    def update_pr_status(cls, pr_id, status):
        """Update the status of a pull request.
        
        Args:
            pr_id: The ID of the GitHubPR object
            status: The new status ('open', 'merged', 'closed')
            
        Returns:
            GitHubPR object if successful, None otherwise
        """
        try:
            pr = GitHubPR.objects.get(id=pr_id)
            pr.pr_status = status
            pr.updated_at = timezone.now()
            pr.save()
            return pr
        except GitHubPR.DoesNotExist:
            logger.error(f"PR with ID {pr_id} not found")
            return None
    
    @classmethod
    def fetch_pr_status(cls, pr_id):
        """Fetch the current status of a pull request from GitHub.
        
        Args:
            pr_id: The ID of the GitHubPR object
            
        Returns:
            Updated GitHubPR object if successful, None otherwise
        """
        if not cls.is_configured():
            logger.error("GitHub integration not configured")
            return None
        
        try:
            pr = GitHubPR.objects.get(id=pr_id)
            token = GitHubSettings.get_token()
            headers = {
                'Authorization': f'token {token}',
                'Accept': 'application/vnd.github.v3+json'
            }
            
            # Get PR info
            pr_api_url = cls.get_api_url(f"/pulls/{pr.pr_number}")
            response = requests.get(pr_api_url, headers=headers)
            response.raise_for_status()
            pr_data = response.json()
            
            # Update status
            state = pr_data.get('state')
            merged = pr_data.get('merged', False)
            
            if merged:
                pr.pr_status = 'merged'
            elif state == 'closed':
                pr.pr_status = 'closed'
            else:
                pr.pr_status = 'open'
            
            pr.updated_at = timezone.now()
            pr.save()
            return pr
            
        except GitHubPR.DoesNotExist:
            logger.error(f"PR with ID {pr_id} not found")
            return None
        except Exception as e:
            logger.error(f"Error fetching PR status: {str(e)}")
            return None
    
    @classmethod
    def get_pull_request(cls, pr_number):
        """Get a pull request by number.
        
        Args:
            pr_number: The pull request number
            
        Returns:
            GitHubPR object if successful, None otherwise
        """
        if not cls.is_configured():
            logger.error("GitHub integration not configured")
            return None
        
        token = GitHubSettings.get_token()
        api_url = cls.get_api_url(f"/pulls/{pr_number}")
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            pr_data = response.json()
            
            # Update or create PR record
            pr, created = GitHubPR.objects.update_or_create(
                pr_number=pr_number,
                defaults={
                    'recipe_id': pr_data.get('title', '').split(':')[0] if ':' in pr_data.get('title', '') else '',
                    'pr_url': pr_data.get('html_url'),
                    'pr_status': 'merged' if pr_data.get('merged') else 'closed' if pr_data.get('state') == 'closed' else 'open',
                    'branch_name': pr_data.get('head', {}).get('ref'),
                    'title': pr_data.get('title'),
                    'description': pr_data.get('body')
                }
            )
            return pr
            
        except Exception as e:
            error_msg = f"Error getting PR #{pr_number}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None
    
    @classmethod
    def update_pull_request_status(cls, pr_number):
        """Update the status of a pull request.
        
        Args:
            pr_number: The pull request number
            
        Returns:
            GitHubPR object if successful, None otherwise
        """
        if not cls.is_configured():
            logger.error("GitHub integration not configured")
            return None
        
        token = GitHubSettings.get_token()
        api_url = cls.get_api_url(f"/pulls/{pr_number}")
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            pr_data = response.json()
            
            # Update PR record
            pr = GitHubPR.objects.get(pr_number=pr_number)
            pr.pr_status = 'merged' if pr_data.get('merged') else 'closed' if pr_data.get('state') == 'closed' else 'open'
            pr.save()
            return pr
            
        except GitHubPR.DoesNotExist:
            error_msg = f"PR #{pr_number} not found in database"
            logger.error(error_msg)
            return None
        except Exception as e:
            error_msg = f"Error updating PR #{pr_number} status: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None
    
    @classmethod
    def get_pull_request_content(cls, pr_number):
        """Get the content of a pull request.
        
        Args:
            pr_number: The pull request number
            
        Returns:
            Dictionary with recipe content if successful, None otherwise
        """
        if not cls.is_configured():
            logger.error("GitHub integration not configured")
            return None
        
        token = GitHubSettings.get_token()
        api_url = cls.get_api_url(f"/pulls/{pr_number}/files")
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            files = response.json()
            
            # Find recipe file
            recipe_file = next((f for f in files if f['filename'].endswith('.yml')), None)
            if not recipe_file:
                error_msg = f"No recipe file found in PR #{pr_number}"
                logger.error(error_msg)
                return None
            
            # Get file content
            content_url = recipe_file['raw_url']
            response = requests.get(content_url)
            response.raise_for_status()
            content = response.text
            
            return {
                'filename': recipe_file['filename'],
                'content': content
            }
            
        except Exception as e:
            error_msg = f"Error getting PR #{pr_number} content: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None
    
    @classmethod
    def push_to_github(cls, instance_or_template, commit_message=None):
        """Push a recipe instance or template to GitHub and create a PR."""
        if not cls.is_configured():
            logger.error("GitHub integration not configured")
            return None
        
        try:
            # Export to YAML
            if isinstance(instance_or_template, RecipeInstance):
                file_path = instance_or_template.export_to_yaml()
                branch_name = f"recipe-instance-{instance_or_template.id}-{int(time.time())}"
                pr_title = f"Update recipe instance: {instance_or_template.name}"
                pr_description = f"This PR updates the recipe instance '{instance_or_template.name}' (ID: {instance_or_template.id})."
            elif isinstance(instance_or_template, RecipeTemplate):
                file_path = instance_or_template.export_to_yaml()
                branch_name = f"recipe-template-{instance_or_template.id}-{int(time.time())}"
                pr_title = f"Update recipe template: {instance_or_template.name}"
                pr_description = f"This PR updates the recipe template '{instance_or_template.name}' (ID: {instance_or_template.id})."
            else:
                raise ValueError("Invalid object type")
            
            if not commit_message:
                commit_message = pr_title
            
            # Get default branch
            api_url = cls.get_api_url()
            response = cls._make_request('GET', api_url)
            default_branch = response.json().get('default_branch', 'main')
            
            # Create branch
            ref_url = cls.get_api_url(f"/git/refs/heads/{default_branch}")
            response = cls._make_request('GET', ref_url)
            sha = response.json().get('object', {}).get('sha')
            
            # Create new branch
            create_ref_url = cls.get_api_url("/git/refs")
            data = {
                'ref': f'refs/heads/{branch_name}',
                'sha': sha
            }
            cls._make_request('POST', create_ref_url, json=data)
            
            # Read file content
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Upload file
            content_url = cls.get_api_url(f"/contents/{file_path.relative_to(Path(__file__).parent.parent.parent)}")
            data = {
                'message': commit_message,
                'content': base64.b64encode(content.encode()).decode(),
                'branch': branch_name
            }
            
            # Check if file exists
            try:
                response = cls._make_request('GET', content_url)
                data['sha'] = response.json().get('sha')
            except requests.exceptions.HTTPError as e:
                if e.response.status_code != 404:
                    raise
            
            cls._make_request('PUT', content_url, json=data)
            
            # Create PR
            pr_url = cls.get_api_url("/pulls")
            data = {
                'title': pr_title,
                'body': pr_description,
                'head': branch_name,
                'base': default_branch
            }
            response = cls._make_request('POST', pr_url, json=data)
            pr_data = response.json()
            
            # Create PR record
            pr = GitHubPR.objects.create(
                recipe_id=instance_or_template.id,
                pr_url=pr_data.get('html_url'),
                pr_number=pr_data.get('number'),
                pr_status='open',
                branch_name=branch_name,
                title=pr_title,
                description=pr_description
            )
            return pr
            
        except Exception as e:
            error_msg = f"Error pushing to GitHub: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None 