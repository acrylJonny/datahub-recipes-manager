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
    encrypted_value = models.TextField(blank=True, null=True)
    
    class Meta:
        unique_together = ('recipe_id', 'variable_name')
        verbose_name = "Recipe Secret"
        verbose_name_plural = "Recipe Secrets"
    
    def __str__(self):
        return f"{self.recipe_id} - {self.variable_name}"

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
    def get_active_prs(cls, recipe_id=None):
        """Get active pull requests, optionally filtered by recipe_id."""
        queryset = GitHubPR.objects.filter(pr_status__in=['open', 'pending'])
        if recipe_id:
            queryset = queryset.filter(recipe_id=recipe_id)
        return queryset
    
    @classmethod
    def create_pull_request(cls, recipe_id, recipe_name, recipe_content):
        """Create a GitHub pull request for a recipe.
        
        Args:
            recipe_id: The ID of the recipe
            recipe_name: The name of the recipe
            recipe_content: The content of the recipe
            
        Returns:
            GitHubPR object if successful, None otherwise
        """
        if not cls.is_configured():
            logger.error("GitHub integration not configured")
            return None
        
        token = GitHubSettings.get_token()
        
        # Create a unique branch name
        timestamp = int(time.time())
        safe_recipe_id = re.sub(r'[^a-zA-Z0-9]', '-', recipe_id)
        branch_name = f"recipe-{safe_recipe_id}-{timestamp}"
        
        # Create PR title and description
        title = f"Update recipe: {recipe_name}"
        description = f"This PR updates the recipe '{recipe_name}' (ID: {recipe_id})."
        
        # Get the default branch
        api_url = cls.get_api_url()
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        try:
            # Get the default branch
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            default_branch = response.json().get('default_branch', 'main')
            
            # Get the reference to the default branch
            ref_url = cls.get_api_url(f"/git/refs/heads/{default_branch}")
            response = requests.get(ref_url, headers=headers)
            response.raise_for_status()
            sha = response.json().get('object', {}).get('sha')
            
            # Create a new branch
            create_ref_url = cls.get_api_url("/git/refs")
            data = {
                'ref': f'refs/heads/{branch_name}',
                'sha': sha
            }
            response = requests.post(create_ref_url, headers=headers, json=data)
            response.raise_for_status()
            
            # Get recipe path
            recipe_path = f"recipes/{recipe_id}.yml"
            
            # Check if file exists
            content_url = cls.get_api_url(f"/contents/{recipe_path}")
            file_exists = True
            try:
                response = requests.get(content_url, headers=headers)
                response.raise_for_status()
                file_sha = response.json().get('sha')
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    file_exists = False
                else:
                    raise
            
            # Create or update file
            data = {
                'message': f"Update recipe: {recipe_name}",
                'content': base64.b64encode(recipe_content.encode()).decode(),
                'branch': branch_name
            }
            
            if file_exists:
                data['sha'] = file_sha
            
            response = requests.put(content_url, headers=headers, json=data)
            response.raise_for_status()
            
            # Create PR
            pr_url = cls.get_api_url("/pulls")
            data = {
                'title': title,
                'body': description,
                'head': branch_name,
                'base': default_branch
            }
            response = requests.post(pr_url, headers=headers, json=data)
            response.raise_for_status()
            pr_data = response.json()
            
            # Create PR record
            pr = GitHubPR.objects.create(
                recipe_id=recipe_id,
                pr_url=pr_data.get('html_url'),
                pr_number=pr_data.get('number'),
                pr_status='open',
                branch_name=branch_name,
                title=title,
                description=description
            )
            return pr
            
        except Exception as e:
            logger.error(f"Error creating PR: {str(e)}")
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