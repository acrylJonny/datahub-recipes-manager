from django import forms
from django.contrib.auth.forms import AuthenticationForm
from django.contrib.auth.models import User
from django.conf import settings
import json
import yaml

from .models import RecipeTemplate, EnvVarsTemplate, EnvVarsInstance, GitHubSettings

class RecipeForm(forms.Form):
    """Form for creating or editing a recipe."""
    recipe_id = forms.CharField(label="Recipe ID", max_length=255, required=True,
                               widget=forms.TextInput(attrs={'class': 'form-control'}))
    recipe_name = forms.CharField(label="Recipe Name", max_length=255, required=True,
                                 widget=forms.TextInput(attrs={'class': 'form-control'}))
    recipe_type = forms.CharField(label="Recipe Type", max_length=50, required=True,
                                 widget=forms.TextInput(attrs={'class': 'form-control'}))
    description = forms.CharField(label="Description", required=False,
                                 widget=forms.Textarea(attrs={'class': 'form-control', 'rows': 3}))
    schedule_cron = forms.CharField(label="Schedule (Cron Expression)", max_length=100, required=False,
                                   widget=forms.TextInput(attrs={'class': 'form-control'}))
    schedule_timezone = forms.CharField(label="Timezone", max_length=50, required=False,
                                       widget=forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'UTC'}))
    recipe_content = forms.CharField(label="Recipe Content (YAML/JSON)", required=True,
                                    widget=forms.Textarea(attrs={'class': 'form-control', 'rows': 15}))
    replace_env_vars = forms.BooleanField(label="Replace environment variables in recipe", required=False,
                                         initial=False,
                                         widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
                                         help_text="If checked, environment variable placeholders will be replaced with their values in the recipe.")

class RecipeImportForm(forms.Form):
    """Form for importing a recipe from a file."""
    recipe_file = forms.FileField(label="Recipe File (YAML/JSON)", required=True,
                                 widget=forms.FileInput(attrs={'class': 'form-control'}))

class PolicyForm(forms.Form):
    """Form for creating or editing a policy."""
    policy_id = forms.CharField(label="Policy ID", max_length=255, required=False,
                               widget=forms.TextInput(attrs={'class': 'form-control'}))
    policy_name = forms.CharField(label="Policy Name", max_length=255, required=True,
                                 widget=forms.TextInput(attrs={'class': 'form-control'}))
    policy_type = forms.ChoiceField(label="Policy Type", required=True,
                                  choices=[('METADATA_POLICY', 'Metadata Policy'),
                                          ('PLATFORM_POLICY', 'Platform Policy')],
                                  widget=forms.Select(attrs={'class': 'form-control'}))
    policy_state = forms.ChoiceField(label="Policy State", required=True,
                                   choices=[('ACTIVE', 'Active'), ('INACTIVE', 'Inactive')],
                                   widget=forms.Select(attrs={'class': 'form-control'}))
    description = forms.CharField(label="Description", required=False,
                                 widget=forms.Textarea(attrs={'class': 'form-control', 'rows': 3}))
    policy_resources = forms.CharField(label="Resources (JSON Array)", required=False,
                                    widget=forms.Textarea(attrs={'class': 'form-control', 'rows': 5}),
                                    help_text="Enter resource objects as a JSON array")
    policy_privileges = forms.CharField(label="Privileges (JSON Array)", required=False,
                                     widget=forms.Textarea(attrs={'class': 'form-control', 'rows': 5}),
                                     help_text="Enter privilege strings as a JSON array")
    policy_actors = forms.CharField(label="Actors (JSON Object)", required=False,
                                  widget=forms.Textarea(attrs={'class': 'form-control', 'rows': 5}),
                                  help_text="Enter actors as a JSON object")

class PolicyImportForm(forms.Form):
    """Form for importing a policy from a file."""
    policy_file = forms.FileField(label="Policy File (JSON)", required=True,
                                 widget=forms.FileInput(attrs={'class': 'form-control'}))

class RecipeTemplateForm(forms.Form):
    """Form for creating or editing a recipe template."""
    name = forms.CharField(label="Template Name", max_length=255, required=True,
                          widget=forms.TextInput(attrs={'class': 'form-control'}))
    description = forms.CharField(label="Description", required=False,
                                 widget=forms.Textarea(attrs={'class': 'form-control', 'rows': 3}))
    recipe_type = forms.CharField(label="Recipe Type", max_length=50, required=True,
                                 widget=forms.TextInput(attrs={'class': 'form-control'}))
    tags = forms.CharField(label="Tags", max_length=255, required=False,
                          widget=forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'comma,separated,tags'}),
                          help_text="Enter comma-separated tags to categorize this template")
    content = forms.CharField(label="Recipe Content (YAML/JSON)", required=True,
                             widget=forms.Textarea(attrs={'class': 'form-control', 'rows': 15}))

class RecipeTemplateImportForm(forms.Form):
    """Form for importing a recipe template from a file."""
    template_file = forms.FileField(label="Recipe Template File (YAML/JSON)", required=True,
                                   widget=forms.FileInput(attrs={'class': 'form-control'}))
    tags = forms.CharField(label="Tags", max_length=255, required=False,
                          widget=forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'comma,separated,tags'}),
                          help_text="Enter comma-separated tags to categorize this template")

class RecipeDeployForm(forms.Form):
    """Form for deploying a recipe template to DataHub."""
    recipe_id = forms.CharField(label="Recipe ID", max_length=255, required=True,
                               widget=forms.TextInput(attrs={'class': 'form-control'}))
    recipe_name = forms.CharField(label="Recipe Name", max_length=255, required=True,
                                 widget=forms.TextInput(attrs={'class': 'form-control'}))
    schedule_cron = forms.CharField(label="Schedule (Cron Expression)", max_length=100, required=False,
                                   widget=forms.TextInput(attrs={'class': 'form-control', 'placeholder': '0 0 * * *'}))
    schedule_timezone = forms.CharField(label="Timezone", max_length=50, required=False,
                                       widget=forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'UTC'}))
    environment_variables = forms.CharField(label="Environment Variables", required=False,
                                          widget=forms.HiddenInput(attrs={'id': 'env_vars_json'}))
    description = forms.CharField(label="Description", required=False,
                                widget=forms.Textarea(attrs={'class': 'form-control', 'rows': 3}))

class EnvVarsTemplateForm(forms.ModelForm):
    """Form for creating or editing an environment variables template."""
    variables = forms.CharField(label="Environment Variables", required=True,
                              widget=forms.HiddenInput(attrs={'id': 'env_vars_template_json'}))
    
    class Meta:
        model = EnvVarsTemplate
        fields = ['name', 'description', 'recipe_type', 'tags', 'variables']
        widgets = {
            'name': forms.TextInput(attrs={'class': 'form-control'}),
            'description': forms.Textarea(attrs={'class': 'form-control', 'rows': 3}),
            'recipe_type': forms.TextInput(attrs={'class': 'form-control'}),
            'tags': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'comma,separated,tags'}),
        }
        help_texts = {
            'tags': "Enter comma-separated tags to categorize this template"
        }

class EnvVarsInstanceForm(forms.Form):
    """Form for creating or editing an environment variables instance."""
    name = forms.CharField(label="Instance Name", max_length=255, required=True,
                         widget=forms.TextInput(attrs={'class': 'form-control'}))
    description = forms.CharField(label="Description", required=False,
                                widget=forms.Textarea(attrs={'class': 'form-control', 'rows': 3}))
    template = forms.ModelChoiceField(label="Template", required=False,
                                    queryset=None,  # Set in __init__
                                    widget=forms.Select(attrs={'class': 'form-control'}),
                                    help_text="Select an optional template to base this instance on")
    recipe_type = forms.CharField(label="Recipe Type", max_length=50, required=True,
                                widget=forms.TextInput(attrs={'class': 'form-control'}))
    variables = forms.CharField(label="Environment Variables", required=True,
                             widget=forms.HiddenInput(attrs={'id': 'env_vars_instance_json'}))
    
    def __init__(self, *args, **kwargs):
        from .models import EnvVarsTemplate
        super().__init__(*args, **kwargs)
        self.fields['template'].queryset = EnvVarsTemplate.objects.all().order_by('name')

class RecipeInstanceForm(forms.Form):
    """Form for creating or editing a recipe instance."""
    name = forms.CharField(
        max_length=255, 
        required=True,
        label="Instance Name",
        widget=forms.TextInput(attrs={'class': 'form-control'})
    )
    description = forms.CharField(
        max_length=1000,
        required=False,
        label="Description",
        widget=forms.Textarea(attrs={'class': 'form-control', 'rows': 3})
    )
    template = forms.ModelChoiceField(
        queryset=RecipeTemplate.objects.all(),
        required=True,
        label="Recipe Template",
        widget=forms.Select(attrs={'class': 'form-control'})
    )
    env_vars_instance = forms.ModelChoiceField(
        queryset=EnvVarsInstance.objects.all(),
        required=False,
        label="Environment Variables Instance",
        widget=forms.Select(attrs={'class': 'form-control'})
    ) 

class GitHubSettingsForm(forms.ModelForm):
    """Form for GitHub integration settings."""
    
    class Meta:
        model = GitHubSettings
        fields = ['username', 'repository', 'token', 'enabled']
        widgets = {
            'token': forms.PasswordInput(render_value=True),
            'username': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'GitHub username or organization'}),
            'repository': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Repository name'}),
        }
        help_texts = {
            'token': 'GitHub Personal Access Token with repo scope',
            'username': 'Your GitHub username or organization name',
            'repository': 'The repository name (not the full URL)',
            'enabled': 'Enable GitHub integration'
        } 