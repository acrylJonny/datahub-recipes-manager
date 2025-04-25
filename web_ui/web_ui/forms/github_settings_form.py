from django import forms
from web_ui.models import GitHubSettings

class GitHubSettingsForm(forms.ModelForm):
    """
    Form for managing GitHub integration settings
    """
    
    class Meta:
        model = GitHubSettings
        fields = ['token', 'repository', 'base_branch', 'username', 'email']
        widgets = {
            'token': forms.PasswordInput(render_value=True),
        }
        labels = {
            'token': 'GitHub Personal Access Token',
            'repository': 'Repository (format: owner/repo)',
            'base_branch': 'Base Branch (e.g., main)',
            'username': 'Git Username',
            'email': 'Git Email',
        }
        help_texts = {
            'token': 'Personal access token with repo scope',
            'repository': 'The repository where recipes will be synced',
            'base_branch': 'The branch to create pull requests against',
            'username': 'Username for git commits',
            'email': 'Email for git commits',
        } 