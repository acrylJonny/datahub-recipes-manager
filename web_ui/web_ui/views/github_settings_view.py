import logging

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect
from django.urls import reverse

from web_ui.forms.github_settings_form import GitHubSettingsForm
from web_ui.models import GitHubSettings, PullRequest

logger = logging.getLogger(__name__)

@login_required
def github_settings(request):
    """
    View for managing GitHub integration settings
    """
    settings = GitHubSettings.get_instance()
    
    if request.method == 'POST':
        form = GitHubSettingsForm(request.POST, instance=settings)
        if form.is_valid():
            form.save()
            messages.success(request, "GitHub settings updated successfully")
            return redirect(reverse('github_settings'))
    else:
        form = GitHubSettingsForm(instance=settings)
    
    # Get recent pull requests
    pull_requests = PullRequest.objects.all().order_by('-created_at')[:10]
    
    context = {
        'form': form,
        'pull_requests': pull_requests,
        'active_tab': 'settings',
        'active_settings_tab': 'github',
    }
    
    return render(request, 'settings/github.html', context) 