import json
import logging
import requests
from django.contrib import messages
from django.http import JsonResponse
from django.shortcuts import render, redirect, get_object_or_404
from django.urls import reverse
from django.views.decorators.http import require_POST
from django.core.paginator import Paginator
from django.contrib.auth.decorators import login_required

from web_ui.models import GitHubSettings, GitHubPR
from web_ui.forms import GitHubSettingsForm

logger = logging.getLogger(__name__)

def github_index(request):
    """Main GitHub integration page."""
    settings = GitHubSettings.get_instance()
    pull_requests = GitHubPR.objects.all().order_by('-created_at')[:10]
    
    context = {
        'github_settings': settings,
        'pull_requests': pull_requests,
        'is_configured': settings.is_configured()
    }
    
    return render(request, 'github/index.html', context)

def github_settings_edit(request):
    """Edit GitHub integration settings."""
    settings = GitHubSettings.get_instance()
    
    if request.method == 'POST':
        form = GitHubSettingsForm(request.POST, instance=settings)
        if form.is_valid():
            form.save()
            messages.success(request, "GitHub settings updated successfully")
            return redirect('github')
    else:
        form = GitHubSettingsForm(instance=settings)
    
    context = {
        'form': form,
        'github_settings': settings
    }
    
    return render(request, 'github/settings.html', context)

def github_pull_requests(request):
    """List all GitHub pull requests."""
    settings = GitHubSettings.get_instance()
    
    # Get all PRs and paginate
    all_prs = GitHubPR.objects.all().order_by('-created_at')
    paginator = Paginator(all_prs, 25)  # Show 25 PRs per page
    
    page_number = request.GET.get('page')
    pull_requests = paginator.get_page(page_number)
    
    context = {
        'github_settings': settings,
        'pull_requests': pull_requests,
        'is_paginated': paginator.num_pages > 1,
        'page_obj': pull_requests
    }
    
    return render(request, 'github/pull_requests.html', context)

@require_POST
def github_test_connection(request):
    """Test GitHub connection."""
    try:
        data = json.loads(request.body)
        username = data.get('username')
        repository = data.get('repository')
        token = data.get('token')
        
        if not all([username, repository, token]):
            return JsonResponse({'success': False, 'error': 'Missing required fields'})
        
        # Test connection with GitHub API
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        # Test repo access
        repo_url = f'https://api.github.com/repos/{username}/{repository}'
        response = requests.get(repo_url, headers=headers)
        
        if response.status_code == 200:
            return JsonResponse({'success': True})
        else:
            error_message = response.json().get('message', 'Unknown error')
            return JsonResponse({'success': False, 'error': error_message})
            
    except Exception as e:
        logger.error(f"Error testing GitHub connection: {str(e)}")
        return JsonResponse({'success': False, 'error': str(e)})

@require_POST
def github_create_branch(request):
    """Create a new branch on GitHub."""
    if not GitHubSettings.is_configured():
        messages.error(request, "GitHub integration is not configured")
        return redirect('github')
    
    branch_name = request.POST.get('branch_name')
    branch_description = request.POST.get('branch_description', '')
    
    if not branch_name:
        messages.error(request, "Branch name is required")
        return redirect('github')
    
    settings = GitHubSettings.get_instance()
    headers = {
        'Authorization': f'token {settings.token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    
    try:
        # First, get the default branch and its sha
        repo_url = f'https://api.github.com/repos/{settings.username}/{settings.repository}'
        repo_response = requests.get(repo_url, headers=headers)
        repo_response.raise_for_status()
        repo_data = repo_response.json()
        default_branch = repo_data.get('default_branch', 'main')
        
        # Get the SHA of the default branch
        branch_url = f'https://api.github.com/repos/{settings.username}/{settings.repository}/branches/{default_branch}'
        branch_response = requests.get(branch_url, headers=headers)
        branch_response.raise_for_status()
        branch_data = branch_response.json()
        sha = branch_data.get('commit', {}).get('sha')
        
        if not sha:
            messages.error(request, f"Could not determine SHA of {default_branch} branch")
            return redirect('github')
        
        # Create the new branch
        create_url = f'https://api.github.com/repos/{settings.username}/{settings.repository}/git/refs'
        create_data = {
            'ref': f'refs/heads/{branch_name}',
            'sha': sha
        }
        
        create_response = requests.post(create_url, headers=headers, json=create_data)
        create_response.raise_for_status()
        
        messages.success(request, f"Branch '{branch_name}' created successfully")
        
        # Create a PR if description is provided
        if branch_description:
            pr_url = f'https://api.github.com/repos/{settings.username}/{settings.repository}/pulls'
            pr_data = {
                'title': f'New branch: {branch_name}',
                'body': branch_description,
                'head': branch_name,
                'base': default_branch
            }
            
            pr_response = requests.post(pr_url, headers=headers, json=pr_data)
            
            if pr_response.status_code == 201:
                pr_result = pr_response.json()
                pr_number = pr_result.get('number')
                html_url = pr_result.get('html_url')
                
                # Create PR record
                GitHubPR.objects.create(
                    recipe_id="N/A",
                    pr_url=html_url,
                    pr_number=pr_number,
                    pr_status='open',
                    branch_name=branch_name,
                    title=f'New branch: {branch_name}',
                    description=branch_description
                )
                
                messages.success(request, f"Pull request #{pr_number} created")
        
        return redirect('github')
        
    except requests.exceptions.RequestException as e:
        try:
            error_data = e.response.json()
            error_message = error_data.get('message', str(e))
            messages.error(request, f"GitHub error: {error_message}")
        except Exception:
            messages.error(request, f"Error creating branch: {str(e)}")
        
        return redirect('github')

def github_sync_recipes(request):
    """Sync all recipes with GitHub."""
    # For now, just redirect back with a message
    # This would be implemented based on your recipe model and requirements
    messages.info(request, "Recipe sync feature is under development")
    return redirect('github')

def github_sync_status(request):
    """Sync PR statuses with GitHub."""
    if not GitHubSettings.is_configured():
        messages.error(request, "GitHub integration is not configured")
        return redirect('github')
    
    settings = GitHubSettings.get_instance()
    headers = {
        'Authorization': f'token {settings.token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    
    # Get all open/pending PRs
    active_prs = GitHubPR.objects.filter(pr_status__in=['open', 'pending'])
    
    updated_count = 0
    error_count = 0
    
    for pr in active_prs:
        try:
            # Get PR status from GitHub
            pr_url = f'https://api.github.com/repos/{settings.username}/{settings.repository}/pulls/{pr.pr_number}'
            response = requests.get(pr_url, headers=headers)
            
            if response.status_code == 200:
                pr_data = response.json()
                
                # Update PR status
                if pr_data.get('merged'):
                    pr.pr_status = 'merged'
                elif pr_data.get('state') == 'closed':
                    pr.pr_status = 'closed'
                else:
                    pr.pr_status = 'open'
                
                pr.save()
                updated_count += 1
            else:
                error_count += 1
                logger.error(f"Error fetching PR #{pr.pr_number}: {response.status_code}")
                
        except Exception as e:
            error_count += 1
            logger.error(f"Exception updating PR #{pr.pr_number}: {str(e)}")
    
    if updated_count > 0:
        messages.success(request, f"Updated status for {updated_count} pull requests")
    
    if error_count > 0:
        messages.warning(request, f"Failed to update {error_count} pull requests")
    
    if updated_count == 0 and error_count == 0:
        messages.info(request, "No pull requests to update")
    
    return redirect('github')

@require_POST
def github_update_pr_status(request, pr_number):
    """Update a specific PR status."""
    if not GitHubSettings.is_configured():
        return JsonResponse({'success': False, 'error': 'GitHub integration not configured'})
    
    try:
        pr = GitHubPR.objects.get(pr_number=pr_number)
        
        settings = GitHubSettings.get_instance()
        headers = {
            'Authorization': f'token {settings.token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        # Get PR status from GitHub
        pr_url = f'https://api.github.com/repos/{settings.username}/{settings.repository}/pulls/{pr_number}'
        response = requests.get(pr_url, headers=headers)
        
        if response.status_code == 200:
            pr_data = response.json()
            
            # Update PR status
            if pr_data.get('merged'):
                pr.pr_status = 'merged'
            elif pr_data.get('state') == 'closed':
                pr.pr_status = 'closed'
            else:
                pr.pr_status = 'open'
            
            pr.save()
            return JsonResponse({'success': True, 'status': pr.pr_status})
            
        else:
            error_message = response.json().get('message', 'Unknown error')
            return JsonResponse({'success': False, 'error': error_message})
            
    except GitHubPR.DoesNotExist:
        return JsonResponse({'success': False, 'error': f'Pull request #{pr_number} not found'})
    except Exception as e:
        logger.error(f"Error updating PR status: {str(e)}")
        return JsonResponse({'success': False, 'error': str(e)})

@require_POST
def github_delete_pr(request, pr_id):
    """Delete a PR record from the database."""
    pr = get_object_or_404(GitHubPR, id=pr_id)
    pr_number = pr.pr_number
    
    try:
        pr.delete()
        messages.success(request, f"Pull request #{pr_number} record deleted")
    except Exception as e:
        messages.error(request, f"Error deleting record: {str(e)}")
    
    return redirect('github_pull_requests')

@login_required
def github_switch_branch(request, branch_name):
    """Switch the current branch in GitHub settings."""
    try:
        github_settings = GitHubSettings.objects.first()
        if not github_settings:
            messages.error(request, "GitHub settings not found")
            return redirect('github_index')

        # Update the current branch
        github_settings.current_branch = branch_name
        github_settings.save()

        messages.success(request, f"Switched to branch: {branch_name}")
    except Exception as e:
        logger.error(f"Error switching branch: {str(e)}")
        messages.error(request, f"Error switching branch: {str(e)}")

    return redirect('github_index') 