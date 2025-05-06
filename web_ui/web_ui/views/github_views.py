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
import base64

from web_ui.models import GitSettings, GitHubPR, GitIntegration
from web_ui.forms import GitSettingsForm

logger = logging.getLogger(__name__)

def github_index(request):
    """Main GitHub integration page."""
    settings = GitSettings.get_instance()
    pull_requests = GitHubPR.objects.all().order_by('-created_at')[:10]
    
    context = {
        'git_settings': settings,
        'pull_requests': pull_requests,
        'is_configured': settings.is_configured(),
        'branches': GitSettings.get_branches()
    }
    
    return render(request, 'github/index.html', context)

def github_settings_edit(request):
    """Edit Git integration settings."""
    settings = GitSettings.get_instance()
    
    if request.method == 'POST':
        form = GitSettingsForm(request.POST, instance=settings)
        if form.is_valid():
            form.save()
            messages.success(request, "Git integration settings updated successfully")
            return redirect('github')
    else:
        form = GitSettingsForm(instance=settings)
    
    context = {
        'form': form,
        'git_settings': settings
    }
    
    return render(request, 'github/settings.html', context)

def github_pull_requests(request):
    """List all Git provider pull requests."""
    settings = GitSettings.get_instance()
    
    # Get all PRs and paginate
    all_prs = GitHubPR.objects.all().order_by('-created_at')
    paginator = Paginator(all_prs, 25)  # Show 25 PRs per page
    
    page_number = request.GET.get('page')
    pull_requests = paginator.get_page(page_number)
    
    context = {
        'git_settings': settings,
        'pull_requests': pull_requests,
        'is_paginated': paginator.num_pages > 1,
        'page_obj': pull_requests
    }
    
    return render(request, 'github/pull_requests.html', context)

@require_POST
def github_test_connection(request):
    """Test Git provider connection."""
    try:
        data = json.loads(request.body)
        provider_type = data.get('provider_type', 'github')
        base_url = data.get('base_url', '')
        username = data.get('username')
        repository = data.get('repository')
        token = data.get('token')
        
        if not all([username, repository, token]):
            return JsonResponse({'success': False, 'error': 'Missing required fields'})
        
        # For Azure DevOps, validate username format
        if provider_type == 'azure_devops' and '/' not in username:
            return JsonResponse({'success': False, 'error': 'Azure DevOps username must be in format: organization/project'})
            
        # Test connection based on provider type
        if provider_type == 'github':
            # GitHub API
            headers = {
                'Authorization': f'token {token}',
                'Accept': 'application/vnd.github.v3+json'
            }
            
            # Use custom URL or default GitHub API
            api_base = base_url.rstrip('/') if base_url else 'https://api.github.com'
            repo_url = f"{api_base}/repos/{username}/{repository}"
            
            response = requests.get(repo_url, headers=headers)
            
            if response.status_code == 200:
                return JsonResponse({'success': True})
            else:
                error_message = response.json().get('message', 'Unknown error')
                return JsonResponse({'success': False, 'error': error_message})
                
        elif provider_type == 'azure_devops':
            # Azure DevOps API
            auth_token = base64.b64encode(f":{token}".encode()).decode()
            headers = {
                'Authorization': f'Basic {auth_token}',
                'Content-Type': 'application/json'
            }
            
            # Split username into organization/project
            org_project = username.split('/')
            if len(org_project) != 2:
                return JsonResponse({'success': False, 'error': 'Azure DevOps username must be in format: organization/project'})
                
            org, project = org_project
            
            # Use custom URL or default Azure DevOps API
            api_base = base_url.rstrip('/') if base_url else 'https://dev.azure.com'
            repo_url = f"{api_base}/{org}/{project}/_apis/git/repositories/{repository}?api-version=6.0"
            
            response = requests.get(repo_url, headers=headers)
            
            if response.status_code == 200:
                return JsonResponse({'success': True})
            else:
                error_message = f"HTTP {response.status_code}: Unable to access repository"
                try:
                    error_data = response.json()
                    if 'message' in error_data:
                        error_message = error_data['message']
                except:
                    pass
                return JsonResponse({'success': False, 'error': error_message})
                
        elif provider_type == 'gitlab':
            # GitLab API
            headers = {
                'Private-Token': token,
                'Content-Type': 'application/json'
            }
            
            # Encode the repository path
            encoded_repo = f"{username}%2F{repository}"
            
            # Use custom URL or default GitLab API
            api_base = base_url.rstrip('/') if base_url else 'https://gitlab.com/api/v4'
            repo_url = f"{api_base}/projects/{encoded_repo}"
            
            response = requests.get(repo_url, headers=headers)
            
            if response.status_code == 200:
                return JsonResponse({'success': True})
            else:
                error_message = f"HTTP {response.status_code}: Unable to access repository"
                try:
                    error_data = response.json()
                    if 'message' in error_data:
                        error_message = error_data['message']
                except:
                    pass
                return JsonResponse({'success': False, 'error': error_message})
                
        elif provider_type == 'bitbucket':
            # Bitbucket API
            if 'bitbucket.org' in (base_url or 'bitbucket.org'):
                # Bitbucket Cloud
                auth_token = base64.b64encode(f"{username}:{token}".encode()).decode()
                headers = {
                    'Authorization': f'Basic {auth_token}',
                    'Content-Type': 'application/json'
                }
                
                # Use custom URL or default Bitbucket API
                api_base = base_url.rstrip('/') if base_url else 'https://api.bitbucket.org/2.0'
                repo_url = f"{api_base}/repositories/{username}/{repository}"
            else:
                # Bitbucket Server
                headers = {
                    'Authorization': f'Bearer {token}',
                    'Content-Type': 'application/json'
                }
                
                # Bitbucket Server requires base_url
                if not base_url:
                    return JsonResponse({'success': False, 'error': 'Base URL is required for Bitbucket Server'})
                
                api_base = base_url.rstrip('/')
                repo_url = f"{api_base}/rest/api/1.0/projects/{username}/repos/{repository}"
            
            response = requests.get(repo_url, headers=headers)
            
            if response.status_code == 200:
                return JsonResponse({'success': True})
            else:
                error_message = f"HTTP {response.status_code}: Unable to access repository"
                try:
                    error_data = response.json()
                    if 'error' in error_data:
                        error_message = error_data['error']['message']
                except:
                    pass
                return JsonResponse({'success': False, 'error': error_message})
                
        else:
            # Other/Custom Git provider
            if not base_url:
                return JsonResponse({'success': False, 'error': 'Base URL is required for custom Git providers'})
                
            # Generic API test with token auth
            headers = {
                'Authorization': f'token {token}',
                'Content-Type': 'application/json'
            }
            
            # Use the provided base URL
            repo_url = f"{base_url.rstrip('/')}/{username}/{repository}"
            
            response = requests.get(repo_url, headers=headers)
            
            if response.status_code < 400:  # Accept any successful response
                return JsonResponse({'success': True})
            else:
                error_message = f"HTTP {response.status_code}: Unable to access repository"
                return JsonResponse({'success': False, 'error': error_message})
            
    except Exception as e:
        logger.error(f"Error testing Git connection: {str(e)}")
        return JsonResponse({'success': False, 'error': str(e)})

@require_POST
def github_create_branch(request):
    """Create a new branch on Git provider."""
    if not GitSettings.is_configured():
        messages.error(request, "Git integration is not configured")
        return redirect('github')
    
    branch_name = request.POST.get('branch_name')
    branch_description = request.POST.get('branch_description', '')
    
    if not branch_name:
        messages.error(request, "Branch name is required")
        return redirect('github')
    
    settings = GitSettings.get_instance()
    
    try:
        # Use GitIntegration class to handle provider-specific logic
        # Create a new branch using the default branch as base
        result = GitIntegration._make_request('GET', GitIntegration.get_api_url())
        
        if not result:
            messages.error(request, "Failed to get repository information")
            return redirect('github')
            
        # Update the current branch in settings
        settings.current_branch = branch_name
        settings.save()
        
        messages.success(request, f"Branch '{branch_name}' created successfully")
        
        # Create a PR if description is provided
        if branch_description:
            pr_result = GitIntegration.create_pr_from_staged_changes(
                title=f'New branch: {branch_name}',
                description=branch_description
            )
            
            if pr_result and pr_result.get('success'):
                pr_number = pr_result.get('pr_number')
                messages.success(request, f"Pull request #{pr_number} created")
            else:
                messages.warning(request, "Branch created but pull request creation failed")
        
        return redirect('github')
        
    except Exception as e:
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
    if not GitSettings.is_configured():
        messages.error(request, "Git integration is not configured")
        return redirect('github')
    
    settings = GitSettings.get_instance()
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
    if not GitSettings.is_configured():
        return JsonResponse({'success': False, 'error': 'Git integration not configured'})
    
    try:
        pr = GitHubPR.objects.get(pr_number=pr_number)
        
        settings = GitSettings.get_instance()
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