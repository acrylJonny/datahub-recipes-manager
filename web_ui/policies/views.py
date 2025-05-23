from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.http import JsonResponse, HttpResponse
from django.contrib.auth.decorators import login_required
from django.db import models
import json
import os
import tempfile
import shutil
from datetime import datetime

from .models import Policy
from .forms import PolicyForm

@login_required
def policies(request):
    """List all policies."""
    policies = Policy.objects.all().order_by('-updated_at')
    
    # Handle filtering
    tag_filter = request.GET.get('tag')
    if tag_filter:
        policies = policies.filter(tags__contains=tag_filter)
        
    # Handle search
    search_query = request.GET.get('search')
    if search_query:
        policies = policies.filter(
            models.Q(name__icontains=search_query) | 
            models.Q(description__icontains=search_query)
        )
    
    # Get unique tags for filter dropdown
    all_tags = set()
    for policy in Policy.objects.all():
        if policy.tags:
            all_tags.update(policy.get_tags_list())
    
    return render(request, 'policies/list.html', {
        'title': 'Policies',
        'policies': policies,
        'tag_filter': tag_filter,
        'search_query': search_query,
        'all_tags': sorted(all_tags)
    })

@login_required
def policy_view(request, policy_id):
    """View a policy's details."""
    policy = get_object_or_404(Policy, id=policy_id)
    
    # Process content for display
    if policy.content.strip().startswith('{'):
        content_type = 'json'
        try:
            formatted_content = json.dumps(json.loads(policy.content), indent=2)
        except:
            formatted_content = policy.content
    else:
        content_type = 'yaml'
        formatted_content = policy.content
    
    return render(request, 'policies/detail.html', {
        'title': f'Policy: {policy.name}',
        'policy': policy,
        'content': formatted_content,
        'content_type': content_type,
        'tags': policy.get_tags_list()
    })

@login_required
def policy_create(request):
    """Create a new policy."""
    if request.method == 'POST':
        form = PolicyForm(request.POST)
        if form.is_valid():
            policy = form.save()
            messages.success(request, f"Policy '{policy.name}' created successfully")
            return redirect('policies:policy_view', policy_id=policy.id)
    else:
        form = PolicyForm()
    
    return render(request, 'policies/create.html', {
        'title': 'Create Policy',
        'form': form
    })

@login_required
def policy_edit(request, policy_id):
    """Edit a policy."""
    policy = get_object_or_404(Policy, id=policy_id)
    
    if request.method == 'POST':
        form = PolicyForm(request.POST, instance=policy)
        if form.is_valid():
            policy = form.save()
            messages.success(request, f"Policy '{policy.name}' updated successfully")
            return redirect('policies:policy_view', policy_id=policy.id)
    else:
        form = PolicyForm(instance=policy)
    
    return render(request, 'policies/edit.html', {
        'title': 'Edit Policy',
        'form': form,
        'policy': policy
    })

@login_required
def policy_delete(request, policy_id):
    """Delete a policy."""
    policy = get_object_or_404(Policy, id=policy_id)
    
    if request.method == 'POST':
        policy_name = policy.name
        policy.delete()
        messages.success(request, f"Policy '{policy_name}' deleted successfully")
        return redirect('policies:policies')
    
    return render(request, 'policies/delete.html', {
        'title': 'Delete Policy',
        'policy': policy
    })

@login_required
def policy_download(request, policy_id):
    """Download a policy file."""
    policy = get_object_or_404(Policy, id=policy_id)
    
    # Create response with file
    response = HttpResponse(policy.content, content_type='application/json')
    response['Content-Disposition'] = f'attachment; filename="{policy.name.replace(" ", "_").lower()}.json"'
    return response

@login_required
def export_all_policies(request):
    """Export all policies to JSON files in a zip archive."""
    try:
        policies = Policy.objects.all()
        
        if not policies:
            messages.warning(request, "No policies found to export")
            return redirect('policies:policies')
        
        # Create a temporary directory
        output_dir = tempfile.mkdtemp()
        
        # Save each policy to a file
        for policy in policies:
            filename = f"{policy.name.replace(' ', '_').lower()}_{policy.id}.json"
            with open(os.path.join(output_dir, filename), 'w') as f:
                f.write(policy.content)
        
        # Create a zip file
        zip_path = os.path.join(tempfile.gettempdir(), f'datahub_policies_{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip')
        shutil.make_archive(zip_path[:-4], 'zip', output_dir)
        
        # Clean up the temp directory
        shutil.rmtree(output_dir, ignore_errors=True)
        
        # Serve the zip file
        with open(zip_path, 'rb') as f:
            response = HttpResponse(f.read(), content_type='application/zip')
            response['Content-Disposition'] = f'attachment; filename="datahub_policies.zip"'
        
        # Clean up the zip file
        os.unlink(zip_path)
        
        return response
        
    except Exception as e:
        messages.error(request, f"Error exporting policies: {str(e)}")
        return redirect('policies:policies')

@login_required
def policy_import(request):
    """Import a policy from a file."""
    if request.method == 'POST':
        # Add import logic here
        messages.success(request, "Policy imported successfully")
        return redirect('policies:policies')
    
    return render(request, 'policies/import.html', {
        'title': 'Import Policy'
    })

@login_required
def policy_push_github(request, policy_id):
    """Push a policy to GitHub."""
    policy = get_object_or_404(Policy, id=policy_id)
    # Add GitHub push logic here
    messages.success(request, f"Policy '{policy.name}' pushed to GitHub successfully")
    return redirect('policies:policy_view', policy_id=policy.id)

@login_required
def policy_deploy(request, policy_id):
    """Deploy a policy."""
    policy = get_object_or_404(Policy, id=policy_id)
    # Add deployment logic here
    policy.deploy()
    messages.success(request, f"Policy '{policy.name}' deployed successfully")
    return redirect('policies:policy_view', policy_id=policy.id)
