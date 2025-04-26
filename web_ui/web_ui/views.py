from django.shortcuts import render, redirect, get_object_or_404
from django.http import HttpResponse, JsonResponse
from django.contrib import messages
from django.views.decorators.http import require_POST
import os
import sys
import json
import yaml
import subprocess
from datetime import datetime
import tempfile
import shutil
import logging
from django.db import models
import re
from django.contrib.auth.decorators import login_required

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Import custom forms
from .forms import RecipeForm, RecipeImportForm, PolicyForm, PolicyImportForm, RecipeTemplateForm, RecipeDeployForm, RecipeTemplateImportForm, EnvVarsTemplateForm, EnvVarsInstanceForm

# Try to import the DataHub client
try:
    from utils.datahub_rest_client import DataHubRestClient
    DATAHUB_CLIENT_AVAILABLE = True
except ImportError:
    DATAHUB_CLIENT_AVAILABLE = False

from .models import AppSettings, GitHubSettings, LogEntry, RecipeTemplate, EnvVarsTemplate, EnvVarsInstance

logger = logging.getLogger(__name__)

def get_datahub_client():
    """Get a DataHub client instance if possible."""
    if not DATAHUB_CLIENT_AVAILABLE:
        logger.warning("DataHub client is not available")
        LogEntry.warning("DataHub client is not available", source="web_ui.views")
        return None
    
    datahub_url = AppSettings.get('datahub_url', os.environ.get('DATAHUB_GMS_URL', ''))
    datahub_token = AppSettings.get('datahub_token', os.environ.get('DATAHUB_TOKEN', ''))
    
    if not datahub_url:
        logger.warning("DataHub URL is not configured")
        LogEntry.warning("DataHub URL is not configured", source="web_ui.views")
        return None
    
    logger.debug(f"Creating DataHub client with URL: {datahub_url}")
    LogEntry.debug(f"Creating DataHub client with URL: {datahub_url}", source="web_ui.views")
    return DataHubRestClient(server_url=datahub_url, token=datahub_token)

def index(request):
    """Main dashboard view."""
    client = get_datahub_client()
    connected = False
    recipes_count = 0
    active_schedules_count = 0
    policies_count = 0
    recent_recipes = []
    recent_policies = []
    
    if client:
        try:
            connected = client.test_connection()
            
            if connected:
                # Get recipes
                recipes = client.list_ingestion_sources()
                if recipes:
                    recipes_count = len(recipes)
                    active_schedules_count = sum(1 for r in recipes if r.get('schedule'))
                    
                    # Get 5 most recent recipes
                    recent_recipes = sorted(
                        recipes, 
                        key=lambda x: x.get('lastUpdated', 0), 
                        reverse=True
                    )[:5]
                
                # Get policies
                try:
                    all_policies = client.list_policies(limit=100)
                    if all_policies:
                        # Process policies to extract IDs from URNs if needed
                        valid_policies = []
                        for policy in all_policies:
                            # If no ID but has URN, extract ID from URN
                            if not policy.get('id') and policy.get('urn'):
                                # Extract ID from URN (format: urn:li:policy:<id>)
                                parts = policy.get('urn').split(':')
                                if len(parts) >= 4:
                                    policy['id'] = parts[3]
                            
                            # Include policies with an ID (either original or extracted)
                            if policy.get('id'):
                                valid_policies.append(policy)
                                
                        policies_count = len(valid_policies)
                        
                        # Get 5 most recent policies with valid IDs
                        recent_policies = valid_policies[:5]  # Assuming they're already sorted
                except Exception as e:
                    logger.error(f"Error fetching policies for dashboard: {str(e)}")
                    # Policies might not be available in older DataHub versions
                    pass
        except Exception as e:
            messages.error(request, f"Error connecting to DataHub: {str(e)}")
    
    return render(request, 'dashboard.html', {
        'title': 'Dashboard',
        'connected': connected,
        'recipes_count': recipes_count,
        'active_schedules_count': active_schedules_count,
        'policies_count': policies_count,
        'recent_recipes': recent_recipes,
        'recent_policies': recent_policies
    })

def recipes(request):
    """List all recipes."""
    client = get_datahub_client()
    recipes_list = []
    connected = False
    
    if client and client.test_connection():
        connected = True
        try:
            recipes_list = client.list_ingestion_sources()
            
            # Process each recipe to format schedule and status
            for recipe in recipes_list:
                # Format schedule nicely
                schedule_data = recipe.get('schedule', {})
                if schedule_data:
                    if isinstance(schedule_data, dict):
                        cron = schedule_data.get('interval', '')
                        timezone = schedule_data.get('timezone', 'UTC')
                        if cron:
                            recipe['formatted_schedule'] = f"{cron} ({timezone})"
                            # If we have a schedule, the recipe is enabled
                            recipe['enabled'] = True
                        else:
                            recipe['formatted_schedule'] = None
                            recipe['enabled'] = False
                    # Handle case where schedule might be a string
                    elif isinstance(schedule_data, str):
                        recipe['formatted_schedule'] = schedule_data
                        recipe['enabled'] = True
                else:
                    recipe['formatted_schedule'] = None
                    recipe['enabled'] = False
                
                # Store the original schedule data for reference
                recipe['schedule_data'] = schedule_data
            
            # Sort by name
            recipes_list.sort(key=lambda x: x.get('name', '').lower())
        except Exception as e:
            messages.error(request, f"Error fetching recipes: {str(e)}")
    else:
        messages.warning(request, "Not connected to DataHub")
    
    # Get refresh rate from settings
    refresh_rate = AppSettings.get_int('refresh_rate', 60)
    
    # Store connection status in session
    request.session['datahub_connected'] = connected
    
    return render(request, 'recipes/list.html', {
        'title': 'Recipes',
        'recipes': recipes_list,
        'refresh_rate': refresh_rate,
        'connection': {'connected': connected}
    })

def recipe_create(request):
    """Create a new recipe."""
    if request.method == 'POST':
        form = RecipeForm(request.POST)
        if form.is_valid():
            client = get_datahub_client()
            if client and client.test_connection():
                try:
                    # Parse recipe content
                    recipe_content = form.cleaned_data['recipe_content']
                    try:
                        if recipe_content.strip().startswith('{'):
                            recipe_json = json.loads(recipe_content)
                        else:
                            # If content is in YAML format, convert to JSON before sending to DataHub
                            import yaml
                            recipe_json = yaml.safe_load(recipe_content)
                    except Exception as e:
                        messages.error(request, f"Invalid recipe format: {str(e)}")
                        return render(request, 'recipes/create.html', {'form': form})
                    
                    # Prepare schedule if provided
                    schedule = None
                    if form.cleaned_data['schedule_cron']:
                        schedule = {
                            'interval': form.cleaned_data['schedule_cron'],
                            'timezone': form.cleaned_data['schedule_timezone'] or 'UTC'
                        }
                    
                    # Create the recipe
                    result = client.create_ingestion_source(
                        name=form.cleaned_data['recipe_name'],
                        type=form.cleaned_data['recipe_type'],
                        recipe=recipe_json,
                        source_id=form.cleaned_data['recipe_id'],
                        schedule=schedule,
                        executor_id="default"
                    )
                    
                    if result:
                        messages.success(request, "Recipe created successfully")
                        return redirect('recipes')
                    else:
                        messages.error(request, "Failed to create recipe")
                except Exception as e:
                    messages.error(request, f"Error creating recipe: {str(e)}")
            else:
                messages.error(request, "Not connected to DataHub")
    else:
        form = RecipeForm()
    
    return render(request, 'recipes/create.html', {'form': form})

def recipe_edit(request, recipe_id):
    """Edit an existing recipe."""
    client = get_datahub_client()
    if not client or not client.test_connection():
        messages.error(request, "Not connected to DataHub")
        return redirect('recipes')
    
    # Get the recipe
    recipe_data = client.get_ingestion_source(recipe_id)
    if not recipe_data:
        messages.error(request, f"Recipe {recipe_id} not found")
        return redirect('recipes')
    
    # Extract recipe content
    recipe_content = recipe_data.get('config', {}).get('recipe', '{}')
    if isinstance(recipe_content, dict):
        try:
            # Convert to YAML for display by default
            import yaml
            recipe_content = yaml.dump(recipe_content, sort_keys=False, default_flow_style=False)
        except:
            recipe_content = json.dumps(recipe_content, indent=2)
    
    # Extract schedule information
    schedule_info = recipe_data.get('schedule', {})
    schedule_cron = schedule_info.get('interval', '')
    schedule_timezone = schedule_info.get('timezone', 'UTC')
    
    # Get environment variables
    env_vars = get_recipe_environment_variables(recipe_id, recipe_content)
    
    if request.method == 'POST':
        form = RecipeForm(request.POST)
        
        # Check if this is an environment variables update
        if 'save_env_vars' in request.POST:
            try:
                env_vars_json = request.POST.get('environment_variables', '{}')
                env_vars_data = json.loads(env_vars_json)
                update_recipe_environment_variables(recipe_id, env_vars_data)
                messages.success(request, "Environment variables updated successfully")
                return redirect('recipe_edit', recipe_id=recipe_id)
            except Exception as e:
                messages.error(request, f"Error updating environment variables: {str(e)}")
                return redirect('recipe_edit', recipe_id=recipe_id)
        
        # Handle regular form submission
        if form.is_valid():
            try:
                # Parse recipe content
                updated_recipe_content = form.cleaned_data['recipe_content']
                try:
                    if updated_recipe_content.strip().startswith('{'):
                        updated_recipe_json = json.loads(updated_recipe_content)
                    else:
                        # If content is in YAML format, convert to JSON before sending to DataHub
                        import yaml
                        updated_recipe_json = yaml.safe_load(updated_recipe_content)
                except Exception as e:
                    messages.error(request, f"Invalid recipe format: {str(e)}")
                    return render(request, 'recipes/edit.html', {
                        'form': form, 
                        'recipe_id': recipe_id,
                        'env_vars': env_vars
                    })
                
                # Apply environment variables
                env_vars_data = request.POST.get('environment_variables')
                if env_vars_data:
                    try:
                        env_vars_dict = json.loads(env_vars_data)
                        # Store all environment variables
                        update_recipe_environment_variables(recipe_id, env_vars_dict)
                        
                        # Only replace values in the recipe if needed
                        # This allows users to use placeholders in recipe that will be
                        # replaced at runtime with actual values
                        if form.cleaned_data.get('replace_env_vars', False):
                            updated_recipe_json = replace_env_vars_with_values(updated_recipe_json, env_vars_dict)
                    except Exception as e:
                        logger.error(f"Error processing environment variables: {str(e)}")
                
                # Prepare schedule if provided
                schedule = None
                if form.cleaned_data['schedule_cron']:
                    schedule = {
                        'interval': form.cleaned_data['schedule_cron'],
                        'timezone': form.cleaned_data['schedule_timezone'] or 'UTC'
                    }
                
                # Update the recipe
                result = client.patch_ingestion_source(
                    urn=recipe_id,
                    name=form.cleaned_data['recipe_name'],
                    recipe=updated_recipe_json,
                    schedule=schedule
                )
                
                if result:
                    messages.success(request, "Recipe updated successfully")
                    return redirect('recipes')
                else:
                    messages.error(request, "Failed to update recipe")
            except Exception as e:
                messages.error(request, f"Error updating recipe: {str(e)}")
    else:
        # Initialize form with existing values
        form = RecipeForm(initial={
            'recipe_id': recipe_id,
            'recipe_name': recipe_data.get('name', ''),
            'recipe_type': recipe_data.get('type', ''),
            'description': recipe_data.get('description', ''),
            'schedule_cron': schedule_cron,
            'schedule_timezone': schedule_timezone,
            'recipe_content': recipe_content
        })
    
    return render(request, 'recipes/edit.html', {
        'form': form, 
        'recipe_id': recipe_id,
        'env_vars': env_vars,
        'env_vars_json': json.dumps(env_vars)
    })

def recipe_import(request):
    """Import a recipe from a file."""
    if request.method == 'POST':
        form = RecipeImportForm(request.POST, request.FILES)
        if form.is_valid():
            recipe_file = request.FILES['recipe_file']
            
            try:
                content = recipe_file.read().decode('utf-8')
                
                # Determine if JSON or YAML
                if recipe_file.name.endswith('.json'):
                    recipe = json.loads(content)
                else:
                    recipe = yaml.safe_load(content)
                
                # Create a pre-filled form for the recipe
                initial_data = {
                    'recipe_id': recipe.get('source_id', ''),
                    'recipe_name': recipe.get('source_name', ''),
                    'recipe_type': recipe.get('source', {}).get('type', ''),
                    'description': recipe.get('description', ''),
                    'recipe_content': content
                }
                
                recipe_form = RecipeForm(initial=initial_data)
                return render(request, 'recipes/create.html', {'form': recipe_form, 'imported': True})
                
            except Exception as e:
                messages.error(request, f"Error parsing recipe file: {str(e)}")
    else:
        form = RecipeImportForm()
    
    return render(request, 'recipes/import.html', {'form': form})

@require_POST
def recipe_delete(request, recipe_id):
    """Delete a recipe."""
    client = get_datahub_client()
    if not client or not client.test_connection():
        return JsonResponse({'success': False, 'message': 'Not connected to DataHub'})
    
    try:
        result = client.delete_ingestion_source(recipe_id)
        if result:
            return JsonResponse({'success': True})
        else:
            return JsonResponse({'success': False, 'message': 'Failed to delete recipe'})
    except Exception as e:
        return JsonResponse({'success': False, 'message': str(e)})

def recipe_run(request, recipe_id):
    """Run a recipe immediately."""
    client = get_datahub_client()
    if not client or not client.test_connection():
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse({'success': False, 'message': 'Not connected to DataHub'})
        messages.error(request, "Not connected to DataHub")
        return redirect('recipes')
    
    try:
        result = client.trigger_ingestion(recipe_id)
        if result:
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return JsonResponse({'success': True, 'execution_id': result})
            messages.success(request, "Recipe execution started successfully!")
            return redirect('recipes')
        else:
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return JsonResponse({'success': False, 'message': 'Failed to trigger ingestion'})
            messages.error(request, "Failed to run recipe")
            return redirect('recipes')
    except Exception as e:
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse({'success': False, 'message': str(e)})
        messages.error(request, f"Error running recipe: {str(e)}")
        return redirect('recipes')

def recipe_download(request, recipe_id):
    """Download a recipe as JSON."""
    client = get_datahub_client()
    if not client or not client.test_connection():
        messages.error(request, "Not connected to DataHub")
        return redirect('recipes')
    
    # Get the recipe
    recipe_data = client.get_ingestion_source(recipe_id)
    if not recipe_data:
        messages.error(request, f"Recipe {recipe_id} not found")
        return redirect('recipes')
    
    # Extract recipe content
    recipe_content = recipe_data.get('config', {}).get('recipe', '{}')
    if isinstance(recipe_content, str):
        try:
            recipe_content = json.loads(recipe_content)
        except:
            pass
    
    # Prepare response
    response = HttpResponse(json.dumps(recipe_content, indent=2), content_type='application/json')
    response['Content-Disposition'] = f'attachment; filename="{recipe_id}.json"'
    return response

def policies(request):
    """List all policies."""
    client = get_datahub_client()
    policies_list = []
    connected = False
    
    if client and client.test_connection():
        connected = True
        try:
            all_policies = client.list_policies(limit=100)
            
            # Process each policy to ensure it has an ID
            for policy in all_policies:
                # If no ID but has URN, extract ID from URN
                if not policy.get('id') and policy.get('urn'):
                    # Extract ID from URN (format: urn:li:policy:<id>)
                    parts = policy.get('urn').split(':')
                    if len(parts) >= 4:
                        policy['id'] = parts[3]
                
                # Only include policies with an ID (either original or extracted)
                if policy.get('id'):
                    policies_list.append(policy)
                    
        except Exception as e:
            messages.error(request, f"Error fetching policies: {str(e)}")
    else:
        messages.warning(request, "Not connected to DataHub")
    
    # Get refresh rate from settings
    refresh_rate = AppSettings.get_int('refresh_rate', 60)
    
    return render(request, 'policies/list.html', {
        'title': 'Policies',
        'policies': policies_list,
        'connected': connected,
        'refresh_rate': refresh_rate
    })

def policy_create(request):
    """Create a new policy."""
    if request.method == 'POST':
        form = PolicyForm(request.POST)
        if form.is_valid():
            client = get_datahub_client()
            if client and client.test_connection():
                try:
                    # Parse JSON fields
                    try:
                        resources = json.loads(form.cleaned_data['policy_resources'] or '[]')
                        privileges = json.loads(form.cleaned_data['policy_privileges'] or '[]')
                        actors = json.loads(form.cleaned_data['policy_actors'] or '[]')
                    except Exception as e:
                        messages.error(request, f"Invalid JSON format: {str(e)}")
                        return render(request, 'policies/create.html', {'form': form})
                    
                    # Create policy data structure
                    policy_data = {
                        'name': form.cleaned_data['policy_name'],
                        'description': form.cleaned_data['description'],
                        'type': form.cleaned_data['policy_type'],
                        'state': form.cleaned_data['policy_state'],
                        'resources': resources,
                        'privileges': privileges,
                        'actors': actors
                    }
                    
                    # Create the policy
                    result = client.create_policy(policy_data)
                    
                    if result:
                        messages.success(request, "Policy created successfully")
                        return redirect('policies')
                    else:
                        messages.error(request, "Failed to create policy")
                except Exception as e:
                    messages.error(request, f"Error creating policy: {str(e)}")
            else:
                messages.error(request, "Not connected to DataHub")
    else:
        form = PolicyForm()
    
    return render(request, 'policies/create.html', {'form': form})

def policy_edit(request, policy_id):
    """Edit a policy."""
    client = get_datahub_client()
    policy = None
    
    if not client or not client.test_connection():
        messages.warning(request, "Not connected to DataHub")
        return redirect('policies')
    
    try:
        # First try to fetch by ID directly
        policy = client.get_policy(policy_id)
        
        # If not found, try with URN format
        if not policy and not policy_id.startswith('urn:'):
            policy = client.get_policy(f"urn:li:policy:{policy_id}")
            
        if not policy:
            messages.error(request, f"Policy with ID {policy_id} not found")
            return redirect('policies')
        
        # Get the policy ID from URN if needed
        actual_policy_id = policy.get('id')
        if not actual_policy_id and policy.get('urn'):
            # Extract ID from URN (format: urn:li:policy:<id>)
            parts = policy.get('urn').split(':')
            if len(parts) >= 4:
                actual_policy_id = parts[3]
                policy['id'] = actual_policy_id
            
        if request.method == 'POST':
            # Process form data
            updated_policy = {
                'id': actual_policy_id,
                'name': request.POST.get('name'),
                'description': request.POST.get('description', ''),
                'type': request.POST.get('type'),
                'state': request.POST.get('state'),
            }
            
            # Handle JSON fields
            for field in ['resources', 'privileges', 'actors']:
                try:
                    json_str = request.POST.get(f'{field}_json', '[]')
                    updated_policy[field] = json.loads(json_str)
                except json.JSONDecodeError:
                    messages.error(request, f"Invalid JSON in {field} field")
                    return render(request, 'policies/edit.html', {
                        'title': 'Edit Policy',
                        'policy': policy
                    })
            
            # Update the policy
            try:
                result = client.update_policy(actual_policy_id, updated_policy)
                if result:
                    messages.success(request, f"Policy '{updated_policy['name']}' updated successfully")
                    return redirect('policy_view', policy_id=actual_policy_id)
                else:
                    messages.error(request, "Failed to update policy")
            except Exception as e:
                messages.error(request, f"Error updating policy: {str(e)}")
                logger.error(f"Error updating policy: {str(e)}")
    except Exception as e:
        messages.error(request, f"Error retrieving policy: {str(e)}")
        logger.error(f"Error retrieving policy: {str(e)}")
        return redirect('policies')
    
    # Format JSON fields for display in form
    for field in ['resources', 'privileges', 'actors']:
        if field in policy and policy[field]:
            policy[f"{field}_json"] = json.dumps(policy[field], indent=2)
    
    return render(request, 'policies/edit.html', {
        'title': 'Edit Policy',
        'policy': policy
    })

def policy_import(request):
    """Import a policy from a file."""
    if request.method == 'POST':
        form = PolicyImportForm(request.POST, request.FILES)
        if form.is_valid():
            policy_file = request.FILES['policy_file']
            
            try:
                # Read and parse the policy file
                content = policy_file.read().decode('utf-8')
                policy = json.loads(content)
                
                # Run the import_policy.py script
                with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as temp:
                    temp.write(content.encode('utf-8'))
                    temp_path = temp.name
                
                script_path = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                    'scripts',
                    'import_policy.py'
                )
                
                try:
                    result = subprocess.run(
                        [sys.executable, script_path, '--input-file', temp_path],
                        capture_output=True,
                        text=True,
                        check=True
                    )
                    messages.success(request, "Policy imported successfully")
                    return redirect('policies')
                finally:
                    # Clean up temp file
                    try:
                        os.unlink(temp_path)
                    except:
                        pass
                    
            except Exception as e:
                messages.error(request, f"Error importing policy: {str(e)}")
    else:
        form = PolicyImportForm()
    
    return render(request, 'policies/import.html', {'form': form})

@require_POST
def policy_delete(request, policy_id):
    """Delete a policy."""
    client = get_datahub_client()
    if not client or not client.test_connection():
        messages.error(request, "Not connected to DataHub")
        return redirect('policies')
    
    try:
        result = client.delete_policy(policy_id)
        if result:
            messages.success(request, f"Policy '{policy_id}' deleted successfully")
            return redirect('policies')
        else:
            messages.error(request, "Failed to delete policy")
            return redirect('policies')
    except Exception as e:
        messages.error(request, f"Error deleting policy: {str(e)}")
        return redirect('policies')

def policy_download(request, policy_id):
    """Download a policy as JSON."""
    client = get_datahub_client()
    if not client or not client.test_connection():
        messages.error(request, "Not connected to DataHub")
        return redirect('policies')
    
    # Get the policy
    policy_data = client.get_policy(policy_id)
    if not policy_data:
        messages.error(request, f"Policy {policy_id} not found")
        return redirect('policies')
    
    # Extract file name from policy name or ID
    filename = f"policy_{policy_data.get('name', policy_id).replace(' ', '_').lower()}.json"
    
    # Prepare response
    response = HttpResponse(json.dumps(policy_data, indent=2), content_type='application/json')
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response

def policy_detail(request, policy_id):
    """Display policy details."""
    client = get_datahub_client()
    policy = None
    
    if client and client.test_connection():
        try:
            # First try to get policy by ID directly
            policy = client.get_policy(policy_id)
            
            # If not found, try to get policy by URN (in case policy_id is actually a URN)
            if not policy and policy_id.startswith('urn:'):
                parts = policy_id.split(':')
                if len(parts) >= 4:
                    actual_policy_id = parts[3]
                    policy = client.get_policy(actual_policy_id)
                    
            if not policy:
                messages.error(request, f"Policy with ID '{policy_id}' not found")
                return redirect('policies')
                
            # Ensure policy has an ID
            if not policy.get('id') and 'urn' in policy:
                policy_urn = policy['urn']
                parts = policy_urn.split(':')
                if len(parts) >= 4:
                    policy['id'] = parts[3]
            
            # Prepare JSON representations for display
            policy_json = json.dumps(policy, indent=2)
            resources_json = json.dumps(policy.get('resources', []), indent=2)
            privileges_json = json.dumps(policy.get('privileges', []), indent=2)
            actors_json = json.dumps(policy.get('actors', {}), indent=2)
                
        except Exception as e:
            messages.error(request, f"Error fetching policy: {str(e)}")
            return redirect('policies')
    else:
        messages.warning(request, "Not connected to DataHub")
        return redirect('home')
    
    return render(request, 'policies/detail.html', {
        'title': f'Policy: {policy.get("name", "")}',
        'policy': policy,
        'policy_json': policy_json,
        'resources_json': resources_json,
        'privileges_json': privileges_json,
        'actors_json': actors_json
    })

def policy_export_all(request):
    """Export all policies to JSON files in a zip archive."""
    client = get_datahub_client()
    if not client or not client.test_connection():
        messages.error(request, "Not connected to DataHub")
        return redirect('policies')
    
    try:
        # Create a temporary directory for policies
        output_dir = tempfile.mkdtemp()
        
        # Get the export script path
        script_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'scripts',
            'export_policy.py'
        )
        
        # Run the export script
        result = subprocess.run(
            [sys.executable, script_path, '--output-dir', output_dir],
            capture_output=True,
            text=True,
            check=True
        )
        
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
        return redirect('policies')

def export_all_policies(request):
    """Alias for policy_export_all for consistency."""
    return policy_export_all(request)

def export_all_recipes(request):
    """Export all recipes to JSON files in a zip archive."""
    client = get_datahub_client()
    if not client or not client.test_connection():
        messages.error(request, "Not connected to DataHub")
        return redirect('recipes')
    
    try:
        # Create a temporary directory for recipes
        output_dir = tempfile.mkdtemp()
        
        # Get all recipes
        recipes_list = client.list_ingestion_sources()
        
        # Save each recipe to a file
        for recipe in recipes_list:
            recipe_id = recipe.get('urn', '').split(':')[-1] if recipe.get('urn') else recipe.get('id', 'unknown')
            recipe_name = recipe.get('name', 'Unnamed').replace(' ', '_').lower()
            filename = f"{recipe_name}_{recipe_id}.json"
            
            # Extract recipe content
            recipe_content = recipe.get('config', {}).get('recipe', '{}')
            if isinstance(recipe_content, str):
                try:
                    recipe_content = json.loads(recipe_content)
                except:
                    pass
            
            # Write recipe to file
            with open(os.path.join(output_dir, filename), 'w') as f:
                json.dump(recipe_content, f, indent=2)
        
        # Create a zip file
        zip_path = os.path.join(tempfile.gettempdir(), f'datahub_recipes_{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip')
        shutil.make_archive(zip_path[:-4], 'zip', output_dir)
        
        # Clean up the temp directory
        shutil.rmtree(output_dir, ignore_errors=True)
        
        # Serve the zip file
        with open(zip_path, 'rb') as f:
            response = HttpResponse(f.read(), content_type='application/zip')
            response['Content-Disposition'] = f'attachment; filename="datahub_recipes.zip"'
        
        # Clean up the zip file
        os.unlink(zip_path)
        
        return response
        
    except Exception as e:
        messages.error(request, f"Error exporting recipes: {str(e)}")
        return redirect('recipes')

def export_all_templates(request):
    """Export all recipe templates to JSON files in a zip archive."""
    try:
        # Get all recipe templates from the database
        templates = RecipeTemplate.objects.all()
        
        if not templates:
            messages.warning(request, "No recipe templates found to export")
            return redirect('recipe_templates')
        
        # Create a temporary directory for templates
        output_dir = tempfile.mkdtemp()
        
        # Save each template to a file
        for template in templates:
            template_name = template.name.replace(' ', '_').lower()
            filename = f"{template_name}_{template.id}.json"
            
            # Extract template content
            template_content = template.content
            if isinstance(template_content, str):
                try:
                    template_content = json.loads(template_content)
                except:
                    pass
            
            # Write template to file
            with open(os.path.join(output_dir, filename), 'w') as f:
                json.dump(template_content, f, indent=2)
        
        # Create a zip file
        zip_path = os.path.join(tempfile.gettempdir(), f'datahub_templates_{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip')
        shutil.make_archive(zip_path[:-4], 'zip', output_dir)
        
        # Clean up the temp directory
        shutil.rmtree(output_dir, ignore_errors=True)
        
        # Serve the zip file
        with open(zip_path, 'rb') as f:
            response = HttpResponse(f.read(), content_type='application/zip')
            response['Content-Disposition'] = f'attachment; filename="datahub_templates.zip"'
        
        # Clean up the zip file
        os.unlink(zip_path)
        
        return response
        
    except Exception as e:
        messages.error(request, f"Error exporting templates: {str(e)}")
        return redirect('recipe_templates')

def settings(request):
    """Settings page."""
    # Initialize the config dictionary
    config = {
        'datahub_url': AppSettings.get('datahub_url', ''),
        'datahub_token': AppSettings.get('datahub_token', ''),
        'verify_ssl': AppSettings.get_bool('verify_ssl', True),
        
        # Policy settings
        'policy_export_dir': AppSettings.get('policy_export_dir', ''),
        'default_policy_type': AppSettings.get('default_policy_type', 'METADATA'),
        'validate_on_import': AppSettings.get_bool('validate_on_import', True),
        'auto_backup_policies': AppSettings.get_bool('auto_backup_policies', True),
        
        # Recipe settings
        'recipe_dir': AppSettings.get('recipe_dir', ''),
        'default_schedule': AppSettings.get('default_schedule', '0 0 * * *'),
        'auto_enable_recipes': AppSettings.get_bool('auto_enable_recipes', False),
        
        # Advanced settings
        'log_level': AppSettings.get('log_level', 'INFO'),
        'timeout': AppSettings.get_int('timeout', 30),
        'debug_mode': AppSettings.get_bool('debug_mode', False),
        'refresh_rate': AppSettings.get_int('refresh_rate', 60),
        
        # GitHub settings
        'github_token': GitHubSettings.get_token(),
        'github_repository': GitHubSettings.get_repository(),
        'github_username': GitHubSettings.get_username(),
    }
    
    # Get connection status
    client = get_datahub_client()
    connected = client and client.test_connection()
    
    if request.method == 'POST':
        # Get the section being updated
        section = request.POST.get('section', 'connection')
        
        if section == 'connection' or section == 'datahub_connection':
            # Update connection settings
            datahub_url = request.POST.get('datahub_url', '')
            datahub_token = request.POST.get('datahub_token', '')
            verify_ssl = 'verify_ssl' in request.POST
            
            # Save settings to database
            AppSettings.set('datahub_url', datahub_url)
            if datahub_token:  # Only update token if provided
                AppSettings.set('datahub_token', datahub_token)
            AppSettings.set('verify_ssl', 'true' if verify_ssl else 'false')
            
            # Update environment variables for current session
            os.environ['DATAHUB_GMS_URL'] = datahub_url
            if datahub_token:
                os.environ['DATAHUB_TOKEN'] = datahub_token
            
            # Test connection if requested
            if 'test_connection' in request.POST:
                if client and client.test_connection():
                    messages.success(request, "Successfully connected to DataHub!")
                    connected = True
                else:
                    messages.error(request, "Failed to connect to DataHub. Please check your settings.")
                    connected = False
            else:
                messages.success(request, "DataHub connection settings updated")
        
        elif section == 'policy_settings':
            # Update policy settings
            policy_export_dir = request.POST.get('policy_export_dir', '')
            default_policy_type = request.POST.get('default_policy_type', 'METADATA')
            validate_on_import = 'validate_on_import' in request.POST
            auto_backup_policies = 'auto_backup_policies' in request.POST
            
            # Save settings to database
            AppSettings.set('policy_export_dir', policy_export_dir)
            AppSettings.set('default_policy_type', default_policy_type)
            AppSettings.set('validate_on_import', 'true' if validate_on_import else 'false')
            AppSettings.set('auto_backup_policies', 'true' if auto_backup_policies else 'false')
            
            messages.success(request, "Policy settings updated")
        
        elif section == 'recipe_settings':
            # Update recipe settings
            recipe_dir = request.POST.get('recipe_dir', '')
            default_schedule = request.POST.get('default_schedule', '0 0 * * *')
            auto_enable_recipes = 'auto_enable_recipes' in request.POST
            
            # Save settings to database
            AppSettings.set('recipe_dir', recipe_dir)
            AppSettings.set('default_schedule', default_schedule)
            AppSettings.set('auto_enable_recipes', 'true' if auto_enable_recipes else 'false')
            
            messages.success(request, "Recipe settings updated")
        
        elif section == 'advanced_settings':
            # Update advanced settings
            timeout = request.POST.get('timeout', '30')
            log_level = request.POST.get('log_level', 'INFO')
            debug_mode = 'debug_mode' in request.POST
            refresh_rate = request.POST.get('refresh_rate', '60')
            
            # Validate and save settings
            try:
                timeout = int(timeout)
                if timeout < 5:
                    timeout = 5
                elif timeout > 300:
                    timeout = 300
                AppSettings.set('timeout', str(timeout))
                
                refresh_rate = int(refresh_rate)
                if refresh_rate < 0:
                    refresh_rate = 0
                elif refresh_rate > 3600:
                    refresh_rate = 3600
                AppSettings.set('refresh_rate', str(refresh_rate))
                
                AppSettings.set('log_level', log_level)
                AppSettings.set('debug_mode', 'true' if debug_mode else 'false')
                
                messages.success(request, "Advanced settings updated")
            except ValueError:
                messages.error(request, "Invalid value for timeout or refresh rate")
        
        elif section == 'github_settings':
            # Update GitHub settings
            github_token = request.POST.get('github_token', '')
            github_repository = request.POST.get('github_repository', '')
            github_username = request.POST.get('github_username', '')
            
            # Save settings to database
            if github_token:  # Only update token if provided
                GitHubSettings.set_token(github_token)
            GitHubSettings.set_repository(github_repository)
            GitHubSettings.set_username(github_username)
            
            messages.success(request, "GitHub settings updated")
            
            # Test GitHub connection if requested
            if 'test_github_connection' in request.POST:
                # TODO: Implement GitHub connection test
                messages.info(request, "GitHub connection test functionality will be implemented soon")
        
        # Refresh config after updates
        config = {
            'datahub_url': AppSettings.get('datahub_url', ''),
            'datahub_token': AppSettings.get('datahub_token', ''),
            'verify_ssl': AppSettings.get_bool('verify_ssl', True),
            
            # Policy settings
            'policy_export_dir': AppSettings.get('policy_export_dir', ''),
            'default_policy_type': AppSettings.get('default_policy_type', 'METADATA'),
            'validate_on_import': AppSettings.get_bool('validate_on_import', True),
            'auto_backup_policies': AppSettings.get_bool('auto_backup_policies', True),
            
            # Recipe settings
            'recipe_dir': AppSettings.get('recipe_dir', ''),
            'default_schedule': AppSettings.get('default_schedule', '0 0 * * *'),
            'auto_enable_recipes': AppSettings.get_bool('auto_enable_recipes', False),
            
            # Advanced settings
            'log_level': AppSettings.get('log_level', 'INFO'),
            'timeout': AppSettings.get_int('timeout', 30),
            'debug_mode': AppSettings.get_bool('debug_mode', False),
            'refresh_rate': AppSettings.get_int('refresh_rate', 60),
            
            # GitHub settings
            'github_token': GitHubSettings.get_token(),
            'github_repository': GitHubSettings.get_repository(),
            'github_username': GitHubSettings.get_username(),
        }
    
    # If the session-based connection method is being used, update it
    if connected:
        request.session['datahub_connected'] = True
        request.session['datahub_url'] = config['datahub_url']
    else:
        request.session['datahub_connected'] = False
        if 'datahub_url' in request.session:
            del request.session['datahub_url']
    
    return render(request, 'settings.html', {
        'title': 'Settings',
        'config': config,
        'connected': connected,
        'github_configured': GitHubSettings.is_configured()
    })

def health(request):
    """Health check endpoint."""
    client = get_datahub_client()
    status = "OK" if client and client.test_connection() else "Disconnected"
    return HttpResponse(status)

def logs(request):
    """View application logs."""
    # Get the configured log level
    log_level = AppSettings.get('log_level', 'INFO')
    
    # Handle POST requests for clearing logs
    if request.method == 'POST' and request.POST.get('action') == 'clear_logs':
        try:
            # Get clear filters
            clear_level = request.POST.get('clear_level', '')
            clear_before_date = request.POST.get('clear_before_date', '')
            
            # Start with all logs
            logs_to_delete = LogEntry.objects.all()
            
            # Apply level filter if provided
            if clear_level:
                level_order = {'DEBUG': 1, 'INFO': 2, 'WARNING': 3, 'ERROR': 4, 'CRITICAL': 5}
                levels_to_delete = [l for l, n in level_order.items() if n <= level_order.get(clear_level, 0)]
                if levels_to_delete:
                    logs_to_delete = logs_to_delete.filter(level__in=levels_to_delete)
            
            # Apply date filter if provided
            if clear_before_date:
                try:
                    from datetime import datetime
                    date = datetime.strptime(clear_before_date, '%Y-%m-%d').date()
                    logs_to_delete = logs_to_delete.filter(timestamp__date__lte=date)
                except ValueError:
                    pass
            
            # Delete the logs
            count = logs_to_delete.count()
            logs_to_delete.delete()
            
            # Add success message
            messages.success(request, f"Successfully cleared {count} log entries.")
            
            # Log this action
            logger.info(f"User cleared {count} log entries.")
            LogEntry.info(f"User cleared {count} log entries.", source="web_ui.views.logs")
            
        except Exception as e:
            messages.error(request, f"Error clearing logs: {str(e)}")
            logger.error(f"Error clearing logs: {str(e)}")
    
    # Generate test logs if requested (for development)
    if request.GET.get('generate_test_logs') == '1':
        generate_test_logs()
        messages.success(request, "Generated test logs for demonstration purposes.")
    
    # Get filter parameters from request
    level_filter = request.GET.get('level', '')
    source_filter = request.GET.get('source', '')
    search_query = request.GET.get('search', '')
    date_filter = request.GET.get('date', '')
    
    # Start with all logs
    logs_query = LogEntry.objects.all()
    
    # Apply filters
    if level_filter:
        logs_query = logs_query.filter(level=level_filter)
    
    if source_filter:
        logs_query = logs_query.filter(source=source_filter)
    
    if search_query:
        logs_query = logs_query.filter(message__icontains=search_query)
    
    if date_filter:
        try:
            # Parse the date and filter by it
            from datetime import datetime
            date = datetime.strptime(date_filter, '%Y-%m-%d').date()
            logs_query = logs_query.filter(timestamp__date=date)
        except ValueError:
            # If date parsing fails, ignore this filter
            pass
    
    # Paginate the results
    from django.core.paginator import Paginator
    paginator = Paginator(logs_query, 50)  # Show 50 logs per page
    page_number = request.GET.get('page', 1)
    logs_page = paginator.get_page(page_number)
    
    # Get unique sources for the filter dropdown
    sources = LogEntry.objects.values_list('source', flat=True).distinct()
    
    return render(request, 'logs.html', {
        'title': 'Logs',
        'logs': logs_page,
        'sources': sources,
        'log_levels': [level[0] for level in LogEntry.LEVEL_CHOICES],
        'current_level': level_filter,
        'current_source': source_filter,
        'search_query': search_query,
        'date_filter': date_filter,
        'configured_level': log_level
    })

def generate_test_logs():
    """Generate test logs for demonstration purposes."""
    import random
    from datetime import timedelta
    from django.utils import timezone
    
    # Sample messages for different log levels
    debug_messages = [
        "Initializing component...",
        "Loading configuration file...",
        "Checking cache status...",
        "Processing item #123...",
        "Established database connection",
    ]
    
    info_messages = [
        "User logged in successfully",
        "Recipe created: analytics-database",
        "Scheduled job completed successfully",
        "Configuration updated",
        "API request completed in 235ms",
    ]
    
    warning_messages = [
        "Connection attempt timed out, retrying...",
        "Deprecated API method called",
        "Cache size exceeding recommended limit",
        "Slow query detected, consider optimization",
        "Failed to load optional component",
    ]
    
    error_messages = [
        "Database connection failed",
        "Unable to parse configuration file",
        "API request failed with status 500",
        "Validation error: missing required field",
        "Task execution timed out after 30s",
    ]
    
    critical_messages = [
        "System is out of disk space",
        "Database corruption detected",
        "Fatal error: unable to start required service",
        "Security breach detected",
        "Unhandled exception in critical process",
    ]
    
    # Sources
    sources = [
        "datahub.rest_client",
        "datahub.graphql_client",
        "web_ui.views",
        "recipe_manager",
        "policy_manager",
        "utils.auth",
    ]
    
    # Generate logs with random timestamps in the last 7 days
    now = timezone.now()
    for i in range(100):  # Generate 100 test log entries
        # Random timestamp in the last 7 days
        timestamp = now - timedelta(
            days=random.randint(0, 7),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        # Random log level
        level_choice = random.choices(
            ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            weights=[40, 30, 20, 8, 2],  # More debug/info, fewer errors/critical
            k=1
        )[0]
        
        # Select a random message based on level
        if level_choice == "DEBUG":
            message = random.choice(debug_messages)
        elif level_choice == "INFO":
            message = random.choice(info_messages)
        elif level_choice == "WARNING":
            message = random.choice(warning_messages)
        elif level_choice == "ERROR":
            message = random.choice(error_messages)
        else:  # CRITICAL
            message = random.choice(critical_messages)
        
        # Random source
        source = random.choice(sources)
        
        # Details for some logs
        details = None
        if random.random() < 0.3:  # 30% chance of having details
            if level_choice in ["ERROR", "CRITICAL"]:
                details = f"Traceback (most recent call last):\n  File 'app.py', line {random.randint(10, 500)}, in <module>\n    raise Exception('Sample error for testing')\nException: Sample error for testing"
            else:
                details = f"Additional information:\n- Request ID: {random.randint(1000, 9999)}\n- Processing time: {random.randint(10, 500)}ms\n- User agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        
        # Create the log entry with the specified timestamp
        log = LogEntry(
            timestamp=timestamp,
            level=level_choice,
            source=source,
            message=message,
            details=details
        )
        log.save()

def policy_view(request, policy_id):
    """View a specific policy."""
    client = get_datahub_client()
    policy = None
    
    if client and client.test_connection():
        try:
            # First try to fetch by ID directly
            policy = client.get_policy(policy_id)
            
            # If not found, try with URN format
            if not policy and not policy_id.startswith('urn:'):
                policy = client.get_policy(f"urn:li:policy:{policy_id}")
                
            if policy:
                # Format JSON fields for display
                policy_json = json.dumps(policy, indent=2)
                resources_json = json.dumps(policy.get('resources', []), indent=2)
                privileges_json = json.dumps(policy.get('privileges', []), indent=2)
                actors_json = json.dumps(policy.get('actors', {}), indent=2)
            else:
                messages.error(request, f"Policy with ID {policy_id} not found")
                
        except Exception as e:
            messages.error(request, f"Error retrieving policy: {str(e)}")
            logger.error(f"Error retrieving policy: {str(e)}")
    else:
        messages.warning(request, "Not connected to DataHub")
    
    return render(request, 'policies/view.html', {
        'title': 'Policy Details',
        'policy': policy,
        'policy_json': policy_json if policy else "{}",
        'resources_json': resources_json if policy else "[]",
        'privileges_json': privileges_json if policy else "[]",
        'actors_json': actors_json if policy else "{}"
    })

# Recipe Templates Management Views
def recipe_templates(request):
    """List all recipe templates."""
    templates = RecipeTemplate.objects.all().order_by('-updated_at')
    
    # Handle filtering
    tag_filter = request.GET.get('tag')
    if tag_filter:
        # Filter by tag (simple contains for comma-separated tags)
        templates = templates.filter(tags__contains=tag_filter)
        
    # Handle search
    search_query = request.GET.get('search')
    if search_query:
        templates = templates.filter(
            models.Q(name__icontains=search_query) | 
            models.Q(description__icontains=search_query) |
            models.Q(recipe_type__icontains=search_query)
        )
    
    # Get unique tags for filter dropdown
    all_tags = set()
    for template in RecipeTemplate.objects.all():
        if template.tags:
            all_tags.update(template.get_tags_list())
    
    return render(request, 'recipes/templates/list.html', {
        'title': 'Recipe Templates',
        'templates': templates,
        'tag_filter': tag_filter,
        'search_query': search_query,
        'all_tags': sorted(all_tags)
    })

def recipe_template_detail(request, template_id):
    """View a recipe template details."""
    template = get_object_or_404(RecipeTemplate, id=template_id)
    
    # Process content for display
    if template.content.strip().startswith('{'):
        content_type = 'json'
        try:
            formatted_content = json.dumps(json.loads(template.content), indent=2)
        except:
            formatted_content = template.content
    else:
        content_type = 'yaml'
        formatted_content = template.content
    
    return render(request, 'recipes/templates/detail.html', {
        'title': f'Template: {template.name}',
        'template': template,
        'content': formatted_content,
        'content_type': content_type,
        'tags': template.get_tags_list()
    })

def recipe_template_create(request):
    """Create a new recipe template."""
    if request.method == 'POST':
        form = RecipeTemplateForm(request.POST)
        if form.is_valid():
            # Create the template
            template = RecipeTemplate(
                name=form.cleaned_data['name'],
                description=form.cleaned_data['description'],
                recipe_type=form.cleaned_data['recipe_type'],
                content=form.cleaned_data['content'],
            )
            
            # Handle tags
            if form.cleaned_data['tags']:
                template.set_tags_list([tag.strip() for tag in form.cleaned_data['tags'].split(',')])
            
            template.save()
            messages.success(request, f"Template '{template.name}' created successfully")
            return redirect('recipe_templates')
    else:
        form = RecipeTemplateForm()
    
    return render(request, 'recipes/templates/create.html', {
        'title': 'Create Recipe Template',
        'form': form
    })

def recipe_template_edit(request, template_id):
    """Edit a recipe template."""
    template = get_object_or_404(RecipeTemplate, id=template_id)
    
    if request.method == 'POST':
        form = RecipeTemplateForm(request.POST)
        if form.is_valid():
            # Update the template
            template.name = form.cleaned_data['name']
            template.description = form.cleaned_data['description']
            template.recipe_type = form.cleaned_data['recipe_type']
            template.content = form.cleaned_data['content']
            
            # Handle tags
            if form.cleaned_data['tags']:
                template.set_tags_list([tag.strip() for tag in form.cleaned_data['tags'].split(',')])
            else:
                template.tags = ''
            
            template.save()
            messages.success(request, f"Template '{template.name}' updated successfully")
            return redirect('recipe_template_detail', template_id=template.id)
    else:
        form = RecipeTemplateForm(initial={
            'name': template.name,
            'description': template.description,
            'recipe_type': template.recipe_type,
            'content': template.content,
            'tags': template.tags
        })
    
    return render(request, 'recipes/templates/edit.html', {
        'title': 'Edit Recipe Template',
        'form': form,
        'template': template
    })

def recipe_template_delete(request, template_id):
    """Delete a recipe template."""
    template = get_object_or_404(RecipeTemplate, id=template_id)
    
    if request.method == 'POST':
        template_name = template.name
        template.delete()
        messages.success(request, f"Template '{template_name}' deleted successfully")
        return redirect('recipe_templates')
    
    return render(request, 'recipes/templates/delete.html', {
        'title': 'Delete Recipe Template',
        'template': template
    })

def recipe_template_export(request, template_id):
    """Export a recipe template to JSON or YAML."""
    template = get_object_or_404(RecipeTemplate, id=template_id)
    
    # Determine content type
    is_json = template.content.strip().startswith('{')
    
    # Set response content type
    content_type = 'application/json' if is_json else 'application/x-yaml'
    file_extension = 'json' if is_json else 'yaml'
    
    # Format content
    if is_json:
        try:
            content = json.dumps(json.loads(template.content), indent=2)
        except:
            content = template.content
    else:
        content = template.content
    
    # Prepare response
    response = HttpResponse(content, content_type=content_type)
    response['Content-Disposition'] = f'attachment; filename="{template.name}.{file_extension}"'
    return response

def recipe_template_import(request):
    """Import a recipe template from a file."""
    if request.method == 'POST':
        form = RecipeTemplateImportForm(request.POST, request.FILES)
        if form.is_valid():
            template_file = request.FILES['template_file']
            
            try:
                content = template_file.read().decode('utf-8')
                
                # Extract name and type from file content if possible
                if template_file.name.endswith('.json'):
                    recipe_data = json.loads(content)
                    recipe_name = recipe_data.get('name', '') or recipe_data.get('source_name', '')
                    recipe_type = recipe_data.get('type', '') or recipe_data.get('source', {}).get('type', '')
                else:
                    # For YAML, attempt to parse
                    import yaml
                    recipe_data = yaml.safe_load(content)
                    recipe_name = recipe_data.get('name', '') or recipe_data.get('source_name', '')
                    recipe_type = recipe_data.get('type', '') or recipe_data.get('source', {}).get('type', '')
                
                # Create a pre-filled form for the template
                initial_data = {
                    'name': recipe_name or os.path.splitext(template_file.name)[0],
                    'recipe_type': recipe_type or '',
                    'content': content,
                    'tags': form.cleaned_data.get('tags', '')
                }
                
                template_form = RecipeTemplateForm(initial=initial_data)
                return render(request, 'recipes/templates/create.html', {
                    'form': template_form,
                    'imported': True
                })
                
            except Exception as e:
                messages.error(request, f"Error parsing template file: {str(e)}")
    else:
        form = RecipeTemplateImportForm()
    
    return render(request, 'recipes/templates/import.html', {
        'title': 'Import Recipe Template',
        'form': form
    })

def recipe_template_deploy(request, template_id):
    """Deploy a recipe template to DataHub."""
    from .forms import RecipeDeployForm
    from .models import RecipeManager
    import uuid
    
    template = get_object_or_404(RecipeTemplate, id=template_id)
    client = get_datahub_client()
    
    if not client or not client.test_connection():
        messages.error(request, "Not connected to DataHub")
        return redirect('recipe_template_detail', template_id=template_id)
    
    if request.method == 'POST':
        form = RecipeDeployForm(request.POST)
        
        if form.is_valid():
            try:
                # Parse recipe content
                if template.content.strip().startswith('{'):
                    recipe = json.loads(template.content)
                else:
                    import yaml
                    recipe = yaml.safe_load(template.content)
                
                # Process environment variables
                if form.cleaned_data.get('environment_variables'):
                    try:
                        env_vars = json.loads(form.cleaned_data['environment_variables'])
                        recipe = replace_env_vars_with_values(recipe, env_vars)
                        
                        # Store secrets in the database for future reference
                        store_recipe_secrets(form.cleaned_data['recipe_id'], env_vars)
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON in environment variables: {form.cleaned_data['environment_variables']}")
                
                # Prepare schedule if provided
                schedule = None
                if form.cleaned_data['schedule_cron']:
                    schedule = {
                        'interval': form.cleaned_data['schedule_cron'],
                        'timezone': form.cleaned_data['schedule_timezone'] or 'UTC'
                    }
                
                # Create the recipe in DataHub
                result = client.create_ingestion_source(
                    name=form.cleaned_data['recipe_name'],
                    type=template.recipe_type,
                    recipe=recipe,
                    source_id=form.cleaned_data['recipe_id'],
                    schedule=schedule,
                    executor_id="default",
                    description=form.cleaned_data.get('description', '')
                )
                
                if result:
                    messages.success(request, f"Recipe '{form.cleaned_data['recipe_name']}' deployed successfully")
                    return redirect('recipes')
                else:
                    messages.error(request, "Failed to deploy recipe")
            
            except Exception as e:
                messages.error(request, f"Error deploying recipe: {str(e)}")
        
    else:
        # Generate a default recipe ID and name based on template
        recipe_id = f"{template.recipe_type.lower()}-{uuid.uuid4().hex[:8]}"
        
        form = RecipeDeployForm(initial={
            'recipe_name': template.name,
            'recipe_id': recipe_id,
            'schedule_cron': RecipeManager.get_default_schedule(),
            'schedule_timezone': 'UTC',
            'description': template.description
        })
    
    return render(request, 'recipes/templates/deploy.html', {
        'title': 'Deploy Recipe Template',
        'template': template,
        'form': form
    })

def replace_env_vars_with_values(recipe_dict, env_vars):
    """
    Replace environment variables in a recipe with their actual values.
    
    Args:
        recipe_dict: The recipe dictionary containing environment variables
        env_vars: Dictionary of environment variables with values and secret flags
    
    Returns:
        Updated recipe with environment variables replaced by values
    """
    def process_node(node):
        if isinstance(node, dict):
            result = {}
            for key, value in node.items():
                result[key] = process_node(value)
            return result
        elif isinstance(node, list):
            result = []
            for item in node:
                result.append(process_node(item))
            return result
        elif isinstance(node, str) and node.startswith('${') and node.endswith('}'):
            # Extract variable name
            var_name = node[2:-1]
            if var_name in env_vars:
                return env_vars[var_name]['value']
            return node
        else:
            return node
    
    return process_node(recipe_dict)

def store_recipe_secrets(recipe_id, env_vars):
    """
    Store secret environment variables for a recipe.
    
    Args:
        recipe_id: ID of the recipe
        env_vars: Dictionary of environment variables with values and secret flags
    """
    # Get only the secret variables
    secrets = {
        var_name: data['value'] 
        for var_name, data in env_vars.items() 
        if data.get('isSecret') and data.get('value')
    }
    
    if not secrets:
        return
    
    try:
        from .models import RecipeSecret
        
        # Delete any existing secrets for this recipe
        RecipeSecret.objects.filter(recipe_id=recipe_id).delete()
        
        # Create new secret entries
        for var_name, value in secrets.items():
            RecipeSecret.objects.create(
                recipe_id=recipe_id,
                variable_name=var_name,
                value=value
            )
    except Exception as e:
        logger.error(f"Error storing recipe secrets: {str(e)}")

def recipe_save_as_template(request, recipe_id):
    """Save an existing recipe as a template."""
    client = get_datahub_client()
    if not client or not client.test_connection():
        messages.error(request, "Not connected to DataHub")
        return redirect('recipes')
    
    # Get the recipe
    recipe_data = client.get_ingestion_source(recipe_id)
    if not recipe_data:
        messages.error(request, f"Recipe {recipe_id} not found")
        return redirect('recipes')
    
    # Extract recipe details
    recipe_name = recipe_data.get('name', '')
    recipe_type = recipe_data.get('type', '')
    recipe_content = recipe_data.get('config', {}).get('recipe', '{}')
    
    # Convert leaf values to environment variables
    if isinstance(recipe_content, dict):
        recipe_content = convert_to_template(recipe_content, recipe_type)
        # Convert to JSON string
        recipe_content = json.dumps(recipe_content, indent=2)
    
    if request.method == 'POST':
        form = RecipeTemplateForm(request.POST)
        
        if form.is_valid():
            from .models import RecipeTemplate
            
            # Create the template
            template = RecipeTemplate(
                name=form.cleaned_data['name'],
                description=form.cleaned_data['description'],
                recipe_type=form.cleaned_data['recipe_type'],
                content=form.cleaned_data['content'],
            )
            
            # Handle tags
            if form.cleaned_data['tags']:
                template.set_tags_list([tag.strip() for tag in form.cleaned_data['tags'].split(',')])
            
            template.save()
            messages.success(request, f"Recipe saved as template '{template.name}' successfully")
            return redirect('recipe_templates')
    else:
        form = RecipeTemplateForm(initial={
            'name': f"{recipe_name} Template",
            'recipe_type': recipe_type,
            'content': recipe_content,
        })
    
    return render(request, 'recipes/templates/create.html', {
        'title': 'Save Recipe as Template',
        'form': form,
        'recipe_id': recipe_id
    })

def convert_to_template(recipe_dict, recipe_type):
    """
    Convert leaf values in a recipe dictionary to environment variables.
    """
    def process_node(node, path=None):
        if path is None:
            path = []
            
        if isinstance(node, dict):
            result = {}
            for key, value in node.items():
                new_path = path + [key]
                result[key] = process_node(value, new_path)
            return result
        elif isinstance(node, list):
            result = []
            for i, item in enumerate(node):
                new_path = path + [str(i)]
                result.append(process_node(item, new_path))
            return result
        else:
            # Leaf value - convert to environment variable
            if isinstance(node, (str, int, float, bool)) and path:
                # Create a variable name based on the path
                var_name = "_".join(path).upper()
                # Remove special characters and replace with underscore
                var_name = re.sub(r'[^A-Z0-9]', '_', var_name)
                # Ensure it starts with a letter
                if not var_name[0].isalpha():
                    var_name = f"VAR_{var_name}"
                # Add prefix based on recipe type
                var_name = f"{recipe_type.upper()}_{var_name}"
                return f"${{{var_name}}}"
            return node
    
    import re
    return process_node(recipe_dict)

def get_recipe_environment_variables(recipe_id, recipe_content):
    """
    Extract environment variables from a recipe and get stored values.
    
    Args:
        recipe_id: ID of the recipe
        recipe_content: Recipe content (JSON or YAML string)
        
    Returns:
        Dictionary of environment variables with their values and secret status
    """
    from .models import RecipeSecret
    
    # Extract environment variables from recipe
    env_vars = {}
    
    # Convert string to dict if needed
    if isinstance(recipe_content, str):
        try:
            if recipe_content.strip().startswith('{'):
                recipe_dict = json.loads(recipe_content)
            else:
                import yaml
                recipe_dict = yaml.safe_load(recipe_content)
        except Exception:
            # If parsing fails, return empty dict
            return env_vars
    else:
        recipe_dict = recipe_content
        
    # Extract variables using regex
    if isinstance(recipe_content, str):
        import re
        regex = r'\${([^}]+)}'
        matches = re.findall(regex, recipe_content)
        
        for var_name in matches:
            env_vars[var_name] = {
                'value': '',
                'isSecret': False
            }
    
    # Extract variables from dict recursively
    def extract_vars(node):
        if isinstance(node, dict):
            for key, value in node.items():
                extract_vars(value)
        elif isinstance(node, list):
            for item in node:
                extract_vars(item)
        elif isinstance(node, str) and node.startswith('${') and node.endswith('}'):
            var_name = node[2:-1]
            if var_name not in env_vars:
                env_vars[var_name] = {
                    'value': '',
                    'isSecret': False
                }
    
    # Extract variables from dict if we have one
    if isinstance(recipe_dict, dict):
        extract_vars(recipe_dict)
    
    # Get stored secrets for this recipe
    try:
        secrets = RecipeSecret.objects.filter(recipe_id=recipe_id)
        for secret in secrets:
            # If it's in env_vars, update the existing entry
            if secret.variable_name in env_vars:
                env_vars[secret.variable_name]['value'] = secret.value
                env_vars[secret.variable_name]['isSecret'] = secret.is_secret
            # Otherwise add a new entry
            else:
                env_vars[secret.variable_name] = {
                    'value': secret.value,
                    'isSecret': secret.is_secret
                }
    except Exception as e:
        # Log error but continue
        logger.error(f"Error retrieving recipe secrets: {str(e)}")
        
    return env_vars 

def update_recipe_environment_variables(recipe_id, env_vars_dict):
    """
    Update environment variables for a recipe.
    
    Args:
        recipe_id: ID of the recipe
        env_vars_dict: Dictionary of environment variables with values and secret flags
    """
    from .models import RecipeSecret
    
    # Delete existing secrets
    RecipeSecret.objects.filter(recipe_id=recipe_id).delete()
    
    # Store all variables
    for var_name, var_data in env_vars_dict.items():
        RecipeSecret.objects.create(
            recipe_id=recipe_id,
            variable_name=var_name,
            value=var_data.get('value', ''),
            is_secret=var_data.get('isSecret', False)
        )

@login_required
def env_vars_templates(request):
    """List all environment variables templates."""
    templates = EnvVarsTemplate.objects.all().order_by('-updated_at')
    return render(request, 'env_vars/templates.html', {'templates': templates})

@login_required
def env_vars_template_create(request):
    """Create a new environment variables template."""
    form = EnvVarsTemplateForm()
    
    if request.method == 'POST':
        form = EnvVarsTemplateForm(request.POST)
        if form.is_valid():
            # Create new template
            template = EnvVarsTemplate(
                name=form.cleaned_data['name'],
                description=form.cleaned_data['description'],
                recipe_type=form.cleaned_data['recipe_type'],
                variables=form.cleaned_data['variables']
            )
            
            # Handle tags
            if form.cleaned_data['tags']:
                template.set_tags_list([tag.strip() for tag in form.cleaned_data['tags'].split(',')])
            
            template.save()
            messages.success(request, f"Environment variables template '{template.name}' created successfully")
            return redirect('env_vars_templates')
        else:
            messages.error(request, "Please correct the errors below")
    
    return render(request, 'env_vars/template_form.html', {
        'form': form,
        'title': 'Create Environment Variables Template',
        'is_new': True
    })

@login_required
def env_vars_instances(request):
    """List all environment variables instances."""
    instances = EnvVarsInstance.objects.all().order_by('-updated_at')
    return render(request, 'env_vars/instances.html', {'instances': instances})

@login_required
def env_vars_instance_create(request):
    """Create a new environment variables instance."""
    form = EnvVarsInstanceForm()
    
    if request.method == 'POST':
        form = EnvVarsInstanceForm(request.POST)
        if form.is_valid():
            # Create new instance
            instance = EnvVarsInstance()
            instance.name = form.cleaned_data['name']
            instance.description = form.cleaned_data['description']
            instance.template = form.cleaned_data['template']
            instance.recipe_type = form.cleaned_data['recipe_type']
            instance.variables = form.cleaned_data['variables']
            instance.save()

            messages.success(request, f"Environment variables instance '{instance.name}' created successfully")
            return redirect('env_vars_instances')
        else:
            messages.error(request, "Please correct the errors below")
    
    return render(request, 'env_vars/instance_form.html', {
        'form': form,
        'title': 'Create Environment Variables Instance',
        'is_new': True
    })

@login_required
def env_vars_instance_edit(request, instance_id):
    """Edit an existing environment variables instance."""
    instance = get_object_or_404(EnvVarsInstance, id=instance_id)
    
    if request.method == 'POST':
        form = EnvVarsInstanceForm(request.POST)
        if form.is_valid():
            # Update instance
            instance.name = form.cleaned_data['name']
            instance.description = form.cleaned_data['description']
            instance.template = form.cleaned_data['template']
            instance.recipe_type = form.cleaned_data['recipe_type']
            instance.variables = form.cleaned_data['variables']
            instance.save()

            messages.success(request, f"Environment variables instance '{instance.name}' updated successfully")
            return redirect('env_vars_instances')
        else:
            messages.error(request, "Please correct the errors below")
    else:
        form = EnvVarsInstanceForm(initial={
            'name': instance.name,
            'description': instance.description,
            'template': instance.template,
            'recipe_type': instance.recipe_type,
            'variables': instance.variables,
        })
    
    return render(request, 'env_vars/instance_form.html', {
        'form': form,
        'instance': instance,
        'title': f'Edit Environment Variables Instance: {instance.name}',
        'is_new': False
    })

@login_required
def env_vars_instance_delete(request, instance_id):
    """Delete an environment variables instance."""
    instance = get_object_or_404(EnvVarsInstance, id=instance_id)
    
    if request.method == 'POST':
        instance_name = instance.name
        instance.delete()
        messages.success(request, f"Environment variables instance '{instance_name}' deleted successfully")
        return redirect('env_vars_instances')
    
    return render(request, 'env_vars/instance_confirm_delete.html', {'instance': instance})

@login_required
def env_vars_instance_detail(request, instance_id):
    """View details of an environment variables instance."""
    instance = get_object_or_404(EnvVarsInstance, id=instance_id)
    variables = instance.get_variables_dict()
    
    return render(request, 'env_vars/instance_detail.html', {
        'instance': instance,
        'variables': variables,
    })

@login_required
def env_vars_template_details(request, template_id):
    """Get the details of an environment variables template as JSON."""
    template = get_object_or_404(EnvVarsTemplate, id=template_id)
    
    variables = template.get_variables_dict()
    
    # Convert to a list of variables with key, description, required, is_secret fields
    variables_list = []
    for key, details in variables.items():
        variables_list.append({
            'key': key,
            'description': details.get('description', ''),
            'required': details.get('required', False),
            'is_secret': details.get('is_secret', False),
            'default_value': details.get('default_value', '')
        })
    
    response_data = {
        'id': template.id,
        'name': template.name,
        'description': template.description,
        'recipe_type': template.recipe_type,
        'variables': variables_list
    }
    
    return JsonResponse(response_data)

@login_required
def env_vars_template_list(request):
    """Get a list of all environment variables templates as JSON."""
    templates = EnvVarsTemplate.objects.all().order_by('name')
    
    templates_list = [{
        'id': template.id,
        'name': template.name,
        'recipe_type': template.recipe_type,
        'description': template.description,
        'tags': template.get_tags_list(),
        'variable_count': len(template.get_variables_dict())
    } for template in templates]
    
    return JsonResponse({'templates': templates_list})

@login_required
def env_vars_template_get(request, template_id):
    """Get a specific environment variables template as JSON."""
    template = get_object_or_404(EnvVarsTemplate, id=template_id)
    
    response_data = {
        'id': template.id,
        'name': template.name,
        'description': template.description,
        'recipe_type': template.recipe_type,
        'variables': template.get_variables_dict(),
        'tags': template.get_tags_list(),
        'created_at': template.created_at.isoformat(),
        'updated_at': template.updated_at.isoformat()
    }
    
    return JsonResponse(response_data)

@login_required
def env_vars_template_delete(request, template_id):
    """Delete an environment variables template."""
    template = get_object_or_404(EnvVarsTemplate, id=template_id)
    
    # Check if template is in use by any instances
    instances_using_template = EnvVarsInstance.objects.filter(template=template).count()
    
    if request.method == 'POST':
        template_name = template.name
        template.delete()
        messages.success(request, f"Environment variables template '{template_name}' deleted successfully")
        return redirect('env_vars_templates')
    
    return render(request, 'env_vars/template_confirm_delete.html', {
        'template': template,
        'instances_count': instances_using_template
    })

@login_required
def env_vars_instance_list(request):
    """Get a list of all environment variables instances as JSON."""
    instances = EnvVarsInstance.objects.all().order_by('name')
    
    instances_list = [{
        'id': instance.id,
        'name': instance.name,
        'recipe_type': instance.recipe_type,
        'description': instance.description,
        'template_id': instance.template.id if instance.template else None,
        'template_name': instance.template.name if instance.template else None,
        'variable_count': len(instance.get_variables_dict())
    } for instance in instances]
    
    return JsonResponse({'instances': instances_list})

@login_required
def env_vars_instance_json(request, instance_id):
    """Get a specific environment variables instance as JSON."""
    instance = get_object_or_404(EnvVarsInstance, id=instance_id)
    
    response_data = {
        'success': True,
        'instance': {
            'id': instance.id,
            'name': instance.name,
            'description': instance.description,
            'recipe_type': instance.recipe_type,
            'template_id': instance.template.id if instance.template else None,
            'template_name': instance.template.name if instance.template else None,
            'variables': instance.get_variables_dict(),
            'created_at': instance.created_at.isoformat(),
            'updated_at': instance.updated_at.isoformat()
        }
    }
    
    return JsonResponse(response_data) 