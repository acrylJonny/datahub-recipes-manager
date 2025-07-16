# Add parent directory to path to import utils
import os
import sys

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from django.shortcuts import render, redirect, get_object_or_404
from django.http import HttpResponse, JsonResponse
from django.contrib import messages
from django.views.decorators.http import require_POST, require_http_methods
from django.urls import reverse
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
from django.utils import timezone
from django.core.paginator import Paginator
import requests
from django.views.decorators.csrf import csrf_exempt
import time
from random import randint
import uuid
from utils.datahub_utils import get_datahub_client, get_datahub_client_from_request, test_datahub_connection
from .datahub_utils import get_datahub_client_info, refresh_all_client_info, test_datahub_connection as test_env_connection
from web_ui.models import AppSettings, GitSettings

# Import custom forms
from .forms import (
    RecipeForm,
    RecipeImportForm,
    PolicyForm,
    PolicyImportForm,
    RecipeTemplateForm,
    RecipeDeployForm,
    RecipeTemplateImportForm,
    EnvVarsTemplateForm,
    EnvVarsInstanceForm,
    RecipeInstanceForm,
    GitSettingsForm,
)

# Try to import the DataHub client
try:
    from utils.datahub_rest_client import DataHubRestClient

    DATAHUB_CLIENT_AVAILABLE = True
except ImportError:
    DATAHUB_CLIENT_AVAILABLE = False

from .models import (
    LogEntry,
    RecipeTemplate,
    PolicyTemplate,
    EnvVarsTemplate,
    EnvVarsInstance,
    RecipeInstance,
    GitHubPR,
    Policy,
    Environment,
    Mutation,
    DataHubClientInfo,
    GitIntegration,
)
from .services.github_service import GitHubService
from .services.git_service import GitService

logger = logging.getLogger(__name__)





def index(request):
    """Main dashboard view."""
    # Return basic dashboard without expensive API calls
    # API calls will be made asynchronously via AJAX
    return render(
        request,
        "dashboard.html",
        {
            "title": "Dashboard",
            "connected": False,  # Will be populated via AJAX
            "recipes_count": 0,  # Will be populated via AJAX
            "active_schedules_count": 0,  # Will be populated via AJAX
            "policies_count": 0,  # Will be populated via AJAX
            "recent_recipes": [],  # Will be populated via AJAX
            "recent_policies": [],  # Will be populated via AJAX
        },
    )


def dashboard_data(request):
    """AJAX endpoint to load dashboard data asynchronously."""
    client = get_datahub_client_from_request(request)
    connected = False
    recipes_count = 0
    active_schedules_count = 0
    policies_count = 0
    recent_recipes = []
    recent_policies = []

    # Initialize new dashboard data
    environments_data = []
    metadata_stats = {}
    git_status = {}
    system_health = {}

    if client:
        try:
            connected = client.test_connection()

            if connected:
                # Get recipes
                try:
                    recipes = client.list_ingestion_sources()
                    if recipes:
                        recipes_count = len(recipes)
                        active_schedules_count = sum(
                            1 for r in recipes if r.get("schedule")
                        )

                        # Get 5 most recent recipes
                        sorted_recipes = sorted(
                            recipes, key=lambda x: x.get("lastUpdated", 0), reverse=True
                        )[:5]
                        # Add is_active property based on schedule
                        for recipe in sorted_recipes:
                            recipe["is_active"] = bool(recipe.get("schedule"))
                        recent_recipes = sorted_recipes
                except Exception as e:
                    logger.error(f"Error fetching recipes for dashboard: {str(e)}")

                # Get policies using cached data (only for dashboard)
                try:
                    from utils.datahub_utils import get_cached_policies
                    
                    all_policies = get_cached_policies()
                    if all_policies:
                        # Process policies to extract IDs from URNs if needed
                        valid_policies = []
                        for policy in all_policies:
                            # If no ID but has URN, extract ID from URN
                            if not policy.get("id") and policy.get("urn"):
                                # Extract ID from URN (format: urn:li:policy:<id>)
                                parts = policy.get("urn").split(":")
                                if len(parts) >= 4:
                                    policy["id"] = parts[3]

                            # Include policies with an ID (either original or extracted)
                            if policy.get("id"):
                                valid_policies.append(policy)

                        policies_count = len(valid_policies)

                        # Get 5 most recent policies with valid IDs
                        recent_policies = sorted(
                            valid_policies,
                            key=lambda x: x.get("lastUpdated", 0),
                            reverse=True,
                        )[:5]
                except Exception as e:
                    logger.error(f"Error fetching policies for dashboard: {str(e)}")
        except Exception as e:
            logger.error(f"Error connecting to DataHub: {str(e)}")

    # Get environments data
    try:
        from metadata_manager.models import Environment
        environments = Environment.objects.all().order_by('-is_default', 'name')
        
        for env in environments:
            env_data = {
                'id': env.id,
                'name': env.name,
                'description': env.description or '',
                'is_default': env.is_default,
                'recipes_count': 0,
                'policies_count': 0
            }
            
            # Note: Environments are logical groupings, not DataHub connections
            # They don't have their own connection status - that's handled by the Connection model
            # We'll just show the environment info without connection testing
            
            environments_data.append(env_data)
    except Exception as e:
        logger.error(f"Error fetching environments for dashboard: {str(e)}")

    # Get metadata statistics
    try:
        from metadata_manager.models import Domain, GlossaryNode, GlossaryTerm, DataProduct, Assertion
        
        metadata_stats = {
            'domains_count': Domain.objects.count(),
            'domains_local': Domain.objects.filter(sync_status__in=['LOCAL_ONLY', 'MODIFIED']).count(),
            'domains_synced': Domain.objects.filter(sync_status='SYNCED').count(),
            
            'glossary_nodes_count': GlossaryNode.objects.count(),
            'glossary_nodes_local': GlossaryNode.objects.filter(sync_status__in=['LOCAL_ONLY', 'MODIFIED']).count(),
            'glossary_nodes_synced': GlossaryNode.objects.filter(sync_status='SYNCED').count(),
            
            'glossary_terms_count': GlossaryTerm.objects.count(),
            'glossary_terms_local': GlossaryTerm.objects.filter(sync_status__in=['LOCAL_ONLY', 'MODIFIED']).count(),
            'glossary_terms_synced': GlossaryTerm.objects.filter(sync_status='SYNCED').count(),
            
            'data_products_count': DataProduct.objects.count(),
            'data_products_local': DataProduct.objects.filter(sync_status__in=['LOCAL_ONLY', 'MODIFIED']).count(),
            'data_products_synced': DataProduct.objects.filter(sync_status='SYNCED').count(),
            
            'assertions_count': Assertion.objects.count(),
            'assertions_local': Assertion.objects.filter(sync_status__in=['LOCAL_ONLY', 'MODIFIED']).count(),
            'assertions_synced': Assertion.objects.filter(sync_status='SYNCED').count(),
        }
        
        # Get recent metadata activity
        recent_domains = Domain.objects.filter(
            sync_status__in=['LOCAL_ONLY', 'MODIFIED']
        ).order_by('-updated_at')[:3]
        
        recent_glossary = list(GlossaryNode.objects.filter(
            sync_status__in=['LOCAL_ONLY', 'MODIFIED']
        ).order_by('-updated_at')[:2]) + list(GlossaryTerm.objects.filter(
            sync_status__in=['LOCAL_ONLY', 'MODIFIED']
        ).order_by('-updated_at')[:2])
        
        metadata_stats['recent_domains'] = [
            {
                'id': d.id,
                'name': d.name,
                'description': d.description[:100] + '...' if d.description and len(d.description) > 100 else d.description,
                'sync_status': d.sync_status,
                'updated_at': d.updated_at.isoformat() if d.updated_at else None
            }
            for d in recent_domains
        ]
        
        metadata_stats['recent_glossary'] = [
            {
                'id': item.id,
                'name': item.name,
                'type': 'Node' if hasattr(item, 'parent_node') else 'Term',  
                'description': (item.description[:100] + '...' if item.description and len(item.description) > 100 else item.description) if item.description else '',
                'sync_status': item.sync_status,
                'updated_at': item.updated_at.isoformat() if item.updated_at else None
            }
            for item in sorted(recent_glossary, key=lambda x: x.updated_at or timezone.now(), reverse=True)[:4]
        ]
        
    except Exception as e:
        logger.error(f"Error fetching metadata stats for dashboard: {str(e)}")

    # Get git status
    try:
        from web_ui.models import GitSettings
        
        git_settings = GitSettings.objects.first()
        if git_settings:
            # Check if git is enabled AND properly configured
            git_configured = bool(git_settings.token and git_settings.username and git_settings.repository)
            git_enabled = git_settings.enabled
            
            git_status = {
                'enabled': git_enabled,
                'configured': git_configured,
                'current_branch': git_settings.current_branch or 'main',
                'repository_url': f"https://github.com/{git_settings.username}/{git_settings.repository}" if git_settings.username and git_settings.repository else '',
                'staged_files_count': 0,  # Could be enhanced with actual git status
                'recent_commits': []  # Could be enhanced with commit history
            }
        else:
            git_status = {
                'enabled': False,
                'configured': False
            }
    except Exception as e:
        logger.error(f"Error fetching git status for dashboard: {str(e)}")
        git_status = {
            'enabled': False,
            'configured': False,
            'error': str(e)
        }

    # System health overview
    system_health = {
        'datahub_connection': connected,
        'environments_configured': len(environments_data),
        'git_integration': git_status.get('enabled', False),
        'metadata_sync_pending': (
            metadata_stats.get('domains_local', 0) + 
            metadata_stats.get('glossary_nodes_local', 0) + 
            metadata_stats.get('glossary_terms_local', 0) +
            metadata_stats.get('data_products_local', 0) +
            metadata_stats.get('assertions_local', 0)
        ),
        'total_metadata_items': (
            metadata_stats.get('domains_count', 0) +
            metadata_stats.get('glossary_nodes_count', 0) +
            metadata_stats.get('glossary_terms_count', 0) +
            metadata_stats.get('data_products_count', 0) +
            metadata_stats.get('assertions_count', 0)
        )
    }

    return JsonResponse(
        {
            # Original data
            "connected": connected,
            "recipes_count": recipes_count,
            "active_schedules_count": active_schedules_count,
            "policies_count": policies_count,
            "recent_recipes": recent_recipes,
            "recent_policies": recent_policies,
            
            # New dashboard data
            "environments": environments_data,
            "metadata_stats": metadata_stats,
            "git_status": git_status,
            "system_health": system_health,
        }
    )


def recipes(request):
    """List all recipes."""
    # Return basic page structure without expensive API calls
    # Data will be loaded asynchronously via AJAX
    
    # Get refresh rate from settings
    refresh_rate = AppSettings.get_int("refresh_rate", 60)
    
    # Quick connection test
    client = get_datahub_client_from_request(request)
    connected = client and client.test_connection() if client else False

    return render(
        request,
        "recipes/list.html",
        {
            "title": "Recipes",
            "recipes": [],  # Will be populated via AJAX
            "refresh_rate": refresh_rate,
            "connection": {"connected": connected},
            "loading_async": True,  # Flag to show loading state
        },
    )


def recipes_data(request):
    """AJAX endpoint to load recipes data asynchronously."""
    client = get_datahub_client_from_request(request)
    recipes_list = []
    connected = False
    
    # Check if we should force refresh the cache
    force_refresh = request.GET.get('refresh', '').lower() == 'true'

    if client and client.test_connection():
        connected = True
        try:
            from utils.datahub_utils import get_cached_recipes
            
            recipes_list = get_cached_recipes(force_refresh=force_refresh)

            # Process each recipe to format schedule and status
            for recipe in recipes_list:
                # Format schedule nicely
                schedule_data = recipe.get("schedule", {})
                if schedule_data:
                    if isinstance(schedule_data, dict):
                        cron = schedule_data.get("interval", "")
                        timezone = schedule_data.get("timezone", "UTC")
                        if cron:
                            recipe["formatted_schedule"] = f"{cron} ({timezone})"
                            # If we have a schedule, the recipe is enabled
                            recipe["enabled"] = True
                        else:
                            recipe["formatted_schedule"] = None
                            recipe["enabled"] = False
                    # Handle case where schedule might be a string
                    elif isinstance(schedule_data, str):
                        recipe["formatted_schedule"] = schedule_data
                        recipe["enabled"] = True
                else:
                    recipe["formatted_schedule"] = None
                    recipe["enabled"] = False

                # Store the original schedule data for reference
                recipe["schedule_data"] = schedule_data

                # Add is_active property for consistency with dashboard
                recipe["is_active"] = bool(recipe.get("schedule"))

            # Sort by name
            recipes_list.sort(key=lambda x: x.get("name", "").lower())
        except Exception as e:
            logger.error(f"Error fetching recipes: {str(e)}")
            return JsonResponse({
                "success": False,
                "error": str(e),
                "connected": connected,
                "recipes": []
            })
    
    return JsonResponse({
        "success": True,
        "connected": connected,
        "recipes": recipes_list
    })


def recipe_create(request):
    """Create a new recipe."""
    if request.method == "POST":
        form = RecipeForm(request.POST)
        if form.is_valid():
            client = get_datahub_client_from_request(request)
            if client and client.test_connection():
                try:
                    # Parse recipe content
                    recipe_content = form.cleaned_data["recipe_content"]
                    try:
                        if recipe_content.strip().startswith("{"):
                            recipe_json = json.loads(recipe_content)
                        else:
                            # If content is in YAML format, convert to JSON before sending to DataHub
                            import yaml

                            recipe_json = yaml.safe_load(recipe_content)
                    except Exception as e:
                        messages.error(request, f"Invalid recipe format: {str(e)}")
                        return render(request, "recipes/create.html", {"form": form})

                    # Prepare schedule if provided
                    schedule = None
                    if form.cleaned_data["schedule_cron"]:
                        schedule = {
                            "interval": form.cleaned_data["schedule_cron"],
                            "timezone": form.cleaned_data["schedule_timezone"] or "UTC",
                        }

                    # Create the recipe
                    result = client.create_ingestion_source(
                        name=form.cleaned_data["recipe_name"],
                        type=form.cleaned_data["recipe_type"],
                        recipe=recipe_json,
                        source_id=form.cleaned_data["recipe_id"],
                        schedule=schedule,
                        executor_id="default",
                    )

                    if result:
                        # Invalidate recipes cache
                        from utils.datahub_utils import invalidate_recipes_cache
                        invalidate_recipes_cache()
                        
                        messages.success(request, "Recipe created successfully")
                        return redirect("recipes")
                    else:
                        messages.error(request, "Failed to create recipe")
                except Exception as e:
                    messages.error(request, f"Error creating recipe: {str(e)}")
            else:
                messages.error(request, "Not connected to DataHub")
    else:
        form = RecipeForm()

    return render(request, "recipes/create.html", {"form": form})


def recipe_edit(request, recipe_id):
    """Edit an existing recipe."""
    client = get_datahub_client_from_request(request)
    if not client or not client.test_connection():
        messages.error(request, "Not connected to DataHub")
        return redirect("recipes")

    # Get the recipe
    recipe_data = client.get_ingestion_source(recipe_id)
    if not recipe_data:
        messages.error(request, f"Recipe {recipe_id} not found")
        return redirect("recipes")

    # Extract recipe content
    config = recipe_data.get("config")
    if config is None:
        config = {}
    recipe_content = config.get("recipe", "{}")
    if isinstance(recipe_content, dict):
        try:
            # Convert to YAML for display by default
            import yaml

            recipe_content = yaml.dump(
                recipe_content, sort_keys=False, default_flow_style=False
            )
        except:
            recipe_content = json.dumps(recipe_content, indent=2)

    # Extract schedule information
    schedule_info = recipe_data.get("schedule")
    if schedule_info is None:
        schedule_info = {}
    schedule_cron = schedule_info.get("interval", "")
    schedule_timezone = schedule_info.get("timezone", "UTC")

    # Get environment variables
    env_vars = get_recipe_environment_variables(recipe_id, recipe_content)

    # Extract just the ID portion from the URN
    display_recipe_id = recipe_id
    if recipe_id.startswith("urn:li:dataHubIngestionSource:"):
        display_recipe_id = recipe_id.split(":")[-1]

    if request.method == "POST":
        form = RecipeForm(request.POST)

        # Check if this is an environment variables update
        if "save_env_vars" in request.POST:
            try:
                env_vars_json = request.POST.get("environment_variables", "{}")
                env_vars_data = json.loads(env_vars_json)
                update_recipe_environment_variables(recipe_id, env_vars_data)
                messages.success(request, "Environment variables updated successfully")
                return redirect("recipe_edit", recipe_id=recipe_id)
            except Exception as e:
                messages.error(
                    request, f"Error updating environment variables: {str(e)}"
                )
                return redirect("recipe_edit", recipe_id=recipe_id)

        # Handle regular form submission
        if form.is_valid():
            try:
                # Parse recipe content
                updated_recipe_content = form.cleaned_data["recipe_content"]
                try:
                    if updated_recipe_content.strip().startswith("{"):
                        updated_recipe_json = json.loads(updated_recipe_content)
                    else:
                        # If content is in YAML format, convert to JSON before sending to DataHub
                        import yaml

                        updated_recipe_json = yaml.safe_load(updated_recipe_content)
                except Exception as e:
                    messages.error(request, f"Invalid recipe format: {str(e)}")
                    return render(
                        request,
                        "recipes/edit.html",
                        {
                            "form": form,
                            "recipe_id": recipe_id,
                            "display_recipe_id": display_recipe_id,
                            "env_vars": env_vars,
                        },
                    )

                # Apply environment variables
                env_vars_data = request.POST.get("environment_variables")
                if env_vars_data:
                    try:
                        env_vars_dict = json.loads(env_vars_data)
                        # Store all environment variables
                        update_recipe_environment_variables(recipe_id, env_vars_dict)

                        # Only replace values in the recipe if needed
                        # This allows users to use placeholders in recipe that will be
                        # replaced at runtime with actual values
                        if form.cleaned_data.get("replace_env_vars", False):
                            updated_recipe_json = replace_env_vars_with_values(
                                updated_recipe_json, env_vars_dict
                            )
                    except Exception as e:
                        logger.error(
                            f"Error processing environment variables: {str(e)}"
                        )

                # Prepare schedule if provided
                schedule = None
                if form.cleaned_data["schedule_cron"]:
                    schedule = {
                        "interval": form.cleaned_data["schedule_cron"],
                        "timezone": form.cleaned_data["schedule_timezone"] or "UTC",
                    }

                # Update the recipe
                result = client.patch_ingestion_source(
                    urn=recipe_id,
                    name=form.cleaned_data["recipe_name"],
                    recipe=updated_recipe_json,
                    schedule=schedule,
                )

                if result:
                    messages.success(request, "Recipe updated successfully")
                    return redirect("recipes")
                else:
                    messages.error(request, "Failed to update recipe")
            except Exception as e:
                messages.error(request, f"Error updating recipe: {str(e)}")
    else:
        # Initialize form with existing values
        form = RecipeForm(
            initial={
                "recipe_id": display_recipe_id,  # Use the simplified ID for display
                "recipe_name": recipe_data.get("name", ""),
                "recipe_type": recipe_data.get("type", ""),
                "description": recipe_data.get("description", ""),
                "schedule_cron": schedule_cron,
                "schedule_timezone": schedule_timezone,
                "recipe_content": recipe_content,
            }
        )

    return render(
        request,
        "recipes/edit.html",
        {
            "form": form,
            "recipe_id": recipe_id,
            "display_recipe_id": display_recipe_id,  # Pass the simplified ID to the template
            "env_vars": env_vars,
            "env_vars_json": json.dumps(env_vars),
        },
    )


def recipe_import(request):
    """Import a recipe from a file."""
    if request.method == "POST":
        form = RecipeImportForm(request.POST, request.FILES)
        if form.is_valid():
            recipe_file = request.FILES["recipe_file"]

            try:
                content = recipe_file.read().decode("utf-8")

                # Determine if JSON or YAML
                if recipe_file.name.endswith(".json"):
                    recipe = json.loads(content)
                else:
                    recipe = yaml.safe_load(content)

                # Create a pre-filled form for the recipe
                initial_data = {
                    "recipe_id": recipe.get("source_id", ""),
                    "recipe_name": recipe.get("source_name", ""),
                    "recipe_type": recipe.get("source", {}).get("type", ""),
                    "description": recipe.get("description", ""),
                    "recipe_content": content,
                }

                recipe_form = RecipeForm(initial=initial_data)
                return render(
                    request,
                    "recipes/create.html",
                    {"form": recipe_form, "imported": True},
                )

            except Exception as e:
                messages.error(request, f"Error parsing recipe file: {str(e)}")
    else:
        form = RecipeImportForm()

    return render(request, "recipes/import.html", {"form": form})


@require_POST
def recipe_delete(request, recipe_id):
    """Delete a recipe."""
    client = get_datahub_client_from_request(request)
    is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"

    if not client or not client.test_connection():
        if is_ajax:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})
        messages.error(request, "Not connected to DataHub")
        return redirect("recipes")

    try:
        result = client.delete_ingestion_source(recipe_id)
        if result:
            # Invalidate recipes cache
            from utils.datahub_utils import invalidate_recipes_cache
            invalidate_recipes_cache()
            
            if is_ajax:
                return JsonResponse({"success": True, "redirect": "/recipes/"})
            messages.success(request, "Recipe deleted successfully")
            return redirect("recipes")
        else:
            if is_ajax:
                return JsonResponse(
                    {"success": False, "error": "Failed to delete recipe"}
                )
            messages.error(request, "Failed to delete recipe")
            return redirect("recipes")
    except Exception as e:
        if is_ajax:
            return JsonResponse({"success": False, "error": str(e)})
        messages.error(request, f"Error deleting recipe: {str(e)}")
        return redirect("recipes")


def extract_recipe_id(recipe_id):
    """
    Extract the ID from a recipe ID or recipe dictionary

    Args:
        recipe_id: A string ID or dictionary containing recipe information

    Returns:
        str: The extracted ID as a string
    """
    # If it's a string, use it directly
    if isinstance(recipe_id, str):
        return recipe_id

    # If it's a dict, extract the ID
    if isinstance(recipe_id, dict):
        # Try to get the ID directly
        if "id" in recipe_id:
            return recipe_id["id"]

        # Try to get the ID from the URN
        if "urn" in recipe_id and isinstance(recipe_id["urn"], str):
            urn = recipe_id["urn"]
            if urn.startswith("urn:li:dataHubIngestionSource:"):
                return urn.split(":")[-1]

    # Return as-is if we couldn't extract anything
    return recipe_id


def recipe_run(request, recipe_id):
    """Run a recipe immediately."""
    client = get_datahub_client_from_request(request)

    # Check if this is a POST or AJAX request
    is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    is_post = request.method == "POST"

    if not client or not client.test_connection():
        if is_ajax:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})
        messages.error(request, "Not connected to DataHub")
        return redirect("recipes")

    # Validate recipe_id
    if not recipe_id or recipe_id == "":
        error_msg = "Empty or missing recipe ID"
        if is_ajax:
            return JsonResponse({"success": False, "error": error_msg})
        messages.error(request, error_msg)
        return redirect("recipes")

    try:
        # Make sure we're always sending a POST for the actual run action
        if is_post or is_ajax:
            # Extract just the ID string if recipe_id is a dict
            actual_id = recipe_id
            if isinstance(recipe_id, dict):
                if "id" in recipe_id:
                    actual_id = recipe_id["id"]
                elif "urn" in recipe_id:
                    # Extract ID from URN if available
                    urn = recipe_id["urn"]
                    if urn and urn.startswith("urn:li:dataHubIngestionSource:"):
                        actual_id = urn.split(":")[-1]

            # Check for empty ID after processing
            if not actual_id or actual_id == "":
                error_msg = "Empty or missing recipe ID after processing"
                if is_ajax:
                    return JsonResponse({"success": False, "error": error_msg})
                messages.error(request, error_msg)
                return redirect("recipes")

            # Create the source URN with the ID
            source_urn = f"urn:li:dataHubIngestionSource:{actual_id}"

            # Create GraphQL mutation following the test_run_now.py pattern
            mutation = """
            mutation createIngestionExecutionRequest($input: CreateIngestionExecutionRequestInput!) {
              createIngestionExecutionRequest(input: $input)
            }
            """

            variables = {"input": {"ingestionSourceUrn": source_urn}}

            # Execute the GraphQL mutation
            execution_request_urn = None
            success = False
            result = None

            try:
                if hasattr(client, "graph"):
                    result = client.graph.execute_graphql(mutation, variables)
                    if (
                        result
                        and isinstance(result, dict)
                        and "data" in result
                        and "createIngestionExecutionRequest" in result["data"]
                    ):
                        execution_request_urn = result["data"][
                            "createIngestionExecutionRequest"
                        ]
                        success = bool(execution_request_urn)
                    elif result and isinstance(result, dict) and "errors" in result:
                        error_msg = f"GraphQL error: {result['errors'][0].get('message', 'Unknown error')}"
                        logging.error(
                            f"GraphQL error when running recipe {actual_id}: {error_msg}"
                        )
                        if is_ajax:
                            return JsonResponse({"success": False, "error": error_msg})
                        messages.error(request, error_msg)
                        return redirect("recipes")
                else:
                    # Fall back to client.trigger_ingestion if no graph client available
                    success = client.trigger_ingestion(actual_id)
            except Exception as e:
                error_msg = f"Error executing GraphQL: {str(e)}"
                logging.error(f"Exception when running recipe {actual_id}: {error_msg}")
                if is_ajax:
                    return JsonResponse({"success": False, "error": error_msg})
                messages.error(request, error_msg)
                return redirect("recipes")

            if success:
                if is_ajax:
                    response_data = {"success": True}
                    if execution_request_urn:
                        response_data["execution_id"] = execution_request_urn
                    return JsonResponse(response_data)
                messages.success(request, "Recipe execution started successfully!")
                return redirect("recipes")
            else:
                error_msg = "Failed to trigger ingestion"
                # Only check result if it's defined and is a dictionary
                if (
                    result
                    and isinstance(result, dict)
                    and "errors" in result
                    and result["errors"]
                ):
                    error_details = (
                        result["errors"][0].get("message", "")
                        if result["errors"]
                        else ""
                    )
                    error_msg = (
                        f"Error: {error_details}" if error_details else error_msg
                    )

                if is_ajax:
                    return JsonResponse({"success": False, "error": error_msg})
                messages.error(request, error_msg)
                return redirect("recipes")
        else:
            # If it's a GET request, just redirect to recipes
            return redirect("recipes")
    except Exception as e:
        logging.exception(f"Unexpected error in recipe_run: {str(e)}")
        if is_ajax:
            return JsonResponse({"success": False, "error": str(e)})
        messages.error(request, f"Error running recipe: {str(e)}")
        return redirect("recipes")


def recipe_download(request, recipe_id):
    """Download a recipe as JSON."""
    client = get_datahub_client_from_request(request)
    if not client or not client.test_connection():
        messages.error(request, "Not connected to DataHub")
        return redirect("recipes")

    # Get the recipe
    recipe_data = client.get_ingestion_source(recipe_id)
    if not recipe_data:
        messages.error(request, f"Recipe {recipe_id} not found")
        return redirect("recipes")

    # Extract recipe content
    recipe_content = recipe_data.get("config", {}).get("recipe", "{}")
    if isinstance(recipe_content, str):
        try:
            recipe_content = json.loads(recipe_content)
        except:
            pass

    # Prepare response
    response = HttpResponse(
        json.dumps(recipe_content, indent=2), content_type="application/json"
    )
    response["Content-Disposition"] = f'attachment; filename="{recipe_id}.json"'
    return response


def policies(request):
    """List all policies from DataHub."""
    # Return basic page structure without expensive API calls
    # Data will be loaded asynchronously via AJAX
    
    # Quick connection test
    client = get_datahub_client_from_request(request)
    connected = client and client.test_connection() if client else False

    # Get local policies from database (quick operation)
    local_policies = Policy.objects.all().order_by("name")
    local_policies_count = local_policies.count()

    # Default active tab
    active_tab = request.GET.get("tab", "server")

    return render(
        request,
        "policies/list.html",
        {
            "title": "DataHub Policies",
            "connected": connected,
            "policies": [],  # Will be populated via AJAX
            "local_policies": local_policies,
            "local_policies_count": local_policies_count,
            "active_tab": active_tab,
            "loading_async": True,  # Flag to show loading state
        },
    )


def policies_data(request):
    """AJAX endpoint to load policies data asynchronously."""
    client = get_datahub_client_from_request(request)
    connected = client and client.test_connection() if client else False

    try:
        # Check if we should force refresh the cache
        force_refresh = request.GET.get('refresh', '').lower() == 'true'
        
        # Get policies from cached data
        server_policies = []
        if connected:
            from utils.datahub_utils import get_cached_policies
            
            server_policies = get_cached_policies(force_refresh=force_refresh)

            # Format policy data for display
            for policy in server_policies:
                # Extract ID from URN if needed
                if "urn" in policy and not policy.get("id"):
                    parts = policy["urn"].split(":")
                    if len(parts) >= 4:
                        policy["id"] = parts[3]

        return JsonResponse({
            "success": True,
            "connected": connected,
            "policies": server_policies
        })

    except Exception as e:
        logger.error(f"Error retrieving policies: {str(e)}", exc_info=True)
        return JsonResponse({
            "success": False,
            "error": str(e),
            "connected": connected,
            "policies": []
        })


def policy_create(request):
    """Create a new policy."""
    # Check if creating a local policy
    is_local = request.GET.get("local", "false").lower() in (
        "true",
        "t",
        "yes",
        "y",
        "1",
    )

    # Only require DataHub connection for non-local policies
    client = get_datahub_client_from_request(request)
    connected = client and client.test_connection()

    if not is_local and not connected:
        messages.warning(
            request, "Not connected to DataHub. Please check your connection settings."
        )
        return redirect("policies")

    if request.method == "POST":
        form = PolicyForm(request.POST)
        if form.is_valid():
            policy_id = form.cleaned_data.get("policy_id")

            if not policy_id and is_local:
                # Generate a random ID for local policies if not provided
                policy_id = f"local-policy-{uuid.uuid4().hex[:8]}"

            policy_data = {
                "id": policy_id,
                "name": form.cleaned_data["policy_name"],
                "description": form.cleaned_data.get("description", ""),
                "type": form.cleaned_data["policy_type"],
                "state": form.cleaned_data["policy_state"],
            }

            # Handle JSON fields
            for field in ["resources", "privileges", "actors"]:
                try:
                    json_data = form.cleaned_data.get(f"policy_{field}", "[]")
                    if json_data:
                        json.loads(json_data)  # Validate JSON
                    policy_data[field] = json_data
                except Exception as e:
                    messages.error(request, f"Invalid JSON in {field} field: {str(e)}")
                    return render(
                        request,
                        "policies/create.html",
                        {
                            "title": "Create Policy",
                            "form": form,
                            "is_local": is_local,
                            "connected": connected,
                            "environments": Environment.objects.all(),
                        },
                    )

            if is_local:
                # Create a local policy record
                try:
                    # Get environment if specified
                    environment_id = request.POST.get("environment")
                    environment = None
                    if environment_id:
                        try:
                            environment = Environment.objects.get(id=environment_id)
                        except Environment.DoesNotExist:
                            pass

                    # Create the Policy model instance
                    Policy.objects.create(
                        id=policy_data["id"],
                        name=policy_data["name"],
                        description=policy_data["description"],
                        type=policy_data["type"],
                        state=policy_data["state"],
                        resources=policy_data.get("resources", "[]"),
                        privileges=policy_data.get("privileges", "[]"),
                        actors=policy_data.get("actors", "{}"),
                        environment=environment,
                    )

                    messages.success(
                        request,
                        f"Local policy '{policy_data['name']}' created successfully",
                    )
                    return redirect("policies")
                except Exception as e:
                    messages.error(request, f"Error creating local policy: {str(e)}")
                    logger.error(
                        f"Error creating local policy: {str(e)}", exc_info=True
                    )
            else:
                # Create a policy in DataHub
                try:
                    result = client.create_policy(policy_data)
                    if result:
                        # Invalidate policies cache
                        from utils.datahub_utils import invalidate_policies_cache
                        invalidate_policies_cache()
                        
                        messages.success(
                            request,
                            f"Policy '{policy_data['name']}' created successfully",
                        )
                        return redirect("policies")
                    else:
                        messages.error(request, "Failed to create policy")
                except Exception as e:
                    messages.error(request, f"Error creating policy: {str(e)}")
                    logger.error(f"Error creating policy: {str(e)}", exc_info=True)
    else:
        form = PolicyForm()

    return render(
        request,
        "policies/create.html",
        {
            "title": "Create Policy",
            "form": form,
            "is_local": is_local,
            "connected": connected,
            "environments": Environment.objects.all(),
        },
    )


def policy_edit(request, policy_id):
    """Edit a policy."""
    # First, check if it's a local policy
    try:
        policy = Policy.objects.get(id=policy_id)
        is_local = True
    except Policy.DoesNotExist:
        is_local = False
        policy = None

    # If not a local policy, try to get from DataHub
    if not is_local:
        client = get_datahub_client_from_request(request)
        if not client or not client.test_connection():
            messages.warning(
                request,
                "Not connected to DataHub. Please check your connection settings.",
            )
            return redirect("policies")

        try:
            policy_data = client.get_policy(policy_id)
            if not policy_data:
                messages.error(request, f"Policy with ID {policy_id} not found")
                return redirect("policies")
        except Exception as e:
            messages.error(request, f"Error retrieving policy: {str(e)}")
            return redirect("policies")

    if request.method == "POST":
        form = PolicyForm(request.POST)
        if form.is_valid():
            if is_local:
                # Update local policy
                try:
                    # Get environment if specified
                    environment_id = request.POST.get("environment")
                    environment = None
                    if environment_id:
                        try:
                            environment = Environment.objects.get(id=environment_id)
                        except Environment.DoesNotExist:
                            pass

                    # Update policy fields
                    policy.name = form.cleaned_data["policy_name"]
                    policy.description = form.cleaned_data.get("description", "")
                    policy.type = form.cleaned_data["policy_type"]
                    policy.state = form.cleaned_data["policy_state"]

                    # Handle JSON fields
                    policy.resources = form.cleaned_data.get("policy_resources", "[]")
                    policy.privileges = form.cleaned_data.get("policy_privileges", "[]")
                    policy.actors = form.cleaned_data.get("policy_actors", "{}")

                    # Set environment
                    policy.environment = environment

                    policy.save()

                    messages.success(
                        request, f"Local policy '{policy.name}' updated successfully"
                    )
                    return redirect("policies")
                except Exception as e:
                    messages.error(request, f"Error updating local policy: {str(e)}")
                    logger.error(
                        f"Error updating local policy: {str(e)}", exc_info=True
                    )
            else:
                # Update a policy in DataHub
                try:
                    # Prepare policy data for DataHub
                    policy_data = {
                        "id": policy_id,
                        "name": form.cleaned_data["policy_name"],
                        "description": form.cleaned_data.get("description", ""),
                        "type": form.cleaned_data["policy_type"],
                        "state": form.cleaned_data["policy_state"],
                    }

                    # Handle JSON fields
                    try:
                        resources = json.loads(
                            form.cleaned_data.get("policy_resources", "[]")
                        )
                        privileges = json.loads(
                            form.cleaned_data.get("policy_privileges", "[]")
                        )
                        actors = json.loads(
                            form.cleaned_data.get("policy_actors", "{}")
                        )

                        policy_data["resources"] = resources
                        policy_data["privileges"] = privileges
                        policy_data["actors"] = actors
                    except Exception as e:
                        messages.error(request, f"Invalid JSON format: {str(e)}")
                        return render(
                            request,
                            "policies/edit.html",
                            {
                                "title": "Edit Policy",
                                "form": form,
                                "policy": policy_data,
                                "is_local": is_local,
                                "environments": Environment.objects.all(),
                            },
                        )

                    # Update the policy in DataHub
                    result = client.update_policy(policy_id, policy_data)
                    if result:
                        messages.success(
                            request,
                            f"Policy '{policy_data['name']}' updated successfully",
                        )
                        return redirect("policies")
                    else:
                        messages.error(request, "Failed to update policy")
                except Exception as e:
                    messages.error(request, f"Error updating policy: {str(e)}")
                    logger.error(f"Error updating policy: {str(e)}", exc_info=True)
    else:
        # Initialize form with policy data
        if is_local:
            initial_data = {
                "policy_id": policy.id,
                "policy_name": policy.name,
                "description": policy.description,
                "policy_type": policy.type,
                "policy_state": policy.state,
                "policy_resources": policy.resources,
                "policy_privileges": policy.privileges,
                "policy_actors": policy.actors,
            }
        else:
            # Convert DataHub policy to form data
            initial_data = {
                "policy_id": policy_data.get("id", ""),
                "policy_name": policy_data.get("name", ""),
                "description": policy_data.get("description", ""),
                "policy_type": policy_data.get("type", ""),
                "policy_state": policy_data.get("state", ""),
            }

            # Handle JSON fields
            if "resources" in policy_data:
                initial_data["policy_resources"] = json.dumps(
                    policy_data["resources"], indent=2
                )

            if "privileges" in policy_data:
                initial_data["policy_privileges"] = json.dumps(
                    policy_data["privileges"], indent=2
                )

            if "actors" in policy_data:
                initial_data["policy_actors"] = json.dumps(
                    policy_data["actors"], indent=2
                )

        form = PolicyForm(initial=initial_data)

    return render(
        request,
        "policies/edit.html",
        {
            "title": "Edit Policy",
            "form": form,
            "policy": policy or policy_data,
            "is_local": is_local,
            "environments": Environment.objects.all(),
        },
    )


def policy_import(request):
    """Import a policy from a file."""
    if request.method == "POST":
        form = PolicyImportForm(request.POST, request.FILES)
        if form.is_valid():
            policy_file = request.FILES["policy_file"]

            try:
                # Read and parse the policy file
                content = policy_file.read().decode("utf-8")
                json.loads(content)

                # Run the import_policy.py script
                with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as temp:
                    temp.write(content.encode("utf-8"))
                    temp_path = temp.name

                script_path = os.path.join(
                    os.path.dirname(
                        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                    ),
                    "scripts",
                    "import_policy.py",
                )

                try:
                    subprocess.run(
                        [sys.executable, script_path, "--input-file", temp_path],
                        capture_output=True,
                        text=True,
                        check=True,
                    )
                    messages.success(request, "Policy imported successfully")
                    return redirect("policies")
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

    return render(request, "policies/import.html", {"form": form})


@require_POST
def policy_delete(request, policy_id):
    """Delete a policy."""
    client = get_datahub_client_from_request(request)
    if not client or not client.test_connection():
        messages.error(request, "Not connected to DataHub")
        return redirect("policies")

    try:
        result = client.delete_policy(policy_id)
        if result:
            # Invalidate policies cache
            from utils.datahub_utils import invalidate_policies_cache
            invalidate_policies_cache()
            
            messages.success(request, f"Policy '{policy_id}' deleted successfully")
            return redirect("policies")
        else:
            messages.error(request, "Failed to delete policy")
            return redirect("policies")
    except Exception as e:
        messages.error(request, f"Error deleting policy: {str(e)}")
        return redirect("policies")


def policy_download(request, policy_id):
    """Download a policy as JSON."""
    client = get_datahub_client_from_request(request)
    if not client or not client.test_connection():
        messages.error(request, "Not connected to DataHub")
        return redirect("policies")

    # Get the policy
    policy_data = client.get_policy(policy_id)
    if not policy_data:
        messages.error(request, f"Policy {policy_id} not found")
        return redirect("policies")

    # Extract file name from policy name or ID
    filename = (
        f"policy_{policy_data.get('name', policy_id).replace(' ', '_').lower()}.json"
    )

    # Prepare response
    response = HttpResponse(
        json.dumps(policy_data, indent=2), content_type="application/json"
    )
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


def policy_detail(request, policy_id):
    """Display policy details."""
    client = get_datahub_client_from_request(request)
    policy = None

    if client and client.test_connection():
        try:
            # First try to get policy by ID directly
            policy = client.get_policy(policy_id)

            # If not found, try to get policy by URN (in case policy_id is actually a URN)
            if not policy and policy_id.startswith("urn:"):
                parts = policy_id.split(":")
                if len(parts) >= 4:
                    actual_policy_id = parts[3]
                    policy = client.get_policy(actual_policy_id)

            if not policy:
                messages.error(request, f"Policy with ID '{policy_id}' not found")
                return redirect("policies")

            # Ensure policy has an ID
            if not policy.get("id") and "urn" in policy:
                policy_urn = policy["urn"]
                parts = policy_urn.split(":")
                if len(parts) >= 4:
                    policy["id"] = parts[3]

            # Prepare JSON representations for display
            policy_json = json.dumps(policy, indent=2)
            resources_json = json.dumps(policy.get("resources", []), indent=2)
            privileges_json = json.dumps(policy.get("privileges", []), indent=2)
            actors_json = json.dumps(policy.get("actors", {}), indent=2)

        except Exception as e:
            messages.error(request, f"Error fetching policy: {str(e)}")
            return redirect("policies")
    else:
        messages.warning(request, "Not connected to DataHub")
        return redirect("home")

    return render(
        request,
        "policies/detail.html",
        {
            "title": f"Policy: {policy.get('name', '')}",
            "policy": policy,
            "policy_json": policy_json,
            "resources_json": resources_json,
            "privileges_json": privileges_json,
            "actors_json": actors_json,
        },
    )


def policy_export_all(request):
    """Export all policies to JSON files in a zip archive."""
    client = get_datahub_client_from_request(request)
    if not client or not client.test_connection():
        messages.error(request, "Not connected to DataHub")
        return redirect("policies")

    try:
        # Create a temporary directory for policies
        output_dir = tempfile.mkdtemp()

        # Get the export script path
        script_path = os.path.join(
            os.path.dirname(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            ),
            "scripts",
            "export_policy.py",
        )

        # Run the export script
        subprocess.run(
            [sys.executable, script_path, "--output-dir", output_dir],
            capture_output=True,
            text=True,
            check=True,
        )

        # Create a zip file
        zip_path = os.path.join(
            tempfile.gettempdir(),
            f"datahub_policies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
        )
        shutil.make_archive(zip_path[:-4], "zip", output_dir)

        # Clean up the temp directory
        shutil.rmtree(output_dir, ignore_errors=True)

        # Serve the zip file
        with open(zip_path, "rb") as f:
            response = HttpResponse(f.read(), content_type="application/zip")
            response["Content-Disposition"] = (
                'attachment; filename="datahub_policies.zip"'
            )

        # Clean up the zip file
        os.unlink(zip_path)

        return response

    except Exception as e:
        messages.error(request, f"Error exporting policies: {str(e)}")
        return redirect("policies")


def export_all_policies(request):
    """Alias for policy_export_all for consistency."""
    return policy_export_all(request)


def export_all_recipes(request):
    """Export all recipes to JSON files in a zip archive."""
    client = get_datahub_client_from_request(request)
    if not client or not client.test_connection():
        messages.error(request, "Not connected to DataHub")
        return redirect("recipes")

    try:
        # Create a temporary directory for recipes
        output_dir = tempfile.mkdtemp()

        # Get all recipes
        recipes_list = client.list_ingestion_sources()

        # Save each recipe to a file
        for recipe in recipes_list:
            recipe_id = (
                recipe.get("urn", "").split(":")[-1]
                if recipe.get("urn")
                else recipe.get("id", "unknown")
            )
            recipe_name = recipe.get("name", "Unnamed").replace(" ", "_").lower()
            filename = f"{recipe_name}_{recipe_id}.json"

            # Extract recipe content
            recipe_content = recipe.get("config", {}).get("recipe", "{}")
            if isinstance(recipe_content, str):
                try:
                    recipe_content = json.loads(recipe_content)
                except:
                    pass

            # Write recipe to file
            with open(os.path.join(output_dir, filename), "w") as f:
                json.dump(recipe_content, f, indent=2)

        # Create a zip file
        zip_path = os.path.join(
            tempfile.gettempdir(),
            f"datahub_recipes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
        )
        shutil.make_archive(zip_path[:-4], "zip", output_dir)

        # Clean up the temp directory
        shutil.rmtree(output_dir, ignore_errors=True)

        # Serve the zip file
        with open(zip_path, "rb") as f:
            response = HttpResponse(f.read(), content_type="application/zip")
            response["Content-Disposition"] = (
                'attachment; filename="datahub_recipes.zip"'
            )

        # Clean up the zip file
        os.unlink(zip_path)

        return response

    except Exception as e:
        messages.error(request, f"Error exporting recipes: {str(e)}")
        return redirect("recipes")


def export_all_templates(request):
    """Export all recipe templates to JSON files in a zip archive."""
    try:
        # Get all recipe templates from the database
        templates = RecipeTemplate.objects.all()

        if not templates:
            messages.warning(request, "No recipe templates found to export")
            return redirect("recipe_templates")

        # Create a temporary directory for templates
        output_dir = tempfile.mkdtemp()

        # Save each template to a file
        for template in templates:
            template_name = template.name.replace(" ", "_").lower()
            filename = f"{template_name}_{template.id}.json"

            # Extract template content
            template_content = template.content
            if isinstance(template_content, str):
                try:
                    template_content = json.loads(template_content)
                except:
                    pass

            # Write template to file
            with open(os.path.join(output_dir, filename), "w") as f:
                json.dump(template_content, f, indent=2)

        # Create a zip file
        zip_path = os.path.join(
            tempfile.gettempdir(),
            f"datahub_templates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
        )
        shutil.make_archive(zip_path[:-4], "zip", output_dir)

        # Clean up the temp directory
        shutil.rmtree(output_dir, ignore_errors=True)

        # Serve the zip file
        with open(zip_path, "rb") as f:
            response = HttpResponse(f.read(), content_type="application/zip")
            response["Content-Disposition"] = (
                'attachment; filename="datahub_templates.zip"'
            )

        # Clean up the zip file
        os.unlink(zip_path)

        return response

    except Exception as e:
        messages.error(request, f"Error exporting templates: {str(e)}")
        return redirect("recipe_templates")


def settings(request):
    """View and update application settings"""
    try:
        if request.method == "POST":
            section = request.POST.get("section", "")

            # Legacy connection handling removed - use Connections management instead
            if section == "connection" or section == "datahub_connection":
                messages.warning(request, "Legacy connection settings have been disabled. Please use the new Connection Management system.")
                return redirect("connections_list")

            elif section == "github_settings":
                # Handle GitHub settings
                from web_ui.models import GitSettings
                
                enabled = "enabled" in request.POST
                provider_type = request.POST.get("provider_type", "github")
                base_url = request.POST.get("base_url", "").strip()
                username = request.POST.get("username", "").strip()
                repository = request.POST.get("repository", "").strip()
                token = request.POST.get("token", "").strip()
                
                # Get or create GitSettings instance
                git_settings = GitSettings.objects.first()
                if not git_settings:
                    git_settings = GitSettings.objects.create()
                
                # Update settings
                git_settings.enabled = enabled
                git_settings.provider_type = provider_type
                git_settings.base_url = base_url
                git_settings.username = username
                git_settings.repository = repository
                if token:  # Only update token if provided
                    git_settings.token = token
                git_settings.save()
                
                # Test connection if requested
                if "test_github_connection" in request.POST:
                    try:
                        # Test GitHub connection logic here
                        messages.success(request, "GitHub connection test completed")
                    except Exception as e:
                        messages.error(request, f"GitHub connection test failed: {str(e)}")
                else:
                    messages.success(request, "Git repository settings updated")
                
                return redirect("settings")

            elif section == "policy_settings":
                # Handle policy settings
                policy_export_dir = request.POST.get("policy_export_dir", "").strip()
                default_policy_type = request.POST.get("default_policy_type", "METADATA")
                validate_on_import = "validate_on_import" in request.POST
                auto_backup_policies = "auto_backup_policies" in request.POST
                
                AppSettings.set("policy_export_dir", policy_export_dir)
                AppSettings.set("default_policy_type", default_policy_type)
                AppSettings.set("validate_on_import", "true" if validate_on_import else "false")
                AppSettings.set("auto_backup_policies", "true" if auto_backup_policies else "false")
                
                messages.success(request, "Policy settings updated")
                return redirect("settings")

            elif section == "recipe_settings":
                # Handle recipe settings
                recipe_dir = request.POST.get("recipe_dir", "").strip()
                default_schedule = request.POST.get("default_schedule", "0 0 * * *")
                auto_enable_recipes = "auto_enable_recipes" in request.POST
                
                AppSettings.set("recipe_dir", recipe_dir)
                AppSettings.set("default_schedule", default_schedule)
                AppSettings.set("auto_enable_recipes", "true" if auto_enable_recipes else "false")
                
                messages.success(request, "Recipe settings updated")
                return redirect("settings")

            elif section == "advanced_settings":
                # Handle advanced settings
                timeout = request.POST.get("timeout", "30")
                log_level = request.POST.get("log_level", "INFO")
                refresh_rate = request.POST.get("refresh_rate", "60")
                debug_mode = "debug_mode" in request.POST
                
                # Validate timeout
                try:
                    timeout_int = int(timeout)
                    if timeout_int < 5 or timeout_int > 300:
                        timeout_int = 30
                except (ValueError, TypeError):
                    timeout_int = 30
                
                # Validate refresh_rate
                try:
                    refresh_rate_int = int(refresh_rate)
                    if refresh_rate_int < 0 or refresh_rate_int > 3600:
                        refresh_rate_int = 60
                except (ValueError, TypeError):
                    refresh_rate_int = 60
                
                AppSettings.set("timeout", str(timeout_int))
                AppSettings.set("log_level", log_level)
                AppSettings.set("refresh_rate", str(refresh_rate_int))
                AppSettings.set("debug_mode", "true" if debug_mode else "false")
                
                messages.success(request, "Advanced settings updated")
                return redirect("settings")

        # Get current settings for display
        try:

            # Git settings
            from web_ui.models import GitSettings
            git_settings = GitSettings.objects.first()
            if not git_settings:
                git_settings = GitSettings.objects.create()

            # Create a form-like object for the template
            github_form = {
                'enabled': {'value': git_settings.enabled},
                'provider_type': {'value': git_settings.provider_type},
                'base_url': {'value': git_settings.base_url or ''},
                'username': {'value': git_settings.username or ''},
                'repository': {'value': git_settings.repository or ''},
                'token': {'value': git_settings.token or ''},
            }

            # Other settings
            config = {
                'policy_export_dir': AppSettings.get("policy_export_dir", ""),
                'default_policy_type': AppSettings.get("default_policy_type", "METADATA"),
                'validate_on_import': AppSettings.get_bool("validate_on_import", True),
                'auto_backup_policies': AppSettings.get_bool("auto_backup_policies", True),
                'recipe_dir': AppSettings.get("recipe_dir", ""),
                'default_schedule': AppSettings.get("default_schedule", "0 0 * * *"),
                'auto_enable_recipes': AppSettings.get_bool("auto_enable_recipes", False),
                'log_level': AppSettings.get("log_level", "INFO"),
                'refresh_rate': AppSettings.get_int("refresh_rate", 60),
                'debug_mode': AppSettings.get_bool("debug_mode", False),
            }

            context = {
                "github_form": github_form,
                "github_configured": git_settings.enabled and git_settings.token and git_settings.username and git_settings.repository,
                "config": config,
                "page_title": "Settings",
            }

            return render(request, "settings.html", context)

        except Exception as e:
            logger.error(f"Error getting settings: {str(e)}")
            messages.error(request, f"Error getting settings: {str(e)}")
            return render(request, "settings.html", {"error": str(e)})

    except Exception as e:
        logger.error(f"Error in settings view: {str(e)}")
        messages.error(request, f"An error occurred: {str(e)}")
        return render(request, "settings.html", {"error": str(e)})


def health(request):
    """Health check endpoint."""
    client = get_datahub_client_from_request(request)
    status = "OK" if client and client.test_connection() else "Disconnected"
    return HttpResponse(status)


def logs(request):
    """View application logs."""
    # Get the configured log level
    log_level = AppSettings.get("log_level", "INFO")

    # Handle POST requests for clearing logs
    if request.method == "POST" and request.POST.get("action") == "clear_logs":
        try:
            # Get clear filters
            clear_level = request.POST.get("clear_level", "")
            clear_before_date = request.POST.get("clear_before_date", "")

            # Start with all logs
            logs_to_delete = LogEntry.objects.all()

            # Apply level filter if provided
            if clear_level:
                level_order = {
                    "DEBUG": 1,
                    "INFO": 2,
                    "WARNING": 3,
                    "ERROR": 4,
                    "CRITICAL": 5,
                }
                levels_to_delete = [
                    l
                    for l, n in level_order.items()
                    if n <= level_order.get(clear_level, 0)
                ]
                if levels_to_delete:
                    logs_to_delete = logs_to_delete.filter(level__in=levels_to_delete)

            # Apply date filter if provided
            if clear_before_date:
                try:
                    from datetime import datetime

                    date = datetime.strptime(clear_before_date, "%Y-%m-%d").date()
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
            LogEntry.info(
                f"User cleared {count} log entries.", source="web_ui.views.logs"
            )

        except Exception as e:
            messages.error(request, f"Error clearing logs: {str(e)}")
            logger.error(f"Error clearing logs: {str(e)}")

    # Generate test logs if requested (for development)
    if request.GET.get("generate_test_logs") == "1":
        generate_test_logs()
        messages.success(request, "Generated test logs for demonstration purposes.")

    # Get filter parameters from request
    level_filter = request.GET.get("level", "")
    source_filter = request.GET.get("source", "")
    search_query = request.GET.get("search", "")
    date_filter = request.GET.get("date", "")

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

            date = datetime.strptime(date_filter, "%Y-%m-%d").date()
            logs_query = logs_query.filter(timestamp__date=date)
        except ValueError:
            # If date parsing fails, ignore this filter
            pass

    # Paginate the results
    from django.core.paginator import Paginator

    paginator = Paginator(logs_query, 50)  # Show 50 logs per page
    page_number = request.GET.get("page", 1)
    logs_page = paginator.get_page(page_number)

    # Get unique sources for the filter dropdown (exclude empty/null sources)
    sources = LogEntry.objects.exclude(
        source__isnull=True
    ).exclude(
        source__exact=""
    ).exclude(
        source__regex=r'^\s*$'  # Exclude whitespace-only sources
    ).values_list("source", flat=True).distinct().order_by("source")
    
    # Convert to list and ensure no duplicates (extra safety)
    sources = sorted(set(sources))

    # Create some test data for debugging the expand/collapse functionality

    return render(
        request,
        "logs.html",
        {
            "title": "Logs",
            "logs": logs_page,
            "sources": sources,
            "log_levels": [level[0] for level in LogEntry.LEVEL_CHOICES],
            "current_level": level_filter,
            "current_source": source_filter,
            "search_query": search_query,
            "date_filter": date_filter,
            "configured_level": log_level,
        },
    )


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
    for i in range(100):
        # Random timestamp in the last 7 days
        timestamp = now - timedelta(
            days=random.randint(0, 7),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )

        # Random log level
        level_choice = random.choices(
            ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            weights=[40, 30, 20, 8, 2],  # More debug/info, fewer errors/critical
            k=1,
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
            details=details,
        )
        log.save()


def policy_view(request, policy_id):
    """View a specific policy."""
    policy = None

    # First try to fetch from local database
    try:
        policy = Policy.objects.get(id=policy_id)
        # Format JSON fields for display
        policy_json = json.dumps(policy.to_dict(), indent=2)
        resources_json = policy.resources_json
        privileges_json = policy.privileges_json
        actors_json = policy.actors_json

        # Ensure the policy has an ID (for local policies this should always be set)
        # But we double-check just to be safe
        if not hasattr(policy, "id") or not policy.id:
            policy.id = policy_id

        is_local = True
    except Policy.DoesNotExist:
        is_local = False

        # If not found locally, try to fetch from DataHub
        client = get_datahub_client_from_request(request)
    if client and client.test_connection():
        try:
            # First try to fetch by ID directly
            policy = client.get_policy(policy_id)

            # If not found, try with URN format
            if not policy and not policy_id.startswith("urn:"):
                policy = client.get_policy(f"urn:li:policy:{policy_id}")

            if policy:
                # Ensure the policy has an ID
                if not policy.get("id"):
                    if policy.get("urn"):
                        parts = policy.get("urn").split(":")
                        if len(parts) >= 4:
                            policy["id"] = parts[3]
                    else:
                        # If no ID or URN is available, use the policy_id from the request
                        policy["id"] = policy_id

                # Format JSON fields for display
                policy_json = json.dumps(policy, indent=2)
                resources_json = json.dumps(policy.get("resources", []), indent=2)
                privileges_json = json.dumps(policy.get("privileges", []), indent=2)
                actors_json = json.dumps(policy.get("actors", {}), indent=2)
            else:
                messages.error(request, f"Policy with ID {policy_id} not found")

        except Exception as e:
            messages.error(request, f"Error retrieving policy: {str(e)}")
            logger.error(f"Error retrieving policy: {str(e)}")
    else:
        messages.warning(request, "Not connected to DataHub")

    if not policy:
        # If policy was not found in either place, redirect to policies list
        return redirect("policies")

    return render(
        request,
        "policies/view.html",
        {
            "title": "Policy Details",
            "policy": policy,
            "policy_json": policy_json if policy else "{}",
            "resources_json": resources_json if policy else "[]",
            "privileges_json": privileges_json if policy else "[]",
            "actors_json": actors_json if policy else "{}",
            "is_local": is_local,
        },
    )


# Recipe Templates Management Views
def recipe_templates(request):
    """List all recipe templates."""
    templates = RecipeTemplate.objects.all().order_by("-updated_at")
    env_vars_instances = EnvVarsInstance.objects.all().order_by("-updated_at")
    recipe_instances = RecipeInstance.objects.all().order_by("-updated_at")

    # Handle filtering
    tag_filter = request.GET.get("tag")
    if tag_filter:
        # Filter by tag (simple contains for comma-separated tags)
        templates = templates.filter(tags__contains=tag_filter)

    # Handle search
    search_query = request.GET.get("search")
    if search_query:
        templates = templates.filter(
            models.Q(name__icontains=search_query)
            | models.Q(description__icontains=search_query)
            | models.Q(recipe_type__icontains=search_query)
        )

    # Get unique tags for filter dropdown
    all_tags = set()
    for template in RecipeTemplate.objects.all():
        if template.tags:
            all_tags.update(template.get_tags_list())

    return render(
        request,
        "recipes/templates/list.html",
        {
            "title": "Recipe Templates",
            "templates": templates,
            "env_vars_instances": env_vars_instances,
            "recipe_instances": recipe_instances,
            "tag_filter": tag_filter,
            "search_query": search_query,
            "all_tags": sorted(all_tags),
        },
    )


def recipe_template_detail(request, template_id):
    """View a recipe template details."""
    template = get_object_or_404(RecipeTemplate, id=template_id)

    # Process content for display
    if template.content.strip().startswith("{"):
        content_type = "json"
        try:
            formatted_content = json.dumps(json.loads(template.content), indent=2)
        except:
            formatted_content = template.content
    else:
        content_type = "yaml"
        formatted_content = template.content

    return render(
        request,
        "recipes/templates/detail.html",
        {
            "title": f"Template: {template.name}",
            "template": template,
            "content": formatted_content,
            "content_type": content_type,
            "tags": template.get_tags_list(),
        },
    )


def recipe_template_create(request):
    """Create a new recipe template."""
    if request.method == "POST":
        form = RecipeTemplateForm(request.POST)
        if form.is_valid():
            # Create the template
            template = RecipeTemplate(
                name=form.cleaned_data["name"],
                description=form.cleaned_data["description"],
                recipe_type=form.cleaned_data["recipe_type"],
                content=form.cleaned_data["content"],
                executor_id=form.cleaned_data.get("executor_id", "default"),
                cron_schedule=form.cleaned_data.get("cron_schedule", "0 0 * * *"),
                timezone=form.cleaned_data.get("timezone", "Etc/UTC"),
            )

            # Handle tags
            if form.cleaned_data["tags"]:
                template.set_tags_list(
                    [tag.strip() for tag in form.cleaned_data["tags"].split(",")]
                )

            template.save()
            messages.success(
                request, f"Template '{template.name}' created successfully"
            )
            return redirect("recipe_templates")
    else:
        form = RecipeTemplateForm()

    return render(
        request,
        "recipes/templates/create.html",
        {"title": "Create Recipe Template", "form": form},
    )


def recipe_template_edit(request, template_id):
    """Edit a recipe template."""
    template = get_object_or_404(RecipeTemplate, id=template_id)

    if request.method == "POST":
        form = RecipeTemplateForm(request.POST)
        if form.is_valid():
            # Update the template
            template.name = form.cleaned_data["name"]
            template.description = form.cleaned_data["description"]
            template.recipe_type = form.cleaned_data["recipe_type"]
            template.content = form.cleaned_data["content"]
            template.executor_id = form.cleaned_data.get("executor_id", "default")
            template.cron_schedule = form.cleaned_data.get("cron_schedule", "0 0 * * *")
            template.timezone = form.cleaned_data.get("timezone", "Etc/UTC")

            # Handle tags
            if form.cleaned_data["tags"]:
                template.set_tags_list(
                    [tag.strip() for tag in form.cleaned_data["tags"].split(",")]
                )
            else:
                template.tags = ""

            template.save()
            messages.success(
                request, f"Template '{template.name}' updated successfully"
            )
            return redirect("recipe_template_detail", template_id=template.id)
    else:
        form = RecipeTemplateForm(
            initial={
                "name": template.name,
                "description": template.description,
                "recipe_type": template.recipe_type,
                "content": template.content,
                "tags": template.tags,
                "executor_id": template.executor_id,
                "cron_schedule": template.cron_schedule,
                "timezone": template.timezone,
            }
        )

    return render(
        request,
        "recipes/templates/edit.html",
        {"title": "Edit Recipe Template", "form": form, "template": template},
    )


def recipe_template_delete(request, template_id):
    """Delete a recipe template."""
    template = get_object_or_404(RecipeTemplate, id=template_id)

    if request.method == "POST":
        template_name = template.name
        template.delete()
        messages.success(request, f"Template '{template_name}' deleted successfully")
        return redirect("recipe_templates")

    return render(
        request,
        "recipes/templates/delete.html",
        {"title": "Delete Recipe Template", "template": template},
    )


def recipe_template_export(request, template_id):
    """Export a recipe template to JSON or YAML."""
    template = get_object_or_404(RecipeTemplate, id=template_id)

    # Determine content type
    is_json = template.content.strip().startswith("{")

    # Set response content type
    content_type = "application/json" if is_json else "application/x-yaml"
    file_extension = "json" if is_json else "yaml"

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
    response["Content-Disposition"] = (
        f'attachment; filename="{template.name}.{file_extension}"'
    )
    return response


def recipe_template_import(request):
    """Import a recipe template from a file."""
    if request.method == "POST":
        form = RecipeTemplateImportForm(request.POST, request.FILES)
        if form.is_valid():
            template_file = request.FILES["template_file"]

            try:
                content = template_file.read().decode("utf-8")

                # Extract name and type from file content if possible
                if template_file.name.endswith(".json"):
                    recipe_data = json.loads(content)
                    recipe_name = recipe_data.get("name", "") or recipe_data.get(
                        "source_name", ""
                    )
                    recipe_type = recipe_data.get("type", "") or recipe_data.get(
                        "source", {}
                    ).get("type", "")
                else:
                    # For YAML, attempt to parse
                    import yaml

                    recipe_data = yaml.safe_load(content)
                    recipe_name = recipe_data.get("name", "") or recipe_data.get(
                        "source_name", ""
                    )
                    recipe_type = recipe_data.get("type", "") or recipe_data.get(
                        "source", {}
                    ).get("type", "")

                # Create a pre-filled form for the template
                initial_data = {
                    "name": recipe_name or os.path.splitext(template_file.name)[0],
                    "recipe_type": recipe_type or "",
                    "content": content,
                    "tags": form.cleaned_data.get("tags", ""),
                }

                template_form = RecipeTemplateForm(initial=initial_data)
                return render(
                    request,
                    "recipes/templates/create.html",
                    {"form": template_form, "imported": True},
                )

            except Exception as e:
                messages.error(request, f"Error parsing template file: {str(e)}")
    else:
        form = RecipeTemplateImportForm()

    return render(
        request,
        "recipes/templates/import.html",
        {"title": "Import Recipe Template", "form": form},
    )


def recipe_template_deploy(request, template_id):
    """Deploy a recipe template to DataHub."""
    from .models import RecipeManager
    import uuid

    template = get_object_or_404(RecipeTemplate, id=template_id)
    client = get_datahub_client_from_request(request)

    if not client or not client.test_connection():
        messages.error(request, "Not connected to DataHub")
        return redirect("recipe_template_detail", template_id=template_id)

    if request.method == "POST":
        form = RecipeDeployForm(request.POST)

        if form.is_valid():
            try:
                # Parse recipe content
                if template.content.strip().startswith("{"):
                    recipe = json.loads(template.content)
                else:
                    import yaml

                    recipe = yaml.safe_load(template.content)

                # Process environment variables
                if form.cleaned_data.get("environment_variables"):
                    try:
                        env_vars = json.loads(
                            form.cleaned_data["environment_variables"]
                        )
                        recipe = replace_env_vars_with_values(recipe, env_vars)

                        # Store secrets in the database for future reference
                        store_recipe_secrets(form.cleaned_data["recipe_id"], env_vars)
                    except json.JSONDecodeError:
                        logger.error(
                            f"Invalid JSON in environment variables: {form.cleaned_data['environment_variables']}"
                        )

                # Prepare schedule if provided
                schedule = None
                if form.cleaned_data["schedule_cron"]:
                    schedule = {
                        "interval": form.cleaned_data["schedule_cron"],
                        "timezone": form.cleaned_data["schedule_timezone"] or "UTC",
                    }

                # Create the recipe in DataHub
                result = client.create_ingestion_source(
                    name=form.cleaned_data["recipe_name"],
                    type=template.recipe_type,
                    recipe=recipe,
                    source_id=form.cleaned_data["recipe_id"],
                    schedule=schedule,
                    executor_id=template.executor_id,
                    description=form.cleaned_data.get("description", ""),
                )

                if result:
                    messages.success(
                        request,
                        f"Recipe '{form.cleaned_data['recipe_name']}' deployed successfully",
                    )
                    return redirect("recipes")
                else:
                    messages.error(request, "Failed to deploy recipe")

            except Exception as e:
                messages.error(request, f"Error deploying recipe: {str(e)}")

    else:
        # Generate a default recipe ID and name based on template
        recipe_id = f"{template.recipe_type.lower()}-{uuid.uuid4().hex[:8]}"

        form = RecipeDeployForm(
            initial={
                "recipe_name": template.name,
                "recipe_id": recipe_id,
                "schedule_cron": RecipeManager.get_default_schedule(),
                "schedule_timezone": "UTC",
                "description": template.description,
            }
        )

    return render(
        request,
        "recipes/templates/deploy.html",
        {"title": "Deploy Recipe Template", "template": template, "form": form},
    )


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
        elif isinstance(node, str) and node.startswith("${") and node.endswith("}"):
            # Extract variable name
            var_name = node[2:-1]
            if var_name in env_vars:
                return env_vars[var_name]["value"]
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
        var_name: data["value"]
        for var_name, data in env_vars.items()
        if data.get("isSecret") and data.get("value")
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
                recipe_id=recipe_id, variable_name=var_name, value=value
            )
    except Exception as e:
        logger.error(f"Error storing recipe secrets: {str(e)}")


def recipe_save_as_template(request, recipe_id):
    """Save an existing recipe as a template."""
    client = get_datahub_client_from_request(request)
    if not client or not client.test_connection():
        messages.error(request, "Not connected to DataHub")
        return redirect("recipes")

    # Get the recipe
    recipe_data = client.get_ingestion_source(recipe_id)
    if not recipe_data:
        messages.error(request, f"Recipe {recipe_id} not found")
        return redirect("recipes")

    # Extract recipe details
    recipe_name = recipe_data.get("name", "")
    recipe_type = recipe_data.get("type", "")
    recipe_content = recipe_data.get("config", {}).get("recipe", "{}")
    original_content = recipe_content

    # Convert leaf values to environment variables
    if isinstance(recipe_content, dict):
        recipe_content = convert_to_template(recipe_content, recipe_type)
        # Convert to JSON string
        recipe_content = json.dumps(recipe_content, indent=2)

    if request.method == "POST":
        form = RecipeTemplateForm(request.POST)

        if form.is_valid():
            from .models import RecipeTemplate, EnvVarsTemplate, EnvVarsInstance
            import re

            # Create the template
            template = RecipeTemplate(
                name=form.cleaned_data["name"],
                description=form.cleaned_data["description"],
                recipe_type=form.cleaned_data["recipe_type"],
                content=form.cleaned_data["content"],
            )

            # Handle tags
            if form.cleaned_data["tags"]:
                template.set_tags_list(
                    [tag.strip() for tag in form.cleaned_data["tags"].split(",")]
                )

            template.save()

            # Extract environment variables
            env_vars = {}

            # Find all environment variables using regex
            regex = r"\${([^}]+)}"
            matches = re.findall(regex, form.cleaned_data["content"])

            # Create environment variables dictionary for the template
            for var_name in matches:
                # Determine if it looks like a secret based on name
                is_secret = any(
                    keyword in var_name.lower()
                    for keyword in ["password", "secret", "key", "token", "auth"]
                )

                # Try to extract actual value from original content
                value = ""
                try:
                    # If we have the original dict content
                    if isinstance(original_content, dict):
                        # Try to find the value in the original content by traversing the path
                        parts = var_name.lower().split("_")
                        current = original_content
                        for part in parts:
                            # Try to find a key that matches (case insensitive)
                            found = False
                            for key in current.keys():
                                if key.lower() == part:
                                    current = current[key]
                                    found = True
                                    break

                            if not found:
                                break

                        # If we found a value and it's a leaf node, use it
                        if found and not isinstance(current, (dict, list)):
                            value = str(current)
                except Exception:
                    # If we can't extract the value, leave it blank
                    pass

                # Format for EnvVarsTemplate - different structure than EnvVarsInstance
                env_vars[var_name] = {
                    "description": f"Extracted from {recipe_name}",
                    "required": True,
                    "is_secret": is_secret,
                    "data_type": "text",
                    "default_value": value,
                }

            # Only create environment variables template if we have variables
            if env_vars:
                try:
                    # Create a matching environment variables template
                    env_vars_template = EnvVarsTemplate(
                        name=f"{template.name} Variables",
                        description=f"Environment variables template extracted from {recipe_name}",
                        recipe_type=template.recipe_type,
                        variables=json.dumps(env_vars),
                    )
                    env_vars_template.save()
                    messages.success(
                        request,
                        f"Created environment variables template '{env_vars_template.name}' with {len(env_vars)} variables",
                    )

                    # Also create an instance from the template
                    env_vars_instance = EnvVarsInstance(
                        name=f"{template.name} Variables Instance",
                        description=f"Environment variables for {recipe_name}",
                        template=env_vars_template,
                        recipe_type=template.recipe_type,
                        variables=json.dumps(
                            {
                                k: {
                                    "value": v.get("default_value", ""),
                                    "isSecret": v.get("is_secret", False),
                                }
                                for k, v in env_vars.items()
                            }
                        ),
                    )
                    env_vars_instance.save()
                    messages.success(
                        request,
                        f"Created environment variables instance '{env_vars_instance.name}'",
                    )

                    # Also create a recipe instance linking them
                    from .models import RecipeInstance

                    instance = RecipeInstance(
                        name=f"{template.name} Instance",
                        description=f"Instance created from {recipe_name}",
                        template=template,
                        env_vars_instance=env_vars_instance,
                    )
                    instance.save()
                    messages.success(
                        request, f"Created recipe instance '{instance.name}'"
                    )
                except Exception as e:
                    messages.warning(
                        request,
                        f"Could not create environment variables template: {str(e)}",
                    )

            messages.success(
                request, f"Recipe saved as template '{template.name}' successfully"
            )
            return redirect("template_manager:recipe_templates")
    else:
        form = RecipeTemplateForm(
            initial={
                "name": f"{recipe_name} Template",
                "recipe_type": recipe_type,
                "content": recipe_content,
            }
        )

    return render(
        request,
        "recipes/templates/create.html",
        {"title": "Save Recipe as Template", "form": form, "recipe_id": recipe_id},
    )


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
                var_name = re.sub(r"[^A-Z0-9]", "_", var_name)
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
            if recipe_content.strip().startswith("{"):
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

        regex = r"\${([^}]+)}"
        matches = re.findall(regex, recipe_content)

        for var_name in matches:
            env_vars[var_name] = {"value": "", "isSecret": False}

    # Extract variables from dict recursively
    def extract_vars(node):
        if isinstance(node, dict):
            for key, value in node.items():
                extract_vars(value)
        elif isinstance(node, list):
            for item in node:
                extract_vars(item)
        elif isinstance(node, str) and node.startswith("${") and node.endswith("}"):
            var_name = node[2:-1]
            if var_name not in env_vars:
                env_vars[var_name] = {"value": "", "isSecret": False}

    # Extract variables from dict if we have one
    if isinstance(recipe_dict, dict):
        extract_vars(recipe_dict)

    # Get stored secrets for this recipe
    try:
        secrets = RecipeSecret.objects.filter(recipe_id=recipe_id)
        for secret in secrets:
            # If it's in env_vars, update the existing entry
            if secret.variable_name in env_vars:
                env_vars[secret.variable_name]["value"] = secret.value
                env_vars[secret.variable_name]["isSecret"] = secret.is_secret
            # Otherwise add a new entry
            else:
                env_vars[secret.variable_name] = {
                    "value": secret.value,
                    "isSecret": secret.is_secret,
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
            value=var_data.get("value", ""),
            is_secret=var_data.get("isSecret", False),
        )


def env_vars_templates(request):
    """List all environment variables templates."""
    templates = EnvVarsTemplate.objects.all().order_by("-updated_at")
    return render(request, "env_vars/templates.html", {"templates": templates})


def env_vars_template_create(request):
    """Create a new environment variables template."""
    EnvVarsTemplate.objects.count() > 0

    # Get all environments
    environments = Environment.objects.all().order_by("name")

    if request.method == "POST":
        form = EnvVarsTemplateForm(request.POST)
        if form.is_valid():
            template = form.save()
            messages.success(
                request, f"Template '{template.name}' created successfully."
            )
            return redirect("env_vars_templates")
    else:
        # Create an initial value with sample variable structure to help with debugging
        initial_data = {"variables": "{}"}
        form = EnvVarsTemplateForm(initial=initial_data)

    return render(
        request,
        "env_vars/template_form.html",
        {
            "form": form,
            "environments": environments,
            "is_new": True,
            "variables": "{}",
            "title": "Create Environment Variables Template",
            "data_types": EnvVarsTemplate.DATA_TYPES,
        },
    )


def env_vars_template_edit(request, template_id):
    """Edit an existing environment variables template."""
    template = get_object_or_404(EnvVarsTemplate, id=template_id)

    # Get all environments
    environments = Environment.objects.all().order_by("name")

    if request.method == "POST":
        form = EnvVarsTemplateForm(request.POST, instance=template)
        if form.is_valid():
            template = form.save()
            messages.success(
                request, f"Template '{template.name}' updated successfully."
            )
            return redirect("env_vars_templates")
    else:
        # Create initial data with the template's existing variables
        variables_dict = template.get_variables_dict()
        initial_data = {"variables": json.dumps(variables_dict)}
        form = EnvVarsTemplateForm(instance=template, initial=initial_data)

    # Get variables as a clean JSON string with no HTML escaping issues
    variables = template.get_variables_dict()

    # Convert variables from {name: props} to {id: {key: name, ...props}}
    # This format is expected by the template's JavaScript
    display_variables = {}
    for var_name, var_props in variables.items():
        var_id = f"var_{int(time.time())}_{randint(1000, 9999)}"
        display_variables[var_id] = {
            "key": var_name,
            "description": var_props.get("description", ""),
            "required": var_props.get("required", False),
            "is_secret": var_props.get("is_secret", False),
            "data_type": var_props.get("data_type", "text"),
            "default_value": var_props.get("default_value", ""),
        }

    # Log the variables for debugging
    print(f"Template variables for template {template_id}: {variables}")
    print(f"Display variables for template {template_id}: {display_variables}")

    # Generate a direct script tag that initializes variables as a global JavaScript object
    script_tag = f"""
    <script type="text/javascript">
        window.templateVariables = {json.dumps(display_variables)};
        console.log("Variables initialized from server:", window.templateVariables);
    </script>
    """

    return render(
        request,
        "env_vars/template_form.html",
        {
            "form": form,
            "template": template,
            "variables": json.dumps(display_variables),
            "variables_json": json.dumps(
                display_variables
            ),  # Add this to match instance_form approach
            "script_init": script_tag,
            "environments": environments,
            "is_new": False,
            "title": f"Edit Environment Variables Template: {template.name}",
            "data_types": EnvVarsTemplate.DATA_TYPES,
        },
    )


def env_vars_instances(request):
    """List all environment variables instances."""
    instances = EnvVarsInstance.objects.all().order_by("-updated_at")
    return render(request, "env_vars/instances.html", {"instances": instances})


def env_vars_instance_create(request):
    """Create a new environment variables instance."""

    # Get all templates
    templates = EnvVarsTemplate.objects.all().order_by("name")

    # Get all environments
    environments = Environment.objects.all().order_by("name")

    if request.method == "POST":
        form = EnvVarsInstanceForm(request.POST)

        if form.is_valid():
            # Get form data
            name = form.cleaned_data["name"]
            description = form.cleaned_data.get("description", "")
            template = form.cleaned_data[
                "template"
            ]  # This is already the template object
            recipe_type = form.cleaned_data["recipe_type"]
            variables = form.cleaned_data["variables"]
            environment = form.cleaned_data.get(
                "environment"
            )  # This is already the Environment object

            # Create instance
            instance = EnvVarsInstance(
                name=name,
                description=description,
                template=template,  # Use the template object directly
                recipe_type=recipe_type,
                variables=variables,
                environment=environment,  # Use the environment object directly
            )

            instance.save()
            messages.success(
                request, f"Environment variables instance '{name}' created successfully"
            )
            return redirect("env_vars_instances")

    else:
        form = EnvVarsInstanceForm()

    return render(
        request,
        "env_vars/instance_form.html",
        {
            "form": form,
            "templates": templates,
            "environments": environments,
            "is_new": True,
            "title": "Create Environment Variables Instance",
        },
    )


def recipe_instance_edit(request, instance_id):
    """Edit an existing recipe instance."""
    instance = get_object_or_404(RecipeInstance, id=instance_id)

    if request.method == "POST":
        form = RecipeInstanceForm(request.POST)
        if form.is_valid():
            # Update the instance
            instance.name = form.cleaned_data["name"]
            instance.description = form.cleaned_data["description"]
            instance.template = form.cleaned_data["template"]
            instance.env_vars_instance = form.cleaned_data["env_vars_instance"]
            instance.save()

            messages.success(
                request, f"Recipe instance '{instance.name}' updated successfully"
            )
            return redirect("recipe_instances")
    else:
        form = RecipeInstanceForm(
            initial={
                "name": instance.name,
                "description": instance.description,
                "template": instance.template.id if instance.template else None,
                "env_vars_instance": instance.env_vars_instance.id if instance.env_vars_instance else None,
            }
        )

    return render(
        request,
        "recipes/instance_form.html",
        {
            "form": form,
            "instance": instance,
            "title": f"Edit Recipe Instance: {instance.name}",
            "is_new": False,
        },
    )


def recipe_instance_delete(request, instance_id):
    """Delete a recipe instance."""
    instance = get_object_or_404(RecipeInstance, id=instance_id)

    if request.method == "POST":
        # Delete the instance
        instance_name = instance.name
        instance.delete()
        messages.success(
            request, f"Recipe instance '{instance_name}' deleted successfully"
        )
        return redirect("recipe_instances")

    return render(
        request, "recipes/instance_confirm_delete.html", {"instance": instance}
    )


def recipe_instance_deploy(request, instance_id):
    """Deploy a recipe instance to DataHub."""
    instance = get_object_or_404(RecipeInstance, id=instance_id)
    client = get_datahub_client_from_request(request)

    if not client or not client.test_connection():
        messages.error(request, "Not connected to DataHub")
        return redirect("recipe_instances")

    try:
        # Get combined content with env vars applied
        recipe_content = instance.get_combined_content()

        # Prepare the recipe for deployment
        recipe_data = {
            "name": instance.name,
            "description": instance.description or "",
            "recipe": recipe_content,
            "type": instance.template.recipe_type,
        }

        # Deploy to DataHub
        result = client.create_ingestion_source(recipe_data)

        if result:
            # Update the instance with deployment info
            instance.datahub_urn = result
            instance.deployed = True
            instance.deployed_at = timezone.now()
            instance.save()

            # If this instance has env vars with secrets, create them in DataHub
            if instance.env_vars_instance:
                secret_vars = instance.env_vars_instance.get_secret_variables()
                if secret_vars:
                    # Create secrets in DataHub
                    for var_name, var_data in secret_vars.items():
                        # TODO: Call DataHub API to create secrets
                        pass

                    # Mark secrets as created
                    instance.env_vars_instance.datahub_secrets_created = True
                    instance.env_vars_instance.save()

            messages.success(
                request, f"Recipe instance '{instance.name}' deployed successfully"
            )
        else:
            messages.error(request, "Failed to deploy recipe instance")
    except Exception as e:
        error_msg = f"Error deploying recipe instance: {str(e)}"
        messages.error(request, error_msg)
        logger.error(error_msg, exc_info=True)

    return redirect("recipe_instances")


def recipe_instance_undeploy(request, instance_id):
    """Undeploy a recipe instance from DataHub by deleting it from the server."""
    instance = get_object_or_404(RecipeInstance, id=instance_id)
    client = get_datahub_client_from_request(request)

    if not client or not client.test_connection():
        messages.error(request, "Not connected to DataHub")
        return redirect("recipe_instances")

    try:
        deletion_succeeded = False
        error_messages = []

        # First, try using the datahub_id (if available)
        if instance.datahub_id:
            # Check that the ID looks valid (should be a UUID, not "dataHubIngestionSource")
            if (
                instance.datahub_id != "dataHubIngestionSource"
                and len(instance.datahub_id) > 5
            ):
                try:
                    logger.info(
                        f"Attempting to delete recipe using ID: {instance.datahub_id}"
                    )
                    result = client.delete_ingestion_source(instance.datahub_id)
                    if result:
                        deletion_succeeded = True
                        logger.info(
                            f"Recipe '{instance.name}' deleted from DataHub using ID: {instance.datahub_id}"
                        )
                    else:
                        error_msg = f"Delete operation returned False for ID {instance.datahub_id}"
                        logger.warning(error_msg)
                        error_messages.append(error_msg)
                except Exception as e:
                    error_msg = f"Failed to delete recipe using ID: {str(e)}"
                    logger.warning(error_msg)
                    error_messages.append(error_msg)
            else:
                logger.warning(f"Skipping invalid datahub_id: {instance.datahub_id}")
        else:
            logger.debug("No datahub_id available for deletion")

        # If that didn't work and we have a URN, extract the ID from it and try again
        if not deletion_succeeded and instance.datahub_urn:
            try:
                # Extract just the ID part from the URN (the last part after the colon)
                if instance.datahub_urn.startswith("urn:li:dataHubIngestionSource:"):
                    urn_id = instance.datahub_urn.split(":")[-1]
                    logger.debug(
                        f"Extracted ID '{urn_id}' from URN: {instance.datahub_urn}"
                    )

                    # Only proceed if this ID is valid and different from datahub_id we already tried
                    if (
                        urn_id != "dataHubIngestionSource"
                        and len(urn_id) > 5
                        and (not instance.datahub_id or urn_id != instance.datahub_id)
                    ):
                        logger.info(
                            f"Attempting to delete recipe using ID from URN: {urn_id}"
                        )
                        result = client.delete_ingestion_source(urn_id)
                        if result:
                            deletion_succeeded = True
                            logger.info(
                                f"Recipe '{instance.name}' deleted from DataHub using ID from URN: {urn_id}"
                            )
                        else:
                            error_msg = f"Delete operation returned False for ID from URN: {urn_id}"
                            logger.warning(error_msg)
                            error_messages.append(error_msg)
                    else:
                        logger.warning(
                            f"Skipping invalid or duplicate ID from URN: {urn_id}"
                        )
                else:
                    logger.warning(
                        f"URN does not have expected format: {instance.datahub_urn}"
                    )
            except Exception as e:
                error_msg = f"Failed to delete recipe using ID from URN: {str(e)}"
                logger.warning(error_msg)
                error_messages.append(error_msg)
        else:
            if not instance.datahub_urn:
                logger.debug("No datahub_urn available for extraction")

        # If we still haven't succeeded, try getting the actual ID from the server
        if not deletion_succeeded and (instance.name or instance.datahub_urn):
            try:
                # List all sources and find by name or partial URN match
                sources = client.list_ingestion_sources()
                if sources:
                    for source in sources:
                        # Match by name
                        if instance.name and source.get("name") == instance.name:
                            source_id = source.get("id")
                            if source_id:
                                logger.info(
                                    f"Found recipe by name. Attempting to delete with ID: {source_id}"
                                )
                                result = client.delete_ingestion_source(source_id)
                                if result:
                                    deletion_succeeded = True
                                    logger.info(
                                        f"Recipe '{instance.name}' deleted from DataHub by matching name, using ID: {source_id}"
                                    )
                                    break

                        # Match by partial URN
                        if (
                            not deletion_succeeded
                            and instance.datahub_urn
                            and source.get("urn")
                            and source.get("urn") in instance.datahub_urn
                        ):
                            source_id = source.get("id")
                            if source_id:
                                logger.info(
                                    f"Found recipe by partial URN match. Attempting to delete with ID: {source_id}"
                                )
                                result = client.delete_ingestion_source(source_id)
                                if result:
                                    deletion_succeeded = True
                                    logger.info(
                                        f"Recipe '{instance.name}' deleted from DataHub by matching URN, using ID: {source_id}"
                                    )
                                    break
            except Exception as e:
                error_msg = f"Failed to delete recipe by searching: {str(e)}"
                logger.warning(error_msg)
                error_messages.append(error_msg)

        # Update the instance status
        instance.deployed = False
        if deletion_succeeded:
            instance.datahub_urn = None
            instance.datahub_id = None
        instance.save()

        if deletion_succeeded:
            messages.success(
                request,
                f"Recipe instance '{instance.name}' undeployed and deleted from DataHub successfully",
            )
        else:
            if error_messages:
                logger.error(f"Deletion errors: {', '.join(error_messages)}")

            messages.warning(
                request,
                f"Recipe instance '{instance.name}' marked as undeployed, but could not confirm deletion from DataHub",
            )
            # Show force undeploy modal
            return render(
                request,
                "recipes/instances.html",
                {
                    "title": "Recipe Instances",
                    "deployed": RecipeInstance.objects.filter(deployed=True).order_by(
                        "-updated_at"
                    ),
                    "staging": RecipeInstance.objects.filter(deployed=False).order_by(
                        "-updated_at"
                    ),
                    "show_force_undeploy": True,
                    "instance": instance,
                    "error": "Failed to delete recipe from DataHub. You can force undeploy to mark it as undeployed locally.",
                },
            )

    except Exception as e:
        error_msg = f"Error undeploying recipe instance: {str(e)}"
        logger.error(error_msg, exc_info=True)
        messages.error(request, error_msg)

    return redirect("recipe_instances")


def recipe_instance_redeploy(request, instance_id):
    """Redeploy a recipe instance by undeploying and deploying it again."""
    get_object_or_404(RecipeInstance, id=instance_id)

    # First undeploy the instance
    recipe_instance_undeploy(request, instance_id)

    # Then deploy it again
    return recipe_instance_deploy(request, instance_id)


def recipe_instance_download(request, instance_id):
    """Download a recipe instance as JSON."""
    instance = get_object_or_404(RecipeInstance, id=instance_id)

    # Get the recipe content with environment variables applied
    recipe_content = instance.get_combined_content()

    # Create a response with the JSON content
    response = HttpResponse(recipe_content, content_type="application/json")
    response["Content-Disposition"] = f'attachment; filename="{instance.name}.json"'

    return response


def recipe_template_preview(request, template_id):
    """API endpoint to preview a recipe template with environment variables applied."""
    template = get_object_or_404(RecipeTemplate, id=template_id)
    template_content = template.get_content()

    # Check if environment variables instance ID was provided
    env_vars_id = request.GET.get("env_vars_id")
    if env_vars_id:
        try:
            env_vars_instance = EnvVarsInstance.objects.get(id=env_vars_id)
            env_vars = env_vars_instance.get_variables_dict()

            # Apply environment variables to the template
            combined_content = replace_env_vars_with_values(template_content, env_vars)
            return JsonResponse({"content": combined_content})
        except EnvVarsInstance.DoesNotExist:
            return JsonResponse(
                {"error": "Environment variables instance not found"}, status=404
            )
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)

    # If no env vars, just return the template content
    return JsonResponse({"content": template_content})


# GitHub integration views
def github_index(request):
    """Main Git integration page."""
    settings = GitSettings.get_instance()
    pull_requests = GitHubPR.objects.all().order_by("-created_at")[:10]

    # Load branches asynchronously to avoid race conditions
    branches = []
    if settings.is_configured():
        try:
            # Try to load branches with a timeout to avoid blocking the page
            from web_ui.models import GitIntegration
            branches = GitIntegration.get_branches()
        except Exception as e:
            logger.warning(f"Failed to load branches on page load: {str(e)}")
            # Branches will be loaded via AJAX to avoid blocking the page

    context = {
        "github_settings": settings,
        "pull_requests": pull_requests,
        "is_configured": settings.is_configured(),
        "branches": branches,
    }

    return render(request, "github/index.html", context)


def github_settings_edit(request):
    """Edit Git integration settings."""
    settings = GitSettings.get_instance()

    if request.method == "POST":
        form = GitSettingsForm(request.POST, instance=settings)
        if form.is_valid():
            form.save()
            messages.success(request, "Git integration settings updated successfully")
            return redirect("github_index")
    else:
        form = GitSettingsForm(instance=settings)

    context = {"form": form, "git_settings": settings}

    return render(request, "github/settings.html", context)


def github_pull_requests(request):
    """List all Git provider pull requests."""
    settings = GitSettings.get_instance()

    # Get all PRs and paginate
    all_prs = GitHubPR.objects.all().order_by("-created_at")
    paginator = Paginator(all_prs, 25)  # Show 25 PRs per page

    page_number = request.GET.get("page")
    pull_requests = paginator.get_page(page_number)

    context = {
        "git_settings": settings,
        "pull_requests": pull_requests,
        "is_paginated": paginator.num_pages > 1,
        "page_obj": pull_requests,
    }

    return render(request, "github/pull_requests.html", context)


@require_POST
def github_test_connection(request):
    """Test Git provider connection."""
    try:
        data = json.loads(request.body)
        data.get("provider_type", "github")
        data.get("base_url", "")
        username = data.get("username")
        repository = data.get("repository")
        token = data.get("token")

        if not all([username, repository, token]):
            return JsonResponse({"success": False, "error": "Missing required fields"})

        # Test connection with GitHub API
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }

        # Test repo access
        repo_url = f"https://api.github.com/repos/{username}/{repository}"
        response = requests.get(repo_url, headers=headers)

        if response.status_code == 200:
            return JsonResponse({"success": True})
        else:
            error_message = response.json().get("message", "Unknown error")
            return JsonResponse({"success": False, "error": error_message})

    except Exception as e:
        logger.error(f"Error testing GitHub connection: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


def validate_github_branch_name(branch_name):
    """Validate GitHub branch name according to Git standards."""
    errors = []
    
    if not branch_name or not branch_name.strip():
        errors.append("Branch name cannot be empty")
        return errors
    
    trimmed = branch_name.strip()
    
    # GitHub branch naming rules
    if ' ' in trimmed:
        errors.append("Branch name cannot contain spaces")
    
    if trimmed.startswith('.') or trimmed.endswith('.'):
        errors.append("Branch name cannot start or end with a period")
    
    if '..' in trimmed:
        errors.append("Branch name cannot contain consecutive periods (..)")
    
    if trimmed.startswith('/') or trimmed.endswith('/'):
        errors.append("Branch name cannot start or end with a slash")
    
    if '//' in trimmed:
        errors.append("Branch name cannot contain consecutive slashes")
    
    # Check for invalid characters
    import re
    if re.search(r'[~^:\\*\[\]?@{}\x00-\x1f\x7f]', trimmed):
        errors.append("Branch name contains invalid characters (~, ^, :, \\, ?, *, [, ], @, {, }, or control characters)")
    
    # Check for reserved names
    if trimmed in ['.', '..']:
        errors.append('Branch name cannot be "." or ".."')
    
    # Check length (GitHub limit is 250 characters)
    if len(trimmed) > 250:
        errors.append("Branch name cannot exceed 250 characters")
    
    # Check for @{ sequence
    if '@{' in trimmed:
        errors.append('Branch name cannot contain "@{" sequence')
    
    return errors


@require_POST
def github_create_branch(request):
    """Create a new branch on GitHub."""
    if not GitSettings.is_configured():
        messages.error(request, "GitHub integration is not configured")
        return redirect("github_index")

    branch_name = request.POST.get("branch_name")
    branch_description = request.POST.get("branch_description", "")

    if not branch_name:
        messages.error(request, "Branch name is required")
        return redirect("github_index")
    
    # Validate branch name
    validation_errors = validate_github_branch_name(branch_name)
    if validation_errors:
        for error in validation_errors:
            messages.error(request, f"Invalid branch name: {error}")
        return redirect("github_index")

    settings = GitSettings.get_instance()
    headers = {
        "Authorization": f"token {settings.token}",
        "Accept": "application/vnd.github.v3+json",
    }

    try:
        # First, get the default branch and its sha
        repo_url = (
            f"https://api.github.com/repos/{settings.username}/{settings.repository}"
        )
        repo_response = requests.get(repo_url, headers=headers)
        repo_response.raise_for_status()
        repo_data = repo_response.json()
        default_branch = repo_data.get("default_branch", "main")

        # Get the SHA of the default branch
        branch_url = f"https://api.github.com/repos/{settings.username}/{settings.repository}/branches/{default_branch}"
        branch_response = requests.get(branch_url, headers=headers)
        branch_response.raise_for_status()
        branch_data = branch_response.json()
        sha = branch_data.get("commit", {}).get("sha")

        if not sha:
            messages.error(
                request, f"Could not determine SHA of {default_branch} branch"
            )
            return redirect("github_index")

        # Create the new branch
        create_url = f"https://api.github.com/repos/{settings.username}/{settings.repository}/git/refs"
        create_data = {"ref": f"refs/heads/{branch_name}", "sha": sha}

        create_response = requests.post(create_url, headers=headers, json=create_data)
        create_response.raise_for_status()

        messages.success(request, f"Branch '{branch_name}' created successfully")

        # Create a PR if description is provided
        if branch_description:
            pr_url = f"https://api.github.com/repos/{settings.username}/{settings.repository}/pulls"
            pr_data = {
                "title": f"New branch: {branch_name}",
                "body": branch_description,
                "head": branch_name,
                "base": default_branch,
            }

            pr_response = requests.post(pr_url, headers=headers, json=pr_data)

            if pr_response.status_code == 201:
                pr_result = pr_response.json()
                pr_number = pr_result.get("number")
                html_url = pr_result.get("html_url")

                # Create PR record
                GitHubPR.objects.create(
                    recipe_id="N/A",
                    pr_url=html_url,
                    pr_number=pr_number,
                    pr_status="open",
                    branch_name=branch_name,
                    title=f"New branch: {branch_name}",
                    description=branch_description,
                )

                messages.success(request, f"Pull request #{pr_number} created")

        return redirect("github_index")

    except requests.exceptions.RequestException as e:
        try:
            error_data = e.response.json()
            error_message = error_data.get("message", str(e))
            messages.error(request, f"GitHub error: {error_message}")
        except Exception:
            messages.error(request, f"Error creating branch: {str(e)}")

        return redirect("github_index")


def github_sync_recipes(request):
    """Sync all recipes with GitHub."""
    # For now, just redirect back with a message
    # This would be implemented based on your recipe model and requirements
    messages.info(request, "Recipe sync feature is under development")
    return redirect("github_index")


def github_sync_status(request):
    """Sync PR statuses with GitHub."""
    if not GitSettings.is_configured():
        messages.error(request, "GitHub integration is not configured")
        return redirect("github_index")

    settings = GitSettings.get_instance()
    headers = {
        "Authorization": f"token {settings.token}",
        "Accept": "application/vnd.github.v3+json",
    }

    # Get all open/pending PRs
    active_prs = GitHubPR.objects.filter(pr_status__in=["open", "pending"])

    updated_count = 0
    error_count = 0

    for pr in active_prs:
        try:
            # Get PR status from GitHub
            pr_url = f"https://api.github.com/repos/{settings.username}/{settings.repository}/pulls/{pr.pr_number}"
            response = requests.get(pr_url, headers=headers)

            if response.status_code == 200:
                pr_data = response.json()

                # Update PR status
                if pr_data.get("merged"):
                    pr.pr_status = "merged"
                elif pr_data.get("state") == "closed":
                    pr.pr_status = "closed"
                else:
                    pr.pr_status = "open"

                pr.save()
                updated_count += 1
            else:
                error_count += 1
                logger.error(
                    f"Error fetching PR #{pr.pr_number}: {response.status_code}"
                )

        except Exception as e:
            error_count += 1
            logger.error(f"Exception updating PR #{pr.pr_number}: {str(e)}")

    if updated_count > 0:
        messages.success(request, f"Updated status for {updated_count} pull requests")

    if error_count > 0:
        messages.warning(request, f"Failed to update {error_count} pull requests")

    if updated_count == 0 and error_count == 0:
        messages.info(request, "No pull requests to update")

    return redirect("github_index")


@require_POST
def github_update_pr_status(request, pr_number):
    """Update a specific PR status."""
    if not GitSettings.is_configured():
        return JsonResponse(
            {"success": False, "error": "GitHub integration not configured"}
        )

    try:
        pr = GitHubPR.objects.get(pr_number=pr_number)

        settings = GitSettings.get_instance()
        headers = {
            "Authorization": f"token {settings.token}",
            "Accept": "application/vnd.github.v3+json",
        }

        # Get PR status from GitHub
        pr_url = f"https://api.github.com/repos/{settings.username}/{settings.repository}/pulls/{pr_number}"
        response = requests.get(pr_url, headers=headers)

        if response.status_code == 200:
            pr_data = response.json()

            # Update PR status
            if pr_data.get("merged"):
                pr.pr_status = "merged"
            elif pr_data.get("state") == "closed":
                pr.pr_status = "closed"
            else:
                pr.pr_status = "open"

            pr.save()
            return JsonResponse({"success": True, "status": pr.pr_status})

        else:
            error_message = response.json().get("message", "Unknown error")
            return JsonResponse({"success": False, "error": error_message})

    except GitHubPR.DoesNotExist:
        return JsonResponse(
            {"success": False, "error": f"Pull request #{pr_number} not found"}
        )
    except Exception as e:
        logger.error(f"Error updating PR status: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


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

    return redirect("github_pull_requests")


def github_switch_branch(request, branch_name):
    """Switch the current branch in GitHub settings."""
    try:
        github_settings = GitSettings.objects.first()
        if not github_settings:
            messages.error(request, "GitHub settings not found")
            return redirect("github_index")

        # Update the current branch
        github_settings.current_branch = branch_name
        github_settings.save()

        messages.success(request, f"Switched to branch: {branch_name}")
    except Exception as e:
        logger.error(f"Error switching branch: {str(e)}")
        messages.error(request, f"Error switching branch: {str(e)}")

    return redirect("github_index")


@require_POST
def recipe_instance_push_github(request, instance_id):
    """Push a recipe instance to GitHub."""
    instance = get_object_or_404(RecipeInstance, id=instance_id)

    try:
        # Get the GitHub integration
        github = GitIntegration()

        # Get current branch
        settings = GitSettings.get_instance()
        current_branch = settings.current_branch or "main"

        # Prevent pushing directly to main/master branch
        if current_branch.lower() in ["main", "master"]:
            return JsonResponse(
                {
                    "success": False,
                    "error": "Cannot push directly to the main branch. Please create and use a feature branch.",
                }
            )

        # Stage changes
        commit_message = f"Update recipe instance: {instance.name}"
        result = github.push_to_git(instance, commit_message)

        if result and result.get("success"):
            return JsonResponse(
                {
                    "success": True,
                    "message": f'Recipe instance "{instance.name}" staged for commit to branch {current_branch}. You can create a PR from the Git Repository tab.',
                    "redirect_url": reverse("github_index"),
                }
            )
        else:
            return JsonResponse(
                {
                    "success": False,
                    "error": f'Failed to stage recipe instance "{instance.name}"',
                }
            )
    except Exception as e:
        logger.error(f"Error pushing instance to GitHub: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_POST
def recipe_template_push_github(request, template_id):
    """Push a recipe template to GitHub."""
    template = get_object_or_404(RecipeTemplate, id=template_id)

    try:
        # Get the GitHub integration
        github = GitIntegration()

        # Get current branch
        settings = GitSettings.get_instance()
        current_branch = settings.current_branch or "main"

        # Prevent pushing directly to main/master branch
        if current_branch.lower() in ["main", "master"]:
            return JsonResponse(
                {
                    "success": False,
                    "error": "Cannot push directly to the main branch. Please create and use a feature branch.",
                }
            )

        # Stage changes
        commit_message = f"Update recipe template: {template.name}"
        result = github.push_to_git(template, commit_message)

        if result and result.get("success"):
            return JsonResponse(
                {
                    "success": True,
                    "message": f'Recipe template "{template.name}" staged for commit to branch {current_branch}. You can create a PR from the Git Repository tab.',
                    "redirect_url": reverse("github_index"),
                }
            )
        else:
            return JsonResponse(
                {
                    "success": False,
                    "error": f'Failed to stage recipe template "{template.name}"',
                }
            )
    except Exception as e:
        logger.error(f"Error pushing template to GitHub: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_POST
def github_create_pr(request):
    """Create a PR from staged changes."""
    if not GitSettings.is_configured():
        return JsonResponse(
            {"success": False, "error": "GitHub integration not configured"}
        )

    try:
        # Get current branch
        settings = GitSettings.get_instance()
        current_branch = settings.current_branch or "main"

        # Prevent creating PRs directly from main/master branch
        if current_branch.lower() in ["main", "master"]:
            return JsonResponse(
                {
                    "success": False,
                    "error": "Cannot create a PR directly from the main branch. Please create and use a feature branch.",
                }
            )

        title = request.POST.get("title")
        description = request.POST.get("description")
        base = request.POST.get("base")

        # Get the GitHub integration
        github = GitIntegration()

        # Create PR from staged changes
        pr = github.create_pr_from_staged_changes(title, description, base)

        if pr:
            return JsonResponse(
                {
                    "success": True,
                    "pr_url": pr["pr_url"],
                    "message": f"Pull request #{pr['pr_number']} created successfully",
                }
            )
        else:
            return JsonResponse(
                {"success": False, "error": "Failed to create pull request"}
            )
    except Exception as e:
        logger.error(f"Error creating PR: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_POST
def env_vars_instance_push_github(request, instance_id):
    """Add environment variables to GitHub PR."""
    if not GitSettings.is_configured():
        return JsonResponse(
            {"success": False, "error": "GitHub integration not configured"}
        )

    try:
        # Get current branch
        settings = GitSettings.get_instance()
        current_branch = settings.current_branch or "main"

        # Prevent pushing directly to main/master branch
        if current_branch.lower() in ["main", "master"]:
            return JsonResponse(
                {
                    "success": False,
                    "error": "Cannot push directly to the main branch. Please create and use a feature branch.",
                }
            )

        # Get the environment variables instance
        try:
            env_instance = EnvVarsInstance.objects.get(id=instance_id)
            logger.info(f"Found environment variable instance: {env_instance.name}")
        except EnvVarsInstance.DoesNotExist:
            logger.error(
                f"Environment variable instance with ID {instance_id} not found"
            )
            return JsonResponse(
                {
                    "success": False,
                    "error": f"Environment variable instance with ID {instance_id} not found",
                }
            )

        # Use the GitIntegration class for consistency
        logger.info(
            f"Pushing environment variables instance {env_instance.name} to Git"
        )
        result = GitIntegration.push_to_git(env_instance)

        if not result:
            logger.error(
                f"Failed to push environment variables instance {env_instance.name} to GitHub - received None result"
            )
            return JsonResponse(
                {
                    "success": False,
                    "error": "Failed to push environment variables to GitHub",
                }
            )

        if not result.get("success", False):
            error_msg = result.get("error", "Unknown error")
            logger.error(
                f"Failed to push environment variables instance {env_instance.name} to GitHub: {error_msg}"
            )
            return JsonResponse({"success": False, "error": error_msg})

        logger.info(
            f"Successfully pushed environment variables instance {env_instance.name} to GitHub"
        )

        # Get environment name for display (default to 'dev')
        env_name = "dev"
        if env_instance.environment:
            env_name = env_instance.environment.name

        # Get file name based on recipe type
        file_name = f"{env_instance.recipe_type.lower()}.yml"

        return JsonResponse(
            {
                "success": True,
                "branch": result.get("branch", current_branch),
                "file_path": result.get("file_path", ""),
                "message": f"Environment variables for '{env_instance.name}' added to recipes/instances/{env_name}/{file_name} in branch {current_branch}",
            }
        )

    except Exception as e:
        logger.error(
            f"Error pushing environment variables to GitHub: {str(e)}", exc_info=True
        )
        return JsonResponse({"success": False, "error": str(e)})


@require_POST
def env_vars_template_push_github(request, template_id):
    """Add environment variables template to GitHub PR."""
    if not GitSettings.is_configured():
        return JsonResponse(
            {"success": False, "error": "GitHub integration not configured"}
        )

    try:
        # Get current branch
        settings = GitSettings.get_instance()
        current_branch = settings.current_branch or "main"

        # Prevent pushing directly to main/master branch
        if current_branch.lower() in ["main", "master"]:
            return JsonResponse(
                {
                    "success": False,
                    "error": "Cannot push directly to the main branch. Please create and use a feature branch.",
                }
            )

        # Get the environment variables template
        try:
            env_template = EnvVarsTemplate.objects.get(id=template_id)
            logger.info(f"Found environment variable template: {env_template.name}")
        except EnvVarsTemplate.DoesNotExist:
            logger.error(
                f"Environment variable template with ID {template_id} not found"
            )
            return JsonResponse(
                {
                    "success": False,
                    "error": f"Environment variable template with ID {template_id} not found",
                }
            )

        # Stage changes
        logger.info(
            f"Pushing environment variables template {env_template.name} to Git"
        )
        commit_message = f"Update environment variables template: {env_template.name}"
        result = GitIntegration.push_to_git(env_template, commit_message)

        if not result:
            logger.error(
                f"Failed to push environment variables template {env_template.name} to GitHub - received None result"
            )
            return JsonResponse(
                {
                    "success": False,
                    "error": "Failed to push environment variables to GitHub",
                }
            )

        if not result.get("success", False):
            error_msg = result.get("error", "Unknown error")
            logger.error(
                f"Failed to push environment variables template {env_template.name} to GitHub: {error_msg}"
            )
            return JsonResponse({"success": False, "error": error_msg})

        logger.info(
            f"Successfully pushed environment variables template {env_template.name} to GitHub"
        )
        return JsonResponse(
            {
                "success": True,
                "branch": result.get("branch", current_branch),
                "file_path": result.get("file_path", ""),
                "message": f'Environment variables template "{env_template.name}" staged for commit to branch {current_branch}. You can create a PR from the Git Repository tab.',
                "redirect_url": reverse("github_index"),
            }
        )

    except Exception as e:
        logger.error(
            f"Error pushing environment variables template to GitHub: {str(e)}",
            exc_info=True,
        )
        return JsonResponse({"success": False, "error": str(e)})


def environments(request):
    """List all environments with mutations."""
    envs = Environment.objects.all().prefetch_related('mutations').order_by("name")
    mutations = Mutation.objects.all().order_by("name")

    return render(
        request,
        "environments/list.html",
        {"title": "Environments", "environments": envs, "mutations": mutations},
    )


def environment_create(request):
    """Create a new environment."""
    if request.method == "POST":
        name = request.POST.get("name")
        description = request.POST.get("description")
        is_default = request.POST.get("is_default") == "on"
        mutations_id = request.POST.get("mutations")
        
        mutations = None
        if mutations_id:
            try:
                mutations = Mutation.objects.get(id=mutations_id)
            except Mutation.DoesNotExist:
                pass

        # Create the environment
        Environment.objects.create(
            name=name, description=description, is_default=is_default, mutations=mutations
        )

        messages.success(request, f'Environment "{name}" created successfully.')

        # Sync the new environment with GitHub if Git integration is configured
        if GitSettings.is_configured():
            try:
                import subprocess
                from pathlib import Path

                # Run the sync script
                script_path = (
                    Path(__file__).parent.parent.parent
                    / "scripts"
                    / "sync_github_environments.py"
                )

                if script_path.exists():
                    result = subprocess.run(
                        [sys.executable, str(script_path)],
                        capture_output=True,
                        text=True,
                    )

                    if result.returncode == 0:
                        logger.info(
                            f"GitHub environment sync successful: {result.stdout}"
                        )
                        messages.success(
                            request, f'Environment "{name}" synchronized with GitHub.'
                        )
                    else:
                        logger.error(f"GitHub environment sync failed: {result.stderr}")
                        # Extract meaningful error message
                        error_msg = result.stderr.strip()
                        if "No module named" in error_msg:
                            error_msg = "Django configuration issue - please check server logs"
                        elif "Missing GitHub credentials" in error_msg:
                            error_msg = "GitHub credentials not configured - please set up GitHub integration"
                        messages.warning(
                            request,
                            f'Environment "{name}" created but GitHub sync failed: {error_msg}',
                        )
                else:
                    logger.warning(
                        f"GitHub environment sync script not found at {script_path}"
                    )
            except Exception as e:
                logger.error(f"Error syncing environment with GitHub: {str(e)}")
                messages.warning(
                    request, f"Failed to synchronize environment with GitHub: {str(e)}"
                )

        return redirect("environments")

    mutations = Mutation.objects.all().order_by("name")
    return render(request, "environments/form.html", {
        "title": "Create Environment", 
        "is_new": True,
        "mutations": mutations
    })


def environment_edit(request, env_id):
    """Edit an existing environment."""
    environment = get_object_or_404(Environment, id=env_id)

    if request.method == "POST":
        # Update the environment
        environment.name = request.POST.get("name")
        environment.description = request.POST.get("description")
        environment.is_default = request.POST.get("is_default") == "on"
        
        mutations_id = request.POST.get("mutations")
        if mutations_id:
            try:
                environment.mutations = Mutation.objects.get(id=mutations_id)
            except Mutation.DoesNotExist:
                environment.mutations = None
        else:
            environment.mutations = None
            
        environment.save()

        messages.success(
            request, f'Environment "{environment.name}" updated successfully.'
        )

        # Sync the updated environment with GitHub if Git integration is configured
        if GitSettings.is_configured():
            try:
                import subprocess
                from pathlib import Path

                # Run the sync script
                script_path = (
                    Path(__file__).parent.parent.parent
                    / "scripts"
                    / "sync_github_environments.py"
                )

                if script_path.exists():
                    result = subprocess.run(
                        [sys.executable, str(script_path)],
                        capture_output=True,
                        text=True,
                    )

                    if result.returncode == 0:
                        logger.info(
                            f"GitHub environment sync successful: {result.stdout}"
                        )
                        messages.success(
                            request,
                            f'Environment "{environment.name}" synchronized with GitHub.',
                        )
                    else:
                        logger.error(f"GitHub environment sync failed: {result.stderr}")
                        # Extract meaningful error message
                        error_msg = result.stderr.strip()
                        if "No module named" in error_msg:
                            error_msg = "Django configuration issue - please check server logs"
                        elif "Missing GitHub credentials" in error_msg:
                            error_msg = "GitHub credentials not configured - please set up GitHub integration"
                        messages.warning(
                            request,
                            f'Environment "{environment.name}" updated but GitHub sync failed: {error_msg}',
                        )
                else:
                    logger.warning(
                        f"GitHub environment sync script not found at {script_path}"
                    )
            except Exception as e:
                logger.error(f"Error syncing environment with GitHub: {str(e)}")
                messages.warning(
                    request, f"Failed to synchronize environment with GitHub: {str(e)}"
                )

        return redirect("environments")

    mutations = Mutation.objects.all().order_by("name")
    return render(
        request,
        "environments/form.html",
        {
            "title": "Edit Environment", 
            "environment": environment, 
            "is_new": False,
            "mutations": mutations
        },
    )


def environment_delete(request, env_id):
    """Delete an environment."""
    environment = get_object_or_404(Environment, id=env_id)

    # Check if this is the default environment
    if environment.is_default:
        messages.error(request, "Cannot delete the default environment")
        return redirect("environments")

    # Check if environment is in use
    recipe_count = RecipeInstance.objects.filter(environment=environment).count()
    env_vars_count = EnvVarsInstance.objects.filter(environment=environment).count()

    if request.method == "POST":
        if recipe_count == 0 and env_vars_count == 0:
            # Delete environment
            name = environment.name
            environment.delete()
            messages.success(request, f"Environment '{name}' deleted successfully")
        else:
            messages.error(
                request,
                f"Cannot delete environment '{environment.name}' - it is in use by {recipe_count} recipe instances and {env_vars_count} environment variable instances",
            )

        return redirect("environments")

    return render(
        request,
        "environments/delete.html",
        {
            "title": f"Delete Environment: {environment.name}",
            "environment": environment,
            "recipe_count": recipe_count,
            "env_vars_count": env_vars_count,
        },
    )


def set_default_environment(request, env_id):
    """Set an environment as the default."""
    if request.method == "POST":
        environment = get_object_or_404(Environment, id=env_id)
        environment.is_default = True
        environment.save()
        messages.success(request, f"Environment '{environment.name}' set as default")
    return redirect("environments")


def github_repo_integration(request):
    """View for GitHub repository integration."""
    if not GitSettings.is_configured():
        return redirect("github_settings")

    # Check recipe and policy template counts
    recipe_count = RecipeTemplate.objects.count()
    policy_count = PolicyTemplate.objects.count()

    settings = GitSettings.get_instance()

    # Get all GitHub PRs
    prs = GitHubPR.objects.all().order_by("-created_at")

    return render(
        request,
        "github_repo.html",
        {
            "settings": settings,
            "recipe_count": recipe_count,
            "policy_count": policy_count,
            "prs": prs,
        },
    )


def github_push_changes(request):
    """Push all staged changes to GitHub."""
    if not GitSettings.is_configured():
        messages.error(request, "GitHub integration is not configured.")
        return redirect("github_settings")

    GitSettings.get_instance()

    # Get changed files from Git
    all_changed_files = GitIntegration.get_staged_changes()
    if not all_changed_files:
        messages.warning(request, "No changes to push.")
        return redirect("github_repo")

    try:
        # Commit the changes
        commit_message = request.POST.get(
            "commit_message", "Update recipes and policies"
        )
        result = GitIntegration.commit_staged_changes(commit_message)

        if result:
            messages.success(request, "Changes pushed successfully.")
        else:
            messages.error(request, "Failed to push changes.")

    except Exception as e:
        messages.error(request, f"Error pushing changes: {str(e)}")

    return redirect("github_repo")


def github_create_pr(request):
    """Create a pull request for pending changes."""
    if not GitSettings.is_configured():
        messages.error(request, "GitHub integration is not configured.")
        return redirect("github_settings")

    settings = GitSettings.get_instance()

    # Check if there are any staged changes
    has_changes = GitIntegration.has_staged_changes()
    if not has_changes:
        messages.warning(
            request, "No staged changes found. Stage changes before creating a PR."
        )
        return redirect("github_repo")

    # Get form data
    pr_title = request.POST.get("pr_title")
    pr_description = request.POST.get("pr_description")
    target_branch = request.POST.get("target_branch")

    if not pr_title:
        messages.error(request, "Pull request title is required.")
        return redirect("github_repo")

    try:
        # Create the PR
        pr_result = GitIntegration.create_pr_from_staged_changes(
            title=pr_title, description=pr_description, base=target_branch
        )

        if pr_result and isinstance(pr_result, dict) and "number" in pr_result:
            pr_number = pr_result["number"]
            pr_url = pr_result.get("html_url", "")

            messages.success(
                request, f"Pull request #{pr_number} created successfully."
            )

            # Create PR record in the database
            GitHubPR.objects.create(
                recipe_id="multiple",  # Multiple recipes in one PR
                pr_url=pr_url,
                pr_number=pr_number,
                pr_status="open",
                branch_name=settings.current_branch,
                title=pr_title,
                description=pr_description or "",
            )
        else:
            messages.error(request, "Failed to create pull request.")
    except Exception as e:
        messages.error(request, f"Error creating pull request: {str(e)}")

    return redirect("github_repo")


def github_fetch_prs(request):
    """Refresh pull request statuses from GitHub."""
    if not GitSettings.is_configured():
        messages.error(request, "GitHub integration is not configured.")
        return redirect("github_settings")

    GitSettings.get_instance()

    try:
        # Get all PRs from database
        prs = GitHubPR.objects.all()

        updated_count = 0
        for pr in prs:
            # Get PR status from GitHub
            pr_data = GitIntegration.get_pull_request(pr.pr_number)
            if pr_data and "state" in pr_data:
                # Update status
                pr_state = pr_data["state"]
                pr_merged = pr_data.get("merged", False)

                if pr_merged:
                    pr.pr_status = "merged"
                elif pr_state == "open":
                    pr.pr_status = "open"
                elif pr_state == "closed":
                    pr.pr_status = "closed"

                pr.save()
                updated_count += 1

        messages.success(request, f"Updated status for {updated_count} pull requests.")
    except Exception as e:
        messages.error(request, f"Error updating pull requests: {str(e)}")

    return redirect("github_repo")


def github_branches(request):
    """View to list and manage GitHub branches."""
    if not GitSettings.is_configured():
        messages.error(request, "GitHub integration is not configured.")
        return redirect("github_settings")

    settings = GitSettings.get_instance()

    # Get all branches from Git
    branches = GitIntegration.get_branches()

    return render(
        request,
        "github_branches.html",
        {
            "settings": settings,
            "branches": branches,
            "current_branch": settings.current_branch,
        },
    )


def github_create_branch(request):
    """Create a new branch in GitHub."""
    if request.method != "POST":
        return redirect("github_branches")

    if not GitSettings.is_configured():
        messages.error(request, "GitHub integration is not configured.")
        return redirect("github_settings")

    settings = GitSettings.get_instance()

    branch_name = request.POST.get("branch_name", "").strip()
    base_branch = request.POST.get(
        "base_branch", settings.current_branch or "main"
    ).strip()

    if not branch_name:
        messages.error(request, "Branch name is required.")
        return redirect("github_branches")

    # Validate branch name
    if not re.match(r"^[a-zA-Z0-9_\-\.\/]+$", branch_name):
        messages.error(request, "Branch name contains invalid characters.")
        return redirect("github_branches")

    try:
        # Create the branch
        success = GitIntegration.create_branch(branch_name, base_branch)

        if success:
            messages.success(request, f'Branch "{branch_name}" created successfully.')

            # Optionally set as current branch
            if request.POST.get("set_as_current", "off") == "on":
                settings.current_branch = branch_name
                settings.save()
                messages.info(request, f'Current branch set to "{branch_name}".')
        else:
            messages.error(request, f'Failed to create branch "{branch_name}".')
    except Exception as e:
        messages.error(request, f"Error creating branch: {str(e)}")

    return redirect("github_branches")


def github_settings_view(request):
    """View for GitHub settings."""
    settings = GitSettings.get_instance()

    if request.method == "POST":
        form = GitSettingsForm(request.POST, instance=settings)
        if form.is_valid():
            form.save()
            messages.success(request, "Git integration settings updated successfully")
            return redirect("github_index")
    else:
        form = GitSettingsForm(instance=settings)

    # Check connection status
    connection_status = "Not connected"
    connection_error = None

    if (
        settings.enabled
        and settings.token
        and settings.username
        and settings.repository
    ):
        try:
            # Initialize Git integration class
            git_service = GitService()

            # Try to get repository information
            result = git_service.make_api_request("GET", "")

            if result and result.status_code == 200:
                connection_status = "Connected"
            else:
                status_code = result.status_code if result else "Unknown"
                connection_status = f"Error ({status_code})"
                connection_error = (
                    result.text if result else "Could not connect to repository"
                )
        except Exception as e:
            connection_status = "Error"
            connection_error = str(e)

    # Get all branches if connected
    branches = []
    if connection_status == "Connected":
        try:
            branches = GitIntegration.get_branches()
        except Exception:
            # Failed to get branches, but continue with empty list
            pass

    context = {
        "form": form,
        "git_settings": settings,
        "connection_status": connection_status,
        "connection_error": connection_error,
        "branches": branches,
        "provider_choices": GitSettings.PROVIDER_CHOICES,
    }

    return render(request, "github/settings.html", context)


def github_sync_recipe(request, recipe_id):
    """Sync a recipe to GitHub."""
    if not GitSettings.is_configured():
        messages.error(request, "GitHub integration is not configured.")
        return redirect("recipes")

    recipe = get_object_or_404(RecipeTemplate, id=recipe_id)

    if request.method == "POST":
        # Get form data
        commit_message = request.POST.get(
            "commit_message", f"Update recipe: {recipe.name}"
        )
        branch_name = request.POST.get("branch_name")

        # Get recipe data
        recipe.get_content()
        recipe_id = recipe.get_recipe_id()

        try:
            # Use the GitIntegration class to sync the recipe
            settings = GitSettings.get_instance()

            if branch_name and branch_name != settings.current_branch:
                # Temporarily switch branch
                original_branch = settings.current_branch
                settings.current_branch = branch_name
                settings.save()

                # Perform the sync
                result = GitIntegration.stage_changes(recipe, commit_message)

                # Switch back to original branch
                settings.current_branch = original_branch
                settings.save()
            else:
                # Use the current branch
                result = GitIntegration.stage_changes(recipe, commit_message)

            if result:
                messages.success(
                    request,
                    f'Recipe "{recipe.name}" staged for commit to branch {settings.current_branch}.',
                )
                return redirect("github_repo")
            else:
                messages.error(request, f'Failed to stage recipe "{recipe.name}".')
        except Exception as e:
            messages.error(request, f"Error syncing recipe: {str(e)}")

        return redirect("recipes")
    else:
        settings = GitSettings.get_instance()

        # Get all branches
        branches = []
        try:
            branches = GitIntegration.get_branches()
        except Exception:
            # Failed to get branches, but continue with empty list
            pass

        return render(
            request,
            "github_sync.html",
            {
                "recipe": recipe,
                "settings": settings,
                "branches": branches,
                "current_branch": settings.current_branch,
            },
        )


def github_sync_all_recipes(request):
    """Sync all recipes to GitHub."""
    if not GitSettings.is_configured():
        messages.error(request, "GitHub integration is not configured.")
        return redirect("recipes")

    settings = GitSettings.get_instance()

    if request.method == "POST":
        # Get form data
        commit_message = request.POST.get("commit_message", "Sync all recipes")
        branch_name = request.POST.get("branch_name", settings.current_branch)

        # Check if we need to switch branch
        original_branch = None
        if branch_name and branch_name != settings.current_branch:
            original_branch = settings.current_branch
            settings.current_branch = branch_name
            settings.save()

        # Get all recipes
        recipes = RecipeTemplate.objects.all()

        success_count = 0
        error_count = 0

        try:
            for recipe in recipes:
                try:
                    # Sync each recipe
                    result = GitIntegration.stage_changes(recipe, commit_message)
                    if result:
                        success_count += 1
                    else:
                        error_count += 1
                except Exception:
                    error_count += 1
                    # Continue with other recipes even if one fails
                    continue

            if success_count > 0:
                messages.success(
                    request, f"Successfully staged {success_count} recipes for commit."
                )
                if error_count > 0:
                    messages.warning(request, f"Failed to stage {error_count} recipes.")
            else:
                messages.error(request, "Failed to stage any recipes.")

            # Switch back to original branch if needed
            if original_branch:
                settings.current_branch = original_branch
                settings.save()

            return redirect("github_repo")

        except Exception as e:
            # Switch back to original branch if needed
            if original_branch:
                settings.current_branch = original_branch
                settings.save()

            messages.error(request, f"Error syncing recipes: {str(e)}")
            return redirect("recipes")
    else:
        # Get all branches
        branches = []
        try:
            branches = GitIntegration.get_branches()
        except Exception:
            # Failed to get branches, but continue with empty list
            pass

        return render(
            request,
            "github_sync_all.html",
            {
                "settings": settings,
                "branches": branches,
                "current_branch": settings.current_branch,
                "recipe_count": RecipeTemplate.objects.count(),
            },
        )


def github_sync_policy(request, policy_id):
    """Sync a policy to GitHub."""
    if not GitSettings.is_configured():
        messages.error(request, "GitHub integration is not configured.")
        return redirect("policies")

    policy = get_object_or_404(PolicyTemplate, id=policy_id)

    if request.method == "POST":
        # Get form data
        commit_message = request.POST.get(
            "commit_message", f"Update policy: {policy.name}"
        )
        branch_name = request.POST.get("branch_name")

        try:
            # Use the GitIntegration class to sync the policy
            settings = GitSettings.get_instance()

            if branch_name and branch_name != settings.current_branch:
                # Temporarily switch branch
                original_branch = settings.current_branch
                settings.current_branch = branch_name
                settings.save()

                # Perform the sync
                result = GitIntegration.stage_changes(policy, commit_message)

                # Switch back to original branch
                settings.current_branch = original_branch
                settings.save()
            else:
                # Use the current branch
                result = GitIntegration.stage_changes(policy, commit_message)

            if result:
                messages.success(
                    request,
                    f'Policy "{policy.name}" staged for commit to branch {settings.current_branch}.',
                )
                return redirect("github_repo")
            else:
                messages.error(request, f'Failed to stage policy "{policy.name}".')
        except Exception as e:
            messages.error(request, f"Error syncing policy: {str(e)}")

        return redirect("policies")
    else:
        settings = GitSettings.get_instance()

        # Get all branches
        branches = []
        try:
            branches = GitIntegration.get_branches()
        except Exception:
            # Failed to get branches, but continue with empty list
            pass

        return render(
            request,
            "github_sync_policy.html",
            {
                "policy": policy,
                "settings": settings,
                "branches": branches,
                "current_branch": settings.current_branch,
            },
        )


def github_sync_all_policies(request):
    """Sync all policies to GitHub."""
    if not GitSettings.is_configured():
        messages.error(request, "GitHub integration is not configured.")
        return redirect("policies")

    settings = GitSettings.get_instance()

    if request.method == "POST":
        # Get form data
        commit_message = request.POST.get("commit_message", "Sync all policies")
        branch_name = request.POST.get("branch_name", settings.current_branch)

        # Check if we need to switch branch
        original_branch = None
        if branch_name and branch_name != settings.current_branch:
            original_branch = settings.current_branch
            settings.current_branch = branch_name
            settings.save()

        # Get all policies
        policies = PolicyTemplate.objects.all()

        success_count = 0
        error_count = 0

        try:
            for policy in policies:
                try:
                    # Sync each policy
                    result = GitIntegration.stage_changes(policy, commit_message)
                    if result:
                        success_count += 1
                    else:
                        error_count += 1
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error staging policy {policy.name}: {str(e)}")
                    # Continue with other policies even if one fails
                    continue

            if success_count > 0:
                messages.success(
                    request, f"Successfully staged {success_count} policies for commit."
                )
                if error_count > 0:
                    messages.warning(
                        request, f"Failed to stage {error_count} policies."
                    )
            else:
                messages.error(request, "Failed to stage any policies.")

            # Switch back to original branch if needed
            if original_branch:
                settings.current_branch = original_branch
                settings.save()

            return redirect("github_repo")

        except Exception as e:
            # Switch back to original branch if needed
            if original_branch:
                settings.current_branch = original_branch
                settings.save()

            messages.error(request, f"Error syncing policies: {str(e)}")
            return redirect("policies")


def github_revert_staged_file(request):
    """Revert/delete a staged file in the Git repository."""
    if request.method != "POST":
        return redirect("github_index")

    if not GitSettings.is_configured():
        messages.error(request, "Git integration is not configured.")
        return redirect("github_settings")

    settings = GitSettings.get_instance()

    file_path = request.POST.get("file_path")
    if not file_path:
        messages.error(request, "File path is required.")
        return redirect("github_index")

    # Initialize Git service
    git_service = GitService()

    try:
        # Revert/delete the staged file
        result = git_service.revert_staged_file(file_path, settings.current_branch)

        if result:
            messages.success(
                request, f'Successfully reverted staged changes for "{file_path}".'
            )
        else:
            messages.error(
                request, f'Failed to revert staged changes for "{file_path}".'
            )
    except Exception as e:
        messages.error(request, f"Error reverting staged changes: {str(e)}")

    return redirect("github_index")


def github_pull_request_detail(request, pr_id):
    """View details of a specific pull request."""
    pr = get_object_or_404(GitHubPR, id=pr_id)
    settings = GitSettings.get_instance()

    context = {
        "pr": pr,
        "github_settings": settings,
        "is_configured": settings.is_configured(),
    }

    return render(request, "github/pull_request_detail.html", context)


@require_POST
def github_delete_branch(request):
    """Delete a branch from GitHub."""
    if not GitSettings.is_configured():
        messages.error(request, "GitHub integration is not configured")
        return redirect("github")

    branch_name = request.POST.get("branch_name")

    if not branch_name:
        messages.error(request, "Branch name is required")
        return redirect("github")

    # Prevent deletion of main/master branches
    if branch_name.lower() in ["main", "master"]:
        messages.error(request, "Cannot delete main/master branch")
        return redirect("github")

    GitSettings.get_instance()


@require_POST
def github_create_pr(request):
    """Create a PR from staged changes."""
    if not GitSettings.is_configured():
        return JsonResponse(
            {"success": False, "error": "GitHub integration not configured"}
        )

    try:
        # Get current branch
        settings = GitSettings.get_instance()
        current_branch = settings.current_branch or "main"

        # Prevent creating PRs directly from main/master branch
        if current_branch.lower() in ["main", "master"]:
            return JsonResponse(
                {
                    "success": False,
                    "error": "Cannot create a PR directly from the main branch. Please create and use a feature branch.",
                }
            )

        title = request.POST.get("title")
        description = request.POST.get("description")
        base = request.POST.get("base")

        # Get the GitHub integration
        github = GitIntegration()

        # Create PR from staged changes
        pr = github.create_pr_from_staged_changes(title, description, base)

        if pr:
            return JsonResponse(
                {
                    "success": True,
                    "pr_url": pr["pr_url"],
                    "message": f"Pull request #{pr['pr_number']} created successfully",
                }
            )
        else:
            return JsonResponse(
                {"success": False, "error": "Failed to create pull request"}
            )
    except Exception as e:
        logger.error(f"Error creating PR: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@csrf_exempt
def github_branch_diff(request):
    """Get the diff between two branches."""
    if not GitSettings.is_configured():
        return JsonResponse(
            {"success": False, "error": "GitHub integration is not configured."}
        )

    # Get settings
    settings = GitSettings.get_instance()

    # Get request data
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            branch = data.get("branch", settings.current_branch or "main")
        except json.JSONDecodeError:
            branch = request.POST.get("branch", settings.current_branch or "main")
    else:
        branch = request.GET.get("branch", settings.current_branch or "main")

    # Initialize git service
    git_service = GitService()

    try:
        # Get staged files
        staged_files = git_service.get_staged_files(branch)

        # Return the list of files
        return JsonResponse({"success": True, "branch": branch, "files": staged_files})
    except Exception as e:
        logger.error(f"Error getting branch diff: {str(e)}")
        return JsonResponse({"success": False, "error": f"Error: {str(e)}"})


@csrf_exempt
def github_file_diff(request):
    """Get the diff for a specific file."""
    if not GitSettings.is_configured():
        return JsonResponse(
            {"success": False, "error": "GitHub integration is not configured."}
        )

    # Get settings
    settings = GitSettings.get_instance()

    # Get request data
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            file_path = data.get("file_path")
            branch = data.get("branch", settings.current_branch or "main")
        except json.JSONDecodeError:
            file_path = request.POST.get("file_path")
            branch = request.POST.get("branch", settings.current_branch or "main")
    else:
        file_path = request.GET.get("file_path")
        branch = request.GET.get("branch", settings.current_branch or "main")

    if not file_path:
        return JsonResponse({"success": False, "error": "File path is required"})

    # Initialize git service
    git_service = GitService()

    try:
        # Get file diff
        diff_content = git_service.get_file_diff(file_path, branch)

        # Return the diff
        return JsonResponse(
            {
                "success": True,
                "file_path": file_path,
                "branch": branch,
                "diff": diff_content,
            }
        )
    except Exception as e:
        logger.error(f"Error getting file diff: {str(e)}")
        return JsonResponse({"success": False, "error": f"Error: {str(e)}"})


@csrf_exempt
def github_workflows_overview(request):
    """Get an overview of GitHub workflows."""
    logger.info(f"Request method: {request.method}")
    logger.info(f"Request GET data: {request.GET}")
    logger.info(f"Request POST data: {request.POST}")

    if not GitSettings.is_configured():
        logger.error("GitHub integration is not configured, cannot fetch workflows")
        return JsonResponse(
            {"success": False, "error": "GitHub integration is not configured."}
        )

    # Get settings
    settings = GitSettings.get_instance()

    # Get branch from query params - preserve case exactly as provided
    branch = request.GET.get("ref", settings.current_branch or "main")
    logger.info(f"Using branch: '{branch}' for workflow overview")

    try:
        # Initialize workflow analyzer
        from .utils.workflow_analyzer import WorkflowAnalyzer

        # Log credentials being used (without the token)
        logger.info(
            f"Using credentials - Username: {settings.username}, Repository: {settings.repository}, Base URL: {settings.base_url or 'default'}"
        )
        analyzer = WorkflowAnalyzer(
            settings.username, settings.repository, settings.token, settings.base_url
        )

        # Get workflow data
        logger.info(f"Fetching workflows for branch: {branch}")
        workflows = analyzer.get_workflows(branch)

        if not workflows:
            logger.warning(f"No workflows found for branch: {branch}")
            return JsonResponse(
                {
                    "success": True,
                    "workflows": [],
                    "message": f"No workflows found in branch: {branch}",
                }
            )

        logger.info(f"Successfully retrieved {len(workflows)} workflows")
        return JsonResponse({"success": True, "workflows": workflows})
    except Exception as e:
        logger.error(f"Error analyzing workflows: {str(e)}")
        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")
        return JsonResponse(
            {"success": False, "error": f"Error analyzing workflows: {str(e)}"}
        )


@require_POST
def policy_push_github(request, policy_id):
    """Push a policy to Git repository."""
    # Add detailed logging for debugging
    logger.info(f"Attempting to push policy to Git. Policy ID: {policy_id}")

    # Check if Git integration is configured
    if not GitSettings.is_configured():
        logger.warning("Git integration not configured")
        return JsonResponse(
            {
                "success": False,
                "error": "Git integration is not configured. Please configure it in the settings.",
            }
        )

    # Get DataHub client
    client = get_datahub_client_from_request(request)
    if not client or not client.test_connection():
        logger.error("DataHub connection not available")
        return JsonResponse(
            {
                "success": False,
                "error": "DataHub connection not available. Please check your connection settings.",
            }
        )

    try:
        # First try to fetch policy from DataHub API
        logger.info(f"Fetching policy with ID: '{policy_id}' from DataHub API")
        datahub_policy = None

        try:
            # Try with the direct ID
            datahub_policy = client.get_policy(policy_id)

            # If not found, try with URN format
            if not datahub_policy and not policy_id.startswith("urn:"):
                datahub_policy = client.get_policy(f"urn:li:policy:{policy_id}")

            if not datahub_policy:
                logger.error(f"Policy with ID '{policy_id}' not found in DataHub")
                return JsonResponse(
                    {
                        "success": False,
                        "error": f"Policy with ID '{policy_id}' not found in DataHub",
                    }
                )
        except Exception as e:
            logger.error(f"Error fetching policy from DataHub: {str(e)}")
            return JsonResponse(
                {
                    "success": False,
                    "error": f"Error fetching policy from DataHub: {str(e)}",
                }
            )

        # Get environment from request (default to None if not provided)
        environment_id = request.POST.get("environment")
        environment = None
        if environment_id:
            try:
                environment = Environment.objects.get(id=environment_id)
                logger.info(f"Using environment '{environment.name}' for policy")
            except Environment.DoesNotExist:
                logger.warning(
                    f"Environment with ID {environment_id} not found, using default"
                )
                environment = Environment.get_default()
        else:
            environment = Environment.get_default()
            logger.info(f"No environment specified, using default: {environment.name}")

        # Convert the DataHub policy to a Django model instance for Git integration
        policy = Policy(
            id=policy_id,
            name=datahub_policy.get("name", "Unnamed Policy"),
            description=datahub_policy.get("description", ""),
            type=datahub_policy.get("type", "METADATA"),
            state=datahub_policy.get("state", "ACTIVE"),
            resources=json.dumps(datahub_policy.get("resources", [])),
            privileges=json.dumps(datahub_policy.get("privileges", [])),
            actors=json.dumps(datahub_policy.get("actors", {})),
            environment=environment,
        )

        # Get Git settings
        settings = GitSettings.get_instance()
        current_branch = settings.current_branch or "main"

        # Prevent pushing directly to main/master branch
        if current_branch.lower() in ["main", "master"]:
            logger.warning(f"Attempted to push directly to {current_branch} branch")
            return JsonResponse(
                {
                    "success": False,
                    "error": "Cannot push directly to the main/master branch. Please create and use a feature branch.",
                }
            )

        # Create commit message
        commit_message = f"Update policy: {policy.name}"

        # Stage the policy to the git repo
        logger.info(f"Staging policy {policy.id} to Git branch {current_branch}")
        git_integration = GitIntegration()
        result = git_integration.push_to_git(policy, commit_message)

        if result and result.get("success"):
            # Success response
            logger.info(
                f"Successfully staged policy {policy.id} to Git branch {current_branch}"
            )
            return JsonResponse(
                {
                    "success": True,
                    "message": f'Policy "{policy.name}" staged for commit to branch {current_branch}. You can create a PR from the Git Repository tab.',
                    "redirect_url": reverse("github_index"),
                }
            )
        else:
            # Failed to stage changes
            error_message = f'Failed to stage policy "{policy.name}"'
            if isinstance(result, dict) and "error" in result:
                error_message += f": {result['error']}"

            logger.error(f"Failed to stage policy: {error_message}")
            return JsonResponse({"success": False, "error": error_message})
    except Exception as e:
        # Log the error
        logger.error(f"Error pushing policy to Git: {str(e)}", exc_info=True)

        # Send a user-friendly error message
        return JsonResponse(
            {
                "success": False,
                "error": f"An error occurred while pushing the policy: {str(e)}",
            }
        )


def github_secrets(request):
    """
    View to list and manage GitHub repository secrets.
    """
    github_service = GitHubService()
    github_configured = github_service.is_configured()
    repo_secrets = []
    environments = []
    env_secrets = {}
    selected_environment = request.GET.get("environment", "")

    if github_configured:
        try:
            # Get repository secrets
            repo_secrets = github_service.get_repository_secrets()

            # Update or create secrets in our database for tracking
            for secret in repo_secrets:
                from .models import GitSecrets

                GitSecrets.objects.update_or_create(
                    name=secret["name"],
                    environment="",
                    defaults={
                        "description": f"Repository secret last updated {secret.get('updated_at', 'unknown')}",
                        "is_configured": True,
                    },
                )

            # Get available environments
            environments = github_service.get_environments()

            # If an environment is selected, get its secrets
            if selected_environment:
                env_secrets = {
                    "name": selected_environment,
                    "secrets": github_service.get_environment_secrets(
                        selected_environment
                    ),
                }

                # Update or create environment secrets in our database
                for secret in env_secrets["secrets"]:
                    from .models import GitSecrets

                    GitSecrets.objects.update_or_create(
                        name=secret["name"],
                        environment=selected_environment,
                        defaults={
                            "description": f"Environment secret last updated {secret.get('updated_at', 'unknown')}",
                            "is_configured": True,
                        },
                    )

        except Exception as e:
            logger.error(f"Error fetching GitHub secrets: {str(e)}")
            messages.error(request, f"Error fetching GitHub secrets: {str(e)}")

    return render(
        request,
        "github/secrets.html",
        {
            "title": "GitHub Secrets",
            "github_configured": github_configured,
            "repo_secrets": repo_secrets,
            "environments": environments,
            "selected_environment": selected_environment,
            "env_secrets": env_secrets,
        },
    )


@require_POST
def github_create_secret(request):
    """
    Create or update a GitHub repository or environment secret.
    """
    secret_name = request.POST.get("name")
    secret_value = request.POST.get("value")
    environment = request.POST.get("environment", "")

    if not secret_name or not secret_value:
        messages.error(request, "Secret name and value are required")
        return redirect("github_secrets")

    github_service = GitHubService()
    if not github_service.is_configured():
        messages.error(request, "GitHub integration is not configured")
        return redirect("github_secrets")

    try:
        success = False

        # Check if we're creating an environment secret or repository secret
        if environment:
            success = github_service.create_or_update_environment_secret(
                environment, secret_name, secret_value
            )

            if success:
                # Save to our database for tracking (we don't store the value)
                from .models import GitSecrets

                GitSecrets.objects.update_or_create(
                    name=secret_name,
                    environment=environment,
                    defaults={
                        "description": f"Environment secret updated on {timezone.now().strftime('%Y-%m-%d %H:%M:%S')}",
                        "is_configured": True,
                    },
                )
                messages.success(
                    request,
                    f"Environment secret '{secret_name}' created/updated successfully for '{environment}'",
                )
            else:
                messages.error(
                    request,
                    f"Failed to create/update environment secret '{secret_name}' for '{environment}'",
                )
        else:
            # Create repository-level secret
            success = github_service.create_or_update_secret(secret_name, secret_value)

            if success:
                # Save to our database for tracking (we don't store the value)
                from .models import GitSecrets

                GitSecrets.objects.update_or_create(
                    name=secret_name,
                    environment="",
                    defaults={
                        "description": f"Repository secret updated on {timezone.now().strftime('%Y-%m-%d %H:%M:%S')}",
                        "is_configured": True,
                    },
                )
                messages.success(
                    request,
                    f"Repository secret '{secret_name}' created/updated successfully",
                )
            else:
                messages.error(
                    request,
                    f"Failed to create/update repository secret '{secret_name}'",
                )

    except Exception as e:
        logger.error(f"Error creating/updating GitHub secret: {str(e)}")
        messages.error(request, f"Error creating/updating secret: {str(e)}")

    # Redirect back to the secrets page, preserving environment selection if any
    if environment:
        return redirect(f"{reverse('github_secrets')}?environment={environment}")
    return redirect("github_secrets")


@require_POST
def github_delete_secret(request):
    """
    Delete a GitHub repository or environment secret.
    """
    secret_name = request.POST.get("name")
    environment = request.POST.get("environment", "")

    if not secret_name:
        messages.error(request, "Secret name is required")
        return redirect("github_secrets")

    github_service = GitHubService()
    if not github_service.is_configured():
        messages.error(request, "GitHub integration is not configured")
        return redirect("github_secrets")

    try:
        success = False

        # Check if we're deleting an environment secret or repository secret
        if environment:
            success = github_service.delete_environment_secret(environment, secret_name)

            if success:
                # Remove from our database
                from .models import GitSecrets

                GitSecrets.objects.filter(
                    name=secret_name, environment=environment
                ).delete()
                messages.success(
                    request,
                    f"Environment secret '{secret_name}' deleted successfully from '{environment}'",
                )
            else:
                messages.error(
                    request,
                    f"Failed to delete environment secret '{secret_name}' from '{environment}'",
                )
        else:
            # Delete repository-level secret
            success = github_service.delete_secret(secret_name)

            if success:
                # Remove from our database
                from .models import GitSecrets

                GitSecrets.objects.filter(name=secret_name, environment="").delete()
                messages.success(
                    request, f"Repository secret '{secret_name}' deleted successfully"
                )
            else:
                messages.error(
                    request, f"Failed to delete repository secret '{secret_name}'"
                )

    except Exception as e:
        logger.error(f"Error deleting GitHub secret: {str(e)}")
        messages.error(request, f"Error deleting secret: {str(e)}")

    # Redirect back to the secrets page, preserving environment selection if any
    if environment:
        return redirect(f"{reverse('github_secrets')}?environment={environment}")
    return redirect("github_secrets")


def refresh_logs(request):
    """View for refreshing logs asynchronously."""
    # Get filter parameters from request
    level_filter = request.GET.get("level", "")
    source_filter = request.GET.get("source", "")
    search_query = request.GET.get("search", "")
    date_filter = request.GET.get("date", "")

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
            from datetime import datetime

            date = datetime.strptime(date_filter, "%Y-%m-%d").date()
            logs_query = logs_query.filter(timestamp__date=date)
        except ValueError:
            pass

    # Paginate the results
    from django.core.paginator import Paginator

    paginator = Paginator(logs_query, 50)  # Show 50 logs per page
    page_number = request.GET.get("page", 1)
    logs_page = paginator.get_page(page_number)

    return render(
        request,
        "logs_table.html",
        {
            "logs": logs_page,
            "current_level": level_filter,
            "current_source": source_filter,
            "search_query": search_query,
            "date_filter": date_filter,
        },
    )


def env_vars_template_list(request):
    """Get a list of all environment variables templates as JSON."""
    templates = EnvVarsTemplate.objects.all().order_by("name")

    templates_list = [
        {
            "id": template.id,
            "name": template.name,
            "recipe_type": template.recipe_type,
            "description": template.description,
            "tags": template.tags,  # Return the raw tags string for splitting in JavaScript
            "variable_count": len(template.get_variables_dict()),
            "created_at": template.created_at.isoformat(),
        }
        for template in templates
    ]

    return JsonResponse({"templates": templates_list})


def env_vars_template_get(request, template_id):
    """Get a specific environment variables template as JSON."""
    template = get_object_or_404(EnvVarsTemplate, id=template_id)

    response_data = {
        "id": template.id,
        "name": template.name,
        "description": template.description,
        "recipe_type": template.recipe_type,
        "variables": template.get_variables_dict(),
        "tags": template.get_tags_list(),
        "created_at": template.created_at.isoformat(),
        "updated_at": template.updated_at.isoformat(),
    }

    return JsonResponse(response_data)


def env_vars_template_details(request, template_id):
    """Get the details of an environment variables template as JSON."""
    template = get_object_or_404(EnvVarsTemplate, id=template_id)

    variables = template.get_variables_dict()

    # Convert to a list of variables with key, description, required, is_secret fields
    variables_list = []
    for key, details in variables.items():
        variables_list.append(
            {
                "key": key,
                "description": details.get("description", ""),
                "required": details.get("required", False),
                "is_secret": details.get("is_secret", False),
                "default_value": details.get("default_value", ""),
                "data_type": details.get("data_type", "text"),
            }
        )

    response_data = {
        "id": template.id,
        "name": template.name,
        "description": template.description,
        "recipe_type": template.recipe_type,
        "variables": variables_list,
    }

    return JsonResponse(response_data)


def env_vars_instance_list(request):
    """Get a list of all environment variables instances as JSON."""
    instances = EnvVarsInstance.objects.all().order_by("name")

    instances_list = [
        {
            "id": instance.id,
            "name": instance.name,
            "recipe_type": instance.recipe_type,
            "description": instance.description,
            "template_id": instance.template.id if instance.template else None,
            "template_name": instance.template.name if instance.template else None,
            "variable_count": len(instance.get_variables_dict()),
        }
        for instance in instances
    ]

    return JsonResponse({"instances": instances_list})


def env_vars_instance_json(request, instance_id):
    """Get a specific environment variables instance as JSON."""
    instance = get_object_or_404(EnvVarsInstance, id=instance_id)

    response_data = {
        "success": True,
        "instance": {
            "id": instance.id,
            "name": instance.name,
            "description": instance.description,
            "recipe_type": instance.recipe_type,
            "template_id": instance.template.id if instance.template else None,
            "template_name": instance.template.name if instance.template else None,
            "variables": instance.get_variables_dict(),
            "created_at": instance.created_at.isoformat(),
            "updated_at": instance.updated_at.isoformat(),
        },
    }

    return JsonResponse(response_data)


def env_vars_template_delete(request, template_id):
    """Delete an environment variables template."""
    template = get_object_or_404(EnvVarsTemplate, id=template_id)

    # Check if template is in use by any instances
    instances_using_template = EnvVarsInstance.objects.filter(template=template).count()

    if request.method == "POST":
        template_name = template.name
        template.delete()
        messages.success(
            request,
            f"Environment variables template '{template_name}' deleted successfully",
        )
        return redirect("env_vars_templates")

    return render(
        request,
        "env_vars/template_confirm_delete.html",
        {"template": template, "instances_count": instances_using_template},
    )


def env_vars_instance_detail(request, instance_id):
    """View details of an environment variables instance."""
    instance = get_object_or_404(EnvVarsInstance, id=instance_id)
    variables = instance.get_variables_dict()

    return render(
        request,
        "env_vars/instance_detail.html",
        {
            "instance": instance,
            "variables": variables,
        },
    )


def env_vars_instance_edit(request, instance_id):
    """Edit an existing environment variables instance."""
    instance = get_object_or_404(EnvVarsInstance, id=instance_id)

    # Get all environments
    environments = Environment.objects.all().order_by("name")

    if request.method == "POST":
        form = EnvVarsInstanceForm(request.POST)
        if form.is_valid():
            # Update instance
            instance.name = form.cleaned_data["name"]
            instance.description = form.cleaned_data["description"]
            instance.template = form.cleaned_data["template"]
            instance.recipe_type = form.cleaned_data["recipe_type"]
            instance.variables = form.cleaned_data["variables"]
            instance.save()

            messages.success(
                request,
                f"Environment variables instance '{instance.name}' updated successfully",
            )
            return redirect("env_vars_instances")
        else:
            messages.error(request, "Please correct the errors below")
    else:
        # Get the variables as a parsed dictionary
        variables_dict = instance.get_variables_dict()

        form = EnvVarsInstanceForm(
            initial={
                "name": instance.name,
                "description": instance.description,
                "template": instance.template.id if instance.template else None,
                "recipe_type": instance.recipe_type,
                "variables": json.dumps(
                    variables_dict
                ),  # Convert the parsed dict back to JSON
            }
        )

    # Pass the variables_json to the template for proper JavaScript initialization
    variables_dict = instance.get_variables_dict()
    variables_json = json.dumps(variables_dict)

    return render(
        request,
        "env_vars/instance_form.html",
        {
            "form": form,
            "instance": instance,
            "environments": environments,
            "variables_json": variables_json,  # Add this to pass properly encoded JSON
            "title": f"Edit Environment Variables Instance: {instance.name}",
            "is_new": False,
        },
    )


def env_vars_instance_delete(request, instance_id):
    """Delete an environment variables instance."""
    instance = get_object_or_404(EnvVarsInstance, id=instance_id)

    if request.method == "POST":
        instance_name = instance.name
        try:
            instance.delete()
            messages.success(
                request,
                f"Environment variables instance '{instance_name}' deleted successfully",
            )
        except Exception as e:
            messages.error(request, f"Error deleting instance: {str(e)}")
        return redirect("env_vars_instances")

    return render(
        request, "env_vars/instance_confirm_delete.html", {"instance": instance}
    )


def recipe_instances(request):
    """List all recipe instances."""
    instances = RecipeInstance.objects.all().order_by("-updated_at")

    # Group by deployment status
    deployed = instances.filter(deployed=True)
    staging = instances.filter(deployed=False)

    return render(
        request,
        "recipes/instances.html",
        {"title": "Recipe Instances", "deployed": deployed, "staging": staging},
    )


def recipe_instance_create(request):
    """Create a new recipe instance."""

    # Get all recipe templates
    templates = RecipeTemplate.objects.all().order_by("name")

    # Get all environment variable instances
    env_vars_instances = EnvVarsInstance.objects.all().order_by("name")

    # Get all environments
    environments = Environment.objects.all().order_by("name")

    if request.method == "POST":
        form = RecipeInstanceForm(request.POST)

        if form.is_valid():
            # Get form data
            name = form.cleaned_data["name"]
            description = form.cleaned_data["description"]
            template = form.cleaned_data[
                "template"
            ]  # This is a RecipeTemplate object, not an ID
            env_vars_instance = form.cleaned_data.get(
                "env_vars_instance"
            )  # This is an EnvVarsInstance object or None
            environment = form.cleaned_data.get(
                "environment"
            )  # This is an Environment object or None
            cron_schedule = form.cleaned_data.get("cron_schedule") or "0 0 * * *"
            timezone = form.cleaned_data.get("timezone") or "UTC"
            debug_mode = form.cleaned_data.get("debug_mode", False)

            # Create recipe instance
            instance = RecipeInstance(
                name=name,
                description=description,
                template=template,  # Use the object directly
                env_vars_instance=env_vars_instance,  # Use the object directly
                environment=environment,  # Use the object directly
                cron_schedule=cron_schedule,
                timezone=timezone,
                debug_mode=debug_mode,
            )

            instance.save()

            messages.success(request, f"Recipe instance '{name}' created successfully")
            return redirect("recipe_instances")

    else:
        form = RecipeInstanceForm()

    return render(
        request,
        "recipes/instance_form.html",
        {
            "form": form,
            "templates": templates,
            "env_vars_instances": env_vars_instances,
            "environments": environments,
            "is_new": True,
            "title": "Create Recipe Instance",
        },
    )


def github_environments(request):
    """
    View to list and manage GitHub environments. It shows local environments
    and GitHub environments, indicating which ones need to be synced.
    """
    github_service = GitHubService()
    github_configured = github_service.is_configured()

    # Get all local environments
    local_environments = Environment.objects.all().order_by("name")

    # Get all GitHub environments if GitHub is configured
    github_environments = []

    if github_configured:
        try:
            github_environments = github_service.get_environments()

            # Create a mapping of environment names (lowercase for case-insensitive comparison)
            github_env_names = {env["name"].lower(): env for env in github_environments}

            # Check which local environments exist in GitHub and add properties directly to each environment object
            for local_env in local_environments:
                local_env.exists_in_github = local_env.name.lower() in github_env_names
                local_env.github_env = github_env_names.get(local_env.name.lower())

        except Exception as e:
            logger.error(f"Error fetching GitHub environments: {str(e)}")
            messages.error(request, f"Error fetching GitHub environments: {str(e)}")

    return render(
        request,
        "github/environments.html",
        {
            "title": "GitHub Environments",
            "github_configured": github_configured,
            "local_environments": local_environments,
            "github_environments": github_environments,
        },
    )


@require_POST
def github_create_environment(request):
    """
    Create a GitHub environment from a local environment.
    """
    environment_id = request.POST.get("environment_id")

    if not environment_id:
        messages.error(request, "Environment ID is required")
        return redirect("github_environments")

    github_service = GitHubService()
    if not github_service.is_configured():
        messages.error(request, "GitHub integration is not configured")
        return redirect("github_environments")

    try:
        # Get the local environment
        environment = get_object_or_404(Environment, id=environment_id)

        # Create the environment in GitHub
        success = github_service.create_environment(
            name=environment.name,
            # Optional parameters can be added here if needed
            # wait_timer=0,
            # reviewers=[],
            # prevent_self_review=False,
            # protected_branches=False
        )

        if success:
            messages.success(
                request,
                f"Environment '{environment.name}' created successfully in GitHub",
            )
        else:
            messages.error(
                request, f"Failed to create environment '{environment.name}' in GitHub"
            )

    except Exception as e:
        logger.error(f"Error creating GitHub environment: {str(e)}")
        messages.error(request, f"Error creating environment: {str(e)}")

    return redirect("github_environments")


@require_POST
def policy_deploy(request, policy_id):
    """Deploy a local policy to DataHub."""
    # Get the policy from the database
    policy = get_object_or_404(Policy, id=policy_id)

    # Get DataHub client
    client = get_datahub_client_from_request(request)
    if not client or not client.test_connection():
        messages.error(
            request, "Not connected to DataHub. Please check your connection settings."
        )
        return redirect("policies")

    try:
        # Check if policy exists on DataHub first
        existing_policy = None
        try:
            existing_policy = client.get_policy(policy.id)
        except Exception as e:
            logger.warning(f"Error checking if policy exists: {str(e)}")

        # Prepare the common policy properties
        base_policy_data = {
            "name": policy.name,
            "description": policy.description or "",
            "type": policy.type,
            "state": policy.state,
            "resources": json.loads(policy.resources) if policy.resources else [],
            "privileges": json.loads(policy.privileges) if policy.privileges else [],
            "actors": json.loads(policy.actors) if policy.actors else {},
        }

        if existing_policy:
            # For updates, include the ID
            datahub_policy = {"id": policy.id, **base_policy_data}

            # Update existing policy
            logger.info(f"Updating existing policy: {policy.id}")
            result = client.update_policy(policy.id, datahub_policy)

            if result:
                messages.success(
                    request, f"Policy '{policy.name}' updated successfully on DataHub"
                )
            else:
                messages.error(
                    request, f"Failed to update policy '{policy.name}' on DataHub"
                )
        else:
            # For new policies, some DataHub versions don't accept 'id' in PolicyUpdateInput
            # Use two different approaches to maximize compatibility
            try:
                # Try first with ID included
                datahub_policy = {"id": policy.id, **base_policy_data}
                logger.info(f"Creating new policy with ID: {policy.id}")
                result = client.create_policy(datahub_policy)

                if not result:
                    # If that fails, retry without explicit ID in the input
                    # DataHub will use the name as ID
                    logger.info("Retrying policy creation without explicit ID")
                    datahub_policy = base_policy_data
                    result = client.create_policy(datahub_policy)
            except Exception as e:
                logger.error(f"Error in first policy creation attempt: {str(e)}")
                # Retry without ID
                datahub_policy = base_policy_data
                result = client.create_policy(datahub_policy)

            if result:
                messages.success(
                    request, f"Policy '{policy.name}' created successfully on DataHub"
                )
            else:
                messages.error(
                    request, f"Failed to create policy '{policy.name}' on DataHub"
                )

        return redirect("policies")
    except Exception as e:
        messages.error(request, f"Error deploying policy: {str(e)}")
        logger.error(f"Error deploying policy: {str(e)}", exc_info=True)
        return redirect("policies")


def template_env_vars_instances(request, template_id):
    """API endpoint to get environment variables instances matching a template's recipe type."""
    template = get_object_or_404(RecipeTemplate, id=template_id)
    recipe_type = template.recipe_type

    # Filter env vars instances by recipe type
    filtered_instances = EnvVarsInstance.objects.filter(
        recipe_type=recipe_type
    ).order_by("name")

    # Prepare response data
    instances = [
        {"id": instance.id, "name": instance.name, "description": instance.description}
        for instance in filtered_instances
    ]

    return JsonResponse({"recipe_type": recipe_type, "instances": instances})


def get_recipe_by_id(recipe_id):
    """
    Get a recipe by its ID.

    Args:
        recipe_id: ID of the recipe to retrieve

    Returns:
        dict: Recipe data if found, None otherwise
    """
    client = get_datahub_client_from_request(request)
    if not client or not client.test_connection():
        logger.error("Not connected to DataHub")
        return None

    try:
        # Get the recipe
        recipe_data = client.get_ingestion_source(recipe_id)
        if not recipe_data:
            logger.error(f"Recipe {recipe_id} not found")
            return None

        return recipe_data
    except Exception as e:
        logger.error(f"Error retrieving recipe {recipe_id}: {str(e)}")
        return None


@csrf_exempt
def github_load_branches(request):
    """Load branches asynchronously to avoid race conditions."""
    if not GitSettings.is_configured():
        return JsonResponse(
            {"success": False, "error": "GitHub integration is not configured."}
        )

    try:
        # Get all branches using GitIntegration instead of GitSettings
        from web_ui.models import GitIntegration
        branches = GitIntegration.get_branches()
        settings = GitSettings.get_instance()
        
        return JsonResponse({
            "success": True, 
            "branches": branches,
            "current_branch": settings.current_branch
        })
    except Exception as e:
        logger.error(f"Error loading branches: {str(e)}")
        return JsonResponse({"success": False, "error": f"Error: {str(e)}"})


def mutations(request):
    """List all mutations."""
    mutations = Mutation.objects.all().order_by("name")
    return render(
        request,
        "mutations/list.html",
        {"title": "Mutations", "mutations": mutations},
    )


def mutation_create(request):
    """Create a new mutation."""
    if request.method == "POST":
        name = request.POST.get("name")
        description = request.POST.get("description")
        env = request.POST.get("env")
        custom_properties = request.POST.get("custom_properties", "{}")
        platform_instance_mapping = request.POST.get("platform_instance_mapping", "{}")
        
        # Get checkbox values for apply mutations to entities
        apply_to_tags = request.POST.get("apply_to_tags") == "on"
        apply_to_glossary_terms = request.POST.get("apply_to_glossary_terms") == "on"
        apply_to_glossary_nodes = request.POST.get("apply_to_glossary_nodes") == "on"
        apply_to_structured_properties = request.POST.get("apply_to_structured_properties") == "on"
        apply_to_domains = request.POST.get("apply_to_domains") == "on"
        apply_to_data_products = request.POST.get("apply_to_data_products") == "on"
        
        try:
            # Validate JSON
            import json
            custom_props = json.loads(custom_properties) if custom_properties else {}
            platform_mapping = json.loads(platform_instance_mapping) if platform_instance_mapping else {}
        except json.JSONDecodeError:
            messages.error(request, "Invalid JSON format for custom properties or platform instance mapping")
            return render(
                request,
                "mutations/create.html",
                {
                    "title": "Create Mutation",
                    "form_data": request.POST,
                },
            )

        # Create the mutation - env field is deprecated and handled via platform_instance_mapping
        Mutation.objects.create(
            name=name,
            description=description,
            env=env or "default",  # Provide default value since field is deprecated
            custom_properties=custom_props,
            platform_instance_mapping=platform_mapping,
            apply_to_tags=apply_to_tags,
            apply_to_glossary_terms=apply_to_glossary_terms,
            apply_to_glossary_nodes=apply_to_glossary_nodes,
            apply_to_structured_properties=apply_to_structured_properties,
            apply_to_domains=apply_to_domains,
            apply_to_data_products=apply_to_data_products,
        )

        messages.success(request, f'Mutation "{name}" created successfully.')
        return redirect("environments")

    return render(request, "mutations/create.html", {"title": "Create Mutation"})


def mutation_edit(request, mutation_id):
    """Edit an existing mutation."""
    mutation = get_object_or_404(Mutation, id=mutation_id)

    if request.method == "POST":
        mutation.name = request.POST.get("name")
        mutation.description = request.POST.get("description")
        # Keep existing value for env since it's handled via mapping
        mutation.env = request.POST.get("env") or mutation.env
        custom_properties = request.POST.get("custom_properties", "{}")
        platform_instance_mapping = request.POST.get("platform_instance_mapping", "{}")
        
        # Get checkbox values for apply mutations to entities
        mutation.apply_to_tags = request.POST.get("apply_to_tags") == "on"
        mutation.apply_to_glossary_terms = request.POST.get("apply_to_glossary_terms") == "on"
        mutation.apply_to_glossary_nodes = request.POST.get("apply_to_glossary_nodes") == "on"
        mutation.apply_to_structured_properties = request.POST.get("apply_to_structured_properties") == "on"
        mutation.apply_to_domains = request.POST.get("apply_to_domains") == "on"
        mutation.apply_to_data_products = request.POST.get("apply_to_data_products") == "on"
        
        try:
            # Validate JSON
            import json
            mutation.custom_properties = json.loads(custom_properties) if custom_properties else {}
            mutation.platform_instance_mapping = json.loads(platform_instance_mapping) if platform_instance_mapping else {}
        except json.JSONDecodeError:
            messages.error(request, "Invalid JSON format for custom properties or platform instance mapping")
            return render(
                request,
                "mutations/edit.html",
                {
                    "title": "Edit Mutation",
                    "mutation": mutation,
                    "form_data": request.POST,
                },
            )

        mutation.save()
        messages.success(request, f'Mutation "{mutation.name}" updated successfully.')
        return redirect("environments")

    # Serialize custom_properties and platform_instance_mapping for display
    import json
    mutation_data = {
        'name': mutation.name,
        'description': mutation.description,
        'env': mutation.env,
        'custom_properties': json.dumps(mutation.custom_properties) if mutation.custom_properties else '{}',
        'platform_instance_mapping': json.dumps(mutation.platform_instance_mapping) if mutation.platform_instance_mapping else '{}'
    }
    
    return render(
        request,
        "mutations/edit.html",
        {"title": "Edit Mutation", "mutation": mutation, "mutation_data": mutation_data},
    )


def mutation_delete(request, mutation_id):
    """Delete a mutation."""
    mutation = get_object_or_404(Mutation, id=mutation_id)

    # Check if mutation is in use
    env_count = Environment.objects.filter(mutations=mutation).count()

    if request.method == "POST":
        if env_count == 0:
            name = mutation.name
            mutation.delete()
            messages.success(request, f"Mutation '{name}' deleted successfully")
        else:
            messages.error(
                request,
                f"Cannot delete mutation '{mutation.name}' - it is in use by {env_count} environments",
            )
        return redirect("environments")

    return render(
        request,
        "mutations/delete.html",
        {
            "title": f"Delete Mutation: {mutation.name}",
            "mutation": mutation,
            "env_count": env_count,
        },
    )


def connections_list(request):
    """List all DataHub connections."""
    try:
        from web_ui.models import Connection
        
        connections = Connection.objects.all().order_by('-is_default', 'name')
        
        context = {
            'connections': connections,
            'page_title': 'DataHub Connections',
        }
        
        return render(request, 'connections/list.html', context)
        
    except Exception as e:
        logger.error(f"Error in connections list view: {str(e)}")
        messages.error(request, f"Error loading connections: {str(e)}")
        return redirect('settings')


def connection_create(request):
    """Create a new DataHub connection."""
    if request.method == 'POST':
        try:
            from web_ui.models import Connection
            
            name = request.POST.get('name', '').strip()
            description = request.POST.get('description', '').strip()
            datahub_url = request.POST.get('datahub_url', '').strip()
            datahub_token = request.POST.get('datahub_token', '').strip()
            verify_ssl = 'verify_ssl' in request.POST
            timeout = int(request.POST.get('timeout', 30))
            is_default = 'is_default' in request.POST
            
            # Validation
            if not name:
                messages.error(request, "Connection name is required.")
                return render(request, 'connections/form.html', {
                    'form_data': request.POST,
                    'page_title': 'Create Connection'
                })
            
            if not datahub_url:
                messages.error(request, "DataHub URL is required.")
                return render(request, 'connections/form.html', {
                    'form_data': request.POST,
                    'page_title': 'Create Connection'
                })
            
            # Create connection
            connection = Connection.objects.create(
                name=name,
                description=description,
                datahub_url=datahub_url,
                datahub_token=datahub_token,
                verify_ssl=verify_ssl,
                timeout=timeout,
                is_default=is_default,
                is_active=True
            )
            
            # Test connection if requested
            if 'test_connection' in request.POST:
                if connection.test_connection():
                    messages.success(request, f"Connection '{name}' created and tested successfully!")
                else:
                    messages.warning(request, f"Connection '{name}' created but failed connection test: {connection.error_message}")
            else:
                messages.success(request, f"Connection '{name}' created successfully!")
            
            return redirect('connections_list')
            
        except Exception as e:
            logger.error(f"Error creating connection: {str(e)}")
            messages.error(request, f"Error creating connection: {str(e)}")
            return render(request, 'connections/form.html', {
                'form_data': request.POST,
                'page_title': 'Create Connection'
            })
    
    return render(request, 'connections/form.html', {
        'page_title': 'Create Connection'
    })


def connection_edit(request, connection_id):
    """Edit an existing DataHub connection."""
    try:
        from web_ui.models import Connection
        
        connection = get_object_or_404(Connection, id=connection_id)
        
        if request.method == 'POST':
            try:
                name = request.POST.get('name', '').strip()
                description = request.POST.get('description', '').strip()
                datahub_url = request.POST.get('datahub_url', '').strip()
                datahub_token = request.POST.get('datahub_token', '').strip()
                verify_ssl = 'verify_ssl' in request.POST
                timeout = int(request.POST.get('timeout', 30))
                is_default = 'is_default' in request.POST
                
                # Validation
                if not name:
                    messages.error(request, "Connection name is required.")
                    return render(request, 'connections/form.html', {
                        'connection': connection,
                        'form_data': request.POST,
                        'page_title': f'Edit Connection: {connection.name}'
                    })
                
                if not datahub_url:
                    messages.error(request, "DataHub URL is required.")
                    return render(request, 'connections/form.html', {
                        'connection': connection,
                        'form_data': request.POST,
                        'page_title': f'Edit Connection: {connection.name}'
                    })
                
                # Update connection
                connection.name = name
                connection.description = description
                connection.datahub_url = datahub_url
                if datahub_token:  # Only update token if provided
                    connection.datahub_token = datahub_token
                connection.verify_ssl = verify_ssl
                connection.timeout = timeout
                connection.is_default = is_default
                connection.save()
                
                # Test connection if requested
                if 'test_connection' in request.POST:
                    if connection.test_connection():
                        messages.success(request, f"Connection '{name}' updated and tested successfully!")
                    else:
                        messages.warning(request, f"Connection '{name}' updated but failed connection test: {connection.error_message}")
                else:
                    messages.success(request, f"Connection '{name}' updated successfully!")
                
                return redirect('connections_list')
                
            except Exception as e:
                logger.error(f"Error updating connection: {str(e)}")
                messages.error(request, f"Error updating connection: {str(e)}")
        
        context = {
            'connection': connection,
            'page_title': f'Edit Connection: {connection.name}',
            'has_token': bool(connection.datahub_token),
        }
        
        return render(request, 'connections/form.html', context)
        
    except Exception as e:
        logger.error(f"Error in connection edit view: {str(e)}")
        messages.error(request, f"Error loading connection: {str(e)}")
        return redirect('connections_list')


def connection_delete(request, connection_id):
    """Delete a DataHub connection."""
    try:
        from web_ui.models import Connection
        
        connection = get_object_or_404(Connection, id=connection_id)
        
        if request.method == 'POST':
            # Check if this is the default connection
            if connection.is_default:
                # Make another connection default if available
                other_connection = Connection.objects.filter(is_active=True).exclude(id=connection_id).first()
                if other_connection:
                    other_connection.is_default = True
                    other_connection.save()
                    messages.info(request, f"Made '{other_connection.name}' the new default connection.")
            
            connection_name = connection.name
            connection.delete()
            messages.success(request, f"Connection '{connection_name}' deleted successfully!")
            return redirect('connections_list')
        
        context = {
            'connection': connection,
            'page_title': f'Delete Connection: {connection.name}',
        }
        
        return render(request, 'connections/delete.html', context)
        
    except Exception as e:
        logger.error(f"Error in connection delete view: {str(e)}")
        messages.error(request, f"Error deleting connection: {str(e)}")
        return redirect('connections_list')

 
def connection_test(request, connection_id):
    """Test a DataHub connection."""
    try:
        from web_ui.models import Connection
        
        connection = get_object_or_404(Connection, id=connection_id)
        
        if connection.test_connection():
            messages.success(request, f"Connection '{connection.name}' tested successfully!")
        else:
            messages.error(request, f"Connection test failed for '{connection.name}': {connection.error_message}")
        
        return redirect('connections_list')
        
    except Exception as e:
        logger.error(f"Error testing connection: {str(e)}")
        messages.error(request, f"Error testing connection: {str(e)}")
        return redirect('connections_list')


def connection_set_default(request, connection_id):
    """Set a connection as the default."""
    try:
        from web_ui.models import Connection
        
        connection = get_object_or_404(Connection, id=connection_id)
        
        # Remove default from all other connections and set this one as default
        Connection.objects.all().update(is_default=False)
        connection.is_default = True
        connection.save()
        
        messages.success(request, f"'{connection.name}' is now the default connection.")
        return redirect('connections_list')
        
    except Exception as e:
        logger.error(f"Error setting default connection: {str(e)}")
        messages.error(request, f"Error setting default connection: {str(e)}")
        return redirect('connections_list')


@require_http_methods(["POST"])
def api_switch_connection(request):
    """API endpoint to switch the current connection context."""
    try:
        from web_ui.models import Connection
        
        connection_id = request.POST.get('connection_id')
        
        if not connection_id:
            return JsonResponse({'success': False, 'error': 'Connection ID is required'})
        
        connection = Connection.objects.filter(id=connection_id, is_active=True).first()
        
        if not connection:
            return JsonResponse({'success': False, 'error': 'Connection not found or inactive'})
        
        # Store the selected connection in session
        request.session['current_connection_id'] = str(connection.id)
        
        return JsonResponse({
            'success': True,
            'connection_name': connection.name,
            'connection_id': connection.id
        })
        
    except Exception as e:
        logger.error(f"Error switching connection: {str(e)}")
        return JsonResponse({'success': False, 'error': str(e)})


def get_current_connection(request):
    """Get the current connection for the user's session."""
    try:
        from web_ui.models import Connection
        
        # Try to get from session first
        connection_id = request.session.get('current_connection_id')
        
        if connection_id:
            connection = Connection.objects.filter(id=connection_id, is_active=True).first()
            if connection:
                return connection
        
        # Fall back to default connection
        return Connection.get_default()
        
    except Exception:
        return None



