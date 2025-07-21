import logging
from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.views import View
from django.contrib import messages
from django.utils import timezone
from django.views.decorators.http import require_POST, require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
import yaml
import subprocess

# Add project root to sys.path
import sys
import os
import subprocess

project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.append(project_root)

# Import the deterministic URN utilities
from utils.urn_utils import get_full_urn_from_name, generate_mutated_urn, get_mutation_config_for_environment
from utils.datahub_utils import test_datahub_connection, get_datahub_client, get_datahub_client_from_request
from utils.datahub_rest_client import DataHubRestClient
from .models import Assertion, AssertionResult, Domain, Environment
from web_ui.models import Environment as DjangoEnvironment

# Optional git integration imports
try:
    from git_integration.models import GitSettings
    from git_integration.core import GitIntegration

    GIT_INTEGRATION_AVAILABLE = True
except ImportError:
    GitSettings = None
    GitIntegration = None
    GIT_INTEGRATION_AVAILABLE = False

logger = logging.getLogger(__name__)


class AssertionListView(View):
    """View to list and create assertions"""
    
    def get(self, request):
        """Display list of assertions"""
        try:
            logger.info("Starting AssertionListView.get")
            
            # Get all local assertions and domains for domain assertions
            local_assertions = Assertion.objects.all().order_by("name")
            domains = Domain.objects.all().order_by("name")

            logger.debug(
                f"Found {local_assertions.count()} local assertions and {domains.count()} domains"
            )

            # Get DataHub connection info (quick test only)
            logger.debug("Testing DataHub connection from AssertionListView")
            connected, client = test_datahub_connection(request)
            logger.debug(f"DataHub connection test result: {connected}")
            
            # Initialize context with local data only
            context = {
                "local_assertions": local_assertions,
                "remote_assertions": [],  # Will be populated via AJAX
                "synced_assertions": [],  # TODO: Implement synced assertions logic
                "domains": domains,
                "has_datahub_connection": connected,
                "datahub_url": None,  # Will be populated via AJAX
                "page_title": "Assertions",
            }

            logger.info("Rendering assertion list template (async loading)")
            return render(request, "metadata_manager/assertions/list.html", context)
        except Exception as e:
            logger.error(f"Error in assertion list view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return render(
                request,
                "metadata_manager/assertions/list.html",
                {"error": str(e), "page_title": "Assertions"},
            )
    
    def post(self, request):
        """Create a new assertion"""
        try:
            name = request.POST.get("name")
            description = request.POST.get("description", "")
            assertion_type = request.POST.get("type")
            
            if not name:
                messages.error(request, "Assertion name is required")
                return redirect("assertion_list")
            
            if not assertion_type:
                messages.error(request, "Assertion type is required")
                return redirect("assertion_list")
            
            # Initialize config based on assertion type
            config = {}
            
            if assertion_type == "domain_exists":
                domain_id = request.POST.get("domain_id")
                if not domain_id:
                    messages.error(
                        request,
                        "Please select a domain for the domain existence assertion",
                    )
                    return redirect("assertion_list")
                
                try:
                    domain = Domain.objects.get(id=domain_id)
                    config = {
                        "domain_id": str(domain.id),
                        "domain_name": domain.name,
                        "domain_urn": domain.urn,
                    }
                except Domain.DoesNotExist:
                    messages.error(request, "Domain not found")
                    return redirect("assertion_list")

            elif assertion_type == "sql":
                sql_query = request.POST.get("sql_query")
                expected_result = request.POST.get("expected_result")
                
                if not sql_query:
                    messages.error(request, "SQL query is required")
                    return redirect("assertion_list")
                
                config = {"query": sql_query, "expected_result": expected_result}
            
            elif assertion_type == "tag_exists":
                tag_name = request.POST.get("tag_name")
                
                if not tag_name:
                    messages.error(request, "Tag name is required")
                    return redirect("assertion_list")
                
                config = {"tag_name": tag_name}
            
            elif assertion_type == "glossary_term_exists":
                term_name = request.POST.get("term_name")
                
                if not term_name:
                    messages.error(request, "Glossary term name is required")
                    return redirect("assertion_list")
                
                config = {"term_name": term_name}
            
            # Create the assertion
            Assertion.objects.create(
                name=name, description=description, type=assertion_type, config=config
            )
            
            messages.success(request, f"Assertion '{name}' created successfully")
            return redirect("assertion_list")
        except Exception as e:
            logger.error(f"Error creating assertion: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("assertion_list")


class AssertionDetailView(View):
    """View to display, edit, and delete assertions"""
    
    def get(self, request, assertion_id):
        """Display assertion details"""
        try:
            assertion = get_object_or_404(Assertion, id=assertion_id)
            results = AssertionResult.objects.filter(assertion=assertion).order_by(
                "-run_at"
            )[:10]

            return render(
                request,
                "metadata_manager/assertions/detail.html",
                {
                    "assertion": assertion,
                    "results": results,
                    "page_title": f"Assertion: {assertion.name}",
                },
            )
        except Exception as e:
            logger.error(f"Error in assertion detail view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("assertion_list")


class AssertionRunView(View):
    """View to run an assertion"""
    
    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def post(self, request, assertion_id):
        """Run an assertion"""
        try:
            assertion = get_object_or_404(Assertion, id=assertion_id)
            
            # Get DataHub connection
            connected, client = test_datahub_connection(request)
            
            if not connected or not client:
                messages.error(
                    request,
                    "Cannot connect to DataHub. Please check your connection settings.",
                )
                return redirect("assertion_detail", assertion_id=assertion_id)
            
            # Run assertion based on type
            result = None
            
            if assertion.type == "domain_exists":
                result = self.run_domain_exists_assertion(assertion, client)
            elif assertion.type == "sql":
                result = self.run_sql_assertion(assertion, client)
            elif assertion.type == "tag_exists":
                result = self.run_tag_exists_assertion(assertion, client)
            elif assertion.type == "glossary_term_exists":
                result = self.run_glossary_term_exists_assertion(assertion, client)
            
            if result:
                # Save the result
                AssertionResult.objects.create(
                    assertion=assertion,
                    status="SUCCESS" if result["success"] else "FAILED",
                    details=result["details"],
                    run_at=timezone.now(),
                )
                
                # Update assertion
                assertion.last_run = timezone.now()
                assertion.last_status = "SUCCESS" if result["success"] else "FAILED"
                assertion.save()
                
                if result["success"]:
                    messages.success(request, f"Assertion '{assertion.name}' passed")
                else:
                    messages.warning(
                        request,
                        f"Assertion '{assertion.name}' failed: {result['details'].get('message', 'Unknown error')}",
                    )
            else:
                messages.error(request, "Failed to run assertion")
            
            return redirect("assertion_detail", assertion_id=assertion_id)
        except Exception as e:
            logger.error(f"Error running assertion: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("assertion_list")
    
    def run_domain_exists_assertion(self, assertion, client):
        """Run a domain_exists assertion"""
        try:
            config = assertion.config
            domain_urn = config.get("domain_urn")
            
            if not domain_urn:
                return {
                    "success": False,
                    "details": {
                        "message": "Domain URN not found in assertion configuration"
                    },
                }
            
            # Check if domain exists in DataHub
            domain = client.get_domain(domain_urn)
            
            exists = domain is not None
            
            return {
                "success": exists,
                "details": {
                    "message": f"Domain {'exists' if exists else 'does not exist'} in DataHub",
                    "domain_urn": domain_urn,
                    "domain_name": config.get("domain_name"),
                    "exists": exists,
                    "datahub_response": domain if domain else "Not found",
                },
            }
        except Exception as e:
            logger.error(f"Error running domain_exists assertion: {str(e)}")
            return {
                "success": False,
                "details": {"message": f"Error running assertion: {str(e)}"},
            }
    
    def run_sql_assertion(self, assertion, client):
        """Run a SQL assertion"""
        # This is a placeholder, actual implementation would depend on DataHub's SQL execution capabilities
        return {
            "success": False,
            "details": {"message": "SQL assertions are not yet implemented"},
        }
    
    def run_tag_exists_assertion(self, assertion, client):
        """Run a tag_exists assertion"""
        try:
            config = assertion.config
            tag_name = config.get("tag_name")
            
            if not tag_name:
                return {
                    "success": False,
                    "details": {
                        "message": "Tag name not found in assertion configuration"
                    },
                }
            
            # Generate the tag URN using consistent system
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            current_environment = getattr(current_connection, 'environment', 'dev')
            base_tag_urn = get_full_urn_from_name("tag", tag_name)
            mutation_config = get_mutation_config_for_environment(current_environment)
            tag_urn = generate_mutated_urn(base_tag_urn, current_environment, "tag", mutation_config)
            
            # Check if tag exists in DataHub
            tag = client.get_tag(tag_urn)
            
            exists = tag is not None
            
            return {
                "success": exists,
                "details": {
                    "message": f"Tag {'exists' if exists else 'does not exist'} in DataHub",
                    "tag_name": tag_name,
                    "tag_urn": tag_urn,
                    "exists": exists,
                    "datahub_response": tag if tag else "Not found",
                },
            }
        except Exception as e:
            logger.error(f"Error running tag_exists assertion: {str(e)}")
            return {
                "success": False,
                "details": {"message": f"Error running assertion: {str(e)}"},
            }
    
    def run_glossary_term_exists_assertion(self, assertion, client):
        """Run a glossary_term_exists assertion"""
        try:
            config = assertion.config
            term_name = config.get("term_name")
            
            if not term_name:
                return {
                    "success": False,
                    "details": {
                        "message": "Glossary term name not found in assertion configuration"
                    },
                }
            
            # Generate the term URN using consistent system
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            current_environment = getattr(current_connection, 'environment', 'dev')
            base_term_urn = get_full_urn_from_name("glossaryTerm", term_name)
            mutation_config = get_mutation_config_for_environment(current_environment)
            term_urn = generate_mutated_urn(base_term_urn, current_environment, "glossaryTerm", mutation_config)
            
            # Check if term exists in DataHub
            term = client.get_glossary_term(term_urn)
            
            exists = term is not None
            
            return {
                "success": exists,
                "details": {
                    "message": f"Glossary term {'exists' if exists else 'does not exist'} in DataHub",
                    "term_name": term_name,
                    "term_urn": term_urn,
                    "exists": exists,
                    "datahub_response": term if term else "Not found",
                },
            }
        except Exception as e:
            logger.error(f"Error running glossary_term_exists assertion: {str(e)}")
            return {
                "success": False,
                "details": {"message": f"Error running assertion: {str(e)}"},
            }


class AssertionDeleteView(View):
    """View to delete an assertion"""
    
    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def post(self, request, assertion_id):
        """Delete an assertion"""
        try:
            assertion = get_object_or_404(Assertion, id=assertion_id)
            
            # Delete the assertion
            assertion_name = assertion.name
            assertion.delete()
            
            messages.success(
                request, f"Assertion '{assertion_name}' deleted successfully"
            )
            return redirect("assertion_list")
        except Exception as e:
            logger.error(f"Error deleting assertion: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("assertion_list")


class AssertionListView(View):
    """View to list and create SQL assertions"""
    
    def get(self, request):
        """Display list of SQL assertions"""
        try:
            assertions = Assertion.objects.all().order_by("-updated_at")
            
            # Check connection to DataHub
            connected, client = test_datahub_connection(request)
            
            return render(
                request,
                "metadata_manager/assertions/list.html",
                {
                    "page_title": "SQL Assertions",
                    "assertions": assertions,
                    "has_datahub_connection": connected,
                },
            )
        except Exception as e:
            logger.error(f"Error in assertion list view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return render(
                request,
                "metadata_manager/assertions/list.html",
                {"page_title": "SQL Assertions", "error": str(e)},
            )
    
    def post(self, request):
        """Create a new SQL assertion"""
        try:
            name = request.POST.get("name")
            description = request.POST.get("description", "")
            assertion_type = request.POST.get("type", "SQL")
            
            # Get SQL query configuration
            database_platform = request.POST.get("database_platform")
            query = request.POST.get("query", "")
            expected_result = request.POST.get("expected_result", "SUCCESS")
            
            if not name:
                messages.error(request, "Assertion name is required")
                return redirect("metadata_manager:assertion_list")
                
            if not query:
                messages.error(request, "SQL query is required")
                return redirect("metadata_manager:assertion_list")
                
            # Create config JSON
            config = {
                "database_platform": database_platform,
                "query": query,
                "expected_result": expected_result,
            }
            
            # Create the assertion
            Assertion.objects.create(
                name=name, description=description, type=assertion_type, config=config
            )
            
            messages.success(request, f"SQL assertion '{name}' created successfully")
            return redirect("metadata_manager:assertion_list")
        except Exception as e:
            logger.error(f"Error creating SQL assertion: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:assertion_list")


class AssertionDetailView(View):
    """View to view, edit and run SQL assertions"""
    
    def get(self, request, assertion_id):
        """Display assertion details"""
        try:
            assertion = get_object_or_404(Assertion, id=assertion_id)
            
            # Get previous results
            results = AssertionResult.objects.filter(assertion=assertion).order_by(
                "-run_at"
            )[:10]
            
            # Check connection to DataHub
            connected, client = test_datahub_connection(request)
            
            return render(
                request,
                "metadata_manager/assertions/detail.html",
                {
                    "page_title": f"SQL Assertion: {assertion.name}",
                    "assertion": assertion,
                    "results": results,
                    "has_datahub_connection": connected,
                },
            )
        except Exception as e:
            logger.error(f"Error in assertion detail view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:assertion_list")
    
    def post(self, request, assertion_id):
        """Update SQL assertion"""
        try:
            assertion = get_object_or_404(Assertion, id=assertion_id)
            
            name = request.POST.get("name")
            description = request.POST.get("description", "")
            
            # Get SQL query configuration
            database_platform = request.POST.get("database_platform")
            query = request.POST.get("query", "")
            expected_result = request.POST.get("expected_result", "SUCCESS")
            
            if not name:
                messages.error(request, "Assertion name is required")
                return redirect(
                    "metadata_manager:assertion_detail", assertion_id=assertion_id
                )
                
            if not query:
                messages.error(request, "SQL query is required")
                return redirect(
                    "metadata_manager:assertion_detail", assertion_id=assertion_id
                )
                
            # Update config JSON
            config = {
                "database_platform": database_platform,
                "query": query,
                "expected_result": expected_result,
            }
            
            # Update the assertion
            assertion.name = name
            assertion.description = description
            assertion.config = config
            assertion.save()
            
            messages.success(request, f"SQL assertion '{name}' updated successfully")
            return redirect(
                "metadata_manager:assertion_detail", assertion_id=assertion_id
            )
        except Exception as e:
            logger.error(f"Error updating SQL assertion: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:assertion_list")
    
    def delete(self, request, assertion_id):
        """Delete assertion"""
        try:
            assertion = get_object_or_404(Assertion, id=assertion_id)
            assertion_name = assertion.name
            assertion.delete()
            return JsonResponse(
                {
                    "success": True,
                    "message": f"SQL assertion '{assertion_name}' deleted successfully",
                }
            )
        except Exception as e:
            logger.error(f"Error deleting SQL assertion: {str(e)}")
            return JsonResponse(
                {"success": False, "message": f"An error occurred: {str(e)}"}
            )


class AssertionRunView(View):
    """View to run a SQL assertion"""
    
    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def post(self, request, assertion_id):
        """Run a SQL assertion"""
        try:
            assertion = get_object_or_404(Assertion, id=assertion_id)
            
            # Check connection to DataHub
            connected, client = test_datahub_connection(request)
            if not connected or not client:
                return JsonResponse(
                    {"success": False, "message": "Not connected to DataHub"}
                )
            
            # Run the assertion through DataHub
            result = client.run_sql_assertion(
                name=assertion.name,
                platform=assertion.config.get("database_platform", ""),
                query=assertion.config.get("query", ""),
                expected_result=assertion.config.get("expected_result", "SUCCESS"),
            )
            
            if result:
                # Create a result record
                status = result.get("status", "UNKNOWN")
                details = result.get("details", {})
                
                AssertionResult.objects.create(
                    assertion=assertion, status=status, details=details
                )
                
                # Update assertion with last run info
                assertion.last_run = timezone.now()
                assertion.last_status = status
                assertion.save()
                
                return JsonResponse(
                    {
                        "success": True,
                        "status": status,
                        "details": details,
                        "message": f"SQL assertion ran with status: {status}",
                    }
                )
            else:
                return JsonResponse(
                    {"success": False, "message": "Failed to run SQL assertion"}
                )
        except Exception as e:
            logger.error(f"Error running SQL assertion: {str(e)}")
            return JsonResponse(
                {"success": False, "message": f"An error occurred: {str(e)}"}
            )


class AssertionGitPushView(View):
    """View to add an assertion to a GitHub PR"""
    
    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def post(self, request, assertion_id):
        """Add assertion to GitHub PR"""
        try:
            assertion = get_object_or_404(Assertion, id=assertion_id)
            
            # Check if Git integration is available
            if not GIT_INTEGRATION_AVAILABLE:
                return JsonResponse(
                    {"success": False, "message": "Git integration not available"}
                )

            # Check if GitHub integration is enabled
            github_settings = GitSettings.objects.first()
            if not github_settings or not github_settings.enabled:
                return JsonResponse(
                    {"success": False, "message": "GitHub integration not enabled"}
                )
            
            # Get environment from request (default to None if not provided)
            environment_id = request.POST.get("environment")
            environment = None
            if environment_id:
                try:
                    environment = Environment.objects.get(id=environment_id)
                    logger.info(f"Using environment '{environment.name}' for assertion")
                except Environment.DoesNotExist:
                    logger.warning(
                        f"Environment with ID {environment_id} not found, using default"
                    )
                    environment = Environment.get_default()
            else:
                environment = Environment.get_default()
                logger.info(
                    f"No environment specified, using default: {environment.name}"
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
            
            # Create a simple Assertion object that GitIntegration can handle
            class SQLAssertion:
                def __init__(self, assertion, environment):
                    self.id = assertion.id
                    self.name = assertion.name
                    self.description = assertion.description
                    self.type = assertion.type
                    self.config = assertion.config
                    self.environment = environment
                    self.content = (
                        self.to_yaml()
                    )  # GitIntegration expects a content attribute
                
                def to_dict(self):
                    # Return a dictionary representation of the assertion
                    return {
                        "id": str(self.id),
                        "name": self.name,
                        "description": self.description,
                        "type": self.type,
                        "config": self.config,
                    }
                
                def to_yaml(self):
                    # Return YAML representation of the assertion
                    return yaml.dump(self.to_dict(), default_flow_style=False)
            
            # Create the assertion object
            sql_assertion = SQLAssertion(assertion, environment)
            
            # Create commit message
            commit_message = f"Add/update SQL assertion: {assertion.name}"
            
            # Stage the assertion to the git repo
            logger.info(
                f"Staging assertion {assertion.id} to Git branch {current_branch}"
            )
            git_integration = GitIntegration()
            
            # Use GitIntegration to stage the changes
            result = git_integration.push_to_git(sql_assertion, commit_message)
            
            if result and result.get("success"):
                # Success response
                logger.info(
                    f"Successfully staged assertion {assertion.id} to Git branch {current_branch}"
                )
                return JsonResponse(
                    {
                        "success": True,
                        "message": f'SQL assertion "{assertion.name}" staged for commit to branch {current_branch}.',
                    }
                )
            else:
                # Failed to stage changes
                error_message = f'Failed to stage SQL assertion "{assertion.name}"'
                if isinstance(result, dict) and "error" in result:
                    error_message += f": {result['error']}"
                
                logger.error(f"Failed to stage assertion: {error_message}")
                return JsonResponse({"success": False, "error": error_message})
        except Exception as e:
            logger.error(f"Error adding assertion to GitHub PR: {str(e)}")
            return JsonResponse({"success": False, "message": f"Error: {str(e)}"})


def get_datahub_assertions(request):
    """
    Get assertions from DataHub and return them as JSON.

    Args:
        request: The HTTP request

    Returns:
        JsonResponse: The assertions in JSON format
    """
    try:
        # Get query parameters
        query = request.GET.get("query", "*")
        start = int(request.GET.get("start", 0))
        count = int(request.GET.get("count", 20))
        status = request.GET.get("status")
        entity_urn = request.GET.get("entity_urn")
        run_events_limit = int(request.GET.get("run_events_limit", 10))

        logger.info(
            f"Getting Assertions: query='{query}', start={start}, count={count}"
        )

        # Get client using standard configuration
        client = get_datahub_client_from_request(request)
        if not client:
            return JsonResponse(
                {"success": False, "error": "Not connected to DataHub"}, status=400
            )

        # Get assertions from DataHub
        result = client.get_assertions(
            start=start,
            count=count,
            query=query,
            status=status,
            entity_urn=entity_urn,
            run_events_limit=run_events_limit,
        )

        if not result.get("success", False):
            logger.error(
                f"Error getting assertions from DataHub: {result.get('error', 'Unknown error')}"
            )
            return JsonResponse(
                {
                    "success": False,
                    "error": result.get(
                        "error", "Failed to get assertions from DataHub"
                    ),
                },
                status=500,
            )

        # Structure the response
        response_data = {
            "start": result["data"].get("start", 0),
            "count": result["data"].get("count", 0),
            "total": result["data"].get("total", 0),
            "searchResults": result["data"].get("searchResults", []),
        }

        # Wrap in the expected structure for the frontend
        response = {"success": True, "data": response_data}

        logger.info(f"Found {len(response_data['searchResults'])} assertions")

        return JsonResponse(response)
    except Exception as e:
        logger.error(f"Error in get_datahub_assertions: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)}, status=500)


def get_remote_assertions_data(request):
    """AJAX endpoint to get enhanced remote assertions data with ownership and relationships"""
    try:
        logger.info("Loading enhanced remote assertions data via AJAX")

        # Get DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})

        # Get current connection to filter assertions by connection (consistent with tags/properties)
        from web_ui.views import get_current_connection
        current_connection = get_current_connection(request)
        logger.debug(f"Using connection: {current_connection.name if current_connection else 'None'}")

        # Get all local assertions
        local_assertions = Assertion.objects.all().order_by("name")

        # Initialize data structures
        synced_items = []
        local_only_items = []
        remote_only_items = []
        datahub_url = None

        try:
            logger.debug("Fetching enhanced remote assertions from DataHub")
            
            # Get DataHub URL for direct links
            datahub_url = client.server_url
            if datahub_url.endswith("/api/gms"):
                datahub_url = datahub_url[:-8]  # Remove /api/gms to get base URL

            # Get all remote assertions from DataHub with enhanced data
            result = client.get_assertions(start=0, count=1000, query="*")
            
            # Process remote assertions data
            enhanced_remote_assertions = {}
            
            if result.get("success", False):
                remote_assertions_data = result["data"].get("searchResults", [])
                logger.debug(f"Fetched {len(remote_assertions_data)} remote assertions")

                for assertion_result in remote_assertions_data:
                    assertion_data = assertion_result.get("entity", {})
                    if assertion_data:
                        assertion_urn = assertion_data.get("urn")
                        if assertion_urn:
                            # Extract basic properties
                            info = assertion_data.get("info", {})
                            
                            enhanced_assertion = {
                                "urn": assertion_urn,
                                "name": info.get("description", assertion_urn.split(":")[-1]),  # Use description as name or fallback to URN part
                                "description": info.get("description", ""),
                                "type": assertion_data.get("type", "Unknown"),
                                "sync_status": "REMOTE_ONLY",
                                "sync_status_display": "Remote Only",
                                
                                # Extract ownership data
                                "ownership": assertion_data.get("ownership"),
                                "owners_count": 0,
                                "owner_names": [],
                                
                                # Extract relationships data  
                                "relationships": assertion_data.get("relationships"),
                                "relationships_count": 0,
                                
                                # Extract assertion-specific data
                                "entity_urn": info.get("entity", {}).get("urn") if info.get("entity") else None,
                                "source": info.get("source", {}),
                                "last_updated": assertion_data.get("lastUpdated"),
                                
                                # Store raw data
                                "raw_data": assertion_data
                            }
                            
                            # Process ownership information
                            if enhanced_assertion["ownership"] and enhanced_assertion["ownership"].get("owners"):
                                owners = enhanced_assertion["ownership"]["owners"]
                                enhanced_assertion["owners_count"] = len(owners)
                                
                                # Extract owner names for display
                                owner_names = []
                                for owner_info in owners:
                                    owner = owner_info.get("owner", {})
                                    if owner.get("properties"):
                                        name = (
                                            owner["properties"].get("displayName") or
                                            owner.get("username") or
                                            owner.get("name") or
                                            "Unknown"
                                        )
                                    else:
                                        name = owner.get("username") or owner.get("name") or "Unknown"
                                    owner_names.append(name)
                                enhanced_assertion["owner_names"] = owner_names
                            
                            # Process relationships information
                            if enhanced_assertion["relationships"] and enhanced_assertion["relationships"].get("relationships"):
                                enhanced_assertion["relationships_count"] = len(enhanced_assertion["relationships"]["relationships"])
                            
                            enhanced_remote_assertions[assertion_urn] = enhanced_assertion

            # Extract assertion URNs that exist locally and map them
            local_assertion_urns = {}  # Map urn -> local assertion  
            local_assertion_ids = {}   # Map assertion_id -> local assertion
            
            try:
                for assertion in local_assertions:
                    # Map by URN if it exists (synced assertions)
                    if hasattr(assertion, 'urn') and assertion.urn:
                        local_assertion_urns[assertion.urn] = assertion
                    # Also map by ID for local-only assertions - handle UUID safely
                    try:
                        assertion_id = str(assertion.id)  # Convert to string to avoid UUID issues
                        local_assertion_ids[assertion_id] = assertion
                    except Exception as e:
                        logger.warning(f"Error processing assertion ID {assertion.id}: {str(e)}")
                        continue
            except Exception as e:
                logger.error(f"Error mapping local assertions: {str(e)}")
                # Fallback to treating all as remote-only
                local_assertion_urns = {}
                local_assertion_ids = {}

            # Process local assertions and categorize them - with error handling
            try:
                for assertion in local_assertions:
                    try:
                        # Create enhanced local assertion data
                        local_assertion_data = {
                            "id": str(assertion.id),  # Convert to string to avoid UUID issues
                            "urn": getattr(assertion, 'urn', None) or f"urn:li:assertion:local:{assertion.id}",
                            "name": assertion.name,
                            "description": assertion.description or "",
                            "type": assertion.assertion_type or assertion.type,
                            "sync_status": getattr(assertion, 'sync_status', 'LOCAL_ONLY'),
                            "sync_status_display": getattr(assertion, 'get_sync_status_display', lambda: 'Local Only')() if hasattr(assertion, 'get_sync_status_display') else 'Local Only',
                            
                            # Use stored ownership and relationship data
                            "ownership": getattr(assertion, 'ownership_data', None),
                            "owners_count": len(getattr(assertion, 'ownership_data', {}).get('owners', [])) if getattr(assertion, 'ownership_data', None) else 0,
                            "owner_names": [],
                            
                            "relationships": getattr(assertion, 'relationships_data', None),
                            "relationships_count": len(getattr(assertion, 'relationships_data', {}).get('relationships', [])) if getattr(assertion, 'relationships_data', None) else 0,
                            
                            # Assertion-specific data
                            "entity_urn": getattr(assertion, 'entity_urn', None),
                            "platform_name": getattr(assertion, 'platform_name', None),
                            "external_url": getattr(assertion, 'external_url', None),
                            "removed": getattr(assertion, 'removed', False),
                            
                            # Local metadata
                            "last_run": assertion.last_run.isoformat() if assertion.last_run else None,
                            "last_status": assertion.last_status,
                            "last_synced": assertion.last_synced.isoformat() if hasattr(assertion, 'last_synced') and assertion.last_synced else None,
                            "created_at": assertion.created_at.isoformat() if assertion.created_at else None,
                            "updated_at": assertion.updated_at.isoformat() if assertion.updated_at else None,
                        }

                        # Extract owner names for display from stored ownership data
                        ownership_data = getattr(assertion, 'ownership_data', None)
                        if ownership_data and ownership_data.get("owners"):
                            owner_names = []
                            for owner_info in ownership_data["owners"]:
                                owner = owner_info.get("owner", {})
                                if owner.get("properties"):
                                    name = (
                                        owner["properties"].get("displayName") or
                                        owner.get("username") or
                                        owner.get("name") or
                                        "Unknown"
                                    )
                                else:
                                    name = owner.get("username") or owner.get("name") or "Unknown"
                                owner_names.append(name)
                            local_assertion_data["owner_names"] = owner_names

                        # Determine connection context for this assertion
                        connection_context = "none"  # Default
                        if assertion.connection is None:
                            connection_context = "none"  # No connection
                        elif current_connection and assertion.connection == current_connection:
                            connection_context = "current"  # Current connection
                        else:
                            connection_context = "different"  # Different connection

                        # Categorize based on sync status AND connection context (like tags/properties)
                        sync_status = getattr(assertion, 'sync_status', 'LOCAL_ONLY')
                        assertion_urn = getattr(assertion, 'urn', None)
                        
                        # Check if this assertion has a remote match
                        remote_match = enhanced_remote_assertions.get(assertion_urn) if assertion_urn else None
                        
                        # Apply the same logic as tags/properties for proper categorization
                        if (sync_status == "SYNCED" and 
                            connection_context == "current" and 
                            current_connection):
                            # This is a synced assertion for the current connection
                            if remote_match:
                                # Found in remote results - perfect sync
                                synced_items.append({
                                    "local": local_assertion_data,
                                    "remote": remote_match,
                                    "combined": {
                                        **local_assertion_data,
                                        "sync_status": "SYNCED",
                                        "sync_status_display": "Synced",
                                        "connection_context": connection_context,
                                        "has_remote_match": True,
                                    }
                                })
                                # Remove from remote-only list since it's synced
                                del enhanced_remote_assertions[assertion_urn]
                            else:
                                # Synced but not found in current remote search (could be indexing delay)
                                synced_items.append({
                                    "local": local_assertion_data,
                                    "remote": None,  # Not found in current search
                                    "combined": {
                                        **local_assertion_data,
                                        "sync_status": "SYNCED",
                                        "sync_status_display": "Synced (Remote Pending Index)",
                                        "connection_context": connection_context,
                                        "has_remote_match": False,
                                    }
                                })
                        else:
                            # Local-only relative to current connection
                            # This includes: different connection, no connection, or not synced
                            local_assertion_data.update({
                                "connection_context": connection_context,
                                "has_remote_match": bool(remote_match),
                            })
                            local_only_items.append(local_assertion_data)
                    except Exception as e:
                        logger.error(f"Error processing assertion {assertion.name}: {str(e)}")
                        # Add to local-only with minimal data
                        local_only_items.append({
                            "id": str(assertion.id) if assertion.id else "unknown",
                            "name": assertion.name,
                            "description": assertion.description or "",
                            "type": assertion.type,
                            "sync_status": "LOCAL_ONLY",
                            "sync_status_display": "Local Only (Error)",
                            "error": str(e)
                        })
                        continue
            except Exception as e:
                logger.error(f"Error processing local assertions: {str(e)}")
                # Add empty results to avoid total failure
                local_only_items = []

            # Remaining remote assertions are remote-only
            remote_only_items = list(enhanced_remote_assertions.values())

            # Calculate statistics
            total_items = len(synced_items) + len(local_only_items) + len(remote_only_items)
            synced_count = len(synced_items)
            owned_items = sum(1 for item in synced_items + local_only_items + remote_only_items 
                            if (item.get("combined", item) if "combined" in item else item).get("owners_count", 0) > 0)
            items_with_relationships = sum(1 for item in synced_items + local_only_items + remote_only_items 
                                         if (item.get("combined", item) if "combined" in item else item).get("relationships_count", 0) > 0)

            statistics = {
                "total_items": total_items,
                "synced_count": synced_count,
                "owned_items": owned_items,
                "items_with_relationships": items_with_relationships,
            }

            logger.debug(
                f"Enhanced assertion categorization: {len(synced_items)} synced, "
                f"{len(local_only_items)} local-only, {len(remote_only_items)} remote-only"
            )

            return JsonResponse({
                "success": True,
                "data": {
                    "synced_items": synced_items,
                    "local_only_items": local_only_items,
                    "remote_only_items": remote_only_items,
                    "statistics": statistics,
                    "datahub_url": datahub_url,
                }
            })

        except Exception as e:
            logger.error(f"Error fetching enhanced remote assertion data: {str(e)}")
            return JsonResponse(
                {"success": False, "error": f"Error fetching remote assertions: {str(e)}"}
            )

    except Exception as e:
        logger.error(f"Error in get_remote_assertions_data: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_http_methods(["POST"])
def run_remote_assertion(request):
    """Run a remote assertion by URN"""
    try:
        import json
        data = json.loads(request.body)
        assertion_urn = data.get('assertion_urn')
        
        if not assertion_urn:
            return JsonResponse({"success": False, "error": "Assertion URN is required"})
        
        # Get DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})
        
        # Import the metadata API client for running assertions
        from utils.datahub_metadata_api import DataHubMetadataApiClient
        
        # Create metadata API client
        metadata_client = DataHubMetadataApiClient(
            server_url=client.server_url,
            token=client.token,
            verify_ssl=client.verify_ssl
        )
        
        # Run the assertion
        result = metadata_client.run_assertion(assertion_urn, save_result=True)
        
        if result:
            logger.info(f"Successfully ran remote assertion: {assertion_urn}")
            return JsonResponse({
                "success": True, 
                "message": "Assertion run successfully",
                "result": result
            })
        else:
            return JsonResponse({"success": False, "error": "Failed to run assertion"})
            
    except Exception as e:
        logger.error(f"Error running remote assertion: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_http_methods(["POST"])
def sync_assertion_to_local(request):
    """Sync a remote assertion to local storage with comprehensive data"""
    try:
        import json
        data = json.loads(request.body)
        assertion_urn = data.get('assertion_urn')
        
        if not assertion_urn:
            return JsonResponse({"success": False, "error": "Assertion URN is required"})
        
        # Get DataHub connection and current connection context
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})
        
        # Get current connection context - essential for proper sync status determination
        from web_ui.views import get_current_connection
        current_connection = None
        try:
            current_connection = get_current_connection(request)
        except Exception as e:
            logger.warning(f"Could not get current connection: {str(e)}")
        
        # Get the assertion data from DataHub
        result = client.get_assertions(query=f'urn:"{assertion_urn}"', count=1)
        
        if not result.get("success") or not result["data"].get("searchResults"):
            return JsonResponse({"success": False, "error": "Assertion not found in DataHub"})
        
        assertion_data = result["data"]["searchResults"][0]["entity"]
        
        # Defensive check - ensure assertion_data is not None
        if not assertion_data or not isinstance(assertion_data, dict):
            return JsonResponse({"success": False, "error": "Invalid assertion data received from DataHub"})
        
        info = assertion_data.get("info", {})
        if not isinstance(info, dict):
            info = {}
        
        # Ensure we have a valid name - never NULL or empty
        description = info.get("description") or ""  # Handle None case
        assertion_name = description.strip() if description else ""
        if not assertion_name:
            # Fallback to URN-based name
            urn_parts = assertion_urn.split(":")
            assertion_name = f"Assertion_{urn_parts[-1]}" if urn_parts else "Unknown_Assertion"
        
        # Extract entity URN from various assertion types
        entity_urn = None
        dataset_assertion = info.get("datasetAssertion")
        if dataset_assertion and isinstance(dataset_assertion, dict) and "datasetUrn" in dataset_assertion:
            entity_urn = dataset_assertion["datasetUrn"]
        else:
            freshness_assertion = info.get("freshnessAssertion")
            if freshness_assertion and isinstance(freshness_assertion, dict) and "entityUrn" in freshness_assertion:
                entity_urn = freshness_assertion["entityUrn"]
            else:
                sql_assertion = info.get("sqlAssertion")
                if sql_assertion and isinstance(sql_assertion, dict) and "entityUrn" in sql_assertion:
                    entity_urn = sql_assertion["entityUrn"]
                else:
                    field_assertion = info.get("fieldAssertion")
                    if field_assertion and isinstance(field_assertion, dict) and "entityUrn" in field_assertion:
                        entity_urn = field_assertion["entityUrn"]
                    else:
                        volume_assertion = info.get("volumeAssertion")
                        if volume_assertion and isinstance(volume_assertion, dict) and "entityUrn" in volume_assertion:
                            entity_urn = volume_assertion["entityUrn"]
                        else:
                            schema_assertion = info.get("schemaAssertion")
                            if schema_assertion and isinstance(schema_assertion, dict) and "entityUrn" in schema_assertion:
                                entity_urn = schema_assertion["entityUrn"]
                            else:
                                custom_assertion = info.get("customAssertion")
                                if custom_assertion and isinstance(custom_assertion, dict) and "entityUrn" in custom_assertion:
                                    entity_urn = custom_assertion["entityUrn"]
        
        # Extract platform instance and browse path from entity URN if available
        platform_instance = None
        browse_path = None
        if entity_urn:
            try:
                # Get entity details to extract platform instance and browse path
                entity_details_result = client.get_datasets_by_urns([entity_urn])
                if entity_details_result.get("success") and entity_details_result.get("data", {}).get("searchResults"):
                    entity_data = entity_details_result["data"]["searchResults"][0].get("entity", {})
                    
                    # Extract platform instance
                    platform_instance_data = entity_data.get("dataPlatformInstance", {})
                    if platform_instance_data and platform_instance_data.get("properties"):
                        platform_instance = platform_instance_data["properties"].get("name")
                    
                    # Extract browse path
                    browse_path = entity_data.get("computed_browse_path", "")
                    
                    logger.debug(f"Extracted platform instance: {platform_instance}, browse path: {browse_path} for entity: {entity_urn}")
            except Exception as e:
                logger.warning(f"Failed to extract platform instance and browse path for entity {entity_urn}: {str(e)}")
        
        # Extract assertion type and handle $UNKNOWN enum case
        assertion_type = info.get("type", "UNKNOWN")
        
        # Handle the $UNKNOWN enum case from DataHub GraphQL
        if assertion_type == "$UNKNOWN" or assertion_type == "UNKNOWN" or not assertion_type:
            # Try to determine type from the info structure
            if info.get("datasetAssertion"):
                assertion_type = "DATASET"
            elif info.get("freshnessAssertion"):
                assertion_type = "FRESHNESS"
            elif info.get("sqlAssertion"):
                assertion_type = "SQL"
            elif info.get("fieldAssertion"):
                assertion_type = "FIELD"
            elif info.get("volumeAssertion"):
                assertion_type = "VOLUME"
            elif info.get("schemaAssertion"):
                assertion_type = "SCHEMA"
            elif info.get("customAssertion"):
                assertion_type = "CUSTOM"
            else:
                # Fallback to SQL if we can't determine type
                assertion_type = "SQL"
                logger.warning(f"Could not determine assertion type for {assertion_urn}, defaulting to SQL")
        
        # Clean up any remaining enum artifacts
        if assertion_type.startswith("$"):
            assertion_type = "SQL"  # Safe fallback
        
        # Extract additional comprehensive data from assertion
        run_events_data = assertion_data.get("runEvents", {})
        monitor_data = assertion_data.get("monitor", {})
        tags_data = assertion_data.get("tags", {})
        
        # Extract schedule information from monitor data
        schedule_info = None
        if monitor_data and isinstance(monitor_data, dict):
            schedule_info = monitor_data.get("schedule", {})
        
        # Extract latest run status from run events
        latest_run_status = None
        if run_events_data and isinstance(run_events_data, dict):
            run_events = run_events_data.get("runEvents", [])
            if run_events and len(run_events) > 0:
                latest_run = run_events[0]  # Assume first is most recent
                latest_run_status = latest_run.get("result", {}).get("type")
        
        logger.debug(f"Extracted comprehensive data for {assertion_urn}: type={assertion_type}, "
                   f"schedule={bool(schedule_info)}, latest_status={latest_run_status}, "
                   f"run_events={len(run_events_data.get('runEvents', []) if run_events_data else [])}, "
                   f"tags={len(tags_data.get('tags', []) if tags_data else [])}")
        
        # Extract platform information
        platform_name = None
        platform_data = assertion_data.get("platform")
        if platform_data and isinstance(platform_data, dict):
            platform_name = platform_data.get("name")
        
        # Extract ownership data and count
        ownership_data = assertion_data.get("ownership")
        owners_count = 0
        if ownership_data and isinstance(ownership_data, dict) and ownership_data.get("owners"):
            try:
                owners_count = len(ownership_data["owners"])
            except (TypeError, KeyError):
                owners_count = 0
        
        # Extract relationships data and count
        relationships_data = assertion_data.get("relationships")
        relationships_count = 0
        if relationships_data and isinstance(relationships_data, dict) and relationships_data.get("relationships"):
            try:
                relationships_count = len(relationships_data["relationships"])
            except (TypeError, KeyError):
                relationships_count = 0
        
        # Generate deterministic URN for local storage
        safe_name = assertion_name.lower().replace(' ', '_').replace('-', '_')
        deterministic_urn = f"urn:li:assertion:synced_{safe_name}_{assertion_urn.split(':')[-1]}"
        
        # Extract status safely
        status_data = assertion_data.get("status")
        removed = False
        if status_data and isinstance(status_data, dict):
            removed = status_data.get("removed", False)
        
        # Check if assertion already exists (by urn)
        existing_assertion = None
        try:
            existing_assertion = Assertion.objects.get(urn=deterministic_urn)
        except Assertion.DoesNotExist:
            # Check if we have an assertion with this urn already
            try:
                existing_assertion = Assertion.objects.get(urn=assertion_urn)
            except Assertion.DoesNotExist:
                pass
        
        if existing_assertion:
            # Update existing assertion
            existing_assertion.name = assertion_name
            existing_assertion.description = description
            existing_assertion.type = assertion_type  # Legacy field
            existing_assertion.assertion_type = assertion_type
            existing_assertion.urn = assertion_urn  # Use the actual DataHub URN
            existing_assertion.entity_urn = entity_urn
            existing_assertion.platform_name = platform_name
            existing_assertion.platform_instance = platform_instance  # Store extracted platform instance
            existing_assertion.external_url = info.get("externalUrl") if isinstance(info, dict) else None
            existing_assertion.removed = removed
            existing_assertion.sync_status = "SYNCED"
            existing_assertion.last_synced = timezone.now()
            
            # Update connection tracking - essential for proper sync/local determination
            existing_assertion.connection = current_connection
            
            # Update comprehensive data with all extracted information
            existing_assertion.info_data = info
            existing_assertion.ownership_data = ownership_data
            existing_assertion.relationships_data = relationships_data
            existing_assertion.run_events_data = run_events_data
            existing_assertion.tags_data = tags_data
            existing_assertion.monitor_data = monitor_data
            
            # Update legacy fields for backward compatibility 
            if latest_run_status:
                existing_assertion.last_status = latest_run_status
            
            # Update config
            existing_assertion.config = existing_assertion.config or {}
            existing_assertion.config.update({
                "synced_from_datahub": True,
                "datahub_urn": assertion_urn,
                "raw_data": assertion_data
            })
            
            existing_assertion.save()
            assertion = existing_assertion
            action = "updated"
        else:
            # Create new assertion from remote data with comprehensive fields
            assertion = Assertion.objects.create(
                name=assertion_name,
                description=description,
                type=assertion_type,  # Legacy field
                assertion_type=assertion_type,
                config={
                    "synced_from_datahub": True,
                    "datahub_urn": assertion_urn,
                    "raw_data": assertion_data
                },
                
                # URN tracking
                urn=assertion_urn,  # Use the actual DataHub URN
                
                # Entity and platform info
                entity_urn=entity_urn,
                platform_name=platform_name,
                platform_instance=platform_instance,  # Store extracted platform instance
                external_url=info.get("externalUrl") if isinstance(info, dict) else None,
                
                # Status
                removed=removed,
                sync_status="SYNCED",  # Mark as synced since we just synced it
                last_synced=timezone.now(),
                
                # Connection tracking - essential for proper sync/local determination
                connection=current_connection,
                
                # Comprehensive data storage - using extracted variables
                info_data=info,
                ownership_data=ownership_data,
                relationships_data=relationships_data,
                run_events_data=run_events_data,
                tags_data=tags_data,
                monitor_data=monitor_data,
                
                # Legacy fields for backward compatibility
                last_status=latest_run_status,
            )
            action = "created"
        
        logger.info(f"Successfully {action} assertion: {assertion_urn}")
        return JsonResponse({
            "success": True,
            "message": f"Assertion '{assertion_name}' {action} successfully",
            "assertion_id": assertion.id,
            "sync_status": assertion.sync_status,
            "action": action
        })
        
    except Exception as e:
        logger.error(f"Error syncing assertion to local: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_http_methods(["POST"])
def push_assertion_to_datahub(request, assertion_id):
    """Push a local assertion to DataHub"""
    try:
        assertion = get_object_or_404(Assertion, id=assertion_id)
        
        # Get DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})
        
        # Import the metadata API client
        from utils.datahub_metadata_api import DataHubMetadataApiClient
        
        # Create metadata API client
        metadata_client = DataHubMetadataApiClient(
            server_url=client.server_url,
            token=client.token,
            verify_ssl=client.verify_ssl
        )
        
        # Convert local assertion to DataHub format and create
        # Handle different assertion types
        assertion_type = assertion.type.lower() if assertion.type else "unknown"
        config = assertion.config or {}
        result_urn = None
        
        if assertion_type == "sql":
            dataset_urn = config.get("dataset_urn", "urn:li:dataset:(urn:li:dataPlatform:unknown,unknown,PROD)")
            sql_statement = config.get("query", "SELECT 1")
            
            result_urn = metadata_client.create_sql_assertion(
                dataset_urn=dataset_urn,
                sql_statement=sql_statement,
                operator="EQUAL_TO",
                value="1",
                description=assertion.description or assertion.name
            )
            
        elif assertion_type == "freshness":
            dataset_urn = config.get("dataset_urn", "urn:li:dataset:(urn:li:dataPlatform:unknown,unknown,PROD)")
            schedule_interval = config.get("schedule_interval", 24)
            schedule_unit = config.get("schedule_unit", "HOUR")
            assertion_timezone = config.get("timezone", "UTC")
            cron = config.get("cron_expression", "0 0 * * *")
            source_type = config.get("source_type", "INFORMATION_SCHEMA")
            
            result_urn = metadata_client.create_freshness_assertion(
                dataset_urn=dataset_urn,
                schedule_interval=schedule_interval,
                schedule_unit=schedule_unit,
                timezone=assertion_timezone,
                cron=cron,
                source_type=source_type,
                description=assertion.description or assertion.name
            )
            
        else:
            return JsonResponse({"success": False, "error": f"Pushing {assertion_type} assertions is not yet implemented"})
        
        if result_urn:
            # Update local assertion with DataHub URN and connection
            assertion.config = assertion.config or {}
            assertion.config["datahub_urn"] = result_urn
            assertion.sync_status = "SYNCED"
            assertion.last_synced = timezone.now()
            assertion.urn = result_urn  # Update URN to match DataHub URN
            
            # Set connection for proper categorization
            from web_ui.views import get_current_connection
            try:
                current_connection = get_current_connection(request)
                assertion.connection = current_connection
            except Exception as e:
                logger.warning(f"Could not get current connection: {str(e)}")
                
            assertion.save()
            
            logger.info(f"Successfully pushed {assertion_type} assertion to DataHub: {assertion.id} -> {result_urn}")
            return JsonResponse({
                "success": True,
                "message": f"{assertion_type.title()} assertion pushed to DataHub successfully",
                "datahub_urn": result_urn
            })
        else:
            return JsonResponse({"success": False, "error": f"Failed to create {assertion_type} assertion in DataHub"})
            
    except Exception as e:
        logger.error(f"Error pushing assertion to DataHub: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_http_methods(["POST"])
def resync_assertion(request, assertion_id):
    """Resync a local assertion with its DataHub counterpart using comprehensive data"""
    try:
        import json
        data = json.loads(request.body)
        assertion_urn = data.get('assertion_urn')
        
        assertion = get_object_or_404(Assertion, id=assertion_id)
        
        if not assertion_urn:
            return JsonResponse({"success": False, "error": "Assertion URN is required"})
        
        # Get DataHub connection and current connection context
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})
        
        # Get current connection context for proper sync handling
        from web_ui.views import get_current_connection
        current_connection = get_current_connection(request)
        
        # Get the latest assertion data from DataHub
        result = client.get_assertions(query=f'urn:"{assertion_urn}"', count=1)
        
        if not result.get("success") or not result["data"].get("searchResults"):
            return JsonResponse({"success": False, "error": "Assertion not found in DataHub"})
        
        assertion_data = result["data"]["searchResults"][0]["entity"]
        
        # Defensive check - ensure assertion_data is not None
        if not assertion_data or not isinstance(assertion_data, dict):
            return JsonResponse({"success": False, "error": "Invalid assertion data received from DataHub"})
        
        info = assertion_data.get("info", {})
        if not isinstance(info, dict):
            info = {}
        
        
        # Update local assertion with latest remote data - ensure name is never empty
        description = info.get("description") or ""  # Handle None case
        new_name = description.strip() if description else ""
        if new_name:  # Only update name if we have a valid non-empty name
            assertion.name = new_name
        
        # Update basic fields
        assertion.description = description or assertion.description
        
        # Extract and update entity URN
        entity_urn = None
        dataset_assertion = info.get("datasetAssertion")
        if dataset_assertion and isinstance(dataset_assertion, dict) and "datasetUrn" in dataset_assertion:
            entity_urn = dataset_assertion["datasetUrn"]
        else:
            freshness_assertion = info.get("freshnessAssertion")
            if freshness_assertion and isinstance(freshness_assertion, dict) and "entityUrn" in freshness_assertion:
                entity_urn = freshness_assertion["entityUrn"]
            else:
                sql_assertion = info.get("sqlAssertion")
                if sql_assertion and isinstance(sql_assertion, dict) and "entityUrn" in sql_assertion:
                    entity_urn = sql_assertion["entityUrn"]
                else:
                    field_assertion = info.get("fieldAssertion")
                    if field_assertion and isinstance(field_assertion, dict) and "entityUrn" in field_assertion:
                        entity_urn = field_assertion["entityUrn"]
                    else:
                        volume_assertion = info.get("volumeAssertion")
                        if volume_assertion and isinstance(volume_assertion, dict) and "entityUrn" in volume_assertion:
                            entity_urn = volume_assertion["entityUrn"]
                        else:
                            schema_assertion = info.get("schemaAssertion")
                            if schema_assertion and isinstance(schema_assertion, dict) and "entityUrn" in schema_assertion:
                                entity_urn = schema_assertion["entityUrn"]
                            else:
                                custom_assertion = info.get("customAssertion")
                                if custom_assertion and isinstance(custom_assertion, dict) and "entityUrn" in custom_assertion:
                                    entity_urn = custom_assertion["entityUrn"]
        
        # Update assertion type and handle $UNKNOWN enum case
        assertion_type = info.get("type", "UNKNOWN")
        
        # Handle the $UNKNOWN enum case from DataHub GraphQL
        if assertion_type == "$UNKNOWN" or assertion_type == "UNKNOWN" or not assertion_type:
            # Try to determine type from the info structure
            if info.get("datasetAssertion"):
                assertion_type = "DATASET"
            elif info.get("freshnessAssertion"):
                assertion_type = "FRESHNESS"
            elif info.get("sqlAssertion"):
                assertion_type = "SQL"
            elif info.get("fieldAssertion"):
                assertion_type = "FIELD"
            elif info.get("volumeAssertion"):
                assertion_type = "VOLUME"
            elif info.get("schemaAssertion"):
                assertion_type = "SCHEMA"
            elif info.get("customAssertion"):
                assertion_type = "CUSTOM"
            else:
                # Fallback to SQL if we can't determine type
                assertion_type = "SQL"
                logger.warning(f"Could not determine assertion type for {assertion_urn}, defaulting to SQL")
        
        # Clean up any remaining enum artifacts
        if assertion_type.startswith("$"):
            assertion_type = "SQL"  # Safe fallback
        
        # Update platform information
        platform_name = None
        platform_data = assertion_data.get("platform")
        if platform_data and isinstance(platform_data, dict):
            platform_name = platform_data.get("name")
        
        # Update ownership data and count
        ownership_data = assertion_data.get("ownership")
        owners_count = 0
        if ownership_data and isinstance(ownership_data, dict) and ownership_data.get("owners"):
            try:
                owners_count = len(ownership_data["owners"])
            except (TypeError, KeyError):
                owners_count = 0
        
        # Update relationships data and count
        relationships_data = assertion_data.get("relationships")
        relationships_count = 0
        if relationships_data and isinstance(relationships_data, dict) and relationships_data.get("relationships"):
            try:
                relationships_count = len(relationships_data["relationships"])
            except (TypeError, KeyError):
                relationships_count = 0
        
        # Extract status safely
        status_data = assertion_data.get("status")
        removed = False
        if status_data and isinstance(status_data, dict):
            removed = status_data.get("removed", False)
        
        # Update all fields
        assertion.type = assertion_type  # Legacy field
        assertion.assertion_type = assertion_type
        assertion.urn = assertion_urn  # Use the actual DataHub URN
        assertion.entity_urn = entity_urn
        assertion.platform_name = platform_name
        assertion.external_url = info.get("externalUrl") if isinstance(info, dict) else None
        assertion.removed = removed
        
        # Extract comprehensive data like in sync_assertion_to_local
        run_events_data = assertion_data.get("runEvents", {})
        monitor_data = assertion_data.get("monitor", {})
        tags_data = assertion_data.get("tags", {})
        
        # Extract latest run status from run events
        latest_run_status = None
        if run_events_data and isinstance(run_events_data, dict):
            run_events = run_events_data.get("runEvents", [])
            if run_events and len(run_events) > 0:
                latest_run = run_events[0]  # Assume first is most recent
                latest_run_status = latest_run.get("result", {}).get("type")
        
        # Update comprehensive data with all extracted information
        assertion.info_data = info
        assertion.ownership_data = ownership_data
        assertion.relationships_data = relationships_data
        assertion.run_events_data = run_events_data
        assertion.tags_data = tags_data
        assertion.monitor_data = monitor_data
        
        # Update connection tracking - essential for proper sync/local determination
        assertion.connection = current_connection
        
        # Update config and sync status
        assertion.config = assertion.config or {}
        assertion.config["raw_data"] = assertion_data
        assertion.config["last_synced"] = timezone.now().isoformat()
        assertion.sync_status = "SYNCED"
        assertion.last_synced = timezone.now()
        
        # Update legacy fields for backward compatibility
        if latest_run_status:
            assertion.last_status = latest_run_status
        
        assertion.save()
        
        logger.info(f"Successfully resynced assertion: {assertion_id} with {assertion_urn}")
        return JsonResponse({
            "success": True,
            "message": "Assertion resynced successfully",
            "sync_status": "SYNCED"
        })
        
    except Exception as e:
        logger.error(f"Error resyncing assertion: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_http_methods(["DELETE"])
def delete_local_assertion(request, assertion_id):
    """Delete a local assertion (local storage only, not DataHub)"""
    try:
        assertion = get_object_or_404(Assertion, id=assertion_id)
        assertion_name = assertion.name
        
        # Delete the local assertion
        assertion.delete()
        
        logger.info(f"Successfully deleted local assertion: {assertion_id} ({assertion_name})")
        return JsonResponse({
            "success": True,
            "message": f"Local assertion '{assertion_name}' deleted successfully"
        })
        
    except Exception as e:
        logger.error(f"Error deleting local assertion: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_http_methods(["POST"])
def create_datahub_assertion(request):
    """Create DataHub assertion with comprehensive type support"""
    try:
        # Get basic info
        name = request.POST.get("name")
        description = request.POST.get("description", "")
        assertion_type = request.POST.get("type")
        dataset_urn = request.POST.get("dataset_urn")
        timezone = request.POST.get("timezone", "America/Los_Angeles")
        cron_expression = request.POST.get("cron_expression", "0 */8 * * *")
        
        if not all([name, assertion_type, dataset_urn]):
            return JsonResponse({
                "success": False,
                "error": "Name, assertion type, and dataset URN are required"
            })
        
        # Get DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({
                "success": False,
                "error": "DataHub connection not available"
            })
        
        # Import the metadata API client
        from utils.datahub_metadata_api import DataHubMetadataApiClient
        
        # Create metadata API client
        metadata_client = DataHubMetadataApiClient(
            server_url=client.server_url,
            token=client.token,
            verify_ssl=client.verify_ssl
        )
        
        assertion_urn = None
        
        # Create assertion based on type
        if assertion_type == "FIELD":
            field_path = request.POST.get("field_path")
            field_type = request.POST.get("field_type", "NUMBER")
            native_type = request.POST.get("native_type", "")
            field_operator = request.POST.get("field_operator")
            field_value = request.POST.get("field_value")
            fail_threshold = int(request.POST.get("fail_threshold", 0))
            
            if not all([field_path, field_operator]):
                return JsonResponse({
                    "success": False,
                    "error": "Field path and operator are required for field assertions"
                })
            
            assertion_urn = metadata_client.create_field_assertion(
                dataset_urn=dataset_urn,
                field_path=field_path,
                field_type=field_type,
                native_type=native_type,
                operator=field_operator,
                value=field_value or "0",
                fail_threshold=fail_threshold,
                timezone=timezone,
                cron=cron_expression,
                description=description
            )
            
        elif assertion_type == "SQL":
            sql_statement = request.POST.get("sql_statement")
            sql_operator = request.POST.get("sql_operator", "EQUAL_TO")
            sql_value = request.POST.get("sql_value", "1")
            
            if not sql_statement:
                return JsonResponse({
                    "success": False,
                    "error": "SQL statement is required for SQL assertions"
                })
            
            assertion_urn = metadata_client.create_sql_assertion(
                dataset_urn=dataset_urn,
                sql_statement=sql_statement,
                operator=sql_operator,
                value=sql_value,
                timezone=timezone,
                cron=cron_expression,
                description=description
            )
            
        elif assertion_type == "VOLUME":
            volume_operator = request.POST.get("volume_operator")
            volume_value = request.POST.get("volume_value")
            volume_max_value = request.POST.get("volume_max_value")
            
            if not volume_operator:
                return JsonResponse({
                    "success": False,
                    "error": "Volume operator is required for volume assertions"
                })
            
            kwargs = {
                "dataset_urn": dataset_urn,
                "operator": volume_operator,
                "timezone": timezone,
                "cron": cron_expression,
                "description": description
            }
            
            if volume_operator == "BETWEEN":
                kwargs["min_value"] = volume_value
                kwargs["max_value"] = volume_max_value
            else:
                kwargs["value"] = volume_value
            
            assertion_urn = metadata_client.create_volume_assertion(**kwargs)
            
        elif assertion_type == "FRESHNESS":
            freshness_interval = int(request.POST.get("freshness_interval", 24))
            freshness_unit = request.POST.get("freshness_unit", "HOUR")
            
            assertion_urn = metadata_client.create_freshness_assertion(
                dataset_urn=dataset_urn,
                schedule_interval=freshness_interval,
                schedule_unit=freshness_unit,
                timezone=timezone,
                cron=cron_expression,
                description=description
            )
            
        elif assertion_type == "SCHEMA":
            schema_compatibility = request.POST.get("schema_compatibility", "EXACT_MATCH")
            expected_fields = request.POST.get("expected_fields", "")
            
            if not expected_fields:
                return JsonResponse({
                    "success": False,
                    "error": "Expected fields are required for schema assertions"
                })
            
            # Parse expected fields
            fields = []
            for line in expected_fields.strip().split('\n'):
                if ':' in line:
                    field_name, field_type = line.strip().split(':', 1)
                    fields.append({
                        "path": field_name.strip(),
                        "type": field_type.strip()
                    })
            
            assertion_urn = metadata_client.create_schema_assertion(
                dataset_urn=dataset_urn,
                fields=fields,
                compatibility=schema_compatibility,
                description=description
            )
            
        elif assertion_type == "CUSTOM":
            custom_logic = request.POST.get("custom_logic")
            
            if not custom_logic:
                return JsonResponse({
                    "success": False,
                    "error": "Custom logic is required for custom assertions"
                })
            
            # For custom assertions, we'll create a local record for now
            # DataHub's custom assertion creation may need specific implementation
            assertion = Assertion.objects.create(
                name=name,
                description=description,
                type="CUSTOM",
                config={
                    "dataset_urn": dataset_urn,
                    "custom_logic": custom_logic,
                    "timezone": timezone,
                    "cron_expression": cron_expression
                }
            )
            
            return JsonResponse({
                "success": True,
                "message": f"Custom assertion '{name}' created locally",
                "assertion_id": assertion.id
            })
        
        else:
            return JsonResponse({
                "success": False,
                "error": f"Unsupported assertion type: {assertion_type}"
            })
        
        if assertion_urn:
            # Create local record of the assertion
            assertion = Assertion.objects.create(
                name=name,
                description=description,
                type=assertion_type,
                config={
                    "dataset_urn": dataset_urn,
                    "datahub_urn": assertion_urn,
                    "timezone": timezone,
                    "cron_expression": cron_expression,
                    **dict(request.POST.items())  # Store all form data
                }
            )
            
            return JsonResponse({
                "success": True,
                "message": f"{assertion_type} assertion '{name}' created successfully in DataHub",
                "assertion_urn": assertion_urn,
                "assertion_id": assertion.id
            })
        else:
            return JsonResponse({
                "success": False,
                "error": f"Failed to create {assertion_type} assertion in DataHub"
            })
            
    except Exception as e:
        logger.error(f"Error creating DataHub assertion: {str(e)}")
        return JsonResponse({
            "success": False,
            "error": str(e)
        })


@require_http_methods(["POST"])
def add_assertion_to_pr(request, assertion_id):
    """Add an assertion to a GitHub PR using the same pattern as recipes and policies"""
    try:
        from web_ui.models import GitIntegration, GitSettings
        import json
        import os
        from pathlib import Path
        from django.conf import settings
        from web_ui.models import Environment as WebUIEnvironment
        
        assertion = get_object_or_404(Assertion, id=assertion_id)
        
        # Check if git integration is enabled
        if not GitIntegration.is_configured():
            return JsonResponse({
                "success": False, 
                "error": "Git integration is not configured. Please configure GitHub settings first."
            })
        
        # Get current branch
        git_settings = GitSettings.get_instance()
        current_branch = git_settings.current_branch or "main"
        
        # Prevent pushing directly to main/master branch
        if current_branch.lower() in ["main", "master"]:
            logger.warning(f"Attempted to push directly to {current_branch} branch")
            return JsonResponse({
                "success": False,
                "error": "Cannot push directly to the main/master branch. Please create and use a feature branch from the Git Repository tab."
            })
        
        # Get environment name - prioritize database environment with ENVIRONMENT variable as override
        environment_name = os.getenv('ENVIRONMENT')
        
        if not environment_name:
            # Use database environment (prioritize web_ui environment over metadata_manager)
            environment = None
            try:
                # Try web_ui environment first (this has the correct "dev" environment)
                environment = WebUIEnvironment.objects.filter(is_default=True).first()
                if not environment:
                    environment = WebUIEnvironment.objects.first()
                logger.info(f"Using web_ui environment: {environment.name if environment else None}")
            except Exception as e:
                logger.info(f"web_ui environment lookup failed: {e}")
                environment_name = "dev"  # Final fallback
            
            if environment:
                environment_name = environment.name.lower().replace(" ", "-")
                logger.info(f"Using database environment: {environment.name} -> {environment_name}")
            else:
                environment_name = "dev"  # Final fallback
                logger.info("No database environment found, using 'dev' fallback")
        else:
            logger.info(f"Using ENVIRONMENT variable override: {environment_name}")
        
        # Normalize environment name (remove spaces, lowercase)
        environment_name = environment_name.lower().replace(" ", "-")
        logger.info(f"Final normalized environment name: {environment_name}")
        
        # Create an assertion-like object that GitIntegration can handle
        # This mimics how policies create a Policy object for GitIntegration
        class AssertionForGit:
            def __init__(self, assertion, environment):
                self.id = assertion.id
                self.name = assertion.name
                self.description = assertion.description
                # Fix assertion type resolution - treat 'UNKNOWN' as invalid and fall back to legacy field
                assertion_type = assertion.assertion_type
                if not assertion_type or assertion_type == "UNKNOWN":
                    assertion_type = assertion.config.get("type") or assertion.type or "SQL"
                self.assertion_type = assertion_type
                self.config = assertion.config
                self.environment = environment
                self.sync_status = assertion.sync_status
                
            def to_dict(self):
                """Convert assertion to DataHub GraphQL format for file output"""
                # Determine operation based on sync status
                operation = "create"  # Default for local assertions
                if self.sync_status == "SYNCED" or self.config.get("synced_from_datahub"):
                    operation = "update"
                
                # Use the assertion_type that was properly resolved in __init__
                assertion_type = self.assertion_type.upper() if self.assertion_type else "UNKNOWN"
                
                assertion_data = {
                    "operation": operation,
                    "assertion_type": assertion_type,
                    "name": self.name,
                    "description": self.description,
                    "config": self.config,
                    "local_id": str(self.id),
                    # Add unique filename for this assertion to prevent overwrites
                    "filename": f"{operation}_{assertion_type}_{self.id}_{self.name.lower().replace(' ', '_')}.json"
                }
                
                # Add specific GraphQL input based on assertion type
                if assertion_type == "FIELD":
                    assertion_data["graphql_input"] = {
                        "mutation": "createFieldAssertion" if operation == "create" else "upsertDatasetFieldAssertionMonitor",
                        "input": generate_field_assertion_input(assertion)
                    }
                elif assertion_type == "SQL":
                    assertion_data["graphql_input"] = {
                        "mutation": "createSqlAssertion" if operation == "create" else "upsertDatasetSqlAssertionMonitor", 
                        "input": generate_sql_assertion_input(assertion)
                    }
                elif assertion_type == "VOLUME":
                    assertion_data["graphql_input"] = {
                        "mutation": "createVolumeAssertion" if operation == "create" else "upsertDatasetVolumeAssertionMonitor",
                        "input": generate_volume_assertion_input(assertion)
                    }
                elif assertion_type == "FRESHNESS":
                    assertion_data["graphql_input"] = {
                        "mutation": "createFreshnessAssertion" if operation == "create" else "upsertDatasetFreshnessAssertionMonitor",
                        "input": generate_freshness_assertion_input(assertion)
                    }
                elif assertion_type == "DATASET":
                    assertion_data["graphql_input"] = {
                        "mutation": "createDatasetAssertion" if operation == "create" else "updateDatasetAssertion",
                        "input": generate_dataset_assertion_input(assertion)
                    }
                elif assertion_type == "SCHEMA":
                    assertion_data["graphql_input"] = {
                        "mutation": "upsertDatasetSchemaAssertionMonitor",
                        "input": generate_schema_assertion_input(assertion)
                    }
                elif assertion_type == "CUSTOM":
                    assertion_data["graphql_input"] = {
                        "mutation": "upsertCustomAssertion",
                        "input": generate_custom_assertion_input(assertion)
                    }
                else:
                    # Default fallback
                    assertion_data["graphql_input"] = {
                        "mutation": "createDatasetAssertion",
                        "input": generate_dataset_assertion_input(assertion)
                    }
                
                return assertion_data

        # Create assertion object for Git integration
        assertion_for_git = AssertionForGit(assertion, environment_name)
        
        # Create commit message
        commit_message = f"Add assertion '{assertion.name}' for {environment_name} environment"
        
        # Use GitIntegration to push to git (same as recipes and policies)
        logger.info(f"Staging assertion {assertion.id} to Git branch {current_branch}")
        git_integration = GitIntegration()
        result = git_integration.push_to_git(assertion_for_git, commit_message)
        
        if result and result.get("success"):
            logger.info(f"Successfully staged assertion {assertion.id} to Git branch {current_branch}")
            
            response_data = {
                "success": True,
                "message": f'Assertion "{assertion.name}" staged for commit to branch {current_branch}. You can create a PR from the Git Repository tab.',
                "environment": environment_name,
                "branch": current_branch,
                "redirect_url": "/github/repo/"  # Same as recipes
            }
            
            # Add file path info if available
            if "file_path" in result:
                response_data["file_path"] = result["file_path"]
            
            return JsonResponse(response_data)
        else:
            # Failed to stage changes
            error_message = f'Failed to stage assertion "{assertion.name}"'
            if isinstance(result, dict) and "error" in result:
                error_message += f": {result['error']}"
            
            logger.error(f"Failed to stage assertion: {error_message}")
            return JsonResponse({"success": False, "error": error_message})
        
    except Exception as e:
        logger.error(f"Error adding assertion to PR: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


def create_git_branch_and_commit(project_root, file_path, assertion_name, environment):
    """Create a Git branch and commit the assertion file for PR workflow"""
    try:
        # Generate branch name
        branch_name = f"assertion/{environment}/{assertion_name.lower().replace(' ', '-')}"
        
        # Change to project root directory
        original_cwd = os.getcwd()
        os.chdir(project_root)
        
        try:
            # Check if we're in a git repository
            subprocess.run(['git', 'status'], check=True, capture_output=True)
            
            # Check if branch already exists
            result = subprocess.run(
                ['git', 'branch', '--list', branch_name], 
                capture_output=True, text=True
            )
            
            if branch_name in result.stdout:
                # Switch to existing branch
                subprocess.run(['git', 'checkout', branch_name], check=True, capture_output=True)
            else:
                # Create and switch to new branch
                subprocess.run(['git', 'checkout', '-b', branch_name], check=True, capture_output=True)
            
            # Add the assertion file
            subprocess.run(['git', 'add', str(file_path.relative_to(project_root))], check=True, capture_output=True)
            
            # Commit the file
            commit_message = f"Add assertion '{assertion_name}' for {environment} environment"
            subprocess.run(['git', 'commit', '-m', commit_message], check=True, capture_output=True)
            
            return {
                "success": True,
                "branch": branch_name,
                "message": f"Created branch '{branch_name}' and committed assertion file"
            }
            
        finally:
            os.chdir(original_cwd)
            
    except subprocess.CalledProcessError as e:
        return {
            "success": False,
            "error": f"Git command failed: {e.stderr.decode() if e.stderr else str(e)}"
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"Git integration error: {str(e)}"
        }


def generate_field_assertion_input(assertion):
    """Generate CreateFieldAssertionInput from assertion config"""
    config = assertion.config
    return {
        "datasetUrn": config.get("dataset_urn", ""),
        "fields": config.get("fields", []),
        "type": config.get("field_assertion_type", "NOT_NULL"),
        "description": assertion.description
    }


def generate_sql_assertion_input(assertion):
    """Generate CreateSqlAssertionInput from assertion config"""
    config = assertion.config
    return {
        "datasetUrn": config.get("dataset_urn", ""),
        "statement": config.get("sql_statement", config.get("query", "")),
        "operator": config.get("operator", "EQUAL_TO"),
        "expectedValue": config.get("expected_value", config.get("expected_result", "1")),
        "description": assertion.description
    }


def generate_volume_assertion_input(assertion):
    """Generate CreateVolumeAssertionInput from assertion config"""
    config = assertion.config
    return {
        "datasetUrn": config.get("dataset_urn", ""),
        "operator": config.get("operator", "GREATER_THAN"),
        "parameters": {
            "value": config.get("value"),
            "maxValue": config.get("max_value"),
            "minValue": config.get("min_value")
        },
        "description": assertion.description
    }


def generate_freshness_assertion_input(assertion):
    """Generate CreateFreshnessAssertionInput from assertion config"""
    config = assertion.config
    return {
        "datasetUrn": config.get("dataset_urn", ""),
        "schedule": {
            "cron": config.get("cron", "0 8 * * *"),
            "timezone": config.get("timezone", "UTC")
        },
        "description": assertion.description
    }


def generate_dataset_assertion_input(assertion):
    """Generate CreateDatasetAssertionInput from assertion config"""
    config = assertion.config
    return {
        "datasetUrn": config.get("dataset_urn", ""),
        "scope": config.get("scope", "DATASET_ROWS"),
        "operator": config.get("operator", "EQUAL_TO"),
        "parameters": config.get("parameters", {}),
        "description": assertion.description
    }


def generate_schema_assertion_input(assertion):
    """Generate Schema assertion input from assertion config"""
    config = assertion.config
    return {
        "datasetUrn": config.get("dataset_urn", ""),
        "description": assertion.description
    }


def generate_custom_assertion_input(assertion):
    """Generate UpsertCustomAssertionInput from assertion config"""
    config = assertion.config
    return {
        "urn": config.get("datahub_urn") or assertion.urn,
        "input": {
            "entityUrn": config.get("entity_urn", config.get("dataset_urn", "")),
            "type": config.get("custom_type", "CUSTOM"),
            "logic": config.get("logic", ""),
            "description": assertion.description
        }
    }


@require_http_methods(["POST"])
def create_local_assertion(request):
    """Create a local-only assertion (not synchronized to DataHub)"""
    try:
        # Get basic info
        name = request.POST.get("name")
        description = request.POST.get("description", "")
        assertion_type = request.POST.get("type", "SQL")
        dataset_urn = request.POST.get("dataset_urn")  # This is the assertee URN
        
        if not name:
            return JsonResponse({
                "success": False,
                "error": "Assertion name is required"
            })
        
        # Get current environment for consistent URN generation
        from web_ui.views import get_current_connection
        current_connection = get_current_connection(request)
        current_environment = getattr(current_connection, 'environment', 'dev')

        # Generate URN using the same system as editable properties export
        base_urn = get_full_urn_from_name("assertion", name)
        mutation_config = get_mutation_config_for_environment(current_environment)
        deterministic_urn = generate_mutated_urn(base_urn, current_environment, "assertion", mutation_config)
        
        # Handle different assertion types for local creation
        config = {}
        
        if assertion_type == "SQL":
            # SQL assertion config
            database_platform = request.POST.get("database_platform")
            query = request.POST.get("query", "")
            expected_result = request.POST.get("expected_result", "SUCCESS")
            
            if not query:
                return JsonResponse({
                    "success": False,
                    "error": "SQL query is required for SQL assertions"
                })
            
            config = {
                "database_platform": database_platform,
                "query": query,
                "expected_result": expected_result,
                "dataset_urn": dataset_urn,  # Store the assertee URN
            }
            
        elif assertion_type == "DOMAIN_EXISTS":
            # Domain existence assertion
            domain_urn = request.POST.get("domain_urn")
            if not domain_urn:
                return JsonResponse({
                    "success": False,
                    "error": "Domain URN is required for domain existence assertions"
                })
            config = {
                "domain_urn": domain_urn,
                "dataset_urn": dataset_urn,
            }
            
        elif assertion_type == "TAG_EXISTS":
            # Tag existence assertion  
            tag_urn = request.POST.get("tag_urn")
            entity_urn = request.POST.get("entity_urn") or dataset_urn
            if not all([tag_urn, entity_urn]):
                return JsonResponse({
                    "success": False,
                    "error": "Tag URN and Entity URN are required for tag existence assertions"
                })
            config = {
                "tag_urn": tag_urn, 
                "entity_urn": entity_urn,
                "dataset_urn": dataset_urn,
            }
            
        elif assertion_type == "GLOSSARY_TERM_EXISTS":
            # Glossary term existence assertion
            term_urn = request.POST.get("term_urn")
            entity_urn = request.POST.get("entity_urn") or dataset_urn
            if not all([term_urn, entity_urn]):
                return JsonResponse({
                    "success": False,
                    "error": "Term URN and Entity URN are required for glossary term existence assertions"
                })
            config = {
                "term_urn": term_urn, 
                "entity_urn": entity_urn,
                "dataset_urn": dataset_urn,
            }
            
        elif assertion_type == "CUSTOM":
            # Custom assertion
            custom_logic = request.POST.get("custom_logic")
            if not custom_logic:
                return JsonResponse({
                    "success": False,
                    "error": "Custom logic is required for custom assertions"
                })
            config = {
                "custom_logic": custom_logic,
                "dataset_urn": dataset_urn,
            }
            
        elif assertion_type in ["FIELD", "VOLUME", "FRESHNESS", "SCHEMA"]:
            # DataHub assertion types - capture all form fields
            config = {key: value for key, value in request.POST.items() 
                     if key not in ['name', 'description', 'type']}
            # Ensure dataset_urn is included
            if dataset_urn:
                config["dataset_urn"] = dataset_urn
                
        else:
            # Generic assertion - store all form data
            config = {key: value for key, value in request.POST.items() 
                     if key not in ['name', 'description', 'type']}
            # Ensure dataset_urn is included
            if dataset_urn:
                config["dataset_urn"] = dataset_urn
        
        # Create the local assertion
        assertion = Assertion.objects.create(
            name=name,
            description=description,
            assertion_type=assertion_type,  # Use the new field
            entity_urn=dataset_urn,  # Store the assertee URN in the entity_urn field  
            urn=deterministic_urn,
            type=assertion_type,  # Keep for backward compatibility
            config=config,
            sync_status="LOCAL_ONLY"  # Mark as local-only
        )
        
        logger.info(f"Successfully created local assertion: {assertion.name} with URN: {assertion.urn}")
        
        return JsonResponse({
            "success": True,
            "message": f"Local assertion '{name}' created successfully",
            "assertion_id": assertion.id,
            "assertion_type": assertion_type,
            "urn": assertion.urn
        })
        
    except Exception as e:
        logger.error(f"Error creating local assertion: {str(e)}")
        return JsonResponse({
            "success": False,
            "error": str(e)
        })


@require_http_methods(["GET", "POST"])
def edit_assertion(request, assertion_id):
    """Edit an existing assertion (both local and synced)"""
    try:
        assertion = get_object_or_404(Assertion, id=assertion_id)
        
        if request.method == "GET":
            # Return assertion data for editing
            return JsonResponse({
                "success": True,
                "assertion": {
                    "id": str(assertion.id),
                    "name": assertion.name,
                    "description": assertion.description,
                    "assertion_type": assertion.assertion_type,
                    "entity_urn": assertion.entity_urn,
                    "sync_status": assertion.sync_status,
                    "config": assertion.config or {},
                }
            })
        
        elif request.method == "POST":
            # Update assertion
            name = request.POST.get("name")
            description = request.POST.get("description", "")
            assertion_type = request.POST.get("type")
            dataset_urn = request.POST.get("dataset_urn")
            
            if not name:
                return JsonResponse({
                    "success": False,
                    "error": "Assertion name is required"
                })
            
            # Update basic fields
            assertion.name = name
            assertion.description = description
            assertion.entity_urn = dataset_urn
            
            # Update assertion type if provided
            if assertion_type:
                assertion.assertion_type = assertion_type
                assertion.type = assertion_type  # Keep legacy field in sync
            
            # Handle type-specific configuration updates
            config = assertion.config or {}
            
            if assertion_type == "SQL":
                # SQL assertion config
                query = request.POST.get("query", "")
                expected_result = request.POST.get("expected_result", "SUCCESS")
                
                if not query:
                    return JsonResponse({
                        "success": False,
                        "error": "SQL query is required for SQL assertions"
                    })
                
                config.update({
                    "query": query,
                    "expected_result": expected_result,
                    "dataset_urn": dataset_urn,
                })
                
            elif assertion_type in ["FIELD", "VOLUME", "FRESHNESS", "SCHEMA", "CUSTOM"]:
                # DataHub assertion types - update all form fields
                for key, value in request.POST.items():
                    if key not in ['name', 'description', 'type']:
                        config[key] = value
                        
                # Ensure dataset_urn is included
                if dataset_urn:
                    config["dataset_urn"] = dataset_urn
                    
            else:
                # Generic assertion - update all form data
                for key, value in request.POST.items():
                    if key not in ['name', 'description', 'type']:
                        config[key] = value
                        
                # Ensure dataset_urn is included
                if dataset_urn:
                    config["dataset_urn"] = dataset_urn
            
            assertion.config = config
            
            # Update sync status if it was synced before
            if assertion.sync_status == "SYNCED":
                assertion.sync_status = "MODIFIED"
            
            assertion.save()
            
            logger.info(f"Successfully updated assertion: {assertion.name}")
            
            return JsonResponse({
                "success": True,
                "message": f"Assertion '{name}' updated successfully",
                "assertion_id": str(assertion.id),
                "assertion_type": assertion_type,
            })
        
    except Exception as e:
        logger.error(f"Error editing assertion: {str(e)}")
        return JsonResponse({
            "success": False,
            "error": str(e)
        })


@method_decorator(csrf_exempt, name="dispatch")
class AssertionAddToStagedChangesView(View):
    """API endpoint to add a local/synced assertion to staged changes"""
    
    def post(self, request, assertion_id):
        try:
            import json
            import os
            import sys
            from pathlib import Path
            
            # Add project root to path to import our Python modules
            sys.path.append(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            )
            
            # Import the function
            from scripts.mcps.assertion_actions import add_assertion_to_staged_changes
            
            # Get the assertion
            assertion = get_object_or_404(Assertion, id=assertion_id)
            
            data = json.loads(request.body)
            
            # Get environment and mutation name
            environment_name = data.get('environment', 'dev')
            mutation_name = data.get('mutation_name')
            
            # Get current user as owner
            owner = request.user.username if request.user.is_authenticated else "admin"
            
            logger.info(f"Adding assertion '{assertion.name}' to staged changes...")
            
            # Add assertion to staged changes using the MCP pattern
            result = add_assertion_to_staged_changes(
                assertion_id=str(assertion.id),
                assertion_urn=assertion.urn,
                assertion_name=assertion.name,
                assertion_type=assertion.assertion_type,
                description=assertion.description,
                entity_urn=assertion.entity_urn,
                config=assertion.config or {},
                environment=environment_name,
                owner=owner,
                mutation_name=mutation_name
            )
            
            if not result.get("success"):
                return JsonResponse({
                    "success": False,
                    "error": result.get("message", "Failed to add assertion to staged changes")
                }, status=500)
            
            # Provide feedback about the operation
            files_created = result.get("files_saved", [])
            files_created_count = len(files_created)
            mcps_created = result.get("mcps_created", 0)
            
            message = f"Assertion added to staged changes: {mcps_created} MCPs created, {files_created_count} files saved"
            
            return JsonResponse({
                'success': True,
                'message': message,
                'files_created': files_created,
                'files_created_count': files_created_count,
                'mcps_created': mcps_created,
                'assertion_id': str(assertion.id),
                'assertion_urn': assertion.urn
            })
            
        except Assertion.DoesNotExist:
            logger.error(f"Assertion with ID {assertion_id} not found")
            return JsonResponse({
                'success': False,
                'error': 'Assertion data not found. Please refresh the page and try again.'
            }, status=404)
        except Exception as e:
            logger.error(f"Error adding assertion to staged changes: {str(e)}")
            return JsonResponse({"error": str(e)}, status=500)


@method_decorator(csrf_exempt, name="dispatch")
class AssertionRemoteAddToStagedChangesView(View):
    """API endpoint to add a remote assertion to staged changes without syncing to local first"""
    
    def post(self, request):
        try:
            import json
            import os
            import sys
            
            # Add project root to path to import our Python modules
            sys.path.append(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            )
            
            # Import the function
            from scripts.mcps.assertion_actions import add_assertion_to_staged_changes
            
            data = json.loads(request.body)
            
            # Get environment and mutation name
            environment_name = data.get('environment', 'dev')
            mutation_name = data.get('mutation_name')
            assertion_data = data.get('item_data')
            
            if not assertion_data:
                return JsonResponse({
                    "status": "error",
                    "error": "Assertion data is required"
                }, status=400)
            
            assertion_urn = assertion_data.get('urn')
            if not assertion_urn:
                return JsonResponse({
                    "status": "error",
                    "error": "Assertion URN is required"
                }, status=400)
            
            # Get current user as owner
            owner = request.user.username if request.user.is_authenticated else "admin"
            
            # Extract assertion ID from URN (last part after colon)
            assertion_id = assertion_urn.split(':')[-1] if assertion_urn else None
            if not assertion_id:
                return JsonResponse({
                    "status": "error",
                    "error": "Could not extract assertion ID from URN"
                }, status=400)
            
            # Extract assertion information from remote data
            assertion_name = assertion_data.get('name', 'Unknown Assertion')
            assertion_type = assertion_data.get('type', 'CUSTOM')
            description = assertion_data.get('description', '')
            entity_urn = assertion_data.get('entity_urn', '')
            config = assertion_data.get('config') or assertion_data.get('definition', {})
            
            logger.info(f"Adding remote assertion '{assertion_name}' to staged changes...")
            
            # Add remote assertion to staged changes using the MCP pattern
            result = add_assertion_to_staged_changes(
                assertion_id=assertion_id,
                assertion_urn=assertion_urn,
                assertion_name=assertion_name,
                assertion_type=assertion_type,
                description=description,
                entity_urn=entity_urn,
                config=config,
                environment=environment_name,
                owner=owner,
                mutation_name=mutation_name
            )
            
            if not result.get("success"):
                return JsonResponse({
                    "status": "error",
                    "error": result.get("message", "Failed to add remote assertion to staged changes")
                }, status=500)
            
            # Provide feedback about the operation
            files_created = result.get("files_saved", [])
            files_created_count = len(files_created)
            mcps_created = result.get("mcps_created", 0)
            
            message = f"Remote assertion added to staged changes: {mcps_created} MCPs created, {files_created_count} files saved"
            
            return JsonResponse({
                "status": "success",
                "message": message,
                "files_created": files_created,
                "files_created_count": files_created_count,
                "mcps_created": mcps_created,
                "assertion_urn": assertion_urn
            })
                
        except Exception as e:
            logger.error(f"Error adding remote assertion to staged changes: {str(e)}")
            return JsonResponse({"error": str(e)}, status=500)
