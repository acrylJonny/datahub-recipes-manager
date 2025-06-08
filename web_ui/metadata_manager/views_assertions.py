import logging
from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.views import View
from django.contrib import messages
from django.utils import timezone
from django.views.decorators.http import require_POST, require_http_methods
from django.utils.decorators import method_decorator
import yaml

# Add project root to sys.path
import sys
import os

project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.append(project_root)

# Import the deterministic URN utilities
from utils.urn_utils import get_full_urn_from_name
from utils.datahub_utils import test_datahub_connection
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
            connected, client = test_datahub_connection()
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
                        "domain_urn": domain.deterministic_urn,
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
            connected, client = test_datahub_connection()
            
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
            
            # Generate the tag URN
            tag_urn = get_full_urn_from_name("tag", tag_name)
            
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
            
            # Generate the term URN (this is a simplified approach, actual URN may depend on parent path)
            term_urn = get_full_urn_from_name("glossaryTerm", term_name)
            
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
            connected, client = test_datahub_connection()
            
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
            connected, client = test_datahub_connection()
            
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
            connected, client = test_datahub_connection()
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


def get_client_from_session(request):
    """
    Get a DataHub client from the session or create a new one.

    Args:
        request (HttpRequest): The request object

    Returns:
        DataHubRestClient: The client instance or None if not connected
    """
    try:
        # Get active environment
        environment = DjangoEnvironment.objects.filter(is_default=True).first()
        if not environment:
            logger.error("No active environment configured")
            return None

        # Initialize and return a client
        client = DataHubRestClient(environment.datahub_url, environment.datahub_token)
        return client
    except Exception as e:
        logger.error(f"Error creating DataHub client: {str(e)}")
        return None


@require_http_methods(["GET"])
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

        # Get client from session
        client = get_client_from_session(request)
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
        connected, client = test_datahub_connection()
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})

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

            # Extract assertion URNs that exist locally (if we had URN mapping)
            # For now, treat all local assertions as local-only since we don't have URN mapping
            local_assertion_urns = set()  # Would be populated if we had URN mapping

            # Process local assertions
            for assertion in local_assertions:
                # Create enhanced local assertion data
                local_assertion_data = {
                    "id": assertion.id,
                    "urn": f"urn:li:assertion:local:{assertion.id}",  # Mock URN for consistency
                    "name": assertion.name,
                    "description": assertion.description or "",
                    "type": assertion.type,
                    "sync_status": "LOCAL_ONLY",
                    "sync_status_display": "Local Only",
                    
                    # Initialize ownership data (not stored locally for assertions)
                    "ownership": None,
                    "owners_count": 0,
                    "owner_names": [],
                    
                    # Initialize relationships data (not stored locally for assertions)
                    "relationships": None,
                    "relationships_count": 0,
                    
                    # Add local metadata
                    "last_run": assertion.last_run.isoformat() if assertion.last_run else None,
                    "last_status": assertion.last_status,
                    "created_at": assertion.created_at.isoformat() if assertion.created_at else None,
                    "updated_at": assertion.updated_at.isoformat() if assertion.updated_at else None,
                }

                # For now, all local assertions are local-only since we don't have syncing implemented
                local_only_items.append(local_assertion_data)

            # All remote assertions are remote-only since we don't have syncing implemented
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
        connected, client = test_datahub_connection()
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
    """Sync a remote assertion to local storage"""
    try:
        import json
        data = json.loads(request.body)
        assertion_urn = data.get('assertion_urn')
        
        if not assertion_urn:
            return JsonResponse({"success": False, "error": "Assertion URN is required"})
        
        # Get DataHub connection
        connected, client = test_datahub_connection()
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})
        
        # Get the assertion data from DataHub
        result = client.get_assertions(query=f'urn:"{assertion_urn}"', count=1)
        
        if not result.get("success") or not result["data"].get("searchResults"):
            return JsonResponse({"success": False, "error": "Assertion not found in DataHub"})
        
        assertion_data = result["data"]["searchResults"][0]["entity"]
        info = assertion_data.get("info", {})
        
        # Create local assertion from remote data
        assertion = Assertion.objects.create(
            name=info.get("description", assertion_urn.split(":")[-1]),
            description=info.get("description", ""),
            type=info.get("type", "Unknown"),
            config={
                "synced_from_datahub": True,
                "original_urn": assertion_urn,
                "raw_data": assertion_data
            }
        )
        
        logger.info(f"Successfully synced assertion to local: {assertion_urn} -> {assertion.id}")
        return JsonResponse({
            "success": True,
            "message": "Assertion synced to local storage successfully",
            "assertion_id": assertion.id
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
        connected, client = test_datahub_connection()
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
        # This is a simplified implementation - you may need to enhance based on assertion type
        if assertion.type == "sql":
            config = assertion.config or {}
            dataset_urn = config.get("dataset_urn", "urn:li:dataset:(urn:li:dataPlatform:unknown,unknown,PROD)")
            sql_statement = config.get("query", "SELECT 1")
            
            result_urn = metadata_client.create_sql_assertion(
                dataset_urn=dataset_urn,
                sql_statement=sql_statement,
                operator="EQUAL_TO",
                value="1",
                description=assertion.description or assertion.name
            )
            
            if result_urn:
                # Update local assertion with DataHub URN
                assertion.config = assertion.config or {}
                assertion.config["datahub_urn"] = result_urn
                assertion.save()
                
                logger.info(f"Successfully pushed assertion to DataHub: {assertion.id} -> {result_urn}")
                return JsonResponse({
                    "success": True,
                    "message": "Assertion pushed to DataHub successfully",
                    "datahub_urn": result_urn
                })
            else:
                return JsonResponse({"success": False, "error": "Failed to create assertion in DataHub"})
        else:
            return JsonResponse({"success": False, "error": f"Pushing {assertion.type} assertions is not yet implemented"})
            
    except Exception as e:
        logger.error(f"Error pushing assertion to DataHub: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_http_methods(["POST"])
def resync_assertion(request, assertion_id):
    """Resync a local assertion with its DataHub counterpart"""
    try:
        import json
        data = json.loads(request.body)
        assertion_urn = data.get('assertion_urn')
        
        assertion = get_object_or_404(Assertion, id=assertion_id)
        
        if not assertion_urn:
            return JsonResponse({"success": False, "error": "Assertion URN is required"})
        
        # Get DataHub connection
        connected, client = test_datahub_connection()
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})
        
        # Get the latest assertion data from DataHub
        result = client.get_assertions(query=f'urn:"{assertion_urn}"', count=1)
        
        if not result.get("success") or not result["data"].get("searchResults"):
            return JsonResponse({"success": False, "error": "Assertion not found in DataHub"})
        
        assertion_data = result["data"]["searchResults"][0]["entity"]
        info = assertion_data.get("info", {})
        
        # Update local assertion with latest remote data
        assertion.name = info.get("description", assertion.name)
        assertion.description = info.get("description", assertion.description)
        assertion.type = info.get("type", assertion.type)
        assertion.config = assertion.config or {}
        assertion.config["raw_data"] = assertion_data
        assertion.config["last_synced"] = timezone.now().isoformat()
        assertion.save()
        
        logger.info(f"Successfully resynced assertion: {assertion_id} with {assertion_urn}")
        return JsonResponse({
            "success": True,
            "message": "Assertion resynced successfully"
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
        
        # Get DataHub client
        connected, client = test_datahub_connection()
        if not connected or not client:
            return JsonResponse({
                "success": False,
                "error": "DataHub connection not available"
            })
        
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
            
            assertion_urn = client.create_field_assertion(
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
            
            assertion_urn = client.create_sql_assertion(
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
            
            assertion_urn = client.create_volume_assertion(**kwargs)
            
        elif assertion_type == "FRESHNESS":
            freshness_interval = int(request.POST.get("freshness_interval", 24))
            freshness_unit = request.POST.get("freshness_unit", "HOUR")
            
            assertion_urn = client.create_freshness_assertion(
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
            
            assertion_urn = client.create_schema_assertion(
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
