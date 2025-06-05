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
                "page_title": "Metadata Assertions",
            }

            logger.info("Rendering assertion list template (async loading)")
            return render(request, "metadata_manager/assertions/list.html", context)
        except Exception as e:
            logger.error(f"Error in assertion list view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return render(
                request,
                "metadata_manager/assertions/list.html",
                {"error": str(e), "page_title": "Metadata Assertions"},
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
            assertion = Assertion.objects.create(
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
            assertion = Assertion.objects.create(
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
            f"Getting DataHub assertions: query='{query}', start={start}, count={count}"
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
    """AJAX endpoint to get remote assertions data"""
    try:
        logger.info("Loading remote assertions data via AJAX")

        # Get DataHub connection
        connected, client = test_datahub_connection()
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})

        # Get all local assertions
        local_assertions = Assertion.objects.all().order_by("name")

        # Fetch remote assertions
        remote_assertions = []
        synced_assertions = []
        datahub_url = None

        try:
            logger.debug("Fetching remote assertions from DataHub")

            # Get DataHub URL for direct links
            datahub_url = client.server_url
            if datahub_url.endswith("/api/gms"):
                datahub_url = datahub_url[:-8]  # Remove /api/gms to get base URL

            # Get assertions from DataHub using the existing method
            result = client.get_assertions(start=0, count=1000, query="*")

            if result.get("success", False):
                remote_assertions_data = result["data"].get("searchResults", [])
                logger.debug(f"Fetched {len(remote_assertions_data)} remote assertions")

                # Process the assertions
                for assertion_result in remote_assertions_data:
                    assertion_data = assertion_result.get("entity", {})
                    if assertion_data:
                        remote_assertions.append(assertion_data)

                # TODO: Implement synced assertions logic when we have assertion syncing
                # For now, all local assertions are considered local-only
                synced_assertions = []

            else:
                logger.warning(
                    f"Failed to fetch remote assertions: {result.get('error', 'Unknown error')}"
                )

            return JsonResponse(
                {
                    "success": True,
                    "data": {
                        "local_assertions": [
                            {
                                "id": assertion.id,
                                "name": assertion.name,
                                "description": assertion.description,
                                "type": assertion.type,
                                "last_run": assertion.last_run.isoformat()
                                if assertion.last_run
                                else None,
                                "last_status": assertion.last_status,
                            }
                            for assertion in local_assertions
                        ],
                        "remote_assertions": remote_assertions,
                        "synced_assertions": synced_assertions,
                        "datahub_url": datahub_url,
                    },
                }
            )

        except Exception as e:
            logger.error(f"Error fetching remote assertion data: {str(e)}")
            return JsonResponse(
                {
                    "success": False,
                    "error": f"Error fetching remote assertions: {str(e)}",
                }
            )

    except Exception as e:
        logger.error(f"Error in get_remote_assertions_data: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})
