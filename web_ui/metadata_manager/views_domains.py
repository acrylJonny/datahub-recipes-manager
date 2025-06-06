from django.shortcuts import render, redirect, get_object_or_404
from django.views import View
from django.contrib import messages
from django.http import JsonResponse, HttpResponse
from django.views.decorators.http import require_POST
from django.utils.decorators import method_decorator
from django.utils import timezone
import json
import logging
import os
import sys

# Add project root to sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

# Import the deterministic URN utilities
from utils.urn_utils import get_full_urn_from_name
from utils.datahub_utils import get_datahub_client, test_datahub_connection
from utils.token_utils import get_token_from_env
from .models import Domain
from web_ui.models import GitSettings

logger = logging.getLogger(__name__)


class DomainListView(View):
    """View to list and create domains"""

    def get(self, request):
        """Display list of domains"""
        try:
            logger.info("Starting DomainListView.get")

            # Get all local domains (database-only operation)
            domains = Domain.objects.all().order_by("name")
            logger.debug(f"Found {domains.count()} total domains")

            # Get DataHub connection info (quick test only)
            logger.debug("Testing DataHub connection from DomainListView")
            connected, client = test_datahub_connection()
            logger.debug(f"DataHub connection test result: {connected}")

            # Initialize basic context with local data only
            context = {
                "domains": domains,
                "page_title": "DataHub Domains",
                "has_datahub_connection": connected,
                "has_git_integration": False,
                # Initialize empty lists - will be populated via AJAX
                "synced_domains": [],
                "local_domains": domains,  # Show all as local initially
                "remote_only_domains": [],
                "datahub_url": None,
            }

            # Check if git integration is enabled
            try:
                github_settings = GitSettings.objects.first()
                context["has_git_integration"] = (
                    github_settings and github_settings.enabled
                )
                logger.debug(
                    f"Git integration enabled: {context['has_git_integration']}"
                )
            except Exception as e:
                logger.warning(f"Error checking git integration: {str(e)}")
                pass

            logger.info("Rendering domain list template (async loading)")
            return render(request, "metadata_manager/domains/list.html", context)
        except Exception as e:
            logger.error(f"Error in domain list view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return render(
                request,
                "metadata_manager/domains/list.html",
                {"error": str(e), "page_title": "DataHub Domains"},
            )

    def post(self, request):
        """Create a new domain"""
        try:
            name = request.POST.get("name")
            description = request.POST.get("description", "")

            if not name:
                messages.error(request, "Domain name is required")
                return redirect("metadata_manager:domain_list")

            # Generate deterministic URN
            deterministic_urn = get_full_urn_from_name("domain", name)

            # Check if domain with this URN already exists
            if Domain.objects.filter(deterministic_urn=deterministic_urn).exists():
                messages.error(request, f"Domain with name '{name}' already exists")
                return redirect("metadata_manager:domain_list")

            # Create the domain
            Domain.objects.create(
                name=name,
                description=description,
                deterministic_urn=deterministic_urn,
                sync_status="LOCAL_ONLY",
            )

            messages.success(request, f"Domain '{name}' created successfully")
            return redirect("metadata_manager:domain_list")
        except Exception as e:
            logger.error(f"Error creating domain: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:domain_list")


class DomainDetailView(View):
    """View to display, edit and delete domains"""

    def get(self, request, domain_id):
        """Display domain details"""
        try:
            domain = get_object_or_404(Domain, id=domain_id)

            # Initialize context with domain data
            context = {
                "domain": domain,
                "page_title": f"Domain: {domain.name}",
                "has_git_integration": False,  # Set this based on checking GitHub settings
            }

            # Check if git integration is enabled
            try:
                github_settings = GitSettings.objects.first()
                context["has_git_integration"] = (
                    github_settings and github_settings.enabled
                )
            except:
                pass

            # Get related entities if DataHub connection is available
            client = get_datahub_client()
            if client and client.test_connection():
                domain_urn = domain.deterministic_urn

                # Find entities with this domain, limit to 50 for performance
                try:
                    related_entities = client.find_entities_with_domain(
                        domain_urn=domain_urn, count=50
                    )

                    # Add to context
                    context["related_entities"] = related_entities.get("entities", [])
                    context["total_related"] = related_entities.get("total", 0)
                    context["has_datahub_connection"] = True

                    # Also add URL for reference
                    if hasattr(client, "server_url"):
                        context["datahub_url"] = client.server_url

                    # Get remote domain information if possible
                    if domain.sync_status != "LOCAL_ONLY":
                        try:
                            remote_domain = client.get_domain(domain_urn)
                            if remote_domain:
                                context["remote_domain"] = remote_domain

                                # Check if the domain needs to be synced
                                local_description = domain.description or ""
                                remote_description = remote_domain.get(
                                    "description", ""
                                )

                                # If different, mark as modified
                                if (
                                    local_description != remote_description
                                    and domain.sync_status != "MODIFIED"
                                ):
                                    domain.sync_status = "MODIFIED"
                                    domain.save(update_fields=["sync_status"])

                                # If the same but marked as modified, update to synced
                                elif (
                                    local_description == remote_description
                                    and domain.sync_status == "MODIFIED"
                                ):
                                    domain.sync_status = "SYNCED"
                                    domain.save(update_fields=["sync_status"])
                        except Exception as e:
                            logger.warning(
                                f"Error fetching remote domain information: {str(e)}"
                            )
                except Exception as e:
                    logger.error(
                        f"Error fetching related entities for domain {domain.name}: {str(e)}"
                    )
                    context["has_datahub_connection"] = (
                        True  # We still have a connection, just failed to get entities
                    )
                    context["related_entities_error"] = str(e)
            else:
                context["has_datahub_connection"] = False

            return render(request, "metadata_manager/domains/detail.html", context)
        except Exception as e:
            logger.error(f"Error in domain detail view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:domain_list")

    def post(self, request, domain_id):
        """Update a domain"""
        try:
            domain = get_object_or_404(Domain, id=domain_id)

            name = request.POST.get("name")
            description = request.POST.get("description", "")

            if not name:
                messages.error(request, "Domain name is required")
                return redirect("metadata_manager:domain_detail", domain_id=domain_id)

            # Update the domain
            domain.name = name
            domain.description = description

            # If the domain was previously synced, mark it as modified
            if domain.sync_status in ["SYNCED", "REMOTE_ONLY"]:
                domain.sync_status = "MODIFIED"

            domain.save()

            messages.success(request, f"Domain '{name}' updated successfully")
            return redirect("metadata_manager:domain_detail", domain_id=domain_id)
        except Exception as e:
            logger.error(f"Error updating domain: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:domain_list")

    def delete(self, request, domain_id):
        """Delete a domain"""
        try:
            domain = get_object_or_404(Domain, id=domain_id)

            # Delete the domain
            domain_name = domain.name
            domain.delete()

            return JsonResponse(
                {
                    "success": True,
                    "message": f"Domain '{domain_name}' deleted successfully",
                }
            )
        except Exception as e:
            logger.error(f"Error deleting domain: {str(e)}")
            return JsonResponse(
                {"success": False, "message": f"An error occurred: {str(e)}"}
            )


class DomainDeployView(View):
    """View to deploy a domain to DataHub"""

    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)

    def post(self, request, domain_id):
        """Deploy a domain to DataHub"""
        try:
            domain = get_object_or_404(Domain, id=domain_id)

            # Check if the domain can be deployed
            if not domain.can_deploy:
                messages.error(
                    request,
                    f"Domain '{domain.name}' cannot be deployed (current status: {domain.get_sync_status_display()})",
                )
                return redirect("metadata_manager:domain_detail", domain_id=domain_id)

            # Get the client
            try:
                get_token_from_env()
                connected, client = test_datahub_connection()
                if not connected or not client:
                    messages.error(
                        request,
                        "Cannot connect to DataHub. Please check your connection settings.",
                    )
                    return redirect(
                        "metadata_manager:domain_detail", domain_id=domain_id
                    )
            except Exception as e:
                logger.error(f"Error initializing client: {str(e)}")
                messages.error(
                    request,
                    "Failed to initialize DataHub client. Please check your connection settings.",
                )
                return redirect("metadata_manager:domain_detail", domain_id=domain_id)

            # Check if this is a new domain or an update
            if domain.sync_status == "LOCAL_ONLY":
                # New domain - use createDomain mutation
                try:
                    # Generate domain ID from name
                    domain_id = domain.name.lower().replace(" ", "_")

                    # Create domain in DataHub
                    result = client.create_domain(
                        domain_id=domain_id,
                        name=domain.name,
                        description=domain.description or "",
                    )

                    if result:
                        # Update domain with remote URN and status
                        domain.original_urn = result
                        domain.sync_status = "SYNCED"
                        domain.last_synced = timezone.now()
                        domain.save()

                        messages.success(
                            request,
                            f"Successfully deployed domain '{domain.name}' to DataHub",
                        )
                    else:
                        messages.error(
                            request,
                            f"Failed to deploy domain '{domain.name}' to DataHub",
                        )
                except Exception as e:
                    logger.error(f"Error deploying domain {domain.name}: {str(e)}")
                    messages.error(request, f"Error deploying domain: {str(e)}")
            else:
                # Update existing domain - use updateDomain mutation
                try:
                    # Update domain in DataHub
                    result = client.update_domain(
                        domain_urn=domain.deterministic_urn,
                        name=domain.name,
                        description=domain.description or "",
                    )

                    if result:
                        # Update domain status
                        domain.sync_status = "SYNCED"
                        domain.last_synced = timezone.now()
                        domain.save()

                        messages.success(
                            request,
                            f"Successfully updated domain '{domain.name}' in DataHub",
                        )
                    else:
                        messages.error(
                            request,
                            f"Failed to update domain '{domain.name}' in DataHub",
                        )
                except Exception as e:
                    logger.error(f"Error updating domain {domain.name}: {str(e)}")
                    messages.error(request, f"Error updating domain: {str(e)}")

            return redirect("metadata_manager:domain_detail", domain_id=domain_id)
        except Exception as e:
            logger.error(f"Error in domain deploy view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:domain_list")


class DomainGitPushView(View):
    """View to push a domain to GitHub"""

    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)

    def post(self, request, domain_id):
        """Push a domain to a GitHub PR"""
        try:
            from web_ui.views import add_entity_to_git_push

            domain = get_object_or_404(Domain, id=domain_id)

            # Add domain to Git staging
            try:
                result = add_entity_to_git_push(domain)

                if result.get("success", False):
                    domain.staged_for_git = True
                    domain.save(update_fields=["staged_for_git"])

                    return JsonResponse(
                        {
                            "success": True,
                            "message": f"Domain '{domain.name}' added to GitHub PR",
                        }
                    )
                else:
                    return JsonResponse(
                        {
                            "success": False,
                            "error": result.get("error", "Unknown error"),
                        }
                    )
            except Exception as e:
                logger.error(f"Error pushing domain to GitHub: {str(e)}")
                return JsonResponse({"success": False, "error": str(e)})
        except Exception as e:
            logger.error(f"Error in domain GitHub push view: {str(e)}")
            return JsonResponse({"success": False, "error": str(e)})


class DomainImportExportView(View):
    """View to import/export domains"""

    def get(self, request):
        """Display import/export page"""
        return render(
            request,
            "metadata_manager/domains/import_export.html",
            {"page_title": "Import/Export Domains"},
        )

    def post(self, request):
        """Handle import or export"""
        try:
            action = request.POST.get("action")

            if action == "export":
                # Export domains to JSON
                domains = Domain.objects.all()

                export_data = {"domains": [domain.to_dict() for domain in domains]}

                response = HttpResponse(
                    json.dumps(export_data, indent=2), content_type="application/json"
                )
                response["Content-Disposition"] = (
                    'attachment; filename="domains_export.json"'
                )
                return response
            elif action == "import":
                # Import from uploaded JSON file
                if "json_file" not in request.FILES:
                    messages.error(request, "No file uploaded")
                    return redirect("metadata_manager:domain_import_export")

                file = request.FILES["json_file"]
                try:
                    import_data = json.loads(file.read().decode("utf-8"))

                    # Import domains
                    domain_count = 0

                    for domain_data in import_data.get("domains", []):
                        domain, created = Domain.objects.get_or_create(
                            deterministic_urn=domain_data.get("urn"),
                            defaults={
                                "name": domain_data.get("name"),
                                "description": domain_data.get("description", ""),
                                "original_urn": domain_data.get("original_urn"),
                                "sync_status": "LOCAL_ONLY",
                            },
                        )

                        if not created:
                            # Update existing domain
                            domain.name = domain_data.get("name")
                            domain.description = domain_data.get("description", "")
                            domain.original_urn = domain_data.get("original_urn")
                            domain.save()

                        domain_count += 1

                    messages.success(
                        request, f"Successfully imported {domain_count} domains"
                    )
                except json.JSONDecodeError:
                    messages.error(request, "Invalid JSON file")
                except Exception as e:
                    logger.error(f"Error importing domain data: {str(e)}")
                    messages.error(request, f"Error importing data: {str(e)}")

                return redirect("metadata_manager:domain_list")
            else:
                messages.error(request, "Invalid action")
                return redirect("metadata_manager:domain_import_export")
        except Exception as e:
            logger.error(f"Error in domain import/export view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:domain_list")


class DomainPullView(View):
    """View to pull domains from DataHub"""

    def get(self, request):
        """Display pull confirmation page"""
        try:
            # Get query parameters
            domain_urn = request.GET.get("domain_urn")
            confirm = request.GET.get("confirm") == "true"

            context = {
                "page_title": "Pull Domains from DataHub",
                "confirm": confirm,
                "domain_urn": domain_urn,
            }

            # Get DataHub connection info
            connected, client = test_datahub_connection()
            context["has_datahub_connection"] = connected

            return render(request, "metadata_manager/domains/pull.html", context)
        except Exception as e:
            logger.error(f"Error in domain pull view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:domain_list")

    def post(self, request):
        """Pull domains from DataHub"""
        try:
            # Check if we're just confirming the pull
            if request.POST.get("confirm") == "true" and not request.POST.get(
                "execute"
            ):
                return self.get(request)

            # Get the client
            connected, client = test_datahub_connection()

            if not connected or not client:
                messages.error(
                    request,
                    "Cannot connect to DataHub. Please check your connection settings.",
                )
                return redirect("metadata_manager:domain_list")

            results = []

            # Check if we're pulling a specific domain
            domain_urn = request.POST.get("domain_urn")

            if domain_urn:
                # Pull specific domain
                try:
                    domain_data = client.get_domain(domain_urn)
                    if domain_data:
                        domain_name = domain_data.get("name", "Unknown Domain")

                        # Check if we already have this domain
                        existing_domain = Domain.objects.filter(
                            deterministic_urn=domain_urn
                        ).first()

                        if existing_domain:
                            # Update existing domain
                            existing_domain.name = domain_data.get(
                                "name", existing_domain.name
                            )
                            existing_domain.description = (
                                domain_data.get("description")
                                or existing_domain.description
                                or ""
                            )
                            existing_domain.original_urn = domain_urn
                            existing_domain.sync_status = "SYNCED"
                            existing_domain.last_synced = timezone.now()
                            existing_domain.save()

                            results.append(
                                {
                                    "domain_name": domain_name,
                                    "success": True,
                                    "message": "Updated existing domain",
                                }
                            )
                        else:
                            # Create new domain
                            Domain.objects.create(
                                name=domain_data.get("name"),
                                description=domain_data.get("description") or "",
                                deterministic_urn=domain_urn,
                                original_urn=domain_urn,
                                sync_status="SYNCED",
                                last_synced=timezone.now(),
                            )

                            results.append(
                                {
                                    "domain_name": domain_name,
                                    "success": True,
                                    "message": "Created new domain",
                                }
                            )
                    else:
                        results.append(
                            {
                                "domain_name": domain_urn,
                                "success": False,
                                "message": "Domain not found in DataHub",
                            }
                        )
                except Exception as e:
                    logger.error(f"Error pulling domain {domain_urn}: {str(e)}")
                    results.append(
                        {
                            "domain_name": domain_urn,
                            "success": False,
                            "message": f"Error: {str(e)}",
                        }
                    )
            else:
                # Pull all domains
                try:
                    logger.info("Pulling all domains from DataHub")
                    remote_domains = client.list_domains(count=1000)

                    if not remote_domains or not isinstance(remote_domains, list):
                        logger.warning(
                            f"Unexpected response when pulling domains: {type(remote_domains)}"
                        )
                        messages.warning(
                            request,
                            "No domains found in DataHub or unexpected response format",
                        )
                        return render(
                            request,
                            "metadata_manager/domains/pull.html",
                            {
                                "has_datahub_connection": True,
                                "page_title": "Pull Domains from DataHub",
                                "results": [],
                            },
                        )

                    logger.info(f"Found {len(remote_domains)} domains in DataHub")

                    # Track domains processed for summary
                    domains_updated = 0
                    domains_created = 0
                    domains_errored = 0

                    for domain_data in remote_domains:
                        try:
                            domain_urn = domain_data.get("urn")
                            if not domain_urn:
                                logger.debug(
                                    f"Skipping domain with no URN: {domain_data}"
                                )
                                continue

                            # Ensure URN is a string
                            domain_urn = str(domain_urn)

                            domain_name = domain_data.get("name", "Unknown Domain")
                            logger.debug(
                                f"Processing domain: {domain_name} ({domain_urn})"
                            )

                            # Check if we already have this domain
                            existing_domain = Domain.objects.filter(
                                deterministic_urn=domain_urn
                            ).first()

                            if existing_domain:
                                # Update existing domain
                                existing_domain.name = domain_data.get(
                                    "name", existing_domain.name
                                )
                                existing_domain.description = (
                                    domain_data.get("description")
                                    or existing_domain.description
                                    or ""
                                )
                                existing_domain.original_urn = domain_urn
                                existing_domain.sync_status = "SYNCED"
                                existing_domain.last_synced = timezone.now()
                                existing_domain.save()

                                domains_updated += 1
                                results.append(
                                    {
                                        "domain_name": domain_name,
                                        "success": True,
                                        "message": "Updated existing domain",
                                    }
                                )
                            else:
                                # Create new domain
                                Domain.objects.create(
                                    name=domain_data.get("name"),
                                    description=domain_data.get("description") or "",
                                    deterministic_urn=domain_urn,
                                    original_urn=domain_urn,
                                    sync_status="SYNCED",
                                    last_synced=timezone.now(),
                                )

                                domains_created += 1
                                results.append(
                                    {
                                        "domain_name": domain_name,
                                        "success": True,
                                        "message": "Created new domain",
                                    }
                                )
                        except Exception as domain_error:
                            logger.error(
                                f"Error processing domain {domain_data.get('name', 'unknown')}: {str(domain_error)}"
                            )
                            domains_errored += 1
                            results.append(
                                {
                                    "domain_name": domain_data.get(
                                        "name", "Unknown Domain"
                                    ),
                                    "success": False,
                                    "message": f"Error: {str(domain_error)}",
                                }
                            )

                    # Provide a detailed summary in the message
                    summary = f"Successfully pulled domains from DataHub: {domains_created} created, {domains_updated} updated"
                    if domains_errored > 0:
                        summary += f", {domains_errored} errors"

                    logger.info(summary)
                    messages.success(request, summary)

                except Exception as e:
                    logger.error(f"Error pulling all domains: {str(e)}")
                    results.append(
                        {
                            "domain_name": "All Domains",
                            "success": False,
                            "message": f"Error: {str(e)}",
                        }
                    )
                    messages.error(request, f"Error pulling domains: {str(e)}")

            # Return to the pull page with results
            return render(
                request,
                "metadata_manager/domains/pull.html",
                {
                    "has_datahub_connection": True,
                    "page_title": "Pull Domains from DataHub",
                    "results": results,
                },
            )
        except Exception as e:
            logger.error(f"Error in domain pull view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:domain_list")


def get_remote_domains_data(request):
    """AJAX endpoint to get enhanced remote domains data with ownership and relationships"""
    try:
        logger.info("Loading enhanced remote domains data via AJAX")

        # Get DataHub connection
        connected, client = test_datahub_connection()
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})

        # Get all local domains
        local_domains = Domain.objects.all().order_by("name")

        # Initialize data structures
        synced_items = []
        local_only_items = []
        remote_only_items = []
        datahub_url = None

        try:
            logger.debug("Fetching enhanced remote domains from DataHub")
            
            # Get DataHub URL for direct links
            datahub_url = client.server_url
            if datahub_url.endswith("/api/gms"):
                datahub_url = datahub_url[:-8]  # Remove /api/gms to get base URL

            # Get all remote domains from DataHub with enhanced data
            remote_domains = client.list_domains(count=1000)
            logger.debug(f"Fetched {len(remote_domains) if remote_domains else 0} remote domains")

            # Enhance remote domains with ownership and relationship data
            enhanced_remote_domains = {}
            for domain in remote_domains:
                domain_urn = domain.get("urn")
                if domain_urn:
                    # Extract basic properties
                    properties = domain.get("properties", {})
                    
                    # Extract parent domain information
                    parent_urn = None
                    parent_domain = domain.get("parentDomain")
                    if parent_domain:
                        parent_urn = parent_domain.get("urn") if isinstance(parent_domain, dict) else parent_domain
                    
                    enhanced_domain = {
                        "urn": domain_urn,
                        "name": properties.get("name", ""),
                        "description": properties.get("description", ""),
                        "type": "domain",
                        "sync_status": "REMOTE_ONLY",
                        "sync_status_display": "Remote Only",
                        
                        # Parent domain information for hierarchy
                        "parent_urn": parent_urn,
                        "parentDomain": parent_urn,  # Alternative field name
                        
                        # Extract ownership data
                        "ownership": domain.get("ownership"),
                        "owners_count": 0,
                        "owner_names": [],
                        
                        # Extract relationships data
                        "relationships": domain.get("relationships"),
                        "relationships_count": 0,
                        
                        # Store raw data
                        "raw_data": domain
                    }
                    
                    # Process ownership information
                    if enhanced_domain["ownership"] and enhanced_domain["ownership"].get("owners"):
                        owners = enhanced_domain["ownership"]["owners"]
                        enhanced_domain["owners_count"] = len(owners)
                        
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
                        enhanced_domain["owner_names"] = owner_names
                    
                    # Process relationships information
                    if enhanced_domain["relationships"] and enhanced_domain["relationships"].get("relationships"):
                        enhanced_domain["relationships_count"] = len(enhanced_domain["relationships"]["relationships"])
                    
                    enhanced_remote_domains[domain_urn] = enhanced_domain

            # Extract domain URNs that exist locally
            local_domain_urns = set(local_domains.values_list("deterministic_urn", flat=True))

            # Process local domains
            for domain in local_domains:
                domain_urn = str(domain.deterministic_urn)
                remote_match = enhanced_remote_domains.get(domain_urn)

                # Create enhanced local domain data
                local_domain_data = {
                    "id": domain.id,
                    "urn": domain_urn,
                    "name": domain.name,
                    "description": domain.description or "",
                    "type": "domain",
                    "sync_status": domain.sync_status,
                    "sync_status_display": domain.get_sync_status_display(),
                    
                    # Initialize parent information (local domains don't store parent currently)
                    "parent_urn": None,
                    "parentDomain": None,
                    
                    # Initialize ownership data (not stored locally for domains)
                    "ownership": None,
                    "owners_count": 0,
                    "owner_names": [],
                    
                    # Initialize relationships data (not stored locally for domains)
                    "relationships": None,
                    "relationships_count": 0,
                    
                    # Add local metadata
                    "created_at": domain.created_at.isoformat() if domain.created_at else None,
                    "updated_at": domain.updated_at.isoformat() if domain.updated_at else None,
                }

                if remote_match:
                    # Check if domain needs status update
                    local_description = domain.description or ""
                    remote_description = remote_match.get("description", "")

                    # Update sync status based on comparison
                    if local_description != remote_description:
                        if domain.sync_status != "MODIFIED":
                            domain.sync_status = "MODIFIED"
                            domain.save(update_fields=["sync_status"])
                            logger.debug(f"Updated domain {domain.name} status to MODIFIED")
                        local_domain_data["sync_status"] = "MODIFIED"
                        local_domain_data["sync_status_display"] = "Modified"
                    else:
                        if domain.sync_status != "SYNCED":
                            domain.sync_status = "SYNCED"
                            domain.save(update_fields=["sync_status"])
                            logger.debug(f"Updated domain {domain.name} status to SYNCED")
                        local_domain_data["sync_status"] = "SYNCED"
                        local_domain_data["sync_status_display"] = "Synced"

                    # Create combined data for synced items
                    combined_data = local_domain_data.copy()
                    # Add remote ownership and relationships data to combined data
                    combined_data.update({
                        "ownership": remote_match.get("ownership"),
                        "owners_count": remote_match.get("owners_count", 0),
                        "owner_names": remote_match.get("owner_names", []),
                        "relationships": remote_match.get("relationships"),
                        "relationships_count": remote_match.get("relationships_count", 0),
                        # Add parent domain information from remote
                        "parent_urn": remote_match.get("parent_urn"),
                        "parentDomain": remote_match.get("parentDomain"),
                    })

                    synced_items.append({
                        "local": local_domain_data,
                        "remote": remote_match,
                        "combined": combined_data
                    })
                else:
                    # Ensure local-only domains have correct status
                    if domain.sync_status != "LOCAL_ONLY":
                        domain.sync_status = "LOCAL_ONLY"
                        domain.save(update_fields=["sync_status"])
                        logger.debug(f"Updated domain {domain.name} status to LOCAL_ONLY")
                    local_domain_data["sync_status"] = "LOCAL_ONLY"
                    local_domain_data["sync_status_display"] = "Local Only"

                    local_only_items.append(local_domain_data)

            # Find domains that exist remotely but not locally
            remote_only_items = [
                enhanced_remote_domains[urn] 
                for urn in enhanced_remote_domains.keys() 
                if urn not in local_domain_urns
            ]

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
                f"Enhanced domain categorization: {len(synced_items)} synced, "
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
            logger.error(f"Error fetching enhanced remote domain data: {str(e)}")
            return JsonResponse(
                {"success": False, "error": f"Error fetching remote domains: {str(e)}"}
            )

    except Exception as e:
        logger.error(f"Error in get_remote_domains_data: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})
