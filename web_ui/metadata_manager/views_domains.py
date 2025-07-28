from django.shortcuts import render, redirect, get_object_or_404
from django.views import View
from django.contrib import messages
from django.http import JsonResponse, HttpResponse
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt
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
from utils.urn_utils import get_full_urn_from_name, generate_mutated_urn, get_mutation_config_for_environment
from utils.datahub_utils import get_datahub_client, test_datahub_connection, get_datahub_client_from_request
from utils.token_utils import get_token_from_env
from .models import Domain
from web_ui.models import GitSettings

# Import git integration
try:
    from web_ui.models import GitIntegration
    GIT_INTEGRATION_AVAILABLE = True
except ImportError:
    GitIntegration = None
    GIT_INTEGRATION_AVAILABLE = False

logger = logging.getLogger(__name__)


class DomainListView(View):
    """View to list and create domains"""
    
    def get(self, request):
        """Display list of domains"""
        try:
            logger.info("Starting DomainListView.get")
            
            # Get current connection to filter domains by connection
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            logger.debug(f"Using connection: {current_connection.name if current_connection else 'None'}")
            
            # Get domains relevant to current connection (database-only operation)
            # Include: domains with no connection (local-only) + domains with current connection (synced)
            # BUT: if there's a synced domain for current connection with same datahub_id, hide the other connection's version
            all_domains = Domain.objects.all().order_by("name")
            domains = filter_domains_by_connection(all_domains, current_connection)
            
            logger.debug(f"Found {len(domains)} domains relevant to current connection")
            
            # Get DataHub connection info (quick test only)
            logger.debug("Testing DataHub connection from DomainListView")
            connected, client = test_datahub_connection(request)
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
            parent_domain_urn = request.POST.get("parent_domain", "")
            icon_name = request.POST.get("icon_name", "folder")
            color_hex = request.POST.get("color_hex", "#6c757d")
            
            if not name:
                messages.error(request, "Domain name is required")
                return redirect("metadata_manager:domain_list")
            
            # Get current connection from request session
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            
            # Determine environment name
            current_environment = getattr(current_connection, 'environment', 'dev')
            
            # Generate URN using the same system as editable properties export
            from utils.urn_utils import generate_urn_for_new_entity
            urn = generate_urn_for_new_entity("domain", name, current_environment)
            
            # Check if domain with this URN already exists
            if Domain.objects.filter(urn=urn).exists():
                messages.error(request, f"Domain with name '{name}' already exists")
                return redirect("metadata_manager:domain_list")
            
            # Process ownership data
            ownership_data = None
            owners = request.POST.getlist("owners")
            ownership_types = request.POST.getlist("ownership_types")
            
            if owners and ownership_types and len(owners) == len(ownership_types):
                owners_list = []
                for owner_urn, ownership_type_urn in zip(owners, ownership_types):
                    if owner_urn and ownership_type_urn:  # Only add non-empty values
                        owners_list.append({
                            "owner_urn": owner_urn,
                            "ownership_type_urn": ownership_type_urn
                        })
                
                if owners_list:
                    ownership_data = {"owners": owners_list}
            
            # Create the domain
            domain = Domain.objects.create(
                name=name,
                description=description,
                urn=urn,
                sync_status="LOCAL_ONLY",
                parent_domain_urn=parent_domain_urn if parent_domain_urn else None,
                icon_name=icon_name,
                icon_style="solid",  # Default style
                icon_library="MATERIAL",  # Default library
                color_hex=color_hex,
                connection=None,  # No connection until synced to DataHub
                ownership_data=ownership_data,
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
        # Let 404 exceptions bubble up naturally
        domain = get_object_or_404(Domain, id=domain_id)
        
        try:
            
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
            client = get_datahub_client_from_request(request)
            if client and client.test_connection():
                domain_urn = domain.urn
                
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
            parent_domain_urn = request.POST.get("parent_domain", "")
            icon_name = request.POST.get("icon_name", "folder")
            color_hex = request.POST.get("color_hex", "#6c757d")
            
            if not name:
                messages.error(request, "Domain name is required")
                return redirect("metadata_manager:domain_detail", domain_id=domain_id)
            
            # Process ownership data
            ownership_data = None
            owners = request.POST.getlist("owners")
            ownership_types = request.POST.getlist("ownership_types")
            
            if owners and ownership_types and len(owners) == len(ownership_types):
                owners_list = []
                for owner_urn, ownership_type_urn in zip(owners, ownership_types):
                    if owner_urn and ownership_type_urn:  # Only add non-empty values
                        owners_list.append({
                            "owner_urn": owner_urn,
                            "ownership_type_urn": ownership_type_urn
                        })
                
                if owners_list:
                    ownership_data = {"owners": owners_list}
            
            # Update the domain
            domain.name = name
            domain.description = description
            domain.parent_domain_urn = parent_domain_urn if parent_domain_urn else None
            domain.icon_name = icon_name
            domain.icon_style = "solid"  # Default style
            domain.icon_library = "MATERIAL"  # Default library
            domain.color_hex = color_hex
            domain.ownership_data = ownership_data
            
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
                connected, client = test_datahub_connection(request)
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
                        
                        # Get current connection to set on domain
                        from web_ui.views import get_current_connection
                        current_connection = get_current_connection(request)
                        
                        domain.sync_status = "SYNCED"
                        domain.last_synced = timezone.now()
                        if current_connection:
                            domain.connection = current_connection
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
                        domain_urn=domain.urn,
                        name=domain.name,
                        description=domain.description or "",
                    )
                    
                    if result:
                        # Update domain status
                        
                        # Get current connection to set on domain
                        from web_ui.views import get_current_connection
                        current_connection = get_current_connection(request)
                        
                        domain.sync_status = "SYNCED"
                        domain.last_synced = timezone.now()
                        if current_connection:
                            domain.connection = current_connection
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
            if not GIT_INTEGRATION_AVAILABLE:
                return JsonResponse(
                    {"success": False, "error": "Git integration not available"}
                )
            
            domain = get_object_or_404(Domain, id=domain_id)
            
            # Get current branch and environment info
            settings = GitSettings.get_instance()
            current_branch = settings.current_branch if settings else "main"
            
            # Get environment name
            environment_name = "default"  # Default environment for domains
            
            commit_message = f"Add/update domain: {domain.name}"
            
            # Use GitIntegration to push to git (pass the domain object directly)
            logger.info(f"Staging domain {domain.id} to Git branch {current_branch}")
            git_integration = GitIntegration()
            result = git_integration.push_to_git(domain, commit_message)
            
            if result and result.get("success"):
                logger.info(f"Successfully staged domain {domain.id} to Git branch {current_branch}")
                
                response_data = {
                    "success": True,
                    "message": f'Domain "{domain.name}" staged for commit to branch {current_branch}. You can create a PR from the Git Repository tab.',
                    "environment": environment_name,
                    "branch": current_branch,
                    "redirect_url": "/github/repo/"
                }
                
                # Add file path info if available
                if "file_path" in result:
                    response_data["file_path"] = result["file_path"]
                
                return JsonResponse(response_data)
            else:
                # Failed to stage changes
                error_message = f'Failed to stage domain "{domain.name}"'
                if isinstance(result, dict) and "error" in result:
                    error_message += f": {result['error']}"
                
                logger.error(f"Failed to stage domain: {error_message}")
                return JsonResponse({"success": False, "error": error_message})
                
        except Exception as e:
            logger.error(f"Error adding domain to PR: {str(e)}")
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
                            urn=domain_data.get("urn"),
                            defaults={
                                "name": domain_data.get("name"),
                                "description": domain_data.get("description", ""),
                
                                "sync_status": "LOCAL_ONLY",
                            },
                        )
                        
                        if not created:
                            # Update existing domain
                            domain.name = domain_data.get("name")
                            domain.description = domain_data.get("description", "")

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
            connected, client = test_datahub_connection(request)
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
            connected, client = test_datahub_connection(request)
            
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
                            urn=domain_urn
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
                            existing_domain.sync_status = "SYNCED"
                            existing_domain.last_synced = timezone.now()
                            
                            # Update display properties from GraphQL
                            display_props = domain_data.get("displayProperties", {})
                            existing_domain.color_hex = display_props.get("colorHex")
                            icon_data = display_props.get("icon", {})
                            existing_domain.icon_name = icon_data.get("name")
                            existing_domain.icon_style = icon_data.get("style", "solid")
                            existing_domain.icon_library = icon_data.get("iconLibrary", "MATERIAL")
                            
                            # Update parent domain
                            parent_domains = domain_data.get("parentDomains", {})
                            if parent_domains.get("domains"):
                                existing_domain.parent_domain_urn = parent_domains["domains"][0].get("urn")
                            else:
                                existing_domain.parent_domain_urn = None
                            
                                                            # Update counts
                                ownership = domain_data.get("ownership", {})
                            
                            relationships = domain_data.get("relationships", {})
                            existing_domain.relationships_count = len(relationships.get("relationships", [])) if relationships else 0
                            
                            # Update entities count
                            try:
                                entities_result = client.find_entities_with_domain(domain_urn, count=1)
                                if entities_result:
                                    existing_domain.entities_count = entities_result.get("total", 0)
                            except Exception as e:
                                logger.warning(f"Error getting entities count for domain {domain_urn}: {str(e)}")
                                existing_domain.entities_count = 0
                            
                            # Store raw GraphQL data
                            existing_domain.raw_data = domain_data
                            
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
                                urn=domain_urn,

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
                                urn=domain_urn
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
                                existing_domain.sync_status = "SYNCED"
                                existing_domain.last_synced = timezone.now()
                                
                                # Update display properties from GraphQL
                                display_props = domain_data.get("displayProperties", {})
                                existing_domain.color_hex = display_props.get("colorHex")
                                icon_data = display_props.get("icon", {})
                                existing_domain.icon_name = icon_data.get("name")
                                existing_domain.icon_style = icon_data.get("style", "solid")
                                existing_domain.icon_library = icon_data.get("iconLibrary", "MATERIAL")
                                
                                # Update parent domain
                                parent_domains = domain_data.get("parentDomains", {})
                                if parent_domains.get("domains"):
                                    existing_domain.parent_domain_urn = parent_domains["domains"][0].get("urn")
                                else:
                                    existing_domain.parent_domain_urn = None
                                
                                # Update counts
                                ownership = domain_data.get("ownership", {})
                                
                                relationships = domain_data.get("relationships", {})
                                existing_domain.relationships_count = len(relationships.get("relationships", [])) if relationships else 0
                                
                                # Update entities count
                                try:
                                    entities_result = client.find_entities_with_domain(domain_urn, count=1)
                                    if entities_result:
                                        existing_domain.entities_count = entities_result.get("total", 0)
                                except Exception as e:
                                    logger.warning(f"Error getting entities count for domain {domain_urn}: {str(e)}")
                                    existing_domain.entities_count = 0
                                
                                # Store raw GraphQL data
                                existing_domain.raw_data = domain_data
                                
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
                                # Extract display properties from GraphQL
                                display_props = domain_data.get("displayProperties", {})
                                icon_data = display_props.get("icon", {})
                                
                                # Extract parent domain
                                parent_domain_urn = None
                                parent_domains = domain_data.get("parentDomains", {})
                                if parent_domains.get("domains"):
                                    parent_domain_urn = parent_domains["domains"][0].get("urn")
                                
                                # Extract counts
                                ownership = domain_data.get("ownership", {})
                                
                                relationships = domain_data.get("relationships", {})
                                relationships_count = len(relationships.get("relationships", [])) if relationships else 0
                                
                                # Get entities count
                                entities_count = 0
                                try:
                                    entities_result = client.find_entities_with_domain(domain_urn, count=1)
                                    if entities_result:
                                        entities_count = entities_result.get("total", 0)
                                except Exception as e:
                                    logger.warning(f"Error getting entities count for domain {domain_urn}: {str(e)}")
                                
                                Domain.objects.create(
                                    name=domain_data.get("name"),
                                    description=domain_data.get("description") or "",
                                    urn=domain_urn,
                                    sync_status="SYNCED",
                                    last_synced=timezone.now(),
                                    # Add new fields
                                    parent_domain_urn=parent_domain_urn,
                                    color_hex=display_props.get("colorHex"),
                                    icon_name=icon_data.get("name"),
                                    icon_style=icon_data.get("style", "solid"),
                                    icon_library=icon_data.get("iconLibrary", "MATERIAL"),
                                    relationships_count=relationships_count,
                                    entities_count=entities_count,
                                    raw_data=domain_data,
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


def filter_domains_by_connection(all_domains, current_connection):
    """
    Filter and return domains based on connection logic - only one row per datahub_id:
    1. If domain is available for current connection_id -> show in synced
    2. If domain is available for different connection_id -> show in local-only  
    3. If domain has no connection_id -> show in local-only
    Priority: current connection > different connection > no connection
    
    Nulls/empty values/empty lists should match to "" or [] for determining if the domain is consistent with the remote datahub
    """
    domains = []
    
    # Group domains by datahub_id to implement the single-row-per-datahub_id logic
    domains_by_datahub_id = {}
    for domain in all_domains:
        datahub_id = domain.datahub_id or f'no_datahub_id_{domain.id}'  # Use unique identifier for domains without datahub_id
        if datahub_id not in domains_by_datahub_id:
            domains_by_datahub_id[datahub_id] = []
        domains_by_datahub_id[datahub_id].append(domain)
    
    # For each datahub_id, select the best domain based on connection priority
    for datahub_id, domain_list in domains_by_datahub_id.items():
        if len(domain_list) == 1:
            # Only one domain with this datahub_id
            domains.append(domain_list[0])
        else:
            # Multiple domains with same datahub_id - apply priority logic
            current_connection_domains = [d for d in domain_list if d.connection == current_connection]
            different_connection_domains = [d for d in domain_list if d.connection is not None and d.connection != current_connection]
            no_connection_domains = [d for d in domain_list if d.connection is None]
            
            if current_connection_domains:
                # Priority 1: Domain for current connection
                domains.append(current_connection_domains[0])
            elif different_connection_domains:
                # Priority 2: Domain for different connection (will appear as local-only)
                domains.append(different_connection_domains[0])
            elif no_connection_domains:
                # Priority 3: Domain with no connection (will appear as local-only)
                domains.append(no_connection_domains[0])
    
    return domains

def get_remote_domains_data(request):
    """AJAX endpoint to get enhanced remote domains data with ownership and relationships"""
    try:
        logger.info("Loading enhanced remote domains data via AJAX")

        # Get DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})

        # Get current connection to filter domains by connection
        from web_ui.views import get_current_connection
        current_connection = get_current_connection(request)
        logger.debug(f"Using connection: {current_connection.name if current_connection else 'None'}")
        
        # Get ALL local domains - we'll categorize them based on connection logic
        all_local_domains = Domain.objects.all().order_by("name")
        logger.debug(f"Found {all_local_domains.count()} total local domains")
        
        # Separate domains based on connection logic:
        # 1. Domains with no connection (LOCAL_ONLY) - should appear as local-only
        # 2. Domains with current connection (SYNCED) - should appear as synced if they match remote
        # 3. Domains with different connection - should appear as local-only relative to current connection
        #    BUT: if there's a synced domain for current connection with same datahub_id, hide the other connection's version
        local_domains = filter_domains_by_connection(all_local_domains, current_connection)
        
        logger.debug(f"Filtered to {len(local_domains)} domains relevant to current connection")

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
            remote_domains_count = len(remote_domains) if remote_domains else 0
            logger.debug(f"Fetched {remote_domains_count} remote domains")
            
            if remote_domains_count == 0:
                logger.warning("No remote domains returned from DataHub")
            elif remote_domains is None:
                logger.warning("Remote domains returned None from DataHub")

            # Enhance remote domains with ownership and relationship data
            enhanced_remote_domains = {}
            for domain in remote_domains or []:
                if not domain:  # Skip None domains
                    continue
                try:
                    domain_urn = domain.get("urn")
                    if not domain_urn:
                        continue
                    # Extract basic properties
                    properties = domain.get("properties") or {}
                    
                    # Extract parent domain information from multiple sources
                    parent_urn = None
                    
                    # Try parentDomain first
                    parent_domain = domain.get("parentDomain")
                    if parent_domain:
                        if isinstance(parent_domain, dict):
                            parent_urn = parent_domain.get("urn") if parent_domain else None
                        else:
                            parent_urn = parent_domain
                    
                    # If not found, try parentDomains structure
                    if not parent_urn:
                        parent_domains = domain.get("parentDomains")
                        if parent_domains and isinstance(parent_domains, dict) and parent_domains.get("domains"):
                            domains_list = parent_domains["domains"]
                            if domains_list and len(domains_list) > 0:
                                parent_domain = domains_list[0]
                                if isinstance(parent_domain, dict):
                                    parent_urn = parent_domain.get("urn") if parent_domain else None
                                else:
                                    parent_urn = parent_domain
                    
                    # If still not found, try incoming parent relationships
                    if not parent_urn:
                        parent_relationships = domain.get("parentRelationships", {})
                        if parent_relationships and parent_relationships.get("relationships"):
                            relationships_list = parent_relationships["relationships"]
                            for rel in relationships_list:
                                if rel and rel.get("type") in ["ParentOf", "Contains"] and rel.get("entity"):
                                    parent_entity = rel["entity"]
                                    if parent_entity.get("type") == "DOMAIN":
                                        parent_urn = parent_entity.get("urn")
                                        break
                    
                    
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
    
                        "owner_names": [],
                        
                        # Store raw data
                        "raw_data": domain
                    }
                    
                    # Process ownership information
                    if enhanced_domain["ownership"] and enhanced_domain["ownership"].get("owners"):
                        owners = enhanced_domain["ownership"]["owners"] or []

                        # Calculate owners count
                        enhanced_domain["owners_count"] = len(owners)
                        
                        # Extract owner names for display
                        owner_names = []
                        for owner_info in owners or []:
                            if not owner_info:
                                continue
                            owner = owner_info.get("owner") or {}
                            owner_props = owner.get("properties") or {}
                            if owner_props:
                                name = (
                                    owner_props.get("displayName") or
                                    owner.get("username") or
                                    owner.get("name") or
                                    "Unknown"
                                )
                            else:
                                name = owner.get("username") or owner.get("name") or "Unknown"
                            owner_names.append(name)
                        enhanced_domain["owner_names"] = owner_names
                    else:
                        enhanced_domain["owners_count"] = 0
                        enhanced_domain["owner_names"] = []
                    
                    # Domains don't have relationships like tags do
                    
                    # Get entities count from GraphQL response
                    entities_data = domain.get("entities")
                    if entities_data and isinstance(entities_data, dict):
                        enhanced_domain["entities_count"] = entities_data.get("total", 0)
                    else:
                        # Fallback to direct entities_count field if available
                        enhanced_domain["entities_count"] = domain.get("entities_count", 0)
                    
                    # Process structured properties
                    structured_props = domain.get("structuredProperties", {})
                    structured_properties_list = []
                    if structured_props and structured_props.get("properties"):
                        structured_properties_list = structured_props["properties"]
                    enhanced_domain["structured_properties"] = structured_properties_list
                    enhanced_domain["structured_properties_count"] = len(structured_properties_list)
                    
                    # Extract display properties
                    display_props = domain.get("displayProperties") or {}
                    enhanced_domain["color_hex"] = display_props.get("colorHex") if display_props else None
                    icon_data = display_props.get("icon") or {} if display_props else {}
                    enhanced_domain["icon_name"] = icon_data.get("name") if icon_data else None
                    enhanced_domain["icon_style"] = icon_data.get("style", "solid") if icon_data else "solid"
                    enhanced_domain["icon_library"] = icon_data.get("iconLibrary", "MATERIAL") if icon_data else "MATERIAL"
                    
                    enhanced_remote_domains[domain_urn] = enhanced_domain
                
                except Exception as e:
                    import traceback
                    logger.error(f"Error processing domain {domain_urn if 'domain_urn' in locals() else 'unknown'}: {str(e)}")
                    logger.error(f"Traceback: {traceback.format_exc()}")
                    continue

            # Extract domain URNs that exist locally
            local_domain_urns = set(domain.urn for domain in local_domains)
            
            # Track which remote domains have been matched to avoid duplicates
            matched_remote_urns = set()

            # Process local domains
            for domain in local_domains:
                domain_urn = str(domain.urn)
                
                # Create connection context info for frontend decision making
                connection_context = {
                    "has_current_connection": current_connection is not None,
                    "domain_belongs_to_current_connection": domain.connection == current_connection,
                    "domain_has_connection": domain.connection is not None,
                    "current_connection_name": current_connection.name if current_connection else None,
                    "domain_connection_name": domain.connection.name if domain.connection else None,
                }
                
                # Try to find remote match - first by URN, then by datahub_id, then by name
                remote_match = None
                
                # First try direct URN match (for local-only domains)
                remote_match = enhanced_remote_domains.get(domain_urn)
                
                # If no direct URN match, try matching by datahub_id (for synced domains)
                if not remote_match and hasattr(domain, 'datahub_id') and domain.datahub_id:
                    datahub_urn = f"urn:li:domain:{domain.datahub_id}"
                    remote_match = enhanced_remote_domains.get(datahub_urn)
                
                # If still no match, try matching by name (fallback)
                if not remote_match and domain.name:
                    for remote_urn, remote_data in enhanced_remote_domains.items():
                        if remote_data.get("name") == domain.name:
                            remote_match = remote_data
                            break

                # Create enhanced local domain data
                local_domain_data = {
                    "id": domain.id,
                    "database_id": str(domain.id),  # Explicitly add database_id for clarity
                    "urn": domain_urn,
                    "name": domain.name,
                    "description": domain.description or "",  # Normalize null to empty string
                    "sync_status": domain.sync_status,
                    "sync_status_display": domain.get_sync_status_display(),
                    "color_hex": domain.color_hex,
                    "icon_name": domain.icon_name,
                    "icon_style": domain.icon_style or "solid",  # Normalize null to default
                    "icon_library": domain.icon_library or "MATERIAL",  # Normalize null to default
                    "type": "domain",
                    "parent_urn": domain.parent_domain_urn,
                    "parentDomain": domain.parent_domain_urn,
                    "owners_count": 0,
                    "owner_names": [],
                    "entities_count": 0,
                    "structured_properties": [],
                    "structured_properties_count": 0,
                    "connection_context": connection_context,  # Add connection context for frontend action determination
                    "has_remote_match": bool(remote_match),  # Whether this domain has a remote match
                }

                if remote_match:
                    # Mark this remote URN as matched to avoid duplicates
                    matched_remote_urns.add(remote_match["urn"])
                    
                    # Domain has a remote match - categorize as synced or local-only based on connection
                    # With single-row-per-datahub_id logic:
                    # - Synced: Domain belongs to current connection AND (exists remotely OR was recently synced)
                    # - Local-only: Domain belongs to different connection OR no connection OR (no remote match AND not recently synced)
                    
                    # Check if domain was recently synced (within last 30 seconds)
                    from django.utils import timezone
                    from datetime import timedelta
                    
                    recently_synced = False
                    if hasattr(domain, 'last_synced') and domain.last_synced:
                        time_since_sync = timezone.now() - domain.last_synced
                        recently_synced = time_since_sync.total_seconds() < 30
                    
                    # If recently synced, preserve the SYNCED status
                    if recently_synced and domain.sync_status == "SYNCED":
                        local_domain_data["sync_status"] = "SYNCED"
                        local_domain_data["sync_status_display"] = "Synced"
                    else:
                        # Check if domain needs status update based on content comparison
                        # Normalize null/empty values for comparison
                        local_description = domain.description or ""
                        remote_description = remote_match.get("description", "")
                        
                        # Normalize other fields that might be null/empty
                        local_parent_urn = domain.parent_domain_urn or ""
                        remote_parent_urn = remote_match.get("parent_urn", "")

                        # Update sync status based on comparison (handle nulls/empty values)
                        if (local_description != remote_description or 
                            local_parent_urn != remote_parent_urn):
                            if domain.sync_status != "MODIFIED":
                                domain.sync_status = "MODIFIED"
                                domain.save(update_fields=["sync_status"])
                            local_domain_data["sync_status"] = "MODIFIED"
                            local_domain_data["sync_status_display"] = "Modified"
                        else:
                            if domain.sync_status != "SYNCED":
                                domain.sync_status = "SYNCED"
                                domain.save(update_fields=["sync_status"])
                            local_domain_data["sync_status"] = "SYNCED"
                            local_domain_data["sync_status_display"] = "Synced"

                    # Create combined data for synced items
                    combined_data = local_domain_data.copy()
                    # Add remote ownership and relationships data to combined data
                    combined_data.update({
                        "ownership": remote_match.get("ownership"),
                        "owners_count": remote_match.get("owners_count", 0),
                        "owner_names": remote_match.get("owner_names", []),
                        "entities_count": remote_match.get("entities_count", 0),
                        # Add structured properties from remote
                        "structured_properties": remote_match.get("structured_properties", []),
                        "structured_properties_count": remote_match.get("structured_properties_count", 0),
                        # Add parent domain information from remote
                        "parent_urn": remote_match.get("parent_urn"),
                        "parentDomain": remote_match.get("parentDomain"),
                        # Add display properties from remote
                        "color_hex": remote_match.get("color_hex"),
                        "icon_name": remote_match.get("icon_name"),
                        "icon_style": remote_match.get("icon_style"),
                        "icon_library": remote_match.get("icon_library"),
                        # Add raw data from remote
                        "raw_data": remote_match.get("raw_data", {}),
                    })

                    synced_items.append({
                        "local": local_domain_data,
                        "remote": remote_match,
                        "combined": combined_data
                    })
                else:
                    # Domain is local-only relative to current connection
                    # This includes:
                    # 1. Domains with no connection (true local-only)
                    # 2. Domains with different connection (local-only relative to current connection)
                    # 3. Domains with current connection but no remote match
                    
                    # Check if the domain was recently synced to avoid overriding fresh sync status
                    from django.utils import timezone
                    from datetime import timedelta
                    
                    recently_synced = (
                        domain.last_synced and 
                        domain.last_synced > timezone.now() - timedelta(minutes=5)  # 5 minute grace period
                    )
                    
                    # CRITICAL: Only update sync status for domains that belong to current connection
                    # Domains from other connections should maintain their original sync_status
                    if domain.connection == current_connection:
                        # Only update status for domains belonging to current connection
                        expected_status = "LOCAL_ONLY"
                        if not remote_match:
                            # Domain was synced to current connection but no longer exists remotely
                            expected_status = "REMOTE_DELETED"
                        
                        # Only update sync status if it wasn't recently synced and status is different
                        if not recently_synced and domain.sync_status != expected_status:
                            domain.sync_status = expected_status
                            domain.save(update_fields=["sync_status"])
                            logger.debug(f"Updated domain {domain.name} status to {expected_status}")
                    
                    # Update local domain data with current status
                    local_domain_data.update({
                        "sync_status": domain.sync_status,
                        "sync_status_display": domain.get_sync_status_display(),
                        "connection_context": connection_context,  # Keep the connection context
                        "has_remote_match": bool(remote_match),  # Whether this domain has a remote match
                    })
                    
                    local_only_items.append(local_domain_data)

            # Find domains that exist remotely but not locally (and weren't matched to any local domain)
            remote_only_items = [
                enhanced_remote_domains[urn] 
                for urn in enhanced_remote_domains.keys() 
                if urn not in local_domain_urns and urn not in matched_remote_urns
            ]

            # Calculate statistics
            total_items = len(synced_items) + len(local_only_items) + len(remote_only_items)
            synced_count = len(synced_items)
            owned_items = sum(1 for item in synced_items + local_only_items + remote_only_items 
                            if (item.get("combined", item) if "combined" in item else item).get("owner_names", []))
            items_with_entities = sum(1 for item in synced_items + local_only_items + remote_only_items 
                                    if (item.get("combined", item) if "combined" in item else item).get("entities_count", 0) > 0)
            items_with_structured_properties = sum(1 for item in synced_items + local_only_items + remote_only_items 
                                                 if (item.get("combined", item) if "combined" in item else item).get("structured_properties_count", 0) > 0)

            statistics = {
                "total_items": total_items,
                "synced_count": synced_count,
                "owned_items": owned_items,
                "items_with_entities": items_with_entities,
                "items_with_structured_properties": items_with_structured_properties,
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


@require_POST
def sync_domain_to_local(request, domain_id=None):
    """AJAX endpoint to sync a domain from DataHub to local storage"""
    try:
        logger.info(f"Syncing domain to local - domain_id: {domain_id}")
        
        # Get the domain URN from the request
        if domain_id:
            # From detail view - get original_urn from database
            try:
                domain = get_object_or_404(Domain, id=domain_id)
                original_urn = domain.urn
            except Domain.DoesNotExist:
                return JsonResponse({"success": False, "error": "Domain not found"})
        else:
            # From list view - get domain_urn from POST body
            domain_urn = request.POST.get("domain_urn")
            if not domain_urn:
                return JsonResponse({"success": False, "error": "Domain URN required"})
            original_urn = domain_urn

        # Test DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})

        # Fetch domain from DataHub
        logger.debug(f"Fetching domain from DataHub: {original_urn}")
        remote_domain = client.get_domain(original_urn)
        if not remote_domain:
            return JsonResponse({"success": False, "error": "Domain not found in DataHub"})

        # Extract domain properties
        properties = remote_domain.get("properties", {})
        domain_name = properties.get("name", "")
        domain_description = properties.get("description", "")
        
        if not domain_name:
            return JsonResponse({"success": False, "error": "Domain name not found in DataHub"})

        # Extract datahub_id from URN (e.g., "5c5f8f55-b2af-46e5-b08e-c9ccfa99434b" from "urn:li:domain:5c5f8f55-b2af-46e5-b08e-c9ccfa99434b")
        datahub_id = original_urn.split(":")[-1] if original_urn else None
        
        # Get current connection from request session
        from web_ui.views import get_current_connection
        current_connection = get_current_connection(request)
        current_environment = getattr(current_connection, 'environment', 'dev')

        # When syncing FROM DataHub TO local, preserve the original DataHub URN
        # Do NOT generate a new deterministic URN - that's for NEW entities created in web UI
        local_urn = domain_urn

        # Prepare ownership data
        ownership_data = None
        ownership = remote_domain.get("ownership", {})
        if ownership and ownership.get("owners"):
            # Convert DataHub ownership format to our format
            owners_list = []
            for owner_info in ownership["owners"]:
                owner = owner_info.get("owner", {})
                ownership_type = owner_info.get("ownershipType", {})
                
                owner_urn = owner.get("urn", "")
                ownership_type_urn = ownership_type.get("urn", "urn:li:ownershipType:__system__technical_owner")
                
                if owner_urn:
                    owners_list.append({
                        "owner_urn": owner_urn,
                        "ownership_type_urn": ownership_type_urn
                    })
            
            if owners_list:
                ownership_data = {"owners": owners_list}

        # Create or update local domain with SYNCED status
        domain, created = Domain.objects.update_or_create(
            urn=local_urn,
            defaults={
                "name": domain_name,
                "description": domain_description,
                "sync_status": "SYNCED",  # Set to SYNCED when syncing from DataHub
                "last_synced": timezone.now(),
                "raw_data": remote_domain,
                "datahub_id": datahub_id,
                "connection": current_connection,
                "ownership_data": ownership_data,
            },
        )

        # Extract and store additional domain properties with parent domain resolution
        parent_domain = remote_domain.get("parentDomain")
        if parent_domain:
            parent_urn = parent_domain.get("urn") if isinstance(parent_domain, dict) else parent_domain
            
            # Try to resolve parent domain URN to local deterministic URN
            resolved_parent_urn = None
            if parent_urn:
                # First try to find a local domain with the same DataHub URN
                try:
                    parent_datahub_id = parent_urn.split(":")[-1] if parent_urn else None
                    if parent_datahub_id:
                        local_parent = Domain.objects.filter(datahub_id=parent_datahub_id).first()
                        if local_parent:
                            resolved_parent_urn = local_parent.urn
                except Exception as e:
                    logger.debug(f"Error resolving parent domain by datahub_id: {str(e)}")
                
                # If not found by datahub_id, try to find by exact URN match
                if not resolved_parent_urn:
                    try:
                        local_parent = Domain.objects.filter(urn=parent_urn).first()
                        if local_parent:
                            resolved_parent_urn = local_parent.urn
                    except Exception as e:
                        logger.debug(f"Error resolving parent domain by URN: {str(e)}")
                
                # If still not found, try to find by extracting name from remote parent and generating deterministic URN
                if not resolved_parent_urn:
                    try:
                        # Try to get the parent domain from DataHub to extract its name
                        remote_parent = client.get_domain(parent_urn)
                        if remote_parent:
                            parent_name = remote_parent.get("properties", {}).get("name")
                            if parent_name:
                                # Generate URN using the same system as editable properties export
                                parent_deterministic_urn = generate_urn_for_new_entity("domain", parent_name, current_environment)
                                
                                # Check if a domain with this deterministic URN exists
                                local_parent = Domain.objects.filter(urn=parent_deterministic_urn).first()
                                if local_parent:
                                    resolved_parent_urn = local_parent.urn
                    except Exception as e:
                        logger.debug(f"Error resolving parent domain by name: {str(e)}")
                
                # Use the resolved URN if found, otherwise use the original DataHub URN as fallback
                domain.parent_domain_urn = resolved_parent_urn or parent_urn
                
                logger.info(f"Parent domain resolution for {domain.name}:")
                logger.info(f"  Original parent URN: {parent_urn}")
                logger.info(f"  Resolved parent URN: {resolved_parent_urn}")
                logger.info(f"  Final parent URN: {domain.parent_domain_urn}")
                
                if resolved_parent_urn and resolved_parent_urn != parent_urn:
                    logger.info(f"✅ Successfully resolved parent domain URN from {parent_urn} to {resolved_parent_urn}")
                else:
                    logger.warning(f"⚠️ Could not resolve parent domain URN {parent_urn} to local deterministic URN")
        else:
            domain.parent_domain_urn = None

        # Store display properties if available
        display_props = remote_domain.get("displayProperties", {})
        if display_props:
            domain.color_hex = display_props.get("colorHex")
            icon = display_props.get("icon", {})
            if icon:
                domain.icon_name = icon.get("name")
                domain.icon_style = icon.get("style", "solid")
                domain.icon_library = icon.get("iconLibrary", "MATERIAL")

        # Store entities count from remote domain data if available
        entities_info = remote_domain.get("entities", {})
        domain.entities_count = entities_info.get("total", 0) if entities_info else 0

        domain.save()

        logger.info(f"Successfully synced domain '{domain_name}' to local storage")
        
        return JsonResponse({
            "success": True,
            "message": f"Domain '{domain_name}' synced successfully",
            "domain_id": str(domain.id),
            "domain_name": domain_name,
        })

    except Exception as e:
        logger.error(f"Error syncing domain to local: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_POST
def resync_domain(request, domain_id):
    """AJAX endpoint to resync a domain with DataHub"""
    try:
        logger.info(f"Resyncing domain: {domain_id}")
        
        # Get the domain
        domain = get_object_or_404(Domain, id=domain_id)
        
        # Test DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})

        # Use the original DataHub URN for synced domains, or the deterministic URN for local domains
        if hasattr(domain, 'datahub_id') and domain.datahub_id:
            # For synced domains, reconstruct the original DataHub URN
            domain_urn = f"urn:li:domain:{domain.datahub_id}"
        else:
            # For local-only domains, use the deterministic URN
            domain_urn = domain.urn
        
        # Fetch domain from DataHub
        logger.debug(f"Fetching domain from DataHub: {domain_urn}")
        remote_domain = client.get_domain(domain_urn)
        if not remote_domain:
            return JsonResponse({"success": False, "error": "Domain not found in DataHub"})

        # Extract domain properties
        properties = remote_domain.get("properties", {})
        domain_name = properties.get("name", "")
        domain_description = properties.get("description", "")

        # Extract datahub_id from URN if not already set
        if not domain.datahub_id and domain_urn:
            domain.datahub_id = domain_urn.split(":")[-1]
        
        # Get current connection from request session
        from web_ui.views import get_current_connection
        current_connection = get_current_connection(request)
        if current_connection:
            domain.connection = current_connection

        # Prepare ownership data
        ownership_data = None
        ownership = remote_domain.get("ownership", {})
        if ownership and ownership.get("owners"):
            # Convert DataHub ownership format to our format
            owners_list = []
            for owner_info in ownership["owners"]:
                owner = owner_info.get("owner", {})
                ownership_type = owner_info.get("ownershipType", {})
                
                owner_urn = owner.get("urn", "")
                ownership_type_urn = ownership_type.get("urn", "urn:li:ownershipType:__system__technical_owner")
                
                if owner_urn:
                    owners_list.append({
                        "owner_urn": owner_urn,
                        "ownership_type_urn": ownership_type_urn
                    })
            
            if owners_list:
                ownership_data = {"owners": owners_list}

        # Update domain with latest data from DataHub
        domain.name = domain_name or domain.name
        domain.description = domain_description
        domain.sync_status = "SYNCED"
        domain.last_synced = timezone.now()
        domain.raw_data = remote_domain
        domain.ownership_data = ownership_data

        # Update additional properties with parent domain resolution
        parent_domain = remote_domain.get("parentDomain")
        if parent_domain:
            parent_urn = parent_domain.get("urn") if isinstance(parent_domain, dict) else parent_domain
            
            # Try to resolve parent domain URN to local deterministic URN
            resolved_parent_urn = None
            if parent_urn:
                # First try to find a local domain with the same DataHub URN
                try:
                    parent_datahub_id = parent_urn.split(":")[-1] if parent_urn else None
                    if parent_datahub_id:
                        local_parent = Domain.objects.filter(datahub_id=parent_datahub_id).first()
                        if local_parent:
                            resolved_parent_urn = local_parent.urn
                except Exception as e:
                    logger.debug(f"Error resolving parent domain by datahub_id: {str(e)}")
                
                # If not found by datahub_id, try to find by exact URN match
                if not resolved_parent_urn:
                    try:
                        local_parent = Domain.objects.filter(urn=parent_urn).first()
                        if local_parent:
                            resolved_parent_urn = local_parent.urn
                    except Exception as e:
                        logger.debug(f"Error resolving parent domain by URN: {str(e)}")
                
                # If still not found, try to find by extracting name from remote parent and generating deterministic URN
                if not resolved_parent_urn:
                    try:
                        # Try to get the parent domain from DataHub to extract its name
                        remote_parent = client.get_domain(parent_urn)
                        if remote_parent:
                            parent_name = remote_parent.get("properties", {}).get("name")
                            if parent_name:
                                # Generate URN using the same system as editable properties export
                                parent_deterministic_urn = generate_urn_for_new_entity("domain", parent_name, current_environment)
                                
                                # Check if a domain with this deterministic URN exists
                                local_parent = Domain.objects.filter(urn=parent_deterministic_urn).first()
                                if local_parent:
                                    resolved_parent_urn = local_parent.urn
                    except Exception as e:
                        logger.debug(f"Error resolving parent domain by name: {str(e)}")
                
                # Use the resolved URN if found, otherwise use the original DataHub URN as fallback
                domain.parent_domain_urn = resolved_parent_urn or parent_urn
                
                logger.info(f"Parent domain resolution for {domain.name}:")
                logger.info(f"  Original parent URN: {parent_urn}")
                logger.info(f"  Resolved parent URN: {resolved_parent_urn}")
                logger.info(f"  Final parent URN: {domain.parent_domain_urn}")
                
                if resolved_parent_urn and resolved_parent_urn != parent_urn:
                    logger.info(f"✅ Successfully resolved parent domain URN from {parent_urn} to {resolved_parent_urn}")
                else:
                    logger.warning(f"⚠️ Could not resolve parent domain URN {parent_urn} to local deterministic URN")
        else:
            domain.parent_domain_urn = None

        # Update display properties
        display_props = remote_domain.get("displayProperties", {})
        if display_props:
            domain.color_hex = display_props.get("colorHex")
            icon = display_props.get("icon", {})
            if icon:
                domain.icon_name = icon.get("name")
                domain.icon_style = icon.get("style", "solid")
                domain.icon_library = icon.get("iconLibrary", "MATERIAL")

        # Update entities count from remote domain data if available
        entities_info = remote_domain.get("entities", {})
        domain.entities_count = entities_info.get("total", 0) if entities_info else 0

        domain.save()

        logger.info(f"Successfully resynced domain '{domain.name}'")
        
        return JsonResponse({
            "success": True,
            "message": f"Domain '{domain.name}' resynced successfully",
        })

    except Exception as e:
        logger.error(f"Error resyncing domain: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_POST
def push_domain_to_datahub(request, domain_id):
    """AJAX endpoint to push a domain to DataHub"""
    try:
        logger.info(f"Pushing domain to DataHub: {domain_id}")
        
        # Get the domain
        domain = get_object_or_404(Domain, id=domain_id)
        
        # Test DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})

        # Use the original DataHub URN for pushing if domain has datahub_id, otherwise use deterministic URN
        if hasattr(domain, 'datahub_id') and domain.datahub_id:
            # For synced domains, use the original DataHub URN
            target_urn = f"urn:li:domain:{domain.datahub_id}"
        else:
            # For local-only domains, use the deterministic URN
            target_urn = domain.urn
        
        try:
            logger.debug(f"Pushing domain to DataHub: {domain.name} with URN: {target_urn}")
            
            # Track what needs to be updated
            updates_needed = []
            update_success = True
            domain_created = False
            
            # First, check if domain exists in DataHub
            existing_domain = client.get_domain(target_urn)
            
            if not existing_domain:
                # Domain doesn't exist, create it first
                logger.info(f"Domain {target_urn} doesn't exist in DataHub, creating it")
                
                # Extract domain ID from URN for creation
                domain_id = target_urn.split(":")[-1]
                
                # Get parent domain URN if exists
                parent_domain_urn = None
                if hasattr(domain, 'parent_urn') and domain.parent_urn:
                    parent_domain_urn = domain.parent_urn
                
                # Create domain
                created_urn = client.create_domain(
                    domain_id=domain_id,
                    name=domain.name,
                    description=domain.description or "",
                    parent_domain_urn=parent_domain_urn
                )
                
                if created_urn:
                    logger.info(f"Successfully created domain in DataHub: {created_urn}")
                    target_urn = created_urn  # Use the returned URN
                    updates_needed.append("created")
                    domain_created = True
                else:
                    logger.error(f"Failed to create domain {domain.name} in DataHub")
                    return JsonResponse({"success": False, "error": f"Failed to create domain {domain.name} in DataHub"})
            else:
                logger.debug(f"Domain {target_urn} already exists in DataHub, updating properties")
            
            # Update description if it exists and domain wasn't just created (creation includes description)
            if domain.description and not domain_created:
                if client.update_domain_description(target_urn, domain.description):
                    updates_needed.append("description")
                else:
                    update_success = False
                    logger.error(f"Failed to update description for domain {domain.name}")
            
            # Update display properties if they exist
            if domain.color_hex or domain.icon_name:
                color_hex = domain.color_hex if domain.color_hex else None
                icon_data = None
                
                if domain.icon_name:
                    icon_data = {"name": domain.icon_name}
                    if domain.icon_style:
                        icon_data["style"] = domain.icon_style
                    # DataHub only supports MATERIAL icon library
                    icon_data["iconLibrary"] = "MATERIAL"
                
                if client.update_domain_display_properties(target_urn, color_hex=color_hex, icon=icon_data):
                    updates_needed.append("display properties")
                else:
                    update_success = False
                    logger.error(f"Failed to update display properties for domain {domain.name}")
            
            # Update ownership if it exists
            if hasattr(domain, 'ownership_data') and domain.ownership_data and domain.ownership_data.get('owners'):
                for owner_info in domain.ownership_data['owners']:
                    owner_urn = owner_info.get('owner_urn')
                    ownership_type_urn = owner_info.get('ownership_type_urn', 'urn:li:ownershipType:__system__business_owner')
                    
                    if owner_urn:
                        if client.add_domain_owner(target_urn, owner_urn, ownership_type_urn):
                            if "ownership" not in updates_needed:
                                updates_needed.append("ownership")
                        else:
                            update_success = False
                            logger.error(f"Failed to add owner {owner_urn} to domain {domain.name}")
            
            if update_success:
                # Update domain status
                
                # Get current connection to set on domain
                from web_ui.views import get_current_connection
                current_connection = get_current_connection(request)
                
                domain.sync_status = "SYNCED"
                domain.last_synced = timezone.now()
                if current_connection:
                    domain.connection = current_connection
                # Store the DataHub ID if this was a local-only domain
                if not hasattr(domain, 'datahub_id') or not domain.datahub_id:
                    domain.datahub_id = target_urn.split(":")[-1]
                domain.save(update_fields=["sync_status", "last_synced", "connection", "datahub_id"])
                
                updates_msg = f" (updated: {', '.join(updates_needed)})" if updates_needed else ""
                logger.info(f"Successfully pushed domain '{domain.name}' to DataHub{updates_msg}")
                
                return JsonResponse({
                    "success": True,
                    "message": f"Domain '{domain.name}' pushed to DataHub successfully{updates_msg}",
                })
            else:
                return JsonResponse({"success": False, "error": "Some domain updates failed"})
                
        except Exception as e:
            logger.error(f"Error pushing domain to DataHub: {str(e)}")
            return JsonResponse({"success": False, "error": str(e)})

    except Exception as e:
        logger.error(f"Error in push_domain_to_datahub: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_POST
def delete_remote_domain(request, domain_id):
    """AJAX endpoint to delete a domain from DataHub"""
    try:
        logger.info(f"Deleting remote domain: {domain_id}")
        
        # Get the domain URN from the request
        domain_urn = request.POST.get("domain_urn")
        if not domain_urn:
            return JsonResponse({"success": False, "error": "Domain URN required"})
        
        # Test DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})

        try:
            # Delete domain from DataHub
            logger.debug(f"Deleting domain from DataHub: {domain_urn}")
            result = client.delete_domain(domain_urn)
            
            if result.get("success"):
                logger.info(f"Successfully deleted domain from DataHub: {domain_urn}")
                
                return JsonResponse({
                    "success": True,
                    "message": "Domain deleted from DataHub successfully",
                })
            else:
                error_msg = result.get("message", "Unknown error")
                logger.error(f"Failed to delete domain from DataHub: {error_msg}")
                return JsonResponse({"success": False, "error": error_msg})
                
        except Exception as e:
            logger.error(f"Error deleting domain from DataHub: {str(e)}")
            return JsonResponse({"success": False, "error": str(e)})

    except Exception as e:
        logger.error(f"Error in delete_remote_domain: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_POST
def add_remote_domain_to_pr(request):
    """AJAX endpoint to add a remote domain to a pull request"""
    try:
        logger.info("Adding remote domain to PR")
        
        # Get the domain URN from the request
        domain_urn = request.POST.get("domain_urn")
        if not domain_urn:
            return JsonResponse({"success": False, "error": "Domain URN required"})
        
        # Test DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})

        # Fetch domain from DataHub
        logger.debug(f"Fetching domain from DataHub: {domain_urn}")
        remote_domain = client.get_domain(domain_urn)
        if not remote_domain:
            return JsonResponse({"success": False, "error": "Domain not found in DataHub"})

        # Extract domain properties
        properties = remote_domain.get("properties", {})
        domain_name = properties.get("name", "")
        
        if not domain_name:
            return JsonResponse({"success": False, "error": "Domain name not found in DataHub"})

        # Create domain file content
        domain_data = {
            "name": domain_name,
            "description": properties.get("description", ""),
            "urn": domain_urn,
        }
        
        # Add parent domain if exists
        parent_domain = remote_domain.get("parentDomain")
        if parent_domain:
            parent_urn = parent_domain.get("urn") if isinstance(parent_domain, dict) else parent_domain
            domain_data["parentDomains"] = {"domains": [{"urn": parent_urn}]}
        
        # Add display properties if they exist
        display_props = remote_domain.get("displayProperties", {})
        if display_props:
            domain_data["displayProperties"] = display_props

        # First, sync the domain to local if it doesn't exist
        local_domain = Domain.objects.filter(urn=domain_urn).first()
        
        if not local_domain:
            # Get current connection to set on domain
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            
            # Create local domain from remote data
            local_domain = Domain.objects.create(
                name=domain_name,
                description=domain_data.get("description", ""),
                urn=domain_urn,
                sync_status="SYNCED",
                last_synced=timezone.now(),
                connection=current_connection,
                raw_data=remote_domain,
            )
            logger.info(f"Created local copy of remote domain: {domain_name}")
        
        # Now use GitIntegration to add to PR
        if not GIT_INTEGRATION_AVAILABLE:
            return JsonResponse(
                {"success": False, "error": "Git integration not available"}
            )
        
        try:
            # Get current branch and environment info
            settings = GitSettings.get_instance()
            current_branch = settings.current_branch if settings else "main"
            
            commit_message = f"Add/update domain: {domain_name}"
            
            # Use GitIntegration to push to git
            logger.info(f"Staging domain {local_domain.id} to Git branch {current_branch}")
            git_integration = GitIntegration()
            result = git_integration.push_to_git(local_domain, commit_message)
            
            if result and result.get("success"):
                logger.info(f"Successfully staged domain {local_domain.id} to Git branch {current_branch}")
                
                return JsonResponse({
                    "success": True,
                    "message": f'Domain "{domain_name}" staged for commit to branch {current_branch}. You can create a PR from the Git Repository tab.',
                    "branch": current_branch,
                    "redirect_url": "/github/repo/"
                })
            else:
                error_message = f'Failed to stage domain "{domain_name}"'
                if isinstance(result, dict) and "error" in result:
                    error_message += f": {result['error']}"
                
                logger.error(f"Failed to stage domain: {error_message}")
                return JsonResponse({"success": False, "error": error_message})
                
        except Exception as git_error:
            logger.error(f"Error staging domain to git: {str(git_error)}")
            return JsonResponse({"success": False, "error": f"Git staging failed: {str(git_error)}"})

    except Exception as e:
        logger.error(f"Error adding remote domain to PR: {str(e)}")
        return JsonResponse(
            {"success": False, "error": f"An error occurred: {str(e)}"}
        )


@require_POST
def bulk_sync_domains_to_local(request):
    """AJAX endpoint to bulk sync multiple domains from DataHub to local storage"""
    try:
        data = json.loads(request.body)
        domain_urns = data.get('domain_urns', [])
        
        if not domain_urns:
            return JsonResponse({
                'success': False,
                'error': 'No domain URNs provided'
            }, status=400)
        
        # Get DataHub client
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({
                'success': False,
                'error': 'Unable to connect to DataHub'
            }, status=500)
        
        # Get current connection from request session
        from web_ui.views import get_current_connection
        current_connection = get_current_connection(request)
        current_environment = getattr(current_connection, 'environment', 'dev')
        mutation_config = get_mutation_config_for_environment(current_environment)
        
        success_count = 0
        error_count = 0
        errors = []
        
        for domain_urn in domain_urns:
            try:
                # Fetch domain from DataHub
                remote_domain = client.get_domain(domain_urn)
                if not remote_domain:
                    error_count += 1
                    errors.append(f"Domain {domain_urn}: Not found in DataHub")
                    continue
                
                # Extract domain properties
                properties = remote_domain.get("properties", {})
                domain_name = properties.get("name", "")
                domain_description = properties.get("description", "")
                
                if not domain_name:
                    error_count += 1
                    errors.append(f"Domain {domain_urn}: Name not found in DataHub")
                    continue
                
                # Extract datahub_id from URN
                datahub_id = domain_urn.split(":")[-1] if domain_urn else None
                
                # When syncing FROM DataHub TO local, preserve the original DataHub URN
                # Do NOT generate a new deterministic URN - that's for NEW entities created in web UI
                local_urn = domain_urn
                
                # Prepare ownership data
                ownership_data = None
                ownership = remote_domain.get("ownership", {})
                if ownership and ownership.get("owners"):
                    # Convert DataHub ownership format to our format
                    owners_list = []
                    for owner_info in ownership["owners"]:
                        owner = owner_info.get("owner", {})
                        ownership_type = owner_info.get("ownershipType", {})
                        
                        owner_urn = owner.get("urn", "")
                        ownership_type_urn = ownership_type.get("urn", "urn:li:ownershipType:__system__technical_owner")
                        
                        if owner_urn:
                            owners_list.append({
                                "owner_urn": owner_urn,
                                "ownership_type_urn": ownership_type_urn
                            })
                    
                    if owners_list:
                        ownership_data = {"owners": owners_list}
                
                # Create or update local domain
                domain, created = Domain.objects.update_or_create(
                    urn=local_urn,
                    defaults={
                        "name": domain_name,
                        "description": domain_description,
                        "sync_status": "SYNCED",
                        "last_synced": timezone.now(),
                        "raw_data": remote_domain,
                        "datahub_id": datahub_id,
                        "connection": current_connection,
                        "ownership_data": ownership_data,
                    },
                )
                
                # Extract and store additional domain properties with parent domain resolution
                parent_domain = remote_domain.get("parentDomain")
                if parent_domain:
                    parent_urn = parent_domain.get("urn") if isinstance(parent_domain, dict) else parent_domain
                    
                    # Try to resolve parent domain URN to local deterministic URN
                    resolved_parent_urn = None
                    if parent_urn:
                        # First try to find a local domain with the same DataHub URN
                        try:
                            parent_datahub_id = parent_urn.split(":")[-1] if parent_urn else None
                            if parent_datahub_id:
                                local_parent = Domain.objects.filter(datahub_id=parent_datahub_id).first()
                                if local_parent:
                                    resolved_parent_urn = local_parent.urn
                        except Exception as e:
                            logger.debug(f"Error resolving parent domain by datahub_id: {str(e)}")
                        
                        # If not found by datahub_id, try to find by exact URN match
                        if not resolved_parent_urn:
                            try:
                                local_parent = Domain.objects.filter(urn=parent_urn).first()
                                if local_parent:
                                    resolved_parent_urn = local_parent.urn
                            except Exception as e:
                                logger.debug(f"Error resolving parent domain by URN: {str(e)}")
                        
                        # If still not found, try to find by extracting name from remote parent and generating deterministic URN
                        if not resolved_parent_urn:
                            try:
                                # Try to get the parent domain from DataHub to extract its name
                                remote_parent = client.get_domain(parent_urn)
                                if remote_parent:
                                    parent_name = remote_parent.get("properties", {}).get("name")
                                    if parent_name:
                                        # Generate URN using the same system as editable properties export
                                        from utils.urn_utils import generate_urn_for_new_entity
                                        parent_deterministic_urn = generate_urn_for_new_entity("domain", parent_name, current_environment)
                                        
                                        # Check if a domain with this deterministic URN exists
                                        local_parent = Domain.objects.filter(urn=parent_deterministic_urn).first()
                                        if local_parent:
                                            resolved_parent_urn = local_parent.urn
                            except Exception as e:
                                logger.debug(f"Error resolving parent domain by name: {str(e)}")
                        
                        # Use the resolved URN if found, otherwise use the original DataHub URN as fallback
                        domain.parent_domain_urn = resolved_parent_urn or parent_urn
                        
                        logger.info(f"Parent domain resolution for {domain_name}:")
                        logger.info(f"  Original parent URN: {parent_urn}")
                        logger.info(f"  Resolved parent URN: {resolved_parent_urn}")
                        logger.info(f"  Final parent URN: {domain.parent_domain_urn}")
                        
                        if resolved_parent_urn and resolved_parent_urn != parent_urn:
                            logger.info(f"✅ Successfully resolved parent domain URN from {parent_urn} to {resolved_parent_urn}")
                        else:
                            logger.warning(f"⚠️ Could not resolve parent domain URN {parent_urn} to local deterministic URN")
                else:
                    domain.parent_domain_urn = None
                
                # Store display properties if available
                display_props = remote_domain.get("displayProperties", {})
                if display_props:
                    domain.color_hex = display_props.get("colorHex")
                    icon = display_props.get("icon", {})
                    if icon:
                        domain.icon_name = icon.get("name")
                        domain.icon_style = icon.get("style", "solid")
                        domain.icon_library = icon.get("iconLibrary", "MATERIAL")
                
                # Store entities count from remote domain data if available
                entities_info = remote_domain.get("entities", {})
                domain.entities_count = entities_info.get("total", 0) if entities_info else 0
                
                domain.save()
                success_count += 1
                
            except Exception as e:
                error_count += 1
                errors.append(f"Domain {domain_urn}: {str(e)}")
                logger.error(f"Error syncing domain {domain_urn}: {str(e)}")
        
        message = f"Bulk sync completed: {success_count} successful, {error_count} failed"
        if errors:
            message += f". Errors: {'; '.join(errors[:5])}"  # Show first 5 errors
            if len(errors) > 5:
                message += f" and {len(errors) - 5} more..."
        
        return JsonResponse({
            'success': True,
            'message': message,
            'success_count': success_count,
            'error_count': error_count,
            'errors': errors
        })
        
    except Exception as e:
        logger.error(f"Error in bulk sync: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_POST
def bulk_add_domains_to_staged_changes(request):
    """API view for adding all domains to staged changes"""
    try:
        data = json.loads(request.body)
        environment = data.get('environment', 'dev')
        mutation_name = data.get('mutation_name')
        domain_ids = data.get('domain_ids', [])
        
        # Get current connection to filter domains by connection
        from web_ui.views import get_current_connection
        current_connection = get_current_connection(request)
        
        # Get domains relevant to current connection
        if domain_ids:
            # Specific domains requested
            all_domains = Domain.objects.filter(id__in=domain_ids)
        else:
            # All domains for current connection
            all_domains = Domain.objects.all()
        
        domains = []
        for domain in all_domains:
            if domain.connection is None:
                # True local-only domains (no connection)
                domains.append(domain)
            elif current_connection and domain.connection == current_connection:
                # Domains synced to current connection
                domains.append(domain)
            elif current_connection is None and domain.connection is None:
                # Backward compatibility: if no current connection, show unconnected domains
                domains.append(domain)
            # Domains with different connections are excluded from this connection's view
        
        if not domains:
            return JsonResponse({
                'success': False,
                'error': 'No domains found to add to staged changes for current connection'
            }, status=400)
        
        # Import the domain actions module
        sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        from scripts.mcps.domain_actions import add_domain_to_staged_changes
        
        success_count = 0
        error_count = 0
        files_created_count = 0
        files_skipped_count = 0
        errors = []
        all_created_files = []
        
        for domain in domains:
            try:
                # Extract domain ID from URN or use database ID
                domain_id = domain.datahub_id or domain.urn.split(":")[-1] if domain.urn else str(domain.id)
                
                # Prepare ownership data
                owners = []
                if domain.ownership_data and isinstance(domain.ownership_data, dict):
                    ownership_list = domain.ownership_data.get("owners", [])
                    for owner_info in ownership_list:
                        owner_urn = owner_info.get("owner_urn")
                        if owner_urn:
                            owners.append(owner_urn)
                
                # Prepare display properties
                display_properties = {}
                if domain.color_hex:
                    display_properties["colorHex"] = domain.color_hex
                if domain.icon_name:
                    display_properties["icon"] = {
                        "name": domain.icon_name,
                        "style": domain.icon_style or "solid",
                        "iconLibrary": domain.icon_library or "MATERIAL"
                    }
                
                # Prepare structured properties from raw_data if available
                structured_properties = []
                if domain.raw_data and isinstance(domain.raw_data, dict):
                    structured_props = domain.raw_data.get("structuredProperties", {})
                    if structured_props and structured_props.get("properties"):
                        for prop in structured_props["properties"]:
                            prop_urn = prop.get("structuredProperty", {}).get("urn")
                            values = prop.get("values", [])
                            if prop_urn and values:
                                structured_properties.append({
                                    "propertyUrn": prop_urn,
                                    "values": values
                                })
                
                # Add domain to staged changes with comprehensive aspects
                result = add_domain_to_staged_changes(
                    domain_id=domain_id,
                    name=domain.name,
                    description=domain.description,
                    owners=owners if owners else None,
                    tags=None,  # TODO: Extract tags if stored
                    terms=None,  # TODO: Extract glossary terms if stored
                    links=None,  # TODO: Extract institutional memory if stored
                    custom_properties=None,  # Domains typically don't have custom properties
                    structured_properties=structured_properties if structured_properties else None,
                    forms=None,  # TODO: Extract forms if stored
                    test_results=None,  # TODO: Extract test results if stored
                    display_properties=display_properties if display_properties else None,
                    parent_domain=domain.parent_domain_urn,
                    include_all_aspects=True,
                    environment=environment,
                    owner="system",  # Default owner
                    base_dir="metadata",
                    # Pass existing URN if domain has one (for proper NEW vs EXISTING handling)
                    existing_urn=domain.urn if domain.urn else None
                )
                
                if result.get("success"):
                    success_count += 1
                    files_created = result.get("files_saved", [])
                    files_created_count += len(files_created)
                    all_created_files.extend(files_created)
                else:
                    error_count += 1
                    errors.append(f"Domain {domain.name}: {result.get('message', 'Unknown error')}")
                
            except Exception as e:
                error_count += 1
                errors.append(f"Domain {domain.name}: {str(e)}")
                logger.error(f"Error adding domain {domain.name} to staged changes: {str(e)}")
        
        # Calculate total files that could have been created
        total_possible_files = success_count * 1  # Assuming 1 file per domain
        files_skipped_count = total_possible_files - files_created_count
        
        message = f"Add to staged changes completed: {success_count} domains processed, {files_created_count} files created, {files_skipped_count} files skipped (unchanged), {error_count} failed"
        if errors:
            message += f". Errors: {'; '.join(errors[:5])}"  # Show first 5 errors
            if len(errors) > 5:
                message += f" and {len(errors) - 5} more..."
        
        return JsonResponse({
            'success': True,
            'message': message,
            'success_count': success_count,
            'error_count': error_count,
            'files_created_count': files_created_count,
            'files_skipped_count': files_skipped_count,
            'errors': errors,
            'files_created': all_created_files
        })
        
    except Exception as e:
        logger.error(f"Error adding domains to staged changes: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_POST
def add_domain_to_staged_changes(request, domain_id):
    """Add a domain to staged changes by creating comprehensive MCP files"""
    try:
        import json
        import os
        import sys
        from pathlib import Path
        
        # Add project root to path to import our Python modules
        sys.path.append(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        )
        
        # Import the comprehensive function
        from scripts.mcps.domain_actions import add_domain_to_staged_changes
        
        # Get the domain from database
        try:
            domain = Domain.objects.get(id=domain_id)
        except Domain.DoesNotExist:
            return JsonResponse({
                "status": "error",
                "error": f"Domain with id {domain_id} not found"
            }, status=404)
        
        # Extract domain ID for MCP creation
        domain_mcp_id = domain.datahub_id or domain.urn.split(":")[-1] if domain.urn else str(domain.id)
        
        # Parse request body for environment and mutation settings
        request_data = {}
        if request.body:
            try:
                request_data = json.loads(request.body)
            except json.JSONDecodeError:
                pass
        
        environment = request_data.get('environment', 'dev')
        mutation_name = request_data.get('mutation_name')
        
        # Prepare ownership data
        owners = []
        if domain.ownership_data and isinstance(domain.ownership_data, dict):
            ownership_list = domain.ownership_data.get("owners", [])
            for owner_info in ownership_list:
                owner_urn = owner_info.get("owner_urn")
                if owner_urn:
                    owners.append(owner_urn)
        
        # Prepare display properties
        display_properties = {}
        if domain.color_hex:
            display_properties["colorHex"] = domain.color_hex
        if domain.icon_name:
            display_properties["icon"] = {
                "name": domain.icon_name,
                "style": domain.icon_style or "solid",
                "iconLibrary": domain.icon_library or "MATERIAL"
            }
        
        # Prepare structured properties from raw_data if available
        structured_properties = []
        if domain.raw_data and isinstance(domain.raw_data, dict):
            structured_props = domain.raw_data.get("structuredProperties", {})
            if structured_props and structured_props.get("properties"):
                for prop in structured_props["properties"]:
                    prop_urn = prop.get("structuredProperty", {}).get("urn")
                    values = prop.get("values", [])
                    if prop_urn and values:
                        structured_properties.append({
                            "propertyUrn": prop_urn,
                            "values": values
                        })
        
        # Prepare institutional memory links
        links = []
        # TODO: Extract from raw_data if institutional memory is stored there
        
        # Create comprehensive staged changes with all aspects
        result = add_domain_to_staged_changes(
            domain_id=domain_mcp_id,
            name=domain.name,
            description=domain.description,
            owners=owners if owners else None,
            tags=None,  # TODO: Extract tags if stored
            terms=None,  # TODO: Extract glossary terms if stored
            links=links if links else None,
            custom_properties=None,  # Domains typically don't have custom properties
            structured_properties=structured_properties if structured_properties else None,
            forms=None,  # TODO: Extract forms if stored
            test_results=None,  # TODO: Extract test results if stored
            display_properties=display_properties if display_properties else None,
            parent_domain=domain.parent_domain_urn,
            include_all_aspects=True,
            environment=environment,
            owner=request.user.username if request.user.is_authenticated else "admin",
            base_dir="metadata-manager",
            # Pass existing URN if domain has one (for proper NEW vs EXISTING handling)
            existing_urn=domain.urn if domain.urn else None
        )
        
        if result.get("success"):
            files_created = result.get("files_saved", [])
            return JsonResponse({
                "status": "success",
                "message": f"Domain '{domain.name}' added to staged changes",
                "files_created": files_created,
                "aspects_included": result.get("aspects_included", [])
            })
        else:
            return JsonResponse({
                "status": "error",
                "error": result.get("message", "Failed to create staged changes")
            })
        
    except Exception as e:
        import traceback
        logger.error(f"Error adding domain to staged changes: {str(e)}")
        logger.error(traceback.format_exc())
        
        return JsonResponse({
            "status": "error",
            "error": f"An error occurred: {str(e)}"
        })


@method_decorator(csrf_exempt, name="dispatch")
class DomainRemoteAddToStagedChangesView(View):
    """API endpoint to add a remote domain to staged changes without syncing to local first"""
    
    def post(self, request):
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
            from scripts.mcps.domain_actions import add_domain_to_staged_changes
            
            data = json.loads(request.body)
            
            # Get the domain data from the request
            domain_data = data.get('domain_data')
            if not domain_data:
                return JsonResponse({
                    "status": "error",
                    "error": "No domain_data provided"
                }, status=400)
            
            # Get environment and mutation name
            environment_name = data.get('environment', 'dev')
            mutation_name = data.get('mutation_name')
            
            # Get current user as owner
            owner = request.user.username if request.user.is_authenticated else "admin"
            
            # For remote domains, we need to ensure we have an ID for MCP creation
            # If the remote domain doesn't have an ID, we'll generate one from the URN or name
            domain_id = domain_data.get('id')
            if not domain_id:
                if domain_data.get('urn'):
                    # Extract ID from URN
                    urn_parts = domain_data['urn'].split(':')
                    if len(urn_parts) >= 3:
                        domain_id = urn_parts[-1]
                    else:
                        domain_id = domain_data['urn']
                elif domain_data.get('name'):
                    # Use name as ID
                    domain_id = domain_data['name'].replace(' ', '_').lower()
                else:
                    return JsonResponse({
                        "status": "error",
                        "error": "Remote domain must have either URN or name for ID generation"
                    }, status=400)
            
            # Prepare ownership data
            owners = []
            if domain_data.get('ownership_data') and isinstance(domain_data['ownership_data'], dict):
                ownership_list = domain_data['ownership_data'].get("owners", [])
                for owner_info in ownership_list:
                    owner_urn = owner_info.get("owner_urn")
                    if owner_urn:
                        owners.append(owner_urn)
            
            # Prepare display properties
            display_properties = {}
            if domain_data.get('color_hex'):
                display_properties["colorHex"] = domain_data['color_hex']
            if domain_data.get('icon_name'):
                display_properties["icon"] = {
                    "name": domain_data['icon_name'],
                    "style": domain_data.get('icon_style', 'solid'),
                    "iconLibrary": domain_data.get('icon_library', 'MATERIAL')
                }
            
            # Prepare structured properties from raw_data if available
            structured_properties = []
            raw_data = domain_data.get('raw_data')
            if raw_data and isinstance(raw_data, dict):
                structured_props = raw_data.get("structuredProperties", {})
                if structured_props and structured_props.get("properties"):
                    for prop in structured_props["properties"]:
                        prop_urn = prop.get("structuredProperty", {}).get("urn")
                        values = prop.get("values", [])
                        if prop_urn and values:
                            structured_properties.append({
                                "propertyUrn": prop_urn,
                                "values": values
                            })
            
            # Add remote domain to staged changes using the comprehensive function
            result = add_domain_to_staged_changes(
                domain_id=domain_id,
                name=domain_data.get('name', 'Unknown'),
                description=domain_data.get('description', ''),
                owners=owners if owners else None,
                tags=None,  # TODO: Extract tags if stored
                terms=None,  # TODO: Extract glossary terms if stored
                links=None,  # TODO: Extract institutional memory if stored
                custom_properties=None,  # Domains typically don't have custom properties
                structured_properties=structured_properties if structured_properties else None,
                forms=None,  # TODO: Extract forms if stored
                test_results=None,  # TODO: Extract test results if stored
                display_properties=display_properties if display_properties else None,
                parent_domain=domain_data.get('parent_domain_urn'),
                include_all_aspects=True,
                environment=environment_name,
                owner=owner,
                base_dir="metadata-manager",
                mutation_name=mutation_name,
                # Remote domains always have existing URNs from DataHub
                existing_urn=domain_data.get('urn')
            )
            
            # Provide feedback about files created
            if result.get("success"):
                files_created = result.get("files_saved", [])
                files_created_count = len(files_created)
                
                message = f"Remote domain added to staged changes: {files_created_count} file created"
                
                # Return success response
                return JsonResponse({
                    "status": "success",
                    "message": message,
                    "files_created": files_created,
                    "files_created_count": files_created_count,
                    "aspects_included": result.get("aspects_included", [])
                })
            else:
                return JsonResponse({
                    "status": "error",
                    "error": result.get("message", "Failed to create staged changes")
                }, status=500)
                
        except Exception as e:
            logger.error(f"Error adding remote domain to staged changes: {str(e)}")
            return JsonResponse({"status": "error", "error": str(e)}, status=500)
