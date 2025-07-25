import logging
import json
import os
import subprocess
import uuid
from datetime import datetime
from django.shortcuts import render, get_object_or_404
from django.views import View
from django.http import JsonResponse
from django.contrib import messages
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods, require_POST
from django.utils.decorators import method_decorator
from django.utils import timezone
from django.db import models

# Add project root to sys.path
import sys
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from utils.datahub_utils import test_datahub_connection, get_datahub_client
from utils.urn_utils import get_full_urn_from_name, generate_mutated_urn, get_mutation_config_for_environment
from .models import DataProduct, Environment

logger = logging.getLogger(__name__)


class DataProductListView(View):
    """View to list data products"""

    def get(self, request):
        """Display list of data products"""
        try:
            logger.info("Starting DataProductListView.get")

            # Get DataHub connection info (quick test only)
            logger.debug("Testing DataHub connection from DataProductListView")
            connected, client = test_datahub_connection(request)
            logger.debug(f"DataHub connection test result: {connected}")

            # Get local data products
            local_data_products = DataProduct.objects.all().order_by('name')

            # Initialize context with local data only - remote data loaded via AJAX
            context = {
                "local_data_products": local_data_products,
                "remote_data_products": [],  # Will be populated via AJAX
                "has_datahub_connection": connected,
                "datahub_url": None,  # Will be populated via AJAX
                "page_title": "Data Products",
            }

            logger.info("Rendering data product list template (async loading)")
            return render(request, "metadata_manager/data_products/list.html", context)
        except Exception as e:
            logger.error(f"Error in data product list view: {str(e)}")
            return render(
                request,
                "metadata_manager/data_products/list.html",
                {"error": str(e), "page_title": "Data Products"},
            )

    def post(self, request):
        """Handle data product creation"""
        try:
            logger.info("Creating new data product")
            
            # Get form data
            name = request.POST.get('name', '').strip()
            description = request.POST.get('description', '').strip()
            external_url = request.POST.get('external_url', '').strip()
            domain_urn = request.POST.get('domain_urn', '').strip()
            entity_urns_str = request.POST.get('entity_urns', '').strip()
            
            # Validate required fields
            if not name:
                messages.error(request, "Data product name is required")
                return self.get(request)
            
            # Parse entity URNs
            entity_urns = []
            if entity_urns_str:
                try:
                    # Split by newlines and commas, filter out empty strings
                    urns = [urn.strip() for line in entity_urns_str.split('\n') 
                           for urn in line.split(',') if urn.strip()]
                    entity_urns = urns
                except Exception as e:
                    logger.warning(f"Error parsing entity URNs: {e}")
            
            # Create local data product
            return create_local_data_product(request, {
                'name': name,
                'description': description,
                'external_url': external_url if external_url else None,
                'domain_urn': domain_urn if domain_urn else None,
                'entity_urns': entity_urns,
            })
            
        except Exception as e:
            logger.error(f"Error in data product creation: {str(e)}")
            messages.error(request, f"Error creating data product: {str(e)}")
            return self.get(request)


def get_remote_data_products_data(request):
    """AJAX endpoint to get remote data products data"""
    try:
        logger.info("Loading remote data products data via AJAX")

        # Get DataHub connection using connection system
        from utils.datahub_utils import test_datahub_connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "No active DataHub connection configured"})

        # Get local data products for comparison
        local_data_products = list(DataProduct.objects.all())
        local_urns = {dp.urn for dp in local_data_products}

        # Fetch remote data products
        remote_data_products = []
        datahub_url = None

        try:
            logger.debug("Fetching remote data products from DataHub")

            # Get DataHub URL for direct links
            datahub_url = client.server_url
            if datahub_url.endswith("/api/gms"):
                datahub_url = datahub_url[:-8]  # Remove /api/gms to get base URL

            # Get data products from DataHub using the new method
            result = client.list_data_products(start=0, count=1000, query="*")

            if result:
                remote_data_products_data = result
                logger.debug(f"Fetched {len(remote_data_products_data)} remote data products")

                # Process the data products
                if remote_data_products_data:
                    for product_data in remote_data_products_data:
                        try:
                            if product_data:
                                # Calculate statistics for each product
                                product_data['owners_count'] = product_data.get('owners_count', 0)
                                product_data['relationships_count'] = 0  # Not available in current query
                                product_data['entities_count'] = product_data.get('numAssets', 0)
                                
                                # Add sync status
                                product_data['sync_status'] = 'REMOTE_ONLY'
                                product_data['sync_status_display'] = 'Remote Only'
                                
                                remote_data_products.append(product_data)
                        except Exception as e:
                            logger.warning(f"Error processing remote data product: {str(e)}")
                            continue

            else:
                error_msg = "Unknown error"
                if result:
                    error_msg = result.get('error', 'Unknown error')
                else:
                    error_msg = "No response from DataHub"
                logger.warning(f"Failed to fetch remote data products: {error_msg}")

            # Process local and synced data
            synced_items = []
            local_only_items = []
            remote_only_items = remote_data_products.copy()

            logger.debug(f"Processing {len(local_data_products)} local data products")
            
            for local_dp in local_data_products:
                local_dict = local_dp.to_dict()
                local_dict['sync_status'] = local_dp.sync_status
                local_dict['sync_status_display'] = dict(DataProduct.SYNC_STATUS_CHOICES).get(
                    local_dp.sync_status, local_dp.sync_status
                )
                
                logger.debug(f"Processing local product: {local_dp.name}, urn: {local_dp.urn}, sync_status: {local_dp.sync_status}")
                
                # Check if this local item exists in remote
                matching_remote = None
                if local_dp.urn:  # Only try to match if there's a URN
                    for remote_dp in remote_data_products:
                        if remote_dp.get('urn') == local_dp.urn:
                            matching_remote = remote_dp
                            # Remove from remote_only_items since it's synced
                            if remote_dp in remote_only_items:
                                remote_only_items.remove(remote_dp)
                            break
                
                if matching_remote:
                    # This is a synced item
                    synced_items.append({
                        'local': local_dict,
                        'remote': matching_remote,
                        'combined': {**matching_remote, **local_dict, 'sync_status': 'SYNCED', 'sync_status_display': 'Synced'}
                    })
                else:
                    # This is local only
                    logger.debug(f"Adding {local_dp.name} to local_only_items")
                    local_only_items.append(local_dict)

            # Calculate statistics
            total_local = len(local_data_products)
            total_remote = len(remote_data_products)
            total_synced = len(synced_items)
            total_local_only = len(local_only_items)
            total_remote_only = len(remote_only_items)
            
            logger.debug(f"Final counts - Local: {total_local}, Remote: {total_remote}, Synced: {total_synced}, Local-only: {total_local_only}, Remote-only: {total_remote_only}")
            
            # Count products with relationships and owners
            owned_count = 0
            relationships_count = 0
            
            for item in synced_items + local_only_items + remote_only_items:
                try:
                    if isinstance(item, dict):
                        data = item.get('combined', item) if item else {}
                        if data and data.get('owners_count', 0) > 0:
                            owned_count += 1
                        if data and data.get('relationships_count', 0) > 0:
                            relationships_count += 1
                except Exception as e:
                    logger.warning(f"Error processing item for statistics: {e}")
                    continue

            statistics = {
                'total_items': max(total_local, total_remote),
                'synced_count': total_synced,
                'local_only_count': total_local_only,
                'remote_only_count': total_remote_only,
                'owned_items': owned_count,
                'items_with_relationships': relationships_count,
            }

            return JsonResponse(
                {
                    "success": True,
                    "data": {
                        "synced_items": synced_items,
                        "local_only_items": local_only_items,
                        "remote_only_items": remote_only_items,
                        "statistics": statistics,
                        "datahub_url": datahub_url,
                    },
                }
            )

        except Exception as e:
            logger.error(f"Error fetching remote data product data: {str(e)}")
            return JsonResponse(
                {
                    "success": False,
                    "error": f"Error fetching remote data products: {str(e)}",
                }
            )

    except Exception as e:
        logger.error(f"Error in get_remote_data_products_data: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


def create_local_data_product(request, data_product_data):
    """Create a local data product"""
    try:
        logger.info(f"Creating local data product: {data_product_data.get('name')}")
        
        name = data_product_data['name']
        description = data_product_data.get('description', '')
        external_url = data_product_data.get('external_url')
        domain_urn = data_product_data.get('domain_urn')
        entity_urns = data_product_data.get('entity_urns', [])
        
        # Get current environment for consistent URN generation
        from web_ui.views import get_current_connection
        current_connection = get_current_connection(request)
        current_environment = getattr(current_connection, 'environment', 'dev')
        
        # Generate URN using the same system as editable properties export
        from utils.urn_utils import generate_urn_for_new_entity
        deterministic_urn = generate_urn_for_new_entity("dataProduct", name, current_environment)
        
        # Check if data product already exists
        if DataProduct.objects.filter(urn=deterministic_urn).exists():
            messages.error(request, f"Data product with name '{name}' already exists")
            return JsonResponse({"success": False, "error": "Data product already exists"})
        
        # Create the data product
        data_product = DataProduct.objects.create(
            name=name,
            description=description,
            urn=deterministic_urn,
            external_url=external_url,
            domain_urn=domain_urn,
            entity_urns=entity_urns,
            sync_status="LOCAL_ONLY",
        )
        
        logger.info(f"Created local data product: {data_product.name} (ID: {data_product.id})")
        messages.success(request, f"Data product '{name}' created successfully")
        
        return JsonResponse({
            "success": True,
            "data_product_id": str(data_product.id),
            "message": f"Data product '{name}' created successfully"
        })
        
    except Exception as e:
        logger.error(f"Error creating local data product: {str(e)}")
        messages.error(request, f"Error creating data product: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@csrf_exempt
def add_data_product_to_pr_comprehensive(request, data_product_id):
    """Add a data product to a GitHub PR using comprehensive Git integration like assertions"""
    try:
        # Import Git integration components like in assertions
        try:
            from web_ui.models import GitIntegration, GitSettings
            from web_ui.models import Environment as WebUIEnvironment
            GIT_INTEGRATION_AVAILABLE = True
        except ImportError:
            GitIntegration = None
            GitSettings = None
            WebUIEnvironment = None
            GIT_INTEGRATION_AVAILABLE = False
        
        data_product = get_object_or_404(DataProduct, id=data_product_id)
        
        # Check if git integration is enabled
        if not GIT_INTEGRATION_AVAILABLE:
            return JsonResponse({
                "success": False, 
                "error": "Git integration is not configured. Please configure GitHub settings first."
            })
        
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
                # Try web_ui environment first
                if WebUIEnvironment:
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
        
        # Create a data product object that GitIntegration can handle
        class DataProductForGit:
            def __init__(self, data_product, environment):
                self.id = data_product.id
                self.name = data_product.name
                self.description = data_product.description
                self.external_url = data_product.external_url
                self.domain_urn = data_product.domain_urn
                self.entity_urns = data_product.entity_urns
                self.environment = environment
                self.sync_status = data_product.sync_status
                self.urn = data_product.urn
                
            def to_dict(self):
                """Convert data product to DataHub GraphQL format for file output"""
                # Determine operation based on sync status
                operation = "create"  # Default for local data products
                if self.sync_status == "SYNCED" or self.urn:
                    operation = "update"
                
                data_product_data = {
                    "operation": operation,
                    "data_product_type": "DATA_PRODUCT",
                    "name": self.name,
                    "description": self.description,
                    "external_url": self.external_url,
                    "domain_urn": self.domain_urn,
                    "entity_urns": self.entity_urns or [],
                    "urn": self.urn,
                    "local_id": str(self.id),
                    "created_at": datetime.now().isoformat(),
                    # Add unique filename for this data product to prevent overwrites
                    "filename": f"{operation}_data_product_{self.id}_{self.name.lower().replace(' ', '_')}.json"
                }
                
                # Add specific GraphQL input based on operation
                if operation == "create":
                    data_product_data["graphql_input"] = {
                        "mutation": "createDataProduct",
                        "input": {
                            "name": self.name,
                            "description": self.description,
                            "urn": self.urn,
                            "externalUrl": self.external_url,
                            "domainUrn": self.domain_urn,
                        }
                    }
                else:
                    data_product_data["graphql_input"] = {
                        "mutation": "updateDataProduct", 
                        "variables": {
                            "urn": self.urn,
                            "input": {
                                "name": self.name,
                                "description": self.description,
                                "externalUrl": self.external_url,
                                "domainUrn": self.domain_urn,
                            }
                        }
                    }
                
                # Remove None values from GraphQL input
                if "input" in data_product_data["graphql_input"]:
                    data_product_data["graphql_input"]["input"] = {
                        k: v for k, v in data_product_data["graphql_input"]["input"].items() 
                        if v is not None
                    }
                
                return data_product_data

        # Create data product object for Git integration
        data_product_for_git = DataProductForGit(data_product, environment_name)
        
        # Create commit message
        commit_message = f"Add data product '{data_product.name}' for {environment_name} environment"
        
        # Use GitIntegration to push to git (same as assertions)
        logger.info(f"Staging data product {data_product.id} to Git branch {current_branch}")
        git_integration = GitIntegration()
        result = git_integration.push_to_git(data_product_for_git, commit_message)
        
        if result and result.get("success"):
            logger.info(f"Successfully staged data product {data_product.id} to Git branch {current_branch}")
            
            response_data = {
                "success": True,
                "message": f'Data product "{data_product.name}" staged for commit to branch {current_branch}. You can create a PR from the Git Repository tab.',
                "environment": environment_name,
                "branch": current_branch,
                "redirect_url": "/github/repo/"  # Same as assertions
            }
            
            # Add file path info if available
            if "file_path" in result:
                response_data["file_path"] = result["file_path"]
            
            return JsonResponse(response_data)
        else:
            # Failed to stage changes
            error_message = f'Failed to stage data product "{data_product.name}"'
            if isinstance(result, dict) and "error" in result:
                error_message += f": {result['error']}"
            
            logger.error(f"Failed to stage data product: {error_message}")
            return JsonResponse({"success": False, "error": error_message})
        
    except Exception as e:
        logger.error(f"Error adding data product to PR: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@csrf_exempt
def add_data_product_to_pr(request, data_product_id):
    """Add a data product to a PR (GitHub workflow)"""
    if request.method != 'POST':
        return JsonResponse({"success": False, "error": "Only POST method allowed"})
    
    try:
        logger.info(f"Adding data product {data_product_id} to PR")
        
        # Get the data product
        data_product = get_object_or_404(DataProduct, id=data_product_id)
        
        # Get environment from env var with fallback to database
        environment = os.getenv('ENVIRONMENT')
        if not environment:
            try:
                default_env = Environment.objects.filter(is_default=True).first()
                environment = default_env.name if default_env else 'dev'
            except:
                environment = 'dev'
        
        logger.info(f"Using environment: {environment}")
        
        # Create the directory structure
        base_dir = f"metadata-manager/{environment}/data_products"
        os.makedirs(base_dir, exist_ok=True)
        
        # Generate the JSON file for the data product
        json_data = {
            "operation": "create",
            "data_product_type": "DATA_PRODUCT",
            "name": data_product.name,
            "description": data_product.description,
            "urn": data_product.urn,
            "external_url": data_product.external_url,
            "domain_urn": data_product.domain_urn,
            "entity_urns": data_product.entity_urns,
            "created_at": datetime.now().isoformat(),
            "graphql_input": {
                "mutation": "createDataProduct",
                "input": {
                    "name": data_product.name,
                    "description": data_product.description,
                    "urn": data_product.urn,
                    "externalUrl": data_product.external_url,
                    "domainUrn": data_product.domain_urn,
                    "entityUrns": data_product.entity_urns,
                }
            }
        }
        
        # Remove None values
        json_data["graphql_input"]["input"] = {
            k: v for k, v in json_data["graphql_input"]["input"].items() 
            if v is not None
        }
        
        # Generate filename
        sanitized_name = data_product.name.replace(' ', '_').replace('/', '_').replace('\\', '_')
        filename = f"create_{sanitized_name}.json"
        filepath = os.path.join(base_dir, filename)
        
        # Write the JSON file
        with open(filepath, 'w') as f:
            json.dump(json_data, f, indent=2)
        
        logger.info(f"Created data product file: {filepath}")
        
        # Create git branch and commit
        try:
            branch_name = f"data_product/{environment}/{data_product.name.replace(' ', '-').lower()}"
            github_urls = create_git_branch_and_commit(filepath, branch_name, data_product.name, "data product")
        except Exception as e:
            logger.error(f"Git operations failed: {str(e)}")
            github_urls = {}
        
        # Update data product status
        data_product.sync_status = "PENDING_PUSH"
        data_product.save()
        
        response_data = {
            "success": True,
            "message": f"Data product '{data_product.name}' added to PR workflow",
            "file_path": filepath,
            "environment": environment,
        }
        
        # Add GitHub URLs if available
        if github_urls:
            response_data.update(github_urls)
        
        return JsonResponse(response_data)
        
    except Exception as e:
        logger.error(f"Error adding data product to PR: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


def create_git_branch_and_commit(file_path, branch_name, data_product_name, entity_type="data product"):
    """Create a git branch and commit the data product file"""
    try:
        logger.info(f"Creating git branch and commit for {entity_type}: {data_product_name}")
        
        # Get current directory
        original_dir = os.getcwd()
        
        # Change to project root (where .git is)
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        os.chdir(project_root)
        
        try:
            # Create and checkout new branch
            subprocess.run(['git', 'checkout', '-b', branch_name], check=True, capture_output=True, text=True)
            logger.info(f"Created branch: {branch_name}")
            
            # Add the file
            subprocess.run(['git', 'add', file_path], check=True, capture_output=True, text=True)
            
            # Commit the changes
            commit_message = f"Add {entity_type}: {data_product_name}"
            subprocess.run(['git', 'commit', '-m', commit_message], check=True, capture_output=True, text=True)
            logger.info(f"Committed changes: {commit_message}")
            
            # Push the branch
            subprocess.run(['git', 'push', '-u', 'origin', branch_name], check=True, capture_output=True, text=True)
            logger.info(f"Pushed branch: {branch_name}")
            
            # Generate GitHub URLs (adjust based on your repository)
            repo_url = "https://github.com/your-org/datahub-recipes-manager"  # Update this
            branch_url = f"{repo_url}/tree/{branch_name}"
            pr_url = f"{repo_url}/compare/{branch_name}?expand=1"
            
            return {
                "branch_name": branch_name,
                "branch_url": branch_url,
                "pr_url": pr_url,
            }
            
        finally:
            # Return to original directory
            os.chdir(original_dir)
            
    except subprocess.CalledProcessError as e:
        logger.error(f"Git command failed: {e}")
        raise Exception(f"Git operation failed: {e}")
    except Exception as e:
        logger.error(f"Error in git operations: {str(e)}")
        raise


class DataProductDetailView(View):
    """View for data product details"""

    def get(self, request, data_product_id):
        """Display data product details"""
        try:
            data_product = get_object_or_404(DataProduct, id=data_product_id)
            
            context = {
                "data_product": data_product,
                "page_title": f"Data Product: {data_product.name}",
            }
            
            return render(request, "metadata_manager/data_products/detail.html", context)
            
        except Exception as e:
            logger.error(f"Error in data product detail view: {str(e)}")
            messages.error(request, f"Error loading data product: {str(e)}")
            return render(request, "metadata_manager/data_products/list.html")

    def post(self, request, data_product_id):
        """Handle data product updates"""
        try:
            data_product = get_object_or_404(DataProduct, id=data_product_id)
            
            # Get form data
            name = request.POST.get('name', '').strip()
            description = request.POST.get('description', '').strip()
            external_url = request.POST.get('external_url', '').strip()
            domain_urn = request.POST.get('domain_urn', '').strip()
            entity_urns_str = request.POST.get('entity_urns', '').strip()
            
            # Validate required fields
            if not name:
                messages.error(request, "Data product name is required")
                return self.get(request, data_product_id)
            
            # Parse entity URNs
            entity_urns = []
            if entity_urns_str:
                try:
                    urns = [urn.strip() for line in entity_urns_str.split('\n') 
                           for urn in line.split(',') if urn.strip()]
                    entity_urns = urns
                except Exception as e:
                    logger.warning(f"Error parsing entity URNs: {e}")
            
            # Update the data product
            data_product.name = name
            data_product.description = description
            data_product.external_url = external_url if external_url else None
            data_product.domain_urn = domain_urn if domain_urn else None
            data_product.entity_urns = entity_urns
            
            # Mark as modified if it was previously synced
            if data_product.sync_status == "SYNCED":
                data_product.sync_status = "MODIFIED"
            
            data_product.save()
            
            messages.success(request, f"Data product '{name}' updated successfully")
            return self.get(request, data_product_id)
            
        except Exception as e:
            logger.error(f"Error updating data product: {str(e)}")
            messages.error(request, f"Error updating data product: {str(e)}")
            return self.get(request, data_product_id)

    def delete(self, request, data_product_id):
        """Handle data product deletion"""
        try:
            data_product = get_object_or_404(DataProduct, id=data_product_id)
            name = data_product.name
            data_product.delete()
            
            messages.success(request, f"Data product '{name}' deleted successfully")
            return JsonResponse({"success": True, "message": f"Data product '{name}' deleted"})
            
        except Exception as e:
            logger.error(f"Error deleting data product: {str(e)}")
            return JsonResponse({"success": False, "error": str(e)})


@csrf_exempt  
def sync_data_product_to_local(request, data_product_id=None):
    """Sync a remote data product to local storage"""
    if request.method != 'POST':
        return JsonResponse({"success": False, "error": "Method not allowed"})
    
    try:
        product_urn = None
        
        # Handle both URL patterns: with data_product_id and without
        if data_product_id:
            # Called from detail view with data_product_id in URL
            try:
                data_product = DataProduct.objects.get(id=data_product_id)
                product_urn = data_product.urn
                if not product_urn:
                    return JsonResponse({"success": False, "error": "Data product has no URN to sync from"})
            except DataProduct.DoesNotExist:
                return JsonResponse({"success": False, "error": "Data product not found"})
        else:
            # Called from list view with URN in POST body
            data = json.loads(request.body)
            product_urn = data.get('product_urn')
        
        if not product_urn:
            return JsonResponse({"success": False, "error": "Data product URN is required"})
        
        logger.info(f"Syncing data product {product_urn} to local storage")
        
        # Get DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})
        
        # Fetch the data product details from DataHub
        logger.info(f"Calling client.list_data_products with query: {product_urn}")
        result = client.list_data_products(query=product_urn, start=0, count=1)
        
        
        if result is None:
            return JsonResponse({"success": False, "error": "No response from DataHub API"})
        
        if not isinstance(result, list):
            return JsonResponse({"success": False, "error": f"Invalid response type from DataHub: {type(result)}"})
        
        if not result:
            return JsonResponse({"success": False, "error": "Data product not found in DataHub"})
        
        # Get the first (and should be only) result
        product_data = result[0]
        if not product_data:
            logger.error("First data product result is None")
            return JsonResponse({"success": False, "error": "Invalid data product data from DataHub"})
        
        # Extract relevant information with defensive programming
        properties = product_data.get('properties', {})
        if properties is None:
            logger.warning(f"No properties section in product data: {product_data}")
            properties = {}
        
        name = properties.get('name', 'Unnamed Data Product') if properties else 'Unnamed Data Product'
        description = properties.get('description') if properties else None
        # Ensure description is None if empty string to avoid constraint issues
        if description == '':
            description = None
        external_url = properties.get('externalUrl') if properties else None
        # Ensure external_url is None if empty string
        if external_url == '':
            external_url = None
        
        # Extract domain information
        domain_urn = None
        domain_data = product_data.get('domain', {})
        if domain_data and domain_data.get('domain'):
            domain_urn = domain_data['domain'].get('urn')
        
        # Extract entity URNs (this might need to be fetched separately in a real implementation)
        entity_urns = []
        entities_data = product_data.get('entities', {})
        if entities_data and entities_data.get('total', 0) > 0:
            # For now, we'll use an empty list since getting the actual entity URNs
            # would require a separate API call to get the data product's assets
            pass
        
        # Use the new create_from_datahub method for consistent data handling
        try:
            # Get current connection context
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            current_environment = getattr(current_connection, 'environment', 'dev')
            
            # Create or update using the model's create_from_datahub method
            created_product = DataProduct.create_from_datahub(product_data, connection=current_connection)
            
            logger.info(f"Successfully synced data product: {created_product.name} (ID: {created_product.id})")
            message = f"Data product '{created_product.name}' synced to local storage successfully"
            
        except Exception as create_error:
            logger.error(f"Error using create_from_datahub: {str(create_error)}")
            # Fallback to original logic if the new method fails
            
            # Extract relevant information with defensive programming
            properties = product_data.get('properties', {})
            if properties is None:
                logger.warning(f"No properties section in product data: {product_data}")
                properties = {}
            
            name = properties.get('name', 'Unnamed Data Product') if properties else 'Unnamed Data Product'
            description = properties.get('description') if properties else None
            # Ensure description is None if empty string to avoid constraint issues
            if description == '':
                description = None
            external_url = properties.get('externalUrl') if properties else None
            # Ensure external_url is None if empty string
            if external_url == '':
                external_url = None
            
            # Extract domain information
            domain_urn = None
            domain_data = product_data.get('domain', {})
            if domain_data and domain_data.get('domain'):
                domain_urn = domain_data['domain'].get('urn')
            
            # Extract entity URNs (this might need to be fetched separately in a real implementation)
            entity_urns = []
            entities_data = product_data.get('entities', {})
            if entities_data and entities_data.get('total', 0) > 0:
                # For now, we'll use an empty list since getting the actual entity URNs
                # would require a separate API call to get the data product's assets
                pass
            
            # When syncing FROM DataHub TO local, preserve the original DataHub URN
            # Do NOT generate a new deterministic URN - that's for NEW entities created in web UI
            local_urn = product_urn
            
            # Check if data product already exists locally
            existing_product = DataProduct.objects.filter(urn=local_urn).first()
            
            if existing_product:
                # Update existing product
                existing_product.name = name
                existing_product.description = description
                existing_product.external_url = external_url
                existing_product.domain_urn = domain_urn
                existing_product.entity_urns = entity_urns
                existing_product.sync_status = "SYNCED"
                existing_product.last_synced = timezone.now()
                
                # Set connection context
                if current_connection:
                    existing_product.connection = current_connection
                
                # Store comprehensive data
                existing_product.properties_data = properties
                ownership_data = product_data.get('ownership')
                existing_product.ownership_data = ownership_data
                existing_product.entities_data = product_data.get('entities')
                existing_product.tags_data = product_data.get('tags')
                existing_product.glossary_terms_data = product_data.get('glossaryTerms')
                existing_product.structured_properties_data = product_data.get('structuredProperties')
                existing_product.institutional_memory_data = product_data.get('institutionalMemory')
                
                existing_product.save()
                
                logger.info(f"Updated existing data product: {name} (ID: {existing_product.id})")
                message = f"Data product '{name}' updated in local storage"
                created_product = existing_product
                
            else:
                # Create new local data product
                new_product = DataProduct.objects.create(
                    name=name,
                    description=description,
                    urn=local_urn,
                    external_url=external_url,
                    domain_urn=domain_urn,
                    entity_urns=entity_urns,
                    sync_status="SYNCED",
                    last_synced=timezone.now(),
                    connection=current_connection,  # Set connection context
                    
                    # Store comprehensive data
                    properties_data=properties,
                    ownership_data=product_data.get('ownership'),
                    entities_data=product_data.get('entities'),
                    tags_data=product_data.get('tags'),
                    glossary_terms_data=product_data.get('glossaryTerms'),
                    structured_properties_data=product_data.get('structuredProperties'),
                    institutional_memory_data=product_data.get('institutionalMemory'),
                )
                
                logger.info(f"Created new data product: {name} (ID: {new_product.id})")
                message = f"Data product '{name}' synced to local storage successfully"
                created_product = new_product
        
        return JsonResponse({
            "success": True,
            "message": message,
            "product_id": created_product.id,
            "product_name": created_product.name
        })
        
    except Exception as e:
        logger.error(f"Error syncing data product to local: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@csrf_exempt
def push_data_product_to_datahub(request, data_product_id):
    """Push a local data product to DataHub"""
    if request.method != 'POST':
        return JsonResponse({"success": False, "error": "Method not allowed"})
    
    try:
        data_product = get_object_or_404(DataProduct, id=data_product_id)
        
        logger.info(f"Pushing data product {data_product.name} to DataHub")
        
        # Get DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})
        
        # Prepare data product data for creation/update
        data_product_data = {
            "name": data_product.name,
            "description": data_product.description,
            "urn": data_product.urn,
        }
        
        # Add optional fields
        if data_product.external_url:
            data_product_data["externalUrl"] = data_product.external_url
        
        if data_product.domain_urn:
            data_product_data["domainUrn"] = data_product.domain_urn
        
        if data_product.entity_urns:
            data_product_data["entity_urns"] = data_product.entity_urns
        
        # Determine if this is an update or create operation
        if data_product.urn:
            # Update existing data product
            result = client.update_data_product(data_product.urn, data_product_data)
        else:
            # Create new data product
            result = client.create_data_product(data_product_data)
        
        if result and result.get("success"):
            # Update local record with successful push
            created_urn = result.get("urn")
            if created_urn and not data_product.urn:
                data_product.urn = created_urn
            
            data_product.sync_status = "SYNCED"
            data_product.last_synced = timezone.now()
            data_product.save()
            
            logger.info(f"Successfully pushed data product to DataHub: {data_product.name}")
            return JsonResponse({
                "success": True,
                "message": f"Data product '{data_product.name}' pushed to DataHub successfully",
                "datahub_urn": created_urn or data_product.urn
            })
        else:
            error_msg = result.get("error", "Unknown error") if result else "No response from DataHub"
            logger.error(f"Failed to push data product to DataHub: {error_msg}")
            return JsonResponse({
                "success": False,
                "error": f"Failed to push to DataHub: {error_msg}"
            })
        
    except Exception as e:
        logger.error(f"Error pushing data product to DataHub: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@csrf_exempt
def resync_data_product(request, data_product_id):
    """Resync a data product with DataHub"""
    if request.method != 'POST':
        return JsonResponse({"success": False, "error": "Method not allowed"})
    
    try:
        data_product = get_object_or_404(DataProduct, id=data_product_id)
        
        logger.info(f"Resyncing data product {data_product.name} with DataHub")
        
        # Get DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})
        
        # Check if we have a URN to resync
        if not data_product.urn:
            return JsonResponse({
                "success": False, 
                "error": "Cannot resync: no DataHub URN found for this data product"
            })
        
        # Fetch the latest data from DataHub
        result = client.list_data_products(query=data_product.urn, start=0, count=1)
        
        if not result:
            error_msg = 'Failed to fetch data product from DataHub'
            logger.error(f"Failed to fetch data product from DataHub during resync: {error_msg}")
            return JsonResponse({"success": False, "error": error_msg})
        
        # Get the updated data product data
        product_data = result[0]
        if not product_data:
            return JsonResponse({"success": False, "error": "Invalid data product data from DataHub"})
        
        # Extract updated information
        properties = product_data.get('properties', {})
        updated_name = properties.get('name', data_product.name)
        updated_description = properties.get('description', data_product.description)
        updated_external_url = properties.get('externalUrl', data_product.external_url)
        
        # Extract domain information
        updated_domain_urn = data_product.domain_urn  # Default to existing
        domain_data = product_data.get('domain', {})
        if domain_data and domain_data.get('domain'):
            updated_domain_urn = domain_data['domain'].get('urn')
        
        # Get current connection context
        from web_ui.views import get_current_connection
        current_connection = get_current_connection(request)
        
        # Update local data product with latest remote data
        data_product.name = updated_name
        data_product.description = updated_description
        data_product.external_url = updated_external_url if updated_external_url else None
        data_product.domain_urn = updated_domain_urn
        data_product.sync_status = "SYNCED"
        data_product.last_synced = timezone.now()
        
        # Set connection context
        if current_connection:
            data_product.connection = current_connection
        
        # Update comprehensive data storage
        data_product.properties_data = properties
        data_product.ownership_data = product_data.get('ownership')
        data_product.owners_count = len(product_data.get('ownership', {}).get('owners', []))
        data_product.entities_data = product_data.get('entities')
        data_product.tags_data = product_data.get('tags')
        data_product.glossary_terms_data = product_data.get('glossaryTerms')
        data_product.structured_properties_data = product_data.get('structuredProperties')
        data_product.institutional_memory_data = product_data.get('institutionalMemory')
        
        # Update entity URNs if entities data is available
        entities_data = product_data.get('entities', {})
        if entities_data and entities_data.get('searchResults'):
            # Extract entity URNs from search results
            entity_urns = [result.get('entity', {}).get('urn') for result in entities_data.get('searchResults', []) if result.get('entity', {}).get('urn')]
            data_product.entity_urns = entity_urns
        
        data_product.save()
        
        logger.info(f"Successfully resynced data product: {data_product.name}")
        return JsonResponse({
            "success": True,
            "message": f"Data product '{data_product.name}' resynced with latest DataHub data successfully"
        })
        
    except Exception as e:
        logger.error(f"Error resyncing data product: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@csrf_exempt
def delete_remote_data_product(request):
    """Delete a remote data product from DataHub"""
    if request.method != 'POST':
        return JsonResponse({"success": False, "error": "Method not allowed"})
    
    try:
        data = json.loads(request.body)
        product_urn = data.get('product_urn')
        
        if not product_urn:
            return JsonResponse({"success": False, "error": "Data product URN is required"})
        
        logger.info(f"Deleting remote data product {product_urn}")
        
        # Get DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})
        
        # Delete the data product from DataHub
        result = client.delete_data_product(product_urn)
        
        if result and result.get("success"):
            logger.info(f"Successfully deleted data product from DataHub: {product_urn}")
            
            # Also remove from local storage if it exists
            try:
                local_product = DataProduct.objects.filter(urn=product_urn).first()
                if local_product:
                    local_product_name = local_product.name
                    local_product.delete()
                    logger.info(f"Also removed local copy of data product: {local_product_name}")
                    message = f"Remote data product deleted successfully and local copy removed"
                else:
                    message = f"Remote data product deleted successfully"
            except Exception as e:
                logger.warning(f"Error removing local copy: {str(e)}")
                message = f"Remote data product deleted successfully (local copy removal failed)"
            
            return JsonResponse({
                "success": True,
                "message": message
            })
        else:
            error_msg = result.get("error", "Unknown error") if result else "No response from DataHub"
            logger.error(f"Failed to delete data product from DataHub: {error_msg}")
            return JsonResponse({
                "success": False,
                "error": f"Failed to delete from DataHub: {error_msg}"
            })
        
    except Exception as e:
        logger.error(f"Error deleting remote data product: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@csrf_exempt
def add_remote_data_product_to_pr(request):
    """Add a remote data product to PR workflow"""
    if request.method != 'POST':
        return JsonResponse({"success": False, "error": "Method not allowed"})
    
    try:
        data = json.loads(request.body)
        product_urn = data.get('product_urn')
        
        if not product_urn:
            return JsonResponse({"success": False, "error": "Data product URN is required"})
        
        logger.info(f"Adding remote data product {product_urn} to PR workflow")
        
        # Get DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})
        
        # Fetch the data product details from DataHub
        result = client.list_data_products(query=product_urn, start=0, count=1)
        
        if not result:
            error_msg = 'Failed to fetch data product from DataHub'
            logger.error(f"Failed to fetch data product from DataHub: {error_msg}")
            return JsonResponse({"success": False, "error": error_msg})
        
        search_results = result
        if not search_results:
            return JsonResponse({"success": False, "error": "Data product not found in DataHub"})
        
        # Get the data product data
        product_data = search_results[0]
        if not product_data:
            return JsonResponse({"success": False, "error": "Invalid data product data from DataHub"})
        
        # Extract relevant information
        properties = product_data.get('properties', {})
        name = properties.get('name', 'Unnamed Data Product')
        description = properties.get('description', '')
        external_url = properties.get('externalUrl', '')
        
        # Extract domain information
        domain_urn = None
        domain_data = product_data.get('domain', {})
        if domain_data and domain_data.get('domain'):
            domain_urn = domain_data['domain'].get('urn')
        
        # Get environment from env var with fallback to database
        environment = os.getenv('ENVIRONMENT')
        if not environment:
            try:
                default_env = Environment.objects.filter(is_default=True).first()
                environment = default_env.name if default_env else 'dev'
            except:
                environment = 'dev'
        
        logger.info(f"Using environment: {environment}")
        
        # Create the directory structure
        base_dir = f"metadata-manager/{environment}/data_products"
        os.makedirs(base_dir, exist_ok=True)
        
        # Generate the JSON file for the remote data product operation
        json_data = {
            "operation": "sync_remote",
            "data_product_type": "DATA_PRODUCT",
            "urn": product_urn,
            "name": name,
            "description": description,
            "external_url": external_url,
            "domain_urn": domain_urn,
            "created_at": datetime.now().isoformat(),
            "graphql_input": {
                "query": "getDataProduct",
                "variables": {
                    "urn": product_urn
                }
            },
            "sync_operation": {
                "type": "pull_remote",
                "source": "datahub",
                "target": "local"
            }
        }
        
        # Remove None values
        json_data = {k: v for k, v in json_data.items() if v is not None}
        
        # Generate filename
        sanitized_name = name.replace(' ', '_').replace('/', '_').replace('\\', '_')
        filename = f"sync_remote_{sanitized_name}.json"
        filepath = os.path.join(base_dir, filename)
        
        # Write the JSON file
        with open(filepath, 'w') as f:
            json.dump(json_data, f, indent=2)
        
        logger.info(f"Created remote data product file: {filepath}")
        
        # Create git branch and commit
        try:
            branch_name = f"data_product/{environment}/sync_{name.replace(' ', '-').lower()}"
            github_urls = create_git_branch_and_commit(filepath, branch_name, name, "remote data product")
        except Exception as e:
            logger.error(f"Git operations failed: {str(e)}")
            github_urls = {}
        
        response_data = {
            "success": True,
            "message": f"Remote data product '{name}' added to PR workflow for syncing",
            "file_path": filepath,
            "environment": environment,
        }
        
        # Add GitHub URLs if available
        if github_urls:
            response_data.update(github_urls)
        
        return JsonResponse(response_data)
        
    except Exception as e:
        logger.error(f"Error adding remote data product to PR: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@csrf_exempt
def edit_data_product(request, data_product_id):
    """Edit an existing data product (both local and synced)"""
    try:
        data_product = get_object_or_404(DataProduct, id=data_product_id)
        
        if request.method == "GET":
            # Return data product data for editing
            return JsonResponse({
                "success": True,
                "data_product": {
                    "id": str(data_product.id),
                    "name": data_product.name,
                    "description": data_product.description,
                    "external_url": data_product.external_url,
                    "domain_urn": data_product.domain_urn,
                    "entity_urns": data_product.entity_urns or [],
                    "sync_status": data_product.sync_status,
                    "urn": data_product.urn,
                }
            })
        
        elif request.method == "POST":
            # Update data product
            name = request.POST.get("name")
            description = request.POST.get("description", "")
            external_url = request.POST.get("external_url", "").strip()
            domain_urn = request.POST.get("domain_urn", "").strip()
            entity_urns_str = request.POST.get("entity_urns", "").strip()
            
            if not name:
                return JsonResponse({
                    "success": False,
                    "error": "Data product name is required"
                })
            
            # Parse entity URNs
            entity_urns = []
            if entity_urns_str:
                try:
                    # Split by newlines and commas, filter out empty strings
                    urns = [urn.strip() for line in entity_urns_str.split('\n') 
                           for urn in line.split(',') if urn.strip()]
                    entity_urns = urns
                except Exception as e:
                    logger.warning(f"Error parsing entity URNs: {e}")
            
            # Update basic fields
            data_product.name = name
            data_product.description = description
            data_product.external_url = external_url if external_url else None
            data_product.domain_urn = domain_urn if domain_urn else None
            data_product.entity_urns = entity_urns
            
            # Update sync status if it was synced before
            if data_product.sync_status == "SYNCED":
                data_product.sync_status = "MODIFIED"
            
            data_product.save()
            
            logger.info(f"Successfully updated data product: {data_product.name}")
            
            return JsonResponse({
                "success": True,
                "message": f"Data product '{name}' updated successfully",
                "data_product_id": str(data_product.id),
            })
        
    except Exception as e:
        logger.error(f"Error editing data product: {str(e)}")
        return JsonResponse({
            "success": False,
            "error": str(e)
        })


@csrf_exempt
def create_local_data_product_comprehensive(request):
    """Create a comprehensive local-only data product"""
    if request.method != 'POST':
        return JsonResponse({"success": False, "error": "Method not allowed"})
    
    try:
        # Get basic info
        name = request.POST.get("name")
        description = request.POST.get("description", "")
        external_url = request.POST.get("external_url", "").strip()
        domain_urn = request.POST.get("domain_urn", "").strip()
        entity_urns_str = request.POST.get("entity_urns", "").strip()
        
        if not name:
            return JsonResponse({
                "success": False,
                "error": "Data product name is required"
            })
        
        # Parse entity URNs
        entity_urns = []
        if entity_urns_str:
            try:
                # Split by newlines and commas, filter out empty strings
                urns = [urn.strip() for line in entity_urns_str.split('\n') 
                       for urn in line.split(',') if urn.strip()]
                entity_urns = urns
            except Exception as e:
                logger.warning(f"Error parsing entity URNs: {e}")
        
        # Get current connection context
        from web_ui.views import get_current_connection
        current_connection = get_current_connection(request)
        current_environment = getattr(current_connection, 'environment', 'dev')
        
        # Generate URN using the same system as editable properties export
        from utils.urn_utils import generate_urn_for_new_entity
        deterministic_urn = generate_urn_for_new_entity("dataProduct", name, current_environment)
        
        # Check if data product already exists
        if DataProduct.objects.filter(urn=deterministic_urn).exists():
            return JsonResponse({
                "success": False,
                "error": f"Data product with name '{name}' already exists"
            })
        
        # Create the local data product
        data_product = DataProduct.objects.create(
            name=name,
            description=description,
            urn=deterministic_urn,
            external_url=external_url if external_url else None,
            domain_urn=domain_urn if domain_urn else None,
            entity_urns=entity_urns,
            sync_status="LOCAL_ONLY",  # Mark as local-only
            connection=current_connection  # Set connection context
        )
        
        logger.info(f"Successfully created local data product: {data_product.name} with URN: {data_product.urn}")
        
        return JsonResponse({
            "success": True,
            "message": f"Local data product '{name}' created successfully",
            "data_product_id": str(data_product.id),
            "urn": data_product.urn
        })
        
    except Exception as e:
        logger.error(f"Error creating local data product: {str(e)}")
        return JsonResponse({
            "success": False,
            "error": str(e)
        })


@require_http_methods(["GET"])
def get_data_products(request):
    """
    Get data products from DataHub.
    
    Args:
        request: The HTTP request
        
    Returns:
        JsonResponse: The data products in JSON format
    """
    try:
        # Get query parameters
        query = request.GET.get("query", "*")
        start = int(request.GET.get("start", 0))
        count = int(request.GET.get("count", 20))
        
        logger.info(f"Getting Data Products: query='{query}', start={start}, count={count}")
        
        # Get client using connection system
        from utils.datahub_utils import get_datahub_client_from_request
        client = get_datahub_client_from_request(request)
        if not client:
            return JsonResponse({
                "success": False,
                "error": "No active DataHub connection configured"
            }, status=400)
            
        # Get data products from DataHub
        result = client.list_data_products(
            query=query, start=start, count=count
        )
        
        if not result:
            logger.error(f"Error getting data products from DataHub: No results returned")
            return JsonResponse({
                "success": False,
                "error": "Failed to get data products from DataHub"
            }, status=500)
            
        # Structure the response  
        response_data = {
            "start": start,
            "count": len(result),
            "total": len(result),  # We don't have total count from this method
            "searchResults": [{"entity": product} for product in result]
        }
        
        # Wrap in the expected structure for the frontend
        response = {
            "success": True,
            "data": response_data
        }
        
        logger.info(f"Found {len(result)} data products")
        
        return JsonResponse(response)
    except Exception as e:
        logger.error(f"Error in get_data_products: {str(e)}")
        return JsonResponse({
            "success": False,
            "error": str(e)
        }, status=500)


@method_decorator(require_POST)
def add_data_product_to_staged_changes(request, data_product_id):
    """Add a data product to staged changes by creating comprehensive MCP files"""
    try:
        import json
        import os
        import sys
        from pathlib import Path
        
        # Add project root to path to import our Python modules
        sys.path.append(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        )
        
        # Import the function (using new approach for single MCP file)
        from scripts.mcps.data_product_actions import add_data_product_to_staged_changes_new as add_data_product_mcps
        
        # Get the data product from database
        try:
            data_product = DataProduct.objects.get(id=data_product_id)
        except DataProduct.DoesNotExist:
            return JsonResponse({
                "success": False,
                "error": f"Data product with id {data_product_id} not found"
            }, status=404)
        
        # Extract custom properties from properties_data if available
        # Custom properties can be stored in different formats:
        # 1. As a dictionary in properties_data.customProperties
        # 2. As an array of {key, value} objects in properties_data.customProperties
        custom_properties = {}
        if data_product.properties_data and isinstance(data_product.properties_data, dict):
            stored_custom_props = data_product.properties_data.get("customProperties")
            if stored_custom_props:
                if isinstance(stored_custom_props, dict):
                    # Format 1: Dictionary
                    custom_properties = stored_custom_props
                elif isinstance(stored_custom_props, list):
                    # Format 2: Array of {key, value} objects
                    for prop in stored_custom_props:
                        if isinstance(prop, dict) and 'key' in prop and 'value' in prop:
                            custom_properties[prop['key']] = prop['value']
        
        # Extract other metadata from stored JSON fields
        owners = []
        if data_product.ownership_data and isinstance(data_product.ownership_data, dict):
            ownership_list = data_product.ownership_data.get("owners", [])
            for owner_info in ownership_list:
                owner_urn = owner_info.get("owner")
                if owner_urn:
                    owners.append(owner_urn)
        
        tags = []
        if data_product.tags_data and isinstance(data_product.tags_data, dict):
            tag_list = data_product.tags_data.get("tags", [])
            for tag_info in tag_list:
                tag_urn = tag_info.get("tag")
                if tag_urn:
                    tags.append(tag_urn)
        
        terms = []
        if data_product.glossary_terms_data and isinstance(data_product.glossary_terms_data, dict):
            terms_list = data_product.glossary_terms_data.get("terms", [])
            for term_info in terms_list:
                term_urn = term_info.get("urn")
                if term_urn:
                    terms.append(term_urn)
        
        structured_properties = []
        if data_product.structured_properties_data and isinstance(data_product.structured_properties_data, dict):
            props_list = data_product.structured_properties_data.get("properties", [])
            for prop in props_list:
                prop_urn = prop.get("structuredProperty", {}).get("urn")
                values = prop.get("values", [])
                if prop_urn and values:
                    structured_properties.append({
                        "propertyUrn": prop_urn,
                        "values": values
                    })
        
        links = []
        if data_product.institutional_memory_data and isinstance(data_product.institutional_memory_data, dict):
            elements = data_product.institutional_memory_data.get("elements", [])
            for element in elements:
                url = element.get("url")
                description = element.get("description", "")
                if url:
                    links.append({
                        "url": url,
                        "description": description
                    })
        
        domains = []
        if data_product.domain_urn:
            domains.append(data_product.domain_urn)
        
        # Prepare comprehensive data product data
        data_product_data = {
            "id": str(data_product.id),
            "name": data_product.name,
            "description": data_product.description,
            "urn": data_product.urn,
            "external_url": data_product.external_url,
            "domain_urn": data_product.domain_urn,
            "entity_urns": data_product.entity_urns or [],
            "entities_count": len(data_product.entity_urns or []),
            "sync_status": data_product.sync_status,
            # Add extracted metadata for MCP creation
            "custom_properties": custom_properties,
            "owners": owners,
            "tags": tags,
            "terms": terms,
            "domains": domains,
            "links": links,
            "structured_properties": structured_properties,
        }
        
        # Create staged changes (using new single MCP file approach)
        result = add_data_product_mcps(
            data_product_data=data_product_data,
            environment="dev",
            owner=request.user.username if request.user.is_authenticated else "admin",
            base_dir="metadata-manager"
        )
        
        return JsonResponse({
            "success": True,
            "message": f"Data product '{data_product.name}' added to staged changes",
            "files_created": len(result),
            "file_paths": result
        })
        
    except Exception as e:
        import traceback
        logger.error(f"Error adding data product to staged changes: {str(e)}")
        logger.error(traceback.format_exc())
        
        return JsonResponse({
            "success": False,
            "error": f"An error occurred: {str(e)}"
        }) 


@method_decorator(csrf_exempt, name="dispatch")
class DataProductRemoteAddToStagedChangesView(View):
    """API endpoint to add a remote data product to staged changes without syncing to local first"""
    
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
            
            # Import the function (using new approach for single MCP file)
            from scripts.mcps.data_product_actions import add_data_product_to_staged_changes_new as add_data_product_mcps_remote
            
            data = json.loads(request.body)
            
            # Get the data product data from the request
            product_data = data.get('product_data')
            if not product_data:
                return JsonResponse({
                    "status": "error",
                    "error": "No product_data provided"
                }, status=400)
            
            # Get environment and mutation name
            environment_name = data.get('environment', 'dev')
            mutation_name = data.get('mutation_name')
            
            # Get current user as owner
            owner = request.user.username if request.user.is_authenticated else "admin"
            
            # For remote data products, we need to ensure we have an ID for MCP creation
            # If the remote product doesn't have an ID, we'll generate one from the URN or name
            product_id = product_data.get('id')
            if not product_id:
                if product_data.get('urn'):
                    # Extract ID from URN
                    urn_parts = product_data['urn'].split(':')
                    if len(urn_parts) >= 3:
                        product_id = urn_parts[-1]
                    else:
                        product_id = product_data['urn']
                elif product_data.get('name'):
                    # Use name as ID
                    product_id = product_data['name'].replace(' ', '_').lower()
                else:
                    return JsonResponse({
                        "status": "error",
                        "error": "Remote data product must have either URN or name for ID generation"
                    }, status=400)
            
            # Prepare ownership data
            owners = []
            if product_data.get('ownership_data') and isinstance(product_data['ownership_data'], dict):
                ownership_list = product_data['ownership_data'].get("owners", [])
                for owner_info in ownership_list:
                    owner_urn = owner_info.get("owner_urn")
                    if owner_urn:
                        owners.append(owner_urn)
            
            # Prepare tags from raw_data if available
            tags = []
            raw_data = product_data.get('raw_data')
            if raw_data and isinstance(raw_data, dict):
                global_tags = raw_data.get("globalTags", {})
                if global_tags and global_tags.get("tags"):
                    for tag_info in global_tags["tags"]:
                        tag_urn = tag_info.get("tag", {}).get("urn")
                        if tag_urn:
                            tags.append(tag_urn)
            
            # Prepare glossary terms from raw_data if available
            terms = []
            if raw_data and isinstance(raw_data, dict):
                glossary_terms = raw_data.get("glossaryTerms", {})
                if glossary_terms and glossary_terms.get("terms"):
                    for term_info in glossary_terms["terms"]:
                        term_urn = term_info.get("term", {}).get("urn")
                        if term_urn:
                            terms.append(term_urn)
            
            # Prepare structured properties from raw_data if available
            structured_properties = []
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
            
            # Prepare institutional memory links from raw_data if available
            links = []
            if raw_data and isinstance(raw_data, dict):
                institutional_memory = raw_data.get("institutionalMemory", {})
                if institutional_memory and institutional_memory.get("elements"):
                    for element in institutional_memory["elements"]:
                        url = element.get("url")
                        description = element.get("description", "")
                        if url:
                            links.append({
                                "url": url,
                                "description": description
                            })
            
            # Prepare custom properties from product_data (they come as array of {key, value} objects)
            custom_properties = {}
            if product_data.get('customProperties') and isinstance(product_data['customProperties'], list):
                for prop in product_data['customProperties']:
                    if isinstance(prop, dict) and 'key' in prop and 'value' in prop:
                        custom_properties[prop['key']] = prop['value']
            
            # Get domain URN(s)
            domains = []
            if product_data.get('domain_urn'):
                domains.append(product_data['domain_urn'])
            elif raw_data and isinstance(raw_data, dict):
                domain_info = raw_data.get("domain", {})
                if domain_info and domain_info.get("urn"):
                    domains.append(domain_info["urn"])
            
            # Prepare data for the new function which expects a data_product_data dictionary
            remote_data_product_data = {
                "id": product_id,
                "name": product_data.get('name', 'Unknown'),
                "description": product_data.get('description', ''),
                "external_url": product_data.get('external_url'),
                "owners": owners if owners else [],
                "tags": tags if tags else [],
                "terms": terms if terms else [],
                "domains": domains if domains else [],
                "links": links if links else [],
                "custom_properties": custom_properties if custom_properties else {},
                "structured_properties": structured_properties if structured_properties else [],
                "sub_types": [],  # TODO: Extract sub_types if stored
                "deprecated": False,  # TODO: Extract deprecated status if stored
                "deprecation_note": "",  # TODO: Extract deprecation note if stored
            }
            
            # Add remote data product to staged changes using the new single MCP file function
            result = add_data_product_mcps_remote(
                data_product_data=remote_data_product_data,
                environment=environment_name,
                owner=owner,
                base_dir="metadata-manager"
            )
            
            # The new function returns a dictionary with "mcp_file" -> path
            # Convert this to match the expected response format
            files_created = list(result.values()) if isinstance(result, dict) else []
            files_created_count = len(files_created)
            
            message = f"Remote data product added to staged changes: {files_created_count} file(s) created"
            
            # Return success response
            return JsonResponse({
                "success": True,
                "message": message,
                "files_created": files_created,
                "files_created_count": files_created_count
            })
                
        except Exception as e:
            logger.error(f"Error adding remote data product to staged changes: {str(e)}")
            return JsonResponse({"success": False, "error": str(e)}, status=500)