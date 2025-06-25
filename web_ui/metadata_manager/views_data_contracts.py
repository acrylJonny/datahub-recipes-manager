import logging
from django.shortcuts import render
from django.views import View
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods, require_POST
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt

# Add project root to sys.path
import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from utils.datahub_utils import get_datahub_client, test_datahub_connection

logger = logging.getLogger(__name__)


class DataContractListView(View):
    """View to list data contracts"""

    def get(self, request):
        """Display list of data contracts"""
        try:
            logger.info("Starting DataContractListView.get")

            # Get DataHub connection info (quick test only)
            logger.debug("Testing DataHub connection from DataContractListView")
            from utils.datahub_utils import test_datahub_connection
            connected, client = test_datahub_connection(request)
            logger.debug(f"DataHub connection test result: {connected}")

            # Initialize context with local data only - remote data loaded via AJAX
            context = {
                "remote_data_contracts": [],  # Will be populated via AJAX
                "has_datahub_connection": connected,
                "datahub_url": None,  # Will be populated via AJAX
                "page_title": "Data Contracts",
            }

            logger.info("Rendering data contract list template (async loading)")
            return render(request, "metadata_manager/data_contracts/list.html", context)
        except Exception as e:
            logger.error(f"Error in data contract list view: {str(e)}")
            return render(
                request,
                "metadata_manager/data_contracts/list.html",
                {"error": str(e), "page_title": "Data Contracts"},
            )


def get_remote_data_contracts_data(request):
    """AJAX endpoint to get both local and remote data contracts data"""
    try:
        logger.info("Loading data contracts data via AJAX")

        # Get DataHub connection using connection system
        from utils.datahub_utils import test_datahub_connection
        from web_ui.views import get_current_connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "No active DataHub connection configured"})

        # Get current connection for local data
        connection = get_current_connection(request)

        # Load local data contracts
        from .models import DataContract
        local_contracts = list(DataContract.objects.filter(connection=connection))
        
        # Prepare local data
        local_only_items = []
        synced_items = []
        
        for contract in local_contracts:
            contract_dict = {
                'id': str(contract.id),
                'urn': contract.urn,
                'name': contract.name,
                'description': contract.description,
                'entity_urn': contract.entity_urn,
                'state': contract.state,
                'result_type': contract.result_type,
                'dataset_name': contract.dataset_name,
                'dataset_platform': contract.dataset_platform,
                'dataset_platform_instance': contract.dataset_platform_instance,
                'dataset_browse_path': contract.dataset_browse_path,
                'sync_status': contract.sync_status,
                'sync_status_display': contract.get_sync_status_display(),
                'last_synced': contract.last_synced.isoformat() if contract.last_synced else None,
                'properties': contract.properties_data,
                'status': contract.status_data,
                'result': contract.result_data,
                'structuredProperties': contract.structured_properties_data,
                'dataset_info': contract.dataset_info_data,
            }
            
            if contract.sync_status == 'LOCAL_ONLY':
                local_only_items.append(contract_dict)
            elif contract.sync_status in ['SYNCED', 'MODIFIED']:
                synced_items.append(contract_dict)

        # Fetch remote data contracts
        remote_data_contracts = []
        datahub_url = None
        datahub_token = None

        try:
            logger.debug("Fetching remote data contracts from DataHub")

            # Get DataHub URL and token for direct links
            datahub_url = client.server_url
            if datahub_url.endswith("/api/gms"):
                datahub_url = datahub_url[:-8]  # Remove /api/gms to get base URL
            
            # Get token if available
            if hasattr(client, 'token'):
                datahub_token = client.token

            # Get data contracts from DataHub using the new method
            result = client.get_data_contracts(start=0, count=1000, query="*")

            if result and result.get("success", False) and result.get("data"):
                remote_data_contracts_data = result["data"].get("searchResults", [])
                logger.debug(f"Fetched {len(remote_data_contracts_data)} remote data contracts")

                # Process the data contracts and check if they exist locally
                local_urns = {contract.urn for contract in local_contracts if contract.urn}
                
                for contract_result in remote_data_contracts_data:
                    if contract_result and isinstance(contract_result, dict):
                        contract_data = contract_result.get("entity", {})
                        if contract_data:
                            contract_urn = contract_data.get('urn')
                            
                            # Check if this contract is already synced locally
                            if contract_urn in local_urns:
                                # Skip remote-only list if already synced
                                continue
                            else:
                                # Add sync status information for remote-only items
                                contract_data['sync_status'] = 'REMOTE_ONLY'
                                contract_data['sync_status_display'] = 'Remote Only'
                                remote_data_contracts.append(contract_data)

            else:
                error_msg = "Unknown error"
                if result:
                    error_msg = result.get('error', 'No data returned')
                else:
                    error_msg = "No result returned from DataHub"
                logger.warning(f"Failed to fetch remote data contracts: {error_msg}")

            # Calculate statistics
            try:
                all_contracts = synced_items + local_only_items + remote_data_contracts
                statistics = {
                    'total_items': len(all_contracts),
                    'synced_count': len(synced_items),
                    'local_only_count': len(local_only_items),
                    'remote_only_count': len(remote_data_contracts),
                    'owned_items': sum(1 for contract in all_contracts if contract and contract.get('ownership', {}) and contract.get('ownership', {}).get('owners')),
                    'items_with_relationships': sum(1 for contract in all_contracts if contract and has_contract_relationships(contract)),
                    'items_with_custom_properties': sum(1 for contract in all_contracts if contract and contract.get('customProperties')),
                    'items_with_structured_properties': sum(1 for contract in all_contracts if contract and contract.get('structuredProperties', {}) and contract.get('structuredProperties', {}).get('properties'))
                }
            except Exception as stats_error:
                logger.warning(f"Error calculating statistics: {stats_error}")
                statistics = {
                    'total_items': len(synced_items) + len(local_only_items) + len(remote_data_contracts),
                    'synced_count': len(synced_items),
                    'local_only_count': len(local_only_items),
                    'remote_only_count': len(remote_data_contracts),
                    'owned_items': 0,
                    'items_with_relationships': 0,
                    'items_with_custom_properties': 0,
                    'items_with_structured_properties': 0
                }

            return JsonResponse(
                {
                    "success": True,
                    "data": {
                        "remote_data_contracts": remote_data_contracts,
                        "synced_items": synced_items,
                        "local_only_items": local_only_items,
                        "remote_only_items": remote_data_contracts,
                        "datahub_url": datahub_url,
                        "datahub_token": datahub_token,
                        "statistics": statistics
                    },
                }
            )

        except Exception as e:
            logger.error(f"Error fetching remote data contract data: {str(e)}")
            return JsonResponse(
                {
                    "success": False,
                    "error": f"Error fetching remote data contracts: {str(e)}",
                }
            )

    except Exception as e:
        logger.error(f"Error in get_remote_data_contracts_data: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


def has_contract_relationships(contract):
    """Check if a contract has relationships (assertions, etc.)"""
    if not contract or not isinstance(contract, dict):
        return False
    
    properties = contract.get('properties', {})
    if not properties:
        return False
        
    return bool(
        properties.get('freshness') or
        properties.get('schema') or
        properties.get('dataQuality') or
        contract.get('relationships')
    )


@require_http_methods(["GET"])
def get_data_contracts(request):
    """
    Get data contracts from DataHub.
    
    Args:
        request: The HTTP request
        
    Returns:
        JsonResponse: The data contracts in JSON format
    """
    try:
        # Get query parameters
        query = request.GET.get("query", "*")
        start = int(request.GET.get("start", 0))
        count = int(request.GET.get("count", 20))
        
        logger.info(f"Getting Data Contracts: query='{query}', start={start}, count={count}")
        
        # Get client using connection system
        from utils.datahub_utils import get_datahub_client_from_request
        client = get_datahub_client_from_request(request)
        if not client:
            return JsonResponse({
                "success": False,
                "error": "No active DataHub connection configured"
            }, status=400)
            
        # Get data contracts from DataHub
        result = client.get_data_contracts(
            start=start,
            count=count,
            query=query
        )
        
        if not result.get("success", False):
            logger.error(f"Error getting data contracts from DataHub: {result.get('error', 'Unknown error')}")
            return JsonResponse({
                "success": False,
                "error": result.get("error", "Failed to get data contracts from DataHub")
            }, status=500)
            
        # Structure the response  
        response_data = {
            "start": result["data"].get("start", 0),
            "count": result["data"].get("count", 0),
            "total": result["data"].get("total", 0),
            "searchResults": result["data"].get("searchResults", [])
        }
        
        # Wrap in the expected structure for the frontend
        response = {
            "success": True,
            "data": response_data
        }
        
        logger.info(f"Found {len(response_data['searchResults'])} data contracts")
        
        return JsonResponse(response)
    except Exception as e:
        logger.error(f"Error in get_data_contracts: {str(e)}")
        return JsonResponse({
            "success": False,
            "error": str(e)
        }, status=500)


@csrf_exempt
@require_POST
def sync_data_contract_to_local(request):
    """Sync a data contract from DataHub to local database"""
    try:
        import json
        from .models import DataContract
        from utils.datahub_utils import test_datahub_connection
        from web_ui.views import get_current_connection
        
        logger.info(f"Sync contract to local called. Request content type: {request.content_type}")
        logger.info(f"Request body: {request.body}")
        
        # Get the data contract URN from the request
        try:
            data = json.loads(request.body)
            logger.info(f"Parsed JSON data: {data}")
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {str(e)}")
            logger.error(f"Request body was: {request.body}")
            return JsonResponse({
                "success": False,
                "error": f"Invalid JSON data: {str(e)}"
            }, status=400)
        
        contract_urn = data.get("urn")
        if not contract_urn:
            logger.error("No URN provided in request")
            return JsonResponse({
                "success": False,
                "error": "Data contract URN is required"
            }, status=400)
        
        logger.info(f"Processing contract URN: {contract_urn}")
        
        # Get DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            logger.error("No DataHub connection available")
            return JsonResponse({
                "success": False,
                "error": "No active DataHub connection configured"
            }, status=400)
        
        # Get current connection for database storage
        connection = get_current_connection(request)
        logger.info(f"Using connection: {connection}")
        
        # Fetch the data contract from DataHub
        try:
            logger.info("Fetching data contracts from DataHub...")
            # Get data contracts using the existing method
            result = client.get_data_contracts(start=0, count=1000, query="*")
            if not result.get("success", False):
                logger.error(f"Failed to fetch data contracts: {result}")
                return JsonResponse({
                    "success": False,
                    "error": "Failed to fetch data contracts from DataHub"
                }, status=500)
            
            # Find the specific contract
            contract_data = None
            search_results = result["data"].get("searchResults", [])
            logger.info(f"Found {len(search_results)} contracts in DataHub")
            
            for contract_result in search_results:
                if contract_result and contract_result.get("entity", {}).get("urn") == contract_urn:
                    contract_data = contract_result.get("entity", {})
                    logger.info(f"Found matching contract: {contract_data.get('urn')}")
                    break
            
            if not contract_data:
                logger.error(f"Contract with URN {contract_urn} not found in {len(search_results)} results")
                return JsonResponse({
                    "success": False,
                    "error": f"Data contract with URN {contract_urn} not found"
                }, status=404)
                
        except Exception as e:
            logger.error(f"Error fetching data contract: {str(e)}")
            return JsonResponse({
                "success": False,
                "error": f"Error fetching data contract: {str(e)}"
            }, status=500)
        
        # Create or update the data contract in local database
        try:
            logger.info("Creating/updating contract in local database...")
            contract = DataContract.create_from_datahub(contract_data, connection=connection)
            logger.info(f"Successfully created/updated contract: {contract.id} - {contract.name}")
            
            return JsonResponse({
                "success": True,
                "message": f"Data contract '{contract.name}' synced to local database",
                "contract_id": str(contract.id),
                "contract_name": contract.name
            })
            
        except Exception as e:
            logger.error(f"Error creating/updating data contract in database: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return JsonResponse({
                "success": False,
                "error": f"Error saving to database: {str(e)}"
            }, status=500)
        
    except Exception as e:
        logger.error(f"Error syncing data contract to local: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return JsonResponse({
            "success": False,
            "error": f"An error occurred: {str(e)}"
        }, status=500)


@method_decorator(require_POST)
def resync_data_contract(request, contract_id):
    """Resync a data contract from DataHub"""
    try:
        from .models import DataContract
        from utils.datahub_utils import test_datahub_connection
        
        # Get the contract
        try:
            contract = DataContract.objects.get(id=contract_id)
        except DataContract.DoesNotExist:
            return JsonResponse({
                "success": False,
                "error": "Data contract not found"
            }, status=404)
        
        # Get DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({
                "success": False,
                "error": "No active DataHub connection configured"
            }, status=400)
        
        # Fetch fresh data from DataHub
        try:
            if not contract.urn:
                return JsonResponse({
                    "success": False,
                    "error": "Contract has no URN and cannot be resynced"
                }, status=400)
            
            # Get data contracts using the existing method
            result = client.get_data_contracts(start=0, count=1000, query="*")
            if not result.get("success", False):
                return JsonResponse({
                    "success": False,
                    "error": "Failed to fetch data contracts from DataHub"
                }, status=500)
            
            # Find the specific contract
            contract_data = None
            for contract_result in result["data"].get("searchResults", []):
                if contract_result and contract_result.get("entity", {}).get("urn") == contract.urn:
                    contract_data = contract_result.get("entity", {})
                    break
            
            if not contract_data:
                return JsonResponse({
                    "success": False,
                    "error": f"Contract '{contract.name}' not found in DataHub"
                }, status=404)
            
            # Update the contract with fresh data
            updated_contract = DataContract.create_from_datahub(contract_data, connection=contract.connection)
            
            return JsonResponse({
                "success": True,
                "message": f"Contract '{updated_contract.name}' resynced successfully from DataHub"
            })
            
        except Exception as e:
            logger.error(f"Error resyncing contract {contract.name}: {str(e)}")
            return JsonResponse({
                "success": False,
                "error": f"Error resyncing contract from DataHub: {str(e)}"
            }, status=500)
        
    except Exception as e:
        logger.error(f"Error in resync data contract: {str(e)}")
        return JsonResponse({
            "success": False,
            "error": f"An error occurred: {str(e)}"
        }, status=500)


@method_decorator(require_POST)
def add_data_contract_to_staged_changes(request):
    """Add a data contract to staged changes by creating comprehensive MCP files"""
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
        from scripts.mcps.data_contract_actions import add_data_contract_to_staged_changes_legacy as add_contract_mcps
        
        # Get the data contract URN from the request
        contract_urn = request.POST.get("contract_urn")
        contract_id = request.POST.get("contract_id")
        
        if not contract_urn and not contract_id:
            return JsonResponse({
                "success": False,
                "error": "Data contract URN or ID is required"
            }, status=400)
        
        # If we have a contract_id, get the contract from database
        if contract_id:
            try:
                from .models import DataContract
                contract = DataContract.objects.get(id=contract_id)
                contract_urn = contract.urn
                
                if not contract_urn:
                    return JsonResponse({
                        "success": False,
                        "error": "Contract has no URN"
                    }, status=400)
                    
            except DataContract.DoesNotExist:
                return JsonResponse({
                    "success": False,
                    "error": f"Data contract with ID {contract_id} not found"
                }, status=404)
        
        # Get DataHub connection
        from utils.datahub_utils import test_datahub_connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({
                "success": False,
                "error": "No active DataHub connection configured"
            }, status=400)
        
        # Fetch the data contract from DataHub
        try:
            contract_data = client.get_data_contract(contract_urn)
            if not contract_data:
                return JsonResponse({
                    "success": False,
                    "error": f"Data contract with URN {contract_urn} not found"
                }, status=404)
        except Exception as e:
            return JsonResponse({
                "success": False,
                "error": f"Error fetching data contract: {str(e)}"
            }, status=500)
        
        # Extract contract ID from URN for file naming
        contract_id_for_file = contract_urn.split(":")[-1] if ":" in contract_urn else contract_urn
        
        # Prepare data contract data
        properties = contract_data.get("properties", {})
        contract_data_processed = {
            "id": contract_id_for_file,
            "urn": contract_urn,
            "entity_urn": properties.get("entity", {}).get("urn") if properties.get("entity") else None,
            "properties": properties,
            "sync_status": "REMOTE_ONLY",
        }
        
        # Create staged changes
        result = add_contract_mcps(
            contract_data=contract_data_processed,
            environment="dev",
            owner=request.user.username if request.user.is_authenticated else "admin",
            base_dir="metadata-manager"
        )
        
        return JsonResponse({
            "success": True,
            "message": f"Data contract '{contract_id_for_file}' added to staged changes",
            "files_created": len(result),
            "file_paths": result
        })
        
    except Exception as e:
        import traceback
        logger.error(f"Error adding data contract to staged changes: {str(e)}")
        logger.error(traceback.format_exc())
        
        return JsonResponse({
            "success": False,
            "error": f"An error occurred: {str(e)}"
        })


@method_decorator(csrf_exempt, name="dispatch")
class DataContractAddAllToStagedChangesView(View):
    """API view for adding all data contracts to staged changes"""
    
    def post(self, request):
        try:
            import json
            import os
            import sys
            
            data = json.loads(request.body)
            environment = data.get('environment', 'dev')
            mutation_name = data.get('mutation_name')
            
            # Get current connection to filter contracts by connection
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            
            # Get all data contracts for current connection
            from .models import DataContract
            contracts = DataContract.objects.filter(connection=current_connection)
            
            if not contracts:
                return JsonResponse({
                    'success': False,
                    'error': 'No data contracts found to add to staged changes for current connection'
                }, status=400)
            
            # Add project root to path to import our Python modules
            sys.path.append(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            )
            
            # Import the function
            from scripts.mcps.data_contract_actions import add_data_contract_to_staged_changes_legacy as add_contract_mcps
            
            success_count = 0
            error_count = 0
            files_created_count = 0
            files_skipped_count = 0
            errors = []
            all_created_files = []
            
            for contract in contracts:
                try:
                    # Extract contract ID from URN for file naming
                    contract_id_for_file = contract.urn.split(":")[-1] if contract.urn and ":" in contract.urn else str(contract.id)
                    
                    # Prepare data contract data
                    contract_data_processed = {
                        "id": contract_id_for_file,
                        "urn": contract.urn,
                        "entity_urn": contract.entity_urn,
                        "properties": contract.properties_data or {},
                        "sync_status": contract.sync_status,
                    }
                    
                    # Add contract to staged changes
                    created_files = add_contract_mcps(
                        contract_data=contract_data_processed,
                        environment=environment,
                        owner=request.user.username if request.user.is_authenticated else "admin",
                        base_dir="metadata-manager"
                    )
                    
                    success_count += 1
                    files_created_count += len(created_files)
                    all_created_files.extend(created_files)
                    
                except Exception as e:
                    error_count += 1
                    errors.append(f"Contract {contract.name or contract.urn}: {str(e)}")
                    logger.error(f"Error adding contract {contract.name or contract.urn} to staged changes: {str(e)}")
            
            # Calculate total files that could have been created
            total_possible_files = success_count * 1  # single MCP file for each contract
            files_skipped_count = total_possible_files - files_created_count
            
            message = f"Add all to staged changes completed: {success_count} contracts processed, {files_created_count} files created, {files_skipped_count} files skipped (unchanged), {error_count} failed"
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
            logger.error(f"Error adding all data contracts to staged changes: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500) 