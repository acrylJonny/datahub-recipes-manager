import logging
from django.shortcuts import render
from django.views import View
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods

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
            connected, client = test_datahub_connection()
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
    """AJAX endpoint to get remote data contracts data"""
    try:
        logger.info("Loading remote data contracts data via AJAX")

        # Get DataHub connection
        connected, client = test_datahub_connection()
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})

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

                # Process the data contracts
                for contract_result in remote_data_contracts_data:
                    if contract_result and isinstance(contract_result, dict):
                        contract_data = contract_result.get("entity", {})
                        if contract_data:
                            # Add sync status information
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
                statistics = {
                    'total_items': len(remote_data_contracts),
                    'synced_count': 0,  # No synced items for now
                    'local_only_count': 0,  # No local items for now
                    'remote_only_count': len(remote_data_contracts),
                    'owned_items': sum(1 for contract in remote_data_contracts if contract and contract.get('ownership', {}) and contract.get('ownership', {}).get('owners')),
                    'items_with_relationships': sum(1 for contract in remote_data_contracts if contract and has_contract_relationships(contract)),
                    'items_with_custom_properties': sum(1 for contract in remote_data_contracts if contract and contract.get('customProperties')),
                    'items_with_structured_properties': sum(1 for contract in remote_data_contracts if contract and contract.get('structuredProperties', {}) and contract.get('structuredProperties', {}).get('properties'))
                }
            except Exception as stats_error:
                logger.warning(f"Error calculating statistics: {stats_error}")
                statistics = {
                    'total_items': len(remote_data_contracts),
                    'synced_count': 0,
                    'local_only_count': 0,
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
                        "synced_items": [],  # Empty for now
                        "local_only_items": [],  # Empty for now
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
        
        # Get client using standard configuration
        client = get_datahub_client()
        if not client:
            return JsonResponse({
                "success": False,
                "error": "Not connected to DataHub"
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