import logging
from django.shortcuts import render
from django.views import View
from django.http import JsonResponse

# Add project root to sys.path
import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from utils.datahub_utils import test_datahub_connection

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

        try:
            logger.debug("Fetching remote data contracts from DataHub")

            # Get DataHub URL for direct links
            datahub_url = client.server_url
            if datahub_url.endswith("/api/gms"):
                datahub_url = datahub_url[:-8]  # Remove /api/gms to get base URL

            # Get data contracts from DataHub using the new method
            result = client.get_data_contracts(start=0, count=1000, query="*")

            if result.get("success", False):
                remote_data_contracts_data = result["data"].get("searchResults", [])
                logger.debug(f"Fetched {len(remote_data_contracts_data)} remote data contracts")

                # Process the data contracts
                for contract_result in remote_data_contracts_data:
                    contract_data = contract_result.get("entity", {})
                    if contract_data:
                        remote_data_contracts.append(contract_data)

            else:
                logger.warning(
                    f"Failed to fetch remote data contracts: {result.get('error', 'Unknown error')}"
                )

            return JsonResponse(
                {
                    "success": True,
                    "data": {
                        "remote_data_contracts": remote_data_contracts,
                        "datahub_url": datahub_url,
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