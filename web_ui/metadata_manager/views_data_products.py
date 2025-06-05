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


class DataProductListView(View):
    """View to list data products"""

    def get(self, request):
        """Display list of data products"""
        try:
            logger.info("Starting DataProductListView.get")

            # Get DataHub connection info (quick test only)
            logger.debug("Testing DataHub connection from DataProductListView")
            connected, client = test_datahub_connection()
            logger.debug(f"DataHub connection test result: {connected}")

            # Initialize context with local data only - remote data loaded via AJAX
            context = {
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


def get_remote_data_products_data(request):
    """AJAX endpoint to get remote data products data"""
    try:
        logger.info("Loading remote data products data via AJAX")

        # Get DataHub connection
        connected, client = test_datahub_connection()
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})

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
            result = client.get_data_products(start=0, count=1000, query="*")

            if result.get("success", False):
                remote_data_products_data = result["data"].get("searchResults", [])
                logger.debug(f"Fetched {len(remote_data_products_data)} remote data products")

                # Process the data products
                for product_result in remote_data_products_data:
                    product_data = product_result.get("entity", {})
                    if product_data:
                        remote_data_products.append(product_data)

            else:
                logger.warning(
                    f"Failed to fetch remote data products: {result.get('error', 'Unknown error')}"
                )

            return JsonResponse(
                {
                    "success": True,
                    "data": {
                        "remote_data_products": remote_data_products,
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