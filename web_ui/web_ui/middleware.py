"""
Middleware for the web UI.
"""

import os
import sys

# Add parent directory to path to import utils
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)


class DataHubConnectionMiddleware:
    """
    Middleware to handle DataHub connection information.
    """

    def __init__(self, get_response):
        self.get_response = get_response
        # One-time configuration and initialization

    def __call__(self, request):
        # Code to be executed for each request before the view is called

        # Try to check DataHub connection status if it's not in the session
        if "datahub_connected" not in request.session:
            try:
                # Import here to avoid issues during project setup
                from utils.datahub_rest_client import DataHubRestClient

                # Get DataHub connection details from environment
                datahub_url = os.environ.get("DATAHUB_GMS_URL", "")
                datahub_token = os.environ.get("DATAHUB_TOKEN", "")

                if datahub_url:
                    # Create client and test connection
                    client = DataHubRestClient(
                        server_url=datahub_url, token=datahub_token
                    )
                    connected = client.test_connection()
                    request.session["datahub_connected"] = connected
                else:
                    request.session["datahub_connected"] = False
            except Exception:
                # If there's any error, assume we're not connected
                request.session["datahub_connected"] = False

        # Process the request
        response = self.get_response(request)

        # Code to be executed for each request/response after the view is called

        return response
