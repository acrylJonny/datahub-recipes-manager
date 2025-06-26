import os
import dotenv
from django.conf import settings
from utils.datahub_client_adapter import DataHubRestClient


class DataHubConnectionMiddleware:
    """
    Middleware that makes DataHub connection information available to the request.
    This allows views to access DataHub connection status without repeating code.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Load DataHub connection info
        self._load_datahub_connection(request)

        # Process the request
        response = self.get_response(request)

        return response

    def _load_datahub_connection(self, request):
        """
        Load DataHub connection information from the .env file and
        store it in the request for use in views and templates.
        """
        # Load environment variables
        if os.path.exists(settings.DATAHUB_CONFIG_FILE):
            dotenv.load_dotenv(settings.DATAHUB_CONFIG_FILE)

        # Get connection details
        server_url = os.environ.get("DATAHUB_GMS_URL", "")
        token = os.environ.get("DATAHUB_TOKEN", "")

        # Store connection details in request
        request.datahub_config = {
            "server_url": server_url,
            "token": token,
            "is_token_set": bool(token and token != "your_token_here"),
        }

        # Only test connection if we have a URL
        if server_url:
            try:
                client = DataHubRestClient(
                    server_url=server_url, token=token if token else None
                )
                is_connected = client.test_connection()
                request.datahub_connection = {
                    "connected": is_connected,
                    "message": "Successfully connected to DataHub"
                    if is_connected
                    else "Failed to connect to DataHub",
                    "error": None
                    if is_connected
                    else "Could not establish connection to DataHub server. Please check your settings.",
                }
            except Exception as e:
                request.datahub_connection = {
                    "connected": False,
                    "message": "Failed to connect to DataHub",
                    "error": str(e),
                }
        else:
            request.datahub_connection = {
                "connected": False,
                "message": "DataHub connection not configured",
                "error": "No DataHub server URL configured. Please update your settings.",
            }
