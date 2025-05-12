#!/usr/bin/env python3
"""
Pytest fixtures for DataHub tests.
"""

import os
import sys
import pytest
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))
from utils.datahub_rest_client import DataHubRestClient

# Load environment variables
load_dotenv()


@pytest.fixture(scope="session")
def client() -> DataHubRestClient:
    """
    Provides a configured DataHubRestClient instance for tests.
    This fixture will be available to all test functions.
    Uses environment variables for configuration.
    """
    # Get DataHub configuration from environment variables
    datahub_server = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080")
    datahub_token = os.environ.get("DATAHUB_TOKEN", "")

    # Initialize and return the client
    client = DataHubRestClient(datahub_server, datahub_token)

    # Optionally, check connection and skip tests if not available
    if not client.test_connection():
        pytest.skip(
            "Could not connect to DataHub server. Tests requiring connection will be skipped."
        )

    return client


@pytest.fixture(scope="session")
def source_id() -> str:
    """
    Provides a test ingestion source ID.
    Uses environment variables or falls back to a default.
    """
    return os.environ.get("TEST_SOURCE_ID", "analytics-database-prod")
