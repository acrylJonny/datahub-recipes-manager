#!/usr/bin/env python

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add the parent directory to the path so we can import the utils package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.datahub_rest_client import DataHubRestClient


def main():
    # Load environment variables from .env file if present
    load_dotenv()

    # Get DataHub connection details
    gms_url = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
    token = os.getenv("DATAHUB_TOKEN")

    print(f"Testing connection to DataHub at: {gms_url}")

    # Initialize client
    client = DataHubRestClient(gms_url, token)

    # Test connection to DataHub
    if client.test_connection():
        print("✅ Successfully connected to DataHub!")
        return 0
    else:
        print("❌ Failed to connect to DataHub")
        return 1


if __name__ == "__main__":
    sys.exit(main())
