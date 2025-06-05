#!/usr/bin/env python3
"""
Test script for DataHub Glossary API connectivity.
This script tests the connection to various DataHub glossary endpoints.
"""

import argparse
import json
import logging

from utils.datahub_rest_client import DataHubRestClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Default DataHub URL
DEFAULT_DATAHUB_URL = "http://localhost:8080"


def test_glossary_connection(verbose=False):
    """Test connectivity to DataHub glossary endpoints."""

    # Initialize client
    try:
        print(f"Connecting to DataHub at: {DEFAULT_DATAHUB_URL}")
        client = DataHubRestClient(DEFAULT_DATAHUB_URL, "your_datahub_pat_token_here")

        # Basic connectivity test
        sources = client.list_ingestion_sources()
        policies = client.list_policies()

        if sources is not None and policies is not None:
            print("✅ Basic DataHub connection successful\n")
        else:
            print("❌ Basic DataHub connection failed")
            return

        # Test direct glossary API endpoints
        print("Testing glossary endpoints...")
        results = client.test_glossary_connection()

        print("\nGlossary API Test Results:")
        print("----------------------------")
        for endpoint, result in results.items():
            if endpoint not in ["graphql_schema", "graphql_glossary_fields"]:
                # Handle different result formats
                if isinstance(result, dict) and "status_code" in result:
                    status = "✅" if result.get("status_code") == 200 else "❌"
                else:
                    # For boolean results
                    status = "✅" if result else "❌"
                print(f"{status} {endpoint.replace('_', ' ').title()}")

        print("\nDetailed Results:")
        print(json.dumps(results, indent=2))

        # Summarize overall REST API compatibility
        api_endpoints = [
            k
            for k in results.keys()
            if k not in ["graphql_schema", "graphql_glossary_fields"]
        ]

        # Count working APIs based on result format
        working_apis = 0
        for endpoint in api_endpoints:
            result = results[endpoint]
            if isinstance(result, dict) and "status_code" in result:
                if result.get("status_code") == 200:
                    working_apis += 1
            elif result:  # For boolean results
                working_apis += 1

        if working_apis == 0:
            print("\n❌ All glossary API endpoints failed")
            print("Possible issues:")
            print("1. The DataHub instance may not have the glossary module enabled")
            print(
                "2. Your authentication token may not have permission to access glossary data"
            )
            print("3. The API endpoints may have changed in your DataHub version")
        else:
            print(
                f"\n✅ {working_apis}/{len(api_endpoints)} glossary API endpoints working"
            )

        # Test list_glossary_nodes method
        print("\nTesting list_glossary_nodes method...")
        nodes = client.list_glossary_nodes()

        if nodes:
            print(f"✅ Successfully retrieved {len(nodes)} glossary nodes")
            if verbose and nodes:
                print("First node sample:")
                print(json.dumps(nodes[0], indent=2))
        else:
            print("⚠️ No glossary nodes were returned (empty list)")
            print("This could mean either:")
            print("1. There are no glossary nodes in your DataHub instance")
            print("2. The API calls are failing silently")

        # Now try list_glossary_terms method
        print("\nTesting list_glossary_terms method...")
        terms = client.list_glossary_terms(count=10)

        if terms:
            if isinstance(terms, dict):
                num_terms = sum(len(terms_list) for terms_list in terms.values())
                print(
                    f"✅ Successfully retrieved terms for {len(terms)} parent nodes ({num_terms} total terms)"
                )
            else:
                print(f"✅ Successfully retrieved {len(terms)} glossary terms")

            if verbose and terms:
                if isinstance(terms, dict):
                    if terms:
                        print("First parent node with terms:")
                        first_parent = next(iter(terms))
                        print(f"Parent URN: {first_parent}")
                        print(f"Terms: {json.dumps(terms[first_parent][0], indent=2)}")
                else:
                    print("First term sample:")
                    print(json.dumps(terms[0], indent=2))
        else:
            print("⚠️ No glossary terms were returned")
            print("This could mean either:")
            print("1. There are no glossary terms in your DataHub instance")
            print("2. The API calls are failing silently")

        # Test get_glossary_node method
        node_urns = []
        for node in client.list_glossary_nodes():
            node_urns.append(node["urn"])

        if node_urns:
            # Test node with no children
            print("\nTesting get_glossary_node method on node with no child terms...")
            node_urn = node_urns[0]
            node_data = client.get_glossary_node(node_urn)

            if node_data:
                print(f"✅ Successfully retrieved node details for {node_urn}")
                print(f"Node name: {node_data.get('name')}")
                print(f"Node description: {node_data.get('description')}")

                # Print child node information
                child_nodes = node_data.get("child_nodes", [])
                print(f"Child nodes: {len(child_nodes)}")
                for child in child_nodes:
                    print(f"  - {child.get('name')}: {child.get('urn')}")

                # Print child term information
                child_terms = node_data.get("child_terms", [])
                print(f"Child terms: {len(child_terms)}")
                for term in child_terms:
                    print(f"  - {term.get('name')}: {term.get('urn')}")
            else:
                print(f"❌ Failed to retrieve node details for {node_urn}")

            # If we have a second node, test it too (should have a child term)
            if len(node_urns) > 1:
                print("\nTesting get_glossary_node method on node with child terms...")
                node_urn = node_urns[1]
                node_data = client.get_glossary_node(node_urn)

                if node_data:
                    print(f"✅ Successfully retrieved node details for {node_urn}")
                    print(f"Node name: {node_data.get('name')}")
                    print(f"Node description: {node_data.get('description')}")

                    # Print child node information
                    child_nodes = node_data.get("child_nodes", [])
                    print(f"Child nodes: {len(child_nodes)}")
                    for child in child_nodes:
                        print(f"  - {child.get('name')}: {child.get('urn')}")

                    # Print child term information
                    child_terms = node_data.get("child_terms", [])
                    print(f"Child terms: {len(child_terms)}")
                    for term in child_terms:
                        print(f"  - {term.get('name')}: {term.get('urn')}")
                else:
                    print(f"❌ Failed to retrieve node details for {node_urn}")
        else:
            print("\nSkipping get_glossary_node test as no nodes were found")

    except Exception as e:
        print(f"Error testing glossary connection: {str(e)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test DataHub glossary connectivity")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")

    args = parser.parse_args()

    test_glossary_connection(verbose=args.verbose)
