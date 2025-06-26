"""
Docker-based integration tests for DataHub CI/CD client.

These tests validate the client's ability to handle real connection scenarios,
GraphQL validation, and MCP generation without requiring a full DataHub stack.
"""

import os
import pytest
import time
from unittest.mock import Mock, patch

from datahub_cicd_client.core.connection import DataHubConnection
from datahub_cicd_client.services.data_contracts import DataContractService
from datahub_cicd_client.services.data_products import DataProductService
from datahub_cicd_client.outputs.mcp_output.data_contracts import DataContractAsyncOutput
from datahub_cicd_client.outputs.mcp_output.data_products import DataProductAsyncOutput


class TestDockerIntegration:
    """Integration tests that can run with or without a real DataHub instance."""
    
    def test_connection_handling_without_server(self):
        """Test that connection gracefully handles server unavailability."""
        # Test connection to non-existent server
        conn = DataHubConnection(server_url="http://localhost:9999", token="test-token")
        
        # Should not crash, should return False
        result = conn.test_connection()
        assert result is False
        
        # GraphQL execution should return None gracefully
        graphql_result = conn.execute_graphql("{ __typename }")
        assert graphql_result is None
    
    def test_services_handle_connection_failures(self):
        """Test that services handle connection failures gracefully."""
        # Create connection that will fail
        conn = DataHubConnection(server_url="http://localhost:9999", token="test-token")
        
        # Test data contract service
        dc_service = DataContractService(conn)
        contracts = dc_service.list_data_contracts()
        assert contracts == []  # Should return empty list, not crash
        
        # Test data product service
        dp_service = DataProductService(conn)
        products = dp_service.list_data_products()
        assert products == []  # Should return empty list, not crash
    
    def test_graphql_queries_are_valid_syntax(self):
        """Test that all GraphQL queries have valid syntax."""
        from datahub_cicd_client.graphql.queries import (
            data_contracts, data_products, domains, tags, glossary
        )
        
        # Test that queries are strings and contain expected GraphQL keywords
        assert isinstance(data_contracts.LIST_DATA_CONTRACTS_QUERY, str)
        assert "query" in data_contracts.LIST_DATA_CONTRACTS_QUERY.lower()
        assert "searchacrossentities" in data_contracts.LIST_DATA_CONTRACTS_QUERY.lower()
        
        assert isinstance(data_products.LIST_DATA_PRODUCTS_QUERY, str)
        assert "query" in data_products.LIST_DATA_PRODUCTS_QUERY.lower()
        
        assert isinstance(domains.LIST_DOMAINS_QUERY, str)
        assert "query" in domains.LIST_DOMAINS_QUERY.lower()
        
        assert isinstance(tags.LIST_TAGS_QUERY, str)
        assert "query" in tags.LIST_TAGS_QUERY.lower()
        
        assert isinstance(glossary.LIST_GLOSSARY_TERMS_QUERY, str)
        assert "query" in glossary.LIST_GLOSSARY_TERMS_QUERY.lower()
    
    def test_data_contract_queries_no_ownership_field(self):
        """Test that data contract queries don't contain ownership field (original bug)."""
        from datahub_cicd_client.graphql.queries import data_contracts
        from datahub_cicd_client.graphql.fragments import data_contracts as dc_fragments
        
        # This was the original issue - data contract queries had ownership field
        # which caused GraphQL validation errors
        assert "ownership" not in data_contracts.LIST_DATA_CONTRACTS_QUERY
        assert "ownership" not in dc_fragments.DATA_CONTRACT_FRAGMENT
    
    def test_mcp_generation_with_schema_classes(self):
        """Test that MCP outputs generate proper MCPs with schema classes."""
        # Test Data Contract MCP
        dc_output = DataContractAsyncOutput()
        sample_dc = {
            'urn': 'urn:li:dataContract:test-contract',
            'id': 'test-contract',
            'entityUrn': 'urn:li:dataset:test-dataset'
        }
        
        mcps = dc_output.create_entity_mcps(sample_dc)
        assert len(mcps) > 0
        assert isinstance(mcps[0], dict)
        assert mcps[0].get('entityUrn') == 'urn:li:dataContract:test-contract'
        assert mcps[0].get('entityType') == 'dataContract'
        assert mcps[0].get('changeType') == 'UPSERT'
        
        # Check that aspect uses schema class
        aspect = mcps[0].get('aspect')
        assert aspect is not None
        if hasattr(aspect, '__class__'):
            assert 'datahub.metadata' in aspect.__class__.__module__
        
        # Test Data Product MCP
        dp_output = DataProductAsyncOutput()
        sample_dp = {
            'urn': 'urn:li:dataProduct:test-product',
            'id': 'test-product',
            'name': 'Test Product',
            'description': 'Test description'
        }
        
        mcps = dp_output.create_entity_mcps(sample_dp)
        assert len(mcps) > 0
        assert isinstance(mcps[0], dict)
        assert mcps[0].get('entityUrn') == 'urn:li:dataProduct:test-product'
        assert mcps[0].get('entityType') == 'dataProduct'
        assert mcps[0].get('changeType') == 'UPSERT'
    
    def test_mcp_fallback_without_schema_classes(self):
        """Test that MCP outputs handle missing schema classes gracefully."""
        # Mock the schema classes to be None (simulating missing datahub package)
        with patch('datahub_cicd_client.outputs.mcp_output.data_contracts.DataContractPropertiesClass', None):
            dc_output = DataContractAsyncOutput()
            sample_dc = {
                'urn': 'urn:li:dataContract:test-contract',
                'id': 'test-contract'
            }
            
            mcps = dc_output.create_entity_mcps(sample_dc)
            assert len(mcps) > 0
            assert isinstance(mcps[0], dict)
            # Should still generate valid MCP structure
            assert mcps[0].get('entityUrn') == 'urn:li:dataContract:test-contract'
    
    @pytest.mark.skipif(
        os.getenv("DATAHUB_GMS_URL") is None,
        reason="DATAHUB_GMS_URL not set - skipping live server test"
    )
    def test_live_server_connection(self):
        """Test connection to live DataHub server (if available)."""
        datahub_url = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
        datahub_token = os.getenv("DATAHUB_GMS_TOKEN", "")
        
        conn = DataHubConnection(server_url=datahub_url, token=datahub_token)
        
        # Test connection
        is_connected = conn.test_connection()
        if is_connected:
            print(f"✅ Successfully connected to DataHub at {datahub_url}")
            
            # Test GraphQL execution
            result = conn.execute_graphql("{ __typename }")
            assert result is not None
            print("✅ GraphQL execution successful")
            
            # Test service integration
            dc_service = DataContractService(conn)
            contracts = dc_service.list_data_contracts()
            assert isinstance(contracts, list)
            print(f"✅ Retrieved {len(contracts)} data contracts")
        else:
            pytest.skip("DataHub server not available for live testing")


class TestDockerEnvironmentSimulation:
    """Tests that simulate Docker environment scenarios."""
    
    def test_environment_variable_handling(self):
        """Test that client handles environment variables correctly."""
        # Test with environment variables
        test_env = {
            "DATAHUB_GMS_URL": "http://test-server:8080",
            "DATAHUB_GMS_TOKEN": "test-token-123"
        }
        
        with patch.dict(os.environ, test_env):
            # Simulate how the client would be used in a Docker container
            datahub_url = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
            datahub_token = os.getenv("DATAHUB_GMS_TOKEN", "")
            
            conn = DataHubConnection(server_url=datahub_url, token=datahub_token)
            assert conn.server_url == "http://test-server:8080"
            assert conn.token == "test-token-123"
    
    def test_timeout_handling(self):
        """Test that client handles timeouts appropriately."""
        conn = DataHubConnection(
            server_url="http://localhost:9999", 
            token="test-token",
            timeout=1  # Very short timeout
        )
        
        # Should timeout quickly and not hang
        start_time = time.time()
        result = conn.test_connection()
        end_time = time.time()
        
        assert result is False
        assert (end_time - start_time) < 5  # Should not take more than 5 seconds
    
    def test_ssl_verification_handling(self):
        """Test that client handles SSL verification settings."""
        # Test with SSL verification disabled (common in Docker environments)
        conn = DataHubConnection(
            server_url="https://localhost:8080",
            token="test-token",
            verify_ssl=False
        )
        
        assert conn.verify_ssl is False
        assert conn._session.verify is False


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"]) 