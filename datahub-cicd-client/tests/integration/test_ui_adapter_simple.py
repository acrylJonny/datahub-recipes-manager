#!/usr/bin/env python3
"""
Simple UI Adapter Tests

Tests that validate the DataHub adapter methods that the UI actually calls,
using the correct method names and ensuring they return expected data types.
"""

import pytest
import logging
import sys
import os
from unittest.mock import Mock, patch

# Add the project root to the path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(project_root)

logger = logging.getLogger(__name__)


class TestUIAdapterSimple:
    """Simple test suite for validating adapter methods used by the UI"""

    def test_adapter_import_and_basic_methods(self):
        """Test that the adapter can be imported and basic methods work"""
        
        # Import the adapter
        from utils.datahub_client_adapter import DataHubRestClient
        
        # Create a mock client
        with patch('datahub_cicd_client.core.connection.DataHubConnection') as mock_connection_class:
            mock_connection = Mock()
            mock_connection.test_connection.return_value = True
            mock_connection.execute_graphql.return_value = {
                "data": {
                    "search": {
                        "total": 5,
                        "searchResults": []
                    }
                }
            }
            mock_connection_class.return_value = mock_connection
            
            client = DataHubRestClient("http://localhost:8080", "test-token")
            
            # Test connection method
            assert client.test_connection() is True
            
            # Test safe_execute_graphql method
            result = client.safe_execute_graphql("query { test }")
            assert isinstance(result, dict)

    def test_count_methods_via_adapter(self):
        """Test count methods through the adapter"""
        
        from utils.datahub_client_adapter import DataHubRestClient
        
        with patch('datahub_cicd_client.core.connection.DataHubConnection') as mock_connection_class:
            mock_connection = Mock()
            mock_connection.test_connection.return_value = True
            mock_connection.execute_graphql.return_value = {
                "data": {
                    "search": {
                        "total": 25,
                        "searchResults": []
                    }
                }
            }
            mock_connection_class.return_value = mock_connection
            
            client = DataHubRestClient("http://localhost:8080", "test-token")
            
            # Test data products count (this method exists)
            data_products_count = client.data_product_service.count_data_products()
            assert isinstance(data_products_count, int)
            assert data_products_count >= 0
            
            # Test glossary terms count (this method exists)
            glossary_terms_count = client.glossary_service.count_glossary_terms()
            assert isinstance(glossary_terms_count, int)
            assert glossary_terms_count >= 0

    def test_list_methods_via_adapter(self):
        """Test list methods through the adapter"""
        
        from utils.datahub_client_adapter import DataHubRestClient
        
        with patch('datahub_cicd_client.core.connection.DataHubConnection') as mock_connection_class:
            mock_connection = Mock()
            mock_connection.test_connection.return_value = True
            mock_connection.execute_graphql.return_value = {
                "data": {
                    "search": {
                        "total": 2,
                        "searchResults": [
                            {
                                "entity": {
                                    "urn": "urn:li:tag:test-tag-1",
                                    "type": "TAG",
                                    "properties": {
                                        "name": "Test Tag 1",
                                        "description": "Test description 1",
                                        "colorHex": "#FF5733"
                                    },
                                    "ownership": {
                                        "owners": []
                                    }
                                }
                            }
                        ]
                    }
                }
            }
            mock_connection_class.return_value = mock_connection
            
            client = DataHubRestClient("http://localhost:8080", "test-token")
            
            # Test list_tags method
            tags = client.list_tags()
            assert isinstance(tags, list)
            
            # Test list_domains method
            domains = client.list_domains()
            assert isinstance(domains, list)
            
            # Test list_data_products method
            data_products = client.list_data_products()
            assert isinstance(data_products, list)

    def test_adapter_methods_handle_none_responses(self):
        """Test that adapter methods handle None responses gracefully"""
        
        from utils.datahub_client_adapter import DataHubRestClient
        
        with patch('datahub_cicd_client.core.connection.DataHubConnection') as mock_connection_class:
            mock_connection = Mock()
            mock_connection.test_connection.return_value = True
            mock_connection.execute_graphql.return_value = None
            mock_connection_class.return_value = mock_connection
            
            client = DataHubRestClient("http://localhost:8080", "test-token")
            
            # Test that methods return safe defaults when GraphQL returns None
            tags = client.list_tags()
            assert isinstance(tags, list)
            assert len(tags) == 0
            
            domains = client.list_domains()
            assert isinstance(domains, list)
            assert len(domains) == 0
            
            data_products = client.list_data_products()
            assert isinstance(data_products, list)
            assert len(data_products) == 0
            
            # Test count methods return 0
            if hasattr(client.data_product_service, 'count_data_products'):
                count = client.data_product_service.count_data_products()
                assert isinstance(count, int)
                assert count == 0

    def test_data_contracts_adapter_method(self):
        """Test the data contracts method through the adapter"""
        
        from utils.datahub_client_adapter import DataHubRestClient
        
        with patch('datahub_cicd_client.core.connection.DataHubConnection') as mock_connection_class:
            mock_connection = Mock()
            mock_connection.test_connection.return_value = True
            mock_connection.execute_graphql.return_value = {
                "data": {
                    "search": {
                        "total": 1,
                        "searchResults": [
                            {
                                "entity": {
                                    "urn": "urn:li:dataContract:test-contract-1",
                                    "type": "DATA_CONTRACT",
                                    "properties": {
                                        "entity": "urn:li:dataset:(urn:li:dataPlatform:hive,test.table,PROD)"
                                    },
                                    "info": {
                                        "type": "DATA_QUALITY",
                                        "state": "ACTIVE"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
            mock_connection_class.return_value = mock_connection
            
            client = DataHubRestClient("http://localhost:8080", "test-token")
            
            # Test get_data_contracts method
            result = client.get_data_contracts()
            assert isinstance(result, dict)
            assert "success" in result
            assert "data" in result
            assert result["success"] is True

    def test_glossary_comprehensive_data_adapter_method(self):
        """Test the comprehensive glossary data method through the adapter"""
        
        from utils.datahub_client_adapter import DataHubRestClient
        
        with patch('datahub_cicd_client.core.connection.DataHubConnection') as mock_connection_class:
            mock_connection = Mock()
            mock_connection.test_connection.return_value = True
            
            # Mock the glossary service method directly
            client = DataHubRestClient("http://localhost:8080", "test-token")
            
            # Mock the get_comprehensive_glossary_data method
            with patch.object(client.glossary_service, 'get_comprehensive_glossary_data') as mock_method:
                mock_method.return_value = {
                    "glossary_nodes": [],
                    "glossary_terms": [],
                    "total_nodes": 0,
                    "total_terms": 0
                }
                
                # Test get_comprehensive_glossary_data method
                result = client.get_comprehensive_glossary_data()
                assert isinstance(result, dict)
                assert "glossary_nodes" in result
                assert "glossary_terms" in result
                assert "total_nodes" in result
                assert "total_terms" in result

    def test_users_and_groups_adapter_methods(self):
        """Test user and group methods through the adapter"""
        
        from utils.datahub_client_adapter import DataHubRestClient
        
        with patch('datahub_cicd_client.core.connection.DataHubConnection') as mock_connection_class:
            mock_connection = Mock()
            mock_connection.test_connection.return_value = True
            mock_connection.execute_graphql.return_value = {
                "data": {
                    "listUsers": {
                        "total": 1,
                        "users": [
                            {
                                "urn": "urn:li:corpuser:testuser",
                                "type": "CORP_USER",
                                "info": {
                                    "displayName": "Test User",
                                    "email": "test@example.com",
                                    "active": True
                                }
                            }
                        ]
                    }
                }
            }
            mock_connection_class.return_value = mock_connection
            
            client = DataHubRestClient("http://localhost:8080", "test-token")
            
            # Test list_users method
            users = client.list_users()
            assert isinstance(users, list)
            
            # Test list_groups method
            mock_connection.execute_graphql.return_value = {
                "data": {
                    "listGroups": {
                        "total": 1,
                        "groups": [
                            {
                                "urn": "urn:li:corpGroup:testgroup",
                                "type": "CORP_GROUP",
                                "info": {
                                    "displayName": "Test Group",
                                    "email": "group@example.com"
                                }
                            }
                        ]
                    }
                }
            }
            
            groups = client.list_groups()
            assert isinstance(groups, list)
            
            # Test list_ownership_types method
            mock_connection.execute_graphql.return_value = {
                "data": {
                    "listOwnershipTypes": {
                        "total": 2,
                        "ownershipTypes": [
                            {
                                "urn": "urn:li:ownershipType:__system__business_owner",
                                "info": {
                                    "name": "Business Owner",
                                    "description": "Business owner of the entity"
                                }
                            }
                        ]
                    }
                }
            }
            
            ownership_types = client.list_ownership_types()
            assert isinstance(ownership_types, list)

    def test_adapter_error_handling(self):
        """Test that adapter handles errors gracefully"""
        
        from utils.datahub_client_adapter import DataHubRestClient
        
        with patch('datahub_cicd_client.core.connection.DataHubConnection') as mock_connection_class:
            mock_connection = Mock()
            mock_connection.test_connection.return_value = False
            mock_connection.execute_graphql.side_effect = Exception("Connection failed")
            mock_connection_class.return_value = mock_connection
            
            client = DataHubRestClient("http://localhost:8080", "test-token")
            
            # Test that connection test returns False
            assert client.test_connection() is False
            
            # Test that safe_execute_graphql returns empty dict on exception
            result = client.safe_execute_graphql("query { test }")
            assert isinstance(result, dict)
            assert result == {}

    def test_adapter_with_empty_server_url_raises_error(self):
        """Test that adapter raises error with empty server URL"""
        
        from utils.datahub_client_adapter import DataHubRestClient
        
        # Test that empty server_url raises ValueError
        with pytest.raises(ValueError, match="server_url cannot be None or empty"):
            DataHubRestClient("", "test-token")
        
        with pytest.raises(ValueError, match="server_url cannot be None or empty"):
            DataHubRestClient(None, "test-token")


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"]) 