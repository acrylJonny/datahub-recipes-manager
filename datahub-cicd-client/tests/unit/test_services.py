"""
Unit tests for service classes.

These tests mock the GraphQL connection and test service logic without requiring
a running DataHub instance.
"""

import pytest
from unittest.mock import Mock, patch
from typing import Dict, Any, List

from datahub_cicd_client.clients.graphql_connection import GraphQLConnection
from datahub_cicd_client.services.data_contracts import DataContractService
from datahub_cicd_client.services.data_products import DataProductService
from datahub_cicd_client.services.domains import DomainService
from datahub_cicd_client.services.tags import TagService
from datahub_cicd_client.services.glossary import GlossaryService


@pytest.fixture
def mock_connection():
    """Create a mock GraphQL connection."""
    mock_conn = Mock(spec=GraphQLConnection)
    mock_conn.execute_query = Mock()
    return mock_conn


class TestDataContractService:
    """Test data contract service methods."""
    
    def test_list_data_contracts_returns_list(self, mock_connection):
        """Test that list_data_contracts returns a list."""
        mock_connection.execute_query.return_value = {
            "search": {
                "searchResults": [
                    {
                        "entity": {
                            "urn": "urn:li:dataContract:test-contract",
                            "properties": {
                                "name": "Test Contract"
                            }
                        }
                    }
                ]
            }
        }
        
        service = DataContractService(mock_connection)
        result = service.list_data_contracts()
        
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["urn"] == "urn:li:dataContract:test-contract"
        mock_connection.execute_query.assert_called_once()
    
    def test_get_data_contracts_returns_dict_structure(self, mock_connection):
        """Test that get_data_contracts returns proper dictionary structure."""
        mock_connection.execute_query.return_value = {
            "search": {
                "searchResults": [
                    {
                        "entity": {
                            "urn": "urn:li:dataContract:test-contract",
                            "properties": {
                                "name": "Test Contract"
                            }
                        }
                    }
                ]
            }
        }
        
        service = DataContractService(mock_connection)
        result = service.get_data_contracts()
        
        assert isinstance(result, dict)
        assert "success" in result
        assert "data" in result
        assert "searchResults" in result["data"]
        assert result["success"] is True
        assert len(result["data"]["searchResults"]) == 1
    
    def test_handles_graphql_errors_gracefully(self, mock_connection):
        """Test that service handles GraphQL errors gracefully."""
        mock_connection.execute_query.side_effect = Exception("GraphQL error")
        
        service = DataContractService(mock_connection)
        result = service.get_data_contracts()
        
        assert isinstance(result, dict)
        assert "success" in result
        assert result["success"] is False
        assert "error" in result


class TestDataProductService:
    """Test data product service methods."""
    
    def test_list_data_products_returns_list(self, mock_connection):
        """Test that list_data_products returns a list."""
        mock_connection.execute_query.return_value = {
            "search": {
                "searchResults": [
                    {
                        "entity": {
                            "urn": "urn:li:dataProduct:test-product",
                            "properties": {
                                "name": "Test Product"
                            },
                            "ownership": {
                                "owners": []
                            }
                        }
                    }
                ]
            }
        }
        
        service = DataProductService(mock_connection)
        result = service.list_data_products()
        
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["urn"] == "urn:li:dataProduct:test-product"
        mock_connection.execute_query.assert_called_once()
    
    def test_search_data_products_with_query(self, mock_connection):
        """Test searching data products with query."""
        mock_connection.execute_query.return_value = {
            "search": {
                "searchResults": []
            }
        }
        
        service = DataProductService(mock_connection)
        result = service.search_data_products("test query")
        
        assert isinstance(result, list)
        mock_connection.execute_query.assert_called_once()
        
        # Verify query was passed correctly
        call_args = mock_connection.execute_query.call_args
        assert "test query" in str(call_args)


class TestDomainService:
    """Test domain service methods."""
    
    def test_list_domains_returns_list(self, mock_connection):
        """Test that list_domains returns a list."""
        mock_connection.execute_query.return_value = {
            "search": {
                "searchResults": [
                    {
                        "entity": {
                            "urn": "urn:li:domain:test-domain",
                            "properties": {
                                "name": "Test Domain"
                            },
                            "ownership": {
                                "owners": []
                            }
                        }
                    }
                ]
            }
        }
        
        service = DomainService(mock_connection)
        result = service.list_domains()
        
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["urn"] == "urn:li:domain:test-domain"
        mock_connection.execute_query.assert_called_once()


class TestTagService:
    """Test tag service methods."""
    
    def test_list_tags_returns_list(self, mock_connection):
        """Test that list_tags returns a list."""
        mock_connection.execute_query.return_value = {
            "search": {
                "searchResults": [
                    {
                        "entity": {
                            "urn": "urn:li:tag:test-tag",
                            "properties": {
                                "name": "Test Tag"
                            },
                            "ownership": {
                                "owners": []
                            }
                        }
                    }
                ]
            }
        }
        
        service = TagService(mock_connection)
        result = service.list_tags()
        
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["urn"] == "urn:li:tag:test-tag"
        mock_connection.execute_query.assert_called_once()


class TestGlossaryService:
    """Test glossary service methods."""
    
    def test_list_glossary_terms_returns_list(self, mock_connection):
        """Test that list_glossary_terms returns a list."""
        mock_connection.execute_query.return_value = {
            "search": {
                "searchResults": [
                    {
                        "entity": {
                            "urn": "urn:li:glossaryTerm:test-term",
                            "glossaryTermInfo": {
                                "name": "Test Term"
                            },
                            "ownership": {
                                "owners": []
                            }
                        }
                    }
                ]
            }
        }
        
        service = GlossaryService(mock_connection)
        result = service.list_glossary_terms()
        
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["urn"] == "urn:li:glossaryTerm:test-term"
        mock_connection.execute_query.assert_called_once()
    
    def test_list_glossary_nodes_returns_list(self, mock_connection):
        """Test that list_glossary_nodes returns a list."""
        mock_connection.execute_query.return_value = {
            "search": {
                "searchResults": [
                    {
                        "entity": {
                            "urn": "urn:li:glossaryNode:test-node",
                            "glossaryNodeInfo": {
                                "name": "Test Node"
                            }
                        }
                    }
                ]
            }
        }
        
        service = GlossaryService(mock_connection)
        result = service.list_glossary_nodes()
        
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["urn"] == "urn:li:glossaryNode:test-node"
        mock_connection.execute_query.assert_called_once()


class TestServiceErrorHandling:
    """Test error handling across all services."""
    
    @pytest.mark.parametrize("service_class", [
        DataContractService,
        DataProductService,
        DomainService,
        TagService,
        GlossaryService,
    ])
    def test_services_handle_connection_errors(self, mock_connection, service_class):
        """Test that all services handle connection errors gracefully."""
        mock_connection.execute_query.side_effect = Exception("Connection error")
        
        service = service_class(mock_connection)
        
        # Try to call a method that exists on the service
        if hasattr(service, 'list_data_contracts'):
            result = service.list_data_contracts()
        elif hasattr(service, 'list_data_products'):
            result = service.list_data_products()
        elif hasattr(service, 'list_domains'):
            result = service.list_domains()
        elif hasattr(service, 'list_tags'):
            result = service.list_tags()
        elif hasattr(service, 'list_glossary_terms'):
            result = service.list_glossary_terms()
        else:
            pytest.skip(f"No suitable method found for {service_class.__name__}")
        
        # Should return empty list or error structure, not raise exception
        assert result is not None
        if isinstance(result, list):
            assert len(result) == 0
        elif isinstance(result, dict):
            assert "success" in result
            assert result["success"] is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 