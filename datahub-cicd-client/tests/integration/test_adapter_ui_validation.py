#!/usr/bin/env python3
"""
Adapter UI Validation Tests

Tests that validate the DataHub adapter methods that the UI actually calls,
ensuring they return the expected data structures and handle edge cases properly.
"""

import pytest
import logging
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any, List

# Import the adapter
import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from utils.datahub_client_adapter import DataHubRestClient

logger = logging.getLogger(__name__)


class TestAdapterUIValidation:
    """Test suite for validating adapter methods used by the UI"""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock DataHub connection"""
        connection = Mock()
        connection.test_connection.return_value = True
        connection.execute_graphql.return_value = {"data": {}}
        return connection

    @pytest.fixture
    def client(self, mock_connection):
        """Create a DataHub client with mocked connection"""
        with patch('datahub_cicd_client.core.connection.DataHubConnection') as mock_connection_class:
            mock_connection_class.return_value = mock_connection
            return DataHubRestClient("http://localhost:8080", "test-token")

    def test_metadata_manager_index_counts(self, client, mock_connection):
        """Test all count methods used by the metadata manager index page"""
        
        # Mock GraphQL response for count queries
        mock_connection.execute_graphql.return_value = {
            "data": {
                "search": {
                    "total": 25,
                    "searchResults": []
                }
            }
        }
        
        # Test all count methods that the UI uses
        count_methods = [
            ('tags', client.tag_service.count_tags),
            ('domains', client.domain_service.count_domains),
            ('glossary_terms', client.glossary_service.count_glossary_terms),
            ('data_products', client.data_product_service.count_data_products),
            ('assertions', client.assertion_service.count_assertions),
            ('metadata_tests', client.metadata_test_service.count_metadata_tests),
            ('data_contracts', client.data_contract_service.count_data_contracts),
            ('structured_properties', client.properties_service.count_structured_properties),
        ]
        
        for entity_type, count_method in count_methods:
            result = count_method()
            assert isinstance(result, int), f"{entity_type} count should return int, got {type(result)}"
            assert result >= 0, f"{entity_type} count should be non-negative, got {result}"

    def test_list_tags_ui_structure(self, client, mock_connection):
        """Test list_tags method returns structure expected by UI"""
        
        # Mock response with all fields the UI expects
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
                                    "owners": [
                                        {
                                            "owner": {
                                                "urn": "urn:li:corpuser:testuser",
                                                "type": "CORP_USER",
                                                "info": {
                                                    "displayName": "Test User",
                                                    "email": "test@example.com"
                                                }
                                            },
                                            "type": "BUSINESS_OWNER"
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        }
        
        # Test the adapter method
        result = client.list_tags(query="*", start=0, count=100)
        
        # Validate structure
        assert isinstance(result, list)
        assert len(result) == 2
        
        tag = result[0]
        # Check required fields for UI
        required_fields = ['urn', 'name', 'description', 'colorHex', 'owners', 'owners_count']
        for field in required_fields:
            assert field in tag, f"Tag missing required field: {field}"
        
        # Check data types
        assert isinstance(tag['urn'], str)
        assert isinstance(tag['name'], str)
        assert isinstance(tag['description'], str)
        assert isinstance(tag['owners'], list)
        assert isinstance(tag['owners_count'], int)

    def test_list_domains_ui_structure(self, client, mock_connection):
        """Test list_domains method returns structure expected by UI"""
        
        # Mock response with all fields the UI expects
        mock_connection.execute_graphql.return_value = {
            "data": {
                "search": {
                    "total": 1,
                    "searchResults": [
                        {
                            "entity": {
                                "urn": "urn:li:domain:test-domain-1",
                                "type": "DOMAIN",
                                "properties": {
                                    "name": "Test Domain 1",
                                    "description": "Test domain description 1"
                                },
                                "ownership": {
                                    "owners": [
                                        {
                                            "owner": {
                                                "urn": "urn:li:corpuser:testuser",
                                                "type": "CORP_USER",
                                                "info": {
                                                    "displayName": "Test User"
                                                }
                                            },
                                            "type": "BUSINESS_OWNER"
                                        }
                                    ]
                                },
                                "parentDomains": {
                                    "domains": []
                                }
                            }
                        }
                    ]
                }
            }
        }
        
        # Test the adapter method
        result = client.list_domains(query="*", start=0, count=100)
        
        # Validate structure
        assert isinstance(result, list)
        assert len(result) == 1
        
        domain = result[0]
        # Check required fields for UI
        required_fields = ['urn', 'name', 'description', 'owners', 'owners_count', 'parent_domains']
        for field in required_fields:
            assert field in domain, f"Domain missing required field: {field}"
        
        # Check data types
        assert isinstance(domain['urn'], str)
        assert isinstance(domain['name'], str)
        assert isinstance(domain['description'], str)
        assert isinstance(domain['owners'], list)
        assert isinstance(domain['owners_count'], int)
        assert isinstance(domain['parent_domains'], list)

    def test_list_data_products_ui_structure(self, client, mock_connection):
        """Test list_data_products method returns structure expected by UI"""
        
        # Mock response with all fields the UI expects
        mock_connection.execute_graphql.return_value = {
            "data": {
                "search": {
                    "total": 1,
                    "searchResults": [
                        {
                            "entity": {
                                "urn": "urn:li:dataProduct:test-product-1",
                                "type": "DATA_PRODUCT",
                                "properties": {
                                    "name": "Test Product 1",
                                    "description": "Test product description 1",
                                    "externalUrl": "https://example.com/product1"
                                },
                                "domain": {
                                    "urn": "urn:li:domain:test-domain",
                                    "properties": {
                                        "name": "Test Domain"
                                    }
                                },
                                "ownership": {
                                    "owners": [
                                        {
                                            "owner": {
                                                "urn": "urn:li:corpuser:testuser",
                                                "type": "CORP_USER",
                                                "info": {
                                                    "displayName": "Test User"
                                                }
                                            },
                                            "type": "BUSINESS_OWNER"
                                        }
                                    ]
                                },
                                "assets": {
                                    "total": 5,
                                    "relationships": []
                                }
                            }
                        }
                    ]
                }
            }
        }
        
        # Test the adapter method
        result = client.list_data_products(query="*", start=0, count=100)
        
        # Validate structure
        assert isinstance(result, list)
        assert len(result) == 1
        
        product = result[0]
        # Check required fields for UI
        required_fields = ['urn', 'name', 'description', 'external_url', 'domain', 'owners', 'owners_count', 'numAssets']
        for field in required_fields:
            assert field in product, f"Data product missing required field: {field}"
        
        # Check data types
        assert isinstance(product['urn'], str)
        assert isinstance(product['name'], str)
        assert isinstance(product['description'], str)
        assert isinstance(product['owners'], list)
        assert isinstance(product['owners_count'], int)
        assert isinstance(product['numAssets'], int)
        
        # Domain can be None or dict
        if product['domain'] is not None:
            assert isinstance(product['domain'], dict)
            assert 'name' in product['domain']

    def test_get_comprehensive_glossary_data_ui_structure(self, client, mock_connection):
        """Test get_comprehensive_glossary_data method returns structure expected by UI"""
        
        # Mock response with all fields the UI expects
        mock_connection.execute_graphql.return_value = {
            "data": {
                "glossaryNodes": {
                    "total": 1,
                    "nodes": [
                        {
                            "urn": "urn:li:glossaryNode:test-node-1",
                            "type": "GLOSSARY_NODE",
                            "properties": {
                                "name": "Test Node 1",
                                "description": "Test node description 1"
                            },
                            "ownership": {
                                "owners": []
                            },
                            "parentNodes": {
                                "nodes": []
                            }
                        }
                    ]
                },
                "glossaryTerms": {
                    "total": 1,
                    "terms": [
                        {
                            "urn": "urn:li:glossaryTerm:test-term-1",
                            "type": "GLOSSARY_TERM",
                            "properties": {
                                "name": "Test Term 1",
                                "description": "Test term description 1",
                                "termSource": "INTERNAL"
                            },
                            "ownership": {
                                "owners": []
                            },
                            "parentNodes": {
                                "nodes": []
                            }
                        }
                    ]
                }
            }
        }
        
        # Test the adapter method
        result = client.get_comprehensive_glossary_data(query="*", start=0, count=100)
        
        # Validate structure
        assert isinstance(result, dict)
        required_top_level_fields = ['glossary_nodes', 'glossary_terms', 'total_nodes', 'total_terms']
        for field in required_top_level_fields:
            assert field in result, f"Glossary data missing required field: {field}"
        
        # Check nodes structure
        nodes = result['glossary_nodes']
        assert isinstance(nodes, list)
        if len(nodes) > 0:
            node = nodes[0]
            node_required_fields = ['urn', 'name', 'description', 'owners', 'owners_count', 'parent_nodes']
            for field in node_required_fields:
                assert field in node, f"Glossary node missing required field: {field}"
        
        # Check terms structure
        terms = result['glossary_terms']
        assert isinstance(terms, list)
        if len(terms) > 0:
            term = terms[0]
            term_required_fields = ['urn', 'name', 'description', 'term_source', 'owners', 'owners_count', 'parent_nodes']
            for field in term_required_fields:
                assert field in term, f"Glossary term missing required field: {field}"

    def test_get_data_contracts_ui_structure(self, client, mock_connection):
        """Test get_data_contracts method returns structure expected by UI"""
        
        # Mock response with all fields the UI expects (no ownership for data contracts)
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
                                    "state": "ACTIVE",
                                    "dataQualityContract": {
                                        "assertion": "urn:li:assertion:test-assertion-1"
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }
        
        # Test the adapter method
        result = client.get_data_contracts(query="*", start=0, count=100)
        
        # Validate structure - this should return the specific format expected by UI
        assert isinstance(result, dict)
        assert "success" in result
        assert "data" in result
        assert result["success"] is True
        
        contracts_data = result["data"]
        assert "searchResults" in contracts_data
        assert "total" in contracts_data
        
        contracts = contracts_data["searchResults"]
        assert isinstance(contracts, list)
        if len(contracts) > 0:
            contract_wrapper = contracts[0]
            assert "entity" in contract_wrapper
            contract = contract_wrapper["entity"]
            
            # Check required fields for UI
            required_fields = ['urn', 'type', 'properties', 'info']
            for field in required_fields:
                assert field in contract, f"Data contract missing required field: {field}"

    def test_list_assertions_ui_structure(self, client, mock_connection):
        """Test list_assertions method returns structure expected by UI"""
        
        # Mock response with all fields the UI expects
        mock_connection.execute_graphql.return_value = {
            "data": {
                "search": {
                    "total": 1,
                    "searchResults": [
                        {
                            "entity": {
                                "urn": "urn:li:assertion:test-assertion-1",
                                "type": "ASSERTION",
                                "info": {
                                    "type": "DATASET",
                                    "datasetAssertion": {
                                        "dataset": "urn:li:dataset:(urn:li:dataPlatform:hive,test.table,PROD)",
                                        "scope": "DATASET_COLUMN",
                                        "fields": ["column1"],
                                        "operator": "GREATER_THAN",
                                        "parameters": {
                                            "value": {
                                                "value": "100",
                                                "type": "NUMBER"
                                            }
                                        }
                                    },
                                    "description": "Test assertion description 1"
                                },
                                "ownership": {
                                    "owners": []
                                },
                                "runEvents": {
                                    "total": 5,
                                    "runEvents": [
                                        {
                                            "timestampMillis": 1640995200000,
                                            "status": "COMPLETE",
                                            "result": {
                                                "type": "SUCCESS"
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        }
        
        # Test the adapter method
        result = client.get_assertions(query="*", start=0, count=100)
        
        # Validate structure
        assert isinstance(result, list)
        if len(result) > 0:
            assertion = result[0]
            # Check required fields for UI
            required_fields = ['urn', 'type', 'description', 'dataset_urn', 'scope', 'fields', 'operator', 'owners', 'owners_count', 'run_events', 'run_events_count', 'last_run_status']
            for field in required_fields:
                assert field in assertion, f"Assertion missing required field: {field}"
            
            # Check data types
            assert isinstance(assertion['urn'], str)
            assert isinstance(assertion['type'], str)
            assert isinstance(assertion['owners'], list)
            assert isinstance(assertion['owners_count'], int)
            assert isinstance(assertion['run_events'], list)
            assert isinstance(assertion['run_events_count'], int)

    def test_list_metadata_tests_ui_structure(self, client, mock_connection):
        """Test list_metadata_tests method returns structure expected by UI"""
        
        # Mock response with all fields the UI expects
        mock_connection.execute_graphql.return_value = {
            "data": {
                "search": {
                    "total": 1,
                    "searchResults": [
                        {
                            "entity": {
                                "urn": "urn:li:test:test-1",
                                "type": "TEST",
                                "definition": {
                                    "type": "JSON",
                                    "json": '{"test_type": "freshness", "threshold": "24h"}'
                                },
                                "info": {
                                    "name": "Test 1",
                                    "description": "Test description 1",
                                    "category": "DATA_QUALITY"
                                },
                                "ownership": {
                                    "owners": []
                                },
                                "testResults": {
                                    "total": 10,
                                    "testResults": [
                                        {
                                            "test": "urn:li:test:test-1",
                                            "timestampMillis": 1640995200000,
                                            "type": "SUCCESS"
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        }
        
        # Test the adapter method
        result = client.list_tests(query="*", start=0, count=100)
        
        # Validate structure
        assert isinstance(result, list)
        if len(result) > 0:
            test = result[0]
            # Check required fields for UI
            required_fields = ['urn', 'name', 'description', 'category', 'definition', 'owners', 'owners_count', 'test_results', 'test_results_count', 'last_result_status']
            for field in required_fields:
                assert field in test, f"Metadata test missing required field: {field}"
            
            # Check data types
            assert isinstance(test['urn'], str)
            assert isinstance(test['name'], str)
            assert isinstance(test['owners'], list)
            assert isinstance(test['owners_count'], int)
            assert isinstance(test['test_results'], list)
            assert isinstance(test['test_results_count'], int)

    def test_list_structured_properties_ui_structure(self, client, mock_connection):
        """Test list_structured_properties method returns structure expected by UI"""
        
        # Mock response with all fields the UI expects
        mock_connection.execute_graphql.return_value = {
            "data": {
                "listStructuredProperties": {
                    "total": 1,
                    "structuredProperties": [
                        {
                            "urn": "urn:li:structuredProperty:test-property-1",
                            "definition": {
                                "qualifiedName": "test.property.1",
                                "displayName": "Test Property 1",
                                "description": "Test property description 1",
                                "valueType": "STRING",
                                "cardinality": "SINGLE",
                                "entityTypes": ["DATASET", "CHART"],
                                "allowedValues": [
                                    {
                                        "value": "value1",
                                        "description": "Value 1 description"
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        }
        
        # Test the adapter method
        result = client.list_structured_properties(query="*", start=0, count=100)
        
        # Validate structure
        assert isinstance(result, list)
        if len(result) > 0:
            property = result[0]
            # Check required fields for UI
            required_fields = ['urn', 'qualified_name', 'display_name', 'description', 'value_type', 'cardinality', 'entity_types', 'allowed_values']
            for field in required_fields:
                assert field in property, f"Structured property missing required field: {field}"
            
            # Check data types
            assert isinstance(property['urn'], str)
            assert isinstance(property['qualified_name'], str)
            assert isinstance(property['display_name'], str)
            assert isinstance(property['value_type'], str)
            assert isinstance(property['cardinality'], str)
            assert isinstance(property['entity_types'], list)
            assert isinstance(property['allowed_values'], list)

    def test_error_handling_returns_safe_defaults(self, client, mock_connection):
        """Test that all methods return safe defaults when errors occur"""
        
        # Test with None response
        mock_connection.execute_graphql.return_value = None
        
        # Test count methods return 0
        assert client.tag_service.count_tags() == 0
        assert client.domain_service.count_domains() == 0
        assert client.data_product_service.count_data_products() == 0
        
        # Test list methods return empty lists
        assert client.list_tags() == []
        assert client.list_domains() == []
        assert client.list_data_products() == []
        
        # Test complex methods return safe defaults
        glossary_data = client.get_comprehensive_glossary_data()
        assert isinstance(glossary_data, dict)
        assert glossary_data['glossary_nodes'] == []
        assert glossary_data['glossary_terms'] == []
        assert glossary_data['total_nodes'] == 0
        assert glossary_data['total_terms'] == 0
        
        # Test data contracts returns proper structure
        contracts_data = client.get_data_contracts()
        assert isinstance(contracts_data, dict)
        assert contracts_data['success'] is True
        assert contracts_data['data']['searchResults'] == []
        assert contracts_data['data']['total'] == 0

    def test_safe_execute_graphql_never_returns_none(self, client, mock_connection):
        """Test that safe_execute_graphql never returns None"""
        
        # Test with None response
        mock_connection.execute_graphql.return_value = None
        result = client.safe_execute_graphql("query { test }")
        assert isinstance(result, dict)
        assert result == {}
        
        # Test with exception
        mock_connection.execute_graphql.side_effect = Exception("Test error")
        result = client.safe_execute_graphql("query { test }")
        assert isinstance(result, dict)
        assert result == {}
        
        # Test with valid response
        mock_connection.execute_graphql.side_effect = None
        mock_connection.execute_graphql.return_value = {"data": {"test": "value"}}
        result = client.safe_execute_graphql("query { test }")
        assert isinstance(result, dict)
        assert result == {"data": {"test": "value"}}

    def test_connection_test_method(self, client, mock_connection):
        """Test the connection test method used by UI"""
        
        # Test successful connection
        mock_connection.test_connection.return_value = True
        assert client.test_connection() is True
        
        # Test failed connection
        mock_connection.test_connection.return_value = False
        assert client.test_connection() is False
        
        # Test exception during connection test
        mock_connection.test_connection.side_effect = Exception("Connection error")
        # Should still return False, not raise exception
        result = client.test_connection()
        assert isinstance(result, bool)

    def test_users_and_groups_methods(self, client, mock_connection):
        """Test user and group methods used by UI for ownership"""
        
        # Mock response for users
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
        
        # Test list_users method
        users = client.list_users(start=0, count=100)
        assert isinstance(users, list)
        if len(users) > 0:
            user = users[0]
            required_fields = ['urn', 'displayName', 'email', 'active']
            for field in required_fields:
                assert field in user, f"User missing required field: {field}"
        
        # Mock response for groups
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
        
        # Test list_groups method
        groups = client.list_groups(start=0, count=100)
        assert isinstance(groups, list)
        if len(groups) > 0:
            group = groups[0]
            required_fields = ['urn', 'displayName', 'email']
            for field in required_fields:
                assert field in group, f"Group missing required field: {field}"

    def test_ownership_types_method(self, client, mock_connection):
        """Test ownership types method used by UI"""
        
        # Mock response for ownership types
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
                        },
                        {
                            "urn": "urn:li:ownershipType:__system__technical_owner",
                            "info": {
                                "name": "Technical Owner",
                                "description": "Technical owner of the entity"
                            }
                        }
                    ]
                }
            }
        }
        
        # Test list_ownership_types method
        ownership_types = client.list_ownership_types(start=0, count=100)
        assert isinstance(ownership_types, list)
        assert len(ownership_types) == 2
        
        ownership_type = ownership_types[0]
        required_fields = ['urn', 'name', 'description']
        for field in required_fields:
            assert field in ownership_type, f"Ownership type missing required field: {field}"


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"])