#!/usr/bin/env python3
"""
UI Integration Tests

Tests that mirror the actual web UI queries to ensure all pages work correctly
and return the expected data structures with all required columns.
"""

import pytest
import logging
import os
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any, List

# Import the adapter and services
from datahub_cicd_client.core.connection import DataHubConnection
from datahub_cicd_client.services.tags import TagService
from datahub_cicd_client.services.domains import DomainService
from datahub_cicd_client.services.glossary import GlossaryService
from datahub_cicd_client.services.data_products import DataProductService
from datahub_cicd_client.services.assertions import AssertionService
from datahub_cicd_client.services.tests import MetadataTestService
from datahub_cicd_client.services.data_contracts import DataContractService
from datahub_cicd_client.services.properties import StructuredPropertiesService

logger = logging.getLogger(__name__)


class TestUIIntegration:
    """Test suite for UI integration - mirrors actual web UI queries"""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock DataHub connection"""
        connection = Mock(spec=DataHubConnection)
        connection.test_connection.return_value = True
        connection.execute_graphql.return_value = {"data": {}}
        return connection

    @pytest.fixture
    def tag_service(self, mock_connection):
        return TagService(mock_connection)

    @pytest.fixture
    def domain_service(self, mock_connection):
        return DomainService(mock_connection)

    @pytest.fixture
    def glossary_service(self, mock_connection):
        return GlossaryService(mock_connection)

    @pytest.fixture
    def data_product_service(self, mock_connection):
        return DataProductService(mock_connection)

    @pytest.fixture
    def assertion_service(self, mock_connection):
        return AssertionService(mock_connection)

    @pytest.fixture
    def metadata_test_service(self, mock_connection):
        return MetadataTestService(mock_connection)

    @pytest.fixture
    def data_contract_service(self, mock_connection):
        return DataContractService(mock_connection)

    @pytest.fixture
    def properties_service(self, mock_connection):
        return StructuredPropertiesService(mock_connection)

    def test_metadata_manager_index_stats(self, tag_service, domain_service, glossary_service, 
                                        data_product_service, assertion_service, metadata_test_service,
                                        data_contract_service, properties_service):
        """Test the main metadata manager dashboard stats - mirrors MetadataIndexView"""
        
        # Mock the GraphQL responses for count queries
        tag_service.connection.execute_graphql.return_value = {
            "data": {
                "search": {
                    "total": 25,
                    "searchResults": []
                }
            }
        }
        
        # Test each count method that the UI uses
        tags_count = tag_service.count_tags()
        assert isinstance(tags_count, int)
        assert tags_count >= 0
        
        domains_count = domain_service.count_domains()
        assert isinstance(domains_count, int)
        assert domains_count >= 0
        
        glossary_terms_count = glossary_service.count_glossary_terms()
        assert isinstance(glossary_terms_count, int)
        assert glossary_terms_count >= 0
        
        data_products_count = data_product_service.count_data_products()
        assert isinstance(data_products_count, int)
        assert data_products_count >= 0
        
        assertions_count = assertion_service.count_assertions()
        assert isinstance(assertions_count, int)
        assert assertions_count >= 0
        
        tests_count = metadata_test_service.count_metadata_tests()
        assert isinstance(tests_count, int)
        assert tests_count >= 0
        
        data_contracts_count = data_contract_service.count_data_contracts()
        assert isinstance(data_contracts_count, int)
        assert data_contracts_count >= 0
        
        properties_count = properties_service.count_structured_properties()
        assert isinstance(properties_count, int)
        assert properties_count >= 0

    def test_tags_list_page_data(self, tag_service):
        """Test tags list page data structure - mirrors TagListView"""
        
        # Mock response with all expected fields
        tag_service.connection.execute_graphql.return_value = {
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
                        },
                        {
                            "entity": {
                                "urn": "urn:li:tag:test-tag-2",
                                "type": "TAG",
                                "properties": {
                                    "name": "Test Tag 2",
                                    "description": "Test description 2",
                                    "colorHex": "#33FF57"
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
        
        # Test list_tags method
        result = tag_service.list_tags(query="*", start=0, count=100)
        
        # Verify the result structure
        assert isinstance(result, list)
        assert len(result) == 2
        
        # Check first tag structure
        tag1 = result[0]
        assert "urn" in tag1
        assert "name" in tag1
        assert "description" in tag1
        assert "colorHex" in tag1
        assert "owners" in tag1
        assert "owners_count" in tag1
        
        # Verify data types and values
        assert tag1["urn"] == "urn:li:tag:test-tag-1"
        assert tag1["name"] == "Test Tag 1"
        assert tag1["description"] == "Test description 1"
        assert tag1["colorHex"] == "#FF5733"
        assert isinstance(tag1["owners"], list)
        assert tag1["owners_count"] == 1
        
        # Check second tag structure (no owners)
        tag2 = result[1]
        assert tag2["owners_count"] == 0
        assert isinstance(tag2["owners"], list)

    def test_domains_list_page_data(self, domain_service):
        """Test domains list page data structure - mirrors DomainListView"""
        
        # Mock response with all expected fields
        domain_service.connection.execute_graphql.return_value = {
            "data": {
                "search": {
                    "total": 2,
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
                        },
                        {
                            "entity": {
                                "urn": "urn:li:domain:test-domain-2",
                                "type": "DOMAIN",
                                "properties": {
                                    "name": "Test Domain 2",
                                    "description": "Test domain description 2"
                                },
                                "ownership": {
                                    "owners": []
                                },
                                "parentDomains": {
                                    "domains": [
                                        {
                                            "urn": "urn:li:domain:parent-domain",
                                            "properties": {
                                                "name": "Parent Domain"
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
        
        # Test list_domains method
        result = domain_service.list_domains(query="*", start=0, count=100)
        
        # Verify the result structure
        assert isinstance(result, list)
        assert len(result) == 2
        
        # Check first domain structure
        domain1 = result[0]
        assert "urn" in domain1
        assert "name" in domain1
        assert "description" in domain1
        assert "owners" in domain1
        assert "owners_count" in domain1
        assert "parent_domains" in domain1
        
        # Verify data types and values
        assert domain1["urn"] == "urn:li:domain:test-domain-1"
        assert domain1["name"] == "Test Domain 1"
        assert domain1["description"] == "Test domain description 1"
        assert isinstance(domain1["owners"], list)
        assert domain1["owners_count"] == 1
        assert isinstance(domain1["parent_domains"], list)
        assert len(domain1["parent_domains"]) == 0
        
        # Check second domain structure (with parent)
        domain2 = result[1]
        assert domain2["owners_count"] == 0
        assert len(domain2["parent_domains"]) == 1
        assert domain2["parent_domains"][0]["name"] == "Parent Domain"

    def test_data_products_list_page_data(self, data_product_service):
        """Test data products list page data structure - mirrors DataProductListView"""
        
        # Mock response with all expected fields
        data_product_service.connection.execute_graphql.return_value = {
            "data": {
                "search": {
                    "total": 2,
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
                        },
                        {
                            "entity": {
                                "urn": "urn:li:dataProduct:test-product-2",
                                "type": "DATA_PRODUCT",
                                "properties": {
                                    "name": "Test Product 2",
                                    "description": "Test product description 2"
                                },
                                "ownership": {
                                    "owners": []
                                },
                                "assets": {
                                    "total": 3,
                                    "relationships": []
                                }
                            }
                        }
                    ]
                }
            }
        }
        
        # Test list_data_products method
        result = data_product_service.list_data_products(query="*", start=0, count=100)
        
        # Verify the result structure
        assert isinstance(result, list)
        assert len(result) == 2
        
        # Check first data product structure
        product1 = result[0]
        assert "urn" in product1
        assert "name" in product1
        assert "description" in product1
        assert "external_url" in product1
        assert "domain" in product1
        assert "owners" in product1
        assert "owners_count" in product1
        assert "numAssets" in product1
        
        # Verify data types and values
        assert product1["urn"] == "urn:li:dataProduct:test-product-1"
        assert product1["name"] == "Test Product 1"
        assert product1["description"] == "Test product description 1"
        assert product1["external_url"] == "https://example.com/product1"
        assert isinstance(product1["owners"], list)
        assert product1["owners_count"] == 1
        assert product1["numAssets"] == 5
        
        # Check domain structure
        assert isinstance(product1["domain"], dict)
        assert product1["domain"]["name"] == "Test Domain"
        
        # Check second product structure (no domain, no external URL)
        product2 = result[1]
        assert product2["owners_count"] == 0
        assert product2["numAssets"] == 3
        assert product2["external_url"] is None
        assert product2["domain"] is None

    def test_glossary_list_page_data(self, glossary_service):
        """Test glossary list page data structure - mirrors GlossaryListView"""
        
        # Mock response with all expected fields for comprehensive glossary data
        glossary_service.connection.execute_graphql.return_value = {
            "data": {
                "glossaryNodes": {
                    "total": 2,
                    "nodes": [
                        {
                            "urn": "urn:li:glossaryNode:test-node-1",
                            "type": "GLOSSARY_NODE",
                            "properties": {
                                "name": "Test Node 1",
                                "description": "Test node description 1"
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
                            "parentNodes": {
                                "nodes": []
                            }
                        }
                    ]
                },
                "glossaryTerms": {
                    "total": 3,
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
                                "nodes": [
                                    {
                                        "urn": "urn:li:glossaryNode:test-node-1",
                                        "properties": {
                                            "name": "Test Node 1"
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        }
        
        # Test get_comprehensive_glossary_data method
        result = glossary_service.get_comprehensive_glossary_data(query="*", start=0, count=100)
        
        # Verify the result structure
        assert isinstance(result, dict)
        assert "glossary_nodes" in result
        assert "glossary_terms" in result
        assert "total_nodes" in result
        assert "total_terms" in result
        
        # Check glossary nodes structure
        nodes = result["glossary_nodes"]
        assert isinstance(nodes, list)
        assert len(nodes) == 2
        
        node1 = nodes[0]
        assert "urn" in node1
        assert "name" in node1
        assert "description" in node1
        assert "owners" in node1
        assert "owners_count" in node1
        assert "parent_nodes" in node1
        
        # Check glossary terms structure
        terms = result["glossary_terms"]
        assert isinstance(terms, list)
        assert len(terms) == 3
        
        term1 = terms[0]
        assert "urn" in term1
        assert "name" in term1
        assert "description" in term1
        assert "term_source" in term1
        assert "owners" in term1
        assert "owners_count" in term1
        assert "parent_nodes" in term1

    def test_assertions_list_page_data(self, assertion_service):
        """Test assertions list page data structure - mirrors AssertionListView"""
        
        # Mock response with all expected fields
        assertion_service.connection.execute_graphql.return_value = {
            "data": {
                "search": {
                    "total": 2,
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
        
        # Test list_assertions method
        result = assertion_service.list_assertions(query="*", start=0, count=100)
        
        # Verify the result structure
        assert isinstance(result, list)
        assert len(result) == 2
        
        # Check first assertion structure
        assertion1 = result[0]
        assert "urn" in assertion1
        assert "type" in assertion1
        assert "description" in assertion1
        assert "dataset_urn" in assertion1
        assert "scope" in assertion1
        assert "fields" in assertion1
        assert "operator" in assertion1
        assert "owners" in assertion1
        assert "owners_count" in assertion1
        assert "run_events" in assertion1
        assert "run_events_count" in assertion1
        assert "last_run_status" in assertion1
        
        # Verify data types and values
        assert assertion1["urn"] == "urn:li:assertion:test-assertion-1"
        assert assertion1["type"] == "DATASET"
        assert assertion1["description"] == "Test assertion description 1"
        assert isinstance(assertion1["owners"], list)
        assert assertion1["owners_count"] == 1
        assert assertion1["run_events_count"] == 5
        assert assertion1["last_run_status"] == "SUCCESS"

    def test_metadata_tests_list_page_data(self, metadata_test_service):
        """Test metadata tests list page data structure - mirrors MetadataTestListView"""
        
        # Mock response with all expected fields
        metadata_test_service.connection.execute_graphql.return_value = {
            "data": {
                "search": {
                    "total": 2,
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
        
        # Test list_metadata_tests method
        result = metadata_test_service.list_metadata_tests(query="*", start=0, count=100)
        
        # Verify the result structure
        assert isinstance(result, list)
        assert len(result) == 2
        
        # Check first test structure
        test1 = result[0]
        assert "urn" in test1
        assert "name" in test1
        assert "description" in test1
        assert "category" in test1
        assert "definition" in test1
        assert "owners" in test1
        assert "owners_count" in test1
        assert "test_results" in test1
        assert "test_results_count" in test1
        assert "last_result_status" in test1
        
        # Verify data types and values
        assert test1["urn"] == "urn:li:test:test-1"
        assert test1["name"] == "Test 1"
        assert test1["description"] == "Test description 1"
        assert test1["category"] == "DATA_QUALITY"
        assert isinstance(test1["owners"], list)
        assert test1["owners_count"] == 1
        assert test1["test_results_count"] == 10
        assert test1["last_result_status"] == "SUCCESS"

    def test_data_contracts_list_page_data(self, data_contract_service):
        """Test data contracts list page data structure - mirrors DataContractListView"""
        
        # Mock response with all expected fields (no ownership field for data contracts)
        data_contract_service.connection.execute_graphql.return_value = {
            "data": {
                "search": {
                    "total": 2,
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
                        },
                        {
                            "entity": {
                                "urn": "urn:li:dataContract:test-contract-2",
                                "type": "DATA_CONTRACT",
                                "properties": {
                                    "entity": "urn:li:dataset:(urn:li:dataPlatform:hive,test.table2,PROD)"
                                },
                                "info": {
                                    "type": "SCHEMA",
                                    "state": "ACTIVE",
                                    "schemaContract": {
                                        "schema": {
                                            "fields": [
                                                {
                                                    "fieldPath": "column1",
                                                    "type": "STRING",
                                                    "nullable": False
                                                }
                                            ]
                                        }
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }
        
        # Test get_data_contracts method
        result = data_contract_service.get_data_contracts(query="*", start=0, count=100)
        
        # Verify the result structure
        assert isinstance(result, dict)
        assert "success" in result
        assert "data" in result
        assert result["success"] is True
        
        contracts_data = result["data"]
        assert "searchResults" in contracts_data
        assert "total" in contracts_data
        
        contracts = contracts_data["searchResults"]
        assert isinstance(contracts, list)
        assert len(contracts) == 2
        
        # Check first contract structure
        contract1 = contracts[0]["entity"]
        assert "urn" in contract1
        assert "type" in contract1
        assert "properties" in contract1
        assert "info" in contract1
        
        # Verify data types and values
        assert contract1["urn"] == "urn:li:dataContract:test-contract-1"
        assert contract1["type"] == "DATA_CONTRACT"
        assert contract1["info"]["type"] == "DATA_QUALITY"
        assert contract1["info"]["state"] == "ACTIVE"

    def test_structured_properties_list_page_data(self, properties_service):
        """Test structured properties list page data structure - mirrors PropertyListView"""
        
        # Mock response with all expected fields
        properties_service.connection.execute_graphql.return_value = {
            "data": {
                "listStructuredProperties": {
                    "total": 2,
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
                                    },
                                    {
                                        "value": "value2",
                                        "description": "Value 2 description"
                                    }
                                ]
                            }
                        },
                        {
                            "urn": "urn:li:structuredProperty:test-property-2",
                            "definition": {
                                "qualifiedName": "test.property.2",
                                "displayName": "Test Property 2",
                                "description": "Test property description 2",
                                "valueType": "NUMBER",
                                "cardinality": "MULTIPLE",
                                "entityTypes": ["DATASET"],
                                "allowedValues": []
                            }
                        }
                    ]
                }
            }
        }
        
        # Test list_structured_properties method
        result = properties_service.list_structured_properties(query="*", start=0, count=100)
        
        # Verify the result structure
        assert isinstance(result, list)
        assert len(result) == 2
        
        # Check first property structure
        property1 = result[0]
        assert "urn" in property1
        assert "qualified_name" in property1
        assert "display_name" in property1
        assert "description" in property1
        assert "value_type" in property1
        assert "cardinality" in property1
        assert "entity_types" in property1
        assert "allowed_values" in property1
        
        # Verify data types and values
        assert property1["urn"] == "urn:li:structuredProperty:test-property-1"
        assert property1["qualified_name"] == "test.property.1"
        assert property1["display_name"] == "Test Property 1"
        assert property1["description"] == "Test property description 1"
        assert property1["value_type"] == "STRING"
        assert property1["cardinality"] == "SINGLE"
        assert isinstance(property1["entity_types"], list)
        assert "DATASET" in property1["entity_types"]
        assert "CHART" in property1["entity_types"]
        assert isinstance(property1["allowed_values"], list)
        assert len(property1["allowed_values"]) == 2
        
        # Check second property structure (no allowed values)
        property2 = result[1]
        assert property2["value_type"] == "NUMBER"
        assert property2["cardinality"] == "MULTIPLE"
        assert len(property2["allowed_values"]) == 0

    def test_error_handling_and_empty_responses(self, tag_service, domain_service):
        """Test that services handle errors and empty responses gracefully"""
        
        # Test with empty response
        tag_service.connection.execute_graphql.return_value = {
            "data": {
                "search": {
                    "total": 0,
                    "searchResults": []
                }
            }
        }
        
        result = tag_service.list_tags()
        assert isinstance(result, list)
        assert len(result) == 0
        
        # Test with None response
        tag_service.connection.execute_graphql.return_value = None
        
        result = tag_service.list_tags()
        assert isinstance(result, list)
        assert len(result) == 0
        
        # Test with GraphQL error
        tag_service.connection.execute_graphql.return_value = {
            "errors": [
                {
                    "message": "Test error",
                    "extensions": {
                        "code": "INTERNAL_ERROR"
                    }
                }
            ]
        }
        
        result = tag_service.list_tags()
        assert isinstance(result, list)
        assert len(result) == 0

    def test_count_methods_return_integers(self, tag_service, domain_service, glossary_service,
                                         data_product_service, assertion_service, metadata_test_service,
                                         data_contract_service, properties_service):
        """Test that all count methods return integers, not None or other types"""
        
        # Mock various response scenarios
        responses = [
            {"data": {"search": {"total": 42}}},  # Normal response
            {"data": {"search": {}}},  # Missing total
            {"data": {}},  # Missing search
            {},  # Empty response
            None,  # None response
        ]
        
        services_and_methods = [
            (tag_service, "count_tags"),
            (domain_service, "count_domains"),
            (glossary_service, "count_glossary_terms"),
            (data_product_service, "count_data_products"),
            (assertion_service, "count_assertions"),
            (metadata_test_service, "count_metadata_tests"),
            (data_contract_service, "count_data_contracts"),
            (properties_service, "count_structured_properties"),
        ]
        
        for response in responses:
            for service, method_name in services_and_methods:
                service.connection.execute_graphql.return_value = response
                
                # Get the method and call it
                method = getattr(service, method_name)
                result = method()
                
                # Verify it's always an integer
                assert isinstance(result, int), f"{method_name} returned {type(result)} instead of int for response {response}"
                assert result >= 0, f"{method_name} returned negative value {result}"

    def test_list_methods_return_lists(self, tag_service, domain_service, glossary_service,
                                     data_product_service, assertion_service, metadata_test_service,
                                     properties_service):
        """Test that all list methods return lists, not None or other types"""
        
        # Mock various response scenarios
        responses = [
            {"data": {"search": {"searchResults": []}}},  # Empty results
            {"data": {"search": {}}},  # Missing searchResults
            {"data": {}},  # Missing search
            {},  # Empty response
            None,  # None response
        ]
        
        services_and_methods = [
            (tag_service, "list_tags"),
            (domain_service, "list_domains"),
            (data_product_service, "list_data_products"),
            (assertion_service, "list_assertions"),
            (metadata_test_service, "list_metadata_tests"),
            (properties_service, "list_structured_properties"),
        ]
        
        for response in responses:
            for service, method_name in services_and_methods:
                service.connection.execute_graphql.return_value = response
                
                # Get the method and call it
                method = getattr(service, method_name)
                result = method()
                
                # Verify it's always a list
                assert isinstance(result, list), f"{method_name} returned {type(result)} instead of list for response {response}"

    def test_adapter_integration_with_ui_methods(self):
        """Test the DataHub adapter integration with methods used by the UI"""
        
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
            
            # Test methods that the UI calls
            assert client.test_connection() is True
            
            # Test count methods
            tags_count = client.tag_service.count_tags()
            assert isinstance(tags_count, int)
            
            domains_count = client.domain_service.count_domains()
            assert isinstance(domains_count, int)
            
            data_products_count = client.data_product_service.count_data_products()
            assert isinstance(data_products_count, int)
            
            # Test list methods
            tags = client.list_tags()
            assert isinstance(tags, list)
            
            domains = client.list_domains()
            assert isinstance(domains, list)
            
            data_products = client.list_data_products()
            assert isinstance(data_products, list)
            
            # Test safe_execute_graphql method
            result = client.safe_execute_graphql("query { test }")
            assert isinstance(result, dict)


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"])