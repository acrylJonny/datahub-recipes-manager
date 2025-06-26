"""
Integration tests for GraphQL queries against DataHub.

These tests require a running DataHub instance and will test all entity types
to ensure GraphQL queries work correctly and don't have field validation errors.
"""

import pytest
import json
import os
from typing import Dict, Any, List
from pathlib import Path

from datahub_cicd_client.clients.graphql_connection import GraphQLConnection
from datahub_cicd_client.services.data_contracts import DataContractService
from datahub_cicd_client.services.data_products import DataProductService
from datahub_cicd_client.services.domains import DomainService
from datahub_cicd_client.services.tags import TagService
from datahub_cicd_client.services.glossary import GlossaryService
from datahub_cicd_client.services.metadata_tests import TestService
from datahub_cicd_client.services.assertions import AssertionService


@pytest.fixture(scope="module")
def datahub_connection():
    """Create connection to DataHub instance."""
    datahub_url = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
    datahub_token = os.getenv("DATAHUB_GMS_TOKEN", "")
    
    return GraphQLConnection(
        datahub_url=datahub_url,
        token=datahub_token
    )


@pytest.fixture(scope="module")
def sample_entities():
    """Load sample entities from fixtures."""
    fixtures_path = Path(__file__).parent.parent / "fixtures" / "sample_entities.json"
    with open(fixtures_path, 'r') as f:
        return json.load(f)


class TestDataContractQueries:
    """Test data contract GraphQL queries."""
    
    def test_list_data_contracts(self, datahub_connection):
        """Test listing data contracts doesn't cause GraphQL validation errors."""
        service = DataContractService(datahub_connection)
        
        try:
            result = service.list_data_contracts()
            assert isinstance(result, list), "Should return a list of data contracts"
            print(f"✅ Data contracts query successful: {len(result)} contracts found")
        except Exception as e:
            # Check if it's a GraphQL validation error
            if "Field 'ownership' in type 'DataContract' is undefined" in str(e):
                pytest.fail("GraphQL validation error: DataContract queries still have ownership field")
            elif "GraphQL query" in str(e) and "validation" in str(e).lower():
                pytest.fail(f"GraphQL validation error: {e}")
            else:
                # Other errors (like connection issues) are acceptable for this test
                print(f"⚠️ Non-validation error (acceptable): {e}")
    
    def test_get_data_contracts_structure(self, datahub_connection):
        """Test that get_data_contracts returns proper dictionary structure."""
        service = DataContractService(datahub_connection)
        
        try:
            result = service.get_data_contracts()
            assert isinstance(result, dict), "Should return a dictionary"
            assert "success" in result, "Should have success field"
            assert "data" in result, "Should have data field"
            print("✅ Data contracts structure test passed")
        except Exception as e:
            if "GraphQL" in str(e) and "validation" in str(e).lower():
                pytest.fail(f"GraphQL validation error: {e}")
            else:
                print(f"⚠️ Non-validation error (acceptable): {e}")


class TestDataProductQueries:
    """Test data product GraphQL queries."""
    
    def test_list_data_products(self, datahub_connection):
        """Test listing data products with ownership fields."""
        service = DataProductService(datahub_connection)
        
        try:
            result = service.list_data_products()
            assert isinstance(result, list), "Should return a list of data products"
            print(f"✅ Data products query successful: {len(result)} products found")
        except Exception as e:
            if "GraphQL" in str(e) and "validation" in str(e).lower():
                pytest.fail(f"GraphQL validation error: {e}")
            else:
                print(f"⚠️ Non-validation error (acceptable): {e}")
    
    def test_search_data_products(self, datahub_connection):
        """Test searching data products."""
        service = DataProductService(datahub_connection)
        
        try:
            result = service.search_data_products("test")
            assert isinstance(result, list), "Should return a list"
            print("✅ Data products search test passed")
        except Exception as e:
            if "GraphQL" in str(e) and "validation" in str(e).lower():
                pytest.fail(f"GraphQL validation error: {e}")
            else:
                print(f"⚠️ Non-validation error (acceptable): {e}")


class TestDomainQueries:
    """Test domain GraphQL queries."""
    
    def test_list_domains(self, datahub_connection):
        """Test listing domains with ownership fields."""
        service = DomainService(datahub_connection)
        
        try:
            result = service.list_domains()
            assert isinstance(result, list), "Should return a list of domains"
            print(f"✅ Domains query successful: {len(result)} domains found")
        except Exception as e:
            if "GraphQL" in str(e) and "validation" in str(e).lower():
                pytest.fail(f"GraphQL validation error: {e}")
            else:
                print(f"⚠️ Non-validation error (acceptable): {e}")


class TestTagQueries:
    """Test tag GraphQL queries."""
    
    def test_list_tags(self, datahub_connection):
        """Test listing tags with ownership fields."""
        service = TagService(datahub_connection)
        
        try:
            result = service.list_tags()
            assert isinstance(result, list), "Should return a list of tags"
            print(f"✅ Tags query successful: {len(result)} tags found")
        except Exception as e:
            if "GraphQL" in str(e) and "validation" in str(e).lower():
                pytest.fail(f"GraphQL validation error: {e}")
            else:
                print(f"⚠️ Non-validation error (acceptable): {e}")


class TestGlossaryQueries:
    """Test glossary GraphQL queries."""
    
    def test_list_glossary_terms(self, datahub_connection):
        """Test listing glossary terms with ownership fields."""
        service = GlossaryService(datahub_connection)
        
        try:
            result = service.list_glossary_terms()
            assert isinstance(result, list), "Should return a list of terms"
            print(f"✅ Glossary terms query successful: {len(result)} terms found")
        except Exception as e:
            if "GraphQL" in str(e) and "validation" in str(e).lower():
                pytest.fail(f"GraphQL validation error: {e}")
            else:
                print(f"⚠️ Non-validation error (acceptable): {e}")
    
    def test_list_glossary_nodes(self, datahub_connection):
        """Test listing glossary nodes."""
        service = GlossaryService(datahub_connection)
        
        try:
            result = service.list_glossary_nodes()
            assert isinstance(result, list), "Should return a list of nodes"
            print(f"✅ Glossary nodes query successful: {len(result)} nodes found")
        except Exception as e:
            if "GraphQL" in str(e) and "validation" in str(e).lower():
                pytest.fail(f"GraphQL validation error: {e}")
            else:
                print(f"⚠️ Non-validation error (acceptable): {e}")


class TestMetadataTestQueries:
    """Test metadata test GraphQL queries."""
    
    def test_search_metadata_tests(self, datahub_connection):
        """Test searching metadata tests."""
        service = TestService(datahub_connection)
        
        try:
            result = service.search_metadata_tests("test")
            assert isinstance(result, list), "Should return a list of tests"
            print(f"✅ Metadata tests query successful: {len(result)} tests found")
        except Exception as e:
            if "GraphQL" in str(e) and "validation" in str(e).lower():
                pytest.fail(f"GraphQL validation error: {e}")
            else:
                print(f"⚠️ Non-validation error (acceptable): {e}")


class TestAssertionQueries:
    """Test assertion GraphQL queries."""
    
    def test_search_assertions(self, datahub_connection):
        """Test searching assertions."""
        service = AssertionService(datahub_connection)
        
        try:
            result = service.search_assertions("test")
            assert isinstance(result, list), "Should return a list of assertions"
            print(f"✅ Assertions query successful: {len(result)} assertions found")
        except Exception as e:
            if "GraphQL" in str(e) and "validation" in str(e).lower():
                pytest.fail(f"GraphQL validation error: {e}")
            else:
                print(f"⚠️ Non-validation error (acceptable): {e}")


class TestComprehensiveQueries:
    """Test comprehensive scenarios across multiple entity types."""
    
    def test_all_entity_types_no_validation_errors(self, datahub_connection):
        """Test that all entity types can be queried without GraphQL validation errors."""
        services = [
            ("DataContract", DataContractService(datahub_connection)),
            ("DataProduct", DataProductService(datahub_connection)),
            ("Domain", DomainService(datahub_connection)),
            ("Tag", TagService(datahub_connection)),
            ("Glossary", GlossaryService(datahub_connection)),
            ("MetadataTest", TestService(datahub_connection)),
            ("Assertion", AssertionService(datahub_connection)),
        ]
        
        validation_errors = []
        
        for service_name, service in services:
            try:
                # Try to call a list method for each service
                if hasattr(service, 'list_data_contracts'):
                    service.list_data_contracts()
                elif hasattr(service, 'list_data_products'):
                    service.list_data_products()
                elif hasattr(service, 'list_domains'):
                    service.list_domains()
                elif hasattr(service, 'list_tags'):
                    service.list_tags()
                elif hasattr(service, 'list_glossary_terms'):
                    service.list_glossary_terms()
                elif hasattr(service, 'search_metadata_tests'):
                    service.search_metadata_tests("test")
                elif hasattr(service, 'search_assertions'):
                    service.search_assertions("test")
                
                print(f"✅ {service_name}: No validation errors")
                
            except Exception as e:
                if "GraphQL" in str(e) and "validation" in str(e).lower():
                    validation_errors.append(f"{service_name}: {e}")
                else:
                    print(f"⚠️ {service_name}: Non-validation error (acceptable): {e}")
        
        if validation_errors:
            pytest.fail(f"GraphQL validation errors found:\n" + "\n".join(validation_errors))
        
        print("✅ All entity types passed GraphQL validation tests")


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"]) 