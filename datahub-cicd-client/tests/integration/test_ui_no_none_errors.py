"""
Test that UI adapter methods never return None or cause NoneType errors.

This test validates that all methods used by the web UI return proper
data structures even when DataHub is unavailable or returns None.
"""

import pytest
from unittest.mock import Mock, patch
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

from utils.datahub_client_adapter import DataHubRestClient


class TestUINoNoneErrors:
    """Test that UI methods never return None or cause NoneType errors."""

    @pytest.fixture
    def mock_client(self):
        """Create a DataHub client with mocked connection that returns None."""
        with patch('datahub_cicd_client.core.connection.DataHubConnection') as mock_conn_class:
            mock_conn = Mock()
            mock_conn.test_connection.return_value = True
            # Mock all GraphQL calls to return None (simulating connection failures)
            mock_conn.execute_graphql.return_value = None
            mock_conn_class.return_value = mock_conn
            
            client = DataHubRestClient('http://localhost:8080', 'test-token')
            return client

    def test_count_methods_never_none(self, mock_client):
        """Test that all count methods return integers, never None."""
        count_methods = [
            'count_tags',
            'count_domains', 
            'count_structured_properties',
            'count_data_products',
            'count_assertions',
            'count_data_contracts',
            'count_tests',
            'count_glossary_nodes',
            'count_glossary_terms'
        ]
        
        for method_name in count_methods:
            method = getattr(mock_client, method_name)
            result = method()
            assert isinstance(result, int), f"{method_name} should return int, got {type(result)}"
            assert result >= 0, f"{method_name} should return non-negative integer"

    def test_list_methods_never_none(self, mock_client):
        """Test that all list methods return lists, never None."""
        list_methods = [
            ('list_tags', []),
            ('list_domains', []),
            ('list_structured_properties', []),
            ('list_data_products', []),
            ('list_glossary_nodes', []),
            ('list_glossary_terms', []),
            ('list_tests', []),
            ('list_users', []),
            ('list_groups', []),
            ('list_ownership_types', [])
        ]
        
        for method_name, expected_default in list_methods:
            method = getattr(mock_client, method_name)
            result = method()
            assert isinstance(result, list), f"{method_name} should return list, got {type(result)}"
            assert result == expected_default, f"{method_name} should return {expected_default} when no data"

    def test_complex_data_methods_never_none(self, mock_client):
        """Test that complex data methods return proper dictionaries, never None."""
        # Test get_comprehensive_glossary_data
        result = mock_client.get_comprehensive_glossary_data()
        assert isinstance(result, dict), "get_comprehensive_glossary_data should return dict"
        assert 'glossary_nodes' in result, "Should have glossary_nodes key"
        assert 'glossary_terms' in result, "Should have glossary_terms key"
        assert isinstance(result['glossary_nodes'], list), "glossary_nodes should be list"
        assert isinstance(result['glossary_terms'], list), "glossary_terms should be list"
        
        # Test get_remote_tags_data
        result = mock_client.get_remote_tags_data()
        assert isinstance(result, dict), "get_remote_tags_data should return dict"
        assert 'searchResults' in result, "Should have searchResults key"
        assert isinstance(result['searchResults'], list), "searchResults should be list"
        
        # Test get_data_contracts
        result = mock_client.get_data_contracts()
        assert isinstance(result, dict), "get_data_contracts should return dict"
        assert 'success' in result, "Should have success key"
        assert 'data' in result, "Should have data key"
        
        # Test get_assertions
        result = mock_client.get_assertions()
        assert isinstance(result, dict), "get_assertions should return dict"
        assert 'success' in result, "Should have success key"
        assert 'data' in result, "Should have data key"

    def test_no_attribute_get_calls_on_none(self, mock_client):
        """Test that methods don't call .get() on None values."""
        # This is the specific error we're trying to prevent:
        # 'NoneType' object has no attribute 'get'
        
        # Mock the service methods to return None explicitly
        with patch.object(mock_client.glossary_service, 'get_comprehensive_glossary_data', return_value=None):
            result = mock_client.get_comprehensive_glossary_data()
            # Should not raise AttributeError
            assert isinstance(result, dict)
            assert result['glossary_nodes'] == []
            assert result['glossary_terms'] == []

    def test_safe_execute_graphql_none_handling(self, mock_client):
        """Test that safe_execute_graphql handles None returns properly."""
        # The safe_execute_graphql method should never return None
        result = mock_client.safe_execute_graphql("query { test }")
        assert result is not None, "safe_execute_graphql should never return None"
        assert isinstance(result, dict), "safe_execute_graphql should return dict"

    def test_all_ui_dashboard_methods(self, mock_client):
        """Test all methods typically called by the UI dashboard."""
        # These are the methods commonly called by the metadata manager dashboard
        dashboard_methods = [
            ('count_tags', int),
            ('count_domains', int),
            ('count_data_products', int),
            ('count_glossary_nodes', int),
            ('count_glossary_terms', int),
            ('count_assertions', int),
            ('count_data_contracts', int),
            ('count_tests', int),
            ('count_structured_properties', int),
            ('list_tags', list),
            ('list_domains', list),
            ('list_data_products', list),
            ('get_comprehensive_glossary_data', dict),
            ('get_remote_tags_data', dict),
            ('get_data_contracts', dict),
            ('get_assertions', dict)
        ]
        
        for method_name, expected_type in dashboard_methods:
            method = getattr(mock_client, method_name)
            result = method()
            assert isinstance(result, expected_type), f"{method_name} should return {expected_type.__name__}, got {type(result)}"
            assert result is not None, f"{method_name} should never return None"

    def test_error_handling_with_exceptions(self, mock_client):
        """Test that methods handle exceptions gracefully."""
        # Mock a service method to raise an exception
        with patch.object(mock_client.tag_service, 'list_tags', side_effect=Exception("Connection error")):
            result = mock_client.list_tags()
            # Should not raise exception, should return empty list
            assert isinstance(result, list)
            assert result == []

        with patch.object(mock_client.tag_service, 'count_tags', side_effect=Exception("Connection error")):
            result = mock_client.count_tags()
            # Should not raise exception, should return 0
            assert isinstance(result, int)
            assert result == 0


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"]) 