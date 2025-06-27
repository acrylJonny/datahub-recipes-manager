"""
Simple test to validate that UI adapter methods never return None or cause NoneType errors.
"""

from unittest.mock import Mock, patch
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

from utils.datahub_client_adapter import DataHubRestClient


def test_ui_no_none_errors():
    """Test that UI methods never return None or cause NoneType errors."""
    
    print("Testing UI adapter methods for NoneType error prevention...")
    
    # Create a DataHub client with mocked connection that returns None
    with patch('datahub_cicd_client.core.connection.DataHubConnection') as mock_conn_class:
        mock_conn = Mock()
        mock_conn.test_connection.return_value = True
        # Mock all GraphQL calls to return None (simulating connection failures)
        mock_conn.execute_graphql.return_value = None
        mock_conn_class.return_value = mock_conn
        
        client = DataHubRestClient('http://localhost:8080', 'test-token')
        
        # Test all count methods
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
        
        print("\n1. Testing count methods...")
        for method_name in count_methods:
            try:
                method = getattr(client, method_name)
                result = method()
                assert isinstance(result, int), f"{method_name} should return int, got {type(result)}"
                assert result >= 0, f"{method_name} should return non-negative integer"
                print(f"  ✓ {method_name}: {result} (int)")
            except Exception as e:
                print(f"  ✗ {method_name}: ERROR - {e}")
                return False
        
        # Test all list methods
        list_methods = [
            'list_tags',
            'list_domains',
            'list_structured_properties',
            'list_data_products',
            'list_glossary_nodes',
            'list_glossary_terms',
            'list_tests',
            'list_users',
            'list_groups',
            'list_ownership_types'
        ]
        
        print("\n2. Testing list methods...")
        for method_name in list_methods:
            try:
                method = getattr(client, method_name)
                result = method()
                assert isinstance(result, list), f"{method_name} should return list, got {type(result)}"
                print(f"  ✓ {method_name}: {len(result)} items (list)")
            except Exception as e:
                print(f"  ✗ {method_name}: ERROR - {e}")
                return False
        
        # Test complex data methods
        complex_methods = [
            ('get_comprehensive_glossary_data', dict, ['glossary_nodes', 'glossary_terms']),
            ('get_remote_tags_data', dict, ['searchResults']),
            ('get_data_contracts', dict, ['success', 'data']),
            ('get_assertions', dict, ['success', 'data'])
        ]
        
        print("\n3. Testing complex data methods...")
        for method_name, expected_type, required_keys in complex_methods:
            try:
                method = getattr(client, method_name)
                result = method()
                assert isinstance(result, expected_type), f"{method_name} should return {expected_type.__name__}, got {type(result)}"
                for key in required_keys:
                    assert key in result, f"{method_name} should have {key} key"
                print(f"  ✓ {method_name}: {expected_type.__name__} with keys {list(result.keys())}")
            except Exception as e:
                print(f"  ✗ {method_name}: ERROR - {e}")
                return False
        
        # Test that methods don't call .get() on None values
        print("\n4. Testing explicit None handling...")
        try:
            with patch.object(client.glossary_service, 'get_comprehensive_glossary_data', return_value=None):
                result = client.get_comprehensive_glossary_data()
                assert isinstance(result, dict), "Should return dict even when service returns None"
                assert result['glossary_nodes'] == [], "Should have empty glossary_nodes"
                assert result['glossary_terms'] == [], "Should have empty glossary_terms"
                print("  ✓ Explicit None handling works correctly")
        except Exception as e:
            print(f"  ✗ Explicit None handling: ERROR - {e}")
            return False
        
        # Test safe_execute_graphql
        print("\n5. Testing safe_execute_graphql...")
        try:
            result = client.safe_execute_graphql("query { test }")
            assert result is not None, "safe_execute_graphql should never return None"
            assert isinstance(result, dict), "safe_execute_graphql should return dict"
            print(f"  ✓ safe_execute_graphql: {type(result)} (never None)")
        except Exception as e:
            print(f"  ✗ safe_execute_graphql: ERROR - {e}")
            return False
        
        # Test error handling with exceptions
        print("\n6. Testing exception handling...")
        try:
            with patch.object(client.tag_service, 'list_tags', side_effect=Exception("Connection error")):
                result = client.list_tags()
                assert isinstance(result, list), "Should return list even on exception"
                assert result == [], "Should return empty list on exception"
                print("  ✓ Exception handling returns proper fallback values")
        except Exception as e:
            print(f"  ✗ Exception handling: ERROR - {e}")
            return False
    
    print("\n✅ All tests passed! UI methods will not cause NoneType errors.")
    return True


if __name__ == "__main__":
    success = test_ui_no_none_errors()
    if success:
        print("\n🎉 SUCCESS: All UI adapter methods are protected against NoneType errors!")
        exit(0)
    else:
        print("\n❌ FAILURE: Some tests failed.")
        exit(1) 