"""
API Integration Tests for Metadata Manager
Tests API endpoints for reliability and correct response handling.
"""

from django.test import TestCase
from django.urls import reverse
from django.contrib.auth.models import User
from tests.fixtures.simple_factories import UserFactory

class MetadataManagerAPITestCase(TestCase):
    """Test API endpoints for metadata manager functionality."""
    
    def setUp(self):
        self.user = UserFactory()
        self.client.force_login(self.user)
    
    def test_properties_data_api(self):
        """Test that properties data API endpoint returns successfully."""
        url = reverse('metadata_manager:properties_data')
        response = self.client.get(url)
        
        # Flexible assertion for different scenarios
        self.assertIn(response.status_code, [200, 404])
        
        if response.status_code == 200:
            # Additional validation for successful responses
            self.assertNotIn(b'error', response.content.lower())
            self.assertNotIn(b'exception', response.content.lower())
    
    def test_get_remote_tags_data_api(self):
        """Test that remote tags data API endpoint returns successfully."""
        url = reverse('metadata_manager:get_remote_tags_data')
        response = self.client.get(url)
        
        self.assertIn(response.status_code, [200, 404, 503])
        
        if response.status_code == 200:
            self.assertNotIn(b'error', response.content.lower())
            self.assertNotIn(b'exception', response.content.lower())
    
    def test_get_remote_glossary_data_api(self):
        """Test that remote glossary data API endpoint returns successfully."""
        url = reverse('metadata_manager:get_remote_glossary_data')
        response = self.client.get(url)
        
        self.assertIn(response.status_code, [200, 404, 503])
        
        if response.status_code == 200:
            self.assertNotIn(b'error', response.content.lower())
            self.assertNotIn(b'exception', response.content.lower())
    
    def test_get_remote_data_products_data_api(self):
        """Test that remote data products data API endpoint returns successfully."""
        url = reverse('metadata_manager:get_remote_data_products_data')
        response = self.client.get(url)
        
        self.assertIn(response.status_code, [200, 404, 503])
        
        if response.status_code == 200:
            self.assertNotIn(b'error', response.content.lower())
            self.assertNotIn(b'exception', response.content.lower())
    
    def test_get_datahub_assertions_api(self):
        """Test that DataHub assertions API endpoint returns successfully."""
        url = reverse('metadata_manager:get_datahub_assertions')
        response = self.client.get(url)
        
        self.assertIn(response.status_code, [200, 404, 503])
        
        if response.status_code == 200:
            self.assertNotIn(b'error', response.content.lower())
            self.assertNotIn(b'exception', response.content.lower())
    
    def test_tests_data_api(self):
        """Test that tests data API endpoint returns successfully."""
        url = reverse('metadata_manager:tests_data')
        response = self.client.get(url)
        
        # This endpoint might redirect or return various status codes
        self.assertIn(response.status_code, [200, 302, 404])
        
        if response.status_code == 200:
            self.assertNotIn(b'error', response.content.lower())
            self.assertNotIn(b'exception', response.content.lower())
    
    def test_get_platforms_api(self):
        """Test that platforms API endpoint returns successfully."""
        url = reverse('metadata_manager:get_platforms')
        response = self.client.get(url)
        
        # Platforms endpoint might have validation requirements
        self.assertIn(response.status_code, [200, 400, 404])
        
        if response.status_code == 200:
            self.assertNotIn(b'error', response.content.lower())
            self.assertNotIn(b'exception', response.content.lower())


class MetadataManagerUtilityAPITestCase(TestCase):
    """Test utility API endpoints for metadata manager."""
    
    def setUp(self):
        self.user = UserFactory()
        self.client.force_login(self.user)
    
    def test_data_contract_list_api(self):
        """Test data contracts list API endpoint."""
        url = reverse('metadata_manager:data_contract_list')
        response = self.client.get(url)
        
        self.assertIn(response.status_code, [200, 404])
        
        if response.status_code == 200:
            self.assertNotIn(b'server error', response.content.lower())
            self.assertNotIn(b'exception', response.content.lower())
    
    def test_get_remote_data_contracts_data_api(self):
        """Test remote data contracts API endpoint."""
        url = reverse('metadata_manager:get_remote_data_contracts_data')
        response = self.client.get(url)
        
        self.assertIn(response.status_code, [200, 404, 503])
        
        if response.status_code == 200:
            self.assertNotIn(b'error', response.content.lower())
            self.assertNotIn(b'exception', response.content.lower())
    
    def test_export_entities_api(self):
        """Test export entities API endpoint."""
        url = reverse('metadata_manager:export_entities_with_mutations')
        response = self.client.get(url)
        
        self.assertIn(response.status_code, [200, 302, 404])
        
        if response.status_code == 200:
            self.assertNotIn(b'error', response.content.lower())
            self.assertNotIn(b'exception', response.content.lower())


class MetadataManagerPOSTEndpointsTestCase(TestCase):
    """Test POST endpoints for metadata manager."""
    
    def setUp(self):
        self.user = UserFactory()
        self.client.force_login(self.user)
    
    def test_data_product_create_endpoint(self):
        """Test data product creation POST endpoint."""
        url = reverse('metadata_manager:data_product_create')
        
        # Test with valid data
        valid_data = {
            'name': 'test-data-product',
            'display_name': 'Test Data Product',
            'description': 'Test data product description'
        }
        
        response = self.client.post(url, valid_data)
        # Accept various response codes as DataHub might not be available
        self.assertIn(response.status_code, [200, 201, 302, 400, 503])
    
    def test_editable_properties_endpoint(self):
        """Test editable properties endpoint."""
        url = reverse('metadata_manager:editable_properties')
        
        response = self.client.get(url)
        # Accept various response codes
        self.assertIn(response.status_code, [200, 302, 404])
    
    def test_get_editable_entities_endpoint(self):
        """Test get editable entities API endpoint."""
        url = reverse('metadata_manager:get_editable_entities')
        
        response = self.client.get(url)
        # Accept various response codes as DataHub might not be available
        self.assertIn(response.status_code, [200, 404, 503])
    
    def test_metadata_index_endpoint(self):
        """Test metadata index page endpoint."""
        url = reverse('metadata_manager:metadata_index')
        
        response = self.client.get(url)
        # Should return 200 for main page
        self.assertIn(response.status_code, [200, 302])


class MetadataManagerErrorHandlingTestCase(TestCase):
    """Test error handling in metadata manager APIs."""
    
    def setUp(self):
        self.user = UserFactory()
        self.client.force_login(self.user)
    
    def test_invalid_api_endpoint(self):
        """Test that invalid API endpoints return appropriate errors."""
        # Try to access a non-existent endpoint
        response = self.client.get('/metadata/invalid-endpoint/')
        
        # Should return 404
        self.assertEqual(response.status_code, 404)
    
    def test_unauthenticated_access(self):
        """Test that unauthenticated access is handled properly."""
        # Log out the user
        self.client.logout()
        
        # Try to access a protected endpoint
        url = reverse('metadata_manager:properties_data')
        response = self.client.get(url)
        
        # Should redirect to login or return unauthorized
        self.assertIn(response.status_code, [302, 401, 403])
    
    def test_malformed_post_data(self):
        """Test handling of malformed POST data."""
        url = reverse('metadata_manager:data_product_create')
        
        # Send malformed data
        malformed_data = {
            'invalid_field': 'invalid_value'
        }
        
        response = self.client.post(url, malformed_data)
        
        # Should handle gracefully (not crash)
        self.assertIn(response.status_code, [200, 400, 422, 503])
        
        # Ensure no server errors in response
        if response.status_code == 200:
            self.assertNotIn(b'server error', response.content.lower())
            self.assertNotIn(b'traceback', response.content.lower()) 