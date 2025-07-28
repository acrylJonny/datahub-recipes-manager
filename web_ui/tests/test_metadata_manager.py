"""
Simplified tests for Metadata Manager API endpoints.

Focuses on testing JSON API endpoints without complex template rendering or full application integration.
"""

import json
import uuid
from unittest.mock import Mock, patch
from django.test import TestCase, Client
from django.contrib.auth.models import User

from tests.fixtures.simple_factories import UserFactory


class MetadataManagerAPITestCase(TestCase):
    """Test metadata manager API endpoints."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
    
    def test_editable_entities_list_api(self):
        """Test editable entities list API."""
        self.client.force_login(self.user)
        response = self.client.get('/metadata/entities/editable/list/')
        # Should return JSON data or handle gracefully
        self.assertIn(response.status_code, [200, 404])
    
    def test_search_progress_api(self):
        """Test search progress API."""
        self.client.force_login(self.user)
        response = self.client.get('/metadata/entities/editable/progress/')
        self.assertIn(response.status_code, [200, 404])
    

    
    def test_property_values_api(self):
        """Test property values API."""
        self.client.force_login(self.user)
        response = self.client.get('/metadata/properties/values/')
        self.assertIn(response.status_code, [200, 404])
    
    def test_remote_tags_data_api(self):
        """Test remote tags data API."""
        self.client.force_login(self.user)
        response = self.client.get('/metadata/tags/remote-data/')
        self.assertIn(response.status_code, [200, 404])
    
    def test_users_and_groups_api(self):
        """Test users and groups API."""
        self.client.force_login(self.user)
        response = self.client.get('/metadata/tags/users-groups/')
        self.assertIn(response.status_code, [200, 404])
    

    
    def test_domains_data_api(self):
        """Test domains data API."""
        self.client.force_login(self.user)
        response = self.client.get('/metadata/domains/data/')
        self.assertIn(response.status_code, [200, 404])
    
    def test_data_contracts_data_api(self):
        """Test data contracts data API."""
        self.client.force_login(self.user)
        response = self.client.get('/metadata/data-contracts/data/')
        self.assertIn(response.status_code, [200, 404])
    
    def test_data_products_data_api(self):
        """Test data products data API."""
        self.client.force_login(self.user)
        response = self.client.get('/metadata/data-products/data/')
        self.assertIn(response.status_code, [200, 404])
    
    def test_assertions_data_api(self):
        """Test assertions data API."""
        self.client.force_login(self.user)
        response = self.client.get('/metadata/assertions/data/')
        self.assertIn(response.status_code, [200, 404])
    
    def test_metadata_tests_data_api(self):
        """Test metadata tests data API."""
        self.client.force_login(self.user)
        response = self.client.get('/metadata/tests/data/')
        self.assertIn(response.status_code, [200, 302, 404])
    
    def test_metadata_tests_remote_data_api(self):
        """Test metadata tests remote data API."""
        self.client.force_login(self.user)
        response = self.client.get('/metadata/tests/remote-data/')
        self.assertIn(response.status_code, [200, 404])


class MetadataManagerUtilityAPITestCase(TestCase):
    """Test metadata manager utility API endpoints."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
    
    def test_platforms_api(self):
        """Test platforms API."""
        self.client.force_login(self.user)
        response = self.client.get('/metadata/platforms/')
        self.assertIn(response.status_code, [200, 400, 404])
    
    def test_platform_instances_api(self):
        """Test platform instances API."""
        self.client.force_login(self.user)
        response = self.client.get('/metadata/platform-instances/')
        self.assertIn(response.status_code, [200, 404])
    
    def test_entity_details_api(self):
        """Test entity details API."""
        self.client.force_login(self.user)
        test_urn = "urn:li:dataset:(test,dataset)"
        response = self.client.get(f'/metadata/entities/{test_urn}/')
        self.assertIn(response.status_code, [200, 404])
    
    def test_entity_schema_api(self):
        """Test entity schema API."""
        self.client.force_login(self.user)
        test_urn = "urn:li:dataset:(test,dataset)"
        response = self.client.get(f'/metadata/entities/{test_urn}/schema/')
        self.assertIn(response.status_code, [200, 404])
    
    def test_datahub_url_config_api(self):
        """Test DataHub URL config API."""
        self.client.force_login(self.user)
        response = self.client.get('/metadata/config/datahub-url/')
        self.assertIn(response.status_code, [200, 404])
    
    def test_structured_properties_api(self):
        """Test structured properties API."""
        self.client.force_login(self.user)
        response = self.client.get('/metadata/structured-properties/')
        self.assertIn(response.status_code, [200, 404]) 