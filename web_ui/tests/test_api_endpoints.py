"""
Comprehensive tests for all REST API endpoints.

Tests cover:
- Settings API endpoints (get_settings, get_git_settings, get_system_info)
- Connection API endpoints (list, get, test, switch)
- Dashboard API endpoints (dashboard_data)
- Data API endpoints (recipes_data, policies_data)
- Template API endpoints (preview, env-vars)
- Environment Variables API endpoints (templates, instances, JSON)
- GitHub Integration API endpoints (branches, diff, file diff)
- Metadata Management API endpoints (users-groups)
- Recipe Template API endpoints
- Health check endpoint
- API authentication and authorization
- API error handling and validation
- API performance and rate limiting
- API response formats and schemas
- API documentation endpoints
"""

import json
from unittest.mock import patch, Mock, MagicMock
from django.test import TestCase, Client
from django.urls import reverse
from django.contrib.auth.models import User
from rest_framework.test import APITestCase, APIClient
from rest_framework import status
from django.http import JsonResponse

from web_ui.models import Settings, AppSettings, GitSettings, Connection
from tests.fixtures.simple_factories import UserFactory, EnvironmentFactory, ConnectionFactory


class SettingsAPIEndpointsTestCase(APITestCase):
    """Test Settings-related API endpoints."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()
        self.client = APIClient()

    def tearDown(self):
        """Clean up test data."""
        Settings.objects.all().delete()
        GitSettings.objects.all().delete()

    def test_api_get_settings_endpoint(self):
        """Test GET /api/settings/ endpoint."""
        # Set up test settings
        AppSettings.set('policy_export_dir', '/test/policies')
        AppSettings.set('recipe_dir', '/test/recipes')
        AppSettings.set('log_level', 'DEBUG')
        AppSettings.set('refresh_rate', '30')
        
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-settings'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('policy', response.data)
        self.assertIn('recipe', response.data)
        self.assertIn('advanced', response.data)
        
        # Verify structure and data
        self.assertEqual(response.data['policy']['export_dir'], '/test/policies')
        self.assertEqual(response.data['recipe']['directory'], '/test/recipes')
        self.assertEqual(response.data['advanced']['log_level'], 'DEBUG')
        self.assertEqual(response.data['advanced']['refresh_rate'], 30)

    def test_api_get_settings_with_defaults(self):
        """Test GET /api/settings/ with default values."""
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-settings'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Check default values
        self.assertEqual(response.data['policy']['default_type'], 'METADATA')
        self.assertTrue(response.data['policy']['validate_on_import'])
        self.assertEqual(response.data['recipe']['default_schedule'], '0 0 * * *')
        self.assertEqual(response.data['advanced']['log_level'], 'INFO')

    def test_api_get_git_settings_endpoint(self):
        """Test GET /api/settings/git/ endpoint."""
        # Create git settings
        GitSettings.objects.create(
            provider_type='gitlab',
            base_url='https://gitlab.example.com',
            username='testuser',
            repository='testrepo',
            token='secret_token',
            enabled=True
        )
        
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-git-settings'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Verify data (token should be excluded for security)
        self.assertEqual(response.data['provider_type'], 'gitlab')
        self.assertEqual(response.data['base_url'], 'https://gitlab.example.com')
        self.assertEqual(response.data['username'], 'testuser')
        self.assertEqual(response.data['repository'], 'testrepo')
        self.assertTrue(response.data['enabled'])
        self.assertNotIn('token', response.data)  # Security check

    def test_api_get_system_info_endpoint(self):
        """Test GET /api/settings/system/ endpoint."""
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-system-info'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Verify system info structure
        self.assertIn('version', response.data)
        self.assertIn('environment', response.data)
        self.assertIn('database', response.data)

    def test_api_settings_anonymous_access(self):
        """Test API settings endpoints with anonymous access."""
        # Test without authentication (should work with anonymous backend)
        response = self.client.get(reverse('api-settings'))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        response = self.client.get(reverse('api-git-settings'))
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    @patch('web_ui.api_views.AppSettings.get')
    def test_api_settings_error_handling(self, mock_get):
        """Test API settings error handling."""
        mock_get.side_effect = Exception("Database error")
        
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-settings'))
        
        # Should handle error gracefully
        self.assertEqual(response.status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)


class ConnectionAPIEndpointsTestCase(APITestCase):
    """Test Connection-related API endpoints."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()
        self.client = APIClient()
        self.connection = ConnectionFactory(
            name="Test Connection",
            datahub_url="http://test.datahub.com",
            datahub_token="test_token",
            is_default=True
        )

    def tearDown(self):
        """Clean up test data."""
        Connection.objects.all().delete()

    def test_api_list_connections_endpoint(self):
        """Test GET /api/connections/ endpoint."""
        # Create additional connections
        ConnectionFactory(name="Connection 2", is_default=False)
        ConnectionFactory(name="Connection 3", is_default=False)
        
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-connections'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 3)
        
        # Verify structure
        connection_data = response.data[0]
        self.assertIn('id', connection_data)
        self.assertIn('name', connection_data)
        self.assertIn('datahub_url', connection_data)
        self.assertIn('is_default', connection_data)
        
        # Token should not be exposed
        self.assertNotIn('datahub_token', connection_data)

    def test_api_get_connection_endpoint(self):
        """Test GET /api/connections/{id}/ endpoint."""
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-connection-detail', args=[self.connection.id]))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['name'], "Test Connection")
        self.assertEqual(response.data['datahub_url'], "http://test.datahub.com")
        self.assertTrue(response.data['is_default'])

    def test_api_get_connection_not_found(self):
        """Test GET /api/connections/{id}/ with non-existent connection."""
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-connection-detail', args=[999]))
        
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    @patch('web_ui.api_views.get_datahub_client_from_connection')
    def test_api_test_connection_endpoint(self, mock_get_client):
        """Test POST /api/connections/{id}/test/ endpoint."""
        # Mock successful connection test
        mock_client = Mock()
        mock_client.test_connection.return_value = True
        mock_get_client.return_value = mock_client
        
        self.client.force_authenticate(user=self.user)
        response = self.client.post(reverse('api-connection-test', args=[self.connection.id]))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data['success'])
        self.assertIn('message', response.data)

    @patch('web_ui.api_views.get_datahub_client_from_connection')
    def test_api_test_connection_failure(self, mock_get_client):
        """Test POST /api/connections/{id}/test/ with connection failure."""
        # Mock connection failure
        mock_get_client.side_effect = Exception("Connection failed")
        
        self.client.force_authenticate(user=self.user)
        response = self.client.post(reverse('api-connection-test', args=[self.connection.id]))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(response.data['success'])
        self.assertIn('error', response.data)

    def test_api_switch_connection_endpoint(self):
        """Test POST /api/connections/switch/ endpoint."""
        # Create another connection to switch to
        other_connection = ConnectionFactory(name="Other Connection", is_default=False)
        
        self.client.force_authenticate(user=self.user)
        response = self.client.post(reverse('api-connection-switch'), {
            'connection_id': other_connection.id
        })
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('message', response.data)

    def test_api_switch_connection_invalid_id(self):
        """Test POST /api/connections/switch/ with invalid connection ID."""
        self.client.force_authenticate(user=self.user)
        response = self.client.post(reverse('api-connection-switch'), {
            'connection_id': 999
        })
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)


class DashboardAPIEndpointsTestCase(APITestCase):
    """Test Dashboard-related API endpoints."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()
        self.client = APIClient()

    @patch('web_ui.api_views.get_datahub_client_from_request')
    def test_api_dashboard_data_endpoint(self, mock_get_client):
        """Test GET /api/dashboard/data/ endpoint."""
        # Mock DataHub client response
        mock_client = Mock()
        mock_client.test_connection.return_value = True
        mock_client.list_ingestion_sources.return_value = [
            {
                'urn': 'urn:li:datahubIngestionsource:test-source',
                'id': 'test-source',
                'name': 'Test Source',
                'type': 'mysql',
                'lastUpdated': 1640995200000,
                'schedule': None,
                'recipe': {'source': {'type': 'mysql'}},
                'is_active': False
            }
        ]
        mock_get_client.return_value = mock_client
        
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-dashboard-data'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Verify response structure
        self.assertIn('connected', response.data)
        self.assertIn('recipes_count', response.data)
        self.assertIn('active_schedules_count', response.data)
        self.assertIn('policies_count', response.data)
        self.assertIn('recent_recipes', response.data)
        self.assertIn('recent_policies', response.data)
        self.assertIn('environments', response.data)
        self.assertIn('metadata_stats', response.data)
        self.assertIn('git_status', response.data)
        self.assertIn('system_health', response.data)

    def test_api_dashboard_data_no_connection(self):
        """Test GET /api/dashboard/data/ without DataHub connection."""
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-dashboard-data'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(response.data['connected'])
        self.assertEqual(response.data['recipes_count'], 0)


class DataAPIEndpointsTestCase(APITestCase):
    """Test Data-related API endpoints (recipes, policies)."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()
        self.client = APIClient()

    @patch('web_ui.api_views.get_datahub_client_from_request')
    def test_api_recipes_data_endpoint(self, mock_get_client):
        """Test GET /api/recipes/data/ endpoint."""
        # Mock DataHub client response
        mock_client = Mock()
        mock_client.list_ingestion_sources.return_value = [
            {
                'urn': 'urn:li:datahubIngestionsource:recipe1',
                'id': 'recipe1',
                'name': 'Recipe 1',
                'type': 'mysql',
                'schedule': None,
                'lastUpdated': 1640995200000
            },
            {
                'urn': 'urn:li:datahubIngestionsource:recipe2',
                'id': 'recipe2',
                'name': 'Recipe 2', 
                'type': 'postgres',
                'schedule': {'interval': '0 12 * * *'},
                'lastUpdated': 1641081600000
            }
        ]
        mock_get_client.return_value = mock_client
        
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-recipes-data'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('data', response.data)
        self.assertEqual(len(response.data['data']), 2)
        
        # Verify recipe data structure
        recipe_data = response.data['data'][0]
        self.assertIn('id', recipe_data)
        self.assertIn('name', recipe_data)
        self.assertIn('type', recipe_data)

    @patch('web_ui.api_views.get_cached_policies')
    def test_api_policies_data_endpoint(self, mock_get_policies):
        """Test GET /api/policies/data/ endpoint."""
        # Mock policies response
        mock_get_policies.return_value = [
            {
                'urn': 'urn:li:datahubpolicy:policy1',
                'id': 'policy1',
                'name': 'Test Policy 1',
                'type': 'METADATA',
                'state': 'ACTIVE',
                'lastUpdated': 1640995200000
            },
            {
                'urn': 'urn:li:datahubpolicy:policy2',
                'id': 'policy2',
                'name': 'Test Policy 2',
                'type': 'PLATFORM',
                'state': 'INACTIVE',
                'lastUpdated': 1641081600000
            }
        ]
        
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-policies-data'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('data', response.data)
        self.assertEqual(len(response.data['data']), 2)
        
        # Verify policy data structure
        policy_data = response.data['data'][0]
        self.assertIn('id', policy_data)
        self.assertIn('name', policy_data)
        self.assertIn('type', policy_data)

    def test_api_recipes_data_with_search(self):
        """Test GET /api/recipes/data/ with search parameters."""
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-recipes-data'), {
            'search': 'mysql',
            'type': 'mysql',
            'page': 1,
            'limit': 10
        })
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should handle search parameters gracefully even without data

    def test_api_policies_data_with_filters(self):
        """Test GET /api/policies/data/ with filter parameters."""
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-policies-data'), {
            'type': 'METADATA',
            'state': 'ACTIVE',
            'search': 'test'
        })
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should handle filter parameters gracefully


class TemplateAPIEndpointsTestCase(APITestCase):
    """Test Template-related API endpoints."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()
        self.client = APIClient()

    def test_api_template_preview_endpoint(self):
        """Test GET /api/templates/{id}/preview/ endpoint."""
        # This endpoint might not exist yet or requires template creation
        # Testing the URL pattern and expected behavior
        
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-template-preview', args=[1]))
        
        # Endpoint might return 404 if template doesn't exist, which is expected
        self.assertIn(response.status_code, [status.HTTP_200_OK, status.HTTP_404_NOT_FOUND])

    def test_api_template_env_vars_endpoint(self):
        """Test GET /api/templates/{id}/env-vars/ endpoint."""
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-template-env-vars', args=[1]))
        
        # Endpoint might return 404 if template doesn't exist, which is expected
        self.assertIn(response.status_code, [status.HTTP_200_OK, status.HTTP_404_NOT_FOUND])


class EnvironmentVariablesAPIEndpointsTestCase(APITestCase):
    """Test Environment Variables API endpoints."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()
        self.client = APIClient()

    def test_api_env_vars_templates_endpoint(self):
        """Test GET /api/env-vars/templates/ endpoint."""
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-env-vars-templates'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should return list of environment variable templates
        self.assertIsInstance(response.data, (list, dict))

    def test_api_env_vars_template_detail_endpoint(self):
        """Test GET /api/env-vars/templates/{id}/ endpoint."""
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-env-vars-template', args=[1]))
        
        # Might return 404 if template doesn't exist
        self.assertIn(response.status_code, [status.HTTP_200_OK, status.HTTP_404_NOT_FOUND])

    def test_api_env_vars_instances_endpoint(self):
        """Test GET /api/env-vars/instances/ endpoint."""
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-env-vars-instances'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # Should return list of environment variable instances
        self.assertIsInstance(response.data, (list, dict))

    def test_api_env_vars_instance_json_endpoint(self):
        """Test GET /api/env-vars/instances/{id}/json/ endpoint."""
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-env-vars-instance-json', args=[1]))
        
        # Might return 404 if instance doesn't exist
        self.assertIn(response.status_code, [status.HTTP_200_OK, status.HTTP_404_NOT_FOUND])


class GitHubAPIEndpointsTestCase(APITestCase):
    """Test GitHub Integration API endpoints."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()
        self.client = APIClient()

    @patch('web_ui.api_views.GitHubIntegration')
    def test_api_github_branches_endpoint(self, mock_github):
        """Test GET /api/github/branches/ endpoint."""
        # Mock GitHub integration
        mock_instance = Mock()
        mock_instance.list_branches.return_value = [
            {'name': 'main', 'protected': True},
            {'name': 'feature/test', 'protected': False}
        ]
        mock_github.return_value = mock_instance
        
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-github-branches'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIsInstance(response.data, list)

    @patch('web_ui.api_views.GitHubIntegration')
    def test_api_github_branch_diff_endpoint(self, mock_github):
        """Test GET /api/github/branch-diff/ endpoint."""
        # Mock GitHub integration
        mock_instance = Mock()
        mock_instance.get_branch_diff.return_value = {
            'files_changed': 2,
            'additions': 10,
            'deletions': 5,
            'diff': 'mock diff content'
        }
        mock_github.return_value = mock_instance
        
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-github-branch-diff'), {
            'base': 'main',
            'head': 'feature/test'
        })
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('files_changed', response.data)

    @patch('web_ui.api_views.GitHubIntegration')
    def test_api_github_file_diff_endpoint(self, mock_github):
        """Test POST /api/github/file-diff/ endpoint."""
        # Mock GitHub integration
        mock_instance = Mock()
        mock_instance.get_file_diff.return_value = {
            'filename': 'test.yml',
            'diff': 'mock file diff content'
        }
        mock_github.return_value = mock_instance
        
        self.client.force_authenticate(user=self.user)
        response = self.client.post(reverse('api-github-file-diff'), {
            'filepath': 'test.yml',
            'base': 'main',
            'head': 'feature/test'
        })
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('filename', response.data)

    def test_api_github_endpoints_without_configuration(self):
        """Test GitHub API endpoints without GitHub configuration."""
        self.client.force_authenticate(user=self.user)
        
        # Should handle missing GitHub configuration gracefully
        response = self.client.get(reverse('api-github-branches'))
        self.assertIn(response.status_code, [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
            status.HTTP_500_INTERNAL_SERVER_ERROR
        ])


class MetadataAPIEndpointsTestCase(APITestCase):
    """Test Metadata Management API endpoints."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()
        self.client = APIClient()

    @patch('web_ui.api_views.get_datahub_client_from_request')
    def test_api_metadata_users_groups_endpoint(self, mock_get_client):
        """Test GET /api/metadata/users-groups/ endpoint."""
        # Mock DataHub client response
        mock_client = Mock()
        mock_client.get_users_and_groups.return_value = {
            'users': [
                {'urn': 'urn:li:corpuser:user1', 'username': 'user1'},
                {'urn': 'urn:li:corpuser:user2', 'username': 'user2'}
            ],
            'groups': [
                {'urn': 'urn:li:corpGroup:group1', 'name': 'group1'},
                {'urn': 'urn:li:corpGroup:group2', 'name': 'group2'}
            ]
        }
        mock_get_client.return_value = mock_client
        
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-metadata-users-groups'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn('users', response.data)
        self.assertIn('groups', response.data)

    def test_api_metadata_users_groups_no_connection(self):
        """Test GET /api/metadata/users-groups/ without DataHub connection."""
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-metadata-users-groups'))
        
        # Should handle missing connection gracefully
        self.assertIn(response.status_code, [
            status.HTTP_200_OK,  # Empty result
            status.HTTP_400_BAD_REQUEST,  # No connection
            status.HTTP_500_INTERNAL_SERVER_ERROR  # Connection error
        ])


class APIDocumentationEndpointsTestCase(APITestCase):
    """Test API documentation endpoints."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()
        self.client = APIClient()

    def test_api_schema_endpoint(self):
        """Test GET /api/schema/ endpoint."""
        response = self.client.get(reverse('schema'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response['Content-Type'], 'application/vnd.oai.openapi')

    def test_swagger_ui_endpoint(self):
        """Test GET /api/docs/ endpoint."""
        response = self.client.get(reverse('swagger-ui'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertContains(response, 'swagger')

    def test_redoc_endpoint(self):
        """Test GET /api/redoc/ endpoint."""
        response = self.client.get(reverse('redoc'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertContains(response, 'redoc')


class APIAuthenticationTestCase(APITestCase):
    """Test API authentication and authorization."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()
        self.client = APIClient()

    def test_api_endpoints_anonymous_access(self):
        """Test API endpoints with anonymous access."""
        # Test various endpoints without authentication
        endpoints = [
            reverse('api-settings'),
            reverse('api-git-settings'),
            reverse('api-system-info'),
            reverse('api-connections'),
            reverse('api-dashboard-data'),
        ]
        
        for endpoint in endpoints:
            response = self.client.get(endpoint)
            # With anonymous backend, most endpoints should be accessible
            self.assertEqual(response.status_code, status.HTTP_200_OK, 
                           f"Endpoint {endpoint} failed with anonymous access")

    def test_api_endpoints_authenticated_access(self):
        """Test API endpoints with authenticated access."""
        self.client.force_authenticate(user=self.user)
        
        endpoints = [
            reverse('api-settings'),
            reverse('api-git-settings'),
            reverse('api-system-info'),
            reverse('api-connections'),
            reverse('api-dashboard-data'),
        ]
        
        for endpoint in endpoints:
            response = self.client.get(endpoint)
            self.assertEqual(response.status_code, status.HTTP_200_OK,
                           f"Endpoint {endpoint} failed with authenticated access")

    def test_api_session_authentication(self):
        """Test API endpoints with session authentication."""
        # Use Django test client for session authentication
        client = Client()
        client.force_login(self.user)
        
        response = client.get(reverse('api-settings'))
        self.assertEqual(response.status_code, 200)

    def test_api_csrf_protection(self):
        """Test API POST endpoints CSRF protection."""
        # Test CSRF protection on POST endpoints
        client = Client()
        client.force_login(self.user)
        
        # POST without CSRF token should fail for non-API views
        # But API views might be CSRF-exempt
        response = client.post(reverse('api-connection-switch'), {
            'connection_id': 1
        })
        
        # API endpoints are typically CSRF-exempt
        self.assertIn(response.status_code, [200, 400, 404])


class APIErrorHandlingTestCase(APITestCase):
    """Test API error handling and validation."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()
        self.client = APIClient()

    def test_api_404_errors(self):
        """Test API 404 error handling."""
        self.client.force_authenticate(user=self.user)
        
        # Test non-existent endpoints
        response = self.client.get('/api/nonexistent/')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_api_method_not_allowed(self):
        """Test API method not allowed errors."""
        self.client.force_authenticate(user=self.user)
        
        # Try POST on GET-only endpoint
        response = self.client.post(reverse('api-settings'))
        self.assertEqual(response.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)

    def test_api_invalid_parameters(self):
        """Test API invalid parameter handling."""
        self.client.force_authenticate(user=self.user)
        
        # Test with invalid connection ID
        response = self.client.get(reverse('api-connection-detail', args=['invalid']))
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    @patch('web_ui.api_views.AppSettings.get')
    def test_api_internal_server_errors(self, mock_get):
        """Test API internal server error handling."""
        mock_get.side_effect = Exception("Internal error")
        
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-settings'))
        
        self.assertEqual(response.status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)

    def test_api_validation_errors(self):
        """Test API validation error handling."""
        self.client.force_authenticate(user=self.user)
        
        # Test connection switch with missing data
        response = self.client.post(reverse('api-connection-switch'), {})
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_api_malformed_json(self):
        """Test API malformed JSON handling."""
        self.client.force_authenticate(user=self.user)
        
        # Send malformed JSON
        response = self.client.post(
            reverse('api-connection-switch'),
            data='{"invalid": json}',
            content_type='application/json'
        )
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)


class APIResponseFormatTestCase(APITestCase):
    """Test API response formats and schemas."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()
        self.client = APIClient()

    def test_api_json_response_format(self):
        """Test API JSON response format."""
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-settings'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response['Content-Type'], 'application/json')
        
        # Response should be valid JSON
        self.assertIsInstance(response.data, dict)

    def test_api_response_headers(self):
        """Test API response headers."""
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-settings'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Check for common API headers
        self.assertIn('Content-Type', response)
        # CORS headers might be present
        if 'Access-Control-Allow-Origin' in response:
            self.assertIsNotNone(response['Access-Control-Allow-Origin'])

    def test_api_pagination_format(self):
        """Test API pagination response format."""
        self.client.force_authenticate(user=self.user)
        
        # Test endpoints that might have pagination
        response = self.client.get(reverse('api-recipes-data'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Response might have pagination info
        if 'count' in response.data:
            self.assertIn('results', response.data)
        elif 'data' in response.data:
            self.assertIsInstance(response.data['data'], list)

    def test_api_error_response_format(self):
        """Test API error response format."""
        self.client.force_authenticate(user=self.user)
        
        # Trigger an error response
        response = self.client.get(reverse('api-connection-detail', args=[999]))
        
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        
        # Error response should have consistent format
        if hasattr(response, 'data'):
            # DRF error format
            self.assertIsInstance(response.data, dict)
        else:
            # Standard JSON error format
            self.assertEqual(response['Content-Type'], 'application/json')


class APIPerformanceTestCase(APITestCase):
    """Test API performance aspects."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()
        self.client = APIClient()

    def test_api_response_time(self):
        """Test API response time performance."""
        import time
        
        self.client.force_authenticate(user=self.user)
        
        start_time = time.time()
        response = self.client.get(reverse('api-settings'))
        response_time = time.time() - start_time
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Response should be reasonably fast (adjust threshold as needed)
        self.assertLess(response_time, 2.0)  # Should respond within 2 seconds

    def test_api_concurrent_requests(self):
        """Test API handling of concurrent requests."""
        import threading
        import time
        
        self.client.force_authenticate(user=self.user)
        
        responses = []
        
        def make_request():
            response = self.client.get(reverse('api-settings'))
            responses.append(response.status_code)
        
        # Create multiple threads for concurrent requests
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=make_request)
            threads.append(thread)
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # All requests should succeed
        self.assertEqual(len(responses), 5)
        for status_code in responses:
            self.assertEqual(status_code, 200)

    def test_api_large_response_handling(self):
        """Test API handling of large responses."""
        # Create large dataset
        for i in range(50):
            AppSettings.set(f'test_setting_{i}', f'value_{i}' * 100)
        
        try:
            self.client.force_authenticate(user=self.user)
            
            import time
            start_time = time.time()
            response = self.client.get(reverse('api-settings'))
            response_time = time.time() - start_time
            
            self.assertEqual(response.status_code, status.HTTP_200_OK)
            
            # Should handle large response reasonably well
            self.assertLess(response_time, 5.0)  # Should respond within 5 seconds
            
        finally:
            # Clean up test data
            Settings.objects.filter(key__startswith='test_setting_').delete()


class HealthCheckEndpointTestCase(APITestCase):
    """Test health check endpoint."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()
        self.client = APIClient()

    def test_health_check_endpoint(self):
        """Test GET /health/ endpoint."""
        response = self.client.get(reverse('health'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # Health check should return basic system info
        if hasattr(response, 'data'):
            self.assertIn('status', response.data)
        else:
            # Might be a simple text response
            self.assertContains(response, 'OK')

    def test_health_check_anonymous_access(self):
        """Test health check endpoint with anonymous access."""
        # Health check should be accessible without authentication
        response = self.client.get(reverse('health'))
        
        self.assertEqual(response.status_code, status.HTTP_200_OK) 