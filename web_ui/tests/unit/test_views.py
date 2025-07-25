"""
Comprehensive unit tests for Django views in the web_ui application.

Tests cover:
- View response status codes and content
- Authentication and permission requirements
- Form handling and validation
- Template rendering and context data
- AJAX endpoints and JSON responses
- Error handling and edge cases
"""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock
from django.test import TestCase, Client
from django.urls import reverse
from django.contrib.auth.models import User
from django.contrib.messages import get_messages
from django.http import JsonResponse
from django.core.files.uploadedfile import SimpleUploadedFile

from web_ui.web_ui.models import Environment, Policy, LogEntry, GitSettings, ScriptRun
from web_ui.web_ui.views import (
    index, dashboard, dashboard_data, policies, policy_create, 
    policy_edit, policy_delete, environments, logs, github_index
)
from tests.fixtures.factories import (
    UserFactory, EnvironmentFactory, PolicyFactory, 
    LogEntryFactory, GitSettingsFactory
)
from tests.utils.base_test import BaseWebUITestCase, MockHelper


class IndexViewTestCase(BaseWebUITestCase):
    """Test cases for the main index view."""
    
    def test_index_view_renders_correctly(self):
        """Test that index view renders with correct template."""
        response = self.client.get(reverse('index'))
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'DataHub Recipes Manager')
        self.assertTemplateUsed(response, 'index.html')
    
    def test_index_view_context_data(self):
        """Test that index view provides correct context data."""
        response = self.client.get(reverse('index'))
        
        self.assertIn('environments', response.context)
        self.assertIn('recent_runs', response.context)
        self.assertIn('system_status', response.context)
    
    def test_index_view_with_environments(self):
        """Test index view with multiple environments."""
        env1 = EnvironmentFactory(name='dev')
        env2 = EnvironmentFactory(name='prod')
        
        response = self.client.get(reverse('index'))
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'dev')
        self.assertContains(response, 'prod')


class DashboardViewTestCase(BaseWebUITestCase):
    """Test cases for the dashboard view."""
    
    def setUp(self):
        super().setUp()
        self.dashboard_url = reverse('dashboard')
    
    def test_dashboard_view_authenticated_access(self):
        """Test dashboard view requires authentication."""
        # Unauthenticated request should redirect
        response = self.client.get(self.dashboard_url)
        self.assertRedirects(response, f'/accounts/login/?next={self.dashboard_url}')
        
        # Authenticated request should work
        self.client.force_login(self.regular_user)
        response = self.client.get(self.dashboard_url)
        self.assertEqual(response.status_code, 200)
    
    def test_dashboard_view_template_and_context(self):
        """Test dashboard view uses correct template and context."""
        self.client.force_login(self.regular_user)
        response = self.client.get(self.dashboard_url)
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'dashboard.html')
        self.assertIn('page_title', response.context)
        self.assertEqual(response.context['page_title'], 'Dashboard')
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_dashboard_view_with_datahub_connection(self, mock_get_client):
        """Test dashboard view with DataHub connection."""
        mock_client = MockHelper.mock_datahub_client(connected=True)
        mock_get_client.return_value = mock_client
        
        self.client.force_login(self.regular_user)
        response = self.client.get(self.dashboard_url)
        
        self.assertEqual(response.status_code, 200)
        self.assertIn('datahub_connected', response.context)
        self.assertTrue(response.context['datahub_connected'])
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_dashboard_view_without_datahub_connection(self, mock_get_client):
        """Test dashboard view without DataHub connection."""
        mock_client = MockHelper.mock_datahub_client(connected=False)
        mock_get_client.return_value = mock_client
        
        self.client.force_login(self.regular_user)
        response = self.client.get(self.dashboard_url)
        
        self.assertEqual(response.status_code, 200)
        self.assertIn('datahub_connected', response.context)
        self.assertFalse(response.context['datahub_connected'])


class DashboardDataAPITestCase(BaseWebUITestCase):
    """Test cases for the dashboard data API endpoint."""
    
    def setUp(self):
        super().setUp()
        self.api_url = reverse('dashboard_data')
    
    def test_dashboard_data_requires_authentication(self):
        """Test that dashboard data API requires authentication."""
        response = self.client.get(self.api_url)
        self.assertEqual(response.status_code, 302)  # Redirect to login
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_dashboard_data_with_connection(self, mock_get_client):
        """Test dashboard data API with successful DataHub connection."""
        mock_client = MockHelper.mock_datahub_client(
            connected=True,
            recipes=[
                {'name': 'recipe1', 'lastUpdated': 1640995200000, 'schedule': {'interval': '0 0 * * *'}},
                {'name': 'recipe2', 'lastUpdated': 1640995300000, 'schedule': None}
            ],
            policies=[
                {'name': 'policy1', 'urn': 'urn:li:dataHubPolicy:policy1'},
                {'name': 'policy2', 'urn': 'urn:li:dataHubPolicy:policy2'}
            ]
        )
        mock_get_client.return_value = mock_client
        
        self.client.force_login(self.regular_user)
        response = self.client.get(self.api_url)
        
        self.assertEqual(response.status_code, 200)
        self.assertJSONResponse(response)
        
        data = self.get_json_response(response)
        self.assertTrue(data['connected'])
        self.assertEqual(data['recipes_count'], 2)
        self.assertEqual(data['active_schedules_count'], 1)
        self.assertEqual(data['policies_count'], 2)
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_dashboard_data_without_connection(self, mock_get_client):
        """Test dashboard data API without DataHub connection."""
        mock_client = MockHelper.mock_datahub_client(connected=False)
        mock_get_client.return_value = mock_client
        
        self.client.force_login(self.regular_user)
        response = self.client.get(self.api_url)
        
        self.assertEqual(response.status_code, 200)
        data = self.get_json_response(response)
        self.assertFalse(data['connected'])
        self.assertEqual(data['recipes_count'], 0)
        self.assertEqual(data['policies_count'], 0)
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_dashboard_data_with_exception(self, mock_get_client):
        """Test dashboard data API handles exceptions gracefully."""
        mock_get_client.side_effect = Exception("Connection failed")
        
        self.client.force_login(self.regular_user)
        response = self.client.get(self.api_url)
        
        self.assertEqual(response.status_code, 200)
        data = self.get_json_response(response)
        self.assertFalse(data['connected'])
        self.assertIn('error', data)


class PoliciesViewTestCase(BaseWebUITestCase):
    """Test cases for policies management views."""
    
    def setUp(self):
        super().setUp()
        self.policies_url = reverse('policies')
        self.environment = EnvironmentFactory()
    
    def test_policies_list_view(self):
        """Test policies list view."""
        policy1 = PolicyFactory(name='Policy 1', environment=self.environment)
        policy2 = PolicyFactory(name='Policy 2', environment=self.environment)
        
        self.client.force_login(self.regular_user)
        response = self.client.get(self.policies_url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Policy 1')
        self.assertContains(response, 'Policy 2')
        self.assertTemplateUsed(response, 'policies/list.html')
    
    def test_policies_list_with_filtering(self):
        """Test policies list with environment filtering."""
        env1 = EnvironmentFactory(name='dev')
        env2 = EnvironmentFactory(name='prod')
        
        policy1 = PolicyFactory(name='Dev Policy', environment=env1)
        policy2 = PolicyFactory(name='Prod Policy', environment=env2)
        
        self.client.force_login(self.regular_user)
        
        # Filter by dev environment
        response = self.client.get(self.policies_url, {'environment': env1.id})
        self.assertContains(response, 'Dev Policy')
        self.assertNotContains(response, 'Prod Policy')
        
        # Filter by prod environment
        response = self.client.get(self.policies_url, {'environment': env2.id})
        self.assertContains(response, 'Prod Policy')
        self.assertNotContains(response, 'Dev Policy')
    
    def test_policy_create_view_get(self):
        """Test policy create view GET request."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('policy_create'))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'policies/create.html')
        self.assertIn('form', response.context)
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_policy_create_view_post_success(self, mock_get_client):
        """Test policy create view POST with valid data."""
        mock_client = MockHelper.mock_datahub_client()
        mock_client.create_policy.return_value = {'success': True, 'urn': 'urn:li:dataHubPolicy:new-policy'}
        mock_get_client.return_value = mock_client
        
        self.client.force_login(self.regular_user)
        
        policy_data = {
            "name": "new-policy",
            "description": "A new test policy",
            "type": "METADATA",
            "state": "ACTIVE"
        }
        
        form_data = {
            'name': 'New Test Policy',
            'description': 'A new test policy',
            'policy_json': json.dumps(policy_data),
            'environment': self.environment.id
        }
        
        response = self.client.post(reverse('policy_create'), form_data)
        
        self.assertEqual(response.status_code, 302)  # Redirect after success
        self.assertTrue(Policy.objects.filter(name='New Test Policy').exists())
        
        # Check success message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any('successfully created' in str(message) for message in messages))
    
    def test_policy_create_view_post_invalid_json(self):
        """Test policy create view POST with invalid JSON."""
        self.client.force_login(self.regular_user)
        
        form_data = {
            'name': 'Invalid Policy',
            'description': 'Policy with invalid JSON',
            'policy_json': 'invalid json content',
            'environment': self.environment.id
        }
        
        response = self.client.post(reverse('policy_create'), form_data)
        
        self.assertEqual(response.status_code, 200)  # Should re-render form
        self.assertFalse(Policy.objects.filter(name='Invalid Policy').exists())
        self.assertContains(response, 'Invalid JSON format')
    
    def test_policy_edit_view_get(self):
        """Test policy edit view GET request."""
        policy = PolicyFactory(environment=self.environment)
        
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('policy_edit', kwargs={'pk': policy.pk}))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'policies/edit.html')
        self.assertIn('form', response.context)
        self.assertIn('policy', response.context)
        self.assertEqual(response.context['policy'], policy)
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_policy_edit_view_post_success(self, mock_get_client):
        """Test policy edit view POST with valid data."""
        policy = PolicyFactory(name='Original Policy', environment=self.environment)
        
        mock_client = MockHelper.mock_datahub_client()
        mock_client.update_policy.return_value = {'success': True}
        mock_get_client.return_value = mock_client
        
        self.client.force_login(self.regular_user)
        
        updated_data = {
            "name": "updated-policy",
            "description": "Updated policy description",
            "type": "METADATA",
            "state": "ACTIVE"
        }
        
        form_data = {
            'name': 'Updated Policy',
            'description': 'Updated policy description',
            'policy_json': json.dumps(updated_data),
            'environment': self.environment.id
        }
        
        response = self.client.post(reverse('policy_edit', kwargs={'pk': policy.pk}), form_data)
        
        self.assertEqual(response.status_code, 302)
        
        # Check policy was updated
        policy.refresh_from_db()
        self.assertEqual(policy.name, 'Updated Policy')
        self.assertEqual(policy.description, 'Updated policy description')
    
    def test_policy_delete_view_get(self):
        """Test policy delete view GET request."""
        policy = PolicyFactory(environment=self.environment)
        
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('policy_delete', kwargs={'pk': policy.pk}))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'policies/delete.html')
        self.assertIn('policy', response.context)
        self.assertEqual(response.context['policy'], policy)
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_policy_delete_view_post_success(self, mock_get_client):
        """Test policy delete view POST request."""
        policy = PolicyFactory(environment=self.environment)
        policy_id = policy.id
        
        mock_client = MockHelper.mock_datahub_client()
        mock_client.delete_policy.return_value = {'success': True}
        mock_get_client.return_value = mock_client
        
        self.client.force_login(self.regular_user)
        response = self.client.post(reverse('policy_delete', kwargs={'pk': policy.pk}))
        
        self.assertEqual(response.status_code, 302)
        self.assertFalse(Policy.objects.filter(id=policy_id).exists())
        
        # Check success message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any('successfully deleted' in str(message) for message in messages))


class EnvironmentsViewTestCase(BaseWebUITestCase):
    """Test cases for environments management views."""
    
    def setUp(self):
        super().setUp()
        self.environments_url = reverse('environments')
    
    def test_environments_list_view(self):
        """Test environments list view."""
        env1 = EnvironmentFactory(name='development')
        env2 = EnvironmentFactory(name='production')
        
        self.client.force_login(self.regular_user)
        response = self.client.get(self.environments_url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'development')
        self.assertContains(response, 'production')
        self.assertTemplateUsed(response, 'environments/list.html')
    
    def test_environment_create_view_get(self):
        """Test environment create view GET request."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('environment_create'))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'environments/form.html')
        self.assertIn('form', response.context)
    
    def test_environment_create_view_post_success(self):
        """Test environment create view POST with valid data."""
        self.client.force_login(self.regular_user)
        
        form_data = {
            'name': 'staging',
            'description': 'Staging environment',
            'datahub_host': 'staging.datahub.com',
            'datahub_token': 'staging-token-123',
            'is_default': False
        }
        
        response = self.client.post(reverse('environment_create'), form_data)
        
        self.assertEqual(response.status_code, 302)
        self.assertTrue(Environment.objects.filter(name='staging').exists())
        
        # Check success message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any('successfully created' in str(message) for message in messages))
    
    def test_environment_create_duplicate_name(self):
        """Test environment create with duplicate name."""
        EnvironmentFactory(name='existing-env')
        
        self.client.force_login(self.regular_user)
        
        form_data = {
            'name': 'existing-env',
            'description': 'Duplicate environment',
            'datahub_host': 'test.datahub.com',
            'datahub_token': 'token-123'
        }
        
        response = self.client.post(reverse('environment_create'), form_data)
        
        self.assertEqual(response.status_code, 200)  # Should re-render form
        self.assertEqual(Environment.objects.filter(name='existing-env').count(), 1)
        self.assertContains(response, 'Environment with this name already exists')
    
    def test_environment_edit_view_get(self):
        """Test environment edit view GET request."""
        environment = EnvironmentFactory()
        
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('environment_edit', kwargs={'pk': environment.pk}))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'environments/form.html')
        self.assertIn('form', response.context)
        self.assertIn('environment', response.context)
    
    def test_environment_edit_view_post_success(self):
        """Test environment edit view POST with valid data."""
        environment = EnvironmentFactory(name='old-name')
        
        self.client.force_login(self.regular_user)
        
        form_data = {
            'name': 'new-name',
            'description': 'Updated description',
            'datahub_host': environment.datahub_host,
            'datahub_token': environment.datahub_token,
            'is_default': environment.is_default
        }
        
        response = self.client.post(reverse('environment_edit', kwargs={'pk': environment.pk}), form_data)
        
        self.assertEqual(response.status_code, 302)
        
        environment.refresh_from_db()
        self.assertEqual(environment.name, 'new-name')
        self.assertEqual(environment.description, 'Updated description')
    
    def test_environment_delete_view_get(self):
        """Test environment delete view GET request."""
        environment = EnvironmentFactory()
        
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('environment_delete', kwargs={'pk': environment.pk}))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'environments/delete.html')
        self.assertIn('environment', response.context)
    
    def test_environment_delete_view_post_success(self):
        """Test environment delete view POST request."""
        environment = EnvironmentFactory()
        environment_id = environment.id
        
        self.client.force_login(self.regular_user)
        response = self.client.post(reverse('environment_delete', kwargs={'pk': environment.pk}))
        
        self.assertEqual(response.status_code, 302)
        self.assertFalse(Environment.objects.filter(id=environment_id).exists())


class LogsViewTestCase(BaseWebUITestCase):
    """Test cases for logs management views."""
    
    def setUp(self):
        super().setUp()
        self.logs_url = reverse('logs')
    
    def test_logs_view_basic_rendering(self):
        """Test logs view renders correctly."""
        LogEntryFactory(level='INFO', message='Test info message')
        LogEntryFactory(level='ERROR', message='Test error message')
        
        self.client.force_login(self.regular_user)
        response = self.client.get(self.logs_url)
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'logs.html')
        self.assertContains(response, 'Test info message')
        self.assertContains(response, 'Test error message')
    
    def test_logs_view_level_filtering(self):
        """Test logs view with level filtering."""
        LogEntryFactory(level='INFO', message='Info message')
        LogEntryFactory(level='ERROR', message='Error message')
        LogEntryFactory(level='WARNING', message='Warning message')
        
        self.client.force_login(self.regular_user)
        
        # Filter by ERROR level
        response = self.client.get(self.logs_url, {'level': 'ERROR'})
        self.assertContains(response, 'Error message')
        self.assertNotContains(response, 'Info message')
    
    def test_logs_view_source_filtering(self):
        """Test logs view with source filtering."""
        LogEntryFactory(source='app.views', message='View message')
        LogEntryFactory(source='app.models', message='Model message')
        
        self.client.force_login(self.regular_user)
        
        # Filter by source
        response = self.client.get(self.logs_url, {'source': 'app.views'})
        self.assertContains(response, 'View message')
        self.assertNotContains(response, 'Model message')
    
    def test_logs_view_search_filtering(self):
        """Test logs view with search filtering."""
        LogEntryFactory(message='Database connection established')
        LogEntryFactory(message='User authentication failed')
        
        self.client.force_login(self.regular_user)
        
        # Search for 'database'
        response = self.client.get(self.logs_url, {'search': 'database'})
        self.assertContains(response, 'Database connection established')
        self.assertNotContains(response, 'User authentication failed')
    
    def test_logs_view_pagination(self):
        """Test logs view pagination."""
        # Create more logs than fit on one page
        for i in range(25):
            LogEntryFactory(message=f'Log message {i}')
        
        self.client.force_login(self.regular_user)
        response = self.client.get(self.logs_url)
        
        self.assertEqual(response.status_code, 200)
        self.assertIn('page_obj', response.context)
        self.assertTrue(response.context['page_obj'].has_next())
    
    def test_logs_clear_all(self):
        """Test clearing all log entries."""
        LogEntryFactory(level='INFO', message='Info message')
        LogEntryFactory(level='ERROR', message='Error message')
        
        self.assertEqual(LogEntry.objects.count(), 2)
        
        self.client.force_login(self.admin_user)
        response = self.client.post(reverse('logs_clear'))
        
        self.assertEqual(response.status_code, 302)
        self.assertEqual(LogEntry.objects.count(), 0)
    
    def test_logs_clear_by_level(self):
        """Test clearing log entries by level."""
        LogEntryFactory(level='INFO', message='Info message')
        LogEntryFactory(level='ERROR', message='Error message')
        LogEntryFactory(level='WARNING', message='Warning message')
        
        self.assertEqual(LogEntry.objects.count(), 3)
        
        self.client.force_login(self.admin_user)
        response = self.client.post(reverse('logs_clear'), {'level': 'ERROR'})
        
        self.assertEqual(response.status_code, 302)
        self.assertEqual(LogEntry.objects.count(), 2)
        self.assertFalse(LogEntry.objects.filter(level='ERROR').exists())


class GitHubViewTestCase(BaseWebUITestCase):
    """Test cases for GitHub integration views."""
    
    def setUp(self):
        super().setUp()
        self.github_url = reverse('github_index')
    
    def test_github_index_view(self):
        """Test GitHub index view."""
        self.client.force_login(self.regular_user)
        response = self.client.get(self.github_url)
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'github/index.html')
    
    @patch('web_ui.services.github_service.GitHubService')
    def test_github_view_with_service(self, mock_github_service):
        """Test GitHub view with mocked service."""
        mock_service = MockHelper.mock_github_service()
        mock_github_service.return_value = mock_service
        
        self.client.force_login(self.regular_user)
        response = self.client.get(self.github_url)
        
        self.assertEqual(response.status_code, 200)
        self.assertIn('github_connected', response.context)


class ViewPermissionTestCase(BaseWebUITestCase):
    """Test cases for view permission requirements."""
    
    def test_admin_required_views(self):
        """Test views that require admin permissions."""
        admin_urls = [
            reverse('logs_clear'),
            reverse('environment_create'),
            reverse('policy_create'),
        ]
        
        # Test with regular user (should be forbidden or redirect)
        self.client.force_login(self.regular_user)
        for url in admin_urls:
            response = self.client.get(url)
            self.assertIn(response.status_code, [302, 403])
        
        # Test with admin user (should work)
        self.client.force_login(self.admin_user)
        for url in admin_urls:
            response = self.client.get(url)
            self.assertIn(response.status_code, [200, 302])
    
    def test_authentication_required_views(self):
        """Test views that require authentication."""
        auth_required_urls = [
            reverse('dashboard'),
            reverse('dashboard_data'),
            reverse('policies'),
            reverse('environments'),
            reverse('logs'),
            reverse('github_index'),
        ]
        
        # Test without authentication (should redirect to login)
        for url in auth_required_urls:
            response = self.client.get(url)
            self.assertRedirects(response, f'/accounts/login/?next={url}')
        
        # Test with authentication (should work)
        self.client.force_login(self.regular_user)
        for url in auth_required_urls:
            response = self.client.get(url)
            self.assertEqual(response.status_code, 200)


class ViewErrorHandlingTestCase(BaseWebUITestCase):
    """Test cases for view error handling."""
    
    def test_404_handling(self):
        """Test 404 error handling for non-existent objects."""
        self.client.force_login(self.regular_user)
        
        # Test non-existent policy
        response = self.client.get(reverse('policy_edit', kwargs={'pk': 99999}))
        self.assertEqual(response.status_code, 404)
        
        # Test non-existent environment
        response = self.client.get(reverse('environment_edit', kwargs={'pk': 99999}))
        self.assertEqual(response.status_code, 404)
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_datahub_connection_error_handling(self, mock_get_client):
        """Test handling of DataHub connection errors."""
        mock_get_client.side_effect = Exception("DataHub connection failed")
        
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('dashboard_data'))
        
        self.assertEqual(response.status_code, 200)
        data = self.get_json_response(response)
        self.assertFalse(data['connected'])
        self.assertIn('error', data)
    
    def test_form_validation_error_handling(self):
        """Test handling of form validation errors."""
        self.client.force_login(self.regular_user)
        
        # Submit invalid form data
        form_data = {
            'name': '',  # Required field left empty
            'description': 'Test description'
        }
        
        response = self.client.post(reverse('environment_create'), form_data)
        
        self.assertEqual(response.status_code, 200)  # Should re-render form
        self.assertContains(response, 'This field is required')


class ViewPerformanceTestCase(BaseWebUITestCase):
    """Test cases for view performance."""
    
    @pytest.mark.performance
    def test_dashboard_data_performance(self):
        """Test dashboard data API performance."""
        # Create test data
        for i in range(10):
            EnvironmentFactory()
            PolicyFactory()
            LogEntryFactory()
        
        self.client.force_login(self.regular_user)
        
        # Time the request
        import time
        start_time = time.time()
        response = self.client.get(reverse('dashboard_data'))
        end_time = time.time()
        
        self.assertEqual(response.status_code, 200)
        self.assertLess(end_time - start_time, 2.0)  # Should complete within 2 seconds
    
    @pytest.mark.performance
    def test_policies_list_performance(self):
        """Test policies list view performance with many policies."""
        environment = EnvironmentFactory()
        
        # Create many policies
        for i in range(50):
            PolicyFactory(environment=environment)
        
        self.client.force_login(self.regular_user)
        
        # Time the request
        import time
        start_time = time.time()
        response = self.client.get(reverse('policies'))
        end_time = time.time()
        
        self.assertEqual(response.status_code, 200)
        self.assertLess(end_time - start_time, 3.0)  # Should complete within 3 seconds 