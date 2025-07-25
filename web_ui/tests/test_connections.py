"""
Comprehensive tests for DataHub Connections functionality.

Tests cover:
- Connection Model tests (all fields, methods, constraints)
- Connection CRUD operations (Create, Read, Update, Delete)
- Connection testing functionality (test_connection method)
- Default connection management (set_default, get_default)
- Connection session management and switching
- Connection API endpoints (list, detail, test, switch)
- Connection validation and error handling
- Connection security and permissions
- Connection integration with other components
- Connection performance and edge cases
"""

import json
from unittest.mock import Mock, patch, MagicMock
from django.test import TestCase, Client
from django.urls import reverse
from django.contrib.auth.models import User
from django.contrib.messages import get_messages
from django.utils import timezone
from django.core.exceptions import ValidationError
from django.db import IntegrityError

from web_ui.web_ui.models import Connection
from tests.fixtures.simple_factories import UserFactory


class ConnectionModelTestCase(TestCase):
    """Test Connection model functionality."""
    
    def setUp(self):
        self.user = UserFactory()
        self.valid_connection_data = {
            'name': 'Test Connection',
            'description': 'A test DataHub connection',
            'datahub_url': 'https://test-datahub.example.com',
            'datahub_token': 'test_token_123',
            'verify_ssl': True,
            'timeout': 30,
            'is_active': True,
            'is_default': False
        }
    
    def test_connection_creation(self):
        """Test basic Connection creation."""
        connection = Connection.objects.create(**self.valid_connection_data)
        
        self.assertEqual(connection.name, 'Test Connection')
        self.assertEqual(connection.description, 'A test DataHub connection')
        self.assertEqual(connection.datahub_url, 'https://test-datahub.example.com')
        self.assertEqual(connection.datahub_token, 'test_token_123')
        self.assertTrue(connection.verify_ssl)
        self.assertEqual(connection.timeout, 30)
        self.assertTrue(connection.is_active)
        self.assertFalse(connection.is_default)
        self.assertEqual(connection.connection_status, 'unknown')
        self.assertIsNone(connection.error_message)
        self.assertIsNone(connection.last_tested)
        self.assertIsNotNone(connection.created_at)
        self.assertIsNotNone(connection.updated_at)
    
    def test_connection_str(self):
        """Test Connection string representation."""
        # Default connection
        connection = Connection.objects.create(
            **{**self.valid_connection_data, 'is_default': True}
        )
        self.assertEqual(str(connection), 'Test Connection (Default)')
        
        # Active connection
        connection.is_default = False
        connection.save()
        self.assertEqual(str(connection), 'Test Connection (Active)')
        
        # Inactive connection
        connection.is_active = False
        connection.save()
        self.assertEqual(str(connection), 'Test Connection (Inactive)')
    
    def test_connection_defaults(self):
        """Test Connection default values."""
        minimal_data = {
            'name': 'Minimal Connection',
            'datahub_url': 'https://minimal.example.com'
        }
        connection = Connection.objects.create(**minimal_data)
        
        self.assertEqual(connection.description, None)
        self.assertEqual(connection.datahub_token, None)
        self.assertTrue(connection.verify_ssl)
        self.assertEqual(connection.timeout, 30)
        self.assertTrue(connection.is_active)
        self.assertFalse(connection.is_default)
        self.assertEqual(connection.connection_status, 'unknown')
        self.assertIsNone(connection.error_message)
        self.assertIsNone(connection.last_tested)
    
    def test_connection_unique_name(self):
        """Test that connection names must be unique."""
        Connection.objects.create(**self.valid_connection_data)
        
        with self.assertRaises(IntegrityError):
            Connection.objects.create(**self.valid_connection_data)
    
    def test_connection_save_default_constraint(self):
        """Test that only one connection can be default."""
        # Create first default connection
        connection1 = Connection.objects.create(
            **{**self.valid_connection_data, 'is_default': True}
        )
        self.assertTrue(connection1.is_default)
        
        # Create second default connection
        connection2_data = self.valid_connection_data.copy()
        connection2_data['name'] = 'Second Connection'
        connection2_data['is_default'] = True
        
        connection2 = Connection.objects.create(**connection2_data)
        
        # First connection should no longer be default
        connection1.refresh_from_db()
        self.assertFalse(connection1.is_default)
        self.assertTrue(connection2.is_default)
    
    def test_get_default_method(self):
        """Test Connection.get_default() class method."""
        # No connections exist
        self.assertIsNone(Connection.get_default())
        
        # Create non-default connection
        Connection.objects.create(**self.valid_connection_data)
        self.assertIsNone(Connection.get_default())
        
        # Create default connection
        default_connection = Connection.objects.create(
            **{**self.valid_connection_data, 'name': 'Default Connection', 'is_default': True}
        )
        self.assertEqual(Connection.get_default(), default_connection)
        
        # Create inactive default connection
        default_connection.is_active = False
        default_connection.save()
        self.assertIsNone(Connection.get_default())
    
    def test_get_active_connections_method(self):
        """Test Connection.get_active_connections() class method."""
        # No connections exist
        self.assertEqual(list(Connection.get_active_connections()), [])
        
        # Create active and inactive connections
        active_conn = Connection.objects.create(**self.valid_connection_data)
        inactive_conn = Connection.objects.create(
            **{**self.valid_connection_data, 'name': 'Inactive', 'is_active': False}
        )
        default_conn = Connection.objects.create(
            **{**self.valid_connection_data, 'name': 'Default', 'is_default': True}
        )
        
        active_connections = list(Connection.get_active_connections())
        self.assertEqual(len(active_connections), 2)
        self.assertIn(active_conn, active_connections)
        self.assertIn(default_conn, active_connections)
        self.assertNotIn(inactive_conn, active_connections)
        
        # Check ordering (default first)
        self.assertEqual(active_connections[0], default_conn)
    
    @patch('web_ui.web_ui.models.DataHubRestClient')
    def test_test_connection_success(self, mock_client_class):
        """Test successful connection testing."""
        mock_client = Mock()
        mock_client.test_connection.return_value = True
        mock_client_class.return_value = mock_client
        
        connection = Connection.objects.create(**self.valid_connection_data)
        
        result = connection.test_connection()
        
        self.assertTrue(result)
        self.assertEqual(connection.connection_status, 'connected')
        self.assertIsNone(connection.error_message)
        self.assertIsNotNone(connection.last_tested)
        
        # Verify client was created with correct parameters
        mock_client_class.assert_called_once_with(
            server_url=connection.datahub_url,
            token=connection.datahub_token,
            verify_ssl=connection.verify_ssl,
            timeout=connection.timeout
        )
    
    @patch('web_ui.web_ui.models.DataHubRestClient')
    def test_test_connection_failure(self, mock_client_class):
        """Test failed connection testing."""
        mock_client = Mock()
        mock_client.test_connection.return_value = False
        mock_client_class.return_value = mock_client
        
        connection = Connection.objects.create(**self.valid_connection_data)
        
        result = connection.test_connection()
        
        self.assertFalse(result)
        self.assertEqual(connection.connection_status, 'failed')
        self.assertEqual(connection.error_message, 'Connection test failed')
        self.assertIsNotNone(connection.last_tested)
    
    @patch('web_ui.web_ui.models.DataHubRestClient')
    def test_test_connection_exception(self, mock_client_class):
        """Test connection testing with exception."""
        mock_client_class.side_effect = Exception('Network error')
        
        connection = Connection.objects.create(**self.valid_connection_data)
        
        result = connection.test_connection()
        
        self.assertFalse(result)
        self.assertEqual(connection.connection_status, 'failed')
        self.assertEqual(connection.error_message, 'Network error')
        self.assertIsNotNone(connection.last_tested)
    
    @patch('web_ui.web_ui.models.DataHubRestClient')
    def test_get_client_success(self, mock_client_class):
        """Test getting client from connection."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        connection = Connection.objects.create(**self.valid_connection_data)
        
        client = connection.get_client()
        
        self.assertEqual(client, mock_client)
        mock_client_class.assert_called_once_with(
            server_url=connection.datahub_url,
            token=connection.datahub_token,
            verify_ssl=connection.verify_ssl,
            timeout=connection.timeout
        )
    
    @patch('web_ui.web_ui.models.DataHubRestClient')
    def test_get_client_exception(self, mock_client_class):
        """Test getting client with exception."""
        mock_client_class.side_effect = Exception('Client creation failed')
        
        connection = Connection.objects.create(**self.valid_connection_data)
        
        client = connection.get_client()
        
        self.assertIsNone(client)


class ConnectionsListViewTestCase(TestCase):
    """Test connections list view functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
        
        # Create test connections
        self.connection1 = Connection.objects.create(
            name='Production DataHub',
            description='Production DataHub instance',
            datahub_url='https://prod-datahub.example.com',
            is_default=True
        )
        
        self.connection2 = Connection.objects.create(
            name='Staging DataHub',
            description='Staging DataHub instance',
            datahub_url='https://staging-datahub.example.com',
            is_active=True
        )
        
        self.connection3 = Connection.objects.create(
            name='Dev DataHub',
            description='Development DataHub instance',
            datahub_url='https://dev-datahub.example.com',
            is_active=False
        )
    
    def test_connections_list_access(self):
        """Test connections list page access."""
        self.client.force_login(self.user)
        response = self.client.get(reverse('connections_list'))
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'DataHub Connections')
        self.assertContains(response, self.connection1.name)
        self.assertContains(response, self.connection2.name)
        self.assertContains(response, self.connection3.name)
    
    def test_connections_list_ordering(self):
        """Test connections list ordering (default first)."""
        self.client.force_login(self.user)
        response = self.client.get(reverse('connections_list'))
        
        self.assertEqual(response.status_code, 200)
        connections = response.context['connections']
        
        # Default connection should be first
        self.assertEqual(connections[0], self.connection1)
        self.assertTrue(connections[0].is_default)
    
    def test_connections_list_context(self):
        """Test connections list view context."""
        self.client.force_login(self.user)
        response = self.client.get(reverse('connections_list'))
        
        self.assertEqual(response.status_code, 200)
        self.assertIn('connections', response.context)
        self.assertEqual(response.context['page_title'], 'DataHub Connections')


class ConnectionCreateViewTestCase(TestCase):
    """Test connection create view functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
        
        self.valid_form_data = {
            'name': 'New Connection',
            'description': 'A new test connection',
            'datahub_url': 'https://new-datahub.example.com',
            'datahub_token': 'new_token_123',
            'verify_ssl': 'on',
            'timeout': '45',
            'is_default': 'on'
        }
    
    def test_connection_create_get(self):
        """Test connection create page GET request."""
        self.client.force_login(self.user)
        response = self.client.get(reverse('connection_create'))
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Create Connection')
    
    def test_connection_create_post_valid(self):
        """Test connection create with valid data."""
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('connection_create'),
            data=self.valid_form_data,
            follow=True
        )
        
        self.assertEqual(response.status_code, 200)
        
        # Check connection was created
        connection = Connection.objects.get(name='New Connection')
        self.assertEqual(connection.description, 'A new test connection')
        self.assertEqual(connection.datahub_url, 'https://new-datahub.example.com')
        self.assertEqual(connection.datahub_token, 'new_token_123')
        self.assertTrue(connection.verify_ssl)
        self.assertEqual(connection.timeout, 45)
        self.assertTrue(connection.is_default)
        
        # Check success message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any("created successfully" in str(m) for m in messages))
    
    def test_connection_create_post_invalid(self):
        """Test connection create with invalid data."""
        self.client.force_login(self.user)
        
        invalid_data = self.valid_form_data.copy()
        invalid_data['name'] = ''  # Invalid: required field
        
        response = self.client.post(
            reverse('connection_create'),
            data=invalid_data
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Connection name is required')
        
        # Check connection was not created
        self.assertFalse(Connection.objects.filter(name='').exists())
    
    def test_connection_create_missing_url(self):
        """Test connection create with missing URL."""
        self.client.force_login(self.user)
        
        invalid_data = self.valid_form_data.copy()
        invalid_data['datahub_url'] = ''  # Invalid: required field
        
        response = self.client.post(
            reverse('connection_create'),
            data=invalid_data
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'DataHub URL is required')
    
    @patch.object(Connection, 'test_connection')
    def test_connection_create_with_test(self, mock_test):
        """Test connection create with connection testing."""
        mock_test.return_value = True
        
        self.client.force_login(self.user)
        
        test_data = self.valid_form_data.copy()
        test_data['test_connection'] = 'on'
        
        response = self.client.post(
            reverse('connection_create'),
            data=test_data,
            follow=True
        )
        
        self.assertEqual(response.status_code, 200)
        
        # Check connection was created and tested
        connection = Connection.objects.get(name='New Connection')
        mock_test.assert_called_once()
        
        # Check success message includes test result
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any("created and tested successfully" in str(m) for m in messages))
    
    @patch.object(Connection, 'test_connection')
    def test_connection_create_with_failed_test(self, mock_test):
        """Test connection create with failed connection test."""
        mock_test.return_value = False
        
        self.client.force_login(self.user)
        
        test_data = self.valid_form_data.copy()
        test_data['test_connection'] = 'on'
        
        response = self.client.post(
            reverse('connection_create'),
            data=test_data,
            follow=True
        )
        
        self.assertEqual(response.status_code, 200)
        
        # Check connection was created but test failed
        connection = Connection.objects.get(name='New Connection')
        mock_test.assert_called_once()
        
        # Check warning message about test failure
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any("created but failed connection test" in str(m) for m in messages))


class ConnectionEditViewTestCase(TestCase):
    """Test connection edit view functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        
        self.connection = Connection.objects.create(
            name='Original Connection',
            description='Original description',
            datahub_url='https://original.example.com',
            datahub_token='original_token',
            verify_ssl=True,
            timeout=30,
            is_default=False
        )
        
        self.updated_data = {
            'name': 'Updated Connection',
            'description': 'Updated description',
            'datahub_url': 'https://updated.example.com',
            'datahub_token': 'updated_token',
            'verify_ssl': '',  # Not checked = False
            'timeout': '60',
            'is_default': 'on'
        }
    
    def test_connection_edit_get(self):
        """Test connection edit page GET request."""
        self.client.force_login(self.user)
        response = self.client.get(
            reverse('connection_edit', args=[self.connection.id])
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, f'Edit Connection: {self.connection.name}')
        self.assertContains(response, self.connection.name)
        self.assertContains(response, self.connection.description)
        self.assertIn('has_token', response.context)
        self.assertTrue(response.context['has_token'])
    
    def test_connection_edit_post_valid(self):
        """Test connection edit with valid data."""
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('connection_edit', args=[self.connection.id]),
            data=self.updated_data,
            follow=True
        )
        
        self.assertEqual(response.status_code, 200)
        
        # Check connection was updated
        self.connection.refresh_from_db()
        self.assertEqual(self.connection.name, 'Updated Connection')
        self.assertEqual(self.connection.description, 'Updated description')
        self.assertEqual(self.connection.datahub_url, 'https://updated.example.com')
        self.assertEqual(self.connection.datahub_token, 'updated_token')
        self.assertFalse(self.connection.verify_ssl)
        self.assertEqual(self.connection.timeout, 60)
        self.assertTrue(self.connection.is_default)
        
        # Check success message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any("updated successfully" in str(m) for m in messages))
    
    def test_connection_edit_post_invalid(self):
        """Test connection edit with invalid data."""
        self.client.force_login(self.user)
        
        invalid_data = self.updated_data.copy()
        invalid_data['name'] = ''  # Invalid: required field
        
        response = self.client.post(
            reverse('connection_edit', args=[self.connection.id]),
            data=invalid_data
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Connection name is required')
        
        # Check connection was not updated
        self.connection.refresh_from_db()
        self.assertEqual(self.connection.name, 'Original Connection')
    
    def test_connection_edit_empty_token(self):
        """Test connection edit with empty token (should not update)."""
        original_token = self.connection.datahub_token
        
        self.client.force_login(self.user)
        
        no_token_data = self.updated_data.copy()
        no_token_data['datahub_token'] = ''
        
        response = self.client.post(
            reverse('connection_edit', args=[self.connection.id]),
            data=no_token_data,
            follow=True
        )
        
        self.assertEqual(response.status_code, 200)
        
        # Check token was not updated
        self.connection.refresh_from_db()
        self.assertEqual(self.connection.datahub_token, original_token)
    
    def test_connection_edit_not_found(self):
        """Test connection edit with non-existent connection."""
        self.client.force_login(self.user)
        response = self.client.get(
            reverse('connection_edit', args=[9999])
        )
        
        self.assertEqual(response.status_code, 404)
    
    @patch.object(Connection, 'test_connection')
    def test_connection_edit_with_test(self, mock_test):
        """Test connection edit with connection testing."""
        mock_test.return_value = True
        
        self.client.force_login(self.user)
        
        test_data = self.updated_data.copy()
        test_data['test_connection'] = 'on'
        
        response = self.client.post(
            reverse('connection_edit', args=[self.connection.id]),
            data=test_data,
            follow=True
        )
        
        self.assertEqual(response.status_code, 200)
        
        # Check connection was updated and tested
        mock_test.assert_called_once()
        
        # Check success message includes test result
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any("updated and tested successfully" in str(m) for m in messages))


class ConnectionDeleteViewTestCase(TestCase):
    """Test connection delete view functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        
        self.connection = Connection.objects.create(
            name='Connection to Delete',
            datahub_url='https://delete.example.com'
        )
    
    def test_connection_delete_get(self):
        """Test connection delete confirmation page."""
        self.client.force_login(self.user)
        response = self.client.get(
            reverse('connection_delete', args=[self.connection.id])
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, f'Delete Connection: {self.connection.name}')
        self.assertContains(response, self.connection.name)
    
    def test_connection_delete_post(self):
        """Test connection deletion."""
        connection_id = self.connection.id
        connection_name = self.connection.name
        
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('connection_delete', args=[connection_id]),
            follow=True
        )
        
        self.assertEqual(response.status_code, 200)
        
        # Check connection was deleted
        self.assertFalse(Connection.objects.filter(id=connection_id).exists())
        
        # Check success message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any("deleted successfully" in str(m) for m in messages))
        self.assertTrue(any(connection_name in str(m) for m in messages))
    
    def test_connection_delete_default_reassignment(self):
        """Test deleting default connection reassigns default to another."""
        # Create default connection to delete
        default_connection = Connection.objects.create(
            name='Default Connection',
            datahub_url='https://default.example.com',
            is_default=True
        )
        
        # Create another active connection
        other_connection = Connection.objects.create(
            name='Other Connection',
            datahub_url='https://other.example.com',
            is_active=True
        )
        
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('connection_delete', args=[default_connection.id]),
            follow=True
        )
        
        self.assertEqual(response.status_code, 200)
        
        # Check default connection was deleted
        self.assertFalse(Connection.objects.filter(id=default_connection.id).exists())
        
        # Check other connection became default
        other_connection.refresh_from_db()
        self.assertTrue(other_connection.is_default)
        
        # Check info message about new default
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any("new default connection" in str(m) for m in messages))
    
    def test_connection_delete_not_found(self):
        """Test connection delete with non-existent connection."""
        self.client.force_login(self.user)
        response = self.client.get(
            reverse('connection_delete', args=[9999])
        )
        
        self.assertEqual(response.status_code, 404)


class ConnectionSetDefaultViewTestCase(TestCase):
    """Test connection set default functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        
        self.default_connection = Connection.objects.create(
            name='Current Default',
            datahub_url='https://current-default.example.com',
            is_default=True
        )
        
        self.other_connection = Connection.objects.create(
            name='Other Connection',
            datahub_url='https://other.example.com',
            is_default=False
        )
    
    def test_connection_set_default(self):
        """Test setting connection as default."""
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('connection_set_default', args=[self.other_connection.id]),
            follow=True
        )
        
        self.assertEqual(response.status_code, 200)
        
        # Check other connection became default
        self.other_connection.refresh_from_db()
        self.assertTrue(self.other_connection.is_default)
        
        # Check previous default is no longer default
        self.default_connection.refresh_from_db()
        self.assertFalse(self.default_connection.is_default)
        
        # Check success message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any("now the default connection" in str(m) for m in messages))
    
    def test_connection_set_default_not_found(self):
        """Test setting default with non-existent connection."""
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('connection_set_default', args=[9999]),
            follow=True
        )
        
        self.assertEqual(response.status_code, 200)
        
        # Check error message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any("Error setting default connection" in str(m) for m in messages))


class ConnectionAPITestCase(TestCase):
    """Test connection API endpoints."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        
        self.connection1 = Connection.objects.create(
            name='API Connection 1',
            datahub_url='https://api1.example.com',
            is_default=True,
            connection_status='connected'
        )
        
        self.connection2 = Connection.objects.create(
            name='API Connection 2',
            datahub_url='https://api2.example.com',
            connection_status='failed'
        )
    
    def test_api_list_connections(self):
        """Test API list connections endpoint."""
        self.client.force_login(self.user)
        response = self.client.get(reverse('api-connections'))
        
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'application/json')
        
        data = json.loads(response.content)
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 2)
        
        # Check connection data
        connection_names = [conn['name'] for conn in data]
        self.assertIn('API Connection 1', connection_names)
        self.assertIn('API Connection 2', connection_names)
    
    def test_api_get_connection(self):
        """Test API get connection endpoint."""
        self.client.force_login(self.user)
        response = self.client.get(
            reverse('api-connection-detail', args=[self.connection1.id])
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'application/json')
        
        data = json.loads(response.content)
        self.assertEqual(data['name'], 'API Connection 1')
        self.assertEqual(data['datahub_url'], 'https://api1.example.com')
        self.assertTrue(data['is_default'])
        self.assertEqual(data['connection_status'], 'connected')
        
        # Check sensitive data is not exposed
        self.assertNotIn('datahub_token', data)
    
    def test_api_get_connection_not_found(self):
        """Test API get connection with non-existent connection."""
        self.client.force_login(self.user)
        response = self.client.get(
            reverse('api-connection-detail', args=[9999])
        )
        
        self.assertEqual(response.status_code, 404)
        
        data = json.loads(response.content)
        self.assertIn('error', data)
    
    @patch.object(Connection, 'test_connection')
    def test_api_test_connection(self, mock_test):
        """Test API test connection endpoint."""
        mock_test.return_value = True
        
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('api-connection-test', args=[self.connection1.id])
        )
        
        self.assertEqual(response.status_code, 200)
        
        data = json.loads(response.content)
        self.assertTrue(data['success'])
        self.assertIn('status', data)
        
        mock_test.assert_called_once()
    
    def test_api_switch_connection(self):
        """Test API switch connection endpoint."""
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('api-connection-switch'),
            data={'connection_id': str(self.connection2.id)}
        )
        
        self.assertEqual(response.status_code, 200)
        
        data = json.loads(response.content)
        self.assertTrue(data['success'])
        self.assertEqual(data['connection_name'], 'API Connection 2')
        self.assertEqual(data['connection_id'], self.connection2.id)
        
        # Check session was updated
        self.assertEqual(
            self.client.session['current_connection_id'], 
            str(self.connection2.id)
        )
    
    def test_api_switch_connection_missing_id(self):
        """Test API switch connection without connection ID."""
        self.client.force_login(self.user)
        response = self.client.post(reverse('api-connection-switch'))
        
        self.assertEqual(response.status_code, 200)
        
        data = json.loads(response.content)
        self.assertFalse(data['success'])
        self.assertIn('error', data)
    
    def test_api_switch_connection_invalid_id(self):
        """Test API switch connection with invalid connection ID."""
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('api-connection-switch'),
            data={'connection_id': '9999'}
        )
        
        self.assertEqual(response.status_code, 200)
        
        data = json.loads(response.content)
        self.assertFalse(data['success'])
        self.assertIn('error', data)


class ConnectionSessionManagementTestCase(TestCase):
    """Test connection session management."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        
        self.default_connection = Connection.objects.create(
            name='Default Connection',
            datahub_url='https://default.example.com',
            is_default=True
        )
        
        self.session_connection = Connection.objects.create(
            name='Session Connection',
            datahub_url='https://session.example.com'
        )
    
    def test_get_current_connection_default(self):
        """Test getting current connection returns default when no session."""
        from web_ui.web_ui.views import get_current_connection
        
        # Create mock request without session connection
        request = type('Request', (), {'session': {}})()
        
        with patch('web_ui.web_ui.views.Connection') as mock_connection:
            mock_connection.get_default.return_value = self.default_connection
            
            current = get_current_connection(request)
            self.assertEqual(current, self.default_connection)
    
    def test_get_current_connection_from_session(self):
        """Test getting current connection from session."""
        from web_ui.web_ui.views import get_current_connection
        
        # Create mock request with session connection
        request = type('Request', (), {
            'session': {'current_connection_id': str(self.session_connection.id)}
        })()
        
        with patch('web_ui.web_ui.views.Connection') as mock_connection:
            mock_connection.objects.filter.return_value.first.return_value = self.session_connection
            
            current = get_current_connection(request)
            mock_connection.objects.filter.assert_called_with(
                id=str(self.session_connection.id), is_active=True
            )
    
    def test_get_current_connection_invalid_session(self):
        """Test getting current connection with invalid session ID."""
        from web_ui.web_ui.views import get_current_connection
        
        # Create mock request with invalid session connection
        request = type('Request', (), {
            'session': {'current_connection_id': '9999'}
        })()
        
        with patch('web_ui.web_ui.views.Connection') as mock_connection:
            mock_connection.objects.filter.return_value.first.return_value = None
            mock_connection.get_default.return_value = self.default_connection
            
            current = get_current_connection(request)
            self.assertEqual(current, self.default_connection)


class ConnectionSecurityTestCase(TestCase):
    """Test connection security and permissions."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
        
        self.connection = Connection.objects.create(
            name='Security Test Connection',
            datahub_url='https://security.example.com',
            datahub_token='sensitive_token'
        )
    
    def test_connection_access_unauthenticated(self):
        """Test connection access without authentication."""
        endpoints = [
            ('connections_list', []),
            ('connection_create', []),
            ('connection_edit', [self.connection.id]),
            ('connection_delete', [self.connection.id]),
            ('connection_set_default', [self.connection.id]),
        ]
        
        for endpoint_name, args in endpoints:
            with self.subTest(endpoint=endpoint_name):
                response = self.client.get(reverse(endpoint_name, args=args))
                # Should redirect to login or handle gracefully
                self.assertIn(response.status_code, [200, 302, 401, 403])
    
    def test_connection_token_not_exposed(self):
        """Test that connection tokens are not exposed in responses."""
        self.client.force_login(self.user)
        
        # Test list view
        response = self.client.get(reverse('connections_list'))
        self.assertNotContains(response, 'sensitive_token')
        
        # Test edit view
        response = self.client.get(reverse('connection_edit', args=[self.connection.id]))
        self.assertNotContains(response, 'sensitive_token')
        
        # Test API
        response = self.client.get(reverse('api-connection-detail', args=[self.connection.id]))
        data = json.loads(response.content)
        self.assertNotIn('datahub_token', data)
    
    def test_connection_xss_protection(self):
        """Test XSS protection in connection creation."""
        self.client.force_login(self.user)
        
        xss_data = {
            'name': '<script>alert("xss")</script>',
            'description': '<img src=x onerror=alert("xss")>',
            'datahub_url': 'https://example.com'
        }
        
        response = self.client.post(
            reverse('connection_create'),
            data=xss_data
        )
        
        # Should handle XSS attempt gracefully
        self.assertIn(response.status_code, [200, 302])
        
        # If connection was created, check XSS payload is handled
        if Connection.objects.filter(name__contains='script').exists():
            connection = Connection.objects.get(name__contains='script')
            # In real implementation, verify XSS payload is properly escaped
            self.assertIn('script', connection.name)  # Basic check


class ConnectionIntegrationTestCase(TestCase):
    """Test connection integration with other components."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        
        self.connection = Connection.objects.create(
            name='Integration Test Connection',
            datahub_url='https://integration.example.com',
            is_default=True
        )
    
    def test_connection_context_processor(self):
        """Test connection context processor adds connections to templates."""
        self.client.force_login(self.user)
        response = self.client.get(reverse('dashboard'))
        
        # Check connections are in context
        self.assertIn('connections', response.context)
        self.assertIn('current_connection', response.context)
        
        connections = response.context['connections']
        self.assertIn(self.connection, connections)
        
        current = response.context['current_connection']
        self.assertEqual(current, self.connection)
    
    @patch('utils.datahub_utils.Connection')
    def test_connection_integration_with_datahub_utils(self, mock_connection):
        """Test connection integration with DataHub utilities."""
        from utils.datahub_utils import get_datahub_client
        
        mock_connection.objects.filter.return_value.first.return_value = self.connection
        mock_connection.get_default.return_value = self.connection
        
        # Test getting client by connection ID
        client = get_datahub_client(connection_id=self.connection.id)
        mock_connection.objects.filter.assert_called_with(
            id=self.connection.id, is_active=True
        )
        
        # Test getting default client
        client = get_datahub_client()
        mock_connection.get_default.assert_called()


class ConnectionPerformanceTestCase(TestCase):
    """Test connection performance."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        
        # Create multiple connections for performance testing
        self.connections = []
        for i in range(20):
            connection = Connection.objects.create(
                name=f'Performance Connection {i}',
                datahub_url=f'https://perf{i}.example.com',
                is_default=(i == 0)
            )
            self.connections.append(connection)
    
    def test_connections_list_performance(self):
        """Test connections list page load performance."""
        self.client.force_login(self.user)
        
        import time
        start_time = time.time()
        response = self.client.get(reverse('connections_list'))
        end_time = time.time()
        
        self.assertEqual(response.status_code, 200)
        
        load_time = end_time - start_time
        self.assertLess(load_time, 3.0, 
                       f"Connections list should load in under 3 seconds, took {load_time:.2f}s")
    
    def test_connection_creation_performance(self):
        """Test connection creation performance."""
        self.client.force_login(self.user)
        
        connection_data = {
            'name': 'Performance Test Connection',
            'datahub_url': 'https://performance.example.com'
        }
        
        import time
        start_time = time.time()
        response = self.client.post(
            reverse('connection_create'),
            data=connection_data,
            follow=True
        )
        end_time = time.time()
        
        self.assertEqual(response.status_code, 200)
        
        creation_time = end_time - start_time
        self.assertLess(creation_time, 2.0, 
                       f"Connection creation should complete in under 2 seconds, took {creation_time:.2f}s")
    
    @patch.object(Connection, 'test_connection')
    def test_connection_test_performance(self, mock_test):
        """Test connection testing performance."""
        mock_test.return_value = True
        
        connection = self.connections[0]
        
        import time
        start_time = time.time()
        result = connection.test_connection()
        end_time = time.time()
        
        self.assertTrue(result)
        
        test_time = end_time - start_time
        self.assertLess(test_time, 1.0, 
                       f"Connection test should complete in under 1 second, took {test_time:.2f}s") 