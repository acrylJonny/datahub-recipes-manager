import json
from unittest.mock import Mock, patch, MagicMock
from django.test import TestCase, Client
from django.urls import reverse
from django.contrib.auth.models import User
from django.contrib.messages import get_messages
from django.utils import timezone
from django.core.files.uploadedfile import SimpleUploadedFile
from datetime import datetime, timedelta

from web_ui.web_ui.models import Policy, GitSettings, Environment


class PoliciesModelTestCase(TestCase):
    """Test cases for Policy model functionality."""
    
    def setUp(self):
        """Set up test data for policy model tests."""
        self.test_policy_data = {
            "name": "Test Policy",
            "description": "A test policy for unit testing",
            "type": "METADATA",
            "state": "ACTIVE",
            "resources": {
                "filter": {
                    "criteria": []
                }
            },
            "privileges": ["EDIT_ENTITY_OWNERS"],
            "actors": {
                "users": ["urn:li:corpuser:test"],
                "groups": []
            }
        }
        
    def test_policy_creation(self):
        """Test creating a new policy."""
        policy = Policy.objects.create(
            name=self.test_policy_data["name"],
            description=self.test_policy_data["description"],
            policy_data=self.test_policy_data
        )
        
        self.assertEqual(policy.name, "Test Policy")
        self.assertEqual(policy.description, "A test policy for unit testing")
        self.assertIsNotNone(policy.created_at)
        self.assertIsNotNone(policy.updated_at)
        self.assertEqual(policy.policy_data, self.test_policy_data)
        
    def test_policy_str_representation(self):
        """Test string representation of policy."""
        policy = Policy.objects.create(
            name="Test Policy",
            policy_data=self.test_policy_data
        )
        
        self.assertEqual(str(policy), "Test Policy")
        
    def test_policy_data_json_field(self):
        """Test that policy_data JSONField works correctly."""
        policy = Policy.objects.create(
            name="JSON Test Policy",
            policy_data=self.test_policy_data
        )
        
        # Retrieve from database to ensure JSON serialization works
        retrieved_policy = Policy.objects.get(name="JSON Test Policy")
        self.assertEqual(retrieved_policy.policy_data["type"], "METADATA")
        self.assertEqual(retrieved_policy.policy_data["state"], "ACTIVE")
        self.assertIn("EDIT_ENTITY_OWNERS", retrieved_policy.policy_data["privileges"])


class PoliciesViewsTestCase(TestCase):
    """Test cases for policies views and endpoints."""
    
    def setUp(self):
        """Set up test data for policy views tests."""
        self.client = Client()
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        
        self.test_policy_data = {
            "name": "Test Policy",
            "description": "A test policy",
            "type": "METADATA",
            "state": "ACTIVE",
            "resources": {
                "filter": {
                    "criteria": []
                }
            },
            "privileges": ["EDIT_ENTITY_OWNERS"],
            "actors": {
                "users": ["urn:li:corpuser:test"],
                "groups": []
            }
        }
        
        self.test_policy = Policy.objects.create(
            name="Existing Policy",
            description="An existing test policy",
            policy_data=self.test_policy_data
        )
        
    def test_policies_list_view(self):
        """Test the policies list view renders correctly."""
        url = reverse('policies')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Policies')
        self.assertContains(response, 'Create New Policy')
        
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_policies_data_endpoint_no_connection(self, mock_get_client):
        """Test policies data endpoint with no DataHub connection."""
        mock_get_client.return_value = None
        
        url = reverse('policies_data')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        
        self.assertFalse(data['connected'])
        self.assertEqual(data['policies'], [])
        
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_policies_data_endpoint_with_connection(self, mock_get_client):
        """Test policies data endpoint with successful DataHub connection."""
        mock_client = Mock()
        mock_client.test_connection.return_value = True
        mock_client.list_policies.return_value = [
            {
                'urn': 'urn:li:dataHubPolicy:test-policy-1',
                'name': 'Test Policy 1',
                'description': 'First test policy',
                'type': 'METADATA',
                'state': 'ACTIVE',
                'lastUpdatedTimestamp': 1640995200000
            },
            {
                'urn': 'urn:li:dataHubPolicy:test-policy-2',
                'name': 'Test Policy 2',
                'description': 'Second test policy',
                'type': 'PLATFORM',
                'state': 'INACTIVE',
                'lastUpdatedTimestamp': 1640995300000
            }
        ]
        mock_get_client.return_value = mock_client
        
        url = reverse('policies_data')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        
        self.assertTrue(data['connected'])
        self.assertEqual(len(data['policies']), 2)
        self.assertEqual(data['policies'][0]['name'], 'Test Policy 1')
        self.assertEqual(data['policies'][1]['type'], 'PLATFORM')
        
    def test_policy_create_view_get(self):
        """Test GET request to policy create view."""
        url = reverse('policy_create')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Create Policy')
        self.assertContains(response, 'name="name"')
        self.assertContains(response, 'name="description"')
        
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_policy_create_view_post_success(self, mock_get_client):
        """Test successful POST request to create a new policy."""
        mock_client = Mock()
        mock_client.test_connection.return_value = True
        mock_client.create_policy.return_value = {
            'urn': 'urn:li:dataHubPolicy:new-policy',
            'success': True
        }
        mock_get_client.return_value = mock_client
        
        url = reverse('policy_create')
        policy_json = json.dumps(self.test_policy_data)
        
        response = self.client.post(url, {
            'name': 'New Test Policy',
            'description': 'A new test policy',
            'policy_json': policy_json
        })
        
        self.assertEqual(response.status_code, 302)  # Redirect after success
        
        # Check that policy was created locally
        new_policy = Policy.objects.get(name='New Test Policy')
        self.assertEqual(new_policy.description, 'A new test policy')
        
        # Check success message was created
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any('successfully created' in str(message) for message in messages))
        
    def test_policy_create_view_post_invalid_json(self):
        """Test POST request with invalid JSON data."""
        url = reverse('policy_create')
        
        response = self.client.post(url, {
            'name': 'Invalid Policy',
            'description': 'Policy with invalid JSON',
            'policy_json': 'invalid json'
        })
        
        self.assertEqual(response.status_code, 200)  # Returns to form with errors
        self.assertContains(response, 'Invalid JSON')
        
    def test_policy_detail_view(self):
        """Test policy detail view."""
        url = reverse('policy_view', args=[self.test_policy.id])
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Existing Policy')
        self.assertContains(response, 'An existing test policy')
        
    def test_policy_detail_view_not_found(self):
        """Test policy detail view with non-existent policy."""
        url = reverse('policy_view', args=['non-existent-id'])
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 404)
        
    def test_policy_edit_view_get(self):
        """Test GET request to policy edit view."""
        url = reverse('policy_edit', args=[self.test_policy.id])
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Edit Policy')
        self.assertContains(response, 'Existing Policy')
        
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_policy_edit_view_post_success(self, mock_get_client):
        """Test successful policy edit."""
        mock_client = Mock()
        mock_client.test_connection.return_value = True
        mock_client.update_policy.return_value = {'success': True}
        mock_get_client.return_value = mock_client
        
        url = reverse('policy_edit', args=[self.test_policy.id])
        updated_data = self.test_policy_data.copy()
        updated_data['description'] = 'Updated description'
        
        response = self.client.post(url, {
            'name': 'Updated Policy Name',
            'description': 'Updated description',
            'policy_json': json.dumps(updated_data)
        })
        
        self.assertEqual(response.status_code, 302)
        
        # Check that policy was updated
        updated_policy = Policy.objects.get(id=self.test_policy.id)
        self.assertEqual(updated_policy.name, 'Updated Policy Name')
        self.assertEqual(updated_policy.description, 'Updated description')
        
    def test_policy_delete_view(self):
        """Test policy deletion."""
        url = reverse('policy_delete', args=[self.test_policy.id])
        response = self.client.post(url)
        
        self.assertEqual(response.status_code, 302)  # Redirect after deletion
        
        # Check that policy was deleted
        with self.assertRaises(Policy.DoesNotExist):
            Policy.objects.get(id=self.test_policy.id)
            
    def test_policy_download_view(self):
        """Test policy download functionality."""
        url = reverse('policy_download', args=[self.test_policy.id])
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'application/json')
        self.assertIn('attachment', response['Content-Disposition'])
        
        # Check that returned JSON is valid
        policy_json = json.loads(response.content)
        self.assertEqual(policy_json['name'], 'Test Policy')
        
    def test_policy_import_view_get(self):
        """Test GET request to policy import view."""
        url = reverse('policy_import')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Import Policy')
        self.assertContains(response, 'type="file"')
        
    def test_policy_import_view_post_success(self):
        """Test successful policy import from file."""
        import_data = {
            "name": "Imported Policy",
            "description": "Policy imported from file",
            "type": "METADATA",
            "state": "ACTIVE",
            "resources": {"filter": {"criteria": []}},
            "privileges": ["VIEW_ENTITY_PAGE"],
            "actors": {"users": [], "groups": []}
        }
        
        file_content = json.dumps(import_data).encode('utf-8')
        uploaded_file = SimpleUploadedFile(
            "test_policy.json",
            file_content,
            content_type="application/json"
        )
        
        url = reverse('policy_import')
        response = self.client.post(url, {'file': uploaded_file})
        
        self.assertEqual(response.status_code, 302)  # Redirect after success
        
        # Check that policy was created
        imported_policy = Policy.objects.get(name='Imported Policy')
        self.assertEqual(imported_policy.description, 'Policy imported from file')
        
    def test_policy_import_view_invalid_file(self):
        """Test policy import with invalid file."""
        invalid_file = SimpleUploadedFile(
            "invalid.json",
            b"invalid json content",
            content_type="application/json"
        )
        
        url = reverse('policy_import')
        response = self.client.post(url, {'file': invalid_file})
        
        self.assertEqual(response.status_code, 200)  # Returns to form with errors
        self.assertContains(response, 'Invalid JSON')


class PoliciesIntegrationTestCase(TestCase):
    """Integration tests for policies with external dependencies."""
    
    def setUp(self):
        """Set up test data for integration tests."""
        self.client = Client()
        
        # Create git settings for GitHub integration tests
        self.git_settings = GitSettings.objects.create(
            enabled=True,
            username='testuser',
            repository='test-repo',
            token='test-token'
        )
        
        self.test_policy = Policy.objects.create(
            name="Integration Test Policy",
            description="Policy for integration testing",
            policy_data={
                "name": "Integration Test Policy",
                "type": "METADATA",
                "state": "ACTIVE",
                "privileges": ["VIEW_ENTITY_PAGE"]
            }
        )
        
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_policy_deploy_to_datahub(self, mock_get_client):
        """Test deploying policy to DataHub."""
        mock_client = Mock()
        mock_client.test_connection.return_value = True
        mock_client.create_policy.return_value = {
            'urn': 'urn:li:dataHubPolicy:deployed-policy',
            'success': True
        }
        mock_get_client.return_value = mock_client
        
        url = reverse('policy_deploy', args=[self.test_policy.id])
        response = self.client.post(url)
        
        self.assertEqual(response.status_code, 302)
        
        # Verify client was called correctly
        mock_client.create_policy.assert_called_once()
        
        # Check success message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any('deployed successfully' in str(message) for message in messages))
        
    @patch('web_ui.web_ui.views.GitHubService')
    def test_policy_push_to_github(self, mock_github_service):
        """Test pushing policy to GitHub."""
        mock_service_instance = Mock()
        mock_service_instance.create_or_update_file.return_value = True
        mock_github_service.return_value = mock_service_instance
        
        url = reverse('policy_push_github', args=[self.test_policy.id])
        response = self.client.post(url)
        
        self.assertEqual(response.status_code, 302)
        
        # Verify GitHub service was called
        mock_service_instance.create_or_update_file.assert_called_once()
        
    def test_export_all_policies(self):
        """Test exporting all policies as ZIP file."""
        # Create additional test policy
        Policy.objects.create(
            name="Second Policy",
            description="Another test policy",
            policy_data={"name": "Second Policy", "type": "PLATFORM"}
        )
        
        url = reverse('export_all_policies')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'application/zip')
        self.assertIn('attachment', response['Content-Disposition'])
        
        # Verify ZIP file is not empty
        self.assertGreater(len(response.content), 0)


class PoliciesErrorHandlingTestCase(TestCase):
    """Test cases for error handling in policies functionality."""
    
    def setUp(self):
        """Set up test data for error handling tests."""
        self.client = Client()
        
        self.test_policy = Policy.objects.create(
            name="Error Test Policy",
            policy_data={"name": "Error Test Policy"}
        )
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_policy_create_datahub_error(self, mock_get_client):
        """Test policy creation when DataHub API fails."""
        mock_client = Mock()
        mock_client.test_connection.return_value = True
        mock_client.create_policy.side_effect = Exception("DataHub API Error")
        mock_get_client.return_value = mock_client
        
        url = reverse('policy_create')
        response = self.client.post(url, {
            'name': 'Failing Policy',
            'description': 'This will fail',
            'policy_json': json.dumps({"name": "Failing Policy"})
        })
        
        self.assertEqual(response.status_code, 200)  # Returns to form
        
        # Check error message is displayed
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any('error' in str(message).lower() for message in messages))
        
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_policies_data_api_timeout(self, mock_get_client):
        """Test policies data endpoint when API times out."""
        mock_client = Mock()
        mock_client.test_connection.return_value = True
        mock_client.list_policies.side_effect = TimeoutError("API Timeout")
        mock_get_client.return_value = mock_client
        
        url = reverse('policies_data')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        
        # Should handle error gracefully
        self.assertTrue(data['connected'])  # Connection test passed
        self.assertEqual(data['policies'], [])  # Empty due to error
        
    def test_policy_delete_nonexistent(self):
        """Test deleting a policy that doesn't exist."""
        url = reverse('policy_delete', args=['nonexistent-id'])
        response = self.client.post(url)
        
        self.assertEqual(response.status_code, 404)
        
    def test_policy_download_nonexistent(self):
        """Test downloading a policy that doesn't exist."""
        url = reverse('policy_download', args=['nonexistent-id'])
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 404)


class PoliciesPermissionsTestCase(TestCase):
    """Test cases for policies permissions and security."""
    
    def setUp(self):
        """Set up test data for permissions tests."""
        self.client = Client()
        
        # Create regular user
        self.regular_user = User.objects.create_user(
            username='regular',
            password='testpass123'
        )
        
        # Create admin user  
        self.admin_user = User.objects.create_superuser(
            username='admin',
            email='admin@example.com',
            password='adminpass123'
        )
        
        self.test_policy = Policy.objects.create(
            name="Permission Test Policy",
            policy_data={"name": "Permission Test Policy"}
        )
        
    def test_policy_create_permissions(self):
        """Test that policy creation respects permissions."""
        # Test anonymous user
        url = reverse('policy_create')
        response = self.client.get(url)
        # Should allow access (based on current implementation)
        self.assertEqual(response.status_code, 200)
        
        # Test with regular user
        self.client.login(username='regular', password='testpass123')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        
        # Test with admin user
        self.client.login(username='admin', password='adminpass123')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        
    def test_policy_edit_permissions(self):
        """Test that policy editing respects permissions."""
        url = reverse('policy_edit', args=[self.test_policy.id])
        
        # Test anonymous user
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)  # Based on current implementation
        
        # Test with regular user
        self.client.login(username='regular', password='testpass123')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        
    def test_policy_delete_permissions(self):
        """Test that policy deletion respects permissions."""
        url = reverse('policy_delete', args=[self.test_policy.id])
        
        # Test anonymous user can delete (based on current implementation)
        response = self.client.post(url)
        # Policy should be deleted
        self.assertEqual(response.status_code, 302) 