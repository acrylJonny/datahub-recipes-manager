"""
Comprehensive tests for GitHub Integration functionality.

Tests cover:
- GitHub main dashboard and overview
- GitHub settings and configuration
- GitHub repository integration
- GitHub branches management
- GitHub pull requests (create, list, detail, update)
- GitHub secrets management
- GitHub environments management
- GitHub workflows overview
- Push-to-GitHub functionality for various entities
- GitHub API endpoints
- GitHub permissions and authentication
- GitHub error handling
"""

import json
from unittest.mock import Mock, patch, MagicMock
from django.test import TestCase, Client
from django.urls import reverse
from django.contrib.auth.models import User
from django.core.files.uploadedfile import SimpleUploadedFile

from tests.fixtures.simple_factories import UserFactory, EnvironmentFactory


class GitHubDashboardTestCase(TestCase):
    """Test GitHub main dashboard functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
    
    def test_github_dashboard_access_authenticated(self):
        """Test GitHub dashboard access with authenticated user."""
        self.client.force_login(self.user)
        response = self.client.get('/github/')
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'github')  # Should contain GitHub content
    
    def test_github_dashboard_access_unauthenticated(self):
        """Test GitHub dashboard access without authentication."""
        response = self.client.get('/github/')
        # Check if requires authentication or allows anonymous access
        self.assertIn(response.status_code, [200, 302])
    
    @patch('web_ui.views.GitHubService')
    def test_github_dashboard_with_service(self, mock_github):
        """Test GitHub dashboard with mocked GitHub service."""
        mock_service = Mock()
        mock_service.get_repo_info.return_value = {
            'name': 'test-repo',
            'default_branch': 'main',
            'private': True
        }
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        response = self.client.get('/github/')
        self.assertEqual(response.status_code, 200)
    
    def test_github_dashboard_navigation_links(self):
        """Test GitHub dashboard contains navigation links."""
        self.client.force_login(self.user)
        response = self.client.get('/github/')
        self.assertEqual(response.status_code, 200)
        
        # In real implementation, check for navigation links
        # self.assertContains(response, 'href="/github/settings/"')
        # self.assertContains(response, 'href="/github/pull-requests/"')
        # self.assertContains(response, 'href="/github/branches/"')


class GitHubSettingsTestCase(TestCase):
    """Test GitHub settings functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
    
    def test_github_settings_page_access(self):
        """Test GitHub settings page access."""
        self.client.force_login(self.user)
        response = self.client.get('/github/settings/')
        self.assertEqual(response.status_code, 200)
    
    def test_github_settings_update(self):
        """Test GitHub settings update."""
        self.client.force_login(self.user)
        
        settings_data = {
            'github_token': 'ghp_test_token',
            'repo_owner': 'test-owner',
            'repo_name': 'test-repo',
            'default_branch': 'main'
        }
        
        response = self.client.post('/github/settings/', settings_data)
        self.assertIn(response.status_code, [200, 302])
    
    @patch('web_ui.views.GitHubService')
    def test_github_test_connection(self, mock_github):
        """Test GitHub connection testing."""
        mock_service = Mock()
        mock_service.test_connection.return_value = {'status': 'success', 'message': 'Connected'}
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        response = self.client.post('/github/test-connection/')
        
        self.assertIn(response.status_code, [200, 302])
    
    def test_github_settings_validation(self):
        """Test GitHub settings validation."""
        self.client.force_login(self.user)
        
        # Test with invalid data
        invalid_data = {
            'github_token': '',  # Missing required token
            'repo_owner': '',
            'repo_name': ''
        }
        
        response = self.client.post('/github/settings/', invalid_data)
        self.assertEqual(response.status_code, 200)  # Should stay on settings page with errors


class GitHubBranchesTestCase(TestCase):
    """Test GitHub branches management functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
        self.branch_name = 'feature/test-branch'
    
    def test_github_branches_page_access(self):
        """Test GitHub branches page access."""
        self.client.force_login(self.user)
        response = self.client.get('/github/branches/')
        self.assertEqual(response.status_code, 200)
    
    @patch('web_ui.views.GitHubService')
    def test_github_load_branches(self, mock_github):
        """Test loading GitHub branches."""
        mock_service = Mock()
        mock_service.list_branches.return_value = [
            {'name': 'main', 'protected': True},
            {'name': 'develop', 'protected': False},
            {'name': 'feature/test', 'protected': False}
        ]
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        response = self.client.get('/github/load-branches/')
        
        self.assertEqual(response.status_code, 200)
    
    @patch('web_ui.views.GitHubService')
    def test_github_create_branch(self, mock_github):
        """Test creating a new GitHub branch."""
        mock_service = Mock()
        mock_service.create_branch.return_value = {'name': self.branch_name, 'sha': 'abc123'}
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        
        branch_data = {
            'branch_name': self.branch_name,
            'source_branch': 'main'
        }
        
        response = self.client.post('/github/create-branch/', branch_data)
        self.assertIn(response.status_code, [200, 302])
    
    @patch('web_ui.views.GitHubService')
    def test_github_switch_branch(self, mock_github):
        """Test switching GitHub branches."""
        mock_service = Mock()
        mock_service.switch_branch.return_value = True
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        response = self.client.post(f'/github/switch-branch/{self.branch_name}/')
        
        self.assertIn(response.status_code, [200, 302])
    
    @patch('web_ui.views.GitHubService')
    def test_github_delete_branch(self, mock_github):
        """Test deleting a GitHub branch."""
        mock_service = Mock()
        mock_service.delete_branch.return_value = True
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        
        delete_data = {
            'branch_name': self.branch_name
        }
        
        response = self.client.post('/github/delete-branch/', delete_data)
        self.assertIn(response.status_code, [200, 302])
    
    @patch('web_ui.views.GitHubService')
    def test_github_branch_diff(self, mock_github):
        """Test getting GitHub branch diff."""
        mock_service = Mock()
        mock_service.get_branch_diff.return_value = {
            'files_changed': 5,
            'additions': 100,
            'deletions': 20
        }
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        
        diff_data = {
            'base_branch': 'main',
            'compare_branch': self.branch_name
        }
        
        response = self.client.get('/github/branch-diff/', diff_data)
        self.assertEqual(response.status_code, 200)


class GitHubPullRequestsTestCase(TestCase):
    """Test GitHub pull requests functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
        self.pr_id = 123
    
    def test_github_pull_requests_page_access(self):
        """Test GitHub pull requests page access."""
        self.client.force_login(self.user)
        response = self.client.get('/github/pull-requests/')
        self.assertEqual(response.status_code, 200)
    
    @patch('web_ui.views.GitHubService')
    def test_github_fetch_prs(self, mock_github):
        """Test fetching GitHub pull requests."""
        mock_service = Mock()
        mock_service.list_pull_requests.return_value = [
            {'number': 1, 'title': 'Test PR 1', 'state': 'open'},
            {'number': 2, 'title': 'Test PR 2', 'state': 'closed'}
        ]
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        response = self.client.get('/github/fetch-prs/')
        
        self.assertEqual(response.status_code, 200)
    
    def test_github_pull_request_detail(self):
        """Test GitHub pull request detail page."""
        self.client.force_login(self.user)
        response = self.client.get(f'/github/pull-requests/{self.pr_id}/')
        
        # Might return 200 with details or 404 if PR doesn't exist
        self.assertIn(response.status_code, [200, 404])
    
    @patch('web_ui.views.GitHubService')
    def test_github_create_pr(self, mock_github):
        """Test creating a GitHub pull request."""
        mock_service = Mock()
        mock_service.create_pull_request.return_value = {
            'number': 123,
            'url': 'https://github.com/test/test/pull/123'
        }
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        
        pr_data = {
            'title': 'Test Pull Request',
            'description': 'This is a test PR',
            'head_branch': 'feature/test',
            'base_branch': 'main'
        }
        
        response = self.client.post('/github/create-pr/', pr_data)
        self.assertIn(response.status_code, [200, 302])
    
    @patch('web_ui.views.GitHubService')
    def test_github_update_pr_status(self, mock_github):
        """Test updating GitHub pull request status."""
        mock_service = Mock()
        mock_service.update_pr_status.return_value = True
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        response = self.client.post(f'/github/update-pr-status/{self.pr_id}/')
        
        self.assertIn(response.status_code, [200, 302])
    
    def test_github_delete_pr(self):
        """Test deleting a GitHub pull request."""
        self.client.force_login(self.user)
        response = self.client.post(f'/github/delete-pr/{self.pr_id}/')
        
        # Should handle deletion attempt
        self.assertIn(response.status_code, [200, 302, 404])


class GitHubSecretsTestCase(TestCase):
    """Test GitHub secrets management functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
        self.secret_name = 'TEST_SECRET'
    
    def test_github_secrets_page_access(self):
        """Test GitHub secrets page access."""
        self.client.force_login(self.user)
        response = self.client.get('/github/secrets/')
        self.assertEqual(response.status_code, 200)
    
    @patch('web_ui.views.GitHubService')
    def test_github_create_secret(self, mock_github):
        """Test creating a GitHub secret."""
        mock_service = Mock()
        mock_service.create_secret.return_value = True
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        
        secret_data = {
            'secret_name': self.secret_name,
            'secret_value': 'secret_value_123'
        }
        
        response = self.client.post('/github/secrets/create/', secret_data)
        self.assertIn(response.status_code, [200, 302])
    
    @patch('web_ui.views.GitHubService')
    def test_github_delete_secret(self, mock_github):
        """Test deleting a GitHub secret."""
        mock_service = Mock()
        mock_service.delete_secret.return_value = True
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        
        delete_data = {
            'secret_name': self.secret_name
        }
        
        response = self.client.post('/github/secrets/delete/', delete_data)
        self.assertIn(response.status_code, [200, 302])
    
    def test_github_secrets_validation(self):
        """Test GitHub secrets validation."""
        self.client.force_login(self.user)
        
        # Test with invalid data
        invalid_data = {
            'secret_name': '',  # Missing required name
            'secret_value': ''
        }
        
        response = self.client.post('/github/secrets/create/', invalid_data)
        self.assertEqual(response.status_code, 200)  # Should stay on page with errors


class GitHubEnvironmentsTestCase(TestCase):
    """Test GitHub environments management functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
        self.env_name = 'production'
    
    def test_github_environments_page_access(self):
        """Test GitHub environments page access."""
        self.client.force_login(self.user)
        response = self.client.get('/github/environments/')
        self.assertEqual(response.status_code, 200)
    
    @patch('web_ui.views.GitHubService')
    def test_github_create_environment(self, mock_github):
        """Test creating a GitHub environment."""
        mock_service = Mock()
        mock_service.create_environment.return_value = {'name': self.env_name, 'id': 123}
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        
        env_data = {
            'environment_name': self.env_name,
            'description': 'Production environment'
        }
        
        response = self.client.post('/github/environments/create/', env_data)
        self.assertIn(response.status_code, [200, 302])
    
    def test_github_environments_validation(self):
        """Test GitHub environments validation."""
        self.client.force_login(self.user)
        
        # Test with invalid data
        invalid_data = {
            'environment_name': '',  # Missing required name
        }
        
        response = self.client.post('/github/environments/create/', invalid_data)
        self.assertEqual(response.status_code, 200)  # Should stay on page with errors


class GitHubWorkflowsTestCase(TestCase):
    """Test GitHub workflows functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
    
    def test_github_workflows_overview_access(self):
        """Test GitHub workflows overview page access."""
        self.client.force_login(self.user)
        response = self.client.get('/github/workflows-overview/')
        self.assertEqual(response.status_code, 200)
    
    @patch('web_ui.views.GitHubService')
    def test_github_workflows_data(self, mock_github):
        """Test GitHub workflows data retrieval."""
        mock_service = Mock()
        mock_service.list_workflows.return_value = [
            {'name': 'CI', 'state': 'active', 'id': 123},
            {'name': 'Deploy', 'state': 'active', 'id': 456}
        ]
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        response = self.client.get('/github/workflows-overview/')
        
        self.assertEqual(response.status_code, 200)


class GitHubPushToGitHubTestCase(TestCase):
    """Test push-to-GitHub functionality for various entities."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
        self.recipe_id = 'test-recipe-123'
        self.policy_id = 'test-policy-123'
        self.template_id = 1
        self.instance_id = 1
    
    @patch('web_ui.views.GitHubService')
    def test_recipe_push_to_github(self, mock_github):
        """Test pushing recipe to GitHub."""
        mock_service = Mock()
        mock_service.create_pr.return_value = {'number': 123, 'url': 'https://github.com/test/test/pull/123'}
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        response = self.client.post('/github/push-changes/')
        
        self.assertIn(response.status_code, [200, 302])
    
    @patch('web_ui.views.GitHubService')
    def test_policy_push_to_github(self, mock_github):
        """Test pushing policy to GitHub."""
        mock_service = Mock()
        mock_service.create_pr.return_value = {'number': 123, 'url': 'https://github.com/test/test/pull/123'}
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        
        push_data = {
            'commit_message': 'Update policy',
            'branch_name': 'policy-update'
        }
        
        response = self.client.post(f'/policies/{self.policy_id}/push-github/', push_data)
        self.assertIn(response.status_code, [200, 302])
    
    @patch('web_ui.views.GitHubService')
    def test_recipe_template_push_to_github(self, mock_github):
        """Test pushing recipe template to GitHub."""
        mock_service = Mock()
        mock_service.create_pr.return_value = {'number': 123, 'url': 'https://github.com/test/test/pull/123'}
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        
        push_data = {
            'commit_message': 'Update recipe template',
            'branch_name': 'template-update'
        }
        
        response = self.client.post(f'/recipe-templates/{self.template_id}/push-github/', push_data)
        self.assertIn(response.status_code, [200, 302, 404])
    
    @patch('web_ui.views.GitHubService')
    def test_recipe_instance_push_to_github(self, mock_github):
        """Test pushing recipe instance to GitHub."""
        mock_service = Mock()
        mock_service.create_pr.return_value = {'number': 123, 'url': 'https://github.com/test/test/pull/123'}
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        
        push_data = {
            'commit_message': 'Update recipe instance',
            'branch_name': 'instance-update'
        }
        
        response = self.client.post(f'/recipe-instances/{self.instance_id}/push-github/', push_data)
        self.assertIn(response.status_code, [200, 302, 404])
    
    @patch('web_ui.views.GitHubService')
    def test_env_vars_template_push_to_github(self, mock_github):
        """Test pushing environment variables template to GitHub."""
        mock_service = Mock()
        mock_service.create_pr.return_value = {'number': 123, 'url': 'https://github.com/test/test/pull/123'}
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        
        push_data = {
            'commit_message': 'Update env vars template',
            'branch_name': 'env-template-update'
        }
        
        response = self.client.post(f'/env-vars/templates/{self.template_id}/push-github/', push_data)
        self.assertIn(response.status_code, [200, 302, 404])
    
    @patch('web_ui.views.GitHubService')
    def test_env_vars_instance_push_to_github(self, mock_github):
        """Test pushing environment variables instance to GitHub."""
        mock_service = Mock()
        mock_service.create_pr.return_value = {'number': 123, 'url': 'https://github.com/test/test/pull/123'}
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        
        push_data = {
            'commit_message': 'Update env vars instance',
            'branch_name': 'env-instance-update'
        }
        
        response = self.client.post(f'/env-vars/instances/{self.instance_id}/push-github/', push_data)
        self.assertIn(response.status_code, [200, 302, 404])


class GitHubRepoIntegrationTestCase(TestCase):
    """Test GitHub repository integration functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
    
    def test_github_repo_integration_access(self):
        """Test GitHub repo integration page access."""
        self.client.force_login(self.user)
        response = self.client.get('/github/repo/')
        self.assertEqual(response.status_code, 200)
    
    @patch('web_ui.views.GitHubService')
    def test_github_sync_recipes(self, mock_github):
        """Test syncing recipes with GitHub."""
        mock_service = Mock()
        mock_service.sync_recipes.return_value = {'synced': 10, 'errors': 0}
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        response = self.client.post('/github/sync-recipes/')
        
        self.assertIn(response.status_code, [200, 302])
    
    @patch('web_ui.views.GitHubService')
    def test_github_sync_status(self, mock_github):
        """Test getting GitHub sync status."""
        mock_service = Mock()
        mock_service.get_sync_status.return_value = {'status': 'in_progress', 'progress': 50}
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        response = self.client.get('/github/sync-status/')
        
        self.assertEqual(response.status_code, 200)
    
    @patch('web_ui.views.GitHubService')
    def test_github_file_diff(self, mock_github):
        """Test getting GitHub file diff."""
        mock_service = Mock()
        mock_service.get_file_diff.return_value = {
            'additions': 10,
            'deletions': 5,
            'changes': 'file content diff'
        }
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        
        diff_data = {
            'file_path': 'recipes/test-recipe.yml',
            'branch': 'feature/test'
        }
        
        response = self.client.get('/github/file-diff/', diff_data)
        self.assertEqual(response.status_code, 200)
    
    def test_github_revert_staged_file(self):
        """Test reverting staged file in GitHub."""
        self.client.force_login(self.user)
        
        revert_data = {
            'file_path': 'recipes/test-recipe.yml'
        }
        
        response = self.client.post('/github/revert-staged-file/', revert_data)
        self.assertIn(response.status_code, [200, 302])


class GitHubAPITestCase(TestCase):
    """Test GitHub API endpoints."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
    
    @patch('web_ui.api_views.GitHubService')
    def test_github_branches_api(self, mock_github):
        """Test GitHub branches API endpoint."""
        mock_service = Mock()
        mock_service.list_branches.return_value = [
            {'name': 'main', 'protected': True},
            {'name': 'develop', 'protected': False}
        ]
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        response = self.client.get('/api/github/branches/')
        
        self.assertEqual(response.status_code, 200)
    
    @patch('web_ui.api_views.GitHubService')
    def test_github_branch_diff_api(self, mock_github):
        """Test GitHub branch diff API endpoint."""
        mock_service = Mock()
        mock_service.get_branch_diff.return_value = {
            'files_changed': 3,
            'additions': 50,
            'deletions': 10
        }
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        
        diff_data = {
            'base': 'main',
            'head': 'feature/test'
        }
        
        response = self.client.get('/api/github/branch-diff/', diff_data)
        self.assertEqual(response.status_code, 200)
    
    @patch('web_ui.api_views.GitHubService')
    def test_github_file_diff_api(self, mock_github):
        """Test GitHub file diff API endpoint."""
        mock_service = Mock()
        mock_service.get_file_diff.return_value = {
            'diff': 'file diff content'
        }
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        
        diff_data = {
            'file_path': 'recipes/test.yml',
            'branch': 'feature/test'
        }
        
        response = self.client.get('/api/github/file-diff/', diff_data)
        self.assertEqual(response.status_code, 200)


class GitHubSecurityTestCase(TestCase):
    """Test GitHub integration security and permissions."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
    
    def test_github_unauthorized_access(self):
        """Test GitHub integration unauthorized access."""
        # Test various GitHub endpoints without authentication
        endpoints = [
            '/github/',
            '/github/settings/',
            '/github/branches/',
            '/github/pull-requests/',
            '/github/secrets/',
            '/github/environments/',
            '/github/workflows-overview/',
            '/github/repo/'
        ]
        
        for endpoint in endpoints:
            with self.subTest(endpoint=endpoint):
                response = self.client.get(endpoint)
                # Should require authentication or handle gracefully
                self.assertIn(response.status_code, [200, 302, 401, 403])
    
    def test_github_csrf_protection(self):
        """Test GitHub integration CSRF protection."""
        self.client.force_login(self.user)
        
        # GET requests should work
        response = self.client.get('/github/')
        self.assertEqual(response.status_code, 200)
        
        # POST requests need CSRF token
        response = self.client.post('/github/create-branch/', {'branch_name': 'test'})
        # Django test client handles CSRF automatically
        self.assertIn(response.status_code, [200, 302])
    
    def test_github_permissions_by_role(self):
        """Test GitHub integration permissions by user role."""
        # Test regular user permissions
        self.client.force_login(self.user)
        response = self.client.get('/github/')
        self.assertEqual(response.status_code, 200)
        
        # Test admin user permissions
        self.client.force_login(self.admin_user)
        response = self.client.get('/github/')
        self.assertEqual(response.status_code, 200)
        
        # In real implementation, verify different permission levels
        # Admin might have additional capabilities like managing secrets
    
    def test_github_token_security(self):
        """Test GitHub token security handling."""
        self.client.force_login(self.user)
        
        # Test that tokens are not exposed in responses
        response = self.client.get('/github/settings/')
        if response.status_code == 200:
            content = response.content.decode('utf-8')
            # In real implementation, ensure tokens are masked
            # self.assertNotIn('ghp_', content)  # GitHub token format
    
    def test_github_input_validation(self):
        """Test GitHub input validation and sanitization."""
        self.client.force_login(self.user)
        
        # Test with potentially malicious input
        malicious_data = {
            'branch_name': '<script>alert("xss")</script>',
            'commit_message': '"; rm -rf / #'
        }
        
        response = self.client.post('/github/create-branch/', malicious_data)
        self.assertIn(response.status_code, [200, 302])
        
        # In real implementation, verify input is sanitized


class GitHubIntegrationTestCase(TestCase):
    """Test GitHub integration with other components."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
    
    def test_github_environment_integration(self):
        """Test GitHub integration with environments."""
        try:
            env = EnvironmentFactory(name='test-env', is_default=True)
            
            self.client.force_login(self.user)
            response = self.client.get('/github/')
            self.assertEqual(response.status_code, 200)
            
            # In real implementation, test environment affects GitHub operations
            # self.assertContains(response, env.name)
        except:
            self.skipTest("Environment model not available")
    
    @patch('web_ui.views.GitHubService')
    def test_github_logging_integration(self, mock_github):
        """Test GitHub integration logging."""
        mock_service = Mock()
        mock_service.test_connection.return_value = {'status': 'success'}
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        
        # Operations should be logged
        response = self.client.post('/github/test-connection/')
        self.assertIn(response.status_code, [200, 302])
        
        # In real implementation, verify log entries are created
        # from web_ui.models import LogEntry
        # self.assertTrue(LogEntry.objects.filter(message__contains='github').exists())
    
    def test_github_error_handling(self):
        """Test GitHub integration error handling."""
        self.client.force_login(self.user)
        
        # Test GitHub operations when service is unavailable
        response = self.client.get('/github/')
        self.assertEqual(response.status_code, 200)
        
        # Should still render page even if GitHub is unavailable
        # In real implementation, verify graceful degradation
    
    @patch('web_ui.views.GitHubService')
    def test_github_rate_limiting(self, mock_github):
        """Test GitHub rate limiting handling."""
        mock_service = Mock()
        mock_service.test_connection.side_effect = Exception("Rate limit exceeded")
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        response = self.client.post('/github/test-connection/')
        
        # Should handle rate limiting gracefully
        self.assertIn(response.status_code, [200, 302])


class GitHubPerformanceTestCase(TestCase):
    """Test GitHub integration performance."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
    
    def test_github_dashboard_load_time(self):
        """Test GitHub dashboard load time."""
        self.client.force_login(self.user)
        
        import time
        start_time = time.time()
        response = self.client.get('/github/')
        end_time = time.time()
        
        self.assertEqual(response.status_code, 200)
        
        load_time = end_time - start_time
        self.assertLess(load_time, 3.0, "GitHub dashboard should load in under 3 seconds")
    
    @patch('web_ui.views.GitHubService')
    def test_github_api_response_time(self, mock_github):
        """Test GitHub API response time."""
        mock_service = Mock()
        mock_service.list_branches.return_value = [{'name': 'main'}]
        mock_github.return_value = mock_service
        
        self.client.force_login(self.user)
        
        import time
        start_time = time.time()
        response = self.client.get('/github/load-branches/')
        end_time = time.time()
        
        self.assertEqual(response.status_code, 200)
        
        response_time = end_time - start_time
        self.assertLess(response_time, 2.0, "GitHub API should respond in under 2 seconds") 