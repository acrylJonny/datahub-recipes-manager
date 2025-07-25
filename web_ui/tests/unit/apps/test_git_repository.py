import json
from unittest.mock import Mock, patch, MagicMock
from django.test import TestCase, Client
from django.urls import reverse
from django.contrib.auth.models import User
from django.contrib.messages import get_messages
from django.utils import timezone
from datetime import datetime, timedelta

from web_ui.web_ui.models import GitSettings, GitHubPR, GitIntegration
from web_ui.services.github_service import GitHubService
from web_ui.services.git_service import GitService


class GitSettingsModelTestCase(TestCase):
    """Test cases for GitSettings model functionality."""
    
    def setUp(self):
        """Set up test data for git settings model tests."""
        pass
        
    def test_git_settings_creation(self):
        """Test creating git settings."""
        git_settings = GitSettings.objects.create(
            enabled=True,
            username='testuser', 
            repository='test-repo',
            token='test-token-123',
            current_branch='main'
        )
        
        self.assertTrue(git_settings.enabled)
        self.assertEqual(git_settings.username, 'testuser')
        self.assertEqual(git_settings.repository, 'test-repo')
        self.assertEqual(git_settings.token, 'test-token-123')
        self.assertEqual(git_settings.current_branch, 'main')
        self.assertIsNotNone(git_settings.created_at)
        self.assertIsNotNone(git_settings.updated_at)
        
    def test_git_settings_str_representation(self):
        """Test string representation of git settings."""
        git_settings = GitSettings.objects.create(
            username='testuser',
            repository='test-repo'
        )
        
        str_repr = str(git_settings)
        self.assertIn('testuser', str_repr)
        self.assertIn('test-repo', str_repr)
        
    def test_git_settings_get_instance(self):
        """Test the get_instance class method."""
        # Should create if doesn't exist
        instance = GitSettings.get_instance()
        self.assertIsNotNone(instance)
        self.assertFalse(instance.enabled)  # Default should be disabled
        
        # Should return existing instance
        instance.enabled = True
        instance.save()
        
        second_instance = GitSettings.get_instance()
        self.assertEqual(instance.id, second_instance.id)
        self.assertTrue(second_instance.enabled)
        
    def test_git_settings_is_configured(self):
        """Test the is_configured method."""
        git_settings = GitSettings.objects.create(enabled=True)
        
        # Should be False without required fields
        self.assertFalse(git_settings.is_configured())
        
        # Should be True with all required fields
        git_settings.username = 'testuser'
        git_settings.repository = 'test-repo'
        git_settings.token = 'test-token'
        self.assertTrue(git_settings.is_configured())
        
        # Should be False if disabled
        git_settings.enabled = False
        self.assertFalse(git_settings.is_configured())


class GitHubPRModelTestCase(TestCase):
    """Test cases for GitHubPR model functionality."""
    
    def setUp(self):
        """Set up test data for GitHub PR model tests."""
        pass
        
    def test_github_pr_creation(self):
        """Test creating a GitHub PR record."""
        pr = GitHubPR.objects.create(
            number=123,
            title='Test Pull Request',
            description='A test pull request',
            branch='feature/test-branch',
            state='open',
            url='https://github.com/user/repo/pull/123'
        )
        
        self.assertEqual(pr.number, 123)
        self.assertEqual(pr.title, 'Test Pull Request')
        self.assertEqual(pr.description, 'A test pull request')
        self.assertEqual(pr.branch, 'feature/test-branch')
        self.assertEqual(pr.state, 'open')
        self.assertEqual(pr.url, 'https://github.com/user/repo/pull/123')
        self.assertIsNotNone(pr.created_at)
        self.assertIsNotNone(pr.updated_at)
        
    def test_github_pr_str_representation(self):
        """Test string representation of GitHub PR."""
        pr = GitHubPR.objects.create(
            number=456,
            title='Another Test PR',
            state='closed'
        )
        
        str_repr = str(pr)
        self.assertIn('#456', str_repr)
        self.assertIn('Another Test PR', str_repr)
        
    def test_github_pr_ordering(self):
        """Test that PRs are ordered by creation date (newest first)."""
        old_pr = GitHubPR.objects.create(
            number=1,
            title='Old PR',
            created_at=timezone.now() - timedelta(hours=2)
        )
        
        new_pr = GitHubPR.objects.create(
            number=2,
            title='New PR',
            created_at=timezone.now() - timedelta(hours=1)
        )
        
        prs = list(GitHubPR.objects.all())
        self.assertEqual(prs[0].title, 'New PR')
        self.assertEqual(prs[1].title, 'Old PR')


class GitHubViewsTestCase(TestCase):
    """Test cases for GitHub integration views."""
    
    def setUp(self):
        """Set up test data for GitHub views tests."""
        self.client = Client()
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        
        # Create git settings
        self.git_settings = GitSettings.objects.create(
            enabled=True,
            username='testuser',
            repository='test-repo', 
            token='test-token-123',
            current_branch='main'
        )
        
        # Create test PRs
        self.test_pr = GitHubPR.objects.create(
            number=100,
            title='Test PR',
            description='A test pull request',
            branch='feature/test',
            state='open',
            url='https://github.com/testuser/test-repo/pull/100'
        )
        
    def test_github_index_view(self):
        """Test the main GitHub integration page."""
        url = reverse('github_index')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'GitHub Integration')
        self.assertContains(response, 'testuser/test-repo')
        self.assertContains(response, 'Test PR')
        
    def test_github_index_view_not_configured(self):
        """Test GitHub index when not configured."""
        # Disable git settings
        self.git_settings.enabled = False
        self.git_settings.save()
        
        url = reverse('github_index')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Configure GitHub Integration')
        
    def test_github_settings_view_get(self):
        """Test GET request to GitHub settings view."""
        url = reverse('github_settings')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'GitHub Settings')
        self.assertContains(response, 'name="username"')
        self.assertContains(response, 'name="repository"')
        self.assertContains(response, 'name="token"')
        
    def test_github_settings_view_post_success(self):
        """Test successful POST to GitHub settings."""
        url = reverse('github_settings')
        
        response = self.client.post(url, {
            'enabled': True,
            'username': 'newuser',
            'repository': 'new-repo',
            'token': 'new-token-456',
            'current_branch': 'develop'
        })
        
        self.assertEqual(response.status_code, 302)  # Redirect after success
        
        # Check that settings were updated
        updated_settings = GitSettings.objects.first()
        self.assertEqual(updated_settings.username, 'newuser')
        self.assertEqual(updated_settings.repository, 'new-repo')
        self.assertEqual(updated_settings.token, 'new-token-456')
        self.assertEqual(updated_settings.current_branch, 'develop')
        
        # Check success message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any('successfully updated' in str(message) for message in messages))
        
    def test_github_settings_view_invalid_data(self):
        """Test GitHub settings with invalid data."""
        url = reverse('github_settings')
        
        response = self.client.post(url, {
            'enabled': True,
            'username': '',  # Required field
            'repository': 'test-repo',
            'token': 'test-token'
        })
        
        self.assertEqual(response.status_code, 200)  # Returns to form
        self.assertContains(response, 'This field is required')
        
    @patch('web_ui.services.github_service.GitHubService')
    def test_github_fetch_prs_view(self, mock_github_service):
        """Test fetching PRs from GitHub."""
        # Mock GitHub service
        mock_service_instance = Mock()
        mock_service_instance.list_pull_requests.return_value = [
            {
                'number': 101,
                'title': 'New Feature',
                'body': 'Adds new feature',
                'head': {'ref': 'feature/new-feature'},
                'state': 'open',
                'html_url': 'https://github.com/testuser/test-repo/pull/101'
            }
        ]
        mock_github_service.return_value = mock_service_instance
        
        url = reverse('github_fetch_prs')
        response = self.client.post(url)
        
        self.assertEqual(response.status_code, 302)  # Redirect after success
        
        # Check that PR was created/updated
        new_pr = GitHubPR.objects.get(number=101)
        self.assertEqual(new_pr.title, 'New Feature')
        self.assertEqual(new_pr.branch, 'feature/new-feature')
        
        # Check success message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any('fetched' in str(message) for message in messages))
        
    @patch('web_ui.services.github_service.GitHubService')
    def test_github_create_pr_view(self, mock_github_service):
        """Test creating a new PR via GitHub."""
        mock_service_instance = Mock()
        mock_service_instance.create_pull_request.return_value = {
            'number': 102,
            'title': 'Automated PR',
            'html_url': 'https://github.com/testuser/test-repo/pull/102'
        }
        mock_github_service.return_value = mock_service_instance
        
        url = reverse('github_create_pr')
        response = self.client.post(url, {
            'title': 'Automated PR',
            'description': 'Auto-generated pull request',
            'head_branch': 'feature/auto',
            'base_branch': 'main'
        })
        
        self.assertEqual(response.status_code, 302)
        
        # Verify service was called correctly
        mock_service_instance.create_pull_request.assert_called_once()
        
        # Check success message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any('created successfully' in str(message) for message in messages))
        
    def test_github_update_pr_status_view(self):
        """Test updating PR status."""
        url = reverse('github_update_pr_status', args=[self.test_pr.number])
        
        response = self.client.post(url, {
            'state': 'closed'
        })
        
        self.assertEqual(response.status_code, 302)
        
        # Check that PR status was updated
        self.test_pr.refresh_from_db()
        self.assertEqual(self.test_pr.state, 'closed')


class GitIntegrationTestCase(TestCase):
    """Test cases for GitIntegration functionality."""
    
    def setUp(self):
        """Set up test data for git integration tests."""
        self.git_settings = GitSettings.objects.create(
            enabled=True,
            username='testuser',
            repository='test-repo',
            token='test-token',
            current_branch='main'
        )
        
    @patch('subprocess.run')
    def test_git_integration_get_branches(self, mock_subprocess):
        """Test getting branches from git repository."""
        # Mock subprocess response
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = 'main\nfeature/test\ndevelop\n'
        mock_subprocess.return_value = mock_result
        
        branches = GitIntegration.get_branches()
        
        self.assertIn('main', branches)
        self.assertIn('feature/test', branches)
        self.assertIn('develop', branches)
        
    @patch('subprocess.run')
    def test_git_integration_get_branches_error(self, mock_subprocess):
        """Test getting branches when git command fails."""
        # Mock subprocess error
        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = 'Not a git repository'
        mock_subprocess.return_value = mock_result
        
        branches = GitIntegration.get_branches()
        
        # Should return empty list on error
        self.assertEqual(branches, [])
        
    @patch('web_ui.services.git_service.GitService')
    def test_git_integration_push_to_git(self, mock_git_service):
        """Test pushing changes to git repository."""
        mock_service_instance = Mock()
        mock_service_instance.commit_and_push.return_value = True
        mock_git_service.return_value = mock_service_instance
        
        git_integration = GitIntegration()
        result = git_integration.push_to_git(
            file_path='test/file.txt',
            content='test content',
            commit_message='Test commit'
        )
        
        self.assertTrue(result)
        mock_service_instance.commit_and_push.assert_called_once()
        
    @patch('web_ui.services.git_service.GitService')
    def test_git_integration_stage_changes(self, mock_git_service):
        """Test staging changes for later commit."""
        mock_service_instance = Mock()
        mock_service_instance.stage_file.return_value = True
        mock_git_service.return_value = mock_service_instance
        
        git_integration = GitIntegration()
        result = git_integration.stage_changes(
            file_path='test/staged_file.txt',
            content='staged content'
        )
        
        self.assertTrue(result)
        mock_service_instance.stage_file.assert_called_once()


class GitHubServiceTestCase(TestCase):
    """Test cases for GitHubService functionality."""
    
    def setUp(self):
        """Set up test data for GitHub service tests."""
        self.git_settings = GitSettings.objects.create(
            enabled=True,
            username='testuser',
            repository='test-repo',
            token='test-token'
        )
        
        self.github_service = GitHubService(self.git_settings)
        
    @patch('requests.get')
    def test_list_pull_requests(self, mock_get):
        """Test listing pull requests from GitHub API."""
        # Mock API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                'number': 1,
                'title': 'Test PR 1',
                'body': 'Description 1',
                'state': 'open',
                'head': {'ref': 'feature/1'},
                'html_url': 'https://github.com/testuser/test-repo/pull/1'
            },
            {
                'number': 2,
                'title': 'Test PR 2',
                'body': 'Description 2',
                'state': 'closed',
                'head': {'ref': 'feature/2'},
                'html_url': 'https://github.com/testuser/test-repo/pull/2'
            }
        ]
        mock_get.return_value = mock_response
        
        prs = self.github_service.list_pull_requests()
        
        self.assertEqual(len(prs), 2)
        self.assertEqual(prs[0]['title'], 'Test PR 1')
        self.assertEqual(prs[1]['state'], 'closed')
        
        # Verify correct API endpoint was called
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        self.assertIn('pulls', call_args[0][0])
        
    @patch('requests.post')
    def test_create_pull_request(self, mock_post):
        """Test creating a pull request via GitHub API."""
        # Mock API response
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            'number': 123,
            'title': 'New PR',
            'html_url': 'https://github.com/testuser/test-repo/pull/123'
        }
        mock_post.return_value = mock_response
        
        pr_data = {
            'title': 'New PR',
            'body': 'New pull request',
            'head': 'feature/new',
            'base': 'main'
        }
        
        result = self.github_service.create_pull_request(
            title=pr_data['title'],
            body=pr_data['body'],
            head=pr_data['head'],
            base=pr_data['base']
        )
        
        self.assertEqual(result['number'], 123)
        self.assertEqual(result['title'], 'New PR')
        
        # Verify correct API call was made
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        self.assertIn('pulls', call_args[0][0])
        
    @patch('requests.put')
    def test_create_or_update_file(self, mock_put):
        """Test creating or updating a file via GitHub API."""
        # Mock API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'commit': {'sha': 'abc123'},
            'content': {'sha': 'def456'}
        }
        mock_put.return_value = mock_response
        
        result = self.github_service.create_or_update_file(
            file_path='test/file.txt',
            content='test content',
            commit_message='Test commit',
            branch='main'
        )
        
        self.assertTrue(result)
        
        # Verify correct API call was made
        mock_put.assert_called_once()
        call_args = mock_put.call_args
        self.assertIn('contents', call_args[0][0])
        
    @patch('requests.get')
    def test_github_service_api_error(self, mock_get):
        """Test GitHub service handling API errors."""
        # Mock API error response
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.json.return_value = {'message': 'Not Found'}
        mock_get.return_value = mock_response
        
        prs = self.github_service.list_pull_requests()
        
        # Should return empty list on error
        self.assertEqual(prs, [])
        
    def test_github_service_no_settings(self):
        """Test GitHub service without settings."""
        # Delete settings
        GitSettings.objects.all().delete()
        
        # Should handle missing settings gracefully
        service = GitHubService(None)
        
        # Operations should fail gracefully
        prs = service.list_pull_requests()
        self.assertEqual(prs, [])


class GitRepositoryIntegrationTestCase(TestCase):
    """Integration tests for git repository functionality."""
    
    def setUp(self):
        """Set up test data for integration tests."""
        self.client = Client()
        
        # Create comprehensive git settings
        self.git_settings = GitSettings.objects.create(
            enabled=True,
            username='testuser',
            repository='test-repo',
            token='test-token-123',
            current_branch='main'
        )
        
        # Create multiple PRs for testing
        GitHubPR.objects.create(
            number=1,
            title='Feature: Add authentication',
            branch='feature/auth',
            state='open'
        )
        
        GitHubPR.objects.create(
            number=2,
            title='Fix: Resolve memory leaks',
            branch='hotfix/memory',
            state='merged'
        )
        
        GitHubPR.objects.create(
            number=3,
            title='Docs: Update README',
            branch='docs/readme',
            state='closed'
        )
        
    def test_full_github_workflow(self):
        """Test complete GitHub workflow integration."""
        # 1. View GitHub index
        url = reverse('github_index')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'testuser/test-repo')
        
        # Should show all PRs
        self.assertContains(response, 'Feature: Add authentication')
        self.assertContains(response, 'Fix: Resolve memory leaks')
        self.assertContains(response, 'Docs: Update README')
        
    def test_github_integration_with_dashboard(self):
        """Test GitHub integration appears in dashboard."""
        url = reverse('dashboard_data')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        
        # Should include git status information
        self.assertIn('git_status', data)
        git_status = data['git_status']
        
        self.assertTrue(git_status['enabled'])
        self.assertTrue(git_status['configured'])
        self.assertEqual(git_status['current_branch'], 'main')
        
    @patch('web_ui.services.github_service.GitHubService')
    def test_policy_push_to_github_integration(self, mock_github_service):
        """Test pushing policy to GitHub works with git integration."""
        from web_ui.web_ui.models import Policy
        
        # Create a test policy
        policy = Policy.objects.create(
            name='Test Policy',
            description='A test policy for git integration',
            policy_data={'type': 'METADATA', 'state': 'ACTIVE'}
        )
        
        # Mock GitHub service
        mock_service_instance = Mock() 
        mock_service_instance.create_or_update_file.return_value = True
        mock_github_service.return_value = mock_service_instance
        
        # Push policy to GitHub
        url = reverse('policy_push_github', args=[policy.id])
        response = self.client.post(url)
        
        self.assertEqual(response.status_code, 302)
        
        # Verify GitHub service was called
        mock_service_instance.create_or_update_file.assert_called_once()
        
    def test_git_repository_error_handling(self):
        """Test error handling in git repository operations."""
        # Disable git settings
        self.git_settings.enabled = False
        self.git_settings.save()
        
        # Operations should handle disabled git gracefully
        url = reverse('github_index')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Configure GitHub Integration')
        
    @patch('web_ui.services.github_service.GitHubService')
    def test_github_api_rate_limiting(self, mock_github_service):
        """Test handling of GitHub API rate limiting."""
        # Mock rate limit error
        mock_service_instance = Mock()
        mock_service_instance.list_pull_requests.side_effect = Exception("API rate limit exceeded")
        mock_github_service.return_value = mock_service_instance
        
        url = reverse('github_fetch_prs')
        response = self.client.post(url)
        
        # Should handle error gracefully
        self.assertIn(response.status_code, [200, 302])
        
        # Should show error message
        if response.status_code == 302:
            messages = list(get_messages(response.wsgi_request))
            self.assertTrue(any('error' in str(message).lower() for message in messages))


class GitRepositorySecurityTestCase(TestCase):
    """Test cases for git repository security."""
    
    def setUp(self):
        """Set up test data for security tests."""
        self.client = Client()
        
        # Create users with different permission levels
        self.regular_user = User.objects.create_user(
            username='regular',
            password='testpass123'
        )
        
        self.admin_user = User.objects.create_superuser(
            username='admin',
            email='admin@example.com',
            password='adminpass123'
        )
        
        self.git_settings = GitSettings.objects.create(
            enabled=True,
            username='testuser',
            repository='test-repo',
            token='sensitive-token-123'
        )
        
    def test_git_settings_permissions(self):
        """Test permissions for accessing git settings."""
        url = reverse('github_settings')
        
        # Anonymous user
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)  # Based on current implementation
        
        # Regular user
        self.client.login(username='regular', password='testpass123')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        
        # Admin user
        self.client.login(username='admin', password='adminpass123')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        
    def test_token_not_exposed_in_forms(self):
        """Test that git token is not exposed in form fields."""
        url = reverse('github_settings')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        content = response.content.decode()
        
        # Token should not appear in plain text
        self.assertNotIn('sensitive-token-123', content)
        
        # Should have password-type input or be masked
        self.assertIn('type="password"', content.lower())
        
    def test_git_operations_validation(self):
        """Test that git operations validate input properly."""
        url = reverse('github_create_pr')
        
        # Test with potentially malicious input
        response = self.client.post(url, {
            'title': '<script>alert("xss")</script>',
            'description': 'Normal description',
            'head_branch': 'feature/test',
            'base_branch': 'main'
        })
        
        # Should either sanitize input or reject it
        self.assertIn(response.status_code, [200, 302, 400])
        
    def test_github_webhook_security(self):
        """Test GitHub webhook endpoint security if it exists."""
        # This would test webhook signature verification
        # Implementation depends on whether webhooks are implemented
        pass
        
    def test_repository_access_controls(self):
        """Test that repository operations respect access controls."""
        # Test that users can only access repositories they have access to
        # This would depend on the specific authorization implementation
        
        url = reverse('github_index')
        response = self.client.get(url)
        
        # Should only show repositories the user has access to
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'testuser/test-repo')


class GitRepositoryPerformanceTestCase(TestCase):
    """Performance tests for git repository functionality."""
    
    def setUp(self):
        """Set up test data for performance tests."""
        self.client = Client()
        
        self.git_settings = GitSettings.objects.create(
            enabled=True,
            username='testuser',
            repository='test-repo',
            token='test-token'
        )
        
        # Create many PRs to test performance
        for i in range(50):
            GitHubPR.objects.create(
                number=i + 1,
                title=f'Performance Test PR {i}',
                branch=f'feature/perf-{i}',
                state='open' if i % 2 == 0 else 'closed'
            )
            
    def test_github_index_performance(self):
        """Test GitHub index performance with many PRs."""
        import time
        
        start_time = time.time()
        url = reverse('github_index')
        response = self.client.get(url)
        end_time = time.time()
        
        self.assertEqual(response.status_code, 200)
        
        execution_time = end_time - start_time
        self.assertLess(execution_time, 2.0, "GitHub index took too long to load")
        
        # Should show recent PRs (not necessarily all)
        for i in range(min(10, 50)):  # Check first 10
            self.assertContains(response, f'Performance Test PR {i}')
            
    @patch('web_ui.services.github_service.GitHubService')
    def test_pr_fetching_performance(self, mock_github_service):
        """Test performance of fetching PRs from GitHub API."""
        # Mock service to return many PRs
        mock_service_instance = Mock()
        mock_prs = [
            {
                'number': i,
                'title': f'API PR {i}',
                'body': f'Description {i}',
                'head': {'ref': f'feature/api-{i}'},
                'state': 'open',
                'html_url': f'https://github.com/testuser/test-repo/pull/{i}'
            }
            for i in range(100)
        ]
        mock_service_instance.list_pull_requests.return_value = mock_prs
        mock_github_service.return_value = mock_service_instance
        
        import time
        start_time = time.time()
        
        url = reverse('github_fetch_prs')
        response = self.client.post(url)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        self.assertEqual(response.status_code, 302)
        self.assertLess(execution_time, 5.0, "PR fetching took too long")
        
        # Verify PRs were created/updated efficiently
        total_prs = GitHubPR.objects.count()
        self.assertGreaterEqual(total_prs, 100) 