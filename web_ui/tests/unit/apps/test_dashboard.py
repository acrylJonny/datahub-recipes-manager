import json
from unittest.mock import Mock, patch, MagicMock
from django.test import TestCase, Client
from django.urls import reverse
from django.contrib.auth.models import User
from django.contrib.messages import get_messages
from django.utils import timezone
from datetime import datetime, timedelta

from web_ui.models import (
    ScriptRun, ScriptResult, Artifact, LogEntry, Environment, 
    Policy, GitSettings, DataHubClientInfo
)
from metadata_manager.models import (
    Domain, GlossaryNode, GlossaryTerm, DataProduct, Assertion
)


class DashboardViewsTestCase(TestCase):
    """Test cases for dashboard views and functionality."""
    
    def setUp(self):
        """Set up test data for dashboard tests."""
        self.client = Client()
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        
        # Create test script runs for dashboard stats
        self.script_run_success = ScriptRun.objects.create(
            script_name='test_recipe',
            status='success',
            parameters={'key': 'value'},
            created_at=timezone.now() - timedelta(hours=1)
        )
        
        self.script_run_failed = ScriptRun.objects.create(
            script_name='test_policy',
            status='failed',
            parameters={'error': 'test'},
            created_at=timezone.now() - timedelta(hours=2)
        )
        
        # Create test environment
        self.test_environment = Environment.objects.create(
            name='test',
            description='Test environment',
            is_default=True
        )
        
        # Create test metadata entities
        self.test_domain = Domain.objects.create(
            name='Test Domain',
            description='A test domain',
            sync_status='LOCAL_ONLY'
        )
        
        # Create git settings
        self.git_settings = GitSettings.objects.create(
            enabled=True,
            username='testuser',
            repository='test-repo',
            token='test-token',
            current_branch='main'
        )
        
    def test_dashboard_view_basic_rendering(self):
        """Test that dashboard view renders correctly without DataHub connection."""
        url = reverse('dashboard')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Dashboard')
        self.assertContains(response, 'System Health Overview')
        self.assertContains(response, 'Recipes')
        self.assertContains(response, 'Policies')
        
    def test_dashboard_view_authenticated(self):
        """Test dashboard view with authenticated user."""
        self.client.login(username='testuser', password='testpass123')
        url = reverse('dashboard')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Dashboard')
        
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_dashboard_data_no_connection(self, mock_get_client):
        """Test dashboard data endpoint with no DataHub connection."""
        mock_get_client.return_value = None
        
        url = reverse('dashboard_data')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        
        self.assertFalse(data['connected'])
        self.assertEqual(data['recipes_count'], 0)
        self.assertEqual(data['policies_count'], 0)
        self.assertIsInstance(data['environments'], list)
        self.assertIsInstance(data['metadata_stats'], dict)
        self.assertIsInstance(data['git_status'], dict)
        
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_dashboard_data_with_connection(self, mock_get_client):
        """Test dashboard data endpoint with successful DataHub connection."""
        # Mock DataHub client
        mock_client = Mock()
        mock_client.test_connection.return_value = True
        mock_client.list_ingestion_sources.return_value = [
            {
                'name': 'test-recipe-1',
                'lastUpdated': 1640995200000,  # 2022-01-01
                'schedule': {'interval': '0 0 * * *'}
            },
            {
                'name': 'test-recipe-2', 
                'lastUpdated': 1640995300000,  # 2022-01-01 + 100s
                'schedule': None
            }
        ]
        mock_client.list_policies.return_value = [
            {'name': 'test-policy-1'},
            {'name': 'test-policy-2'}
        ]
        mock_get_client.return_value = mock_client
        
        url = reverse('dashboard_data')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        
        self.assertTrue(data['connected'])
        self.assertEqual(data['recipes_count'], 2)
        self.assertEqual(data['active_schedules_count'], 1)
        self.assertEqual(data['policies_count'], 2)
        self.assertEqual(len(data['recent_recipes']), 2)
        self.assertEqual(len(data['recent_policies']), 2)
        
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_dashboard_data_connection_error(self, mock_get_client):
        """Test dashboard data endpoint when DataHub connection fails."""
        mock_client = Mock()
        mock_client.test_connection.return_value = False
        mock_get_client.return_value = mock_client
        
        url = reverse('dashboard_data')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        
        self.assertFalse(data['connected'])
        self.assertEqual(data['recipes_count'], 0)
        self.assertEqual(data['policies_count'], 0)
        
    def test_dashboard_data_environments(self):
        """Test that dashboard data includes environment information."""
        url = reverse('dashboard_data')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        
        self.assertIn('environments', data)
        environments = data['environments']
        self.assertEqual(len(environments), 1)
        self.assertEqual(environments[0]['name'], 'test')
        self.assertTrue(environments[0]['is_default'])
        
    def test_dashboard_data_metadata_stats(self):
        """Test that dashboard data includes metadata statistics."""
        url = reverse('dashboard_data')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        
        self.assertIn('metadata_stats', data)
        stats = data['metadata_stats']
        self.assertEqual(stats['domains_count'], 1)
        self.assertEqual(stats['domains_local'], 1)
        self.assertEqual(stats['domains_synced'], 0)
        
    def test_dashboard_data_git_status(self):
        """Test that dashboard data includes git status information."""
        url = reverse('dashboard_data')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        
        self.assertIn('git_status', data)
        git_status = data['git_status']
        self.assertTrue(git_status['enabled'])
        self.assertTrue(git_status['configured'])
        self.assertEqual(git_status['current_branch'], 'main')
        self.assertIn('github.com/testuser/test-repo', git_status['repository_url'])
        
    def test_dashboard_data_system_health(self):
        """Test that dashboard data includes system health overview."""
        url = reverse('dashboard_data')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        
        self.assertIn('system_health', data)
        health = data['system_health']
        self.assertIn('datahub_connection', health)
        self.assertIn('environments_configured', health)
        self.assertIn('git_integration', health)
        self.assertIn('metadata_sync_pending', health)
        self.assertIn('total_metadata_items', health)
        
        self.assertEqual(health['environments_configured'], 1)
        self.assertTrue(health['git_integration'])
        self.assertEqual(health['metadata_sync_pending'], 1)  # One local domain
        self.assertEqual(health['total_metadata_items'], 1)
        
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_dashboard_data_api_errors(self, mock_get_client):
        """Test dashboard data endpoint handles API errors gracefully."""
        mock_client = Mock()
        mock_client.test_connection.return_value = True
        mock_client.list_ingestion_sources.side_effect = Exception("API Error")
        mock_client.list_policies.side_effect = Exception("API Error")
        mock_get_client.return_value = mock_client
        
        url = reverse('dashboard_data')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        
        # Should still return data despite API errors
        self.assertTrue(data['connected'])
        self.assertEqual(data['recipes_count'], 0)
        self.assertEqual(data['policies_count'], 0)
        self.assertEqual(data['recent_recipes'], [])
        self.assertEqual(data['recent_policies'], [])
        
    def test_dashboard_with_no_git_settings(self):
        """Test dashboard data when git settings don't exist."""
        GitSettings.objects.all().delete()
        
        url = reverse('dashboard_data')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        
        git_status = data['git_status']
        self.assertFalse(git_status['enabled'])
        self.assertFalse(git_status['configured'])
        
    def test_dashboard_with_incomplete_git_settings(self):
        """Test dashboard data when git settings are incomplete."""
        self.git_settings.token = ''
        self.git_settings.save()
        
        url = reverse('dashboard_data')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        
        git_status = data['git_status']
        self.assertTrue(git_status['enabled'])
        self.assertFalse(git_status['configured'])  # Missing token


class DashboardIntegrationTestCase(TestCase):
    """Integration tests for dashboard with complex scenarios."""
    
    def setUp(self):
        """Set up complex test data."""
        self.client = Client()
        
        # Create multiple environments
        Environment.objects.create(name='dev', is_default=True)
        Environment.objects.create(name='staging', is_default=False)
        Environment.objects.create(name='prod', is_default=False)
        
        # Create mixed metadata entities
        for i in range(3):
            Domain.objects.create(
                name=f'Domain {i}',
                sync_status='LOCAL_ONLY' if i < 2 else 'SYNCED'
            )
            
        for i in range(2):
            GlossaryTerm.objects.create(
                name=f'Term {i}',
                sync_status='MODIFIED' if i == 0 else 'SYNCED'
            )
            
        # Create log entries
        LogEntry.objects.create(
            level='INFO',
            message='Test info message',
            source='test.module'
        )
        LogEntry.objects.create(
            level='ERROR', 
            message='Test error message',
            source='test.module'
        )
        
    def test_complex_dashboard_data(self):
        """Test dashboard data with complex metadata scenarios."""
        url = reverse('dashboard_data')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        
        # Check environments
        self.assertEqual(len(data['environments']), 3)
        default_env = next(env for env in data['environments'] if env['is_default'])
        self.assertEqual(default_env['name'], 'dev')
        
        # Check metadata stats
        stats = data['metadata_stats']
        self.assertEqual(stats['domains_count'], 3)
        self.assertEqual(stats['domains_local'], 2)  # 2 LOCAL_ONLY
        self.assertEqual(stats['domains_synced'], 1)  # 1 SYNCED
        
        self.assertEqual(stats['glossary_terms_count'], 2)
        self.assertEqual(stats['glossary_terms_local'], 1)  # 1 MODIFIED
        self.assertEqual(stats['glossary_terms_synced'], 1)  # 1 SYNCED
        
        # Check system health
        health = data['system_health']
        self.assertEqual(health['environments_configured'], 3)
        self.assertEqual(health['metadata_sync_pending'], 3)  # 2 domains + 1 term
        self.assertEqual(health['total_metadata_items'], 5)  # 3 domains + 2 terms


class DashboardPerformanceTestCase(TestCase):
    """Performance tests for dashboard functionality."""
    
    def setUp(self):
        """Set up large amounts of test data."""
        self.client = Client()
        
        # Create many metadata entities to test performance
        for i in range(50):
            Domain.objects.create(
                name=f'Performance Domain {i}',
                sync_status='LOCAL_ONLY' if i % 2 == 0 else 'SYNCED'
            )
            
        for i in range(30):
            GlossaryTerm.objects.create(
                name=f'Performance Term {i}',
                sync_status='MODIFIED' if i % 3 == 0 else 'SYNCED'
            )
            
    def test_dashboard_data_performance(self):
        """Test that dashboard data loads efficiently with large datasets."""
        import time
        
        start_time = time.time()
        url = reverse('dashboard_data')
        response = self.client.get(url)
        end_time = time.time()
        
        self.assertEqual(response.status_code, 200)
        
        # Should complete within reasonable time (adjust threshold as needed)
        execution_time = end_time - start_time
        self.assertLess(execution_time, 2.0, "Dashboard data took too long to load")
        
        data = json.loads(response.content)
        
        # Verify correct counts despite large dataset
        stats = data['metadata_stats']
        self.assertEqual(stats['domains_count'], 50)
        self.assertEqual(stats['glossary_terms_count'], 30) 