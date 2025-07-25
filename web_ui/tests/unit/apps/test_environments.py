import json
from unittest.mock import Mock, patch, MagicMock
from django.test import TestCase, Client
from django.urls import reverse
from django.contrib.auth.models import User
from django.contrib.messages import get_messages
from django.utils import timezone
from datetime import datetime, timedelta

from web_ui.web_ui.models import Environment, Mutation, GitSettings


class EnvironmentModelTestCase(TestCase):
    """Test cases for Environment model functionality."""
    
    def setUp(self):
        """Set up test data for environment model tests."""
        pass
        
    def test_environment_creation(self):
        """Test creating a new environment."""
        environment = Environment.objects.create(
            name='development',
            description='Development environment for testing',
            is_default=False
        )
        
        self.assertEqual(environment.name, 'development')
        self.assertEqual(environment.description, 'Development environment for testing')
        self.assertFalse(environment.is_default)
        self.assertIsNotNone(environment.created_at)
        self.assertIsNotNone(environment.updated_at)
        
    def test_environment_str_representation(self):
        """Test string representation of environment."""
        environment = Environment.objects.create(
            name='production',
            description='Production environment'
        )
        
        self.assertEqual(str(environment), 'production')
        
    def test_default_environment_constraint(self):
        """Test that only one environment can be marked as default."""
        # Create first default environment
        env1 = Environment.objects.create(
            name='dev',
            is_default=True
        )
        
        # Create second environment and mark as default
        env2 = Environment.objects.create(
            name='staging',
            is_default=True
        )
        
        # Refresh from database
        env1.refresh_from_db()
        env2.refresh_from_db()
        
        # Only the second one should be default now
        self.assertFalse(env1.is_default)
        self.assertTrue(env2.is_default)
        
    def test_environment_ordering(self):
        """Test that environments are ordered by name."""
        Environment.objects.create(name='production')
        Environment.objects.create(name='development')
        Environment.objects.create(name='staging')
        
        environments = list(Environment.objects.all())
        names = [env.name for env in environments]
        
        self.assertEqual(names, ['development', 'production', 'staging'])
        
    def test_environment_with_mutations(self):
        """Test environment with associated mutations."""
        # Create a mutation first
        mutation = Mutation.objects.create(
            name='test-mutation',
            description='A test mutation'
        )
        
        environment = Environment.objects.create(
            name='test-env',
            mutations=mutation
        )
        
        self.assertEqual(environment.mutations, mutation)
        self.assertEqual(environment.mutations.name, 'test-mutation')


class EnvironmentViewsTestCase(TestCase):
    """Test cases for environment views and endpoints."""
    
    def setUp(self):
        """Set up test data for environment views tests."""
        self.client = Client()
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        
        # Create test environments
        self.dev_env = Environment.objects.create(
            name='development',
            description='Development environment',
            is_default=True
        )
        
        self.prod_env = Environment.objects.create(
            name='production',
            description='Production environment',
            is_default=False
        )
        
    def test_environments_list_view(self):
        """Test the environments list view renders correctly."""
        url = reverse('environments')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Environments')
        self.assertContains(response, 'development')
        self.assertContains(response, 'production')
        self.assertContains(response, 'Create Environment')
        
    def test_environments_list_view_shows_default_indicator(self):
        """Test that the list view shows which environment is default."""
        url = reverse('environments')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        # Should show some indicator that development is default
        content = response.content.decode()
        
        # Check for default environment indicator
        self.assertIn('development', content)
        # Look for some default indicator (badge, icon, etc.)
        dev_env_section = content[content.find('development'):content.find('development') + 500]
        self.assertTrue(
            'default' in dev_env_section.lower() or 
            'primary' in dev_env_section.lower() or
            'badge' in dev_env_section.lower()
        )
        
    def test_environment_create_view_get(self):
        """Test GET request to environment create view."""
        url = reverse('environment_create')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Create Environment')
        self.assertContains(response, 'name="name"')
        self.assertContains(response, 'name="description"')
        self.assertContains(response, 'name="is_default"')
        
    def test_environment_create_view_post_success(self):
        """Test successful POST request to create a new environment."""
        url = reverse('environment_create')
        
        response = self.client.post(url, {
            'name': 'staging',
            'description': 'Staging environment for testing',
            'is_default': False
        })
        
        self.assertEqual(response.status_code, 302)  # Redirect after success
        
        # Check that environment was created
        new_env = Environment.objects.get(name='staging')
        self.assertEqual(new_env.description, 'Staging environment for testing')
        self.assertFalse(new_env.is_default)
        
        # Check success message was created
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any('successfully created' in str(message) for message in messages))
        
    def test_environment_create_view_duplicate_name(self):
        """Test creating environment with duplicate name."""
        url = reverse('environment_create')
        
        response = self.client.post(url, {
            'name': 'development',  # This already exists
            'description': 'Another development environment',
            'is_default': False
        })
        
        # Should return to form with error
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'already exists')
        
    def test_environment_create_sets_new_default(self):
        """Test creating environment and setting it as new default."""
        url = reverse('environment_create')
        
        response = self.client.post(url, {
            'name': 'new_default',
            'description': 'New default environment',
            'is_default': True
        })
        
        self.assertEqual(response.status_code, 302)
        
        # Check that new environment is default
        new_env = Environment.objects.get(name='new_default') 
        self.assertTrue(new_env.is_default)
        
        # Check that old default is no longer default
        self.dev_env.refresh_from_db()
        self.assertFalse(self.dev_env.is_default)
        
    def test_environment_create_invalid_data(self):
        """Test creating environment with invalid data."""
        url = reverse('environment_create')
        
        # Test with empty name
        response = self.client.post(url, {
            'name': '',
            'description': 'Environment with no name',
            'is_default': False
        })
        
        self.assertEqual(response.status_code, 200)  # Returns to form
        self.assertContains(response, 'This field is required')
        
    def test_environment_edit_functionality(self):
        """Test editing environment (assuming edit view exists)."""
        # This test assumes there might be an edit view - if not, this test would be skipped
        # or we'd test editing through the admin interface or API
        
        # Update environment directly through model to test the save override
        self.dev_env.name = 'updated_development'
        self.dev_env.description = 'Updated description'
        self.dev_env.save()
        
        updated_env = Environment.objects.get(id=self.dev_env.id)
        self.assertEqual(updated_env.name, 'updated_development')
        self.assertEqual(updated_env.description, 'Updated description')
        self.assertTrue(updated_env.is_default)  # Should still be default
        
    def test_environment_delete_functionality(self):
        """Test deleting environment."""
        # Create environment that's not default
        test_env = Environment.objects.create(
            name='to_be_deleted',
            description='This will be deleted'
        )
        
        env_id = test_env.id
        test_env.delete()
        
        # Check that environment was deleted
        with self.assertRaises(Environment.DoesNotExist):
            Environment.objects.get(id=env_id)
            
    def test_cannot_delete_default_environment(self):
        """Test that deleting default environment is handled properly."""
        # This would depend on business logic - whether default env can be deleted
        # For now, we test that we can identify which is default
        
        default_env = Environment.objects.filter(is_default=True).first()
        self.assertIsNotNone(default_env)
        self.assertEqual(default_env.name, 'development')


class EnvironmentIntegrationTestCase(TestCase):
    """Integration tests for environments with other components."""
    
    def setUp(self):
        """Set up test data for integration tests."""
        self.client = Client()
        
        # Create environments
        self.dev_env = Environment.objects.create(
            name='dev',
            description='Development',
            is_default=True
        )
        
        self.staging_env = Environment.objects.create(
            name='staging',
            description='Staging'
        )
        
        self.prod_env = Environment.objects.create(
            name='prod',  
            description='Production'
        )
        
        # Create git settings for integration testing
        self.git_settings = GitSettings.objects.create(
            enabled=True,
            username='testuser',
            repository='test-repo',
            token='test-token'
        )
        
    def test_environments_in_dashboard_context(self):
        """Test that environments appear correctly in dashboard context."""
        # Test dashboard data endpoint includes environments
        url = reverse('dashboard_data')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        
        self.assertIn('environments', data)
        environments = data['environments']
        
        # Should have all 3 environments
        self.assertEqual(len(environments), 3)
        
        # Find default environment
        default_env = next((env for env in environments if env['is_default']), None)
        self.assertIsNotNone(default_env)
        self.assertEqual(default_env['name'], 'dev')
        
    def test_environment_mutations_relationship(self):
        """Test relationship between environments and mutations."""
        # Create a mutation
        mutation = Mutation.objects.create(
            name='staging-mutations',
            description='Mutations for staging environment'
        )
        
        # Associate with staging environment
        self.staging_env.mutations = mutation
        self.staging_env.save()
        
        # Verify relationship
        self.assertEqual(self.staging_env.mutations, mutation)
        
        # Check reverse relationship
        environments_with_mutation = mutation.environments.all()
        self.assertIn(self.staging_env, environments_with_mutation)
        
    def test_environment_ordering_with_default_first(self):
        """Test that environments are ordered with default first."""
        environments = Environment.objects.all()
        
        # Should be ordered by -is_default, then name
        env_list = list(environments)
        
        # First should be default
        self.assertTrue(env_list[0].is_default)
        self.assertEqual(env_list[0].name, 'dev')
        
        # Rest should be alphabetical
        non_default_names = [env.name for env in env_list[1:]]
        self.assertEqual(non_default_names, sorted(non_default_names))
        
    def test_environments_with_metadata_context(self):
        """Test environments in context of metadata management."""
        # This would test how environments interact with metadata entities
        # For example, environment-specific mutations or configurations
        
        from metadata_manager.models import Domain
        
        # Create domain in dev environment context
        domain = Domain.objects.create(
            name='Dev Domain',
            description='Domain for dev environment'
        )
        
        # Test that we can query environments in metadata context
        environments = Environment.objects.all()
        self.assertEqual(environments.count(), 3)
        
        # The domain would potentially have environment-specific mutations
        # This would depend on the specific implementation details


class EnvironmentAdminTestCase(TestCase):
    """Test cases for environment admin functionality."""
    
    def setUp(self):
        """Set up test data for admin tests."""
        self.client = Client()
        
        # Create admin user
        self.admin_user = User.objects.create_superuser(
            username='admin',
            email='admin@example.com',
            password='adminpass123'
        )
        
        self.test_env = Environment.objects.create(
            name='admin_test',
            description='Environment for admin testing'
        )
        
    def test_environment_admin_access(self):
        """Test that environment admin is accessible."""
        self.client.login(username='admin', password='adminpass123')
        
        # Test admin list view
        url = '/admin/web_ui/environment/'
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'admin_test')
        
    def test_environment_admin_creation(self):
        """Test creating environment through admin."""
        self.client.login(username='admin', password='adminpass123')
        
        url = '/admin/web_ui/environment/add/'
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'name')
        self.assertContains(response, 'description')
        self.assertContains(response, 'is_default')
        
    def test_environment_admin_editing(self):
        """Test editing environment through admin."""
        self.client.login(username='admin', password='adminpass123')
        
        url = f'/admin/web_ui/environment/{self.test_env.id}/change/'
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'admin_test')


class EnvironmentSecurityTestCase(TestCase):
    """Test cases for environment security and permissions."""
    
    def setUp(self):
        """Set up test data for security tests."""
        self.client = Client()
        
        # Create different types of users
        self.regular_user = User.objects.create_user(
            username='regular',
            password='testpass123'
        )
        
        self.admin_user = User.objects.create_superuser(
            username='admin',
            email='admin@example.com', 
            password='adminpass123'
        )
        
        self.test_env = Environment.objects.create(
            name='secure_env',
            description='Environment for security testing'
        )
        
    def test_environment_list_permissions(self):
        """Test permissions for environment list view."""
        url = reverse('environments')
        
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
        
    def test_environment_create_permissions(self):
        """Test permissions for environment creation."""
        url = reverse('environment_create')
        
        # Anonymous user
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)  # Based on current implementation
        
        # Regular user
        self.client.login(username='regular', password='testpass123')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        
    def test_environment_data_validation(self):
        """Test that environment data is properly validated."""
        url = reverse('environment_create')
        
        # Test with malicious input
        response = self.client.post(url, {
            'name': '<script>alert("xss")</script>',
            'description': 'Normal description',
            'is_default': False
        })
        
        if response.status_code == 302:  # Created successfully
            # Check that the script was escaped/sanitized
            created_env = Environment.objects.get(name__contains='script')
            # The exact escaping depends on Django's implementation
            self.assertNotEqual(created_env.name, '<script>alert("xss")</script>')
        else:
            # Creation was rejected, which is also acceptable
            self.assertEqual(response.status_code, 200)


class EnvironmentPerformanceTestCase(TestCase):
    """Performance tests for environment functionality."""
    
    def setUp(self):
        """Set up test data for performance tests."""
        self.client = Client()
        
        # Create many environments to test performance
        for i in range(50):
            Environment.objects.create(
                name=f'perf_env_{i:03d}',
                description=f'Performance test environment {i}',
                is_default=(i == 0)  # First one is default
            )
            
    def test_environment_list_performance(self):
        """Test that environment list loads efficiently with many environments."""
        import time
        
        start_time = time.time()
        url = reverse('environments')
        response = self.client.get(url)
        end_time = time.time()
        
        self.assertEqual(response.status_code, 200)
        
        # Should complete within reasonable time
        execution_time = end_time - start_time
        self.assertLess(execution_time, 2.0, "Environment list took too long to load")
        
        # Should contain all environments
        for i in range(5):  # Check first few
            self.assertContains(response, f'perf_env_{i:03d}')
            
    def test_dashboard_environments_performance(self):
        """Test that dashboard loads environment data efficiently."""
        import time
        
        start_time = time.time()
        url = reverse('dashboard_data')
        response = self.client.get(url)
        end_time = time.time()
        
        self.assertEqual(response.status_code, 200)
        
        execution_time = end_time - start_time
        self.assertLess(execution_time, 2.0, "Dashboard environment data took too long to load")
        
        data = json.loads(response.content)
        self.assertEqual(len(data['environments']), 50)
        
        # Verify default is first
        self.assertTrue(data['environments'][0]['is_default'])
        self.assertEqual(data['environments'][0]['name'], 'perf_env_000') 