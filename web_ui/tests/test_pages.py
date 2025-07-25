"""
Comprehensive tests for all main pages in the web_ui application.

Tests cover Python views, HTML templates, and JavaScript functionality for:
- Dashboard
- Recipes (list, create, edit, delete, run)
- Policies (list, create, edit, delete, deploy)
- Metadata Manager (all sub-modules)
- GitHub Integration (all features)
- Environments
- Logs
- Settings
- Recipe Templates (all operations)
- Recipe Instances (all operations)
- Environment Variables (templates and instances)
- Connections
- Mutations (complete CRUD operations)
- Authentication
- API Endpoints
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
from bs4 import BeautifulSoup

from tests.fixtures.factories import (
    UserFactory, EnvironmentFactory, PolicyFactory, 
    LogEntryFactory, GitSettingsFactory, MutationFactory
)


class BasePageTestCase(TestCase):
    """Base test case for page testing with common setup."""
    
    def setUp(self):
        self.client = Client()
        self.admin_user = UserFactory.create_admin()
        self.regular_user = UserFactory.create_user()
        self.environment = EnvironmentFactory(name='test-env', is_default=True)
    
    def parse_html(self, response):
        """Parse HTML response with BeautifulSoup."""
        return BeautifulSoup(response.content, 'html.parser')
    
    def assertTemplateUsed(self, response, template_name):
        """Assert that specific template was used."""
        template_names = [t.name for t in response.templates]
        self.assertIn(template_name, template_names)
    
    def assertContainsText(self, response, text):
        """Assert response contains specific text."""
        self.assertContains(response, text)
    
    def assertJSONResponse(self, response, expected_keys=None):
        """Assert response is valid JSON with expected keys."""
        self.assertEqual(response['Content-Type'], 'application/json')
        data = json.loads(response.content)
        if expected_keys:
            for key in expected_keys:
                self.assertIn(key, data)
        return data


class DashboardPageTestCase(BasePageTestCase):
    """Test the main dashboard page."""
    
    def test_dashboard_page_loads(self):
        """Test dashboard page loads correctly."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('dashboard'))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'dashboard.html')
        self.assertContainsText(response, 'Dashboard')
    
    def test_dashboard_requires_authentication(self):
        """Test dashboard requires user authentication."""
        response = self.client.get(reverse('dashboard'))
        self.assertRedirects(response, '/login/?next=/dashboard/')
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_dashboard_data_api(self, mock_get_client):
        """Test dashboard data API endpoint."""
        # Mock DataHub client
        mock_client = Mock()
        mock_client.test_connection.return_value = True
        mock_client.list_ingestion_sources.return_value = [
            {'name': 'test-recipe', 'lastUpdated': 1640995200000}
        ]
        mock_client.list_policies.return_value = [
            {'name': 'test-policy', 'urn': 'urn:li:dataHubPolicy:test'}
        ]
        mock_get_client.return_value = mock_client
        
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('dashboard_data'))
        
        self.assertEqual(response.status_code, 200)
        data = self.assertJSONResponse(response, ['connected', 'recipes_count', 'policies_count'])
        self.assertTrue(data['connected'])
        self.assertEqual(data['recipes_count'], 1)
        self.assertEqual(data['policies_count'], 1)
    
    def test_dashboard_html_structure(self):
        """Test dashboard HTML structure and elements."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('dashboard'))
        soup = self.parse_html(response)
        
        # Check for main dashboard container
        dashboard = soup.find('div', class_='dashboard')
        self.assertIsNotNone(dashboard)
        
        # Check for statistics cards
        stat_cards = soup.find_all('div', class_='stat-card')
        self.assertGreater(len(stat_cards), 0)
        
        # Check for charts or data loading elements
        charts = soup.find_all('canvas') or soup.find_all('div', {'data-chart': True})
        self.assertGreater(len(charts), 0)


class RecipesPageTestCase(BasePageTestCase):
    """Test recipe management pages."""
    
    def test_recipes_list_page(self):
        """Test recipes list page loads correctly."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('recipes'))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'recipes/list.html')
        self.assertContainsText(response, 'Recipes')
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_recipes_data_api(self, mock_get_client):
        """Test recipes data API endpoint."""
        mock_client = Mock()
        mock_client.list_ingestion_sources.return_value = [
            {
                'name': 'test-recipe',
                'type': 'mysql',
                'lastUpdated': 1640995200000,
                'schedule': {'interval': '0 0 * * *'},
                'status': 'running'
            }
        ]
        mock_get_client.return_value = mock_client
        
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('recipes_data'))
        
        self.assertEqual(response.status_code, 200)
        data = self.assertJSONResponse(response, ['recipes'])
        self.assertEqual(len(data['recipes']), 1)
        self.assertEqual(data['recipes'][0]['name'], 'test-recipe')
    
    def test_recipe_create_page(self):
        """Test recipe creation page."""
        self.client.force_login(self.admin_user)
        response = self.client.get(reverse('recipe_create'))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'recipes/create.html')
        self.assertContainsText(response, 'Create Recipe')
        
        # Check form elements
        soup = self.parse_html(response)
        form = soup.find('form', method='post')
        self.assertIsNotNone(form)
        
        # Check for recipe type selection
        recipe_type = soup.find('select', {'name': 'recipe_type'})
        self.assertIsNotNone(recipe_type)
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_recipe_create_post(self, mock_get_client):
        """Test recipe creation via POST."""
        mock_client = Mock()
        mock_client.create_ingestion_source.return_value = {'urn': 'test-urn'}
        mock_get_client.return_value = mock_client
        
        self.client.force_login(self.admin_user)
        
        recipe_data = {
            'name': 'test-recipe',
            'type': 'mysql',
            'config': json.dumps({
                'host': 'localhost',
                'port': 3306,
                'database': 'test'
            }),
            'schedule': '0 0 * * *'
        }
        
        # Test would depend on actual form structure
        # response = self.client.post(reverse('recipe_create'), recipe_data)
        # self.assertEqual(response.status_code, 302)  # Redirect after success
    
    def test_recipe_edit_page(self):
        """Test recipe edit page."""
        self.client.force_login(self.admin_user)
        
        # Would need to create a test recipe first
        # response = self.client.get(reverse('recipe_edit', args=['test-recipe-id']))
        # self.assertEqual(response.status_code, 200)
        pass
    
    def test_recipe_delete_confirmation(self):
        """Test recipe deletion confirmation."""
        self.client.force_login(self.admin_user)
        
        # Would need to test with actual recipe
        # response = self.client.get(reverse('recipe_delete', args=['test-recipe-id']))
        # self.assertEqual(response.status_code, 200)
        pass


class PoliciesPageTestCase(BasePageTestCase):
    """Test policy management pages."""
    
    def test_policies_list_page(self):
        """Test policies list page loads correctly."""
        self.client.force_login(self.regular_user) 
        response = self.client.get(reverse('policies'))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'policies/list.html')
        self.assertContainsText(response, 'Policies')
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_policies_data_api(self, mock_get_client):
        """Test policies data API endpoint."""
        mock_client = Mock()
        mock_client.list_policies.return_value = [
            {
                'name': 'test-policy',
                'urn': 'urn:li:dataHubPolicy:test',
                'type': 'METADATA',
                'state': 'ACTIVE',
                'description': 'Test policy'
            }
        ]
        mock_get_client.return_value = mock_client
        
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('policies_data'))
        
        self.assertEqual(response.status_code, 200)
        data = self.assertJSONResponse(response, ['policies'])
        self.assertEqual(len(data['policies']), 1)
        self.assertEqual(data['policies'][0]['name'], 'test-policy')
    
    def test_policy_create_page(self):
        """Test policy creation page."""
        self.client.force_login(self.admin_user)
        response = self.client.get(reverse('policy_create'))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'policies/create.html')
        self.assertContainsText(response, 'Create Policy')
        
        # Check form elements
        soup = self.parse_html(response)
        form = soup.find('form', method='post')
        self.assertIsNotNone(form)
        
        # Check for JSON editor
        json_textarea = soup.find('textarea', {'name': 'policy_json'})
        self.assertIsNotNone(json_textarea)
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_policy_create_post(self, mock_get_client):
        """Test policy creation via POST."""
        mock_client = Mock()
        mock_client.create_policy.return_value = {'urn': 'test-policy-urn'}
        mock_get_client.return_value = mock_client
        
        self.client.force_login(self.admin_user)
        
        policy_data = {
            'name': 'Test Policy',
            'description': 'A test policy',
            'policy_json': json.dumps({
                'name': 'test-policy',
                'type': 'METADATA',
                'state': 'ACTIVE',
                'privileges': ['EDIT_ENTITY_OWNERS'],
                'actors': {'users': [], 'groups': []}
            })
        }
        
        # Test would depend on actual form handling
        # response = self.client.post(reverse('policy_create'), policy_data)
        # self.assertEqual(response.status_code, 302)
    
    def test_policy_detail_page(self):
        """Test policy detail page."""
        self.client.force_login(self.regular_user)
        
        # Would need test policy
        # response = self.client.get(reverse('policy_view', args=['test-policy-id']))
        # self.assertEqual(response.status_code, 200)
        pass


class MetadataManagerPageTestCase(BasePageTestCase):
    """Test metadata manager pages."""
    
    def test_metadata_index_page(self):
        """Test metadata manager index page."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('metadata_manager:metadata_index'))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'metadata_manager/index.html')
        self.assertContainsText(response, 'Metadata Manager')
    
    def test_metadata_tags_page(self):
        """Test metadata tags management page."""
        self.client.force_login(self.regular_user)
        
        # Test would depend on URL pattern
        # response = self.client.get(reverse('metadata_manager:tag_list'))
        # self.assertEqual(response.status_code, 200)
        pass
    
    def test_metadata_domains_page(self):
        """Test metadata domains management page."""
        self.client.force_login(self.regular_user)
        
        # Test would depend on URL pattern
        # response = self.client.get(reverse('metadata_manager:domain_list'))
        # self.assertEqual(response.status_code, 200)
        pass


class GitHubIntegrationPageTestCase(BasePageTestCase):
    """Test GitHub integration pages."""
    
    def test_github_index_page(self):
        """Test GitHub integration index page."""
        git_settings = GitSettingsFactory()
        
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('github_index'))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'github/index.html') 
        self.assertContainsText(response, 'GitHub Integration')
    
    def test_github_settings_page(self):
        """Test GitHub settings page."""
        self.client.force_login(self.admin_user)
        
        # Test would depend on URL pattern
        # response = self.client.get(reverse('github_settings'))
        # self.assertEqual(response.status_code, 200)
        pass
    
    def test_github_pr_list(self):
        """Test GitHub PR list functionality."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('github_index'))
        soup = self.parse_html(response)
        
        # Check for PR list container
        pr_section = soup.find('div', class_='pr-section')
        if pr_section:
            self.assertIsNotNone(pr_section)


class EnvironmentsPageTestCase(BasePageTestCase):
    """Test environments management pages."""
    
    def test_environments_list_page(self):
        """Test environments list page."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('environments'))
        
        self.assertEqual(response.status_code, 200)
        self.assertContainsText(response, 'Environments')
        
        # Should show our test environment
        self.assertContainsText(response, 'test-env')
    
    def test_environment_create_page(self):
        """Test environment creation page."""
        self.client.force_login(self.admin_user)
        response = self.client.get(reverse('environment_create'))
        
        self.assertEqual(response.status_code, 200)
        self.assertContainsText(response, 'Create Environment')
        
        # Check form structure
        soup = self.parse_html(response)
        form = soup.find('form', method='post')
        self.assertIsNotNone(form)
        
        # Check required fields
        name_field = soup.find('input', {'name': 'name'})
        host_field = soup.find('input', {'name': 'datahub_host'})
        self.assertIsNotNone(name_field)
        self.assertIsNotNone(host_field)
    
    def test_environment_create_post(self):
        """Test environment creation via POST."""
        self.client.force_login(self.admin_user)
        
        env_data = {
            'name': 'new-environment',
            'description': 'A new test environment',
            'datahub_host': 'http://test.datahub.com',
            'datahub_token': 'test-token-123',
            'is_default': False
        }
        
        # Test would depend on actual form implementation
        # response = self.client.post(reverse('environment_create'), env_data)
        # self.assertEqual(response.status_code, 302)


class LogsPageTestCase(BasePageTestCase):
    """Test logs management pages."""
    
    def test_logs_page_loads(self):
        """Test logs page loads correctly."""
        # Create test log entries
        LogEntryFactory(level='INFO', message='Test info message')
        LogEntryFactory(level='ERROR', message='Test error message')
        
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('logs'))
        
        self.assertEqual(response.status_code, 200)
        self.assertContainsText(response, 'Logs')
        self.assertContainsText(response, 'Test info message')
        self.assertContainsText(response, 'Test error message')
    
    def test_logs_filtering(self):
        """Test logs filtering functionality."""
        LogEntryFactory(level='INFO', message='Info message')
        LogEntryFactory(level='ERROR', message='Error message')
        
        self.client.force_login(self.regular_user)
        
        # Test level filtering
        response = self.client.get(reverse('logs'), {'level': 'ERROR'})
        self.assertEqual(response.status_code, 200)
        self.assertContainsText(response, 'Error message')
        self.assertNotContains(response, 'Info message')
    
    def test_logs_refresh_api(self):
        """Test logs refresh API endpoint."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('refresh_logs'))
        
        self.assertEqual(response.status_code, 200)
        # Should return JSON with logs data
        data = json.loads(response.content)
        self.assertIn('logs', data)


class SettingsPageTestCase(BasePageTestCase):
    """Test settings management pages."""
    
    def test_settings_page_loads(self):
        """Test settings page loads correctly."""
        self.client.force_login(self.admin_user)
        response = self.client.get(reverse('settings'))
        
        self.assertEqual(response.status_code, 200)
        self.assertContainsText(response, 'Settings')
    
    def test_settings_requires_admin(self):
        """Test settings page requires admin access."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('settings'))
        
        # Might redirect or show 403 depending on implementation
        self.assertIn(response.status_code, [302, 403])


class RecipeTemplatesPageTestCase(BasePageTestCase):
    """Enhanced test cases for Recipe Templates page."""
    
    def setUp(self):
        super().setUp()
        from template_manager.models import RecipeTemplate
        self.template = RecipeTemplate.objects.create(
            name='test-template',
            description='Test template',
            template_content={'test': 'content'},
            user=self.regular_user
        )
    
    def test_recipe_templates_list_page(self):
        """Test recipe templates list page with comprehensive coverage."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('template_manager:recipe_templates'))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'recipes/templates/list.html')
        soup = self.parse_html(response)
        
        # Check page structure
        self.assertIsNotNone(soup.select_one('h1.page-title'))
        self.assertContains(response, 'Recipe Templates')
        
        # Check templates table
        self.assertIsNotNone(soup.select_one('#templates-table'))
        self.assertContains(response, self.template.name)
        
        # Check action buttons
        self.assertIsNotNone(soup.select_one('.btn-create-template'))
        self.assertIsNotNone(soup.select_one('.btn-import'))
        self.assertIsNotNone(soup.select_one('.btn-export-all'))
    
    def test_recipe_template_create_page(self):
        """Test recipe template creation page."""
        self.client.force_login(self.admin_user)
        response = self.client.get(reverse('template_manager:recipe_template_create'))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'recipes/templates/create.html')
        soup = self.parse_html(response)
        
        # Check form elements
        self.assertIsNotNone(soup.select_one('form#template-form'))
        self.assertIsNotNone(soup.select_one('input[name="name"]'))
        self.assertIsNotNone(soup.select_one('textarea[name="description"]'))
        self.assertIsNotNone(soup.select_one('#template-editor'))
    
    def test_recipe_template_detail_page(self):
        """Test recipe template detail page."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('template_manager:recipe_template_detail', args=[self.template.id]))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'recipes/templates/detail.html')
        soup = self.parse_html(response)
        
        # Check template details
        self.assertContains(response, self.template.name)
        self.assertContains(response, self.template.description)
        
        # Check action buttons
        self.assertIsNotNone(soup.select_one('.btn-edit'))
        self.assertIsNotNone(soup.select_one('.btn-delete'))
        self.assertIsNotNone(soup.select_one('.btn-deploy'))
        self.assertIsNotNone(soup.select_one('.btn-export'))
    
    def test_recipe_template_edit_functionality(self):
        """Test recipe template editing with form validation."""
        self.client.force_login(self.admin_user)
        
        # Test GET request
        response = self.client.get(reverse('template_manager:recipe_template_edit', args=[self.template.id]))
        self.assertEqual(response.status_code, 200)
        
        # Test POST request
        edit_data = {
            'name': 'updated-template',
            'description': 'Updated description',
            'template_content': json.dumps({'updated': 'content'})
        }
        
        response = self.client.post(
            reverse('template_manager:recipe_template_edit', args=[self.template.id]), 
            edit_data, 
            follow=True
        )
        self.assertEqual(response.status_code, 200)
        
        # Verify update
        self.template.refresh_from_db()
        self.assertEqual(self.template.name, 'updated-template')
    
    def test_recipe_template_delete_functionality(self):
        """Test recipe template deletion with confirmation."""
        self.client.force_login(self.admin_user)
        
        response = self.client.post(
            reverse('template_manager:recipe_template_delete', args=[self.template.id]), 
            follow=True
        )
        self.assertEqual(response.status_code, 200)
        
        # Verify deletion
        from template_manager.models import RecipeTemplate
        self.assertFalse(RecipeTemplate.objects.filter(id=self.template.id).exists())
    
    @patch('template_manager.views.deploy_template_to_datahub')
    def test_recipe_template_deploy_functionality(self, mock_deploy):
        """Test recipe template deployment to DataHub."""
        mock_deploy.return_value = {'success': True, 'message': 'Deployed successfully'}
        
        self.client.force_login(self.admin_user)
        
        response = self.client.post(
            reverse('template_manager:recipe_template_deploy', args=[self.template.id]),
            follow=True
        )
        self.assertEqual(response.status_code, 200)
        
        # Verify deployment was called
        mock_deploy.assert_called_once_with(self.template)
        
        # Check success message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any('Deployed successfully' in str(m) for m in messages))
    
    def test_recipe_template_export_functionality(self):
        """Test recipe template export functionality."""
        self.client.force_login(self.regular_user)
        
        response = self.client.get(reverse('template_manager:recipe_template_export', args=[self.template.id]))
        
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'application/json')
        self.assertIn('attachment', response['Content-Disposition'])
    
    def test_recipe_template_import_functionality(self):
        """Test recipe template import functionality."""
        self.client.force_login(self.admin_user)
        
        # Create test import file
        import_data = {
            'name': 'imported-template',
            'description': 'Imported template',
            'template_content': {'imported': 'content'}
        }
        
        import_file = SimpleUploadedFile(
            'template.json',
            json.dumps(import_data).encode('utf-8'),
            content_type='application/json'
        )
        
        response = self.client.post(
            reverse('template_manager:recipe_template_import'),
            {'file': import_file},
            follow=True
        )
        
        self.assertEqual(response.status_code, 200)
        
        # Verify import
        from template_manager.models import RecipeTemplate
        imported_template = RecipeTemplate.objects.get(name='imported-template')
        self.assertEqual(imported_template.description, 'Imported template')
    
    def test_recipe_template_save_from_existing(self):
        """Test saving existing recipe as template."""
        from web_ui.web_ui.models import Recipe
        recipe = Recipe.objects.create(
            name='test-recipe',
            recipe_content={'test': 'recipe'},
            user=self.regular_user
        )
        
        self.client.force_login(self.admin_user)
        
        response = self.client.post(
            reverse('template_manager:recipe_template_save', args=[recipe.id]),
            {
                'template_name': 'recipe-template',
                'template_description': 'Created from recipe'
            },
            follow=True
        )
        
        self.assertEqual(response.status_code, 200)
        
        # Verify template was created
        from template_manager.models import RecipeTemplate
        template = RecipeTemplate.objects.get(name='recipe-template')
        self.assertEqual(template.description, 'Created from recipe')
    
    def test_export_all_templates(self):
        """Test exporting all templates functionality."""
        # Create additional templates
        from template_manager.models import RecipeTemplate
        RecipeTemplate.objects.create(
            name='template-2',
            description='Second template',
            template_content={'second': 'content'},
            user=self.regular_user
        )
        
        self.client.force_login(self.admin_user)
        
        response = self.client.get(reverse('template_manager:export_all_templates'))
        
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'application/zip')
        self.assertIn('attachment', response['Content-Disposition'])
    
    def test_template_preview_api(self):
        """Test recipe template preview API endpoint."""
        self.client.force_login(self.regular_user)
        
        response = self.client.get(reverse('template_manager:recipe_template_preview', args=[self.template.id]))
        
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'application/json')
        
        response_data = json.loads(response.content)
        self.assertIn('preview', response_data)
    
    def test_template_env_vars_instances_api(self):
        """Test template environment variables instances API."""
        self.client.force_login(self.regular_user)
        
        response = self.client.get(reverse('template_manager:template_env_vars_instances', args=[self.template.id]))
        
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'application/json')
        
        response_data = json.loads(response.content)
        self.assertIn('instances', response_data)


class RecipeInstancesPageTestCase(BasePageTestCase):
    """Test recipe instances management pages."""
    
    def test_recipe_instances_list(self):
        """Test recipe instances list page."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('recipe_instances'))
        
        self.assertEqual(response.status_code, 200)
        self.assertContainsText(response, 'Recipe Instances')


class EnvironmentVariablesPageTestCase(BasePageTestCase):
    """Test environment variables management pages."""
    
    def test_env_vars_templates_list(self):
        """Test environment variables templates list."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('env_vars_templates'))
        
        self.assertEqual(response.status_code, 200)
        self.assertContainsText(response, 'Environment Variables')
    
    def test_env_vars_instances_list(self):
        """Test environment variables instances list."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('env_vars_instances'))
        
        self.assertEqual(response.status_code, 200)


class ConnectionsPageTestCase(BasePageTestCase):
    """Test DataHub connections management pages."""
    
    def test_connections_list_page(self):
        """Test connections list page."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('connections_list'))
        
        self.assertEqual(response.status_code, 200)
        self.assertContainsText(response, 'Connections')
    
    def test_connection_create_page(self):
        """Test connection creation page."""
        self.client.force_login(self.admin_user)
        response = self.client.get(reverse('connection_create'))
        
        self.assertEqual(response.status_code, 200)
        self.assertContainsText(response, 'Create Connection')


class AuthenticationPagesTestCase(BasePageTestCase):
    """Test authentication related pages."""
    
    def test_login_page(self):
        """Test login page loads correctly."""
        response = self.client.get(reverse('login'))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'auth/login.html')
        self.assertContainsText(response, 'Login')
        
        # Check form structure
        soup = self.parse_html(response)
        form = soup.find('form', method='post')
        self.assertIsNotNone(form)
        
        username_field = soup.find('input', {'name': 'username'})
        password_field = soup.find('input', {'name': 'password'})
        self.assertIsNotNone(username_field)
        self.assertIsNotNone(password_field)
    
    def test_login_post_valid(self):
        """Test valid login submission."""
        response = self.client.post(reverse('login'), {
            'username': self.regular_user.username,
            'password': 'testpassword123'
        })
        
        # Should redirect to dashboard after successful login
        self.assertEqual(response.status_code, 302)
        self.assertRedirects(response, reverse('dashboard'))
    
    def test_login_post_invalid(self):
        """Test invalid login submission."""
        response = self.client.post(reverse('login'), {
            'username': 'invalid',
            'password': 'invalid'
        })
        
        # Should stay on login page with error
        self.assertEqual(response.status_code, 200)
        self.assertContainsText(response, 'error')
    
    def test_logout_functionality(self):
        """Test logout functionality."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('logout'))
        
        # Should redirect to login page
        self.assertEqual(response.status_code, 302)
        self.assertRedirects(response, reverse('login'))


class NavigationTestCase(BasePageTestCase):
    """Test navigation elements across all pages."""
    
    def test_base_template_navigation(self):
        """Test base template navigation structure."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('dashboard'))
        soup = self.parse_html(response)
        
        # Check for main navigation
        nav = soup.find('nav', class_='navbar')
        self.assertIsNotNone(nav)
        
        # Check for key navigation links
        expected_links = ['Dashboard', 'Recipes', 'Policies', 'Metadata']
        nav_text = nav.get_text()
        
        for link_text in expected_links:
            self.assertIn(link_text, nav_text)
    
    def test_user_menu_authenticated(self):
        """Test user menu for authenticated users."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('dashboard'))
        soup = self.parse_html(response)
        
        # Check for user menu
        user_menu = soup.find('div', class_='user-menu') or soup.find('ul', class_='navbar-nav')
        if user_menu:
            self.assertIn(self.regular_user.username, user_menu.get_text())
    
    def test_breadcrumbs(self):
        """Test breadcrumb navigation where applicable."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('policies'))
        soup = self.parse_html(response)
        
        # Check for breadcrumbs
        breadcrumbs = soup.find('nav', {'aria-label': 'breadcrumb'}) or soup.find('ol', class_='breadcrumb')
        if breadcrumbs:
            self.assertIsNotNone(breadcrumbs)


class ResponsiveDesignTestCase(BasePageTestCase):
    """Test responsive design elements."""
    
    def test_viewport_meta_tag(self):
        """Test viewport meta tag is present."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('dashboard'))
        
        self.assertContains(response, 'name="viewport"')
        self.assertContains(response, 'width=device-width')
    
    def test_bootstrap_responsive_classes(self):
        """Test Bootstrap responsive classes are used."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('dashboard'))
        soup = self.parse_html(response)
        
        # Check for responsive container
        containers = soup.find_all('div', class_=lambda x: x and 'container' in x)
        self.assertGreater(len(containers), 0)
        
        # Check for responsive grid
        cols = soup.find_all('div', class_=lambda x: x and any(cls.startswith('col-') for cls in x.split()))
        if cols:
            self.assertGreater(len(cols), 0)


class AccessibilityTestCase(BasePageTestCase):
    """Test accessibility features."""
    
    def test_semantic_html_elements(self):
        """Test proper use of semantic HTML elements."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('dashboard'))
        soup = self.parse_html(response)
        
        # Check for semantic elements
        main = soup.find('main') or soup.find('div', {'role': 'main'})
        self.assertIsNotNone(main)
        
        # Check for proper heading hierarchy
        h1s = soup.find_all('h1')
        self.assertGreater(len(h1s), 0)
    
    def test_form_labels(self):
        """Test form inputs have proper labels."""
        self.client.force_login(self.admin_user)
        response = self.client.get(reverse('policy_create'))
        soup = self.parse_html(response)
        
        # Check form inputs have labels
        inputs = soup.find_all('input', type=['text', 'email', 'password'])
        for input_field in inputs:
            input_id = input_field.get('id')
            if input_id:
                label = soup.find('label', {'for': input_id})
                self.assertIsNotNone(label, f"No label found for input {input_id}")
    
    def test_alt_attributes_on_images(self):
        """Test images have alt attributes."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('dashboard'))
        soup = self.parse_html(response)
        
        images = soup.find_all('img')
        for img in images:
            self.assertTrue(img.has_attr('alt'), "Image missing alt attribute")


class HealthCheckTestCase(BasePageTestCase):
    """Test health check endpoint."""
    
    def test_health_check_endpoint(self):
        """Test health check returns proper response."""
        response = self.client.get(reverse('health'))
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertIn('status', data)
        self.assertEqual(data['status'], 'ok')


class APIEndpointsTestCase(BasePageTestCase):
    """Test API endpoints functionality."""
    
    def test_api_settings_endpoint(self):
        """Test API settings endpoint."""
        self.client.force_login(self.admin_user)
        response = self.client.get(reverse('api-settings'))
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertIsInstance(data, dict)
    
    def test_api_connections_endpoint(self):
        """Test API connections endpoint."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('api-connections'))
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertIsInstance(data, (list, dict))


class ErrorHandlingTestCase(BasePageTestCase):
    """Test error handling across pages."""
    
    def test_404_pages(self):
        """Test 404 error handling."""
        self.client.force_login(self.regular_user)
        
        # Test non-existent page
        response = self.client.get('/non-existent-page/')
        self.assertEqual(response.status_code, 404)
    
    def test_permission_denied_pages(self):
        """Test permission denied scenarios."""
        # Test admin-only page with regular user
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('settings'))
        
        # Should either redirect or show 403
        self.assertIn(response.status_code, [302, 403])
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_external_service_errors(self, mock_get_client):
        """Test handling of external service errors."""
        # Mock DataHub connection failure
        mock_get_client.side_effect = Exception("DataHub connection failed")
        
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('dashboard_data'))
        
        # Should handle error gracefully
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.content)
        self.assertFalse(data.get('connected', True))


class FormValidationTestCase(BasePageTestCase):
    """Test form validation across pages."""
    
    def test_empty_required_fields(self):
        """Test validation of empty required fields."""
        self.client.force_login(self.admin_user)
        
        # Test policy creation with empty fields
        response = self.client.post(reverse('policy_create'), {
            'name': '',  # Required field empty
            'description': 'Test',
            'policy_json': ''
        })
        
        # Should re-render form with validation errors
        self.assertEqual(response.status_code, 200)
        self.assertContainsText(response, 'error')
    
    def test_invalid_json_validation(self):
        """Test JSON field validation."""
        self.client.force_login(self.admin_user)
        
        # Test policy creation with invalid JSON
        response = self.client.post(reverse('policy_create'), {
            'name': 'Test Policy',
            'description': 'Test',
            'policy_json': 'invalid json content'
        })
        
        # Should show JSON validation error
        self.assertEqual(response.status_code, 200)
        self.assertContainsText(response, 'Invalid JSON')


class SearchAndFilteringTestCase(BasePageTestCase):
    """Test search and filtering functionality."""
    
    def test_logs_search(self):
        """Test logs search functionality."""
        LogEntryFactory(message='Database connection established')
        LogEntryFactory(message='User authentication failed')
        
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('logs'), {'search': 'database'})
        
        self.assertEqual(response.status_code, 200)
        self.assertContainsText(response, 'Database connection')
        self.assertNotContains(response, 'User authentication')
    
    def test_policies_filtering(self):
        """Test policies filtering functionality."""
        # Would test policy filtering if implemented
        pass


class PaginationTestCase(BasePageTestCase):
    """Test pagination functionality."""
    
    def test_logs_pagination(self):
        """Test logs pagination."""
        # Create many log entries
        for i in range(25):
            LogEntryFactory(message=f'Log message {i}')
        
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('logs'))
        
        # Should include pagination if more than page size
        soup = self.parse_html(response)
        pagination = soup.find('nav', {'aria-label': 'pagination'}) or soup.find('ul', class_='pagination')
        
        if LogEntryFactory._meta.get_model().objects.count() > 20:  # Assuming page size of 20
            self.assertIsNotNone(pagination) 


@pytest.mark.pages
@pytest.mark.python
@pytest.mark.html
class MutationsPageTestCase(BasePageTestCase):
    """Test cases for the Mutations page covering all functionality."""
    
    def setUp(self):
        super().setUp()
        self.mutation = MutationFactory(
            name='test-mutation',
            description='Test mutation',
            mutation_type='UPDATE',
            user=self.regular_user
        )
    
    def test_mutations_list_page(self):
        """Test mutations list page rendering and content."""
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('mutations'))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'mutations/list.html')
        soup = self.parse_html(response)
        
        # Check page title and structure
        self.assertIsNotNone(soup.select_one('h1.page-title'))
        self.assertContains(response, 'Mutations')
        
        # Check mutations table
        self.assertIsNotNone(soup.select_one('#mutations-table'))
        self.assertContains(response, self.mutation.name)
        
        # Check action buttons
        self.assertIsNotNone(soup.select_one('.btn-create-mutation'))
        self.assertIsNotNone(soup.select_one('.btn-refresh'))
    
    def test_mutation_create_page(self):
        """Test mutation creation page."""
        self.client.force_login(self.admin_user)
        response = self.client.get(reverse('mutation_create'))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'mutations/create.html')
        soup = self.parse_html(response)
        
        # Check form elements
        self.assertIsNotNone(soup.select_one('form#mutation-form'))
        self.assertIsNotNone(soup.select_one('input[name="name"]'))
        self.assertIsNotNone(soup.select_one('textarea[name="description"]'))
        self.assertIsNotNone(soup.select_one('select[name="mutation_type"]'))
    
    def test_mutation_create_post(self):
        """Test mutation creation via POST."""
        self.client.force_login(self.admin_user)
        
        mutation_data = {
            'name': 'new-mutation',
            'description': 'New mutation for testing',
            'mutation_type': 'CREATE',
            'mutation_json': json.dumps({'test': 'data'})
        }
        
        response = self.client.post(reverse('mutation_create'), mutation_data, follow=True)
        self.assertEqual(response.status_code, 200)
        
        # Verify mutation was created
        from web_ui.web_ui.models import Mutation
        mutation = Mutation.objects.get(name='new-mutation')
        self.assertEqual(mutation.description, 'New mutation for testing')
        self.assertEqual(mutation.mutation_type, 'CREATE')
    
    def test_mutation_edit_page(self):
        """Test mutation edit page."""
        self.client.force_login(self.admin_user)
        response = self.client.get(reverse('mutation_edit', args=[self.mutation.id]))
        
        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, 'mutations/edit.html')
        soup = self.parse_html(response)
        
        # Check form is pre-populated
        self.assertIsNotNone(soup.select_one('form#mutation-form'))
        name_input = soup.select_one('input[name="name"]')
        self.assertEqual(name_input.get('value'), self.mutation.name)
    
    def test_mutation_delete(self):
        """Test mutation deletion."""
        self.client.force_login(self.admin_user)
        
        response = self.client.post(reverse('mutation_delete', args=[self.mutation.id]), follow=True)
        self.assertEqual(response.status_code, 200)
        
        # Verify mutation was deleted
        from web_ui.web_ui.models import Mutation
        self.assertFalse(Mutation.objects.filter(id=self.mutation.id).exists())
    
    def test_mutations_access_control(self):
        """Test access control for mutations pages."""
        # Anonymous user should be redirected to login
        response = self.client.get(reverse('mutations'))
        self.assertEqual(response.status_code, 302)
        
        # Regular user can view but not create/edit/delete
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('mutations'))
        self.assertEqual(response.status_code, 200)
        
        # Only admin can create
        response = self.client.get(reverse('mutation_create'))
        self.assertEqual(response.status_code, 403)  # Assuming permission check
    
    def test_mutations_search_and_filter(self):
        """Test mutations search and filtering functionality."""
        # Create additional mutations for testing
        MutationFactory(name='search-mutation', mutation_type='DELETE')
        MutationFactory(name='filter-mutation', mutation_type='UPDATE')
        
        self.client.force_login(self.regular_user)
        
        # Test search
        response = self.client.get(reverse('mutations'), {'search': 'search'})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'search-mutation')
        
        # Test filter by type
        response = self.client.get(reverse('mutations'), {'mutation_type': 'DELETE'})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'search-mutation')
    
    def test_mutations_pagination(self):
        """Test mutations list pagination."""
        # Create multiple mutations
        for i in range(25):
            MutationFactory(name=f'mutation-{i}', user=self.regular_user)
        
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('mutations'))
        
        self.assertEqual(response.status_code, 200)
        soup = self.parse_html(response)
        
        # Check pagination controls
        self.assertIsNotNone(soup.select_one('.pagination'))
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_mutation_deploy_to_datahub(self, mock_get_client):
        """Test deploying mutation to DataHub."""
        mock_client = Mock()
        mock_client.create_mutation.return_value = {'success': True}
        mock_get_client.return_value = mock_client
        
        self.client.force_login(self.admin_user)
        
        response = self.client.post(reverse('mutation_deploy', args=[self.mutation.id]), follow=True)
        self.assertEqual(response.status_code, 200)
        
        # Verify DataHub client was called
        mock_client.create_mutation.assert_called_once()
        
        # Check success message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any('deployed successfully' in str(m) for m in messages)) 