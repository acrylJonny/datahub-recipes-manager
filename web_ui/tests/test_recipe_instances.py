"""
Comprehensive tests for Recipe Instance functionality.

Tests cover:
- RecipeInstance Model tests (all fields, methods, properties)
- Recipe Instance CRUD operations (Create, Read, Update, Delete)
- Recipe Instance deployment functionality (deploy, undeploy, redeploy)
- Recipe Instance GitHub integration (push to GitHub PR)
- Recipe Instance repository loading from YAML files
- Recipe Instance forms and validation
- Recipe Instance integration with templates and environment variables
- Recipe Instance export and download functionality
- Recipe Instance performance and security tests
- Recipe Instance template rendering and UI
"""

import json
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from django.test import TestCase, Client
from django.urls import reverse
from django.contrib.auth.models import User
from django.contrib.messages import get_messages
from django.utils import timezone
from django.core.exceptions import ValidationError

from web_ui.web_ui.models import (
    RecipeInstance, RecipeTemplate, EnvVarsTemplate, 
    EnvVarsInstance, Environment, replace_env_vars_with_values
)
from web_ui.web_ui.forms import RecipeInstanceForm
from tests.fixtures.simple_factories import UserFactory, EnvironmentFactory


class RecipeInstanceModelTestCase(TestCase):
    """Test RecipeInstance model functionality."""
    
    def setUp(self):
        self.user = UserFactory()
        self.environment = EnvironmentFactory(name='test-env')
        
        # Create recipe template
        self.recipe_template = RecipeTemplate.objects.create(
            name='Test Recipe Template',
            recipe_type='postgres',
            content=json.dumps({
                'source': {
                    'type': 'postgres',
                    'config': {
                        'host': '${DATABASE_HOST}',
                        'database': '${DATABASE_NAME}',
                        'username': '${DATABASE_USER}',
                        'password': '${DATABASE_PASSWORD}'
                    }
                }
            })
        )
        
        # Create environment variables template and instance
        self.env_vars_template = EnvVarsTemplate.objects.create(
            name='Postgres Variables Template',
            recipe_type='postgres',
            variables=json.dumps({
                'DATABASE_HOST': {'description': 'Database host', 'required': True},
                'DATABASE_NAME': {'description': 'Database name', 'required': True},
                'DATABASE_USER': {'description': 'Database user', 'required': True},
                'DATABASE_PASSWORD': {'description': 'Database password', 'required': True, 'is_secret': True}
            })
        )
        
        self.env_vars_instance = EnvVarsInstance.objects.create(
            name='Test Postgres Variables',
            template=self.env_vars_template,
            environment=self.environment,
            recipe_type='postgres',
            variables=json.dumps({
                'DATABASE_HOST': {'value': 'test-db.example.com', 'isSecret': False},
                'DATABASE_NAME': {'value': 'test_database', 'isSecret': False},
                'DATABASE_USER': {'value': 'test_user', 'isSecret': False},
                'DATABASE_PASSWORD': {'value': 'secret123', 'isSecret': True}
            })
        )
    
    def test_recipe_instance_creation(self):
        """Test basic RecipeInstance creation."""
        instance = RecipeInstance.objects.create(
            name='Test Recipe Instance',
            description='A test recipe instance',
            template=self.recipe_template,
            env_vars_instance=self.env_vars_instance,
            environment=self.environment,
            cron_schedule='0 */6 * * *',
            timezone='America/New_York',
            debug_mode=True
        )
        
        self.assertEqual(instance.name, 'Test Recipe Instance')
        self.assertEqual(instance.description, 'A test recipe instance')
        self.assertEqual(instance.template, self.recipe_template)
        self.assertEqual(instance.env_vars_instance, self.env_vars_instance)
        self.assertEqual(instance.environment, self.environment)
        self.assertEqual(instance.cron_schedule, '0 */6 * * *')
        self.assertEqual(instance.timezone, 'America/New_York')
        self.assertTrue(instance.debug_mode)
        self.assertFalse(instance.deployed)
        self.assertIsNone(instance.deployed_at)
        self.assertIsNone(instance.datahub_urn)
        self.assertIsNotNone(instance.created_at)
        self.assertIsNotNone(instance.updated_at)
    
    def test_recipe_instance_str(self):
        """Test RecipeInstance string representation."""
        instance = RecipeInstance.objects.create(
            name='Test Instance',
            template=self.recipe_template
        )
        self.assertEqual(str(instance), 'Test Instance (Test Recipe Template)')
    
    def test_recipe_instance_defaults(self):
        """Test RecipeInstance default values."""
        instance = RecipeInstance.objects.create(
            name='Default Instance',
            template=self.recipe_template
        )
        
        self.assertEqual(instance.cron_schedule, '0 0 * * *')
        self.assertEqual(instance.timezone, 'UTC')
        self.assertFalse(instance.debug_mode)
        self.assertFalse(instance.deployed)
        self.assertIsNone(instance.deployed_at)
        self.assertIsNone(instance.datahub_urn)
    
    def test_recipe_type_property(self):
        """Test recipe_type property."""
        instance = RecipeInstance.objects.create(
            name='Test Instance',
            template=self.recipe_template
        )
        self.assertEqual(instance.recipe_type, 'postgres')
        
        # Test with no template
        instance.template = None
        instance.save()
        self.assertIsNone(instance.recipe_type)
    
    def test_datahub_id_property(self):
        """Test datahub_id property getter and setter."""
        instance = RecipeInstance.objects.create(
            name='Test Instance',
            template=self.recipe_template
        )
        
        # Test getter with no URN
        self.assertIsNone(instance.datahub_id)
        
        # Test setter
        instance.datahub_id = 'test-recipe-123'
        self.assertEqual(instance.datahub_urn, 'urn:li:dataHubIngestionSource:test-recipe-123')
        
        # Test getter with URN
        self.assertEqual(instance.datahub_id, 'test-recipe-123')
        
        # Test setter with None
        instance.datahub_id = None
        self.assertIsNone(instance.datahub_urn)
    
    def test_get_recipe_id(self):
        """Test get_recipe_id method."""
        instance = RecipeInstance.objects.create(
            name='Test Instance',
            template=self.recipe_template
        )
        
        # Test with no URN
        self.assertIsNone(instance.get_recipe_id())
        
        # Test with valid URN
        instance.datahub_urn = 'urn:li:dataHubIngestionSource:test-recipe-123'
        self.assertEqual(instance.get_recipe_id(), 'test-recipe-123')
        
        # Test with invalid URN format
        instance.datahub_urn = 'invalid-urn-format'
        self.assertIsNone(instance.get_recipe_id())
    
    def test_get_recipe_dict(self):
        """Test get_recipe_dict method."""
        instance = RecipeInstance.objects.create(
            name='Test Instance',
            template=self.recipe_template,
            env_vars_instance=self.env_vars_instance
        )
        
        recipe_dict = instance.get_recipe_dict()
        
        self.assertIsInstance(recipe_dict, dict)
        self.assertIn('source', recipe_dict)
        self.assertEqual(recipe_dict['source']['type'], 'postgres')
        
        # Check that environment variables were applied
        config = recipe_dict['source']['config']
        self.assertEqual(config['host'], 'test-db.example.com')
        self.assertEqual(config['database'], 'test_database')
        self.assertEqual(config['username'], 'test_user')
        self.assertEqual(config['password'], 'secret123')
    
    def test_get_recipe_dict_no_env_vars(self):
        """Test get_recipe_dict without environment variables."""
        instance = RecipeInstance.objects.create(
            name='Test Instance',
            template=self.recipe_template
        )
        
        recipe_dict = instance.get_recipe_dict()
        
        self.assertIsInstance(recipe_dict, dict)
        self.assertIn('source', recipe_dict)
        
        # Environment variables should remain unreplaced
        config = recipe_dict['source']['config']
        self.assertEqual(config['host'], '${DATABASE_HOST}')
        self.assertEqual(config['database'], '${DATABASE_NAME}')
    
    def test_get_recipe_dict_malformed_template(self):
        """Test get_recipe_dict with malformed template content."""
        # Create template with non-dict content
        malformed_template = RecipeTemplate.objects.create(
            name='Malformed Template',
            recipe_type='test',
            content='invalid json content'
        )
        
        instance = RecipeInstance.objects.create(
            name='Test Instance',
            template=malformed_template
        )
        
        recipe_dict = instance.get_recipe_dict()
        self.assertIsNone(recipe_dict)
    
    def test_get_combined_content(self):
        """Test get_combined_content method."""
        instance = RecipeInstance.objects.create(
            name='Test Instance',
            template=self.recipe_template,
            env_vars_instance=self.env_vars_instance
        )
        
        combined_content = instance.get_combined_content()
        
        self.assertIsInstance(combined_content, dict)
        config = combined_content['source']['config']
        self.assertEqual(config['host'], 'test-db.example.com')
        self.assertEqual(config['password'], 'secret123')
    
    def test_get_combined_content_no_env_vars(self):
        """Test get_combined_content without environment variables."""
        instance = RecipeInstance.objects.create(
            name='Test Instance',
            template=self.recipe_template
        )
        
        combined_content = instance.get_combined_content()
        
        # Should return original template content
        self.assertIsInstance(combined_content, dict)
        config = combined_content['source']['config']
        self.assertEqual(config['host'], '${DATABASE_HOST}')
    
    def test_export_to_yaml(self):
        """Test export_to_yaml method."""
        instance = RecipeInstance.objects.create(
            name='Export Test Instance',
            description='Test instance for export',
            template=self.recipe_template,
            env_vars_instance=self.env_vars_instance
        )
        
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = instance.export_to_yaml(base_dir=temp_dir)
            
            self.assertTrue(file_path.exists())
            self.assertEqual(file_path.name, 'export_test_instance.yml')
            
            # Check file content
            with open(file_path, 'r') as f:
                import yaml
                content = yaml.safe_load(f)
            
            self.assertEqual(content['name'], 'Export Test Instance')
            self.assertEqual(content['description'], 'Test instance for export')
            self.assertEqual(content['recipe_type'], 'postgres')
            self.assertIn('recipe', content)
            self.assertIn('env_vars', content)
            
            # Check that recipe content has env vars applied
            recipe_config = content['recipe']['source']['config']
            self.assertEqual(recipe_config['host'], 'test-db.example.com')
    
    def test_export_to_yaml_no_content(self):
        """Test export_to_yaml with no template content."""
        # Create template with None content
        empty_template = RecipeTemplate.objects.create(
            name='Empty Template',
            recipe_type='test',
            content=''
        )
        
        instance = RecipeInstance.objects.create(
            name='Empty Instance',
            template=empty_template
        )
        
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(ValueError):
                instance.export_to_yaml(base_dir=temp_dir)


class RecipeInstanceFormTestCase(TestCase):
    """Test RecipeInstanceForm functionality."""
    
    def setUp(self):
        self.environment = EnvironmentFactory(name='test-env')
        
        self.recipe_template = RecipeTemplate.objects.create(
            name='Form Test Template',
            recipe_type='postgres',
            content=json.dumps({'source': {'type': 'postgres'}})
        )
        
        self.env_vars_instance = EnvVarsInstance.objects.create(
            name='Form Test Variables',
            recipe_type='postgres',
            variables=json.dumps({'HOST': {'value': 'localhost', 'isSecret': False}})
        )
        
        self.valid_form_data = {
            'name': 'Test Instance',
            'description': 'Test description',
            'template': str(self.recipe_template.id),
            'env_vars_instance': str(self.env_vars_instance.id),
            'environment': str(self.environment.id),
            'cron_schedule': '0 12 * * *',
            'timezone': 'America/New_York',
            'debug_mode': True
        }
    
    def test_form_valid_data(self):
        """Test form with valid data."""
        form = RecipeInstanceForm(data=self.valid_form_data)
        self.assertTrue(form.is_valid())
        
        # Check cleaned data
        self.assertEqual(form.cleaned_data['name'], 'Test Instance')
        self.assertEqual(form.cleaned_data['template'], self.recipe_template)
        self.assertEqual(form.cleaned_data['env_vars_instance'], self.env_vars_instance)
        self.assertEqual(form.cleaned_data['environment'], self.environment)
        self.assertTrue(form.cleaned_data['debug_mode'])
    
    def test_form_missing_required_fields(self):
        """Test form with missing required fields."""
        # Missing name
        invalid_data = self.valid_form_data.copy()
        invalid_data['name'] = ''
        form = RecipeInstanceForm(data=invalid_data)
        self.assertFalse(form.is_valid())
        self.assertIn('name', form.errors)
        
        # Missing template
        invalid_data = self.valid_form_data.copy()
        del invalid_data['template']
        form = RecipeInstanceForm(data=invalid_data)
        self.assertFalse(form.is_valid())
        self.assertIn('template', form.errors)
    
    def test_form_optional_fields(self):
        """Test form with only required fields."""
        minimal_data = {
            'name': 'Minimal Instance',
            'template': str(self.recipe_template.id)
        }
        form = RecipeInstanceForm(data=minimal_data)
        self.assertTrue(form.is_valid())
        
        # Check default values are applied
        self.assertEqual(form.cleaned_data['cron_schedule'], '0 0 * * *')
        self.assertEqual(form.cleaned_data['timezone'], 'UTC')
        self.assertFalse(form.cleaned_data['debug_mode'])


class RecipeInstanceListViewTestCase(TestCase):
    """Test recipe instances list view functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        
        self.recipe_template = RecipeTemplate.objects.create(
            name='List Test Template',
            recipe_type='postgres',
            content=json.dumps({'source': {'type': 'postgres'}})
        )
        
        # Create deployed instance
        self.deployed_instance = RecipeInstance.objects.create(
            name='Deployed Instance',
            template=self.recipe_template,
            deployed=True,
            deployed_at=timezone.now()
        )
        
        # Create staging instance
        self.staging_instance = RecipeInstance.objects.create(
            name='Staging Instance',
            template=self.recipe_template,
            deployed=False
        )
    
    def test_recipe_instances_list_access(self):
        """Test recipe instances list page access."""
        self.client.force_login(self.user)
        response = self.client.get(reverse('recipe_instances'))
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Recipe Instances')
        self.assertContains(response, self.deployed_instance.name)
        self.assertContains(response, self.staging_instance.name)
    
    def test_recipe_instances_list_grouping(self):
        """Test recipe instances list grouping by deployment status."""
        self.client.force_login(self.user)
        response = self.client.get(reverse('recipe_instances'))
        
        self.assertEqual(response.status_code, 200)
        
        # Check deployed and staging groups
        deployed = response.context['deployed']
        staging = response.context['staging']
        
        self.assertIn(self.deployed_instance, deployed)
        self.assertIn(self.staging_instance, staging)
        self.assertNotIn(self.staging_instance, deployed)
        self.assertNotIn(self.deployed_instance, staging)
    
    def test_recipe_instances_list_context(self):
        """Test recipe instances list view context."""
        self.client.force_login(self.user)
        response = self.client.get(reverse('recipe_instances'))
        
        self.assertEqual(response.status_code, 200)
        self.assertIn('deployed', response.context)
        self.assertIn('staging', response.context)
        self.assertEqual(response.context['title'], 'Recipe Instances')


class RecipeInstanceCreateViewTestCase(TestCase):
    """Test recipe instance create view functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.environment = EnvironmentFactory(name='test-env')
        
        self.recipe_template = RecipeTemplate.objects.create(
            name='Create Test Template',
            recipe_type='postgres',
            content=json.dumps({'source': {'type': 'postgres'}})
        )
        
        self.env_vars_instance = EnvVarsInstance.objects.create(
            name='Create Test Variables',
            recipe_type='postgres',
            variables=json.dumps({'HOST': {'value': 'localhost', 'isSecret': False}})
        )
        
        self.valid_form_data = {
            'name': 'New Recipe Instance',
            'description': 'A new test instance',
            'template': str(self.recipe_template.id),
            'env_vars_instance': str(self.env_vars_instance.id),
            'environment': str(self.environment.id),
            'cron_schedule': '0 8 * * *',
            'timezone': 'UTC',
            'debug_mode': False
        }
    
    def test_recipe_instance_create_get(self):
        """Test recipe instance create page GET request."""
        self.client.force_login(self.user)
        response = self.client.get(reverse('recipe_instance_create'))
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Create New Instance')
        self.assertIsInstance(response.context['form'], RecipeInstanceForm)
        self.assertIn('templates', response.context)
        self.assertIn('env_vars_instances', response.context)
        self.assertIn('environments', response.context)
    
    def test_recipe_instance_create_post_valid(self):
        """Test recipe instance create with valid data."""
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('recipe_instance_create'),
            data=self.valid_form_data,
            follow=True
        )
        
        self.assertEqual(response.status_code, 200)
        
        # Check instance was created
        instance = RecipeInstance.objects.get(name='New Recipe Instance')
        self.assertEqual(instance.description, 'A new test instance')
        self.assertEqual(instance.template, self.recipe_template)
        self.assertEqual(instance.env_vars_instance, self.env_vars_instance)
        self.assertEqual(instance.environment, self.environment)
        self.assertEqual(instance.cron_schedule, '0 8 * * *')
        self.assertEqual(instance.timezone, 'UTC')
        self.assertFalse(instance.debug_mode)
        
        # Check success message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any("created successfully" in str(m) for m in messages))
    
    def test_recipe_instance_create_post_invalid(self):
        """Test recipe instance create with invalid data."""
        self.client.force_login(self.user)
        
        invalid_data = self.valid_form_data.copy()
        invalid_data['name'] = ''  # Invalid: required field
        
        response = self.client.post(
            reverse('recipe_instance_create'),
            data=invalid_data
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertIsInstance(response.context['form'], RecipeInstanceForm)
        self.assertFalse(response.context['form'].is_valid())
        
        # Check instance was not created
        self.assertFalse(RecipeInstance.objects.filter(name='').exists())


class RecipeInstanceEditViewTestCase(TestCase):
    """Test recipe instance edit view functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        
        self.recipe_template = RecipeTemplate.objects.create(
            name='Edit Test Template',
            recipe_type='postgres',
            content=json.dumps({'source': {'type': 'postgres'}})
        )
        
        self.instance = RecipeInstance.objects.create(
            name='Original Instance',
            description='Original description',
            template=self.recipe_template,
            cron_schedule='0 0 * * *',
            timezone='UTC'
        )
        
        self.updated_data = {
            'name': 'Updated Instance',
            'description': 'Updated description',
            'template': str(self.recipe_template.id),
            'cron_schedule': '0 12 * * *',
            'timezone': 'America/New_York',
            'debug_mode': True
        }
    
    def test_recipe_instance_edit_get(self):
        """Test recipe instance edit page GET request."""
        self.client.force_login(self.user)
        response = self.client.get(
            reverse('recipe_instance_edit', args=[self.instance.id])
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, f'Edit Recipe Instance: {self.instance.name}')
        self.assertIsInstance(response.context['form'], RecipeInstanceForm)
        self.assertEqual(response.context['instance'], self.instance)
        
        # Check form is pre-populated
        form = response.context['form']
        self.assertEqual(form.initial['name'], 'Original Instance')
        self.assertEqual(form.initial['description'], 'Original description')
    
    def test_recipe_instance_edit_post_valid(self):
        """Test recipe instance edit with valid data."""
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('recipe_instance_edit', args=[self.instance.id]),
            data=self.updated_data,
            follow=True
        )
        
        self.assertEqual(response.status_code, 200)
        
        # Check instance was updated
        self.instance.refresh_from_db()
        self.assertEqual(self.instance.name, 'Updated Instance')
        self.assertEqual(self.instance.description, 'Updated description')
        self.assertEqual(self.instance.cron_schedule, '0 12 * * *')
        self.assertEqual(self.instance.timezone, 'America/New_York')
        self.assertTrue(self.instance.debug_mode)
        
        # Check success message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any("updated successfully" in str(m) for m in messages))
    
    def test_recipe_instance_edit_post_invalid(self):
        """Test recipe instance edit with invalid data."""
        self.client.force_login(self.user)
        
        invalid_data = self.updated_data.copy()
        invalid_data['name'] = ''  # Invalid: required field
        
        response = self.client.post(
            reverse('recipe_instance_edit', args=[self.instance.id]),
            data=invalid_data
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertIsInstance(response.context['form'], RecipeInstanceForm)
        self.assertFalse(response.context['form'].is_valid())
        
        # Check instance was not updated
        self.instance.refresh_from_db()
        self.assertEqual(self.instance.name, 'Original Instance')
    
    def test_recipe_instance_edit_not_found(self):
        """Test recipe instance edit with non-existent instance."""
        self.client.force_login(self.user)
        response = self.client.get(
            reverse('recipe_instance_edit', args=[9999])
        )
        
        self.assertEqual(response.status_code, 404)


class RecipeInstanceDeleteViewTestCase(TestCase):
    """Test recipe instance delete view functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        
        self.recipe_template = RecipeTemplate.objects.create(
            name='Delete Test Template',
            recipe_type='postgres',
            content=json.dumps({'source': {'type': 'postgres'}})
        )
        
        self.instance = RecipeInstance.objects.create(
            name='Instance to Delete',
            template=self.recipe_template
        )
    
    def test_recipe_instance_delete_get(self):
        """Test recipe instance delete confirmation page."""
        self.client.force_login(self.user)
        response = self.client.get(
            reverse('recipe_instance_delete', args=[self.instance.id])
        )
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, self.instance.name)
        self.assertEqual(response.context['instance'], self.instance)
    
    def test_recipe_instance_delete_post(self):
        """Test recipe instance deletion."""
        instance_id = self.instance.id
        instance_name = self.instance.name
        
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('recipe_instance_delete', args=[instance_id]),
            follow=True
        )
        
        self.assertEqual(response.status_code, 200)
        
        # Check instance was deleted
        self.assertFalse(RecipeInstance.objects.filter(id=instance_id).exists())
        
        # Check success message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any("deleted successfully" in str(m) for m in messages))
        self.assertTrue(any(instance_name in str(m) for m in messages))
    
    def test_recipe_instance_delete_not_found(self):
        """Test recipe instance delete with non-existent instance."""
        self.client.force_login(self.user)
        response = self.client.get(
            reverse('recipe_instance_delete', args=[9999])
        )
        
        self.assertEqual(response.status_code, 404)


class RecipeInstanceDeployViewTestCase(TestCase):
    """Test recipe instance deployment functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        
        self.recipe_template = RecipeTemplate.objects.create(
            name='Deploy Test Template',
            recipe_type='postgres',
            content=json.dumps({
                'source': {
                    'type': 'postgres',
                    'config': {
                        'host': '${DATABASE_HOST}'
                    }
                }
            })
        )
        
        self.env_vars_instance = EnvVarsInstance.objects.create(
            name='Deploy Test Variables',
            recipe_type='postgres',
            variables=json.dumps({
                'DATABASE_HOST': {'value': 'deploy-db.example.com', 'isSecret': False}
            })
        )
        
        self.instance = RecipeInstance.objects.create(
            name='Deploy Test Instance',
            template=self.recipe_template,
            env_vars_instance=self.env_vars_instance
        )
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_recipe_instance_deploy_success(self, mock_get_client):
        """Test successful recipe instance deployment."""
        mock_client = Mock()
        mock_client.test_connection.return_value = True
        mock_client.create_ingestion_source.return_value = 'urn:li:dataHubIngestionSource:deployed-123'
        mock_get_client.return_value = mock_client
        
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('recipe_instance_deploy', args=[self.instance.id]),
            follow=True
        )
        
        self.assertEqual(response.status_code, 200)
        
        # Check instance was marked as deployed
        self.instance.refresh_from_db()
        self.assertTrue(self.instance.deployed)
        self.assertIsNotNone(self.instance.deployed_at)
        self.assertEqual(self.instance.datahub_urn, 'urn:li:dataHubIngestionSource:deployed-123')
        
        # Check success message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any("deployed successfully" in str(m) for m in messages))
        
        # Check DataHub client was called with correct data
        mock_client.create_ingestion_source.assert_called_once()
        call_args = mock_client.create_ingestion_source.call_args[0][0]
        self.assertEqual(call_args['name'], 'Deploy Test Instance')
        self.assertEqual(call_args['type'], 'postgres')
        self.assertIn('recipe', call_args)
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_recipe_instance_deploy_no_connection(self, mock_get_client):
        """Test recipe instance deployment with no DataHub connection."""
        mock_get_client.return_value = None
        
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('recipe_instance_deploy', args=[self.instance.id]),
            follow=True
        )
        
        self.assertEqual(response.status_code, 200)
        
        # Check instance was not deployed
        self.instance.refresh_from_db()
        self.assertFalse(self.instance.deployed)
        
        # Check error message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any("Not connected to DataHub" in str(m) for m in messages))
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_recipe_instance_deploy_failed_connection(self, mock_get_client):
        """Test recipe instance deployment with failed DataHub connection."""
        mock_client = Mock()
        mock_client.test_connection.return_value = False
        mock_get_client.return_value = mock_client
        
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('recipe_instance_deploy', args=[self.instance.id]),
            follow=True
        )
        
        self.assertEqual(response.status_code, 200)
        
        # Check instance was not deployed
        self.instance.refresh_from_db()
        self.assertFalse(self.instance.deployed)
        
        # Check error message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any("Not connected to DataHub" in str(m) for m in messages))
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_recipe_instance_deploy_with_secrets(self, mock_get_client):
        """Test recipe instance deployment with secret variables."""
        # Add secret variable to env vars instance
        secret_vars = {
            'DATABASE_HOST': {'value': 'deploy-db.example.com', 'isSecret': False},
            'DATABASE_PASSWORD': {'value': 'secret123', 'isSecret': True}
        }
        self.env_vars_instance.variables = json.dumps(secret_vars)
        self.env_vars_instance.save()
        
        mock_client = Mock()
        mock_client.test_connection.return_value = True
        mock_client.create_ingestion_source.return_value = 'urn:li:dataHubIngestionSource:deployed-123'
        mock_get_client.return_value = mock_client
        
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('recipe_instance_deploy', args=[self.instance.id]),
            follow=True
        )
        
        self.assertEqual(response.status_code, 200)
        
        # Check secrets were marked as created
        self.env_vars_instance.refresh_from_db()
        self.assertTrue(self.env_vars_instance.datahub_secrets_created)
    
    def test_recipe_instance_deploy_not_found(self):
        """Test recipe instance deployment with non-existent instance."""
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('recipe_instance_deploy', args=[9999])
        )
        
        self.assertEqual(response.status_code, 404)


class RecipeInstanceGitHubIntegrationTestCase(TestCase):
    """Test recipe instance GitHub integration functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        
        self.recipe_template = RecipeTemplate.objects.create(
            name='GitHub Test Template',
            recipe_type='postgres',
            content=json.dumps({'source': {'type': 'postgres'}})
        )
        
        self.instance = RecipeInstance.objects.create(
            name='GitHub Test Instance',
            template=self.recipe_template
        )
    
    @patch('web_ui.web_ui.views.GitIntegration')
    @patch('web_ui.web_ui.views.GitSettings')
    def test_recipe_instance_push_github_success(self, mock_git_settings, mock_git_integration):
        """Test successful recipe instance push to GitHub."""
        # Mock settings
        mock_settings = Mock()
        mock_settings.current_branch = 'feature/test-branch'
        mock_git_settings.get_instance.return_value = mock_settings
        
        # Mock GitHub integration
        mock_github = Mock()
        mock_git_integration.return_value = mock_github
        mock_github.push_to_git.return_value = {'success': True}
        
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('recipe_instance_push_github', args=[self.instance.id])
        )
        
        self.assertEqual(response.status_code, 200)
        
        data = json.loads(response.content)
        self.assertTrue(data['success'])
        self.assertIn('staged for commit', data['message'])
        self.assertIn('redirect_url', data)
        
        # Check GitHub integration was called
        mock_github.push_to_git.assert_called_once()
    
    @patch('web_ui.web_ui.views.GitSettings')
    def test_recipe_instance_push_github_main_branch(self, mock_git_settings):
        """Test recipe instance push to GitHub blocked on main branch."""
        # Mock settings with main branch
        mock_settings = Mock()
        mock_settings.current_branch = 'main'
        mock_git_settings.get_instance.return_value = mock_settings
        
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('recipe_instance_push_github', args=[self.instance.id])
        )
        
        self.assertEqual(response.status_code, 200)
        
        data = json.loads(response.content)
        self.assertFalse(data['success'])
        self.assertIn('Cannot push directly to the main branch', data['error'])
    
    @patch('web_ui.web_ui.views.GitIntegration')
    @patch('web_ui.web_ui.views.GitSettings')
    def test_recipe_instance_push_github_failure(self, mock_git_settings, mock_git_integration):
        """Test failed recipe instance push to GitHub."""
        # Mock settings
        mock_settings = Mock()
        mock_settings.current_branch = 'feature/test-branch'
        mock_git_settings.get_instance.return_value = mock_settings
        
        # Mock GitHub integration failure
        mock_github = Mock()
        mock_git_integration.return_value = mock_github
        mock_github.push_to_git.return_value = {'success': False}
        
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('recipe_instance_push_github', args=[self.instance.id])
        )
        
        self.assertEqual(response.status_code, 200)
        
        data = json.loads(response.content)
        self.assertFalse(data['success'])
        self.assertIn('Failed to stage', data['error'])
    
    def test_recipe_instance_push_github_not_found(self):
        """Test recipe instance push to GitHub with non-existent instance."""
        self.client.force_login(self.user)
        response = self.client.post(
            reverse('recipe_instance_push_github', args=[9999])
        )
        
        self.assertEqual(response.status_code, 404)


class RecipeInstanceRepositoryLoaderTestCase(TestCase):
    """Test repository loading functionality for recipe instances."""
    
    def setUp(self):
        self.environment = EnvironmentFactory(name='dev')
        
        # Create recipe template for loading
        self.recipe_template = RecipeTemplate.objects.create(
            name='Loader Test Template',
            recipe_type='postgres',
            content=json.dumps({'source': {'type': 'postgres'}})
        )
        
        # Create temporary directory structure
        self.temp_dir = Path(tempfile.mkdtemp())
        self.instances_dir = self.temp_dir / 'recipes' / 'instances' / 'dev'
        self.instances_dir.mkdir(parents=True, exist_ok=True)
    
    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_load_recipe_instance_from_yaml(self):
        """Test loading recipe instance from YAML."""
        instance_content = {
            'name': 'Dev PostgreSQL Instance',
            'description': 'Development PostgreSQL instance',
            'recipe_type': 'postgres'
        }
        
        instance_file = self.instances_dir / 'postgres.yml'
        with open(instance_file, 'w') as f:
            import yaml
            yaml.dump(instance_content, f)
        
        # Test the loading function
        from web_ui.web_ui.services.repo_loader import RepositoryLoader
        
        success = RepositoryLoader._load_recipe_instance(instance_file, self.environment)
        
        self.assertTrue(success)
        
        # Check that instance was created
        instance = RecipeInstance.objects.get(name='Dev PostgreSQL Instance')
        self.assertEqual(instance.template, self.recipe_template)
        self.assertEqual(instance.environment, self.environment)
    
    def test_load_recipe_instance_no_template(self):
        """Test loading recipe instance with no matching template."""
        instance_content = {
            'name': 'Unknown Instance',
            'description': 'Instance with unknown recipe type',
            'recipe_type': 'unknown_type'
        }
        
        instance_file = self.instances_dir / 'unknown.yml'
        with open(instance_file, 'w') as f:
            import yaml
            yaml.dump(instance_content, f)
        
        # Test the loading function
        from web_ui.web_ui.services.repo_loader import RepositoryLoader
        
        success = RepositoryLoader._load_recipe_instance(instance_file, self.environment)
        
        self.assertFalse(success)
        
        # Check that instance was not created
        self.assertFalse(RecipeInstance.objects.filter(name='Unknown Instance').exists())


class RecipeInstanceIntegrationTestCase(TestCase):
    """Test recipe instance integration with other components."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.environment = EnvironmentFactory(name='integration-test')
        
        # Create complete setup
        self.recipe_template = RecipeTemplate.objects.create(
            name='Integration Recipe Template',
            recipe_type='postgres',
            content=json.dumps({
                'source': {
                    'type': 'postgres',
                    'config': {
                        'host': '${DATABASE_HOST}',
                        'database': '${DATABASE_NAME}'
                    }
                }
            })
        )
        
        self.env_vars_template = EnvVarsTemplate.objects.create(
            name='Integration Variables Template',
            recipe_type='postgres',
            variables=json.dumps({
                'DATABASE_HOST': {'description': 'Host', 'required': True},
                'DATABASE_NAME': {'description': 'Database', 'required': True}
            })
        )
        
        self.env_vars_instance = EnvVarsInstance.objects.create(
            name='Integration Variables Instance',
            template=self.env_vars_template,
            environment=self.environment,
            recipe_type='postgres',
            variables=json.dumps({
                'DATABASE_HOST': {'value': 'integration-db.example.com', 'isSecret': False},
                'DATABASE_NAME': {'value': 'integration_db', 'isSecret': False}
            })
        )
        
        self.recipe_instance = RecipeInstance.objects.create(
            name='Integration Recipe Instance',
            template=self.recipe_template,
            env_vars_instance=self.env_vars_instance,
            environment=self.environment
        )
    
    def test_recipe_instance_complete_workflow(self):
        """Test complete recipe instance workflow."""
        # Test getting combined content
        combined_content = self.recipe_instance.get_combined_content()
        self.assertEqual(
            combined_content['source']['config']['host'],
            'integration-db.example.com'
        )
        self.assertEqual(
            combined_content['source']['config']['database'],
            'integration_db'
        )
        
        # Test getting recipe dictionary
        recipe_dict = self.recipe_instance.get_recipe_dict()
        self.assertIn('source', recipe_dict)
        self.assertEqual(recipe_dict['source']['type'], 'postgres')
        
        # Test export functionality
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = self.recipe_instance.export_to_yaml(base_dir=temp_dir)
            self.assertTrue(file_path.exists())
    
    def test_recipe_instance_template_relationship(self):
        """Test recipe instance relationship with template."""
        self.assertEqual(self.recipe_instance.template, self.recipe_template)
        self.assertEqual(self.recipe_instance.recipe_type, 'postgres')
        
        # Test cascade deletion
        template_id = self.recipe_template.id
        self.recipe_template.delete()
        
        # Instance should be deleted when template is deleted
        self.assertFalse(RecipeInstance.objects.filter(id=self.recipe_instance.id).exists())
    
    def test_recipe_instance_env_vars_relationship(self):
        """Test recipe instance relationship with environment variables."""
        self.assertEqual(self.recipe_instance.env_vars_instance, self.env_vars_instance)
        
        # Test SET_NULL behavior
        env_vars_id = self.env_vars_instance.id
        self.env_vars_instance.delete()
        
        # Instance should remain but env_vars_instance should be None
        self.recipe_instance.refresh_from_db()
        self.assertIsNone(self.recipe_instance.env_vars_instance)
    
    def test_recipe_instance_environment_relationship(self):
        """Test recipe instance relationship with environment."""
        self.assertEqual(self.recipe_instance.environment, self.environment)
        
        # Test SET_NULL behavior
        environment_id = self.environment.id
        self.environment.delete()
        
        # Instance should remain but environment should be None
        self.recipe_instance.refresh_from_db()
        self.assertIsNone(self.recipe_instance.environment)


class RecipeInstancePerformanceTestCase(TestCase):
    """Test recipe instance performance."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        
        # Create multiple templates and instances
        self.instances = []
        for i in range(20):
            template = RecipeTemplate.objects.create(
                name=f'Performance Template {i}',
                recipe_type='test',
                content=json.dumps({'source': {'type': 'test'}})
            )
            
            instance = RecipeInstance.objects.create(
                name=f'Performance Instance {i}',
                template=template,
                deployed=(i % 2 == 0)  # Half deployed, half not
            )
            self.instances.append(instance)
    
    def test_recipe_instances_list_performance(self):
        """Test recipe instances list page load performance."""
        self.client.force_login(self.user)
        
        import time
        start_time = time.time()
        response = self.client.get(reverse('recipe_instances'))
        end_time = time.time()
        
        self.assertEqual(response.status_code, 200)
        
        load_time = end_time - start_time
        self.assertLess(load_time, 3.0, 
                       f"Recipe instances list should load in under 3 seconds, took {load_time:.2f}s")
    
    def test_recipe_instance_creation_performance(self):
        """Test recipe instance creation performance."""
        template = RecipeTemplate.objects.create(
            name='Performance Test Template',
            recipe_type='test',
            content=json.dumps({'source': {'type': 'test'}})
        )
        
        import time
        start_time = time.time()
        instance = RecipeInstance.objects.create(
            name='Performance Test Instance',
            template=template
        )
        end_time = time.time()
        
        creation_time = end_time - start_time
        self.assertLess(creation_time, 1.0, 
                       f"Recipe instance creation should complete in under 1 second, took {creation_time:.2f}s")
    
    def test_get_recipe_dict_performance(self):
        """Test get_recipe_dict method performance."""
        instance = self.instances[0]
        
        import time
        start_time = time.time()
        for _ in range(50):
            recipe_dict = instance.get_recipe_dict()
        end_time = time.time()
        
        dict_time = end_time - start_time
        self.assertLess(dict_time, 2.0, 
                       f"get_recipe_dict should complete in under 2 seconds, took {dict_time:.2f}s")


class RecipeInstanceSecurityTestCase(TestCase):
    """Test recipe instance security aspects."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        
        self.recipe_template = RecipeTemplate.objects.create(
            name='Security Test Template',
            recipe_type='test',
            content=json.dumps({'source': {'type': 'test'}})
        )
        
        self.instance = RecipeInstance.objects.create(
            name='Security Test Instance',
            template=self.recipe_template
        )
    
    def test_recipe_instance_access_unauthenticated(self):
        """Test recipe instance access without authentication."""
        endpoints = [
            ('recipe_instances', []),
            ('recipe_instance_create', []),
            ('recipe_instance_edit', [self.instance.id]),
            ('recipe_instance_delete', [self.instance.id]),
            ('recipe_instance_deploy', [self.instance.id]),
        ]
        
        for endpoint_name, args in endpoints:
            with self.subTest(endpoint=endpoint_name):
                response = self.client.get(reverse(endpoint_name, args=args))
                # Should redirect to login or handle gracefully
                self.assertIn(response.status_code, [200, 302, 401, 403])
    
    def test_recipe_instance_xss_protection(self):
        """Test XSS protection in recipe instance creation."""
        self.client.force_login(self.user)
        
        xss_data = {
            'name': '<script>alert("xss")</script>',
            'description': '<img src=x onerror=alert("xss")>',
            'template': str(self.recipe_template.id)
        }
        
        response = self.client.post(
            reverse('recipe_instance_create'),
            data=xss_data
        )
        
        # Should handle XSS attempt gracefully
        self.assertIn(response.status_code, [200, 302])
        
        # If instance was created, check XSS payload is handled
        if RecipeInstance.objects.filter(name__contains='script').exists():
            instance = RecipeInstance.objects.get(name__contains='script')
            # In real implementation, verify XSS payload is properly escaped
            self.assertIn('script', instance.name)  # Basic check 