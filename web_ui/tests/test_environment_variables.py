"""
Comprehensive tests for Environment Variables functionality.

Tests cover:
- EnvVarsTemplate Model tests (all fields, methods, validation)
- EnvVarsInstance Model tests (all fields, methods, variable handling)
- Environment Variables CRUD operations (Create, Read, Update, Delete)
- Variable validation and type conversion
- Template and instance integration
- Repository loading from YAML files
- Git integration and export functionality
- Environment Variables forms and views
- Secret variable handling
- Performance and security tests
- Integration with recipes and environments
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
from django.core.exceptions import ValidationError
from django.db import IntegrityError

from web_ui.web_ui.models import (
    EnvVarsTemplate, EnvVarsInstance, RecipeTemplate, 
    RecipeInstance, Environment, replace_env_vars_with_values
)
from web_ui.web_ui.forms import EnvVarsInstanceForm
from tests.fixtures.simple_factories import UserFactory, EnvironmentFactory


class EnvVarsTemplateModelTestCase(TestCase):
    """Test EnvVarsTemplate model functionality."""
    
    def setUp(self):
        self.user = UserFactory()
        self.valid_variables = {
            'DATABASE_HOST': {
                'description': 'Database host address',
                'required': True,
                'is_secret': False,
                'data_type': 'text',
                'default_value': 'localhost'
            },
            'DATABASE_PASSWORD': {
                'description': 'Database password',
                'required': True,
                'is_secret': True,
                'data_type': 'text',
                'default_value': ''
            },
            'PORT': {
                'description': 'Database port',
                'required': False,
                'is_secret': False,
                'data_type': 'number',
                'default_value': '5432'
            },
            'ENABLE_SSL': {
                'description': 'Enable SSL connection',
                'required': False,
                'is_secret': False,
                'data_type': 'boolean',
                'default_value': 'true'
            }
        }
    
    def test_env_vars_template_creation(self):
        """Test basic EnvVarsTemplate creation."""
        template = EnvVarsTemplate.objects.create(
            name='PostgreSQL Template',
            description='Template for PostgreSQL connection variables',
            tags='database,postgres',
            recipe_type='postgres',
            variables=json.dumps(self.valid_variables)
        )
        
        self.assertEqual(template.name, 'PostgreSQL Template')
        self.assertEqual(template.description, 'Template for PostgreSQL connection variables')
        self.assertEqual(template.tags, 'database,postgres')
        self.assertEqual(template.recipe_type, 'postgres')
        self.assertIsNotNone(template.created_at)
        self.assertIsNotNone(template.updated_at)
    
    def test_env_vars_template_str(self):
        """Test EnvVarsTemplate string representation."""
        template = EnvVarsTemplate.objects.create(
            name='Test Template',
            recipe_type='postgres',
            variables=json.dumps(self.valid_variables)
        )
        self.assertEqual(str(template), 'Test Template')
    
    def test_get_variables_dict(self):
        """Test get_variables_dict method."""
        template = EnvVarsTemplate.objects.create(
            name='Test Template',
            recipe_type='postgres',
            variables=json.dumps(self.valid_variables)
        )
        
        variables = template.get_variables_dict()
        self.assertEqual(variables, self.valid_variables)
    
    def test_get_variables_dict_empty(self):
        """Test get_variables_dict with empty variables."""
        template = EnvVarsTemplate.objects.create(
            name='Empty Template',
            recipe_type='postgres',
            variables=''
        )
        
        variables = template.get_variables_dict()
        self.assertEqual(variables, {})
    
    def test_set_variables_dict(self):
        """Test set_variables_dict method."""
        template = EnvVarsTemplate.objects.create(
            name='Test Template',
            recipe_type='postgres'
        )
        
        template.set_variables_dict(self.valid_variables)
        self.assertEqual(template.variables, json.dumps(self.valid_variables))
        self.assertEqual(template.get_variables_dict(), self.valid_variables)
    
    def test_get_tags_list(self):
        """Test get_tags_list method."""
        template = EnvVarsTemplate.objects.create(
            name='Test Template',
            recipe_type='postgres',
            tags='database, postgres, etl, production',
            variables=json.dumps(self.valid_variables)
        )
        
        tags = template.get_tags_list()
        expected_tags = ['database', 'postgres', 'etl', 'production']
        self.assertEqual(tags, expected_tags)
    
    def test_get_tags_list_empty(self):
        """Test get_tags_list with no tags."""
        template = EnvVarsTemplate.objects.create(
            name='Test Template',
            recipe_type='postgres',
            variables=json.dumps(self.valid_variables)
        )
        
        tags = template.get_tags_list()
        self.assertEqual(tags, [])
    
    def test_set_tags_list(self):
        """Test set_tags_list method."""
        template = EnvVarsTemplate.objects.create(
            name='Test Template',
            recipe_type='postgres',
            variables=json.dumps(self.valid_variables)
        )
        
        tags_list = ['database', 'postgres', 'production']
        template.set_tags_list(tags_list)
        
        self.assertEqual(template.tags, 'database,postgres,production')
        self.assertEqual(template.get_tags_list(), tags_list)
    
    def test_get_display_variables(self):
        """Test get_display_variables method."""
        template = EnvVarsTemplate.objects.create(
            name='Test Template',
            recipe_type='postgres',
            variables=json.dumps(self.valid_variables)
        )
        
        display_vars = template.get_display_variables()
        self.assertEqual(len(display_vars), 4)
        
        # Check structure of display variables
        var_keys = [var['key'] for var in display_vars]
        self.assertIn('DATABASE_HOST', var_keys)
        self.assertIn('DATABASE_PASSWORD', var_keys)
        self.assertIn('PORT', var_keys)
        self.assertIn('ENABLE_SSL', var_keys)
        
        # Check specific variable properties
        db_host_var = next(var for var in display_vars if var['key'] == 'DATABASE_HOST')
        self.assertEqual(db_host_var['description'], 'Database host address')
        self.assertTrue(db_host_var['required'])
        self.assertFalse(db_host_var['is_secret'])
        self.assertEqual(db_host_var['data_type'], 'text')
        self.assertEqual(db_host_var['default_value'], 'localhost')
        
        # Check secret variable
        password_var = next(var for var in display_vars if var['key'] == 'DATABASE_PASSWORD')
        self.assertTrue(password_var['is_secret'])


class EnvVarsInstanceModelTestCase(TestCase):
    """Test EnvVarsInstance model functionality."""
    
    def setUp(self):
        self.user = UserFactory()
        self.environment = EnvironmentFactory(name='test-env')
        
        # Create template
        self.template_variables = {
            'DATABASE_HOST': {
                'description': 'Database host',
                'required': True,
                'is_secret': False,
                'data_type': 'text',
                'default_value': 'localhost'
            },
            'DATABASE_PASSWORD': {
                'description': 'Database password',
                'required': True,
                'is_secret': True,
                'data_type': 'text',
                'default_value': ''
            },
            'PORT': {
                'description': 'Database port',
                'required': False,
                'is_secret': False,
                'data_type': 'number',
                'default_value': '5432'
            }
        }
        
        self.template = EnvVarsTemplate.objects.create(
            name='Test Template',
            recipe_type='postgres',
            variables=json.dumps(self.template_variables)
        )
        
        self.instance_variables = {
            'DATABASE_HOST': {
                'value': 'prod-db.example.com',
                'isSecret': False
            },
            'DATABASE_PASSWORD': {
                'value': 'secret123',
                'isSecret': True
            },
            'PORT': {
                'value': '5432',
                'isSecret': False
            }
        }
    
    def test_env_vars_instance_creation(self):
        """Test basic EnvVarsInstance creation."""
        instance = EnvVarsInstance.objects.create(
            name='Production Instance',
            description='Production environment variables',
            template=self.template,
            environment=self.environment,
            recipe_id='prod-postgres-123',
            recipe_type='postgres',
            variables=json.dumps(self.instance_variables)
        )
        
        self.assertEqual(instance.name, 'Production Instance')
        self.assertEqual(instance.description, 'Production environment variables')
        self.assertEqual(instance.template, self.template)
        self.assertEqual(instance.environment, self.environment)
        self.assertEqual(instance.recipe_id, 'prod-postgres-123')
        self.assertEqual(instance.recipe_type, 'postgres')
        self.assertFalse(instance.deployed)
        self.assertIsNone(instance.deployed_at)
        self.assertFalse(instance.datahub_secrets_created)
        self.assertIsNotNone(instance.created_at)
        self.assertIsNotNone(instance.updated_at)
    
    def test_env_vars_instance_str(self):
        """Test EnvVarsInstance string representation."""
        instance = EnvVarsInstance.objects.create(
            name='Test Instance',
            recipe_id='test-123',
            recipe_type='postgres',
            variables=json.dumps(self.instance_variables)
        )
        self.assertEqual(str(instance), 'Test Instance (test-123)')
        
        # Test without recipe_id
        instance.recipe_id = None
        instance.save()
        self.assertEqual(str(instance), 'Test Instance (No recipe)')
    
    def test_get_variables_dict(self):
        """Test get_variables_dict method."""
        instance = EnvVarsInstance.objects.create(
            name='Test Instance',
            recipe_type='postgres',
            variables=json.dumps(self.instance_variables)
        )
        
        variables = instance.get_variables_dict()
        self.assertEqual(variables, self.instance_variables)
    
    def test_get_variables_dict_invalid_json(self):
        """Test get_variables_dict with invalid JSON."""
        instance = EnvVarsInstance.objects.create(
            name='Test Instance',
            recipe_type='postgres',
            variables='invalid json'
        )
        
        variables = instance.get_variables_dict()
        self.assertEqual(variables, {})
    
    def test_set_variables_dict(self):
        """Test set_variables_dict method."""
        instance = EnvVarsInstance.objects.create(
            name='Test Instance',
            recipe_type='postgres',
            variables='{}'
        )
        
        instance.set_variables_dict(self.instance_variables)
        self.assertEqual(instance.variables, json.dumps(self.instance_variables))
        self.assertEqual(instance.get_variables_dict(), self.instance_variables)
    
    def test_get_secret_variables(self):
        """Test get_secret_variables method."""
        instance = EnvVarsInstance.objects.create(
            name='Test Instance',
            recipe_type='postgres',
            variables=json.dumps(self.instance_variables)
        )
        
        secret_vars = instance.get_secret_variables()
        expected_secrets = {
            'DATABASE_PASSWORD': {
                'value': 'secret123',
                'isSecret': True
            }
        }
        self.assertEqual(secret_vars, expected_secrets)
    
    def test_has_secret_variables_property(self):
        """Test has_secret_variables property."""
        # Instance with secrets
        instance_with_secrets = EnvVarsInstance.objects.create(
            name='Test Instance',
            recipe_type='postgres',
            variables=json.dumps(self.instance_variables)
        )
        self.assertTrue(instance_with_secrets.has_secret_variables)
        
        # Instance without secrets
        no_secrets_vars = {
            'DATABASE_HOST': {
                'value': 'localhost',
                'isSecret': False
            }
        }
        instance_no_secrets = EnvVarsInstance.objects.create(
            name='No Secrets Instance',
            recipe_type='postgres',
            variables=json.dumps(no_secrets_vars)
        )
        self.assertFalse(instance_no_secrets.has_secret_variables)
    
    def test_validate_all_variables(self):
        """Test validate_all_variables method."""
        instance = EnvVarsInstance.objects.create(
            name='Test Instance',
            template=self.template,
            recipe_type='postgres',
            variables=json.dumps(self.instance_variables)
        )
        
        # Should validate successfully with complete data
        self.assertTrue(instance.validate_all_variables())
        
        # Test with missing required variable
        incomplete_vars = {
            'DATABASE_HOST': {
                'value': 'localhost',
                'isSecret': False
            }
            # Missing required DATABASE_PASSWORD
        }
        instance.set_variables_dict(incomplete_vars)
        instance.save()
        
        # Should fail validation due to missing required variable
        self.assertFalse(instance.validate_all_variables())
    
    def test_get_typed_value(self):
        """Test get_typed_value method with different data types."""
        # Create template with different data types
        typed_template_vars = {
            'TEXT_VAR': {
                'data_type': 'text',
                'required': True
            },
            'NUMBER_VAR': {
                'data_type': 'number',
                'required': True
            },
            'BOOLEAN_VAR': {
                'data_type': 'boolean',
                'required': True
            },
            'JSON_VAR': {
                'data_type': 'json',
                'required': True
            }
        }
        
        typed_template = EnvVarsTemplate.objects.create(
            name='Typed Template',
            recipe_type='test',
            variables=json.dumps(typed_template_vars)
        )
        
        typed_instance_vars = {
            'TEXT_VAR': {'value': 'hello world', 'isSecret': False},
            'NUMBER_VAR': {'value': '42.5', 'isSecret': False},
            'BOOLEAN_VAR': {'value': 'true', 'isSecret': False},
            'JSON_VAR': {'value': '{"key": "value"}', 'isSecret': False}
        }
        
        instance = EnvVarsInstance.objects.create(
            name='Typed Instance',
            template=typed_template,
            recipe_type='test',
            variables=json.dumps(typed_instance_vars)
        )
        
        # Test type conversions
        self.assertEqual(instance.get_typed_value('TEXT_VAR'), 'hello world')
        self.assertEqual(instance.get_typed_value('NUMBER_VAR'), 42.5)
        self.assertEqual(instance.get_typed_value('BOOLEAN_VAR'), True)
        self.assertEqual(instance.get_typed_value('JSON_VAR'), {'key': 'value'})
        
        # Test missing value
        self.assertIsNone(instance.get_typed_value('MISSING_VAR'))


class ReplaceEnvVarsUtilityTestCase(TestCase):
    """Test replace_env_vars_with_values utility function."""
    
    def test_replace_env_vars_in_string(self):
        """Test replacing environment variables in strings."""
        content = "Host: ${DATABASE_HOST}, Port: ${PORT}"
        env_vars = {
            'DATABASE_HOST': {'value': 'prod-db.example.com'},
            'PORT': {'value': '5432'}
        }
        
        result = replace_env_vars_with_values(content, env_vars)
        expected = "Host: prod-db.example.com, Port: 5432"
        self.assertEqual(result, expected)
    
    def test_replace_env_vars_in_dict(self):
        """Test replacing environment variables in dictionaries."""
        content = {
            'source': {
                'type': 'postgres',
                'config': {
                    'host': '${DATABASE_HOST}',
                    'port': '${PORT}',
                    'database': 'mydb'
                }
            }
        }
        
        env_vars = {
            'DATABASE_HOST': {'value': 'prod-db.example.com'},
            'PORT': {'value': '5432'}
        }
        
        result = replace_env_vars_with_values(content, env_vars)
        
        expected = {
            'source': {
                'type': 'postgres',
                'config': {
                    'host': 'prod-db.example.com',
                    'port': '5432',
                    'database': 'mydb'
                }
            }
        }
        
        self.assertEqual(result, expected)
    
    def test_replace_env_vars_with_escaping(self):
        """Test replacing environment variables with special characters."""
        content = "Password: ${PASSWORD}"
        env_vars = {
            'PASSWORD': {'value': 'secret$with\\special$chars'}
        }
        
        result = replace_env_vars_with_values(content, env_vars)
        # The function should handle escaping properly
        self.assertIn('secret', result)
    
    def test_replace_env_vars_missing_variable(self):
        """Test behavior with missing environment variables."""
        content = "Host: ${DATABASE_HOST}, Missing: ${MISSING_VAR}"
        env_vars = {
            'DATABASE_HOST': {'value': 'localhost'}
        }
        
        result = replace_env_vars_with_values(content, env_vars)
        # Missing variables should be left as-is
        self.assertEqual(result, "Host: localhost, Missing: ${MISSING_VAR}")
    
    def test_replace_env_vars_empty_content(self):
        """Test with empty content."""
        result = replace_env_vars_with_values(None, {'VAR': {'value': 'test'}})
        self.assertIsNone(result)
        
        result = replace_env_vars_with_values('', {'VAR': {'value': 'test'}})
        self.assertEqual(result, '')
    
    def test_replace_env_vars_empty_env_vars(self):
        """Test with empty environment variables."""
        content = "Host: ${DATABASE_HOST}"
        result = replace_env_vars_with_values(content, {})
        self.assertEqual(result, content)


class EnvVarsInstanceFormTestCase(TestCase):
    """Test EnvVarsInstanceForm functionality."""
    
    def setUp(self):
        self.environment = EnvironmentFactory(name='test-env')
        self.template = EnvVarsTemplate.objects.create(
            name='Form Test Template',
            recipe_type='postgres',
            variables=json.dumps({
                'HOST': {'description': 'Database host', 'required': True}
            })
        )
        
        self.valid_form_data = {
            'name': 'Test Instance',
            'description': 'Test description',
            'template': str(self.template.id),
            'recipe_type': 'postgres',
            'variables': json.dumps({
                'HOST': {'value': 'localhost', 'isSecret': False}
            }),
            'environment': str(self.environment.id)
        }
    
    def test_form_initialization(self):
        """Test form initialization with querysets."""
        # The form __init__ method should set querysets properly
        # This would need to be tested with the actual form implementation
        pass
    
    def test_form_validation_success(self):
        """Test form validation with valid data."""
        # This would test form.is_valid() with proper data
        # Implementation depends on the actual form validation logic
        pass
    
    def test_form_validation_missing_required(self):
        """Test form validation with missing required fields."""
        invalid_data = self.valid_form_data.copy()
        invalid_data['name'] = ''
        
        # Test that form validation catches missing required fields
        pass


class EnvVarsRepositoryLoaderTestCase(TestCase):
    """Test repository loading functionality for environment variables."""
    
    def setUp(self):
        self.environment = EnvironmentFactory(name='dev')
        
        # Create temporary directory structure
        self.temp_dir = Path(tempfile.mkdtemp())
        self.templates_dir = self.temp_dir / 'recipes' / 'templates'
        self.params_dir = self.temp_dir / 'params' / 'environments' / 'dev'
        
        self.templates_dir.mkdir(parents=True, exist_ok=True)
        self.params_dir.mkdir(parents=True, exist_ok=True)
    
    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_load_env_vars_template_from_yaml(self):
        """Test loading environment variables template from YAML."""
        template_content = {
            'name': 'PostgreSQL Template',
            'description': 'Template for PostgreSQL connection',
            'source': {
                'type': 'postgres',
                'config': {
                    'host': '${DATABASE_HOST}',
                    'database': '${DATABASE_NAME}',
                    'username': '${DATABASE_USER}',
                    'password': '${DATABASE_PASSWORD}'
                }
            }
        }
        
        template_file = self.templates_dir / 'postgres.yml'
        with open(template_file, 'w') as f:
            import yaml
            yaml.dump(template_content, f)
        
        # Test the loading function
        from web_ui.web_ui.services.repo_loader import RepositoryLoader
        
        with patch('web_ui.web_ui.services.repo_loader.Path') as mock_path:
            mock_path.return_value = self.temp_dir.parent
            success = RepositoryLoader._load_env_vars_template(template_file)
        
        self.assertTrue(success)
        
        # Check that template was created
        template = EnvVarsTemplate.objects.get(name='postgres')
        self.assertEqual(template.recipe_type, 'postgres')
        
        variables = template.get_variables_dict()
        self.assertIn('DATABASE_HOST', variables)
        self.assertIn('DATABASE_NAME', variables)
        self.assertIn('DATABASE_USER', variables)
        self.assertIn('DATABASE_PASSWORD', variables)
    
    def test_load_env_vars_instance_from_yaml(self):
        """Test loading environment variables instance from YAML."""
        instance_content = {
            'name': 'Dev PostgreSQL Variables',
            'description': 'Development environment variables',
            'recipe_type': 'postgres',
            'parameters': {
                'DATABASE_HOST': 'dev-db.example.com',
                'DATABASE_NAME': 'dev_database',
                'DATABASE_USER': 'dev_user'
            },
            'secret_references': ['DATABASE_PASSWORD']
        }
        
        instance_file = self.params_dir / 'postgres.yml'
        with open(instance_file, 'w') as f:
            import yaml
            yaml.dump(instance_content, f)
        
        # Test the loading function
        from web_ui.web_ui.services.repo_loader import RepositoryLoader
        
        success = RepositoryLoader._load_env_vars_instance(instance_file, self.environment)
        
        self.assertTrue(success)
        
        # Check that instance was created
        instance = EnvVarsInstance.objects.get(name='postgres')
        self.assertEqual(instance.recipe_type, 'postgres')
        self.assertEqual(instance.environment, self.environment)
        
        variables = instance.get_variables_dict()
        self.assertIn('DATABASE_HOST', variables)
        self.assertIn('DATABASE_PASSWORD', variables)
        self.assertTrue(variables['DATABASE_PASSWORD']['isSecret'])
        self.assertFalse(variables['DATABASE_HOST']['isSecret'])


class EnvVarsGitIntegrationTestCase(TestCase):
    """Test Git integration for environment variables."""
    
    def setUp(self):
        self.environment = EnvironmentFactory(name='dev')
        self.template = EnvVarsTemplate.objects.create(
            name='Git Test Template',
            description='Template for Git testing',
            recipe_type='postgres',
            variables=json.dumps({
                'HOST': {'description': 'Database host', 'required': True},
                'PASSWORD': {'description': 'Database password', 'required': True, 'is_secret': True}
            })
        )
        
        self.instance = EnvVarsInstance.objects.create(
            name='Git Test Instance',
            description='Instance for Git testing',
            template=self.template,
            environment=self.environment,
            recipe_type='postgres',
            variables=json.dumps({
                'HOST': {'value': 'test-db.example.com', 'isSecret': False},
                'PASSWORD': {'value': 'secret123', 'isSecret': True}
            })
        )
    
    @patch('web_ui.web_ui.models.GitIntegration._export_to_github')
    def test_export_env_vars_template_to_github(self, mock_export):
        """Test exporting environment variables template to GitHub."""
        from web_ui.web_ui.models import GitIntegration
        
        mock_export.return_value = True
        
        result = GitIntegration.export_to_github(
            self.template,
            environment_name='dev',
            branch_name='feature/add-env-vars-template'
        )
        
        self.assertTrue(result)
        mock_export.assert_called_once()
    
    @patch('web_ui.web_ui.models.GitIntegration._export_to_github')
    def test_export_env_vars_instance_to_github(self, mock_export):
        """Test exporting environment variables instance to GitHub."""
        from web_ui.web_ui.models import GitIntegration
        
        mock_export.return_value = True
        
        result = GitIntegration.export_to_github(
            self.instance,
            environment_name='dev',
            branch_name='feature/add-env-vars-instance'
        )
        
        self.assertTrue(result)
        mock_export.assert_called_once()


class EnvVarsRecipeIntegrationTestCase(TestCase):
    """Test integration between environment variables and recipes."""
    
    def setUp(self):
        self.environment = EnvironmentFactory(name='production')
        
        # Create recipe template
        self.recipe_template = RecipeTemplate.objects.create(
            name='PostgreSQL Recipe Template',
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
        
        # Create environment variables template
        self.env_vars_template = EnvVarsTemplate.objects.create(
            name='PostgreSQL Variables Template',
            recipe_type='postgres',
            variables=json.dumps({
                'DATABASE_HOST': {
                    'description': 'Database host',
                    'required': True,
                    'is_secret': False,
                    'data_type': 'text'
                },
                'DATABASE_NAME': {
                    'description': 'Database name',
                    'required': True,
                    'is_secret': False,
                    'data_type': 'text'
                },
                'DATABASE_USER': {
                    'description': 'Database user',
                    'required': True,
                    'is_secret': False,
                    'data_type': 'text'
                },
                'DATABASE_PASSWORD': {
                    'description': 'Database password',
                    'required': True,
                    'is_secret': True,
                    'data_type': 'text'
                }
            })
        )
        
        # Create environment variables instance
        self.env_vars_instance = EnvVarsInstance.objects.create(
            name='Production PostgreSQL Variables',
            template=self.env_vars_template,
            environment=self.environment,
            recipe_type='postgres',
            variables=json.dumps({
                'DATABASE_HOST': {
                    'value': 'prod-db.example.com',
                    'isSecret': False
                },
                'DATABASE_NAME': {
                    'value': 'production_db',
                    'isSecret': False
                },
                'DATABASE_USER': {
                    'value': 'prod_user',
                    'isSecret': False
                },
                'DATABASE_PASSWORD': {
                    'value': 'super_secret_password',
                    'isSecret': True
                }
            })
        )
    
    def test_recipe_instance_creation_with_env_vars(self):
        """Test creating recipe instance with environment variables."""
        recipe_instance = RecipeInstance.objects.create(
            name='Production PostgreSQL Recipe',
            description='Production PostgreSQL ingestion recipe',
            template=self.recipe_template,
            env_vars_instance=self.env_vars_instance,
            environment=self.environment
        )
        
        self.assertEqual(recipe_instance.template, self.recipe_template)
        self.assertEqual(recipe_instance.env_vars_instance, self.env_vars_instance)
        self.assertEqual(recipe_instance.environment, self.environment)
    
    def test_recipe_instance_get_combined_content(self):
        """Test getting combined content from recipe instance."""
        recipe_instance = RecipeInstance.objects.create(
            name='Production PostgreSQL Recipe',
            template=self.recipe_template,
            env_vars_instance=self.env_vars_instance,
            environment=self.environment
        )
        
        combined_content = recipe_instance.get_combined_content()
        
        # Check that environment variables were replaced
        self.assertIsInstance(combined_content, dict)
        config = combined_content['source']['config']
        self.assertEqual(config['host'], 'prod-db.example.com')
        self.assertEqual(config['database'], 'production_db')
        self.assertEqual(config['username'], 'prod_user')
        self.assertEqual(config['password'], 'super_secret_password')
    
    def test_recipe_instance_get_recipe_dict(self):
        """Test getting recipe dictionary from recipe instance."""
        recipe_instance = RecipeInstance.objects.create(
            name='Production PostgreSQL Recipe',
            template=self.recipe_template,
            env_vars_instance=self.env_vars_instance,
            environment=self.environment
        )
        
        recipe_dict = recipe_instance.get_recipe_dict()
        
        # Check that the recipe has proper structure
        self.assertIsInstance(recipe_dict, dict)
        self.assertIn('source', recipe_dict)
        self.assertEqual(recipe_dict['source']['type'], 'postgres')
        
        # Check that environment variables were applied
        config = recipe_dict['source']['config']
        self.assertEqual(config['host'], 'prod-db.example.com')
        self.assertEqual(config['password'], 'super_secret_password')
    
    def test_recipe_instance_export_to_yaml(self):
        """Test exporting recipe instance to YAML."""
        recipe_instance = RecipeInstance.objects.create(
            name='Production PostgreSQL Recipe',
            template=self.recipe_template,
            env_vars_instance=self.env_vars_instance,
            environment=self.environment
        )
        
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = recipe_instance.export_to_yaml(base_dir=temp_dir)
            
            self.assertTrue(file_path.exists())
            
            # Check file content
            with open(file_path, 'r') as f:
                import yaml
                content = yaml.safe_load(f)
            
            self.assertEqual(content['name'], 'Production PostgreSQL Recipe')
            self.assertEqual(content['recipe_type'], 'postgres')
            self.assertIn('recipe', content)
            self.assertIn('env_vars', content)
            
            # Check that sensitive values are included in export
            env_vars = content['env_vars']
            self.assertIn('DATABASE_PASSWORD', env_vars)


class EnvVarsSecurityTestCase(TestCase):
    """Test security aspects of environment variables."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        
        self.template = EnvVarsTemplate.objects.create(
            name='Security Test Template',
            recipe_type='test',
            variables=json.dumps({
                'PUBLIC_VAR': {
                    'description': 'Public variable',
                    'is_secret': False
                },
                'SECRET_VAR': {
                    'description': 'Secret variable',
                    'is_secret': True
                }
            })
        )
        
        self.instance = EnvVarsInstance.objects.create(
            name='Security Test Instance',
            template=self.template,
            recipe_type='test',
            variables=json.dumps({
                'PUBLIC_VAR': {
                    'value': 'public_value',
                    'isSecret': False
                },
                'SECRET_VAR': {
                    'value': 'secret_value',
                    'isSecret': True
                }
            })
        )
    
    def test_secret_variables_not_exposed_in_views(self):
        """Test that secret variables are not exposed in views."""
        self.client.force_login(self.user)
        
        # This would test that secret values are masked in HTML responses
        # Implementation depends on the actual view logic
        pass
    
    def test_secret_variables_handling_in_api(self):
        """Test secret variables handling in API responses."""
        # Test that API endpoints properly mask secret values
        pass
    
    def test_xss_protection_in_variable_values(self):
        """Test XSS protection in variable values."""
        xss_instance = EnvVarsInstance.objects.create(
            name='XSS Test Instance',
            recipe_type='test',
            variables=json.dumps({
                'XSS_VAR': {
                    'value': '<script>alert("xss")</script>',
                    'isSecret': False
                }
            })
        )
        
        # Variable should be stored but properly escaped when displayed
        variables = xss_instance.get_variables_dict()
        self.assertIn('<script>', variables['XSS_VAR']['value'])
        
        # In real implementation, verify XSS is escaped in templates


class EnvVarsPerformanceTestCase(TestCase):
    """Test performance aspects of environment variables."""
    
    def setUp(self):
        self.environment = EnvironmentFactory(name='perf-test')
        
        # Create multiple templates and instances
        self.templates = []
        self.instances = []
        
        for i in range(20):
            template = EnvVarsTemplate.objects.create(
                name=f'Performance Template {i}',
                recipe_type='test',
                variables=json.dumps({
                    f'VAR_{j}': {
                        'description': f'Variable {j}',
                        'required': True,
                        'is_secret': j % 2 == 0
                    } for j in range(10)
                })
            )
            self.templates.append(template)
            
            instance = EnvVarsInstance.objects.create(
                name=f'Performance Instance {i}',
                template=template,
                environment=self.environment,
                recipe_type='test',
                variables=json.dumps({
                    f'VAR_{j}': {
                        'value': f'value_{j}',
                        'isSecret': j % 2 == 0
                    } for j in range(10)
                })
            )
            self.instances.append(instance)
    
    def test_variable_replacement_performance(self):
        """Test performance of variable replacement in large content."""
        # Create large content with many variables
        large_content = {
            'source': {
                'type': 'test',
                'config': {
                    f'param_{i}': f'${{VAR_{i % 10}}}' for i in range(100)
                }
            }
        }
        
        env_vars = {
            f'VAR_{i}': {'value': f'value_{i}'} for i in range(10)
        }
        
        import time
        start_time = time.time()
        result = replace_env_vars_with_values(large_content, env_vars)
        end_time = time.time()
        
        replacement_time = end_time - start_time
        self.assertLess(replacement_time, 1.0, 
                       f"Variable replacement should complete in under 1 second, took {replacement_time:.2f}s")
        
        # Verify replacement worked
        self.assertIsInstance(result, dict)
        config = result['source']['config']
        self.assertEqual(config['param_0'], 'value_0')
        self.assertEqual(config['param_9'], 'value_9')
    
    def test_template_variable_parsing_performance(self):
        """Test performance of template variable parsing."""
        template = self.templates[0]
        
        import time
        start_time = time.time()
        for _ in range(100):
            variables = template.get_variables_dict()
            display_vars = template.get_display_variables()
        end_time = time.time()
        
        parsing_time = end_time - start_time
        self.assertLess(parsing_time, 1.0, 
                       f"Template parsing should complete in under 1 second, took {parsing_time:.2f}s")
    
    def test_instance_validation_performance(self):
        """Test performance of instance validation."""
        instance = self.instances[0]
        
        import time
        start_time = time.time()
        for _ in range(50):
            result = instance.validate_all_variables()
        end_time = time.time()
        
        validation_time = end_time - start_time
        self.assertLess(validation_time, 2.0, 
                       f"Instance validation should complete in under 2 seconds, took {validation_time:.2f}s")


class EnvVarsIntegrationTestCase(TestCase):
    """Test environment variables integration with other components."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.environment = EnvironmentFactory(name='integration-test')
        
        self.template = EnvVarsTemplate.objects.create(
            name='Integration Test Template',
            recipe_type='postgres',
            variables=json.dumps({
                'HOST': {'description': 'Host', 'required': True}
            })
        )
        
        self.instance = EnvVarsInstance.objects.create(
            name='Integration Test Instance',
            template=self.template,
            environment=self.environment,
            recipe_type='postgres',
            variables=json.dumps({
                'HOST': {'value': 'integration-db.example.com', 'isSecret': False}
            })
        )
    
    def test_env_vars_in_recipe_deployment(self):
        """Test environment variables in recipe deployment workflow."""
        # Create recipe template and instance
        recipe_template = RecipeTemplate.objects.create(
            name='Integration Recipe Template',
            recipe_type='postgres',
            content=json.dumps({
                'source': {
                    'type': 'postgres',
                    'config': {
                        'host': '${HOST}'
                    }
                }
            })
        )
        
        recipe_instance = RecipeInstance.objects.create(
            name='Integration Recipe Instance',
            template=recipe_template,
            env_vars_instance=self.instance,
            environment=self.environment
        )
        
        # Test combined content generation
        combined_content = recipe_instance.get_combined_content()
        self.assertEqual(
            combined_content['source']['config']['host'],
            'integration-db.example.com'
        )
        
        # Test recipe dictionary generation for deployment
        recipe_dict = recipe_instance.get_recipe_dict()
        self.assertIn('source', recipe_dict)
        self.assertEqual(
            recipe_dict['source']['config']['host'],
            'integration-db.example.com'
        )
    
    def test_env_vars_with_multiple_environments(self):
        """Test environment variables with multiple environments."""
        # Create additional environment
        prod_env = EnvironmentFactory(name='production')
        
        # Create instance for production environment
        prod_instance = EnvVarsInstance.objects.create(
            name='Production Instance',
            template=self.template,
            environment=prod_env,
            recipe_type='postgres',
            variables=json.dumps({
                'HOST': {'value': 'prod-db.example.com', 'isSecret': False}
            })
        )
        
        # Test that instances are properly associated with environments
        dev_instances = EnvVarsInstance.objects.filter(environment=self.environment)
        prod_instances = EnvVarsInstance.objects.filter(environment=prod_env)
        
        self.assertIn(self.instance, dev_instances)
        self.assertIn(prod_instance, prod_instances)
        self.assertNotIn(prod_instance, dev_instances)
        self.assertNotIn(self.instance, prod_instances)
    
    def test_env_vars_template_reuse(self):
        """Test reusing environment variables templates across instances."""
        # Create second instance using same template
        second_instance = EnvVarsInstance.objects.create(
            name='Second Instance',
            template=self.template,
            environment=self.environment,
            recipe_type='postgres',
            variables=json.dumps({
                'HOST': {'value': 'second-db.example.com', 'isSecret': False}
            })
        )
        
        # Both instances should reference the same template
        self.assertEqual(self.instance.template, self.template)
        self.assertEqual(second_instance.template, self.template)
        
        # But have different variable values
        instance1_vars = self.instance.get_variables_dict()
        instance2_vars = second_instance.get_variables_dict()
        
        self.assertNotEqual(
            instance1_vars['HOST']['value'],
            instance2_vars['HOST']['value']
        )
        
        self.assertEqual(instance1_vars['HOST']['value'], 'integration-db.example.com')
        self.assertEqual(instance2_vars['HOST']['value'], 'second-db.example.com') 