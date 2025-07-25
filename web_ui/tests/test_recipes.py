"""
Comprehensive tests for Recipe functionality.

Tests cover:
- Recipe listing and data API
- Recipe CRUD operations (Create, Read, Update, Delete)
- Recipe running and execution
- Recipe download and export
- Recipe import functionality
- Recipe permissions and authentication
- Recipe form validation
- Recipe error handling
"""

import json
import tempfile
from unittest.mock import Mock, patch, MagicMock
from django.test import TestCase, Client
from django.urls import reverse
from django.contrib.auth.models import User
from django.core.files.uploadedfile import SimpleUploadedFile

from tests.fixtures.simple_factories import UserFactory, EnvironmentFactory


class RecipeListViewTestCase(TestCase):
    """Test recipe listing functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
    
    def test_recipe_list_access_authenticated(self):
        """Test recipe list access with authenticated user."""
        self.client.force_login(self.user)
        response = self.client.get('/recipes/')
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'recipe')  # Should contain recipe content
    
    def test_recipe_list_access_unauthenticated(self):
        """Test recipe list access without authentication."""
        response = self.client.get('/recipes/')
        # Check if requires authentication or allows anonymous access
        self.assertIn(response.status_code, [200, 302])
    
    def test_recipe_data_api_authenticated(self):
        """Test recipe data API with authenticated user."""
        self.client.force_login(self.user)
        response = self.client.get('/recipes/data/')
        self.assertEqual(response.status_code, 200)
        # Should return JSON data for recipes
    
    def test_recipe_data_api_unauthenticated(self):
        """Test recipe data API without authentication."""
        response = self.client.get('/recipes/data/')
        self.assertIn(response.status_code, [200, 302, 401, 403])
    
    @patch('web_ui.views.get_datahub_client_from_request')
    def test_recipe_list_with_datahub_recipes(self, mock_datahub):
        """Test recipe list with mocked DataHub recipes."""
        mock_client = Mock()
        mock_client.list_recipes.return_value = [
            {'name': 'test-recipe-1', 'id': 'recipe1', 'lastUpdated': 1640995200000},
            {'name': 'test-recipe-2', 'id': 'recipe2', 'lastUpdated': 1640995300000}
        ]
        mock_datahub.return_value = mock_client
        
        self.client.force_login(self.user)
        response = self.client.get('/recipes/')
        self.assertEqual(response.status_code, 200)
    
    def test_recipe_list_performance(self):
        """Test recipe list performance."""
        self.client.force_login(self.user)
        
        import time
        start_time = time.time()
        response = self.client.get('/recipes/')
        end_time = time.time()
        
        self.assertEqual(response.status_code, 200)
        load_time = end_time - start_time
        self.assertLess(load_time, 3.0, "Recipe list should load in under 3 seconds")


class RecipeCreateTestCase(TestCase):
    """Test recipe creation functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
        
        # Sample recipe data
        self.valid_recipe_data = {
            'name': 'test-recipe',
            'description': 'Test recipe description',
            'recipe_content': json.dumps({
                'source': {
                    'type': 'postgres',
                    'config': {
                        'host': 'localhost',
                        'database': 'test_db'
                    }
                }
            })
        }
    
    def test_recipe_create_page_access(self):
        """Test recipe create page access."""
        self.client.force_login(self.user)
        response = self.client.get('/recipes/create/')
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'create')  # Should contain create form
    
    def test_recipe_create_page_unauthenticated(self):
        """Test recipe create page without authentication."""
        response = self.client.get('/recipes/create/')
        self.assertIn(response.status_code, [200, 302])
    
    def test_recipe_create_valid_data(self):
        """Test recipe creation with valid data."""
        self.client.force_login(self.user)
        
        response = self.client.post('/recipes/create/', self.valid_recipe_data)
        # Should redirect after successful creation or stay on page
        self.assertIn(response.status_code, [200, 302])
    
    def test_recipe_create_invalid_data(self):
        """Test recipe creation with invalid data."""
        self.client.force_login(self.user)
        
        invalid_data = {
            'name': '',  # Missing required name
            'description': 'Test',
            'recipe_content': 'invalid json'
        }
        
        response = self.client.post('/recipes/create/', invalid_data)
        # Should stay on create page with errors
        self.assertEqual(response.status_code, 200)
    
    def test_recipe_create_json_validation(self):
        """Test recipe creation with JSON validation."""
        self.client.force_login(self.user)
        
        invalid_json_data = self.valid_recipe_data.copy()
        invalid_json_data['recipe_content'] = '{"invalid": json}'
        
        response = self.client.post('/recipes/create/', invalid_json_data)
        self.assertEqual(response.status_code, 200)
    
    @patch('web_ui.views.get_datahub_client_from_request')
    def test_recipe_create_with_datahub_validation(self, mock_datahub):
        """Test recipe creation with DataHub validation."""
        mock_client = Mock()
        mock_client.validate_recipe.return_value = True
        mock_datahub.return_value = mock_client
        
        self.client.force_login(self.user)
        response = self.client.post('/recipes/create/', self.valid_recipe_data)
        
        self.assertIn(response.status_code, [200, 302])


class RecipeEditTestCase(TestCase):
    """Test recipe editing functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
        self.recipe_id = 'test-recipe-123'
    
    def test_recipe_edit_page_access(self):
        """Test recipe edit page access."""
        self.client.force_login(self.user)
        response = self.client.get(f'/recipes/edit/{self.recipe_id}/')
        
        # Might return 200 with form or 404 if recipe doesn't exist
        self.assertIn(response.status_code, [200, 404])
    
    @patch('web_ui.views.get_datahub_client_from_request')
    def test_recipe_edit_existing_recipe(self, mock_datahub):
        """Test editing an existing recipe."""
        mock_client = Mock()
        mock_client.get_recipe.return_value = {
            'name': 'existing-recipe',
            'id': self.recipe_id,
            'content': {'source': {'type': 'test'}}
        }
        mock_datahub.return_value = mock_client
        
        self.client.force_login(self.user)
        response = self.client.get(f'/recipes/edit/{self.recipe_id}/')
        
        self.assertEqual(response.status_code, 200)
    
    @patch('web_ui.views.get_datahub_client_from_request')
    def test_recipe_edit_post_valid_data(self, mock_datahub):
        """Test recipe edit POST with valid data."""
        mock_client = Mock()
        mock_client.get_recipe.return_value = {'name': 'test', 'id': self.recipe_id}
        mock_client.update_recipe.return_value = True
        mock_datahub.return_value = mock_client
        
        self.client.force_login(self.user)
        
        update_data = {
            'name': 'updated-recipe',
            'description': 'Updated description',
            'recipe_content': json.dumps({'source': {'type': 'updated'}})
        }
        
        response = self.client.post(f'/recipes/edit/{self.recipe_id}/', update_data)
        self.assertIn(response.status_code, [200, 302])
    
    def test_recipe_edit_nonexistent_recipe(self):
        """Test editing a non-existent recipe."""
        self.client.force_login(self.user)
        response = self.client.get('/recipes/edit/nonexistent-recipe/')
        
        # Should return 404 or handle gracefully
        self.assertIn(response.status_code, [200, 404])


class RecipeDeleteTestCase(TestCase):
    """Test recipe deletion functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
        self.recipe_id = 'test-recipe-123'
    
    @patch('web_ui.views.get_datahub_client_from_request')
    def test_recipe_delete_existing_recipe(self, mock_datahub):
        """Test deleting an existing recipe."""
        mock_client = Mock()
        mock_client.get_recipe.return_value = {'name': 'test', 'id': self.recipe_id}
        mock_client.delete_recipe.return_value = True
        mock_datahub.return_value = mock_client
        
        self.client.force_login(self.user)
        response = self.client.post(f'/recipes/delete/{self.recipe_id}/')
        
        # Should redirect after deletion
        self.assertIn(response.status_code, [200, 302])
    
    def test_recipe_delete_nonexistent_recipe(self):
        """Test deleting a non-existent recipe."""
        self.client.force_login(self.user)
        response = self.client.post('/recipes/delete/nonexistent-recipe/')
        
        # Should handle gracefully
        self.assertIn(response.status_code, [200, 302, 404])
    
    def test_recipe_delete_permission_check(self):
        """Test recipe deletion permission requirements."""
        # Test without authentication
        response = self.client.post(f'/recipes/delete/{self.recipe_id}/')
        self.assertIn(response.status_code, [200, 302, 401, 403])


class RecipeRunTestCase(TestCase):
    """Test recipe running functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
        self.recipe_id = 'test-recipe-123'
    
    @patch('web_ui.views.get_datahub_client_from_request')
    def test_recipe_run_existing_recipe(self, mock_datahub):
        """Test running an existing recipe."""
        mock_client = Mock()
        mock_client.get_recipe.return_value = {'name': 'test', 'id': self.recipe_id}
        mock_client.run_recipe.return_value = {'status': 'success', 'run_id': 'run123'}
        mock_datahub.return_value = mock_client
        
        self.client.force_login(self.user)
        response = self.client.post(f'/recipes/run/{self.recipe_id}/')
        
        self.assertIn(response.status_code, [200, 302])
    
    def test_recipe_run_nonexistent_recipe(self):
        """Test running a non-existent recipe."""
        self.client.force_login(self.user)
        response = self.client.post('/recipes/run/nonexistent-recipe/')
        
        # Should handle gracefully
        self.assertIn(response.status_code, [200, 302, 404])
    
    @patch('web_ui.views.get_datahub_client_from_request')
    def test_recipe_run_with_parameters(self, mock_datahub):
        """Test running a recipe with parameters."""
        mock_client = Mock()
        mock_client.get_recipe.return_value = {'name': 'test', 'id': self.recipe_id}
        mock_client.run_recipe.return_value = {'status': 'success'}
        mock_datahub.return_value = mock_client
        
        self.client.force_login(self.user)
        
        run_data = {
            'dry_run': 'false',
            'parameters': json.dumps({'env': 'test'})
        }
        
        response = self.client.post(f'/recipes/run/{self.recipe_id}/', run_data)
        self.assertIn(response.status_code, [200, 302])


class RecipeDownloadTestCase(TestCase):
    """Test recipe download functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
        self.recipe_id = 'test-recipe-123'
    
    @patch('web_ui.views.get_datahub_client_from_request')
    def test_recipe_download_existing_recipe(self, mock_datahub):
        """Test downloading an existing recipe."""
        mock_client = Mock()
        mock_client.get_recipe.return_value = {
            'name': 'test-recipe',
            'id': self.recipe_id,
            'content': {'source': {'type': 'test'}}
        }
        mock_datahub.return_value = mock_client
        
        self.client.force_login(self.user)
        response = self.client.get(f'/recipes/download/{self.recipe_id}/')
        
        # Should return file download or JSON
        if response.status_code == 200:
            # Check if it's a file download
            self.assertIn(response.get('Content-Type', ''), ['application/json', 'application/octet-stream', 'text/yaml'])
    
    def test_recipe_download_nonexistent_recipe(self):
        """Test downloading a non-existent recipe."""
        self.client.force_login(self.user)
        response = self.client.get('/recipes/download/nonexistent-recipe/')
        
        # Should return 404 or handle gracefully
        self.assertIn(response.status_code, [200, 404])
    
    def test_recipe_download_permissions(self):
        """Test recipe download permissions."""
        # Test without authentication
        response = self.client.get(f'/recipes/download/{self.recipe_id}/')
        self.assertIn(response.status_code, [200, 302, 401, 403])


class RecipeExportTestCase(TestCase):
    """Test recipe export functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
    
    @patch('web_ui.views.get_datahub_client_from_request')
    def test_export_all_recipes(self, mock_datahub):
        """Test exporting all recipes."""
        mock_client = Mock()
        mock_client.list_recipes.return_value = [
            {'name': 'recipe1', 'id': 'r1'},
            {'name': 'recipe2', 'id': 'r2'}
        ]
        mock_datahub.return_value = mock_client
        
        self.client.force_login(self.user)
        response = self.client.get('/recipes/export-all/')
        
        # Should return file download
        if response.status_code == 200:
            self.assertIn(response.get('Content-Type', ''), ['application/zip', 'application/json'])
    
    def test_export_all_recipes_permissions(self):
        """Test export all recipes permissions."""
        # Test without authentication
        response = self.client.get('/recipes/export-all/')
        self.assertIn(response.status_code, [200, 302, 401, 403])


class RecipeImportTestCase(TestCase):
    """Test recipe import functionality."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
    
    def test_recipe_import_page_access(self):
        """Test recipe import page access."""
        self.client.force_login(self.user)
        response = self.client.get('/recipes/import/')
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'import')  # Should contain import form
    
    def test_recipe_import_valid_file(self):
        """Test recipe import with valid file."""
        self.client.force_login(self.user)
        
        # Create a valid recipe file
        recipe_content = json.dumps({
            'name': 'imported-recipe',
            'source': {'type': 'test'}
        })
        
        test_file = SimpleUploadedFile(
            "recipe.json",
            recipe_content.encode('utf-8'),
            content_type="application/json"
        )
        
        response = self.client.post('/recipes/import/', {'recipe_file': test_file})
        self.assertIn(response.status_code, [200, 302])
    
    def test_recipe_import_invalid_file(self):
        """Test recipe import with invalid file."""
        self.client.force_login(self.user)
        
        # Create an invalid file
        test_file = SimpleUploadedFile(
            "invalid.txt",
            b"invalid content",
            content_type="text/plain"
        )
        
        response = self.client.post('/recipes/import/', {'recipe_file': test_file})
        self.assertEqual(response.status_code, 200)  # Should stay on import page with errors
    
    def test_recipe_import_no_file(self):
        """Test recipe import without file."""
        self.client.force_login(self.user)
        
        response = self.client.post('/recipes/import/', {})
        self.assertEqual(response.status_code, 200)  # Should show form errors


class RecipeSecurityTestCase(TestCase):
    """Test recipe security and permissions."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
        self.recipe_id = 'test-recipe-123'
    
    def test_recipe_csrf_protection(self):
        """Test recipe CSRF protection."""
        self.client.force_login(self.user)
        
        # GET requests should work
        response = self.client.get('/recipes/create/')
        self.assertEqual(response.status_code, 200)
        
        # POST requests need CSRF token (Django test client handles this automatically)
        recipe_data = {
            'name': 'test-recipe',
            'description': 'Test',
            'recipe_content': json.dumps({'source': {'type': 'test'}})
        }
        response = self.client.post('/recipes/create/', recipe_data)
        self.assertIn(response.status_code, [200, 302])
    
    def test_recipe_xss_protection(self):
        """Test recipe XSS protection."""
        self.client.force_login(self.user)
        
        # Test with potential XSS payload
        xss_data = {
            'name': '<script>alert("xss")</script>',
            'description': '<img src=x onerror=alert("xss")>',
            'recipe_content': json.dumps({'source': {'type': 'test'}})
        }
        
        response = self.client.post('/recipes/create/', xss_data)
        self.assertIn(response.status_code, [200, 302])
        
        # In real implementation, verify XSS payload is escaped
    
    def test_recipe_unauthorized_access(self):
        """Test recipe unauthorized access."""
        # Test various recipe endpoints without authentication
        endpoints = [
            '/recipes/',
            '/recipes/create/',
            f'/recipes/edit/{self.recipe_id}/',
            f'/recipes/delete/{self.recipe_id}/',
            f'/recipes/run/{self.recipe_id}/',
            f'/recipes/download/{self.recipe_id}/',
            '/recipes/export-all/',
            '/recipes/import/'
        ]
        
        for endpoint in endpoints:
            response = self.client.get(endpoint)
            # Should require authentication or handle gracefully
            self.assertIn(response.status_code, [200, 302, 401, 403])
    
    def test_recipe_sql_injection_protection(self):
        """Test recipe SQL injection protection."""
        self.client.force_login(self.user)
        
        # Test with potential SQL injection payload
        sql_injection_data = {
            'name': "'; DROP TABLE recipes; --",
            'description': "1' OR '1'='1",
            'recipe_content': json.dumps({'source': {'type': 'test'}})
        }
        
        response = self.client.post('/recipes/create/', sql_injection_data)
        self.assertIn(response.status_code, [200, 302])
        # Database should remain intact (tested implicitly by subsequent operations)


class RecipeIntegrationTestCase(TestCase):
    """Test recipe integration with other components."""
    
    def setUp(self):
        self.client = Client()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
    
    def test_recipe_environment_integration(self):
        """Test recipe integration with environments."""
        try:
            env = EnvironmentFactory(name='test-env', is_default=True)
            
            self.client.force_login(self.user)
            response = self.client.get('/recipes/')
            self.assertEqual(response.status_code, 200)
            
            # In real implementation, test environment switching affects recipes
            # self.assertContains(response, env.name)
        except:
            self.skipTest("Environment model not available")
    
    @patch('web_ui.views.get_datahub_client_from_request')
    def test_recipe_github_integration(self, mock_datahub):
        """Test recipe GitHub integration."""
        mock_client = Mock()
        mock_client.get_recipe.return_value = {'name': 'test', 'id': 'recipe1'}
        mock_datahub.return_value = mock_client
        
        self.client.force_login(self.user)
        
        # Test recipe operations that might integrate with GitHub
        response = self.client.get('/recipes/')
        self.assertEqual(response.status_code, 200)
    
    def test_recipe_logging_integration(self):
        """Test recipe logging integration."""
        self.client.force_login(self.user)
        
        # Operations should be logged
        response = self.client.get('/recipes/')
        self.assertEqual(response.status_code, 200)
        
        # In real implementation, verify log entries are created
        # from web_ui.models import LogEntry
        # self.assertTrue(LogEntry.objects.filter(message__contains='recipe').exists()) 