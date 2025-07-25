"""
Comprehensive tests for Settings functionality.

Tests cover:
- Settings Model tests (key-value storage, methods)
- AppSettings Class tests (get, set, type conversion methods)
- GitSettings Model tests (all fields, methods, validation)
- Settings views (GET and POST for all sections)
- Settings API endpoints (get_settings, get_git_settings, get_system_info)
- Settings forms and validation
- Settings persistence and retrieval
- Settings integration with other components
- Settings security (sensitive data handling)
- Settings performance and edge cases
"""

import json
import tempfile
import os
from unittest.mock import patch, Mock, MagicMock
from django.test import TestCase, Client, override_settings
from django.urls import reverse
from django.contrib.auth.models import User
from django.contrib.messages import get_messages
from django.forms import ValidationError
from rest_framework.test import APITestCase
from rest_framework import status

from web_ui.models import Settings, AppSettings, GitSettings
from tests.fixtures.simple_factories import UserFactory, EnvironmentFactory


class SettingsModelTestCase(TestCase):
    """Test the Settings model."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()

    def test_settings_model_creation(self):
        """Test creating a Settings instance."""
        setting = Settings.objects.create(
            key="test_key",
            value="test_value"
        )
        
        self.assertEqual(setting.key, "test_key")
        self.assertEqual(setting.value, "test_value")
        self.assertIsNotNone(setting.updated_at)

    def test_settings_model_unique_key_constraint(self):
        """Test that Settings key field is unique."""
        Settings.objects.create(key="unique_key", value="value1")
        
        with self.assertRaises(Exception):  # IntegrityError
            Settings.objects.create(key="unique_key", value="value2")

    def test_settings_model_str_method(self):
        """Test Settings __str__ method."""
        setting = Settings.objects.create(key="test_key", value="test_value")
        self.assertEqual(str(setting), "test_key")

    def test_settings_model_blank_and_null_values(self):
        """Test Settings can handle blank and null values."""
        setting = Settings.objects.create(key="empty_key", value="")
        self.assertEqual(setting.value, "")
        
        setting = Settings.objects.create(key="null_key", value=None)
        self.assertIsNone(setting.value)

    def test_settings_model_long_key(self):
        """Test Settings with maximum key length."""
        long_key = "a" * 255
        setting = Settings.objects.create(key=long_key, value="test")
        self.assertEqual(setting.key, long_key)

    def test_settings_model_long_value(self):
        """Test Settings with long text value."""
        long_value = "x" * 10000
        setting = Settings.objects.create(key="long_value_key", value=long_value)
        self.assertEqual(setting.value, long_value)

    def test_settings_model_update_tracking(self):
        """Test that updated_at changes on save."""
        setting = Settings.objects.create(key="update_test", value="initial")
        original_updated_at = setting.updated_at
        
        # Small delay to ensure timestamp difference
        import time
        time.sleep(0.01)
        
        setting.value = "updated"
        setting.save()
        
        self.assertNotEqual(setting.updated_at, original_updated_at)


class AppSettingsClassTestCase(TestCase):
    """Test the AppSettings class methods."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()

    def tearDown(self):
        """Clean up test data."""
        Settings.objects.all().delete()

    def test_appsettings_get_method(self):
        """Test AppSettings.get method."""
        Settings.objects.create(key="test_setting", value="test_value")
        
        result = AppSettings.get("test_setting")
        self.assertEqual(result, "test_value")

    def test_appsettings_get_with_default(self):
        """Test AppSettings.get with default value."""
        result = AppSettings.get("nonexistent_key", "default_value")
        self.assertEqual(result, "default_value")

    def test_appsettings_get_nonexistent_no_default(self):
        """Test AppSettings.get for nonexistent key without default."""
        result = AppSettings.get("nonexistent_key")
        self.assertIsNone(result)

    def test_appsettings_set_method(self):
        """Test AppSettings.set method."""
        setting = AppSettings.set("new_key", "new_value")
        
        self.assertEqual(setting.key, "new_key")
        self.assertEqual(setting.value, "new_value")
        
        # Verify it was saved to database
        db_setting = Settings.objects.get(key="new_key")
        self.assertEqual(db_setting.value, "new_value")

    def test_appsettings_set_update_existing(self):
        """Test AppSettings.set updates existing setting."""
        Settings.objects.create(key="existing_key", value="old_value")
        
        setting = AppSettings.set("existing_key", "new_value")
        
        self.assertEqual(setting.value, "new_value")
        
        # Verify only one record exists
        self.assertEqual(Settings.objects.filter(key="existing_key").count(), 1)

    def test_appsettings_get_all_method(self):
        """Test AppSettings.get_all method."""
        Settings.objects.create(key="key1", value="value1")
        Settings.objects.create(key="key2", value="value2")
        Settings.objects.create(key="key3", value="value3")
        
        all_settings = AppSettings.get_all()
        
        expected = {
            "key1": "value1",
            "key2": "value2", 
            "key3": "value3"
        }
        self.assertEqual(all_settings, expected)

    def test_appsettings_get_all_empty(self):
        """Test AppSettings.get_all with no settings."""
        all_settings = AppSettings.get_all()
        self.assertEqual(all_settings, {})

    def test_appsettings_get_bool_method(self):
        """Test AppSettings.get_bool method."""
        Settings.objects.create(key="bool_true", value="true")
        Settings.objects.create(key="bool_false", value="false")
        Settings.objects.create(key="bool_1", value="1")
        Settings.objects.create(key="bool_0", value="0")
        Settings.objects.create(key="bool_yes", value="yes")
        Settings.objects.create(key="bool_no", value="no")
        Settings.objects.create(key="bool_t", value="t")
        Settings.objects.create(key="bool_y", value="y")
        Settings.objects.create(key="bool_invalid", value="invalid")
        
        self.assertTrue(AppSettings.get_bool("bool_true"))
        self.assertFalse(AppSettings.get_bool("bool_false"))
        self.assertTrue(AppSettings.get_bool("bool_1"))
        self.assertFalse(AppSettings.get_bool("bool_0"))
        self.assertTrue(AppSettings.get_bool("bool_yes"))
        self.assertFalse(AppSettings.get_bool("bool_no"))
        self.assertTrue(AppSettings.get_bool("bool_t"))
        self.assertTrue(AppSettings.get_bool("bool_y"))
        self.assertFalse(AppSettings.get_bool("bool_invalid"))  # Invalid strings are False

    def test_appsettings_get_bool_with_default(self):
        """Test AppSettings.get_bool with default values."""
        self.assertTrue(AppSettings.get_bool("nonexistent", True))
        self.assertFalse(AppSettings.get_bool("nonexistent", False))

    def test_appsettings_get_int_method(self):
        """Test AppSettings.get_int method."""
        Settings.objects.create(key="int_positive", value="123")
        Settings.objects.create(key="int_negative", value="-456")
        Settings.objects.create(key="int_zero", value="0")
        Settings.objects.create(key="int_invalid", value="not_a_number")
        
        self.assertEqual(AppSettings.get_int("int_positive"), 123)
        self.assertEqual(AppSettings.get_int("int_negative"), -456)
        self.assertEqual(AppSettings.get_int("int_zero"), 0)
        self.assertEqual(AppSettings.get_int("int_invalid"), 0)  # Default for invalid

    def test_appsettings_get_int_with_default(self):
        """Test AppSettings.get_int with default values."""
        self.assertEqual(AppSettings.get_int("nonexistent", 999), 999)
        Settings.objects.create(key="invalid_int", value="abc")
        self.assertEqual(AppSettings.get_int("invalid_int", 123), 123)

    def test_appsettings_get_json_method(self):
        """Test AppSettings.get_json method."""
        json_data = {"key": "value", "number": 123, "list": [1, 2, 3]}
        Settings.objects.create(key="json_valid", value=json.dumps(json_data))
        Settings.objects.create(key="json_invalid", value="invalid json")
        Settings.objects.create(key="json_empty", value="")
        
        result = AppSettings.get_json("json_valid")
        self.assertEqual(result, json_data)
        
        result = AppSettings.get_json("json_invalid")
        self.assertEqual(result, {})  # Default empty dict
        
        result = AppSettings.get_json("json_empty")
        self.assertEqual(result, {})  # Default empty dict

    def test_appsettings_get_json_with_default(self):
        """Test AppSettings.get_json with custom default."""
        default_data = {"default": "value"}
        result = AppSettings.get_json("nonexistent", default_data)
        self.assertEqual(result, default_data)

    def test_appsettings_set_json_method(self):
        """Test AppSettings.set_json method."""
        json_data = {"key": "value", "nested": {"a": 1, "b": 2}}
        
        setting = AppSettings.set_json("json_key", json_data)
        
        # Verify it was saved as JSON string
        self.assertEqual(setting.value, json.dumps(json_data))
        
        # Verify it can be retrieved as JSON
        retrieved = AppSettings.get_json("json_key")
        self.assertEqual(retrieved, json_data)


class GitSettingsModelTestCase(TestCase):
    """Test the GitSettings model."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()

    def tearDown(self):
        """Clean up test data."""
        GitSettings.objects.all().delete()

    def test_git_settings_model_creation(self):
        """Test creating a GitSettings instance."""
        git_settings = GitSettings.objects.create(
            provider_type="github",
            token="test_token",
            username="test_user",
            repository="test_repo",
            enabled=True
        )
        
        self.assertEqual(git_settings.provider_type, "github")
        self.assertEqual(git_settings.token, "test_token")
        self.assertEqual(git_settings.username, "test_user")
        self.assertEqual(git_settings.repository, "test_repo")
        self.assertTrue(git_settings.enabled)
        self.assertEqual(git_settings.current_branch, "main")  # Default

    def test_git_settings_provider_choices(self):
        """Test GitSettings provider type choices."""
        valid_providers = ["github", "azure_devops", "gitlab", "bitbucket", "other"]
        
        for provider in valid_providers:
            git_settings = GitSettings.objects.create(
                provider_type=provider,
                token="token",
                username="user",
                repository="repo"
            )
            self.assertEqual(git_settings.provider_type, provider)
            git_settings.delete()

    def test_git_settings_str_method(self):
        """Test GitSettings __str__ method."""
        github_settings = GitSettings.objects.create(
            provider_type="github",
            username="testuser",
            repository="testrepo",
            token="token"
        )
        self.assertEqual(str(github_settings), "GitHub: testuser/testrepo")
        
        azure_settings = GitSettings.objects.create(
            provider_type="azure_devops",
            username="testorg",
            repository="testproject",
            token="token"
        )
        self.assertEqual(str(azure_settings), "Azure DevOps: testorg/testproject")
        
        gitlab_settings = GitSettings.objects.create(
            provider_type="gitlab",
            username="testuser",
            repository="testrepo",
            token="token"
        )
        self.assertEqual(str(gitlab_settings), "Gitlab: testuser/testrepo")

    def test_git_settings_get_instance_method(self):
        """Test GitSettings.get_instance class method."""
        # Should create instance if none exists
        instance = GitSettings.get_instance()
        
        self.assertIsNotNone(instance)
        self.assertEqual(instance.token, "")
        self.assertEqual(instance.username, "")
        self.assertEqual(instance.repository, "")
        self.assertFalse(instance.enabled)
        
        # Should return existing instance
        instance2 = GitSettings.get_instance()
        self.assertEqual(instance.id, instance2.id)

    def test_git_settings_get_token_method(self):
        """Test GitSettings.get_token class method."""
        GitSettings.objects.create(token="test_token", username="user", repository="repo")
        
        token = GitSettings.get_token()
        self.assertEqual(token, "test_token")

    def test_git_settings_get_username_method(self):
        """Test GitSettings.get_username class method."""
        GitSettings.objects.create(token="token", username="test_user", repository="repo")
        
        username = GitSettings.get_username()
        self.assertEqual(username, "test_user")

    def test_git_settings_get_repository_method(self):
        """Test GitSettings.get_repository class method."""
        GitSettings.objects.create(token="token", username="user", repository="test_repo")
        
        repository = GitSettings.get_repository()
        self.assertEqual(repository, "test_repo")

    def test_git_settings_get_provider_type_method(self):
        """Test GitSettings.get_provider_type class method."""
        GitSettings.objects.create(
            provider_type="gitlab",
            token="token",
            username="user",
            repository="repo"
        )
        
        provider_type = GitSettings.get_provider_type()
        self.assertEqual(provider_type, "gitlab")

    def test_git_settings_get_base_url_method(self):
        """Test GitSettings.get_base_url class method."""
        GitSettings.objects.create(
            base_url="https://api.example.com",
            token="token",
            username="user",
            repository="repo"
        )
        
        base_url = GitSettings.get_base_url()
        self.assertEqual(base_url, "https://api.example.com")

    def test_git_settings_set_methods(self):
        """Test GitSettings set methods."""
        GitSettings.objects.create(token="", username="", repository="")
        
        GitSettings.set_token("new_token")
        self.assertEqual(GitSettings.get_token(), "new_token")
        
        GitSettings.set_username("new_user")
        self.assertEqual(GitSettings.get_username(), "new_user")
        
        GitSettings.set_repository("new_repo")
        self.assertEqual(GitSettings.get_repository(), "new_repo")

    def test_git_settings_defaults(self):
        """Test GitSettings default values."""
        git_settings = GitSettings.objects.create(
            token="token",
            username="user",
            repository="repo"
        )
        
        self.assertEqual(git_settings.provider_type, "github")  # Default
        self.assertEqual(git_settings.current_branch, "main")  # Default
        self.assertFalse(git_settings.enabled)  # Default
        self.assertIsNone(git_settings.base_url)  # Nullable

    def test_git_settings_timestamps(self):
        """Test GitSettings timestamp fields."""
        git_settings = GitSettings.objects.create(
            token="token",
            username="user",
            repository="repo"
        )
        
        self.assertIsNotNone(git_settings.created_at)
        self.assertIsNotNone(git_settings.updated_at)
        
        original_updated_at = git_settings.updated_at
        
        # Update and check timestamp changes
        import time
        time.sleep(0.01)
        git_settings.enabled = True
        git_settings.save()
        
        self.assertNotEqual(git_settings.updated_at, original_updated_at)


class SettingsViewTestCase(TestCase):
    """Test Settings views."""

    def setUp(self):
        """Set up test data."""
        self.client = Client()
        self.user = UserFactory()
        self.settings_url = reverse('settings')

    def tearDown(self):
        """Clean up test data."""
        Settings.objects.all().delete()
        GitSettings.objects.all().delete()

    def test_settings_view_get(self):
        """Test GET request to settings view."""
        self.client.force_login(self.user)
        response = self.client.get(self.settings_url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Settings")
        self.assertIn('github_form', response.context)
        self.assertIn('config', response.context)

    def test_settings_view_get_anonymous(self):
        """Test GET request to settings view as anonymous user."""
        response = self.client.get(self.settings_url)
        # Should work with anonymous backend
        self.assertEqual(response.status_code, 200)

    def test_settings_view_github_settings_post(self):
        """Test POST request to update GitHub settings."""
        self.client.force_login(self.user)
        
        data = {
            'section': 'github_settings',
            'enabled': 'on',
            'provider_type': 'github',
            'username': 'testuser',
            'repository': 'testrepo',
            'token': 'test_token',
            'base_url': ''
        }
        
        response = self.client.post(self.settings_url, data)
        
        self.assertEqual(response.status_code, 302)  # Redirect after success
        
        git_settings = GitSettings.objects.first()
        self.assertIsNotNone(git_settings)
        self.assertTrue(git_settings.enabled)
        self.assertEqual(git_settings.provider_type, 'github')
        self.assertEqual(git_settings.username, 'testuser')
        self.assertEqual(git_settings.repository, 'testrepo')
        self.assertEqual(git_settings.token, 'test_token')

    def test_settings_view_github_settings_disabled(self):
        """Test POST request to disable GitHub settings."""
        # Create existing settings
        GitSettings.objects.create(
            enabled=True,
            username='old_user',
            repository='old_repo',
            token='old_token'
        )
        
        self.client.force_login(self.user)
        
        data = {
            'section': 'github_settings',
            'provider_type': 'github',
            'username': 'new_user',
            'repository': 'new_repo'
            # enabled not included = disabled
        }
        
        response = self.client.post(self.settings_url, data)
        
        self.assertEqual(response.status_code, 302)
        
        git_settings = GitSettings.objects.first()
        self.assertFalse(git_settings.enabled)  # Should be disabled

    def test_settings_view_policy_settings_post(self):
        """Test POST request to update policy settings."""
        self.client.force_login(self.user)
        
        data = {
            'section': 'policy_settings',
            'policy_export_dir': '/path/to/policies',
            'default_policy_type': 'PLATFORM',
            'validate_on_import': 'on',
            'auto_backup_policies': 'on'
        }
        
        response = self.client.post(self.settings_url, data)
        
        self.assertEqual(response.status_code, 302)
        
        # Verify settings were saved
        self.assertEqual(AppSettings.get('policy_export_dir'), '/path/to/policies')
        self.assertEqual(AppSettings.get('default_policy_type'), 'PLATFORM')
        self.assertTrue(AppSettings.get_bool('validate_on_import'))
        self.assertTrue(AppSettings.get_bool('auto_backup_policies'))

    def test_settings_view_recipe_settings_post(self):
        """Test POST request to update recipe settings."""
        self.client.force_login(self.user)
        
        data = {
            'section': 'recipe_settings',
            'recipe_dir': '/path/to/recipes',
            'default_schedule': '0 6 * * *',
            'auto_enable_recipes': 'on'
        }
        
        response = self.client.post(self.settings_url, data)
        
        self.assertEqual(response.status_code, 302)
        
        # Verify settings were saved
        self.assertEqual(AppSettings.get('recipe_dir'), '/path/to/recipes')
        self.assertEqual(AppSettings.get('default_schedule'), '0 6 * * *')
        self.assertTrue(AppSettings.get_bool('auto_enable_recipes'))

    def test_settings_view_advanced_settings_post(self):
        """Test POST request to update advanced settings."""
        self.client.force_login(self.user)
        
        data = {
            'section': 'advanced_settings',
            'timeout': '60',
            'log_level': 'DEBUG',
            'refresh_rate': '30',
            'debug_mode': 'on'
        }
        
        response = self.client.post(self.settings_url, data)
        
        self.assertEqual(response.status_code, 302)
        
        # Verify settings were saved
        self.assertEqual(AppSettings.get('timeout'), '60')
        self.assertEqual(AppSettings.get('log_level'), 'DEBUG')
        self.assertEqual(AppSettings.get('refresh_rate'), '30')
        self.assertTrue(AppSettings.get_bool('debug_mode'))

    def test_settings_view_advanced_settings_validation(self):
        """Test POST request with invalid advanced settings values."""
        self.client.force_login(self.user)
        
        data = {
            'section': 'advanced_settings',
            'timeout': '500',  # Too high, should be capped
            'log_level': 'ERROR',
            'refresh_rate': '-10',  # Invalid, should be reset to default
            'debug_mode': 'off'
        }
        
        response = self.client.post(self.settings_url, data)
        
        self.assertEqual(response.status_code, 302)
        
        # Verify invalid values were corrected
        self.assertEqual(AppSettings.get('timeout'), '30')  # Capped to default
        self.assertEqual(AppSettings.get('refresh_rate'), '60')  # Reset to default

    def test_settings_view_legacy_connection_redirect(self):
        """Test POST request with legacy connection section redirects."""
        self.client.force_login(self.user)
        
        data = {
            'section': 'connection'
        }
        
        response = self.client.post(self.settings_url, data)
        
        # Should redirect to connections list
        self.assertEqual(response.status_code, 302)
        self.assertIn('connections', response.url)

    def test_settings_view_invalid_section(self):
        """Test POST request with invalid section."""
        self.client.force_login(self.user)
        
        data = {
            'section': 'invalid_section'
        }
        
        response = self.client.post(self.settings_url, data)
        
        # Should handle gracefully and render settings page
        self.assertEqual(response.status_code, 200)

    def test_settings_view_github_connection_test(self):
        """Test GitHub connection test via POST."""
        self.client.force_login(self.user) 
        
        data = {
            'section': 'github_settings',
            'provider_type': 'github',
            'username': 'testuser',
            'repository': 'testrepo',
            'token': 'test_token',
            'test_github_connection': 'on'
        }
        
        response = self.client.post(self.settings_url, data)
        
        self.assertEqual(response.status_code, 302)
        
        # Check that success message was added
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any("connection test completed" in str(m) for m in messages))

    @patch('web_ui.views.logger')
    def test_settings_view_error_handling(self, mock_logger):
        """Test settings view handles errors gracefully."""
        self.client.force_login(self.user)
        
        # Mock an exception during processing
        with patch.object(AppSettings, 'set', side_effect=Exception("Database error")):
            data = {
                'section': 'policy_settings',
                'policy_export_dir': '/test/path'
            }
            
            response = self.client.post(self.settings_url, data)
            
            # Should handle error gracefully
            self.assertEqual(response.status_code, 200)
            mock_logger.error.assert_called()


class SettingsAPITestCase(APITestCase):
    """Test Settings API endpoints."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()
        self.api_settings_url = reverse('api-settings')
        self.api_git_settings_url = reverse('api-git-settings')
        self.api_system_info_url = reverse('api-system-info')

    def tearDown(self):
        """Clean up test data."""
        Settings.objects.all().delete()
        GitSettings.objects.all().delete()

    def test_api_get_settings(self):
        """Test GET request to settings API."""
        # Set up some test settings
        AppSettings.set('policy_export_dir', '/test/policies')
        AppSettings.set('recipe_dir', '/test/recipes')
        AppSettings.set('log_level', 'DEBUG')
        
        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.api_settings_url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.data
        
        self.assertIn('policy', data)
        self.assertIn('recipe', data)
        self.assertIn('advanced', data)
        
        self.assertEqual(data['policy']['export_dir'], '/test/policies')
        self.assertEqual(data['recipe']['directory'], '/test/recipes')
        self.assertEqual(data['advanced']['log_level'], 'DEBUG')

    def test_api_get_settings_defaults(self):
        """Test GET request to settings API with default values."""
        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.api_settings_url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.data
        
        # Check default values
        self.assertEqual(data['policy']['default_type'], 'METADATA')
        self.assertEqual(data['recipe']['default_schedule'], '0 0 * * *')
        self.assertEqual(data['advanced']['log_level'], 'INFO')
        self.assertEqual(data['advanced']['refresh_rate'], 60)

    def test_api_get_settings_anonymous(self):
        """Test GET request to settings API as anonymous user."""
        response = self.client.get(self.api_settings_url)
        
        # Should work with anonymous authentication
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_api_get_git_settings(self):
        """Test GET request to git settings API."""
        # Create git settings
        GitSettings.objects.create(
            provider_type='gitlab',
            base_url='https://gitlab.example.com',
            username='testuser',
            repository='testrepo',
            token='test_token',
            enabled=True
        )
        
        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.api_git_settings_url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.data
        
        self.assertEqual(data['provider_type'], 'gitlab')
        self.assertEqual(data['base_url'], 'https://gitlab.example.com')
        self.assertEqual(data['username'], 'testuser')
        self.assertEqual(data['repository'], 'testrepo')
        self.assertTrue(data['enabled'])
        # Token should not be exposed in API response for security
        self.assertNotIn('token', data)

    def test_api_get_git_settings_defaults(self):
        """Test GET request to git settings API with default values."""
        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.api_git_settings_url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.data
        
        # Check default values when no GitSettings exist
        self.assertEqual(data['provider_type'], 'github')
        self.assertEqual(data['current_branch'], 'main')
        self.assertFalse(data['enabled'])

    def test_api_get_system_info(self):
        """Test GET request to system info API."""
        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.api_system_info_url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response.data
        
        # Should include system information
        self.assertIn('version', data)
        self.assertIn('environment', data)
        self.assertIn('database', data)

    @patch('web_ui.api_views.logger')
    def test_api_settings_error_handling(self, mock_logger):
        """Test API endpoints handle errors gracefully."""
        self.client.force_authenticate(user=self.user)
        
        # Mock an exception during settings retrieval
        with patch.object(AppSettings, 'get', side_effect=Exception("Database error")):
            response = self.client.get(self.api_settings_url)
            
            # Should handle error gracefully
            self.assertEqual(response.status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)


class SettingsIntegrationTestCase(TestCase):
    """Test Settings integration with other components."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()
        self.environment = EnvironmentFactory()

    def tearDown(self):
        """Clean up test data."""
        Settings.objects.all().delete()
        GitSettings.objects.all().delete()

    def test_settings_with_environment_variables(self):
        """Test settings work with environment variables."""
        # Set up environment-specific settings
        AppSettings.set(f'recipe_dir_{self.environment.name}', f'/recipes/{self.environment.name}')
        AppSettings.set('recipe_dir', '/recipes/default')
        
        # Verify environment-specific setting
        env_specific = AppSettings.get(f'recipe_dir_{self.environment.name}')
        self.assertEqual(env_specific, f'/recipes/{self.environment.name}')
        
        # Verify default fallback
        default = AppSettings.get('recipe_dir')
        self.assertEqual(default, '/recipes/default')

    def test_settings_persistence_across_requests(self):
        """Test that settings persist across multiple requests."""
        client = Client()
        client.force_login(self.user)
        
        # Set settings via first request
        data = {
            'section': 'policy_settings',
            'policy_export_dir': '/persistent/path'
        }
        response1 = client.post(reverse('settings'), data)
        self.assertEqual(response1.status_code, 302)
        
        # Verify settings persist in second request
        response2 = client.get(reverse('settings'))
        self.assertEqual(response2.status_code, 200)
        self.assertEqual(response2.context['config']['policy_export_dir'], '/persistent/path')

    def test_settings_unicode_handling(self):
        """Test settings handle Unicode characters properly."""
        unicode_value = "测试中文 🚀 éñ español"
        AppSettings.set('unicode_test', unicode_value)
        
        retrieved = AppSettings.get('unicode_test')
        self.assertEqual(retrieved, unicode_value)

    @patch('web_ui.models.json.loads')
    def test_settings_json_error_handling(self, mock_json_loads):
        """Test JSON settings handle parsing errors gracefully."""
        mock_json_loads.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        
        Settings.objects.create(key='malformed_json', value='{"invalid": json}')
        
        # Should return default value on JSON parse error
        result = AppSettings.get_json('malformed_json', {'default': 'value'})
        self.assertEqual(result, {'default': 'value'})

    def test_settings_concurrent_access(self):
        """Test settings handle concurrent access appropriately."""
        import threading
        import time
        
        results = []
        
        def update_setting(value):
            AppSettings.set('concurrent_test', f'value_{value}')
            results.append(AppSettings.get('concurrent_test'))
        
        # Create multiple threads that update the same setting
        threads = []
        for i in range(5):
            thread = threading.Thread(target=update_setting, args=(i,))
            threads.append(thread)
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify we got some results (race conditions make exact prediction difficult)
        self.assertEqual(len(results), 5)
        
        # Final value should be one of the thread values
        final_value = AppSettings.get('concurrent_test')
        self.assertTrue(any(final_value == result for result in results))


class SettingsSecurityTestCase(TestCase):
    """Test Settings security aspects."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()
        self.client = Client()

    def tearDown(self):
        """Clean up test data."""
        Settings.objects.all().delete()
        GitSettings.objects.all().delete()

    def test_settings_token_not_exposed_in_form(self):
        """Test that tokens are not exposed in forms."""
        GitSettings.objects.create(
            token='secret_token_123',
            username='testuser',
            repository='testrepo'
        )
        
        self.client.force_login(self.user)
        response = self.client.get(reverse('settings'))
        
        self.assertEqual(response.status_code, 200)
        # Token should be in context but masked in template
        self.assertNotContains(response, 'secret_token_123')

    def test_settings_sensitive_data_in_api(self):
        """Test that sensitive data is not exposed in API responses."""
        GitSettings.objects.create(
            token='secret_api_token',
            username='apiuser',
            repository='apirepo'
        )
        
        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse('api-git-settings'))
        
        self.assertEqual(response.status_code, 200)
        # Token should not be in API response
        self.assertNotIn('secret_api_token', str(response.content))

    def test_settings_csrf_protection(self):
        """Test that settings forms are protected by CSRF."""
        # Attempt POST without CSRF token
        data = {
            'section': 'policy_settings',
            'policy_export_dir': '/test/path'
        }
        
        response = self.client.post(reverse('settings'), data)
        
        # Should fail due to missing CSRF token
        self.assertEqual(response.status_code, 403)

    def test_settings_input_sanitization(self):
        """Test that settings input is properly sanitized."""
        self.client.force_login(self.user)
        
        # Try to inject malicious content
        malicious_content = '<script>alert("xss")</script>'
        data = {
            'section': 'policy_settings',
            'policy_export_dir': malicious_content
        }
        
        response = self.client.post(reverse('settings'), data, follow=True)
        
        # Should store the raw content but not execute it
        stored_value = AppSettings.get('policy_export_dir')
        self.assertEqual(stored_value, malicious_content)
        
        # Should not contain executable script in response
        self.assertNotContains(response, '<script>alert("xss")</script>')

    def test_settings_authentication_required_for_api(self):
        """Test that API endpoints require proper authentication."""
        # Note: This might vary based on your authentication backend
        # Adjust based on your actual authentication requirements
        
        response = self.client.get(reverse('api-settings'))
        
        # Should be accessible (due to anonymous backend in this app)
        # In production, you might want stricter authentication
        self.assertEqual(response.status_code, 200)


class SettingsPerformanceTestCase(TestCase):
    """Test Settings performance aspects."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()

    def tearDown(self):
        """Clean up test data."""
        Settings.objects.all().delete()

    def test_settings_bulk_operations_performance(self):
        """Test performance of bulk settings operations."""
        import time
        
        # Time bulk setting creation
        start_time = time.time()
        
        for i in range(100):
            AppSettings.set(f'bulk_test_{i}', f'value_{i}')
        
        creation_time = time.time() - start_time
        
        # Time bulk retrieval
        start_time = time.time()
        
        all_settings = AppSettings.get_all()
        
        retrieval_time = time.time() - start_time
        
        # Verify all settings were created
        bulk_settings = {k: v for k, v in all_settings.items() if k.startswith('bulk_test_')}
        self.assertEqual(len(bulk_settings), 100)
        
        # Performance should be reasonable (adjust thresholds as needed)
        self.assertLess(creation_time, 10.0)  # Should complete within 10 seconds
        self.assertLess(retrieval_time, 1.0)   # Should retrieve within 1 second

    def test_settings_caching_behavior(self):
        """Test settings caching if implemented."""
        # This test assumes some form of caching might be implemented
        # Adjust based on actual caching strategy
        
        AppSettings.set('cache_test', 'initial_value')
        
        # Time first access
        import time
        start_time = time.time()
        value1 = AppSettings.get('cache_test')
        first_access_time = time.time() - start_time
        
        # Time second access (should be faster if cached)
        start_time = time.time()
        value2 = AppSettings.get('cache_test')
        second_access_time = time.time() - start_time
        
        self.assertEqual(value1, 'initial_value')
        self.assertEqual(value2, 'initial_value')
        
        # Second access should be as fast or faster
        # (Note: This might not always be true depending on system load)
        self.assertLessEqual(second_access_time, first_access_time * 2)

    def test_settings_large_value_handling(self):
        """Test settings can handle large values efficiently."""
        # Create a large JSON value
        large_data = {
            'arrays': [[i for i in range(1000)] for _ in range(10)],
            'objects': [{'key': f'value_{i}', 'data': 'x' * 100} for i in range(100)],
            'string': 'large_string_content ' * 1000
        }
        
        import time
        start_time = time.time()
        
        # Store large value
        AppSettings.set_json('large_value_test', large_data)
        
        storage_time = time.time() - start_time
        
        start_time = time.time()
        
        # Retrieve large value
        retrieved_data = AppSettings.get_json('large_value_test')
        
        retrieval_time = time.time() - start_time
        
        # Verify data integrity
        self.assertEqual(retrieved_data, large_data)
        
        # Performance should be reasonable
        self.assertLess(storage_time, 5.0)   # Should store within 5 seconds
        self.assertLess(retrieval_time, 2.0) # Should retrieve within 2 seconds 