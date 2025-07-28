"""
Comprehensive integration tests for full workflows across the web_ui application.

Tests cover:
- Complete user workflows from frontend to backend
- Integration between Python views, HTML templates, and JavaScript
- End-to-end policy management workflows
- Environment management and switching
- Log monitoring and management workflows
- GitHub integration workflows
- Multi-user scenarios and permissions
- Data flow across all application layers
"""

import pytest
import json
import time
from unittest.mock import patch, Mock
from django.test import TransactionTestCase, LiveServerTestCase
from django.contrib.auth.models import User
from django.urls import reverse
from django.db import transaction
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

from web_ui.web_ui.models import Environment, Policy, LogEntry, GitSettings, Mutation
from tests.fixtures.factories import (
    UserFactory, EnvironmentFactory, PolicyFactory, 
    LogEntryFactory, GitSettingsFactory, MutationFactory
)
from tests.utils.base_test import BaseWebUITestCase, MockHelper


class PolicyManagementWorkflowTestCase(LiveServerTestCase):
    """Test complete policy management workflows."""
    
    def setUp(self):
        super().setUp()
        self.browser = webdriver.Chrome(options=self._get_chrome_options())
        self.wait = WebDriverWait(self.browser, 10)
        
        # Create test users and environments
        self.admin_user = UserFactory.create_admin()
        self.regular_user = UserFactory.create_user()
        self.dev_env = EnvironmentFactory(name='development', is_default=True)
        self.prod_env = EnvironmentFactory(name='production')
    
    def tearDown(self):
        self.browser.quit()
        super().tearDown()
    
    def _get_chrome_options(self):
        """Get Chrome options for testing."""
        from selenium.webdriver.chrome.options import Options
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        return options
    
    def _login_user(self, user):
        """Login user via browser."""
        self.browser.get(f"{self.live_server_url}/accounts/login/")
        
        username_input = self.browser.find_element(By.NAME, "username")
        password_input = self.browser.find_element(By.NAME, "password")
        
        username_input.send_keys(user.username)
        password_input.send_keys("testpassword123")
        password_input.send_keys(Keys.RETURN)
        
        # Wait for redirect
        self.wait.until(EC.url_changes(f"{self.live_server_url}/accounts/login/"))
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_complete_policy_creation_workflow(self, mock_get_client):
        """Test complete policy creation from UI to database."""
        # Mock DataHub client
        mock_client = MockHelper.mock_datahub_client()
        mock_client.create_policy.return_value = {'success': True, 'urn': 'urn:li:dataHubPolicy:new-policy'}
        mock_get_client.return_value = mock_client
        
        # Login as admin
        self._login_user(self.admin_user)
        
        # Navigate to policy creation
        self.browser.get(f"{self.live_server_url}/policies/")
        create_button = self.wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Create Policy")))
        create_button.click()
        
        # Fill out policy form
        name_input = self.wait.until(EC.presence_of_element_located((By.NAME, "name")))
        description_input = self.browser.find_element(By.NAME, "description")
        environment_select = Select(self.browser.find_element(By.NAME, "environment"))
        policy_json_input = self.browser.find_element(By.NAME, "policy_json")
        
        # Input policy data
        policy_name = "Test Integration Policy"
        policy_description = "A policy created through integration test"
        policy_json = {
            "name": "test-integration-policy",
            "description": policy_description,
            "type": "METADATA",
            "state": "ACTIVE",
            "privileges": ["EDIT_ENTITY_OWNERS"],
            "actors": {"users": ["urn:li:corpuser:testuser"], "groups": []}
        }
        
        name_input.send_keys(policy_name)
        description_input.send_keys(policy_description)
        environment_select.select_by_visible_text(self.dev_env.name)
        policy_json_input.clear()
        policy_json_input.send_keys(json.dumps(policy_json, indent=2))
        
        # Submit form
        submit_button = self.browser.find_element(By.TYPE, "submit")
        submit_button.click()
        
        # Wait for redirect to policies list
        self.wait.until(EC.url_contains("/policies/"))
        
        # Verify policy was created in database
        policy = Policy.objects.filter(name=policy_name).first()
        self.assertIsNotNone(policy)
        self.assertEqual(policy.description, policy_description)
        self.assertEqual(policy.environment, self.dev_env)
        self.assertEqual(policy.policy_data['name'], 'test-integration-policy')
        
        # Verify policy appears in UI
        self.assertIn(policy_name, self.browser.page_source)
        
        # Verify DataHub client was called
        mock_client.create_policy.assert_called_once()
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_policy_edit_workflow(self, mock_get_client):
        """Test complete policy editing workflow."""
        # Create existing policy
        existing_policy = PolicyFactory(
            name="Original Policy",
            description="Original description",
            environment=self.dev_env
        )
        
        # Mock DataHub client
        mock_client = MockHelper.mock_datahub_client()
        mock_client.update_policy.return_value = {'success': True}
        mock_get_client.return_value = mock_client
        
        # Login and navigate to policy edit
        self._login_user(self.admin_user)
        self.browser.get(f"{self.live_server_url}/policies/{existing_policy.id}/edit/")
        
        # Update policy details
        name_input = self.wait.until(EC.presence_of_element_located((By.NAME, "name")))
        description_input = self.browser.find_element(By.NAME, "description")
        
        name_input.clear()
        name_input.send_keys("Updated Policy Name")
        
        description_input.clear()
        description_input.send_keys("Updated policy description")
        
        # Submit changes
        submit_button = self.browser.find_element(By.TYPE, "submit")
        submit_button.click()
        
        # Wait for redirect
        self.wait.until(EC.url_contains("/policies/"))
        
        # Verify changes in database
        existing_policy.refresh_from_db()
        self.assertEqual(existing_policy.name, "Updated Policy Name")
        self.assertEqual(existing_policy.description, "Updated policy description")
        
        # Verify changes in UI
        self.assertIn("Updated Policy Name", self.browser.page_source)
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_policy_deletion_workflow(self, mock_get_client):
        """Test complete policy deletion workflow."""
        # Create policy to delete
        policy_to_delete = PolicyFactory(
            name="Policy to Delete",
            environment=self.dev_env
        )
        policy_id = policy_to_delete.id
        
        # Mock DataHub client
        mock_client = MockHelper.mock_datahub_client()
        mock_client.delete_policy.return_value = {'success': True}
        mock_get_client.return_value = mock_client
        
        # Login and navigate to policies
        self._login_user(self.admin_user)
        self.browser.get(f"{self.live_server_url}/policies/")
        
        # Find and click delete button
        delete_link = self.wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, f"a[href*='/policies/{policy_id}/delete/']"))
        )
        delete_link.click()
        
        # Confirm deletion
        confirm_button = self.wait.until(EC.element_to_be_clickable((By.TYPE, "submit")))
        confirm_button.click()
        
        # Wait for redirect
        self.wait.until(EC.url_contains("/policies/"))
        
        # Verify policy was deleted from database
        self.assertFalse(Policy.objects.filter(id=policy_id).exists())
        
        # Verify policy no longer appears in UI
        self.assertNotIn("Policy to Delete", self.browser.page_source)


class EnvironmentManagementWorkflowTestCase(LiveServerTestCase):
    """Test complete environment management workflows."""
    
    def setUp(self):
        super().setUp()
        self.browser = webdriver.Chrome(options=self._get_chrome_options())
        self.wait = WebDriverWait(self.browser, 10)
        self.admin_user = UserFactory.create_admin()
    
    def tearDown(self):
        self.browser.quit()
        super().tearDown()
    
    def _get_chrome_options(self):
        from selenium.webdriver.chrome.options import Options
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        return options
    
    def _login_user(self, user):
        self.browser.get(f"{self.live_server_url}/accounts/login/")
        username_input = self.browser.find_element(By.NAME, "username")
        password_input = self.browser.find_element(By.NAME, "password")
        username_input.send_keys(user.username)
        password_input.send_keys("testpassword123")
        password_input.send_keys(Keys.RETURN)
        self.wait.until(EC.url_changes(f"{self.live_server_url}/accounts/login/"))
    
    def test_environment_creation_and_default_switching(self):
        """Test creating environment and default environment switching."""
        # Login as admin
        self._login_user(self.admin_user)
        
        # Navigate to environments
        self.browser.get(f"{self.live_server_url}/environments/")
        
        # Create first environment
        create_button = self.wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Create Environment")))
        create_button.click()
        
        # Fill out environment form
        name_input = self.wait.until(EC.presence_of_element_located((By.NAME, "name")))
        description_input = self.browser.find_element(By.NAME, "description")
        host_input = self.browser.find_element(By.NAME, "datahub_host")
        token_input = self.browser.find_element(By.NAME, "datahub_token")
        default_checkbox = self.browser.find_element(By.NAME, "is_default")
        
        name_input.send_keys("Development")
        description_input.send_keys("Development environment")
        host_input.send_keys("http://dev.datahub.com")
        token_input.send_keys("dev-token-123")
        default_checkbox.click()  # Make it default
        
        # Submit form
        submit_button = self.browser.find_element(By.TYPE, "submit")
        submit_button.click()
        
        # Wait for redirect
        self.wait.until(EC.url_contains("/environments/"))
        
        # Verify environment created and is default
        dev_env = Environment.objects.filter(name="Development").first()
        self.assertIsNotNone(dev_env)
        self.assertTrue(dev_env.is_default)
        
        # Create second environment and make it default
        create_button = self.browser.find_element(By.LINK_TEXT, "Create Environment")
        create_button.click()
        
        name_input = self.wait.until(EC.presence_of_element_located((By.NAME, "name")))
        description_input = self.browser.find_element(By.NAME, "description")
        host_input = self.browser.find_element(By.NAME, "datahub_host")
        token_input = self.browser.find_element(By.NAME, "datahub_token")
        default_checkbox = self.browser.find_element(By.NAME, "is_default")
        
        name_input.send_keys("Production")
        description_input.send_keys("Production environment")
        host_input.send_keys("http://prod.datahub.com")
        token_input.send_keys("prod-token-123")
        default_checkbox.click()  # Make this default instead
        
        submit_button = self.browser.find_element(By.TYPE, "submit")
        submit_button.click()
        
        self.wait.until(EC.url_contains("/environments/"))
        
        # Verify default environment switched
        dev_env.refresh_from_db()
        prod_env = Environment.objects.filter(name="Production").first()
        
        self.assertFalse(dev_env.is_default)
        self.assertTrue(prod_env.is_default)
        
        # Verify UI shows correct default
        self.assertIn("Default", self.browser.page_source)


class DashboardIntegrationWorkflowTestCase(LiveServerTestCase):
    """Test dashboard integration workflows."""
    
    def setUp(self):
        super().setUp()
        self.browser = webdriver.Chrome(options=self._get_chrome_options())
        self.wait = WebDriverWait(self.browser, 10)
        
        # Create test data
        self.user = UserFactory.create_user()
        self.environments = [
            EnvironmentFactory(name='dev', is_default=True),
            EnvironmentFactory(name='staging'),
            EnvironmentFactory(name='prod')
        ]
        self.policies = [
            PolicyFactory(name=f'Policy {i}', environment=self.environments[i % len(self.environments)])
            for i in range(5)
        ]
        self.logs = [
            LogEntryFactory(level='INFO', message=f'Info message {i}')
            for i in range(10)
        ]
    
    def tearDown(self):
        self.browser.quit()
        super().tearDown()
    
    def _get_chrome_options(self):
        from selenium.webdriver.chrome.options import Options
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        return options
    
    def _login_user(self, user):
        self.browser.get(f"{self.live_server_url}/accounts/login/")
        username_input = self.browser.find_element(By.NAME, "username")
        password_input = self.browser.find_element(By.NAME, "password")
        username_input.send_keys(user.username)
        password_input.send_keys("testpassword123")
        password_input.send_keys(Keys.RETURN)
        self.wait.until(EC.url_changes(f"{self.live_server_url}/accounts/login/"))
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_dashboard_data_integration(self, mock_get_client):
        """Test dashboard loads and displays integrated data from all components."""
        # Mock DataHub client
        mock_client = MockHelper.mock_datahub_client(
            connected=True,
            recipes=[
                {'name': 'recipe1', 'lastUpdated': 1640995200000, 'schedule': {'interval': '0 0 * * *'}},
                {'name': 'recipe2', 'lastUpdated': 1640995300000, 'schedule': None}
            ],
            policies=[
                {'name': 'datahub-policy-1', 'urn': 'urn:li:dataHubPolicy:policy1'},
                {'name': 'datahub-policy-2', 'urn': 'urn:li:dataHubPolicy:policy2'}
            ]
        )
        mock_get_client.return_value = mock_client
        
        # Login and navigate to dashboard
        self._login_user(self.user)
        self.browser.get(f"{self.live_server_url}/dashboard/")
        
        # Wait for dashboard to load
        dashboard_element = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "dashboard")))
        
        # Wait for AJAX data to load
        time.sleep(2)  # Allow time for AJAX requests
        
        # Verify environment count is displayed
        env_stat = self.browser.find_element(By.CSS_SELECTOR, "[data-stat='environments'] .stat-value")
        self.assertEqual(env_stat.text, str(len(self.environments)))
        
        # Verify policy count includes both local and DataHub policies
        policy_stat = self.browser.find_element(By.CSS_SELECTOR, "[data-stat='policies'] .stat-value")
        expected_policies = len(self.policies) + 2  # Local + DataHub policies
        self.assertEqual(policy_stat.text, str(expected_policies))
        
        # Verify log count is displayed
        logs_stat = self.browser.find_element(By.CSS_SELECTOR, "[data-stat='logs'] .stat-value")
        self.assertEqual(logs_stat.text, str(len(self.logs)))
        
        # Verify connection status shows connected
        connection_status = self.browser.find_element(By.CLASS_NAME, "connection-status")
        self.assertIn("Connected", connection_status.text)
    
    def test_dashboard_navigation_integration(self):
        """Test navigation from dashboard to other sections works correctly."""
        self._login_user(self.user)
        self.browser.get(f"{self.live_server_url}/dashboard/")
        
        # Test navigation to policies
        policies_link = self.wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Policies")))
        policies_link.click()
        
        self.wait.until(EC.url_contains("/policies/"))
        
        # Verify policies are displayed
        self.assertIn("Policy 0", self.browser.page_source)
        
        # Navigate back to dashboard
        dashboard_link = self.browser.find_element(By.LINK_TEXT, "Dashboard")
        dashboard_link.click()
        
        self.wait.until(EC.url_contains("/dashboard/"))
        
        # Test navigation to logs
        logs_link = self.browser.find_element(By.LINK_TEXT, "Logs")
        logs_link.click()
        
        self.wait.until(EC.url_contains("/logs/"))
        
        # Verify logs are displayed
        self.assertIn("Info message", self.browser.page_source)


class LogMonitoringWorkflowTestCase(LiveServerTestCase):
    """Test log monitoring and management workflows."""
    
    def setUp(self):
        super().setUp()
        self.browser = webdriver.Chrome(options=self._get_chrome_options())
        self.wait = WebDriverWait(self.browser, 10)
        
        self.admin_user = UserFactory.create_admin()
        self.regular_user = UserFactory.create_user()
        
        # Create diverse log entries
        self.logs = [
            LogEntryFactory(level='INFO', message='Application started', source='app.main'),
            LogEntryFactory(level='ERROR', message='Database connection failed', source='app.db'),
            LogEntryFactory(level='WARNING', message='High memory usage detected', source='app.monitor'),
            LogEntryFactory(level='DEBUG', message='Processing user request', source='app.views'),
            LogEntryFactory(level='CRITICAL', message='System overload', source='app.system')
        ]
    
    def tearDown(self):
        self.browser.quit()
        super().tearDown()
    
    def _get_chrome_options(self):
        from selenium.webdriver.chrome.options import Options
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        return options
    
    def _login_user(self, user):
        self.browser.get(f"{self.live_server_url}/accounts/login/")
        username_input = self.browser.find_element(By.NAME, "username")
        password_input = self.browser.find_element(By.NAME, "password")
        username_input.send_keys(user.username)
        password_input.send_keys("testpassword123")
        password_input.send_keys(Keys.RETURN)
        self.wait.until(EC.url_changes(f"{self.live_server_url}/accounts/login/"))
    
    def test_log_filtering_workflow(self):
        """Test complete log filtering workflow."""
        self._login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/logs/")
        
        # Wait for logs to load
        logs_table = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "logs-table")))
        
        # Test level filtering
        level_filter = Select(self.browser.find_element(By.NAME, "level"))
        level_filter.select_by_visible_text("ERROR")
        
        # Submit filter
        filter_form = self.browser.find_element(By.CLASS_NAME, "filter-form")
        filter_form.submit()
        
        # Wait for filtered results
        self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "logs-table")))
        
        # Verify only ERROR logs are shown
        log_rows = self.browser.find_elements(By.CSS_SELECTOR, ".logs-table tbody tr")
        for row in log_rows:
            self.assertIn("ERROR", row.text)
        
        # Test source filtering
        source_filter = Select(self.browser.find_element(By.NAME, "source"))
        source_filter.select_by_visible_text("app.db")
        
        filter_form.submit()
        self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "logs-table")))
        
        # Verify filtering by source works
        log_rows = self.browser.find_elements(By.CSS_SELECTOR, ".logs-table tbody tr")
        for row in log_rows:
            self.assertIn("app.db", row.text)
    
    def test_log_search_workflow(self):
        """Test log search functionality workflow."""
        self._login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/logs/")
        
        # Wait for logs to load
        self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "logs-table")))
        
        # Test search functionality
        search_input = self.browser.find_element(By.NAME, "search")
        search_input.clear()
        search_input.send_keys("database")
        search_input.send_keys(Keys.RETURN)
        
        # Wait for search results
        self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "logs-table")))
        
        # Verify search results
        log_rows = self.browser.find_elements(By.CSS_SELECTOR, ".logs-table tbody tr")
        for row in log_rows:
            self.assertIn("database", row.text.lower())
    
    def test_log_management_workflow(self):
        """Test log management (clearing) workflow."""
        self._login_user(self.admin_user)
        self.browser.get(f"{self.live_server_url}/logs/")
        
        # Wait for logs to load
        self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "logs-table")))
        
        # Get initial log count
        initial_rows = len(self.browser.find_elements(By.CSS_SELECTOR, ".logs-table tbody tr"))
        self.assertGreater(initial_rows, 0)
        
        # Clear logs (if clear button exists)
        clear_buttons = self.browser.find_elements(By.CLASS_NAME, "btn-clear-logs")
        if clear_buttons:
            clear_button = clear_buttons[0]
            clear_button.click()
            
            # Confirm if there's a confirmation dialog
            try:
                confirm_button = self.wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "btn-confirm")))
                confirm_button.click()
            except:
                pass  # No confirmation dialog
            
            # Wait for page to reload
            time.sleep(1)
            
            # Verify logs were cleared
            remaining_rows = len(self.browser.find_elements(By.CSS_SELECTOR, ".logs-table tbody tr"))
            self.assertLess(remaining_rows, initial_rows)


class MultiUserWorkflowTestCase(TransactionTestCase):
    """Test multi-user scenarios and permission workflows."""
    
    def setUp(self):
        super().setUp()
        self.admin_user = UserFactory.create_admin()
        self.regular_user = UserFactory.create_user()
        self.environment = EnvironmentFactory(name='shared-env')
    
    def test_policy_collaboration_workflow(self):
        """Test policy collaboration between different users."""
        # Admin creates a policy
        self.client.force_login(self.admin_user)
        
        policy_data = {
            'name': 'Shared Policy',
            'description': 'A policy for collaboration testing',
            'policy_json': json.dumps({
                "name": "shared-policy",
                "type": "METADATA",
                "state": "ACTIVE"
            }),
            'environment': self.environment.id
        }
        
        with patch('web_ui.web_ui.views.get_datahub_client_from_request') as mock_get_client:
            mock_client = MockHelper.mock_datahub_client()
            mock_client.create_policy.return_value = {'success': True, 'urn': 'urn:li:dataHubPolicy:shared'}
            mock_get_client.return_value = mock_client
            
            response = self.client.post(reverse('policy_create'), policy_data)
            self.assertEqual(response.status_code, 302)
        
        # Verify policy exists
        policy = Policy.objects.filter(name='Shared Policy').first()
        self.assertIsNotNone(policy)
        
        # Regular user should be able to view the policy
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('policies'))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Shared Policy')
        
        # Regular user should not be able to edit the policy (depends on permissions)
        response = self.client.get(reverse('policy_edit', kwargs={'pk': policy.pk}))
        # This might be 403 or redirect depending on permission implementation
        self.assertIn(response.status_code, [302, 403])
    
    def test_environment_access_workflow(self):
        """Test environment access across different users."""
        # Create environments with different access levels
        public_env = EnvironmentFactory(name='public-env')
        private_env = EnvironmentFactory(name='private-env')
        
        # Admin can access all environments
        self.client.force_login(self.admin_user)
        response = self.client.get(reverse('environments'))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'public-env')
        self.assertContains(response, 'private-env')
        
        # Regular user can access environments (based on current implementation)
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('environments'))
        self.assertEqual(response.status_code, 200)
        # Exact access depends on permission implementation


class DataFlowIntegrationTestCase(BaseWebUITestCase):
    """Test data flow across all application layers."""
    
    def setUp(self):
        super().setUp()
        self.environment = EnvironmentFactory(name='test-env')
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_policy_data_flow(self, mock_get_client):
        """Test policy data flows correctly from form to database to DataHub API."""
        # Mock DataHub client
        mock_client = MockHelper.mock_datahub_client()
        mock_client.create_policy.return_value = {'success': True, 'urn': 'urn:li:dataHubPolicy:test'}
        mock_get_client.return_value = mock_client
        
        # Create policy via form submission
        policy_data = {
            'name': 'Data Flow Test Policy',
            'description': 'Testing data flow across layers',
            'policy_json': json.dumps({
                "name": "data-flow-policy",
                "description": "Testing data flow",
                "type": "METADATA",
                "state": "ACTIVE",
                "privileges": ["EDIT_ENTITY_OWNERS"],
                "actors": {"users": ["urn:li:corpuser:testuser"], "groups": []}
            }),
            'environment': self.environment.id
        }
        
        self.client.force_login(self.admin_user)
        response = self.client.post(reverse('policy_create'), policy_data)
        
        # Verify form processing
        self.assertEqual(response.status_code, 302)
        
        # Verify database storage
        policy = Policy.objects.filter(name='Data Flow Test Policy').first()
        self.assertIsNotNone(policy)
        self.assertEqual(policy.description, 'Testing data flow across layers')
        self.assertEqual(policy.environment, self.environment)
        self.assertEqual(policy.policy_data['name'], 'data-flow-policy')
        
        # Verify DataHub API call
        mock_client.create_policy.assert_called_once()
        call_args = mock_client.create_policy.call_args[0][0]
        self.assertEqual(call_args['name'], 'data-flow-policy')
        self.assertEqual(call_args['type'], 'METADATA')
        
        # Verify policy appears in list view
        response = self.client.get(reverse('policies'))
        self.assertContains(response, 'Data Flow Test Policy')
    
    def test_environment_data_flow(self):
        """Test environment data flows correctly across layers."""
        # Create environment via form
        env_data = {
            'name': 'flow-test-env',
            'description': 'Environment for testing data flow',
            'datahub_host': 'http://test.datahub.com',
            'datahub_token': 'test-token-123',
            'is_default': True
        }
        
        self.client.force_login(self.admin_user)
        response = self.client.post(reverse('environment_create'), env_data)
        
        # Verify form processing
        self.assertEqual(response.status_code, 302)
        
        # Verify database storage
        environment = Environment.objects.filter(name='flow-test-env').first()
        self.assertIsNotNone(environment)
        self.assertEqual(environment.description, 'Environment for testing data flow')
        self.assertEqual(environment.datahub_host, 'http://test.datahub.com')
        self.assertTrue(environment.is_default)
        
        # Verify default environment constraint worked
        # (Other environments should no longer be default)
        other_defaults = Environment.objects.filter(is_default=True).exclude(id=environment.id)
        self.assertEqual(other_defaults.count(), 0)
        
        # Verify environment appears in lists and dashboard
        response = self.client.get(reverse('environments'))
        self.assertContains(response, 'flow-test-env')
        
        # Check dashboard includes this environment in count
        response = self.client.get(reverse('dashboard_data'))
        data = json.loads(response.content)
        self.assertGreaterEqual(data['environments_count'], 1)
    
    def test_log_data_flow(self):
        """Test log data flows correctly from generation to display."""
        import logging
        
        # Generate log entries through Django logging
        logger = logging.getLogger('web_ui.test')
        
        # Create log entries
        logger.info('Test info message for data flow')
        logger.error('Test error message for data flow')
        logger.warning('Test warning message for data flow')
        
        # Wait a moment for async log processing if any
        time.sleep(0.1)
        
        # Verify logs in database
        info_logs = LogEntry.objects.filter(message__contains='Test info message')
        error_logs = LogEntry.objects.filter(message__contains='Test error message')
        warning_logs = LogEntry.objects.filter(message__contains='Test warning message')
        
        self.assertGreater(info_logs.count(), 0)
        self.assertGreater(error_logs.count(), 0)
        self.assertGreater(warning_logs.count(), 0)
        
        # Verify logs appear in UI
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse('logs'))
        
        self.assertContains(response, 'Test info message for data flow')
        self.assertContains(response, 'Test error message for data flow')
        self.assertContains(response, 'Test warning message for data flow')
        
        # Verify logs are included in dashboard counts
        response = self.client.get(reverse('dashboard_data'))
        data = json.loads(response.content)
        self.assertGreater(data['logs_count'], 0)


class ErrorHandlingIntegrationTestCase(BaseWebUITestCase):
    """Test error handling across integration points."""
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_datahub_connection_error_handling(self, mock_get_client):
        """Test handling of DataHub connection errors across the application."""
        # Mock connection failure
        mock_get_client.side_effect = Exception("DataHub connection failed")
        
        self.client.force_login(self.regular_user)
        
        # Dashboard should handle connection error gracefully
        response = self.client.get(reverse('dashboard_data'))
        self.assertEqual(response.status_code, 200)
        
        data = json.loads(response.content)
        self.assertFalse(data['connected'])
        self.assertIn('error', data)
        
        # Policy creation should handle error gracefully
        policy_data = {
            'name': 'Test Policy',
            'description': 'Test description',
            'policy_json': json.dumps({"name": "test", "type": "METADATA"}),
            'environment': EnvironmentFactory().id
        }
        
        response = self.client.post(reverse('policy_create'), policy_data)
        
        # Should either succeed with local storage or show error message
        self.assertIn(response.status_code, [200, 302])
    
    def test_database_error_recovery(self):
        """Test recovery from database errors."""
        # This would require more complex setup to simulate database failures
        # For now, test basic error handling
        
        self.client.force_login(self.regular_user)
        
        # Try to access non-existent policy
        response = self.client.get(reverse('policy_edit', kwargs={'pk': 99999}))
        self.assertEqual(response.status_code, 404)
        
        # Try to access non-existent environment
        response = self.client.get(reverse('environment_edit', kwargs={'pk': 99999}))
        self.assertEqual(response.status_code, 404)
    
    def test_form_validation_error_integration(self):
        """Test form validation error handling across layers."""
        self.client.force_login(self.admin_user)
        
        # Submit invalid policy form
        invalid_policy_data = {
            'name': '',  # Required field empty
            'description': 'Test description',
            'policy_json': 'invalid json',  # Invalid JSON
            'environment': 99999  # Non-existent environment
        }
        
        response = self.client.post(reverse('policy_create'), invalid_policy_data)
        
        # Should re-render form with errors
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'error')
        
        # No policy should be created
        self.assertFalse(Policy.objects.filter(description='Test description').exists()) 