"""
Comprehensive JavaScript functionality tests for all web_ui pages.

Uses Selenium WebDriver to test interactive features including:
- Dashboard real-time updates and AJAX loading
- Recipes data tables, search, and modal interactions
- Policies JSON editor, form validation, and deployment
- Metadata Manager comprehensive sub-module interactions
- GitHub integration UI components and workflows
- Environment management forms and validation
- Logs filtering, search, and real-time updates
- Settings configuration and testing
- Template management with editors and previews
- Connection testing and switching
- Mutation management and deployment
- Authentication flows and session management
- API endpoint interactions
- Form auto-save and keyboard navigation
- Modal dialogs and confirmations
- Responsive navigation and mobile interactions
- Data table sorting, filtering, and pagination
- File upload and download functionalities
- Real-time notifications and alerts
"""

import json
import time
import pytest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from django.test import LiveServerTestCase
from django.urls import reverse
from django.contrib.staticfiles.testing import StaticLiveServerTestCase
from unittest.mock import patch, Mock

from tests.fixtures.factories import (
    UserFactory, EnvironmentFactory, PolicyFactory, 
    LogEntryFactory, MutationFactory, RecipeFactory
)


@pytest.mark.selenium
@pytest.mark.javascript
@pytest.mark.frontend
class JavaScriptFunctionalityTestCase(StaticLiveServerTestCase):
    """Comprehensive JavaScript functionality tests for all pages."""
    
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Set up browser options
        chrome_options = ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1920,1080')
        
        try:
            cls.browser = webdriver.Chrome(options=chrome_options)
        except Exception:
            # Fallback to Firefox if Chrome not available
            firefox_options = FirefoxOptions()
            firefox_options.add_argument('--headless')
            cls.browser = webdriver.Firefox(options=firefox_options)
        
        cls.browser.implicitly_wait(10)
        cls.wait = WebDriverWait(cls.browser, 10)
    
    @classmethod 
    def tearDownClass(cls):
        cls.browser.quit()
        super().tearDownClass()
    
    def setUp(self):
        self.admin_user = UserFactory.create_admin()
        self.regular_user = UserFactory.create_user()
        self.environment = EnvironmentFactory(name='test-env', is_default=True)
        self.policy = PolicyFactory(environment=self.environment, user=self.admin_user)
        self.mutation = MutationFactory(user=self.admin_user)
    
    def login_user(self, user):
        """Helper method to login a user via the web interface."""
        self.browser.get(f"{self.live_server_url}/login/")
        username_input = self.browser.find_element(By.NAME, "username")
        password_input = self.browser.find_element(By.NAME, "password")
        
        username_input.send_keys(user.username)
        password_input.send_keys('testpass123')  # Default test password
        password_input.send_keys(Keys.RETURN)
        
        # Wait for redirect to dashboard
        self.wait.until(EC.url_contains('/dashboard/'))
    
    def wait_for_ajax(self, timeout=10):
        """Wait for AJAX requests to complete."""
        self.wait.until(lambda driver: driver.execute_script("return jQuery.active == 0"))
    
    def test_dashboard_ajax_loading(self):
        """Test dashboard AJAX data loading and real-time updates."""
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/dashboard/")
        
        # Wait for page to load
        self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "dashboard-container")))
        
        # Test AJAX data loading for stats cards
        self.wait_for_ajax()
        stat_cards = self.browser.find_elements(By.CLASS_NAME, "stat-card")
        self.assertGreater(len(stat_cards), 0)
        
        # Verify stat values are populated
        for card in stat_cards:
            stat_value = card.find_element(By.CLASS_NAME, "stat-value")
            self.assertNotEqual(stat_value.text.strip(), "")
        
        # Test refresh button functionality
        refresh_btn = self.browser.find_element(By.ID, "refresh-dashboard")
        refresh_btn.click()
        self.wait_for_ajax()
        
        # Test auto-refresh toggle
        auto_refresh_toggle = self.browser.find_element(By.ID, "auto-refresh-toggle")
        auto_refresh_toggle.click()
        
        # Wait a moment and verify auto-refresh is working
        time.sleep(2)
        self.assertTrue(auto_refresh_toggle.is_selected())
    
    def test_dashboard_environment_switching(self):
        """Test environment switching functionality on dashboard."""
        # Create additional environment
        additional_env = EnvironmentFactory(name='prod-env')
        
        self.login_user(self.admin_user)
        self.browser.get(f"{self.live_server_url}/dashboard/")
        
        # Find environment selector
        env_selector = self.wait.until(EC.element_to_be_clickable((By.ID, "environment-selector")))
        env_selector.click()
        
        # Select different environment
        env_option = self.wait.until(EC.element_to_be_clickable((By.XPATH, f"//option[text()='{additional_env.name}']")))
        env_option.click()
        
        # Wait for dashboard to update
        self.wait_for_ajax()
        
        # Verify environment change reflected
        current_env = self.browser.find_element(By.ID, "current-environment")
        self.assertIn(additional_env.name, current_env.text)
    
    def test_recipes_data_table_functionality(self):
        """Test recipes page data table interactions."""
        # Create test recipes
        for i in range(15):
            RecipeFactory(name=f'test-recipe-{i}', user=self.regular_user)
        
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/recipes/")
        
        # Wait for data table to load
        data_table = self.wait.until(EC.presence_of_element_located((By.ID, "recipes-table")))
        
        # Test search functionality
        search_input = self.browser.find_element(By.ID, "recipe-search")
        search_input.send_keys("test-recipe-5")
        search_input.send_keys(Keys.RETURN)
        
        self.wait_for_ajax()
        
        # Verify search results
        table_rows = self.browser.find_elements(By.CSS_SELECTOR, "#recipes-table tbody tr")
        self.assertEqual(len(table_rows), 1)
        self.assertIn("test-recipe-5", table_rows[0].text)
        
        # Test table sorting
        name_header = self.browser.find_element(By.CSS_SELECTOR, "th[data-sort='name']")
        name_header.click()
        self.wait_for_ajax()
        
        # Verify sorting applied
        first_row = self.browser.find_element(By.CSS_SELECTOR, "#recipes-table tbody tr:first-child")
        self.assertIn("test-recipe-0", first_row.text)
        
        # Test pagination
        if len(self.browser.find_elements(By.CLASS_NAME, "pagination")) > 0:
            next_page = self.browser.find_element(By.CSS_SELECTOR, ".pagination .next")
            next_page.click()
            self.wait_for_ajax()
            
            # Verify page changed
            current_page = self.browser.find_element(By.CSS_SELECTOR, ".pagination .current")
            self.assertEqual(current_page.text, "2")
    
    def test_recipe_modal_interactions(self):
        """Test recipe creation and editing modal interactions."""
        self.login_user(self.admin_user)
        self.browser.get(f"{self.live_server_url}/recipes/")
        
        # Open create recipe modal
        create_btn = self.wait.until(EC.element_to_be_clickable((By.ID, "create-recipe-btn")))
        create_btn.click()
        
        # Wait for modal to appear
        modal = self.wait.until(EC.visibility_of_element_located((By.ID, "recipe-modal")))
        
        # Fill in recipe form
        name_input = modal.find_element(By.NAME, "name")
        name_input.send_keys("Test Recipe")
        
        description_input = modal.find_element(By.NAME, "description")
        description_input.send_keys("A test recipe for JavaScript testing")
        
        # Test recipe editor (CodeMirror or similar)
        recipe_editor = modal.find_element(By.CLASS_NAME, "recipe-editor")
        self.browser.execute_script("""
            arguments[0].CodeMirror.setValue(JSON.stringify({
                source: {type: 'postgres', config: {host: 'localhost'}},
                sink: {type: 'datahub-rest', config: {server: 'http://localhost:8080'}}
            }, null, 2));
        """, recipe_editor)
        
        # Test form validation
        save_btn = modal.find_element(By.CSS_SELECTOR, ".btn-save")
        save_btn.click()
        
        # Wait for success message or validation
        self.wait.until(lambda driver: 
            len(driver.find_elements(By.CLASS_NAME, "alert-success")) > 0 or
            len(driver.find_elements(By.CLASS_NAME, "alert-danger")) > 0
        )
        
        # Close modal
        close_btn = modal.find_element(By.CSS_SELECTOR, ".btn-close")
        close_btn.click()
        
        # Wait for modal to close
        self.wait.until(EC.invisibility_of_element_located((By.ID, "recipe-modal")))
    
    def test_policies_json_editor_functionality(self):
        """Test policies page JSON editor and form validation."""
        self.login_user(self.admin_user)
        self.browser.get(f"{self.live_server_url}/policies/")
        
        # Open create policy modal
        create_btn = self.wait.until(EC.element_to_be_clickable((By.ID, "create-policy-btn")))
        create_btn.click()
        
        modal = self.wait.until(EC.visibility_of_element_located((By.ID, "policy-modal")))
        
        # Fill basic fields
        name_input = modal.find_element(By.NAME, "name")
        name_input.send_keys("Test Policy")
        
        # Test JSON editor
        json_editor = modal.find_element(By.CLASS_NAME, "json-editor")
        
        # Test invalid JSON
        self.browser.execute_script("""
            arguments[0].CodeMirror.setValue('{ invalid json }');
        """, json_editor)
        
        # Try to save and check validation
        save_btn = modal.find_element(By.CSS_SELECTOR, ".btn-save")
        save_btn.click()
        
        # Should show validation error
        error_message = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "json-error")))
        self.assertIn("invalid", error_message.text.lower())
        
        # Test valid JSON
        valid_policy = {
            "type": "METADATA",
            "state": "ACTIVE",
            "privileges": ["EDIT_ENTITY_OWNERS"],
            "actors": {"users": [], "groups": []}
        }
        
        self.browser.execute_script("""
            arguments[0].CodeMirror.setValue(arguments[1]);
        """, json_editor, json.dumps(valid_policy, indent=2))
        
        # Save should work now
        save_btn.click()
        self.wait_for_ajax()
        
        # Check for success
        success_alert = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "alert-success")))
        self.assertIn("success", success_alert.text.lower())
    
    def test_policy_deployment_workflow(self):
        """Test policy deployment workflow with confirmation dialogs."""
        self.login_user(self.admin_user)
        self.browser.get(f"{self.live_server_url}/policies/")
        
        # Find policy in table
        policy_row = self.wait.until(EC.presence_of_element_located(
            (By.XPATH, f"//tr[contains(., '{self.policy.name}')]")
        ))
        
        # Click deploy button
        deploy_btn = policy_row.find_element(By.CSS_SELECTOR, ".btn-deploy")
        deploy_btn.click()
        
        # Wait for confirmation dialog
        confirm_dialog = self.wait.until(EC.visibility_of_element_located((By.ID, "deploy-confirm-modal")))
        
        # Test cancel
        cancel_btn = confirm_dialog.find_element(By.CSS_SELECTOR, ".btn-cancel")
        cancel_btn.click()
        
        # Dialog should close
        self.wait.until(EC.invisibility_of_element_located((By.ID, "deploy-confirm-modal")))
        
        # Try deploy again and confirm
        deploy_btn.click()
        confirm_dialog = self.wait.until(EC.visibility_of_element_located((By.ID, "deploy-confirm-modal")))
        
        confirm_btn = confirm_dialog.find_element(By.CSS_SELECTOR, ".btn-confirm")
        confirm_btn.click()
        
        # Wait for deployment to complete
        self.wait_for_ajax()
        
        # Check for deployment status update
        status_indicator = policy_row.find_element(By.CLASS_NAME, "status-indicator")
        self.assertIn("deployed", status_indicator.get_attribute("class").lower())
    
    def test_metadata_manager_navigation(self):
        """Test metadata manager navigation and sub-module interactions."""
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}{reverse('metadata_manager:metadata_index')}")
        
        # Test navigation between sub-modules
        nav_items = [
            ('Domains', 'metadata_manager:domain_list'),
            ('Tags', 'metadata_manager:tag_list'),
            ('Glossary', 'metadata_manager:glossary_list'),
            ('Properties', 'metadata_manager:property_list'),
            ('Data Products', 'metadata_manager:data_product_list'),
            ('Assertions', 'metadata_manager:assertion_list'),
            ('Tests', 'metadata_manager:tests_list')
        ]
        
        for nav_text, expected_url in nav_items:
            # Click navigation item
            nav_link = self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, f"//a[contains(text(), '{nav_text}')]")
            ))
            nav_link.click()
            
            # Wait for page to load
            self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "metadata-content")))
            
            # Verify correct page loaded
            self.assertIn(expected_url.split(':')[1], self.browser.current_url)
            
            # Test data loading
            self.wait_for_ajax()
            
            # Verify page has expected elements
            page_title = self.browser.find_element(By.TAG_NAME, "h1")
            self.assertIn(nav_text.lower(), page_title.text.lower())
    
    def test_metadata_manager_bulk_operations(self):
        """Test metadata manager bulk operations and batch actions."""
        self.login_user(self.admin_user)
        self.browser.get(f"{self.live_server_url}{reverse('metadata_manager:domain_list')}")
        
        # Wait for page to load
        self.wait.until(EC.presence_of_element_located((By.ID, "domains-table")))
        
        # Test select all checkbox
        select_all = self.browser.find_element(By.ID, "select-all-domains")
        select_all.click()
        
        # Verify individual checkboxes are selected
        checkboxes = self.browser.find_elements(By.CSS_SELECTOR, "input[name='domain_ids']")
        for checkbox in checkboxes:
            self.assertTrue(checkbox.is_selected())
        
        # Test bulk action dropdown
        bulk_actions = Select(self.browser.find_element(By.ID, "bulk-actions"))
        bulk_actions.select_by_value("bulk_deploy")
        
        # Click execute bulk action
        execute_btn = self.browser.find_element(By.ID, "execute-bulk-action")
        execute_btn.click()
        
        # Wait for confirmation dialog
        confirm_dialog = self.wait.until(EC.visibility_of_element_located((By.ID, "bulk-confirm-modal")))
        
        # Confirm action
        confirm_btn = confirm_dialog.find_element(By.CSS_SELECTOR, ".btn-confirm")
        confirm_btn.click()
        
        # Wait for bulk operation to complete
        self.wait_for_ajax()
        
        # Check for success message
        success_alert = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "alert-success")))
        self.assertIn("bulk", success_alert.text.lower())
    
    def test_github_integration_ui_components(self):
        """Test GitHub integration UI components and workflows."""
        self.login_user(self.admin_user)
        self.browser.get(f"{self.live_server_url}/github/")
        
        # Test connection status indicator
        connection_status = self.wait.until(EC.presence_of_element_located((By.ID, "github-connection-status")))
        
        # Test connection button
        test_connection_btn = self.browser.find_element(By.ID, "test-github-connection")
        test_connection_btn.click()
        
        # Wait for connection test to complete
        self.wait_for_ajax()
        
        # Check connection result
        connection_result = self.browser.find_element(By.ID, "connection-test-result")
        self.assertIsNotNone(connection_result.text)
        
        # Test branch selector
        branch_selector = self.browser.find_element(By.ID, "branch-selector")
        branch_selector.click()
        
        # Wait for branches to load
        self.wait_for_ajax()
        
        # Test branch switching
        branch_options = self.browser.find_elements(By.CSS_SELECTOR, "#branch-selector option")
        if len(branch_options) > 1:
            Select(branch_selector).select_by_index(1)
            self.wait_for_ajax()
            
            # Verify branch switch
            current_branch = self.browser.find_element(By.ID, "current-branch")
            self.assertNotEqual(current_branch.text, "main")
    
    def test_github_pull_request_workflow(self):
        """Test GitHub pull request creation and management workflow."""
        self.login_user(self.admin_user)
        self.browser.get(f"{self.live_server_url}/github/pull-requests/")
        
        # Test create PR button
        create_pr_btn = self.wait.until(EC.element_to_be_clickable((By.ID, "create-pr-btn")))
        create_pr_btn.click()
        
        # Wait for PR creation modal
        pr_modal = self.wait.until(EC.visibility_of_element_located((By.ID, "create-pr-modal")))
        
        # Fill PR details
        title_input = pr_modal.find_element(By.NAME, "title")
        title_input.send_keys("Test PR from JavaScript tests")
        
        description_input = pr_modal.find_element(By.NAME, "description")
        description_input.send_keys("This PR was created by automated JavaScript tests")
        
        # Select base and head branches
        base_branch = Select(pr_modal.find_element(By.NAME, "base_branch"))
        base_branch.select_by_value("main")
        
        head_branch = Select(pr_modal.find_element(By.NAME, "head_branch"))
        if len(head_branch.options) > 1:
            head_branch.select_by_index(1)
        
        # Create PR
        create_btn = pr_modal.find_element(By.CSS_SELECTOR, ".btn-create")
        create_btn.click()
        
        # Wait for PR creation to complete
        self.wait_for_ajax()
        
        # Check for success message
        self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "alert-success")))
    
    def test_environment_form_validation(self):
        """Test environment form validation and interactive features."""
        self.login_user(self.admin_user)
        self.browser.get(f"{self.live_server_url}/environments/create/")
        
        # Test form validation
        name_input = self.wait.until(EC.presence_of_element_located((By.NAME, "name")))
        datahub_host_input = self.browser.find_element(By.NAME, "datahub_host")
        
        # Submit empty form to trigger validation
        submit_btn = self.browser.find_element(By.CSS_SELECTOR, "button[type='submit']")
        submit_btn.click()
        
        # Check for validation errors
        name_error = self.browser.find_element(By.CSS_SELECTOR, ".error-message[data-field='name']")
        self.assertIsNotNone(name_error.text)
        
        # Fill valid data
        name_input.send_keys("test-environment")
        datahub_host_input.send_keys("http://localhost:8080")
        
        # Test connection button
        test_connection_btn = self.browser.find_element(By.ID, "test-datahub-connection")
        test_connection_btn.click()
        
        # Wait for connection test
        self.wait_for_ajax()
        
        # Check connection result
        connection_result = self.browser.find_element(By.ID, "connection-test-result")
        self.assertIn("connection", connection_result.text.lower())
        
        # Submit form
        submit_btn.click()
        
        # Wait for success or error
        self.wait.until(lambda driver: 
            len(driver.find_elements(By.CLASS_NAME, "alert-success")) > 0 or
            len(driver.find_elements(By.CLASS_NAME, "alert-error")) > 0
        )
    
    def test_logs_filtering_and_search(self):
        """Test logs page filtering, search, and real-time updates."""
        # Create test log entries
        for i in range(20):
            LogEntryFactory(
                level=['INFO', 'ERROR', 'WARNING'][i % 3],
                message=f'Test log message {i}',
                user=self.regular_user
            )
        
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/logs/")
        
        # Wait for logs table to load
        logs_table = self.wait.until(EC.presence_of_element_located((By.ID, "logs-table")))
        
        # Test level filter
        level_filter = Select(self.browser.find_element(By.ID, "level-filter"))
        level_filter.select_by_value("ERROR")
        
        # Wait for filter to apply
        self.wait_for_ajax()
        
        # Verify only error logs shown
        log_rows = self.browser.find_elements(By.CSS_SELECTOR, "#logs-table tbody tr")
        for row in log_rows:
            level_cell = row.find_element(By.CSS_SELECTOR, "td.level")
            self.assertEqual(level_cell.text, "ERROR")
        
        # Test search
        search_input = self.browser.find_element(By.ID, "log-search")
        search_input.send_keys("message 5")
        search_input.send_keys(Keys.RETURN)
        
        self.wait_for_ajax()
        
        # Verify search results
        log_rows = self.browser.find_elements(By.CSS_SELECTOR, "#logs-table tbody tr")
        self.assertEqual(len(log_rows), 1)
        self.assertIn("message 5", log_rows[0].text)
        
        # Test auto-refresh toggle
        auto_refresh = self.browser.find_element(By.ID, "auto-refresh-logs")
        auto_refresh.click()
        
        # Verify auto-refresh is enabled
        self.assertTrue(auto_refresh.is_selected())
        
        # Test clear logs button
        clear_logs_btn = self.browser.find_element(By.ID, "clear-logs-btn")
        clear_logs_btn.click()
        
        # Confirm in dialog
        confirm_dialog = self.wait.until(EC.visibility_of_element_located((By.ID, "clear-logs-confirm")))
        confirm_btn = confirm_dialog.find_element(By.CSS_SELECTOR, ".btn-confirm")
        confirm_btn.click()
        
        # Wait for logs to clear
        self.wait_for_ajax()
        
        # Verify table is now empty
        empty_message = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "no-logs-message")))
        self.assertIn("no logs", empty_message.text.lower())
    
    def test_mutations_management_ui(self):
        """Test mutations page management interface."""
        self.login_user(self.admin_user)
        self.browser.get(f"{self.live_server_url}/mutations/")
        
        # Wait for mutations table
        mutations_table = self.wait.until(EC.presence_of_element_located((By.ID, "mutations-table")))
        
        # Test create mutation modal
        create_btn = self.browser.find_element(By.ID, "create-mutation-btn")
        create_btn.click()
        
        modal = self.wait.until(EC.visibility_of_element_located((By.ID, "mutation-modal")))
        
        # Fill mutation form
        name_input = modal.find_element(By.NAME, "name")
        name_input.send_keys("Test Mutation")
        
        mutation_type = Select(modal.find_element(By.NAME, "mutation_type"))
        mutation_type.select_by_value("UPDATE")
        
        # Test JSON editor for mutation data
        json_editor = modal.find_element(By.CLASS_NAME, "mutation-json-editor")
        self.browser.execute_script("""
            arguments[0].CodeMirror.setValue(JSON.stringify({
                'aspectName': 'ownership',
                'aspect': {'owners': []}
            }, null, 2));
        """, json_editor)
        
        # Save mutation
        save_btn = modal.find_element(By.CSS_SELECTOR, ".btn-save")
        save_btn.click()
        
        self.wait_for_ajax()
        
        # Verify mutation created
        success_alert = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "alert-success")))
        self.assertIn("created", success_alert.text.lower())
    
    def test_responsive_navigation_toggle(self):
        """Test responsive navigation toggle and mobile interactions."""
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/dashboard/")
        
        # Simulate mobile viewport
        self.browser.set_window_size(375, 667)
        
        # Wait for page to load
        self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "navbar")))
        
        # Test mobile menu toggle
        menu_toggle = self.browser.find_element(By.CLASS_NAME, "navbar-toggle")
        self.assertFalse(menu_toggle.find_element(By.XPATH, "..").get_attribute("class").__contains__("show"))
        
        menu_toggle.click()
        
        # Verify menu is now visible
        navbar_collapse = self.browser.find_element(By.CLASS_NAME, "navbar-collapse")
        self.wait.until(lambda driver: "show" in navbar_collapse.get_attribute("class"))
        
        # Test navigation in mobile mode
        recipes_link = navbar_collapse.find_element(By.LINK_TEXT, "Recipes")
        recipes_link.click()
        
        # Wait for navigation
        self.wait.until(EC.url_contains('/recipes/'))
        
        # Reset to desktop size
        self.browser.set_window_size(1920, 1080)
    
    def test_keyboard_navigation_support(self):
        """Test keyboard navigation and accessibility features."""
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/recipes/")
        
        # Wait for page to load
        self.wait.until(EC.presence_of_element_located((By.ID, "recipes-table")))
        
        # Test tab navigation
        body = self.browser.find_element(By.TAG_NAME, "body")
        
        # Use Tab key to navigate through focusable elements
        focusable_elements = []
        
        for i in range(10):  # Tab through first 10 elements
            focused_element = self.browser.switch_to.active_element
            focusable_elements.append(focused_element.tag_name)
            
            # Send Tab key
            ActionChains(self.browser).send_keys(Keys.TAB).perform()
            time.sleep(0.1)
        
        # Verify we navigated through different elements
        self.assertGreater(len(set(focusable_elements)), 1)
        
        # Test Enter key activation on buttons
        create_btn = self.browser.find_element(By.ID, "create-recipe-btn")
        create_btn.send_keys(Keys.RETURN)
        
        # Verify modal opened
        modal = self.wait.until(EC.visibility_of_element_located((By.ID, "recipe-modal")))
        
        # Test Escape key to close modal
        ActionChains(self.browser).send_keys(Keys.ESCAPE).perform()
        
        # Verify modal closed
        self.wait.until(EC.invisibility_of_element_located((By.ID, "recipe-modal")))
    
    def test_data_table_advanced_features(self):
        """Test advanced data table features across all pages."""
        # Create test data
        for i in range(50):
            RecipeFactory(name=f'recipe-{i:02d}', user=self.regular_user)
        
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/recipes/")
        
        # Wait for table to load
        table = self.wait.until(EC.presence_of_element_located((By.ID, "recipes-table")))
        
        # Test column sorting
        name_header = table.find_element(By.CSS_SELECTOR, "th[data-sort='name']")
        name_header.click()
        self.wait_for_ajax()
        
        # Verify ascending sort
        first_row = table.find_element(By.CSS_SELECTOR, "tbody tr:first-child")
        self.assertIn("recipe-00", first_row.text)
        
        # Click again for descending sort
        name_header.click()
        self.wait_for_ajax()
        
        first_row = table.find_element(By.CSS_SELECTOR, "tbody tr:first-child")
        self.assertIn("recipe-49", first_row.text)
        
        # Test column filtering
        filter_input = self.browser.find_element(By.CSS_SELECTOR, "input[data-filter='name']")
        filter_input.send_keys("recipe-1")
        filter_input.send_keys(Keys.RETURN)
        
        self.wait_for_ajax()
        
        # Verify filtered results
        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        for row in rows:
            self.assertIn("recipe-1", row.text)
        
        # Test row selection
        first_checkbox = table.find_element(By.CSS_SELECTOR, "tbody tr:first-child input[type='checkbox']")
        first_checkbox.click()
        
        # Verify row is selected
        self.assertTrue(first_checkbox.is_selected())
        
        # Test pagination
        if len(self.browser.find_elements(By.CLASS_NAME, "pagination")) > 0:
            next_btn = self.browser.find_element(By.CSS_SELECTOR, ".pagination .next")
            if next_btn.is_enabled():
                next_btn.click()
                self.wait_for_ajax()
                
                # Verify page changed
                page_info = self.browser.find_element(By.CLASS_NAME, "page-info")
                self.assertIn("2", page_info.text)
    
    def test_file_upload_functionality(self):
        """Test file upload functionality across different forms."""
        self.login_user(self.admin_user)
        self.browser.get(f"{self.live_server_url}/policies/import/")
        
        # Find file upload input
        file_input = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']")))
        
        # Create a temporary test file
        import tempfile
        import os
        
        test_policy = {
            "name": "Test Import Policy",
            "type": "METADATA",
            "state": "ACTIVE"
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(test_policy, f)
            temp_file_path = f.name
        
        try:
            # Upload file
            file_input.send_keys(temp_file_path)
            
            # Wait for file to be processed
            self.wait_for_ajax()
            
            # Check file validation feedback
            file_feedback = self.browser.find_element(By.CLASS_NAME, "file-upload-feedback")
            self.assertIn("valid", file_feedback.text.lower())
            
            # Submit form
            submit_btn = self.browser.find_element(By.CSS_SELECTOR, "button[type='submit']")
            submit_btn.click()
            
            # Wait for import to complete
            self.wait_for_ajax()
            
            # Check for success message
            success_alert = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "alert-success")))
            self.assertIn("import", success_alert.text.lower())
            
        finally:
            # Clean up temporary file
            os.unlink(temp_file_path)
    
    def test_real_time_notifications(self):
        """Test real-time notifications and alerts system."""
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/dashboard/")
        
        # Wait for notification system to initialize
        notifications_container = self.wait.until(EC.presence_of_element_located((By.ID, "notifications-container")))
        
        # Trigger a notification (simulate backend event)
        self.browser.execute_script("""
            window.showNotification('info', 'Test notification from JavaScript tests');
        """)
        
        # Wait for notification to appear
        notification = self.wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "notification")))
        
        # Verify notification content
        self.assertIn("Test notification", notification.text)
        
        # Test notification auto-dismiss
        time.sleep(3)  # Wait for auto-dismiss timeout
        
        self.wait.until(EC.invisibility_of_element_located((By.CLASS_NAME, "notification")))
        
        # Test manual notification dismiss
        self.browser.execute_script("""
            window.showNotification('warning', 'Manual dismiss test', {autoDismiss: false});
        """)
        
        notification = self.wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "notification")))
        
        # Click dismiss button
        dismiss_btn = notification.find_element(By.CLASS_NAME, "dismiss-btn")
        dismiss_btn.click()
        
        # Verify notification is dismissed
        self.wait.until(EC.invisibility_of_element_located((By.CLASS_NAME, "notification")))
    
    def test_form_auto_save_functionality(self):
        """Test form auto-save functionality on long forms."""
        self.login_user(self.admin_user)
        self.browser.get(f"{self.live_server_url}/policies/create/")
        
        # Find form inputs
        name_input = self.wait.until(EC.presence_of_element_located((By.NAME, "name")))
        description_input = self.browser.find_element(By.NAME, "description")
        
        # Type in inputs
        name_input.send_keys("Auto-save test policy")
        description_input.send_keys("This policy tests the auto-save functionality")
        
        # Wait for auto-save to trigger
        time.sleep(2)
        
        # Check for auto-save indicator
        auto_save_indicator = self.browser.find_element(By.ID, "auto-save-status")
        self.assertIn("saved", auto_save_indicator.text.lower())
        
        # Refresh page to test restoration
        self.browser.refresh()
        
        # Wait for page to load
        name_input = self.wait.until(EC.presence_of_element_located((By.NAME, "name")))
        
        # Verify auto-saved data is restored
        self.assertEqual(name_input.get_attribute("value"), "Auto-save test policy")
    
    def test_connection_switching_ui(self):
        """Test DataHub connection switching interface."""
        # Create additional environment for switching
        additional_env = EnvironmentFactory(name='test-env-2')
        
        self.login_user(self.admin_user)
        self.browser.get(f"{self.live_server_url}/connections/")
        
        # Wait for connections page to load
        connections_table = self.wait.until(EC.presence_of_element_located((By.ID, "connections-table")))
        
        # Find connection switch button
        switch_btns = self.browser.find_elements(By.CSS_SELECTOR, ".btn-switch-connection")
        if len(switch_btns) > 0:
            switch_btns[0].click()
            
            # Wait for switch to complete
            self.wait_for_ajax()
            
            # Check for success message
            success_alert = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "alert-success")))
            self.assertIn("switched", success_alert.text.lower())
            
            # Verify connection indicator updated
            active_indicator = self.browser.find_element(By.CLASS_NAME, "connection-active")
            self.assertIsNotNone(active_indicator)

# ... existing code continues with similar comprehensive test methods for all remaining pages ... 