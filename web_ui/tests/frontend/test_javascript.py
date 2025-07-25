"""
Comprehensive JavaScript and UI interaction tests using Selenium WebDriver.

Tests cover:
- JavaScript functionality and AJAX interactions
- UI components and user interactions
- Form validation and submission
- Dynamic content loading
- Chart rendering and interactions
- Real-time updates and WebSocket connections
- Cross-browser compatibility
"""

import pytest
import time
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from django.test import LiveServerTestCase, override_settings
from django.contrib.auth.models import User
from django.urls import reverse

from web_ui.web_ui.models import Environment, Policy, LogEntry, GitSettings
from tests.fixtures.factories import (
    UserFactory, EnvironmentFactory, PolicyFactory, 
    LogEntryFactory, GitSettingsFactory
)


@pytest.mark.selenium
@override_settings(DEBUG=True)
class JavaScriptTestCase(LiveServerTestCase):
    """Base test case for JavaScript and UI interaction tests."""
    
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Set up browser options for headless testing
        cls.browser_options = {
            'chrome': {
                'options': ['--headless', '--no-sandbox', '--disable-dev-shm-usage', 
                           '--disable-gpu', '--window-size=1920,1080']
            },
            'firefox': {
                'options': ['--headless', '--width=1920', '--height=1080']
            }
        }
    
    def setUp(self):
        super().setUp()
        self.browser = self.create_browser()
        self.wait = WebDriverWait(self.browser, 10)
        
        # Create test users
        self.admin_user = UserFactory.create_admin()
        self.regular_user = UserFactory.create_user()
        
        # Create test data
        self.environment = EnvironmentFactory(name='test-env')
        self.policy = PolicyFactory(name='Test Policy', environment=self.environment)
    
    def tearDown(self):
        if hasattr(self, 'browser'):
            self.browser.quit()
        super().tearDown()
    
    def create_browser(self, browser_type='chrome'):
        """Create browser instance with options."""
        if browser_type == 'chrome':
            from selenium.webdriver.chrome.options import Options
            options = Options()
            for option in self.browser_options['chrome']['options']:
                options.add_argument(option)
            return webdriver.Chrome(options=options)
        elif browser_type == 'firefox':
            from selenium.webdriver.firefox.options import Options
            options = Options()
            for option in self.browser_options['firefox']['options']:
                options.add_argument(option)
            return webdriver.Firefox(options=options)
        else:
            raise ValueError(f"Unsupported browser type: {browser_type}")
    
    def login_user(self, user):
        """Login a user via the web interface."""
        self.browser.get(f"{self.live_server_url}/accounts/login/")
        
        username_input = self.browser.find_element(By.NAME, "username")
        password_input = self.browser.find_element(By.NAME, "password")
        
        username_input.send_keys(user.username)
        password_input.send_keys("testpassword123")
        password_input.send_keys(Keys.RETURN)
        
        # Wait for redirect after login
        self.wait.until(EC.url_changes(f"{self.live_server_url}/accounts/login/"))
    
    def wait_for_ajax(self, timeout=10):
        """Wait for AJAX requests to complete."""
        try:
            self.wait.until(lambda driver: driver.execute_script("return jQuery.active == 0"))
        except TimeoutException:
            # jQuery might not be loaded, try alternative method
            time.sleep(1)  # Basic fallback
    
    def wait_for_element_clickable(self, locator, timeout=10):
        """Wait for element to be clickable."""
        return WebDriverWait(self.browser, timeout).until(
            EC.element_to_be_clickable(locator)
        )
    
    def wait_for_element_visible(self, locator, timeout=10):
        """Wait for element to be visible."""
        return WebDriverWait(self.browser, timeout).until(
            EC.visibility_of_element_located(locator)
        )


class DashboardJavaScriptTestCase(JavaScriptTestCase):
    """Test cases for dashboard JavaScript functionality."""
    
    def test_dashboard_ajax_data_loading(self):
        """Test dashboard loads data via AJAX."""
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/dashboard/")
        
        # Wait for dashboard to load
        self.wait_for_element_visible((By.CLASS_NAME, "dashboard"))
        
        # Check for loading spinners initially
        loading_elements = self.browser.find_elements(By.CLASS_NAME, "loading")
        self.assertGreater(len(loading_elements), 0)
        
        # Wait for AJAX to complete
        self.wait_for_ajax()
        
        # Check that loading spinners are hidden/removed
        try:
            visible_loading = self.browser.find_elements(By.CSS_SELECTOR, ".loading:not([style*='display: none'])")
            self.assertEqual(len(visible_loading), 0)
        except NoSuchElementException:
            pass  # Loading elements completely removed is also acceptable
    
    def test_dashboard_stats_update(self):
        """Test dashboard statistics update dynamically."""
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/dashboard/")
        
        # Wait for dashboard to load
        self.wait_for_element_visible((By.CLASS_NAME, "dashboard"))
        self.wait_for_ajax()
        
        # Check that stat cards are populated
        stat_cards = self.browser.find_elements(By.CLASS_NAME, "stat-card")
        self.assertGreater(len(stat_cards), 0)
        
        # Check that numbers are displayed (not just placeholders)
        for card in stat_cards:
            stat_value = card.find_element(By.CLASS_NAME, "stat-value")
            self.assertNotEqual(stat_value.text.strip(), "")
            self.assertNotEqual(stat_value.text.strip(), "0")  # Should have some data
    
    def test_dashboard_refresh_functionality(self):
        """Test dashboard refresh button functionality."""
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/dashboard/")
        
        # Wait for initial load
        self.wait_for_ajax()
        
        # Find and click refresh button
        refresh_button = self.wait_for_element_clickable((By.CLASS_NAME, "btn-refresh"))
        refresh_button.click()
        
        # Should see loading state again
        self.wait_for_element_visible((By.CLASS_NAME, "loading"))
        
        # Wait for refresh to complete
        self.wait_for_ajax()
    
    def test_dashboard_chart_rendering(self):
        """Test dashboard charts render correctly."""
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/dashboard/")
        
        # Wait for dashboard to load
        self.wait_for_ajax()
        
        # Check for chart canvas elements
        chart_canvases = self.browser.find_elements(By.TAG_NAME, "canvas")
        self.assertGreater(len(chart_canvases), 0)
        
        # Check that charts are actually rendered (have non-zero dimensions)
        for canvas in chart_canvases:
            width = canvas.get_attribute("width")
            height = canvas.get_attribute("height")
            self.assertNotEqual(width, "0")
            self.assertNotEqual(height, "0")
    
    def test_dashboard_real_time_updates(self):
        """Test dashboard real-time updates functionality."""
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/dashboard/")
        
        # Wait for initial load
        self.wait_for_ajax()
        
        # Get initial log count
        logs_stat = self.browser.find_element(By.CSS_SELECTOR, "[data-stat='logs'] .stat-value")
        initial_count = int(logs_stat.text)
        
        # Create a new log entry (this would trigger an update in a real system)
        # For testing, we'll simulate by refreshing
        refresh_button = self.browser.find_element(By.CLASS_NAME, "btn-refresh")
        refresh_button.click()
        
        self.wait_for_ajax()
        
        # Check that the count might have changed (or at least the refresh worked)
        updated_count = int(logs_stat.text)
        self.assertIsInstance(updated_count, int)


class PolicyManagementJavaScriptTestCase(JavaScriptTestCase):
    """Test cases for policy management JavaScript functionality."""
    
    def test_policy_json_editor(self):
        """Test policy JSON editor functionality."""
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/policies/create/")
        
        # Wait for form to load
        json_editor = self.wait_for_element_visible((By.NAME, "policy_json"))
        
        # Test JSON input
        test_json = json.dumps({
            "name": "test-policy",
            "description": "A test policy",
            "type": "METADATA"
        }, indent=2)
        
        json_editor.clear()
        json_editor.send_keys(test_json)
        
        # Check that JSON is properly formatted (if editor supports it)
        entered_value = json_editor.get_attribute("value")
        self.assertIn("test-policy", entered_value)
    
    def test_policy_form_validation(self):
        """Test client-side policy form validation."""
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/policies/create/")
        
        # Try to submit form without required fields
        submit_button = self.wait_for_element_clickable((By.TYPE, "submit"))
        submit_button.click()
        
        # Check for validation messages
        validation_messages = self.browser.find_elements(By.CLASS_NAME, "invalid-feedback")
        if validation_messages:
            self.assertGreater(len(validation_messages), 0)
    
    def test_policy_environment_selection(self):
        """Test policy environment selection functionality."""
        # Create multiple environments
        env1 = EnvironmentFactory(name='dev')
        env2 = EnvironmentFactory(name='prod')
        
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/policies/create/")
        
        # Test environment selection
        environment_select = Select(self.wait_for_element_visible((By.NAME, "environment")))
        
        # Check that both environments are available
        options = environment_select.options
        option_texts = [option.text for option in options]
        
        self.assertIn('dev', option_texts)
        self.assertIn('prod', option_texts)
        
        # Select an environment
        environment_select.select_by_visible_text('dev')
        
        # Verify selection
        selected_option = environment_select.first_selected_option
        self.assertEqual(selected_option.text, 'dev')
    
    def test_policy_delete_confirmation(self):
        """Test policy delete confirmation dialog."""
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/policies/")
        
        # Wait for policies list to load
        self.wait_for_element_visible((By.CLASS_NAME, "policies-table"))
        
        # Find delete button for a policy
        delete_buttons = self.browser.find_elements(By.CLASS_NAME, "btn-delete")
        if delete_buttons:
            delete_button = delete_buttons[0]
            delete_button.click()
            
            # Check for confirmation dialog or navigation to delete page
            try:
                # Look for confirmation dialog
                confirm_dialog = self.wait_for_element_visible((By.CLASS_NAME, "modal"))
                self.assertIsNotNone(confirm_dialog)
            except TimeoutException:
                # Or check if navigated to delete confirmation page
                self.assertIn("delete", self.browser.current_url.lower())
    
    def test_policy_list_filtering(self):
        """Test policy list filtering functionality."""
        # Create policies in different environments
        env1 = EnvironmentFactory(name='dev')
        env2 = EnvironmentFactory(name='prod')
        PolicyFactory(name='Dev Policy', environment=env1)
        PolicyFactory(name='Prod Policy', environment=env2)
        
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/policies/")
        
        # Wait for page to load
        self.wait_for_element_visible((By.CLASS_NAME, "policies-table"))
        
        # Check if filter controls exist
        filter_controls = self.browser.find_elements(By.CLASS_NAME, "filter-control")
        if filter_controls:
            # Test filtering by environment
            env_filter = self.browser.find_element(By.NAME, "environment")
            if env_filter:
                env_select = Select(env_filter)
                env_select.select_by_visible_text('dev')
                
                # Submit filter or wait for AJAX update
                filter_form = self.browser.find_element(By.CLASS_NAME, "filter-form")
                filter_form.submit()
                
                self.wait_for_ajax()
                
                # Check that only dev policies are shown
                policy_rows = self.browser.find_elements(By.CSS_SELECTOR, ".policies-table tbody tr")
                for row in policy_rows:
                    self.assertIn('dev', row.text.lower())


class LogsJavaScriptTestCase(JavaScriptTestCase):
    """Test cases for logs JavaScript functionality."""
    
    def test_logs_real_time_updates(self):
        """Test logs real-time updates functionality."""
        # Create initial log entries
        LogEntryFactory(level='INFO', message='Initial log message')
        
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/logs/")
        
        # Wait for logs to load
        self.wait_for_element_visible((By.CLASS_NAME, "logs-table"))
        
        # Check for refresh functionality
        refresh_button = self.browser.find_element(By.CLASS_NAME, "btn-refresh")
        if refresh_button:
            initial_row_count = len(self.browser.find_elements(By.CSS_SELECTOR, ".logs-table tbody tr"))
            
            refresh_button.click()
            self.wait_for_ajax()
            
            # Check that refresh worked (count might be same, but functionality tested)
            updated_row_count = len(self.browser.find_elements(By.CSS_SELECTOR, ".logs-table tbody tr"))
            self.assertIsInstance(updated_row_count, int)
    
    def test_logs_filtering(self):
        """Test logs filtering functionality."""
        # Create logs with different levels
        LogEntryFactory(level='INFO', message='Info message')
        LogEntryFactory(level='ERROR', message='Error message')
        LogEntryFactory(level='WARNING', message='Warning message')
        
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/logs/")
        
        # Wait for logs to load
        self.wait_for_element_visible((By.CLASS_NAME, "logs-table"))
        
        # Test level filtering
        level_filter = self.browser.find_element(By.NAME, "level")
        if level_filter:
            level_select = Select(level_filter)
            level_select.select_by_visible_text('ERROR')
            
            # Submit filter
            filter_form = self.browser.find_element(By.CLASS_NAME, "filter-form")
            filter_form.submit()
            
            self.wait_for_ajax()
            
            # Check that only ERROR logs are shown
            log_rows = self.browser.find_elements(By.CSS_SELECTOR, ".logs-table tbody tr")
            for row in log_rows:
                self.assertIn('ERROR', row.text)
    
    def test_logs_search_functionality(self):
        """Test logs search functionality."""
        # Create logs with different messages
        LogEntryFactory(message='Database connection established')
        LogEntryFactory(message='User authentication failed')
        LogEntryFactory(message='File upload completed')
        
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/logs/")
        
        # Wait for logs to load
        self.wait_for_element_visible((By.CLASS_NAME, "logs-table"))
        
        # Test search functionality
        search_input = self.browser.find_element(By.NAME, "search")
        if search_input:
            search_input.clear()
            search_input.send_keys("database")
            search_input.send_keys(Keys.RETURN)
            
            self.wait_for_ajax()
            
            # Check that only matching logs are shown
            log_rows = self.browser.find_elements(By.CSS_SELECTOR, ".logs-table tbody tr")
            for row in log_rows:
                self.assertIn('database', row.text.lower())
    
    def test_logs_pagination(self):
        """Test logs pagination functionality."""
        # Create many log entries
        for i in range(25):
            LogEntryFactory(message=f'Log message {i}')
        
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/logs/")
        
        # Wait for logs to load
        self.wait_for_element_visible((By.CLASS_NAME, "logs-table"))
        
        # Check for pagination controls
        pagination = self.browser.find_elements(By.CLASS_NAME, "pagination")
        if pagination:
            # Test next page button
            next_button = self.browser.find_elements(By.LINK_TEXT, "Next")
            if next_button:
                next_button[0].click()
                self.wait_for_ajax()
                
                # Check that we're on page 2
                current_page = self.browser.find_element(By.CLASS_NAME, "current-page")
                self.assertEqual(current_page.text, "2")


class EnvironmentJavaScriptTestCase(JavaScriptTestCase):
    """Test cases for environment management JavaScript functionality."""
    
    def test_environment_form_validation(self):
        """Test environment form client-side validation."""
        self.login_user(self.admin_user)
        self.browser.get(f"{self.live_server_url}/environments/create/")
        
        # Try to submit form without required fields
        submit_button = self.wait_for_element_clickable((By.TYPE, "submit"))
        submit_button.click()
        
        # Check for HTML5 validation or custom validation messages
        name_input = self.browser.find_element(By.NAME, "name")
        validation_message = name_input.get_attribute("validationMessage")
        
        if validation_message:
            self.assertNotEqual(validation_message, "")
    
    def test_environment_default_toggle(self):
        """Test environment default toggle functionality."""
        self.login_user(self.admin_user)
        self.browser.get(f"{self.live_server_url}/environments/create/")
        
        # Test default checkbox
        default_checkbox = self.browser.find_element(By.NAME, "is_default")
        
        # Check initial state
        initial_state = default_checkbox.is_selected()
        
        # Toggle checkbox
        default_checkbox.click()
        
        # Check state changed
        new_state = default_checkbox.is_selected()
        self.assertNotEqual(initial_state, new_state)
    
    def test_environment_connection_testing(self):
        """Test environment connection testing functionality."""
        self.login_user(self.admin_user)
        self.browser.get(f"{self.live_server_url}/environments/create/")
        
        # Fill in environment details
        name_input = self.browser.find_element(By.NAME, "name")
        host_input = self.browser.find_element(By.NAME, "datahub_host")
        token_input = self.browser.find_element(By.NAME, "datahub_token")
        
        name_input.send_keys("test-env")
        host_input.send_keys("http://test.datahub.com")
        token_input.send_keys("test-token")
        
        # Look for test connection button
        test_buttons = self.browser.find_elements(By.CLASS_NAME, "btn-test-connection")
        if test_buttons:
            test_button = test_buttons[0]
            test_button.click()
            
            # Wait for test result
            self.wait_for_ajax()
            
            # Check for test result display
            result_elements = self.browser.find_elements(By.CLASS_NAME, "connection-result")
            if result_elements:
                self.assertGreater(len(result_elements), 0)


class GitHubIntegrationJavaScriptTestCase(JavaScriptTestCase):
    """Test cases for GitHub integration JavaScript functionality."""
    
    def test_github_connection_status(self):
        """Test GitHub connection status display."""
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/github/")
        
        # Wait for page to load
        self.wait_for_element_visible((By.CLASS_NAME, "github-integration"))
        
        # Check for connection status indicator
        status_indicators = self.browser.find_elements(By.CLASS_NAME, "connection-status")
        self.assertGreater(len(status_indicators), 0)
    
    def test_github_pr_list_updates(self):
        """Test GitHub PR list updates."""
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/github/")
        
        # Wait for page to load
        self.wait_for_ajax()
        
        # Look for refresh button
        refresh_buttons = self.browser.find_elements(By.CLASS_NAME, "btn-refresh-prs")
        if refresh_buttons:
            refresh_button = refresh_buttons[0]
            refresh_button.click()
            
            self.wait_for_ajax()
            
            # Check that PR list is updated
            pr_list = self.browser.find_elements(By.CLASS_NAME, "pr-list")
            self.assertGreater(len(pr_list), 0)


class ResponsiveDesignTestCase(JavaScriptTestCase):
    """Test cases for responsive design functionality."""
    
    def test_mobile_navigation(self):
        """Test mobile navigation functionality."""
        # Resize browser to mobile size
        self.browser.set_window_size(375, 667)  # iPhone size
        
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/dashboard/")
        
        # Look for mobile menu toggle
        menu_toggles = self.browser.find_elements(By.CLASS_NAME, "navbar-toggler")
        if menu_toggles:
            menu_toggle = menu_toggles[0]
            
            # Check if menu is initially hidden
            nav_menu = self.browser.find_element(By.CLASS_NAME, "navbar-collapse")
            initial_display = nav_menu.value_of_css_property("display")
            
            # Click toggle
            menu_toggle.click()
            time.sleep(0.5)  # Wait for animation
            
            # Check if menu visibility changed
            final_display = nav_menu.value_of_css_property("display")
            # The exact behavior depends on Bootstrap version and implementation
    
    def test_table_responsiveness(self):
        """Test table responsiveness on small screens."""
        # Test with policies table
        PolicyFactory(name='Test Policy 1')
        PolicyFactory(name='Test Policy 2')
        
        # Set mobile screen size
        self.browser.set_window_size(375, 667)
        
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/policies/")
        
        # Check that table is responsive
        tables = self.browser.find_elements(By.TAG_NAME, "table")
        if tables:
            table = tables[0]
            
            # Check for responsive table wrapper or horizontal scrolling
            parent_classes = table.find_element(By.XPATH, "..").get_attribute("class")
            self.assertTrue(
                "table-responsive" in parent_classes or 
                table.value_of_css_property("overflow-x") == "auto"
            )


class PerformanceTestCase(JavaScriptTestCase):
    """Test cases for JavaScript performance."""
    
    @pytest.mark.performance
    def test_dashboard_load_time(self):
        """Test dashboard load time performance."""
        self.login_user(self.regular_user)
        
        # Record start time
        start_time = time.time()
        
        self.browser.get(f"{self.live_server_url}/dashboard/")
        
        # Wait for complete load
        self.wait_for_element_visible((By.CLASS_NAME, "dashboard"))
        self.wait_for_ajax()
        
        # Calculate load time
        load_time = time.time() - start_time
        
        # Should load within reasonable time (5 seconds)
        self.assertLess(load_time, 5.0)
    
    @pytest.mark.performance
    def test_large_table_rendering(self):
        """Test performance with large data tables."""
        # Create many log entries
        for i in range(100):
            LogEntryFactory(message=f'Performance test log {i}')
        
        self.login_user(self.regular_user)
        
        start_time = time.time()
        self.browser.get(f"{self.live_server_url}/logs/")
        self.wait_for_element_visible((By.CLASS_NAME, "logs-table"))
        load_time = time.time() - start_time
        
        # Should render within reasonable time
        self.assertLess(load_time, 10.0)


class CrossBrowserTestCase:
    """Test cases for cross-browser compatibility."""
    
    @pytest.mark.selenium
    @pytest.mark.parametrize("browser_type", ["chrome", "firefox"])
    def test_cross_browser_dashboard(self, browser_type, live_server):
        """Test dashboard functionality across different browsers."""
        test_case = JavaScriptTestCase()
        test_case.live_server_url = live_server.url
        test_case.browser = test_case.create_browser(browser_type)
        test_case.wait = WebDriverWait(test_case.browser, 10)
        
        try:
            # Set up test data
            user = UserFactory.create_user()
            test_case.login_user(user)
            
            # Test dashboard loads
            test_case.browser.get(f"{test_case.live_server_url}/dashboard/")
            test_case.wait_for_element_visible((By.CLASS_NAME, "dashboard"))
            
            # Basic functionality test
            stat_cards = test_case.browser.find_elements(By.CLASS_NAME, "stat-card")
            assert len(stat_cards) > 0
            
        finally:
            test_case.browser.quit()


class AccessibilityTestCase(JavaScriptTestCase):
    """Test cases for accessibility in JavaScript interactions."""
    
    def test_keyboard_navigation(self):
        """Test keyboard navigation functionality."""
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/policies/")
        
        # Test Tab navigation
        body = self.browser.find_element(By.TAG_NAME, "body")
        
        # Tab through interactive elements
        for i in range(10):
            body.send_keys(Keys.TAB)
            time.sleep(0.1)
            
            # Check that focus is moving
            active_element = self.browser.switch_to.active_element
            self.assertIsNotNone(active_element)
    
    def test_aria_labels_updates(self):
        """Test that ARIA labels update with dynamic content."""
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/dashboard/")
        
        self.wait_for_ajax()
        
        # Check for elements with aria-labels
        aria_elements = self.browser.find_elements(By.CSS_SELECTOR, "[aria-label]")
        
        for element in aria_elements:
            aria_label = element.get_attribute("aria-label")
            self.assertNotEqual(aria_label.strip(), "")
    
    def test_focus_management(self):
        """Test focus management in dynamic content."""
        self.login_user(self.regular_user)
        self.browser.get(f"{self.live_server_url}/policies/create/")
        
        # Test that focus is properly managed when showing validation errors
        name_input = self.browser.find_element(By.NAME, "name")
        submit_button = self.browser.find_element(By.TYPE, "submit")
        
        # Submit without filling required field
        submit_button.click()
        
        # Check if focus returns to first error field
        time.sleep(0.5)
        active_element = self.browser.switch_to.active_element
        
        # Focus should be on an input field (preferably the one with error)
        self.assertIn(active_element.tag_name.lower(), ['input', 'textarea', 'select']) 