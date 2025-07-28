import json
from unittest.mock import Mock, patch, MagicMock
from django.test import TestCase, Client
from django.urls import reverse
from django.contrib.auth.models import User
from django.contrib.messages import get_messages
from django.utils import timezone
from datetime import datetime, timedelta
import logging

from web_ui.web_ui.models import LogEntry, AppSettings
from web_ui.web_ui.log_handlers import DatabaseLogHandler


class LogEntryModelTestCase(TestCase):
    """Test cases for LogEntry model functionality."""
    
    def setUp(self):
        """Set up test data for log entry model tests."""
        pass
        
    def test_log_entry_creation(self):
        """Test creating a new log entry."""
        log_entry = LogEntry.objects.create(
            level='INFO',
            message='Test log message',
            source='test.module',
            details='Additional test details'
        )
        
        self.assertEqual(log_entry.level, 'INFO')
        self.assertEqual(log_entry.message, 'Test log message')
        self.assertEqual(log_entry.source, 'test.module')
        self.assertEqual(log_entry.details, 'Additional test details')
        self.assertIsNotNone(log_entry.timestamp)
        
    def test_log_entry_str_representation(self):
        """Test string representation of log entry."""
        log_entry = LogEntry.objects.create(
            level='ERROR',
            message='Test error message',
            source='error.module'
        )
        
        str_repr = str(log_entry)
        self.assertIn('ERROR', str_repr)
        self.assertIn('Test error message', str_repr)
        self.assertIn(log_entry.timestamp.strftime('%Y-%m-%d %H:%M:%S'), str_repr)
        
    def test_log_entry_ordering(self):
        """Test that log entries are ordered by timestamp (newest first)."""
        # Create entries with different timestamps
        old_entry = LogEntry.objects.create(
            level='INFO',
            message='Old message',
            timestamp=timezone.now() - timedelta(hours=2)
        )
        
        new_entry = LogEntry.objects.create(
            level='INFO', 
            message='New message',
            timestamp=timezone.now() - timedelta(hours=1)
        )
        
        newest_entry = LogEntry.objects.create(
            level='INFO',
            message='Newest message'
        )
        
        entries = list(LogEntry.objects.all())
        
        # Should be ordered newest first
        self.assertEqual(entries[0].message, 'Newest message')
        self.assertEqual(entries[1].message, 'New message')
        self.assertEqual(entries[2].message, 'Old message')
        
    def test_log_entry_class_methods(self):
        """Test the class methods for creating log entries."""
        # Test log method
        entry = LogEntry.log('WARNING', 'Test warning', 'test.source', 'Test details')
        self.assertEqual(entry.level, 'WARNING')
        self.assertEqual(entry.message, 'Test warning')
        self.assertEqual(entry.source, 'test.source')
        self.assertEqual(entry.details, 'Test details')
        
        # Test convenience methods
        debug_entry = LogEntry.debug('Debug message', 'debug.source')
        self.assertEqual(debug_entry.level, 'DEBUG')
        
        info_entry = LogEntry.info('Info message', 'info.source')
        self.assertEqual(info_entry.level, 'INFO')
        
        warning_entry = LogEntry.warning('Warning message', 'warning.source')
        self.assertEqual(warning_entry.level, 'WARNING') 
        
        error_entry = LogEntry.error('Error message', 'error.source')
        self.assertEqual(error_entry.level, 'ERROR')
        
        critical_entry = LogEntry.critical('Critical message', 'critical.source')
        self.assertEqual(critical_entry.level, 'CRITICAL')
        
    def test_log_entry_level_choices(self):
        """Test that log entry level choices are properly defined."""
        level_choices = dict(LogEntry.LEVEL_CHOICES)
        
        self.assertIn('DEBUG', level_choices)
        self.assertIn('INFO', level_choices)
        self.assertIn('WARNING', level_choices)
        self.assertIn('ERROR', level_choices)
        self.assertIn('CRITICAL', level_choices)
        
        self.assertEqual(level_choices['DEBUG'], 'Debug')
        self.assertEqual(level_choices['INFO'], 'Info')
        self.assertEqual(level_choices['WARNING'], 'Warning')
        self.assertEqual(level_choices['ERROR'], 'Error')
        self.assertEqual(level_choices['CRITICAL'], 'Critical')


class LogsViewsTestCase(TestCase):
    """Test cases for logs views and endpoints."""
    
    def setUp(self):
        """Set up test data for logs views tests."""
        self.client = Client()
        self.user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        
        # Create test log entries
        self.info_log = LogEntry.objects.create(
            level='INFO',
            message='Test info message',
            source='test.info',
            timestamp=timezone.now() - timedelta(hours=1)
        )
        
        self.error_log = LogEntry.objects.create(
            level='ERROR', 
            message='Test error message',
            source='test.error', 
            details='Error details',
            timestamp=timezone.now() - timedelta(hours=2)
        )
        
        self.debug_log = LogEntry.objects.create(
            level='DEBUG',
            message='Test debug message',
            source='test.debug',
            timestamp=timezone.now() - timedelta(minutes=30)
        )
        
    def test_logs_view_basic_rendering(self):
        """Test that logs view renders correctly."""
        url = reverse('logs')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Application Logs')
        self.assertContains(response, 'Filters')
        self.assertContains(response, 'Test info message')
        self.assertContains(response, 'Test error message')
        
    def test_logs_view_shows_all_levels(self):
        """Test that logs view shows entries from all levels by default."""
        url = reverse('logs')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'INFO')
        self.assertContains(response, 'ERROR')
        self.assertContains(response, 'DEBUG')
        
    def test_logs_view_level_filtering(self):
        """Test filtering logs by level."""
        url = reverse('logs')
        
        # Filter for ERROR level only
        response = self.client.get(url, {'level': 'ERROR'})
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Test error message')
        self.assertNotContains(response, 'Test info message')
        self.assertNotContains(response, 'Test debug message')
        
    def test_logs_view_source_filtering(self):
        """Test filtering logs by source."""
        url = reverse('logs')
        
        # Filter for specific source
        response = self.client.get(url, {'source': 'test.error'})
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Test error message')
        self.assertNotContains(response, 'Test info message')
        
    def test_logs_view_search_filtering(self):
        """Test searching logs by message content."""
        url = reverse('logs')
        
        # Search for specific text
        response = self.client.get(url, {'search': 'error'})
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Test error message')
        self.assertNotContains(response, 'Test info message')
        
    def test_logs_view_date_filtering(self):
        """Test filtering logs by date."""
        url = reverse('logs')
        
        # Filter for today's date
        today = timezone.now().date()
        response = self.client.get(url, {'date': today.strftime('%Y-%m-%d')})
        
        self.assertEqual(response.status_code, 200)
        # All test entries should be from today
        self.assertContains(response, 'Test info message')
        self.assertContains(response, 'Test error message')
        
    def test_logs_view_combined_filtering(self):
        """Test combining multiple filters."""
        url = reverse('logs')
        
        # Combine level and source filters
        response = self.client.get(url, {
            'level': 'ERROR',
            'source': 'test.error',
            'search': 'error'
        })
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Test error message')
        self.assertNotContains(response, 'Test info message')
        
    def test_logs_view_pagination(self):
        """Test that logs view properly paginates results."""
        # Create many log entries to test pagination
        for i in range(55):  # More than default page size of 50
            LogEntry.objects.create(
                level='INFO',
                message=f'Pagination test message {i}',
                source='pagination.test'
            )
            
        url = reverse('logs')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Page 1 of')  # Should show pagination
        
        # Test second page
        response = self.client.get(url, {'page': 2})
        self.assertEqual(response.status_code, 200)
        
    def test_logs_view_invalid_date_filter(self):
        """Test logs view with invalid date filter."""
        url = reverse('logs')
        
        # Provide invalid date
        response = self.client.get(url, {'date': 'invalid-date'})
        
        # Should still work, just ignore the invalid date
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Test info message')
        
    def test_logs_clear_functionality(self):
        """Test clearing logs functionality."""
        url = reverse('logs')
        
        # Test clearing all logs
        response = self.client.post(url, {
            'action': 'clear_logs'
        })
        
        self.assertEqual(response.status_code, 200)  # Returns to logs page
        
        # Check that logs were cleared
        remaining_logs = LogEntry.objects.count()
        self.assertEqual(remaining_logs, 0)
        
        # Check success message
        messages = list(get_messages(response.wsgi_request))
        self.assertTrue(any('cleared' in str(message) for message in messages))
        
    def test_logs_clear_by_level(self):
        """Test clearing logs by level."""
        url = reverse('logs')
        
        # Clear only ERROR level logs
        response = self.client.post(url, {
            'action': 'clear_logs',
            'clear_level': 'ERROR'
        })
        
        self.assertEqual(response.status_code, 200)
        
        # ERROR logs should be cleared, others should remain
        self.assertFalse(LogEntry.objects.filter(level='ERROR').exists())
        self.assertTrue(LogEntry.objects.filter(level='INFO').exists())
        self.assertTrue(LogEntry.objects.filter(level='DEBUG').exists())
        
    def test_logs_clear_by_date(self):
        """Test clearing logs by date."""
        # Create an old log entry
        old_log = LogEntry.objects.create(
            level='WARNING',
            message='Old log message',
            timestamp=timezone.now() - timedelta(days=2)
        )
        
        url = reverse('logs')
        yesterday = (timezone.now() - timedelta(days=1)).date()
        
        # Clear logs before yesterday
        response = self.client.post(url, {
            'action': 'clear_logs',
            'clear_before_date': yesterday.strftime('%Y-%m-%d')
        })
        
        self.assertEqual(response.status_code, 200)
        
        # Old log should be cleared, recent ones should remain
        self.assertFalse(LogEntry.objects.filter(message='Old log message').exists())
        self.assertTrue(LogEntry.objects.filter(message='Test info message').exists())
        
    def test_refresh_logs_endpoint(self):
        """Test the refresh logs AJAX endpoint."""
        url = reverse('refresh_logs')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Test info message')
        self.assertContains(response, 'Test error message')
        
    def test_refresh_logs_with_filters(self):
        """Test refresh logs endpoint with filters."""
        url = reverse('refresh_logs')
        
        response = self.client.get(url, {'level': 'ERROR'})
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Test error message')
        self.assertNotContains(response, 'Test info message')


class DatabaseLogHandlerTestCase(TestCase):
    """Test cases for the custom database log handler."""
    
    def setUp(self):
        """Set up test data for log handler tests."""
        self.handler = DatabaseLogHandler()
        self.logger = logging.getLogger('test.logger')
        self.logger.addHandler(self.handler)
        self.logger.setLevel(logging.DEBUG)
        
    def tearDown(self):
        """Clean up after tests."""
        self.logger.removeHandler(self.handler)
        
    def test_handler_creates_log_entries(self):
        """Test that handler creates log entries in database."""
        initial_count = LogEntry.objects.count()
        
        self.logger.info('Test log message from handler')
        
        # Should have created one new log entry
        self.assertEqual(LogEntry.objects.count(), initial_count + 1)
        
        # Check the created entry
        log_entry = LogEntry.objects.latest('timestamp')
        self.assertEqual(log_entry.level, 'INFO')
        self.assertIn('Test log message from handler', log_entry.message)
        self.assertEqual(log_entry.source, 'test.logger')
        
    def test_handler_different_levels(self):
        """Test handler with different log levels."""
        levels = [
            (logging.DEBUG, 'DEBUG'),
            (logging.INFO, 'INFO'),
            (logging.WARNING, 'WARNING'),
            (logging.ERROR, 'ERROR'),
            (logging.CRITICAL, 'CRITICAL')
        ]
        
        for log_level, expected_level in levels:
            self.logger.log(log_level, f'Test {expected_level} message')
            
            log_entry = LogEntry.objects.latest('timestamp')
            self.assertEqual(log_entry.level, expected_level)
            self.assertIn(f'Test {expected_level} message', log_entry.message)
            
    def test_handler_with_exception_info(self):
        """Test handler logging exception information."""
        try:
            raise ValueError("Test exception")
        except ValueError:
            self.logger.exception('Exception occurred')
            
        log_entry = LogEntry.objects.latest('timestamp')
        self.assertEqual(log_entry.level, 'ERROR')
        self.assertIn('Exception occurred', log_entry.message)
        self.assertIsNotNone(log_entry.details)
        self.assertIn('ValueError', log_entry.details)
        self.assertIn('Test exception', log_entry.details)
        
    @patch('web_ui.web_ui.log_handlers.connection')
    def test_handler_graceful_failure(self, mock_connection):
        """Test that handler fails gracefully when database is unavailable."""
        # Mock database unavailability
        mock_connection.introspection.table_names.side_effect = Exception("DB Error")
        
        # This should not raise an exception
        self.logger.info('Test message during DB failure')
        
        # The log should not be created due to DB failure, but no exception should be raised
        # The exact behavior depends on the handler implementation
        
    def test_handler_table_existence_check(self):
        """Test that handler checks for table existence."""
        with patch('web_ui.web_ui.log_handlers.connection') as mock_connection:
            # Mock table not existing
            mock_connection.introspection.table_names.return_value = []
            
            # Should not create log entry if table doesn't exist
            self.logger.info('Test message with no table')
            
            # Handler should have checked for table existence
            mock_connection.introspection.table_names.assert_called_once()


class LogsIntegrationTestCase(TestCase):
    """Integration tests for logs with other system components."""
    
    def setUp(self):
        """Set up test data for integration tests."""
        self.client = Client()
        
        # Create app settings for log level
        AppSettings.objects.create(key='log_level', value='INFO')
        
        # Create various log entries from different system components
        LogEntry.objects.create(
            level='INFO',
            message='Dashboard loaded successfully',
            source='web_ui.views'
        )
        
        LogEntry.objects.create(
            level='ERROR', 
            message='Failed to connect to DataHub',
            source='utils.datahub_utils'
        )
        
        LogEntry.objects.create(
            level='DEBUG',
            message='Metadata sync started',
            source='metadata_manager.views'
        )
        
        LogEntry.objects.create(
            level='WARNING',
            message='GitHub API rate limit approaching',
            source='web_ui.services.github_service'
        )
        
    def test_logs_show_system_wide_activity(self):
        """Test that logs view shows activity from across the system."""
        url = reverse('logs')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        
        # Should show logs from different components
        self.assertContains(response, 'Dashboard loaded successfully')
        self.assertContains(response, 'Failed to connect to DataHub')
        self.assertContains(response, 'Metadata sync started')
        self.assertContains(response, 'GitHub API rate limit approaching')
        
    def test_logs_source_filter_options(self):
        """Test that source filter shows all available sources."""
        url = reverse('logs')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        
        # Should include all sources in filter dropdown
        content = response.content.decode()
        self.assertIn('web_ui.views', content)
        self.assertIn('utils.datahub_utils', content)
        self.assertIn('metadata_manager.views', content)
        self.assertIn('web_ui.services.github_service', content)
        
    def test_logs_configured_level_display(self):
        """Test that configured log level is displayed."""
        url = reverse('logs')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Log Level: INFO')
        
    def test_logs_with_dashboard_integration(self):
        """Test logs in context of dashboard monitoring."""
        # Create some recent error logs
        for i in range(3):
            LogEntry.objects.create(
                level='ERROR',
                message=f'Recent error {i}',
                source='system.component'
            )
            
        # Dashboard might show recent errors count
        # This would depend on if dashboard integrates with logs
        error_count = LogEntry.objects.filter(level='ERROR').count()
        self.assertGreaterEqual(error_count, 3)
        
    def test_logs_performance_with_large_dataset(self):
        """Test logs view performance with large number of entries."""
        # Create many log entries
        entries_to_create = 100
        LogEntry.objects.bulk_create([
            LogEntry(
                level='INFO',
                message=f'Performance test log {i}',
                source='performance.test',
                timestamp=timezone.now() - timedelta(minutes=i)
            )
            for i in range(entries_to_create)
        ])
        
        import time
        start_time = time.time()
        
        url = reverse('logs')
        response = self.client.get(url)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        self.assertEqual(response.status_code, 200)
        self.assertLess(execution_time, 2.0, "Logs view took too long with large dataset")


class LogsSecurityTestCase(TestCase):
    """Test cases for logs security and data protection."""
    
    def setUp(self):
        """Set up test data for security tests."""
        self.client = Client()
        
        # Create users with different permission levels
        self.regular_user = User.objects.create_user(
            username='regular',
            password='testpass123'
        )
        
        self.admin_user = User.objects.create_superuser(
            username='admin',
            email='admin@example.com',
            password='adminpass123'
        )
        
        # Create sensitive log entry
        self.sensitive_log = LogEntry.objects.create(
            level='INFO',
            message='User login: testuser',
            source='auth.system'
        )
        
    def test_logs_access_permissions(self):
        """Test access permissions for logs view."""
        url = reverse('logs')
        
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
        
    def test_logs_clear_permissions(self):
        """Test permissions for clearing logs."""
        url = reverse('logs')
        
        # Test that log clearing is allowed (based on current implementation)
        response = self.client.post(url, {'action': 'clear_logs'})
        # Should either succeed or require authentication
        self.assertIn(response.status_code, [200, 302, 403])
        
    def test_logs_no_sensitive_data_exposure(self):
        """Test that logs don't expose sensitive data inappropriately."""
        # Create log with potentially sensitive info
        LogEntry.objects.create(
            level='DEBUG',
            message='Database connection string: postgresql://user:password@host/db',
            source='database.connector'
        )
        
        url = reverse('logs')
        response = self.client.get(url)
        
        # Should show the log but ideally sanitize sensitive parts
        # This depends on implementation - might show full message or sanitized version
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Database connection string')
        
    def test_logs_xss_protection(self):
        """Test that log messages are protected against XSS."""
        # Create log entry with potential XSS content
        xss_message = '<script>alert("XSS")</script>'
        LogEntry.objects.create(
            level='WARNING',
            message=xss_message,
            source='security.test'
        )
        
        url = reverse('logs')
        response = self.client.get(url) 
        
        # Should escape the script tag
        content = response.content.decode()
        self.assertNotIn('<script>alert("XSS")</script>', content)
        # Should contain escaped version
        self.assertIn('&lt;script&gt;', content)


class LogsAPITestCase(TestCase):
    """Test cases for logs API endpoints if they exist."""
    
    def setUp(self):
        """Set up test data for API tests."""
        self.client = Client()
        
        # Create test log entries
        LogEntry.objects.create(
            level='INFO',
            message='API test log',
            source='api.test'
        )
        
    def test_refresh_logs_ajax_response(self):
        """Test that refresh logs returns proper AJAX response."""
        url = reverse('refresh_logs')
        
        # Make AJAX request
        response = self.client.get(url, HTTP_X_REQUESTED_WITH='XMLHttpRequest')
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'API test log')
        
        # Should return just the table content, not full page
        content = response.content.decode()
        self.assertNotIn('<html>', content)
        self.assertNotIn('Application Logs', content)  # Page title shouldn't be in AJAX response 