"""
Shared test utilities and helper functions for web_ui test suite.

This module provides common testing utilities, assertion helpers,
and mock setup functions used across all test modules.
"""

import json
import logging
from unittest.mock import Mock, patch
from django.test import TestCase, Client
from django.contrib.auth.models import User
from django.contrib.messages import get_messages
from django.urls import reverse
from django.utils import timezone
from contextlib import contextmanager
from typing import Dict, Any, List, Optional


class BaseWebUITestCase(TestCase):
    """Base test case class with common setup and utilities."""
    
    def setUp(self):
        """Common setup for all web UI tests."""
        self.client = Client()
        self.maxDiff = None  # Show full diffs in test failures
        
        # Create standard test users
        self.regular_user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        
        self.admin_user = User.objects.create_superuser(
            username='admin',
            email='admin@example.com',
            password='adminpass123'
        )
        
    def login_regular_user(self):
        """Login as regular user."""
        self.client.login(username='testuser', password='testpass123')
        
    def login_admin_user(self):
        """Login as admin user."""
        self.client.login(username='admin', password='adminpass123')
        
    def assertMessagesContain(self, response, message_text, level=None):
        """Assert that response contains a specific message."""
        messages = list(get_messages(response.wsgi_request))
        message_strings = [str(message) for message in messages]
        
        found = any(message_text in msg for msg in message_strings)
        self.assertTrue(
            found, 
            f"Message '{message_text}' not found in messages: {message_strings}"
        )
        
        if level:
            level_found = any(
                message.level_tag == level and message_text in str(message) 
                for message in messages
            )
            self.assertTrue(
                level_found,
                f"Message '{message_text}' with level '{level}' not found"
            )
            
    def assertNoMessages(self, response):
        """Assert that response has no messages."""
        messages = list(get_messages(response.wsgi_request))
        self.assertEqual(
            len(messages), 0,
            f"Expected no messages, but found: {[str(msg) for msg in messages]}"
        )
        
    def assertJSONResponse(self, response, expected_status=200):
        """Assert response is valid JSON with expected status."""
        self.assertEqual(response.status_code, expected_status)
        self.assertEqual(response['Content-Type'], 'application/json')
        
        try:
            return json.loads(response.content)
        except json.JSONDecodeError as e:
            self.fail(f"Response is not valid JSON: {e}")
            
    def assertHTMLResponse(self, response, expected_status=200):
        """Assert response is HTML with expected status."""
        self.assertEqual(response.status_code, expected_status)
        self.assertIn('text/html', response['Content-Type'])
        
    def assertRedirectsToUrl(self, response, expected_url):
        """Assert response redirects to specific URL."""
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, expected_url)
        
    def get_json_response(self, url, method='GET', data=None, **kwargs):
        """Make request and return parsed JSON response."""
        if method.upper() == 'GET':
            response = self.client.get(url, data or {}, **kwargs)
        elif method.upper() == 'POST':
            response = self.client.post(url, data or {}, **kwargs)
        else:
            raise ValueError(f"Unsupported method: {method}")
            
        return self.assertJSONResponse(response)


class MockHelper:
    """Helper class for creating common mocks."""
    
    @staticmethod
    def mock_datahub_client(connected=True, recipes=None, policies=None):
        """Create a mock DataHub client."""
        mock_client = Mock()
        mock_client.test_connection.return_value = connected
        
        if recipes is not None:
            mock_client.list_ingestion_sources.return_value = recipes
        else:
            mock_client.list_ingestion_sources.return_value = []
            
        if policies is not None:
            mock_client.list_policies.return_value = policies
        else:
            mock_client.list_policies.return_value = []
            
        # Add other common methods
        mock_client.create_policy.return_value = {'success': True}
        mock_client.update_policy.return_value = {'success': True}
        mock_client.delete_policy.return_value = {'success': True}
        
        return mock_client
        
    @staticmethod
    def mock_github_service(prs=None, create_pr_result=None):
        """Create a mock GitHub service."""
        mock_service = Mock()
        
        if prs is not None:
            mock_service.list_pull_requests.return_value = prs
        else:
            mock_service.list_pull_requests.return_value = []
            
        if create_pr_result is not None:
            mock_service.create_pull_request.return_value = create_pr_result
        else:
            mock_service.create_pull_request.return_value = {
                'number': 123,
                'html_url': 'https://github.com/user/repo/pull/123'
            }
            
        mock_service.create_or_update_file.return_value = True
        
        return mock_service
        
    @staticmethod
    def mock_git_service():
        """Create a mock Git service."""
        mock_service = Mock()
        mock_service.commit_and_push.return_value = True
        mock_service.stage_file.return_value = True
        mock_service.get_branches.return_value = ['main', 'develop', 'feature/test']
        
        return mock_service


class DatabaseTestMixin:
    """Mixin for database-related test utilities."""
    
    def assertDatabaseCount(self, model_class, expected_count):
        """Assert database contains expected number of model instances."""
        actual_count = model_class.objects.count()
        self.assertEqual(
            actual_count, expected_count,
            f"Expected {expected_count} {model_class.__name__} instances, "
            f"but found {actual_count}"
        )
        
    def assertModelExists(self, model_class, **kwargs):
        """Assert model instance exists with given criteria."""
        try:
            obj = model_class.objects.get(**kwargs)
            return obj
        except model_class.DoesNotExist:
            self.fail(f"{model_class.__name__} with {kwargs} does not exist")
        except model_class.MultipleObjectsReturned:
            self.fail(f"Multiple {model_class.__name__} instances found with {kwargs}")
            
    def assertModelDoesNotExist(self, model_class, **kwargs):
        """Assert model instance does not exist with given criteria."""
        exists = model_class.objects.filter(**kwargs).exists()
        self.assertFalse(
            exists,
            f"{model_class.__name__} with {kwargs} should not exist"
        )


class LogCapture:
    """Context manager for capturing log output during tests."""
    
    def __init__(self, logger_name='', level=logging.DEBUG):
        self.logger_name = logger_name
        self.level = level
        self.handler = None
        self.logs = []
        
    def __enter__(self):
        # Create custom handler to capture logs
        self.handler = logging.Handler()
        self.handler.setLevel(self.level)
        
        # Override emit method to capture logs
        original_emit = self.handler.emit
        def capture_emit(record):
            self.logs.append(record)
            return original_emit(record)
        self.handler.emit = capture_emit
        
        # Add handler to logger
        logger = logging.getLogger(self.logger_name)
        logger.addHandler(self.handler)
        logger.setLevel(self.level)
        
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Remove handler
        logger = logging.getLogger(self.logger_name)
        logger.removeHandler(self.handler)
        
    def has_log(self, message, level=None):
        """Check if logs contain a specific message."""
        for log_record in self.logs:
            if message in log_record.getMessage():
                if level is None or log_record.levelno == level:
                    return True
        return False
        
    def get_logs(self, level=None):
        """Get all captured logs, optionally filtered by level."""
        if level is None:
            return self.logs
        return [log for log in self.logs if log.levelno == level]


@contextmanager
def temporary_setting(setting_name, value):
    """Temporarily change a Django setting for testing."""
    from django.conf import settings
    
    original_value = getattr(settings, setting_name, None)
    setattr(settings, setting_name, value)
    
    try:
        yield
    finally:
        if original_value is not None:
            setattr(settings, setting_name, original_value)
        else:
            delattr(settings, setting_name)


@contextmanager
def mock_datahub_connection(connected=True, client_data=None):
    """Context manager for mocking DataHub connection."""
    mock_client = MockHelper.mock_datahub_client(
        connected=connected,
        recipes=client_data.get('recipes') if client_data else None,
        policies=client_data.get('policies') if client_data else None
    )
    
    with patch('web_ui.web_ui.views.get_datahub_client_from_request') as mock_get_client:
        mock_get_client.return_value = mock_client if connected else None
        yield mock_client


def create_test_policy_data(name="Test Policy", policy_type="METADATA", state="ACTIVE"):
    """Create realistic test policy data."""
    return {
        "name": name,
        "description": f"Test policy: {name}",
        "type": policy_type,
        "state": state,
        "resources": {
            "filter": {
                "criteria": []
            }
        },
        "privileges": ["EDIT_ENTITY_OWNERS", "VIEW_ENTITY_PAGE"],
        "actors": {
            "users": ["urn:li:corpuser:testuser"],
            "groups": []
        }
    }


def create_test_recipe_data(source_type="postgres", name="test-recipe"):
    """Create realistic test recipe data."""
    return {
        "source": {
            "type": source_type,
            "config": {
                "host_port": "localhost:5432",
                "database": "testdb",
                "username": "testuser",
                "password": "${POSTGRES_PASSWORD}"
            }
        },
        "sink": {
            "type": "datahub-rest",
            "config": {
                "server": "http://localhost:8080"
            }
        },
        "name": name
    }


def assert_form_errors(test_case, response, field_name=None):
    """Assert that form has errors and optionally check specific field."""
    test_case.assertContains(response, 'error')
    
    if field_name:
        test_case.assertContains(response, f'name="{field_name}"')


def assert_success_message(test_case, response, message_text):
    """Assert that response contains a success message."""
    messages = list(get_messages(response.wsgi_request))
    success_messages = [
        str(msg) for msg in messages 
        if msg.level_tag == 'success'
    ]
    
    found = any(message_text in msg for msg in success_messages)
    test_case.assertTrue(
        found,
        f"Success message '{message_text}' not found in: {success_messages}"
    )


def assert_error_message(test_case, response, message_text):
    """Assert that response contains an error message."""
    messages = list(get_messages(response.wsgi_request))
    error_messages = [
        str(msg) for msg in messages 
        if msg.level_tag == 'error'
    ]
    
    found = any(message_text in msg for msg in error_messages)
    test_case.assertTrue(
        found,
        f"Error message '{message_text}' not found in: {error_messages}"
    )


class PerformanceTestMixin:
    """Mixin for performance testing utilities."""
    
    def assertExecutionTime(self, max_seconds=2.0):
        """Context manager to assert execution time is within limit."""
        return ExecutionTimeAssertion(self, max_seconds)


class ExecutionTimeAssertion:
    """Context manager for asserting execution time."""
    
    def __init__(self, test_case, max_seconds):
        self.test_case = test_case
        self.max_seconds = max_seconds
        self.start_time = None
        
    def __enter__(self):
        import time
        self.start_time = time.time()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        import time
        execution_time = time.time() - self.start_time
        self.test_case.assertLess(
            execution_time, self.max_seconds,
            f"Execution took {execution_time:.2f}s, expected < {self.max_seconds}s"
        )


def skip_if_no_network(test_func):
    """Decorator to skip tests if network is not available."""
    import unittest
    import socket
    
    def check_network():
        try:
            socket.create_connection(("8.8.8.8", 53), timeout=3)
            return True
        except OSError:
            return False
    
    return unittest.skipUnless(check_network(), "Network not available")(test_func)


def create_temporary_file(content, suffix=".json"):
    """Create a temporary file with given content for testing."""
    import tempfile
    import os
    
    fd, path = tempfile.mkstemp(suffix=suffix)
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(content)
        return path
    except:
        os.unlink(path)
        raise


class APITestMixin:
    """Mixin for API testing utilities."""
    
    def api_get(self, url, data=None, **kwargs):
        """Make GET request to API endpoint."""
        return self.client.get(url, data or {}, **kwargs)
        
    def api_post(self, url, data=None, **kwargs):
        """Make POST request to API endpoint."""
        return self.client.post(
            url, 
            json.dumps(data or {}),
            content_type='application/json',
            **kwargs
        )
        
    def assert_api_success(self, response, expected_status=200):
        """Assert API response is successful."""
        self.assertEqual(response.status_code, expected_status)
        
        if 'application/json' in response.get('Content-Type', ''):
            data = json.loads(response.content)
            return data
        return None
        
    def assert_api_error(self, response, expected_status=400):
        """Assert API response is an error."""
        self.assertGreaterEqual(response.status_code, expected_status)
        
        if 'application/json' in response.get('Content-Type', ''):
            data = json.loads(response.content)
            self.assertIn('error', data)
            return data
        return None 