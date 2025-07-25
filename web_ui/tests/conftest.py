"""
Pytest configuration and shared fixtures for web_ui tests.

Provides centralized configuration and commonly used fixtures for the test suite.
"""

import os
import pytest
from django.conf import settings
from django.test import TestCase, Client
from django.contrib.auth.models import User
from unittest.mock import Mock, patch

# Ensure Django is properly configured for testing
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'web_ui.settings')

# Pytest configuration
pytest_plugins = ['django']

def pytest_configure(config):
    """Configure pytest with Django settings."""
    if not settings.configured:
        settings.configure(
            DATABASES={
                'default': {
                    'ENGINE': 'django.db.backends.sqlite3',
                    'NAME': ':memory:',
                }
            },
            INSTALLED_APPS=[
                'django.contrib.auth',
                'django.contrib.contenttypes',
                'web_ui',
                'tests',
            ],
            SECRET_KEY='test-secret-key-for-testing-only',
            DEBUG=True,
            USE_TZ=True,
        )


# Pytest markers
def pytest_configure_node(node):
    """Configure custom pytest markers."""
    # Register custom markers to avoid warnings
    markers = [
        'slow: marks tests as slow running',
        'integration: marks tests as integration tests',
        'frontend: marks tests as frontend/UI tests', 
        'selenium: marks tests that require browser automation',
        'performance: marks tests as performance benchmarks',
        'network: marks tests that require network connectivity',
        'unit: marks tests as unit tests',
        'javascript: marks tests for JavaScript functionality',
        'html: marks tests for HTML template functionality',
        'python: marks tests for Python view functionality',
        'pages: marks tests for complete page functionality',
        'auth: marks tests for authentication functionality',
        'api: marks tests for API endpoints',
        'database: marks tests that require database access',
        'mock: marks tests that use extensive mocking',
    ]
    
    for marker in markers:
        node.config.addinivalue_line("markers", marker)


# Basic fixtures
@pytest.fixture
def client():
    """Django test client."""
    return Client()


@pytest.fixture
def admin_user(db):
    """Create an admin user for testing."""
    return User.objects.create_user(
        username='admin',
        email='admin@test.com',
        password='testpass123',
        is_staff=True,
        is_superuser=True
    )


@pytest.fixture
def regular_user(db):
    """Create a regular user for testing."""
    return User.objects.create_user(
        username='testuser',
        email='testuser@test.com', 
        password='testpass123'
    )


@pytest.fixture
def authenticated_client(client, regular_user):
    """Client logged in as regular user."""
    client.force_login(regular_user)
    return client


@pytest.fixture
def admin_client(client, admin_user):
    """Client logged in as admin user."""
    client.force_login(admin_user)
    return client


# Mock fixtures
@pytest.fixture
def mock_datahub_client():
    """Mock DataHub client for testing."""
    mock_client = Mock()
    mock_client.test_connection.return_value = True
    mock_client.get_users_and_groups.return_value = {'users': [], 'groups': []}
    mock_client.list_policies.return_value = []
    mock_client.list_domains.return_value = []
    return mock_client


@pytest.fixture
def mock_github_service():
    """Mock GitHub service for testing."""
    mock_service = Mock()
    mock_service.test_connection.return_value = {'status': 'success'}
    mock_service.list_branches.return_value = ['main', 'develop']
    mock_service.list_pull_requests.return_value = []
    return mock_service


@pytest.fixture
def mock_git_service():
    """Mock Git service for testing."""
    mock_service = Mock()
    mock_service.get_current_branch.return_value = 'main'
    mock_service.get_status.return_value = {'clean': True}
    return mock_service


# Data fixtures - only create if models are available
@pytest.fixture
def test_environment(db):
    """Create a test environment if Environment model exists."""
    try:
        from web_ui.web_ui.models import Environment
        return Environment.objects.create(
            name='test-env',
            description='Test environment',
            datahub_host='http://localhost:8080',
            datahub_token='test-token',
            is_default=True
        )
    except ImportError:
        return None


@pytest.fixture  
def test_policy(db, test_environment):
    """Create a test policy if Policy model exists."""
    if not test_environment:
        return None
        
    try:
        from web_ui.web_ui.models import Policy
        import json
        return Policy.objects.create(
            name='test-policy',
            description='Test policy',
            policy_json=json.dumps({'type': 'METADATA', 'state': 'ACTIVE'}),
            environment=test_environment
        )
    except ImportError:
        return None


@pytest.fixture
def test_log_entry(db, regular_user):
    """Create a test log entry if LogEntry model exists."""
    try:
        from web_ui.web_ui.models import LogEntry
        return LogEntry.objects.create(
            level='INFO',
            message='Test log message',
            logger_name='test',
            user=regular_user
        )
    except ImportError:
        return None


# Selenium fixtures (only if selenium is available)
@pytest.fixture(scope='session')
def browser_driver():
    """Browser driver type for Selenium tests."""
    return os.environ.get('BROWSER', 'chrome')


@pytest.fixture
def chrome_options():
    """Chrome options for headless testing."""
    try:
        from selenium.webdriver.chrome.options import Options
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        return options
    except ImportError:
        return None


@pytest.fixture
def firefox_options():
    """Firefox options for headless testing."""
    try:
        from selenium.webdriver.firefox.options import Options
        options = Options()
        options.add_argument('--headless')
        return options
    except ImportError:
        return None


@pytest.fixture
def browser(browser_driver, chrome_options, firefox_options):
    """Create a browser instance for Selenium tests."""
    try:
        from selenium import webdriver
        
        if browser_driver == 'chrome' and chrome_options:
            driver = webdriver.Chrome(options=chrome_options)
        elif browser_driver == 'firefox' and firefox_options:
            driver = webdriver.Firefox(options=firefox_options)
        else:
            pytest.skip("Selenium webdriver not available")
        
        yield driver
        driver.quit()
    except ImportError:
        pytest.skip("Selenium not available")


# Utility fixtures
@pytest.fixture
def temp_media_root(tmp_path):
    """Temporary media root for file uploads."""
    with patch('django.conf.settings.MEDIA_ROOT', str(tmp_path)):
        yield str(tmp_path)


@pytest.fixture
def mock_datahub_connection():
    """Mock DataHub connection for views."""
    with patch('web_ui.web_ui.views.get_datahub_client_from_request') as mock:
        mock_client = Mock()
        mock_client.test_connection.return_value = True
        mock.return_value = mock_client
        yield mock_client 