# Web UI Test Suite Documentation

This document provides comprehensive information about the test suite for the DataHub Recipes Manager Web UI application.

## Overview

The Web UI test suite is designed to ensure reliability, maintainability, and quality of the web application across all layers - Python backend, HTML templates, JavaScript frontend, and their integrations. It covers multiple testing methodologies including unit tests, integration tests, frontend tests, performance tests, and security tests.

### Test Coverage Areas

- **Python Backend**: Models, views, forms, utilities, and business logic
- **HTML Templates**: Template rendering, inheritance, custom tags, and accessibility  
- **JavaScript Frontend**: UI interactions, AJAX calls, form validation, and user experience
- **Integration**: End-to-end workflows across all application layers
- **Performance**: Load testing, benchmarking, and performance regression detection
- **Security**: Authentication, authorization, input validation, and vulnerability scanning

## Test Structure

### Directory Organization

The test suite is organized into a structured directory hierarchy:

```
web_ui/tests/
├── __init__.py
├── conftest.py                 # Pytest configuration and shared fixtures
├── README_TESTING.md          # This documentation
├── unit/                      # Unit tests for individual components
│   ├── __init__.py
│   ├── test_models.py         # Django model tests
│   ├── test_views.py          # Django view tests
│   └── apps/                  # App-specific unit tests
│       ├── __init__.py
│       ├── test_dashboard.py  # Dashboard functionality
│       ├── test_policies.py   # Policy management
│       ├── test_environments.py # Environment management
│       ├── test_logs.py       # Logging system
│       └── test_git_repository.py # Git/GitHub integration
├── frontend/                  # Frontend and UI tests
│   ├── __init__.py
│   ├── test_templates.py      # HTML template tests
│   └── test_javascript.py     # JavaScript/Selenium tests
├── integration/               # Integration and workflow tests
│   ├── __init__.py
│   └── test_full_workflows.py # End-to-end workflow tests
├── fixtures/                  # Test data and factories
│   ├── __init__.py
│   └── factories.py           # Factory Boy factories for test data
└── utils/                     # Test utilities and helpers
    ├── __init__.py
    └── base_test.py           # Base test classes and utilities
```

### Test Categories

#### Unit Tests (`tests/unit/`)
- **Models** (`test_models.py`): Database model functionality, constraints, and relationships
- **Views** (`test_views.py`): HTTP request/response handling, form processing, and authentication
- **Apps** (`apps/`): Application-specific business logic and features
  - Dashboard functionality and data aggregation
  - Policy CRUD operations and DataHub integration
  - Environment management and configuration
  - Logging system and filtering
  - Git/GitHub integration and repository operations

#### Frontend Tests (`tests/frontend/`)
- **Templates** (`test_templates.py`): HTML rendering, template inheritance, custom tags, and accessibility
- **JavaScript** (`test_javascript.py`): UI interactions, AJAX functionality, and browser automation with Selenium

#### Integration Tests (`tests/integration/`)
- **Full Workflows** (`test_full_workflows.py`): Complete user journeys from frontend to backend
- **Cross-Component**: Testing interactions between different parts of the system

#### Supporting Components
- **Fixtures** (`fixtures/factories.py`): Consistent test data generation using Factory Boy
- **Utils** (`utils/base_test.py`): Shared test utilities, base classes, and helper functions

## Running Tests

### Prerequisites

Ensure you have the required dependencies installed:

```bash
pip install -r requirements-dev.txt
pip install pytest-cov pytest-django pytest-benchmark selenium
pip install beautifulsoup4 factory-boy pytest-mock
```

For Selenium tests, you'll also need:
- Chrome or Firefox browser
- ChromeDriver or GeckoDriver (handled automatically in CI)

### Running All Tests

```bash
# Navigate to web_ui directory
cd web_ui

# Run all tests
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ -v --cov=web_ui --cov-report=html
```

### Running Specific Test Categories

#### Unit Tests
```bash
# All unit tests
python -m pytest tests/unit/ -v

# Specific unit test files
python -m pytest tests/unit/test_models.py -v
python -m pytest tests/unit/test_views.py -v

# App-specific tests
python -m pytest tests/unit/apps/test_dashboard.py -v
python -m pytest tests/unit/apps/test_policies.py -v
```

#### Frontend Tests
```bash
# Template tests
python -m pytest tests/frontend/test_templates.py -v

# JavaScript/Selenium tests (requires browser)
python -m pytest tests/frontend/test_javascript.py -v -m selenium
```

#### Integration Tests
```bash
# All integration tests
python -m pytest tests/integration/ -v

# Specific workflow tests
python -m pytest tests/integration/test_full_workflows.py -v
```

### Running Tests by Markers

```bash
# Performance tests only
python -m pytest tests/ -v -m performance

# Selenium tests only
python -m pytest tests/ -v -m selenium

# Integration tests only
python -m pytest tests/ -v -m integration

# Exclude slow tests
python -m pytest tests/ -v -m "not slow"
```

### Database Setup

#### PostgreSQL (Recommended for CI/Production-like testing)
```bash
# Set environment variable
export DATABASE_URL=postgresql://username:password@localhost:5432/test_webui

# Run migrations
python manage.py migrate --settings=web_ui.settings
```

#### SQLite (Default for local development)
```bash
# Uses in-memory SQLite database automatically
python -m pytest tests/ -v
```

## GitHub Actions Automation

The test suite runs automatically on GitHub Actions for every push and pull request. The workflow includes:

### Matrix Testing
- **Python versions**: 3.9, 3.10, 3.11
- **Django versions**: 4.2, 5.0
- **Database**: PostgreSQL 13

### Test Execution Pipeline
1. **Unit Tests**: Models, views, and app-specific functionality
2. **Frontend Tests**: Template rendering and JavaScript interactions
3. **Integration Tests**: End-to-end workflow validation
4. **Code Coverage**: Comprehensive coverage reporting
5. **Linting**: Code quality checks (black, flake8, mypy, bandit)
6. **Performance**: Benchmark testing for performance regression
7. **Security**: Dependency vulnerability scanning

### Workflow Configuration
The workflow configuration is in `.github/workflows/test-web-ui.yml` and includes:
- Automatic browser setup for Selenium tests
- PostgreSQL service container
- Coverage reporting to Codecov
- Test result artifacts
- Performance benchmarking

## Test Structure and Patterns

### Test Classes

Each test file contains multiple test classes organized by functionality:

```python
class ModelTestCase(TestCase):
    """Tests for model functionality."""
    
class ViewsTestCase(TestCase):
    """Tests for view endpoints."""
    
class IntegrationTestCase(TestCase):
    """Tests for cross-component integration."""
    
class PerformanceTestCase(TestCase):
    """Tests for performance benchmarks."""
    
class SecurityTestCase(TestCase):
    """Tests for security and permissions."""
```

### Base Test Classes

The test suite provides several base classes for consistency:

- `BaseWebUITestCase`: Standard setup with authenticated users and common assertions
- `DatabaseTestMixin`: Database assertion helpers
- `APITestMixin`: API testing utilities
- `PerformanceTestMixin`: Performance testing helpers

### Fixtures and Factories

Test data is generated using Factory Boy factories:

```python
# Create test users
admin_user = UserFactory.create_admin()
regular_user = UserFactory.create_user()

# Create test environments
dev_env = EnvironmentFactory.create_default()
prod_env = EnvironmentFactory(name='production')

# Create test policies
policy = PolicyFactory(name='Test Policy', environment=dev_env)
```

### Mocking Strategy

External dependencies are mocked for fast, reliable testing:

```python
@patch('web_ui.web_ui.views.get_datahub_client_from_request')
def test_dashboard_with_datahub(self, mock_get_client):
    mock_client = MockHelper.mock_datahub_client(connected=True)
    mock_get_client.return_value = mock_client
    
    response = self.client.get(reverse('dashboard_data'))
    self.assertTrue(response.json()['connected'])
```

## Test Categories and Best Practices

### Unit Tests
- **Focus**: Single component functionality
- **Speed**: Fast execution (< 1 second per test)
- **Isolation**: Mock external dependencies
- **Coverage**: High code coverage for business logic

### Integration Tests
- **Focus**: Component interactions and workflows
- **Scope**: Multi-step user scenarios
- **Data**: Realistic test data and scenarios
- **Validation**: End-to-end functionality

### Frontend Tests
- **Templates**: Rendering, context, and accessibility
- **JavaScript**: User interactions and AJAX calls
- **Cross-browser**: Chrome and Firefox compatibility
- **Responsive**: Mobile and desktop layouts

### Performance Tests
- **Benchmarking**: Response time measurements
- **Load Testing**: High-volume data scenarios
- **Regression**: Performance comparison over time
- **Thresholds**: Defined performance expectations

### Security Tests
- **Authentication**: Login and permission checks
- **Authorization**: Access control validation
- **Input Validation**: XSS and injection prevention
- **Vulnerability**: Dependency security scanning

## Writing New Tests

### Test Organization
1. Place tests in the appropriate directory (`unit/`, `frontend/`, `integration/`)
2. Use descriptive test class and method names
3. Group related tests in the same class
4. Follow the AAA pattern (Arrange, Act, Assert)

### Test Data
1. Use Factory Boy factories for consistent test data
2. Create minimal data needed for each test
3. Use fixtures for complex setup scenarios
4. Clean up test data appropriately

### Assertions
1. Use Django's assertion methods (`assertContains`, `assertRedirects`)
2. Use custom assertion helpers from base classes
3. Provide descriptive failure messages
4. Test both positive and negative cases

### Mocking
1. Mock external APIs and services
2. Use `MockHelper` for common mock scenarios
3. Verify mock calls when appropriate
4. Keep mocks simple and focused

### Example Test Structure
```python
class PolicyManagementTestCase(BaseWebUITestCase):
    """Test cases for policy management functionality."""
    
    def setUp(self):
        super().setUp()
        self.environment = EnvironmentFactory()
        self.policy_data = {
            'name': 'Test Policy',
            'description': 'A test policy',
            'policy_json': json.dumps({'type': 'METADATA'}),
            'environment': self.environment.id
        }
    
    @patch('web_ui.web_ui.views.get_datahub_client_from_request')
    def test_policy_creation_success(self, mock_get_client):
        """Test successful policy creation workflow."""
        # Arrange
        mock_client = MockHelper.mock_datahub_client()
        mock_get_client.return_value = mock_client
        
        # Act
        response = self.client.post(reverse('policy_create'), self.policy_data)
        
        # Assert
        self.assertEqual(response.status_code, 302)
        self.assertTrue(Policy.objects.filter(name='Test Policy').exists())
        mock_client.create_policy.assert_called_once()
```

## Troubleshooting

### Common Issues

#### Database Connection Errors
```bash
# Check PostgreSQL is running
pg_isready -h localhost -p 5432

# Verify database URL
echo $DATABASE_URL

# Reset test database
dropdb test_webui && createdb test_webui
```

#### Selenium Test Failures
```bash
# Check browser installation
google-chrome --version
chromedriver --version

# Run with visible browser (debugging)
python -m pytest tests/frontend/test_javascript.py -v -s --no-headless
```

#### Import Errors
```bash
# Check Python path
export PYTHONPATH=/path/to/project

# Verify Django settings
python -c "import django; django.setup()"
```

#### Permission Errors
```bash
# Check file permissions
chmod +x manage.py

# Verify user permissions
whoami
groups
```

### Debugging Tests

#### Verbose Output
```bash
# Detailed test output
python -m pytest tests/ -v -s

# Show local variables on failure
python -m pytest tests/ -v --tb=long

# Stop on first failure
python -m pytest tests/ -v -x
```

#### Interactive Debugging
```python
import pytest

def test_example():
    # Add breakpoint
    pytest.set_trace()
    
    # Test logic here
    assert True
```

#### Log Capture
```python
import logging

def test_with_logs(caplog):
    with caplog.at_level(logging.INFO):
        # Test code that generates logs
        pass
    
    assert "Expected log message" in caplog.text
```

## Configuration

### Django Settings

Test-specific settings are configured in `conftest.py`:

```python
@pytest.fixture(scope='session')
def django_db_setup():
    settings.DATABASES['default'] = {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': ':memory:',
    }
```

### Pytest Configuration

Pytest markers and options are configured in `pytest.ini`:

```ini
[tool:pytest]
DJANGO_SETTINGS_MODULE = web_ui.settings
addopts = --reuse-db --nomigrations
markers =
    integration: Integration tests
    frontend: Frontend tests
    selenium: Selenium tests
    performance: Performance tests
    slow: Slow-running tests
```

### Environment Variables

Key environment variables for testing:

- `DATABASE_URL`: Database connection string
- `DJANGO_SETTINGS_MODULE`: Django settings module
- `SECRET_KEY`: Django secret key for testing
- `DEBUG`: Enable/disable debug mode
- `BROWSER`: Browser choice for Selenium tests

## CI/CD Quality Gates

### Coverage Requirements
- **Minimum Coverage**: 85% overall
- **Critical Components**: 95% coverage for models and views
- **New Code**: 90% coverage for new additions

### Performance Thresholds
- **Response Time**: < 2 seconds for dashboard
- **Database Queries**: < 50 queries per request
- **Memory Usage**: < 100MB per test

### Security Checks
- **Vulnerability Scanning**: Zero high-severity vulnerabilities
- **Code Security**: Clean bandit security scan
- **Dependencies**: Up-to-date and secure packages

### Code Quality
- **Formatting**: Black code formatting compliance
- **Imports**: isort import organization
- **Linting**: flake8 style guide compliance
- **Type Checking**: mypy type checking (warnings allowed)

## Contributing

### Adding New Tests
1. **Identify the appropriate test category** (unit, frontend, integration)
2. **Create tests following existing patterns** and naming conventions
3. **Ensure comprehensive coverage** of the new functionality
4. **Add appropriate mocks** for external dependencies
5. **Update documentation** if adding new test patterns

### Test Review Checklist
- [ ] Tests are in the correct directory
- [ ] Test names are descriptive and clear
- [ ] Tests follow AAA pattern (Arrange, Act, Assert)
- [ ] External dependencies are properly mocked
- [ ] Test data is minimal and focused
- [ ] Both positive and negative cases are covered
- [ ] Performance implications are considered
- [ ] Security aspects are tested where relevant

### Continuous Improvement
- **Monitor test execution time** and optimize slow tests
- **Review test failures** for flaky tests and improve reliability
- **Update test data** to reflect real-world scenarios
- **Enhance test coverage** for edge cases and error conditions
- **Refactor test code** to reduce duplication and improve maintainability

This comprehensive test suite ensures the reliability, performance, and security of the DataHub Recipes Manager Web UI across all layers of the application stack. 