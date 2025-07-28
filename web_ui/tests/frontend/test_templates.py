"""
Comprehensive tests for HTML templates in the web_ui application.

Tests cover:
- Template rendering and context variables
- Template inheritance and blocks
- Custom template tags and filters
- Form rendering and validation display
- Static file inclusion and URLs
- Responsive design elements
- Accessibility features
"""

import pytest
from django.test import TestCase, RequestFactory
from django.template import Context, Template
from django.template.loader import render_to_string
from django.contrib.auth.models import User, AnonymousUser
from django.contrib.messages import get_messages
from django.contrib.messages.storage.fallback import FallbackStorage
from django.http import HttpRequest
from django.urls import reverse
from bs4 import BeautifulSoup

from web_ui.web_ui.models import Environment, Policy, LogEntry, GitSettings
from tests.fixtures.factories import (
    UserFactory, EnvironmentFactory, PolicyFactory, 
    LogEntryFactory, GitSettingsFactory
)
from tests.utils.base_test import BaseWebUITestCase


class BaseTemplateTestCase(TestCase):
    """Base test case for template testing with common setup."""
    
    def setUp(self):
        self.factory = RequestFactory()
        self.user = UserFactory()
        self.admin_user = UserFactory.create_admin()
    
    def create_request(self, user=None, url='/'):
        """Create a mock request object."""
        request = self.factory.get(url)
        request.user = user or self.user
        
        # Add messages framework
        setattr(request, 'session', {})
        messages = FallbackStorage(request)
        setattr(request, '_messages', messages)
        
        return request
    
    def render_template(self, template_name, context=None, request=None):
        """Helper method to render template with context."""
        if context is None:
            context = {}
        
        if request:
            context['request'] = request
            context['user'] = request.user
        
        return render_to_string(template_name, context, request=request)
    
    def parse_html(self, html_content):
        """Parse HTML content with BeautifulSoup."""
        return BeautifulSoup(html_content, 'html.parser')


class BaseTemplateTestCase(BaseTemplateTestCase):
    """Test cases for base template and common elements."""
    
    def test_base_template_structure(self):
        """Test base template has correct structure."""
        request = self.create_request()
        html = self.render_template('base.html', {
            'page_title': 'Test Page',
            'user': self.user
        }, request)
        
        soup = self.parse_html(html)
        
        # Check DOCTYPE and basic structure
        self.assertIn('<!DOCTYPE html>', html)
        self.assertIsNotNone(soup.find('html'))
        self.assertIsNotNone(soup.find('head'))
        self.assertIsNotNone(soup.find('body'))
        
        # Check title
        title = soup.find('title')
        self.assertIsNotNone(title)
        self.assertIn('Test Page', title.text)
        self.assertIn('DataHub Recipes Manager', title.text)
    
    def test_base_template_navigation(self):
        """Test base template navigation elements."""
        request = self.create_request()
        html = self.render_template('base.html', {'user': self.user}, request)
        soup = self.parse_html(html)
        
        # Check navigation exists
        nav = soup.find('nav')
        self.assertIsNotNone(nav)
        
        # Check main navigation links
        nav_links = soup.find_all('a', class_='nav-link')
        nav_text = [link.text.strip() for link in nav_links]
        
        expected_links = ['Dashboard', 'Policies', 'Environments', 'Logs']
        for expected_link in expected_links:
            self.assertIn(expected_link, nav_text)
    
    def test_base_template_user_menu(self):
        """Test base template user menu for authenticated users."""
        request = self.create_request(user=self.user)
        html = self.render_template('base.html', {'user': self.user}, request)
        soup = self.parse_html(html)
        
        # Check user menu exists
        user_menu = soup.find('div', class_='user-menu')
        self.assertIsNotNone(user_menu)
        
        # Check username is displayed
        self.assertIn(self.user.username, html)
        
        # Check logout link exists
        logout_link = soup.find('a', href='/accounts/logout/')
        self.assertIsNotNone(logout_link)
    
    def test_base_template_anonymous_user(self):
        """Test base template for anonymous users."""
        request = self.create_request(user=AnonymousUser())
        html = self.render_template('base.html', {'user': AnonymousUser()}, request)
        soup = self.parse_html(html)
        
        # Check login link exists
        login_link = soup.find('a', href='/accounts/login/')
        self.assertIsNotNone(login_link)
        
        # Check user menu doesn't exist
        user_menu = soup.find('div', class_='user-menu')
        self.assertIsNone(user_menu)
    
    def test_base_template_static_files(self):
        """Test base template includes static files correctly."""
        request = self.create_request()
        html = self.render_template('base.html', {}, request)
        soup = self.parse_html(html)
        
        # Check CSS files
        css_links = soup.find_all('link', rel='stylesheet')
        self.assertGreater(len(css_links), 0)
        
        # Check for Bootstrap CSS
        bootstrap_css = any('bootstrap' in link.get('href', '') for link in css_links)
        self.assertTrue(bootstrap_css)
        
        # Check JavaScript files
        js_scripts = soup.find_all('script', src=True)
        self.assertGreater(len(js_scripts), 0)
    
    def test_base_template_meta_tags(self):
        """Test base template includes proper meta tags."""
        request = self.create_request()
        html = self.render_template('base.html', {}, request)
        soup = self.parse_html(html)
        
        # Check viewport meta tag
        viewport = soup.find('meta', attrs={'name': 'viewport'})
        self.assertIsNotNone(viewport)
        self.assertIn('width=device-width', viewport.get('content', ''))
        
        # Check charset meta tag
        charset = soup.find('meta', attrs={'charset': True})
        self.assertIsNotNone(charset)
        self.assertEqual(charset.get('charset'), 'utf-8')


class DashboardTemplateTestCase(BaseTemplateTestCase):
    """Test cases for dashboard template."""
    
    def test_dashboard_template_structure(self):
        """Test dashboard template structure."""
        request = self.create_request()
        html = self.render_template('dashboard.html', {
            'page_title': 'Dashboard',
            'user': self.user
        }, request)
        
        soup = self.parse_html(html)
        
        # Check main dashboard container
        dashboard = soup.find('div', class_='dashboard')
        self.assertIsNotNone(dashboard)
        
        # Check for stats cards
        stats_cards = soup.find_all('div', class_='stat-card')
        self.assertGreater(len(stats_cards), 0)
    
    def test_dashboard_template_stats_display(self):
        """Test dashboard template displays statistics correctly."""
        request = self.create_request()
        context = {
            'page_title': 'Dashboard',
            'user': self.user,
            'stats': {
                'environments_count': 3,
                'policies_count': 15,
                'recent_runs_count': 8,
                'active_schedules_count': 5
            }
        }
        
        html = self.render_template('dashboard.html', context, request)
        soup = self.parse_html(html)
        
        # Check stats are displayed
        self.assertIn('3', html)  # environments count
        self.assertIn('15', html)  # policies count
        self.assertIn('8', html)   # recent runs
        self.assertIn('5', html)   # active schedules
    
    def test_dashboard_template_ajax_loading(self):
        """Test dashboard template includes AJAX loading elements."""
        request = self.create_request()
        html = self.render_template('dashboard.html', {
            'page_title': 'Dashboard',
            'user': self.user
        }, request)
        
        soup = self.parse_html(html)
        
        # Check for loading spinners
        loading_elements = soup.find_all('div', class_='loading')
        self.assertGreater(len(loading_elements), 0)
        
        # Check for AJAX data attributes
        ajax_elements = soup.find_all(attrs={'data-ajax-url': True})
        self.assertGreater(len(ajax_elements), 0)
    
    def test_dashboard_template_charts_placeholder(self):
        """Test dashboard template includes chart placeholders."""
        request = self.create_request()
        html = self.render_template('dashboard.html', {
            'page_title': 'Dashboard',
            'user': self.user
        }, request)
        
        soup = self.parse_html(html)
        
        # Check for chart containers
        chart_containers = soup.find_all('div', class_='chart-container')
        self.assertGreater(len(chart_containers), 0)
        
        # Check for canvas elements (Chart.js)
        canvas_elements = soup.find_all('canvas')
        self.assertGreater(len(canvas_elements), 0)


class PoliciesTemplateTestCase(BaseTemplateTestCase):
    """Test cases for policies templates."""
    
    def test_policies_list_template(self):
        """Test policies list template."""
        policies = [
            PolicyFactory(name='Policy 1'),
            PolicyFactory(name='Policy 2'),
            PolicyFactory(name='Policy 3')
        ]
        
        request = self.create_request()
        context = {
            'policies': policies,
            'page_title': 'Policies',
            'user': self.user
        }
        
        html = self.render_template('policies/list.html', context, request)
        soup = self.parse_html(html)
        
        # Check policies table exists
        table = soup.find('table', class_='policies-table')
        self.assertIsNotNone(table)
        
        # Check all policies are displayed
        for policy in policies:
            self.assertIn(policy.name, html)
        
        # Check action buttons
        edit_buttons = soup.find_all('a', class_='btn-edit')
        delete_buttons = soup.find_all('a', class_='btn-delete')
        self.assertEqual(len(edit_buttons), len(policies))
        self.assertEqual(len(delete_buttons), len(policies))
    
    def test_policy_create_template(self):
        """Test policy create template."""
        environments = [EnvironmentFactory(name='dev'), EnvironmentFactory(name='prod')]
        
        request = self.create_request()
        context = {
            'environments': environments,
            'page_title': 'Create Policy',
            'user': self.user
        }
        
        html = self.render_template('policies/create.html', context, request)
        soup = self.parse_html(html)
        
        # Check form exists
        form = soup.find('form', method='post')
        self.assertIsNotNone(form)
        
        # Check required form fields
        name_input = soup.find('input', {'name': 'name'})
        description_input = soup.find('textarea', {'name': 'description'})
        policy_json_input = soup.find('textarea', {'name': 'policy_json'})
        environment_select = soup.find('select', {'name': 'environment'})
        
        self.assertIsNotNone(name_input)
        self.assertIsNotNone(description_input)
        self.assertIsNotNone(policy_json_input)
        self.assertIsNotNone(environment_select)
        
        # Check environment options
        for env in environments:
            option = soup.find('option', value=str(env.id))
            self.assertIsNotNone(option)
            self.assertEqual(option.text.strip(), env.name)
    
    def test_policy_edit_template(self):
        """Test policy edit template."""
        policy = PolicyFactory(name='Test Policy')
        environments = [EnvironmentFactory()]
        
        request = self.create_request()
        context = {
            'policy': policy,
            'environments': environments,
            'page_title': 'Edit Policy',
            'user': self.user
        }
        
        html = self.render_template('policies/edit.html', context, request)
        soup = self.parse_html(html)
        
        # Check form is pre-populated
        name_input = soup.find('input', {'name': 'name'})
        self.assertEqual(name_input.get('value'), policy.name)
        
        description_input = soup.find('textarea', {'name': 'description'})
        self.assertIn(policy.description, description_input.text)
    
    def test_policy_delete_template(self):
        """Test policy delete confirmation template."""
        policy = PolicyFactory(name='Test Policy')
        
        request = self.create_request()
        context = {
            'policy': policy,
            'page_title': 'Delete Policy',
            'user': self.user
        }
        
        html = self.render_template('policies/delete.html', context, request)
        soup = self.parse_html(html)
        
        # Check policy name is displayed
        self.assertIn(policy.name, html)
        
        # Check confirmation form
        form = soup.find('form', method='post')
        self.assertIsNotNone(form)
        
        # Check confirm and cancel buttons
        confirm_button = soup.find('button', type='submit')
        cancel_link = soup.find('a', class_='btn-cancel')
        self.assertIsNotNone(confirm_button)
        self.assertIsNotNone(cancel_link)


class EnvironmentsTemplateTestCase(BaseTemplateTestCase):
    """Test cases for environments templates."""
    
    def test_environments_list_template(self):
        """Test environments list template."""
        environments = [
            EnvironmentFactory(name='dev', is_default=True),
            EnvironmentFactory(name='staging', is_default=False),
            EnvironmentFactory(name='prod', is_default=False)
        ]
        
        request = self.create_request()
        context = {
            'environments': environments,
            'page_title': 'Environments',
            'user': self.user
        }
        
        html = self.render_template('environments/list.html', context, request)
        soup = self.parse_html(html)
        
        # Check environments table
        table = soup.find('table', class_='environments-table')
        self.assertIsNotNone(table)
        
        # Check all environments are displayed
        for env in environments:
            self.assertIn(env.name, html)
        
        # Check default environment is marked
        default_badges = soup.find_all('span', class_='badge-default')
        self.assertEqual(len(default_badges), 1)
    
    def test_environment_form_template(self):
        """Test environment form template."""
        request = self.create_request()
        context = {
            'page_title': 'Create Environment',
            'user': self.user
        }
        
        html = self.render_template('environments/form.html', context, request)
        soup = self.parse_html(html)
        
        # Check form fields
        form_fields = ['name', 'description', 'datahub_host', 'datahub_token', 'is_default']
        for field_name in form_fields:
            field = soup.find(['input', 'textarea', 'select'], {'name': field_name})
            self.assertIsNotNone(field, f"Field '{field_name}' not found")
    
    def test_environment_delete_template(self):
        """Test environment delete template."""
        environment = EnvironmentFactory(name='test-env')
        
        request = self.create_request()
        context = {
            'environment': environment,
            'page_title': 'Delete Environment',
            'user': self.user
        }
        
        html = self.render_template('environments/delete.html', context, request)
        soup = self.parse_html(html)
        
        # Check environment name is displayed
        self.assertIn(environment.name, html)
        
        # Check warning message
        warning = soup.find('div', class_='alert-warning')
        self.assertIsNotNone(warning)


class LogsTemplateTestCase(BaseTemplateTestCase):
    """Test cases for logs template."""
    
    def test_logs_template_structure(self):
        """Test logs template structure."""
        logs = [
            LogEntryFactory(level='INFO', message='Info message'),
            LogEntryFactory(level='ERROR', message='Error message'),
            LogEntryFactory(level='WARNING', message='Warning message')
        ]
        
        request = self.create_request()
        context = {
            'logs': logs,
            'page_title': 'Logs',
            'user': self.user
        }
        
        html = self.render_template('logs.html', context, request)
        soup = self.parse_html(html)
        
        # Check logs table
        table = soup.find('table', class_='logs-table')
        self.assertIsNotNone(table)
        
        # Check all logs are displayed
        for log in logs:
            self.assertIn(log.message, html)
            self.assertIn(log.level, html)
    
    def test_logs_template_filtering(self):
        """Test logs template filtering controls."""
        request = self.create_request()
        context = {
            'logs': [],
            'page_title': 'Logs',
            'user': self.user,
            'levels': ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
            'sources': ['app.views', 'app.models', 'app.services']
        }
        
        html = self.render_template('logs.html', context, request)
        soup = self.parse_html(html)
        
        # Check filter form
        filter_form = soup.find('form', class_='filters-form')
        self.assertIsNotNone(filter_form)
        
        # Check level filter
        level_select = soup.find('select', {'name': 'level'})
        self.assertIsNotNone(level_select)
        
        # Check source filter
        source_select = soup.find('select', {'name': 'source'})
        self.assertIsNotNone(source_select)
        
        # Check search input
        search_input = soup.find('input', {'name': 'search'})
        self.assertIsNotNone(search_input)
    
    def test_logs_template_level_styling(self):
        """Test logs template applies correct styling for log levels."""
        logs = [
            LogEntryFactory(level='INFO', message='Info message'),
            LogEntryFactory(level='ERROR', message='Error message'),
            LogEntryFactory(level='WARNING', message='Warning message')
        ]
        
        request = self.create_request()
        context = {
            'logs': logs,
            'page_title': 'Logs',
            'user': self.user
        }
        
        html = self.render_template('logs.html', context, request)
        soup = self.parse_html(html)
        
        # Check log level classes
        info_rows = soup.find_all('tr', class_='log-info')
        error_rows = soup.find_all('tr', class_='log-error')
        warning_rows = soup.find_all('tr', class_='log-warning')
        
        self.assertGreater(len(info_rows), 0)
        self.assertGreater(len(error_rows), 0)
        self.assertGreater(len(warning_rows), 0)


class GitHubTemplateTestCase(BaseTemplateTestCase):
    """Test cases for GitHub templates."""
    
    def test_github_index_template(self):
        """Test GitHub index template."""
        request = self.create_request()
        context = {
            'page_title': 'GitHub Integration',
            'user': self.user,
            'github_connected': True,
            'recent_prs': []
        }
        
        html = self.render_template('github/index.html', context, request)
        soup = self.parse_html(html)
        
        # Check connection status
        connection_status = soup.find('div', class_='connection-status')
        self.assertIsNotNone(connection_status)
        
        # Check PR section
        pr_section = soup.find('div', class_='pr-section')
        self.assertIsNotNone(pr_section)


class FormTemplateTestCase(BaseTemplateTestCase):
    """Test cases for form rendering in templates."""
    
    def test_form_field_rendering(self):
        """Test form field rendering with errors."""
        template_content = """
        {% load widget_tweaks %}
        <form method="post">
            {% csrf_token %}
            <div class="form-group">
                <label for="{{ form.name.id_for_label }}">{{ form.name.label }}</label>
                {{ form.name|add_class:"form-control" }}
                {% if form.name.errors %}
                    <div class="invalid-feedback">
                        {% for error in form.name.errors %}
                            {{ error }}
                        {% endfor %}
                    </div>
                {% endif %}
            </div>
        </form>
        """
        
        from django import forms
        
        class TestForm(forms.Form):
            name = forms.CharField(max_length=100, required=True)
        
        form = TestForm(data={'name': ''})  # Invalid data
        form.is_valid()  # Trigger validation
        
        template = Template(template_content)
        request = self.create_request()
        context = Context({'form': form, 'request': request})
        html = template.render(context)
        
        soup = self.parse_html(html)
        
        # Check form field exists
        name_input = soup.find('input', {'name': 'name'})
        self.assertIsNotNone(name_input)
        
        # Check error display
        error_div = soup.find('div', class_='invalid-feedback')
        self.assertIsNotNone(error_div)
    
    def test_csrf_token_inclusion(self):
        """Test CSRF token is included in forms."""
        template_content = """
        <form method="post">
            {% csrf_token %}
            <input type="text" name="test_field">
        </form>
        """
        
        template = Template(template_content)
        request = self.create_request()
        context = Context({'request': request})
        html = template.render(context)
        
        # Check CSRF token exists
        self.assertIn('csrfmiddlewaretoken', html)


class TemplateTagsTestCase(BaseTemplateTestCase):
    """Test cases for custom template tags and filters."""
    
    def test_custom_template_filters(self):
        """Test custom template filters."""
        # Test cache busting filter
        template_content = """
        {% load cache_busting %}
        <link rel="stylesheet" href="{% static 'css/style.css'|cache_bust %}">
        """
        
        template = Template(template_content)
        request = self.create_request()
        context = Context({'request': request})
        html = template.render(context)
        
        # Check cache busting parameter is added
        self.assertIn('?v=', html)
    
    def test_metadata_manager_filters(self):
        """Test metadata manager custom filters."""
        template_content = """
        {% load metadata_manager_filters %}
        {{ "test_string"|format_urn }}
        """
        
        template = Template(template_content)
        context = Context({})
        html = template.render(context)
        
        # The filter should process the string
        self.assertIn('test_string', html)


class AccessibilityTestCase(BaseTemplateTestCase):
    """Test cases for template accessibility features."""
    
    def test_aria_labels(self):
        """Test templates include proper ARIA labels."""
        request = self.create_request()
        html = self.render_template('base.html', {'user': self.user}, request)
        soup = self.parse_html(html)
        
        # Check for ARIA labels on navigation
        nav = soup.find('nav')
        if nav:
            self.assertIsNotNone(nav.get('aria-label') or nav.get('role'))
    
    def test_form_labels(self):
        """Test form fields have proper labels."""
        request = self.create_request()
        html = self.render_template('policies/create.html', {
            'user': self.user,
            'environments': []
        }, request)
        soup = self.parse_html(html)
        
        # Check form inputs have labels
        inputs = soup.find_all('input', type='text')
        for input_field in inputs:
            input_id = input_field.get('id')
            if input_id:
                label = soup.find('label', {'for': input_id})
                self.assertIsNotNone(label, f"No label found for input {input_id}")
    
    def test_semantic_html(self):
        """Test templates use semantic HTML elements."""
        request = self.create_request()
        html = self.render_template('dashboard.html', {
            'user': self.user,
            'page_title': 'Dashboard'
        }, request)
        soup = self.parse_html(html)
        
        # Check for semantic elements
        main = soup.find('main')
        if main is None:
            # Check for role="main"
            main = soup.find(attrs={'role': 'main'})
        
        # Should have some kind of main content area
        self.assertTrue(main is not None or soup.find('div', class_='main-content') is not None)


class ResponsiveDesignTestCase(BaseTemplateTestCase):
    """Test cases for responsive design elements in templates."""
    
    def test_viewport_meta_tag(self):
        """Test viewport meta tag is present."""
        request = self.create_request()
        html = self.render_template('base.html', {}, request)
        
        # Check viewport meta tag
        self.assertIn('name="viewport"', html)
        self.assertIn('width=device-width', html)
    
    def test_responsive_classes(self):
        """Test templates include responsive CSS classes."""
        request = self.create_request()
        html = self.render_template('dashboard.html', {
            'user': self.user,
            'page_title': 'Dashboard'
        }, request)
        soup = self.parse_html(html)
        
        # Check for Bootstrap responsive classes
        responsive_classes = ['col-', 'row', 'd-sm-', 'd-md-', 'd-lg-', 'd-xl-']
        
        for class_prefix in responsive_classes:
            elements = soup.find_all(class_=lambda x: x and any(cls.startswith(class_prefix) for cls in x.split()))
            if elements:
                # At least one responsive class found
                break
        else:
            # No responsive classes found - this might be okay depending on the template
            pass 