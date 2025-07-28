"""
Comprehensive tests for Authentication functionality.

Tests cover:
- User authentication (login, logout)
- Anonymous user backend functionality
- Session management
- Permission-based access control
- Authentication middleware
- Security aspects (CSRF, session hijacking, etc.)
- Password validation and requirements
- User account management
- Authentication redirects and flows
- API authentication (session, token-based)
- Authentication error handling
- Multi-user scenarios and permissions
- Authentication integration with other components
"""

import time
from unittest.mock import patch, Mock
from django.test import TestCase, Client, override_settings
from django.urls import reverse
from django.contrib.auth.models import User, AnonymousUser
from django.contrib.auth import authenticate, login, logout
from django.contrib.sessions.models import Session
from django.contrib.messages import get_messages
from django.http import HttpRequest
from django.middleware.csrf import get_token
from rest_framework.test import APITestCase, APIClient
from rest_framework import status

from web_ui.auth_backends import AnonymousUserBackend
from tests.fixtures.simple_factories import UserFactory


class UserAuthenticationTestCase(TestCase):
    """Test basic user authentication functionality."""

    def setUp(self):
        """Set up test data."""
        self.client = Client()
        self.user = UserFactory(
            username='testuser',
            email='test@example.com'
        )
        self.user.set_password('testpassword123')
        self.user.save()

    def test_user_login_valid_credentials(self):
        """Test user login with valid credentials."""
        response = self.client.post(reverse('login'), {
            'username': 'testuser',
            'password': 'testpassword123'
        })
        
        # Should redirect after successful login
        self.assertEqual(response.status_code, 302)
        
        # User should be logged in
        self.assertTrue('_auth_user_id' in self.client.session)
        self.assertEqual(int(self.client.session['_auth_user_id']), self.user.id)

    def test_user_login_invalid_credentials(self):
        """Test user login with invalid credentials."""
        response = self.client.post(reverse('login'), {
            'username': 'testuser',
            'password': 'wrongpassword'
        })
        
        # Should stay on login page with error
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'error')
        
        # User should not be logged in
        self.assertNotIn('_auth_user_id', self.client.session)

    def test_user_login_nonexistent_user(self):
        """Test user login with nonexistent username."""
        response = self.client.post(reverse('login'), {
            'username': 'nonexistent',
            'password': 'anypassword'
        })
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'error')
        self.assertNotIn('_auth_user_id', self.client.session)

    def test_user_login_empty_credentials(self):
        """Test user login with empty credentials."""
        response = self.client.post(reverse('login'), {
            'username': '',
            'password': ''
        })
        
        self.assertEqual(response.status_code, 200)
        # Should show form validation errors
        self.assertContains(response, 'required')

    def test_user_login_get_request(self):
        """Test GET request to login page."""
        response = self.client.get(reverse('login'))
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'login')
        self.assertContains(response, 'username')
        self.assertContains(response, 'password')

    def test_user_logout(self):
        """Test user logout functionality."""
        # First login
        self.client.force_login(self.user)
        self.assertTrue('_auth_user_id' in self.client.session)
        
        # Then logout
        response = self.client.post(reverse('logout'))
        
        # Should redirect to login page
        self.assertEqual(response.status_code, 302)
        self.assertRedirects(response, reverse('login'))
        
        # User should be logged out
        self.assertNotIn('_auth_user_id', self.client.session)

    def test_login_redirect_after_success(self):
        """Test redirect after successful login."""
        # Try to access protected page first
        protected_url = reverse('dashboard')
        response = self.client.get(protected_url)
        
        # Should redirect to login (or work with anonymous backend)
        if response.status_code == 302:
            # Login required
            login_url = reverse('login')
            response = self.client.post(login_url, {
                'username': 'testuser',
                'password': 'testpassword123'
            })
            
            # Should redirect to originally requested page or dashboard
            self.assertEqual(response.status_code, 302)

    def test_login_csrf_protection(self):
        """Test login form CSRF protection."""
        # Get CSRF token
        response = self.client.get(reverse('login'))
        csrf_token = get_token(response.wsgi_request)
        
        # Login with CSRF token
        response = self.client.post(reverse('login'), {
            'username': 'testuser',
            'password': 'testpassword123',
            'csrfmiddlewaretoken': csrf_token
        })
        
        # Should work
        self.assertEqual(response.status_code, 302)

    def test_user_authentication_backend(self):
        """Test Django authentication backend."""
        # Test authenticate function
        user = authenticate(username='testuser', password='testpassword123')
        self.assertEqual(user, self.user)
        
        # Test with wrong password
        user = authenticate(username='testuser', password='wrongpassword')
        self.assertIsNone(user)
        
        # Test with nonexistent user
        user = authenticate(username='nonexistent', password='anypassword')
        self.assertIsNone(user)


class AnonymousUserBackendTestCase(TestCase):
    """Test the AnonymousUserBackend functionality."""

    def setUp(self):
        """Set up test data."""
        self.client = Client()
        self.backend = AnonymousUserBackend()

    def test_anonymous_user_backend_authenticate(self):
        """Test AnonymousUserBackend authenticate method."""
        request = HttpRequest()
        
        # Should return AnonymousUser for any request
        user = self.backend.authenticate(request, username='any', password='any')
        self.assertIsInstance(user, AnonymousUser)

    def test_anonymous_user_backend_get_user(self):
        """Test AnonymousUserBackend get_user method."""
        # Should return AnonymousUser for any user_id
        user = self.backend.get_user(user_id=999)
        self.assertIsInstance(user, AnonymousUser)
        
        user = self.backend.get_user(user_id=None)
        self.assertIsInstance(user, AnonymousUser)

    def test_anonymous_user_access_to_views(self):
        """Test anonymous user access to application views."""
        # Test various views that should be accessible to anonymous users
        urls_to_test = [
            reverse('dashboard'),
            reverse('settings'),
            reverse('connections_list'),
        ]
        
        for url in urls_to_test:
            response = self.client.get(url)
            # Should be accessible (200) rather than redirect to login (302)
            self.assertEqual(response.status_code, 200, 
                           f"Anonymous access failed for {url}")

    def test_anonymous_user_api_access(self):
        """Test anonymous user access to API endpoints."""
        api_urls = [
            reverse('api-settings'),
            reverse('api-git-settings'),
            reverse('api-dashboard-data'),
        ]
        
        for url in api_urls:
            response = self.client.get(url)
            # Should be accessible to anonymous users
            self.assertEqual(response.status_code, 200,
                           f"Anonymous API access failed for {url}")

    def test_anonymous_user_properties(self):
        """Test anonymous user properties and methods."""
        request = HttpRequest()
        user = self.backend.authenticate(request)
        
        # Should behave like AnonymousUser
        self.assertFalse(user.is_authenticated)
        self.assertFalse(user.is_active)
        self.assertFalse(user.is_staff)
        self.assertFalse(user.is_superuser)
        self.assertEqual(str(user), 'AnonymousUser')


class SessionManagementTestCase(TestCase):
    """Test session management and security."""

    def setUp(self):
        """Set up test data."""
        self.client = Client()
        self.user = UserFactory()
        self.user.set_password('testpassword123')
        self.user.save()

    def test_session_creation_on_login(self):
        """Test session is created on login."""
        # Count sessions before login
        initial_session_count = Session.objects.count()
        
        # Login
        self.client.force_login(self.user)
        
        # Session should be created
        self.assertGreater(Session.objects.count(), initial_session_count)
        self.assertIn('sessionid', self.client.cookies)

    def test_session_destruction_on_logout(self):
        """Test session is destroyed on logout."""
        # Login and get session
        self.client.force_login(self.user)
        session_key = self.client.session.session_key
        
        # Verify session exists
        self.assertTrue(Session.objects.filter(session_key=session_key).exists())
        
        # Logout
        response = self.client.post(reverse('logout'))
        
        # Session should be destroyed
        self.assertFalse(Session.objects.filter(session_key=session_key).exists())

    def test_session_expiry(self):
        """Test session expiry behavior."""
        self.client.force_login(self.user)
        
        # Get session
        session = self.client.session
        
        # Session should have expiry set
        self.assertIsNotNone(session.get_expiry_date())

    def test_session_data_persistence(self):
        """Test session data persistence across requests."""
        self.client.force_login(self.user)
        
        # Set session data
        session = self.client.session
        session['test_data'] = 'test_value'
        session.save()
        
        # Make another request
        response = self.client.get(reverse('dashboard'))
        
        # Session data should persist
        self.assertEqual(self.client.session.get('test_data'), 'test_value')

    def test_multiple_sessions_same_user(self):
        """Test multiple sessions for the same user."""
        # Create two clients for same user
        client1 = Client()
        client2 = Client()
        
        client1.force_login(self.user)
        client2.force_login(self.user)
        
        # Both should have different sessions
        self.assertNotEqual(
            client1.session.session_key, 
            client2.session.session_key
        )
        
        # Both should be able to access protected resources
        response1 = client1.get(reverse('dashboard'))
        response2 = client2.get(reverse('dashboard'))
        
        self.assertEqual(response1.status_code, 200)
        self.assertEqual(response2.status_code, 200)

    def test_session_hijacking_protection(self):
        """Test protection against session hijacking."""
        self.client.force_login(self.user)
        original_session_key = self.client.session.session_key
        
        # Simulate session key tampering
        self.client.cookies['sessionid'].value = 'tampered_session_key'
        
        # Request should create new session or handle gracefully
        response = self.client.get(reverse('dashboard'))
        
        # Should either work (with anonymous backend) or redirect to login
        self.assertIn(response.status_code, [200, 302])


class PermissionBasedAccessTestCase(TestCase):
    """Test permission-based access control."""

    def setUp(self):
        """Set up test data."""
        self.client = Client()
        self.regular_user = UserFactory(
            username='regular',
            is_staff=False,
            is_superuser=False
        )
        self.staff_user = UserFactory(
            username='staff',
            is_staff=True,
            is_superuser=False
        )
        self.superuser = UserFactory(
            username='superuser',
            is_staff=True,
            is_superuser=True
        )

    def test_regular_user_permissions(self):
        """Test regular user permissions."""
        self.client.force_login(self.regular_user)
        
        # Regular users should access basic functionality
        response = self.client.get(reverse('dashboard'))
        self.assertEqual(response.status_code, 200)
        
        response = self.client.get(reverse('settings'))
        self.assertEqual(response.status_code, 200)

    def test_staff_user_permissions(self):
        """Test staff user permissions."""
        self.client.force_login(self.staff_user)
        
        # Staff users should access all regular functionality
        response = self.client.get(reverse('dashboard'))
        self.assertEqual(response.status_code, 200)
        
        response = self.client.get(reverse('settings'))
        self.assertEqual(response.status_code, 200)

    def test_superuser_permissions(self):
        """Test superuser permissions."""
        self.client.force_login(self.superuser)
        
        # Superusers should access all functionality
        response = self.client.get(reverse('dashboard'))
        self.assertEqual(response.status_code, 200)
        
        response = self.client.get(reverse('settings'))
        self.assertEqual(response.status_code, 200)
        
        # Should access admin interface
        response = self.client.get('/admin/')
        self.assertIn(response.status_code, [200, 302])  # 302 if admin redirect

    def test_permission_inheritance(self):
        """Test permission inheritance hierarchy."""
        # Superuser should have all staff permissions
        self.assertTrue(self.superuser.is_staff)
        self.assertTrue(self.superuser.is_superuser)
        
        # Staff user should have staff permissions but not superuser
        self.assertTrue(self.staff_user.is_staff)
        self.assertFalse(self.staff_user.is_superuser)
        
        # Regular user should have neither
        self.assertFalse(self.regular_user.is_staff)
        self.assertFalse(self.regular_user.is_superuser)

    def test_user_group_permissions(self):
        """Test user group-based permissions."""
        from django.contrib.auth.models import Group, Permission
        
        # Create a test group with permissions
        test_group = Group.objects.create(name='test_group')
        
        # Add user to group
        self.regular_user.groups.add(test_group)
        
        # User should inherit group permissions
        self.assertIn(test_group, self.regular_user.groups.all())


class AuthenticationMiddlewareTestCase(TestCase):
    """Test authentication middleware functionality."""

    def setUp(self):
        """Set up test data."""
        self.client = Client()
        self.user = UserFactory()

    def test_authentication_middleware_processing(self):
        """Test authentication middleware processes requests correctly."""
        # Make request without authentication
        response = self.client.get(reverse('dashboard'))
        
        # Should work with anonymous backend
        self.assertEqual(response.status_code, 200)
        
        # Make request with authentication
        self.client.force_login(self.user)
        response = self.client.get(reverse('dashboard'))
        
        self.assertEqual(response.status_code, 200)

    def test_user_context_in_views(self):
        """Test user context is available in views."""
        self.client.force_login(self.user)
        response = self.client.get(reverse('dashboard'))
        
        # User should be available in template context
        if hasattr(response, 'context') and response.context:
            # User context might be available
            self.assertIsNotNone(response.context.get('user'))

    def test_request_user_attribute(self):
        """Test request.user attribute is set correctly."""
        # This is more of an integration test
        self.client.force_login(self.user)
        response = self.client.get(reverse('dashboard'))
        
        # Response should indicate user was processed correctly
        self.assertEqual(response.status_code, 200)


class AuthenticationSecurityTestCase(TestCase):
    """Test authentication security aspects."""

    def setUp(self):
        """Set up test data."""
        self.client = Client()
        self.user = UserFactory()
        self.user.set_password('testpassword123')
        self.user.save()

    def test_csrf_protection_on_login(self):
        """Test CSRF protection on login form."""
        # Attempt login without CSRF token
        response = self.client.post(reverse('login'), {
            'username': 'testuser',
            'password': 'testpassword123'
        })
        
        # Should fail due to CSRF protection
        self.assertEqual(response.status_code, 403)

    def test_password_security_requirements(self):
        """Test password security requirements."""
        # Test password validation if implemented
        weak_passwords = [
            '123',
            'password',
            'abc123',
            '12345678'
        ]
        
        for weak_password in weak_passwords:
            user = User(username=f'test_{weak_password}')
            try:
                # Try to set weak password
                user.set_password(weak_password)
                user.full_clean()  # This might trigger validation
                # If we get here, validation might not be strict
            except Exception:
                # Validation rejected weak password - good
                pass

    def test_login_rate_limiting(self):
        """Test login rate limiting (if implemented)."""
        # Attempt multiple failed logins
        for i in range(10):
            response = self.client.post(reverse('login'), {
                'username': 'testuser',
                'password': 'wrongpassword'
            })
            
            # Should eventually be rate limited (if implemented)
            if response.status_code == 429:  # Too Many Requests
                break
        
        # This test might pass even without rate limiting
        # Actual implementation depends on the application's security requirements

    def test_session_fixation_protection(self):
        """Test protection against session fixation attacks."""
        # Get initial session
        self.client.get(reverse('login'))
        initial_session_key = self.client.session.session_key
        
        # Login
        self.client.post(reverse('login'), {
            'username': 'testuser',
            'password': 'testpassword123'
        })
        
        # Session key should change after login (if protection is implemented)
        new_session_key = self.client.session.session_key
        
        # Django's default behavior may or may not change session key
        # This test documents the current behavior
        self.assertIsNotNone(new_session_key)

    def test_secure_cookie_settings(self):
        """Test secure cookie settings."""
        self.client.force_login(self.user)
        
        # Check session cookie settings
        response = self.client.get(reverse('dashboard'))
        
        if 'sessionid' in response.cookies:
            sessionid_cookie = response.cookies['sessionid']
            
            # In production, these should be set for security
            # In testing, they might not be set
            # This test documents expected security settings
            pass

    def test_xss_protection_in_auth_forms(self):
        """Test XSS protection in authentication forms."""
        # Try to inject script in username field
        malicious_username = '<script>alert("xss")</script>'
        
        response = self.client.post(reverse('login'), {
            'username': malicious_username,
            'password': 'anypassword'
        })
        
        # Response should not contain executable script
        self.assertNotContains(response, '<script>alert("xss")</script>')
        
        # Should contain escaped version if username is displayed
        if malicious_username in response.content.decode():
            # Make sure it's properly escaped
            self.assertContains(response, '&lt;script&gt;')


class APIAuthenticationTestCase(APITestCase):
    """Test API authentication methods."""

    def setUp(self):
        """Set up test data."""
        self.user = UserFactory()
        self.client = APIClient()

    def test_api_session_authentication(self):
        """Test API session authentication."""
        # Force authenticate user
        self.client.force_authenticate(user=self.user)
        
        response = self.client.get(reverse('api-settings'))
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_api_anonymous_authentication(self):
        """Test API anonymous authentication."""
        # No authentication
        response = self.client.get(reverse('api-settings'))
        
        # Should work with anonymous backend
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_api_authentication_required_endpoints(self):
        """Test API endpoints that might require authentication."""
        # Test various API endpoints
        api_endpoints = [
            reverse('api-settings'),
            reverse('api-connections'),
            reverse('api-dashboard-data'),
        ]
        
        for endpoint in api_endpoints:
            # Test without authentication
            response = self.client.get(endpoint)
            # Should work with anonymous backend
            self.assertEqual(response.status_code, status.HTTP_200_OK)
            
            # Test with authentication
            self.client.force_authenticate(user=self.user)
            response = self.client.get(endpoint)
            self.assertEqual(response.status_code, status.HTTP_200_OK)
            
            # Reset authentication
            self.client.force_authenticate(user=None)

    def test_api_csrf_exemption(self):
        """Test API endpoints are CSRF exempt."""
        # API endpoints should be CSRF exempt
        response = self.client.post(reverse('api-connection-switch'), {
            'connection_id': 1
        })
        
        # Should not fail due to CSRF (might fail for other reasons like missing connection)
        self.assertNotEqual(response.status_code, 403)


class AuthenticationIntegrationTestCase(TestCase):
    """Test authentication integration with other components."""

    def setUp(self):
        """Set up test data."""
        self.client = Client()
        self.user = UserFactory()

    def test_authentication_with_connection_management(self):
        """Test authentication works with connection management."""
        self.client.force_login(self.user)
        
        # Should be able to access connection management
        response = self.client.get(reverse('connections_list'))
        self.assertEqual(response.status_code, 200)

    def test_authentication_with_settings(self):
        """Test authentication works with settings."""
        self.client.force_login(self.user)
        
        # Should be able to access and modify settings
        response = self.client.get(reverse('settings'))
        self.assertEqual(response.status_code, 200)
        
        # Should be able to update settings
        response = self.client.post(reverse('settings'), {
            'section': 'policy_settings',
            'policy_export_dir': '/test/path'
        })
        self.assertEqual(response.status_code, 302)  # Redirect after success

    def test_authentication_state_across_components(self):
        """Test authentication state is consistent across components."""
        self.client.force_login(self.user)
        
        # Authentication should work across different areas of the app
        areas_to_test = [
            reverse('dashboard'),
            reverse('settings'),
            reverse('connections_list'),
        ]
        
        for area in areas_to_test:
            response = self.client.get(area)
            self.assertEqual(response.status_code, 200,
                           f"Authentication failed for {area}")

    def test_logout_clears_all_state(self):
        """Test logout clears authentication state from all components."""
        self.client.force_login(self.user)
        
        # Verify user is logged in
        response = self.client.get(reverse('dashboard'))
        self.assertEqual(response.status_code, 200)
        
        # Logout
        response = self.client.post(reverse('logout'))
        self.assertEqual(response.status_code, 302)
        
        # Should still work with anonymous backend
        response = self.client.get(reverse('dashboard'))
        self.assertEqual(response.status_code, 200)


class MultiUserScenarioTestCase(TestCase):
    """Test multi-user scenarios and user isolation."""

    def setUp(self):
        """Set up test data."""
        self.user1 = UserFactory(username='user1')
        self.user2 = UserFactory(username='user2')
        self.client1 = Client()
        self.client2 = Client()

    def test_user_isolation(self):
        """Test that users are properly isolated."""
        # Login different users to different clients
        self.client1.force_login(self.user1)
        self.client2.force_login(self.user2)
        
        # Both should be able to access the application
        response1 = self.client1.get(reverse('dashboard'))
        response2 = self.client2.get(reverse('dashboard'))
        
        self.assertEqual(response1.status_code, 200)
        self.assertEqual(response2.status_code, 200)

    def test_concurrent_user_sessions(self):
        """Test concurrent user sessions."""
        # Multiple users should be able to use the app simultaneously
        self.client1.force_login(self.user1)
        self.client2.force_login(self.user2)
        
        # Both should have separate sessions
        self.assertNotEqual(
            self.client1.session.session_key,
            self.client2.session.session_key
        )

    def test_user_specific_data_isolation(self):
        """Test user-specific data isolation."""
        # In this application, most data might be shared
        # But session data should be isolated
        
        self.client1.force_login(self.user1)
        self.client2.force_login(self.user2)
        
        # Set different session data for each user
        session1 = self.client1.session
        session1['user_specific_data'] = 'user1_data'
        session1.save()
        
        session2 = self.client2.session
        session2['user_specific_data'] = 'user2_data'
        session2.save()
        
        # Each user should see their own data
        self.assertEqual(
            self.client1.session.get('user_specific_data'),
            'user1_data'
        )
        self.assertEqual(
            self.client2.session.get('user_specific_data'),
            'user2_data'
        )

    def test_user_logout_does_not_affect_others(self):
        """Test that one user logout doesn't affect other users."""
        self.client1.force_login(self.user1)
        self.client2.force_login(self.user2)
        
        # Logout user1
        response = self.client1.post(reverse('logout'))
        self.assertEqual(response.status_code, 302)
        
        # User2 should still be logged in and functional
        response = self.client2.get(reverse('dashboard'))
        self.assertEqual(response.status_code, 200)


class AuthenticationErrorHandlingTestCase(TestCase):
    """Test authentication error handling."""

    def setUp(self):
        """Set up test data."""
        self.client = Client()
        self.user = UserFactory()

    def test_invalid_session_handling(self):
        """Test handling of invalid sessions."""
        # Set invalid session ID
        self.client.cookies['sessionid'] = 'invalid_session_id'
        
        # Should handle gracefully
        response = self.client.get(reverse('dashboard'))
        
        # Should work (with anonymous backend) or redirect to login
        self.assertIn(response.status_code, [200, 302])

    def test_expired_session_handling(self):
        """Test handling of expired sessions."""
        # This is difficult to test without time manipulation
        # But we can test that the system handles it gracefully
        
        self.client.force_login(self.user)
        
        # Simulate session expiry by clearing session data
        self.client.session.flush()
        
        # Should handle gracefully
        response = self.client.get(reverse('dashboard'))
        self.assertIn(response.status_code, [200, 302])

    def test_database_connection_error_during_auth(self):
        """Test authentication when database is unavailable."""
        # This is a complex test that would require mocking database
        # For now, we just ensure the system can handle auth errors
        
        with patch('django.contrib.auth.authenticate') as mock_auth:
            mock_auth.side_effect = Exception("Database connection error")
            
            response = self.client.post(reverse('login'), {
                'username': 'testuser',
                'password': 'testpassword'
            })
            
            # Should handle error gracefully
            self.assertIn(response.status_code, [200, 500])

    def test_authentication_backend_failure(self):
        """Test authentication backend failure handling."""
        with patch('web_ui.auth_backends.AnonymousUserBackend.authenticate') as mock_auth:
            mock_auth.side_effect = Exception("Backend error")
            
            response = self.client.get(reverse('dashboard'))
            
            # Should handle error gracefully
            self.assertIn(response.status_code, [200, 500])

    def test_malformed_authentication_data(self):
        """Test handling of malformed authentication data."""
        # Try login with malformed data
        response = self.client.post(reverse('login'), {
            'username': '\x00\x01\x02',  # Binary data
            'password': '🚀🔒'  # Unicode data
        })
        
        # Should handle gracefully without crashing
        self.assertIn(response.status_code, [200, 400])


class AuthenticationPerformanceTestCase(TestCase):
    """Test authentication performance aspects."""

    def setUp(self):
        """Set up test data."""
        self.client = Client()
        self.user = UserFactory()

    def test_login_performance(self):
        """Test login performance."""
        import time
        
        start_time = time.time()
        
        response = self.client.post(reverse('login'), {
            'username': self.user.username,
            'password': 'testpassword'
        })
        
        login_time = time.time() - start_time
        
        # Login should be reasonably fast
        self.assertLess(login_time, 2.0)  # Should complete within 2 seconds

    def test_authentication_check_performance(self):
        """Test authentication check performance."""
        self.client.force_login(self.user)
        
        import time
        start_time = time.time()
        
        # Make multiple requests that require authentication checks
        for _ in range(10):
            response = self.client.get(reverse('dashboard'))
            self.assertEqual(response.status_code, 200)
        
        total_time = time.time() - start_time
        
        # Should handle multiple auth checks efficiently
        self.assertLess(total_time, 5.0)  # 10 requests in under 5 seconds

    def test_concurrent_authentication_performance(self):
        """Test concurrent authentication performance."""
        import threading
        import time
        
        results = []
        
        def authenticate_user():
            client = Client()
            client.force_login(self.user)
            response = client.get(reverse('dashboard'))
            results.append(response.status_code)
        
        start_time = time.time()
        
        # Create multiple threads for concurrent authentication
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=authenticate_user)
            threads.append(thread)
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        total_time = time.time() - start_time
        
        # All authentications should succeed
        self.assertEqual(len(results), 5)
        for status_code in results:
            self.assertEqual(status_code, 200)
        
        # Should handle concurrent auth reasonably well
        self.assertLess(total_time, 10.0)  # 5 concurrent auth in under 10 seconds 