"""
Simple test to verify that the test setup works.
"""

import pytest
from django.test import TestCase
from django.contrib.auth.models import User


class SimpleTestCase(TestCase):
    """Simple test case to verify setup."""
    
    def test_basic_setup(self):
        """Test that basic Django test setup works."""
        self.assertTrue(True)
    
    def test_user_creation(self):
        """Test that we can create users."""
        user = User.objects.create_user(
            username='testuser',
            email='test@example.com',
            password='testpass123'
        )
        self.assertEqual(user.username, 'testuser')
        self.assertEqual(User.objects.count(), 1)
    
    def test_client_access(self):
        """Test that test client works."""
        response = self.client.get('/')
        # This might redirect or return different status codes
        # Just verify we get a response
        self.assertIsNotNone(response)


@pytest.mark.unit
def test_pytest_setup():
    """Test that pytest setup works."""
    assert True


@pytest.mark.unit
def test_user_creation_with_fixtures(regular_user):
    """Test user creation with fixtures."""
    assert regular_user.username == 'testuser'
    assert regular_user.email == 'testuser@test.com'


@pytest.mark.unit  
def test_client_fixture(client):
    """Test client fixture works."""
    response = client.get('/')
    assert response is not None 