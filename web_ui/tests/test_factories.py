"""
Test the test data factories.
"""

import pytest
from django.test import TestCase
from django.contrib.auth.models import User


class FactoriesTestCase(TestCase):
    """Test that our test factories work correctly."""
    
    def test_user_factory_import(self):
        """Test that we can import the UserFactory."""
        try:
            from tests.fixtures.factories import UserFactory
            self.assertTrue(True)  # Import successful
        except ImportError as e:
            self.fail(f"Failed to import UserFactory: {e}")
    
    def test_user_factory_creation(self):
        """Test that we can create users with the factory."""
        from tests.fixtures.factories import UserFactory
        
        user = UserFactory.create_user()
        self.assertIsInstance(user, User)
        self.assertTrue(user.username.startswith('user'))
        self.assertFalse(user.is_staff)
        self.assertFalse(user.is_superuser)
    
    def test_admin_user_factory_creation(self):
        """Test that we can create admin users with the factory."""
        from tests.fixtures.factories import UserFactory
        
        admin_user = UserFactory.create_admin()
        self.assertIsInstance(admin_user, User)
        self.assertTrue(admin_user.is_staff)
        self.assertTrue(admin_user.is_superuser)
    
    def test_environment_factory_import(self):
        """Test that we can import Environment factory if available."""
        try:
            from tests.fixtures.factories import EnvironmentFactory
            self.assertTrue(True)  # Import successful
        except ImportError:
            # Environment factory might not be available if models don't exist
            self.skipTest("Environment factory not available")
    
    def test_environment_factory_creation(self):
        """Test Environment factory creation if available."""
        try:
            from tests.fixtures.factories import EnvironmentFactory
            from web_ui.web_ui.models import Environment
            
            env = EnvironmentFactory()
            self.assertIsInstance(env, Environment)
            self.assertTrue(env.name.startswith('env-'))
        except ImportError:
            self.skipTest("Environment factory or model not available")
    
    def test_policy_factory_import(self):
        """Test that we can import Policy factory if available."""
        try:
            from tests.fixtures.factories import PolicyFactory
            self.assertTrue(True)  # Import successful
        except ImportError:
            # Policy factory might not be available if models don't exist
            self.skipTest("Policy factory not available")
    
    def test_policy_factory_creation(self):
        """Test Policy factory creation if available."""
        try:
            from tests.fixtures.factories import PolicyFactory
            from web_ui.web_ui.models import Policy
            from tests.fixtures.factories import EnvironmentFactory
            
            # Create an environment first
            env = EnvironmentFactory()
            policy = PolicyFactory(environment=env)
            
            self.assertIsInstance(policy, Policy)
            self.assertTrue(policy.name.startswith('policy-'))
            self.assertEqual(policy.environment, env)
        except ImportError:
            self.skipTest("Policy factory or model not available") 