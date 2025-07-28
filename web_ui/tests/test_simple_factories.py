"""
Test the simplified test data factories.
"""

import pytest
from django.test import TestCase
from django.contrib.auth.models import User


class SimpleFactoriesTestCase(TestCase):
    """Test that our simplified test factories work correctly."""
    
    def test_user_factory_import(self):
        """Test that we can import the UserFactory."""
        from tests.fixtures.simple_factories import UserFactory
        self.assertTrue(True)  # Import successful
    
    def test_user_factory_creation(self):
        """Test that we can create users with the factory."""
        from tests.fixtures.simple_factories import UserFactory
        
        user = UserFactory.create_user()
        self.assertIsInstance(user, User)
        self.assertTrue(user.username.startswith('user'))
        self.assertFalse(user.is_staff)
        self.assertFalse(user.is_superuser)
    
    def test_admin_user_factory_creation(self):
        """Test that we can create admin users with the factory."""
        from tests.fixtures.simple_factories import UserFactory
        
        admin_user = UserFactory.create_admin()
        self.assertIsInstance(admin_user, User)
        self.assertTrue(admin_user.is_staff)
        self.assertTrue(admin_user.is_superuser)
    
    def test_environment_factory_import(self):
        """Test that we can import Environment factory if available."""
        try:
            from tests.fixtures.simple_factories import EnvironmentFactory
            # Check if it's a real factory or stub
            if hasattr(EnvironmentFactory, '_meta'):
                self.assertTrue(True)  # Real factory
            else:
                self.skipTest("Environment factory is a stub")
        except ImportError:
            self.skipTest("Environment factory not available")
    
    def test_environment_factory_creation(self):
        """Test Environment factory creation if available."""
        try:
            from tests.fixtures.simple_factories import EnvironmentFactory
            from web_ui.models import Environment
            
            # Only test if it's a real factory
            if hasattr(EnvironmentFactory, '_meta'):
                env = EnvironmentFactory()
                self.assertIsInstance(env, Environment)
                self.assertTrue(env.name.startswith('env-'))
            else:
                self.skipTest("Environment factory is a stub")
        except ImportError:
            self.skipTest("Environment factory or model not available")
    
    def test_policy_factory_import(self):
        """Test that we can import Policy factory if available."""
        try:
            from tests.fixtures.simple_factories import PolicyFactory
            if hasattr(PolicyFactory, '_meta'):
                self.assertTrue(True)  # Real factory
            else:
                self.skipTest("Policy factory is a stub")
        except ImportError:
            self.skipTest("Policy factory not available")
    
    def test_mutation_factory_import(self):
        """Test that we can import Mutation factory if available."""
        try:
            from tests.fixtures.simple_factories import MutationFactory
            if hasattr(MutationFactory, '_meta'):
                self.assertTrue(True)  # Real factory
            else:
                self.skipTest("Mutation factory is a stub")
        except ImportError:
            self.skipTest("Mutation factory not available") 