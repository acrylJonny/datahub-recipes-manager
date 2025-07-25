"""
Simplified test data factories for creating model instances in tests.

Provides Factory Boy factories for basic models only.
"""

import json
import factory
from django.contrib.auth.models import User
from django.utils import timezone
from datetime import datetime
from factory.django import DjangoModelFactory
from factory import Faker, SubFactory, LazyFunction, Sequence, Iterator, LazyAttribute


class UserFactory(DjangoModelFactory):
    """Factory for creating User instances."""
    
    class Meta:
        model = User
    
    username = Sequence(lambda n: f'user{n}')
    first_name = Faker('first_name')
    last_name = Faker('last_name')
    email = Faker('email')
    is_active = True
    is_staff = False
    is_superuser = False
    
    @classmethod
    def create_admin(cls, **kwargs):
        """Create an admin user."""
        return cls.create(is_staff=True, is_superuser=True, **kwargs)
    
    @classmethod
    def create_user(cls, **kwargs):
        """Create a regular user."""
        return cls.create(is_staff=False, is_superuser=False, **kwargs)


# Core web_ui model factories - only create if models are available
try:
    from web_ui.models import Environment
    
    class EnvironmentFactory(DjangoModelFactory):
        """Factory for creating Environment instances."""
        
        class Meta:
            model = Environment
        
        name = Sequence(lambda n: f'env-{n}')
        description = Faker('text', max_nb_chars=200)
        is_default = False
        created_at = Faker('date_time_this_year')
        
        @factory.post_generation
        def set_default(obj, create, extracted, **kwargs):
            """Ensure only one default environment exists."""
            if extracted or obj.is_default:
                Environment.objects.filter(is_default=True).exclude(id=obj.id).update(is_default=False)
                obj.is_default = True
                if create:
                    obj.save()

except ImportError:
    class EnvironmentFactory:
        pass


try:
    from web_ui.models import Policy
    
    class PolicyFactory(DjangoModelFactory):
        """Factory for creating Policy instances."""
        
        class Meta:
            model = Policy
        
        name = Sequence(lambda n: f'policy-{n}')
        description = Faker('text', max_nb_chars=200)
        policy_json = LazyFunction(lambda: json.dumps({'type': 'METADATA', 'state': 'ACTIVE'}))
        user = SubFactory(UserFactory)
        created_at = Faker('date_time_this_year')
        updated_at = Faker('date_time_this_year')

except ImportError:
    class PolicyFactory:
        pass


try:
    from web_ui.models import Mutation
    
    class MutationFactory(DjangoModelFactory):
        """Factory for creating Mutation instances."""
        
        class Meta:
            model = Mutation
        
        name = Sequence(lambda n: f'mutation-{n}')
        description = Faker('text', max_nb_chars=200)
        mutation_type = Iterator(['CREATE', 'UPDATE', 'DELETE'])
        mutation_json = LazyFunction(lambda: json.dumps({'test': 'mutation_data'}))
        user = SubFactory(UserFactory)
        created_at = Faker('date_time_this_year')
        updated_at = Faker('date_time_this_year')

except ImportError:
    class MutationFactory:
        pass


try:
    from web_ui.models import LogEntry
    
    class LogEntryFactory(DjangoModelFactory):
        """Factory for creating LogEntry instances."""
        
        class Meta:
            model = LogEntry
        
        level = Iterator(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
        message = Faker('sentence')
        logger_name = Faker('word')
        created_at = Faker('date_time_this_year')
        user = SubFactory(UserFactory)

except ImportError:
    class LogEntryFactory:
        pass


try:
    from web_ui.models import Connection
    
    class ConnectionFactory(DjangoModelFactory):
        """Factory for creating Connection instances."""
        
        class Meta:
            model = Connection
        
        name = Sequence(lambda n: f'connection-{n}')
        description = Faker('text', max_nb_chars=200)
        datahub_url = Faker('url')
        datahub_token = Faker('uuid4')
        verify_ssl = True
        timeout = 30
        is_active = True
        is_default = False
        connection_status = Iterator(['connected', 'failed', 'unknown'])
        created_at = Faker('date_time_this_year')
        updated_at = Faker('date_time_this_year')
        
        @classmethod
        def create_default(cls, **kwargs):
            """Create a default connection."""
            return cls.create(is_default=True, **kwargs)

except ImportError:
    class ConnectionFactory:
        pass 