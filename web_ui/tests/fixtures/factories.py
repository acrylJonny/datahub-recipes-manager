"""
Test data factories for creating model instances in tests.

Provides Factory Boy factories for all major models in the web_ui application.
"""

import json
import factory
from django.contrib.auth.models import User
from django.utils import timezone
from datetime import datetime
from factory.django import DjangoModelFactory
from factory import Faker, SubFactory, LazyFunction, Sequence, Iterator, LazyAttribute

# Core web_ui model imports
try:
    from web_ui.models import (
        Environment, Mutation, Policy, LogEntry, GitSettings, GitHubPR, 
        DataHubClientInfo
    )
    WEB_UI_MODELS_AVAILABLE = True
except ImportError:
    WEB_UI_MODELS_AVAILABLE = False

# Try to import other models, handle gracefully if not available
try:
    from web_ui.models import Recipe, RecipeInstance, Settings
    EXTENDED_MODELS_AVAILABLE = True
except ImportError:
    Recipe = RecipeInstance = Settings = None
    EXTENDED_MODELS_AVAILABLE = False


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


if WEB_UI_MODELS_AVAILABLE:
    class EnvironmentFactory(DjangoModelFactory):
        """Factory for creating Environment instances."""
        
        class Meta:
            model = Environment
        
        name = Sequence(lambda n: f'env-{n}')
        description = Faker('text', max_nb_chars=200)
        datahub_host = Faker('url')
        datahub_token = Faker('uuid4')
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
        updated_at = Faker('date_time_this_year', )


    class PolicyFactory(DjangoModelFactory):
        """Factory for creating Policy instances."""
        
        class Meta:
            model = Policy
        
        name = Sequence(lambda n: f'policy-{n}')
        description = Faker('text', max_nb_chars=200)
        policy_json = LazyFunction(lambda: json.dumps({'type': 'METADATA', 'state': 'ACTIVE'}))
        environment = SubFactory(EnvironmentFactory)
        user = SubFactory(UserFactory)
        created_at = Faker('date_time_this_year')
        updated_at = Faker('date_time_this_year', )


    class LogEntryFactory(DjangoModelFactory):
        """Factory for creating LogEntry instances."""
        
        class Meta:
            model = LogEntry
        
        level = Iterator(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
        message = Faker('sentence')
        logger_name = Faker('word')
        created_at = Faker('date_time_this_year')
        user = SubFactory(UserFactory)
        metadata = LazyFunction(lambda: {'source': 'test'})


    class GitSettingsFactory(DjangoModelFactory):
        """Factory for creating GitSettings instances."""
        
        class Meta:
            model = GitSettings
        
        repository_url = Faker('url')
        branch = 'main'
        username = Faker('user_name')
        token = Faker('uuid4')
        email = Faker('email')
        is_active = True


    class GitHubPRFactory(DjangoModelFactory):
        """Factory for creating GitHubPR instances."""
        
        class Meta:
            model = GitHubPR
        
        number = Sequence(lambda n: n + 1)
        title = Faker('sentence')
        description = Faker('text')
        state = Iterator(['open', 'closed', 'merged'])
        created_at = Faker('date_time_this_year')
        updated_at = Faker('date_time_this_year', )
        author = Faker('user_name')
        url = Faker('url')


    class DataHubClientInfoFactory(DjangoModelFactory):
        """Factory for creating DataHubClientInfo instances."""
        
        class Meta:
            model = DataHubClientInfo
        
        host = Faker('url')
        token = Faker('uuid4')
        is_connected = True
        last_connection_check = Faker('date_time_this_year', )
        version = '0.10.0'
        environment = SubFactory(EnvironmentFactory)
        
else:
    # Create stub classes if models aren't available
    class EnvironmentFactory:
        pass
    class MutationFactory:
        pass
    class PolicyFactory:
        pass
    class LogEntryFactory:
        pass
    class GitSettingsFactory:
        pass
    class GitHubPRFactory:
        pass
    class DataHubClientInfoFactory:
        pass


if EXTENDED_MODELS_AVAILABLE and Recipe:
    class RecipeFactory(DjangoModelFactory):
        """Factory for creating Recipe instances."""
        
        class Meta:
            model = Recipe
        
        name = Sequence(lambda n: f'recipe-{n}')
        description = Faker('text', max_nb_chars=200)
        recipe_content = LazyFunction(lambda: {'source': {'type': 'test'}})
        user = SubFactory(UserFactory)
        environment = SubFactory(EnvironmentFactory) if WEB_UI_MODELS_AVAILABLE else None
        created_at = Faker('date_time_this_year')
        updated_at = Faker('date_time_this_year', )


if EXTENDED_MODELS_AVAILABLE and RecipeInstance:
    class RecipeInstanceFactory(DjangoModelFactory):
        """Factory for creating RecipeInstance instances."""
        
        class Meta:
            model = RecipeInstance
        
        name = Sequence(lambda n: f'recipe-instance-{n}')
        description = Faker('text', max_nb_chars=200)
        environment = SubFactory(EnvironmentFactory) if WEB_UI_MODELS_AVAILABLE else None
        user = SubFactory(UserFactory)
        status = Iterator(['CREATED', 'DEPLOYED', 'FAILED'])
        created_at = Faker('date_time_this_year')


if EXTENDED_MODELS_AVAILABLE and Settings:
    class SettingsFactory(DjangoModelFactory):
        """Factory for creating Settings instances."""
        
        class Meta:
            model = Settings
        
        key = Sequence(lambda n: f'setting-key-{n}')
        value = Faker('text', max_nb_chars=100)
        description = Faker('text', max_nb_chars=200)
        is_active = True
        created_at = Faker('date_time_this_year')


# Metadata Manager Factories (conditional imports)
try:
    from metadata_manager.models import Domain, GlossaryNode, GlossaryTerm
    METADATA_MODELS_AVAILABLE = True
    
    class DomainFactory(DjangoModelFactory):
        """Factory for creating Domain instances."""
        
        class Meta:
            model = Domain
        
        name = Sequence(lambda n: f'domain-{n}')
        description = Faker('text', max_nb_chars=200)
        urn = LazyAttribute(lambda obj: f'urn:li:domain:{obj.name}')
        owners = LazyFunction(lambda: [])
        created_at = Faker('date_time_this_year')
    
    
    class GlossaryNodeFactory(DjangoModelFactory):
        """Factory for creating GlossaryNode instances."""
        
        class Meta:
            model = GlossaryNode
        
        name = Sequence(lambda n: f'glossary-node-{n}')
        description = Faker('text', max_nb_chars=200)
        urn = LazyAttribute(lambda obj: f'urn:li:glossaryNode:{obj.name}')
        created_at = Faker('date_time_this_year')
    
    
    class GlossaryTermFactory(DjangoModelFactory):
        """Factory for creating GlossaryTerm instances."""
        
        class Meta:
            model = GlossaryTerm
        
        name = Sequence(lambda n: f'glossary-term-{n}')
        description = Faker('text', max_nb_chars=200)
        urn = LazyAttribute(lambda obj: f'urn:li:glossaryTerm:{obj.name}')
        glossary_node = SubFactory(GlossaryNodeFactory)
        created_at = Faker('date_time_this_year')

except ImportError:
    # Metadata manager models not available, create stub classes
    METADATA_MODELS_AVAILABLE = False
    class DomainFactory:
        pass
    class GlossaryNodeFactory:
        pass
    class GlossaryTermFactory:
        pass


# Additional factories for optional models
try:
    from metadata_manager.models import Tag, StructuredProperty
    EXTENDED_METADATA_MODELS_AVAILABLE = True
    
    class TagFactory(DjangoModelFactory):
        """Factory for creating Tag instances."""
        
        class Meta:
            model = Tag
        
        name = Sequence(lambda n: f'tag-{n}')
        description = Faker('text', max_nb_chars=200)
        urn = LazyAttribute(lambda obj: f'urn:li:tag:{obj.name}')
        color = Faker('color')
        created_at = Faker('date_time_this_year')
    
    
    class StructuredPropertyFactory(DjangoModelFactory):
        """Factory for creating StructuredProperty instances."""
        
        class Meta:
            model = StructuredProperty
        
        qualified_name = Sequence(lambda n: f'property-{n}')
        display_name = Faker('words', nb=2)
        description = Faker('text', max_nb_chars=200)
        urn = LazyAttribute(lambda obj: f'urn:li:structuredProperty:{obj.qualified_name}')
        property_type = Iterator(['STRING', 'NUMBER', 'DATE'])
        created_at = Faker('date_time_this_year')

except ImportError:
    EXTENDED_METADATA_MODELS_AVAILABLE = False
    class TagFactory:
        pass
    class StructuredPropertyFactory:
        pass 