"""
Comprehensive unit tests for Django models in the web_ui application.

Tests cover:
- Model creation and validation
- Field constraints and relationships
- Custom model methods
- Model managers and querysets
- Database constraints and triggers
"""

import pytest
from datetime import datetime, timedelta
from django.test import TestCase
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.contrib.auth.models import User
from django.utils import timezone

from web_ui.web_ui.models import (
    Environment, Policy, LogEntry, GitSettings, GitHubPR, 
    GitIntegration, ScriptRun, ScriptResult, Artifact, 
    Settings, Mutation, DataHubClientInfo
)
from tests.fixtures.factories import (
    UserFactory, EnvironmentFactory, PolicyFactory, 
    LogEntryFactory, GitSettingsFactory, GitHubPRFactory,
    GitIntegrationFactory, ScriptRunFactory, MutationFactory
)


class EnvironmentModelTestCase(TestCase):
    """Test cases for Environment model functionality."""
    
    def test_environment_creation(self):
        """Test creating an environment with all fields."""
        environment = Environment.objects.create(
            name='production',
            description='Production environment',
            datahub_host='prod.datahub.com',
            datahub_token='prod-token-123',
            is_default=False,
            created_at=timezone.now()
        )
        
        self.assertEqual(environment.name, 'production')
        self.assertEqual(environment.description, 'Production environment')
        self.assertEqual(environment.datahub_host, 'prod.datahub.com')
        self.assertEqual(environment.datahub_token, 'prod-token-123')
        self.assertFalse(environment.is_default)
        self.assertIsNotNone(environment.created_at)
    
    def test_environment_string_representation(self):
        """Test string representation of environment."""
        environment = EnvironmentFactory(name='test-env')
        self.assertEqual(str(environment), 'test-env')
    
    def test_default_environment_constraint(self):
        """Test that only one environment can be marked as default."""
        # Create first default environment
        env1 = Environment.objects.create(name='dev', is_default=True)
        self.assertTrue(env1.is_default)
        
        # Create second default environment - should make first non-default
        env2 = Environment.objects.create(name='staging', is_default=True)
        
        # Refresh from database
        env1.refresh_from_db()
        env2.refresh_from_db()
        
        self.assertFalse(env1.is_default)
        self.assertTrue(env2.is_default)
    
    def test_environment_ordering(self):
        """Test environment default ordering."""
        env1 = EnvironmentFactory(name='z-last')
        env2 = EnvironmentFactory(name='a-first')
        env3 = EnvironmentFactory(name='m-middle')
        
        environments = list(Environment.objects.all())
        self.assertEqual(environments[0].name, 'a-first')
        self.assertEqual(environments[1].name, 'm-middle')
        self.assertEqual(environments[2].name, 'z-last')
    
    def test_environment_validation(self):
        """Test environment field validation."""
        # Test name field validation
        with self.assertRaises(ValidationError):
            environment = Environment(name='', datahub_host='test.com')
            environment.full_clean()
    
    def test_get_default_environment(self):
        """Test getting the default environment."""
        # No default environment initially
        default = Environment.get_default()
        self.assertIsNone(default)
        
        # Create default environment
        env = EnvironmentFactory(is_default=True)
        default = Environment.get_default()
        self.assertEqual(default, env)
    
    def test_environment_get_absolute_url(self):
        """Test environment absolute URL generation."""
        environment = EnvironmentFactory()
        url = environment.get_absolute_url()
        self.assertIn(str(environment.id), url)


class PolicyModelTestCase(TestCase):
    """Test cases for Policy model functionality."""
    
    def test_policy_creation(self):
        """Test creating a policy with all fields."""
        policy_data = {
            "name": "test-policy",
            "description": "A test policy",
            "type": "METADATA",
            "state": "ACTIVE"
        }
        
        policy = Policy.objects.create(
            name='Test Policy',
            description='A test policy for metadata',
            policy_data=policy_data,
            environment=EnvironmentFactory()
        )
        
        self.assertEqual(policy.name, 'Test Policy')
        self.assertEqual(policy.description, 'A test policy for metadata')
        self.assertEqual(policy.policy_data['name'], 'test-policy')
        self.assertIsNotNone(policy.environment)
        self.assertIsNotNone(policy.created_at)
        self.assertIsNotNone(policy.updated_at)
    
    def test_policy_string_representation(self):
        """Test string representation of policy."""
        policy = PolicyFactory(name='Test Policy')
        self.assertEqual(str(policy), 'Test Policy')
    
    def test_policy_json_field_handling(self):
        """Test JSON field operations."""
        policy_data = {
            "name": "complex-policy",
            "privileges": ["EDIT_ENTITY_OWNERS", "EDIT_ENTITY_TAGS"],
            "actors": {
                "users": ["urn:li:corpuser:user1", "urn:li:corpuser:user2"],
                "groups": ["urn:li:corpGroup:group1"]
            }
        }
        
        policy = PolicyFactory(policy_data=policy_data)
        
        # Test accessing nested JSON data
        self.assertEqual(policy.policy_data['name'], 'complex-policy')
        self.assertEqual(len(policy.policy_data['privileges']), 2)
        self.assertEqual(len(policy.policy_data['actors']['users']), 2)
    
    def test_policy_deployment_tracking(self):
        """Test policy deployment status tracking."""
        policy = PolicyFactory()
        
        # Initially not deployed
        self.assertIsNone(policy.deployed_at)
        self.assertFalse(policy.is_deployed)
        
        # Mark as deployed
        policy.deployed_at = timezone.now()
        policy.save()
        
        self.assertTrue(policy.is_deployed)
    
    def test_policy_update_timestamp(self):
        """Test that updated_at timestamp changes on save."""
        policy = PolicyFactory()
        original_updated_at = policy.updated_at
        
        # Wait a small amount and update
        import time
        time.sleep(0.01)
        policy.description = "Updated description"
        policy.save()
        
        self.assertGreater(policy.updated_at, original_updated_at)
    
    def test_policy_queryset_methods(self):
        """Test custom queryset methods."""
        env1 = EnvironmentFactory(name='dev')
        env2 = EnvironmentFactory(name='prod')
        
        policy1 = PolicyFactory(environment=env1)
        policy2 = PolicyFactory(environment=env2)
        
        # Test filtering by environment
        dev_policies = Policy.objects.filter(environment=env1)
        self.assertIn(policy1, dev_policies)
        self.assertNotIn(policy2, dev_policies)


class LogEntryModelTestCase(TestCase):
    """Test cases for LogEntry model functionality."""
    
    def test_log_entry_creation(self):
        """Test creating a log entry."""
        log_entry = LogEntry.objects.create(
            level='INFO',
            message='Test log message',
            source='test.module',
            timestamp=timezone.now()
        )
        
        self.assertEqual(log_entry.level, 'INFO')
        self.assertEqual(log_entry.message, 'Test log message')
        self.assertEqual(log_entry.source, 'test.module')
        self.assertIsNotNone(log_entry.timestamp)
    
    def test_log_entry_string_representation(self):
        """Test string representation of log entry."""
        log_entry = LogEntryFactory(
            level='ERROR',
            message='Test error message'
        )
        expected = f"ERROR: Test error message"
        self.assertEqual(str(log_entry), expected)
    
    def test_log_entry_ordering(self):
        """Test log entry ordering by timestamp (newest first)."""
        old_log = LogEntryFactory(
            timestamp=timezone.now() - timedelta(hours=1)
        )
        new_log = LogEntryFactory(
            timestamp=timezone.now()
        )
        
        logs = list(LogEntry.objects.all())
        self.assertEqual(logs[0], new_log)  # Newest first
        self.assertEqual(logs[1], old_log)
    
    def test_log_entry_level_choices(self):
        """Test log level choices validation."""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        
        for level in valid_levels:
            log_entry = LogEntryFactory(level=level)
            self.assertEqual(log_entry.level, level)
    
    def test_log_entry_class_methods(self):
        """Test class methods for log entry management."""
        # Create various log entries
        LogEntryFactory(level='INFO', message='Info message 1')
        LogEntryFactory(level='ERROR', message='Error message 1')
        LogEntryFactory(level='WARNING', message='Warning message 1')
        
        # Test count by level
        self.assertEqual(LogEntry.objects.filter(level='INFO').count(), 1)
        self.assertEqual(LogEntry.objects.filter(level='ERROR').count(), 1)
        self.assertEqual(LogEntry.objects.filter(level='WARNING').count(), 1)
        
        # Test cleanup old entries
        old_log = LogEntryFactory(
            timestamp=timezone.now() - timedelta(days=8)
        )
        
        # Should be 4 total entries now
        self.assertEqual(LogEntry.objects.count(), 4)
        
        # Clean up entries older than 7 days
        LogEntry.cleanup_old_entries(days=7)
        
        # Should be 3 entries now (old one removed)
        self.assertEqual(LogEntry.objects.count(), 3)


class GitSettingsModelTestCase(TestCase):
    """Test cases for GitSettings model functionality."""
    
    def test_git_settings_creation(self):
        """Test creating git settings."""
        git_settings = GitSettings.objects.create(
            enabled=True,
            username='testuser',
            repository='test-repo',
            token='github-token-123',
            current_branch='main'
        )
        
        self.assertTrue(git_settings.enabled)
        self.assertEqual(git_settings.username, 'testuser')
        self.assertEqual(git_settings.repository, 'test-repo')
        self.assertEqual(git_settings.token, 'github-token-123')
        self.assertEqual(git_settings.current_branch, 'main')
    
    def test_git_settings_string_representation(self):
        """Test string representation of git settings."""
        git_settings = GitSettingsFactory(
            username='testuser',
            repository='test-repo'
        )
        expected = "testuser/test-repo"
        self.assertEqual(str(git_settings), expected)
    
    def test_git_settings_singleton_pattern(self):
        """Test that only one GitSettings instance should exist."""
        settings1 = GitSettingsFactory()
        settings2 = GitSettingsFactory()
        
        # Should have only one settings object
        self.assertEqual(GitSettings.objects.count(), 1)
    
    def test_git_settings_validation(self):
        """Test git settings field validation."""
        # Test invalid repository format
        with self.assertRaises(ValidationError):
            git_settings = GitSettings(
                username='user',
                repository='',  # Empty repository
                token='token'
            )
            git_settings.full_clean()


class GitHubPRModelTestCase(TestCase):
    """Test cases for GitHubPR model functionality."""
    
    def test_github_pr_creation(self):
        """Test creating a GitHub PR record."""
        pr = GitHubPR.objects.create(
            pr_number=123,
            title='Test PR',
            body='This is a test pull request',
            state='open',
            branch='feature/test',
            created_by=UserFactory()
        )
        
        self.assertEqual(pr.pr_number, 123)
        self.assertEqual(pr.title, 'Test PR')
        self.assertEqual(pr.body, 'This is a test pull request')
        self.assertEqual(pr.state, 'open')
        self.assertEqual(pr.branch, 'feature/test')
        self.assertIsNotNone(pr.created_by)
        self.assertIsNotNone(pr.created_at)
    
    def test_github_pr_string_representation(self):
        """Test string representation of GitHub PR."""
        pr = GitHubPRFactory(pr_number=456, title='Test PR Title')
        expected = "PR #456: Test PR Title"
        self.assertEqual(str(pr), expected)
    
    def test_github_pr_state_choices(self):
        """Test GitHub PR state choices."""
        valid_states = ['open', 'closed', 'merged']
        
        for state in valid_states:
            pr = GitHubPRFactory(state=state)
            self.assertEqual(pr.state, state)
    
    def test_github_pr_ordering(self):
        """Test GitHub PR ordering by creation date."""
        old_pr = GitHubPRFactory(
            created_at=timezone.now() - timedelta(hours=1)
        )
        new_pr = GitHubPRFactory(
            created_at=timezone.now()
        )
        
        prs = list(GitHubPR.objects.all())
        self.assertEqual(prs[0], new_pr)  # Newest first
        self.assertEqual(prs[1], old_pr)


class ScriptRunModelTestCase(TestCase):
    """Test cases for ScriptRun model functionality."""
    
    def test_script_run_creation(self):
        """Test creating a script run record."""
        script_run = ScriptRun.objects.create(
            script_name='test_script.py',
            status='running',
            started_by=UserFactory(),
            environment=EnvironmentFactory()
        )
        
        self.assertEqual(script_run.script_name, 'test_script.py')
        self.assertEqual(script_run.status, 'running')
        self.assertIsNotNone(script_run.started_by)
        self.assertIsNotNone(script_run.environment)
        self.assertIsNotNone(script_run.started_at)
    
    def test_script_run_completion(self):
        """Test marking script run as completed."""
        script_run = ScriptRunFactory(status='running')
        self.assertIsNone(script_run.completed_at)
        
        # Mark as completed
        script_run.status = 'completed'
        script_run.completed_at = timezone.now()
        script_run.save()
        
        self.assertEqual(script_run.status, 'completed')
        self.assertIsNotNone(script_run.completed_at)
    
    def test_script_run_duration_calculation(self):
        """Test calculation of script run duration."""
        start_time = timezone.now()
        end_time = start_time + timedelta(minutes=5)
        
        script_run = ScriptRunFactory(
            started_at=start_time,
            completed_at=end_time
        )
        
        duration = script_run.duration
        self.assertEqual(duration.total_seconds(), 300)  # 5 minutes
    
    def test_script_run_status_choices(self):
        """Test script run status choices."""
        valid_statuses = ['pending', 'running', 'completed', 'failed']
        
        for status in valid_statuses:
            script_run = ScriptRunFactory(status=status)
            self.assertEqual(script_run.status, status)


class MutationModelTestCase(TestCase):
    """Test cases for Mutation model functionality."""
    
    def test_mutation_creation(self):
        """Test creating a mutation record."""
        mutation_data = {
            "entityUrn": "urn:li:dataset:test",
            "mutationType": "UPDATE",
            "aspectName": "datasetProperties"
        }
        
        mutation = Mutation.objects.create(
            mutation_type='update',
            entity_urn='urn:li:dataset:test',
            aspect_name='datasetProperties',
            mutation_data=mutation_data,
            environment=EnvironmentFactory(),
            created_by=UserFactory()
        )
        
        self.assertEqual(mutation.mutation_type, 'update')
        self.assertEqual(mutation.entity_urn, 'urn:li:dataset:test')
        self.assertEqual(mutation.aspect_name, 'datasetProperties')
        self.assertEqual(mutation.mutation_data['entityUrn'], 'urn:li:dataset:test')
        self.assertIsNotNone(mutation.environment)
        self.assertIsNotNone(mutation.created_by)
    
    def test_mutation_string_representation(self):
        """Test string representation of mutation."""
        mutation = MutationFactory(
            mutation_type='create',
            entity_urn='urn:li:dataset:test'
        )
        expected = "create: urn:li:dataset:test"
        self.assertEqual(str(mutation), expected)
    
    def test_mutation_status_tracking(self):
        """Test mutation status tracking."""
        mutation = MutationFactory(status='pending')
        
        # Mark as applied
        mutation.status = 'applied'
        mutation.applied_at = timezone.now()
        mutation.save()
        
        self.assertEqual(mutation.status, 'applied')
        self.assertIsNotNone(mutation.applied_at)


class DataHubClientInfoModelTestCase(TestCase):
    """Test cases for DataHubClientInfo model functionality."""
    
    def test_client_info_creation(self):
        """Test creating client info record."""
        client_info = DataHubClientInfo.objects.create(
            environment=EnvironmentFactory(),
            server_version='0.10.0',
            client_version='0.9.5',
            last_connection=timezone.now(),
            connection_status='connected'
        )
        
        self.assertEqual(client_info.server_version, '0.10.0')
        self.assertEqual(client_info.client_version, '0.9.5')
        self.assertEqual(client_info.connection_status, 'connected')
        self.assertIsNotNone(client_info.last_connection)
    
    def test_client_info_connection_status(self):
        """Test connection status tracking."""
        client_info = DataHubClientInfo.objects.create(
            environment=EnvironmentFactory(),
            connection_status='disconnected'
        )
        
        # Update connection status
        client_info.connection_status = 'connected'
        client_info.last_connection = timezone.now()
        client_info.save()
        
        self.assertEqual(client_info.connection_status, 'connected')
        self.assertIsNotNone(client_info.last_connection)


@pytest.mark.django_db
class ModelRelationshipTestCase(TestCase):
    """Test cases for model relationships and foreign keys."""
    
    def test_environment_policy_relationship(self):
        """Test relationship between Environment and Policy models."""
        environment = EnvironmentFactory()
        policy1 = PolicyFactory(environment=environment)
        policy2 = PolicyFactory(environment=environment)
        
        # Test forward relationship
        self.assertEqual(policy1.environment, environment)
        self.assertEqual(policy2.environment, environment)
        
        # Test reverse relationship
        policies = environment.policy_set.all()
        self.assertIn(policy1, policies)
        self.assertIn(policy2, policies)
        self.assertEqual(policies.count(), 2)
    
    def test_user_script_run_relationship(self):
        """Test relationship between User and ScriptRun models."""
        user = UserFactory()
        script_run1 = ScriptRunFactory(started_by=user)
        script_run2 = ScriptRunFactory(started_by=user)
        
        # Test forward relationship
        self.assertEqual(script_run1.started_by, user)
        self.assertEqual(script_run2.started_by, user)
        
        # Test reverse relationship
        script_runs = user.scriptrun_set.all()
        self.assertIn(script_run1, script_runs)
        self.assertIn(script_run2, script_runs)
        self.assertEqual(script_runs.count(), 2)
    
    def test_cascade_deletion(self):
        """Test cascade deletion behavior."""
        user = UserFactory()
        environment = EnvironmentFactory()
        
        script_run = ScriptRunFactory(started_by=user, environment=environment)
        policy = PolicyFactory(environment=environment)
        
        # Delete environment should cascade to related objects
        environment_id = environment.id
        environment.delete()
        
        # Script run and policy should be deleted
        self.assertFalse(ScriptRun.objects.filter(environment_id=environment_id).exists())
        self.assertFalse(Policy.objects.filter(environment_id=environment_id).exists())
        
        # User should still exist
        self.assertTrue(User.objects.filter(id=user.id).exists())


@pytest.mark.django_db
class ModelValidationTestCase(TestCase):
    """Test cases for model field validation and constraints."""
    
    def test_required_field_validation(self):
        """Test validation of required fields."""
        # Test Environment name is required
        with self.assertRaises(ValidationError):
            environment = Environment(name='')
            environment.full_clean()
        
        # Test Policy name is required
        with self.assertRaises(ValidationError):
            policy = Policy(name='', environment=EnvironmentFactory())
            policy.full_clean()
    
    def test_unique_constraint_validation(self):
        """Test unique constraint validation."""
        # Create environment with specific name
        EnvironmentFactory(name='unique-env')
        
        # Try to create another with same name should fail
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                Environment.objects.create(name='unique-env')
    
    def test_json_field_validation(self):
        """Test JSON field validation."""
        # Valid JSON should work
        valid_data = {"name": "test", "type": "METADATA"}
        policy = PolicyFactory(policy_data=valid_data)
        self.assertEqual(policy.policy_data['name'], 'test')
        
        # Invalid JSON structure should still be stored (Django JSONField is flexible)
        policy.policy_data = {"invalid": "but", "still": "stored"}
        policy.save()
        self.assertEqual(policy.policy_data['invalid'], 'but')
    
    def test_choice_field_validation(self):
        """Test choice field validation."""
        # Valid choice should work
        log_entry = LogEntryFactory(level='INFO')
        self.assertEqual(log_entry.level, 'INFO')
        
        # Invalid choice should raise validation error
        with self.assertRaises(ValidationError):
            log_entry = LogEntry(
                level='INVALID_LEVEL',
                message='Test message'
            )
            log_entry.full_clean()


@pytest.mark.django_db
class ModelManagerTestCase(TestCase):
    """Test cases for custom model managers and querysets."""
    
    def test_environment_default_manager(self):
        """Test Environment default manager methods."""
        # Create multiple environments
        env1 = EnvironmentFactory(is_default=True)
        env2 = EnvironmentFactory(is_default=False)
        env3 = EnvironmentFactory(is_default=False)
        
        # Test getting default environment
        default_env = Environment.objects.filter(is_default=True).first()
        self.assertEqual(default_env, env1)
        
        # Test ordering (alphabetical by name)
        all_envs = list(Environment.objects.all())
        names = [env.name for env in all_envs]
        self.assertEqual(names, sorted(names))
    
    def test_log_entry_level_filtering(self):
        """Test LogEntry filtering by level."""
        # Create log entries with different levels
        LogEntryFactory(level='INFO', message='Info message')
        LogEntryFactory(level='ERROR', message='Error message')
        LogEntryFactory(level='WARNING', message='Warning message')
        LogEntryFactory(level='DEBUG', message='Debug message')
        
        # Test filtering by specific level
        info_logs = LogEntry.objects.filter(level='INFO')
        self.assertEqual(info_logs.count(), 1)
        self.assertEqual(info_logs.first().message, 'Info message')
        
        # Test filtering by multiple levels
        error_warning_logs = LogEntry.objects.filter(level__in=['ERROR', 'WARNING'])
        self.assertEqual(error_warning_logs.count(), 2)
    
    def test_script_run_status_filtering(self):
        """Test ScriptRun filtering by status."""
        # Create script runs with different statuses
        ScriptRunFactory(status='pending', script_name='script1.py')
        ScriptRunFactory(status='running', script_name='script2.py')
        ScriptRunFactory(status='completed', script_name='script3.py')
        ScriptRunFactory(status='failed', script_name='script4.py')
        
        # Test filtering by status
        running_scripts = ScriptRun.objects.filter(status='running')
        self.assertEqual(running_scripts.count(), 1)
        self.assertEqual(running_scripts.first().script_name, 'script2.py')
        
        # Test filtering for active scripts (pending or running)
        active_scripts = ScriptRun.objects.filter(status__in=['pending', 'running'])
        self.assertEqual(active_scripts.count(), 2) 