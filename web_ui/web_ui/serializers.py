"""
Serializers for DataHub Recipes Manager API
"""
from rest_framework import serializers
from .models import Connection, AppSettings


class ConnectionSerializer(serializers.ModelSerializer):
    """Serializer for DataHub Connection model"""
    
    class Meta:
        model = Connection
        fields = [
            'id', 'name', 'description', 'datahub_url', 
            'verify_ssl', 'timeout', 'is_active', 'is_default',
            'connection_status', 'last_tested', 'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'connection_status', 'last_tested', 'created_at', 'updated_at']
        extra_kwargs = {
            'datahub_token': {'write_only': True}  # Don't expose token in responses
        }


class PolicySettingsSerializer(serializers.Serializer):
    """Serializer for policy management settings"""
    export_dir = serializers.CharField(allow_blank=True, required=False)
    default_type = serializers.ChoiceField(choices=['METADATA', 'PLATFORM'], default='METADATA')
    validate_on_import = serializers.BooleanField(default=True)
    auto_backup = serializers.BooleanField(default=True)


class RecipeSettingsSerializer(serializers.Serializer):
    """Serializer for recipe management settings"""
    directory = serializers.CharField(allow_blank=True, required=False)
    default_schedule = serializers.CharField(default='0 0 * * *')
    auto_enable = serializers.BooleanField(default=False)


class AdvancedSettingsSerializer(serializers.Serializer):
    """Serializer for advanced system settings"""
    log_level = serializers.ChoiceField(
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], 
        default='INFO'
    )
    refresh_rate = serializers.IntegerField(min_value=0, max_value=3600, default=60)
    debug_mode = serializers.BooleanField(default=False)


class SettingsSerializer(serializers.Serializer):
    """Serializer for all application settings"""
    policy = PolicySettingsSerializer()
    recipe = RecipeSettingsSerializer()
    advanced = AdvancedSettingsSerializer()


class GitSettingsSerializer(serializers.Serializer):
    """Serializer for Git integration settings"""
    enabled = serializers.BooleanField(default=False)
    provider_type = serializers.ChoiceField(
        choices=['github', 'gitlab', 'azure_devops', 'bitbucket', 'custom'],
        default='github'
    )
    base_url = serializers.URLField(allow_blank=True, required=False)
    username = serializers.CharField(max_length=255)
    repository = serializers.CharField(max_length=255)
    current_branch = serializers.CharField(max_length=255, allow_blank=True, required=False)
    is_configured = serializers.BooleanField(read_only=True)


class SystemInfoSerializer(serializers.Serializer):
    """Serializer for system information"""
    class SystemSerializer(serializers.Serializer):
        platform = serializers.CharField()
        python_version = serializers.CharField()
        django_version = serializers.CharField()
    
    class DatabaseSerializer(serializers.Serializer):
        total_connections = serializers.IntegerField()
        active_connections = serializers.IntegerField()
        has_default_connection = serializers.BooleanField()
    
    class FeaturesSerializer(serializers.Serializer):
        git_integration = serializers.BooleanField()
        multi_connection_support = serializers.BooleanField()
        api_documentation = serializers.BooleanField()
    
    system = SystemSerializer()
    database = DatabaseSerializer()
    features = FeaturesSerializer()


class ConnectionTestResultSerializer(serializers.Serializer):
    """Serializer for connection test results"""
    success = serializers.BooleanField()
    status = serializers.CharField()
    error_message = serializers.CharField(allow_null=True, required=False)
    last_tested = serializers.DateTimeField(allow_null=True, required=False)


class DashboardDataSerializer(serializers.Serializer):
    """Serializer for dashboard data"""
    recipes_count = serializers.IntegerField()
    policies_count = serializers.IntegerField()
    active_connections = serializers.IntegerField()
    recent_activities = serializers.ListField(child=serializers.DictField())


class PaginatedDataSerializer(serializers.Serializer):
    """Base serializer for paginated data"""
    count = serializers.IntegerField()
    next = serializers.URLField(allow_null=True)
    previous = serializers.URLField(allow_null=True)
    results = serializers.ListField(child=serializers.DictField())


class RecipeDataSerializer(PaginatedDataSerializer):
    """Serializer for recipe data with pagination"""
    pass


class PolicyDataSerializer(PaginatedDataSerializer):
    """Serializer for policy data with pagination"""
    pass


class TemplatePreviewSerializer(serializers.Serializer):
    """Serializer for template preview"""
    content = serializers.CharField()
    template_name = serializers.CharField()
    variables_applied = serializers.DictField()


class EnvironmentVariableTemplateSerializer(serializers.Serializer):
    """Serializer for environment variable template"""
    id = serializers.IntegerField()
    name = serializers.CharField()
    description = serializers.CharField(allow_blank=True)
    variables = serializers.DictField()
    created_at = serializers.DateTimeField()
    updated_at = serializers.DateTimeField()


class EnvironmentVariableInstanceSerializer(serializers.Serializer):
    """Serializer for environment variable instance"""
    id = serializers.IntegerField()
    template_id = serializers.IntegerField()
    name = serializers.CharField()
    environment = serializers.CharField()
    variables = serializers.DictField()
    created_at = serializers.DateTimeField()
    updated_at = serializers.DateTimeField()


class GitHubBranchSerializer(serializers.Serializer):
    """Serializer for GitHub branch information"""
    name = serializers.CharField()
    sha = serializers.CharField()
    is_default = serializers.BooleanField()


class GitHubDiffSerializer(serializers.Serializer):
    """Serializer for GitHub diff information"""
    files_changed = serializers.IntegerField()
    additions = serializers.IntegerField()
    deletions = serializers.IntegerField()
    diff_content = serializers.CharField()


class UserGroupSerializer(serializers.Serializer):
    """Serializer for DataHub users and groups"""
    users = serializers.ListField(child=serializers.DictField())
    groups = serializers.ListField(child=serializers.DictField())
    ownership_types = serializers.ListField(child=serializers.DictField()) 