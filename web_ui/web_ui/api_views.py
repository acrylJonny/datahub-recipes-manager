"""
API views for DataHub Recipes Manager
"""
from rest_framework import generics, status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiResponse
from drf_spectacular.openapi import OpenApiTypes
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from .models import Connection, AppSettings, GitSettings
from .serializers import (
    ConnectionSerializer, SettingsSerializer, GitSettingsSerializer, 
    SystemInfoSerializer, ConnectionTestResultSerializer
)


@extend_schema(
    tags=['Settings'],
    summary='Get application settings',
    description='Retrieve current application settings including policy, recipe, and advanced settings',
    responses={200: SettingsSerializer}
)
@api_view(['GET'])
def get_settings(request):
    """Get application settings"""
    settings = {
        'policy': {
            'export_dir': AppSettings.get('policy_export_dir', ''),
            'default_type': AppSettings.get('default_policy_type', 'METADATA'),
            'validate_on_import': AppSettings.get_bool('validate_on_import', True),
            'auto_backup': AppSettings.get_bool('auto_backup_policies', True),
        },
        'recipe': {
            'directory': AppSettings.get('recipe_dir', ''),
            'default_schedule': AppSettings.get('default_schedule', '0 0 * * *'),
            'auto_enable': AppSettings.get_bool('auto_enable_recipes', False),
        },
        'advanced': {
            'log_level': AppSettings.get('log_level', 'INFO'),
            'refresh_rate': AppSettings.get_int('refresh_rate', 60),
            'debug_mode': AppSettings.get_bool('debug_mode', False),
        }
    }
    return Response(settings)


@extend_schema(
    tags=['Connections'],
    summary='List DataHub connections',
    description='Get a list of all configured DataHub connections',
    responses={200: ConnectionSerializer(many=True)}
)
@api_view(['GET'])
def list_connections(request):
    """List all DataHub connections"""
    connections = Connection.objects.all().order_by('-is_default', 'name')
    serializer = ConnectionSerializer(connections, many=True)
    return Response(serializer.data)


@extend_schema(
    tags=['Connections'],
    summary='Get connection details',
    description='Get detailed information about a specific DataHub connection',
    parameters=[
        OpenApiParameter(
            name='connection_id',
            type=OpenApiTypes.INT,
            location=OpenApiParameter.PATH,
            description='ID of the connection to retrieve'
        )
    ],
    responses={
        200: ConnectionSerializer,
        404: OpenApiResponse(description='Connection not found')
    }
)
@api_view(['GET'])
def get_connection(request, connection_id):
    """Get connection details"""
    try:
        connection = Connection.objects.get(id=connection_id)
        serializer = ConnectionSerializer(connection)
        return Response(serializer.data)
    except Connection.DoesNotExist:
        return Response(
            {'error': 'Connection not found'}, 
            status=status.HTTP_404_NOT_FOUND
        )


@extend_schema(
    tags=['Connections'],
    summary='Test connection',
    description='Test connectivity to a DataHub instance',
    parameters=[
        OpenApiParameter(
            name='connection_id',
            type=OpenApiTypes.INT,
            location=OpenApiParameter.PATH,
            description='ID of the connection to test'
        )
    ],
    responses={
        200: ConnectionTestResultSerializer,
        404: OpenApiResponse(description='Connection not found')
    }
)
@api_view(['POST'])
def test_connection(request, connection_id):
    """Test DataHub connection"""
    try:
        connection = Connection.objects.get(id=connection_id)
        success = connection.test_connection()
        
        return Response({
            'success': success,
            'status': connection.connection_status,
            'error_message': connection.error_message if not success else None,
            'last_tested': connection.last_tested
        })
    except Connection.DoesNotExist:
        return Response(
            {'error': 'Connection not found'}, 
            status=status.HTTP_404_NOT_FOUND
        )


@extend_schema(
    tags=['Connections'],
    summary='Switch active connection',
    description='Switch the current connection context for the session',
    request={'application/json': {'connection_id': 'integer'}},
    responses={
        200: OpenApiResponse(description='Connection switched successfully'),
        400: OpenApiResponse(description='Invalid request or connection not found')
    }
)
@api_view(['POST'])
def switch_connection(request):
    """Switch active connection - wraps the existing api_switch_connection view"""
    from . import views as web_ui_views
    return web_ui_views.api_switch_connection(request)


@extend_schema(
    tags=['Settings'],
    summary='Get Git integration settings',
    description='Retrieve current Git repository integration settings',
    responses={200: GitSettingsSerializer}
)
@api_view(['GET'])
def get_git_settings(request):
    """Get Git integration settings"""
    git_settings = GitSettings.get_instance()
    
    return Response({
        'enabled': git_settings.enabled,
        'provider_type': git_settings.provider_type,
        'base_url': git_settings.base_url,
        'username': git_settings.username,
        'repository': git_settings.repository,
        'current_branch': git_settings.current_branch,
        'is_configured': git_settings.is_configured(),
    })


@extend_schema(
    tags=['Settings'],
    summary='Get system information',
    description='Get general system information and health status',
    responses={200: SystemInfoSerializer}
)
@api_view(['GET'])
def get_system_info(request):
    """Get system information"""
    from django.conf import settings as django_settings
    import platform
    
    # Get connection count
    connection_count = Connection.objects.count()
    active_connections = Connection.objects.filter(is_active=True).count()
    default_connection = Connection.get_default()
    
    return Response({
        'system': {
            'platform': platform.system(),
            'python_version': platform.python_version(),
            'django_version': django_settings.DJANGO_VERSION if hasattr(django_settings, 'DJANGO_VERSION') else 'Unknown',
        },
        'database': {
            'total_connections': connection_count,
            'active_connections': active_connections,
            'has_default_connection': default_connection is not None,
        },
        'features': {
            'git_integration': GitSettings.is_configured(),
            'multi_connection_support': True,
            'api_documentation': True,
        }
    })


@extend_schema(
    tags=['Templates'],
    summary='Preview recipe template',
    description='Preview a recipe template with environment variables applied',
    parameters=[
        OpenApiParameter(
            name='template_id',
            type=OpenApiTypes.INT,
            location=OpenApiParameter.PATH,
            description='ID of the template to preview'
        )
    ],
    responses={
        200: OpenApiResponse(description='Template preview content'),
        404: OpenApiResponse(description='Template not found')
    }
)
@api_view(['GET'])
def recipe_template_preview(request, template_id):
    """Preview recipe template - wraps the existing template manager view"""
    from template_manager import views as template_views
    return template_views.recipe_template_preview(request, template_id)


@extend_schema(
    tags=['Templates'],
    summary='Get template environment variable instances',
    description='Get environment variable instances for a specific template',
    parameters=[
        OpenApiParameter(
            name='template_id',
            type=OpenApiTypes.INT,
            location=OpenApiParameter.PATH,
            description='ID of the template'
        )
    ],
    responses={
        200: OpenApiResponse(description='Environment variable instances'),
        404: OpenApiResponse(description='Template not found')
    }
)
@api_view(['GET'])
def template_env_vars_instances(request, template_id):
    """Get template environment variable instances"""
    from . import views as web_ui_views
    return web_ui_views.template_env_vars_instances(request, template_id)


@extend_schema(
    tags=['Environment Variables'],
    summary='List environment variable templates',
    description='Get a list of all environment variable templates',
    responses={200: OpenApiResponse(description='List of environment variable templates')}
)
@api_view(['GET'])
def env_vars_template_list(request):
    """List environment variable templates"""
    from . import views as web_ui_views
    return web_ui_views.env_vars_template_list(request)


@extend_schema(
    tags=['Environment Variables'],
    summary='Get environment variable template',
    description='Get details of a specific environment variable template',
    parameters=[
        OpenApiParameter(
            name='template_id',
            type=OpenApiTypes.INT,
            location=OpenApiParameter.PATH,
            description='ID of the template'
        )
    ],
    responses={
        200: OpenApiResponse(description='Environment variable template details'),
        404: OpenApiResponse(description='Template not found')
    }
)
@api_view(['GET'])
def env_vars_template_get(request, template_id):
    """Get environment variable template"""
    from . import views as web_ui_views
    return web_ui_views.env_vars_template_get(request, template_id)


@extend_schema(
    tags=['Environment Variables'],
    summary='List environment variable instances',
    description='Get a list of all environment variable instances',
    responses={200: OpenApiResponse(description='List of environment variable instances')}
)
@api_view(['GET'])
def env_vars_instance_list(request):
    """List environment variable instances"""
    from . import views as web_ui_views
    return web_ui_views.env_vars_instance_list(request)


@extend_schema(
    tags=['Environment Variables'],
    summary='Get environment variable instance JSON',
    description='Get JSON representation of an environment variable instance',
    parameters=[
        OpenApiParameter(
            name='instance_id',
            type=OpenApiTypes.INT,
            location=OpenApiParameter.PATH,
            description='ID of the instance'
        )
    ],
    responses={
        200: OpenApiResponse(description='Environment variable instance JSON'),
        404: OpenApiResponse(description='Instance not found')
    }
)
@api_view(['GET'])
def env_vars_instance_json(request, instance_id):
    """Get environment variable instance JSON"""
    from . import views as web_ui_views
    return web_ui_views.env_vars_instance_json(request, instance_id)


@extend_schema(
    tags=['GitHub'],
    summary='Load GitHub branches',
    description='Load available branches from the configured GitHub repository',
    responses={200: OpenApiResponse(description='List of available branches')}
)
@csrf_exempt
@api_view(['GET', 'POST'])
def github_load_branches(request):
    """Load GitHub branches"""
    from . import views as web_ui_views
    return web_ui_views.github_load_branches(request)


@extend_schema(
    tags=['GitHub'],
    summary='Get branch diff',
    description='Get diff between branches in GitHub repository',
    responses={200: OpenApiResponse(description='Branch diff information')}
)
@csrf_exempt
@api_view(['GET', 'POST'])
def github_branch_diff(request):
    """Get GitHub branch diff"""
    from . import views as web_ui_views
    return web_ui_views.github_branch_diff(request)


@extend_schema(
    tags=['GitHub'],
    summary='Get file diff',
    description='Get diff for a specific file in GitHub repository',
    responses={200: OpenApiResponse(description='File diff information')}
)
@csrf_exempt
@api_view(['GET', 'POST'])
def github_file_diff(request):
    """Get GitHub file diff"""
    from . import views as web_ui_views
    return web_ui_views.github_file_diff(request)


@extend_schema(
    tags=['Metadata'],
    summary='Get users and groups',
    description='Get users and groups from DataHub for metadata management',
    responses={200: OpenApiResponse(description='List of users and groups')}
)
@api_view(['GET'])
def get_users_and_groups(request):
    """Get users and groups"""
    from metadata_manager import views_tags
    return views_tags.get_users_and_groups(request)


@extend_schema(
    tags=['Dashboard'],
    summary='Get dashboard data',
    description='Get data for the main dashboard including recipes and policies count',
    responses={200: OpenApiResponse(description='Dashboard data including counts and status')}
)
@api_view(['GET'])
def dashboard_data(request):
    """Get dashboard data"""
    from . import views as web_ui_views
    return web_ui_views.dashboard_data(request)


@extend_schema(
    tags=['Recipes'],
    summary='Get recipes data',
    description='Get data for recipes list with filtering and pagination',
    responses={200: OpenApiResponse(description='Recipes data with pagination')}
)
@api_view(['GET'])
def recipes_data(request):
    """Get recipes data"""
    from . import views as web_ui_views
    return web_ui_views.recipes_data(request)


@extend_schema(
    tags=['Policies'],
    summary='Get policies data',
    description='Get data for policies list with filtering and pagination',
    responses={200: OpenApiResponse(description='Policies data with pagination')}
)
@api_view(['GET'])
def policies_data(request):
    """Get policies data"""
    from . import views as web_ui_views
    return web_ui_views.policies_data(request) 