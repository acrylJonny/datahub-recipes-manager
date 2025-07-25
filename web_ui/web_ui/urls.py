"""
URL configuration for web_ui project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""

from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from django.shortcuts import redirect
from web_ui import views as web_ui_views
from django.contrib.auth import views as auth_views
from django.views.generic import RedirectView
from drf_spectacular.views import SpectacularAPIView, SpectacularSwaggerView, SpectacularRedocView
from . import api_views


# Redirect to dashboard by default
def home_redirect(request):
    return redirect("dashboard")


urlpatterns = [
    path("admin/", admin.site.urls),
    # Authentication
    path("accounts/login/", RedirectView.as_view(url="/login/", permanent=True)),
    path(
        "login/",
        auth_views.LoginView.as_view(template_name="auth/login.html"),
        name="login",
    ),
    path("logout/", auth_views.LogoutView.as_view(next_page="login"), name="logout"),
    # Homepage and redirects
    path("", home_redirect, name="home"),
    path("dashboard/", web_ui_views.index, name="dashboard"),
    path("dashboard/data/", web_ui_views.dashboard_data, name="dashboard_data"),
    # Metadata Manager
    path("metadata/", include("metadata_manager.urls", namespace="metadata_manager")),
    # Recipe management
    path("recipes/", web_ui_views.recipes, name="recipes"),
    path("recipes/data/", web_ui_views.recipes_data, name="recipes_data"),
    path("recipes/create/", web_ui_views.recipe_create, name="recipe_create"),
    path("recipes/import/", web_ui_views.recipe_import, name="recipe_import"),
    path("recipes/edit/<str:recipe_id>/", web_ui_views.recipe_edit, name="recipe_edit"),
    path(
        "recipes/delete/<str:recipe_id>/",
        web_ui_views.recipe_delete,
        name="recipe_delete",
    ),
    path("recipes/run/<str:recipe_id>/", web_ui_views.recipe_run, name="recipe_run"),
    path(
        "recipes/download/<str:recipe_id>/",
        web_ui_views.recipe_download,
        name="recipe_download",
    ),
    path(
        "recipes/export-all/",
        web_ui_views.export_all_recipes,
        name="export_all_recipes",
    ),
    # Recipe Templates
    path("recipes/templates/", include("template_manager.urls")),
    # Policy management
    path("policies/", web_ui_views.policies, name="policies"),
    path("policies/data/", web_ui_views.policies_data, name="policies_data"),
    path(
        "policies/detail/<str:policy_id>/", web_ui_views.policy_view, name="policy_view"
    ),
    path("policies/create/", web_ui_views.policy_create, name="policy_create"),
    path("policies/import/", web_ui_views.policy_import, name="policy_import"),
    path(
        "policies/edit/<str:policy_id>/", web_ui_views.policy_edit, name="policy_edit"
    ),
    path(
        "policies/delete/<str:policy_id>/",
        web_ui_views.policy_delete,
        name="policy_delete",
    ),
    path(
        "policies/download/<str:policy_id>/",
        web_ui_views.policy_download,
        name="policy_download",
    ),
    path(
        "policies/export-all/",
        web_ui_views.export_all_policies,
        name="export_all_policies",
    ),
    path(
        "policies/<str:policy_id>/push-github/",
        web_ui_views.policy_push_github,
        name="policy_push_github",
    ),
    path(
        "policies/<str:policy_id>/deploy/",
        web_ui_views.policy_deploy,
        name="policy_deploy",
    ),
    # Logs
    path("logs/", web_ui_views.logs, name="logs"),
    path("refresh-logs/", web_ui_views.refresh_logs, name="refresh_logs"),
    # Settings
    path("settings/", web_ui_views.settings, name="settings"),
    # API Documentation
    path("api/schema/", SpectacularAPIView.as_view(), name="schema"),
    path("api/docs/", SpectacularSwaggerView.as_view(url_name="schema"), name="swagger-ui"),
    path("api/redoc/", SpectacularRedocView.as_view(url_name="schema"), name="redoc"),
    # API Endpoints
    path("api/settings/", api_views.get_settings, name="api-settings"),
    path("api/settings/git/", api_views.get_git_settings, name="api-git-settings"),
    path("api/settings/system/", api_views.get_system_info, name="api-system-info"),
    path("api/connections/", api_views.list_connections, name="api-connections"),
    path("api/connections/<int:connection_id>/", api_views.get_connection, name="api-connection-detail"),
    path("api/connections/<int:connection_id>/test/", api_views.test_connection, name="api-connection-test"),
    path("api/connections/switch/", api_views.switch_connection, name="api-connection-switch"),
    # Templates and Environment Variables
    path("api/templates/<int:template_id>/preview/", api_views.recipe_template_preview, name="api-template-preview"),
    path("api/templates/<int:template_id>/env-vars/", api_views.template_env_vars_instances, name="api-template-env-vars"),
    path("api/env-vars/templates/", api_views.env_vars_template_list, name="api-env-vars-templates"),
    path("api/env-vars/templates/<int:template_id>/", api_views.env_vars_template_get, name="api-env-vars-template"),
    path("api/env-vars/instances/", api_views.env_vars_instance_list, name="api-env-vars-instances"),
    path("api/env-vars/instances/<int:instance_id>/json/", api_views.env_vars_instance_json, name="api-env-vars-instance-json"),
    # GitHub Integration
    path("api/github/branches/", api_views.github_load_branches, name="api-github-branches"),
    path("api/github/branch-diff/", api_views.github_branch_diff, name="api-github-branch-diff"),
    path("api/github/file-diff/", api_views.github_file_diff, name="api-github-file-diff"),
    # Metadata Management
    path("api/metadata/users-groups/", api_views.get_users_and_groups, name="api-metadata-users-groups"),
    # Dashboard and Data
    path("api/dashboard/data/", api_views.dashboard_data, name="api-dashboard-data"),
    path("api/recipes/data/", api_views.recipes_data, name="api-recipes-data"),
    path("api/policies/data/", api_views.policies_data, name="api-policies-data"),
    # DataHub Connections
    path("connections/", web_ui_views.connections_list, name="connections_list"),
    path("connections/create/", web_ui_views.connection_create, name="connection_create"),
    path("connections/<int:connection_id>/edit/", web_ui_views.connection_edit, name="connection_edit"),
    path("connections/<int:connection_id>/delete/", web_ui_views.connection_delete, name="connection_delete"),
    path("connections/test-all/", web_ui_views.test_all_connections, name="test_all_connections"),
    path("connections/<int:connection_id>/test/", web_ui_views.connection_test, name="connection_test"),
    path("connections/<int:connection_id>/set-default/", web_ui_views.connection_set_default, name="connection_set_default"),
    path("api/switch-connection/", web_ui_views.api_switch_connection, name="api_switch_connection"),
    # Environment Variables Templates
    path(
        "env-vars/templates/",
        web_ui_views.env_vars_templates,
        name="env_vars_templates",
    ),
    path(
        "env-vars/templates/create/",
        web_ui_views.env_vars_template_create,
        name="env_vars_template_create",
    ),
    path(
        "env-vars/templates/<int:template_id>/edit/",
        web_ui_views.env_vars_template_edit,
        name="env_vars_template_edit",
    ),
    path(
        "env-vars/templates/list/",
        web_ui_views.env_vars_template_list,
        name="env_vars_template_list",
    ),
    path(
        "env-vars/templates/get/<int:template_id>/",
        web_ui_views.env_vars_template_get,
        name="env_vars_template_get",
    ),
    path(
        "env-vars/templates/delete/<int:template_id>/",
        web_ui_views.env_vars_template_delete,
        name="env_vars_template_delete",
    ),
    path(
        "env-vars/templates/<int:template_id>/details/",
        web_ui_views.env_vars_template_details,
        name="env_vars_template_details",
    ),
    # Environment Variables Instances
    path(
        "env-vars/instances/",
        web_ui_views.env_vars_instances,
        name="env_vars_instances",
    ),
    path(
        "env-vars/instances/create/",
        web_ui_views.env_vars_instance_create,
        name="env_vars_instance_create",
    ),
    path(
        "env-vars/instances/<int:instance_id>/",
        web_ui_views.env_vars_instance_detail,
        name="env_vars_instance_detail",
    ),
    path(
        "env-vars/instances/<int:instance_id>/edit/",
        web_ui_views.env_vars_instance_edit,
        name="env_vars_instance_edit",
    ),
    path(
        "env-vars/instances/<int:instance_id>/delete/",
        web_ui_views.env_vars_instance_delete,
        name="env_vars_instance_delete",
    ),
    path(
        "env-vars/instances/list/",
        web_ui_views.env_vars_instance_list,
        name="env_vars_instance_list",
    ),
    path(
        "env-vars/instances/<int:instance_id>/json/",
        web_ui_views.env_vars_instance_json,
        name="env_vars_instance_json",
    ),
    # Health check
    path("health/", web_ui_views.health, name="health"),
    # Recipe Instances
    path("recipe-instances/", web_ui_views.recipe_instances, name="recipe_instances"),
    path(
        "recipe-instances/create/",
        web_ui_views.recipe_instance_create,
        name="recipe_instance_create",
    ),
    path(
        "recipe-instances/<int:instance_id>/edit/",
        web_ui_views.recipe_instance_edit,
        name="recipe_instance_edit",
    ),
    path(
        "recipe-instances/<int:instance_id>/delete/",
        web_ui_views.recipe_instance_delete,
        name="recipe_instance_delete",
    ),
    path(
        "recipe-instances/<int:instance_id>/deploy/",
        web_ui_views.recipe_instance_deploy,
        name="recipe_instance_deploy",
    ),
    path(
        "recipe-instances/<int:instance_id>/undeploy/",
        web_ui_views.recipe_instance_undeploy,
        name="recipe_instance_undeploy",
    ),
    path(
        "recipe-instances/<int:instance_id>/redeploy/",
        web_ui_views.recipe_instance_redeploy,
        name="recipe_instance_redeploy",
    ),
    path(
        "recipe-instances/<int:instance_id>/download/",
        web_ui_views.recipe_instance_download,
        name="recipe_instance_download",
    ),
    # API endpoints for recipe templates
    path(
        "api/recipe-templates/<int:template_id>/preview/",
        web_ui_views.recipe_template_preview,
        name="recipe_template_preview",
    ),
    path(
        "api/recipe-templates/<int:template_id>/env-vars-instances/",
        web_ui_views.template_env_vars_instances,
        name="template_env_vars_instances",
    ),
    # GitHub integration
    path("github/", web_ui_views.github_index, name="github_index"),
    path("github/settings/", web_ui_views.github_settings_view, name="github_settings"),
    path(
        "github/pull-requests/",
        web_ui_views.github_pull_requests,
        name="github_pull_requests",
    ),
    path(
        "github/pull-requests/<int:pr_id>/",
        web_ui_views.github_pull_request_detail,
        name="github_pull_request_detail",
    ),
    path(
        "github/switch-branch/<path:branch_name>/",
        web_ui_views.github_switch_branch,
        name="github_switch_branch",
    ),
    path(
        "github/test-connection/",
        web_ui_views.github_test_connection,
        name="github_test_connection",
    ),
    path(
        "github/create-branch/",
        web_ui_views.github_create_branch,
        name="github_create_branch",
    ),
    path("github/branches/", web_ui_views.github_branches, name="github_branches"),
    path(
        "github/delete-branch/",
        web_ui_views.github_delete_branch,
        name="github_delete_branch",
    ),
    path(
        "github/sync-recipes/",
        web_ui_views.github_sync_recipes,
        name="github_sync_recipes",
    ),
    path(
        "github/sync-status/",
        web_ui_views.github_sync_status,
        name="github_sync_status",
    ),
    path(
        "github/update-pr-status/<int:pr_number>/",
        web_ui_views.github_update_pr_status,
        name="github_update_pr_status",
    ),
    path(
        "github/delete-pr/<int:pr_id>/",
        web_ui_views.github_delete_pr,
        name="github_delete_pr",
    ),
    path("github/create-pr/", web_ui_views.github_create_pr, name="github_create_pr"),
    path(
        "github/push-changes/",
        web_ui_views.github_push_changes,
        name="github_push_changes",
    ),
    path(
        "github/fetch-prs/",
        web_ui_views.github_fetch_prs,
        name="github_fetch_prs",
    ),
    path(
        "github/branch-diff/",
        web_ui_views.github_branch_diff,
        name="github_branch_diff",
    ),
    path("github/file-diff/", web_ui_views.github_file_diff, name="github_file_diff"),
    path(
        "github/workflows-overview/",
        web_ui_views.github_workflows_overview,
        name="github_workflows_overview",
    ),
    path(
        "github/load-branches/",
        web_ui_views.github_load_branches,
        name="github_load_branches",
    ),
    path(
        "github/revert-staged-file/",
        web_ui_views.github_revert_staged_file,
        name="github_revert_staged_file",
    ),
    path("github/repo/", web_ui_views.github_repo_integration, name="github_repo"),
    # GitHub Secrets management
    path("github/secrets/", web_ui_views.github_secrets, name="github_secrets"),
    path(
        "github/secrets/create/",
        web_ui_views.github_create_secret,
        name="github_create_secret",
    ),
    path(
        "github/secrets/delete/",
        web_ui_views.github_delete_secret,
        name="github_delete_secret",
    ),
    # GitHub Environments management
    path(
        "github/environments/",
        web_ui_views.github_environments,
        name="github_environments",
    ),
    path(
        "github/environments/create/",
        web_ui_views.github_create_environment,
        name="github_create_environment",
    ),
    # Recipe instance and template GitHub push endpoints
    path(
        "recipe-instances/<int:instance_id>/push-github/",
        web_ui_views.recipe_instance_push_github,
        name="recipe_instance_push_github",
    ),
    path(
        "recipe-templates/<int:template_id>/push-github/",
        web_ui_views.recipe_template_push_github,
        name="recipe_template_push_github",
    ),
    # Policy and environment variables GitHub push endpoints
    path(
        "policies/<str:policy_id>/push-github/",
        web_ui_views.policy_push_github,
        name="policy_push_github",
    ),
    path(
        "env-vars/instances/<int:instance_id>/push-github/",
        web_ui_views.env_vars_instance_push_github,
        name="env_vars_instance_push_github",
    ),
    path(
        "env-vars/templates/<int:template_id>/push-github/",
        web_ui_views.env_vars_template_push_github,
        name="env_vars_template_push_github",
    ),
    # Environment management
    path("environments/", web_ui_views.environments, name="environments"),
    path("environments/list/", web_ui_views.environments, name="environments_list"),
    path(
        "environments/create/",
        web_ui_views.environment_create,
        name="environment_create",
    ),
    path(
        "environments/<int:env_id>/edit/",
        web_ui_views.environment_edit,
        name="environment_edit",
    ),
    path(
        "environments/<int:env_id>/delete/",
        web_ui_views.environment_delete,
        name="environment_delete",
    ),
    path(
        "environments/<int:env_id>/set-default/",
        web_ui_views.set_default_environment,
        name="set_default_environment",
    ),
    # Mutation management
    path("mutations/", web_ui_views.mutations, name="mutations"),
    path(
        "mutations/create/",
        web_ui_views.mutation_create,
        name="mutation_create",
    ),
    path(
        "mutations/<int:mutation_id>/edit/",
        web_ui_views.mutation_edit,
        name="mutation_edit",
    ),
    path(
        "mutations/<int:mutation_id>/delete/",
        web_ui_views.mutation_delete,
        name="mutation_delete",
    ),

]

# Add media files URL
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
