from django.contrib import admin
from django.urls import path, include
from django.shortcuts import redirect
from django.views.generic import RedirectView
from django.contrib.auth import views as auth_views
from web_ui.web_ui import views as web_ui_views


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
    # Settings
    path("settings/", web_ui_views.settings, name="settings"),
    # Environment Variables 
    path(
        "env-vars/templates/",
        web_ui_views.env_vars_templates,
        name="env_vars_templates",
    ),
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
    # GitHub integration
    path("github/", web_ui_views.github_index, name="github_index"),
    path("github/settings/", web_ui_views.github_settings_view, name="github_settings"),
    path("github/repo/", web_ui_views.github_repo_integration, name="github_repo"),
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
        "github/create-pr/",
        web_ui_views.github_create_pr,
        name="github_create_pr",
    ),
    path(
        "github/update-pr-status/<int:pr_number>/",
        web_ui_views.github_update_pr_status,
        name="github_update_pr_status",
    ),
    # Environments
    path("environments/", web_ui_views.environments, name="environments"),
    path(
        "environments/create/",
        web_ui_views.environment_create,
        name="environment_create",
    ),
    # Logs
    path("logs/", web_ui_views.logs, name="logs"),
    # Health check
    path("health/", web_ui_views.health, name="health"),
]
