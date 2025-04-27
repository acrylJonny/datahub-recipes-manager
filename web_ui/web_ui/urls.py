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

# Redirect to dashboard by default
def home_redirect(request):
    return redirect('dashboard')

urlpatterns = [
    path("admin/", admin.site.urls),
    
    # Authentication
    path("login/", auth_views.LoginView.as_view(template_name='auth/login.html'), name="login"),
    path("logout/", auth_views.LogoutView.as_view(next_page='login'), name="logout"),
    path("accounts/login/", auth_views.LoginView.as_view(template_name='auth/login.html')),  # Fallback for default Django auth
    
    # Homepage and redirects
    path("", home_redirect, name="home"),
    path("dashboard/", web_ui_views.index, name="dashboard"),
    
    # Recipe management
    path("recipes/", web_ui_views.recipes, name="recipes"),
    path("recipes/create/", web_ui_views.recipe_create, name="recipe_create"),
    path("recipes/import/", web_ui_views.recipe_import, name="recipe_import"),
    path("recipes/edit/<str:recipe_id>/", web_ui_views.recipe_edit, name="recipe_edit"),
    path("recipes/delete/<str:recipe_id>/", web_ui_views.recipe_delete, name="recipe_delete"),
    path("recipes/run/<str:recipe_id>/", web_ui_views.recipe_run, name="recipe_run"),
    path("recipes/download/<str:recipe_id>/", web_ui_views.recipe_download, name="recipe_download"),
    path("recipes/export-all/", web_ui_views.export_all_recipes, name="export_all_recipes"),
    
    # Recipe Templates
    path("recipes/templates/", web_ui_views.recipe_templates, name="recipe_templates"),
    path("recipes/templates/create/", web_ui_views.recipe_template_create, name="recipe_template_create"),
    path("recipes/templates/import/", web_ui_views.recipe_template_import, name="recipe_template_import"),
    path("recipes/templates/export-all/", web_ui_views.export_all_templates, name="export_all_templates"),
    path("recipes/templates/<int:template_id>/", web_ui_views.recipe_template_detail, name="recipe_template_detail"),
    path("recipes/templates/<int:template_id>/edit/", web_ui_views.recipe_template_edit, name="recipe_template_edit"),
    path("recipes/templates/<int:template_id>/delete/", web_ui_views.recipe_template_delete, name="recipe_template_delete"),
    path("recipes/templates/<int:template_id>/export/", web_ui_views.recipe_template_export, name="recipe_template_export"),
    path("recipes/templates/<int:template_id>/deploy/", web_ui_views.recipe_template_deploy, name="recipe_template_deploy"),
    path("recipes/save-as-template/<str:recipe_id>/", web_ui_views.recipe_save_as_template, name="recipe_template_save"),
    # path("recipes/convert-to-template-instance/<str:recipe_id>/", web_ui_views.recipe_convert_to_template_instance, name="recipe_convert_to_template_instance"),
    
    # Policy management
    path("policies/", web_ui_views.policies, name="policies"),
    path("policies/detail/<str:policy_id>/", web_ui_views.policy_view, name="policy_view"),
    path("policies/create/", web_ui_views.policy_create, name="policy_create"),
    path("policies/import/", web_ui_views.policy_import, name="policy_import"),
    path("policies/edit/<str:policy_id>/", web_ui_views.policy_edit, name="policy_edit"),
    path("policies/delete/<str:policy_id>/", web_ui_views.policy_delete, name="policy_delete"),
    path("policies/download/<str:policy_id>/", web_ui_views.policy_download, name="policy_download"),
    path("policies/export-all/", web_ui_views.export_all_policies, name="export_all_policies"),
    
    # Logs
    path("logs/", web_ui_views.logs, name="logs"),
    
    # Settings
    path("settings/", web_ui_views.settings, name="settings"),
    
    # Environment Variables Templates
    path("env-vars/templates/", web_ui_views.env_vars_templates, name="env_vars_templates"),
    path("env-vars/templates/create/", web_ui_views.env_vars_template_create, name="env_vars_template_create"),
    path("env-vars/templates/list/", web_ui_views.env_vars_template_list, name="env_vars_template_list"),
    path("env-vars/templates/get/<int:template_id>/", web_ui_views.env_vars_template_get, name="env_vars_template_get"),
    path("env-vars/templates/delete/<int:template_id>/", web_ui_views.env_vars_template_delete, name="env_vars_template_delete"),
    path("env-vars/templates/<int:template_id>/details/", web_ui_views.env_vars_template_details, name="env_vars_template_details"),
    
    # Environment Variables Instances
    path("env-vars/instances/", web_ui_views.env_vars_instances, name="env_vars_instances"),
    path("env-vars/instances/create/", web_ui_views.env_vars_instance_create, name="env_vars_instance_create"),
    path("env-vars/instances/<int:instance_id>/", web_ui_views.env_vars_instance_detail, name="env_vars_instance_detail"),
    path("env-vars/instances/<int:instance_id>/edit/", web_ui_views.env_vars_instance_edit, name="env_vars_instance_edit"),
    path("env-vars/instances/<int:instance_id>/delete/", web_ui_views.env_vars_instance_delete, name="env_vars_instance_delete"),
    path("env-vars/instances/list/", web_ui_views.env_vars_instance_list, name="env_vars_instance_list"),
    path("env-vars/instances/<int:instance_id>/json/", web_ui_views.env_vars_instance_json, name="env_vars_instance_json"),
    
    # Health check
    path("health/", web_ui_views.health, name="health"),

    # Recipe Instances
    path('recipe-instances/', web_ui_views.recipe_instances, name='recipe_instances'),
    path('recipe-instances/create/', web_ui_views.recipe_instance_create, name='recipe_instance_create'),
    path('recipe-instances/<int:instance_id>/edit/', web_ui_views.recipe_instance_edit, name='recipe_instance_edit'),
    path('recipe-instances/<int:instance_id>/delete/', web_ui_views.recipe_instance_delete, name='recipe_instance_delete'),
    path('recipe-instances/<int:instance_id>/deploy/', web_ui_views.recipe_instance_deploy, name='recipe_instance_deploy'),
    path('recipe-instances/<int:instance_id>/undeploy/', web_ui_views.recipe_instance_undeploy, name='recipe_instance_undeploy'),
    path('recipe-instances/<int:instance_id>/redeploy/', web_ui_views.recipe_instance_redeploy, name='recipe_instance_redeploy'),
    path('recipe-instances/<int:instance_id>/download/', web_ui_views.recipe_instance_download, name='recipe_instance_download'),

    # API endpoints for recipe templates
    path('api/recipe-templates/<int:template_id>/preview/', web_ui_views.recipe_template_preview, name='recipe_template_preview'),
    
    # GitHub integration
    path('github/', web_ui_views.github_index, name='github'),
    path('github/settings/', web_ui_views.github_settings_edit, name='github_settings_edit'),
    path('github/pull-requests/', web_ui_views.github_pull_requests, name='github_pull_requests'),
    path('github/test-connection/', web_ui_views.github_test_connection, name='github_test_connection'),
    path('github/create-branch/', web_ui_views.github_create_branch, name='github_create_branch'),
    path('github/sync-recipes/', web_ui_views.github_sync_recipes, name='github_sync_recipes'),
    path('github/sync-status/', web_ui_views.github_sync_status, name='github_sync_status'),
    path('github/pull-requests/<int:pr_number>/update/', web_ui_views.github_update_pr_status, name='github_update_pr_status'),
    path('github/pull-requests/<int:pr_id>/delete/', web_ui_views.github_delete_pr, name='github_delete_pr'),
]

# Add media files URL
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
