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

# Redirect to dashboard by default
def home_redirect(request):
    return redirect('dashboard')

urlpatterns = [
    path("admin/", admin.site.urls),
    
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
    
    # Recipe Templates
    path("recipes/templates/", web_ui_views.recipe_templates, name="recipe_templates"),
    path("recipes/templates/create/", web_ui_views.recipe_template_create, name="recipe_template_create"),
    path("recipes/templates/import/", web_ui_views.recipe_template_import, name="recipe_template_import"),
    path("recipes/templates/<int:template_id>/", web_ui_views.recipe_template_detail, name="recipe_template_detail"),
    path("recipes/templates/<int:template_id>/edit/", web_ui_views.recipe_template_edit, name="recipe_template_edit"),
    path("recipes/templates/<int:template_id>/delete/", web_ui_views.recipe_template_delete, name="recipe_template_delete"),
    path("recipes/templates/<int:template_id>/export/", web_ui_views.recipe_template_export, name="recipe_template_export"),
    path("recipes/templates/<int:template_id>/deploy/", web_ui_views.recipe_template_deploy, name="recipe_template_deploy"),
    path("recipes/save-as-template/<str:recipe_id>/", web_ui_views.recipe_save_as_template, name="recipe_save_as_template"),
    
    # Policy management
    path("policies/", web_ui_views.policies, name="policies"),
    path("policies/detail/<str:policy_id>/", web_ui_views.policy_view, name="policy_view"),
    path("policies/create/", web_ui_views.policy_create, name="policy_create"),
    path("policies/import/", web_ui_views.policy_import, name="policy_import"),
    path("policies/edit/<str:policy_id>/", web_ui_views.policy_edit, name="policy_edit"),
    path("policies/delete/<str:policy_id>/", web_ui_views.policy_delete, name="policy_delete"),
    path("policies/download/<str:policy_id>/", web_ui_views.policy_download, name="policy_download"),
    path("policies/export-all/", web_ui_views.policy_export_all, name="policy_export_all"),
    
    # Logs
    path("logs/", web_ui_views.logs, name="logs"),
    
    # Settings
    path("settings/", web_ui_views.settings, name="settings"),
    
    # Health check
    path("health/", web_ui_views.health, name="health"),
]

# Add media files URL
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
