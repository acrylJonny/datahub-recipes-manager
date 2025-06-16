from django.urls import path
from . import views

app_name = "template_manager"

urlpatterns = [
    # Recipe Templates
    path("", views.recipe_templates, name="recipe_templates"),
    path("create/", views.recipe_template_create, name="recipe_template_create"),
    path("import/", views.recipe_template_import, name="recipe_template_import"),
    path("export-all/", views.export_all_templates, name="export_all_templates"),
    path(
        "<int:template_id>/",
        views.recipe_template_detail,
        name="recipe_template_detail",
    ),
    path(
        "<int:template_id>/edit/",
        views.recipe_template_edit,
        name="recipe_template_edit",
    ),
    path(
        "<int:template_id>/delete/",
        views.recipe_template_delete,
        name="recipe_template_delete",
    ),
    path(
        "<int:template_id>/export/",
        views.recipe_template_export,
        name="recipe_template_export",
    ),
    path(
        "<int:template_id>/deploy/",
        views.recipe_template_deploy,
        name="recipe_template_deploy",
    ),
    path(
        "save-as-template/<str:recipe_id>/",
        views.recipe_save_as_template,
        name="recipe_template_save",
    ),
    # API endpoints for recipe templates
    path(
        "api/<int:template_id>/preview/",
        views.recipe_template_preview,
        name="recipe_template_preview",
    ),
    path(
        "api/<int:template_id>/env-vars-instances/",
        views.template_env_vars_instances,
        name="template_env_vars_instances",
    ),
]
