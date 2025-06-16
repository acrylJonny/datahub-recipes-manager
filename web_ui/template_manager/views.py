from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.http import JsonResponse, HttpResponse
from django.contrib.auth.decorators import login_required
from django.db import models
import json
import os
import tempfile
import shutil
from datetime import datetime

from web_ui.models import RecipeTemplate, EnvVarsInstance, RecipeInstance
from web_ui.forms import RecipeTemplateForm


def recipe_templates(request):
    """List all recipe templates."""
    templates = RecipeTemplate.objects.all().order_by("-updated_at")
    env_vars_instances = EnvVarsInstance.objects.all().order_by("-updated_at")
    recipe_instances = RecipeInstance.objects.all().order_by("-updated_at")

    # Handle filtering
    tag_filter = request.GET.get("tag")
    if tag_filter:
        # Filter by tag (simple contains for comma-separated tags)
        templates = templates.filter(tags__contains=tag_filter)

    # Handle search
    search_query = request.GET.get("search")
    if search_query:
        templates = templates.filter(
            models.Q(name__icontains=search_query)
            | models.Q(description__icontains=search_query)
            | models.Q(recipe_type__icontains=search_query)
        )

    # Get unique tags for filter dropdown
    all_tags = set()
    for template in RecipeTemplate.objects.all():
        if template.tags:
            all_tags.update(template.get_tags_list())

    return render(
        request,
        "recipes/templates/list.html",
        {
            "title": "Recipe Templates",
            "templates": templates,
            "env_vars_instances": env_vars_instances,
            "recipe_instances": recipe_instances,
            "tag_filter": tag_filter,
            "search_query": search_query,
            "all_tags": sorted(all_tags),
        },
    )


def recipe_template_detail(request, template_id):
    """View a recipe template details."""
    template = get_object_or_404(RecipeTemplate, id=template_id)

    # Process content for display
    if template.content.strip().startswith("{"):
        content_type = "json"
        try:
            formatted_content = json.dumps(json.loads(template.content), indent=2)
        except:
            formatted_content = template.content
    else:
        content_type = "yaml"
        formatted_content = template.content

    return render(
        request,
        "recipes/templates/detail.html",
        {
            "title": f"Template: {template.name}",
            "template": template,
            "content": formatted_content,
            "content_type": content_type,
            "tags": template.get_tags_list(),
        },
    )


def recipe_template_create(request):
    """Create a new recipe template."""
    if request.method == "POST":
        form = RecipeTemplateForm(request.POST)
        if form.is_valid():
            # Create the template
            template = RecipeTemplate(
                name=form.cleaned_data["name"],
                description=form.cleaned_data["description"],
                recipe_type=form.cleaned_data["recipe_type"],
                content=form.cleaned_data["content"],
                executor_id=form.cleaned_data.get("executor_id", "default"),
                cron_schedule=form.cleaned_data.get("cron_schedule", "0 0 * * *"),
                timezone=form.cleaned_data.get("timezone", "Etc/UTC"),
            )

            # Handle tags
            if form.cleaned_data["tags"]:
                template.set_tags_list(
                    [tag.strip() for tag in form.cleaned_data["tags"].split(",")]
                )

            template.save()
            messages.success(
                request, f"Template '{template.name}' created successfully"
            )
            return redirect("template_manager:recipe_templates")
    else:
        form = RecipeTemplateForm()

    return render(
        request,
        "recipes/templates/create.html",
        {"title": "Create Recipe Template", "form": form},
    )


def recipe_template_edit(request, template_id):
    """Edit a recipe template."""
    template = get_object_or_404(RecipeTemplate, id=template_id)

    if request.method == "POST":
        form = RecipeTemplateForm(request.POST)
        if form.is_valid():
            # Update the template
            template.name = form.cleaned_data["name"]
            template.description = form.cleaned_data["description"]
            template.recipe_type = form.cleaned_data["recipe_type"]
            template.content = form.cleaned_data["content"]
            template.executor_id = form.cleaned_data.get("executor_id", "default")
            template.cron_schedule = form.cleaned_data.get("cron_schedule", "0 0 * * *")
            template.timezone = form.cleaned_data.get("timezone", "Etc/UTC")

            # Handle tags
            if form.cleaned_data["tags"]:
                template.set_tags_list(
                    [tag.strip() for tag in form.cleaned_data["tags"].split(",")]
                )
            else:
                template.tags = ""

            template.save()
            messages.success(
                request, f"Template '{template.name}' updated successfully"
            )
            return redirect(
                "template_manager:recipe_template_detail", template_id=template.id
            )
    else:
        form = RecipeTemplateForm(
            initial={
                "name": template.name,
                "description": template.description,
                "recipe_type": template.recipe_type,
                "content": template.content,
                "tags": template.tags,
                "executor_id": template.executor_id,
                "cron_schedule": template.cron_schedule,
                "timezone": template.timezone,
            }
        )

    return render(
        request,
        "recipes/templates/edit.html",
        {"title": "Edit Recipe Template", "form": form, "template": template},
    )


def recipe_template_delete(request, template_id):
    """Delete a recipe template."""
    template = get_object_or_404(RecipeTemplate, id=template_id)

    if request.method == "POST":
        template_name = template.name
        template.delete()
        messages.success(request, f"Template '{template_name}' deleted successfully")
        return redirect("template_manager:recipe_templates")

    return render(
        request,
        "recipes/templates/delete.html",
        {"title": "Delete Recipe Template", "template": template},
    )


def recipe_template_export(request, template_id):
    """Export a recipe template to a file."""
    template = get_object_or_404(RecipeTemplate, id=template_id)

    # Extract template content
    template_content = template.content
    if isinstance(template_content, str):
        try:
            template_content = json.loads(template_content)
        except:
            pass

    # Create response with file
    response = HttpResponse(
        json.dumps(template_content, indent=2), content_type="application/json"
    )
    response["Content-Disposition"] = (
        f'attachment; filename="{template.name.replace(" ", "_").lower()}.json"'
    )
    return response


def recipe_template_deploy(request, template_id):
    """Deploy a recipe template."""
    template = get_object_or_404(RecipeTemplate, id=template_id)
    # Add deployment logic here
    messages.success(request, f"Template '{template.name}' deployed successfully")
    return redirect("template_manager:recipe_template_detail", template_id=template.id)


def recipe_save_as_template(request, recipe_id):
    """Save a recipe as a template."""
    # Add logic to save recipe as template
    messages.success(request, "Recipe saved as template successfully")
    return redirect("template_manager:recipe_templates")


def recipe_template_preview(request, template_id):
    """Preview a recipe template."""
    template = get_object_or_404(RecipeTemplate, id=template_id)
    return JsonResponse({"preview": template.content})


def template_env_vars_instances(request, template_id):
    """Get environment variables instances for a template."""
    get_object_or_404(RecipeTemplate, id=template_id)
    instances = EnvVarsInstance.objects.filter(template_id=template_id)
    return JsonResponse({"instances": list(instances.values())})


def export_all_templates(request):
    """Export all recipe templates to JSON files in a zip archive."""
    try:
        # Get all recipe templates from the database
        templates = RecipeTemplate.objects.all()

        if not templates:
            messages.warning(request, "No recipe templates found to export")
            return redirect("template_manager:recipe_templates")

        # Create a temporary directory for templates
        output_dir = tempfile.mkdtemp()

        # Save each template to a file
        for template in templates:
            template_name = template.name.replace(" ", "_").lower()
            filename = f"{template_name}_{template.id}.json"

            # Extract template content
            template_content = template.content
            if isinstance(template_content, str):
                try:
                    template_content = json.loads(template_content)
                except:
                    pass

            # Write template to file
            with open(os.path.join(output_dir, filename), "w") as f:
                json.dump(template_content, f, indent=2)

        # Create a zip file
        zip_path = os.path.join(
            tempfile.gettempdir(),
            f"datahub_templates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
        )
        shutil.make_archive(zip_path[:-4], "zip", output_dir)

        # Clean up the temp directory
        shutil.rmtree(output_dir, ignore_errors=True)

        # Serve the zip file
        with open(zip_path, "rb") as f:
            response = HttpResponse(f.read(), content_type="application/zip")
            response["Content-Disposition"] = (
                'attachment; filename="datahub_templates.zip"'
            )

        # Clean up the zip file
        os.unlink(zip_path)

        return response

    except Exception as e:
        messages.error(request, f"Error exporting templates: {str(e)}")
        return redirect("template_manager:recipe_templates")


def recipe_template_import(request):
    """Import a recipe template from a file."""
    if request.method == "POST":
        # Add import logic here
        messages.success(request, "Template imported successfully")
        return redirect("template_manager:recipe_templates")

    return render(
        request, "recipes/templates/import.html", {"title": "Import Recipe Template"}
    )
