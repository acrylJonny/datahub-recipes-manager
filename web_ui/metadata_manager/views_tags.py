from django.shortcuts import render, redirect, get_object_or_404
from django.views import View
from django.contrib import messages
from django.http import JsonResponse
import json
import logging
import os
import sys
from django.views.decorators.http import require_POST
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone

# Add project root to sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

# Import the deterministic URN utilities
from utils.urn_utils import get_full_urn_from_name
from utils.datahub_utils import get_datahub_client, test_datahub_connection
from utils.data_sanitizer import sanitize_api_response
from web_ui.models import GitSettings, Environment
from .models import Tag

logger = logging.getLogger(__name__)


class TagListView(View):
    """View to list and create tags"""

    def get(self, request):
        """Display list of tags"""
        try:
            logger.info("Starting TagListView.get")

            # Get all local tags (database-only operation)
            tags = Tag.objects.all().order_by("name")
            logger.debug(f"Found {tags.count()} total tags")

            # Check DataHub connection (quick test)
            logger.debug("Testing DataHub connection from TagListView")
            connected, client = test_datahub_connection()
            logger.debug(f"DataHub connection test result: {connected}")

            # Initialize basic context with local data only
            context = {
                "tags": tags,
                "page_title": "DataHub Tags",
                "has_datahub_connection": connected,
                "has_git_integration": False,
                # Initialize empty lists - will be populated via AJAX
                "synced_tags": [],
                "local_tags": tags,  # Show all as local initially
                "remote_only_tags": [],
                "datahub_url": None,
            }

            # Check if git integration is enabled
            try:
                github_settings = GitSettings.objects.first()
                context["has_git_integration"] = (
                    github_settings and github_settings.enabled
                )
                logger.debug(
                    f"Git integration enabled: {context['has_git_integration']}"
                )
            except Exception as e:
                logger.warning(f"Error checking git integration: {str(e)}")
                pass

            logger.info("Rendering tag list template (async loading)")
            return render(request, "metadata_manager/tags/list.html", context)
        except Exception as e:
            logger.error(f"Error in tag list view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return render(
                request,
                "metadata_manager/tags/list.html",
                {"error": str(e), "page_title": "DataHub Tags"},
            )

    def post(self, request):
        """Create a new tag"""
        try:
            name = request.POST.get("name")
            description = request.POST.get("description", "")
            color = request.POST.get("color", "#0d6efd")
            owners = request.POST.getlist("owners")  # Get list of selected owners
            ownership_types = request.POST.getlist("ownership_types")  # Get list of ownership types

            # If color is empty, set to default color rather than None
            if not color or color.strip() == "":
                color = "#0d6efd"  # Default bootstrap primary color

            if not name:
                messages.error(request, "Tag name is required")
                return redirect("metadata_manager:tag_list")

            # Generate deterministic URN
            deterministic_urn = get_full_urn_from_name("tag", name)

            # Check if tag with this URN already exists
            if Tag.objects.filter(deterministic_urn=deterministic_urn).exists():
                messages.error(request, f"Tag with name '{name}' already exists")
                return redirect("metadata_manager:tag_list")

            # Create the tag
            tag = Tag.objects.create(
                name=name,
                description=description,
                color=color,
                deterministic_urn=deterministic_urn,
                sync_status="LOCAL_ONLY",
            )

            # Store owners information if provided
            if owners and ownership_types:
                # Filter out empty values and pair owners with ownership types
                valid_owners = []
                for i, owner in enumerate(owners):
                    if owner.strip() and i < len(ownership_types) and ownership_types[i].strip():
                        valid_owners.append({
                            'owner_urn': owner.strip(),
                            'ownership_type_urn': ownership_types[i].strip()
                        })
                
                if valid_owners:
                    # Store owners with ownership types as JSON data
                    tag.ownership_data = {
                        'owners': valid_owners,
                        'owners_count': len(valid_owners)
                    }
                    tag.owners_count = len(valid_owners)
                    tag.save(update_fields=['ownership_data', 'owners_count'])

            messages.success(request, f"Tag '{name}' created successfully" + 
                           (f" with {len(valid_owners)} owner(s)" if 'valid_owners' in locals() and valid_owners else ""))
            return redirect("metadata_manager:tag_list")
        except Exception as e:
            logger.error(f"Error creating tag: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:tag_list")


class TagDetailView(View):
    """View to display, edit and delete tags"""

    def get(self, request, tag_id):
        """Display tag details"""
        try:
            tag = get_object_or_404(Tag, id=tag_id)

            # Initialize context with tag data
            context = {
                "tag": tag,
                "page_title": f"Tag: {tag.name}",
                "has_git_integration": False,  # Set this based on checking GitHub settings
            }

            # Check if git integration is enabled
            try:
                github_settings = GitSettings.objects.first()
                context["has_git_integration"] = (
                    github_settings and github_settings.enabled
                )
            except:
                pass

            # Get related entities if DataHub connection is available
            client = get_datahub_client()
            if client and client.test_connection():
                tag_urn = tag.deterministic_urn

                # Find entities with this tag, limit to 50 for performance
                try:
                    related_entities = client.find_entities_with_metadata(
                        field_type="tags", metadata_urn=tag_urn, count=50
                    )

                    # Add to context
                    context["related_entities"] = related_entities.get("entities", [])
                    context["total_related"] = related_entities.get("total", 0)
                    context["has_datahub_connection"] = True

                    # Also add URL for reference
                    if hasattr(client, "server_url"):
                        context["datahub_url"] = client.server_url

                    # Get remote tag information if possible
                    if tag.sync_status != "LOCAL_ONLY":
                        try:
                            remote_tag = client.get_tag(tag_urn)
                            if remote_tag:
                                context["remote_tag"] = remote_tag

                                # Check if the tag needs to be synced
                                local_description = tag.description or ""
                                remote_description = remote_tag.get("description", "")

                                properties = remote_tag.get("properties", {}) or {}
                                remote_color = properties.get("colorHex", "#0d6efd")
                                local_color = tag.color or "#0d6efd"

                                # If different, mark as modified
                                if (
                                    local_description != remote_description
                                    or local_color != remote_color
                                ) and tag.sync_status != "MODIFIED":
                                    tag.sync_status = "MODIFIED"
                                    tag.save(update_fields=["sync_status"])

                                # If the same but marked as modified, update to synced
                                elif (
                                    local_description == remote_description
                                    and local_color == remote_color
                                    and tag.sync_status == "MODIFIED"
                                ):
                                    tag.sync_status = "SYNCED"
                                    tag.save(update_fields=["sync_status"])
                        except Exception as e:
                            logger.warning(
                                f"Error fetching remote tag information: {str(e)}"
                            )
                except Exception as e:
                    logger.error(
                        f"Error fetching related entities for tag {tag.name}: {str(e)}"
                    )
                    context["has_datahub_connection"] = (
                        True  # We still have a connection, just failed to get entities
                    )
                    context["related_entities_error"] = str(e)
            else:
                context["has_datahub_connection"] = False

            return render(request, "metadata_manager/tags/detail.html", context)
        except Exception as e:
            logger.error(f"Error in tag detail view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:tag_list")

    def post(self, request, tag_id):
        """Update a tag"""
        try:
            tag = get_object_or_404(Tag, id=tag_id)

            name = request.POST.get("name")
            description = request.POST.get("description", "")
            color = request.POST.get("color", tag.color)

            # If color is empty, set to default color rather than None
            if not color or color.strip() == "":
                color = "#0d6efd"  # Default bootstrap primary color

            if not name:
                messages.error(request, "Tag name is required")
                return redirect("metadata_manager:tag_detail", tag_id=tag.id)

            # If name changed, update deterministic URN
            if name != tag.name:
                new_urn = get_full_urn_from_name("tag", name)
                # Check if another tag with this URN already exists
                if (
                    Tag.objects.filter(deterministic_urn=new_urn)
                    .exclude(id=tag.id)
                    .exists()
                ):
                    messages.error(request, f"Tag with name '{name}' already exists")
                    return redirect("metadata_manager:tag_detail", tag_id=tag.id)
                tag.deterministic_urn = new_urn

            # If this tag exists remotely and we're changing details, mark as modified
            if tag.sync_status in ["SYNCED", "REMOTE_ONLY"] and (
                tag.description != description or tag.color != color
            ):
                tag.sync_status = "MODIFIED"

            # Update the tag
            tag.name = name
            tag.description = description
            tag.color = color
            tag.save()

            messages.success(request, f"Tag '{name}' updated successfully")
            return redirect("metadata_manager:tag_detail", tag_id=tag.id)
        except Exception as e:
            logger.error(f"Error updating tag: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:tag_detail", tag_id=tag.id)

    def delete(self, request, tag_id):
        """Delete a tag"""
        try:
            tag = get_object_or_404(Tag, id=tag_id)
            name = tag.name

            # If this tag exists remotely, try to delete it there too
            if tag.sync_status in ["SYNCED", "REMOTE_ONLY", "MODIFIED"]:
                client = get_datahub_client()
                if client:
                    try:
                        result = client.delete_tag(tag.deterministic_urn)
                        if result:
                            logger.info(
                                f"Successfully deleted tag {tag.name} from DataHub"
                            )
                    except Exception as e:
                        logger.warning(f"Failed to delete tag from DataHub: {str(e)}")

            # Delete the local tag regardless
            tag.delete()

            return JsonResponse(
                {"success": True, "message": f"Tag '{name}' deleted successfully"}
            )
        except Exception as e:
            logger.error(f"Error deleting tag: {str(e)}")
            return JsonResponse(
                {"success": False, "message": f"An error occurred: {str(e)}"},
                status=500,
            )


class TagImportExportView(View):
    """View to handle tag import and export"""

    def get(self, request):
        """Display import/export page or export all tags"""
        try:
            return render(
                request,
                "metadata_manager/tags/import_export.html",
                {"page_title": "Import/Export Tags"},
            )
        except Exception as e:
            logger.error(f"Error in tag import/export view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:tag_list")

    def post(self, request):
        """Import tags from JSON file"""
        try:
            import_file = request.FILES.get("import_file")
            if not import_file:
                messages.error(request, "No file was uploaded")
                return redirect("metadata_manager:tag_import_export")

            # Read the file
            try:
                tag_data = json.loads(import_file.read().decode("utf-8"))
            except json.JSONDecodeError:
                messages.error(request, "Invalid JSON file")
                return redirect("metadata_manager:tag_import_export")

            # Process tags
            imported_count = 0
            errors = []

            for i, tag_item in enumerate(tag_data):
                try:
                    name = tag_item.get("name")
                    description = tag_item.get("description", "")
                    color = tag_item.get("color", "#0d6efd")

                    if not name:
                        errors.append(f"Item #{i + 1}: Missing required field 'name'")
                        continue

                    # Generate deterministic URN
                    deterministic_urn = get_full_urn_from_name("tag", name)

                    # Check if tag exists, update it if it does
                    tag, created = Tag.objects.update_or_create(
                        deterministic_urn=deterministic_urn,
                        defaults={
                            "name": name,
                            "description": description,
                            "color": color,
                        },
                    )

                    imported_count += 1
                except Exception as e:
                    errors.append(f"Item #{i + 1}: {str(e)}")

            # Report results
            if imported_count > 0:
                messages.success(
                    request, f"Successfully imported {imported_count} tags"
                )

            if errors:
                messages.warning(
                    request, f"Encountered {len(errors)} errors during import"
                )
                for error in errors[:5]:  # Show the first 5 errors
                    messages.error(request, error)
                if len(errors) > 5:
                    messages.error(request, f"... and {len(errors) - 5} more errors")

            return redirect("metadata_manager:tag_list")
        except Exception as e:
            logger.error(f"Error importing tags: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:tag_import_export")


@method_decorator(require_POST, name="dispatch")
class TagDeployView(View):
    """View to deploy a tag to DataHub"""

    def post(self, request, tag_id):
        """Deploy a tag to DataHub"""
        try:
            # Get the tag
            tag = get_object_or_404(Tag, id=tag_id)

            # Get DataHub client
            client = get_datahub_client()
            if not client:
                messages.error(
                    request,
                    "Could not connect to DataHub. Check your connection settings.",
                )
                return redirect("metadata_manager:tag_list")

            # Deploy to DataHub
            # Ensure the deterministic_urn is a string before splitting
            tag_id_portion = (
                str(tag.deterministic_urn).split(":")[-1]
                if tag.deterministic_urn
                else None
            )
            if not tag_id_portion:
                messages.error(request, "Invalid tag URN")
                return redirect("metadata_manager:tag_detail", tag_id=tag.id)

            # Create or update the tag in DataHub
            result = client.create_tag(
                tag_id=tag_id_portion, name=tag.name, description=tag.description
            )

            if result:
                # Set color if specified and not empty
                if tag.color and tag.color.strip():
                    client.set_tag_color(result, tag.color)

                # Update tag with remote info
                tag.original_urn = result
                tag.datahub_id = tag_id_portion
                tag.sync_status = "SYNCED"
                tag.last_synced = timezone.now()
                tag.save()

                messages.success(
                    request, f"Tag '{tag.name}' successfully deployed to DataHub"
                )
            else:
                messages.error(request, "Failed to deploy tag to DataHub")

            # Redirect based on source
            redirect_url = request.POST.get("redirect_url", None)
            if redirect_url:
                return redirect(redirect_url)
            else:
                return redirect("metadata_manager:tag_detail", tag_id=tag.id)

        except Exception as e:
            logger.error(f"Error deploying tag: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:tag_list")


@method_decorator(require_POST, name="dispatch")
class TagPullView(View):
    """View to handle pulling tags from DataHub (POST only)"""

    def get(self, request, only_post=False):
        """Redirect to tag list for GET requests"""
        # We no longer want to show a separate page for pulling tags
        messages.info(request, "Use the 'Pull from DataHub' button on the Tags page")
        return redirect("metadata_manager:tag_list")

    def post(self, request, only_post=False):
        """Pull tags from DataHub"""
        try:
            # Get client from central utility
            client = get_datahub_client()

            if not client:
                messages.error(
                    request,
                    "Could not connect to DataHub. Check your connection settings.",
                )
                return redirect("metadata_manager:tag_list")

            # Test the connection
            if not client.test_connection():
                messages.error(
                    request,
                    "Could not connect to DataHub. Check your connection settings.",
                )
                return redirect("metadata_manager:tag_list")

            # Check if a specific tag URN was provided
            specific_tag_urn = request.POST.get("tag_urn")

            # Process and import tags
            imported_count = 0
            updated_count = 0
            error_count = 0

            if specific_tag_urn:
                # Import a single tag
                try:
                    # Get the tag data from the form instead of making an API call
                    tag_name = request.POST.get("tag_name")
                    tag_description = request.POST.get("tag_description", "")
                    tag_color_hex = request.POST.get("tag_color_hex", "")

                    if not tag_name:
                        messages.error(request, "Tag name is required for import")
                        return redirect("metadata_manager:tag_list")

                    # Create tag data dictionary from form data
                    tag_data = {
                        "urn": specific_tag_urn,
                        "name": tag_name,
                        "description": tag_description,
                        "properties": {
                            "colorHex": tag_color_hex if tag_color_hex else None
                        },
                    }

                    # Process the tag
                    result = self._process_tag(client, tag_data)
                    if result == "imported":
                        imported_count += 1
                        messages.success(
                            request,
                            f"Successfully imported tag '{tag_name}' from DataHub",
                        )
                    elif result == "updated":
                        updated_count += 1
                        messages.success(
                            request,
                            f"Successfully updated tag '{tag_name}' from DataHub",
                        )
                    elif result == "error":
                        error_count += 1
                        messages.error(
                            request, f"Error importing tag '{tag_name}' from DataHub"
                        )
                except Exception as e:
                    logger.error(
                        f"Error processing tag with URN {specific_tag_urn}: {str(e)}"
                    )
                    messages.error(request, f"Error importing tag: {str(e)}")
                    error_count += 1
            else:
                # Import all tags
                tags = client.list_tags(query="*", count=1000)

                for tag_data in tags:
                    try:
                        result = self._process_tag(client, tag_data)
                        if result == "imported":
                            imported_count += 1
                        elif result == "updated":
                            updated_count += 1
                        elif result == "error":
                            error_count += 1
                    except Exception as e:
                        logger.error(
                            f"Error processing tag {tag_data.get('name')}: {str(e)}"
                        )
                    error_count += 1

                # Report bulk import results
            if imported_count > 0:
                messages.success(
                    request,
                    f"Successfully imported {imported_count} new tags from DataHub",
                )

            if updated_count > 0:
                messages.info(request, f"Updated {updated_count} existing tags")

            if error_count > 0:
                messages.warning(
                    request, f"Encountered {error_count} errors during tag import"
                )

            if imported_count == 0 and updated_count == 0 and error_count == 0:
                messages.info(request, "No tags found in DataHub")

            return redirect("metadata_manager:tag_list")
        except Exception as e:
            logger.error(f"Error pulling tags from DataHub: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:tag_list")

    def _process_tag(self, client, tag_data):
        """Process a single tag from DataHub. Returns 'imported', 'updated', or 'error'."""
        # Extract the tag ID from the URN
        tag_urn = tag_data.get("urn")
        if not tag_urn:
            logger.warning(f"Skipping tag without URN: {tag_data}")
            return "error"

        name = tag_data.get("name")
        if not name:
            logger.warning(f"Skipping tag without name: {tag_data}")
            return "error"

        # Extract properties
        properties = tag_data.get("properties", {}) or {}  # Handle None case
        color_hex = properties.get("colorHex")  # Will be None if not present

        # Use a default color if none provided (to satisfy NOT NULL constraint)
        if color_hex is None or color_hex.strip() == "":
            color_hex = "#0d6efd"  # Default bootstrap primary color

        description = tag_data.get("description", "")

        # Try to find an existing tag with the same URN
        # Ensure tag_urn is a string for database query
        existing_tag = Tag.objects.filter(deterministic_urn=str(tag_urn)).first()

        if existing_tag:
            # Update the existing tag
            existing_tag.name = name
            existing_tag.description = description
            existing_tag.color = color_hex  # Now has default value if none provided
            existing_tag.original_urn = tag_urn
            existing_tag.sync_status = "SYNCED"
            existing_tag.last_synced = timezone.now()
            existing_tag.save()
            return "updated"
        else:
            # Create a new tag
            # Ensure tag_urn is a string before attempting to split it
            tag_id = str(tag_urn).split(":")[-1] if tag_urn else None

            Tag.objects.create(
                name=name,
                description=description,
                color=color_hex,  # Now has default value if none provided
                deterministic_urn=tag_urn,
                original_urn=tag_urn,
                datahub_id=tag_id,
                sync_status="SYNCED",
                last_synced=timezone.now(),
            )
            return "imported"


@method_decorator(csrf_exempt, name="dispatch")
class TagGitPushView(View):
    """View to handle adding a tag to a GitHub PR"""

    def post(self, request, tag_id):
        """Add tag to GitHub PR"""
        try:
            tag = get_object_or_404(Tag, id=tag_id)

            # Check if Git integration is enabled
            try:
                github_settings = GitSettings.objects.first()
                if not github_settings or not github_settings.enabled:
                    return JsonResponse(
                        {"success": False, "error": "Git integration is not enabled"}
                    )
            except Exception as e:
                logger.error(f"Error checking GitHub settings: {str(e)}")
                return JsonResponse(
                    {"success": False, "error": "Git integration is not available"}
                )

            # Convert tag to JSON format for Git
            tag_data = tag.to_dict()

            try:
                # Get default environment or use current

                # Try to stage the file in Git
                from web_ui.views import github_add_file_to_staging

                file_path = f"metadata/tags/{tag.name}.json"
                content = json.dumps(tag_data, indent=2)

                result = github_add_file_to_staging(
                    file_path, content, f"Added tag {tag.name}"
                )

                if result and result.get("success"):
                    # Update tag status
                    tag.staged_for_git = True
                    tag.sync_status = "PENDING_PUSH"
                    tag.save()

                    return JsonResponse({"success": True})
                else:
                    error_msg = result.get("error", "Unknown error")
                    return JsonResponse({"success": False, "error": error_msg})

            except Exception as e:
                logger.error(f"Error adding tag to Git PR: {str(e)}")
                return JsonResponse({"success": False, "error": str(e)})

        except Exception as e:
            logger.error(f"Error in tag Git push view: {str(e)}")
            return JsonResponse(
                {"success": False, "message": f"An error occurred: {str(e)}"},
                status=500,
            )


@method_decorator(csrf_exempt, name="dispatch")
class TagEntityView(View):
    """View to handle applying tags to entities"""

    def post(self, request):
        """Apply a tag to an entity"""
        try:
            # Parse JSON request
            data = json.loads(request.body)
            entity_urn = data.get("entity_urn")
            tag_urn = data.get("tag_urn")
            color_hex = data.get("color_hex")

            if not entity_urn or not tag_urn:
                return JsonResponse(
                    {
                        "success": False,
                        "message": "Entity URN and Tag URN are required",
                    },
                    status=400,
                )

            # Get DataHub client
            client = get_datahub_client()
            if not client or not client.test_connection():
                return JsonResponse(
                    {"success": False, "message": "Unable to connect to DataHub"},
                    status=500,
                )

            # Apply tag to entity with optional color
            if color_hex and color_hex.strip():
                result = client.add_tag_to_entity(entity_urn, tag_urn, color_hex)
            else:
                result = client.add_tag_to_entity(entity_urn, tag_urn, None)

            if result:
                logger.info(
                    f"Successfully applied tag {tag_urn} to entity {entity_urn}"
                )
                return JsonResponse(
                    {"success": True, "message": "Tag applied successfully"}
                )
            else:
                logger.error(f"Failed to apply tag {tag_urn} to entity {entity_urn}")
                return JsonResponse(
                    {"success": False, "message": "Failed to apply tag"}, status=500
                )

        except json.JSONDecodeError:
            return JsonResponse(
                {"success": False, "message": "Invalid JSON data"}, status=400
            )
        except Exception as e:
            logger.error(f"Error applying tag to entity: {str(e)}")
            return JsonResponse(
                {"success": False, "message": f"An error occurred: {str(e)}"},
                status=500,
            )

    def delete(self, request):
        """Remove a tag from an entity"""
        try:
            # Parse JSON request
            data = json.loads(request.body)
            entity_urn = data.get("entity_urn")
            tag_urn = data.get("tag_urn")

            if not entity_urn or not tag_urn:
                return JsonResponse(
                    {
                        "success": False,
                        "message": "Entity URN and Tag URN are required",
                    },
                    status=400,
                )

            # Get DataHub client
            client = get_datahub_client()
            if not client or not client.test_connection():
                return JsonResponse(
                    {"success": False, "message": "Unable to connect to DataHub"},
                    status=500,
                )

            # Remove tag from entity
            result = client.remove_tag_from_entity(entity_urn, tag_urn)

            if result:
                logger.info(
                    f"Successfully removed tag {tag_urn} from entity {entity_urn}"
                )
                return JsonResponse(
                    {"success": True, "message": "Tag removed successfully"}
                )
            else:
                logger.error(f"Failed to remove tag {tag_urn} from entity {entity_urn}")
                return JsonResponse(
                    {"success": False, "message": "Failed to remove tag"}, status=500
                )

        except json.JSONDecodeError:
            return JsonResponse(
                {"success": False, "message": "Invalid JSON data"}, status=400
            )
        except Exception as e:
            logger.error(f"Error removing tag from entity: {str(e)}")
            return JsonResponse(
                {"success": False, "message": f"An error occurred: {str(e)}"},
                status=500,
            )


def get_users_and_groups(request):
    """Get users and groups from DataHub for owner selection with database caching."""
    try:
        if request.method != 'POST':
            return JsonResponse({
                "success": False,
                "error": "Only POST method allowed"
            })
        
        data = json.loads(request.body)
        request_type = data.get('type', 'all')  # Support 'users', 'groups', 'ownership_types', or 'all'
        force_refresh = data.get('force_refresh', False)
        
        from .models import DataHubUser, DataHubGroup, DataHubOwnershipType
        from django.utils import timezone
        from datetime import timedelta
        
        # Check cache age (refresh if older than 5 minutes)
        cache_expiry = timezone.now() - timedelta(minutes=5)
        
        if request_type in ['users', 'all']:
            users_need_refresh = force_refresh or not DataHubUser.objects.filter(last_updated__gte=cache_expiry).exists()
        else:
            users_need_refresh = False
            
        if request_type in ['groups', 'all']:
            groups_need_refresh = force_refresh or not DataHubGroup.objects.filter(last_updated__gte=cache_expiry).exists()
        else:
            groups_need_refresh = False
            
        if request_type in ['ownership_types', 'all']:
            ownership_types_need_refresh = force_refresh or not DataHubOwnershipType.objects.filter(last_updated__gte=cache_expiry).exists()
        else:
            ownership_types_need_refresh = False
        
        # If cache is fresh, return cached data
        if not users_need_refresh and not groups_need_refresh and not ownership_types_need_refresh:
            logger.info("Using cached users, groups, and ownership types data")
            
            result_data = {}
            if request_type in ['users', 'all']:
                users = DataHubUser.objects.all().values('urn', 'username', 'display_name', 'email')
                result_data['users'] = list(users)
            
            if request_type in ['groups', 'all']:
                groups = DataHubGroup.objects.all().values('urn', 'display_name', 'description')
                result_data['groups'] = list(groups)
                
            if request_type in ['ownership_types', 'all']:
                ownership_types = DataHubOwnershipType.objects.all().values('urn', 'name')
                result_data['ownership_types'] = list(ownership_types)
            
            return JsonResponse({
                "success": True,
                "data": result_data if request_type == 'all' else result_data.get(request_type, []),
                "cached": True
            })
        
        # Need to refresh from DataHub
        logger.info("Refreshing users and groups from DataHub")
        
        # Get DataHub configuration from AppSettings
        from web_ui.models import AppSettings
        import os
        
        datahub_url = AppSettings.get("datahub_url", os.environ.get("DATAHUB_GMS_URL", ""))
        datahub_token = AppSettings.get("datahub_token", os.environ.get("DATAHUB_TOKEN", ""))
        
        if not datahub_url:
            return JsonResponse({
                "success": False,
                "error": "DataHub URL is not configured. Please configure it in the Configuration tab."
            })
        
        # Initialize DataHub client  
        from utils.datahub_rest_client import DataHubRestClient
        client = DataHubRestClient(datahub_url, datahub_token)
        
        # Test connection
        if not client.test_connection():
            return JsonResponse({
                "success": False,
                "error": "Cannot connect to DataHub"
            })
        
        result_data = {}
        
        # Fetch users if needed
        if users_need_refresh and request_type in ['users', 'all']:
            users_result = client.list_users(start=0, count=200)
            
            if users_result and "users" in users_result:
                users_data = users_result["users"]
                
                # Update database cache
                DataHubUser.objects.all().delete()  # Clear old cache
                
                users_to_create = []
                for user in users_data:
                    properties = user.get('properties') or {}
                    users_to_create.append(DataHubUser(
                        urn=user['urn'],
                        username=user.get('username', ''),
                        display_name=properties.get('displayName', ''),
                        email=properties.get('email', '')
                    ))
                
                DataHubUser.objects.bulk_create(users_to_create, ignore_conflicts=True)
                
                result_data['users'] = [
                    {
                        'urn': user['urn'],
                        'username': user.get('username', ''),
                        'display_name': user.get('properties', {}).get('displayName', ''),
                        'email': user.get('properties', {}).get('email', '')
                    }
                    for user in users_data
                ]
                
                logger.info(f"Cached {len(users_data)} users")
            else:
                logger.error("Failed to fetch users from DataHub")
                if request_type == 'users':
                    return JsonResponse({
                        "success": False,
                        "error": "Failed to fetch users from DataHub"
                    })
        
        # Fetch groups if needed
        if groups_need_refresh and request_type in ['groups', 'all']:
            groups_result = client.list_groups(start=0, count=200)
            
            if groups_result and "groups" in groups_result:
                groups_data = groups_result["groups"]
                
                # Update database cache
                DataHubGroup.objects.all().delete()  # Clear old cache
                
                groups_to_create = []
                for group in groups_data:
                    properties = group.get('properties') or {}
                    groups_to_create.append(DataHubGroup(
                        urn=group['urn'],
                        display_name=properties.get('displayName', ''),
                        description=properties.get('description', '')
                    ))
                
                DataHubGroup.objects.bulk_create(groups_to_create, ignore_conflicts=True)
                
                result_data['groups'] = [
                    {
                        'urn': group['urn'],
                        'display_name': group.get('properties', {}).get('displayName', ''),
                        'description': group.get('properties', {}).get('description', '')
                    }
                    for group in groups_data
                ]
                
                logger.info(f"Cached {len(groups_data)} groups")
            else:
                logger.error("Failed to fetch groups from DataHub")
                if request_type == 'groups':
                    return JsonResponse({
                        "success": False,
                        "error": "Failed to fetch groups from DataHub"
                    })
        
        # Fetch ownership types if needed
        if ownership_types_need_refresh and request_type in ['ownership_types', 'all']:
            ownership_types_result = client.list_ownership_types(start=0, count=100)
            
            if ownership_types_result and "ownershipTypes" in ownership_types_result:
                ownership_types_data = ownership_types_result["ownershipTypes"]
                
                # Update database cache
                DataHubOwnershipType.objects.all().delete()  # Clear old cache
                
                ownership_types_to_create = []
                for ownership_type in ownership_types_data:
                    info = ownership_type.get('info') or {}
                    ownership_types_to_create.append(DataHubOwnershipType(
                        urn=ownership_type['urn'],
                        name=info.get('name', '')
                    ))
                
                DataHubOwnershipType.objects.bulk_create(ownership_types_to_create, ignore_conflicts=True)
                
                result_data['ownership_types'] = [
                    {
                        'urn': ownership_type['urn'],
                        'name': ownership_type.get('info', {}).get('name', '')
                    }
                    for ownership_type in ownership_types_data
                ]
                
                logger.info(f"Cached {len(ownership_types_data)} ownership types")
            else:
                logger.error("Failed to fetch ownership types from DataHub")
                if request_type == 'ownership_types':
                    return JsonResponse({
                        "success": False,
                        "error": "Failed to fetch ownership types from DataHub"
                    })
        
        # Apply sanitization to prevent issues with long descriptions and malformed data
        sanitized_result = sanitize_api_response(result_data if request_type == 'all' else result_data.get(request_type, []))
        
        # Return fresh data
        return JsonResponse({
            "success": True,
            "data": sanitized_result,
            "cached": False
        })
            
    except Exception as e:
        logger.error(f"Error getting users and groups: {str(e)}")
        return JsonResponse({
            "success": False,
            "error": str(e)
        })


def get_remote_tags_data(request):
    """Get comprehensive tags data with local/remote/synced categorization and enhanced information."""
    try:
        logger.info("Loading comprehensive tags data via AJAX")
        
        # Get DataHub configuration from AppSettings (same as main views)
        from web_ui.models import AppSettings
        import os
        
        datahub_url = AppSettings.get("datahub_url", os.environ.get("DATAHUB_GMS_URL", ""))
        datahub_token = AppSettings.get("datahub_token", os.environ.get("DATAHUB_TOKEN", ""))
        
        if not datahub_url:
            return JsonResponse({
                "success": False,
                "error": "DataHub URL is not configured. Please configure it in the Configuration tab."
            })
        
        # Get parameters
        query = request.GET.get("query", "*")
        start = int(request.GET.get("start", 0))
        count = int(request.GET.get("count", 100))
        
        # Initialize DataHub client  
        from utils.datahub_rest_client import DataHubRestClient
        client = DataHubRestClient(datahub_url, datahub_token)
        
        # Test connection
        if not client.test_connection():
            return JsonResponse({
                "success": False,
                "error": "Cannot connect to DataHub"
            })
        
        # Get all local tags
        local_tags = Tag.objects.all().order_by("name")
        logger.debug(f"Found {local_tags.count()} local tags")
        
        # Get remote tags with enhanced data
        remote_tags_result = client.get_remote_tags_data(query=query, start=0, count=1000)
        
        if not remote_tags_result.get("success"):
            # Fallback if enhanced method fails
            remote_tags_list = client.list_tags(query=query, start=0, count=1000) or []
            remote_tags = {tag.get("urn"): tag for tag in remote_tags_list}
        else:
            remote_tags_list = remote_tags_result["data"]["tags"]
            remote_tags = {tag.get("urn"): tag for tag in remote_tags_list}
        
        logger.debug(f"Found {len(remote_tags)} remote tags")
        
        # Categorize tags
        synced_tags = []
        local_only_tags = []
        remote_only_tags = []
        
        # Extract local tag URNs
        local_tag_urns = set(local_tags.values_list("deterministic_urn", flat=True))
        
        # Process local tags and match with remote
        for local_tag in local_tags:
            tag_urn = str(local_tag.deterministic_urn)
            remote_match = remote_tags.get(tag_urn)
            
            local_tag_data = {
                "id": local_tag.id,
                "name": local_tag.name,
                "description": local_tag.description,
                "color": local_tag.color,
                "colorHex": local_tag.color,
                "urn": tag_urn,
                "sync_status": local_tag.sync_status,
                "sync_status_display": local_tag.get_sync_status_display(),
                # Add empty ownership and relationships for local-only tags
                "owners_count": 0,
                "owner_names": [],
                "relationships_count": 0,
                "ownership": None,
                "relationships": None,
            }
            
            if remote_match:
                # Check if tag needs status update
                local_description = local_tag.description or ""
                remote_description = remote_match.get("description", "")
                
                remote_color = remote_match.get("colorHex", "#0d6efd")
                local_color = local_tag.color or "#0d6efd"
                
                # Update sync status based on comparison
                if (local_description != remote_description or local_color != remote_color):
                    if local_tag.sync_status != "MODIFIED":
                        local_tag.sync_status = "MODIFIED"
                        local_tag.save(update_fields=["sync_status"])
                        logger.debug(f"Updated tag {local_tag.name} status to MODIFIED")
                else:
                    if local_tag.sync_status != "SYNCED":
                        local_tag.sync_status = "SYNCED" 
                        local_tag.save(update_fields=["sync_status"])
                        logger.debug(f"Updated tag {local_tag.name} status to SYNCED")
                
                # Update local tag data with remote information
                local_tag_data.update({
                    "sync_status": local_tag.sync_status,
                    "sync_status_display": local_tag.get_sync_status_display(),
                    "owners_count": remote_match.get("owners_count", 0),
                    "owner_names": remote_match.get("owner_names", []),
                    "relationships_count": remote_match.get("relationships_count", 0),
                    "ownership": remote_match.get("ownership"),
                    "relationships": remote_match.get("relationships"),
                })
                
                synced_tags.append({
                    "local": local_tag_data,
                    "remote": remote_match,
                    "combined": local_tag_data  # Enhanced data for display
                })
            else:
                # Ensure local-only tags have correct status
                if local_tag.sync_status != "LOCAL_ONLY":
                    local_tag.sync_status = "LOCAL_ONLY"
                    local_tag.save(update_fields=["sync_status"])
                    logger.debug(f"Updated tag {local_tag.name} status to LOCAL_ONLY")
                
                local_tag_data["sync_status"] = "LOCAL_ONLY"
                local_tag_data["sync_status_display"] = "Local Only"
                local_only_tags.append(local_tag_data)
        
        # Find remote-only tags
        for tag_urn, remote_tag in remote_tags.items():
            if tag_urn not in local_tag_urns:
                # Add enhanced remote tag data
                remote_tag_enhanced = {
                    **remote_tag,
                    "sync_status": "REMOTE_ONLY",
                    "sync_status_display": "Remote Only",
                }
                remote_only_tags.append(remote_tag_enhanced)
        
        # Calculate statistics
        total_tags = len(synced_tags) + len(local_only_tags) + len(remote_only_tags)
        owned_tags = sum(1 for tag_list in [synced_tags, local_only_tags, remote_only_tags] 
                        for tag in tag_list 
                        if (tag.get("combined", tag) if "combined" in tag else tag).get("owners_count", 0) > 0)
        deprecated_tags = sum(1 for tag_list in [synced_tags, local_only_tags, remote_only_tags]
                            for tag in tag_list
                            if (tag.get("combined", tag) if "combined" in tag else tag).get("deprecated", False) or 
                               (tag.get("combined", tag) if "combined" in tag else tag).get("properties", {}).get("deprecated", False))
        
        # Get DataHub URL for external links
        if datahub_url.endswith("/api/gms"):
            datahub_url = datahub_url[:-8]
        
        logger.debug(
            f"Categorized tags: {len(synced_tags)} synced, {len(local_only_tags)} local-only, {len(remote_only_tags)} remote-only"
        )
        
        # Prepare response data
        response_data = {
            "synced_tags": synced_tags,
            "local_only_tags": local_only_tags,
            "remote_only_tags": remote_only_tags,
            "datahub_url": datahub_url,
            "statistics": {
                "total_tags": total_tags,
                "synced_count": len(synced_tags),
                "local_count": len(local_only_tags),
                "remote_count": len(remote_only_tags),
                "owned_tags": owned_tags,
                "deprecated_tags": deprecated_tags,
                "percentage_owned": round((owned_tags / total_tags * 100) if total_tags else 0, 1),
            }
        }
        
        # Apply global sanitization to prevent issues with long descriptions and malformed data
        sanitized_data = sanitize_api_response(response_data)
        
        return JsonResponse({
            "success": True,
            "data": sanitized_data
        })
        
    except Exception as e:
        logger.error(f"Error getting comprehensive tags data: {str(e)}")
        return JsonResponse({
            "success": False,
            "error": str(e)
        })


@method_decorator(csrf_exempt, name="dispatch")
class TagSyncToLocalView(View):
    """API endpoint to sync a remote tag to local"""

    def post(self, request, tag_id):
        try:
            tag = get_object_or_404(Tag, id=tag_id)
            
            # Get DataHub client
            client = get_datahub_client()
            if not client:
                return JsonResponse({"error": "Not connected to DataHub"}, status=400)
            
            # Get the tag data from DataHub
            remote_tag = client.get_tag(tag.deterministic_urn)
            if not remote_tag:
                return JsonResponse({"error": "Tag not found in DataHub"}, status=404)

            # Update local tag with remote data
            tag.name = remote_tag.get("properties", {}).get("name", tag.name)
            tag.description = remote_tag.get("properties", {}).get("description", "")
            tag.color = remote_tag.get("properties", {}).get("colorHex", tag.color)
            tag.sync_status = "SYNCED"
            tag.last_synced = timezone.now()
            
            # Update ownership data if available
            if "ownership" in remote_tag:
                ownership_data = remote_tag.get("ownership", {})
                owners = ownership_data.get("owners", [])
                tag.owners_count = len(owners)
                # Make sure ownership_data is properly formatted
                if isinstance(ownership_data, dict):
                    tag.ownership_data = sanitize_api_response(ownership_data)
                else:
                    tag.ownership_data = {"owners": []}
            else:
                # Set default empty ownership data to avoid None issues
                tag.ownership_data = {"owners": []}
                tag.owners_count = 0
            
            tag.save()
            
            logger.info(f"Tag {tag.name} (ID: {tag_id}) synced to local")
            return JsonResponse({
                "status": "success",
                "message": "Tag synced successfully",
                "refresh_needed": True
            })
                
        except Exception as e:
            logger.error(f"Error syncing tag to local: {str(e)}")
            return JsonResponse({"error": str(e)}, status=500)


@method_decorator(csrf_exempt, name="dispatch")
class TagDownloadJsonView(View):
    """API endpoint to download tag data as JSON"""
    
    def get(self, request, tag_id):
        try:
            tag = get_object_or_404(Tag, id=tag_id)
            
            # Create JSON data
            tag_data = {
                "id": str(tag.id),
                "name": tag.name,
                "description": tag.description,
                "color": tag.color,
                "deterministic_urn": tag.deterministic_urn,
                "sync_status": tag.sync_status,
                "last_synced": tag.last_synced.isoformat() if tag.last_synced else None,
                "owners_count": tag.owners_count,
                "ownership_data": tag.ownership_data
            }
            
            # Add remote data if available
            client = get_datahub_client()
            if client:
                try:
                    remote_tag = client.get_tag(tag.deterministic_urn)
                    if remote_tag:
                        tag_data["remote"] = remote_tag
                except Exception as e:
                    logger.warning(f"Error fetching remote tag data: {str(e)}")
            
            # Return JSON response with content disposition header for download
            response = JsonResponse(tag_data, json_dumps_params={"indent": 2})
            response["Content-Disposition"] = f'attachment; filename="tag-{tag.name}.json"'
            return response
                
        except Exception as e:
            logger.error(f"Error generating tag JSON: {str(e)}")
            return JsonResponse({"error": str(e)}, status=500)


@method_decorator(csrf_exempt, name="dispatch")
class TagAddToStagedChangesView(View):
    """API endpoint to add a tag to staged changes"""
    
    def post(self, request, tag_id):
        try:
            import json
            import os
            import sys
            from pathlib import Path
            
            # Add project root to path to import our Python modules
            sys.path.append(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            )
            
            # Import the function
            from scripts.mcps.tag_actions import add_tag_to_staged_changes
            
            # Get tag and request data
            tag = get_object_or_404(Tag, id=tag_id)
            data = json.loads(request.body)
            
            # Get environment and mutation name
            environment_name = data.get('environment', 'dev')
            mutation_name = data.get('mutation_name')
            
            # Get current user as owner
            owner = request.user.username if request.user.is_authenticated else "admin"
            
            # Create tag data dictionary
            tag_data = {
                "id": str(tag.id),
                "name": tag.name,
                "description": tag.description,
                "color": tag.color,
                "urn": tag.deterministic_urn,
                "properties": {
                    "name": tag.name,
                    "description": tag.description,
                    "colorHex": tag.color
                }
            }
            
            if tag.ownership_data:
                tag_data["ownership"] = tag.ownership_data
            
            # Get environment
            try:
                environment = Environment.objects.get(name=environment_name)
            except Environment.DoesNotExist:
                # Create default environment if it doesn't exist
                environment = Environment.objects.create(
                    name=environment_name,
                    description=f"Auto-created {environment_name} environment"
                )
            
            # Build base directory path
            base_dir = Path(os.getcwd()) / "metadata-manager" / environment_name
            base_dir.mkdir(parents=True, exist_ok=True)
            
            # Add tag to staged changes
            result = add_tag_to_staged_changes(
                tag_data=tag_data,
                environment=environment_name,
                owner=owner,
                base_dir=str(base_dir),
                mutation_name=mutation_name
            )
            
            # Return success response
            return JsonResponse({
                "status": "success",
                "message": "Tag added to staged changes",
                "files_created": list(result.values())
            })
                
        except Exception as e:
            logger.error(f"Error adding tag to staged changes: {str(e)}")
            return JsonResponse({"error": str(e)}, status=500)


# Update URLs
def update_urls():
    """
    This function isn't part of the view but serves as documentation for URLs that need to be added:
    
    1. path(
        "api/metadata_manager/tags/<uuid:tag_id>/sync_to_local/",
        views_tags.TagSyncToLocalView.as_view(),
        name="tag_sync_to_local_api"
    )
    
    2. path(
        "api/metadata_manager/tags/<uuid:tag_id>/download/",
        views_tags.TagDownloadJsonView.as_view(),
        name="tag_download_json_api"
    )
    
    3. path(
        "api/metadata_manager/tags/<uuid:tag_id>/stage_changes/",
        views_tags.TagAddToStagedChangesView.as_view(),
        name="tag_add_to_staged_changes_api"
    )
    """
    pass
