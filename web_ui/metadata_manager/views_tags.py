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
import uuid
from datetime import datetime
from pathlib import Path
from django.db import connection

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


def sanitize_ownership_data(data):
    """
    Specifically sanitize ownership data to ensure it's properly formatted for database storage.
    
    Args:
        data: The ownership data to sanitize
        
    Returns:
        Sanitized ownership data with consistent structure
    """
    if not data:
        return {"owners": []}
    
    if isinstance(data, dict):
        # Check if this already has the correct structure with owners
        if "owners" in data and isinstance(data["owners"], list):
            # This is the standard DataHub ownership format
            sanitized_data = {}
            
            # Preserve the owners array
            sanitized_data["owners"] = _sanitize_for_json(data["owners"])
            
            # Preserve other ownership metadata if present
            if "lastModified" in data:
                sanitized_data["lastModified"] = _sanitize_for_json(data["lastModified"])
            
            # Preserve any other fields in the ownership object
            for key, value in data.items():
                if key not in ["owners", "lastModified"]:
                    sanitized_data[key] = _sanitize_for_json(value)
            
            return sanitized_data
        
        # If no owners key, try to find owners in other keys
        for key, value in data.items():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                # This looks like an owners list
                return {"owners": _sanitize_for_json(value)}
        
        # No suitable owners list found, but preserve the structure
        return _sanitize_for_json(data)
    
    elif isinstance(data, list):
        # Direct list of owners
        return {"owners": _sanitize_for_json(data)}
    
    # Default empty structure
    return {"owners": []}


def _sanitize_for_json(obj):
    """Basic JSON sanitization for internal use."""
    if obj is None:
        return None
    
    if isinstance(obj, (str, int, float, bool)):
        return obj
    
    if isinstance(obj, list):
        return [_sanitize_for_json(item) for item in obj]
    
    if isinstance(obj, dict):
        sanitized = {}
        for key, value in obj.items():
            if callable(value):
                continue
            sanitized[str(key)] = _sanitize_for_json(value)
        return sanitized
    
    try:
        return str(obj)
    except Exception:
        return f"<{type(obj).__name__} object>"

# Helper function to find a tag by flexible ID lookup
def find_tag_by_flexible_id(tag_id):
    """
    Find a tag using multiple ID formats (id, datahub_id, urn, or formatted UUID)
    Returns (tag, None) if found, or (None, error_response) if not found
    """
    try:
        # First try with the ID as is
        tag = Tag.objects.get(id=tag_id)
        return tag, None
    except (Tag.DoesNotExist, ValueError):
        try:
            # Try with the ID as datahub_id
            tag = Tag.objects.get(datahub_id=tag_id)
            return tag, None
        except Tag.DoesNotExist:
            try:
                # Try with the ID as URN
                tag = Tag.objects.get(urn=tag_id)
                return tag, None
            except Tag.DoesNotExist:
                # If we still can't find it, try with a formatted UUID
                try:
                    # Try to format the ID as a UUID
                    formatted_id = str(uuid.UUID(tag_id.replace('-', '')))
                    tag = Tag.objects.get(id=formatted_id)
                    return tag, None
                except (Tag.DoesNotExist, ValueError):
                    # If all attempts fail, return error
                    logger.error(f"Tag not found with any ID format: {tag_id}")
                    error_response = JsonResponse(
                        {"success": False, "message": f"Tag not found with ID: {tag_id}"},
                        status=404,
                    )
                    return None, error_response

def check_ownership_data_column_exists():
    """
    Check if the ownership_data column exists in the metadata_manager_tag table.
    
    Returns:
        bool: True if the column exists, False otherwise
    """
    from django.db import connection
    
    try:
        with connection.cursor() as cursor:
            # For PostgreSQL
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'metadata_manager_tag' 
                AND column_name = 'ownership_data'
            """)
            result = cursor.fetchone()
            
            if result:
                logger.info("ownership_data column exists in the database")
                return True
            else:
                logger.critical("ownership_data column DOES NOT exist in the database!")
                return False
    except Exception as e:
        logger.error(f"Error checking for ownership_data column: {str(e)}")
        return False

def check_for_pending_migrations():
    """
    Check if there are any pending migrations related to the ownership_data field.
    
    Returns:
        bool: True if there are pending migrations, False otherwise
    """
    try:
        from django.db.migrations.recorder import MigrationRecorder
        from django.db import connection
        
        # Get the list of applied migrations
        recorder = MigrationRecorder(connection)
        applied_migrations = set(
            (migration.app, migration.name) 
            for migration in recorder.migration_qs.all()
        )
        
        # Check for specific migrations related to ownership_data
        ownership_migrations = [
            ('metadata_manager', '0002_rename_urn_fields_and_remove_counts'),
            ('metadata_manager', '0003_remove_assertion_relationships_count_and_more')
        ]
        
        for app, name in ownership_migrations:
            if (app, name) not in applied_migrations:
                logger.critical(f"Migration {app}.{name} has not been applied!")
                return True
        
        logger.info("All ownership_data migrations have been applied")
        return False
    except Exception as e:
        logger.error(f"Error checking for pending migrations: {str(e)}")
        return False

def ensure_ownership_data_column_exists():
    """
    Ensure the ownership_data column exists in the metadata_manager_tag table.
    If it doesn't exist, create it.
    
    Returns:
        bool: True if the column exists or was created successfully, False otherwise
    """
    from django.db import connection
    
    try:
        # First check if the column exists
        if check_ownership_data_column_exists():
            return True
        
        # If not, create it
        logger.critical("Attempting to create ownership_data column...")
        with connection.cursor() as cursor:
            # For PostgreSQL
            cursor.execute("""
                ALTER TABLE metadata_manager_tag 
                ADD COLUMN IF NOT EXISTS ownership_data JSONB NULL
            """)
            
        # Verify the column was created
        return check_ownership_data_column_exists()
    except Exception as e:
        logger.error(f"Error creating ownership_data column: {str(e)}")
        return False

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
            owners = request.POST.getlist("owners[]")  # Get list of selected owners
            ownership_types = request.POST.getlist("ownership_types[]")  # Get list of ownership types
            
            logger.debug(f"Tag creation form data: name={name}, owners={owners}, ownership_types={ownership_types}")
            
            # Also try alternate form of ownership params (forms can send them differently)
            if not owners:
                # Try without [] suffix which some form processors remove
                owners = request.POST.getlist("owners")
                ownership_types = request.POST.getlist("ownership_types")
                logger.debug(f"Alternate ownership params: owners={owners}, ownership_types={ownership_types}")

            # If color is empty, set to default color rather than None
            if not color or color.strip() == "":
                color = "#0d6efd"  # Default bootstrap primary color

            if not name:
                messages.error(request, "Tag name is required")
                return redirect("metadata_manager:tag_list")

            # Generate deterministic URN
            deterministic_urn = get_full_urn_from_name("tag", name)

            # Check if tag with this URN already exists
            if Tag.objects.filter(urn=deterministic_urn).exists():
                messages.error(request, f"Tag with name '{name}' already exists")
                return redirect("metadata_manager:tag_list")

            # Create the tag
            tag = Tag.objects.create(
                name=name,
                description=description,
                color=color,
                urn=deterministic_urn,
                sync_status="LOCAL_ONLY",
            )

            # Store owners information if provided
            if owners and ownership_types:
                # Filter out empty values and pair owners with ownership types
                valid_owners = []
                logger.debug(f"Processing ownership data: {len(owners)} owners and {len(ownership_types)} types")
                
                for i, owner in enumerate(owners):
                    if owner and owner.strip() and i < len(ownership_types) and ownership_types[i] and ownership_types[i].strip():
                        valid_owners.append({
                            'owner_urn': owner.strip(),
                            'ownership_type_urn': ownership_types[i].strip()
                        })
                        logger.debug(f"Added valid owner: {owner.strip()} with type {ownership_types[i].strip()}")
                
                if valid_owners:
                    # Store owners with ownership types as JSON data
                    tag.ownership_data = {
                        'owners': valid_owners
                    }
                    tag.save(update_fields=['ownership_data'])
                    logger.info(f"Saved {len(valid_owners)} owners for tag {name}")

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
                tag_urn = tag.urn

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
                    Tag.objects.filter(urn=new_urn)
                    .exclude(id=tag.id)
                    .exists()
                ):
                    messages.error(request, f"Tag with name '{name}' already exists")
                    return redirect("metadata_manager:tag_detail", tag_id=tag.id)
                tag.urn = new_urn

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

            # We only delete the local tag, not the remote one on DataHub
            logger.info(f"Deleting local tag only, not affecting remote tag on DataHub")

            # Delete the local tag regardless
            logger.info(f"Attempting to delete local tag from database: {tag.name} (ID: {tag_id})")
            
            try:
                # Get tag count before deletion
                before_count = Tag.objects.count()
                logger.info(f"Tag count before deletion: {before_count}")
                
                # Delete the tag
                tag.delete()
                
                # Get tag count after deletion
                after_count = Tag.objects.count()
                logger.info(f"Tag count after deletion: {after_count}")
                
                if before_count == after_count:
                    logger.error(f"Tag count did not change after deletion! Before: {before_count}, After: {after_count}")
                else:
                    logger.info(f"Successfully deleted local tag: {name} (ID: {tag_id})")
                    
                # Double check tag was deleted
                try:
                    check_tag = Tag.objects.get(id=tag_id)
                    logger.error(f"Tag still exists after deletion: {check_tag.name} (ID: {check_tag.id})")
                except Tag.DoesNotExist:
                    logger.info(f"Confirmed tag {tag_id} no longer exists in database")
                    
            except Exception as db_error:
                logger.error(f"Database error during tag deletion: {str(db_error)}")
                logger.exception("Database deletion exception details:")
                raise

            return JsonResponse(
                {"success": True, "message": f"Tag '{name}' deleted successfully"}
            )
        except Exception as e:
            logger.error(f"Error deleting tag: {str(e)}")
            logger.exception("Exception details:")
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
                        urn=deterministic_urn,
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
            # Ensure the urn is a string before splitting
            tag_id_portion = (
                str(tag.urn).split(":")[-1]
                if tag.urn
                else None
            )
            if not tag_id_portion:
                messages.error(request, "Invalid tag urn")
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


@method_decorator(csrf_exempt, name="dispatch")
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

            # Check for JSON request with specific tags
            specific_tag_urns = []
            if request.body:
                try:
                    data = json.loads(request.body)
                    specific_tag_urns = data.get("urns", [])
                    if specific_tag_urns:
                        logger.info(f"Pulling specific tags: {specific_tag_urns}")
                except json.JSONDecodeError:
                    logger.warning("Invalid JSON in request body")
            
            # Check for form submission with a single tag
            specific_tag_urn = request.POST.get("tag_urn")

            # Process and import tags
            imported_count = 0
            updated_count = 0
            error_count = 0

            if specific_tag_urns:
                # Process multiple specific tags from JSON request
                for tag_urn in specific_tag_urns:
                    try:
                        # Get the tag data from DataHub
                        tag_data = client.get_tag(tag_urn)
                        if not tag_data:
                            logger.warning(f"Tag with URN {tag_urn} not found in DataHub")
                            error_count += 1
                            continue
                        
                        # Process the tag
                        result = self._process_tag(client, tag_data)
                        if result == "imported":
                            imported_count += 1
                        elif result == "updated":
                            updated_count += 1
                        elif result == "error":
                            error_count += 1
                    except Exception as e:
                        logger.error(f"Error processing tag with URN {tag_urn}: {str(e)}")
                        error_count += 1
                
                # Return JSON response for API calls
                if request.headers.get('Accept') == 'application/json' or request.content_type == 'application/json':
                    return JsonResponse({
                        "success": True,
                        "imported": imported_count,
                        "updated": updated_count,
                        "errors": error_count,
                        "message": f"Processed {len(specific_tag_urns)} tags: {imported_count} imported, {updated_count} updated, {error_count} errors"
                    })
            
            elif specific_tag_urn:
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
                        f"Error processing tag with urn {specific_tag_urn}: {str(e)}"
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
        # Extract the tag urn from the data
        tag_urn = tag_data.get("urn")
        if not tag_urn:
            logger.warning(f"Skipping tag without urn: {tag_data}")
            return "error"

        name = tag_data.get("name")
        if not name:
            # Try to extract name from URN for tags without explicit names
            try:
                # Extract the part after the last colon (e.g., "DBDATE" from "urn:li:tag:DBDATE")
                tag_parts = tag_urn.split(":")
                if len(tag_parts) >= 4:
                    # Handle complex tags like "urn:li:tag:bigquery_label:test"
                    if len(tag_parts) > 4:
                        # For tags with namespaces like "bigquery_label:test"
                        name = ":".join(tag_parts[3:])
                    else:
                        name = tag_parts[-1]
                    logger.info(f"Generated name '{name}' from urn {tag_urn}")
                else:
                    name = tag_parts[-1]
                    logger.info(f"Generated name '{name}' from urn {tag_urn}")
            except Exception as e:
                logger.warning(f"Failed to extract name from urn {tag_urn}: {str(e)}")
                logger.warning(f"Skipping tag without name: {tag_data}")
                return "error"
            
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

        # Extract and process ownership data
        ownership_data = None
        if "ownership" in tag_data and tag_data["ownership"]:
            logger.info(f"Processing ownership data for tag {name}")
            ownership_data = sanitize_ownership_data(tag_data["ownership"])
            logger.debug(f"Sanitized ownership data: {ownership_data}")

        # Try to find an existing tag with the same urn
        # Ensure tag_urn is a string for database query
        existing_tag = Tag.objects.filter(urn=str(tag_urn)).first()

        if existing_tag:
            # Update the existing tag
            existing_tag.name = name
            existing_tag.description = description
            existing_tag.color = color_hex  # Now has default value if none provided
            existing_tag.ownership_data = ownership_data  # Add ownership data
            existing_tag.sync_status = "SYNCED"
            existing_tag.last_synced = timezone.now()
            existing_tag.save()
            logger.info(f"Updated tag {name} with ownership data: {ownership_data is not None}")
            return "updated"
        else:
            # Create a new tag
            # Ensure tag_urn is a string before attempting to split it
            tag_id = str(tag_urn).split(":")[-1] if tag_urn else None

            new_tag = Tag.objects.create(
                name=name,
                description=description,
                color=color_hex,  # Now has default value if none provided
                urn=tag_urn,
                datahub_id=tag_id,
                ownership_data=ownership_data,  # Add ownership data
                sync_status="SYNCED",
                last_synced=timezone.now(),
            )
            logger.info(f"Created new tag {name} with ownership data: {ownership_data is not None}")
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
                        "message": "Entity urn and Tag urn are required",
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
                        "message": "Entity urn and Tag urn are required",
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


@csrf_exempt
def get_users_and_groups(request):
    """Get users and groups from DataHub for owner selection with database caching."""
    try:
        if request.method != 'POST':
            return JsonResponse({
                "success": False,
                "error": "Only POST method allowed"
            })
        
        try:
            data = json.loads(request.body) if request.body else {}
        except json.JSONDecodeError:
            logger.error("Invalid JSON in request body")
            data = {}
        
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
                        'display_name': (user.get('properties') or {}).get('displayName', ''),
                        'email': (user.get('properties') or {}).get('email', '')
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
                        'display_name': (group.get('properties') or {}).get('displayName', ''),
                        'description': (group.get('properties') or {}).get('description', '')
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
                        'name': (ownership_type.get('info') or {}).get('name', '')
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
        data_to_sanitize = result_data if request_type == 'all' else result_data.get(request_type, [])
        sanitized_result = sanitize_api_response(data_to_sanitize) if data_to_sanitize else {}
        
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
        
        # Create a mapping of urns to Tag IDs for quick lookup
        local_urn_to_id_map = {str(tag.urn): str(tag.id) for tag in local_tags}
        
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
        
        # Extract local tag urns
        local_tag_urns = set(local_tags.values_list("urn", flat=True))
        
        # Process local tags and match with remote
        for local_tag in local_tags:
            tag_urn = str(local_tag.urn)
            remote_match = remote_tags.get(tag_urn)
            
            local_tag_data = {
                "id": str(local_tag.id),  # Ensure ID is always a string
                "database_id": str(local_tag.id),  # Explicitly add database_id for clarity
                "name": local_tag.name,
                "description": local_tag.description,
                "color": local_tag.color,
                "colorHex": local_tag.color,
                "urn": tag_urn,
                "sync_status": local_tag.sync_status,
                "sync_status_display": local_tag.get_sync_status_display(),
                # Add empty ownership and relationships for local-only tags
                "owners_count": len(local_tag.ownership_data.get('owners', [])) if local_tag.ownership_data else 0,
                "owner_names": [],
                "relationships_count": 0,
                "ownership": local_tag.ownership_data or None,
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
                    "owners_count": len(remote_match.get("ownership", {}).get("owners", [])) if remote_match.get("ownership") else 0,
                    "owner_names": remote_match.get("owner_names", []),
                    "relationships_count": remote_match.get("relationships_count", 0),
                    "ownership": remote_match.get("ownership") or local_tag.ownership_data,
                    "relationships": remote_match.get("relationships"),
                })
                
                # Create combined data with explicit database_id
                combined_data = local_tag_data.copy()
                combined_data["database_id"] = str(local_tag.id)  # Ensure database_id is set
                
                synced_tags.append({
                    "local": local_tag_data,
                    "remote": remote_match,
                    "combined": combined_data  # Enhanced data for display with database_id
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
                # Extract name from properties or urn
                tag_name = None
                if "name" in remote_tag:
                    tag_name = remote_tag["name"]
                elif "properties" in remote_tag and remote_tag["properties"] and "name" in remote_tag["properties"]:
                    tag_name = remote_tag["properties"]["name"]
                
                # If name is still not found, extract it from the urn
                if not tag_name and tag_urn:
                    try:
                        # Extract the part after the last colon (e.g., "DBDATE" from "urn:li:tag:DBDATE")
                        tag_parts = tag_urn.split(":")
                        if len(tag_parts) >= 4:
                            # Handle complex tags like "urn:li:tag:bigquery_label:test"
                            if len(tag_parts) > 4:
                                # For tags with namespaces like "bigquery_label:test"
                                tag_name = ":".join(tag_parts[3:])
                            else:
                                tag_name = tag_parts[-1]
                        else:
                            tag_name = tag_parts[-1]
                    except Exception as e:
                        logger.warning(f"Failed to extract tag name from urn {tag_urn}: {str(e)}")
                
                # Use "Unnamed Tag" only if all extraction methods fail
                if not tag_name:
                    tag_name = "Unnamed Tag"
                
                # Add enhanced remote tag data
                remote_tag_enhanced = {
                    **remote_tag,
                    "name": tag_name,  # Set the extracted name
                    "sync_status": "REMOTE_ONLY",
                    "sync_status_display": "Remote Only",
                }
                
                # For remote-only tags, we need the tag name for generating a consistent ID
                # This is needed for frontend actions like deletion
                tag_name = remote_tag.get("name") or remote_tag.get("properties", {}).get("name") or tag_urn.split(":")[-1]
                
                # Generate a deterministic UUID based on the urn
                import hashlib
                # Create a stable UUID based on the urn - this ensures consistent IDs
                urn_hash = hashlib.md5(tag_urn.encode()).hexdigest()
                deterministic_uuid = str(uuid.UUID(urn_hash))
                
                # Add the UUID to the remote tag data
                remote_tag_enhanced["id"] = deterministic_uuid
                # Add a specific database_id field to make it clear this is for database operations
                remote_tag_enhanced["database_id"] = deterministic_uuid
                
                remote_only_tags.append(remote_tag_enhanced)
        
        # Calculate statistics
        total_tags = len(synced_tags) + len(local_only_tags) + len(remote_only_tags)
        
        # Calculate owned_tags by checking ownership data
        owned_tags = 0
        for tag_list in [synced_tags, local_only_tags, remote_only_tags]:
            for tag in tag_list:
                tag_data = tag.get("combined", tag) if "combined" in tag else tag
                if (tag_data.get("ownership") and tag_data.get("ownership", {}).get("owners")) or tag_data.get("owner_names"):
                    owned_tags += 1
                    
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
        """Sync a remote tag to local database"""
        try:
            # CRITICAL DEBUG: Log the incoming request details
            logger.critical(f"=== SYNC REQUEST START ===")
            logger.critical(f"Received tag_id: {tag_id} (type: {type(tag_id)})")
            logger.critical(f"Request method: {request.method}")
            logger.critical(f"Request path: {request.path}")
            logger.critical(f"Request headers: {dict(request.headers)}")
            
            # Try to parse request body
            if request.body:
                try:
                    body_data = json.loads(request.body)
                    logger.critical(f"Request body: {body_data}")
                except:
                    logger.critical(f"Request body (raw): {request.body}")
            else:
                logger.critical("No request body")
            
            logger.info(f"Syncing tag {tag_id} to local")
            
            # Use the helper function to find the tag
            tag, error_response = find_tag_by_flexible_id(tag_id)
            if not tag:
                logger.critical(f"Tag not found for ID: {tag_id}")
                return error_response
            
            logger.critical(f"Found tag: ID={tag.id}, Name={tag.name}, URN={tag.urn}")
            
            # Get DataHub client
            client = get_datahub_client()
            if not client:
                logger.critical("DataHub client not available")
                return JsonResponse({
                    "success": False,
                    "error": "DataHub client not available"
                }, status=500)
            
            logger.critical("DataHub client obtained successfully")
            
            # Fetch remote tag data using the tag's URN
            logger.critical(f"Fetching remote tag data for URN: {tag.urn}")
            remote_tag = client.get_tag(tag.urn)
            
            if not remote_tag:
                logger.critical(f"Remote tag not found for URN: {tag.urn}")
                return JsonResponse({
                    "success": False,
                    "error": f"Remote tag not found for URN: {tag.urn}"
                }, status=404)
            
            logger.critical(f"Successfully fetched remote tag data")
            logger.critical(f"Remote tag keys: {list(remote_tag.keys()) if isinstance(remote_tag, dict) else 'Not a dict'}")
            
            # Log the full remote tag structure for debugging
            logger.critical(f"FULL REMOTE TAG DATA: {json.dumps(remote_tag, indent=2, default=str)}")

            # Update basic tag fields
            properties = remote_tag.get("properties") or {}
            tag.name = properties.get("name", tag.name)
            tag.description = properties.get("description", "")
            tag.color = properties.get("colorHex", tag.color)
            tag.sync_status = "SYNCED"
            tag.last_synced = timezone.now()
            
            logger.critical(f"Updated basic fields - Name: {tag.name}, Description: {tag.description}, Color: {tag.color}")
            
            # Update ownership data if available
            ownership_data = None
            
            # Check for ownership data in different locations
            if "ownership" in remote_tag and remote_tag["ownership"]:
                ownership_data = remote_tag["ownership"]
                logger.critical(f"Found ownership data in main object")
            elif "relationships" in remote_tag and "ownership" in remote_tag.get("relationships", {}):
                ownership_data = remote_tag["relationships"]["ownership"]
                logger.critical(f"Found ownership data in relationships")
            else:
                logger.critical(f"No ownership data found in remote tag")
                logger.critical(f"Available keys in remote_tag: {list(remote_tag.keys()) if isinstance(remote_tag, dict) else 'Not a dict'}")
            
            # Process ownership data if found
            if ownership_data:
                logger.critical(f"Processing ownership data: {ownership_data}")
                
                # Use the specialized sanitize_ownership_data function
                sanitized_ownership = sanitize_ownership_data(ownership_data)
                logger.critical(f"Sanitized ownership data: {sanitized_ownership}")
                
                # Save ownership data to tag
                tag.ownership_data = sanitized_ownership
                logger.critical(f"Set tag.ownership_data to: {tag.ownership_data}")
            else:
                logger.critical("No ownership data to process")
                tag.ownership_data = None
            
            # Save the tag
            try:
                tag.save()
                logger.critical("Tag saved successfully")
                
                # Verify the save worked
                tag.refresh_from_db()
                logger.critical(f"After refresh - ownership_data: {tag.ownership_data}")
                
            except Exception as save_error:
                logger.critical(f"Error saving tag: {str(save_error)}")
                raise save_error
            
            logger.critical(f"=== SYNC REQUEST COMPLETE ===")
            
            response_data = {
                "success": True,
                "status": "success",
                "message": "Tag synced successfully",
                "refresh_needed": True,
                "tag": {
                    "id": str(tag.id),
                    "name": tag.name,
                    "urn": tag.urn
                }
            }
            logger.critical(f"Returning success response: {response_data}")
            return JsonResponse(response_data)
                
        except Tag.DoesNotExist:
            logger.critical(f"Tag with ID {tag_id} not found - DoesNotExist exception")
            return JsonResponse({
                "success": False,
                "error": f"Tag with ID {tag_id} not found"
            }, status=404)
        except Exception as e:
            logger.critical(f"Error syncing tag to local: {str(e)}")
            logger.exception("Stack trace:")
            return JsonResponse({
                "success": False,
                "error": str(e)
            }, status=500)


@method_decorator(csrf_exempt, name="dispatch")
class TagDownloadJsonView(View):
    """API endpoint to download tag data as JSON"""
    
    def get(self, request, tag_id):
        try:
            # Use the helper function to find the tag
            tag, error_response = find_tag_by_flexible_id(tag_id)
            if not tag:
                return error_response
            
            logger.info(f"Found tag using flexible ID lookup: {tag.name} (ID: {tag.id})")
            
            # Create JSON data
            tag_data = {
                "id": str(tag.id),
                "name": tag.name,
                "description": tag.description,
                "color": tag.color,
                "urn": tag.urn,
                "sync_status": tag.sync_status,
                "last_synced": tag.last_synced.isoformat() if tag.last_synced else None,
                "ownership_data": tag.ownership_data
            }
            
            # Add remote data if available
            client = get_datahub_client()
            if client:
                try:
                    remote_tag = client.get_tag(tag.urn)
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
            
            # Use the helper function to find the tag
            tag, error_response = find_tag_by_flexible_id(tag_id)
            if not tag:
                return error_response
            
            logger.info(f"Found tag using flexible ID lookup: {tag.name} (ID: {tag.id})")
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
                "urn": tag.urn,
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


@method_decorator(csrf_exempt, name="dispatch")
class TagDeleteView(View):
    """View to handle tag deletion via AJAX or form submission"""

    def post(self, request, tag_id):
        """Delete a tag and return JSON response or redirect based on Accept header"""
        try:
            logger.info(f"TagDeleteView.post called for tag_id: {tag_id}")
            
            # Log request details
            logger.info(f"Request method: {request.method}")
            logger.info(f"Request content type: {request.content_type}")
            
            # Use the helper function to find the tag
            tag, error_response = find_tag_by_flexible_id(tag_id)
            if not tag:
                return error_response
            
            name = tag.name
            logger.info(f"Found tag using flexible ID lookup: {name} (ID: {tag.id})")
            
            logger.info(f"Found tag to delete: {name} (ID: {tag_id})")
            logger.info(f"Tag details - Name: {tag.name}, URN: {tag.urn}, Status: {tag.sync_status}")

            # We only delete the local tag, not the remote one on DataHub
            logger.info(f"Deleting local tag only, not affecting remote tag on DataHub")

            # Delete the local tag regardless
            logger.info(f"Attempting to delete local tag from database: {tag.name} (ID: {tag_id})")
            
            try:
                # Get tag count before deletion
                before_count = Tag.objects.count()
                logger.info(f"Tag count before deletion: {before_count}")
                
                # Delete the tag
                tag.delete()
                
                # Get tag count after deletion
                after_count = Tag.objects.count()
                logger.info(f"Tag count after deletion: {after_count}")
                
                if before_count == after_count:
                    logger.error(f"Tag count did not change after deletion! Before: {before_count}, After: {after_count}")
                else:
                    logger.info(f"Successfully deleted local tag: {name} (ID: {tag_id})")
                    
                # Double check tag was deleted
                try:
                    check_tag = Tag.objects.get(id=tag_id)
                    logger.error(f"Tag still exists after deletion: {check_tag.name} (ID: {check_tag.id})")
                except Tag.DoesNotExist:
                    logger.info(f"Confirmed tag {tag_id} no longer exists in database")
                    
            except Exception as db_error:
                logger.error(f"Database error during tag deletion: {str(db_error)}")
                logger.exception("Database deletion exception details:")
                raise

            # Check if this is an AJAX request or a form submission
            if request.headers.get('Accept') == 'application/json' or request.content_type == 'application/json':
                # Return JSON response for AJAX requests
                return JsonResponse(
                    {"success": True, "message": f"Tag '{name}' deleted successfully"}
                )
            else:
                # For form submissions, redirect back to the tag list page with a success message
                from django.contrib import messages
                from django.shortcuts import redirect
                
                messages.success(request, f"Tag '{name}' deleted successfully")
                return redirect('metadata_manager:tag_list')
                
        except Exception as e:
            logger.error(f"Error deleting tag: {str(e)}")
            logger.exception("Exception details:")
            
            # Check if this is an AJAX request or a form submission
            if request.headers.get('Accept') == 'application/json' or request.content_type == 'application/json':
                # Return JSON error response for AJAX requests
                return JsonResponse(
                    {"success": False, "message": f"An error occurred: {str(e)}"},
                    status=500,
                )
            else:
                # For form submissions, redirect back to the tag list page with an error message
                from django.contrib import messages
                from django.shortcuts import redirect
                
                messages.error(request, f"Error deleting tag: {str(e)}")
                return redirect('metadata_manager:tag_list')


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
