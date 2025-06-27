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
from datahub_cicd_client.integrations.urn_utils import get_full_urn_from_name
from utils.datahub_client_adapter import get_datahub_client, test_datahub_connection, get_datahub_client_from_request
from utils.data_sanitizer import sanitize_api_response
from web_ui.models import GitSettings, Environment
from .models import Tag

logger = logging.getLogger(__name__)


def sanitize_ownership_data(data):
    """
    Specifically sanitize ownership data to ensure it's properly formatted for database storage.
    Standardizes the format to match UI creation format.
    
    Args:
        data: The ownership data to sanitize
        
    Returns:
        Sanitized ownership data with consistent structure: {"owners": [{"owner_urn": "...", "ownership_type_urn": "..."}]}
    """
    if not data:
        return {"owners": []}
    
    if isinstance(data, dict):
        # Check if this already has the correct structure with owners
        if "owners" in data and isinstance(data["owners"], list):
            # This is the standard DataHub ownership format - convert to UI format
            sanitized_owners = []
            
            for owner_info in data["owners"]:
                if isinstance(owner_info, dict):
                    # Handle different remote formats
                    owner_urn = None
                    ownership_type_urn = None
                    
                    # Format 1: DataHub GraphQL format
                    if "owner" in owner_info and isinstance(owner_info["owner"], dict):
                        owner_urn = owner_info["owner"].get("urn")
                        if "ownershipType" in owner_info and isinstance(owner_info["ownershipType"], dict):
                            ownership_type_urn = owner_info["ownershipType"].get("urn")
                        elif "type" in owner_info:
                            ownership_type_urn = owner_info["type"]
                    
                    # Format 2: Direct URN format (already in UI format)
                    elif "owner_urn" in owner_info and "ownership_type_urn" in owner_info:
                        owner_urn = owner_info["owner_urn"]
                        ownership_type_urn = owner_info["ownership_type_urn"]
                    
                    # Format 3: Simplified format
                    elif "owner" in owner_info and isinstance(owner_info["owner"], str):
                        owner_urn = owner_info["owner"]
                        ownership_type_urn = owner_info.get("type", "urn:li:ownershipType:__system__technical_owner")
                    
                    # Add to sanitized list if we have valid data
                    if owner_urn and ownership_type_urn:
                        sanitized_owners.append({
                            "owner_urn": owner_urn,
                            "ownership_type_urn": ownership_type_urn
                        })
            
            return {"owners": sanitized_owners}
        
        # If no owners key, try to find owners in other keys
        for key, value in data.items():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                # This looks like an owners list - recursively sanitize
                return sanitize_ownership_data({"owners": value})
        
        # No suitable owners list found, but preserve the structure
        return {"owners": []}
    
    elif isinstance(data, list):
        # Direct list of owners - convert to standard format
        return sanitize_ownership_data({"owners": data})
    
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
    Returns the tag if found, or None if not found
    """
    try:
        # First try with the ID as is
        tag = Tag.objects.get(id=tag_id)
        return tag
    except (Tag.DoesNotExist, ValueError):
        try:
            # Try with the ID as datahub_id
            tag = Tag.objects.get(datahub_id=tag_id)
            return tag
        except Tag.DoesNotExist:
            try:
                # Try with the ID as URN
                tag = Tag.objects.get(urn=tag_id)
                return tag
            except Tag.DoesNotExist:
                # If we still can't find it, try with a formatted UUID
                try:
                    # Try to format the ID as a UUID
                    formatted_id = str(uuid.UUID(tag_id.replace('-', '')))
                    tag = Tag.objects.get(id=formatted_id)
                    return tag
                except (Tag.DoesNotExist, ValueError):
                    # If all attempts fail, return None
                    logger.error(f"Tag not found with any ID format: {tag_id}")
                    return None

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

def filter_tags_by_connection(all_tags, current_connection):
    """
    Filter and return tags based on connection logic - only one row per datahub_id:
    1. If tag is available for current connection_id -> show in synced
    2. If tag is available for different connection_id -> show in local-only  
    3. If tag has no connection_id -> show in local-only
    Priority: current connection > different connection > no connection
    """
    tags = []
    
    # Group tags by datahub_id to implement the single-row-per-datahub_id logic
    tags_by_datahub_id = {}
    for tag in all_tags:
        datahub_id = tag.datahub_id or f'no_datahub_id_{tag.id}'  # Use unique identifier for tags without datahub_id
        if datahub_id not in tags_by_datahub_id:
            tags_by_datahub_id[datahub_id] = []
        tags_by_datahub_id[datahub_id].append(tag)
    
    for datahub_id, tag_group in tags_by_datahub_id.items():
        # Find the best tag to show based on priority
        selected_tag = None
        
        # Priority 1: Tag with current connection (highest priority)
        for tag in tag_group:
            if current_connection and tag.connection == current_connection:
                selected_tag = tag
                break
        
        # Priority 2: Tag with different connection (if no current connection tag found)
        if not selected_tag:
            for tag in tag_group:
                if tag.connection and tag.connection != current_connection:
                    selected_tag = tag
                    break
        
        # Priority 3: Tag with no connection (lowest priority)
        if not selected_tag:
            for tag in tag_group:
                if tag.connection is None:
                    selected_tag = tag
                    break
        
        # Fallback: If somehow no tag matches above criteria, take the first one
        if not selected_tag and tag_group:
            selected_tag = tag_group[0]
        
        # Add the selected tag to results
        if selected_tag:
            tags.append(selected_tag)
    
    return tags

class TagListView(View):
    """View to list and create tags"""

    def get(self, request):
        """Display list of tags"""
        try:
            logger.info("Starting TagListView.get")

            # Get current connection to filter tags by connection
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            logger.debug(f"Using connection: {current_connection.name if current_connection else 'None'}")
            
            # Get tags relevant to current connection (database-only operation)
            # Include: tags with no connection (local-only) + tags with current connection (synced)
            # BUT: if there's a synced tag for current connection with same datahub_id, hide the other connection's version
            all_tags = Tag.objects.all().order_by("name")
            tags = filter_tags_by_connection(all_tags, current_connection)
            
            logger.debug(f"Found {len(tags)} tags relevant to current connection")

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
            color = request.POST.get("color")
            owners = request.POST.getlist("owners[]")  # Get list of selected owners
            ownership_types = request.POST.getlist("ownership_types[]")  # Get list of ownership types
            logger.debug(f"Tag creation form data: name={name}, owners={owners}, ownership_types={ownership_types}")
            
            # Also try alternate form of ownership params (forms can send them differently)
            if not owners:
                # Try without [] suffix which some form processors remove
                owners = request.POST.getlist("owners")
                ownership_types = request.POST.getlist("ownership_types")
                logger.debug(f"Alternate ownership params: owners={owners}, ownership_types={ownership_types}")

            # If color is empty, set to None to preserve null values
            if not color or color.strip() == "":
                color = None

            if not name:
                messages.error(request, "Tag name is required")
                return redirect("metadata_manager:tag_list")

            # Generate deterministic URN
            deterministic_urn = get_full_urn_from_name("tag", name)

            # Get current connection
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            
            # Check if tag with this URN already exists for the current connection
            existing_tag_query = Tag.objects.filter(urn=deterministic_urn)
            if current_connection:
                existing_tag_query = existing_tag_query.filter(connection=current_connection)
            else:
                existing_tag_query = existing_tag_query.filter(connection__isnull=True)
                
            if existing_tag_query.exists():
                messages.error(request, f"Tag with name '{name}' already exists for this connection")
                return redirect("metadata_manager:tag_list")

            # Create the tag (no connection until synced)
            tag = Tag.objects.create(
                name=name,
                description=description,
                color=color,
                urn=deterministic_urn,
                sync_status="LOCAL_ONLY",
                connection=None,  # No connection until synced to DataHub
            )

            # Store owners information if provided
            logger.info(f"Raw ownership data for creation - owners: {owners}, ownership_types: {ownership_types}")
            
            if owners and ownership_types:
                # Filter out empty values and pair owners with ownership types
                valid_owners = []
                logger.info(f"Processing ownership data: {len(owners)} owners and {len(ownership_types)} types")
                logger.info(f"Owners list: {owners}")
                logger.info(f"Ownership types list: {ownership_types}")
                
                for i, owner in enumerate(owners):
                    logger.info(f"Processing owner {i}: '{owner}' with type {ownership_types[i] if i < len(ownership_types) else 'N/A'}")
                    if owner and owner.strip() and i < len(ownership_types) and ownership_types[i] and ownership_types[i].strip():
                        valid_owners.append({
                            'owner_urn': owner.strip(),
                            'ownership_type_urn': ownership_types[i].strip()
                        })
                        logger.info(f"Added valid owner: {owner.strip()} with type {ownership_types[i].strip()}")
                    else:
                        logger.warning(f"Skipped owner {i}: owner='{owner}', type='{ownership_types[i] if i < len(ownership_types) else 'N/A'}'")
                
                if valid_owners:
                    # Store owners with ownership types as JSON data
                    tag.ownership_data = {
                        'owners': valid_owners
                    }
                    tag.save(update_fields=['ownership_data'])
                    logger.info(f"Saved {len(valid_owners)} owners for tag {name}: {valid_owners}")

            messages.success(request, f"Tag '{name}' created successfully" + 
                           (f" with {len(valid_owners)} owner(s)" if 'valid_owners' in locals() and valid_owners else ""))
            return redirect("metadata_manager:tag_list")
        except Exception as e:
            logger.error(f"Error creating tag: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:tag_list")


@method_decorator(csrf_exempt, name="dispatch")
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
            client = get_datahub_client_from_request(request)
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
                                remote_color = properties.get("colorHex")
                                local_color = tag.color
                                
                                # Normalize colors for comparison (handle null/empty values properly)
                                remote_color_normalized = remote_color if remote_color and remote_color.strip() else None
                                local_color_normalized = local_color if local_color and local_color.strip() else None

                                # If different, mark as modified
                                if (
                                    local_description != remote_description
                                    or local_color_normalized != remote_color_normalized
                                ) and tag.sync_status != "MODIFIED":
                                    tag.sync_status = "MODIFIED"
                                    tag.save(update_fields=["sync_status"])
                                    logger.debug(f"Updated tag {local_tag.name} status to MODIFIED")
                                else:
                                    if local_tag.sync_status != "SYNCED":
                                        local_tag.sync_status = "SYNCED" 
                                        local_tag.save(update_fields=["sync_status"])
                                        logger.debug(f"Updated tag {local_tag.name} status to SYNCED")
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
            logger.info(f"Starting tag update for tag_id: {tag_id}")
            logger.info(f"Request method: {request.method}")
            logger.info(f"Request headers: {dict(request.headers)}")
            logger.info(f"Request POST data keys: {list(request.POST.keys())}")
            
            tag = get_object_or_404(Tag, id=tag_id)
            logger.info(f"Found tag: {tag.name} (ID: {tag.id})")

            # Check if this is an AJAX request (from the modal form)
            is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.POST.get('_method') == 'PUT'

            name = request.POST.get("name")
            description = request.POST.get("description", "")
            color = request.POST.get("color", tag.color)
            owners = request.POST.getlist("owners[]") or request.POST.getlist("owners")
            ownership_types = request.POST.getlist("ownership_types[]") or request.POST.getlist("ownership_types")
            
            # Debug logging
            logger.info(f"Tag update request data:")
            logger.info(f"  name: '{name}'")
            logger.info(f"  description: '{description}'")
            logger.info(f"  color: '{color}'")
            logger.info(f"  owners: {owners}")
            logger.info(f"  ownership_types: {ownership_types}")
            logger.info(f"  is_ajax: {is_ajax}")

            # If color is empty, set to None to preserve null values
            if not color or color.strip() == "":
                color = None

            if not name:
                error_msg = "Tag name is required"
                logger.error(f"Tag update failed: {error_msg}")
                if is_ajax:
                    return JsonResponse({"success": False, "error": error_msg}, status=400)
                messages.error(request, error_msg)
                return redirect("metadata_manager:tag_detail", tag_id=tag.id)

            # Check if name changed and if there's a conflict with existing tags
            if name != tag.name:
                # Check if another tag with this name already exists (case-insensitive)
                existing_tag_by_name = Tag.objects.filter(name__iexact=name).exclude(id=tag.id).first()
                
                if existing_tag_by_name:
                    error_msg = f"Tag with name '{name}' already exists (existing tag: '{existing_tag_by_name.name}')"
                    logger.error(f"Tag name conflict: trying to rename '{tag.name}' to '{name}', but '{existing_tag_by_name.name}' already exists")
                    if is_ajax:
                        return JsonResponse({"success": False, "error": error_msg}, status=400)
                    messages.error(request, error_msg)
                    return redirect("metadata_manager:tag_detail", tag_id=tag.id)
                
                logger.info(f"Updating tag name from '{tag.name}' to '{name}' (URN remains: '{tag.urn}')")

            # If this tag exists remotely and we're changing details, mark as modified
            if tag.sync_status in ["SYNCED", "REMOTE_ONLY"] and (
                tag.name != name or tag.description != description or tag.color != color
            ):
                tag.sync_status = "MODIFIED"
                logger.info(f"Tag '{tag.name}' marked as MODIFIED due to local changes")

            # Update the tag
            tag.name = name
            tag.description = description
            tag.color = color

            # Handle ownership data
            logger.info(f"Raw ownership data - owners: {owners}, ownership_types: {ownership_types}")
            
            try:
                if owners and ownership_types:
                    # Filter out empty values and pair owners with ownership types
                    valid_owners = []
                    logger.info(f"Processing ownership data for update: {len(owners)} owners and {len(ownership_types)} types")
                    logger.info(f"Owners list: {owners}")
                    logger.info(f"Ownership types list: {ownership_types}")
                    
                    # Handle case where we have more owners than ownership types
                    # Use the first ownership type for all owners if there's a mismatch
                    default_ownership_type = ownership_types[0] if ownership_types else None
                    
                    for i, owner in enumerate(owners):
                        if owner and owner.strip():
                            # Use the corresponding ownership type, or default if not available
                            ownership_type = ownership_types[i] if i < len(ownership_types) else default_ownership_type
                            
                            logger.info(f"Processing owner {i}: '{owner}' with type '{ownership_type}'")
                            
                            if ownership_type and ownership_type.strip():
                                valid_owners.append({
                                    'owner_urn': owner.strip(),
                                    'ownership_type_urn': ownership_type.strip()
                                })
                                logger.info(f"Added valid owner: {owner.strip()} with type {ownership_type.strip()}")
                            else:
                                logger.warning(f"Skipped owner {i}: '{owner}' - no valid ownership type")
                        else:
                            logger.warning(f"Skipped empty owner at index {i}")
                    
                    if valid_owners:
                        # Store owners with ownership types as JSON data
                        tag.ownership_data = {
                            'owners': valid_owners
                        }
                        logger.info(f"Updated {len(valid_owners)} owners for tag {name}: {valid_owners}")
                    else:
                        # Clear ownership data if no valid owners
                        tag.ownership_data = None
                        logger.info(f"Cleared ownership data for tag {name} - no valid owners found")
                else:
                    # Clear ownership data if no owners provided
                    tag.ownership_data = None
                    logger.info(f"Cleared ownership data for tag {name} (no owners provided)")

                logger.info(f"About to save tag with data: name='{tag.name}'")
                tag.save()
                logger.info(f"Tag saved successfully: {tag.name}")
            except Exception as ownership_error:
                logger.error(f"Error processing ownership data: {str(ownership_error)}")
                logger.exception("Ownership processing exception details:")
                raise

            success_msg = f"Tag '{name}' updated successfully"
            if is_ajax:
                return JsonResponse({"success": True, "message": success_msg})
            
            messages.success(request, success_msg)
            return redirect("metadata_manager:tag_detail", tag_id=tag.id)
        except Exception as e:
            logger.error(f"Error updating tag: {str(e)}")
            logger.exception("Full exception details:")
            error_msg = f"An error occurred: {str(e)}"
            
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.POST.get('_method') == 'PUT':
                return JsonResponse({"success": False, "error": error_msg}, status=400)
            
            messages.error(request, error_msg)
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



@method_decorator(require_POST, name="dispatch")
class TagDeployView(View):
    """View to deploy a tag to DataHub"""

    def post(self, request, tag_id):
        """Deploy a tag to DataHub"""
        try:
            # Get the tag
            tag = get_object_or_404(Tag, id=tag_id)

            # Get DataHub client
            client = get_datahub_client_from_request(request)
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
                tag.urn = result  # Store the DataHub URN
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
            client = get_datahub_client_from_request(request)

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
                        result = self._process_tag(client, tag_data, request)
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
                    result = self._process_tag(client, tag_data, request)
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
                        result = self._process_tag(client, tag_data, request)
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

    def _process_tag(self, client, tag_data, request=None):
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

        # Preserve null/empty colors from DataHub instead of forcing a default
        # Only use default if the database requires NOT NULL and we have no value
        if color_hex is not None and color_hex.strip() == "":
            color_hex = None  # Convert empty string to None for consistency

        description = tag_data.get("description", "")

        # Extract and process ownership data
        ownership_data = None
        if "ownership" in tag_data and tag_data["ownership"]:
            logger.info(f"Processing ownership data for tag {name}")
            ownership_data = sanitize_ownership_data(tag_data["ownership"])
            logger.debug(f"Sanitized ownership data: {ownership_data}")

        # Get current connection from request session
        current_connection = None
        if request:
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            logger.info(f"Using connection: {current_connection.name if current_connection else 'None'}")

        # Try to find an existing tag with the same urn
        # Ensure tag_urn is a string for database query
        existing_tag = Tag.objects.filter(urn=str(tag_urn)).first()

        if existing_tag:
            # Update the existing tag
            existing_tag.name = name
            existing_tag.description = description
            existing_tag.color = color_hex  # Preserve null/empty colors from DataHub
            existing_tag.ownership_data = ownership_data  # Add ownership data
            existing_tag.sync_status = "SYNCED"
            existing_tag.last_synced = timezone.now()
            # Set connection if available
            if current_connection:
                existing_tag.connection = current_connection
            existing_tag.save()
            logger.info(f"Updated tag {name} with ownership data: {ownership_data is not None}, connection: {current_connection.name if current_connection else 'None'}")
            return "updated"
        else:
            # Create a new tag
            # Ensure tag_urn is a string before attempting to split it
            tag_id = str(tag_urn).split(":")[-1] if tag_urn else None

            new_tag = Tag.objects.create(
                name=name,
                description=description,
                color=color_hex,  # Preserve null/empty colors from DataHub
                urn=tag_urn,
                datahub_id=tag_id,
                ownership_data=ownership_data,  # Add ownership data
                sync_status="SYNCED",
                last_synced=timezone.now(),
                connection=current_connection,  # Set connection
            )
            logger.info(f"Created new tag {name} with ownership data: {ownership_data is not None}, connection: {current_connection.name if current_connection else 'None'}")
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
            client = get_datahub_client_from_request(request)
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
            client = get_datahub_client_from_request(request)
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
        
        # Get DataHub client using proper connection system
        from utils.datahub_client_adapter import test_datahub_connection
        connected, client = test_datahub_connection(request)
        
        if not connected or not client:
            return JsonResponse({
                "success": False,
                "error": "No active DataHub connection configured. Please configure a connection."
            })
        
        result_data = {}
        
        # Fetch users if needed
        if users_need_refresh and request_type in ['users', 'all']:
            users_result = client.list_users(start=0, count=200)
            
            # Handle the adapter response format: {'success': True, 'data': {'searchResults': [...]}}
            if users_result and users_result.get('success'):
                # Extract users from searchResults
                adapter_data = users_result.get('data', {})
                search_results = adapter_data.get('searchResults', [])
                users_data = [result.get('entity', {}) for result in search_results if result.get('entity')]
            elif users_result and "users" in users_result:
                # Old format fallback
                users_data = users_result["users"]
            else:
                users_data = []
            
            # Always treat any response as success (empty results are valid)
            # Update database cache
            DataHubUser.objects.all().delete()  # Clear old cache
            
            users_to_create = []
            for user in users_data:
                properties = user.get('properties') or {}
                users_to_create.append(DataHubUser(
                    urn=user.get('urn', ''),
                    username=user.get('username', ''),
                    display_name=properties.get('displayName', ''),
                    email=properties.get('email', '')
                ))
            
            DataHubUser.objects.bulk_create(users_to_create, ignore_conflicts=True)
            
            result_data['users'] = [
                {
                    'urn': user.get('urn', ''),
                    'username': user.get('username', ''),
                    'display_name': (user.get('properties') or {}).get('displayName', ''),
                    'email': (user.get('properties') or {}).get('email', '')
                }
                for user in users_data
            ]
            
            logger.info(f"Cached {len(users_data)} users")
        
        # Fetch groups if needed
        if groups_need_refresh and request_type in ['groups', 'all']:
            groups_result = client.list_groups(start=0, count=200)
            
            # Handle the adapter response format: {'success': True, 'data': {'searchResults': [...]}}
            if groups_result and groups_result.get('success'):
                # Extract groups from searchResults
                adapter_data = groups_result.get('data', {})
                search_results = adapter_data.get('searchResults', [])
                groups_data = [result.get('entity', {}) for result in search_results if result.get('entity')]
            elif groups_result and "groups" in groups_result:
                # Old format fallback
                groups_data = groups_result["groups"]
            else:
                groups_data = []
            
            # Always treat any response as success (empty results are valid)
            # Update database cache
            DataHubGroup.objects.all().delete()  # Clear old cache
            
            groups_to_create = []
            for group in groups_data:
                properties = group.get('properties') or {}
                groups_to_create.append(DataHubGroup(
                    urn=group.get('urn', ''),
                    display_name=properties.get('displayName', ''),
                    description=properties.get('description', '')
                ))
            
            DataHubGroup.objects.bulk_create(groups_to_create, ignore_conflicts=True)
            
            result_data['groups'] = [
                {
                    'urn': group.get('urn', ''),
                    'display_name': (group.get('properties') or {}).get('displayName', ''),
                    'description': (group.get('properties') or {}).get('description', '')
                }
                for group in groups_data
            ]
            
            logger.info(f"Cached {len(groups_data)} groups")
        
        # Fetch ownership types if needed
        if ownership_types_need_refresh and request_type in ['ownership_types', 'all']:
            ownership_types_result = client.list_ownership_types(start=0, count=100)
            
            # Handle the adapter response format: {'success': True, 'data': {'ownershipTypes': [...]}}
            if ownership_types_result and ownership_types_result.get('success'):
                # Extract the actual data from the adapter response
                adapter_data = ownership_types_result.get('data', {})
                ownership_types_data = adapter_data.get('ownershipTypes', [])
            elif ownership_types_result:
                # Handle other response formats
                if isinstance(ownership_types_result, list):
                    ownership_types_data = ownership_types_result
                elif isinstance(ownership_types_result, dict) and "ownershipTypes" in ownership_types_result:
                    ownership_types_data = ownership_types_result["ownershipTypes"]
                else:
                    ownership_types_data = []
            else:
                ownership_types_data = []
            
            # Always treat any non-empty result as success (including default types)
            if ownership_types_data:
                # Update database cache
                DataHubOwnershipType.objects.all().delete()  # Clear old cache
                
                ownership_types_to_create = []
                for ownership_type in ownership_types_data:
                    # Handle both adapter format and service format
                    if 'display_name' in ownership_type:
                        # Adapter format
                        name = ownership_type.get('display_name', ownership_type.get('name', ''))
                    else:
                        # Service format
                        info = ownership_type.get('info') or {}
                        name = info.get('name', ownership_type.get('name', ''))
                    
                    ownership_types_to_create.append(DataHubOwnershipType(
                        urn=ownership_type['urn'],
                        name=name
                    ))
                
                DataHubOwnershipType.objects.bulk_create(ownership_types_to_create, ignore_conflicts=True)
                
                result_data['ownership_types'] = [
                    {
                        'urn': ownership_type['urn'],
                        'name': ownership_type.get('display_name') or (ownership_type.get('info') or {}).get('name', ownership_type.get('name', ''))
                    }
                    for ownership_type in ownership_types_data
                ]
                
                logger.info(f"Cached {len(ownership_types_data)} ownership types (including defaults)")
            else:
                logger.warning("No ownership types returned from service")
                if request_type == 'ownership_types':
                    return JsonResponse({
                        "success": False,
                        "error": "No ownership types available"
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
        
        # Get DataHub client using connection system
        from utils.datahub_client_adapter import get_datahub_client_from_request
        client = get_datahub_client_from_request(request)
        
        if not client:
            return JsonResponse({
                "success": False,
                "error": "No active DataHub connection configured. Please configure a connection."
            })
        
        # Test connection
        if not client.test_connection():
            return JsonResponse({
                "success": False,
                "error": "Cannot connect to DataHub with current connection"
            })
        
        # Get parameters
        query = request.GET.get("query", "*")
        start = int(request.GET.get("start", 0))
        count = int(request.GET.get("count", 100))
        skip_sync_validation = request.GET.get("skip_sync_validation", "false").lower() == "true"
        
        # Get current connection to filter tags by connection
        from web_ui.views import get_current_connection
        current_connection = get_current_connection(request)
        logger.debug(f"Using connection: {current_connection.name if current_connection else 'None'}")
        
        # Get ALL local tags - we'll categorize them based on connection logic
        all_local_tags = Tag.objects.all().order_by("name")
        logger.debug(f"Found {all_local_tags.count()} total local tags")
        
        # Separate tags based on connection logic:
        # 1. Tags with no connection (LOCAL_ONLY) - should appear as local-only
        # 2. Tags with current connection (SYNCED) - should appear as synced if they match remote
        # 3. Tags with different connection - should appear as local-only relative to current connection
        #    BUT: if there's a synced tag for current connection with same datahub_id, hide the other connection's version
        local_tags = filter_tags_by_connection(all_local_tags, current_connection)
        
        logger.debug(f"Filtered to {len(local_tags)} tags relevant to current connection")
        
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
        local_tag_urns = set(tag.urn for tag in local_tags)
        
        # Process local tags and match with remote
        for local_tag in local_tags:
            tag_urn = str(local_tag.urn)
            remote_match = remote_tags.get(tag_urn)
            
            # Extract owner names from ownership_data for local tags
            owner_names = []
            owners_count = 0
            if local_tag.ownership_data and local_tag.ownership_data.get('owners'):
                owners_count = len(local_tag.ownership_data['owners'])
                for owner_info in local_tag.ownership_data['owners']:
                    if owner_info and owner_info.get('owner_urn'):
                        owner_urn = owner_info['owner_urn']
                        # Extract username/group name from URN
                        if ':corpuser:' in owner_urn:
                            owner_name = owner_urn.split(':corpuser:')[-1]
                        elif ':corpGroup:' in owner_urn:
                            owner_name = owner_urn.split(':corpGroup:')[-1]
                        else:
                            owner_name = owner_urn.split(':')[-1] if ':' in owner_urn else owner_urn
                        owner_names.append(owner_name)
            
            # Determine connection context for frontend decision making
            connection_context = "none"  # Default for tags with no connection
            if local_tag.connection:
                if local_tag.connection == current_connection:
                    connection_context = "current"
                else:
                    connection_context = "different"
            
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
                # Add connection context for frontend action determination
                "connection_context": connection_context,
                "has_remote_match": bool(remote_match),
                # Add ownership information for local tags
                "owners_count": owners_count,
                "owner_names": owner_names,
                "relationships_count": 0,
                "ownership": local_tag.ownership_data or None,
                "ownership_data": local_tag.ownership_data or None,  # Keep both for compatibility
                "relationships": None,
            }
            
            # Determine categorization based on connection and remote match
            # With single-row-per-datahub_id logic:
            # - Synced: Tag belongs to current connection AND exists remotely
            # - Local-only: Tag belongs to different connection OR no connection OR no remote match
            if local_tag.connection == current_connection and remote_match:
                # Tag exists remotely AND is synced to current connection - categorize as synced
                
                # Enhanced sync status validation - check if tag needs status update
                # Skip validation if requested (e.g., after single tag edits)
                if not skip_sync_validation:
                    needs_status_update = False
                    status_change_reason = []
                    
                    # Check description changes - handle null/empty values properly
                    local_description = local_tag.description or ""
                    remote_description = remote_match.get("description", "")
                    
                    # Normalize descriptions for comparison
                    # Handle cases where remote description might be the string "None", None, or empty
                    def normalize_description(desc):
                        if desc is None or desc == "None" or desc == "" or (isinstance(desc, str) and desc.strip() == ""):
                            return ""
                        return str(desc).strip()
                    
                    local_description_normalized = normalize_description(local_description)
                    remote_description_normalized = normalize_description(remote_description)
                    
                    if local_description_normalized != remote_description_normalized:
                        needs_status_update = True
                        status_change_reason.append("description")
                        logger.debug(f"Description change detected for {local_tag.name}: local='{local_description_normalized}' vs remote='{remote_description_normalized}' (original: local='{local_description}', remote='{remote_description}')")
                    
                    # Check color changes - preserve null/empty values from DataHub
                    remote_color = remote_match.get("colorHex")  # Don't use default here
                    local_color = local_tag.color
                    
                    # Handle null/empty comparisons properly
                    # Both null/empty should be considered equal
                    remote_color_normalized = remote_color if remote_color and remote_color.strip() else None
                    local_color_normalized = local_color if local_color and local_color.strip() else None
                    
                    if remote_color_normalized != local_color_normalized:
                        needs_status_update = True
                        status_change_reason.append("color")
                        logger.debug(f"Color change detected for {local_tag.name}: local='{local_color_normalized}' vs remote='{remote_color_normalized}'")
                    
                    # Check ownership changes - handle null/empty ownership properly
                    local_ownership = local_tag.ownership_data or {}
                    local_owners = local_ownership.get("owners", [])
                    
                    remote_ownership = remote_match.get("ownership", {}) or {}
                    remote_owners = remote_ownership.get("owners", []) or []
                    
                    # Normalize empty/null ownership to empty lists for comparison
                    # If local ownership_data is None, treat as empty
                    if local_tag.ownership_data is None:
                        local_owners = []
                    
                    # If remote ownership is empty/null or has no owners, treat as empty
                    if not remote_ownership or not remote_owners:
                        remote_owners = []
                    
                    # Convert remote ownership format to local format for comparison
                    remote_owners_normalized = []
                    for remote_owner in remote_owners:
                        if remote_owner and remote_owner.get("owner", {}).get("urn"):
                            owner_urn = remote_owner["owner"]["urn"]
                            ownership_type = remote_owner.get("ownershipType", {}).get("urn", "urn:li:ownershipType:__system__technical_owner")
                            remote_owners_normalized.append({
                                "owner_urn": owner_urn,
                                "ownership_type_urn": ownership_type
                            })
                    
                    # Compare ownership by creating sets of (owner_urn, ownership_type_urn) tuples
                    local_ownership_set = set()
                    for owner in local_owners:
                        if owner and owner.get("owner_urn"):
                            local_ownership_set.add((owner["owner_urn"], owner.get("ownership_type_urn", "urn:li:ownershipType:__system__technical_owner")))
                    
                    remote_ownership_set = set()
                    for owner in remote_owners_normalized:
                        remote_ownership_set.add((owner["owner_urn"], owner["ownership_type_urn"]))
                    
                    # Both empty ownership should be considered equal
                    if local_ownership_set != remote_ownership_set:
                        needs_status_update = True
                        status_change_reason.append("ownership")
                        logger.debug(f"Ownership change detected for {local_tag.name}: local={len(local_ownership_set)} owners, remote={len(remote_ownership_set)} owners")
                    
                    # Only update sync status if the tag belongs to the current connection
                    # AND hasn't been recently synced to avoid overriding fresh syncs
                    from django.utils import timezone
                    from datetime import timedelta
                    
                    recently_synced = (
                        local_tag.last_synced and 
                        local_tag.last_synced > timezone.now() - timedelta(seconds=30)
                    )
                    
                    # CRITICAL: Only update sync status for tags that belong to current connection
                    # Tags from other connections should not have their sync_status modified
                    if local_tag.connection == current_connection and not recently_synced:
                        if needs_status_update:
                            # Tag has been modified in DataHub or locally
                            if local_tag.sync_status != "MODIFIED":
                                local_tag.sync_status = "MODIFIED"
                                local_tag.save(update_fields=["sync_status"])
                                logger.info(f"Updated tag {local_tag.name} status to MODIFIED due to changes in: {', '.join(status_change_reason)}")
                        else:
                            # Tag is in sync
                            if local_tag.sync_status != "SYNCED":
                                local_tag.sync_status = "SYNCED" 
                                local_tag.save(update_fields=["sync_status"])
                                logger.debug(f"Updated tag {local_tag.name} status to SYNCED")
                
                # Update local tag data with remote information, but preserve local ownership if remote is empty
                remote_ownership = remote_match.get("ownership")
                remote_owners_count = len(remote_ownership.get("owners", [])) if remote_ownership else 0
                remote_owner_names = remote_match.get("owner_names", [])
                
                # Use remote ownership if available, otherwise fall back to local
                final_ownership = remote_ownership or local_tag.ownership_data
                final_owners_count = remote_owners_count if remote_ownership else owners_count
                final_owner_names = remote_owner_names if remote_owner_names else owner_names
                
                local_tag_data.update({
                    "sync_status": local_tag.sync_status,
                    "sync_status_display": local_tag.get_sync_status_display(),
                    "connection_context": connection_context,  # Keep the connection context
                    "has_remote_match": True,  # This tag definitely has a remote match
                    "owners_count": final_owners_count,
                    "owner_names": final_owner_names,
                    "relationships_count": remote_match.get("relationships_count", 0),
                    "ownership": final_ownership,
                    "ownership_data": local_tag.ownership_data,  # Always preserve local ownership_data
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
                # Tag is local-only relative to current connection
                # This includes:
                # 1. Tags with no connection (true local-only)
                # 2. Tags with different connection (local-only relative to current connection)
                # 3. Tags with current connection but no remote match
                
                # Check if the tag was recently synced to avoid overriding fresh sync status
                from django.utils import timezone
                from datetime import timedelta
                
                recently_synced = (
                    local_tag.last_synced and 
                    local_tag.last_synced > timezone.now() - timedelta(minutes=5)  # 5 minute grace period
                )
                
                # CRITICAL: Only update sync status for tags that belong to current connection
                # Tags from other connections should maintain their original sync_status
                # Skip validation if requested (e.g., after single tag edits)
                if not skip_sync_validation and local_tag.connection == current_connection:
                    # Only update status for tags belonging to current connection
                    expected_status = "LOCAL_ONLY"
                    if not remote_match:
                        # Tag was synced to current connection but no longer exists remotely
                        expected_status = "REMOTE_DELETED"
                    
                    # Only update sync status if it wasn't recently synced and status is different
                    if not recently_synced and local_tag.sync_status != expected_status:
                        local_tag.sync_status = expected_status
                        local_tag.save(update_fields=["sync_status"])
                        logger.debug(f"Updated tag {local_tag.name} status to {expected_status}")
                elif skip_sync_validation:
                    logger.debug(f"Skipping sync status validation for tag {local_tag.name} - skip_sync_validation=true")
                
                # Update local tag data with current status
                local_tag_data.update({
                    "sync_status": local_tag.sync_status,
                    "sync_status_display": local_tag.get_sync_status_display(),
                    "connection_context": connection_context,  # Keep the connection context
                    "has_remote_match": bool(remote_match),  # Whether this tag has a remote match
                })
                
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
                    

        
        # Get DataHub URL for external links
        datahub_url = client.server_url if hasattr(client, 'server_url') else ""
        if datahub_url and datahub_url.endswith("/api/gms"):
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
            client = get_datahub_client_from_request(request)
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

            # Get current connection from request session
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            logger.critical(f"Using connection: {current_connection.name if current_connection else 'None'}")

            # Update basic tag fields
            properties = remote_tag.get("properties") or {}
            tag.name = properties.get("name", tag.name)
            tag.description = properties.get("description", "")
            tag.color = properties.get("colorHex", tag.color)

            tag.sync_status = "SYNCED"
            tag.last_synced = timezone.now()
            # Set connection
            if current_connection:
                tag.connection = current_connection
            
            logger.critical(f"Updated basic fields - Name: {tag.name}, Description: {tag.description}, Color: {tag.color}, Connection: {current_connection.name if current_connection else 'None'}")
            
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
            tag = find_tag_by_flexible_id(tag_id)
            if not tag:
                return JsonResponse({
                    "success": False,
                    "error": f"Tag with ID {tag_id} not found"
                }, status=404)
            
            logger.info(f"Found tag using flexible ID lookup: {tag.name} (ID: {tag.id})")
            
            # Create JSON data
            tag_data = {
                "id": str(tag.id),
                "name": tag.name,
                "description": tag.description,
                "color": tag.color,
                "deprecated": tag.deprecated,
                "urn": tag.urn,
                "sync_status": tag.sync_status,
                "last_synced": tag.last_synced.isoformat() if tag.last_synced else None,
                "ownership_data": tag.ownership_data
            }
            
            # Add remote data if available
            client = get_datahub_client_from_request(request)
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
            tag = find_tag_by_flexible_id(tag_id)
            if not tag:
                return JsonResponse({
                    "success": False,
                    "error": f"Tag with ID {tag_id} not found"
                }, status=404)
            
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
            
            # Build base directory path - find repo root and use correct location
            # When running from web_ui/, we need to go up one level to get to repo root
            current_dir = Path(os.getcwd())
            if current_dir.name == "web_ui":
                repo_root = current_dir.parent
            else:
                # Try to find repo root by looking for characteristic files
                repo_root = current_dir
                for _ in range(5):  # Search up to 5 levels
                    if (repo_root / "README.md").exists() and (repo_root / "scripts").exists() and (repo_root / "web_ui").exists():
                        break
                    repo_root = repo_root.parent
                else:
                    # Fallback: assume current directory
                    repo_root = current_dir
            
            base_dir = repo_root / "metadata-manager" / environment_name / "tags"
            base_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Using base directory for staged changes: {base_dir}")
            
            # Add tag to staged changes
            result = add_tag_to_staged_changes(
                tag_data=tag_data,
                environment=environment_name,
                owner=owner,
                base_dir=str(base_dir),
                mutation_name=mutation_name
            )
            
            # Provide feedback about deduplication
            files_created = list(result.values())
            files_created_count = len(files_created)
            
            # Calculate expected files (now 1 combined MCP file instead of separate files)
            expected_files = 1  # single combined MCP file
            
            files_skipped_count = expected_files - files_created_count
            
            if files_skipped_count > 0:
                message = f"Tag added to staged changes: {files_created_count} file created, {files_skipped_count} file skipped (unchanged)"
            else:
                message = f"Tag added to staged changes: {files_created_count} file created"
            
            # Return success response
            return JsonResponse({
                "status": "success",
                "message": message,
                "files_created": files_created,
                "files_created_count": files_created_count,
                "files_skipped_count": files_skipped_count
            })
                
        except Exception as e:
            logger.error(f"Error adding tag to staged changes: {str(e)}")
            return JsonResponse({"error": str(e)}, status=500)


@method_decorator(csrf_exempt, name="dispatch")
class TagRemoteAddToStagedChangesView(View):
    """API endpoint to add a remote tag to staged changes without syncing to local first"""
    
    def post(self, request):
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
            
            data = json.loads(request.body)
            
            # Get the tag data from the request
            tag_data = data.get('tag_data')
            if not tag_data:
                return JsonResponse({
                    "success": False,
                    "error": "No tag_data provided"
                }, status=400)
            
            # Get environment and mutation name
            environment_name = data.get('environment', 'dev')
            mutation_name = data.get('mutation_name')
            
            # Get current user as owner
            owner = request.user.username if request.user.is_authenticated else "admin"
            
            # For remote tags, we need to ensure we have an ID for MCP creation
            # If the remote tag doesn't have an ID, we'll generate one from the URN or name
            tag_id = tag_data.get('id')
            if not tag_id:
                if tag_data.get('urn'):
                    # Extract ID from URN
                    urn_parts = tag_data['urn'].split(':')
                    if len(urn_parts) >= 3:
                        tag_id = urn_parts[-1]
                    else:
                        tag_id = tag_data['urn']
                elif tag_data.get('name'):
                    # Use name as ID
                    tag_id = tag_data['name'].replace(' ', '_').lower()
                else:
                    return JsonResponse({
                        "success": False,
                        "error": "Remote tag must have either URN or name for ID generation"
                    }, status=400)
            
            # Build the tag data dictionary for MCP creation
            tag_mcp_data = {
                "id": tag_id,
                "name": tag_data.get('name', 'Unknown Tag'),
                "description": tag_data.get('description', ''),
                "color": tag_data.get('color') or tag_data.get('colorHex', '#1890FF'),
                "urn": tag_data.get('urn'),
                "properties": {
                    "name": tag_data.get('name', 'Unknown Tag'),
                    "description": tag_data.get('description', ''),
                    "colorHex": tag_data.get('color') or tag_data.get('colorHex', '#1890FF')
                }
            }
            
            # Add ownership data if available
            if tag_data.get('ownership_data'):
                tag_mcp_data["ownership"] = tag_data['ownership_data']
            elif tag_data.get('ownership'):
                tag_mcp_data["ownership"] = tag_data['ownership']
            
            # Get environment
            try:
                environment = Environment.objects.get(name=environment_name)
            except Environment.DoesNotExist:
                # Create default environment if it doesn't exist
                environment = Environment.objects.create(
                    name=environment_name,
                    description=f"Auto-created {environment_name} environment"
                )
            
            # Build base directory path - find repo root and use correct location
            current_dir = Path(os.getcwd())
            if current_dir.name == "web_ui":
                repo_root = current_dir.parent
            else:
                # Try to find repo root by looking for characteristic files
                repo_root = current_dir
                for _ in range(5):  # Search up to 5 levels
                    if (repo_root / "README.md").exists() and (repo_root / "scripts").exists() and (repo_root / "web_ui").exists():
                        break
                    repo_root = repo_root.parent
                else:
                    # Fallback: assume current directory
                    repo_root = current_dir
            
            base_dir = repo_root / "metadata-manager" / environment_name / "tags"
            base_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Using base directory for remote tag staged changes: {base_dir}")
            
            # Add remote tag to staged changes using the comprehensive function
            result = add_tag_to_staged_changes(
                tag_data=tag_mcp_data,
                environment=environment_name,
                owner=owner,
                base_dir=str(base_dir),
                mutation_name=mutation_name
            )
            
            # Provide feedback about files created
            files_created = list(result.values())
            files_created_count = len(files_created)
            
            # Calculate expected files (now 1 combined MCP file instead of separate files)
            expected_files = 1  # single combined MCP file
            
            files_skipped_count = expected_files - files_created_count
            
            if files_skipped_count > 0:
                message = f"Remote tag added to staged changes: {files_created_count} file created, {files_skipped_count} file skipped (unchanged)"
            else:
                message = f"Remote tag added to staged changes: {files_created_count} file created"
            
            # Return success response
            return JsonResponse({
                "status": "success",
                "message": message,
                "files_created": files_created,
                "files_created_count": files_created_count,
                "files_skipped_count": files_skipped_count
            })
                
        except Exception as e:
            logger.error(f"Error adding remote tag to staged changes: {str(e)}")
            return JsonResponse({"error": str(e)}, status=500)


@method_decorator(csrf_exempt, name="dispatch")
class TagSyncToDataHubView(View):
    """View to sync local tags to DataHub with ownership support"""

    def post(self, request, tag_id):
        """Sync a local tag to DataHub"""
        try:
            # Find the tag by ID using flexible search
            tag = find_tag_by_flexible_id(tag_id)
            if not tag:
                return JsonResponse({
                    "success": False,
                    "error": f"Tag with ID {tag_id} not found"
                }, status=404)

            # Get current connection from request session
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            
            # Check if this tag already has a connection and it's different from current connection
            if tag.connection is not None and current_connection and tag.connection != current_connection:
                # Create a new row for the current connection instead of modifying existing one
                logger.info(f"Tag '{tag.name}' has connection {tag.connection.name}, creating new row for {current_connection.name}")
                
                # Check if a tag with same datahub_id already exists for current connection
                existing_for_current_connection = Tag.objects.filter(
                    datahub_id=tag.datahub_id,
                    connection=current_connection
                ).first()
                
                if existing_for_current_connection:
                    return JsonResponse({
                        "success": False,
                        "error": f"Tag '{tag.name}' already exists for the current connection"
                    }, status=400)
                
                # Create new tag for current connection
                new_tag = Tag.objects.create(
                    name=tag.name,
                    description=tag.description,
                    color=tag.color,
                    urn=tag.urn,
                    datahub_id=tag.datahub_id,
                    ownership_data=tag.ownership_data,
                    sync_status="LOCAL_ONLY",  # Will be updated to SYNCED after successful sync
                    connection=current_connection,
                )
                tag = new_tag  # Use the new tag for the rest of the sync process

            # Get DataHub client
            client = get_datahub_client_from_request(request)
            if not client:
                return JsonResponse({
                    "success": False,
                    "error": "Could not connect to DataHub. Check your connection settings."
                }, status=500)

            # Test connection
            if not client.test_connection():
                return JsonResponse({
                    "success": False,
                    "error": "Could not connect to DataHub. Check your connection settings."
                }, status=500)

            # Get the tag ID portion from URN
            tag_id_portion = (
                str(tag.urn).split(":")[-1]
                if tag.urn
                else None
            )
            if not tag_id_portion:
                return JsonResponse({
                    "success": False,
                    "error": "Invalid tag URN"
                }, status=400)

            # Create or update the tag in DataHub
            result = client.create_or_update_tag(
                tag_id=tag_id_portion, 
                name=tag.name, 
                description=tag.description
            )

            if not result:
                return JsonResponse({
                    "success": False,
                    "error": f"Failed to create or update tag '{tag.name}' in DataHub. Check DataHub logs for more details."
                }, status=500)

            # Set color if specified and not empty
            if tag.color and tag.color.strip() and tag.color != "#0d6efd":
                try:
                    client.set_tag_color(result, tag.color)
                except Exception as e:
                    logger.warning(f"Failed to set tag color: {str(e)}")

            # Handle ownership - ensure only intended owners are set
            intended_owners = []
            if tag.ownership_data and isinstance(tag.ownership_data, dict):
                owners = tag.ownership_data.get("owners", [])
                for owner in owners:
                    try:
                        # Handle the standardized UI format
                        owner_urn = owner.get("owner_urn") or owner.get("owner")
                        ownership_type = owner.get("ownership_type_urn") or owner.get("type", "urn:li:ownershipType:__system__technical_owner")
                        
                        if owner_urn:
                            client.add_tag_owner(result, owner_urn, ownership_type)
                            intended_owners.append((owner_urn, ownership_type))
                    except Exception as e:
                        logger.warning(f"Failed to add owner {owner_urn}: {str(e)}")

            # Remove the sync user from ownership if they're not in the intended owners list
            try:
                current_user = client.get_current_user()
                
                if current_user and current_user.get("urn"):
                    sync_user_urn = current_user["urn"]
                    
                    # Check if sync user is in intended owners
                    sync_user_in_intended = any(owner_urn == sync_user_urn for owner_urn, _ in intended_owners)
                    
                    if not sync_user_in_intended:
                        logger.debug(f"Removing sync user {sync_user_urn} from tag '{tag.name}' ownership")
                        # Remove sync user with common ownership types
                        common_ownership_types = [
                            "urn:li:ownershipType:__system__technical_owner",
                            "urn:li:ownershipType:__system__business_owner",
                            "urn:li:ownershipType:__system__data_steward"
                        ]
                        
                        for ownership_type in common_ownership_types:
                            try:
                                client.remove_tag_owner(result, sync_user_urn, ownership_type)
                                logger.debug(f"Removed sync user {sync_user_urn} with ownership type {ownership_type}")
                            except Exception as e:
                                # It's normal for this to fail if the user doesn't have this ownership type
                                logger.debug(f"Could not remove sync user with ownership type {ownership_type}: {str(e)}")
                    else:
                        logger.debug(f"Sync user {sync_user_urn} is in intended owners list, keeping them")
                else:
                    logger.debug(f"Could not get current user information from DataHub for ownership cleanup")
            except Exception as e:
                logger.warning(f"Failed to handle sync user ownership cleanup: {str(e)}")

            # Update tag with remote info
            tag.urn = result  # Store the DataHub URN
            tag.datahub_id = tag_id_portion
            tag.sync_status = "SYNCED"
            tag.last_synced = timezone.now()
            if current_connection:
                tag.connection = current_connection
            tag.save()

            return JsonResponse({
                "success": True,
                "message": f"Tag '{tag.name}' successfully synced to DataHub",
                "tag_urn": result
            })

        except Exception as e:
            logger.error(f"Error syncing tag to DataHub: {str(e)}")
            return JsonResponse({
                "success": False,
                "error": str(e)
            }, status=500)


@method_decorator(csrf_exempt, name="dispatch")
class TagPushToDataHubView(View):
    """View to push modified synced tags to DataHub (update existing tags)"""

    def post(self, request, tag_id):
        """Push a modified synced tag to DataHub"""
        try:
            # Find the tag by ID using flexible search
            tag = find_tag_by_flexible_id(tag_id)
            if not tag:
                return JsonResponse({
                    "success": False,
                    "error": f"Tag with ID {tag_id} not found"
                }, status=404)

            # Ensure this is a synced tag that can be pushed
            if tag.sync_status not in ['MODIFIED', 'SYNCED']:
                return JsonResponse({
                    "success": False,
                    "error": f"Cannot push tag with status '{tag.sync_status}'. Only MODIFIED or SYNCED tags can be pushed."
                }, status=400)

            # Get DataHub client
            client = get_datahub_client_from_request(request)
            if not client:
                return JsonResponse({
                    "success": False,
                    "error": "Could not connect to DataHub. Check your connection settings."
                }, status=500)

            # Test connection
            if not client.test_connection():
                return JsonResponse({
                    "success": False,
                    "error": "Could not connect to DataHub. Check your connection settings."
                }, status=500)

            # For pushing, we should use the existing URN to update the tag
            if not tag.urn:
                return JsonResponse({
                    "success": False,
                    "error": "Cannot push tag without URN. This tag may not be properly synced."
                }, status=400)

            # Update the tag description if it's different
            if tag.description:
                success = client.update_tag_description(tag.urn, tag.description)
                if not success:
                    return JsonResponse({
                        "success": False,
                        "error": f"Failed to update tag description in DataHub"
                    }, status=500)

            # Set color if specified and not empty
            if tag.color and tag.color.strip() and tag.color != "#0d6efd":
                try:
                    client.set_tag_color(tag.urn, tag.color)
                except Exception as e:
                    logger.warning(f"Failed to set tag color: {str(e)}")

            # Handle ownership - ensure only intended owners are set
            intended_owners = []
            if tag.ownership_data and isinstance(tag.ownership_data, dict):
                owners = tag.ownership_data.get("owners", [])
                for owner in owners:
                    try:
                        # Handle the standardized UI format
                        owner_urn = owner.get("owner_urn") or owner.get("owner")
                        ownership_type = owner.get("ownership_type_urn") or owner.get("type", "urn:li:ownershipType:__system__technical_owner")
                        
                        if owner_urn:
                            client.add_tag_owner(tag.urn, owner_urn, ownership_type)
                            intended_owners.append((owner_urn, ownership_type))
                    except Exception as e:
                        logger.warning(f"Failed to add owner {owner_urn}: {str(e)}")

            # Remove the sync user from ownership if they're not in the intended owners list
            try:
                current_user = client.get_current_user()
                
                if current_user and current_user.get("urn"):
                    sync_user_urn = current_user["urn"]
                    
                    # Check if sync user is in intended owners
                    sync_user_in_intended = any(owner_urn == sync_user_urn for owner_urn, _ in intended_owners)
                    
                    if not sync_user_in_intended:
                        logger.debug(f"Removing sync user {sync_user_urn} from tag '{tag.name}' ownership")
                        # Remove sync user with common ownership types
                        common_ownership_types = [
                            "urn:li:ownershipType:__system__technical_owner",
                            "urn:li:ownershipType:__system__business_owner",
                            "urn:li:ownershipType:__system__data_steward"
                        ]
                        
                        for ownership_type in common_ownership_types:
                            try:
                                client.remove_tag_owner(tag.urn, sync_user_urn, ownership_type)
                                logger.debug(f"Removed sync user {sync_user_urn} with ownership type {ownership_type}")
                            except Exception as e:
                                # It's normal for this to fail if the user doesn't have this ownership type
                                logger.debug(f"Could not remove sync user with ownership type {ownership_type}: {str(e)}")
                    else:
                        logger.debug(f"Sync user {sync_user_urn} is in intended owners list, keeping them")
                else:
                    logger.debug(f"Could not get current user information from DataHub for ownership cleanup")
            except Exception as e:
                logger.warning(f"Failed to handle sync user ownership cleanup: {str(e)}")

            # Update tag sync status
            tag.sync_status = "SYNCED"
            tag.last_synced = timezone.now()
            tag.save()

            return JsonResponse({
                "success": True,
                "message": f"Tag '{tag.name}' successfully pushed to DataHub",
                "tag_urn": tag.urn
            })

        except Exception as e:
            logger.error(f"Error pushing tag to DataHub: {str(e)}")
            return JsonResponse({
                "success": False,
                "error": str(e)
            }, status=500)


@method_decorator(csrf_exempt, name="dispatch")
class TagBulkSyncToDataHubView(View):
    """View to bulk sync multiple local tags to DataHub"""

    def post(self, request):
        """Bulk sync local tags to DataHub"""
        try:
            data = json.loads(request.body)
            tag_ids = data.get("tag_ids", [])
            
            if not tag_ids:
                return JsonResponse({
                    "success": False,
                    "error": "No tags selected for sync"
                }, status=400)

            # Get DataHub client
            client = get_datahub_client_from_request(request)
            if not client:
                return JsonResponse({
                    "success": False,
                    "error": "Could not connect to DataHub. Check your connection settings."
                }, status=500)

            # Test connection
            if not client.test_connection():
                return JsonResponse({
                    "success": False,
                    "error": "Could not connect to DataHub. Check your connection settings."
                }, status=500)

            success_count = 0
            error_count = 0
            errors = []

            for tag_id in tag_ids:
                try:
                    tag = Tag.objects.get(id=tag_id)
                    
                    # Get the tag ID portion from URN
                    tag_id_portion = (
                        str(tag.urn).split(":")[-1]
                        if tag.urn
                        else None
                    )
                    if not tag_id_portion:
                        errors.append(f"Invalid URN for tag '{tag.name}'")
                        error_count += 1
                        continue

                    # Create or update the tag in DataHub
                    result = client.create_or_update_tag(
                        tag_id=tag_id_portion, 
                        name=tag.name, 
                        description=tag.description
                    )

                    if not result:
                        errors.append(f"Failed to create or update tag '{tag.name}' in DataHub")
                        error_count += 1
                        continue

                    # Set color if specified and not empty
                    if tag.color and tag.color.strip() and tag.color != "#0d6efd":
                        try:
                            client.set_tag_color(result, tag.color)
                        except Exception as e:
                            logger.warning(f"Failed to set color for tag '{tag.name}': {str(e)}")

                    # Handle ownership - ensure only intended owners are set
                    intended_owners = []
                    if tag.ownership_data and isinstance(tag.ownership_data, dict):
                        owners = tag.ownership_data.get("owners", [])
                        for owner in owners:
                            try:
                                # Handle the standardized UI format
                                owner_urn = owner.get("owner_urn") or owner.get("owner")
                                ownership_type = owner.get("ownership_type_urn") or owner.get("type", "urn:li:ownershipType:__system__technical_owner")
                                
                                if owner_urn:
                                    client.add_tag_owner(result, owner_urn, ownership_type)
                                    intended_owners.append((owner_urn, ownership_type))
                            except Exception as e:
                                logger.warning(f"Failed to add owner {owner_urn} to tag '{tag.name}': {str(e)}")

                    # Remove the sync user from ownership if they're not in the intended owners list
                    try:
                        current_user = client.get_current_user()
                        if current_user and current_user.get("urn"):
                            sync_user_urn = current_user["urn"]
                            
                            # Check if sync user is in intended owners
                            sync_user_in_intended = any(owner_urn == sync_user_urn for owner_urn, _ in intended_owners)
                            
                            if not sync_user_in_intended:
                                # Remove sync user with common ownership types
                                common_ownership_types = [
                                    "urn:li:ownershipType:__system__technical_owner",
                                    "urn:li:ownershipType:__system__business_owner",
                                    "urn:li:ownershipType:__system__data_steward"
                                ]
                                
                                for ownership_type in common_ownership_types:
                                    try:
                                        client.remove_tag_owner(result, sync_user_urn, ownership_type)
                                        logger.debug(f"Removed sync user {sync_user_urn} from tag '{tag.name}' with ownership type {ownership_type}")
                                    except Exception as e:
                                        # It's normal for this to fail if the user doesn't have this ownership type
                                        logger.debug(f"Could not remove sync user from tag '{tag.name}' with ownership type {ownership_type}: {str(e)}")
                                else:
                                    logger.debug(f"Sync user {sync_user_urn} is in intended owners list for tag '{tag.name}', keeping them")
                            else:
                                logger.debug(f"Could not get current user information from DataHub for ownership cleanup")
                        else:
                            logger.debug(f"Could not get current user information from DataHub for ownership cleanup")
                    except Exception as e:
                        logger.warning(f"Failed to handle sync user ownership cleanup for tag '{tag.name}': {str(e)}")

                    # Get current connection from request session
                    from web_ui.views import get_current_connection
                    current_connection = get_current_connection(request)
                    
                    # Update tag with remote info
                    tag.urn = result  # Store the DataHub URN
                    tag.datahub_id = tag_id_portion
                    tag.sync_status = "SYNCED"
                    tag.last_synced = timezone.now()
                    if current_connection:
                        tag.connection = current_connection
                    tag.save()

                    success_count += 1

                except Tag.DoesNotExist:
                    errors.append(f"Tag with ID {tag_id} not found")
                    error_count += 1
                except Exception as e:
                    errors.append(f"Error syncing tag with ID {tag_id}: {str(e)}")
                    error_count += 1

            return JsonResponse({
                "success": True,
                "synced": success_count,
                "errors": error_count,
                "error_details": errors,
                "message": f"Bulk sync completed: {success_count} synced, {error_count} errors"
            })

        except Exception as e:
            logger.error(f"Error in bulk sync: {str(e)}")
            return JsonResponse({
                "success": False,
                "error": str(e)
            }, status=500)


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
            tag = find_tag_by_flexible_id(tag_id)
            if not tag:
                return JsonResponse({
                    "success": False,
                    "error": f"Tag with ID {tag_id} not found"
                }, status=404)
            
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


@method_decorator(csrf_exempt, name="dispatch")
class TagBulkResyncView(View):
    """API view for bulk resyncing tags from DataHub"""
    
    def post(self, request):
        try:
            data = json.loads(request.body)
            tag_ids = data.get('tag_ids', [])
            
            if not tag_ids:
                return JsonResponse({
                    'success': False,
                    'error': 'No tag IDs provided'
                }, status=400)
            
            # Get DataHub client
            client = get_datahub_client_from_request(request)
            if not client:
                return JsonResponse({
                    'success': False,
                    'error': 'Unable to connect to DataHub'
                }, status=500)
            
            success_count = 0
            error_count = 0
            errors = []
            
            for tag_id in tag_ids:
                try:
                    # Find the tag
                    tag = find_tag_by_flexible_id(tag_id)
                    if not tag:
                        error_count += 1
                        errors.append(f"Tag {tag_id}: Not found")
                        continue
                    
                    # Get remote tag data
                    if tag.urn:
                        remote_tag = client.get_tag(tag.urn)
                        if remote_tag:
                            # Update tag with remote data - refresh completely with DataHub version
                            properties = remote_tag.get("properties") if remote_tag else None
                            if properties is None:
                                properties = {}
                            
                            tag_name = properties.get("name") if properties else None
                            if not tag_name:
                                tag_name = remote_tag.get("name") if remote_tag else tag.name
                            
                            tag.name = tag_name or tag.name
                            tag.description = properties.get("description", "") if properties else (remote_tag.get("description", "") if remote_tag else "")
                            tag.color = properties.get("colorHex") if properties else None

                            # Extract and update datahub_id from URN
                            if tag.urn and ':' in tag.urn:
                                tag.datahub_id = tag.urn.split(':')[-1]
                            
                            # Get current connection and set it
                            from web_ui.views import get_current_connection
                            current_connection = get_current_connection(request)
                            if current_connection:
                                tag.connection = current_connection
                            
                            # Convert and update ownership data from DataHub format to local format
                            ownership_data = None
                            remote_ownership = remote_tag.get("ownership") if remote_tag else None
                            if remote_ownership and remote_ownership.get("owners"):
                                # Convert DataHub ownership format to our local format
                                owners_list = []
                                for owner_info in remote_ownership["owners"]:
                                    if not owner_info:
                                        continue
                                    owner = owner_info.get("owner", {}) if owner_info else {}
                                    ownership_type = owner_info.get("ownershipType", {}) if owner_info else {}
                                    
                                    owner_urn = owner.get("urn", "") if owner else ""
                                    ownership_type_urn = ownership_type.get("urn", "urn:li:ownershipType:__system__technical_owner") if ownership_type else "urn:li:ownershipType:__system__technical_owner"
                                    
                                    if owner_urn:
                                        owners_list.append({
                                            "owner_urn": owner_urn,
                                            "ownership_type_urn": ownership_type_urn
                                        })
                                
                                if owners_list:
                                    ownership_data = {"owners": owners_list}
                            
                            # Set ownership data (clear if none in remote)
                            tag.ownership_data = ownership_data
                            
                            # Mark as synced and update sync timestamp
                            tag.sync_status = "SYNCED"
                            tag.last_synced = timezone.now()
                            tag.save()
                            success_count += 1
                            logger.info(f"Successfully resynced tag '{tag.name}' from DataHub")
                        else:
                            error_count += 1
                            errors.append(f"Tag {tag.name}: Not found in DataHub")
                    else:
                        error_count += 1
                        errors.append(f"Tag {tag.name}: No URN available")
                        
                except Exception as e:
                    error_count += 1
                    errors.append(f"Tag {tag_id}: {str(e)}")
                    logger.error(f"Error resyncing tag {tag_id}: {str(e)}")
            
            message = f"Resync completed: {success_count} successful, {error_count} failed"
            if errors:
                message += f". Errors: {'; '.join(errors[:5])}"  # Show first 5 errors
                if len(errors) > 5:
                    message += f" and {len(errors) - 5} more..."
            
            return JsonResponse({
                'success': True,
                'message': message,
                'success_count': success_count,
                'error_count': error_count,
                'errors': errors
            })
            
        except Exception as e:
            logger.error(f"Error in bulk resync: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)


@method_decorator(csrf_exempt, name="dispatch")
class TagResyncAllView(View):
    """API view for resyncing all tags from DataHub"""
    
    def post(self, request):
        try:
            # Get DataHub client
            client = get_datahub_client_from_request(request)
            if not client:
                return JsonResponse({
                    'success': False,
                    'error': 'Unable to connect to DataHub'
                }, status=500)
            
            # Get all tags with URNs
            tags = Tag.objects.exclude(urn__isnull=True).exclude(urn='')
            
            success_count = 0
            error_count = 0
            errors = []
            
            for tag in tags:
                try:
                    # Get remote tag data
                    remote_tag = client.get_tag(tag.urn)
                    if remote_tag:
                        # Update tag with remote data - refresh completely with DataHub version
                        properties = remote_tag.get("properties") if remote_tag else None
                        if properties is None:
                            properties = {}
                        
                        tag_name = properties.get("name") if properties else None
                        if not tag_name:
                            tag_name = remote_tag.get("name") if remote_tag else tag.name
                        
                        tag.name = tag_name or tag.name
                        tag.description = properties.get("description", "") if properties else (remote_tag.get("description", "") if remote_tag else "")
                        tag.color = properties.get("colorHex") if properties else None

                        
                        # Extract and update datahub_id from URN
                        if tag.urn and ':' in tag.urn:
                            tag.datahub_id = tag.urn.split(':')[-1]
                        
                        # Get current connection and set it
                        from web_ui.views import get_current_connection
                        current_connection = get_current_connection(request)
                        if current_connection:
                            tag.connection = current_connection
                        
                        # Convert and update ownership data from DataHub format to local format
                        ownership_data = None
                        remote_ownership = remote_tag.get("ownership") if remote_tag else None
                        if remote_ownership and remote_ownership.get("owners"):
                            # Convert DataHub ownership format to our local format
                            owners_list = []
                            for owner_info in remote_ownership["owners"]:
                                if not owner_info:
                                    continue
                                owner = owner_info.get("owner", {}) if owner_info else {}
                                ownership_type = owner_info.get("ownershipType", {}) if owner_info else {}
                                
                                owner_urn = owner.get("urn", "") if owner else ""
                                ownership_type_urn = ownership_type.get("urn", "urn:li:ownershipType:__system__technical_owner") if ownership_type else "urn:li:ownershipType:__system__technical_owner"
                                
                                if owner_urn:
                                    owners_list.append({
                                        "owner_urn": owner_urn,
                                        "ownership_type_urn": ownership_type_urn
                                    })
                            
                            if owners_list:
                                ownership_data = {"owners": owners_list}
                        
                        # Set ownership data (clear if none in remote)
                        tag.ownership_data = ownership_data
                        
                        # Mark as synced and update sync timestamp
                        tag.sync_status = "SYNCED"
                        tag.last_synced = timezone.now()
                        tag.save()
                        success_count += 1
                        logger.info(f"Successfully resynced tag '{tag.name}' from DataHub")
                    else:
                        error_count += 1
                        errors.append(f"Tag {tag.name}: Not found in DataHub")
                        
                except Exception as e:
                    error_count += 1
                    errors.append(f"Tag {tag.name}: {str(e)}")
                    logger.error(f"Error resyncing tag {tag.name}: {str(e)}")
            
            message = f"Resync all completed: {success_count} successful, {error_count} failed"
            if errors:
                message += f". Errors: {'; '.join(errors[:5])}"  # Show first 5 errors
                if len(errors) > 5:
                    message += f" and {len(errors) - 5} more..."
            
            return JsonResponse({
                'success': True,
                'message': message,
                'success_count': success_count,
                'error_count': error_count,
                'errors': errors
            })
            
        except Exception as e:
            logger.error(f"Error in resync all: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)


@method_decorator(csrf_exempt, name="dispatch")
class TagExportAllView(View):
    """API view for exporting all tags as JSON"""
    
    def get(self, request):
        try:
            # Get current connection to filter tags by connection
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            
            # Get tags relevant to current connection
            # Include: tags with no connection (local-only) + tags with current connection (synced)
            # BUT: if there's a synced tag for current connection with same datahub_id, hide the other connection's version
            all_tags = Tag.objects.all()
            tags = filter_tags_by_connection(all_tags, current_connection)
            
            tags_data = []
            for tag in tags:
                tag_data = {
                    "id": str(tag.id),
                    "name": tag.name,
                    "description": tag.description,
                    "color": tag.color,
                    "urn": tag.urn,
                    "datahub_id": tag.datahub_id,
                    "sync_status": tag.sync_status,
                    "last_synced": tag.last_synced.isoformat() if tag.last_synced else None,
                    "ownership_data": tag.ownership_data,
                    "connection_id": tag.connection.id if tag.connection else None,
                    "connection_name": tag.connection.name if tag.connection else None,
                    "created_at": tag.created_at.isoformat() if hasattr(tag, 'created_at') and tag.created_at else None,
                    "updated_at": tag.updated_at.isoformat() if hasattr(tag, 'updated_at') and tag.updated_at else None
                }
                tags_data.append(tag_data)
            
            # Create export data
            export_data = {
                "export_info": {
                    "exported_at": timezone.now().isoformat(),
                    "total_tags": len(tags_data),
                    "export_source": "DataHub CI/CD Manager"
                },
                "tags": tags_data
            }
            
            from django.http import HttpResponse
            import json
            
            # Create response with JSON data
            response = HttpResponse(
                json.dumps(export_data, indent=2, default=str),
                content_type='application/json'
            )
            
            # Set filename for download
            timestamp = timezone.now().strftime('%Y%m%d_%H%M%S')
            filename = f'tags_export_{timestamp}.json'
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
            
            return response
            
        except Exception as e:
            logger.error(f"Error exporting all tags: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)


@method_decorator(csrf_exempt, name="dispatch")
class TagImportJsonView(View):
    """API view for importing tags from JSON file"""
    
    def post(self, request):
        try:
            # Check if file was uploaded
            if 'import_file' not in request.FILES:
                return JsonResponse({
                    'success': False,
                    'error': 'No file uploaded'
                }, status=400)
            
            file = request.FILES['import_file']
            overwrite_existing = request.POST.get('overwrite_existing', 'true').lower() == 'true'
            
            # Read and parse JSON file
            try:
                file_content = file.read().decode('utf-8')
                data = json.loads(file_content)
            except Exception as e:
                return JsonResponse({
                    'success': False,
                    'error': f'Invalid JSON file: {str(e)}'
                }, status=400)
            
            # Extract tags data
            tags_data = data.get('tags', [])
            if not tags_data:
                return JsonResponse({
                    'success': False,
                    'error': 'No tags found in JSON file'
                }, status=400)
            
            # Get current connection
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            
            success_count = 0
            error_count = 0
            skipped_count = 0
            errors = []
            
            for tag_data in tags_data:
                try:
                    name = tag_data.get('name')
                    if not name:
                        error_count += 1
                        errors.append("Tag without name skipped")
                        continue
                    
                    # Check if tag already exists for current connection
                    existing_tag = None
                    if tag_data.get('urn'):
                        if current_connection:
                            existing_tag = Tag.objects.filter(urn=tag_data['urn'], connection=current_connection).first()
                        else:
                            existing_tag = Tag.objects.filter(urn=tag_data['urn'], connection__isnull=True).first()
                    if not existing_tag and tag_data.get('datahub_id'):
                        if current_connection:
                            existing_tag = Tag.objects.filter(datahub_id=tag_data['datahub_id'], connection=current_connection).first()
                        else:
                            existing_tag = Tag.objects.filter(datahub_id=tag_data['datahub_id'], connection__isnull=True).first()
                    if not existing_tag:
                        if current_connection:
                            existing_tag = Tag.objects.filter(name=name, connection=current_connection).first()
                        else:
                            existing_tag = Tag.objects.filter(name=name, connection__isnull=True).first()
                    
                    if existing_tag and not overwrite_existing:
                        skipped_count += 1
                        continue
                    
                    # Create or update tag
                    if existing_tag:
                        tag = existing_tag
                    else:
                        tag = Tag()
                    
                    # Set tag fields
                    tag.name = name
                    tag.description = tag_data.get('description', '')
                    tag.color = tag_data.get('color')

                    tag.urn = tag_data.get('urn', '')
                    tag.datahub_id = tag_data.get('datahub_id', '')
                    # Determine sync status and connection based on import context
                    if tag_data.get('sync_status') == 'SYNCED' or tag_data.get('connection_id'):
                        # Tag was previously synced, set connection and mark as synced
                        tag.sync_status = 'SYNCED'
                        tag.last_synced = timezone.now()
                        tag.connection = current_connection
                    else:
                        # Tag is local-only, no connection until synced
                        tag.sync_status = 'LOCAL_ONLY'
                        tag.connection = None
                    
                    tag.ownership_data = tag_data.get('ownership_data')
                    
                    # Generate URN if missing
                    if not tag.urn and name:
                        from datahub_cicd_client.integrations.urn_utils import generate_deterministic_urn
                        tag.urn = generate_deterministic_urn("tag", name)
                    
                    tag.save()
                    success_count += 1
                    
                except Exception as e:
                    error_count += 1
                    errors.append(f"Tag {tag_data.get('name', 'unknown')}: {str(e)}")
                    logger.error(f"Error importing tag: {str(e)}")
            
            message = f"Import completed: {success_count} imported, {skipped_count} skipped, {error_count} failed"
            if errors:
                message += f". Errors: {'; '.join(errors[:5])}"  # Show first 5 errors
                if len(errors) > 5:
                    message += f" and {len(errors) - 5} more..."
            
            return JsonResponse({
                'success': True,
                'message': message,
                'success_count': success_count,
                'skipped_count': skipped_count,
                'error_count': error_count,
                'errors': errors
            })
            
        except Exception as e:
            logger.error(f"Error importing tags: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)


@method_decorator(csrf_exempt, name="dispatch")
class TagAddAllToStagedChangesView(View):
    """API view for adding all tags to staged changes"""
    
    def post(self, request):
        try:
            data = json.loads(request.body)
            environment = data.get('environment', 'dev')
            mutation_name = data.get('mutation_name')
            
            # Get current connection to filter tags by connection
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            
            # Get tags relevant to current connection
            # Include: tags with no connection (local-only) + tags with current connection (synced)
            # BUT: if there's a synced tag for current connection with same datahub_id, hide the other connection's version
            all_tags = Tag.objects.all()
            tags = filter_tags_by_connection(all_tags, current_connection)
            
            if not tags:
                return JsonResponse({
                    'success': False,
                    'error': 'No tags found to add to staged changes for current connection'
                }, status=400)
            
            # Import the tag actions module
            sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
            from scripts.mcps.tag_actions import add_tag_to_staged_changes
            
            success_count = 0
            error_count = 0
            files_created_count = 0
            files_skipped_count = 0
            errors = []
            all_created_files = []
            
            for tag in tags:
                try:
                    # Convert tag to dictionary format
                    tag_data = {
                        "urn": tag.urn,
                        "name": tag.name,
                        "description": tag.description,
                        "properties": {
                            "name": tag.name,
                            "description": tag.description,
                            "colorHex": tag.color
                        }
                    }
                    
                    # Add tag to staged changes
                    created_files = add_tag_to_staged_changes(
                        tag_data=tag_data,
                        environment=environment,
                        owner="system",  # Default owner
                        mutation_name=mutation_name
                    )
                    
                    success_count += 1
                    files_created_count += len(created_files)
                    all_created_files.extend(list(created_files.values()))
                    
                    # Log deduplication info for individual tags
                    if not created_files:
                        files_skipped_count += 1  # All files were skipped due to deduplication
                    
                except Exception as e:
                    error_count += 1
                    errors.append(f"Tag {tag.name}: {str(e)}")
                    logger.error(f"Error adding tag {tag.name} to staged changes: {str(e)}")
            
            # Calculate total files that could have been created (now 1 combined MCP file per tag)
            total_possible_files = success_count * 1  # single combined MCP file for each tag
            
            files_skipped_count = total_possible_files - files_created_count
            
            message = f"Add all to staged changes completed: {success_count} tags processed, {files_created_count} files created, {files_skipped_count} files skipped (unchanged), {error_count} failed"
            if errors:
                message += f". Errors: {'; '.join(errors[:5])}"  # Show first 5 errors
                if len(errors) > 5:
                    message += f" and {len(errors) - 5} more..."
            
            return JsonResponse({
                'success': True,
                'message': message,
                'success_count': success_count,
                'error_count': error_count,
                'files_created_count': files_created_count,
                'files_skipped_count': files_skipped_count,
                'errors': errors,
                'files_created': all_created_files
            })
            
        except Exception as e:
            logger.error(f"Error adding all tags to staged changes: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)


@method_decorator(csrf_exempt, name="dispatch")
class TagResyncView(View):
    """API view for resyncing a single tag from DataHub"""
    
    def post(self, request, tag_id):
        try:
            # Find the tag by ID
            tag = find_tag_by_flexible_id(tag_id)
            if not tag:
                return JsonResponse({
                    'success': False,
                    'error': f'Tag with ID {tag_id} not found'
                }, status=404)
            
            # Get DataHub client
            client = get_datahub_client_from_request(request)
            if not client:
                return JsonResponse({
                    "success": False,
                    "error": "Could not connect to DataHub. Check your connection settings."
                }, status=500)

            # Test connection
            if not client.test_connection():
                return JsonResponse({
                    "success": False,
                    "error": "Could not connect to DataHub. Check your connection settings."
                }, status=500)
            
            # Get the tag URN
            tag_urn = tag.urn
            if not tag_urn:
                return JsonResponse({
                    'success': False,
                    'error': 'Tag has no URN to resync from DataHub'
                }, status=400)
            
            # Fetch tag from DataHub
            try:
                remote_tag = client.get_tag(tag_urn)
                if not remote_tag:
                    return JsonResponse({
                        'success': False,
                        'error': f'Tag not found in DataHub: {tag_urn}'
                    }, status=404)
            except Exception as e:
                return JsonResponse({
                    'success': False,
                    'error': f'Error fetching tag from DataHub: {str(e)}'
                }, status=500)
            
            # Update local tag with remote data - refresh completely with DataHub version
            try:
                logger.info(f"Resyncing tag '{tag.name}' with DataHub data")
                
                # Extract basic properties from remote tag with proper null checks
                properties = remote_tag.get('properties') if remote_tag else None
                if properties is None:
                    properties = {}
                
                tag_name = properties.get('name') if properties else None
                if not tag_name:
                    tag_name = remote_tag.get('name') if remote_tag else tag.name
                if not tag_name and tag_urn:
                    tag_name = tag_urn.split(':')[-1]
                
                # Update all basic tag properties from DataHub
                tag.name = tag_name or tag.name
                tag.description = properties.get('description', '') if properties else (remote_tag.get('description', '') if remote_tag else '')
                tag.color = properties.get('colorHex') if properties else None
                tag.urn = tag_urn
                
                # Extract and update datahub_id from URN
                if tag_urn and ':' in tag_urn:
                    tag.datahub_id = tag_urn.split(':')[-1]
                
                # Get current connection and set it
                from web_ui.views import get_current_connection
                current_connection = get_current_connection(request)
                if current_connection:
                    tag.connection = current_connection
                
                # Convert and update ownership data from DataHub format to local format
                ownership_data = None
                remote_ownership = remote_tag.get('ownership') if remote_tag else None
                if remote_ownership and remote_ownership.get('owners'):
                    # Convert DataHub ownership format to our local format
                    owners_list = []
                    for owner_info in remote_ownership['owners']:
                        if not owner_info:
                            continue
                        owner = owner_info.get('owner', {}) if owner_info else {}
                        ownership_type = owner_info.get('ownershipType', {}) if owner_info else {}
                        
                        owner_urn = owner.get('urn', '') if owner else ''
                        ownership_type_urn = ownership_type.get('urn', 'urn:li:ownershipType:__system__technical_owner') if ownership_type else 'urn:li:ownershipType:__system__technical_owner'
                        
                        if owner_urn:
                            owners_list.append({
                                'owner_urn': owner_urn,
                                'ownership_type_urn': ownership_type_urn
                            })
                    
                    if owners_list:
                        ownership_data = {'owners': owners_list}
                        logger.info(f"Updated ownership data with {len(owners_list)} owners")
                
                # Set ownership data (clear if none in remote)
                tag.ownership_data = ownership_data
                
                # Mark as synced and update sync timestamp
                tag.sync_status = 'SYNCED'
                tag.last_synced = timezone.now()
                
                # Save all changes
                tag.save()
                
                logger.info(f"Successfully resynced tag '{tag.name}' from DataHub")
                return JsonResponse({
                    'success': True,
                    'message': f"Tag '{tag.name}' successfully resynced from DataHub"
                })
                
            except Exception as e:
                logger.error(f"Error updating local tag during resync: {str(e)}")
                return JsonResponse({
                    'success': False,
                    'error': f'Error updating local tag: {str(e)}'
                }, status=500)
            
        except Exception as e:
            logger.error(f"Error resyncing tag: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)


@method_decorator(csrf_exempt, name="dispatch")
class TagDeleteRemoteView(View):
    """API view for deleting a remote tag from DataHub"""
    
    def post(self, request):
        try:
            data = json.loads(request.body)
            tag_urn = data.get('urn')
            
            if not tag_urn:
                return JsonResponse({
                    'success': False,
                    'error': 'Tag URN is required'
                }, status=400)
            
            # Get DataHub client
            client = get_datahub_client_from_request(request)
            if not client:
                return JsonResponse({
                    "success": False,
                    "error": "Could not connect to DataHub. Check your connection settings."
                }, status=500)

            # Test connection
            if not client.test_connection():
                return JsonResponse({
                    "success": False,
                    "error": "Could not connect to DataHub. Check your connection settings."
                }, status=500)
            
            # Delete the tag from DataHub using the deleteTag mutation
            try:
                result = client.delete_tag(tag_urn)
                logger.info(f"Delete tag result: {result}")
                
                # For delete operations, we'll be more lenient about what constitutes success
                # Sometimes the GraphQL response doesn't clearly indicate success/failure
                if result is not False:  # Accept True, None, or other truthy/null values as success
                    logger.info(f"Tag deletion operation completed: {tag_urn}")
                    return JsonResponse({
                        'success': True,
                        'message': f"Tag successfully deleted from DataHub: {tag_urn}"
                    })
                else:
                    logger.warning(f"Tag deletion returned False: {tag_urn}")
                    # Even if result is False, let's check if the tag was actually deleted
                    try:
                        import time
                        time.sleep(0.5)
                        check_tag = client.get_tag(tag_urn)
                        if not check_tag:
                            logger.info(f"Tag confirmed deleted despite False result: {tag_urn}")
                            return JsonResponse({
                                'success': True,
                                'message': f"Tag successfully deleted from DataHub: {tag_urn}"
                            })
                    except Exception:
                        logger.info(f"Tag likely deleted (error fetching): {tag_urn}")
                        return JsonResponse({
                            'success': True,
                            'message': f"Tag successfully deleted from DataHub: {tag_urn}"
                        })
                    
                    return JsonResponse({
                        'success': False,
                        'error': f'Failed to delete tag from DataHub: {tag_urn}'
                    }, status=500)
                    
            except Exception as e:
                logger.error(f"Error deleting tag from DataHub: {str(e)}")
                return JsonResponse({
                    'success': False,
                    'error': f'Error deleting tag from DataHub: {str(e)}'
                }, status=500)
            
        except Exception as e:
            logger.error(f"Error in delete remote tag request: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)