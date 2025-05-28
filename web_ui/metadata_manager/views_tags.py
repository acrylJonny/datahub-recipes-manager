from django.shortcuts import render, redirect, get_object_or_404
from django.views import View
from django.contrib import messages
from django.http import JsonResponse, HttpResponse
import json
import logging
import os
import sys
from django.views.decorators.http import require_POST
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Import the deterministic URN utilities
from utils.urn_utils import generate_deterministic_urn, get_full_urn_from_name
from utils.datahub_utils import get_datahub_client, test_datahub_connection
from web_ui.models import GitSettings
from .models import Tag

logger = logging.getLogger(__name__)

class TagListView(View):
    """View to list and create tags"""
    
    def get(self, request):
        """Display list of tags"""
        try:
            logger.info("Starting TagListView.get")
            
            # Get all tags
            tags = Tag.objects.all().order_by('name')
            logger.debug(f"Found {tags.count()} total tags")
            
            # Get DataHub connection info
            logger.debug("Testing DataHub connection from TagListView")
            connected, client = test_datahub_connection()
            logger.debug(f"DataHub connection test result: {connected}")
            
            # Initialize context
            context = {
                'tags': tags,
                'page_title': 'DataHub Tags',
                'has_datahub_connection': connected,
                'has_git_integration': False
            }
            
            # Fetch remote tags if connected
            synced_tags = []
            local_tags = []
            remote_only_tags = []
            datahub_url = None
            
            if connected and client:
                logger.debug("Connected to DataHub, fetching remote tags")
                try:
                    # Get all remote tags from DataHub
                    remote_tags = client.list_tags(count=1000)
                    logger.debug(f"Fetched {len(remote_tags) if remote_tags else 0} remote tags")
                    
                    # Get DataHub URL for direct links
                    datahub_url = client.server_url
                    if datahub_url.endswith('/api/gms'):
                        datahub_url = datahub_url[:-8]  # Remove /api/gms to get base URL
                    
                    # Extract tag URNs that exist locally
                    local_tag_urns = set(tags.values_list('deterministic_urn', flat=True))
                    
                    # Process tags by comparing local and remote
                    for tag in tags:
                        tag_urn = str(tag.deterministic_urn)
                        remote_match = next((t for t in remote_tags if t.get('urn') == tag_urn), None)
                        
                        if remote_match:
                            synced_tags.append({
                                'local': tag,
                                'remote': remote_match
                            })
                        else:
                            local_tags.append(tag)
                    
                    # Find tags that exist remotely but not locally
                    remote_only_tags = [t for t in remote_tags if t.get('urn') not in local_tag_urns]
                    
                    logger.debug(f"Categorized tags: {len(synced_tags)} synced, {len(local_tags)} local-only, {len(remote_only_tags)} remote-only")
                    
                except Exception as e:
                    logger.error(f"Error fetching remote tag data: {str(e)}")
            else:
                # All tags are local-only if not connected
                local_tags = tags
            
            # Update context with processed tags
            context.update({
                'synced_tags': synced_tags,
                'local_tags': local_tags,
                'remote_only_tags': remote_only_tags,
                'datahub_url': datahub_url
            })
            
            # Check if git integration is enabled
            try:
                github_settings = GitSettings.objects.first()
                context['has_git_integration'] = github_settings and github_settings.enabled
                logger.debug(f"Git integration enabled: {context['has_git_integration']}")
            except Exception as e:
                logger.warning(f"Error checking git integration: {str(e)}")
                pass
            
            logger.info("Rendering tag list template")
            return render(request, 'metadata_manager/tags/list.html', context)
        except Exception as e:
            logger.error(f"Error in tag list view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return render(request, 'metadata_manager/tags/list.html', {
                'error': str(e),
                'page_title': 'DataHub Tags'
            })
    
    def post(self, request):
        """Create a new tag"""
        try:
            name = request.POST.get('name')
            description = request.POST.get('description', '')
            color = request.POST.get('color', '#0d6efd')
            
            if not name:
                messages.error(request, "Tag name is required")
                return redirect('tag_list')
            
            # Generate deterministic URN
            deterministic_urn = get_full_urn_from_name("tag", name)
            
            # Check if tag with this URN already exists
            if Tag.objects.filter(deterministic_urn=deterministic_urn).exists():
                messages.error(request, f"Tag with name '{name}' already exists")
                return redirect('tag_list')
            
            # Create the tag
            tag = Tag.objects.create(
                name=name,
                description=description,
                color=color,
                deterministic_urn=deterministic_urn,
                sync_status='LOCAL_ONLY'
            )
            
            messages.success(request, f"Tag '{name}' created successfully")
            return redirect('tag_list')
        except Exception as e:
            logger.error(f"Error creating tag: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('tag_list')

class TagDetailView(View):
    """View to display, edit and delete tags"""
    
    def get(self, request, tag_id):
        """Display tag details"""
        try:
            tag = get_object_or_404(Tag, id=tag_id)
            
            # Initialize context with tag data
            context = {
                'tag': tag,
                'page_title': f'Tag: {tag.name}',
                'has_git_integration': False  # Set this based on checking GitHub settings
            }
            
            # Check if git integration is enabled
            try:
                github_settings = GitSettings.objects.first()
                context['has_git_integration'] = github_settings and github_settings.enabled
            except:
                pass
            
            # Get related entities if DataHub connection is available
            client = get_datahub_client()
            if client and client.test_connection():
                tag_urn = tag.deterministic_urn
                
                # Find entities with this tag, limit to 50 for performance
                try:
                    related_entities = client.find_entities_with_metadata(
                        field_type="tags",
                        metadata_urn=tag_urn,
                        count=50
                    )
                    
                    # Add to context
                    context['related_entities'] = related_entities.get('entities', [])
                    context['total_related'] = related_entities.get('total', 0)
                    context['has_datahub_connection'] = True
                    
                    # Also add URL for reference
                    if hasattr(client, 'server_url'):
                        context['datahub_url'] = client.server_url
                        
                    # Get remote tag information if possible
                    if tag.sync_status != 'LOCAL_ONLY':
                        try:
                            remote_tag = client.get_tag(tag_urn)
                            if remote_tag:
                                context['remote_tag'] = remote_tag
                                
                                # Check if the tag needs to be synced
                                local_description = tag.description or ""
                                remote_description = remote_tag.get('description', "")
                                
                                properties = remote_tag.get('properties', {}) or {}
                                remote_color = properties.get('colorHex', "#0d6efd")
                                local_color = tag.color or "#0d6efd"
                                
                                # If different, mark as modified
                                if (local_description != remote_description or 
                                   local_color != remote_color) and tag.sync_status != 'MODIFIED':
                                    tag.sync_status = 'MODIFIED'
                                    tag.save(update_fields=['sync_status'])
                                
                                # If the same but marked as modified, update to synced
                                elif (local_description == remote_description and 
                                     local_color == remote_color and tag.sync_status == 'MODIFIED'):
                                    tag.sync_status = 'SYNCED'
                                    tag.save(update_fields=['sync_status'])
                        except Exception as e:
                            logger.warning(f"Error fetching remote tag information: {str(e)}")
                except Exception as e:
                    logger.error(f"Error fetching related entities for tag {tag.name}: {str(e)}")
                    context['has_datahub_connection'] = True  # We still have a connection, just failed to get entities
                    context['related_entities_error'] = str(e)
            else:
                context['has_datahub_connection'] = False
            
            return render(request, 'metadata_manager/tags/detail.html', context)
        except Exception as e:
            logger.error(f"Error in tag detail view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('tag_list')
    
    def post(self, request, tag_id):
        """Update a tag"""
        try:
            tag = get_object_or_404(Tag, id=tag_id)
            
            name = request.POST.get('name')
            description = request.POST.get('description', '')
            color = request.POST.get('color', tag.color)
            
            if not name:
                messages.error(request, "Tag name is required")
                return redirect('tag_detail', tag_id=tag.id)
            
            # If name changed, update deterministic URN
            if name != tag.name:
                new_urn = get_full_urn_from_name("tag", name)
                # Check if another tag with this URN already exists
                if Tag.objects.filter(deterministic_urn=new_urn).exclude(id=tag.id).exists():
                    messages.error(request, f"Tag with name '{name}' already exists")
                    return redirect('tag_detail', tag_id=tag.id)
                tag.deterministic_urn = new_urn
            
            # If this tag exists remotely and we're changing details, mark as modified
            if tag.sync_status in ['SYNCED', 'REMOTE_ONLY'] and (tag.description != description or tag.color != color):
                tag.sync_status = 'MODIFIED'
            
            # Update the tag
            tag.name = name
            tag.description = description
            tag.color = color
            tag.save()
            
            messages.success(request, f"Tag '{name}' updated successfully")
            return redirect('tag_detail', tag_id=tag.id)
        except Exception as e:
            logger.error(f"Error updating tag: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('tag_detail', tag_id=tag.id)
    
    def delete(self, request, tag_id):
        """Delete a tag"""
        try:
            tag = get_object_or_404(Tag, id=tag_id)
            name = tag.name
            
            # If this tag exists remotely, try to delete it there too
            if tag.sync_status in ['SYNCED', 'REMOTE_ONLY', 'MODIFIED']:
                client = get_datahub_client()
                if client:
                    try:
                        result = client.delete_tag(tag.deterministic_urn)
                        if result:
                            logger.info(f"Successfully deleted tag {tag.name} from DataHub")
                    except Exception as e:
                        logger.warning(f"Failed to delete tag from DataHub: {str(e)}")
            
            # Delete the local tag regardless
            tag.delete()
            
            return JsonResponse({
                'success': True,
                'message': f"Tag '{name}' deleted successfully"
            })
        except Exception as e:
            logger.error(f"Error deleting tag: {str(e)}")
            return JsonResponse({
                'success': False,
                'message': f"An error occurred: {str(e)}"
            }, status=500)

class TagImportExportView(View):
    """View to handle tag import and export"""
    
    def get(self, request):
        """Display import/export page or export all tags"""
        try:
            return render(request, 'metadata_manager/tags/import_export.html', {
                'page_title': 'Import/Export Tags'
            })
        except Exception as e:
            logger.error(f"Error in tag import/export view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('tag_list')
    
    def post(self, request):
        """Import tags from JSON file"""
        try:
            import_file = request.FILES.get('import_file')
            if not import_file:
                messages.error(request, "No file was uploaded")
                return redirect('tag_import_export')
            
            # Read the file
            try:
                tag_data = json.loads(import_file.read().decode('utf-8'))
            except json.JSONDecodeError:
                messages.error(request, "Invalid JSON file")
                return redirect('tag_import_export')
            
            # Process tags
            imported_count = 0
            errors = []
            
            for i, tag_item in enumerate(tag_data):
                try:
                    name = tag_item.get('name')
                    description = tag_item.get('description', '')
                    color = tag_item.get('color', '#0d6efd')
                    
                    if not name:
                        errors.append(f"Item #{i+1}: Missing required field 'name'")
                        continue
                    
                    # Generate deterministic URN
                    deterministic_urn = get_full_urn_from_name("tag", name)
                    
                    # Check if tag exists, update it if it does
                    tag, created = Tag.objects.update_or_create(
                        deterministic_urn=deterministic_urn,
                        defaults={
                            'name': name,
                            'description': description,
                            'color': color
                        }
                    )
                    
                    imported_count += 1
                except Exception as e:
                    errors.append(f"Item #{i+1}: {str(e)}")
            
            # Report results
            if imported_count > 0:
                messages.success(request, f"Successfully imported {imported_count} tags")
            
            if errors:
                messages.warning(request, f"Encountered {len(errors)} errors during import")
                for error in errors[:5]:  # Show the first 5 errors
                    messages.error(request, error)
                if len(errors) > 5:
                    messages.error(request, f"... and {len(errors) - 5} more errors")
            
            return redirect('tag_list')
        except Exception as e:
            logger.error(f"Error importing tags: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('tag_import_export')

@method_decorator(require_POST, name='dispatch')
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
                messages.error(request, "Could not connect to DataHub. Check your connection settings.")
                return redirect('tag_list')
            
            # Deploy to DataHub
            # Ensure the deterministic_urn is a string before splitting
            tag_id_portion = str(tag.deterministic_urn).split(':')[-1] if tag.deterministic_urn else None
            if not tag_id_portion:
                messages.error(request, "Invalid tag URN")
                return redirect('tag_detail', tag_id=tag.id)
            
            # Create or update the tag in DataHub
            result = client.create_tag(
                tag_id=tag_id_portion,
                name=tag.name,
                description=tag.description
            )
            
            if result:
                # Set color if specified
                if tag.color:
                    client.set_tag_color(result, tag.color)
                
                # Update tag with remote info
                tag.original_urn = result
                tag.datahub_id = tag_id_portion
                tag.sync_status = 'SYNCED'
                tag.last_synced = timezone.now()
                tag.save()
                
                messages.success(request, f"Tag '{tag.name}' successfully deployed to DataHub")
            else:
                messages.error(request, "Failed to deploy tag to DataHub")
            
            # Redirect based on source
            redirect_url = request.POST.get('redirect_url', None)
            if redirect_url:
                return redirect(redirect_url)
            else:
                return redirect('tag_detail', tag_id=tag.id)
                
        except Exception as e:
            logger.error(f"Error deploying tag: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('tag_list')

@method_decorator(require_POST, name='dispatch')
class TagPullView(View):
    """View to handle pulling tags from DataHub (POST only)"""
    
    def get(self, request, only_post=False):
        """Redirect to tag list for GET requests"""
        # We no longer want to show a separate page for pulling tags
        messages.info(request, "Use the 'Pull from DataHub' button on the Tags page")
        return redirect('tag_list')
    
    def post(self, request, only_post=False):
        """Pull tags from DataHub"""
        try:
            # Get client from central utility
            client = get_datahub_client()
            
            if not client:
                messages.error(request, "Could not connect to DataHub. Check your connection settings.")
                return redirect('tag_list')
            
            # Test the connection
            if not client.test_connection():
                messages.error(request, "Could not connect to DataHub. Check your connection settings.")
                return redirect('tag_list')
            
            # Fetch all tags from DataHub
            tags = client.list_tags(query="*", count=1000)
            
            # Process and import tags
            imported_count = 0
            updated_count = 0
            error_count = 0
            
            for tag_data in tags:
                try:
                    # Extract the tag ID from the URN
                    tag_urn = tag_data.get('urn')
                    if not tag_urn:
                        logger.warning(f"Skipping tag without URN: {tag_data}")
                        error_count += 1
                        continue
                        
                    name = tag_data.get('name')
                    if not name:
                        logger.warning(f"Skipping tag without name: {tag_data}")
                        error_count += 1
                        continue
                    
                    # Extract properties
                    properties = tag_data.get('properties', {})
                    color_hex = properties.get('colorHex') if properties else None
                    description = tag_data.get('description', '')
                    
                    # Try to find an existing tag with the same URN
                    # Ensure tag_urn is a string for database query
                    existing_tag = Tag.objects.filter(deterministic_urn=str(tag_urn)).first()
                    
                    if existing_tag:
                        # Update the existing tag
                        existing_tag.name = name
                        existing_tag.description = description
                        existing_tag.color = color_hex
                        existing_tag.original_urn = tag_urn
                        existing_tag.sync_status = 'SYNCED'
                        existing_tag.last_synced = timezone.now()
                        existing_tag.save()
                        updated_count += 1
                    else:
                        # Create a new tag
                        # Ensure tag_urn is a string before attempting to split it
                        tag_id = str(tag_urn).split(':')[-1] if tag_urn else None
                        
                        Tag.objects.create(
                            name=name,
                            description=description,
                            color=color_hex,
                            deterministic_urn=tag_urn,
                            original_urn=tag_urn,
                            datahub_id=tag_id,
                            sync_status='REMOTE_ONLY',
                            last_synced=timezone.now()
                        )
                        imported_count += 1
                except Exception as e:
                    logger.error(f"Error processing tag {tag_data.get('name')}: {str(e)}")
                    error_count += 1
            
            # Report results
            if imported_count > 0:
                messages.success(request, f"Successfully imported {imported_count} new tags from DataHub")
            
            if updated_count > 0:
                messages.info(request, f"Updated {updated_count} existing tags")
            
            if error_count > 0:
                messages.warning(request, f"Encountered {error_count} errors during tag import")
            
            if imported_count == 0 and updated_count == 0 and error_count == 0:
                messages.info(request, "No tags found in DataHub")
            
            return redirect('tag_list')
        except Exception as e:
            logger.error(f"Error pulling tags from DataHub: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('tag_list')

@method_decorator(csrf_exempt, name='dispatch')
class TagGitPushView(View):
    """View to add a tag to Git PR"""
    
    def post(self, request, tag_id):
        """Add a tag to Git PR"""
        try:
            tag = get_object_or_404(Tag, id=tag_id)
            
            # Check if Git integration is enabled
            try:
                github_settings = GitSettings.objects.first()
                if not github_settings or not github_settings.enabled:
                    return JsonResponse({'success': False, 'error': 'Git integration is not enabled'})
            except Exception as e:
                logger.error(f"Error checking GitHub settings: {str(e)}")
                return JsonResponse({'success': False, 'error': 'Git integration is not available'})
            
            # Convert tag to JSON format for Git
            tag_data = tag.to_dict()
            
            try:
                # Get default environment or use current
                environment = tag.environment
                
                # Try to stage the file in Git
                from web_ui.views import github_add_file_to_staging
                file_path = f"metadata/tags/{tag.name}.json"
                content = json.dumps(tag_data, indent=2)
                
                result = github_add_file_to_staging(file_path, content, f"Added tag {tag.name}")
                
                if result and result.get('success'):
                    # Update tag status
                    tag.staged_for_git = True
                    tag.sync_status = 'PENDING_PUSH'
                    tag.save()
                    
                    return JsonResponse({'success': True})
                else:
                    error_msg = result.get('error', 'Unknown error')
                    return JsonResponse({'success': False, 'error': error_msg})
                    
            except Exception as e:
                logger.error(f"Error adding tag to Git PR: {str(e)}")
                return JsonResponse({'success': False, 'error': str(e)})
                
        except Exception as e:
            logger.error(f"Error in tag Git push view: {str(e)}")
            return JsonResponse({'success': False, 'error': str(e)}) 