from django.shortcuts import render, redirect, get_object_or_404
from django.views import View
from django.contrib import messages
from django.http import JsonResponse, HttpResponse
from django.views.decorators.http import require_POST
from django.utils.decorators import method_decorator
import json
import logging
import yaml
import os
import sys
from django.urls import reverse

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.datahub_utils import get_datahub_client, test_datahub_connection
from web_ui.models import GitSettings, Environment, GitIntegration  # Import for GitHub integration and environment

logger = logging.getLogger(__name__)

class TestListView(View):
    """View to list metadata tests"""
    
    def get(self, request):
        """Display list of metadata tests"""
        try:
            # Check connection to DataHub
            connected, client = test_datahub_connection()
            
            tests = []
            if connected and client:
                # Fetch tests from DataHub
                try:
                    tests_response = client.list_tests()
                    if tests_response:
                        tests = tests_response
                except Exception as e:
                    logger.error(f"Error fetching tests: {str(e)}")
                    messages.error(request, f"Error fetching tests: {str(e)}")
            
            # Check if git integration is enabled
            has_git_integration = False
            try:
                github_settings = GitSettings.objects.first()
                has_git_integration = github_settings and github_settings.enabled
            except Exception as e:
                logger.warning(f"Error checking git integration: {str(e)}")
            
            return render(request, 'metadata_manager/tests/list.html', {
                'page_title': 'Metadata Tests',
                'tests': tests,
                'has_datahub_connection': connected,
                'has_git_integration': has_git_integration
            })
        except Exception as e:
            logger.error(f"Error in test list view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('metadata_manager:metadata_index')
    
    def post(self, request):
        """Handle test deletion and other actions"""
        action = request.POST.get('action')
        test_urn = request.POST.get('test_urn')
        
        if action == 'delete' and test_urn:
            try:
                # Check connection to DataHub
                connected, client = test_datahub_connection()
                
                if connected and client:
                    # Delete test from DataHub
                    success = client.delete_test(test_urn)
                    if success:
                        messages.success(request, "Test deleted successfully.")
                    else:
                        messages.error(request, "Failed to delete test.")
                else:
                    messages.error(request, "Not connected to DataHub. Please check your connection settings.")
            except Exception as e:
                logger.error(f"Error deleting test: {str(e)}")
                messages.error(request, f"Error deleting test: {str(e)}")
        
        # Redirect back to the list view
        return redirect('metadata_manager:tests_list')

class TestDetailView(View):
    """View to create or edit metadata tests"""
    
    def get(self, request, test_urn=None):
        """Display test details for viewing or editing"""
        try:
            # Check connection to DataHub
            connected, client = test_datahub_connection()
            
            test = None
            is_new = test_urn is None
            is_local_edit = False
            
            if not is_new and connected and client:
                try:
                    # Fetch test from DataHub
                    test = client.get_test(test_urn)
                except Exception as e:
                    logger.error(f"Error fetching test: {str(e)}")
                    messages.error(request, f"Error fetching test: {str(e)}")
            
            # Available test categories
            categories = [
                "ENTITY_LIFECYCLE",
                "METADATA_QUALITY",
                "METADATA_VALIDATION",
                "DATA_QUALITY",
                "DATA_RELIABILITY",
                "GLOSSARY_COMPLIANCE",
                "ACCESS_CONTROL",
                "CUSTOM"
            ]
            
            # Check if git integration is enabled
            has_git_integration = False
            try:
                github_settings = GitSettings.objects.first()
                has_git_integration = github_settings and github_settings.enabled
            except Exception as e:
                logger.warning(f"Error checking git integration: {str(e)}")
            
            return render(request, 'metadata_manager/tests/detail.html', {
                'page_title': 'New Metadata Test' if is_new else f'Edit Test: {test.get("name", "") if test else ""}',
                'test': test,
                'test_urn': test_urn,
                'is_new': is_new,
                'is_local_edit': is_local_edit,
                'categories': categories,
                'has_datahub_connection': connected,
                'has_git_integration': has_git_integration
            })
        except Exception as e:
            logger.error(f"Error in test detail view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('metadata_manager:tests_list')
    
    def post(self, request, test_urn=None):
        """Create or update a test"""
        try:
            # Get form data
            name = request.POST.get('name', '')
            category = request.POST.get('category', '')
            description = request.POST.get('description', '')
            yaml_definition = request.POST.get('yaml_definition', '')
            source = request.POST.get('source', 'server')
            local_index = request.POST.get('local_index', '-1')
            
            # Validate required fields
            if not name:
                messages.error(request, "Name is required.")
                return redirect(request.path)
            
            if not yaml_definition:
                messages.error(request, "YAML definition is required.")
                return redirect(request.path)
            
            # Handle local storage or server storage based on source
            if source == 'local':
                # This is for when the form was specifically submitted to save locally
                # But this is now handled via JavaScript, so we may not need this code path
                messages.success(request, f"Test '{name}' saved locally.")
                return redirect('metadata_manager:tests_list')
            
            # For server storage, we need to connect to DataHub
            connected, client = test_datahub_connection()
            
            if not connected or not client:
                messages.error(request, "Not connected to DataHub. Please check your connection settings.")
                return redirect(request.path)
            
            # Try to parse YAML to ensure it's valid
            try:
                test_definition = yaml.safe_load(yaml_definition)
                if not isinstance(test_definition, dict):
                    messages.error(request, "Invalid YAML: must be a dictionary/object.")
                    return redirect(request.path)
            except Exception as e:
                messages.error(request, f"Invalid YAML: {str(e)}")
                return redirect(request.path)
            
            # Create or update test on DataHub
            if test_urn:  # Update existing test
                try:
                    updated_test = client.update_test(
                        test_urn=test_urn,
                        name=name,
                        description=description,
                        category=category,
                        definition_json=test_definition
                    )
                    
                    if updated_test:
                        messages.success(request, f"Test '{name}' updated successfully.")
                        
                        # If this was a push from local storage, handle removing from local storage
                        if local_index != '-1' and int(local_index) >= 0:
                            # We don't actually need to do anything server-side for this
                            # The client-side JavaScript handles this
                            pass
                            
                        return redirect('metadata_manager:tests_list')
                    else:
                        messages.error(request, f"Failed to update test '{name}'.")
                except Exception as e:
                    logger.error(f"Error updating test: {str(e)}")
                    messages.error(request, f"Error updating test: {str(e)}")
            else:  # Create new test
                try:
                    created_test = client.create_test(
                        name=name,
                        description=description,
                        category=category,
                        definition_json=test_definition
                    )
                    
                    if created_test:
                        messages.success(request, f"Test '{name}' created successfully.")
                        
                        # If this was a push from local storage, handle removing from local storage
                        if local_index != '-1' and int(local_index) >= 0:
                            # We don't actually need to do anything server-side for this
                            # The client-side JavaScript handles this
                            pass
                            
                        return redirect('metadata_manager:tests_list')
                    else:
                        messages.error(request, f"Failed to create test '{name}'.")
                except Exception as e:
                    logger.error(f"Error creating test: {str(e)}")
                    messages.error(request, f"Error creating test: {str(e)}")
            
            return redirect(request.path)
            
        except Exception as e:
            logger.error(f"Error in test form submission: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect(request.path)

class TestDeleteView(View):
    """View to delete a metadata test"""
    
    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def post(self, request, test_urn):
        """Delete a test"""
        try:
            # Check connection to DataHub
            connected, client = test_datahub_connection()
            
            if connected and client:
                # Delete test from DataHub
                success = client.delete_test(test_urn)
                if success:
                    messages.success(request, "Test deleted successfully.")
                else:
                    messages.error(request, "Failed to delete test.")
            else:
                messages.error(request, "Not connected to DataHub. Please check your connection settings.")
        except Exception as e:
            logger.error(f"Error deleting test: {str(e)}")
            messages.error(request, f"Error deleting test: {str(e)}")
        
        # Redirect back to the list view
        return redirect('metadata_manager:tests_list')

class TestGitPushView(View):
    """View to add a test to a GitHub PR"""
    
    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def post(self, request, test_urn):
        """Add test to GitHub PR"""
        try:
            # Check connection to DataHub
            connected, client = test_datahub_connection()
            
            if not connected or not client:
                return JsonResponse({"success": False, "message": "Not connected to DataHub"})
            
            # Check if GitHub integration is enabled
            github_settings = GitSettings.objects.first()
            if not github_settings or not github_settings.enabled:
                return JsonResponse({"success": False, "message": "GitHub integration not enabled"})
            
            # Get test from DataHub
            test = client.get_test(test_urn)
            if not test:
                return JsonResponse({"success": False, "message": "Test not found"})
            
            # Get environment from request (default to None if not provided)
            environment_id = request.POST.get('environment')
            environment = None
            if environment_id:
                try:
                    environment = Environment.objects.get(id=environment_id)
                    logger.info(f"Using environment '{environment.name}' for test")
                except Environment.DoesNotExist:
                    logger.warning(f"Environment with ID {environment_id} not found, using default")
                    environment = Environment.get_default()
            else:
                environment = Environment.get_default()
                logger.info(f"No environment specified, using default: {environment.name}")
            
            # Get Git settings
            settings = GitSettings.get_instance()
            current_branch = settings.current_branch or 'main'
            
            # Prevent pushing directly to main/master branch
            if current_branch.lower() in ['main', 'master']:
                logger.warning(f"Attempted to push directly to {current_branch} branch")
                return JsonResponse({
                    'success': False, 
                    'error': 'Cannot push directly to the main/master branch. Please create and use a feature branch.'
                })
            
            # Extract test details
            test_name = test.get('name', 'Unnamed Test')
            test_definition = test.get('yaml_definition', '')
            
            if not test_definition:
                return JsonResponse({"success": False, "message": "Test definition is empty"})
            
            # Create a simple Test object that GitIntegration can handle
            class MetadataTest:
                def __init__(self, test_urn, name, definition, environment):
                    self.id = test_urn
                    self.name = name
                    self.definition = definition
                    self.environment = environment
                    self.content = definition  # GitIntegration expects a content attribute
                    
                def to_dict(self):
                    # Return a dictionary representation of the test
                    return {
                        'id': self.id,
                        'name': self.name,
                        'definition': self.definition
                    }
                
                def to_yaml(self, path=None):
                    # Return YAML representation of the test
                    import yaml
                    if isinstance(self.definition, dict):
                        return yaml.dump(self.definition, default_flow_style=False)
                    return self.definition
            
            # Create the test object
            metadata_test = MetadataTest(test_urn, test_name, test_definition, environment)
            
            # Create commit message
            commit_message = f"Add/update metadata test: {test_name}"
            
            # Stage the test to the git repo
            logger.info(f"Staging test {test_urn} to Git branch {current_branch}")
            git_integration = GitIntegration()
            
            # Use GitIntegration to stage the changes
            result = git_integration.push_to_git(metadata_test, commit_message)
            
            if result and result.get('success'):
                # Success response
                logger.info(f"Successfully staged test {test_urn} to Git branch {current_branch}")
                return JsonResponse({
                    'success': True,
                    'message': f'Test "{test_name}" staged for commit to branch {current_branch}. You can create a PR from the Git Repository tab.',
                    'redirect_url': reverse('github_index')
                })
            else:
                # Failed to stage changes
                error_message = f'Failed to stage test "{test_name}"'
                if isinstance(result, dict) and 'error' in result:
                    error_message += f": {result['error']}"
                
                logger.error(f"Failed to stage test: {error_message}")
                return JsonResponse({
                    'success': False,
                    'error': error_message
                })
            
        except Exception as e:
            logger.error(f"Error adding test to GitHub PR: {str(e)}")
            return JsonResponse({"success": False, "message": f"Error: {str(e)}"})

class TestExportView(View):
    """View to export a test as YAML or JSON"""
    
    def get(self, request, test_urn):
        """Export test as YAML or JSON"""
        try:
            format = request.GET.get('format', 'yaml')
            
            # Check connection to DataHub
            connected, client = test_datahub_connection()
            
            if not connected or not client:
                messages.error(request, "Not connected to DataHub. Please check your connection settings.")
                return redirect('metadata_manager:tests_list')
            
            # Get test from DataHub
            test = client.get_test(test_urn)
            if not test:
                messages.error(request, "Test not found.")
                return redirect('metadata_manager:tests_list')
            
            # Extract test definition
            test_definition = test.get('yaml_definition', '')
            test_name = test.get('name', 'test')
            
            # Create response based on requested format
            if format == 'json':
                # Convert YAML to JSON if necessary
                try:
                    if isinstance(test_definition, str):
                        definition_dict = yaml.safe_load(test_definition)
                    else:
                        definition_dict = test_definition
                    
                    # Create JSON response
                    response = HttpResponse(
                        json.dumps(definition_dict, indent=2),
                        content_type='application/json'
                    )
                    response['Content-Disposition'] = f'attachment; filename="{test_name}.json"'
                except Exception as e:
                    logger.error(f"Error converting test to JSON: {str(e)}")
                    messages.error(request, f"Error exporting test: {str(e)}")
                    return redirect('metadata_manager:tests_list')
            else:
                # Create YAML response
                if isinstance(test_definition, dict):
                    # Convert dict to YAML string if necessary
                    test_definition = yaml.dump(test_definition, default_flow_style=False)
                
                response = HttpResponse(test_definition, content_type='text/yaml')
                response['Content-Disposition'] = f'attachment; filename="{test_name}.yaml"'
            
            return response
            
        except Exception as e:
            logger.error(f"Error exporting test: {str(e)}")
            messages.error(request, f"Error exporting test: {str(e)}")
            return redirect('metadata_manager:tests_list')

class TestImportView(View):
    """View to import a test from YAML or JSON file"""
    
    def get(self, request):
        """Display import form"""
        try:
            # Check connection to DataHub
            connected, client = test_datahub_connection()
            
            return render(request, 'metadata_manager/tests/import.html', {
                'page_title': 'Import Metadata Test',
                'has_datahub_connection': connected
            })
        except Exception as e:
            logger.error(f"Error in test import view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('metadata_manager:tests_list')
    
    def post(self, request):
        """Import test from file"""
        try:
            # Check if file was uploaded
            if 'test_file' not in request.FILES:
                messages.error(request, "No file selected.")
                return redirect('metadata_manager:test_import')
            
            file = request.FILES['test_file']
            
            # Check file extension
            if file.name.endswith('.json'):
                # Parse JSON file
                try:
                    test_data = json.loads(file.read().decode('utf-8'))
                except Exception as e:
                    messages.error(request, f"Invalid JSON file: {str(e)}")
                    return redirect('metadata_manager:test_import')
            elif file.name.endswith('.yaml') or file.name.endswith('.yml'):
                # Parse YAML file
                try:
                    test_data = yaml.safe_load(file.read().decode('utf-8'))
                except Exception as e:
                    messages.error(request, f"Invalid YAML file: {str(e)}")
                    return redirect('metadata_manager:test_import')
            else:
                messages.error(request, "Unsupported file format. Please upload a JSON or YAML file.")
                return redirect('metadata_manager:test_import')
            
            # Handle import destination
            destination = request.POST.get('destination', 'server')
            
            if destination == 'server':
                # Import to DataHub server
                # Check connection to DataHub
                connected, client = test_datahub_connection()
                
                if not connected or not client:
                    messages.error(request, "Not connected to DataHub. Please check your connection settings.")
                    return redirect('metadata_manager:test_import')
                
                # Extract test data
                name = test_data.get('name', '')
                description = test_data.get('description', '')
                category = test_data.get('category', 'CUSTOM')
                definition = test_data.get('definition', test_data)
                
                # Create test on DataHub
                try:
                    created_test = client.create_test(
                        name=name,
                        description=description,
                        category=category,
                        definition_json=definition
                    )
                    
                    if created_test:
                        messages.success(request, f"Test '{name}' imported successfully.")
                    else:
                        messages.error(request, f"Failed to import test '{name}'.")
                except Exception as e:
                    logger.error(f"Error importing test: {str(e)}")
                    messages.error(request, f"Error importing test: {str(e)}")
            else:
                # Import to local storage
                # This is now handled via JavaScript in the client
                messages.success(request, "Test imported to local storage.")
            
            return redirect('metadata_manager:tests_list')
            
        except Exception as e:
            logger.error(f"Error importing test: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('metadata_manager:test_import') 