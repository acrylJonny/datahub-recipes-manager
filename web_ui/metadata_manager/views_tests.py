from django.shortcuts import render, redirect
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
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from utils.datahub_utils import test_datahub_connection
from web_ui.models import (
    GitSettings,
    Environment,
    GitIntegration,
)  # Import for GitHub integration and environment
from metadata_manager.models import Test

logger = logging.getLogger(__name__)


class TestListView(View):
    """View to list metadata tests"""

    def get(self, request):
        """Display list of metadata tests or return JSON data for AJAX requests"""
        # Check if this is an AJAX request for data
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return self._get_tests_data(request)
        
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

            return render(
                request,
                "metadata_manager/tests/list.html",
                {
                    "page_title": "Metadata Tests",
                    "tests": tests,
                    "has_datahub_connection": connected,
                    "has_git_integration": has_git_integration,
                },
            )
        except Exception as e:
            logger.error(f"Error in test list view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:metadata_index")

    def post(self, request):
        """Handle test deletion and other actions"""
        action = request.POST.get("action")
        test_urn = request.POST.get("test_urn")

        if action == "delete" and test_urn:
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
                    messages.error(
                        request,
                        "Not connected to DataHub. Please check your connection settings.",
                    )
            except Exception as e:
                logger.error(f"Error deleting test: {str(e)}")
                messages.error(request, f"Error deleting test: {str(e)}")

        # Redirect back to the list view
        return redirect("metadata_manager:tests_list")

    def _get_tests_data(self, request):
        """Return JSON data for AJAX requests"""
        try:
            # Get local tests from database
            local_tests = Test.objects.all()
            

            
            # Check connection to DataHub for remote tests
            connected, client = test_datahub_connection()
            remote_tests = []
            remote_tests_dict = {}
            
            if connected and client:
                try:
                    # Use smaller count to avoid hitting corrupted test data
                    tests_response = client.list_tests(query="*", start=0, count=100)
                    if tests_response:
                        remote_tests = tests_response
                        # Create dictionary for fast lookups
                        remote_tests_dict = {test.get('urn'): test for test in remote_tests if test.get('urn')}
                except Exception as e:
                    logger.error(f"Error fetching remote tests: {str(e)}")

            # Get local test URNs for comparison
            local_test_urns = set(test.urn for test in local_tests if test.urn)
            
            # Categorize tests
            synced_tests = []
            local_only_tests = []
            remote_only_tests = []
            
            # Process local tests
            for local_test in local_tests:
                test_urn = local_test.urn
                if not test_urn:
                    continue
                    
                remote_match = None
                if connected and client and test_urn in remote_tests_dict:
                    remote_match = remote_tests_dict[test_urn]

                # Create local test data
                local_test_data = {
                    'id': str(local_test.id),
                    'urn': test_urn,
                    'name': local_test.name,
                    'description': local_test.description or '',
                    'category': local_test.category or '',
                    'type': local_test.category or 'Test',
                    'environment': 'default',
                    'definition_json': local_test.definition_json,
                    'sync_status': local_test.sync_status,
                    # Add empty results for local-only tests
                    'results': {},
                    'has_results': False,
                    'is_failing': False,
                    'passing_count': 0,
                    'failing_count': 0,
                    'last_run': None,
                }
                
                if remote_match:
                    # SYNCED: exists in both local and remote
                    results = remote_match.get('results', {})
                    has_results = bool(results.get('passingCount', 0) > 0 or results.get('failingCount', 0) > 0)
                    is_failing = bool(results.get('failingCount', 0) > 0)
                    
                    # Update local test data with remote information
                    local_test_data.update({
                        'status': 'synced',
                        'definition_json': local_test.definition_json or remote_match.get('definition_json', ''),
                        'results': results,
                        'has_results': has_results,
                        'is_failing': is_failing,
                        'passing_count': results.get('passingCount', 0),
                        'failing_count': results.get('failingCount', 0),
                        'last_run': results.get('lastRunTimestampMillis'),
                    })
                    
                    synced_tests.append({
                        'local': local_test_data,
                        'remote': remote_match,
                        'combined': local_test_data  # Enhanced data for display
                    })
                else:
                    # LOCAL_ONLY: exists only locally
                    local_test_data['status'] = 'local_only'
                    local_only_tests.append(local_test_data)
            
            # Find remote-only tests
            for test_urn, remote_test in remote_tests_dict.items():
                if test_urn not in local_test_urns:
                    # REMOTE_ONLY: exists only on DataHub
                    test_name = remote_test.get('name', '').strip()
                    results = remote_test.get('results', {})
                    has_results = bool(results.get('passingCount', 0) > 0 or results.get('failingCount', 0) > 0)
                    is_failing = bool(results.get('failingCount', 0) > 0)
                    
                    # Generate datahub_id for remote-only test
                    datahub_id = f"{current_connection.name if current_connection else 'default'}_{test_urn}"
                    
                    remote_test_enhanced = {
                        'id': test_urn,  # Use URN as ID for remote-only tests
                        'urn': test_urn,
                        'name': test_name,
                        'description': remote_test.get('description', ''),
                        'category': remote_test.get('category', ''),
                        'type': remote_test.get('category', 'Test'),
                        'environment': 'default',
                        'status': 'remote_only',
                        'definition_json': remote_test.get('definition_json', ''),
                        'results': results,
                        'has_results': has_results,
                        'is_failing': is_failing,
                        'passing_count': results.get('passingCount', 0),
                        'failing_count': results.get('failingCount', 0),
                        'last_run': results.get('lastRunTimestampMillis'),
                        'sync_status': 'REMOTE_ONLY',
                        'datahub_id': datahub_id,
                        'connection_context': 'current',
                        'has_remote_match': True,
                    }
                    remote_only_tests.append(remote_test_enhanced)

            # Combine all tests for final output
            all_tests = []
            
            # Add synced tests (using combined data)
            for synced_test in synced_tests:
                all_tests.append(synced_test['combined'])
            
            # Add local-only tests
            all_tests.extend(local_only_tests)
            
            # Add remote-only tests  
            all_tests.extend(remote_only_tests)



            return JsonResponse({
                'success': True,
                'tests': all_tests,
                'total': len(all_tests)
            })

        except Exception as e:
            logger.error(f"Error getting tests data: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': f"Error getting tests data: {str(e)}",
                'tests': []
            })


def get_remote_tests_data(request):
    """Get comprehensive tests data with proper connection handling for AJAX requests."""
    try:
        logger.info("Loading comprehensive tests data via AJAX")
        
        # Get DataHub client using connection system (this handles connection switching)
        from utils.datahub_utils import get_datahub_client_from_request
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
        
        # Get current connection info for debugging
        from web_ui.views import get_current_connection
        current_connection = get_current_connection(request)
        logger.debug(f"Using connection: {current_connection.name if current_connection else 'None'}")
        
        # Get local tests from database
        local_tests = Test.objects.all()
        logger.debug(f"Found {local_tests.count()} local tests")
        
        # Get remote tests from DataHub
        remote_tests = []
        remote_tests_dict = {}
        
        try:
            # Use smaller count to avoid hitting corrupted test data
            tests_response = client.list_tests(query="*", start=0, count=100)
            if tests_response:
                remote_tests = tests_response
                # Create dictionary for fast lookups
                remote_tests_dict = {test.get('urn'): test for test in remote_tests if test.get('urn')}
                logger.debug(f"Found {len(remote_tests)} remote tests")
        except Exception as e:
            logger.error(f"Error fetching remote tests: {str(e)}")

        # Get local test URNs for comparison
        local_test_urns = set(test.urn for test in local_tests if test.urn)
        
        # Categorize tests
        synced_tests = []
        local_only_tests = []
        remote_only_tests = []
        
        # Process local tests
        for local_test in local_tests:
            test_urn = local_test.urn
            if not test_urn:
                continue
                
            remote_match = None
            if test_urn in remote_tests_dict:
                remote_match = remote_tests_dict[test_urn]

            # Generate datahub_id based on connection and URN
            connection_context = 'current' if current_connection else 'none'
            datahub_id = f"{current_connection.name if current_connection else 'default'}_{test_urn}"
            
            # Create local test data
            local_test_data = {
                'id': str(local_test.id),
                'urn': test_urn,
                'name': local_test.name,
                'description': local_test.description or '',
                'category': local_test.category or '',
                'type': local_test.category or 'Test',
                'environment': 'default',
                'definition_json': local_test.definition_json,
                'sync_status': local_test.sync_status,
                'datahub_id': datahub_id,
                'connection_context': connection_context,
                'has_remote_match': test_urn in remote_tests_dict,
                # Add empty results for local-only tests
                'results': {},
                'has_results': False,
                'is_failing': False,
                'passing_count': 0,
                'failing_count': 0,
                'last_run': None,
            }
            
            if remote_match:
                # SYNCED: exists in both local and remote
                results = remote_match.get('results', {})
                has_results = bool(results.get('passingCount', 0) > 0 or results.get('failingCount', 0) > 0)
                is_failing = bool(results.get('failingCount', 0) > 0)
                
                # Update local test data with remote information
                local_test_data.update({
                    'status': 'synced',
                    'definition_json': local_test.definition_json or remote_match.get('definition_json', ''),
                    'results': results,
                    'has_results': has_results,
                    'is_failing': is_failing,
                    'passing_count': results.get('passingCount', 0),
                    'failing_count': results.get('failingCount', 0),
                    'last_run': results.get('lastRunTimestampMillis'),
                })
                
                synced_tests.append(local_test_data)
            else:
                # LOCAL_ONLY: exists only locally
                local_test_data['status'] = 'local_only'
                local_only_tests.append(local_test_data)
        
        # Find remote-only tests
        for test_urn, remote_test in remote_tests_dict.items():
            if test_urn not in local_test_urns:
                # REMOTE_ONLY: exists only on DataHub
                test_name = remote_test.get('name', '').strip()
                results = remote_test.get('results', {})
                has_results = bool(results.get('passingCount', 0) > 0 or results.get('failingCount', 0) > 0)
                is_failing = bool(results.get('failingCount', 0) > 0)
                
                remote_test_enhanced = {
                    'id': test_urn,  # Use URN as ID for remote-only tests
                    'urn': test_urn,
                    'name': test_name,
                    'description': remote_test.get('description', ''),
                    'category': remote_test.get('category', ''),
                    'type': remote_test.get('category', 'Test'),
                    'environment': 'default',
                    'status': 'remote_only',
                    'definition_json': remote_test.get('definition_json', ''),
                    'results': results,
                    'has_results': has_results,
                    'is_failing': is_failing,
                    'passing_count': results.get('passingCount', 0),
                    'failing_count': results.get('failingCount', 0),
                    'last_run': results.get('lastRunTimestampMillis'),
                    'sync_status': 'REMOTE_ONLY',
                }
                remote_only_tests.append(remote_test_enhanced)

        # Combine all tests for final output
        all_tests = []
        all_tests.extend(synced_tests)
        all_tests.extend(local_only_tests)
        all_tests.extend(remote_only_tests)

        logger.info(f"Returning {len(all_tests)} tests: {len(synced_tests)} synced, {len(local_only_tests)} local-only, {len(remote_only_tests)} remote-only")

        return JsonResponse({
            'success': True,
            'tests': all_tests,
            'total': len(all_tests)
        })

    except Exception as e:
        logger.error(f"Error getting remote tests data: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': f"Error getting remote tests data: {str(e)}",
            'tests': []
        })


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
                    # Fetch test from DataHub using list_tests with smaller count to avoid corrupted data
                    tests = client.list_tests(query="*", start=0, count=100)
                    test = next((t for t in tests if t.get('urn') == test_urn), None)
                    if not test:
                        logger.warning(f"Test not found with URN: {test_urn}")
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
                "CUSTOM",
            ]

            # Check if git integration is enabled
            has_git_integration = False
            try:
                github_settings = GitSettings.objects.first()
                has_git_integration = github_settings and github_settings.enabled
            except Exception as e:
                logger.warning(f"Error checking git integration: {str(e)}")

            return render(
                request,
                "metadata_manager/tests/detail.html",
                {
                    "page_title": "New Metadata Test"
                    if is_new
                    else f"Edit Test: {test.get('name', '') if test else ''}",
                    "test": test,
                    "test_urn": test_urn,
                    "is_new": is_new,
                    "is_local_edit": is_local_edit,
                    "categories": categories,
                    "has_datahub_connection": connected,
                    "has_git_integration": has_git_integration,
                },
            )
        except Exception as e:
            logger.error(f"Error in test detail view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:tests_list")

    def post(self, request, test_urn=None):
        """Create or update a test"""
        try:
            # Check if this is an AJAX request
            is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            
            # Get form data
            name = request.POST.get("name", "")
            category = request.POST.get("category", "")
            description = request.POST.get("description", "")
            yaml_definition = request.POST.get("yaml_definition", "")
            source = request.POST.get("source", "server")
            local_index = request.POST.get("local_index", "-1")

            # Validate required fields
            if not name:
                error_msg = "Name is required."
                if is_ajax:
                    return JsonResponse({'success': False, 'error': error_msg})
                messages.error(request, error_msg)
                return redirect(request.path)

            if not yaml_definition:
                error_msg = "YAML definition is required."
                if is_ajax:
                    return JsonResponse({'success': False, 'error': error_msg})
                messages.error(request, error_msg)
                return redirect(request.path)

            # Handle local storage or server storage based on source
            if source == "local":
                # This is for when the form was specifically submitted to save locally
                # But this is now handled via JavaScript, so we may not need this code path
                success_msg = f"Test '{name}' saved locally."
                if is_ajax:
                    return JsonResponse({'success': True, 'message': success_msg})
                messages.success(request, success_msg)
                return redirect("metadata_manager:tests_list")

            # For server storage, we need to connect to DataHub
            connected, client = test_datahub_connection()

            if not connected or not client:
                error_msg = "Not connected to DataHub. Please check your connection settings."
                if is_ajax:
                    return JsonResponse({'success': False, 'error': error_msg})
                messages.error(request, error_msg)
                return redirect(request.path)

            # Try to parse YAML to ensure it's valid
            try:
                test_definition = yaml.safe_load(yaml_definition)
                if not isinstance(test_definition, dict):
                    error_msg = "Invalid YAML: must be a dictionary/object."
                    if is_ajax:
                        return JsonResponse({'success': False, 'error': error_msg})
                    messages.error(request, error_msg)
                    return redirect(request.path)
            except Exception as e:
                error_msg = f"Invalid YAML: {str(e)}"
                if is_ajax:
                    return JsonResponse({'success': False, 'error': error_msg})
                messages.error(request, error_msg)
                return redirect(request.path)

            # Create or update test on DataHub
            if test_urn:  # Update existing test
                try:
                    updated_test = client.update_test(
                        test_urn=test_urn,
                        name=name,
                        description=description,
                        category=category,
                        definition_json=test_definition,
                    )

                    if updated_test:
                        success_msg = f"Test '{name}' updated successfully."
                        
                        # If this was a push from local storage, handle removing from local storage
                        if local_index != "-1" and int(local_index) >= 0:
                            # We don't actually need to do anything server-side for this
                            # The client-side JavaScript handles this
                            pass

                        if is_ajax:
                            return JsonResponse({'success': True, 'message': success_msg})
                        messages.success(request, success_msg)
                        return redirect("metadata_manager:tests_list")
                    else:
                        error_msg = f"Failed to update test '{name}'."
                        if is_ajax:
                            return JsonResponse({'success': False, 'error': error_msg})
                        messages.error(request, error_msg)
                except Exception as e:
                    logger.error(f"Error updating test: {str(e)}")
                    error_msg = f"Error updating test: {str(e)}"
                    if is_ajax:
                        return JsonResponse({'success': False, 'error': error_msg})
                    messages.error(request, error_msg)
            else:  # Create new test
                try:
                    created_test = client.create_test(
                        name=name,
                        description=description,
                        category=category,
                        definition_json=test_definition,
                    )

                    if created_test:
                        success_msg = f"Test '{name}' created successfully."

                        # If this was a push from local storage, handle removing from local storage
                        if local_index != "-1" and int(local_index) >= 0:
                            # We don't actually need to do anything server-side for this
                            # The client-side JavaScript handles this
                            pass

                        if is_ajax:
                            return JsonResponse({'success': True, 'message': success_msg})
                        messages.success(request, success_msg)
                        return redirect("metadata_manager:tests_list")
                    else:
                        error_msg = f"Failed to create test '{name}'."
                        if is_ajax:
                            return JsonResponse({'success': False, 'error': error_msg})
                        messages.error(request, error_msg)
                except Exception as e:
                    logger.error(f"Error creating test: {str(e)}")
                    error_msg = f"Error creating test: {str(e)}"
                    if is_ajax:
                        return JsonResponse({'success': False, 'error': error_msg})
                    messages.error(request, error_msg)

            if is_ajax:
                return JsonResponse({'success': False, 'error': 'Unknown error occurred'})
            return redirect(request.path)

        except Exception as e:
            logger.error(f"Error in test form submission: {str(e)}")
            error_msg = f"An error occurred: {str(e)}"
            if is_ajax:
                return JsonResponse({'success': False, 'error': error_msg})
            messages.error(request, error_msg)
            return redirect(request.path)


class TestDeleteView(View):
    """View to delete a metadata test"""

    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)

    def post(self, request, test_id):
        """Delete a test (AJAX endpoint)"""
        try:
            # For AJAX requests, return JSON response
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or \
               request.content_type == 'application/json':
                
                # Parse request body to get delete type
                delete_type = 'both'  # default
                if request.content_type == 'application/json':
                    import json
                    try:
                        body = json.loads(request.body)
                        delete_type = body.get('delete_type', 'both')
                    except:
                        pass
                
                success_message = 'Test deleted successfully.'
                
                # Handle different delete types
                if delete_type == 'local_only':
                    # For local_only, delete from local database only
                    try:
                        # Try to find local test by ID first, then by URN
                        local_test = None
                        try:
                            local_test = Test.objects.get(id=test_id)
                        except Test.DoesNotExist:
                            try:
                                local_test = Test.objects.get(urn=test_id)
                            except Test.DoesNotExist:
                                pass
                        
                        if local_test:
                            test_name = local_test.name
                            local_test.delete()
                            success_message = f'Test "{test_name}" removed from local database (still exists on DataHub).'
                        else:
                            success_message = 'Test removed from local view (still exists on DataHub).'
                        
                        return JsonResponse({
                            'success': True,
                            'message': success_message
                        })
                    except Exception as e:
                        logger.error(f"Error deleting local test: {str(e)}")
                        return JsonResponse({
                            'success': False,
                            'error': f'Error deleting local test: {str(e)}'
                        })
                
                elif delete_type == 'remote_only':
                    # Delete only from DataHub
                    connected, client = test_datahub_connection()
                    if not connected or not client:
                        return JsonResponse({
                            'success': False,
                            'error': 'Not connected to DataHub. Please check your connection settings.'
                        })
                    
                    success = client.delete_test(test_id)
                    if success:
                        success_message = 'Test deleted from DataHub successfully.'
                        return JsonResponse({
                            'success': True,
                            'message': success_message
                        })
                    else:
                        return JsonResponse({
                            'success': False,
                            'error': 'Failed to delete test from DataHub.'
                        })
                
                else:  # delete_type == 'both' or default
                    # Delete from DataHub (original behavior)
                    connected, client = test_datahub_connection()
                    if not connected or not client:
                        return JsonResponse({
                            'success': False,
                            'error': 'Not connected to DataHub. Please check your connection settings.'
                        })
                    
                    success = client.delete_test(test_id)
                    if success:
                        return JsonResponse({
                            'success': True,
                            'message': success_message
                        })
                    else:
                        return JsonResponse({
                            'success': False,
                            'error': 'Failed to delete test.'
                        })
            
            # For regular form submissions, handle as before
            test_urn = test_id
            connected, client = test_datahub_connection()

            if connected and client:
                # Delete test from DataHub
                success = client.delete_test(test_urn)
                if success:
                    messages.success(request, "Test deleted successfully.")
                else:
                    messages.error(request, "Failed to delete test.")
            else:
                messages.error(
                    request,
                    "Not connected to DataHub. Please check your connection settings.",
                )
                
        except Exception as e:
            logger.error(f"Error deleting test: {str(e)}")
            
            # Return JSON for AJAX, messages for regular requests
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or \
               request.content_type == 'application/json':
                return JsonResponse({
                    'success': False,
                    'error': f'Error deleting test: {str(e)}'
                })
            else:
                messages.error(request, f"Error deleting test: {str(e)}")

        # Redirect back to the list view for regular requests
        return redirect("metadata_manager:tests_list")


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
                return JsonResponse(
                    {"success": False, "message": "Not connected to DataHub"}
                )

            # Check if GitHub integration is enabled
            github_settings = GitSettings.objects.first()
            if not github_settings or not github_settings.enabled:
                return JsonResponse(
                    {"success": False, "message": "GitHub integration not enabled"}
                )

            # Get test from DataHub using list_tests with smaller count to avoid corrupted data
            tests = client.list_tests(query="*", start=0, count=100)
            test = next((t for t in tests if t.get('urn') == test_urn), None)
            if not test:
                return JsonResponse({"success": False, "message": "Test not found"})

            # Get environment from request (default to None if not provided)
            environment_id = request.POST.get("environment")
            environment = None
            if environment_id:
                try:
                    environment = Environment.objects.get(id=environment_id)
                    logger.info(f"Using environment '{environment.name}' for test")
                except Environment.DoesNotExist:
                    logger.warning(
                        f"Environment with ID {environment_id} not found, using default"
                    )
                    environment = Environment.get_default()
            else:
                environment = Environment.get_default()
                logger.info(
                    f"No environment specified, using default: {environment.name}"
                )

            # Get Git settings
            settings = GitSettings.get_instance()
            current_branch = settings.current_branch or "main"

            # Prevent pushing directly to main/master branch
            if current_branch.lower() in ["main", "master"]:
                logger.warning(f"Attempted to push directly to {current_branch} branch")
                return JsonResponse(
                    {
                        "success": False,
                        "error": "Cannot push directly to the main/master branch. Please create and use a feature branch.",
                    }
                )

            # Extract test details
            test_name = test.get("name", "Unnamed Test")
            test_definition = test.get("yaml_definition", "")

            if not test_definition:
                return JsonResponse(
                    {"success": False, "message": "Test definition is empty"}
                )

            # Create a simple Test object that GitIntegration can handle
            class MetadataTest:
                def __init__(self, test_urn, name, definition, environment):
                    self.id = test_urn
                    self.name = name
                    self.definition = definition
                    self.environment = environment
                    self.content = (
                        definition  # GitIntegration expects a content attribute
                    )

                def to_dict(self):
                    # Return a dictionary representation of the test
                    return {
                        "id": self.id,
                        "name": self.name,
                        "definition": self.definition,
                    }

                def to_yaml(self, path=None):
                    # Return YAML representation of the test
                    import yaml

                    if isinstance(self.definition, dict):
                        return yaml.dump(self.definition, default_flow_style=False)
                    return self.definition

            # Create the test object
            metadata_test = MetadataTest(
                test_urn, test_name, test_definition, environment
            )

            # Create commit message
            commit_message = f"Add/update metadata test: {test_name}"

            # Stage the test to the git repo
            logger.info(f"Staging test {test_urn} to Git branch {current_branch}")
            git_integration = GitIntegration()

            # Use GitIntegration to stage the changes
            result = git_integration.push_to_git(metadata_test, commit_message)

            if result and result.get("success"):
                # Success response
                logger.info(
                    f"Successfully staged test {test_urn} to Git branch {current_branch}"
                )
                return JsonResponse(
                    {
                        "success": True,
                        "message": f'Test "{test_name}" staged for commit to branch {current_branch}. You can create a PR from the Git Repository tab.',
                        "redirect_url": reverse("github_index"),
                    }
                )
            else:
                # Failed to stage changes
                error_message = f'Failed to stage test "{test_name}"'
                if isinstance(result, dict) and "error" in result:
                    error_message += f": {result['error']}"

                logger.error(f"Failed to stage test: {error_message}")
                return JsonResponse({"success": False, "error": error_message})

        except Exception as e:
            logger.error(f"Error adding test to GitHub PR: {str(e)}")
            return JsonResponse({"success": False, "message": f"Error: {str(e)}"})


class TestExportView(View):
    """View to export a test as YAML or JSON"""

    def get(self, request, test_urn):
        """Export test as YAML or JSON"""
        try:
            format = request.GET.get("format", "yaml")

            # Check connection to DataHub
            connected, client = test_datahub_connection()

            if not connected or not client:
                messages.error(
                    request,
                    "Not connected to DataHub. Please check your connection settings.",
                )
                return redirect("metadata_manager:tests_list")

            # Get test from DataHub using list_tests with smaller count to avoid corrupted data
            tests = client.list_tests(query="*", start=0, count=100)
            test = next((t for t in tests if t.get('urn') == test_urn), None)
            if not test:
                messages.error(request, "Test not found.")
                return redirect("metadata_manager:tests_list")

            # Extract test definition
            test_definition = test.get("yaml_definition", "")
            test_name = test.get("name", "test")

            # Create response based on requested format
            if format == "json":
                # Convert YAML to JSON if necessary
                try:
                    if isinstance(test_definition, str):
                        definition_dict = yaml.safe_load(test_definition)
                    else:
                        definition_dict = test_definition

                    # Create JSON response
                    response = HttpResponse(
                        json.dumps(definition_dict, indent=2),
                        content_type="application/json",
                    )
                    response["Content-Disposition"] = (
                        f'attachment; filename="{test_name}.json"'
                    )
                except Exception as e:
                    logger.error(f"Error converting test to JSON: {str(e)}")
                    messages.error(request, f"Error exporting test: {str(e)}")
                    return redirect("metadata_manager:tests_list")
            else:
                # Create YAML response
                if isinstance(test_definition, dict):
                    # Convert dict to YAML string if necessary
                    test_definition = yaml.dump(
                        test_definition, default_flow_style=False
                    )

                response = HttpResponse(test_definition, content_type="text/yaml")
                response["Content-Disposition"] = (
                    f'attachment; filename="{test_name}.yaml"'
                )

            return response

        except Exception as e:
            logger.error(f"Error exporting test: {str(e)}")
            messages.error(request, f"Error exporting test: {str(e)}")
            return redirect("metadata_manager:tests_list")


class TestImportView(View):
    """View to import a test from YAML or JSON file"""

    def get(self, request):
        """Display import form"""
        try:
            # Check connection to DataHub
            connected, client = test_datahub_connection()

            return render(
                request,
                "metadata_manager/tests/import.html",
                {
                    "page_title": "Import Metadata Test",
                    "has_datahub_connection": connected,
                },
            )
        except Exception as e:
            logger.error(f"Error in test import view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:tests_list")

    def post(self, request):
        """Import test from file"""
        try:
            # Check if file was uploaded
            if "test_file" not in request.FILES:
                messages.error(request, "No file selected.")
                return redirect("metadata_manager:test_import")

            file = request.FILES["test_file"]

            # Check file extension
            if file.name.endswith(".json"):
                # Parse JSON file
                try:
                    test_data = json.loads(file.read().decode("utf-8"))
                except Exception as e:
                    messages.error(request, f"Invalid JSON file: {str(e)}")
                    return redirect("metadata_manager:test_import")
            elif file.name.endswith(".yaml") or file.name.endswith(".yml"):
                # Parse YAML file
                try:
                    test_data = yaml.safe_load(file.read().decode("utf-8"))
                except Exception as e:
                    messages.error(request, f"Invalid YAML file: {str(e)}")
                    return redirect("metadata_manager:test_import")
            else:
                messages.error(
                    request,
                    "Unsupported file format. Please upload a JSON or YAML file.",
                )
                return redirect("metadata_manager:test_import")

            # Handle import destination
            destination = request.POST.get("destination", "server")

            if destination == "server":
                # Import to DataHub server
                # Check connection to DataHub
                connected, client = test_datahub_connection()

                if not connected or not client:
                    messages.error(
                        request,
                        "Not connected to DataHub. Please check your connection settings.",
                    )
                    return redirect("metadata_manager:test_import")

                # Extract test data
                name = test_data.get("name", "")
                description = test_data.get("description", "")
                category = test_data.get("category", "CUSTOM")
                definition = test_data.get("definition", test_data)

                # Create test on DataHub
                try:
                    created_test = client.create_test(
                        name=name,
                        description=description,
                        category=category,
                        definition_json=definition,
                    )

                    if created_test:
                        messages.success(
                            request, f"Test '{name}' imported successfully."
                        )
                    else:
                        messages.error(request, f"Failed to import test '{name}'.")
                except Exception as e:
                    logger.error(f"Error importing test: {str(e)}")
                    messages.error(request, f"Error importing test: {str(e)}")
            else:
                # Import to local storage
                # This is now handled via JavaScript in the client
                messages.success(request, "Test imported to local storage.")

            return redirect("metadata_manager:tests_list")

        except Exception as e:
            logger.error(f"Error importing test: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:test_import")


class TestStageChangesView(View):
    """View to add a test to staged changes"""
    
    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def post(self, request, test_id):
        """Add a test to staged changes"""
        try:
            # Get the test from database
            test = Test.objects.get(id=test_id)
            
            # Get environment and mutation name from request
            data = json.loads(request.body)
            environment = data.get('environment', 'dev')
            mutation_name = data.get('mutation_name')
            
            # Create test JSON object for staging
            test_data = {
                'urn': test.urn,
                'name': test.name,
                'description': test.description or '',
                'category': test.category or '',
                'definition': test.definition_json or {},
                'yaml_definition': test.yaml_definition or '',
                'environment': environment
            }
            
            # Determine the metadata_tests directory path
            metadata_tests_dir = os.path.join('metadata_tests', environment)
            os.makedirs(metadata_tests_dir, exist_ok=True)
            
            # Create individual JSON file for this test (following the pattern for tests)
            # Extract test name from URN for filename
            test_name_for_file = test.urn.split(':')[-1] if test.urn else test.name
            test_filename = f"test_{test_name_for_file}.json"
            test_file_path = os.path.join(metadata_tests_dir, test_filename)
            
            # Write test JSON file
            with open(test_file_path, 'w') as f:
                json.dump(test_data, f, indent=2)
            
            logger.info(f"Test staged to {test_file_path}")
            
            return JsonResponse({
                'success': True,
                'message': f'Test successfully added to staged changes',
                'files_created': [test_file_path]
            })
            
        except Test.DoesNotExist:
            logger.error(f"Test with ID {test_id} not found")
            return JsonResponse({
                'success': False,
                'error': 'Test not found'
            }, status=404)
        except Exception as e:
            logger.error(f"Error staging test changes: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': f'Error staging test changes: {str(e)}'
            }, status=500)


class TestRemoteStageChangesView(View):
    """View to add a remote test to staged changes"""
    
    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def post(self, request):
        """Add a remote test to staged changes"""
        try:
            # Get the test URN from request
            data = json.loads(request.body)
            test_urn = data.get('test_urn')
            environment = data.get('environment', 'dev')
            mutation_name = data.get('mutation_name')
            
            if not test_urn:
                return JsonResponse({
                    'success': False,
                    'error': 'Test URN is required'
                }, status=400)
            
            # Get remote test data from DataHub
            from utils.datahub_utils import get_datahub_client_from_request
            client = get_datahub_client_from_request(request)
            
            if not client:
                return JsonResponse({
                    'success': False,
                    'error': 'No active DataHub connection'
                }, status=400)
            
            # Find the test in remote tests
            remote_tests = client.list_tests(query="*", start=0, count=100)
            remote_test = None
            for test in remote_tests:
                if test.get('urn') == test_urn:
                    remote_test = test
                    break
            
            if not remote_test:
                return JsonResponse({
                    'success': False,
                    'error': 'Remote test not found'
                }, status=404)
            
            # Create test JSON object for staging
            test_data = {
                'urn': test_urn,
                'name': remote_test.get('name', ''),
                'description': remote_test.get('description', ''),
                'category': remote_test.get('category', ''),
                'definition': remote_test.get('definition_json', {}),
                'environment': environment
            }
            
            # Determine the metadata_tests directory path
            metadata_tests_dir = os.path.join('metadata_tests', environment)
            os.makedirs(metadata_tests_dir, exist_ok=True)
            
            # Create individual JSON file for this test
            # Extract test name from URN for filename
            test_name_for_file = test_urn.split(':')[-1] if test_urn else 'unknown_test'
            test_filename = f"test_{test_name_for_file}.json"
            test_file_path = os.path.join(metadata_tests_dir, test_filename)
            
            # Write test JSON file
            with open(test_file_path, 'w') as f:
                json.dump(test_data, f, indent=2)
            
            logger.info(f"Remote test staged to {test_file_path}")
            
            return JsonResponse({
                'success': True,
                'message': f'Remote test successfully added to staged changes',
                'files_created': [test_file_path]
            })
            
        except Exception as e:
            logger.error(f"Error staging remote test changes: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': f'Error staging remote test changes: {str(e)}'
            }, status=500)
