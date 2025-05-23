from django.shortcuts import render, redirect, get_object_or_404
from django.views import View
from django.contrib import messages
from django.http import JsonResponse, HttpResponse
from django.views.decorators.http import require_POST
from django.utils.decorators import method_decorator
from django.utils import timezone
import json
import logging
import os
import sys

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Import the deterministic URN utilities
from utils.urn_utils import generate_deterministic_urn, get_full_urn_from_name
from utils.datahub_utils import get_datahub_client, test_datahub_connection
from .models import Assertion, AssertionResult, Domain

logger = logging.getLogger(__name__)

class AssertionListView(View):
    """View to list and create assertions"""
    
    def get(self, request):
        """Display list of assertions"""
        try:
            logger.info("Starting AssertionListView.get")
            
            # Get all assertions and domains for domain assertions
            assertions = Assertion.objects.all().order_by('name')
            domains = Domain.objects.all().order_by('name')
            
            logger.debug(f"Found {assertions.count()} assertions and {domains.count()} domains")
            
            # Get DataHub connection info
            logger.debug("Testing DataHub connection from AssertionListView")
            connected, client = test_datahub_connection()
            logger.debug(f"DataHub connection test result: {connected}")
            
            # Initialize context
            context = {
                'assertions': assertions,
                'domains': domains,
                'has_datahub_connection': connected,
                'page_title': 'Metadata Assertions'
            }
            
            logger.info("Rendering assertion list template")
            return render(request, 'metadata_manager/assertions/list.html', context)
        except Exception as e:
            logger.error(f"Error in assertion list view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return render(request, 'metadata_manager/assertions/list.html', {
                'error': str(e),
                'page_title': 'Metadata Assertions'
            })
    
    def post(self, request):
        """Create a new assertion"""
        try:
            name = request.POST.get('name')
            description = request.POST.get('description', '')
            assertion_type = request.POST.get('type')
            
            if not name:
                messages.error(request, "Assertion name is required")
                return redirect('assertion_list')
            
            if not assertion_type:
                messages.error(request, "Assertion type is required")
                return redirect('assertion_list')
            
            # Initialize config based on assertion type
            config = {}
            
            if assertion_type == 'domain_exists':
                domain_id = request.POST.get('domain_id')
                if not domain_id:
                    messages.error(request, "Please select a domain for the domain existence assertion")
                    return redirect('assertion_list')
                
                try:
                    domain = Domain.objects.get(id=domain_id)
                    config = {
                        'domain_id': str(domain.id),
                        'domain_name': domain.name,
                        'domain_urn': domain.deterministic_urn
                    }
                except Domain.DoesNotExist:
                    messages.error(request, f"Domain not found")
                    return redirect('assertion_list')
            
            elif assertion_type == 'sql':
                sql_query = request.POST.get('sql_query')
                expected_result = request.POST.get('expected_result')
                
                if not sql_query:
                    messages.error(request, "SQL query is required")
                    return redirect('assertion_list')
                
                config = {
                    'query': sql_query,
                    'expected_result': expected_result
                }
            
            elif assertion_type == 'tag_exists':
                tag_name = request.POST.get('tag_name')
                
                if not tag_name:
                    messages.error(request, "Tag name is required")
                    return redirect('assertion_list')
                
                config = {
                    'tag_name': tag_name
                }
            
            elif assertion_type == 'glossary_term_exists':
                term_name = request.POST.get('term_name')
                
                if not term_name:
                    messages.error(request, "Glossary term name is required")
                    return redirect('assertion_list')
                
                config = {
                    'term_name': term_name
                }
            
            # Create the assertion
            assertion = Assertion.objects.create(
                name=name,
                description=description,
                type=assertion_type,
                config=config
            )
            
            messages.success(request, f"Assertion '{name}' created successfully")
            return redirect('assertion_list')
        except Exception as e:
            logger.error(f"Error creating assertion: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('assertion_list')


class AssertionDetailView(View):
    """View to display, edit, and delete assertions"""
    
    def get(self, request, assertion_id):
        """Display assertion details"""
        try:
            assertion = get_object_or_404(Assertion, id=assertion_id)
            results = AssertionResult.objects.filter(assertion=assertion).order_by('-run_at')[:10]
            
            return render(request, 'metadata_manager/assertions/detail.html', {
                'assertion': assertion,
                'results': results,
                'page_title': f'Assertion: {assertion.name}'
            })
        except Exception as e:
            logger.error(f"Error in assertion detail view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('assertion_list')


class AssertionRunView(View):
    """View to run an assertion"""
    
    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def post(self, request, assertion_id):
        """Run an assertion"""
        try:
            assertion = get_object_or_404(Assertion, id=assertion_id)
            
            # Get DataHub connection
            connected, client = test_datahub_connection()
            
            if not connected or not client:
                messages.error(request, "Cannot connect to DataHub. Please check your connection settings.")
                return redirect('assertion_detail', assertion_id=assertion_id)
            
            # Run assertion based on type
            result = None
            
            if assertion.type == 'domain_exists':
                result = self.run_domain_exists_assertion(assertion, client)
            elif assertion.type == 'sql':
                result = self.run_sql_assertion(assertion, client)
            elif assertion.type == 'tag_exists':
                result = self.run_tag_exists_assertion(assertion, client)
            elif assertion.type == 'glossary_term_exists':
                result = self.run_glossary_term_exists_assertion(assertion, client)
            
            if result:
                # Save the result
                AssertionResult.objects.create(
                    assertion=assertion,
                    status="SUCCESS" if result['success'] else "FAILED",
                    details=result['details'],
                    run_at=timezone.now()
                )
                
                # Update assertion
                assertion.last_run = timezone.now()
                assertion.last_status = "SUCCESS" if result['success'] else "FAILED"
                assertion.save()
                
                if result['success']:
                    messages.success(request, f"Assertion '{assertion.name}' passed")
                else:
                    messages.warning(request, f"Assertion '{assertion.name}' failed: {result['details'].get('message', 'Unknown error')}")
            else:
                messages.error(request, f"Failed to run assertion")
            
            return redirect('assertion_detail', assertion_id=assertion_id)
        except Exception as e:
            logger.error(f"Error running assertion: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('assertion_list')
    
    def run_domain_exists_assertion(self, assertion, client):
        """Run a domain_exists assertion"""
        try:
            config = assertion.config
            domain_urn = config.get('domain_urn')
            
            if not domain_urn:
                return {
                    'success': False,
                    'details': {
                        'message': "Domain URN not found in assertion configuration"
                    }
                }
            
            # Check if domain exists in DataHub
            domain = client.get_domain(domain_urn)
            
            exists = domain is not None
            
            return {
                'success': exists,
                'details': {
                    'message': f"Domain {'exists' if exists else 'does not exist'} in DataHub",
                    'domain_urn': domain_urn,
                    'domain_name': config.get('domain_name'),
                    'exists': exists,
                    'datahub_response': domain if domain else "Not found"
                }
            }
        except Exception as e:
            logger.error(f"Error running domain_exists assertion: {str(e)}")
            return {
                'success': False,
                'details': {
                    'message': f"Error running assertion: {str(e)}"
                }
            }
    
    def run_sql_assertion(self, assertion, client):
        """Run a SQL assertion"""
        # This is a placeholder, actual implementation would depend on DataHub's SQL execution capabilities
        return {
            'success': False,
            'details': {
                'message': "SQL assertions are not yet implemented"
            }
        }
    
    def run_tag_exists_assertion(self, assertion, client):
        """Run a tag_exists assertion"""
        try:
            config = assertion.config
            tag_name = config.get('tag_name')
            
            if not tag_name:
                return {
                    'success': False,
                    'details': {
                        'message': "Tag name not found in assertion configuration"
                    }
                }
            
            # Generate the tag URN
            tag_urn = get_full_urn_from_name("tag", tag_name)
            
            # Check if tag exists in DataHub
            tag = client.get_tag(tag_urn)
            
            exists = tag is not None
            
            return {
                'success': exists,
                'details': {
                    'message': f"Tag {'exists' if exists else 'does not exist'} in DataHub",
                    'tag_name': tag_name,
                    'tag_urn': tag_urn,
                    'exists': exists,
                    'datahub_response': tag if tag else "Not found"
                }
            }
        except Exception as e:
            logger.error(f"Error running tag_exists assertion: {str(e)}")
            return {
                'success': False,
                'details': {
                    'message': f"Error running assertion: {str(e)}"
                }
            }
    
    def run_glossary_term_exists_assertion(self, assertion, client):
        """Run a glossary_term_exists assertion"""
        try:
            config = assertion.config
            term_name = config.get('term_name')
            
            if not term_name:
                return {
                    'success': False,
                    'details': {
                        'message': "Glossary term name not found in assertion configuration"
                    }
                }
            
            # Generate the term URN (this is a simplified approach, actual URN may depend on parent path)
            term_urn = get_full_urn_from_name("glossaryTerm", term_name)
            
            # Check if term exists in DataHub
            term = client.get_glossary_term(term_urn)
            
            exists = term is not None
            
            return {
                'success': exists,
                'details': {
                    'message': f"Glossary term {'exists' if exists else 'does not exist'} in DataHub",
                    'term_name': term_name,
                    'term_urn': term_urn,
                    'exists': exists,
                    'datahub_response': term if term else "Not found"
                }
            }
        except Exception as e:
            logger.error(f"Error running glossary_term_exists assertion: {str(e)}")
            return {
                'success': False,
                'details': {
                    'message': f"Error running assertion: {str(e)}"
                }
            }


class AssertionDeleteView(View):
    """View to delete an assertion"""
    
    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def post(self, request, assertion_id):
        """Delete an assertion"""
        try:
            assertion = get_object_or_404(Assertion, id=assertion_id)
            
            # Delete the assertion
            assertion_name = assertion.name
            assertion.delete()
            
            messages.success(request, f"Assertion '{assertion_name}' deleted successfully")
            return redirect('assertion_list')
        except Exception as e:
            logger.error(f"Error deleting assertion: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('assertion_list') 