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
from utils.urn_utils import generate_deterministic_urn, get_full_urn_from_name, get_parent_path
from utils.datahub_utils import get_datahub_client, test_datahub_connection
from .models import GlossaryNode, GlossaryTerm

logger = logging.getLogger(__name__)

class GlossaryListView(View):
    """View to list glossary nodes and terms"""
    
    def get(self, request):
        """Display list of glossary nodes and terms"""
        try:
            logger.info("Starting GlossaryListView.get")
            
            # Get all nodes and terms
            nodes = GlossaryNode.objects.all().order_by('name')
            root_nodes = nodes.filter(parent=None).order_by('name')
            terms = GlossaryTerm.objects.all().order_by('name')
            
            logger.debug(f"Found {nodes.count()} total nodes, {root_nodes.count()} root nodes, and {terms.count()} terms")
            
            # Separate root terms (terms without a parent node)
            root_terms = terms.filter(parent_node=None).order_by('name')
            
            # Group terms by sync status
            synced_terms = terms.filter(sync_status='SYNCED').order_by('name')
            local_only_terms = terms.filter(sync_status='LOCAL_ONLY').order_by('name')
            remote_only_terms = terms.filter(sync_status='REMOTE_ONLY').order_by('name')
            modified_terms = terms.filter(sync_status='MODIFIED').order_by('name')
            
            # Group nodes by sync status
            synced_nodes = nodes.filter(sync_status='SYNCED').order_by('name')
            local_only_nodes = nodes.filter(sync_status='LOCAL_ONLY').order_by('name')
            remote_only_nodes = nodes.filter(sync_status='REMOTE_ONLY').order_by('name')
            modified_nodes = nodes.filter(sync_status='MODIFIED').order_by('name')
            
            # Group root terms by sync status
            synced_root_terms = root_terms.filter(sync_status='SYNCED').order_by('name')
            local_only_root_terms = root_terms.filter(sync_status='LOCAL_ONLY').order_by('name')
            remote_only_root_terms = root_terms.filter(sync_status='REMOTE_ONLY').order_by('name')
            modified_root_terms = root_terms.filter(sync_status='MODIFIED').order_by('name')
            
            logger.debug(f"Found {root_terms.count()} root-level terms")
            
            # Get DataHub connection info using the shared utility
            logger.debug("Testing DataHub connection from GlossaryListView")
            connected, client = test_datahub_connection()
            logger.debug(f"DataHub connection test result: {connected}")
            
            # Fetch remote-only nodes and terms if connected
            remote_only_nodes = []
            remote_only_terms = []
            datahub_url = None
            
            if connected and client:
                logger.debug("Connected to DataHub, fetching remote nodes and terms")
                try:
                    # Get all remote nodes and terms from DataHub
                    remote_nodes = client.list_glossary_nodes(count=1000)
                    remote_terms = client.list_glossary_terms(count=1000)
                    
                    logger.debug(f"Fetched {len(remote_nodes) if remote_nodes else 0} remote nodes and {len(remote_terms) if remote_terms else 0} remote terms")
                    
                    # Extract node URNs that exist locally
                    local_node_urns = set(nodes.values_list('deterministic_urn', flat=True))
                    
                    # Find nodes that exist remotely but not locally
                    remote_only_nodes = [node for node in remote_nodes if node.get('urn') not in local_node_urns]
                    
                    # Extract term URNs that exist locally
                    local_term_urns = set(terms.values_list('deterministic_urn', flat=True))
                    
                    # Find terms that exist remotely but not locally
                    remote_only_terms = [term for term in remote_terms if term.get('urn') not in local_term_urns]
                    
                    logger.debug(f"Identified {len(remote_only_nodes)} remote-only nodes and {len(remote_only_terms)} remote-only terms")
                    
                    # Get DataHub URL for direct links
                    datahub_url = client.server_url
                    if datahub_url.endswith('/api/gms'):
                        datahub_url = datahub_url[:-8]  # Remove /api/gms to get base URL
                except Exception as e:
                    logger.error(f"Error fetching remote glossary data: {str(e)}")
                    # Try alternative query if the standard one fails
                    try:
                        logger.info("Trying alternative GraphQL query for terms")
                        # Use a more generic query that's compatible with more DataHub versions
                        query = """
                        query glossaryNodes($input: ListGlossaryNodesInput!) {
                            glossaryNodes(input: $input) {
                                nodes {
                                    urn
                                    properties {
                                        name
                                    }
                                    parentNode {
                                        urn
                                    }
                                }
                            }
                        }
                        """
                        variables = {"input": {"start": 0, "count": 1000}}
                        result = client.execute_graphql(query, variables)
                        remote_nodes = []
                        if result and 'data' in result and 'glossaryNodes' in result['data']:
                            for node in result['data']['glossaryNodes']['nodes']:
                                remote_nodes.append(node)
                        
                        # Use a more generic query for terms as well
                        query = """
                        query {
                            glossaryTerms(start: 0, count: 1000) {
                                terms {
                                    urn
                                    name
                                    hierarchicalName
                                    properties {
                                        name
                                    }
                                    parentNodes {
                                        nodes {
                                            urn
                                        }
                                    }
                                }
                            }
                        }
                        """
                        variables = {}
                        result = client.execute_graphql(query, variables)
                        remote_terms = []
                        if result and 'data' in result and 'glossaryTerms' in result['data']:
                            for term in result['data']['glossaryTerms']['terms']:
                                remote_terms.append(term)
                        
                        # Extract node URNs that exist locally
                        local_node_urns = set(nodes.values_list('deterministic_urn', flat=True))
                        
                        # Find nodes that exist remotely but not locally
                        remote_only_nodes = [node for node in remote_nodes if node.get('urn') not in local_node_urns]
                        
                        # Extract term URNs that exist locally
                        local_term_urns = set(terms.values_list('deterministic_urn', flat=True))
                        
                        # Find terms that exist remotely but not locally
                        remote_only_terms = [term for term in remote_terms if term.get('urn') not in local_term_urns]
                        
                        logger.debug(f"Identified {len(remote_only_nodes)} remote-only nodes and {len(remote_only_terms)} remote-only terms using alternative query")
                        
                        # Get DataHub URL for direct links
                        datahub_url = client.server_url
                        if datahub_url.endswith('/api/gms'):
                            datahub_url = datahub_url[:-8]  # Remove /api/gms to get base URL
                    except Exception as alt_e:
                        logger.error(f"Alternative query also failed: {str(alt_e)}")
                        remote_only_nodes = []
                        remote_only_terms = []
            
            # Initialize context
            context = {
                'nodes': nodes,
                'root_nodes': root_nodes,
                'terms': terms,
                'root_terms': root_terms,
                'synced_terms': synced_terms,
                'local_only_terms': local_only_terms,
                'remote_only_terms': remote_only_terms,
                'modified_terms': modified_terms,
                'synced_nodes': synced_nodes,
                'local_only_nodes': local_only_nodes,
                'remote_only_nodes': remote_only_nodes,
                'modified_nodes': modified_nodes,
                'synced_root_terms': synced_root_terms,
                'local_only_root_terms': local_only_root_terms,
                'remote_only_root_terms': remote_only_root_terms,
                'modified_root_terms': modified_root_terms,
                'datahub_url': datahub_url,
                'has_datahub_connection': connected,
                'page_title': 'DataHub Glossary',
                'has_git_integration': False
            }
            
            # Check if git integration is enabled
            try:
                from web_ui.web_ui.models import GitSettings
                github_settings = GitSettings.objects.first()
                context['has_git_integration'] = github_settings and github_settings.enabled
                logger.debug(f"Git integration enabled: {context['has_git_integration']}")
            except Exception as e:
                logger.warning(f"Error checking git integration: {str(e)}")
                pass
            
            logger.info("Rendering glossary list template")
            return render(request, 'metadata_manager/glossary/list.html', context)
        except Exception as e:
            logger.error(f"Error in glossary list view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return render(request, 'metadata_manager/glossary/list.html', {
                'error': str(e),
                'page_title': 'DataHub Glossary'
            })
    
    def post(self, request):
        """Handle glossary node creation and batch actions"""
        try:
            # Check for batch actions
            action = request.POST.get('action')
            
            # Handle different actions
            if action == 'push_selected':
                return self._handle_push_selected(request)
            elif action == 'push_all':
                return self._handle_push_all(request)
            elif action == 'add_to_git':
                return self._handle_add_to_git(request)
            else:
                # Default behavior: create a new glossary node
                name = request.POST.get('name')
                description = request.POST.get('description', '')
                parent_id = request.POST.get('parent_id')
                
                if not name:
                    messages.error(request, "Glossary node name is required")
                    return redirect('glossary_list')
                
                # Handle parent node if specified
                parent = None
                if parent_id:
                    try:
                        parent = GlossaryNode.objects.get(id=parent_id)
                    except GlossaryNode.DoesNotExist:
                        messages.error(request, f"Parent node not found")
                        return redirect('glossary_list')
                
                # Generate deterministic URN
                # For nodes with a parent, include the parent path in the URN
                if parent:
                    parent_path = get_parent_path(parent)
                    deterministic_urn = get_full_urn_from_name("glossaryNode", name, parent_path=parent_path)
                else:
                    deterministic_urn = get_full_urn_from_name("glossaryNode", name)
                
                # Check if node with this URN already exists
                if GlossaryNode.objects.filter(deterministic_urn=deterministic_urn).exists():
                    messages.error(request, f"Glossary node with name '{name}' already exists")
                    return redirect('glossary_list')
                
                # Create the node
                node = GlossaryNode.objects.create(
                    name=name,
                    description=description,
                    parent=parent,
                    deterministic_urn=deterministic_urn
                )
                
                messages.success(request, f"Glossary node '{name}' created successfully")
                return redirect('glossary_list')
        except Exception as e:
            logger.error(f"Error in glossary list view post: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('glossary_list')
            
    def _handle_push_selected(self, request):
        """Push selected glossary nodes and terms to DataHub"""
        logger.info("Pushing selected glossary items to DataHub")
        
        # Get the node and term IDs from the request
        node_ids = request.POST.getlist('node_ids')
        term_ids = request.POST.getlist('term_ids')
        
        logger.debug(f"Selected node IDs: {node_ids}")
        logger.debug(f"Selected term IDs: {term_ids}")
        
        # Check if any items were selected
        if not node_ids and not term_ids:
            messages.warning(request, "No glossary items were selected to push")
            return redirect('glossary_list')
        
        # Get DataHub connection info
        connected, client = test_datahub_connection()
        
        if not connected or not client:
            messages.error(request, "Cannot connect to DataHub. Please check your connection settings.")
            return redirect('glossary_list')
        
        # Track results
        success_count = 0
        error_count = 0
        
        # Push selected nodes
        if node_ids:
            for node_id in node_ids:
                try:
                    node = GlossaryNode.objects.get(id=node_id)
                    
                    # Check if parent needs to be pushed first
                    if node.parent and node.parent.sync_status in ['LOCAL_ONLY', 'MODIFIED']:
                        logger.warning(f"Node '{node.name}' has a parent '{node.parent.name}' that is not synced with DataHub")
                        
                        # Try to push parent first
                        try:
                            parent_urn = client.create_glossary_node(
                                node_id=str(node.parent.id),
                                name=node.parent.name,
                                description=node.parent.description or "",
                                parent_urn=None if not node.parent.parent else node.parent.parent.deterministic_urn
                            )
                            
                            if parent_urn:
                                # Update parent node status
                                node.parent.original_urn = parent_urn
                                node.parent.sync_status = 'SYNCED'
                                node.parent.last_synced = timezone.now()
                                node.parent.save()
                                logger.info(f"Parent node '{node.parent.name}' pushed successfully")
                        except Exception as parent_error:
                            logger.error(f"Error pushing parent node '{node.parent.name}': {str(parent_error)}")
                    
                    # Push the node
                    node_urn = client.create_glossary_node(
                        node_id=str(node.id),
                        name=node.name,
                        description=node.description or "",
                        parent_urn=None if not node.parent else node.parent.deterministic_urn
                    )
                    
                    if node_urn:
                        # Update node status
                        node.original_urn = node_urn
                        node.sync_status = 'SYNCED'
                        node.last_synced = timezone.now()
                        node.save()
                        
                        success_count += 1
                        logger.info(f"Node '{node.name}' pushed successfully")
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error pushing node '{node_id}': {str(e)}")
        
        # Push selected terms
        if term_ids:
            for term_id in term_ids:
                try:
                    term = GlossaryTerm.objects.get(id=term_id)
                    
                    # Check if parent node needs to be pushed first
                    if term.parent_node and term.parent_node.sync_status in ['LOCAL_ONLY', 'MODIFIED']:
                        logger.warning(f"Term '{term.name}' has a parent node '{term.parent_node.name}' that is not synced with DataHub")
                        
                        # Try to push parent node first
                        try:
                            parent_urn = client.create_glossary_node(
                                node_id=str(term.parent_node.id),
                                name=term.parent_node.name,
                                description=term.parent_node.description or "",
                                parent_urn=None if not term.parent_node.parent else term.parent_node.parent.deterministic_urn
                            )
                            
                            if parent_urn:
                                # Update parent node status
                                term.parent_node.original_urn = parent_urn
                                term.parent_node.sync_status = 'SYNCED'
                                term.parent_node.last_synced = timezone.now()
                                term.parent_node.save()
                                logger.info(f"Parent node '{term.parent_node.name}' pushed successfully")
                        except Exception as parent_error:
                            logger.error(f"Error pushing parent node '{term.parent_node.name}': {str(parent_error)}")
                    
                    # Push the term
                    term_urn = client.create_glossary_term(
                        term_id=str(term.id),
                        name=term.name,
                        description=term.description or "",
                        parent_node_urn=None if not term.parent_node else term.parent_node.deterministic_urn,
                        term_source=term.term_source
                    )
                    
                    if term_urn:
                        # Update term status
                        term.original_urn = term_urn
                        term.sync_status = 'SYNCED'
                        term.last_synced = timezone.now()
                        term.save()
                        
                        success_count += 1
                        logger.info(f"Term '{term.name}' pushed successfully")
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error pushing term '{term_id}': {str(e)}")
        
        # Show results
        if success_count > 0:
            messages.success(request, f"Successfully pushed {success_count} glossary items to DataHub")
        if error_count > 0:
            messages.warning(request, f"Failed to push {error_count} glossary items. Check logs for details.")
        
        return redirect('glossary_list')
    
    def _handle_push_all(self, request):
        """Push all glossary nodes and terms to DataHub"""
        logger.info("Pushing all glossary items to DataHub")
        
        # Get DataHub connection info
        connected, client = test_datahub_connection()
        
        if not connected or not client:
            messages.error(request, "Cannot connect to DataHub. Please check your connection settings.")
            return redirect('glossary_list')
        
        # Get all nodes and terms that need to be pushed
        nodes_to_push = GlossaryNode.objects.filter(sync_status__in=['LOCAL_ONLY', 'MODIFIED'])
        terms_to_push = GlossaryTerm.objects.filter(sync_status__in=['LOCAL_ONLY', 'MODIFIED'])
        
        # Track results
        success_count = 0
        error_count = 0
        
        # First push all nodes (starting with root nodes)
        root_nodes = nodes_to_push.filter(parent=None)
        processed_nodes = set()
        
        def push_node_tree(node, depth=0):
            nonlocal success_count, error_count
            
            # Skip if already processed
            if node.id in processed_nodes:
                return
            
            try:
                # Push the node
                node_urn = client.create_glossary_node(
                    node_id=str(node.id),
                    name=node.name,
                    description=node.description or "",
                    parent_urn=None if not node.parent else node.parent.deterministic_urn
                )
                
                if node_urn:
                    # Update node status
                    node.original_urn = node_urn
                    node.sync_status = 'SYNCED'
                    node.last_synced = timezone.now()
                    node.save()
                    processed_nodes.add(node.id)
                    
                    success_count += 1
                    logger.info(f"Node '{node.name}' pushed successfully")
                    
                    # Now push child nodes
                    for child_node in node.children.filter(sync_status__in=['LOCAL_ONLY', 'MODIFIED']):
                        push_node_tree(child_node, depth + 1)
            except Exception as e:
                error_count += 1
                logger.error(f"Error pushing node '{node.name}': {str(e)}")
        
        # Start with root nodes
        for root_node in root_nodes:
            push_node_tree(root_node)
        
        # Now push any remaining nodes
        remaining_nodes = nodes_to_push.exclude(id__in=processed_nodes)
        for node in remaining_nodes:
            push_node_tree(node)
        
        # Now push all terms
        for term in terms_to_push:
            try:
                # Push the term
                term_urn = client.create_glossary_term(
                    term_id=str(term.id),
                    name=term.name,
                    description=term.description or "",
                    parent_node_urn=None if not term.parent_node else term.parent_node.deterministic_urn,
                    term_source=term.term_source
                )
                
                if term_urn:
                    # Update term status
                    term.original_urn = term_urn
                    term.sync_status = 'SYNCED'
                    term.last_synced = timezone.now()
                    term.save()
                    
                    success_count += 1
                    logger.info(f"Term '{term.name}' pushed successfully")
            except Exception as e:
                error_count += 1
                logger.error(f"Error pushing term '{term.name}': {str(e)}")
        
        # Show results
        if success_count > 0:
            messages.success(request, f"Successfully pushed {success_count} glossary items to DataHub")
        if error_count > 0:
            messages.warning(request, f"Failed to push {error_count} glossary items. Check logs for details.")
        if success_count == 0 and error_count == 0:
            messages.info(request, "No glossary items needed to be pushed.")
        
        return redirect('glossary_list')
    
    def _handle_add_to_git(self, request):
        """Add selected glossary nodes and terms to a GitHub PR"""
        logger.info("Adding selected glossary items to Git PR")
        
        # Get the node and term IDs from the request
        node_ids = request.POST.getlist('node_ids')
        term_ids = request.POST.getlist('term_ids')
        
        logger.debug(f"Selected node IDs for Git: {node_ids}")
        logger.debug(f"Selected term IDs for Git: {term_ids}")
        
        # Check if any items were selected
        if not node_ids and not term_ids:
            messages.warning(request, "No glossary items were selected for Git PR")
            return redirect('glossary_list')
        
        # Check if git integration is enabled
        try:
            from web_ui.web_ui.models import GitSettings
            from web_ui.views import add_entity_to_git_push
            
            github_settings = GitSettings.objects.first()
            if not github_settings or not github_settings.enabled:
                messages.error(request, "GitHub integration is not enabled. Please check your settings.")
                return redirect('glossary_list')
                
            # Track results
            success_count = 0
            error_count = 0
            
            # Add nodes to Git PR
            if node_ids:
                for node_id in node_ids:
                    try:
                        node = GlossaryNode.objects.get(id=node_id)
                        result = add_entity_to_git_push(request, "glossary_node", node.id)
                        
                        if result.get('success'):
                            node.staged_for_git = True
                            node.save()
                            success_count += 1
                            logger.info(f"Node '{node.name}' added to Git PR successfully")
                        else:
                            error_count += 1
                            logger.error(f"Error adding node '{node.name}' to Git PR: {result.get('error')}")
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Error adding node to Git PR: {str(e)}")
            
            # Add terms to Git PR
            if term_ids:
                for term_id in term_ids:
                    try:
                        term = GlossaryTerm.objects.get(id=term_id)
                        result = add_entity_to_git_push(request, "glossary_term", term.id)
                        
                        if result.get('success'):
                            term.staged_for_git = True
                            term.save()
                            success_count += 1
                            logger.info(f"Term '{term.name}' added to Git PR successfully")
                        else:
                            error_count += 1
                            logger.error(f"Error adding term '{term.name}' to Git PR: {result.get('error')}")
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Error adding term to Git PR: {str(e)}")
            
            # Show results
            if success_count > 0:
                messages.success(request, f"Successfully added {success_count} glossary items to Git PR")
            if error_count > 0:
                messages.warning(request, f"Failed to add {error_count} glossary items to Git PR. Check logs for details.")
            
        except ImportError:
            messages.error(request, "GitHub integration module is not available")
            logger.error("Error importing Git integration modules")
        except Exception as e:
            messages.error(request, f"Error adding items to Git PR: {str(e)}")
            logger.error(f"Error in add_to_git: {str(e)}")
        
        return redirect('glossary_list')


class GlossaryPullView(View):
    """View to pull glossary nodes and terms from DataHub"""
    
    def get(self, request):
        """Display pull confirmation page"""
        try:
            # Get query parameters
            node_urn = request.GET.get('node_urn')
            term_urn = request.GET.get('term_urn')
            confirm = request.GET.get('confirm') == 'true'
            
            context = {
                'page_title': 'Pull Glossary from DataHub',
                'confirm': confirm,
                'node_urn': node_urn,
                'term_urn': term_urn
            }
            
            # Get DataHub connection info
            connected, client = test_datahub_connection()
            context['has_datahub_connection'] = connected
            
            return render(request, 'metadata_manager/glossary/pull.html', context)
        except Exception as e:
            logger.error(f"Error in glossary pull view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('glossary_list')
    
    def post(self, request):
        """Pull glossary nodes and terms from DataHub"""
        try:
            # Check if we're just confirming the pull
            if request.POST.get('confirm') == 'true' and not request.POST.get('execute'):
                return self.get(request)
            
            # Get the client
            connected, client = test_datahub_connection()
            
            if not connected or not client:
                messages.error(request, "Cannot connect to DataHub. Please check your connection settings.")
                return redirect('glossary_list')
            
            results = []
            
            # Check if we're pulling a specific node or term
            node_urn = request.POST.get('node_urn')
            term_urn = request.POST.get('term_urn')
            pull_all = request.POST.get('pull_all') == 'true'
            
            if pull_all:
                # Pull all glossary nodes and terms
                logger.info("Pulling all glossary nodes and terms from DataHub")
                
                # First pull nodes
                try:
                    remote_nodes = client.list_glossary_nodes(count=1000)
                    if remote_nodes and isinstance(remote_nodes, list):
                        logger.info(f"Found {len(remote_nodes)} glossary nodes in DataHub")
                        
                        # Create a map of parent URNs to node objects for resolving parent-child relationships
                        parent_map = {}
                        
                        # Process nodes in multiple passes to handle parent-child relationships
                        for pass_num in range(2):  # Do two passes to resolve parent relationships
                            for remote_node in remote_nodes:
                                try:
                                    node_urn = remote_node.get('urn')
                                    if not node_urn:
                                        continue
                                        
                                    # Convert to string
                                    node_urn = str(node_urn)
                                    
                                    # Skip already imported nodes on second pass
                                    if pass_num == 1 and node_urn in parent_map:
                                        continue
                                        
                                    node_name = remote_node.get('name', 'Unknown Node')
                                    node_description = remote_node.get('description', '')
                                    parent_urn = remote_node.get('parentNode')
                                    
                                    # Find existing node or create new one
                                    existing_node = GlossaryNode.objects.filter(deterministic_urn=node_urn).first()
                                    
                                    if existing_node:
                                        # Update existing node
                                        existing_node.name = node_name
                                        existing_node.description = node_description
                                        existing_node.original_urn = node_urn
                                        existing_node.sync_status = 'SYNCED'
                                        existing_node.last_synced = timezone.now()
                                        
                                        # Update parent if available and resolved
                                        if parent_urn and parent_urn in parent_map:
                                            existing_node.parent = parent_map[parent_urn]
                                            
                                        existing_node.save()
                                        
                                        # Remember node for parent resolution
                                        parent_map[node_urn] = existing_node
                                        
                                        results.append(f"Updated glossary node: {node_name}")
                                    else:
                                        # Create new node
                                        parent_node = None
                                        if parent_urn and parent_urn in parent_map:
                                            parent_node = parent_map[parent_urn]
                                            
                                        # Only create on first pass if no parent, or on second pass
                                        if pass_num == 1 or not parent_urn or parent_node:
                                            new_node = GlossaryNode.objects.create(
                                                name=node_name,
                                                description=node_description,
                                                deterministic_urn=node_urn,
                                                original_urn=node_urn,
                                                parent=parent_node,
                                                sync_status='SYNCED',
                                                last_synced=timezone.now()
                                            )
                                            
                                            # Remember node for parent resolution
                                            parent_map[node_urn] = new_node
                                            
                                            results.append(f"Created glossary node: {node_name}")
                                except Exception as node_error:
                                    logger.error(f"Error importing node {remote_node.get('name', 'unknown')}: {str(node_error)}")
                                    results.append(f"Error importing node {remote_node.get('name', 'unknown')}: {str(node_error)}")
                        
                        # Now pull and associate terms
                        try:
                            remote_terms = client.list_glossary_terms(count=1000)
                            if remote_terms and isinstance(remote_terms, list):
                                logger.info(f"Found {len(remote_terms)} glossary terms in DataHub")
                                
                                for remote_term in remote_terms:
                                    try:
                                        term_urn = remote_term.get('urn')
                                        if not term_urn:
                                            continue
                                            
                                        # Convert to string
                                        term_urn = str(term_urn)
                                        
                                        term_name = remote_term.get('name', 'Unknown Term')
                                        term_description = remote_term.get('description', '')
                                        parent_node_urn = remote_term.get('parentNode')
                                        term_source = remote_term.get('source')
                                        
                                        # Find existing term or create new one
                                        existing_term = GlossaryTerm.objects.filter(deterministic_urn=term_urn).first()
                                        
                                        # Resolve parent node if available
                                        parent_node = None
                                        if parent_node_urn and parent_node_urn in parent_map:
                                            parent_node = parent_map[parent_node_urn]
                                        
                                        if existing_term:
                                            # Update existing term
                                            existing_term.name = term_name
                                            existing_term.description = term_description
                                            existing_term.original_urn = term_urn
                                            existing_term.term_source = term_source
                                            existing_term.parent_node = parent_node
                                            existing_term.sync_status = 'SYNCED'
                                            existing_term.last_synced = timezone.now()
                                            existing_term.save()
                                            
                                            results.append(f"Updated glossary term: {term_name}")
                                        else:
                                            # Create new term
                                            new_term = GlossaryTerm.objects.create(
                                                name=term_name,
                                                description=term_description,
                                                deterministic_urn=term_urn,
                                                original_urn=term_urn,
                                                term_source=term_source,
                                                parent_node=parent_node,
                                                sync_status='SYNCED',
                                                last_synced=timezone.now()
                                            )
                                            
                                            results.append(f"Created glossary term: {term_name}")
                                    except Exception as term_error:
                                        logger.error(f"Error importing term {remote_term.get('name', 'unknown')}: {str(term_error)}")
                                        results.append(f"Error importing term {remote_term.get('name', 'unknown')}: {str(term_error)}")
                        except Exception as terms_error:
                            logger.error(f"Error pulling glossary terms: {str(terms_error)}")
                            results.append(f"Error pulling glossary terms: {str(terms_error)}")
                    
                except Exception as nodes_error:
                    logger.error(f"Error pulling glossary nodes: {str(nodes_error)}")
                    results.append(f"Error pulling glossary nodes: {str(nodes_error)}")
                
                if results:
                    messages.success(request, f"Successfully pulled glossary from DataHub. {len(results)} items processed.")
                else:
                    messages.warning(request, "No glossary items were found or processed.")
                    
                return render(request, 'metadata_manager/glossary/pull.html', {
                    'page_title': 'Pull Glossary from DataHub',
                    'results': results,
                    'has_datahub_connection': True
                })
            
            elif node_urn:
                # Pull specific node
                try:
                    node_data = client.get_glossary_node(node_urn)
                    if node_data:
                        node_name = node_data.get('name', 'Unknown Node')
                        
                        # Check if we already have this node
                        existing_node = GlossaryNode.objects.filter(deterministic_urn=node_urn).first()
                        
                        if existing_node:
                            # Update existing node
                            existing_node.name = node_data.get('name', existing_node.name)
                            existing_node.description = node_data.get('description', existing_node.description)
                            existing_node.original_urn = node_urn
                            existing_node.sync_status = 'SYNCED'
                            existing_node.last_synced = timezone.now()
                            existing_node.save()
                            
                            results.append({
                                'name': node_name,
                                'type': 'node',
                                'success': True,
                                'message': 'Updated existing node'
                            })
                        else:
                            # Create new node
                            # Handle parent if needed
                            parent_urn = node_data.get('parentNode', {}).get('urn')
                            parent = None
                            if parent_urn:
                                parent = GlossaryNode.objects.filter(deterministic_urn=parent_urn).first()
                            
                            GlossaryNode.objects.create(
                                name=node_data.get('name'),
                                description=node_data.get('description', ''),
                                deterministic_urn=node_urn,
                                original_urn=node_urn,
                                parent=parent,
                                sync_status='SYNCED',
                                last_synced=timezone.now()
                            )
                            
                            results.append({
                                'name': node_name,
                                'type': 'node',
                                'success': True,
                                'message': 'Created new node'
                            })
                    else:
                        results.append({
                            'name': node_urn,
                            'type': 'node',
                            'success': False,
                            'message': 'Node not found in DataHub'
                        })
                except Exception as e:
                    logger.error(f"Error pulling glossary node {node_urn}: {str(e)}")
                    results.append({
                        'name': node_urn,
                        'type': 'node',
                        'success': False,
                        'message': f'Error: {str(e)}'
                    })
            elif term_urn:
                # Pull specific term
                try:
                    term_data = client.get_glossary_term(term_urn)
                    if term_data:
                        term_name = term_data.get('name', 'Unknown Term')
                        
                        # Check if we already have this term
                        existing_term = GlossaryTerm.objects.filter(deterministic_urn=term_urn).first()
                        
                        if existing_term:
                            # Update existing term
                            existing_term.name = term_data.get('name', existing_term.name)
                            existing_term.description = term_data.get('description', existing_term.description)
                            existing_term.original_urn = term_urn
                            existing_term.sync_status = 'SYNCED'
                            existing_term.last_synced = timezone.now()
                            
                            # Handle parent node
                            parent_node_urn = None
                            if term_data.get('parentNodes') and term_data['parentNodes'].get('nodes') and len(term_data['parentNodes']['nodes']) > 0:
                                parent_node_urn = term_data['parentNodes']['nodes'][0].get('urn')
                            elif term_data.get('parent_node_urn'):  # For backward compatibility with client method returns
                                parent_node_urn = term_data.get('parent_node_urn')
                                
                            if parent_node_urn:
                                parent_node = GlossaryNode.objects.filter(deterministic_urn=parent_node_urn).first()
                                if parent_node:
                                    existing_term.parent_node = parent_node
                            
                            existing_term.save()
                            
                            results.append({
                                'name': term_name,
                                'type': 'term',
                                'success': True,
                                'message': 'Updated existing term'
                            })
                        else:
                            # Create new term
                            # Need to ensure parent node exists
                            parent_node_urn = term_data.get('parentNode', {}).get('urn')
                            parent_node = None
                            
                            if parent_node_urn:
                                parent_node = GlossaryNode.objects.filter(deterministic_urn=parent_node_urn).first()
                                if not parent_node:
                                    # Try to pull parent node first
                                    try:
                                        parent_node_data = client.get_glossary_node(parent_node_urn)
                                        if parent_node_data:
                                            parent_node = GlossaryNode.objects.create(
                                                name=parent_node_data.get('name'),
                                                description=parent_node_data.get('description', ''),
                                                deterministic_urn=parent_node_urn,
                                                original_urn=parent_node_urn,
                                                sync_status='SYNCED',
                                                last_synced=timezone.now()
                                            )
                                            
                                            results.append({
                                                'name': parent_node_data.get('name'),
                                                'type': 'node',
                                                'success': True,
                                                'message': 'Created parent node'
                                            })
                                    except Exception as e:
                                        logger.error(f"Error pulling parent node {parent_node_urn}: {str(e)}")
                                        results.append({
                                            'name': parent_node_urn,
                                            'type': 'node',
                                            'success': False,
                                            'message': f'Error creating parent node: {str(e)}'
                                        })
                            
                            if parent_node_urn and not parent_node:
                                results.append({
                                    'name': term_name,
                                    'type': 'term',
                                    'success': False,
                                    'message': 'Parent node not found and could not be created'
                                })
                            else:
                                GlossaryTerm.objects.create(
                                    name=term_data.get('name'),
                                    description=term_data.get('description', ''),
                                    deterministic_urn=term_urn,
                                    original_urn=term_urn,
                                    parent_node=parent_node,
                                    term_source=term_data.get('termSource'),
                                    sync_status='SYNCED',
                                    last_synced=timezone.now()
                                )
                                
                                results.append({
                                    'name': term_name,
                                    'type': 'term',
                                    'success': True,
                                    'message': 'Created new term'
                                })
                    else:
                        results.append({
                            'name': term_urn,
                            'type': 'term',
                            'success': False,
                            'message': 'Term not found in DataHub'
                        })
                except Exception as e:
                    logger.error(f"Error pulling glossary term {term_urn}: {str(e)}")
                    results.append({
                        'name': term_urn,
                        'type': 'term',
                        'success': False,
                        'message': f'Error: {str(e)}'
                    })
            else:
                # Pull all nodes and terms
                try:
                    # First, get all nodes
                    remote_nodes = client.list_glossary_nodes(count=1000)
                    
                    # Create a mapping of URNs to parent URNs for later dependency resolution
                    node_parents = {}
                    for node in remote_nodes:
                        node_urn = node.get('urn')
                        parent_urn = node.get('parentNode', {}).get('urn')
                        node_parents[node_urn] = parent_urn
                    
                    # Process nodes in order of dependency (parents first)
                    processed_nodes = set()
                    while node_parents and len(processed_nodes) < len(node_parents):
                        for node_urn, parent_urn in list(node_parents.items()):
                            # Process nodes with no parents or with parents already processed
                            if not parent_urn or parent_urn in processed_nodes:
                                node_data = next((n for n in remote_nodes if n.get('urn') == node_urn), None)
                                if node_data:
                                    existing_node = GlossaryNode.objects.filter(deterministic_urn=node_urn).first()
                                    
                                    parent = None
                                    if parent_urn:
                                        parent = GlossaryNode.objects.filter(deterministic_urn=parent_urn).first()
                                    
                                    if existing_node:
                                        # Update existing node
                                        existing_node.name = node_data.get('name', existing_node.name)
                                        existing_node.description = node_data.get('description', existing_node.description)
                                        existing_node.original_urn = node_urn
                                        existing_node.parent = parent
                                        existing_node.sync_status = 'SYNCED'
                                        existing_node.last_synced = timezone.now()
                                        existing_node.save()
                                        
                                        results.append({
                                            'name': node_data.get('name'),
                                            'type': 'node',
                                            'success': True,
                                            'message': 'Updated existing node'
                                        })
                                    else:
                                        # Create new node
                                        GlossaryNode.objects.create(
                                            name=node_data.get('name'),
                                            description=node_data.get('description', ''),
                                            deterministic_urn=node_urn,
                                            original_urn=node_urn,
                                            parent=parent,
                                            sync_status='SYNCED',
                                            last_synced=timezone.now()
                                        )
                                        
                                        results.append({
                                            'name': node_data.get('name'),
                                            'type': 'node',
                                            'success': True,
                                            'message': 'Created new node'
                                        })
                                
                                processed_nodes.add(node_urn)
                                del node_parents[node_urn]
                    
                    # Now process terms
                    remote_terms = client.list_glossary_terms(count=1000)
                    for term_data in remote_terms:
                        term_urn = term_data.get('urn')
                        if not term_urn:
                            continue
                        
                        existing_term = GlossaryTerm.objects.filter(deterministic_urn=term_urn).first()
                        
                        # Handle parent node
                        parent_node_urn = None
                        if term_data.get('parentNodes') and term_data['parentNodes'].get('nodes') and len(term_data['parentNodes']['nodes']) > 0:
                            parent_node_urn = term_data['parentNodes']['nodes'][0].get('urn')
                        elif term_data.get('parent_node_urn'):  # For backward compatibility with client method returns
                            parent_node_urn = term_data.get('parent_node_urn')
                            
                        parent_node = None
                        if parent_node_urn:
                            parent_node = GlossaryNode.objects.filter(deterministic_urn=parent_node_urn).first()
                        
                        if existing_term:
                            # Update existing term
                            existing_term.name = term_data.get('name', existing_term.name)
                            existing_term.description = term_data.get('description', existing_term.description)
                            existing_term.original_urn = term_urn
                            existing_term.parent_node = parent_node
                            existing_term.term_source = term_data.get('termSource')
                            existing_term.sync_status = 'SYNCED'
                            existing_term.last_synced = timezone.now()
                            existing_term.save()
                            
                            results.append({
                                'name': term_data.get('name'),
                                'type': 'term',
                                'success': True,
                                'message': 'Updated existing term'
                            })
                        else:
                            # Create new term
                            GlossaryTerm.objects.create(
                                name=term_data.get('name'),
                                description=term_data.get('description', ''),
                                deterministic_urn=term_urn,
                                original_urn=term_urn,
                                parent_node=parent_node,
                                term_source=term_data.get('termSource'),
                                sync_status='SYNCED',
                                last_synced=timezone.now()
                            )
                            
                            results.append({
                                'name': term_data.get('name'),
                                'type': 'term',
                                'success': True,
                                'message': 'Created new term'
                            })
                    
                    messages.success(request, f"Successfully pulled {len(remote_nodes)} nodes and {len(remote_terms)} terms from DataHub")
                except Exception as e:
                    logger.error(f"Error pulling all glossary data: {str(e)}")
                    results.append({
                        'name': 'All Glossary Data',
                        'type': 'node',
                        'success': False,
                        'message': f'Error: {str(e)}'
                    })
                    messages.error(request, f"Error pulling glossary data: {str(e)}")
            
            # Return to the pull page with results
            return render(request, 'metadata_manager/glossary/pull.html', {
                'has_datahub_connection': True,
                'page_title': 'Pull Glossary from DataHub',
                'results': results
            })
        except Exception as e:
            logger.error(f"Error in glossary pull view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('glossary_list')


class GlossaryImportExportView(View):
    """View to import/export glossary nodes and terms"""
    
    def get(self, request):
        """Display import/export page"""
        # Get available environments
        try:
            from web_ui.metadata_manager.models import Environment
            environments = Environment.objects.all().order_by('-is_default', 'name')
        except Exception as e:
            environments = []
            logger.warning(f"Error fetching environments: {str(e)}")
            
        return render(request, 'metadata_manager/glossary/import_export.html', {
            'page_title': 'Import/Export Glossary',
            'environments': environments
        })
    
    def post(self, request):
        """Handle import or export"""
        try:
            action = request.POST.get('action')
            
            if action == 'export':
                # Check if environment-specific export is requested
                environment_id = request.POST.get('environment_id')
                environment_config = None
                
                if environment_id:
                    try:
                        from web_ui.metadata_manager.models import Environment
                        environment = Environment.objects.get(id=environment_id)
                        environment_config = {
                            'id': str(environment.id),
                            'name': environment.name,
                            'datahub_url': environment.datahub_url,
                            'git_branch': environment.git_branch,
                            'add_environment_to_description': True
                        }
                    except Exception as e:
                        logger.warning(f"Error fetching environment {environment_id}: {str(e)}")
                
                # Get all nodes and terms
                nodes = GlossaryNode.objects.all()
                terms = GlossaryTerm.objects.all()
                
                # Apply mutations if environment is specified
                if environment_config:
                    # Import mutation store
                    from web_ui.services.mutation_store import mutation_store
                    
                    # Transform entities
                    transformed_nodes = []
                    for node in nodes:
                        # Create a copy of the node to avoid modifying the original
                        node_copy = GlossaryNode(
                            name=node.name,
                            description=node.description,
                            deterministic_urn=node.deterministic_urn,
                            original_urn=node.original_urn
                        )
                        
                        # Set parent if applicable
                        if node.parent:
                            node_copy.parent = node.parent
                            
                        # Apply mutations
                        transformed_node = mutation_store.transform_entity(node_copy, environment_config)
                        transformed_nodes.append(transformed_node)
                    
                    transformed_terms = []
                    for term in terms:
                        # Create a copy of the term to avoid modifying the original
                        term_copy = GlossaryTerm(
                            name=term.name,
                            description=term.description,
                            deterministic_urn=term.deterministic_urn,
                            original_urn=term.original_urn,
                            term_source=term.term_source
                        )
                        
                        # Set parent node if applicable
                        if term.parent_node:
                            term_copy.parent_node = term.parent_node
                            
                        # Apply mutations
                        transformed_term = mutation_store.transform_entity(term_copy, environment_config)
                        transformed_terms.append(transformed_term)
                        
                    export_data = {
                        'nodes': [node.to_dict() for node in transformed_nodes],
                        'terms': [term.to_dict() for term in transformed_terms],
                        'environment': environment_config.get('name') if environment_config else None
                    }
                else:
                    # No environment specified, use original entities
                    export_data = {
                        'nodes': [node.to_dict() for node in nodes],
                        'terms': [term.to_dict() for term in terms],
                        'environment': None
                    }
                
                # Set filename based on environment
                if environment_config:
                    filename = f"glossary_export_{environment_config.get('name')}.json"
                else:
                    filename = "glossary_export.json"
                
                response = HttpResponse(
                    json.dumps(export_data, indent=2),
                    content_type='application/json'
                )
                response['Content-Disposition'] = f'attachment; filename="{filename}"'
                return response
            elif action == 'import':
                # Import from uploaded JSON file
                if 'json_file' not in request.FILES:
                    messages.error(request, "No file uploaded")
                    return redirect('glossary_import_export')
                
                file = request.FILES['json_file']
                try:
                    import_data = json.loads(file.read().decode('utf-8'))
                    
                    # Import nodes first
                    node_count = 0
                    term_count = 0
                    
                    # Import nodes (parent nodes first)
                    imported_nodes = {}
                    
                    # First pass - create nodes without parents
                    for node_data in import_data.get('nodes', []):
                        if 'parent_urn' not in node_data:
                            node, created = GlossaryNode.objects.get_or_create(
                                deterministic_urn=node_data.get('urn'),
                                defaults={
                                    'name': node_data.get('name'),
                                    'description': node_data.get('description', ''),
                                    'original_urn': node_data.get('original_urn'),
                                    'sync_status': 'LOCAL_ONLY'
                                }
                            )
                            
                            if not created:
                                # Update existing node
                                node.name = node_data.get('name')
                                node.description = node_data.get('description', '')
                                node.original_urn = node_data.get('original_urn')
                                node.save()
                            
                            imported_nodes[node_data.get('urn')] = node
                            node_count += 1
                    
                    # Second pass - create nodes with parents
                    for node_data in import_data.get('nodes', []):
                        if 'parent_urn' in node_data:
                            parent = imported_nodes.get(node_data.get('parent_urn'))
                            
                            node, created = GlossaryNode.objects.get_or_create(
                                deterministic_urn=node_data.get('urn'),
                                defaults={
                                    'name': node_data.get('name'),
                                    'description': node_data.get('description', ''),
                                    'original_urn': node_data.get('original_urn'),
                                    'parent': parent,
                                    'sync_status': 'LOCAL_ONLY'
                                }
                            )
                            
                            if not created:
                                # Update existing node
                                node.name = node_data.get('name')
                                node.description = node_data.get('description', '')
                                node.original_urn = node_data.get('original_urn')
                                node.parent = parent
                                node.save()
                            
                            imported_nodes[node_data.get('urn')] = node
                            node_count += 1
                    
                    # Import terms
                    for term_data in import_data.get('terms', []):
                        parent_node = None
                        if 'parent_node_urn' in term_data:
                            parent_node = imported_nodes.get(term_data.get('parent_node_urn'))
                        
                        term, created = GlossaryTerm.objects.get_or_create(
                            deterministic_urn=term_data.get('urn'),
                            defaults={
                                'name': term_data.get('name'),
                                'description': term_data.get('description', ''),
                                'original_urn': term_data.get('original_urn'),
                                'parent_node': parent_node,
                                'term_source': term_data.get('term_source'),
                                'sync_status': 'LOCAL_ONLY'
                            }
                        )
                        
                        if not created:
                            # Update existing term
                            term.name = term_data.get('name')
                            term.description = term_data.get('description', '')
                            term.original_urn = term_data.get('original_urn')
                            term.parent_node = parent_node
                            term.term_source = term_data.get('term_source')
                            term.save()
                        
                        term_count += 1
                    
                    messages.success(request, f"Successfully imported {node_count} nodes and {term_count} terms")
                except json.JSONDecodeError:
                    messages.error(request, "Invalid JSON file")
                except Exception as e:
                    logger.error(f"Error importing glossary data: {str(e)}")
                    messages.error(request, f"Error importing data: {str(e)}")
                
                return redirect('glossary_list')
            else:
                messages.error(request, "Invalid action")
                return redirect('glossary_import_export')
        except Exception as e:
            logger.error(f"Error in glossary import/export view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('glossary_list')


class GlossaryNodeCreateView(View):
    """View to create a new glossary node"""
    
    def post(self, request):
        """Create a new glossary node"""
        try:
            name = request.POST.get('name')
            description = request.POST.get('description', '')
            parent_id = request.POST.get('parent_id')
            
            if not name:
                messages.error(request, "Glossary node name is required")
                return redirect('glossary_list')
            
            # Handle parent node if specified
            parent = None
            if parent_id:
                try:
                    parent = GlossaryNode.objects.get(id=parent_id)
                except GlossaryNode.DoesNotExist:
                    messages.error(request, f"Parent node not found")
                    return redirect('glossary_list')
            
            # Generate deterministic URN
            # For nodes with a parent, include the parent path in the URN
            if parent:
                parent_path = get_parent_path(parent)
                deterministic_urn = get_full_urn_from_name("glossaryNode", name, parent_path=parent_path)
            else:
                deterministic_urn = get_full_urn_from_name("glossaryNode", name)
            
            # Check if node with this URN already exists
            if GlossaryNode.objects.filter(deterministic_urn=deterministic_urn).exists():
                messages.error(request, f"Glossary node with name '{name}' already exists")
                return redirect('glossary_list')
            
            # Create the node
            node = GlossaryNode.objects.create(
                name=name,
                description=description,
                parent=parent,
                deterministic_urn=deterministic_urn
            )
            
            messages.success(request, f"Glossary node '{name}' created successfully")
            return redirect('glossary_list')
        except Exception as e:
            logger.error(f"Error creating glossary node: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('glossary_list')


class GlossaryNodeDetailView(View):
    """View to display, edit and delete glossary nodes"""
    
    def get(self, request, node_id):
        """Display glossary node details"""
        try:
            node = get_object_or_404(GlossaryNode, id=node_id)
            
            # Get related terms and child nodes
            terms = GlossaryTerm.objects.filter(parent_node=node).order_by('name')
            children = GlossaryNode.objects.filter(parent=node).order_by('name')
            
            # Get available environments
            try:
                from web_ui.metadata_manager.models import Environment
                environments = Environment.objects.all().order_by('-is_default', 'name')
            except Exception as e:
                environments = []
                logger.warning(f"Error fetching environments: {str(e)}")
            
            # Initialize context
            context = {
                'node': node,
                'terms': terms,
                'children': children,
                'environments': environments,
                'page_title': f'Glossary Node: {node.name}',
                'has_git_integration': False  # Set this based on checking GitHub settings
            }
            
            # Check if git integration is enabled
            try:
                from web_ui.web_ui.models import GitSettings
                github_settings = GitSettings.objects.first()
                context['has_git_integration'] = github_settings and github_settings.enabled
            except:
                pass
            
            # Get DataHub connection info
            connected, client = test_datahub_connection()
            if connected and client:
                context['has_datahub_connection'] = True
                
                # Get the DataHub URL to display
                if hasattr(client, 'server_url'):
                    context['datahub_url'] = client.server_url
                
                # Get remote node information if possible
                if node.sync_status != 'LOCAL_ONLY':
                    try:
                        remote_node = client.get_glossary_node(node.deterministic_urn)
                        if remote_node:
                            context['remote_node'] = remote_node
                            
                            # Check if the node needs to be synced
                            local_description = node.description or ""
                            remote_description = remote_node.get('description', "")
                            
                            # If different, mark as modified
                            if local_description != remote_description and node.sync_status != 'MODIFIED':
                                node.sync_status = 'MODIFIED'
                                node.save(update_fields=['sync_status'])
                            
                            # If the same but marked as modified, update to synced
                            elif local_description == remote_description and node.sync_status == 'MODIFIED':
                                node.sync_status = 'SYNCED'
                                node.save(update_fields=['sync_status'])
                    except Exception as e:
                        logger.warning(f"Error fetching remote node information: {str(e)}")
            else:
                context['has_datahub_connection'] = False
            
            return render(request, 'metadata_manager/glossary/node_detail.html', context)
        except Exception as e:
            logger.error(f"Error in glossary node detail view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('glossary_list')
    
    def post(self, request, node_id):
        """Update a glossary node"""
        try:
            node = get_object_or_404(GlossaryNode, id=node_id)
            
            name = request.POST.get('name')
            description = request.POST.get('description', '')
            parent_id = request.POST.get('parent_id')
            
            if not name:
                messages.error(request, "Glossary node name is required")
                return redirect('glossary_node_detail', node_id=node_id)
            
            # Handle parent node if specified
            parent = None
            if parent_id:
                if str(parent_id) == str(node_id):
                    messages.error(request, "A node cannot be its own parent")
                    return redirect('glossary_node_detail', node_id=node_id)
                
                try:
                    parent = GlossaryNode.objects.get(id=parent_id)
                    
                    # Check for circular references
                    current_parent = parent
                    while current_parent:
                        if str(current_parent.id) == str(node_id):
                            messages.error(request, "Circular reference detected in parent hierarchy")
                            return redirect('glossary_node_detail', node_id=node_id)
                        current_parent = current_parent.parent
                except GlossaryNode.DoesNotExist:
                    messages.error(request, f"Parent node not found")
                    return redirect('glossary_node_detail', node_id=node_id)
            
            # Update the node
            node.name = name
            node.description = description
            node.parent = parent
            
            # If the node was previously synced, mark it as modified
            if node.sync_status in ['SYNCED', 'REMOTE_ONLY']:
                node.sync_status = 'MODIFIED'
            
            node.save()
            
            messages.success(request, f"Glossary node '{name}' updated successfully")
            return redirect('glossary_node_detail', node_id=node_id)
        except Exception as e:
            logger.error(f"Error updating glossary node: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('glossary_list')
    
    def delete(self, request, node_id):
        """Delete a glossary node"""
        try:
            node = get_object_or_404(GlossaryNode, id=node_id)
            
            # Check if node has children or terms
            children = GlossaryNode.objects.filter(parent=node)
            terms = GlossaryTerm.objects.filter(parent_node=node)
            
            if children.exists() or terms.exists():
                return JsonResponse({
                    'success': False,
                    'message': "Cannot delete node with children or terms. Please delete or move them first."
                })
            
            # Delete the node
            node_name = node.name
            node.delete()
            
            return JsonResponse({
                'success': True,
                'message': f"Glossary node '{node_name}' deleted successfully"
            })
        except Exception as e:
            logger.error(f"Error deleting glossary node: {str(e)}")
            return JsonResponse({
                'success': False,
                'message': f"An error occurred: {str(e)}"
            })


class GlossaryNodeDeployView(View):
    """View to deploy a glossary node to DataHub"""
    
    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def post(self, request, node_id):
        """Deploy a glossary node to DataHub"""
        try:
            node = get_object_or_404(GlossaryNode, id=node_id)
            
            # Check if the node can be deployed
            if not node.can_deploy:
                messages.error(request, f"Node '{node.name}' cannot be deployed (current status: {node.get_sync_status_display()})")
                return redirect('glossary_node_detail', node_id=node_id)
            
            # Get the client
            connected, client = test_datahub_connection()
            
            if not connected or not client:
                messages.error(request, "Cannot connect to DataHub. Please check your connection settings.")
                return redirect('glossary_node_detail', node_id=node_id)
            
            # Check if this is a new node or an update
            if node.sync_status == 'LOCAL_ONLY':
                # New node
                try:
                    # Get parent node URN if applicable
                    parent_urn = None
                    if node.parent:
                        parent_urn = node.parent.deterministic_urn
                    
                    # Create node in DataHub
                    node_id_part = node.deterministic_urn.split(':')[-1]
                    result = client.create_glossary_node(
                        node_id=node_id_part,
                        name=node.name,
                        description=node.description or '',
                        parent_urn=parent_urn
                    )
                    
                    if result:
                        # Update node with remote URN and status
                        node.original_urn = result
                        node.sync_status = 'SYNCED'
                        node.last_synced = timezone.now()
                        node.save()
                        
                        messages.success(request, f"Successfully deployed node '{node.name}' to DataHub")
                    else:
                        messages.error(request, f"Failed to deploy node '{node.name}' to DataHub")
                except Exception as e:
                    logger.error(f"Error deploying glossary node {node.name}: {str(e)}")
                    messages.error(request, f"Error deploying node: {str(e)}")
            else:
                # Update existing node
                try:
                    # Update node in DataHub
                    result = client.update_glossary_node(
                        node_urn=node.deterministic_urn,
                        name=node.name,
                        description=node.description or ''
                    )
                    
                    if result:
                        # Update node status
                        node.sync_status = 'SYNCED'
                        node.last_synced = timezone.now()
                        node.save()
                        
                        messages.success(request, f"Successfully updated node '{node.name}' in DataHub")
                    else:
                        messages.error(request, f"Failed to update node '{node.name}' in DataHub")
                except Exception as e:
                    logger.error(f"Error updating glossary node {node.name}: {str(e)}")
                    messages.error(request, f"Error updating node: {str(e)}")
            
            return redirect('glossary_node_detail', node_id=node_id)
        except Exception as e:
            logger.error(f"Error in glossary node deploy view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('glossary_list')


class GlossaryNodeGitPushView(View):
    """View to push a glossary node to GitHub"""
    
    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def post(self, request, node_id):
        """Push a glossary node to a GitHub PR"""
        try:
            from web_ui.web_ui.models import GitSettings
            from web_ui.views import add_entity_to_git_push
            
            node = get_object_or_404(GlossaryNode, id=node_id)
            
            # Get GitHub settings
            github_settings = GitSettings.objects.first()
            if not github_settings or not github_settings.enabled:
                return JsonResponse({
                    'success': False,
                    'error': 'GitHub integration is not enabled'
                })
            
            # Apply environment mutations if environment is specified
            environment_id = request.POST.get('environment_id')
            if environment_id:
                try:
                    from web_ui.metadata_manager.models import Environment
                    from web_ui.services.mutation_store import mutation_store
                    
                    environment = Environment.objects.get(id=environment_id)
                    environment_config = {
                        'id': str(environment.id),
                        'name': environment.name,
                        'datahub_url': environment.datahub_url,
                        'git_branch': environment.git_branch,
                        'add_environment_to_description': True
                    }
                    
                    # Create a copy of the node to avoid modifying the original
                    node_copy = GlossaryNode(
                        name=node.name,
                        description=node.description,
                        deterministic_urn=node.deterministic_urn,
                        original_urn=node.original_urn
                    )
                    
                    # Set parent if applicable
                    if node.parent:
                        node_copy.parent = node.parent
                        
                    # Apply mutations
                    node_to_push = mutation_store.transform_entity(node_copy, environment_config)
                    
                    # Add a note about the environment
                    if not node_to_push.description:
                        node_to_push.description = f"Environment: {environment.name}"
                    elif f"Environment: {environment.name}" not in node_to_push.description:
                        node_to_push.description += f"\nEnvironment: {environment.name}"
                except Exception as e:
                    logger.error(f"Error applying environment mutations: {str(e)}")
                    # Fall back to original node
                    node_to_push = node
            else:
                # No environment specified, use original node
                node_to_push = node
            
            # Add node to Git staging
            try:
                result = add_entity_to_git_push(node_to_push)
                
                if result.get('success', False):
                    return JsonResponse({
                        'success': True,
                        'message': f"Node '{node_to_push.name}' added to GitHub PR"
                    })
                else:
                    return JsonResponse({
                        'success': False,
                        'error': result.get('error', 'Unknown error')
                    })
            except Exception as e:
                logger.error(f"Error pushing glossary node to GitHub: {str(e)}")
                return JsonResponse({
                    'success': False,
                    'error': str(e)
                })
        except Exception as e:
            logger.error(f"Error in glossary node GitHub push view: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            })


class GlossaryTermCreateView(View):
    """View to create a new glossary term"""
    
    def post(self, request):
        """Create a new glossary term"""
        try:
            name = request.POST.get('name')
            description = request.POST.get('description', '')
            parent_node_id = request.POST.get('parent_node_id')
            term_source = request.POST.get('term_source', '')
            
            if not name:
                messages.error(request, "Glossary term name is required")
                return redirect('glossary_list')
            
            # Handle with or without parent node
            parent_node = None
            if parent_node_id:
                try:
                    parent_node = GlossaryNode.objects.get(id=parent_node_id)
                except GlossaryNode.DoesNotExist:
                    messages.error(request, f"Parent node not found")
                    return redirect('glossary_list')
            
            # Generate deterministic URN
            if parent_node:
                # For terms with a parent, include the parent node path in the URN
                parent_path = get_parent_path(parent_node)
                deterministic_urn = get_full_urn_from_name("glossaryTerm", name, parent_path=parent_path)
            else:
                # For root-level terms
                deterministic_urn = get_full_urn_from_name("glossaryTerm", name)
            
            # Check if term with this URN already exists
            if GlossaryTerm.objects.filter(deterministic_urn=deterministic_urn).exists():
                messages.error(request, f"Glossary term with name '{name}' already exists under this parent")
                return redirect('glossary_list')
            
            # Create the term
            term = GlossaryTerm.objects.create(
                name=name,
                description=description,
                parent_node=parent_node,
                term_source=term_source,
                deterministic_urn=deterministic_urn,
                sync_status='LOCAL_ONLY'
            )
            
            messages.success(request, f"Glossary term '{name}' created successfully")
            return redirect('glossary_list')
        except Exception as e:
            logger.error(f"Error creating glossary term: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('glossary_list')


class GlossaryTermDetailView(View):
    """View to display, edit and delete glossary terms"""
    
    def get(self, request, term_id):
        """Display glossary term details"""
        try:
            term = get_object_or_404(GlossaryTerm, id=term_id)
            
            # Get available environments
            try:
                from web_ui.metadata_manager.models import Environment
                environments = Environment.objects.all().order_by('-is_default', 'name')
            except Exception as e:
                environments = []
                logger.warning(f"Error fetching environments: {str(e)}")
            
            # Get related data from DataHub if connected
            related_terms = []
            owners = []
            domain = None
            related_entities = []
            
            # Check if DataHub connection is available
            connected, client = test_datahub_connection()
            
            if connected and client and term.original_urn:
                try:
                    # Get term details from DataHub
                    remote_term = client.get_glossary_term(term.deterministic_urn or term.original_urn)
                    
                    if remote_term:
                        # Extract related terms
                        if "relatedTerms" in remote_term:
                            related_terms = remote_term.get("relatedTerms", [])
                            
                        # Extract owners
                        if "ownership" in remote_term and "owners" in remote_term["ownership"]:
                            owners = remote_term["ownership"]["owners"]
                            
                        # Extract domain
                        if "domain" in remote_term and remote_term["domain"]:
                            domain = remote_term["domain"]
                            
                        # Extract related entities
                        # This may require a separate query depending on DataHub version
                        try:
                            # Get entities with this term applied
                            entity_results = client.find_entities_with_metadata(
                                field_type="glossaryTerms",
                                metadata_urn=term.deterministic_urn or term.original_urn,
                                count=10
                            )
                            
                            if entity_results and "entities" in entity_results:
                                related_entities = entity_results["entities"]
                        except Exception as e:
                            logger.warning(f"Error fetching related entities: {str(e)}")
                except Exception as e:
                    logger.warning(f"Error fetching related term data: {str(e)}")
            
            # Initialize context
            context = {
                'term': term,
                'nodes': GlossaryNode.objects.all().order_by('name'),  # For parent node selection
                'environments': environments,
                'related_terms': related_terms,
                'owners': owners,
                'domain': domain,
                'related_entities': related_entities,
                'has_datahub_connection': connected,
                'page_title': f'Glossary Term: {term.name}',
                'has_git_integration': False  # Set this based on checking GitHub settings
            }
            
            # Check if git integration is enabled
            try:
                from web_ui.web_ui.models import GitSettings
                github_settings = GitSettings.objects.first()
                context['has_git_integration'] = github_settings and github_settings.enabled
            except:
                pass
            
            # Get DataHub connection info
            if connected and client:
                context['has_datahub_connection'] = True
                
                # Get the DataHub URL to display
                if hasattr(client, 'server_url'):
                    context['datahub_url'] = client.server_url
                
                # Get remote term information if possible
                if term.sync_status != 'LOCAL_ONLY':
                    try:
                        remote_term = client.get_glossary_term(term.deterministic_urn)
                        if remote_term:
                            context['remote_term'] = remote_term
                            
                            # Check if the term needs to be synced
                            local_description = term.description or ""
                            remote_description = remote_term.get('description', "")
                            
                            # If different, mark as modified
                            if local_description != remote_description and term.sync_status != 'MODIFIED':
                                term.sync_status = 'MODIFIED'
                                term.save(update_fields=['sync_status'])
                            
                            # If the same but marked as modified, update to synced
                            elif local_description == remote_description and term.sync_status == 'MODIFIED':
                                term.sync_status = 'SYNCED'
                                term.save(update_fields=['sync_status'])
                    except Exception as e:
                        logger.warning(f"Error fetching remote term information: {str(e)}")
            else:
                context['has_datahub_connection'] = False
            
            return render(request, 'metadata_manager/glossary/term_detail.html', context)
        except Exception as e:
            logger.error(f"Error in glossary term detail view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('glossary_list')
    
    def post(self, request, term_id):
        """Update a glossary term"""
        try:
            action = request.POST.get('action', 'update')
            term = get_object_or_404(GlossaryTerm, id=term_id)
            
            # Handle different actions
            if action == 'update':
                # Basic term update
                name = request.POST.get('name')
                description = request.POST.get('description', '')
                parent_node_id = request.POST.get('parent_node_id')
                term_source = request.POST.get('term_source', '')
                
                if not name:
                    messages.error(request, "Glossary term name is required")
                    return redirect('glossary_term_detail', term_id=term_id)
                
                # Handle with or without parent node
                parent_node = None
                if parent_node_id:
                    try:
                        parent_node = GlossaryNode.objects.get(id=parent_node_id)
                    except GlossaryNode.DoesNotExist:
                        messages.error(request, f"Parent node not found")
                        return redirect('glossary_term_detail', term_id=term_id)
                
                # Update the term
                term.name = name
                term.description = description
                term.parent_node = parent_node
                term.term_source = term_source
                
                # If the term was previously synced, mark it as modified
                if term.sync_status in ['SYNCED', 'REMOTE_ONLY']:
                    term.sync_status = 'MODIFIED'
                
                term.save()
                
                messages.success(request, f"Glossary term '{name}' updated successfully")
                
            elif action == 'set_domain':
                # Set domain for the term
                domain_urn = request.POST.get('domain_urn', '')
                
                # Update local model first
                term.domain_urn = domain_urn
                term.sync_status = 'MODIFIED' if term.is_remote else 'LOCAL_ONLY'
                term.save()
                
                # Try to update in DataHub if connected
                if term.is_remote:
                    connected, client = test_datahub_connection()
                    if connected and client:
                        term_urn = term.deterministic_urn or term.original_urn
                        
                        try:
                            if domain_urn:
                                # Set domain
                                result = client.set_domain_for_glossary_term(term_urn, domain_urn)
                                if result:
                                    messages.success(request, f"Domain '{domain_urn}' set for this term locally and in DataHub")
                                else:
                                    messages.warning(request, f"Domain '{domain_urn}' set locally, but failed to update in DataHub")
                            else:
                                # Unset domain
                                result = client.unset_domain_for_glossary_term(term_urn)
                                if result:
                                    messages.success(request, "Domain removed from this term locally and in DataHub")
                                else:
                                    messages.warning(request, "Domain removed locally, but failed to update in DataHub")
                        except Exception as e:
                            logger.error(f"Error updating domain in DataHub: {str(e)}")
                            if domain_urn:
                                messages.warning(request, f"Domain '{domain_urn}' set locally, but not updated in DataHub: {str(e)}")
                            else:
                                messages.warning(request, f"Domain removed locally, but not updated in DataHub: {str(e)}")
                    else:
                        if domain_urn:
                            messages.success(request, f"Domain '{domain_urn}' set locally. Not connected to DataHub for remote update.")
                        else:
                            messages.success(request, "Domain removed locally. Not connected to DataHub for remote update.")
                else:
                    # Term only exists locally
                    if domain_urn:
                        messages.success(request, f"Domain '{domain_urn}' set for this term locally")
                    else:
                        messages.success(request, "Domain removed from this term locally")
                
            elif action == 'add_owner':
                # Add owner to the term
                owner_urn = request.POST.get('owner_urn')
                ownership_type = request.POST.get('ownership_type', 'urn:li:ownershipType:__system__technical_owner')
                
                if not owner_urn:
                    messages.error(request, "Owner URN is required")
                    return redirect('glossary_term_detail', term_id=term_id)
                
                # Get DataHub connection
                connected, client = test_datahub_connection()
                
                if not connected or not client:
                    messages.error(request, "Cannot connect to DataHub. Please check your connection settings.")
                    return redirect('glossary_term_detail', term_id=term_id)
                
                if not term.deterministic_urn and not term.original_urn:
                    messages.error(request, "Term URN is required for this operation")
                    return redirect('glossary_term_detail', term_id=term_id)
                
                term_urn = term.deterministic_urn or term.original_urn
                
                # Add owner
                result = client.add_owners_to_glossary_term(term_urn, owner_urn, ownership_type)
                if result:
                    messages.success(request, "Owner has been added to this term")
                else:
                    messages.error(request, "Failed to add owner to this term")
                
            elif action == 'remove_owner':
                # Remove owner from the term
                owner_urn = request.POST.get('owner_urn')
                ownership_type = request.POST.get('ownership_type', 'urn:li:ownershipType:__system__technical_owner')
                
                if not owner_urn:
                    messages.error(request, "Owner URN is required")
                    return redirect('glossary_term_detail', term_id=term_id)
                
                # Get DataHub connection
                connected, client = test_datahub_connection()
                
                if not connected or not client:
                    messages.error(request, "Cannot connect to DataHub. Please check your connection settings.")
                    return redirect('glossary_term_detail', term_id=term_id)
                
                if not term.deterministic_urn and not term.original_urn:
                    messages.error(request, "Term URN is required for this operation")
                    return redirect('glossary_term_detail', term_id=term_id)
                
                term_urn = term.deterministic_urn or term.original_urn
                
                # Remove owner
                result = client.remove_owner_from_glossary_term(term_urn, owner_urn, ownership_type)
                if result:
                    messages.success(request, "Owner has been removed from this term")
                else:
                    messages.error(request, "Failed to remove owner from this term")
                
            elif action == 'add_related_term':
                # Add related term
                related_term_urn = request.POST.get('related_term_urn')
                relationship_type = request.POST.get('relationship_type', 'hasA')
                
                if not related_term_urn:
                    messages.error(request, "Related term URN is required")
                    return redirect('glossary_term_detail', term_id=term_id)
                
                # Get DataHub connection
                connected, client = test_datahub_connection()
                
                if not connected or not client:
                    messages.error(request, "Cannot connect to DataHub. Please check your connection settings.")
                    return redirect('glossary_term_detail', term_id=term_id)
                
                if not term.deterministic_urn and not term.original_urn:
                    messages.error(request, "Term URN is required for this operation")
                    return redirect('glossary_term_detail', term_id=term_id)
                
                term_urn = term.deterministic_urn or term.original_urn
                
                # Add related term
                result = client.add_related_terms(term_urn, related_term_urn, relationship_type)
                if result:
                    messages.success(request, "Related term has been added")
                else:
                    messages.error(request, "Failed to add related term")
                
            elif action == 'remove_related_term':
                # Remove related term
                related_term_urn = request.POST.get('related_term_urn')
                relationship_type = request.POST.get('relationship_type', 'hasA')
                
                if not related_term_urn:
                    messages.error(request, "Related term URN is required")
                    return redirect('glossary_term_detail', term_id=term_id)
                
                # Get DataHub connection
                connected, client = test_datahub_connection()
                
                if not connected or not client:
                    messages.error(request, "Cannot connect to DataHub. Please check your connection settings.")
                    return redirect('glossary_term_detail', term_id=term_id)
                
                if not term.deterministic_urn and not term.original_urn:
                    messages.error(request, "Term URN is required for this operation")
                    return redirect('glossary_term_detail', term_id=term_id)
                
                term_urn = term.deterministic_urn or term.original_urn
                
                # Remove related term
                result = client.remove_related_terms(term_urn, related_term_urn, relationship_type)
                if result:
                    messages.success(request, "Related term has been removed")
                else:
                    messages.error(request, "Failed to remove related term")
            
            # Handle moving a term
            elif action == 'move_term':
                move_destination = request.POST.get('move_destination')
                
                if move_destination == 'root':
                    # Move to root (no parent)
                    old_parent = term.parent_node
                    term.parent_node = None
                    
                    # Generate new URN (at root level)
                    new_urn = get_full_urn_from_name("glossaryTerm", term.name)
                    
                    # Check if this would cause a conflict
                    if GlossaryTerm.objects.filter(deterministic_urn=new_urn).exclude(id=term.id).exists():
                        messages.error(request, f"Cannot move term to root: A term with the name '{term.name}' already exists at the root level")
                        return redirect('glossary_term_detail', term_id=term.id)
                    
                    # Update the URN
                    term.deterministic_urn = new_urn
                    term.sync_status = 'MODIFIED' if term.is_remote else 'LOCAL_ONLY'
                    term.save()
                    
                    messages.success(request, f"Term '{term.name}' moved to root level")
                    
                elif move_destination == 'node':
                    # Move to a specific node
                    new_parent_id = request.POST.get('new_parent_node_id')
                    
                    if not new_parent_id:
                        messages.error(request, "Please select a parent node")
                        return redirect('glossary_term_detail', term_id=term.id)
                    
                    try:
                        new_parent = GlossaryNode.objects.get(id=new_parent_id)
                    except GlossaryNode.DoesNotExist:
                        messages.error(request, "Selected parent node does not exist")
                        return redirect('glossary_term_detail', term_id=term.id)
                    
                    # If this is the same parent, no change needed
                    if term.parent_node and term.parent_node.id == new_parent.id:
                        messages.info(request, f"Term '{term.name}' is already under '{new_parent.name}'")
                        return redirect('glossary_term_detail', term_id=term.id)
                    
                    # Generate new URN with the new parent path
                    parent_path = get_parent_path(new_parent)
                    new_urn = get_full_urn_from_name("glossaryTerm", term.name, parent_path=parent_path)
                    
                    # Check if this would cause a conflict
                    if GlossaryTerm.objects.filter(deterministic_urn=new_urn).exclude(id=term.id).exists():
                        messages.error(request, f"Cannot move term: A term with the name '{term.name}' already exists under '{new_parent.name}'")
                        return redirect('glossary_term_detail', term_id=term.id)
                    
                    # Update the term
                    old_parent = term.parent_node
                    term.parent_node = new_parent
                    term.deterministic_urn = new_urn
                    term.sync_status = 'MODIFIED' if term.is_remote else 'LOCAL_ONLY'
                    term.save()
                    
                    messages.success(request, f"Term '{term.name}' moved to '{new_parent.name}'")
                
                return redirect('glossary_term_detail', term_id=term.id)
            
            return redirect('glossary_term_detail', term_id=term_id)
        except Exception as e:
            logger.error(f"Error updating glossary term: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('glossary_list')
            
    def delete(self, request, term_id):
        """Delete a glossary term"""
        try:
            term = get_object_or_404(GlossaryTerm, id=term_id)
            
            # Delete the term
            term_name = term.name
            term.delete()
            
            return JsonResponse({
                'success': True,
                'message': f"Glossary term '{term_name}' deleted successfully"
            })
        except Exception as e:
            logger.error(f"Error deleting glossary term: {str(e)}")
            return JsonResponse({
                'success': False,
                'message': f"An error occurred: {str(e)}"
            })


class GlossaryTermDeployView(View):
    """View to deploy a glossary term to DataHub"""
    
    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def post(self, request, term_id):
        """Deploy a glossary term to DataHub"""
        try:
            term = get_object_or_404(GlossaryTerm, id=term_id)
            
            # Check if the term can be deployed
            if not term.can_deploy:
                messages.error(request, f"Term '{term.name}' cannot be deployed (current status: {term.get_sync_status_display()})")
                return redirect('glossary_term_detail', term_id=term_id)
            
            # Get the client
            connected, client = test_datahub_connection()
            
            if not connected or not client:
                messages.error(request, "Cannot connect to DataHub. Please check your connection settings.")
                return redirect('glossary_term_detail', term_id=term_id)
            
            # Check if the parent node exists in DataHub
            if not term.parent_node:
                messages.error(request, "Terms must have a parent node to be deployed to DataHub")
                return redirect('glossary_term_detail', term_id=term_id)
            
            parent_node_urn = term.parent_node.deterministic_urn
            
            # Check if this is a new term or an update
            if term.sync_status == 'LOCAL_ONLY':
                # New term
                try:
                    # Create term in DataHub
                    term_id_part = term.deterministic_urn.split(':')[-1]
                    result = client.create_glossary_term(
                        term_id=term_id_part,
                        name=term.name,
                        description=term.description or '',
                        parent_node_urn=parent_node_urn,
                        term_source=term.term_source
                    )
                    
                    if result:
                        # Update term with remote URN and status
                        term.original_urn = result
                        term.sync_status = 'SYNCED'
                        term.last_synced = timezone.now()
                        term.save()
                        
                        messages.success(request, f"Successfully deployed term '{term.name}' to DataHub")
                    else:
                        messages.error(request, f"Failed to deploy term '{term.name}' to DataHub")
                except Exception as e:
                    logger.error(f"Error deploying glossary term {term.name}: {str(e)}")
                    messages.error(request, f"Error deploying term: {str(e)}")
            else:
                # Update existing term
                try:
                    # Update term in DataHub
                    result = client.update_glossary_term(
                        term_urn=term.deterministic_urn,
                        name=term.name,
                        description=term.description or '',
                        parent_node_urn=parent_node_urn,
                        term_source=term.term_source
                    )
                    
                    if result:
                        # Update term status
                        term.sync_status = 'SYNCED'
                        term.last_synced = timezone.now()
                        term.save()
                        
                        messages.success(request, f"Successfully updated term '{term.name}' in DataHub")
                    else:
                        messages.error(request, f"Failed to update term '{term.name}' in DataHub")
                except Exception as e:
                    logger.error(f"Error updating glossary term {term.name}: {str(e)}")
                    messages.error(request, f"Error updating term: {str(e)}")
            
            return redirect('glossary_term_detail', term_id=term_id)
        except Exception as e:
            logger.error(f"Error in glossary term deploy view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('glossary_list')


class GlossaryTermGitPushView(View):
    """View to push a glossary term to GitHub"""
    
    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def post(self, request, term_id):
        """Push a glossary term to a GitHub PR"""
        try:
            from web_ui.web_ui.models import GitSettings
            from web_ui.views import add_entity_to_git_push
            
            term = get_object_or_404(GlossaryTerm, id=term_id)
            
            # Get GitHub settings
            github_settings = GitSettings.objects.first()
            if not github_settings or not github_settings.enabled:
                return JsonResponse({
                    'success': False,
                    'error': 'GitHub integration is not enabled'
                })
            
            # Apply environment mutations if environment is specified
            environment_id = request.POST.get('environment_id')
            if environment_id:
                try:
                    from web_ui.metadata_manager.models import Environment
                    from web_ui.services.mutation_store import mutation_store
                    
                    environment = Environment.objects.get(id=environment_id)
                    environment_config = {
                        'id': str(environment.id),
                        'name': environment.name,
                        'datahub_url': environment.datahub_url,
                        'git_branch': environment.git_branch,
                        'add_environment_to_description': True
                    }
                    
                    # Create a copy of the term to avoid modifying the original
                    term_copy = GlossaryTerm(
                        name=term.name,
                        description=term.description,
                        deterministic_urn=term.deterministic_urn,
                        original_urn=term.original_urn,
                        term_source=term.term_source
                    )
                    
                    # Set parent node if applicable
                    if term.parent_node:
                        term_copy.parent_node = term.parent_node
                        
                    # Apply mutations
                    term_to_push = mutation_store.transform_entity(term_copy, environment_config)
                    
                    # Add a note about the environment
                    if not term_to_push.description:
                        term_to_push.description = f"Environment: {environment.name}"
                    elif f"Environment: {environment.name}" not in term_to_push.description:
                        term_to_push.description += f"\nEnvironment: {environment.name}"
                except Exception as e:
                    logger.error(f"Error applying environment mutations: {str(e)}")
                    # Fall back to original term
                    term_to_push = term
            else:
                # No environment specified, use original term
                term_to_push = term
            
            # Add term to Git staging
            try:
                result = add_entity_to_git_push(term_to_push)
                
                if result.get('success', False):
                    return JsonResponse({
                        'success': True,
                        'message': f"Term '{term_to_push.name}' added to GitHub PR"
                    })
                else:
                    return JsonResponse({
                        'success': False,
                        'error': result.get('error', 'Unknown error')
                    })
            except Exception as e:
                logger.error(f"Error pushing glossary term to GitHub: {str(e)}")
                return JsonResponse({
                    'success': False,
                    'error': str(e)
                })
        except Exception as e:
            logger.error(f"Error in glossary term GitHub push view: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            }) 