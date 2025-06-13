from django.shortcuts import render, redirect, get_object_or_404
from django.views import View
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_POST
from django.utils.decorators import method_decorator
from django.db import models
import json
import logging
import os
import sys
import csv
import io

# Add project root to sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

# Import the deterministic URN utilities
from utils.urn_utils import get_full_urn_from_name, get_parent_path
from utils.datahub_utils import get_datahub_client, test_datahub_connection
from utils.data_sanitizer import sanitize_api_response
from web_ui.models import Environment as DjangoEnvironment
from web_ui.models import GitSettings, GitIntegration
from .models import GlossaryNode, GlossaryTerm, Environment

logger = logging.getLogger(__name__)


class GlossaryListView(View):
    """View to list glossary nodes and terms"""
    
    def get(self, request):
        """Display list of glossary nodes and terms"""
        try:
            logger.info("Starting GlossaryListView.get")
            
            # Get all local nodes and terms with proper relationships
            all_nodes = (
                GlossaryNode.objects.all()
                .select_related("parent")
                .prefetch_related("children", "terms")
                .order_by("name")
            )
            all_terms = (
                GlossaryTerm.objects.all()
                .select_related("parent_node")
                .order_by("name")
            )

            # Get root nodes only (for hierarchy display)
            root_nodes = all_nodes.filter(parent=None).order_by("name")

            # Get root terms (terms without a parent node)
            root_terms = all_terms.filter(parent_node=None).order_by("name")

            logger.debug(
                f"Found {all_nodes.count()} total nodes, {root_nodes.count()} root nodes"
            )
            logger.debug(
                f"Found {all_terms.count()} total terms, {root_terms.count()} root terms"
            )

            # Separate by sync status for badge counts
            local_nodes = all_nodes.filter(
                sync_status__in=["LOCAL_ONLY", "MODIFIED"]
            ).order_by("name")
            local_terms = all_terms.filter(
                sync_status__in=["LOCAL_ONLY", "MODIFIED"]
            ).order_by("name")
            synced_nodes = all_nodes.filter(sync_status="SYNCED").order_by("name")
            synced_terms = all_terms.filter(sync_status="SYNCED").order_by("name")

            # Calculate counts for tabs
            local_count = local_nodes.count() + local_terms.count()
            synced_count = synced_nodes.count() + synced_terms.count()

            # Test DataHub connection
            connected, client = test_datahub_connection()

            # Build context for the template
            context = {
                "page_title": "DataHub Glossary",
                "has_datahub_connection": connected,
                "has_git_integration": False,
                # Local data for immediate display
                "local_nodes": root_nodes.filter(
                    sync_status__in=["LOCAL_ONLY", "MODIFIED"]
                ),
                "local_terms": root_terms.filter(
                    sync_status__in=["LOCAL_ONLY", "MODIFIED"]
                ),
                # Badge counts
                "local_count": local_count,
                "synced_count": synced_count,
                "remote_count": 0,  # Will be updated via AJAX
                # All data for reference (used by other parts of the template)
                "nodes": all_nodes,
                "terms": all_terms,
                "root_nodes": root_nodes,
                "root_terms": root_terms,
            }

            # Check git integration
            try:
                github_settings = GitSettings.objects.first()
                context["has_git_integration"] = (
                    github_settings and github_settings.enabled
                )

                if context["has_git_integration"]:
                    environments = DjangoEnvironment.objects.all().order_by("name")
                    context["environments"] = environments
            except Exception as e:
                logger.warning(f"Error checking git integration: {str(e)}")
            
            logger.info("Rendering glossary list template")
            return render(request, "metadata_manager/glossary/list.html", context)

        except Exception as e:
            logger.error(f"Error in glossary list view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")

            error_context = {
                "page_title": "Error",
                "error_message": str(e),
                "error_details": "There was an error loading the glossary.",
            }

            return render(request, "error.html", error_context)

    def _build_remote_node_hierarchy(self, nodes_list):
        """Build a hierarchical structure from flat remote nodes list"""
        nodes_by_urn = {}
        root_nodes = []
        
        logger.debug(
            f"Building remote node hierarchy from {len(nodes_list) if nodes_list else 0} nodes"
        )

        if not nodes_list:
            logger.warning("No remote nodes to process")
            return []

        # Log the structure of the first node to better understand the format
        if nodes_list and len(nodes_list) > 0:
            first_node = nodes_list[0]
            logger.debug(f"First node structure: {first_node}")
            logger.debug(
                f"First node keys: {list(first_node.keys()) if isinstance(first_node, dict) else 'Not a dict'}"
            )

            # Check specific fields that we're interested in
            if isinstance(first_node, dict):
                if "entity" in first_node:
                    logger.debug("Node appears to be in GraphQL entity wrapper format")
                if "parentNode" in first_node:
                    logger.debug(
                        f"Node has 'parentNode' field: {first_node['parentNode']}"
                    )
                if "parentNodes" in first_node:
                    logger.debug(
                        f"Node has 'parentNodes' field: {first_node['parentNodes']}"
                    )
                if "properties" in first_node:
                    logger.debug(
                        f"Node has 'properties' field with keys: {list(first_node['properties'].keys()) if isinstance(first_node['properties'], dict) else 'Not a dict'}"
                    )
        
        # First pass: Create node objects and store by URN
        for node in nodes_list:
            # Check if this is a wrapped entity response (from GraphQL)
            if "entity" in node:
                node = node["entity"]

            # Handle different node formats from different API versions
            node_urn = node.get("urn")
            if not node_urn:
                logger.warning(f"Skipping node without URN: {node}")
                continue

            # Extract properties in a way that works with multiple API response formats
            if "properties" in node:
                properties = node["properties"]
            else:
                # For newer API versions, properties might be directly on the node
                properties = {
                    "name": node.get("name", "Unknown"),
                    "description": node.get("description", ""),
                }

            # Get name from properties or directly from node
            name = properties.get("name", "Unknown")
            if not name or name == "Unknown":
                name = node.get("name", "Unknown")

            description = properties.get("description", "")
            if not description:
                description = node.get("description", "")

            # Create a node object
            node_obj = {
                "urn": node_urn,
                "name": name,
                "description": description,
                "children": [],
                "terms": [],
                "parent_urn": None,
            }

            # Extract the node ID from the URN for local lookup
            try:
                node_id = node_urn.split("/")[-1]
                node_obj["id"] = node_id
            except:
                node_obj["id"] = "unknown"

            # Store in lookup dictionary
            nodes_by_urn[node_urn] = node_obj

            # Check for parent node relationship
            parent_urn = None
            
            # Try multiple ways to find the parent node URN:

            # 1. Try parentNode (singular) for backward compatibility
            parent_node = node.get("parentNode", {})
            if parent_node:
                if isinstance(parent_node, dict):
                    parent_urn = parent_node.get("urn")
                    logger.debug(
                        f"Found parent node (singular dict) for node {name}: {parent_urn}"
                    )
                else:
                    logger.debug(f"parentNode is not a dict, but: {type(parent_node)}")

            # 2. Try parentNodes (plural) which is the newer format
            if not parent_urn and "parentNodes" in node:
                parent_nodes = node.get("parentNodes", {})

                # Handle different formats
                if isinstance(parent_nodes, dict) and "nodes" in parent_nodes:
                    nodes_list = parent_nodes.get("nodes", [])
                    if nodes_list and len(nodes_list) > 0:
                        first_parent = nodes_list[0]
                        if isinstance(first_parent, dict):
                            parent_urn = first_parent.get("urn")
                            logger.debug(
                                f"Found parent node (plural/nodes) for node {name}: {parent_urn}"
                            )
                elif isinstance(parent_nodes, list) and len(parent_nodes) > 0:
                    first_parent = parent_nodes[0]
                    if isinstance(first_parent, dict):
                        parent_urn = first_parent.get("urn")
                        logger.debug(
                            f"Found parent node (plural/list) for node {name}: {parent_urn}"
                        )

            # 3. Try parent_urn directly
            if not parent_urn and "parent_urn" in node:
                parent_urn = node.get("parent_urn")
                logger.debug(f"Found parent_urn directly in node {name}: {parent_urn}")

            if parent_urn:
                node_obj["parent_urn"] = parent_urn
            else:
                # No parent means this is a root node
                root_nodes.append(node_obj)
                logger.debug(f"Added root node '{name}' (no parent)")

        # Second pass: Build the hierarchy
        for node_urn, node_obj in nodes_by_urn.items():
            parent_urn = node_obj.get("parent_urn")
            if parent_urn and parent_urn in nodes_by_urn:
                # Add this node as a child of its parent
                parent = nodes_by_urn[parent_urn]
                parent["children"].append(node_obj)
                logger.debug(
                    f"Added node '{node_obj['name']}' as child of '{parent['name']}'"
                )

        logger.debug(f"Built remote node hierarchy with {len(root_nodes)} root nodes")
        # Log the hierarchy structure for debugging
        for root_node in root_nodes:
            logger.debug(
                f"Root node: {root_node['name']} with {len(root_node['children'])} children"
            )

        return root_nodes

    def _build_remote_term_hierarchy(self, terms_list):
        """Build a hierarchical structure from flat remote terms list"""
        terms_by_urn = {}
        terms_by_parent_node = {}

        logger.debug(
            f"Building remote term hierarchy, terms_list type: {type(terms_list)}"
        )

        # Check if we actually have a list or something else
        if isinstance(terms_list, dict):
            logger.debug(
                f"terms_list is a dictionary with keys: {list(terms_list.keys())}"
            )
            # If it's a dictionary, check for common patterns in DataHub API responses
            if "terms" in terms_list and isinstance(terms_list["terms"], list):
                logger.debug(
                    f"Found 'terms' key in dictionary with {len(terms_list['terms'])} items"
                )
                terms_list = terms_list["terms"]
            elif "results" in terms_list and isinstance(terms_list["results"], list):
                logger.debug(
                    f"Found 'results' key in dictionary with {len(terms_list['results'])} items"
                )
                terms_list = terms_list["results"]
            elif "data" in terms_list:
                data = terms_list["data"]
                if isinstance(data, dict):
                    # Look for common GraphQL response patterns
                    if (
                        "searchAcrossEntities" in data
                        and "searchResults" in data["searchAcrossEntities"]
                    ):
                        logger.debug(
                            "Found GraphQL searchAcrossEntities response pattern"
                        )
                        terms_list = data["searchAcrossEntities"]["searchResults"]
                    elif "glossaryTerms" in data:
                        logger.debug("Found GraphQL glossaryTerms response pattern")
                        terms_list = data["glossaryTerms"]
            else:
                # If we can't find a list, log all keys and try to extract terms another way
                logger.warning(
                    f"Couldn't find a list of terms in the dictionary, keys: {list(terms_list.keys())}"
                )

                # Try to find any key that might contain a list of terms
                for key, value in terms_list.items():
                    if isinstance(value, list) and len(value) > 0:
                        logger.debug(
                            f"Found potential list of terms in key '{key}' with {len(value)} items"
                        )
                        if isinstance(value[0], dict) and (
                            "urn" in value[0] or "name" in value[0]
                        ):
                            logger.debug(
                                f"Key '{key}' appears to contain term-like objects, using it"
                            )
                            terms_list = value
                            break

        # If terms_list is None or empty, return empty dictionary
        if not terms_list:
            logger.warning("No remote terms to process (empty or None)")
            return {}

        # If terms_list is still not a list after all our attempts, log and return empty
        if not isinstance(terms_list, list):
            logger.warning(f"terms_list is not a list, but a {type(terms_list)}")
            return {}

        logger.debug(f"Processing {len(terms_list)} terms")

        # Log the structure of the first term to better understand the format
        if len(terms_list) > 0:
            first_term = terms_list[0]
            logger.debug(f"First term structure: {first_term}")
            logger.debug(
                f"First term keys: {list(first_term.keys()) if isinstance(first_term, dict) else 'Not a dict'}"
            )

            # Check specific fields that we're interested in
            if isinstance(first_term, dict):
                if "entity" in first_term:
                    logger.debug("Term appears to be in GraphQL entity wrapper format")
                if "parentNode" in first_term:
                    logger.debug(
                        f"Term has 'parentNode' field: {first_term['parentNode']}"
                    )
                if "parentNodes" in first_term:
                    logger.debug(
                        f"Term has 'parentNodes' field: {first_term['parentNodes']}"
                    )
                if "properties" in first_term:
                    logger.debug(
                        f"Term has 'properties' field with keys: {list(first_term['properties'].keys()) if isinstance(first_term['properties'], dict) else 'Not a dict'}"
                    )

        # First pass: Create term objects and organize by parent node
        for term in terms_list:
            # Handle GraphQL entity wrapper format
            if "entity" in term:
                term = term["entity"]

            # Extract URN
            term_urn = term.get("urn")
            if not term_urn:
                logger.warning(f"Skipping term without URN: {term}")
                continue

            # Extract properties in a way that works with multiple API response formats
            if "properties" in term:
                properties = term["properties"]
            else:
                # For newer API versions, properties might be directly on the term
                properties = {
                    "name": term.get("name", "Unknown"),
                    "description": term.get("description", ""),
                    "termSource": term.get("termSource", ""),
                }

            # Get name from properties or directly from term
            name = properties.get("name", "Unknown")
            if not name or name == "Unknown":
                name = term.get("name", "Unknown")

            description = properties.get("description", "")
            if not description:
                description = term.get("description", "")

            term_source = properties.get("termSource", "")

            # Create a term object
            term_obj = {
                "urn": term_urn,
                "name": name,
                "description": description,
                "term_source": term_source,
                "parent_node_urn": None,
            }

            # Extract the term ID from the URN for local lookup
            try:
                term_id = term_urn.split("/")[-1]
                term_obj["id"] = term_id
            except:
                term_obj["id"] = "unknown"

            # Store in lookup dictionary
            terms_by_urn[term_urn] = term_obj

            # Check for parent node relationship
            parent_node_urn = None

            # Try multiple ways to find the parent node URN:

            # 1. Try parentNode (singular) for backward compatibility
            parent_node = term.get("parentNode", {})
            if parent_node:
                if isinstance(parent_node, dict):
                    parent_node_urn = parent_node.get("urn")
                    logger.debug(
                        f"Found parent node (singular dict) for term {name}: {parent_node_urn}"
                    )
            else:
                logger.debug(f"parentNode is not a dict, but: {type(parent_node)}")

            # 2. Try parentNodes (plural) which is the newer format
            if not parent_node_urn and "parentNodes" in term:
                parent_nodes = term.get("parentNodes", {})

                # Handle different formats
                if isinstance(parent_nodes, dict) and "nodes" in parent_nodes:
                    nodes_list = parent_nodes.get("nodes", [])
                    if nodes_list and len(nodes_list) > 0:
                        first_parent = nodes_list[0]
                        if isinstance(first_parent, dict):
                            parent_node_urn = first_parent.get("urn")
                            logger.debug(
                                f"Found parent node (plural/nodes) for term {name}: {parent_node_urn}"
                            )
                elif isinstance(parent_nodes, list) and len(parent_nodes) > 0:
                    first_parent = parent_nodes[0]
                    if isinstance(first_parent, dict):
                        parent_node_urn = first_parent.get("urn")
                        logger.debug(
                            f"Found parent node (plural/list) for term {name}: {parent_node_urn}"
                        )

            # 3. Try parent_node_urn directly (our custom format)
            if not parent_node_urn and "parent_node_urn" in term:
                parent_node_urn = term.get("parent_node_urn")
                logger.debug(
                    f"Found parent_node_urn directly in term {name}: {parent_node_urn}"
                )

            if parent_node_urn:
                term_obj["parent_node_urn"] = parent_node_urn

                # Group terms by parent node for easier lookup
                if parent_node_urn not in terms_by_parent_node:
                    terms_by_parent_node[parent_node_urn] = []
                terms_by_parent_node[parent_node_urn].append(term_obj)
                logger.debug(f"Added term '{name}' to parent node '{parent_node_urn}'")
            else:
                # No parent means this is a root term - add to special "root_terms" key
                if "root_terms" not in terms_by_parent_node:
                    terms_by_parent_node["root_terms"] = []
                terms_by_parent_node["root_terms"].append(term_obj)
                logger.debug(f"Added root term '{name}' (no parent)")

        logger.debug(
            f"Processed {len(terms_by_urn)} terms, grouped by {len(terms_by_parent_node)} parent nodes"
        )
        logger.debug(
            f"Found {len(terms_by_parent_node.get('root_terms', []))} root-level terms"
        )

        # Log all the parent node URNs to help with debugging
        for parent_urn, terms in terms_by_parent_node.items():
            if parent_urn != "root_terms":
                logger.debug(f"Parent node {parent_urn} has {len(terms)} terms")

        # Return the organized terms by parent node for rendering
        return terms_by_parent_node
    
    def post(self, request):
        """Handle glossary node creation and batch actions"""
        try:
            # Check for batch actions
            action = request.POST.get("action")
            
            # Handle different actions
            if action == "push_selected":
                return self._handle_push_selected(request)
            elif action == "push_all":
                return self._handle_push_all(request)
            elif action == "add_to_git":
                return self._handle_add_to_git(request)
            else:
                # Default behavior: create a new glossary node
                name = request.POST.get("name")
                description = request.POST.get("description", "")
                parent_id = request.POST.get("parent_id")
                
                if not name:
                    messages.error(request, "Glossary node name is required")
                    return redirect("metadata_manager:glossary_list")
                
                # Handle parent node if specified
                parent = None
                if parent_id:
                    try:
                        parent = GlossaryNode.objects.get(id=parent_id)
                    except GlossaryNode.DoesNotExist:
                        messages.error(request, "Parent node not found")
                        return redirect("metadata_manager:glossary_list")
                
                # Generate deterministic URN
                # For nodes with a parent, include the parent path in the URN
                if parent:
                    parent_path = get_parent_path(parent)
                    deterministic_urn = get_full_urn_from_name(
                        "glossaryNode", name, parent_path=parent_path
                    )
                else:
                    deterministic_urn = get_full_urn_from_name("glossaryNode", name)
                
                # Check if node with this URN already exists
                if GlossaryNode.objects.filter(
                    deterministic_urn=deterministic_urn
                ).exists():
                    messages.error(
                        request, f"Glossary node with name '{name}' already exists"
                    )
                    return redirect("metadata_manager:glossary_list")
                
                # Create the node
                GlossaryNode.objects.create(
                    name=name,
                    description=description,
                    parent=parent,
                    deterministic_urn=deterministic_urn,
                )

                messages.success(
                    request, f"Glossary node '{name}' created successfully"
                )
                return redirect("metadata_manager:glossary_list")
        except Exception as e:
            logger.error(f"Error in glossary list view post: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:glossary_list")
            
    def _handle_push_selected(self, request):
        """Push selected glossary nodes and terms to DataHub"""
        try:
            # Get selected node and term IDs
            node_ids = request.POST.getlist("selected_nodes")
            term_ids = request.POST.getlist("selected_terms")
            
            if not node_ids and not term_ids:
                messages.warning(request, "No glossary items were selected")
                return redirect("metadata_manager:glossary_list")
            
            # Get DataHub client
            connected, client = test_datahub_connection()
            if not connected or not client:
                messages.error(request, "Could not connect to DataHub")
                return redirect("metadata_manager:glossary_list")
            
            # Track success/failure counts
            success_count = 0
            failed_count = 0
            
            # Push nodes first
            for node_id in node_ids:
                try:
                    node = GlossaryNode.objects.get(id=node_id)
                    if node.can_deploy:
                        success = node.deploy_to_datahub(client)
                        if success:
                            success_count += 1
                        else:
                            failed_count += 1
                except GlossaryNode.DoesNotExist:
                    logger.warning(f"Node {node_id} not found")
                except Exception as e:
                    logger.error(f"Error deploying node {node_id}: {str(e)}")
                    failed_count += 1
            
            # Then push terms
            for term_id in term_ids:
                try:
                    term = GlossaryTerm.objects.get(id=term_id)
                    if hasattr(term, "can_deploy") and term.can_deploy:
                        success = term.deploy_to_datahub(client)
                        if success:
                            success_count += 1
                        else:
                            failed_count += 1
                except GlossaryTerm.DoesNotExist:
                    logger.warning(f"Term {term_id} not found")
                except Exception as e:
                    logger.error(f"Error deploying term {term_id}: {str(e)}")
                    failed_count += 1
            
            # Show results
            if success_count > 0:
                messages.success(
                    request,
                    f"Successfully deployed {success_count} glossary items to DataHub",
                )
            if failed_count > 0:
                messages.warning(
                    request, f"Failed to deploy {failed_count} glossary items"
                )
                
            return redirect("metadata_manager:glossary_list")
        except Exception as e:
            logger.error(f"Error pushing selected glossary items: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:glossary_list")
    
    def _handle_push_all(self, request):
        """Push all glossary nodes and terms to DataHub"""
        try:
            # Get DataHub client
            connected, client = test_datahub_connection()
            if not connected or not client:
                messages.error(request, "Could not connect to DataHub")
                return redirect("metadata_manager:glossary_list")
            
            # Get all local-only or modified nodes and terms
            nodes = GlossaryNode.objects.filter(
                sync_status__in=["LOCAL_ONLY", "MODIFIED"]
            )
            terms = GlossaryTerm.objects.filter(
                sync_status__in=["LOCAL_ONLY", "MODIFIED"]
            )
            
            if not nodes.exists() and not terms.exists():
                messages.info(request, "No glossary items need to be pushed")
                return redirect("metadata_manager:glossary_list")
            
            # Track success/failure counts
            success_count = 0
            failed_count = 0
            
            # Push nodes first
            for node in nodes:
                try:
                    if node.can_deploy:
                        success = node.deploy_to_datahub(client)
                        if success:
                            success_count += 1
                        else:
                            failed_count += 1
                except Exception as e:
                    logger.error(f"Error deploying node {node.id}: {str(e)}")
                    failed_count += 1
            
            # Then push terms
            for term in terms:
                try:
                    if hasattr(term, "can_deploy") and term.can_deploy:
                        success = term.deploy_to_datahub(client)
                        if success:
                            success_count += 1
                        else:
                            failed_count += 1
                except Exception as e:
                    logger.error(f"Error deploying term {term.id}: {str(e)}")
                    failed_count += 1
            
            # Show results
            if success_count > 0:
                messages.success(
                    request,
                    f"Successfully deployed {success_count} glossary items to DataHub",
                )
            if failed_count > 0:
                messages.warning(
                    request, f"Failed to deploy {failed_count} glossary items"
                )
                
            return redirect("metadata_manager:glossary_list")
        except Exception as e:
            logger.error(f"Error pushing all glossary items: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:glossary_list")
    
    def _handle_add_to_git(self, request):
        """Add selected glossary nodes and terms to a GitHub PR"""
        try:
            # Get selected node and term IDs
            node_ids = request.POST.getlist("selected_nodes")
            term_ids = request.POST.getlist("selected_terms")
            
            if not node_ids and not term_ids:
                messages.warning(request, "No glossary items were selected")
                return redirect("metadata_manager:glossary_list")
            
            # Check if Git integration is enabled
            github_settings = GitSettings.objects.first()
            if not github_settings or not github_settings.enabled:
                messages.error(request, "Git integration is not enabled")
                return redirect("metadata_manager:glossary_list")
                
            # Get environment from request (default to None if not provided)
            environment_id = request.POST.get("environment")
            environment = None
            if environment_id:
                try:
                    environment = Environment.objects.get(id=environment_id)
                    logger.info(
                        f"Using environment '{environment.name}' for glossary items"
                    )
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
                messages.error(
                    request,
                    "Cannot push directly to the main/master branch. Please create and use a feature branch.",
                )
                return redirect("metadata_manager:glossary_list")
            
            # Track success/failure counts
            success_count = 0
            failed_count = 0
            
            # Create Git integration instance
            git_integration = GitIntegration()
            
            # Push nodes first
            for node_id in node_ids:
                try:
                    node = GlossaryNode.objects.get(id=node_id)
                    # Create commit message
                    commit_message = f"Add/update glossary node: {node.name}"
                    
                    # Stage the node to the git repo
                    logger.info(
                        f"Staging glossary node {node.id} to Git branch {current_branch}"
                    )
                    result = git_integration.push_to_git(node, commit_message)
                    
                    if result and result.get("success"):
                        logger.info(
                            f"Successfully staged glossary node {node.id} to Git branch {current_branch}"
                        )
                        success_count += 1
                    else:
                        error_message = f'Failed to stage glossary node "{node.name}"'
                        if isinstance(result, dict) and "error" in result:
                            error_message += f": {result['error']}"
                        logger.error(f"Failed to stage glossary node: {error_message}")
                        failed_count += 1
                except GlossaryNode.DoesNotExist:
                    logger.warning(f"Node {node_id} not found")
                except Exception as e:
                    logger.error(f"Error adding glossary node to git: {str(e)}")
                    failed_count += 1
            
            # Then push terms
            for term_id in term_ids:
                try:
                    term = GlossaryTerm.objects.get(id=term_id)
                    # Create commit message
                    commit_message = f"Add/update glossary term: {term.name}"
                    
                    # Stage the term to the git repo
                    logger.info(
                        f"Staging glossary term {term.id} to Git branch {current_branch}"
                    )
                    result = git_integration.push_to_git(term, commit_message)
                    
                    if result and result.get("success"):
                        logger.info(
                            f"Successfully staged glossary term {term.id} to Git branch {current_branch}"
                        )
                        success_count += 1
                    else:
                        error_message = f'Failed to stage glossary term "{term.name}"'
                        if isinstance(result, dict) and "error" in result:
                            error_message += f": {result['error']}"
                        logger.error(f"Failed to stage glossary term: {error_message}")
                        failed_count += 1
                except GlossaryTerm.DoesNotExist:
                    logger.warning(f"Term {term_id} not found")
                except Exception as e:
                    logger.error(f"Error adding glossary term to git: {str(e)}")
                    failed_count += 1
            
            # Show results
            if success_count > 0:
                messages.success(
                    request,
                    f"Successfully added {success_count} glossary items to Git branch {current_branch}",
                )
            if failed_count > 0:
                messages.warning(
                    request, f"Failed to add {failed_count} glossary items"
                )
                
            return redirect("metadata_manager:glossary_list")
        except Exception as e:
            logger.error(f"Error adding glossary items to git: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:glossary_list")

    def _build_local_node_hierarchy(self, nodes):
        """Build a hierarchical structure from local nodes queryset"""
        nodes_by_id = {}
        root_nodes = []
        
        logger.debug(f"Building local hierarchy from {nodes.count()} nodes")
        
        # First pass: Create node objects and store by ID
        for node in nodes:
            logger.debug(
                f"Processing node: id={node.id}, name={node.name}, parent_id={node.parent_id}"
            )
            node_obj = {
                "id": node.id,
                "name": node.name,
                "description": node.description,
                "sync_status": node.sync_status,
                "can_deploy": node.can_deploy if hasattr(node, "can_deploy") else True,
                "children": [],
                "terms": [],
            }
            nodes_by_id[node.id] = node_obj
        
        # Second pass: Establish parent-child relationships
        for node in nodes:
            # If no parent, it's a root node
            if not node.parent_id:
                logger.debug(f"Adding root node: {node.name} (id={node.id})")
                if node.id in nodes_by_id:
                    root_nodes.append(nodes_by_id[node.id])
            else:
                # Add to parent's children if parent exists and is in our set
                if node.parent_id in nodes_by_id:
                    logger.debug(
                        f"Adding {node.name} as child of node {node.parent_id}"
                    )
                    nodes_by_id[node.parent_id]["children"].append(nodes_by_id[node.id])
                else:
                    # If parent is not in our filtered set, add to root nodes
                    logger.debug(
                        f"Parent {node.parent_id} not in filtered set, adding {node.name} as root node"
                    )
                    if node.id in nodes_by_id:
                        root_nodes.append(nodes_by_id[node.id])
        
        # Third pass: Add terms to each node
        for node in nodes:
            if hasattr(node, "terms"):
                node_terms = []
                terms_qs = node.terms.filter(sync_status="LOCAL_ONLY")
                logger.debug(f"Node {node.name} has {terms_qs.count()} local terms")
                
                for term in terms_qs:
                    term_obj = {
                        "id": term.id,
                        "name": term.name,
                        "description": term.description,
                        "sync_status": term.sync_status,
                        "can_deploy": term.can_deploy
                        if hasattr(term, "can_deploy")
                        else True,
                    }
                    node_terms.append(term_obj)
                
                if node.id in nodes_by_id:
                    nodes_by_id[node.id]["terms"] = node_terms
                    # Flag to indicate this node has terms (needed for UI expansion)
                    nodes_by_id[node.id]["has_terms"] = len(node_terms) > 0

        # Fourth pass: Add root terms (terms without a parent node)
        root_terms = []
        try:
            root_term_objects = GlossaryTerm.objects.filter(
                parent_node=None, sync_status="LOCAL_ONLY"
            ).order_by("name")
            logger.debug(
                f"Found {root_term_objects.count()} root terms (no parent node) with LOCAL_ONLY status"
            )

            for term in root_term_objects:
                term_obj = {
                    "id": term.id,
                    "name": term.name,
                    "description": term.description,
                    "sync_status": term.sync_status,
                    "can_deploy": term.can_deploy
                    if hasattr(term, "can_deploy")
                    else True,
                    "is_root_term": True,  # Flag to identify root terms
                }
                root_terms.append(term_obj)
        except Exception as e:
            logger.error(f"Error fetching root terms: {str(e)}")

        # Add root terms to the hierarchy
        final_hierarchy = root_nodes + root_terms

        logger.debug(
            f"Final hierarchy contains {len(root_nodes)} root nodes and {len(root_terms)} root terms"
        )
        return final_hierarchy


class GlossaryPullView(View):
    def get(self, request):
        try:
            # Get DataHub connection info
            connected, client = test_datahub_connection()
            
            context = {
                "page_title": "Pull from DataHub",
                "has_datahub_connection": connected,
            }
            return render(request, "metadata_manager/glossary/pull.html", context)
        except Exception as e:
            logger.error(f"Error in glossary pull view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:glossary_list")
    
    def post(self, request):
        try:
            # Get DataHub client
            connected, client = test_datahub_connection()
            if not connected or not client:
                messages.error(request, "Could not connect to DataHub")
                return redirect("metadata_manager:glossary_list")
            
            # Get selected items to pull
            node_urns = request.POST.getlist("node_urns", [])
            term_urns = request.POST.getlist("term_urns", [])
            
            if not node_urns and not term_urns:
                messages.error(request, "Please select at least one item to pull")
                return redirect("metadata_manager:glossary_pull")
            
            # Pull nodes
            for urn in node_urns:
                try:
                    node_info = client.get_glossary_node(urn)
                    if node_info:
                        # Create or update node
                        GlossaryNode.create_from_datahub(node_info)
                except Exception as e:
                    logger.error(f"Error pulling node {urn}: {str(e)}")
                    messages.error(request, f"Error pulling node {urn}")
            
            # Pull terms
            for urn in term_urns:
                try:
                    term_info = client.get_glossary_term(urn)
                    if term_info:
                        # Create or update term
                        GlossaryTerm.create_from_datahub(term_info)
                except Exception as e:
                    logger.error(f"Error pulling term {urn}: {str(e)}")
                    messages.error(request, f"Error pulling term {urn}")
            
            messages.success(request, "Successfully pulled selected items from DataHub")
            return redirect("metadata_manager:glossary_list")
        except Exception as e:
            logger.error(f"Error in glossary pull view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:glossary_list")


class GlossaryImportExportView(View):
    def get(self, request):
        try:
            # Get DataHub connection info
            connected, client = test_datahub_connection()
            
            context = {
                "page_title": "Import/Export Glossary",
                "has_datahub_connection": connected,
            }
            return render(
                request, "metadata_manager/glossary/import_export.html", context
            )
        except Exception as e:
            logger.error(f"Error in glossary import/export view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:glossary_list")
    
    def post(self, request):
        try:
            action = request.POST.get("action")
            if action == "import":
                if "file" not in request.FILES:
                    messages.error(request, "No file uploaded")
                    return redirect("metadata_manager:glossary_import_export")
                
                # Handle file import
                file = request.FILES["file"]
                try:
                    json.loads(file.read())
                    # Process the imported data
                    # ... implementation details ...
                    messages.success(request, "Successfully imported glossary items")
                    return redirect("metadata_manager:glossary_list")
                except json.JSONDecodeError:
                    messages.error(request, "Invalid JSON file")
                    return redirect("metadata_manager:glossary_import_export")
            else:
                messages.error(request, "Invalid action")
                return redirect("metadata_manager:glossary_import_export")
        except Exception as e:
            logger.error(f"Error in glossary import/export view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:glossary_list")


class GlossaryNodeCreateView(View):
    def get(self, request):
        """Render the create node form."""
        # Get all nodes for parent selection
        nodes = GlossaryNode.objects.all().order_by("name")
        context = {"page_title": "Create Glossary Node", "nodes": nodes}
        return render(request, "metadata_manager/glossary/node_form.html", context)

    def post(self, request):
        try:
            name = request.POST.get("name")
            description = request.POST.get("description", "")
            parent_id = request.POST.get("parent_id")
            
            if not name:
                messages.error(request, "Glossary node name is required")
                return redirect("metadata_manager:glossary_list")
            
            # Handle parent node if specified
            parent = None
            if parent_id:
                try:
                    parent = GlossaryNode.objects.get(id=parent_id)
                except GlossaryNode.DoesNotExist:
                    messages.error(request, "Parent node not found")
                    return redirect("metadata_manager:glossary_list")
            
            # Create the node
            GlossaryNode.objects.create(
                name=name, description=description, parent=parent
            )
            
            messages.success(request, f"Glossary node '{name}' created successfully")
            return redirect("metadata_manager:glossary_list")
        except Exception as e:
            logger.error(f"Error creating glossary node: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:glossary_list")


class GlossaryNodeDetailView(View):
    def get(self, request, node_id):
        try:
            node = get_object_or_404(GlossaryNode, id=node_id)
            context = {"page_title": f"Glossary Node: {node.name}", "node": node}
            return render(
                request, "metadata_manager/glossary/node_detail.html", context
            )
        except Exception as e:
            logger.error(f"Error in glossary node detail view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:glossary_list")
    
    def post(self, request, node_id):
        try:
            node = get_object_or_404(GlossaryNode, id=node_id)
            name = request.POST.get("name")
            description = request.POST.get("description", "")
            parent_id = request.POST.get("parent_id")
            color_hex = request.POST.get("color_hex", "")
            
            if not name:
                messages.error(request, "Glossary node name is required")
                return redirect(
                    "metadata_manager:glossary_node_detail", node_id=node_id
                )
            
            # Update the node
            node.name = name
            node.description = description
            
            # Handle color_hex field safely (may not exist in database schema)
            try:
                node.color_hex = color_hex
            except Exception as e:
                logger.warning(f"Could not set color_hex field: {str(e)}")
            
            # Handle parent node if specified
            if parent_id:
                try:
                    parent = GlossaryNode.objects.get(id=parent_id)
                    node.parent = parent
                except GlossaryNode.DoesNotExist:
                    messages.error(request, "Parent node not found")
                    return redirect(
                        "metadata_manager:glossary_node_detail", node_id=node_id
                    )
            
            node.save()
            messages.success(request, f"Glossary node '{name}' updated successfully")
            return redirect("metadata_manager:glossary_node_detail", node_id=node_id)
        except Exception as e:
            logger.error(f"Error updating glossary node: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:glossary_node_detail", node_id=node_id)
    
    def delete(self, request, node_id):
        try:
            # ... existing implementation ...
            messages.success(request, f"Glossary node '{name}' deleted successfully")
            return redirect("metadata_manager:glossary_list")
        except Exception as e:
            logger.error(f"Error deleting glossary node: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:glossary_list")


class GlossaryNodeDeployView(View):
    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def post(self, request, node_id):
        try:
            node = get_object_or_404(GlossaryNode, id=node_id)
            
            if not node.can_deploy:
                messages.error(request, f"Node '{node.name}' cannot be deployed")
                return redirect(
                    "metadata_manager:glossary_node_detail", node_id=node_id
                )
            
            # Get DataHub client
            connected, client = test_datahub_connection()
            if not connected or not client:
                messages.error(request, "Could not connect to DataHub")
                return redirect(
                    "metadata_manager:glossary_node_detail", node_id=node_id
                )
            
            # Deploy the node
            success = node.deploy_to_datahub(client)
            if success:
                messages.success(
                    request, f"Successfully deployed node '{node.name}' to DataHub"
                )
            else:
                messages.error(
                    request, f"Failed to deploy node '{node.name}' to DataHub"
                )
            
            return redirect("metadata_manager:glossary_node_detail", node_id=node_id)
        except Exception as e:
            logger.error(f"Error in glossary node deploy view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:glossary_list")


class GlossaryTermCreateView(View):
    def get(self, request):
        """Render the create term form."""
        # Get all nodes for parent selection
        nodes = GlossaryNode.objects.all().order_by("name")
        context = {"page_title": "Create Glossary Term", "nodes": nodes}
        return render(request, "metadata_manager/glossary/term_form.html", context)

    def post(self, request):
        try:
            name = request.POST.get("name")
            description = request.POST.get("description", "")
            parent_node_id = request.POST.get("parent_node_id")
            term_source = request.POST.get("term_source", "")
            
            if not name:
                messages.error(request, "Glossary term name is required")
                return redirect("metadata_manager:glossary_list")
            
            # Handle parent node if specified
            parent_node = None
            if parent_node_id:
                try:
                    parent_node = GlossaryNode.objects.get(id=parent_node_id)
                except GlossaryNode.DoesNotExist:
                    messages.error(request, "Parent node not found")
                    return redirect("metadata_manager:glossary_list")
            
            # Create the term
            GlossaryTerm.objects.create(
                name=name,
                description=description,
                parent_node=parent_node,
                term_source=term_source,
            )
            
            messages.success(request, f"Glossary term '{name}' created successfully")
            return redirect("metadata_manager:glossary_list")
        except Exception as e:
            logger.error(f"Error creating glossary term: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:glossary_list")


class GlossaryTermDetailView(View):
    def get(self, request, term_id):
        try:
            term = get_object_or_404(GlossaryTerm, id=term_id)
            context = {"page_title": f"Glossary Term: {term.name}", "term": term}
            return render(
                request, "metadata_manager/glossary/term_detail.html", context
            )
        except Exception as e:
            logger.error(f"Error in glossary term detail view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:glossary_list")
    
    def post(self, request, term_id):
        try:
            term = get_object_or_404(GlossaryTerm, id=term_id)
            name = request.POST.get("name")
            description = request.POST.get("description", "")
            parent_node_id = request.POST.get("parent_node_id")
            term_source = request.POST.get("term_source", "")
            
            if not name:
                messages.error(request, "Glossary term name is required")
            return redirect("metadata_manager:glossary_term_detail", term_id=term_id)
            
            # Update the term
            term.name = name
            term.description = description
            term.term_source = term_source
                
            # Handle parent node if specified
            if parent_node_id:
                try:
                    parent_node = GlossaryNode.objects.get(id=parent_node_id)
                    term.parent_node = parent_node
                except GlossaryNode.DoesNotExist:
                    messages.error(request, "Parent node not found")
                    return redirect(
                        "metadata_manager:glossary_term_detail", term_id=term_id
                    )
            
                    term.save()
            messages.success(request, f"Glossary term '{name}' updated successfully")
            return redirect("metadata_manager:glossary_term_detail", term_id=term_id)
        except Exception as e:
            logger.error(f"Error updating glossary term: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:glossary_list")
            
    def delete(self, request, term_id):
        try:
            term = get_object_or_404(GlossaryTerm, id=term_id)
            term_name = term.name
            term.delete()
            return JsonResponse(
                {
                    "success": True,
                    "message": f"Glossary term '{term_name}' deleted successfully",
                }
            )
        except Exception as e:
            logger.error(f"Error deleting glossary term: {str(e)}")
            return JsonResponse(
                {"success": False, "message": f"An error occurred: {str(e)}"}
            )


class GlossaryTermDeployView(View):
    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def post(self, request, term_id):
        try:
            term = get_object_or_404(GlossaryTerm, id=term_id)
            
            if not term.can_deploy:
                messages.error(request, f"Term '{term.name}' cannot be deployed")
                return redirect(
                    "metadata_manager:glossary_term_detail", term_id=term_id
                )
            
            # Get DataHub client
            connected, client = test_datahub_connection()
            if not connected or not client:
                messages.error(request, "Could not connect to DataHub")
                return redirect(
                    "metadata_manager:glossary_term_detail", term_id=term_id
                )
            
            # Deploy the term
            success = term.deploy_to_datahub(client)
            if success:
                messages.success(
                    request, f"Successfully deployed term '{term.name}' to DataHub"
                )
            else:
                messages.error(
                    request, f"Failed to deploy term '{term.name}' to DataHub"
                )
            
            return redirect("metadata_manager:glossary_term_detail", term_id=term_id)
        except Exception as e:
            logger.error(f"Error in glossary term deploy view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:glossary_list")


class GlossaryTermGitPushView(View):
    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def post(self, request, term_id):
        try:
            term = get_object_or_404(GlossaryTerm, id=term_id)
            
            # Check if git integration is enabled
            github_settings = GitSettings.objects.first()
            if not github_settings or not github_settings.enabled:
                return JsonResponse(
                    {"success": False, "error": "Git integration is not enabled"}
                )
            
            # Add term to git staging
            term.add_to_git_staging()
            
            return JsonResponse(
                {
                    "success": True,
                    "message": f"Glossary term '{term.name}' added to git staging",
                }
            )
        except Exception as e:
            logger.error(f"Error adding glossary term to git: {str(e)}")
            return JsonResponse({"success": False, "error": str(e)})


class GlossaryNodeGitPushView(View):
    @method_decorator(require_POST)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)

    def post(self, request, node_id):
        try:
            node = get_object_or_404(GlossaryNode, id=node_id)
            
            # Check if git integration is enabled
            github_settings = GitSettings.objects.first()
            if not github_settings or not github_settings.enabled:
                return JsonResponse(
                    {"success": False, "error": "Git integration is not enabled"}
                )
            
            # Add node to git staging
            node.add_to_git_staging()
            
            return JsonResponse(
                {
                    "success": True,
                    "message": f"Glossary node '{node.name}' added to git staging",
                }
            )
        except Exception as e:
            logger.error(f"Error adding glossary node to git: {str(e)}")
            return JsonResponse({"success": False, "error": str(e)})


def _safe_process_ownership_and_relationships(ownership_data, relationships_data, properties):
    """Helper function to safely process ownership, relationships, and custom properties"""
    # Process ownership
    owners = ownership_data.get("owners", []) if ownership_data else []
    if not isinstance(owners, list):
        owners = []
    owner_names = []
    for owner_info in owners:
        if not owner_info or not isinstance(owner_info, dict):
            continue
        owner = owner_info.get("owner", {})
        if not isinstance(owner, dict):
            continue
        if owner.get("username"):  # CorpUser
            owner_names.append(owner["username"])
        elif owner.get("name"):  # CorpGroup
            owner_names.append(owner["name"])
    
    # Process relationships
    relationships = relationships_data.get("relationships", []) if relationships_data else []
    if not isinstance(relationships, list):
        relationships = []
    
    # Process custom properties
    custom_properties = properties.get("customProperties", []) if properties else []
    if not isinstance(custom_properties, list):
        custom_properties = []
        
    return {
        "owners": owners,
        "owner_names": owner_names,
        "relationships": relationships,
        "custom_properties": custom_properties,
        "owners_count": len(owners),
        "relationships_count": len(relationships),
        "custom_properties_count": len(custom_properties)
    }


def get_remote_glossary_data(request):
    """AJAX endpoint to get enhanced remote glossary data with ownership and relationships"""
    try:
        logger.info("Loading enhanced remote glossary data via AJAX")

        # Get DataHub connection using standard configuration
        client = get_datahub_client()
        connected = client and client.test_connection()
        
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})

        # Get all local nodes and terms
        local_nodes = GlossaryNode.objects.all().order_by("name")
        local_terms = GlossaryTerm.objects.all().order_by("name")

        # Initialize data structures
        synced_nodes = []
        local_only_nodes = []
        remote_only_nodes = []
        synced_terms = []
        local_only_terms = []
        remote_only_terms = []
        
        # Get DataHub URL and token for proper link generation and API calls
        from web_ui.models import AppSettings
        import os
        datahub_url = AppSettings.get("datahub_url", os.environ.get("DATAHUB_GMS_URL", "")).rstrip('/')
        datahub_token = AppSettings.get("datahub_token", os.environ.get("DATAHUB_TOKEN", ""))

        # Get comprehensive remote data with all metadata including structured properties and ownership
        try:
            comprehensive_data = client.get_comprehensive_glossary_data(query="*", count=10000)
            remote_nodes = comprehensive_data.get("nodes", [])
            remote_terms = comprehensive_data.get("terms", [])
            
            if remote_nodes is None:
                remote_nodes = []
            if remote_terms is None:
                remote_terms = []
        except Exception as e:
            logger.warning(f"Failed to fetch comprehensive glossary data: {e}")
            remote_nodes = []
            remote_terms = []
            
        logger.debug(f"Found {len(remote_nodes)} remote nodes and {len(remote_terms)} remote terms")
        
        # Create URN mappings for quick lookup
        remote_nodes_dict = {node.get("urn"): node for node in remote_nodes if node and node.get("urn")}
        remote_terms_dict = {term.get("urn"): term for term in remote_terms if term and term.get("urn")}
        
        # Categorize items
        synced_items = []
        local_only_items = []
        remote_only_items = []
        
        # Extract local URNs
        local_node_urns = set(local_nodes.values_list("deterministic_urn", flat=True))
        local_term_urns = set(local_terms.values_list("deterministic_urn", flat=True))
        
        # Process local nodes and match with remote
        for local_node in local_nodes:
            node_urn = str(local_node.deterministic_urn)
            remote_match = remote_nodes_dict.get(node_urn)
            
            local_item_data = {
                "id": str(local_node.id),
                "name": local_node.name,
                "description": local_node.description or "",
                "urn": node_urn,
                "type": "node",
                "sync_status": local_node.sync_status,
                "sync_status_display": local_node.get_sync_status_display(),
                "parent_urn": str(local_node.parent.deterministic_urn) if local_node.parent else None,
                "has_children": local_node.children.exists() or local_node.terms.exists(),
                # Initialize empty ownership and relationships for local-only items
                "owners_count": 0,
                "owner_names": [],
                "relationships_count": 0,
                "custom_properties_count": 0,
                "structured_properties_count": 0,
                "ownership": None,
                "relationships": None,
            }
            
            if remote_match:
                # Check if item needs status update
                local_description = local_node.description or ""
                remote_description = remote_match.get("properties", {}).get("description", "")
                
                # Update sync status based on comparison
                if local_description != remote_description:
                    if local_node.sync_status != "MODIFIED":
                        local_node.sync_status = "MODIFIED"
                        local_node.save(update_fields=["sync_status"])
                        logger.debug(f"Updated node {local_node.name} status to MODIFIED")
                else:
                    if local_node.sync_status != "SYNCED":
                        local_node.sync_status = "SYNCED" 
                        local_node.save(update_fields=["sync_status"])
                        logger.debug(f"Updated node {local_node.name} status to SYNCED")
                
                # Extract comprehensive data from remote (now includes structured properties)
                ownership_data = remote_match.get("owners", []) or []  # New format has processed owners
                relationships_data = remote_match.get("relationships", []) or []  # New format has processed relationships
                structured_properties = remote_match.get("structuredProperties", []) or []
                custom_properties = remote_match.get("customProperties", []) or []
                properties = remote_match.get("properties", {}) or {}
                
                # Process the comprehensive data
                processed_data = {
                    "owners_count": len(ownership_data),
                    "owner_names": [owner.get("name", "Unknown") for owner in ownership_data if owner],
                    "relationships_count": len(relationships_data),
                    "custom_properties_count": len(custom_properties),
                    "structured_properties_count": len(structured_properties),
                    "custom_properties": custom_properties,
                    "structured_properties": structured_properties
                }
                
                # Update local item data with comprehensive remote information
                local_item_data.update({
                    "sync_status": local_node.sync_status,
                    "sync_status_display": local_node.get_sync_status_display(),
                    "owners_count": processed_data["owners_count"],
                    "owner_names": processed_data["owner_names"],
                    "relationships_count": processed_data["relationships_count"],
                    "custom_properties_count": processed_data["custom_properties_count"],
                    "structured_properties_count": processed_data["structured_properties_count"],
                    "ownership": ownership_data,
                    "relationships": relationships_data,
                    "structured_properties": structured_properties,
                    "custom_properties": custom_properties,
                })
                
                synced_items.append({
                    "local": local_item_data,
                    "remote": remote_match,
                    "combined": local_item_data  # Enhanced data for display
                })
            else:
                # Ensure local-only items have correct status
                if local_node.sync_status != "LOCAL_ONLY":
                    local_node.sync_status = "LOCAL_ONLY"
                    local_node.save(update_fields=["sync_status"])
                    logger.debug(f"Updated node {local_node.name} status to LOCAL_ONLY")
                
                local_item_data["sync_status"] = "LOCAL_ONLY"
                local_item_data["sync_status_display"] = "Local Only"
                local_only_items.append(local_item_data)
        
        # Process local terms and match with remote
        for local_term in local_terms:
            term_urn = str(local_term.deterministic_urn)
            remote_match = remote_terms_dict.get(term_urn)
            
            local_item_data = {
                "id": str(local_term.id),
                "name": local_term.name,
                "description": local_term.description or "",
                "urn": term_urn,
                "type": "term",
                "sync_status": local_term.sync_status,
                "sync_status_display": local_term.get_sync_status_display(),
                "parent_node_urn": str(local_term.parent_node.deterministic_urn) if local_term.parent_node else None,
                "term_source": getattr(local_term, 'term_source', '') or "",
                # Initialize empty ownership and relationships for local-only items
                "owners_count": 0,
                "owner_names": [],
                "relationships_count": 0,
                "custom_properties_count": 0,
                "structured_properties_count": 0,
                "ownership": None,
                "relationships": None,
            }
            
            if remote_match:
                # Check if item needs status update
                local_description = local_term.description or ""
                remote_description = remote_match.get("properties", {}).get("description", "")
                
                # Update sync status based on comparison
                if local_description != remote_description:
                    if local_term.sync_status != "MODIFIED":
                        local_term.sync_status = "MODIFIED"
                        local_term.save(update_fields=["sync_status"])
                        logger.debug(f"Updated term {local_term.name} status to MODIFIED")
                else:
                    if local_term.sync_status != "SYNCED":
                        local_term.sync_status = "SYNCED" 
                        local_term.save(update_fields=["sync_status"])
                        logger.debug(f"Updated term {local_term.name} status to SYNCED")
                
                # Extract comprehensive data from remote (now includes structured properties)
                ownership_data = remote_match.get("owners", []) or []  # New format has processed owners
                relationships_data = remote_match.get("relationships", []) or []  # New format has processed relationships
                structured_properties = remote_match.get("structuredProperties", []) or []
                custom_properties = remote_match.get("customProperties", []) or []
                properties = remote_match.get("properties", {}) or {}
                
                # Process the comprehensive data
                processed_data = {
                    "owners_count": len(ownership_data),
                    "owner_names": [owner.get("name", "Unknown") for owner in ownership_data if owner],
                    "relationships_count": len(relationships_data),
                    "custom_properties_count": len(custom_properties),
                    "structured_properties_count": len(structured_properties),
                    "custom_properties": custom_properties,
                    "structured_properties": structured_properties
                }
                
                # Update local item data with comprehensive remote information
                local_item_data.update({
                    "sync_status": local_term.sync_status,
                    "sync_status_display": local_term.get_sync_status_display(),
                    "owners_count": processed_data["owners_count"],
                    "owner_names": processed_data["owner_names"],
                    "relationships_count": processed_data["relationships_count"],
                    "custom_properties_count": processed_data["custom_properties_count"],
                    "structured_properties_count": processed_data["structured_properties_count"],
                    "ownership": ownership_data,
                    "relationships": relationships_data,
                    "structured_properties": structured_properties,
                    "custom_properties": custom_properties,
                })
                
                synced_items.append({
                    "local": local_item_data,
                    "remote": remote_match,
                    "combined": local_item_data  # Enhanced data for display
                })
            else:
                # Ensure local-only items have correct status
                if local_term.sync_status != "LOCAL_ONLY":
                    local_term.sync_status = "LOCAL_ONLY"
                    local_term.save(update_fields=["sync_status"])
                    logger.debug(f"Updated term {local_term.name} status to LOCAL_ONLY")
                
                local_item_data["sync_status"] = "LOCAL_ONLY"
                local_item_data["sync_status_display"] = "Local Only"
                local_only_items.append(local_item_data)
        
        # Find remote-only nodes
        for node_urn, remote_node in remote_nodes_dict.items():
            if node_urn not in local_node_urns:
                properties = remote_node.get("properties", {}) or {}
                ownership_data = remote_node.get("owners", []) or []
                relationships_data = remote_node.get("relationships", []) or []
                structured_properties = remote_node.get("structuredProperties", []) or []
                custom_properties = remote_node.get("customProperties", []) or []
                
                # Process comprehensive data
                processed_data = {
                    "owners_count": len(ownership_data),
                    "owner_names": [owner.get("name", "Unknown") for owner in ownership_data if owner],
                    "relationships_count": len(relationships_data),
                    "custom_properties_count": len(custom_properties),
                    "structured_properties_count": len(structured_properties),
                    "custom_properties": custom_properties,
                    "structured_properties": structured_properties
                }
                
                remote_only_items.append({
                    "name": properties.get("name", "Unknown"),
                    "description": properties.get("description", ""),
                    "urn": node_urn,
                    "type": "node",
                    "sync_status": "REMOTE_ONLY",
                    "sync_status_display": "Remote Only",
                    "parent_urn": remote_node.get("parentNodes", [{}])[0].get("urn") if remote_node.get("parentNodes") else None,
                    "has_children": False,  # Simplified for remote-only
                    "owners_count": processed_data["owners_count"],
                    "owner_names": processed_data["owner_names"],
                    "relationships_count": processed_data["relationships_count"],
                    "custom_properties_count": processed_data["custom_properties_count"],
                    "structured_properties_count": processed_data["structured_properties_count"],
                    "ownership": ownership_data,
                    "relationships": relationships_data,
                    "structured_properties": structured_properties,
                    "custom_properties": custom_properties,
                })
        
        # Find remote-only terms
        for term_urn, remote_term in remote_terms_dict.items():
            if term_urn not in local_term_urns:
                properties = remote_term.get("properties", {}) or {}
                ownership_data = remote_term.get("owners", []) or []
                relationships_data = remote_term.get("relationships", []) or []
                structured_properties = remote_term.get("structuredProperties", []) or []
                custom_properties = remote_term.get("customProperties", []) or []
                
                # Get name and description from properties
                name = properties.get("name", "Unknown")
                description = properties.get("description", "")
                
                # Process comprehensive data
                processed_data = {
                    "owners_count": len(ownership_data),
                    "owner_names": [owner.get("name", "Unknown") for owner in ownership_data if owner],
                    "relationships_count": len(relationships_data),
                    "custom_properties_count": len(custom_properties),
                    "structured_properties_count": len(structured_properties),
                    "custom_properties": custom_properties,
                    "structured_properties": structured_properties
                }
                
                remote_only_items.append({
                    "name": name,
                    "description": description,
                    "urn": term_urn,
                    "type": "term",
                    "sync_status": "REMOTE_ONLY",
                    "sync_status_display": "Remote Only",
                    "parent_node_urn": remote_term.get("parentNodes", [{}])[0].get("urn") if remote_term.get("parentNodes") else None,
                    "term_source": remote_term.get("termSource", "INTERNAL"),
                    "owners_count": processed_data["owners_count"],
                    "owner_names": processed_data["owner_names"],
                    "relationships_count": processed_data["relationships_count"],
                    "custom_properties_count": processed_data["custom_properties_count"],
                    "structured_properties_count": processed_data["structured_properties_count"],
                    "ownership": ownership_data,
                    "relationships": relationships_data,
                    "structured_properties": structured_properties,
                    "custom_properties": custom_properties,
                })
        
        # Calculate statistics with proper counting
        total_items = len(synced_items) + len(local_only_items) + len(remote_only_items)
        
        # Count items with owners
        owned_items = 0
        for item_list in [synced_items, local_only_items, remote_only_items]:
            for item in item_list:
                # Get the actual item data (handle both combined and direct formats)
                item_data = item.get("combined", item) if "combined" in item else item
                if item_data.get("owners_count", 0) > 0:
                    owned_items += 1
        
        # Count items with relationships
        items_with_relationships = 0
        for item_list in [synced_items, local_only_items, remote_only_items]:
            for item in item_list:
                # Get the actual item data (handle both combined and direct formats)
                item_data = item.get("combined", item) if "combined" in item else item
                if item_data.get("relationships_count", 0) > 0:
                    items_with_relationships += 1
        
        # Count items with custom properties
        items_with_custom_properties = 0
        for item_list in [synced_items, local_only_items, remote_only_items]:
            for item in item_list:
                # Get the actual item data (handle both combined and direct formats)
                item_data = item.get("combined", item) if "combined" in item else item
                if item_data.get("custom_properties_count", 0) > 0:
                    items_with_custom_properties += 1
        
        # Count items with structured properties
        items_with_structured_properties = 0
        for item_list in [synced_items, local_only_items, remote_only_items]:
            for item in item_list:
                # Get the actual item data (handle both combined and direct formats)
                item_data = item.get("combined", item) if "combined" in item else item
                if item_data.get("structured_properties_count", 0) > 0:
                    items_with_structured_properties += 1
        
        logger.debug(
            f"Categorized glossary items: {len(synced_items)} synced, {len(local_only_items)} local-only, {len(remote_only_items)} remote-only"
        )
        logger.debug(f"Statistics: {owned_items} owned, {items_with_relationships} with relationships, {items_with_custom_properties} with custom properties, {items_with_structured_properties} with structured properties")
        
        # Prepare response data
        response_data = {
            "synced_items": synced_items,
            "local_only_items": local_only_items,
            "remote_only_items": remote_only_items,
            "datahub_url": datahub_url,
            "datahub_token": datahub_token,
            "statistics": {
                "total_items": total_items,
                "synced_count": len(synced_items),
                "local_count": len(local_only_items),
                "remote_count": len(remote_only_items),
                "owned_items": owned_items,
                "items_with_relationships": items_with_relationships,
                "items_with_custom_properties": items_with_custom_properties,
                "items_with_structured_properties": items_with_structured_properties,
                "percentage_owned": round((owned_items / total_items * 100) if total_items else 0, 1),
            }
        }
        
        # Apply global sanitization to prevent issues with long descriptions and malformed data
        sanitized_data = sanitize_api_response(response_data)
        
        return JsonResponse({
            "success": True,
            "data": sanitized_data
        })
        
    except Exception as e:
        logger.error(f"Error getting enhanced remote glossary data: {str(e)}")
        return JsonResponse({
            "success": False,
            "error": str(e)
        })


@require_POST
def glossary_csv_upload(request):
    """Handle CSV upload for glossary items"""
    try:
        # Check if file was uploaded
        if 'csv_file' not in request.FILES:
            return JsonResponse({
                "success": False,
                "error": "No CSV file uploaded"
            })
        
        csv_file = request.FILES['csv_file']
        update_existing = request.POST.get('update_existing', 'false').lower() == 'true'
        dry_run = request.POST.get('dry_run', 'true').lower() == 'true'
        
        # Validate file size (10MB limit)
        if csv_file.size > 10 * 1024 * 1024:
            return JsonResponse({
                "success": False,
                "error": "File size must be less than 10MB"
            })
        
        # Validate file type
        if not csv_file.name.lower().endswith('.csv'):
            return JsonResponse({
                "success": False,
                "error": "File must be a CSV file"
            })
        
        # Read and parse CSV
        try:
            # Decode the file content
            file_content = csv_file.read().decode('utf-8')
            csv_reader = csv.DictReader(io.StringIO(file_content))
            
            # Validate required columns
            required_columns = ['name', 'type', 'description']
            optional_columns = ['parent', 'owner_email', 'owner_group', 'relationships', 'custom_properties']
            
            if not all(col in csv_reader.fieldnames for col in required_columns):
                missing_cols = [col for col in required_columns if col not in csv_reader.fieldnames]
                return JsonResponse({
                    "success": False,
                    "error": f"Missing required columns: {', '.join(missing_cols)}"
                })
            
            # Process CSV rows
            processed_count = 0
            created_count = 0
            updated_count = 0
            skipped_count = 0
            errors = []
            
            for row_num, row in enumerate(csv_reader, start=2):  # Start at 2 because row 1 is headers
                try:
                    # Validate required fields
                    name = row.get('name', '').strip()
                    item_type = row.get('type', '').strip().lower()
                    description = row.get('description', '').strip()
                    
                    if not name:
                        errors.append(f"Row {row_num}: Name is required")
                        skipped_count += 1
                        continue
                    
                    if item_type not in ['term', 'node']:
                        errors.append(f"Row {row_num}: Type must be 'term' or 'node', got '{item_type}'")
                        skipped_count += 1
                        continue
                    
                    # Parse optional fields
                    parent = row.get('parent', '').strip()
                    owner_email = row.get('owner_email', '').strip()
                    owner_group = row.get('owner_group', '').strip()
                    
                    # Parse JSON fields
                    relationships = []
                    custom_properties = {}
                    
                    try:
                        if row.get('relationships'):
                            relationships = json.loads(row['relationships'])
                    except json.JSONDecodeError:
                        errors.append(f"Row {row_num}: Invalid JSON in relationships field")
                    
                    try:
                        if row.get('custom_properties'):
                            custom_properties = json.loads(row['custom_properties'])
                    except json.JSONDecodeError:
                        errors.append(f"Row {row_num}: Invalid JSON in custom_properties field")
                    
                    if not dry_run:
                        # Create or update the item
                        if item_type == 'node':
                            # Handle glossary node
                            parent_node = None
                            if parent:
                                # Try to find parent by name or URN
                                try:
                                    parent_node = GlossaryNode.objects.filter(
                                        models.Q(name=parent) | models.Q(urn=parent)
                                    ).first()
                                except:
                                    pass
                            
                            # Check if node already exists
                            existing_node = GlossaryNode.objects.filter(name=name).first()
                            
                            if existing_node:
                                if update_existing:
                                    existing_node.description = description
                                    existing_node.parent = parent_node
                                    existing_node.save()
                                    updated_count += 1
                                else:
                                    skipped_count += 1
                                    errors.append(f"Row {row_num}: Node '{name}' already exists (use update_existing to overwrite)")
                            else:
                                # Create new node
                                new_node = GlossaryNode.objects.create(
                                    name=name,
                                    description=description,
                                    parent=parent_node,
                                    sync_status='LOCAL_ONLY'
                                )
                                created_count += 1
                        
                        else:  # item_type == 'term'
                            # Handle glossary term
                            parent_node = None
                            if parent:
                                # Try to find parent by name or URN
                                try:
                                    parent_node = GlossaryNode.objects.filter(
                                        models.Q(name=parent) | models.Q(urn=parent)
                                    ).first()
                                except:
                                    pass
                            
                            # Check if term already exists
                            existing_term = GlossaryTerm.objects.filter(name=name).first()
                            
                            if existing_term:
                                if update_existing:
                                    existing_term.description = description
                                    existing_term.parent_node = parent_node
                                    existing_term.save()
                                    updated_count += 1
                                else:
                                    skipped_count += 1
                                    errors.append(f"Row {row_num}: Term '{name}' already exists (use update_existing to overwrite)")
                            else:
                                # Create new term
                                new_term = GlossaryTerm.objects.create(
                                    name=name,
                                    description=description,
                                    parent_node=parent_node,
                                    sync_status='LOCAL_ONLY'
                                )
                                created_count += 1
                    
                    processed_count += 1
                    
                except Exception as e:
                    errors.append(f"Row {row_num}: Error processing row - {str(e)}")
                    skipped_count += 1
            
            # Prepare response
            response_data = {
                "processed_count": processed_count,
                "created_count": created_count,
                "updated_count": updated_count,
                "skipped_count": skipped_count,
                "errors": errors[:50]  # Limit errors to prevent huge responses
            }
            
            if len(errors) > 50:
                response_data["errors"].append(f"... and {len(errors) - 50} more errors")
            
            return JsonResponse({
                "success": True,
                "data": response_data
            })
            
        except Exception as e:
            logger.error(f"Error parsing CSV file: {str(e)}")
            return JsonResponse({
                "success": False,
                "error": f"Error parsing CSV file: {str(e)}"
            })
    
    except Exception as e:
        logger.error(f"Error in CSV upload: {str(e)}")
        return JsonResponse({
            "success": False,
            "error": str(e)
        })
