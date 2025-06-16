from django import template
import json
from datetime import datetime

register = template.Library()


@register.filter
def pretty_json(value):
    """Format JSON for human readability"""
    if value:
        try:
            if isinstance(value, str):
                parsed = json.loads(value)
            else:
                parsed = value
            return json.dumps(parsed, indent=2, sort_keys=True)
        except Exception:
            return value
    return ""


@register.filter
def short_date(value):
    """Format date to MM/DD/YY"""
    if value:
        try:
            if isinstance(value, str):
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return value.strftime("%m/%d/%y")
        except Exception:
            return value
    return ""


@register.filter
def get_item(dictionary, key):
    """Get a value from a dictionary by key"""
    import logging

    logger = logging.getLogger(__name__)

    if dictionary is None:
        logger.debug(f"get_item: dictionary is None, returning None for key {key}")
        return None

    # Ensure key is a string for dictionary lookup
    str_key = str(key)

    # For regular dictionaries
    if isinstance(dictionary, dict):
        # Try with the key as is
        if key in dictionary:
            return dictionary[key]
        # Try with string conversion of the key
        if str_key in dictionary:
            return dictionary[str_key]
        logger.debug(
            f"get_item: key {key} not found in dictionary keys: {list(dictionary.keys())}"
        )
        return []

    # For objects with a get method
    if hasattr(dictionary, "get") and callable(dictionary.get):
        # Try with key as is
        result = dictionary.get(key, None)
        if result is not None:
            return result
        # Try with string conversion
        result = dictionary.get(str_key, [])
        return result

    # For objects with attributes
    if hasattr(dictionary, key):
        return getattr(dictionary, key)

    # If all else fails
    logger.debug(f"get_item: key {key} not found in {type(dictionary)}")
    return []


@register.filter
def safe_get_terms(node):
    """Safely get terms from a node, handling both dictionaries and model instances"""
    try:
        # For dictionary-like objects
        if hasattr(node, "get") and callable(node.get):
            return node.get("terms", [])
        # For Django model instances
        elif hasattr(node, "terms"):
            # Check if it's a manager
            if hasattr(node.terms, "all") and callable(node.terms.all):
                return list(node.terms.all())
            return node.terms
        return []
    except Exception:
        return []


@register.inclusion_tag(
    "metadata_manager/glossary/includes/node_hierarchy.html", takes_context=True
)
def render_node_hierarchy(context, node, node_type="remote", level=1, terms_dict=None):
    """Render a node and its children recursively

    Args:
        context: The template context
        node: The node to render
        node_type: The type of node (remote, local, synced, modified)
        level: The nesting level (for indentation)
        terms_dict: Dictionary of terms indexed by parent node URN
    """
    # Initialize variables
    children = []
    has_terms = False
    node_terms = []

    try:
        # Check if node is a dictionary-like object or a model instance
        if hasattr(node, "get") and callable(node.get):
            # Dictionary-like object
            children = node.get("children", [])
        elif hasattr(node, "children"):
            # Django model with direct children access
            if hasattr(node.children, "all") and callable(node.children.all):
                # It's a RelatedManager, call .all()
                children = list(node.children.all())
            else:
                # It's a regular attribute
                children = node.children
        elif hasattr(node, "child_nodes"):
            # Django model with child_nodes relationship
            if hasattr(node.child_nodes, "all") and callable(node.child_nodes.all):
                # It's a RelatedManager, call .all()
                children = list(node.child_nodes.all())
            else:
                children = node.child_nodes
    except Exception as e:
        # Log the error but continue
        import logging

        logger = logging.getLogger(__name__)
        logger.error(f"Error getting children for node {node}: {str(e)}")
        children = []

    try:
        # Handle terms properly
        # For dictionary-like objects
        if hasattr(node, "get") and callable(node.get):
            node_terms = node.get("terms", [])
            has_terms = bool(node_terms)
        # For Django model instances with a terms relation
        elif hasattr(node, "terms"):
            # Check if it's a manager (Django ORM relationship)
            if hasattr(node.terms, "all") and callable(node.terms.all):
                node_terms = list(node.terms.all())
                has_terms = bool(node_terms)

        # Check if this node has terms in the terms_dict
        if not has_terms and terms_dict and hasattr(node, "urn"):
            node_urn = node.urn
            has_terms = node_urn in terms_dict and bool(terms_dict[node_urn])
    except Exception as e:
        # Log the error but continue
        import logging

        logger = logging.getLogger(__name__)
        logger.error(f"Error getting terms for node {node}: {str(e)}")
        node_terms = []
        has_terms = False

    # Create new context with the variables we need
    result = {
        "node": node,
        "node_type": node_type,
        "level": level,
        "terms_dict": terms_dict,
        "children": children,
        "has_terms": has_terms,
        "node_terms": node_terms,
    }

    # Add connection status variables from the parent context if available
    result["has_datahub_connection"] = context.get("has_datahub_connection", False)
    result["has_git_integration"] = context.get("has_git_integration", False)

    return result


@register.filter
def debug(value):
    """Print the structure of an object for debugging"""
    import json

    try:
        if isinstance(value, dict):
            # Handle dictionary-like objects
            return json.dumps(value, indent=2, default=str)
        elif hasattr(value, "__dict__"):
            # Handle objects with __dict__ attribute
            return json.dumps(value.__dict__, indent=2, default=str)
        else:
            # Try to convert to string
            return str(value)
    except Exception as e:
        return f"Error debugging object: {str(e)}"


@register.filter
def node_to_json(node):
    """Convert a GlossaryNode model instance to JSON for JavaScript use"""
    try:
        return json.dumps(
            {
                "id": node.id,
                "name": node.name,
                "description": node.description or "",
                "urn": str(node.deterministic_urn),
                "sync_status": node.sync_status,
                "parent_urn": str(node.parent.deterministic_urn)
                if node.parent
                else None,
                "has_children": node.children.exists() or node.terms.exists(),
                "related_items": [],  # Local nodes don't have relationship data by default
                "owners": [],  # Local nodes don't have owner data loaded by default
                "custom_properties": [],
            },
            default=str,
        )
    except Exception as e:
        import logging

        logger = logging.getLogger(__name__)
        logger.error(f"Error converting node to JSON: {str(e)}")
        return json.dumps({})


@register.filter
def term_to_json(term):
    """Convert a GlossaryTerm model instance to JSON for JavaScript use"""
    try:
        return json.dumps(
            {
                "id": term.id,
                "name": term.name,
                "description": term.description or "",
                "urn": str(term.deterministic_urn),
                "sync_status": term.sync_status,
                "parent_node_urn": str(term.parent_node.deterministic_urn)
                if term.parent_node
                else None,
                "related_items": [],  # Local terms don't have relationship data by default
                "owners": [],  # Local terms don't have owner data loaded by default
                "domain": None,  # Local terms don't have domain data loaded by default
                "term_source": term.term_source,
                "source_ref": getattr(term, "source_ref", None),
                "source_url": getattr(term, "source_url", None),
                "custom_properties": [],
                "deprecated": getattr(term, "deprecated", False),
            },
            default=str,
        )
    except Exception as e:
        import logging

        logger = logging.getLogger(__name__)
        logger.error(f"Error converting term to JSON: {str(e)}")
        return json.dumps({})


@register.filter
def datahub_url(base_url, urn):
    """Create a proper DataHub URL without double slashes and without encoding URN colons"""
    if not base_url or not urn:
        return "#"
    
    # Remove trailing slashes from base URL
    clean_base_url = base_url.rstrip('/')
    
    # Don't encode the URN - DataHub expects colons to remain as colons
    return f"{clean_base_url}/tag/{urn}"


@register.filter  
def datahub_entity_url(base_url, entity_info):
    """Create a proper DataHub URL for entities without double slashes and without encoding URN colons"""
    if not base_url or not entity_info:
        return "#"
    
    # Handle both dictionary and object formats
    if isinstance(entity_info, dict):
        entity_type = entity_info.get('type', '').lower()
        entity_urn = entity_info.get('urn', '')
    else:
        entity_type = getattr(entity_info, 'type', '').lower()
        entity_urn = getattr(entity_info, 'urn', '')
    
    if not entity_type or not entity_urn:
        return "#"
    
    # Remove trailing slashes from base URL
    clean_base_url = base_url.rstrip('/')
    
    # Don't encode the URN - DataHub expects colons to remain as colons
    return f"{clean_base_url}/{entity_type}/{entity_urn}"


@register.filter
def datahub_domain_url(base_url, urn):
    """Create a proper DataHub URL for domains without double slashes and without encoding URN colons"""
    if not base_url or not urn:
        return "#"
    
    # Remove trailing slashes from base URL
    clean_base_url = base_url.rstrip('/')
    
    # Don't encode the URN - DataHub expects colons to remain as colons
    return f"{clean_base_url}/domain/{urn}"


@register.filter
def datahub_glossary_term_url(base_url, urn):
    """Create a proper DataHub URL for glossary terms without double slashes and without encoding URN colons"""
    if not base_url or not urn:
        return "#"
    
    # Remove trailing slashes from base URL
    clean_base_url = base_url.rstrip('/')
    
    # Don't encode the URN - DataHub expects colons to remain as colons
    return f"{clean_base_url}/glossaryTerm/{urn}"
