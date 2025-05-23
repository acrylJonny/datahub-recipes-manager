#!/usr/bin/env python3
"""
Assertions for validating metadata in DataHub.

This module provides functions for asserting conditions about metadata entities,
their relationships, and properties.
"""

import json
import logging
import os
import sys
from typing import Dict, Any, List, Optional, Set, Union, Callable

# Add the parent directory to the sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.datahub_metadata_api import DataHubMetadataApiClient
from utils.urn_utils import (
    generate_deterministic_urn,
    get_full_urn_from_name,
    extract_name_from_properties,
    get_parent_path
)
from scripts.metadata_tests.entity_relationship_utils import (
    get_entities_with_glossary_term,
    get_entities_with_tag,
    get_entity_structured_properties,
    get_entities_with_structured_property
)

logger = logging.getLogger(__name__)


class AssertionResult:
    """Class representing the result of a metadata assertion"""
    
    def __init__(
        self,
        success: bool,
        message: str,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize an assertion result
        
        Args:
            success: Whether the assertion passed
            message: Message describing the assertion result
            details: Additional details about the assertion
        """
        self.success = success
        self.message = message
        self.details = details or {}
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert assertion result to dictionary"""
        return {
            "success": self.success,
            "message": self.message,
            "details": self.details
        }
        
    def __str__(self) -> str:
        """String representation of assertion result"""
        status = "PASS" if self.success else "FAIL"
        return f"[{status}] {self.message}"


def get_deterministic_tag_urn(tag_name: str) -> str:
    """
    Get a deterministic URN for a tag based on its name.
    
    Args:
        tag_name: Name of the tag
        
    Returns:
        Deterministic URN for the tag
    """
    return get_full_urn_from_name("tag", tag_name)


def get_deterministic_glossary_term_urn(term_name: str, parent_urn: Optional[str] = None) -> str:
    """
    Get a deterministic URN for a glossary term based on its name and parent.
    
    Args:
        term_name: Name of the glossary term
        parent_urn: Optional URN of the parent node
        
    Returns:
        Deterministic URN for the glossary term
    """
    return get_full_urn_from_name("glossaryTerm", term_name, parent_urn)


def assert_entity_has_tag(
    client: DataHubMetadataApiClient,
    entity_urn: str,
    tag_urn: str,
    use_deterministic_urns: bool = True
) -> AssertionResult:
    """
    Assert that an entity has a specific tag.
    
    Args:
        client: DataHub metadata client
        entity_urn: URN of the entity
        tag_urn: URN of the tag or tag name (if use_deterministic_urns is True)
        use_deterministic_urns: Whether to generate deterministic URNs for tag names
        
    Returns:
        Assertion result
    """
    # Convert tag name to deterministic URN if needed
    if use_deterministic_urns and not tag_urn.startswith("urn:li:tag:"):
        tag_urn = get_deterministic_tag_urn(tag_urn)
    
    # GraphQL query to check if an entity has a specific tag
    query = """
    query entityHasTag($entityUrn: String!, $tagUrn: String!) {
      entity(urn: $entityUrn) {
        ... on EntityWithTags {
          tags {
            tags {
              tag {
                urn
              }
            }
          }
        }
      }
    }
    """
    
    variables = {
        "entityUrn": entity_urn,
        "tagUrn": tag_urn
    }
    
    try:
        result = client.execute_graphql(query, variables)
        
        # Extract tag URNs from the response
        tag_urns = []
        if (
            result 
            and "data" in result 
            and "entity" in result["data"]
            and result["data"]["entity"]
            and "tags" in result["data"]["entity"]
            and "tags" in result["data"]["entity"]["tags"]
        ):
            for tag_item in result["data"]["entity"]["tags"]["tags"]:
                if "tag" in tag_item and "urn" in tag_item["tag"]:
                    tag_urns.append(tag_item["tag"]["urn"])
        
        # Check if the specified tag URN is in the list
        if tag_urn in tag_urns:
            return AssertionResult(
                True,
                f"Entity {entity_urn} has tag {tag_urn}",
                {"entityUrn": entity_urn, "tagUrn": tag_urn}
            )
        else:
            return AssertionResult(
                False,
                f"Entity {entity_urn} does not have tag {tag_urn}",
                {"entityUrn": entity_urn, "tagUrn": tag_urn, "presentTags": tag_urns}
            )
    
    except Exception as e:
        logger.error(f"Error checking if entity {entity_urn} has tag {tag_urn}: {str(e)}")
        return AssertionResult(
            False,
            f"Error checking if entity {entity_urn} has tag {tag_urn}: {str(e)}",
            {"entityUrn": entity_urn, "tagUrn": tag_urn, "error": str(e)}
        )


def assert_entity_has_glossary_term(
    client: DataHubMetadataApiClient,
    entity_urn: str,
    term_urn: str,
    use_deterministic_urns: bool = True,
    parent_urn: Optional[str] = None
) -> AssertionResult:
    """
    Assert that an entity has a specific glossary term.
    
    Args:
        client: DataHub metadata client
        entity_urn: URN of the entity
        term_urn: URN of the glossary term or term name (if use_deterministic_urns is True)
        use_deterministic_urns: Whether to generate deterministic URNs for term names
        parent_urn: Optional parent node URN (only used if term_urn is a name)
        
    Returns:
        Assertion result
    """
    # Convert term name to deterministic URN if needed
    if use_deterministic_urns and not term_urn.startswith("urn:li:glossaryTerm:"):
        term_urn = get_deterministic_glossary_term_urn(term_urn, parent_urn)
    
    # GraphQL query to check if an entity has a specific glossary term
    query = """
    query entityHasGlossaryTerm($entityUrn: String!, $termUrn: String!) {
      entity(urn: $entityUrn) {
        ... on EntityWithGlossaryTerms {
          glossaryTerms {
            terms {
              term {
                urn
              }
            }
          }
        }
      }
    }
    """
    
    variables = {
        "entityUrn": entity_urn,
        "termUrn": term_urn
    }
    
    try:
        result = client.execute_graphql(query, variables)
        
        # Extract term URNs from the response
        term_urns = []
        if (
            result 
            and "data" in result 
            and "entity" in result["data"]
            and result["data"]["entity"]
            and "glossaryTerms" in result["data"]["entity"]
            and "terms" in result["data"]["entity"]["glossaryTerms"]
        ):
            for term_item in result["data"]["entity"]["glossaryTerms"]["terms"]:
                if "term" in term_item and "urn" in term_item["term"]:
                    term_urns.append(term_item["term"]["urn"])
        
        # Check if the specified term URN is in the list
        if term_urn in term_urns:
            return AssertionResult(
                True,
                f"Entity {entity_urn} has glossary term {term_urn}",
                {"entityUrn": entity_urn, "termUrn": term_urn}
            )
        else:
            return AssertionResult(
                False,
                f"Entity {entity_urn} does not have glossary term {term_urn}",
                {"entityUrn": entity_urn, "termUrn": term_urn, "presentTerms": term_urns}
            )
    
    except Exception as e:
        logger.error(f"Error checking if entity {entity_urn} has glossary term {term_urn}: {str(e)}")
        return AssertionResult(
            False,
            f"Error checking if entity {entity_urn} has glossary term {term_urn}: {str(e)}",
            {"entityUrn": entity_urn, "termUrn": term_urn, "error": str(e)}
        )


def assert_entity_has_structured_property(
    client: DataHubMetadataApiClient,
    entity_urn: str,
    property_name: str,
    expected_value: Optional[Any] = None
) -> AssertionResult:
    """
    Assert that an entity has a specific structured property.
    
    Args:
        client: DataHub metadata client
        entity_urn: URN of the entity
        property_name: Name of the structured property
        expected_value: Optional expected value of the property
        
    Returns:
        Assertion result
    """
    try:
        properties = get_entity_structured_properties(client, entity_urn)
        
        if not properties:
            return AssertionResult(
                False,
                f"Entity {entity_urn} has no structured properties",
                {"entityUrn": entity_urn, "propertyName": property_name}
            )
        
        if property_name not in properties:
            return AssertionResult(
                False,
                f"Entity {entity_urn} does not have structured property '{property_name}'",
                {"entityUrn": entity_urn, "propertyName": property_name, "presentProperties": list(properties.keys())}
            )
        
        actual_value = properties[property_name]
        
        if expected_value is not None and actual_value != expected_value:
            return AssertionResult(
                False,
                f"Entity {entity_urn} has structured property '{property_name}' but its value '{actual_value}' does not match expected value '{expected_value}'",
                {"entityUrn": entity_urn, "propertyName": property_name, "actualValue": actual_value, "expectedValue": expected_value}
            )
        
        value_info = f" with value '{actual_value}'" if expected_value is not None else ""
        return AssertionResult(
            True,
            f"Entity {entity_urn} has structured property '{property_name}'{value_info}",
            {"entityUrn": entity_urn, "propertyName": property_name, "value": actual_value}
        )
    
    except Exception as e:
        logger.error(f"Error checking if entity {entity_urn} has structured property '{property_name}': {str(e)}")
        return AssertionResult(
            False,
            f"Error checking if entity {entity_urn} has structured property '{property_name}': {str(e)}",
            {"entityUrn": entity_urn, "propertyName": property_name, "error": str(e)}
        )


def assert_all_entities_of_type_have_tag(
    client: DataHubMetadataApiClient,
    entity_type: str,
    tag_urn: str,
    filter_condition: Optional[Callable[[Dict[str, Any]], bool]] = None,
    use_deterministic_urns: bool = True
) -> AssertionResult:
    """
    Assert that all entities of a specific type have a particular tag.
    
    Args:
        client: DataHub metadata client
        entity_type: Type of entities to check
        tag_urn: URN of the tag or tag name (if use_deterministic_urns is True)
        filter_condition: Optional function to filter entities
        use_deterministic_urns: Whether to generate deterministic URNs for tag names
        
    Returns:
        Assertion result
    """
    # Convert tag name to deterministic URN if needed
    if use_deterministic_urns and not tag_urn.startswith("urn:li:tag:"):
        tag_urn = get_deterministic_tag_urn(tag_urn)
    
    # Get all entities of the specified type
    query = """
    query getEntitiesOfType($type: EntityType!, $start: Int!, $count: Int!) {
      searchAcrossEntities(
        input: {
          types: [$type],
          start: $start,
          count: $count
        }
      ) {
        searchResults {
          entity {
            urn
            type
            ... on EntityWithTags {
              tags {
                tags {
                  tag {
                    urn
                  }
                }
              }
            }
          }
        }
        total
      }
    }
    """
    
    try:
        # Paginate through results to get all entities
        start = 0
        count = 100
        all_entities = []
        entities_without_tag = []
        
        while True:
            variables = {
                "type": entity_type,
                "start": start,
                "count": count
            }
            
            result = client.execute_graphql(query, variables)
            
            if (
                not result
                or "data" not in result
                or "searchAcrossEntities" not in result["data"]
                or "searchResults" not in result["data"]["searchAcrossEntities"]
            ):
                break
            
            search_results = result["data"]["searchAcrossEntities"]["searchResults"]
            total = result["data"]["searchAcrossEntities"].get("total", 0)
            
            if not search_results:
                break
            
            # Process entities
            for search_result in search_results:
                entity = search_result.get("entity")
                if not entity:
                    continue
                
                # Skip if filter condition is provided and entity doesn't match
                if filter_condition and not filter_condition(entity):
                    continue
                
                all_entities.append(entity)
                
                # Check if the entity has the tag
                entity_urn = entity.get("urn")
                has_tag = False
                
                if (
                    "tags" in entity 
                    and "tags" in entity["tags"]
                ):
                    for tag_item in entity["tags"]["tags"]:
                        if tag_item.get("tag", {}).get("urn") == tag_urn:
                            has_tag = True
                            break
                
                if not has_tag:
                    entities_without_tag.append(entity_urn)
            
            # Check if we've fetched all entities
            if len(search_results) < count or start + count >= total:
                break
            
            start += count
        
        # Create assertion result
        if not all_entities:
            return AssertionResult(
                False,
                f"No entities of type {entity_type} found",
                {"entityType": entity_type, "tagUrn": tag_urn}
            )
        
        if not entities_without_tag:
            return AssertionResult(
                True,
                f"All {len(all_entities)} entities of type {entity_type} have tag {tag_urn}",
                {"entityType": entity_type, "tagUrn": tag_urn, "entityCount": len(all_entities)}
            )
        else:
            return AssertionResult(
                False,
                f"{len(entities_without_tag)} out of {len(all_entities)} entities of type {entity_type} do not have tag {tag_urn}",
                {
                    "entityType": entity_type, 
                    "tagUrn": tag_urn, 
                    "entitiesWithoutTag": entities_without_tag,
                    "totalEntityCount": len(all_entities),
                    "missingEntityCount": len(entities_without_tag)
                }
            )
    
    except Exception as e:
        logger.error(f"Error checking if all entities of type {entity_type} have tag {tag_urn}: {str(e)}")
        return AssertionResult(
            False,
            f"Error checking if all entities of type {entity_type} have tag {tag_urn}: {str(e)}",
            {"entityType": entity_type, "tagUrn": tag_urn, "error": str(e)}
        )


def run_assertion(assertion_func: Callable, *args, **kwargs) -> Dict[str, Any]:
    """
    Run an assertion function and return the result in a standardized format.
    
    Args:
        assertion_func: Assertion function to run
        *args: Positional arguments to pass to the assertion function
        **kwargs: Keyword arguments to pass to the assertion function
        
    Returns:
        Dictionary with assertion result
    """
    try:
        assertion_name = assertion_func.__name__
        result = assertion_func(*args, **kwargs)
        
        return {
            "name": assertion_name,
            "success": result.success,
            "message": result.message,
            "details": result.details
        }
    except Exception as e:
        logger.error(f"Error running assertion {assertion_func.__name__}: {str(e)}")
        return {
            "name": assertion_func.__name__,
            "success": False,
            "message": f"Error running assertion: {str(e)}",
            "details": {"error": str(e)}
        }


def run_assertions(assertions: List[Dict[str, Any]], use_deterministic_urns: bool = True) -> Dict[str, Any]:
    """
    Run a list of assertions and return the results.
    
    Args:
        assertions: List of assertion specifications
        use_deterministic_urns: Whether to use deterministic URNs for entity references
        
    Returns:
        Dictionary with assertion results
    """
    results = []
    
    for assertion in assertions:
        assertion_type = assertion.get("type")
        assertion_params = assertion.get("params", {})
        
        # Add the use_deterministic_urns parameter if it's not already set
        if "use_deterministic_urns" not in assertion_params:
            assertion_params["use_deterministic_urns"] = use_deterministic_urns
        
        # Map assertion type to function
        assertion_funcs = {
            "entity_has_tag": assert_entity_has_tag,
            "entity_has_glossary_term": assert_entity_has_glossary_term,
            "entity_has_structured_property": assert_entity_has_structured_property,
            "all_entities_of_type_have_tag": assert_all_entities_of_type_have_tag
        }
        
        if assertion_type in assertion_funcs:
            assertion_func = assertion_funcs[assertion_type]
            result = run_assertion(assertion_func, **assertion_params)
            results.append(result)
        else:
            results.append({
                "name": f"unknown_assertion_{assertion_type}",
                "success": False,
                "message": f"Unknown assertion type: {assertion_type}",
                "details": {"assertionType": assertion_type}
            })
    
    return {
        "assertionCount": len(assertions),
        "successCount": sum(1 for r in results if r["success"]),
        "failureCount": sum(1 for r in results if not r["success"]),
        "results": results
    } 