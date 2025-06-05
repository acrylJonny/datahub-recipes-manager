#!/usr/bin/env python3
"""
Utilities for working with entity relationships in DataHub.

This module provides functions for querying entities based on their relationships
with glossary terms, tags, and structured properties.
"""

import logging
import os
import sys
from typing import Dict, Any, List, Optional

# Add the parent directory to the sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from utils.datahub_metadata_api import DataHubMetadataApiClient

logger = logging.getLogger(__name__)


def get_entities_with_glossary_term(
    client: DataHubMetadataApiClient,
    term_urn: str,
    entity_types: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Get all entities that have a specific glossary term applied to them.

    Args:
        client: DataHub metadata client
        term_urn: URN of the glossary term
        entity_types: Optional list of entity types to filter by (e.g., "dataset", "dashboard")

    Returns:
        List of entities with the specified glossary term
    """
    # GraphQL query to get entities with the specified glossary term
    query = """
    query getEntitiesWithGlossaryTerm($termUrn: String!, $types: [EntityType!]) {
      searchAcrossEntities(
        input: {
          query: "",
          filters: [
            {
              field: "glossaryTerms",
              value: $termUrn
            }
          ],
          types: $types,
          start: 0,
          count: 1000
        }
      ) {
        searchResults {
          entity {
            urn
            type
            ... on Dataset {
              name
              properties {
                description
              }
              ownership {
                owners {
                  owner {
                    __typename
                    ... on CorpUser {
                      username
                    }
                    ... on CorpGroup {
                      name
                    }
                  }
                  type
                }
              }
            }
            ... on Dashboard {
              properties {
                name
                description
              }
              ownership {
                owners {
                  owner {
                    __typename
                    ... on CorpUser {
                      username
                    }
                    ... on CorpGroup {
                      name
                    }
                  }
                  type
                }
              }
            }
            # You can add additional entity types as needed
          }
        }
        total
      }
    }
    """

    variables = {"termUrn": term_urn, "types": entity_types}

    # Execute the query
    try:
        result = client.execute_graphql(query, variables)

        if (
            result
            and "data" in result
            and "searchAcrossEntities" in result["data"]
            and "searchResults" in result["data"]["searchAcrossEntities"]
        ):
            entities = []
            for search_result in result["data"]["searchAcrossEntities"][
                "searchResults"
            ]:
                entity = search_result.get("entity")
                if entity:
                    entities.append(entity)

            total = result["data"]["searchAcrossEntities"].get("total", 0)
            logger.info(
                f"Found {len(entities)} entities (out of {total}) with glossary term {term_urn}"
            )

            return entities
        else:
            logger.error(
                f"Unexpected response format when querying entities with glossary term {term_urn}"
            )
            return []

    except Exception as e:
        logger.error(f"Error querying entities with glossary term {term_urn}: {str(e)}")
        return []


def get_entities_with_tag(
    client: DataHubMetadataApiClient,
    tag_urn: str,
    entity_types: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Get all entities that have a specific tag applied to them.

    Args:
        client: DataHub metadata client
        tag_urn: URN of the tag
        entity_types: Optional list of entity types to filter by (e.g., "dataset", "dashboard")

    Returns:
        List of entities with the specified tag
    """
    # GraphQL query to get entities with the specified tag
    query = """
    query getEntitiesWithTag($tagUrn: String!, $types: [EntityType!]) {
      searchAcrossEntities(
        input: {
          query: "",
          filters: [
            {
              field: "tags",
              value: $tagUrn
            }
          ],
          types: $types,
          start: 0,
          count: 1000
        }
      ) {
        searchResults {
          entity {
            urn
            type
            ... on Dataset {
              name
              properties {
                description
              }
              ownership {
                owners {
                  owner {
                    __typename
                    ... on CorpUser {
                      username
                    }
                    ... on CorpGroup {
                      name
                    }
                  }
                  type
                }
              }
            }
            ... on Dashboard {
              properties {
                name
                description
              }
              ownership {
                owners {
                  owner {
                    __typename
                    ... on CorpUser {
                      username
                    }
                    ... on CorpGroup {
                      name
                    }
                  }
                  type
                }
              }
            }
            # You can add additional entity types as needed
          }
        }
        total
      }
    }
    """

    variables = {"tagUrn": tag_urn, "types": entity_types}

    # Execute the query
    try:
        result = client.execute_graphql(query, variables)

        if (
            result
            and "data" in result
            and "searchAcrossEntities" in result["data"]
            and "searchResults" in result["data"]["searchAcrossEntities"]
        ):
            entities = []
            for search_result in result["data"]["searchAcrossEntities"][
                "searchResults"
            ]:
                entity = search_result.get("entity")
                if entity:
                    entities.append(entity)

            total = result["data"]["searchAcrossEntities"].get("total", 0)
            logger.info(
                f"Found {len(entities)} entities (out of {total}) with tag {tag_urn}"
            )

            return entities
        else:
            logger.error(
                f"Unexpected response format when querying entities with tag {tag_urn}"
            )
            return []

    except Exception as e:
        logger.error(f"Error querying entities with tag {tag_urn}: {str(e)}")
        return []


def get_entity_structured_properties(
    client: DataHubMetadataApiClient, entity_urn: str
) -> Dict[str, Any]:
    """
    Get all structured properties for a specific entity.

    Args:
        client: DataHub metadata client
        entity_urn: URN of the entity

    Returns:
        Dictionary of structured properties
    """
    # GraphQL query to get structured properties for the entity
    query = """
    query getEntityStructuredProperties($urn: String!) {
      entity(urn: $urn) {
        structuredProperties {
          properties
        }
      }
    }
    """

    variables = {"urn": entity_urn}

    # Execute the query
    try:
        result = client.execute_graphql(query, variables)

        if (
            result
            and "data" in result
            and "entity" in result["data"]
            and "structuredProperties" in result["data"]["entity"]
            and "properties" in result["data"]["entity"]["structuredProperties"]
        ):
            return result["data"]["entity"]["structuredProperties"]["properties"]
        else:
            logger.error(
                f"Unexpected response format when querying structured properties for entity {entity_urn}"
            )
            return {}

    except Exception as e:
        logger.error(
            f"Error querying structured properties for entity {entity_urn}: {str(e)}"
        )
        return {}


def set_entity_structured_property(
    client: DataHubMetadataApiClient,
    entity_urn: str,
    property_name: str,
    property_value: Any,
) -> bool:
    """
    Set a structured property for a specific entity.

    Args:
        client: DataHub metadata client
        entity_urn: URN of the entity
        property_name: Name of the property to set
        property_value: Value to set for the property

    Returns:
        True if the operation was successful, False otherwise
    """
    # GraphQL mutation to set a structured property
    mutation = """
    mutation setStructuredProperty($input: SetStructuredPropertyInput!) {
      setStructuredProperty(input: $input) {
        success
      }
    }
    """

    variables = {
        "input": {
            "entityUrn": entity_urn,
            "name": property_name,
            "value": property_value,
        }
    }

    # Execute the mutation
    try:
        result = client.execute_graphql(mutation, variables)

        if (
            result
            and "data" in result
            and "setStructuredProperty" in result["data"]
            and "success" in result["data"]["setStructuredProperty"]
        ):
            success = result["data"]["setStructuredProperty"]["success"]
            if success:
                logger.info(
                    f"Successfully set structured property '{property_name}' for entity {entity_urn}"
                )
            else:
                logger.error(
                    f"Failed to set structured property '{property_name}' for entity {entity_urn}"
                )

            return success
        else:
            logger.error(
                f"Unexpected response format when setting structured property for entity {entity_urn}"
            )
            return False

    except Exception as e:
        logger.error(
            f"Error setting structured property for entity {entity_urn}: {str(e)}"
        )
        return False


def get_entities_with_structured_property(
    client: DataHubMetadataApiClient,
    property_name: str,
    property_value: Optional[Any] = None,
    entity_types: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Get all entities that have a specific structured property.

    Args:
        client: DataHub metadata client
        property_name: Name of the structured property
        property_value: Optional value to filter by (if None, will return all entities with the property)
        entity_types: Optional list of entity types to filter by (e.g., "dataset", "dashboard")

    Returns:
        List of entities with the specified structured property
    """
    # GraphQL query to get entities with the specified structured property
    query = """
    query getEntitiesWithStructuredProperty(
        $propertyName: String!, 
        $propertyValue: String, 
        $types: [EntityType!]
    ) {
      searchAcrossEntities(
        input: {
          query: "",
          filters: [
            {
              field: "structuredProperty",
              structuredPropertyFilter: {
                name: $propertyName,
                value: $propertyValue
              }
            }
          ],
          types: $types,
          start: 0,
          count: 1000
        }
      ) {
        searchResults {
          entity {
            urn
            type
            ... on Dataset {
              name
              properties {
                description
              }
              structuredProperties {
                properties
              }
            }
            ... on Dashboard {
              properties {
                name
                description
              }
              structuredProperties {
                properties
              }
            }
            # You can add additional entity types as needed
          }
        }
        total
      }
    }
    """

    variables = {
        "propertyName": property_name,
        "propertyValue": property_value,
        "types": entity_types,
    }

    # Execute the query
    try:
        result = client.execute_graphql(query, variables)

        if (
            result
            and "data" in result
            and "searchAcrossEntities" in result["data"]
            and "searchResults" in result["data"]["searchAcrossEntities"]
        ):
            entities = []
            for search_result in result["data"]["searchAcrossEntities"][
                "searchResults"
            ]:
                entity = search_result.get("entity")
                if entity:
                    entities.append(entity)

            total = result["data"]["searchAcrossEntities"].get("total", 0)
            value_info = (
                f" with value '{property_value}'" if property_value is not None else ""
            )
            logger.info(
                f"Found {len(entities)} entities (out of {total}) with structured property '{property_name}'{value_info}"
            )

            return entities
        else:
            logger.error(
                f"Unexpected response format when querying entities with structured property '{property_name}'"
            )
            return []

    except Exception as e:
        logger.error(
            f"Error querying entities with structured property '{property_name}': {str(e)}"
        )
        return []
