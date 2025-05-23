#!/usr/bin/env python3
"""
DataHub metadata extraction and import client.
This module provides utilities for exporting and importing metadata elements
from DataHub including domains, business glossaries, tags, and structured properties.
"""

import json
import logging
from typing import Dict, Any, List, Optional, Union, Set

from datahub.emitter.mce_builder import make_tag_urn, make_term_urn, make_domain_urn
from datahub.metadata.schema_classes import (
    GlossaryTermsClass,
    GlossaryTermAssociationClass,
    TagAssociationClass,
    GlobalTagsClass,
    DomainAssociationClass
)

from utils.datahub_rest_client import DataHubRestClient

logger = logging.getLogger(__name__)


class DataHubMetadataClient:
    """
    Client for extracting and importing metadata to/from DataHub
    """

    def __init__(self, server_url: str, token: Optional[str] = None):
        """
        Initialize the DataHub metadata client
        
        Args:
            server_url: DataHub GMS server URL
            token: DataHub authentication token (optional)
        """
        self.client = DataHubRestClient(server_url, token)
        self.server_url = server_url
        self.token = token

    def list_domains(self) -> List[Dict[str, Any]]:
        """
        List all domains in DataHub
        
        Returns:
            List of domain objects
        """
        query = """
        query listDomains($input: ListDomainsInput!) {
          listDomains(input: $input) {
            start
            count
            total
            domains {
              urn
              id
              type
              properties {
                name
                description
                __typename
              }
              parentDomains {
                domains {
                  urn
                  type
                  properties {
                    name
                    description
                    __typename
                  }
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
        }
        """
        
        variables = {
            "input": {
                "start": 0,
                "count": 1000  # Reasonable limit
            }
        }
        
        try:
            result = self.client.execute_graphql(query, variables)
            
            if not result or "errors" in result:
                error_messages = []
                if "errors" in result:
                    error_messages = [error.get("message", "Unknown error") for error in result.get("errors", [])]
                    logger.error(f"GraphQL errors: {', '.join(error_messages)}")
                return []
            
            domains_data = result.get("data", {}).get("listDomains", {}).get("domains", [])
            
            return domains_data
        except Exception as e:
            logger.error(f"Error listing domains: {str(e)}")
            return []
            
    def get_root_glossary_nodes(self) -> List[Dict[str, Any]]:
        """
        Get all root glossary nodes
        
        Returns:
            List of root glossary nodes
        """
        query = """
        query getRootGlossaryNodes {
          getRootGlossaryNodes(input: {start: 0, count: 1000}) {
            count
            start
            total
            nodes {
              urn
              type
              properties {
                name
                description
                __typename
              }
              displayProperties {
                colorHex
                icon {
                  name
                  style
                  iconLibrary
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
        }
        """
        
        try:
            result = self.client.execute_graphql(query)
            
            if not result or "errors" in result:
                error_messages = []
                if "errors" in result:
                    error_messages = [error.get("message", "Unknown error") for error in result.get("errors", [])]
                    logger.error(f"GraphQL errors: {', '.join(error_messages)}")
                return []
            
            nodes_data = result.get("data", {}).get("getRootGlossaryNodes", {}).get("nodes", [])
            
            return nodes_data
        except Exception as e:
            logger.error(f"Error getting root glossary nodes: {str(e)}")
            return []
    
    def get_root_glossary_terms(self) -> List[Dict[str, Any]]:
        """
        Get all root glossary terms
        
        Returns:
            List of root glossary terms
        """
        query = """
        query getRootGlossaryTerms {
          getRootGlossaryTerms(input: {start: 0, count: 1000}) {
            count
            start
            total
            terms {
              urn
              type
              name
              hierarchicalName
              properties {
                name
                description
                __typename
              }
              __typename
            }
            __typename
          }
        }
        """
        
        try:
            result = self.client.execute_graphql(query)
            
            if not result or "errors" in result:
                error_messages = []
                if "errors" in result:
                    error_messages = [error.get("message", "Unknown error") for error in result.get("errors", [])]
                    logger.error(f"GraphQL errors: {', '.join(error_messages)}")
                return []
            
            terms_data = result.get("data", {}).get("getRootGlossaryTerms", {}).get("terms", [])
            
            return terms_data
        except Exception as e:
            logger.error(f"Error getting root glossary terms: {str(e)}")
            return []
    
    def get_glossary_node(self, node_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a glossary node and its children by URN
        
        Args:
            node_urn: Glossary node URN
            
        Returns:
            Glossary node data or None if not found
        """
        query = """
        query getGlossaryNode($urn: String!) {
          glossaryNode(urn: $urn) {
            urn
            type
            exists
            properties {
              name
              description
              __typename
            }
            displayProperties {
              colorHex
              icon {
                name
                style
                iconLibrary
                __typename
              }
              __typename
            }
            children: relationships(
              input: {types: ["IsPartOf"], direction: INCOMING, start: 0, count: 10000}
            ) {
              total
              relationships {
                direction
                entity {
                  type
                  urn
                  ... on GlossaryNode {
                    properties {
                      name
                      description
                      __typename
                    }
                    __typename
                  }
                  ... on GlossaryTerm {
                    name
                    hierarchicalName
                    properties {
                      name
                      description
                      __typename
                    }
                    __typename
                  }
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
        }
        """
        
        variables = {
            "urn": node_urn
        }
        
        try:
            result = self.client.execute_graphql(query, variables)
            
            if not result or "errors" in result:
                error_messages = []
                if "errors" in result:
                    error_messages = [error.get("message", "Unknown error") for error in result.get("errors", [])]
                    logger.error(f"GraphQL errors: {', '.join(error_messages)}")
                return None
            
            node_data = result.get("data", {}).get("glossaryNode")
            
            return node_data
        except Exception as e:
            logger.error(f"Error getting glossary node: {str(e)}")
            return None
            
    def list_all_tags(self) -> List[Dict[str, Any]]:
        """
        List all tags in DataHub
        
        Returns:
            List of tag objects
        """
        query = """
        query getSearchResultsForMultiple($input: SearchAcrossEntitiesInput!) {
          searchAcrossEntities(input: $input) {
            start
            count
            total
            searchResults {
              entity {
                urn
                type
                ... on Tag {
                  name
                  properties {
                    name
                    colorHex
                    __typename
                  }
                  description
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
        }
        """
        
        variables = {
            "input": {
                "types": ["TAG"],
                "query": "*",
                "start": 0,
                "count": 1000  # Reasonable limit
            }
        }
        
        try:
            result = self.client.execute_graphql(query, variables)
            
            if not result or "errors" in result:
                error_messages = []
                if "errors" in result:
                    error_messages = [error.get("message", "Unknown error") for error in result.get("errors", [])]
                    logger.error(f"GraphQL errors: {', '.join(error_messages)}")
                return []
            
            search_results = result.get("data", {}).get("searchAcrossEntities", {}).get("searchResults", [])
            tags = [result.get("entity") for result in search_results if result.get("entity", {}).get("type") == "TAG"]
            
            return tags
        except Exception as e:
            logger.error(f"Error listing tags: {str(e)}")
            return []
    
    def list_structured_properties(self) -> List[Dict[str, Any]]:
        """
        List all structured properties in DataHub
        
        Returns:
            List of structured property objects
        """
        query = """
        query getSearchResultsForMultiple($input: SearchAcrossEntitiesInput!) {
          searchAcrossEntities(input: $input) {
            start
            count
            total
            searchResults {
              entity {
                urn
                type
                ... on StructuredPropertyEntity {
                  definition {
                    displayName
                    qualifiedName
                    description
                    cardinality
                    immutable
                    valueType {
                      urn
                      type
                      info {
                        type
                        displayName
                        __typename
                      }
                      __typename
                    }
                    entityTypes {
                      urn
                      type
                      info {
                        type
                        __typename
                      }
                      __typename
                    }
                    filterStatus
                    typeQualifier {
                      allowedTypes {
                        urn
                        type
                        info {
                          type
                          displayName
                          __typename
                        }
                        __typename
                      }
                      __typename
                    }
                    allowedValues {
                      value {
                        ... on StringValue {
                          stringValue
                          __typename
                        }
                        ... on NumberValue {
                          numberValue
                          __typename
                        }
                        __typename
                      }
                      description
                      __typename
                    }
                    __typename
                  }
                  settings {
                    isHidden
                    showInSearchFilters
                    showAsAssetBadge
                    showInAssetSummary
                    showInColumnsTable
                    __typename
                  }
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
        }
        """
        
        variables = {
            "input": {
                "types": ["STRUCTURED_PROPERTY_ENTITY"],
                "query": "*",
                "start": 0,
                "count": 1000  # Reasonable limit
            }
        }
        
        try:
            result = self.client.execute_graphql(query, variables)
            
            if not result or "errors" in result:
                error_messages = []
                if "errors" in result:
                    error_messages = [error.get("message", "Unknown error") for error in result.get("errors", [])]
                    logger.error(f"GraphQL errors: {', '.join(error_messages)}")
                return []
            
            search_results = result.get("data", {}).get("searchAcrossEntities", {}).get("searchResults", [])
            props = [result.get("entity") for result in search_results if result.get("entity", {}).get("type") == "STRUCTURED_PROPERTY_ENTITY"]
            
            return props
        except Exception as e:
            logger.error(f"Error listing structured properties: {str(e)}")
            return []
    
    def list_metadata_tests(self) -> List[Dict[str, Any]]:
        """
        List all metadata tests in DataHub
        
        Returns:
            List of test objects
        """
        query = """
        query getSearchResultsForMultiple($input: SearchAcrossEntitiesInput!) {
          searchAcrossEntities(input: $input) {
            start
            count
            total
            searchResults {
              entity {
                urn
                type
                ... on Test {
                  name
                  description
                  category
                  params
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
        }
        """
        
        variables = {
            "input": {
                "types": ["TEST"],
                "query": "*",
                "start": 0,
                "count": 1000  # Reasonable limit
            }
        }
        
        try:
            result = self.client.execute_graphql(query, variables)
            
            if not result or "errors" in result:
                error_messages = []
                if "errors" in result:
                    error_messages = [error.get("message", "Unknown error") for error in result.get("errors", [])]
                    logger.error(f"GraphQL errors: {', '.join(error_messages)}")
                return []
            
            search_results = result.get("data", {}).get("searchAcrossEntities", {}).get("searchResults", [])
            tests = [result.get("entity") for result in search_results if result.get("entity", {}).get("type") == "TEST"]
            
            return tests
        except Exception as e:
            logger.error(f"Error listing metadata tests: {str(e)}")
            return []
    
    def export_domain(self, domain_urn: str, include_entities: bool = False) -> Dict[str, Any]:
        """
        Export a domain with its properties
        
        Args:
            domain_urn: Domain URN
            include_entities: Whether to include entities belonging to the domain
            
        Returns:
            Dictionary with domain data
        """
        query = """
        query getDomain($urn: String!) {
          domain(urn: $urn) {
            urn
            type
            properties {
              name
              description
              __typename
            }
            ownership {
              owners {
                owner {
                  ... on CorpUser {
                    urn
                    username
                    __typename
                  }
                  ... on CorpGroup {
                    urn
                    name
                    __typename
                  }
                  __typename
                }
                type
                associatedUrn
                __typename
              }
              __typename
            }
            parentDomains {
              domains {
                urn
                type
                properties {
                  name
                  description
                  __typename
                }
                __typename
              }
              __typename
            }
            displayProperties {
              colorHex
              icon {
                name
                style
                iconLibrary
                __typename
              }
              __typename
            }
            __typename
          }
        }
        """
        
        variables = {
            "urn": domain_urn
        }
        
        try:
            result = self.client.execute_graphql(query, variables)
            
            if not result or "errors" in result:
                error_messages = []
                if "errors" in result:
                    error_messages = [error.get("message", "Unknown error") for error in result.get("errors", [])]
                    logger.error(f"GraphQL errors: {', '.join(error_messages)}")
                return {}
            
            domain_data = result.get("data", {}).get("domain", {})
            
            # If we want to include entities, make another query
            if include_entities:
                entities_query = """
                query getDomainEntities($urn: String!, $input: SearchInput!) {
                  domain(urn: $urn) {
                    urn
                    entities(input: $input) {
                      start
                      count
                      total
                      searchResults {
                        entity {
                          urn
                          type
                          __typename
                        }
                        __typename
                      }
                      __typename
                    }
                    __typename
                  }
                }
                """
                
                entities_variables = {
                    "urn": domain_urn,
                    "input": {
                        "start": 0,
                        "count": 1000,
                        "query": "*"
                    }
                }
                
                entities_result = self.client.execute_graphql(entities_query, entities_variables)
                
                if entities_result and "data" in entities_result and "domain" in entities_result["data"]:
                    entities_data = entities_result["data"]["domain"].get("entities", {}).get("searchResults", [])
                    domain_data["entities"] = [r.get("entity", {}) for r in entities_data]
            
            return domain_data
        except Exception as e:
            logger.error(f"Error exporting domain: {str(e)}")
            return {}
            
    def export_glossary_term(self, term_urn: str) -> Dict[str, Any]:
        """
        Export a glossary term with its properties
        
        Args:
            term_urn: Glossary term URN
            
        Returns:
            Dictionary with glossary term data
        """
        query = """
        query getGlossaryTerm($urn: String!) {
          glossaryTerm(urn: $urn) {
            urn
            type
            name
            hierarchicalName
            properties {
              name
              description
              definition
              termSource
              sourceRef
              sourceUrl
              customProperties {
                key
                value
                __typename
              }
              __typename
            }
            ownership {
              owners {
                owner {
                  ... on CorpUser {
                    urn
                    username
                    __typename
                  }
                  ... on CorpGroup {
                    urn
                    name
                    __typename
                  }
                  __typename
                }
                type
                associatedUrn
                __typename
              }
              __typename
            }
            parentNodes {
              count
              nodes {
                urn
                type
                properties {
                  name
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
        }
        """
        
        variables = {
            "urn": term_urn
        }
        
        try:
            result = self.client.execute_graphql(query, variables)
            
            if not result or "errors" in result:
                error_messages = []
                if "errors" in result:
                    error_messages = [error.get("message", "Unknown error") for error in result.get("errors", [])]
                    logger.error(f"GraphQL errors: {', '.join(error_messages)}")
                return {}
            
            term_data = result.get("data", {}).get("glossaryTerm", {})
            
            return term_data
        except Exception as e:
            logger.error(f"Error exporting glossary term: {str(e)}")
            return {}
            
    def export_tag(self, tag_urn: str) -> Dict[str, Any]:
        """
        Export a tag with its properties
        
        Args:
            tag_urn: Tag URN
            
        Returns:
            Dictionary with tag data
        """
        query = """
        query getTag($urn: String!) {
          tag(urn: $urn) {
            urn
            type
            name
            description
            properties {
              name
              description
              colorHex
              __typename
            }
            __typename
          }
        }
        """
        
        variables = {
            "urn": tag_urn
        }
        
        try:
            result = self.client.execute_graphql(query, variables)
            
            if not result or "errors" in result:
                error_messages = []
                if "errors" in result:
                    error_messages = [error.get("message", "Unknown error") for error in result.get("errors", [])]
                    logger.error(f"GraphQL errors: {', '.join(error_messages)}")
                return {}
            
            tag_data = result.get("data", {}).get("tag", {})
            
            return tag_data
        except Exception as e:
            logger.error(f"Error exporting tag: {str(e)}")
            return {}
    
    def import_domain(self, domain_data: Dict[str, Any]) -> Optional[str]:
        """
        Import a domain from data
        
        Args:
            domain_data: Domain data
            
        Returns:
            Domain URN if successful, None otherwise
        """
        # Implementation will require creating the domain via GraphQL mutation
        # This would need DataHub write permissions and would follow similar patterns
        # to other DataHub entities creation
        logger.error("Domain import not yet implemented")
        return None
        
    def import_glossary_term(self, term_data: Dict[str, Any]) -> Optional[str]:
        """
        Import a glossary term from data
        
        Args:
            term_data: Glossary term data
            
        Returns:
            Term URN if successful, None otherwise
        """
        # Implementation will require creating the glossary term via GraphQL mutation
        logger.error("Glossary term import not yet implemented")
        return None
        
    def import_tag(self, tag_data: Dict[str, Any]) -> Optional[str]:
        """
        Import a tag from data
        
        Args:
            tag_data: Tag data
            
        Returns:
            Tag URN if successful, None otherwise
        """
        # Implementation will require creating the tag via GraphQL mutation
        logger.error("Tag import not yet implemented")
        return None
        
    def export_all_metadata(self, output_file: str) -> bool:
        """
        Export all metadata (domains, glossary, tags, properties) to a file
        
        Args:
            output_file: Path to output file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get all domains
            domains = self.list_domains()
            
            # Get all root glossary nodes and traverse them
            root_nodes = self.get_root_glossary_nodes()
            glossary_nodes = root_nodes.copy()
            
            # Get all root glossary terms
            root_terms = self.get_root_glossary_terms()
            glossary_terms = root_terms.copy()
            
            # Get all tags
            tags = self.list_all_tags()
            
            # Get all structured properties
            structured_properties = self.list_structured_properties()
            
            # Get all tests
            tests = self.list_metadata_tests()
            
            # Create the metadata export package
            metadata_package = {
                "version": "1.0",
                "domains": domains,
                "glossary": {
                    "nodes": glossary_nodes,
                    "terms": glossary_terms
                },
                "tags": tags,
                "structured_properties": structured_properties,
                "tests": tests
            }
            
            # Write to file
            with open(output_file, 'w') as f:
                json.dump(metadata_package, f, indent=2)
            
            logger.info(f"Successfully exported metadata to {output_file}")
            return True
        except Exception as e:
            logger.error(f"Error exporting metadata: {str(e)}")
            return False
            
    def import_metadata_from_file(self, input_file: str) -> bool:
        """
        Import metadata from a file
        
        Args:
            input_file: Path to input file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Read the metadata package
            with open(input_file, 'r') as f:
                metadata_package = json.load(f)
            
            # Validate the metadata package
            if "version" not in metadata_package:
                logger.error("Invalid metadata package: missing version")
                return False
                
            # TODO: Implement the actual import logic for each metadata type
            # This would involve creating all the entities in the correct order
            # (e.g., domains before glossary terms that reference them)
            
            logger.error("Metadata import not yet fully implemented")
            return False
        except Exception as e:
            logger.error(f"Error importing metadata: {str(e)}")
            return False 