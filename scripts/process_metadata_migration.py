#!/usr/bin/env python3
"""
Metadata Migration Processor

This script processes exported entity metadata and generates MCPs (Metadata Change Proposals)
for migrating metadata between DataHub environments using browse path and name matching.

Usage:
    python process_metadata_migration.py --input exported_entities.json --target-env staging --dry-run
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import logging
from urllib.parse import urlparse

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.datahub_api import DataHubClient
from utils.datahub_metadata_api import DataHubMetadataApiClient

try:
    from datahub.metadata.schema_classes import (
        ChangeTypeClass,
        GlobalTagsClass,
        TagAssociationClass,
        GlossaryTermsClass,
        GlossaryTermAssociationClass,
        DomainsClass,
        StructuredPropertiesClass,
        StructuredPropertyValueAssignmentClass,
        EditableSchemaMetadataClass,
        SchemaFieldDataTypeClass,
        GlobalTagsClass as FieldTagsClass,
        GlossaryTermsClass as FieldGlossaryTermsClass,
    )
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
except ImportError as e:
    logging.error(f"Failed to import DataHub schema classes: {e}")
    sys.exit(1)

@dataclass
class EntityMatch:
    """Represents a matched entity between source and target environments"""
    source_entity: Dict[str, Any]
    target_urn: str
    target_name: str
    browse_path: str
    confidence: float

@dataclass
class MCPTask:
    """Represents an MCP generation task"""
    entity_urn: str
    mcp_type: str
    aspect_data: Dict[str, Any]
    source_urns: List[str]  # Original URNs that need mutation

class MetadataMigrationProcessor:
    """Main processor for metadata migration"""
    
    def __init__(self, target_environment: str, mutations: Optional[Dict] = None, dry_run: bool = False):
        self.target_environment = target_environment
        self.mutations = mutations or {}
        self.dry_run = dry_run
        self.api_client = None
        self.metadata_api = None
        
        # Track if mutations were already applied during export
        self.mutations_already_applied = False
        self.export_metadata = None
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize API clients if not dry run
        if not dry_run:
            self._initialize_api_clients()
    
    def safe_get(self, obj: Any, *keys: str, default: Any = None) -> Any:
        """Safely get nested values from dictionaries, handling None values"""
        if not obj:
            return default
            
        current = obj
        for key in keys:
            if not isinstance(current, dict):
                return default
            current = current.get(key)
            if current is None:
                return default
        return current
    
    def _initialize_api_clients(self):
        """Initialize DataHub API clients"""
        try:
            # For now, we'll initialize with placeholder values since we need actual connection details
            # In a real scenario, these would come from environment variables or config
            server_url = os.getenv('DATAHUB_GMS_URL', 'http://localhost:8080')
            token = os.getenv('DATAHUB_GMS_TOKEN')
            
            self.api_client = DataHubClient(server_url, token)
            self.metadata_api = DataHubMetadataApiClient(server_url, token)
            self.logger.info(f"Initialized API clients for environment: {self.target_environment}")
        except Exception as e:
            self.logger.error(f"Failed to initialize API clients: {e}")
            if not self.dry_run:
                raise
    
    def load_exported_entities(self, filepath: str) -> List[Dict[str, Any]]:
        """Load exported entities from JSON file"""
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            # Handle different export formats
            if isinstance(data, dict):
                # New format: check for metadata and entities
                if 'metadata' in data and 'entities' in data:
                    # Extract metadata information
                    metadata = data['metadata']
                    entities = data['entities']
                    
                    # Log export information
                    self.logger.info(f"Loaded export from {metadata.get('environment', 'unknown')} environment")
                    self.logger.info(f"Export timestamp: {metadata.get('export_timestamp', 'unknown')}")
                    self.logger.info(f"Mutations already applied: {metadata.get('mutations_applied', False)}")
                    
                    # If mutations were already applied, we don't need to apply them again
                    if metadata.get('mutations_applied', False):
                        self.logger.info("Mutations were already applied during export - skipping mutation step")
                        self.mutations_already_applied = True
                        self.export_metadata = metadata
                        
                        # Extract mutations from export metadata for browse path searches
                        if 'mutation_config' in metadata:
                            mutation_config = metadata['mutation_config']
                            if 'platform_instance_mapping' in mutation_config:
                                # Convert export format to internal format
                                self.mutations = {
                                    'platform_instances': mutation_config['platform_instance_mapping']
                                }
                                self.logger.info(f"Loaded platform instance mappings from export: {self.mutations['platform_instances']}")
                            else:
                                self.mutations = {}
                        else:
                            self.mutations = {}
                    
                elif 'entities' in data:
                    # Legacy format with entities key
                    entities = data['entities']
                elif 'export_data' in data:
                    # Legacy format with export_data key
                    entities = data['export_data']
                else:
                    # Single entity format
                    entities = [data]
            else:
                # List format
                entities = data
            
            # Filter out None values and invalid entities
            valid_entities = []
            invalid_count = 0
            for entity in entities:
                if entity is not None and isinstance(entity, dict) and len(entity) > 0:
                    valid_entities.append(entity)
                else:
                    invalid_count += 1
                    
            self.logger.info(f"Loaded {len(valid_entities)} valid entities from {filepath}")
            if invalid_count > 0:
                self.logger.warning(f"Filtered out {invalid_count} invalid/null entities")
            return valid_entities
            
        except Exception as e:
            self.logger.error(f"Failed to load entities from {filepath}: {e}")
            raise
    
    def extract_browse_path(self, entity: Dict[str, Any]) -> str:
        """Extract browse path from entity using the same logic as JavaScript"""
        if not entity or not isinstance(entity, dict):
            return ''
            
        browse_paths = []
        
        # Check browsePathV2 first (preferred)
        browse_path_v2 = self.safe_get(entity, 'browsePathV2', 'path')
        if browse_path_v2:
            path_parts = []
            for path_item in browse_path_v2:
                path_name = self.safe_get(path_item, 'entity', 'properties', 'name')
                if path_name:
                    path_parts.append(path_name)
            if path_parts:
                browse_paths.append('/' + '/'.join(path_parts))
        
        # Fallback to legacy browsePaths
        if not browse_paths and entity.get('browsePaths'):
            browse_paths.extend(entity['browsePaths'])
        
        # Return the first (most specific) browse path
        return browse_paths[0] if browse_paths else ''
    
    def extract_entity_name(self, entity: Dict[str, Any]) -> str:
        """Extract entity name from entity data"""
        if not entity or not isinstance(entity, dict):
            return ''
            
        # Try different sources for entity name
        editable_properties = entity.get('editableProperties') or {}
        properties = entity.get('properties') or {}
        
        name = (
            entity.get('name') or
            editable_properties.get('name') or
            properties.get('name') or
            ''
        )
        
        # If still no name, extract from URN
        if not name and entity.get('urn'):
            urn_parts = entity['urn'].split('/')
            if urn_parts:
                name = urn_parts[-1].split(',')[-2] if ',' in urn_parts[-1] else urn_parts[-1]
        
        return name
    
    def fetch_target_entities(self, source_entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Fetch entities from target environment based on browse paths and name/platform matching"""
        if self.dry_run:
            self.logger.info("DRY RUN: Would fetch target entities based on browse paths and name/platform matching")
            # Show what the query would be even in dry-run mode
            self._show_graphql_query_preview(source_entities)
            return []
        
        try:
            # Extract unique browse paths from source entities and apply mutations
            browse_paths = set()
            platforms = set()
            entity_types = set()
            entity_names = set()
            platform_instances = set()
            mutated_browse_paths = set()
            
            for entity in source_entities:
                if not entity or not isinstance(entity, dict):
                    continue
                    
                # Build browse path including platform instance
                path_parts = []
                
                # Add platform instance as first component if available
                platform_instance = self.safe_get(entity, 'dataPlatformInstance', 'instanceId')
                if platform_instance:
                    path_parts.append(platform_instance)
                
                # Add browse path components
                browse_path = self.extract_browse_path(entity)
                if browse_path:
                    # Extract path components for search
                    browse_path_parts = [part.strip() for part in browse_path.strip('/').split('/') if part.strip()]
                    path_parts.extend(browse_path_parts)
                
                if path_parts:
                    browse_paths.update(path_parts)
                    
                    # Apply platform instance mutations to browse path components
                    mutated_path_parts = []
                    for part in path_parts:
                        # Check if this part is a platform instance that needs mutation
                        if part in self.mutations.get('platform_instances', {}):
                            mutated_part = self.mutations['platform_instances'][part]
                            mutated_path_parts.append(mutated_part)
                            self.logger.debug(f"Mutated browse path component: {part} -> {mutated_part}")
                        else:
                            mutated_path_parts.append(part)
                    mutated_browse_paths.update(mutated_path_parts)
                
                # Also collect platforms, entity types, names, and platform instances
                if entity.get('platform', {}).get('name'):
                    platforms.add(entity['platform']['name'])
                if entity.get('type'):
                    entity_types.add(entity['type'])
                if entity.get('name'):
                    entity_names.add(entity['name'])
                if entity.get('dataPlatformInstance', {}).get('instanceId'):
                    platform_instances.add(entity['dataPlatformInstance']['instanceId'])
            
            # Determine which browse paths to use for searching
            # Even if mutations were "already applied", we need to search for the mutated browse paths
            # in the target environment since the source entities may still contain original platform instances
            search_browse_paths = list(mutated_browse_paths) if mutated_browse_paths else list(browse_paths)
            if self.mutations_already_applied:
                if mutated_browse_paths:
                    self.logger.info("Mutations were marked as applied during export, but applying them for browse path search")
                    self.logger.info(f"Original browse paths: {list(browse_paths)} -> Mutated browse paths: {list(mutated_browse_paths)}")
                else:
                    self.logger.info("Using original browse paths since no mutations were needed")
            else:
                self.logger.info(f"Applied platform instance mutations to browse paths: {list(browse_paths)} -> {list(mutated_browse_paths)}")
            
            # Search for entities using multiple approaches
            target_entities = []
            
            # Approach 1: Search by browse paths (with mutations applied)
            if search_browse_paths:
                self.logger.info(f"Searching target DataHub for entities with browse path components: {search_browse_paths}")
                browse_path_entities = self._search_entities_by_browse_paths(search_browse_paths, list(platforms), list(entity_types), list(platform_instances))
                target_entities.extend(browse_path_entities)
                self.logger.info(f"Browse path search found {len(browse_path_entities)} entities")
            
            # Approach 2: Search by name and platform (for different platform instances)
            # This is the primary matching approach when browse paths don't match
            if entity_names and platforms:
                self.logger.info(f"Searching target DataHub for entities by name and platform: {sorted(entity_names)[:10]}{'...' if len(entity_names) > 10 else ''} on {sorted(platforms)}")
                if platform_instances:
                    self.logger.info(f"Source entities have platform instances: {sorted(platform_instances)}")
                name_platform_entities = self._search_entities_by_name_and_platform(
                    list(entity_names),
                    list(platforms),
                    list(entity_types),
                    list(platform_instances)
                )
                target_entities.extend(name_platform_entities)
                self.logger.info(f"Name/platform search found {len(name_platform_entities)} entities")
            
            # Remove duplicates based on URN
            unique_entities = {}
            for entity in target_entities:
                urn = entity.get('urn')
                if urn and urn not in unique_entities:
                    unique_entities[urn] = entity
            
            final_entities = list(unique_entities.values())
            self.logger.info(f"Fetched {len(final_entities)} unique entities from target environment")
            return final_entities
            
        except Exception as e:
            self.logger.error(f"Failed to fetch target entities: {e}")
            return []
    
    def _search_entities_by_browse_paths(self, browse_path_components: List[str], platforms: List[str], entity_types: List[str], platform_instances: List[str] = None) -> List[Dict[str, Any]]:
        """Search for entities in target DataHub using browse path components"""
        try:
            # Create GraphQL query for searching entities by browse paths
            query = """
            query GetEntitiesWithBrowsePathsForSearch($input: SearchAcrossEntitiesInput!) {
              searchAcrossEntities(input: $input) {
                start
                count
                total
                searchResults {
                  entity {
                    urn
                    type
                    ... on Dataset {
                      name
                      platform {
                        name
                        properties {
                          displayName
                        }
                      }
                      dataPlatformInstance {
                        instanceId
                        platform {
                          name
                        }
                      }
                      domain {
                        domain {
                          urn
                        }
                      }
                      browsePaths {
                        path
                      }
                      browsePathV2 {
                        path {
                          entity {
                            ... on Container {
                              properties {
                                name
                              }
                            }
                          }
                        }
                      }
                      editableProperties {
                        name
                        description
                      }
                      tags {
                        tags {
                          tag {
                            urn
                          }
                        }
                      }
                      glossaryTerms {
                        terms {
                          term {
                            urn
                            glossaryTermInfo {
                              name
                            }
                          }
                        }
                      }
                      structuredProperties {
                        properties {
                          structuredProperty {
                            urn
                          }
                          values {
                            ... on StringValue {
                              stringValue
                            }
                            ... on NumberValue {
                              numberValue
                            }
                          }
                          valueEntities {
                            urn
                          }
                        }
                      }
                    }
                    ... on Container {
                      properties {
                        name
                      }
                      platform {
                        name
                        properties {
                          displayName
                        }
                      }
                      dataPlatformInstance {
                        instanceId
                        platform {
                          name
                        }
                      }
                      domain {
                        domain {
                          urn
                        }
                      }
                      browsePathV2 {
                        path {
                          entity {
                            ... on Container {
                              properties {
                                name
                              }
                            }
                          }
                        }
                      }
                      editableProperties {
                        description
                      }
                      tags {
                        tags {
                          tag {
                            urn
                          }
                        }
                      }
                      glossaryTerms {
                        terms {
                          term {
                            urn
                            glossaryTermInfo {
                              name
                            }
                          }
                        }
                      }
                      structuredProperties {
                        properties {
                          structuredProperty {
                            urn
                          }
                          values {
                            ... on StringValue {
                              stringValue
                            }
                            ... on NumberValue {
                              numberValue
                            }
                          }
                          valueEntities {
                            urn
                          }
                        }
                      }
                    }
                    ... on Chart {
                      properties {
                        name
                      }
                      platform {
                        name
                        properties {
                          displayName
                        }
                      }
                      dataPlatformInstance {
                        instanceId
                        platform {
                          name
                        }
                      }
                      domain {
                        domain {
                          urn
                        }
                      }
                      browsePaths {
                        path
                      }
                      browsePathV2 {
                        path {
                          entity {
                            ... on Container {
                              properties {
                                name
                              }
                            }
                          }
                        }
                      }
                      editableProperties {
                        description
                      }
                      tags {
                        tags {
                          tag {
                            urn
                          }
                        }
                      }
                      glossaryTerms {
                        terms {
                          term {
                            urn
                            glossaryTermInfo {
                              name
                            }
                          }
                        }
                      }
                      structuredProperties {
                        properties {
                          structuredProperty {
                            urn
                          }
                          values {
                            ... on StringValue {
                              stringValue
                            }
                            ... on NumberValue {
                              numberValue
                            }
                          }
                          valueEntities {
                            urn
                          }
                        }
                      }
                    }
                    ... on Dashboard {
                      properties {
                        name
                      }
                      platform {
                        name
                        properties {
                          displayName
                        }
                      }
                      dataPlatformInstance {
                        instanceId
                        platform {
                          name
                        }
                      }
                      domain {
                        domain {
                          urn
                        }
                      }
                      browsePaths {
                        path
                      }
                      browsePathV2 {
                        path {
                          entity {
                            ... on Container {
                              properties {
                                name
                              }
                            }
                          }
                        }
                      }
                      editableProperties {
                        description
                      }
                      tags {
                        tags {
                          tag {
                            urn
                          }
                        }
                      }
                      glossaryTerms {
                        terms {
                          term {
                            urn
                            glossaryTermInfo {
                              name
                            }
                          }
                        }
                      }
                      structuredProperties {
                        properties {
                          structuredProperty {
                            urn
                          }
                          values {
                            ... on StringValue {
                              stringValue
                            }
                            ... on NumberValue {
                              numberValue
                            }
                          }
                          valueEntities {
                            urn
                          }
                        }
                      }
                    }
                    ... on DataFlow {
                      properties {
                        name
                      }
                      platform {
                        name
                        properties {
                          displayName
                        }
                      }
                      dataPlatformInstance {
                        instanceId
                        platform {
                          name
                        }
                      }
                      domain {
                        domain {
                          urn
                        }
                      }
                      browsePaths {
                        path
                      }
                      browsePathV2 {
                        path {
                          entity {
                            ... on Container {
                              properties {
                                name
                              }
                            }
                          }
                        }
                      }
                      editableProperties {
                        description
                      }
                      tags {
                        tags {
                          tag {
                            urn
                          }
                        }
                      }
                      glossaryTerms {
                        terms {
                          term {
                            urn
                            glossaryTermInfo {
                              name
                            }
                          }
                        }
                      }
                      structuredProperties {
                        properties {
                          structuredProperty {
                            urn
                          }
                          values {
                            ... on StringValue {
                              stringValue
                            }
                            ... on NumberValue {
                              numberValue
                            }
                          }
                          valueEntities {
                            urn
                          }
                        }
                      }
                    }
                    ... on DataJob {
                      properties {
                        name
                      }
                      dataFlow {
                        flowId
                        properties {
                          name
                        }
                      }
                      dataPlatformInstance {
                        instanceId
                        platform {
                          name
                        }
                      }
                      domain {
                        domain {
                          urn
                        }
                      }
                      browsePaths {
                        path
                      }
                      browsePathV2 {
                        path {
                          entity {
                            ... on Container {
                              properties {
                                name
                              }
                            }
                          }
                        }
                      }
                      editableProperties {
                        description
                      }
                      tags {
                        tags {
                          tag {
                            urn
                          }
                        }
                      }
                      glossaryTerms {
                        terms {
                          term {
                            urn
                            glossaryTermInfo {
                              name
                            }
                          }
                        }
                      }
                      structuredProperties {
                        properties {
                          structuredProperty {
                            urn
                          }
                          values {
                            ... on StringValue {
                              stringValue
                            }
                            ... on NumberValue {
                              numberValue
                            }
                          }
                          valueEntities {
                            urn
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """
            
            # Create OR filters for browse paths
            or_filters = []
            
            # Add browsePathV2 filter
            if browse_path_components:
                or_filters.append({
                    "and": [
                        {
                            "field": "browsePathV2",
                            "condition": "CONTAIN",
                            "values": browse_path_components,
                            "negated": False
                        }
                    ]
                })
                
                # Add browsePath filter as fallback
                or_filters.append({
                    "and": [
                        {
                            "field": "browsePath",
                            "condition": "CONTAIN",
                            "values": browse_path_components,
                            "negated": False
                        }
                    ]
                })
            
            # Note: Platform filtering via GraphQL orFilters doesn't seem to work reliably
            # So we'll do broader search and filter manually on client side
            # if platforms:
            #     or_filters.append({
            #         "and": [
            #             {
            #                 "field": "platform",
            #                 "condition": "EQUAL",
            #                 "values": platforms,
            #                 "negated": False
            #             }
            #         ]
            #     })
            
            # Add platform instance filter if we have platform instances
            # Note: Only add this filter if we know the target entities have platform instances
            # Many target environments may not have platform instances set, so we'll match by name/platform primarily
            if platform_instances:
                self.logger.info(f"Source entities have platform instances: {platform_instances}")
                self.logger.info("Note: Target entities may not have platform instances - will match by name/platform primarily")
                # Don't add platformInstance filter as it may exclude valid matches
                # or_filters.append({
                #     "and": [
                #         {
                #             "field": "platformInstance",
                #             "condition": "EQUAL",
                #             "values": platform_instances,
                #             "negated": False
                #         }
                #     ]
                # })
            
            variables = {
                "input": {
                    "query": "",
                    "start": 0,
                    "count": 1000,
                    "orFilters": or_filters
                }
            }
            
            # Add entity types filter if we have them
            if entity_types:
                variables["input"]["types"] = entity_types
            
            self.logger.info(f"Executing GraphQL query with variables: {variables}")
            if platform_instances:
                self.logger.info(f"Including platform instances in search: {platform_instances}")
            
            # Execute the GraphQL query
            result = self.metadata_api.context.graph.execute_graphql(query, variables)
            
            if result and "searchAcrossEntities" in result:
                search_results = result["searchAcrossEntities"].get("searchResults", [])
                entities = []
                
                for search_result in search_results:
                    entity = search_result.get("entity")
                    if entity:
                        # Apply platform filtering manually since GraphQL filter doesn't work reliably
                        entity_platform = entity.get("platform", {}).get("name", "")
                        if not platforms or entity_platform in platforms:
                            entities.append(entity)
                        else:
                            self.logger.debug(f"Filtered out entity {entity.get('name', 'N/A')} with platform {entity_platform}")
                
                self.logger.info(f"Found {len(entities)} entities in target DataHub (after platform filtering)")
                return entities
            else:
                self.logger.warning("No search results found in GraphQL response")
                return []
            
        except Exception as e:
            self.logger.error(f"Error searching entities by browse paths: {e}")
            return []
    
    def _search_entities_by_name_and_platform(self, entity_names: List[str], platforms: List[str], entity_types: List[str], platform_instances: List[str] = None) -> List[Dict[str, Any]]:
        """Search for entities in target DataHub using name and platform (for different platform instances)"""
        try:
            # Create GraphQL query for searching entities by name and platform
            query = """
            query GetEntitiesByNameAndPlatform($input: SearchAcrossEntitiesInput!) {
              searchAcrossEntities(input: $input) {
                start
                count
                total
                searchResults {
                  entity {
                    urn
                    type
                    ... on Dataset {
                      name
                      platform {
                        name
                        properties {
                          displayName
                        }
                      }
                      dataPlatformInstance {
                        instanceId
                        platform {
                          name
                        }
                      }
                      domain {
                        domain {
                          urn
                        }
                      }
                      browsePaths {
                        path
                      }
                      browsePathV2 {
                        path {
                          entity {
                            ... on Container {
                              properties {
                                name
                              }
                            }
                          }
                        }
                      }
                      editableProperties {
                        name
                        description
                      }
                      tags {
                        tags {
                          tag {
                            urn
                          }
                        }
                      }
                      glossaryTerms {
                        terms {
                          term {
                            urn
                            glossaryTermInfo {
                              name
                            }
                          }
                        }
                      }
                      structuredProperties {
                        properties {
                          structuredProperty {
                            urn
                          }
                          values {
                            ... on StringValue {
                              stringValue
                            }
                            ... on NumberValue {
                              numberValue
                            }
                          }
                          valueEntities {
                            urn
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """
            
            # Create OR filters for each entity name combined with platform
            or_filters = []
            
            # Note: Platform filtering via GraphQL orFilters doesn't seem to work reliably
            # So we'll do broader search and filter manually on client side
            # if platforms:
            #     or_filters.append({
            #         "and": [
            #             {
            #                 "field": "platform",
            #                 "condition": "EQUAL",
            #                 "values": platforms,
            #                 "negated": False
            #             }
            #         ]
            #     })
            
            # Add platform instance filter
            # Note: Many target environments may not have platform instances set
            # so we'll match by name/platform primarily and not filter by platform instance
            if platform_instances:
                self.logger.info(f"Source entities have platform instances: {platform_instances}")
                self.logger.info("Note: Target entities may not have platform instances - will match by name/platform primarily")
                # Don't add platformInstance filter as it may exclude valid matches
                # or_filters.append({
                #     "and": [
                #         {
                #             "field": "platformInstance",
                #             "condition": "EQUAL",
                #             "values": platform_instances,
                #             "negated": False
                #         }
                #     ]
                # })
            
            # Use a broad search to get all entities from the platform and filter manually
            # This handles cases where entity names might have schema prefixes or differences
            query_string = "*"  # Search for all entities, rely on platform filter
            
            variables = {
                "input": {
                    "query": query_string,
                    "start": 0,
                    "count": 1000,
                    "orFilters": or_filters
                }
            }
            
            # Add entity types filter if we have them
            if entity_types:
                variables["input"]["types"] = entity_types
            
            self.logger.info(f"Executing name/platform GraphQL query with variables: {variables}")
            
            # Execute the GraphQL query
            result = self.metadata_api.context.graph.execute_graphql(query, variables)
            
            if result and "searchAcrossEntities" in result:
                search_results = result["searchAcrossEntities"]["searchResults"]
                all_entities = []
                
                for search_result in search_results:
                    entity = search_result.get("entity")
                    if entity:
                        # Apply platform filtering manually since GraphQL filter doesn't work reliably
                        entity_platform = entity.get("platform", {}).get("name", "")
                        if not platforms or entity_platform in platforms:
                            all_entities.append(entity)
                        else:
                            self.logger.debug(f"Filtered out entity {entity.get('name', 'N/A')} with platform {entity_platform}")
                
                # Filter entities manually to match source entity names
                matched_entities = []
                self.logger.debug(f"Filtering {len(all_entities)} entities against {len(entity_names)} source names")
                for entity in all_entities:
                    entity_name = entity.get("name", "")
                    entity_urn = entity.get("urn", "")
                    
                    # Check if this entity matches any of our source entity names
                    matched = False
                    for source_name in entity_names:
                        if self._name_matches(entity_name, source_name, entity_urn):
                            matched_entities.append(entity)
                            self.logger.info(f"Matched entity: {entity_name} -> {source_name}")
                            matched = True
                            break
                    
                    if not matched:
                        self.logger.debug(f"No match found for target entity: {entity_name} (URN: {entity_urn})")
                
                self.logger.info(f"Found {len(all_entities)} total entities (after platform filtering), {len(matched_entities)} matched by name/platform search")
                return matched_entities
            else:
                self.logger.warning("No search results found in name/platform GraphQL response")
                return []
            
        except Exception as e:
            self.logger.error(f"Error searching entities by name and platform: {e}")
            return []
    
    def _name_matches(self, entity_name: str, source_name: str, entity_urn: str) -> bool:
        """Check if an entity name matches a source name, handling schema prefixes and variations"""
        if not entity_name or not source_name:
            return False
            
        # Exact match
        if entity_name == source_name:
            return True
            
        # Check if entity name ends with the source name (e.g., rfam._annotated_file matches _annotated_file)
        if entity_name.endswith('.' + source_name):
            return True
        if entity_name.endswith('_' + source_name):
            return True
            
        # Check if entity name starts with the source name (e.g., _annotated_file.something matches _annotated_file)
        if entity_name.startswith(source_name + '.'):
            return True
        if entity_name.startswith(source_name + '_'):
            return True
            
        # Check URN for additional context - extract the table name from URN
        if entity_urn:
            # URN format: urn:li:dataset:(urn:li:dataPlatform:mysql,rfam._annotated_file,PROD)
            try:
                # Extract the table name from the URN
                if ',PROD)' in entity_urn:
                    table_part = entity_urn.split(',PROD)')[0].split(',')[-1]
                    if table_part.endswith('.' + source_name):
                        return True
                    if table_part.endswith('_' + source_name):
                        return True
            except:
                pass
        
        return False
    
    def _show_graphql_query_preview(self, source_entities: List[Dict[str, Any]]):
        """Show what the GraphQL query would look like in dry-run mode"""
        try:
            # Extract browse paths and other info (same logic as the real method)
            browse_paths = set()
            platforms = set()
            entity_types = set()
            mutated_browse_paths = set()
            
            for entity in source_entities:
                if not entity or not isinstance(entity, dict):
                    continue
                    
                # Build browse path including platform instance
                path_parts = []
                
                # Add platform instance as first component if available
                platform_instance = self.safe_get(entity, 'dataPlatformInstance', 'instanceId')
                if platform_instance:
                    path_parts.append(platform_instance)
                
                # Add browse path components
                browse_path = self.extract_browse_path(entity)
                if browse_path:
                    # Extract path components for search
                    browse_path_parts = [part.strip() for part in browse_path.strip('/').split('/') if part.strip()]
                    path_parts.extend(browse_path_parts)
                
                if path_parts:
                    browse_paths.update(path_parts)
                    
                    # Apply platform instance mutations to browse path components
                    mutated_path_parts = []
                    for part in path_parts:
                        # Check if this part is a platform instance that needs mutation
                        if part in self.mutations.get('platform_instances', {}):
                            mutated_part = self.mutations['platform_instances'][part]
                            mutated_path_parts.append(mutated_part)
                        else:
                            mutated_path_parts.append(part)
                    mutated_browse_paths.update(mutated_path_parts)
                
                # Also collect platforms and entity types
                if entity.get('platform', {}).get('name'):
                    platforms.add(entity['platform']['name'])
                if entity.get('type'):
                    entity_types.add(entity['type'])
            
            # Determine which browse paths to use for search
            # Even if mutations were "already applied", we need to search for the mutated browse paths
            # in the target environment since the source entities may still contain original platform instances
            search_browse_paths = list(mutated_browse_paths) if mutated_browse_paths else list(browse_paths)
            
            # Create the same OR filters as the real method
            or_filters = []
            
            if search_browse_paths:
                or_filters.append({
                    "and": [
                        {
                            "field": "browsePathV2",
                            "condition": "CONTAIN",
                            "values": search_browse_paths,
                            "negated": False
                        }
                    ]
                })
                
                or_filters.append({
                    "and": [
                        {
                            "field": "browsePath",
                            "condition": "CONTAIN",
                            "values": search_browse_paths,
                            "negated": False
                        }
                    ]
                })
            
            if platforms:
                or_filters.append({
                    "and": [
                        {
                            "field": "platform",
                            "condition": "EQUAL",
                            "values": list(platforms),
                            "negated": False
                        }
                    ]
                })
            
            variables = {
                "input": {
                    "query": "",
                    "start": 0,
                    "count": 1000,
                    "orFilters": or_filters
                }
            }
            
            if entity_types:
                variables["input"]["types"] = list(entity_types)
            
            self.logger.info("=== DRY RUN: GraphQL Query Preview ===")
            self.logger.info(f"Query: searchAcrossEntities")
            if mutated_browse_paths:
                self.logger.info(f"Original browse path components: {list(browse_paths)}")
                self.logger.info(f"Mutated browse path components: {list(mutated_browse_paths)}")
            else:
                self.logger.info(f"Browse path components: {list(browse_paths)}")
            self.logger.info(f"Platforms: {list(platforms)}")
            self.logger.info(f"Entity types: {list(entity_types)}")
            self.logger.info(f"Variables: {variables}")
            self.logger.info("=== End Query Preview ===")
            
        except Exception as e:
            self.logger.error(f"Error showing GraphQL preview: {e}")
    
    def match_entities(self, source_entities: List[Dict[str, Any]], target_entities: List[Dict[str, Any]]) -> List[EntityMatch]:
        """Match source entities with target entities based on browse path and name"""
        matches = []
        
        # Create lookup for target entities
        target_lookup = {}
        for target_entity in target_entities:
            # Skip None or empty entities
            if not target_entity or not isinstance(target_entity, dict):
                self.logger.warning(f"Skipping invalid target entity: {target_entity}")
                continue
                
            browse_path = self.extract_browse_path(target_entity)
            name = self.extract_entity_name(target_entity)
            entity_type = target_entity.get('type', '')
            
            key = f"{entity_type}:{browse_path}:{name}".lower()
            target_lookup[key] = target_entity
            
            # DEBUG: Log target entity details
            self.logger.debug(f"Target entity - URN: {target_entity.get('urn')}, Name: {name}, Browse path: {browse_path}, Key: {key}")
        
        # Match source entities
        for source_entity in source_entities:
            # Skip None or empty entities
            if not source_entity or not isinstance(source_entity, dict):
                self.logger.warning(f"Skipping invalid source entity: {source_entity}")
                continue
            
            source_name = self.extract_entity_name(source_entity)
            source_type = source_entity.get('type', '')
            
            # Calculate what the target browse path should be (with mutations applied)
            source_browse_path = self.extract_browse_path(source_entity)
            
            # Build the expected target entity name with platform instance
            platform_instance = self.safe_get(source_entity, 'dataPlatformInstance', 'instanceId')
            if platform_instance:
                # For target matching, we need to find entities with the CURRENT platform instance (abc)
                # The mutation will be applied in the MCP, not in the matching
                current_platform_instance = platform_instance  # Keep original for matching
                
                # Build expected target entity name (with original platform instance)
                if source_browse_path:
                    browse_path_parts = [part.strip() for part in source_browse_path.strip('/').split('/') if part.strip()]
                    target_entity_name = f"{current_platform_instance}.{'.'.join(browse_path_parts)}.{source_name}"
                else:
                    target_entity_name = f"{current_platform_instance}.{source_name}"
            else:
                target_entity_name = source_name
            
            # Try to match using the expected target entity name
            target_key = f"{source_type}::{target_entity_name}".lower()  # Empty browse path since target entities don't have browse paths
            
            # DEBUG: Log matching attempt
            self.logger.debug(f"Source entity - Name: {source_name}, Type: {source_type}, Target key: {target_key}")
            
            if target_key in target_lookup:
                target_entity = target_lookup[target_key]
                match = EntityMatch(
                    source_entity=source_entity,
                    target_urn=target_entity['urn'],
                    target_name=self.extract_entity_name(target_entity),
                    browse_path=self.extract_browse_path(target_entity),
                    confidence=1.0  # Exact match
                )
                matches.append(match)
                self.logger.info(f"Matched: {source_name} -> {target_entity['urn']} (platform instance match)")
            else:
                # Fallback: try matching by simple name only
                fallback_matches = [entity for key, entity in target_lookup.items() if key.endswith(f":{source_name}".lower())]
                
                if fallback_matches:
                    # Take the first match if multiple found
                    target_entity = fallback_matches[0]
                    match = EntityMatch(
                        source_entity=source_entity,
                        target_urn=target_entity['urn'],
                        target_name=self.extract_entity_name(target_entity),
                        browse_path=self.extract_browse_path(target_entity),
                        confidence=0.8  # Lower confidence for name-only match
                    )
                    matches.append(match)
                    self.logger.info(f"Fallback matched: {source_name} -> {target_entity['urn']} (name-only match)")
                else:
                    self.logger.warning(f"No match found for: {source_type}:{source_browse_path}:{source_name} (expected target: {target_entity_name})")
        
        self.logger.info(f"Found {len(matches)} entity matches out of {len(source_entities)} source entities")
        return matches
    
    def apply_urn_mutations(self, urn: str) -> str:
        """Apply environment-specific mutations to URNs"""
        # Skip mutations if they were already applied during export
        if self.mutations_already_applied:
            self.logger.debug(f"Skipping mutation for {urn} - already applied during export")
            return urn
            
        if not self.mutations:
            return urn
        
        mutated_urn = urn
        
        # Apply mutations based on configuration
        for mutation_type, mutation_config in self.mutations.items():
            if mutation_type == 'platform_instance_mapping':
                # Handle platform instance mutations
                for from_instance, to_instance in mutation_config.items():
                    if from_instance in mutated_urn:
                        mutated_urn = mutated_urn.replace(from_instance, to_instance)
            
            elif mutation_type == 'custom_properties':
                # Handle custom property mutations (URN transformations)
                for prop_key, prop_value in mutation_config.items():
                    if prop_key in mutated_urn:
                        mutated_urn = mutated_urn.replace(prop_key, prop_value)
        
        return mutated_urn
    
    def apply_urn_mutations_for_mcp(self, urn: str) -> str:
        """Apply environment-specific mutations to URNs for MCP generation (always applies mutations)"""
        if not self.mutations:
            return urn
        
        mutated_urn = urn
        
        # Apply mutations based on configuration
        for mutation_type, mutation_config in self.mutations.items():
            if mutation_type == 'platform_instances':
                # Handle platform instance mutations
                for from_instance, to_instance in mutation_config.items():
                    if from_instance in mutated_urn:
                        mutated_urn = mutated_urn.replace(from_instance, to_instance)
                        self.logger.debug(f"Applied platform instance mutation: {from_instance} -> {to_instance}")
            
            elif mutation_type == 'custom_properties':
                # Handle custom property mutations (URN transformations)
                for prop_key, prop_value in mutation_config.items():
                    if prop_key in mutated_urn:
                        mutated_urn = mutated_urn.replace(prop_key, prop_value)
        
        return mutated_urn
    
    def generate_mcps_for_match(self, match: EntityMatch) -> List[MCPTask]:
        """Generate MCPs for a matched entity"""
        if not match:
            self.logger.warning(f"Skipping None match")
            return []
        
        if not match.source_entity:
            self.logger.warning(f"Skipping match with None source_entity: {match}")
            return []
            
        if not isinstance(match.source_entity, dict):
            self.logger.warning(f"Skipping match with invalid source_entity type: {type(match.source_entity)}")
            return []
            
        mcps = []
        source_entity = match.source_entity
        target_urn = match.target_urn
        
        # Additional debug logging
        self.logger.debug(f"Initial source_entity assignment - type: {type(source_entity)}, is None: {source_entity is None}")
        if source_entity is None:
            self.logger.error(f"source_entity is None after assignment from match.source_entity")
            return []
        
        # Use the mutated URN (apply platform instance mutations to the target URN)
        # This creates the URN with the mutated platform instance
        mutated_entity_urn = self.apply_urn_mutations_for_mcp(target_urn)
        
        self.logger.info(f"Generating MCPs for entity: {target_urn} -> {mutated_entity_urn}")
        self.logger.debug(f"After URN assignment - source_entity type: {type(source_entity)}, is None: {source_entity is None}")
        
        # DEBUG: Log what metadata is actually present in this entity
        entity_name = source_entity.get('name', 'Unknown')
        self.logger.info(f"DEBUG: Entity {entity_name} has keys: {list(source_entity.keys())}")
        
        has_tags = bool(self.safe_get(source_entity, 'tags', 'tags'))
        has_glossary_terms = bool(self.safe_get(source_entity, 'glossaryTerms', 'terms'))
        has_domain = bool(self.safe_get(source_entity, 'domain', 'domain'))
        has_structured_props = bool(self.safe_get(source_entity, 'structuredProperties', 'properties'))
        has_schema_fields = bool(self.safe_get(source_entity, 'schemaMetadata', 'fields'))
        
        self.logger.info(f"DEBUG: Entity {entity_name} metadata check:")
        self.logger.info(f"  - Has tags: {has_tags}")
        self.logger.info(f"  - Has glossary terms: {has_glossary_terms}")
        self.logger.info(f"  - Has domain: {has_domain}")
        self.logger.info(f"  - Has structured properties: {has_structured_props}")
        self.logger.info(f"  - Has schema fields: {has_schema_fields}")
        
        if has_tags:
            self.logger.info(f"  - Tags structure: {source_entity.get('tags', {})}")
        if has_glossary_terms:
            self.logger.info(f"  - Glossary terms structure: {source_entity.get('glossaryTerms', {})}")
        if has_domain:
            self.logger.info(f"  - Domain structure: {source_entity.get('domain', {})}")
        if has_structured_props:
            self.logger.info(f"  - Structured properties structure: {self.safe_get(source_entity, 'structuredProperties', default={})}")
        
        # Process tags
        tags_list = self.safe_get(source_entity, 'tags', 'tags')
        if tags_list:
            tag_associations = []
            source_urns = []
            
            for tag in tags_list:
                tag_urn = self.safe_get(tag, 'tag', 'urn')
                if tag_urn:
                    mutated_tag_urn = self.apply_urn_mutations(tag_urn)
                    source_urns.append(tag_urn)
                    tag_associations.append(TagAssociationClass(tag=mutated_tag_urn))
            
            if tag_associations:
                mcps.append(MCPTask(
                    entity_urn=mutated_entity_urn,
                    mcp_type='globalTags',
                    aspect_data={'tags': tag_associations},
                    source_urns=source_urns
                ))
        
        # Process glossary terms
        terms_list = self.safe_get(source_entity, 'glossaryTerms', 'terms')
        if terms_list:
            term_associations = []
            source_urns = []
            
            for term in terms_list:
                term_urn = self.safe_get(term, 'term', 'urn')
                if term_urn:
                    mutated_term_urn = self.apply_urn_mutations(term_urn)
                    source_urns.append(term_urn)
                    term_associations.append(GlossaryTermAssociationClass(urn=mutated_term_urn))
            
            if term_associations:
                mcps.append(MCPTask(
                    entity_urn=mutated_entity_urn,
                    mcp_type='glossaryTerms',
                    aspect_data={'terms': term_associations},
                    source_urns=source_urns
                ))
        
        # Process domain
        domain_urn = self.safe_get(source_entity, 'domain', 'domain', 'urn')
        if domain_urn:
            mutated_domain_urn = self.apply_urn_mutations(domain_urn)
            
            mcps.append(MCPTask(
                entity_urn=mutated_entity_urn,
                mcp_type='domains',
                aspect_data={'domains': [mutated_domain_urn]},
                source_urns=[domain_urn]
            ))
        
        # Process structured properties
        self.logger.debug(f"About to check structured properties - source_entity type: {type(source_entity)}, is None: {source_entity is None}")
        structured_props = source_entity.get('structuredProperties') if source_entity else None
        self.logger.debug(f"structured_props type: {type(structured_props)}, is None: {structured_props is None}")
        if structured_props and structured_props.get('properties'):
            property_assignments = []
            source_urns = []
            
            for prop in structured_props['properties']:
                prop_urn = prop.get('structuredProperty', {}).get('urn', '')
                if prop_urn:
                    mutated_prop_urn = self.apply_urn_mutations(prop_urn)
                    source_urns.append(prop_urn)
                    
                    property_assignments.append(StructuredPropertyValueAssignmentClass(
                        propertyUrn=mutated_prop_urn,
                        values=prop.get('values', [])
                    ))
            
            if property_assignments:
                mcps.append(MCPTask(
                    entity_urn=mutated_entity_urn,
                    mcp_type='structuredProperties',
                    aspect_data={'properties': property_assignments},
                    source_urns=source_urns
                ))
        
        # Process schema field metadata (tags and glossary terms on fields)
        if source_entity.get('schemaMetadata', {}).get('fields'):
            field_mcps = self._process_schema_field_metadata(source_entity, mutated_entity_urn)
            mcps.extend(field_mcps)
        
        return mcps
    
    def _process_schema_field_metadata(self, source_entity: Dict[str, Any], mutated_entity_urn: str) -> List[MCPTask]:
        """Process schema field metadata (tags and glossary terms on fields)"""
        mcps = []
        
        for field in source_entity['schemaMetadata']['fields']:
            self.logger.debug(f"Processing field: type={type(field)}, is None: {field is None}")
            if not field or not isinstance(field, dict):
                self.logger.warning(f"Skipping invalid field: {field}")
                continue
                
            field_path = field.get('fieldPath', '')
            schema_field_entity = field.get('schemaFieldEntity', {})
            
            # Process field tags
            self.logger.debug(f"About to check field tags - field type: {type(field)}, is None: {field is None}")
            field_tags = field.get('tags') if field else None
            self.logger.debug(f"field_tags type: {type(field_tags)}, is None: {field_tags is None}")
            if field_tags and field_tags.get('tags'):
                tag_associations = []
                source_urns = []
                
                for tag in field_tags['tags']:
                    tag_urn = tag.get('tag', {}).get('urn', '')
                    if tag_urn:
                        mutated_tag_urn = self.apply_urn_mutations(tag_urn)
                        source_urns.append(tag_urn)
                        tag_associations.append(TagAssociationClass(tag=mutated_tag_urn))
                
                if tag_associations:
                    mcps.append(MCPTask(
                        entity_urn=mutated_entity_urn,
                        mcp_type='editableSchemaFieldInfo',
                        aspect_data={
                            'fieldPath': field_path,
                            'globalTags': {'tags': tag_associations}
                        },
                        source_urns=source_urns
                    ))
            
            # Process field glossary terms
            field_glossary_terms = field.get('glossaryTerms') if field else None
            if field_glossary_terms and field_glossary_terms.get('terms'):
                term_associations = []
                source_urns = []
                
                for term in field_glossary_terms['terms']:
                    term_urn = term.get('term', {}).get('urn', '')
                    if term_urn:
                        mutated_term_urn = self.apply_urn_mutations(term_urn)
                        source_urns.append(term_urn)
                        term_associations.append(GlossaryTermAssociationClass(urn=mutated_term_urn))
                
                if term_associations:
                    mcps.append(MCPTask(
                        entity_urn=mutated_entity_urn,
                        mcp_type='editableSchemaFieldInfo',
                        aspect_data={
                            'fieldPath': field_path,
                            'glossaryTerms': {'terms': term_associations}
                        },
                        source_urns=source_urns
                    ))
        
        return mcps
    
    def create_mcp_from_task(self, task: MCPTask) -> MetadataChangeProposalWrapper:
        """Create an MCP from an MCP task"""
        if task.mcp_type == 'globalTags':
            aspect = GlobalTagsClass(tags=task.aspect_data['tags'])
        elif task.mcp_type == 'glossaryTerms':
            aspect = GlossaryTermsClass(terms=task.aspect_data['terms'])
        elif task.mcp_type == 'domains':
            aspect = DomainsClass(domains=task.aspect_data['domains'])
        elif task.mcp_type == 'structuredProperties':
            aspect = StructuredPropertiesClass(properties=task.aspect_data['properties'])
        elif task.mcp_type == 'editableSchemaFieldInfo':
            # Handle field-level metadata
            # This requires more complex handling for schema field updates
            # For now, return None and handle separately
            return None
        else:
            self.logger.warning(f"Unknown MCP type: {task.mcp_type}")
            return None
        
        return MetadataChangeProposalWrapper(
            entityUrn=task.entity_urn,
            aspect=aspect,
            changeType=ChangeTypeClass.UPSERT
        )
    
    def emit_mcps(self, mcps: List[MetadataChangeProposalWrapper], output_dir: str = None):
        """Emit MCPs to DataHub or save to files"""
        if self.dry_run:
            if output_dir:
                self._save_mcps_to_files(mcps, output_dir)
            else:
                self.logger.info(f"DRY RUN: Would emit {len(mcps)} MCPs")
                for i, mcp in enumerate(mcps[:5]):  # Show first 5 as preview
                    self.logger.info(f"MCP {i+1}: {mcp.entityUrn} -> {mcp.aspect.__class__.__name__}")
        else:
            # Emit to DataHub
            for mcp in mcps:
                try:
                    self.metadata_api.context.graph.emit_mcp(mcp)
                    self.logger.debug(f"Emitted MCP for {mcp.entityUrn}")
                except Exception as e:
                    self.logger.error(f"Failed to emit MCP for {mcp.entityUrn}: {e}")
    
    def _save_mcps_to_files(self, mcps: List[MetadataChangeProposalWrapper], output_dir: str):
        """Save MCPs to JSON files for review"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        for i, mcp in enumerate(mcps):
            filename = f"mcp_{i+1}_{mcp.entityUrn.split('/')[-1]}.json"
            filepath = output_path / filename
            
            # Convert MCP to serializable dict
            mcp_dict = {
                'entityUrn': mcp.entityUrn,
                'aspectName': mcp.aspect.__class__.__name__,
                'changeType': mcp.changeType,
                'aspect': mcp.aspect.__dict__ if hasattr(mcp.aspect, '__dict__') else str(mcp.aspect)
            }
            
            with open(filepath, 'w') as f:
                json.dump(mcp_dict, f, indent=2)
        
        self.logger.info(f"Saved {len(mcps)} MCPs to {output_dir}")
    
    def process_migration(self, exported_entities_file: str, output_dir: str = None) -> Dict[str, Any]:
        """Main method to process metadata migration"""
        try:
            # Load exported entities
            source_entities = self.load_exported_entities(exported_entities_file)
            
            # Extract unique platforms and entity types
            platforms = set()
            entity_types = set()
            for i, entity in enumerate(source_entities):
                self.logger.debug(f"Processing entity {i}: type={type(entity)}, value={entity}")
                
                # Skip None or empty entities
                if not entity or not isinstance(entity, dict):
                    self.logger.warning(f"Skipping invalid entity {i}: {entity}")
                    continue
                
                # Double check to ensure entity is not None before accessing
                if entity is None:
                    self.logger.error(f"Entity {i} is None after validation!")
                    continue
                    
                platform_info = entity.get('platform') or {}
                if platform_info.get('name'):
                    platforms.add(platform_info['name'])
                if entity.get('type'):
                    entity_types.add(entity['type'])
            
            # Fetch target entities based on browse paths from source entities
            target_entities = self.fetch_target_entities(source_entities)
            
            # Match entities
            matches = self.match_entities(source_entities, target_entities)
            
            # Generate MCPs
            all_mcps = []
            all_tasks = []
            
            for match in matches:
                tasks = self.generate_mcps_for_match(match)
                all_tasks.extend(tasks)
                
                for task in tasks:
                    mcp = self.create_mcp_from_task(task)
                    if mcp:
                        all_mcps.append(mcp)
            
            # Emit MCPs
            self.emit_mcps(all_mcps, output_dir)
            
            # Return summary
            return {
                'source_entities': len(source_entities),
                'target_entities': len(target_entities),
                'matches': len(matches),
                'mcps_generated': len(all_mcps),
                'tasks_generated': len(all_tasks),
                'platforms': list(platforms),
                'entity_types': list(entity_types)
            }
            
        except Exception as e:
            import traceback
            self.logger.error(f"Migration processing failed: {e}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise

def main():
    parser = argparse.ArgumentParser(description='Process metadata migration between DataHub environments')
    parser.add_argument('--input', required=True, help='Input JSON file with exported entities')
    parser.add_argument('--target-env', required=True, help='Target environment name')
    parser.add_argument('--output-dir', help='Output directory for generated MCPs (dry run)')
    parser.add_argument('--dry-run', action='store_true', help='Generate MCPs without emitting them')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        # Set up verbose logging
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Note: Mutations are now included in the export file, no need for separate mutations file
    # The mutation configuration is in the export metadata and mutations are already applied
    
    # Initialize processor
    processor = MetadataMigrationProcessor(
        target_environment=args.target_env,
        mutations={},  # Empty mutations since they're already applied
        dry_run=args.dry_run
    )
    
    # Process migration
    try:
        result = processor.process_migration(args.input, args.output_dir)
        
        print("\n" + "="*50)
        print("MIGRATION PROCESSING SUMMARY")
        print("="*50)
        print(f"Source entities: {result['source_entities']}")
        print(f"Target entities: {result['target_entities']}")
        print(f"Entity matches: {result['matches']}")
        print(f"MCPs generated: {result['mcps_generated']}")
        print(f"Tasks generated: {result['tasks_generated']}")
        print(f"Platforms: {', '.join(result['platforms'])}")
        print(f"Entity types: {', '.join(result['entity_types'])}")
        
        # Show mutation information from export
        if hasattr(processor, 'export_metadata') and processor.export_metadata:
            metadata = processor.export_metadata
            print(f"Export environment: {metadata.get('environment', 'unknown')}")
            print(f"Mutations applied: {metadata.get('mutations_applied', 'unknown')}")
            print(f"Export timestamp: {metadata.get('export_timestamp', 'unknown')}")
        
        if args.dry_run:
            print(f"\nDRY RUN: MCPs saved to {args.output_dir or 'logs'}")
        else:
            print(f"\nMCPs emitted to {args.target_env} environment")
        
        print("="*50)
        
    except Exception as e:
        import traceback
        logging.error(f"Migration failed: {e}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == '__main__':
    main() 