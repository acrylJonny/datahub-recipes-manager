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
                if 'entities' in data:
                    entities = data['entities']
                elif 'export_data' in data:
                    entities = data['export_data']
                else:
                    entities = [data]  # Single entity
            else:
                entities = data  # List of entities
            
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
    
    def fetch_target_entities(self, platforms: List[str], entity_types: List[str]) -> List[Dict[str, Any]]:
        """Fetch entities from target environment"""
        if self.dry_run:
            self.logger.info("DRY RUN: Would fetch target entities")
            return []
        
        try:
            target_entities = []
            
            for platform in platforms:
                for entity_type in entity_types:
                    query = f"platform:{platform}"
                    
                    # Use the graph client to search
                    results = self.metadata_api.context.graph.search_entities(
                        entity_types=[entity_type.lower()],
                        query=query,
                        start=0,
                        count=10000  # Large count to get all entities
                    )
                    
                    if results:
                        for entity in results:
                            if entity:
                                target_entities.append(entity)
            
            self.logger.info(f"Fetched {len(target_entities)} entities from target environment")
            return target_entities
            
        except Exception as e:
            self.logger.error(f"Failed to fetch target entities: {e}")
            return []
    
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
        
        # Match source entities
        for source_entity in source_entities:
            # Skip None or empty entities
            if not source_entity or not isinstance(source_entity, dict):
                self.logger.warning(f"Skipping invalid source entity: {source_entity}")
                continue
            source_browse_path = self.extract_browse_path(source_entity)
            source_name = self.extract_entity_name(source_entity)
            source_type = source_entity.get('type', '')
            
            key = f"{source_type}:{source_browse_path}:{source_name}".lower()
            
            if key in target_lookup:
                target_entity = target_lookup[key]
                match = EntityMatch(
                    source_entity=source_entity,
                    target_urn=target_entity['urn'],
                    target_name=self.extract_entity_name(target_entity),
                    browse_path=source_browse_path,
                    confidence=1.0  # Exact match
                )
                matches.append(match)
                self.logger.debug(f"Matched: {source_name} -> {target_entity['urn']}")
            else:
                self.logger.warning(f"No match found for: {source_type}:{source_browse_path}:{source_name}")
        
        self.logger.info(f"Found {len(matches)} entity matches out of {len(source_entities)} source entities")
        return matches
    
    def apply_urn_mutations(self, urn: str) -> str:
        """Apply environment-specific mutations to URNs"""
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
    
    def generate_mcps_for_match(self, match: EntityMatch) -> List[MCPTask]:
        """Generate MCPs for a matched entity"""
        if not match or not match.source_entity or not isinstance(match.source_entity, dict):
            self.logger.warning(f"Skipping invalid match: {match}")
            return []
            
        mcps = []
        source_entity = match.source_entity
        target_urn = match.target_urn
        
        # Process tags
        tags_list = self.safe_get(source_entity, 'tags', 'tags')
        if tags_list:
            tag_associations = []
            source_urns = []
            
            for tag in tags_list:
                tag_urn = self.safe_get(tag, 'tag', 'urn', '')
                if tag_urn:
                    mutated_tag_urn = self.apply_urn_mutations(tag_urn)
                    source_urns.append(tag_urn)
                    tag_associations.append(TagAssociationClass(tag=mutated_tag_urn))
            
            if tag_associations:
                mcps.append(MCPTask(
                    entity_urn=target_urn,
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
                term_urn = self.safe_get(term, 'term', 'urn', '')
                if term_urn:
                    mutated_term_urn = self.apply_urn_mutations(term_urn)
                    source_urns.append(term_urn)
                    term_associations.append(GlossaryTermAssociationClass(urn=mutated_term_urn))
            
            if term_associations:
                mcps.append(MCPTask(
                    entity_urn=target_urn,
                    mcp_type='glossaryTerms',
                    aspect_data={'terms': term_associations},
                    source_urns=source_urns
                ))
        
        # Process domain
        domain_urn = self.safe_get(source_entity, 'domain', 'urn')
        if domain_urn:
            mutated_domain_urn = self.apply_urn_mutations(domain_urn)
            
            mcps.append(MCPTask(
                entity_urn=target_urn,
                mcp_type='domains',
                aspect_data={'domains': [mutated_domain_urn]},
                source_urns=[domain_urn]
            ))
        
        # Process structured properties
        if source_entity.get('structuredProperties', {}).get('properties'):
            property_assignments = []
            source_urns = []
            
            for prop in source_entity['structuredProperties']['properties']:
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
                    entity_urn=target_urn,
                    mcp_type='structuredProperties',
                    aspect_data={'properties': property_assignments},
                    source_urns=source_urns
                ))
        
        # Process schema field metadata (tags and glossary terms on fields)
        if source_entity.get('schemaMetadata', {}).get('fields'):
            field_mcps = self._process_schema_field_metadata(source_entity, target_urn)
            mcps.extend(field_mcps)
        
        return mcps
    
    def _process_schema_field_metadata(self, source_entity: Dict[str, Any], target_urn: str) -> List[MCPTask]:
        """Process schema field metadata (tags and glossary terms on fields)"""
        mcps = []
        
        for field in source_entity['schemaMetadata']['fields']:
            field_path = field.get('fieldPath', '')
            schema_field_entity = field.get('schemaFieldEntity', {})
            
            # Process field tags
            if field.get('tags', {}).get('tags'):
                tag_associations = []
                source_urns = []
                
                for tag in field['tags']['tags']:
                    tag_urn = tag.get('tag', {}).get('urn', '')
                    if tag_urn:
                        mutated_tag_urn = self.apply_urn_mutations(tag_urn)
                        source_urns.append(tag_urn)
                        tag_associations.append(TagAssociationClass(tag=mutated_tag_urn))
                
                if tag_associations:
                    mcps.append(MCPTask(
                        entity_urn=target_urn,
                        mcp_type='editableSchemaFieldInfo',
                        aspect_data={
                            'fieldPath': field_path,
                            'globalTags': {'tags': tag_associations}
                        },
                        source_urns=source_urns
                    ))
            
            # Process field glossary terms
            if field.get('glossaryTerms', {}).get('terms'):
                term_associations = []
                source_urns = []
                
                for term in field['glossaryTerms']['terms']:
                    term_urn = term.get('term', {}).get('urn', '')
                    if term_urn:
                        mutated_term_urn = self.apply_urn_mutations(term_urn)
                        source_urns.append(term_urn)
                        term_associations.append(GlossaryTermAssociationClass(urn=mutated_term_urn))
                
                if term_associations:
                    mcps.append(MCPTask(
                        entity_urn=target_urn,
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
            
            # Fetch target entities
            target_entities = self.fetch_target_entities(list(platforms), list(entity_types))
            
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
    parser.add_argument('--mutations-file', help='JSON file with environment mutations')
    parser.add_argument('--output-dir', help='Output directory for generated MCPs (dry run)')
    parser.add_argument('--dry-run', action='store_true', help='Generate MCPs without emitting them')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        # Set up verbose logging
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Load mutations if provided
    mutations = {}
    if args.mutations_file:
        try:
            with open(args.mutations_file, 'r') as f:
                mutations = json.load(f)
        except Exception as e:
            logging.error(f"Failed to load mutations file: {e}")
            sys.exit(1)
    
    # Initialize processor
    processor = MetadataMigrationProcessor(
        target_environment=args.target_env,
        mutations=mutations,
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