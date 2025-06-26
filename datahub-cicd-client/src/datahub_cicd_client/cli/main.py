#!/usr/bin/env python3
"""
DataHub CI/CD CLI - Command Line Interface for DataHub operations

This CLI provides a comprehensive interface for DataHub operations suitable for CI/CD pipelines.
It uses the modular DataHub client architecture with direct service imports.
"""

import argparse
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.connection import DataHubConnection
from datahub_cicd_client.services.data_products import DataProductService
from datahub_cicd_client.services.domains import DomainService
from datahub_cicd_client.services.glossary import GlossaryService
from datahub_cicd_client.services.ingestion import IngestionService
from datahub_cicd_client.services.properties import StructuredPropertiesService
from datahub_cicd_client.services.tags import TagService


class DataHubCLIClient:
    """
    Simple client wrapper for CLI operations that uses individual services.
    """

    def __init__(self, server_url: str, token: str, verify_ssl: bool = True, timeout: int = 30):
        """Initialize the CLI client with individual services."""
        self.connection = DataHubConnection(
            server_url=server_url,
            token=token,
            verify_ssl=verify_ssl,
            timeout=timeout
        )

        # Initialize services
        self.tag_service = TagService(self.connection)
        self.domain_service = DomainService(self.connection)
        self.properties_service = StructuredPropertiesService(self.connection)
        self.glossary_service = GlossaryService(self.connection)
        self.data_product_service = DataProductService(self.connection)
        self.ingestion_service = IngestionService(self.connection)

    def test_connection(self) -> bool:
        """Test connection to DataHub."""
        return self.connection.test_connection()

    # Tag methods
    def list_tags(self, query="*", start=0, count=100):
        return self.tag_service.list_tags(query=query, start=start, count=count)

    def get_tag(self, tag_urn: str):
        return self.tag_service.get_tag(tag_urn)

    def create_tag(self, tag_id: str, name: str, description: str = ""):
        return self.tag_service.create_tag(tag_id, name, description)

    def delete_tag(self, tag_urn: str):
        return self.tag_service.delete_tag(tag_urn)

    def set_tag_color(self, tag_urn: str, color_hex: str):
        return self.tag_service.set_tag_color(tag_urn, color_hex)

    def add_tag_to_entity(self, entity_urn: str, tag_urn: str, color_hex: str = None):
        return self.tag_service.add_tag_to_entity(entity_urn, tag_urn, color_hex)

    # Domain methods
    def list_domains(self, query="*", start=0, count=100):
        return self.domain_service.list_domains(query=query, start=start, count=count)

    def get_domain(self, domain_urn: str):
        return self.domain_service.get_domain(domain_urn)

    def create_domain(self, domain_id: str, name: str, description: str = "", parent_domain_urn: str = None):
        return self.domain_service.create_domain(domain_id, name, description, parent_domain_urn)

    def delete_domain(self, domain_urn: str):
        return self.domain_service.delete_domain(domain_urn)

    def find_entities_with_domain(self, domain_urn: str, start: int = 0, count: int = 50):
        return self.domain_service.find_entities_with_domain(domain_urn, start, count)

    # Properties methods
    def list_structured_properties(self, query="*", start=0, count=100):
        return self.properties_service.list_structured_properties(query=query, start=start, count=count)

    def get_structured_property(self, property_urn: str):
        return self.properties_service.get_structured_property(property_urn)

    def create_structured_property(self, display_name: str, description: str = "",
                                 value_type: str = "STRING", cardinality: str = "SINGLE",
                                 entity_types: List[str] = None, allowed_values: List[Any] = None,
                                 qualified_name: str = None, **kwargs):
        return self.properties_service.create_structured_property(
            display_name=display_name,
            description=description,
            value_type=value_type,
            cardinality=cardinality,
            entity_types=entity_types,
            allowed_values=allowed_values,
            qualified_name=qualified_name,
            **kwargs
        )

    def delete_structured_property(self, property_urn: str):
        return self.properties_service.delete_structured_property(property_urn)

    # Glossary methods
    def list_glossary_nodes(self, query=None, count=100, start=0):
        return self.glossary_service.list_glossary_nodes(query=query, count=count, start=start)

    def list_glossary_terms(self, node_urn=None, query=None, count=100, start=0):
        return self.glossary_service.list_glossary_terms(node_urn=node_urn, query=query, count=count, start=start)

    def get_glossary_node(self, node_urn):
        return self.glossary_service.get_glossary_node(node_urn)

    def get_glossary_term(self, term_urn):
        return self.glossary_service.get_glossary_term(term_urn)

    def create_glossary_node(self, node_id, name, description="", parent_urn=None):
        return self.glossary_service.create_glossary_node(node_id, name, description, parent_urn)

    def create_glossary_term(self, term_id, name, description="", parent_node_urn=None, term_source="INTERNAL"):
        return self.glossary_service.create_glossary_term(term_id, name, description, parent_node_urn, term_source)

    def delete_glossary_node(self, node_urn: str):
        return self.glossary_service.delete_glossary_node(node_urn)

    def delete_glossary_term(self, term_urn: str):
        return self.glossary_service.delete_glossary_term(term_urn)

    def get_comprehensive_glossary_data(self, query="*", start=0, count=100):
        return self.glossary_service.get_comprehensive_glossary_data(query=query, start=start, count=count)

    # Data Products methods
    def list_data_products(self, query="*", start=0, count=100):
        return self.data_product_service.list_data_products(query=query, start=start, count=count)

    def get_data_product(self, data_product_urn: str):
        return self.data_product_service.get_data_product(data_product_urn)

    def create_data_product(self, product_id: str, name: str, description: str = "", external_url: Optional[str] = None):
        return self.data_product_service.create_data_product(product_id, name, description, external_url)

    def delete_data_product(self, product_urn: str):
        return self.data_product_service.delete_data_product(product_urn)

    # Ingestion methods
    def list_ingestion_sources(self):
        return self.ingestion_service.list_ingestion_sources()

    def get_ingestion_source(self, source_id):
        return self.ingestion_service.get_ingestion_source(source_id)

    def create_ingestion_source(self, recipe: Dict[str, Any], **kwargs):
        return self.ingestion_service.create_ingestion_source(recipe, **kwargs)

    def delete_ingestion_source(self, source_id: str):
        return self.ingestion_service.delete_ingestion_source(source_id)

    def trigger_ingestion(self, ingestion_source_id: str):
        return self.ingestion_service.execute_ingestion_source(ingestion_source_id)

    def get_ingestion_executions(self, source_id: str, start: int = 0, count: int = 10):
        return self.ingestion_service.get_ingestion_executions(source_id, start, count)

    def get_ingestion_source_stats(self, source_id: str):
        return self.ingestion_service.get_ingestion_source_stats(source_id)

    def patch_ingestion_source(self, source_id: str, **kwargs):
        return self.ingestion_service.patch_ingestion_source(source_id, **kwargs)


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def get_client_from_env() -> DataHubCLIClient:
    """Create DataHub client from environment variables."""
    server_url = os.getenv('DATAHUB_SERVER_URL', 'http://localhost:8080')
    token = os.getenv('DATAHUB_TOKEN')
    verify_ssl = os.getenv('DATAHUB_VERIFY_SSL', 'true').lower() == 'true'

    if not token:
        print("Error: DATAHUB_TOKEN environment variable is required")
        sys.exit(1)

    return DataHubCLIClient(
        server_url=server_url,
        token=token,
        verify_ssl=verify_ssl
    )


def output_result(result: Any, output_format: str = 'json'):
    """Output result in specified format."""
    if output_format == 'json':
        print(json.dumps(result, indent=2, default=str))
    elif output_format == 'table':
        # Simple table format for lists
        if isinstance(result, list) and result:
            if isinstance(result[0], dict):
                # Print headers
                headers = result[0].keys()
                print('\t'.join(headers))
                # Print rows
                for item in result:
                    print('\t'.join(str(item.get(h, '')) for h in headers))
            else:
                for item in result:
                    print(item)
        else:
            print(result)
    else:
        print(result)


# Tag Commands
def cmd_tags_list(client: DataHubCLIClient, args):
    """List tags."""
    result = client.list_tags(query=args.query, start=args.start, count=args.count)
    output_result(result, args.format)


def cmd_tags_get(client: DataHubCLIClient, args):
    """Get a specific tag."""
    result = client.get_tag(args.urn)
    if result:
        output_result(result, args.format)
    else:
        print(f"Tag not found: {args.urn}")
        sys.exit(1)


def cmd_tags_create(client: DataHubCLIClient, args):
    """Create a new tag."""
    result = client.create_tag(args.id, args.name, args.description or "")
    if result:
        print(f"Created tag with URN: {result}")
    else:
        print("Failed to create tag")
        sys.exit(1)


def cmd_tags_delete(client: DataHubCLIClient, args):
    """Delete a tag."""
    result = client.delete_tag(args.urn)
    if result:
        print(f"Successfully deleted tag: {args.urn}")
    else:
        print(f"Failed to delete tag: {args.urn}")
        sys.exit(1)


def cmd_tags_set_color(client: DataHubCLIClient, args):
    """Set tag color."""
    result = client.set_tag_color(args.urn, args.color)
    if result:
        print(f"Successfully set color for tag: {args.urn}")
    else:
        print(f"Failed to set color for tag: {args.urn}")
        sys.exit(1)


def cmd_tags_add_to_entity(client: DataHubCLIClient, args):
    """Add tag to entity."""
    result = client.add_tag_to_entity(args.entity_urn, args.tag_urn, args.color)
    if result:
        print(f"Successfully added tag {args.tag_urn} to entity {args.entity_urn}")
    else:
        print("Failed to add tag to entity")
        sys.exit(1)


# Domain Commands
def cmd_domains_list(client: DataHubCLIClient, args):
    """List domains."""
    result = client.list_domains(query=args.query, start=args.start, count=args.count)
    output_result(result, args.format)


def cmd_domains_get(client: DataHubCLIClient, args):
    """Get a specific domain."""
    result = client.get_domain(args.urn)
    if result:
        output_result(result, args.format)
    else:
        print(f"Domain not found: {args.urn}")
        sys.exit(1)


def cmd_domains_create(client: DataHubCLIClient, args):
    """Create a new domain."""
    result = client.create_domain(args.id, args.name, args.description or "", args.parent)
    if result:
        print(f"Created domain with URN: {result}")
    else:
        print("Failed to create domain")
        sys.exit(1)


def cmd_domains_delete(client: DataHubCLIClient, args):
    """Delete a domain."""
    result = client.delete_domain(args.urn)
    if result:
        print(f"Successfully deleted domain: {args.urn}")
    else:
        print(f"Failed to delete domain: {args.urn}")
        sys.exit(1)


def cmd_domains_find_entities(client: DataHubCLIClient, args):
    """Find entities in a domain."""
    result = client.find_entities_with_domain(args.urn, args.start, args.count)
    output_result(result, args.format)


# Structured Properties Commands
def cmd_properties_list(client: DataHubCLIClient, args):
    """List structured properties."""
    result = client.list_structured_properties(query=args.query, start=args.start, count=args.count)
    output_result(result, args.format)


def cmd_properties_get(client: DataHubCLIClient, args):
    """Get a specific structured property."""
    result = client.get_structured_property(args.urn)
    if result:
        output_result(result, args.format)
    else:
        print(f"Structured property not found: {args.urn}")
        sys.exit(1)


def cmd_properties_create(client: DataHubCLIClient, args):
    """Create a new structured property."""
    entity_types = args.entity_types.split(',') if args.entity_types else None
    allowed_values = json.loads(args.allowed_values) if args.allowed_values else None

    result = client.create_structured_property(
        display_name=args.name,
        description=args.description or "",
        value_type=args.value_type,
        cardinality=args.cardinality,
        entity_types=entity_types,
        allowed_values=allowed_values,
        qualified_name=args.qualified_name
    )
    if result:
        print(f"Created structured property with URN: {result}")
    else:
        print("Failed to create structured property")
        sys.exit(1)


def cmd_properties_delete(client: DataHubCLIClient, args):
    """Delete a structured property."""
    result = client.delete_structured_property(args.urn)
    if result:
        print(f"Successfully deleted structured property: {args.urn}")
    else:
        print(f"Failed to delete structured property: {args.urn}")
        sys.exit(1)


# Glossary Commands
def cmd_glossary_list_nodes(client: DataHubCLIClient, args):
    """List glossary nodes."""
    result = client.list_glossary_nodes(query=args.query, start=args.start, count=args.count)
    output_result(result, args.format)


def cmd_glossary_list_terms(client: DataHubCLIClient, args):
    """List glossary terms."""
    result = client.list_glossary_terms(node_urn=args.node_urn, query=args.query, start=args.start, count=args.count)
    output_result(result, args.format)


def cmd_glossary_get_node(client: DataHubCLIClient, args):
    """Get a specific glossary node."""
    result = client.get_glossary_node(args.urn)
    if result:
        output_result(result, args.format)
    else:
        print(f"Glossary node not found: {args.urn}")
        sys.exit(1)


def cmd_glossary_get_term(client: DataHubCLIClient, args):
    """Get a specific glossary term."""
    result = client.get_glossary_term(args.urn)
    if result:
        output_result(result, args.format)
    else:
        print(f"Glossary term not found: {args.urn}")
        sys.exit(1)


def cmd_glossary_create_node(client: DataHubCLIClient, args):
    """Create a new glossary node."""
    result = client.create_glossary_node(args.id, args.name, args.description or "", args.parent)
    if result:
        print(f"Created glossary node with URN: {result}")
    else:
        print("Failed to create glossary node")
        sys.exit(1)


def cmd_glossary_create_term(client: DataHubCLIClient, args):
    """Create a new glossary term."""
    result = client.create_glossary_term(args.id, args.name, args.description or "", args.parent, args.source or "INTERNAL")
    if result:
        print(f"Created glossary term with URN: {result}")
    else:
        print("Failed to create glossary term")
        sys.exit(1)


def cmd_glossary_delete_node(client: DataHubCLIClient, args):
    """Delete a glossary node."""
    result = client.delete_glossary_node(args.urn)
    if result:
        print(f"Successfully deleted glossary node: {args.urn}")
    else:
        print(f"Failed to delete glossary node: {args.urn}")
        sys.exit(1)


def cmd_glossary_delete_term(client: DataHubCLIClient, args):
    """Delete a glossary term."""
    result = client.delete_glossary_term(args.urn)
    if result:
        print(f"Successfully deleted glossary term: {args.urn}")
    else:
        print(f"Failed to delete glossary term: {args.urn}")
        sys.exit(1)


def cmd_glossary_comprehensive(client: DataHubCLIClient, args):
    """Get comprehensive glossary data."""
    result = client.get_comprehensive_glossary_data(query=args.query, start=args.start, count=args.count)
    output_result(result, args.format)


# Data Product Commands
def cmd_data_products_list(client: DataHubCLIClient, args):
    """List data products."""
    result = client.list_data_products(query=args.query, start=args.start, count=args.count)
    output_result(result, args.format)


def cmd_data_products_get(client: DataHubCLIClient, args):
    """Get a specific data product."""
    result = client.get_data_product(args.urn)
    if result:
        output_result(result, args.format)
    else:
        print(f"Data product not found: {args.urn}")
        sys.exit(1)


def cmd_data_products_create(client: DataHubCLIClient, args):
    """Create a new data product."""
    result = client.create_data_product(args.id, args.name, args.description or "", args.external_url)
    if result:
        print(f"Created data product with URN: {result}")
    else:
        print("Failed to create data product")
        sys.exit(1)


def cmd_data_products_delete(client: DataHubCLIClient, args):
    """Delete a data product."""
    result = client.delete_data_product(args.urn)
    if result:
        print(f"Successfully deleted data product: {args.urn}")
    else:
        print(f"Failed to delete data product: {args.urn}")
        sys.exit(1)


# Ingestion Commands
def cmd_ingestion_list(client: DataHubCLIClient, args):
    """List ingestion sources."""
    result = client.list_ingestion_sources()
    if args.format == 'table':
        # Custom table format for ingestion sources
        if result:
            print("ID\tName\tType\tStatus\tLast Execution")
            for source in result:
                last_exec = source.get('last_execution', {})
                status = last_exec.get('status', 'N/A') if last_exec else 'N/A'
                print(f"{source.get('id', '')}\t{source.get('name', '')}\t{source.get('type', '')}\t{status}\t{last_exec.get('startTimeMs', 'N/A') if last_exec else 'N/A'}")
        else:
            print("No ingestion sources found")
    else:
        output_result(result, args.format)


def cmd_ingestion_get(client: DataHubCLIClient, args):
    """Get a specific ingestion source."""
    result = client.get_ingestion_source(args.id)
    if result:
        output_result(result, args.format)
    else:
        print(f"Ingestion source not found: {args.id}")
        sys.exit(1)


def cmd_ingestion_create(client: DataHubCLIClient, args):
    """Create a new ingestion source."""
    # Load recipe from file or use provided JSON
    if args.recipe_file:
        with open(args.recipe_file) as f:
            recipe = json.load(f)
    else:
        recipe = json.loads(args.recipe)

    result = client.create_ingestion_source(
        recipe=recipe,
        name=args.name,
        source_type=args.source_type,
        schedule_interval=args.schedule_interval,
        timezone=args.timezone,
        source_id=args.source_id,
        debug_mode=args.debug_mode
    )
    if result:
        print(f"Created ingestion source: {result.get('id')}")
        output_result(result, args.format)
    else:
        print("Failed to create ingestion source")
        sys.exit(1)


def cmd_ingestion_delete(client: DataHubCLIClient, args):
    """Delete an ingestion source."""
    result = client.delete_ingestion_source(args.id)
    if result:
        print(f"Successfully deleted ingestion source: {args.id}")
    else:
        print(f"Failed to delete ingestion source: {args.id}")
        sys.exit(1)


def cmd_ingestion_trigger(client: DataHubCLIClient, args):
    """Trigger an ingestion source."""
    result = client.trigger_ingestion(args.id)
    if result:
        print(f"Successfully triggered ingestion source: {args.id}")
    else:
        print(f"Failed to trigger ingestion source: {args.id}")
        sys.exit(1)


def cmd_ingestion_executions(client: DataHubCLIClient, args):
    """Get execution history for an ingestion source."""
    result = client.get_ingestion_executions(args.id, args.start, args.count)
    if args.format == 'table':
        # Custom table format for executions
        if result:
            print("ID\tStatus\tStart Time\tDuration (ms)\tRequested At")
            for exec in result:
                print(f"{exec.get('id', '')}\t{exec.get('status', '')}\t{exec.get('startTimeMs', '')}\t{exec.get('durationMs', '')}\t{exec.get('requestedAt', '')}")
        else:
            print("No executions found")
    else:
        output_result(result, args.format)


def cmd_ingestion_stats(client: DataHubCLIClient, args):
    """Get statistics for an ingestion source."""
    result = client.get_ingestion_source_stats(args.id)
    if result:
        output_result(result, args.format)
    else:
        print(f"Failed to get stats for ingestion source: {args.id}")
        sys.exit(1)


def cmd_ingestion_patch(client: DataHubCLIClient, args):
    """Patch an ingestion source."""
    patch_data = {}

    if args.name:
        patch_data['name'] = args.name
    if args.recipe_file:
        with open(args.recipe_file) as f:
            patch_data['recipe'] = json.load(f)
    elif args.recipe:
        patch_data['recipe'] = json.loads(args.recipe)
    if args.schedule_interval:
        patch_data['schedule_interval'] = args.schedule_interval
    if args.timezone:
        patch_data['timezone'] = args.timezone
    if args.debug_mode is not None:
        patch_data['debug_mode'] = args.debug_mode

    if not patch_data:
        print("No patch data provided")
        sys.exit(1)

    result = client.patch_ingestion_source(args.id, **patch_data)
    if result:
        print(f"Successfully patched ingestion source: {args.id}")
        output_result(result, args.format)
    else:
        print(f"Failed to patch ingestion source: {args.id}")
        sys.exit(1)


# Connection Commands
def cmd_test_connection(client: DataHubCLIClient, args):
    """Test connection to DataHub."""
    result = client.test_connection()
    if result:
        print("✅ Connection successful")
    else:
        print("❌ Connection failed")
        sys.exit(1)


# Bulk Operations
def cmd_bulk_import(client: DataHubCLIClient, args):
    """Bulk import from JSON file."""
    try:
        with open(args.file) as f:
            data = json.load(f)

        results = []
        for item in data:
            item_type = item.get('type')
            if item_type == 'tag':
                result = client.create_tag(item['id'], item['name'], item.get('description', ''))
                results.append({'type': 'tag', 'id': item['id'], 'success': bool(result), 'urn': result})
            elif item_type == 'domain':
                result = client.create_domain(item['id'], item['name'], item.get('description', ''), item.get('parent'))
                results.append({'type': 'domain', 'id': item['id'], 'success': bool(result), 'urn': result})
            elif item_type == 'structured_property':
                result = client.create_structured_property(
                    display_name=item['name'],
                    description=item.get('description', ''),
                    value_type=item.get('value_type', 'STRING'),
                    cardinality=item.get('cardinality', 'SINGLE'),
                    entity_types=item.get('entity_types'),
                    allowed_values=item.get('allowed_values')
                )
                results.append({'type': 'structured_property', 'name': item['name'], 'success': bool(result), 'urn': result})

        output_result(results, args.format)

    except Exception as e:
        print(f"Error during bulk import: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description='DataHub CLI - Comprehensive DataHub operations')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--format', choices=['json', 'table'], default='json', help='Output format')

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Connection commands
    conn_parser = subparsers.add_parser('test', help='Test connection to DataHub')
    conn_parser.set_defaults(func=cmd_test_connection)

    # Tag commands
    tags_parser = subparsers.add_parser('tags', help='Tag operations')
    tags_subparsers = tags_parser.add_subparsers(dest='tags_command')

    # Tags list
    tags_list_parser = tags_subparsers.add_parser('list', help='List tags')
    tags_list_parser.add_argument('--query', default='*', help='Search query')
    tags_list_parser.add_argument('--start', type=int, default=0, help='Start offset')
    tags_list_parser.add_argument('--count', type=int, default=100, help='Number of results')
    tags_list_parser.set_defaults(func=cmd_tags_list)

    # Tags get
    tags_get_parser = tags_subparsers.add_parser('get', help='Get tag by URN')
    tags_get_parser.add_argument('urn', help='Tag URN')
    tags_get_parser.set_defaults(func=cmd_tags_get)

    # Tags create
    tags_create_parser = tags_subparsers.add_parser('create', help='Create tag')
    tags_create_parser.add_argument('id', help='Tag ID')
    tags_create_parser.add_argument('name', help='Tag name')
    tags_create_parser.add_argument('--description', help='Tag description')
    tags_create_parser.set_defaults(func=cmd_tags_create)

    # Tags delete
    tags_delete_parser = tags_subparsers.add_parser('delete', help='Delete tag')
    tags_delete_parser.add_argument('urn', help='Tag URN')
    tags_delete_parser.set_defaults(func=cmd_tags_delete)

    # Tags set-color
    tags_color_parser = tags_subparsers.add_parser('set-color', help='Set tag color')
    tags_color_parser.add_argument('urn', help='Tag URN')
    tags_color_parser.add_argument('color', help='Color hex code (e.g., #FF0000)')
    tags_color_parser.set_defaults(func=cmd_tags_set_color)

    # Tags add-to-entity
    tags_add_parser = tags_subparsers.add_parser('add-to-entity', help='Add tag to entity')
    tags_add_parser.add_argument('entity_urn', help='Entity URN')
    tags_add_parser.add_argument('tag_urn', help='Tag URN')
    tags_add_parser.add_argument('--color', help='Tag color')
    tags_add_parser.set_defaults(func=cmd_tags_add_to_entity)

    # Domain commands
    domains_parser = subparsers.add_parser('domains', help='Domain operations')
    domains_subparsers = domains_parser.add_subparsers(dest='domains_command')

    # Domains list
    domains_list_parser = domains_subparsers.add_parser('list', help='List domains')
    domains_list_parser.add_argument('--query', default='*', help='Search query')
    domains_list_parser.add_argument('--start', type=int, default=0, help='Start offset')
    domains_list_parser.add_argument('--count', type=int, default=100, help='Number of results')
    domains_list_parser.set_defaults(func=cmd_domains_list)

    # Domains get
    domains_get_parser = domains_subparsers.add_parser('get', help='Get domain by URN')
    domains_get_parser.add_argument('urn', help='Domain URN')
    domains_get_parser.set_defaults(func=cmd_domains_get)

    # Domains create
    domains_create_parser = domains_subparsers.add_parser('create', help='Create domain')
    domains_create_parser.add_argument('id', help='Domain ID')
    domains_create_parser.add_argument('name', help='Domain name')
    domains_create_parser.add_argument('--description', help='Domain description')
    domains_create_parser.add_argument('--parent', help='Parent domain URN')
    domains_create_parser.set_defaults(func=cmd_domains_create)

    # Domains delete
    domains_delete_parser = domains_subparsers.add_parser('delete', help='Delete domain')
    domains_delete_parser.add_argument('urn', help='Domain URN')
    domains_delete_parser.set_defaults(func=cmd_domains_delete)

    # Domains find-entities
    domains_entities_parser = domains_subparsers.add_parser('find-entities', help='Find entities in domain')
    domains_entities_parser.add_argument('urn', help='Domain URN')
    domains_entities_parser.add_argument('--start', type=int, default=0, help='Start offset')
    domains_entities_parser.add_argument('--count', type=int, default=50, help='Number of results')
    domains_entities_parser.set_defaults(func=cmd_domains_find_entities)

    # Structured Properties commands
    props_parser = subparsers.add_parser('properties', help='Structured properties operations')
    props_subparsers = props_parser.add_subparsers(dest='properties_command')

    # Properties list
    props_list_parser = props_subparsers.add_parser('list', help='List structured properties')
    props_list_parser.add_argument('--query', default='*', help='Search query')
    props_list_parser.add_argument('--start', type=int, default=0, help='Start offset')
    props_list_parser.add_argument('--count', type=int, default=100, help='Number of results')
    props_list_parser.set_defaults(func=cmd_properties_list)

    # Properties get
    props_get_parser = props_subparsers.add_parser('get', help='Get structured property by URN')
    props_get_parser.add_argument('urn', help='Structured property URN')
    props_get_parser.set_defaults(func=cmd_properties_get)

    # Properties create
    props_create_parser = props_subparsers.add_parser('create', help='Create structured property')
    props_create_parser.add_argument('name', help='Property display name')
    props_create_parser.add_argument('--description', help='Property description')
    props_create_parser.add_argument('--value-type', default='STRING', help='Value type (STRING, NUMBER, etc.)')
    props_create_parser.add_argument('--cardinality', default='SINGLE', help='Cardinality (SINGLE, MULTIPLE)')
    props_create_parser.add_argument('--entity-types', help='Comma-separated entity types')
    props_create_parser.add_argument('--allowed-values', help='JSON array of allowed values')
    props_create_parser.add_argument('--qualified-name', help='Qualified name')
    props_create_parser.set_defaults(func=cmd_properties_create)

    # Properties delete
    props_delete_parser = props_subparsers.add_parser('delete', help='Delete structured property')
    props_delete_parser.add_argument('urn', help='Structured property URN')
    props_delete_parser.set_defaults(func=cmd_properties_delete)

    # Glossary commands
    glossary_parser = subparsers.add_parser('glossary', help='Glossary operations')
    glossary_subparsers = glossary_parser.add_subparsers(dest='glossary_command')

    # Glossary list nodes
    glossary_nodes_parser = glossary_subparsers.add_parser('list-nodes', help='List glossary nodes')
    glossary_nodes_parser.add_argument('--query', default='*', help='Search query')
    glossary_nodes_parser.add_argument('--start', type=int, default=0, help='Start offset')
    glossary_nodes_parser.add_argument('--count', type=int, default=100, help='Number of results')
    glossary_nodes_parser.set_defaults(func=cmd_glossary_list_nodes)

    # Glossary list terms
    glossary_terms_parser = glossary_subparsers.add_parser('list-terms', help='List glossary terms')
    glossary_terms_parser.add_argument('--query', default='*', help='Search query')
    glossary_terms_parser.add_argument('--node-urn', help='Parent node URN to filter by')
    glossary_terms_parser.add_argument('--start', type=int, default=0, help='Start offset')
    glossary_terms_parser.add_argument('--count', type=int, default=100, help='Number of results')
    glossary_terms_parser.set_defaults(func=cmd_glossary_list_terms)

    # Glossary get node
    glossary_get_node_parser = glossary_subparsers.add_parser('get-node', help='Get glossary node by URN')
    glossary_get_node_parser.add_argument('urn', help='Glossary node URN')
    glossary_get_node_parser.set_defaults(func=cmd_glossary_get_node)

    # Glossary get term
    glossary_get_term_parser = glossary_subparsers.add_parser('get-term', help='Get glossary term by URN')
    glossary_get_term_parser.add_argument('urn', help='Glossary term URN')
    glossary_get_term_parser.set_defaults(func=cmd_glossary_get_term)

    # Glossary create node
    glossary_create_node_parser = glossary_subparsers.add_parser('create-node', help='Create glossary node')
    glossary_create_node_parser.add_argument('id', help='Node ID')
    glossary_create_node_parser.add_argument('name', help='Node name')
    glossary_create_node_parser.add_argument('--description', help='Node description')
    glossary_create_node_parser.add_argument('--parent', help='Parent node URN')
    glossary_create_node_parser.set_defaults(func=cmd_glossary_create_node)

    # Glossary create term
    glossary_create_term_parser = glossary_subparsers.add_parser('create-term', help='Create glossary term')
    glossary_create_term_parser.add_argument('id', help='Term ID')
    glossary_create_term_parser.add_argument('name', help='Term name')
    glossary_create_term_parser.add_argument('--description', help='Term description')
    glossary_create_term_parser.add_argument('--parent', help='Parent node URN')
    glossary_create_term_parser.add_argument('--source', help='Term source (default: INTERNAL)')
    glossary_create_term_parser.set_defaults(func=cmd_glossary_create_term)

    # Glossary delete node
    glossary_delete_node_parser = glossary_subparsers.add_parser('delete-node', help='Delete glossary node')
    glossary_delete_node_parser.add_argument('urn', help='Glossary node URN')
    glossary_delete_node_parser.set_defaults(func=cmd_glossary_delete_node)

    # Glossary delete term
    glossary_delete_term_parser = glossary_subparsers.add_parser('delete-term', help='Delete glossary term')
    glossary_delete_term_parser.add_argument('urn', help='Glossary term URN')
    glossary_delete_term_parser.set_defaults(func=cmd_glossary_delete_term)

    # Glossary comprehensive
    glossary_comp_parser = glossary_subparsers.add_parser('comprehensive', help='Get comprehensive glossary data')
    glossary_comp_parser.add_argument('--query', default='*', help='Search query')
    glossary_comp_parser.add_argument('--start', type=int, default=0, help='Start offset')
    glossary_comp_parser.add_argument('--count', type=int, default=100, help='Number of results')
    glossary_comp_parser.set_defaults(func=cmd_glossary_comprehensive)

    # Data Product commands
    data_products_parser = subparsers.add_parser('data-products', help='Data product operations')
    data_products_subparsers = data_products_parser.add_subparsers(dest='data_products_command')

    # Data products list
    data_products_list_parser = data_products_subparsers.add_parser('list', help='List data products')
    data_products_list_parser.add_argument('--query', default='*', help='Search query')
    data_products_list_parser.add_argument('--start', type=int, default=0, help='Start offset')
    data_products_list_parser.add_argument('--count', type=int, default=100, help='Number of results')
    data_products_list_parser.set_defaults(func=cmd_data_products_list)

    # Data products get
    data_products_get_parser = data_products_subparsers.add_parser('get', help='Get data product by URN')
    data_products_get_parser.add_argument('urn', help='Data product URN')
    data_products_get_parser.set_defaults(func=cmd_data_products_get)

    # Data products create
    data_products_create_parser = data_products_subparsers.add_parser('create', help='Create data product')
    data_products_create_parser.add_argument('id', help='Data product ID')
    data_products_create_parser.add_argument('name', help='Data product name')
    data_products_create_parser.add_argument('--description', help='Data product description')
    data_products_create_parser.add_argument('--external-url', help='External URL')
    data_products_create_parser.set_defaults(func=cmd_data_products_create)

    # Data products delete
    data_products_delete_parser = data_products_subparsers.add_parser('delete', help='Delete data product')
    data_products_delete_parser.add_argument('urn', help='Data product URN')
    data_products_delete_parser.set_defaults(func=cmd_data_products_delete)

    # Ingestion commands
    ingestion_parser = subparsers.add_parser('ingestion', help='Ingestion source operations')
    ingestion_subparsers = ingestion_parser.add_subparsers(dest='ingestion_command')

    # Ingestion list
    ingestion_list_parser = ingestion_subparsers.add_parser('list', help='List ingestion sources')
    ingestion_list_parser.set_defaults(func=cmd_ingestion_list)

    # Ingestion get
    ingestion_get_parser = ingestion_subparsers.add_parser('get', help='Get ingestion source by ID')
    ingestion_get_parser.add_argument('id', help='Ingestion source ID')
    ingestion_get_parser.set_defaults(func=cmd_ingestion_get)

    # Ingestion create
    ingestion_create_parser = ingestion_subparsers.add_parser('create', help='Create ingestion source')
    ingestion_create_parser.add_argument('--recipe', help='Recipe JSON string')
    ingestion_create_parser.add_argument('--recipe-file', help='Recipe JSON file path')
    ingestion_create_parser.add_argument('--name', help='Source name')
    ingestion_create_parser.add_argument('--source-type', help='Source type')
    ingestion_create_parser.add_argument('--schedule-interval', default='0 0 * * *', help='Schedule interval (cron)')
    ingestion_create_parser.add_argument('--timezone', default='UTC', help='Timezone')
    ingestion_create_parser.add_argument('--source-id', help='Custom source ID')
    ingestion_create_parser.add_argument('--debug-mode', action='store_true', help='Enable debug mode')
    ingestion_create_parser.set_defaults(func=cmd_ingestion_create)

    # Ingestion delete
    ingestion_delete_parser = ingestion_subparsers.add_parser('delete', help='Delete ingestion source')
    ingestion_delete_parser.add_argument('id', help='Ingestion source ID')
    ingestion_delete_parser.set_defaults(func=cmd_ingestion_delete)

    # Ingestion trigger
    ingestion_trigger_parser = ingestion_subparsers.add_parser('trigger', help='Trigger ingestion source')
    ingestion_trigger_parser.add_argument('id', help='Ingestion source ID')
    ingestion_trigger_parser.set_defaults(func=cmd_ingestion_trigger)

    # Ingestion executions
    ingestion_exec_parser = ingestion_subparsers.add_parser('executions', help='Get execution history')
    ingestion_exec_parser.add_argument('id', help='Ingestion source ID')
    ingestion_exec_parser.add_argument('--start', type=int, default=0, help='Start offset')
    ingestion_exec_parser.add_argument('--count', type=int, default=10, help='Number of executions')
    ingestion_exec_parser.set_defaults(func=cmd_ingestion_executions)

    # Ingestion stats
    ingestion_stats_parser = ingestion_subparsers.add_parser('stats', help='Get source statistics')
    ingestion_stats_parser.add_argument('id', help='Ingestion source ID')
    ingestion_stats_parser.set_defaults(func=cmd_ingestion_stats)

    # Ingestion patch
    ingestion_patch_parser = ingestion_subparsers.add_parser('patch', help='Patch ingestion source')
    ingestion_patch_parser.add_argument('id', help='Ingestion source ID')
    ingestion_patch_parser.add_argument('--name', help='New name')
    ingestion_patch_parser.add_argument('--recipe', help='New recipe JSON string')
    ingestion_patch_parser.add_argument('--recipe-file', help='New recipe JSON file path')
    ingestion_patch_parser.add_argument('--schedule-interval', help='New schedule interval')
    ingestion_patch_parser.add_argument('--timezone', help='New timezone')
    ingestion_patch_parser.add_argument('--debug-mode', type=bool, help='Debug mode (true/false)')
    ingestion_patch_parser.set_defaults(func=cmd_ingestion_patch)

    # Bulk operations
    bulk_parser = subparsers.add_parser('bulk', help='Bulk operations')
    bulk_subparsers = bulk_parser.add_subparsers(dest='bulk_command')

    # Bulk import
    bulk_import_parser = bulk_subparsers.add_parser('import', help='Bulk import from JSON file')
    bulk_import_parser.add_argument('file', help='JSON file path')
    bulk_import_parser.set_defaults(func=cmd_bulk_import)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    setup_logging(args.verbose)

    try:
        client = get_client_from_env()
        args.func(client, args)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
