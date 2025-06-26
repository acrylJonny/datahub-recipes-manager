#!/usr/bin/env python3
"""
Example script showing how to use the new datahub-cicd-client package
instead of the old MCP creation logic.

This demonstrates the migration path from the old scripts/mcps/*.py files
to the new package-based architecture.
"""

import logging
import sys
import os
from typing import Dict, Any, List

# Add the package to path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../datahub-cicd-client/src'))

# Import from the new package
from datahub_cicd_client.mcp_builders import DataProductMCPBuilder, TagMCPBuilder
from datahub_cicd_client.emitters import FileEmitter, DataHubEmitter
from datahub_cicd_client.services import DataProductService
from datahub_cicd_client.core.connection import DataHubConnection

logger = logging.getLogger(__name__)


def create_data_product_with_new_architecture():
    """
    Example: Create a data product using the new MCP builder architecture.
    
    This replaces the old create_data_product_mcps.py and data_product_actions.py logic.
    """
    print("🏗️  Creating data product with new architecture...")
    
    # Initialize the MCP builder
    builder = DataProductMCPBuilder(output_dir="metadata-manager/dev")
    
    # Define data product data
    data_product_data = {
        "id": "customer_analytics_product",
        "name": "Customer Analytics Data Product", 
        "description": "Comprehensive analytics for customer behavior and segmentation",
        "external_url": "https://wiki.company.com/customer-analytics",
        "owners": ["urn:li:corpuser:data_team"],
        "tags": ["urn:li:tag:analytics", "urn:li:tag:customer"],
        "terms": ["urn:li:glossaryTerm:customer", "urn:li:glossaryTerm:analytics"],
        "domains": ["urn:li:domain:marketing"],
        "links": [
            {
                "url": "https://docs.company.com/customer-analytics",
                "description": "Data Product Documentation"
            }
        ],
        "custom_properties": {
            "team": "Data Engineering",
            "criticality": "high"
        },
        "structured_properties": [
            {
                "propertyUrn": "urn:li:structuredProperty:data_freshness",
                "value": "daily"
            }
        ]
    }
    
    # Create MCPs using the builder
    mcps = builder.create_entity_mcps(data_product_data)
    
    print(f"✅ Created {len(mcps)} MCPs for data product")
    
    # Option 1: Save to staged changes (file-based)
    result = builder.save_staged_changes(data_product_data, environment="dev")
    print(f"📁 Staged changes result: {result}")
    
    # Option 2: Use file emitter directly
    file_emitter = FileEmitter("metadata-manager/dev")
    file_path = file_emitter.emit(mcps, "data_product_customer_analytics.json")
    print(f"📄 MCPs written to: {file_path}")
    
    # Option 3: Emit directly to DataHub (if configured)
    # datahub_emitter = DataHubEmitter("http://localhost:8080", token="your_token")
    # results = datahub_emitter.emit(mcps)
    # print(f"🚀 Emission results: {results}")
    
    return mcps


def create_tag_with_new_architecture():
    """
    Example: Create a tag using the new MCP builder architecture.
    
    This replaces the old create_tag_mcps.py and tag_actions.py logic.
    """
    print("\n🏷️  Creating tag with new architecture...")
    
    # Initialize the MCP builder
    builder = TagMCPBuilder(output_dir="metadata-manager/dev")
    
    # Define tag data
    tag_data = {
        "id": "high_quality",
        "name": "High Quality",
        "description": "Indicates datasets that meet high quality standards",
        "color_hex": "#00FF00",
        "owners": ["urn:li:corpuser:data_quality_team"]
    }
    
    # Create MCPs using the builder
    mcps = builder.create_entity_mcps(tag_data)
    
    print(f"✅ Created {len(mcps)} MCPs for tag")
    
    # Save to staged changes
    result = builder.save_staged_changes(tag_data, environment="dev")
    print(f"📁 Staged changes result: {result}")
    
    return mcps


def use_service_for_existing_entities():
    """
    Example: Use services to interact with existing entities.
    
    This shows how to combine the service layer with MCP builders.
    """
    print("\n🔧 Using services with new architecture...")
    
    # Initialize connection and service
    connection = DataHubConnection(
        server_url="http://localhost:8080",
        token="your_token_here"
    )
    
    service = DataProductService(connection)
    
    # List existing data products
    existing_products = service.list_data_products(query="*", count=5)
    print(f"📊 Found {len(existing_products)} existing data products")
    
    # For each existing product, we could create update MCPs
    builder = DataProductMCPBuilder()
    
    for product in existing_products[:1]:  # Just first one for demo
        # Get full product details
        product_details = service.get_data_product(product.get("urn"))
        
        if product_details:
            # Create update MCPs (e.g., add a tag)
            update_data = {
                "urn": product_details["urn"],
                "name": product_details.get("name", "Unknown"),
                "tags": ["urn:li:tag:migrated"]  # Add a migration tag
            }
            
            mcps = builder.create_entity_mcps(update_data)
            print(f"🔄 Created {len(mcps)} update MCPs for {product_details['urn']}")


def migration_example():
    """
    Example showing how to migrate from old scripts to new architecture.
    """
    print("\n🔄 Migration example...")
    
    # OLD WAY (from scripts/mcps/data_product_actions.py):
    # from scripts.mcps.data_product_actions import add_data_product_to_staged_changes
    # result = add_data_product_to_staged_changes(...)
    
    # NEW WAY:
    builder = DataProductMCPBuilder()
    
    # Same data, but now using the package
    data = {
        "id": "migrated_product",
        "name": "Migrated Data Product",
        "description": "This was created with the new architecture"
    }
    
    result = builder.save_staged_changes(data)
    print(f"✨ Migration complete: {result}")


def main():
    """Main function demonstrating the new architecture."""
    logging.basicConfig(level=logging.INFO)
    
    print("🚀 DataHub CI/CD Client - New Architecture Examples")
    print("=" * 60)
    
    try:
        # Create entities with new architecture
        create_data_product_with_new_architecture()
        create_tag_with_new_architecture()
        
        # Show service integration
        # use_service_for_existing_entities()  # Uncomment when you have a DataHub instance
        
        # Show migration path
        migration_example()
        
        print("\n✅ All examples completed successfully!")
        print("\nNext steps:")
        print("1. Move remaining MCP creation logic from scripts/mcps/ to datahub-cicd-client")
        print("2. Update CLI scripts to use the new package")
        print("3. Add emitter functionality to services for direct DataHub operations")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
