#!/usr/bin/env python3
"""
Script to ingest test data into DataHub for integration testing.

This script creates sample entities of all types to ensure our GraphQL queries
and MCP outputs work correctly against real DataHub data.
"""

import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Any, List

# Add the datahub package to the path
sys.path.insert(0, '/opt/datahub/datahub-ingestion/src')

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass,
    DomainPropertiesClass,
    TagPropertiesClass,
    GlossaryTermInfoClass,
    GlossaryNodeInfoClass,
    DataProductPropertiesClass,
    DataContractPropertiesClass,
    TestDefinitionClass,
    TestInfoClass,
    AssertionInfoClass,
    AuditStampClass,
    ChangeTypeClass
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_rest_emitter() -> DatahubRestEmitter:
    """Create DataHub REST emitter."""
    gms_url = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
    token = os.getenv("DATAHUB_GMS_TOKEN", "")
    
    logger.info(f"Connecting to DataHub at {gms_url}")
    
    return DatahubRestEmitter(
        gms_server=gms_url,
        token=token if token else None
    )


def ingest_sample_datasets(emitter: DatahubRestEmitter) -> None:
    """Ingest sample datasets with properties and ownership."""
    logger.info("Ingesting sample datasets...")
    
    datasets = [
        {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_db.public.users,PROD)",
            "name": "users",
            "description": "User information table",
            "platform": "postgres"
        },
        {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:postgres,test_db.public.orders,PROD)",
            "name": "orders", 
            "description": "Order transaction data",
            "platform": "postgres"
        },
        {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.analytics_summary,PROD)",
            "name": "analytics_summary",
            "description": "Analytics summary table",
            "platform": "bigquery"
        }
    ]
    
    for dataset in datasets:
        # Dataset properties
        properties_mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset["urn"],
            aspect=DatasetPropertiesClass(
                name=dataset["name"],
                description=dataset["description"],
                customProperties={
                    "test_property": "test_value",
                    "platform": dataset["platform"]
                }
            )
        )
        
        # Ownership
        ownership_mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset["urn"],
            aspect=OwnershipClass(
                owners=[
                    OwnerClass(
                        owner="urn:li:corpuser:test_user",
                        type=OwnershipTypeClass.DATAOWNER
                    )
                ]
            )
        )
        
        emitter.emit([properties_mcp, ownership_mcp])
        logger.info(f"Ingested dataset: {dataset['name']}")


def ingest_sample_domains(emitter: DatahubRestEmitter) -> None:
    """Ingest sample domains."""
    logger.info("Ingesting sample domains...")
    
    domains = [
        {
            "urn": "urn:li:domain:test-domain",
            "name": "Test Domain",
            "description": "A test domain for integration testing"
        },
        {
            "urn": "urn:li:domain:analytics-domain",
            "name": "Analytics Domain",
            "description": "Domain for analytics datasets"
        }
    ]
    
    for domain in domains:
        properties_mcp = MetadataChangeProposalWrapper(
            entityUrn=domain["urn"],
            aspect=DomainPropertiesClass(
                name=domain["name"],
                description=domain["description"]
            )
        )
        
        ownership_mcp = MetadataChangeProposalWrapper(
            entityUrn=domain["urn"],
            aspect=OwnershipClass(
                owners=[
                    OwnerClass(
                        owner="urn:li:corpuser:test_user",
                        type=OwnershipTypeClass.DATAOWNER
                    )
                ]
            )
        )
        
        emitter.emit([properties_mcp, ownership_mcp])
        logger.info(f"Ingested domain: {domain['name']}")


def ingest_sample_tags(emitter: DatahubRestEmitter) -> None:
    """Ingest sample tags."""
    logger.info("Ingesting sample tags...")
    
    tags = [
        {
            "urn": "urn:li:tag:test-tag",
            "name": "Test Tag",
            "description": "A test tag for integration testing",
            "colorHex": "#FF0000"
        },
        {
            "urn": "urn:li:tag:pii",
            "name": "PII",
            "description": "Personally Identifiable Information",
            "colorHex": "#FFA500"
        }
    ]
    
    for tag in tags:
        properties_mcp = MetadataChangeProposalWrapper(
            entityUrn=tag["urn"],
            aspect=TagPropertiesClass(
                name=tag["name"],
                description=tag["description"],
                colorHex=tag["colorHex"]
            )
        )
        
        ownership_mcp = MetadataChangeProposalWrapper(
            entityUrn=tag["urn"],
            aspect=OwnershipClass(
                owners=[
                    OwnerClass(
                        owner="urn:li:corpuser:test_user",
                        type=OwnershipTypeClass.DATAOWNER
                    )
                ]
            )
        )
        
        emitter.emit([properties_mcp, ownership_mcp])
        logger.info(f"Ingested tag: {tag['name']}")


def ingest_sample_glossary(emitter: DatahubRestEmitter) -> None:
    """Ingest sample glossary terms and nodes."""
    logger.info("Ingesting sample glossary...")
    
    # Glossary nodes (categories)
    nodes = [
        {
            "urn": "urn:li:glossaryNode:test-node",
            "name": "Test Node",
            "description": "A test glossary node"
        }
    ]
    
    for node in nodes:
        properties_mcp = MetadataChangeProposalWrapper(
            entityUrn=node["urn"],
            aspect=GlossaryNodeInfoClass(
                name=node["name"],
                description=node["description"]
            )
        )
        
        emitter.emit([properties_mcp])
        logger.info(f"Ingested glossary node: {node['name']}")
    
    # Glossary terms
    terms = [
        {
            "urn": "urn:li:glossaryTerm:test-term",
            "name": "Test Term",
            "description": "A test glossary term"
        },
        {
            "urn": "urn:li:glossaryTerm:customer",
            "name": "Customer",
            "description": "An individual or organization that purchases goods or services"
        }
    ]
    
    for term in terms:
        properties_mcp = MetadataChangeProposalWrapper(
            entityUrn=term["urn"],
            aspect=GlossaryTermInfoClass(
                name=term["name"],
                description=term["description"]
            )
        )
        
        ownership_mcp = MetadataChangeProposalWrapper(
            entityUrn=term["urn"],
            aspect=OwnershipClass(
                owners=[
                    OwnerClass(
                        owner="urn:li:corpuser:test_user",
                        type=OwnershipTypeClass.DATAOWNER
                    )
                ]
            )
        )
        
        emitter.emit([properties_mcp, ownership_mcp])
        logger.info(f"Ingested glossary term: {term['name']}")


def ingest_sample_data_products(emitter: DatahubRestEmitter) -> None:
    """Ingest sample data products."""
    logger.info("Ingesting sample data products...")
    
    products = [
        {
            "urn": "urn:li:dataProduct:test-product",
            "name": "Test Data Product",
            "description": "A test data product"
        },
        {
            "urn": "urn:li:dataProduct:user-analytics",
            "name": "User Analytics Product",
            "description": "Data product for user analytics"
        }
    ]
    
    for product in products:
        properties_mcp = MetadataChangeProposalWrapper(
            entityUrn=product["urn"],
            aspect=DataProductPropertiesClass(
                name=product["name"],
                description=product["description"]
            )
        )
        
        ownership_mcp = MetadataChangeProposalWrapper(
            entityUrn=product["urn"],
            aspect=OwnershipClass(
                owners=[
                    OwnerClass(
                        owner="urn:li:corpuser:test_user",
                        type=OwnershipTypeClass.DATAOWNER
                    )
                ]
            )
        )
        
        emitter.emit([properties_mcp, ownership_mcp])
        logger.info(f"Ingested data product: {product['name']}")


def main():
    """Main function to ingest all test data."""
    try:
        emitter = create_rest_emitter()
        
        # Ingest all entity types
        ingest_sample_datasets(emitter)
        ingest_sample_domains(emitter)
        ingest_sample_tags(emitter)
        ingest_sample_glossary(emitter)
        ingest_sample_data_products(emitter)
        
        logger.info("✅ All test data ingested successfully!")
        
    except Exception as e:
        logger.error(f"❌ Failed to ingest test data: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 