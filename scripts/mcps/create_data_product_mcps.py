#!/usr/bin/env python3
"""
Script to create MCP (Metadata Change Proposal) files for data products in DataHub
"""

import argparse
import json
import logging
import os
import sys
import time
from typing import Dict, Optional, Any, List

# Add parent directory to sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

try:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        # Common aspect classes
        OwnershipClass,
        OwnerClass,
        OwnershipTypeClass,
        OwnershipSourceClass,
        OwnershipSourceTypeClass,
        AuditStampClass,
        ChangeTypeClass,
        StatusClass,
        GlobalTagsClass,
        TagAssociationClass,
        GlossaryTermsClass,
        GlossaryTermAssociationClass,
        InstitutionalMemoryClass,
        InstitutionalMemoryMetadataClass,
        BrowsePathsClass,
        StructuredPropertiesClass,
        StructuredPropertyValueAssignmentClass,
        DomainsClass,
        SubTypesClass,
        DeprecationClass,
        
        # Data Product specific classes
        DataProductPropertiesClass,
        DataProductPropertiesClass,
    )
    DATAHUB_AVAILABLE = True
except ImportError as e:
    print(f"Warning: DataHub SDK not available: {e}")
    DATAHUB_AVAILABLE = False

# Import local utilities
from utils.urn_utils import generate_deterministic_urn, extract_name_from_properties

logger = logging.getLogger(__name__)


def setup_logging(log_level="INFO"):
    """Set up logging configuration"""
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def create_data_product_properties_mcp(
    data_product_urn: str,
    name: str,
    description: Optional[str] = None,
    external_url: Optional[str] = None,
    custom_properties: Optional[Dict[str, str]] = None,
    **kwargs
) -> Optional[MetadataChangeProposalWrapper]:
    """Create data product properties MCP"""
    if not DATAHUB_AVAILABLE:
        logger.error("DataHub SDK not available")
        return None
    
    try:
        properties = DataProductPropertiesClass(
            name=name,
            description=description,
            externalUrl=external_url,
            customProperties=custom_properties or {}
        )
        
        return MetadataChangeProposalWrapper(
            entityUrn=data_product_urn,
            aspect=properties,
            changeType=ChangeTypeClass.UPSERT
        )
    except Exception as e:
        logger.error(f"Error creating data product properties MCP: {e}")
        return None


def create_editable_data_product_properties_mcp(
    data_product_urn: str,
    description: Optional[str] = None,
    **kwargs
) -> Optional[MetadataChangeProposalWrapper]:
    """Create editable data product properties MCP"""
    if not DATAHUB_AVAILABLE:
        logger.error("DataHub SDK not available")
        return None
    
    try:
        data_product_properties = DataProductPropertiesClass(
            description=description
        )
        
        return MetadataChangeProposalWrapper(
            entityUrn=data_product_urn,
            aspect=data_product_properties,
            changeType=ChangeTypeClass.UPSERT
        )
    except Exception as e:
        logger.error(f"Error creating editable data product properties MCP: {e}")
        return None


def create_data_product_ownership_mcp(
    data_product_urn: str,
    owners: List[str],
    **kwargs
) -> Optional[MetadataChangeProposalWrapper]:
    """Create data product ownership MCP"""
    if not DATAHUB_AVAILABLE:
        logger.error("DataHub SDK not available")
        return None
    
    try:
        owner_objects = []
        for owner_urn in owners:
            owner_objects.append(
                OwnerClass(
                    owner=owner_urn,
                    type=OwnershipTypeClass.DATAOWNER,
                    source=OwnershipSourceClass(
                        type=OwnershipSourceTypeClass.MANUAL
                    )
                )
            )
        
        ownership = OwnershipClass(
            owners=owner_objects,
            lastModified=AuditStampClass(
                time=int(time.time() * 1000),
                actor="urn:li:corpuser:datahub"
            )
        )
        
        return MetadataChangeProposalWrapper(
            entityUrn=data_product_urn,
            aspect=ownership,
            changeType=ChangeTypeClass.UPSERT
        )
    except Exception as e:
        logger.error(f"Error creating data product ownership MCP: {e}")
        return None


def create_data_product_status_mcp(
    data_product_urn: str,
    removed: bool = False,
    **kwargs
) -> Optional[MetadataChangeProposalWrapper]:
    """Create data product status MCP"""
    if not DATAHUB_AVAILABLE:
        logger.error("DataHub SDK not available")
        return None
    
    try:
        status = StatusClass(removed=removed)
        
        return MetadataChangeProposalWrapper(
            entityUrn=data_product_urn,
            aspect=status,
            changeType=ChangeTypeClass.UPSERT
        )
    except Exception as e:
        logger.error(f"Error creating data product status MCP: {e}")
        return None


def create_data_product_global_tags_mcp(
    data_product_urn: str,
    tags: List[str],
    **kwargs
) -> Optional[MetadataChangeProposalWrapper]:
    """Create data product global tags MCP"""
    if not DATAHUB_AVAILABLE:
        logger.error("DataHub SDK not available")
        return None
    
    try:
        tag_associations = []
        for tag_urn in tags:
            tag_associations.append(
                TagAssociationClass(tag=tag_urn)
            )
        
        global_tags = GlobalTagsClass(tags=tag_associations)
        
        return MetadataChangeProposalWrapper(
            entityUrn=data_product_urn,
            aspect=global_tags,
            changeType=ChangeTypeClass.UPSERT
        )
    except Exception as e:
        logger.error(f"Error creating data product global tags MCP: {e}")
        return None


def create_data_product_glossary_terms_mcp(
    data_product_urn: str,
    terms: List[str],
    **kwargs
) -> Optional[MetadataChangeProposalWrapper]:
    """Create data product glossary terms MCP"""
    if not DATAHUB_AVAILABLE:
        logger.error("DataHub SDK not available")
        return None
    
    try:
        term_associations = []
        for term_urn in terms:
            term_associations.append(
                GlossaryTermAssociationClass(urn=term_urn)
            )
        
        glossary_terms = GlossaryTermsClass(
            terms=term_associations,
            auditStamp=AuditStampClass(
                time=int(time.time() * 1000),
                actor="urn:li:corpuser:datahub"
            )
        )
        
        return MetadataChangeProposalWrapper(
            entityUrn=data_product_urn,
            aspect=glossary_terms,
            changeType=ChangeTypeClass.UPSERT
        )
    except Exception as e:
        logger.error(f"Error creating data product glossary terms MCP: {e}")
        return None


def create_data_product_institutional_memory_mcp(
    data_product_urn: str,
    links: List[Dict[str, str]],
    **kwargs
) -> Optional[MetadataChangeProposalWrapper]:
    """Create data product institutional memory MCP"""
    if not DATAHUB_AVAILABLE:
        logger.error("DataHub SDK not available")
        return None
    
    try:
        memory_elements = []
        for link in links:
            memory_elements.append(
                InstitutionalMemoryMetadataClass(
                    url=link.get("url", ""),
                    description=link.get("description", ""),
                    createStamp=AuditStampClass(
                        time=int(time.time() * 1000),
                        actor="urn:li:corpuser:datahub"
                    )
                )
            )
        
        institutional_memory = InstitutionalMemoryClass(elements=memory_elements)
        
        return MetadataChangeProposalWrapper(
            entityUrn=data_product_urn,
            aspect=institutional_memory,
            changeType=ChangeTypeClass.UPSERT
        )
    except Exception as e:
        logger.error(f"Error creating data product institutional memory MCP: {e}")
        return None


def create_data_product_browse_paths_mcp(
    data_product_urn: str,
    paths: List[str],
    **kwargs
) -> Optional[MetadataChangeProposalWrapper]:
    """Create data product browse paths MCP"""
    if not DATAHUB_AVAILABLE:
        logger.error("DataHub SDK not available")
        return None
    
    try:
        browse_paths = BrowsePathsClass(paths=paths)
        
        return MetadataChangeProposalWrapper(
            entityUrn=data_product_urn,
            aspect=browse_paths,
            changeType=ChangeTypeClass.UPSERT
        )
    except Exception as e:
        logger.error(f"Error creating data product browse paths MCP: {e}")
        return None


def create_data_product_structured_properties_mcp(
    data_product_urn: str,
    structured_properties: List[Dict[str, Any]],
    **kwargs
) -> Optional[MetadataChangeProposalWrapper]:
    """Create data product structured properties MCP"""
    if not DATAHUB_AVAILABLE:
        logger.error("DataHub SDK not available")
        return None
    
    try:
        properties = {}
        for prop in structured_properties:
            property_urn = prop.get("propertyUrn")
            values = prop.get("values", [])
            if property_urn and values:
                properties[property_urn] = StructuredPropertyValueAssignmentClass(
                    values=values
                )
        
        structured_props = StructuredPropertiesClass(properties=properties)
        
        return MetadataChangeProposalWrapper(
            entityUrn=data_product_urn,
            aspect=structured_props,
            changeType=ChangeTypeClass.UPSERT
        )
    except Exception as e:
        logger.error(f"Error creating data product structured properties MCP: {e}")
        return None


def create_data_product_domains_mcp(
    data_product_urn: str,
    domains: List[str],
    **kwargs
) -> Optional[MetadataChangeProposalWrapper]:
    """Create data product domains MCP"""
    if not DATAHUB_AVAILABLE:
        logger.error("DataHub SDK not available")
        return None
    
    try:
        domain_list = DomainsClass(domains=domains)
        
        return MetadataChangeProposalWrapper(
            entityUrn=data_product_urn,
            aspect=domain_list,
            changeType=ChangeTypeClass.UPSERT
        )
    except Exception as e:
        logger.error(f"Error creating data product domains MCP: {e}")
        return None


def create_data_product_sub_types_mcp(
    data_product_urn: str,
    sub_types: List[str],
    **kwargs
) -> Optional[MetadataChangeProposalWrapper]:
    """Create data product sub types MCP"""
    if not DATAHUB_AVAILABLE:
        logger.error("DataHub SDK not available")
        return None
    
    try:
        sub_types_obj = SubTypesClass(typeNames=sub_types)
        
        return MetadataChangeProposalWrapper(
            entityUrn=data_product_urn,
            aspect=sub_types_obj,
            changeType=ChangeTypeClass.UPSERT
        )
    except Exception as e:
        logger.error(f"Error creating data product sub types MCP: {e}")
        return None


def create_data_product_deprecation_mcp(
    data_product_urn: str,
    deprecated: bool = False,
    deprecation_note: str = "",
    **kwargs
) -> Optional[MetadataChangeProposalWrapper]:
    """Create data product deprecation MCP"""
    if not DATAHUB_AVAILABLE:
        logger.error("DataHub SDK not available")
        return None
    
    try:
        deprecation = DeprecationClass(
            deprecated=deprecated,
            note=deprecation_note,
            decommissionTime=None
        )
        
        return MetadataChangeProposalWrapper(
            entityUrn=data_product_urn,
            aspect=deprecation,
            changeType=ChangeTypeClass.UPSERT
        )
    except Exception as e:
        logger.error(f"Error creating data product deprecation MCP: {e}")
        return None


def create_data_product_staged_changes(
    data_product_urn: str,
    name: str,
    description: Optional[str] = None,
    external_url: Optional[str] = None,
    owners: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    terms: Optional[List[str]] = None,
    domains: Optional[List[str]] = None,
    links: Optional[List[Dict[str, str]]] = None,
    custom_properties: Optional[Dict[str, str]] = None,
    structured_properties: Optional[List[Dict[str, Any]]] = None,
    sub_types: Optional[List[str]] = None,
    deprecated: bool = False,
    deprecation_note: str = "",
    include_all_aspects: bool = True,
    custom_aspects: Optional[Dict[str, Any]] = None,
    **kwargs
) -> List[MetadataChangeProposalWrapper]:
    """
    Create comprehensive data product MCPs for all supported aspects
    
    Args:
        data_product_urn: Data product URN
        name: Data product name
        description: Data product description
        external_url: External URL
        owners: List of owner URNs
        tags: List of tag URNs
        terms: List of glossary term URNs
        domains: List of domain URNs
        links: List of documentation links
        custom_properties: Custom properties
        structured_properties: Structured properties
        sub_types: Sub types
        deprecated: Whether deprecated
        deprecation_note: Deprecation note
        include_all_aspects: Include all aspects
        custom_aspects: Custom aspects
        **kwargs: Additional arguments
    
    Returns:
        List of MCP wrappers
    """
    mcps = []
    
    # Core data product properties (always included)
    properties_mcp = create_data_product_properties_mcp(
        data_product_urn=data_product_urn,
        name=name,
        description=description,
        external_url=external_url,
        custom_properties=custom_properties,
        **kwargs
    )
    if properties_mcp:
        mcps.append(properties_mcp)
    
    # Editable properties (if description provided)
    if description:
        editable_mcp = create_editable_data_product_properties_mcp(
            data_product_urn=data_product_urn,
            description=description,
            **kwargs
        )
        if editable_mcp:
            mcps.append(editable_mcp)
    
    if include_all_aspects:
        # Ownership
        if owners:
            ownership_mcp = create_data_product_ownership_mcp(
                data_product_urn=data_product_urn,
                owners=owners,
                **kwargs
            )
            if ownership_mcp:
                mcps.append(ownership_mcp)
        
        # Status (soft delete)
        status_mcp = create_data_product_status_mcp(
            data_product_urn=data_product_urn,
            removed=False,
            **kwargs
        )
        if status_mcp:
            mcps.append(status_mcp)
        
        # Global tags
        if tags:
            tags_mcp = create_data_product_global_tags_mcp(
                data_product_urn=data_product_urn,
                tags=tags,
                **kwargs
            )
            if tags_mcp:
                mcps.append(tags_mcp)
        
        # Glossary terms
        if terms:
            terms_mcp = create_data_product_glossary_terms_mcp(
                data_product_urn=data_product_urn,
                terms=terms,
                **kwargs
            )
            if terms_mcp:
                mcps.append(terms_mcp)
        
        # Institutional memory
        if links:
            memory_mcp = create_data_product_institutional_memory_mcp(
                data_product_urn=data_product_urn,
                links=links,
                **kwargs
            )
            if memory_mcp:
                mcps.append(memory_mcp)
        
        # Structured properties
        if structured_properties:
            struct_props_mcp = create_data_product_structured_properties_mcp(
                data_product_urn=data_product_urn,
                structured_properties=structured_properties,
                **kwargs
            )
            if struct_props_mcp:
                mcps.append(struct_props_mcp)
        
        # Domains
        if domains:
            domains_mcp = create_data_product_domains_mcp(
                data_product_urn=data_product_urn,
                domains=domains,
                **kwargs
            )
            if domains_mcp:
                mcps.append(domains_mcp)
        
        # Sub types
        if sub_types:
            sub_types_mcp = create_data_product_sub_types_mcp(
                data_product_urn=data_product_urn,
                sub_types=sub_types,
                **kwargs
            )
            if sub_types_mcp:
                mcps.append(sub_types_mcp)
        
        # Deprecation
        if deprecated or deprecation_note:
            deprecation_mcp = create_data_product_deprecation_mcp(
                data_product_urn=data_product_urn,
                deprecated=deprecated,
                deprecation_note=deprecation_note,
                **kwargs
            )
            if deprecation_mcp:
                mcps.append(deprecation_mcp)
    
    # Custom aspects
    if custom_aspects:
        for aspect_name, aspect_data in custom_aspects.items():
            logger.info(f"Custom aspect {aspect_name} provided but not implemented")
    
    return mcps


def save_mcps_to_files(
    mcps: List[MetadataChangeProposalWrapper],
    base_directory: str = "metadata",
    entity_id: str = "data_product"
) -> List[str]:
    """Save MCPs to individual JSON files"""
    saved_files = []
    
    # Create data_products subdirectory
    data_products_dir = os.path.join(base_directory, "data_products")
    os.makedirs(data_products_dir, exist_ok=True)
    
    for mcp in mcps:
        aspect_name = mcp.aspect.__class__.__name__.replace("Class", "").lower()
        if aspect_name.endswith("properties"):
            aspect_name = aspect_name.replace("properties", "_properties")
        
        filename = f"{entity_id}_{aspect_name}.json"
        filepath = os.path.join(data_products_dir, filename)
        
        try:
            with open(filepath, "w") as f:
                json.dump(mcp.to_obj(), f, indent=2)
            saved_files.append(filepath)
            logger.info(f"Saved MCP to {filepath}")
        except Exception as e:
            logger.error(f"Error saving MCP to {filepath}: {e}")
    
    return saved_files


def main():
    """Main function for CLI usage"""
    parser = argparse.ArgumentParser(description="Create data product MCPs")
    parser.add_argument("--data-product-id", required=True, help="Data product ID")
    parser.add_argument("--name", required=True, help="Data product name")
    parser.add_argument("--description", help="Data product description")
    parser.add_argument("--external-url", help="External URL")
    parser.add_argument("--owners", nargs="*", help="Owner URNs")
    parser.add_argument("--tags", nargs="*", help="Tag URNs")
    parser.add_argument("--terms", nargs="*", help="Glossary term URNs")
    parser.add_argument("--domains", nargs="*", help="Domain URNs")
    parser.add_argument("--base-dir", default="metadata", help="Base directory")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    
    args = parser.parse_args()
    setup_logging(args.log_level)
    
    # Generate data product URN
    data_product_urn = f"urn:li:dataProduct:{args.data_product_id}"
    
    # Create MCPs
    mcps = create_data_product_staged_changes(
        data_product_urn=data_product_urn,
        name=args.name,
        description=args.description,
        external_url=args.external_url,
        owners=args.owners or [],
        tags=args.tags or [],
        terms=args.terms or [],
        domains=args.domains or []
    )
    
    # Save MCPs
    saved_files = save_mcps_to_files(mcps, args.base_dir, args.data_product_id)
    
    print(f"Created {len(mcps)} MCPs for data product {args.data_product_id}")
    print(f"Saved {len(saved_files)} files")


if __name__ == "__main__":
    main() 