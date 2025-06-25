#!/usr/bin/env python3
"""
Script to create MCP (Metadata Change Proposal) files for data contracts in DataHub
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
        # Data Contract specific classes
        DataContractPropertiesClass,
        DataContractStatusClass,
        DataContractStateClass,
        SchemaContractClass,
        FreshnessContractClass,
        DataQualityContractClass,
        
        # Common aspect classes
        StatusClass,
        StructuredPropertiesClass,
        StructuredPropertyValueAssignmentClass,
        AuditStampClass,
        ChangeTypeClass,
    )
    DATAHUB_AVAILABLE = True
except ImportError:
    DATAHUB_AVAILABLE = False

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_data_contract_properties_mcp(
    data_contract_urn: str,
    entity_urn: str,
    schema_assertions: Optional[List[str]] = None,
    freshness_assertions: Optional[List[str]] = None,
    data_quality_assertions: Optional[List[str]] = None,
    raw_contract: Optional[str] = None,
    **kwargs
) -> Optional[MetadataChangeProposalWrapper]:
    """Create Data Contract Properties MCP"""
    if not DATAHUB_AVAILABLE:
        logger.warning("DataHub SDK not available, cannot create MCP")
        return None
    
    try:
        # Build schema contracts
        schema_contracts = []
        if schema_assertions:
            for assertion_urn in schema_assertions:
                schema_contracts.append(SchemaContractClass(assertion=assertion_urn))
        
        # Build freshness contracts
        freshness_contracts = []
        if freshness_assertions:
            for assertion_urn in freshness_assertions:
                freshness_contracts.append(FreshnessContractClass(assertion=assertion_urn))
        
        # Build data quality contracts
        data_quality_contracts = []
        if data_quality_assertions:
            for assertion_urn in data_quality_assertions:
                data_quality_contracts.append(DataQualityContractClass(assertion=assertion_urn))
        
        aspect = DataContractPropertiesClass(
            entity=entity_urn,
            schema=schema_contracts if schema_contracts else None,
            freshness=freshness_contracts if freshness_contracts else None,
            dataQuality=data_quality_contracts if data_quality_contracts else None,
            rawContract=raw_contract
        )
        
        return MetadataChangeProposalWrapper(
            entityType="dataContract",
            entityUrn=data_contract_urn,
            changeType=ChangeTypeClass.UPSERT,
            aspectName="dataContractProperties",
            aspect=aspect
        )
    except Exception as e:
        logger.error(f"Error creating data contract properties MCP: {e}")
        return None


def create_data_contract_status_mcp(
    data_contract_urn: str,
    state: str = "ACTIVE",
    custom_properties: Optional[Dict[str, str]] = None,
    **kwargs
) -> Optional[MetadataChangeProposalWrapper]:
    """Create Data Contract Status MCP"""
    if not DATAHUB_AVAILABLE:
        logger.warning("DataHub SDK not available, cannot create MCP")
        return None
    
    try:
        # Map string to enum
        state_enum = DataContractStateClass.ACTIVE
        if state.upper() == "PENDING":
            state_enum = DataContractStateClass.PENDING
        
        aspect = DataContractStatusClass(
            customProperties=custom_properties or {},
            state=state_enum
        )
        
        return MetadataChangeProposalWrapper(
            entityType="dataContract",
            entityUrn=data_contract_urn,
            changeType=ChangeTypeClass.UPSERT,
            aspectName="dataContractStatus",
            aspect=aspect
        )
    except Exception as e:
        logger.error(f"Error creating data contract status MCP: {e}")
        return None


def create_status_mcp(
    data_contract_urn: str,
    removed: bool = False,
    **kwargs
) -> Optional[MetadataChangeProposalWrapper]:
    """Create Status MCP"""
    if not DATAHUB_AVAILABLE:
        logger.warning("DataHub SDK not available, cannot create MCP")
        return None
    
    try:
        aspect = StatusClass(removed=removed)
        
        return MetadataChangeProposalWrapper(
            entityType="dataContract",
            entityUrn=data_contract_urn,
            changeType=ChangeTypeClass.UPSERT,
            aspectName="status",
            aspect=aspect
        )
    except Exception as e:
        logger.error(f"Error creating status MCP: {e}")
        return None


def create_structured_properties_mcp(
    data_contract_urn: str,
    properties: List[Dict[str, Any]],
    **kwargs
) -> Optional[MetadataChangeProposalWrapper]:
    """Create Structured Properties MCP"""
    if not DATAHUB_AVAILABLE:
        logger.warning("DataHub SDK not available, cannot create MCP")
        return None
    
    try:
        current_time = int(time.time() * 1000)
        audit_stamp = AuditStampClass(
            time=current_time,
            actor="urn:li:corpuser:datahub"
        )
        
        property_assignments = []
        for prop in properties:
            property_assignments.append(StructuredPropertyValueAssignmentClass(
                propertyUrn=prop.get("propertyUrn", ""),
                values=prop.get("values", []),
                created=audit_stamp,
                lastModified=audit_stamp
            ))
        
        aspect = StructuredPropertiesClass(properties=property_assignments)
        
        return MetadataChangeProposalWrapper(
            entityType="dataContract",
            entityUrn=data_contract_urn,
            changeType=ChangeTypeClass.UPSERT,
            aspectName="structuredProperties",
            aspect=aspect
        )
    except Exception as e:
        logger.error(f"Error creating structured properties MCP: {e}")
        return None


def create_data_contract_staged_changes(
    data_contract_urn: str,
    entity_urn: str,
    schema_assertions: Optional[List[str]] = None,
    freshness_assertions: Optional[List[str]] = None,
    data_quality_assertions: Optional[List[str]] = None,
    raw_contract: Optional[str] = None,
    state: str = "ACTIVE",
    custom_properties: Optional[Dict[str, str]] = None,
    structured_properties: Optional[List[Dict[str, Any]]] = None,
    include_all_aspects: bool = True,
    custom_aspects: Optional[Dict[str, Any]] = None,
    **kwargs
) -> List[MetadataChangeProposalWrapper]:
    """
    Create comprehensive staged changes for a data contract with all supported aspects.
    
    Supported aspects for Data Contract:
    1. dataContractProperties - Core data contract information (entity, schema, freshness, data quality contracts)
    2. dataContractStatus - Status information (state, custom properties)
    3. status - Soft delete status
    4. structuredProperties - Structured property assignments
    """
    if not DATAHUB_AVAILABLE:
        logger.warning("DataHub SDK not available, cannot create MCPs")
        return []
    
    mcps = []
    
    # Core data contract properties (always included)
    properties_mcp = create_data_contract_properties_mcp(
        data_contract_urn=data_contract_urn,
        entity_urn=entity_urn,
        schema_assertions=schema_assertions,
        freshness_assertions=freshness_assertions,
        data_quality_assertions=data_quality_assertions,
        raw_contract=raw_contract,
        **kwargs
    )
    if properties_mcp:
        mcps.append(properties_mcp)
    
    if include_all_aspects:
        # Data contract status
        status_mcp = create_data_contract_status_mcp(
            data_contract_urn=data_contract_urn,
            state=state,
            custom_properties=custom_properties,
            **kwargs
        )
        if status_mcp:
            mcps.append(status_mcp)
        
        # Status (always create with removed=False)
        general_status_mcp = create_status_mcp(
            data_contract_urn=data_contract_urn,
            removed=False,
            **kwargs
        )
        if general_status_mcp:
            mcps.append(general_status_mcp)
        
        # Structured properties
        if structured_properties:
            struct_props_mcp = create_structured_properties_mcp(
                data_contract_urn=data_contract_urn,
                properties=structured_properties,
                **kwargs
            )
            if struct_props_mcp:
                mcps.append(struct_props_mcp)
    
    # Custom aspects
    if custom_aspects:
        for aspect_name, aspect_data in custom_aspects.items():
            logger.info(f"Custom aspect {aspect_name} provided but not implemented")
    
    logger.info(f"Created {len(mcps)} MCPs for data contract {data_contract_urn}")
    return mcps


def save_mcps_to_files(
    mcps: List[MetadataChangeProposalWrapper],
    base_directory: str = "metadata",
    entity_id: Optional[str] = None
) -> List[str]:
    """Save MCPs to individual JSON files"""
    if not mcps:
        return []
    
    # Create directory structure
    data_contract_dir = os.path.join(base_directory, "data_contracts")
    os.makedirs(data_contract_dir, exist_ok=True)
    
    saved_files = []
    
    for mcp in mcps:
        try:
            # Generate filename
            aspect_name = mcp.aspectName
            entity_id_part = entity_id or "unknown"
            filename = f"{entity_id_part}_{aspect_name}.json"
            filepath = os.path.join(data_contract_dir, filename)
            
            # Convert MCP to dict
            mcp_dict = mcp.to_obj()
            
            # Save to file
            with open(filepath, 'w') as f:
                json.dump(mcp_dict, f, indent=2)
            
            saved_files.append(filepath)
            logger.info(f"Saved MCP to {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving MCP {mcp.aspectName}: {e}")
    
    return saved_files


def main():
    parser = argparse.ArgumentParser(description="Create Data Contract MCPs")
    parser.add_argument("--data-contract-urn", required=True, help="Data Contract URN")
    parser.add_argument("--entity-urn", required=True, help="Entity URN that this contract is for")
    parser.add_argument("--schema-assertions", nargs="*", help="Schema assertion URNs")
    parser.add_argument("--freshness-assertions", nargs="*", help="Freshness assertion URNs")
    parser.add_argument("--data-quality-assertions", nargs="*", help="Data quality assertion URNs")
    parser.add_argument("--raw-contract", help="Raw YAML contract definition")
    parser.add_argument("--state", default="ACTIVE", choices=["ACTIVE", "PENDING"], help="Contract state")
    parser.add_argument("--base-directory", default="metadata", help="Base directory for output files")
    parser.add_argument("--entity-id", help="Entity ID for file naming")
    parser.add_argument("--include-all-aspects", action="store_true", default=True, help="Include all supported aspects")
    
    args = parser.parse_args()
    
    if not DATAHUB_AVAILABLE:
        logger.error("DataHub SDK not available. Please install datahub package.")
        sys.exit(1)
    
    # Create MCPs
    mcps = create_data_contract_staged_changes(
        data_contract_urn=args.data_contract_urn,
        entity_urn=args.entity_urn,
        schema_assertions=args.schema_assertions or [],
        freshness_assertions=args.freshness_assertions or [],
        data_quality_assertions=args.data_quality_assertions or [],
        raw_contract=args.raw_contract,
        state=args.state,
        include_all_aspects=args.include_all_aspects
    )
    
    # Save to files
    saved_files = save_mcps_to_files(
        mcps=mcps,
        base_directory=args.base_directory,
        entity_id=args.entity_id
    )
    
    logger.info(f"Successfully created {len(saved_files)} MCP files for data contract {args.data_contract_urn}")


if __name__ == "__main__":
    main() 