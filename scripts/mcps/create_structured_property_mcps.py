#!/usr/bin/env python3
"""
Script to create MCP (Metadata Change Proposal) files for structured properties in DataHub
"""

import argparse
import json
import logging
import os
import sys
import time
from typing import Dict, Optional, Any, List
from datetime import datetime

# Add parent directory to sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

try:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        # Structured Property specific classes
        StructuredPropertyDefinitionClass,
        PropertyValueClass,
        PropertyCardinalityClass,
        DataHubSearchConfigClass,
        SearchFieldTypeClass,
        
        # Common aspect classes
        OwnershipClass,
        OwnerClass,
        OwnershipTypeClass,
        AuditStampClass,
        ChangeTypeClass,
        StatusClass,
        InstitutionalMemoryClass,
        InstitutionalMemoryMetadataClass,
        
        # URL and other utility classes
        TimeStampClass,
    )
except ImportError:
    print("DataHub SDK not found. Please install with: pip install 'acryl-datahub>=0.10.0'")
    sys.exit(1)

from utils.urn_utils import generate_deterministic_urn


logger = logging.getLogger(__name__)


def setup_logging(log_level: str):
    """Set up logging configuration"""
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def create_structured_property_definition_mcp(
    property_id: str,
    qualified_name: str,
    value_type: str,  # e.g. "urn:li:dataType:datahub.string"
    entity_types: List[str],  # e.g. ["urn:li:entityType:datahub.dataset"]
    owner: str,
    display_name: Optional[str] = None,
    description: Optional[str] = None,
    cardinality: str = "SINGLE",  # "SINGLE" or "MULTIPLE"
    allowedValues: Optional[List[Dict[str, Any]]] = None,
    type_qualifier: Optional[Dict[str, List[str]]] = None,
    search_config: Optional[Dict[str, Any]] = None,
    show_in_search_filters: bool = False,
    show_in_asset_summary: bool = False,
    show_as_asset_badge: bool = False,
    show_in_columns_table: bool = False,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for structured property definition
    
    Args:
        property_id: The property identifier
        qualified_name: The fully qualified name (e.g. io.acryl.datahub.myProperty)
        value_type: The value type URN (e.g. urn:li:dataType:datahub.string)
        entity_types: List of entity type URNs this property applies to
        owner: The owner of the property
        display_name: Display name for the property
        description: Description of the property
        cardinality: Property cardinality ("SINGLE" or "MULTIPLE")
        allowedValues: List of allowed values with descriptions
        type_qualifier: Type specialization for the valueType
        search_config: Search configuration for the property
        is_hidden: Whether the property should be hidden
        show_in_search_filters: Whether to show in search filters
        show_in_asset_summary: Whether to show in asset summary
        show_as_asset_badge: Whether to show as asset badge
        show_in_columns_table: Whether to show in columns table
        environment: Environment name for URN generation
        mutation_name: Mutation name for URN generation
    
    Returns:
        Dictionary representation of the MCP
    """
    property_urn = generate_deterministic_urn(
        "structuredProperty", property_id, environment=environment, mutation_name=mutation_name
    )

    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )

    # Process allowed values
    property_allowed_values = None
    if allowedValues:
        property_allowed_values = []
        for value in allowedValues:
            property_allowed_values.append(PropertyValueClass(
                value=value.get("value"),
                description=value.get("description")
            ))

    # Process search config
    search_configuration = None
    if search_config:
        search_configuration = DataHubSearchConfigClass(
            fieldName=search_config.get("fieldName"),
            fieldType=SearchFieldTypeClass.from_string(search_config.get("fieldType", "KEYWORD")),
            queryByDefault=search_config.get("queryByDefault", False),
            enableAutocomplete=search_config.get("enableAutocomplete", False),
            addToFilters=search_config.get("addToFilters", False),
            addHasValuesToFilters=search_config.get("addHasValuesToFilters", True)
        )

    # Create structured property definition
    property_definition = StructuredPropertyDefinitionClass(
        qualifiedName=qualified_name,
        displayName=display_name,
        valueType=value_type,
        typeQualifier=type_qualifier,
        allowedValues=property_allowed_values,
        cardinality=getattr(PropertyCardinalityClass, cardinality.upper(), PropertyCardinalityClass.SINGLE),
        entityTypes=entity_types,
        description=description,
        searchConfiguration=search_configuration,
        
        lastModified=audit_stamp
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=property_urn,
        entityType="structuredProperty",
        aspectName="propertyDefinition",
        aspect=property_definition,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_structured_property_ownership_mcp(
    property_id: str,
    owner: str,
    ownership_type: str = "urn:li:ownershipType:dataowner",
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for structured property ownership
    """
    property_urn = generate_deterministic_urn(
        "structuredProperty", property_id, environment=environment, mutation_name=mutation_name
    )

    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )

    ownership = OwnershipClass(
        owners=[
            OwnerClass(
                owner=f"urn:li:corpuser:{owner}",
                type=OwnershipTypeClass.from_string(ownership_type),
                source=None
            )
        ],
        lastModified=audit_stamp
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=property_urn,
        entityType="structuredProperty",
        aspectName="ownership",
        aspect=ownership,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_structured_property_status_mcp(
    property_id: str,
    removed: bool = False,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for structured property status (soft delete)
    """
    property_urn = generate_deterministic_urn(
        "structuredProperty", property_id, environment=environment, mutation_name=mutation_name
    )

    status = StatusClass(removed=removed)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=property_urn,
        entityType="structuredProperty",
        aspectName="status",
        aspect=status,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_structured_property_institutional_memory_mcp(
    property_id: str,
    memory_elements: List[Dict[str, str]],  # [{"url": "...", "description": "..."}, ...]
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for structured property institutional memory
    """
    property_urn = generate_deterministic_urn(
        "structuredProperty", property_id, environment=environment, mutation_name=mutation_name
    )

    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )
    
    memory_metadata = []
    for element in memory_elements:
        memory_metadata.append(InstitutionalMemoryMetadataClass(
            url=element["url"],
            description=element["description"],
            createStamp=audit_stamp
        ))

    institutional_memory = InstitutionalMemoryClass(elements=memory_metadata)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=property_urn,
        entityType="structuredProperty",
        aspectName="institutionalMemory",
        aspect=institutional_memory,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def save_mcp_to_file(mcp: Dict[str, Any], output_path: str, enable_dedup: bool = True) -> bool:
    """
    Save an MCP to a JSON file with optional deduplication
    
    Args:
        mcp: The MCP dictionary to save
        output_path: Path where to save the file
        enable_dedup: Whether to enable deduplication (skip if file exists with same content)
    
    Returns:
        True if file was created/updated, False if skipped due to deduplication
    """
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    if enable_dedup and os.path.exists(output_path):
        # Check if the content is the same
        try:
            with open(output_path, 'r') as f:
                existing_mcp = json.load(f)
            
            # Remove timestamp fields for comparison
            mcp_copy = json.loads(json.dumps(mcp))
            existing_copy = json.loads(json.dumps(existing_mcp))
            _remove_timestamp_fields(mcp_copy)
            _remove_timestamp_fields(existing_copy)
            
            if _mcps_are_equal(mcp_copy, existing_copy):
                logger.debug(f"Skipping {output_path} - content unchanged")
                return False
        except (json.JSONDecodeError, KeyError):
            # If we can't read/parse the existing file, overwrite it
            pass
    
    # Write the file
    with open(output_path, 'w') as f:
        json.dump(mcp, f, indent=2)
    
    logger.info(f"Created MCP file: {output_path}")
    return True


def _mcps_are_equal(mcp1: Dict[str, Any], mcp2: Dict[str, Any]) -> bool:
    """
    Compare two MCPs for equality, ignoring timestamp fields
    """
    try:
        return json.dumps(mcp1, sort_keys=True) == json.dumps(mcp2, sort_keys=True)
    except Exception:
        return False


def _remove_timestamp_fields(mcp: Dict[str, Any]) -> None:
    """
    Remove timestamp fields from MCP for comparison purposes
    """
    if isinstance(mcp, dict):
        # Remove common timestamp fields
        timestamp_fields = ['time', 'created', 'lastModified', 'lastComputed']
        for field in timestamp_fields:
            mcp.pop(field, None)
        
        # Recursively process nested objects
        for value in mcp.values():
            if isinstance(value, dict):
                _remove_timestamp_fields(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        _remove_timestamp_fields(item)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create Structured Property MCPs")
    parser.add_argument("--property-id", required=True, help="Property ID")
    parser.add_argument("--qualified-name", required=True, help="Fully qualified name")
    parser.add_argument("--value-type", required=True, help="Value type URN")
    parser.add_argument("--entity-types", required=True, nargs="+", help="Entity type URNs")
    parser.add_argument("--owner", required=True, help="Owner username")
    parser.add_argument("--display-name", help="Display name")
    parser.add_argument("--description", help="Property description")
    parser.add_argument("--cardinality", default="SINGLE", choices=["SINGLE", "MULTIPLE"], help="Property cardinality")
    parser.add_argument("--environment", help="Environment name")
    parser.add_argument("--mutation-name", help="Mutation name")
    parser.add_argument("--output-dir", default="metadata/structured_properties", help="Output directory")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    
    args = parser.parse_args()
    
    setup_logging(args.log_level)
    
    # Create property definition MCP
    property_definition_mcp = create_structured_property_definition_mcp(
        property_id=args.property_id,
        qualified_name=args.qualified_name,
        value_type=args.value_type,
        entity_types=args.entity_types,
        owner=args.owner,
        display_name=args.display_name,
        description=args.description,
        cardinality=args.cardinality,
        environment=args.environment,
        mutation_name=args.mutation_name
    )
    
    # Create ownership MCP
    ownership_mcp = create_structured_property_ownership_mcp(
        property_id=args.property_id,
        owner=args.owner,
        environment=args.environment,
        mutation_name=args.mutation_name
    )
    
    # Save MCPs
    os.makedirs(args.output_dir, exist_ok=True)
    
    definition_path = os.path.join(args.output_dir, f"{args.property_id}_property_definition.json")
    ownership_path = os.path.join(args.output_dir, f"{args.property_id}_ownership.json")
    
    save_mcp_to_file(property_definition_mcp, definition_path)
    save_mcp_to_file(ownership_mcp, ownership_path)
    
    print(f"Created structured property MCPs:")
    print(f"  - Definition: {definition_path}")
    print(f"  - Ownership: {ownership_path}")


def create_structured_property_staged_changes(
    property_urn: str,
    qualified_name: str,
    display_name: Optional[str] = None,
    description: Optional[str] = None,
    value_type: str = "STRING",
    cardinality: str = "SINGLE",
    allowedValues: Optional[List[Any]] = None,
    entity_types: Optional[List[str]] = None,
    owners: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    terms: Optional[List[str]] = None,
    links: Optional[List[Dict[str, str]]] = None,
    custom_properties: Optional[Dict[str, str]] = None,
    include_all_aspects: bool = True,
    custom_aspects: Optional[Dict[str, Any]] = None,
    **kwargs
) -> List[Dict[str, Any]]:
    """
    Create comprehensive MCPs for a structured property with all aspects
    
    Args:
        property_urn: Property URN
        qualified_name: Qualified name for the property
        display_name: Display name for the property
        description: Property description
        value_type: Value type (STRING, NUMBER, etc.)
        cardinality: Cardinality (SINGLE, MULTIPLE)
        allowedValues: List of allowed values
        entity_types: List of entity types this property can be applied to
        owners: List of owner URNs
        tags: List of tag URNs
        terms: List of glossary term URNs
        links: List of documentation links
        custom_properties: Custom properties dictionary
        include_all_aspects: Whether to include all supported aspects
        custom_aspects: Custom aspects dictionary
        **kwargs: Additional arguments
    
    Returns:
        List of MCP dictionaries
    """
    mcps = []
    property_id = property_urn.split(":")[-1]
    
    # Map simple value types to DataHub value type URNs
    value_type_mapping = {
        "STRING": "urn:li:dataType:datahub.string",
        "NUMBER": "urn:li:dataType:datahub.number",
        "BOOLEAN": "urn:li:dataType:datahub.boolean",
        "DATE": "urn:li:dataType:datahub.date",
        "URN": "urn:li:dataType:datahub.urn"
    }
    
    datahub_value_type = value_type_mapping.get(value_type.upper(), "urn:li:dataType:datahub.string")
    
    # Default entity types if not provided
    if not entity_types:
        entity_types = ["urn:li:entityType:datahub.dataset"]
    
    # 1. Property Definition (always include)
    definition_mcp = create_structured_property_definition_mcp(
        property_id=property_id,
        qualified_name=qualified_name,
        value_type=datahub_value_type,
        entity_types=entity_types,
        owner=owners[0] if owners else "admin",
        display_name=display_name,
        description=description,
        cardinality=cardinality,
        allowedValues=allowedValues
    )
    mcps.append(definition_mcp)
    
    # 2. Ownership (if owners provided)
    if owners:
        ownership_mcp = create_structured_property_ownership_mcp(
            property_id=property_id,
            owner=owners[0]
        )
        mcps.append(ownership_mcp)
    
    # 3. Status (always include)
    status_mcp = create_structured_property_status_mcp(
        property_id=property_id,
        removed=False
    )
    mcps.append(status_mcp)
    
    # 4. Institutional Memory (if links provided)
    if links:
        memory_mcp = create_structured_property_institutional_memory_mcp(
            property_id=property_id,
            memory_elements=links,
            owner=owners[0] if owners else "admin"
        )
        mcps.append(memory_mcp)
    
    logger.info(f"Created {len(mcps)} MCPs for structured property {property_urn}")
    return mcps


def save_mcps_to_files(
    mcps: List[Dict[str, Any]],
    base_directory: str,
    entity_id: str
) -> List[str]:
    """
    Save multiple MCPs to individual files
    
    Args:
        mcps: List of MCP dictionaries
        base_directory: Base directory for saving files
        entity_id: Entity ID for file naming
    
    Returns:
        List of file paths created
    """
    saved_files = []
    
    # Create base directory
    os.makedirs(base_directory, exist_ok=True)
    
    for mcp in mcps:
        aspect_name = mcp.get("aspectName", "unknown")
        filename = f"{entity_id}_{aspect_name}.json"
        file_path = os.path.join(base_directory, filename)
        
        # Save the MCP
        if save_mcp_to_file(mcp, file_path):
            saved_files.append(file_path)
            logger.info(f"Saved MCP to {file_path}")
    
    return saved_files


def save_structured_property_to_single_file(
    mcps: List[Dict[str, Any]],
    base_directory: str,
    entity_id: str,
    filename: str = "mcp_file"
) -> Optional[str]:
    """
    Save structured property MCPs to a single consolidated file matching glossary format.
    
    This function creates a consolidated MCP file containing all aspects for a structured property
    in the same format as the glossary mcp_file.json:
    - mcps: Array of all MCP objects
    - metadata: Information about entities and totals
    
    Args:
        mcps: List of MCP dictionaries containing the aspects
        base_directory: Base directory for saving the file
        entity_id: Entity ID for the structured property
        filename: Name of the output file (default: "mcp_file")
    
    Returns:
        File path if successful, None if failed
    """
    if not mcps:
        logger.warning("No MCPs provided to save")
        return None
    
    # Create base directory
    os.makedirs(base_directory, exist_ok=True)
    
    # Create file path
    file_path = os.path.join(base_directory, f"{filename}.json")
    
    # Load existing MCP file or create new structure (like glossary)
    existing_mcp_data = {"mcps": [], "metadata": {"entities": []}}
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                existing_mcp_data = json.load(f)
                # Ensure required structure exists
                if "mcps" not in existing_mcp_data:
                    existing_mcp_data["mcps"] = []
                if "metadata" not in existing_mcp_data:
                    existing_mcp_data["metadata"] = {"entities": []}
                if "entities" not in existing_mcp_data["metadata"]:
                    existing_mcp_data["metadata"]["entities"] = []
            logger.info(f"Loaded existing MCP file with {len(existing_mcp_data['mcps'])} existing MCPs")
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Could not load existing MCP file: {e}. Creating new file.")
            existing_mcp_data = {"mcps": [], "metadata": {"entities": []}}
    
    # Get the entity URN from the first MCP (if available)
    entity_urn = None
    property_name = f"property_{entity_id}"
    if mcps:
        entity_urn = mcps[0].get("entityUrn")
        # Try to extract a more meaningful name if available
        first_mcp_aspect = mcps[0].get("aspect", {})
        if isinstance(first_mcp_aspect, dict):
            property_name = first_mcp_aspect.get("name", property_name)
    
    # Remove any existing MCPs for this entity URN to avoid duplicates
    if entity_urn:
        existing_mcp_data["mcps"] = [
            mcp for mcp in existing_mcp_data["mcps"] 
            if mcp.get("entityUrn") != entity_urn
        ]
        
        # Remove existing metadata entry for this entity
        existing_mcp_data["metadata"]["entities"] = [
            entity for entity in existing_mcp_data["metadata"]["entities"]
            if entity.get("datahub_entity_urn") != entity_urn
        ]
    
    # Add new MCPs
    existing_mcp_data["mcps"].extend(mcps)
    
    # Add metadata entry for this entity
    current_time = int(datetime.now().timestamp() * 1000)
    entity_metadata = {
        "entity_name": property_name,
        "entity_id": entity_id,
        "entity_type": "structuredProperty",
        "entity_urn": f"local:structuredProperty:{entity_id}",  # Local URN
        "datahub_entity_urn": entity_urn,  # DataHub URN
        "environment": "dev",  # Default environment
        "owner": "admin",
        "updated_at": current_time,
        "mcp_count": len(mcps)
    }
    existing_mcp_data["metadata"]["entities"].append(entity_metadata)
    
    # Update global metadata
    existing_mcp_data["metadata"].update({
        "total_mcps": len(existing_mcp_data["mcps"]),
        "total_entities": len(existing_mcp_data["metadata"]["entities"]),
        "last_updated": current_time,
        "environment": "dev"
    })
    
    try:
        # Save the consolidated MCP file
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(existing_mcp_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved consolidated structured property MCP to {file_path} with {len(mcps)} MCPs. Total MCPs in file: {len(existing_mcp_data['mcps'])}")
        return file_path
        
    except Exception as e:
        logger.error(f"Failed to save consolidated MCP to {file_path}: {e}")
        return None


def create_structured_property_settings_mcp(
    property_id: str,
    is_hidden: bool = False,
    show_in_search_filters: bool = False,
    show_in_asset_summary: bool = False,
    show_as_asset_badge: bool = False,
    show_in_columns_table: bool = False,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a structured property settings MCP.
    
    Args:
        property_id: Property ID for URN generation
        is_hidden: Whether the property is hidden
        show_in_search_filters: Whether to show in search filters
        show_in_asset_summary: Whether to show in asset summary
        show_as_asset_badge: Whether to show as asset badge
        show_in_columns_table: Whether to show in columns table
        environment: Environment name
        mutation_name: Mutation name
    
    Returns:
        MCP dictionary with structured property settings
    """
    # Create URN for the structured property
    property_urn = f"urn:li:structuredProperty:{property_id}"
    
    # Create the settings aspect
    settings_aspect = {
        "isHidden": is_hidden,
        "showInSearchFilters": show_in_search_filters,
        "showInAssetSummary": show_in_asset_summary,
        "showAsAssetBadge": show_as_asset_badge,
        "showInColumnsTable": show_in_columns_table,
        "lastModified": {
            "time": int(datetime.now().timestamp() * 1000),
            "actor": "urn:li:corpuser:datahub"
        }
    }
    
    # Create the MCP structure
    mcp = {
        "entityType": "structuredProperty",
        "entityUrn": property_urn,
        "changeType": "UPSERT",
        "aspectName": "structuredPropertySettings",
        "aspect": settings_aspect
    }
    
    logger.info(f"Created structured property settings MCP for {property_urn}")
    return mcp 