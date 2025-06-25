#!/usr/bin/env python3
"""
Script to create MCP (Metadata Change Proposal) files for tags in DataHub
"""

import argparse
import json
import logging
import os
import sys
import time
from typing import Dict, Optional, Any

# Add parent directory to sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

try:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        TagPropertiesClass,
        OwnershipClass,
        OwnerClass,
        OwnershipTypeClass,
        AuditStampClass,
        ChangeTypeClass,
    )
except ImportError:
    print("DataHub SDK not found. Please install with: pip install 'acryl-datahub>=0.10.0'")
    sys.exit(1)

from utils.urn_utils import generate_deterministic_urn

# Try to import the new URN generation utilities
try:
    sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'web_ui'))
    from utils.urn_utils import generate_tag_urn, get_mutation_config_for_environment
    HAS_NEW_URN_UTILS = True
except ImportError:
    HAS_NEW_URN_UTILS = False


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


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Create MCP files for DataHub tags"
    )

    parser.add_argument(
        "--tag-id",
        required=True,
        help="Tag ID (e.g. 'PII')",
    )

    parser.add_argument(
        "--tag-name",
        help="Display name for the tag (defaults to tag ID)",
    )

    parser.add_argument(
        "--description",
        help="Description of the tag",
    )

    parser.add_argument(
        "--color-hex",
        help="Hex color code for the tag (e.g. '#FF5733')",
    )

    parser.add_argument(
        "--owner",
        required=True,
        help="Owner of the tag (DataHub username)",
    )
    
    parser.add_argument(
        "--environment",
        help="Environment name (deprecated, use --mutation-name instead)",
    )
    
    parser.add_argument(
        "--mutation-name",
        help="Mutation name for deterministic URN generation",
    )

    parser.add_argument(
        "--output-dir",
        help="Output directory for MCP files",
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)",
    )

    return parser.parse_args()


def create_tag_properties_mcp(
    tag_id: str,
    owner: str,
    tag_name: Optional[str] = None,
    description: Optional[str] = None,
    color_hex: Optional[str] = None,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
    custom_urn: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for tag properties

    Args:
        tag_id: The tag identifier
        owner: The owner of the tag
        tag_name: Display name for the tag (optional, defaults to tag_id)
        description: Description of the tag (optional)
        color_hex: Hex color code for the tag (optional)
        environment: Environment name (deprecated, use mutation_name instead)
        mutation_name: Mutation name for deterministic URN (optional)
        custom_urn: Custom URN to use instead of generating one (optional)

    Returns:
        Dictionary representation of the MCP
    """
    # Set defaults
    if tag_name is None:
        tag_name = tag_id

    # Create tag URN - use custom URN if provided, otherwise generate deterministic URN
    if custom_urn:
        tag_urn = custom_urn
    else:
        tag_urn = generate_deterministic_urn(
            "tag", tag_id, environment=environment, mutation_name=mutation_name
        )

    # Create audit stamp
    current_time = int(time.time() * 1000)  # Current time in milliseconds
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )

    # Create tag properties
    tag_properties = TagPropertiesClass(
        name=tag_name,
        description=description or f"Tag: {tag_name}",
        colorHex=color_hex
    )

    # Create the MCP
    mcp = MetadataChangeProposalWrapper(
        entityUrn=tag_urn,
        entityType="tag",
        aspectName="tagProperties",
        aspect=tag_properties,
        changeType=ChangeTypeClass.UPSERT
    )

    # Convert to dictionary for JSON serialization
    return mcp.to_obj()



def create_tag_ownership_mcp(
    tag_id: str,
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
    custom_urn: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for tag ownership

    Args:
        tag_id: The tag identifier
        owner: The owner of the tag
        environment: Environment name (deprecated, use mutation_name instead)
        mutation_name: Mutation name for deterministic URN (optional)
        custom_urn: Custom URN to use instead of generating one (optional)

    Returns:
        Dictionary representation of the MCP
    """
    # Create tag URN - use custom URN if provided, otherwise generate deterministic URN
    if custom_urn:
        tag_urn = custom_urn
    else:
        tag_urn = generate_deterministic_urn(
            "tag", tag_id, environment=environment, mutation_name=mutation_name
        )

    # Create audit stamp
    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )

    # Create ownership info
    ownership = OwnershipClass(
        owners=[
            OwnerClass(
                owner=f"urn:li:corpuser:{owner}",
                type=OwnershipTypeClass.DATAOWNER
            )
        ],
        lastModified=audit_stamp
    )

    # Create the MCP for ownership
    mcp = MetadataChangeProposalWrapper(
        entityUrn=tag_urn,
        entityType="tag",
        aspectName="ownership",
        aspect=ownership,
        changeType=ChangeTypeClass.UPSERT
    )

    # Convert to dictionary for JSON serialization
    return mcp.to_obj()


def save_mcp_to_file(mcp: Dict[str, Any], output_path: str, enable_dedup: bool = True) -> bool:
    """
    Save an MCP dictionary to a JSON file with optional deduplication

    Args:
        mcp: The MCP dictionary
        output_path: File path to save to
        enable_dedup: Whether to enable deduplication (default: True)

    Returns:
        True if file was saved, False if skipped due to deduplication
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Check for deduplication if enabled
        if enable_dedup and os.path.exists(output_path):
            try:
                with open(output_path, 'r') as f:
                    existing_mcp = json.load(f)
                
                # Compare the MCPs for equality
                if _mcps_are_equal(existing_mcp, mcp):
                    logger.info(f"MCP file unchanged, skipping: {output_path}")
                    return False
                else:
                    logger.info(f"MCP file changed, updating: {output_path}")
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Could not read existing MCP file for comparison: {e}")
                # Continue with saving if we can't read the existing file
        
        # Write to file
        with open(output_path, 'w') as f:
            json.dump(mcp, f, indent=2, default=str)
            
        logger.info(f"Saved MCP to: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to save MCP: {str(e)}")
        raise


def _mcps_are_equal(mcp1: Dict[str, Any], mcp2: Dict[str, Any]) -> bool:
    """
    Compare two MCP dictionaries for equality, ignoring timestamp fields
    
    Args:
        mcp1: First MCP dictionary
        mcp2: Second MCP dictionary
    
    Returns:
        True if MCPs are functionally equivalent, False otherwise
    """
    try:
        # Create deep copies to avoid modifying originals
        import copy
        mcp1_copy = copy.deepcopy(mcp1)
        mcp2_copy = copy.deepcopy(mcp2)
        
        # Remove timestamp fields that change on every run
        _remove_timestamp_fields(mcp1_copy)
        _remove_timestamp_fields(mcp2_copy)
        
        # Compare the sanitized MCPs
        return mcp1_copy == mcp2_copy
    except Exception as e:
        logger.warning(f"Error comparing MCPs: {e}")
        return False


def _remove_timestamp_fields(mcp: Dict[str, Any]) -> None:
    """
    Remove timestamp fields from MCP dictionary (modifies in place)
    
    Args:
        mcp: MCP dictionary to modify
    """
    try:
        # Remove common timestamp fields
        if isinstance(mcp, dict):
            # Remove auditStamp timestamps
            if "aspect" in mcp and isinstance(mcp["aspect"], dict):
                aspect = mcp["aspect"]
                
                # Remove timestamps from ownership aspect
                if "lastModified" in aspect and isinstance(aspect["lastModified"], dict):
                    if "time" in aspect["lastModified"]:
                        del aspect["lastModified"]["time"]
                
                # Remove timestamps from any other audit stamps
                for key, value in aspect.items():
                    if isinstance(value, dict) and "time" in value:
                        del value["time"]
                    elif isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict) and "time" in item:
                                del item["time"]
            
            # Remove any top-level timestamp fields
            if "systemMetadata" in mcp and isinstance(mcp["systemMetadata"], dict):
                if "lastObserved" in mcp["systemMetadata"]:
                    del mcp["systemMetadata"]["lastObserved"]
                if "runId" in mcp["systemMetadata"]:
                    del mcp["systemMetadata"]["runId"]
    except Exception as e:
        logger.warning(f"Error removing timestamp fields: {e}")


def main():
    """Main function"""
    args = parse_args()
    setup_logging(args.log_level)
    
    tag_id = args.tag_id
    
    # Use mutation name if available, otherwise fall back to environment
    mutation_name = args.mutation_name or args.environment
    env_name = args.environment or args.mutation_name or "default"
    
    # Determine output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        # Default to metadata-manager/ENVIRONMENT/tags/
        output_dir = os.path.join(
            "metadata-manager", 
            env_name, 
            "tags"
        )
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate a filename-safe version of the tag_id
    safe_tag_id = tag_id.replace(" ", "_").lower()
    
    # Get mutation configuration for environment-based URN generation
    mutation_config = None
    custom_urn = None
    if HAS_NEW_URN_UTILS and env_name != "default":
        try:
            mutation_config = get_mutation_config_for_environment(env_name)
            if mutation_config:
                # Generate a temporary URN to test mutation
                temp_urn = f"urn:li:tag:{tag_id}"
                mutated_urn = generate_tag_urn(temp_urn, env_name, mutation_config)
                if mutated_urn != temp_urn:
                    custom_urn = mutated_urn
                    logger.info(f"Using mutated URN for tag: {temp_urn} -> {mutated_urn}")
        except Exception as e:
            logger.warning(f"Could not get mutation config for environment '{env_name}': {e}")

    # Create properties MCP
    logger.info(f"Creating properties MCP for tag '{tag_id}'...")
    properties_mcp = create_tag_properties_mcp(
        tag_id=tag_id,
        owner=args.owner,
        tag_name=args.tag_name,
        description=args.description,
        color_hex=args.color_hex,
        environment=args.environment,
        mutation_name=args.mutation_name,
        custom_urn=custom_urn
    )
    
    # Save properties MCP
    properties_file = os.path.join(output_dir, f"{safe_tag_id}_properties.json")
    properties_saved = save_mcp_to_file(properties_mcp, properties_file)
    
    # Create ownership MCP
    logger.info(f"Creating ownership MCP for tag '{tag_id}'...")
    ownership_mcp = create_tag_ownership_mcp(
        tag_id=tag_id,
        owner=args.owner,
        environment=args.environment,
        mutation_name=args.mutation_name,
        custom_urn=custom_urn
    )
    
    # Save ownership MCP
    ownership_file = os.path.join(output_dir, f"{safe_tag_id}_ownership.json")
    ownership_saved = save_mcp_to_file(ownership_mcp, ownership_file)
    
    # Log deduplication results
    files_created = []
    files_skipped = []
    
    if properties_saved:
        files_created.append("properties")
    else:
        files_skipped.append("properties")
        
    if ownership_saved:
        files_created.append("ownership")
    else:
        files_skipped.append("ownership")
    
    if files_created:
        logger.info(f"Created MCP files for tag '{tag_id}': {', '.join(files_created)}")
    if files_skipped:
        logger.info(f"Skipped unchanged MCP files for tag '{tag_id}': {', '.join(files_skipped)}")
    
    logger.info(f"Tag URN: {generate_deterministic_urn('tag', tag_id, environment=args.environment, mutation_name=args.mutation_name)}")


if __name__ == "__main__":
    main() 