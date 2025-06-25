#!/usr/bin/env python3
"""
Script to create MCP (Metadata Change Proposal) files for domains in DataHub
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
        # Domain-specific classes
        DomainPropertiesClass,
        
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
        BrowsePathsClass,
        InstitutionalMemoryClass,
        InstitutionalMemoryMetadataClass,
        DeprecationClass,
        SubTypesClass,
        StructuredPropertiesClass,
        StructuredPropertyValueAssignmentClass,
        FormsClass,
        FormAssociationClass,
        FormPromptAssociationClass,
        TestResultsClass,
        TestResultClass,
        TestResultTypeClass,
        DisplayPropertiesClass,
        IconPropertiesClass,
        IconLibraryClass,
        
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


def create_domain_properties_mcp(
    domain_id: str,
    domain_name: str,
    owner: str,
    description: Optional[str] = None,
    parent_domain_urn: Optional[str] = None,
    custom_properties: Optional[Dict[str, str]] = None,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for domain properties
    """
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )

    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )

    domain_properties = DomainPropertiesClass(
        name=domain_name,
        description=description,
        customProperties=custom_properties or {},
        created=audit_stamp,
        parentDomain=parent_domain_urn
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="domainProperties",
        aspect=domain_properties,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_domain_ownership_mcp(
    domain_id: str,
    owner: str,
    ownership_type: str = "urn:li:ownershipType:__system__technical_owner",
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> MetadataChangeProposalWrapper:
    """
    Create an MCP for domain ownership
    """
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )

    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )

    # Convert ownership type string to enum
    try:
        ownership_type_enum = getattr(OwnershipTypeClass, ownership_type.upper())
    except AttributeError:
        # Fallback to DATAOWNER if the type is not recognized
        ownership_type_enum = OwnershipTypeClass.DATAOWNER

    ownership = OwnershipClass(
        owners=[
            OwnerClass(
                owner=f"urn:li:corpuser:{owner}",
                type=ownership_type_enum,
                source=None
            )
        ],
        lastModified=audit_stamp
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="ownership",
        aspect=ownership,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp


def create_domain_status_mcp(
    domain_id: str,
    removed: bool = False,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> MetadataChangeProposalWrapper:
    """
    Create an MCP for domain status (soft delete)
    """
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )

    status = StatusClass(removed=removed)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="status",
        aspect=status,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp


def create_domain_global_tags_mcp(
    domain_id: str,
    tags: List[str],
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> MetadataChangeProposalWrapper:
    """
    Create an MCP for domain global tags
    """
    # Import URN utilities with graceful fallback
    try:
        from utils.urn_utils import (
            get_mutation_config_for_environment,
            generate_mutated_urn
        )
        urn_utils_available = True
    except ImportError:
        urn_utils_available = False
        logger.warning("URN utilities not available - using original URNs")
    
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )
    
    # Get mutation configuration if URN utilities are available
    mutation_config = None
    if urn_utils_available and (environment or mutation_name):
        env_name = environment or mutation_name
        mutation_config = get_mutation_config_for_environment(env_name)
    
    tag_associations = []
    for tag in tags:
        tag_urn = f"urn:li:tag:{tag}"
        
        # Apply URN mutation to the tag URN if utilities are available
        if urn_utils_available and mutation_config and (environment or mutation_name):
            env_name = environment or mutation_name
            tag_urn = generate_mutated_urn(
                tag_urn, env_name, 'tag', mutation_config
            )
        
        tag_associations.append(TagAssociationClass(
            tag=tag_urn,  # Use mutated URN
            context=None,
            attribution=None
        ))

    global_tags = GlobalTagsClass(tags=tag_associations)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="globalTags",
        aspect=global_tags,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp


def create_domain_glossary_terms_mcp(
    domain_id: str,
    glossary_terms: List[str],
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> MetadataChangeProposalWrapper:
    """
    Create an MCP for domain glossary terms associations
    """
    # Import URN utilities with graceful fallback
    try:
        from utils.urn_utils import (
            get_mutation_config_for_environment,
            generate_mutated_urn
        )
        urn_utils_available = True
    except ImportError:
        urn_utils_available = False
        logger.warning("URN utilities not available - using original URNs")
    
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )

    # Get mutation configuration if URN utilities are available
    mutation_config = None
    if urn_utils_available and (environment or mutation_name):
        env_name = environment or mutation_name
        mutation_config = get_mutation_config_for_environment(env_name)

    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )
    
    term_associations = []
    for term in glossary_terms:
        term_urn = f"urn:li:glossaryTerm:{term}"
        
        # Apply URN mutation to the glossary term URN if utilities are available
        if urn_utils_available and mutation_config and (environment or mutation_name):
            env_name = environment or mutation_name
            term_urn = generate_mutated_urn(
                term_urn, env_name, 'glossaryTerm', mutation_config
            )
        
        term_associations.append(GlossaryTermAssociationClass(
            urn=term_urn,  # Use mutated URN
            actor=f"urn:li:corpuser:{owner}",
            context=None,
            attribution=None
        ))

    glossary_terms_aspect = GlossaryTermsClass(
        terms=term_associations,
        auditStamp=audit_stamp
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="glossaryTerms",
        aspect=glossary_terms_aspect,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp


def create_domain_browse_paths_mcp(
    domain_id: str,
    browse_paths: List[str],
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for domain browse paths
    """
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )

    browse_paths_aspect = BrowsePathsClass(paths=browse_paths)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="browsePaths",
        aspect=browse_paths_aspect,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_domain_institutional_memory_mcp(
    domain_id: str,
    memory_elements: List[Dict[str, str]],  # [{"url": "...", "description": "..."}, ...]
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> MetadataChangeProposalWrapper:
    """
    Create an MCP for domain institutional memory
    """
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
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
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="institutionalMemory",
        aspect=institutional_memory,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp


def create_domain_structured_properties_mcp(
    domain_id: str,
    properties: List[Dict[str, Any]],  # [{"propertyUrn": "...", "values": [...]}, ...]
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> MetadataChangeProposalWrapper:
    """
    Create an MCP for domain structured properties
    """
    # Import URN utilities with graceful fallback
    try:
        from utils.urn_utils import (
            get_mutation_config_for_environment,
            generate_mutated_urn
        )
        urn_utils_available = True
    except ImportError:
        urn_utils_available = False
        logger.warning("URN utilities not available - using original URNs")
    
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )

    # Get mutation configuration if URN utilities are available
    mutation_config = None
    if urn_utils_available and (environment or mutation_name):
        env_name = environment or mutation_name
        mutation_config = get_mutation_config_for_environment(env_name)

    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )
    
    property_assignments = []
    for prop in properties:
        prop_urn = prop["propertyUrn"]
        
        # Apply URN mutation to the property URN if utilities are available
        if urn_utils_available and mutation_config and (environment or mutation_name):
            env_name = environment or mutation_name
            prop_urn = generate_mutated_urn(
                prop_urn, env_name, 'structuredProperty', mutation_config
            )
        
        property_assignments.append(StructuredPropertyValueAssignmentClass(
            propertyUrn=prop_urn,  # Use mutated URN
            values=prop["values"],
            created=audit_stamp,
            lastModified=audit_stamp
        ))

    structured_properties = StructuredPropertiesClass(properties=property_assignments)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="structuredProperties",
        aspect=structured_properties,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp


def create_domain_forms_mcp(
    domain_id: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
    incomplete_forms: List[str] = None,
    completed_forms: List[str] = None,
) -> MetadataChangeProposalWrapper:
    """
    Create an MCP for domain forms
    """
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )

    incomplete_form_associations = []
    if incomplete_forms:
        for form_urn in incomplete_forms:
            incomplete_form_associations.append(FormAssociationClass(
                urn=form_urn,
                incompletePrompts=[],
                completedPrompts=[]
            ))

    completed_form_associations = []
    if completed_forms:
        for form_urn in completed_forms:
            completed_form_associations.append(FormAssociationClass(
                urn=form_urn,
                incompletePrompts=[],
                completedPrompts=[]
            ))

    forms = FormsClass(
        incompleteForms=incomplete_form_associations,
        completedForms=completed_form_associations,
        verifications=[]
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="forms",
        aspect=forms,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp


def create_domain_test_results_mcp(
    domain_id: str,
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
    failing_tests: List[str] = None,
    passing_tests: List[str] = None,
) -> MetadataChangeProposalWrapper:
    """
    Create an MCP for domain test results
    """
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )

    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )

    failing_test_results = []
    if failing_tests:
        for test_urn in failing_tests:
            failing_test_results.append(TestResultClass(
                test=test_urn,
                type=TestResultTypeClass.FAILURE,
                testDefinitionMd5=None,
                lastComputed=audit_stamp
            ))

    passing_test_results = []
    if passing_tests:
        for test_urn in passing_tests:
            passing_test_results.append(TestResultClass(
                test=test_urn,
                type=TestResultTypeClass.SUCCESS,
                testDefinitionMd5=None,
                lastComputed=audit_stamp
            ))

    test_results = TestResultsClass(
        failing=failing_test_results,
        passing=passing_test_results
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="testResults",
        aspect=test_results,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp


def create_domain_display_properties_mcp(
    domain_id: str,
    color_hex: Optional[str] = None,
    icon_library: Optional[str] = None,
    icon_name: Optional[str] = None,
    icon_style: Optional[str] = None,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> MetadataChangeProposalWrapper:
    """
    Create an MCP for domain display properties
    """
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )

    icon_properties = None
    if icon_library and icon_name and icon_style:
        icon_properties = IconPropertiesClass(
            iconLibrary=IconLibraryClass.from_string(icon_library),
            name=icon_name,
            style=icon_style
        )

    display_properties = DisplayPropertiesClass(
        colorHex=color_hex,
        icon=icon_properties
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="displayProperties",
        aspect=display_properties,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp


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


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Create MCP files for domains")
    parser.add_argument("domain_id", help="Domain ID")
    parser.add_argument("--name", help="Domain name (defaults to domain_id)")
    parser.add_argument("--description", help="Domain description")
    parser.add_argument("--owner", default="admin", help="Owner username")
    parser.add_argument("--environment", default="dev", help="Environment name")
    parser.add_argument("--mutation-name", help="Mutation name for URN generation")
    parser.add_argument("--output-dir", help="Output directory")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    parser.add_argument("--parent-domain", help="Parent domain URN")
    parser.add_argument("--color-hex", help="Domain color in hex format")
    parser.add_argument("--icon-name", help="Icon name")
    parser.add_argument("--icon-style", default="solid", help="Icon style")
    parser.add_argument("--icon-library", default="material", help="Icon library")
    
    return parser.parse_args()


def main():
    """Main function"""
    args = parse_args()
    setup_logging(args.log_level)
    
    domain_id = args.domain_id
    domain_name = args.name or domain_id
    
    # Use mutation name if available, otherwise fall back to environment
    mutation_name = args.mutation_name or args.environment
    env_name = args.environment or args.mutation_name or "default"
    
    # Determine output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        # Default to metadata-manager/ENVIRONMENT/domains/
        output_dir = os.path.join(
            "metadata-manager", 
            env_name, 
            "domains"
        )
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate a filename-safe version of the domain_id
    safe_domain_id = domain_id.replace(" ", "_").lower()
    
    # Create properties MCP
    logger.info(f"Creating properties MCP for domain '{domain_id}'...")
    properties_mcp = create_domain_properties_mcp(
        domain_id=domain_id,
        domain_name=domain_name,
        owner=args.owner,
        description=args.description,
        parent_domain_urn=args.parent_domain,
        environment=args.environment,
        mutation_name=args.mutation_name
    )
    
    # Save properties MCP
    properties_file = os.path.join(output_dir, f"{safe_domain_id}_properties.json")
    properties_saved = save_mcp_to_file(properties_mcp, properties_file)
    
    # Create ownership MCP
    logger.info(f"Creating ownership MCP for domain '{domain_id}'...")
    ownership_mcp = create_domain_ownership_mcp(
        domain_id=domain_id,
        owner=args.owner,
        environment=args.environment,
        mutation_name=args.mutation_name
    )
    
    # Save ownership MCP
    ownership_file = os.path.join(output_dir, f"{safe_domain_id}_ownership.json")
    ownership_saved = save_mcp_to_file(ownership_mcp.to_obj(), ownership_file)
    
    # Create status MCP
    logger.info(f"Creating status MCP for domain '{domain_id}'...")
    status_mcp = create_domain_status_mcp(
        domain_id=domain_id,
        environment=args.environment,
        mutation_name=args.mutation_name
    )
    
    # Save status MCP
    status_file = os.path.join(output_dir, f"{safe_domain_id}_status.json")
    status_saved = save_mcp_to_file(status_mcp.to_obj(), status_file)
    
    # Create display properties MCP if display properties are provided
    display_saved = False
    if args.color_hex or args.icon_name:
        logger.info(f"Creating display properties MCP for domain '{domain_id}'...")
        display_mcp = create_domain_display_properties_mcp(
            domain_id=domain_id,
            color_hex=args.color_hex,
            icon_name=args.icon_name,
            icon_style=args.icon_style,
            icon_library=args.icon_library,
            environment=args.environment,
            mutation_name=args.mutation_name
        )
        
        # Save display properties MCP
        display_file = os.path.join(output_dir, f"{safe_domain_id}_display_properties.json")
        display_saved = save_mcp_to_file(display_mcp.to_obj(), display_file)
    
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
        
    if status_saved:
        files_created.append("status")
    else:
        files_skipped.append("status")
        
    if display_saved:
        files_created.append("display_properties")
    else:
        files_skipped.append("display_properties")
    
    if files_created:
        logger.info(f"Created MCP files for domain '{domain_id}': {', '.join(files_created)}")
    if files_skipped:
        logger.info(f"Skipped unchanged MCP files for domain '{domain_id}': {', '.join(files_skipped)}")
    
    logger.info(f"Domain URN: {generate_deterministic_urn('domain', domain_id, environment=args.environment, mutation_name=args.mutation_name)}")


if __name__ == "__main__":
    main()


def create_domain_staged_changes(
    domain_urn: str,
    name: str,
    description: Optional[str] = None,
    owners: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    terms: Optional[List[str]] = None,
    links: Optional[List[Dict[str, str]]] = None,
    custom_properties: Optional[Dict[str, str]] = None,
    structured_properties: Optional[List[Dict[str, Any]]] = None,
    forms: Optional[List[Dict[str, Any]]] = None,
    test_results: Optional[List[Dict[str, Any]]] = None,
    display_properties: Optional[Dict[str, Any]] = None,
    parent_domain: Optional[str] = None,
    include_all_aspects: bool = True,
    custom_aspects: Optional[Dict[str, Any]] = None,
    custom_urn: Optional[str] = None,
    **kwargs
) -> List[Dict[str, Any]]:
    """
    Create comprehensive MCPs for a domain with all aspects
    
    Args:
        domain_urn: Domain URN
        name: Domain name
        description: Domain description
        owners: List of owner URNs
        tags: List of tag URNs
        terms: List of glossary term URNs
        links: List of documentation links
        custom_properties: Custom properties dictionary
        structured_properties: List of structured properties
        forms: List of form associations
        test_results: List of test results
        display_properties: Display properties (color, icon, etc.)
        parent_domain: Parent domain URN
        include_all_aspects: Whether to include all supported aspects
        custom_aspects: Custom aspects dictionary
        **kwargs: Additional arguments
    
    Returns:
        List of MCP dictionaries
    """
    mcps = []
    # Use custom_urn if provided, otherwise use the passed domain_urn
    effective_urn = custom_urn if custom_urn else domain_urn
    domain_id = effective_urn.split(":")[-1]
    
    # 1. Domain Properties (always include)
    properties_mcp = create_domain_properties_mcp(
        domain_id=domain_id,
        domain_name=name,
        owner=owners[0] if owners else "admin",
        description=description,
        parent_domain_urn=parent_domain,
        custom_properties=custom_properties or {}
    )
    mcps.append(properties_mcp)
    
    # 2. Ownership (always include)
    if owners:
        ownership_mcp = create_domain_ownership_mcp(
            domain_id=domain_id,
            owner=owners[0]
        )
        mcps.append(ownership_mcp)
    
    # 3. Status (always include)
    status_mcp = create_domain_status_mcp(
        domain_id=domain_id,
        removed=False
    )
    mcps.append(status_mcp)
    
    # 4. Global Tags (if tags provided)
    if tags:
        tags_mcp = create_domain_global_tags_mcp(
            domain_id=domain_id,
            tags=tags,
            owner=owners[0] if owners else "admin"
        )
        mcps.append(tags_mcp)
    
    # 5. Glossary Terms (if terms provided)
    if terms:
        terms_mcp = create_domain_glossary_terms_mcp(
            domain_id=domain_id,
            glossary_terms=terms,
            owner=owners[0] if owners else "admin"
        )
        mcps.append(terms_mcp)
    
    # 6. Institutional Memory (if links provided)
    if links:
        memory_mcp = create_domain_institutional_memory_mcp(
            domain_id=domain_id,
            memory_elements=links,
            owner=owners[0] if owners else "admin"
        )
        mcps.append(memory_mcp)
    
    # 7. Structured Properties (if provided)
    if structured_properties:
        props_mcp = create_domain_structured_properties_mcp(
            domain_id=domain_id,
            properties=structured_properties,
            owner=owners[0] if owners else "admin"
        )
        mcps.append(props_mcp)
    
    # 8. Forms (if provided)
    if forms:
        forms_mcp = create_domain_forms_mcp(
            domain_id=domain_id,
            completed_forms=[f.get("urn") for f in forms if f.get("urn")]
        )
        mcps.append(forms_mcp)
    
    # 9. Test Results (if provided)
    if test_results:
        test_mcp = create_domain_test_results_mcp(
            domain_id=domain_id,
            owner=owners[0] if owners else "admin",
            passing_tests=[t.get("urn") for t in test_results if t.get("status") == "PASS" and t.get("urn")],
            failing_tests=[t.get("urn") for t in test_results if t.get("status") == "FAIL" and t.get("urn")]
        )
        mcps.append(test_mcp)
    
    # 10. Display Properties (if provided)
    if display_properties:
        display_mcp = create_domain_display_properties_mcp(
            domain_id=domain_id,
            color_hex=display_properties.get("colorHex"),
            icon_library=display_properties.get("icon", {}).get("library"),
            icon_name=display_properties.get("icon", {}).get("name"),
            icon_style=display_properties.get("icon", {}).get("style")
        )
        mcps.append(display_mcp)
    
    logger.info(f"Created {len(mcps)} MCPs for domain {domain_urn}")
    logger.info(f"MCPS: {mcps}")
    return mcps


def save_mcps_to_files(
    mcps: List[Dict[str, Any]],
    base_directory: str,
    entity_id: str
) -> List[str]:
    """
    Save multiple MCPs to a single mcp_file.json containing all MCPs as a list
    
    Args:
        mcps: List of MCP dictionaries
        base_directory: Base directory for saving files
        entity_id: Entity ID for deduplication
    
    Returns:
        List of file paths created
    """
    saved_files = []
    
    # Create base directory
    os.makedirs(base_directory, exist_ok=True)
    
    # Use constant filename
    mcp_file_path = os.path.join(base_directory, "mcp_file.json")
    
    # Load existing MCP file or create new list - should be a simple list of MCPs
    existing_mcps = []
    if os.path.exists(mcp_file_path):
        try:
            with open(mcp_file_path, "r") as f:
                file_content = json.load(f)
                # Handle both old format (with metadata wrapper) and new format (simple list)
                if isinstance(file_content, list):
                    existing_mcps = file_content
                elif isinstance(file_content, dict) and "mcps" in file_content:
                    # Migrate from old format - extract just the MCPs
                    existing_mcps = file_content["mcps"]
                    logger.info(f"Migrating from old format - extracted {len(existing_mcps)} MCPs")
                else:
                    logger.warning(f"Unknown MCP file format, starting fresh")
                    existing_mcps = []
            logger.info(f"Loaded existing MCP file with {len(existing_mcps)} existing MCPs")
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Could not load existing MCP file: {e}. Creating new file.")
            existing_mcps = []
    
    # Convert MCPs to dictionaries if needed
    new_mcps = []
    for mcp in mcps:
        if isinstance(mcp, dict):
            mcp_dict = mcp
        else:
            mcp_dict = mcp.to_obj() if hasattr(mcp, "to_obj") else dict(mcp)
        new_mcps.append(mcp_dict)
    
    # Remove any existing MCPs for this domain to avoid duplicates
    # Get the domain URN from the first MCP
    domain_entity_urn = None
    if new_mcps:
        domain_entity_urn = new_mcps[0].get("entityUrn")
    
    if domain_entity_urn:
        existing_mcps = [
            mcp for mcp in existing_mcps 
            if mcp.get("entityUrn") != domain_entity_urn
        ]
    
    # Add new MCPs to the list
    existing_mcps.extend(new_mcps)
    
    # Save updated MCP file as a simple list
    mcp_saved = save_mcp_to_file(existing_mcps, mcp_file_path)
    
    if mcp_saved:
        saved_files.append(mcp_file_path)
    
    logger.info(f"Successfully added domain '{entity_id}' to staged changes with {len(new_mcps)} MCPs. Total MCPs in file: {len(existing_mcps)}")
    
    return saved_files


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