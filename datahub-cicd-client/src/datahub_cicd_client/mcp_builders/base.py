"""
Base MCP Builder for DataHub entities.

This module provides the foundation for building Metadata Change Proposals (MCPs)
for different DataHub entities with proper file writing capabilities.
"""

import json
import logging
import os
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

try:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        AuditStampClass,
        ChangeTypeClass,
    )

    DATAHUB_AVAILABLE = True
except ImportError as e:
    logging.warning(f"DataHub SDK not available: {e}")
    DATAHUB_AVAILABLE = False

    # Create mock classes for type hints when DataHub SDK is not available
    class MetadataChangeProposalWrapper:
        pass

    class ChangeTypeClass:
        UPSERT = "UPSERT"


class BaseMCPBuilder(ABC):
    """
    Base class for building MCPs for DataHub entities.

    Provides common functionality for:
    - Creating MCPs with proper metadata
    - Writing MCPs to files
    - Batch operations
    - Error handling and logging
    """

    def __init__(self, output_dir: Optional[str] = None):
        """
        Initialize the MCP builder.

        Args:
            output_dir: Directory to write MCP files to (optional)
        """
        self.output_dir = output_dir
        self.logger = logging.getLogger(self.__class__.__name__)

        if not DATAHUB_AVAILABLE:
            self.logger.warning("DataHub SDK not available - MCPs will be created as dictionaries")

    @abstractmethod
    def get_entity_type(self) -> str:
        """Get the DataHub entity type for this builder."""
        pass

    def create_audit_stamp(
        self, actor: str = "urn:li:corpuser:datahub"
    ) -> Optional[AuditStampClass]:
        """Create an audit stamp for MCPs."""
        if not DATAHUB_AVAILABLE:
            return None

        return AuditStampClass(time=int(time.time() * 1000), actor=actor)

    def create_mcp_wrapper(
        self, entity_urn: str, aspect: Any, change_type: str = "UPSERT"
    ) -> Union[MetadataChangeProposalWrapper, Dict[str, Any]]:
        """
        Create an MCP wrapper.

        Args:
            entity_urn: URN of the entity
            aspect: The aspect data
            change_type: Type of change (UPSERT, CREATE, DELETE)

        Returns:
            MetadataChangeProposalWrapper if DataHub SDK available, else dict
        """
        if DATAHUB_AVAILABLE:
            return MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=aspect,
                changeType=getattr(ChangeTypeClass, change_type, ChangeTypeClass.UPSERT),
            )
        else:
            # Return dictionary representation when SDK not available
            return {
                "entityUrn": entity_urn,
                "entityType": self.get_entity_type(),
                "aspect": aspect.__dict__ if hasattr(aspect, "__dict__") else aspect,
                "aspectName": aspect.__class__.__name__
                if hasattr(aspect, "__class__")
                else str(type(aspect)),
                "changeType": change_type,
            }

    def write_mcps_to_file(
        self,
        mcps: List[Union[MetadataChangeProposalWrapper, Dict[str, Any]]],
        filename: str,
        base_directory: Optional[str] = None,
    ) -> str:
        """
        Write MCPs to a JSON file.

        Args:
            mcps: List of MCPs to write
            filename: Name of the file to write
            base_directory: Base directory (uses self.output_dir if not provided)

        Returns:
            Path to the written file
        """
        if base_directory is None:
            base_directory = self.output_dir or "metadata-manager"

        # Ensure directory exists
        os.makedirs(base_directory, exist_ok=True)

        # Convert MCPs to serializable format
        mcp_dicts = []
        for mcp in mcps:
            if DATAHUB_AVAILABLE and isinstance(mcp, MetadataChangeProposalWrapper):
                # Convert DataHub MCP to dictionary
                mcp_dict = {
                    "entityUrn": mcp.entityUrn,
                    "entityType": mcp.entityType,
                    "aspectName": mcp.aspectName,
                    "changeType": mcp.changeType,
                    "aspect": mcp.aspect.to_obj() if hasattr(mcp.aspect, "to_obj") else mcp.aspect,
                }
            else:
                # Already a dictionary
                mcp_dict = mcp

            mcp_dicts.append(mcp_dict)

        # Write to file
        file_path = os.path.join(base_directory, filename)
        with open(file_path, "w") as f:
            json.dump(mcp_dicts, f, indent=2, default=str)

        self.logger.info(f"Wrote {len(mcp_dicts)} MCPs to {file_path}")
        return file_path

    def write_single_mcp_file(
        self,
        mcps: List[Union[MetadataChangeProposalWrapper, Dict[str, Any]]],
        entity_id: str,
        base_directory: Optional[str] = None,
    ) -> str:
        """
        Write MCPs to a single mcp_file.json (following the pattern used by tags/properties).

        Args:
            mcps: List of MCPs to write
            entity_id: ID of the entity for filename
            base_directory: Base directory

        Returns:
            Path to the written file
        """
        filename = "mcp_file.json"
        return self.write_mcps_to_file(mcps, filename, base_directory)

    def create_batch_mcps(
        self, entities_data: List[Dict[str, Any]], **kwargs
    ) -> List[Union[MetadataChangeProposalWrapper, Dict[str, Any]]]:
        """
        Create MCPs for multiple entities.

        Args:
            entities_data: List of entity data dictionaries
            **kwargs: Additional arguments for MCP creation

        Returns:
            List of MCPs for all entities
        """
        all_mcps = []
        for entity_data in entities_data:
            try:
                entity_mcps = self.create_entity_mcps(entity_data, **kwargs)
                all_mcps.extend(entity_mcps)
            except Exception as e:
                self.logger.error(
                    f"Error creating MCPs for entity {entity_data.get('urn', 'unknown')}: {e}"
                )

        return all_mcps

    @abstractmethod
    def create_entity_mcps(
        self, entity_data: Dict[str, Any], **kwargs
    ) -> List[Union[MetadataChangeProposalWrapper, Dict[str, Any]]]:
        """
        Create all MCPs for a single entity.

        Args:
            entity_data: Entity data dictionary
            **kwargs: Additional arguments

        Returns:
            List of MCPs for the entity
        """
        pass

    def save_staged_changes(
        self,
        entity_data: Dict[str, Any],
        environment: str = "dev",
        base_directory: Optional[str] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Save entity changes as staged MCPs.

        Args:
            entity_data: Entity data dictionary
            environment: Environment name
            base_directory: Base directory for files
            **kwargs: Additional arguments

        Returns:
            Dictionary with operation results
        """
        try:
            # Create MCPs
            mcps = self.create_entity_mcps(entity_data, **kwargs)

            if not mcps:
                return {
                    "success": False,
                    "message": "No MCPs created",
                    "entity_id": entity_data.get("id", "unknown"),
                    "mcps_created": 0,
                    "files_saved": [],
                }

            # Determine output directory
            if base_directory is None:
                base_directory = f"metadata-manager/{environment}"

            # Save to file
            entity_id = entity_data.get("id", entity_data.get("urn", "unknown"))
            file_path = self.write_single_mcp_file(mcps, entity_id, base_directory)

            return {
                "success": True,
                "message": f"Successfully created {len(mcps)} MCPs for {self.get_entity_type()} {entity_id}",
                "entity_id": entity_id,
                "entity_urn": entity_data.get("urn"),
                "mcps_created": len(mcps),
                "files_saved": [file_path],
                "aspects_included": [
                    mcp.aspectName
                    if hasattr(mcp, "aspectName")
                    else mcp.get("aspectName", "unknown")
                    for mcp in mcps
                ],
            }

        except Exception as e:
            self.logger.error(f"Error saving staged changes for {self.get_entity_type()}: {e}")
            return {
                "success": False,
                "message": f"Error saving staged changes: {str(e)}",
                "entity_id": entity_data.get("id", "unknown"),
                "mcps_created": 0,
                "files_saved": [],
            }
