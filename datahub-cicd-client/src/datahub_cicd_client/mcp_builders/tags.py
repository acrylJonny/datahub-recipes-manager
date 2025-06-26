"""
Tag MCP Builder for DataHub.

This module provides functionality to create Metadata Change Proposals (MCPs)
for DataHub tags with all supported aspects.
"""

import logging
from typing import Any, Dict, List, Optional, Union

from .base import BaseMCPBuilder

try:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        ChangeTypeClass,
        OwnerClass,
        OwnershipClass,
        OwnershipSourceClass,
        OwnershipSourceTypeClass,
        OwnershipTypeClass,
        StatusClass,
        TagPropertiesClass,
    )

    DATAHUB_AVAILABLE = True
except ImportError as e:
    logging.warning(f"DataHub SDK not available: {e}")
    DATAHUB_AVAILABLE = False


class TagMCPBuilder(BaseMCPBuilder):
    """
    Builder for creating Tag MCPs with comprehensive aspect support.
    """

    def get_entity_type(self) -> str:
        """Get the DataHub entity type for tags."""
        return "tag"

    def create_properties_mcp(
        self,
        tag_urn: str,
        name: str,
        description: Optional[str] = None,
        color_hex: Optional[str] = None,
        **kwargs,
    ) -> Optional[Union[MetadataChangeProposalWrapper, Dict[str, Any]]]:
        """Create tag properties MCP."""
        if not DATAHUB_AVAILABLE:
            self.logger.error("DataHub SDK not available")
            return None

        try:
            properties = TagPropertiesClass(name=name, description=description, colorHex=color_hex)

            return self.create_mcp_wrapper(
                entity_urn=tag_urn, aspect=properties, change_type="UPSERT"
            )
        except Exception as e:
            self.logger.error(f"Error creating tag properties MCP: {e}")
            return None

    def create_ownership_mcp(
        self, tag_urn: str, owners: List[str], **kwargs
    ) -> Optional[Union[MetadataChangeProposalWrapper, Dict[str, Any]]]:
        """Create tag ownership MCP."""
        if not DATAHUB_AVAILABLE:
            self.logger.error("DataHub SDK not available")
            return None

        try:
            owner_objects = []
            for owner_urn in owners:
                owner_objects.append(
                    OwnerClass(
                        owner=owner_urn,
                        type=OwnershipTypeClass.DATAOWNER,
                        source=OwnershipSourceClass(type=OwnershipSourceTypeClass.MANUAL),
                    )
                )

            ownership = OwnershipClass(owners=owner_objects, lastModified=self.create_audit_stamp())

            return self.create_mcp_wrapper(
                entity_urn=tag_urn, aspect=ownership, change_type="UPSERT"
            )
        except Exception as e:
            self.logger.error(f"Error creating tag ownership MCP: {e}")
            return None

    def create_status_mcp(
        self, tag_urn: str, removed: bool = False, **kwargs
    ) -> Optional[Union[MetadataChangeProposalWrapper, Dict[str, Any]]]:
        """Create tag status MCP."""
        if not DATAHUB_AVAILABLE:
            self.logger.error("DataHub SDK not available")
            return None

        try:
            status = StatusClass(removed=removed)

            return self.create_mcp_wrapper(entity_urn=tag_urn, aspect=status, change_type="UPSERT")
        except Exception as e:
            self.logger.error(f"Error creating tag status MCP: {e}")
            return None

    def create_entity_mcps(
        self, entity_data: Dict[str, Any], include_all_aspects: bool = True, **kwargs
    ) -> List[Union[MetadataChangeProposalWrapper, Dict[str, Any]]]:
        """
        Create all MCPs for a tag entity.

        Args:
            entity_data: Tag data dictionary
            include_all_aspects: Whether to include all supported aspects
            **kwargs: Additional arguments

        Returns:
            List of MCPs for the tag
        """
        mcps = []

        tag_urn = entity_data.get("urn")
        if not tag_urn:
            tag_id = entity_data.get("id") or entity_data.get("key")
            if tag_id:
                tag_urn = f"urn:li:tag:{tag_id}"
            else:
                raise ValueError("Either 'urn', 'id', or 'key' must be provided in entity_data")

        # Required properties MCP
        name = entity_data.get("name")
        if name:
            properties_mcp = self.create_properties_mcp(
                tag_urn=tag_urn,
                name=name,
                description=entity_data.get("description"),
                color_hex=entity_data.get("color_hex"),
            )
            if properties_mcp:
                mcps.append(properties_mcp)

        # Status MCP (always include)
        status_mcp = self.create_status_mcp(
            tag_urn=tag_urn, removed=entity_data.get("removed", False)
        )
        if status_mcp:
            mcps.append(status_mcp)

        if include_all_aspects:
            # Ownership MCP
            owners = entity_data.get("owners")
            if owners:
                ownership_mcp = self.create_ownership_mcp(tag_urn, owners)
                if ownership_mcp:
                    mcps.append(ownership_mcp)

        return mcps
