"""
Asynchronous MCP output operations for edited data in DataHub.

This module provides functionality for generating MCP (Metadata Change Proposal) files
for edited data changes that can be applied to DataHub later.
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.mcp_emitter import MCPEmitter
from datahub_cicd_client.outputs.mcp_output.base_mcp_output import BaseAsyncOutput


class EditedDataAsyncOutput(BaseAsyncOutput):
    """
    Asynchronous MCP output service for edited data operations.

    This service handles generation of MCP files for edited data changes
    that can be applied to DataHub later through ingestion.
    """

    def __init__(self, output_dir: Optional[str] = None):
        """
        Initialize edited data async output service.

        Args:
            output_dir: Directory for MCP file outputs
        """
        super().__init__(output_dir)
        self.entity_type = "edited_data"

    def _create_mcp_emitter(self) -> MCPEmitter:
        """Create MCP emitter for edited data."""
        return MCPEmitter(self.output_dir, entity_type=self.entity_type)

    def _generate_entity_urn(self, entity_data: Dict[str, Any]) -> str:
        """Generate entity URN from entity data."""
        # This is a simplified URN generation - in practice, this would need
        # to be more sophisticated based on the entity type and data
        return entity_data.get("urn", f"urn:li:dataset:unknown_{hash(str(entity_data))}")

    def create_entity_mcps(self, entity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create MCPs for entity creation (not applicable for edited data)."""
        return []

    def delete_entity_mcps(self, entity_urn: str) -> List[Dict[str, Any]]:
        """Create MCPs for entity deletion (not applicable for edited data)."""
        return []

    def update_entity_mcps(
        self, entity_urn: str, entity_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Create MCPs for entity updates."""
        mcps = []

        if "description" in entity_data:
            mcps.append(self.create_entity_description_mcp(entity_urn, entity_data["description"]))

        if "custom_properties" in entity_data:
            mcps.append(
                self.create_custom_properties_mcp(entity_urn, entity_data["custom_properties"])
            )

        return mcps

    def create_entity_description_mcp(self, entity_urn: str, description: str) -> Dict[str, Any]:
        """
        Create MCP for updating an entity's description.

        Args:
            entity_urn: URN of the entity to update
            description: New description

        Returns:
            MCP dictionary
        """
        return {
            "entityType": "dataset",  # This would need to be determined from the URN
            "entityUrn": entity_urn,
            "changeType": "UPSERT",
            "aspectName": "editableDatasetProperties",
            "aspect": {"description": description},
        }

    def create_custom_properties_mcp(
        self, entity_urn: str, custom_properties: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Create MCP for updating an entity's custom properties.

        Args:
            entity_urn: URN of the entity to update
            custom_properties: Dictionary of custom properties

        Returns:
            MCP dictionary
        """
        return {
            "entityType": "dataset",  # This would need to be determined from the URN
            "entityUrn": entity_urn,
            "changeType": "UPSERT",
            "aspectName": "datasetProperties",
            "aspect": {"customProperties": custom_properties},
        }

    def generate_entity_update_mcps(
        self,
        entity_urn: str,
        description: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Generate MCPs for entity updates.

        Args:
            entity_urn: URN of the entity to update
            description: New description (optional)
            custom_properties: Dictionary of custom properties (optional)

        Returns:
            List of MCP dictionaries
        """
        mcps = []

        if description is not None:
            mcps.append(self.create_entity_description_mcp(entity_urn, description))

        if custom_properties:
            mcps.append(self.create_custom_properties_mcp(entity_urn, custom_properties))

        return mcps

    def write_entity_updates_to_file(
        self, updates: List[Dict[str, Any]], filename: Optional[str] = None
    ) -> Optional[str]:
        """
        Write entity updates to MCP file.

        Args:
            updates: List of entity update dictionaries
            filename: Output filename (optional)

        Returns:
            Path to the generated file or None if failed
        """
        try:
            # Convert updates to MCPs
            all_mcps = []
            for update in updates:
                entity_urn = update.get("entity_urn")
                description = update.get("description")
                custom_properties = update.get("custom_properties")

                if entity_urn:
                    mcps = self.generate_entity_update_mcps(
                        entity_urn, description, custom_properties
                    )
                    all_mcps.extend(mcps)

            # Add MCPs to emitter
            self.mcp_emitter.add_mcps(all_mcps)

            # Write to file
            return self.emit_mcps(filename)

        except Exception as e:
            self.logger.error(f"Error writing entity updates to file: {str(e)}")
            return None

    def export_entity_updates(
        self, updates_data: List[Dict[str, Any]], filename: Optional[str] = None
    ) -> Optional[str]:
        """
        Export entity updates to MCP file.

        Args:
            updates_data: List of entity update data
            filename: Output filename (optional)

        Returns:
            Path to the generated file or None if failed
        """
        return self.write_entity_updates_to_file(updates_data, filename)
