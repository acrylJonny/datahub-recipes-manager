"""
Enhanced Tag service for DataHub operations with input/output capabilities.

This module provides functionality for managing tags in DataHub,
including CRUD operations, ownership management, and entity tagging.
Supports both synchronous (GraphQL) and asynchronous (MCP) operations.
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.mcp_emitter import TagMCPEmitter
from datahub_cicd_client.outputs.mcp_output.tags import TagAsyncOutput
from datahub_cicd_client.outputs.sync.tags import TagSyncOutput
from datahub_cicd_client.services.base_service import (
    BaseInputOutputService,
    BatchOperationResult,
    OperationResult,
)
from datahub_cicd_client.services.tags import (
    TagService,  # Import existing service for input operations
)


class EnhancedTagService(BaseInputOutputService):
    """
    Enhanced service for managing DataHub tags with input/output capabilities.

    This service combines:
    - Input operations: Reading data from DataHub (queries)
    - Output operations: Writing data to DataHub (sync GraphQL or async MCP)
    """

    def __init__(self, connection, output_dir: Optional[str] = None):
        """
        Initialize enhanced tag service.

        Args:
            connection: DataHub connection instance
            output_dir: Directory for MCP file outputs (optional)
        """
        super().__init__(connection, output_dir)

        # Input service (existing functionality)
        self.input_service = TagService(connection)

        # Output services
        self.sync_output = TagSyncOutput(connection)
        self.async_output = TagAsyncOutput(output_dir)

    def _create_mcp_emitter(self) -> TagMCPEmitter:
        """Create tag-specific MCP emitter."""
        return TagMCPEmitter(self.output_dir)

    # ============================================
    # INPUT OPERATIONS (Reading from DataHub)
    # ============================================

    def list_tags(self, query: str = "*", start: int = 0, count: int = 100) -> List[Dict[str, Any]]:
        """List tags from DataHub."""
        return self.input_service.list_tags(query, start, count)

    def get_tag(self, tag_urn: str) -> Optional[Dict[str, Any]]:
        """Get a single tag from DataHub."""
        return self.input_service.get_tag(tag_urn)

    def get_remote_tags_data(
        self, query: str = "*", start: int = 0, count: int = 100
    ) -> Dict[str, Any]:
        """Get comprehensive tags data from DataHub."""
        return self.input_service.get_remote_tags_data(query, start, count)

    def find_entities_with_tag(
        self, tag_urn: str, start: int = 0, count: int = 50
    ) -> Dict[str, Any]:
        """Find entities that have a specific tag."""
        return self.input_service.find_entities_with_tag(tag_urn, start, count)

    # ============================================
    # OUTPUT OPERATIONS (Writing to DataHub)
    # ============================================

    def create_tag(
        self,
        tag_id: str,
        name: str,
        description: str = "",
        color_hex: Optional[str] = None,
        owner: Optional[str] = None,
    ) -> OperationResult:
        """
        Create a new tag using current operation mode.

        Args:
            tag_id: ID for the tag
            name: Display name for the tag
            description: Optional description
            color_hex: Optional hex color code
            owner: Optional owner username

        Returns:
            OperationResult with success status and details
        """
        entity_data = {
            "id": tag_id,
            "name": name,
            "description": description,
            "colorHex": color_hex,
            "owner": owner,
        }

        operation = self.create_operation("create_tag", entity_data)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="create_tag",
                entity_urn=f"urn:li:tag:{tag_id}",
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def update_tag(
        self,
        tag_urn: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        color_hex: Optional[str] = None,
    ) -> OperationResult:
        """Update an existing tag."""
        entity_data = {}
        if name is not None:
            entity_data["name"] = name
        if description is not None:
            entity_data["description"] = description
        if color_hex is not None:
            entity_data["colorHex"] = color_hex

        operation = self.create_operation("update_tag", entity_data, entity_urn=tag_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="update_tag",
                entity_urn=tag_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def delete_tag(self, tag_urn: str) -> OperationResult:
        """Delete a tag."""
        operation = self.create_operation("delete_tag", {}, entity_urn=tag_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="delete_tag",
                entity_urn=tag_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def add_tag_owner(
        self,
        tag_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner",
    ) -> OperationResult:
        """Add owner to tag."""
        operation = self.create_operation(
            "add_owner",
            {"owner_urn": owner_urn, "ownership_type": ownership_type},
            entity_urn=tag_urn,
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="add_tag_owner",
                entity_urn=tag_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def remove_tag_owner(
        self,
        tag_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner",
    ) -> OperationResult:
        """Remove owner from tag."""
        operation = self.create_operation(
            "remove_owner",
            {"owner_urn": owner_urn, "ownership_type": ownership_type},
            entity_urn=tag_urn,
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="remove_tag_owner",
                entity_urn=tag_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def assign_tag_to_entity(self, entity_urn: str, tag_urn: str) -> OperationResult:
        """Assign tag to entity."""
        operation = self.create_operation("assign_tag", {"tag_urn": tag_urn}, entity_urn=entity_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="assign_tag_to_entity",
                entity_urn=entity_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def remove_tag_from_entity(self, entity_urn: str, tag_urn: str) -> OperationResult:
        """Remove tag from entity."""
        operation = self.create_operation("remove_tag", {"tag_urn": tag_urn}, entity_urn=entity_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="remove_tag_from_entity",
                entity_urn=entity_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    # ============================================
    # BATCH OPERATIONS
    # ============================================

    def bulk_create_tags(self, tags_data: List[Dict[str, Any]]) -> BatchOperationResult:
        """Create multiple tags in batch."""
        results = []

        for tag_data in tags_data:
            result = self.create_tag(
                tag_id=tag_data["id"],
                name=tag_data["name"],
                description=tag_data.get("description", ""),
                color_hex=tag_data.get("colorHex"),
                owner=tag_data.get("owner"),
            )
            results.append(result)

        return BatchOperationResult(results)

    def bulk_assign_tags(self, entity_urn: str, tag_urns: List[str]) -> BatchOperationResult:
        """Assign multiple tags to an entity."""
        results = []

        if self.sync_mode:
            # Use sync output for batch assignment
            for tag_urn in tag_urns:
                result = self.sync_output.assign_to_entity(entity_urn, tag_urn)
                results.append(result)
        else:
            # Use async output to create bulk MCPs
            mcps = self.async_output.create_entity_tagging_mcps(entity_urn, tag_urns)
            self.async_output.mcp_emitter.add_mcps(mcps)

            result = OperationResult(
                success=True,
                operation_type="bulk_assign_tags",
                entity_urn=entity_urn,
                mcps_generated=len(mcps),
            )
            results.append(result)

        return BatchOperationResult(results)

    def bulk_tag_assignment(self, tag_urn: str, entity_urns: List[str]) -> BatchOperationResult:
        """Assign a tag to multiple entities."""
        results = []

        if self.sync_mode:
            # Use sync output for individual assignments
            for entity_urn in entity_urns:
                result = self.sync_output.assign_to_entity(entity_urn, tag_urn)
                results.append(result)
        else:
            # Use async output to create bulk MCPs
            mcps = self.async_output.create_tag_assignment_mcps(tag_urn, entity_urns)
            self.async_output.mcp_emitter.add_mcps(mcps)

            for entity_urn in entity_urns:
                result = OperationResult(
                    success=True,
                    operation_type="assign_tag_to_entity",
                    entity_urn=entity_urn,
                    mcps_generated=1,
                )
                results.append(result)

        return BatchOperationResult(results)

    # ============================================
    # OPERATION EXECUTION (Internal)
    # ============================================

    def _execute_sync_operation(self, operation: Dict[str, Any]) -> OperationResult:
        """Execute synchronous operation via GraphQL."""
        op_type = operation["type"]
        data = operation["data"]
        entity_urn = operation.get("entity_urn")

        if op_type == "create_tag":
            return self.sync_output.create_entity(data)
        elif op_type == "update_tag":
            return self.sync_output.update_entity(entity_urn, data)
        elif op_type == "delete_tag":
            return self.sync_output.delete_entity(entity_urn)
        elif op_type == "add_owner":
            return self.sync_output.add_owner(entity_urn, data["owner_urn"], data["ownership_type"])
        elif op_type == "remove_owner":
            return self.sync_output.remove_owner(
                entity_urn, data["owner_urn"], data["ownership_type"]
            )
        elif op_type == "assign_tag":
            return self.sync_output.assign_to_entity(entity_urn, data["tag_urn"])
        elif op_type == "remove_tag":
            return self.sync_output.remove_from_entity(entity_urn, data["tag_urn"])
        else:
            return OperationResult(
                success=False,
                operation_type=op_type,
                error_message=f"Unknown operation type: {op_type}",
            )

    def _execute_async_operation(self, operation: Dict[str, Any]) -> OperationResult:
        """Execute asynchronous operation via MCP generation."""
        op_type = operation["type"]
        data = operation["data"]
        entity_urn = operation.get("entity_urn")

        if op_type == "create_tag":
            return self.async_output.create_entity(data)
        elif op_type == "update_tag":
            return self.async_output.update_entity(entity_urn, data)
        elif op_type == "delete_tag":
            return self.async_output.delete_entity(entity_urn)
        elif op_type == "add_owner":
            return self.async_output.add_owner(
                entity_urn, data["owner_urn"], data["ownership_type"]
            )
        elif op_type == "remove_owner":
            return self.async_output.remove_owner(
                entity_urn, data["owner_urn"], data["ownership_type"]
            )
        elif op_type == "assign_tag":
            return self.async_output.assign_to_entity(entity_urn, data["tag_urn"])
        elif op_type == "remove_tag":
            return self.async_output.remove_from_entity(entity_urn, data["tag_urn"])
        else:
            return OperationResult(
                success=False,
                operation_type=op_type,
                error_message=f"Unknown operation type: {op_type}",
            )

    # ============================================
    # CONVENIENCE METHODS
    # ============================================

    def import_tags_from_json(self, json_data: List[Dict[str, Any]]) -> BatchOperationResult:
        """Import tags from JSON data."""
        return self.bulk_create_tags(json_data)

    def export_tags_to_mcps(
        self, tags_data: List[Dict[str, Any]], filename: Optional[str] = None
    ) -> Optional[str]:
        """Export tags as MCPs to file."""
        # Switch to async mode temporarily
        original_sync_mode = self.sync_mode
        self.set_sync_mode(False)

        try:
            # Create MCPs for all tags
            mcps = self.async_output.create_bulk_tag_mcps(tags_data)
            self.async_output.mcp_emitter.add_mcps(mcps)

            # Emit to file
            return self.async_output.emit_mcps(filename)
        finally:
            # Restore original mode
            self.set_sync_mode(original_sync_mode)

    def get_operation_statistics(self) -> Dict[str, Any]:
        """Get statistics about pending operations and MCPs."""
        summary = self.get_operation_summary()

        # Add service-specific statistics
        summary.update(
            {
                "sync_output_available": self.sync_output is not None,
                "async_output_available": self.async_output is not None,
                "async_mcp_count": self.async_output.get_mcp_count() if self.async_output else 0,
            }
        )

        return summary
