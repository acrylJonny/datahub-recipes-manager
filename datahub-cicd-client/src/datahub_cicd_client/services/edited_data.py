"""
Edited Data service for DataHub operations with comprehensive input/output capabilities.

This module provides functionality for managing editable entities and properties in DataHub,
including CRUD operations, property management, and entity editing workflows.
Supports both synchronous (GraphQL) and asynchronous (MCP) operations.
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.mcp_emitter import MCPEmitter
from datahub_cicd_client.graphql.queries.edited_data import (
    SEARCH_EDITABLE_ENTITIES_QUERY,
)
from datahub_cicd_client.outputs.mcp_output.edited_data import EditedDataAsyncOutput
from datahub_cicd_client.outputs.sync.edited_data import EditedDataSyncOutput
from datahub_cicd_client.services.base_service import (
    BaseInputOutputService,
    BatchOperationResult,
    OperationResult,
)


class EditedDataService(BaseInputOutputService):
    """
    Comprehensive service for managing DataHub edited data with input/output capabilities.

    This service combines:
    - Input operations: Reading editable entities from DataHub (queries)
    - Output operations: Writing editable entities to DataHub (sync GraphQL or async MCP)
    - Batch operations: Bulk processing capabilities
    - CI/CD integration: Perfect for automated metadata editing workflows
    """

    def __init__(self, connection, output_dir: Optional[str] = None):
        """
        Initialize edited data service.

        Args:
            connection: DataHub connection instance
            output_dir: Directory for MCP file outputs (optional)
        """
        super().__init__(connection, output_dir)

        # Output services
        self.sync_output = EditedDataSyncOutput(connection)
        self.async_output = EditedDataAsyncOutput(output_dir)

    def _create_mcp_emitter(self) -> MCPEmitter:
        """Create edited data-specific MCP emitter."""
        return MCPEmitter(self.output_dir)

    # ============================================
    # INPUT OPERATIONS (Reading from DataHub)
    # ============================================

    def list_editable_entities(self, query: str = "*", start: int = 0, count: int = 100) -> List[Dict[str, Any]]:
        """
        List editable entities in DataHub.

        Args:
            query: Search query to filter editable entities
            start: Starting offset for pagination
            count: Maximum number of editable entities to return

        Returns:
            List of editable entity objects
        """
        self.logger.info(f"Listing editable entities with query: {query}, start: {start}, count: {count}")

        variables = {
            "input": {
                "types": ["DATASET", "DASHBOARD", "CHART", "DATA_PRODUCT"],
                "query": query,
                "start": start,
                "count": count,
                "filters": [],
            }
        }

        try:
            data = self.safe_execute_graphql(SEARCH_EDITABLE_ENTITIES_QUERY, variables)
            if not data or "searchAcrossEntities" not in data:
                return []

            search_results = data["searchAcrossEntities"]["searchResults"]
            entities = []

            for item in search_results:
                entity = item.get("entity")
                if not entity:
                    continue

                editable_entity = {
                    "urn": entity.get("urn"),
                    "type": entity.get("type"),
                    "properties": entity.get("properties", {}),
                    "editableProperties": entity.get("editableProperties", {})
                }

                entities.append(editable_entity)

            return entities

        except Exception as e:
            self.logger.error(f"Error listing editable entities: {str(e)}")
            return []

    def get_editable_entity(self, entity_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a single editable entity by URN.

        Args:
            entity_urn: The URN of the editable entity to retrieve

        Returns:
            Editable entity data or None if not found
        """
        try:
            variables = {"urn": entity_urn}
            data = self.safe_execute_graphql(SEARCH_EDITABLE_ENTITIES_QUERY, variables)
            if not data or "entity" not in data:
                return None

            return data["entity"]

        except Exception as e:
            self.logger.error(f"Error getting editable entity {entity_urn}: {str(e)}")
            return None

    # ============================================
    # OUTPUT OPERATIONS (Writing to DataHub)
    # ============================================

    def update_entity_description(
        self,
        entity_urn: str,
        description: str
    ) -> OperationResult:
        """
        Update the description of an entity.

        Args:
            entity_urn: URN of the entity
            description: New description

        Returns:
            OperationResult with success status and details
        """
        entity_data = {
            "description": description
        }

        operation = self.create_operation("update_entity_description", entity_data, entity_urn=entity_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="update_entity_description",
                entity_urn=entity_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def update_entity_custom_properties(
        self,
        entity_urn: str,
        custom_properties: Dict[str, str]
    ) -> OperationResult:
        """Update custom properties of an entity."""
        entity_data = {
            "customProperties": custom_properties
        }

        operation = self.create_operation("update_entity_custom_properties", entity_data, entity_urn=entity_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="update_entity_custom_properties",
                entity_urn=entity_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def add_custom_property(
        self,
        entity_urn: str,
        key: str,
        value: str
    ) -> OperationResult:
        """Add a single custom property to an entity."""
        entity_data = {
            "customProperty": {
                "key": key,
                "value": value
            }
        }

        operation = self.create_operation("add_custom_property", entity_data, entity_urn=entity_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="add_custom_property",
                entity_urn=entity_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def remove_custom_property(
        self,
        entity_urn: str,
        key: str
    ) -> OperationResult:
        """Remove a custom property from an entity."""
        entity_data = {
            "customPropertyKey": key
        }

        operation = self.create_operation("remove_custom_property", entity_data, entity_urn=entity_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="remove_custom_property",
                entity_urn=entity_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def bulk_update_entity_properties(
        self,
        entity_urn: str,
        description: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None
    ) -> OperationResult:
        """Bulk update entity properties."""
        entity_data = {}
        if description is not None:
            entity_data["description"] = description
        if custom_properties is not None:
            entity_data["customProperties"] = custom_properties

        operation = self.create_operation("bulk_update_entity_properties", entity_data, entity_urn=entity_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="bulk_update_entity_properties",
                entity_urn=entity_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    # ============================================
    # BATCH OPERATIONS
    # ============================================

    def bulk_update_entities(self, updates_data: List[Dict[str, Any]]) -> BatchOperationResult:
        """Update multiple entities in batch."""
        results = []

        for update_data in updates_data:
            result = self.bulk_update_entity_properties(
                entity_urn=update_data["urn"],
                description=update_data.get("description"),
                custom_properties=update_data.get("customProperties")
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

        if op_type == "update_entity_description":
            return self.sync_output.update_description(entity_urn, data["description"])
        elif op_type == "update_entity_custom_properties":
            return self.sync_output.update_custom_properties(entity_urn, data["customProperties"])
        elif op_type == "add_custom_property":
            return self.sync_output.add_custom_property(
                entity_urn, data["customProperty"]["key"], data["customProperty"]["value"]
            )
        elif op_type == "remove_custom_property":
            return self.sync_output.remove_custom_property(entity_urn, data["customPropertyKey"])
        elif op_type == "bulk_update_entity_properties":
            return self.sync_output.bulk_update_properties(entity_urn, data)
        else:
            return OperationResult(
                success=False,
                operation_type=op_type,
                error_message=f"Unknown operation type: {op_type}"
            )

    def _execute_async_operation(self, operation: Dict[str, Any]) -> OperationResult:
        """Execute asynchronous operation via MCP generation."""
        op_type = operation["type"]
        data = operation["data"]
        entity_urn = operation.get("entity_urn")

        if op_type == "update_entity_description":
            return self.async_output.update_description(entity_urn, data["description"])
        elif op_type == "update_entity_custom_properties":
            return self.async_output.update_custom_properties(entity_urn, data["customProperties"])
        elif op_type == "add_custom_property":
            return self.async_output.add_custom_property(
                entity_urn, data["customProperty"]["key"], data["customProperty"]["value"]
            )
        elif op_type == "remove_custom_property":
            return self.async_output.remove_custom_property(entity_urn, data["customPropertyKey"])
        elif op_type == "bulk_update_entity_properties":
            return self.async_output.bulk_update_properties(entity_urn, data)
        else:
            return OperationResult(
                success=False,
                operation_type=op_type,
                error_message=f"Unknown operation type: {op_type}"
            )

    # ============================================
    # CONVENIENCE METHODS
    # ============================================

    def import_entity_updates_from_json(self, json_data: List[Dict[str, Any]]) -> BatchOperationResult:
        """Import entity updates from JSON data."""
        return self.bulk_update_entities(json_data)

    def export_entity_updates_to_mcps(self, updates_data: List[Dict[str, Any]], filename: Optional[str] = None) -> Optional[str]:
        """Export entity updates as MCPs to file."""
        # Switch to async mode temporarily
        original_sync_mode = self.sync_mode
        self.set_sync_mode(False)

        try:
            # Create MCPs for all entity updates
            mcps = self.async_output.create_bulk_entity_update_mcps(updates_data)
            self.async_output.mcp_emitter.add_mcps(mcps)

            # Emit to file
            return self.async_output.emit_mcps(filename)
        finally:
            # Restore original mode
            self.set_sync_mode(original_sync_mode)
