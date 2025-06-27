"""
Assertion service for DataHub operations with comprehensive input/output capabilities.

This module provides functionality for managing assertions in DataHub,
including CRUD operations, ownership management, and assertion validation.
Supports both synchronous (GraphQL) and asynchronous (MCP) operations.
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.mcp_emitter import MCPEmitter
from datahub_cicd_client.graphql.queries.assertions import (
    GET_ASSERTION_QUERY,
    SEARCH_ASSERTIONS_QUERY,
)
from datahub_cicd_client.outputs.mcp_output.assertions import AssertionAsyncOutput
from datahub_cicd_client.outputs.sync.assertions import AssertionSyncOutput
from datahub_cicd_client.services.base_service import (
    BaseInputOutputService,
    BatchOperationResult,
    OperationResult,
)


class AssertionService(BaseInputOutputService):
    """
    Comprehensive service for managing DataHub assertions with input/output capabilities.

    This service combines:
    - Input operations: Reading assertions from DataHub (queries)
    - Output operations: Writing assertions to DataHub (sync GraphQL or async MCP)
    - Batch operations: Bulk processing capabilities
    - CI/CD integration: Perfect for automated data quality workflows
    """

    def __init__(self, connection, output_dir: Optional[str] = None):
        """
        Initialize assertion service.

        Args:
            connection: DataHub connection instance
            output_dir: Directory for MCP file outputs (optional)
        """
        super().__init__(connection, output_dir)

        # Output services
        self.sync_output = AssertionSyncOutput(connection)
        self.async_output = AssertionAsyncOutput(output_dir)

    def _create_mcp_emitter(self) -> MCPEmitter:
        """Create assertion-specific MCP emitter."""
        return MCPEmitter(self.output_dir)

    # ============================================
    # INPUT OPERATIONS (Reading from DataHub)
    # ============================================

    def list_assertions(
        self, query: str = "*", start: int = 0, count: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List assertions in DataHub.

        Args:
            query: Search query to filter assertions
            start: Starting offset for pagination
            count: Maximum number of assertions to return

        Returns:
            List of assertion objects
        """
        self.logger.info(f"Listing assertions with query: {query}, start: {start}, count: {count}")

        variables = {
            "input": {
                "types": ["ASSERTION"],
                "query": query,
                "start": start,
                "count": count,
                "filters": [],
            }
        }

        try:
            data = self.safe_execute_graphql(SEARCH_ASSERTIONS_QUERY, variables)

            if not data or "searchAcrossEntities" not in data:
                return []

            search_results = data["searchAcrossEntities"]["searchResults"]
            assertions = []

            for item in search_results:
                entity = item.get("entity")
                if not entity:
                    continue

                assertion = {
                    "urn": entity.get("urn"),
                    "type": entity.get("type"),
                    "info": entity.get("info", {}),
                    "ownership": entity.get("ownership", {}),
                }

                assertions.append(assertion)

            return assertions

        except Exception as e:
            self.logger.error(f"Error listing assertions: {str(e)}")
            return []

    def get_assertion(self, assertion_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a single assertion by URN.

        Args:
            assertion_urn: The URN of the assertion to retrieve

        Returns:
            Assertion data or None if not found
        """
        try:
            variables = {"urn": assertion_urn}
            data = self.safe_execute_graphql(GET_ASSERTION_QUERY, variables)

            if not data or "assertion" not in data:
                return None

            return data["assertion"]

        except Exception as e:
            self.logger.error(f"Error getting assertion {assertion_urn}: {str(e)}")
            return None

    # ============================================
    # OUTPUT OPERATIONS (Writing to DataHub)
    # ============================================

    def create_assertion(
        self,
        assertion_urn: str,
        assertion_type: str,
        description: str = "",
        custom_properties: Optional[Dict[str, str]] = None,
    ) -> OperationResult:
        """
        Create a new assertion.

        Args:
            assertion_urn: URN for the assertion
            assertion_type: Type of assertion (e.g., "FIELD", "VOLUME", "FRESHNESS")
            description: Optional description
            custom_properties: Optional custom properties

        Returns:
            OperationResult with success status and details
        """
        entity_data = {
            "urn": assertion_urn,
            "type": assertion_type,
            "description": description,
            "customProperties": custom_properties or {},
        }

        operation = self.create_operation("create_assertion", entity_data)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="create_assertion",
                entity_urn=assertion_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def update_assertion(
        self,
        assertion_urn: str,
        description: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
    ) -> OperationResult:
        """Update an existing assertion."""
        entity_data = {}
        if description is not None:
            entity_data["description"] = description
        if custom_properties is not None:
            entity_data["customProperties"] = custom_properties

        operation = self.create_operation("update_assertion", entity_data, entity_urn=assertion_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="update_assertion",
                entity_urn=assertion_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def delete_assertion(self, assertion_urn: str) -> OperationResult:
        """Delete an assertion."""
        operation = self.create_operation("delete_assertion", {}, entity_urn=assertion_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="delete_assertion",
                entity_urn=assertion_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def add_assertion_owner(
        self,
        assertion_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner",
    ) -> OperationResult:
        """Add owner to assertion."""
        operation = self.create_operation(
            "add_owner",
            {"owner_urn": owner_urn, "ownership_type": ownership_type},
            entity_urn=assertion_urn,
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="add_assertion_owner",
                entity_urn=assertion_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    # ============================================
    # BATCH OPERATIONS
    # ============================================

    def bulk_create_assertions(self, assertions_data: List[Dict[str, Any]]) -> BatchOperationResult:
        """Create multiple assertions in batch."""
        results = []

        for assertion_data in assertions_data:
            result = self.create_assertion(
                assertion_urn=assertion_data["urn"],
                assertion_type=assertion_data["type"],
                description=assertion_data.get("description", ""),
                custom_properties=assertion_data.get("customProperties"),
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

        if op_type == "create_assertion":
            return self.sync_output.create_entity(data)
        elif op_type == "update_assertion":
            return self.sync_output.update_entity(entity_urn, data)
        elif op_type == "delete_assertion":
            return self.sync_output.delete_entity(entity_urn)
        elif op_type == "add_owner":
            return self.sync_output.add_owner(entity_urn, data["owner_urn"], data["ownership_type"])
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

        if op_type == "create_assertion":
            return self.async_output.create_entity(data)
        elif op_type == "update_assertion":
            return self.async_output.update_entity(entity_urn, data)
        elif op_type == "delete_assertion":
            return self.async_output.delete_entity(entity_urn)
        elif op_type == "add_owner":
            return self.async_output.add_owner(
                entity_urn, data["owner_urn"], data["ownership_type"]
            )
        else:
            return OperationResult(
                success=False,
                operation_type=op_type,
                error_message=f"Unknown operation type: {op_type}",
            )

    # ============================================
    # CONVENIENCE METHODS
    # ============================================

    def import_assertions_from_json(self, json_data: List[Dict[str, Any]]) -> BatchOperationResult:
        """Import assertions from JSON data."""
        return self.bulk_create_assertions(json_data)

    def export_assertions_to_mcps(
        self, assertions_data: List[Dict[str, Any]], filename: Optional[str] = None
    ) -> Optional[str]:
        """Export assertions as MCPs to file."""
        # Switch to async mode temporarily
        original_sync_mode = self.sync_mode
        self.set_sync_mode(False)

        try:
            # Create MCPs for all assertions
            mcps = self.async_output.create_bulk_assertion_mcps(assertions_data)
            self.async_output.mcp_emitter.add_mcps(mcps)

            # Emit to file
            return self.async_output.emit_mcps(filename)
        finally:
            # Restore original mode
            self.set_sync_mode(original_sync_mode)

    def count_assertions(self, query: str = "*") -> int:
        """
        Get the count of assertions matching the query.
        
        Args:
            query: Search query to filter assertions
            
        Returns:
            Number of assertions matching the query
        """
        try:
            assertions = self.list_assertions(query=query, start=0, count=1)
            # Since we don't have a direct count query, we'll need to get all and count
            # This is not optimal but works for the UI
            all_assertions = self.list_assertions(query=query, start=0, count=10000)
            return len(all_assertions) if all_assertions else 0
        except Exception as e:
            self.logger.error(f"Error counting assertions: {str(e)}")
            return 0
