"""
Metadata Test service for DataHub operations with comprehensive input/output capabilities.

This module provides functionality for managing metadata tests in DataHub,
including CRUD operations, test execution, and result tracking.
Supports both synchronous (GraphQL) and asynchronous (MCP) operations.
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.mcp_emitter import MCPEmitter
from datahub_cicd_client.graphql.queries.tests import (
    GET_METADATA_TEST_QUERY,
    SEARCH_METADATA_TESTS_QUERY,
)
from datahub_cicd_client.outputs.mcp_output.metadata_tests import MetadataTestAsyncOutput
from datahub_cicd_client.outputs.sync.metadata_tests import MetadataTestSyncOutput
from datahub_cicd_client.services.base_service import (
    BaseInputOutputService,
    BatchOperationResult,
    OperationResult,
)


class MetadataTestService(BaseInputOutputService):
    """
    Comprehensive service for managing DataHub metadata tests with input/output capabilities.

    This service combines:
    - Input operations: Reading metadata tests from DataHub (queries)
    - Output operations: Writing metadata tests to DataHub (sync GraphQL or async MCP)
    - Batch operations: Bulk processing capabilities
    - CI/CD integration: Perfect for automated metadata validation workflows
    """

    def __init__(self, connection, output_dir: Optional[str] = None):
        """
        Initialize metadata test service.

        Args:
            connection: DataHub connection instance
            output_dir: Directory for MCP file outputs (optional)
        """
        super().__init__(connection, output_dir)

        # Output services
        self.sync_output = MetadataTestSyncOutput(connection)
        self.async_output = MetadataTestAsyncOutput(output_dir)

    def _create_mcp_emitter(self) -> MCPEmitter:
        """Create metadata test-specific MCP emitter."""
        return MCPEmitter(self.output_dir)

    # ============================================
    # INPUT OPERATIONS (Reading from DataHub)
    # ============================================

    def list_metadata_tests(
        self, query: str = "*", start: int = 0, count: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List metadata tests in DataHub.

        Args:
            query: Search query to filter metadata tests
            start: Starting offset for pagination
            count: Maximum number of metadata tests to return

        Returns:
            List of metadata test objects
        """
        self.logger.info(
            f"Listing metadata tests with query: {query}, start: {start}, count: {count}"
        )

        variables = {
            "input": {
                "types": ["TEST"],
                "query": query,
                "start": start,
                "count": count,
                "filters": [],
            }
        }

        try:
            data = self.safe_execute_graphql(SEARCH_METADATA_TESTS_QUERY, variables)

            if not data or "searchAcrossEntities" not in data:
                return []

            search_results = data["searchAcrossEntities"]["searchResults"]
            tests = []

            for item in search_results:
                entity = item.get("entity")
                if not entity:
                    continue

                test = {
                    "urn": entity.get("urn"),
                    "type": entity.get("type"),
                    "properties": entity.get("properties", {}),
                    "ownership": entity.get("ownership", {}),
                }

                tests.append(test)

            return tests

        except Exception as e:
            self.logger.error(f"Error listing metadata tests: {str(e)}")
            return []

    def get_metadata_test(self, test_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a single metadata test by URN.

        Args:
            test_urn: The URN of the metadata test to retrieve

        Returns:
            Metadata test data or None if not found
        """
        try:
            variables = {"urn": test_urn}
            data = self.safe_execute_graphql(GET_METADATA_TEST_QUERY, variables)

            if not data or "test" not in data:
                return None

            return data["test"]

        except Exception as e:
            self.logger.error(f"Error getting metadata test {test_urn}: {str(e)}")
            return None

    # ============================================
    # OUTPUT OPERATIONS (Writing to DataHub)
    # ============================================

    def create_metadata_test(
        self,
        test_urn: str,
        name: str,
        description: str = "",
        category: str = "METADATA",
        custom_properties: Optional[Dict[str, str]] = None,
    ) -> OperationResult:
        """
        Create a new metadata test.

        Args:
            test_urn: URN for the metadata test
            name: Name of the metadata test
            description: Optional description
            category: Test category (e.g., "METADATA", "QUALITY", "SCHEMA")
            custom_properties: Optional custom properties

        Returns:
            OperationResult with success status and details
        """
        entity_data = {
            "urn": test_urn,
            "name": name,
            "description": description,
            "category": category,
            "customProperties": custom_properties or {},
        }

        operation = self.create_operation("create_metadata_test", entity_data)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="create_metadata_test",
                entity_urn=test_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def update_metadata_test(
        self,
        test_urn: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        category: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
    ) -> OperationResult:
        """Update an existing metadata test."""
        entity_data = {}
        if name is not None:
            entity_data["name"] = name
        if description is not None:
            entity_data["description"] = description
        if category is not None:
            entity_data["category"] = category
        if custom_properties is not None:
            entity_data["customProperties"] = custom_properties

        operation = self.create_operation("update_metadata_test", entity_data, entity_urn=test_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="update_metadata_test",
                entity_urn=test_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def delete_metadata_test(self, test_urn: str) -> OperationResult:
        """Delete a metadata test."""
        operation = self.create_operation("delete_metadata_test", {}, entity_urn=test_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="delete_metadata_test",
                entity_urn=test_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def add_metadata_test_owner(
        self,
        test_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner",
    ) -> OperationResult:
        """Add owner to metadata test."""
        operation = self.create_operation(
            "add_owner",
            {"owner_urn": owner_urn, "ownership_type": ownership_type},
            entity_urn=test_urn,
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="add_metadata_test_owner",
                entity_urn=test_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    # ============================================
    # BATCH OPERATIONS
    # ============================================

    def bulk_create_metadata_tests(self, tests_data: List[Dict[str, Any]]) -> BatchOperationResult:
        """Create multiple metadata tests in batch."""
        results = []

        for test_data in tests_data:
            result = self.create_metadata_test(
                test_urn=test_data["urn"],
                name=test_data["name"],
                description=test_data.get("description", ""),
                category=test_data.get("category", "METADATA"),
                custom_properties=test_data.get("customProperties"),
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

        if op_type == "create_metadata_test":
            return self.sync_output.create_entity(data)
        elif op_type == "update_metadata_test":
            return self.sync_output.update_entity(entity_urn, data)
        elif op_type == "delete_metadata_test":
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

        if op_type == "create_metadata_test":
            return self.async_output.create_entity(data)
        elif op_type == "update_metadata_test":
            return self.async_output.update_entity(entity_urn, data)
        elif op_type == "delete_metadata_test":
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

    def import_metadata_tests_from_json(
        self, json_data: List[Dict[str, Any]]
    ) -> BatchOperationResult:
        """Import metadata tests from JSON data."""
        return self.bulk_create_metadata_tests(json_data)

    def export_metadata_tests_to_mcps(
        self, tests_data: List[Dict[str, Any]], filename: Optional[str] = None
    ) -> Optional[str]:
        """Export metadata tests as MCPs to file."""
        # Switch to async mode temporarily
        original_sync_mode = self.sync_mode
        self.set_sync_mode(False)

        try:
            # Create MCPs for all metadata tests
            mcps = self.async_output.create_bulk_metadata_test_mcps(tests_data)
            self.async_output.mcp_emitter.add_mcps(mcps)

            # Emit to file
            return self.async_output.emit_mcps(filename)
        finally:
            # Restore original mode
            self.set_sync_mode(original_sync_mode)
