"""
Policy service for DataHub operations with comprehensive input/output capabilities.

This module provides functionality for managing policies in DataHub,
including CRUD operations, policy enforcement, and access control management.
Supports both synchronous (GraphQL) and asynchronous (MCP) operations.
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.mcp_emitter import MCPEmitter
from datahub_cicd_client.graphql.queries.policies import (
    GET_POLICY_QUERY,
    LIST_POLICIES_QUERY,
)
from datahub_cicd_client.outputs.mcp_output.policies import PolicyAsyncOutput
from datahub_cicd_client.outputs.sync.policies import PolicySyncOutput
from datahub_cicd_client.services.base_service import (
    BaseInputOutputService,
    BatchOperationResult,
    OperationResult,
)


class PolicyService(BaseInputOutputService):
    """
    Comprehensive service for managing DataHub policies with input/output capabilities.

    This service combines:
    - Input operations: Reading policies from DataHub (queries)
    - Output operations: Writing policies to DataHub (sync GraphQL or async MCP)
    - Batch operations: Bulk processing capabilities
    - CI/CD integration: Perfect for automated access control workflows
    """

    def __init__(self, connection, output_dir: Optional[str] = None):
        """
        Initialize policy service.

        Args:
            connection: DataHub connection instance
            output_dir: Directory for MCP file outputs (optional)
        """
        super().__init__(connection, output_dir)

        # Output services
        self.sync_output = PolicySyncOutput(connection)
        self.async_output = PolicyAsyncOutput(output_dir)

    def _create_mcp_emitter(self) -> MCPEmitter:
        """Create policy-specific MCP emitter."""
        return MCPEmitter(self.output_dir, entity_type="policy")

    # ============================================
    # INPUT OPERATIONS (Reading from DataHub)
    # ============================================

    def list_policies(self, query: str = "*", start: int = 0, count: int = 100) -> List[Dict[str, Any]]:
        """
        List policies in DataHub.

        Args:
            query: Search query to filter policies
            start: Starting offset for pagination
            count: Maximum number of policies to return

        Returns:
            List of policy objects
        """
        self.logger.info(f"Listing policies with query: {query}, start: {start}, count: {count}")

        try:
            data = self.safe_execute_graphql(LIST_POLICIES_QUERY, {"start": start, "count": count})
            if not data or "listPolicies" not in data:
                return []

            policies_data = data["listPolicies"]
            return policies_data.get("policies", [])

        except Exception as e:
            self.logger.error(f"Error listing policies: {str(e)}")
            return []

    def get_policy(self, policy_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a single policy by URN.

        Args:
            policy_urn: The URN of the policy to retrieve

        Returns:
            Policy data or None if not found
        """
        try:
            variables = {"urn": policy_urn}
            data = self.safe_execute_graphql(GET_POLICY_QUERY, variables)

            if not data or "policy" not in data:
                return None

            return data["policy"]

        except Exception as e:
            self.logger.error(f"Error getting policy {policy_urn}: {str(e)}")
            return None

    # ============================================
    # OUTPUT OPERATIONS (Writing to DataHub)
    # ============================================

    def create_policy(
        self,
        name: str,
        description: str = "",
        type: str = "METADATA",
        state: str = "ACTIVE",
        privileges: Optional[List[str]] = None,
        actors: Optional[Dict[str, Any]] = None,
        resources: Optional[Dict[str, Any]] = None
    ) -> OperationResult:
        """
        Create a new policy.

        Args:
            name: Name of the policy
            description: Optional description
            type: Policy type (e.g., "METADATA", "PLATFORM")
            state: Policy state ("ACTIVE" or "INACTIVE")
            privileges: List of privileges
            actors: Actor configuration (users, groups, etc.)
            resources: Resource configuration

        Returns:
            OperationResult with success status and details
        """
        entity_data = {
            "name": name,
            "description": description,
            "type": type,
            "state": state,
            "privileges": privileges or [],
            "actors": actors or {},
            "resources": resources or {}
        }

        operation = self.create_operation("create_policy", entity_data)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="create_policy",
                entity_urn=f"urn:li:dataHubPolicy:{name}",
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def update_policy(
        self,
        policy_urn: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        state: Optional[str] = None,
        privileges: Optional[List[str]] = None,
        actors: Optional[Dict[str, Any]] = None,
        resources: Optional[Dict[str, Any]] = None
    ) -> OperationResult:
        """Update an existing policy."""
        entity_data = {}
        if name is not None:
            entity_data["name"] = name
        if description is not None:
            entity_data["description"] = description
        if state is not None:
            entity_data["state"] = state
        if privileges is not None:
            entity_data["privileges"] = privileges
        if actors is not None:
            entity_data["actors"] = actors
        if resources is not None:
            entity_data["resources"] = resources

        operation = self.create_operation("update_policy", entity_data, entity_urn=policy_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="update_policy",
                entity_urn=policy_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def delete_policy(self, policy_urn: str) -> OperationResult:
        """Delete a policy."""
        operation = self.create_operation("delete_policy", {}, entity_urn=policy_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="delete_policy",
                entity_urn=policy_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def activate_policy(self, policy_urn: str) -> OperationResult:
        """Activate a policy."""
        return self.update_policy(policy_urn, state="ACTIVE")

    def deactivate_policy(self, policy_urn: str) -> OperationResult:
        """Deactivate a policy."""
        return self.update_policy(policy_urn, state="INACTIVE")

    # ============================================
    # BATCH OPERATIONS
    # ============================================

    def bulk_create_policies(self, policies_data: List[Dict[str, Any]]) -> BatchOperationResult:
        """Create multiple policies in batch."""
        results = []

        for policy_data in policies_data:
            result = self.create_policy(
                name=policy_data["name"],
                description=policy_data.get("description", ""),
                type=policy_data.get("type", "METADATA"),
                state=policy_data.get("state", "ACTIVE"),
                privileges=policy_data.get("privileges"),
                actors=policy_data.get("actors"),
                resources=policy_data.get("resources")
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

        if op_type == "create_policy":
            return self.sync_output.create_entity(data)
        elif op_type == "update_policy":
            return self.sync_output.update_entity(entity_urn, data)
        elif op_type == "delete_policy":
            return self.sync_output.delete_entity(entity_urn)
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

        if op_type == "create_policy":
            return self.async_output.create_entity(data)
        elif op_type == "update_policy":
            return self.async_output.update_entity(entity_urn, data)
        elif op_type == "delete_policy":
            return self.async_output.delete_entity(entity_urn)
        else:
            return OperationResult(
                success=False,
                operation_type=op_type,
                error_message=f"Unknown operation type: {op_type}"
            )

    # ============================================
    # CONVENIENCE METHODS
    # ============================================

    def import_policies_from_json(self, json_data: List[Dict[str, Any]]) -> BatchOperationResult:
        """Import policies from JSON data."""
        return self.bulk_create_policies(json_data)

    def export_policies_to_mcps(self, policies_data: List[Dict[str, Any]], filename: Optional[str] = None) -> Optional[str]:
        """Export policies as MCPs to file."""
        # Switch to async mode temporarily
        original_sync_mode = self.sync_mode
        self.set_sync_mode(False)

        try:
            # Create operations and add to batch
            for policy_data in policies_data:
                self.create_policy(**policy_data)

            # Execute batch and get file path
            batch_result = self.execute_batch()
            if batch_result.success:
                return self.async_output.get_last_written_file()

            return None
        finally:
            # Restore original sync mode
            self.set_sync_mode(original_sync_mode)
