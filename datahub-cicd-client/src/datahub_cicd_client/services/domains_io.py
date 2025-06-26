"""
Domain service for DataHub operations with input/output capabilities.

This module provides functionality for managing domains in DataHub,
including CRUD operations, ownership management, and entity domain assignment.
Supports both synchronous (GraphQL) and asynchronous (MCP) operations.
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.mcp_emitter import DomainMCPEmitter
from datahub_cicd_client.outputs.mcp_output.domains import DomainAsyncOutput
from datahub_cicd_client.outputs.sync.domains import DomainSyncOutput
from datahub_cicd_client.services.base_service import (
    BaseInputOutputService,
    BatchOperationResult,
    OperationResult,
)
from datahub_cicd_client.services.domains import (
    DomainService,  # Import existing service for input operations
)


class DomainIOService(BaseInputOutputService):
    """
    Service for managing DataHub domains with input/output capabilities.

    This service combines:
    - Input operations: Reading data from DataHub (queries)
    - Output operations: Writing data to DataHub (sync GraphQL or async MCP)
    """

    def __init__(self, connection, output_dir: Optional[str] = None):
        """
        Initialize domain I/O service.

        Args:
            connection: DataHub connection instance
            output_dir: Directory for MCP file outputs (optional)
        """
        super().__init__(connection, output_dir)

        # Input service (existing functionality)
        self.input_service = DomainService(connection)

        # Output services
        self.sync_output = DomainSyncOutput(connection)
        self.async_output = DomainAsyncOutput(output_dir)

    def _create_mcp_emitter(self) -> DomainMCPEmitter:
        """Create domain-specific MCP emitter."""
        return DomainMCPEmitter(self.output_dir)

    # ============================================
    # INPUT OPERATIONS (Reading from DataHub)
    # ============================================

    def list_domains(
        self, query: str = "*", start: int = 0, count: int = 100
    ) -> List[Dict[str, Any]]:
        """List domains from DataHub."""
        return self.input_service.list_domains(query, start, count)

    def get_domain(self, domain_urn: str) -> Optional[Dict[str, Any]]:
        """Get a single domain from DataHub."""
        return self.input_service.get_domain(domain_urn)

    def get_comprehensive_domain_data(
        self, query: str = "*", start: int = 0, count: int = 100
    ) -> Dict[str, Any]:
        """Get comprehensive domains data from DataHub."""
        return self.input_service.get_comprehensive_domain_data(query, start, count)

    def find_entities_in_domain(
        self, domain_urn: str, start: int = 0, count: int = 50
    ) -> Dict[str, Any]:
        """Find entities that belong to a specific domain."""
        return self.input_service.find_entities_in_domain(domain_urn, start, count)

    # ============================================
    # OUTPUT OPERATIONS (Writing to DataHub)
    # ============================================

    def create_domain(
        self,
        domain_id: str,
        name: str,
        description: str = "",
        parent_domain: Optional[str] = None,
        owner: Optional[str] = None,
    ) -> OperationResult:
        """
        Create a new domain using current operation mode.

        Args:
            domain_id: ID for the domain
            name: Display name for the domain
            description: Optional description
            parent_domain: Optional parent domain URN
            owner: Optional owner username

        Returns:
            OperationResult with success status and details
        """
        entity_data = {
            "id": domain_id,
            "name": name,
            "description": description,
            "parentDomain": parent_domain,
            "owner": owner,
        }

        operation = self.create_operation("create_domain", entity_data)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="create_domain",
                entity_urn=f"urn:li:domain:{domain_id}",
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def update_domain(
        self,
        domain_urn: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        parent_domain: Optional[str] = None,
    ) -> OperationResult:
        """Update an existing domain."""
        entity_data = {}
        if name is not None:
            entity_data["name"] = name
        if description is not None:
            entity_data["description"] = description
        if parent_domain is not None:
            entity_data["parentDomain"] = parent_domain

        operation = self.create_operation("update_domain", entity_data, entity_urn=domain_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="update_domain",
                entity_urn=domain_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def delete_domain(self, domain_urn: str) -> OperationResult:
        """Delete a domain."""
        operation = self.create_operation("delete_domain", {}, entity_urn=domain_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="delete_domain",
                entity_urn=domain_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def add_domain_owner(
        self,
        domain_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner",
    ) -> OperationResult:
        """Add owner to domain."""
        operation = self.create_operation(
            "add_owner",
            {"owner_urn": owner_urn, "ownership_type": ownership_type},
            entity_urn=domain_urn,
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="add_domain_owner",
                entity_urn=domain_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def remove_domain_owner(
        self,
        domain_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner",
    ) -> OperationResult:
        """Remove owner from domain."""
        operation = self.create_operation(
            "remove_owner",
            {"owner_urn": owner_urn, "ownership_type": ownership_type},
            entity_urn=domain_urn,
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="remove_domain_owner",
                entity_urn=domain_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def assign_domain_to_entity(self, entity_urn: str, domain_urn: str) -> OperationResult:
        """Assign domain to entity."""
        operation = self.create_operation(
            "assign_domain", {"domain_urn": domain_urn}, entity_urn=entity_urn
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="assign_domain_to_entity",
                entity_urn=entity_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def remove_domain_from_entity(self, entity_urn: str, domain_urn: str) -> OperationResult:
        """Remove domain from entity."""
        operation = self.create_operation(
            "remove_domain", {"domain_urn": domain_urn}, entity_urn=entity_urn
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="remove_domain_from_entity",
                entity_urn=entity_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    # ============================================
    # BATCH OPERATIONS
    # ============================================

    def bulk_create_domains(self, domains_data: List[Dict[str, Any]]) -> BatchOperationResult:
        """Create multiple domains in batch."""
        results = []

        for domain_data in domains_data:
            result = self.create_domain(
                domain_id=domain_data["id"],
                name=domain_data["name"],
                description=domain_data.get("description", ""),
                parent_domain=domain_data.get("parentDomain"),
                owner=domain_data.get("owner"),
            )
            results.append(result)

        return BatchOperationResult(results)

    def bulk_assign_domain(self, domain_urn: str, entity_urns: List[str]) -> BatchOperationResult:
        """Assign a domain to multiple entities."""
        results = []

        if self.sync_mode:
            # Use sync output for individual assignments
            for entity_urn in entity_urns:
                result = self.sync_output.assign_to_entity(entity_urn, domain_urn)
                results.append(result)
        else:
            # Use async output to create bulk MCPs
            mcps = self.async_output.create_domain_assignment_mcps(domain_urn, entity_urns)
            self.async_output.mcp_emitter.add_mcps(mcps)

            for entity_urn in entity_urns:
                result = OperationResult(
                    success=True,
                    operation_type="assign_domain_to_entity",
                    entity_urn=entity_urn,
                    mcps_generated=1,
                )
                results.append(result)

        return BatchOperationResult(results)

    def bulk_entity_domain_assignment(
        self, assignments: List[Dict[str, str]]
    ) -> BatchOperationResult:
        """Assign domains to multiple entities based on assignment list."""
        results = []

        for assignment in assignments:
            result = self.assign_domain_to_entity(
                assignment["entity_urn"], assignment["domain_urn"]
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

        if op_type == "create_domain":
            return self.sync_output.create_entity(data)
        elif op_type == "update_domain":
            return self.sync_output.update_entity(entity_urn, data)
        elif op_type == "delete_domain":
            return self.sync_output.delete_entity(entity_urn)
        elif op_type == "add_owner":
            return self.sync_output.add_owner(entity_urn, data["owner_urn"], data["ownership_type"])
        elif op_type == "remove_owner":
            return self.sync_output.remove_owner(
                entity_urn, data["owner_urn"], data["ownership_type"]
            )
        elif op_type == "assign_domain":
            return self.sync_output.assign_to_entity(entity_urn, data["domain_urn"])
        elif op_type == "remove_domain":
            return self.sync_output.remove_from_entity(entity_urn, data["domain_urn"])
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

        if op_type == "create_domain":
            return self.async_output.create_entity(data)
        elif op_type == "update_domain":
            return self.async_output.update_entity(entity_urn, data)
        elif op_type == "delete_domain":
            return self.async_output.delete_entity(entity_urn)
        elif op_type == "add_owner":
            return self.async_output.add_owner(
                entity_urn, data["owner_urn"], data["ownership_type"]
            )
        elif op_type == "remove_owner":
            return self.async_output.remove_owner(
                entity_urn, data["owner_urn"], data["ownership_type"]
            )
        elif op_type == "assign_domain":
            return self.async_output.assign_to_entity(entity_urn, data["domain_urn"])
        elif op_type == "remove_domain":
            return self.async_output.remove_from_entity(entity_urn, data["domain_urn"])
        else:
            return OperationResult(
                success=False,
                operation_type=op_type,
                error_message=f"Unknown operation type: {op_type}",
            )

    # ============================================
    # CONVENIENCE METHODS
    # ============================================

    def import_domains_from_json(self, json_data: List[Dict[str, Any]]) -> BatchOperationResult:
        """Import domains from JSON data."""
        return self.bulk_create_domains(json_data)

    def export_domains_to_mcps(
        self, domains_data: List[Dict[str, Any]], filename: Optional[str] = None
    ) -> Optional[str]:
        """Export domains as MCPs to file."""
        # Switch to async mode temporarily
        original_sync_mode = self.sync_mode
        self.set_sync_mode(False)

        try:
            # Create MCPs for all domains
            mcps = self.async_output.create_bulk_domain_mcps(domains_data)
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
