"""
Glossary service for DataHub operations with input/output capabilities.

This module provides functionality for managing glossary entities in DataHub,
including CRUD operations for nodes and terms, ownership management, and term-entity assignment.
Supports both synchronous (GraphQL) and asynchronous (MCP) operations.
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.mcp_emitter import GlossaryMCPEmitter
from datahub_cicd_client.outputs.mcp_output.glossary import GlossaryAsyncOutput
from datahub_cicd_client.outputs.sync.glossary import GlossarySyncOutput
from datahub_cicd_client.services.base_service import (
    BaseInputOutputService,
    BatchOperationResult,
    OperationResult,
)
from datahub_cicd_client.services.glossary import (
    GlossaryService,  # Import existing service for input operations
)


class GlossaryIOService(BaseInputOutputService):
    """
    Service for managing DataHub glossary with input/output capabilities.

    This service combines:
    - Input operations: Reading data from DataHub (queries)
    - Output operations: Writing data to DataHub (sync GraphQL or async MCP)
    """

    def __init__(self, connection, output_dir: Optional[str] = None):
        """
        Initialize glossary I/O service.

        Args:
            connection: DataHub connection instance
            output_dir: Directory for MCP file outputs (optional)
        """
        super().__init__(connection, output_dir)

        # Input service (existing functionality)
        self.input_service = GlossaryService(connection)

        # Output services
        self.sync_output = GlossarySyncOutput(connection)
        self.async_output = GlossaryAsyncOutput(output_dir)

    def _create_mcp_emitter(self) -> GlossaryMCPEmitter:
        """Create glossary-specific MCP emitter."""
        return GlossaryMCPEmitter(self.output_dir)

    # ============================================
    # INPUT OPERATIONS (Reading from DataHub)
    # ============================================

    def list_glossary_nodes(
        self, query: str = "*", start: int = 0, count: int = 100
    ) -> List[Dict[str, Any]]:
        """List glossary nodes from DataHub."""
        return self.input_service.list_glossary_nodes(query, start, count)

    def list_glossary_terms(
        self, query: str = "*", start: int = 0, count: int = 100
    ) -> List[Dict[str, Any]]:
        """List glossary terms from DataHub."""
        return self.input_service.list_glossary_terms(query, start, count)

    def get_glossary_node(self, node_urn: str) -> Optional[Dict[str, Any]]:
        """Get a single glossary node from DataHub."""
        return self.input_service.get_glossary_node(node_urn)

    def get_glossary_term(self, term_urn: str) -> Optional[Dict[str, Any]]:
        """Get a single glossary term from DataHub."""
        return self.input_service.get_glossary_term(term_urn)

    def get_comprehensive_glossary_data(
        self, query: str = "*", start: int = 0, count: int = 100
    ) -> Dict[str, Any]:
        """Get comprehensive glossary data from DataHub."""
        return self.input_service.get_comprehensive_glossary_data(query, start, count)

    def find_entities_with_glossary_term(
        self, term_urn: str, start: int = 0, count: int = 50
    ) -> Dict[str, Any]:
        """Find entities that have a specific glossary term."""
        return self.input_service.find_entities_with_glossary_term(term_urn, start, count)

    # ============================================
    # OUTPUT OPERATIONS (Writing to DataHub)
    # ============================================

    def create_glossary_node(
        self,
        node_id: str,
        name: str,
        description: str = "",
        parent_node: Optional[str] = None,
        owner: Optional[str] = None,
    ) -> OperationResult:
        """
        Create a new glossary node using current operation mode.

        Args:
            node_id: ID for the glossary node
            name: Display name for the node
            description: Optional description
            parent_node: Optional parent node URN
            owner: Optional owner username

        Returns:
            OperationResult with success status and details
        """
        entity_data = {
            "id": node_id,
            "name": name,
            "description": description,
            "parentNode": parent_node,
            "owner": owner,
            "entity_type": "node",
        }

        operation = self.create_operation("create_glossary_node", entity_data)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="create_glossary_node",
                entity_urn=f"urn:li:glossaryNode:{node_id}",
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def create_glossary_term(
        self,
        term_id: str,
        name: str,
        description: str = "",
        parent_node: Optional[str] = None,
        term_source: str = "INTERNAL",
        owner: Optional[str] = None,
    ) -> OperationResult:
        """
        Create a new glossary term using current operation mode.

        Args:
            term_id: ID for the glossary term
            name: Display name for the term
            description: Optional description
            parent_node: Optional parent node URN
            term_source: Source of the term (INTERNAL or EXTERNAL)
            owner: Optional owner username

        Returns:
            OperationResult with success status and details
        """
        entity_data = {
            "id": term_id,
            "name": name,
            "description": description,
            "parentNode": parent_node,
            "termSource": term_source,
            "owner": owner,
            "entity_type": "term",
        }

        operation = self.create_operation("create_glossary_term", entity_data)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="create_glossary_term",
                entity_urn=f"urn:li:glossaryTerm:{term_id}",
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def update_glossary_entity(
        self,
        entity_urn: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        parent_node: Optional[str] = None,
        term_source: Optional[str] = None,
    ) -> OperationResult:
        """Update an existing glossary entity (node or term)."""
        entity_data = {}
        if name is not None:
            entity_data["name"] = name
        if description is not None:
            entity_data["description"] = description
        if parent_node is not None:
            entity_data["parentNode"] = parent_node
        if term_source is not None:
            entity_data["termSource"] = term_source

        operation = self.create_operation(
            "update_glossary_entity", entity_data, entity_urn=entity_urn
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="update_glossary_entity",
                entity_urn=entity_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def delete_glossary_entity(self, entity_urn: str) -> OperationResult:
        """Delete a glossary entity (node or term)."""
        operation = self.create_operation("delete_glossary_entity", {}, entity_urn=entity_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="delete_glossary_entity",
                entity_urn=entity_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def add_glossary_owner(
        self,
        entity_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner",
    ) -> OperationResult:
        """Add owner to glossary entity."""
        operation = self.create_operation(
            "add_owner",
            {"owner_urn": owner_urn, "ownership_type": ownership_type},
            entity_urn=entity_urn,
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="add_glossary_owner",
                entity_urn=entity_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def remove_glossary_owner(
        self,
        entity_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner",
    ) -> OperationResult:
        """Remove owner from glossary entity."""
        operation = self.create_operation(
            "remove_owner",
            {"owner_urn": owner_urn, "ownership_type": ownership_type},
            entity_urn=entity_urn,
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="remove_glossary_owner",
                entity_urn=entity_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def assign_glossary_term_to_entity(self, entity_urn: str, term_urn: str) -> OperationResult:
        """Assign glossary term to entity."""
        operation = self.create_operation(
            "assign_glossary_term", {"term_urn": term_urn}, entity_urn=entity_urn
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="assign_glossary_term_to_entity",
                entity_urn=entity_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def remove_glossary_term_from_entity(self, entity_urn: str, term_urn: str) -> OperationResult:
        """Remove glossary term from entity."""
        operation = self.create_operation(
            "remove_glossary_term", {"term_urn": term_urn}, entity_urn=entity_urn
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="remove_glossary_term_from_entity",
                entity_urn=entity_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    # ============================================
    # BATCH OPERATIONS
    # ============================================

    def bulk_create_glossary_entities(
        self, entities_data: List[Dict[str, Any]]
    ) -> BatchOperationResult:
        """Create multiple glossary entities in batch."""
        results = []

        for entity_data in entities_data:
            entity_type = entity_data.get("entity_type", "term")

            if entity_type == "node":
                result = self.create_glossary_node(
                    node_id=entity_data["id"],
                    name=entity_data["name"],
                    description=entity_data.get("description", ""),
                    parent_node=entity_data.get("parentNode"),
                    owner=entity_data.get("owner"),
                )
            else:
                result = self.create_glossary_term(
                    term_id=entity_data["id"],
                    name=entity_data["name"],
                    description=entity_data.get("description", ""),
                    parent_node=entity_data.get("parentNode"),
                    term_source=entity_data.get("termSource", "INTERNAL"),
                    owner=entity_data.get("owner"),
                )

            results.append(result)

        return BatchOperationResult(results)

    def bulk_assign_glossary_term(
        self, term_urn: str, entity_urns: List[str]
    ) -> BatchOperationResult:
        """Assign a glossary term to multiple entities."""
        results = []

        if self.sync_mode:
            # Use sync output for individual assignments
            for entity_urn in entity_urns:
                result = self.sync_output.assign_to_entity(entity_urn, term_urn)
                results.append(result)
        else:
            # Use async output to create bulk MCPs
            mcps = self.async_output.create_glossary_term_assignment_mcps(term_urn, entity_urns)
            self.async_output.mcp_emitter.add_mcps(mcps)

            for entity_urn in entity_urns:
                result = OperationResult(
                    success=True,
                    operation_type="assign_glossary_term_to_entity",
                    entity_urn=entity_urn,
                    mcps_generated=1,
                )
                results.append(result)

        return BatchOperationResult(results)

    def bulk_entity_glossary_term_assignment(
        self, assignments: List[Dict[str, str]]
    ) -> BatchOperationResult:
        """Assign glossary terms to multiple entities based on assignment list."""
        results = []

        for assignment in assignments:
            result = self.assign_glossary_term_to_entity(
                assignment["entity_urn"], assignment["term_urn"]
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

        if op_type == "create_glossary_node":
            return self.sync_output.create_glossary_node(data)
        elif op_type == "create_glossary_term":
            return self.sync_output.create_glossary_term(data)
        elif op_type == "update_glossary_entity":
            return self.sync_output.update_entity(entity_urn, data)
        elif op_type == "delete_glossary_entity":
            return self.sync_output.delete_entity(entity_urn)
        elif op_type == "add_owner":
            return self.sync_output.add_owner(entity_urn, data["owner_urn"], data["ownership_type"])
        elif op_type == "remove_owner":
            return self.sync_output.remove_owner(
                entity_urn, data["owner_urn"], data["ownership_type"]
            )
        elif op_type == "assign_glossary_term":
            return self.sync_output.assign_to_entity(entity_urn, data["term_urn"])
        elif op_type == "remove_glossary_term":
            return self.sync_output.remove_from_entity(entity_urn, data["term_urn"])
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

        if op_type == "create_glossary_node":
            return self.async_output.create_entity(data)
        elif op_type == "create_glossary_term":
            return self.async_output.create_entity(data)
        elif op_type == "update_glossary_entity":
            return self.async_output.update_entity(entity_urn, data)
        elif op_type == "delete_glossary_entity":
            return self.async_output.delete_entity(entity_urn)
        elif op_type == "add_owner":
            return self.async_output.add_owner(
                entity_urn, data["owner_urn"], data["ownership_type"]
            )
        elif op_type == "remove_owner":
            return self.async_output.remove_owner(
                entity_urn, data["owner_urn"], data["ownership_type"]
            )
        elif op_type == "assign_glossary_term":
            return self.async_output.assign_to_entity(entity_urn, data["term_urn"])
        elif op_type == "remove_glossary_term":
            return self.async_output.remove_from_entity(entity_urn, data["term_urn"])
        else:
            return OperationResult(
                success=False,
                operation_type=op_type,
                error_message=f"Unknown operation type: {op_type}",
            )

    # ============================================
    # CONVENIENCE METHODS
    # ============================================

    def import_glossary_from_json(self, json_data: List[Dict[str, Any]]) -> BatchOperationResult:
        """Import glossary entities from JSON data."""
        return self.bulk_create_glossary_entities(json_data)

    def export_glossary_to_mcps(
        self, glossary_data: List[Dict[str, Any]], filename: Optional[str] = None
    ) -> Optional[str]:
        """Export glossary entities as MCPs to file."""
        # Switch to async mode temporarily
        original_sync_mode = self.sync_mode
        self.set_sync_mode(False)

        try:
            # Create MCPs for all glossary entities
            mcps = self.async_output.create_bulk_glossary_mcps(glossary_data)
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
