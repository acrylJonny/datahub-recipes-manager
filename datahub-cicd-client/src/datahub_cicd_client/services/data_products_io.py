"""
Data Products service for DataHub operations with input/output capabilities.

This module provides functionality for managing data products in DataHub,
including CRUD operations, ownership management, asset management, and metadata assignment.
Supports both synchronous (GraphQL) and asynchronous (MCP) operations.
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.mcp_emitter import DataProductMCPEmitter
from datahub_cicd_client.outputs.mcp_output.data_products import DataProductAsyncOutput
from datahub_cicd_client.outputs.sync.data_products import DataProductSyncOutput
from datahub_cicd_client.services.base_service import (
    BaseInputOutputService,
    BatchOperationResult,
    OperationResult,
)
from datahub_cicd_client.services.data_products import (
    DataProductService,  # Import existing service for input operations
)


class DataProductIOService(BaseInputOutputService):
    """
    Service for managing DataHub data products with input/output capabilities.

    This service combines:
    - Input operations: Reading data from DataHub (queries)
    - Output operations: Writing data to DataHub (sync GraphQL or async MCP)
    """

    def __init__(self, connection, output_dir: Optional[str] = None):
        """
        Initialize data products I/O service.

        Args:
            connection: DataHub connection instance
            output_dir: Directory for MCP file outputs (optional)
        """
        super().__init__(connection, output_dir)

        # Input service (existing functionality)
        self.input_service = DataProductService(connection)

        # Output services
        self.sync_output = DataProductSyncOutput(connection)
        self.async_output = DataProductAsyncOutput(output_dir)

    def _create_mcp_emitter(self) -> DataProductMCPEmitter:
        """Create data product-specific MCP emitter."""
        return DataProductMCPEmitter(self.output_dir)

    # ============================================
    # INPUT OPERATIONS (Reading from DataHub)
    # ============================================

    def list_data_products(self, query: str = "*", start: int = 0, count: int = 100) -> List[Dict[str, Any]]:
        """List data products from DataHub."""
        return self.input_service.list_data_products(query, start, count)

    def get_data_product(self, data_product_urn: str) -> Optional[Dict[str, Any]]:
        """Get a single data product from DataHub."""
        return self.input_service.get_data_product(data_product_urn)

    def get_data_product_assets(self, data_product_urn: str, start: int = 0, count: int = 100) -> Dict[str, Any]:
        """Get assets for a data product."""
        return self.input_service.get_data_product_assets(data_product_urn, start, count)

    def find_data_products_by_domain(self, domain_urn: str, start: int = 0, count: int = 50) -> Dict[str, Any]:
        """Find data products in a specific domain."""
        return self.input_service.find_data_products_by_domain(domain_urn, start, count)

    def find_data_products_by_owner(self, owner_urn: str, start: int = 0, count: int = 50) -> Dict[str, Any]:
        """Find data products owned by a specific user."""
        return self.input_service.find_data_products_by_owner(owner_urn, start, count)

    # ============================================
    # OUTPUT OPERATIONS (Writing to DataHub)
    # ============================================

    def create_data_product(
        self,
        data_product_id: str,
        name: str,
        description: str = "",
        domain: Optional[str] = None,
        assets: Optional[List[str]] = None,
        owner: Optional[str] = None
    ) -> OperationResult:
        """
        Create a new data product using current operation mode.

        Args:
            data_product_id: ID for the data product
            name: Display name for the data product
            description: Optional description
            domain: Optional domain URN
            assets: Optional list of asset URNs
            owner: Optional owner username

        Returns:
            OperationResult with success status and details
        """
        entity_data = {
            "id": data_product_id,
            "name": name,
            "description": description,
            "domain": domain,
            "assets": assets or [],
            "owner": owner
        }

        operation = self.create_operation("create_data_product", entity_data)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="create_data_product",
                entity_urn=f"urn:li:dataProduct:{data_product_id}",
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def update_data_product(
        self,
        data_product_urn: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        domain: Optional[str] = None,
        assets: Optional[List[str]] = None
    ) -> OperationResult:
        """Update an existing data product."""
        entity_data = {}
        if name is not None:
            entity_data["name"] = name
        if description is not None:
            entity_data["description"] = description
        if domain is not None:
            entity_data["domain"] = domain
        if assets is not None:
            entity_data["assets"] = assets

        operation = self.create_operation("update_data_product", entity_data, entity_urn=data_product_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="update_data_product",
                entity_urn=data_product_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def delete_data_product(self, data_product_urn: str) -> OperationResult:
        """Delete a data product."""
        operation = self.create_operation("delete_data_product", {}, entity_urn=data_product_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="delete_data_product",
                entity_urn=data_product_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def add_data_product_owner(
        self,
        data_product_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner"
    ) -> OperationResult:
        """Add owner to data product."""
        operation = self.create_operation(
            "add_owner",
            {"owner_urn": owner_urn, "ownership_type": ownership_type},
            entity_urn=data_product_urn
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="add_data_product_owner",
                entity_urn=data_product_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def remove_data_product_owner(
        self,
        data_product_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner"
    ) -> OperationResult:
        """Remove owner from data product."""
        operation = self.create_operation(
            "remove_owner",
            {"owner_urn": owner_urn, "ownership_type": ownership_type},
            entity_urn=data_product_urn
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="remove_data_product_owner",
                entity_urn=data_product_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def add_data_product_tag(self, data_product_urn: str, tag_urn: str) -> OperationResult:
        """Add tag to data product."""
        operation = self.create_operation(
            "add_tag",
            {"tag_urn": tag_urn},
            entity_urn=data_product_urn
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="add_data_product_tag",
                entity_urn=data_product_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def remove_data_product_tag(self, data_product_urn: str, tag_urn: str) -> OperationResult:
        """Remove tag from data product."""
        operation = self.create_operation(
            "remove_tag",
            {"tag_urn": tag_urn},
            entity_urn=data_product_urn
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="remove_data_product_tag",
                entity_urn=data_product_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def add_data_product_glossary_term(self, data_product_urn: str, glossary_term_urn: str) -> OperationResult:
        """Add glossary term to data product."""
        operation = self.create_operation(
            "add_glossary_term",
            {"glossary_term_urn": glossary_term_urn},
            entity_urn=data_product_urn
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="add_data_product_glossary_term",
                entity_urn=data_product_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def remove_data_product_glossary_term(self, data_product_urn: str, glossary_term_urn: str) -> OperationResult:
        """Remove glossary term from data product."""
        operation = self.create_operation(
            "remove_glossary_term",
            {"glossary_term_urn": glossary_term_urn},
            entity_urn=data_product_urn
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="remove_data_product_glossary_term",
                entity_urn=data_product_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def set_data_product_domain(self, data_product_urn: str, domain_urn: str) -> OperationResult:
        """Set domain for data product."""
        operation = self.create_operation(
            "set_domain",
            {"domain_urn": domain_urn},
            entity_urn=data_product_urn
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="set_data_product_domain",
                entity_urn=data_product_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def unset_data_product_domain(self, data_product_urn: str) -> OperationResult:
        """Unset domain for data product."""
        operation = self.create_operation(
            "unset_domain",
            {},
            entity_urn=data_product_urn
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="unset_data_product_domain",
                entity_urn=data_product_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def add_assets_to_data_product(self, data_product_urn: str, asset_urns: List[str]) -> OperationResult:
        """Add assets to data product."""
        operation = self.create_operation(
            "add_assets",
            {"asset_urns": asset_urns},
            entity_urn=data_product_urn
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="add_assets_to_data_product",
                entity_urn=data_product_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def remove_assets_from_data_product(self, data_product_urn: str, asset_urns: List[str]) -> OperationResult:
        """Remove assets from data product."""
        operation = self.create_operation(
            "remove_assets",
            {"asset_urns": asset_urns},
            entity_urn=data_product_urn
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="remove_assets_from_data_product",
                entity_urn=data_product_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    # ============================================
    # BATCH OPERATIONS
    # ============================================

    def bulk_create_data_products(self, data_products_data: List[Dict[str, Any]]) -> BatchOperationResult:
        """Create multiple data products in batch."""
        results = []

        for data_product_data in data_products_data:
            result = self.create_data_product(
                data_product_id=data_product_data["id"],
                name=data_product_data["name"],
                description=data_product_data.get("description", ""),
                domain=data_product_data.get("domain"),
                assets=data_product_data.get("assets", []),
                owner=data_product_data.get("owner")
            )
            results.append(result)

        return BatchOperationResult(results)

    def bulk_assign_assets_to_data_product(self, data_product_urn: str, asset_urns: List[str]) -> BatchOperationResult:
        """Assign multiple assets to a data product."""
        results = []

        if self.sync_mode:
            # Use sync output for asset assignment
            result = self.sync_output.add_assets(data_product_urn, asset_urns)
            results.append(result)
        else:
            # Use async output to create bulk MCPs
            mcps = self.async_output.create_data_product_asset_assignment_mcps(data_product_urn, asset_urns)
            self.async_output.mcp_emitter.add_mcps(mcps)

            result = OperationResult(
                success=True,
                operation_type="add_assets_to_data_product",
                entity_urn=data_product_urn,
                mcps_generated=len(mcps)
            )
            results.append(result)

        return BatchOperationResult(results)

    def bulk_data_product_domain_assignment(self, assignments: List[Dict[str, str]]) -> BatchOperationResult:
        """Assign domains to multiple data products based on assignment list."""
        results = []

        for assignment in assignments:
            result = self.set_data_product_domain(
                assignment["data_product_urn"],
                assignment["domain_urn"]
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

        if op_type == "create_data_product":
            return self.sync_output.create_entity(data)
        elif op_type == "update_data_product":
            return self.sync_output.update_entity(entity_urn, data)
        elif op_type == "delete_data_product":
            return self.sync_output.delete_entity(entity_urn)
        elif op_type == "add_owner":
            return self.sync_output.add_owner(
                entity_urn, data["owner_urn"], data["ownership_type"]
            )
        elif op_type == "remove_owner":
            return self.sync_output.remove_owner(
                entity_urn, data["owner_urn"], data["ownership_type"]
            )
        elif op_type == "add_tag":
            return self.sync_output.add_tag(entity_urn, data["tag_urn"])
        elif op_type == "remove_tag":
            return self.sync_output.remove_tag(entity_urn, data["tag_urn"])
        elif op_type == "add_glossary_term":
            return self.sync_output.add_glossary_term(entity_urn, data["glossary_term_urn"])
        elif op_type == "remove_glossary_term":
            return self.sync_output.remove_glossary_term(entity_urn, data["glossary_term_urn"])
        elif op_type == "set_domain":
            return self.sync_output.set_domain(entity_urn, data["domain_urn"])
        elif op_type == "unset_domain":
            return self.sync_output.unset_domain(entity_urn)
        elif op_type == "add_assets":
            return self.sync_output.add_assets(entity_urn, data["asset_urns"])
        elif op_type == "remove_assets":
            return self.sync_output.remove_assets(entity_urn, data["asset_urns"])
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

        if op_type == "create_data_product":
            return self.async_output.create_entity(data)
        elif op_type == "update_data_product":
            return self.async_output.update_entity(entity_urn, data)
        elif op_type == "delete_data_product":
            return self.async_output.delete_entity(entity_urn)
        elif op_type == "add_owner":
            return self.async_output.add_owner(
                entity_urn, data["owner_urn"], data["ownership_type"]
            )
        elif op_type == "remove_owner":
            return self.async_output.remove_owner(
                entity_urn, data["owner_urn"], data["ownership_type"]
            )
        elif op_type == "add_tag":
            return self.async_output.add_tag(entity_urn, data["tag_urn"])
        elif op_type == "remove_tag":
            return self.async_output.remove_tag(entity_urn, data["tag_urn"])
        elif op_type == "add_glossary_term":
            return self.async_output.add_glossary_term(entity_urn, data["glossary_term_urn"])
        elif op_type == "remove_glossary_term":
            return self.async_output.remove_glossary_term(entity_urn, data["glossary_term_urn"])
        elif op_type == "set_domain":
            return self.async_output.set_domain(entity_urn, data["domain_urn"])
        elif op_type == "unset_domain":
            return self.async_output.unset_domain(entity_urn)
        elif op_type == "add_assets":
            return self.async_output.add_assets(entity_urn, data["asset_urns"])
        elif op_type == "remove_assets":
            return self.async_output.remove_assets(entity_urn, data["asset_urns"])
        else:
            return OperationResult(
                success=False,
                operation_type=op_type,
                error_message=f"Unknown operation type: {op_type}"
            )

    # ============================================
    # CONVENIENCE METHODS
    # ============================================

    def import_data_products_from_json(self, json_data: List[Dict[str, Any]]) -> BatchOperationResult:
        """Import data products from JSON data."""
        return self.bulk_create_data_products(json_data)

    def export_data_products_to_mcps(self, data_products_data: List[Dict[str, Any]], filename: Optional[str] = None) -> Optional[str]:
        """Export data products as MCPs to file."""
        # Switch to async mode temporarily
        original_sync_mode = self.sync_mode
        self.set_sync_mode(False)

        try:
            # Create MCPs for all data products
            mcps = self.async_output.create_bulk_data_product_mcps(data_products_data)
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
        summary.update({
            "sync_output_available": self.sync_output is not None,
            "async_output_available": self.async_output is not None,
            "async_mcp_count": self.async_output.get_mcp_count() if self.async_output else 0
        })

        return summary
