"""
Data Contract service for DataHub operations with comprehensive input/output capabilities.

This module provides functionality for managing data contracts in DataHub,
including CRUD operations, ownership management, and contract validation.
Supports both synchronous (GraphQL) and asynchronous (MCP) operations.
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.mcp_emitter import MCPEmitter
from datahub_cicd_client.graphql.queries.data_contracts import (
    GET_DATA_CONTRACT_QUERY,
    GET_DATASETS_BY_URNS_QUERY,
    LIST_DATA_CONTRACTS_QUERY,
)
from datahub_cicd_client.outputs.mcp_output.data_contracts import DataContractAsyncOutput
from datahub_cicd_client.outputs.sync.data_contracts import DataContractSyncOutput
from datahub_cicd_client.services.base_service import (
    BaseInputOutputService,
    BatchOperationResult,
    OperationResult,
)


class DataContractService(BaseInputOutputService):
    """
    Comprehensive service for managing DataHub data contracts with input/output capabilities.

    This service combines:
    - Input operations: Reading data contracts from DataHub (queries)
    - Output operations: Writing data contracts to DataHub (sync GraphQL or async MCP)
    - Batch operations: Bulk processing capabilities
    - CI/CD integration: Perfect for automated data governance workflows
    """

    def __init__(self, connection, output_dir: Optional[str] = None):
        """
        Initialize data contract service.

        Args:
            connection: DataHub connection instance
            output_dir: Directory for MCP file outputs (optional)
        """
        super().__init__(connection, output_dir)

        # Output services
        self.sync_output = DataContractSyncOutput(connection)
        self.async_output = DataContractAsyncOutput(output_dir)

    def _create_mcp_emitter(self) -> MCPEmitter:
        """Create data contract-specific MCP emitter."""
        return MCPEmitter(self.output_dir)

    # ============================================
    # INPUT OPERATIONS (Reading from DataHub)
    # ============================================

    def get_data_contracts(
        self, query: str = "*", start: int = 0, count: int = 100
    ) -> Dict[str, Any]:
        """
        Get data contracts from DataHub with comprehensive information including related dataset details.
        This method returns the structure expected by the UI.

        Args:
            query (str): Search query to filter data contracts (default: "*")
            start (int): Starting offset for pagination
            count (int): Maximum number of data contracts to return

        Returns:
            Dictionary with success status and data contracts information
        """
        self.logger.info(
            f"Getting data contracts with query: {query}, start: {start}, count: {count}"
        )

        variables = {
            "input": {
                "types": ["DATA_CONTRACT"],
                "query": query,
                "start": start,
                "count": count,
                "filters": [],
            }
        }

        try:
            result = self.safe_execute_graphql(LIST_DATA_CONTRACTS_QUERY, variables)

            if result and "searchAcrossEntities" in result:
                search_data = result["searchAcrossEntities"]

                # Extract entity URNs from data contracts
                entity_urns = []
                search_results = search_data.get("searchResults", [])

                for contract_result in search_results:
                    entity = contract_result.get("entity", {})
                    if entity:
                        properties = entity.get("properties", {})
                        entity_urn = properties.get("entityUrn")
                        if entity_urn:
                            entity_urns.append(entity_urn)
                            self.logger.debug(f"Found entity URN for dataset lookup: {entity_urn}")

                self.logger.info(
                    f"Extracted {len(entity_urns)} entity URNs from {len(search_results)} data contracts"
                )

                # Get dataset information for the entity URNs
                dataset_info = {}
                if entity_urns:
                    self.logger.info(
                        f"Fetching dataset information for {len(entity_urns)} entities"
                    )
                    dataset_result = self.get_datasets_by_urns(entity_urns)

                    if dataset_result.get("success") and dataset_result.get("data"):
                        dataset_search_results = dataset_result["data"].get("searchResults", [])
                        self.logger.info(
                            f"Dataset query returned {len(dataset_search_results)} results"
                        )

                        # Create a mapping of URN to dataset info
                        for dataset_result_item in dataset_search_results:
                            dataset_entity = dataset_result_item.get("entity", {})
                            if dataset_entity and dataset_entity.get("urn"):
                                dataset_urn = dataset_entity["urn"]
                                dataset_info[dataset_urn] = dataset_entity
                                self.logger.debug(
                                    f"Mapped dataset URN {dataset_urn} to dataset info"
                                )
                    else:
                        self.logger.warning(
                            f"Dataset query failed or returned no data: {dataset_result}"
                        )

                # Enhance contract data with dataset information
                enhanced_results = []
                for contract_result in search_results:
                    entity = contract_result.get("entity", {})
                    if entity:
                        properties = entity.get("properties", {})
                        entity_urn = properties.get("entityUrn")

                        # Add dataset information if available
                        if entity_urn and entity_urn in dataset_info:
                            entity["dataset_info"] = dataset_info[entity_urn]

                        enhanced_results.append(contract_result)

                return {
                    "success": True,
                    "data": {
                        "start": search_data.get("start", start),
                        "count": search_data.get("count", 0),
                        "total": search_data.get("total", 0),
                        "searchResults": enhanced_results,
                    },
                }

            else:
                error_msg = "No data returned from DataHub"
                if result and "errors" in result:
                    error_messages = [e.get("message", "") for e in result.get("errors", [])]
                    error_msg = f"GraphQL errors: {', '.join(error_messages)}"

                self.logger.error(f"Error in get_data_contracts: {error_msg}")
                return {"success": False, "error": error_msg}

        except Exception as e:
            self.logger.error(f"Error getting data contracts: {str(e)}")
            return {"success": False, "error": str(e)}

    def get_datasets_by_urns(self, urns: List[str]) -> Dict[str, Any]:
        """
        Get datasets by their URNs for enriching data contract information.

        Args:
            urns: List of dataset URNs to fetch

        Returns:
            Dictionary with success status and dataset information
        """
        try:
            self.logger.info(f"Getting datasets by URNs: {urns}")

            variables = {"urns": urns}
            result = self.safe_execute_graphql(GET_DATASETS_BY_URNS_QUERY, variables)

            if result and "searchAcrossEntities" in result:
                return {"success": True, "data": result["searchAcrossEntities"]}
            else:
                error_msg = "No data returned from DataHub"
                if result and "errors" in result:
                    error_messages = [e.get("message", "") for e in result.get("errors", [])]
                    error_msg = f"GraphQL errors: {', '.join(error_messages)}"

                return {"success": False, "error": error_msg}

        except Exception as e:
            self.logger.error(f"Error getting datasets by URNs: {str(e)}")
            return {"success": False, "error": str(e)}

    def list_data_contracts(
        self, query: str = "*", start: int = 0, count: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List data contracts in DataHub.
        This method returns a simple list for backward compatibility.

        Args:
            query: Search query to filter data contracts
            start: Starting offset for pagination
            count: Maximum number of data contracts to return

        Returns:
            List of data contract objects
        """
        self.logger.info(
            f"Listing data contracts with query: {query}, start: {start}, count: {count}"
        )

        # Use the comprehensive method and extract just the contracts
        result = self.get_data_contracts(query=query, start=start, count=count)

        if result.get("success", False) and result.get("data"):
            search_results = result["data"].get("searchResults", [])
            contracts = []

            for item in search_results:
                entity = item.get("entity")
                if entity:
                    contracts.append(entity)

            return contracts
        else:
            self.logger.error(
                f"Error listing data contracts: {result.get('error', 'Unknown error')}"
            )
            return []

    def get_data_contract(self, contract_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a single data contract by URN.

        Args:
            contract_urn: The URN of the data contract to retrieve

        Returns:
            Data contract data or None if not found
        """
        try:
            variables = {"urn": contract_urn}
            data = self.safe_execute_graphql(GET_DATA_CONTRACT_QUERY, variables)

            if not data or "dataContract" not in data:
                return None

            return data["dataContract"]

        except Exception as e:
            self.logger.error(f"Error getting data contract {contract_urn}: {str(e)}")
            return None

    # ============================================
    # OUTPUT OPERATIONS (Writing to DataHub)
    # ============================================

    def create_data_contract(
        self,
        contract_urn: str,
        name: str,
        description: str = "",
        custom_properties: Optional[Dict[str, str]] = None,
        schema: Optional[Dict[str, Any]] = None,
    ) -> OperationResult:
        """
        Create a new data contract.

        Args:
            contract_urn: URN for the data contract
            name: Name of the data contract
            description: Optional description
            custom_properties: Optional custom properties
            schema: Optional schema definition

        Returns:
            OperationResult with success status and details
        """
        entity_data = {
            "urn": contract_urn,
            "name": name,
            "description": description,
            "customProperties": custom_properties or {},
            "schema": schema,
        }

        operation = self.create_operation("create_data_contract", entity_data)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="create_data_contract",
                entity_urn=contract_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def update_data_contract(
        self,
        contract_urn: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
        schema: Optional[Dict[str, Any]] = None,
    ) -> OperationResult:
        """Update an existing data contract."""
        entity_data = {}
        if name is not None:
            entity_data["name"] = name
        if description is not None:
            entity_data["description"] = description
        if custom_properties is not None:
            entity_data["customProperties"] = custom_properties
        if schema is not None:
            entity_data["schema"] = schema

        operation = self.create_operation(
            "update_data_contract", entity_data, entity_urn=contract_urn
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="update_data_contract",
                entity_urn=contract_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def delete_data_contract(self, contract_urn: str) -> OperationResult:
        """Delete a data contract."""
        operation = self.create_operation("delete_data_contract", {}, entity_urn=contract_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="delete_data_contract",
                entity_urn=contract_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    def add_data_contract_owner(
        self,
        contract_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner",
    ) -> OperationResult:
        """Add owner to data contract."""
        operation = self.create_operation(
            "add_owner",
            {"owner_urn": owner_urn, "ownership_type": ownership_type},
            entity_urn=contract_urn,
        )

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="add_data_contract_owner",
                entity_urn=contract_urn,
                result_data="Added to batch",
            )
        else:
            return self._execute_operation(operation)

    # ============================================
    # BATCH OPERATIONS
    # ============================================

    def bulk_create_data_contracts(
        self, contracts_data: List[Dict[str, Any]]
    ) -> BatchOperationResult:
        """Create multiple data contracts in batch."""
        results = []

        for contract_data in contracts_data:
            result = self.create_data_contract(
                contract_urn=contract_data["urn"],
                name=contract_data["name"],
                description=contract_data.get("description", ""),
                custom_properties=contract_data.get("customProperties"),
                schema=contract_data.get("schema"),
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

        if op_type == "create_data_contract":
            return self.sync_output.create_entity(data)
        elif op_type == "update_data_contract":
            return self.sync_output.update_entity(entity_urn, data)
        elif op_type == "delete_data_contract":
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

        if op_type == "create_data_contract":
            return self.async_output.create_entity(data)
        elif op_type == "update_data_contract":
            return self.async_output.update_entity(entity_urn, data)
        elif op_type == "delete_data_contract":
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

    def import_data_contracts_from_json(
        self, json_data: List[Dict[str, Any]]
    ) -> BatchOperationResult:
        """Import data contracts from JSON data."""
        return self.bulk_create_data_contracts(json_data)

    def export_data_contracts_to_mcps(
        self, contracts_data: List[Dict[str, Any]], filename: Optional[str] = None
    ) -> Optional[str]:
        """Export data contracts as MCPs to file."""
        # Switch to async mode temporarily
        original_sync_mode = self.sync_mode
        self.set_sync_mode(False)

        try:
            # Create MCPs for all data contracts
            mcps = self.async_output.create_bulk_data_contract_mcps(contracts_data)
            self.async_output.mcp_emitter.add_mcps(mcps)

            # Emit to file
            return self.async_output.emit_mcps(filename)
        finally:
            # Restore original mode
            self.set_sync_mode(original_sync_mode)

    def count_data_contracts(self, query: str = "*") -> int:
        """
        Get the count of data contracts matching the query.
        
        Args:
            query: Search query to filter data contracts
            
        Returns:
            Number of data contracts matching the query
        """
        try:
            # Get data contracts using the existing method
            result = self.get_data_contracts(query=query, start=0, count=1)
            
            if isinstance(result, dict) and "data" in result and "searchResults" in result["data"]:
                return result["data"].get("total", 0)
            
            return 0
        except Exception as e:
            self.logger.error(f"Error counting data contracts: {str(e)}")
            return 0
