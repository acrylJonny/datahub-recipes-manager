"""
Recipe service for DataHub operations with comprehensive input/output capabilities.

This module provides functionality for managing recipes and ingestion sources in DataHub,
including CRUD operations, recipe execution, and ingestion monitoring.
Supports both synchronous (GraphQL) and asynchronous (MCP) operations.
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.mcp_emitter import MCPEmitter
from datahub_cicd_client.graphql.queries.recipes import (
    GET_INGESTION_SOURCE_QUERY,
)
from datahub_cicd_client.outputs.mcp_output.recipes import RecipeAsyncOutput
from datahub_cicd_client.outputs.sync.recipes import RecipeSyncOutput
from datahub_cicd_client.services.base_service import (
    BaseInputOutputService,
    BatchOperationResult,
    OperationResult,
)


class RecipeService(BaseInputOutputService):
    """
    Comprehensive service for managing DataHub recipes with input/output capabilities.

    This service combines:
    - Input operations: Reading recipes from DataHub (queries)
    - Output operations: Writing recipes to DataHub (sync GraphQL or async MCP)
    - Batch operations: Bulk processing capabilities
    - CI/CD integration: Perfect for automated ingestion workflows
    """

    def __init__(self, connection, output_dir: Optional[str] = None):
        """
        Initialize recipe service.

        Args:
            connection: DataHub connection instance
            output_dir: Directory for MCP file outputs (optional)
        """
        super().__init__(connection, output_dir)

        # Output services
        self.sync_output = RecipeSyncOutput(connection)
        self.async_output = RecipeAsyncOutput(output_dir)

    def _create_mcp_emitter(self) -> MCPEmitter:
        """Create recipe-specific MCP emitter."""
        return MCPEmitter(self.output_dir, entity_type="recipe")

    # ============================================
    # INPUT OPERATIONS (Reading from DataHub)
    # ============================================

    def list_recipes(self) -> List[Dict[str, Any]]:
        """List recipes."""
        return []

    def get_recipe(self, recipe_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a single recipe by URN.

        Args:
            recipe_urn: The URN of the recipe to retrieve

        Returns:
            Recipe data or None if not found
        """
        try:
            variables = {"urn": recipe_urn}
            data = self.safe_execute_graphql(GET_INGESTION_SOURCE_QUERY, variables)
            if not data or "ingestionSource" not in data:
                return None

            return data["ingestionSource"]

        except Exception as e:
            self.logger.error(f"Error getting recipe {recipe_urn}: {str(e)}")
            return None

    # ============================================
    # OUTPUT OPERATIONS (Writing to DataHub)
    # ============================================

    def create_recipe(
        self,
        name: str,
        type: str,
        config: Dict[str, Any],
        platform_urn: Optional[str] = None,
        schedule: Optional[Dict[str, Any]] = None
    ) -> OperationResult:
        """
        Create a new recipe.

        Args:
            name: Name of the recipe
            type: Recipe type (e.g., "mysql", "postgres", "databricks")
            config: Recipe configuration
            platform_urn: Optional platform URN
            schedule: Optional schedule configuration

        Returns:
            OperationResult with success status and details
        """
        entity_data = {
            "name": name,
            "type": type,
            "config": config,
            "platformUrn": platform_urn,
            "schedule": schedule
        }

        operation = self.create_operation("create_recipe", entity_data)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="create_recipe",
                entity_urn=f"urn:li:dataHubIngestionSource:{name}",
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def update_recipe(
        self,
        recipe_urn: str,
        name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        schedule: Optional[Dict[str, Any]] = None
    ) -> OperationResult:
        """Update an existing recipe."""
        entity_data = {}
        if name is not None:
            entity_data["name"] = name
        if config is not None:
            entity_data["config"] = config
        if schedule is not None:
            entity_data["schedule"] = schedule

        operation = self.create_operation("update_recipe", entity_data, entity_urn=recipe_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="update_recipe",
                entity_urn=recipe_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def delete_recipe(self, recipe_urn: str) -> OperationResult:
        """Delete a recipe."""
        operation = self.create_operation("delete_recipe", {}, entity_urn=recipe_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="delete_recipe",
                entity_urn=recipe_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    def execute_recipe(self, recipe_urn: str) -> OperationResult:
        """Execute a recipe (trigger ingestion)."""
        operation = self.create_operation("execute_recipe", {}, entity_urn=recipe_urn)

        if self.batch_mode:
            self.add_to_batch(operation)
            return OperationResult(
                success=True,
                operation_type="execute_recipe",
                entity_urn=recipe_urn,
                result_data="Added to batch"
            )
        else:
            return self._execute_operation(operation)

    # ============================================
    # BATCH OPERATIONS
    # ============================================

    def bulk_create_recipes(self, recipes_data: List[Dict[str, Any]]) -> BatchOperationResult:
        """Create multiple recipes in batch."""
        results = []

        for recipe_data in recipes_data:
            result = self.create_recipe(
                name=recipe_data["name"],
                type=recipe_data["type"],
                config=recipe_data["config"],
                platform_urn=recipe_data.get("platformUrn"),
                schedule=recipe_data.get("schedule")
            )
            results.append(result)

        return BatchOperationResult(results)

    def bulk_execute_recipes(self, recipe_urns: List[str]) -> BatchOperationResult:
        """Execute multiple recipes in batch."""
        results = []

        for recipe_urn in recipe_urns:
            result = self.execute_recipe(recipe_urn)
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

        if op_type == "create_recipe":
            return self.sync_output.create_entity(data)
        elif op_type == "update_recipe":
            return self.sync_output.update_entity(entity_urn, data)
        elif op_type == "delete_recipe":
            return self.sync_output.delete_entity(entity_urn)
        elif op_type == "execute_recipe":
            return self.sync_output.execute_recipe(entity_urn)
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

        if op_type == "create_recipe":
            return self.async_output.create_entity(data)
        elif op_type == "update_recipe":
            return self.async_output.update_entity(entity_urn, data)
        elif op_type == "delete_recipe":
            return self.async_output.delete_entity(entity_urn)
        elif op_type == "execute_recipe":
            return self.async_output.execute_recipe(entity_urn)
        else:
            return OperationResult(
                success=False,
                operation_type=op_type,
                error_message=f"Unknown operation type: {op_type}"
            )

    # ============================================
    # CONVENIENCE METHODS
    # ============================================

    def import_recipes_from_json(self, json_data: List[Dict[str, Any]]) -> BatchOperationResult:
        """Import recipes from JSON data."""
        return self.bulk_create_recipes(json_data)

    def export_recipes_to_mcps(self, recipes_data: List[Dict[str, Any]], filename: Optional[str] = None) -> Optional[str]:
        """Export recipes as MCPs to file."""
        # Switch to async mode temporarily
        original_sync_mode = self.sync_mode
        self.set_sync_mode(False)

        try:
            # Create MCPs for all recipes
            mcps = self.async_output.create_bulk_recipe_mcps(recipes_data)
            self.async_output.mcp_emitter.add_mcps(mcps)

            # Emit to file
            return self.async_output.emit_mcps(filename)
        finally:
            # Restore original mode
            self.set_sync_mode(original_sync_mode)

    # ============================================
    # RECIPE-SPECIFIC METHODS
    # ============================================

    def validate_recipe_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate recipe configuration.

        Args:
            config: Recipe configuration to validate

        Returns:
            Validation result with success status and any errors
        """
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": []
        }

        # Basic validation logic
        if not config.get("source"):
            validation_result["valid"] = False
            validation_result["errors"].append("Missing 'source' configuration")

        if not config.get("sink"):
            validation_result["valid"] = False
            validation_result["errors"].append("Missing 'sink' configuration")

        # Add more validation logic as needed

        return validation_result

    def get_recipe_execution_history(self, recipe_urn: str, start: int = 0, count: int = 10) -> List[Dict[str, Any]]:
        """Get execution history for a recipe."""
        try:
            variables = {"urn": recipe_urn, "start": start, "count": count}
            data = self.safe_execute_graphql(GET_INGESTION_SOURCE_QUERY, variables)
            if data and "ingestionSource" in data and data["ingestionSource"]:
                executions = data["ingestionSource"].get("executions", {})
                return executions.get("executionRequests", [])

            return []

        except Exception as e:
            self.logger.error(f"Error getting recipe execution history for {recipe_urn}: {str(e)}")
            return []
