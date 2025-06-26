"""
Synchronous output operations for edited data in DataHub.

This module provides functionality for writing edited data changes directly to DataHub
using GraphQL mutations in real-time.
"""

from typing import Any, Dict, Optional

from datahub_cicd_client.outputs.sync.base_sync_output import BaseSyncOutput
from datahub_cicd_client.services.base_service import OperationResult


class EditedDataSyncOutput(BaseSyncOutput):
    """
    Synchronous output service for edited data operations.

    This service handles real-time writing of edited data changes to DataHub
    using GraphQL mutations.
    """

    def __init__(self, connection):
        """
        Initialize edited data sync output service.

        Args:
            connection: DataHub connection instance
        """
        super().__init__(connection)
        self.entity_type = "edited_data"

    def create_entity(self, entity_data: Dict[str, Any]) -> OperationResult:
        """Create a new entity (not applicable for edited data)."""
        return self._create_error_result(
            "create_entity", error_message="Create entity not supported for edited data"
        )

    def delete_entity(self, entity_urn: str) -> OperationResult:
        """Delete an entity (not applicable for edited data)."""
        return self._create_error_result(
            "delete_entity",
            entity_urn=entity_urn,
            error_message="Delete entity not supported for edited data",
        )

    def update_entity(self, entity_urn: str, entity_data: Dict[str, Any]) -> OperationResult:
        """Update an entity's editable properties."""
        return self.update_entity_description(entity_urn, entity_data.get("description", ""))

    def update_entity_description(self, entity_urn: str, description: str) -> OperationResult:
        """
        Update an entity's description.

        Args:
            entity_urn: URN of the entity to update
            description: New description

        Returns:
            OperationResult with operation status
        """
        try:
            variables = {
                "input": {"urn": entity_urn, "editableProperties": {"description": description}}
            }

            result = self.execute_graphql(
                """
                mutation updateEditableProperties($input: EditablePropertiesUpdateInput!) {
                    updateEditableProperties(input: $input)
                }
            """,
                variables,
            )

            if result and result.get("updateEditableProperties"):
                return self._create_success_result(
                    "update_entity_description",
                    entity_urn,
                    f"Successfully updated description for {entity_urn}",
                )
            else:
                return self._create_error_result(
                    "update_entity_description", entity_urn, "Failed to update description"
                )

        except Exception as e:
            self.logger.error(f"Error updating entity description: {str(e)}")
            return self._create_error_result("update_entity_description", entity_urn, str(e))

    def update_custom_properties(
        self, entity_urn: str, custom_properties: Dict[str, str]
    ) -> OperationResult:
        """
        Update an entity's custom properties.

        Args:
            entity_urn: URN of the entity to update
            custom_properties: Dictionary of custom properties

        Returns:
            OperationResult with operation status
        """
        try:
            # Convert custom properties to the format expected by DataHub
            custom_props_list = [
                {"key": key, "value": value} for key, value in custom_properties.items()
            ]

            variables = {"input": {"urn": entity_urn, "customProperties": custom_props_list}}

            result = self.execute_graphql(
                """
                mutation updateCustomProperties($input: CustomPropertiesUpdateInput!) {
                    updateCustomProperties(input: $input)
                }
            """,
                variables,
            )

            if result and result.get("updateCustomProperties"):
                return self._create_success_result(
                    "update_custom_properties",
                    entity_urn,
                    f"Successfully updated custom properties for {entity_urn}",
                )
            else:
                return self._create_error_result(
                    "update_custom_properties", entity_urn, "Failed to update custom properties"
                )

        except Exception as e:
            self.logger.error(f"Error updating custom properties: {str(e)}")
            return self._create_error_result("update_custom_properties", entity_urn, str(e))

    def bulk_update_properties(
        self,
        entity_urn: str,
        description: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
    ) -> OperationResult:
        """
        Bulk update multiple properties for an entity.

        Args:
            entity_urn: URN of the entity to update
            description: New description (optional)
            custom_properties: Dictionary of custom properties (optional)

        Returns:
            OperationResult with operation status
        """
        try:
            success_operations = []
            failed_operations = []

            # Update description if provided
            if description is not None:
                desc_result = self.update_entity_description(entity_urn, description)
                if desc_result.success:
                    success_operations.append("description")
                else:
                    failed_operations.append(f"description: {desc_result.error_message}")

            # Update custom properties if provided
            if custom_properties:
                props_result = self.update_custom_properties(entity_urn, custom_properties)
                if props_result.success:
                    success_operations.append("custom_properties")
                else:
                    failed_operations.append(f"custom_properties: {props_result.error_message}")

            # Determine overall success
            overall_success = len(failed_operations) == 0

            message = []
            if success_operations:
                message.append(f"Successfully updated: {', '.join(success_operations)}")
            if failed_operations:
                message.append(f"Failed to update: {', '.join(failed_operations)}")

            if overall_success:
                return self._create_success_result(
                    "bulk_update_properties",
                    entity_urn,
                    "; ".join(message) if message else "No operations performed",
                )
            else:
                return self._create_error_result(
                    "bulk_update_properties",
                    entity_urn,
                    "; ".join(failed_operations) if failed_operations else "Unknown error",
                )

        except Exception as e:
            self.logger.error(f"Error in bulk update properties: {str(e)}")
            return self._create_error_result("bulk_update_properties", entity_urn, str(e))
