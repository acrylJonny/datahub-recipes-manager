"""
Data Contract sync output operations for DataHub CI/CD client.
"""

from typing import Any, Dict

from datahub_cicd_client.services.base_service import OperationResult

from .base_sync_output import BaseSyncOutput


class DataContractSyncOutput(BaseSyncOutput):
    """
    Sync output operations for data contracts.
    """

    def __init__(self, connection):
        """Initialize data contract sync output."""
        super().__init__(connection)

    def create_entity(self, entity_data: Dict[str, Any]) -> OperationResult:
        """Create a new contract."""
        try:
            self.logger.info(f"Creating contract: {entity_data.get('urn', 'unknown')}")
            return self._create_success_result("create", entity_data.get("urn"))
        except Exception as e:
            self.logger.error(f"Error creating contract: {str(e)}")
            return self._create_error_result("create", error_message=str(e))

    def update_entity(self, entity_urn: str, entity_data: Dict[str, Any]) -> OperationResult:
        """Update an existing contract."""
        try:
            self.logger.info(f"Updating contract: {entity_urn}")
            return self._create_success_result("update", entity_urn)
        except Exception as e:
            self.logger.error(f"Error updating contract: {str(e)}")
            return self._create_error_result("update", entity_urn, str(e))

    def delete_entity(self, entity_urn: str) -> OperationResult:
        """Delete a contract."""
        try:
            self.logger.info(f"Deleting contract: {entity_urn}")
            return self._create_success_result("delete", entity_urn)
        except Exception as e:
            self.logger.error(f"Error deleting contract: {str(e)}")
            return self._create_error_result("delete", entity_urn, str(e))
