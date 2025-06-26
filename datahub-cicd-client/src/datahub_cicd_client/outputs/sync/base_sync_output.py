"""
Base class for synchronous output operations using GraphQL mutations.

This module provides the foundation for services that write data to DataHub
using GraphQL mutations in real-time.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.connection import DataHubConnection
from datahub_cicd_client.services.base_service import OperationResult


class BaseSyncOutput(ABC):
    """
    Base class for synchronous output operations.

    Handles immediate execution of GraphQL mutations to write data to DataHub.
    """

    def __init__(self, connection: DataHubConnection):
        """Initialize with DataHub connection."""
        self.connection = connection
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute_graphql(
        self, query: str, variables: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Execute GraphQL query/mutation."""
        return self.connection.execute_graphql(query, variables)

    def _check_graphql_errors(self, result: Optional[Dict[str, Any]]) -> bool:
        """Check GraphQL result for errors."""
        if not result:
            return False

        if "errors" in result:
            error_messages = [error.get("message", "Unknown error") for error in result["errors"]]
            self.logger.error(f"GraphQL errors: {', '.join(error_messages)}")
            return False

        return True

    @abstractmethod
    def create_entity(self, entity_data: Dict[str, Any]) -> OperationResult:
        """Create a new entity."""
        pass

    @abstractmethod
    def update_entity(self, entity_urn: str, entity_data: Dict[str, Any]) -> OperationResult:
        """Update an existing entity."""
        pass

    @abstractmethod
    def delete_entity(self, entity_urn: str) -> OperationResult:
        """Delete an entity."""
        pass

    def batch_create_entities(self, entities_data: List[Dict[str, Any]]) -> List[OperationResult]:
        """Create multiple entities in batch."""
        results = []
        for entity_data in entities_data:
            result = self.create_entity(entity_data)
            results.append(result)
        return results

    def batch_update_entities(self, updates: List[Dict[str, Any]]) -> List[OperationResult]:
        """Update multiple entities in batch."""
        results = []
        for update in updates:
            entity_urn = update.get("entity_urn")
            entity_data = update.get("data", {})
            result = self.update_entity(entity_urn, entity_data)
            results.append(result)
        return results

    def batch_delete_entities(self, entity_urns: List[str]) -> List[OperationResult]:
        """Delete multiple entities in batch."""
        results = []
        for entity_urn in entity_urns:
            result = self.delete_entity(entity_urn)
            results.append(result)
        return results

    def _create_success_result(
        self, operation_type: str, entity_urn: str, result_data: Any = None
    ) -> OperationResult:
        """Create a successful operation result."""
        return OperationResult(
            success=True,
            operation_type=operation_type,
            entity_urn=entity_urn,
            result_data=result_data,
        )

    def _create_error_result(
        self,
        operation_type: str,
        entity_urn: Optional[str] = None,
        error_message: str = "Unknown error",
    ) -> OperationResult:
        """Create a failed operation result."""
        return OperationResult(
            success=False,
            operation_type=operation_type,
            entity_urn=entity_urn,
            error_message=error_message,
        )


class EntityRelationshipSyncOutput(BaseSyncOutput):
    """Base class for handling entity relationships synchronously."""

    @abstractmethod
    def add_owner(self, entity_urn: str, owner_urn: str, ownership_type: str) -> OperationResult:
        """Add owner to entity."""
        pass

    @abstractmethod
    def remove_owner(self, entity_urn: str, owner_urn: str, ownership_type: str) -> OperationResult:
        """Remove owner from entity."""
        pass

    def batch_add_owners(self, ownership_data: List[Dict[str, Any]]) -> List[OperationResult]:
        """Add owners to multiple entities in batch."""
        results = []
        for data in ownership_data:
            result = self.add_owner(
                data["entity_urn"],
                data["owner_urn"],
                data.get("ownership_type", "urn:li:ownershipType:__system__business_owner"),
            )
            results.append(result)
        return results

    def batch_remove_owners(self, ownership_data: List[Dict[str, Any]]) -> List[OperationResult]:
        """Remove owners from multiple entities in batch."""
        results = []
        for data in ownership_data:
            result = self.remove_owner(
                data["entity_urn"],
                data["owner_urn"],
                data.get("ownership_type", "urn:li:ownershipType:__system__business_owner"),
            )
            results.append(result)
        return results


class MetadataAssignmentSyncOutput(BaseSyncOutput):
    """Base class for handling metadata assignments synchronously."""

    @abstractmethod
    def assign_to_entity(self, entity_urn: str, metadata_urn: str, **kwargs) -> OperationResult:
        """Assign metadata to entity."""
        pass

    @abstractmethod
    def remove_from_entity(self, entity_urn: str, metadata_urn: str) -> OperationResult:
        """Remove metadata from entity."""
        pass

    def batch_assign_to_entities(self, assignments: List[Dict[str, Any]]) -> List[OperationResult]:
        """Assign metadata to multiple entities in batch."""
        results = []
        for assignment in assignments:
            result = self.assign_to_entity(
                assignment["entity_urn"], assignment["metadata_urn"], **assignment.get("kwargs", {})
            )
            results.append(result)
        return results

    def batch_remove_from_entities(self, removals: List[Dict[str, Any]]) -> List[OperationResult]:
        """Remove metadata from multiple entities in batch."""
        results = []
        for removal in removals:
            result = self.remove_from_entity(removal["entity_urn"], removal["metadata_urn"])
            results.append(result)
        return results
