"""
Base class for asynchronous output operations using MCP generation.

This module provides the foundation for services that generate MCPs
for batch processing and file-based workflows.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.mcp_emitter import MCPEmitter
from datahub_cicd_client.services.base_service import OperationResult


class BaseAsyncOutput(ABC):
    """
    Base class for asynchronous output operations.

    Handles MCP generation for batch processing and file-based workflows.
    """

    def __init__(self, output_dir: Optional[str] = None):
        """Initialize with optional output directory."""
        self.output_dir = output_dir
        self.logger = logging.getLogger(self.__class__.__name__)
        self.mcp_emitter = self._create_mcp_emitter()

    @abstractmethod
    def _create_mcp_emitter(self) -> MCPEmitter:
        """Create appropriate MCP emitter for this service."""
        pass

    @abstractmethod
    def create_entity_mcps(self, entity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create MCPs for entity creation."""
        pass

    @abstractmethod
    def update_entity_mcps(self, entity_urn: str, entity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create MCPs for entity update."""
        pass

    @abstractmethod
    def delete_entity_mcps(self, entity_urn: str) -> List[Dict[str, Any]]:
        """Create MCPs for entity deletion."""
        pass

    def create_entity(self, entity_data: Dict[str, Any]) -> OperationResult:
        """Create entity by generating MCPs."""
        try:
            mcps = self.create_entity_mcps(entity_data)
            self.mcp_emitter.add_mcps(mcps)

            entity_urn = entity_data.get("urn") or self._generate_entity_urn(entity_data)

            return OperationResult(
                success=True,
                operation_type="create",
                entity_urn=entity_urn,
                mcps_generated=len(mcps)
            )
        except Exception as e:
            self.logger.error(f"Error creating entity MCPs: {e}")
            return OperationResult(
                success=False,
                operation_type="create",
                error_message=str(e)
            )

    def update_entity(self, entity_urn: str, entity_data: Dict[str, Any]) -> OperationResult:
        """Update entity by generating MCPs."""
        try:
            mcps = self.update_entity_mcps(entity_urn, entity_data)
            self.mcp_emitter.add_mcps(mcps)

            return OperationResult(
                success=True,
                operation_type="update",
                entity_urn=entity_urn,
                mcps_generated=len(mcps)
            )
        except Exception as e:
            self.logger.error(f"Error updating entity MCPs: {e}")
            return OperationResult(
                success=False,
                operation_type="update",
                entity_urn=entity_urn,
                error_message=str(e)
            )

    def delete_entity(self, entity_urn: str) -> OperationResult:
        """Delete entity by generating MCPs."""
        try:
            mcps = self.delete_entity_mcps(entity_urn)
            self.mcp_emitter.add_mcps(mcps)

            return OperationResult(
                success=True,
                operation_type="delete",
                entity_urn=entity_urn,
                mcps_generated=len(mcps)
            )
        except Exception as e:
            self.logger.error(f"Error deleting entity MCPs: {e}")
            return OperationResult(
                success=False,
                operation_type="delete",
                entity_urn=entity_urn,
                error_message=str(e)
            )

    def batch_create_entities(self, entities_data: List[Dict[str, Any]]) -> List[OperationResult]:
        """Create multiple entities by generating MCPs."""
        results = []
        for entity_data in entities_data:
            result = self.create_entity(entity_data)
            results.append(result)
        return results

    def batch_update_entities(self, updates: List[Dict[str, Any]]) -> List[OperationResult]:
        """Update multiple entities by generating MCPs."""
        results = []
        for update in updates:
            entity_urn = update.get("entity_urn")
            entity_data = update.get("data", {})
            result = self.update_entity(entity_urn, entity_data)
            results.append(result)
        return results

    def batch_delete_entities(self, entity_urns: List[str]) -> List[OperationResult]:
        """Delete multiple entities by generating MCPs."""
        results = []
        for entity_urn in entity_urns:
            result = self.delete_entity(entity_urn)
            results.append(result)
        return results

    def emit_mcps(self, filename: Optional[str] = None, format: str = "json") -> Optional[str]:
        """Emit collected MCPs to file."""
        if not self.mcp_emitter.mcps:
            self.logger.warning("No MCPs to emit")
            return None

        if not filename:
            filename = f"{self.__class__.__name__.lower()}_mcps.{format}"

        return self.mcp_emitter.emit_to_file(filename, format)

    def clear_mcps(self) -> None:
        """Clear all collected MCPs."""
        self.mcp_emitter.clear_mcps()

    def get_mcps(self) -> List[Dict[str, Any]]:
        """Get all collected MCPs."""
        return self.mcp_emitter.get_mcps()

    def get_mcp_count(self) -> int:
        """Get count of collected MCPs."""
        return len(self.mcp_emitter.mcps)

    @abstractmethod
    def _generate_entity_urn(self, entity_data: Dict[str, Any]) -> str:
        """Generate entity URN from entity data."""
        pass


class EntityRelationshipAsyncOutput(BaseAsyncOutput):
    """Base class for handling entity relationships asynchronously."""

    @abstractmethod
    def add_owner_mcps(self, entity_urn: str, owner_urn: str, ownership_type: str) -> List[Dict[str, Any]]:
        """Create MCPs for adding owner to entity."""
        pass

    @abstractmethod
    def remove_owner_mcps(self, entity_urn: str, owner_urn: str, ownership_type: str) -> List[Dict[str, Any]]:
        """Create MCPs for removing owner from entity."""
        pass

    def add_owner(self, entity_urn: str, owner_urn: str, ownership_type: str) -> OperationResult:
        """Add owner to entity by generating MCPs."""
        try:
            mcps = self.add_owner_mcps(entity_urn, owner_urn, ownership_type)
            self.mcp_emitter.add_mcps(mcps)

            return OperationResult(
                success=True,
                operation_type="add_owner",
                entity_urn=entity_urn,
                mcps_generated=len(mcps)
            )
        except Exception as e:
            self.logger.error(f"Error adding owner MCPs: {e}")
            return OperationResult(
                success=False,
                operation_type="add_owner",
                entity_urn=entity_urn,
                error_message=str(e)
            )

    def remove_owner(self, entity_urn: str, owner_urn: str, ownership_type: str) -> OperationResult:
        """Remove owner from entity by generating MCPs."""
        try:
            mcps = self.remove_owner_mcps(entity_urn, owner_urn, ownership_type)
            self.mcp_emitter.add_mcps(mcps)

            return OperationResult(
                success=True,
                operation_type="remove_owner",
                entity_urn=entity_urn,
                mcps_generated=len(mcps)
            )
        except Exception as e:
            self.logger.error(f"Error removing owner MCPs: {e}")
            return OperationResult(
                success=False,
                operation_type="remove_owner",
                entity_urn=entity_urn,
                error_message=str(e)
            )

    def batch_add_owners(self, ownership_data: List[Dict[str, Any]]) -> List[OperationResult]:
        """Add owners to multiple entities by generating MCPs."""
        results = []
        for data in ownership_data:
            result = self.add_owner(
                data["entity_urn"],
                data["owner_urn"],
                data.get("ownership_type", "urn:li:ownershipType:__system__business_owner")
            )
            results.append(result)
        return results

    def batch_remove_owners(self, ownership_data: List[Dict[str, Any]]) -> List[OperationResult]:
        """Remove owners from multiple entities by generating MCPs."""
        results = []
        for data in ownership_data:
            result = self.remove_owner(
                data["entity_urn"],
                data["owner_urn"],
                data.get("ownership_type", "urn:li:ownershipType:__system__business_owner")
            )
            results.append(result)
        return results


class MetadataAssignmentAsyncOutput(BaseAsyncOutput):
    """Base class for handling metadata assignments asynchronously."""

    @abstractmethod
    def assign_to_entity_mcps(self, entity_urn: str, metadata_urn: str, **kwargs) -> List[Dict[str, Any]]:
        """Create MCPs for assigning metadata to entity."""
        pass

    @abstractmethod
    def remove_from_entity_mcps(self, entity_urn: str, metadata_urn: str) -> List[Dict[str, Any]]:
        """Create MCPs for removing metadata from entity."""
        pass

    def assign_to_entity(self, entity_urn: str, metadata_urn: str, **kwargs) -> OperationResult:
        """Assign metadata to entity by generating MCPs."""
        try:
            mcps = self.assign_to_entity_mcps(entity_urn, metadata_urn, **kwargs)
            self.mcp_emitter.add_mcps(mcps)

            return OperationResult(
                success=True,
                operation_type="assign_metadata",
                entity_urn=entity_urn,
                mcps_generated=len(mcps)
            )
        except Exception as e:
            self.logger.error(f"Error assigning metadata MCPs: {e}")
            return OperationResult(
                success=False,
                operation_type="assign_metadata",
                entity_urn=entity_urn,
                error_message=str(e)
            )

    def remove_from_entity(self, entity_urn: str, metadata_urn: str) -> OperationResult:
        """Remove metadata from entity by generating MCPs."""
        try:
            mcps = self.remove_from_entity_mcps(entity_urn, metadata_urn)
            self.mcp_emitter.add_mcps(mcps)

            return OperationResult(
                success=True,
                operation_type="remove_metadata",
                entity_urn=entity_urn,
                mcps_generated=len(mcps)
            )
        except Exception as e:
            self.logger.error(f"Error removing metadata MCPs: {e}")
            return OperationResult(
                success=False,
                operation_type="remove_metadata",
                entity_urn=entity_urn,
                error_message=str(e)
            )

    def batch_assign_to_entities(self, assignments: List[Dict[str, Any]]) -> List[OperationResult]:
        """Assign metadata to multiple entities by generating MCPs."""
        results = []
        for assignment in assignments:
            result = self.assign_to_entity(
                assignment["entity_urn"],
                assignment["metadata_urn"],
                **assignment.get("kwargs", {})
            )
            results.append(result)
        return results

    def batch_remove_from_entities(self, removals: List[Dict[str, Any]]) -> List[OperationResult]:
        """Remove metadata from multiple entities by generating MCPs."""
        results = []
        for removal in removals:
            result = self.remove_from_entity(
                removal["entity_urn"],
                removal["metadata_urn"]
            )
            results.append(result)
        return results
