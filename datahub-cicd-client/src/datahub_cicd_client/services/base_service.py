"""
Enhanced base service supporting both input and output operations.

This module provides the foundation for services that can both read from DataHub
(input operations) and write to DataHub via synchronous (GraphQL) or asynchronous (MCP) methods.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.base_client import BaseDataHubClient
from datahub_cicd_client.core.mcp_emitter import MCPEmitter


class BaseInputOutputService(BaseDataHubClient, ABC):
    """
    Enhanced base service supporting both input and output operations.

    Input operations: Read data from DataHub (queries)
    Output operations: Write data to DataHub (mutations or MCPs)
    """

    def __init__(self, connection, output_dir: Optional[str] = None):
        """
        Initialize service with connection and optional output directory.

        Args:
            connection: DataHub connection instance
            output_dir: Directory for MCP file outputs (optional)
        """
        super().__init__(connection)
        self.output_dir = output_dir
        self.mcp_emitter = self._create_mcp_emitter()

        # Operation mode settings
        self.sync_mode = True  # Default to synchronous operations
        self.emit_to_file = False  # Default to direct emission
        self.batch_mode = False  # Default to immediate operations

        # Batch collection for async operations
        self.pending_operations = []

    @abstractmethod
    def _create_mcp_emitter(self) -> MCPEmitter:
        """Create appropriate MCP emitter for this service."""
        pass

    def set_sync_mode(self, sync: bool = True) -> None:
        """Set operation mode to synchronous (GraphQL) or asynchronous (MCP)."""
        self.sync_mode = sync
        self.logger.info(f"Set operation mode to {'synchronous' if sync else 'asynchronous'}")

    def set_output_mode(self, emit_to_file: bool = False, output_dir: Optional[str] = None) -> None:
        """Set output mode for MCP operations."""
        self.emit_to_file = emit_to_file
        if output_dir:
            self.output_dir = output_dir
            self.mcp_emitter = self._create_mcp_emitter()
        self.logger.info(f"Set output mode to {'file emission' if emit_to_file else 'direct emission'}")

    def set_batch_mode(self, batch: bool = False) -> None:
        """Set batch mode for collecting operations."""
        self.batch_mode = batch
        if not batch:
            self.flush_batch()
        self.logger.info(f"Set batch mode to {'enabled' if batch else 'disabled'}")

    def add_to_batch(self, operation: Dict[str, Any]) -> None:
        """Add operation to batch queue."""
        if self.batch_mode:
            self.pending_operations.append(operation)
        else:
            self._execute_operation(operation)

    def flush_batch(self) -> List[Any]:
        """Execute all pending batch operations."""
        if not self.pending_operations:
            return []

        results = []
        for operation in self.pending_operations:
            result = self._execute_operation(operation)
            results.append(result)

        self.pending_operations.clear()
        self.logger.info(f"Flushed batch of {len(results)} operations")
        return results

    def _execute_operation(self, operation: Dict[str, Any]) -> Any:
        """Execute a single operation based on current mode settings."""
        if self.sync_mode:
            return self._execute_sync_operation(operation)
        else:
            return self._execute_async_operation(operation)

    @abstractmethod
    def _execute_sync_operation(self, operation: Dict[str, Any]) -> Any:
        """Execute synchronous operation via GraphQL."""
        pass

    @abstractmethod
    def _execute_async_operation(self, operation: Dict[str, Any]) -> Any:
        """Execute asynchronous operation via MCP."""
        pass

    def emit_mcps(self, filename: Optional[str] = None, format: str = "json") -> Optional[str]:
        """
        Emit collected MCPs to file or directly to DataHub.

        Args:
            filename: Output filename (required for file emission)
            format: Output format for files (json, jsonl)

        Returns:
            File path if emitted to file, None if emitted directly
        """
        if not self.mcp_emitter.mcps:
            self.logger.warning("No MCPs to emit")
            return None

        if self.emit_to_file:
            if not filename:
                filename = f"{self.__class__.__name__.lower()}_mcps.json"
            return self.mcp_emitter.emit_to_file(filename, format)
        else:
            success = self.mcp_emitter.emit_direct(self.connection)
            if success:
                self.logger.info(f"Successfully emitted {len(self.mcp_emitter.mcps)} MCPs directly")
            return None

    def clear_mcps(self) -> None:
        """Clear all collected MCPs."""
        self.mcp_emitter.clear_mcps()

    def get_mcps(self) -> List[Dict[str, Any]]:
        """Get all collected MCPs."""
        return self.mcp_emitter.get_mcps()

    def get_operation_summary(self) -> Dict[str, Any]:
        """Get summary of current operation settings and pending operations."""
        return {
            "sync_mode": self.sync_mode,
            "emit_to_file": self.emit_to_file,
            "batch_mode": self.batch_mode,
            "output_dir": str(self.output_dir) if self.output_dir else None,
            "pending_operations": len(self.pending_operations),
            "pending_mcps": len(self.mcp_emitter.mcps)
        }


class InputOutputMixin:
    """
    Mixin class to add input/output capabilities to existing services.

    This can be used to enhance existing services without changing their inheritance.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_dir = kwargs.get('output_dir')
        self.sync_mode = True
        self.emit_to_file = False
        self.batch_mode = False
        self.pending_operations = []

    def configure_output(
        self,
        sync_mode: bool = True,
        emit_to_file: bool = False,
        batch_mode: bool = False,
        output_dir: Optional[str] = None
    ) -> None:
        """Configure output settings."""
        self.sync_mode = sync_mode
        self.emit_to_file = emit_to_file
        self.batch_mode = batch_mode
        if output_dir:
            self.output_dir = output_dir

        self.logger.info(f"Configured output: sync={sync_mode}, file={emit_to_file}, batch={batch_mode}")

    def create_operation(
        self,
        operation_type: str,
        entity_data: Dict[str, Any],
        **kwargs
    ) -> Dict[str, Any]:
        """Create an operation descriptor."""
        return {
            "type": operation_type,
            "data": entity_data,
            "timestamp": kwargs.get("timestamp"),
            "metadata": kwargs.get("metadata", {}),
            **kwargs
        }


class OperationResult:
    """Result of an input/output operation."""

    def __init__(
        self,
        success: bool,
        operation_type: str,
        entity_urn: Optional[str] = None,
        result_data: Optional[Any] = None,
        error_message: Optional[str] = None,
        mcps_generated: int = 0,
        file_path: Optional[str] = None
    ):
        self.success = success
        self.operation_type = operation_type
        self.entity_urn = entity_urn
        self.result_data = result_data
        self.error_message = error_message
        self.mcps_generated = mcps_generated
        self.file_path = file_path

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "success": self.success,
            "operation_type": self.operation_type,
            "entity_urn": self.entity_urn,
            "result_data": self.result_data,
            "error_message": self.error_message,
            "mcps_generated": self.mcps_generated,
            "file_path": self.file_path
        }

    def __repr__(self) -> str:
        status = "SUCCESS" if self.success else "FAILED"
        return f"OperationResult({status}, {self.operation_type}, {self.entity_urn})"


class BatchOperationResult:
    """Result of a batch operation."""

    def __init__(self, results: List[OperationResult]):
        self.results = results
        self.total_operations = len(results)
        self.successful_operations = sum(1 for r in results if r.success)
        self.failed_operations = self.total_operations - self.successful_operations
        self.total_mcps = sum(r.mcps_generated for r in results)

    def get_summary(self) -> Dict[str, Any]:
        """Get batch operation summary."""
        return {
            "total_operations": self.total_operations,
            "successful_operations": self.successful_operations,
            "failed_operations": self.failed_operations,
            "success_rate": self.successful_operations / self.total_operations if self.total_operations > 0 else 0,
            "total_mcps_generated": self.total_mcps,
            "operation_types": list({r.operation_type for r in self.results})
        }

    def get_failed_operations(self) -> List[OperationResult]:
        """Get list of failed operations."""
        return [r for r in self.results if not r.success]

    def get_successful_operations(self) -> List[OperationResult]:
        """Get list of successful operations."""
        return [r for r in self.results if r.success]
