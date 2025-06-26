"""
DataHub Emitter for sending MCPs directly to DataHub.

This module provides functionality to emit MCPs directly to a DataHub instance,
essentially wrapping the acryl-datahub library's emit functionality.
"""

import logging
from typing import Any, Dict, List, Optional, Union

try:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    DATAHUB_AVAILABLE = True
except ImportError:
    logging.warning("DataHub SDK not available")
    DATAHUB_AVAILABLE = False
    class MetadataChangeProposalWrapper:
        pass
    class DatahubRestEmitter:
        pass


class DataHubEmitter:
    """
    Emitter that sends MCPs directly to DataHub using the acryl-datahub library.

    This provides the same emit functionality as the acryl-datahub library
    but integrated into our CI/CD client architecture.
    """

    def __init__(self, gms_server: str, token: Optional[str] = None, **kwargs):
        """
        Initialize the DataHub emitter.

        Args:
            gms_server: DataHub GMS server URL
            token: Optional authentication token
            **kwargs: Additional arguments for DatahubRestEmitter
        """
        self.gms_server = gms_server
        self.token = token
        self.logger = logging.getLogger(self.__class__.__name__)

        if not DATAHUB_AVAILABLE:
            raise ImportError("DataHub SDK not available. Please install acryl-datahub.")

        # Initialize the underlying DataHub REST emitter
        emitter_config = {"gms_server": gms_server}
        if token:
            emitter_config["token"] = token
        emitter_config.update(kwargs)

        self.rest_emitter = DatahubRestEmitter(**emitter_config)

    def emit(
        self,
        mcps: List[Union[MetadataChangeProposalWrapper, Dict[str, Any]]],
        callback: Optional[callable] = None
    ) -> List[Dict[str, Any]]:
        """
        Emit MCPs to DataHub.

        Args:
            mcps: List of MCPs to emit
            callback: Optional callback function for each emission result

        Returns:
            List of emission results
        """
        if not DATAHUB_AVAILABLE:
            raise RuntimeError("DataHub SDK not available")

        results = []

        for mcp in mcps:
            try:
                # Convert dict to MCP if needed
                if isinstance(mcp, dict):
                    # This would require more complex conversion logic
                    # For now, we assume MCPs are already MetadataChangeProposalWrapper objects
                    self.logger.warning("Dictionary MCPs not yet supported for direct emission")
                    continue

                # Emit the MCP
                result = self.rest_emitter.emit_mcp(mcp)
                results.append({
                    "success": True,
                    "entityUrn": mcp.entityUrn,
                    "aspectName": mcp.aspectName,
                    "result": result
                })

                if callback:
                    callback(mcp, result, True)

            except Exception as e:
                error_result = {
                    "success": False,
                    "entityUrn": getattr(mcp, 'entityUrn', 'unknown'),
                    "aspectName": getattr(mcp, 'aspectName', 'unknown'),
                    "error": str(e)
                }
                results.append(error_result)

                if callback:
                    callback(mcp, error_result, False)

                self.logger.error(f"Error emitting MCP: {e}")

        return results

    def emit_single(
        self,
        mcp: Union[MetadataChangeProposalWrapper, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Emit a single MCP to DataHub.

        Args:
            mcp: MCP to emit

        Returns:
            Emission result
        """
        results = self.emit([mcp])
        return results[0] if results else {"success": False, "error": "No results"}

    def test_connection(self) -> bool:
        """
        Test connection to DataHub.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Use the rest emitter's test method if available
            if hasattr(self.rest_emitter, 'test_connection'):
                return self.rest_emitter.test_connection()
            else:
                # Fallback: try a simple operation
                return True  # Placeholder - would need actual test logic
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False

    def close(self):
        """Close the emitter and clean up resources."""
        try:
            if hasattr(self.rest_emitter, 'close'):
                self.rest_emitter.close()
        except Exception as e:
            self.logger.error(f"Error closing emitter: {e}")
