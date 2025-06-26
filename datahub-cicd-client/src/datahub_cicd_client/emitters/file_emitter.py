"""
File Emitter for DataHub MCPs.

This module provides functionality to emit MCPs to files instead of directly to DataHub.
Perfect for CI/CD workflows where MCPs need to be reviewed before emission.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

try:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    DATAHUB_AVAILABLE = True
except ImportError:
    DATAHUB_AVAILABLE = False
    class MetadataChangeProposalWrapper:
        pass


class FileEmitter:
    """
    Emitter that writes MCPs to files instead of sending them to DataHub.

    This is particularly useful for:
    - CI/CD pipelines where MCPs need review before emission
    - Backup/audit trails of metadata changes
    - Batch processing workflows
    - Testing and development
    """

    def __init__(self, output_dir: str = "metadata-manager"):
        """
        Initialize the file emitter.

        Args:
            output_dir: Directory to write MCP files to
        """
        self.output_dir = Path(output_dir)
        self.logger = logging.getLogger(self.__class__.__name__)

        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def emit(
        self,
        mcps: List[Union[MetadataChangeProposalWrapper, Dict[str, Any]]],
        filename: Optional[str] = None
    ) -> str:
        """
        Emit MCPs to a file.

        Args:
            mcps: List of MCPs to emit
            filename: Optional filename (defaults to timestamped file)

        Returns:
            Path to the written file
        """
        if filename is None:
            import time
            timestamp = int(time.time())
            filename = f"mcps_{timestamp}.json"

        file_path = self.output_dir / filename

        # Convert MCPs to serializable format
        mcp_dicts = []
        for mcp in mcps:
            if DATAHUB_AVAILABLE and isinstance(mcp, MetadataChangeProposalWrapper):
                # Convert DataHub MCP to dictionary
                mcp_dict = {
                    "entityUrn": mcp.entityUrn,
                    "entityType": mcp.entityType,
                    "aspectName": mcp.aspectName,
                    "changeType": mcp.changeType,
                    "aspect": mcp.aspect.to_obj() if hasattr(mcp.aspect, 'to_obj') else mcp.aspect
                }
            else:
                # Already a dictionary
                mcp_dict = mcp

            mcp_dicts.append(mcp_dict)

        # Write to file
        with open(file_path, 'w') as f:
            json.dump(mcp_dicts, f, indent=2, default=str)

        self.logger.info(f"Emitted {len(mcp_dicts)} MCPs to {file_path}")
        return str(file_path)

    def emit_single_mcp_file(
        self,
        mcps: List[Union[MetadataChangeProposalWrapper, Dict[str, Any]]],
        entity_id: str,
        environment: str = "dev"
    ) -> str:
        """
        Emit MCPs to a single mcp_file.json (following the pattern used by tags/properties).

        Args:
            mcps: List of MCPs to emit
            entity_id: ID of the entity for directory structure
            environment: Environment name

        Returns:
            Path to the written file
        """
        # Create environment-specific directory
        env_dir = self.output_dir / environment
        env_dir.mkdir(parents=True, exist_ok=True)

        return self.emit(mcps, "mcp_file.json")

    def emit_batch(
        self,
        batch_mcps: Dict[str, List[Union[MetadataChangeProposalWrapper, Dict[str, Any]]]],
        environment: str = "dev"
    ) -> Dict[str, str]:
        """
        Emit multiple sets of MCPs for different entities.

        Args:
            batch_mcps: Dictionary mapping entity IDs to their MCPs
            environment: Environment name

        Returns:
            Dictionary mapping entity IDs to file paths
        """
        results = {}

        for entity_id, mcps in batch_mcps.items():
            try:
                file_path = self.emit_single_mcp_file(mcps, entity_id, environment)
                results[entity_id] = file_path
            except Exception as e:
                self.logger.error(f"Error emitting MCPs for entity {entity_id}: {e}")
                results[entity_id] = f"ERROR: {str(e)}"

        return results
