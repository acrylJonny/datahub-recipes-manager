"""
MCP (Metadata Change Proposal) emitter utilities.

This module provides utilities for generating and emitting MCPs,
supporting both file output and direct emission to DataHub.
"""

import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


class MCPEmitter:
    """Base class for MCP emission operations."""

    def __init__(self, output_dir: Optional[str] = None):
        """
        Initialize MCP emitter.

        Args:
            output_dir: Directory for file outputs (optional)
        """
        self.output_dir = Path(output_dir) if output_dir else None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.mcps = []

    def create_mcp(
        self,
        entity_urn: str,
        aspect_name: str,
        aspect_value: Dict[str, Any],
        change_type: str = "UPSERT",
        system_metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Create a single MCP.

        Args:
            entity_urn: URN of the entity
            aspect_name: Name of the aspect
            aspect_value: Aspect data
            change_type: Type of change (UPSERT, DELETE, etc.)
            system_metadata: Optional system metadata

        Returns:
            MCP dictionary
        """
        mcp = {
            "entityUrn": entity_urn,
            "entityType": self._extract_entity_type(entity_urn),
            "changeType": change_type,
            "aspectName": aspect_name,
            "aspect": {"json": aspect_value},
        }

        if system_metadata:
            mcp["systemMetadata"] = system_metadata
        else:
            mcp["systemMetadata"] = {
                "lastObserved": int(datetime.now().timestamp() * 1000),
                "runId": str(uuid.uuid4()),
            }

        return mcp

    def add_mcp(self, mcp: Dict[str, Any]) -> None:
        """Add an MCP to the collection."""
        self.mcps.append(mcp)

    def add_mcps(self, mcps: List[Dict[str, Any]]) -> None:
        """Add multiple MCPs to the collection."""
        self.mcps.extend(mcps)

    def clear_mcps(self) -> None:
        """Clear all MCPs."""
        self.mcps = []

    def get_mcps(self) -> List[Dict[str, Any]]:
        """Get all MCPs."""
        return self.mcps.copy()

    def emit_to_file(self, filename: str, format: str = "json") -> str:
        """
        Emit MCPs to a file.

        Args:
            filename: Output filename
            format: Output format (json, jsonl)

        Returns:
            Path to the created file
        """
        if not self.output_dir:
            raise ValueError("Output directory not configured")

        self.output_dir.mkdir(parents=True, exist_ok=True)
        filepath = self.output_dir / filename

        if format == "json":
            with open(filepath, "w") as f:
                json.dump(self.mcps, f, indent=2)
        elif format == "jsonl":
            with open(filepath, "w") as f:
                for mcp in self.mcps:
                    f.write(json.dumps(mcp) + "\n")
        else:
            raise ValueError(f"Unsupported format: {format}")

        self.logger.info(f"Emitted {len(self.mcps)} MCPs to {filepath}")
        return str(filepath)

    def emit_direct(self, connection) -> bool:
        """
        Emit MCPs directly to DataHub.

        Args:
            connection: DataHub connection instance

        Returns:
            Success status
        """
        try:
            # TODO: Implement direct emission using DataHub SDK
            self.logger.info(f"Would emit {len(self.mcps)} MCPs directly to DataHub")
            return True
        except Exception as e:
            self.logger.error(f"Failed to emit MCPs directly: {e}")
            return False

    def _extract_entity_type(self, entity_urn: str) -> str:
        """Extract entity type from URN."""
        # URN format: urn:li:entityType:entityId
        parts = entity_urn.split(":")
        if len(parts) >= 3:
            return parts[2]
        return "unknown"


class TagMCPEmitter(MCPEmitter):
    """Specialized MCP emitter for tags."""

    def create_tag_mcp(
        self, tag_urn: str, name: str, description: str = "", color_hex: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create MCP for tag creation/update."""
        aspect_value = {"name": name, "description": description}

        if color_hex:
            aspect_value["colorHex"] = color_hex

        return self.create_mcp(
            entity_urn=tag_urn, aspect_name="tagProperties", aspect_value=aspect_value
        )

    def create_tag_ownership_mcp(
        self, tag_urn: str, owners: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Create MCP for tag ownership."""
        aspect_value = {
            "owners": owners,
            "lastModified": {
                "time": int(datetime.now().timestamp() * 1000),
                "actor": "urn:li:corpuser:datahub",
            },
        }

        return self.create_mcp(
            entity_urn=tag_urn, aspect_name="ownership", aspect_value=aspect_value
        )


class DomainMCPEmitter(MCPEmitter):
    """Specialized MCP emitter for domains."""

    def create_domain_mcp(
        self,
        domain_urn: str,
        name: str,
        description: str = "",
        parent_domain_urn: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create MCP for domain creation/update."""
        aspect_value = {"name": name, "description": description}

        if parent_domain_urn:
            aspect_value["parentDomain"] = parent_domain_urn

        return self.create_mcp(
            entity_urn=domain_urn, aspect_name="domainProperties", aspect_value=aspect_value
        )


class StructuredPropertyMCPEmitter(MCPEmitter):
    """Specialized MCP emitter for structured properties."""

    def create_structured_property_mcp(
        self,
        property_urn: str,
        qualified_name: str,
        display_name: str,
        value_type: str,
        cardinality: str = "SINGLE",
        description: str = "",
        entity_types: Optional[List[str]] = None,
        allowed_values: Optional[List[Any]] = None,
    ) -> Dict[str, Any]:
        """Create MCP for structured property creation/update."""
        aspect_value = {
            "qualifiedName": qualified_name,
            "displayName": display_name,
            "valueType": value_type,
            "cardinality": cardinality,
            "description": description,
        }

        if entity_types:
            aspect_value["entityTypes"] = entity_types

        if allowed_values:
            aspect_value["allowedValues"] = allowed_values

        return self.create_mcp(
            entity_urn=property_urn,
            aspect_name="structuredPropertyDefinition",
            aspect_value=aspect_value,
        )


class GlossaryMCPEmitter(MCPEmitter):
    """Specialized MCP emitter for glossary entities."""

    def create_glossary_node_mcp(
        self, node_urn: str, name: str, description: str = "", parent_node_urn: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create MCP for glossary node creation/update."""
        aspect_value = {"name": name, "description": description}

        if parent_node_urn:
            aspect_value["parentNode"] = parent_node_urn

        return self.create_mcp(
            entity_urn=node_urn, aspect_name="glossaryNodeInfo", aspect_value=aspect_value
        )

    def create_glossary_term_mcp(
        self,
        term_urn: str,
        name: str,
        description: str = "",
        parent_node_urn: Optional[str] = None,
        term_source: str = "INTERNAL",
    ) -> Dict[str, Any]:
        """Create MCP for glossary term creation/update."""
        aspect_value = {"name": name, "description": description, "termSource": term_source}

        if parent_node_urn:
            aspect_value["parentNode"] = parent_node_urn

        return self.create_mcp(
            entity_urn=term_urn, aspect_name="glossaryTermInfo", aspect_value=aspect_value
        )


class DataProductMCPEmitter(MCPEmitter):
    """Specialized MCP emitter for data products."""

    def create_data_product_mcp(
        self, product_urn: str, name: str, description: str = "", external_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create MCP for data product creation/update."""
        aspect_value = {"name": name, "description": description}

        if external_url:
            aspect_value["externalUrl"] = external_url

        return self.create_mcp(
            entity_urn=product_urn, aspect_name="dataProductProperties", aspect_value=aspect_value
        )


class IngestionMCPEmitter(MCPEmitter):
    """Specialized MCP emitter for ingestion sources."""

    def create_ingestion_source_mcp(
        self,
        source_urn: str,
        name: str,
        source_type: str,
        recipe: Dict[str, Any],
        schedule_interval: str = "0 0 * * *",
        timezone: str = "UTC",
        executor_id: str = "default",
        debug_mode: bool = False,
    ) -> List[Dict[str, Any]]:
        """Create MCPs for ingestion source creation/update."""
        mcps = []

        # Key aspect
        key_aspect = {"id": source_urn.split(":")[-1]}
        mcps.append(
            self.create_mcp(
                entity_urn=source_urn,
                aspect_name="dataHubIngestionSourceKey",
                aspect_value=key_aspect,
            )
        )

        # Info aspect
        info_aspect = {
            "name": name,
            "type": source_type,
            "schedule": {"interval": schedule_interval, "timezone": timezone},
            "config": {
                "recipe": json.dumps(recipe) if isinstance(recipe, dict) else recipe,
                "executorId": executor_id,
                "debugMode": debug_mode,
            },
        }
        mcps.append(
            self.create_mcp(
                entity_urn=source_urn,
                aspect_name="dataHubIngestionSourceInfo",
                aspect_value=info_aspect,
            )
        )

        return mcps
