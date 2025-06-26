"""
Integration module for existing scripts with the new input/output architecture.

This module demonstrates how to integrate existing script functionality
with the new enhanced services that support both sync and async operations.
"""

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add scripts directory to path for imports
scripts_dir = Path(__file__).parent.parent.parent.parent / "scripts"
sys.path.append(str(scripts_dir))

from datahub_cicd_client.core.connection import DataHubConnection
from datahub_cicd_client.services.tags_enhanced import EnhancedTagService


class ScriptsIntegration:
    """Integration layer for existing scripts with enhanced services."""

    def __init__(self, connection: DataHubConnection, output_dir: Optional[str] = None):
        """Initialize with connection and optional output directory."""
        self.connection = connection
        self.output_dir = output_dir

        # Initialize enhanced services
        self.tag_service = EnhancedTagService(connection, output_dir)

    def migrate_tag_script_functionality(self, environment: str = "dev") -> Dict[str, Any]:
        """
        Demonstrate migration of tag script functionality to enhanced service.

        This shows how existing script operations can be performed using
        the new input/output architecture.
        """
        results = {"sync_operations": [], "async_operations": [], "batch_operations": []}

        # Example 1: Synchronous operations (immediate GraphQL execution)
        self.tag_service.set_sync_mode(True)
        self.tag_service.set_output_mode(emit_to_file=False)

        # Create a tag synchronously
        sync_result = self.tag_service.create_tag(
            tag_id="example_sync_tag",
            name="Example Sync Tag",
            description="Created via synchronous operation",
            color_hex="#FF5733",
            owner="datahub",
        )
        results["sync_operations"].append(sync_result.to_dict())

        # Example 2: Asynchronous operations (MCP generation)
        self.tag_service.set_sync_mode(False)
        self.tag_service.set_output_mode(emit_to_file=True, output_dir=self.output_dir)

        # Create a tag asynchronously (generates MCPs)
        async_result = self.tag_service.create_tag(
            tag_id="example_async_tag",
            name="Example Async Tag",
            description="Created via asynchronous operation",
            color_hex="#33FF57",
            owner="datahub",
        )
        results["async_operations"].append(async_result.to_dict())

        # Example 3: Batch operations
        self.tag_service.set_batch_mode(True)

        # Add multiple operations to batch
        batch_tags = [
            {
                "id": f"batch_tag_{i}",
                "name": f"Batch Tag {i}",
                "description": f"Tag created in batch operation {i}",
                "colorHex": f"#{'%06x' % (i * 111111 % 16777215)}",
                "owner": "datahub",
            }
            for i in range(3)
        ]

        batch_result = self.tag_service.bulk_create_tags(batch_tags)
        results["batch_operations"].append(
            {"operation": "bulk_create_tags", "summary": batch_result.get_summary()}
        )

        # Flush batch to execute all operations
        flush_results = self.tag_service.flush_batch()
        results["batch_operations"].append(
            {"operation": "flush_batch", "results_count": len(flush_results)}
        )

        # Emit any collected MCPs
        if self.tag_service.get_mcps():
            mcp_file = self.tag_service.emit_mcps(f"tags_integration_{environment}.json")
            results["mcp_file"] = mcp_file

        return results

    def import_from_existing_scripts(self, script_output_dir: str) -> Dict[str, Any]:
        """
        Import data from existing script outputs.

        This demonstrates how to take existing script outputs and process them
        through the new architecture.
        """
        results = {"imported_files": [], "operations": []}

        script_path = Path(script_output_dir)
        if not script_path.exists():
            return {"error": f"Script output directory not found: {script_output_dir}"}

        # Look for JSON files from existing scripts
        for json_file in script_path.glob("*.json"):
            try:
                with open(json_file) as f:
                    data = json.load(f)

                # Process different types of script outputs
                if "tags" in str(json_file).lower():
                    # Handle tag data
                    if isinstance(data, list):
                        # Bulk import tags
                        self.tag_service.set_sync_mode(False)  # Use async for bulk import
                        batch_result = self.tag_service.import_tags_from_json(data)

                        results["operations"].append(
                            {
                                "file": str(json_file),
                                "type": "tag_import",
                                "summary": batch_result.get_summary(),
                            }
                        )

                results["imported_files"].append(str(json_file))

            except Exception as e:
                results["errors"] = results.get("errors", [])
                results["errors"].append(f"Error processing {json_file}: {str(e)}")

        return results

    def export_for_ci_cd(self, environment: str, entities: List[str]) -> Dict[str, Any]:
        """
        Export data in formats suitable for CI/CD pipelines.

        This demonstrates how to use the new architecture to generate
        outputs suitable for automated deployment pipelines.
        """
        results = {"exports": [], "files": []}

        # Set up for MCP generation
        self.tag_service.set_sync_mode(False)
        self.tag_service.set_output_mode(emit_to_file=True, output_dir=self.output_dir)

        for entity_type in entities:
            if entity_type.lower() == "tags":
                # Export all tags as MCPs
                tags_data = self.tag_service.get_remote_tags_data()
                if tags_data.get("success"):
                    tags = tags_data["data"]["tags"]

                    # Convert to format suitable for MCP generation
                    export_data = []
                    for tag in tags:
                        tag_export = {
                            "id": tag["urn"].split(":")[-1],
                            "name": tag["name"],
                            "description": tag.get("description", ""),
                            "colorHex": tag.get("colorHex"),
                        }
                        if tag.get("owner_names"):
                            tag_export["owner"] = tag["owner_names"][0]

                        export_data.append(tag_export)

                    # Export as MCPs
                    mcp_file = self.tag_service.export_tags_to_mcps(
                        export_data, f"{environment}_tags_export.json"
                    )

                    if mcp_file:
                        results["files"].append(mcp_file)
                        results["exports"].append(
                            {"entity_type": "tags", "count": len(export_data), "file": mcp_file}
                        )

        return results

    def demonstrate_workflow_patterns(self) -> Dict[str, Any]:
        """
        Demonstrate common workflow patterns using the new architecture.
        """
        results = {"patterns": []}

        # Pattern 1: Development workflow (sync operations)
        results["patterns"].append(
            {
                "name": "development_workflow",
                "description": "Interactive development with immediate feedback",
                "config": {"sync_mode": True, "emit_to_file": False, "batch_mode": False},
            }
        )

        # Pattern 2: Staging workflow (batch with file output)
        results["patterns"].append(
            {
                "name": "staging_workflow",
                "description": "Batch operations with file output for review",
                "config": {"sync_mode": False, "emit_to_file": True, "batch_mode": True},
            }
        )

        # Pattern 3: Production workflow (async with direct emission)
        results["patterns"].append(
            {
                "name": "production_workflow",
                "description": "Asynchronous operations with direct emission",
                "config": {"sync_mode": False, "emit_to_file": False, "batch_mode": True},
            }
        )

        # Pattern 4: CI/CD workflow (file-based with validation)
        results["patterns"].append(
            {
                "name": "cicd_workflow",
                "description": "File-based operations for automated pipelines",
                "config": {
                    "sync_mode": False,
                    "emit_to_file": True,
                    "batch_mode": True,
                    "validation": True,
                },
            }
        )

        return results

    def get_migration_summary(self) -> Dict[str, Any]:
        """Get summary of migration capabilities and benefits."""
        return {
            "capabilities": {
                "input_operations": [
                    "Read existing data from DataHub",
                    "Query and search functionality",
                    "Comprehensive data retrieval",
                ],
                "output_operations": [
                    "Synchronous operations via GraphQL",
                    "Asynchronous operations via MCP generation",
                    "Batch processing capabilities",
                    "File-based workflows",
                ],
                "integration_features": [
                    "Backward compatibility with existing scripts",
                    "Flexible operation modes",
                    "CI/CD pipeline support",
                    "Error handling and validation",
                ],
            },
            "benefits": {
                "developer_experience": [
                    "Unified API for all operations",
                    "Flexible sync/async modes",
                    "Comprehensive error handling",
                    "Built-in batch processing",
                ],
                "operational_benefits": [
                    "Improved performance with batch operations",
                    "Better error recovery",
                    "Audit trail and logging",
                    "Standardized output formats",
                ],
                "ci_cd_benefits": [
                    "File-based workflows for version control",
                    "Automated validation",
                    "Rollback capabilities",
                    "Environment-specific configurations",
                ],
            },
            "migration_path": [
                "1. Install new enhanced services alongside existing scripts",
                "2. Gradually migrate script functionality to enhanced services",
                "3. Use integration layer for backward compatibility",
                "4. Deprecate old scripts once migration is complete",
            ],
        }
