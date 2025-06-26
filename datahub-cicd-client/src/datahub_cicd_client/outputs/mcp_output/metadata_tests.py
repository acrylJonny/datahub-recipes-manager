"""
Metadata Test MCP output operations for DataHub CI/CD client.
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.mcp_emitter import MCPEmitter

from .base_mcp_output import BaseAsyncOutput

# Import DataHub schema classes
try:
    from datahub.metadata.schema_classes import (
        AuditStampClass,
        ChangeTypeClass,
        MetadataChangeProposalClass,
        SystemMetadataClass,
        TestDefinitionClass,
        TestDefinitionTypeClass,
        TestInfoClass,
        TestResultClass,
        TestResultTypeClass,
    )
except ImportError:
    # Fallback if schema classes not available
    TestDefinitionClass = None
    TestDefinitionTypeClass = None
    TestInfoClass = None
    TestResultClass = None
    TestResultTypeClass = None
    ChangeTypeClass = None
    SystemMetadataClass = None
    AuditStampClass = None
    MetadataChangeProposalClass = None


class MetadataTestAsyncOutput(BaseAsyncOutput):
    """
    MCP output operations for metadata tests using proper DataHub schema classes.
    """

    def __init__(self, output_dir: Optional[str] = None):
        """Initialize metadata test MCP output."""
        super().__init__(output_dir)
        self._check_schema_classes()

    def _check_schema_classes(self):
        """Check if schema classes are available."""
        if not TestDefinitionClass:
            self.logger.warning("DataHub schema classes not available, using basic dictionaries")

    def _create_mcp_emitter(self) -> MCPEmitter:
        """Create test-specific MCP emitter."""
        return MCPEmitter(self.output_dir)

    def create_entity_mcps(self, entity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create MCPs for metadata test creation using proper schema classes."""
        mcps = []

        try:
            test_urn = entity_data.get("urn") or self._generate_entity_urn(entity_data)

            if TestDefinitionClass and TestInfoClass:
                # Create proper DataHub MCP using schema classes
                test_definition = TestDefinitionClass(
                    type=TestDefinitionTypeClass._from_dict(
                        {"code": entity_data.get("type", "CUSTOM")}
                    )
                    if TestDefinitionTypeClass
                    else "CUSTOM"
                )

                test_info = TestInfoClass(
                    name=entity_data.get("name", ""),
                    category=entity_data.get("category", ""),
                    description=entity_data.get("description"),
                    definition=test_definition,
                )

                # Create MCP for test info
                mcp = MetadataChangeProposalClass(
                    entityUrn=test_urn,
                    entityType="test",
                    aspectName="testInfo",
                    aspect=test_info,
                    changeType=ChangeTypeClass.UPSERT,
                )

                mcps.append(mcp.to_obj())

                # Add test results if provided
                if entity_data.get("results"):
                    result_mcps = self._create_test_result_mcps(test_urn, entity_data["results"])
                    mcps.extend(result_mcps)

            else:
                # Fallback to basic dictionary structure
                mcps.append(
                    {
                        "entityUrn": test_urn,
                        "entityType": "test",
                        "aspectName": "testInfo",
                        "aspect": entity_data,
                        "changeType": "UPSERT",
                    }
                )

        except Exception as e:
            self.logger.error(f"Error creating metadata test MCPs: {e}")
            # Fallback to basic structure
            mcps.append({"operation": "create", "entity": entity_data, "error": str(e)})

        return mcps

    def _create_test_result_mcps(
        self, test_urn: str, results: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Create test result MCPs."""
        mcps = []

        if TestResultClass:
            try:
                for result_data in results:
                    test_result = TestResultClass(
                        type=TestResultTypeClass._from_dict(
                            {"code": result_data.get("type", "SUCCESS")}
                        )
                        if TestResultTypeClass
                        else "SUCCESS",
                        nativeResults=result_data.get("nativeResults", {}),
                    )

                    mcp = MetadataChangeProposalClass(
                        entityUrn=test_urn,
                        entityType="test",
                        aspectName="testResults",
                        aspect=test_result,
                        changeType=ChangeTypeClass.UPSERT,
                    )

                    mcps.append(mcp.to_obj())

            except Exception as e:
                self.logger.error(f"Error creating test result MCPs: {e}")
                # Fallback
                mcps.append(
                    {
                        "entityUrn": test_urn,
                        "aspectName": "testResults",
                        "aspect": {"results": results},
                        "changeType": "UPSERT",
                    }
                )

        return mcps

    def update_entity_mcps(
        self, entity_urn: str, entity_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Create MCPs for metadata test update using proper schema classes."""
        # For updates, add the URN to entity data and use create_entity_mcps
        entity_data["urn"] = entity_urn
        return self.create_entity_mcps(entity_data)

    def delete_entity_mcps(self, entity_urn: str) -> List[Dict[str, Any]]:
        """Create MCPs for metadata test deletion."""
        mcps = []

        try:
            if MetadataChangeProposalClass:
                # Create proper deletion MCP by removing all aspects
                aspects_to_remove = ["testInfo", "testResults"]

                for aspect_name in aspects_to_remove:
                    mcp = MetadataChangeProposalClass(
                        entityUrn=entity_urn,
                        entityType="test",
                        aspectName=aspect_name,
                        aspect=None,
                        changeType=ChangeTypeClass.DELETE if ChangeTypeClass else "DELETE",
                    )

                    mcps.append(
                        mcp.to_obj()
                        if hasattr(mcp, "to_obj")
                        else {
                            "entityUrn": entity_urn,
                            "entityType": "test",
                            "aspectName": aspect_name,
                            "aspect": None,
                            "changeType": "DELETE",
                        }
                    )
            else:
                # Fallback
                mcps.append({"operation": "delete", "urn": entity_urn, "entityType": "test"})

        except Exception as e:
            self.logger.error(f"Error creating deletion MCPs: {e}")
            mcps.append({"operation": "delete", "urn": entity_urn, "error": str(e)})

        return mcps

    def _generate_entity_urn(self, entity_data: Dict[str, Any]) -> str:
        """Generate metadata test URN from entity data."""
        test_id = entity_data.get("id", entity_data.get("name", "unknown"))
        return f"urn:li:test:{test_id}"
