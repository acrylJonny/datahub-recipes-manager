"""
Assertion MCP output operations for DataHub CI/CD client.
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.mcp_emitter import MCPEmitter

from .base_mcp_output import BaseAsyncOutput

# Import DataHub schema classes
try:
    from datahub.metadata.schema_classes import (
        AssertionActionClass,
        AssertionActionTypeClass,
        AssertionInfoClass,
        AssertionKeyClass,
        AssertionResultClass,
        AssertionResultTypeClass,
        AssertionRunEventClass,
        AssertionTypeClass,
        AuditStampClass,
        ChangeTypeClass,
        MetadataChangeProposalClass,
        SystemMetadataClass,
    )
except ImportError:
    # Fallback if schema classes not available
    AssertionKeyClass = None
    AssertionInfoClass = None
    AssertionTypeClass = None
    AssertionActionClass = None
    AssertionActionTypeClass = None
    AssertionResultClass = None
    AssertionResultTypeClass = None
    AssertionRunEventClass = None
    ChangeTypeClass = None
    SystemMetadataClass = None
    AuditStampClass = None
    MetadataChangeProposalClass = None


class AssertionAsyncOutput(BaseAsyncOutput):
    """
    MCP output operations for assertions using proper DataHub schema classes.
    """

    def __init__(self, output_dir: Optional[str] = None):
        """Initialize assertion MCP output."""
        super().__init__(output_dir)
        self._check_schema_classes()

    def _check_schema_classes(self):
        """Check if schema classes are available."""
        if not AssertionKeyClass:
            self.logger.warning("DataHub schema classes not available, using basic dictionaries")

    def _create_mcp_emitter(self) -> MCPEmitter:
        """Create assertion-specific MCP emitter."""
        return MCPEmitter(self.output_dir)

    def create_entity_mcps(self, entity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create MCPs for assertion creation using proper schema classes."""
        mcps = []

        try:
            assertion_urn = entity_data.get("urn") or self._generate_entity_urn(entity_data)

            if AssertionKeyClass and AssertionInfoClass:
                # Create proper DataHub MCP using schema classes
                key = AssertionKeyClass(assertionId=entity_data.get("id", "unknown"))

                info = AssertionInfoClass(
                    type=AssertionTypeClass._from_dict({"code": entity_data.get("type", "DATASET")})
                    if AssertionTypeClass
                    else "DATASET",
                    description=entity_data.get("description"),
                    customProperties=entity_data.get("customProperties", {}),
                )

                # Create MCP for assertion info
                mcp = MetadataChangeProposalClass(
                    entityUrn=assertion_urn,
                    entityType="assertion",
                    aspectName="assertionInfo",
                    aspect=info,
                    changeType=ChangeTypeClass.UPSERT,
                )

                mcps.append(mcp.to_obj())

                # Add assertion actions if provided
                if entity_data.get("actions"):
                    action_mcps = self._create_assertion_action_mcps(
                        assertion_urn, entity_data["actions"]
                    )
                    mcps.extend(action_mcps)

                # Add assertion results if provided
                if entity_data.get("results"):
                    result_mcps = self._create_assertion_result_mcps(
                        assertion_urn, entity_data["results"]
                    )
                    mcps.extend(result_mcps)

            else:
                # Fallback to basic dictionary structure
                mcps.append(
                    {
                        "entityUrn": assertion_urn,
                        "entityType": "assertion",
                        "aspectName": "assertionInfo",
                        "aspect": entity_data,
                        "changeType": "UPSERT",
                    }
                )

        except Exception as e:
            self.logger.error(f"Error creating assertion MCPs: {e}")
            # Fallback to basic structure
            mcps.append({"operation": "create", "entity": entity_data, "error": str(e)})

        return mcps

    def _create_assertion_action_mcps(
        self, assertion_urn: str, actions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Create assertion action MCPs."""
        mcps = []

        if AssertionActionClass:
            try:
                for action_data in actions:
                    assertion_action = AssertionActionClass(
                        type=AssertionActionTypeClass._from_dict(
                            {"code": action_data.get("type", "FAIL")}
                        )
                        if AssertionActionTypeClass
                        else "FAIL"
                    )

                    mcp = MetadataChangeProposalClass(
                        entityUrn=assertion_urn,
                        entityType="assertion",
                        aspectName="assertionActions",
                        aspect=assertion_action,
                        changeType=ChangeTypeClass.UPSERT,
                    )

                    mcps.append(mcp.to_obj())

            except Exception as e:
                self.logger.error(f"Error creating assertion action MCPs: {e}")
                # Fallback
                mcps.append(
                    {
                        "entityUrn": assertion_urn,
                        "aspectName": "assertionActions",
                        "aspect": {"actions": actions},
                        "changeType": "UPSERT",
                    }
                )

        return mcps

    def _create_assertion_result_mcps(
        self, assertion_urn: str, results: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Create assertion result MCPs."""
        mcps = []

        if AssertionResultClass:
            try:
                for result_data in results:
                    assertion_result = AssertionResultClass(
                        type=AssertionResultTypeClass._from_dict(
                            {"code": result_data.get("type", "SUCCESS")}
                        )
                        if AssertionResultTypeClass
                        else "SUCCESS",
                        nativeResults=result_data.get("nativeResults", {}),
                    )

                    mcp = MetadataChangeProposalClass(
                        entityUrn=assertion_urn,
                        entityType="assertion",
                        aspectName="assertionResults",
                        aspect=assertion_result,
                        changeType=ChangeTypeClass.UPSERT,
                    )

                    mcps.append(mcp.to_obj())

            except Exception as e:
                self.logger.error(f"Error creating assertion result MCPs: {e}")
                # Fallback
                mcps.append(
                    {
                        "entityUrn": assertion_urn,
                        "aspectName": "assertionResults",
                        "aspect": {"results": results},
                        "changeType": "UPSERT",
                    }
                )

        return mcps

    def update_entity_mcps(
        self, entity_urn: str, entity_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Create MCPs for assertion update using proper schema classes."""
        # For updates, add the URN to entity data and use create_entity_mcps
        entity_data["urn"] = entity_urn
        return self.create_entity_mcps(entity_data)

    def delete_entity_mcps(self, entity_urn: str) -> List[Dict[str, Any]]:
        """Create MCPs for assertion deletion."""
        mcps = []

        try:
            if MetadataChangeProposalClass:
                # Create proper deletion MCP by removing all aspects
                aspects_to_remove = ["assertionInfo", "assertionActions", "assertionResults"]

                for aspect_name in aspects_to_remove:
                    mcp = MetadataChangeProposalClass(
                        entityUrn=entity_urn,
                        entityType="assertion",
                        aspectName=aspect_name,
                        aspect=None,
                        changeType=ChangeTypeClass.DELETE if ChangeTypeClass else "DELETE",
                    )

                    mcps.append(
                        mcp.to_obj()
                        if hasattr(mcp, "to_obj")
                        else {
                            "entityUrn": entity_urn,
                            "entityType": "assertion",
                            "aspectName": aspect_name,
                            "aspect": None,
                            "changeType": "DELETE",
                        }
                    )
            else:
                # Fallback
                mcps.append({"operation": "delete", "urn": entity_urn, "entityType": "assertion"})

        except Exception as e:
            self.logger.error(f"Error creating deletion MCPs: {e}")
            mcps.append({"operation": "delete", "urn": entity_urn, "error": str(e)})

        return mcps

    def _generate_entity_urn(self, entity_data: Dict[str, Any]) -> str:
        """Generate assertion URN from entity data."""
        assertion_id = entity_data.get("id", entity_data.get("name", "unknown"))
        return f"urn:li:assertion:{assertion_id}"

    def create_assertion_mcp(self, assertion_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create an assertion MCP."""
        try:
            self.logger.info(f"Creating assertion MCP: {assertion_data.get('urn', 'unknown')}")
            mcps = self.create_entity_mcps(assertion_data)
            return {"success": True, "urn": assertion_data.get("urn"), "mcps_generated": len(mcps)}
        except Exception as e:
            self.logger.error(f"Error creating assertion MCP: {str(e)}")
            return {"success": False, "error": str(e)}
