"""
Tag MCP output operations for DataHub CI/CD client.
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.mcp_emitter import MCPEmitter

from .base_mcp_output import (
    EntityRelationshipAsyncOutput,
    MetadataAssignmentAsyncOutput,
)

# Import DataHub schema classes
try:
    from datahub.metadata.schema_classes import (
        AuditStampClass,
        ChangeTypeClass,
        GlobalTagsClass,
        MetadataChangeProposalClass,
        OwnerClass,
        OwnershipClass,
        OwnershipTypeClass,
        SystemMetadataClass,
        TagAssociationClass,
        TagKeyClass,
        TagPropertiesClass,
    )
except ImportError:
    # Fallback if schema classes not available
    TagKeyClass = None
    TagPropertiesClass = None
    OwnershipClass = None
    OwnerClass = None
    OwnershipTypeClass = None
    GlobalTagsClass = None
    TagAssociationClass = None
    ChangeTypeClass = None
    SystemMetadataClass = None
    AuditStampClass = None
    MetadataChangeProposalClass = None


class TagAsyncOutput(EntityRelationshipAsyncOutput, MetadataAssignmentAsyncOutput):
    """
    MCP output operations for tags using proper DataHub schema classes.
    """

    def __init__(self, output_dir: Optional[str] = None):
        """Initialize tag MCP output."""
        super().__init__(output_dir)
        self._check_schema_classes()

    def _check_schema_classes(self):
        """Check if schema classes are available."""
        if not TagKeyClass:
            self.logger.warning("DataHub schema classes not available, using basic dictionaries")

    def _create_mcp_emitter(self) -> MCPEmitter:
        """Create tag-specific MCP emitter."""
        return MCPEmitter(self.output_dir)

    def create_entity_mcps(self, entity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create MCPs for tag creation using proper schema classes."""
        mcps = []

        try:
            tag_urn = entity_data.get("urn") or self._generate_entity_urn(entity_data)

            if TagKeyClass and TagPropertiesClass:
                # Create proper DataHub MCP using schema classes
                key = TagKeyClass(name=entity_data.get("name", "unknown"))

                properties = TagPropertiesClass(
                    name=entity_data.get("name", ""),
                    description=entity_data.get("description"),
                    colorHex=entity_data.get("colorHex"),
                )

                # Create MCP for properties
                mcp = MetadataChangeProposalClass(
                    entityUrn=tag_urn,
                    entityType="tag",
                    aspectName="tagProperties",
                    aspect=properties,
                    changeType=ChangeTypeClass.UPSERT,
                )

                mcps.append(mcp.to_obj())

                # Add ownership if provided
                if entity_data.get("owners"):
                    ownership_mcps = self._create_ownership_mcps(tag_urn, entity_data["owners"])
                    mcps.extend(ownership_mcps)

            else:
                # Fallback to basic dictionary structure
                mcps.append(
                    {
                        "entityUrn": tag_urn,
                        "entityType": "tag",
                        "aspectName": "tagProperties",
                        "aspect": entity_data,
                        "changeType": "UPSERT",
                    }
                )

        except Exception as e:
            self.logger.error(f"Error creating tag MCPs: {e}")
            # Fallback to basic structure
            mcps.append({"operation": "create", "entity": entity_data, "error": str(e)})

        return mcps

    def _create_ownership_mcps(
        self, entity_urn: str, owners: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Create ownership MCPs."""
        mcps = []

        if OwnershipClass and OwnerClass:
            try:
                owner_objects = []
                for owner_data in owners:
                    owner = OwnerClass(
                        owner=owner_data.get("owner", ""),
                        type=OwnershipTypeClass._from_dict(
                            {"code": owner_data.get("type", "DATAOWNER")}
                        )
                        if OwnershipTypeClass
                        else "DATAOWNER",
                    )
                    owner_objects.append(owner)

                ownership = OwnershipClass(owners=owner_objects)

                mcp = MetadataChangeProposalClass(
                    entityUrn=entity_urn,
                    entityType="tag",
                    aspectName="ownership",
                    aspect=ownership,
                    changeType=ChangeTypeClass.UPSERT,
                )

                mcps.append(mcp.to_obj())

            except Exception as e:
                self.logger.error(f"Error creating ownership MCPs: {e}")
                # Fallback
                mcps.append(
                    {
                        "entityUrn": entity_urn,
                        "aspectName": "ownership",
                        "aspect": {"owners": owners},
                        "changeType": "UPSERT",
                    }
                )

        return mcps

    def update_entity_mcps(
        self, entity_urn: str, entity_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Create MCPs for tag update using proper schema classes."""
        # For updates, add the URN to entity data and use create_entity_mcps
        entity_data["urn"] = entity_urn
        return self.create_entity_mcps(entity_data)

    def delete_entity_mcps(self, entity_urn: str) -> List[Dict[str, Any]]:
        """Create MCPs for tag deletion."""
        mcps = []

        try:
            if MetadataChangeProposalClass:
                # Create proper deletion MCP by removing all aspects
                aspects_to_remove = ["tagProperties", "ownership"]

                for aspect_name in aspects_to_remove:
                    mcp = MetadataChangeProposalClass(
                        entityUrn=entity_urn,
                        entityType="tag",
                        aspectName=aspect_name,
                        aspect=None,
                        changeType=ChangeTypeClass.DELETE if ChangeTypeClass else "DELETE",
                    )

                    mcps.append(
                        mcp.to_obj()
                        if hasattr(mcp, "to_obj")
                        else {
                            "entityUrn": entity_urn,
                            "entityType": "tag",
                            "aspectName": aspect_name,
                            "aspect": None,
                            "changeType": "DELETE",
                        }
                    )
            else:
                # Fallback
                mcps.append({"operation": "delete", "urn": entity_urn, "entityType": "tag"})

        except Exception as e:
            self.logger.error(f"Error creating deletion MCPs: {e}")
            mcps.append({"operation": "delete", "urn": entity_urn, "error": str(e)})

        return mcps

    def add_owner_mcps(
        self, entity_urn: str, owner_urn: str, ownership_type: str
    ) -> List[Dict[str, Any]]:
        """Create MCPs for adding owner to tag."""
        return self._create_ownership_mcps(
            entity_urn, [{"owner": owner_urn, "type": ownership_type}]
        )

    def remove_owner_mcps(
        self, entity_urn: str, owner_urn: str, ownership_type: str
    ) -> List[Dict[str, Any]]:
        """Create MCPs for removing owner from tag."""
        # This would require getting current ownership and removing the specific owner
        # For now, return empty list as this is complex to implement
        return []

    def assign_to_entity_mcps(
        self, entity_urn: str, tag_urn: str, **kwargs
    ) -> List[Dict[str, Any]]:
        """Create MCPs for assigning tag to entity."""
        if GlobalTagsClass and TagAssociationClass:
            try:
                tag_assoc = TagAssociationClass(tag=tag_urn)
                global_tags = GlobalTagsClass(tags=[tag_assoc])

                mcp = MetadataChangeProposalClass(
                    entityUrn=entity_urn,
                    entityType=kwargs.get("entity_type", "dataset"),
                    aspectName="globalTags",
                    aspect=global_tags,
                    changeType=ChangeTypeClass.UPSERT,
                )

                return [mcp.to_obj()]

            except Exception as e:
                self.logger.error(f"Error creating tag assignment MCPs: {e}")

        # Fallback
        return [
            {
                "entityUrn": entity_urn,
                "aspectName": "globalTags",
                "aspect": {"tags": [{"tag": tag_urn}]},
                "changeType": "UPSERT",
            }
        ]

    def remove_from_entity_mcps(self, entity_urn: str, tag_urn: str) -> List[Dict[str, Any]]:
        """Create MCPs for removing tag from entity."""
        # This would require getting current tags and removing the specific tag
        # For now, return empty list as this is complex to implement
        return []

    def _generate_entity_urn(self, entity_data: Dict[str, Any]) -> str:
        """Generate tag URN from entity data."""
        tag_id = entity_data.get("id", entity_data.get("name", "unknown"))
        return f"urn:li:tag:{tag_id}"

    def create_bulk_tag_mcps(self, tags_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create MCPs for multiple tags at once."""
        all_mcps = []
        for tag_data in tags_data:
            tag_mcps = self.create_entity_mcps(tag_data)
            all_mcps.extend(tag_mcps)
        return all_mcps

    def create_tag_assignment_mcps(
        self, tag_urn: str, entity_urns: List[str]
    ) -> List[Dict[str, Any]]:
        """Create MCPs for assigning a tag to multiple entities."""
        mcps = []
        for entity_urn in entity_urns:
            entity_mcps = self.assign_to_entity_mcps(entity_urn, tag_urn)
            mcps.extend(entity_mcps)
        return mcps

    def create_entity_tagging_mcps(
        self, entity_urn: str, tag_urns: List[str]
    ) -> List[Dict[str, Any]]:
        """Create MCPs for assigning multiple tags to an entity."""
        global_tags_mcp = self.mcp_emitter.create_mcp(
            entity_urn=entity_urn,
            aspect_name="globalTags",
            aspect_value={"tags": [{"tag": tag_urn} for tag_urn in tag_urns]},
        )
        return [global_tags_mcp]
