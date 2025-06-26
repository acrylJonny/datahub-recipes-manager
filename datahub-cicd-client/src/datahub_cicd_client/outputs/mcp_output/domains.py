"""
Domain MCP output operations for DataHub CI/CD client.
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
        DomainKeyClass,
        DomainPropertiesClass,
        GlobalTagsClass,
        MetadataChangeProposalClass,
        OwnerClass,
        OwnershipClass,
        OwnershipTypeClass,
        SystemMetadataClass,
        TagAssociationClass,
    )
except ImportError:
    # Fallback if schema classes not available
    DomainKeyClass = None
    DomainPropertiesClass = None
    OwnershipClass = None
    OwnerClass = None
    OwnershipTypeClass = None
    GlobalTagsClass = None
    TagAssociationClass = None
    ChangeTypeClass = None
    SystemMetadataClass = None
    AuditStampClass = None
    MetadataChangeProposalClass = None


class DomainAsyncOutput(EntityRelationshipAsyncOutput, MetadataAssignmentAsyncOutput):
    """
    MCP output operations for domains using proper DataHub schema classes.
    """

    def __init__(self, output_dir: Optional[str] = None):
        """Initialize domain MCP output."""
        super().__init__(output_dir)
        self._check_schema_classes()

    def _check_schema_classes(self):
        """Check if schema classes are available."""
        if not DomainKeyClass:
            self.logger.warning("DataHub schema classes not available, using basic dictionaries")

    def _create_mcp_emitter(self) -> MCPEmitter:
        """Create domain-specific MCP emitter."""
        return MCPEmitter(self.output_dir)

    def create_entity_mcps(self, entity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create MCPs for domain creation using proper schema classes."""
        mcps = []

        try:
            domain_urn = entity_data.get("urn") or self._generate_entity_urn(entity_data)

            if DomainKeyClass and DomainPropertiesClass:
                # Create proper DataHub MCP using schema classes
                key = DomainKeyClass(id=entity_data.get("id", "unknown"))

                properties = DomainPropertiesClass(
                    name=entity_data.get("name", ""),
                    description=entity_data.get("description"),
                    parentDomain=entity_data.get("parentDomain"),
                    customProperties=entity_data.get("customProperties", {}),
                )

                # Create MCP for properties
                mcp = MetadataChangeProposalClass(
                    entityUrn=domain_urn,
                    entityType="domain",
                    aspectName="domainProperties",
                    aspect=properties,
                    changeType=ChangeTypeClass.UPSERT,
                )

                mcps.append(mcp.to_obj())

                # Add ownership if provided
                if entity_data.get("owners"):
                    ownership_mcps = self._create_ownership_mcps(domain_urn, entity_data["owners"])
                    mcps.extend(ownership_mcps)

                # Add tags if provided
                if entity_data.get("globalTags"):
                    tag_mcps = self._create_tag_mcps(domain_urn, entity_data["globalTags"])
                    mcps.extend(tag_mcps)

            else:
                # Fallback to basic dictionary structure
                mcps.append(
                    {
                        "entityUrn": domain_urn,
                        "entityType": "domain",
                        "aspectName": "domainProperties",
                        "aspect": entity_data,
                        "changeType": "UPSERT",
                    }
                )

        except Exception as e:
            self.logger.error(f"Error creating domain MCPs: {e}")
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
                    entityType="domain",
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

    def _create_tag_mcps(self, entity_urn: str, tags: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create tag MCPs."""
        mcps = []

        if GlobalTagsClass and TagAssociationClass:
            try:
                tag_associations = []
                for tag_data in tags:
                    tag_assoc = TagAssociationClass(
                        tag=tag_data.get("tag", ""), context=tag_data.get("context")
                    )
                    tag_associations.append(tag_assoc)

                global_tags = GlobalTagsClass(tags=tag_associations)

                mcp = MetadataChangeProposalClass(
                    entityUrn=entity_urn,
                    entityType="domain",
                    aspectName="globalTags",
                    aspect=global_tags,
                    changeType=ChangeTypeClass.UPSERT,
                )

                mcps.append(mcp.to_obj())

            except Exception as e:
                self.logger.error(f"Error creating tag MCPs: {e}")
                # Fallback
                mcps.append(
                    {
                        "entityUrn": entity_urn,
                        "aspectName": "globalTags",
                        "aspect": {"tags": tags},
                        "changeType": "UPSERT",
                    }
                )

        return mcps

    def update_entity_mcps(
        self, entity_urn: str, entity_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Create MCPs for domain update using proper schema classes."""
        # For updates, add the URN to entity data and use create_entity_mcps
        entity_data["urn"] = entity_urn
        return self.create_entity_mcps(entity_data)

    def delete_entity_mcps(self, entity_urn: str) -> List[Dict[str, Any]]:
        """Create MCPs for domain deletion."""
        mcps = []

        try:
            if MetadataChangeProposalClass:
                # Create proper deletion MCP by removing all aspects
                aspects_to_remove = ["domainProperties", "ownership", "globalTags"]

                for aspect_name in aspects_to_remove:
                    mcp = MetadataChangeProposalClass(
                        entityUrn=entity_urn,
                        entityType="domain",
                        aspectName=aspect_name,
                        aspect=None,
                        changeType=ChangeTypeClass.DELETE if ChangeTypeClass else "DELETE",
                    )

                    mcps.append(
                        mcp.to_obj()
                        if hasattr(mcp, "to_obj")
                        else {
                            "entityUrn": entity_urn,
                            "entityType": "domain",
                            "aspectName": aspect_name,
                            "aspect": None,
                            "changeType": "DELETE",
                        }
                    )
            else:
                # Fallback
                mcps.append({"operation": "delete", "urn": entity_urn, "entityType": "domain"})

        except Exception as e:
            self.logger.error(f"Error creating deletion MCPs: {e}")
            mcps.append({"operation": "delete", "urn": entity_urn, "error": str(e)})

        return mcps

    def add_owner_mcps(
        self, entity_urn: str, owner_urn: str, ownership_type: str
    ) -> List[Dict[str, Any]]:
        """Create MCPs for adding owner to domain."""
        return self._create_ownership_mcps(
            entity_urn, [{"owner": owner_urn, "type": ownership_type}]
        )

    def remove_owner_mcps(
        self, entity_urn: str, owner_urn: str, ownership_type: str
    ) -> List[Dict[str, Any]]:
        """Create MCPs for removing owner from domain."""
        # This would require getting current ownership and removing the specific owner
        # For now, return empty list as this is complex to implement
        return []

    def assign_to_entity_mcps(
        self, entity_urn: str, metadata_urn: str, **kwargs
    ) -> List[Dict[str, Any]]:
        """Create MCPs for assigning metadata (tags) to domain."""
        metadata_type = kwargs.get("metadata_type", "tag")

        if metadata_type == "tag":
            return self._create_tag_mcps(entity_urn, [{"tag": metadata_urn}])

        return []

    def remove_from_entity_mcps(self, entity_urn: str, metadata_urn: str) -> List[Dict[str, Any]]:
        """Create MCPs for removing metadata from domain."""
        # This would require getting current metadata and removing the specific item
        # For now, return empty list as this is complex to implement
        return []

    def _generate_entity_urn(self, entity_data: Dict[str, Any]) -> str:
        """Generate domain URN from entity data."""
        domain_id = entity_data.get("id", entity_data.get("name", "unknown"))
        return f"urn:li:domain:{domain_id}"

    def create_bulk_domain_mcps(self, domains_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create MCPs for multiple domains at once."""
        all_mcps = []
        for domain_data in domains_data:
            domain_mcps = self.create_entity_mcps(domain_data)
            all_mcps.extend(domain_mcps)
        return all_mcps

    def create_domain_assignment_mcps(
        self, domain_urn: str, entity_urns: List[str]
    ) -> List[Dict[str, Any]]:
        """Create MCPs for assigning a domain to multiple entities."""
        mcps = []
        for entity_urn in entity_urns:
            entity_mcps = self.assign_to_entity_mcps(entity_urn, domain_urn)
            mcps.extend(entity_mcps)
        return mcps

    def create_entity_domain_assignment_mcps(
        self, entity_urn: str, domain_urn: str
    ) -> List[Dict[str, Any]]:
        """Create MCPs for assigning a domain to an entity."""
        domain_mcp = self.mcp_emitter.create_mcp(
            entity_urn=entity_urn, aspect_name="domains", aspect_value={"domains": [domain_urn]}
        )
        return [domain_mcp]
