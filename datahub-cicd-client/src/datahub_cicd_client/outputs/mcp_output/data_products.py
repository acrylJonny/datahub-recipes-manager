"""
Data Product MCP output operations for DataHub CI/CD client.
"""

from typing import Dict, Any, Optional, List
from .base_mcp_output import BaseAsyncOutput, EntityRelationshipAsyncOutput, MetadataAssignmentAsyncOutput
from datahub_cicd_client.core.mcp_emitter import MCPEmitter

# Import DataHub schema classes
try:
    from datahub.metadata.schema_classes import (
        DataProductKeyClass,
        DataProductPropertiesClass,
        DataProductAssociationClass,
        OwnershipClass,
        OwnerClass,
        OwnershipTypeClass,
        GlobalTagsClass,
        TagAssociationClass,
        DomainsClass,
        ChangeTypeClass,
        SystemMetadataClass,
        AuditStampClass,
        MetadataChangeProposalClass
    )
except ImportError:
    # Fallback if schema classes not available
    DataProductKeyClass = None
    DataProductPropertiesClass = None
    DataProductAssociationClass = None
    OwnershipClass = None
    OwnerClass = None
    OwnershipTypeClass = None
    GlobalTagsClass = None
    TagAssociationClass = None
    DomainsClass = None
    ChangeTypeClass = None
    SystemMetadataClass = None
    AuditStampClass = None
    MetadataChangeProposalClass = None


class DataProductAsyncOutput(EntityRelationshipAsyncOutput, MetadataAssignmentAsyncOutput):
    """
    MCP output operations for data products using proper DataHub schema classes.
    """
    
    def __init__(self, output_dir: Optional[str] = None):
        """Initialize data product MCP output."""
        super().__init__(output_dir)
        self._check_schema_classes()
    
    def _check_schema_classes(self):
        """Check if schema classes are available."""
        if not DataProductKeyClass:
            self.logger.warning("DataHub schema classes not available, using basic dictionaries")
    
    def _create_mcp_emitter(self) -> MCPEmitter:
        """Create data product-specific MCP emitter."""
        return MCPEmitter(self.output_dir)
    
    def create_entity_mcps(self, entity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create MCPs for data product creation using proper schema classes."""
        mcps = []
        
        try:
            product_urn = entity_data.get("urn") or self._generate_entity_urn(entity_data)
            
            if DataProductKeyClass and DataProductPropertiesClass:
                # Create proper DataHub MCP using schema classes
                key = DataProductKeyClass(
                    id=entity_data.get("id", "unknown")
                )
                
                properties = DataProductPropertiesClass(
                    name=entity_data.get("name", ""),
                    description=entity_data.get("description"),
                    customProperties=entity_data.get("customProperties", {}),
                    tags=entity_data.get("tags", [])
                )
                
                # Create MCP for properties
                mcp = MetadataChangeProposalClass(
                    entityUrn=product_urn,
                    entityType="dataProduct",
                    aspectName="dataProductProperties",
                    aspect=properties,
                    changeType=ChangeTypeClass.UPSERT
                )
                
                mcps.append(mcp.to_obj())
                
                # Add ownership if provided
                if entity_data.get("owners"):
                    ownership_mcps = self._create_ownership_mcps(product_urn, entity_data["owners"])
                    mcps.extend(ownership_mcps)
                
                # Add domains if provided
                if entity_data.get("domains"):
                    domain_mcps = self._create_domain_mcps(product_urn, entity_data["domains"])
                    mcps.extend(domain_mcps)
                    
                # Add tags if provided
                if entity_data.get("globalTags"):
                    tag_mcps = self._create_tag_mcps(product_urn, entity_data["globalTags"])
                    mcps.extend(tag_mcps)
                    
            else:
                # Fallback to basic dictionary structure
                mcps.append({
                    "entityUrn": product_urn,
                    "entityType": "dataProduct",
                    "aspectName": "dataProductProperties",
                    "aspect": entity_data,
                    "changeType": "UPSERT"
                })
                
        except Exception as e:
            self.logger.error(f"Error creating data product MCPs: {e}")
            # Fallback to basic structure
            mcps.append({
                "operation": "create",
                "entity": entity_data,
                "error": str(e)
            })
        
        return mcps
    
    def _create_ownership_mcps(self, entity_urn: str, owners: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create ownership MCPs."""
        mcps = []
        
        if OwnershipClass and OwnerClass:
            try:
                owner_objects = []
                for owner_data in owners:
                    owner = OwnerClass(
                        owner=owner_data.get("owner", ""),
                        type=OwnershipTypeClass._from_dict({"code": owner_data.get("type", "DATAOWNER")}) if OwnershipTypeClass else "DATAOWNER"
                    )
                    owner_objects.append(owner)
                
                ownership = OwnershipClass(owners=owner_objects)
                
                mcp = MetadataChangeProposalClass(
                    entityUrn=entity_urn,
                    entityType="dataProduct",
                    aspectName="ownership",
                    aspect=ownership,
                    changeType=ChangeTypeClass.UPSERT
                )
                
                mcps.append(mcp.to_obj())
                
            except Exception as e:
                self.logger.error(f"Error creating ownership MCPs: {e}")
                # Fallback
                mcps.append({
                    "entityUrn": entity_urn,
                    "aspectName": "ownership",
                    "aspect": {"owners": owners},
                    "changeType": "UPSERT"
                })
        
        return mcps
    
    def _create_domain_mcps(self, entity_urn: str, domains: List[str]) -> List[Dict[str, Any]]:
        """Create domain MCPs."""
        mcps = []
        
        if DomainsClass:
            try:
                domains_obj = DomainsClass(domains=domains)
                
                mcp = MetadataChangeProposalClass(
                    entityUrn=entity_urn,
                    entityType="dataProduct",
                    aspectName="domains",
                    aspect=domains_obj,
                    changeType=ChangeTypeClass.UPSERT
                )
                
                mcps.append(mcp.to_obj())
                
            except Exception as e:
                self.logger.error(f"Error creating domain MCPs: {e}")
                # Fallback
                mcps.append({
                    "entityUrn": entity_urn,
                    "aspectName": "domains",
                    "aspect": {"domains": domains},
                    "changeType": "UPSERT"
                })
        
        return mcps
    
    def _create_tag_mcps(self, entity_urn: str, tags: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create tag MCPs."""
        mcps = []
        
        if GlobalTagsClass and TagAssociationClass:
            try:
                tag_associations = []
                for tag_data in tags:
                    tag_assoc = TagAssociationClass(
                        tag=tag_data.get("tag", ""),
                        context=tag_data.get("context")
                    )
                    tag_associations.append(tag_assoc)
                
                global_tags = GlobalTagsClass(tags=tag_associations)
                
                mcp = MetadataChangeProposalClass(
                    entityUrn=entity_urn,
                    entityType="dataProduct",
                    aspectName="globalTags",
                    aspect=global_tags,
                    changeType=ChangeTypeClass.UPSERT
                )
                
                mcps.append(mcp.to_obj())
                
            except Exception as e:
                self.logger.error(f"Error creating tag MCPs: {e}")
                # Fallback
                mcps.append({
                    "entityUrn": entity_urn,
                    "aspectName": "globalTags",
                    "aspect": {"tags": tags},
                    "changeType": "UPSERT"
                })
        
        return mcps
    
    def update_entity_mcps(self, entity_urn: str, entity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create MCPs for data product update using proper schema classes."""
        # For updates, add the URN to entity data and use create_entity_mcps
        entity_data["urn"] = entity_urn
        return self.create_entity_mcps(entity_data)
    
    def delete_entity_mcps(self, entity_urn: str) -> List[Dict[str, Any]]:
        """Create MCPs for data product deletion."""
        mcps = []
        
        try:
            if MetadataChangeProposalClass:
                # Create proper deletion MCP by removing all aspects
                aspects_to_remove = ["dataProductProperties", "ownership", "domains", "globalTags"]
                
                for aspect_name in aspects_to_remove:
                    mcp = MetadataChangeProposalClass(
                        entityUrn=entity_urn,
                        entityType="dataProduct",
                        aspectName=aspect_name,
                        aspect=None,
                        changeType=ChangeTypeClass.DELETE if ChangeTypeClass else "DELETE"
                    )
                    
                    mcps.append(mcp.to_obj() if hasattr(mcp, 'to_obj') else {
                        "entityUrn": entity_urn,
                        "entityType": "dataProduct",
                        "aspectName": aspect_name,
                        "aspect": None,
                        "changeType": "DELETE"
                    })
            else:
                # Fallback
                mcps.append({
                    "operation": "delete",
                    "urn": entity_urn,
                    "entityType": "dataProduct"
                })
                
        except Exception as e:
            self.logger.error(f"Error creating deletion MCPs: {e}")
            mcps.append({
                "operation": "delete",
                "urn": entity_urn,
                "error": str(e)
            })
        
        return mcps
    
    def add_owner_mcps(self, entity_urn: str, owner_urn: str, ownership_type: str) -> List[Dict[str, Any]]:
        """Create MCPs for adding owner to data product."""
        return self._create_ownership_mcps(entity_urn, [{"owner": owner_urn, "type": ownership_type}])
    
    def remove_owner_mcps(self, entity_urn: str, owner_urn: str, ownership_type: str) -> List[Dict[str, Any]]:
        """Create MCPs for removing owner from data product."""
        # This would require getting current ownership and removing the specific owner
        # For now, return empty list as this is complex to implement
        return []
    
    def assign_to_entity_mcps(self, entity_urn: str, metadata_urn: str, **kwargs) -> List[Dict[str, Any]]:
        """Create MCPs for assigning metadata (tags/domains) to data product."""
        metadata_type = kwargs.get("metadata_type", "tag")
        
        if metadata_type == "tag":
            return self._create_tag_mcps(entity_urn, [{"tag": metadata_urn}])
        elif metadata_type == "domain":
            return self._create_domain_mcps(entity_urn, [metadata_urn])
        
        return []
    
    def remove_from_entity_mcps(self, entity_urn: str, metadata_urn: str) -> List[Dict[str, Any]]:
        """Create MCPs for removing metadata from data product."""
        # This would require getting current metadata and removing the specific item
        # For now, return empty list as this is complex to implement
        return []
    
    def _generate_entity_urn(self, entity_data: Dict[str, Any]) -> str:
        """Generate data product URN from entity data."""
        product_id = entity_data.get("id", entity_data.get("name", "unknown"))
        return f"urn:li:dataProduct:{product_id}" 