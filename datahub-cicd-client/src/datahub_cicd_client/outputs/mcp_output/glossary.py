"""
Glossary MCP output operations for DataHub CI/CD client.
"""

from typing import Dict, Any, Optional, List
from .base_mcp_output import BaseAsyncOutput, EntityRelationshipAsyncOutput, MetadataAssignmentAsyncOutput
from datahub_cicd_client.core.mcp_emitter import MCPEmitter

# Import DataHub schema classes
try:
    from datahub.metadata.schema_classes import (
        GlossaryNodeKeyClass,
        GlossaryNodeInfoClass,
        GlossaryTermKeyClass,
        GlossaryTermInfoClass,
        OwnershipClass,
        OwnerClass,
        OwnershipTypeClass,
        GlossaryTermsClass,
        GlossaryTermAssociationClass,
        ChangeTypeClass,
        SystemMetadataClass,
        AuditStampClass,
        MetadataChangeProposalClass
    )
except ImportError:
    # Fallback if schema classes not available
    GlossaryNodeKeyClass = None
    GlossaryNodeInfoClass = None
    GlossaryTermKeyClass = None
    GlossaryTermInfoClass = None
    OwnershipClass = None
    OwnerClass = None
    OwnershipTypeClass = None
    GlossaryTermsClass = None
    GlossaryTermAssociationClass = None
    ChangeTypeClass = None
    SystemMetadataClass = None
    AuditStampClass = None
    MetadataChangeProposalClass = None


class GlossaryAsyncOutput(EntityRelationshipAsyncOutput, MetadataAssignmentAsyncOutput):
    """
    MCP output operations for glossary entities using proper DataHub schema classes.
    """
    
    def __init__(self, output_dir: Optional[str] = None):
        """Initialize glossary MCP output."""
        super().__init__(output_dir)
        self._check_schema_classes()
    
    def _check_schema_classes(self):
        """Check if schema classes are available."""
        if not GlossaryNodeKeyClass:
            self.logger.warning("DataHub schema classes not available, using basic dictionaries")
    
    def _create_mcp_emitter(self) -> MCPEmitter:
        """Create glossary-specific MCP emitter."""
        return MCPEmitter(self.output_dir)
    
    def create_entity_mcps(self, entity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create MCPs for glossary entity creation."""
        entity_type = entity_data.get("entity_type", "term")
        
        if entity_type == "node":
            return self.create_glossary_node_mcps(entity_data)
        else:
            return self.create_glossary_term_mcps(entity_data)
    
    def create_glossary_node_mcps(self, entity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create MCPs for glossary node creation using proper schema classes."""
        mcps = []
        
        try:
            node_urn = entity_data.get("urn") or self._generate_node_urn(entity_data)
            
            if GlossaryNodeKeyClass and GlossaryNodeInfoClass:
                # Create proper DataHub MCP using schema classes
                key = GlossaryNodeKeyClass(
                    name=entity_data.get("name", "unknown")
                )
                
                info = GlossaryNodeInfoClass(
                    definition=entity_data.get("description", ""),
                    parentNode=entity_data.get("parentNode")
                )
                
                # Create MCP for node info
                mcp = MetadataChangeProposalClass(
                    entityUrn=node_urn,
                    entityType="glossaryNode",
                    aspectName="glossaryNodeInfo",
                    aspect=info,
                    changeType=ChangeTypeClass.UPSERT
                )
                
                mcps.append(mcp.to_obj())
                
                # Add ownership if provided
                if entity_data.get("owners"):
                    ownership_mcps = self._create_ownership_mcps(node_urn, entity_data["owners"], "glossaryNode")
                    mcps.extend(ownership_mcps)
                    
            else:
                # Fallback to basic dictionary structure
                mcps.append({
                    "entityUrn": node_urn,
                    "entityType": "glossaryNode",
                    "aspectName": "glossaryNodeInfo",
                    "aspect": entity_data,
                    "changeType": "UPSERT"
                })
                
        except Exception as e:
            self.logger.error(f"Error creating glossary node MCPs: {e}")
            # Fallback to basic structure
            mcps.append({
                "operation": "create",
                "entity": entity_data,
                "error": str(e)
            })
        
        return mcps
    
    def create_glossary_term_mcps(self, entity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create MCPs for glossary term creation using proper schema classes."""
        mcps = []
        
        try:
            term_urn = entity_data.get("urn") or self._generate_term_urn(entity_data)
            
            if GlossaryTermKeyClass and GlossaryTermInfoClass:
                # Create proper DataHub MCP using schema classes
                key = GlossaryTermKeyClass(
                    name=entity_data.get("name", "unknown")
                )
                
                info = GlossaryTermInfoClass(
                    definition=entity_data.get("description", ""),
                    parentNode=entity_data.get("parentNode"),
                    termSource=entity_data.get("termSource", "INTERNAL")
                )
                
                # Create MCP for term info
                mcp = MetadataChangeProposalClass(
                    entityUrn=term_urn,
                    entityType="glossaryTerm",
                    aspectName="glossaryTermInfo",
                    aspect=info,
                    changeType=ChangeTypeClass.UPSERT
                )
                
                mcps.append(mcp.to_obj())
                
                # Add ownership if provided
                if entity_data.get("owners"):
                    ownership_mcps = self._create_ownership_mcps(term_urn, entity_data["owners"], "glossaryTerm")
                    mcps.extend(ownership_mcps)
                    
            else:
                # Fallback to basic dictionary structure
                mcps.append({
                    "entityUrn": term_urn,
                    "entityType": "glossaryTerm",
                    "aspectName": "glossaryTermInfo",
                    "aspect": entity_data,
                    "changeType": "UPSERT"
                })
                
        except Exception as e:
            self.logger.error(f"Error creating glossary term MCPs: {e}")
            # Fallback to basic structure
            mcps.append({
                "operation": "create",
                "entity": entity_data,
                "error": str(e)
            })
        
        return mcps
    
    def _create_ownership_mcps(self, entity_urn: str, owners: List[Dict[str, Any]], entity_type: str) -> List[Dict[str, Any]]:
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
                    entityType=entity_type,
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
    
    def update_entity_mcps(self, entity_urn: str, entity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create MCPs for glossary entity update."""
        entity_type = entity_urn.split(":")[2]  # Extract type from URN
        
        if entity_type == "glossaryNode":
            entity_data["entity_type"] = "node"
            entity_data["urn"] = entity_urn
            return self.create_glossary_node_mcps(entity_data)
        elif entity_type == "glossaryTerm":
            entity_data["entity_type"] = "term"
            entity_data["urn"] = entity_urn
            return self.create_glossary_term_mcps(entity_data)
        else:
            return []
    
    def delete_entity_mcps(self, entity_urn: str) -> List[Dict[str, Any]]:
        """Create MCPs for glossary entity deletion."""
        mcps = []
        entity_type = entity_urn.split(":")[2]  # Extract type from URN
        
        try:
            if MetadataChangeProposalClass:
                if entity_type == "glossaryNode":
                    aspects_to_remove = ["glossaryNodeInfo", "ownership"]
                elif entity_type == "glossaryTerm":
                    aspects_to_remove = ["glossaryTermInfo", "ownership"]
                else:
                    aspects_to_remove = []
                
                for aspect_name in aspects_to_remove:
                    mcp = MetadataChangeProposalClass(
                        entityUrn=entity_urn,
                        entityType=entity_type,
                        aspectName=aspect_name,
                        aspect=None,
                        changeType=ChangeTypeClass.DELETE if ChangeTypeClass else "DELETE"
                    )
                    
                    mcps.append(mcp.to_obj() if hasattr(mcp, 'to_obj') else {
                        "entityUrn": entity_urn,
                        "entityType": entity_type,
                        "aspectName": aspect_name,
                        "aspect": None,
                        "changeType": "DELETE"
                    })
            else:
                # Fallback
                mcps.append({
                    "operation": "delete",
                    "urn": entity_urn,
                    "entityType": entity_type
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
        """Create MCPs for adding owner to glossary entity."""
        entity_type = entity_urn.split(":")[2]
        return self._create_ownership_mcps(entity_urn, [{"owner": owner_urn, "type": ownership_type}], entity_type)
    
    def remove_owner_mcps(self, entity_urn: str, owner_urn: str, ownership_type: str) -> List[Dict[str, Any]]:
        """Create MCPs for removing owner from glossary entity."""
        # This would require getting current ownership and removing the specific owner
        # For now, return empty list as this is complex to implement
        return []
    
    def assign_to_entity_mcps(self, entity_urn: str, glossary_term_urn: str, **kwargs) -> List[Dict[str, Any]]:
        """Create MCPs for assigning glossary term to entity."""
        if GlossaryTermsClass and GlossaryTermAssociationClass:
            try:
                term_assoc = GlossaryTermAssociationClass(urn=glossary_term_urn)
                glossary_terms = GlossaryTermsClass(terms=[term_assoc])
                
                mcp = MetadataChangeProposalClass(
                    entityUrn=entity_urn,
                    entityType=kwargs.get("entity_type", "dataset"),
                    aspectName="glossaryTerms",
                    aspect=glossary_terms,
                    changeType=ChangeTypeClass.UPSERT
                )
                
                return [mcp.to_obj()]
                
            except Exception as e:
                self.logger.error(f"Error creating glossary term assignment MCPs: {e}")
        
        # Fallback
        return [{
            "entityUrn": entity_urn,
            "aspectName": "glossaryTerms",
            "aspect": {"terms": [{"urn": glossary_term_urn}]},
            "changeType": "UPSERT"
        }]
    
    def remove_from_entity_mcps(self, entity_urn: str, glossary_term_urn: str) -> List[Dict[str, Any]]:
        """Create MCPs for removing glossary term from entity."""
        # This would require getting current glossary terms and removing the specific term
        # For now, return empty list as this is complex to implement
        return []
    
    def _generate_node_urn(self, entity_data: Dict[str, Any]) -> str:
        """Generate glossary node URN from entity data."""
        node_id = entity_data.get("id", entity_data.get("name", "unknown"))
        return f"urn:li:glossaryNode:{node_id}"
    
    def _generate_term_urn(self, entity_data: Dict[str, Any]) -> str:
        """Generate glossary term URN from entity data."""
        term_id = entity_data.get("id", entity_data.get("name", "unknown"))
        return f"urn:li:glossaryTerm:{term_id}"
    
    def _generate_entity_urn(self, entity_data: Dict[str, Any]) -> str:
        """Generate entity URN from entity data."""
        entity_type = entity_data.get("entity_type", "term")
        if entity_type == "node":
            return self._generate_node_urn(entity_data)
        else:
            return self._generate_term_urn(entity_data)
    
    def create_bulk_glossary_mcps(self, glossary_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create MCPs for multiple glossary entities at once."""
        all_mcps = []
        for entity_data in glossary_data:
            entity_mcps = self.create_entity_mcps(entity_data)
            all_mcps.extend(entity_mcps)
        return all_mcps
    
    def create_glossary_term_assignment_mcps(
        self, 
        glossary_term_urn: str, 
        entity_urns: List[str]
    ) -> List[Dict[str, Any]]:
        """Create MCPs for assigning a glossary term to multiple entities."""
        mcps = []
        for entity_urn in entity_urns:
            entity_mcps = self.assign_to_entity_mcps(entity_urn, glossary_term_urn)
            mcps.extend(entity_mcps)
        return mcps
    
    def create_entity_glossary_term_assignment_mcps(
        self,
        entity_urn: str,
        glossary_term_urns: List[str]
    ) -> List[Dict[str, Any]]:
        """Create MCPs for assigning multiple glossary terms to an entity."""
        terms = [{"urn": term_urn} for term_urn in glossary_term_urns]
        
        glossary_terms_mcp = self.mcp_emitter.create_mcp(
            entity_urn=entity_urn,
            aspect_name="glossaryTerms",
            aspect_value={
                "terms": terms
            }
        )
        return [glossary_terms_mcp] 