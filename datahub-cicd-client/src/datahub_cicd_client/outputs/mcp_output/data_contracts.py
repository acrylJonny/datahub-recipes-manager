"""
Data Contract MCP output operations for DataHub CI/CD client.
"""

from typing import Dict, Any, Optional, List
from .base_mcp_output import BaseAsyncOutput
from datahub_cicd_client.core.mcp_emitter import MCPEmitter

# Import DataHub schema classes
try:
    from datahub.metadata.schema_classes import (
        DataContractKeyClass,
        DataContractPropertiesClass,
        DataContractStateClass,
        ChangeTypeClass,
        SystemMetadataClass,
        AuditStampClass,
        MetadataChangeProposalClass
    )
except ImportError:
    # Fallback if schema classes not available
    DataContractKeyClass = None
    DataContractPropertiesClass = None
    DataContractStateClass = None
    ChangeTypeClass = None
    SystemMetadataClass = None
    AuditStampClass = None
    MetadataChangeProposalClass = None


class DataContractAsyncOutput(BaseAsyncOutput):
    """
    MCP output operations for data contracts using proper DataHub schema classes.
    """
    
    def __init__(self, output_dir: Optional[str] = None):
        """Initialize data contract MCP output."""
        super().__init__(output_dir)
        self._check_schema_classes()
    
    def _check_schema_classes(self):
        """Check if schema classes are available."""
        if not DataContractKeyClass:
            self.logger.warning("DataHub schema classes not available, using basic dictionaries")
    
    def _create_mcp_emitter(self) -> MCPEmitter:
        """Create contract-specific MCP emitter."""
        return MCPEmitter(self.output_dir)
    
    def create_entity_mcps(self, entity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create MCPs for data contract creation using proper schema classes."""
        mcps = []
        
        try:
            contract_urn = entity_data.get("urn") or self._generate_entity_urn(entity_data)
            
            if DataContractKeyClass and DataContractPropertiesClass:
                # Create proper DataHub MCP using schema classes
                key = DataContractKeyClass(
                    id=entity_data.get("id", "unknown")
                )
                
                properties = DataContractPropertiesClass(
                    entityUrn=entity_data.get("entityUrn", ""),
                    freshness=entity_data.get("freshness"),
                    schema=entity_data.get("schema"),
                    dataQuality=entity_data.get("dataQuality")
                )
                
                # Create MCP
                mcp = MetadataChangeProposalClass(
                    entityUrn=contract_urn,
                    entityType="dataContract",
                    aspectName="dataContractProperties",
                    aspect=properties,
                    changeType=ChangeTypeClass.UPSERT
                )
                
                mcps.append(mcp.to_obj())
                
                # Add status if provided
                if entity_data.get("status"):
                    status = DataContractStateClass(
                        state=entity_data["status"].get("state", "ACTIVE")
                    )
                    
                    status_mcp = MetadataChangeProposalClass(
                        entityUrn=contract_urn,
                        entityType="dataContract",
                        aspectName="status",
                        aspect=status,
                        changeType=ChangeTypeClass.UPSERT
                    )
                    
                    mcps.append(status_mcp.to_obj())
            else:
                # Fallback to basic dictionary structure
                mcps.append({
                    "entityUrn": contract_urn,
                    "entityType": "dataContract",
                    "aspectName": "dataContractProperties",
                    "aspect": entity_data,
                    "changeType": "UPSERT"
                })
                
        except Exception as e:
            self.logger.error(f"Error creating data contract MCPs: {e}")
            # Fallback to basic structure
            mcps.append({
                "operation": "create",
                "entity": entity_data,
                "error": str(e)
            })
        
        return mcps
    
    def update_entity_mcps(self, entity_urn: str, entity_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create MCPs for data contract update using proper schema classes."""
        # For updates, add the URN to entity data and use create_entity_mcps
        entity_data["urn"] = entity_urn
        return self.create_entity_mcps(entity_data)
    
    def delete_entity_mcps(self, entity_urn: str) -> List[Dict[str, Any]]:
        """Create MCPs for data contract deletion."""
        mcps = []
        
        try:
            if MetadataChangeProposalClass:
                # Create proper deletion MCP
                mcp = MetadataChangeProposalClass(
                    entityUrn=entity_urn,
                    entityType="dataContract",
                    aspectName="status",
                    aspect=DataContractStateClass(state="REMOVED") if DataContractStateClass else {"state": "REMOVED"},
                    changeType=ChangeTypeClass.UPSERT if ChangeTypeClass else "UPSERT"
                )
                
                mcps.append(mcp.to_obj() if hasattr(mcp, 'to_obj') else {
                    "entityUrn": entity_urn,
                    "entityType": "dataContract",
                    "aspectName": "status",
                    "aspect": {"state": "REMOVED"},
                    "changeType": "UPSERT"
                })
            else:
                # Fallback
                mcps.append({
                    "operation": "delete",
                    "urn": entity_urn,
                    "entityType": "dataContract"
                })
                
        except Exception as e:
            self.logger.error(f"Error creating deletion MCPs: {e}")
            mcps.append({
                "operation": "delete",
                "urn": entity_urn,
                "error": str(e)
            })
        
        return mcps
    
    def _generate_entity_urn(self, entity_data: Dict[str, Any]) -> str:
        """Generate data contract URN from entity data."""
        contract_id = entity_data.get("id", entity_data.get("name", "unknown"))
        return f"urn:li:dataContract:{contract_id}"
