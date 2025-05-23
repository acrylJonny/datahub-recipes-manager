#!/usr/bin/env python3
"""
DataHub metadata API client using ingestion framework.
This module provides utilities for exporting and importing metadata elements
from DataHub using the native ingestion framework and MCPs.
"""

import logging
from typing import Dict, Any, List, Optional, Iterable, Set, Tuple
from dataclasses import dataclass
import json

from datahub.emitter.mce_builder import make_domain_urn, make_tag_urn, make_term_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DomainPropertiesClass,
    DomainsClass,
    DomainClass,
    DomainKeyClass,
    DomainAssociationClass,
    DisplayPropertiesClass,
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass,
    TagPropertiesClass,
    TagKeyClass,
    GlobalTagsClass,
    TagAssociationClass,
    GlossaryTermInfo,
    GlossaryTermKeyClass,
    GlossaryTermPropertiesClass,
    GlossaryNodeInfo,
    GlossaryNodeKeyClass,
    GlossaryNodePropertiesClass,
    ParentNodesClass,
    GlossaryRelationshipClass,
    GlossaryTermsClass,
    GlossaryTermAssociationClass,
)


logger = logging.getLogger(__name__)


class DataHubMetadataApiClient:
    """
    Client for extracting and importing metadata to/from DataHub
    using the native ingestion framework
    """

    def __init__(self, server_url: str, token: Optional[str] = None):
        """
        Initialize the DataHub metadata API client
        
        Args:
            server_url: DataHub GMS server URL
            token: DataHub authentication token (optional)
        """
        self.server_url = server_url
        self.token = token
        self.context = self._create_pipeline_context()

    def _create_pipeline_context(self) -> PipelineContext:
        """
        Create a pipeline context for DataHub ingestion
        
        Returns:
            PipelineContext instance
        """
        from datahub.ingestion.graph.client import DataHubGraph
        
        # Create DataHub graph client
        graph = DataHubGraph(self.server_url, token=self.token)
        
        # Create pipeline context
        return PipelineContext(
            run_id="metadata-api-client",
            graph=graph,
        )

    def list_domains(self) -> List[Dict[str, Any]]:
        """
        List all domains in DataHub
        
        Returns:
            List of domain objects
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return []
        
        try:
            # Use the graph client to search for all domains
            domains = self.context.graph.search_entities(
                entity_types=["domain"], 
                query="*"
            )
            
            result = []
            for domain in domains:
                domain_urn = domain.get("urn")
                if not domain_urn:
                    continue
                
                # Get full domain info
                domain_info = self.export_domain(domain_urn)
                if domain_info:
                    result.append(domain_info)
            
            return result
        except Exception as e:
            logger.error(f"Error listing domains: {str(e)}")
            return []

    def export_domain(self, domain_urn: str, include_entities: bool = False) -> Optional[Dict[str, Any]]:
        """
        Export a domain with its properties
        
        Args:
            domain_urn: Domain URN
            include_entities: Whether to include entities belonging to the domain
            
        Returns:
            Dictionary with domain data
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return None
        
        try:
            # Get the domain entity
            domain_entity = self.context.graph.get_entity(domain_urn)
            if not domain_entity:
                logger.error(f"Domain not found: {domain_urn}")
                return None
            
            # Convert to dictionary
            result = {
                "urn": domain_urn,
                "type": "DOMAIN",
            }
            
            # Extract properties
            if "properties" in domain_entity:
                result["properties"] = domain_entity["properties"]
            
            # Extract display properties
            if "displayProperties" in domain_entity:
                result["displayProperties"] = domain_entity["displayProperties"]
            
            # Extract ownership
            if "ownership" in domain_entity:
                result["ownership"] = domain_entity["ownership"]
            
            # Extract parent domains
            if "parentDomains" in domain_entity:
                result["parentDomains"] = domain_entity["parentDomains"]
            
            # Include entities if requested
            if include_entities:
                try:
                    # Search for entities associated with this domain
                    domain_id = domain_urn.split(":")[-1]
                    entities = self.context.graph.search_across_entities(
                        query=f"domains:{domain_id}",
                        start=0,
                        count=1000  # Reasonable limit
                    )
                    
                    result["entities"] = [{"urn": entity["urn"], "type": entity["entityType"]} 
                                         for entity in entities.get("searchResults", [])]
                except Exception as e:
                    logger.error(f"Error getting domain entities: {str(e)}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error exporting domain: {str(e)}")
            return None

    def create_domain_workunit(self, domain_data: Dict[str, Any]) -> Optional[MetadataWorkUnit]:
        """
        Create a MetadataWorkUnit for domain creation/update
        
        Args:
            domain_data: Domain data
            
        Returns:
            MetadataWorkUnit if successful, None otherwise
        """
        try:
            # Get required properties
            properties = domain_data.get("properties", {})
            name = properties.get("name")
            description = properties.get("description", "")
            
            if not name:
                logger.error("Domain name is required")
                return None
            
            # Create domain URN - either use existing or create from name
            if "urn" in domain_data:
                domain_urn = domain_data["urn"]
            else:
                # Convert name to ID by replacing spaces with underscores and making lowercase
                domain_id = name.lower().replace(" ", "_")
                domain_urn = make_domain_urn(domain_id)
            
            # Create domain properties aspect
            domain_properties = DomainPropertiesClass(
                name=name,
                description=description
            )
            
            # Create MCP for domain properties
            mcp_properties = MetadataChangeProposalWrapper(
                entityUrn=domain_urn,
                aspect=domain_properties,
                changeType=ChangeTypeClass.UPSERT,
            )
            
            # Create workunit for domain properties
            workunit = MetadataWorkUnit(
                id=f"{domain_urn}-properties",
                mcp=mcp_properties,
            )
            
            # Add display properties if present
            if "displayProperties" in domain_data:
                display_props = domain_data["displayProperties"]
                color_hex = display_props.get("colorHex")
                icon = display_props.get("icon", {})
                
                display_properties = DisplayPropertiesClass(
                    colorHex=color_hex,
                    icon=icon
                )
                
                mcp_display = MetadataChangeProposalWrapper(
                    entityUrn=domain_urn,
                    aspect=display_properties,
                    changeType=ChangeTypeClass.UPSERT,
                )
                
                # Add display properties to the same workunit
                workunit.metadata.mcp2.append(mcp_display)
            
            # Add parent domains if present
            if "parentDomains" in domain_data and "domains" in domain_data["parentDomains"]:
                parent_domains = []
                for parent in domain_data["parentDomains"]["domains"]:
                    if "urn" in parent:
                        parent_domains.append(parent["urn"])
                
                if parent_domains:
                    # Create domain association aspect
                    domains_aspect = DomainsClass(
                        domains=[DomainClass(urn=parent) for parent in parent_domains]
                    )
                    
                    mcp_parents = MetadataChangeProposalWrapper(
                        entityUrn=domain_urn,
                        aspect=domains_aspect,
                        changeType=ChangeTypeClass.UPSERT,
                    )
                    
                    # Add parent domains to the same workunit
                    workunit.metadata.mcp2.append(mcp_parents)
            
            # Add ownership if present
            if "ownership" in domain_data and "owners" in domain_data["ownership"]:
                owners = []
                for owner_data in domain_data["ownership"]["owners"]:
                    if "owner" not in owner_data or "urn" not in owner_data["owner"]:
                        continue
                    
                    owner_urn = owner_data["owner"]["urn"]
                    owner_type = owner_data.get("type", "UNKNOWN")
                    
                    ownership_type_urn = "urn:li:ownershipType:Technical"
                    if "ownershipType" in owner_data and "urn" in owner_data["ownershipType"]:
                        ownership_type_urn = owner_data["ownershipType"]["urn"]
                    
                    owners.append(
                        OwnerClass(
                            owner=owner_urn,
                            type=owner_type,
                            ownershipType=OwnershipTypeClass(ownership_type_urn)
                        )
                    )
                
                if owners:
                    ownership = OwnershipClass(owners=owners)
                    
                    mcp_ownership = MetadataChangeProposalWrapper(
                        entityUrn=domain_urn,
                        aspect=ownership,
                        changeType=ChangeTypeClass.UPSERT,
                    )
                    
                    # Add ownership to the same workunit
                    workunit.metadata.mcp2.append(mcp_ownership)
            
            return workunit
            
        except Exception as e:
            logger.error(f"Error creating domain workunit: {str(e)}")
            return None
    
    def create_domain_association_workunit(self, entity_urn: str, domain_urn: str) -> Optional[MetadataWorkUnit]:
        """
        Create a MetadataWorkUnit for associating an entity with a domain
        
        Args:
            entity_urn: URN of the entity to associate
            domain_urn: URN of the domain
            
        Returns:
            MetadataWorkUnit if successful, None otherwise
        """
        try:
            # Create domain association aspect
            domain_association = DomainAssociationClass(
                domains=[
                    DomainClass(urn=domain_urn)
                ]
            )
            
            # Create MCP for domain association
            mcp = MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=domain_association,
                changeType=ChangeTypeClass.UPSERT,
            )
            
            # Create workunit for domain association
            workunit = MetadataWorkUnit(
                id=f"{entity_urn}-{domain_urn}-association",
                mcp=mcp,
            )
            
            return workunit
            
        except Exception as e:
            logger.error(f"Error creating domain association workunit: {str(e)}")
            return None

    def delete_domain_workunit(self, domain_urn: str) -> Optional[MetadataWorkUnit]:
        """
        Create a MetadataWorkUnit for domain deletion
        
        Args:
            domain_urn: URN of the domain to delete
            
        Returns:
            MetadataWorkUnit if successful, None otherwise
        """
        try:
            # Create key aspect for the domain
            domain_key = DomainKeyClass()
            
            # Create MCP for domain deletion
            mcp = MetadataChangeProposalWrapper(
                entityUrn=domain_urn,
                aspect=domain_key,
                changeType=ChangeTypeClass.DELETE,
            )
            
            # Create workunit for domain deletion
            workunit = MetadataWorkUnit(
                id=f"{domain_urn}-delete",
                mcp=mcp,
            )
            
            return workunit
            
        except Exception as e:
            logger.error(f"Error creating domain deletion workunit: {str(e)}")
            return None

    def emit_workunit(self, workunit: MetadataWorkUnit) -> bool:
        """
        Emit a MetadataWorkUnit to DataHub
        
        Args:
            workunit: MetadataWorkUnit to emit
            
        Returns:
            True if successful, False otherwise
        """
        from datahub.ingestion.sink.datahub_rest import DatahubRestSink
        
        try:
            # Create DataHub REST sink
            sink_config = {
                "server": self.server_url,
                "token": self.token,
                "disable_flush_on_flush": False,
            }
            
            # Initialize the sink
            sink = DatahubRestSink.create(sink_config, self.context)
            
            # Emit the workunit
            sink.write_workunit(workunit)
            
            # Flush the sink to ensure the workunit is sent
            sink.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Error emitting workunit: {str(e)}")
            return False

    def import_domain(self, domain_data: Dict[str, Any]) -> Optional[str]:
        """
        Import a domain from data
        
        Args:
            domain_data: Domain data
            
        Returns:
            Domain URN if successful, None otherwise
        """
        try:
            # Create workunit for domain
            workunit = self.create_domain_workunit(domain_data)
            if not workunit:
                logger.error("Failed to create domain workunit")
                return None
            
            # Emit the workunit
            success = self.emit_workunit(workunit)
            if not success:
                logger.error("Failed to emit domain workunit")
                return None
            
            # Get the domain URN from the workunit
            domain_urn = workunit.metadata.mcp.entityUrn
            
            # Return the URN as a string
            return str(domain_urn)
            
        except Exception as e:
            logger.error(f"Error importing domain: {str(e)}")
            return None

    def update_domain(self, domain_urn: str, domain_data: Dict[str, Any]) -> bool:
        """
        Update an existing domain
        
        Args:
            domain_urn: URN of the domain to update
            domain_data: Updated domain data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure the URN in the data matches the provided URN
            domain_data["urn"] = domain_urn
            
            # Create workunit for domain update
            workunit = self.create_domain_workunit(domain_data)
            if not workunit:
                logger.error("Failed to create domain update workunit")
                return False
            
            # Emit the workunit
            success = self.emit_workunit(workunit)
            if not success:
                logger.error("Failed to emit domain update workunit")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating domain: {str(e)}")
            return False

    def delete_domain(self, domain_urn: str, force: bool = False) -> bool:
        """
        Delete a domain
        
        Args:
            domain_urn: URN of the domain to delete
            force: Whether to force delete even if domain has children or entity associations
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check for dependencies if not forcing
            if not force:
                has_children, has_entities = self.check_domain_dependencies(domain_urn)
                
                if has_children:
                    logger.error(f"Domain {domain_urn} has child domains. Use force=True to delete anyway.")
                    return False
                
                if has_entities:
                    logger.error(f"Domain {domain_urn} has associated entities. Use force=True to delete anyway.")
                    return False
            
            # Create workunit for domain deletion
            workunit = self.delete_domain_workunit(domain_urn)
            if not workunit:
                logger.error("Failed to create domain deletion workunit")
                return False
            
            # Emit the workunit
            success = self.emit_workunit(workunit)
            if not success:
                logger.error("Failed to emit domain deletion workunit")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error deleting domain: {str(e)}")
            return False

    def check_domain_dependencies(self, domain_urn: str) -> Tuple[bool, bool]:
        """
        Check if a domain has child domains or associated entities
        
        Args:
            domain_urn: URN of the domain to check
            
        Returns:
            Tuple of (has_children, has_entities)
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return False, False
        
        try:
            # Get all domains
            all_domains = self.list_domains()
            
            # Check for child domains
            has_children = False
            for domain in all_domains:
                if "parentDomains" in domain and "domains" in domain["parentDomains"]:
                    parent_urns = [parent.get("urn") for parent in domain["parentDomains"]["domains"]]
                    if domain_urn in parent_urns:
                        has_children = True
                        break
            
            # Check for associated entities
            domain_with_entities = self.export_domain(domain_urn, include_entities=True)
            has_entities = domain_with_entities is not None and "entities" in domain_with_entities and len(domain_with_entities["entities"]) > 0
            
            return has_children, has_entities
            
        except Exception as e:
            logger.error(f"Error checking domain dependencies: {str(e)}")
            return False, False

    def list_tags(self) -> List[Dict[str, Any]]:
        """
        List all tags in DataHub
        
        Returns:
            List of tag objects
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return []
        
        try:
            # Use the graph client to search for all tags
            tags = self.context.graph.search_entities(
                entity_types=["tag"], 
                query="*"
            )
            
            result = []
            for tag in tags:
                tag_urn = tag.get("urn")
                if not tag_urn:
                    continue
                
                # Get full tag info
                tag_info = self.export_tag(tag_urn)
                if tag_info:
                    result.append(tag_info)
            
            return result
        except Exception as e:
            logger.error(f"Error listing tags: {str(e)}")
            return []

    def export_tag(self, tag_urn: str, include_entities: bool = False) -> Optional[Dict[str, Any]]:
        """
        Export a tag with its properties
        
        Args:
            tag_urn: Tag URN
            include_entities: Whether to include entities tagged with this tag
            
        Returns:
            Dictionary with tag data
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return None
        
        try:
            # Get the tag entity
            tag_entity = self.context.graph.get_entity(tag_urn)
            if not tag_entity:
                logger.error(f"Tag not found: {tag_urn}")
                return None
            
            # Convert to dictionary
            result = {
                "urn": tag_urn,
                "type": "TAG",
            }
            
            # Extract properties
            if "properties" in tag_entity:
                result["properties"] = tag_entity["properties"]
            
            # Extract name (could be in properties or directly in entity)
            if "name" in tag_entity:
                result["name"] = tag_entity["name"]
            
            # Extract description
            if "description" in tag_entity:
                result["description"] = tag_entity["description"]
            
            # Include entities if requested
            if include_entities:
                try:
                    # Extract tag name from URN or properties
                    tag_name = tag_urn.split(":")[-1]
                    if "name" in result:
                        tag_name = result["name"]
                    elif "properties" in result and "name" in result["properties"]:
                        tag_name = result["properties"]["name"]
                    
                    # Search for entities with this tag
                    entities = self.context.graph.search_across_entities(
                        query=f"tags:{tag_name}",
                        start=0,
                        count=1000  # Reasonable limit
                    )
                    
                    result["entities"] = [{"urn": entity["urn"], "type": entity["entityType"]} 
                                         for entity in entities.get("searchResults", [])]
                except Exception as e:
                    logger.error(f"Error getting tagged entities: {str(e)}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error exporting tag: {str(e)}")
            return None

    def create_tag_workunit(self, tag_data: Dict[str, Any]) -> Optional[MetadataWorkUnit]:
        """
        Create a MetadataWorkUnit for tag creation/update
        
        Args:
            tag_data: Tag data
            
        Returns:
            MetadataWorkUnit if successful, None otherwise
        """
        try:
            # Get required properties
            properties = tag_data.get("properties", {})
            name = properties.get("name")
            if not name and "name" in tag_data:
                name = tag_data["name"]
            
            description = properties.get("description", "")
            if not description and "description" in tag_data:
                description = tag_data["description"]
            
            color_hex = properties.get("colorHex", "#0072b1")  # Default LinkedIn blue
            
            if not name:
                logger.error("Tag name is required")
                return None
            
            # Create tag URN - either use existing or create from name
            if "urn" in tag_data:
                tag_urn = tag_data["urn"]
            else:
                # Convert name to ID by replacing spaces with underscores and making lowercase
                tag_id = name.lower().replace(" ", "_")
                tag_urn = make_tag_urn(tag_id)
            
            # Create tag properties aspect
            tag_properties = TagPropertiesClass(
                name=name,
                description=description,
                colorHex=color_hex
            )
            
            # Create MCP for tag properties
            mcp_properties = MetadataChangeProposalWrapper(
                entityUrn=tag_urn,
                aspect=tag_properties,
                changeType=ChangeTypeClass.UPSERT,
            )
            
            # Create workunit for tag properties
            workunit = MetadataWorkUnit(
                id=f"{tag_urn}-properties",
                mcp=mcp_properties,
            )
            
            return workunit
            
        except Exception as e:
            logger.error(f"Error creating tag workunit: {str(e)}")
            return None

    def create_tag_association_workunit(self, entity_urn: str, tag_urn: str) -> Optional[MetadataWorkUnit]:
        """
        Create a MetadataWorkUnit for associating an entity with a tag
        
        Args:
            entity_urn: URN of the entity to associate
            tag_urn: URN of the tag
            
        Returns:
            MetadataWorkUnit if successful, None otherwise
        """
        try:
            # Create tag association
            tag_association = TagAssociationClass(
                tag=tag_urn
            )
            
            # Get existing tags if any
            entity = None
            existing_tags = []
            try:
                if self.context.graph:
                    entity = self.context.graph.get_entity(entity_urn)
                    if entity and "globalTags" in entity and "tags" in entity["globalTags"]:
                        existing_tags = entity["globalTags"]["tags"]
            except Exception:
                pass  # If we can't get existing tags, we'll just add the new one
            
            # Check if tag already exists
            for existing_tag in existing_tags:
                if existing_tag.get("tag") == tag_urn:
                    logger.info(f"Tag {tag_urn} already associated with {entity_urn}")
                    return None
            
            # Add the new tag to existing ones
            tags_aspect = GlobalTagsClass(
                tags=existing_tags + [tag_association]
            )
            
            # Create MCP for tag association
            mcp = MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=tags_aspect,
                changeType=ChangeTypeClass.UPSERT,
            )
            
            # Create workunit for tag association
            workunit = MetadataWorkUnit(
                id=f"{entity_urn}-{tag_urn}-association",
                mcp=mcp,
            )
            
            return workunit
            
        except Exception as e:
            logger.error(f"Error creating tag association workunit: {str(e)}")
            return None

    def delete_tag_workunit(self, tag_urn: str) -> Optional[MetadataWorkUnit]:
        """
        Create a MetadataWorkUnit for tag deletion
        
        Args:
            tag_urn: URN of the tag to delete
            
        Returns:
            MetadataWorkUnit if successful, None otherwise
        """
        try:
            # Create key aspect for the tag
            tag_key = TagKeyClass()
            
            # Create MCP for tag deletion
            mcp = MetadataChangeProposalWrapper(
                entityUrn=tag_urn,
                aspect=tag_key,
                changeType=ChangeTypeClass.DELETE,
            )
            
            # Create workunit for tag deletion
            workunit = MetadataWorkUnit(
                id=f"{tag_urn}-delete",
                mcp=mcp,
            )
            
            return workunit
            
        except Exception as e:
            logger.error(f"Error creating tag deletion workunit: {str(e)}")
            return None

    def import_tag(self, tag_data: Dict[str, Any]) -> Optional[str]:
        """
        Import a tag from data
        
        Args:
            tag_data: Tag data
            
        Returns:
            Tag URN if successful, None otherwise
        """
        try:
            # Create workunit for tag
            workunit = self.create_tag_workunit(tag_data)
            if not workunit:
                logger.error("Failed to create tag workunit")
                return None
            
            # Emit the workunit
            success = self.emit_workunit(workunit)
            if not success:
                logger.error("Failed to emit tag workunit")
                return None
            
            # Get the tag URN from the workunit
            tag_urn = workunit.metadata.mcp.entityUrn
            
            # Return the URN as a string
            return str(tag_urn)
            
        except Exception as e:
            logger.error(f"Error importing tag: {str(e)}")
            return None

    def update_tag(self, tag_urn: str, tag_data: Dict[str, Any]) -> bool:
        """
        Update an existing tag
        
        Args:
            tag_urn: URN of the tag to update
            tag_data: Updated tag data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure the URN in the data matches the provided URN
            tag_data["urn"] = tag_urn
            
            # Create workunit for tag update
            workunit = self.create_tag_workunit(tag_data)
            if not workunit:
                logger.error("Failed to create tag update workunit")
                return False
            
            # Emit the workunit
            success = self.emit_workunit(workunit)
            if not success:
                logger.error("Failed to emit tag update workunit")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating tag: {str(e)}")
            return False

    def delete_tag(self, tag_urn: str, force: bool = False) -> bool:
        """
        Delete a tag
        
        Args:
            tag_urn: URN of the tag to delete
            force: Whether to force delete even if tag has entity associations
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check for entities using this tag if not forcing
            if not force:
                tag_with_entities = self.export_tag(tag_urn, include_entities=True)
                has_entities = tag_with_entities is not None and "entities" in tag_with_entities and len(tag_with_entities["entities"]) > 0
                
                if has_entities:
                    logger.error(f"Tag {tag_urn} is associated with entities. Use force=True to delete anyway.")
                    return False
            
            # Create workunit for tag deletion
            workunit = self.delete_tag_workunit(tag_urn)
            if not workunit:
                logger.error("Failed to create tag deletion workunit")
                return False
            
            # Emit the workunit
            success = self.emit_workunit(workunit)
            if not success:
                logger.error("Failed to emit tag deletion workunit")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error deleting tag: {str(e)}")
            return False
            
    # ============================
    # Glossary methods
    # ============================
    
    def list_glossary_nodes(self) -> List[Dict[str, Any]]:
        """
        List all glossary nodes in DataHub
        
        Returns:
            List of glossary node objects
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return []
        
        try:
            # Use the graph client to search for all glossary nodes
            nodes = self.context.graph.search_entities(
                entity_types=["glossaryNode"], 
                query="*"
            )
            
            result = []
            for node in nodes:
                node_urn = node.get("urn")
                if not node_urn:
                    continue
                
                # Get full node info
                node_info = self.export_glossary_node(node_urn)
                if node_info:
                    result.append(node_info)
            
            return result
        except Exception as e:
            logger.error(f"Error listing glossary nodes: {str(e)}")
            return []
    
    def list_root_glossary_nodes(self) -> List[Dict[str, Any]]:
        """
        List all root glossary nodes in DataHub
        
        Returns:
            List of root glossary node objects
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return []
        
        try:
            # Get all nodes
            all_nodes = self.list_glossary_nodes()
            
            # Filter for root nodes (those without parent nodes)
            root_nodes = []
            for node in all_nodes:
                parent_nodes = node.get("parentNodes", {}).get("nodes", [])
                if not parent_nodes:
                    root_nodes.append(node)
            
            return root_nodes
        except Exception as e:
            logger.error(f"Error listing root glossary nodes: {str(e)}")
            return []
    
    def export_glossary_node(self, node_urn: str, include_children: bool = False) -> Optional[Dict[str, Any]]:
        """
        Export a glossary node with its properties
        
        Args:
            node_urn: Glossary node URN
            include_children: Whether to include child nodes and terms
            
        Returns:
            Dictionary with glossary node data
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return None
        
        try:
            # Get the glossary node entity
            node_entity = self.context.graph.get_entity(node_urn)
            if not node_entity:
                logger.error(f"Glossary node not found: {node_urn}")
                return None
            
            # Convert to dictionary
            result = {
                "urn": node_urn,
                "type": "GLOSSARY_NODE",
            }
            
            # Extract properties
            if "properties" in node_entity:
                result["properties"] = node_entity["properties"]
            
            # Extract display properties
            if "displayProperties" in node_entity:
                result["displayProperties"] = node_entity["displayProperties"]
            
            # Extract ownership
            if "ownership" in node_entity:
                result["ownership"] = node_entity["ownership"]
            
            # Extract parent nodes
            if "parentNodes" in node_entity:
                result["parentNodes"] = node_entity["parentNodes"]
            
            # Include children if requested
            if include_children and "children" in node_entity:
                result["children"] = node_entity["children"]
            
            return result
            
        except Exception as e:
            logger.error(f"Error exporting glossary node: {str(e)}")
            return None
    
    def create_glossary_node_workunit(self, node_data: Dict[str, Any]) -> Optional[MetadataWorkUnit]:
        """
        Create a MetadataWorkUnit for glossary node creation/update
        
        Args:
            node_data: Glossary node data
            
        Returns:
            MetadataWorkUnit if successful, None otherwise
        """
        try:
            # Get required properties
            properties = node_data.get("properties", {})
            name = properties.get("name")
            description = properties.get("description", "")
            
            if not name:
                logger.error("Glossary node name is required")
                return None
            
            # Create node URN - either use existing or create from name
            if "urn" in node_data:
                node_urn = node_data["urn"]
            else:
                # For new nodes, use the last part of a DataHub-generated URN
                # This is a simplification - in practice, DataHub generates unique IDs
                node_id = f"glossaryNode_{name.lower().replace(' ', '_')}"
                node_urn = f"urn:li:glossaryNode:{node_id}"
            
            # Create node properties aspect
            node_properties = GlossaryNodePropertiesClass(
                name=name,
                description=description
            )
            
            # Create MCP for node properties
            mcp_properties = MetadataChangeProposalWrapper(
                entityUrn=node_urn,
                aspect=node_properties,
                changeType=ChangeTypeClass.UPSERT,
            )
            
            # Create workunit for node properties
            workunit = MetadataWorkUnit(
                id=f"{node_urn}-properties",
                mcp=mcp_properties,
            )
            
            # Add display properties if present
            if "displayProperties" in node_data:
                display_props = node_data["displayProperties"]
                color_hex = display_props.get("colorHex")
                icon = display_props.get("icon", {})
                
                display_properties = DisplayPropertiesClass(
                    colorHex=color_hex,
                    icon=icon
                )
                
                mcp_display = MetadataChangeProposalWrapper(
                    entityUrn=node_urn,
                    aspect=display_properties,
                    changeType=ChangeTypeClass.UPSERT,
                )
                
                # Add display properties to the same workunit
                workunit.metadata.mcp2.append(mcp_display)
            
            # Add parent nodes if present
            if "parentNodes" in node_data and "nodes" in node_data["parentNodes"]:
                parent_nodes = []
                for parent in node_data["parentNodes"]["nodes"]:
                    if "urn" in parent:
                        parent_nodes.append(parent["urn"])
                
                if parent_nodes:
                    # Create parent nodes aspect
                    parent_nodes_aspect = ParentNodesClass(
                        nodes=[GlossaryNodeInfo(urn=parent) for parent in parent_nodes]
                    )
                    
                    mcp_parents = MetadataChangeProposalWrapper(
                        entityUrn=node_urn,
                        aspect=parent_nodes_aspect,
                        changeType=ChangeTypeClass.UPSERT,
                    )
                    
                    # Add parent nodes to the same workunit
                    workunit.metadata.mcp2.append(mcp_parents)
            
            # Add ownership if present
            if "ownership" in node_data and "owners" in node_data["ownership"]:
                owners = []
                for owner_data in node_data["ownership"]["owners"]:
                    if "owner" not in owner_data or "urn" not in owner_data["owner"]:
                        continue
                    
                    owner_urn = owner_data["owner"]["urn"]
                    owner_type = owner_data.get("type", "UNKNOWN")
                    
                    ownership_type_urn = "urn:li:ownershipType:Technical"
                    if "ownershipType" in owner_data and "urn" in owner_data["ownershipType"]:
                        ownership_type_urn = owner_data["ownershipType"]["urn"]
                    
                    owners.append(
                        OwnerClass(
                            owner=owner_urn,
                            type=owner_type,
                            ownershipType=OwnershipTypeClass(ownership_type_urn)
                        )
                    )
                
                if owners:
                    ownership = OwnershipClass(owners=owners)
                    
                    mcp_ownership = MetadataChangeProposalWrapper(
                        entityUrn=node_urn,
                        aspect=ownership,
                        changeType=ChangeTypeClass.UPSERT,
                    )
                    
                    # Add ownership to the same workunit
                    workunit.metadata.mcp2.append(mcp_ownership)
            
            return workunit
            
        except Exception as e:
            logger.error(f"Error creating glossary node workunit: {str(e)}")
            return None
    
    def delete_glossary_node_workunit(self, node_urn: str) -> Optional[MetadataWorkUnit]:
        """
        Create a MetadataWorkUnit for glossary node deletion
        
        Args:
            node_urn: URN of the glossary node to delete
            
        Returns:
            MetadataWorkUnit if successful, None otherwise
        """
        try:
            # Create key aspect for the glossary node
            node_key = GlossaryNodeKeyClass()
            
            # Create MCP for glossary node deletion
            mcp = MetadataChangeProposalWrapper(
                entityUrn=node_urn,
                aspect=node_key,
                changeType=ChangeTypeClass.DELETE,
            )
            
            # Create workunit for glossary node deletion
            workunit = MetadataWorkUnit(
                id=f"{node_urn}-delete",
                mcp=mcp,
            )
            
            return workunit
            
        except Exception as e:
            logger.error(f"Error creating glossary node deletion workunit: {str(e)}")
            return None
    
    def import_glossary_node(self, node_data: Dict[str, Any]) -> Optional[str]:
        """
        Import a glossary node from data
        
        Args:
            node_data: Glossary node data
            
        Returns:
            Glossary node URN if successful, None otherwise
        """
        try:
            # Create workunit for glossary node
            workunit = self.create_glossary_node_workunit(node_data)
            if not workunit:
                logger.error("Failed to create glossary node workunit")
                return None
            
            # Emit the workunit
            success = self.emit_workunit(workunit)
            if not success:
                logger.error("Failed to emit glossary node workunit")
                return None
            
            # Get the glossary node URN from the workunit
            node_urn = workunit.metadata.mcp.entityUrn
            
            # Return the URN as a string
            return str(node_urn)
            
        except Exception as e:
            logger.error(f"Error importing glossary node: {str(e)}")
            return None
    
    def update_glossary_node(self, node_urn: str, node_data: Dict[str, Any]) -> bool:
        """
        Update an existing glossary node
        
        Args:
            node_urn: URN of the glossary node to update
            node_data: Updated glossary node data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure the URN in the data matches the provided URN
            node_data["urn"] = node_urn
            
            # Create workunit for glossary node update
            workunit = self.create_glossary_node_workunit(node_data)
            if not workunit:
                logger.error("Failed to create glossary node update workunit")
                return False
            
            # Emit the workunit
            success = self.emit_workunit(workunit)
            if not success:
                logger.error("Failed to emit glossary node update workunit")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating glossary node: {str(e)}")
            return False
    
    def delete_glossary_node(self, node_urn: str, force: bool = False) -> bool:
        """
        Delete a glossary node
        
        Args:
            node_urn: URN of the glossary node to delete
            force: Whether to force delete even if node has children
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check for child nodes and terms if not forcing
            if not force:
                node_with_children = self.export_glossary_node(node_urn, include_children=True)
                has_children = node_with_children is not None and "children" in node_with_children and len(node_with_children["children"].get("relationships", [])) > 0
                
                if has_children:
                    logger.error(f"Glossary node {node_urn} has child nodes or terms. Use force=True to delete anyway.")
                    return False
            
            # Create workunit for glossary node deletion
            workunit = self.delete_glossary_node_workunit(node_urn)
            if not workunit:
                logger.error("Failed to create glossary node deletion workunit")
                return False
            
            # Emit the workunit
            success = self.emit_workunit(workunit)
            if not success:
                logger.error("Failed to emit glossary node deletion workunit")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error deleting glossary node: {str(e)}")
            return False

    def list_glossary_terms(self) -> List[Dict[str, Any]]:
        """
        List all glossary terms in DataHub
        
        Returns:
            List of glossary term objects
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return []
        
        try:
            # Use the graph client to search for all glossary terms
            terms = self.context.graph.search_entities(
                entity_types=["glossaryTerm"], 
                query="*"
            )
            
            result = []
            for term in terms:
                term_urn = term.get("urn")
                if not term_urn:
                    continue
                
                # Get full term info
                term_info = self.export_glossary_term(term_urn)
                if term_info:
                    result.append(term_info)
            
            return result
        except Exception as e:
            logger.error(f"Error listing glossary terms: {str(e)}")
            return []
    
    def list_root_glossary_terms(self) -> List[Dict[str, Any]]:
        """
        List all root glossary terms in DataHub (terms without parent nodes)
        
        Returns:
            List of root glossary term objects
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return []
        
        try:
            # Get all terms
            all_terms = self.list_glossary_terms()
            
            # Filter for root terms (those without parent nodes)
            root_terms = []
            for term in all_terms:
                parent_nodes = term.get("parentNodes", {}).get("nodes", [])
                if not parent_nodes:
                    root_terms.append(term)
            
            return root_terms
        except Exception as e:
            logger.error(f"Error listing root glossary terms: {str(e)}")
            return []
    
    def export_glossary_term(self, term_urn: str, include_related: bool = False) -> Optional[Dict[str, Any]]:
        """
        Export a glossary term with its properties
        
        Args:
            term_urn: Glossary term URN
            include_related: Whether to include related entities (entities tagged with this term)
            
        Returns:
            Dictionary with glossary term data
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return None
        
        try:
            # Get the glossary term entity
            term_entity = self.context.graph.get_entity(term_urn)
            if not term_entity:
                logger.error(f"Glossary term not found: {term_urn}")
                return None
            
            # Convert to dictionary
            result = {
                "urn": term_urn,
                "type": "GLOSSARY_TERM",
            }
            
            # Extract properties
            if "properties" in term_entity:
                result["properties"] = term_entity["properties"]
            
            # Extract name and hierarchicalName
            if "name" in term_entity:
                result["name"] = term_entity["name"]
            
            if "hierarchicalName" in term_entity:
                result["hierarchicalName"] = term_entity["hierarchicalName"]
            
            # Extract ownership
            if "ownership" in term_entity:
                result["ownership"] = term_entity["ownership"]
            
            # Extract parent nodes
            if "parentNodes" in term_entity:
                result["parentNodes"] = term_entity["parentNodes"]
            
            # Include related entities if requested
            if include_related:
                try:
                    # Extract term name from URN or properties
                    term_name = term_urn.split(":")[-1]
                    if "name" in result:
                        term_name = result["name"]
                    elif "properties" in result and "name" in result["properties"]:
                        term_name = result["properties"]["name"]
                    
                    # Search for entities with this term
                    entities = self.context.graph.search_across_entities(
                        query=f"glossaryTerms:{term_name}",
                        start=0,
                        count=1000  # Reasonable limit
                    )
                    
                    result["relatedEntities"] = [{"urn": entity["urn"], "type": entity["entityType"]} 
                                               for entity in entities.get("searchResults", [])]
                except Exception as e:
                    logger.error(f"Error getting related entities: {str(e)}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error exporting glossary term: {str(e)}")
            return None

    def create_glossary_term_workunit(self, term_data: Dict[str, Any]) -> Optional[MetadataWorkUnit]:
        """
        Create a MetadataWorkUnit for glossary term creation/update
        
        Args:
            term_data: Glossary term data
            
        Returns:
            MetadataWorkUnit if successful, None otherwise
        """
        try:
            # Get required properties
            properties = term_data.get("properties", {})
            name = properties.get("name")
            if not name and "name" in term_data:
                name = term_data["name"]
                
            description = properties.get("description", "")
            if not description and "description" in term_data:
                description = term_data["description"]
                
            definition = properties.get("definition", "")
            term_source = properties.get("termSource", "")
            source_ref = properties.get("sourceRef", "")
            source_url = properties.get("sourceUrl", "")
            
            if not name:
                logger.error("Glossary term name is required")
                return None
            
            # Create term URN - either use existing or create from name
            if "urn" in term_data:
                term_urn = term_data["urn"]
            else:
                # For new terms, we can use the make_term_urn helper
                # This assumes the term belongs to a default glossary, which might need adjustment
                term_id = name.lower().replace(" ", "_")
                term_urn = make_term_urn(term_id)
            
            # Custom properties if any
            custom_properties = properties.get("customProperties", [])
            
            # Create term properties aspect
            term_properties = GlossaryTermPropertiesClass(
                name=name,
                description=description,
                definition=definition,
                termSource=term_source,
                sourceRef=source_ref,
                sourceUrl=source_url,
                customProperties=custom_properties
            )
            
            # Create MCP for term properties
            mcp_properties = MetadataChangeProposalWrapper(
                entityUrn=term_urn,
                aspect=term_properties,
                changeType=ChangeTypeClass.UPSERT,
            )
            
            # Create workunit for term properties
            workunit = MetadataWorkUnit(
                id=f"{term_urn}-properties",
                mcp=mcp_properties,
            )
            
            # Add parent nodes if present
            if "parentNodes" in term_data and "nodes" in term_data["parentNodes"]:
                parent_nodes = []
                for parent in term_data["parentNodes"]["nodes"]:
                    if "urn" in parent:
                        parent_nodes.append(parent["urn"])
                
                if parent_nodes:
                    # Create parent nodes aspect
                    parent_nodes_aspect = ParentNodesClass(
                        nodes=[GlossaryNodeInfo(urn=parent) for parent in parent_nodes]
                    )
                    
                    mcp_parents = MetadataChangeProposalWrapper(
                        entityUrn=term_urn,
                        aspect=parent_nodes_aspect,
                        changeType=ChangeTypeClass.UPSERT,
                    )
                    
                    # Add parent nodes to the same workunit
                    workunit.metadata.mcp2.append(mcp_parents)
            
            # Add ownership if present
            if "ownership" in term_data and "owners" in term_data["ownership"]:
                owners = []
                for owner_data in term_data["ownership"]["owners"]:
                    if "owner" not in owner_data or "urn" not in owner_data["owner"]:
                        continue
                    
                    owner_urn = owner_data["owner"]["urn"]
                    owner_type = owner_data.get("type", "UNKNOWN")
                    
                    ownership_type_urn = "urn:li:ownershipType:Technical"
                    if "ownershipType" in owner_data and "urn" in owner_data["ownershipType"]:
                        ownership_type_urn = owner_data["ownershipType"]["urn"]
                    
                    owners.append(
                        OwnerClass(
                            owner=owner_urn,
                            type=owner_type,
                            ownershipType=OwnershipTypeClass(ownership_type_urn)
                        )
                    )
                
                if owners:
                    ownership = OwnershipClass(owners=owners)
                    
                    mcp_ownership = MetadataChangeProposalWrapper(
                        entityUrn=term_urn,
                        aspect=ownership,
                        changeType=ChangeTypeClass.UPSERT,
                    )
                    
                    # Add ownership to the same workunit
                    workunit.metadata.mcp2.append(mcp_ownership)
            
            return workunit
            
        except Exception as e:
            logger.error(f"Error creating glossary term workunit: {str(e)}")
            return None
    
    def create_glossary_term_association_workunit(self, entity_urn: str, term_urn: str) -> Optional[MetadataWorkUnit]:
        """
        Create a MetadataWorkUnit for associating an entity with a glossary term
        
        Args:
            entity_urn: URN of the entity to associate
            term_urn: URN of the glossary term
            
        Returns:
            MetadataWorkUnit if successful, None otherwise
        """
        try:
            # Create term association
            term_association = GlossaryTermAssociationClass(
                urn=term_urn
            )
            
            # Get existing terms if any
            entity = None
            existing_terms = []
            try:
                if self.context.graph:
                    entity = self.context.graph.get_entity(entity_urn)
                    if entity and "glossaryTerms" in entity and "terms" in entity["glossaryTerms"]:
                        existing_terms = entity["glossaryTerms"]["terms"]
            except Exception:
                pass  # If we can't get existing terms, we'll just add the new one
            
            # Check if term already exists
            for existing_term in existing_terms:
                if existing_term.get("urn") == term_urn:
                    logger.info(f"Term {term_urn} already associated with {entity_urn}")
                    return None
            
            # Add the new term to existing ones
            terms_aspect = GlossaryTermsClass(
                terms=existing_terms + [term_association]
            )
            
            # Create MCP for term association
            mcp = MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=terms_aspect,
                changeType=ChangeTypeClass.UPSERT,
            )
            
            # Create workunit for term association
            workunit = MetadataWorkUnit(
                id=f"{entity_urn}-{term_urn}-association",
                mcp=mcp,
            )
            
            return workunit
            
        except Exception as e:
            logger.error(f"Error creating glossary term association workunit: {str(e)}")
            return None
    
    def delete_glossary_term_workunit(self, term_urn: str) -> Optional[MetadataWorkUnit]:
        """
        Create a MetadataWorkUnit for glossary term deletion
        
        Args:
            term_urn: URN of the glossary term to delete
            
        Returns:
            MetadataWorkUnit if successful, None otherwise
        """
        try:
            # Create key aspect for the glossary term
            term_key = GlossaryTermKeyClass()
            
            # Create MCP for glossary term deletion
            mcp = MetadataChangeProposalWrapper(
                entityUrn=term_urn,
                aspect=term_key,
                changeType=ChangeTypeClass.DELETE,
            )
            
            # Create workunit for glossary term deletion
            workunit = MetadataWorkUnit(
                id=f"{term_urn}-delete",
                mcp=mcp,
            )
            
            return workunit
            
        except Exception as e:
            logger.error(f"Error creating glossary term deletion workunit: {str(e)}")
            return None
    
    def import_glossary_term(self, term_data: Dict[str, Any]) -> Optional[str]:
        """
        Import a glossary term from data
        
        Args:
            term_data: Glossary term data
            
        Returns:
            Glossary term URN if successful, None otherwise
        """
        try:
            # Create workunit for glossary term
            workunit = self.create_glossary_term_workunit(term_data)
            if not workunit:
                logger.error("Failed to create glossary term workunit")
                return None
            
            # Emit the workunit
            success = self.emit_workunit(workunit)
            if not success:
                logger.error("Failed to emit glossary term workunit")
                return None
            
            # Get the glossary term URN from the workunit
            term_urn = workunit.metadata.mcp.entityUrn
            
            # Return the URN as a string
            return str(term_urn)
            
        except Exception as e:
            logger.error(f"Error importing glossary term: {str(e)}")
            return None
    
    def update_glossary_term(self, term_urn: str, term_data: Dict[str, Any]) -> bool:
        """
        Update an existing glossary term
        
        Args:
            term_urn: URN of the glossary term to update
            term_data: Updated glossary term data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure the URN in the data matches the provided URN
            term_data["urn"] = term_urn
            
            # Create workunit for glossary term update
            workunit = self.create_glossary_term_workunit(term_data)
            if not workunit:
                logger.error("Failed to create glossary term update workunit")
                return False
            
            # Emit the workunit
            success = self.emit_workunit(workunit)
            if not success:
                logger.error("Failed to emit glossary term update workunit")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating glossary term: {str(e)}")
            return False
    
    def delete_glossary_term(self, term_urn: str, force: bool = False) -> bool:
        """
        Delete a glossary term
        
        Args:
            term_urn: URN of the glossary term to delete
            force: Whether to force delete even if term has entity associations
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check for entities using this term if not forcing
            if not force:
                term_with_entities = self.export_glossary_term(term_urn, include_related=True)
                has_entities = term_with_entities is not None and "relatedEntities" in term_with_entities and len(term_with_entities["relatedEntities"]) > 0
                
                if has_entities:
                    logger.error(f"Glossary term {term_urn} is associated with entities. Use force=True to delete anyway.")
                    return False
            
            # Create workunit for glossary term deletion
            workunit = self.delete_glossary_term_workunit(term_urn)
            if not workunit:
                logger.error("Failed to create glossary term deletion workunit")
                return False
            
            # Emit the workunit
            success = self.emit_workunit(workunit)
            if not success:
                logger.error("Failed to emit glossary term deletion workunit")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error deleting glossary term: {str(e)}")
            return False
            
    # ============================
    # Assertion methods
    # ============================
    
    def list_assertions(self, dataset_urn: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List data quality assertions in DataHub
        
        Args:
            dataset_urn: Optional dataset URN to filter assertions by dataset
            
        Returns:
            List of assertion objects
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return []
        
        try:
            assertions = []
            
            if dataset_urn:
                # Get assertions for a specific dataset
                query = """
                query getDatasetAssertions($urn: String!) {
                  dataset(urn: $urn) {
                    assertions(start: 0, count: 1000) {
                      assertions {
                        urn
                        info {
                          type
                          description
                        }
                      }
                    }
                  }
                }
                """
                
                variables = {
                    "urn": dataset_urn
                }
                
                result = self.context.graph.execute_graphql(query, variables)
                
                if result and "data" in result and "dataset" in result["data"] and result["data"]["dataset"]:
                    assertions_data = result["data"]["dataset"]["assertions"]["assertions"]
                    
                    # Get detailed assertion info
                    for assertion_data in assertions_data:
                        assertion_urn = assertion_data.get("urn")
                        if assertion_urn:
                            assertion_info = self.get_assertion(assertion_urn)
                            if assertion_info:
                                assertions.append(assertion_info)
            else:
                # Get all assertions using search (there's no direct API to get all assertions)
                assertions_search = self.context.graph.search_across_entities(
                    query="*",
                    entity_types=["assertion"],
                    start=0,
                    count=1000
                )
                
                if assertions_search and "searchResults" in assertions_search:
                    for result in assertions_search["searchResults"]:
                        if "entity" in result and "urn" in result["entity"]:
                            assertion_urn = result["entity"]["urn"]
                            assertion_info = self.get_assertion(assertion_urn)
                            if assertion_info:
                                assertions.append(assertion_info)
            
            return assertions
        except Exception as e:
            logger.error(f"Error listing assertions: {str(e)}")
            return []
    
    def get_assertion(self, assertion_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get assertion details by URN
        
        Args:
            assertion_urn: Assertion URN
            
        Returns:
            Dictionary with assertion data or None if not found
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return None
        
        try:
            query = """
            query getAssertion($urn: String!) {
              assertion(urn: $urn) {
                urn
                runEvents(status: COMPLETE, limit: 5) {
                  total
                  failed
                  succeeded
                  runEvents {
                    timestampMillis
                    status
                    result {
                      type
                      nativeResults {
                        key
                        value
                      }
                    }
                  }
                }
                info {
                  type
                  description
                  lastUpdated {
                    time
                    actor
                  }
                  datasetAssertion {
                    datasetUrn
                    scope
                    aggregation
                    operator
                    parameters {
                      value {
                        value
                        type
                      }
                      minValue {
                        value
                        type
                      }
                      maxValue {
                        value
                        type
                      }
                    }
                  }
                  freshnessAssertion {
                    type
                    entityUrn
                    schedule {
                      type
                      cron {
                        cron
                        timezone
                      }
                      fixedInterval {
                        unit
                        multiple
                      }
                    }
                  }
                  sqlAssertion {
                    type
                    entityUrn
                    statement
                    operator
                    parameters {
                      value {
                        value
                        type
                      }
                    }
                  }
                  fieldAssertion {
                    type
                    entityUrn
                    fieldValuesAssertion {
                      field {
                        path
                        type
                        nativeType
                      }
                      operator
                      parameters {
                        value {
                          value
                          type
                        }
                      }
                      failThreshold {
                        type
                        value
                      }
                    }
                  }
                  volumeAssertion {
                    type
                    entityUrn
                    rowCountTotal {
                      operator
                      parameters {
                        value {
                          value
                          type
                        }
                        minValue {
                          value
                          type
                        }
                        maxValue {
                          value
                          type
                        }
                      }
                    }
                    rowCountChange {
                      type
                      operator
                      parameters {
                        value {
                          value
                          type
                        }
                      }
                    }
                  }
                  schemaAssertion {
                    entityUrn
                    compatibility
                    fields {
                      path
                      type
                    }
                  }
                }
              }
            }
            """
            
            variables = {
                "urn": assertion_urn
            }
            
            result = self.context.graph.execute_graphql(query, variables)
            
            if result and "data" in result and "assertion" in result["data"]:
                return result["data"]["assertion"]
            
            return None
        except Exception as e:
            logger.error(f"Error getting assertion: {str(e)}")
            return None
    
    def create_freshness_assertion(self, dataset_urn: str, schedule_interval: int, 
                                schedule_unit: str, timezone: str, cron: str, 
                                source_type: str = "INFORMATION_SCHEMA", 
                                description: str = "") -> Optional[str]:
        """
        Create a freshness assertion for a dataset
        
        Args:
            dataset_urn: Dataset URN
            schedule_interval: Interval value (e.g., 8 for 8 hours)
            schedule_unit: Interval unit (DAY, HOUR, MINUTE)
            timezone: Timezone for evaluation (e.g., "America/Los_Angeles")
            cron: Cron expression for evaluation schedule
            source_type: Source type for evaluation (default: INFORMATION_SCHEMA)
            description: Optional description
            
        Returns:
            Assertion URN if successful, None otherwise
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return None
        
        try:
            query = """
            mutation upsertDatasetFreshnessAssertionMonitor(
                $entityUrn: String!,
                $scheduleType: ScheduleType!,
                $scheduleUnit: IntervalUnit!,
                $scheduleMultiple: Int!,
                $timezone: String!,
                $cron: String!,
                $sourceType: AssertionEvaluationSourceType!,
                $description: String
            ) {
              upsertDatasetFreshnessAssertionMonitor(
                input: {
                  entityUrn: $entityUrn
                  schedule: {
                    type: $scheduleType
                    fixedInterval: { unit: $scheduleUnit, multiple: $scheduleMultiple }
                  }
                  evaluationSchedule: {
                    timezone: $timezone
                    cron: $cron
                  }
                  evaluationParameters: { sourceType: $sourceType }
                  mode: ACTIVE
                  description: $description
                }
              ) {
                urn
              }
            }
            """
            
            variables = {
                "entityUrn": dataset_urn,
                "scheduleType": "FIXED_INTERVAL",
                "scheduleUnit": schedule_unit,
                "scheduleMultiple": schedule_interval,
                "timezone": timezone,
                "cron": cron,
                "sourceType": source_type,
                "description": description
            }
            
            result = self.context.graph.execute_graphql(query, variables)
            
            if (result and "data" in result and 
                "upsertDatasetFreshnessAssertionMonitor" in result["data"] and
                "urn" in result["data"]["upsertDatasetFreshnessAssertionMonitor"]):
                return result["data"]["upsertDatasetFreshnessAssertionMonitor"]["urn"]
            
            logger.error(f"Failed to create freshness assertion: {result}")
            return None
        except Exception as e:
            logger.error(f"Error creating freshness assertion: {str(e)}")
            return None
    
    def create_volume_assertion(self, dataset_urn: str, operator: str, 
                               min_value: Optional[str] = None, max_value: Optional[str] = None,
                               value: Optional[str] = None, timezone: str = "America/Los_Angeles",
                               cron: str = "0 */8 * * *", source_type: str = "INFORMATION_SCHEMA",
                               description: str = "") -> Optional[str]:
        """
        Create a volume assertion for a dataset
        
        Args:
            dataset_urn: Dataset URN
            operator: Comparison operator (EQUAL_TO, GREATER_THAN, LESS_THAN, BETWEEN, etc.)
            min_value: Minimum value for BETWEEN operator
            max_value: Maximum value for BETWEEN operator
            value: Single value for other operators
            timezone: Timezone for evaluation (default: "America/Los_Angeles")
            cron: Cron expression for evaluation schedule (default: "0 */8 * * *")
            source_type: Source type for evaluation (default: INFORMATION_SCHEMA)
            description: Optional description
            
        Returns:
            Assertion URN if successful, None otherwise
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return None
        
        try:
            query = """
            mutation upsertDatasetVolumeAssertionMonitor(
                $entityUrn: String!,
                $operator: AssertionOperator!,
                $minValue: AssertionValueInput,
                $maxValue: AssertionValueInput,
                $value: AssertionValueInput,
                $timezone: String!,
                $cron: String!,
                $sourceType: AssertionEvaluationSourceType!,
                $description: String
            ) {
              upsertDatasetVolumeAssertionMonitor(
                input: {
                  entityUrn: $entityUrn
                  type: ROW_COUNT_TOTAL
                  rowCountTotal: {
                    operator: $operator
                    parameters: {
                      minValue: $minValue
                      maxValue: $maxValue
                      value: $value
                    }
                  }
                  evaluationSchedule: {
                    timezone: $timezone
                    cron: $cron
                  }
                  evaluationParameters: { sourceType: $sourceType }
                  mode: ACTIVE
                  description: $description
                }
              ) {
                urn
              }
            }
            """
            
            # Prepare parameters based on operator
            parameters = {
                "entityUrn": dataset_urn,
                "operator": operator,
                "timezone": timezone,
                "cron": cron,
                "sourceType": source_type,
                "description": description,
                "minValue": None,
                "maxValue": None,
                "value": None
            }
            
            if operator == "BETWEEN":
                if min_value is None or max_value is None:
                    logger.error("Both min_value and max_value are required for BETWEEN operator")
                    return None
                
                parameters["minValue"] = {"value": min_value, "type": "NUMBER"}
                parameters["maxValue"] = {"value": max_value, "type": "NUMBER"}
            else:
                if value is None:
                    logger.error("Value is required for operator: " + operator)
                    return None
                
                parameters["value"] = {"value": value, "type": "NUMBER"}
            
            result = self.context.graph.execute_graphql(query, parameters)
            
            if (result and "data" in result and 
                "upsertDatasetVolumeAssertionMonitor" in result["data"] and
                "urn" in result["data"]["upsertDatasetVolumeAssertionMonitor"]):
                return result["data"]["upsertDatasetVolumeAssertionMonitor"]["urn"]
            
            logger.error(f"Failed to create volume assertion: {result}")
            return None
        except Exception as e:
            logger.error(f"Error creating volume assertion: {str(e)}")
            return None
    
    def create_field_assertion(self, dataset_urn: str, field_path: str, field_type: str,
                              native_type: str, operator: str, value: str, value_type: str = "NUMBER",
                              fail_threshold: int = 0, exclude_nulls: bool = True,
                              timezone: str = "America/Los_Angeles", cron: str = "0 */8 * * *",
                              source_type: str = "ALL_ROWS_QUERY", description: str = "") -> Optional[str]:
        """
        Create a field assertion for a dataset column
        
        Args:
            dataset_urn: Dataset URN
            field_path: Column/field name
            field_type: Field type (NUMBER, STRING, etc.)
            native_type: Native field type in the source system
            operator: Comparison operator
            value: Value to compare against
            value_type: Type of the value (NUMBER, STRING, etc.)
            fail_threshold: Failure threshold count
            exclude_nulls: Whether to exclude nulls from the check
            timezone: Timezone for evaluation
            cron: Cron expression for evaluation schedule
            source_type: Source type for evaluation
            description: Optional description
            
        Returns:
            Assertion URN if successful, None otherwise
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return None
        
        try:
            query = """
            mutation upsertDatasetFieldAssertionMonitor(
                $entityUrn: String!,
                $fieldPath: String!,
                $fieldType: String!,
                $nativeType: String!,
                $operator: AssertionOperator!,
                $value: AssertionValueInput!,
                $failThresholdType: AssertionThresholdType!,
                $failThresholdValue: Float!,
                $excludeNulls: Boolean!,
                $timezone: String!,
                $cron: String!,
                $sourceType: AssertionEvaluationSourceType!,
                $description: String
            ) {
              upsertDatasetFieldAssertionMonitor(
                input: {
                  entityUrn: $entityUrn
                  type: FIELD_VALUES
                  fieldValuesAssertion: {
                    field: {
                      path: $fieldPath
                      type: $fieldType
                      nativeType: $nativeType
                    }
                    operator: $operator
                    parameters: { value: $value }
                    failThreshold: { type: $failThresholdType, value: $failThresholdValue }
                    excludeNulls: $excludeNulls
                  }
                  evaluationSchedule: {
                    timezone: $timezone
                    cron: $cron
                  }
                  evaluationParameters: { sourceType: $sourceType }
                  mode: ACTIVE
                  description: $description
                }
              ) {
                urn
              }
            }
            """
            
            variables = {
                "entityUrn": dataset_urn,
                "fieldPath": field_path,
                "fieldType": field_type,
                "nativeType": native_type,
                "operator": operator,
                "value": {"value": value, "type": value_type},
                "failThresholdType": "COUNT",
                "failThresholdValue": float(fail_threshold),
                "excludeNulls": exclude_nulls,
                "timezone": timezone,
                "cron": cron,
                "sourceType": source_type,
                "description": description
            }
            
            result = self.context.graph.execute_graphql(query, variables)
            
            if (result and "data" in result and 
                "upsertDatasetFieldAssertionMonitor" in result["data"] and
                "urn" in result["data"]["upsertDatasetFieldAssertionMonitor"]):
                return result["data"]["upsertDatasetFieldAssertionMonitor"]["urn"]
            
            logger.error(f"Failed to create field assertion: {result}")
            return None
        except Exception as e:
            logger.error(f"Error creating field assertion: {str(e)}")
            return None
    
    def create_sql_assertion(self, dataset_urn: str, sql_statement: str, operator: str,
                            value: str, value_type: str = "NUMBER", timezone: str = "America/Los_Angeles",
                            cron: str = "0 */6 * * *", description: str = "") -> Optional[str]:
        """
        Create a custom SQL assertion for a dataset
        
        Args:
            dataset_urn: Dataset URN
            sql_statement: SQL query to be evaluated
            operator: Comparison operator
            value: Value to compare against
            value_type: Type of the value (NUMBER, STRING, etc.)
            timezone: Timezone for evaluation
            cron: Cron expression for evaluation schedule
            description: Optional description
            
        Returns:
            Assertion URN if successful, None otherwise
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return None
        
        try:
            query = """
            mutation upsertDatasetSqlAssertionMonitor(
                $entityUrn: String!,
                $statement: String!,
                $operator: AssertionOperator!,
                $value: AssertionValueInput!,
                $timezone: String!,
                $cron: String!,
                $description: String
            ) {
              upsertDatasetSqlAssertionMonitor(
                input: {
                  entityUrn: $entityUrn
                  type: METRIC
                  statement: $statement
                  operator: $operator
                  parameters: { value: $value }
                  evaluationSchedule: {
                    timezone: $timezone
                    cron: $cron
                  }
                  mode: ACTIVE
                  description: $description
                }
              ) {
                urn
              }
            }
            """
            
            variables = {
                "entityUrn": dataset_urn,
                "statement": sql_statement,
                "operator": operator,
                "value": {"value": value, "type": value_type},
                "timezone": timezone,
                "cron": cron,
                "description": description
            }
            
            result = self.context.graph.execute_graphql(query, variables)
            
            if (result and "data" in result and 
                "upsertDatasetSqlAssertionMonitor" in result["data"] and
                "urn" in result["data"]["upsertDatasetSqlAssertionMonitor"]):
                return result["data"]["upsertDatasetSqlAssertionMonitor"]["urn"]
            
            logger.error(f"Failed to create SQL assertion: {result}")
            return None
        except Exception as e:
            logger.error(f"Error creating SQL assertion: {str(e)}")
            return None
    
    def create_schema_assertion(self, dataset_urn: str, fields: List[Dict[str, str]],
                               compatibility: str = "EXACT_MATCH", description: str = "") -> Optional[str]:
        """
        Create a schema assertion for a dataset
        
        Args:
            dataset_urn: Dataset URN
            fields: List of field definitions (dicts with 'path' and 'type' keys)
            compatibility: Schema compatibility type
            description: Optional description
            
        Returns:
            Assertion URN if successful, None otherwise
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return None
        
        try:
            query = """
            mutation upsertDatasetSchemaAssertionMonitor(
                $entityUrn: String!,
                $compatibility: SchemaCompatibilityLevel!,
                $fields: [SchemaFieldInput!]!,
                $description: String
            ) {
              upsertDatasetSchemaAssertionMonitor(
                input: {
                  entityUrn: $entityUrn
                  assertion: {
                    compatibility: $compatibility
                    fields: $fields
                  }
                  description: $description
                  mode: ACTIVE
                }
              ) {
                urn
              }
            }
            """
            
            # Convert fields to the expected format
            schema_fields = []
            for field in fields:
                schema_fields.append({
                    "path": field["path"],
                    "type": field["type"]
                })
            
            variables = {
                "entityUrn": dataset_urn,
                "compatibility": compatibility,
                "fields": schema_fields,
                "description": description
            }
            
            result = self.context.graph.execute_graphql(query, variables)
            
            if (result and "data" in result and 
                "upsertDatasetSchemaAssertionMonitor" in result["data"] and
                "urn" in result["data"]["upsertDatasetSchemaAssertionMonitor"]):
                return result["data"]["upsertDatasetSchemaAssertionMonitor"]["urn"]
            
            logger.error(f"Failed to create schema assertion: {result}")
            return None
        except Exception as e:
            logger.error(f"Error creating schema assertion: {str(e)}")
            return None
    
    def run_assertion(self, assertion_urn: str, save_result: bool = True) -> Optional[Dict[str, Any]]:
        """
        Run a specific assertion
        
        Args:
            assertion_urn: Assertion URN
            save_result: Whether to save the result
            
        Returns:
            Assertion result or None if failed
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return None
        
        try:
            query = """
            mutation runAssertion($urn: String!, $saveResult: Boolean!) {
              runAssertion(urn: $urn, saveResult: $saveResult) {
                type
                nativeResults {
                  key
                  value
                }
              }
            }
            """
            
            variables = {
                "urn": assertion_urn,
                "saveResult": save_result
            }
            
            result = self.context.graph.execute_graphql(query, variables)
            
            if result and "data" in result and "runAssertion" in result["data"]:
                return result["data"]["runAssertion"]
            
            logger.error(f"Failed to run assertion: {result}")
            return None
        except Exception as e:
            logger.error(f"Error running assertion: {str(e)}")
            return None
    
    def run_assertions_for_asset(self, dataset_urn: str, 
                                tag_urns: Optional[List[str]] = None,
                                save_result: bool = True) -> Optional[Dict[str, Any]]:
        """
        Run all assertions for a dataset
        
        Args:
            dataset_urn: Dataset URN
            tag_urns: Optional list of tag URNs to filter assertions
            save_result: Whether to save the result
            
        Returns:
            Dictionary of assertion results or None if failed
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return None
        
        try:
            query = """
            mutation runAssertionsForAsset($urn: String!, $tagUrns: [String!], $saveResult: Boolean!) {
              runAssertionsForAsset(urn: $urn, tagUrns: $tagUrns, saveResult: $saveResult) {
                results {
                  key
                  value {
                    type
                    nativeResults {
                      key
                      value
                    }
                  }
                }
              }
            }
            """
            
            variables = {
                "urn": dataset_urn,
                "tagUrns": tag_urns,
                "saveResult": save_result
            }
            
            result = self.context.graph.execute_graphql(query, variables)
            
            if result and "data" in result and "runAssertionsForAsset" in result["data"]:
                # Convert results to a more usable format
                assertion_results = {}
                if "results" in result["data"]["runAssertionsForAsset"]:
                    for entry in result["data"]["runAssertionsForAsset"]["results"]:
                        assertion_results[entry["key"]] = entry["value"]
                
                return {
                    "results": assertion_results
                }
            
            logger.error(f"Failed to run assertions for asset: {result}")
            return None
        except Exception as e:
            logger.error(f"Error running assertions for asset: {str(e)}")
            return None
    
    def delete_assertion(self, assertion_urn: str) -> bool:
        """
        Delete an assertion
        
        Args:
            assertion_urn: Assertion URN
            
        Returns:
            True if successful, False otherwise
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return False
        
        try:
            query = """
            mutation deleteAssertion($urn: String!) {
              deleteAssertion(urn: $urn)
            }
            """
            
            variables = {
                "urn": assertion_urn
            }
            
            result = self.context.graph.execute_graphql(query, variables)
            
            if result and "data" in result and "deleteAssertion" in result["data"]:
                return result["data"]["deleteAssertion"]
            
            logger.error(f"Failed to delete assertion: {result}")
            return False
        except Exception as e:
            logger.error(f"Error deleting assertion: {str(e)}")
            return False
    
    def add_tag_to_assertion(self, assertion_urn: str, tag_urn: str) -> bool:
        """
        Add a tag to an assertion
        
        Args:
            assertion_urn: Assertion URN
            tag_urn: Tag URN
            
        Returns:
            True if successful, False otherwise
        """
        if self.context.graph is None:
            logger.error("Graph client is not initialized")
            return False
        
        try:
            query = """
            mutation addTag($resourceUrn: String!, $tagUrn: String!) {
              addTag(
                input: {
                  resourceUrn: $resourceUrn
                  tagUrn: $tagUrn
                }
              )
            }
            """
            
            variables = {
                "resourceUrn": assertion_urn,
                "tagUrn": tag_urn
            }
            
            result = self.context.graph.execute_graphql(query, variables)
            
            if result and "data" in result and "addTag" in result["data"]:
                return result["data"]["addTag"]
            
            logger.error(f"Failed to add tag to assertion: {result}")
            return False
        except Exception as e:
            logger.error(f"Error adding tag to assertion: {str(e)}")
            return False 