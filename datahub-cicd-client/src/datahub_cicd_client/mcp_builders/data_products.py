"""
Data Product MCP Builder for DataHub.

This module provides functionality to create Metadata Change Proposals (MCPs)
for DataHub data products with all supported aspects.
"""

import logging
import time
from typing import Any, Dict, List, Optional, Union

from .base import BaseMCPBuilder

try:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (  # Data Product specific classes; Common aspect classes
        AuditStampClass,
        BrowsePathsClass,
        ChangeTypeClass,
        DataProductPropertiesClass,
        DeprecationClass,
        DomainsClass,
        GlobalTagsClass,
        GlossaryTermAssociationClass,
        GlossaryTermsClass,
        InstitutionalMemoryClass,
        InstitutionalMemoryMetadataClass,
        OwnerClass,
        OwnershipClass,
        OwnershipSourceClass,
        OwnershipSourceTypeClass,
        OwnershipTypeClass,
        StatusClass,
        StructuredPropertiesClass,
        StructuredPropertyValueAssignmentClass,
        SubTypesClass,
        TagAssociationClass,
    )

    DATAHUB_AVAILABLE = True
except ImportError as e:
    logging.warning(f"DataHub SDK not available: {e}")
    DATAHUB_AVAILABLE = False


class DataProductMCPBuilder(BaseMCPBuilder):
    """
    Builder for creating Data Product MCPs with comprehensive aspect support.
    """

    def get_entity_type(self) -> str:
        """Get the DataHub entity type for data products."""
        return "dataProduct"

    def create_properties_mcp(
        self,
        data_product_urn: str,
        name: str,
        description: Optional[str] = None,
        external_url: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> Optional[Union[MetadataChangeProposalWrapper, Dict[str, Any]]]:
        """Create data product properties MCP."""
        if not DATAHUB_AVAILABLE:
            self.logger.error("DataHub SDK not available")
            return None

        try:
            properties = DataProductPropertiesClass(
                name=name,
                description=description,
                externalUrl=external_url,
                customProperties=custom_properties or {},
            )

            return self.create_mcp_wrapper(
                entity_urn=data_product_urn, aspect=properties, change_type="UPSERT"
            )
        except Exception as e:
            self.logger.error(f"Error creating data product properties MCP: {e}")
            return None

    def create_ownership_mcp(
        self, data_product_urn: str, owners: List[str], **kwargs
    ) -> Optional[Union[MetadataChangeProposalWrapper, Dict[str, Any]]]:
        """Create data product ownership MCP."""
        if not DATAHUB_AVAILABLE:
            self.logger.error("DataHub SDK not available")
            return None

        try:
            owner_objects = []
            for owner_urn in owners:
                owner_objects.append(
                    OwnerClass(
                        owner=owner_urn,
                        type=OwnershipTypeClass.DATAOWNER,
                        source=OwnershipSourceClass(type=OwnershipSourceTypeClass.MANUAL),
                    )
                )

            ownership = OwnershipClass(owners=owner_objects, lastModified=self.create_audit_stamp())

            return self.create_mcp_wrapper(
                entity_urn=data_product_urn, aspect=ownership, change_type="UPSERT"
            )
        except Exception as e:
            self.logger.error(f"Error creating data product ownership MCP: {e}")
            return None

    def create_status_mcp(
        self, data_product_urn: str, removed: bool = False, **kwargs
    ) -> Optional[Union[MetadataChangeProposalWrapper, Dict[str, Any]]]:
        """Create data product status MCP."""
        if not DATAHUB_AVAILABLE:
            self.logger.error("DataHub SDK not available")
            return None

        try:
            status = StatusClass(removed=removed)

            return self.create_mcp_wrapper(
                entity_urn=data_product_urn, aspect=status, change_type="UPSERT"
            )
        except Exception as e:
            self.logger.error(f"Error creating data product status MCP: {e}")
            return None

    def create_global_tags_mcp(
        self, data_product_urn: str, tags: List[str], **kwargs
    ) -> Optional[Union[MetadataChangeProposalWrapper, Dict[str, Any]]]:
        """Create data product global tags MCP."""
        if not DATAHUB_AVAILABLE:
            self.logger.error("DataHub SDK not available")
            return None

        try:
            tag_associations = []
            for tag_urn in tags:
                tag_associations.append(TagAssociationClass(tag=tag_urn))

            global_tags = GlobalTagsClass(tags=tag_associations)

            return self.create_mcp_wrapper(
                entity_urn=data_product_urn, aspect=global_tags, change_type="UPSERT"
            )
        except Exception as e:
            self.logger.error(f"Error creating data product global tags MCP: {e}")
            return None

    def create_glossary_terms_mcp(
        self, data_product_urn: str, terms: List[str], **kwargs
    ) -> Optional[Union[MetadataChangeProposalWrapper, Dict[str, Any]]]:
        """Create data product glossary terms MCP."""
        if not DATAHUB_AVAILABLE:
            self.logger.error("DataHub SDK not available")
            return None

        try:
            term_associations = []
            for term_urn in terms:
                term_associations.append(GlossaryTermAssociationClass(urn=term_urn))

            glossary_terms = GlossaryTermsClass(
                terms=term_associations, auditStamp=self.create_audit_stamp()
            )

            return self.create_mcp_wrapper(
                entity_urn=data_product_urn, aspect=glossary_terms, change_type="UPSERT"
            )
        except Exception as e:
            self.logger.error(f"Error creating data product glossary terms MCP: {e}")
            return None

    def create_institutional_memory_mcp(
        self, data_product_urn: str, links: List[Dict[str, str]], **kwargs
    ) -> Optional[Union[MetadataChangeProposalWrapper, Dict[str, Any]]]:
        """Create data product institutional memory MCP."""
        if not DATAHUB_AVAILABLE:
            self.logger.error("DataHub SDK not available")
            return None

        try:
            elements = []
            for link in links:
                elements.append(
                    InstitutionalMemoryMetadataClass(
                        url=link.get("url", ""),
                        description=link.get("description", ""),
                        createStamp=self.create_audit_stamp(),
                    )
                )

            institutional_memory = InstitutionalMemoryClass(elements=elements)

            return self.create_mcp_wrapper(
                entity_urn=data_product_urn, aspect=institutional_memory, change_type="UPSERT"
            )
        except Exception as e:
            self.logger.error(f"Error creating data product institutional memory MCP: {e}")
            return None

    def create_structured_properties_mcp(
        self, data_product_urn: str, structured_properties: List[Dict[str, Any]], **kwargs
    ) -> Optional[Union[MetadataChangeProposalWrapper, Dict[str, Any]]]:
        """Create data product structured properties MCP."""
        if not DATAHUB_AVAILABLE:
            self.logger.error("DataHub SDK not available")
            return None

        try:
            properties = {}
            for prop in structured_properties:
                prop_urn = prop.get("propertyUrn")
                value = prop.get("value")
                if prop_urn and value is not None:
                    properties[prop_urn] = StructuredPropertyValueAssignmentClass(values=[value])

            structured_props = StructuredPropertiesClass(properties=properties)

            return self.create_mcp_wrapper(
                entity_urn=data_product_urn, aspect=structured_props, change_type="UPSERT"
            )
        except Exception as e:
            self.logger.error(f"Error creating data product structured properties MCP: {e}")
            return None

    def create_domains_mcp(
        self, data_product_urn: str, domains: List[str], **kwargs
    ) -> Optional[Union[MetadataChangeProposalWrapper, Dict[str, Any]]]:
        """Create data product domains MCP."""
        if not DATAHUB_AVAILABLE:
            self.logger.error("DataHub SDK not available")
            return None

        try:
            domains_aspect = DomainsClass(domains=domains)

            return self.create_mcp_wrapper(
                entity_urn=data_product_urn, aspect=domains_aspect, change_type="UPSERT"
            )
        except Exception as e:
            self.logger.error(f"Error creating data product domains MCP: {e}")
            return None

    def create_sub_types_mcp(
        self, data_product_urn: str, sub_types: List[str], **kwargs
    ) -> Optional[Union[MetadataChangeProposalWrapper, Dict[str, Any]]]:
        """Create data product sub types MCP."""
        if not DATAHUB_AVAILABLE:
            self.logger.error("DataHub SDK not available")
            return None

        try:
            sub_types_aspect = SubTypesClass(typeNames=sub_types)

            return self.create_mcp_wrapper(
                entity_urn=data_product_urn, aspect=sub_types_aspect, change_type="UPSERT"
            )
        except Exception as e:
            self.logger.error(f"Error creating data product sub types MCP: {e}")
            return None

    def create_deprecation_mcp(
        self, data_product_urn: str, deprecated: bool = False, deprecation_note: str = "", **kwargs
    ) -> Optional[Union[MetadataChangeProposalWrapper, Dict[str, Any]]]:
        """Create data product deprecation MCP."""
        if not DATAHUB_AVAILABLE:
            self.logger.error("DataHub SDK not available")
            return None

        try:
            deprecation = DeprecationClass(
                deprecated=deprecated,
                note=deprecation_note,
                decommissionTime=int(time.time() * 1000) if deprecated else None,
            )

            return self.create_mcp_wrapper(
                entity_urn=data_product_urn, aspect=deprecation, change_type="UPSERT"
            )
        except Exception as e:
            self.logger.error(f"Error creating data product deprecation MCP: {e}")
            return None

    def create_entity_mcps(
        self,
        entity_data: Dict[str, Any],
        include_all_aspects: bool = True,
        custom_aspects: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> List[Union[MetadataChangeProposalWrapper, Dict[str, Any]]]:
        """
        Create all MCPs for a data product entity.

        Args:
            entity_data: Data product data dictionary
            include_all_aspects: Whether to include all supported aspects
            custom_aspects: Custom aspects dictionary
            **kwargs: Additional arguments

        Returns:
            List of MCPs for the data product
        """
        mcps = []

        data_product_urn = entity_data.get("urn")
        if not data_product_urn:
            data_product_id = entity_data.get("id")
            if data_product_id:
                data_product_urn = f"urn:li:dataProduct:{data_product_id}"
            else:
                raise ValueError("Either 'urn' or 'id' must be provided in entity_data")

        # Required properties MCP
        name = entity_data.get("name")
        if name:
            properties_mcp = self.create_properties_mcp(
                data_product_urn=data_product_urn,
                name=name,
                description=entity_data.get("description"),
                external_url=entity_data.get("external_url"),
                custom_properties=entity_data.get("custom_properties"),
            )
            if properties_mcp:
                mcps.append(properties_mcp)

        # Status MCP (always include)
        status_mcp = self.create_status_mcp(
            data_product_urn=data_product_urn, removed=entity_data.get("removed", False)
        )
        if status_mcp:
            mcps.append(status_mcp)

        if include_all_aspects:
            # Ownership MCP
            owners = entity_data.get("owners")
            if owners:
                ownership_mcp = self.create_ownership_mcp(data_product_urn, owners)
                if ownership_mcp:
                    mcps.append(ownership_mcp)

            # Tags MCP
            tags = entity_data.get("tags")
            if tags:
                tags_mcp = self.create_global_tags_mcp(data_product_urn, tags)
                if tags_mcp:
                    mcps.append(tags_mcp)

            # Glossary terms MCP
            terms = entity_data.get("terms")
            if terms:
                terms_mcp = self.create_glossary_terms_mcp(data_product_urn, terms)
                if terms_mcp:
                    mcps.append(terms_mcp)

            # Links/Documentation MCP
            links = entity_data.get("links")
            if links:
                memory_mcp = self.create_institutional_memory_mcp(data_product_urn, links)
                if memory_mcp:
                    mcps.append(memory_mcp)

            # Structured properties MCP
            structured_properties = entity_data.get("structured_properties")
            if structured_properties:
                props_mcp = self.create_structured_properties_mcp(
                    data_product_urn, structured_properties
                )
                if props_mcp:
                    mcps.append(props_mcp)

            # Domains MCP
            domains = entity_data.get("domains")
            if domains:
                domains_mcp = self.create_domains_mcp(data_product_urn, domains)
                if domains_mcp:
                    mcps.append(domains_mcp)

            # Sub types MCP
            sub_types = entity_data.get("sub_types")
            if sub_types:
                sub_types_mcp = self.create_sub_types_mcp(data_product_urn, sub_types)
                if sub_types_mcp:
                    mcps.append(sub_types_mcp)

            # Deprecation MCP
            if entity_data.get("deprecated", False):
                deprecation_mcp = self.create_deprecation_mcp(
                    data_product_urn=data_product_urn,
                    deprecated=True,
                    deprecation_note=entity_data.get("deprecation_note", ""),
                )
                if deprecation_mcp:
                    mcps.append(deprecation_mcp)

        # Custom aspects
        if custom_aspects:
            for aspect_name, _aspect_data in custom_aspects.items():
                self.logger.info(f"Custom aspect {aspect_name} provided but not implemented")

        return mcps
