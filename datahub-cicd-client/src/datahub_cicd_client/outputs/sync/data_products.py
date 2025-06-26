"""
Synchronous output operations for data products using GraphQL mutations.
"""

from typing import Any, Dict, List

from datahub_cicd_client.core.connection import DataHubConnection
from datahub_cicd_client.graphql.mutations.data_products import *
from datahub_cicd_client.outputs.sync.base_sync_output import (
    BaseSyncOutput,
    EntityRelationshipSyncOutput,
    MetadataAssignmentSyncOutput,
)
from datahub_cicd_client.services.base_service import OperationResult


class DataProductSyncOutput(BaseSyncOutput, EntityRelationshipSyncOutput, MetadataAssignmentSyncOutput):
    """Synchronous output operations for data products."""

    def __init__(self, connection: DataHubConnection):
        super().__init__(connection)

    def create_entity(self, entity_data: Dict[str, Any]) -> OperationResult:
        """Create a new data product."""
        try:
            result = self.execute_graphql(
                CREATE_DATA_PRODUCT_MUTATION,
                {
                    "input": {
                        "id": entity_data["id"],
                        "name": entity_data["name"],
                        "description": entity_data.get("description", ""),
                        "domain": entity_data.get("domain"),
                        "assets": entity_data.get("assets", [])
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("create_data_product", error_message="GraphQL errors occurred")

            data_product_urn = result["data"]["createDataProduct"]
            return self._create_success_result("create_data_product", data_product_urn, result)

        except Exception as e:
            self.logger.error(f"Error creating data product: {e}")
            return self._create_error_result("create_data_product", error_message=str(e))

    def update_entity(self, entity_urn: str, entity_data: Dict[str, Any]) -> OperationResult:
        """Update an existing data product."""
        try:
            # Update description if provided
            if "description" in entity_data:
                result = self.execute_graphql(
                    UPDATE_DATA_PRODUCT_DESCRIPTION_MUTATION,
                    {
                        "input": {
                            "dataProductUrn": entity_urn,
                            "description": entity_data["description"]
                        }
                    }
                )

                if not self._check_graphql_errors(result):
                    return self._create_error_result("update_data_product", entity_urn, "Failed to update description")

            # Update domain if provided
            if "domain" in entity_data:
                domain_result = self.execute_graphql(
                    SET_DATA_PRODUCT_DOMAIN_MUTATION,
                    {
                        "input": {
                            "dataProductUrn": entity_urn,
                            "domainUrn": entity_data["domain"]
                        }
                    }
                )

                if not self._check_graphql_errors(domain_result):
                    return self._create_error_result("update_data_product", entity_urn, "Failed to update domain")

            # Update assets if provided
            if "assets" in entity_data:
                assets_result = self.execute_graphql(
                    ADD_ASSETS_TO_DATA_PRODUCT_MUTATION,
                    {
                        "input": {
                            "dataProductUrn": entity_urn,
                            "assetUrns": entity_data["assets"]
                        }
                    }
                )

                if not self._check_graphql_errors(assets_result):
                    return self._create_error_result("update_data_product", entity_urn, "Failed to update assets")

            return self._create_success_result("update_data_product", entity_urn)

        except Exception as e:
            self.logger.error(f"Error updating data product: {e}")
            return self._create_error_result("update_data_product", entity_urn, str(e))

    def delete_entity(self, entity_urn: str) -> OperationResult:
        """Delete a data product."""
        try:
            result = self.execute_graphql(
                DELETE_DATA_PRODUCT_MUTATION,
                {"dataProductUrn": entity_urn}
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("delete_data_product", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("delete_data_product", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error deleting data product: {e}")
            return self._create_error_result("delete_data_product", entity_urn, str(e))

    def add_owner(self, entity_urn: str, owner_urn: str, ownership_type: str) -> OperationResult:
        """Add owner to data product."""
        try:
            result = self.execute_graphql(
                ADD_DATA_PRODUCT_OWNER_MUTATION,
                {
                    "input": {
                        "dataProductUrn": entity_urn,
                        "owners": [{
                            "ownerUrn": owner_urn,
                            "type": ownership_type
                        }]
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("add_data_product_owner", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("add_data_product_owner", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error adding data product owner: {e}")
            return self._create_error_result("add_data_product_owner", entity_urn, str(e))

    def remove_owner(self, entity_urn: str, owner_urn: str, ownership_type: str) -> OperationResult:
        """Remove owner from data product."""
        try:
            result = self.execute_graphql(
                REMOVE_DATA_PRODUCT_OWNER_MUTATION,
                {
                    "input": {
                        "dataProductUrn": entity_urn,
                        "ownerUrn": owner_urn
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("remove_data_product_owner", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("remove_data_product_owner", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error removing data product owner: {e}")
            return self._create_error_result("remove_data_product_owner", entity_urn, str(e))

    def add_tag(self, entity_urn: str, tag_urn: str) -> OperationResult:
        """Add tag to data product."""
        try:
            result = self.execute_graphql(
                ADD_DATA_PRODUCT_TAG_MUTATION,
                {
                    "input": {
                        "dataProductUrn": entity_urn,
                        "tagUrns": [tag_urn]
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("add_data_product_tag", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("add_data_product_tag", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error adding data product tag: {e}")
            return self._create_error_result("add_data_product_tag", entity_urn, str(e))

    def remove_tag(self, entity_urn: str, tag_urn: str) -> OperationResult:
        """Remove tag from data product."""
        try:
            result = self.execute_graphql(
                REMOVE_DATA_PRODUCT_TAG_MUTATION,
                {
                    "input": {
                        "dataProductUrn": entity_urn,
                        "tagUrn": tag_urn
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("remove_data_product_tag", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("remove_data_product_tag", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error removing data product tag: {e}")
            return self._create_error_result("remove_data_product_tag", entity_urn, str(e))

    def add_glossary_term(self, entity_urn: str, glossary_term_urn: str) -> OperationResult:
        """Add glossary term to data product."""
        try:
            result = self.execute_graphql(
                ADD_DATA_PRODUCT_GLOSSARY_TERM_MUTATION,
                {
                    "input": {
                        "dataProductUrn": entity_urn,
                        "glossaryTermUrns": [glossary_term_urn]
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("add_data_product_glossary_term", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("add_data_product_glossary_term", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error adding data product glossary term: {e}")
            return self._create_error_result("add_data_product_glossary_term", entity_urn, str(e))

    def remove_glossary_term(self, entity_urn: str, glossary_term_urn: str) -> OperationResult:
        """Remove glossary term from data product."""
        try:
            result = self.execute_graphql(
                REMOVE_DATA_PRODUCT_GLOSSARY_TERM_MUTATION,
                {
                    "input": {
                        "dataProductUrn": entity_urn,
                        "glossaryTermUrn": glossary_term_urn
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("remove_data_product_glossary_term", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("remove_data_product_glossary_term", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error removing data product glossary term: {e}")
            return self._create_error_result("remove_data_product_glossary_term", entity_urn, str(e))

    def set_domain(self, entity_urn: str, domain_urn: str) -> OperationResult:
        """Set domain for data product."""
        try:
            result = self.execute_graphql(
                SET_DATA_PRODUCT_DOMAIN_MUTATION,
                {
                    "input": {
                        "dataProductUrn": entity_urn,
                        "domainUrn": domain_urn
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("set_data_product_domain", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("set_data_product_domain", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error setting data product domain: {e}")
            return self._create_error_result("set_data_product_domain", entity_urn, str(e))

    def unset_domain(self, entity_urn: str) -> OperationResult:
        """Unset domain for data product."""
        try:
            result = self.execute_graphql(
                UNSET_DATA_PRODUCT_DOMAIN_MUTATION,
                {
                    "input": {
                        "dataProductUrn": entity_urn
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("unset_data_product_domain", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("unset_data_product_domain", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error unsetting data product domain: {e}")
            return self._create_error_result("unset_data_product_domain", entity_urn, str(e))

    def add_assets(self, entity_urn: str, asset_urns: List[str]) -> OperationResult:
        """Add assets to data product."""
        try:
            result = self.execute_graphql(
                ADD_ASSETS_TO_DATA_PRODUCT_MUTATION,
                {
                    "input": {
                        "dataProductUrn": entity_urn,
                        "assetUrns": asset_urns
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("add_assets_to_data_product", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("add_assets_to_data_product", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error adding assets to data product: {e}")
            return self._create_error_result("add_assets_to_data_product", entity_urn, str(e))

    def remove_assets(self, entity_urn: str, asset_urns: List[str]) -> OperationResult:
        """Remove assets from data product."""
        try:
            result = self.execute_graphql(
                REMOVE_ASSETS_FROM_DATA_PRODUCT_MUTATION,
                {
                    "input": {
                        "dataProductUrn": entity_urn,
                        "assetUrns": asset_urns
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("remove_assets_from_data_product", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("remove_assets_from_data_product", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error removing assets from data product: {e}")
            return self._create_error_result("remove_assets_from_data_product", entity_urn, str(e))
