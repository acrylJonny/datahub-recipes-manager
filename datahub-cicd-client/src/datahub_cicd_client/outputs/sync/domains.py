"""
Synchronous output operations for domains using GraphQL mutations.
"""

from typing import Any, Dict

from datahub_cicd_client.core.connection import DataHubConnection
from datahub_cicd_client.graphql.mutations.domains import *
from datahub_cicd_client.outputs.sync.base_sync_output import (
    BaseSyncOutput,
    EntityRelationshipSyncOutput,
)
from datahub_cicd_client.services.base_service import OperationResult


class DomainSyncOutput(BaseSyncOutput, EntityRelationshipSyncOutput):
    """Synchronous output operations for domains."""

    def __init__(self, connection: DataHubConnection):
        super().__init__(connection)

    def create_entity(self, entity_data: Dict[str, Any]) -> OperationResult:
        """Create a new domain."""
        try:
            result = self.execute_graphql(
                CREATE_DOMAIN_MUTATION,
                {
                    "input": {
                        "id": entity_data["id"],
                        "name": entity_data["name"],
                        "description": entity_data.get("description", ""),
                        "parentDomain": entity_data.get("parentDomain")
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("create_domain", error_message="GraphQL errors occurred")

            domain_urn = result["data"]["createDomain"]
            return self._create_success_result("create_domain", domain_urn, result)

        except Exception as e:
            self.logger.error(f"Error creating domain: {e}")
            return self._create_error_result("create_domain", error_message=str(e))

    def update_entity(self, entity_urn: str, entity_data: Dict[str, Any]) -> OperationResult:
        """Update an existing domain."""
        try:
            # Update description if provided
            if "description" in entity_data:
                result = self.execute_graphql(
                    UPDATE_DOMAIN_DESCRIPTION_MUTATION,
                    {
                        "input": {
                            "domainUrn": entity_urn,
                            "description": entity_data["description"]
                        }
                    }
                )

                if not self._check_graphql_errors(result):
                    return self._create_error_result("update_domain", entity_urn, "Failed to update description")

            # Update parent domain if provided
            if "parentDomain" in entity_data:
                parent_result = self.execute_graphql(
                    SET_DOMAIN_PARENT_MUTATION,
                    {
                        "input": {
                            "domainUrn": entity_urn,
                            "parentDomainUrn": entity_data["parentDomain"]
                        }
                    }
                )

                if not self._check_graphql_errors(parent_result):
                    return self._create_error_result("update_domain", entity_urn, "Failed to update parent domain")

            return self._create_success_result("update_domain", entity_urn)

        except Exception as e:
            self.logger.error(f"Error updating domain: {e}")
            return self._create_error_result("update_domain", entity_urn, str(e))

    def delete_entity(self, entity_urn: str) -> OperationResult:
        """Delete a domain."""
        try:
            result = self.execute_graphql(
                DELETE_DOMAIN_MUTATION,
                {"domainUrn": entity_urn}
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("delete_domain", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("delete_domain", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error deleting domain: {e}")
            return self._create_error_result("delete_domain", entity_urn, str(e))

    def add_owner(self, entity_urn: str, owner_urn: str, ownership_type: str) -> OperationResult:
        """Add owner to domain."""
        try:
            result = self.execute_graphql(
                ADD_DOMAIN_OWNER_MUTATION,
                {
                    "input": {
                        "domainUrn": entity_urn,
                        "owners": [{
                            "ownerUrn": owner_urn,
                            "type": ownership_type
                        }]
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("add_domain_owner", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("add_domain_owner", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error adding domain owner: {e}")
            return self._create_error_result("add_domain_owner", entity_urn, str(e))

    def remove_owner(self, entity_urn: str, owner_urn: str, ownership_type: str) -> OperationResult:
        """Remove owner from domain."""
        try:
            result = self.execute_graphql(
                REMOVE_DOMAIN_OWNER_MUTATION,
                {
                    "input": {
                        "domainUrn": entity_urn,
                        "ownerUrn": owner_urn
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("remove_domain_owner", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("remove_domain_owner", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error removing domain owner: {e}")
            return self._create_error_result("remove_domain_owner", entity_urn, str(e))

    def assign_to_entity(self, entity_urn: str, domain_urn: str, **kwargs) -> OperationResult:
        """Assign domain to entity."""
        try:
            result = self.execute_graphql(
                SET_ENTITY_DOMAIN_MUTATION,
                {
                    "input": {
                        "entityUrn": entity_urn,
                        "domainUrn": domain_urn
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("set_entity_domain", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("set_entity_domain", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error setting entity domain: {e}")
            return self._create_error_result("set_entity_domain", entity_urn, str(e))

    def remove_from_entity(self, entity_urn: str, domain_urn: str) -> OperationResult:
        """Remove domain from entity."""
        try:
            result = self.execute_graphql(
                UNSET_ENTITY_DOMAIN_MUTATION,
                {
                    "input": {
                        "entityUrn": entity_urn
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("unset_entity_domain", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("unset_entity_domain", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error unsetting entity domain: {e}")
            return self._create_error_result("unset_entity_domain", entity_urn, str(e))
