"""
Synchronous output operations for tags using GraphQL mutations.
"""

from typing import Any, Dict

from datahub_cicd_client.core.connection import DataHubConnection
from datahub_cicd_client.graphql.mutations.tags import *
from datahub_cicd_client.outputs.sync.base_sync_output import (
    BaseSyncOutput,
    EntityRelationshipSyncOutput,
    MetadataAssignmentSyncOutput,
)
from datahub_cicd_client.services.base_service import OperationResult


class TagSyncOutput(BaseSyncOutput, EntityRelationshipSyncOutput, MetadataAssignmentSyncOutput):
    """Synchronous output operations for tags."""

    def __init__(self, connection: DataHubConnection):
        super().__init__(connection)

    def create_entity(self, entity_data: Dict[str, Any]) -> OperationResult:
        """Create a new tag."""
        try:
            result = self.execute_graphql(
                CREATE_TAG_MUTATION,
                {
                    "input": {
                        "id": entity_data["id"],
                        "name": entity_data["name"],
                        "description": entity_data.get("description", ""),
                        "colorHex": entity_data.get("colorHex")
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("create_tag", error_message="GraphQL errors occurred")

            tag_urn = result["data"]["createTag"]
            return self._create_success_result("create_tag", tag_urn, result)

        except Exception as e:
            self.logger.error(f"Error creating tag: {e}")
            return self._create_error_result("create_tag", error_message=str(e))

    def update_entity(self, entity_urn: str, entity_data: Dict[str, Any]) -> OperationResult:
        """Update an existing tag."""
        try:
            # Update description if provided
            if "description" in entity_data:
                result = self.execute_graphql(
                    UPDATE_TAG_DESCRIPTION_MUTATION,
                    {
                        "input": {
                            "tagUrn": entity_urn,
                            "description": entity_data["description"]
                        }
                    }
                )

                if not self._check_graphql_errors(result):
                    return self._create_error_result("update_tag", entity_urn, "Failed to update description")

            # Update color if provided
            if "colorHex" in entity_data:
                color_result = self.set_tag_color(entity_urn, entity_data["colorHex"])
                if not color_result.success:
                    return color_result

            return self._create_success_result("update_tag", entity_urn)

        except Exception as e:
            self.logger.error(f"Error updating tag: {e}")
            return self._create_error_result("update_tag", entity_urn, str(e))

    def delete_entity(self, entity_urn: str) -> OperationResult:
        """Delete a tag."""
        try:
            result = self.execute_graphql(
                DELETE_TAG_MUTATION,
                {"tagUrn": entity_urn}
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("delete_tag", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("delete_tag", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error deleting tag: {e}")
            return self._create_error_result("delete_tag", entity_urn, str(e))

    def set_tag_color(self, tag_urn: str, color_hex: str) -> OperationResult:
        """Set tag color."""
        try:
            result = self.execute_graphql(
                SET_TAG_COLOR_MUTATION,
                {
                    "input": {
                        "tagUrn": tag_urn,
                        "colorHex": color_hex
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("set_tag_color", tag_urn, "GraphQL errors occurred")

            return self._create_success_result("set_tag_color", tag_urn, result)

        except Exception as e:
            self.logger.error(f"Error setting tag color: {e}")
            return self._create_error_result("set_tag_color", tag_urn, str(e))

    def add_owner(self, entity_urn: str, owner_urn: str, ownership_type: str) -> OperationResult:
        """Add owner to tag."""
        try:
            result = self.execute_graphql(
                ADD_TAG_OWNER_MUTATION,
                {
                    "input": {
                        "tagUrn": entity_urn,
                        "owners": [{
                            "ownerUrn": owner_urn,
                            "type": ownership_type
                        }]
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("add_tag_owner", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("add_tag_owner", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error adding tag owner: {e}")
            return self._create_error_result("add_tag_owner", entity_urn, str(e))

    def remove_owner(self, entity_urn: str, owner_urn: str, ownership_type: str) -> OperationResult:
        """Remove owner from tag."""
        try:
            result = self.execute_graphql(
                REMOVE_TAG_OWNER_MUTATION,
                {
                    "input": {
                        "tagUrn": entity_urn,
                        "ownerUrn": owner_urn
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("remove_tag_owner", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("remove_tag_owner", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error removing tag owner: {e}")
            return self._create_error_result("remove_tag_owner", entity_urn, str(e))

    def assign_to_entity(self, entity_urn: str, tag_urn: str, **kwargs) -> OperationResult:
        """Assign tag to entity."""
        try:
            result = self.execute_graphql(
                ADD_TAG_TO_ENTITY_MUTATION,
                {
                    "input": {
                        "resourceUrn": entity_urn,
                        "tagUrns": [tag_urn]
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("add_tag_to_entity", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("add_tag_to_entity", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error adding tag to entity: {e}")
            return self._create_error_result("add_tag_to_entity", entity_urn, str(e))

    def remove_from_entity(self, entity_urn: str, tag_urn: str) -> OperationResult:
        """Remove tag from entity."""
        try:
            result = self.execute_graphql(
                REMOVE_TAG_FROM_ENTITY_MUTATION,
                {
                    "input": {
                        "resourceUrn": entity_urn,
                        "tagUrn": tag_urn
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("remove_tag_from_entity", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("remove_tag_from_entity", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error removing tag from entity: {e}")
            return self._create_error_result("remove_tag_from_entity", entity_urn, str(e))
