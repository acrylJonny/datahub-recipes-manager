"""
Synchronous output operations for glossary using GraphQL mutations.
"""

from typing import Any, Dict

from datahub_cicd_client.core.connection import DataHubConnection
from datahub_cicd_client.graphql.mutations.glossary import *
from datahub_cicd_client.outputs.sync.base_sync_output import (
    BaseSyncOutput,
    EntityRelationshipSyncOutput,
    MetadataAssignmentSyncOutput,
)
from datahub_cicd_client.services.base_service import OperationResult


class GlossarySyncOutput(BaseSyncOutput, EntityRelationshipSyncOutput, MetadataAssignmentSyncOutput):
    """Synchronous output operations for glossary entities."""

    def __init__(self, connection: DataHubConnection):
        super().__init__(connection)

    def create_entity(self, entity_data: Dict[str, Any]) -> OperationResult:
        """Create a new glossary entity (node or term)."""
        try:
            entity_type = entity_data.get("entity_type", "term")

            if entity_type == "node":
                return self.create_glossary_node(entity_data)
            else:
                return self.create_glossary_term(entity_data)

        except Exception as e:
            self.logger.error(f"Error creating glossary entity: {e}")
            return self._create_error_result("create_glossary_entity", error_message=str(e))

    def create_glossary_node(self, entity_data: Dict[str, Any]) -> OperationResult:
        """Create a new glossary node."""
        try:
            result = self.execute_graphql(
                CREATE_GLOSSARY_NODE_MUTATION,
                {
                    "input": {
                        "id": entity_data["id"],
                        "name": entity_data["name"],
                        "description": entity_data.get("description", ""),
                        "parentNode": entity_data.get("parentNode")
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("create_glossary_node", error_message="GraphQL errors occurred")

            node_urn = result["data"]["createGlossaryNode"]
            return self._create_success_result("create_glossary_node", node_urn, result)

        except Exception as e:
            self.logger.error(f"Error creating glossary node: {e}")
            return self._create_error_result("create_glossary_node", error_message=str(e))

    def create_glossary_term(self, entity_data: Dict[str, Any]) -> OperationResult:
        """Create a new glossary term."""
        try:
            result = self.execute_graphql(
                CREATE_GLOSSARY_TERM_MUTATION,
                {
                    "input": {
                        "id": entity_data["id"],
                        "name": entity_data["name"],
                        "description": entity_data.get("description", ""),
                        "parentNode": entity_data.get("parentNode"),
                        "termSource": entity_data.get("termSource", "INTERNAL")
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("create_glossary_term", error_message="GraphQL errors occurred")

            term_urn = result["data"]["createGlossaryTerm"]
            return self._create_success_result("create_glossary_term", term_urn, result)

        except Exception as e:
            self.logger.error(f"Error creating glossary term: {e}")
            return self._create_error_result("create_glossary_term", error_message=str(e))

    def update_entity(self, entity_urn: str, entity_data: Dict[str, Any]) -> OperationResult:
        """Update an existing glossary entity."""
        try:
            entity_type = entity_urn.split(":")[2]  # Extract type from URN

            if entity_type == "glossaryNode":
                return self.update_glossary_node(entity_urn, entity_data)
            elif entity_type == "glossaryTerm":
                return self.update_glossary_term(entity_urn, entity_data)
            else:
                return self._create_error_result("update_glossary_entity", entity_urn, f"Unknown entity type: {entity_type}")

        except Exception as e:
            self.logger.error(f"Error updating glossary entity: {e}")
            return self._create_error_result("update_glossary_entity", entity_urn, str(e))

    def update_glossary_node(self, node_urn: str, entity_data: Dict[str, Any]) -> OperationResult:
        """Update a glossary node."""
        try:
            if "description" in entity_data:
                result = self.execute_graphql(
                    UPDATE_GLOSSARY_NODE_DESCRIPTION_MUTATION,
                    {
                        "input": {
                            "nodeUrn": node_urn,
                            "description": entity_data["description"]
                        }
                    }
                )

                if not self._check_graphql_errors(result):
                    return self._create_error_result("update_glossary_node", node_urn, "Failed to update description")

            return self._create_success_result("update_glossary_node", node_urn)

        except Exception as e:
            self.logger.error(f"Error updating glossary node: {e}")
            return self._create_error_result("update_glossary_node", node_urn, str(e))

    def update_glossary_term(self, term_urn: str, entity_data: Dict[str, Any]) -> OperationResult:
        """Update a glossary term."""
        try:
            if "description" in entity_data:
                result = self.execute_graphql(
                    UPDATE_GLOSSARY_TERM_DESCRIPTION_MUTATION,
                    {
                        "input": {
                            "termUrn": term_urn,
                            "description": entity_data["description"]
                        }
                    }
                )

                if not self._check_graphql_errors(result):
                    return self._create_error_result("update_glossary_term", term_urn, "Failed to update description")

            return self._create_success_result("update_glossary_term", term_urn)

        except Exception as e:
            self.logger.error(f"Error updating glossary term: {e}")
            return self._create_error_result("update_glossary_term", term_urn, str(e))

    def delete_entity(self, entity_urn: str) -> OperationResult:
        """Delete a glossary entity."""
        try:
            entity_type = entity_urn.split(":")[2]  # Extract type from URN

            if entity_type == "glossaryNode":
                mutation = DELETE_GLOSSARY_NODE_MUTATION
                variable_name = "nodeUrn"
            elif entity_type == "glossaryTerm":
                mutation = DELETE_GLOSSARY_TERM_MUTATION
                variable_name = "termUrn"
            else:
                return self._create_error_result("delete_glossary_entity", entity_urn, f"Unknown entity type: {entity_type}")

            result = self.execute_graphql(
                mutation,
                {variable_name: entity_urn}
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("delete_glossary_entity", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("delete_glossary_entity", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error deleting glossary entity: {e}")
            return self._create_error_result("delete_glossary_entity", entity_urn, str(e))

    def add_owner(self, entity_urn: str, owner_urn: str, ownership_type: str) -> OperationResult:
        """Add owner to glossary entity."""
        try:
            entity_type = entity_urn.split(":")[2]  # Extract type from URN

            if entity_type == "glossaryNode":
                mutation = ADD_GLOSSARY_NODE_OWNER_MUTATION
                variable_name = "nodeUrn"
            elif entity_type == "glossaryTerm":
                mutation = ADD_GLOSSARY_TERM_OWNER_MUTATION
                variable_name = "termUrn"
            else:
                return self._create_error_result("add_glossary_owner", entity_urn, f"Unknown entity type: {entity_type}")

            result = self.execute_graphql(
                mutation,
                {
                    "input": {
                        variable_name: entity_urn,
                        "owners": [{
                            "ownerUrn": owner_urn,
                            "type": ownership_type
                        }]
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("add_glossary_owner", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("add_glossary_owner", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error adding glossary owner: {e}")
            return self._create_error_result("add_glossary_owner", entity_urn, str(e))

    def remove_owner(self, entity_urn: str, owner_urn: str, ownership_type: str) -> OperationResult:
        """Remove owner from glossary entity."""
        try:
            entity_type = entity_urn.split(":")[2]  # Extract type from URN

            if entity_type == "glossaryNode":
                mutation = REMOVE_GLOSSARY_NODE_OWNER_MUTATION
                variable_name = "nodeUrn"
            elif entity_type == "glossaryTerm":
                mutation = REMOVE_GLOSSARY_TERM_OWNER_MUTATION
                variable_name = "termUrn"
            else:
                return self._create_error_result("remove_glossary_owner", entity_urn, f"Unknown entity type: {entity_type}")

            result = self.execute_graphql(
                mutation,
                {
                    "input": {
                        variable_name: entity_urn,
                        "ownerUrn": owner_urn
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("remove_glossary_owner", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("remove_glossary_owner", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error removing glossary owner: {e}")
            return self._create_error_result("remove_glossary_owner", entity_urn, str(e))

    def assign_to_entity(self, entity_urn: str, glossary_term_urn: str, **kwargs) -> OperationResult:
        """Assign glossary term to entity."""
        try:
            result = self.execute_graphql(
                ADD_GLOSSARY_TERM_TO_ENTITY_MUTATION,
                {
                    "input": {
                        "resourceUrn": entity_urn,
                        "glossaryTermUrns": [glossary_term_urn]
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("add_glossary_term_to_entity", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("add_glossary_term_to_entity", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error adding glossary term to entity: {e}")
            return self._create_error_result("add_glossary_term_to_entity", entity_urn, str(e))

    def remove_from_entity(self, entity_urn: str, glossary_term_urn: str) -> OperationResult:
        """Remove glossary term from entity."""
        try:
            result = self.execute_graphql(
                REMOVE_GLOSSARY_TERM_FROM_ENTITY_MUTATION,
                {
                    "input": {
                        "resourceUrn": entity_urn,
                        "glossaryTermUrn": glossary_term_urn
                    }
                }
            )

            if not self._check_graphql_errors(result):
                return self._create_error_result("remove_glossary_term_from_entity", entity_urn, "GraphQL errors occurred")

            return self._create_success_result("remove_glossary_term_from_entity", entity_urn, result)

        except Exception as e:
            self.logger.error(f"Error removing glossary term from entity: {e}")
            return self._create_error_result("remove_glossary_term_from_entity", entity_urn, str(e))
