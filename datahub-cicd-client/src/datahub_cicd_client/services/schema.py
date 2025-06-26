"""
Schema service for DataHub CI/CD operations.
"""

import logging
from typing import Any, Dict, Optional

from datahub_cicd_client.core.base_client import BaseDataHubClient
from datahub_cicd_client.graphql.queries.schema import (
    GET_ENTITY_SCHEMA_QUERY,
)


class SchemaService(BaseDataHubClient):
    """Service for schema-related operations."""

    def __init__(self, connection):
        super().__init__(connection)
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_entity_schema(self, entity_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get schema details for an entity.

        Args:
            entity_urn: The URN of the entity to get schema for

        Returns:
            Schema data or None if not found
        """
        self.logger.info(f"Getting schema for entity: {entity_urn}")

        variables = {"urn": entity_urn}

        try:
            data = self.safe_execute_graphql(GET_ENTITY_SCHEMA_QUERY, variables)

            if data and "entity" in data:
                entity = data["entity"]
                self.logger.info(f"Retrieved schema for entity: {entity_urn}")
                return entity
            else:
                self.logger.warning(f"No schema data found for entity: {entity_urn}")
                return None

        except Exception as e:
            self.logger.error(f"Error getting schema for entity {entity_urn}: {str(e)}")
            return None
