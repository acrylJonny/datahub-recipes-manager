"""
Analytics service for DataHub aggregation operations.
"""

import logging
from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.base_client import BaseDataHubClient
from datahub_cicd_client.graphql.queries.analytics import (
    AGGREGATE_ACROSS_ENTITIES_QUERY,
)


class AnalyticsService(BaseDataHubClient):
    """Service for analytics and aggregation operations."""

    def __init__(self, connection):
        super().__init__(connection)
        self.logger = logging.getLogger(self.__class__.__name__)

    def aggregate_across_entities(
        self, query: str = "*", entity_types: List[str] = None, aggregation_types: List[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Aggregate data across entities.

        Args:
            query: Search query
            entity_types: List of entity types to aggregate
            aggregation_types: List of aggregation types to perform

        Returns:
            Dict with aggregation results or None if failed
        """
        self.logger.info(f"Aggregating across entities with query: {query}")

        variables = {
            "input": {
                "query": query,
                "types": entity_types or ["DATASET", "DASHBOARD", "CHART"],
                "start": 0,
                "count": 1000,
            }
        }

        try:
            data = self.safe_execute_graphql(AGGREGATE_ACROSS_ENTITIES_QUERY, variables)

            if data and "aggregateAcrossEntities" in data:
                result = data["aggregateAcrossEntities"]
                self.logger.info(f"Retrieved aggregation data for query: {query}")
                return result
            else:
                self.logger.warning(f"No aggregation data found for query: {query}")
                return None

        except Exception as e:
            self.logger.error(f"Error aggregating across entities: {str(e)}")
            return None
