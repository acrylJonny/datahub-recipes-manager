#!/usr/bin/env python3
"""
DataHub API client utilities that use the official acryl-datahub SDK.
This provides a more reliable and maintainable way to interact with DataHub.
"""

import logging
import json
import uuid
from typing import Dict, Any, List, Optional, Union, Set

from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DataHubIngestionSourceConfigClass,
    DataHubIngestionSourceInfoClass,
    DataHubIngestionSourceScheduleClass,
)
from datahub.configuration.common import ConfigModel

# Define a complete Config class for compatibility with the latest SDK
class DataHubConfig(ConfigModel):
    server: str
    token: Optional[str] = None
    timeout_sec: int = 30
    retry_status_codes: Set[int] = {429, 500, 502, 503, 504}
    retry_max_times: int = 3
    extra_headers: Dict[str, str] = {}
    ca_certificate_path: Optional[str] = None
    client_certificate_path: Optional[str] = None
    disable_ssl_verification: bool = False
    max_threads: int = 1

logger = logging.getLogger(__name__)


class DataHubClient:
    """
    Client for interacting with DataHub using the official SDK
    """

    def __init__(self, server_url: str, token: Optional[str] = None):
        """
        Initialize the DataHub client

        Args:
            server_url: DataHub GMS server URL
            token: DataHub authentication token (optional)
        """
        self.server_url = server_url.rstrip('/')
        self.token = token
        
        # Create a complete config object for DataHubGraph
        config = DataHubConfig(
            server=self.server_url,
            token=self.token,
            timeout_sec=30,
            retry_max_times=3
        )
        
        # Initialize the graph client
        try:
            # Latest SDK version approach
            self.graph = DataHubGraph(config)
            logger.info("Successfully initialized DataHub client with config object")
        except Exception as e:
            logger.warning(f"Error initializing DataHub client with config: {str(e)}")
            # Try alternate initialization approach - use requests directly
            logger.error(f"Failed to initialize DataHub client: {str(e)}")
            raise Exception(f"Could not initialize DataHub client. Error: {str(e)}")

    def create_ingestion_source(
            self,
            recipe: Dict[str, Any],
            name: str,
            source_type: str,
            schedule_interval: str = "0 0 * * *",
            timezone: str = "UTC",
            executor_id: str = "default",
            source_id: Optional[str] = None,
            debug_mode: bool = False,
            extra_args: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a DataHub ingestion source using the SDK

        Args:
            recipe: The ingestion recipe configuration
            name: Human-readable name for the ingestion source
            source_type: The type of source (e.g., mssql, snowflake)
            schedule_interval: Cron expression for ingestion schedule
            timezone: Timezone for the schedule
            executor_id: Executor ID (default, remote, etc.)
            source_id: Optional custom source ID
            debug_mode: Whether to enable debug mode
            extra_args: Additional arguments for the ingestion

        Returns:
            Dictionary with source information including URN
        """
        if source_id is None:
            source_id = str(uuid.uuid4())

        source_urn = f"urn:li:dataHubIngestionSource:{source_id}"

        # Create the config object
        ingestion_config = DataHubIngestionSourceConfigClass(
            recipe=json.dumps(recipe),
            executorId=executor_id,
            debugMode=debug_mode,
            extraArgs=extra_args or {},
        )

        # Create the schedule object
        schedule = DataHubIngestionSourceScheduleClass(
            interval=schedule_interval,
            timezone=timezone,
        )

        # Create the source info
        source_info = DataHubIngestionSourceInfoClass(
            name=name,
            type=source_type,
            config=ingestion_config,
            schedule=schedule,
        )

        # Create or update the ingestion source
        try:
            # First, check if the source already exists
            try:
                existing_source = self.graph.get_aspect(
                    entity_urn=source_urn,
                    aspect_type=DataHubIngestionSourceInfoClass,
                )
                if existing_source:
                    logger.info(f"Ingestion source {source_urn} already exists, updating it")
                    # Update the existing source
                    self.graph.update_aspect(
                        entity_urn=source_urn,
                        aspect=source_info,
                    )
                    return {
                        "urn": source_urn,
                        "status": "updated",
                        "name": name,
                        "type": source_type,
                    }
            except Exception as e:
                # Source doesn't exist, create it
                logger.info(f"Ingestion source {source_urn} doesn't exist, creating it")
                pass

            # Create the source
            self.graph.emit_mcp(
                entity_urn=source_urn,
                aspect_name="dataHubIngestionSourceInfo",
                aspect=source_info,
            )

            return {
                "urn": source_urn,
                "status": "created",
                "name": name,
                "type": source_type,
            }
        except Exception as e:
            logger.error(f"Error creating/updating ingestion source: {str(e)}")
            raise

    def get_ingestion_source(self, source_id: str) -> Optional[Dict[str, Any]]:
        """
        Get ingestion source by ID

        Args:
            source_id: Source ID

        Returns:
            Dictionary with source information or None if not found
        """
        source_urn = f"urn:li:dataHubIngestionSource:{source_id}"

        try:
            source_info = self.graph.get_aspect(
                entity_urn=source_urn,
                aspect_type=DataHubIngestionSourceInfoClass,
            )

            if not source_info:
                return None

            # Parse the recipe from JSON string to dict
            recipe = json.loads(source_info.config.recipe)

            return {
                "urn": source_urn,
                "id": source_id,
                "name": source_info.name,
                "type": source_info.type,
                "recipe": recipe,
                "schedule": {
                    "interval": source_info.schedule.interval,
                    "timezone": source_info.schedule.timezone,
                },
                "executor_id": source_info.config.executorId,
                "debug_mode": source_info.config.debugMode,
                "extra_args": source_info.config.extraArgs,
            }
        except Exception as e:
            logger.error(f"Error getting ingestion source: {str(e)}")
            return None

    def list_ingestion_sources(self) -> List[Dict[str, Any]]:
        """
        List all ingestion sources

        Returns:
            List of dictionaries with source information
        """
        try:
            # Query all ingestion sources
            query = """
            query listIngestionSources {
                listIngestionSources {
                    start
                    count
                    total
                    sources {
                        urn
                        type
                        name
                    }
                }
            }
            """

            result = self.graph.execute_graphql(query)
            sources_data = result["listIngestionSources"]["sources"]

            # Get detailed information for each source
            sources = []
            for source_data in sources_data:
                source_urn = source_data["urn"]
                source_id = source_urn.split(":")[-1]

                # Get detailed information
                source_info = self.get_ingestion_source(source_id)
                if source_info:
                    sources.append(source_info)

            return sources
        except Exception as e:
            logger.error(f"Error listing ingestion sources: {str(e)}")
            return []

    def update_ingestion_schedule(
            self,
            source_id: str,
            schedule_interval: str,
            timezone: str = "UTC"
    ) -> bool:
        """
        Update the schedule of an ingestion source

        Args:
            source_id: Source ID
            schedule_interval: Cron expression for ingestion schedule
            timezone: Timezone for the schedule

        Returns:
            True if successful, False otherwise
        """
        source_urn = f"urn:li:dataHubIngestionSource:{source_id}"

        try:
            # Get current source info
            source_info = self.graph.get_aspect(
                entity_urn=source_urn,
                aspect_type=DataHubIngestionSourceInfoClass,
            )

            if not source_info:
                logger.error(f"Ingestion source {source_urn} not found")
                return False

            # Update schedule
            source_info.schedule.interval = schedule_interval
            source_info.schedule.timezone = timezone

            # Update the source
            self.graph.update_aspect(
                entity_urn=source_urn,
                aspect=source_info,
            )

            return True
        except Exception as e:
            logger.error(f"Error updating ingestion schedule: {str(e)}")
            return False

    def trigger_ingestion(self, source_id: str) -> bool:
        """
        Trigger an immediate ingestion for a source

        Args:
            source_id: Source ID

        Returns:
            True if successful, False otherwise
        """
        source_urn = f"urn:li:dataHubIngestionSource:{source_id}"

        try:
            # Execute the graphql mutation
            query = """
            mutation createIngestionExecutionRequest($input: CreateIngestionExecutionRequestInput!) {
                createIngestionExecutionRequest(input: $input)
            }
            """

            variables = {
                "input": {
                    "ingestionSourceUrn": source_urn
                }
            }

            result = self.graph.execute_graphql(query, variables)

            return "createIngestionExecutionRequest" in result
        except Exception as e:
            logger.error(f"Error triggering ingestion: {str(e)}")
            return False

    def delete_ingestion_source(self, source_id: str) -> bool:
        """
        Delete an ingestion source

        Args:
            source_id: Source ID

        Returns:
            True if successful, False otherwise
        """
        source_urn = f"urn:li:dataHubIngestionSource:{source_id}"

        try:
            # Execute the graphql mutation
            query = """
            mutation deleteIngestionSource($urn: String!) {
                deleteIngestionSource(urn: $urn)
            }
            """

            variables = {
                "urn": source_urn
            }

            result = self.graph.execute_graphql(query, variables)

            return "deleteIngestionSource" in result
        except Exception as e:
            logger.error(f"Error deleting ingestion source: {str(e)}")
            return False