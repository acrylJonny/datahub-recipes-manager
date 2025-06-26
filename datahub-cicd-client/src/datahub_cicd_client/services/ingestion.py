"""
Ingestion service for DataHub operations.
Handles ingestion sources, execution requests, and related operations.
"""

import json
import uuid
from typing import Any, Dict, List, Optional, Union

from datahub_cicd_client.core.base_client import BaseDataHubClient
from datahub_cicd_client.core.exceptions import DataHubError
from datahub_cicd_client.graphql.mutations.ingestion import (
    CREATE_INGESTION_EXECUTION_REQUEST_MUTATION,
    CREATE_INGESTION_SOURCE_MUTATION,
    DELETE_INGESTION_SOURCE_MUTATION,
    EXECUTE_INGESTION_SOURCE_MUTATION,
    UPDATE_INGESTION_SOURCE_MUTATION,
)
from datahub_cicd_client.graphql.queries.ingestion import (
    COUNT_INGESTION_SOURCES_QUERY,
    FIND_INGESTION_SOURCES_BY_PLATFORM_QUERY,
    FIND_INGESTION_SOURCES_BY_TYPE_QUERY,
    GET_EXECUTION_REQUEST_QUERY,
    GET_INGESTION_EXECUTIONS_QUERY,
    GET_INGESTION_SOURCE_QUERY,
    GET_INGESTION_SOURCE_STATS_QUERY,
    GET_INGESTION_SOURCE_WITH_EXECUTIONS_QUERY,
    LIST_INGESTION_SOURCES_QUERY,
    LIST_INGESTION_SOURCES_SIMPLE_QUERY,
)


class IngestionService(BaseDataHubClient):
    """Service for managing DataHub ingestion sources and executions."""

    def __init__(self, connection):
        super().__init__(connection)

    def _ensure_source_urn(self, source_id: str) -> str:
        """Ensure source ID is in URN format."""
        if source_id.startswith("urn:li:dataHubIngestionSource:"):
            return source_id
        return f"urn:li:dataHubIngestionSource:{source_id}"

    def _extract_source_id(self, source_urn: str) -> str:
        """Extract source ID from URN."""
        if source_urn.startswith("urn:li:dataHubIngestionSource:"):
            return source_urn.replace("urn:li:dataHubIngestionSource:", "")
        return source_urn

    def _parse_recipe(self, recipe_str: Optional[str]) -> Optional[Dict[str, Any]]:
        """Parse recipe string to dictionary."""
        if not recipe_str:
            return None

        try:
            return json.loads(recipe_str)
        except json.JSONDecodeError as e:
            self.logger.warning(f"Failed to parse recipe JSON: {e}")
            return {"raw": recipe_str}

    def _serialize_recipe(self, recipe: Union[Dict[str, Any], str]) -> str:
        """Serialize recipe to JSON string."""
        if isinstance(recipe, str):
            return recipe
        return json.dumps(recipe)

    def _process_ingestion_source(self, source_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process raw ingestion source data from GraphQL."""
        if not source_data:
            return {}

        # Extract basic information
        result = {
            "urn": source_data.get("urn"),
            "id": self._extract_source_id(source_data.get("urn", "")),
            "name": source_data.get("name"),
            "type": source_data.get("type"),
            "schedule": source_data.get("schedule", {}),
            "platform": source_data.get("platform", {})
        }

        # Process config
        config = source_data.get("config", {}) or {}
        result["config"] = {
            "executorId": config.get("executorId", "default"),
            "debugMode": config.get("debugMode", False),
            "version": config.get("version", "0.8.42"),
            "extraArgs": config.get("extraArgs", [])
        }

        # Parse recipe
        recipe_str = config.get("recipe")
        result["recipe"] = self._parse_recipe(recipe_str)

        # Process executions if available
        executions = source_data.get("executions", {})
        if executions and executions.get("executionRequests"):
            exec_requests = executions.get("executionRequests", [])
            if exec_requests:
                latest_execution = exec_requests[0]
                result["last_execution"] = {
                    "id": latest_execution.get("id"),
                    "status": latest_execution.get("result", {}).get("status"),
                    "startTimeMs": latest_execution.get("result", {}).get("startTimeMs"),
                    "durationMs": latest_execution.get("result", {}).get("durationMs"),
                    "requestedAt": latest_execution.get("input", {}).get("requestedAt"),
                    "actorUrn": latest_execution.get("input", {}).get("actorUrn")
                }

        return result

    # Core CRUD Operations

    def list_ingestion_sources(
        self,
        start: int = 0,
        count: int = 100,
        include_executions: bool = True,
        filters: Optional[List[Dict[str, Any]]] = None
    ) -> List[Dict[str, Any]]:
        """
        List all ingestion sources.

        Args:
            start: Starting offset for pagination
            count: Number of sources to return
            include_executions: Whether to include execution history
            filters: Optional filters to apply

        Returns:
            List of ingestion source dictionaries
        """
        # Default filters exclude system sources
        if filters is None:
            filters = [{"field": "sourceType", "values": ["SYSTEM"], "negated": True}]

        variables = {
            "input": {
                "start": start,
                "count": count,
                "filters": filters
            }
        }

        query = LIST_INGESTION_SOURCES_QUERY if include_executions else LIST_INGESTION_SOURCES_SIMPLE_QUERY

        try:
            result = self.safe_execute_graphql(query, variables)

            if not self._log_graphql_errors(result):
                return []

            data = result.get("data", {}).get("listIngestionSources", {})
            if not data:
                return []

            raw_sources = data.get("ingestionSources", []) or []
            return [self._process_ingestion_source(source) for source in raw_sources]

        except Exception as e:
            self.logger.error(f"Error listing ingestion sources: {e}")
            raise DataHubError(f"Failed to list ingestion sources: {e}")

    def get_ingestion_source(
        self,
        source_id: str,
        include_executions: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Get a specific ingestion source by ID.

        Args:
            source_id: Source ID or URN
            include_executions: Whether to include execution history

        Returns:
            Ingestion source dictionary or None if not found
        """
        source_urn = self._ensure_source_urn(source_id)

        variables = {"urn": source_urn}
        query = GET_INGESTION_SOURCE_WITH_EXECUTIONS_QUERY if include_executions else GET_INGESTION_SOURCE_QUERY

        try:
            result = self.safe_execute_graphql(query, variables)

            if not self._log_graphql_errors(result):
                return None

            source_data = result.get("data", {}).get("ingestionSource")
            if not source_data:
                return None

            return self._process_ingestion_source(source_data)

        except Exception as e:
            self.logger.error(f"Error getting ingestion source {source_id}: {e}")
            return None

    def create_ingestion_source(
        self,
        recipe: Union[Dict[str, Any], str],
        name: Optional[str] = None,
        source_type: Optional[str] = None,
        schedule_interval: str = "0 0 * * *",
        timezone: str = "UTC",
        executor_id: str = "default",
        source_id: Optional[str] = None,
        debug_mode: bool = False,
        extra_args: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Optional[Dict[str, Any]]:
        """
        Create a new ingestion source.

        Args:
            recipe: Recipe configuration (dict or JSON string)
            name: Source name (auto-generated if not provided)
            source_type: Source type (inferred from recipe if not provided)
            schedule_interval: Cron expression for schedule
            timezone: Timezone for schedule
            executor_id: Executor ID
            source_id: Custom source ID (auto-generated if not provided)
            debug_mode: Enable debug mode
            extra_args: Additional arguments
            **kwargs: Additional parameters for backward compatibility

        Returns:
            Created source information or None if failed
        """
        # Handle backward compatibility parameters
        if kwargs.get("type"):
            source_type = kwargs["type"]
        if kwargs.get("schedule"):
            if isinstance(kwargs["schedule"], dict):
                schedule_interval = kwargs["schedule"].get("interval", schedule_interval)
                timezone = kwargs["schedule"].get("timezone", timezone)
            elif isinstance(kwargs["schedule"], str):
                schedule_interval = kwargs["schedule"]

        # Parse recipe
        if isinstance(recipe, str):
            try:
                recipe_dict = json.loads(recipe)
            except json.JSONDecodeError:
                raise DataHubError("Invalid recipe JSON format")
        else:
            recipe_dict = recipe

        # Auto-generate missing values
        if not source_id:
            source_id = str(uuid.uuid4())

        if not name:
            name = recipe_dict.get("source", {}).get("config", {}).get("name", f"source-{source_id}")

        if not source_type:
            source_type = recipe_dict.get("source", {}).get("type", "unknown")

        # Prepare input
        recipe_str = self._serialize_recipe(recipe)

        variables = {
            "input": {
                "id": source_id,
                "name": name,
                "type": source_type,
                "schedule": {
                    "interval": schedule_interval,
                    "timezone": timezone
                },
                "config": {
                    "recipe": recipe_str,
                    "executorId": executor_id,
                    "debugMode": debug_mode
                }
            }
        }

        if extra_args:
            variables["input"]["config"]["extraArgs"] = [
                {"key": k, "value": str(v)} for k, v in extra_args.items()
            ]

        try:
            result = self.safe_execute_graphql(CREATE_INGESTION_SOURCE_MUTATION, variables)

            if not self._log_graphql_errors(result):
                return None

            created_urn = result.get("data", {}).get("createIngestionSource")
            if created_urn:
                return {
                    "urn": created_urn,
                    "id": source_id,
                    "name": name,
                    "type": source_type,
                    "status": "created",
                    "config": {
                        "recipe": recipe_str,
                        "executorId": executor_id,
                        "debugMode": debug_mode,
                        "extraArgs": extra_args
                    }
                }

            return None

        except Exception as e:
            self.logger.error(f"Error creating ingestion source: {e}")
            raise DataHubError(f"Failed to create ingestion source: {e}")

    def update_ingestion_source(
        self,
        source_id: str,
        name: Optional[str] = None,
        recipe: Optional[Union[Dict[str, Any], str]] = None,
        schedule_interval: Optional[str] = None,
        timezone: Optional[str] = None,
        executor_id: Optional[str] = None,
        debug_mode: Optional[bool] = None,
        extra_args: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Update an existing ingestion source.

        Args:
            source_id: Source ID or URN
            name: New name
            recipe: New recipe configuration
            schedule_interval: New schedule interval
            timezone: New timezone
            executor_id: New executor ID
            debug_mode: New debug mode setting
            extra_args: New extra arguments

        Returns:
            Updated source information or None if failed
        """
        source_urn = self._ensure_source_urn(source_id)

        # Build update input
        update_input = {}

        if name is not None:
            update_input["name"] = name

        if recipe is not None:
            recipe_str = self._serialize_recipe(recipe)
            if "config" not in update_input:
                update_input["config"] = {}
            update_input["config"]["recipe"] = recipe_str

        if schedule_interval is not None or timezone is not None:
            update_input["schedule"] = {}
            if schedule_interval is not None:
                update_input["schedule"]["interval"] = schedule_interval
            if timezone is not None:
                update_input["schedule"]["timezone"] = timezone

        if executor_id is not None:
            if "config" not in update_input:
                update_input["config"] = {}
            update_input["config"]["executorId"] = executor_id

        if debug_mode is not None:
            if "config" not in update_input:
                update_input["config"] = {}
            update_input["config"]["debugMode"] = debug_mode

        if extra_args is not None:
            if "config" not in update_input:
                update_input["config"] = {}
            update_input["config"]["extraArgs"] = [
                {"key": k, "value": str(v)} for k, v in extra_args.items()
            ]

        if not update_input:
            self.logger.warning("No updates provided for ingestion source")
            return self.get_ingestion_source(source_id)

        variables = {
            "urn": source_urn,
            "input": update_input
        }

        try:
            result = self.safe_execute_graphql(UPDATE_INGESTION_SOURCE_MUTATION, variables)

            if not self._log_graphql_errors(result):
                return None

            # Return updated source
            return self.get_ingestion_source(source_id)

        except Exception as e:
            self.logger.error(f"Error updating ingestion source {source_id}: {e}")
            raise DataHubError(f"Failed to update ingestion source: {e}")

    def delete_ingestion_source(self, source_id: str) -> bool:
        """
        Delete an ingestion source.

        Args:
            source_id: Source ID or URN

        Returns:
            True if successful, False otherwise
        """
        source_urn = self._ensure_source_urn(source_id)

        variables = {"urn": source_urn}

        try:
            result = self.safe_execute_graphql(DELETE_INGESTION_SOURCE_MUTATION, variables)

            if not self._log_graphql_errors(result):
                return False

            self.logger.info(f"Successfully deleted ingestion source: {source_id}")
            return True

        except Exception as e:
            self.logger.error(f"Error deleting ingestion source {source_id}: {e}")
            return False

    # Execution Management

    def trigger_ingestion(self, source_id: str) -> bool:
        """
        Trigger immediate execution of an ingestion source.

        Args:
            source_id: Source ID or URN

        Returns:
            True if successful, False otherwise
        """
        source_urn = self._ensure_source_urn(source_id)

        variables = {
            "input": {
                "ingestionSourceUrn": source_urn
            }
        }

        try:
            # Try primary method first
            result = self.safe_execute_graphql(CREATE_INGESTION_EXECUTION_REQUEST_MUTATION, variables)

            if self._log_graphql_errors(result):
                self.logger.info(f"Successfully triggered ingestion for source: {source_id}")
                return True

            # Fallback to legacy method
            self.logger.debug("Trying legacy execution method...")
            legacy_variables = {"input": {"urn": source_urn}}
            result = self.safe_execute_graphql(EXECUTE_INGESTION_SOURCE_MUTATION, legacy_variables)

            if self._log_graphql_errors(result):
                self.logger.info(f"Successfully triggered ingestion using legacy method: {source_id}")
                return True

            return False

        except Exception as e:
            self.logger.error(f"Error triggering ingestion for {source_id}: {e}")
            return False

    def get_ingestion_executions(
        self,
        source_id: str,
        start: int = 0,
        count: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get execution history for an ingestion source.

        Args:
            source_id: Source ID or URN
            start: Starting offset
            count: Number of executions to return

        Returns:
            List of execution dictionaries
        """
        source_urn = self._ensure_source_urn(source_id)

        variables = {
            "urn": source_urn,
            "start": start,
            "count": count
        }

        try:
            result = self.safe_execute_graphql(GET_INGESTION_EXECUTIONS_QUERY, variables)

            if not self._log_graphql_errors(result):
                return []

            source_data = result.get("data", {}).get("ingestionSource", {})
            executions = source_data.get("executions", {})

            if not executions:
                return []

            execution_requests = executions.get("executionRequests", []) or []

            return [
                {
                    "id": req.get("id"),
                    "urn": req.get("urn"),
                    "status": req.get("result", {}).get("status"),
                    "startTimeMs": req.get("result", {}).get("startTimeMs"),
                    "durationMs": req.get("result", {}).get("durationMs"),
                    "requestedAt": req.get("input", {}).get("requestedAt"),
                    "actorUrn": req.get("input", {}).get("actorUrn"),
                    "structuredReport": req.get("result", {}).get("structuredReport")
                }
                for req in execution_requests
            ]

        except Exception as e:
            self.logger.error(f"Error getting executions for {source_id}: {e}")
            return []

    def get_execution_request(self, execution_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get details of a specific execution request.

        Args:
            execution_urn: Execution request URN

        Returns:
            Execution request details or None if not found
        """
        variables = {"urn": execution_urn}

        try:
            result = self.safe_execute_graphql(GET_EXECUTION_REQUEST_QUERY, variables)

            if not self._log_graphql_errors(result):
                return None

            execution_data = result.get("data", {}).get("executionRequest")
            if not execution_data:
                return None

            return {
                "id": execution_data.get("id"),
                "urn": execution_data.get("urn"),
                "status": execution_data.get("result", {}).get("status"),
                "startTimeMs": execution_data.get("result", {}).get("startTimeMs"),
                "durationMs": execution_data.get("result", {}).get("durationMs"),
                "requestedAt": execution_data.get("input", {}).get("requestedAt"),
                "actorUrn": execution_data.get("input", {}).get("actorUrn"),
                "structuredReport": execution_data.get("result", {}).get("structuredReport")
            }

        except Exception as e:
            self.logger.error(f"Error getting execution request {execution_urn}: {e}")
            return None

    # Search and Filter Operations

    def find_ingestion_sources_by_platform(
        self,
        platform_urn: str,
        start: int = 0,
        count: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Find ingestion sources by platform.

        Args:
            platform_urn: Platform URN to filter by
            start: Starting offset
            count: Number of sources to return

        Returns:
            List of matching ingestion sources
        """
        filters = [
            {"field": "platform", "values": [platform_urn]},
            {"field": "sourceType", "values": ["SYSTEM"], "negated": True}
        ]

        variables = {
            "input": {
                "start": start,
                "count": count,
                "filters": filters
            }
        }

        try:
            result = self.safe_execute_graphql(FIND_INGESTION_SOURCES_BY_PLATFORM_QUERY, variables)

            if not self._log_graphql_errors(result):
                return []

            data = result.get("data", {}).get("listIngestionSources", {})
            if not data:
                return []

            raw_sources = data.get("ingestionSources", []) or []
            return [self._process_ingestion_source(source) for source in raw_sources]

        except Exception as e:
            self.logger.error(f"Error finding sources by platform {platform_urn}: {e}")
            return []

    def find_ingestion_sources_by_type(
        self,
        source_type: str,
        start: int = 0,
        count: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Find ingestion sources by type.

        Args:
            source_type: Source type to filter by
            start: Starting offset
            count: Number of sources to return

        Returns:
            List of matching ingestion sources
        """
        filters = [
            {"field": "type", "values": [source_type]},
            {"field": "sourceType", "values": ["SYSTEM"], "negated": True}
        ]

        variables = {
            "input": {
                "start": start,
                "count": count,
                "filters": filters
            }
        }

        try:
            result = self.safe_execute_graphql(FIND_INGESTION_SOURCES_BY_TYPE_QUERY, variables)

            if not self._log_graphql_errors(result):
                return []

            data = result.get("data", {}).get("listIngestionSources", {})
            if not data:
                return []

            raw_sources = data.get("ingestionSources", []) or []
            return [self._process_ingestion_source(source) for source in raw_sources]

        except Exception as e:
            self.logger.error(f"Error finding sources by type {source_type}: {e}")
            return []

    # Statistics and Monitoring

    def get_ingestion_source_stats(self, source_id: str) -> Dict[str, Any]:
        """
        Get statistics for an ingestion source.

        Args:
            source_id: Source ID or URN

        Returns:
            Statistics dictionary with execution counts and status
        """
        source_urn = self._ensure_source_urn(source_id)

        variables = {"urn": source_urn}

        try:
            result = self.safe_execute_graphql(GET_INGESTION_SOURCE_STATS_QUERY, variables)

            if not self._log_graphql_errors(result):
                return {}

            source_data = result.get("data", {}).get("ingestionSource", {})
            if not source_data:
                return {}

            executions = source_data.get("executions", {})
            execution_requests = executions.get("executionRequests", []) or []

            # Calculate statistics
            total_executions = executions.get("total", 0)
            successful_executions = 0
            failed_executions = 0
            running_executions = 0
            total_duration = 0

            for req in execution_requests:
                result_data = req.get("result", {})
                status = result_data.get("status", "UNKNOWN")
                duration = result_data.get("durationMs", 0)

                if status == "SUCCESS":
                    successful_executions += 1
                elif status == "FAILURE":
                    failed_executions += 1
                elif status in ["RUNNING", "PENDING"]:
                    running_executions += 1

                if duration:
                    total_duration += duration

            avg_duration = total_duration / len(execution_requests) if execution_requests else 0

            return {
                "source_urn": source_urn,
                "source_name": source_data.get("name"),
                "source_type": source_data.get("type"),
                "total_executions": total_executions,
                "successful_executions": successful_executions,
                "failed_executions": failed_executions,
                "running_executions": running_executions,
                "success_rate": successful_executions / total_executions if total_executions > 0 else 0,
                "average_duration_ms": avg_duration,
                "last_execution": execution_requests[0] if execution_requests else None
            }

        except Exception as e:
            self.logger.error(f"Error getting stats for {source_id}: {e}")
            return {}

    def count_ingestion_sources(self, filters: Optional[List[Dict[str, Any]]] = None) -> int:
        """
        Count total number of ingestion sources.

        Args:
            filters: Optional filters to apply

        Returns:
            Total count of sources
        """
        if filters is None:
            filters = [{"field": "sourceType", "values": ["SYSTEM"], "negated": True}]

        variables = {
            "input": {
                "start": 0,
                "count": 1,
                "filters": filters
            }
        }

        try:
            result = self.safe_execute_graphql(COUNT_INGESTION_SOURCES_QUERY, variables)

            if not self._log_graphql_errors(result):
                return 0

            data = result.get("data", {}).get("listIngestionSources", {})
            return data.get("total", 0) if data else 0

        except Exception as e:
            self.logger.error(f"Error counting ingestion sources: {e}")
            return 0

    # Convenience Methods

    def patch_ingestion_source(
        self,
        source_id: str,
        **kwargs
    ) -> Optional[Dict[str, Any]]:
        """
        Patch (partially update) an ingestion source.
        This is an alias for update_ingestion_source for backward compatibility.

        Args:
            source_id: Source ID or URN
            **kwargs: Fields to update

        Returns:
            Updated source information or None if failed
        """
        return self.update_ingestion_source(source_id, **kwargs)

    def run_ingestion_source(self, source_id: str) -> bool:
        """
        Run an ingestion source immediately.
        This is an alias for trigger_ingestion for backward compatibility.

        Args:
            source_id: Source ID or URN

        Returns:
            True if successful, False otherwise
        """
        return self.trigger_ingestion(source_id)
