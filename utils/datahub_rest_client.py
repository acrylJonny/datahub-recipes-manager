#!/usr/bin/env python3
"""
DataHub REST client that uses direct HTTP requests to the DataHub API.
This approach avoids SDK compatibility issues by using the REST API directly.
Also supports DataHubGraph client for advanced GraphQL functionality.
"""

import json
import logging
import uuid
import requests
import time
from typing import Dict, Any, List, Optional, Union

# Add DataHubGraph client imports if available, with fallback
try:
    from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

    DATAHUB_SDK_AVAILABLE = True
except ImportError:
    DATAHUB_SDK_AVAILABLE = False

logger = logging.getLogger(__name__)


class DataHubRestClient:
    """
    Client for interacting with DataHub using direct REST API calls and Graph API.
    """

    def __init__(self, server_url: str, token: Optional[str] = None, verify_ssl=True, timeout=30):
        """
        Initialize the DataHub REST client

        Args:
            server_url: DataHub GMS server URL
            token: DataHub authentication token (optional)
            verify_ssl: Whether to verify SSL certificates (default: True)
            timeout: Request timeout in seconds (default: 30)
        """
        self.server_url = server_url.rstrip("/")
        self.token = token
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
        }

        # Create a session for reusing connections
        self._session = requests.Session()
        self._session.headers.update(self.headers)
        self._session.verify = self.verify_ssl

        # Add logger attribute
        self.logger = logging.getLogger(__name__)

        # Initialize schema validation flag to True by default
        # This will be set to False if GraphQL schema validation fails
        self._schema_validated = True

        if token:
            self.headers["Authorization"] = f"Bearer {token}"
            self._session.headers["Authorization"] = f"Bearer {token}"

        # Log connection details for debugging
        self.logger.debug(
            f"Initialized DataHub client with URL: {server_url}, token provided: {token is not None}, verify_ssl: {verify_ssl}"
        )

        # Initialize Graph client if SDK is available
        self.graph = None
        if DATAHUB_SDK_AVAILABLE:
            try:
                config = DatahubClientConfig(
                    server=server_url,
                    token=token,
                )
                # Add verify_ssl parameter if available in the SDK version
                if hasattr(config, "verify_ssl"):
                    config.verify_ssl = verify_ssl

                self.graph = DataHubGraph(config=config)
                # Also set verify_ssl on the graph client if available
                if hasattr(self.graph, "verify_ssl"):
                    self.graph.verify_ssl = verify_ssl

                logger.info("DataHubGraph client initialized successfully")
                logger.debug(f"DataHubGraph verify_ssl set to: {verify_ssl}")
            except Exception as e:
                logger.warning(f"Failed to initialize DataHubGraph client: {str(e)}")
                logger.warning("Advanced GraphQL functionality will not be available")

    def _get_auth_headers(self):
        """Return the authorization headers if a token is available"""
        if self.token:
            return {"Authorization": f"Bearer {self.token}", **self.headers}
        return self.headers

    def execute_graphql(self, query, variables=None):
        """
        Execute a GraphQL query against the DataHub GraphQL API.

        Args:
            query (str): The GraphQL query to execute
            variables (dict, optional): Variables for the GraphQL query

        Returns:
            dict: The GraphQL response
        """
        try:
            # Reduced logging - only log query name and key details for entity searches
            query_name = "Unknown"
            if "query " in query:
                query_name = query.split("query ")[1].split("(")[0].strip()
            elif "mutation " in query:
                query_name = query.split("mutation ")[1].split("(")[0].strip()
            
            # Only log minimal info to reduce clutter
            self.logger.debug(f"Executing GraphQL {query_name}")

            # Try with DataHubGraph client if available
            if hasattr(self, "dhg_client") and self.dhg_client:
                return self.dhg_client.execute_graphql(query, variables)

            # Fallback to direct HTTP request
            payload = {"query": query, "variables": variables or {}}
            graphql_url = f"{self.server_url}/api/graphql"

            response = self._session.post(
                graphql_url, json=payload, headers=self._get_auth_headers()
            )

            # Only log if there's an error
            if response.status_code != 200:
                self.logger.warning(f"GraphQL {query_name} failed with status {response.status_code}")
                return None

            result = response.json()
            # Only log GraphQL errors if they exist
            if "errors" in result:
                self.logger.warning(f"GraphQL {query_name} returned errors: {len(result['errors'])} error(s)")
                # Only log the first error message to avoid spam  
                if result['errors']:
                    self.logger.debug(f"First error: {result['errors'][0].get('message', 'Unknown error')}")
            return result
        except Exception as e:
            self.logger.error(f"Error executing GraphQL query: {str(e)}")
            return None

    def _execute_graphql(self, query, variables=None):
        """
        Private wrapper around execute_graphql method.

        Used internally by various methods to execute GraphQL queries and mutations.

        Args:
            query (str): The GraphQL query to execute
            variables (dict, optional): Variables for the GraphQL query

        Returns:
            The result data from the GraphQL response if successful, otherwise None
        """
        result = self.execute_graphql(query, variables)

        # Check if result is None
        if result is None:
            return None

        # Check for errors
        if result and "errors" in result and result["errors"]:
            error_msg = result["errors"][0].get("message", "Unknown GraphQL error")
            self.logger.error(f"Error in GraphQL query: {error_msg}")
            return None

        # Return the data from the response
        return result.get("data")

    def test_connection(self) -> bool:
        """
        Test basic connection to DataHub (lightweight test)

        Returns:
            True if connection successful, False otherwise
        """
        try:
            response = self._session.get(f"{self.server_url}/config", timeout=self.timeout)
            self.logger.debug(f"Config endpoint response: {response.status_code}")
            if response.status_code != 200:
                logger.error(
                    f"Failed to access config endpoint: {response.status_code}"
                )
                return False

            return True
        except Exception as e:
            logger.error(f"Error testing connection: {str(e)}")
            return False

    def test_connection_with_permissions(self) -> bool:
        """
        Test connection to DataHub with comprehensive permission testing
        (includes fetching policies and recipes)

        Returns:
            True if connection successful with full permissions, False otherwise
        """
        try:
            # First test basic connection
            if not self.test_connection():
                return False

            # Try to list recipes to check permissions  
            try:
                sources = self.list_ingestion_sources()
                if not isinstance(sources, list):
                    logger.error(
                        "Failed to list ingestion sources: Invalid response format"
                    )
                    return False
            except Exception as e:
                logger.error(f"Failed to list ingestion sources: {str(e)}")
                return False

            # Try to list policies to check permissions
            try:
                policies = self.list_policies()
                if not isinstance(policies, list):
                    logger.error("Failed to list policies: Invalid response format")
                    return False
            except Exception as e:
                logger.error(f"Failed to list policies: {str(e)}")
                return False

            return True
        except Exception as e:
            logger.error(f"Error testing connection with permissions: {str(e)}")
            return False

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
        type: Optional[
            str
        ] = None,  # Added to handle type parameter instead of source_type
        schedule: Optional[
            Union[Dict[str, str], str]
        ] = None,  # Added to handle schedule as dict
        **kwargs,  # Add **kwargs to handle any additional parameters
    ) -> Optional[Dict[str, Any]]:
        """
        Create a DataHub ingestion source.

        Args:
            recipe: Recipe configuration (dict) or template reference (string starting with @)
               OR a complete dictionary of ingestion source parameters
            name: Human-readable name for the ingestion source
            source_type: Type of source (e.g., postgres, snowflake, bigquery)
            type: Alternative parameter for source_type (deprecated, use source_type instead)
            schedule_interval: Cron expression for the schedule
            timezone: Timezone for the schedule
            schedule: Alternative schedule param as dict with 'interval' and 'timezone' keys
            executor_id: Executor ID to use
            source_id: Optional custom ID for the source
            debug_mode: Enable debug mode
            extra_args: Extra arguments for the ingestion
            **kwargs: Additional parameters

        Returns:
            dict: Created source information including URN
        """
        # Check if the first parameter is a complete config dict
        if isinstance(recipe, dict) and ("name" in recipe or "recipe" in recipe):
            # Extract parameters from the dict
            config_dict = recipe

            # Now extract individual parameters from the config dict
            source_id = config_dict.get("source_id", config_dict.get("id", source_id))
            name = config_dict.get("name", name)
            source_type = config_dict.get(
                "source_type", config_dict.get("type", source_type)
            )
            schedule = config_dict.get("schedule", schedule)
            executor_id = config_dict.get(
                "executor_id",
                config_dict.get("config", {}).get("executorId", executor_id),
            )
            debug_mode = config_dict.get(
                "debug_mode", config_dict.get("config", {}).get("debugMode", debug_mode)
            )
            extra_args = config_dict.get(
                "extra_args", config_dict.get("config", {}).get("extraArgs", extra_args)
            )

            if "recipe" in config_dict:
                recipe = config_dict["recipe"]
            elif "config" in config_dict and "recipe" in config_dict["config"]:
                recipe = config_dict["config"]["recipe"]

        self.logger.info(f"Creating ingestion source with name: {name}")

        if name is None:
            raise ValueError("'name' parameter is required")

        # Handle type parameter compatibility
        if not source_type and type:
            source_type = type

        if not source_type:
            raise ValueError(
                "Either 'source_type' or 'type' parameter must be provided"
            )

        # Handle schedule parameter compatibility
        if schedule:
            if isinstance(schedule, dict):
                schedule_interval = schedule.get("interval", schedule_interval)
                timezone = schedule.get("timezone", timezone)
            elif isinstance(schedule, str):
                schedule_interval = schedule

        # Generate source_id if not provided
        if not source_id:
            source_id = str(uuid.uuid4())

        # Generate URN for the ingestion source
        source_urn = f"urn:li:dataHubIngestionSource:{source_id}"

        # Process recipe based on type
        recipe_str = None
        if isinstance(recipe, dict):
            self.logger.debug("Converting recipe dict to JSON string")
            recipe_str = json.dumps(recipe)
        elif isinstance(recipe, str) and recipe.strip().startswith("@"):
            # Handle template reference as-is - don't try to parse it as JSON
            self.logger.debug(f"Found template reference in recipe: {recipe}")
            recipe_str = recipe.strip()
        else:
            # For other formats, use as-is
            recipe_str = recipe

        # Try GraphQL approach first
        try:
            self.logger.info("Creating ingestion source via GraphQL")

            # Check if the GraphQL schema has been validated before
            if hasattr(self, "_schema_validated") and not self._schema_validated:
                self.logger.info(
                    "Skipping GraphQL approach due to previous schema validation failures"
                )
                raise Exception(
                    "Schema validation previously failed, falling back to REST API"
                )

            # Fixed mutation without subselections, just returns a string
            mutation = """
            mutation createIngestionSource($input: CreateIngestionSourceInput!) {
              createIngestionSource(input: $input)
            }
            """

            # Build input object with all required fields
            graphql_input = {
                "type": source_type,
                "name": name,
                "schedule": {"interval": schedule_interval, "timezone": timezone},
                "config": {"executorId": executor_id, "debugMode": debug_mode},
            }

            # Handle recipe differently based on its type
            if isinstance(recipe, dict) and "source" in recipe:
                # Direct dictionary recipe with source - convert to string
                recipe_str = json.dumps(recipe)

            # Only add recipe if provided
            if recipe_str is not None:
                graphql_input["config"]["recipe"] = recipe_str

            # Add extra args if provided
            if extra_args:
                graphql_input["config"]["extraArgs"] = extra_args

            variables = {"input": graphql_input}

            self.logger.debug(f"GraphQL variables: {json.dumps(variables)}")
            result = self.execute_graphql(mutation, variables)

            if "errors" in result:
                # Check for schema validation errors which indicate incompatible API versions
                schema_errors = [
                    e
                    for e in result.get("errors", [])
                    if e.get("message", "").find("Validation error (UnknownType)") >= 0
                    or e.get("message", "").find("Unknown type") >= 0
                ]

                if schema_errors:
                    # This is a GraphQL schema mismatch, likely due to API version differences
                    self.logger.info(
                        "Detected GraphQL schema mismatch. Your client is likely connecting to a different DataHub API version than expected."
                    )
                    self.logger.info(
                        "This is normal when using this client with different DataHub versions. Falling back to REST API."
                    )
                    # Mark schema as invalid to avoid trying GraphQL in future calls
                    self._schema_validated = False
                    # Skip the direct GraphQL endpoint which would also fail
                    raise Exception(
                        "Schema validation failed, falling back to REST API"
                    )
                else:
                    # Other types of GraphQL errors
                    error_msgs = [
                        e.get("message", "") for e in result.get("errors", [])
                    ]
                    self.logger.warning(
                        f"GraphQL errors when creating ingestion source: {', '.join(error_msgs)}"
                    )
                    # Continue to REST API fallback
            else:
                # Success - createIngestionSource returns the URN
                created_urn = result.get("data", {}).get("createIngestionSource")
                if created_urn:
                    self.logger.info(
                        f"Successfully created ingestion source via GraphQL: {source_id}"
                    )
                    # Mark schema as valid since we succeeded
                    self._schema_validated = True
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
                            "extraArgs": extra_args,
                        },
                    }
                else:
                    self.logger.warning("GraphQL mutation returned success but no URN")
                    # We'll still return base info since the mutation didn't report errors
                    # Mark schema as valid since we succeeded
                    self._schema_validated = True
                    return {
                        "urn": source_urn,
                        "id": source_id,
                        "name": name,
                        "type": source_type,
                        "status": "created",
                        "config": {
                            "recipe": recipe_str,
                            "executorId": executor_id,
                            "debugMode": debug_mode,
                            "extraArgs": extra_args,
                        },
                    }
        except Exception as e:
            is_schema_error = (
                str(e).find("Schema validation") >= 0
                or str(e).find("Unknown type") >= 0
            )

            if is_schema_error:
                # Skip direct GraphQL endpoint if schema errors detected
                self.logger.info(
                    "Detected schema validation error: Schema validation failed or Unknown type found"
                )
                self.logger.info(
                    "This is normal when using this client with different DataHub versions. Falling back to REST API"
                )
            else:
                self.logger.warning(
                    f"Error creating ingestion source via GraphQL: {str(e)}"
                )

                # Continue to direct GraphQL endpoint only if not a schema error
                try:
                    self.logger.info("Trying direct GraphQL endpoint for creation")

                    # Set up headers
                    headers = (
                        self.headers.copy()
                        if hasattr(self, "headers")
                        else {"Content-Type": "application/json"}
                    )
                    if hasattr(self, "token") and self.token:
                        headers["Authorization"] = f"Bearer {self.token}"

                    # Simple GraphQL mutation with variables
                    direct_mutation = {
                        "query": """
                            mutation createIngestionSource($input: CreateIngestionSourceInput!) {
                                createIngestionSource(input: $input)
                            }
                        """,
                        "variables": variables,
                    }

                    direct_response = requests.post(
                        f"{self.server_url}/api/graphql",
                        headers=headers,
                        json=direct_mutation,
                    )

                    if direct_response.status_code == 200:
                        direct_result = direct_response.json()
                        if "errors" not in direct_result:
                            created_urn = direct_result.get("data", {}).get(
                                "createIngestionSource"
                            )
                            self.logger.info(
                                f"Successfully created ingestion source via direct GraphQL: {source_id}"
                            )
                            return {
                                "urn": created_urn or source_urn,
                                "id": source_id,
                                "name": name,
                                "type": source_type,
                                "status": "created",
                                "config": {
                                    "recipe": recipe_str,
                                    "executorId": executor_id,
                                    "debugMode": debug_mode,
                                    "extraArgs": extra_args,
                                },
                            }
                        else:
                            # Check for schema validation errors in the direct endpoint too
                            direct_schema_errors = [
                                e
                                for e in direct_result.get("errors", [])
                                if isinstance(e, dict)
                                and e.get("message", "").find(
                                    "Validation error (UnknownType)"
                                )
                                >= 0
                            ]

                            if direct_schema_errors:
                                self.logger.info(
                                    "GraphQL schema mismatch with direct endpoint. Falling back to REST API."
                                )
                                self._schema_validated = False
                            else:
                                self.logger.warning(
                                    f"GraphQL errors with direct endpoint: {direct_result.get('errors')}"
                                )
                    else:
                        self.logger.warning(
                            f"Failed with direct GraphQL endpoint: {direct_response.status_code}"
                        )
                except Exception as direct_e:
                    self.logger.warning(
                        f"Error with direct GraphQL endpoint: {str(direct_e)}"
                    )

        # Fall back to REST API
        try:
            self.logger.info(f"Creating ingestion source via REST API: {name}")

            # Prepare payload for OpenAPI v3
            payload = [
                {
                    "urn": source_urn,
                    "dataHubIngestionSourceKey": {"value": {"id": source_id}},
                    "dataHubIngestionSourceInfo": {
                        "value": {
                            "name": name,
                            "type": source_type,
                            "schedule": {
                                "interval": schedule_interval,
                                "timezone": timezone,
                            },
                            "config": {
                                "executorId": executor_id,
                                "debugMode": debug_mode,
                            },
                        }
                    },
                }
            ]

            # Handle recipe differently based on type
            if isinstance(recipe, dict) and "source" in recipe:
                # If recipe has a source field, convert to string
                recipe_str = json.dumps(recipe)

            # Add recipe to config if provided
            if recipe_str is not None:
                payload[0]["dataHubIngestionSourceInfo"]["value"]["config"][
                    "recipe"
                ] = recipe_str

            # Add extra args only if provided
            if extra_args:
                payload[0]["dataHubIngestionSourceInfo"]["value"]["config"][
                    "extraArgs"
                ] = extra_args

            self.logger.debug(f"REST API payload: {json.dumps(payload)}")

            response = requests.post(
                f"{self.server_url}/openapi/v3/entity/datahubingestionsource",
                headers=self.headers,
                json=payload,
            )

            if response.status_code in (200, 201, 202):
                self.logger.info(
                    f"Successfully created ingestion source via REST API: {source_id}"
                )
                try:
                    response_data = response.json()
                    created_urn = response_data[0].get("urn", source_urn)
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
                            "extraArgs": extra_args,
                        },
                    }
                except Exception as e:
                    self.logger.warning(f"Error parsing REST API response: {str(e)}")
                    # Still return success with known data
                    return {
                        "urn": source_urn,
                        "id": source_id,
                        "name": name,
                        "type": source_type,
                        "status": "created",
                        "config": {
                            "recipe": recipe_str,
                            "executorId": executor_id,
                            "debugMode": debug_mode,
                            "extraArgs": extra_args,
                        },
                    }
            else:
                self.logger.error(
                    f"Failed to create ingestion source via REST API: {response.status_code} - {response.text}"
                )
                return None
        except Exception as e:
            self.logger.error(f"Error creating ingestion source via REST API: {str(e)}")
            return None

    def trigger_ingestion(self, ingestion_source_id: str) -> bool:
        """
        Triggers ingestion for a DataHub ingestion source by its ID.
        This method tries multiple approaches to handle different DataHub versions:
        1. First tries the /runs endpoint (DataHub v0.12.0+)
        2. Then tries the /ingest/{id} endpoint
        3. Then tries the legacy ?action=ingest endpoint
        4. Then tries the createIngestionExecutionRequest GraphQL mutation
        5. Finally tries the executeIngestionSource GraphQL mutation for older versions

        Args:
            ingestion_source_id: The ID of the ingestion source to trigger

        Returns:
            True if any of the methods succeeded, False otherwise
        """
        source_urn = f"urn:li:dataHubIngestionSource:{ingestion_source_id}"
        logger.info(
            f"Attempting to trigger ingestion for source: {ingestion_source_id} (URN: {source_urn})"
        )

        # Method 1: Use /runs endpoint (DataHub v0.12.0+)
        try:
            logger.debug("Trying /runs endpoint...")
            url = f"{self.server_url}/runs?urn={source_urn}"
            response = requests.post(url, headers=self.headers)
            if response.status_code == 200:
                logger.info("Successfully triggered ingestion using /runs endpoint")
                return True
            logger.debug(
                f"Failed to trigger using /runs endpoint: {response.status_code} - {response.text}"
            )
        except Exception as e:
            logger.debug(f"Error triggering using /runs endpoint: {e}")

        # Method 2: Use /ingest/{id} endpoint
        try:
            logger.debug("Trying /ingest/{id} endpoint...")
            url = f"{self.server_url}/ingest/{ingestion_source_id}"
            response = requests.post(url, headers=self.headers)
            if response.status_code == 200:
                logger.info(
                    f"Successfully triggered ingestion using /ingest/{id} endpoint"
                )
                return True
            logger.debug(
                f"Failed to trigger using /ingest/{id} endpoint: {response.status_code} - {response.text}"
            )
        except Exception as e:
            logger.debug(f"Error triggering using /ingest/{id} endpoint: {e}")

        # Method 3: Use legacy ?action=ingest endpoint
        try:
            logger.debug("Trying legacy ?action=ingest endpoint...")
            url = f"{self.server_url}/ingestion-sources/{ingestion_source_id}?action=ingest"
            response = requests.post(url, headers=self.headers)
            if response.status_code == 200:
                logger.info("Successfully triggered ingestion using legacy endpoint")
                return True
            logger.debug(
                f"Failed to trigger using legacy endpoint: {response.status_code} - {response.text}"
            )
        except Exception as e:
            logger.debug(f"Error triggering using legacy endpoint: {e}")

        # Method 4: Use createIngestionExecutionRequest GraphQL mutation
        try:
            logger.debug("Trying createIngestionExecutionRequest GraphQL mutation...")
            graphql_query = """
            mutation createIngestionExecutionRequest($input: CreateIngestionExecutionRequestInput!) {
              createIngestionExecutionRequest(input: $input)
            }
            """
            variables = {"input": {"ingestionSourceUrn": source_urn}}

            result = self.execute_graphql(graphql_query, variables)

            if result and "errors" not in result:
                logger.info(
                    "Successfully triggered ingestion using createIngestionExecutionRequest mutation"
                )
                return True
            logger.debug(
                f"GraphQL mutation returned errors: {result.get('errors') if result else 'No result'}"
            )
        except Exception as e:
            logger.debug(
                f"Error triggering using createIngestionExecutionRequest mutation: {e}"
            )

        # Method 5: Use executeIngestionSource GraphQL mutation (legacy fallback)
        try:
            logger.debug(
                "Trying executeIngestionSource GraphQL mutation (legacy fallback)..."
            )
            graphql_query = """
            mutation executeIngestionSource($input: ExecuteIngestionSourceInput!) {
                executeIngestionSource(input: $input) {
                    executionId
                }
            }
            """
            variables = {"input": {"urn": source_urn}}

            result = self.execute_graphql(graphql_query, variables)

            if result and "errors" not in result:
                logger.info(
                    "Successfully triggered ingestion using executeIngestionSource mutation"
                )
                return True
            logger.debug(
                f"GraphQL mutation returned errors: {result.get('errors') if result else 'No result'}"
            )
        except Exception as e:
            logger.debug(f"Error triggering using executeIngestionSource mutation: {e}")

        logger.error(
            f"All methods to trigger ingestion for source {ingestion_source_id} failed"
        )
        return False

    def list_ingestion_sources(self):
        """
        List all ingestion sources defined in DataHub.
        Uses GraphQL primarily, with REST API fallbacks if needed.

        Returns:
            list: A list of dictionaries containing source information
        """
        self.logger.info("Listing ingestion sources")
        sources = []

        # Use GraphQL with the correct query structure
        query = """
        query listIngestionSources($input: ListIngestionSourcesInput!) {
          listIngestionSources(input: $input) {
            start
            count
            total
            ingestionSources {
              urn
              name
              type
              config {
                recipe
                version
                executorId
                debugMode
                extraArgs {
                  key
                  value
                  __typename
                }
                __typename
              }
              schedule {
                interval
                timezone
                __typename
              }
              platform {
                urn
                __typename
              }
              executions(start: 0, count: 1) {
                start
                count
                total
                executionRequests {
                  urn
                  id
                  input {
                    requestedAt
                    actorUrn
                    __typename
                  }
                  result {
                    status
                    startTimeMs
                    durationMs
                    structuredReport {
                      type
                      serializedValue
                      __typename
                    }
                    __typename
                  }
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
        }
        """

        variables = {
            "input": {
                "start": 0,
                "count": 100,
                "filters": [
                    {"field": "sourceType", "values": ["SYSTEM"], "negated": True}
                ],
            }
        }

        try:
            self.logger.debug("Listing ingestion sources using GraphQL")
            result = self.execute_graphql(query, variables)

            if (
                result
                and isinstance(result, dict)
                and "data" in result
                and result["data"]
                and "listIngestionSources" in result["data"]
            ):
                response_data = result["data"]["listIngestionSources"]
                if response_data is None:
                    self.logger.warning(
                        "listIngestionSources returned None in GraphQL response"
                    )
                    raw_sources = []
                else:
                    raw_sources = response_data.get("ingestionSources", [])
                    if raw_sources is None:
                        self.logger.warning(
                            "ingestionSources is None in GraphQL response"
                        )
                        raw_sources = []

                self.logger.info(
                    f"Successfully retrieved {len(raw_sources)} ingestion sources using GraphQL"
                )

                # Process each source
                for source in raw_sources:
                    if source is None:
                        self.logger.warning("Skipping None source in response")
                        continue

                    try:
                        urn = source.get("urn")
                        if not urn:
                            self.logger.warning(
                                f"Source missing URN, skipping: {source}"
                            )
                            continue

                        source_id = urn.split(":")[-1] if urn else None
                        if not source_id:
                            self.logger.warning(
                                f"Could not extract source ID from URN: {urn}"
                            )
                            continue

                        # Get config
                        config = source.get("config", {}) or {}
                        if config is None:
                            config = {}

                        # Get schedule
                        schedule = source.get("schedule", {}) or {}
                        if schedule is None:
                            schedule = {}

                        # Parse recipe
                        recipe_str = config.get("recipe", "{}")
                        recipe = {}
                        try:
                            if recipe_str is None:
                                recipe = {}
                            elif isinstance(recipe_str, dict):
                                recipe = recipe_str
                            else:
                                recipe = json.loads(recipe_str)
                        except (json.JSONDecodeError, TypeError):
                            self.logger.warning(
                                f"Could not parse recipe JSON for {source_id}"
                            )

                        # Get latest execution information
                        executions = source.get("executions", {}) or {}
                        if executions is None:
                            executions = {}

                        exec_requests = executions.get("executionRequests", []) or []
                        if exec_requests is None:
                            exec_requests = []

                        latest_execution = exec_requests[0] if exec_requests else None

                        # Create a simplified source object
                        simplified_source = {
                            "urn": urn,
                            "id": source_id,
                            "name": source.get("name", ""),
                            "type": source.get("type", ""),
                            "recipe": recipe,
                            "schedule": source.get("schedule", {}),
                            "config": {
                                "executorId": config.get("executorId", "default"),
                                "debugMode": config.get("debugMode", False),
                                "version": config.get("version", "0.8.42"),
                            },
                        }

                        # Add execution status if available
                        if latest_execution and latest_execution.get("result"):
                            simplified_source["last_execution"] = {
                                "id": latest_execution.get("id"),
                                "status": latest_execution.get("result", {}).get(
                                    "status"
                                ),
                                "startTimeMs": latest_execution.get("result", {}).get(
                                    "startTimeMs"
                                ),
                                "durationMs": latest_execution.get("result", {}).get(
                                    "durationMs"
                                ),
                            }

                        sources.append(simplified_source)
                    except Exception as e:
                        self.logger.warning(
                            f"Error processing source {source.get('name', 'unknown')}: {str(e)}"
                        )

                if sources:
                    self.logger.info(
                        f"Successfully processed {len(sources)} ingestion sources"
                    )
                    return sources

            # Check for specific errors
            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.warning(f"GraphQL errors: {', '.join(error_messages)}")
            else:
                self.logger.warning(
                    "Failed to retrieve ingestion sources using GraphQL or no sources found"
                )

        except Exception as e:
            self.logger.warning(
                f"Error listing ingestion sources via GraphQL: {str(e)}"
            )

        # Try OpenAPI v3 endpoint
        try:
            openapi_url = f"{self.server_url}/openapi/v3/entity/datahubingestionsource"
            self.logger.debug(
                f"Listing ingestion sources via OpenAPI v3: GET {openapi_url}"
            )

            response = requests.get(openapi_url, headers=self.headers)

            if response.status_code == 200:
                try:
                    data = response.json()

                    # Parse entities from the response according to the OpenAPI v3 schema
                    entities = data.get("entities", []) or []
                    if entities is None:
                        entities = []
                    self.logger.debug(
                        f"Found {len(entities)} entities in OpenAPI v3 response"
                    )

                    for entity in entities:
                        if entity is None:
                            continue

                        try:
                            urn = entity.get("urn", "")
                            if not urn:
                                continue

                            # Source ID from the key or the URN
                            source_id = urn.split(":")[-1]

                            # Source information is nested under dataHubIngestionSourceInfo.value
                            source_info = (
                                entity.get("dataHubIngestionSourceInfo", {}) or {}
                            )
                            if source_info is None:
                                source_info = {}

                            source_info_value = source_info.get("value", {}) or {}
                            if source_info_value is None:
                                source_info_value = {}

                            if not source_info_value:
                                continue

                            # Get recipe
                            recipe = {}
                            config = source_info_value.get("config", {}) or {}
                            if config is None:
                                config = {}

                            if config and "recipe" in config:
                                try:
                                    recipe_str = config["recipe"]
                                    if recipe_str is None:
                                        recipe = {}
                                    elif isinstance(recipe_str, str):
                                        recipe = json.loads(recipe_str)
                                    elif isinstance(recipe_str, dict):
                                        recipe = recipe_str
                                except json.JSONDecodeError:
                                    self.logger.warning(
                                        f"Failed to parse recipe for source {urn}"
                                    )

                            # Create simplified source object
                            simplified_source = {
                                "urn": urn,
                                "id": source_info_value.get("id", source_id),
                                "name": source_info_value.get("name", source_id),
                                "type": source_info_value.get("type", ""),
                                "platform": source_info_value.get("platform", ""),
                                "recipe": recipe,
                                "schedule": source_info_value.get("schedule", {}) or {},
                                "config": {
                                    "executorId": config.get("executorId", "default"),
                                    "debugMode": config.get("debugMode", False),
                                    "version": config.get("version", "0.8.42"),
                                    "extraArgs": config.get("extraArgs", {}) or {},
                                },
                            }

                            sources.append(simplified_source)
                        except Exception as e:
                            self.logger.warning(
                                f"Error processing entity {entity.get('urn')}: {str(e)}"
                            )

                    if sources:
                        self.logger.info(
                            f"Successfully retrieved {len(sources)} ingestion sources via OpenAPI v3"
                        )
                        return sources
                except json.JSONDecodeError:
                    self.logger.warning(
                        "Failed to parse JSON response from OpenAPI v3 endpoint"
                    )
            else:
                self.logger.warning(
                    f"Failed to list ingestion sources via OpenAPI v3: {response.status_code} - {response.text}"
                )

                # Try one more alternative endpoint format
                try:
                    alt_url = f"{self.server_url}/api/v2/ingestion/sources"
                    self.logger.debug(f"Trying alternative API endpoint: GET {alt_url}")
                    alt_response = requests.get(alt_url, headers=self.headers)

                    if alt_response.status_code == 200:
                        try:
                            sources_data = alt_response.json()
                            if isinstance(sources_data, list):
                                for source in sources_data:
                                    if source is None:
                                        continue

                                    try:
                                        source_id = source.get("id", "")
                                        if not source_id:
                                            continue

                                        # Create simplified source object
                                        simplified_source = {
                                            "urn": f"urn:li:dataHubIngestionSource:{source_id}",
                                            "id": source_id,
                                            "name": source.get("name", source_id),
                                            "type": source.get("type", ""),
                                            "recipe": {},
                                            "schedule": source.get("schedule", {})
                                            or {},
                                        }

                                        # Parse recipe if present
                                        if "recipe" in source:
                                            try:
                                                recipe_str = source["recipe"]
                                                if recipe_str is None:
                                                    simplified_source["recipe"] = {}
                                                elif isinstance(recipe_str, str):
                                                    simplified_source["recipe"] = (
                                                        json.loads(recipe_str)
                                                    )
                                                elif isinstance(recipe_str, dict):
                                                    simplified_source["recipe"] = (
                                                        recipe_str
                                                    )
                                            except json.JSONDecodeError:
                                                pass

                                        sources.append(simplified_source)
                                    except Exception as e:
                                        self.logger.warning(
                                            f"Error processing source {source.get('id')}: {str(e)}"
                                        )

                                if sources:
                                    self.logger.info(
                                        f"Retrieved {len(sources)} ingestion sources via alternative API"
                                    )
                                    return sources
                        except json.JSONDecodeError:
                            self.logger.warning(
                                "Failed to parse JSON from alternative API endpoint"
                            )
                except Exception as e:
                    self.logger.debug(f"Error with alternative API endpoint: {str(e)}")
        except Exception as e:
            self.logger.error(
                f"Error listing ingestion sources via OpenAPI v3: {str(e)}"
            )

        return sources or []

    def get_ingestion_source(self, source_id):
        """
        Get ingestion source by ID.
        First tries GraphQL, then falls back to OpenAPI v3 endpoint.

        Args:
            source_id: ID or URN of the ingestion source to retrieve

        Returns:
            dict: Source information if found, None otherwise. Returns a minimal
                  default source object if all retrieval attempts fail but the source_id is provided.
        """
        # If source_id is already a URN, use it as is
        if source_id.startswith("urn:li:dataHubIngestionSource:"):
            self.logger.debug(f"Using provided URN: {source_id}")
            source_urn = source_id
            # Extract just the ID part for logging
            source_id = source_id.replace("urn:li:dataHubIngestionSource:", "")
        else:
            self.logger.debug(f"Converting source ID to URN: {source_id}")
            source_urn = f"urn:li:dataHubIngestionSource:{source_id}"

        self.logger.info(f"Fetching ingestion source: {source_id}")

        # Try GraphQL approach first - preferred method
        query = """
        query ingestionSource($urn: String!) {
          ingestionSource(urn: $urn) {
            urn
            type
            name
            schedule {
              interval
              timezone
            }
            config {
              recipe
              executorId
              debugMode
              version
            }
          }
        }
        """

        variables = {"urn": source_urn}

        try:
            self.logger.debug(f"Using GraphQL to fetch ingestion source: {source_id}")
            result = self.execute_graphql(query, variables)

            if result and "data" in result and result["data"].get("ingestionSource"):
                self.logger.debug(
                    f"Successfully retrieved ingestion source via GraphQL: {source_id}"
                )

                ingestion_source = result["data"]["ingestionSource"]
                source_info = {
                    "urn": ingestion_source["urn"],
                    "id": source_id,
                    "name": ingestion_source["name"],
                    "type": ingestion_source["type"],
                    "schedule": ingestion_source["schedule"],
                    "config": {},  # Initialize config to ensure it's never None
                }

                # Parse the recipe JSON
                config = ingestion_source.get("config", {}) or {}
                if config is None:
                    config = {}

                recipe_str = config.get("recipe")
                self.logger.debug(f"Raw recipe string from GraphQL: {recipe_str}")

                try:
                    # Handle different recipe formats
                    if recipe_str is None:
                        recipe = {}
                    elif isinstance(recipe_str, dict):
                        recipe = recipe_str
                    elif isinstance(recipe_str, str):
                        if not recipe_str.strip():
                            recipe = {}
                        # Check if it's a template reference (starting with @)
                        elif recipe_str.strip().startswith("@"):
                            self.logger.debug(
                                f"Found template reference in recipe: {recipe_str}"
                            )
                            recipe = recipe_str.strip()
                        else:
                            # Try to parse as JSON, with fallback to raw string
                            try:
                                recipe = json.loads(recipe_str)
                                self.logger.debug(
                                    f"Successfully parsed recipe JSON: {json.dumps(recipe)}"
                                )
                            except json.JSONDecodeError:
                                self.logger.warning(
                                    f"Could not parse recipe JSON for {source_id}, treating as raw string"
                                )
                                # If it's not valid JSON, treat it as a raw string
                                # This could be a template or other format
                                recipe = recipe_str
                    else:
                        self.logger.warning(
                            f"Unexpected recipe type: {type(recipe_str)}, using empty dict"
                        )
                        recipe = {}

                    # Build config object with all relevant fields
                    source_info["config"] = {
                        "recipe": recipe_str,  # Store original recipe string to avoid double-parsing
                        "executorId": config.get("executorId", "default"),
                        "debugMode": config.get("debugMode", False),
                        "version": config.get("version"),
                    }
                except Exception as e:
                    self.logger.warning(
                        f"Error processing recipe for {source_id}: {str(e)}"
                    )
                    source_info["config"]["recipe"] = (
                        recipe_str or {}
                    )  # Use empty object as fallback

                return source_info

            # Check for errors
            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.warning(f"GraphQL errors: {', '.join(error_messages)}")
            else:
                self.logger.warning(
                    f"GraphQL query for ingestion source returned no results: {source_id}"
                )

            # Fall through to OpenAPI v3
        except Exception as e:
            self.logger.warning(f"Error getting ingestion source via GraphQL: {str(e)}")
            # Fall through to OpenAPI v3

        # Try OpenAPI v3 endpoint with the exact schema format
        try:
            openapi_url = f"{self.server_url}/openapi/v3/entity/datahubingestionsource/{source_urn}"
            self.logger.debug(
                f"Fetching ingestion source via OpenAPI v3: GET {openapi_url}"
            )

            response = requests.get(openapi_url, headers=self.headers)

            if response.status_code == 200:
                self.logger.debug(
                    f"Successfully retrieved ingestion source via OpenAPI v3: {source_id}"
                )
                try:
                    data = response.json()
                    self.logger.debug(f"OpenAPI v3 response: {json.dumps(data)}")

                    # Extract source info from the OpenAPI v3 format
                    # The structure has nested dataHubIngestionSourceInfo.value
                    source_info_wrapper = data.get("dataHubIngestionSourceInfo", {})
                    if not source_info_wrapper:
                        self.logger.warning(
                            f"No dataHubIngestionSourceInfo in response for {source_id}"
                        )

                    source_info = (
                        source_info_wrapper.get("value", {})
                        if source_info_wrapper
                        else {}
                    )

                    if source_info:
                        # Create the result object
                        result = {
                            "urn": source_urn,
                            "id": source_id,
                            "name": source_info.get("name", source_id),
                            "type": source_info.get("type", ""),
                            "platform": source_info.get("platform", ""),
                            "schedule": source_info.get("schedule", {}),
                            "config": {},  # Initialize config to ensure it's never None
                        }

                        # Parse the recipe JSON if it exists
                        config = source_info.get("config", {}) or {}
                        if config is None:
                            config = {}

                        recipe_str = config.get("recipe")
                        self.logger.debug(
                            f"Raw recipe string from OpenAPI: {recipe_str}"
                        )

                        # Build config object with all relevant fields
                        result["config"] = {
                            "recipe": recipe_str,  # Store original recipe string to avoid double-parsing
                            "executorId": config.get("executorId", "default"),
                            "debugMode": config.get("debugMode", False),
                            "version": config.get("version"),
                        }

                        return result
                    else:
                        self.logger.warning(
                            f"No source info found in OpenAPI v3 response for {source_id}"
                        )
                except json.JSONDecodeError:
                    self.logger.warning(
                        f"Could not parse JSON response from OpenAPI v3 for {source_id}"
                    )
                except Exception as e:
                    self.logger.warning(
                        f"Error processing OpenAPI v3 response for {source_id}: {str(e)}"
                    )
            else:
                self.logger.warning(
                    f"OpenAPI v3 GET failed: {response.status_code} - {response.text}"
                )

            # Try all available sources endpoint as last resort
            try:
                sources = self.list_ingestion_sources()
                for source in sources:
                    if source.get("id") == source_id or source.get("urn") == source_urn:
                        self.logger.info(
                            f"Found ingestion source {source_id} in list of all sources"
                        )
                        return source
                self.logger.warning(
                    f"Ingestion source {source_id} not found in list of all sources"
                )
            except Exception as e:
                self.logger.warning(
                    f"Error trying to find source {source_id} in list of all sources: {str(e)}"
                )
        except Exception as e:
            self.logger.warning(
                f"Error getting ingestion source via OpenAPI v3: {str(e)}"
            )

        # All attempts to retrieve the source have failed
        self.logger.warning(
            f"All attempts to retrieve ingestion source {source_id} failed"
        )

        # Return a minimal default source object if we have at least the ID
        # This allows the call site to have some information to work with
        if source_id:
            self.logger.warning(
                f"Returning default source info for {source_id} as all attempts failed"
            )
            return {
                "urn": source_urn,
                "id": source_id,
                "name": source_id,  # Use ID as name
                "type": "",  # Unknown type
                "schedule": {
                    "interval": "0 0 * * *",
                    "timezone": "UTC",
                },  # Default schedule
                "config": {
                    "recipe": {},  # Empty recipe
                    "executorId": "default",
                    "debugMode": False,
                    "version": None,
                },
            }

        return None

    def delete_ingestion_source(self, source_id: str) -> bool:
        """
        Delete a DataHub ingestion source by ID.
        Uses GraphQL mutation first, then falls back to OpenAPI v3.

        Args:
            source_id: Ingestion source ID

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Deleting ingestion source: {source_id}")
        source_urn = f"urn:li:dataHubIngestionSource:{source_id}"

        # Try GraphQL mutation first
        mutation = """
        mutation deleteIngestionSource($urn: String!) {
            deleteIngestionSource(urn: $urn)
        }
        """

        variables = {"urn": source_urn}

        try:
            self.logger.debug("Deleting ingestion source using GraphQL mutation")
            result = self.execute_graphql(mutation, variables)

            if "errors" not in result:
                self.logger.info(
                    f"Successfully deleted ingestion source: {source_id} using GraphQL"
                )
                return True

            self.logger.warning(
                f"GraphQL errors when deleting ingestion source: {result.get('errors')}"
            )
            # Fall back to OpenAPI v3
        except Exception as e:
            self.logger.warning(
                f"Error deleting ingestion source using GraphQL: {str(e)}"
            )
            # Fall back to OpenAPI v3

        # Try OpenAPI v3 endpoint
        try:
            openapi_url = f"{self.server_url}/openapi/v3/entity/datahubingestionsource/{source_urn}"
            self.logger.debug(
                f"Deleting ingestion source via OpenAPI v3: DELETE {openapi_url}"
            )

            response = requests.delete(openapi_url, headers=self.headers)

            if response.status_code in (200, 201, 202, 204):
                self.logger.info(
                    f"Successfully deleted ingestion source: {source_id} using OpenAPI v3"
                )
                return True

            self.logger.error(
                f"Failed to delete ingestion source via OpenAPI v3: {response.status_code} - {response.text}"
            )
        except Exception as e:
            self.logger.error(
                f"Error deleting ingestion source via OpenAPI v3: {str(e)}"
            )

        return False

    def create_secret(
        self, name: str, value: str, description: Optional[str] = None
    ) -> bool:
        """
        Create a secret in DataHub.

        If the secret already exists, this method will attempt to update it instead.

        Args:
            name: Secret name
            value: Secret value
            description: Optional description of the secret

        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Creating secret: {name}")

        # Try GraphQL first
        mutation = """
        mutation createSecret($input: CreateSecretInput!) {
          createSecret(input: $input)
        }
        """

        variables = {
            "input": {
                "name": name,
                "value": value,
                "description": description
                or "Secret managed by datahub-recipes-manager",
            }
        }

        try:
            result = self.execute_graphql(mutation, variables)
            if result and "errors" not in result:
                self.logger.info(f"Successfully created secret: {name}")
                return True
            else:
                # Check if the error is because the secret already exists
                error_message = ""
                if result and "errors" in result:
                    errors = result["errors"]
                    if isinstance(errors, list) and len(errors) > 0:
                        error_message = str(errors[0].get("message", ""))

                if "already exists" in error_message:
                    self.logger.info(
                        f"Secret {name} already exists, attempting to update it instead"
                    )
                    return self.update_secret(name, value, description)

                self.logger.warning(
                    f"GraphQL errors when creating secret: {result.get('errors')}"
                )
                self.logger.warning("Falling back to REST API")
        except Exception as e:
            self.logger.warning(f"Error creating secret via GraphQL: {str(e)}")
            self.logger.warning("Falling back to REST API")

        # Fall back to REST API using multiple possible endpoints
        try:
            # Set up headers
            headers = (
                self.headers.copy()
                if hasattr(self, "headers")
                else {"Content-Type": "application/json"}
            )
            if hasattr(self, "token") and self.token:
                headers["Authorization"] = f"Bearer {self.token}"

            # Try multiple possible endpoints
            endpoints = [
                f"{self.server_url}/secrets",
                f"{self.server_url}/openapi/secrets",
                f"{self.server_url}/api/v2/secrets",
            ]

            # Prepare payload
            payload = {
                "name": name,
                "value": value,
                "description": description
                or "Secret managed by datahub-recipes-manager",
            }

            # Try each endpoint
            for url in endpoints:
                self.logger.info(f"Attempting to create secret via REST API: {url}")

                response = requests.post(url, headers=headers, json=payload)

                if response.status_code in (200, 201, 204):
                    self.logger.info(
                        f"Successfully created secret via endpoint {url}: {name}"
                    )
                    return True
                elif response.status_code == 409 or response.status_code == 400:
                    # 409 Conflict or 400 Bad Request with "already exists" in the response
                    if "already exists" in response.text:
                        self.logger.info(
                            f"Secret {name} already exists in REST API, attempting to update it"
                        )
                        return self.update_secret(name, value, description)
                else:
                    self.logger.warning(
                        f"Failed to create secret at {url}: {response.status_code} - {response.text}"
                    )

            # If all endpoints failed, try update as a last resort
            self.logger.warning(
                f"All direct creation methods failed for secret {name}, trying update as last resort"
            )
            return self.update_secret(name, value, description)
        except Exception as e:
            self.logger.error(f"Error creating secret: {str(e)}")

        return False

    def delete_secret(self, name_or_urn: str) -> bool:
        """
        Delete a secret from DataHub using GraphQL.

        Args:
            name_or_urn: Secret name or URN

        Returns:
            bool: True if successful, False otherwise
        """
        # Convert name to URN if needed
        if not name_or_urn.startswith("urn:li:dataHubSecret:"):
            urn = f"urn:li:dataHubSecret:{name_or_urn}"
        else:
            urn = name_or_urn

        self.logger.info(f"Deleting secret: {name_or_urn}")

        # Delete the secret using GraphQL
        mutation = """
        mutation deleteSecret($urn: String!) {
          deleteSecret(urn: $urn)
        }
        """

        variables = {"urn": urn}

        try:
            self.logger.debug("Deleting secret using GraphQL mutation")
            result = self.execute_graphql(mutation, variables)

            if "errors" not in result:
                self.logger.info(
                    f"Successfully deleted secret: {name_or_urn} using GraphQL"
                )
                return True

            self.logger.error(
                f"GraphQL errors when deleting secret: {result.get('errors')}"
            )
            return False
        except Exception as e:
            self.logger.error(f"Error deleting secret using GraphQL: {str(e)}")
            return False

    def update_secret(
        self, name: str, value: str, description: Optional[str] = None
    ) -> bool:
        """
        Update an existing secret in DataHub.

        This method uses a delete-then-create approach to ensure the secret is properly updated.

        Args:
            name: Secret name
            value: New secret value
            description: Optional description of the secret

        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Updating secret: {name}")

        # First, delete the existing secret
        if not self.delete_secret(name):
            self.logger.warning(
                f"Failed to delete existing secret {name} before update. Will try to create anyway."
            )
        else:
            self.logger.debug(
                f"Successfully deleted existing secret {name} before update"
            )

        # Then create the secret with the new value
        return self.create_secret(name, value, description)

    def update_ingestion_source(
        self, source_id: str, recipe_json: dict, schedule: Optional[str] = None
    ) -> Optional[dict]:
        """
        Update a DataHub ingestion source by ID.

        Args:
            source_id: Ingestion source ID
            recipe_json: Recipe configuration as a dictionary
            schedule: Optional cron schedule (use None to keep existing schedule)

        Returns:
            Updated source information or None if update failed
        """
        source_urn = f"urn:li:dataHubIngestionSource:{source_id}"

        try:
            logger.info(f"Updating ingestion source: {source_id}")

            # Get the existing source to merge fields
            existing = self.get_ingestion_source(source_id)
            if not existing:
                logger.error(f"Unable to find existing source with ID: {source_id}")
                return None

            # Create config with recipe and schedule
            config = {
                "recipe": json.dumps(recipe_json),
                "type": "SCHEDULED",
                "name": source_id,
                "scheduleCron": schedule
                if schedule
                else existing.get("scheduleCron", "0 0 * * *"),
                "executions": existing.get("executions", []),
            }

            query = """
                mutation updateIngestionSource($urn: String!, $input: UpdateIngestionSourceInput!) {
                    updateIngestionSource(urn: $urn, input: $input) {
                        urn
                        type
                        name
                        schedule {
                            interval
                            timezone
                        }
                        config {
                            recipe
                            executorId
                            version
                        }
                    }
                }
            """

            variables = {
                "urn": source_urn,
                "input": {
                    "type": config["type"],
                    "name": config["name"],
                    "schedule": {"interval": config["scheduleCron"], "timezone": "UTC"},
                    "config": {
                        "recipe": config["recipe"],
                        "executorId": "default",
                        "version": "0.0.1",
                    },
                },
            }

            # Use the execute_graphql method which will use datahubgraph if available
            result = self.execute_graphql(query, variables)

            if "errors" in result:
                logger.error(f"GraphQL errors: {result['errors']}")
                return None

            logger.info(f"Ingestion source updated: {source_urn}")
            return result.get("data", {}).get("updateIngestionSource")
        except Exception as e:
            logger.error(f"Error updating ingestion source: {str(e)}")
            return None

    def patch_ingestion_source(
        self,
        urn: str,
        recipe: Optional[Union[Dict[str, Any], str]] = None,
        name: Optional[str] = None,
        schedule_interval: Optional[str] = None,
        timezone: Optional[str] = None,
        source_type: Optional[str] = None,
        executor_id: Optional[str] = None,
        debug_mode: Optional[bool] = None,
        extra_args: Optional[Dict[str, Any]] = None,
        schedule: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Update an existing ingestion source.

        Args:
            urn: URN of the ingestion source to update or plain source ID
            recipe: New recipe configuration (dict) or template reference (string starting with @)
            name: New name for the ingestion source
            schedule_interval: New cron expression for the schedule
            timezone: New timezone for the schedule
            schedule: Alternative schedule param as dict with 'interval' and 'timezone' keys
            source_type: New type of source
            executor_id: New executor ID
            debug_mode: New debug mode setting
            extra_args: New extra arguments for the ingestion

        Returns:
            dict: Updated source information
        """
        # Convert source ID to URN if needed
        if not urn.startswith("urn:li:dataHubIngestionSource:"):
            self.logger.debug(f"Converting source ID '{urn}' to URN")
            urn = f"urn:li:dataHubIngestionSource:{urn}"

        self.logger.info(f"Patching ingestion source with URN: {urn}")

        # Prepare recipe value if provided
        recipe_str = None
        if recipe is not None:
            if isinstance(recipe, dict):
                self.logger.debug("Converting recipe dict to JSON string")
                recipe_str = json.dumps(recipe)
            elif isinstance(recipe, str) and recipe.strip().startswith("@"):
                # Handle template reference as-is - don't try to parse it as JSON
                self.logger.debug(f"Found template reference in recipe: {recipe}")
                recipe_str = recipe.strip()
            else:
                # For other string formats, use as-is
                recipe_str = recipe

        # Handle schedule parameter compatibility
        if schedule:
            if isinstance(schedule, dict):
                schedule_interval = schedule.get("interval", schedule_interval)
                timezone = schedule.get("timezone", timezone)

        # Try GraphQL approach first
        try:
            self.logger.info("Patching ingestion source via GraphQL")

            # First get the current source to ensure we have all the data
            current_source = self.get_ingestion_source(urn)
            if not current_source:
                self.logger.error(f"Could not fetch source with URN {urn} for patching")
                return None

            # Prepare the update mutation
            mutation = """
            mutation updateIngestionSource($urn: String!, $input: UpdateIngestionSourceInput!) {
                updateIngestionSource(urn: $urn, input: $input)
            }
            """

            # Start with current values and update with provided values
            input_obj = {}

            # Always include the type from the current source - this is required for the mutation
            input_obj["type"] = source_type or current_source.get("type")
            if not input_obj["type"]:
                self.logger.error(
                    "Source type is required but not available from current source or parameters"
                )
                return None

            # Only add name if provided
            if name is not None:
                input_obj["name"] = name
            else:
                # Always include the current name - required for the mutation
                input_obj["name"] = current_source.get("name")

            # Build config object - always initialize it
            config = {}

            # Get current config values to avoid nulls
            current_config = current_source.get("config", {}) or {}

            # Add recipe if provided or keep existing
            if recipe is not None:
                # Convert any recipe dict to string
                config["recipe"] = recipe_str
            elif "recipe" in current_config and current_config["recipe"] is not None:
                config["recipe"] = current_config["recipe"]

            # Always include executorId - it's required by the GraphQL schema
            if executor_id is not None:
                config["executorId"] = executor_id
            elif current_config.get("executorId"):
                config["executorId"] = current_config.get("executorId")
            else:
                config["executorId"] = "default"  # Fallback to default executor

            if debug_mode is not None:
                config["debugMode"] = debug_mode
            elif current_config.get("debugMode") is not None:
                config["debugMode"] = current_config.get("debugMode")

            if extra_args is not None:
                config["extraArgs"] = extra_args
            elif current_config.get("extraArgs") is not None:
                config["extraArgs"] = current_config.get("extraArgs")

            # Only add config if we have something to update
            if config:
                input_obj["config"] = config

            # Build schedule object only if we have something to update
            if schedule_interval is not None or timezone is not None:
                schedule_obj = {}

                # Use current values as defaults if available
                current_schedule = current_source.get("schedule", {}) or {}

                # Update with new values if provided
                if schedule_interval is not None:
                    schedule_obj["interval"] = schedule_interval
                elif current_schedule and "interval" in current_schedule:
                    schedule_obj["interval"] = current_schedule["interval"]

                if timezone is not None:
                    schedule_obj["timezone"] = timezone
                elif current_schedule and "timezone" in current_schedule:
                    schedule_obj["timezone"] = current_schedule["timezone"]

                if schedule_obj:
                    input_obj["schedule"] = schedule_obj
            elif current_source.get("schedule"):
                # Include the current schedule in the update
                input_obj["schedule"] = current_source.get("schedule")

            # Only proceed if we have something to update
            if not input_obj or (
                len(input_obj) == 2 and "type" in input_obj and "name" in input_obj
            ):
                self.logger.warning("No updates to apply to ingestion source")
                return current_source

            variables = {"urn": urn, "input": input_obj}

            self.logger.debug(f"GraphQL variables: {json.dumps(variables)}")
            result = self.execute_graphql(mutation, variables)

            # Check for errors in the GraphQL response
            if result and "errors" in result and result["errors"]:
                error_messages = [
                    error.get("message", "Unknown error") for error in result["errors"]
                ]
                self.logger.warning(
                    f"GraphQL errors when patching ingestion source: {', '.join(error_messages)}"
                )
                # Continue to REST API fallback
            # GraphQL operation succeeded
            elif result:
                self.logger.info("Successfully patched ingestion source via GraphQL")
                # Get the updated source to return
                return self.get_ingestion_source(urn)

        except Exception as e:
            self.logger.warning(f"GraphQL patch failed: {str(e)}")
            self.logger.info("Falling back to REST API for patching")

        # Fallback to REST API
        try:
            self.logger.info(f"Patching ingestion source via REST API: {urn}")

            # If we don't have current_source from above, get it now
            if not current_source:
                current_source = self.get_ingestion_source(urn)
                if not current_source:
                    self.logger.error(
                        f"Could not fetch source info for REST API patching: {urn}"
                    )
                    return None

            # Get current configuration to avoid nulls
            current_config = current_source.get("config", {}) or {}

            # Prepare payload
            payload = {}
            if name is not None:
                payload["name"] = name
            if source_type is not None:
                payload["type"] = source_type

            # Ensure config exists in the payload
            payload["config"] = payload.get("config", {})

            if recipe_str is not None:
                payload["config"]["recipe"] = recipe_str
            elif "recipe" in current_config:
                payload["config"]["recipe"] = current_config["recipe"]

            if executor_id is not None:
                payload["config"]["executorId"] = executor_id
            elif "executorId" in current_config:
                payload["config"]["executorId"] = current_config["executorId"]

            if debug_mode is not None:
                payload["config"]["debugMode"] = debug_mode
            elif "debugMode" in current_config:
                payload["config"]["debugMode"] = current_config["debugMode"]

            if extra_args is not None:
                payload["config"]["extraArgs"] = extra_args
            elif "extraArgs" in current_config:
                payload["config"]["extraArgs"] = current_config["extraArgs"]

            if schedule_interval is not None or timezone is not None:
                payload["schedule"] = payload.get("schedule", {})
                current_schedule = current_source.get("schedule", {}) or {}

                if schedule_interval is not None:
                    payload["schedule"]["interval"] = schedule_interval
                elif current_schedule and "interval" in current_schedule:
                    payload["schedule"]["interval"] = current_schedule["interval"]

                if timezone is not None:
                    payload["schedule"]["timezone"] = timezone
                elif current_schedule and "timezone" in current_schedule:
                    payload["schedule"]["timezone"] = current_schedule["timezone"]

            self.logger.debug(f"REST API payload: {json.dumps(payload)}")

            response = requests.patch(
                f"{self.server_url}/openapi/v3/entity/datahubingestionsource/{urn}",
                headers=self.headers,
                json=payload,
            )

            if response.status_code == 200:
                self.logger.info(
                    f"Successfully patched ingestion source via REST API: {urn}"
                )
                return self.get_ingestion_source(urn)
            else:
                self.logger.warning(
                    f"PATCH endpoint failed: {response.status_code} - {response.text}"
                )
                # Try the PUT endpoint with OpenAPI v3 if PATCH fails
                try:
                    self.logger.info("Trying OpenAPI v3 PUT endpoint for patching")

                    # For PUT, we need to construct a full entity with all data
                    entity = {"urn": urn, "dataHubIngestionSourceInfo": {"value": {}}}

                    info = entity["dataHubIngestionSourceInfo"]["value"]

                    # Add fields from the current source
                    if current_source:
                        if "type" in current_source:
                            info["type"] = current_source["type"]
                        if "name" in current_source:
                            info["name"] = current_source["name"]
                        if "config" in current_source:
                            info["config"] = current_source["config"]
                        if "schedule" in current_source:
                            info["schedule"] = current_source["schedule"]

                    # Update with new values
                    if name is not None:
                        info["name"] = name
                    if source_type is not None:
                        info["type"] = source_type

                    # Update config
                    if "config" not in info:
                        info["config"] = {}

                    if recipe_str is not None:
                        info["config"]["recipe"] = recipe_str
                    if executor_id is not None:
                        info["config"]["executorId"] = executor_id
                    if debug_mode is not None:
                        info["config"]["debugMode"] = debug_mode
                    if extra_args is not None:
                        info["config"]["extraArgs"] = extra_args

                    # Update schedule
                    if schedule_interval is not None or timezone is not None:
                        if "schedule" not in info:
                            info["schedule"] = {}

                        if schedule_interval is not None:
                            info["schedule"]["interval"] = schedule_interval
                        if timezone is not None:
                            info["schedule"]["timezone"] = timezone

                    self.logger.debug(f"OpenAPI PUT payload: {json.dumps([entity])}")
                    response = requests.put(
                        f"{self.server_url}/openapi/v3/entity/datahubingestionsource",
                        headers=self.headers,
                        json=[entity],
                    )

                    if response.status_code in (200, 201):
                        self.logger.info(
                            f"Successfully patched ingestion source via OpenAPI PUT: {urn}"
                        )
                        return self.get_ingestion_source(urn)
                    else:
                        self.logger.error(
                            f"OpenAPI PUT failed: {response.status_code} - {response.text}"
                        )
                        return None
                except Exception as e:
                    self.logger.error(f"Exception with OpenAPI PUT endpoint: {str(e)}")
                    return None
        except Exception as e:
            self.logger.error(f"Error patching ingestion source via REST API: {str(e)}")
            return None

    def _deep_update(self, d, u):
        """Helper method to recursively update nested dictionaries."""
        for k, v in u.items():
            if isinstance(v, dict) and k in d and isinstance(d[k], dict):
                self._deep_update(d[k], v)
            else:
                d[k] = v

    def _deep_merge_dicts(self, d1, d2):
        """
        Deep merge two dictionaries recursively.
        d1 is updated with values from d2.

        Args:
            d1: First dictionary (base)
            d2: Second dictionary (to merge on top)

        Returns:
            Merged dictionary
        """
        result = d1.copy()

        for k, v in d2.items():
            if k in result and isinstance(result[k], dict) and isinstance(v, dict):
                # If both values are dicts, recursively merge them
                result[k] = self._deep_merge_dicts(result[k], v)
            else:
                # Otherwise just overwrite with the value from d2
                result[k] = v

        return result

    def run_ingestion_source(self, source_id: str) -> bool:
        """
        Trigger an ingestion source to run immediately.
        First tries direct GraphQL, then falls back to other methods if it fails.

        Args:
            source_id (str): The ID of the ingestion source to run

        Returns:
            bool: True if the ingestion source was successfully triggered, False otherwise
        """
        self.logger.info(f"Triggering immediate run for ingestion source: {source_id}")
        source_urn = f"urn:li:dataHubIngestionSource:{source_id}"

        # Primary approach: Use direct GraphQL endpoint
        try:
            graphql_query = {
                "operationName": "createIngestionExecutionRequest",
                "variables": {"input": {"ingestionSourceUrn": source_urn}},
                "query": """
                    mutation createIngestionExecutionRequest($input: CreateIngestionExecutionRequestInput!) {
                        createIngestionExecutionRequest(input: $input)
                    }
                """,
            }

            # Set up headers
            headers = (
                self.headers.copy()
                if hasattr(self, "headers")
                else {"Content-Type": "application/json"}
            )
            if hasattr(self, "token") and self.token:
                headers["Authorization"] = f"Bearer {self.token}"

            self.logger.debug(
                f"Attempting to trigger ingestion via direct GraphQL: {source_id}"
            )

            response = requests.post(
                f"{self.server_url}/api/v2/graphql", headers=headers, json=graphql_query
            )

            if response.status_code == 200:
                result = response.json()
                if "errors" not in result:
                    self.logger.info(
                        f"Successfully triggered ingestion source via direct GraphQL: {source_id}"
                    )
                    return True
                else:
                    error_msg = f"GraphQL errors when triggering ingestion: {result.get('errors')}"
                    self.logger.warning(error_msg)
            else:
                self.logger.warning(
                    f"Failed to trigger ingestion via direct GraphQL: {response.status_code} - {response.text}"
                )

            # Fall through to other methods
        except Exception as e:
            self.logger.warning(
                f"Error triggering ingestion via direct GraphQL: {str(e)}"
            )
            # Fall through to other methods

        # Fallback: Try with DataHubGraph client if available
        try:
            self.logger.debug(
                f"Attempting to trigger ingestion via DataHubGraph client: {source_id}"
            )

            mutation = """
            mutation createIngestionExecutionRequest($input: CreateIngestionExecutionRequestInput!) {
            createIngestionExecutionRequest(input: $input)
            }
            """

            variables = {"input": {"ingestionSourceUrn": source_urn}}

            result = self.execute_graphql(mutation, variables)

            if (
                result
                and "data" in result
                and "createIngestionExecutionRequest" in result["data"]
            ):
                self.logger.info(
                    f"Successfully triggered ingestion source via DataHubGraph client: {source_id}"
                )
                return True

            # Check for specific errors
            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.warning(f"GraphQL errors: {', '.join(error_messages)}")

            # Fall through to REST API approaches
        except Exception as e:
            self.logger.warning(
                f"Error using DataHubGraph client to trigger ingestion: {e}"
            )
            # Fall through to REST API approaches

        # Fallback approaches: Try various REST API endpoints

        # Build a list of potential endpoints to try
        endpoints = []

        # 1. OpenAPI v3 /runs endpoint (DataHub v0.12.0+)
        endpoints.append(
            {
                "url": f"{self.server_url}/runs?urn={source_urn}",
                "method": "post",
                "description": "OpenAPI v3 /runs endpoint",
            }
        )

        # 2. Various direct execution endpoints
        endpoints.append(
            {
                "url": f"{self.server_url}/api/v2/ingest/{source_id}",
                "method": "post",
                "description": "Direct ingest API",
            }
        )

        # 3. Legacy endpoint with action parameter
        endpoints.append(
            {
                "url": f"{self.server_url}/ingestion-sources/{source_id}?action=ingest",
                "method": "post",
                "description": "Legacy ingestion API with action=ingest",
            }
        )

        # 4. Try various OpenAPI specific endpoints
        endpoints.append(
            {
                "url": f"{self.server_url}/openapi/v3/ingest?urn={source_urn}",
                "method": "post",
                "description": "OpenAPI v3 ingest endpoint",
            }
        )

        endpoints.append(
            {
                "url": f"{self.server_url}/openapi/v3/ingestion/sources/{source_id}/run",
                "method": "post",
                "description": "OpenAPI v3 ingestion source run endpoint",
            }
        )

        # Try each endpoint in order until one succeeds
        for endpoint in endpoints:
            try:
                self.logger.debug(
                    f"Trying {endpoint['description']}: {endpoint['url']}"
                )

                if endpoint["method"].lower() == "post":
                    response = requests.post(endpoint["url"], headers=self.headers)
                else:
                    response = requests.get(endpoint["url"], headers=self.headers)

                if response.status_code in (200, 201, 202, 204):
                    self.logger.info(
                        f"Successfully triggered ingestion using {endpoint['description']}"
                    )
                    return True

                self.logger.debug(
                    f"Endpoint {endpoint['url']} returned {response.status_code}: {response.text}"
                )
            except Exception as e:
                self.logger.debug(f"Error with {endpoint['description']}: {str(e)}")

        # If we've tried all approaches and none worked, return failure
        self.logger.error(
            f"All methods to trigger ingestion for source {source_id} failed"
        )
        return False

    def list_secrets(self, start: int = 0, count: int = 100) -> List[Dict[str, Any]]:
        """
        List all secrets available in DataHub using GraphQL.

        Args:
            start: Starting index for pagination (0-based)
            count: Number of secrets to return

        Returns:
            List of dictionaries with secret information (name, urn, description)
        """
        query = """
            query listSecrets($input: ListSecretsInput!) {
              listSecrets(input: $input) {
                start
                count
                total
                secrets {
                  urn
                  name
                  description
                  __typename
                }
                __typename
              }
            }
        """

        variables = {"input": {"start": start, "count": count}}

        try:
            logger.info("Listing secrets using GraphQL")
            result = self.execute_graphql(query, variables)

            if "errors" in result:
                logger.warning(
                    f"GraphQL errors while listing secrets: {result['errors']}"
                )
                return []

            # Extract secrets from response
            secrets_data = (
                result.get("data", {}).get("listSecrets", {}).get("secrets", [])
            )
            logger.info(f"Retrieved {len(secrets_data)} secrets")
            return secrets_data
        except Exception as e:
            logger.error(f"Error listing secrets: {str(e)}")
            return []

    def list_policies(self, limit=100, start=0):
        """
        List all policies in DataHub.

        Args:
            limit (int): Maximum number of policies to return
            start (int): Starting offset for pagination

        Returns:
            list: List of policy objects
        """
        self.logger.info(f"Listing policies with limit={limit}, start={start}")

        # GraphQL query with correct structure
        query = """
        query listPolicies($input: ListPoliciesInput!) {
          listPolicies(input: $input) {
            start
            count
            total
            policies {
              urn
              type
              name
              description
              state
              resources {
                type
                allResources
                resources
                filter {
                  criteria {
                    field
                    values {
                      value
                      __typename
                    }
                    condition
                    __typename
                  }
                  __typename
                }
                __typename
              }
              privileges
              actors {
                users
                groups
                allUsers
                allGroups
                resourceOwners
                resourceOwnersTypes
                __typename
              }
              editable
              __typename
            }
            __typename
          }
        }
        """

        variables = {
            "input": {
                "start": start,
                "count": limit,
                "orFilters": [
                    {
                        "and": [
                            {
                                "field": "state",
                                "values": ["ACTIVE"],
                                "condition": "EQUAL",
                            }
                        ]
                    }
                ],
            }
        }

        try:
            result = self.execute_graphql(query, variables)

            if (
                result
                and "data" in result
                and result["data"]
                and "listPolicies" in result["data"]
            ):
                policies_data = result["data"]["listPolicies"]
                if policies_data is None:
                    self.logger.warning(
                        "listPolicies returned None in GraphQL response"
                    )
                    return []

                policies = policies_data.get("policies", [])
                if policies is None:
                    policies = []

                self.logger.info(f"Successfully retrieved {len(policies)} policies")
                return policies

            # Check for errors
            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]

                # Check for schema validation errors specifically
                schema_validation_errors = [
                    e
                    for e in error_messages
                    if "Unknown type 'ListPoliciesInput'" in e
                    or "Validation error" in e
                ]

                if schema_validation_errors:
                    # Log schema validation errors at info level since they're normal with different DataHub versions
                    self.logger.info(
                        f"GraphQL schema validation errors: {schema_validation_errors}"
                    )
                    self.logger.info(
                        "These errors are normal when using different DataHub versions. Falling back to REST API."
                    )
                else:
                    self.logger.warning(
                        f"GraphQL errors when listing policies: {', '.join(error_messages)}"
                    )
            else:
                self.logger.warning("Failed to retrieve policies using GraphQL")
        except Exception as e:
            self.logger.warning(f"Error listing policies via GraphQL: {str(e)}")

        # Fall back to OpenAPI v3 endpoint
        try:
            url = f"{self.server_url}/openapi/v3/entity/datahubpolicy"
            self.logger.debug(f"Listing policies via OpenAPI v3: GET {url}")

            # Set up headers
            headers = (
                self.headers.copy()
                if hasattr(self, "headers")
                else {"Content-Type": "application/json"}
            )
            if hasattr(self, "token") and self.token:
                headers["Authorization"] = f"Bearer {self.token}"

            response = requests.get(
                url, headers=headers, params={"start": start, "count": limit}
            )
            if response.status_code == 200:
                data = response.json()
                entities = data.get("entities", [])
                policies = []

                for entity in entities:
                    try:
                        policy_urn = entity.get("urn")
                        policy_id = policy_urn.split(":")[-1] if policy_urn else None

                        if not policy_id:
                            continue

                        # Extract policy info
                        policy_info = entity.get("dataHubPolicyInfo", {})
                        if not policy_info:
                            continue

                        policy_value = policy_info.get("value", {})
                        if not policy_value:
                            continue

                        # Create simplified policy object
                        policy = {
                            "urn": policy_urn,
                            "id": policy_id,
                            "name": policy_value.get("displayName", ""),
                            "description": policy_value.get("description", ""),
                            "type": policy_value.get("type", ""),
                            "state": policy_value.get("state", ""),
                            "privileges": policy_value.get("privileges", []),
                            "resources": policy_value.get("resources", {}),
                            "actors": policy_value.get("actors", {}),
                        }

                        policies.append(policy)
                    except Exception as e:
                        self.logger.warning(f"Error processing policy: {str(e)}")

                self.logger.info(
                    f"Successfully retrieved {len(policies)} policies via OpenAPI v3"
                )
                return policies
            else:
                self.logger.warning(
                    f"Failed to list policies via OpenAPI v3: {response.status_code} - {response.text}"
                )
        except Exception as e:
            self.logger.warning(f"Error listing policies via OpenAPI v3: {str(e)}")

        return []

    def get_policy(self, policy_id):
        """
        Get a policy from DataHub by ID or URN.

        This method attempts to get the policy using GraphQL first and falls back to REST API if needed.

        Args:
            policy_id (str): Policy ID or URN

        Returns:
            dict: Policy details or None if not found
        """
        if not policy_id:
            self.logger.error("Policy ID is required")
            return None

        self.logger.info(f"Getting policy: {policy_id}")

        # Convert ID to URN if necessary
        policy_urn = policy_id
        if not policy_id.startswith("urn:"):
            policy_urn = f"urn:li:dataHubPolicy:{policy_id}"

        # Try GraphQL first
        graphql_query = """
        query policy($urn: String!) {
          policy(urn: $urn) {
            urn
            type
            id
            name
            description
            state
            privileges
            resources {
              type
              allResources
              resources
              filter {
                criteria {
                  field
                  values {
                    value
                  }
                  condition
                }
              }
            }
            actors {
              users
              groups
              allUsers
              allGroups
              resourceOwners
              resourceOwnersTypes
            }
            editable
          }
        }
        """

        variables = {"urn": policy_urn}

        try:
            result = self.execute_graphql(graphql_query, variables)
            if (
                result
                and "data" in result
                and result["data"]
                and "policy" in result["data"]
            ):
                policy_data = result["data"]["policy"]
                if policy_data:
                    self.logger.info(
                        f"Successfully retrieved policy {policy_id} via GraphQL"
                    )
                    return policy_data

            # Check for errors
            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]

                # Check for schema validation errors specifically
                schema_validation_errors = [
                    e
                    for e in error_messages
                    if "Unknown type" in e or "Validation error" in e
                ]

                if schema_validation_errors:
                    # Log schema validation errors at info level since they're normal with different DataHub versions
                    self.logger.info(
                        f"GraphQL schema validation errors when getting policy: {schema_validation_errors}"
                    )
                    self.logger.info(
                        "These errors are normal when using different DataHub versions. Falling back to REST API."
                    )
                else:
                    # Log other errors as warnings
                    self.logger.warning(
                        f"GraphQL policy retrieval failed: {', '.join(error_messages)}, falling back to REST API"
                    )
            else:
                self.logger.warning(
                    "GraphQL policy retrieval failed with unknown error, falling back to REST API"
                )
        except Exception as e:
            self.logger.warning(f"Error retrieving policy via GraphQL: {str(e)}")

        # Fall back to OpenAPI v3 endpoint
        try:
            url = f"{self.server_url}/openapi/v3/entity/datahubpolicy/{policy_urn}"
            self.logger.info(f"Attempting to get policy via OpenAPI v3: GET {url}")

            # Set up headers
            headers = (
                self.headers.copy()
                if hasattr(self, "headers")
                else {"Content-Type": "application/json"}
            )
            if hasattr(self, "token") and self.token:
                headers["Authorization"] = f"Bearer {self.token}"

            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                entity_data = response.json()

                # Extract the policy information
                if (
                    "dataHubPolicyInfo" in entity_data
                    and entity_data["dataHubPolicyInfo"]
                ):
                    policy_info = entity_data["dataHubPolicyInfo"]
                    policy_value = policy_info.get("value", {})

                    # Create a simplified policy object
                    policy_data = {
                        "urn": entity_data.get("urn", ""),
                        "id": entity_data.get("urn", "").split(":")[-1],
                        "name": policy_value.get("displayName", ""),
                        "description": policy_value.get("description", ""),
                        "type": policy_value.get("type", ""),
                        "state": policy_value.get("state", ""),
                        "privileges": policy_value.get("privileges", []),
                        "resources": policy_value.get("resources", {}),
                        "actors": policy_value.get("actors", {}),
                    }

                    self.logger.info(
                        f"Successfully retrieved policy {policy_id} via OpenAPI v3"
                    )
                    return policy_data
                else:
                    self.logger.warning(
                        f"Policy response missing dataHubPolicyInfo: {entity_data}"
                    )
            else:
                self.logger.warning(
                    f"Failed to get policy via OpenAPI v3: {response.status_code} - {response.text}"
                )
        except Exception as e:
            self.logger.error(f"Error retrieving policy via OpenAPI v3: {str(e)}")

        self.logger.error(f"Policy not found: {policy_id}")
        return None

    def create_policy(self, policy_data):
        """
        Create a new policy in DataHub.

        This method attempts to create the policy using GraphQL first and falls back to REST API if needed.

        Args:
            policy_data (dict): Policy details including:
                - name: Name of the policy (required)
                - description: Description of the policy
                - type: Policy type (e.g., METADATA)
                - state: Policy state (e.g., ACTIVE, INACTIVE)
                - resources: Dict containing resources the policy applies to
                - privileges: List of privileges granted by the policy
                - actors: Dict containing users/groups the policy applies to

        Returns:
            dict: Created policy details or None if creation failed
        """
        self.logger.info(
            f"Creating new policy with name: {policy_data.get('name', 'Unnamed')}"
        )

        if not policy_data.get("name"):
            self.logger.error("Policy name is required")
            return None

        # Prepare input for GraphQL
        graphql_input = policy_data.copy()

        # Important: Remove 'id' field from GraphQL input since it's not part of PolicyUpdateInput
        # This is a key difference between GraphQL and REST API schemas
        if "id" in graphql_input:
            policy_id = graphql_input.pop("id")
            self.logger.info(
                f"Removed 'id' field from GraphQL input (value: {policy_id})"
            )

        # Ensure resources is properly formatted as a dict with filter if it's a list
        if "resources" in graphql_input and isinstance(
            graphql_input["resources"], list
        ):
            # Convert list format to the expected structure with filter.criteria
            resources_data = {"filter": {"criteria": []}}

            # Add resource type information if available
            if graphql_input["resources"] and "type" in graphql_input["resources"][0]:
                resources_data["type"] = graphql_input["resources"][0].get("type")

            # Add allResources if applicable
            if (
                not graphql_input["resources"]
                or graphql_input["resources"][0].get("resource") == "*"
            ):
                resources_data["allResources"] = True
            elif "resource" in graphql_input["resources"][0]:
                resources_data["resources"] = [
                    r.get("resource")
                    for r in graphql_input["resources"]
                    if "resource" in r
                ]

            graphql_input["resources"] = resources_data

        # Ensure actors object has all required fields
        if "actors" in graphql_input:
            default_actors = {
                "users": [],
                "groups": [],
                "allUsers": False,
                "allGroups": False,
                "resourceOwners": False,
                "resourceOwnersTypes": None,
            }

            if isinstance(graphql_input["actors"], dict):
                # Update with provided values
                actors_data = {**default_actors, **graphql_input["actors"]}
                graphql_input["actors"] = actors_data
        else:
            # Set default actors if not provided
            graphql_input["actors"] = {
                "users": [],
                "groups": [],
                "allUsers": True,
                "allGroups": False,
                "resourceOwners": False,
                "resourceOwnersTypes": None,
            }

        # Try GraphQL first
        graphql_mutation = """
        mutation createPolicy($input: PolicyUpdateInput!) {
          createPolicy(input: $input)
        }
        """

        variables = {"input": graphql_input}

        try:
            result = self.execute_graphql(graphql_mutation, variables)
            if (
                result
                and "data" in result
                and result["data"]
                and "createPolicy" in result["data"]
            ):
                created_policy_urn = result["data"]["createPolicy"]
                if created_policy_urn:
                    self.logger.info(
                        f"Successfully created policy {policy_data.get('name')} via GraphQL"
                    )

                    # Return policy data with URN
                    policy_with_urn = {**policy_data, "urn": created_policy_urn}
                    return policy_with_urn

            # Check for errors
            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]

                # Check for schema validation errors specifically
                schema_validation_errors = [
                    e
                    for e in error_messages
                    if "Unknown type 'PolicyUpdateInput'" in e
                    or "Validation error" in e
                    or "contains a field name" in e  # Additional error pattern to catch
                ]

                if schema_validation_errors:
                    # Log schema validation errors at info level since they're normal with different DataHub versions
                    self.logger.info(
                        f"GraphQL schema validation errors when creating policy: {schema_validation_errors}"
                    )
                    self.logger.info(
                        "These errors are normal when using different DataHub versions. Falling back to REST API."
                    )
                else:
                    # Log other errors as warnings
                    self.logger.warning(
                        f"GraphQL policy creation failed: {', '.join(error_messages)}, falling back to REST API"
                    )
            else:
                self.logger.warning(
                    "GraphQL policy creation failed with unknown error, falling back to REST API"
                )
        except Exception as e:
            self.logger.warning(f"Error creating policy via GraphQL: {str(e)}")

        # Fall back to OpenAPI v3 endpoint
        try:
            url = f"{self.server_url}/openapi/v3/entity/datahubpolicy"
            self.logger.info(f"Attempting to create policy via OpenAPI v3: {url}")

            # Set up headers
            headers = (
                self.headers.copy()
                if hasattr(self, "headers")
                else {"Content-Type": "application/json"}
            )
            if hasattr(self, "token") and self.token:
                headers["Authorization"] = f"Bearer {self.token}"

            # Format the request according to the OpenAPI v3 specification
            # Use explicit ID from original policy_data if available, otherwise fallback to name
            policy_id = policy_data.get("id") or policy_data.get(
                "name", ""
            ).lower().replace(" ", "-")
            if not policy_id:
                policy_id = str(uuid.uuid4())

            # Create the nested structure required by OpenAPI v3
            request_body = [
                {
                    "urn": f"urn:li:dataHubPolicy:{policy_id}",
                    "dataHubPolicyKey": {
                        "value": {"id": policy_id},
                        "systemMetadata": {
                            "runId": "manual-creation",
                            "lastObserved": int(time.time() * 1000),
                        },
                    },
                    "dataHubPolicyInfo": {
                        "value": {
                            "displayName": policy_data.get("name", ""),
                            "description": policy_data.get("description", ""),
                            "type": policy_data.get("type", "METADATA"),
                            "state": policy_data.get("state", "ACTIVE"),
                            "privileges": policy_data.get("privileges", []),
                            "editable": True,
                        },
                        "systemMetadata": {
                            "runId": "manual-creation",
                            "lastObserved": int(time.time() * 1000),
                        },
                    },
                }
            ]

            # Add resources if available
            if "resources" in policy_data:
                resources_data = {}

                if isinstance(policy_data["resources"], dict):
                    # Already in the correct format
                    resources_data = policy_data["resources"]
                elif isinstance(policy_data["resources"], list):
                    # Convert list to the expected format
                    if (
                        policy_data["resources"]
                        and "type" in policy_data["resources"][0]
                    ):
                        resources_data["type"] = policy_data["resources"][0].get("type")

                    if (
                        not policy_data["resources"]
                        or policy_data["resources"][0].get("resource") == "*"
                    ):
                        resources_data["allResources"] = True
                    elif "resource" in policy_data["resources"][0]:
                        resources_data["resources"] = [
                            r.get("resource")
                            for r in policy_data["resources"]
                            if "resource" in r
                        ]

                request_body[0]["dataHubPolicyInfo"]["value"]["resources"] = (
                    resources_data
                )

            # Add actors if available
            if "actors" in policy_data:
                request_body[0]["dataHubPolicyInfo"]["value"]["actors"] = policy_data[
                    "actors"
                ]

            response = requests.post(url, headers=headers, json=request_body)
            # A 202 status is common for successfully accepted requests
            if response.status_code in (200, 201, 202):
                self.logger.info(
                    f"Successfully created policy {policy_data.get('name')} via OpenAPI v3"
                )

                # Return a simplified policy object that matches the GraphQL response format
                created_policy = {
                    **policy_data,
                    "urn": f"urn:li:dataHubPolicy:{policy_id}",
                    "id": policy_id,
                }
                return created_policy
            else:
                # Check if the response indicates success despite non-200 status
                try:
                    resp_json = response.json()
                    # Some DataHub versions return a list of objects on success
                    if (
                        isinstance(resp_json, list)
                        and len(resp_json) > 0
                        and "urn" in resp_json[0]
                    ):
                        self.logger.info(
                            f"Policy created successfully despite status code {response.status_code}"
                        )
                        created_policy = {
                            **policy_data,
                            "urn": resp_json[0]["urn"],
                            "id": policy_id,
                        }
                        return created_policy
                except Exception:
                    pass

                self.logger.error(
                    f"Failed to create policy via OpenAPI v3: {response.status_code} - {response.text}"
                )
        except Exception as e:
            self.logger.error(f"Error creating policy via REST API: {str(e)}")

        return None

    def update_policy(self, policy_id, policy_data):
        """
        Update an existing policy in DataHub.

        This method attempts to update the policy using GraphQL first and falls back to REST API if needed.

        Args:
            policy_id (str): ID or URN of the policy to update
            policy_data (dict): Policy data to update with the same fields as create_policy

        Returns:
            dict: Updated policy details or None if update failed
        """
        self.logger.info(f"Updating policy with ID: {policy_id}")

        if not policy_id:
            self.logger.error("Policy ID is required")
            return None

        # Convert ID to URN if necessary
        policy_urn = policy_id
        if not policy_id.startswith("urn:"):
            policy_urn = f"urn:li:dataHubPolicy:{policy_id}"

        # Prepare input for GraphQL
        graphql_input = policy_data.copy()

        # Ensure resources is properly formatted as a dict with filter if it's a list
        if "resources" in graphql_input and isinstance(
            graphql_input["resources"], list
        ):
            # Convert list format to the expected structure with filter.criteria
            resources_data = {"filter": {"criteria": []}}

            # Add resource type information if available
            if graphql_input["resources"] and "type" in graphql_input["resources"][0]:
                resources_data["type"] = graphql_input["resources"][0].get("type")

            # Add allResources if applicable
            if (
                not graphql_input["resources"]
                or graphql_input["resources"][0].get("resource") == "*"
            ):
                resources_data["allResources"] = True
            elif "resource" in graphql_input["resources"][0]:
                resources_data["resources"] = [
                    r.get("resource")
                    for r in graphql_input["resources"]
                    if "resource" in r
                ]

            graphql_input["resources"] = resources_data

        # Ensure actors object has all required fields
        if "actors" in graphql_input:
            default_actors = {
                "users": [],
                "groups": [],
                "allUsers": False,
                "allGroups": False,
                "resourceOwners": False,
                "resourceOwnersTypes": None,
            }

            if isinstance(graphql_input["actors"], dict):
                # Update with provided values
                actors_data = {**default_actors, **graphql_input["actors"]}
                graphql_input["actors"] = actors_data
        else:
            # Set default actors if not provided
            graphql_input["actors"] = {
                "users": [],
                "groups": [],
                "allUsers": True,
                "allGroups": False,
                "resourceOwners": False,
                "resourceOwnersTypes": None,
            }

        # Try GraphQL first
        graphql_mutation = """
        mutation updatePolicy($urn: String!, $input: PolicyUpdateInput!) {
          updatePolicy(urn: $urn, input: $input)
        }
        """

        variables = {"urn": policy_urn, "input": graphql_input}

        try:
            result = self.execute_graphql(graphql_mutation, variables)
            if (
                result
                and "data" in result
                and result["data"]
                and "updatePolicy" in result["data"]
            ):
                updated_policy_urn = result["data"]["updatePolicy"]
                if updated_policy_urn:
                    self.logger.info(
                        f"Successfully updated policy {policy_id} via GraphQL"
                    )

                    # Return policy data with URN
                    policy_with_urn = {**policy_data, "urn": updated_policy_urn}
                    return policy_with_urn

            # Check for errors
            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]

                # Check for schema validation errors specifically
                schema_validation_errors = [
                    e
                    for e in error_messages
                    if "Unknown type 'PolicyUpdateInput'" in e
                    or "Validation error" in e
                ]

                if schema_validation_errors:
                    # Log schema validation errors at info level since they're normal with different DataHub versions
                    self.logger.info(
                        f"GraphQL schema validation errors when updating policy: {schema_validation_errors}"
                    )
                    self.logger.info(
                        "These errors are normal when using different DataHub versions. Falling back to REST API."
                    )
                else:
                    # Log other errors as warnings
                    self.logger.warning(
                        f"GraphQL policy update failed: {', '.join(error_messages)}, falling back to REST API"
                    )
            else:
                self.logger.warning(
                    "GraphQL policy update failed with unknown error, falling back to REST API"
                )
        except Exception as e:
            self.logger.warning(f"Error updating policy via GraphQL: {str(e)}")

        # Fall back to OpenAPI v3 endpoint
        try:
            url = f"{self.server_url}/openapi/v3/entity/datahubpolicy"
            self.logger.debug(f"Updating policy via OpenAPI v3: PATCH {url}")

            # Set up headers
            headers = (
                self.headers.copy()
                if hasattr(self, "headers")
                else {"Content-Type": "application/json"}
            )
            if hasattr(self, "token") and self.token:
                headers["Authorization"] = f"Bearer {self.token}"

            # Extract policy ID from URN if needed
            policy_id = policy_urn.split(":")[-1] if policy_urn else policy_id

            # Create the patch operations
            patch_operations = []

            # Add operations for each field in policy_data
            for field, value in policy_data.items():
                if field == "name":
                    patch_operations.append(
                        {"op": "replace", "path": "/displayName", "value": value}
                    )
                elif field == "description":
                    patch_operations.append(
                        {"op": "replace", "path": "/description", "value": value}
                    )
                elif field in ["type", "state", "privileges", "actors", "resources"]:
                    patch_operations.append(
                        {"op": "replace", "path": f"/{field}", "value": value}
                    )

            # Create the nested structure required by OpenAPI v3 for patching
            request_body = [
                {
                    "urn": policy_urn,
                    "dataHubPolicyInfo": {
                        "value": {"patch": patch_operations, "forceGenericPatch": True},
                        "systemMetadata": {
                            "runId": "manual-update",
                            "lastObserved": int(time.time() * 1000),
                        },
                    },
                }
            ]

            response = requests.patch(url, headers=headers, json=request_body)

            if response.status_code in (200, 201, 204):
                self.logger.info(
                    f"Successfully updated policy {policy_id} via OpenAPI v3"
                )

                # Return the updated policy data
                updated_policy = {**policy_data, "urn": policy_urn, "id": policy_id}
                return updated_policy

            self.logger.error(
                f"Failed to update policy via OpenAPI v3: {response.status_code} - {response.text}"
            )
        except Exception as e:
            self.logger.error(f"Error updating policy via OpenAPI v3: {str(e)}")

        return None

    def delete_policy(self, policy_id):
        """
        Delete a policy from DataHub.

        This method attempts to delete the policy using GraphQL first and falls back to REST API if needed.

        Args:
            policy_id (str): ID or URN of the policy to delete

        Returns:
            bool: True if deletion was successful, False otherwise
        """
        self.logger.info(f"Deleting policy with ID: {policy_id}")

        if not policy_id:
            self.logger.error("Policy ID is required")
            return False

        # Convert ID to URN if necessary
        policy_urn = policy_id
        if not policy_id.startswith("urn:"):
            policy_urn = f"urn:li:dataHubPolicy:{policy_id}"

        # Try GraphQL first
        graphql_mutation = """
        mutation deletePolicy($urn: String!) {
          deletePolicy(urn: $urn)
        }
        """

        variables = {"urn": policy_urn}

        try:
            result = self.execute_graphql(graphql_mutation, variables)
            if (
                result
                and "data" in result
                and result["data"]
                and "deletePolicy" in result["data"]
            ):
                if result["data"]["deletePolicy"]:
                    self.logger.info(
                        f"Successfully deleted policy {policy_id} via GraphQL"
                    )
                    return True

            # Check for errors
            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]

                # Check for schema validation errors specifically
                schema_validation_errors = [
                    e
                    for e in error_messages
                    if "Unknown type" in e or "Validation error" in e
                ]

                if schema_validation_errors:
                    # Log schema validation errors at info level since they're normal with different DataHub versions
                    self.logger.info(
                        f"GraphQL schema validation errors when deleting policy: {schema_validation_errors}"
                    )
                    self.logger.info(
                        "These errors are normal when using different DataHub versions. Falling back to REST API."
                    )
                else:
                    # Log other errors as warnings
                    self.logger.warning(
                        f"GraphQL policy deletion failed: {', '.join(error_messages)}, falling back to REST API"
                    )
            else:
                self.logger.warning(
                    "GraphQL policy deletion failed with unknown error, falling back to REST API"
                )
        except Exception as e:
            self.logger.warning(f"Error deleting policy via GraphQL: {str(e)}")

        # Fall back to OpenAPI v3 endpoint
        try:
            url = f"{self.server_url}/openapi/v3/entity/datahubpolicy/{policy_urn}"
            self.logger.info(
                f"Attempting to delete policy via OpenAPI v3: DELETE {url}"
            )

            # Set up headers
            headers = (
                self.headers.copy()
                if hasattr(self, "headers")
                else {"Content-Type": "application/json"}
            )
            if hasattr(self, "token") and self.token:
                headers["Authorization"] = f"Bearer {self.token}"

            response = requests.delete(url, headers=headers)

            if response.status_code in (200, 204):
                self.logger.info(
                    f"Successfully deleted policy {policy_id} via OpenAPI v3"
                )
                return True
            else:
                self.logger.error(
                    f"Failed to delete policy via OpenAPI v3: {response.status_code} - {response.text}"
                )
        except Exception as e:
            self.logger.error(f"Error deleting policy via OpenAPI v3: {str(e)}")

        return False

    # Tag Management Methods

    def list_tags(self, query="*", start=0, count=100) -> List[Dict[str, Any]]:
        """
        List tags in DataHub with comprehensive information including ownership and relationships.

        Args:
            query (str): Search query to filter tags (default: "*")
            start (int): Starting offset for pagination
            count (int): Maximum number of tags to return

        Returns:
            List of tag objects with detailed information
        """
        self.logger.info(
            f"Listing tags with query: {query}, start: {start}, count: {count}"
        )
        self.logger.debug(
            f"Server URL: {self.server_url}, Token provided: {self.token is not None and len(self.token) > 0}, Verify SSL: {self.verify_ssl}"
        )

        # Enhanced GraphQL query with ownership and relationships
        graphql_query = """
        query GetTags($input: SearchAcrossEntitiesInput!) {
          searchAcrossEntities(input: $input) {
            start
            count
            total
            searchResults {
              entity {
                ... on Tag {
                  urn
                  type  
                  properties {
                    name
                    description
                    colorHex
                  }
                  ownership {
                    owners {
                      owner {
                        ... on CorpUser { urn, username, properties { displayName } }
                        ... on CorpGroup { urn, name, properties { displayName } }
                      }
                      ownershipType { urn, info { name } }
                      source {
                        type
                        url
                      }
                    }
                    lastModified {
                      time
                      actor
                    }
                  }
                }
              }
            }
          }
        }
        """

        variables = {
            "input": {
                "types": ["TAG"],
                "query": query,
                "start": start,
                "count": count,
                "filters": [],
            }
        }

        try:
            result = self.execute_graphql(graphql_query, variables)

            if result and "data" in result and "searchAcrossEntities" in result["data"]:
                search_results = result["data"]["searchAcrossEntities"]["searchResults"]
                tags = []

                for item in search_results:
                    if "entity" in item and item["entity"] is not None:
                        entity = item["entity"]
                        
                        # Skip if entity is None (defensive programming)
                        if entity is None:
                            continue
                        
                        # Extract basic tag information with proper None checking
                        properties = entity.get("properties") or {}
                        tag = {
                            "urn": entity.get("urn"),
                            "type": entity.get("type"),
                            "name": properties.get("name"),
                            "description": properties.get("description"),
                            "colorHex": properties.get("colorHex"),
                        }
                        
                        # Add properties for backward compatibility
                        if properties:
                            tag["properties"] = {
                                "name": properties.get("name"),
                                "description": properties.get("description"), 
                                "colorHex": properties.get("colorHex"),
                            }

                        # Add ownership information with proper None checking
                        ownership = entity.get("ownership")
                        if ownership:
                            tag["ownership"] = ownership
                            
                            # Extract owner count and names for display
                            owners = ownership.get("owners") or []
                            tag["owners_count"] = len(owners)
                            tag["owner_names"] = []
                            
                            for owner_info in owners:
                                if not owner_info:
                                    continue
                                owner = owner_info.get("owner") or {}
                                if owner.get("username"):  # CorpUser
                                    owner_props = owner.get("properties") or {}
                                    display_name = owner_props.get("displayName")
                                    tag["owner_names"].append(display_name or owner["username"])
                                elif owner.get("name"):  # CorpGroup
                                    owner_props = owner.get("properties") or {}
                                    display_name = owner_props.get("displayName")
                                    tag["owner_names"].append(display_name or owner["name"])
                        else:
                            tag["owners_count"] = 0
                            tag["owner_names"] = []

                        # Set relationships count to 0 since we're not querying for them
                        tag["relationships_count"] = 0

                        tags.append(tag)

                return tags

            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.warning(
                    f"GraphQL errors when listing tags: {', '.join(error_messages)}"
                )

            return []
        except Exception as e:
            self.logger.error(f"Error listing tags: {str(e)}")
            return []

    def get_remote_tags_data(self, query="*", start=0, count=100) -> Dict[str, Any]:
        """
        Get remote tags data for async loading with comprehensive information.
        
        Args:
            query (str): Search query to filter tags
            start (int): Starting offset for pagination
            count (int): Maximum number of tags to return
            
        Returns:
            Dict containing enhanced tags data with statistics
        """
        try:
            # Use the enhanced list_tags method
            tags = self.list_tags(query=query, start=start, count=count)
            
            # Get total count from a separate query
            total_result = self.execute_graphql(
                """
                query GetTagsCount($input: SearchAcrossEntitiesInput!) {
                  searchAcrossEntities(input: $input) {
                    total
                  }
                }
                """,
                {
                    "input": {
                        "types": ["TAG"],
                        "query": query,
                        "start": 0,
                        "count": 1,
                        "filters": [],
                    }
                }
            )
            
            total_count = 0
            if total_result and "data" in total_result and "searchAcrossEntities" in total_result["data"]:
                total_count = total_result["data"]["searchAcrossEntities"].get("total", 0)
            
            # Calculate statistics
            owned_tags = [tag for tag in tags if tag.get("owners_count", 0) > 0]
            tags_with_relationships = [tag for tag in tags if tag.get("relationships_count", 0) > 0]
            
            return {
                "success": True,
                "data": {
                    "tags": tags,
                    "total": total_count,
                    "start": start,
                    "count": len(tags),
                    "statistics": {
                        "total_tags": total_count,
                        "owned_tags": len(owned_tags),
                        "tags_with_relationships": len(tags_with_relationships),
                        "percentage_owned": round((len(owned_tags) / len(tags) * 100) if tags else 0, 1),
                    }
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting remote tags data: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }

    def get_tag(self, tag_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a tag by its URN.

        Args:
            tag_urn (str): The URN of the tag to retrieve

        Returns:
            dict: Tag information or None if not found
        """
        self.logger.info(f"Getting tag: {tag_urn}")

        graphql_query = """
        query getTag($urn: String!) {
          tag(urn: $urn) {
            urn
            type
            name
            description
            properties {
              name
              colorHex
              __typename
            }
            ownership {
              owners {
                owner {
                  ... on CorpUser {
                    urn
                    username
                    properties {
                      displayName
                      email
                    }
                  }
                  ... on CorpGroup {
                    urn
                    name
                    properties {
                      displayName
                      email
                    }
                  }
                }
                type
                ownershipType {
                  urn
                  type
                  info {
                    name
                    description
                  }
                }
              }
            }
            __typename
          }
        }
        """

        variables = {"urn": tag_urn}

        try:
            result = self.execute_graphql(graphql_query, variables)

            if result and "data" in result and "tag" in result["data"]:
                return result["data"]["tag"]

            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.warning(
                    f"GraphQL errors when getting tag: {', '.join(error_messages)}"
                )

            return None
        except Exception as e:
            self.logger.error(f"Error getting tag: {str(e)}")
            return None

    def create_tag(
        self, tag_id: str, name: str, description: str = ""
    ) -> Optional[str]:
        """
        Create a new tag.

        Args:
            tag_id (str): Tag ID
            name (str): Tag name
            description (str): Tag description

        Returns:
            str: URN of the created tag, or None if unsuccessful
        """
        self.logger.info(f"Creating tag with ID {tag_id}")

        mutation = """
        mutation createTag($input: CreateTagInput!) {
          createTag(input: $input)
        }
        """

        variables = {"input": {"id": tag_id, "name": name, "description": description}}

        try:
            result = self.execute_graphql(mutation, variables)

            if result and "data" in result and "createTag" in result["data"]:
                created_urn = result["data"]["createTag"]
                self.logger.info(f"Successfully created tag: {created_urn}")
                return created_urn

            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                
                # Check if the error is that the tag already exists
                if any("This Tag already exists!" in msg for msg in error_messages):
                    self.logger.info(f"Tag with ID {tag_id} already exists, attempting to retrieve existing tag")
                    
                    # Construct the expected URN and try to get the existing tag
                    expected_urn = f"urn:li:tag:{tag_id}"
                    existing_tag = self.get_tag(expected_urn)
                    
                    if existing_tag and existing_tag.get("urn"):
                        self.logger.info(f"Successfully found existing tag: {existing_tag['urn']}")
                        # Update the description if it's different
                        if description and existing_tag.get("description") != description:
                            self.update_tag_description(existing_tag["urn"], description)
                        return existing_tag["urn"]
                
                self.logger.error(
                    f"GraphQL errors when creating tag: {', '.join(error_messages)}"
                )

            return None
        except Exception as e:
            self.logger.error(f"Error creating tag: {str(e)}")
            return None

    def create_or_update_tag(
        self, tag_id: str, name: str, description: str = ""
    ) -> Optional[str]:
        """
        Create a new tag or return existing tag if it already exists.
        This is a convenience method that wraps create_tag with better error handling.

        Args:
            tag_id (str): Tag ID
            name (str): Tag name
            description (str): Tag description

        Returns:
            str: URN of the created or existing tag, or None if unsuccessful
        """
        return self.create_tag(tag_id, name, description)

    def set_tag_color(self, tag_urn: str, color_hex: str) -> bool:
        """
        Set the color of a tag.

        Args:
            tag_urn (str): Tag URN
            color_hex (str): Hex color code (e.g., "#d23939")

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Setting color for tag {tag_urn} to {color_hex}")

        mutation = """
        mutation setTagColor($urn: String!, $colorHex: String!) {
          setTagColor(urn: $urn, colorHex: $colorHex)
        }
        """

        variables = {"urn": tag_urn, "colorHex": color_hex}

        try:
            result = self.execute_graphql(mutation, variables)

            if result and "data" in result and "setTagColor" in result["data"]:
                success = result["data"]["setTagColor"]
                if success:
                    self.logger.info(f"Successfully set color for tag {tag_urn}")
                    return True

            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.error(
                    f"GraphQL errors when setting tag color: {', '.join(error_messages)}"
                )

            return False
        except Exception as e:
            self.logger.error(f"Error setting tag color: {str(e)}")
            return False

    def update_tag_description(self, tag_urn: str, description: str) -> bool:
        """
        Update the description of a tag.

        Args:
            tag_urn (str): Tag URN
            description (str): New description

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Updating description for tag {tag_urn}")

        mutation = """
        mutation updateDescription($input: DescriptionUpdateInput!) {
          updateDescription(input: $input)
        }
        """

        variables = {"input": {"resourceUrn": tag_urn, "description": description}}

        try:
            result = self.execute_graphql(mutation, variables)

            if result and "data" in result and "updateDescription" in result["data"]:
                success = result["data"]["updateDescription"]
                if success:
                    self.logger.info(
                        f"Successfully updated description for tag {tag_urn}"
                    )
                    return True

            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.error(
                    f"GraphQL errors when updating tag description: {', '.join(error_messages)}"
                )

            return False
        except Exception as e:
            self.logger.error(f"Error updating tag description: {str(e)}")
            return False

    def add_tag_owner(
        self,
        tag_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner",
    ) -> bool:
        """
        Add an owner to a tag.

        Args:
            tag_urn (str): Tag URN
            owner_urn (str): Owner URN (urn:li:corpuser:username or urn:li:corpGroup:groupname)
            ownership_type (str): Ownership type URN

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Adding owner {owner_urn} to tag {tag_urn}")

        # Determine the owner entity type
        owner_entity_type = "CORP_USER" if "corpuser:" in owner_urn else "CORP_GROUP"

        mutation = """
        mutation batchAddOwners($input: BatchAddOwnersInput!) {
          batchAddOwners(input: $input)
        }
        """

        variables = {
            "input": {
                "owners": [
                    {
                        "ownerUrn": owner_urn,
                        "ownerEntityType": owner_entity_type,
                        "ownershipTypeUrn": ownership_type,
                    }
                ],
                "resources": [{"resourceUrn": tag_urn}],
            }
        }

        try:
            result = self.execute_graphql(mutation, variables)

            if result and "data" in result and "batchAddOwners" in result["data"]:
                success = result["data"]["batchAddOwners"]
                if success:
                    self.logger.info(
                        f"Successfully added owner {owner_urn} to tag {tag_urn}"
                    )
                    return True

            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.error(
                    f"GraphQL errors when adding tag owner: {', '.join(error_messages)}"
                )

            return False
        except Exception as e:
            self.logger.error(f"Error adding tag owner: {str(e)}")
            return False

    def remove_tag_owner(
        self,
        tag_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner",
    ) -> bool:
        """
        Remove an owner from a tag.

        Args:
            tag_urn (str): Tag URN
            owner_urn (str): Owner URN to remove
            ownership_type (str): Ownership type URN

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Removing owner {owner_urn} from tag {tag_urn}")

        mutation = """
        mutation removeOwner($input: RemoveOwnerInput!) {
          removeOwner(input: $input)
        }
        """

        variables = {
            "input": {
                "ownerUrn": owner_urn,
                "ownershipTypeUrn": ownership_type,
                "resourceUrn": tag_urn,
            }
        }

        try:
            result = self.execute_graphql(mutation, variables)

            if result and "data" in result and "removeOwner" in result["data"]:
                success = result["data"]["removeOwner"]
                if success:
                    self.logger.info(
                        f"Successfully removed owner {owner_urn} from tag {tag_urn}"
                    )
                    return True

            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.error(
                    f"GraphQL errors when removing tag owner: {', '.join(error_messages)}"
                )

            return False
        except Exception as e:
            self.logger.error(f"Error removing tag owner: {str(e)}")
            return False

    def create_domain(self, domain_id: str, name: str, description: str = "", parent_domain_urn: str = None) -> Optional[str]:
        """
        Create a new domain in DataHub.

        Args:
            domain_id (str): Domain ID (will be used to construct URN)
            name (str): Domain name
            description (str): Domain description
            parent_domain_urn (str): Parent domain URN (optional)

        Returns:
            str: Domain URN if successful, None otherwise
        """
        self.logger.info(f"Creating domain: {name} with ID: {domain_id}")

        mutation = """
        mutation createDomain($input: CreateDomainInput!) {
          createDomain(input: $input)
        }
        """

        input_data = {
            "id": domain_id,
            "name": name,
            "description": description
        }
        
        if parent_domain_urn:
            input_data["parentDomain"] = parent_domain_urn

        variables = {"input": input_data}

        try:
            result = self.execute_graphql(mutation, variables)

            if result and "data" in result and "createDomain" in result["data"]:
                domain_urn = result["data"]["createDomain"]
                if domain_urn:
                    self.logger.info(f"Successfully created domain {name} with URN: {domain_urn}")
                    return domain_urn

            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.error(f"GraphQL errors when creating domain: {', '.join(error_messages)}")

            return None
        except Exception as e:
            self.logger.error(f"Error creating domain: {str(e)}")
            return None

    def update_domain_description(self, domain_urn: str, description: str) -> bool:
        """
        Update the description of a domain.

        Args:
            domain_urn (str): Domain URN
            description (str): New description

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Updating description for domain {domain_urn}")

        mutation = """
        mutation updateDescription($input: DescriptionUpdateInput!) {
          updateDescription(input: $input)
        }
        """

        variables = {"input": {"resourceUrn": domain_urn, "description": description}}

        try:
            result = self.execute_graphql(mutation, variables)

            if result and "data" in result and "updateDescription" in result["data"]:
                success = result["data"]["updateDescription"]
                if success:
                    self.logger.info(
                        f"Successfully updated description for domain {domain_urn}"
                    )
                    return True

            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.error(
                    f"GraphQL errors when updating domain description: {', '.join(error_messages)}"
                )

            return False
        except Exception as e:
            self.logger.error(f"Error updating domain description: {str(e)}")
            return False

    def update_domain_display_properties(self, domain_urn: str, color_hex: str = None, icon: Dict[str, str] = None) -> bool:
        """
        Update the display properties of a domain.

        Args:
            domain_urn (str): Domain URN
            color_hex (str): Hex color code (e.g., "#914b4b")
            icon (dict): Icon configuration with name, style, iconLibrary

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Updating display properties for domain {domain_urn}")

        mutation = """
        mutation updateDisplayProperties($urn: String!, $input: DisplayPropertiesUpdateInput!) {
          updateDisplayProperties(urn: $urn, input: $input)
        }
        """

        input_data = {}
        if color_hex:
            input_data["colorHex"] = color_hex
        if icon:
            input_data["icon"] = icon

        variables = {"urn": domain_urn, "input": input_data}

        try:
            result = self.execute_graphql(mutation, variables)

            if result and "data" in result and "updateDisplayProperties" in result["data"]:
                success = result["data"]["updateDisplayProperties"]
                if success:
                    self.logger.info(
                        f"Successfully updated display properties for domain {domain_urn}"
                    )
                    return True

            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.error(
                    f"GraphQL errors when updating domain display properties: {', '.join(error_messages)}"
                )

            return False
        except Exception as e:
            self.logger.error(f"Error updating domain display properties: {str(e)}")
            return False

    def update_domain_structured_properties(self, domain_urn: str, structured_properties: List[Dict[str, Any]]) -> bool:
        """
        Update structured properties of a domain.

        Args:
            domain_urn (str): Domain URN
            structured_properties (list): List of structured property updates

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Updating structured properties for domain {domain_urn}")

        mutation = """
        mutation upsertStructuredProperties($input: UpsertStructuredPropertiesInput!) {
          upsertStructuredProperties(input: $input) {
            properties {
              ...structuredPropertiesFields
              __typename
            }
            __typename
          }
        }

        fragment structuredPropertiesFields on StructuredPropertiesEntry {
          structuredProperty {
            ...structuredPropertyFields
            __typename
          }
          values {
            ... on StringValue {
              stringValue
              __typename
            }
            ... on NumberValue {
              numberValue
              __typename
            }
            __typename
          }
          valueEntities {
            urn
            type
            ...entityDisplayNameFields
            __typename
          }
          associatedUrn
          __typename
        }

        fragment structuredPropertyFields on StructuredPropertyEntity {
          urn
          type
          exists
          definition {
            displayName
            qualifiedName
            description
            cardinality
            immutable
            valueType {
              urn
              type
              info {
                type
                displayName
                __typename
              }
              __typename
            }
            entityTypes {
              urn
              type
              info {
                type
                __typename
              }
              __typename
            }
            cardinality
            filterStatus
            typeQualifier {
              allowedTypes {
                urn
                type
                info {
                  type
                  displayName
                  __typename
                }
                __typename
              }
              __typename
            }
            allowedValues {
              value {
                ... on StringValue {
                  stringValue
                  __typename
                }
                ... on NumberValue {
                  numberValue
                  __typename
                }
                __typename
              }
              description
              __typename
            }
            created {
              time
              actor {
                urn
                editableProperties {
                  displayName
                  pictureLink
                  __typename
                }
                ...entityDisplayNameFields
                __typename
              }
              __typename
            }
            lastModified {
              time
              actor {
                urn
                editableProperties {
                  displayName
                  pictureLink
                  __typename
                }
                ...entityDisplayNameFields
                __typename
              }
              __typename
            }
            __typename
          }
          settings {
            isHidden
            showInSearchFilters
            showAsAssetBadge
            showInAssetSummary
            showInColumnsTable
            __typename
          }
          __typename
        }

        fragment entityDisplayNameFields on Entity {
          urn
          type
          ... on Dataset {
            name
            properties {
              name
              qualifiedName
              __typename
            }
            __typename
          }
          ... on CorpUser {
            username
            editableProperties {
              displayName
              email
              __typename
            }
            properties {
              displayName
              title
              firstName
              lastName
              fullName
              email
              __typename
            }
            info {
              active
              displayName
              title
              firstName
              lastName
              fullName
              email
              __typename
            }
            __typename
          }
          ... on CorpGroup {
            name
            info {
              displayName
              __typename
            }
            __typename
          }
          ... on Dashboard {
            dashboardId
            properties {
              name
              __typename
            }
            __typename
          }
          ... on Chart {
            chartId
            properties {
              name
              __typename
            }
            __typename
          }
          ... on DataFlow {
            properties {
              name
              __typename
            }
            __typename
          }
          ... on DataJob {
            jobId
            properties {
              name
              __typename
            }
            __typename
          }
          ... on GlossaryTerm {
            name
            hierarchicalName
            properties {
              name
              __typename
            }
            parentNodes {
              nodes {
                urn
                __typename
              }
              __typename
            }
            __typename
          }
          ... on GlossaryNode {
            properties {
              name
              description
              __typename
            }
            __typename
          }
          ... on Domain {
            properties {
              name
              __typename
            }
            __typename
          }
          ... on Container {
            properties {
              name
              __typename
            }
            __typename
          }
          ... on MLFeatureTable {
            name
            __typename
          }
          ... on MLFeature {
            name
            __typename
          }
          ... on MLPrimaryKey {
            name
            __typename
          }
          ... on MLModel {
            name
            __typename
          }
          ... on MLModelGroup {
            name
            __typename
          }
          ... on Tag {
            name
            properties {
              name
              colorHex
              __typename
            }
            __typename
          }
          ... on DataPlatform {
            ...nonConflictingPlatformFields
            __typename
          }
          ... on DataProduct {
            properties {
              name
              __typename
            }
            __typename
          }
          ... on Application {
            properties {
              name
              __typename
            }
            __typename
          }
          ... on DataPlatformInstance {
            instanceId
            __typename
          }
          ... on StructuredPropertyEntity {
            definition {
              displayName
              qualifiedName
              __typename
            }
            __typename
          }
          ... on SchemaFieldEntity {
            fieldPath
            __typename
          }
          ... on OwnershipTypeEntity {
            info {
              name
              __typename
            }
            __typename
          }
          __typename
        }

        fragment nonConflictingPlatformFields on DataPlatform {
          urn
          type
          name
          properties {
            displayName
            datasetNameDelimiter
            logoUrl
            __typename
          }
          displayName
          info {
            type
            displayName
            datasetNameDelimiter
            logoUrl
            __typename
          }
          __typename
        }
        """

        variables = {
            "input": {
                "assetUrn": domain_urn,
                "structuredPropertyInputParams": structured_properties,
            }
        }

        try:
            result = self.execute_graphql(mutation, variables)

            if result and "data" in result and "upsertStructuredProperties" in result["data"]:
                success = result["data"]["upsertStructuredProperties"]
                if success:
                    self.logger.info(
                        f"Successfully updated structured properties for domain {domain_urn}"
                    )
                    return True

            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.error(
                    f"GraphQL errors when updating domain structured properties: {', '.join(error_messages)}"
                )

            return False
        except Exception as e:
            self.logger.error(f"Error updating domain structured properties: {str(e)}")
            return False

    def add_domain_owner(
        self,
        domain_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner",
    ) -> bool:
        """
        Add an owner to a domain.

        Args:
            domain_urn (str): Domain URN
            owner_urn (str): Owner URN (urn:li:corpuser:username or urn:li:corpGroup:groupname)
            ownership_type (str): Ownership type URN

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Adding owner {owner_urn} to domain {domain_urn}")

        # Determine the owner entity type
        owner_entity_type = "CORP_USER" if "corpuser:" in owner_urn else "CORP_GROUP"

        mutation = """
        mutation batchAddOwners($input: BatchAddOwnersInput!) {
          batchAddOwners(input: $input)
        }
        """

        variables = {
            "input": {
                "owners": [
                    {
                        "ownerUrn": owner_urn,
                        "ownerEntityType": owner_entity_type,
                        "ownershipTypeUrn": ownership_type,
                    }
                ],
                "resources": [{"resourceUrn": domain_urn}],
            }
        }

        try:
            result = self.execute_graphql(mutation, variables)

            if result and "data" in result and "batchAddOwners" in result["data"]:
                success = result["data"]["batchAddOwners"]
                if success:
                    self.logger.info(
                        f"Successfully added owner {owner_urn} to domain {domain_urn}"
                    )
                    return True

            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.error(
                    f"GraphQL errors when adding domain owner: {', '.join(error_messages)}"
                )

            return False
        except Exception as e:
            self.logger.error(f"Error adding domain owner: {str(e)}")
            return False

    def delete_tag(self, tag_urn: str) -> bool:
        """
        Delete a tag from DataHub.

        Args:
            tag_urn (str): Tag URN to delete

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Deleting tag: {tag_urn}")

        mutation = """
        mutation deleteTag($urn: String!) {
          deleteTag(urn: $urn)
        }
        """

        variables = {"urn": tag_urn}

        try:
            result = self.execute_graphql(mutation, variables)
            self.logger.debug(f"Delete tag GraphQL result: {result}")

            # Check for explicit errors first
            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.error(
                    f"GraphQL errors when deleting tag: {', '.join(error_messages)}"
                )
                return False

            # If we have a result with data and no errors, consider it successful
            if result and "data" in result:
                delete_result = result["data"].get("deleteTag")
                self.logger.debug(f"Delete tag success value: {delete_result}")
                
                # For delete operations, we'll be very permissive:
                # - True = explicit success
                # - None/null = likely success (common in GraphQL mutations)
                # - Only False = explicit failure
                if delete_result is not False:
                    self.logger.info(f"Successfully deleted tag {tag_urn} (result: {delete_result})")
                    return True
                else:
                    self.logger.warning(f"Tag deletion returned explicit False: {tag_urn}")
                    return False

            # If we have any result without errors, assume success
            if result:
                self.logger.info(f"Tag deletion assumed successful (no errors): {tag_urn}")
                return True

            self.logger.warning(f"No result returned for tag deletion: {tag_urn}")
            return False
        except Exception as e:
            self.logger.error(f"Error deleting tag: {str(e)}")
            return False

    def add_tag_to_entity(
        self, entity_urn: str, tag_urn: str, color_hex: str = None
    ) -> bool:
        """
        Add a tag to an entity.

        Args:
            entity_urn (str): Entity URN to tag
            tag_urn (str): Tag URN to add

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Adding tag {tag_urn} to entity {entity_urn}")

        mutation = """
        mutation addTag($input: AddTagInput!) {
          addTag(input: $input)
        }
        """

        variables = {
            "input": {"resourceUrn": entity_urn, "tag": tag_urn, "colorHex": color_hex}
        }

        try:
            result = self.execute_graphql(mutation, variables)

            if result and "data" in result and "addTag" in result["data"]:
                success = result["data"]["addTag"]
                if success:
                    self.logger.info(
                        f"Successfully added tag {tag_urn} to entity {entity_urn}"
                    )
                    return True

            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.error(
                    f"GraphQL errors when adding tag to entity: {', '.join(error_messages)}"
                )

            return False
        except Exception as e:
            self.logger.error(f"Error adding tag to entity: {str(e)}")
            return False

    def remove_tag_from_entity(self, entity_urn: str, tag_urn: str) -> bool:
        """
        Remove a tag from an entity.

        Args:
            entity_urn (str): Entity URN
            tag_urn (str): Tag URN to remove

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Removing tag {tag_urn} from entity {entity_urn}")

        mutation = """
        mutation removeTag($input: RemoveTagInput!) {
          removeTag(input: $input)
        }
        """

        variables = {"input": {"resourceUrn": entity_urn, "tagUrn": tag_urn}}

        try:
            result = self.execute_graphql(mutation, variables)

            if result and "data" in result and "removeTag" in result["data"]:
                success = result["data"]["removeTag"]
                if success:
                    self.logger.info(
                        f"Successfully removed tag {tag_urn} from entity {entity_urn}"
                    )
                    return True

            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.error(
                    f"GraphQL errors when removing tag from entity: {', '.join(error_messages)}"
                )

            return False
        except Exception as e:
            self.logger.error(f"Error removing tag from entity: {str(e)}")
            return False

    def find_entities_with_metadata(
        self, field_type: str, metadata_urn: str, start: int = 0, count: int = 10
    ) -> Dict[str, Any]:
        """
        Find entities that have a specific tag, glossary term, or domain assigned.

        Args:
            field_type (str): Type of metadata field to search by ("tags", "glossaryTerms", or "domains")
            metadata_urn (str): URN of the tag, glossary term, or domain to search for
            start (int): Pagination start index
            count (int): Number of entities to return

        Returns:
            Dict containing search results with entities that have the specified metadata
        """
        self.logger.info(f"Finding entities with {field_type} = {metadata_urn}")

        if field_type not in ["tags", "glossaryTerms", "domains"]:
            self.logger.error(
                f"Invalid field_type: {field_type}. Must be one of 'tags', 'glossaryTerms', or 'domains'"
            )
            return {"searchResults": []}

        # This is a complex query that returns entities with extensive metadata
        query = """
        query getSearchResultsForMultiple($input: SearchAcrossEntitiesInput!) {
          searchAcrossEntities(input: $input) {
            start
            count
            total
            searchResults {
              entity {
                urn
                type
                ... on Tag {
                  name
                  properties {
                    name
                    colorHex
                    __typename
                  }
                  description
                  __typename
                }
                ... on Dashboard {
                  dashboardId
                  properties {
                    name
                    description
                  }
                  platform {
                    name
                  }
                }
                ... on Chart {
                  chartId
                  properties {
                    name
                    description
                  }
                }
                ... on DataFlow {
                  flowId
                  properties {
                    name
                    description
                  }
                }
                ... on DataJob {
                  jobId
                  properties {
                    name
                    description
                  }
                }
                ... on GlossaryTerm {
                  name
                  properties {
                    name
                    description
                  }
                }
                ... on Tag {
                  name
                  properties {
                    name
                    colorHex
                  }
                  description
                }
                ... on CorpUser {
                  username
                  properties {
                    displayName
                    email
                  }
                }
                ... on CorpGroup {
                  name
                  info {
                    displayName
                  }
                }
                ... on DataProduct {
                  properties {
                    name
                    description
                  }
                }
              }
            }
            facets {
              field
              displayName
              aggregations {
                value
                count
                entity {
                  urn
                  type
                }
              }
            }
          }
        }
        """

        variables = {
            "input": {
                "types": [],
                "query": "",
                "start": start,
                "count": count,
                "filters": [],
                "orFilters": [
                    {
                        "and": [
                            {
                                "field": field_type,
                                "condition": "EQUAL",
                                "values": [metadata_urn],
                                "negated": False,
                            }
                        ]
                    }
                ],
                "searchFlags": {
                    "getSuggestions": True,
                    "includeStructuredPropertyFacets": True,
                },
            }
        }

        try:
            result = self.execute_graphql(query, variables)

            if result and "data" in result and "searchAcrossEntities" in result["data"]:
                search_results = result["data"]["searchAcrossEntities"]

                # Process and simplify results
                simplified_results = {
                    "start": search_results.get("start", 0),
                    "count": search_results.get("count", 0),
                    "total": search_results.get("total", 0),
                    "entities": [],
                    "facets": search_results.get("facets", []),
                }

                # Extract and simplify entity information
                for item in search_results.get("searchResults", []):
                    if "entity" in item and item["entity"] is not None:
                        entity = item["entity"]

                        # Create a base entity object with common fields
                        simplified_entity = {
                            "urn": entity.get("urn"),
                            "type": entity.get("type"),
                        }

                        # Add entity type-specific fields
                        if entity.get("type") == "DATASET":
                            simplified_entity.update(
                                {
                                    "name": entity.get("name"),
                                    "platform": entity.get("platform", {}).get("name")
                                    if entity.get("platform")
                                    else None,
                                    "platformDisplay": (
                                        entity.get("platform", {})
                                        .get("properties", {})
                                        .get("displayName")
                                    )
                                    if entity.get("platform")
                                    and entity.get("platform", {}).get("properties")
                                    else None,
                                    "description": entity.get("properties", {}).get(
                                        "description"
                                    )
                                    if entity.get("properties")
                                    else None,
                                }
                            )
                        elif entity.get("type") == "DASHBOARD":
                            simplified_entity.update(
                                {
                                    "name": entity.get("properties", {}).get("name")
                                    if entity.get("properties")
                                    else None,
                                    "description": entity.get("properties", {}).get(
                                        "description"
                                    )
                                    if entity.get("properties")
                                    else None,
                                    "dashboardId": entity.get("dashboardId"),
                                    "platform": entity.get("platform", {}).get("name")
                                    if entity.get("platform")
                                    else None,
                                }
                            )
                        elif entity.get("type") == "CHART":
                            simplified_entity.update(
                                {
                                    "name": entity.get("properties", {}).get("name")
                                    if entity.get("properties")
                                    else None,
                                    "description": entity.get("properties", {}).get(
                                        "description"
                                    )
                                    if entity.get("properties")
                                    else None,
                                    "chartId": entity.get("chartId"),
                                }
                            )
                        elif entity.get("type") == "DATA_FLOW":
                            simplified_entity.update(
                                {
                                    "name": entity.get("properties", {}).get("name")
                                    if entity.get("properties")
                                    else None,
                                    "description": entity.get("properties", {}).get(
                                        "description"
                                    )
                                    if entity.get("properties")
                                    else None,
                                    "flowId": entity.get("flowId"),
                                }
                            )
                        elif entity.get("type") == "DATA_JOB":
                            simplified_entity.update(
                                {
                                    "name": entity.get("properties", {}).get("name")
                                    if entity.get("properties")
                                    else None,
                                    "description": entity.get("properties", {}).get(
                                        "description"
                                    )
                                    if entity.get("properties")
                                    else None,
                                    "jobId": entity.get("jobId"),
                                }
                            )
                        elif entity.get("type") == "GLOSSARY_TERM":
                            simplified_entity.update(
                                {
                                    "name": entity.get("name"),
                                    "description": entity.get("properties", {}).get(
                                        "description"
                                    )
                                    if entity.get("properties")
                                    else None,
                                }
                            )
                        elif entity.get("type") == "TAG":
                            simplified_entity.update(
                                {
                                    "name": entity.get("name"),
                                    "description": entity.get("description"),
                                    "colorHex": entity.get("properties", {}).get(
                                        "colorHex"
                                    )
                                    if entity.get("properties")
                                    else None,
                                }
                            )
                        elif entity.get("type") == "CORP_USER":
                            simplified_entity.update(
                                {
                                    "username": entity.get("username"),
                                    "displayName": entity.get("properties", {}).get(
                                        "displayName"
                                    )
                                    if entity.get("properties")
                                    else None,
                                    "email": entity.get("properties", {}).get("email")
                                    if entity.get("properties")
                                    else None,
                                }
                            )
                        elif entity.get("type") == "CORP_GROUP":
                            simplified_entity.update(
                                {
                                    "name": entity.get("name"),
                                    "displayName": entity.get("info", {}).get(
                                        "displayName"
                                    )
                                    if entity.get("info")
                                    else None,
                                }
                            )
                        elif entity.get("type") == "DATA_PRODUCT":
                            simplified_entity.update(
                                {
                                    "name": entity.get("properties", {}).get("name")
                                    if entity.get("properties")
                                    else None,
                                    "description": entity.get("properties", {}).get(
                                        "description"
                                    )
                                    if entity.get("properties")
                                    else None,
                                }
                            )

                        simplified_results["entities"].append(simplified_entity)

                return simplified_results

            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.warning(
                    f"GraphQL errors when finding entities: {', '.join(error_messages)}"
                )

            return {"entities": [], "total": 0}
        except Exception as e:
            self.logger.error(
                f"Error finding entities with {field_type} {metadata_urn}: {str(e)}"
            )
            return {"entities": [], "total": 0}

    def find_entities_with_domain(self, domain_urn: str, start: int = 0, count: int = 50) -> Dict[str, Any]:
        """
        Find entities that belong to a specific domain.

        Args:
            domain_urn (str): URN of the domain to search for
            start (int): Pagination start index
            count (int): Number of entities to return

        Returns:
            Dict containing search results with entities that belong to the domain
        """
        return self.find_entities_with_metadata("domains", domain_urn, start, count)

    def list_glossary_nodes(self, query=None, count=100, start=0):
        """
        List glossary nodes from DataHub with optional filtering.

        Args:
            query (str, optional): Search query to filter nodes by name/description
            count (int, optional): Number of nodes to return (default 100)
            start (int, optional): Starting offset for pagination (default 0)

        Returns:
            list: List of glossary node objects
        """
        self.logger.info(
            f"Listing glossary nodes with query={query}, count={count}, start={start}"
        )

        try:
            self.logger.info("Using comprehensive GraphQL search for glossary nodes")

            # Use the comprehensive query that properly captures parent relationships
            search_query = """
            query GetAllGlossaryNodes($input: SearchAcrossEntitiesInput!) {
              searchAcrossEntities(input: $input) {
                start
                count
                total
                searchResults {
                  entity {
                    urn
                    type
                    ... on GlossaryNode {
                      urn
                      ownership {
                        owners {
                          owner {
                            ...on CorpUser {
                              urn
                              username
                            }
                            ...on CorpGroup {
                              urn
                              name
                            }
                          }
                        }
                        lastModified {
                          time
                          actor
                        }
                      }
                      properties {
                        name
                        description
                        customProperties {
                          key
                          value
                        }
                      }
                      relationships(input: {types: ["isA", "hasA", "relatedTo"], direction: OUTGOING, start: 0, count: 50}) {
                        relationships {
                          type
                          direction
                          entity {
                            urn
                            type
                            ... on GlossaryTerm {
                  properties {
                    name
                              }
                            }
                            ... on GlossaryNode {
                              properties {
                                name
                              }
                            }
                          }
                          created {
                            time
                            actor
                          }
                        }
                }
                parentNodes {
                  nodes {
                    urn
                    properties {
                      name
                    }
                  }
                }
              }
            }
                }
              }
            }
            """

            variables = {
                "input": {
                    "query": query or "*",
                    "types": ["GLOSSARY_NODE"],
                    "count": count,
                    "start": start,
                }
            }

            self.logger.debug(
                f"Executing comprehensive GraphQL search query with variables: {variables}"
            )
            result = self.execute_graphql(search_query, variables)
            self.logger.debug(f"Search result: {result}")

            if result and "data" in result and "searchAcrossEntities" in result["data"]:
                search_data = result["data"]["searchAcrossEntities"]
                total = search_data.get("total", 0)

                if total > 0:
                    search_results = search_data.get("searchResults", [])

                    # Process results into standardized format
                    processed_nodes = []
                    for result in search_results:
                        entity = result.get("entity", {})

                        # Extract basic properties
                        urn = entity.get("urn")
                        entity_type = entity.get("type")

                        # Get properties if available
                        properties = entity.get("properties", {})
                        name = properties.get("name", "Unknown")
                        description = properties.get("description", "")

                        # Create processed node
                        processed_node = {
                            "urn": urn,
                            "type": entity_type,
                            "name": name,
                            "description": description,
                            "properties": properties,
                        }

                        # Add parent node info if available
                        parent_nodes = entity.get("parentNodes", {}).get("nodes", [])
                        if parent_nodes and len(parent_nodes) > 0:
                            parent = parent_nodes[0]  # Usually a node has one parent
                            processed_node["parent_urn"] = parent.get("urn")
                            processed_node["parentNode"] = {
                                "urn": parent.get("urn"),
                                "name": parent.get("properties", {}).get(
                                    "name", "Unknown Parent"
                                ),
                            }

                        processed_nodes.append(processed_node)

                    self.logger.info(
                        f"Successfully retrieved {len(processed_nodes)} glossary nodes via comprehensive GraphQL search"
                    )
                    return processed_nodes
                else:
                    self.logger.info("No glossary nodes found in search results")
                    return []
            else:
                errors = self._get_graphql_errors(result)
                if errors:
                    self.logger.warning(f"GraphQL search returned errors: {errors}")

        except Exception as e:
            self.logger.warning(
                f"Error using comprehensive GraphQL search for glossary nodes: {str(e)}"
            )

        # All approaches failed, return empty list
        self.logger.warning("Comprehensive glossary node GraphQL query failed")
        return []

    def get_glossary_node(self, node_urn):
        """
        Get a specific glossary node by URN (basic version without children).

        Args:
            node_urn (str): The URN of the glossary node to retrieve

        Returns:
            dict: Dictionary with basic node details or None if not found
        """
        self.logger.info(f"Getting glossary node with URN: {node_urn}")

        try:
            # Simple query for just the node details
            node_query = """
            query($urn: String!) {
              glossaryNode(urn: $urn) {
                    urn
                    type
                      properties {
                        name
                        description
                }
                parentNodes {
                  nodes {
                    urn
                    properties {
                      name
                    }
                  }
                }
              }
            }
            """

            variables = {"urn": node_urn}

            self.logger.debug(f"Executing GraphQL query for node details: {node_urn}")
            result = self.execute_graphql(node_query, variables)

            if (
                not result
                or "data" not in result
                or "glossaryNode" not in result["data"]
                or result["data"]["glossaryNode"] is None
            ):
                errors = self._get_graphql_errors(result)
                if errors:
                    self.logger.warning(
                        f"GraphQL query for node {node_urn} returned errors: {errors}"
                    )
                else:
                    self.logger.warning(f"Node {node_urn} not found in DataHub")
                return None

            node_data = result["data"]["glossaryNode"]

            # Process the node data - simplified version
            properties = node_data.get("properties", {})
            parent_nodes_data = node_data.get("parentNodes", {}) or {}
            parent_nodes = parent_nodes_data.get("nodes", []) if parent_nodes_data else []
            
            processed_node = {
                "urn": node_data.get("urn"),
                "type": node_data.get("type"),
                "name": properties.get("name", "Unknown"),
                "description": properties.get("description", ""),
                "properties": properties,
                "parentNodes": [{"urn": p.get("urn"), "name": p.get("properties", {}).get("name", "") if p.get("properties") else ""} for p in parent_nodes if p]
            }

            return processed_node
        except Exception as e:
            self.logger.error(f"Error getting glossary node {node_urn}: {str(e)}")
            return None

    def get_glossary_term(self, term_urn):
        """
        Get a specific glossary term by URN.

        Args:
            term_urn (str): The URN of the glossary term to retrieve

        Returns:
            dict: Dictionary with term details or None if not found
        """
        self.logger.info(f"Getting glossary term with URN: {term_urn}")

        try:
            # Query for the specific term
            term_query = """
            query($urn: String!) {
              glossaryTerm(urn: $urn) {
                urn
                type
                properties {
                  name
                  description
                  termSource
                  sourceRef
                  sourceUrl
                  customProperties {
                    key
                    value
                  }
                }
                domain {
                  domain {
                    urn
                    properties {
                      name
                      description
                    }
                  }
                }
                ownership {
                  owners {
                    owner {
                      ... on CorpUser {
                        urn
                        username
                        properties {
                          displayName
                          email
                        }
                      }
                      ... on CorpGroup {
                        urn
                        name
                        properties {
                          displayName
                        }
                      }
                    }
                    ownershipType {
                      urn
                      info {
                        name
                      }
                    }
                  }
                  lastModified {
                    actor
                    time
                  }
                }
                parentNodes {
                  nodes {
                    ... on GlossaryNode {
                      urn
                      properties {
                        name
                      }
                    }
                  }
                }
                deprecation {
                  deprecated
                }
                relationships(input: {types: ["IsA", "HasA"], direction: OUTGOING}) {
                  relationships {
                    entity {
                      ... on GlossaryTerm {
                        urn
                        properties {
                          name
                        }
                      }
                    }
                  }
                }
                structuredProperties {
                  properties {
                    structuredProperty {
                      urn
                      definition {
                        displayName
                        qualifiedName
                      }
                    }
                    values {
                      ... on StringValue {
                        stringValue
                      }
                      ... on NumberValue {
                        numberValue
                      }
                    }
                  }
                }
              }
            }
            """

            variables = {"urn": term_urn}

            self.logger.debug(f"Executing GraphQL query for term details: {term_urn}")
            result = self.execute_graphql(term_query, variables)

            if (
                not result
                or "data" not in result
                or "glossaryTerm" not in result["data"]
                or result["data"]["glossaryTerm"] is None
            ):
                errors = self._get_graphql_errors(result)
                if errors:
                    self.logger.warning(
                        f"GraphQL query for term {term_urn} returned errors: {errors}"
                    )
                else:
                    self.logger.warning(f"Term {term_urn} not found in DataHub")
                return None

            term_data = result["data"]["glossaryTerm"]

            # Process the term data using the same logic as _process_glossary_term
            return self._process_glossary_term(term_data)

        except Exception as e:
            self.logger.error(f"Error getting glossary term {term_urn}: {str(e)}")
            return None

    def create_glossary_node(self, node_id, name, description="", parent_urn=None):
        """
        Create a new glossary node in DataHub.

        Args:
            node_id (str): ID for the node (will be used in the URN)
            name (str): Display name for the node
            description (str, optional): Description for the node
            parent_urn (str, optional): URN of the parent node

        Returns:
            str: URN of the created node or None if failed
        """
        self.logger.info(f"Creating glossary node: {name}")

    def list_glossary_terms(self, node_urn=None, query=None, count=100, start=0):
        """
        List glossary terms, optionally filtered by parent node or search query.

        Args:
            node_urn (str, optional): Filter terms by parent node URN
            query (str, optional): Search query string to filter terms
            count (int, optional): Maximum number of terms to return
            start (int, optional): Offset for pagination

        Returns:
            list: List of glossary term objects
        """
        self.logger.info(
            f"Listing glossary terms with node_urn={node_urn}, query={query}, count={count}, start={start}"
        )

        try:
            # Build a comprehensive query for GLOSSARY_TERM entity search
            self.logger.info("Using comprehensive GraphQL search for glossary terms")

            search_query = """
            query GetAllGlossaryTerms($input: SearchAcrossEntitiesInput!) {
                searchAcrossEntities(input: $input) {
                    start
                    count
                    total
                    searchResults {
                        entity {
                            urn
                            type
                    ... on GlossaryTerm {
                      urn
                      ownership {
                        owners {
                          owner {
                            ...on CorpUser {
                              urn
                            }
                            ...on CorpGroup {
                                                    urn
                                                }
                                            }
                                        }
                                    }
                      domain {
                        domain {
                          urn
                        }
                      }
                      institutionalMemory {
                        elements {
                          url
                          label
                          actor {
                            ... on CorpUser {
                              urn
                              username
                            }
                            ... on CorpGroup {
                              urn
                              name
                            }
                          }
                          created {
                            time
                            actor
                          }
                          updated {
                            time
                            actor
                          }
                          settings {
                            showInAssetPreview
                          }
                        }
                      }
                      properties {
                        name
                                    description
                        customProperties {
                          key
                          value
                                }
                            }
                      glossaryTermInfo {
                        name
                                    description
                        termSource
                        sourceRef
                        sourceUrl
                        customProperties {
                          key
                          value
                          associatedUrn
                        }
                        rawSchema
                      }
                      deprecation {
                        deprecated
                      }
                      relationships(input: {types: ["isA", "hasA", "relatedTo"], direction: OUTGOING, start: 0, count: 50}) {
                        relationships {
                          type
                          direction
                          entity {
                            urn
                            type
                            ... on GlossaryTerm {
                              properties {
                                name
                              }
                            }
                            ... on GlossaryNode {
                              properties {
                                name
                              }
                            }
                          }
                          created {
                            time
                            actor
                          }
                        }
                      }
                      parentNodes {
                        nodes {
                          urn
                          properties {
                            name
                            }
                        }
                    }
                }
            }
                }
              }
            }
            """

            # Build search query string
            search_string = "*"
            if node_urn:
                # Filter by parent node URN
                search_string = f'parentNodes.urn:"{node_urn}"'
            elif query:
                search_string = query

            search_variables = {
                "input": {
                    "query": search_string,
                    "types": ["GLOSSARY_TERM"],
                    "count": count,
                    "start": start,
                }
            }

            result = self.execute_graphql(search_query, search_variables)

            if result and "data" in result and "searchAcrossEntities" in result["data"]:
                search_across_entities = result["data"]["searchAcrossEntities"]

                if search_across_entities is None:
                    return []

                search_results = search_across_entities.get("searchResults", [])

                if not search_results:
                    self.logger.info("No glossary terms found in search results")
                    return []

                # Process terms into a standardized format
                processed_terms = []

                for i, result_item in enumerate(search_results):
                    try:
                        if result_item is None:
                            continue

                        entity = result_item.get("entity") if result_item else None

                        if entity is None:
                            continue

                        if entity.get("type") != "GLOSSARY_TERM":
                            continue

                        term_urn = entity.get("urn")

                        # Get properties from multiple possible locations
                        properties = entity.get("properties", {}) or {}
                        glossary_term_info = entity.get("glossaryTermInfo", {}) or {}

                        # Extract term data, preferring glossaryTermInfo over properties
                        name = glossary_term_info.get("name") or properties.get(
                            "name", "Unknown"
                        )
                        description = glossary_term_info.get(
                            "description"
                        ) or properties.get("description", "")
                        term_source = glossary_term_info.get("termSource", "INTERNAL")

                        # Find parent node information
                        parent_nodes_data = entity.get("parentNodes", {}) or {}
                        parent_nodes = (
                            parent_nodes_data.get("nodes", [])
                            if parent_nodes_data
                            else []
                        )
                        parent_node_urn = None
                        parent_node_name = None

                        if parent_nodes and len(parent_nodes) > 0:
                            first_parent = parent_nodes[0]
                            if first_parent:
                                parent_node_urn = first_parent.get("urn")
                                parent_properties = (
                                    first_parent.get("properties", {}) or {}
                                )
                                parent_node_name = parent_properties.get(
                                    "name", "Unknown"
                                )

                        # Format the term data
                        term_data = {
                            "urn": term_urn,
                            "type": entity.get("type"),
                            "name": name,
                            "description": description,
                            "term_source": term_source,
                            "parent_node_urn": parent_node_urn,
                            "parent_node_name": parent_node_name,
                            "properties": properties,
                            "glossaryTermInfo": glossary_term_info,
                        }

                        # Extract the term ID from the URN for local lookup
                        try:
                            term_id = term_urn.split("/")[-1] if term_urn else "unknown"
                            term_data["id"] = term_id
                        except Exception:
                            term_data["id"] = "unknown"

                        processed_terms.append(term_data)

                    except Exception as e:
                        self.logger.error(
                            f"Error processing search result {i}: {str(e)}"
                        )
                        continue

                self.logger.info(
                    f"Successfully retrieved {len(processed_terms)} glossary terms via comprehensive GraphQL search"
                )
                return processed_terms
            else:
                self.logger.warning(f"Unexpected GraphQL result structure: {result}")

            # If we get here, either there were no terms or there was an error
            errors = self._get_graphql_errors(result)
            if errors:
                self.logger.warning(
                    f"GraphQL query for terms returned errors: {errors}"
                )

            self.logger.info("No glossary terms found")
            return []

        except Exception as e:
            self.logger.error(f"Error listing glossary terms: {str(e)}")
            return []

    def _get_graphql_errors(self, result):
        """
        Helper method to extract GraphQL errors from a result.

        Args:
            result: GraphQL result dictionary

        Returns:
            list: List of error messages, empty if no errors
        """
        if not result or not isinstance(result, dict):
            return []

        errors = result.get("errors", [])
        if not errors:
            return []

        error_messages = []
        for error in errors:
            if isinstance(error, dict):
                error_messages.append(error.get("message", "Unknown error"))
            else:
                error_messages.append(str(error))

        return error_messages

    def list_structured_properties(self, query="*", start=0, count=100):
        """
        List structured properties in DataHub.
        
        Args:
            query (str): Search query to filter properties
            start (int): Starting offset for pagination
            count (int): Maximum number of properties to return
            
        Returns:
            list: List of structured property objects
        """
        self.logger.info(
            f"Listing structured properties with query: {query}, start: {start}, count: {count}"
        )
        
        # First, try to check if StructuredProperty type is supported
        try:
            # Simple test query to check if StructuredProperty type exists
            test_query = """
            query testStructuredPropertySupport($input: SearchAcrossEntitiesInput!) {
              searchAcrossEntities(input: $input) {
                total
              }
            }
            """
            
            test_variables = {
                "input": {
                    "types": ["STRUCTURED_PROPERTY"],
                    "query": "*",
                    "start": 0,
                    "count": 1
                }
            }
            
            test_result = self.execute_graphql(test_query, test_variables)
            
            # Check if the test query failed due to unknown type
            if test_result and "errors" in test_result:
                for error in test_result["errors"]:
                    if "Unknown type 'StructuredProperty'" in error.get("message", ""):
                        self.logger.warning("StructuredProperty type not supported in this DataHub version")
                        return []
                    elif "UnknownType" in error.get("message", ""):
                        self.logger.warning("StructuredProperty type not supported in this DataHub version")
                        return []
            
            # If test passed, proceed with full query
            graphql_query = """
            query getSearchResultsForMultiple($input: SearchAcrossEntitiesInput!) {
              searchAcrossEntities(input: $input) {
                start
                count
                total
                searchResults {
                  entity {
                    urn
                    type
                    ... on StructuredPropertyEntity {
                      definition {
                        displayName
                        qualifiedName
                        description
                        cardinality
                        immutable
                        valueType {
                          urn
                          type
                          info {
                            type
                            displayName
                          }
                        }
                        entityTypes {
                          urn
                          type
                          info {
                            type
                          }
                        }
                        filterStatus
                        typeQualifier {
                          allowedTypes {
                            urn
                            type
                            info {
                              type
                              displayName
                            }
                          }
                        }
                        allowedValues {
                          value {
                            ... on StringValue {
                              stringValue
                            }
                            ... on NumberValue {
                              numberValue
                            }
                          }
                          description
                        }
                      }
                      settings {
                        isHidden
                        showInSearchFilters
                        showAsAssetBadge
                        showInAssetSummary
                        showInColumnsTable
                      }
                    }
                  }
                }
              }
            }
            """
            
            variables = {
                "input": {
                    "types": ["STRUCTURED_PROPERTY"],
                    "query": query,
                    "start": start,
                    "count": count
                }
            }
            
            result = self.execute_graphql(graphql_query, variables)
            
            if result and "errors" in result:
                # Check for specific GraphQL validation errors
                errors = self._get_graphql_errors(result)
                for error in errors:
                    self.logger.error(f"GraphQL error listing structured properties: {error}")
                return []
            
            if result and "data" in result and "searchAcrossEntities" in result["data"]:
                search_results = result["data"]["searchAcrossEntities"].get("searchResults", [])
                
                # Process the results
                processed_properties = []
                for search_result in search_results:
                    entity = search_result.get("entity", {})
                    if entity.get("type") == "STRUCTURED_PROPERTY":
                        processed_properties.append(entity)
                
                return processed_properties
            
            return []
            
        except Exception as e:
            # Check if it's a type validation error
            error_str = str(e)
            if "argument of type 'NoneType' is not iterable" in error_str:
                self.logger.warning("StructuredProperty type not supported in this DataHub version")
                return []
            elif "Unknown type" in error_str:
                self.logger.warning("StructuredProperty type not supported in this DataHub version")
                return []
            else:
                self.logger.error(f"Error listing structured properties: {error_str}")
                return []

    def get_structured_properties(self, start=0, count=1000):
        """
        Get all structured property entities.
        
        Args:
            start: Start index for pagination
            count: Number of entities to return
            
        Returns:
            Dictionary with structured property URNs and filter fields
        """
        query = """
        query GetEntitiesWithBrowsePathsForSearch($input: SearchAcrossEntitiesInput!) {
          searchAcrossEntities(input: $input) {
            start
            count
            total
            searchResults {
              entity {
                urn
              }
            }
          }
        }
        """
        
        variables = {
            "input": {
                "start": start,
                "count": count,
                "query": "*",
                "types": ["STRUCTURED_PROPERTY"]
            }
        }
        
        result = self.execute_graphql(query, variables)
        
        if not result or "errors" in result:
            self._log_graphql_errors(result)
            return {"success": False, "error": "Failed to fetch structured properties"}
        
        try:
            search_data = result.get("data", {}).get("searchAcrossEntities", {})
            structured_properties = []
            
            for item in search_data.get("searchResults", []):
                entity = item.get("entity", {})
                urn = entity.get("urn")
                
                if urn and ("urn:li:structuredProperty:" in urn or "urn:li:structuredproperty:" in urn):
                    # Extract the ID from the URN by removing the prefix (handle both formats)
                    if "urn:li:structuredProperty:" in urn:
                        property_id = urn.replace("urn:li:structuredProperty:", "")
                    else:
                        property_id = urn.replace("urn:li:structuredproperty:", "")
                    
                    structured_properties.append({
                        "urn": urn,
                        "id": property_id,
                        "filter_field": f"structuredProperties.{property_id}"
                    })
            
            return {
                "success": True,
                "total": search_data.get("total", 0),
                "structured_properties": structured_properties
            }
            
        except Exception as e:
            logging.error(f"Error processing structured properties: {str(e)}")
            return {"success": False, "error": str(e)}
    def get_assertions(self, entity_urn=None, query="*", start=0, count=100):
        """
        Get assertions from DataHub using comprehensive GraphQL query.
        
        Args:
            entity_urn (str, optional): Filter by specific entity URN
            query (str): Search query (default: "*")
            start (int): Start index for pagination (default: 0)
            count (int): Number of results to return (default: 100)
        
        Returns:
            dict: Response with success status and assertion data
        """
        try:
            logger.info(f"Getting assertions with query='{query}', start={start}, count={count}")
            
            # Comprehensive GraphQL query for assertions
            graphql_query = """
            query GetAssertions($input: SearchAcrossEntitiesInput!) {
              searchAcrossEntities(input: $input) {
                start
                count
                total
                searchResults {
                  entity {
                    urn
                    type
                    ... on Assertion {
                      urn
                      type
                      platform {
                        name
                        urn
                      }
                      info {
                        type
                        description
                        externalUrl
                        datasetAssertion {
                          datasetUrn
                          scope
                          fields {
                            urn
                            path
                          }
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
                          nativeType
                          nativeParameters {
                            key
                            value
                          }
                          logic
                        }
                        freshnessAssertion {
                          entityUrn
                          type
                          schedule {
                            type
                            cron {
                              cron
                              timezone
                              windowStartOffsetMs
                            }
                            fixedInterval {
                              unit
                              multiple
                            }
                            exclusions {
                              type
                              displayName
                              fixedRange {
                                startTimeMillis
                                endTimeMillis
                              }
                              weekly {
                                daysOfWeek
                                startTime
                                endTime
                                timezone
                              }
                              holiday {
                                name
                                region
                                timezone
                              }
                            }
                          }
                          filter {
                            type
                            sql
                          }
                        }
                        volumeAssertion {
                          entityUrn
                          type
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
                          incrementingSegmentRowCountTotal {
                            segment {
                              field {
                                path
                                type
                                nativeType
                              }
                              transformer {
                                type
                                nativeType
                              }
                            }
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
                          filter {
                            type
                            sql
                          }
                        }
                        sqlAssertion {
                          type
                          entityUrn
                          statement
                          changeType
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
                        fieldAssertion {
                          type
                          entityUrn
                          fieldValuesAssertion {
                            field {
                              path
                              type
                              nativeType
                            }
                            transform {
                              type
                            }
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
                            failThreshold {
                              type
                              value
                            }
                            excludeNulls
                          }
                          fieldMetricAssertion {
                            field {
                              path
                              type
                              nativeType
                            }
                            metric
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
                          filter {
                            type
                            sql
                          }
                        }
                        schemaAssertion {
                          entityUrn
                          fields {
                            path
                            type
                            nativeType
                          }
                          schema {
                            aspectVersion
                            datasetUrn
                            name
                            platformUrn
                            version
                            cluster
                            hash
                            platformSchema {
                              ...on TableSchema {
                                schema
                              }
                              ...on KeyValueSchema {
                                keySchema
                                valueSchema
                              }
                            }
                            fields {
                              fieldPath
                              jsonPath
                              label
                              nullable
                              description
                              type
                              nativeDataType
                              recursive
                              tags {
                                tags {
                                  tag {
                                    urn
                                    type
                                    properties {
                                      name
                                      description
                                      colorHex
                                    }
                                    ownership {
                                      owners {
                                        owner {
                                          ...on CorpUser {
                                            urn
                                          }
                                          ...on CorpGroup {
                                            urn
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                              glossaryTerms {
                                terms {
                                  term {
                                    urn
                                  }
                                  actor {
                                    urn
                                  }
                                }
                              }
                              isPartOfKey
                              isPartitioningKey
                              jsonProps
                              schemaFieldEntity {
                                urn
                              }
                            }
                            createdAt
                          }
                          compatibility
                        }
                        customAssertion {
                          type
                          entityUrn
                          field {
                            urn
                            path
                          }
                          logic
                        }
                        source {
                          type
                          created {
                            actor
                          }
                        }
                        lastUpdated {
                          actor
                        }
                      }
                      runEvents(limit: 10) {
                        ...on AssertionRunEventsResult {
                          total
                          failed
                          succeeded
                          errored
                          runEvents {
                            timestampMillis
                            runId
                            status
                            asserteeUrn
                            batchSpec {
                              nativeBatchId
                              query
                              limit
                            }
                            result {
                              type
                              rowCount
                              missingCount
                              unexpectedCount
                              actualAggValue
                              externalUrl
                              nativeResults {
                                key
                                value
                              }
                            }
                            runtimeContext {
                              key
                              value
                            }
                          }
                        }
                      }
                      status {
                        removed
                      }
                      tags {
                        tags {
                          tag {
                            urn
                            type
                            properties {
                              name
                              description
                              colorHex
                            }
                            ownership {
                              owners {
                                owner {
                                  ...on CorpUser {
                                    urn
                                  }
                                  ...on CorpGroup {
                                    urn
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                      actions {
                        onSuccess {
                          type
                        }
                        onFailure {
                          type
                        }
                      }
                      monitor {
                        urn
                        type
                        entity {
                          urn
                        }
                        info {
                          type
                          assertionMonitor {
                            assertions {
                              assertion {
                                urn
                              }
                              schedule {
                                cron
                              }
                              parameters {
                                type
                                datasetFieldParameters {
                                  sourceType
                                  changedRowsField {
                                    path
                                    type
                                    nativeType
                                    kind
                                  }
                                }
                                datasetVolumeParameters {
                                  sourceType
                                }
                                datasetSchemaParameters {
                                  sourceType
                                }
                                datasetFreshnessParameters {
                                  sourceType
                                }
                              }
                              context {
                                embeddedAssertions {
                                  rawAssertion
                                }
                                stdDev
                                inferenceDetails {
                                  modelId
                                  modelVersion
                                  confidence
                                  parameters {
                                    value
                                  }
                                  adjustmentSettings {
                                    algorithm
                                    algorithmName
                                    context {
                                      value
                                    }
                                    exclusionWindows {
                                      type
                                      displayName
                                      fixedRange {
                                        startTimeMillis
                                        endTimeMillis
                                      }
                                      weekly {
                                        daysOfWeek
                                        startTime
                                        endTime
                                        timezone
                                      }
                                      holiday {
                                        name
                                        region
                                        timezone
                                      }
                                    }
                                    trainingDataLookbackWindowDays
                                    sensitivity {
                                      level
                                    }
                                  }
                                  generatedAt
                                }
                              }
                              rawParameters
                            }
                            bootstrapStatus {
                              metricsCubeBootstrapStatus {
                                state
                                message
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """
            
            # Build search input
            search_input = {
                "types": ["ASSERTION"],
                "query": query,
                "start": start,
                "count": count
            }
            
            # Add entity filter if specified
            if entity_urn:
                search_input["filters"] = [
                    {
                        "field": "entity",
                        "values": [entity_urn]
                    }
                ]
            
            variables = {
                "input": search_input
            }
            
            result = self.execute_graphql(graphql_query, variables)
            
            if result is None:
                logger.error("GraphQL execution returned None")
                return {"success": False, "error": "GraphQL execution failed"}
            
            # Check for GraphQL errors
            if "errors" in result:
                error_messages = []
                for error in result["errors"]:
                    error_msg = error.get("message", "Unknown GraphQL error")
                    error_messages.append(error_msg)
                    logger.error(f"GraphQL error: {error_msg}")
                
                # Check if this is an enum error that we can handle
                enum_error_keywords = ["AssertionType.$UNKNOWN", "No enum constant"]
                if any(keyword in " ".join(error_messages) for keyword in enum_error_keywords):
                    logger.warning("Detected assertion enum compatibility issue - trying simple fallback")
                    return self._get_assertions_simple(query, start, count)
                
                return {"success": False, "error": f"GraphQL errors: {'; '.join(error_messages)}"}
            
            # Extract data from response
            if "data" not in result or not result["data"]:
                logger.error("No data in GraphQL response")
                return {"success": False, "error": "No data in response"}
            
            search_data = result["data"].get("searchAcrossEntities", {})
            
            # Clean enum data if needed
            if "searchResults" in search_data:
                search_data = self._clean_assertion_enum_data(search_data)
            
            logger.info(f"Successfully retrieved {len(search_data.get('searchResults', []))} assertions")
            
            return {
                "success": True,
                "data": search_data
            }
            
        except Exception as e:
            logger.error(f"Error getting assertions: {str(e)}")
            # Try fallback on any error
            logger.info("Trying fallback assertion query due to error")
            return self._get_assertions_simple(query, start, count)

    def _clean_assertion_enum_data(self, search_results):
        """
        Clean assertion data to handle enum compatibility issues at the application layer.
        
        Args:
            search_results: Raw search results from GraphQL
            
        Returns:
            Cleaned search results with enum issues resolved
        """
        if not isinstance(search_results, dict) or "searchResults" not in search_results:
            return search_results
            
        cleaned_results = search_results.copy()
        
        for search_result in cleaned_results.get("searchResults", []):
            entity = search_result.get("entity", {})
            
            if entity.get("type") == "ASSERTION":
                # Handle potential enum issues in assertion info
                info = entity.get("info", {})
                if isinstance(info, dict):
                    # If info.type has enum issues, replace with safe string
                    if "type" in info and info["type"] is None:
                        info["type"] = "UNKNOWN"
                    
                    # Handle enum issues in various assertion types
                    for assertion_type in ["datasetAssertion", "freshnessAssertion", "volumeAssertion", 
                                         "sqlAssertion", "fieldAssertion", "schemaAssertion", "customAssertion"]:
                        if assertion_type in info and isinstance(info[assertion_type], dict):
                            assertion_data = info[assertion_type]
                            # Replace any None enum values with safe strings
                            if "type" in assertion_data and assertion_data["type"] is None:
                                assertion_data["type"] = "UNKNOWN"
                
                # Ensure required fields exist for UI compatibility
                if "info" not in entity:
                    entity["info"] = {
                        "description": "Assertion details partially available",
                        "type": "UNKNOWN"
                    }
                
                if "platform" not in entity:
                    entity["platform"] = {
                        "name": "Unknown Platform"
                    }
        
        return cleaned_results

    def _get_assertions_simple(self, query="*", start=0, count=100):
        """
        Fallback method for getting assertions with a simpler query when advanced fields are not supported.
        
        Returns:
            dict: Response containing assertions data
        """
        try:
            # Very basic assertion query
            graphql_query = """
                query GetAssertionsSimple($input: SearchAcrossEntitiesInput!) {
                  searchAcrossEntities(input: $input) {
                    start
                    count
                    total
                    searchResults {
                      entity {
                        urn
                        type
                        ... on Assertion {
                          urn
                          type
                          platform {
                            name
                          }
                          info {
                            type
                            description
                          }
                        }
                      }
                    }
                  }
                }
            """
            
            variables = {
                "input": {
                    "query": query,
                    "start": start,
                    "count": count,
                    "types": ["ASSERTION"]
                }
            }
            
            result = self.execute_graphql(graphql_query, variables)
            
            if result and "errors" in result:
                errors = self._get_graphql_errors(result)
                # Check for enum compatibility issues
                for error in errors:
                    if "AssertionType.$UNKNOWN" in error or "No enum constant" in error:
                        self.logger.warning("Enum compatibility issue in simple assertion query, falling back to ultra-simple")
                        return self._get_assertions_ultra_simple(query, start, count)
                
                self.logger.warning(f"Simple assertion query failed: {errors}")
                return {"success": True, "data": {"searchResults": [], "total": 0, "start": start, "count": 0}}
                
            search_results = result.get("data", {}).get("searchAcrossEntities", {})
            return {"success": True, "data": search_results}
            
        except Exception as e:
            error_str = str(e)
            if "AssertionType.$UNKNOWN" in error_str or "No enum constant" in error_str:
                self.logger.warning("Enum compatibility issue in simple assertion query, falling back to ultra-simple")
                return self._get_assertions_ultra_simple(query, start, count)
            else:
                self.logger.error(f"Error in simple assertions query: {error_str}")
                return {"success": True, "data": {"searchResults": [], "total": 0, "start": start, "count": 0}}

    def _get_assertions_ultra_simple(self, query="*", start=0, count=100):
        """
        Ultra-simple fallback method for getting assertions that avoids all enum fields and complex structures.
        This handles cases where there are enum compatibility issues (e.g., AssertionType.$UNKNOWN).
        
        Returns:
            dict: Response containing basic assertions data
        """
        try:
            # Ultra-basic assertion query with minimal fields to avoid enum issues
            graphql_query = """
                query GetAssertionsUltraSimple($input: SearchAcrossEntitiesInput!) {
                  searchAcrossEntities(input: $input) {
                    start
                    count
                    total
                    searchResults {
                      entity {
                        urn
                        type
                      }
                    }
                  }
                }
            """
            
            variables = {
                "input": {
                    "query": query,
                    "start": start,
                    "count": count,
                    "types": ["ASSERTION"]
                }
            }
            
            result = self.execute_graphql(graphql_query, variables)
            
            if result and "errors" in result:
                errors = self._get_graphql_errors(result)
                # Check if it's still an enum error  
                enum_errors = [e for e in errors if "AssertionType.$UNKNOWN" in e or "No enum constant" in e]
                if enum_errors:
                    self.logger.warning(f"Critical enum error even in ultra-simple query - assertions not compatible with this DataHub version")
                    return {"success": True, "data": {"searchResults": [], "total": 0, "start": start, "count": 0}}
                else:
                    self.logger.warning(f"Ultra-simple assertion query failed with non-enum errors: {errors}")
                    return {"success": True, "data": {"searchResults": [], "total": 0, "start": start, "count": 0}}
                
            search_results = result.get("data", {}).get("searchAcrossEntities", {})
            
            # Process results and add minimal info to avoid enum issues
            if "searchResults" in search_results:
                for search_result in search_results["searchResults"]:
                    entity = search_result.get("entity", {})
                    if entity.get("type") == "ASSERTION":
                        # Add basic info structure to maintain compatibility with views
                        entity["info"] = {
                            "description": "Assertion details not available due to schema compatibility",
                            "type": "SQL"  # Use SQL as fallback instead of UNKNOWN to avoid enum issues
                        }
                        # Add basic platform info
                        entity["platform"] = {
                            "name": "Unknown Platform"
                        }
                        # Clean up any enum artifacts that might cause issues
                        if "type" in entity and entity["type"] == "$UNKNOWN":
                            entity["type"] = "ASSERTION"
            
            return {"success": True, "data": search_results}
            
        except Exception as e:
            error_str = str(e)
            if "AssertionType.$UNKNOWN" in error_str or "No enum constant" in error_str:
                self.logger.error("Critical enum compatibility issue - returning empty results")
                return {"success": True, "data": {"searchResults": [], "total": 0, "start": start, "count": 0}}
            else:
                self.logger.error(f"Error in ultra-simple assertions query: {error_str}")
                return {"success": True, "data": {"searchResults": [], "total": 0, "start": start, "count": 0}}

    def list_domains(self, query="*", start=0, count=100):
        """
        List domains in DataHub.
        
        Args:
            query (str): Search query to filter domains
            start (int): Starting offset for pagination
            count (int): Maximum number of domains to return
            
        Returns:
            list: List of domain objects
        """
        self.logger.info(
            f"Listing domains with query: {query}, start: {start}, count: {count}"
        )
        
        graphql_query = """
        query getSearchResultsForMultiple($input: SearchAcrossEntitiesInput!) {
          searchAcrossEntities(input: $input) {
            start
            count
            total
            searchResults {
              entity {
                ... on Domain {
                  urn
                  id
                  properties {
                    name
                    description
                  }
                  parentDomains {
                    domains {
                      urn
                    }
                  }
                  ownership {
                    owners {
                      owner {
                        ... on CorpUser {
                          urn
                          username
                          properties {
                            displayName
                            fullName
                          }
                        }
                        ... on CorpGroup {
                          urn
                          name
                          properties {
                            displayName
                          }
                        }
                      }
                      ownershipType {
                        urn
                        info {
                          name
                          description
                        }
                      }
                      source {
                        type
                        url
                      }
                    }
                    lastModified {
                      time
                      actor
                    }
                  }
                  institutionalMemory {
                    elements {
                      url
                      label
                      actor {
                        ... on CorpUser {
                          urn
                          username
                          properties {
                            displayName
                            fullName
                          }
                        }
                        ... on CorpGroup {
                          urn
                          name
                          properties {
                            displayName
                          }
                        }
                      }
                      created {
                        time
                        actor
                      }
                      updated {
                        time
                        actor
                      }
                      settings {
                        showInAssetPreview
                      }
                    }
                  }
                  structuredProperties {
                    properties {
                      structuredProperty {
                        urn
                      }
                      values {
                        ... on StringValue {
                          stringValue
                        }
                        ... on NumberValue {
                          numberValue
                        }
                      }
                      valueEntities {
                        urn
                      }
                    }
                  }
                  displayProperties {
                    colorHex
                    icon {
                      iconLibrary
                      name
                      style
                    }
                  }
                  entities(input: { start: 0, count: 1, query: "*" }) {
                    total
                  }
                }
              }
            }
          }
        }
        """
        
        variables = {
            "input": {
                "types": ["DOMAIN"],
                "query": query,
                "start": start,
                "count": count,
                "filters": [],
            }
        }
        
        try:
            result = self.execute_graphql(graphql_query, variables)
            
            if result and "data" in result and "searchAcrossEntities" in result["data"]:
                search_results = result["data"]["searchAcrossEntities"]["searchResults"]
                domains = []
                
                for item in search_results:
                    if "entity" in item and item["entity"] is not None:
                        entity = item["entity"]
                        properties = entity.get("properties", {})
                        
                        # Extract parent domain URN from multiple sources
                        parent_urn = None
                        
                        # First try parentDomains structure
                        parent_domains = entity.get("parentDomains")
                        if parent_domains and parent_domains.get("domains"):
                            domains_list = parent_domains["domains"]
                            if domains_list and len(domains_list) > 0:
                                parent_urn = domains_list[0].get("urn")
                        
                        # Extract entities count from GraphQL response
                        entities_info = entity.get("entities", {})
                        entities_count = entities_info.get("total", 0) if entities_info else 0

                        domain = {
                            "urn": entity.get("urn"),
                            "id": entity.get("id"),
                            "name": properties.get("name"),
                            "description": properties.get("description"),
                            "properties": properties,
                            "parentDomain": parent_urn,  # For backward compatibility
                            "parentDomains": entity.get("parentDomains"),
                            "ownership": entity.get("ownership"),
                            "institutionalMemory": entity.get("institutionalMemory"),
                            "displayProperties": entity.get("displayProperties"),
                            "entities": entity.get("entities"),  # Include full entities data for processing
                            "entities_count": entities_count,  # Add entities count from GraphQL
                            "structuredProperties": entity.get("structuredProperties"),  # Add structured properties
                        }
                        
                        domains.append(domain)
                
                return domains
            
            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.warning(
                    f"GraphQL errors when listing domains: {', '.join(error_messages)}"
                )
            
            return []
        except Exception as e:
            self.logger.error(f"Error listing domains: {str(e)}")
            return []

    def get_domain(self, domain_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific domain by URN.
        
        Args:
            domain_urn (str): Domain URN to fetch
            
        Returns:
            dict: Domain data or None if not found
        """
        self.logger.info(f"Getting domain: {domain_urn}")
        
        graphql_query = """
        query getDomain($urn: String!) {
          domain(urn: $urn) {
            urn
            id
            properties {
              name
              description
            }
            parentDomains {
              domains {
                urn
              }
            }
            ownership {
              owners {
                owner {
                  ... on CorpUser {
                    urn
                    username
                    properties {
                      displayName
                      fullName
                    }
                  }
                  ... on CorpGroup {
                    urn
                    name
                    properties {
                      displayName
                    }
                  }
                }
                ownershipType {
                  urn
                  info {
                    name
                    description
                  }
                }
                source {
                  type
                  url
                }
              }
              lastModified {
                time
                actor
              }
            }
            institutionalMemory {
              elements {
                url
                label
                actor {
                  ... on CorpUser {
                    urn
                    username
                    properties {
                      displayName
                      fullName
                    }
                  }
                  ... on CorpGroup {
                    urn
                    name
                    properties {
                      displayName
                    }
                  }
                }
                created {
                  time
                  actor
                }
                updated {
                  time
                  actor
                }
                settings {
                  showInAssetPreview
                }
              }
            }
            structuredProperties {
              properties {
                structuredProperty {
                  urn
                }
                values {
                  ... on StringValue {
                    stringValue
                  }
                  ... on NumberValue {
                    numberValue
                  }
                }
                valueEntities {
                  urn
                }
              }
            }
            entities(input: { start: 0, count: 1, query: "*" }) {
              total
            }
            displayProperties {
              colorHex
              icon {
                iconLibrary
                name
                style
              }
            }
          }
        }
        """
        
        variables = {"urn": domain_urn}
        
        try:
            result = self.execute_graphql(graphql_query, variables)
            
            if result and "data" in result and "domain" in result["data"]:
                domain_data = result["data"]["domain"]
                if not domain_data:
                    return None
                
                properties = domain_data.get("properties", {})
                
                # Extract parent domain URN from multiple sources
                parent_urn = None
                
                # First try parentDomains structure
                parent_domains = domain_data.get("parentDomains")
                if parent_domains and parent_domains.get("domains"):
                    domains_list = parent_domains["domains"]
                    if domains_list and len(domains_list) > 0:
                        parent_urn = domains_list[0].get("urn")
                
                # Extract entities count from GraphQL response
                entities_info = domain_data.get("entities", {})
                entities_count = entities_info.get("total", 0) if entities_info else 0

                domain = {
                    "urn": domain_data.get("urn"),
                    "id": domain_data.get("id"),
                    "name": properties.get("name"),
                    "description": properties.get("description"),
                    "properties": properties,
                    "parentDomain": parent_urn,  # For backward compatibility
                    "parentDomains": domain_data.get("parentDomains"),
                    "ownership": domain_data.get("ownership"),
                    "institutionalMemory": domain_data.get("institutionalMemory"),
                    "displayProperties": domain_data.get("displayProperties"),
                    "entities": domain_data.get("entities"),  # Include full entities data for processing
                    "entities_count": entities_count,  # Add entities count from GraphQL
                    "structuredProperties": domain_data.get("structuredProperties"),  # Add structured properties
                }
                
                return domain
            
            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.warning(
                    f"GraphQL errors when getting domain {domain_urn}: {', '.join(error_messages)}"
                )
            
            return None
        except Exception as e:
            self.logger.error(f"Error getting domain {domain_urn}: {str(e)}")
            return None

    def list_tests(self, query="*", start=0, count=100):
        """
        List tests in DataHub using the dedicated listTests query.
        
        Args:
            query (str): Search query to filter tests
            start (int): Starting offset for pagination
            count (int): Maximum number of tests to return
            
        Returns:
            list: List of test objects
        """
        self.logger.info(
            f"Listing tests with query: {query}, start: {start}, count: {count}"
        )
        
        try:
            # Use the dedicated listTests query
            graphql_query = """
            query listTests($input: ListTestsInput!) {
              listTests(input: $input) {
                start
                count
                total
                tests {
                  ... on Test {
                    urn
                    name
                    category
                    description
                    definition {
                      json
                    }
                    results {
                      passingCount
                      failingCount
                      lastRunTimestampMillis
                    }
                  }
                }
              }
            }
            """
            
            variables = {
                "input": {
                    "query": query,
                    "start": start,
                    "count": count
                }
            }
            
            result = self.execute_graphql(graphql_query, variables)
            
            if result and "errors" in result:
                # Check for specific GraphQL validation errors
                errors = self._get_graphql_errors(result)
                self.logger.warning(f"GraphQL listTests returned errors: {len(errors)} error(s)")
                if errors:
                    self.logger.debug(f"First error: {errors[0]}")
                
                for error in errors:
                    if "FieldUndefined" in error or "Unknown type" in error or "listTests" in error:
                        self.logger.warning("listTests query not supported in this DataHub version")
                        return []
                    elif "non null type" in error or "wrongly returned a null value" in error:
                        self.logger.warning("listTests query returned null values, skipping failed query")
                        return []
                    else:
                        self.logger.error(f"GraphQL error listing tests: {error}")
                return []
            
            if result and "data" in result and "listTests" in result["data"]:
                list_tests_data = result["data"]["listTests"]
                tests = list_tests_data.get("tests", [])
                
                # Process the results
                processed_tests = []
                for test in tests:
                    if test:
                        # Extract test information including definition and results
                        definition_json = ""
                        if test.get("definition") and test["definition"].get("json"):
                            definition_json = test["definition"]["json"]
                        
                        results = test.get("results", {})
                        
                        test_data = {
                            "urn": test.get("urn"),
                            "type": "TEST",  # All items from listTests are tests
                            "name": test.get("name", ""),
                            "description": test.get("description", ""),
                            "category": test.get("category", ""),
                            "definition_json": definition_json,
                            "results": {
                                "passingCount": results.get("passingCount", 0),
                                "failingCount": results.get("failingCount", 0), 
                                "lastRunTimestampMillis": results.get("lastRunTimestampMillis"),
                            }
                        }
                        processed_tests.append(test_data)
                
                self.logger.info(f"Successfully retrieved {len(processed_tests)} tests")
                return processed_tests
            
            return []
            
        except Exception as e:
            error_str = str(e)
            if "listTests" in error_str or "ListTestsInput" in error_str:
                self.logger.warning("listTests query not supported in this DataHub version")
                return []
            else:
                self.logger.error(f"Error listing tests: {error_str}")
                return []

    def get_editable_entities(self, start=0, count=20, query="*", entity_type=None, platform=None, use_platform_pagination=False, sort_by=None, editable_only=True, orFilters=None):
        """
        Get entities with editable properties or schema metadata.
        
        Args:
            start: Start index for pagination
            count: Number of entities to return
            query: Search query
            entity_type: Type of entity to filter by
            platform: Platform to filter by
            use_platform_pagination: Whether to use platform-based pagination for complete results
            sort_by: Field to sort by (name, type, updated)
            editable_only: If True, only return entities with editableProperties or editableSchemaMetadata
            orFilters: List of OR filters to apply (each containing AND conditions)
            
        Returns:
            Dictionary with search results
        """
        variables = {
            "input": {
                "query": query,
                "start": start,
                "count": count,
                "orFilters": orFilters if orFilters else []
            }
        }
        
        if entity_type:
            variables["input"]["types"] = [entity_type]
            
        if platform:
            # Format the platform value with proper URN format if it doesn't already have it
            platform_value = platform
            if not platform.startswith("urn:li:dataPlatform:"):
                platform_value = f"urn:li:dataPlatform:{platform}"
                
            variables["input"]["filters"] = [{
                "field": "platform",
                "value": platform_value
            }]
            
        # Use platform pagination if specified (for comprehensive search)
        if use_platform_pagination and platform and not orFilters:
            # Format the platform value with proper URN format if it doesn't already have it
            platform_value = platform
            if not platform.startswith("urn:li:dataPlatform:"):
                platform_value = f"urn:li:dataPlatform:{platform}"
                
            variables["input"]["orFilters"] = [{
                "and": [{
                    "field": "platform",
                    "condition": "EQUAL",
                    "values": [platform_value],
                    "negated": False
                }]
            }]

        # Comprehensive query that gets all necessary metadata including ownership, editableProperties, structured properties, tags, domains, and glossary terms
        graphql_query = """
query GetEntitiesWithBrowsePathsForSearch($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    start
    count
    total
    searchResults {
      entity {
        urn
        type
        ... on Dataset {
          name
          ownership {
            owners {
              owner {
                ... on CorpUser {
                  urn
                }
                ... on CorpGroup {
                  urn
                }
              }
              ownershipType {
                urn
              }
            }
          }
          platform {
            name
            properties {
              displayName
            }
          }
          dataPlatformInstance {
            instanceId
            platform {
              name
            }
          }
          domain {
            domain {
              urn
            }
          }
          browsePaths {
            path
          }
          browsePathV2 {
            path {
              name
              entity {
                ... on Container {
                  container {
                    urn
                    properties {
                      name
                    }
                  }
                }
              }
            }
          }
          editableProperties {
            name
            description
          }
          editableSchemaMetadata {
            editableSchemaFieldInfo {
              fieldPath
              description
              tags {
                tags {
                  tag {
                    urn
                  }
                }
              }
              glossaryTerms {
                terms {
                  term {
                    urn
                    glossaryTermInfo {
                      name
                    }
                  }
                }
              }
            }
          }
          deprecation {
            deprecated
          }
          schemaMetadata {
            fields {
              fieldPath
              schemaFieldEntity {
                deprecation {
                  deprecated
                }
                structuredProperties {
                  properties {
                    structuredProperty {
                      urn
                    }
                    values {
                      ... on StringValue {
                        stringValue
                      }
                      ... on NumberValue {
                        numberValue
                      }
                    }
                    valueEntities {
                      urn
                    }
                  }
                }
              }
              tags {
                tags {
                  tag {
                    urn
                    properties {
                      name
                    }
                  }
                }
              }
              glossaryTerms {
                terms {
                  term {
                    urn
                    glossaryTermInfo {
                      name
                    }
                  }
                }
              }
            }
          }
          tags {
            tags {
              tag {
                urn
              }
            }
          }
          glossaryTerms {
            terms {
              term {
                urn
                glossaryTermInfo {
                  name
                }
              }
            }
          }
          structuredProperties {
            properties {
              structuredProperty {
                urn
              }
              values {
                ... on StringValue {
                  stringValue
                }
                ... on NumberValue {
                  numberValue
                }
              }
              valueEntities {
                urn
              }
            }
          }
        }
        ... on Container {
          properties {
            name
          }
          ownership {
            owners {
              owner {
                ... on CorpUser {
                  urn
                }
                ... on CorpGroup {
                  urn
                }
              }
              ownershipType {
                urn
              }
            }
          }
          platform {
            name
            properties {
              displayName
            }
          }
          dataPlatformInstance {
            instanceId
            platform {
              name
            }
          }
          deprecation {
            deprecated
          }
          domain {
            domain {
              urn
            }
          }
          browsePathV2 {
            path {
              name
              entity {
                ... on Container {
                  container {
                    urn
                    properties {
                      name
                    }
                  }
                }
              }
            }
          }
          editableProperties {
            description
          }
          tags {
            tags {
              tag {
                urn
              }
            }
          }
          glossaryTerms {
            terms {
              term {
                urn
                glossaryTermInfo {
                  name
                }
              }
            }
          }
          structuredProperties {
            properties {
              structuredProperty {
                urn
              }
              values {
                ... on StringValue {
                  stringValue
                }
                ... on NumberValue {
                  numberValue
                }
              }
              valueEntities {
                urn
              }
            }
          }
        }
        ... on Chart {
          properties {
            name
          }
          ownership {
            owners {
              owner {
                ... on CorpUser {
                  urn
                }
                ... on CorpGroup {
                  urn
                }
              }
              ownershipType {
                urn
              }
            }
          }
          platform {
            name
            properties {
              displayName
            }
          }
          dataPlatformInstance {
            instanceId
            platform {
              name
            }
          }
          deprecation {
            deprecated
          }
          domain {
            domain {
              urn
            }
          }
          browsePaths {
            path
          }
          browsePathV2 {
            path {
              name
              entity {
                ... on Container {
                  container {
                    urn
                    properties {
                      name
                    }
                  }
                }
              }
            }
          }
          editableProperties {
            description
          }
          tags {
            tags {
              tag {
                urn
              }
            }
          }
          glossaryTerms {
            terms {
              term {
                urn
                glossaryTermInfo {
                  name
                }
              }
            }
          }
          structuredProperties {
            properties {
              structuredProperty {
                urn
              }
              values {
                ... on StringValue {
                  stringValue
                }
                ... on NumberValue {
                  numberValue
                }
              }
              valueEntities {
                urn
              }
            }
          }
        }
        ... on Dashboard {
          properties {
            name
          }
          ownership {
            owners {
              owner {
                ... on CorpUser {
                  urn
                }
                ... on CorpGroup {
                  urn
                }
              }
              ownershipType {
                urn
              }
            }
          }
          platform {
            name
            properties {
              displayName
            }
          }
          dataPlatformInstance {
            instanceId
            platform {
              name
            }
          }
          deprecation {
            deprecated
          }
          domain {
            domain {
              urn
            }
          }
          browsePaths {
            path
          }
          browsePathV2 {
            path {
              name
              entity {
                ... on Container {
                  container {
                    urn
                    properties {
                      name
                    }
                  }
                }
              }
            }
          }
          editableProperties {
            description
          }
          tags {
            tags {
              tag {
                urn
              }
            }
          }
          glossaryTerms {
            terms {
              term {
                urn
                glossaryTermInfo {
                  name
                }
              }
            }
          }
          structuredProperties {
            properties {
              structuredProperty {
                urn
              }
              values {
                ... on StringValue {
                  stringValue
                }
                ... on NumberValue {
                  numberValue
                }
              }
              valueEntities {
                urn
              }
            }
          }
        }
        ... on DataFlow {
          properties {
            name
          }
          ownership {
            owners {
              owner {
                ... on CorpUser {
                  urn
                }
                ... on CorpGroup {
                  urn
                }
              }
              ownershipType {
                urn
              }
            }
          }
          platform {
            name
            properties {
              displayName
            }
          }
          dataPlatformInstance {
            instanceId
            platform {
              name
            }
          }
          deprecation {
            deprecated
          }
          domain {
            domain {
              urn
            }
          }
          browsePaths {
            path
          }
          browsePathV2 {
            path {
              name
              entity {
                ... on Container {
                  container {
                    urn
                    properties {
                      name
                    }
                  }
                }
              }
            }
          }
          editableProperties {
            description
          }
          tags {
            tags {
              tag {
                urn
              }
            }
          }
          glossaryTerms {
            terms {
              term {
                urn
                glossaryTermInfo {
                  name
                }
              }
            }
          }
          structuredProperties {
            properties {
              structuredProperty {
                urn
              }
              values {
                ... on StringValue {
                  stringValue
                }
                ... on NumberValue {
                  numberValue
                }
              }
              valueEntities {
                urn
              }
            }
          }
        }
        ... on DataJob {
          properties {
            name
          }
          ownership {
            owners {
              owner {
                ... on CorpUser {
                  urn
                }
                ... on CorpGroup {
                  urn
                }
              }
              ownershipType {
                urn
              }
            }
          }
          dataFlow {
            flowId
            properties {
              name
            }
          }
          dataPlatformInstance {
            instanceId
            platform {
              name
            }
          }
          deprecation {
            deprecated
          }
          domain {
            domain {
              urn
            }
          }
          browsePaths {
            path
          }
          browsePathV2 {
            path {
              name
              entity {
                ... on Container {
                  container {
                    urn
                    properties {
                      name
                    }
                  }
                }
              }
            }
          }
          editableProperties {
            description
          }
          tags {
            tags {
              tag {
                urn
              }
            }
          }
          glossaryTerms {
            terms {
              term {
                urn
                glossaryTermInfo {
                  name
                }
              }
            }
          }
          structuredProperties {
            properties {
              structuredProperty {
                urn
              }
              values {
                ... on StringValue {
                  stringValue
                }
                ... on NumberValue {
                  numberValue
                }
              }
              valueEntities {
                urn
              }
            }
          }
        }
        ... on MLFeatureTable {
          name
          ownership {
            owners {
              owner {
                ... on CorpUser {
                  urn
                }
                ... on CorpGroup {
                  urn
                }
              }
              ownershipType {
                urn
              }
            }
          }
          editableProperties {
            description
          }
          deprecation {
            deprecated
          }
          domain {
            domain {
              urn
            }
          }
          platform {
            name
            properties {
              displayName
            }
          }
          dataPlatformInstance {
            instanceId
            platform {
              name
            }
          }
          browsePaths {
            path
          }
          browsePathV2 {
            path {
              name
              entity {
                ... on Container {
                  container {
                    urn
                    properties {
                      name
                    }
                  }
                }
              }
            }
          }
          tags {
            tags {
              tag {
                urn
              }
            }
          }
          glossaryTerms {
            terms {
              term {
                urn
                glossaryTermInfo {
                  name
                }
              }
            }
          }
          structuredProperties {
            properties {
              structuredProperty {
                urn
              }
              values {
                ... on StringValue {
                  stringValue
                }
                ... on NumberValue {
                  numberValue
                }
              }
              valueEntities {
                urn
              }
            }
          }
        }
        ... on MLFeature {
          name
          ownership {
            owners {
              owner {
                ... on CorpUser {
                  urn
                }
                ... on CorpGroup {
                  urn
                }
              }
              ownershipType {
                urn
              }
            }
          }
          editableProperties {
            description
          }
          dataPlatformInstance {
            instanceId
            platform {
              name
            }
          }
          browsePathV2 {
            path {
              name
              entity {
                ... on Container {
                  container {
                    urn
                    properties {
                      name
                    }
                  }
                }
              }
            }
          }
          tags {
            tags {
              tag {
                urn
              }
            }
          }
          glossaryTerms {
            terms {
              term {
                urn
                glossaryTermInfo {
                  name
                }
              }
            }
          }
          structuredProperties {
            properties {
              structuredProperty {
                urn
              }
              values {
                ... on StringValue {
                  stringValue
                }
                ... on NumberValue {
                  numberValue
                }
              }
              valueEntities {
                urn
              }
            }
          }
        }
        ... on MLModel {
          name
          ownership {
            owners {
              owner {
                ... on CorpUser {
                  urn
                }
                ... on CorpGroup {
                  urn
                }
              }
              ownershipType {
                urn
              }
            }
          }
          editableProperties {
            description
          }
          dataPlatformInstance {
            instanceId
            platform {
              name
            }
          }
          browsePathV2 {
            path {
              name
              entity {
                ... on Container {
                  container {
                    urn
                    properties {
                      name
                    }
                  }
                }
              }
            }
          }
          tags {
            tags {
              tag {
                urn
              }
            }
          }
          glossaryTerms {
            terms {
              term {
                urn
                glossaryTermInfo {
                  name
                }
              }
            }
          }
          structuredProperties {
            properties {
              structuredProperty {
                urn
              }
              values {
                ... on StringValue {
                  stringValue
                }
                ... on NumberValue {
                  numberValue
                }
              }
              valueEntities {
                urn
              }
            }
          }
        }
        ... on MLModelGroup {
          name
          ownership {
            owners {
              owner {
                ... on CorpUser {
                  urn
                }
                ... on CorpGroup {
                  urn
                }
              }
              ownershipType {
                urn
              }
            }
          }
          editableProperties {
            description
          }
          deprecation {
            deprecated
          }
          domain {
            domain {
              urn
            }
          }
          tags {
            tags {
              tag {
                urn
              }
            }
          }
          glossaryTerms {
            terms {
              term {
                urn
                glossaryTermInfo {
                  name
                }
              }
            }
          }
          structuredProperties {
            properties {
              structuredProperty {
                urn
              }
              values {
                ... on StringValue {
                  stringValue
                }
                ... on NumberValue {
                  numberValue
                }
              }
              valueEntities {
                urn
              }
            }
          }
        }
        ... on MLPrimaryKey {
          name
          ownership {
            owners {
              owner {
                ... on CorpUser {
                  urn
                }
                ... on CorpGroup {
                  urn
                }
              }
              ownershipType {
                urn
              }
            }
          }
          editableProperties {
            description
          }
          deprecation {
            deprecated
          }
          domain {
            domain {
              urn
            }
          }
          tags {
            tags {
              tag {
                urn
              }
            }
          }
          glossaryTerms {
            terms {
              term {
                urn
                glossaryTermInfo {
                  name
                }
              }
            }
          }
          structuredProperties {
            properties {
              structuredProperty {
                urn
              }
              values {
                ... on StringValue {
                  stringValue
                }
                ... on NumberValue {
                  numberValue
                }
              }
              valueEntities {
                urn
              }
            }
          }
        }
      }
    }
  }
}
        """
        
        result = self.execute_graphql(graphql_query, variables)
        
        if "errors" in result:
            self._log_graphql_errors(result)
            return {"success": False, "error": f"GraphQL error: {result['errors'][0]['message']}"}
            
        search_results = result.get("data", {}).get("searchAcrossEntities", {})
        
        # If editable_only is True, filter results to only include entities with any metadata
        # (editable properties, schema metadata, domains, tags, glossary terms, or structured properties)
        if editable_only and "searchResults" in search_results:
            filtered_results = []
            total_count = 0
            
            for result in search_results.get("searchResults", []):
                entity = result.get("entity", {})
                
                # Count all metadata types
                counts = self._count_entity_metadata(entity)
                total_metadata_count = sum(counts.values())
                
                # Only include entities that have at least one piece of metadata
                if total_metadata_count > 0:
                    filtered_results.append(result)
                    total_count += 1
            
            # Replace the search results with the filtered results
            filtered_search_results = {
                "start": search_results.get("start", 0),
                "count": len(filtered_results),
                "total": search_results.get("total", 0),
                "filtered_total": total_count,
                "searchResults": filtered_results
            }
            
            return {"success": True, "data": filtered_search_results}
        
        return {"success": True, "data": search_results}

    def _count_entity_metadata(self, entity):
        """
        Count metadata elements for an entity to determine if it has meaningful content.
        
        Args:
            entity (dict): The entity data from GraphQL
            
        Returns:
            dict: Dictionary with counts for each metadata type
        """
        counts = {
            'editable_properties': 0,
            'schema_metadata': 0,
            'domains': 0,
            'glossary_terms': 0,
            'tags': 0,
            'structured_properties': 0
        }
        
        # Count editable properties
        if entity.get("editableProperties"):
            editable_props = entity["editableProperties"]
            if editable_props.get("name"):
                counts['editable_properties'] += 1
            if editable_props.get("description"):
                counts['editable_properties'] += 1
            # Count other editable properties
            other_props = [k for k in editable_props.keys() if k not in ['name', 'description']]
            counts['editable_properties'] += len(other_props)
        
        # Count schema metadata fields
        if entity.get("editableSchemaMetadata") and entity["editableSchemaMetadata"].get("editableSchemaFieldInfo"):
            counts['schema_metadata'] += len(entity["editableSchemaMetadata"]["editableSchemaFieldInfo"])
        
        if entity.get("schemaMetadata") and entity["schemaMetadata"].get("fields"):
            counts['schema_metadata'] += len(entity["schemaMetadata"]["fields"])
        
        # Count domains
        if entity.get("domain") and entity["domain"].get("domain") and entity["domain"]["domain"].get("urn"):
            counts['domains'] = 1
        
        # Count entity-level glossary terms
        if entity.get("glossaryTerms") and entity["glossaryTerms"].get("terms"):
            counts['glossary_terms'] += len(entity["glossaryTerms"]["terms"])
        
        # Count schema-level glossary terms
        if entity.get("editableSchemaMetadata") and entity["editableSchemaMetadata"].get("editableSchemaFieldInfo"):
            for field in entity["editableSchemaMetadata"]["editableSchemaFieldInfo"]:
                if field.get("glossaryTerms") and field["glossaryTerms"].get("terms"):
                    counts['glossary_terms'] += len(field["glossaryTerms"]["terms"])
        
        if entity.get("schemaMetadata") and entity["schemaMetadata"].get("fields"):
            for field in entity["schemaMetadata"]["fields"]:
                if field.get("glossaryTerms") and field["glossaryTerms"].get("terms"):
                    counts['glossary_terms'] += len(field["glossaryTerms"]["terms"])
        
        # Count entity-level tags
        if entity.get("tags") and entity["tags"].get("tags"):
            counts['tags'] += len(entity["tags"]["tags"])
        
        # Count schema-level tags
        if entity.get("editableSchemaMetadata") and entity["editableSchemaMetadata"].get("editableSchemaFieldInfo"):
            for field in entity["editableSchemaMetadata"]["editableSchemaFieldInfo"]:
                if field.get("tags") and field["tags"].get("tags"):
                    counts['tags'] += len(field["tags"]["tags"])
        
        if entity.get("schemaMetadata") and entity["schemaMetadata"].get("fields"):
            for field in entity["schemaMetadata"]["fields"]:
                if field.get("tags") and field["tags"].get("tags"):
                    counts['tags'] += len(field["tags"]["tags"])
        
        # Count entity-level structured properties
        if entity.get("structuredProperties") and entity["structuredProperties"].get("properties"):
            counts['structured_properties'] += len(entity["structuredProperties"]["properties"])
        
        # Count schema-level structured properties
        if entity.get("schemaMetadata") and entity["schemaMetadata"].get("fields"):
            for field in entity["schemaMetadata"]["fields"]:
                if (field.get("schemaFieldEntity") and 
                    field["schemaFieldEntity"].get("structuredProperties") and 
                    field["schemaFieldEntity"]["structuredProperties"].get("properties")):
                    counts['structured_properties'] += len(field["schemaFieldEntity"]["structuredProperties"]["properties"])
        
        return counts

    def update_entity_properties(self, entity_urn: str, entity_type: str, properties: dict) -> bool:
        """Update editable properties of an entity.
        
        Args:
            entity_urn (str): URN of the entity to update
            entity_type (str): Type of the entity (e.g., 'DATASET', 'CONTAINER', etc.)
            properties (dict): Dictionary containing the properties to update
            
        Returns:
            bool: True if update was successful, False otherwise
        """
        self.logger.info(f"Updating properties for {entity_type} {entity_urn}")
        
        # Prepare update input
        update_input = {
            'urn': entity_urn,
            'editableProperties': properties.get('editableProperties', {}),
        }
        
        # Add schema metadata for datasets if provided
        if entity_type == 'DATASET' and 'editableSchemaMetadata' in properties:
            update_input['editableSchemaMetadata'] = properties['editableSchemaMetadata']
            
        # Define GraphQL mutation
        mutation = """
        mutation updateEntity($input: UpdateEntityInput!) {
            updateEntity(input: $input)
        }
        """
        
        variables = {'input': update_input}
        
        try:
            result = self._execute_graphql(mutation, variables)
            if result and 'updateEntity' in result:
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error updating entity properties: {str(e)}")
            return False

    def _log_graphql_errors(self, result):
        """
        Log GraphQL errors from a result.
        
        Args:
            result (dict): The GraphQL response with errors
        """
        errors = self._get_graphql_errors(result)
        if errors:
            # Only log the first error to avoid spam
            self.logger.error(f"GraphQL error: {errors[0]}")
            if len(errors) > 1:
                self.logger.debug(f"Additional {len(errors) - 1} GraphQL errors occurred")

    def get_comprehensive_glossary_data(self, query="*", start=0, count=100):
        """
        Get comprehensive glossary data including nodes and terms with all metadata.
        
        Args:
            query (str): Search query to filter results
            start (int): Starting offset for pagination
            count (int): Number of items to return
            
        Returns:
            dict: Dictionary containing nodes and terms with comprehensive metadata
        """
        self.logger.info(f"Getting comprehensive glossary data with query={query}, start={start}, count={count}")
        
        # Query for both nodes and terms with comprehensive data - updated to match new format
        comprehensive_query = """
        query GetGlossary($input: SearchAcrossEntitiesInput!) {
          searchAcrossEntities(input: $input) {
            start
            count
            total
            searchResults {
              entity {
                urn
                type
                ... on GlossaryNode {
                  ownership {
                    owners {
                      owner {
                        ... on CorpUser {
                          urn
                          username
                          properties {
                            displayName
                            email
                          }
                        }
                        ... on CorpGroup {
                          urn
                          name
                          properties {
                            displayName
                          }
                        }
                      }
                      ownershipType {
                        urn
                        info {
                          name
                        }
                      }
                    }
                    lastModified {
                      actor
                      time
                    }
                  }
                  properties {
                    name
                    description
                    customProperties {
                      key
                      value
                    }
                  }
                  parentNodes {
                    nodes {
                      ... on GlossaryNode {
                        urn
                        properties {
                          name
                        }
                      }
                    }
                  }
                  displayProperties {
                    colorHex
                    icon {
                      iconLibrary
                      name
                      style
                    }
                  }
                  structuredProperties {
                    properties {
                      structuredProperty {
                        urn
                        definition {
                          displayName
                          qualifiedName
                        }
                      }
                      values {
                        ... on StringValue {
                          stringValue
                        }
                        ... on NumberValue {
                          numberValue
                        }
                      }
                    }
                  }
                  institutionalMemory {
                    elements {
                      url
                      label
                      actor {
                        ... on CorpUser {
                          urn
                        }
                        ... on CorpGroup {
                          urn
                        }
                      }
                      created {
                        actor
                        time
                      }
                      updated {
                        actor
                        time
                      }
                      settings {
                        showInAssetPreview
                      }
                    }
                  }
                }
                ... on GlossaryTerm {
                  ownership {
                    owners {
                      owner {
                        ... on CorpUser {
                          urn
                          username
                          properties {
                            displayName
                            email
                          }
                        }
                        ... on CorpGroup {
                          urn
                          name
                          properties {
                            displayName
                          }
                        }
                      }
                      ownershipType {
                        urn
                        info {
                          name
                        }
                      }
                    }
                    lastModified {
                      actor
                      time
                    }
                  }
                  domain {
                    domain {
                      urn
                      properties {
                        name
                        description
                      }
                    }
                  }
                  application {
                    application {
                      urn
                    }
                  }
                  properties {
                    name
                    description
                    termSource
                    sourceRef
                    sourceUrl
                    customProperties {
                      key
                      value
                    }
                  }
                  deprecation {
                    deprecated
                  }
                  isRelatedTerms: relationships(input: {types: ["IsA"], direction: OUTGOING}) {
                    relationships {
                      entity {
                        ... on GlossaryTerm {
                          urn
                          properties {
                            name
                          }
                        }
                      }
                    }
                  }
                  hasRelatedTerms: relationships(input: {types: ["HasA"], direction: OUTGOING}) {
                    relationships {
                      entity {
                        ... on GlossaryTerm {
                          urn
                          properties {
                            name
                          }
                        }
                      }
                    }
                  }
                  parentNodes {
                    nodes {
                      ... on GlossaryNode {
                        urn
                        properties {
                          name
                        }
                      }
                    }
                  }
                  institutionalMemory {
                    elements {
                      url
                      label
                      actor {
                        ... on CorpUser {
                          urn
                        }
                        ... on CorpGroup {
                          urn
                        }
                      }
                      created {
                        actor
                        time
                      }
                      updated {
                        actor
                        time
                      }
                      settings {
                        showInAssetPreview
                      }
                    }
                  }
                  structuredProperties {
                    properties {
                      structuredProperty {
                        urn
                        definition {
                          displayName
                          qualifiedName
                        }
                      }
                      values {
                        ... on StringValue {
                          stringValue
                        }
                        ... on NumberValue {
                          numberValue
                        }
                      }
                      valueEntities {
                        urn
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """
        
        variables = {
            "input": {
                "query": query,
                "types": ["GLOSSARY_NODE", "GLOSSARY_TERM"],
                "count": count,
                "start": start,
            }
        }
        
        try:
            result = self.execute_graphql(comprehensive_query, variables)
            
            # Add detailed logging for debugging
            self.logger.debug(f"GraphQL result keys: {list(result.keys()) if result else 'None'}")
            if result and "data" in result:
                self.logger.debug(f"Data keys: {list(result['data'].keys()) if result['data'] else 'None'}")
            
            if result and "data" in result and "searchAcrossEntities" in result["data"]:
                search_data = result["data"]["searchAcrossEntities"]
                
                # Handle case where search_data might be None
                if search_data is None:
                    self.logger.warning("searchAcrossEntities returned None")
                    return {"nodes": [], "terms": [], "total": 0, "start": 0, "count": 0}
                
                search_results = search_data.get("searchResults", [])
                
                nodes = []
                terms = []
                
                for result_item in search_results:
                    if result_item is None:
                        continue
                        
                    entity = result_item.get("entity", {})
                    if entity is None:
                        continue
                        
                    entity_type = entity.get("type")
                    
                    if entity_type == "GLOSSARY_NODE":
                        processed_node = self._process_glossary_node(entity)
                        if processed_node:
                            nodes.append(processed_node)
                    elif entity_type == "GLOSSARY_TERM":
                        processed_term = self._process_glossary_term(entity)
                        if processed_term:
                            terms.append(processed_term)
                
                # Resolve relationship names after processing all items
                all_items = nodes + terms
                self._resolve_relationship_names(all_items)
                
                return {
                    "nodes": nodes,
                    "terms": terms,
                    "total": search_data.get("total", 0),
                    "start": search_data.get("start", 0),
                    "count": search_data.get("count", 0)
                }
            else:
                self.logger.error(f"Failed to get comprehensive glossary data: {result}")
                return {"nodes": [], "terms": [], "total": 0, "start": 0, "count": 0}
                
        except Exception as e:
            self.logger.error(f"Error getting comprehensive glossary data: {str(e)}")
            return {"nodes": [], "terms": [], "total": 0, "start": 0, "count": 0}
    
    def _process_glossary_node(self, entity):
        """Process a glossary node entity into standardized format"""
        if entity is None:
            return None
            
        # Check if entity has a valid URN - if not, skip it
        entity_urn = entity.get("urn")
        if not entity_urn or entity_urn.strip() == "":
            self.logger.warning(f"Skipping glossary node with missing or empty URN: {entity}")
            return None
            
        properties = entity.get("properties", {}) or {}
        ownership = entity.get("ownership", {}) or {}
        structured_props = entity.get("structuredProperties", {}) or {}
        parent_nodes_data = entity.get("parentNodes", {}) or {}
        parent_nodes = parent_nodes_data.get("nodes", []) if parent_nodes_data else []
        relationships_data = entity.get("relationships", {}) or {}
        relationships = relationships_data.get("relationships", []) if relationships_data else []
        incoming_relationships_data = entity.get("incomingRelationships", {}) or {}
        incoming_relationships = incoming_relationships_data.get("relationships", []) if incoming_relationships_data else []
        
        # Process ownership
        owners = []
        ownership_owners = ownership.get("owners", []) if ownership else []
        for owner_data in ownership_owners:
            if owner_data is None:
                continue
            owner = owner_data.get("owner", {}) or {}
            ownership_type = owner_data.get("ownershipType", {}) or {}
            
            owner_properties = owner.get("properties", {}) or {}
            ownership_info = ownership_type.get("info", {}) or {}
            
            # Extract name from URN if not available in properties
            owner_name = owner.get("username") or owner.get("name")
            if not owner_name and owner.get("urn"):
                # Extract from URN: urn:li:corpuser:username -> username
                urn_parts = owner.get("urn").split(":")
                if len(urn_parts) >= 4:
                    owner_name = urn_parts[-1]  # Get the last part (username)
            
            owner_info = {
                "urn": owner.get("urn"),
                "type": owner.get("type"),
                "name": owner_name or "Unknown",
                "displayName": owner_properties.get("displayName", ""),
                "email": owner_properties.get("email", ""),
                "ownershipType": {
                    "urn": ownership_type.get("urn"),
                    "name": ownership_info.get("name", "Unknown")
                }
            }
            owners.append(owner_info)
        
        # Process structured properties
        structured_properties = []
        structured_props_list = structured_props.get("properties", []) if structured_props else []
        for prop_data in structured_props_list:
            if prop_data is None:
                continue
            structured_property = prop_data.get("structuredProperty", {}) or {}
            prop_def = structured_property.get("definition", {}) or {}
            values = prop_data.get("values", []) or []
            
            prop_info = {
                "urn": structured_property.get("urn"),
                "displayName": prop_def.get("displayName", ""),
                "qualifiedName": prop_def.get("qualifiedName", ""),
                "values": [v.get("stringValue") or v.get("numberValue") for v in values if v]
            }
            structured_properties.append(prop_info)
        
        # Process relationships - combine both outgoing and incoming
        processed_relationships = []
        all_relationships = relationships + incoming_relationships
        for rel in all_relationships:
            if rel is None:
                continue
            rel_entity = rel.get("entity", {}) or {}
            rel_entity_properties = rel_entity.get("properties", {}) or {}
            created = rel.get("created", {}) or {}
            processed_relationships.append({
                "type": rel.get("type"),
                "direction": rel.get("direction"),
                "entity": {
                    "urn": rel_entity.get("urn"),
                    "type": rel_entity.get("type"),
                    "name": rel_entity_properties.get("name", "Unknown")
                },
                "created": {
                    "time": created.get("time"),
                    "actor": created.get("actor")
                } if created else None
            })
        
        return {
            "urn": entity.get("urn"),
            "type": entity.get("type"),
            "name": properties.get("name", "Unknown"),
            "description": properties.get("description", ""),
            "customProperties": properties.get("customProperties", []),
            "owners": owners,
            "structuredProperties": structured_properties,
            "parentNodes": [{"urn": p.get("urn"), "name": p.get("properties", {}).get("name", "") if p.get("properties") else ""} for p in parent_nodes if p],
            "relationships": processed_relationships,
            "properties": properties
        }
    
    def _process_glossary_term(self, entity):
        """Process a glossary term entity into standardized format"""
        if entity is None:
            return None
            
        # Check if entity has a valid URN - if not, skip it
        entity_urn = entity.get("urn")
        if not entity_urn or entity_urn.strip() == "":
            self.logger.warning(f"Skipping glossary term with missing or empty URN: {entity}")
            return None
            
        properties = entity.get("properties", {}) or {}
        ownership = entity.get("ownership", {}) or {}
        structured_props = entity.get("structuredProperties", {}) or {}
        parent_nodes_data = entity.get("parentNodes", {}) or {}
        parent_nodes = parent_nodes_data.get("nodes", []) if parent_nodes_data else []
        relationships_data = entity.get("relationships", {}) or {}
        relationships = relationships_data.get("relationships", []) if relationships_data else []
        incoming_relationships_data = entity.get("incomingRelationships", {}) or {}
        incoming_relationships = incoming_relationships_data.get("relationships", []) if incoming_relationships_data else []
        
        # Process ownership
        owners = []
        ownership_owners = ownership.get("owners", []) if ownership else []
        for owner_data in ownership_owners:
            if owner_data is None:
                continue
            owner = owner_data.get("owner", {}) or {}
            ownership_type = owner_data.get("ownershipType", {}) or {}
            
            owner_properties = owner.get("properties", {}) or {}
            ownership_info = ownership_type.get("info", {}) or {}
            
            # Extract name from URN if not available in properties
            owner_name = owner.get("username") or owner.get("name")
            if not owner_name and owner.get("urn"):
                # Extract from URN: urn:li:corpuser:username -> username
                urn_parts = owner.get("urn").split(":")
                if len(urn_parts) >= 4:
                    owner_name = urn_parts[-1]  # Get the last part (username)
            
            owner_info = {
                "urn": owner.get("urn"),
                "type": owner.get("type"),
                "name": owner_name or "Unknown",
                "displayName": owner_properties.get("displayName", ""),
                "email": owner_properties.get("email", ""),
                "ownershipType": {
                    "urn": ownership_type.get("urn"),
                    "name": ownership_info.get("name", "Unknown")
                }
            }
            owners.append(owner_info)
        
        # Process structured properties
        structured_properties = []
        structured_props_list = structured_props.get("properties", []) if structured_props else []
        for prop_data in structured_props_list:
            if prop_data is None:
                continue
            structured_property = prop_data.get("structuredProperty", {}) or {}
            prop_def = structured_property.get("definition", {}) or {}
            values = prop_data.get("values", []) or []
            
            prop_info = {
                "urn": structured_property.get("urn"),
                "displayName": prop_def.get("displayName", ""),
                "qualifiedName": prop_def.get("qualifiedName", ""),
                "values": [v.get("stringValue") or v.get("numberValue") for v in values if v]
            }
            structured_properties.append(prop_info)
        
        # Process specific relationship types for glossary terms
        processed_relationships = []
        
        # Process isRelatedTerms (IsA relationships)
        is_related_terms = entity.get("isRelatedTerms", {}) or {}
        is_related_relationships = is_related_terms.get("relationships", []) if is_related_terms else []
        for rel in is_related_relationships:
            if rel is None:
                continue
            rel_entity = rel.get("entity", {}) or {}
            rel_entity_properties = rel_entity.get("properties", {}) or {}
            processed_relationships.append({
                "type": "IsA",
                "direction": "OUTGOING",
                "entity": {
                    "urn": rel_entity.get("urn"),
                    "type": rel_entity.get("type", "GlossaryTerm"),
                    "name": rel_entity_properties.get("name", "Unknown")
                }
            })
        
        # Process hasRelatedTerms (HasA relationships)
        has_related_terms = entity.get("hasRelatedTerms", {}) or {}
        has_related_relationships = has_related_terms.get("relationships", []) if has_related_terms else []
        for rel in has_related_relationships:
            if rel is None:
                continue
            rel_entity = rel.get("entity", {}) or {}
            rel_entity_properties = rel_entity.get("properties", {}) or {}
            processed_relationships.append({
                "type": "HasA",
                "direction": "OUTGOING",
                "entity": {
                    "urn": rel_entity.get("urn"),
                    "type": rel_entity.get("type", "GlossaryTerm"),
                    "name": rel_entity_properties.get("name", "Unknown")
                }
            })
        
        # Also process any other relationships from the generic relationships field
        all_relationships = relationships + incoming_relationships
        for rel in all_relationships:
            if rel is None:
                continue
            rel_entity = rel.get("entity", {}) or {}
            rel_entity_properties = rel_entity.get("properties", {}) or {}
            created = rel.get("created", {}) or {}
            processed_relationships.append({
                "type": rel.get("type"),
                "direction": rel.get("direction"),
                "entity": {
                    "urn": rel_entity.get("urn"),
                    "type": rel_entity.get("type"),
                    "name": rel_entity_properties.get("name", "Unknown")
                },
                "created": {
                    "time": created.get("time"),
                    "actor": created.get("actor")
                } if created else None
            })
        
        # Process domain information
        domain = None
        domain_data = entity.get("domain", {}) or {}
        if domain_data:
            domain_info = domain_data.get("domain", {}) or {}
            if domain_info and domain_info.get("urn"):
                domain_properties = domain_info.get("properties", {}) or {}
                domain = {
                    "urn": domain_info.get("urn"),
                    "name": domain_properties.get("name", ""),
                    "description": domain_properties.get("description", "")
                }
        
        return {
            "urn": entity.get("urn"),
            "type": entity.get("type"),
            "name": properties.get("name", "Unknown"),
            "description": properties.get("description", ""),
            "termSource": properties.get("termSource", "INTERNAL"),
            "customProperties": properties.get("customProperties", []),
            "owners": owners,
            "structuredProperties": structured_properties,
            "parentNodes": [{"urn": p.get("urn"), "name": p.get("properties", {}).get("name", "") if p.get("properties") else ""} for p in parent_nodes if p],
            "relationships": processed_relationships,
            "domains": {"domain": domain} if domain else None,
            "domain": domain,  # Keep both formats for compatibility
            "properties": properties
        }

    def _resolve_relationship_names(self, items):
        """
        Resolve relationship names by looking up URNs in the processed items.
        This should be called after all items have been processed.
        """
        # Create a URN to name mapping
        urn_to_name = {}
        for item in items:
            if item and item.get("urn") and item.get("name"):
                urn_to_name[item["urn"]] = item["name"]
        
        # Update relationship names
        for item in items:
            if item and item.get("relationships"):
                for relationship in item["relationships"]:
                    if relationship and relationship.get("entity"):
                        entity_urn = relationship["entity"].get("urn")
                        if entity_urn and entity_urn in urn_to_name:
                            relationship["entity"]["name"] = urn_to_name[entity_urn]
        
        return items

    def list_users(self, start=0, count=100):
        """
        List users from DataHub
        
        Args:
            start (int): Starting offset for pagination
            count (int): Number of users to return
            
        Returns:
            dict: Users data with pagination info
        """
        query = """
        query listUsers($input: ListUsersInput!) {
          listUsers(input: $input) {
            start
            count
            total
            users {
              urn 
              type
              username
              properties {
                displayName
                email
              }
            }
          }
        }
        """
        
        variables = {
            "input": {
                "start": start,
                "count": count
            }
        }
        
        try:
            result = self.execute_graphql(query, variables)
            if result and "data" in result and "listUsers" in result["data"]:
                return result["data"]["listUsers"]
            else:
                self.logger.error(f"Failed to list users: {result}")
                return None
        except Exception as e:
            self.logger.error(f"Error listing users: {str(e)}")
            return None

    def list_groups(self, start=0, count=100):
        """
        List groups from DataHub
        
        Args:
            start (int): Starting offset for pagination
            count (int): Number of groups to return
            
        Returns:
            dict: Groups data with pagination info
        """
        query = """
        query listGroups($input: ListGroupsInput!) {
          listGroups(input: $input) {
            start
            count
            total
            groups {
              urn
              type
              properties {
                displayName
                description
              }
            } 
          }
        }
        """
        
        variables = {
            "input": {
                "start": start,
                "count": count
            }
        }
        
        try:
            result = self.execute_graphql(query, variables)
            if result and "data" in result and "listGroups" in result["data"]:
                return result["data"]["listGroups"]
            else:
                self.logger.error(f"Failed to list groups: {result}")
                return None
        except Exception as e:
            self.logger.error(f"Error listing groups: {str(e)}")
            return None

    def list_ownership_types(self, start=0, count=100):
        """
        List ownership types from DataHub
        
        Args:
            start (int): Starting offset for pagination
            count (int): Number of ownership types to return
            
        Returns:
            dict: Ownership types data with pagination info
        """
        query = """
        query listOwnershipTypes($input: ListOwnershipTypesInput!) {
          listOwnershipTypes(input: $input) {
            start
            count
            total
            ownershipTypes {
              ... on OwnershipTypeEntity {
                urn
                type
                info {
                  name
                }
              }
            }
          }
        }
        """
        
        variables = {
            "input": {
                "start": start,
                "count": count
            }
        }
        
        try:
            result = self.execute_graphql(query, variables)
            if result and "data" in result and "listOwnershipTypes" in result["data"]:
                return result["data"]["listOwnershipTypes"]
            else:
                self.logger.error(f"Failed to list ownership types: {result}")
                return None
        except Exception as e:
            self.logger.error(f"Error listing ownership types: {str(e)}")
            return None

    # Data Product Management Methods

    def list_data_products(self, query="*", start=0, count=100) -> List[Dict[str, Any]]:
        """
        List data products in DataHub with comprehensive information including ownership and relationships.

        Args:
            query (str): Search query to filter data products (default: "*")
            start (int): Starting offset for pagination
            count (int): Maximum number of data products to return

        Returns:
            List of data product objects with detailed information
        """
        self.logger.info(
            f"Listing data products with query: {query}, start: {start}, count: {count}"
        )
        self.logger.debug(
            f"Server URL: {self.server_url}, Token provided: {self.token is not None and len(self.token) > 0}, Verify SSL: {self.verify_ssl}"
        )

        # GraphQL query for data products with comprehensive information
        graphql_query = """
        query GetDataProducts($input: SearchAcrossEntitiesInput!) {
          searchAcrossEntities(input: $input) {
            start
            count
            total
            searchResults {
              entity {
                urn
                type
                ... on DataProduct {
                  properties {
                    name
                    description
                    externalUrl
                    numAssets
                    customProperties {
                      key
                      value
                    }
                  }
                  ownership {
                    owners {
                      owner {
                        ... on CorpUser {
                          urn
                          username
                          properties {
                            displayName
                          }
                        }
                        ... on CorpGroup {
                          urn
                          name
                          properties {
                            displayName
                          }
                        }
                      }
                      ownershipType {
                        urn
                        info {
                          name
                        }
                      }
                      source {
                        type
                        url
                      }
                    }
                  }
                  institutionalMemory {
                    elements {
                      url
                      label
                      created {
                        actor
                      }
                      updated {
                        actor
                      }
                    }
                  }
                  glossaryTerms {
                    terms {
                      term {
                        urn
                      }
                    }
                  }
                  domain {
                    domain {
                      urn
                    }
                  }
                  tags {
                    tags {
                      tag {
                        urn
                      }
                    }
                  }
                  structuredProperties {
                    properties {
                      structuredProperty {
                        urn
                      }
                      values {
                        ... on StringValue {
                          stringValue
                        }
                        ... on NumberValue {
                          numberValue
                        }
                      }
                      valueEntities {
                        urn
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """

        variables = {
            "input": {
                "types": ["DATA_PRODUCT"],
                "query": query,
                "start": start,
                "count": count,
                "filters": [],
            }
        }

        try:
            result = self.execute_graphql(graphql_query, variables)

            if result and "data" in result and "searchAcrossEntities" in result["data"]:
                search_results = result["data"]["searchAcrossEntities"]["searchResults"]
                data_products = []

                for item in search_results:
                    if "entity" in item and item["entity"] is not None:
                        entity = item["entity"]
                        
                        # Skip if entity is None (defensive programming)
                        if entity is None:
                            continue
                        
                        # Extract basic data product information with proper None checking
                        properties = entity.get("properties") or {}
                        data_product = {
                            "urn": entity.get("urn"),
                            "type": entity.get("type"),
                            "name": properties.get("name"),
                            "description": properties.get("description"),
                            "externalUrl": properties.get("externalUrl"),
                            "numAssets": properties.get("numAssets", 0),
                            "customProperties": properties.get("customProperties", []),
                        }
                        
                        # Add properties for backward compatibility
                        if properties:
                            data_product["properties"] = properties

                        # Add ownership information with proper None checking
                        ownership = entity.get("ownership")
                        if ownership:
                            data_product["ownership"] = ownership
                            
                            # Extract owner count and names for display
                            owners = ownership.get("owners") or []
                            data_product["owners_count"] = len(owners)
                            data_product["owner_names"] = []
                            
                            for owner_info in owners:
                                if not owner_info:
                                    continue
                                owner = owner_info.get("owner") or {}
                                if owner.get("username"):  # CorpUser
                                    owner_props = owner.get("properties") or {}
                                    display_name = owner_props.get("displayName")
                                    data_product["owner_names"].append(display_name or owner["username"])
                                elif owner.get("name"):  # CorpGroup
                                    owner_props = owner.get("properties") or {}
                                    display_name = owner_props.get("displayName")
                                    data_product["owner_names"].append(display_name or owner["name"])
                        else:
                            data_product["owners_count"] = 0
                            data_product["owner_names"] = []

                        # Add other metadata
                        data_product["institutionalMemory"] = entity.get("institutionalMemory")
                        data_product["glossaryTerms"] = entity.get("glossaryTerms")
                        data_product["domain"] = entity.get("domain")
                        data_product["application"] = entity.get("application")
                        data_product["tags"] = entity.get("tags")
                        data_product["structuredProperties"] = entity.get("structuredProperties")

                        # Calculate additional counts for display
                        glossary_terms = entity.get("glossaryTerms", {})
                        terms = glossary_terms.get("terms", []) if glossary_terms else []
                        data_product["glossary_terms_count"] = len(terms)

                        tags = entity.get("tags", {})
                        tag_list = tags.get("tags", []) if tags else []
                        data_product["tags_count"] = len(tag_list)

                        structured_props = entity.get("structuredProperties", {})
                        props = structured_props.get("properties", []) if structured_props else []
                        data_product["structured_properties_count"] = len(props)

                        data_products.append(data_product)

                return data_products

            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.warning(
                    f"GraphQL errors when listing data products: {', '.join(error_messages)}"
                )

            return []
        except Exception as e:
            self.logger.error(f"Error listing data products: {str(e)}")
            return []

    def get_data_product(self, data_product_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific data product by URN.

        Args:
            data_product_urn (str): URN of the data product to retrieve

        Returns:
            Data product object if found, None otherwise
        """
        self.logger.info(f"Getting data product: {data_product_urn}")

        # Use the same query as list_data_products but with specific URN
        result = self.list_data_products(query=data_product_urn, start=0, count=1)
        
        if result and len(result) > 0:
            return result[0]
        
        return None

    def get_datasets_by_urns(self, entity_urns: List[str]) -> Dict[str, Any]:
        """
        Get dataset information for a list of entity URNs.
        
        Args:
            entity_urns (List[str]): List of entity URNs to fetch dataset information for
            
        Returns:
            Dictionary with success status and dataset information
        """
        if not entity_urns:
            return {"success": True, "data": {"searchResults": []}}
        
        self.logger.info(f"Getting dataset information for {len(entity_urns)} entity URNs: {entity_urns}")
        
        # GraphQL query for dataset information
        graphql_query = """
        query GetDatasets($input: SearchAcrossEntitiesInput!) {
          searchAcrossEntities(input: $input) {
            start
            count
            total
            searchResults {
              entity {
                urn
                ... on Dataset {
                  properties {
                    name
                  }
                  browsePaths {
                    path
                  }
                  browsePathV2 {
                    path {
                      entity {
                        ... on Container {
                          properties {
                            name
                          }
                        }
                      }
                    }
                  }
                  platform {
                    name
                  }
                  dataPlatformInstance {
                    properties {
                      name
                    }
                  }
                }
              }
            }
          }
        }
        """
        
        variables = {
            "input": {
                "types": ["DATASET"],
                "query": "*",
                "start": 0,
                "count": 100,
                "orFilters": [
                    {
                        "and": [
                            {
                                "field": "urn",
                                "condition": "IN",
                                "values": entity_urns
                            }
                        ]
                    }
                ]
            }
        }
        
        try:
            self.logger.debug(f"Executing dataset query with variables: {variables}")
            result = self.execute_graphql(graphql_query, variables)
            
            if result and "data" in result and "searchAcrossEntities" in result["data"]:
                search_data = result["data"]["searchAcrossEntities"]
                
                # Process the results to build browse paths
                processed_results = []
                for search_result in search_data.get("searchResults", []):
                    entity = search_result.get("entity", {})
                    if entity:
                        self.logger.debug(f"Processing dataset entity: {entity.get('urn', 'Unknown URN')}")
                        # Build browse path from browsePathV2, fallback to browsePaths
                        browse_path = self._build_browse_path(entity)
                        entity["computed_browse_path"] = browse_path
                        self.logger.debug(f"Computed browse path: {browse_path}")
                        processed_results.append(search_result)
                
                return {
                    "success": True,
                    "data": {
                        "start": search_data.get("start", 0),
                        "count": search_data.get("count", 0),
                        "total": search_data.get("total", 0),
                        "searchResults": processed_results
                    }
                }
            else:
                error_msg = "No data returned from DataHub"
                if result and "errors" in result:
                    errors = self._get_graphql_errors(result)
                    error_msg = f"GraphQL errors: {', '.join(errors)}"
                
                self.logger.error(f"Error in get_datasets_by_urns: {error_msg}")
                return {
                    "success": False,
                    "error": error_msg
                }
                
        except Exception as e:
            self.logger.error(f"Error getting datasets by URNs: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _build_browse_path(self, entity: Dict[str, Any]) -> str:
        """
        Build a browse path from entity data, preferring browsePathV2 over browsePaths.
        
        Args:
            entity: Entity data containing browsePath information
            
        Returns:
            String representation of the browse path
        """
        browse_path_parts = []
        
        try:
            # Try browsePathV2 first (preferred)
            browse_path_v2 = entity.get("browsePathV2", {})
            if browse_path_v2 and browse_path_v2.get("path"):
                path_entities = browse_path_v2.get("path", [])
                for path_entity in path_entities:
                    # Check if path_entity and its entity are not None
                    if path_entity and path_entity.get("entity") is not None:
                        entity_data = path_entity.get("entity", {})
                        if entity_data and entity_data.get("properties", {}).get("name"):
                            browse_path_parts.append(entity_data["properties"]["name"])
            
            # Fallback to browsePaths if browsePathV2 is empty or has no valid entities
            if not browse_path_parts:
                browse_paths = entity.get("browsePaths", [])
                if browse_paths and isinstance(browse_paths, list):
                    first_browse_path = browse_paths[0]
                    
                                         # browsePaths is a list of objects with 'path' property
                    if isinstance(first_browse_path, dict) and "path" in first_browse_path:
                        path_value = first_browse_path.get("path")
                        
                        # Case 1: path is a list of strings (like ['prod', 'hive'])
                        if isinstance(path_value, list):
                            # Remove first and last values from the list
                            if len(path_value) > 2:
                                middle_parts = path_value[1:-1]  # Remove first and last
                                browse_path_parts.extend([str(part) for part in middle_parts if part])
                            # If list has 2 or fewer elements, removing first and last leaves empty
                        
                        # Case 2: path is a single string (like "/prod/hive")
                        elif isinstance(path_value, str) and path_value:
                            path_parts = [part for part in path_value.strip("/").split("/") if part]
                            # Remove first and last values from the split parts
                            if len(path_parts) > 2:
                                middle_parts = path_parts[1:-1]  # Remove first and last
                                browse_path_parts.extend(middle_parts)
                    
                                         # browsePaths is a list of strings directly
                    elif isinstance(first_browse_path, str):
                        path_parts = [part for part in first_browse_path.strip("/").split("/") if part]
                        # Remove first and last values from the split parts
                        if len(path_parts) > 2:
                            middle_parts = path_parts[1:-1]  # Remove first and last
                            browse_path_parts.extend(middle_parts)
                    
                    else:
                        self.logger.debug(f"Unexpected browsePaths structure: {browse_paths}")
            
            # Don't add the entity name at the end - this was causing duplication
            
            # Join with '/' to create the final browse path
            return "/".join(browse_path_parts) if browse_path_parts else ""
            
        except Exception as e:
            self.logger.error(f"Error building browse path for entity: {e}")
            self.logger.debug(f"Entity data: {entity}")
            # Fallback to just the entity name if available
            properties = entity.get("properties", {})
            entity_name = properties.get("name", "")
            return entity_name

    def get_data_contracts(self, query="*", start=0, count=100) -> Dict[str, Any]:
        """
        Get data contracts from DataHub with comprehensive information including related dataset details.

        Args:
            query (str): Search query to filter data contracts (default: "*")
            start (int): Starting offset for pagination
            count (int): Maximum number of data contracts to return

        Returns:
            Dictionary with success status and data contracts information
        """
        self.logger.info(
            f"Getting data contracts with query: {query}, start: {start}, count: {count}"
        )

        # GraphQL query for data contracts with comprehensive information
        graphql_query = """
        query GetDataContracts($input: SearchAcrossEntitiesInput!) {
          searchAcrossEntities(input: $input) {
            start
            count
            total
            searchResults {
              entity {
                urn
                ... on DataContract {
                  properties {
                    entityUrn
                    freshness {
                      assertion {
                        urn
                        info {
                          description
                        }
                      }
                    }
                    schema {
                      assertion {
                        urn
                        info {
                          description
                        }
                      }
                    }
                    dataQuality {
                      assertion {
                        urn
                        info {
                          description
                        }
                      }
                    }
                  }
                  status {
                    state
                  }
                  structuredProperties {
                    properties {
                      structuredProperty {
                        urn
                      }
                      values {
                        ... on StringValue {
                          stringValue
                        }
                        ... on NumberValue {
                          numberValue
                        }
                      }
                      valueEntities {
                        urn
                      }
                    }
                  }
                  result(refresh: false) {
                    type
                  }
                }
              }
            }
          }
        }
        """

        variables = {
            "input": {
                "types": ["DATA_CONTRACT"],
                "query": query,
                "start": start,
                "count": count,
                "filters": [],
            }
        }

        try:
            result = self.execute_graphql(graphql_query, variables)

            if result and "data" in result and "searchAcrossEntities" in result["data"]:
                search_data = result["data"]["searchAcrossEntities"]
                
                # Extract entity URNs from data contracts
                entity_urns = []
                search_results = search_data.get("searchResults", [])
                
                for contract_result in search_results:
                    entity = contract_result.get("entity", {})
                    if entity:
                        properties = entity.get("properties", {})
                        entity_urn = properties.get("entityUrn")
                        if entity_urn:
                            entity_urns.append(entity_urn)
                            self.logger.debug(f"Found entity URN for dataset lookup: {entity_urn}")
                
                self.logger.info(f"Extracted {len(entity_urns)} entity URNs from {len(search_results)} data contracts")
                
                # Get dataset information for the entity URNs
                dataset_info = {}
                if entity_urns:
                    self.logger.info(f"Fetching dataset information for {len(entity_urns)} entities: {entity_urns}")
                    dataset_result = self.get_datasets_by_urns(entity_urns)
                    
                    if dataset_result.get("success") and dataset_result.get("data"):
                        dataset_search_results = dataset_result["data"].get("searchResults", [])
                        self.logger.info(f"Dataset query returned {len(dataset_search_results)} results")
                        
                        # Create a mapping of URN to dataset info
                        for dataset_result_item in dataset_search_results:
                            dataset_entity = dataset_result_item.get("entity", {})
                            if dataset_entity and dataset_entity.get("urn"):
                                dataset_urn = dataset_entity["urn"]
                                dataset_info[dataset_urn] = dataset_entity
                                self.logger.debug(f"Mapped dataset URN {dataset_urn} to dataset info")
                    else:
                        self.logger.warning(f"Dataset query failed or returned no data: {dataset_result}")
                
                # Enhance contract data with dataset information
                enhanced_results = []
                for contract_result in search_results:
                    entity = contract_result.get("entity", {})
                    if entity:
                        properties = entity.get("properties", {})
                        entity_urn = properties.get("entityUrn")
                        
                        # Add dataset information if available
                        if entity_urn and entity_urn in dataset_info:
                            entity["dataset_info"] = dataset_info[entity_urn]
                        
                        enhanced_results.append(contract_result)
                
                return {
                    "success": True,
                    "data": {
                        "start": search_data.get("start", start),
                        "count": search_data.get("count", 0),
                        "total": search_data.get("total", 0),
                        "searchResults": enhanced_results
                    }
                }

            else:
                error_msg = "No data returned from DataHub"
                if result and "errors" in result:
                    errors = self._get_graphql_errors(result)
                    error_msg = f"GraphQL errors: {', '.join(errors)}"
                
                self.logger.error(f"Error in get_data_contracts: {error_msg}")
                return {
                    "success": False,
                    "error": error_msg
                }

        except Exception as e:
            self.logger.error(f"Error getting data contracts: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }

    def import_glossary_node(self, node_data: Dict[str, Any]) -> Optional[str]:
        """
        Import a glossary node using direct GraphQL mutation.

        Args:
            node_data: Glossary node data containing properties, parentNode, ownership, etc.

        Returns:
            Glossary node URN if successful, None otherwise
        """
        self.logger.info(f"Importing glossary node: {node_data.get('properties', {}).get('name', 'Unknown')}")
        
        try:
            # Extract data from nested structure
            properties = node_data.get('properties', {})
            name = properties.get('name')
            description = properties.get('description', '')
            urn = node_data.get('urn')
            
            if not name:
                self.logger.error("Glossary node name is required")
                return None
            
            # Build mutation variables
            variables = {
                "input": {
                    "name": name,
                    "description": description,
                }
            }
            
            # Add URN if provided
            if urn:
                variables["input"]["urn"] = urn
            
            # Add parent node if exists
            parent_node = node_data.get('parentNode')
            if parent_node and parent_node.get('urn'):
                variables["input"]["parentNode"] = parent_node.get('urn')
            
            # Add ownership if exists
            ownership = node_data.get('ownership')
            if ownership and ownership.get('owners'):
                variables["input"]["ownership"] = ownership
            
            # Execute GraphQL mutation
            mutation = """
            mutation createGlossaryNode($input: CreateGlossaryNodeInput!) {
                createGlossaryNode(input: $input) {
                    urn
                }
            }
            """
            
            result = self._execute_graphql_query(mutation, variables)
            
            if result and 'data' in result and 'createGlossaryNode' in result['data']:
                created_urn = result['data']['createGlossaryNode']['urn']
                self.logger.info(f"Successfully created glossary node: {created_urn}")
                return created_urn
            else:
                self.logger.error(f"Failed to create glossary node: {result}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error importing glossary node: {str(e)}")
            return None

    def import_glossary_term(self, term_data: Dict[str, Any]) -> Optional[str]:
        """
        Import a glossary term to DataHub.
        
        Args:
            term_data: Dictionary containing term data with keys like 'id', 'name', 'description', etc.
            
        Returns:
            URN of the created/updated term if successful, None otherwise
        """
        try:
            term_id = term_data.get('id') or term_data.get('qualified_name')
            if not term_id:
                self.logger.error("Term data must contain 'id' or 'qualified_name'")
                return None
                
            term_urn = f"urn:li:glossaryTerm:{term_id}"
            
            mutation = """
            mutation createGlossaryTerm($input: CreateGlossaryTermInput!) {
                createGlossaryTerm(input: $input)
            }
            """
            
            variables = {
                "input": {
                    "id": term_id,
                    "name": term_data.get('name', term_id),
                    "description": term_data.get('description', ''),
                    "parentNode": term_data.get('parent_node_urn')
                }
            }
            
            result = self.execute_graphql(mutation, variables)
            
            if result and "data" in result and "createGlossaryTerm" in result["data"]:
                created_urn = result["data"]["createGlossaryTerm"]
                if created_urn:
                    self.logger.info(f"Successfully imported glossary term: {created_urn}")
                    return created_urn
            
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.error(f"GraphQL errors when importing glossary term: {', '.join(error_messages)}")
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error importing glossary term: {str(e)}")
            return None

    def delete_structured_property(self, property_urn: str) -> bool:
        """
        Delete a structured property from DataHub.

        Args:
            property_urn (str): Structured Property URN to delete

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Deleting structured property: {property_urn}")

        mutation = """
        mutation deleteStructuredProperty($urn: String!) {
          deleteStructuredProperty(urn: $urn)
        }
        """

        variables = {"urn": property_urn}

        try:
            result = self.execute_graphql(mutation, variables)
            self.logger.debug(f"Delete structured property GraphQL result: {result}")

            # Check for explicit errors first
            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.error(
                    f"GraphQL errors when deleting structured property: {', '.join(error_messages)}"
                )
                return False

            # If we have a result with data and no errors, consider it successful
            if result and "data" in result:
                delete_result = result["data"].get("deleteStructuredProperty")
                self.logger.debug(f"Delete structured property success value: {delete_result}")
                
                # For delete operations, we'll be very permissive:
                # - True = explicit success
                # - None/null = likely success (common in GraphQL mutations)
                # - Only False = explicit failure
                if delete_result is not False:
                    self.logger.info(f"Successfully deleted structured property {property_urn} (result: {delete_result})")
                    return True
                else:
                    self.logger.warning(f"Structured property deletion returned explicit False: {property_urn}")
                    return False

            # If we have any result without errors, assume success
            if result:
                self.logger.info(f"Structured property deletion assumed successful (no errors): {property_urn}")
                return True

            self.logger.warning(f"No result returned for structured property deletion: {property_urn}")
            return False
        except Exception as e:
            self.logger.error(f"Error deleting structured property: {str(e)}")
            return False

    def create_structured_property(
        self, display_name: str, description: str = "",
        value_type: str = "STRING", cardinality: str = "SINGLE",
        entity_types: List[str] = None, allowedValues: List[Any] = None,
        qualified_name: str = None, **kwargs):
        """
        Create a structured property in DataHub
        
        Args:
            display_name (str): Display name for the property
            description (str): Description for the property  
            value_type (str): Value type (STRING, NUMBER, etc.)
            cardinality (str): Cardinality (SINGLE, MULTIPLE)
            entity_types (List[str]): List of entity types this property applies to
            allowedValues (List[Any]): List of allowed values
            qualified_name (str): Qualified name for the property
            **kwargs: Additional arguments
            
        Returns:
            str: The URN of the created property
        """
        self.logger.info(f"Creating structured property: {display_name}")

        mutation = """
        mutation createStructuredProperty($input: CreateStructuredPropertyInput!) {
          createStructuredProperty(input: $input) {
            urn
            type
            exists
            definition {
              displayName
              qualifiedName
              description
              valueType {
                urn
                type
                __typename
              }
              cardinality
              entityTypes {
                urn
                type
                __typename
              }
              __typename
            }
            __typename
          }
        }
        """

        # Map simple value types to DataHub URNs if needed
        value_type_mapping = {
            "STRING": "urn:li:dataType:datahub.string",
            "NUMBER": "urn:li:dataType:datahub.number",
            "DATE": "urn:li:dataType:datahub.date",
            "BOOLEAN": "urn:li:dataType:datahub.boolean",
            "URN": "urn:li:dataType:datahub.urn"
        }
        
        # Use the mapped value type if it's a simple string, otherwise use as-is
        formatted_value_type = value_type_mapping.get(value_type.upper(), value_type) if isinstance(value_type, str) and not value_type.startswith("urn:") else value_type

        # Build the input according to DataHub's CreateStructuredPropertyInput schema
        input_data = {
            "id": display_name,
            "qualifiedName": qualified_name or display_name,
            "displayName": display_name,
            "description": description,
            "valueType": formatted_value_type,
            "cardinality": cardinality
        }

        if entity_types:
            # Ensure entity types are formatted as URNs
            formatted_entity_types = []
            for entity_type in entity_types:
                if isinstance(entity_type, str) and not entity_type.startswith("urn:"):
                    formatted_entity_types.append(f"urn:li:entityType:datahub.{entity_type}")
                else:
                    formatted_entity_types.append(entity_type)
            input_data["entityTypes"] = formatted_entity_types

        if allowedValues:
            # Format allowed values according to DataHub's AllowedValueInput structure
            formatted_allowed_values = []
            for allowed_value in allowedValues:
                formatted_value = {}
                
                # Handle different input formats
                if isinstance(allowed_value, dict):
                    # If it's already a dict, check if it has the correct structure
                    if 'value' in allowed_value:
                        # Extract the actual value from the nested structure
                        raw_value = allowed_value['value']
                        if isinstance(raw_value, dict):
                            # Handle nested value structures like {"double": 30}
                            if 'double' in raw_value:
                                formatted_value["numberValue"] = raw_value['double']
                            elif 'string' in raw_value:
                                formatted_value["stringValue"] = raw_value['string']
                            else:
                                # Try to infer the type
                                for key, val in raw_value.items():
                                    if key in ['double', 'float', 'number']:
                                        formatted_value["numberValue"] = val
                                    elif key in ['string', 'str']:
                                        formatted_value["stringValue"] = val
                        else:
                            # Direct value, infer type
                            if isinstance(raw_value, (int, float)):
                                formatted_value["numberValue"] = float(raw_value)
                            else:
                                formatted_value["stringValue"] = str(raw_value)
                    elif 'stringValue' in allowed_value or 'numberValue' in allowed_value:
                        # Already in correct format
                        formatted_value = allowed_value.copy()
                    else:
                        # Dict without proper structure, try to infer
                        if 'description' in allowed_value:
                            formatted_value["description"] = allowed_value['description']
                        
                        # Look for value indicators
                        for key, val in allowed_value.items():
                            if key not in ['description']:
                                if isinstance(val, (int, float)):
                                    formatted_value["numberValue"] = float(val)
                                else:
                                    formatted_value["stringValue"] = str(val)
                                break
                else:
                    # Direct value, infer type
                    if isinstance(allowed_value, (int, float)):
                        formatted_value["numberValue"] = float(allowed_value)
                    else:
                        formatted_value["stringValue"] = str(allowed_value)
                
                formatted_allowed_values.append(formatted_value)
            
            input_data["allowedValues"] = formatted_allowed_values

        variables = {
            "input": input_data
        }

        try:
            result = self.execute_graphql(mutation, variables)
            self.logger.debug(f"Create structured property GraphQL result: {result}")

            if result and "data" in result and "createStructuredProperty" in result["data"]:
                created_property = result["data"]["createStructuredProperty"]
                if created_property and created_property.get("urn"):
                    created_urn = created_property["urn"]
                    self.logger.info(f"Successfully created structured property: {created_urn}")
                    return created_urn

            if result and "errors" in result:
                error_messages = [
                    e.get("message", "") for e in result.get("errors", [])
                ]
                self.logger.error(
                    f"GraphQL errors when creating structured property: {', '.join(error_messages)}"
                )

            return None
        except Exception as e:
            self.logger.error(f"Error creating structured property: {str(e)}")
            return None
