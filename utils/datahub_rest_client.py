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

    def __init__(self, server_url: str, token: Optional[str] = None, verify_ssl=True):
        """
        Initialize the DataHub REST client

        Args:
            server_url: DataHub GMS server URL
            token: DataHub authentication token (optional)
            verify_ssl: Whether to verify SSL certificates (default: True)
        """
        self.server_url = server_url.rstrip('/')
        self.token = token
        self.verify_ssl = verify_ssl
        self.headers = {
            "accept": "application/json",
            "Content-Type": "application/json"
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
        self.logger.debug(f"Initialized DataHub client with URL: {server_url}, token provided: {token is not None}, verify_ssl: {verify_ssl}")
            
        # Initialize Graph client if SDK is available
        self.graph = None
        if DATAHUB_SDK_AVAILABLE:
            try:
                config = DatahubClientConfig(
                    server=server_url,
                    token=token,
                )
                # Add verify_ssl parameter if available in the SDK version
                if hasattr(config, 'verify_ssl'):
                    config.verify_ssl = verify_ssl
                
                self.graph = DataHubGraph(config=config)
                # Also set verify_ssl on the graph client if available
                if hasattr(self.graph, 'verify_ssl'):
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
            # Only log a portion of the query to avoid overwhelming the logs
            query_preview = query.replace('\n', ' ').replace('  ', ' ')[:200] + "..."
            self.logger.info(f"Query: {query_preview}")
            self.logger.info(f"Variables: {variables}")
            
            # Try with DataHubGraph client if available
            if hasattr(self, 'dhg_client') and self.dhg_client:
                self.logger.debug(f"Executing GraphQL query with DataHubGraph client: {query[:100]}...")
                self.logger.debug(f"Variables: {variables}")
                return self.dhg_client.execute_graphql(query, variables)
                
            # Fallback to direct HTTP request
            self.logger.debug(f"Executing GraphQL query with direct HTTP request: {query[:100]}...")
            self.logger.debug(f"Variables: {variables}")
            
            payload = {
                "query": query,
                "variables": variables or {}
            }
            
            graphql_url = f"{self.server_url}/api/graphql"
            self.logger.info(f"Sending request to: {graphql_url}")
            
            response = self._session.post(
                graphql_url,
                json=payload,
                headers=self._get_auth_headers()
            )
            
            self.logger.info(f"GraphQL response status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                # Check for GraphQL errors
                if "errors" in result:
                    self.logger.warning(f"GraphQL query returned errors: {result['errors']}")
                return result
            else:
                self.logger.warning(f"GraphQL query failed with status {response.status_code}: {response.text}")
                return None
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
        
        # Check for errors
        if "errors" in result and result["errors"]:
            error_msg = result["errors"][0].get("message", "Unknown GraphQL error")
            logger.error(f"Error listing glossary nodes: {error_msg}")
            return None
            
        # Return the data from the response
        return result.get("data")
    
    def test_connection(self) -> bool:
        """
        Test connection to DataHub
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            response = self._session.get(
                f"{self.server_url}/config",
                timeout=10
            )
            self.logger.debug(f"Config endpoint response: {response.status_code}")
            if response.status_code != 200:
                logger.error(f"Failed to access config endpoint: {response.status_code}")
                return False

            # Try to list recipes to check permissions
            try:
                sources = self.list_ingestion_sources()
                if not isinstance(sources, list):
                    logger.error("Failed to list ingestion sources: Invalid response format")
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
            logger.error(f"Error testing connection: {str(e)}")
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
        type: Optional[str] = None,  # Added to handle type parameter instead of source_type
        schedule: Optional[Union[Dict[str, str], str]] = None,  # Added to handle schedule as dict
        **kwargs  # Add **kwargs to handle any additional parameters
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
            source_type = config_dict.get("source_type", config_dict.get("type", source_type))
            schedule_dict = config_dict.get("schedule", schedule)
            executor_id = config_dict.get("executor_id", config_dict.get("config", {}).get("executorId", executor_id))
            debug_mode = config_dict.get("debug_mode", config_dict.get("config", {}).get("debugMode", debug_mode))
            extra_args = config_dict.get("extra_args", config_dict.get("config", {}).get("extraArgs", extra_args))
            
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
            raise ValueError("Either 'source_type' or 'type' parameter must be provided")
        
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
            self.logger.debug(f"Converting recipe dict to JSON string")
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
            self.logger.info(f"Creating ingestion source via GraphQL")
            
            # Check if the GraphQL schema has been validated before
            if hasattr(self, '_schema_validated') and not self._schema_validated:
                self.logger.info(f"Skipping GraphQL approach due to previous schema validation failures")
                raise Exception("Schema validation previously failed, falling back to REST API")
            
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
                "schedule": {
                    "interval": schedule_interval,
                    "timezone": timezone
                },
                "config": {
                    "executorId": executor_id,
                    "debugMode": debug_mode
                }
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
                
            variables = {
                "input": graphql_input
            }
                
            self.logger.debug(f"GraphQL variables: {json.dumps(variables)}")
            result = self.execute_graphql(mutation, variables)
            
            if "errors" in result:
                # Check for schema validation errors which indicate incompatible API versions
                schema_errors = [e for e in result.get("errors", []) 
                               if e.get("message", "").find("Validation error (UnknownType)") >= 0 
                               or e.get("message", "").find("Unknown type") >= 0]
                
                if schema_errors:
                    # This is a GraphQL schema mismatch, likely due to API version differences
                    self.logger.info("Detected GraphQL schema mismatch. Your client is likely connecting to a different DataHub API version than expected.")
                    self.logger.info("This is normal when using this client with different DataHub versions. Falling back to REST API.")
                    # Mark schema as invalid to avoid trying GraphQL in future calls
                    self._schema_validated = False
                    # Skip the direct GraphQL endpoint which would also fail
                    raise Exception("Schema validation failed, falling back to REST API")
                else:
                    # Other types of GraphQL errors
                    error_msgs = [e.get("message", "") for e in result.get("errors", [])]
                    self.logger.warning(f"GraphQL errors when creating ingestion source: {', '.join(error_msgs)}")
                    # Continue to REST API fallback
            else:
                # Success - createIngestionSource returns the URN
                created_urn = result.get("data", {}).get("createIngestionSource")
                if created_urn:
                    self.logger.info(f"Successfully created ingestion source via GraphQL: {source_id}")
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
                            "extraArgs": extra_args
                        }
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
                            "extraArgs": extra_args
                        }
                    }
        except Exception as e:
            is_schema_error = str(e).find("Schema validation") >= 0 or str(e).find("Unknown type") >= 0
            
            if is_schema_error:
                # Skip direct GraphQL endpoint if schema errors detected
                self.logger.info("Detected schema validation error: Schema validation failed or Unknown type found")
                self.logger.info("This is normal when using this client with different DataHub versions. Falling back to REST API")
            else:
                self.logger.warning(f"Error creating ingestion source via GraphQL: {str(e)}")
                
                # Continue to direct GraphQL endpoint only if not a schema error
                try:
                    self.logger.info(f"Trying direct GraphQL endpoint for creation")
                    
                    # Set up headers
                    headers = self.headers.copy() if hasattr(self, 'headers') else {'Content-Type': 'application/json'}
                    if hasattr(self, 'token') and self.token:
                        headers['Authorization'] = f'Bearer {self.token}'
                    
                    # Simple GraphQL mutation with variables
                    direct_mutation = {
                        "query": """
                            mutation createIngestionSource($input: CreateIngestionSourceInput!) {
                                createIngestionSource(input: $input)
                            }
                        """,
                        "variables": variables
                    }
                    
                    direct_response = requests.post(
                        f"{self.server_url}/api/graphql",
                        headers=headers,
                        json=direct_mutation
                    )
                    
                    if direct_response.status_code == 200:
                        direct_result = direct_response.json()
                        if "errors" not in direct_result:
                            created_urn = direct_result.get("data", {}).get("createIngestionSource")
                            self.logger.info(f"Successfully created ingestion source via direct GraphQL: {source_id}")
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
                                    "extraArgs": extra_args
                                }
                            }
                        else:
                            # Check for schema validation errors in the direct endpoint too
                            direct_schema_errors = [e for e in direct_result.get("errors", []) 
                                                 if isinstance(e, dict) and 
                                                 e.get("message", "").find("Validation error (UnknownType)") >= 0]
                            
                            if direct_schema_errors:
                                self.logger.info("GraphQL schema mismatch with direct endpoint. Falling back to REST API.")
                                self._schema_validated = False
                            else:
                                self.logger.warning(f"GraphQL errors with direct endpoint: {direct_result.get('errors')}")
                    else:
                        self.logger.warning(f"Failed with direct GraphQL endpoint: {direct_response.status_code}")
                except Exception as direct_e:
                    self.logger.warning(f"Error with direct GraphQL endpoint: {str(direct_e)}")
            
        # Fall back to REST API
        try:
            self.logger.info(f"Creating ingestion source via REST API: {name}")
            
            # Prepare payload for OpenAPI v3
            payload = [{
                "urn": source_urn,
                "dataHubIngestionSourceKey": {
                    "value": {
                        "id": source_id
                    }
                },
                "dataHubIngestionSourceInfo": {
                    "value": {
                        "name": name,
                        "type": source_type,
                        "schedule": {
                            "interval": schedule_interval,
                            "timezone": timezone
                        },
                        "config": {
                            "executorId": executor_id,
                            "debugMode": debug_mode
                        }
                    }
                }
            }]
            
            # Handle recipe differently based on type
            if isinstance(recipe, dict) and "source" in recipe:
                # If recipe has a source field, convert to string
                recipe_str = json.dumps(recipe)
            
            # Add recipe to config if provided
            if recipe_str is not None:
                payload[0]["dataHubIngestionSourceInfo"]["value"]["config"]["recipe"] = recipe_str
            
            # Add extra args only if provided
            if extra_args:
                payload[0]["dataHubIngestionSourceInfo"]["value"]["config"]["extraArgs"] = extra_args
            
            self.logger.debug(f"REST API payload: {json.dumps(payload)}")
            
            response = requests.post(
                f"{self.server_url}/openapi/v3/entity/datahubingestionsource",
                headers=self.headers,
                json=payload
            )
            
            if response.status_code in (200, 201, 202):
                self.logger.info(f"Successfully created ingestion source via REST API: {source_id}")
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
                            "extraArgs": extra_args
                        }
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
                            "extraArgs": extra_args
                        }
                    }
            else:
                self.logger.error(f"Failed to create ingestion source via REST API: {response.status_code} - {response.text}")
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
        logger.info(f"Attempting to trigger ingestion for source: {ingestion_source_id} (URN: {source_urn})")

        # Method 1: Use /runs endpoint (DataHub v0.12.0+)
        try:
            logger.debug("Trying /runs endpoint...")
            url = f"{self.server_url}/runs?urn={source_urn}"
            response = requests.post(url, headers=self.headers)
            if response.status_code == 200:
                logger.info(f"Successfully triggered ingestion using /runs endpoint")
                return True
            logger.debug(f"Failed to trigger using /runs endpoint: {response.status_code} - {response.text}")
        except Exception as e:
            logger.debug(f"Error triggering using /runs endpoint: {e}")

        # Method 2: Use /ingest/{id} endpoint
        try:
            logger.debug("Trying /ingest/{id} endpoint...")
            url = f"{self.server_url}/ingest/{ingestion_source_id}"
            response = requests.post(url, headers=self.headers)
            if response.status_code == 200:
                logger.info(f"Successfully triggered ingestion using /ingest/{id} endpoint")
                return True
            logger.debug(f"Failed to trigger using /ingest/{id} endpoint: {response.status_code} - {response.text}")
        except Exception as e:
            logger.debug(f"Error triggering using /ingest/{id} endpoint: {e}")

        # Method 3: Use legacy ?action=ingest endpoint
        try:
            logger.debug("Trying legacy ?action=ingest endpoint...")
            url = f"{self.server_url}/ingestion-sources/{ingestion_source_id}?action=ingest"
            response = requests.post(url, headers=self.headers)
            if response.status_code == 200:
                logger.info(f"Successfully triggered ingestion using legacy endpoint")
                return True
            logger.debug(f"Failed to trigger using legacy endpoint: {response.status_code} - {response.text}")
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
            variables = {
                "input": {
                    "ingestionSourceUrn": source_urn
                }
            }
            
            result = self.execute_graphql(graphql_query, variables)
            
            if result and "errors" not in result:
                logger.info(f"Successfully triggered ingestion using createIngestionExecutionRequest mutation")
                return True
            logger.debug(f"GraphQL mutation returned errors: {result.get('errors') if result else 'No result'}")
        except Exception as e:
            logger.debug(f"Error triggering using createIngestionExecutionRequest mutation: {e}")

        # Method 5: Use executeIngestionSource GraphQL mutation (legacy fallback)
        try:
            logger.debug("Trying executeIngestionSource GraphQL mutation (legacy fallback)...")
            graphql_query = """
            mutation executeIngestionSource($input: ExecuteIngestionSourceInput!) {
                executeIngestionSource(input: $input) {
                    executionId
                }
            }
            """
            variables = {
                "input": {
                    "urn": source_urn
                }
            }
            
            result = self.execute_graphql(graphql_query, variables)
            
            if result and "errors" not in result:
                logger.info(f"Successfully triggered ingestion using executeIngestionSource mutation")
                return True
            logger.debug(f"GraphQL mutation returned errors: {result.get('errors') if result else 'No result'}")
        except Exception as e:
            logger.debug(f"Error triggering using executeIngestionSource mutation: {e}")

        logger.error(f"All methods to trigger ingestion for source {ingestion_source_id} failed")
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
                    {
                        "field": "sourceType",
                        "values": ["SYSTEM"],
                        "negated": True
                    }
                ]
            }
        }
        
        try:
            self.logger.debug("Listing ingestion sources using GraphQL")
            result = self.execute_graphql(query, variables)
            
            if result and isinstance(result, dict) and "data" in result and result["data"] and "listIngestionSources" in result["data"]:
                response_data = result["data"]["listIngestionSources"]
                if response_data is None:
                    self.logger.warning("listIngestionSources returned None in GraphQL response")
                    raw_sources = []
                else:
                    raw_sources = response_data.get("ingestionSources", [])
                    if raw_sources is None:
                        self.logger.warning("ingestionSources is None in GraphQL response")
                        raw_sources = []
                
                self.logger.info(f"Successfully retrieved {len(raw_sources)} ingestion sources using GraphQL")
                
                # Process each source
                for source in raw_sources:
                    if source is None:
                        self.logger.warning("Skipping None source in response")
                        continue
                        
                    try:
                        urn = source.get("urn")
                        if not urn:
                            self.logger.warning(f"Source missing URN, skipping: {source}")
                            continue
                            
                        source_id = urn.split(":")[-1] if urn else None
                        if not source_id:
                            self.logger.warning(f"Could not extract source ID from URN: {urn}")
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
                            self.logger.warning(f"Could not parse recipe JSON for {source_id}")
                        
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
                                "version": config.get("version", "0.8.42")
                            }
                        }
                        
                        # Add execution status if available
                        if latest_execution and latest_execution.get("result"):
                            simplified_source["last_execution"] = {
                                "id": latest_execution.get("id"),
                                "status": latest_execution.get("result", {}).get("status"),
                                "startTimeMs": latest_execution.get("result", {}).get("startTimeMs"),
                                "durationMs": latest_execution.get("result", {}).get("durationMs")
                            }
                        
                        sources.append(simplified_source)
                    except Exception as e:
                        self.logger.warning(f"Error processing source {source.get('name', 'unknown')}: {str(e)}")
                
                if sources:
                    self.logger.info(f"Successfully processed {len(sources)} ingestion sources")
                    return sources
            
            # Check for specific errors
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.warning(f"GraphQL errors: {', '.join(error_messages)}")
            else:
                self.logger.warning("Failed to retrieve ingestion sources using GraphQL or no sources found")
            
        except Exception as e:
            self.logger.warning(f"Error listing ingestion sources via GraphQL: {str(e)}")
        
        # Try OpenAPI v3 endpoint
        try:
            openapi_url = f"{self.server_url}/openapi/v3/entity/datahubingestionsource"
            self.logger.debug(f"Listing ingestion sources via OpenAPI v3: GET {openapi_url}")
            
            response = requests.get(openapi_url, headers=self.headers)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    
                    # Parse entities from the response according to the OpenAPI v3 schema
                    entities = data.get("entities", []) or []
                    if entities is None:
                        entities = []
                    self.logger.debug(f"Found {len(entities)} entities in OpenAPI v3 response")
                    
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
                            source_info = entity.get("dataHubIngestionSourceInfo", {}) or {}
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
                                    self.logger.warning(f"Failed to parse recipe for source {urn}")
                                    
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
                                    "extraArgs": config.get("extraArgs", {}) or {}
                                }
                            }
                            
                            sources.append(simplified_source)
                        except Exception as e:
                            self.logger.warning(f"Error processing entity {entity.get('urn')}: {str(e)}")
                    
                    if sources:
                        self.logger.info(f"Successfully retrieved {len(sources)} ingestion sources via OpenAPI v3")                    
                        return sources
                except json.JSONDecodeError:
                    self.logger.warning("Failed to parse JSON response from OpenAPI v3 endpoint")
            else:
                self.logger.warning(f"Failed to list ingestion sources via OpenAPI v3: {response.status_code} - {response.text}")
                
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
                                            "schedule": source.get("schedule", {}) or {}
                                        }
                                        
                                        # Parse recipe if present
                                        if "recipe" in source:
                                            try:
                                                recipe_str = source["recipe"]
                                                if recipe_str is None:
                                                    simplified_source["recipe"] = {}
                                                elif isinstance(recipe_str, str):
                                                    simplified_source["recipe"] = json.loads(recipe_str)
                                                elif isinstance(recipe_str, dict):
                                                    simplified_source["recipe"] = recipe_str
                                            except json.JSONDecodeError:
                                                pass
                                                
                                        sources.append(simplified_source)
                                    except Exception as e:
                                        self.logger.warning(f"Error processing source {source.get('id')}: {str(e)}")
                                        
                                if sources:
                                    self.logger.info(f"Retrieved {len(sources)} ingestion sources via alternative API")
                                    return sources
                        except json.JSONDecodeError:
                            self.logger.warning("Failed to parse JSON from alternative API endpoint")
                except Exception as e:
                    self.logger.debug(f"Error with alternative API endpoint: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error listing ingestion sources via OpenAPI v3: {str(e)}")
        
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
        
        variables = {
            "urn": source_urn
        }
        
        try:
            self.logger.debug(f"Using GraphQL to fetch ingestion source: {source_id}")
            result = self.execute_graphql(query, variables)
            
            if result and "data" in result and result["data"].get("ingestionSource"):
                self.logger.debug(f"Successfully retrieved ingestion source via GraphQL: {source_id}")
                
                ingestion_source = result["data"]["ingestionSource"]
                source_info = {
                    "urn": ingestion_source["urn"],
                    "id": source_id,
                    "name": ingestion_source["name"],
                    "type": ingestion_source["type"],
                    "schedule": ingestion_source["schedule"],
                    "config": {}  # Initialize config to ensure it's never None
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
                            self.logger.debug(f"Found template reference in recipe: {recipe_str}")
                            recipe = recipe_str.strip()
                        else:
                            # Try to parse as JSON, with fallback to raw string
                            try:
                                recipe = json.loads(recipe_str)
                                self.logger.debug(f"Successfully parsed recipe JSON: {json.dumps(recipe)}")
                            except json.JSONDecodeError:
                                self.logger.warning(f"Could not parse recipe JSON for {source_id}, treating as raw string")
                                # If it's not valid JSON, treat it as a raw string
                                # This could be a template or other format
                                recipe = recipe_str
                    else:
                        self.logger.warning(f"Unexpected recipe type: {type(recipe_str)}, using empty dict")
                        recipe = {}
                    
                    # Build config object with all relevant fields    
                    source_info["config"] = {
                        "recipe": recipe_str,  # Store original recipe string to avoid double-parsing
                        "executorId": config.get("executorId", "default"),
                        "debugMode": config.get("debugMode", False),
                        "version": config.get("version")
                    }
                except Exception as e:
                    self.logger.warning(f"Error processing recipe for {source_id}: {str(e)}")
                    source_info["config"]["recipe"] = recipe_str or {}  # Use empty object as fallback
                
                return source_info
            
            # Check for errors
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.warning(f"GraphQL errors: {', '.join(error_messages)}")
            else:
                self.logger.warning(f"GraphQL query for ingestion source returned no results: {source_id}")
            
            # Fall through to OpenAPI v3
        except Exception as e:
            self.logger.warning(f"Error getting ingestion source via GraphQL: {str(e)}")
            # Fall through to OpenAPI v3
        
        # Try OpenAPI v3 endpoint with the exact schema format
        try:
            openapi_url = f"{self.server_url}/openapi/v3/entity/datahubingestionsource/{source_urn}"
            self.logger.debug(f"Fetching ingestion source via OpenAPI v3: GET {openapi_url}")
            
            response = requests.get(openapi_url, headers=self.headers)
            
            if response.status_code == 200:
                self.logger.debug(f"Successfully retrieved ingestion source via OpenAPI v3: {source_id}")
                try:
                    data = response.json()
                    self.logger.debug(f"OpenAPI v3 response: {json.dumps(data)}")
                    
                    # Extract source info from the OpenAPI v3 format
                    # The structure has nested dataHubIngestionSourceInfo.value
                    source_info_wrapper = data.get("dataHubIngestionSourceInfo", {})
                    if not source_info_wrapper:
                        self.logger.warning(f"No dataHubIngestionSourceInfo in response for {source_id}")
                        
                    source_info = source_info_wrapper.get("value", {}) if source_info_wrapper else {}
                    
                    if source_info:
                        # Create the result object
                        result = {
                            "urn": source_urn,
                            "id": source_id,
                            "name": source_info.get("name", source_id),
                            "type": source_info.get("type", ""),
                            "platform": source_info.get("platform", ""),
                            "schedule": source_info.get("schedule", {}),
                            "config": {}  # Initialize config to ensure it's never None
                        }
                        
                        # Parse the recipe JSON if it exists
                        config = source_info.get("config", {}) or {}
                        if config is None:
                            config = {}
                            
                        recipe_str = config.get("recipe")
                        self.logger.debug(f"Raw recipe string from OpenAPI: {recipe_str}")
                        
                        # Build config object with all relevant fields
                        result["config"] = {
                            "recipe": recipe_str,  # Store original recipe string to avoid double-parsing
                            "executorId": config.get("executorId", "default"),
                            "debugMode": config.get("debugMode", False),
                            "version": config.get("version")
                        }
                        
                        return result
                    else:
                        self.logger.warning(f"No source info found in OpenAPI v3 response for {source_id}")
                except json.JSONDecodeError:
                    self.logger.warning(f"Could not parse JSON response from OpenAPI v3 for {source_id}")
                except Exception as e:
                    self.logger.warning(f"Error processing OpenAPI v3 response for {source_id}: {str(e)}")
            else:
                self.logger.warning(f"OpenAPI v3 GET failed: {response.status_code} - {response.text}")
                
            # Try all available sources endpoint as last resort
            try:
                sources = self.list_ingestion_sources()
                for source in sources:
                    if source.get("id") == source_id or source.get("urn") == source_urn:
                        self.logger.info(f"Found ingestion source {source_id} in list of all sources")
                        return source
                self.logger.warning(f"Ingestion source {source_id} not found in list of all sources")
            except Exception as e:
                self.logger.warning(f"Error trying to find source {source_id} in list of all sources: {str(e)}")
        except Exception as e:
            self.logger.warning(f"Error getting ingestion source via OpenAPI v3: {str(e)}")
            
        # All attempts to retrieve the source have failed
        self.logger.warning(f"All attempts to retrieve ingestion source {source_id} failed")
        
        # Return a minimal default source object if we have at least the ID
        # This allows the call site to have some information to work with
        if source_id:
            self.logger.warning(f"Returning default source info for {source_id} as all attempts failed")
            return {
                "urn": source_urn,
                "id": source_id,
                "name": source_id,  # Use ID as name
                "type": "",  # Unknown type
                "schedule": {"interval": "0 0 * * *", "timezone": "UTC"},  # Default schedule
                "config": {
                    "recipe": {},  # Empty recipe
                    "executorId": "default",
                    "debugMode": False,
                    "version": None
                }
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
        
        variables = {
            "urn": source_urn
        }
        
        try:
            self.logger.debug("Deleting ingestion source using GraphQL mutation")
            result = self.execute_graphql(mutation, variables)
            
            if "errors" not in result:
                self.logger.info(f"Successfully deleted ingestion source: {source_id} using GraphQL")
                return True
                
            self.logger.warning(f"GraphQL errors when deleting ingestion source: {result.get('errors')}")
            # Fall back to OpenAPI v3
        except Exception as e:
            self.logger.warning(f"Error deleting ingestion source using GraphQL: {str(e)}")
            # Fall back to OpenAPI v3
        
        # Try OpenAPI v3 endpoint
        try:
            openapi_url = f"{self.server_url}/openapi/v3/entity/datahubingestionsource/{source_urn}"
            self.logger.debug(f"Deleting ingestion source via OpenAPI v3: DELETE {openapi_url}")
            
            response = requests.delete(
                openapi_url,
                headers=self.headers
            )
            
            if response.status_code in (200, 201, 202, 204):
                self.logger.info(f"Successfully deleted ingestion source: {source_id} using OpenAPI v3")
                return True
                
            self.logger.error(f"Failed to delete ingestion source via OpenAPI v3: {response.status_code} - {response.text}")
        except Exception as e:
            self.logger.error(f"Error deleting ingestion source via OpenAPI v3: {str(e)}")
        
        return False
    
    def create_secret(self, name: str, value: str, description: Optional[str] = None) -> bool:
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
                "description": description or f"Secret managed by datahub-recipes-manager"
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
                    self.logger.info(f"Secret {name} already exists, attempting to update it instead")
                    return self.update_secret(name, value, description)
                
                self.logger.warning(f"GraphQL errors when creating secret: {result.get('errors')}")
                self.logger.warning("Falling back to REST API")
        except Exception as e:
            self.logger.warning(f"Error creating secret via GraphQL: {str(e)}")
            self.logger.warning("Falling back to REST API")
        
        # Fall back to REST API using multiple possible endpoints
        try:
            # Set up headers
            headers = self.headers.copy() if hasattr(self, 'headers') else {'Content-Type': 'application/json'}
            if hasattr(self, 'token') and self.token:
                headers['Authorization'] = f'Bearer {self.token}'
            
            # Try multiple possible endpoints
            endpoints = [
                f"{self.server_url}/secrets",
                f"{self.server_url}/openapi/secrets",
                f"{self.server_url}/api/v2/secrets"
            ]
            
            # Prepare payload
            payload = {
                "name": name,
                "value": value,
                "description": description or f"Secret managed by datahub-recipes-manager"
            }
            
            # Try each endpoint
            for url in endpoints:
                self.logger.info(f"Attempting to create secret via REST API: {url}")
                
                response = requests.post(url, headers=headers, json=payload)
                
                if response.status_code in (200, 201, 204):
                    self.logger.info(f"Successfully created secret via endpoint {url}: {name}")
                    return True
                elif response.status_code == 409 or response.status_code == 400:
                    # 409 Conflict or 400 Bad Request with "already exists" in the response
                    if "already exists" in response.text:
                        self.logger.info(f"Secret {name} already exists in REST API, attempting to update it")
                        return self.update_secret(name, value, description)
                else:
                    self.logger.warning(f"Failed to create secret at {url}: {response.status_code} - {response.text}")
            
            # If all endpoints failed, try update as a last resort
            self.logger.warning(f"All direct creation methods failed for secret {name}, trying update as last resort")
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
        
        variables = {
            "urn": urn
        }
        
        try:
            self.logger.debug("Deleting secret using GraphQL mutation")
            result = self.execute_graphql(mutation, variables)
            
            if "errors" not in result:
                self.logger.info(f"Successfully deleted secret: {name_or_urn} using GraphQL")
                return True
                
            self.logger.error(f"GraphQL errors when deleting secret: {result.get('errors')}")
            return False
        except Exception as e:
            self.logger.error(f"Error deleting secret using GraphQL: {str(e)}")
            return False

    def update_secret(self, name: str, value: str, description: Optional[str] = None) -> bool:
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
            self.logger.warning(f"Failed to delete existing secret {name} before update. Will try to create anyway.")
        else:
            self.logger.debug(f"Successfully deleted existing secret {name} before update")
        
        # Then create the secret with the new value
        return self.create_secret(name, value, description)

    def update_ingestion_source(self, source_id: str, recipe_json: dict, schedule: Optional[str] = None) -> Optional[dict]:
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
                "scheduleCron": schedule if schedule else existing.get("scheduleCron", "0 0 * * *"),
                "executions": existing.get("executions", [])
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
                    "schedule": {
                        "interval": config["scheduleCron"],
                        "timezone": "UTC"
                    },
                    "config": {
                        "recipe": config["recipe"],
                        "executorId": "default",
                        "version": "0.0.1"
                    }
                }
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
        schedule: Optional[Dict[str, str]] = None
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
                self.logger.debug(f"Converting recipe dict to JSON string")
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
            self.logger.info(f"Patching ingestion source via GraphQL")
            
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
                self.logger.error("Source type is required but not available from current source or parameters")
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
            if not input_obj or (len(input_obj) == 2 and "type" in input_obj and "name" in input_obj):
                self.logger.warning("No updates to apply to ingestion source")
                return current_source
            
            variables = {
                "urn": urn,
                "input": input_obj
            }
            
            self.logger.debug(f"GraphQL variables: {json.dumps(variables)}")
            result = self.execute_graphql(mutation, variables)
            
            # Check for errors in the GraphQL response
            if result and "errors" in result and result["errors"]:
                error_messages = [error.get("message", "Unknown error") for error in result["errors"]]
                self.logger.warning(f"GraphQL errors when patching ingestion source: {', '.join(error_messages)}")
                # Continue to REST API fallback
            # GraphQL operation succeeded
            elif result:
                self.logger.info(f"Successfully patched ingestion source via GraphQL")
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
                    self.logger.error(f"Could not fetch source info for REST API patching: {urn}")
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
                json=payload
            )
            
            if response.status_code == 200:
                self.logger.info(f"Successfully patched ingestion source via REST API: {urn}")
                return self.get_ingestion_source(urn)
            else:
                self.logger.warning(f"PATCH endpoint failed: {response.status_code} - {response.text}")
                # Try the PUT endpoint with OpenAPI v3 if PATCH fails
                try:
                    self.logger.info(f"Trying OpenAPI v3 PUT endpoint for patching")
                    
                    # For PUT, we need to construct a full entity with all data
                    entity = {
                        "urn": urn,
                        "dataHubIngestionSourceInfo": {
                            "value": {}
                        }
                    }
                    
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
                        json=[entity]
                    )
                    
                    if response.status_code in (200, 201):
                        self.logger.info(f"Successfully patched ingestion source via OpenAPI PUT: {urn}")
                        return self.get_ingestion_source(urn)
                    else:
                        self.logger.error(f"OpenAPI PUT failed: {response.status_code} - {response.text}")
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
                "variables": {
                    "input": {
                        "ingestionSourceUrn": source_urn
                    }
                },
                "query": """
                    mutation createIngestionExecutionRequest($input: CreateIngestionExecutionRequestInput!) {
                        createIngestionExecutionRequest(input: $input)
                    }
                """
            }
            
            # Set up headers
            headers = self.headers.copy() if hasattr(self, 'headers') else {'Content-Type': 'application/json'}
            if hasattr(self, 'token') and self.token:
                headers['Authorization'] = f'Bearer {self.token}'
                
            self.logger.debug(f"Attempting to trigger ingestion via direct GraphQL: {source_id}")
            
            response = requests.post(
                f"{self.server_url}/api/v2/graphql",
                headers=headers,
                json=graphql_query
            )
            
            if response.status_code == 200:
                result = response.json()
                if "errors" not in result:
                    self.logger.info(f"Successfully triggered ingestion source via direct GraphQL: {source_id}")
                    return True
                else:
                    error_msg = f"GraphQL errors when triggering ingestion: {result.get('errors')}"
                    self.logger.warning(error_msg)
            else:
                self.logger.warning(f"Failed to trigger ingestion via direct GraphQL: {response.status_code} - {response.text}")
                
            # Fall through to other methods
        except Exception as e:
            self.logger.warning(f"Error triggering ingestion via direct GraphQL: {str(e)}")
            # Fall through to other methods
            
        # Fallback: Try with DataHubGraph client if available
        try:
            self.logger.debug(f"Attempting to trigger ingestion via DataHubGraph client: {source_id}")
            
            mutation = """
            mutation createIngestionExecutionRequest($input: CreateIngestionExecutionRequestInput!) {
            createIngestionExecutionRequest(input: $input)
            }
            """
            
            variables = {
                "input": {
                    "ingestionSourceUrn": source_urn
                }
            }
        
            result = self.execute_graphql(mutation, variables)
            
            if result and "data" in result and "createIngestionExecutionRequest" in result["data"]:
                self.logger.info(f"Successfully triggered ingestion source via DataHubGraph client: {source_id}")
                return True
            
            # Check for specific errors
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.warning(f"GraphQL errors: {', '.join(error_messages)}")
                
            # Fall through to REST API approaches
        except Exception as e:
            self.logger.warning(f"Error using DataHubGraph client to trigger ingestion: {e}")
            # Fall through to REST API approaches
            
        # Fallback approaches: Try various REST API endpoints
        
        # Build a list of potential endpoints to try
        endpoints = []
        
        # 1. OpenAPI v3 /runs endpoint (DataHub v0.12.0+)
        endpoints.append({
            "url": f"{self.server_url}/runs?urn={source_urn}",
            "method": "post",
            "description": "OpenAPI v3 /runs endpoint"
        })
        
        # 2. Various direct execution endpoints
        endpoints.append({
            "url": f"{self.server_url}/api/v2/ingest/{source_id}",
            "method": "post",
            "description": "Direct ingest API"
        })
        
        # 3. Legacy endpoint with action parameter
        endpoints.append({
            "url": f"{self.server_url}/ingestion-sources/{source_id}?action=ingest",
            "method": "post",
            "description": "Legacy ingestion API with action=ingest"
        })
        
        # 4. Try various OpenAPI specific endpoints
        endpoints.append({
            "url": f"{self.server_url}/openapi/v3/ingest?urn={source_urn}",
            "method": "post",
            "description": "OpenAPI v3 ingest endpoint"
        })
        
        endpoints.append({
            "url": f"{self.server_url}/openapi/v3/ingestion/sources/{source_id}/run",
            "method": "post",
            "description": "OpenAPI v3 ingestion source run endpoint"
        })
        
        # Try each endpoint in order until one succeeds
        for endpoint in endpoints:
            try:
                self.logger.debug(f"Trying {endpoint['description']}: {endpoint['url']}")
                
                if endpoint['method'].lower() == 'post':
                    response = requests.post(endpoint['url'], headers=self.headers)
                else:
                    response = requests.get(endpoint['url'], headers=self.headers)
                
                if response.status_code in (200, 201, 202, 204):
                    self.logger.info(f"Successfully triggered ingestion using {endpoint['description']}")
                    return True
                    
                self.logger.debug(f"Endpoint {endpoint['url']} returned {response.status_code}: {response.text}")
            except Exception as e:
                self.logger.debug(f"Error with {endpoint['description']}: {str(e)}")
                
        # If we've tried all approaches and none worked, return failure
        self.logger.error(f"All methods to trigger ingestion for source {source_id} failed")
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
        
        variables = {
            "input": {
                "start": start,
                "count": count
            }
        }
        
        try:
            logger.info("Listing secrets using GraphQL")
            result = self.execute_graphql(query, variables)
            
            if "errors" in result:
                logger.warning(f"GraphQL errors while listing secrets: {result['errors']}")
                return []
            
            # Extract secrets from response
            secrets_data = result.get("data", {}).get("listSecrets", {}).get("secrets", [])
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
                                "condition": "EQUAL"
                            }
                        ]
                    }
                ]
            }
        }
        
        try:
            result = self.execute_graphql(query, variables)
            
            if result and "data" in result and result["data"] and "listPolicies" in result["data"]:
                policies_data = result["data"]["listPolicies"]
                if policies_data is None:
                    self.logger.warning("listPolicies returned None in GraphQL response")
                    return []
                
                policies = policies_data.get("policies", [])
                if policies is None:
                    policies = []
                
                self.logger.info(f"Successfully retrieved {len(policies)} policies")
                return policies
            
            # Check for errors
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                
                # Check for schema validation errors specifically
                schema_validation_errors = [
                    e for e in error_messages 
                    if "Unknown type 'ListPoliciesInput'" in e or "Validation error" in e
                ]
                
                if schema_validation_errors:
                    # Log schema validation errors at info level since they're normal with different DataHub versions
                    self.logger.info(f"GraphQL schema validation errors: {schema_validation_errors}")
                    self.logger.info("These errors are normal when using different DataHub versions. Falling back to REST API.")
                else:
                    self.logger.warning(f"GraphQL errors when listing policies: {', '.join(error_messages)}")
            else:
                self.logger.warning("Failed to retrieve policies using GraphQL")
        except Exception as e:
            self.logger.warning(f"Error listing policies via GraphQL: {str(e)}")
        
        # Fall back to OpenAPI v3 endpoint
        try:
            url = f"{self.server_url}/openapi/v3/entity/datahubpolicy"
            self.logger.debug(f"Listing policies via OpenAPI v3: GET {url}")
            
            # Set up headers
            headers = self.headers.copy() if hasattr(self, 'headers') else {'Content-Type': 'application/json'}
            if hasattr(self, 'token') and self.token:
                headers['Authorization'] = f'Bearer {self.token}'
            
            response = requests.get(url, headers=headers, params={"start": start, "count": limit})
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
                            "actors": policy_value.get("actors", {})
                        }
                        
                        policies.append(policy)
                    except Exception as e:
                        self.logger.warning(f"Error processing policy: {str(e)}")
                
                self.logger.info(f"Successfully retrieved {len(policies)} policies via OpenAPI v3")
                return policies
            else:
                self.logger.warning(f"Failed to list policies via OpenAPI v3: {response.status_code} - {response.text}")
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
            if result and "data" in result and result["data"] and "policy" in result["data"]:
                policy_data = result["data"]["policy"]
                if policy_data:
                    self.logger.info(f"Successfully retrieved policy {policy_id} via GraphQL")
                    return policy_data
            
            # Check for errors
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                
                # Check for schema validation errors specifically
                schema_validation_errors = [
                    e for e in error_messages 
                    if "Unknown type" in e or "Validation error" in e
                ]
                
                if schema_validation_errors:
                    # Log schema validation errors at info level since they're normal with different DataHub versions
                    self.logger.info(f"GraphQL schema validation errors when getting policy: {schema_validation_errors}")
                    self.logger.info("These errors are normal when using different DataHub versions. Falling back to REST API.")
                else:
                    # Log other errors as warnings
                    self.logger.warning(f"GraphQL policy retrieval failed: {', '.join(error_messages)}, falling back to REST API")
            else:
                self.logger.warning("GraphQL policy retrieval failed with unknown error, falling back to REST API")
        except Exception as e:
            self.logger.warning(f"Error retrieving policy via GraphQL: {str(e)}")
        
        # Fall back to OpenAPI v3 endpoint
        try:
            url = f"{self.server_url}/openapi/v3/entity/datahubpolicy/{policy_urn}"
            self.logger.info(f"Attempting to get policy via OpenAPI v3: GET {url}")
            
            # Set up headers
            headers = self.headers.copy() if hasattr(self, 'headers') else {'Content-Type': 'application/json'}
            if hasattr(self, 'token') and self.token:
                headers['Authorization'] = f'Bearer {self.token}'
            
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                entity_data = response.json()
                
                # Extract the policy information
                if "dataHubPolicyInfo" in entity_data and entity_data["dataHubPolicyInfo"]:
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
                        "actors": policy_value.get("actors", {})
                    }
                    
                    self.logger.info(f"Successfully retrieved policy {policy_id} via OpenAPI v3")
                    return policy_data
                else:
                    self.logger.warning(f"Policy response missing dataHubPolicyInfo: {entity_data}")
            else:
                self.logger.warning(f"Failed to get policy via OpenAPI v3: {response.status_code} - {response.text}")
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
        self.logger.info(f"Creating new policy with name: {policy_data.get('name', 'Unnamed')}")
        
        if not policy_data.get('name'):
            self.logger.error("Policy name is required")
            return None
        
        # Prepare input for GraphQL
        graphql_input = policy_data.copy()
        
        # Important: Remove 'id' field from GraphQL input since it's not part of PolicyUpdateInput
        # This is a key difference between GraphQL and REST API schemas
        if 'id' in graphql_input:
            policy_id = graphql_input.pop('id')
            self.logger.info(f"Removed 'id' field from GraphQL input (value: {policy_id})")
        
        # Ensure resources is properly formatted as a dict with filter if it's a list
        if "resources" in graphql_input and isinstance(graphql_input["resources"], list):
            # Convert list format to the expected structure with filter.criteria
            resources_data = {
                "filter": {
                    "criteria": []
                }
            }
            
            # Add resource type information if available 
            if graphql_input["resources"] and "type" in graphql_input["resources"][0]:
                resources_data["type"] = graphql_input["resources"][0].get("type")
            
            # Add allResources if applicable
            if not graphql_input["resources"] or graphql_input["resources"][0].get("resource") == "*":
                resources_data["allResources"] = True
            elif "resource" in graphql_input["resources"][0]:
                resources_data["resources"] = [r.get("resource") for r in graphql_input["resources"] if "resource" in r]
            
            graphql_input["resources"] = resources_data
        
        # Ensure actors object has all required fields
        if "actors" in graphql_input:
            default_actors = {
                "users": [],
                "groups": [],
                "allUsers": False,
                "allGroups": False,
                "resourceOwners": False,
                "resourceOwnersTypes": None
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
                "resourceOwnersTypes": None
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
            if result and "data" in result and result["data"] and "createPolicy" in result["data"]:
                created_policy_urn = result["data"]["createPolicy"]
                if created_policy_urn:
                    self.logger.info(f"Successfully created policy {policy_data.get('name')} via GraphQL")
                    
                    # Return policy data with URN
                    policy_with_urn = {**policy_data, "urn": created_policy_urn}
                    return policy_with_urn
            
            # Check for errors
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                
                # Check for schema validation errors specifically
                schema_validation_errors = [
                    e for e in error_messages 
                    if "Unknown type 'PolicyUpdateInput'" in e or "Validation error" in e 
                    or "contains a field name" in e  # Additional error pattern to catch
                ]
                
                if schema_validation_errors:
                    # Log schema validation errors at info level since they're normal with different DataHub versions
                    self.logger.info(f"GraphQL schema validation errors when creating policy: {schema_validation_errors}")
                    self.logger.info("These errors are normal when using different DataHub versions. Falling back to REST API.")
                else:
                    # Log other errors as warnings
                    self.logger.warning(f"GraphQL policy creation failed: {', '.join(error_messages)}, falling back to REST API")
            else:
                self.logger.warning("GraphQL policy creation failed with unknown error, falling back to REST API")
        except Exception as e:
            self.logger.warning(f"Error creating policy via GraphQL: {str(e)}")
        
        # Fall back to OpenAPI v3 endpoint
        try:
            url = f"{self.server_url}/openapi/v3/entity/datahubpolicy"
            self.logger.info(f"Attempting to create policy via OpenAPI v3: {url}")
            
            # Set up headers
            headers = self.headers.copy() if hasattr(self, 'headers') else {'Content-Type': 'application/json'}
            if hasattr(self, 'token') and self.token:
                headers['Authorization'] = f'Bearer {self.token}'
            
            # Format the request according to the OpenAPI v3 specification
            # Use explicit ID from original policy_data if available, otherwise fallback to name
            policy_id = policy_data.get("id") or policy_data.get("name", "").lower().replace(" ", "-")
            if not policy_id:
                policy_id = str(uuid.uuid4())
            
            # Create the nested structure required by OpenAPI v3
            request_body = [{
                "urn": f"urn:li:dataHubPolicy:{policy_id}",
                "dataHubPolicyKey": {
                    "value": {
                        "id": policy_id
                    },
                    "systemMetadata": {
                        "runId": "manual-creation",
                        "lastObserved": int(time.time() * 1000)
                    }
                },
                "dataHubPolicyInfo": {
                    "value": {
                        "displayName": policy_data.get("name", ""),
                        "description": policy_data.get("description", ""),
                        "type": policy_data.get("type", "METADATA"),
                        "state": policy_data.get("state", "ACTIVE"),
                        "privileges": policy_data.get("privileges", []),
                        "editable": True
                    },
                    "systemMetadata": {
                        "runId": "manual-creation",
                        "lastObserved": int(time.time() * 1000)
                    }
                }
            }]
            
            # Add resources if available
            if "resources" in policy_data:
                resources_data = {}
                
                if isinstance(policy_data["resources"], dict):
                    # Already in the correct format
                    resources_data = policy_data["resources"]
                elif isinstance(policy_data["resources"], list):
                    # Convert list to the expected format
                    if policy_data["resources"] and "type" in policy_data["resources"][0]:
                        resources_data["type"] = policy_data["resources"][0].get("type")
                    
                    if not policy_data["resources"] or policy_data["resources"][0].get("resource") == "*":
                        resources_data["allResources"] = True
                    elif "resource" in policy_data["resources"][0]:
                        resources_data["resources"] = [r.get("resource") for r in policy_data["resources"] if "resource" in r]
                
                request_body[0]["dataHubPolicyInfo"]["value"]["resources"] = resources_data
            
            # Add actors if available
            if "actors" in policy_data:
                request_body[0]["dataHubPolicyInfo"]["value"]["actors"] = policy_data["actors"]
            
            response = requests.post(url, headers=headers, json=request_body)
            # A 202 status is common for successfully accepted requests
            if response.status_code in (200, 201, 202):
                self.logger.info(f"Successfully created policy {policy_data.get('name')} via OpenAPI v3")
                
                # Return a simplified policy object that matches the GraphQL response format
                created_policy = {
                    **policy_data,
                    "urn": f"urn:li:dataHubPolicy:{policy_id}",
                    "id": policy_id
                }
                return created_policy
            else:
                # Check if the response indicates success despite non-200 status
                try:
                    resp_json = response.json()
                    # Some DataHub versions return a list of objects on success
                    if isinstance(resp_json, list) and len(resp_json) > 0 and "urn" in resp_json[0]:
                        self.logger.info(f"Policy created successfully despite status code {response.status_code}")
                        created_policy = {
                            **policy_data,
                            "urn": resp_json[0]["urn"],
                            "id": policy_id
                        }
                        return created_policy
                except:
                    pass
                
                self.logger.error(f"Failed to create policy via OpenAPI v3: {response.status_code} - {response.text}")
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
        if "resources" in graphql_input and isinstance(graphql_input["resources"], list):
            # Convert list format to the expected structure with filter.criteria
            resources_data = {
                "filter": {
                    "criteria": []
                }
            }
            
            # Add resource type information if available 
            if graphql_input["resources"] and "type" in graphql_input["resources"][0]:
                resources_data["type"] = graphql_input["resources"][0].get("type")
            
            # Add allResources if applicable
            if not graphql_input["resources"] or graphql_input["resources"][0].get("resource") == "*":
                resources_data["allResources"] = True
            elif "resource" in graphql_input["resources"][0]:
                resources_data["resources"] = [r.get("resource") for r in graphql_input["resources"] if "resource" in r]
            
            graphql_input["resources"] = resources_data
        
        # Ensure actors object has all required fields
        if "actors" in graphql_input:
            default_actors = {
                "users": [],
                "groups": [],
                "allUsers": False,
                "allGroups": False,
                "resourceOwners": False,
                "resourceOwnersTypes": None
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
                "resourceOwnersTypes": None
            }
        
        # Try GraphQL first
        graphql_mutation = """
        mutation updatePolicy($urn: String!, $input: PolicyUpdateInput!) {
          updatePolicy(urn: $urn, input: $input)
        }
        """
        
        variables = {
            "urn": policy_urn,
            "input": graphql_input
        }
        
        try:
            result = self.execute_graphql(graphql_mutation, variables)
            if result and "data" in result and result["data"] and "updatePolicy" in result["data"]:
                updated_policy_urn = result["data"]["updatePolicy"]
                if updated_policy_urn:
                    self.logger.info(f"Successfully updated policy {policy_id} via GraphQL")
                    
                    # Return policy data with URN
                    policy_with_urn = {**policy_data, "urn": updated_policy_urn}
                    return policy_with_urn
            
            # Check for errors
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                
                # Check for schema validation errors specifically
                schema_validation_errors = [
                    e for e in error_messages 
                    if "Unknown type 'PolicyUpdateInput'" in e or "Validation error" in e
                ]
                
                if schema_validation_errors:
                    # Log schema validation errors at info level since they're normal with different DataHub versions
                    self.logger.info(f"GraphQL schema validation errors when updating policy: {schema_validation_errors}")
                    self.logger.info("These errors are normal when using different DataHub versions. Falling back to REST API.")
                else:
                    # Log other errors as warnings
                    self.logger.warning(f"GraphQL policy update failed: {', '.join(error_messages)}, falling back to REST API")
            else:
                self.logger.warning("GraphQL policy update failed with unknown error, falling back to REST API")
        except Exception as e:
            self.logger.warning(f"Error updating policy via GraphQL: {str(e)}")
        
        # Fall back to OpenAPI v3 endpoint
        try:
            url = f"{self.server_url}/openapi/v3/entity/datahubpolicy"
            self.logger.debug(f"Updating policy via OpenAPI v3: PATCH {url}")
            
            # Set up headers
            headers = self.headers.copy() if hasattr(self, 'headers') else {'Content-Type': 'application/json'}
            if hasattr(self, 'token') and self.token:
                headers['Authorization'] = f'Bearer {self.token}'
                
            # Extract policy ID from URN if needed
            policy_id = policy_urn.split(":")[-1] if policy_urn else policy_id
            
            # Create the patch operations
            patch_operations = []
            
            # Add operations for each field in policy_data
            for field, value in policy_data.items():
                if field == "name":
                    patch_operations.append({
                        "op": "replace",
                        "path": "/displayName",
                        "value": value
                    })
                elif field == "description":
                    patch_operations.append({
                        "op": "replace",
                        "path": "/description",
                        "value": value
                    })
                elif field in ["type", "state", "privileges", "actors", "resources"]:
                    patch_operations.append({
                        "op": "replace",
                        "path": f"/{field}",
                        "value": value
                    })
            
            # Create the nested structure required by OpenAPI v3 for patching
            request_body = [{
                "urn": policy_urn,
                "dataHubPolicyInfo": {
                    "value": {
                        "patch": patch_operations,
                        "forceGenericPatch": True
                    },
                    "systemMetadata": {
                        "runId": "manual-update",
                        "lastObserved": int(time.time() * 1000)
                    }
                }
            }]
            
            response = requests.patch(url, headers=headers, json=request_body)
            
            if response.status_code in (200, 201, 204):
                self.logger.info(f"Successfully updated policy {policy_id} via OpenAPI v3")
                
                # Return the updated policy data
                updated_policy = {
                    **policy_data,
                    "urn": policy_urn,
                    "id": policy_id
                }
                return updated_policy
            
            self.logger.error(f"Failed to update policy via OpenAPI v3: {response.status_code} - {response.text}")
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
            if result and "data" in result and result["data"] and "deletePolicy" in result["data"]:
                if result["data"]["deletePolicy"] == True:
                    self.logger.info(f"Successfully deleted policy {policy_id} via GraphQL")
                    return True
            
            # Check for errors
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                
                # Check for schema validation errors specifically
                schema_validation_errors = [
                    e for e in error_messages 
                    if "Unknown type" in e or "Validation error" in e
                ]
                
                if schema_validation_errors:
                    # Log schema validation errors at info level since they're normal with different DataHub versions
                    self.logger.info(f"GraphQL schema validation errors when deleting policy: {schema_validation_errors}")
                    self.logger.info("These errors are normal when using different DataHub versions. Falling back to REST API.")
                else:
                    # Log other errors as warnings
                    self.logger.warning(f"GraphQL policy deletion failed: {', '.join(error_messages)}, falling back to REST API")
            else:
                self.logger.warning("GraphQL policy deletion failed with unknown error, falling back to REST API")
        except Exception as e:
            self.logger.warning(f"Error deleting policy via GraphQL: {str(e)}")
        
        # Fall back to OpenAPI v3 endpoint
        try:
            url = f"{self.server_url}/openapi/v3/entity/datahubpolicy/{policy_urn}"
            self.logger.info(f"Attempting to delete policy via OpenAPI v3: DELETE {url}")
            
            # Set up headers
            headers = self.headers.copy() if hasattr(self, 'headers') else {'Content-Type': 'application/json'}
            if hasattr(self, 'token') and self.token:
                headers['Authorization'] = f'Bearer {self.token}'
                
            response = requests.delete(url, headers=headers)
            
            if response.status_code in (200, 204):
                self.logger.info(f"Successfully deleted policy {policy_id} via OpenAPI v3")
                return True
            else:
                self.logger.error(f"Failed to delete policy via OpenAPI v3: {response.status_code} - {response.text}")
        except Exception as e:
            self.logger.error(f"Error deleting policy via OpenAPI v3: {str(e)}")
        
        return False

    # Tag Management Methods
    
    def list_tags(self, query="*", start=0, count=100) -> List[Dict[str, Any]]:
        """
        List tags in DataHub with optional filtering.
        
        Args:
            query (str): Search query to filter tags (default: "*")
            start (int): Starting offset for pagination
            count (int): Maximum number of tags to return
            
        Returns:
            List of tag objects
        """
        self.logger.info(f"Listing tags with query: {query}, start: {start}, count: {count}")
        self.logger.debug(f"Server URL: {self.server_url}, Token provided: {self.token is not None and len(self.token) > 0}, Verify SSL: {self.verify_ssl}")
        
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
                "types": ["TAG"],
                "query": query,
                "start": start,
                "count": count,
                "filters": []
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
                        tag = {
                            "urn": entity.get("urn"),
                            "name": entity.get("name"),
                            "description": entity.get("description"),
                        }
                        
                        # Add properties if available
                        if "properties" in entity and entity["properties"] is not None:
                            tag["properties"] = {
                                "name": entity["properties"].get("name"),
                                "colorHex": entity["properties"].get("colorHex")
                            }
                        
                        tags.append(tag)
                
                return tags
            
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.warning(f"GraphQL errors when listing tags: {', '.join(error_messages)}")
            
            return []
        except Exception as e:
            self.logger.error(f"Error listing tags: {str(e)}")
            return []
    
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
        
        variables = {
            "urn": tag_urn
        }
        
        try:
            result = self.execute_graphql(graphql_query, variables)
            
            if result and "data" in result and "tag" in result["data"]:
                return result["data"]["tag"]
            
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.warning(f"GraphQL errors when getting tag: {', '.join(error_messages)}")
            
            return None
        except Exception as e:
            self.logger.error(f"Error getting tag: {str(e)}")
            return None

    def create_tag(self, tag_id: str, name: str, description: str = "") -> Optional[str]:
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
        
        variables = {
            "input": {
                "id": tag_id,
                "name": name,
                "description": description
            }
        }
        
        try:
            result = self.execute_graphql(mutation, variables)
            
            if result and "data" in result and "createTag" in result["data"]:
                created_urn = result["data"]["createTag"]
                self.logger.info(f"Successfully created tag: {created_urn}")
                return created_urn
            
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.error(f"GraphQL errors when creating tag: {', '.join(error_messages)}")
            
            return None
        except Exception as e:
            self.logger.error(f"Error creating tag: {str(e)}")
            return None

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
        
        variables = {
            "urn": tag_urn,
            "colorHex": color_hex
        }
        
        try:
            result = self.execute_graphql(mutation, variables)
            
            if result and "data" in result and "setTagColor" in result["data"]:
                success = result["data"]["setTagColor"]
                if success:
                    self.logger.info(f"Successfully set color for tag {tag_urn}")
                    return True
            
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.error(f"GraphQL errors when setting tag color: {', '.join(error_messages)}")
            
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
        
        variables = {
            "input": {
                "resourceUrn": tag_urn,
                "description": description
            }
        }
        
        try:
            result = self.execute_graphql(mutation, variables)
            
            if result and "data" in result and "updateDescription" in result["data"]:
                success = result["data"]["updateDescription"]
                if success:
                    self.logger.info(f"Successfully updated description for tag {tag_urn}")
                    return True
            
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.error(f"GraphQL errors when updating tag description: {', '.join(error_messages)}")
            
            return False
        except Exception as e:
            self.logger.error(f"Error updating tag description: {str(e)}")
            return False

    def add_tag_owner(self, tag_urn: str, owner_urn: str, ownership_type: str = "urn:li:ownershipType:__system__business_owner") -> bool:
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
                        "ownershipTypeUrn": ownership_type
                    }
                ],
                "resources": [
                    {
                        "resourceUrn": tag_urn
                    }
                ]
            }
        }
        
        try:
            result = self.execute_graphql(mutation, variables)
            
            if result and "data" in result and "batchAddOwners" in result["data"]:
                success = result["data"]["batchAddOwners"]
                if success:
                    self.logger.info(f"Successfully added owner {owner_urn} to tag {tag_urn}")
                    return True
            
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.error(f"GraphQL errors when adding tag owner: {', '.join(error_messages)}")
            
            return False
        except Exception as e:
            self.logger.error(f"Error adding tag owner: {str(e)}")
            return False

    def remove_tag_owner(self, tag_urn: str, owner_urn: str, ownership_type: str = "urn:li:ownershipType:__system__business_owner") -> bool:
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
                "resourceUrn": tag_urn
            }
        }
        
        try:
            result = self.execute_graphql(mutation, variables)
            
            if result and "data" in result and "removeOwner" in result["data"]:
                success = result["data"]["removeOwner"]
                if success:
                    self.logger.info(f"Successfully removed owner {owner_urn} from tag {tag_urn}")
                    return True
            
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.error(f"GraphQL errors when removing tag owner: {', '.join(error_messages)}")
            
            return False
        except Exception as e:
            self.logger.error(f"Error removing tag owner: {str(e)}")
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
        
        variables = {
            "urn": tag_urn
        }
        
        try:
            result = self.execute_graphql(mutation, variables)
            
            if result and "data" in result and "deleteTag" in result["data"]:
                success = result["data"]["deleteTag"]
                if success:
                    self.logger.info(f"Successfully deleted tag {tag_urn}")
                    return True
            
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.error(f"GraphQL errors when deleting tag: {', '.join(error_messages)}")
            
            return False
        except Exception as e:
            self.logger.error(f"Error deleting tag: {str(e)}")
            return False

    def add_tag_to_entity(self, entity_urn: str, tag_urn: str, color_hex: str = None) -> bool:
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
            "input": {
                "resourceUrn": entity_urn,
                "tag": tag_urn,
                "colorHex": color_hex
            }
        }
        
        try:
            result = self.execute_graphql(mutation, variables)
            
            if result and "data" in result and "addTag" in result["data"]:
                success = result["data"]["addTag"]
                if success:
                    self.logger.info(f"Successfully added tag {tag_urn} to entity {entity_urn}")
                    return True
            
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.error(f"GraphQL errors when adding tag to entity: {', '.join(error_messages)}")
            
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
        
        variables = {
            "input": {
                "resourceUrn": entity_urn,
                "tagUrn": tag_urn
            }
        }
        
        try:
            result = self.execute_graphql(mutation, variables)
            
            if result and "data" in result and "removeTag" in result["data"]:
                success = result["data"]["removeTag"]
                if success:
                    self.logger.info(f"Successfully removed tag {tag_urn} from entity {entity_urn}")
                    return True
            
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.error(f"GraphQL errors when removing tag from entity: {', '.join(error_messages)}")
            
            return False
        except Exception as e:
            self.logger.error(f"Error removing tag from entity: {str(e)}")
            return False
            
    def find_entities_with_metadata(self, 
                                    field_type: str,
                                    metadata_urn: str, 
                                    start: int = 0, 
                                    count: int = 10) -> Dict[str, Any]:
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
            self.logger.error(f"Invalid field_type: {field_type}. Must be one of 'tags', 'glossaryTerms', or 'domains'")
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
                                "values": [
                                    metadata_urn
                                ],
                                "negated": False
                            }
                        ]
                    }
                ],
                "searchFlags": {
                    "getSuggestions": True,
                    "includeStructuredPropertyFacets": True
                }
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
                    "facets": search_results.get("facets", [])
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
                            simplified_entity.update({
                                "name": entity.get("name"),
                                "platform": entity.get("platform", {}).get("name") if entity.get("platform") else None,
                                "platformDisplay": (
                                    entity.get("platform", {})
                                    .get("properties", {})
                                    .get("displayName")
                                ) if entity.get("platform") and entity.get("platform", {}).get("properties") else None,
                                "description": entity.get("properties", {}).get("description") if entity.get("properties") else None,
                            })
                        elif entity.get("type") == "DASHBOARD":
                            simplified_entity.update({
                                "name": entity.get("properties", {}).get("name") if entity.get("properties") else None,
                                "description": entity.get("properties", {}).get("description") if entity.get("properties") else None,
                                "dashboardId": entity.get("dashboardId"),
                                "platform": entity.get("platform", {}).get("name") if entity.get("platform") else None,
                            })
                        elif entity.get("type") == "CHART":
                            simplified_entity.update({
                                "name": entity.get("properties", {}).get("name") if entity.get("properties") else None,
                                "description": entity.get("properties", {}).get("description") if entity.get("properties") else None,
                                "chartId": entity.get("chartId"),
                            })
                        elif entity.get("type") == "DATA_FLOW":
                            simplified_entity.update({
                                "name": entity.get("properties", {}).get("name") if entity.get("properties") else None,
                                "description": entity.get("properties", {}).get("description") if entity.get("properties") else None,
                                "flowId": entity.get("flowId"),
                            })
                        elif entity.get("type") == "DATA_JOB":
                            simplified_entity.update({
                                "name": entity.get("properties", {}).get("name") if entity.get("properties") else None,
                                "description": entity.get("properties", {}).get("description") if entity.get("properties") else None,
                                "jobId": entity.get("jobId"),
                            })
                        elif entity.get("type") == "GLOSSARY_TERM":
                            simplified_entity.update({
                                "name": entity.get("name"),
                                "description": entity.get("properties", {}).get("description") if entity.get("properties") else None,
                            })
                        elif entity.get("type") == "TAG":
                            simplified_entity.update({
                                "name": entity.get("name"),
                                "description": entity.get("description"),
                                "colorHex": entity.get("properties", {}).get("colorHex") if entity.get("properties") else None,
                            })
                        elif entity.get("type") == "CORP_USER":
                            simplified_entity.update({
                                "username": entity.get("username"),
                                "displayName": entity.get("properties", {}).get("displayName") if entity.get("properties") else None,
                                "email": entity.get("properties", {}).get("email") if entity.get("properties") else None,
                            })
                        elif entity.get("type") == "CORP_GROUP":
                            simplified_entity.update({
                                "name": entity.get("name"),
                                "displayName": entity.get("info", {}).get("displayName") if entity.get("info") else None,
                            })
                        elif entity.get("type") == "DATA_PRODUCT":
                            simplified_entity.update({
                                "name": entity.get("properties", {}).get("name") if entity.get("properties") else None,
                                "description": entity.get("properties", {}).get("description") if entity.get("properties") else None,
                            })
                        
                        simplified_results["entities"].append(simplified_entity)
                
                return simplified_results
            
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.warning(f"GraphQL errors when finding entities: {', '.join(error_messages)}")
            
            return {"entities": [], "total": 0}
        except Exception as e:
            self.logger.error(f"Error finding entities with {field_type} {metadata_urn}: {str(e)}")
            return {"entities": [], "total": 0}

    def set_tag_color(self, tag_urn, color_hex):
        """
        Set the color for a tag.
        
        Args:
            tag_urn (str): The URN of the tag
            color_hex (str): Hex color code (e.g., '#ff0000')
            
        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Setting color for tag {tag_urn} to {color_hex}")
        
        try:
            # Try GraphQL first
            mutation = """
            mutation updateTagProperties($input: UpdateTagPropertiesInput!) {
              updateTagProperties(input: $input)
            }
            """
            
            variables = {
                "input": {
                    "urn": tag_urn,
                    "colorHex": color_hex
                }
            }
            
            result = self._execute_graphql(mutation, variables)
            if result and result.get('updateTagProperties') is not None:
                return True
                
            # Fallback to REST API if needed
            if tag_urn:
                tag_id = tag_urn.split(':')[-1]
                endpoint = f"{self.server_url}/tags/{tag_id}"
                data = {"properties": {"colorHex": color_hex}}
                
                response = self._session.put(
                    endpoint,
                    json=data,
                    headers=self._get_auth_headers()
                )
                
                if response.status_code == 200:
                    return True
            
            return False
        except Exception as e:
            self.logger.error(f"Error setting tag color: {str(e)}")
            return False
            
    #
    # Glossary Node Methods
    #
            
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
        self.logger.info(f"Listing glossary nodes with query={query}, count={count}, start={start}")
        
        # Try using GraphQL search with GLOSSARY_NODE entity type
        try:
            self.logger.info("Trying GraphQL search with GLOSSARY_NODE entity type")
            
            # Search query that will return all glossary nodes
            search_query = """
            query($query: String!, $count: Int, $start: Int) {
              search(input: {
                type: GLOSSARY_NODE, 
                query: $query, 
                count: $count, 
                start: $start
              }) {
                start
                count
                total
                searchResults {
                  entity {
                    urn
                    type
                    ... on GlossaryNode {
                      properties {
                        name
                        description
                      }
                      parentNodes {
                        count
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
                "query": query or "*",
                "count": count,
                "start": start
            }
            
            self.logger.debug(f"Executing GraphQL search query with variables: {variables}")
            result = self.execute_graphql(search_query, variables)
            self.logger.debug(f"Search result: {result}")
            
            if result and 'data' in result and 'search' in result['data']:
                search_data = result['data']['search']
                total = search_data.get('total', 0)
                
                if total > 0:
                    search_results = search_data.get('searchResults', [])
                    
                    # Process results into standardized format
                    processed_nodes = []
                    for result in search_results:
                        entity = result.get('entity', {})
                        
                        # Extract basic properties
                        urn = entity.get('urn')
                        entity_type = entity.get('type')
                        
                        # Get properties if available
                        properties = entity.get('properties', {})
                        name = properties.get('name', 'Unknown')
                        description = properties.get('description', '')
                        
                        # Create processed node
                        processed_node = {
                            'urn': urn,
                            'type': entity_type,
                            'name': name,
                            'description': description,
                            'properties': properties
                        }
                        
                        # Add parent node info if available
                        parent_nodes = entity.get('parentNodes', {}).get('nodes', [])
                        if parent_nodes and len(parent_nodes) > 0:
                            parent = parent_nodes[0]  # Usually a node has one parent
                            processed_node['parentNode'] = {
                                'urn': parent.get('urn'),
                                'name': parent.get('properties', {}).get('name', 'Unknown Parent')
                            }
                            
                        processed_nodes.append(processed_node)
                    
                    self.logger.info(f"Successfully retrieved {len(processed_nodes)} glossary nodes via GraphQL search")
                    return processed_nodes
                else:
                    self.logger.info("No glossary nodes found in search results")
            else:
                errors = self._get_graphql_errors(result)
                if errors:
                    self.logger.warning(f"GraphQL search returned errors: {errors}")
        except Exception as e:
            self.logger.warning(f"Error using GraphQL search for glossary nodes: {str(e)}")
            
        # Try using getRootGlossaryNodes which requires an input parameter
        try:
            self.logger.info("Trying getRootGlossaryNodes GraphQL query with input parameter")
            
            # This query uses the correct format with input parameter including required fields
            gql_query = """
            query {
              getRootGlossaryNodes(input: {start: 0, count: 100}) {
                count
                start
                total
                nodes {
                  urn
                  type
                  properties {
                    name
                    description
                  }
                }
              }
            }
            """
            
            self.logger.debug(f"Executing GraphQL query: {gql_query}")
            result = self.execute_graphql(gql_query)
            self.logger.debug(f"getRootGlossaryNodes result: {result}")
            
            # Check if result contains data in the expected format
            if result and 'data' in result and 'getRootGlossaryNodes' in result['data']:
                nodes_data = result['data']['getRootGlossaryNodes']
                nodes = nodes_data.get('nodes', [])
                
                # Process nodes into a standardized format
                processed_nodes = []
                
                for node in nodes:
                    properties = node.get('properties', {})
                    
                    processed_node = {
                        'urn': node.get('urn'),
                        'name': properties.get('name', 'Unknown'),
                        'description': properties.get('description', ''),
                        'properties': properties
                    }
                    
                    processed_nodes.append(processed_node)
                
                self.logger.info(f"Successfully retrieved {len(processed_nodes)} glossary nodes via getRootGlossaryNodes")
                return processed_nodes
            else:
                errors = self._get_graphql_errors(result)
                if errors:
                    self.logger.warning(f"getRootGlossaryNodes query returned errors: {errors}")
        except Exception as e:
            self.logger.warning(f"Error using getRootGlossaryNodes GraphQL query: {str(e)}")
        
        # If all GraphQL approaches failed, try REST API approaches (unchanged)
        # ...
        
        # All approaches failed, return empty list
        self.logger.warning("All glossary node API approaches failed")
        return []
    
    def get_glossary_node(self, node_urn):
        """
        Get a specific glossary node and its children by URN.
        
        Args:
            node_urn (str): The URN of the glossary node to retrieve
            
        Returns:
            dict: Dictionary with node details and its children (both nodes and terms)
        """
        self.logger.info(f"Getting glossary node with URN: {node_urn}")
        
        try:
            # First, get the node details with a simpler query
            node_query = """
            query($urn: String!) {
              glossaryNode(urn: $urn) {
                urn
                type
                properties {
                  name
                  description
                }
              }
            }
            """
            
            variables = {"urn": node_urn}
            
            self.logger.debug(f"Executing GraphQL query for node details: {node_urn}")
            result = self.execute_graphql(node_query, variables)
            
            if not result or 'data' not in result or 'glossaryNode' not in result['data']:
                errors = self._get_graphql_errors(result)
                if errors:
                    self.logger.warning(f"GraphQL query for node {node_urn} returned errors: {errors}")
                return None
                
            node_data = result['data']['glossaryNode']
            
            # Process the node data
            properties = node_data.get('properties', {})
            processed_node = {
                'urn': node_data.get('urn'),
                'type': node_data.get('type'),
                'name': properties.get('name', 'Unknown'),
                'description': properties.get('description', ''),
                'properties': properties,
                'child_nodes': [],
                'child_terms': []
            }
            
            # Now get child nodes using searchAcrossEntities
            search_query = """
            query($input: SearchAcrossEntitiesInput!) {
              searchAcrossEntities(input: $input) {
                searchResults {
                  entity {
                    urn
                    type
                    ... on GlossaryNode {
                properties {
                  name
                  description
                }
                parentNodes {
                  nodes {
                    urn
                        }
                      }
                    }
                  }
                }
              }
            }
            """
            
            search_variables = {
                "input": {
                    "query": f"parentNodes.urn:\"{node_urn}\"",
                    "types": ["GLOSSARY_NODE"],
                    "count": 100,
                }
            }
            
            self.logger.debug(f"Searching for child nodes of {node_urn}")
            search_result = self.execute_graphql(search_query, search_variables)
            
            if search_result and 'data' in search_result and 'searchAcrossEntities' in search_result['data']:
                search_data = search_result['data']['searchAcrossEntities']
                if 'searchResults' in search_data:
                    for result in search_data['searchResults']:
                        entity = result.get('entity', {})
                        child_properties = entity.get('properties', {})
                        child_node = {
                            'urn': entity.get('urn'),
                            'type': entity.get('type'),
                            'name': child_properties.get('name', 'Unknown'),
                            'description': child_properties.get('description', ''),
                            'properties': child_properties,
                            'parent_node_urn': node_urn
                        }
                        processed_node['child_nodes'].append(child_node)
            
            # Get child terms using searchAcrossEntities for GLOSSARY_TERM type
            term_search_variables = {
                "input": {
                    "query": f"parentNodes.urn:\"{node_urn}\"",
                    "types": ["GLOSSARY_TERM"],
                    "count": 100,
                }
            }
            
            self.logger.debug(f"Searching for child terms of {node_urn}")
            term_search_result = self.execute_graphql(search_query, term_search_variables)
            
            if term_search_result and 'data' in term_search_result and 'searchAcrossEntities' in term_search_result['data']:
                search_data = term_search_result['data']['searchAcrossEntities']
                if 'searchResults' in search_data:
                    term_results = search_data['searchResults']
                    
                    if term_results and len(term_results) > 0:
                        self.logger.info(f"Found {len(term_results)} child terms for node {node_urn}")
                        for result in term_results:
                            entity = result.get('entity', {})
                            term_properties = entity.get('properties', {})
                            term_data = {
                                'urn': entity.get('urn'),
                                'type': entity.get('type'),
                                'name': term_properties.get('name', 'Unknown'),
                                'description': term_properties.get('description', ''),
                                'term_source': term_properties.get('termSource', 'INTERNAL'),
                                'properties': term_properties,
                                'parent_node_urn': node_urn
                            }
                            processed_node['child_terms'].append(term_data)
                    else:
                        self.logger.info(f"No child terms found in direct search for node {node_urn}")
            else:
                errors = self._get_graphql_errors(term_search_result)
                if errors:
                    self.logger.warning(f"GraphQL term search for node {node_urn} returned errors: {errors}")
                
            # If no terms found via direct search, try list_glossary_terms as fallback
            if not processed_node['child_terms']:
                self.logger.info(f"Trying list_glossary_terms as fallback for node {node_urn}")
                all_terms = self.list_glossary_terms()
                
                # All terms should be a dictionary with parent URNs as keys
                if isinstance(all_terms, dict) and node_urn in all_terms:
                    self.logger.info(f"Found {len(all_terms[node_urn])} terms for node {node_urn} via list_glossary_terms")
                    processed_node['child_terms'] = all_terms[node_urn]
                else:
                    self.logger.info(f"No child terms found for node {node_urn} in list_glossary_terms results")
                    # Try a direct search with node_urn parameter
                    node_terms = self.list_glossary_terms(node_urn=node_urn)
                    if node_terms and node_urn in node_terms:
                        self.logger.info(f"Found {len(node_terms[node_urn])} terms via direct list_glossary_terms search")
                        processed_node['child_terms'] = node_terms[node_urn]
            
            return processed_node
        except Exception as e:
            self.logger.error(f"Error getting glossary node {node_urn}: {str(e)}")
        
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
        
        try:
            # Use the newer GraphQL mutation
            mutation = """
            mutation createGlossaryNode($input: CreateGlossaryEntityInput!) {
              createGlossaryNode(input: $input)
            }
            """
            
            variables = {
                "input": {
                    "id": node_id,
                    "name": name,
                    "description": description
                }
            }
            
            if parent_urn:
                variables["input"]["parentNode"] = parent_urn
            
            result = self._execute_graphql(mutation, variables)
            if result and "createGlossaryNode" in result:
                return result["createGlossaryNode"]
                
            # Fallback to REST API if needed
            endpoint = f"{self.server_url}/glossary/nodes"
            
            data = {
                "id": node_id,
                "name": name,
                "description": description
            }
            
            if parent_urn:
                data["parentNode"] = parent_urn
                
            response = self._session.post(
                endpoint,
                json=data,
                headers=self._get_auth_headers()
            )
            
            if response.status_code == 201:
                return response.json().get("urn")
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error creating glossary node: {str(e)}")
            return None
    
    def update_glossary_node(self, node_urn, name=None, description=None, parent_urn=None):
        """
        Update an existing glossary node.
        
        Args:
            node_urn (str): URN of the node to update
            name (str, optional): New name for the node
            description (str, optional): New description for the node
            parent_urn (str, optional): New parent node URN
            
        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Updating glossary node: {node_urn}")
        
        try:
            # Try GraphQL first
            mutation = """
            mutation updateGlossaryNode($input: UpdateGlossaryNodeInput!) {
              updateGlossaryNode(input: $input)
            }
            """
            
            variables = {
                "input": {
                    "urn": node_urn
                }
            }
            
            if name is not None:
                variables["input"]["name"] = name
                
            if description is not None:
                variables["input"]["description"] = description
                
            if parent_urn is not None:
                variables["input"]["parentNode"] = parent_urn
                
            result = self._execute_graphql(mutation, variables)
            if result and result.get("updateGlossaryNode") is not None:
                return True
                
            # Fallback to REST API if needed
            if node_urn:
                node_id = node_urn.split(':')[-1]
                endpoint = f"{self.server_url}/glossary/nodes/{node_id}"
                
                data = {}
                if name is not None:
                    data["name"] = name
                if description is not None:
                    data["description"] = description
                if parent_urn is not None:
                    data["parentNode"] = parent_urn
                    
                response = self._session.put(
                    endpoint,
                    json=data,
                    headers=self._get_auth_headers()
                )
                
                if response.status_code == 200:
                    return True
            
            return False
        except Exception as e:
            self.logger.error(f"Error updating glossary node: {str(e)}")
            return False
    
    def delete_glossary_node(self, node_urn):
        """
        Delete a glossary node from DataHub.
        
        Args:
            node_urn (str): URN of the node to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Deleting glossary node: {node_urn}")
        
        try:
            # Try GraphQL first
            mutation = """
            mutation deleteGlossaryNode($input: DeleteGlossaryNodeInput!) {
              deleteGlossaryNode(input: $input)
            }
            """
            
            variables = {
                "input": {
                    "urn": node_urn
                }
            }
            
            result = self._execute_graphql(mutation, variables)
            if result and result.get("deleteGlossaryNode") is not None:
                return True
                
            # Fallback to REST API if needed
            if node_urn:
                node_id = node_urn.split(':')[-1]
                endpoint = f"{self.server_url}/glossary/nodes/{node_id}"
                
                response = self._session.delete(
                    endpoint,
                    headers=self._get_auth_headers()
                )
                
                if response.status_code == 204:
                    return True
            
            return False
        except Exception as e:
            self.logger.error(f"Error deleting glossary node: {str(e)}")
            return False
            
    #
    # Glossary Term Methods
    #
    
    def list_glossary_terms(self, node_urn=None, query=None, count=100, start=0):
        """
        List glossary terms, optionally filtered by parent node or search query.
        
        Args:
            node_urn (str, optional): Filter terms by parent node URN
            query (str, optional): Search query string to filter terms
            count (int, optional): Maximum number of terms to return
            start (int, optional): Offset for pagination
            
        Returns:
            dict: Dictionary mapping parent node URNs to their child terms
        """
        self.logger.info(f"Listing glossary terms with node_urn={node_urn}, query={query}, count={count}, start={start}")
        
        try:
            # Build a query for GLOSSARY_TERM entity search
            self.logger.info(f"Trying GraphQL search with GLOSSARY_TERM entity type")
            
            search_query = """
            query($input: SearchAcrossEntitiesInput!) {
              searchAcrossEntities(input: $input) {
                searchResults {
                  entity {
                    urn
                    type
                    ... on GlossaryTerm {
                properties {
                  name
                  description
                  termSource
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
            
            search_variables = {
                "input": {
                    "query": "*",
                    "types": ["GLOSSARY_TERM"],
                    "count": count,
                    "start": start
                }
            }
            
            # If we have a parent node filter, add it to the query
            if node_urn:
                search_variables["input"]["query"] = f"parentNodes.urn:\"{node_urn}\""
            elif query:
                search_variables["input"]["query"] = query
            
            self.logger.debug(f"Executing searchAcrossEntities with variables: {search_variables}")
            result = self.execute_graphql(search_query, search_variables)
            
            if result and 'data' in result and 'searchAcrossEntities' in result['data']:
                search_results = result['data']['searchAcrossEntities'].get('searchResults', [])
                
                if not search_results:
                    self.logger.info(f"No glossary terms found in search results")
                    return {} if node_urn else []
                
                # Group terms by parent node
                terms_by_parent = {}
                
                for result in search_results:
                    entity = result.get('entity', {})
                    if entity and 'properties' in entity:
                        term_urn = entity.get('urn')
                        term_properties = entity.get('properties', {})
                        
                        # Find parent node information
                        parent_nodes = entity.get('parentNodes', {}).get('nodes', [])
                        parent_urn = None
                        parent_name = None
                        
                        if parent_nodes and len(parent_nodes) > 0:
                            parent_urn = parent_nodes[0].get('urn')
                            parent_name = parent_nodes[0].get('properties', {}).get('name', 'Unknown')
                        
                        # Format the term data
                        term_data = {
                            'urn': term_urn,
                            'type': entity.get('type'),
                            'name': term_properties.get('name', 'Unknown'),
                            'description': term_properties.get('description', ''),
                            'term_source': term_properties.get('termSource', 'INTERNAL'),
                            'parent_node_urn': parent_urn,
                            'parent_node_name': parent_name,
                            'properties': term_properties
                        }
                        
                        # If no parent_urn is found but node_urn was specified, use it
                        if not parent_urn and node_urn:
                            parent_urn = node_urn
                            term_data['parent_node_urn'] = node_urn
                        
                        # Add term to the parent's list of terms
                        if parent_urn:
                            if parent_urn not in terms_by_parent:
                                terms_by_parent[parent_urn] = []
                            terms_by_parent[parent_urn].append(term_data)
                else:
                            # No parent node, add to a special "root" category
                            root_key = "root_terms"
                            if root_key not in terms_by_parent:
                                terms_by_parent[root_key] = []
                            terms_by_parent[root_key].append(term_data)
                
                if terms_by_parent:
                    self.logger.info(f"Successfully retrieved glossary terms for {len(terms_by_parent)} parent nodes")
                    return terms_by_parent
                elif not node_urn:
                    # If we weren't searching for a specific node, return an empty list
                    return []
            
            # If we get here, either there were no terms or there was an error
            errors = self._get_graphql_errors(result)
            if errors:
                self.logger.warning(f"GraphQL query for terms returned errors: {errors}")
                
            self.logger.info(f"No glossary terms found for node_urn={node_urn}")
            return {} if node_urn else []
                
        except Exception as e:
            self.logger.error(f"Error listing glossary terms: {str(e)}")
            return {} if node_urn else []
        
    def add_owners_to_glossary_term(self, term_urn, owner_urns, ownership_type="urn:li:ownershipType:__system__technical_owner"):
        """
        Add owners to a glossary term.
        
        Args:
            term_urn: The URN of the glossary term
            owner_urns: List of owner URNs to add
            ownership_type: The ownership type URN
            
        Returns:
            True if successful, False otherwise
        """
        if not isinstance(owner_urns, list):
            owner_urns = [owner_urns]
            
        mutation = """
        mutation batchAddOwners($input: BatchAddOwnersInput!) {
          batchAddOwners(input: $input)
        }
        """
        
        # Prepare the owners list
        owners = []
        for owner_urn in owner_urns:
            # Determine owner entity type from URN
            owner_entity_type = "CORP_USER"
            if ":corpGroup:" in owner_urn:
                owner_entity_type = "CORP_GROUP"
                
            owners.append({
                "ownerUrn": owner_urn,
                "ownerEntityType": owner_entity_type,
                "ownershipTypeUrn": ownership_type
            })
            
        variables = {
            "input": {
                "owners": owners,
                "resources": [
                    {
                        "resourceUrn": term_urn
                    }
                ]
            }
        }
        
        result = self._execute_graphql(mutation, variables)
        return result is not None and "batchAddOwners" in result
        
    def remove_owner_from_glossary_term(self, term_urn, owner_urn, ownership_type="urn:li:ownershipType:__system__technical_owner"):
        """
        Remove an owner from a glossary term.
        
        Args:
            term_urn: The URN of the glossary term
            owner_urn: The URN of the owner to remove
            ownership_type: The ownership type URN
            
        Returns:
            True if successful, False otherwise
        """
        mutation = """
        mutation removeOwner($input: RemoveOwnerInput!) {
          removeOwner(input: $input)
        }
        """
        
        variables = {
            "input": {
                "ownerUrn": owner_urn,
                "ownershipTypeUrn": ownership_type,
                "resourceUrn": term_urn
            }
        }
        
        result = self._execute_graphql(mutation, variables)
        return result is not None and "removeOwner" in result
        
    def set_domain_for_glossary_term(self, term_urn, domain_urn):
        """
        Set a domain for a glossary term.
        
        Args:
            term_urn: The URN of the glossary term
            domain_urn: The URN of the domain to set
            
        Returns:
            True if successful, False otherwise
        """
        mutation = """
        mutation batchSetDomain($input: BatchSetDomainInput!) {
          batchSetDomain(input: $input)
        }
        """
        
        variables = {
            "input": {
                "resources": [
                    {
                        "resourceUrn": term_urn
                    }
                ],
                "domainUrn": domain_urn
            }
        }
        
        result = self._execute_graphql(mutation, variables)
        return result is not None and "batchSetDomain" in result
        
    def unset_domain_for_glossary_term(self, term_urn):
        """
        Unset the domain for a glossary term.
        
        Args:
            term_urn: The URN of the glossary term
            
        Returns:
            True if successful, False otherwise
        """
        mutation = """
        mutation unsetDomain($entityUrn: String!) {
          unsetDomain(entityUrn: $entityUrn)
        }
        """
        
        variables = {
            "entityUrn": term_urn
        }
        
        result = self._execute_graphql(mutation, variables)
        return result is not None and "unsetDomain" in result
        
    def add_related_terms(self, term_urn, related_term_urns, relationship_type="hasA"):
        """
        Add related terms to a glossary term.
        
        Args:
            term_urn: The URN of the glossary term
            related_term_urns: List of related term URNs to add
            relationship_type: The relationship type ("hasA" for Contains or "isA" for Inherits)
            
        Returns:
            True if successful, False otherwise
        """
        if not isinstance(related_term_urns, list):
            related_term_urns = [related_term_urns]
            
        if relationship_type not in ["hasA", "isA"]:
            self.logger.warning(f"Invalid relationship type: {relationship_type}. Using 'hasA' as default.")
            relationship_type = "hasA"
            
        mutation = """
        mutation addRelatedTerms($input: RelatedTermsInput!) {
          addRelatedTerms(input: $input)
        }
        """
        
        variables = {
            "input": {
                "urn": term_urn,
                "termUrns": related_term_urns,
                "relationshipType": relationship_type
            }
        }
        
        result = self._execute_graphql(mutation, variables)
        return result is not None and "addRelatedTerms" in result
        
    def remove_related_terms(self, term_urn, related_term_urns, relationship_type="hasA"):
        """
        Remove related terms from a glossary term.
        
        Args:
            term_urn: The URN of the glossary term
            related_term_urns: List of related term URNs to remove
            relationship_type: The relationship type ("hasA" for Contains or "isA" for Inherits)
            
        Returns:
            True if successful, False otherwise
        """
        if not isinstance(related_term_urns, list):
            related_term_urns = [related_term_urns]
            
        if relationship_type not in ["hasA", "isA"]:
            self.logger.warning(f"Invalid relationship type: {relationship_type}. Using 'hasA' as default.")
            relationship_type = "hasA"
            
        mutation = """
        mutation removeRelatedTerms($input: RelatedTermsInput!) {
          removeRelatedTerms(input: $input)
        }
        """
        
        variables = {
            "input": {
                "urn": term_urn,
                "termUrns": related_term_urns,
                "relationshipType": relationship_type
            }
        }
        
        result = self._execute_graphql(mutation, variables)
        return result is not None and "removeRelatedTerms" in result
    
    #
    # Domain Methods
    #
    
    def list_domains(self, query=None, count=10, start=0):
        """
        List domains from DataHub.
        
        Args:
            query (str, optional): Search query to filter domains
            count (int, optional): Number of results to return
            start (int, optional): Offset for pagination
            
        Returns:
            list: List of domains
        """
        self.logger.info(f"Listing domains with query: {query}")
        
        try:
            # Try to use GraphQL to get the domains
            gql_query = """
            query listDomains($input: ListDomainsInput!) {
              listDomains(input: $input) {
                domains {
                  urn
                  properties {
                    name
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
            
            if query:
                variables["input"]["query"] = query

            # Add detailed logging for the GraphQL query and variables
            self.logger.debug(f"GraphQL Query for list_domains: {gql_query.strip()}")
            self.logger.debug(f"GraphQL Variables for list_domains: {variables}")
                
            result = self._execute_graphql(gql_query, variables)
            if result and "listDomains" in result:
                domains = result["listDomains"]["domains"]
                
                # Process the domains
                processed_domains = []
                for domain in domains:
                    domain_data = {
                        "urn": domain["urn"],
                        "name": domain.get("properties", {}).get("name", ""),
                        "description": domain.get("properties", {}).get("description", "")
                    }
                        
                    processed_domains.append(domain_data)
                    
                return processed_domains
                
            # Fallback to REST API
            endpoint = f"{self.server_url}/domains"
            params = {"count": count, "start": start}
            
            if query:
                params["query"] = query
                
            response = self._session.get(
                endpoint,
                params=params,
                headers=self._get_auth_headers()
            )
            
            if response.status_code == 200:
                return response.json()
                
            self.logger.warning(f"Failed to list domains: {response.status_code}")
            return []
            
        except Exception as e:
            self.logger.error(f"Error listing domains: {str(e)}")
            return []
    
    def get_domain(self, domain_urn):
        """
        Get details of a domain by URN.
        
        Args:
            domain_urn (str): URN of the domain
            
        Returns:
            dict: Domain details or None if not found
        """
        self.logger.info(f"Getting domain with URN: {domain_urn}")
        
        try:
            # Try GraphQL first
            gql_query = """
            query getDomain($urn: String!) {
              domain(urn: $urn) {
                urn
                properties {
                  name
                  description
                }
              }
            }
            """
            
            variables = {"urn": domain_urn}
            
            result = self._execute_graphql(gql_query, variables)
            if result and "domain" in result:
                domain = result["domain"]
                domain_data = {
                    "urn": domain["urn"],
                    "name": domain.get("properties", {}).get("name", ""),
                    "description": domain.get("properties", {}).get("description", "")
                }
                    
                return domain_data
                
            # Fallback to REST API if needed
            if domain_urn:
                domain_id = domain_urn.split(':')[-1]
                endpoint = f"{self.server_url}/domains/{domain_id}"
                
                response = self._session.get(
                    endpoint,
                    headers=self._get_auth_headers()
                )
                
                if response.status_code == 200:
                    return response.json()
            
            return None
        except Exception as e:
            self.logger.error(f"Error getting domain: {str(e)}")
            return None
    
    def create_domain(self, domain_id, name, description=""):
        """
        Create a new domain in DataHub.
        
        Args:
            domain_id (str): ID for the domain (will be used in the URN)
            name (str): Display name for the domain
            description (str, optional): Description for the domain
            
        Returns:
            str: URN of the created domain or None if failed
        """
        self.logger.info(f"Creating domain: {name}")
        
        try:
            # Try GraphQL first
            mutation = """
            mutation createDomain($input: CreateDomainInput!) {
              createDomain(input: $input)
            }
            """
            
            variables = {
                "input": {
                    "name": name,
                    "description": description
                }
            }
            
            result = self._execute_graphql(mutation, variables)
            if result and "data" in result and "createDomain" in result["data"]:
                return result["data"]["createDomain"]
                
            # Fallback to REST API if needed
            endpoint = f"{self.server_url}/domains"
            
            data = {
                "id": domain_id,
                "name": name,
                "description": description
            }
                
            response = self._session.post(
                endpoint,
                json=data,
                headers=self._get_auth_headers()
            )
            
            if response.status_code == 201:
                return response.json().get("urn")
            
            self.logger.warning(f"Failed to create domain: {response.status_code}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error creating domain: {str(e)}")
            return None
    
    def update_domain(self, domain_urn, name=None, description=None):
        """
        Update an existing domain.
        
        Args:
            domain_urn (str): URN of the domain to update
            name (str, optional): New name for the domain
            description (str, optional): New description for the domain
            
        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Updating domain: {domain_urn}")
        
        try:
            # Try GraphQL first
            mutation = """
            mutation updateDomain($input: UpdateDomainInput!) {
              updateDomain(input: $input)
            }
            """
            
            variables = {
                "input": {
                    "urn": domain_urn
                }
            }
            
            if name is not None:
                variables["input"]["name"] = name
                
            if description is not None:
                variables["input"]["description"] = description
                
            result = self._execute_graphql(mutation, variables)
            if result and result.get("updateDomain") is not None:
                return True
                
            # Fallback to REST API if needed
            if domain_urn:
                domain_id = domain_urn.split(':')[-1]
                endpoint = f"{self.server_url}/domains/{domain_id}"
                
                data = {}
                if name is not None:
                    data["name"] = name
                if description is not None:
                    data["description"] = description
                    
                response = self._session.put(
                    endpoint,
                    json=data,
                    headers=self._get_auth_headers()
                )
                
                if response.status_code == 200:
                    return True
            
            return False
        except Exception as e:
            self.logger.error(f"Error updating domain: {str(e)}")
            return False
    
    def delete_domain(self, domain_urn):
        """
        Delete a domain from DataHub.
        
        Args:
            domain_urn (str): URN of the domain to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Deleting domain: {domain_urn}")
        
        try:
            # Try GraphQL first
            mutation = """
            mutation deleteDomain($input: DeleteDomainInput!) {
              deleteDomain(input: $input)
            }
            """
            
            variables = {
                "input": {
                    "urn": domain_urn
                }
            }
            
            result = self._execute_graphql(mutation, variables)
            if result and result.get("deleteDomain") is not None:
                return True
                
            # Fallback to REST API if needed
            if domain_urn:
                domain_id = domain_urn.split(':')[-1]
                endpoint = f"{self.server_url}/domains/{domain_id}"
                
                response = self._session.delete(
                    endpoint,
                    headers=self._get_auth_headers()
                )
                
                if response.status_code == 204:
                    return True
            
            return False
        except Exception as e:
            self.logger.error(f"Error deleting domain: {str(e)}")
            return False
    
    def find_entities_with_domain(self, domain_urn, count=50, start=0):
        """
        Find entities associated with a specific domain.
        
        Args:
            domain_urn (str): Domain URN
            count (int, optional): Number of results to return
            start (int, optional): Offset for pagination
            
        Returns:
            dict: Dictionary with entities and total count
        """
        return self.find_entities_with_metadata(field_type="domains", metadata_urn=domain_urn, count=count, start=start)
    
    # 
    # Enhanced Glossary Methods
    #
    
    def update_glossary_entity_parent(self, resource_urn, parent_node_urn):
        """
        Move a glossary term or node to a new parent node.
        
        Args:
            resource_urn (str): URN of the glossary term or node to move
            parent_node_urn (str): URN of the new parent node
            
        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Moving glossary entity {resource_urn} to parent {parent_node_urn}")
        
        mutation = """
        mutation updateParentNode($input: UpdateParentNodeInput!) {
          updateParentNode(input: $input)
        }
        """
        
        variables = {
            "input": {
                "resourceUrn": resource_urn,
                "parentNode": parent_node_urn
            }
        }
        
        try:
            result = self.execute_graphql(mutation, variables)
            if result and "data" in result and "updateParentNode" in result["data"]:
                success = result["data"]["updateParentNode"]
                if success:
                    self.logger.info(f"Successfully moved glossary entity {resource_urn} to parent {parent_node_urn}")
                    return True
            
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.error(f"GraphQL errors when moving glossary entity: {', '.join(error_messages)}")
            
            return False
        except Exception as e:
            self.logger.error(f"Error moving glossary entity: {str(e)}")
            return False
    
    def create_glossary_node_v2(self, node_id, name, description="", parent_node_urn=None):
        """
        Create a new glossary node using the latest DataHub GraphQL schema.
        
        Args:
            node_id (str): ID for the node (will be used in the URN)
            name (str): Display name for the node
            description (str, optional): Description for the node
            parent_node_urn (str, optional): URN of the parent node
            
        Returns:
            str: URN of the created node or None if failed
        """
        self.logger.info(f"Creating glossary node: {name}")
        
        mutation = """
        mutation createGlossaryNode($input: CreateGlossaryEntityInput!) {
          createGlossaryNode(input: $input)
        }
        """
        
        variables = {
            "input": {
                "id": node_id,
                "name": name,
                "description": description
            }
        }
        
        if parent_node_urn:
            variables["input"]["parentNode"] = parent_node_urn
        
        try:
            result = self.execute_graphql(mutation, variables)
            if result and "data" in result and "createGlossaryNode" in result["data"]:
                created_urn = result["data"]["createGlossaryNode"]
                self.logger.info(f"Successfully created glossary node: {created_urn}")
                return created_urn
            
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.error(f"GraphQL errors when creating glossary node: {', '.join(error_messages)}")
            
            return None
        except Exception as e:
            self.logger.error(f"Error creating glossary node: {str(e)}")
            return None
    
    def delete_glossary_entity(self, entity_urn):
        """
        Delete a glossary entity (node or term) from DataHub.
        
        Args:
            entity_urn (str): URN of the glossary entity to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Deleting glossary entity: {entity_urn}")
        
        mutation = """
        mutation deleteGlossaryEntity($urn: String!) {
          deleteGlossaryEntity(urn: $urn)
        }
        """
        
        variables = {
            "urn": entity_urn
        }
        
        try:
            result = self.execute_graphql(mutation, variables)
            if result and "data" in result and "deleteGlossaryEntity" in result["data"]:
                success = result["data"]["deleteGlossaryEntity"]
                if success:
                    self.logger.info(f"Successfully deleted glossary entity {entity_urn}")
                    return True
            
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.error(f"GraphQL errors when deleting glossary entity: {', '.join(error_messages)}")
            
            return False
        except Exception as e:
            self.logger.error(f"Error deleting glossary entity: {str(e)}")
            return False
    
    def update_glossary_entity_description(self, resource_urn, description):
        """
        Update the description of a glossary entity.
        
        Args:
            resource_urn (str): URN of the glossary entity (node or term)
            description (str): New description
            
        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Updating description for glossary entity {resource_urn}")
        
        mutation = """
        mutation updateDescription($input: DescriptionUpdateInput!) {
          updateDescription(input: $input)
        }
        """
        
        variables = {
            "input": {
                "description": description,
                "resourceUrn": resource_urn
            }
        }
        
        try:
            result = self.execute_graphql(mutation, variables)
            if result and "data" in result and "updateDescription" in result["data"]:
                success = result["data"]["updateDescription"]
                if success:
                    self.logger.info(f"Successfully updated description for glossary entity {resource_urn}")
                    return True
            
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.error(f"GraphQL errors when updating glossary entity description: {', '.join(error_messages)}")
            
            return False
        except Exception as e:
            self.logger.error(f"Error updating glossary entity description: {str(e)}")
            return False
    
    def add_owners_to_glossary_entity(self, resource_urn, owner_urns, owner_type="CORP_USER", ownership_type="urn:li:ownershipType:__system__technical_owner"):
        """
        Add owners to a glossary entity (node or term).
        
        Args:
            resource_urn (str): URN of the glossary entity
            owner_urns (list or str): Owner URN(s) to add
            owner_type (str): Owner type (CORP_USER or CORP_GROUP)
            ownership_type (str): Ownership type URN
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not isinstance(owner_urns, list):
            owner_urns = [owner_urns]
            
        self.logger.info(f"Adding owners to glossary entity {resource_urn}")
        
        mutation = """
        mutation batchAddOwners($input: BatchAddOwnersInput!) {
          batchAddOwners(input: $input)
        }
        """
        
        # Prepare the owners list
        owners = []
        for owner_urn in owner_urns:
            # Determine owner entity type from URN if not specified
            entity_type = owner_type
            if owner_type is None or owner_type == "auto":
                if ":corpuser:" in owner_urn:
                    entity_type = "CORP_USER"
                elif ":corpGroup:" in owner_urn:
                    entity_type = "CORP_GROUP"
                else:
                    entity_type = "CORP_USER"  # Default
                
            owners.append({
                "ownerUrn": owner_urn,
                "ownerEntityType": entity_type,
                "ownershipTypeUrn": ownership_type
            })
            
        variables = {
            "input": {
                "owners": owners,
                "resources": [
                    {
                        "resourceUrn": resource_urn
                    }
                ]
            }
        }
        
        try:
            result = self.execute_graphql(mutation, variables)
            if result and "data" in result and "batchAddOwners" in result["data"]:
                success = result["data"]["batchAddOwners"]
                if success:
                    self.logger.info(f"Successfully added owners to glossary entity {resource_urn}")
                    return True
            
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.error(f"GraphQL errors when adding owners to glossary entity: {', '.join(error_messages)}")
            
            return False
        except Exception as e:
            self.logger.error(f"Error adding owners to glossary entity: {str(e)}")
            return False
    
    def remove_owner_from_glossary_entity(self, resource_urn, owner_urn, ownership_type="urn:li:ownershipType:__system__technical_owner"):
        """
        Remove an owner from a glossary entity (node or term).
        
        Args:
            resource_urn (str): URN of the glossary entity
            owner_urn (str): Owner URN to remove
            ownership_type (str): Ownership type URN
            
        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Removing owner {owner_urn} from glossary entity {resource_urn}")
        
        mutation = """
        mutation removeOwner($input: RemoveOwnerInput!) {
          removeOwner(input: $input)
        }
        """
        
        variables = {
            "input": {
                "ownerUrn": owner_urn,
                "ownershipTypeUrn": ownership_type,
                "resourceUrn": resource_urn
            }
        }
        
        try:
            result = self.execute_graphql(mutation, variables)
            return result.get("removeOwner", False)
        except Exception as e:
            self.logger.error(f"Error removing owner from glossary entity: {str(e)}")
            return False

    def get_editable_entities(self, start=0, count=20, query="*", entity_type=None, platform=None, use_platform_pagination=False, sort_by="name", editable_only=True):
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
            
        Returns:
            Dictionary with search results
        """
        variables = {
            "input": {
                "query": query,
                "start": start,
                "count": count,
                "orFilters": []
            }
        }
        
        if entity_type:
            variables["input"]["types"] = [entity_type]
            
        if platform:
            variables["input"]["filters"] = [{
                "field": "platform",
                "value": platform
            }]
            
        # Set sorting based on sort_by parameter
        # Note: sortCriteria is not supported in SearchAcrossEntitiesInput  
        # DataHub uses its default relevance-based sorting
        if sort_by == "updated":
            # sortCriteria not supported in SearchAcrossEntitiesInput
            pass
        elif sort_by == "type":
            # sortCriteria not supported in SearchAcrossEntitiesInput
            pass
        else:  # Default: sort by name
            # sortCriteria not supported in SearchAcrossEntitiesInput
            pass
            
        # Use platform pagination if specified (for comprehensive search)
        if use_platform_pagination and platform:
            variables["input"]["orFilters"] = [{
                "and": [{
                    "field": "platform",
                    "value": platform
                }]
            }]

        # Comprehensive query that gets all necessary metadata including editableProperties and browsePathV2
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
                      platform { 
                        name
                        properties { displayName }
                      }
                      dataPlatformInstance {
                        instanceId
                        platform { name }
                      }
                      browsePaths { path }
                      browsePathV2 {
                        path {
                          name
                          entity {
                            ... on Container {
                              container {
                                urn
                                properties { name }
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
                              associatedUrn
                              tag { urn }
                            }
                          }
                        }
                      }
                    }
                    ... on Container {
                      properties { name }
                      platform { 
                        name
                        properties { displayName }
                      }
                      dataPlatformInstance {
                        instanceId
                        platform { name }
                      }
                      browsePathV2 {
                        path {
                          name
                          entity {
                            ... on Container {
                              container {
                                urn
                                properties { name }
                              }
                            }
                          }
                        }
                      }
                      editableProperties {
                        description
                      }
                    }
                    ... on Chart {
                      properties { name }
                      platform { 
                        name
                        properties { displayName }
                      }
                      dataPlatformInstance {
                        instanceId
                        platform { name }
                      }
                      browsePaths { path }
                      browsePathV2 {
                        path {
                          name
                          entity {
                            ... on Container {
                              container {
                                urn
                                properties { name }
                              }
                            }
                          }
                        }
                      }
                      editableProperties {
                        description
                      }
                    }
                    ... on Dashboard {
                      properties { name }
                      platform { 
                        name
                        properties { displayName }
                      }
                      dataPlatformInstance {
                        instanceId
                        platform { name }
                      }
                      browsePaths { path }
                      browsePathV2 {
                        path {
                          name
                          entity {
                            ... on Container {
                              container {
                                urn
                                properties { name }
                              }
                            }
                          }
                        }
                      }
                      editableProperties {
                        description
                      }
                    }
                    ... on DataFlow {
                      properties { name }
                      platform { 
                        name
                        properties { displayName }
                      }
                      dataPlatformInstance {
                        instanceId
                        platform { name }
                      }
                      browsePaths { path }
                      browsePathV2 {
                        path {
                          name
                          entity {
                            ... on Container {
                              container {
                                urn
                                properties { name }
                              }
                            }
                          }
                        }
                      }
                      editableProperties {
                        description
                      }
                    }
                    ... on DataJob {
                      properties { name }
                      dataFlow { 
                        flowId
                        properties { name }
                      }
                      dataPlatformInstance {
                        instanceId
                        platform { name }
                      }
                      browsePaths { path }
                      browsePathV2 {
                        path {
                          name
                          entity {
                            ... on Container {
                              container {
                                urn
                                properties { name }
                              }
                            }
                          }
                        }
                      }
                      editableProperties {
                        description
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
        
        # If editable_only is True, filter results to only include entities with 
        # editableProperties or editableSchemaMetadata
        if editable_only and "searchResults" in search_results:
            filtered_results = []
            total_count = 0
            
            for result in search_results.get("searchResults", []):
                entity = result.get("entity", {})
                
                # Check if entity has editable properties or schema metadata
                has_editable_properties = entity.get("editableProperties") is not None
                has_editable_schema = entity.get("editableSchemaMetadata") is not None
                
                if has_editable_properties or has_editable_schema:
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

            
        except Exception as e:
            self.logger.error(f"Error listing glossary terms with searchAcrossEntities: {str(e)}")
            return self.list_glossary_terms(node_urn, query, count, start)

    def list_tests(self, start=0, count=100):
        """
        List metadata tests from DataHub using GraphQL API.
        
        Args:
            start (int, optional): Offset for pagination
            count (int, optional): Number of results to return
            
        Returns:
            list: List of metadata tests
        """
        self.logger.info(f"Listing metadata tests with start={start}, count={count}")
        
        try:
            # Define GraphQL query for listing tests
            gql_query = """
            query listTests($input: ListTestsInput!) {
              listTests(input: $input) {
                start
                count
                total
                tests {
                  urn
                  name
                  category
                  description
                  definition {
                    json
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
                    "start": start,
                    "count": count
                }
            }
            
            # Execute the GraphQL query
            result = self._execute_graphql(gql_query, variables)
            
            if result and 'listTests' in result and 'tests' in result['listTests']:
                return result['listTests']['tests']
            
            self.logger.warning("Failed to list metadata tests")
            return []
            
        except Exception as e:
            self.logger.error(f"Error listing metadata tests: {str(e)}")
            return []
    
    def get_test(self, test_urn):
        """
        Get details of a metadata test by URN.
        
        Args:
            test_urn (str): URN of the metadata test
            
        Returns:
            dict: Test details or None if not found
        """
        self.logger.info(f"Getting metadata test with URN: {test_urn}")
        
        try:
            # Define GraphQL query for getting a test by URN
            gql_query = """
            query test($urn: String!) {
              test(urn: $urn) {
                urn
                name
                category
                description
                definition {
                  json
                  __typename
                }
                __typename
              }
            }
            """
            
            variables = {"urn": test_urn}
            
            # Execute the GraphQL query
            result = self._execute_graphql(gql_query, variables)
            
            if result and 'test' in result:
                return result['test']
            
            self.logger.warning(f"Failed to get metadata test with URN {test_urn}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting metadata test: {str(e)}")
            return None
    
    def create_test(self, name, description, category, definition_json):
        """
        Create a metadata test in DataHub.
        
        Args:
            name (str): Name of the test
            description (str): Description of the test
            category (str): Category of the test (e.g., "Data Governance")
            definition_json (str): JSON string of the test definition
            
        Returns:
            str: URN of the created test or None if failed
        """
        self.logger.info(f"Creating metadata test: {name}")
        
        try:
            # Define GraphQL mutation for creating a test
            gql_mutation = """
            mutation createTest($input: CreateTestInput!) {
              createTest(input: $input)
            }
            """
            
            input_data = {
                "name": name,
                "description": description,
                "category": category,
                "definition": {
                    "json": definition_json
                }
            }
            
            variables = {"input": input_data}
            
            # Execute the GraphQL mutation
            result = self._execute_graphql(gql_mutation, variables)
            
            if result and 'createTest' in result:
                return result['createTest']
            
            self.logger.warning(f"Failed to create metadata test: {name}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error creating metadata test: {str(e)}")
            return None
    
    def update_test(self, test_urn, name=None, description=None, category=None, definition_json=None):
        """
        Update a metadata test in DataHub.
        
        Args:
            test_urn (str): URN of the test to update
            name (str, optional): New name for the test
            description (str, optional): New description for the test
            category (str, optional): New category for the test
            definition_json (str, optional): New JSON string of the test definition
            
        Returns:
            bool: True if update was successful, False otherwise
        """
        self.logger.info(f"Updating metadata test with URN: {test_urn}")
        
        try:
            # Prepare update input
            update_input = {}
            
            if name is not None:
                update_input["name"] = name
                
            if description is not None:
                update_input["description"] = description
                
            if category is not None:
                update_input["category"] = category
                
            if definition_json is not None:
                update_input["definition"] = {"json": definition_json}
                
            if not update_input:
                self.logger.warning("No fields to update for metadata test")
                return False
                
            # Define GraphQL mutation for updating a test
            gql_mutation = """
            mutation updateTest($urn: String!, $input: UpdateTestInput!) {
              updateTest(urn: $urn, input: $input)
            }
            """
            
            variables = {
                "urn": test_urn,
                "input": update_input
            }
            
            # Execute the GraphQL mutation
            result = self._execute_graphql(gql_mutation, variables)
            
            if result and 'updateTest' in result:
                return True
            
            self.logger.warning(f"Failed to update metadata test with URN {test_urn}")
            return False
            
        except Exception as e:
            self.logger.error(f"Error updating metadata test: {str(e)}")
            return False
    
    def delete_test(self, test_urn):
        """
        Delete a metadata test from DataHub.
        
        Args:
            test_urn (str): URN of the test to delete
            
        Returns:
            bool: True if deletion was successful, False otherwise
        """
        self.logger.info(f"Deleting metadata test with URN: {test_urn}")
        
        try:
            # Define GraphQL mutation for deleting a test
            gql_mutation = """
            mutation deleteTest($urn: String!) {
              deleteTest(urn: $urn)
            }
            """
            
            variables = {"urn": test_urn}
            
            # Execute the GraphQL mutation
            result = self._execute_graphql(gql_mutation, variables)
            
            if result and 'deleteTest' in result:
                return True
            
            self.logger.warning(f"Failed to delete metadata test with URN {test_urn}")
            return False
            
        except Exception as e:
            self.logger.error(f"Error deleting metadata test: {str(e)}")
            return False

    def test_glossary_connection(self):
        """
        Test connection to glossary endpoints to validate API connectivity.
        
        Returns:
            dict: Dictionary with test results for different glossary endpoints
        """
        self.logger.info("Testing glossary connection to different endpoints")
        
        results = {
            "business_glossary_api": False,
            "entity_search_api": False,
            "v2_glossary_api": False,
            "legacy_glossary_api": False,
            "detailed_results": {}
        }
        
        # Test business glossary API
        try:
            endpoint = f"{self.server_url}/api/metadata/business-glossary/nodes"
            self.logger.info(f"Testing business glossary API: {endpoint}")
            
            response = self._session.get(
                endpoint,
                params={"count": 1},
                headers=self._get_auth_headers()
            )
            
            results["detailed_results"]["business_glossary_api"] = {
                "status_code": response.status_code,
                "response_preview": str(response.text)[:100] if response.text else None
            }
            
            if response.status_code == 200:
                results["business_glossary_api"] = True
                self.logger.info("Business glossary API test: SUCCESS")
            else:
                self.logger.warning(f"Business glossary API test: FAILED with status {response.status_code}")
        except Exception as e:
            self.logger.error(f"Business glossary API test: ERROR - {str(e)}")
            results["detailed_results"]["business_glossary_api"] = {"error": str(e)}
        
        # Test entity search API
        try:
            endpoint = f"{self.server_url}/api/v2/search"
            self.logger.info(f"Testing entity search API: {endpoint}")
            
            response = self._session.get(
                endpoint,
                params={"entity": "glossaryNode", "count": 1},
                headers=self._get_auth_headers()
            )
            
            results["detailed_results"]["entity_search_api"] = {
                "status_code": response.status_code,
                "response_preview": str(response.text)[:100] if response.text else None
            }
            
            if response.status_code == 200:
                results["entity_search_api"] = True
                self.logger.info("Entity search API test: SUCCESS")
            else:
                self.logger.warning(f"Entity search API test: FAILED with status {response.status_code}")
        except Exception as e:
            self.logger.error(f"Entity search API test: ERROR - {str(e)}")
            results["detailed_results"]["entity_search_api"] = {"error": str(e)}
        
        # Test V2 glossary API
        try:
            endpoint = f"{self.server_url}/api/v2/glossary/nodes"
            self.logger.info(f"Testing V2 glossary API: {endpoint}")
            
            response = self._session.get(
                endpoint,
                params={"count": 1},
                headers=self._get_auth_headers()
            )
            
            results["detailed_results"]["v2_glossary_api"] = {
                "status_code": response.status_code,
                "response_preview": str(response.text)[:100] if response.text else None
            }
            
            if response.status_code == 200:
                results["v2_glossary_api"] = True
                self.logger.info("V2 glossary API test: SUCCESS")
            else:
                self.logger.warning(f"V2 glossary API test: FAILED with status {response.status_code}")
        except Exception as e:
            self.logger.error(f"V2 glossary API test: ERROR - {str(e)}")
            results["detailed_results"]["v2_glossary_api"] = {"error": str(e)}
        
        # Test legacy glossary API
        try:
            endpoint = f"{self.server_url}/glossary/nodes"
            self.logger.info(f"Testing legacy glossary API: {endpoint}")
            
            response = self._session.get(
                endpoint,
                params={"count": 1},
                headers=self._get_auth_headers()
            )
            
            results["detailed_results"]["legacy_glossary_api"] = {
                "status_code": response.status_code,
                "response_preview": str(response.text)[:100] if response.text else None
            }
            
            if response.status_code == 200:
                results["legacy_glossary_api"] = True
                self.logger.info("Legacy glossary API test: SUCCESS")
            else:
                self.logger.warning(f"Legacy glossary API test: FAILED with status {response.status_code}")
        except Exception as e:
            self.logger.error(f"Legacy glossary API test: ERROR - {str(e)}")
            results["detailed_results"]["legacy_glossary_api"] = {"error": str(e)}
        
        # Test GraphQL query for glossary
        try:
            self.logger.info("Testing GraphQL query for glossary nodes")
            
            # Basic GraphQL query to get glossary nodes
            query = """
            query {
              __schema {
                queryType {
                  fields {
                    name
                    description
                  }
                }
              }
            }
            """
            
            result = self.execute_graphql(query)
            
            results["detailed_results"]["graphql_schema"] = {
                "has_result": bool(result),
                "result_preview": str(result)[:200] if result else None
            }
            
            self.logger.info(f"GraphQL schema query result: {result is not None}")
            
            # Look for glossary-related fields in the schema
            if result and 'data' in result and '__schema' in result['data']:
                query_fields = result['data']['__schema']['queryType']['fields']
                glossary_fields = [field for field in query_fields if 'glossary' in field['name'].lower()]
                
                results["detailed_results"]["graphql_glossary_fields"] = glossary_fields
                self.logger.info(f"Found {len(glossary_fields)} glossary-related GraphQL fields: {glossary_fields}")
        except Exception as e:
            self.logger.error(f"GraphQL schema test: ERROR - {str(e)}")
            results["detailed_results"]["graphql_schema"] = {"error": str(e)}
        
        return results

    def _get_graphql_errors(self, result):
        """
        Extract and format GraphQL errors from a result object
        
        Args:
            result (dict): The GraphQL result
            
        Returns:
            str: Formatted error message or None if no errors
        """
        errors = []
        
        if not result:
            return None
            
        # Add debug logging
        self.logger.debug(f"GraphQL result structure: {result.keys() if isinstance(result, dict) else type(result)}")
            
        # Check for errors directly in the result
        if isinstance(result, dict) and "errors" in result:
            errors.extend(result["errors"])
        
        # Check for nested errors in the data field
        if isinstance(result, dict) and "data" in result and isinstance(result["data"], dict):
            for key, value in result["data"].items():
                if isinstance(value, dict) and "errors" in value:
                    for error in value["errors"]:
                        errors.append({"message": f"Error in {key}: {error['message']}"})
        
        if errors:
            error_messages = []
            for error in errors:
                if isinstance(error, dict) and "message" in error:
                    error_messages.append(error)
                elif isinstance(error, str):
                    error_messages.append({"message": error})
                else:
                    error_messages.append({"message": str(error)})
            
            formatted_errors = f"Error executing graphql query: {error_messages}"
            return formatted_errors
        
        return None

    def list_structured_properties(self, start=0, count=100) -> List[Dict[str, Any]]:
        """
        List all structured properties in DataHub.
        
        Args:
            start: The offset to start from
            count: The number of properties to return
            
        Returns:
            List of structured property objects
        """
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
                "types": ["STRUCTURED_PROPERTY"],
                "query": "*",
                "start": start,
                "count": count
            }
        }
        
        result = self.execute_graphql(query, variables)
        
        if not result or "errors" in result or "data" not in result:
            self._handle_error(result, "Error listing structured properties")
            return []
        
        search_results = result.get("data", {}).get("searchAcrossEntities", {}).get("searchResults", [])
        props = [result.get("entity") for result in search_results if result.get("entity", {}).get("type") == "STRUCTURED_PROPERTY"]
        
        return props
    
    def get_structured_property(self, property_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a structured property by URN.
        
        Args:
            property_urn: The URN of the structured property
            
        Returns:
            The structured property object if found, None otherwise
        """
        query = """
        query getStructuredProperty($urn: String!) {
          structuredPropertyEntity(urn: $urn) {
            urn
            type
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
        }
        """
        
        variables = {
            "urn": property_urn
        }
        
        result = self.execute_graphql(query, variables)
        
        if not result or "errors" in result or "data" not in result:
            self._handle_error(result, f"Error fetching structured property {property_urn}")
            return None
        
        return result.get("data", {}).get("structuredPropertyEntity")
    
    def create_structured_property(self, 
                                 display_name: str,
                                 description: str = "",
                                 value_type: str = "STRING",
                                 entity_types: List[str] = None,
                                 cardinality: str = "SINGLE",
                                 allowed_values: List[Dict[str, Any]] = None,
                                 immutable: bool = False,
                                 show_in_search: bool = True,
                                 show_as_badge: bool = True,
                                 show_in_summary: bool = True,
                                 qualified_name: Optional[str] = None) -> Optional[str]:
        """
        Create a new structured property in DataHub.
        
        Args:
            display_name: The display name of the property
            description: The description of the property
            value_type: The type of value (STRING, NUMBER, BOOLEAN, etc.)
            entity_types: List of entity types this property applies to (DATASET, DASHBOARD, etc.)
            cardinality: SINGLE or MULTIPLE
            allowed_values: List of allowed values with their descriptions
            immutable: Whether the property is immutable once set
            show_in_search: Whether to show in search filters
            show_as_badge: Whether to show as asset badge
            show_in_summary: Whether to show in asset summary
            qualified_name: The qualified name (will be generated from display_name if not provided)
            
        Returns:
            The URN of the created property if successful, None otherwise
        """
        if not qualified_name:
            # Generate qualified name from display name
            qualified_name = display_name.lower().replace(" ", "_")
        
        if not entity_types:
            entity_types = ["DATASET", "DASHBOARD", "CHART", "DATA_FLOW", "DATA_JOB"]
        
        mutation = """
        mutation createStructuredProperty($input: CreateStructuredPropertyEntityInput!) {
          createStructuredPropertyEntity(input: $input) {
            urn
          }
        }
        """
        
        # Prepare entity types input
        entity_type_inputs = []
        for entity_type in entity_types:
            entity_type_inputs.append({
                "type": entity_type
            })
        
        # Prepare allowed values input if provided
        allowed_value_inputs = []
        if allowed_values:
            for value_item in allowed_values:
                value = value_item.get("value")
                desc = value_item.get("description", "")
                
                value_input = None
                if value_type == "STRING":
                    value_input = {"string": value}
                elif value_type == "NUMBER":
                    value_input = {"number": float(value)}
                elif value_type == "BOOLEAN":
                    value_input = {"boolean": value.lower() == "true"}
                
                if value_input:
                    allowed_value_inputs.append({
                        "value": value_input,
                        "description": desc
                    })
        
        variables = {
            "input": {
                "definition": {
                    "displayName": display_name,
                    "qualifiedName": qualified_name,
                    "description": description,
                    "cardinality": cardinality,
                    "immutable": immutable,
                    "valueType": {
                        "type": value_type
                    },
                    "entityTypes": entity_type_inputs
                },
                "settings": {
                    "showInSearchFilters": show_in_search,
                    "showAsAssetBadge": show_as_badge,
                    "showInAssetSummary": show_in_summary
                }
            }
        }
        
        # Add allowed values if provided
        if allowed_value_inputs:
            variables["input"]["definition"]["allowedValues"] = allowed_value_inputs
        
        result = self.execute_graphql(mutation, variables)
        
        if not result or "errors" in result or "data" not in result:
            self._handle_error(result, f"Error creating structured property {display_name}")
            return None
        
        return result.get("data", {}).get("createStructuredPropertyEntity", {}).get("urn")
    
    def update_structured_property(self, 
                                 property_urn: str,
                                 display_name: Optional[str] = None,
                                 description: Optional[str] = None,
                                 show_in_search: Optional[bool] = None,
                                 show_as_badge: Optional[bool] = None,
                                 show_in_summary: Optional[bool] = None) -> bool:
        """
        Update a structured property in DataHub.
        
        Args:
            property_urn: The URN of the property to update
            display_name: New display name (optional)
            description: New description (optional)
            show_in_search: Whether to show in search filters (optional)
            show_as_badge: Whether to show as asset badge (optional)
            show_in_summary: Whether to show in asset summary (optional)
            
        Returns:
            True if successful, False otherwise
        """
        mutation = """
        mutation updateStructuredProperty($urn: String!, $input: UpdateStructuredPropertyEntityInput!) {
          updateStructuredPropertyEntity(urn: $urn, input: $input) {
            urn
          }
        }
        """
        
        update_input = {}
        
        # Add definition updates if provided
        definition_updates = {}
        if display_name is not None:
            definition_updates["displayName"] = display_name
        if description is not None:
            definition_updates["description"] = description
        
        if definition_updates:
            update_input["definition"] = definition_updates
        
        # Add settings updates if provided
        settings_updates = {}
        if show_in_search is not None:
            settings_updates["showInSearchFilters"] = show_in_search
        if show_as_badge is not None:
            settings_updates["showAsAssetBadge"] = show_as_badge
        if show_in_summary is not None:
            settings_updates["showInAssetSummary"] = show_in_summary
        
        if settings_updates:
            update_input["settings"] = settings_updates
        
        # If nothing to update, return success
        if not update_input:
            return True
        
        variables = {
            "urn": property_urn,
            "input": update_input
        }
        
        result = self.execute_graphql(mutation, variables)
        
        if not result or "errors" in result or "data" not in result:
            self._handle_error(result, f"Error updating structured property {property_urn}")
            return False
        
        return True
    
    def delete_structured_property(self, property_urn: str) -> bool:
        """
        Delete a structured property from DataHub.
        
        Args:
            property_urn: The URN of the property to delete
            
        Returns:
            True if successful, False otherwise
        """
        mutation = """
        mutation deleteStructuredProperty($urn: String!) {
          deleteStructuredPropertyEntity(urn: $urn)
        }
        """
        
        variables = {
            "urn": property_urn
        }
        
        result = self.execute_graphql(mutation, variables)
        
        if not result or "errors" in result or "data" not in result:
            self._handle_error(result, f"Error deleting structured property {property_urn}")
            return False
        
        return result.get("data", {}).get("deleteStructuredPropertyEntity", False)
    
    def get_property_values(self, property_urn: str, search_term: str = "", limit: int = 20) -> List[Dict[str, Any]]:
        """Get entities that have values for a specific property
        
        Args:
            property_urn: URN of the structured property
            search_term: Optional search term to filter entities
            limit: Maximum number of results to return
            
        Returns:
            List of entities with their property values
        """
        from utils.graphql_queries import PROPERTY_VALUES_QUERY
        
        try:
            result = self._execute_graphql(
                PROPERTY_VALUES_QUERY,
                {"propertyUrn": property_urn, "search": search_term or "*", "limit": limit}
            )
            
            if result and "data" in result and "searchAcrossEntities" in result["data"]:
                return result["data"]["searchAcrossEntities"]["searchResults"]
            return []
        except Exception as e:
            logger.error(f"Error getting property values for {property_urn}: {e}")
            return []

    def _get_graphql_errors(self, result):
        """
        Extract error messages from a GraphQL result.
        
        Args:
            result (dict): The GraphQL response
            
        Returns:
            list: List of error messages
        """
        errors = []
        if isinstance(result, dict) and 'errors' in result:
            for error in result['errors']:
                if isinstance(error, dict) and 'message' in error:
                    errors.append(error['message'])
                else:
                    errors.append(str(error))
        return errors

    def _log_graphql_errors(self, result):
        """
        Log GraphQL errors from a result.
        
        Args:
            result (dict): The GraphQL response with errors
        """
        errors = self._get_graphql_errors(result)
        for error in errors:
            logger.error(f"GraphQL error: {error}")
        
        # Also log any extensions if available
        if isinstance(result, dict) and 'errors' in result:
            for error in result['errors']:
                if isinstance(error, dict) and 'extensions' in error:
                    logger.debug(f"GraphQL error extensions: {error['extensions']}")

    def get_assertions(self, start=0, count=20, query="*", status=None, entity_urn=None, start_time_millis=None, end_time_millis=None, run_events_limit=10):
        """
        Get assertions from DataHub using GraphQL.
        
        Args:
            start (int): Starting offset for pagination
            count (int): Number of results to return
            query (str): Search query
            status (str): Filter by assertion run status
            entity_urn (str): Filter by entity URN
            start_time_millis (int): Start time for run events filter
            end_time_millis (int): End time for run events filter
            run_events_limit (int): Limit for run events
            
        Returns:
            dict: Response containing assertions data
        """
        variables = {
            "input": {
                "types": ["ASSERTION"],
                "query": query,
                "start": start,
                "count": count
            }
        }

        # Corrected GraphQL query for assertions
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
                              ... on TableSchema {
                                schema
                              }
                              ... on KeyValueSchema {
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
                                          ... on CorpUser {
                                            urn
                                          }
                                          ... on CorpGroup {
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
                      runEvents(limit: %s) {
                        ... on AssertionRunEventsResult {
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
                                  ... on CorpUser {
                                    urn
                                  }
                                  ... on CorpGroup {
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
        """ % run_events_limit

        result = self.execute_graphql(graphql_query, variables)
        
        if "errors" in result:
            self._log_graphql_errors(result)
            return {"success": False, "error": f"GraphQL error: {result['errors'][0]['message']}"}
            
        search_results = result.get("data", {}).get("searchAcrossEntities", {})
        return {"success": True, "data": search_results}