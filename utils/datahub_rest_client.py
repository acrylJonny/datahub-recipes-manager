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

    def __init__(self, server_url: str, token: Optional[str] = None):
        """
        Initialize the DataHub REST client

        Args:
            server_url: DataHub GMS server URL
            token: DataHub authentication token (optional)
        """
        self.server_url = server_url.rstrip('/')
        self.token = token
        self.headers = {
            "accept": "application/json",
            "Content-Type": "application/json"
        }
        
        # Add logger attribute
        self.logger = logging.getLogger(__name__)
        
        if token:
            self.headers["Authorization"] = f"Bearer {token}"
            
        # Initialize Graph client if SDK is available
        self.graph = None
        if DATAHUB_SDK_AVAILABLE:
            try:
                self.graph = DataHubGraph(
                    config=DatahubClientConfig(
                        server=server_url,
                        token=token,
                    )
                )
                logger.info("DataHubGraph client initialized successfully")
            except Exception as e:
                logger.warning(f"Failed to initialize DataHubGraph client: {str(e)}")
                logger.warning("Advanced GraphQL functionality will not be available")
    
    def execute_graphql(self, query, variables=None):
        """
        Execute a GraphQL query against the DataHub GraphQL API.
        
        Uses only the datahub SDK for GraphQL execution, with no HTTP fallback.
        
        Args:
            query (str): The GraphQL query to execute
            variables (dict, optional): Variables for the GraphQL query
            
        Returns:
            dict: The GraphQL response
        """
        if variables is None:
            variables = {}
            
        # If we have a datahubgraph client available, use it
        if self.graph is not None:
            try:
                logger.debug(f"Executing GraphQL via datahubgraph SDK: {query[:100]}...")
                result = self.graph.execute_graphql(query, variables)
                
                if isinstance(result, dict) and "data" in result:
                    return result
                else:
                    logger.debug(f"Converting GraphQL response from format: {type(result)} to standard format")
                    # Try to convert the result to a proper format if possible
                    try:
                        if hasattr(result, '__dict__'):
                            converted_result = vars(result)
                            if "data" not in converted_result:
                                # Add an empty data field to prevent NoneType errors
                                converted_result["data"] = {}
                            return converted_result
                        elif hasattr(result, 'to_dict'):
                            converted_result = result.to_dict()
                            if "data" not in converted_result:
                                converted_result["data"] = {}
                            return converted_result
                        else:
                            # Ensure data field exists to prevent NoneType errors
                            return {"data": result if result is not None else {}}
                    except Exception as e:
                        logger.warning(f"Could not convert GraphQL SDK result to dict: {str(e)}")
                        return {"errors": [{"message": f"Failed to format GraphQL result: {str(e)}"}], "data": {}}
            except Exception as e:
                logger.warning(f"Error executing GraphQL via SDK: {str(e)}")
                return {"errors": [{"message": str(e)}], "data": {}}
        else:
            logger.error("DataHubGraph client is not available. Cannot execute GraphQL query.")
            return {"errors": [{"message": "DataHubGraph client is not available"}], "data": {}}
    
    def test_connection(self) -> bool:
        """
        Test connection to DataHub
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            response = requests.get(
                f"{self.server_url}/config",
                headers=self.headers,
                timeout=10
            )
            return response.status_code == 200
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
            
            # Fixed mutation without subselections, just returns a string
            mutation = """
            mutation createIngestionSource($input: CreateIngestionSourceInput!) {
                createIngestionSource(input: $input)
            }
            """
            
            variables = {
                "input": {
                    "type": source_type,
                    "name": name,
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
                variables["input"]["config"]["extraArgs"] = extra_args
                
            self.logger.debug(f"GraphQL variables: {json.dumps(variables)}")
            result = self.execute_graphql(mutation, variables)
            
            if "errors" in result:
                error_msgs = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.warning(f"GraphQL errors when creating ingestion source: {', '.join(error_msgs)}")
                # Continue to REST API fallback
            else:
                # Success - createIngestionSource returns the URN
                created_urn = result.get("data", {}).get("createIngestionSource")
                if created_urn:
                    self.logger.info(f"Successfully created ingestion source via GraphQL: {source_id}")
                    return {
                        "urn": created_urn,
                        "id": source_id,
                        "name": name,
                        "type": source_type,
                        "status": "created"
                    }
                else:
                    self.logger.warning("GraphQL mutation returned success but no URN")
                    # We'll still return base info since the mutation didn't report errors
                    return {
                        "urn": source_urn,
                        "id": source_id,
                        "name": name, 
                        "type": source_type,
                        "status": "created"
                    }
        except Exception as e:
            self.logger.warning(f"Error creating ingestion source via GraphQL: {str(e)}")
            # Continue to direct GraphQL endpoint
            
        # Try direct GraphQL endpoint
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
                "variables": {
                    "input": variables["input"]
                }
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
                        "status": "created"
                    }
                else:
                    self.logger.warning(f"GraphQL errors with direct endpoint: {direct_result.get('errors')}")
            else:
                self.logger.warning(f"Failed with direct GraphQL endpoint: {direct_response.status_code}")
        except Exception as e:
            self.logger.warning(f"Error with direct GraphQL endpoint: {str(e)}")
            
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
                            "recipe": recipe_str,
                            "executorId": executor_id,
                            "debugMode": debug_mode,
                            "extraArgs": extra_args or {}
                        }
                    }
                }
            }]
            
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
                        "status": "created"
                    }
                except Exception as e:
                    self.logger.warning(f"Error parsing REST API response: {str(e)}")
                    # Still return success with known data
                    return {
                        "urn": source_urn,
                        "id": source_id,
                        "name": name,
                        "type": source_type,
                        "status": "created"
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
                    executorId
                    __typename
                  }
                  result {
                    status
                    startTimeMs
                    durationMs
                    executorInstanceId
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
                }
                
                # Parse the recipe JSON
                config = ingestion_source.get("config", {}) or {}
                if config is None:
                    config = {}
                    
                recipe_str = config.get("recipe", "{}")
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
                                self.logger.debug(f"Successfully parsed recipe JSON: {json.dumps(recipe)[:100]}...")
                            except json.JSONDecodeError:
                                self.logger.warning(f"Could not parse recipe JSON for {source_id}, treating as raw string")
                                # If it's not valid JSON, treat it as a raw string
                                # This could be a template or other format
                                recipe = recipe_str
                    else:
                        self.logger.warning(f"Unexpected recipe type: {type(recipe_str)}, using empty dict")
                        recipe = {}
                        
                    source_info["recipe"] = recipe
                    source_info["config"] = {
                        "executorId": config.get("executorId", "default"),
                        "debugMode": config.get("debugMode", False),
                        "version": config.get("version")
                    }
                except Exception as e:
                    self.logger.warning(f"Error processing recipe for {source_id}: {str(e)}")
                    source_info["recipe"] = {}
                
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
                    self.logger.debug(f"OpenAPI v3 response: {json.dumps(data)[:200]}...")
                    
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
                        }
                        
                        # Parse the recipe JSON if it exists
                        config = source_info.get("config", {})
                        recipe_str = config.get("recipe", "{}")
                        self.logger.debug(f"Raw recipe string from OpenAPI: {recipe_str}")
                        
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
                                        self.logger.debug(f"Successfully parsed recipe JSON: {json.dumps(recipe)[:100]}...")
                                    except json.JSONDecodeError:
                                        self.logger.warning(f"Could not parse recipe JSON for {source_id}, treating as raw string")
                                        recipe = recipe_str
                            else:
                                self.logger.warning(f"Unexpected recipe type: {type(recipe_str)}, using empty dict")
                                recipe = {}
                                
                            result["recipe"] = recipe
                            result["config"] = {
                                "executorId": config.get("executorId", "default"),
                                "debugMode": config.get("debugMode", False),
                                "version": config.get("version")
                            }
                        except Exception as e:
                            self.logger.warning(f"Error processing recipe for {source_id}: {str(e)}")
                            result["recipe"] = {}
                        
                        return result
                    else:
                        self.logger.warning(f"No source info found in OpenAPI v3 response for {source_id}")
                except json.JSONDecodeError:
                    self.logger.warning(f"Could not parse JSON response from OpenAPI v3 for {source_id}")
                except Exception as e:
                    self.logger.warning(f"Error processing OpenAPI v3 response for {source_id}: {str(e)}")
            else:
                self.logger.warning(f"OpenAPI v3 GET failed: {response.status_code} - {response.text[:100]}")
                
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
                "recipe": {},  # Empty recipe
                "config": {
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
                f"{self.server_url}/api/v2/secretes"
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
            graphql_input = {}
            
            # Always include the type from the current source - this is required for the mutation
            graphql_input["type"] = source_type or current_source.get("type")
            if not graphql_input["type"]:
                self.logger.error("Source type is required but not available from current source or parameters")
                return None
                
            # Only add name if provided
            if name is not None:
                graphql_input["name"] = name
            else:
                # Always include the current name - required for the mutation
                graphql_input["name"] = current_source.get("name")
            
            # Build config object
            config = {}
            if recipe_str is not None:
                config["recipe"] = recipe_str
            
            # Always include executorId - it's required by the GraphQL schema
            if executor_id is not None:
                config["executorId"] = executor_id
            elif current_source.get("config", {}).get("executorId"):
                config["executorId"] = current_source.get("config", {}).get("executorId")
            else:
                config["executorId"] = "default"  # Fallback to default executor
                
            if debug_mode is not None:
                config["debugMode"] = debug_mode
            elif current_source.get("config", {}).get("debugMode") is not None:
                config["debugMode"] = current_source.get("config", {}).get("debugMode")
                
            if extra_args is not None:
                config["extraArgs"] = extra_args
                
            # If we don't have recipe in config but need to update other config fields,
            # get the current recipe from the source
            if "recipe" not in config and current_source.get("config", {}).get("recipe") and config:
                config["recipe"] = current_source.get("config", {}).get("recipe")
            
            if config:
                graphql_input["config"] = config
            
            # Build schedule object only if we have something to update
            if schedule_interval is not None or timezone is not None:
                schedule_obj = {}
                
                # Use current values as defaults if available
                current_schedule = current_source.get("schedule", {})
                
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
                    graphql_input["schedule"] = schedule_obj
            elif current_source.get("schedule"):
                # Include the current schedule in the update
                graphql_input["schedule"] = current_source.get("schedule")
            
            # Only proceed if we have something to update
            if not graphql_input:
                self.logger.warning("No updates to apply to ingestion source")
                return current_source
                
            variables = {
                "urn": urn,
                "input": graphql_input
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
                    
            # Prepare payload
            payload = {}
            if name is not None:
                payload["name"] = name
            if source_type is not None:
                payload["type"] = source_type
            if recipe_str is not None:
                payload["config"] = payload.get("config", {})
                payload["config"]["recipe"] = recipe_str
            if executor_id is not None:
                payload["config"] = payload.get("config", {})
                payload["config"]["executorId"] = executor_id
            if debug_mode is not None:
                payload["config"] = payload.get("config", {})
                payload["config"]["debugMode"] = debug_mode
            if extra_args is not None:
                payload["config"] = payload.get("config", {})
                payload["config"]["extraArgs"] = extra_args
            if schedule_interval is not None or timezone is not None:
                payload["schedule"] = payload.get("schedule", {})
                current_schedule = current_source.get("schedule", {})
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
                    
                self.logger.debug(f"Endpoint {endpoint['url']} returned {response.status_code}: {response.text[:100]}")
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
        List policies from DataHub.
        
        This method attempts to list policies using GraphQL first and falls back to REST API if needed.
        
        Args:
            limit (int): Maximum number of policies to return
            start (int): Starting offset for pagination
                
        Returns:
            list: List of policy dictionaries or empty list if none found
        """
        self.logger.info(f"Listing policies (limit={limit}, start={start})")
        
        # Try GraphQL first
        graphql_query = """
        query listPolicies($input: ListPoliciesInput!) {
          listPolicies(input: $input) {
            start
            count
            total
            policies {
              urn
              type
              id
              name
              description
              state
              privileges
              resources {
                type
                resource
              }
              actors {
                users
                groups
                allUsers
                allGroups
                resourceOwners
              }
              editable
              createdAt
              createdBy
              lastModified {
                time
                actor
              }
            }
          }
        }
        """
        
        variables = {
            "input": {
                "start": start,
                "count": limit
            }
        }
        
        try:
            result = self.execute_graphql(graphql_query, variables)
            if result and "errors" not in result:
                policy_list = result.get("data", {}).get("listPolicies", {}).get("policies", [])
                if policy_list:
                    self.logger.info(f"Successfully retrieved {len(policy_list)} policies via GraphQL")
                    return policy_list
            else:
                error_msg = result.get("errors", [])[0].get("message") if result and "errors" in result else "Unknown error"
                self.logger.warning(f"GraphQL policy listing failed: {error_msg}, falling back to REST API")
        except Exception as e:
            self.logger.warning(f"Error listing policies via GraphQL: {str(e)}")
        
        # Fall back to OpenAPI v3 endpoint
        try:
            url = f"{self.server_url}/openapi/policies"
            params = {"start": start, "count": limit}
            
            # Set up headers
            headers = self.headers.copy() if hasattr(self, 'headers') else {'Content-Type': 'application/json'}
            if hasattr(self, 'token') and self.token:
                headers['Authorization'] = f'Bearer {self.token}'
            
            self.logger.info(f"Attempting to list policies via REST API: {url}")
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                result = response.json()
                policies = result.get("policies", [])
                self.logger.info(f"Successfully retrieved {len(policies)} policies via REST API")
                return policies
            else:
                self.logger.error(f"Failed to list policies via REST API: {response.status_code} - {response.text}")
                
                # Try the legacy endpoint format if available
                legacy_url = f"{self.server_url}/policies"
                legacy_response = requests.get(legacy_url, headers=headers, params=params)
                
                if legacy_response.status_code == 200:
                    legacy_result = legacy_response.json()
                    legacy_policies = legacy_result.get("policies", [])
                    self.logger.info(f"Successfully retrieved {len(legacy_policies)} policies via legacy REST API")
                    return legacy_policies
                else:
                    self.logger.error(f"Failed to list policies via legacy REST API: {legacy_response.status_code}")
        except Exception as e:
            self.logger.error(f"Error listing policies via REST API: {str(e)}")
        
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
              resource
            }
            actors {
              users
              groups
              allUsers
              allGroups
              resourceOwners
            }
            editable
            createdAt
            createdBy
            lastModified {
              time
              actor
            }
          }
        }
        """
        
        variables = {"urn": policy_urn}
        
        try:
            result = self.execute_graphql(graphql_query, variables)
            if result and "errors" not in result:
                policy_data = result.get("data", {}).get("policy")
                if policy_data:
                    self.logger.info(f"Successfully retrieved policy {policy_id} via GraphQL")
                    return policy_data
            else:
                error_msg = result.get("errors", [])[0].get("message") if result and "errors" in result else "Unknown error"
                self.logger.warning(f"GraphQL policy retrieval failed: {error_msg}, falling back to REST API")
        except Exception as e:
            self.logger.warning(f"Error retrieving policy via GraphQL: {str(e)}")
        
        # Fall back to REST API
        try:
            # Set up headers
            headers = self.headers.copy() if hasattr(self, 'headers') else {'Content-Type': 'application/json'}
            if hasattr(self, 'token') and self.token:
                headers['Authorization'] = f'Bearer {self.token}'
            
            # Try with both ID and URN
            urls = [
                f"{self.server_url}/openapi/policies/{policy_id}",
                f"{self.server_url}/openapi/policies/{policy_urn}"
            ]
            
            for url in urls:
                self.logger.info(f"Attempting to get policy via REST API: {url}")
                response = requests.get(url, headers=headers)
                
                if response.status_code == 200:
                    policy_data = response.json()
                    self.logger.info(f"Successfully retrieved policy {policy_id} via REST API")
                    return policy_data
                elif response.status_code == 404:
                    self.logger.warning(f"Policy not found at {url}, trying next endpoint")
                else:
                    self.logger.warning(f"Failed to get policy at {url}: {response.status_code} - {response.text}")
            
            # Try legacy endpoint
            legacy_url = f"{self.server_url}/policies/{policy_id}"
            self.logger.info(f"Attempting to get policy via legacy API: {legacy_url}")
            legacy_response = requests.get(legacy_url, headers=headers)
            
            if legacy_response.status_code == 200:
                legacy_data = legacy_response.json()
                self.logger.info(f"Successfully retrieved policy {policy_id} via legacy API")
                return legacy_data
            else:
                self.logger.warning(f"Failed to get policy via legacy API: {legacy_response.status_code}")
                
        except Exception as e:
            self.logger.error(f"Error retrieving policy via REST API: {str(e)}")
        
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
                - type: Policy type (e.g., METADATA_POLICY)
                - state: Policy state (e.g., ACTIVE, INACTIVE)
                - resources: List of resources the policy applies to
                - privileges: List of privileges granted by the policy
                - actors: Dict containing users/groups the policy applies to
                
        Returns:
            dict: Created policy details or None if creation failed
        """
        self.logger.info(f"Creating new policy with name: {policy_data.get('name', 'Unnamed')}")
        
        if not policy_data.get('name'):
            self.logger.error("Policy name is required")
            return None
        
        # Try GraphQL first
        graphql_mutation = """
        mutation createPolicy($input: CreatePolicyInput!) {
          createPolicy(input: $input) {
            urn
            type
            id
            name
            description
            state
            privileges
            resources {
              type
              resource
            }
            actors {
              users
              groups
              allUsers
              allGroups
              resourceOwners
            }
            editable
            createdAt
            createdBy
          }
        }
        """
        
        # Prepare input variable
        create_input = {
            "type": policy_data.get("type", "METADATA_POLICY"),
            "name": policy_data.get("name"),
            "description": policy_data.get("description", ""),
            "state": policy_data.get("state", "ACTIVE"),
            "privileges": policy_data.get("privileges", []),
            "resources": policy_data.get("resources", []),
            "actors": policy_data.get("actors", {})
        }
        
        variables = {"input": create_input}
        
        try:
            result = self.execute_graphql(graphql_mutation, variables)
            if result and "errors" not in result:
                created_policy = result.get("data", {}).get("createPolicy")
                if created_policy:
                    self.logger.info(f"Successfully created policy {policy_data.get('name')} via GraphQL")
                    return created_policy
            else:
                error_msg = result.get("errors", [])[0].get("message") if result and "errors" in result else "Unknown error"
                self.logger.warning(f"GraphQL policy creation failed: {error_msg}, falling back to REST API")
        except Exception as e:
            self.logger.warning(f"Error creating policy via GraphQL: {str(e)}")
        
        # Fall back to OpenAPI v3 endpoint
        try:
            url = f"{self.server_url}/openapi/policies"
            self.logger.info(f"Attempting to create policy via REST API: {url}")
            
            # Set up headers
            headers = self.headers.copy() if hasattr(self, 'headers') else {'Content-Type': 'application/json'}
            if hasattr(self, 'token') and self.token:
                headers['Authorization'] = f'Bearer {self.token}'
            
            response = requests.post(url, headers=headers, json=policy_data)
            if response.status_code in (200, 201):
                created_policy = response.json()
                self.logger.info(f"Successfully created policy {policy_data.get('name')} via REST API")
                return created_policy
            else:
                self.logger.error(f"Failed to create policy via REST API: {response.status_code} - {response.text}")
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
        
        # Try GraphQL first
        graphql_mutation = """
        mutation updatePolicy($urn: String!, $input: UpdatePolicyInput!) {
          updatePolicy(urn: $urn, input: $input) {
            urn
            type
            id
            name
            description
            state
            privileges
            resources {
              type
              resource
            }
            actors {
              users
              groups
              allUsers
              allGroups
              resourceOwners
            }
            editable
            lastModified {
              time
              actor
            }
          }
        }
        """
        
        # Prepare input variable
        update_input = {}
        for field in ["name", "description", "type", "state", "privileges", "resources", "actors"]:
            if field in policy_data:
                update_input[field] = policy_data[field]
        
        variables = {
            "urn": policy_urn,
            "input": update_input
        }
        
        try:
            result = self.execute_graphql(graphql_mutation, variables)
            if result and "errors" not in result:
                updated_policy = result.get("data", {}).get("updatePolicy")
                if updated_policy:
                    self.logger.info(f"Successfully updated policy {policy_id} via GraphQL")
                    return updated_policy
            else:
                error_msg = result.get("errors", [])[0].get("message") if result and "errors" in result else "Unknown error"
                self.logger.warning(f"GraphQL policy update failed: {error_msg}, falling back to REST API")
        except Exception as e:
            self.logger.warning(f"Error updating policy via GraphQL: {str(e)}")
        
        # Fall back to OpenAPI v3 endpoint
        try:
            # Set up headers
            headers = self.headers.copy() if hasattr(self, 'headers') else {'Content-Type': 'application/json'}
            if hasattr(self, 'token') and self.token:
                headers['Authorization'] = f'Bearer {self.token}'
                
            # Try with ID first
            urls = [
                f"{self.server_url}/openapi/policies/{policy_id}",
                f"{self.server_url}/openapi/policies/{policy_urn}"
            ]
            
            for url in urls:
                self.logger.info(f"Attempting to update policy via REST API: {url}")
                response = requests.put(url, headers=headers, json=policy_data)
                
                if response.status_code == 200:
                    updated_policy = response.json()
                    self.logger.info(f"Successfully updated policy {policy_id} via REST API")
                    return updated_policy
                elif response.status_code == 404:
                    self.logger.warning(f"Policy not found at {url}, trying next endpoint")
                else:
                    self.logger.warning(f"Failed to update policy at {url}: {response.status_code} - {response.text}")
            
            self.logger.error("Failed to update policy via any endpoint")
        except Exception as e:
            self.logger.error(f"Error updating policy via REST API: {str(e)}")
        
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
            if result and "errors" not in result:
                if result.get("data", {}).get("deletePolicy") == True:
                    self.logger.info(f"Successfully deleted policy {policy_id} via GraphQL")
                    return True
            else:
                error_msg = result.get("errors", [])[0].get("message") if result and "errors" in result else "Unknown error"
                self.logger.warning(f"GraphQL policy deletion failed: {error_msg}, falling back to REST API")
        except Exception as e:
            self.logger.warning(f"Error deleting policy via GraphQL: {str(e)}")
        
        # Fall back to OpenAPI v3 endpoint
        try:
            # Set up headers
            headers = self.headers.copy() if hasattr(self, 'headers') else {'Content-Type': 'application/json'}
            if hasattr(self, 'token') and self.token:
                headers['Authorization'] = f'Bearer {self.token}'
                
            # Try with both ID and URN
            urls = [
                f"{self.server_url}/openapi/policies/{policy_id}",
                f"{self.server_url}/openapi/policies/{policy_urn}"
            ]
            
            for url in urls:
                self.logger.info(f"Attempting to delete policy via REST API: {url}")
                response = requests.delete(url, headers=headers)
                
                if response.status_code in (200, 204):
                    self.logger.info(f"Successfully deleted policy {policy_id} via REST API")
                    return True
                elif response.status_code == 404:
                    self.logger.warning(f"Policy not found at {url}, trying next endpoint")
                else:
                    self.logger.warning(f"Failed to delete policy at {url}: {response.status_code} - {response.text}")
            
            self.logger.error("Failed to delete policy via any endpoint")
        except Exception as e:
            self.logger.error(f"Error deleting policy via REST API: {str(e)}")
        
        return False 