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
            
    def create_ingestion_source(self, ingestion_source: dict) -> dict:
        """
        Create a new ingestion source in DataHub.
        
        This method attempts to create the ingestion source using GraphQL first and falls back to REST API if needed.
        
        Args:
            ingestion_source: Dictionary with ingestion source details
            
        Returns:
            Dictionary with created ingestion source details or None if creation failed
        """
        source_id = ingestion_source.get("id") or ingestion_source.get("name", "unknown")
        self.logger.info(f"Creating ingestion source: {source_id}")
        
        # Clean up payload to remove fields that shouldn't be sent to DataHub
        payload = ingestion_source.copy()
        
        # Make sure executor_id is available but don't set to default in overall payload
        executor_id = payload.get("executor_id", "default")
        
        # Remove schedule if not explicitly specified
        if "schedule" in payload and not (payload["schedule"].get("cron") or payload["schedule"].get("interval")):
            self.logger.debug("Removing empty schedule from payload")
            payload.pop("schedule", None)
        
        # First, try with GraphQL
        try:
            # DataHub doesn't have a CreateIngestionSourceInput type, but does use a similar structure to UpdateIngestionSourceInput
            graphql_mutation = """
            mutation createIngestionSource($input: UpdateIngestionSourceInput!) {
              createIngestionSource(input: $input)
            }
            """
            
            # Prepare the GraphQL input
            graphql_input = {
                "name": payload.get("name", source_id),
                "type": payload.get("type", "metadata"),
                "config": {
                    "recipe": payload.get("recipe", payload.get("config", {}).get("recipe", "{}")),
                    "executorId": executor_id  # Always include executorId as it's required by the GraphQL schema
                }
            }
            
            # Add optional fields if they exist
            if "description" in payload:
                graphql_input["description"] = payload["description"]
                
            if "schedule" in payload and payload["schedule"].get("cron"):
                graphql_input["schedule"] = {
                    "cron": payload["schedule"]["cron"],
                    "timezone": payload["schedule"].get("timezone", "UTC")
                }
            
            # Add debug_mode if present
            if "debug_mode" in payload:
                graphql_input["config"]["debugMode"] = payload["debug_mode"]
                
            self.logger.debug(f"Creating ingestion source with GraphQL: {graphql_input}")
            
            result = self.execute_graphql(graphql_mutation, {"input": graphql_input})
            if result and "errors" not in result:
                source_urn = result.get("data", {}).get("createIngestionSource")
                if source_urn:
                    self.logger.info(f"Successfully created ingestion source via GraphQL: {source_id}")
                    # Get the full details of the created source
                    return self.get_ingestion_source(source_id)
            else:
                error_msg = "GraphQL errors when creating ingestion source: "
                if result and "errors" in result:
                    error_msg += str(result["errors"])
                self.logger.warning(f"{error_msg}, falling back to REST API")
        except Exception as e:
            self.logger.warning(f"Error executing GraphQL via SDK: {e}")
            self.logger.warning("Falling back to REST API")
        
        # Fall back to REST API (try multiple endpoint patterns)
        try:
            # Set up headers
            headers = self.headers.copy() if hasattr(self, 'headers') else {'Content-Type': 'application/json'}
            if hasattr(self, 'token') and self.token:
                headers['Authorization'] = f'Bearer {self.token}'
            
            # Try multiple possible endpoints paths
            endpoints = [
                f"{self.server_url}/ingestion",
                f"{self.server_url}/sources",
                f"{self.server_url}/ingestionsources",
                f"{self.server_url}/api/v2/ingestionsource"
            ]
            
            # Add executor_id to payload for REST API if not present
            if "executor_id" not in payload:
                payload["executor_id"] = executor_id
                
            # Try each endpoint
            for url in endpoints:
                self.logger.info(f"Attempting to create ingestion source via REST API: {url}")
                
                response = requests.post(url, headers=headers, json=payload)
                
                if response.status_code in (200, 201):
                    self.logger.info(f"Successfully created ingestion source via endpoint {url}: {source_id}")
                    return self.get_ingestion_source(source_id)
                else:
                    self.logger.warning(f"Failed to create ingestion source at {url}: {response.status_code} - {response.text}")
            
            # If we've tried all endpoints and still failed
            self.logger.error("All REST API endpoints failed for creating ingestion source")
        except Exception as e:
            self.logger.error(f"Error creating ingestion source: {str(e)}")
        
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
            source_id: ID of the ingestion source to retrieve
            
        Returns:
            dict: Source information if found, None otherwise
        """
        self.logger.info(f"Fetching ingestion source: {source_id}")
        source_urn = f"urn:li:dataHubIngestionSource:{source_id}"
        
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
                try:
                    # Handle different recipe formats
                    if recipe_str is None:
                        recipe = {}
                    elif isinstance(recipe_str, dict):
                        recipe = recipe_str
                    elif isinstance(recipe_str, str):
                        if not recipe_str.strip():
                            recipe = {}
                        else:
                            # Try to parse as JSON, with fallback to empty dict
                            try:
                                recipe = json.loads(recipe_str)
                            except json.JSONDecodeError:
                                self.logger.warning(f"Could not parse recipe JSON for {source_id}, using empty dict")
                                recipe = {}
                    else:
                        self.logger.warning(f"Unexpected recipe type: {type(recipe_str)}, using empty dict")
                        recipe = {}
                        
                    source_info["recipe"] = recipe
                    source_info["executor_id"] = config.get("executorId")
                    source_info["debug_mode"] = config.get("debugMode", False)
                    source_info["version"] = config.get("version")
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
        
        # Try OpenAPI v3 endpoint with the new format
        try:
            openapi_url = f"{self.server_url}/openapi/v3/entity/datahubingestionsource/{source_urn}"
            self.logger.debug(f"Fetching ingestion source via OpenAPI v3: GET {openapi_url}")
            
            response = requests.get(openapi_url, headers=self.headers)
            
            if response.status_code == 200:
                self.logger.debug(f"Successfully retrieved ingestion source via OpenAPI v3: {source_id}")
                try:
                    data = response.json()
                    
                    # Extract source info from the OpenAPI v3 format
                    # The structure has nested dataHubIngestionSourceInfo.value
                    source_info = data.get("dataHubIngestionSourceInfo", {}).get("value", {})
                    
                    if source_info:
                        # Parse the recipe JSON if it exists
                        config = source_info.get("config", {})
                        recipe_str = config.get("recipe", "{}")
                        try:
                            # Handle different recipe formats
                            if recipe_str is None:
                                recipe = {}
                            elif isinstance(recipe_str, dict):
                                recipe = recipe_str
                            elif isinstance(recipe_str, str):
                                if not recipe_str.strip():
                                    recipe = {}
                                else:
                                    # Try to parse as JSON, with fallback to empty dict
                                    try:
                                        recipe = json.loads(recipe_str)
                                    except json.JSONDecodeError:
                                        self.logger.warning(f"Could not parse recipe JSON for {source_id}, using empty dict")
                                        recipe = {}
                            else:
                                self.logger.warning(f"Unexpected recipe type: {type(recipe_str)}, using empty dict")
                                recipe = {}
                        except Exception as e:
                            self.logger.warning(f"Error processing recipe for {source_id}: {str(e)}")
                            recipe = {}
                        
                        # Create simplified source object
                        result = {
                            "urn": source_urn,
                            "id": source_id,
                            "name": source_info.get("name", source_id),
                            "type": source_info.get("type", ""),
                            "platform": source_info.get("platform", ""),
                            "recipe": recipe,
                            "schedule": source_info.get("schedule", {}),
                            "config": {
                                "executorId": config.get("executorId", "default"),
                                "debugMode": config.get("debugMode", False),
                                "version": config.get("version", "0.8.42"),
                                "extraArgs": config.get("extraArgs", {})
                            }
                        }
                        
                        return result
                    else:
                        self.logger.warning(f"No source info found in OpenAPI v3 response for {source_id}")
                except json.JSONDecodeError:
                    self.logger.warning(f"Failed to parse JSON from OpenAPI v3 response for {source_id}")
            elif response.status_code == 404:
                self.logger.warning(f"Ingestion source not found (404): {source_id}")
            else:
                self.logger.warning(f"Failed to get ingestion source via OpenAPI v3: {response.status_code} - {response.text}")
        except Exception as e:
            self.logger.error(f"Error getting ingestion source via OpenAPI v3: {str(e)}")
        
        # Last resort: Try direct HTTP GET to different formats of the API
        try:
            self.logger.debug(f"Trying direct HTTP GET for source: {source_id}")
            alternate_urls = [
                f"{self.server_url}/api/v2/ingestion/sources/{source_id}",
                f"{self.server_url}/ingestion-sources/{source_id}",
                f"{self.server_url}/api/v2/ingestion-sources/{source_id}"
            ]
            
            for url in alternate_urls:
                try:
                    self.logger.debug(f"Trying URL: {url}")
                    response = requests.get(url, headers=self.headers)
                    
                    if response.status_code == 200:
                        self.logger.info(f"Successfully retrieved ingestion source via direct HTTP GET: {url}")
                        try:
                            data = response.json()
                            
                            # Create minimal source info
                            source_info = {
                                "urn": source_urn,
                                "id": source_id,
                                "name": data.get("name", source_id),
                                "type": data.get("type", "BATCH"),
                                "schedule": data.get("schedule", {"interval": "0 0 * * *", "timezone": "UTC"}),
                                "recipe": {}
                            }
                            
                            # Extract recipe if available
                            recipe = {}
                            if "recipe" in data:
                                try:
                                    recipe_data = data["recipe"]
                                    if isinstance(recipe_data, dict):
                                        recipe = recipe_data
                                    elif isinstance(recipe_data, str):
                                        if recipe_data.strip():
                                            try:
                                                recipe = json.loads(recipe_data)
                                            except json.JSONDecodeError:
                                                self.logger.warning(f"Could not parse recipe JSON from API response for {source_id}")
                                except Exception as e:
                                    self.logger.warning(f"Error processing recipe from API response: {str(e)}")
                                
                            source_info["recipe"] = recipe
                                    
                            return source_info
                        except json.JSONDecodeError:
                            self.logger.warning(f"Could not parse JSON response from {url}")
                except Exception as e:
                    self.logger.debug(f"Error with URL {url}: {str(e)}")
                    
            # If we got here, we've tried all URLs and failed
            self.logger.warning(f"All attempts to retrieve ingestion source {source_id} failed")
        except Exception as e:
            self.logger.error(f"Error with direct HTTP fallback: {str(e)}")
        
        # If all attempts failed, return minimal info based on ID
        default_source = {
            "urn": source_urn,
            "id": source_id,
            "name": source_id,
            "type": "BATCH",
            "schedule": {"interval": "0 0 * * *", "timezone": "UTC"},
            "recipe": {},
            "config": {
                "executorId": "default",
                "debugMode": False,
                "version": "0.8.42"
            }
        }
        
        self.logger.warning(f"Returning default source info for {source_id} as all attempts failed")
        return default_source
    
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
        source_id: str, 
        recipe_config: Optional[Dict] = None,
        schedule: Optional[Dict] = None,
        name: Optional[str] = None,
        executor_id: Optional[str] = None,
        debug_mode: Optional[bool] = None
    ) -> bool:
        """
        Patch an existing ingestion source with partial updates.
        
        This method uses a JSON Patch approach to update only the specified fields.
        
        Args:
            source_id: ID of the ingestion source to patch
            recipe_config: Optional updated recipe configuration
            schedule: Optional updated schedule (dict with cron and timezone)
            name: Optional new name for the source
            executor_id: Optional executor ID
            debug_mode: Optional debug mode flag
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Patching ingestion source: {source_id}")
        
        # First, get the current source to determine what needs to be updated
        current_source = self.get_ingestion_source(source_id)
        if not current_source:
            self.logger.error(f"Failed to get current source: {source_id}")
            return False
        
        # Prepare the update payload with only fields that need to be updated
        update_payload = {}
        
        if name is not None:
            update_payload["name"] = name
        
        if recipe_config is not None:
            # Get current recipe
            current_recipe_str = current_source.get("config", {}).get("recipe", "{}")
            try:
                current_recipe = json.loads(current_recipe_str) if isinstance(current_recipe_str, str) else current_recipe_str
                self.logger.debug(f"Current recipe: {json.dumps(current_recipe)}")
            except json.JSONDecodeError:
                self.logger.warning(f"Failed to parse current recipe JSON, treating as empty: {current_recipe_str}")
                current_recipe = {}
            
            # Merge with new recipe config
            updated_recipe = {**current_recipe, **recipe_config}
            self.logger.debug(f"Updated recipe: {json.dumps(updated_recipe)}")
            
            # Add to update payload
            update_payload["config"] = {
                "recipe": json.dumps(updated_recipe) if isinstance(updated_recipe, dict) else updated_recipe
            }
        else:
            update_payload["config"] = {}
        
        # Get current executor_id if available
        current_executor_id = current_source.get("config", {}).get("executorId", "default")
        
        # Add executor_id to the config
        if executor_id is not None:
            update_payload["config"]["executorId"] = executor_id
        else:
            # Always include executorId in GraphQL as it's required
            update_payload["config"]["executorId"] = current_executor_id
        
        if schedule is not None:
            update_payload["schedule"] = schedule
            
        if debug_mode is not None:
            update_payload["config"]["debugMode"] = debug_mode
        
        # If no updates were specified (except for executorId), return early
        if (len(update_payload) == 1 and "config" in update_payload and 
            len(update_payload["config"]) == 1 and "executorId" in update_payload["config"] and 
            update_payload["config"]["executorId"] == current_executor_id):
            self.logger.info(f"No meaningful updates specified for source: {source_id}")
            return True
        
        # Try GraphQL first
        try:
            graphql_mutation = """
            mutation updateIngestionSource($urn: String!, $input: UpdateIngestionSourceInput!) {
              updateIngestionSource(urn: $urn, input: $input)
            }
            """
            
            source_urn = f"urn:li:dataHubIngestionSource:{source_id}"
            
            # Prepare GraphQL input
            graphql_input = {}
            
            # Always include name field for GraphQL - it's a required non-null field
            # Use the name from update_payload if provided, otherwise use name from current_source
            graphql_input["name"] = update_payload.get("name", current_source.get("name", source_id))
            
            # Always include type field for GraphQL - it's also a required non-null field
            graphql_input["type"] = current_source.get("type", "BATCH")
            
            # Config is required with recipe for GraphQL
            graphql_input["config"] = {
                "executorId": update_payload.get("config", {}).get("executorId", current_executor_id)
            }
            
            # Get current recipe and format it for inclusion
            current_recipe = current_source.get("recipe", {})
            if isinstance(current_recipe, str):
                if not current_recipe:
                    current_recipe = "{}"
            elif current_recipe is None:
                current_recipe = "{}"
                
            # Add recipe to config - required field
            if "config" in update_payload and "recipe" in update_payload["config"]:
                graphql_input["config"]["recipe"] = update_payload["config"]["recipe"]
            else:
                # Use current recipe as fallback - required field
                graphql_input["config"]["recipe"] = current_recipe
                
            # Add debug mode if specified
            if "config" in update_payload and "debugMode" in update_payload["config"]:
                graphql_input["config"]["debugMode"] = update_payload["config"]["debugMode"]
            
            if "schedule" in update_payload:
                graphql_input["schedule"] = {
                    "interval": update_payload["schedule"].get("interval", ""),
                    "timezone": update_payload["schedule"].get("timezone", "UTC")
                }
            
            variables = {
                "urn": source_urn,
                "input": graphql_input
            }
            
            self.logger.debug(f"Patching source via GraphQL: {json.dumps(variables)}")
            
            result = self.execute_graphql(graphql_mutation, variables)
            if result and "errors" not in result:
                if result.get("data", {}).get("updateIngestionSource") is True:
                    self.logger.info(f"Successfully patched source via GraphQL: {source_id}")
                    return True
            else:
                error_msg = "GraphQL errors when patching source: "
                if result and "errors" in result:
                    error_msg += str(result["errors"])
                self.logger.warning(f"{error_msg}, falling back to REST API")
        except Exception as e:
            self.logger.warning(f"Error patching source via GraphQL: {str(e)}")
            self.logger.warning("Falling back to REST API")
        
        # Fall back to REST API using JSON Patch format
        try:
            # Try multiple possible endpoints paths
            endpoints = [
                f"{self.server_url}/ingestion/{source_id}",
                f"{self.server_url}/sources/{source_id}",
                f"{self.server_url}/ingestionsources/{source_id}",
                f"{self.server_url}/api/v2/ingestionsource/{source_id}"
            ]
            
            # Convert to JSON Patch format
            patch_operations = []
            
            if "name" in update_payload:
                patch_operations.append({
                    "op": "replace",
                    "path": "/name",
                    "value": update_payload["name"]
                })
            
            if "config" in update_payload:
                if "recipe" in update_payload["config"]:
                    patch_operations.append({
                        "op": "replace",
                        "path": "/config/recipe",
                        "value": update_payload["config"]["recipe"]
                    })
                
                if "executorId" in update_payload["config"]:
                    patch_operations.append({
                        "op": "replace",
                        "path": "/config/executorId",
                        "value": update_payload["config"]["executorId"]
                    })
                
                if "debugMode" in update_payload["config"]:
                    patch_operations.append({
                        "op": "replace",
                        "path": "/config/debugMode",
                        "value": update_payload["config"]["debugMode"]
                    })
            
            if "schedule" in update_payload:
                patch_operations.append({
                    "op": "replace",
                    "path": "/schedule",
                    "value": update_payload["schedule"]
                })
            
            self.logger.debug(f"JSON Patch operations: {json.dumps(patch_operations)}")
            
            # Set up headers
            headers = self.headers.copy() if hasattr(self, 'headers') else {'Content-Type': 'application/json'}
            if hasattr(self, 'token') and self.token:
                headers['Authorization'] = f'Bearer {self.token}'
            
            # Try each endpoint with PATCH
            for url in endpoints:
                self.logger.info(f"Attempting to patch source via REST API: {url}")
                
                # Use PATCH method with JSON Patch payload
                response = requests.patch(url, headers=headers, json=patch_operations)
                
                if response.status_code in (200, 204):
                    self.logger.info(f"Successfully patched source via REST API at {url}: {source_id}")
                    return True
                else:
                    self.logger.warning(f"Failed to patch source at {url}: {response.status_code} - {response.text}")
            
            # If PATCH fails, try PUT with complete updated payload
            merged_payload = current_source.copy()
            self._deep_update(merged_payload, update_payload)
            
            for url in endpoints:
                self.logger.info(f"Attempting to update source via PUT at REST API: {url}")
                legacy_response = requests.put(url, headers=headers, json=merged_payload)
                
                if legacy_response.status_code in (200, 204):
                    self.logger.info(f"Successfully updated source via PUT at {url}: {source_id}")
                    return True
                else:
                    self.logger.warning(f"Failed to update source via PUT at {url}: {legacy_response.status_code} - {legacy_response.text}")
            
            self.logger.error("All REST API endpoints failed for patching source")
            
        except Exception as e:
            self.logger.error(f"Error patching source: {str(e)}")
        
        return False

    def _deep_update(self, d, u):
        """Helper method to recursively update nested dictionaries."""
        for k, v in u.items():
            if isinstance(v, dict) and k in d and isinstance(d[k], dict):
                self._deep_update(d[k], v)
            else:
                d[k] = v

    def run_ingestion_source(self, source_id: str) -> bool:
        """
        Trigger an ingestion source to run immediately.
        First tries GraphQL mutation, then falls back to REST API endpoints
        if GraphQL fails.
        
        Args:
            source_id (str): The ID of the ingestion source to run
            
        Returns:
            bool: True if the ingestion source was successfully triggered, False otherwise
        """
        self.logger.info(f"Triggering immediate run for ingestion source: {source_id}")
        source_urn = f"urn:li:dataHubIngestionSource:{source_id}"
        
        # Primary approach: Use GraphQL with createIngestionExecutionRequest mutation
        self.logger.debug(f"Attempting to trigger ingestion via GraphQL: {source_id}")
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
        
        try:
            result = self.execute_graphql(mutation, variables)
            
            if result and "data" in result and "createIngestionExecutionRequest" in result["data"]:
                self.logger.info(f"Successfully triggered ingestion source via GraphQL: {source_id}")
                return True
            
            # Check for specific errors
            if result and "errors" in result:
                error_messages = [e.get("message", "") for e in result.get("errors", [])]
                self.logger.warning(f"GraphQL errors: {', '.join(error_messages)}")
                
                # If there are specific errors that indicate we should try a different approach,
                # we'll continue to the fallback methods
            else:
                self.logger.warning("GraphQL response format unexpected")
                
            # Fall through to REST API approaches
        except Exception as e:
            self.logger.warning(f"Error using GraphQL to trigger ingestion: {e}")
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