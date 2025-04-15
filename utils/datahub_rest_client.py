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
        
        If datahub SDK is available and initialized, use it to execute the query.
        Otherwise, fall back to direct HTTP requests.
        
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
                return result
            except Exception as e:
                logger.warning(f"Error executing GraphQL via SDK: {str(e)}")
                # Fall back to direct HTTP request
        
        # Direct HTTP request fallback
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
            
        graphql_url = f"{self.server_url}/api/v2/graphql"
        logger.debug(f"Executing GraphQL via direct HTTP request to {graphql_url}")
        
        try:
            response = requests.post(
                graphql_url,
                headers=headers,
                json={"query": query, "variables": variables}
            )
            
            if response.status_code != 200:
                logger.warning(f"GraphQL request failed with status {response.status_code}: {response.text}")
                return {"errors": [{"message": f"GraphQL request failed with status {response.status_code}"}]}
                
            return response.json()
        except Exception as e:
            logger.error(f"Error making GraphQL request: {str(e)}")
            return {"errors": [{"message": str(e)}]}
    
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
        Create a DataHub ingestion source using direct REST API

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
        # Generate unique ID if not provided
        if not source_id:
            source_id = str(uuid.uuid4())
            
        source_urn = f"urn:li:dataHubIngestionSource:{source_id}"
        
        # Prepare the payload
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
                        "recipe": json.dumps(recipe),
                        "extraArgs": extra_args or {},
                        "executorId": executor_id,
                        "debugMode": debug_mode
                    }
                }
            }
        }]
        
        try:
            # Create the ingestion source
            response = requests.post(
                f"{self.server_url}/openapi/v3/entity/datahubingestionsource",
                params={
                    "async": "true",
                    "systemMetadata": "false"
                },
                headers=self.headers,
                data=json.dumps(payload)
            )
            
            response.raise_for_status()
            creation_response = response.json()
            
            # Extract the URN from the response
            returned_urn = creation_response[0]["urn"]
            
            logger.info(f"Recipe created successfully. Status code: {response.status_code}")
            logger.info(f"Source URN: {returned_urn}")
            
            return {
                "urn": returned_urn,
                "status": "created",
                "name": name,
                "type": source_type,
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error creating ingestion source: {str(e)}")
            raise
    
    def trigger_ingestion(self, source_id: str) -> bool:
        """
        Trigger an immediate ingestion for a source

        Args:
            source_id: Source ID

        Returns:
            True if successful, False otherwise
        """
        source_urn = f"urn:li:dataHubIngestionSource:{source_id}"
        
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
        
        try:
            logger.info(f"Triggering immediate ingestion for source: {source_id}")
            
            # Use the execute_graphql method which will use datahubgraph if available
            result = self.execute_graphql(query, variables)
            
            if "errors" in result:
                logger.error(f"GraphQL errors: {result['errors']}")
                return False
            
            logger.info(f"Ingestion triggered successfully for source: {source_urn}")
            return True
        except Exception as e:
            logger.error(f"Error triggering ingestion: {str(e)}")
            return False
    
    def list_ingestion_sources(self):
        """
        List all ingestion sources defined in DataHub.
        
        Returns:
            list: A list of ingestion sources
        """
        # GraphQL query to list all ingestion sources
        query = """
        query listIngestionSources {
          listIngestionSources {
            count
            start
            total
            sources {
              urn
              type
              name
              config
              schedule {
                interval
                timezone
              }
              created {
                time
                actor
              }
              lastUpdated {
                time
                actor
              }
            }
          }
        }
        """
        
        # Execute the GraphQL query
        result = self.execute_graphql(query)
        
        # Process the results
        sources = []
        if result and "data" in result and "listIngestionSources" in result["data"]:
            response_data = result["data"]["listIngestionSources"]
            raw_sources = response_data.get("sources", [])
            
            logger.debug(f"Retrieved {len(raw_sources)} ingestion sources from GraphQL")
            
            for source in raw_sources:
                try:
                    # Parse the config which is a string containing JSON
                    config = json.loads(source.get("config", "{}"))
                    
                    # Extract the source_id from the URN
                    urn = source.get("urn", "")
                    source_id = urn.split(":")[-1] if urn else ""
                    
                    # Create a simplified source object
                    simplified_source = {
                        "urn": urn,
                        "source_id": source_id,
                        "name": source.get("name", ""),
                        "type": source.get("type", ""),
                        "config": config,
                        "schedule": source.get("schedule", {})
                    }
                    
                    sources.append(simplified_source)
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse config for source {source.get('name')}: {str(e)}")
                except Exception as e:
                    logger.warning(f"Error processing source {source.get('name')}: {str(e)}")
        
        elif "errors" in result:
            errors = result.get("errors", [])
            error_msgs = [error.get("message", "Unknown error") for error in errors]
            logger.warning(f"GraphQL errors when listing sources: {', '.join(error_msgs)}")
            
            # If GraphQL fails, try to fetch a known source as fallback
            logger.debug("Attempting to fetch a known source as fallback")
            try:
                known_source = self.get_ingestion_source("analytics-database-prod")
                if known_source:
                    logger.debug(f"Retrieved single source: {known_source.get('source_id')}")
                    sources = [known_source]
            except Exception as e:
                logger.debug(f"Fallback source retrieval also failed: {str(e)}")
        
        logger.info(f"Retrieved {len(sources)} ingestion sources")
        return sources
    
    def get_ingestion_source(self, source_id: str) -> Optional[Dict[str, Any]]:
        """
        Get details of a specific ingestion source by ID.
        
        Args:
            source_id: ID of the ingestion source to retrieve
            
        Returns:
            Dictionary with ingestion source details or None if not found
        """
        logger.info(f"Fetching ingestion source with ID: {source_id}")
        source_urn = f"urn:li:dataHubIngestionSource:{source_id}"
        url = f"{self.server_url}/api/v2/graphql"
        logger.debug(f"Request URL: {url}")
        
        # First try GraphQL
        query = """
        query getIngestionSource($urn: String!) {
            ingestionSource(urn: $urn) {
                urn
                type
                name
                config
                schedule {
                    interval
                    timezone
                }
            }
        }
        """
        
        variables = {
            "urn": source_urn
        }
        
        try:
            result = self.execute_graphql(query, variables)
            
            if "errors" not in result and "data" in result and "ingestionSource" in result["data"]:
                source = result["data"]["ingestionSource"]
                
                if not source:
                    logger.warning(f"Ingestion source not found with ID: {source_id}")
                else:
                    source_info = {
                        "source_id": source_id,
                        "urn": source["urn"],
                        "type": source["type"],
                        "name": source["name"],
                        "config": source["config"]
                    }
                    
                    if source.get("schedule"):
                        source_info["schedule"] = {
                            "interval": source["schedule"]["interval"],
                            "timezone": source["schedule"]["timezone"]
                        }
                    
                    logger.info(f"Successfully retrieved ingestion source: {source_id} via GraphQL")
                    return source_info
            else:
                if "errors" in result:
                    logger.warning(f"GraphQL errors retrieving ingestion source: {result['errors']}")
                else:
                    logger.warning(f"Unexpected response format when retrieving ingestion source via GraphQL")
                
                # GraphQL failed, try REST API
                logger.info(f"Falling back to REST API to retrieve ingestion source: {source_id}")
        except Exception as e:
            logger.warning(f"Error retrieving ingestion source via GraphQL: {str(e)}")
            # Continue to REST API fallback
        
        # REST API fallback
        try:
            logger.info(f"Fetching ingestion source with ID: {source_id} via REST API")
            rest_url = f"{self.server_url}/openapi/v3/entity/datahubingestionsource/{source_urn}"
            
            response = requests.get(
                rest_url,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 404:
                logger.warning(f"Ingestion source not found with ID: {source_id}")
                return None
            elif response.status_code != 200:
                logger.warning(f"Error retrieving ingestion source via REST API: {response.status_code} - {response.text}")
                return None
            
            try:
                data = response.json()
                if not data:
                    logger.warning(f"Empty response when retrieving source: {source_id}")
                    return None
                
                # Parse the response
                source_info = {
                    "source_id": source_id,
                    "urn": source_urn,
                }
                
                # Extract source info from the response
                if "dataHubIngestionSourceInfo" in data and "value" in data["dataHubIngestionSourceInfo"]:
                    info = data["dataHubIngestionSourceInfo"]["value"]
                    source_info["name"] = info.get("name", source_id)
                    source_info["type"] = info.get("type", "unknown")
                    
                    # Extract schedule
                    if "schedule" in info:
                        source_info["schedule"] = {
                            "interval": info["schedule"].get("interval", "0 0 * * *"),
                            "timezone": info["schedule"].get("timezone", "UTC")
                        }
                    
                    # Extract config including recipe
                    if "config" in info:
                        config = info["config"]
                        if "recipe" in config:
                            try:
                                source_info["recipe"] = json.loads(config["recipe"])
                            except json.JSONDecodeError:
                                logger.warning(f"Failed to parse recipe JSON for source {source_id}")
                                source_info["recipe"] = {}
                
                logger.info(f"Successfully retrieved ingestion source: {source_id} via REST API")
                return source_info
            except json.JSONDecodeError:
                logger.error(f"Failed to parse JSON response for source {source_id}")
                return None
        except Exception as e:
            logger.error(f"Error retrieving ingestion source {source_id} via REST API: {str(e)}")
            return None
            
    def delete_ingestion_source(self, source_id: str) -> bool:
        """
        Delete a DataHub ingestion source by ID

        Args:
            source_id: Ingestion source ID

        Returns:
            True if successful, False otherwise
        """
        source_urn = f"urn:li:dataHubIngestionSource:{source_id}"
        
        query = """
            mutation deleteIngestionSource($urn: String!) {
                deleteIngestionSource(urn: $urn)
            }
        """
        
        variables = {
            "urn": source_urn
        }
        
        try:
            logger.info(f"Deleting ingestion source: {source_id}")
            
            # Use the execute_graphql method which will use datahubgraph if available
            result = self.execute_graphql(query, variables)
            
            if "errors" in result:
                logger.error(f"GraphQL errors: {result['errors']}")
                return False
            
            logger.info(f"Ingestion source deleted: {source_urn}")
            return True
        except Exception as e:
            logger.error(f"Error deleting ingestion source: {str(e)}")
            return False
    
    def create_secret(self, name: str, value: str) -> bool:
        """
        Create a secret in DataHub

        Args:
            name: Secret name
            value: Secret value

        Returns:
            True if successful, False otherwise
        """
        # Try the OpenAPI v3 endpoint
        try:
            logger.info(f"Creating secret with name: {name} using OpenAPI v3")
            
            # Create a unique ID for the secret based on the name
            secret_id = name.lower().replace(" ", "_")
            secret_urn = f"urn:li:dataHubSecret:{secret_id}"
            
            payload = [{
                "urn": secret_urn,
                "dataHubSecretValue": {
                    "value": {
                        "name": name,
                        "value": value,
                        "description": f"Secret created by DataHub Recipe Manager: {name}"
                    }
                },
                "dataHubSecretKey": {
                    "value": {
                        "id": secret_id
                    }
                }
            }]
            
            response = requests.post(
                f"{self.server_url}/openapi/v3/entity/datahubsecret",
                headers=self.headers,
                json=payload,
                timeout=10
            )
            
            if response.status_code in (200, 201, 202):
                logger.info(f"Secret created successfully using OpenAPI v3: {name}")
                return True
            else:
                logger.warning(f"Failed to create secret using OpenAPI v3: {response.status_code} - {response.text}")
        except Exception as e:
            logger.warning(f"Error creating secret using OpenAPI v3: {str(e)}")
        
        # If OpenAPI v3 fails, try OpenAPI endpoint
        try:
            logger.info(f"Creating secret with name: {name} using OpenAPI")
            
            # Check if secret already exists
            check_response = requests.get(
                f"{self.server_url}/openapi/secrets/{name}",
                headers=self.headers,
                timeout=10
            )
            
            method = "put" if check_response.status_code == 200 else "post"
            
            # Create or update the secret
            rest_endpoint = f"{self.server_url}/openapi/secrets" if method == "post" else f"{self.server_url}/openapi/secrets/{name}"
            
            payload = {
                "name": name,
                "value": value,
                "description": f"Secret created by DataHub Recipe Manager: {name}"
            }
            
            response = getattr(requests, method)(
                rest_endpoint,
                headers=self.headers,
                json=payload,
                timeout=10
            )
            
            if response.status_code in (200, 201):
                logger.info(f"Secret created successfully using OpenAPI: {name}")
                return True
            else:
                logger.warning(f"Failed to create secret using OpenAPI: {response.status_code} - {response.text}")
        except Exception as e:
            logger.warning(f"Error creating secret using OpenAPI: {str(e)}")
        
        # If REST API fails, try GraphQL
        query = """
            mutation createSecret($input: CreateSecretInput!) {
              createSecret(input: $input)
            }
        """
        
        variables = {
            "input": {
                "name": name,
                "value": value
            }
        }
        
        try:
            logger.info(f"Creating secret with name: {name} using GraphQL")
            result = self.execute_graphql(query, variables)
            
            if "errors" not in result:
                logger.info(f"Secret created successfully using GraphQL: {name}")
                return True
            else:
                logger.warning(f"GraphQL errors while creating secret: {result['errors']}")
        except Exception as e:
            logger.warning(f"Error creating secret using GraphQL: {str(e)}")
        
        # We've tried all endpoints and none worked
        logger.error("Failed to create secret. The API endpoints may not be enabled or available.")
        logger.error("Please check your DataHub instance configuration or consult the documentation.")
        return False

    def list_secrets(self, start: int = 0, count: int = 100) -> List[Dict[str, Any]]:
        """
        List all secrets available in DataHub

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

    def delete_secret(self, name_or_urn: str) -> bool:
        """
        Delete a secret from DataHub

        Args:
            name_or_urn: Secret name or URN

        Returns:
            True if successful, False otherwise
        """
        # Convert name to URN if needed
        if not name_or_urn.startswith("urn:li:dataHubSecret:"):
            urn = f"urn:li:dataHubSecret:{name_or_urn.lower().replace(' ', '_')}"
        else:
            urn = name_or_urn
        
        # Try GraphQL mutation first
        query = """
            mutation deleteSecret($urn: String!) {
              deleteSecret(urn: $urn)
            }
        """
        
        variables = {
            "urn": urn
        }
        
        try:
            logger.info(f"Deleting secret {name_or_urn} using GraphQL")
            result = self.execute_graphql(query, variables)
            
            if "errors" not in result:
                logger.info(f"Secret deleted successfully: {name_or_urn}")
                return True
            else:
                logger.warning(f"GraphQL errors while deleting secret: {result['errors']}")
        except Exception as e:
            logger.warning(f"Error deleting secret using GraphQL: {str(e)}")
        
        # Try OpenAPI v3 endpoint as fallback
        try:
            logger.info(f"Deleting secret {name_or_urn} using OpenAPI v3")
            
            response = requests.delete(
                f"{self.server_url}/openapi/v3/entity/datahubsecret/{urn}",
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code in (200, 201, 202, 204):
                logger.info(f"Secret deleted successfully using OpenAPI v3: {name_or_urn}")
                return True
            else:
                logger.warning(f"Failed to delete secret using OpenAPI v3: {response.status_code} - {response.text}")
        except Exception as e:
            logger.warning(f"Error deleting secret using OpenAPI v3: {str(e)}")
        
        # We've tried all endpoints and none worked
        logger.error(f"Failed to delete secret {name_or_urn}. The API endpoints may not be enabled or available.")
        return False

    def update_secret(self, name: str, value: str, description: Optional[str] = None) -> bool:
        """
        Update an existing secret in DataHub using GraphQL

        Args:
            name: Secret name
            value: New secret value
            description: Optional description for the secret

        Returns:
            True if successful, False otherwise
        """
        # Convert name to URN
        secret_id = name.lower().replace(" ", "_")
        urn = f"urn:li:dataHubSecret:{secret_id}"
        
        if description is None:
            description = f"Secret updated by DataHub Recipe Manager: {name}"
        
        # Try GraphQL mutation first
        query = """
            mutation updateSecret($input: UpdateSecretInput!) {
              updateSecret(input: $input)
            }
        """
        
        variables = {
            "input": {
                "urn": urn,
                "name": name,
                "value": value,
                "description": description
            }
        }
        
        try:
            logger.info(f"Updating secret {name} using GraphQL")
            result = self.execute_graphql(query, variables)
            
            if "errors" not in result:
                logger.info(f"Secret updated successfully using GraphQL: {name}")
                return True
            else:
                logger.warning(f"GraphQL errors while updating secret: {result['errors']}")
                # If GraphQL fails, fall back to create_secret which tries multiple methods
        except Exception as e:
            logger.warning(f"Error updating secret using GraphQL: {str(e)}")
            # Fall back to create_secret
        
        # Fall back to create_secret which tries multiple endpoints
        logger.info(f"Falling back to create_secret for updating {name}")
        return self.create_secret(name, value)

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
            Update (patch) an existing ingestion source with new configuration parameters.
            This method preserves existing values for fields that are not provided.
            
            Args:
                source_id: ID of the ingestion source to update
                recipe_config: New recipe configuration (optional)
                schedule: New schedule configuration (optional, dict with interval and timezone)
                name: New name for the ingestion source (optional)
                executor_id: New executor ID (optional)
                debug_mode: Enable or disable debug mode (optional)
                
            Returns:
                Boolean indicating success or failure
            """
            logger.info(f"Patching ingestion source with ID: {source_id}")
            source_urn = f"urn:li:dataHubIngestionSource:{source_id}"
            
            if not any([recipe_config, schedule, name, executor_id, debug_mode is not None]):
                logger.warning("No updates provided for patching ingestion source")
                return False
            
            # First get the current source to ensure it exists and to preserve existing values
            current_source = self.get_ingestion_source(source_id)
            if not current_source:
                logger.error(f"Cannot patch ingestion source with ID {source_id} - source not found")
                return False
            
            # Try using the REST API first
            try:
                logger.info(f"Patching ingestion source {source_id} using REST API")
                
                # Get existing values to merge with new values
                current_name = current_source.get("name", source_id)
                current_type = current_source.get("type", "unknown")
                current_recipe = current_source.get("recipe", {})
                current_schedule = current_source.get("schedule", {"interval": "0 0 * * *", "timezone": "UTC"})
                
                # Merge with provided values
                patch_name = name if name is not None else current_name
                patch_recipe = recipe_config if recipe_config is not None else current_recipe
                patch_schedule = {
                    "interval": schedule.get("interval", current_schedule.get("interval", "0 0 * * *")) if schedule else current_schedule.get("interval", "0 0 * * *"),
                    "timezone": schedule.get("timezone", current_schedule.get("timezone", "UTC")) if schedule else current_schedule.get("timezone", "UTC")
                }
                patch_executor = executor_id if executor_id is not None else "default"
                patch_debug = debug_mode if debug_mode is not None else False
                
                # Prepare the payload for the REST API
                payload = [{
                    "urn": source_urn,
                    "dataHubIngestionSourceInfo": {
                        "value": {
                            "name": patch_name,
                            "type": current_type,
                            "schedule": patch_schedule,
                            "config": {
                                "recipe": json.dumps(patch_recipe) if isinstance(patch_recipe, dict) else patch_recipe,
                                "executorId": patch_executor,
                                "debugMode": patch_debug,
                                "extraArgs": {}
                            }
                        }
                    }
                }]
                
                response = requests.put(
                    f"{self.server_url}/openapi/v3/entity/datahubingestionsource",
                    headers=self.headers,
                    json=payload,
                    timeout=30
                )
                
                if response.status_code in (200, 201, 202):
                    logger.info(f"Successfully patched ingestion source {source_id} using REST API")
                    return True
                else:
                    logger.warning(f"Failed to patch ingestion source using REST API: {response.status_code} - {response.text}")
                    # Continue to GraphQL fallback
            except Exception as e:
                logger.warning(f"Error patching ingestion source using REST API: {str(e)}")
                # Continue to GraphQL fallback
            
            # Try GraphQL as fallback
            try:
                logger.info(f"Patching ingestion source {source_id} using GraphQL")
                
                # Prepare variables for the GraphQL mutation
                mutation = """
                mutation updateIngestionSource($urn: String!, $input: UpdateIngestionSourceInput!) {
                    updateIngestionSource(urn: $urn, input: $input) {
                        urn
                    }
                }
                """
                
                # Prepare the input variables
                variables = {
                    "urn": source_urn,
                    "input": {}
                }
                
                # Add fields that are being updated
                if name is not None:
                    variables["input"]["name"] = name
                
                if recipe_config is not None:
                    variables["input"]["config"] = {
                        "recipe": json.dumps(recipe_config) if isinstance(recipe_config, dict) else recipe_config,
                        "executorId": executor_id if executor_id is not None else "default",
                        "debugMode": debug_mode if debug_mode is not None else False
                    }
                
                if schedule is not None:
                    variables["input"]["schedule"] = {
                        "interval": schedule.get("interval", "0 0 * * *"),
                        "timezone": schedule.get("timezone", "UTC")
                    }
                
                result = self.execute_graphql(mutation, variables)
                
                if "errors" in result:
                    logger.error(f"GraphQL errors when patching ingestion source: {result['errors']}")
                    return False
                
                logger.info(f"Successfully patched ingestion source: {source_id} using GraphQL")
                return True
            except Exception as e:
                logger.error(f"Error patching ingestion source {source_id} using GraphQL: {str(e)}")
                return False
    
    def run_ingestion_source(self, source_id: str) -> bool:
        """
        Trigger an ingestion source to run immediately.
        
        Args:
            source_id (str): The ID of the ingestion source to run
            
        Returns:
            bool: True if the ingestion source was successfully triggered, False otherwise
        """
        self.logger.info(f"Triggering immediate run for ingestion source: {source_id}")
        
        # Try the various endpoints in order of newest to oldest
        
        # Try newest /runs endpoint (most recent DataHub versions)
        runs_url = f"{self.server_url}/openapi/v3/ingestion/sources/{source_id}/runs"
        try:
            self.logger.debug(f"POST {runs_url}")
            response = requests.post(runs_url, headers=self.headers)
            
            if response.status_code in (200, 201, 202, 204):
                self.logger.info(f"Successfully triggered ingestion source: {source_id} using /runs endpoint")
                return True
            else:
                self.logger.warning(f"Failed to run ingestion source using /runs endpoint: {response.status_code} - {response.text}")
                # Continue to next endpoint
        except Exception as e:
            self.logger.warning(f"Error triggering ingestion source using /runs endpoint: {str(e)}")
            # Continue to next endpoint
        
        # Try direct /ingest/{id} endpoint
        ingest_url = f"{self.server_url}/actions/ingest/{source_id}"
        try:
            self.logger.debug(f"POST {ingest_url}")
            response = requests.post(ingest_url, headers=self.headers)
            
            if response.status_code in (200, 201, 202, 204):
                self.logger.info(f"Successfully triggered ingestion source: {source_id} using /actions/ingest endpoint")
                return True
            else:
                self.logger.warning(f"Failed to run ingestion source using /actions/ingest endpoint: {response.status_code} - {response.text}")
                # Continue to next endpoint
        except Exception as e:
            self.logger.warning(f"Error triggering ingestion source using /actions/ingest endpoint: {str(e)}")
            # Continue to next endpoint
        
        # Try legacy ?action=ingest endpoint
        legacy_url = f"{self.server_url}/actions?action=ingest&runId={source_id}"
        try:
            self.logger.debug(f"POST {legacy_url}")
            response = requests.post(legacy_url, headers=self.headers)
            
            if response.status_code in (200, 201, 202, 204):
                self.logger.info(f"Successfully triggered ingestion source: {source_id} using legacy endpoint")
                return True
            else:
                self.logger.warning(f"Failed to run ingestion source using legacy endpoint: {response.status_code} - {response.text}")
                # Try GraphQL as final fallback
        except Exception as e:
            self.logger.warning(f"Error triggering ingestion source using legacy endpoint: {str(e)}")
            # Try GraphQL as final fallback
        
        # Try GraphQL as final fallback
        self.logger.info(f"Attempting to run ingestion source using GraphQL fallback")
        source_urn = f"urn:li:dataHubIngestionSource:{source_id}"
        mutation = """
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
        
        try:
            result = self.execute_graphql(mutation, variables)
            
            if "errors" in result:
                self.logger.error(f"GraphQL errors when running ingestion source: {result['errors']}")
                return False
                
            # Check if we got an execution ID back
            if result.get("data", {}).get("executeIngestionSource", {}).get("executionId"):
                execution_id = result["data"]["executeIngestionSource"]["executionId"]
                self.logger.info(f"Successfully triggered ingestion source: {source_id} using GraphQL, execution ID: {execution_id}")
                return True
            else:
                # If we got a successful response but no execution ID, still consider it successful
                self.logger.info(f"Successfully triggered ingestion source: {source_id} using GraphQL (no execution ID returned)")
                return True
        except Exception as e:
            self.logger.error(f"All attempts to trigger ingestion source failed: {str(e)}")
            return False 