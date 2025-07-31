#!/usr/bin/env python3
"""
Script to push DataHub ingestion recipes to a DataHub instance.
Uses GraphQL mutations for creating ingestion sources, secrets, and triggering ingestion.
"""

import argparse
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Dict, Any, Optional, List

import requests
import yaml
from dotenv import load_dotenv

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))
from utils.template_renderer import render_template
from utils.datahub_rest_client import DataHubRestClient
from utils.recipe_util import apply_docker_networking


def load_yaml_file(file_path: str) -> Dict[str, Any]:
    """Load YAML file and return as dictionary."""
    with open(file_path, "r") as f:
        return yaml.safe_load(f)


def resolve_secrets(config: Dict[str, Any], env_vars: Dict[str, str]) -> Dict[str, Any]:
    """
    Resolve secrets referenced in the config from environment variables.
    """

    def _resolve_value(value):
        if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            env_var = value[2:-1]
            if env_var in env_vars:
                return env_vars[env_var]
            else:
                raise ValueError(f"Environment variable {env_var} not found")
        elif isinstance(value, dict):
            return {k: _resolve_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [_resolve_value(v) for v in value]
        else:
            return value

    return {k: _resolve_value(v) for k, v in config.items()}


def log_http_response(response, debug_mode: bool = False) -> None:
    """
    Log the HTTP response details for debugging.
    
    Args:
        response: requests.Response object
        debug_mode: Whether to show detailed debug information
    """
    if not debug_mode:
        return
        
    print(f"    📥 HTTP RESPONSE DETAILS:")
    print(f"       Status: {response.status_code} {response.reason}")
    print(f"       Headers:")
    for key, value in response.headers.items():
        print(f"         {key}: {value}")
    
    try:
        response_json = response.json()
        print(f"       JSON Body: ({len(response.text)} characters)")
        if len(response.text) > 1000:
            print(f"         {json.dumps(response_json, indent=2)[:500]}...")
            print(f"         ...{response.text[-200:]}")
        else:
            print(f"         {json.dumps(response_json, indent=2)}")
    except json.JSONDecodeError:
        print(f"       Text Body: ({len(response.text)} characters)")
        if len(response.text) > 500:
            print(f"         {response.text[:250]}...")
            print(f"         ...{response.text[-250:]}")
        else:
            print(f"         {response.text}")
    print()


def log_http_request(method: str, url: str, headers: Dict, data: str = None, params: Dict = None, debug_mode: bool = False) -> None:
    """
    Log the exact HTTP request being made for debugging purposes.
    
    Args:
        method: HTTP method (GET, POST, etc.)
        url: Full URL
        headers: Request headers
        data: Request body (for POST requests)
        params: URL parameters
        debug_mode: Whether to show detailed debug information
    """
    if not debug_mode:
        return
        
    print(f"    🌐 HTTP REQUEST DETAILS:")
    print(f"       Method: {method}")
    print(f"       URL: {url}")
    
    if params:
        print(f"       Params: {params}")
    
    print(f"       Headers:")
    for key, value in headers.items():
        # Mask sensitive headers
        if key.lower() in ['authorization']:
            if value and value.startswith('Bearer '):
                masked_value = f"Bearer {value[7:15]}...{value[-8:]}" if len(value) > 23 else "Bearer [MASKED]"
            else:
                masked_value = "[MASKED]"
            print(f"         {key}: {masked_value}")
        else:
            print(f"         {key}: {value}")
    
    if data:
        print(f"       Body: ({len(data)} characters)")
        # Show first 500 and last 200 characters for very long bodies
        if len(data) > 1000:
            print(f"         {data[:500]}...")
            print(f"         ...{data[-200:]}")
        else:
            print(f"         {data}")
    
    # Generate curl equivalent
    curl_cmd = f"curl -X {method}"
    
    for key, value in headers.items():
        if key.lower() == 'authorization':
            curl_cmd += f" -H '{key}: [MASKED]'"
        else:
            curl_cmd += f" -H '{key}: {value}'"
    
    if data:
        # Escape quotes in data for curl
        escaped_data = data.replace("'", "'\"'\"'")
        curl_cmd += f" -d '{escaped_data}'"
    
    if params:
        url_with_params = url + "?" + "&".join([f"{k}={v}" for k, v in params.items()])
        curl_cmd += f" '{url_with_params}'"
    else:
        curl_cmd += f" '{url}'"
    
    print(f"    📋 CURL EQUIVALENT:")
    print(f"       {curl_cmd}")
    print()



def list_existing_sources(datahub_config: Dict[str, str], debug_mode: bool = False) -> None:
    """
    List existing ingestion sources for debugging purposes.
    """
    try:
        print(f"\n📋 Listing existing ingestion sources...")
        
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json"
        }
        
        if datahub_config["token"]:
            headers["Authorization"] = f"Bearer {datahub_config['token']}"
        
        url = f"{datahub_config['server']}/openapi/v3/entity/datahubingestionsource"
        params = {"count": 20}
        log_http_request("GET", url, headers, params=params, debug_mode=debug_mode)
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            sources = response.json()
            entities = sources.get('entities', [])
            print(f"    Found {len(entities)} existing ingestion sources:")
            
            for i, source in enumerate(entities[:10]):  # Show first 10
                source_info = source.get('dataHubIngestionSourceInfo', {}).get('value', {})
                source_key = source.get('dataHubIngestionSourceKey', {}).get('value', {})
                
                name = source_info.get('name', 'Unknown')
                source_type = source_info.get('type', 'Unknown')
                source_id = source_key.get('id', 'Unknown')
                urn = source.get('urn', 'Unknown')
                
                print(f"    {i+1:2d}. {name} ({source_type}) - ID: {source_id}")
                print(f"        URN: {urn}")
            
            if len(entities) > 10:
                print(f"    ... and {len(entities) - 10} more")
        else:
            print(f"    ❌ Failed to list sources (status: {response.status_code}): {response.text}")
            
    except Exception as e:
        print(f"    ❌ Error listing sources: {str(e)}")


def create_datahub_recipe(
    recipe_config: Dict[str, Any],
    datahub_config: Dict[str, str],
    source_id: Optional[str] = None,
    run_ingestion: bool = False,
    create_secrets: bool = False,
    secret_references: Optional[List[str]] = None,
    debug_mode: bool = False,
) -> Dict:
    """
    Create a DataHub ingestion recipe using GraphQL mutations

    Args:
        recipe_config: The full template configuration (including executor_id and schedule)
        datahub_config: DataHub connection configuration
        source_id: Optional source ID
        run_ingestion: Whether to trigger immediate ingestion
        create_secrets: Whether to create secrets in DataHub
        secret_references: List of secret references to create

    Returns:
        Dictionary with source information
    """
    if debug_mode:
        print(f"🔄 Processing recipe configuration...")
    
    # Separate recipe content from executor/schedule configuration
    # The recipe should only contain source/sink configuration
    recipe_only = {}
    executor_config = {}
    schedule_config = {}
    
    # Extract the core recipe configuration (source, sink, etc.)
    for key, value in recipe_config.items():
        if key == "executor_id":
            executor_config["executor_id"] = value
        elif key == "schedule":
            # Handle schedule as dict with cron/timezone or interval/timezone
            if isinstance(value, dict):
                if "cron" in value:
                    schedule_config["interval"] = value["cron"]
                    schedule_config["timezone"] = value.get("timezone", "UTC")
                else:
                    schedule_config["interval"] = value.get("interval", "0 0 * * *")
                    schedule_config["timezone"] = value.get("timezone", "UTC")
            else:
                schedule_config["interval"] = "0 0 * * *"
                schedule_config["timezone"] = "UTC"
        elif key not in ["executorId", "debug_mode", "extraArgs"]:
            # Include all recipe content (source, sink, transformers, etc.)
            recipe_only[key] = value
    
    # Extract additional configuration
    executor_id = executor_config.get("executor_id", recipe_config.get("executorId", "default"))
    debug_mode = recipe_config.get("debug_mode", False)
    extra_args = recipe_config.get("extraArgs", {})
    
    # Default schedule if not specified
    if not schedule_config:
        schedule_config = {"interval": "0 0 * * *", "timezone": "UTC"}
    
    # Extract source type and generate pipeline name
    source_type = recipe_only.get("source", {}).get("type", "unknown")
    pipeline_name = (
        recipe_only.get("source", {})
        .get("config", {})
        .get("name", f"DataHub {source_type.capitalize()} Ingestion")
    )
    
    if debug_mode:
        print(f"📊 Recipe details:")
        print(f"   Source type: {source_type}")
        print(f"   Pipeline name: {pipeline_name}")
        print(f"   Executor ID: {executor_id}")
        print(f"   Schedule: {schedule_config['interval']} ({schedule_config['timezone']})")
        print(f"   Debug mode: {debug_mode}")
    
    # Apply Docker networking to recipe content only
    recipe_only = apply_docker_networking(recipe_only)

    try:
        # Create DataHub client with or without token
        if debug_mode:
            print(f"🔗 Connecting to DataHub at {datahub_config['server']}...")
            if datahub_config["token"]:
                print("   Using token authentication")
            else:
                print("   No authentication token provided")
                
        if datahub_config["token"]:
            client = DataHubRestClient(
                datahub_config["server"], datahub_config["token"]
            )
        else:
            client = DataHubRestClient(datahub_config["server"])

        # Create secrets if requested
        if create_secrets and secret_references:
            print("\n🔑 Creating secrets in DataHub...")
            secret_created = False
            failed_secrets = []
            successful_secrets = []

            # Filter out DataHub connection credentials - these shouldn't be stored as secrets
            excluded_secrets = ["DATAHUB_GMS_URL", "DATAHUB_TOKEN"]
            filtered_secrets = [
                s for s in secret_references if s not in excluded_secrets
            ]

            if not filtered_secrets:
                if debug_mode:
                    print("⚠️  No valid secrets to create. Skipping secret creation.")
                    print("    Note: DATAHUB_GMS_URL and DATAHUB_TOKEN are automatically excluded as they are")
                    print("    connection parameters, not ingestion secrets.")
            else:
                if debug_mode:
                    print(f"    Processing {len(filtered_secrets)} secrets...")

            for secret_name in filtered_secrets:
                secret_value = os.environ.get(secret_name)
                if secret_value:
                    if debug_mode:
                        print(f"    Creating secret: {secret_name}")
                    
                    # Use GraphQL mutation for creating secrets
                    mutation = """
                    mutation createSecret($input: CreateSecretInput!) {
                        createSecret(input: $input)
                    }
                    """
                    
                    variables = {
                        "input": {
                            "name": secret_name,
                            "value": secret_value,
                            "description": f"Secret managed by datahub-recipes-manager for {pipeline_name}"
                        }
                    }
                    
                    try:
                        # Log the GraphQL request
                        graphql_request = {
                            "query": mutation,
                            "variables": variables
                        }
                        graphql_url = f"{datahub_config['server']}/api/graphql"
                        graphql_headers = {"Content-Type": "application/json"}
                        if datahub_config["token"]:
                            graphql_headers["Authorization"] = f"Bearer {datahub_config['token']}"
                        
                        log_http_request("POST", graphql_url, graphql_headers, data=json.dumps(graphql_request), debug_mode=debug_mode)
                        
                        result = client.execute_graphql(mutation, variables)
                        if result and "errors" not in result:
                            print(f"    ✅ Secret {secret_name} created successfully")
                            successful_secrets.append(secret_name)
                            secret_created = True
                        elif result and "errors" in result:
                            # Check if error is "secret already exists" which is fine
                            errors = result.get("errors", [])
                            already_exists = any(
                                "already exists" in str(error.get("message", "")).lower() 
                                for error in errors
                            )
                            
                            if already_exists:
                                print(f"    ℹ️  Secret {secret_name} already exists (this is fine)")
                                successful_secrets.append(secret_name)
                                secret_created = True
                            else:
                                error_msg = errors
                                print(f"    ❌ Failed to create secret {secret_name}: {error_msg}")
                                failed_secrets.append(secret_name)
                        else:
                            print(f"    ❌ Failed to create secret {secret_name}: No response")
                            failed_secrets.append(secret_name)
                    except Exception as e:
                        print(f"    ❌ Error creating secret {secret_name}: {str(e)}")
                        failed_secrets.append(secret_name)
                else:
                    print(f"    ⚠️  Skipping secret {secret_name} - not found in environment")

            if successful_secrets:
                print(f"    ✅ Successfully processed {len(successful_secrets)} secrets")
                if debug_mode:
                    print(f"       Secrets: {', '.join(successful_secrets)}")
            
            if failed_secrets:
                print(f"    ⚠️  Failed to create {len(failed_secrets)} secrets")
                if debug_mode:
                    print(f"       Failed secrets: {', '.join(failed_secrets)}")
                    print("       This may be because:")
                    print("       - Your DataHub instance doesn't have the secrets API enabled")
                    print("       - You don't have sufficient permissions to create secrets")
                    print("       - The secrets already exist")
                    print("       The recipe will still be pushed using environment variables.")

        # Create the ingestion source using GraphQL mutation
        print(f"\n📤 Creating ingestion source: {pipeline_name}")
        if debug_mode:
            print(f"    Source ID: {source_id}")
            print(f"    Source type: {source_type}")
        
        # Generate source URN
        source_urn = f"urn:li:dataHubIngestionSource:{source_id}"
        
        # Prepare the config object
        config = {
            "recipe": json.dumps(recipe_only),
            "executorId": executor_id,
            "debugMode": debug_mode
        }
        
        # Add extra args if provided
        if extra_args:
            config["extraArgs"] = extra_args
        else:
            config["extraArgs"] = []
        
        if debug_mode:
            print(f"    Recipe JSON length: {len(config['recipe'])} characters")
            print(f"    Schedule: {schedule_config['interval']} ({schedule_config['timezone']})")
            print(f"    Executor: {executor_id}")
            
            # Save config to file for manual inspection in debug mode only
            debug_file = f"debug_config_{source_id}.json"
            with open(debug_file, 'w') as f:
                json.dump({
                    "source_id": source_id,
                    "source_urn": source_urn,
                    "pipeline_name": pipeline_name,
                    "source_type": source_type,
                    "config": config,
                    "schedule": schedule_config,
                    "recipe_only": recipe_only
                }, f, indent=2)
            print(f"    📄 Configuration saved to {debug_file} for inspection")
        
        try:
            if debug_mode:
                print("    Executing GraphQL mutation...")
            
            # Use GraphQL mutation for creating ingestion source
            mutation = """
            mutation createIngestionSource($input: UpdateIngestionSourceInput!) {
                createIngestionSource(input: $input)
            }
            """
            
            variables = {
                "input": {
                    "type": source_type,
                    "name": pipeline_name,
                    "config": config,
                    "schedule": {
                        "interval": schedule_config["interval"],
                        "timezone": schedule_config["timezone"]
                    }
                }
            }
            
            # Prepare headers for GraphQL
            headers = {
                "Content-Type": "application/json"
            }
            
            if datahub_config["token"]:
                headers["Authorization"] = f"Bearer {datahub_config['token']}"
            
            # Prepare GraphQL request
            graphql_request = {
                "query": mutation,
                "variables": variables
            }
            
            # Log the exact HTTP request
            url = f"{datahub_config['server']}/api/graphql"
            data = json.dumps(graphql_request)
            log_http_request("POST", url, headers, data=data, debug_mode=debug_mode)
            
            # Make GraphQL request
            response = requests.post(url, headers=headers, json=graphql_request)
            
            # Log the response details
            log_http_response(response, debug_mode=debug_mode)
            
            response.raise_for_status()
            result = response.json()
            
            if "errors" in result:
                error_msg = result["errors"]
                print(f"    ❌ GraphQL mutation failed: {error_msg}")
                raise Exception(f"Failed to create ingestion source via GraphQL: {error_msg}")
            else:
                print(f"    ✅ Ingestion source created successfully!")
                creation_response = result.get("data", {}).get("createIngestionSource")
                
                if not debug_mode:
                    print(f"    Pipeline: {pipeline_name}")
                    print(f"    Schedule: {schedule_config['interval']} ({schedule_config['timezone']})")
                    print(f"    Executor: {executor_id}")
                
                # Return success response
                source_result = {
                    "id": source_id,
                    "name": pipeline_name,
                    "type": source_type,
                    "status": "created",
                    "config": config,
                    "schedule": schedule_config,
                    "response": creation_response
                }
                
        except requests.exceptions.RequestException as e:
            print(f"    ❌ GraphQL request failed: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"    Response status: {e.response.status_code}")
                print(f"    Response text: {e.response.text}")
            raise Exception(f"Failed to create ingestion source via GraphQL: {str(e)}")
        except Exception as e:
            print(f"    ❌ Error executing GraphQL mutation: {str(e)}")
            raise Exception(f"Failed to create ingestion source: {str(e)}")

        # Trigger immediate ingestion if requested
        if run_ingestion:
            print(f"\n🚀 Triggering immediate ingestion for source: {source_id}")
            print(f"    ⚠️  Note: Since we don't know the actual URN (it uses UUID), ingestion triggering may fail")
            print(f"    You can manually trigger ingestion from the DataHub UI if needed")
            
            try:
                # Use GraphQL mutation for triggering ingestion
                # Note: This may fail since we don't know the actual URN which uses a UUID
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
                
                # Prepare headers for GraphQL
                headers = {
                    "Content-Type": "application/json"
                }
                
                if datahub_config["token"]:
                    headers["Authorization"] = f"Bearer {datahub_config['token']}"
                
                print("    Executing GraphQL mutation for ingestion trigger...")
                
                # Log the GraphQL request
                url = f"{datahub_config['server']}/api/graphql"
                data = json.dumps(graphql_query)
                log_http_request("POST", url, headers, data=data, debug_mode=debug_mode)
                
                # Make GraphQL request
                response = requests.post(url, headers=headers, json=graphql_query)
                
                # Log the response
                log_http_response(response, debug_mode=debug_mode)
                
                response.raise_for_status()
                result = response.json()
                
                if "errors" in result:
                    error_msg = result["errors"]
                    print(f"    ❌ GraphQL errors (expected if URN doesn't match): {error_msg}")
                    print(f"    💡 You can manually trigger ingestion from the DataHub UI")
                    source_result["ingestion_triggered"] = False
                else:
                    print(f"    ✅ Ingestion triggered successfully!")
                    source_result["ingestion_triggered"] = True
                    
            except requests.exceptions.RequestException as e:
                print(f"    ❌ Error triggering ingestion (HTTP): {str(e)}")
                print(f"    💡 You can manually trigger ingestion from the DataHub UI")
                if hasattr(e, 'response') and e.response is not None:
                    print(f"    Response status: {e.response.status_code}")
                    print(f"    Response text: {e.response.text}")
                source_result["ingestion_triggered"] = False
            except Exception as e:
                print(f"    ❌ Error triggering ingestion: {str(e)}")
                print(f"    💡 You can manually trigger ingestion from the DataHub UI")
                source_result["ingestion_triggered"] = False

        return source_result

    except Exception as e:
        print(f"\n❌ Error creating/updating recipe: {str(e)}")
        raise


def main():
    parser = argparse.ArgumentParser(description="Push DataHub ingestion recipes using GraphQL mutations")
    parser.add_argument(
        "--instance", required=True, help="Path to the instance YAML file"
    )
    parser.add_argument("--env-file", help="Path to .env file for secrets")
    parser.add_argument(
        "--run-ingestion", action="store_true", help="Trigger immediate ingestion"
    )
    parser.add_argument(
        "--force", action="store_true", help="Force update if recipe exists"
    )
    parser.add_argument(
        "--create-secrets", action="store_true", help="Create secrets in DataHub"
    )
    parser.add_argument(
        "--debug", action="store_true", help="Enable verbose debug output including full payload"
    )

    args = parser.parse_args()

    print("🚀 DataHub Recipe Push Tool")
    print("=" * 50)
    print(f"Instance file: {args.instance}")
    print(f"Environment file: {args.env_file or 'default (.env)'}")
    print(f"Create secrets: {args.create_secrets}")
    print(f"Run ingestion: {args.run_ingestion}")
    print(f"Debug mode: {args.debug}")
    print("=" * 50)

    # Load environment variables
    if args.env_file:
        print(f"📄 Loading environment variables from: {args.env_file}")
        load_dotenv(args.env_file)
    else:
        print("📄 Loading environment variables from default .env file")
        load_dotenv()

    # Load instance configuration
    print(f"📋 Loading instance configuration from: {args.instance}")
    instance_config = load_yaml_file(args.instance)

    # Get template path
    template_type = instance_config.get("recipe_type")
    if not template_type:
        raise ValueError("Recipe type not specified in instance configuration")

    base_dir = Path(__file__).parent.parent
    template_path = base_dir / "recipes" / "templates" / f"{template_type}.yml"

    if not template_path.exists():
        raise FileNotFoundError(f"Template file not found: {template_path}")

    print(f"📝 Using template: {template_path}")
    print(f"   Template type: {template_type}")

    # Render template with parameters
    parameters = instance_config.get("parameters", {})
    print(f"🔧 Rendering template with {len(parameters)} parameters")
    rendered_template = render_template(str(template_path), parameters)

    # Check for required environment variables
    print("\n🔍 Checking DataHub connection configuration...")
    datahub_config = {
        "server": os.environ.get("DATAHUB_GMS_URL", ""),
        "token": os.environ.get("DATAHUB_TOKEN", ""),
    }

    if not datahub_config["server"]:
        raise ValueError("DATAHUB_GMS_URL environment variable must be set")

    print(f"   DataHub server: {datahub_config['server']}")

    # Empty or default token is treated as not provided
    if (
        not datahub_config["token"]
        or datahub_config["token"] == "your_datahub_pat_token_here"
    ):
        print("   ⚠️  DATAHUB_TOKEN is not set. Will attempt to connect without authentication.")
        print("      This will only work if your DataHub instance doesn't require authentication.")
        datahub_config["token"] = None
    else:
        print(f"   Authentication: Token provided (length: {len(datahub_config['token'])})")

    # Check for secret references and ensure they're in the environment
    secret_refs = instance_config.get("secret_references", [])
    print(f"\n🔐 Checking secret references ({len(secret_refs)} configured)...")
    
    missing_secrets = []
    available_secrets = []

    for secret in secret_refs:
        if os.environ.get(secret):
            available_secrets.append(secret)
            print(f"   ✅ {secret}: Available")
        else:
            missing_secrets.append(secret)
            print(f"   ❌ {secret}: Missing")

    if missing_secrets:
        print(f"\n❌ Missing required environment variables: {', '.join(missing_secrets)}")
        print("   Please set these environment variables and try again.")
        raise ValueError(f"Missing required environment variables: {', '.join(missing_secrets)}")

    # Create source ID based on recipe_id if available
    source_id = instance_config.get("recipe_id", str(uuid.uuid4()))
    print(f"\n🆔 Source ID: {source_id}")

    # List existing sources first for reference (debug mode only)
    if args.debug:
        list_existing_sources(datahub_config, debug_mode=True)

    # Push recipe to DataHub using GraphQL mutations
    print(f"\n🎯 Pushing recipe to DataHub...")
    try:
        result = create_datahub_recipe(
            recipe_config=rendered_template,
            datahub_config=datahub_config,
            source_id=source_id,
            run_ingestion=args.run_ingestion,
            create_secrets=args.create_secrets,
            secret_references=secret_refs if args.create_secrets else None,
            debug_mode=args.debug,
        )

        print(f"\n🎉 SUCCESS! Recipe pushed successfully")
        print(f"   Recipe ID: {source_id}")
        print(f"   Pipeline Name: {result.get('name', 'N/A')}")
        print(f"   Status: {result.get('status', 'N/A')}")
        
        if result.get('ingestion_triggered'):
            print(f"   Ingestion: Triggered successfully")
        elif args.run_ingestion:
            print(f"   Ingestion: Failed to trigger")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ FAILED! Error pushing recipe: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
