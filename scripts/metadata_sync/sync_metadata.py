#!/usr/bin/env python3
"""
Sync metadata between DataHub environments (e.g., DEV to PROD).

This script exports metadata from a source environment and imports it into a target environment,
making it suitable for CICD workflows.
"""

import argparse
import json
import logging
import os
import sys
from typing import Dict, Any, List, Optional, Set, Tuple

# Add the parent directory to the sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.datahub_metadata_api import DataHubMetadataApiClient
from utils.token_utils import get_token_from_env
from utils.urn_utils import (
    generate_deterministic_urn,
    get_full_urn_from_name,
    extract_name_from_properties,
    get_parent_path
)

logger = logging.getLogger(__name__)


def setup_logging(log_level: str):
    """
    Set up logging configuration
    """
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def parse_args():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(description="Sync metadata between DataHub environments")
    
    parser.add_argument(
        "--source-url",
        required=True,
        help="Source DataHub server URL (e.g., http://dev-datahub:8080)",
    )
    
    parser.add_argument(
        "--target-url",
        required=True,
        help="Target DataHub server URL (e.g., http://prod-datahub:8080)",
    )
    
    parser.add_argument(
        "--source-token-file",
        help="File containing source DataHub access token",
    )
    
    parser.add_argument(
        "--target-token-file",
        help="File containing target DataHub access token",
    )
    
    parser.add_argument(
        "--entity-type",
        choices=["glossaryTerm", "glossaryNode", "tag", "domain", "all"],
        default="all",
        help="Type of entities to sync (default: all)",
    )
    
    parser.add_argument(
        "--entity-urns",
        help="Comma-separated list of specific entity URNs to sync",
    )
    
    parser.add_argument(
        "--config-file",
        help="JSON configuration file with sync settings",
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a dry run without making changes to the target environment",
    )
    
    parser.add_argument(
        "--output-file",
        help="Output file to save the sync results",
    )
    
    parser.add_argument(
        "--include-properties",
        action="store_true",
        help="Include structured properties in the sync",
    )
    
    parser.add_argument(
        "--include-relationships",
        action="store_true",
        help="Include related entities in the sync",
    )
    
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)",
    )
    
    parser.add_argument(
        "--use-deterministic-urns",
        action="store_true",
        help="Use deterministic URNs when syncing entities to ensure consistency across environments",
    )
    
    return parser.parse_args()


class MetadataSyncer:
    """Class for syncing metadata between DataHub environments"""
    
    def __init__(
        self,
        source_client: DataHubMetadataApiClient,
        target_client: DataHubMetadataApiClient,
        dry_run: bool = False,
        use_deterministic_urns: bool = True
    ):
        """
        Initialize the metadata syncer
        
        Args:
            source_client: Client for the source DataHub environment
            target_client: Client for the target DataHub environment
            dry_run: Whether to perform a dry run without making changes
            use_deterministic_urns: Whether to use deterministic URNs for syncing
        """
        self.source_client = source_client
        self.target_client = target_client
        self.dry_run = dry_run
        self.use_deterministic_urns = use_deterministic_urns
        
    def get_deterministic_urn(self, entity_data: Dict[str, Any], entity_type: str) -> Optional[str]:
        """
        Generate a deterministic URN for an entity based on its properties
        
        Args:
            entity_data: Entity data from DataHub
            entity_type: Type of the entity (glossaryTerm, glossaryNode, tag, domain)
            
        Returns:
            Deterministic URN for the entity, or None if cannot be generated
        """
        if not self.use_deterministic_urns:
            return entity_data.get("urn")
            
        # Extract the name from entity properties
        name = extract_name_from_properties(entity_data)
        if not name:
            logger.warning(f"Could not extract name from {entity_type} entity data")
            return entity_data.get("urn")
        
        # For hierarchical entities like glossary terms and nodes, get the parent path
        namespace = None
        if entity_type in ["glossaryTerm", "glossaryNode"]:
            parent_urn = get_parent_path(entity_data)
            if parent_urn:
                namespace = parent_urn
        
        # Generate a deterministic URN
        return get_full_urn_from_name(entity_type, name, namespace)
    
    def sync_glossary_term(self, term_urn: str) -> Dict[str, Any]:
        """
        Sync a glossary term from source to target
        
        Args:
            term_urn: URN of the glossary term to sync
            
        Returns:
            Dictionary with sync result
        """
        logger.info(f"Syncing glossary term: {term_urn}")
        
        try:
            # Export term from source
            term_data = self.source_client.export_glossary_term(term_urn)
            if not term_data:
                return {
                    "status": "error",
                    "message": f"Failed to export glossary term from source: {term_urn}",
                    "urn": term_urn
                }
            
            # Generate deterministic URN if enabled
            if self.use_deterministic_urns:
                original_urn = term_data.get("urn")
                deterministic_urn = self.get_deterministic_urn(term_data, "glossaryTerm")
                
                if deterministic_urn and deterministic_urn != original_urn:
                    logger.info(f"Remapping glossary term URN: {original_urn} -> {deterministic_urn}")
                    term_data["urn"] = deterministic_urn
                    term_urn = deterministic_urn
            
            # Check if term exists in target
            target_term = self.target_client.export_glossary_term(term_urn)
            action = "update" if target_term else "create"
            
            # Import term to target
            if not self.dry_run:
                success = self.target_client.import_glossary_term(term_data)
                if not success:
                    return {
                        "status": "error",
                        "message": f"Failed to import glossary term to target: {term_urn}",
                        "urn": term_urn
                    }
            
            return {
                "status": "success",
                "message": f"Successfully {'would sync' if self.dry_run else 'synced'} glossary term: {term_urn}",
                "urn": term_urn,
                "action": action,
                "dry_run": self.dry_run
            }
        
        except Exception as e:
            logger.error(f"Error syncing glossary term {term_urn}: {str(e)}")
            return {
                "status": "error",
                "message": f"Error syncing glossary term: {str(e)}",
                "urn": term_urn
            }
    
    def sync_glossary_node(self, node_urn: str) -> Dict[str, Any]:
        """
        Sync a glossary node from source to target
        
        Args:
            node_urn: URN of the glossary node to sync
            
        Returns:
            Dictionary with sync result
        """
        logger.info(f"Syncing glossary node: {node_urn}")
        
        try:
            # Export node from source
            node_data = self.source_client.export_glossary_node(node_urn)
            if not node_data:
                return {
                    "status": "error",
                    "message": f"Failed to export glossary node from source: {node_urn}",
                    "urn": node_urn
                }
            
            # Generate deterministic URN if enabled
            if self.use_deterministic_urns:
                original_urn = node_data.get("urn")
                deterministic_urn = self.get_deterministic_urn(node_data, "glossaryNode")
                
                if deterministic_urn and deterministic_urn != original_urn:
                    logger.info(f"Remapping glossary node URN: {original_urn} -> {deterministic_urn}")
                    node_data["urn"] = deterministic_urn
                    node_urn = deterministic_urn
            
            # Check if node exists in target
            target_node = self.target_client.export_glossary_node(node_urn)
            action = "update" if target_node else "create"
            
            # Import node to target
            if not self.dry_run:
                success = self.target_client.import_glossary_node(node_data)
                if not success:
                    return {
                        "status": "error",
                        "message": f"Failed to import glossary node to target: {node_urn}",
                        "urn": node_urn
                    }
            
            return {
                "status": "success",
                "message": f"Successfully {'would sync' if self.dry_run else 'synced'} glossary node: {node_urn}",
                "urn": node_urn,
                "action": action,
                "dry_run": self.dry_run
            }
        
        except Exception as e:
            logger.error(f"Error syncing glossary node {node_urn}: {str(e)}")
            return {
                "status": "error",
                "message": f"Error syncing glossary node: {str(e)}",
                "urn": node_urn
            }
    
    def sync_tag(self, tag_urn: str) -> Dict[str, Any]:
        """
        Sync a tag from source to target
        
        Args:
            tag_urn: URN of the tag to sync
            
        Returns:
            Dictionary with sync result
        """
        logger.info(f"Syncing tag: {tag_urn}")
        
        try:
            # Export tag from source
            tag_data = self.source_client.export_tag(tag_urn)
            if not tag_data:
                return {
                    "status": "error",
                    "message": f"Failed to export tag from source: {tag_urn}",
                    "urn": tag_urn
                }
            
            # Generate deterministic URN if enabled
            if self.use_deterministic_urns:
                original_urn = tag_data.get("urn")
                deterministic_urn = self.get_deterministic_urn(tag_data, "tag")
                
                if deterministic_urn and deterministic_urn != original_urn:
                    logger.info(f"Remapping tag URN: {original_urn} -> {deterministic_urn}")
                    tag_data["urn"] = deterministic_urn
                    tag_urn = deterministic_urn
            
            # Check if tag exists in target
            target_tag = self.target_client.export_tag(tag_urn)
            action = "update" if target_tag else "create"
            
            # Import tag to target
            if not self.dry_run:
                success = self.target_client.import_tag(tag_data)
                if not success:
                    return {
                        "status": "error",
                        "message": f"Failed to import tag to target: {tag_urn}",
                        "urn": tag_urn
                    }
            
            return {
                "status": "success",
                "message": f"Successfully {'would sync' if self.dry_run else 'synced'} tag: {tag_urn}",
                "urn": tag_urn,
                "action": action,
                "dry_run": self.dry_run
            }
        
        except Exception as e:
            logger.error(f"Error syncing tag {tag_urn}: {str(e)}")
            return {
                "status": "error",
                "message": f"Error syncing tag: {str(e)}",
                "urn": tag_urn
            }
    
    def sync_domain(self, domain_urn: str) -> Dict[str, Any]:
        """
        Sync a domain from source to target
        
        Args:
            domain_urn: URN of the domain to sync
            
        Returns:
            Dictionary with sync result
        """
        logger.info(f"Syncing domain: {domain_urn}")
        
        try:
            # Export domain from source
            domain_data = self.source_client.export_domain(domain_urn)
            if not domain_data:
                return {
                    "status": "error",
                    "message": f"Failed to export domain from source: {domain_urn}",
                    "urn": domain_urn
                }
            
            # Generate deterministic URN if enabled
            if self.use_deterministic_urns:
                original_urn = domain_data.get("urn")
                deterministic_urn = self.get_deterministic_urn(domain_data, "domain")
                
                if deterministic_urn and deterministic_urn != original_urn:
                    logger.info(f"Remapping domain URN: {original_urn} -> {deterministic_urn}")
                    domain_data["urn"] = deterministic_urn
                    domain_urn = deterministic_urn
            
            # Check if domain exists in target
            target_domain = self.target_client.export_domain(domain_urn)
            action = "update" if target_domain else "create"
            
            # Import domain to target
            if not self.dry_run:
                success = self.target_client.import_domain(domain_data)
                if not success:
                    return {
                        "status": "error",
                        "message": f"Failed to import domain to target: {domain_urn}",
                        "urn": domain_urn
                    }
            
            return {
                "status": "success",
                "message": f"Successfully {'would sync' if self.dry_run else 'synced'} domain: {domain_urn}",
                "urn": domain_urn,
                "action": action,
                "dry_run": self.dry_run
            }
        
        except Exception as e:
            logger.error(f"Error syncing domain {domain_urn}: {str(e)}")
            return {
                "status": "error",
                "message": f"Error syncing domain: {str(e)}",
                "urn": domain_urn
            }
    
    def sync_structured_properties(self, entity_urn: str) -> Dict[str, Any]:
        """
        Sync structured properties for an entity from source to target
        
        Args:
            entity_urn: URN of the entity to sync properties for
            
        Returns:
            Dictionary with sync result
        """
        logger.info(f"Syncing structured properties for entity: {entity_urn}")
        
        try:
            # GraphQL query to get structured properties for the entity
            query = """
            query getEntityStructuredProperties($urn: String!) {
              entity(urn: $urn) {
                structuredProperties {
                  properties
                }
              }
            }
            """
            
            variables = {
                "urn": entity_urn
            }
            
            # Execute the query on the source client
            source_result = self.source_client.execute_graphql(query, variables)
            
            if (
                not source_result
                or "data" not in source_result
                or "entity" not in source_result["data"]
                or "structuredProperties" not in source_result["data"]["entity"]
                or "properties" not in source_result["data"]["entity"]["structuredProperties"]
            ):
                return {
                    "status": "error",
                    "message": f"Failed to get structured properties for entity from source: {entity_urn}",
                    "urn": entity_urn
                }
            
            properties = source_result["data"]["entity"]["structuredProperties"]["properties"]
            
            if not properties:
                return {
                    "status": "info",
                    "message": f"No structured properties found for entity: {entity_urn}",
                    "urn": entity_urn
                }
            
            # Sync each property to the target
            sync_results = []
            
            for prop_name, prop_value in properties.items():
                if not self.dry_run:
                    mutation = """
                    mutation setStructuredProperty($input: SetStructuredPropertyInput!) {
                      setStructuredProperty(input: $input) {
                        success
                      }
                    }
                    """
                    
                    mutation_variables = {
                        "input": {
                            "entityUrn": entity_urn,
                            "name": prop_name,
                            "value": prop_value
                        }
                    }
                    
                    # Execute the mutation on the target client
                    target_result = self.target_client.execute_graphql(mutation, mutation_variables)
                    
                    success = (
                        target_result
                        and "data" in target_result
                        and "setStructuredProperty" in target_result["data"]
                        and "success" in target_result["data"]["setStructuredProperty"]
                        and target_result["data"]["setStructuredProperty"]["success"]
                    )
                    
                    if success:
                        sync_results.append({
                            "status": "success",
                            "property": prop_name,
                            "message": f"Successfully synced property '{prop_name}' for entity {entity_urn}"
                        })
                    else:
                        sync_results.append({
                            "status": "error",
                            "property": prop_name,
                            "message": f"Failed to sync property '{prop_name}' for entity {entity_urn}"
                        })
                else:
                    sync_results.append({
                        "status": "info",
                        "property": prop_name,
                        "message": f"Would sync property '{prop_name}' for entity {entity_urn}"
                    })
            
            return {
                "status": "success",
                "message": f"Successfully {'would sync' if self.dry_run else 'synced'} structured properties for entity: {entity_urn}",
                "urn": entity_urn,
                "properties": sync_results,
                "dry_run": self.dry_run
            }
        
        except Exception as e:
            logger.error(f"Error syncing structured properties for entity {entity_urn}: {str(e)}")
            return {
                "status": "error",
                "message": f"Error syncing structured properties: {str(e)}",
                "urn": entity_urn
            }
    
    def sync_all_glossary_terms(self) -> List[Dict[str, Any]]:
        """
        Sync all glossary terms from source to target
        
        Returns:
            List of sync results
        """
        logger.info("Syncing all glossary terms")
        
        try:
            # Get all terms from source
            terms = self.source_client.list_glossary_terms()
            if not terms:
                return [{
                    "status": "info",
                    "message": "No glossary terms found in source environment"
                }]
            
            # Sync each term
            results = []
            for term in terms:
                term_urn = term.get("urn")
                if term_urn:
                    result = self.sync_glossary_term(term_urn)
                    results.append(result)
            
            return results
        
        except Exception as e:
            logger.error(f"Error syncing all glossary terms: {str(e)}")
            return [{
                "status": "error",
                "message": f"Error syncing all glossary terms: {str(e)}"
            }]
    
    def sync_all_glossary_nodes(self) -> List[Dict[str, Any]]:
        """
        Sync all glossary nodes from source to target
        
        Returns:
            List of sync results
        """
        logger.info("Syncing all glossary nodes")
        
        try:
            # Get all nodes from source
            nodes = self.source_client.list_glossary_nodes()
            if not nodes:
                return [{
                    "status": "info",
                    "message": "No glossary nodes found in source environment"
                }]
            
            # Sync each node
            results = []
            for node in nodes:
                node_urn = node.get("urn")
                if node_urn:
                    result = self.sync_glossary_node(node_urn)
                    results.append(result)
            
            return results
        
        except Exception as e:
            logger.error(f"Error syncing all glossary nodes: {str(e)}")
            return [{
                "status": "error",
                "message": f"Error syncing all glossary nodes: {str(e)}"
            }]
    
    def sync_all_tags(self) -> List[Dict[str, Any]]:
        """
        Sync all tags from source to target
        
        Returns:
            List of sync results
        """
        logger.info("Syncing all tags")
        
        try:
            # Get all tags from source
            tags = self.source_client.list_tags()
            if not tags:
                return [{
                    "status": "info",
                    "message": "No tags found in source environment"
                }]
            
            # Sync each tag
            results = []
            for tag in tags:
                tag_urn = tag.get("urn")
                if tag_urn:
                    result = self.sync_tag(tag_urn)
                    results.append(result)
            
            return results
        
        except Exception as e:
            logger.error(f"Error syncing all tags: {str(e)}")
            return [{
                "status": "error",
                "message": f"Error syncing all tags: {str(e)}"
            }]
    
    def sync_all_domains(self) -> List[Dict[str, Any]]:
        """
        Sync all domains from source to target
        
        Returns:
            List of sync results
        """
        logger.info("Syncing all domains")
        
        try:
            # Get all domains from source
            domains = self.source_client.list_domains()
            if not domains:
                return [{
                    "status": "info",
                    "message": "No domains found in source environment"
                }]
            
            # Sync each domain
            results = []
            for domain in domains:
                domain_urn = domain.get("urn")
                if domain_urn:
                    result = self.sync_domain(domain_urn)
                    results.append(result)
            
            return results
        
        except Exception as e:
            logger.error(f"Error syncing all domains: {str(e)}")
            return [{
                "status": "error",
                "message": f"Error syncing all domains: {str(e)}"
            }]


def main():
    args = parse_args()
    setup_logging(args.log_level)
    
    # Get tokens for source and target environments
    source_token = None
    target_token = None
    
    if args.source_token_file:
        try:
            with open(args.source_token_file, "r") as f:
                source_token = f.read().strip()
        except Exception as e:
            logger.error(f"Error reading source token file: {str(e)}")
            sys.exit(1)
    else:
        source_token = get_token_from_env("SOURCE_DATAHUB_TOKEN")
    
    if args.target_token_file:
        try:
            with open(args.target_token_file, "r") as f:
                target_token = f.read().strip()
        except Exception as e:
            logger.error(f"Error reading target token file: {str(e)}")
            sys.exit(1)
    else:
        target_token = get_token_from_env("TARGET_DATAHUB_TOKEN")
    
    # Initialize clients
    try:
        source_client = DataHubMetadataApiClient(args.source_url, source_token)
        target_client = DataHubMetadataApiClient(args.target_url, target_token)
    except Exception as e:
        logger.error(f"Error initializing DataHub clients: {str(e)}")
        sys.exit(1)
    
    # Initialize syncer
    syncer = MetadataSyncer(source_client, target_client, args.dry_run, 
                           use_deterministic_urns=args.use_deterministic_urns)
    
    # Load config file if provided
    config = {}
    if args.config_file:
        try:
            with open(args.config_file, "r") as f:
                config = json.load(f)
        except Exception as e:
            logger.error(f"Error loading config file: {str(e)}")
            sys.exit(1)
    
    # Determine entity URNs to sync
    entity_urns = []
    if args.entity_urns:
        entity_urns = [urn.strip() for urn in args.entity_urns.split(",")]
    elif "entity_urns" in config:
        entity_urns = config.get("entity_urns", [])
    
    # Initialize results
    results = {
        "source": args.source_url,
        "target": args.target_url,
        "entityType": args.entity_type,
        "dryRun": args.dry_run,
        "results": []
    }
    
    try:
        # Sync metadata based on entity type
        if entity_urns:
            # Sync specific entities
            for urn in entity_urns:
                if urn.startswith("urn:li:glossaryTerm:"):
                    result = syncer.sync_glossary_term(urn)
                    results["results"].append(result)
                    
                    if args.include_properties:
                        prop_result = syncer.sync_structured_properties(urn)
                        results["results"].append(prop_result)
                        
                elif urn.startswith("urn:li:glossaryNode:"):
                    result = syncer.sync_glossary_node(urn)
                    results["results"].append(result)
                    
                    if args.include_properties:
                        prop_result = syncer.sync_structured_properties(urn)
                        results["results"].append(prop_result)
                        
                elif urn.startswith("urn:li:tag:"):
                    result = syncer.sync_tag(urn)
                    results["results"].append(result)
                    
                elif urn.startswith("urn:li:domain:"):
                    result = syncer.sync_domain(urn)
                    results["results"].append(result)
                    
                    if args.include_properties:
                        prop_result = syncer.sync_structured_properties(urn)
                        results["results"].append(prop_result)
                else:
                    logger.warning(f"Unsupported entity URN: {urn}")
        
        else:
            # Sync all entities of specified type
            if args.entity_type == "glossaryTerm" or args.entity_type == "all":
                term_results = syncer.sync_all_glossary_terms()
                results["results"].extend(term_results)
                
                if args.include_properties:
                    for result in term_results:
                        if result.get("status") == "success" and "urn" in result:
                            prop_result = syncer.sync_structured_properties(result["urn"])
                            results["results"].append(prop_result)
            
            if args.entity_type == "glossaryNode" or args.entity_type == "all":
                node_results = syncer.sync_all_glossary_nodes()
                results["results"].extend(node_results)
                
                if args.include_properties:
                    for result in node_results:
                        if result.get("status") == "success" and "urn" in result:
                            prop_result = syncer.sync_structured_properties(result["urn"])
                            results["results"].append(prop_result)
            
            if args.entity_type == "tag" or args.entity_type == "all":
                tag_results = syncer.sync_all_tags()
                results["results"].extend(tag_results)
            
            if args.entity_type == "domain" or args.entity_type == "all":
                domain_results = syncer.sync_all_domains()
                results["results"].extend(domain_results)
                
                if args.include_properties:
                    for result in domain_results:
                        if result.get("status") == "success" and "urn" in result:
                            prop_result = syncer.sync_structured_properties(result["urn"])
                            results["results"].append(prop_result)
        
        # Calculate summary statistics
        success_count = sum(1 for r in results["results"] if r.get("status") == "success")
        error_count = sum(1 for r in results["results"] if r.get("status") == "error")
        info_count = sum(1 for r in results["results"] if r.get("status") == "info")
        
        results["summary"] = {
            "total": len(results["results"]),
            "success": success_count,
            "error": error_count,
            "info": info_count
        }
        
        # Save results to file if specified
        if args.output_file:
            output_dir = os.path.dirname(args.output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
                
            with open(args.output_file, "w") as f:
                json.dump(results, f, indent=4)
            
            logger.info(f"Sync results saved to {args.output_file}")
        
        # Print summary
        logger.info(f"Sync completed: {success_count} succeeded, {error_count} failed, {info_count} informational")
        
        if error_count > 0:
            logger.warning("Some entities failed to sync. Check the results for details.")
            if args.dry_run:
                logger.info("This was a dry run. No changes were made to the target environment.")
            sys.exit(1)
        else:
            logger.info("All entities synced successfully.")
            if args.dry_run:
                logger.info("This was a dry run. No changes were made to the target environment.")
    
    except Exception as e:
        logger.error(f"Error during sync: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 