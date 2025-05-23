#!/usr/bin/env python3
"""
Update a glossary node or term in DataHub.
"""

import argparse
import json
import logging
import os
import sys
from typing import Dict, Any, Optional, List

# Add the parent directory to the sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.datahub_metadata_api import DataHubMetadataApiClient
from utils.token_utils import get_token_from_env
from scripts.glossary.glossary_utils import (
    load_glossary_node_from_file, 
    load_glossary_term_from_file,
    get_glossary_node_name, 
    get_glossary_term_name
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
    parser = argparse.ArgumentParser(description="Update a glossary node or term in DataHub")
    
    parser.add_argument(
        "--server-url",
        "-s",
        required=True,
        help="DataHub server URL (e.g., http://localhost:8080)",
    )
    
    # Create subparsers for node and term commands
    subparsers = parser.add_subparsers(dest="entity_type", help="Entity type to update")
    
    # Node subparser
    node_parser = subparsers.add_parser("node", help="Update a glossary node")
    
    node_parser.add_argument(
        "--urn",
        required=True,
        help="URN of the glossary node to update",
    )
    
    node_parser.add_argument(
        "--name",
        "-n",
        help="New name for the glossary node",
    )
    
    node_parser.add_argument(
        "--description",
        "-d",
        help="New description for the glossary node",
    )
    
    node_parser.add_argument(
        "--parent-node",
        help="New parent node URN (to move the node to a different parent)",
    )
    
    node_parser.add_argument(
        "--remove-parent",
        action="store_true",
        help="Remove the node from its parent (make it a root node)",
    )
    
    node_parser.add_argument(
        "--owners",
        help="List of owner URNs, comma-separated (e.g., urn:li:corpuser:alice,urn:li:corpgroup:engineering)",
    )
    
    node_parser.add_argument(
        "--add-owners",
        help="List of owner URNs to add, comma-separated",
    )
    
    node_parser.add_argument(
        "--remove-owners",
        help="List of owner URNs to remove, comma-separated",
    )
    
    node_parser.add_argument(
        "--input-file",
        "-i",
        help="JSON file containing updated glossary node definition",
    )
    
    # Term subparser
    term_parser = subparsers.add_parser("term", help="Update a glossary term")
    
    term_parser.add_argument(
        "--urn",
        required=True,
        help="URN of the glossary term to update",
    )
    
    term_parser.add_argument(
        "--name",
        "-n",
        help="New name for the glossary term",
    )
    
    term_parser.add_argument(
        "--description",
        "-d",
        help="New description for the glossary term",
    )
    
    term_parser.add_argument(
        "--definition",
        help="New definition for the glossary term",
    )
    
    term_parser.add_argument(
        "--source",
        help="New source for the glossary term",
    )
    
    term_parser.add_argument(
        "--parent-node",
        help="New parent node URN (to move the term to a different parent)",
    )
    
    term_parser.add_argument(
        "--remove-parent",
        action="store_true",
        help="Remove the term from its parent (make it a root term)",
    )
    
    term_parser.add_argument(
        "--owners",
        help="List of owner URNs, comma-separated (e.g., urn:li:corpuser:alice,urn:li:corpgroup:engineering)",
    )
    
    term_parser.add_argument(
        "--add-owners",
        help="List of owner URNs to add, comma-separated",
    )
    
    term_parser.add_argument(
        "--remove-owners",
        help="List of owner URNs to remove, comma-separated",
    )
    
    term_parser.add_argument(
        "--input-file",
        "-i",
        help="JSON file containing updated glossary term definition",
    )
    
    # Common arguments
    parser.add_argument(
        "--token-file",
        help="File containing DataHub access token",
    )
    
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)",
    )
    
    return parser.parse_args()


def update_node_from_args(client: DataHubMetadataApiClient, args) -> bool:
    """
    Update a glossary node from command line arguments
    
    Args:
        client: DataHub metadata client
        args: Command line arguments
        
    Returns:
        True if successful, False otherwise
    """
    # First, get the current node
    current_node = client.export_glossary_node(args.urn)
    if not current_node:
        logger.error(f"Failed to retrieve glossary node with URN: {args.urn}")
        return False
    
    # Create a copy of the current node to modify
    updated_node = current_node.copy()
    
    # Update the node properties if provided
    if args.name or args.description:
        if "properties" not in updated_node:
            updated_node["properties"] = {}
        
        if args.name:
            updated_node["properties"]["name"] = args.name
        
        if args.description:
            updated_node["properties"]["description"] = args.description
    
    # Update parent node if specified
    if args.parent_node:
        updated_node["parentNode"] = {"urn": args.parent_node}
    elif args.remove_parent:
        if "parentNode" in updated_node:
            del updated_node["parentNode"]
    
    # Update owners if specified
    if args.owners:
        owner_urns = args.owners.split(",")
        owners = []
        
        for urn in owner_urns:
            urn = urn.strip()
            if urn.startswith("urn:li:corpuser:"):
                owner_type = "USER"
            elif urn.startswith("urn:li:corpgroup:"):
                owner_type = "GROUP"
            else:
                logger.warning(f"Unknown owner URN type: {urn}")
                continue
            
            owners.append({
                "owner": {"urn": urn},
                "type": owner_type,
                "ownershipType": {"urn": "urn:li:ownershipType:Technical"}
            })
        
        if owners:
            updated_node["ownership"] = {"owners": owners}
    elif args.add_owners or args.remove_owners:
        # Get current owners
        current_owners = []
        if "ownership" in current_node and "owners" in current_node["ownership"]:
            current_owners = current_node["ownership"]["owners"]
        
        # Add new owners if specified
        if args.add_owners:
            add_owner_urns = args.add_owners.split(",")
            for urn in add_owner_urns:
                urn = urn.strip()
                
                # Check if owner already exists
                exists = False
                for owner in current_owners:
                    if owner["owner"]["urn"] == urn:
                        exists = True
                        break
                
                if not exists:
                    if urn.startswith("urn:li:corpuser:"):
                        owner_type = "USER"
                    elif urn.startswith("urn:li:corpgroup:"):
                        owner_type = "GROUP"
                    else:
                        logger.warning(f"Unknown owner URN type: {urn}")
                        continue
                    
                    current_owners.append({
                        "owner": {"urn": urn},
                        "type": owner_type,
                        "ownershipType": {"urn": "urn:li:ownershipType:Technical"}
                    })
        
        # Remove owners if specified
        if args.remove_owners:
            remove_owner_urns = args.remove_owners.split(",")
            current_owners = [
                owner for owner in current_owners 
                if owner["owner"]["urn"] not in remove_owner_urns
            ]
        
        if current_owners:
            updated_node["ownership"] = {"owners": current_owners}
        elif "ownership" in updated_node:
            del updated_node["ownership"]
    
    # Import the updated node
    return client.import_glossary_node(updated_node) is not None


def update_term_from_args(client: DataHubMetadataApiClient, args) -> bool:
    """
    Update a glossary term from command line arguments
    
    Args:
        client: DataHub metadata client
        args: Command line arguments
        
    Returns:
        True if successful, False otherwise
    """
    # First, get the current term
    current_term = client.export_glossary_term(args.urn)
    if not current_term:
        logger.error(f"Failed to retrieve glossary term with URN: {args.urn}")
        return False
    
    # Create a copy of the current term to modify
    updated_term = current_term.copy()
    
    # Update the term properties if provided
    if args.name or args.description or args.definition or args.source:
        if "properties" not in updated_term:
            updated_term["properties"] = {}
        
        if args.name:
            updated_term["properties"]["name"] = args.name
        
        if args.description:
            updated_term["properties"]["description"] = args.description
        
        if args.definition:
            updated_term["properties"]["definition"] = args.definition
        
        if args.source:
            updated_term["properties"]["termSource"] = args.source
    
    # Update parent node if specified
    if args.parent_node:
        updated_term["parentNode"] = {"urn": args.parent_node}
    elif args.remove_parent:
        if "parentNode" in updated_term:
            del updated_term["parentNode"]
    
    # Update owners if specified
    if args.owners:
        owner_urns = args.owners.split(",")
        owners = []
        
        for urn in owner_urns:
            urn = urn.strip()
            if urn.startswith("urn:li:corpuser:"):
                owner_type = "USER"
            elif urn.startswith("urn:li:corpgroup:"):
                owner_type = "GROUP"
            else:
                logger.warning(f"Unknown owner URN type: {urn}")
                continue
            
            owners.append({
                "owner": {"urn": urn},
                "type": owner_type,
                "ownershipType": {"urn": "urn:li:ownershipType:Technical"}
            })
        
        if owners:
            updated_term["ownership"] = {"owners": owners}
    elif args.add_owners or args.remove_owners:
        # Get current owners
        current_owners = []
        if "ownership" in current_term and "owners" in current_term["ownership"]:
            current_owners = current_term["ownership"]["owners"]
        
        # Add new owners if specified
        if args.add_owners:
            add_owner_urns = args.add_owners.split(",")
            for urn in add_owner_urns:
                urn = urn.strip()
                
                # Check if owner already exists
                exists = False
                for owner in current_owners:
                    if owner["owner"]["urn"] == urn:
                        exists = True
                        break
                
                if not exists:
                    if urn.startswith("urn:li:corpuser:"):
                        owner_type = "USER"
                    elif urn.startswith("urn:li:corpgroup:"):
                        owner_type = "GROUP"
                    else:
                        logger.warning(f"Unknown owner URN type: {urn}")
                        continue
                    
                    current_owners.append({
                        "owner": {"urn": urn},
                        "type": owner_type,
                        "ownershipType": {"urn": "urn:li:ownershipType:Technical"}
                    })
        
        # Remove owners if specified
        if args.remove_owners:
            remove_owner_urns = args.remove_owners.split(",")
            current_owners = [
                owner for owner in current_owners 
                if owner["owner"]["urn"] not in remove_owner_urns
            ]
        
        if current_owners:
            updated_term["ownership"] = {"owners": current_owners}
        elif "ownership" in updated_term:
            del updated_term["ownership"]
    
    # Import the updated term
    return client.import_glossary_term(updated_term) is not None


def main():
    args = parse_args()
    setup_logging(args.log_level)
    
    # Get token from file or environment
    token = None
    if args.token_file:
        try:
            with open(args.token_file, "r") as f:
                token = f.read().strip()
        except Exception as e:
            logger.error(f"Error reading token file: {str(e)}")
            sys.exit(1)
    else:
        token = get_token_from_env()
    
    # Initialize the client
    try:
        client = DataHubMetadataApiClient(args.server_url, token)
    except Exception as e:
        logger.error(f"Error initializing client: {str(e)}")
        sys.exit(1)
    
    try:
        success = False
        entity_name = args.urn
        
        # Determine entity type and update accordingly
        if args.entity_type == "node":
            # Update glossary node
            if args.input_file:
                # Load from file
                node_data = load_glossary_node_from_file(args.input_file)
                node_data["urn"] = args.urn  # Ensure the URN is set
                entity_name = get_glossary_node_name(node_data) or args.urn
                
                logger.info(f"Updating glossary node '{entity_name}' from file")
                success = client.import_glossary_node(node_data) is not None
            else:
                # Update from command line arguments
                logger.info(f"Updating glossary node: {args.urn}")
                success = update_node_from_args(client, args)
                
                # Get the name for logging
                current_node = client.export_glossary_node(args.urn)
                if current_node:
                    entity_name = get_glossary_node_name(current_node) or args.urn
        
        elif args.entity_type == "term":
            # Update glossary term
            if args.input_file:
                # Load from file
                term_data = load_glossary_term_from_file(args.input_file)
                term_data["urn"] = args.urn  # Ensure the URN is set
                entity_name = get_glossary_term_name(term_data) or args.urn
                
                logger.info(f"Updating glossary term '{entity_name}' from file")
                success = client.import_glossary_term(term_data) is not None
            else:
                # Update from command line arguments
                logger.info(f"Updating glossary term: {args.urn}")
                success = update_term_from_args(client, args)
                
                # Get the name for logging
                current_term = client.export_glossary_term(args.urn)
                if current_term:
                    entity_name = get_glossary_term_name(current_term) or args.urn
        
        else:
            logger.error("Entity type must be specified (node or term)")
            sys.exit(1)
        
        if success:
            logger.info(f"Successfully updated glossary {args.entity_type}: {entity_name}")
        else:
            logger.error(f"Failed to update glossary {args.entity_type}: {entity_name}")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"Error updating glossary entity: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 