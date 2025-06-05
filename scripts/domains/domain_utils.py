#!/usr/bin/env python3
"""
Common utilities for domain operations.
"""

import json
import logging
import os
import sys
from typing import Dict, Any, List, Tuple

# Add the parent directory to the sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from utils.datahub_metadata_api import DataHubMetadataApiClient

logger = logging.getLogger(__name__)


def load_domain_from_file(file_path: str) -> Dict[str, Any]:
    """
    Load domain definition from a JSON file

    Args:
        file_path: Path to the JSON file

    Returns:
        Dictionary containing domain definition
    """
    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        # Check if the file contains a domain directly or wrapped in a container
        if "domain" in data:
            return data["domain"]
        elif "properties" in data:
            return data
        else:
            logger.warning(
                "Domain definition not found in expected format, using entire file content"
            )
            return data
    except Exception as e:
        logger.error(f"Error loading domain from file: {str(e)}")
        raise


def save_domain_to_file(
    domain: Dict[str, Any], file_path: str, pretty_print: bool = True
) -> bool:
    """
    Save domain definition to a JSON file

    Args:
        domain: Domain data to save
        file_path: Path to save the domain data
        pretty_print: Whether to format the JSON with indentation

    Returns:
        True if successful, False otherwise
    """
    try:
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(file_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        with open(file_path, "w") as f:
            json.dump(domain, f, indent=4 if pretty_print else None)
        return True
    except Exception as e:
        logger.error(f"Error saving domain to file: {str(e)}")
        return False


def get_domain_name(domain: Dict[str, Any]) -> str:
    """
    Get the name of a domain

    Args:
        domain: Domain data

    Returns:
        Name of the domain or empty string if not found
    """
    if not domain:
        return ""

    if "properties" in domain and "name" in domain["properties"]:
        return domain["properties"]["name"]

    if "name" in domain:
        return domain["name"]

    if "urn" in domain:
        return domain["urn"].split(":")[-1]

    return ""


def get_domain_urn_from_id(domain_id: str) -> str:
    """
    Convert a domain ID to a URN

    Args:
        domain_id: Domain ID (e.g., 'engineering')

    Returns:
        Domain URN (e.g., 'urn:li:domain:engineering')
    """
    if not domain_id:
        raise ValueError("Domain ID cannot be empty")

    # If it's already a URN, return it
    if domain_id.startswith("urn:li:domain:"):
        return domain_id

    return f"urn:li:domain:{domain_id}"


def get_domain_id_from_urn(domain_urn: str) -> str:
    """
    Extract domain ID from a URN

    Args:
        domain_urn: Domain URN (e.g., 'urn:li:domain:engineering')

    Returns:
        Domain ID (e.g., 'engineering')
    """
    if not domain_urn.startswith("urn:li:domain:"):
        raise ValueError(f"Invalid domain URN: {domain_urn}")

    return domain_urn.split(":")[-1]


def get_parent_domains(
    client: DataHubMetadataApiClient, domain_urn: str
) -> List[Dict[str, Any]]:
    """
    Get parent domains of a given domain

    Args:
        client: DataHub metadata client
        domain_urn: URN of the domain

    Returns:
        List of parent domains
    """
    domain = client.export_domain(domain_urn)

    if not domain or "parentDomains" not in domain:
        return []

    parent_domains = domain.get("parentDomains", {}).get("domains", [])
    return parent_domains


def get_domain_children(
    client: DataHubMetadataApiClient, domain_urn: str
) -> List[Dict[str, Any]]:
    """
    Get child domains of a given domain

    Args:
        client: DataHub metadata client
        domain_urn: URN of the domain

    Returns:
        List of child domains
    """
    # Get all domains and filter for those with this domain as parent
    all_domains = client.list_domains()

    children = []
    for domain in all_domains:
        parent_domains = domain.get("parentDomains", {}).get("domains", [])
        parent_urns = [parent.get("urn") for parent in parent_domains]

        if domain_urn in parent_urns:
            children.append(domain)

    return children


def check_domain_dependencies(
    client: DataHubMetadataApiClient, domain_urn: str
) -> Tuple[bool, bool]:
    """
    Check if a domain has child domains or associated entities

    Args:
        client: DataHub metadata client
        domain_urn: URN of the domain

    Returns:
        Tuple of (has_children, has_entities)
    """
    return client.check_domain_dependencies(domain_urn)


def create_owner_object(owner_urn: str) -> Dict[str, Any]:
    """
    Create an owner object from an owner URN

    Args:
        owner_urn: URN of the owner (corpuser or corpgroup)

    Returns:
        Owner object for domain ownership
    """
    if owner_urn.startswith("urn:li:corpuser:"):
        owner_type = "USER"
    elif owner_urn.startswith("urn:li:corpgroup:"):
        owner_type = "GROUP"
    else:
        raise ValueError(f"Invalid owner URN: {owner_urn}")

    return {
        "owner": {"urn": owner_urn},
        "type": owner_type,
        "ownershipType": {"urn": "urn:li:ownershipType:Technical"},
    }
