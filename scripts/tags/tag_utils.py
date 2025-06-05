#!/usr/bin/env python3
"""
Common utilities for tag operations.
"""

import json
import logging
import os
import sys
from typing import Dict, Any

# Add the parent directory to the sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from utils.datahub_metadata_api import DataHubMetadataApiClient

logger = logging.getLogger(__name__)


def load_tag_from_file(file_path: str) -> Dict[str, Any]:
    """
    Load tag definition from a JSON file

    Args:
        file_path: Path to the JSON file

    Returns:
        Dictionary containing tag definition
    """
    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        # Check if the file contains a tag directly or wrapped in a container
        if "tag" in data:
            return data["tag"]
        elif "properties" in data:
            return data
        else:
            logger.warning(
                "Tag definition not found in expected format, using entire file content"
            )
            return data
    except Exception as e:
        logger.error(f"Error loading tag from file: {str(e)}")
        raise


def save_tag_to_file(
    tag: Dict[str, Any], file_path: str, pretty_print: bool = True
) -> bool:
    """
    Save tag definition to a JSON file

    Args:
        tag: Tag data to save
        file_path: Path to save the tag data
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
            json.dump(tag, f, indent=4 if pretty_print else None)
        return True
    except Exception as e:
        logger.error(f"Error saving tag to file: {str(e)}")
        return False


def get_tag_name(tag: Dict[str, Any]) -> str:
    """
    Get the name of a tag

    Args:
        tag: Tag data

    Returns:
        Name of the tag or empty string if not found
    """
    if not tag:
        return ""

    if "properties" in tag and "name" in tag["properties"]:
        return tag["properties"]["name"]

    if "name" in tag:
        return tag["name"]

    if "urn" in tag:
        return tag["urn"].split(":")[-1]

    return ""


def get_tag_urn_from_id(tag_id: str) -> str:
    """
    Convert a tag ID to a URN

    Args:
        tag_id: Tag ID (e.g., 'pii')

    Returns:
        Tag URN (e.g., 'urn:li:tag:pii')
    """
    if not tag_id:
        raise ValueError("Tag ID cannot be empty")

    # If it's already a URN, return it
    if tag_id.startswith("urn:li:tag:"):
        return tag_id

    return f"urn:li:tag:{tag_id}"


def get_tag_id_from_urn(tag_urn: str) -> str:
    """
    Extract tag ID from a URN

    Args:
        tag_urn: Tag URN (e.g., 'urn:li:tag:pii')

    Returns:
        Tag ID (e.g., 'pii')
    """
    if not tag_urn.startswith("urn:li:tag:"):
        raise ValueError(f"Invalid tag URN: {tag_urn}")

    return tag_urn.split(":")[-1]


def check_tag_dependencies(client: DataHubMetadataApiClient, tag_urn: str) -> bool:
    """
    Check if a tag has associated entities

    Args:
        client: DataHub metadata client
        tag_urn: URN of the tag

    Returns:
        True if tag has associated entities, False otherwise
    """
    tag_with_entities = client.export_tag(tag_urn, include_entities=True)
    return (
        tag_with_entities is not None
        and "entities" in tag_with_entities
        and len(tag_with_entities["entities"]) > 0
    )
