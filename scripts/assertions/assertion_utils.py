#!/usr/bin/env python3
"""
Common utilities for assertion operations.
"""

import json
import logging
import os
import sys
from typing import Dict, Any, Optional

# Add the parent directory to the sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)


logger = logging.getLogger(__name__)


def load_assertion_from_file(file_path: str) -> Dict[str, Any]:
    """
    Load assertion definition from a JSON file

    Args:
        file_path: Path to the JSON file

    Returns:
        Dictionary containing assertion definition
    """
    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        # Check if the file contains an assertion directly or wrapped in a container
        if "assertion" in data:
            return data["assertion"]
        elif "info" in data:
            return data
        else:
            logger.warning(
                "Assertion definition not found in expected format, using entire file content"
            )
            return data
    except Exception as e:
        logger.error(f"Error loading assertion from file: {str(e)}")
        raise


def save_assertion_to_file(
    assertion: Dict[str, Any], file_path: str, pretty_print: bool = True
) -> bool:
    """
    Save assertion definition to a JSON file

    Args:
        assertion: Assertion data to save
        file_path: Path to save the assertion data
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
            json.dump(assertion, f, indent=4 if pretty_print else None)
        return True
    except Exception as e:
        logger.error(f"Error saving assertion to file: {str(e)}")
        return False


def get_assertion_type(assertion: Dict[str, Any]) -> str:
    """
    Get the type of an assertion

    Args:
        assertion: Assertion data

    Returns:
        Type of the assertion or empty string if not found
    """
    if not assertion:
        return ""

    if "info" in assertion and "type" in assertion["info"]:
        return assertion["info"]["type"]

    return ""


def get_assertion_description(assertion: Dict[str, Any]) -> str:
    """
    Get the description of an assertion

    Args:
        assertion: Assertion data

    Returns:
        Description of the assertion or empty string if not found
    """
    if not assertion:
        return ""

    if "info" in assertion and "description" in assertion["info"]:
        return assertion["info"]["description"]

    return ""


def get_assertion_entity_urn(assertion: Dict[str, Any]) -> Optional[str]:
    """
    Get the entity URN associated with an assertion

    Args:
        assertion: Assertion data

    Returns:
        Entity URN or None if not found
    """
    if not assertion or "info" not in assertion:
        return None

    # Different assertion types store the entity URN in different places
    info = assertion["info"]

    # Dataset assertion
    if "datasetAssertion" in info and "datasetUrn" in info["datasetAssertion"]:
        return info["datasetAssertion"]["datasetUrn"]

    # Freshness assertion
    if "freshnessAssertion" in info and "entityUrn" in info["freshnessAssertion"]:
        return info["freshnessAssertion"]["entityUrn"]

    # SQL assertion
    if "sqlAssertion" in info and "entityUrn" in info["sqlAssertion"]:
        return info["sqlAssertion"]["entityUrn"]

    # Field assertion
    if "fieldAssertion" in info and "entityUrn" in info["fieldAssertion"]:
        return info["fieldAssertion"]["entityUrn"]

    # Volume assertion
    if "volumeAssertion" in info and "entityUrn" in info["volumeAssertion"]:
        return info["volumeAssertion"]["entityUrn"]

    # Schema assertion
    if "schemaAssertion" in info and "entityUrn" in info["schemaAssertion"]:
        return info["schemaAssertion"]["entityUrn"]

    return None


def get_assertion_field_path(assertion: Dict[str, Any]) -> Optional[str]:
    """
    Get the field path for a field assertion

    Args:
        assertion: Assertion data

    Returns:
        Field path or None if not a field assertion
    """
    if not assertion or "info" not in assertion:
        return None

    info = assertion["info"]

    # Field assertion
    if (
        "fieldAssertion" in info
        and "fieldValuesAssertion" in info["fieldAssertion"]
        and "field" in info["fieldAssertion"]["fieldValuesAssertion"]
    ):
        return info["fieldAssertion"]["fieldValuesAssertion"]["field"].get("path")

    return None


def get_assertion_id_from_urn(assertion_urn: str) -> str:
    """
    Extract assertion ID from a URN

    Args:
        assertion_urn: Assertion URN (e.g., 'urn:li:assertion:12345')

    Returns:
        Assertion ID
    """
    if not assertion_urn.startswith("urn:li:assertion:"):
        raise ValueError(f"Invalid assertion URN: {assertion_urn}")

    return assertion_urn.split(":")[-1]


def format_assertion_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format assertion run result for display

    Args:
        result: Raw assertion result

    Returns:
        Formatted result
    """
    formatted = {}

    if not result:
        return formatted

    if "type" in result:
        formatted["status"] = result["type"]

    if "nativeResults" in result:
        native_results = {}
        for item in result["nativeResults"]:
            if "key" in item and "value" in item:
                native_results[item["key"]] = item["value"]
        formatted["details"] = native_results

    return formatted
