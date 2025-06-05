#!/usr/bin/env python3
"""
Generate deterministic URNs for DataHub entities.

This script helps generate consistent URNs for entities across different environments,
which is useful for metadata synchronization and testing.
"""

import argparse
import json
import logging
import os
import sys
from typing import Dict, List, Optional

# Add the parent directory to the sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from utils.urn_utils import generate_deterministic_urn

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
    parser = argparse.ArgumentParser(
        description="Generate deterministic URNs for DataHub entities"
    )

    parser.add_argument(
        "--entity-type",
        required=True,
        choices=["tag", "glossaryTerm", "glossaryNode", "domain"],
        help="Type of entity to generate URN for",
    )

    parser.add_argument(
        "--name",
        required=True,
        help="Name of the entity (e.g., 'PII' for a tag)",
    )

    parser.add_argument(
        "--namespace",
        help="Optional namespace for scoping (e.g., parent node URN for a glossary term)",
    )

    parser.add_argument(
        "--input-file",
        help="Optional JSON file with multiple entities to generate URNs for",
    )

    parser.add_argument(
        "--output-file",
        help="Output file to save the generated URNs (JSON format)",
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)",
    )

    return parser.parse_args()


def generate_urn_from_args(
    entity_type: str, name: str, namespace: Optional[str] = None
) -> Dict[str, str]:
    """
    Generate a deterministic URN from arguments

    Args:
        entity_type: Type of entity (tag, glossaryTerm, glossaryNode, domain)
        name: Name of the entity
        namespace: Optional namespace for scoping

    Returns:
        Dictionary with entity details and URN
    """
    deterministic_urn = generate_deterministic_urn(entity_type, name, namespace)

    result = {"entityType": entity_type, "name": name, "urn": deterministic_urn}

    if namespace:
        result["namespace"] = namespace

    return result


def generate_urns_from_file(input_file: str) -> List[Dict[str, str]]:
    """
    Generate deterministic URNs for entities defined in a JSON file

    Args:
        input_file: Path to JSON file with entity definitions

    Returns:
        List of dictionaries with entity details and URNs
    """
    try:
        with open(input_file, "r") as f:
            entities = json.load(f)

        results = []

        for entity in entities:
            entity_type = entity.get("entityType")
            name = entity.get("name")
            namespace = entity.get("namespace")

            if not entity_type or not name:
                logger.warning(f"Skipping entity with missing type or name: {entity}")
                continue

            result = generate_urn_from_args(entity_type, name, namespace)
            results.append(result)

        return results

    except Exception as e:
        logger.error(f"Error generating URNs from file: {str(e)}")
        return []


def main():
    args = parse_args()
    setup_logging(args.log_level)

    results = []

    # Generate URNs from file if provided
    if args.input_file:
        logger.info(f"Generating URNs from file: {args.input_file}")
        results = generate_urns_from_file(args.input_file)

    # Generate URN from command line arguments
    else:
        logger.info(f"Generating URN for {args.entity_type} with name '{args.name}'")
        result = generate_urn_from_args(args.entity_type, args.name, args.namespace)
        results = [result]

    # Output results
    if args.output_file:
        try:
            # Create output directory if it doesn't exist
            output_dir = os.path.dirname(args.output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)

            with open(args.output_file, "w") as f:
                json.dump(results, f, indent=4)

            logger.info(f"Generated URNs saved to {args.output_file}")

        except Exception as e:
            logger.error(f"Error saving URNs to file: {str(e)}")

    else:
        # Print to stdout
        print(json.dumps(results, indent=4))


if __name__ == "__main__":
    main()
