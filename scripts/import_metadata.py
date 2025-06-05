#!/usr/bin/env python3
"""
Import metadata into DataHub including domains, business glossaries, tags,
structured properties, metadata tests and assertions.
"""

import argparse
import json
import logging
import os
import sys
from typing import Dict, Any

# Add the parent directory to the sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils._datahub_metadata_client import DataHubMetadataClient
from utils.token_utils import get_token_from_env

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
    parser = argparse.ArgumentParser(description="Import metadata into DataHub")

    parser.add_argument(
        "--server-url",
        "-s",
        required=True,
        help="DataHub server URL (e.g., http://localhost:8080)",
    )

    parser.add_argument(
        "--input-file",
        "-i",
        required=True,
        help="Input file containing the metadata to import",
    )

    parser.add_argument(
        "--import-type",
        "-t",
        choices=["all", "domains", "glossary", "tags", "properties", "tests"],
        default="all",
        help="Type of metadata to import (default: all)",
    )

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

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the input file but don't perform the import",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing metadata if it exists",
    )

    return parser.parse_args()


def validate_metadata(metadata: Dict[str, Any], import_type: str) -> bool:
    """
    Validate the input metadata

    Args:
        metadata: The metadata to validate
        import_type: Type of metadata to import

    Returns:
        True if valid, False otherwise
    """
    # Basic validation
    if "version" not in metadata:
        logger.error("Invalid metadata format: missing version field")
        return False

    # Validate based on import type
    if import_type == "all":
        # Minimal validation for a complete metadata package
        required_sections = [
            "domains",
            "glossary",
            "tags",
            "structured_properties",
            "tests",
        ]
        missing_sections = [s for s in required_sections if s not in metadata]

        if missing_sections:
            logger.warning(
                f"Metadata package is missing sections: {', '.join(missing_sections)}"
            )
            logger.warning("Import will only process available sections")

    elif import_type == "domains":
        if "domains" not in metadata:
            logger.error("Missing 'domains' section in metadata")
            return False

    elif import_type == "glossary":
        if "glossary" not in metadata:
            logger.error("Missing 'glossary' section in metadata")
            return False

        glossary = metadata.get("glossary", {})
        if (
            not isinstance(glossary, dict)
            or "nodes" not in glossary
            or "terms" not in glossary
        ):
            logger.error(
                "Invalid glossary structure: should contain 'nodes' and 'terms'"
            )
            return False

    elif import_type == "tags":
        if "tags" not in metadata:
            logger.error("Missing 'tags' section in metadata")
            return False

    elif import_type == "properties":
        if "structured_properties" not in metadata:
            logger.error("Missing 'structured_properties' section in metadata")
            return False

    elif import_type == "tests":
        if "tests" not in metadata:
            logger.error("Missing 'tests' section in metadata")
            return False

    return True


def main():
    args = parse_args()
    setup_logging(args.log_level)

    # Read input file
    try:
        with open(args.input_file, "r") as f:
            metadata = json.load(f)
    except Exception as e:
        logger.error(f"Error reading input file: {str(e)}")
        sys.exit(1)

    # Validate metadata
    if not validate_metadata(metadata, args.import_type):
        logger.error("Invalid metadata. Import aborted.")
        sys.exit(1)

    # If dry run, exit after validation
    if args.dry_run:
        logger.info("Dry run completed. Input file is valid.")
        sys.exit(0)

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
        client = DataHubMetadataClient(args.server_url, token)
    except Exception as e:
        logger.error(f"Error initializing client: {str(e)}")
        sys.exit(1)

    # Import metadata based on type
    if args.import_type == "all":
        try:
            result = client.import_metadata_from_file(args.input_file)
            if not result:
                logger.error("Failed to import metadata")
                sys.exit(1)
        except Exception as e:
            logger.error(f"Error importing metadata: {str(e)}")
            sys.exit(1)
    else:
        # Import specific sections
        try:
            if args.import_type == "domains":
                domains = metadata.get("domains", [])
                logger.info(f"Importing {len(domains)} domains")

                # TODO: Complete domain import logic
                # This would need to be implemented in the client
                logger.error("Domain import not yet fully implemented")
                sys.exit(1)

            elif args.import_type == "glossary":
                glossary = metadata.get("glossary", {})
                nodes = glossary.get("nodes", [])
                terms = glossary.get("terms", [])

                logger.info(
                    f"Importing {len(nodes)} glossary nodes and {len(terms)} glossary terms"
                )

                # TODO: Complete glossary import logic
                # This would need to be implemented in the client
                logger.error("Glossary import not yet fully implemented")
                sys.exit(1)

            elif args.import_type == "tags":
                tags = metadata.get("tags", [])
                logger.info(f"Importing {len(tags)} tags")

                # TODO: Complete tag import logic
                # This would need to be implemented in the client
                logger.error("Tag import not yet fully implemented")
                sys.exit(1)

            elif args.import_type == "properties":
                properties = metadata.get("structured_properties", [])
                logger.info(f"Importing {len(properties)} structured properties")

                # TODO: Complete properties import logic
                # This would need to be implemented in the client
                logger.error("Structured properties import not yet fully implemented")
                sys.exit(1)

            elif args.import_type == "tests":
                tests = metadata.get("tests", [])
                logger.info(f"Importing {len(tests)} metadata tests")

                # TODO: Complete tests import logic
                # This would need to be implemented in the client
                logger.error("Metadata tests import not yet fully implemented")
                sys.exit(1)

        except Exception as e:
            logger.error(f"Error importing {args.import_type}: {str(e)}")
            sys.exit(1)

    logger.info("Import completed successfully")


if __name__ == "__main__":
    main()
