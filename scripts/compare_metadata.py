#!/usr/bin/env python3
"""
Compare metadata between two DataHub environments.
Useful for ensuring that two environments have the same metadata definitions.
"""

import argparse
import json
import logging
import os
import sys
from typing import Dict, Any, List, Tuple

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
    parser = argparse.ArgumentParser(
        description="Compare metadata between two DataHub environments"
    )

    parser.add_argument(
        "--source-url",
        required=True,
        help="Source DataHub server URL (e.g., http://source-datahub:8080)",
    )

    parser.add_argument(
        "--target-url",
        required=True,
        help="Target DataHub server URL (e.g., http://target-datahub:8080)",
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
        "--output-file",
        "-o",
        help="Output file to save the comparison results",
    )

    parser.add_argument(
        "--compare-type",
        "-t",
        choices=["all", "domains", "glossary", "tags", "properties", "tests"],
        default="all",
        help="Type of metadata to compare (default: all)",
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)",
    )

    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Only show summary of differences, not detailed differences",
    )

    return parser.parse_args()


def get_entity_name(entity: Dict[str, Any]) -> str:
    """
    Get the name of an entity

    Args:
        entity: The entity

    Returns:
        Name of the entity
    """
    if "name" in entity:
        return entity["name"]
    elif "properties" in entity and "name" in entity["properties"]:
        return entity["properties"]["name"]
    elif "urn" in entity:
        return entity["urn"]
    else:
        return str(entity)


def compare_lists(
    source_list: List[Dict[str, Any]],
    target_list: List[Dict[str, Any]],
    id_field: str = "urn",
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Compare two lists of entities

    Args:
        source_list: Source list
        target_list: Target list
        id_field: Field to use as identifier

    Returns:
        Tuple of (missing_in_target, missing_in_source, different_entities)
    """
    source_dict = {item.get(id_field): item for item in source_list if id_field in item}
    target_dict = {item.get(id_field): item for item in target_list if id_field in item}

    source_ids = set(source_dict.keys())
    target_ids = set(target_dict.keys())

    missing_in_target_ids = source_ids - target_ids
    missing_in_source_ids = target_ids - source_ids
    common_ids = source_ids.intersection(target_ids)

    # Items missing in target
    missing_in_target = [source_dict[id_] for id_ in missing_in_target_ids]

    # Items missing in source
    missing_in_source = [target_dict[id_] for id_ in missing_in_source_ids]

    # Items that exist in both but may have differences
    different_entities = []
    for id_ in common_ids:
        source_item = source_dict[id_]
        target_item = target_dict[id_]

        # Compare simplified versions (ignore timestamps, etc.)
        if not are_entities_equivalent(source_item, target_item):
            different_entities.append(
                {"urn": id_, "source": source_item, "target": target_item}
            )

    return missing_in_target, missing_in_source, different_entities


def are_entities_equivalent(source: Dict[str, Any], target: Dict[str, Any]) -> bool:
    """
    Check if two entities are semantically equivalent

    Args:
        source: Source entity
        target: Target entity

    Returns:
        True if equivalent, False otherwise
    """
    # Compare core properties like name and description
    if "properties" in source and "properties" in target:
        source_props = source["properties"]
        target_props = target["properties"]

        # Compare name
        if source_props.get("name") != target_props.get("name"):
            return False

        # Compare description
        if source_props.get("description") != target_props.get("description"):
            return False

    # For tags, compare color
    if (
        "properties" in source
        and "properties" in target
        and "colorHex" in source["properties"]
    ):
        if source["properties"].get("colorHex") != target["properties"].get("colorHex"):
            return False

    # For structured properties, check definition
    if "definition" in source and "definition" in target:
        source_def = source["definition"]
        target_def = target["definition"]

        if source_def.get("displayName") != target_def.get("displayName"):
            return False

        if source_def.get("description") != target_def.get("description"):
            return False

        if source_def.get("cardinality") != target_def.get("cardinality"):
            return False

    return True


def summarize_differences(comparison_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a summary of the differences

    Args:
        comparison_results: Full comparison results

    Returns:
        Summary of differences
    """
    summary = {}

    for section, data in comparison_results.items():
        if section in ["domains", "glossary", "tags", "structured_properties", "tests"]:
            section_summary = {}

            if section == "glossary":
                # Handle nested glossary structure
                nodes_missing_in_target = data.get("nodes", {}).get(
                    "missing_in_target", []
                )
                nodes_missing_in_source = data.get("nodes", {}).get(
                    "missing_in_source", []
                )
                nodes_different = data.get("nodes", {}).get("different", [])

                terms_missing_in_target = data.get("terms", {}).get(
                    "missing_in_target", []
                )
                terms_missing_in_source = data.get("terms", {}).get(
                    "missing_in_source", []
                )
                terms_different = data.get("terms", {}).get("different", [])

                section_summary["nodes"] = {
                    "missing_in_target_count": len(nodes_missing_in_target),
                    "missing_in_source_count": len(nodes_missing_in_source),
                    "different_count": len(nodes_different),
                }

                section_summary["terms"] = {
                    "missing_in_target_count": len(terms_missing_in_target),
                    "missing_in_source_count": len(terms_missing_in_source),
                    "different_count": len(terms_different),
                }
            else:
                # Regular sections
                missing_in_target = data.get("missing_in_target", [])
                missing_in_source = data.get("missing_in_source", [])
                different = data.get("different", [])

                section_summary["missing_in_target_count"] = len(missing_in_target)
                section_summary["missing_in_source_count"] = len(missing_in_source)
                section_summary["different_count"] = len(different)

                # Add names of entities with differences
                if missing_in_target:
                    section_summary["missing_in_target_names"] = [
                        get_entity_name(entity) for entity in missing_in_target[:10]
                    ]
                    if len(missing_in_target) > 10:
                        section_summary["missing_in_target_names"].append("...")

                if missing_in_source:
                    section_summary["missing_in_source_names"] = [
                        get_entity_name(entity) for entity in missing_in_source[:10]
                    ]
                    if len(missing_in_source) > 10:
                        section_summary["missing_in_source_names"].append("...")

                if different:
                    section_summary["different_names"] = [
                        get_entity_name(entity["source"]) for entity in different[:10]
                    ]
                    if len(different) > 10:
                        section_summary["different_names"].append("...")

            summary[section] = section_summary

    return summary


def main():
    args = parse_args()
    setup_logging(args.log_level)

    # Get tokens
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
        source_token = (
            get_token_from_env("DATAHUB_SOURCE_TOKEN") or get_token_from_env()
        )

    if args.target_token_file:
        try:
            with open(args.target_token_file, "r") as f:
                target_token = f.read().strip()
        except Exception as e:
            logger.error(f"Error reading target token file: {str(e)}")
            sys.exit(1)
    else:
        target_token = (
            get_token_from_env("DATAHUB_TARGET_TOKEN") or get_token_from_env()
        )

    # Initialize clients
    try:
        source_client = DataHubMetadataClient(args.source_url, source_token)
        target_client = DataHubMetadataClient(args.target_url, target_token)
    except Exception as e:
        logger.error(f"Error initializing clients: {str(e)}")
        sys.exit(1)

    # Perform comparison based on type
    comparison_results = {}

    try:
        if args.compare_type in ["all", "domains"]:
            logger.info("Comparing domains...")
            source_domains = source_client.list_domains()
            target_domains = target_client.list_domains()

            missing_in_target, missing_in_source, different = compare_lists(
                source_domains, target_domains
            )

            comparison_results["domains"] = {
                "missing_in_target": missing_in_target,
                "missing_in_source": missing_in_source,
                "different": different,
            }

            logger.info(
                f"Domains comparison complete. Found {len(missing_in_target)} missing in target, {len(missing_in_source)} missing in source, {len(different)} different."
            )

        if args.compare_type in ["all", "glossary"]:
            logger.info("Comparing glossary...")
            # Compare glossary nodes
            source_nodes = source_client.get_root_glossary_nodes()
            target_nodes = target_client.get_root_glossary_nodes()

            nodes_missing_in_target, nodes_missing_in_source, nodes_different = (
                compare_lists(source_nodes, target_nodes)
            )

            # Compare glossary terms
            source_terms = source_client.get_root_glossary_terms()
            target_terms = target_client.get_root_glossary_terms()

            terms_missing_in_target, terms_missing_in_source, terms_different = (
                compare_lists(source_terms, target_terms)
            )

            comparison_results["glossary"] = {
                "nodes": {
                    "missing_in_target": nodes_missing_in_target,
                    "missing_in_source": nodes_missing_in_source,
                    "different": nodes_different,
                },
                "terms": {
                    "missing_in_target": terms_missing_in_target,
                    "missing_in_source": terms_missing_in_source,
                    "different": terms_different,
                },
            }

            logger.info(
                f"Glossary comparison complete. Nodes: {len(nodes_missing_in_target)} missing in target, {len(nodes_missing_in_source)} missing in source, {len(nodes_different)} different. Terms: {len(terms_missing_in_target)} missing in target, {len(terms_missing_in_source)} missing in source, {len(terms_different)} different."
            )

        if args.compare_type in ["all", "tags"]:
            logger.info("Comparing tags...")
            source_tags = source_client.list_all_tags()
            target_tags = target_client.list_all_tags()

            missing_in_target, missing_in_source, different = compare_lists(
                source_tags, target_tags
            )

            comparison_results["tags"] = {
                "missing_in_target": missing_in_target,
                "missing_in_source": missing_in_source,
                "different": different,
            }

            logger.info(
                f"Tags comparison complete. Found {len(missing_in_target)} missing in target, {len(missing_in_source)} missing in source, {len(different)} different."
            )

        if args.compare_type in ["all", "properties"]:
            logger.info("Comparing structured properties...")
            source_props = source_client.list_structured_properties()
            target_props = target_client.list_structured_properties()

            missing_in_target, missing_in_source, different = compare_lists(
                source_props, target_props
            )

            comparison_results["structured_properties"] = {
                "missing_in_target": missing_in_target,
                "missing_in_source": missing_in_source,
                "different": different,
            }

            logger.info(
                f"Structured properties comparison complete. Found {len(missing_in_target)} missing in target, {len(missing_in_source)} missing in source, {len(different)} different."
            )

        if args.compare_type in ["all", "tests"]:
            logger.info("Comparing metadata tests...")
            source_tests = source_client.list_metadata_tests()
            target_tests = target_client.list_metadata_tests()

            missing_in_target, missing_in_source, different = compare_lists(
                source_tests, target_tests
            )

            comparison_results["tests"] = {
                "missing_in_target": missing_in_target,
                "missing_in_source": missing_in_source,
                "different": different,
            }

            logger.info(
                f"Metadata tests comparison complete. Found {len(missing_in_target)} missing in target, {len(missing_in_source)} missing in source, {len(different)} different."
            )

        # Generate summary if requested
        if args.summary_only:
            comparison_results = {"summary": summarize_differences(comparison_results)}

        # Output results
        if args.output_file:
            with open(args.output_file, "w") as f:
                json.dump(comparison_results, f, indent=2)
            logger.info(f"Comparison results saved to {args.output_file}")
        else:
            # Print results to stdout
            summary = summarize_differences(comparison_results)
            print(json.dumps(summary, indent=2))

    except Exception as e:
        logger.error(f"Error performing comparison: {str(e)}")
        sys.exit(1)

    logger.info("Comparison completed successfully")


if __name__ == "__main__":
    main()
