#!/usr/bin/env python3
"""
Run assertions for a dataset in DataHub.
This script runs all assertions associated with a specific dataset.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime

# Add the parent directory to the sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.datahub_metadata_api import DataHubMetadataApiClient
from utils.token_utils import get_token_from_env
from scripts.assertions.assertion_utils import format_assertion_result

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
    parser = argparse.ArgumentParser(description="Run assertions for a dataset in DataHub")
    
    parser.add_argument(
        "--server-url",
        "-s",
        required=True,
        help="DataHub server URL (e.g., http://localhost:8080)",
    )
    
    parser.add_argument(
        "--dataset-urn",
        "-d",
        required=True,
        help="DataHub dataset URN",
    )
    
    parser.add_argument(
        "--tag-urns",
        "-t",
        nargs="*",
        help="List of tag URNs to filter assertions (optional)",
    )
    
    parser.add_argument(
        "--output-file",
        "-o",
        help="Output file to save the assertion results (optional)",
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
        "--pretty-print",
        action="store_true",
        help="Pretty print JSON output",
    )
    
    parser.add_argument(
        "--save-results",
        action="store_true",
        default=True,
        help="Save assertion results in DataHub (default: True)",
    )
    
    return parser.parse_args()


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
        # Run assertions for the dataset
        logger.info(f"Running assertions for dataset {args.dataset_urn}...")
        
        results = client.run_assertions_for_asset(
            dataset_urn=args.dataset_urn,
            tag_urns=args.tag_urns,
            save_result=args.save_results
        )
        
        if not results:
            logger.error(f"Failed to run assertions for dataset {args.dataset_urn}")
            sys.exit(1)
        
        # Format the results
        formatted_results = {}
        assertion_results = results.get("results", {})
        for assertion_urn, result in assertion_results.items():
            formatted_results[assertion_urn] = format_assertion_result(result)
        
        # Summarize results
        success_count = 0
        failure_count = 0
        for assertion_urn, result in formatted_results.items():
            status = result.get("status", "UNKNOWN")
            if status == "SUCCESS":
                success_count += 1
            elif status == "FAILURE":
                failure_count += 1
        
        # Create the final output
        output = {
            "version": "1.0",
            "executed_at": datetime.now().isoformat(),
            "dataset_urn": args.dataset_urn,
            "summary": {
                "total": len(formatted_results),
                "succeeded": success_count,
                "failed": failure_count
            },
            "results": formatted_results
        }
        
        # Output results
        if args.output_file:
            # Create output directory if it doesn't exist
            output_dir = os.path.dirname(args.output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
                
            with open(args.output_file, "w") as f:
                json.dump(output, f, indent=4 if args.pretty_print else None)
            logger.info(f"Successfully saved assertion results to {args.output_file}")
        else:
            # Print to stdout
            print(json.dumps(output, indent=4 if args.pretty_print else None))
        
        # Log summary
        logger.info(f"Ran {len(formatted_results)} assertions: {success_count} succeeded, {failure_count} failed")
        
        # Exit with failure if any assertions failed
        if failure_count > 0:
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"Error running assertions: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 