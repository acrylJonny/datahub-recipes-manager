#!/usr/bin/env python3
"""
Run metadata tests against DataHub entities.
"""

import argparse
import json
import logging
import os
import sys
from typing import Dict, Any, List, Optional

# Add the parent directory to the sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.datahub_metadata_api import DataHubMetadataApiClient
from utils.token_utils import get_token_from_env
from scripts.metadata_tests.metadata_test_utils import (
    MetadataTest,
    MetadataTestSuite,
    TestResult,
    TestSeverity,
    check_required_fields,
    check_string_length
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
    parser = argparse.ArgumentParser(description="Run metadata tests against DataHub entities")
    
    parser.add_argument(
        "--server-url",
        "-s",
        required=True,
        help="DataHub server URL (e.g., http://localhost:8080)",
    )
    
    parser.add_argument(
        "--output-file",
        "-o",
        help="Output file to save the test results",
    )
    
    parser.add_argument(
        "--entity-type",
        choices=["dataset", "dashboard", "chart", "dataFlow", "dataJob", "glossaryTerm", "glossaryNode", "domain", "container", "tag"],
        help="Type of entity to test (if not specified, all will be tested)",
    )
    
    parser.add_argument(
        "--entity-urn",
        help="Specific entity URN to test",
    )
    
    parser.add_argument(
        "--test-config",
        help="JSON file containing test configuration",
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
        "--fail-on-error",
        action="store_true",
        help="Exit with error code if any tests fail",
    )
    
    return parser.parse_args()


# ==================================
# Test implementations
# ==================================

class DomainRequiredFieldsTest(MetadataTest):
    """Test that domains have all required fields"""
    
    def run(self) -> List[TestResult]:
        results = []
        
        # Get all domains
        domains = self.client.list_domains()
        
        for domain in domains:
            domain_urn = domain.get("urn")
            if not domain_urn:
                continue
                
            # Get detailed domain info
            domain_info = self.client.export_domain(domain_urn)
            if not domain_info:
                results.append(self.create_result(
                    False,
                    f"Failed to retrieve detailed info for domain: {domain_urn}",
                    entity_urn=domain_urn
                ))
                continue
            
            # Check required fields
            required_fields = ["properties.name", "properties.description"]
            success, message, details = check_required_fields(domain_info, required_fields, domain_urn)
            
            results.append(self.create_result(
                success,
                message,
                entity_urn=domain_urn,
                details=details
            ))
            
            # Check name and description length if they exist
            if "properties" in domain_info and "name" in domain_info["properties"]:
                name = domain_info["properties"]["name"]
                name_success, name_message, name_details = check_string_length(
                    name, "name", min_length=2, max_length=100
                )
                
                results.append(self.create_result(
                    name_success,
                    name_message,
                    entity_urn=domain_urn,
                    severity=TestSeverity.WARNING if not name_success else TestSeverity.INFO,
                    details=name_details
                ))
            
            if "properties" in domain_info and "description" in domain_info["properties"]:
                desc = domain_info["properties"]["description"]
                desc_success, desc_message, desc_details = check_string_length(
                    desc, "description", min_length=10, max_length=1000
                )
                
                results.append(self.create_result(
                    desc_success,
                    desc_message,
                    entity_urn=domain_urn,
                    severity=TestSeverity.WARNING if not desc_success else TestSeverity.INFO,
                    details=desc_details
                ))
        
        return results


class GlossaryTermRequiredFieldsTest(MetadataTest):
    """Test that glossary terms have all required fields"""
    
    def run(self) -> List[TestResult]:
        results = []
        
        # Get all glossary terms
        terms = self.client.list_glossary_terms()
        
        for term in terms:
            term_urn = term.get("urn")
            if not term_urn:
                continue
                
            # Get detailed term info
            term_info = self.client.export_glossary_term(term_urn)
            if not term_info:
                results.append(self.create_result(
                    False,
                    f"Failed to retrieve detailed info for glossary term: {term_urn}",
                    entity_urn=term_urn
                ))
                continue
            
            # Check required fields
            required_fields = ["properties.name", "properties.definition"]
            success, message, details = check_required_fields(term_info, required_fields, term_urn)
            
            results.append(self.create_result(
                success,
                message,
                entity_urn=term_urn,
                details=details
            ))
            
            # Check name and definition length if they exist
            if "properties" in term_info and "name" in term_info["properties"]:
                name = term_info["properties"]["name"]
                name_success, name_message, name_details = check_string_length(
                    name, "name", min_length=2, max_length=100
                )
                
                results.append(self.create_result(
                    name_success,
                    name_message,
                    entity_urn=term_urn,
                    severity=TestSeverity.WARNING if not name_success else TestSeverity.INFO,
                    details=name_details
                ))
            
            if "properties" in term_info and "definition" in term_info["properties"]:
                definition = term_info["properties"]["definition"]
                def_success, def_message, def_details = check_string_length(
                    definition, "definition", min_length=10, max_length=5000
                )
                
                results.append(self.create_result(
                    def_success,
                    def_message,
                    entity_urn=term_urn,
                    severity=TestSeverity.WARNING if not def_success else TestSeverity.INFO,
                    details=def_details
                ))
        
        return results


def load_tests(
    client: DataHubMetadataApiClient, 
    entity_type: Optional[str] = None, 
    entity_urn: Optional[str] = None,
    config_file: Optional[str] = None
) -> MetadataTestSuite:
    """
    Load tests based on entity type and configuration
    
    Args:
        client: DataHub metadata client
        entity_type: Type of entity to test
        entity_urn: Specific entity URN to test
        config_file: Test configuration file
        
    Returns:
        Test suite with loaded tests
    """
    suite = MetadataTestSuite("DataHub Metadata Quality Tests", client)
    
    # Add tests based on entity type or include all if not specified
    if not entity_type or entity_type == "domain":
        suite.add_test(DomainRequiredFieldsTest(client))
    
    if not entity_type or entity_type == "glossaryTerm":
        suite.add_test(GlossaryTermRequiredFieldsTest(client))
    
    # TODO: Add more tests for other entity types
    
    # Load custom test configuration
    if config_file:
        try:
            with open(config_file, "r") as f:
                config = json.load(f)
                
            # Apply configuration to tests
            # (Implementation would depend on what customization is needed)
            logger.info(f"Loaded test configuration from {config_file}")
        except Exception as e:
            logger.error(f"Error loading test configuration: {str(e)}")
    
    return suite


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
        # Load and run tests
        test_suite = load_tests(client, args.entity_type, args.entity_urn, args.test_config)
        
        if not test_suite.tests:
            logger.error("No tests loaded.")
            sys.exit(1)
        
        logger.info(f"Running {len(test_suite.tests)} tests...")
        results = test_suite.run_all()
        
        # Determine if there were errors
        has_errors = False
        for test_name, test_results in results.items():
            for result in test_results:
                if not result.success and result.severity in [TestSeverity.ERROR, TestSeverity.CRITICAL]:
                    has_errors = True
                    break
        
        # Save results if output file specified
        if args.output_file:
            test_suite.save_results(results, args.output_file)
        
        # Exit with error code if requested and there were errors
        if args.fail_on_error and has_errors:
            logger.error("One or more tests failed with ERROR or CRITICAL severity.")
            sys.exit(1)
            
        logger.info("Test execution completed successfully.")
        
    except Exception as e:
        logger.error(f"Error running metadata tests: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 