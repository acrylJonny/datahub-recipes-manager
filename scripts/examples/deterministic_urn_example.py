#!/usr/bin/env python3
"""
Example script demonstrating the use of deterministic URNs for DataHub entities.

This script shows how to generate and use deterministic URNs for tags, glossary terms,
domains, and other DataHub entities to ensure consistency across environments.
"""

import argparse
import json
import logging
import os
import sys
from typing import Dict, Any, List

# Add the parent directory to the sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.urn_utils import (
    generate_deterministic_urn,
    get_full_urn_from_name
)

logger = logging.getLogger(__name__)


def setup_logging(log_level: str):
    """Set up logging configuration"""
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Demonstrate deterministic URN features for DataHub entities"
    )
    
    parser.add_argument(
        "--tag-name",
        default="PII",
        help="Tag name to use in examples (default: PII)",
    )
    
    parser.add_argument(
        "--glossary-term-name",
        default="Sensitive Data",
        help="Glossary term name to use in examples (default: Sensitive Data)",
    )
    
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)",
    )
    
    return parser.parse_args()


def demonstrate_deterministic_urns():
    """Show examples of deterministic URN generation"""
    # Example tags
    tag_names = ["PII", "Sensitive", "Confidential", "Public"]
    
    print("\n=== Deterministic Tag URNs (MD5) ===")
    for tag_name in tag_names:
        tag_urn = get_full_urn_from_name("tag", tag_name)
        print(f"Tag '{tag_name}': {tag_urn}")
    
    # Example glossary terms with different parent contexts
    print("\n=== Deterministic Glossary Term URNs (MD5) ===")
    
    # Terms without parent
    term_names = ["Customer Data", "Financial Data", "Product Data"]
    for term_name in term_names:
        term_urn = get_full_urn_from_name("glossaryTerm", term_name)
        print(f"Term '{term_name}' (no parent): {term_urn}")
    
    # Terms with parent nodes
    parent_node_urn = "urn:li:glossaryNode:12345"
    for term_name in term_names:
        term_urn = get_full_urn_from_name("glossaryTerm", term_name, parent_node_urn)
        print(f"Term '{term_name}' (with parent): {term_urn}")
    
    # Example domains
    print("\n=== Deterministic Domain URNs (MD5) ===")
    domain_names = ["Marketing", "Finance", "Engineering", "Sales"]
    for domain_name in domain_names:
        domain_urn = get_full_urn_from_name("domain", domain_name)
        print(f"Domain '{domain_name}': {domain_urn}")
    
    # Example with same name but different entity types
    print("\n=== Different Entity Types with Same Name (MD5) ===")
    name = "Finance"
    entity_types = ["tag", "glossaryTerm", "domain"]
    for entity_type in entity_types:
        urn = get_full_urn_from_name(entity_type, name)
        print(f"{entity_type} '{name}': {urn}")
    
    # Example with different casing/spacing to show normalization
    print("\n=== Normalization Examples (MD5) ===")
    variations = ["sensitive data", "Sensitive Data", "SENSITIVE DATA", " sensitive data "]
    for variation in variations:
        urn = get_full_urn_from_name("glossaryTerm", variation)
        print(f"Term '{variation}': {urn}")


def main():
    """Main function"""
    args = parse_args()
    setup_logging(args.log_level)
    
    # Demonstrate deterministic URN generation
    demonstrate_deterministic_urns()
    
    print("\n=== MD5 Hash Verification ===")
    print("Tag 'PII':")
    md5_hash = generate_deterministic_urn("tag", "PII").split(":")[-1]
    print(f"MD5 Hash: {md5_hash}")
    
    # Verification command example
    print("\nTo verify in terminal:")
    print(f"echo -n 'tag:pii' | md5sum")
    # For macOS
    print(f"echo -n 'tag:pii' | md5")


if __name__ == "__main__":
    main() 