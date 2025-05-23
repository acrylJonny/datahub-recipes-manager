#!/usr/bin/env python3
"""
DataHub Domain Manager - A unified tool for domain operations.

This script provides a single entry point for all domain management operations,
including listing, getting, creating, updating, and deleting domains.
"""

import argparse
import logging
import os
import sys
import importlib

# Add the parent directory to the sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
        description="DataHub Domain Manager - A unified tool for domain operations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all domains
  python domain_manager.py list --server-url http://localhost:8080
  
  # Get a specific domain
  python domain_manager.py get --server-url http://localhost:8080 --domain-urn urn:li:domain:engineering
  
  # Create a domain
  python domain_manager.py create --server-url http://localhost:8080 --name "Engineering" --description "Engineering domain"
  
  # Update a domain
  python domain_manager.py update --server-url http://localhost:8080 --domain-urn urn:li:domain:engineering --description "Updated description"
  
  # Delete a domain
  python domain_manager.py delete --server-url http://localhost:8080 --domain-urn urn:li:domain:engineering
        """
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Common arguments for all commands
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument(
        "--server-url",
        "-s",
        required=True,
        help="DataHub server URL (e.g., http://localhost:8080)",
    )
    common_parser.add_argument(
        "--token-file",
        help="File containing DataHub access token",
    )
    common_parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)",
    )
    
    # List command
    list_parser = subparsers.add_parser("list", parents=[common_parser], help="List domains")
    list_parser.add_argument(
        "--output-file",
        "-o",
        help="Output file to save the listed domains (optional)",
    )
    list_parser.add_argument(
        "--pretty-print",
        action="store_true",
        help="Pretty print JSON output",
    )
    list_parser.add_argument(
        "--include-entities",
        action="store_true",
        help="Include entities associated with domains (can significantly increase size)",
    )
    
    # Get command
    get_parser = subparsers.add_parser("get", parents=[common_parser], help="Get a specific domain")
    get_parser.add_argument(
        "--domain-urn",
        "-u",
        required=True,
        help="URN of the domain to retrieve (e.g., urn:li:domain:engineering)",
    )
    get_parser.add_argument(
        "--output-file",
        "-o",
        help="Output file to save the domain details (optional)",
    )
    get_parser.add_argument(
        "--pretty-print",
        action="store_true",
        help="Pretty print JSON output",
    )
    get_parser.add_argument(
        "--include-entities",
        action="store_true",
        help="Include entities associated with the domain",
    )
    
    # Create command
    create_parser = subparsers.add_parser("create", parents=[common_parser], help="Create a new domain")
    create_parser.add_argument(
        "--name",
        "-n",
        required=True,
        help="Name of the domain to create",
    )
    create_parser.add_argument(
        "--description",
        "-d",
        help="Description of the domain",
    )
    create_parser.add_argument(
        "--parent-domain",
        help="URN of the parent domain (if creating a sub-domain)",
    )
    create_parser.add_argument(
        "--color",
        help="Color hex code for the domain (e.g., #0077b6)",
    )
    create_parser.add_argument(
        "--icon",
        help="Icon name for the domain (e.g., domain)",
    )
    create_parser.add_argument(
        "--owners",
        help="List of owner URNs, comma-separated (e.g., urn:li:corpuser:alice,urn:li:corpgroup:engineering)",
    )
    create_parser.add_argument(
        "--input-file",
        "-i",
        help="JSON file containing domain definition (alternative to command line arguments)",
    )
    
    # Update command
    update_parser = subparsers.add_parser("update", parents=[common_parser], help="Update an existing domain")
    update_parser.add_argument(
        "--domain-urn",
        "-u",
        required=True,
        help="URN of the domain to update (e.g., urn:li:domain:engineering)",
    )
    update_parser.add_argument(
        "--name",
        "-n",
        help="New name of the domain",
    )
    update_parser.add_argument(
        "--description",
        "-d",
        help="New description of the domain",
    )
    update_parser.add_argument(
        "--parent-domain",
        help="URN of the new parent domain (if changing parent)",
    )
    update_parser.add_argument(
        "--remove-parent",
        action="store_true",
        help="Remove parent domain relationship",
    )
    update_parser.add_argument(
        "--color",
        help="New color hex code for the domain (e.g., #0077b6)",
    )
    update_parser.add_argument(
        "--icon",
        help="New icon name for the domain (e.g., domain)",
    )
    update_parser.add_argument(
        "--add-owners",
        help="List of owner URNs to add, comma-separated (e.g., urn:li:corpuser:alice,urn:li:corpgroup:engineering)",
    )
    update_parser.add_argument(
        "--remove-owners",
        help="List of owner URNs to remove, comma-separated",
    )
    update_parser.add_argument(
        "--input-file",
        "-i",
        help="JSON file containing updated domain definition (alternative to command line arguments)",
    )
    
    # Delete command
    delete_parser = subparsers.add_parser("delete", parents=[common_parser], help="Delete a domain")
    delete_parser.add_argument(
        "--domain-urn",
        "-u",
        required=True,
        help="URN of the domain to delete (e.g., urn:li:domain:engineering)",
    )
    delete_parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Force delete even if domain has children or entity associations",
    )
    delete_parser.add_argument(
        "--confirm",
        action="store_true",
        help="Automatically confirm deletion without prompting (use with caution!)",
    )
    
    return parser.parse_args()


def main():
    args = parse_args()
    setup_logging(args.log_level)
    
    if not args.command:
        print("Error: No command specified")
        print("Use --help for usage information")
        sys.exit(1)
    
    # Import the appropriate module based on the command
    try:
        if args.command == "list":
            module = importlib.import_module("scripts.domains.list_domains")
        elif args.command == "get":
            module = importlib.import_module("scripts.domains.get_domain")
        elif args.command == "create":
            module = importlib.import_module("scripts.domains.create_domain")
        elif args.command == "update":
            module = importlib.import_module("scripts.domains.update_domain")
        elif args.command == "delete":
            module = importlib.import_module("scripts.domains.delete_domain")
        else:
            logger.error(f"Unknown command: {args.command}")
            sys.exit(1)
            
        # Override sys.argv with our parsed args to pass to the module's main function
        sys.argv = [f"scripts/domains/{args.command}_domain.py"]
        
        # Add all arguments
        for key, value in vars(args).items():
            if key != "command" and value is not None:
                if isinstance(value, bool):
                    if value:
                        sys.argv.append(f"--{key.replace('_', '-')}")
                else:
                    sys.argv.append(f"--{key.replace('_', '-')}")
                    sys.argv.append(str(value))
        
        # Call the module's main function
        module.main()
    
    except Exception as e:
        logger.error(f"Error executing command {args.command}: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 