import json
from django.core.management.base import BaseCommand
import sys
import os

# Add the parent directory to the path to find utils package
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../.."))
)
from utils.datahub_client_adapter import test_datahub_connection
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("glossary_connection_test")


class Command(BaseCommand):
    help = "Test connectivity to DataHub glossary endpoints"

    def add_arguments(self, parser):
        parser.add_argument(
            "--verbose", action="store_true", help="Print detailed diagnostics"
        )

    def handle(self, *args, **options):
        verbose = options.get("verbose", False)

        # Set log level based on verbosity
        if verbose:
            logger.setLevel(logging.DEBUG)

        # Test basic DataHub connection
        self.stdout.write("Testing DataHub connection...")
        connected, client = test_datahub_connection()

        if not connected or not client:
            self.stdout.write(self.style.ERROR("❌ DataHub connection failed"))
            return

        self.stdout.write(self.style.SUCCESS("✅ Basic DataHub connection successful"))

        # Test specific glossary endpoints
        self.stdout.write("\nTesting glossary endpoints...")

        # Test the glossary connection using our new method
        try:
            results = client.test_glossary_connection()

            # Print results summary
            self.stdout.write("\nGlossary API Test Results:")
            self.stdout.write("----------------------------")

            # Business Glossary API
            status = "✅" if results["business_glossary_api"] else "❌"
            self.stdout.write(f"{status} Business Glossary API")

            # Entity Search API
            status = "✅" if results["entity_search_api"] else "❌"
            self.stdout.write(f"{status} Entity Search API")

            # V2 Glossary API
            status = "✅" if results["v2_glossary_api"] else "❌"
            self.stdout.write(f"{status} V2 Glossary API")

            # Legacy Glossary API
            status = "✅" if results["legacy_glossary_api"] else "❌"
            self.stdout.write(f"{status} Legacy Glossary API")

            # Print detailed results if verbose
            if verbose:
                self.stdout.write("\nDetailed Results:")
                self.stdout.write(json.dumps(results["detailed_results"], indent=2))

            # Check if any glossary API is working
            any_glossary_api_working = any(
                [
                    results["business_glossary_api"],
                    results["entity_search_api"],
                    results["v2_glossary_api"],
                    results["legacy_glossary_api"],
                ]
            )

            if any_glossary_api_working:
                self.stdout.write(
                    self.style.SUCCESS(
                        "\n✅ At least one glossary API endpoint is working"
                    )
                )
            else:
                self.stdout.write(
                    self.style.ERROR("\n❌ All glossary API endpoints failed")
                )
                self.stdout.write("Possible issues:")
                self.stdout.write(
                    "1. The DataHub instance may not have the glossary module enabled"
                )
                self.stdout.write(
                    "2. Your authentication token may not have permission to access glossary data"
                )
                self.stdout.write(
                    "3. The API endpoints may have changed in your DataHub version"
                )

            # Now try the actual list_glossary_nodes method
            self.stdout.write("\nTesting list_glossary_nodes method...")
            nodes = client.list_glossary_nodes(count=10)

            if nodes:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"✅ Successfully retrieved {len(nodes)} glossary nodes"
                    )
                )
                if verbose and nodes:
                    self.stdout.write("First node sample:")
                    self.stdout.write(json.dumps(nodes[0], indent=2))
            else:
                self.stdout.write(
                    self.style.WARNING("⚠️ No glossary nodes were returned (empty list)")
                )
                self.stdout.write("This could mean either:")
                self.stdout.write(
                    "1. There are no glossary nodes in your DataHub instance"
                )
                self.stdout.write("2. The API calls are failing silently")

            # Now try list_glossary_terms method
            self.stdout.write("\nTesting list_glossary_terms method...")
            terms = client.list_glossary_terms(count=10)

            if terms:
                if isinstance(terms, dict):
                    num_terms = sum(len(terms_list) for terms_list in terms.values())
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"✅ Successfully retrieved terms for {len(terms)} parent nodes ({num_terms} total terms)"
                        )
                    )
                else:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"✅ Successfully retrieved {len(terms)} glossary terms"
                        )
                    )

                if verbose and terms:
                    if isinstance(terms, dict):
                        self.stdout.write("First parent node with terms:")
                        first_parent = next(iter(terms))
                        self.stdout.write(f"Parent URN: {first_parent}")
                        self.stdout.write(
                            f"Terms: {json.dumps(terms[first_parent][0], indent=2)}"
                        )
                    else:
                        self.stdout.write("First term sample:")
                        self.stdout.write(json.dumps(terms[0], indent=2))
            else:
                self.stdout.write(
                    self.style.WARNING("⚠️ No glossary terms were returned")
                )
                self.stdout.write("This could mean either:")
                self.stdout.write(
                    "1. There are no glossary terms in your DataHub instance"
                )
                self.stdout.write("2. The API calls are failing silently")

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Error testing glossary connection: {str(e)}")
            )
