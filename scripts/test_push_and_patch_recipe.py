#!/usr/bin/env python3
"""
Test script for testing recipe push and patch functionality.
Tests the integration between push_recipe.py, patch_recipe.py, and the DataHub REST client.
"""

import json
import logging
import os
import sys
import unittest
from unittest.mock import patch

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.datahub_rest_client import DataHubRestClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Load environment variables
DATAHUB_URL = os.environ.get("DATAHUB_GMS_URL", "http://localhost:8080")
DATAHUB_TOKEN = os.environ.get("DATAHUB_TOKEN", "")

# Check if running in CI environment
IN_CI = (
    os.environ.get("CI", "false").lower() == "true"
    or os.environ.get("GITHUB_ACTIONS", "false").lower() == "true"
)

# Skip message for CI environments
CI_SKIP_MESSAGE = "Skipping live integration tests in CI environment"


@unittest.skipIf(IN_CI, CI_SKIP_MESSAGE)
class TestRecipeIntegration(unittest.TestCase):
    """Test class for recipe push and patch functionality."""

    def setUp(self):
        """Set up test environment."""
        self.client = DataHubRestClient(server_url=DATAHUB_URL, token=DATAHUB_TOKEN)

        # Sample recipe for testing
        self.test_recipe = {
            "source": {
                "type": "postgres",
                "config": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "postgres",
                    "username": "postgres",
                    "password": "postgres",
                    "table_pattern": {"allow": ["public.%"]},
                    "schema_pattern": {"allow": ["public"]},
                },
            },
            "sink": {
                "type": "datahub-rest",
                "config": {
                    "server": "${DATAHUB_GMS_URL}",
                    "token": "${DATAHUB_GMS_TOKEN}",
                },
            },
            "pipeline_name": "test-postgres-pipeline",
            "pipeline_version": "0.1",
            "source_to_platform_map": {"postgres": "postgres"},
            "transformers": [],
            "pipeline_config": {"run_id": "test-run", "profiling": {"enabled": True}},
        }

        self.source_id = "test-postgres-source-unit-test"
        self.source_name = "Test Postgres Source (Unit Test)"

    def test_01_push_recipe(self):
        """Test pushing a recipe to DataHub."""
        # Test connection first
        self.assertTrue(self.client.test_connection(), "Failed to connect to DataHub")

        # Create or update the source
        result = self.client.create_ingestion_source(
            name=self.source_name,
            type="postgres",
            recipe=self.test_recipe,
            source_id=self.source_id,
            schedule={"interval": "0 0 * * *", "timezone": "UTC"},
            executor_id="default",
            debug_mode=False,
        )

        self.assertIsNotNone(result, "Failed to create/update ingestion source")

        # Verify source was created
        source = self.client.get_ingestion_source(self.source_id)
        self.assertIsNotNone(
            source, f"Failed to retrieve source {self.source_id} after creation"
        )
        self.assertEqual(
            source.get("name"), self.source_name, "Source name doesn't match"
        )

    def test_02_patch_recipe(self):
        """Test patching an existing recipe."""
        # Get the current source
        source = self.client.get_ingestion_source(self.source_id)
        self.assertIsNotNone(source, f"Source {self.source_id} not found for patching")

        # Get current recipe
        try:
            current_recipe_str = source.get("config", {}).get("recipe", "{}")
            current_recipe = (
                json.loads(current_recipe_str)
                if isinstance(current_recipe_str, str)
                else current_recipe_str
            )
        except Exception as e:
            self.fail(f"Failed to parse current recipe: {e}")

        # Create patch recipe with modified configuration
        patch_recipe = current_recipe.copy()

        # Ensure the necessary keys exist
        if "pipeline_config" not in patch_recipe:
            patch_recipe["pipeline_config"] = {}

        if "profiling" not in patch_recipe["pipeline_config"]:
            patch_recipe["pipeline_config"]["profiling"] = {"enabled": False}
        else:
            # Flip profiling setting
            current_profiling = patch_recipe["pipeline_config"]["profiling"].get(
                "enabled", True
            )
            patch_recipe["pipeline_config"]["profiling"][
                "enabled"
            ] = not current_profiling

        # Patch the source with updated recipe
        patch_result = self.client.patch_ingestion_source(
            urn=self.source_id, name=self.source_name, recipe=patch_recipe
        )

        self.assertIsNotNone(patch_result, "Failed to patch ingestion source")

        # Verify patch was applied
        updated_source = self.client.get_ingestion_source(self.source_id)
        self.assertIsNotNone(
            updated_source, f"Failed to retrieve updated source {self.source_id}"
        )

        # Parse updated recipe and verify changes
        try:
            updated_recipe_str = updated_source.get("config", {}).get("recipe", "{}")
            updated_recipe = (
                json.loads(updated_recipe_str)
                if isinstance(updated_recipe_str, str)
                else updated_recipe_str
            )

            # Check if profiling setting was flipped
            expected_profiling = (
                not current_recipe.get("pipeline_config", {})
                .get("profiling", {})
                .get("enabled", True)
            )
            actual_profiling = (
                updated_recipe.get("pipeline_config", {})
                .get("profiling", {})
                .get("enabled", False)
            )

            self.assertEqual(
                actual_profiling,
                expected_profiling,
                "Profiling setting wasn't updated as expected",
            )
        except Exception as e:
            self.fail(f"Failed to verify recipe changes: {e}")

    def test_03_fallback_mechanism(self):
        """Test fallback to REST API when GraphQL fails."""
        # Create a test for forcing GraphQL to fail and testing fallback
        with patch.object(
            self.client,
            "execute_graphql",
            side_effect=Exception("Forced GraphQL error"),
        ):
            # Try to patch recipe again
            patch_result = self.client.patch_ingestion_source(
                urn=self.source_id,
                name=f"{self.source_name} Updated",
                debug_mode=True,  # Change debug mode to test the update
            )

            self.assertIsNotNone(patch_result, "REST API fallback mechanism failed")

            # Verify fallback worked
            final_source = self.client.get_ingestion_source(self.source_id)
            self.assertIsNotNone(
                final_source, "Failed to retrieve source after REST fallback"
            )
            self.assertEqual(
                final_source.get("name"),
                f"{self.source_name} Updated",
                "Name wasn't updated via REST fallback",
            )
            self.assertTrue(
                final_source.get("config", {}).get("debugMode"),
                "Debug mode wasn't updated via REST fallback",
            )

    def test_04_cleanup(self):
        """Clean up by deleting the test source."""
        # Delete the test source
        deleted = self.client.delete_ingestion_source(self.source_id)
        self.assertTrue(deleted, f"Failed to delete test source {self.source_id}")

        # Verify deletion
        source = self.client.get_ingestion_source(self.source_id)
        self.assertIsNone(
            source, f"Source {self.source_id} still exists after deletion"
        )


def test_recipe_integration():
    """Run the test suite."""
    if IN_CI:
        logger.info(CI_SKIP_MESSAGE)
        return True  # Return success in CI environments

    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestRecipeIntegration))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


if __name__ == "__main__":
    success = test_recipe_integration()
    sys.exit(0 if success else 1)
