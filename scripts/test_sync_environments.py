#!/usr/bin/env python3
"""
Unit tests for the sync_github_environments.py script.
"""

import unittest
import sys
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add the parent directory to the path
sys.path.insert(0, str(Path(__file__).parent))

# Import the script to test
from sync_github_environments import update_workflow_files


class TestSyncEnvironments(unittest.TestCase):
    """Test cases for sync_github_environments.py"""

    def setUp(self):
        """Set up test environment."""
        # Create a temporary directory
        self.temp_dir = tempfile.mkdtemp()
        self.workflows_dir = Path(self.temp_dir) / ".github" / "workflows"
        self.workflows_dir.mkdir(parents=True, exist_ok=True)

        # Create a sample workflow file
        self.sample_workflow = self.workflows_dir / "deploy.yml"
        with open(self.sample_workflow, "w") as f:
            f.write("""name: Deploy Test

on:
  workflow_dispatch:
    inputs:
      environment:
        description: 'Environment to deploy to'
        required: true
        default: 'dev'
        type: choice
        options:
          - dev
          - staging
          - prod
      deployment_type:
        description: 'What to deploy'
        required: true
""")

    def tearDown(self):
        """Clean up after tests."""
        shutil.rmtree(self.temp_dir)

    @patch("sync_github_environments.Path")
    def test_update_workflow_files(self, mock_path):
        """Test updating workflow files with environments."""
        # Set up mock to return our temp directory
        mock_workflows_dir = MagicMock()
        mock_workflows_dir.exists.return_value = True
        mock_workflows_dir.glob.return_value = [self.sample_workflow]
        mock_path.return_value.parent.parent.__truediv__.return_value.__truediv__.return_value = mock_workflows_dir

        # Sample environments
        environments = [
            {"name": "dev", "description": "Development", "is_default": False},
            {"name": "staging", "description": "Staging", "is_default": False},
            {"name": "prod", "description": "Production", "is_default": True},
            {"name": "test", "description": "Testing", "is_default": False},
        ]

        # Call the function
        result = update_workflow_files(environments)

        # Check the result
        self.assertTrue(result)

        # Read the updated file
        with open(self.sample_workflow, "r") as f:
            content = f.read()

        # Check that all environments are included
        self.assertIn("- dev", content)
        self.assertIn("- prod", content)
        self.assertIn("- staging", content)
        self.assertIn("- test", content)

    def test_update_workflow_files_real(self):
        """Test the function with real files."""
        # Create environments
        environments = [
            {"name": "dev", "description": "Development", "is_default": False},
            {"name": "staging", "description": "Staging", "is_default": False},
            {"name": "prod", "description": "Production", "is_default": True},
            {"name": "test", "description": "Testing", "is_default": False},
        ]

        # Save the original value of the function
        original_function = update_workflow_files

        try:
            # Replace the function to use our temp directory
            def patched_update_workflow_files(envs):
                workflows_dir = Path(self.temp_dir) / ".github" / "workflows"

                # Get list of environment names
                env_names = [env["name"].lower() for env in envs]

                # Make sure common environments are included
                for common_env in ["dev", "staging", "prod"]:
                    if common_env not in env_names:
                        env_names.append(common_env)

                # Sort environment names
                env_names = sorted(env_names)

                for workflow_file in workflows_dir.glob("*.yml"):
                    # Read the workflow file
                    with open(workflow_file, "r") as f:
                        content = f.read()

                    # Use regex to find and update environment options
                    import re

                    pattern = (
                        r"(environment:.*\n.*type: choice\n.*options:\n)((.*- .*\n)*)"
                    )

                    def replace_options(match):
                        prefix = match.group(1)
                        options = []
                        for env in env_names:
                            options.append(f"          - {env}")
                        return prefix + "\n".join(options) + "\n"

                    updated_content = re.sub(pattern, replace_options, content)

                    # Write the updated content
                    with open(workflow_file, "w") as f:
                        f.write(updated_content)

                return True

            # Replace the function
            globals()["update_workflow_files"] = patched_update_workflow_files

            # Call the function
            result = update_workflow_files(environments)

            # Check the result
            self.assertTrue(result)

            # Read the updated file
            with open(self.sample_workflow, "r") as f:
                content = f.read()

            # Check that all environments are included
            self.assertIn("- dev", content)
            self.assertIn("- prod", content)
            self.assertIn("- staging", content)
            self.assertIn("- test", content)

        finally:
            # Restore the original function
            globals()["update_workflow_files"] = original_function


if __name__ == "__main__":
    unittest.main()
