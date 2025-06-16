#!/usr/bin/env python3
"""
Script to synchronize environments between the DataHub Recipes Manager web application
and GitHub repository environments.

This script ensures that any environment created in the web application is also
created in GitHub, allowing for environment-specific policies, secrets, and workflows.
It also updates GitHub workflow files to include the environments in dropdown options.
"""

import os
import sys
import requests
import argparse
import logging
import re
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_github_token():
    """Get GitHub token from environment or .env file."""
    token = os.environ.get("GITHUB_TOKEN")

    if not token:
        # Try to load from .env file
        env_path = Path(__file__).parent.parent / ".env"
        if env_path.exists():
            with open(env_path, "r") as f:
                for line in f:
                    if line.startswith("GITHUB_TOKEN="):
                        token = line.strip().split("=", 1)[1].strip("\"'")
                        break

    return token


def get_github_settings():
    """Get GitHub repository settings from environment or database."""
    # First try environment variables
    repo_owner = os.environ.get("GITHUB_OWNER")
    repo_name = os.environ.get("GITHUB_REPO")

    if not repo_owner or not repo_name:
        # Try to get from Django settings
        try:
            # Add the parent directory to the path so we can import Django settings
            sys.path.insert(0, str(Path(__file__).parent.parent))

            # Set up Django environment with minimal logging
            os.environ.setdefault("DJANGO_SETTINGS_MODULE", "web_ui.web_ui.settings")
            
            # Disable Django logging to avoid database handler issues
            import logging
            logging.disable(logging.CRITICAL)
            
            import django
            django.setup()
            
            # Re-enable logging for our script
            logging.disable(logging.NOTSET)

            # Now import the model
            from web_ui.models import GitSettings

            # Get the settings
            settings = GitSettings.get_instance()
            if settings:
                repo_owner = settings.username
                repo_name = settings.repository
        except Exception as e:
            logger.error(f"Error accessing Django settings: {e}")

    return repo_owner, repo_name


def get_environments_from_webapp():
    """Get environments from the web application database."""
    try:
        # Set up Django environment if not already set
        if "DJANGO_SETTINGS_MODULE" not in os.environ:
            sys.path.insert(0, str(Path(__file__).parent.parent))
            os.environ.setdefault("DJANGO_SETTINGS_MODULE", "web_ui.web_ui.settings")
            
            # Disable Django logging to avoid database handler issues
            import logging
            logging.disable(logging.CRITICAL)
            
            import django
            django.setup()
            
            # Re-enable logging for our script
            logging.disable(logging.NOTSET)

        # Import the Environment model
        from web_ui.models import Environment

        # Get all environments
        environments = []
        for env in Environment.objects.all():
            environments.append(
                {
                    "id": env.id,
                    "name": env.name,
                    "description": env.description,
                    "is_default": env.is_default,
                }
            )

        return environments
    except Exception as e:
        logger.error(f"Error accessing environments from database: {e}")
        return []


def get_github_environments(token, repo_owner, repo_name):
    """Get existing environments from GitHub."""
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/environments"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # Extract environment names
        environments = response.json().get("environments", [])
        return [env["name"] for env in environments]
    except Exception as e:
        logger.error(f"Error fetching GitHub environments: {e}")
        return []


def create_github_environment(token, repo_owner, repo_name, env_name):
    """Create a new environment in GitHub."""
    # GitHub API doesn't have a direct endpoint to create environments
    # They are created when you reference them in other API calls
    # We'll create an environment-specific variable to initialize it

    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/environments/{env_name}/variables"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    data = {"name": "ENVIRONMENT_INITIALIZED", "value": "true"}

    try:
        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 201:
            logger.info(f"Created environment '{env_name}' in GitHub")
            return True
        else:
            logger.error(
                f"Failed to create environment '{env_name}': {response.status_code} {response.text}"
            )
            return False
    except Exception as e:
        logger.error(f"Error creating GitHub environment '{env_name}': {e}")
        return False


def update_workflow_files(environments):
    """Update GitHub workflow files to include environments in options."""
    workflows_dir = Path(__file__).parent.parent / ".github" / "workflows"

    if not workflows_dir.exists():
        logger.error(f"Workflows directory not found: {workflows_dir}")
        return False

    # Get list of environment names
    env_names = [env["name"].lower() for env in environments]

    # Make sure common environments are included
    for common_env in ["dev", "staging", "prod"]:
        if common_env not in env_names:
            env_names.append(common_env)

    # Sort environment names
    env_names = sorted(env_names)

    success = True
    for workflow_file in workflows_dir.glob("*.yml"):
        try:
            logger.info(f"Checking workflow file: {workflow_file.name}")

            # Read the workflow file
            with open(workflow_file, "r") as f:
                content = f.read()

            # Use regex to find and update environment options
            pattern = r"(environment:.*\n.*type: choice\n.*options:\n)((.*- .*\n)*)"

            def replace_options(match):
                prefix = match.group(1)
                options = []
                for env in env_names:
                    options.append(f"          - {env}")
                return prefix + "\n".join(options) + "\n"

            updated_content = re.sub(pattern, replace_options, content)

            # Only write to the file if changes were made
            if updated_content != content:
                logger.info(f"Updating environment options in {workflow_file.name}")
                with open(workflow_file, "w") as f:
                    f.write(updated_content)
        except Exception as e:
            logger.error(f"Error updating workflow file {workflow_file.name}: {e}")
            success = False

    return success


def sync_environments():
    """Synchronize environments between the web app and GitHub."""
    # Get GitHub credentials and settings
    token = get_github_token()
    repo_owner, repo_name = get_github_settings()

    if not token or not repo_owner or not repo_name:
        logger.error("Missing GitHub credentials or repository settings")
        return False

    # Get environments from the web app
    webapp_environments = get_environments_from_webapp()

    if not webapp_environments:
        logger.warning("No environments found in the web application")
        return False

    # Get existing GitHub environments
    github_environments = get_github_environments(token, repo_owner, repo_name)

    # Create environments that exist in the web app but not in GitHub
    success = True
    for env in webapp_environments:
        env_name = env["name"].lower()
        if env_name not in github_environments:
            logger.info(f"Creating environment '{env_name}' in GitHub")
            if not create_github_environment(token, repo_owner, repo_name, env_name):
                success = False

    # Update workflow files with the current set of environments
    if not update_workflow_files(webapp_environments):
        success = False

    return success


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Synchronize environments between web app and GitHub"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    args = parser.parse_args()

    if args.dry_run:
        logger.info("Dry run mode - no changes will be made")

        # Get environments from the web app
        webapp_environments = get_environments_from_webapp()
        logger.info(
            f"Found {len(webapp_environments)} environments in the web application:"
        )
        for env in webapp_environments:
            logger.info(f"  - {env['name']} (Default: {env['is_default']})")

        # Get GitHub settings
        token = get_github_token()
        repo_owner, repo_name = get_github_settings()

        if token and repo_owner and repo_name:
            # Get existing GitHub environments
            github_environments = get_github_environments(token, repo_owner, repo_name)

            logger.info(f"Found {len(github_environments)} environments in GitHub:")
            for env in github_environments:
                logger.info(f"  - {env}")

            # Show environments that would be created
            for env in webapp_environments:
                env_name = env["name"].lower()
                if env_name not in github_environments:
                    logger.info(f"Would create environment '{env_name}' in GitHub")

            # Show workflow files that would be updated
            logger.info("Would update environment options in workflow files")
        else:
            logger.error("Missing GitHub credentials or repository settings")
    else:
        # Run the sync
        if sync_environments():
            logger.info("Environment synchronization completed successfully")
        else:
            logger.error("Environment synchronization completed with errors")
            sys.exit(1)


if __name__ == "__main__":
    main()
