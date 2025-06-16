#!/usr/bin/env python3
"""
GitHub service module for managing GitHub integration features.
"""

import logging
import requests

logger = logging.getLogger(__name__)


class GitHubService:
    """Service class for GitHub API interactions."""

    def __init__(self, settings):
        """Initialize the GitHub service with settings."""
        self.settings = settings
        self.headers = {
            "Authorization": f"token {settings.token}",
            "Accept": "application/vnd.github.v3+json",
        }

    def is_configured(self):
        """Check if GitHub integration is properly configured."""
        return (
            self.settings
            and hasattr(self.settings, "token")
            and hasattr(self.settings, "username")
            and hasattr(self.settings, "repository")
            and self.settings.token
            and self.settings.username
            and self.settings.repository
        )

    def delete_environment_secret(self, environment: str, name: str) -> bool:
        """
        Delete a GitHub environment secret.

        Args:
            environment: Name of the GitHub environment
            name: Name of the secret to delete

        Returns:
            True if successful, False otherwise
        """
        if not self.is_configured():
            logger.warning("GitHub integration not configured")
            return False

        if not environment:
            logger.warning("Environment name is required")
            return False

        url = f"https://api.github.com/repos/{self.settings.username}/{self.settings.repository}/environments/{environment}/secrets/{name}"

        try:
            response = requests.delete(url, headers=self.headers)

            if response.status_code in [
                204,
                404,
            ]:  # 204: Deleted successfully, 404: Already deleted
                return True
            else:
                logger.error(
                    f"Failed to delete environment secret. Status: {response.status_code}, Response: {response.text}"
                )
                return False

        except Exception as e:
            logger.error(f"Exception deleting environment secret: {str(e)}")
            return False

    def create_environment(
        self,
        name: str,
        wait_timer: int = 0,
        reviewers: list = None,
        prevent_self_review: bool = False,
        protected_branches: bool = False,
    ) -> bool:
        """
        Create or update a GitHub environment.

        Args:
            name: Name of the environment to create or update
            wait_timer: Optional wait timer in minutes (0-43200)
            reviewers: Optional list of reviewers (users or teams) [{'type': 'User|Team', 'id': 123}]
            prevent_self_review: Whether to prevent creators from approving their own deployments
            protected_branches: Whether to only allow protected branches to deploy

        Returns:
            True if successful, False otherwise
        """
        if not self.is_configured():
            logger.warning("GitHub integration not configured")
            return False

        if not name:
            logger.warning("Environment name is required")
            return False

        url = f"https://api.github.com/repos/{self.settings.username}/{self.settings.repository}/environments/{name}"

        # Prepare request payload
        payload = {
            "deployment_branch_policy": {
                "protected_branches": protected_branches,
                "custom_branch_policies": not protected_branches,
            }
        }

        # Add optional parameters if provided
        if wait_timer > 0:
            if wait_timer > 43200:  # Max 30 days (43200 minutes)
                wait_timer = 43200
            payload["wait_timer"] = wait_timer

        if prevent_self_review:
            payload["prevent_self_review"] = prevent_self_review

        if reviewers:
            payload["reviewers"] = reviewers

        try:
            response = requests.put(url, headers=self.headers, json=payload)

            if response.status_code in [200, 201]:
                logger.info(f"Environment '{name}' created/updated successfully")
                return True
            else:
                logger.error(
                    f"Failed to create/update environment. Status: {response.status_code}, Response: {response.text}"
                )
                return False

        except Exception as e:
            logger.error(f"Exception creating/updating environment: {str(e)}")
            return False
