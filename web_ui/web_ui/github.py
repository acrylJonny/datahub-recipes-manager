import re
import base64
import time
import logging
import requests
from web_ui.models import GitSettings, GitHubPR

logger = logging.getLogger(__name__)


class GitHubIntegration:
    """
    Integration with GitHub for recipe management.
    Handles repository operations including pull requests, commits, and status checks.
    """

    @staticmethod
    def is_configured():
        """Check if GitHub integration is configured."""
        try:
            settings = GitSettings.objects.first()
            return settings is not None and settings.token and settings.repository
        except Exception as e:
            logger.error(f"Error checking GitHub configuration: {str(e)}")
            return False

    @staticmethod
    def get_repo_url():
        """Get the repository URL based on settings."""
        settings = GitSettings.objects.first()
        if not settings:
            return None
        return f"https://github.com/{settings.username}/{settings.repository}"

    @staticmethod
    def get_api_url(endpoint=""):
        """Get the GitHub API URL for the configured repository."""
        settings = GitSettings.objects.first()
        if not settings:
            return None
        base_url = (
            f"https://api.github.com/repos/{settings.username}/{settings.repository}"
        )
        return f"{base_url}/{endpoint}" if endpoint else base_url

    @staticmethod
    def get_headers():
        """Get the headers for GitHub API requests."""
        settings = GitSettings.objects.first()
        if not settings or not settings.token:
            return {}
        return {
            "Authorization": f"token {settings.token}",
            "Accept": "application/vnd.github.v3+json",
        }

    @classmethod
    def get_active_prs(cls, recipe_id=None):
        """
        Get active pull requests from GitHub.

        Args:
            recipe_id: Optional recipe ID to filter PRs

        Returns:
            List of PRs
        """
        if not cls.is_configured():
            return []

        # First check the database
        query = {}
        if recipe_id:
            query["recipe_id"] = recipe_id

        # Filter to only include open or pending PRs
        query["pr_status__in"] = ["open", "pending"]

        try:
            return list(GitHubPR.objects.filter(**query))
        except Exception as e:
            logger.error(f"Error fetching active PRs: {str(e)}")
            return []

    @classmethod
    def create_pull_request(
        cls, recipe_id, recipe_name, recipe_content, description=""
    ):
        """
        Create a pull request for a recipe change.

        Args:
            recipe_id: ID of the recipe
            recipe_name: Name of the recipe
            recipe_content: Content of the recipe
            description: PR description

        Returns:
            PR object or None on failure
        """
        if not cls.is_configured():
            return None

        GitSettings.objects.first()
        headers = cls.get_headers()

        # Create a unique branch name
        timestamp = int(time.time())
        branch_name = f"recipe-{recipe_id}-update-{timestamp}"

        # Get the default branch (usually main or master)
        try:
            repo_response = requests.get(cls.get_api_url(), headers=headers)
            repo_response.raise_for_status()
            default_branch = repo_response.json().get("default_branch", "main")
        except Exception as e:
            logger.error(f"Error getting repository info: {str(e)}")
            return None

        # Create a new branch
        try:
            # Get the reference to the default branch
            ref_response = requests.get(
                cls.get_api_url(f"git/refs/heads/{default_branch}"), headers=headers
            )
            ref_response.raise_for_status()

            sha = ref_response.json().get("object", {}).get("sha")

            # Create new branch
            branch_data = {"ref": f"refs/heads/{branch_name}", "sha": sha}

            branch_response = requests.post(
                cls.get_api_url("git/refs"), headers=headers, json=branch_data
            )
            branch_response.raise_for_status()
        except Exception as e:
            logger.error(f"Error creating branch: {str(e)}")
            return None

        # Sanitize recipe name for filename
        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", recipe_name)
        file_path = f"recipes/{recipe_id}/{safe_name}.yml"

        # Check if file exists
        file_exists = False
        try:
            file_check = requests.get(
                cls.get_api_url(f"contents/{file_path}"), headers=headers
            )
            file_exists = file_check.status_code == 200
        except Exception:
            # File doesn't exist, will create it
            pass

        # Create or update file
        try:
            file_data = {
                "message": f"Update recipe {recipe_name}",
                "content": base64.b64encode(recipe_content.encode()).decode(),
                "branch": branch_name,
            }

            # If file exists, we need the SHA
            if file_exists:
                file_data["sha"] = file_check.json().get("sha")

            file_url = cls.get_api_url(f"contents/{file_path}")
            file_response = requests.put(file_url, headers=headers, json=file_data)
            file_response.raise_for_status()
        except Exception as e:
            logger.error(f"Error updating file: {str(e)}")
            return None

        # Create pull request
        try:
            pr_title = f"Update Recipe: {recipe_name}"
            pr_data = {
                "title": pr_title,
                "body": description or f"Update recipe {recipe_name} (ID: {recipe_id})",
                "head": branch_name,
                "base": default_branch,
            }

            pr_response = requests.post(
                cls.get_api_url("pulls"), headers=headers, json=pr_data
            )
            pr_response.raise_for_status()

            pr_json = pr_response.json()

            # Save PR to database
            pr = GitHubPR(
                recipe_id=recipe_id,
                pr_url=pr_json.get("html_url"),
                pr_number=pr_json.get("number"),
                branch_name=branch_name,
                title=pr_title,
                description=description
                or f"Update recipe {recipe_name} (ID: {recipe_id})",
            )
            pr.save()

            return pr
        except Exception as e:
            logger.error(f"Error creating pull request: {str(e)}")
            return None

    @classmethod
    def update_pr_status(cls, pr_number, status):
        """
        Update the status of a PR in the database.

        Args:
            pr_number: PR number
            status: New status ('open', 'merged', 'closed', 'pending')
        """
        try:
            pr = GitHubPR.objects.get(pr_number=pr_number)
            pr.pr_status = status
            pr.save()
            return True
        except GitHubPR.DoesNotExist:
            logger.error(f"PR #{pr_number} not found")
            return False
        except Exception as e:
            logger.error(f"Error updating PR status: {str(e)}")
            return False

    @classmethod
    def get_pr_status(cls, pr_number):
        """
        Get the current status of a PR from GitHub.

        Args:
            pr_number: PR number

        Returns:
            Status string ('open', 'merged', 'closed') or None on failure
        """
        if not cls.is_configured():
            return None

        try:
            headers = cls.get_headers()

            pr_response = requests.get(
                cls.get_api_url(f"pulls/{pr_number}"), headers=headers
            )
            pr_response.raise_for_status()

            pr_data = pr_response.json()

            if pr_data.get("merged"):
                return "merged"
            elif pr_data.get("state") == "closed":
                return "closed"
            else:
                return "open"
        except Exception as e:
            logger.error(f"Error getting PR status: {str(e)}")
            return None

    @classmethod
    def sync_pr_statuses(cls):
        """Sync the statuses of all PRs with GitHub."""
        if not cls.is_configured():
            return

        try:
            for pr in GitHubPR.objects.filter(pr_status__in=["open", "pending"]):
                status = cls.get_pr_status(pr.pr_number)
                if status and status != pr.pr_status:
                    pr.pr_status = status
                    pr.save()
        except Exception as e:
            logger.error(f"Error syncing PR statuses: {str(e)}")
