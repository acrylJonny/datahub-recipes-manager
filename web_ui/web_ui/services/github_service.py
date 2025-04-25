import base64
import json
import logging
import os
import re
from typing import Dict, Optional, Tuple, Union, List

import requests
from django.conf import settings
from django.urls import reverse

from web_ui.models import GitHubSettings, PullRequest

logger = logging.getLogger(__name__)

class GitHubService:
    """Service to handle GitHub API interactions"""
    
    def __init__(self):
        self.settings = GitHubSettings.get_instance()
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {self.settings.token}",
        }
        self.base_url = f"https://api.github.com/repos/{self.settings.username}/{self.settings.repository}"
    
    def is_configured(self) -> bool:
        """Check if GitHub integration is enabled and configured correctly"""
        return (
            self.settings.enabled and
            self.settings.token and
            self.settings.username and
            self.settings.repository
        )
    
    def get_file_content(self, path: str, ref: str = "main") -> Optional[Dict]:
        """
        Get the content of a file from the repository
        
        Args:
            path: Path to the file in the repository
            ref: Branch or commit reference
            
        Returns:
            Dict with file content and metadata or None if not found
        """
        if not self.is_configured():
            logger.warning("GitHub integration not configured")
            return None
            
        url = f"{self.base_url}/contents/{path}"
        params = {"ref": ref}
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                # File doesn't exist
                return None
            else:
                logger.error(f"Failed to get file content. Status: {response.status_code}, Response: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Exception getting file content: {str(e)}")
            return None
    
    def create_branch(self, branch_name: str, base_branch: str = "main") -> bool:
        """
        Create a new branch in the repository
        
        Args:
            branch_name: Name of the branch to create
            base_branch: Base branch to create from
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_configured():
            logger.warning("GitHub integration not configured")
            return False
            
        # Get the SHA of the latest commit on the base branch
        try:
            response = requests.get(
                f"{self.base_url}/git/ref/heads/{base_branch}",
                headers=self.headers
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to get base branch ref. Status: {response.status_code}, Response: {response.text}")
                return False
                
            base_sha = response.json()["object"]["sha"]
            
            # Create the new branch
            payload = {
                "ref": f"refs/heads/{branch_name}",
                "sha": base_sha
            }
            
            response = requests.post(
                f"{self.base_url}/git/refs",
                headers=self.headers,
                json=payload
            )
            
            if response.status_code == 201:
                return True
            elif response.status_code == 422:
                # Branch might already exist
                logger.warning(f"Branch {branch_name} already exists")
                return True
            else:
                logger.error(f"Failed to create branch. Status: {response.status_code}, Response: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Exception creating branch: {str(e)}")
            return False
    
    def commit_file(self, path: str, content: str, branch: str, message: str, update: bool = False) -> bool:
        """
        Commit a file to the repository
        
        Args:
            path: Path where to save the file
            content: Content of the file
            branch: Branch to commit to
            message: Commit message
            update: Whether this is an update to an existing file
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_configured():
            logger.warning("GitHub integration not configured")
            return False
            
        url = f"{self.base_url}/contents/{path}"
        
        try:
            # If update is True, we need to get the current file's SHA
            sha = None
            if update:
                current_file = self.get_file_content(path, ref=branch)
                if current_file:
                    sha = current_file["sha"]
                else:
                    # File doesn't exist, so treat as a new file
                    update = False
            
            # Prepare the request payload
            payload = {
                "message": message,
                "content": base64.b64encode(content.encode()).decode(),
                "branch": branch
            }
            
            if update and sha:
                payload["sha"] = sha
            
            # Make the request
            response = requests.put(url, headers=self.headers, json=payload)
            
            if response.status_code in [200, 201]:
                return True
            else:
                logger.error(f"Failed to commit file. Status: {response.status_code}, Response: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Exception committing file: {str(e)}")
            return False
    
    def create_pull_request(self, title: str, body: str, head_branch: str, base_branch: str = "main") -> Optional[Dict]:
        """
        Create a pull request
        
        Args:
            title: Title of the pull request
            body: Description of the pull request
            head_branch: Branch with changes
            base_branch: Target branch for the pull request
            
        Returns:
            Dict with pull request data or None if failed
        """
        if not self.is_configured():
            logger.warning("GitHub integration not configured")
            return None
            
        url = f"{self.base_url}/pulls"
        
        payload = {
            "title": title,
            "body": body,
            "head": head_branch,
            "base": base_branch
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            
            if response.status_code == 201:
                return response.json()
            else:
                logger.error(f"Failed to create PR. Status: {response.status_code}, Response: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Exception creating PR: {str(e)}")
            return None
    
    def get_pull_request(self, pr_number: int) -> Optional[Dict]:
        """
        Get details of a pull request
        
        Args:
            pr_number: The pull request number
            
        Returns:
            Dict with pull request data or None if failed
        """
        if not self.is_configured():
            logger.warning("GitHub integration not configured")
            return None
            
        url = f"{self.base_url}/pulls/{pr_number}"
        
        try:
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get PR. Status: {response.status_code}, Response: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Exception getting PR: {str(e)}")
            return None
    
    def update_pull_request_status(self, pull_request: PullRequest) -> bool:
        """
        Update the local status of a pull request based on its GitHub status
        
        Args:
            pull_request: The PullRequest model instance to update
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_configured():
            logger.warning("GitHub integration not configured")
            return False
            
        try:
            pr_data = self.get_pull_request(pull_request.pr_number)
            if not pr_data:
                return False
                
            # Update the status
            if pr_data["merged"]:
                status = "merged"
            elif pr_data["state"] == "closed":
                status = "closed"
            elif pr_data["draft"]:
                status = "draft"
            else:
                status = "open"
                
            # Update the PullRequest model
            pull_request.status = status
            pull_request.save()
            
            return True
            
        except Exception as e:
            logger.error(f"Exception updating PR status: {str(e)}")
            return False
    
    def sync_recipe_to_github(self, recipe_id: str, recipe_name: str, recipe_data: Dict, 
                             recipe_description: str = "", recipe_type: str = "batch", 
                             commit_message: str = None) -> Optional[PullRequest]:
        """
        Sync a recipe to GitHub by creating a branch, committing the recipe data, and creating a PR
        
        Args:
            recipe_id: The ID of the recipe
            recipe_name: The name of the recipe
            recipe_data: The recipe data dictionary
            recipe_description: Optional recipe description
            recipe_type: Type of recipe
            commit_message: Optional custom commit message
            
        Returns:
            The created PullRequest instance or None if failed
        """
        if not self.is_configured():
            logger.warning("GitHub integration not configured")
            return None
            
        try:
            # Create a branch name from the recipe name
            branch_name = f"recipe-{recipe_name.lower().replace(' ', '-')}-{recipe_id}"
            
            # Create the branch
            if not self.create_branch(branch_name):
                logger.error(f"Failed to create branch {branch_name}")
                return None
                
            # Format the recipe data as JSON
            formatted_data = {
                "id": recipe_id,
                "name": recipe_name,
                "description": recipe_description,
                "type": recipe_type,
                "data": recipe_data,
            }
            
            # Format the content nicely
            content = json.dumps(formatted_data, indent=2)
            
            # Create a path for the recipe file
            path = f"recipes/{recipe_type}/{recipe_name.lower().replace(' ', '_')}.json"
            
            # Commit message
            if not commit_message:
                commit_message = f"{'Update'} {recipe_type} recipe: {recipe_name}"
                
            # Commit the file
            if not self.commit_file(path, content, branch_name, commit_message, update=True):
                logger.error(f"Failed to commit recipe file to {path}")
                return None
                
            # Create a pull request
            pr_title = f"{'Update'} {recipe_type} recipe: {recipe_name}"
            pr_body = recipe_description or f"Changes to {recipe_name} recipe"
            
            pr_data = self.create_pull_request(pr_title, pr_body, branch_name)
            if not pr_data:
                logger.error("Failed to create pull request")
                return None
                
            # Create a PullRequest record
            pull_request = PullRequest.objects.create(
                recipe_id=recipe_id,
                pr_number=pr_data["number"],
                pr_url=pr_data["html_url"],
                status="open" if not pr_data["draft"] else "draft",
                title=pr_data["title"],
                description=pr_data["body"],
                branch_name=branch_name
            )
            
            return pull_request
            
        except Exception as e:
            logger.error(f"Exception syncing recipe to GitHub: {str(e)}")
            return None 