import base64
import json
import logging
import os
import re
import time
import yaml
from typing import Dict, Optional, Tuple, Union, List

import requests
from django.conf import settings
from django.urls import reverse

from web_ui.models import GitSettings, PullRequest, GitHubPR

logger = logging.getLogger(__name__)

class GitService:
    """Service to handle Git provider API interactions"""
    
    def __init__(self):
        self.settings = GitSettings.get_instance()
        self.provider = self.settings.provider_type
        self.setup_api_settings()
    
    def setup_api_settings(self):
        """Set up API settings based on the Git provider"""
        if self.provider == 'github':
            # GitHub API
            base_url = self.settings.base_url.rstrip('/') if self.settings.base_url else 'https://api.github.com'
            self.api_base = f"{base_url}/repos/{self.settings.username}/{self.settings.repository}"
            self.headers = {
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"token {self.settings.token}",
            }
        elif self.provider == 'azure_devops':
            # Azure DevOps API
            base_url = self.settings.base_url.rstrip('/') if self.settings.base_url else 'https://dev.azure.com'
            org_project = self.settings.username.split('/')
            if len(org_project) != 2:
                logger.error(f"Invalid Azure DevOps username format: {self.settings.username}")
                self.api_base = None
                self.headers = {}
                return
            
            org, project = org_project
            self.api_base = f"{base_url}/{org}/{project}/_apis/git/repositories/{self.settings.repository}"
            auth_token = base64.b64encode(f":{self.settings.token}".encode()).decode()
            self.headers = {
                "Authorization": f"Basic {auth_token}",
                "Content-Type": "application/json"
            }
        elif self.provider == 'gitlab':
            # GitLab API
            base_url = self.settings.base_url.rstrip('/') if self.settings.base_url else 'https://gitlab.com/api/v4'
            # Encode the repository path
            encoded_repo = f"{self.settings.username}%2F{self.settings.repository}"
            self.api_base = f"{base_url}/projects/{encoded_repo}"
            self.headers = {
                "Private-Token": self.settings.token,
                "Content-Type": "application/json"
            }
        elif self.provider == 'bitbucket':
            # Bitbucket API
            if 'bitbucket.org' in (self.settings.base_url or 'bitbucket.org'):
                # Bitbucket Cloud
                base_url = self.settings.base_url.rstrip('/') if self.settings.base_url else 'https://api.bitbucket.org/2.0'
                self.api_base = f"{base_url}/repositories/{self.settings.username}/{self.settings.repository}"
                auth_token = base64.b64encode(f"{self.settings.username}:{self.settings.token}".encode()).decode()
                self.headers = {
                    "Authorization": f"Basic {auth_token}",
                    "Content-Type": "application/json"
                }
            else:
                # Bitbucket Server
                base_url = self.settings.base_url.rstrip('/')
                self.api_base = f"{base_url}/rest/api/1.0/projects/{self.settings.username}/repos/{self.settings.repository}"
                self.headers = {
                    "Authorization": f"Bearer {self.settings.token}",
                    "Content-Type": "application/json"
                }
        else:
            # Other/Custom Git provider
            self.api_base = f"{self.settings.base_url.rstrip('/')}/{self.settings.username}/{self.settings.repository}"
            self.headers = {
                "Authorization": f"token {self.settings.token}",
                "Content-Type": "application/json"
            }
    
    def is_configured(self) -> bool:
        """Check if Git integration is enabled and configured correctly"""
        return (
            self.settings.enabled and
            self.settings.token and
            self.settings.username and
            self.settings.repository and
            self.api_base is not None
        )
    
    def make_api_request(self, method, endpoint, params=None, json_data=None, headers=None):
        """Make an API request to the Git provider"""
        if not self.is_configured():
            logger.warning("Git integration not configured")
            return None
        
        url = f"{self.api_base}{endpoint}"
        request_headers = self.headers.copy()
        if headers:
            request_headers.update(headers)
            
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=request_headers,
                params=params,
                json=json_data
            )
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e}")
            return e.response
        except Exception as e:
            logger.error(f"Error making API request: {e}")
            return None
    
    def get_file_content(self, path: str, ref: str = None) -> Optional[Dict]:
        """
        Get the content of a file from the repository
        
        Args:
            path: Path to the file in the repository
            ref: Branch or commit reference
            
        Returns:
            Dict with file content and metadata or None if not found
        """
        if not self.is_configured():
            logger.warning("Git integration not configured")
            return None
        
        if not ref:
            ref = self.settings.current_branch or "main"
            
        if self.provider == "github":
            # GitHub API
            params = {"ref": ref}
            response = self.make_api_request("GET", f"/contents/{path}", params=params)
            if response and response.status_code == 200:
                return response.json()
            
        elif self.provider == "azure_devops":
            # Azure DevOps API
            params = {"versionDescriptor.version": ref, "api-version": "6.0"}
            response = self.make_api_request("GET", f"/items?path={path}", params=params)
            if response and response.status_code == 200:
                content = response.text
                return {
                    "content": base64.b64encode(content.encode()).decode(),
                    "encoding": "base64",
                    "name": os.path.basename(path),
                    "path": path
                }
                
        elif self.provider == "gitlab":
            # GitLab API
            path_encoded = path.replace('/', '%2F')
            params = {"ref": ref}
            response = self.make_api_request("GET", f"/repository/files/{path_encoded}", params=params)
            if response and response.status_code == 200:
                data = response.json()
                return {
                    "content": data.get("content", ""),
                    "encoding": data.get("encoding", "base64"),
                    "name": os.path.basename(path),
                    "path": path,
                    "sha": data.get("last_commit_id", "")
                }
                
        elif self.provider == "bitbucket":
            # Bitbucket API
            if 'bitbucket.org' in (self.settings.base_url or 'bitbucket.org'):
                # Bitbucket Cloud
                response = self.make_api_request("GET", f"/src/{ref}/{path}")
                if response and response.status_code == 200:
                    content = response.text
                    return {
                        "content": base64.b64encode(content.encode()).decode(),
                        "encoding": "base64",
                        "name": os.path.basename(path),
                        "path": path
                    }
            else:
                # Bitbucket Server
                response = self.make_api_request("GET", f"/browse/{path}?at={ref}&raw")
                if response and response.status_code == 200:
                    content = response.text
                    return {
                        "content": base64.b64encode(content.encode()).decode(),
                        "encoding": "base64",
                        "name": os.path.basename(path),
                        "path": path
                    }
                    
        return None
    
    def delete_file(self, path: str, branch: str = None, message: str = "Delete file") -> bool:
        """
        Delete a file from the repository
        
        Args:
            path: Path to the file to delete
            branch: Branch where the file is located
            message: Commit message for the deletion
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_configured():
            logger.warning("Git integration not configured")
            return False
            
        if not branch:
            branch = self.settings.current_branch or "main"
            
        # Get current file info
        file_info = self.get_file_content(path, ref=branch)
        if not file_info:
            logger.warning(f"File not found for deletion: {path}")
            return False
            
        if self.provider == "github":
            # GitHub API
            data = {
                "message": message,
                "sha": file_info.get("sha"),
                "branch": branch
            }
            response = self.make_api_request("DELETE", f"/contents/{path}", json_data=data)
            if response and response.status_code == 200:
                return True
                
        elif self.provider == "azure_devops":
            # Azure DevOps API - Uses pushes endpoint
            response = self.make_api_request("GET", f"?api-version=6.0")
            if not response or response.status_code != 200:
                return False
                
            repo_info = response.json()
            default_branch = repo_info.get("defaultBranch", "refs/heads/main").replace("refs/heads/", "")
            
            # Get latest commit on branch
            response = self.make_api_request("GET", f"/refs?filter=heads/{branch}&api-version=6.0")
            if not response or response.status_code != 200:
                return False
                
            refs = response.json().get("value", [])
            if not refs:
                return False
                
            ref = refs[0]
            object_id = ref.get("objectId")
            
            # Create push data
            push_data = {
                "refUpdates": [
                    {
                        "name": f"refs/heads/{branch}",
                        "oldObjectId": object_id
                    }
                ],
                "commits": [
                    {
                        "comment": message,
                        "changes": [
                            {
                                "changeType": "delete",
                                "item": {
                                    "path": path
                                }
                            }
                        ]
                    }
                ]
            }
            
            response = self.make_api_request("POST", f"/pushes?api-version=6.0", json_data=push_data)
            if response and (response.status_code == 200 or response.status_code == 201):
                return True
                
        elif self.provider == "gitlab":
            # GitLab API
            path_encoded = path.replace('/', '%2F')
            data = {
                "branch": branch,
                "commit_message": message
            }
            response = self.make_api_request("DELETE", f"/repository/files/{path_encoded}", json_data=data)
            if response and response.status_code == 204:
                return True
                
        elif self.provider == "bitbucket":
            # Bitbucket API differs between Cloud and Server
            if 'bitbucket.org' in (self.settings.base_url or 'bitbucket.org'):
                # Bitbucket Cloud
                # Need to create a commit
                data = {
                    "message": message,
                    "branch": branch,
                }
                headers = {
                    "Content-Type": "application/x-www-form-urlencoded"
                }
                response = self.make_api_request("POST", f"/src", 
                    params={"message": message, "branch": branch, f"{path}": ""},
                    headers=headers)
                if response and response.status_code in [200, 201, 204]:
                    return True
            else:
                # Bitbucket Server
                # Get latest commit on branch
                response = self.make_api_request("GET", f"/branches")
                if not response or response.status_code != 200:
                    return False
                    
                branches = response.json()
                branch_info = None
                for b in branches.get("values", []):
                    if b.get("displayId") == branch:
                        branch_info = b
                        break
                
                if not branch_info:
                    return False
                    
                latest_commit = branch_info.get("latestCommit")
                
                # Create commit
                data = {
                    "message": message,
                    "parents": [latest_commit],
                    "branch": branch,
                    "files": {
                        path: None  # None means delete
                    }
                }
                response = self.make_api_request("POST", f"/commits", json_data=data)
                if response and response.status_code in [200, 201, 204]:
                    return True
        
        return False
    
    def commit_file(self, path: str, content: str, branch: str = None, message: str = "Update file", update: bool = False) -> bool:
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
            logger.warning("Git integration not configured")
            return False
            
        if not branch:
            branch = self.settings.current_branch or "main"
            
        if self.provider == "github":
            # GitHub API
            data = {
                "message": message,
                "content": base64.b64encode(content.encode()).decode(),
                "branch": branch
            }
            
            # If update is True, we need to get the current file's SHA
            if update:
                file_info = self.get_file_content(path, ref=branch)
                if file_info:
                    data["sha"] = file_info.get("sha")
            
            response = self.make_api_request("PUT", f"/contents/{path}", json_data=data)
            if response and response.status_code in [200, 201]:
                return True
                
        elif self.provider == "azure_devops":
            # Azure DevOps API - Uses pushes endpoint
            change_type = "edit" if update else "add"
            
            # Get latest commit on branch
            response = self.make_api_request("GET", f"/refs?filter=heads/{branch}&api-version=6.0")
            if not response or response.status_code != 200:
                return False
                
            refs = response.json().get("value", [])
            if not refs:
                return False
                
            ref = refs[0]
            object_id = ref.get("objectId")
            
            # Create push data
            push_data = {
                "refUpdates": [
                    {
                        "name": f"refs/heads/{branch}",
                        "oldObjectId": object_id
                    }
                ],
                "commits": [
                    {
                        "comment": message,
                        "changes": [
                            {
                                "changeType": change_type,
                                "item": {
                                    "path": path
                                },
                                "newContent": {
                                    "content": content,
                                    "contentType": "rawtext"
                                }
                            }
                        ]
                    }
                ]
            }
            
            response = self.make_api_request("POST", f"/pushes?api-version=6.0", json_data=push_data)
            if response and (response.status_code == 200 or response.status_code == 201):
                return True
                
        elif self.provider == "gitlab":
            # GitLab API
            path_encoded = path.replace('/', '%2F')
            data = {
                "branch": branch,
                "content": content,
                "commit_message": message
            }
            
            method = "PUT" if update else "POST"
            response = self.make_api_request(method, f"/repository/files/{path_encoded}", json_data=data)
            if response and response.status_code in [200, 201]:
                return True
                
        elif self.provider == "bitbucket":
            # Bitbucket API differs between Cloud and Server
            if 'bitbucket.org' in (self.settings.base_url or 'bitbucket.org'):
                # Bitbucket Cloud
                # Uses multipart form data
                files = {
                    path: (path, content)
                }
                response = self.make_api_request("POST", f"/src/{branch}", 
                                              params={"message": message},
                                              headers={"Content-Type": None})
                if response and response.status_code in [200, 201]:
                    return True
            else:
                # Bitbucket Server
                # Get latest commit on branch
                response = self.make_api_request("GET", f"/branches")
                if not response or response.status_code != 200:
                    return False
                    
                branches = response.json()
                branch_info = None
                for b in branches.get("values", []):
                    if b.get("displayId") == branch:
                        branch_info = b
                        break
                
                if not branch_info:
                    return False
                    
                latest_commit = branch_info.get("latestCommit")
                
                # Create commit
                data = {
                    "message": message,
                    "parents": [latest_commit],
                    "branch": branch,
                    "files": {
                        path: {
                            "content": content
                        }
                    }
                }
                response = self.make_api_request("POST", f"/commits", json_data=data)
                if response and response.status_code in [200, 201]:
                    return True
        
        return False
    
    def create_pull_request(self, title: str, body: str, head_branch: str, base_branch: str = None) -> Optional[Dict]:
        """
        Create a pull request
        
        Args:
            title: PR title
            body: PR description
            head_branch: Source branch
            base_branch: Target branch
            
        Returns:
            Dictionary with PR details or None on failure
        """
        if not self.is_configured():
            logger.warning("Git integration not configured")
            return None
            
        if not base_branch:
            # Get default branch
            if self.provider == "github":
                response = self.make_api_request("GET", "")
                if response and response.status_code == 200:
                    base_branch = response.json().get("default_branch", "main")
                else:
                    base_branch = "main"
            elif self.provider == "azure_devops":
                response = self.make_api_request("GET", "?api-version=6.0")
                if response and response.status_code == 200:
                    base_branch = response.json().get("defaultBranch", "refs/heads/main").replace("refs/heads/", "")
                else:
                    base_branch = "main"
            else:
                base_branch = "main"
        
        # Create PR based on provider
        if self.provider == "github":
            data = {
                "title": title,
                "body": body,
                "head": head_branch,
                "base": base_branch
            }
            response = self.make_api_request("POST", "/pulls", json_data=data)
            if response and response.status_code == 201:
                pr_data = response.json()
                # Create PR record
                pr = GitHubPR.objects.create(
                    recipe_id="multiple", # Generic ID
                    pr_url=pr_data.get("html_url"),
                    pr_number=pr_data.get("number"),
                    pr_status='open',
                    branch_name=head_branch,
                    title=title,
                    description=body
                )
                return {
                    "id": pr.id,
                    "number": pr_data.get("number"),
                    "url": pr_data.get("html_url"),
                    "status": "open"
                }
                
        elif self.provider == "azure_devops":
            data = {
                "sourceRefName": f"refs/heads/{head_branch}",
                "targetRefName": f"refs/heads/{base_branch}",
                "title": title,
                "description": body
            }
            response = self.make_api_request("POST", "/pullrequests?api-version=6.0", json_data=data)
            if response and response.status_code == 201:
                pr_data = response.json()
                # Create PR record
                pr = GitHubPR.objects.create(
                    recipe_id="multiple", # Generic ID
                    pr_url=pr_data.get("url"),
                    pr_number=pr_data.get("pullRequestId"),
                    pr_status='open',
                    branch_name=head_branch,
                    title=title,
                    description=body
                )
                return {
                    "id": pr.id,
                    "number": pr_data.get("pullRequestId"),
                    "url": pr_data.get("url"),
                    "status": "open"
                }
                
        elif self.provider == "gitlab":
            data = {
                "source_branch": head_branch,
                "target_branch": base_branch,
                "title": title,
                "description": body
            }
            response = self.make_api_request("POST", "/merge_requests", json_data=data)
            if response and response.status_code == 201:
                mr_data = response.json()
                # Create PR record
                pr = GitHubPR.objects.create(
                    recipe_id="multiple", # Generic ID
                    pr_url=mr_data.get("web_url"),
                    pr_number=mr_data.get("iid"),
                    pr_status='open',
                    branch_name=head_branch,
                    title=title,
                    description=body
                )
                return {
                    "id": pr.id,
                    "number": mr_data.get("iid"),
                    "url": mr_data.get("web_url"),
                    "status": "open"
                }
                
        elif self.provider == "bitbucket":
            if 'bitbucket.org' in (self.settings.base_url or 'bitbucket.org'):
                # Bitbucket Cloud
                data = {
                    "title": title,
                    "description": body,
                    "source": {
                        "branch": {
                            "name": head_branch
                        }
                    },
                    "destination": {
                        "branch": {
                            "name": base_branch
                        }
                    }
                }
                response = self.make_api_request("POST", "/pullrequests", json_data=data)
                if response and response.status_code == 201:
                    pr_data = response.json()
                    # Create PR record
                    pr = GitHubPR.objects.create(
                        recipe_id="multiple", # Generic ID
                        pr_url=pr_data.get("links", {}).get("html", {}).get("href"),
                        pr_number=pr_data.get("id"),
                        pr_status='open',
                        branch_name=head_branch,
                        title=title,
                        description=body
                    )
                    return {
                        "id": pr.id,
                        "number": pr_data.get("id"),
                        "url": pr_data.get("links", {}).get("html", {}).get("href"),
                        "status": "open"
                    }
            else:
                # Bitbucket Server
                data = {
                    "title": title,
                    "description": body,
                    "fromRef": {
                        "id": f"refs/heads/{head_branch}"
                    },
                    "toRef": {
                        "id": f"refs/heads/{base_branch}"
                    }
                }
                response = self.make_api_request("POST", "/pull-requests", json_data=data)
                if response and response.status_code == 201:
                    pr_data = response.json()
                    # Create PR record
                    pr = GitHubPR.objects.create(
                        recipe_id="multiple", # Generic ID
                        pr_url=pr_data.get("links", {}).get("self", [{}])[0].get("href"),
                        pr_number=pr_data.get("id"),
                        pr_status='open',
                        branch_name=head_branch,
                        title=title,
                        description=body
                    )
                    return {
                        "id": pr.id,
                        "number": pr_data.get("id"),
                        "url": pr_data.get("links", {}).get("self", [{}])[0].get("href"),
                        "status": "open"
                    }
        
        return None
    
    def list_files(self, path: str = "", branch: str = None, recursive: bool = False) -> List[Dict]:
        """
        List files in a directory
        
        Args:
            path: Directory path to list
            branch: Branch to list files from
            recursive: Whether to list files recursively
            
        Returns:
            List of file information dictionaries
        """
        if not self.is_configured():
            logger.warning("Git integration not configured")
            return []
            
        if not branch:
            branch = self.settings.current_branch or "main"
            
        result = []
        
        if self.provider == "github":
            # GitHub API
            params = {"ref": branch}
            response = self.make_api_request("GET", f"/contents/{path}", params=params)
            if response and response.status_code == 200:
                items = response.json()
                if isinstance(items, list):
                    for item in items:
                        result.append({
                            "name": item.get("name"),
                            "path": item.get("path"),
                            "type": item.get("type"),
                            "sha": item.get("sha")
                        })
                        if recursive and item.get("type") == "dir":
                            # Recursively list files in subdirectory
                            sub_files = self.list_files(item.get("path"), branch, recursive)
                            result.extend(sub_files)
                else:
                    # Single file
                    result.append({
                        "name": items.get("name"),
                        "path": items.get("path"),
                        "type": items.get("type"),
                        "sha": items.get("sha")
                    })
                    
        elif self.provider == "azure_devops":
            # Azure DevOps API
            params = {
                "versionDescriptor.version": branch,
                "api-version": "6.0",
                "recursionLevel": "Full" if recursive else "OneLevel"
            }
            response = self.make_api_request("GET", f"/items?path={path}&scopePath={path}", params=params)
            if response and response.status_code == 200:
                data = response.json()
                items = data.get("value", [])
                for item in items:
                    # Skip the directory itself
                    if item.get("path") == path:
                        continue
                    result.append({
                        "name": os.path.basename(item.get("path")),
                        "path": item.get("path"),
                        "type": "dir" if item.get("isFolder", False) else "file",
                        "sha": item.get("objectId")
                    })
                    
        elif self.provider == "gitlab":
            # GitLab API
            params = {
                "ref": branch,
                "path": path,
                "recursive": recursive
            }
            response = self.make_api_request("GET", "/repository/tree", params=params)
            if response and response.status_code == 200:
                items = response.json()
                for item in items:
                    result.append({
                        "name": item.get("name"),
                        "path": item.get("path"),
                        "type": item.get("type"),
                        "sha": item.get("id")
                    })
                    
        elif self.provider == "bitbucket":
            if 'bitbucket.org' in (self.settings.base_url or 'bitbucket.org'):
                # Bitbucket Cloud
                params = {}
                response = self.make_api_request("GET", f"/src/{branch}/{path}")
                if response and response.status_code == 200:
                    data = response.json()
                    # Directories
                    dirs = data.get("values", [])
                    for d in dirs:
                        result.append({
                            "name": d.get("path").split("/")[-1],
                            "path": d.get("path"),
                            "type": "dir" if d.get("type") == "commit_directory" else "file",
                            "sha": None  # Bitbucket Cloud doesn't provide SHA in listing
                        })
                        if recursive and d.get("type") == "commit_directory":
                            sub_files = self.list_files(d.get("path"), branch, recursive)
                            result.extend(sub_files)
            else:
                # Bitbucket Server
                params = {
                    "at": branch,
                    "type": "ALL" if recursive else "BOTH"
                }
                path_str = f"?path={path}" if path else ""
                response = self.make_api_request("GET", f"/files{path_str}", params=params)
                if response and response.status_code == 200:
                    data = response.json()
                    for item in data.get("values", []):
                        result.append({
                            "name": item.get("path").split("/")[-1],
                            "path": item.get("path"),
                            "type": "dir" if item.get("type") == "DIRECTORY" else "file",
                            "sha": None  # Not provided in listing
                        })
        
        return result
    
    def get_staged_files(self, branch: str = None) -> List[Dict]:
        """
        Get files that are staged in the current branch
        
        Args:
            branch: Branch to check
            
        Returns:
            List of staged files with actual differences
        """
        if not branch:
            branch = self.settings.current_branch
        
        # For our purposes, we'll consider all files in the current branch
        # that match our pattern for DataHub recipes as "staged"
        
        # Paths to check for staged changes
        paths_to_check = [
            "recipes/templates",
            "recipes/instances",
            "policies",
            "params/environments"
        ]
        
        staged_files = []
        
        # Skip diff checking if we're on main/master branch
        if branch and branch.lower() in ["main", "master"]:
            return []
        
        for path in paths_to_check:
            files = self.list_files(path, branch)
            for file in files:
                if file["type"] == "file":
                    # Check if there's actually a diff for this file
                    diff = self.get_file_diff(file["path"], branch)
                    if diff.strip():  # Only add if diff is not empty
                        # Try to get file content to determine size
                        try:
                            file_content = self.get_file_content(file["path"], ref=branch)
                            if file_content and "content" in file_content:
                                # Add file size if available
                                if file_content.get("encoding") == "base64" and file_content["content"]:
                                    file["size"] = len(base64.b64decode(file_content["content"]))
                        except Exception as e:
                            logger.debug(f"Error getting file size for {file['path']}: {e}")
                        
                        staged_files.append(file)
                elif file["type"] == "dir":
                    # For directories, check recursively
                    sub_files = self.list_files(file["path"], branch, recursive=True)
                    for f in sub_files:
                        if f["type"] == "file":
                            # Check if there's actually a diff for this file
                            diff = self.get_file_diff(f["path"], branch)
                            if diff.strip():  # Only add if diff is not empty
                                # Try to get file content to determine size
                                try:
                                    file_content = self.get_file_content(f["path"], ref=branch)
                                    if file_content and "content" in file_content:
                                        # Add file size if available
                                        if file_content.get("encoding") == "base64" and file_content["content"]:
                                            f["size"] = len(base64.b64decode(file_content["content"]))
                                except Exception as e:
                                    logger.debug(f"Error getting file size for {f['path']}: {e}")
                                
                                staged_files.append(f)
        
        return staged_files
    
    def get_file_diff(self, path: str, branch: str = None) -> str:
        """
        Get the diff for a file between the current branch and main
        
        Args:
            path: Path to the file
            branch: Current branch (defaults to settings.current_branch)
            
        Returns:
            Diff content as string
        """
        if not self.is_configured():
            logger.warning("Git integration not configured")
            return ""
            
        if not branch:
            branch = self.settings.current_branch or "main"
            
        # If branch is main, there's no diff to show
        if branch.lower() in ["main", "master"]:
            return ""
            
        # Get base branch (usually main)
        base_branch = "main"
        
        try:
            # First get content from current branch
            current_content = self.get_file_content(path, ref=branch)
            
            # Try to get content from base branch
            base_content = None
            try:
                base_content = self.get_file_content(path, ref=base_branch)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    # This is expected for new files - log at debug level
                    logger.debug(f"File {path} not found in {base_branch} branch (new file)")
                else:
                    # Only log other HTTP errors as errors
                    logger.error(f"HTTP error getting file from {base_branch}: {str(e)}")
            except Exception as e:
                logger.error(f"Error getting file from {base_branch}: {str(e)}")
                
            # Decode content
            if current_content and "content" in current_content:
                if current_content.get("encoding") == "base64":
                    current_text = base64.b64decode(current_content["content"]).decode("utf-8")
                else:
                    current_text = current_content["content"]
            else:
                current_text = ""
                
            if base_content and "content" in base_content:
                if base_content.get("encoding") == "base64":
                    base_text = base64.b64decode(base_content["content"]).decode("utf-8")
                else:
                    base_text = base_content["content"]
            else:
                base_text = ""
                
            # Special case for new files
            if not base_text and current_text:
                # For new files, create a diff that shows the entire file as added
                lines = current_text.splitlines()
                diff_lines = [
                    f"--- /dev/null",
                    f"+++ b/{path}",
                    f"@@ -0,0 +1,{len(lines)} @@"
                ]
                diff_lines.extend([f"+ {line}" for line in lines])
                return "\n".join(diff_lines)
                
            # Generate standard diff for modified files
            import difflib
            diff = difflib.unified_diff(
                base_text.splitlines(),
                current_text.splitlines(),
                fromfile=f"{path} (base)",
                tofile=f"{path} (current)",
                lineterm=""
            )
            
            return "\n".join(diff)
            
        except Exception as e:
            logger.error(f"Error getting file diff: {e}")
            return f"Error getting diff: {str(e)}"
    
    def commit_yaml_file(self, data: Dict, path: str, branch: str = None, message: str = None) -> bool:
        """
        Commit a YAML file to the repository
        
        Args:
            data: Data to write as YAML
            path: Path where to save the file
            branch: Branch to commit to
            message: Commit message
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert data to YAML
            yaml_content = yaml.dump(data, default_flow_style=False, sort_keys=False)
            
            # Commit the file
            return self.commit_file(
                path=path,
                content=yaml_content,
                branch=branch,
                message=message or f"Update {path}",
                update=True  # Try to update if exists
            )
        except Exception as e:
            logger.error(f"Error committing YAML file: {e}")
            return False
    
    def revert_staged_file(self, path: str, branch: str = None) -> bool:
        """
        Revert/delete a staged file in the repository
        
        Args:
            path: Path to the file to revert/delete
            branch: Branch where the file is located
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_configured():
            logger.warning("Git integration not configured")
            return False
        
        if not branch:
            branch = self.settings.current_branch or "main"
        
        try:
            # Check if the file exists
            file_info = self.get_file_content(path, ref=branch)
            if not file_info:
                logger.warning(f"File not found to revert: {path}")
                return False
            
            # Use delete_file to remove the staged file
            message = f"Revert staged changes for {path}"
            return self.delete_file(path, branch, message)
        except Exception as e:
            logger.error(f"Error reverting staged file: {e}")
            return False 