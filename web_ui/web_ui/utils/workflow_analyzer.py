import yaml
import re
import os
import requests
import base64
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class WorkflowAnalyzer:
    """Utility class to analyze GitHub workflow files and generate intelligent descriptions."""
    
    def __init__(self, username, repository, token, base_url=None):
        """
        Initialize the WorkflowAnalyzer with repository details.
        
        Args:
            username: The username/organization name of the repository owner
            repository: The repository name
            token: The Git provider API token
            base_url: Optional base URL for the Git provider API (defaults to GitHub API)
        """
        self.username = username
        self.repository = repository
        self.token = token
        self.base_url = base_url or "https://api.github.com"
        
        # Remove trailing slash if present
        if self.base_url.endswith('/'):
            self.base_url = self.base_url[:-1]
            
        # Set up headers
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {self.token}",
        }
    
    def get_workflows(self, branch='main'):
        """
        Get all workflow files from the repository for a specific branch.
        
        Args:
            branch: The branch to fetch workflows from
            
        Returns:
            List of workflow information dictionaries
        """
        try:
            # Determine the API endpoint based on the base URL
            if "github.com" in self.base_url:
                # GitHub API
                workflows_url = f"{self.base_url}/repos/{self.username}/{self.repository}/contents/.github/workflows?ref={branch}"
            elif "dev.azure.com" in self.base_url or "visualstudio.com" in self.base_url:
                # Azure DevOps API
                project = self.repository
                repo = self.username
                workflows_url = f"{self.base_url}/{project}/_apis/git/repositories/{repo}/items?path=/.github/workflows&versionDescriptor.version={branch}&api-version=6.0"
            elif "gitlab.com" in self.base_url:
                # GitLab API
                encoded_path = "%2Egithub%2Fworkflows"
                workflows_url = f"{self.base_url}/api/v4/projects/{self.username}%2F{self.repository}/repository/tree?path={encoded_path}&ref={branch}"
            elif "bitbucket.org" in self.base_url:
                # Bitbucket API
                workflows_url = f"{self.base_url}/2.0/repositories/{self.username}/{self.repository}/src/{branch}/.github/workflows"
            else:
                # Generic Git API - use GitHub style as default
                workflows_url = f"{self.base_url}/repos/{self.username}/{self.repository}/contents/.github/workflows?ref={branch}"
            
            logger.info(f"Fetching workflows from: {workflows_url}")
            
            response = requests.get(workflows_url, headers=self.headers)
            response.raise_for_status()
            
            # The response format varies between Git providers
            workflows = []
            
            if "github.com" in self.base_url or "dev.azure.com" not in self.base_url and "gitlab.com" not in self.base_url and "bitbucket.org" not in self.base_url:
                # GitHub API style response
                files = response.json()
                
                if not isinstance(files, list):
                    logger.warning(f"Expected a list of files but got: {type(files)}")
                    return []
                
                for file_info in files:
                    if not isinstance(file_info, dict) or 'name' not in file_info:
                        continue
                        
                    if not (file_info['name'].endswith('.yml') or file_info['name'].endswith('.yaml')):
                        continue
                    
                    # Fetch the file content
                    file_url = file_info.get('url', None)
                    if not file_url:
                        file_url = f"{self.base_url}/repos/{self.username}/{self.repository}/contents/.github/workflows/{file_info['name']}?ref={branch}"
                    
                    file_response = requests.get(file_url, headers=self.headers)
                    if file_response.status_code != 200:
                        logger.error(f"Failed to fetch workflow file {file_info['name']}: {file_response.status_code}")
                        continue
                    
                    file_data = file_response.json()
                    if not isinstance(file_data, dict) or 'content' not in file_data:
                        logger.error(f"Invalid file content response for {file_info['name']}")
                        continue
                    
                    content = base64.b64decode(file_data['content']).decode('utf-8')
                    
                    # Analyze the workflow
                    workflow_info = self.analyze_workflow(content)
                    workflow = {
                        'filename': file_info['name'],
                        'name': workflow_info['name'],
                        'description': workflow_info['description'],
                        'on': workflow_info['triggers'],
                        'actions': workflow_info['actions'],
                        'environments': workflow_info['environments'],
                        'inputs': workflow_info['inputs'],
                        'jobs': workflow_info['jobs'],
                        'steps': workflow_info['steps'],
                        'raw_url': file_data.get('html_url', file_info.get('html_url', ''))
                    }
                    workflows.append(workflow)
            
            # For other Git providers, implement specific parsing logic as needed
            # This is a simplified implementation that assumes GitHub API format
            
            return workflows
            
        except Exception as e:
            logger.error(f"Error fetching workflows: {str(e)}")
            return []
    
    @staticmethod
    def analyze_workflow(workflow_content: str) -> Dict:
        """
        Analyze a GitHub workflow file and return a dictionary with key information.
        
        Args:
            workflow_content: The content of the workflow file as a string
            
        Returns:
            Dictionary with extracted workflow information
        """
        try:
            # Parse YAML content with safe_load
            try:
                # First, try to parse as YAML
                workflow_data = yaml.safe_load(workflow_content)
            except yaml.YAMLError as yaml_err:
                # If that fails, try to sanitize the content
                logger.warning(f"YAML parsing error: {str(yaml_err)}")
                
                # Return error information
                return {
                    "name": "Error analyzing workflow",
                    "description": f"Error: {str(yaml_err)}",
                    "triggers": [],
                    "actions": [],
                    "environments": [],
                    "inputs": [],
                    "jobs": 0,
                    "steps": 0
                }
            
            if not workflow_data:
                return {
                    "name": "Unknown workflow",
                    "description": "Empty workflow file",
                    "triggers": [],
                    "actions": [],
                    "environments": [],
                    "inputs": [],
                    "jobs": 0,
                    "steps": 0
                }
            
            # Extract workflow name
            name = workflow_data.get("name", "Unnamed workflow")
            
            # Extract triggers
            triggers = WorkflowAnalyzer._extract_triggers(workflow_data)
            
            # Extract environments
            environments = WorkflowAnalyzer._extract_environments(workflow_data)
            
            # Extract inputs
            inputs = WorkflowAnalyzer._extract_inputs(workflow_data)
            
            # Count jobs and steps
            jobs_count, steps_count, actions = WorkflowAnalyzer._count_jobs_and_steps(workflow_data)
            
            # Extract job dependencies
            dependencies = WorkflowAnalyzer._extract_job_dependencies(workflow_data)
            
            # Extract permissions and security info
            permissions = WorkflowAnalyzer._extract_permissions(workflow_data)
            
            # Calculate complexity score
            complexity_score = WorkflowAnalyzer._calculate_complexity(workflow_data, actions, steps_count)
            
            # Estimate execution time
            estimated_time = WorkflowAnalyzer._estimate_execution_time(workflow_data, actions, steps_count)
            
            # Generate description
            description = WorkflowAnalyzer._generate_description(
                name, triggers, actions, environments, inputs, jobs_count, steps_count, 
                dependencies, permissions, complexity_score, estimated_time
            )
            
            return {
                "name": name,
                "description": description,
                "triggers": triggers,
                "actions": actions,
                "environments": environments,
                "inputs": inputs,
                "jobs": jobs_count,
                "steps": steps_count,
                "dependencies": dependencies,
                "permissions": permissions,
                "complexity_score": complexity_score,
                "estimated_time": estimated_time
            }
            
        except Exception as e:
            return {
                "name": "Error analyzing workflow",
                "description": f"Error: {str(e)}",
                "triggers": [],
                "actions": [],
                "environments": [],
                "inputs": [],
                "jobs": 0,
                "steps": 0,
                "dependencies": [],
                "permissions": {},
                "complexity_score": 0,
                "estimated_time": "unknown"
            }
    
    @staticmethod
    def _extract_triggers(workflow_data: Dict) -> List[str]:
        """Extract workflow triggers from the workflow data."""
        triggers = []
        on_field = workflow_data.get("on", {})
        
        if isinstance(on_field, str):
            # Simple trigger like "on: push"
            triggers.append(on_field)
        elif isinstance(on_field, list):
            # List of triggers like "on: [push, pull_request]"
            triggers.extend(on_field)
        elif isinstance(on_field, dict):
            # Complex triggers with configuration
            for trigger, config in on_field.items():
                triggers.append(trigger)
        
        return triggers
    
    @staticmethod
    def _extract_environments(workflow_data: Dict) -> List[str]:
        """Extract environments used in the workflow."""
        environments = set()
        
        # Check for environment in inputs
        if "workflow_dispatch" in workflow_data.get("on", {}):
            inputs = workflow_data.get("on", {}).get("workflow_dispatch", {}).get("inputs", {})
            if "environment" in inputs:
                env_input = inputs.get("environment", {})
                if isinstance(env_input, dict) and "options" in env_input:
                    environments.update(env_input.get("options", []))
        
        # Check for environment in jobs
        for job_id, job_config in workflow_data.get("jobs", {}).items():
            if "environment" in job_config:
                env_value = job_config["environment"]
                if isinstance(env_value, str):
                    # Direct environment reference
                    environments.add(env_value)
                elif isinstance(env_value, dict):
                    # Environment with configuration
                    environments.add(env_value.get("name", ""))
        
        return list(environments)
    
    @staticmethod
    def _extract_inputs(workflow_data: Dict) -> List[Dict]:
        """Extract workflow inputs from workflow_dispatch trigger."""
        inputs = []
        
        if "workflow_dispatch" in workflow_data.get("on", {}):
            input_configs = workflow_data.get("on", {}).get("workflow_dispatch", {}).get("inputs", {})
            
            for input_name, input_config in input_configs.items():
                input_info = {
                    "name": input_name,
                    "description": input_config.get("description", ""),
                    "required": input_config.get("required", False),
                    "type": input_config.get("type", "string")
                }
                
                # Add options if it's a choice type
                if input_config.get("type") == "choice" and "options" in input_config:
                    input_info["options"] = input_config["options"]
                
                inputs.append(input_info)
        
        return inputs
    
    @staticmethod
    def _count_jobs_and_steps(workflow_data: Dict) -> Tuple[int, int, List[str]]:
        """Count jobs and steps in the workflow and extract distinct actions."""
        jobs_count = len(workflow_data.get("jobs", {}))
        steps_count = 0
        actions = set()
        
        for job_id, job_config in workflow_data.get("jobs", {}).items():
            steps = job_config.get("steps", [])
            steps_count += len(steps)
            
            for step in steps:
                if "uses" in step:
                    # Extract action name (e.g., "actions/checkout@v2" -> "checkout")
                    action_path = step["uses"].split("@")[0]
                    action_name = action_path.split("/")[-1] if "/" in action_path else action_path
                    actions.add(action_name)
        
        return jobs_count, steps_count, sorted(list(actions))
    
    @staticmethod
    def _extract_job_dependencies(workflow_data: Dict) -> List[Dict]:
        """Extract job dependencies from the workflow data."""
        dependencies = []
        jobs = workflow_data.get("jobs", {})
        
        for job_id, job_config in jobs.items():
            if isinstance(job_config, dict) and "needs" in job_config:
                needs = job_config["needs"]
                if isinstance(needs, str):
                    dependencies.append({"source": job_id, "target": needs})
                elif isinstance(needs, list):
                    for need in needs:
                        dependencies.append({"source": job_id, "target": need})
        
        return dependencies
    
    @staticmethod
    def _extract_permissions(workflow_data: Dict) -> Dict:
        """Extract permission settings from the workflow data."""
        permissions = {}
        
        # Check for top-level permissions
        if "permissions" in workflow_data:
            perms = workflow_data["permissions"]
            if isinstance(perms, str) and perms in ["read-all", "write-all"]:
                permissions["workflow"] = perms
            elif isinstance(perms, dict):
                permissions["workflow"] = {k: v for k, v in perms.items()}
        
        # Check for job-level permissions
        jobs_with_perms = []
        for job_id, job_config in workflow_data.get("jobs", {}).items():
            if isinstance(job_config, dict) and "permissions" in job_config:
                perms = job_config["permissions"]
                if isinstance(perms, str):
                    # Simple permission string
                    jobs_with_perms.append(job_id)
                elif isinstance(perms, dict):
                    # Detailed permissions
                    jobs_with_perms.append(job_id)
        
        if jobs_with_perms:
            permissions["jobs_with_permissions"] = jobs_with_perms
        
        # Check for specific high-privilege scopes
        high_privilege_scopes = [
            "contents: write", "id-token: write", "packages: write",
            "deployments: write", "actions: write", "security-events: write"
        ]
        sensitive_perms = []
        
        # Check top-level workflow permissions
        if isinstance(permissions.get("workflow"), dict):
            for scope, access in permissions["workflow"].items():
                perm_str = f"{scope}: {access}"
                if perm_str in high_privilege_scopes:
                    sensitive_perms.append(perm_str)
        
        # Check job-level permissions (would require deeper parsing to check each job's permissions)
        permissions["sensitive_permissions"] = sensitive_perms
        
        return permissions
    
    @staticmethod
    def _calculate_complexity(workflow_data: Dict, actions: List[str], steps_count: int) -> int:
        """Calculate a complexity score for the workflow."""
        complexity = 0
        
        # Base complexity from number of steps
        complexity += steps_count * 2
        
        # Add complexity for each job
        complexity += len(workflow_data.get("jobs", {})) * 5
        
        # Add complexity for job dependencies (parallel vs sequential execution)
        for job_id, job_config in workflow_data.get("jobs", {}).items():
            if isinstance(job_config, dict) and "needs" in job_config:
                needs = job_config["needs"]
                if isinstance(needs, list):
                    complexity += len(needs) * 3
                else:
                    complexity += 3
        
        # Add complexity for conditional execution
        for job_id, job_config in workflow_data.get("jobs", {}).items():
            if isinstance(job_config, dict):
                # Check for if conditions on jobs
                if "if" in job_config:
                    complexity += 5
                
                # Check for steps with if conditions
                for step in job_config.get("steps", []):
                    if isinstance(step, dict) and "if" in step:
                        complexity += 2
        
        # Add complexity for specialized actions
        complex_actions = ["docker", "kubernetes", "terraform", "aws", "azure", "gcp"]
        for action in actions:
            if any(ca in action.lower() for ca in complex_actions):
                complexity += 5
        
        # Normalize the complexity score to a 1-10 scale
        normalized_complexity = min(10, max(1, complexity // 10))
        return normalized_complexity
    
    @staticmethod
    def _estimate_execution_time(workflow_data: Dict, actions: List[str], steps_count: int) -> str:
        """Estimate the execution time for the workflow based on its properties."""
        # Base time calculation in seconds
        base_time = 30  # Base overhead for GitHub Actions setup
        
        # Add time for each action based on known averages
        action_times = {
            "checkout": 10,
            "setup-node": 20,
            "setup-python": 20,
            "setup-java": 25,
            "cache": 15,
            "docker": 60,
            "terraform": 60,
            "aws": 30,
            "deploy": 120,
            "test": 120
        }
        
        for action in actions:
            for action_name, time_estimate in action_times.items():
                if action_name in action.lower():
                    base_time += time_estimate
                    break
            else:
                # Default time for unrecognized actions
                base_time += 30
        
        # Add time for steps without actions (run steps)
        steps_with_actions = sum(1 for job_id, job in workflow_data.get("jobs", {}).items() 
                              for step in job.get("steps", []) if "uses" in step)
        run_steps = steps_count - steps_with_actions
        base_time += run_steps * 20  # Average time for run steps
        
        # Adjust for job dependencies (parallel vs sequential)
        jobs = workflow_data.get("jobs", {})
        sequential_chain_length = 0
        
        # Build a dependency graph
        graph = {}
        for job_id, job in jobs.items():
            if isinstance(job, dict) and "needs" in job:
                needs = job["needs"]
                if isinstance(needs, list):
                    graph[job_id] = needs
                else:
                    graph[job_id] = [needs]
            else:
                graph[job_id] = []
        
        # Find longest chain
        visited = set()
        
        def find_longest_path(node, path_length=0):
            if node in visited:
                return path_length
            visited.add(node)
            max_path = path_length
            if node in graph:
                for dep in graph[node]:
                    max_path = max(max_path, find_longest_path(dep, path_length + 1))
            return max_path
        
        for job_id in graph:
            sequential_chain_length = max(sequential_chain_length, find_longest_path(job_id))
        
        # If there's a sequential chain, multiply time by a factor
        if sequential_chain_length > 0:
            base_time *= (1 + (sequential_chain_length * 0.3))
        
        # Convert seconds to a human-readable format
        if base_time < 60:
            return "< 1 minute"
        elif base_time < 300:
            return "1-5 minutes"
        elif base_time < 600:
            return "5-10 minutes"
        elif base_time < 1800:
            return "10-30 minutes"
        else:
            return "30+ minutes"
    
    @staticmethod
    def _generate_description(
        name: str, 
        triggers: List[str], 
        actions: List[str],
        environments: List[str], 
        inputs: List[Dict], 
        jobs_count: int, 
        steps_count: int,
        dependencies: List[Dict],
        permissions: Dict,
        complexity_score: int,
        estimated_time: str
    ) -> str:
        """Generate a comprehensive description of the workflow."""
        description_parts = []
        
        # Start with a basic description based on the name and triggers
        trigger_desc = ", ".join(triggers) if triggers else "manually"
        description_parts.append(f"Workflow that runs on {trigger_desc}")
        
        # Add information about what the workflow does based on its actions
        if actions:
            # Detect deployment workflows
            deployment_actions = ["deploy", "publish", "release"]
            if any(da in " ".join(actions).lower() for da in deployment_actions) or "deploy" in name.lower():
                description_parts.append("deploys code or artifacts")
            
            # Detect build workflows
            build_actions = ["build", "compile", "package"]
            if any(ba in " ".join(actions).lower() for ba in build_actions) or "build" in name.lower():
                description_parts.append("builds code or packages")
            
            # Detect test workflows
            test_actions = ["test", "lint", "check", "validate"]
            if any(ta in " ".join(actions).lower() for ta in test_actions) or "test" in name.lower():
                description_parts.append("runs tests or validation")
            
            # Detect infrastructure workflows
            infra_actions = ["terraform", "aws", "azure", "gcp", "kubernetes", "k8s"]
            if any(ia in " ".join(actions).lower() for ia in infra_actions):
                description_parts.append("manages infrastructure")
            
            # Specific descriptions based on workflow name patterns
            if "manage-ingestion" in name.lower() or any("ingestion" in a.lower() for a in actions):
                description_parts.append("manages DataHub ingestion sources")
            elif "manage-policy" in name.lower() or any("policy" in a.lower() for a in actions):
                description_parts.append("manages DataHub policies")
            elif "manage-env" in name.lower() or any(("env" in a.lower() or "var" in a.lower()) for a in actions):
                description_parts.append("manages environment variables")
        
        # Add information about environments
        if environments:
            env_list = ", ".join(environments)
            description_parts.append(f"targets {env_list} environments")
        
        # Add information about workflow complexity
        complexity_terms = {
            1: "very simple",
            2: "simple",
            3: "straightforward",
            4: "moderate",
            5: "average",
            6: "somewhat complex",
            7: "complex",
            8: "very complex",
            9: "highly complex",
            10: "extremely complex"
        }
        complexity_term = complexity_terms.get(complexity_score, "moderate")
        
        # Add workflow structure information
        if dependencies:
            description_parts.append(f"uses a {complexity_term} structure with dependent jobs")
        else:
            if jobs_count > 1:
                description_parts.append(f"uses a {complexity_term} structure with {jobs_count} parallel jobs")
            else:
                description_parts.append(f"has a {complexity_term} structure")
        
        # Add performance insights
        description_parts.append(f"typically completes in {estimated_time}")
        
        # Add permission insights
        sensitive_perms = permissions.get("sensitive_permissions", [])
        if sensitive_perms:
            description_parts.append("requires elevated permissions")
        
        # Join all parts with appropriate conjunctions
        description = ". This workflow ".join(part.capitalize() for part in description_parts)
        if not description.endswith("."):
            description += "."
            
        return description
    
    @staticmethod
    def analyze_all_workflows(workflow_dir: str) -> Dict[str, Dict]:
        """
        Analyze all workflow files in a directory and return a dictionary of workflow information.
        
        Args:
            workflow_dir: Path to the directory containing workflow files
            
        Returns:
            Dictionary mapping filenames to workflow information
        """
        result = {}
        
        if not os.path.exists(workflow_dir) or not os.path.isdir(workflow_dir):
            return result
            
        for filename in os.listdir(workflow_dir):
            if filename.endswith((".yml", ".yaml")):
                filepath = os.path.join(workflow_dir, filename)
                try:
                    with open(filepath, "r") as f:
                        content = f.read()
                    
                    workflow_info = WorkflowAnalyzer.analyze_workflow(content)
                    result[filename] = workflow_info
                except Exception as e:
                    result[filename] = {
                        "name": f"Error analyzing {filename}",
                        "description": f"Error: {str(e)}",
                        "triggers": [],
                        "actions": [],
                        "environments": [],
                        "inputs": [],
                        "jobs": 0,
                        "steps": 0
                    }
        
        return result 