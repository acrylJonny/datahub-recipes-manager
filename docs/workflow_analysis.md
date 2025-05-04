# GitHub Workflow Analysis

This document describes the workflow analysis feature that generates intelligent descriptions based on the contents of GitHub workflow files.

## Overview

The workflow analysis system automatically generates comprehensive descriptions of GitHub workflow files by analyzing their structure, triggers, jobs, steps, and other properties. This provides users with better insights about what each workflow does without having to read through the entire YAML file.

## Features

- **Intelligent Descriptions**: Automatically generates human-readable descriptions based on workflow content
- **Trigger Analysis**: Identifies and explains what triggers the workflow (e.g., manual, push, pull request)
- **Environment Detection**: Identifies which environments (dev, staging, prod) the workflow can target
- **Input Analysis**: Lists the required inputs for manual workflow runs
- **Complexity Metrics**: Provides metrics about job and step count
- **Action Recognition**: Identifies the GitHub Actions being used

## Components

### WorkflowAnalyzer Class

The `WorkflowAnalyzer` class in `web_ui/web_ui/utils/workflow_analyzer.py` provides methods to analyze workflow files:

- `analyze_workflow(workflow_content)`: Analyzes a single workflow content string
- `analyze_all_workflows(workflow_dir)`: Analyzes all workflow files in a directory

### Web UI Integration

The workflow analysis is integrated into the GitHub tab of the web UI:

1. The workflow information is fetched from the GitHub repository
2. The `WorkflowAnalyzer` processes each workflow file
3. The results are displayed in a user-friendly format showing:
   - Workflow name
   - Generated description
   - Trigger mechanisms
   - Environments
   - Job and step counts
   - Used actions

### Command-Line Tool

A command-line tool (`scripts/analyze_workflows.py`) is provided to analyze workflows outside the web UI:

```bash
# Analyze all workflows and output in text format
python scripts/analyze_workflows.py

# Analyze workflows in a specific directory and output as YAML
python scripts/analyze_workflows.py --dir path/to/workflows --format yaml

# Save the analysis to a file
python scripts/analyze_workflows.py --output workflow_analysis.json --format json
```

## Example Generated Descriptions

Here are examples of automatically generated descriptions for the workflows:

- **Manage DataHub Ingestion Sources**: "Workflow that runs workflow_dispatch. Checks out repository code. Manages DataHub ingestion sources. Targets dev, qa, prod environments. With 1 job and 11 steps. Requires action, environment, source_id."

- **Manage DataHub Policies**: "Workflow that runs workflow_dispatch. Checks out repository code. Manages DataHub policies. Targets dev, staging, prod environments. With 1 job and 16 steps. Requires action, environment."

- **Manage Environment Variables**: "Workflow that runs workflow_dispatch. Checks out repository code. Manages environment variables. Targets dev, staging, prod environments. With 1 job and 13 steps. Requires action, environment."

## How to Extend

To add more intelligence to the workflow analysis:

1. Modify the `_generate_description` method in `WorkflowAnalyzer` class
2. Add new pattern recognition by updating the action and workflow name detection logic
3. Extract additional workflow information by adding new helper methods 