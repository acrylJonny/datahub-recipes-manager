#!/usr/bin/env python
"""
Analyze GitHub workflow files and generate intelligent descriptions.

This script analyzes all YAML workflow files in a specified directory (default: .github/workflows)
and generates intelligent descriptions based on their contents.

Usage:
    python analyze_workflows.py [--dir DIRECTORY] [--output OUTPUT_FILE] [--format {json,yaml,text}]

Options:
    --dir DIRECTORY      Directory containing workflow files (default: .github/workflows)
    --output OUTPUT_FILE Path to output file (if not specified, output to console)
    --format FORMAT      Output format: json, yaml, or text (default: text)

Example:
    python analyze_workflows.py --format yaml --output workflow_descriptions.yaml
"""

import os
import sys
import json
import yaml
import argparse
from datetime import datetime

# Add parent directory to path to allow importing from web_ui
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

try:
    from web_ui.web_ui.utils.workflow_analyzer import WorkflowAnalyzer
except ImportError:
    # If the module is not found, check if we're running in a different environment
    try:
        sys.path.append(os.path.join(parent_dir, 'web_ui'))
        from web_ui.utils.workflow_analyzer import WorkflowAnalyzer
    except ImportError:
        print("Error: Could not import WorkflowAnalyzer. Make sure you're running this script from the project root.")
        sys.exit(1)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Analyze GitHub workflow files and generate intelligent descriptions.")
    parser.add_argument("--dir", default=".github/workflows", help="Directory containing workflow files (default: .github/workflows)")
    parser.add_argument("--output", help="Path to output file (if not specified, output to console)")
    parser.add_argument("--format", choices=["json", "yaml", "text"], default="text", help="Output format (default: text)")
    return parser.parse_args()

def format_text_output(workflows):
    """Format workflow analysis as plain text."""
    output = []
    output.append("# GitHub Workflow Descriptions")
    output.append(f"# Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    for filename, workflow in workflows.items():
        output.append(f"## {workflow['name']} ({filename})")
        output.append(f"Description: {workflow['description']}")
        
        if workflow['triggers']:
            output.append(f"Triggers: {', '.join(workflow['triggers'])}")
        
        if workflow['environments']:
            output.append(f"Environments: {', '.join(workflow['environments'])}")
        
        if workflow['inputs']:
            output.append("Required Inputs:")
            for input_item in [i for i in workflow['inputs'] if i.get('required')]:
                output.append(f"  - {input_item['name']}: {input_item['description']}")
        
        if workflow['actions']:
            output.append(f"Actions: {', '.join(workflow['actions'])}")
        
        output.append(f"Complexity: {workflow['jobs']} jobs, {workflow['steps']} steps\n")
    
    return "\n".join(output)

def main():
    """Main function to analyze workflows and output results."""
    args = parse_args()
    
    if not os.path.exists(args.dir):
        print(f"Error: Directory '{args.dir}' does not exist.")
        sys.exit(1)
    
    # Analyze all workflows in the directory
    workflows = WorkflowAnalyzer.analyze_all_workflows(args.dir)
    
    if not workflows:
        print(f"No workflow files found in '{args.dir}'.")
        sys.exit(0)
    
    # Format the output based on the specified format
    if args.format == "json":
        output = json.dumps(workflows, indent=2)
    elif args.format == "yaml":
        output = yaml.dump(workflows, default_flow_style=False, sort_keys=False)
    else:  # text
        output = format_text_output(workflows)
    
    # Write output to file or console
    if args.output:
        with open(args.output, "w") as f:
            f.write(output)
        print(f"Analysis written to '{args.output}'.")
    else:
        print(output)

if __name__ == "__main__":
    main() 