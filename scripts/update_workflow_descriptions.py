#!/usr/bin/env python
"""
Update GitHub workflow files with intelligent descriptions in comments.

This script analyzes all YAML workflow files in a specified directory (default: .github/workflows)
and adds or updates a comment at the top of each file with an auto-generated description.

Usage:
    python update_workflow_descriptions.py [--dir DIRECTORY] [--dry-run]

Options:
    --dir DIRECTORY      Directory containing workflow files (default: .github/workflows)
    --dry-run            Show what would be changed without making changes

Example:
    python update_workflow_descriptions.py --dry-run
"""

import os
import sys
import re
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
    parser = argparse.ArgumentParser(description="Update GitHub workflow files with intelligent descriptions in comments.")
    parser.add_argument("--dir", default=".github/workflows", help="Directory containing workflow files (default: .github/workflows)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be changed without making changes")
    return parser.parse_args()

def format_description_comment(workflow_info):
    """Format the workflow description as a YAML comment block."""
    comment_lines = []
    comment_lines.append("# =====================================================")
    comment_lines.append(f"# {workflow_info['name']}")
    comment_lines.append("# =====================================================")
    comment_lines.append("# AUTO-GENERATED DESCRIPTION:")
    comment_lines.append(f"# {workflow_info['description']}")
    comment_lines.append("#")
    
    if workflow_info['triggers']:
        comment_lines.append(f"# Triggers: {', '.join(workflow_info['triggers'])}")
    
    if workflow_info['environments']:
        comment_lines.append(f"# Environments: {', '.join(workflow_info['environments'])}")
    
    required_inputs = [i for i in workflow_info['inputs'] if i.get('required', False)]
    if required_inputs:
        input_descriptions = [f"{i['name']} ({i['description']})" for i in required_inputs]
        comment_lines.append(f"# Required inputs: {', '.join(input_descriptions)}")
    
    if workflow_info['actions']:
        comment_lines.append(f"# Actions: {', '.join(workflow_info['actions'])}")
    
    comment_lines.append(f"# Complexity: {workflow_info['jobs']} jobs, {workflow_info['steps']} steps")
    comment_lines.append(f"# Last updated: {datetime.now().strftime('%Y-%m-%d')}")
    comment_lines.append("# =====================================================")
    
    return "\n".join(comment_lines)

def update_workflow_file(file_path, dry_run=False):
    """Update a workflow file with an auto-generated description comment."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Analyze the workflow
        workflow_info = WorkflowAnalyzer.analyze_workflow(content)
        description_comment = format_description_comment(workflow_info)
        
        # Check if file already has an auto-generated description
        auto_desc_pattern = re.compile(r'# ={20,}\n# .*\n# ={20,}\n# AUTO-GENERATED DESCRIPTION:.*?# ={20,}', re.DOTALL)
        
        if auto_desc_pattern.search(content):
            # Replace existing description
            new_content = auto_desc_pattern.sub(description_comment, content)
        else:
            # Add description at the top of the file
            new_content = f"{description_comment}\n\n{content}"
        
        if dry_run:
            print(f"\n{'='*70}\nChanges for {file_path}:\n{'='*70}")
            if new_content == content:
                print("No changes needed.")
            else:
                # Print first 300 characters of the new content
                preview = new_content[:300] + "..." if len(new_content) > 300 else new_content
                print(preview)
        else:
            if new_content != content:
                with open(file_path, 'w') as f:
                    f.write(new_content)
                print(f"Updated: {file_path}")
            else:
                print(f"No changes needed: {file_path}")
        
        return True
    except Exception as e:
        print(f"Error updating {file_path}: {str(e)}")
        return False

def main():
    """Main function to update workflow files with descriptions."""
    args = parse_args()
    
    if not os.path.exists(args.dir):
        print(f"Error: Directory '{args.dir}' does not exist.")
        sys.exit(1)
    
    # Find all workflow files
    workflow_files = []
    for filename in os.listdir(args.dir):
        if filename.endswith((".yml", ".yaml")):
            workflow_files.append(os.path.join(args.dir, filename))
    
    if not workflow_files:
        print(f"No workflow files found in '{args.dir}'.")
        sys.exit(0)
    
    print(f"Found {len(workflow_files)} workflow files.")
    
    if args.dry_run:
        print("Running in dry-run mode. No changes will be made.")
    
    # Update each workflow file
    success_count = 0
    for file_path in workflow_files:
        if update_workflow_file(file_path, args.dry_run):
            success_count += 1
    
    if not args.dry_run:
        print(f"\nSuccessfully updated {success_count} of {len(workflow_files)} workflow files.")
    else:
        print(f"\nDry run complete. {len(workflow_files)} files would be processed.")

if __name__ == "__main__":
    main() 