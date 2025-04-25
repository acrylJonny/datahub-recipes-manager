#!/usr/bin/env python3
"""
Script to validate DataHub ingestion recipe files.
Validates both template files and instance files.
"""

import argparse
import glob
import os
import sys
from pathlib import Path
from typing import Dict, Any, List

import yaml
import jsonschema

# Schema for template files
TEMPLATE_SCHEMA = {
    "type": "object",
    "required": ["source"],
    "properties": {
        "source": {
            "type": "object",
            "required": ["type", "config"],
            "properties": {"type": {"type": "string"}, "config": {"type": "object"}},
        },
        "pipeline_name": {"type": "string"},
        "pipeline_description": {"type": "string"},
        "datahub_api": {
            "type": "object",
            "properties": {"server": {"type": "string"}, "token": {"type": "string"}},
        },
        "executor_id": {"type": "string"},
        "schedule": {
            "type": "object",
            "properties": {"cron": {"type": "string"}, "timezone": {"type": "string"}},
        },
    },
}

# Schema for instance files
INSTANCE_SCHEMA = {
    "type": "object",
    "required": ["recipe_id", "recipe_type", "parameters"],
    "properties": {
        "recipe_id": {"type": "string"},
        "recipe_type": {"type": "string"},
        "description": {"type": "string"},
        "parameters": {"type": "object"},
        "secret_references": {"type": "array", "items": {"type": "string"}},
    },
}


def validate_yaml_file(file_path: str, schema: Dict[str, Any]) -> List[str]:
    """
    Validate a YAML file against a JSON schema

    Args:
        file_path: Path to the YAML file
        schema: JSON schema to validate against

    Returns:
        List of validation errors (empty if valid)
    """
    try:
        # Load YAML file
        with open(file_path, "r") as f:
            yaml_data = yaml.safe_load(f)

        # Validate against schema
        jsonschema.validate(instance=yaml_data, schema=schema)
        return []
    except yaml.YAMLError as e:
        return [f"YAML syntax error in {file_path}: {str(e)}"]
    except jsonschema.exceptions.ValidationError as e:
        return [f"Validation error in {file_path}: {str(e)}"]
    except Exception as e:
        return [f"Unexpected error validating {file_path}: {str(e)}"]


def validate_instance_file(instance_path: str) -> List[str]:
    """
    Validate an instance file, including checking for template existence

    Args:
        instance_path: Path to the instance YAML file

    Returns:
        List of validation errors (empty if valid)
    """
    errors = validate_yaml_file(instance_path, INSTANCE_SCHEMA)
    if errors:
        return errors

    # Load YAML file
    with open(instance_path, "r") as f:
        instance_data = yaml.safe_load(f)

    # Check if the referenced template exists
    recipe_type = instance_data.get("recipe_type")
    if not recipe_type:
        return [f"Missing recipe_type in {instance_path}"]

    # Use a consistent base path for template resolution
    instance_dir = Path(instance_path).parent
    project_root = Path(
        os.path.abspath(__file__)
    ).parent.parent  # Go up from scripts to root
    template_path = project_root / "recipes" / "templates" / f"{recipe_type}.yml"

    # Alternative paths to check
    alt_paths = [
        project_root / "recipes" / "templates" / f"{recipe_type}.yml",
        Path(instance_path).parent.parent.parent / "templates" / f"{recipe_type}.yml",
        Path("recipes") / "templates" / f"{recipe_type}.yml",
    ]

    template_exists = template_path.exists()
    template_path_used = template_path

    # If template not found at primary location, check alternative locations
    if not template_exists:
        for alt_path in alt_paths:
            if alt_path.exists():
                template_exists = True
                template_path_used = alt_path
                break

    if not template_exists:
        return [
            f"Template {recipe_type}.yml referenced in {instance_path} does not exist"
        ]

    # Check parameter references
    parameters = instance_data.get("parameters", {})
    parameters_set = set(parameters.keys())

    # Load template file
    with open(template_path_used, "r") as f:
        template_content = f.read()

    # Find all parameter references in the template
    param_refs = set()
    for line in template_content.split("\n"):
        for param in parameters_set:
            placeholder = "${" + param + "}"
            if placeholder in line:
                param_refs.add(param)

    # Check for parameters that are specified but not used
    unused_params = parameters_set - param_refs
    if unused_params:
        return [
            f"Warning: Unused parameters in {instance_path}: {', '.join(unused_params)}"
        ]

    return []


def main():
    parser = argparse.ArgumentParser(
        description="Validate DataHub ingestion recipe files"
    )
    parser.add_argument("--templates", nargs="+", help="Template files to validate")
    parser.add_argument("--instances", nargs="+", help="Instance files to validate")

    args = parser.parse_args()

    all_errors = []

    # Validate template files
    if args.templates:
        for template_pattern in args.templates:
            for template_path in glob.glob(template_pattern, recursive=True):
                print(f"Validating template: {template_path}")
                errors = validate_yaml_file(template_path, TEMPLATE_SCHEMA)
                if errors:
                    all_errors.extend(errors)
                    for error in errors:
                        print(f"  - {error}")
                else:
                    print(f"  - Valid")

    # Validate instance files
    if args.instances:
        for instance_pattern in args.instances:
            for instance_path in glob.glob(instance_pattern, recursive=True):
                print(f"Validating instance: {instance_path}")
                errors = validate_instance_file(instance_path)
                if errors:
                    all_errors.extend(errors)
                    for error in errors:
                        print(f"  - {error}")
                else:
                    print(f"  - Valid")

    if all_errors:
        print(f"\nFound {len(all_errors)} errors or warnings")
        # Only exit with non-zero status if there are actual errors (not warnings)
        non_warning_errors = [e for e in all_errors if not e.startswith("Warning:")]
        if non_warning_errors:
            return 1
    else:
        print("\nAll files are valid")

    return 0


if __name__ == "__main__":
    sys.exit(main())
