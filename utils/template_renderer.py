#!/usr/bin/env python3
"""
Utility for rendering DataHub recipe templates with parameters.
"""

from typing import Dict, Any

import yaml


def render_template(template_path: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Render a template YAML file with parameters.

    Args:
        template_path: Path to the template YAML file
        parameters: Dictionary of parameters to substitute in the template

    Returns:
        Rendered template as a dictionary
    """
    # Read template file
    with open(template_path, "r") as f:
        template_content = f.read()

    # First pass: Substitute parameters in the YAML string
    for key, value in parameters.items():
        # Skip complex types like lists and dicts - they'll be handled in the YAML parsing
        if not isinstance(value, (str, int, float, bool)):
            continue

        placeholder = "${" + key + "}"
        template_content = template_content.replace(placeholder, str(value))

    # Parse YAML
    template_yaml = yaml.safe_load(template_content)

    # Second pass: Handle complex parameters (lists, dicts)
    def _process_value(value, params):
        if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            # Extract parameter name
            param_name = value[2:-1]
            if param_name in params:
                return params[param_name]
            else:
                # If parameter not found, leave as is
                return value
        elif isinstance(value, dict):
            return {k: _process_value(v, params) for k, v in value.items()}
        elif isinstance(value, list):
            return [_process_value(item, params) for item in value]
        else:
            return value

    # Process the entire YAML structure
    rendered_yaml = _process_value(template_yaml, parameters)

    return rendered_yaml


def render_template_to_string(template_path: str, parameters: Dict[str, Any]) -> str:
    """
    Render a template YAML file with parameters and return as a string.

    Args:
        template_path: Path to the template YAML file
        parameters: Dictionary of parameters to substitute in the template

    Returns:
        Rendered template as a YAML string
    """
    rendered_dict = render_template(template_path, parameters)
    return yaml.dump(rendered_dict, default_flow_style=False, sort_keys=False)


if __name__ == "__main__":
    # Simple test
    import sys

    if len(sys.argv) < 3:
        print("Usage: template_renderer.py <template_path> <parameters_path>")
        sys.exit(1)

    template_path = sys.argv[1]
    parameters_path = sys.argv[2]

    with open(parameters_path, "r") as f:
        parameters = yaml.safe_load(f)

    rendered = render_template_to_string(template_path, parameters)
    print(rendered)
