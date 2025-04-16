#!/usr/bin/env python3
"""
Script to test rendering a recipe instance with its template.
Takes a recipe instance path as input and validates the rendered result.
"""

import sys
import yaml
import os
import argparse

# Add utils to path for importing template_renderer
sys.path.append('utils')
from template_renderer import render_template

def test_render_instance(instance_path):
    """
    Test rendering a recipe instance with its template.
    
    Args:
        instance_path: Path to the recipe instance YAML file
        
    Returns:
        True if successful, False if failed
    """
    try:
        print(f"Testing render for {instance_path}")
        
        # Load instance config
        with open(instance_path, 'r') as f:
            instance_config = yaml.safe_load(f)
        
        # Get template path
        template_type = instance_config.get('recipe_type')
        if not template_type:
            print(f'Error: Recipe type not specified in instance configuration: {instance_config}')
            return False
        
        template_path = f'recipes/templates/{template_type}.yml'
        if not os.path.exists(template_path):
            print(f'Error: Template file not found: {template_path}')
            return False
        
        # Render template with parameters
        parameters = instance_config.get('parameters', {})
        try:
            rendered = render_template(template_path, parameters)
            print(f'Successfully rendered template for {os.path.basename(instance_path)}')
            
            # Validate essential keys in the rendered template
            if 'source' not in rendered or not isinstance(rendered['source'], dict):
                print(f'Error: Rendered template missing source configuration')
                return False
                
            if 'type' not in rendered['source']:
                print(f'Error: Rendered template missing source type')
                return False
                
            return True
        except Exception as e:
            print(f'Error rendering template: {str(e)}')
            return False
    except Exception as e:
        print(f'Error processing instance {instance_path}: {str(e)}')
        return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Test rendering recipe instances')
    parser.add_argument('instance_path', help='Path to the recipe instance file')
    
    args = parser.parse_args()
    
    success = test_render_instance(args.instance_path)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 