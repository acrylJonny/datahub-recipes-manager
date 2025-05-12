#!/bin/bash
# Test template rendering for instances
cd "$(dirname "$0")/.." || exit  # Move to project root

# Test rendering recipes with different parameter sources
echo "Rendering recipes from parameters..."
for instance in recipes/instances/dev/*.yml; do
  echo "Testing render for $instance"
  template_type=$(grep "recipe_type" "$instance" | cut -d ":" -f2 | tr -d ' ')
  python -c "
import sys, yaml, os
sys.path.append('utils')
from template_renderer import render_template

# Load instance config
with open('$instance', 'r') as f:
    instance_config = yaml.safe_load(f)

# Render template with parameters
parameters = instance_config.get('parameters', {})
template_path = 'recipes/templates/${template_type}.yml'
try:
    rendered = render_template(template_path, parameters)
    print(f'✅ Successfully rendered template for {os.path.basename(\"$instance\")}')
except Exception as e:
    print(f'❌ Error rendering template: {str(e)}')
    sys.exit(1)
"
done