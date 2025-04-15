#!/bin/bash
# Run from the project root directory, not from the test directory
cd "$(dirname "$0")/.." || exit
echo "Running validation from $(pwd)"

# Validate recipe templates
python scripts/validate_recipe.py --templates recipes/templates/*.yml

# Validate recipe instances
python scripts/validate_recipe.py --instances recipes/instances/*.yml