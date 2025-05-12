#!/bin/bash
# Auto-document CI workflows
# This script runs both workflow analysis and updates workflow descriptions

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
WORKFLOW_DIR=".github/workflows"

# Default options
DRY_RUN=false
OUTPUT_FILE=""
OUTPUT_FORMAT="text"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --output)
      OUTPUT_FILE="$2"
      shift 2
      ;;
    --format)
      OUTPUT_FORMAT="$2"
      shift 2
      ;;
    --dir)
      WORKFLOW_DIR="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

echo "=== DataHub CI Workflow Auto-Documentation Tool ==="
echo "Workflow directory: $WORKFLOW_DIR"

# Step 1: Run workflow analysis
echo -e "\n=== Running workflow analysis ==="
if [ -n "$OUTPUT_FILE" ]; then
  echo "Generating analysis to $OUTPUT_FILE in $OUTPUT_FORMAT format"
  python "$SCRIPT_DIR/analyze_workflows.py" --dir "$WORKFLOW_DIR" --format "$OUTPUT_FORMAT" --output "$OUTPUT_FILE"
else
  echo "Generating analysis to console"
  python "$SCRIPT_DIR/analyze_workflows.py" --dir "$WORKFLOW_DIR" --format "$OUTPUT_FORMAT"
fi

# Step 2: Update workflow descriptions in files
echo -e "\n=== Updating workflow descriptions ==="
if [ "$DRY_RUN" = true ]; then
  echo "Running in dry-run mode (no changes will be made)"
  python "$SCRIPT_DIR/update_workflow_descriptions.py" --dir "$WORKFLOW_DIR" --dry-run
else
  echo "Updating workflow files with descriptions"
  python "$SCRIPT_DIR/update_workflow_descriptions.py" --dir "$WORKFLOW_DIR"
fi

echo -e "\n=== Workflow documentation complete ===" 