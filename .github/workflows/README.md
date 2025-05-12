# DataHub Recipes Manager Workflows

This directory contains GitHub Actions workflows for automating the testing, validation, and deployment of DataHub recipes and configurations.

## Workflow Overview

### PR Validation (`pr-validation.yml`)

This workflow runs whenever a Pull Request is opened, updated, or reopened against the `main` branch. It performs several validation steps:

1. **Code Validation**:
   - Runs linting on Python code
   - Executes the test suite

2. **Recipe Validation**:
   - Validates recipe templates and instances
   - Ensures they follow the correct schema and contain required fields

3. **Secret Validation**:
   - Extracts all GitHub secrets referenced in workflows and recipe templates
   - Checks for the existence of these secrets in the GitHub environment
   - Warns if any required secrets are missing

4. **Deployment Preview**:
   - Identifies which recipes would be deployed when the PR is merged
   - Generates a report and adds it as a comment to the PR
   - This helps reviewers understand the impact of changes before approving

### Recipe Deployment (`deploy.yml`)

This workflow handles the actual deployment of recipes to DataHub environments. It runs in two scenarios:

1. **Automatic** - When changes are pushed to the `main` branch (typically through PR merges)
2. **Manual** - When triggered manually through the GitHub UI with specific parameters

Deployment options include:

- **Environment**: `dev`, `staging`, or `prod`
- **Deployment Type**:
  - `all`: Deploy all recipes for the selected environment
  - `selected`: Deploy specific recipes (comma-separated list)
  - `recent`: Deploy recipes modified in the last N hours
- **Create Secrets**: Optionally create secrets in DataHub during deployment

All deployments generate detailed reports that are stored as workflow artifacts.

## Best Practices

1. **Always use Pull Requests**:
   - Never push directly to `main`
   - All changes should go through PR validation

2. **Review Deployment Previews**:
   - Check the deployment preview comment on PRs
   - Understand which recipes will be affected before approving

3. **Phased Deployments**:
   - Use the manual workflow trigger for controlled deployments
   - Deploy to `dev` first, then `staging`, and finally `prod`

4. **Manage Secrets Carefully**:
   - Keep all referenced secrets up to date in GitHub
   - Use separate secrets for different environments when appropriate

## Troubleshooting

If deployments fail:

1. Check the workflow logs for detailed error messages
2. Verify that all required secrets are properly configured
3. Ensure recipes are valid and follow the correct schema
4. Test the connection to DataHub before deploying

For more information, see the DataHub documentation and project README. 