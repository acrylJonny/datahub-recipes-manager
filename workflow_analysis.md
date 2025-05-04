# GitHub Workflow Descriptions
# Generated on 2025-05-04 19:36:39

## Patch DataHub Ingestion Source (patch-ingestion-source.yml)
Description: Workflow that runs manually. Checks out repository code. With 1 job and 4 steps.
Actions: checkout, setup-python
Complexity: 1 jobs, 4 steps

## Run DataHub Recipe Now (run-recipe.yml)
Description: Workflow that runs manually. Checks out repository code. With 1 job and 4 steps.
Actions: checkout, setup-python
Complexity: 1 jobs, 4 steps

## Test DataHub Integration (test-integration.yml)
Description: Workflow that runs manually. Checks out repository code. With 1 job and 7 steps.
Actions: checkout, setup-python
Complexity: 1 jobs, 7 steps

## Manage DataHub Ingestion Sources (manage-ingestion.yml)
Description: Workflow that runs manually. Checks out repository code. Targets ${{ github.event.inputs.environment }} environments. With 1 job and 9 steps.
Environments: ${{ github.event.inputs.environment }}
Actions: checkout, setup-python
Complexity: 1 jobs, 9 steps

## Patch DataHub Recipe (patch-recipe.yml)
Description: Workflow that runs manually. Checks out repository code. Targets ${{ github.event.inputs.environment }} environments. With 1 job and 4 steps.
Environments: ${{ github.event.inputs.environment }}
Actions: checkout, setup-python
Complexity: 1 jobs, 4 steps

## Deploy Recipes (deploy.yml)
Description: Workflow that runs manually. Checks out repository code. Targets ${{ github.event.inputs.environment || 'dev' }} environments. With 1 job and 7 steps.
Environments: ${{ github.event.inputs.environment || 'dev' }}
Actions: checkout, setup-python, upload-artifact
Complexity: 1 jobs, 7 steps

## Run DataHub Ingestion Source Now (run-now.yml)
Description: Workflow that runs manually. Checks out repository code. With 1 job and 4 steps.
Actions: checkout, setup-python
Complexity: 1 jobs, 4 steps

## Manage DataHub Secrets (manage-secrets.yml)
Description: Workflow that runs manually. Checks out repository code. Targets ${{ github.event.inputs.environment }} environments. With 1 job and 8 steps.
Environments: ${{ github.event.inputs.environment }}
Actions: checkout, setup-python
Complexity: 1 jobs, 8 steps

## Manage DataHub Policies (manage-policy.yml)
Description: Workflow that runs manually. Checks out repository code. Targets ${{ github.event.inputs.environment }} environments. With 1 job and 14 steps.
Environments: ${{ github.event.inputs.environment }}
Actions: checkout, download-artifact, setup-python, upload-artifact
Complexity: 1 jobs, 14 steps

## Manage Environment Variables (manage-env-vars.yml)
Description: Workflow that runs manually. Checks out repository code. Targets ${{ github.event.inputs.environment }} environments. With 1 job and 13 steps.
Environments: ${{ github.event.inputs.environment }}
Actions: checkout, download-artifact, setup-python, upload-artifact
Complexity: 1 jobs, 13 steps

## Validate Recipes (ci.yml)
Description: Workflow that runs manually. Checks out repository code. With 3 jobs and 15 steps.
Actions: checkout, setup-python
Complexity: 3 jobs, 15 steps
