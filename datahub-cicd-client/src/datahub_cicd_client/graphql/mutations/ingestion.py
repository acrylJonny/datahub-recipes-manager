"""
GraphQL mutations for ingestion operations.
"""

# Create ingestion source
CREATE_INGESTION_SOURCE_MUTATION = """
mutation createIngestionSource($input: CreateIngestionSourceInput!) {
    createIngestionSource(input: $input)
}
"""

# Update ingestion source
UPDATE_INGESTION_SOURCE_MUTATION = """
mutation updateIngestionSource($urn: String!, $input: UpdateIngestionSourceInput!) {
    updateIngestionSource(urn: $urn, input: $input) {
        urn
    }
}
"""

# Delete ingestion source
DELETE_INGESTION_SOURCE_MUTATION = """
mutation deleteIngestionSource($urn: String!) {
    deleteIngestionSource(urn: $urn)
}
"""

# Create ingestion execution request (trigger ingestion)
CREATE_INGESTION_EXECUTION_REQUEST_MUTATION = """
mutation createIngestionExecutionRequest($input: CreateIngestionExecutionRequestInput!) {
    createIngestionExecutionRequest(input: $input)
}
"""

# Execute ingestion source (legacy fallback)
EXECUTE_INGESTION_SOURCE_MUTATION = """
mutation executeIngestionSource($input: ExecuteIngestionSourceInput!) {
    executeIngestionSource(input: $input) {
        executionId
    }
}
"""

# Cancel ingestion execution
CANCEL_INGESTION_EXECUTION_MUTATION = """
mutation cancelIngestionExecution($input: CancelIngestionExecutionInput!) {
    cancelIngestionExecution(input: $input)
}
"""

# Update ingestion source schedule
UPDATE_INGESTION_SOURCE_SCHEDULE_MUTATION = """
mutation updateIngestionSourceSchedule($urn: String!, $input: UpdateIngestionSourceScheduleInput!) {
    updateIngestionSourceSchedule(urn: $urn, input: $input) {
        urn
    }
}
"""

# Update ingestion source config
UPDATE_INGESTION_SOURCE_CONFIG_MUTATION = """
mutation updateIngestionSourceConfig($urn: String!, $input: UpdateIngestionSourceConfigInput!) {
    updateIngestionSourceConfig(urn: $urn, input: $input) {
        urn
    }
}
"""

# Patch ingestion source (partial update)
PATCH_INGESTION_SOURCE_MUTATION = """
mutation updateIngestionSource($urn: String!, $input: UpdateIngestionSourceInput!) {
    updateIngestionSource(urn: $urn, input: $input)
}
"""
