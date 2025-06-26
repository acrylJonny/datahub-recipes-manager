"""
GraphQL fragments for ingestion-related entities.
"""

# Basic ingestion source fragment
INGESTION_SOURCE_FRAGMENT = """
fragment IngestionSourceFragment on DataHubIngestionSource {
    urn
    type
    name
    schedule {
        interval
        timezone
    }
    config {
        recipe
        version
        executorId
        debugMode
        extraArgs {
            key
            value
        }
    }
    platform {
        urn
        name
        type
    }
}
"""

# Ingestion source with execution info
INGESTION_SOURCE_WITH_EXECUTIONS_FRAGMENT = """
fragment IngestionSourceWithExecutionsFragment on DataHubIngestionSource {
    urn
    type
    name
    schedule {
        interval
        timezone
    }
    config {
        recipe
        version
        executorId
        debugMode
        extraArgs {
            key
            value
        }
    }
    platform {
        urn
        name
        type
    }
    executions(start: 0, count: 10) {
        start
        count
        total
        executionRequests {
            urn
            id
            input {
                requestedAt
                actorUrn
            }
            result {
                status
                startTimeMs
                durationMs
                structuredReport {
                    type
                    serializedValue
                }
            }
        }
    }
}
"""

# Simple ingestion source fragment for lists
INGESTION_SOURCE_SIMPLE_FRAGMENT = """
fragment IngestionSourceSimpleFragment on DataHubIngestionSource {
    urn
    type
    name
    platform {
        urn
        name
        type
    }
}
"""

# Ingestion execution fragment
INGESTION_EXECUTION_FRAGMENT = """
fragment IngestionExecutionFragment on IngestionExecutionRequest {
    urn
    id
    input {
        requestedAt
        actorUrn
    }
    result {
        status
        startTimeMs
        durationMs
        structuredReport {
            type
            serializedValue
        }
    }
}
"""

# Execution result fragment
EXECUTION_RESULT_FRAGMENT = """
fragment ExecutionResultFragment on ExecutionRequestResult {
    status
    startTimeMs
    durationMs
    structuredReport {
        type
        serializedValue
    }
}
"""
