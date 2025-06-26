"""
GraphQL queries for recipe/ingestion source operations.
"""

# Get ingestion source
GET_INGESTION_SOURCE_QUERY = """
query getIngestionSource($urn: String!) {
    ingestionSource(urn: $urn) {
        urn
        type
        name
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
            properties {
                displayName
                logoUrl
            }
        }
        schedule {
            interval
            timezone
        }
        executions(start: 0, count: 1) {
            start
            count
            total
            executionRequests {
                urn
                id
                input {
                    task
                    arguments {
                        key
                        value
                    }
                }
                result {
                    status
                    report
                    startTimeMs
                    durationMs
                }
            }
        }
    }
}
"""

# List ingestion sources
LIST_INGESTION_SOURCES_QUERY = """
query listIngestionSources($input: ListIngestionSourcesInput!) {
    listIngestionSources(input: $input) {
        start
        count
        total
        ingestionSources {
            urn
            type
            name
            config {
                recipe
                version
                executorId
                debugMode
            }
            platform {
                urn
                properties {
                    displayName
                    logoUrl
                }
            }
            schedule {
                interval
                timezone
            }
        }
    }
}
"""

# Get recipe executions
GET_RECIPE_EXECUTIONS_QUERY = """
query getRecipeExecutions($urn: String!, $start: Int!, $count: Int!) {
    ingestionSource(urn: $urn) {
        urn
        executions(start: $start, count: $count) {
            start
            count
            total
            executionRequests {
                urn
                id
                input {
                    task
                    arguments {
                        key
                        value
                    }
                }
                result {
                    status
                    report
                    startTimeMs
                    durationMs
                }
                created {
                    time
                    actor
                }
            }
        }
    }
}
"""

# Get execution request details
GET_EXECUTION_REQUEST_QUERY = """
query getExecutionRequest($urn: String!) {
    executionRequest(urn: $urn) {
        urn
        id
        input {
            task
            arguments {
                key
                value
            }
        }
        result {
            status
            report
            startTimeMs
            durationMs
            structuredReport {
                type
                serializedValue
            }
        }
        created {
            time
            actor
        }
    }
}
"""

# Count ingestion sources
COUNT_INGESTION_SOURCES_QUERY = """
query countIngestionSources($input: ListIngestionSourcesInput!) {
    listIngestionSources(input: $input) {
        total
    }
}
"""
