"""
GraphQL queries for ingestion operations.
"""

from datahub_cicd_client.graphql.fragments.ingestion import (
    INGESTION_EXECUTION_FRAGMENT,
    INGESTION_SOURCE_FRAGMENT,
    INGESTION_SOURCE_SIMPLE_FRAGMENT,
    INGESTION_SOURCE_WITH_EXECUTIONS_FRAGMENT,
)

# List all ingestion sources
LIST_INGESTION_SOURCES_QUERY = f"""
{INGESTION_SOURCE_WITH_EXECUTIONS_FRAGMENT}

query listIngestionSources($input: ListIngestionSourcesInput!) {{
    listIngestionSources(input: $input) {{
        start
        count
        total
        ingestionSources {{
            ...IngestionSourceWithExecutionsFragment
        }}
    }}
}}
"""

# List ingestion sources (simple version without executions)
LIST_INGESTION_SOURCES_SIMPLE_QUERY = f"""
{INGESTION_SOURCE_SIMPLE_FRAGMENT}

query listIngestionSourcesSimple($input: ListIngestionSourcesInput!) {{
    listIngestionSources(input: $input) {{
        start
        count
        total
        ingestionSources {{
            ...IngestionSourceSimpleFragment
        }}
    }}
}}
"""

# Get single ingestion source
GET_INGESTION_SOURCE_QUERY = f"""
{INGESTION_SOURCE_FRAGMENT}

query ingestionSource($urn: String!) {{
    ingestionSource(urn: $urn) {{
        ...IngestionSourceFragment
    }}
}}
"""

# Get ingestion source with execution history
GET_INGESTION_SOURCE_WITH_EXECUTIONS_QUERY = f"""
{INGESTION_SOURCE_WITH_EXECUTIONS_FRAGMENT}

query ingestionSourceWithExecutions($urn: String!) {{
    ingestionSource(urn: $urn) {{
        ...IngestionSourceWithExecutionsFragment
    }}
}}
"""

# Count ingestion sources
COUNT_INGESTION_SOURCES_QUERY = """
query countIngestionSources($input: ListIngestionSourcesInput!) {
    listIngestionSources(input: $input) {
        total
    }
}
"""

# Get ingestion executions for a source
GET_INGESTION_EXECUTIONS_QUERY = f"""
{INGESTION_EXECUTION_FRAGMENT}

query getIngestionExecutions($urn: String!, $start: Int, $count: Int) {{
    ingestionSource(urn: $urn) {{
        executions(start: $start, count: $count) {{
            start
            count
            total
            executionRequests {{
                ...IngestionExecutionFragment
            }}
        }}
    }}
}}
"""

# Get execution request by ID
GET_EXECUTION_REQUEST_QUERY = f"""
{INGESTION_EXECUTION_FRAGMENT}

query getExecutionRequest($urn: String!) {{
    executionRequest(urn: $urn) {{
        ...IngestionExecutionFragment
    }}
}}
"""

# Find ingestion sources by platform
FIND_INGESTION_SOURCES_BY_PLATFORM_QUERY = f"""
{INGESTION_SOURCE_SIMPLE_FRAGMENT}

query findIngestionSourcesByPlatform($input: ListIngestionSourcesInput!) {{
    listIngestionSources(input: $input) {{
        start
        count
        total
        ingestionSources {{
            ...IngestionSourceSimpleFragment
        }}
    }}
}}
"""

# Find ingestion sources by type
FIND_INGESTION_SOURCES_BY_TYPE_QUERY = f"""
{INGESTION_SOURCE_SIMPLE_FRAGMENT}

query findIngestionSourcesByType($input: ListIngestionSourcesInput!) {{
    listIngestionSources(input: $input) {{
        start
        count
        total
        ingestionSources {{
            ...IngestionSourceSimpleFragment
        }}
    }}
}}
"""

# Get ingestion source statistics
GET_INGESTION_SOURCE_STATS_QUERY = """
query getIngestionSourceStats($urn: String!) {
    ingestionSource(urn: $urn) {
        urn
        name
        type
        executions(start: 0, count: 100) {
            total
            executionRequests {
                result {
                    status
                    startTimeMs
                    durationMs
                }
            }
        }
    }
}
"""
