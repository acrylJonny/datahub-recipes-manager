"""
GraphQL queries for analytics operations.
"""

# Aggregate across entities
AGGREGATE_ACROSS_ENTITIES_QUERY = """
query aggregateAcrossEntities($input: SearchInput!) {
    aggregateAcrossEntities(input: $input) {
        facets {
            field
            displayName
            aggregations {
                value
                count
                entity {
                    urn
                    type
                }
            }
        }
    }
}
"""

# Search and aggregate
SEARCH_AND_AGGREGATE_QUERY = """
query searchAndAggregate($input: SearchInput!) {
    searchAcrossEntities(input: $input) {
        start
        count
        total
        searchResults {
            entity {
                urn
                type
            }
        }
        facets {
            field
            displayName
            aggregations {
                value
                count
            }
        }
    }
}
"""
