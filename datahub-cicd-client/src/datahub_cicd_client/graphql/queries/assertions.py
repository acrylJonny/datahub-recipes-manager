"""
GraphQL queries for assertion operations.
"""

# Search assertions
SEARCH_ASSERTIONS_QUERY = """
query searchAssertions($input: SearchInput!) {
    searchAcrossEntities(input: $input) {
        start
        count
        total
        searchResults {
            entity {
                urn
                type
                ... on Assertion {
                    properties {
                        type
                        source {
                            type
                            created {
                                time
                                actor
                            }
                        }
                        customProperties {
                            key
                            value
                        }
                    }
                    info {
                        type
                        description
                        lastUpdated {
                            time
                            actor
                        }
                        datasetAssertion {
                            dataset
                            scope
                            fields
                            aggregation
                            operator
                            parameters {
                                value {
                                    type
                                    value
                                }
                                minValue {
                                    type
                                    value
                                }
                                maxValue {
                                    type
                                    value
                                }
                            }
                            nativeType
                            nativeParameters {
                                key
                                value
                            }
                            logic
                        }
                    }
                    status {
                        type
                    }
                    runEvents(limit: 10) {
                        total
                        failed
                        succeeded
                        runEvents {
                            timestampMillis
                            status
                            result {
                                type
                                rowCount
                                missingCount
                                unexpectedCount
                                actualAggValue
                                externalUrl
                                nativeResults {
                                    key
                                    value
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
"""

# Get specific assertion
GET_ASSERTION_QUERY = """
query getAssertion($urn: String!) {
    assertion(urn: $urn) {
        urn
        type
        properties {
            type
            source {
                type
                created {
                    time
                    actor
                }
            }
            customProperties {
                key
                value
            }
        }
        info {
            type
            description
            lastUpdated {
                time
                actor
            }
            datasetAssertion {
                dataset
                scope
                fields
                aggregation
                operator
                parameters {
                    value {
                        type
                        value
                    }
                    minValue {
                        type
                        value
                    }
                    maxValue {
                        type
                        value
                    }
                }
                nativeType
                nativeParameters {
                    key
                    value
                }
                logic
            }
        }
        status {
            type
        }
        runEvents(limit: 10) {
            total
            failed
            succeeded
            runEvents {
                timestampMillis
                status
                result {
                    type
                    rowCount
                    missingCount
                    unexpectedCount
                    actualAggValue
                    externalUrl
                    nativeResults {
                        key
                        value
                    }
                }
            }
        }
    }
}
"""

# Count assertions
COUNT_ASSERTIONS_QUERY = """
query countAssertions($input: SearchInput!) {
    searchAcrossEntities(input: $input) {
        total
    }
}
"""
