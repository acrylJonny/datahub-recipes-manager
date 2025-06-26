"""
GraphQL queries for metadata test operations.
"""

# Search metadata tests
SEARCH_METADATA_TESTS_QUERY = """
query searchMetadataTests($input: SearchInput!) {
    searchAcrossEntities(input: $input) {
        start
        count
        total
        searchResults {
            entity {
                urn
                type
                ... on Test {
                    properties {
                        name
                        description
                        category
                        definition {
                            type
                            json
                        }
                    }
                    ownership {
                        owners {
                            owner {
                                ... on CorpUser {
                                    username
                                    properties {
                                        displayName
                                    }
                                }
                                ... on CorpGroup {
                                    name
                                    properties {
                                        displayName
                                    }
                                }
                            }
                            type
                        }
                    }
                    status {
                        removed
                    }
                }
            }
        }
    }
}
"""

# Get specific metadata test
GET_METADATA_TEST_QUERY = """
query getMetadataTest($urn: String!) {
    test(urn: $urn) {
        urn
        type
        properties {
            name
            description
            category
            definition {
                type
                json
            }
        }
        ownership {
            owners {
                owner {
                    ... on CorpUser {
                        username
                        properties {
                            displayName
                        }
                    }
                    ... on CorpGroup {
                        name
                        properties {
                            displayName
                        }
                    }
                }
                type
            }
        }
        status {
            removed
        }
    }
}
"""

# Count metadata tests
COUNT_METADATA_TESTS_QUERY = """
query countMetadataTests($input: SearchInput!) {
    searchAcrossEntities(input: $input) {
        total
    }
}
"""
