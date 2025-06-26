"""
GraphQL queries for data contract operations.
"""

# List data contracts with comprehensive information
LIST_DATA_CONTRACTS_QUERY = """
query GetDataContracts($input: SearchAcrossEntitiesInput!) {
    searchAcrossEntities(input: $input) {
        start
        count
        total
        searchResults {
            entity {
                urn
                ... on DataContract {
                    properties {
                        entityUrn
                        freshness {
                            assertion {
                                urn
                                info {
                                    description
                                }
                            }
                        }
                        schema {
                            assertion {
                                urn
                                info {
                                    description
                                }
                            }
                        }
                        dataQuality {
                            assertion {
                                urn
                                info {
                                    description
                                }
                            }
                        }
                    }
                    status {
                        state
                    }
                    structuredProperties {
                        properties {
                            structuredProperty {
                                urn
                            }
                            values {
                                ... on StringValue {
                                    stringValue
                                }
                                ... on NumberValue {
                                    numberValue
                                }
                            }
                            valueEntities {
                                urn
                            }
                        }
                    }
                    result(refresh: false) {
                        type
                    }
                }
            }
        }
    }
}
"""

# Get a specific data contract with full details
GET_DATA_CONTRACT_QUERY = """
query getDataContract($urn: String!) {
    dataContract(urn: $urn) {
        urn
        type
        properties {
            entityUrn
            freshness {
                assertion {
                    urn
                    info {
                        description
                        type
                    }
                }
            }
            schema {
                assertion {
                    urn
                    info {
                        description
                        type
                    }
                }
            }
            dataQuality {
                assertion {
                    urn
                    info {
                        description
                        type
                    }
                }
            }
        }
        status {
            state
        }
        structuredProperties {
            properties {
                structuredProperty {
                    urn
                }
                values {
                    ... on StringValue {
                        stringValue
                    }
                    ... on NumberValue {
                        numberValue
                    }
                }
                valueEntities {
                    urn
                }
            }
        }
        result(refresh: false) {
            type
        }
    }
}
"""

# Count data contracts
COUNT_DATA_CONTRACTS_QUERY = """
query countDataContracts($input: SearchAcrossEntitiesInput!) {
    searchAcrossEntities(input: $input) {
        total
    }
}
"""

# Query to get datasets by URNs (for dataset info enrichment)
GET_DATASETS_BY_URNS_QUERY = """
query getDatasetsByUrns($urns: [String!]!) {
    searchAcrossEntities(
        input: {
            types: ["DATASET"]
            query: "*"
            orFilters: [
                {
                    and: [
                        {
                            field: "urn"
                            values: $urns
                        }
                    ]
                }
            ]
            start: 0
            count: 1000
        }
    ) {
        start
        count
        total
        searchResults {
            entity {
                urn
                type
                ... on Dataset {
                    name
                    properties {
                        name
                        description
                        qualifiedName
                        created {
                            time
                        }
                        lastModified {
                            time
                        }
                    }
                    platform {
                        urn
                        name
                        properties {
                            displayName
                        }
                    }
                    platformInstance {
                        urn
                        properties {
                            name
                        }
                    }
                    browsePaths
                    ownership {
                        owners {
                            owner {
                                ... on CorpUser {
                                    urn
                                    username
                                }
                                ... on CorpGroup {
                                    urn
                                    name
                                }
                            }
                            type
                        }
                    }
                    tags {
                        tags {
                            tag {
                                urn
                                properties {
                                    name
                                }
                            }
                        }
                    }
                    glossaryTerms {
                        terms {
                            term {
                                urn
                                properties {
                                    name
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
