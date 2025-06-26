"""
GraphQL queries for policy operations.
"""

# List policies
LIST_POLICIES_QUERY = """
query listPolicies($start: Int!, $count: Int!) {
    listPolicies(input: { start: $start, count: $count }) {
        start
        count
        total
        policies {
            urn
            type
            name
            description
            state
            privileges
            actors {
                users
                groups
                resourceOwners
                allUsers
                allGroups
            }
            resources {
                filter {
                    type
                    resources
                    allResources
                }
            }
            editable
            lastUpdatedTimestamp
        }
    }
}
"""

# Get specific policy
GET_POLICY_QUERY = """
query getPolicy($urn: String!) {
    policy(urn: $urn) {
        urn
        type
        name
        description
        state
        privileges
        actors {
            users
            groups
            resourceOwners
            allUsers
            allGroups
        }
        resources {
            filter {
                type
                resources
                allResources
            }
        }
        editable
        lastUpdatedTimestamp
    }
}
"""

# Count policies
COUNT_POLICIES_QUERY = """
query countPolicies {
    listPolicies(input: { start: 0, count: 1 }) {
        total
    }
}
"""

# Search policies (if needed)
SEARCH_POLICIES_QUERY = """
query searchPolicies($input: SearchInput!) {
    searchAcrossEntities(input: $input) {
        start
        count
        total
        searchResults {
            entity {
                urn
                type
                ... on DataHubPolicy {
                    properties {
                        name
                        description
                        type
                        state
                        privileges
                        actors {
                            users
                            groups
                            resourceOwners
                            allUsers
                            allGroups
                        }
                        resources {
                            filter {
                                type
                                resources
                                allResources
                            }
                        }
                        editable
                        lastUpdatedTimestamp
                    }
                }
            }
        }
    }
}
"""
