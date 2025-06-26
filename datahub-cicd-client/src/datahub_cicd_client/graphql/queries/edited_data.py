"""
GraphQL queries for editable entity operations.
"""

# Search editable entities
SEARCH_EDITABLE_ENTITIES_QUERY = """
query searchEditableEntities($input: SearchInput!) {
    searchAcrossEntities(input: $input) {
        start
        count
        total
        searchResults {
            entity {
                urn
                type
                ... on Dataset {
                    properties {
                        name
                        description
                        qualifiedName
                        created {
                            time
                            actor
                        }
                        lastModified {
                            time
                            actor
                        }
                        customProperties {
                            key
                            value
                        }
                    }
                    editableProperties {
                        description
                        created {
                            time
                            actor
                        }
                        lastModified {
                            time
                            actor
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
                    globalTags {
                        tags {
                            tag {
                                urn
                                properties {
                                    name
                                    description
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
                                    description
                                }
                            }
                        }
                    }
                    status {
                        removed
                    }
                }
                ... on Chart {
                    properties {
                        title
                        description
                        created {
                            time
                            actor
                        }
                        lastModified {
                            time
                            actor
                        }
                        customProperties {
                            key
                            value
                        }
                    }
                    editableProperties {
                        description
                        created {
                            time
                            actor
                        }
                        lastModified {
                            time
                            actor
                        }
                    }
                }
                ... on Dashboard {
                    properties {
                        title
                        description
                        created {
                            time
                            actor
                        }
                        lastModified {
                            time
                            actor
                        }
                        customProperties {
                            key
                            value
                        }
                    }
                    editableProperties {
                        description
                        created {
                            time
                            actor
                        }
                        lastModified {
                            time
                            actor
                        }
                    }
                }
            }
        }
    }
}
"""

# Get specific editable entity
GET_EDITABLE_ENTITY_QUERY = """
query getEditableEntity($urn: String!) {
    entity(urn: $urn) {
        urn
        type
        ... on Dataset {
            properties {
                name
                description
                qualifiedName
                created {
                    time
                    actor
                }
                lastModified {
                    time
                    actor
                }
                customProperties {
                    key
                    value
                }
            }
            editableProperties {
                description
                created {
                    time
                    actor
                }
                lastModified {
                    time
                    actor
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
            globalTags {
                tags {
                    tag {
                        urn
                        properties {
                            name
                            description
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
                            description
                        }
                    }
                }
            }
            status {
                removed
            }
        }
        ... on Chart {
            properties {
                title
                description
                created {
                    time
                    actor
                }
                lastModified {
                    time
                    actor
                }
                customProperties {
                    key
                    value
                }
            }
            editableProperties {
                description
                created {
                    time
                    actor
                }
                lastModified {
                    time
                    actor
                }
            }
        }
        ... on Dashboard {
            properties {
                title
                description
                created {
                    time
                    actor
                }
                lastModified {
                    time
                    actor
                }
                customProperties {
                    key
                    value
                }
            }
            editableProperties {
                description
                created {
                    time
                    actor
                }
                lastModified {
                    time
                    actor
                }
            }
        }
    }
}
"""

# Count editable entities
COUNT_EDITABLE_ENTITIES_QUERY = """
query countEditableEntities($input: SearchInput!) {
    searchAcrossEntities(input: $input) {
        total
    }
}
"""
