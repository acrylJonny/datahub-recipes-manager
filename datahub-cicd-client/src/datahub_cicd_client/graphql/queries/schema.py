"""
GraphQL queries for schema operations.
"""

# Get entity schema
GET_ENTITY_SCHEMA_QUERY = """
query getEntitySchema($urn: String!) {
    dataset(urn: $urn) {
        urn
        type
        schemaMetadata {
            name
            version
            hash
            platformSchema {
                ... on TableSchema {
                    schema
                }
                ... on KeyValueSchema {
                    keySchema
                    valueSchema
                }
            }
            fields {
                fieldPath
                type
                nativeDataType
                description
                nullable
                recursive
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
            }
        }
    }
}
"""

# Get multiple entity schemas
GET_ENTITY_SCHEMAS_QUERY = """
query getEntitySchemas($urns: [String!]!) {
    batchGetEntities(urns: $urns) {
        urn
        type
        ... on Dataset {
            schemaMetadata {
                name
                version
                hash
                platformSchema {
                    ... on TableSchema {
                        schema
                    }
                    ... on KeyValueSchema {
                        keySchema
                        valueSchema
                    }
                }
                fields {
                    fieldPath
                    type
                    nativeDataType
                    description
                    nullable
                    recursive
                }
            }
        }
    }
}
"""
