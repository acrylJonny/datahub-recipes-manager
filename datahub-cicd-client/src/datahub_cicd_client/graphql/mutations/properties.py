"""
GraphQL mutations for structured properties operations.
"""

# Mutation to create a structured property
CREATE_STRUCTURED_PROPERTY_MUTATION = """
mutation createStructuredProperty($input: CreateStructuredPropertyInput!) {
  createStructuredProperty(input: $input) {
    urn
  }
}
"""

# Mutation to update a structured property
UPDATE_STRUCTURED_PROPERTY_MUTATION = """
mutation updateStructuredProperty($input: UpdateStructuredPropertyInput!) {
  updateStructuredProperty(input: $input) {
    urn
  }
}
"""

# Mutation to delete a structured property
DELETE_STRUCTURED_PROPERTY_MUTATION = """
mutation deleteStructuredProperty($urn: String!) {
  deleteStructuredProperty(urn: $urn)
}
"""

# Mutation to upsert structured properties on an entity
UPSERT_STRUCTURED_PROPERTIES_MUTATION = """
mutation upsertStructuredProperties($input: UpsertStructuredPropertiesInput!) {
  upsertStructuredProperties(input: $input) {
    properties {
      structuredProperty {
        urn
        definition {
          displayName
          qualifiedName
          description
          cardinality
          immutable
          valueType {
            urn
            type
            info {
              type
              displayName
            }
          }
          entityTypes {
            urn
            type
            info {
              type
            }
          }
          filterStatus
          typeQualifier {
            allowedTypes {
              urn
              type
              info {
                type
                displayName
              }
            }
          }
          allowedValues {
            value {
              ... on StringValue {
                stringValue
              }
              ... on NumberValue {
                numberValue
              }
            }
            description
          }
        }
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
        type
      }
      associatedUrn
    }
  }
}
"""

# Mutation to remove structured properties from an entity
REMOVE_STRUCTURED_PROPERTIES_MUTATION = """
mutation removeStructuredProperties($input: RemoveStructuredPropertiesInput!) {
  removeStructuredProperties(input: $input)
}
"""
