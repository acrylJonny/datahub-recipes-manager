"""
GraphQL queries for structured properties operations.
"""

# Query to list structured properties
LIST_STRUCTURED_PROPERTIES_QUERY = """
query getSearchResultsForMultiple($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    start
    count
    total
    searchResults {
      entity {
        urn
        type
        ... on StructuredPropertyEntity {
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
          settings {
            isHidden
            showInSearchFilters
            showAsAssetBadge
            showInAssetSummary
            showInColumnsTable
          }
        }
      }
    }
  }
}
"""

# Query to get structured properties URNs for filtering
GET_STRUCTURED_PROPERTIES_URNS_QUERY = """
query GetEntitiesWithBrowsePathsForSearch($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    start
    count
    total
    searchResults {
      entity {
        urn
      }
    }
  }
}
"""

# Query to test if structured properties are supported
TEST_STRUCTURED_PROPERTY_SUPPORT_QUERY = """
query testStructuredPropertySupport($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    total
  }
}
"""

# Query to get a specific structured property
GET_STRUCTURED_PROPERTY_QUERY = """
query getStructuredProperty($urn: String!) {
  structuredProperty(urn: $urn) {
    urn
    type
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
    settings {
      isHidden
      showInSearchFilters
      showAsAssetBadge
      showInAssetSummary
      showInColumnsTable
    }
  }
}
"""
