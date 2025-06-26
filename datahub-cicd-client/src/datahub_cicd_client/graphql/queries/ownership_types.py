"""
GraphQL queries for ownership type management operations.
"""

# Query to list ownership types
LIST_OWNERSHIP_TYPES_QUERY = """
query ListOwnershipTypes($input: ListOwnershipTypesInput!) {
  listOwnershipTypes(input: $input) {
    start
    count
    total
    ownershipTypes {
      urn
      info {
        name
        description
      }
    }
  }
}
"""

# Query to get a single ownership type by URN
GET_OWNERSHIP_TYPE_QUERY = """
query GetOwnershipType($urn: String!) {
  ownershipType(urn: $urn) {
    urn
    info {
      name
      description
    }
  }
}
"""

# Alternative query for listing ownership types using search
LIST_OWNERSHIP_TYPES_SEARCH_QUERY = """
query SearchOwnershipTypes($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    start
    count
    total
    searchResults {
      entity {
        ... on OwnershipType {
          urn
          info {
            name
            description
          }
        }
      }
    }
  }
}
"""
