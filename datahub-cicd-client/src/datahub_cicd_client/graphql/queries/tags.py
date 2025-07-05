"""
GraphQL queries for tag operations.
"""

# Query to list tags with comprehensive information
LIST_TAGS_QUERY = """
query GetTags($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    start
    count
    total
    searchResults {
      entity {
        urn
        type
        ... on Tag {
          properties {
            name
            description
            colorHex
          }
          ownership {
            owners {
              owner {
                ... on CorpUser {
                  urn
                  username
                  properties { displayName }
                }
                ... on CorpGroup {
                  urn
                  name
                  properties { displayName }
                }
              }
              ownershipType {
                urn
                info { name }
              }
              source {
                type
                url
              }
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

# Query to get a single tag by URN
GET_TAG_QUERY = """
query GetTag($urn: String!) {
  entity(urn: $urn) {
    urn
    type
    ... on Tag {
      properties {
        name
        description
        colorHex
      }
      ownership {
        owners {
          owner {
            ... on CorpUser {
              urn
              username
              properties {
                displayName
                fullName
                email
              }
            }
            ... on CorpGroup {
              urn
              name
              properties {
                displayName
                description
              }
            }
          }
          ownershipType {
            urn
            info {
              name
              description
            }
          }
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

# Query to count tags
COUNT_TAGS_QUERY = """
query GetTagsCount($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    total
  }
}
"""

# Query to find entities with a specific tag
FIND_ENTITIES_WITH_TAG_QUERY = """
query FindEntitiesWithTag($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
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
          }
          platform {
            urn
            name
            properties {
              displayName
              type
              logoUrl
            }
          }
          globalTags {
            tags {
              tag {
                urn
                type
                name
                properties {
                  name
                  description
                  colorHex
                }
              }
            }
          }
        }
        ... on Dashboard {
          properties {
            title
            description
          }
          dashboardTool
        }
        ... on Chart {
          properties {
            title
            description
          }
          chartTool
        }
      }
    }
  }
}
"""
