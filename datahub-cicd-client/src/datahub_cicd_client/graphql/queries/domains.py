"""
GraphQL queries for domain operations.
"""


# Query to list domains with comprehensive information
LIST_DOMAINS_QUERY = """
query getSearchResultsForMultiple($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    start
    count
    total
    searchResults {
      entity {
        ... on Domain {
          urn
          id
          properties {
            name
            description
          }
          parentDomains {
            domains {
              urn
            }
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
                  }
                }
                ... on CorpGroup {
                  urn
                  name
                  properties {
                    displayName
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
          institutionalMemory {
            elements {
              url
              label
              actor {
                ... on CorpUser {
                  urn
                  username
                  properties {
                    displayName
                    fullName
                  }
                }
                ... on CorpGroup {
                  urn
                  name
                  properties {
                    displayName
                  }
                }
              }
              created {
                time
                actor
              }
            }
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
          displayProperties {
            colorHex
            icon {
              iconLibrary
              name
              style
            }
          }
          entities(input: { start: 0, count: 1, query: "*" }) {
            total
          }
        }
      }
    }
  }
}
"""

# Query to get a single domain by URN
GET_DOMAIN_QUERY = """
query getDomain($urn: String!) {
  domain(urn: $urn) {
    urn
    id
    properties {
      name
      description
    }
    parentDomains {
      domains {
        urn
      }
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
            }
          }
          ... on CorpGroup {
            urn
            name
            properties {
              displayName
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
    institutionalMemory {
      elements {
        url
        label
        actor {
          ... on CorpUser {
            urn
            username
            properties {
              displayName
              fullName
            }
          }
          ... on CorpGroup {
            urn
            name
            properties {
              displayName
            }
          }
        }
        created {
          time
          actor
        }
      }
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
    entities(input: { start: 0, count: 1, query: "*" }) {
      total
    }
    displayProperties {
      colorHex
      icon {
        iconLibrary
        name
        style
      }
    }
  }
}
"""

# Query to find entities within a domain
FIND_ENTITIES_WITH_DOMAIN_QUERY = """
query searchAcrossEntities($input: SearchAcrossEntitiesInput!) {
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
          domain {
            urn
            properties {
              name
              description
            }
          }
        }
        ... on Dashboard {
          properties {
            title
            description
          }
          dashboardTool
          domain {
            urn
            properties {
              name
              description
            }
          }
        }
        ... on Chart {
          properties {
            title
            description
          }
          chartTool
          domain {
            urn
            properties {
              name
              description
            }
          }
        }
        ... on DataProduct {
          properties {
            name
            description
          }
          domain {
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
"""

# Query to count domains
COUNT_DOMAINS_QUERY = """
query getDomainsCount($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    total
  }
}
"""
