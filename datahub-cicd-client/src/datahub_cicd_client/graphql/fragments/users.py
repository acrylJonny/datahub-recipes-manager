"""
GraphQL fragments for user-related fields.
"""

# Basic user fragment with essential info
USER_FRAGMENT = """
fragment UserFragment on CorpUser {
  urn
  username
  properties {
    displayName
    firstName
    lastName
    fullName
    email
    title
    departmentName
  }
  info {
    displayName
    firstName
    lastName
    fullName
    email
  }
  editableProperties {
    displayName
    title
    pictureLink
    teams
    skills
  }
}
"""

# Simple user fragment for basic display
USER_SIMPLE_FRAGMENT = """
fragment UserSimpleFragment on CorpUser {
  urn
  username
  properties {
    displayName
    fullName
    email
  }
  info {
    displayName
    fullName
    email
  }
}
"""

# User fragment with ownership information
USER_WITH_OWNERSHIP_FRAGMENT = """
fragment UserWithOwnershipFragment on CorpUser {
  urn
  username
  properties {
    displayName
    firstName
    lastName
    fullName
    email
    title
    departmentName
  }
  info {
    displayName
    firstName
    lastName
    fullName
    email
  }
  editableProperties {
    displayName
    title
    pictureLink
    teams
    skills
  }
  ownership {
    owners {
      owner {
        ... on CorpUser {
          urn
          username
          properties {
            displayName
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
        }
      }
    }
  }
}
"""
