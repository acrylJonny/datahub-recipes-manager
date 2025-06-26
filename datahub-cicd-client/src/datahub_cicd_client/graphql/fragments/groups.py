"""
GraphQL fragments for group-related fields.
"""

# Basic group fragment with essential info
GROUP_FRAGMENT = """
fragment GroupFragment on CorpGroup {
  urn
  name
  properties {
    displayName
    description
    email
  }
  info {
    displayName
    description
    email
  }
  editableProperties {
    description
    pictureLink
  }
}
"""

# Simple group fragment for basic display
GROUP_SIMPLE_FRAGMENT = """
fragment GroupSimpleFragment on CorpGroup {
  urn
  name
  properties {
    displayName
    description
  }
  info {
    displayName
    description
  }
}
"""

# Group fragment with members information (simplified since members field doesn't exist)
GROUP_WITH_MEMBERS_FRAGMENT = """
fragment GroupWithMembersFragment on CorpGroup {
  urn
  name
  properties {
    displayName
    description
    email
  }
  info {
    displayName
    description
    email
  }
  editableProperties {
    description
    pictureLink
  }
}
"""

# Group fragment with ownership information
GROUP_WITH_OWNERSHIP_FRAGMENT = """
fragment GroupWithOwnershipFragment on CorpGroup {
  urn
  name
  properties {
    displayName
    description
    email
  }
  info {
    displayName
    description
    email
  }
  editableProperties {
    description
    pictureLink
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
