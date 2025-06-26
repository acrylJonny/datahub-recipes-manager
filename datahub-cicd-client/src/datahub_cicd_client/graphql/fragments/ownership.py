"""
GraphQL fragments for ownership-related fields.
"""

# Basic ownership fragment
OWNERSHIP_FRAGMENT = """
fragment OwnershipFragment on Ownership {
    owners {
        owner {
            ... on CorpUser {
                urn
                username
                info {
                    displayName
                    firstName
                    lastName
                    fullName
                    email
                }
            }
            ... on CorpGroup {
                urn
                name
                info {
                    displayName
                    description
                }
            }
        }
        type
        associatedUrn
    }
    lastModified {
        time
        actor
    }
}
"""

# Simplified ownership fragment for basic info
OWNERSHIP_SIMPLE_FRAGMENT = """
fragment OwnershipSimpleFragment on Ownership {
    owners {
        owner {
            urn
            ... on CorpUser {
                username
                info {
                    displayName
                    fullName
                }
            }
            ... on CorpGroup {
                name
                info {
                    displayName
                }
            }
        }
        type
    }
}
"""

# Owner entity fragment for detailed user/group info
OWNER_ENTITY_FRAGMENT = """
fragment OwnerEntityFragment on Entity {
    urn
    type
    ... on CorpUser {
        username
        properties {
            displayName
            firstName
            lastName
            fullName
            email
            title
            departmentName
            managerUrn
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
    ... on CorpGroup {
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
            displayName
            description
            pictureLink
        }
    }
}
"""

# Ownership type fragment
OWNERSHIP_TYPE_FRAGMENT = """
fragment OwnershipTypeFragment on OwnershipType {
    urn
    name
    info {
        name
        description
    }
}
"""
