"""
GraphQL fragments for common metadata fields.
"""

# Basic entity fragment
ENTITY_FRAGMENT = """
fragment EntityFragment on Entity {
    urn
    type
    exists
}
"""

# Entity with last ingested time
ENTITY_WITH_LAST_INGESTED_FRAGMENT = """
fragment EntityWithLastIngestedFragment on Entity {
    urn
    type
    exists
    lastIngested
}
"""

# Browse path fragment
BROWSE_PATH_FRAGMENT = """
fragment BrowsePathFragment on BrowsePath {
    path
}
"""

# Deprecation fragment
DEPRECATION_FRAGMENT = """
fragment DeprecationFragment on Deprecation {
    deprecated
    note
    decommissionTime
    actor
}
"""

# Data platform instance fragment
DATA_PLATFORM_INSTANCE_FRAGMENT = """
fragment DataPlatformInstanceFragment on DataPlatformInstance {
    urn
    platform {
        urn
        name
        properties {
            displayName
            type
            logoUrl
        }
    }
    instanceId
}
"""

# Domain fragment
DOMAIN_FRAGMENT = """
fragment DomainFragment on Domain {
    urn
    type
    name
    properties {
        name
        description
    }
    parentDomains {
        urn
        type
        name
        properties {
            name
            description
        }
    }
}
"""

# Container fragment
CONTAINER_FRAGMENT = """
fragment ContainerFragment on Container {
    urn
    type
    platform {
        urn
        name
        properties {
            displayName
            type
            logoUrl
        }
    }
    properties {
        name
        description
        qualifiedName
        customProperties {
            key
            value
        }
    }
}
"""

# Subtype fragment for different entity types
SUBTYPE_FRAGMENT = """
fragment SubtypeFragment on SubTypes {
    typeNames
}
"""

# Health fragment
HEALTH_FRAGMENT = """
fragment HealthFragment on Health {
    type
    status
    message
    causes
}
"""

# Status fragment
STATUS_FRAGMENT = """
fragment StatusFragment on Status {
    removed
}
"""

# Common entity metadata fragment combining multiple aspects
ENTITY_METADATA_FRAGMENT = """
fragment EntityMetadataFragment on Entity {
    urn
    type
    exists
    lastIngested
    aspects
    
    ... on Dataset {
        platform {
            urn
            name
            properties {
                displayName
                type
                logoUrl
            }
        }
        name
        properties {
            name
            description
            qualifiedName
            customProperties {
                key
                value
            }
        }
        editableProperties {
            description
        }
      }
        }
        status {
            removed
        }
        deprecation {
            deprecated
            note
            decommissionTime
            actor
        }
        browsePaths {
            path
        }
        subTypes {
            typeNames
        }
    }
}
"""

# Custom properties fragment
CUSTOM_PROPERTIES_FRAGMENT = """
fragment CustomPropertiesFragment on CustomProperties {
    customProperties {
        key
        value
    }
}
"""
