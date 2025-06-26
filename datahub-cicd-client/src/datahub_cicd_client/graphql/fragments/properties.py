"""
GraphQL fragments for properties-related fields.
"""

# Structured properties fragment
STRUCTURED_PROPERTIES_FRAGMENT = """
fragment StructuredPropertiesFragment on StructuredProperties {
    properties {
        propertyUrn
        values {
            value
            entity {
                urn
                type
            }
        }
    }
}
"""

# Structured property definition fragment
STRUCTURED_PROPERTY_DEFINITION_FRAGMENT = """
fragment StructuredPropertyDefinitionFragment on StructuredPropertyDefinition {
    urn
    type
    qualifiedName
    displayName
    description
    valueType
    typeQualifier {
        allowedTypes
    }
    cardinality
    allowedValues {
        value
        description
    }
    entityTypes
    immutable
    version
}
"""

# Global tags fragment
GLOBAL_TAGS_FRAGMENT = """
fragment GlobalTagsFragment on GlobalTags {
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
        associatedUrn
    }
}
"""

# Glossary terms fragment
GLOSSARY_TERMS_FRAGMENT = """
fragment GlossaryTermsFragment on GlossaryTerms {
    terms {
        term {
            urn
            type
            name
            properties {
                name
                description
                definition
                termSource
                sourceRef
                sourceUrl
                rawSchema
                customProperties
            }
            hierarchicalName
        }
        associatedUrn
    }
}
"""

# Editable properties fragment
EDITABLE_PROPERTIES_FRAGMENT = """
fragment EditablePropertiesFragment on EditableDatasetProperties {
    description
    created {
        time
        actor
    }
    lastModified {
        time
        actor
    }
}
"""

# Dataset properties fragment
DATASET_PROPERTIES_FRAGMENT = """
fragment DatasetPropertiesFragment on DatasetProperties {
    name
    description
    qualifiedName
    uri
    platform {
        urn
        name
        properties {
            displayName
            type
            datasetNameDelimiter
            logoUrl
        }
    }
    platformInstance {
        urn
        instanceId
        platform {
            urn
            name
        }
    }
    origin
    tags
    created {
        time
        actor
    }
    lastModified {
        time
        actor
    }
    externalUrl
    customProperties {
        key
        value
    }
}
"""

# Institution fragment for dataset properties
INSTITUTION_FRAGMENT = """
fragment InstitutionFragment on DataPlatformInstance {
    urn
    instanceId
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
    }
}
"""
