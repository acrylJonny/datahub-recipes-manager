"""
GraphQL fragments for data contracts.
"""

DATA_CONTRACT_FRAGMENT = """
fragment DataContractFragment on DataContract {
    urn
    type
    properties {
        name
        description
        externalUrl
        customProperties {
            key
            value
        }
    }
    status {
        removed
    }
}
"""

DATA_CONTRACT_SIMPLE_FRAGMENT = """
fragment DataContractSimpleFragment on DataContract {
    urn
    type
    properties {
        name
        description
    }
}
"""

DATA_CONTRACT_PROPERTIES_FRAGMENT = """
fragment DataContractPropertiesFragment on DataContractProperties {
    name
    description
    externalUrl
    customProperties {
        key
        value
    }
}
"""

DATA_CONTRACT_STATUS_FRAGMENT = """
fragment DataContractStatusFragment on Status {
    removed
}
"""

DATA_CONTRACT_WITH_DATASET_FRAGMENT = """
fragment DataContractWithDatasetFragment on DataContract {
    urn
    type
    properties {
        name
        description
        externalUrl
        customProperties {
            key
            value
        }
    }
    status {
        removed
    }
    entity {
        urn
        type
        ... on Dataset {
            properties {
                name
                description
            }
        }
    }
}
"""
