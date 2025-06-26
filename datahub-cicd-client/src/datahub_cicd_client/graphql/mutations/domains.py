"""
GraphQL mutations for domain operations.
"""

# Mutation to create a new domain
CREATE_DOMAIN_MUTATION = """
mutation createDomain($input: CreateDomainInput!) {
  createDomain(input: $input)
}
"""

# Mutation to update domain description
UPDATE_DOMAIN_DESCRIPTION_MUTATION = """
mutation updateDescription($input: DescriptionUpdateInput!) {
  updateDescription(input: $input)
}
"""

# Mutation to update domain display properties
UPDATE_DOMAIN_DISPLAY_PROPERTIES_MUTATION = """
mutation updateDisplayProperties($urn: String!, $input: DisplayPropertiesUpdateInput!) {
  updateDisplayProperties(urn: $urn, input: $input)
}
"""

# Mutation to update domain structured properties
UPDATE_DOMAIN_STRUCTURED_PROPERTIES_MUTATION = """
mutation upsertStructuredProperties($input: UpsertStructuredPropertiesInput!) {
  upsertStructuredProperties(input: $input) {
    properties {
      ...structuredPropertiesFields
      __typename
    }
    __typename
  }
}

fragment structuredPropertiesFields on StructuredPropertiesEntry {
  structuredProperty {
    ...structuredPropertyFields
    __typename
  }
  values {
    ... on StringValue {
      stringValue
      __typename
    }
    ... on NumberValue {
      numberValue
      __typename
    }
    __typename
  }
  valueEntities {
    urn
    type
    ...entityDisplayNameFields
    __typename
  }
  associatedUrn
  __typename
}

fragment structuredPropertyFields on StructuredPropertyEntity {
  urn
  type
  exists
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
        __typename
      }
      __typename
    }
    entityTypes {
      urn
      type
      info {
        type
        __typename
      }
      __typename
    }
    cardinality
    filterStatus
    typeQualifier {
      allowedTypes {
        urn
        type
        info {
          type
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
  __typename
}

fragment entityDisplayNameFields on Entity {
  ... on CorpUser {
    properties {
      displayName
      fullName
      __typename
    }
    __typename
  }
  ... on CorpGroup {
    properties {
      displayName
      __typename
    }
    __typename
  }
  ... on Tag {
    properties {
      name
      description
      colorHex
      __typename
    }
    __typename
  }
  ... on GlossaryTerm {
    properties {
      name
      description
      __typename
    }
    hierarchicalName
    __typename
  }
  ... on GlossaryNode {
    properties {
      name
      description
      __typename
    }
    __typename
  }
  __typename
}
"""

# Mutation to add owner to domain
ADD_DOMAIN_OWNER_MUTATION = """
mutation addOwner($input: AddOwnerInput!) {
  addOwner(input: $input)
}
"""

# Mutation to remove owner from domain
REMOVE_DOMAIN_OWNER_MUTATION = """
mutation removeOwner($input: RemoveOwnerInput!) {
  removeOwner(input: $input)
}
"""

# Mutation to delete domain
DELETE_DOMAIN_MUTATION = """
mutation deleteDomain($urn: String!) {
  deleteDomain(urn: $urn)
}
"""
