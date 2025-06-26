# GraphQL mutations for glossary operations

# Create a new glossary node
CREATE_GLOSSARY_NODE_MUTATION = """
mutation CreateGlossaryNode($input: CreateGlossaryEntityInput!) {
  createGlossaryNode(input: $input) {
    urn
  }
}
"""

# Create a new glossary term
CREATE_GLOSSARY_TERM_MUTATION = """
mutation CreateGlossaryTerm($input: CreateGlossaryEntityInput!) {
  createGlossaryTerm(input: $input) {
    urn
  }
}
"""

# Update glossary node properties
UPDATE_GLOSSARY_NODE_MUTATION = """
mutation UpdateGlossaryNode($urn: String!, $input: GlossaryNodeUpdateInput!) {
  updateGlossaryNode(urn: $urn, input: $input) {
    urn
  }
}
"""

# Update glossary term properties
UPDATE_GLOSSARY_TERM_MUTATION = """
mutation UpdateGlossaryTerm($urn: String!, $input: GlossaryTermUpdateInput!) {
  updateGlossaryTerm(urn: $urn, input: $input) {
    urn
  }
}
"""

# Delete a glossary node
DELETE_GLOSSARY_NODE_MUTATION = """
mutation DeleteGlossaryNode($urn: String!) {
  deleteEntity(urn: $urn)
}
"""

# Delete a glossary term
DELETE_GLOSSARY_TERM_MUTATION = """
mutation DeleteGlossaryTerm($urn: String!) {
  deleteEntity(urn: $urn)
}
"""

# Add owner to glossary node
ADD_GLOSSARY_NODE_OWNER_MUTATION = """
mutation AddGlossaryNodeOwner($input: AddOwnerInput!) {
  addOwner(input: $input)
}
"""

# Remove owner from glossary node
REMOVE_GLOSSARY_NODE_OWNER_MUTATION = """
mutation RemoveGlossaryNodeOwner($input: RemoveOwnerInput!) {
  removeOwner(input: $input)
}
"""

# Add owner to glossary term
ADD_GLOSSARY_TERM_OWNER_MUTATION = """
mutation AddGlossaryTermOwner($input: AddOwnerInput!) {
  addOwner(input: $input)
}
"""

# Remove owner from glossary term
REMOVE_GLOSSARY_TERM_OWNER_MUTATION = """
mutation RemoveGlossaryTermOwner($input: RemoveOwnerInput!) {
  removeOwner(input: $input)
}
"""

# Add glossary term to entity
ADD_GLOSSARY_TERM_TO_ENTITY_MUTATION = """
mutation AddGlossaryTermToEntity($input: AddTermInput!) {
  addTerm(input: $input)
}
"""

# Remove glossary term from entity
REMOVE_GLOSSARY_TERM_FROM_ENTITY_MUTATION = """
mutation RemoveGlossaryTermFromEntity($input: RemoveTermInput!) {
  removeTerm(input: $input)
}
"""

# Update glossary node description
UPDATE_GLOSSARY_NODE_DESCRIPTION_MUTATION = """
mutation UpdateGlossaryNodeDescription($urn: String!, $description: String!) {
  updateDescription(input: {
    resourceUrn: $urn,
    description: $description
  })
}
"""

# Update glossary term description
UPDATE_GLOSSARY_TERM_DESCRIPTION_MUTATION = """
mutation UpdateGlossaryTermDescription($urn: String!, $description: String!) {
  updateDescription(input: {
    resourceUrn: $urn,
    description: $description
  })
}
"""

# Upsert structured properties for glossary node
UPSERT_GLOSSARY_NODE_STRUCTURED_PROPERTIES_MUTATION = """
mutation UpsertGlossaryNodeStructuredProperties($input: UpsertStructuredPropertiesInput!) {
  upsertStructuredProperties(input: $input) {
    urn
  }
}
"""

# Upsert structured properties for glossary term
UPSERT_GLOSSARY_TERM_STRUCTURED_PROPERTIES_MUTATION = """
mutation UpsertGlossaryTermStructuredProperties($input: UpsertStructuredPropertiesInput!) {
  upsertStructuredProperties(input: $input) {
    urn
  }
}
"""

# Remove structured properties from glossary node
REMOVE_GLOSSARY_NODE_STRUCTURED_PROPERTIES_MUTATION = """
mutation RemoveGlossaryNodeStructuredProperties($input: RemoveStructuredPropertiesInput!) {
  removeStructuredProperties(input: $input) {
    urn
  }
}
"""

# Remove structured properties from glossary term
REMOVE_GLOSSARY_TERM_STRUCTURED_PROPERTIES_MUTATION = """
mutation RemoveGlossaryTermStructuredProperties($input: RemoveStructuredPropertiesInput!) {
  removeStructuredProperties(input: $input) {
    urn
  }
}
"""
