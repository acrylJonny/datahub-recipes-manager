# GraphQL mutations for data product operations

# Create a new data product
CREATE_DATA_PRODUCT_MUTATION = """
mutation CreateDataProduct($input: CreateDataProductInput!) {
  createDataProduct(input: $input) {
    urn
  }
}
"""

# Update data product properties
UPDATE_DATA_PRODUCT_MUTATION = """
mutation UpdateDataProduct($urn: String!, $input: DataProductUpdateInput!) {
  updateDataProduct(urn: $urn, input: $input) {
    urn
  }
}
"""

# Delete a data product
DELETE_DATA_PRODUCT_MUTATION = """
mutation DeleteDataProduct($urn: String!) {
  deleteEntity(urn: $urn)
}
"""

# Add owner to data product
ADD_DATA_PRODUCT_OWNER_MUTATION = """
mutation AddDataProductOwner($input: AddOwnerInput!) {
  addOwner(input: $input)
}
"""

# Remove owner from data product
REMOVE_DATA_PRODUCT_OWNER_MUTATION = """
mutation RemoveDataProductOwner($input: RemoveOwnerInput!) {
  removeOwner(input: $input)
}
"""

# Update data product description
UPDATE_DATA_PRODUCT_DESCRIPTION_MUTATION = """
mutation UpdateDataProductDescription($urn: String!, $description: String!) {
  updateDescription(input: {
    resourceUrn: $urn,
    description: $description
  })
}
"""

# Add tag to data product
ADD_DATA_PRODUCT_TAG_MUTATION = """
mutation AddDataProductTag($input: AddTagInput!) {
  addTag(input: $input)
}
"""

# Remove tag from data product
REMOVE_DATA_PRODUCT_TAG_MUTATION = """
mutation RemoveDataProductTag($input: RemoveTagInput!) {
  removeTag(input: $input)
}
"""

# Add glossary term to data product
ADD_DATA_PRODUCT_GLOSSARY_TERM_MUTATION = """
mutation AddDataProductGlossaryTerm($input: AddTermInput!) {
  addTerm(input: $input)
}
"""

# Remove glossary term from data product
REMOVE_DATA_PRODUCT_GLOSSARY_TERM_MUTATION = """
mutation RemoveDataProductGlossaryTerm($input: RemoveTermInput!) {
  removeTerm(input: $input)
}
"""

# Set data product domain
SET_DATA_PRODUCT_DOMAIN_MUTATION = """
mutation SetDataProductDomain($input: SetDomainInput!) {
  setDomain(input: $input)
}
"""

# Unset data product domain
UNSET_DATA_PRODUCT_DOMAIN_MUTATION = """
mutation UnsetDataProductDomain($input: UnsetDomainInput!) {
  unsetDomain(input: $input)
}
"""

# Upsert structured properties for data product
UPSERT_DATA_PRODUCT_STRUCTURED_PROPERTIES_MUTATION = """
mutation UpsertDataProductStructuredProperties($input: UpsertStructuredPropertiesInput!) {
  upsertStructuredProperties(input: $input) {
    urn
  }
}
"""

# Remove structured properties from data product
REMOVE_DATA_PRODUCT_STRUCTURED_PROPERTIES_MUTATION = """
mutation RemoveDataProductStructuredProperties($input: RemoveStructuredPropertiesInput!) {
  removeStructuredProperties(input: $input) {
    urn
  }
}
"""

# Add assets to data product
ADD_ASSETS_TO_DATA_PRODUCT_MUTATION = """
mutation AddAssetsToDataProduct($input: AddAssetsToDataProductInput!) {
  addAssetsToDataProduct(input: $input) {
    urn
  }
}
"""

# Remove assets from data product
REMOVE_ASSETS_FROM_DATA_PRODUCT_MUTATION = """
mutation RemoveAssetsFromDataProduct($input: RemoveAssetsFromDataProductInput!) {
  removeAssetsFromDataProduct(input: $input) {
    urn
  }
}
"""
