"""
GraphQL mutations for tag operations.
"""

# Mutation to create a new tag
CREATE_TAG_MUTATION = """
mutation CreateTag($input: CreateTagInput!) {
  createTag(input: $input)
}
"""

# Mutation to update tag properties
UPDATE_TAG_MUTATION = """
mutation UpdateTag($urn: String!, $input: TagUpdateInput!) {
  updateTag(urn: $urn, input: $input) {
    urn
  }
}
"""

# Mutation to delete a tag
DELETE_TAG_MUTATION = """
mutation DeleteTag($urn: String!) {
  deleteTag(urn: $urn)
}
"""

# Mutation to add owner to tag
ADD_TAG_OWNER_MUTATION = """
mutation AddOwners($input: AddOwnerInput!) {
  addOwner(input: $input)
}
"""

# Mutation to remove owner from tag
REMOVE_TAG_OWNER_MUTATION = """
mutation RemoveOwner($input: RemoveOwnerInput!) {
  removeOwner(input: $input)
}
"""

# Mutation to add tag to entity
ADD_TAG_TO_ENTITY_MUTATION = """
mutation AddTags($input: AddTagsInput!) {
  addTags(input: $input)
}
"""

# Mutation to remove tag from entity
REMOVE_TAG_FROM_ENTITY_MUTATION = """
mutation RemoveTags($input: RemoveTagsInput!) {
  removeTags(input: $input)
}
"""

# Mutation to set tag color
SET_TAG_COLOR_MUTATION = """
mutation UpdateTag($urn: String!, $input: TagUpdateInput!) {
  updateTag(urn: $urn, input: $input) {
    urn
    properties {
      colorHex
    }
  }
}
"""

# Mutation to update tag description
UPDATE_TAG_DESCRIPTION_MUTATION = """
mutation UpdateTag($urn: String!, $input: TagUpdateInput!) {
  updateTag(urn: $urn, input: $input) {
    urn
    properties {
      description
    }
  }
}
"""
