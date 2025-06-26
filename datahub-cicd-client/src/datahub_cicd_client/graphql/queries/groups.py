"""
GraphQL queries for group management operations.
"""

from datahub_cicd_client.graphql.fragments.groups import (
    GROUP_FRAGMENT,
    GROUP_SIMPLE_FRAGMENT,
    GROUP_WITH_MEMBERS_FRAGMENT,
    GROUP_WITH_OWNERSHIP_FRAGMENT,
)

# Query to list groups
LIST_GROUPS_QUERY = f"""
{GROUP_FRAGMENT}

query ListGroups($input: SearchAcrossEntitiesInput!) {{
  searchAcrossEntities(input: $input) {{
    start
    count
    total
    searchResults {{
      entity {{
        ...GroupFragment
      }}
    }}
  }}
}}
"""

# Query to list groups with simple fragment (for performance)
LIST_GROUPS_SIMPLE_QUERY = f"""
{GROUP_SIMPLE_FRAGMENT}

query ListGroupsSimple($input: SearchAcrossEntitiesInput!) {{
  searchAcrossEntities(input: $input) {{
    start
    count
    total
    searchResults {{
      entity {{
        ...GroupSimpleFragment
      }}
    }}
  }}
}}
"""

# Query to get a single group by URN with full details
GET_GROUP_QUERY = f"""
{GROUP_WITH_MEMBERS_FRAGMENT}
{GROUP_WITH_OWNERSHIP_FRAGMENT}

query GetGroup($urn: String!) {{
  corpGroup(urn: $urn) {{
    ...GroupWithMembersFragment
    ...GroupWithOwnershipFragment
  }}
}}
"""

# Query to count groups
COUNT_GROUPS_QUERY = """
query CountGroups($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    total
  }
}
"""
