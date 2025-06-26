"""
GraphQL queries for user operations.
"""

from datahub_cicd_client.graphql.fragments.users import USER_FRAGMENT, USER_SIMPLE_FRAGMENT

# Query to list users
LIST_USERS_QUERY = f"""
{USER_FRAGMENT}

query ListUsers($input: SearchAcrossEntitiesInput!) {{
  searchAcrossEntities(input: $input) {{
    start
    count
    total
    searchResults {{
      entity {{
        ...UserFragment
      }}
    }}
  }}
}}
"""

# Query to list users with simple fragment (for performance)
LIST_USERS_SIMPLE_QUERY = f"""
{USER_SIMPLE_FRAGMENT}

query ListUsersSimple($input: SearchAcrossEntitiesInput!) {{
  searchAcrossEntities(input: $input) {{
    start
    count
    total
    searchResults {{
      entity {{
        ...UserSimpleFragment
      }}
    }}
  }}
}}
"""

# Query to get a single user by URN
GET_USER_QUERY = f"""
{USER_FRAGMENT}

query GetUser($urn: String!) {{
  corpUser(urn: $urn) {{
    ...UserFragment
  }}
}}
"""

# Query to count users
COUNT_USERS_QUERY = """
query CountUsers($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    total
  }
}
"""
