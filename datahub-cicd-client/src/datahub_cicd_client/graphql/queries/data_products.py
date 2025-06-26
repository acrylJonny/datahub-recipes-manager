# GraphQL queries for data product operations
from datahub_cicd_client.graphql.fragments.data_products import (
    DATA_PRODUCT_FRAGMENT,
    DATA_PRODUCT_SIMPLE_FRAGMENT,
)

# List all data products
LIST_DATA_PRODUCTS_QUERY = f"""
{DATA_PRODUCT_FRAGMENT}

query ListDataProducts($input: SearchAcrossEntitiesInput!) {{
  searchAcrossEntities(input: $input) {{
    start
    count
    total
    searchResults {{
      entity {{
        ...DataProductFragment
      }}
    }}
  }}
}}
"""

# List data products with simple fragment (for performance)
LIST_DATA_PRODUCTS_SIMPLE_QUERY = f"""
{DATA_PRODUCT_SIMPLE_FRAGMENT}

query ListDataProductsSimple($input: SearchAcrossEntitiesInput!) {{
  searchAcrossEntities(input: $input) {{
    start
    count
    total
    searchResults {{
      entity {{
        ...DataProductSimpleFragment
      }}
    }}
  }}
}}
"""

# Get a specific data product
GET_DATA_PRODUCT_QUERY = f"""
{DATA_PRODUCT_FRAGMENT}

query GetDataProduct($urn: String!) {{
  dataProduct(urn: $urn) {{
    ...DataProductFragment
  }}
}}
"""

# Count data products
COUNT_DATA_PRODUCTS_QUERY = """
query CountDataProducts($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    total
  }
}
"""

# Find data products by domain
FIND_DATA_PRODUCTS_BY_DOMAIN_QUERY = f"""
{DATA_PRODUCT_FRAGMENT}

query FindDataProductsByDomain($input: SearchAcrossEntitiesInput!) {{
  searchAcrossEntities(input: $input) {{
    start
    count
    total
    searchResults {{
      entity {{
        ...DataProductFragment
      }}
    }}
  }}
}}
"""

# Find data products by owner
FIND_DATA_PRODUCTS_BY_OWNER_QUERY = f"""
{DATA_PRODUCT_FRAGMENT}

query FindDataProductsByOwner($input: SearchAcrossEntitiesInput!) {{
  searchAcrossEntities(input: $input) {{
    start
    count
    total
    searchResults {{
      entity {{
        ...DataProductFragment
      }}
    }}
  }}
}}
"""

# Get data product assets/entities
GET_DATA_PRODUCT_ASSETS_QUERY = """
query GetDataProductAssets($urn: String!, $input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    start
    count
    total
    searchResults {
      entity {
        urn
        type
        ... on Dataset {
          properties {
            name
            description
          }
          platform {
            urn
            properties {
              displayName
            }
          }
        }
        ... on Chart {
          properties {
            title
            description
          }
        }
        ... on Dashboard {
          properties {
            title
            description
          }
        }
        ... on DataJob {
          properties {
            name
            description
          }
        }
        ... on DataFlow {
          properties {
            name
            description
          }
        }
      }
    }
  }
}
"""
