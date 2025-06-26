# GraphQL queries for glossary operations
from datahub_cicd_client.graphql.fragments.glossary import (
    GLOSSARY_NODE_FRAGMENT,
    GLOSSARY_TERM_FRAGMENT,
)

# List all glossary nodes
LIST_GLOSSARY_NODES_QUERY = f"""
{GLOSSARY_NODE_FRAGMENT}

query ListGlossaryNodes($input: SearchAcrossEntitiesInput!) {{
  searchAcrossEntities(input: $input) {{
    start
    count
    total
    searchResults {{
      entity {{
        ...GlossaryNodeFragment
      }}
    }}
  }}
}}
"""

# Get a specific glossary node
GET_GLOSSARY_NODE_QUERY = f"""
{GLOSSARY_NODE_FRAGMENT}

query GetGlossaryNode($urn: String!) {{
  glossaryNode(urn: $urn) {{
    ...GlossaryNodeFragment
  }}
}}
"""

# List all glossary terms
LIST_GLOSSARY_TERMS_QUERY = f"""
{GLOSSARY_TERM_FRAGMENT}

query ListGlossaryTerms($input: SearchAcrossEntitiesInput!) {{
  searchAcrossEntities(input: $input) {{
    start
    count
    total
    searchResults {{
      entity {{
        ...GlossaryTermFragment
      }}
    }}
  }}
}}
"""

# Get a specific glossary term
GET_GLOSSARY_TERM_QUERY = f"""
{GLOSSARY_TERM_FRAGMENT}

query GetGlossaryTerm($urn: String!) {{
  glossaryTerm(urn: $urn) {{
    ...GlossaryTermFragment
  }}
}}
"""

# Comprehensive glossary data query (both nodes and terms)
GET_COMPREHENSIVE_GLOSSARY_QUERY = f"""
{GLOSSARY_NODE_FRAGMENT}
{GLOSSARY_TERM_FRAGMENT}

query GetComprehensiveGlossary($input: SearchAcrossEntitiesInput!) {{
  searchAcrossEntities(input: $input) {{
    start
    count
    total
    searchResults {{
      entity {{
        urn
        type
        ... on GlossaryNode {{
          ...GlossaryNodeFragment
        }}
        ... on GlossaryTerm {{
          ...GlossaryTermFragment
        }}
      }}
    }}
  }}
}}
"""

# Count glossary nodes
COUNT_GLOSSARY_NODES_QUERY = """
query CountGlossaryNodes($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    total
  }
}
"""

# Count glossary terms
COUNT_GLOSSARY_TERMS_QUERY = """
query CountGlossaryTerms($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    total
  }
}
"""

# Find entities related to a glossary term
FIND_ENTITIES_WITH_GLOSSARY_TERM_QUERY = """
query FindEntitiesWithGlossaryTerm($input: SearchAcrossEntitiesInput!) {
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
            name
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
