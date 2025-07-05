# GraphQL fragments for data product operations

# Basic data product fragment
DATA_PRODUCT_FRAGMENT = """
fragment DataProductFragment on DataProduct {
  urn
  type
  properties {
    name
    description
    externalUrl
    numAssets
    customProperties {
      key
      value
    }
  }
  ownership {
    owners {
      owner {
        ... on CorpUser {
          urn
          username
          properties {
            displayName
            email
          }
        }
        ... on CorpGroup {
          urn
          name
          properties {
            displayName
          }
        }
      }
      ownershipType {
        urn
        info {
          name
        }
      }
      source {
        type
        url
      }
    }
    lastModified {
      actor
      time
    }
  }
  glossaryTerms {
    terms {
      term {
        urn
        properties {
          name
        }
      }
    }
  }
  domain {
    domain {
      urn
      properties {
        name
        description
      }
    }
  }
  tags {
    tags {
      tag {
        urn
        properties {
          name
          description
        }
      }
    }
  }
  structuredProperties {
    properties {
      structuredProperty {
        urn
        definition {
          displayName
          qualifiedName
        }
      }
      values {
        ... on StringValue {
          stringValue
        }
        ... on NumberValue {
          numberValue
        }
      }
      valueEntities {
        urn
      }
    }
  }
  application {
    application {
      urn
    }
  }
}
"""

# Simple data product fragment for listing
DATA_PRODUCT_SIMPLE_FRAGMENT = """
fragment DataProductSimpleFragment on DataProduct {
  urn
  type
  properties {
    name
    description
    externalUrl
    numAssets
  }
  ownership {
    owners {
      owner {
        ... on CorpUser {
          urn
          username
          properties {
            displayName
          }
        }
        ... on CorpGroup {
          urn
          name
          properties {
            displayName
          }
        }
      }
    }
  }
  domain {
    domain {
      urn
      properties {
        name
      }
    }
  }
}
"""
