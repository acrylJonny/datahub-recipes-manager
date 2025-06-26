# GraphQL fragments for glossary operations

# Basic glossary node fragment
GLOSSARY_NODE_FRAGMENT = """
fragment GlossaryNodeFragment on GlossaryNode {
  urn
  type
  properties {
    name
    description
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
    }
    lastModified {
      actor
      time
    }
  }
  parentNodes {
    nodes {
      urn
      properties {
        name
      }
    }
  }
  displayProperties {
    colorHex
    icon {
      iconLibrary
      name
      style
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
  institutionalMemory {
    elements {
      url
      label
      actor {
        ... on CorpUser {
          urn
        }
        ... on CorpGroup {
          urn
        }
      }
      created {
        actor
        time
      }
      updated {
        actor
        time
      }
      settings {
        showInAssetPreview
      }
    }
  }
}
"""

# Basic glossary term fragment
GLOSSARY_TERM_FRAGMENT = """
fragment GlossaryTermFragment on GlossaryTerm {
  urn
  type
  properties {
    name
    description
    termSource
    sourceRef
    sourceUrl
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
    }
    lastModified {
      actor
      time
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
  application {
    application {
      urn
    }
  }
  deprecation {
    deprecated
  }
  isRelatedTerms: relationships(input: {types: ["IsA"], direction: OUTGOING}) {
    relationships {
      entity {
        ... on GlossaryTerm {
          urn
          properties {
            name
          }
        }
      }
    }
  }
  hasRelatedTerms: relationships(input: {types: ["HasA"], direction: OUTGOING}) {
    relationships {
      entity {
        ... on GlossaryTerm {
          urn
          properties {
            name
          }
        }
      }
    }
  }
  parentNodes {
    nodes {
      urn
      properties {
        name
      }
    }
  }
  institutionalMemory {
    elements {
      url
      label
      actor {
        ... on CorpUser {
          urn
        }
        ... on CorpGroup {
          urn
        }
      }
      created {
        actor
        time
      }
      updated {
        actor
        time
      }
      settings {
        showInAssetPreview
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
}
"""

# Relationship fragment for glossary entities
GLOSSARY_RELATIONSHIP_FRAGMENT = """
fragment GlossaryRelationshipFragment on EntityRelationshipLegacy {
  type
  direction
  entity {
    urn
    type
    ... on GlossaryTerm {
      properties {
        name
      }
    }
    ... on GlossaryNode {
      properties {
        name
      }
    }
  }
  created {
    time
    actor
  }
}
"""
