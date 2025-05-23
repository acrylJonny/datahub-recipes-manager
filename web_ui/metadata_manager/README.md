# Metadata Manager

The Metadata Manager is a Django application for managing DataHub metadata entities with deterministic URNs. This enables consistent management and synchronization of tags, glossary terms, domains, and assertions across different environments.

## Features

- **Deterministic URN Generation**: Uses MD5 hashing to generate consistent URNs based on entity names
- **Web UI for Metadata Management**: User-friendly interface for managing metadata entities
- **Pull from DataHub**: Import entities directly from DataHub instances
- **Import/Export**: Export entities to JSON files for version control and CICD workflows
- **Environment Synchronization**: Sync metadata between environments (dev, test, prod)
- **Platform Instance Override**: Modify platform instances for environment-specific deployments

## Metadata Entity Types

The following metadata entity types are supported:

- **Tags**: DataHub tags for data assets
- **Glossary Terms**: Business glossary terms and definitions
- **Glossary Nodes**: Hierarchical organization of glossary terms
- **Domains**: Business domains for data ownership
- **Assertions**: Metadata validation rules and tests

## CICD Workflow Integration

The Metadata Manager is designed to be integrated into CICD workflows:

1. **Develop**: Create and modify metadata entities in the development environment
2. **Version Control**: Export metadata to JSON files and check into Git
3. **Pull Request**: Review changes through your Git workflow
4. **Deploy**: Sync metadata to target environments (test, staging, production)

## Deterministic URN Implementation

Deterministic URNs are generated using MD5 hashing with the following format:

```
urn:li:<entity_type>:<md5_hash>
```

Where:
- `<entity_type>` is one of: tag, glossaryTerm, glossaryNode, domain
- `<md5_hash>` is the MD5 hash of the entity type and name (and parent for hierarchical entities)

Example MD5 generation:
```bash
# For a tag named "PII"
echo -n "tag:pii" | md5sum

# For a glossary term with a parent node
echo -n "urn:li:glossarynode:parent_id:glossaryterm:customer data" | md5sum
```

This ensures that the same entity gets the same URN across all environments, making synchronization reliable. 