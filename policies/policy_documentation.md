# DataHub Policy Structure and Examples

## Overview

DataHub policies control access to metadata and platform features. Policies define:
- **Who** can access (actors)
- **What** they can access (resources)
- **What** they can do (privileges)

## Policy Types

DataHub supports two main policy types:

1. **METADATA** - Controls access to data assets and their metadata
2. **PLATFORM** - Controls access to platform-level features and administrative functions

## Policy Structure

A policy JSON file consists of two main sections:

```json
{
  "policy": { /* Policy definition */ },
  "metadata": { /* Export metadata */ }
}
```

### Policy Definition

The policy definition includes:

| Field | Description | Required |
|-------|-------------|----------|
| name | Display name for the policy | Yes |
| description | Brief description of the policy purpose | No |
| type | Policy type: METADATA or PLATFORM | Yes |
| state | Policy state: ACTIVE or INACTIVE | Yes |
| privileges | List of actions allowed by this policy | Yes |
| resources | Resources the policy applies to | Yes |
| actors | Users and groups the policy applies to | Yes |

### Common Privileges

#### Metadata Privileges
- `VIEW_ENTITY_PAGE` - View entity details page
- `EDIT_ENTITY_TAGS` - Add/remove tags on entities
- `EDIT_ENTITY_OWNERSHIP` - Change entity ownership
- `EDIT_ENTITY_DOCS` - Edit entity documentation
- `EDIT_ENTITY_STATUS` - Change entity status
- `EDIT_ENTITY_DOMAIN` - Change entity domain
- `EDIT_ENTITY_DEPRECATION` - Mark entity as deprecated
- `EDIT_ENTITY_OWNERS` - Edit entity owners
- `EDIT_ENTITY_PROPERTIES` - Edit entity properties
- `EDIT_GLOSSARY_TERMS` - Edit glossary terms
- `GENERATE_ENTITY_LINEAGE` - Generate lineage for an entity
- `VIEW_ENTITY_USAGE` - View entity usage statistics
- `VIEW_ENTITY_LINEAGE` - View entity lineage graph

#### Platform Privileges
- `MANAGE_POLICIES` - Create, update, and delete policies
- `MANAGE_USERS_AND_GROUPS` - Manage platform users and groups
- `MANAGE_INGESTION` - Manage ingestion sources
- `MANAGE_SECRETS` - Manage platform secrets
- `MANAGE_DOMAINS` - Manage domains
- `MANAGE_GLOSSARIES` - Manage glossary terms
- `MANAGE_ACCESS_TOKENS` - Manage access tokens
- `MANAGE_ASSERTIONS` - Manage data quality assertions

### Resource Types

Resources can be of different types:
- `DATASET` - Data tables and views
- `DASHBOARD` - BI dashboards
- `CHART` - BI charts and visualizations
- `DATA_FLOW` - Data pipelines
- `ML_FEATURE_TABLE` - ML feature tables
- `ML_MODEL` - Machine learning models
- `ALL` - All resource types

### Filter Criteria

Resource filters use criteria to match specific entities:

| Field | Description | Example Values |
|-------|-------------|----------------|
| origin | Source system | snowflake, bigquery, tableau |
| domain | Data domain | marketing, finance, operations |
| tags | Entity tags | PII, sensitive, public |
| container | Container/project | data-engineering, marketing-analytics |

### Actors

Actors specify who the policy applies to:

| Field | Description |
|-------|-------------|
| users | List of specific users |
| groups | List of user groups |
| allUsers | Boolean; applies to all users if true |
| allGroups | Boolean; applies to all groups if true |
| resourceOwners | Boolean; applies to resource owners if true |
| resourceOwnersTypes | Types of owners (TECHNICAL_OWNER, BUSINESS_OWNER, etc.) |

## Example Policies

### Basic Metadata View Policy

```json
{
  "policy": {
    "name": "Example Metadata Access Policy",
    "description": "This policy grants view access to all users for datasets with the 'public' tag",
    "type": "METADATA",
    "state": "ACTIVE",
    "privileges": [
      "VIEW_ENTITY_PAGE", 
      "VIEW_ENTITY_USAGE", 
      "VIEW_ENTITY_LINEAGE"
    ],
    "resources": {
      "type": "DATASET",
      "allResources": false,
      "resources": [],
      "filter": {
        "criteria": [
          {
            "field": "tags",
            "values": [
              {"value": "public"}
            ],
            "condition": "EQUALS"
          }
        ]
      }
    },
    "actors": {
      "users": [],
      "groups": [],
      "allUsers": true,
      "allGroups": false,
      "resourceOwners": false,
      "resourceOwnersTypes": []
    }
  }
}
```

### Dashboard Access Policy

```json
{
  "policy": {
    "name": "Data Analytics Team Dashboard Access",
    "description": "Grants dashboard access and edit permissions to the data analytics team for dashboards in the analytics domain",
    "type": "METADATA",
    "state": "ACTIVE",
    "privileges": [
      "VIEW_ENTITY_PAGE",
      "EDIT_ENTITY_OWNERS",
      "EDIT_ENTITY_DOCS",
      "EDIT_ENTITY_TAGS"
    ],
    "resources": {
      "type": "DASHBOARD",
      "allResources": false,
      "resources": [],
      "filter": {
        "criteria": [
          {
            "field": "origin",
            "values": [
              {"value": "superset"},
              {"value": "looker"},
              {"value": "tableau"}
            ],
            "condition": "EQUALS"
          },
          {
            "field": "domain",
            "values": [
              {"value": "analytics"}
            ],
            "condition": "EQUALS"
          }
        ]
      }
    },
    "actors": {
      "users": [
        "alice",
        "bob"
      ],
      "groups": [
        "data-analytics-team"
      ],
      "allUsers": false,
      "allGroups": false,
      "resourceOwners": true,
      "resourceOwnersTypes": [
        "TECHNICAL_OWNER"
      ]
    }
  }
}
```

### Platform Admin Policy

```json
{
  "policy": {
    "name": "Platform Admin Access",
    "description": "Grants administrative access to platform-level features for the admin team",
    "type": "PLATFORM",
    "state": "ACTIVE",
    "privileges": [
      "MANAGE_POLICIES",
      "MANAGE_USERS_AND_GROUPS",
      "MANAGE_INGESTION",
      "MANAGE_SECRETS"
    ],
    "resources": {
      "type": "PLATFORM",
      "allResources": true,
      "resources": []
    },
    "actors": {
      "users": [
        "admin1",
        "admin2"
      ],
      "groups": [
        "platform-admins"
      ],
      "allUsers": false,
      "allGroups": false,
      "resourceOwners": false,
      "resourceOwnersTypes": []
    }
  }
}
```

## Managing Policies

### Creating a Policy

```bash
python scripts/manage_policy.py create --name "Test Policy" --description "Description" --type METADATA --privileges '["VIEW_ENTITY_PAGE"]'
```

### Updating a Policy

```bash
python scripts/manage_policy.py update my-policy-id --description "Updated description" --state INACTIVE
```

### Exporting Policies

```bash
python scripts/export_policy.py --output-dir policies/
```

### Importing Policies

```bash
python scripts/import_policy.py --input-dir policies/ --force-update
```

## Best Practices

1. **Start restrictive**: Begin with minimal access and expand as needed
2. **Use specific filters**: Target resources precisely rather than using broad permissions
3. **Leverage groups**: Assign policies to groups rather than individual users
4. **Document policies**: Add clear descriptions to all policies
5. **Audit regularly**: Review and update policies on a scheduled basis
6. **Version control**: Store policies in git for change tracking and review
7. **Environment promotion**: Test policies in dev/staging before applying to production 