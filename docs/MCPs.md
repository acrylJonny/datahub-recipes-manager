# MCP (Metadata Change Proposal) Scripts

This directory contains scripts for creating and managing MCP files for DataHub metadata entities with comprehensive support for all available aspects.

## Overview

MCP files are JSON files that contain metadata change proposals for DataHub. They define the structure and content of metadata entities like tags, glossary nodes, glossary terms, and domains with full support for all aspects defined in the DataHub metamodel.

## Supported Entities and Aspects

### Glossary Nodes
**Script:** `create_glossary_mcps.py` | **Actions:** `glossary_actions.py`
**Supported Aspects:**
- `glossaryNodeInfo` - Core node information (name, description, parent)
- `ownership` - Node ownership information
- `status` - Soft delete status
- `globalTags` - Associated tags
- `glossaryTerms` - Associated glossary terms
- `browsePaths` - Browse path hierarchy
- `institutionalMemory` - Documentation links and context

### Glossary Terms
**Script:** `create_glossary_mcps.py` | **Actions:** `glossary_actions.py`
**Supported Aspects:**
- `glossaryTermInfo` - Core term information (name, definition, source)
- `ownership` - Term ownership information
- `status` - Soft delete status
- `globalTags` - Associated tags
- `glossaryTerms` - Associated glossary terms
- `browsePaths` - Browse path hierarchy
- `institutionalMemory` - Documentation links and context
- `glossaryRelatedTerms` - Related terms and relationships

### Domains
**Script:** `create_domain_mcps.py` | **Actions:** `domain_actions.py`
**Supported Aspects:**
- `domainProperties` - Core domain information (name, description, parent)
- `ownership` - Domain ownership information
- `institutionalMemory` - Documentation links and context
- `structuredProperties` - Structured property assignments
- `forms` - Form associations (incomplete/completed)
- `testResults` - Test execution results
- `displayProperties` - Visual display properties (color, icon)

### Structured Properties
**Script:** `create_structured_property_mcps.py` | **Actions:** `structured_property_actions.py`
**Supported Aspects:**
- `structuredPropertyDefinition` - Core property definition (type, cardinality, allowed values)
- `ownership` - Property ownership information
- `status` - Soft delete status
- `globalTags` - Associated tags
- `glossaryTerms` - Associated glossary terms
- `institutionalMemory` - Documentation links and context

### Data Products
**Script:** `create_data_product_mcps.py` | **Actions:** `data_product_actions.py`
**Supported Aspects:**
- `dataProductProperties` - Core data product information
- `editableDataProductProperties` - Editable properties
- `ownership` - Data product ownership information
- `status` - Soft delete status
- `globalTags` - Associated tags
- `glossaryTerms` - Associated glossary terms
- `institutionalMemory` - Documentation links
- `browsePaths` - Browse path hierarchy
- `structuredProperties` - Structured property assignments
- `domains` - Associated domains
- `subTypes` - Entity sub-types
- `deprecation` - Deprecation status

### Data Contracts
**Script:** `create_data_contract_mcps.py` | **Actions:** `data_contract_actions.py`
**Supported Aspects:**
- `dataContractProperties` - Core contract information (entity, schema, freshness, data quality contracts)
- `dataContractStatus` - Status information (state, custom properties)
- `status` - Soft delete status
- `structuredProperties` - Structured property assignments

## Scripts

### Core MCP Creation Scripts
- `create_glossary_mcps.py` - Comprehensive MCP creation for glossary nodes and terms
- `create_domain_mcps.py` - Comprehensive MCP creation for domains
- `create_structured_property_mcps.py` - Comprehensive MCP creation for structured properties
- `create_data_product_mcps.py` - Comprehensive MCP creation for data products
- `create_data_contract_mcps.py` - Comprehensive MCP creation for data contracts

### Action Scripts (Web UI Integration)
- `glossary_actions.py` - Glossary entity staged changes and web UI integration
- `domain_actions.py` - Domain entity staged changes and web UI integration
- `structured_property_actions.py` - Structured property staged changes and web UI integration
- `data_product_actions.py` - Data product staged changes and web UI integration
- `data_contract_actions.py` - Data contract staged changes and web UI integration

### CLI Interface Scripts
- `cli_glossary_actions.py` - Command-line interface for glossary operations
- `cli_domain_actions.py` - Command-line interface for domain operations
- `cli_structured_property_actions.py` - Command-line interface for structured property operations
- `cli_data_product_actions.py` - Command-line interface for data product operations
- `cli_data_contract_actions.py` - Command-line interface for data contract operations
- `cli_tag_actions.py` - Command-line interface for tag operations

### Legacy Tag Support
- `create_tag_mcps.py` - Creates MCP files for tags with properties and ownership
- `tag_actions.py` - Helper functions for tag-related actions in the web UI

## Usage

### Creating Comprehensive Glossary Node MCPs

```python
from scripts.mcps.glossary_actions import add_glossary_node_to_staged_changes

# Node data with all supported aspects
node_data = {
    "id": "business_metrics",
    "name": "Business Metrics",
    "description": "Key business performance indicators",
    "tags": ["Business", "KPI", "Important"],
    "glossary_terms": ["revenue", "profit", "growth"],
    "browse_paths": ["/Business/Finance", "/Business/Operations"],
    "institutional_memory": [
        {
            "url": "https://wiki.company.com/business-metrics",
            "description": "Business metrics documentation"
        }
    ]
}

# Create comprehensive MCPs for all aspects
result = add_glossary_node_to_staged_changes(
    node_data=node_data,
    environment="prod",
    owner="data_steward",
    include_all_aspects=True
)

print(f"Created {len(result)} MCP files:")
for aspect, path in result.items():
    print(f"  - {aspect}: {path}")
```

### Creating Comprehensive Glossary Term MCPs

```python
from scripts.mcps.glossary_actions import add_glossary_term_to_staged_changes

# Term data with all supported aspects
term_data = {
    "id": "annual_recurring_revenue",
    "name": "Annual Recurring Revenue",
    "description": "Total predictable revenue from subscriptions",
    "parent_urn": "urn:li:glossaryNode:business_metrics",
    "source_ref": "FINANCE_SYSTEM",
    "source_url": "https://finance.company.com/metrics/arr",
    "tags": ["Finance", "Revenue", "KPI"],
    "related_terms": [
        {
            "urn": "urn:li:glossaryTerm:monthly_recurring_revenue",
            "relationshipType": "RELATED"
        }
    ],
    "institutional_memory": [
        {
            "url": "https://wiki.company.com/arr",
            "description": "ARR calculation methodology"
        }
    ]
}

# Create comprehensive MCPs for all aspects
result = add_glossary_term_to_staged_changes(
    term_data=term_data,
    environment="prod",
    owner="finance_team",
    include_all_aspects=True
)
```

### Creating Comprehensive Domain MCPs

```python
from scripts.mcps.domain_actions import add_domain_to_staged_changes

# Domain data with all supported aspects
domain_data = {
    "id": "finance_domain",
    "name": "Finance",
    "description": "Financial data and analytics domain",
    "custom_properties": {
        "department": "Finance",
        "cost_center": "FIN-001",
        "data_classification": "Confidential"
    },
    "structured_properties": [
        {
            "propertyUrn": "urn:li:structuredProperty:department",
            "values": ["Finance"]
        },
        {
            "propertyUrn": "urn:li:structuredProperty:priority",
            "values": ["High"]
        }
    ],
    "institutional_memory": [
        {
            "url": "https://wiki.company.com/finance-domain",
            "description": "Finance domain overview"
        }
    ],
    "color_hex": "#2E8B57",
    "icon_library": "FONT_AWESOME",
    "icon_name": "dollar-sign",
    "icon_style": "SOLID"
}

# Create comprehensive MCPs for all aspects
result = add_domain_to_staged_changes(
    domain_data=domain_data,
    environment="prod",
    owner="domain_owner",
    include_all_aspects=True
)
```

### Creating Structured Property MCPs

```python
from scripts.mcps.structured_property_actions import add_structured_property_to_staged_changes

# Create a structured property for department classification
result = add_structured_property_to_staged_changes(
    property_id="department",
    qualified_name="io.company.metadata.department",
    display_name="Department",
    description="The department that owns this entity",
    value_type="STRING",
    cardinality="SINGLE",
    allowed_values=["Finance", "Engineering", "Marketing", "Sales"],
    entity_types=["dataset", "dashboard", "chart"],
    owners=["urn:li:corpuser:data_governance"],
    tags=["urn:li:tag:Governance", "urn:li:tag:Classification"],
    include_all_aspects=True
)
```

### Creating Data Product MCPs

```python
from scripts.mcps.data_product_actions import add_data_product_to_staged_changes

# Create a data product with comprehensive metadata
result = add_data_product_to_staged_changes(
    data_product_id="customer_analytics",
    name="Customer Analytics",
    description="Comprehensive customer behavior and analytics data product",
    external_url="https://analytics.company.com/customer",
    owners=["urn:li:corpuser:analytics_team"],
    tags=["urn:li:tag:Analytics", "urn:li:tag:Customer"],
    terms=["urn:li:glossaryTerm:customer", "urn:li:glossaryTerm:analytics"],
    domains=["urn:li:domain:customer_domain"],
    links=[
        {
            "url": "https://wiki.company.com/customer-analytics",
            "description": "Customer Analytics Documentation"
        }
    ],
    structured_properties=[
        {
            "propertyUrn": "urn:li:structuredProperty:department",
            "values": ["Analytics"]
        }
    ],
    include_all_aspects=True
)
```

### Creating Data Contract MCPs

```python
from scripts.mcps.data_contract_actions import add_data_contract_to_staged_changes

# Create a data contract for a dataset
result = add_data_contract_to_staged_changes(
    data_contract_id="customer_data_contract",
    entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_data,PROD)",
    schema_assertions=["urn:li:assertion:customer_schema_check"],
    freshness_assertions=["urn:li:assertion:customer_freshness_check"],
    data_quality_assertions=["urn:li:assertion:customer_quality_check"],
    raw_contract="""
    schema:
      - field: customer_id
        type: string
        required: true
      - field: email
        type: string
        required: true
    freshness:
      - cron: "0 6 * * *"
        max_age: "24h"
    quality:
      - field: customer_id
        rules:
          - type: not_null
          - type: unique
    """,
    state="ACTIVE",
    custom_properties={
        "contract_version": "1.0",
        "team": "data_platform"
    },
    include_all_aspects=True
)
```

## CLI Usage

All entity types have dedicated command-line interfaces for easy automation and scripting. Each CLI script follows a consistent pattern with subcommands for different operations.

### Glossary Operations

#### Stage Glossary Node
```bash
# Basic usage
python scripts/mcps/cli_glossary_actions.py stage-node \
  --node-file examples/glossary_node.json \
  --environment prod \
  --owner data_steward

# With custom aspects
python scripts/mcps/cli_glossary_actions.py stage-node \
  --node-file examples/glossary_node.json \
  --custom-aspects '{"customAspect": {"key": "value"}}'
```

#### Stage Glossary Term
```bash
python scripts/mcps/cli_glossary_actions.py stage-term \
  --term-file examples/glossary_term.json \
  --environment prod \
  --owner data_steward
```

### Domain Operations

```bash
python scripts/mcps/cli_domain_actions.py stage-domain \
  --domain-file examples/domain.json \
  --environment prod \
  --owner domain_owner
```

### Structured Property Operations

```bash
python scripts/mcps/cli_structured_property_actions.py stage-property \
  --property-file examples/structured_property.json \
  --environment prod \
  --owner data_governance
```

### Data Product Operations

```bash
python scripts/mcps/cli_data_product_actions.py stage-product \
  --product-file examples/data_product.json \
  --environment prod \
  --owner product_owner
```

### Data Contract Operations

```bash
python scripts/mcps/cli_data_contract_actions.py stage-contract \
  --contract-file examples/data_contract.json \
  --environment prod \
  --owner contract_owner
```

### Example JSON Files

#### Glossary Node (`examples/glossary_node.json`)
```json
{
  "id": "business_metrics",
  "name": "Business Metrics",
  "description": "Key business performance indicators",
  "tags": ["Business", "KPI", "Important"],
  "glossary_terms": ["revenue", "profit", "growth"],
  "browse_paths": ["/Business/Finance", "/Business/Operations"],
  "institutional_memory": [
    {
      "url": "https://wiki.company.com/business-metrics",
      "description": "Business metrics documentation"
    }
  ]
}
```

#### Structured Property (`examples/structured_property.json`)
```json
{
  "property_id": "department",
  "qualified_name": "io.company.metadata.department",
  "display_name": "Department",
  "description": "The department that owns this entity",
  "value_type": "STRING",
  "cardinality": "SINGLE",
  "allowed_values": ["Finance", "Engineering", "Marketing", "Sales"],
  "entity_types": ["dataset", "dashboard", "chart"],
  "owners": ["urn:li:corpuser:data_governance"],
  "tags": ["urn:li:tag:Governance"]
}
```

#### Data Product (`examples/data_product.json`)
```json
{
  "data_product_id": "customer_analytics",
  "name": "Customer Analytics",
  "description": "Comprehensive customer behavior and analytics data product",
  "external_url": "https://analytics.company.com/customer",
  "owners": ["urn:li:corpuser:analytics_team"],
  "tags": ["urn:li:tag:Analytics", "urn:li:tag:Customer"],
  "domains": ["urn:li:domain:customer_domain"],
  "structured_properties": [
    {
      "propertyUrn": "urn:li:structuredProperty:department",
      "values": ["Analytics"]
    }
  ]
}
```

#### Data Contract (`examples/data_contract.json`)
```json
{
  "data_contract_id": "customer_data_contract",
  "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_data,PROD)",
  "schema_assertions": ["urn:li:assertion:customer_schema_check"],
  "freshness_assertions": ["urn:li:assertion:customer_freshness_check"],
  "data_quality_assertions": ["urn:li:assertion:customer_quality_check"],
  "state": "ACTIVE",
  "custom_properties": {
    "contract_version": "1.0",
    "team": "data_platform"
  }
}
```

### CLI Options

All CLI scripts support these common options:

- `--environment` - Environment name for URN generation (default: "dev")
- `--owner` - Owner username (default: "admin")
- `--base-dir` - Base directory for output files (default: "metadata")
- `--include-all-aspects` - Include all supported aspects (default: true)
- `--custom-aspects` - JSON string with custom aspect data
- `--log-level` - Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

### Individual Aspect Creation

You can also create individual aspects:

```python
from scripts.mcps.create_glossary_mcps import (
    create_glossary_node_info_mcp,
    create_glossary_ownership_mcp,
    create_glossary_global_tags_mcp,
    create_domain_properties_mcp,
    create_domain_display_properties_mcp
)

# Create individual MCPs
node_info = create_glossary_node_info_mcp(
    node_id="my_node",
    owner="admin",
    node_name="My Business Node",
    description="A business glossary node"
)

ownership = create_glossary_ownership_mcp(
    entity_id="my_node",
    entity_type="glossaryNode",
    owner="data_steward"
)

tags = create_glossary_global_tags_mcp(
    entity_id="my_node",
    entity_type="glossaryNode",
    tags=["Business", "Important"],
    owner="admin"
)
```

## Web UI Integration

The comprehensive MCP generation is integrated with the web UI through "Add to Staged Changes" buttons:

### Glossary Nodes and Terms
- **Endpoint:** `/metadata/glossary/nodes/<id>/stage_changes/`
- **Endpoint:** `/metadata/glossary/terms/<id>/stage_changes/`
- **JavaScript:** `addToStagedChanges()` function in `glossary_enhanced.js`

### Domains
- **Endpoint:** `/metadata/domains/<id>/stage_changes/`
- **JavaScript:** Similar integration pattern for domain pages

## File Organization

Generated MCP files are organized by entity type:

```
metadata/
├── glossary/
│   ├── {node_id}_glossary_node_info.json
│   ├── {node_id}_ownership.json
│   ├── {node_id}_status.json
│   ├── {node_id}_global_tags.json
│   ├── {term_id}_glossary_term_info.json
│   ├── {term_id}_glossary_related_terms.json
│   └── ...
├── domains/
│   ├── {domain_id}_domain_properties.json
│   ├── {domain_id}_ownership.json
│   ├── {domain_id}_structured_properties.json
│   ├── {domain_id}_display_properties.json
│   └── ...
├── structured_properties/
│   ├── {property_id}_structured_property_definition.json
│   ├── {property_id}_ownership.json
│   ├── {property_id}_status.json
│   └── ...
├── data_products/
│   ├── {product_id}_data_product_properties.json
│   ├── {product_id}_editable_data_product_properties.json
│   ├── {product_id}_ownership.json
│   ├── {product_id}_structured_properties.json
│   └── ...
└── data_contracts/
    ├── {contract_id}_data_contract_properties.json
    ├── {contract_id}_data_contract_status.json
    ├── {contract_id}_status.json
      └── ...
```

## Configuration Options

### Environment and URN Generation
- `environment` - Environment name for deterministic URN generation
- `mutation_name` - Alternative to environment for URN generation
- `owner` - Default owner for ownership aspects

### Aspect Control
- `include_all_aspects=True` - Generate all supported aspects
- `include_all_aspects=False` - Generate only core aspects (info + ownership)
- `custom_aspects` - Dictionary of custom aspect data to include

### File Management
- `base_dir` - Base directory for MCP file generation
- Automatic deduplication of identical MCPs
- Timestamp field removal for consistent comparison

## DataHub Metamodel Compliance

This implementation follows the official DataHub metamodel specifications:

- **Glossary Nodes:** [docs.datahub.io/docs/generated/metamodel/entities/glossarynode](https://docs.datahub.io/docs/generated/metamodel/entities/glossarynode)
- **Glossary Terms:** [docs.datahub.io/docs/generated/metamodel/entities/glossaryterm](https://docs.datahub.io/docs/generated/metamodel/entities/glossaryterm)
- **Domains:** [docs.datahub.io/docs/generated/metamodel/entities/domain](https://docs.datahub.io/docs/generated/metamodel/entities/domain)
- **Structured Properties:** [docs.datahub.io/docs/generated/metamodel/entities/structuredproperty](https://docs.datahub.io/docs/generated/metamodel/entities/structuredproperty)
- **Data Products:** [docs.datahub.io/docs/generated/metamodel/entities/dataproduct](https://docs.datahub.io/docs/generated/metamodel/entities/dataproduct)
- **Data Contracts:** [docs.datahub.io/docs/generated/metamodel/entities/datacontract](https://docs.datahub.io/docs/generated/metamodel/entities/datacontract)

All aspects and their properties are implemented according to the official schema definitions, ensuring compatibility with DataHub ingestion and API operations.

## Tag Actions

The `tag_actions.py`