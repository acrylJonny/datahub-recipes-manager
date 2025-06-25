# Comprehensive URN Generation System

This document describes the comprehensive URN generation system that has been implemented across all entity types in the DataHub Recipes Manager. The system provides consistent, environment-aware URN generation using MD5 hashing when mutations are enabled.

## Overview

The URN generation system allows for:
- **Environment-aware URN generation**: Different environments generate different URNs for the same entity
- **Conditional mutations**: URNs are only mutated when mutation checkboxes are enabled in the UI
- **Consistent hashing**: Same input always produces the same output URN
- **Cross-entity support**: Works with all supported entity types
- **Backward compatibility**: Existing workflows continue to work unchanged

## Supported Entity Types

The system now supports all major DataHub entity types:

### 1. Tags
- **Actions file**: `scripts/mcps/tag_actions.py`
- **MCP creation**: `scripts/mcps/create_tag_mcps.py`
- **CLI**: `scripts/mcps/cli_tag_actions.py`
- **URN function**: `generate_tag_urn()`

### 2. Glossary Terms
- **Actions file**: `scripts/mcps/glossary_actions.py`
- **MCP creation**: `scripts/mcps/create_glossary_mcps.py`
- **CLI**: `scripts/mcps/cli_glossary_actions.py`
- **URN function**: `generate_glossary_term_urn()`

### 3. Glossary Nodes
- **Actions file**: `scripts/mcps/glossary_actions.py`
- **MCP creation**: `scripts/mcps/create_glossary_mcps.py`
- **CLI**: `scripts/mcps/cli_glossary_actions.py`
- **URN function**: `generate_glossary_node_urn()`

### 4. Structured Properties
- **Actions file**: `scripts/mcps/structured_property_actions.py`
- **MCP creation**: `scripts/mcps/create_structured_property_mcps.py`
- **CLI**: `scripts/mcps/cli_structured_property_actions.py`
- **URN function**: `generate_structured_property_urn()`

### 5. Domains
- **Actions file**: `scripts/mcps/domain_actions.py`
- **MCP creation**: `scripts/mcps/create_domain_mcps.py`
- **CLI**: `scripts/mcps/cli_domain_actions.py`
- **URN function**: `generate_domain_urn()`

### 6. Data Products
- **Actions file**: `scripts/mcps/data_product_actions.py` (to be implemented)
- **MCP creation**: `scripts/mcps/create_data_product_mcps.py` (to be implemented)
- **CLI**: `scripts/mcps/cli_data_product_actions.py` (to be implemented)
- **URN function**: `generate_data_product_urn()`

## Core Components

### 1. URN Generation Utilities (`web_ui/utils/urn_utils.py`)

The core utilities provide:
- `generate_mutated_urn()`: Main function for URN generation with MD5 hashing
- Entity-specific functions: `generate_tag_urn()`, `generate_glossary_term_urn()`, etc.
- `get_mutation_config_for_environment()`: Retrieves mutation settings from database
- `apply_urn_mutations_to_entity()`: Applies mutations to entity data structures

### 2. Database Integration

The system integrates with the mutations page configuration:
- **Model**: `web_ui.models.Mutation`
- **Fields**: `apply_to_tags`, `apply_to_glossary_terms`, `apply_to_glossary_nodes`, `apply_to_structured_properties`, `apply_to_domains`, `apply_to_data_products`
- **Environment association**: Each mutation is linked to a specific environment

### 3. MCP Script Integration

All MCP creation scripts now support:
- **Environment parameter**: `--environment dev|staging|prod`
- **Mutation name parameter**: `--mutation-name <name>`
- **Custom URN parameter**: `custom_urn` for direct URN specification
- **Graceful fallback**: Works without new utilities for backward compatibility

## URN Generation Examples

### Original URNs
```
urn:li:tag:PII
urn:li:glossaryTerm:CustomerData
urn:li:glossaryNode:DataGovernance
urn:li:structuredProperty:department
urn:li:domain:finance
```

### Mutated URNs (when mutations enabled)

#### Development Environment
```
urn:li:tag:4b7df797f1e116cd
urn:li:glossaryTerm:8a3f2c1d5e9b7a42
urn:li:glossaryNode:f2e8d4a6b1c9e7f3
urn:li:structuredProperty:7c5e9b2a4f8d1c6e
urn:li:domain:3a8f5c2e9b7d4a1f
```

#### Staging Environment
```
urn:li:tag:17444c13a108cc40
urn:li:glossaryTerm:9d2f8e5a3c7b4f1e
urn:li:glossaryNode:e4b7a2d8f5c9e3a6
urn:li:structuredProperty:6f9c3e7a2d5b8f4c
urn:li:domain:2c9e6a4f7b3d8e1a
```

#### Production Environment
```
urn:li:tag:450e358dcb43fb09
urn:li:glossaryTerm:a5c8f3e6b2d9a7e4
urn:li:glossaryNode:d7f4a9c2e6b8d3f7
urn:li:structuredProperty:8e2a5f9c4d7b3e6a
urn:li:domain:4f7d2a9e5c8b6f3d
```

## Usage Examples

### CLI Usage

#### Tags
```bash
# Add tag to staged changes with environment-specific URN
python scripts/mcps/cli_tag_actions.py stage \
  --tag-data '{"id": "PII", "name": "Personally Identifiable Information"}' \
  --environment staging \
  --owner admin

# Using file input
python scripts/mcps/cli_tag_actions.py stage \
  --tag-data /path/to/tag.json \
  --environment prod \
  --mutation-name custom_mutation
```

#### Glossary Terms
```bash
# Add glossary term with environment-specific URN
python scripts/mcps/cli_glossary_actions.py stage \
  --entity-type term \
  --entity-data '{"id": "CustomerData", "name": "Customer Data"}' \
  --environment dev \
  --owner admin
```

#### Structured Properties
```bash
# Add structured property with environment-specific URN
python scripts/mcps/cli_structured_property_actions.py stage-property \
  --property-file property.json \
  --environment staging \
  --owner admin \
  --mutation-name custom_mutation
```

#### Domains
```bash
# Add domain with environment-specific URN
python scripts/mcps/cli_domain_actions.py stage-domain \
  --domain-file domain.json \
  --environment prod \
  --owner admin
```

### Programmatic Usage

```python
from utils.urn_utils import (
    generate_tag_urn,
    generate_glossary_term_urn,
    generate_structured_property_urn,
    get_mutation_config_for_environment
)

# Get mutation configuration for environment
mutation_config = get_mutation_config_for_environment('staging')

# Generate mutated URNs
original_tag_urn = "urn:li:tag:PII"
mutated_tag_urn = generate_tag_urn(original_tag_urn, 'staging', mutation_config)

original_term_urn = "urn:li:glossaryTerm:CustomerData"
mutated_term_urn = generate_glossary_term_urn(original_term_urn, 'staging', mutation_config)

# Apply mutations to entity data
from utils.urn_utils import apply_urn_mutations_to_entity

entity_data = {
    "urn": "urn:li:tag:PII",
    "name": "Personally Identifiable Information"
}

mutated_data = apply_urn_mutations_to_entity(
    entity_data, 'staging', mutation_config, 'tag'
)
```

### Web UI Integration

The system is automatically integrated with the web UI:

1. **Mutations Page**: Configure which entity types should have mutated URNs
2. **Environment Selection**: Choose target environment for staged changes
3. **Staged Changes**: All "Add to Staged Changes" actions use the new system
4. **Bulk Operations**: Bulk staged changes operations support the new URN generation

## File Structure

### Staged Changes Output

All entity types now create files in the same structure:
```
metadata-manager/
├── dev/
│   ├── tags/mcp_file.json
│   ├── glossary/mcp_file.json
│   ├── structured_properties/mcp_file.json
│   ├── domains/mcp_file.json
│   └── data_products/mcp_file.json
├── staging/
│   ├── tags/mcp_file.json
│   ├── glossary/mcp_file.json
│   ├── structured_properties/mcp_file.json
│   ├── domains/mcp_file.json
│   └── data_products/mcp_file.json
└── prod/
    ├── tags/mcp_file.json
    ├── glossary/mcp_file.json
    ├── structured_properties/mcp_file.json
    ├── domains/mcp_file.json
    └── data_products/mcp_file.json
```

### MCP File Format

All `mcp_file.json` files use the same simple list format:
```json
[
  {
    "entityType": "tag",
    "entityUrn": "urn:li:tag:4b7df797f1e116cd",
    "changeType": "UPSERT",
    "aspectName": "tagProperties",
    "aspect": { ... }
  },
  {
    "entityType": "tag",
    "entityUrn": "urn:li:tag:4b7df797f1e116cd",
    "changeType": "UPSERT",
    "aspectName": "ownership",
    "aspect": { ... }
  }
]
```

## Migration Path

### For Existing Scripts
1. **No changes required**: Existing scripts continue to work
2. **Optional enhancement**: Add `--environment` parameter to CLI calls
3. **Gradual adoption**: Update scripts one at a time as needed

### For New Scripts
1. **Use environment parameter**: Always specify `--environment`
2. **Follow patterns**: Use the established patterns from existing scripts
3. **Test mutations**: Verify URN generation works as expected

## Testing

### Consistency Testing
```bash
# Test that same input produces same output
python -c "
from utils.urn_utils import generate_tag_urn, get_mutation_config_for_environment
config = get_mutation_config_for_environment('dev')
urn1 = generate_tag_urn('urn:li:tag:PII', 'dev', config)
urn2 = generate_tag_urn('urn:li:tag:PII', 'dev', config)
assert urn1 == urn2, f'Inconsistent: {urn1} != {urn2}'
print('✅ Consistency test passed')
"
```

### Environment Differentiation Testing
```bash
# Test that different environments produce different URNs
python -c "
from utils.urn_utils import generate_tag_urn, get_mutation_config_for_environment
dev_config = get_mutation_config_for_environment('dev')
staging_config = get_mutation_config_for_environment('staging')
dev_urn = generate_tag_urn('urn:li:tag:PII', 'dev', dev_config)
staging_urn = generate_tag_urn('urn:li:tag:PII', 'staging', staging_config)
assert dev_urn != staging_urn, f'Same URN: {dev_urn} == {staging_urn}'
print('✅ Environment differentiation test passed')
"
```

## Troubleshooting

### Common Issues

1. **ImportError for new utilities**: 
   - **Cause**: New utilities not available in older installations
   - **Solution**: System gracefully falls back to original URN generation

2. **Mutation config not found**:
   - **Cause**: No mutation configured for the environment
   - **Solution**: Original URNs are used unchanged

3. **Inconsistent URNs**:
   - **Cause**: Environment name mismatch or mutation config changes
   - **Solution**: Verify environment name and mutation configuration

### Debug Mode

Enable debug logging to see URN generation details:
```bash
python scripts/mcps/cli_tag_actions.py stage \
  --tag-data '{"id": "test"}' \
  --environment dev \
  --log-level DEBUG
```

## Future Enhancements

1. **Data Products**: Complete implementation for data products
2. **Additional Entity Types**: Support for charts, dashboards, datasets
3. **Custom Hash Functions**: Support for different hashing algorithms
4. **URN Validation**: Enhanced validation for generated URNs
5. **Performance Optimization**: Caching for frequently accessed configurations

## Related Documentation

- [Original URN Generation README](README_URN_GENERATION.md) - Tags-specific implementation
- [Mutation System Documentation](../../web_ui/README_MUTATIONS.md) - Web UI mutation configuration
- [MCP Scripts Overview](README.md) - General MCP scripts documentation 