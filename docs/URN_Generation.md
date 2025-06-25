# Environment-Based URN Generation System

This document explains the new consistent URN generation system that creates MD5-based URNs when mutation checkboxes are enabled for specific entity types.

## Overview

The system provides:
1. **Consistent URN Generation**: Uses MD5 hashing based on environment name + input URN
2. **Conditional Mutations**: Only applies mutations when checkboxes are enabled in the mutations page
3. **Environment Differentiation**: Different environments produce different URNs
4. **Backward Compatibility**: Falls back to original URNs when mutations are disabled

## How It Works

### 1. Mutation Configuration
In the mutations page (`/mutations/1/edit/`), you can enable mutations for:
- ✅ Tags
- ✅ Glossary Terms  
- ✅ Glossary Nodes (newly added)
- ✅ Structured Properties
- ✅ Domains
- ✅ Data Products (newly added)

### 2. URN Generation Logic
When a checkbox is checked for an entity type:
```
Input: environment_name + original_urn
Process: MD5 hash → first 16 characters
Output: urn:li:entityType:md5_hash
```

When checkbox is unchecked or no mutations exist:
```
Output: original_urn (unchanged)
```

### 3. Example URN Transformations

**Environment: `dev`, Tags mutations enabled:**
```
Input:  urn:li:tag:PII
Output: urn:li:tag:4b7df797f1e116cd
```

**Environment: `staging`, Tags mutations enabled:**
```
Input:  urn:li:tag:PII  
Output: urn:li:tag:17444c13a108cc40
```

**Environment: `dev`, Tags mutations disabled:**
```
Input:  urn:li:tag:PII
Output: urn:li:tag:PII (unchanged)
```

## Usage in MCP Scripts

### Automatic Integration
The MCP scripts have been updated to automatically use the new URN generation system:

```python
# In tag_actions.py, glossary_actions.py, etc.
from utils.urn_utils import generate_tag_urn, get_mutation_config_for_environment

# Get mutation configuration for environment
mutation_config = get_mutation_config_for_environment(environment)

# Generate mutated URN if mutations are configured
mutated_urn = generate_tag_urn(original_urn, environment, mutation_config)
```

### CLI Usage with Environment Parameter

All MCP scripts now support the `--environment` parameter:

```bash
# Create tag with environment-based URN generation
python scripts/mcps/create_tag_mcps.py \
  --tag-id "PII" \
  --tag-name "Personally Identifiable Information" \
  --owner "datahub" \
  --environment "dev"

# Use CLI tag actions
python scripts/mcps/cli_tag_actions.py \
  --action stage \
  --tag-urn "urn:li:tag:PII" \
  --tag-name "PII" \
  --environment "dev" \
  --owner "datahub"
```

## Available Utility Functions

### Core Functions
- `generate_mutated_urn(input_urn, environment_name, entity_type, mutation_config)`
- `get_mutation_config_for_environment(environment_name)`
- `apply_urn_mutations_to_entity(entity_data, environment_name)`

### Convenience Functions
- `generate_tag_urn(input_urn, environment_name, mutation_config)`
- `generate_glossary_term_urn(input_urn, environment_name, mutation_config)`
- `generate_glossary_node_urn(input_urn, environment_name, mutation_config)`
- `generate_structured_property_urn(input_urn, environment_name, mutation_config)`
- `generate_domain_urn(input_urn, environment_name, mutation_config)`
- `generate_data_product_urn(input_urn, environment_name, mutation_config)`

## Platform Instance Mapping

The system also handles platform instance mapping when exporting. If:
1. Platform instance is present in the URN
2. A platform instance mapping exists in mutations
3. The mapping has a "from" → "to" transformation

Then the URN will be updated accordingly during staged changes.

## Migration Path

### Existing Scripts
All existing MCP scripts continue to work as before. The new URN generation is:
- **Opt-in**: Only applies when mutations are configured
- **Backward compatible**: Falls back to original behavior
- **Environment aware**: Uses environment parameter if provided

### New Projects
For new projects, simply:
1. Configure mutations in the web UI (`/mutations/`)
2. Use the `--environment` parameter in CLI scripts
3. The system will automatically handle URN generation

## Testing

The system has been thoroughly tested with:
- ✅ Consistency checks (same input → same output)
- ✅ Environment differentiation (different environments → different URNs)
- ✅ Mutation flag respect (only mutate when enabled)
- ✅ Backward compatibility (unchanged when disabled)

## Files Modified

### Core System
- `web_ui/utils/urn_utils.py` - New URN generation utilities
- `web_ui/web_ui/models.py` - Added mutation fields for glossary nodes and data products
- `web_ui/templates/mutations/` - Updated mutation forms

### MCP Scripts
- `scripts/mcps/tag_actions.py` - Updated for new URN generation
- `scripts/mcps/create_tag_mcps.py` - Added custom_urn parameter support
- `scripts/mcps/cli_tag_actions.py` - Enhanced CLI with environment parameter

### Database
- Migration: `web_ui/migrations/0015_add_glossary_nodes_data_products_to_mutations.py` 