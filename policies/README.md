# DataHub Policies

This directory contains example policy definitions and documentation for DataHub's policy management features.

## Contents

### Documentation

- `policy_documentation.md` - Comprehensive documentation on DataHub policy structure, privileges, and examples

### Example Policy Files

- `example_policy.json` - Basic example of a metadata policy for public datasets
- `complex_policy_example.json` - Example policy with multiple criteria for dashboard access
- `platform_policy_example.json` - Example of a platform-level admin policy
- `comprehensive_policy_example.json` - Detailed example with extensive privileges and filtering

## Usage

These example policy files can be imported into DataHub using the policy management scripts:

```bash
# Import a single policy
python scripts/import_policy.py --input-file policies/example_policy.json

# Import all policies in this directory
python scripts/import_policy.py --input-dir policies/
```

For more details on policy management, see the main documentation in `policy_documentation.md`.

## Best Practices

When adding new policy examples to this directory:

1. Use descriptive filenames that indicate the policy's purpose
2. Include comprehensive comments in the policy definition
3. Test policies in development environments before using in production
4. Consider adding entries to `policy_documentation.md` for any new policy patterns 