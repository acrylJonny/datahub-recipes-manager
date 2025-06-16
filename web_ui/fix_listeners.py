import re

# Read the file
with open('templates/metadata_manager/assertions/list.html', 'r') as f:
    content = f.read()

# Remove the second DOMContentLoaded listener (bulk selection)
pattern1 = r'document\.addEventListener\(\'DOMContentLoaded\', function\(\) \{\s*initializeBulkSelection\(\);\s*handleCreateAssertionDropdown\(\);\s*\}\);'
content = re.sub(pattern1, '// Bulk selection and dropdown initialization moved to main DOMContentLoaded listener', content, flags=re.MULTILINE | re.DOTALL)

# Remove the third DOMContentLoaded listener (create assertion form) - more comprehensive pattern
pattern2 = r'\/\/ Create Assertion Form Handler\s*document\.addEventListener\(\'DOMContentLoaded\', function\(\) \{.*?\}\);'
content = re.sub(pattern2, '// Create Assertion Form Handler - moved to main DOMContentLoaded listener', content, flags=re.MULTILINE | re.DOTALL)

# Write back
with open('templates/metadata_manager/assertions/list.html', 'w') as f:
    f.write(content)

print('Fixed duplicate DOMContentLoaded listeners') 