# Universal Notification Messages for Metadata Manager

This document describes how to use the standardized notification messages across all metadata manager pages.

## Overview

The `metadata_notifications.js` file provides a universal notification system that standardizes all notification messages for action buttons across different metadata types (tags, properties, domains, glossary, data products, etc.).

## Usage

### 1. Include the Script

Add the universal notification script to your template before other metadata manager scripts:

```html
<!-- Universal Metadata Notifications -->
<script src="{% static 'metadata_manager/metadata_notifications.js' %}"></script>

<!-- Your specific metadata scripts -->
<script src="{% static 'metadata_manager/your_module/your_script.js' %}"></script>
```

### 2. Basic Usage

Replace hardcoded notification messages with standardized ones:

```javascript
// Old way - hardcoded messages
showNotification('success', 'Tag successfully synced to DataHub');
showNotification('error', 'Error syncing tag: Connection failed');

// New way - universal system
MetadataNotifications.show('sync', 'sync_to_datahub_success', 'tag', { name: 'MyTag' });
MetadataNotifications.show('sync', 'sync_to_datahub_error', 'tag', { error: 'Connection failed' });
```

### 3. Supported Entity Types

- `tag` - Tags
- `property` - Structured Properties  
- `domain` - Domains
- `glossary` - Glossary Items (general)
- `term` - Glossary Terms
- `node` - Glossary Nodes
- `data_product` - Data Products
- `data_contract` - Data Contracts
- `assertion` - Assertions
- `test` - Metadata Tests
- `entity` - Generic entities

### 4. Common Action Patterns

#### Selection Validation
```javascript
// Check if items are selected
if (selectedItems.length === 0) {
    MetadataNotifications.show('selection', 'none_selected', 'tag');
    return;
}

// Check for invalid selections
if (invalidCount > 0) {
    MetadataNotifications.show('selection', 'invalid_selection', 'tag', { count: invalidCount });
    return;
}
```

#### Sync Operations
```javascript
// Sync to DataHub
MetadataNotifications.show('sync', 'sync_to_datahub_start', 'tag', { name: tagName });
// On success:
MetadataNotifications.show('sync', 'sync_to_datahub_success', 'tag', { name: tagName });
// On error:
MetadataNotifications.show('sync', 'sync_to_datahub_error', 'tag', { error: errorMessage });

// Bulk sync
MetadataNotifications.show('sync', 'sync_to_datahub_bulk_start', 'tag', { count: selectedTags.length });
// On completion:
MetadataNotifications.show('sync', 'sync_to_datahub_bulk_success', 'tag', { 
    successCount: successCount, 
    errorCount: errorCount 
});
```

#### Delete Operations
```javascript
// Delete local item
MetadataNotifications.show('delete', 'delete_local_start', 'tag', { name: tagName });
MetadataNotifications.show('delete', 'delete_local_success', 'tag', { name: tagName });

// Delete from DataHub
MetadataNotifications.show('delete', 'delete_remote_start', 'tag', { name: tagName });
MetadataNotifications.show('delete', 'delete_remote_success', 'tag', { name: tagName });
```

#### Staged Changes
```javascript
// Add to staged changes
MetadataNotifications.show('staged_changes', 'add_to_staged_start', 'tag', { name: tagName });
MetadataNotifications.show('staged_changes', 'add_to_staged_success', 'tag', { 
    name: tagName, 
    files: ['tag-mytag.json'] 
});

// Bulk add to staged changes
MetadataNotifications.show('staged_changes', 'add_to_staged_bulk_start', 'tag', { count: selectedCount });
MetadataNotifications.show('staged_changes', 'add_to_staged_bulk_success', 'tag', { 
    successCount: successCount, 
    errorCount: errorCount 
});
```

#### Export/Import
```javascript
// Export
MetadataNotifications.show('export', 'export_start', 'tag', { count: selectedTags.length });
MetadataNotifications.show('export', 'export_success', 'tag', { count: selectedTags.length });

// Import
MetadataNotifications.show('import', 'import_start', 'tag');
MetadataNotifications.show('import', 'import_success', 'tag', { count: importedCount });
```

#### CRUD Operations
```javascript
// Create
MetadataNotifications.show('crud', 'create_success', 'tag', { name: newTagName });
MetadataNotifications.show('crud', 'create_error', 'tag', { error: errorMessage });

// Update
MetadataNotifications.show('crud', 'update_success', 'tag', { name: tagName });
MetadataNotifications.show('crud', 'update_error', 'tag', { error: errorMessage });
```

### 5. Special Utility Functions

#### Progress Notifications
```javascript
// Show progress for bulk operations
MetadataNotifications.showProgress(processedCount, totalCount);
```

#### Clipboard Operations
```javascript
// Copy to clipboard
navigator.clipboard.writeText(urn).then(() => {
    MetadataNotifications.showCopyResult(true);
}).catch(() => {
    MetadataNotifications.showCopyResult(false);
});
```

### 6. Fallback Support

The system includes fallback support for existing notification functions:

```javascript
// This will work even if MetadataNotifications is not loaded
if (typeof MetadataNotifications !== 'undefined') {
    MetadataNotifications.show('sync', 'sync_success', 'tag', { name: tagName });
} else {
    showNotification('success', `Tag "${tagName}" synced successfully`);
}
```

### 7. Migration Guide

To migrate existing code:

1. **Include the script** in your template
2. **Replace hardcoded messages** with universal system calls
3. **Use entity-specific types** instead of generic "item"
4. **Standardize message patterns** across similar operations
5. **Add fallback support** for backward compatibility

### 8. Benefits

- **Consistency**: All notification messages follow the same patterns
- **Maintainability**: Single place to update message formats
- **Localization Ready**: Easy to add multi-language support
- **Type Safety**: Structured approach reduces typos and inconsistencies
- **Extensibility**: Easy to add new entity types and actions

## Examples from Tags Implementation

The tags page has been updated to demonstrate the universal system:

```javascript
// Selection validation
if (selectedTags.length === 0) {
    MetadataNotifications.show('selection', 'none_selected', 'tag');
    return;
}

// Bulk resync
MetadataNotifications.show('sync', 'resync_bulk_start', 'tag', { count: selectedTags.length });

// Pull from DataHub
MetadataNotifications.show('sync', 'pull_success', 'tag', { name: tagData.name });

// Clipboard copy
MetadataNotifications.showCopyResult(true);
```

This ensures all tags-related notifications use consistent, professional language and can be easily updated globally if needed. 