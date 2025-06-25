/**
 * Universal Notification Messages for Metadata Manager
 * Standardized notification messages for action buttons across all metadata types
 * 
 * Usage:
 * - Import this file in your metadata manager JavaScript files
 * - Use MetadataNotifications.getMessage(action, type, entityType, options) to get standardized messages
 * - Use MetadataNotifications.show(action, type, entityType, options) to show notifications directly
 */

window.MetadataNotifications = (function() {
    
    // Entity type mappings for proper pluralization and display names
    const ENTITY_TYPES = {
        'tag': { singular: 'tag', plural: 'tags', displayName: 'Tag' },
        'property': { singular: 'property', plural: 'properties', displayName: 'Property' },
        'domain': { singular: 'domain', plural: 'domains', displayName: 'Domain' },
        'glossary': { singular: 'glossary item', plural: 'glossary items', displayName: 'Glossary Item' },
        'term': { singular: 'term', plural: 'terms', displayName: 'Term' },
        'node': { singular: 'node', plural: 'nodes', displayName: 'Node' },
        'data_product': { singular: 'data product', plural: 'data products', displayName: 'Data Product' },
        'data_contract': { singular: 'data contract', plural: 'data contracts', displayName: 'Data Contract' },
        'assertion': { singular: 'assertion', plural: 'assertions', displayName: 'Assertion' },
        'test': { singular: 'test', plural: 'tests', displayName: 'Test' },
        'entity': { singular: 'entity', plural: 'entities', displayName: 'Entity' }
    };

    // Standard notification messages organized by action type
    const MESSAGES = {
        // Selection validation messages
        selection: {
            none_selected: (entityType) => `Please select ${ENTITY_TYPES[entityType]?.plural || 'items'} to proceed.`,
            invalid_selection: (entityType, count) => `${count} selected ${ENTITY_TYPES[entityType]?.plural || 'items'} are invalid or from stale data. Please refresh the page and try again.`,
            no_valid_ids: (entityType) => `No valid ${ENTITY_TYPES[entityType]?.singular || 'item'} IDs found for this operation.`
        },

        // Sync operations
        sync: {
            // Sync to DataHub
            sync_to_datahub_start: (entityType, name) => name ? 
                `Syncing ${ENTITY_TYPES[entityType]?.singular || 'item'} "${name}" to DataHub...` : 
                `Starting sync to DataHub...`,
            sync_to_datahub_bulk_start: (entityType, count) => 
                `Starting sync of ${count} ${count === 1 ? ENTITY_TYPES[entityType]?.singular : ENTITY_TYPES[entityType]?.plural || 'items'} to DataHub...`,
            sync_to_datahub_success: (entityType, name) => name ? 
                `${ENTITY_TYPES[entityType]?.displayName || 'Item'} "${name}" successfully synced to DataHub` : 
                `Successfully synced to DataHub`,
            sync_to_datahub_bulk_success: (entityType, successCount, errorCount) => 
                `Completed: ${successCount} ${successCount === 1 ? ENTITY_TYPES[entityType]?.singular : ENTITY_TYPES[entityType]?.plural || 'items'} synced successfully${errorCount > 0 ? `, ${errorCount} failed` : ''}`,
            sync_to_datahub_error: (entityType, error) => 
                `Error syncing ${ENTITY_TYPES[entityType]?.singular || 'item'}: ${error}`,
            sync_to_datahub_missing_id: (entityType) => 
                `Error syncing ${ENTITY_TYPES[entityType]?.singular || 'item'}: Missing database ID`,

            // Sync to Local
            sync_to_local_start: (entityType, name) => name ? 
                `Syncing ${ENTITY_TYPES[entityType]?.singular || 'item'} "${name}" to local database...` : 
                `Starting sync to local database...`,
            sync_to_local_bulk_start: (entityType, count) => 
                `Starting sync of ${count} ${count === 1 ? ENTITY_TYPES[entityType]?.singular : ENTITY_TYPES[entityType]?.plural || 'items'} to local database...`,
            sync_to_local_success: (entityType, name) => name ? 
                `${ENTITY_TYPES[entityType]?.displayName || 'Item'} "${name}" successfully synced to local database` : 
                `Successfully synced to local database`,
            sync_to_local_bulk_success: (entityType, successCount, errorCount) => 
                `Completed: ${successCount} ${successCount === 1 ? ENTITY_TYPES[entityType]?.singular : ENTITY_TYPES[entityType]?.plural || 'items'} synced successfully${errorCount > 0 ? `, ${errorCount} failed` : ''}`,
            sync_to_local_error: (entityType, error) => 
                `Error syncing ${ENTITY_TYPES[entityType]?.singular || 'item'} to local: ${error}`,

            // Pull from DataHub
            pull_success: (entityType, name) => name ? 
                `${ENTITY_TYPES[entityType]?.displayName || 'Item'} "${name}" successfully pulled from DataHub` : 
                `Successfully pulled from DataHub`,
            pull_error: (entityType, error) => 
                `Error pulling ${ENTITY_TYPES[entityType]?.singular || 'item'} from DataHub: ${error}`,

            // Resync operations
            resync_start: (entityType, name) => name ? 
                `Resyncing ${ENTITY_TYPES[entityType]?.singular || 'item'} "${name}" from DataHub...` : 
                `Starting resync from DataHub...`,
            resync_bulk_start: (entityType, count) => 
                `Starting resync of ${count} ${count === 1 ? ENTITY_TYPES[entityType]?.singular : ENTITY_TYPES[entityType]?.plural || 'items'} from DataHub...`,
            resync_all_start: (entityType) => 
                `Starting resync of all ${ENTITY_TYPES[entityType]?.plural || 'items'} from DataHub...`,
            resync_success: (entityType, name) => name ? 
                `${ENTITY_TYPES[entityType]?.displayName || 'Item'} "${name}" successfully resynced from DataHub` : 
                `Successfully resynced from DataHub`,
            resync_bulk_success: (entityType, successCount, errorCount) => 
                `Completed: ${successCount} ${successCount === 1 ? ENTITY_TYPES[entityType]?.singular : ENTITY_TYPES[entityType]?.plural || 'items'} resynced successfully${errorCount > 0 ? `, ${errorCount} failed` : ''}`,
            resync_error: (entityType, error) => 
                `Error resyncing ${ENTITY_TYPES[entityType]?.singular || 'item'}: ${error}`,
            resync_missing_id: (entityType) => 
                `Error resyncing ${ENTITY_TYPES[entityType]?.singular || 'item'}: Missing database ID`
        },

        // Push operations
        push: {
            push_start: (entityType, name) => name ? 
                `Pushing ${ENTITY_TYPES[entityType]?.singular || 'item'} "${name}" changes to DataHub...` : 
                `Starting push to DataHub...`,
            push_bulk_start: (entityType, count) => 
                `Starting push of ${count} ${count === 1 ? ENTITY_TYPES[entityType]?.singular : ENTITY_TYPES[entityType]?.plural || 'items'} to DataHub...`,
            push_success: (entityType, name) => name ? 
                `${ENTITY_TYPES[entityType]?.displayName || 'Item'} "${name}" changes successfully pushed to DataHub` : 
                `Successfully pushed to DataHub`,
            push_bulk_success: (entityType, successCount, errorCount) => 
                `Completed: ${successCount} ${successCount === 1 ? ENTITY_TYPES[entityType]?.singular : ENTITY_TYPES[entityType]?.plural || 'items'} pushed successfully${errorCount > 0 ? `, ${errorCount} failed` : ''}`,
            push_error: (entityType, error) => 
                `Error pushing ${ENTITY_TYPES[entityType]?.singular || 'item'}: ${error}`,
            push_missing_id: (entityType) => 
                `Error pushing ${ENTITY_TYPES[entityType]?.singular || 'item'}: Missing database ID`
        },

        // Delete operations
        delete: {
            delete_local_start: (entityType, name) => name ? 
                `Deleting local ${ENTITY_TYPES[entityType]?.singular || 'item'} "${name}"...` : 
                `Starting deletion of local ${ENTITY_TYPES[entityType]?.singular || 'item'}...`,
            delete_local_bulk_start: (entityType, count) => 
                `Starting deletion of ${count} local ${count === 1 ? ENTITY_TYPES[entityType]?.singular : ENTITY_TYPES[entityType]?.plural || 'items'}...`,
            delete_local_success: (entityType, name) => name ? 
                `Local ${ENTITY_TYPES[entityType]?.singular || 'item'} "${name}" successfully deleted` : 
                `Local ${ENTITY_TYPES[entityType]?.singular || 'item'} successfully deleted`,
            delete_local_bulk_success: (entityType, successCount, errorCount) => 
                `Completed: ${successCount} ${successCount === 1 ? ENTITY_TYPES[entityType]?.singular : ENTITY_TYPES[entityType]?.plural || 'items'} deleted${errorCount > 0 ? `, ${errorCount} failed` : ''}`,
            delete_local_error: (entityType, error) => 
                `Error deleting local ${ENTITY_TYPES[entityType]?.singular || 'item'}: ${error}`,

            delete_remote_start: (entityType, name) => name ? 
                `Deleting ${ENTITY_TYPES[entityType]?.singular || 'item'} "${name}" from DataHub...` : 
                `Starting deletion from DataHub...`,
            delete_remote_success: (entityType, name) => name ? 
                `${ENTITY_TYPES[entityType]?.displayName || 'Item'} "${name}" successfully deleted from DataHub` : 
                `Successfully deleted from DataHub`,
            delete_remote_error: (entityType, error) => 
                `Error deleting ${ENTITY_TYPES[entityType]?.singular || 'item'} from DataHub: ${error}`,
            delete_remote_missing_urn: (entityType) => 
                `Error deleting ${ENTITY_TYPES[entityType]?.singular || 'item'}: Missing URN`
        },

        // Staged changes operations
        staged_changes: {
            add_to_staged_start: (entityType, name) => name ? 
                `Adding ${ENTITY_TYPES[entityType]?.singular || 'item'} "${name}" to staged changes...` : 
                `Adding to staged changes...`,
            add_to_staged_bulk_start: (entityType, count) => 
                `Starting to add ${count} ${count === 1 ? ENTITY_TYPES[entityType]?.singular : ENTITY_TYPES[entityType]?.plural || 'items'} to staged changes...`,
            add_to_staged_all_start: (entityType) => 
                `Starting to add all ${ENTITY_TYPES[entityType]?.plural || 'items'} to staged changes...`,
            add_to_staged_success: (entityType, name, files) => {
                const baseMessage = name ? 
                    `${ENTITY_TYPES[entityType]?.displayName || 'Item'} "${name}" successfully added to staged changes` : 
                    `Successfully added to staged changes`;
                return files ? `${baseMessage}: ${files.join(', ')}` : baseMessage;
            },
            add_to_staged_bulk_success: (entityType, successCount, errorCount) => 
                `Completed: ${successCount} ${successCount === 1 ? ENTITY_TYPES[entityType]?.singular : ENTITY_TYPES[entityType]?.plural || 'items'} added to staged changes${errorCount > 0 ? `, ${errorCount} failed` : ''}`,
            add_to_staged_error: (entityType, error) => 
                `Error adding ${ENTITY_TYPES[entityType]?.singular || 'item'} to staged changes: ${error}`,
            add_to_staged_missing_id: (entityType) => 
                `Error adding ${ENTITY_TYPES[entityType]?.singular || 'item'} to staged changes: Missing database ID`,
            add_to_staged_failed_all: (entityType, errorCount) => 
                `Failed to add any ${ENTITY_TYPES[entityType]?.plural || 'items'} to staged changes. ${errorCount} errors occurred.`
        },

        // Export/Download operations
        export: {
            export_start: (entityType, count) => count ? 
                `Starting export of ${count} ${count === 1 ? ENTITY_TYPES[entityType]?.singular : ENTITY_TYPES[entityType]?.plural || 'items'}...` : 
                `Starting export of all ${ENTITY_TYPES[entityType]?.plural || 'items'}...`,
            export_success: (entityType, count) => count ? 
                `${count} ${count === 1 ? ENTITY_TYPES[entityType]?.singular : ENTITY_TYPES[entityType]?.plural || 'items'} exported successfully` : 
                `All ${ENTITY_TYPES[entityType]?.plural || 'items'} exported successfully`,
            export_error: (entityType, error) => 
                `Error exporting ${ENTITY_TYPES[entityType]?.plural || 'items'}: ${error}`,
            export_none_selected: (entityType) => 
                `Please select ${ENTITY_TYPES[entityType]?.plural || 'items'} to download.`
        },

        // Import operations
        import: {
            import_start: (entityType) => 
                `Starting import of ${ENTITY_TYPES[entityType]?.plural || 'items'}...`,
            import_success: (entityType, count) => count ? 
                `Successfully imported ${count} ${count === 1 ? ENTITY_TYPES[entityType]?.singular : ENTITY_TYPES[entityType]?.plural || 'items'}` : 
                `${ENTITY_TYPES[entityType]?.displayName || 'Items'} imported successfully`,
            import_error: (entityType, error) => 
                `Error importing ${ENTITY_TYPES[entityType]?.plural || 'items'}: ${error}`
        },

        // Create/Update operations
        crud: {
            create_success: (entityType, name) => name ? 
                `${ENTITY_TYPES[entityType]?.displayName || 'Item'} "${name}" created successfully` : 
                `${ENTITY_TYPES[entityType]?.displayName || 'Item'} created successfully`,
            create_error: (entityType, error) => 
                `Error creating ${ENTITY_TYPES[entityType]?.singular || 'item'}: ${error}`,
            update_success: (entityType, name) => name ? 
                `${ENTITY_TYPES[entityType]?.displayName || 'Item'} "${name}" updated successfully` : 
                `${ENTITY_TYPES[entityType]?.displayName || 'Item'} updated successfully`,
            update_error: (entityType, error) => 
                `Error updating ${ENTITY_TYPES[entityType]?.singular || 'item'}: ${error}`,
            edit_missing_id: (entityType) => 
                `Error editing ${ENTITY_TYPES[entityType]?.singular || 'item'}: Missing database ID`
        },

        // General operations
        general: {
            progress: (processed, total) => 
                `Progress: ${processed}/${total} items processed`,
            copy_success: () => 
                `URN copied to clipboard`,
            copy_error: () => 
                `Failed to copy URN to clipboard`,
            missing_connection: () => 
                `Not connected to DataHub. Please check your connection settings.`,
            refresh_required: () => 
                `Please refresh the page to see the latest changes.`
        }
    };

    /**
     * Get a standardized notification message
     * @param {string} action - The action type (sync, push, delete, etc.)
     * @param {string} type - The message type (start, success, error, etc.)
     * @param {string} entityType - The entity type (tag, property, domain, etc.)
     * @param {Object} options - Additional options for message formatting
     * @returns {string} The formatted message
     */
    function getMessage(action, type, entityType, options = {}) {
        const messageCategory = MESSAGES[action];
        if (!messageCategory) {
            console.warn(`Unknown action type: ${action}`);
            return `${action} ${type}`;
        }

        const messageTemplate = messageCategory[type];
        if (!messageTemplate) {
            console.warn(`Unknown message type: ${type} for action: ${action}`);
            return `${action} ${type}`;
        }

        // Call the message template function with the provided options
        if (typeof messageTemplate === 'function') {
            return messageTemplate(entityType, options.name, options.count, options.successCount, options.errorCount, options.files, options.error);
        }

        return messageTemplate;
    }

    /**
     * Show a notification using the global notification system
     * @param {string} action - The action type (sync, push, delete, etc.)
     * @param {string} type - The message type (start, success, error, etc.)
     * @param {string} entityType - The entity type (tag, property, domain, etc.)
     * @param {Object} options - Additional options for message formatting
     * @param {string} notificationType - The notification type (success, error, info, warning)
     */
    function show(action, type, entityType, options = {}, notificationType = null) {
        const message = getMessage(action, type, entityType, options);
        
        // Determine notification type if not provided
        if (!notificationType) {
            if (type.includes('error') || type.includes('missing')) {
                notificationType = 'error';
            } else if (type.includes('success')) {
                notificationType = 'success';
            } else if (type.includes('start') || type.includes('progress')) {
                notificationType = 'info';
            } else {
                notificationType = 'info';
            }
        }

        // Use global notification system
        if (typeof showToast === 'function') {
            showToast(notificationType, message);
        } else if (typeof showNotification === 'function') {
            showNotification(notificationType, message);
        } else {
            console.warn('No global notification system available. Message:', message);
        }
    }

    /**
     * Show a progress notification for bulk operations
     * @param {number} processed - Number of items processed
     * @param {number} total - Total number of items
     */
    function showProgress(processed, total) {
        show('general', 'progress', 'entity', { processed, total }, 'info');
    }

    /**
     * Show a clipboard copy notification
     * @param {boolean} success - Whether the copy was successful
     */
    function showCopyResult(success) {
        if (success) {
            show('general', 'copy_success', 'entity', {}, 'success');
        } else {
            show('general', 'copy_error', 'entity', {}, 'error');
        }
    }

    // Public API
    return {
        getMessage: getMessage,
        show: show,
        showProgress: showProgress,
        showCopyResult: showCopyResult,
        ENTITY_TYPES: ENTITY_TYPES
    };
})(); 