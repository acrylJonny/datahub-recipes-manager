function propertyActionsGetDatabaseId(propertyData) {
    if (propertyData.database_id) {
        return propertyData.database_id;
    }
    
    if (propertyData.local) {
        if (propertyData.local.database_id) {
            return propertyData.local.database_id;
        }
        if (propertyData.local.id) {
            return propertyData.local.id;
        }
    }
    
    if (propertyData.id) {
        return propertyData.id;
    }
    
    console.warn('Could not find database ID for property:', propertyData);
    return null;
}

function propertyActionsDownloadJson(property) {
    const propertyData = property.combined || property;
    const jsonData = JSON.stringify(propertyData, null, 2);
    const blob = new Blob([jsonData], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    
    const link = document.createElement('a');
    link.href = url;
    link.download = `property-${propertyData.name || 'data'}.json`;
    document.body.appendChild(link);
    link.click();
    
    setTimeout(() => {
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }, 100);
}

function propertyActionsSyncToLocal(property) {
    propertyActionsShowActionLoading('sync');
    
    const propertyData = property.combined || property;
    const propertyId = propertyActionsGetDatabaseId(propertyData);
    
    if (propertyData.sync_status === 'REMOTE_ONLY') {
        
        // For remote-only properties, we need to create them locally first
        // This is done by pulling the property from the remote server
        fetch('/metadata/properties/pull/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': propertyActionsGetCsrfToken()
            },
            body: JSON.stringify({
                urns: [propertyData.urn],  // Send as array for specific property(s)
                pull_specific: true,
                debug_info: true  // Add debugging info
            })
        })
        .then(response => {
            // Log detailed response information
            console.log('Pull response status:', response.status);
            console.log('Pull response headers:', Array.from(response.headers.entries()));
            
            // Check for non-JSON responses that might be returning HTML error pages
            const contentType = response.headers.get('content-type');
            console.log('Pull response content type:', contentType);
            
            if (contentType && contentType.indexOf('application/json') !== -1) {
                // This is a JSON response, proceed normally
                if (!response.ok) {
                    return response.json().then(data => {
                        throw new Error(data.error || 'Failed to pull property from DataHub');
                    });
                }
                return response.json();
                          } else {
                  // This is likely an HTML error page or unexpected response
                  console.error('Received non-JSON response from pull API:', contentType);
                  return response.text().then(text => {
                      throw new Error('Server returned an unexpected response format. Check server logs.');
                  });
              }
        })
                  .then(data => {
              if (data.success) {
                MetadataNotifications.show('sync', 'pull_success', 'property', { name: propertyData.name });
                // Refresh the data to show the newly created property
                if (typeof loadPropertiesData === 'function') {
                    loadPropertiesData();
                } else {
                    // Fallback to page reload if loadPropertiesData is not available
                    window.location.reload();
                }
            } else {
                throw new Error(data.error || 'Failed to pull property');
            }
        })
        .catch(error => {
            console.error('Error pulling remote property:', error);
            MetadataNotifications.show('sync', 'pull_error', 'property', { error: error.message });
        })
        .finally(() => {
            propertyActionsHideActionLoading('sync');
        });
        
        return;
    }
    
    // Handle missing ID case
    if (!propertyId) {
        console.error('Cannot sync property without a database ID:', propertyData);
        MetadataNotifications.show('sync', 'sync_to_local_missing_id', 'property');
        propertyActionsHideActionLoading('sync');
        return;
    }
    
    // Format the property ID as a UUID with dashes if needed
    const formattedPropertyId = propertyActionsFormatPropertyId(propertyId);
    
    // Make API call to sync property to local
    fetch(`/metadata/api/properties/${formattedPropertyId}/sync_to_local/`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': propertyActionsGetCsrfToken()
        },
        // Add debugging info to the request body
        body: JSON.stringify({
            debug_info: true
        })
    })
    .then(response => {
        // Check for non-JSON responses that might be returning HTML error pages
        const contentType = response.headers.get('content-type');
        
        if (contentType && contentType.indexOf('application/json') !== -1) {
            // This is a JSON response, proceed normally
            if (!response.ok) {
                return response.json().then(data => {
                    throw new Error(data.error || 'Failed to sync property to local database');
                });
            }
            return response.json();
        } else {
            // This is likely an HTML error page or unexpected response
            console.error('Received non-JSON response from sync API:', contentType);
            return response.text().then(text => {
                throw new Error('Server returned an unexpected response format. Check server logs.');
            });
        }
    })
    .then(data => {
        if (data.success) {
            MetadataNotifications.show('sync', 'sync_to_local_success', 'property', { name: propertyData.name });
            // Refresh the data to show the updated property
            if (typeof loadPropertiesData === 'function') {
                loadPropertiesData();
            } else {
                // Fallback to page reload if loadPropertiesData is not available
                window.location.reload();
            }
        } else {
            throw new Error(data.error || 'Failed to sync property');
        }
    })
    .catch(error => {
        console.error('Error syncing property:', error);
        MetadataNotifications.show('sync', 'sync_to_local_error', 'property', { error: error.message });
    })
    .finally(() => {
        propertyActionsHideActionLoading('sync');
    });
}

/**
 * Format property ID as UUID with dashes
 * @param {string} id - The property ID
 * @returns {string} - Formatted UUID
 */
function propertyActionsFormatPropertyId(id) {
    if (!id) return '';
    
    // Remove any existing dashes and convert to string
    const cleanId = id.toString().replace(/-/g, '');
    
    // If it's already a valid UUID format, return as is
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)) {
        return id;
    }
    
    // If it's 32 characters (UUID without dashes), add dashes
    if (cleanId.length === 32) {
        return `${cleanId.slice(0, 8)}-${cleanId.slice(8, 12)}-${cleanId.slice(12, 16)}-${cleanId.slice(16, 20)}-${cleanId.slice(20, 32)}`;
    }
    
    // Return as is if it doesn't match UUID patterns
    return id;
}

/**
 * Add property to staged changes (MCP)
 * @param {Object} property - The property object
 */
function propertyActionsAddToStagedChanges(property) {
    
    // Show loading spinner
    propertyActionsShowActionLoading('staged');
    
    // Get property data
    const propertyData = property.combined || property;
    const propertyId = propertyActionsGetDatabaseId(propertyData);
    
    // Handle missing ID case
    if (!propertyId) {
        console.error('Cannot add property to staged changes without a database ID:', propertyData);
        MetadataNotifications.show('staged_changes', 'add_to_staged_missing_id', 'property');
        propertyActionsHideActionLoading('staged');
        return;
    }
    
    // Debug info
    console.log('Attempting to add property to staged changes with database ID:', propertyId);
    
    // Format the property ID as a UUID with dashes if needed
    const formattedPropertyId = propertyActionsFormatPropertyId(propertyId);
    console.log(`Formatted property ID for staged changes: ${formattedPropertyId}`);
    
    // Make API call to add property to staged changes
    fetch(`/metadata/properties/${formattedPropertyId}/add_to_staged_changes/`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': propertyActionsGetCsrfToken()
        },
        body: JSON.stringify({
            debug_info: true
        })
    })
    .then(response => {
        console.log('Staged changes response status:', response.status);
        console.log('Staged changes response headers:', Array.from(response.headers.entries()));
        
        // Check for non-JSON responses that might be returning HTML error pages
        const contentType = response.headers.get('content-type');
        console.log('Response content type:', contentType);
        
        if (contentType && contentType.indexOf('application/json') !== -1) {
            // This is a JSON response, proceed normally
            if (!response.ok) {
                return response.json().then(data => {
                    throw new Error(data.error || 'Failed to add property to staged changes');
                });
            }
            return response.json();
        } else {
            // This is likely an HTML error page or unexpected response
            console.error('Received non-JSON response:', contentType);
            return response.text().then(text => {
                console.error('Staged changes response content (first 500 chars):', text.substring(0, 500) + '...');
                throw new Error('Server returned an unexpected response format. See console for details.');
            });
        }
    })
    .then(data => {
        console.log('Staged changes success response data:', data);
        if (data.success) {
            MetadataNotifications.show('staged_changes', 'add_to_staged_success', 'property', { name: propertyData.name });
            // Optionally refresh the data to show updated status
            if (typeof loadPropertiesData === 'function') {
                loadPropertiesData();
            }
        } else {
            throw new Error(data.error || 'Failed to add property to staged changes');
        }
    })
    .catch(error => {
        console.error('Error adding property to staged changes:', error);
        MetadataNotifications.show('staged_changes', 'add_to_staged_error', 'property', { error: error.message });
    })
    .finally(() => {
        propertyActionsHideActionLoading('staged');
    });
}

/**
 * Get CSRF token from the page
 * @returns {string} - CSRF token
 */
function propertyActionsGetCsrfToken() {
    const token = document.querySelector('[name=csrfmiddlewaretoken]');
    return token ? token.value : '';
}

/**
 * Show loading spinner for specific action
 * @param {string} actionType - Type of action (sync, staged, etc.)
 */
function propertyActionsShowActionLoading(actionType) {
    // Find the button that triggered this action and show loading state
    const buttons = document.querySelectorAll(`[onclick*="${actionType}"]`);
    buttons.forEach(button => {
        const originalText = button.innerHTML;
        button.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i> Loading...';
        button.disabled = true;
        button.dataset.originalText = originalText;
    });
}

/**
 * Hide loading spinner for specific action
 * @param {string} actionType - Type of action (sync, staged, etc.)
 */
function propertyActionsHideActionLoading(actionType) {
    // Find the button that triggered this action and restore original state
    const buttons = document.querySelectorAll(`[onclick*="${actionType}"]`);
    buttons.forEach(button => {
        if (button.dataset.originalText) {
            button.innerHTML = button.dataset.originalText;
            button.disabled = false;
            delete button.dataset.originalText;
        }
    });
}

// Note: Duplicate notification function removed - using MetadataNotifications.show() instead
// This ensures consistent, standardized notification messages across all metadata types

/**
 * Show notification message
 * @param {string} type - Type of notification (success, error, warning, info)
 * @param {string} message - Message to display
 */
function propertyActionsShowNotification(type, message) {
    // Use global notification system
    if (typeof showToast === 'function') {
        showToast(type, message);
    } else {
        console.log(`${type.toUpperCase()}: ${message}`);
    }
}

/**
 * Sync property to DataHub
 * @param {Object} property - The property object
 */
function propertyActionsSyncToDataHub(property) {
    console.log('propertyActionsSyncToDataHub called with:', property);
    
    // Show loading spinner
    propertyActionsShowActionLoading('push');
    
    // Get property data
    const propertyData = property.combined || property;
    const propertyId = propertyActionsGetDatabaseId(propertyData);
    
    // Handle missing ID case
    if (!propertyId) {
        console.error('Cannot sync property to DataHub without a database ID:', propertyData);
        MetadataNotifications.show('sync', 'sync_to_datahub_missing_id', 'property');
        propertyActionsHideActionLoading('push');
        return;
    }
    
    // Debug info
    console.log('Attempting to sync property to DataHub with database ID:', propertyId);
    
    // Format the property ID as a UUID with dashes if needed
    const formattedPropertyId = propertyActionsFormatPropertyId(propertyId);
    console.log(`Formatted property ID for DataHub sync: ${formattedPropertyId}`);
    
    // Make API call to sync property to DataHub
    fetch(`/metadata/api/properties/${formattedPropertyId}/sync_to_datahub/`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': propertyActionsGetCsrfToken()
        },
        body: JSON.stringify({
            debug_info: true
        })
    })
    .then(response => {
        console.log('DataHub sync response status:', response.status);
        console.log('DataHub sync response headers:', Array.from(response.headers.entries()));
        
        // Check for non-JSON responses that might be returning HTML error pages
        const contentType = response.headers.get('content-type');
        console.log('Response content type:', contentType);
        
        if (contentType && contentType.indexOf('application/json') !== -1) {
            // This is a JSON response, proceed normally
            if (!response.ok) {
                return response.json().then(data => {
                    throw new Error(data.error || 'Failed to sync property to DataHub');
                });
            }
            return response.json();
        } else {
            // This is likely an HTML error page or unexpected response
            console.error('Received non-JSON response:', contentType);
            return response.text().then(text => {
                console.error('DataHub sync response content (first 500 chars):', text.substring(0, 500) + '...');
                throw new Error('Server returned an unexpected response format. See console for details.');
            });
        }
    })
    .then(data => {
        console.log('DataHub sync success response data:', data);
        if (data.success) {
            MetadataNotifications.show('sync', 'sync_to_datahub_success', 'property', { name: propertyData.name });
            // Refresh the data to show the updated property
            if (typeof loadPropertiesData === 'function') {
                loadPropertiesData();
            } else {
                // Fallback to page reload if loadPropertiesData is not available
                window.location.reload();
            }
        } else {
            throw new Error(data.error || 'Failed to sync property to DataHub');
        }
    })
    .catch(error => {
        console.error('Error syncing property to DataHub:', error);
        propertyActionsShowNotification('error', `Error syncing property to DataHub: ${error.message}`);
    })
    .finally(() => {
        propertyActionsHideActionLoading('push');
    });
} 