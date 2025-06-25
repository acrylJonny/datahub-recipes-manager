/**
 * Tag Actions - Additional functionality for tag management
 * Contains functions for downloading tag JSON, syncing to local, and adding to staged changes
 */

/**
 * Get the actual database ID from a tag object
 * This is needed because the tag data might contain datahub_id instead of the actual database id
 * @param {Object} tagData - The tag data object
 * @returns {string|null} - The database ID or null if not found
 */
function getDatabaseId(tagData) {
    // First, check if we have an explicit database_id field
    if (tagData.database_id) {
        return tagData.database_id;
    }
    
    // For combined objects (synced tags), check if local has database_id or id
    if (tagData.local) {
        if (tagData.local.database_id) {
            return tagData.local.database_id;
        }
        if (tagData.local.id) {
            return tagData.local.id;
        }
    }
    
    // For remote-only tags or as a fallback, use the id property
    if (tagData.id) {
        return tagData.id;
    }
    
    // Log warning if we couldn't find an ID
    console.warn('Could not find database ID for tag:', tagData);
    return null;
}

/**
 * Download tag JSON data
 * @param {Object} tag - The tag object
 */
function downloadTagJson(tag) {
    console.log('downloadTagJson called with:', tag);
    
    // Get raw tag data
    const tagData = tag.combined || tag;
    
    // Convert to pretty JSON
    const jsonData = JSON.stringify(tagData, null, 2);
    
    // Create a blob and initiate download
    const blob = new Blob([jsonData], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    
    // Create temporary link and trigger download
    const link = document.createElement('a');
    link.href = url;
    link.download = `tag-${tagData.name || 'data'}.json`;
    document.body.appendChild(link);
    link.click();
    
    // Clean up
    setTimeout(() => {
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }, 100);
}

/**
 * Sync tag to local database
 * @param {Object} tag - The tag object
 */
function syncTagToLocal(tag) {
    console.log('syncTagToLocal called with:', tag);
    
    // Show loading spinner
    showActionLoading('sync');
    
    // Get tag data
    const tagData = tag.combined || tag;
    // IMPORTANT: We must use the UUID (id) for the API endpoint, not the URN
    const tagId = getDatabaseId(tagData);
    
    // Special handling for remote-only tags
    if (tagData.sync_status === 'REMOTE_ONLY') {
        console.log('Remote-only tag detected:', tagData);
        console.log('Tag URN:', tagData.urn);
        
        // For remote-only tags, we need to create them locally first
        // This is done by pulling the tag from the remote server
        fetch('/metadata/tags/pull/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCsrfToken()
            },
            body: JSON.stringify({
                urns: [tagData.urn],  // Send as array for specific tag(s)
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
                        throw new Error(data.error || 'Failed to pull tag from DataHub');
                    });
                }
                return response.json();
            } else {
                // This is likely an HTML error page or unexpected response
                console.error('Received non-JSON response:', contentType);
                return response.text().then(text => {
                    console.error('Pull response content (first 500 chars):', text.substring(0, 500) + '...');
                    throw new Error('Server returned an unexpected response format. See console for details.');
                });
            }
        })
        .then(data => {
            console.log('Pull success response data:', data);
            if (data.success) {
                showNotification('success', 'Tag successfully pulled from DataHub');
                // Refresh the data to show the newly created tag
                if (typeof loadTagsData === 'function') {
                    loadTagsData();
                } else {
                    // Fallback to page reload if loadTagsData is not available
                    window.location.reload();
                }
            } else {
                throw new Error(data.error || 'Failed to pull tag');
            }
        })
        .catch(error => {
            console.error('Error pulling remote tag:', error);
            showNotification('error', `Error syncing tag: ${error.message}`);
        })
        .finally(() => {
            hideActionLoading('sync');
        });
        
        return;
    }
    
    // Handle missing ID case
    if (!tagId) {
        console.error('Cannot sync tag without a database ID:', tagData);
        showNotification('error', 'Error syncing tag: Missing tag database ID.');
        hideActionLoading('sync');
        return;
    }
    
    // Debug info
    console.log('Attempting to sync tag with database ID:', tagId);
    
    // Format the tag ID as a UUID with dashes if needed
    const formattedTagId = formatTagId(tagId);
    console.log(`Formatted tag ID for sync: ${formattedTagId}`);
    
    // Make API call to sync tag to local
    // Use the API endpoint that's decorated with csrf_exempt
    fetch(`/metadata/api/tags/${formattedTagId}/sync_to_local/`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCsrfToken()
        },
        // Add debugging info to the request body
        body: JSON.stringify({
            debug_info: true
        })
    })
    .then(response => {
        console.log('Sync response status:', response.status);
        console.log('Sync response headers:', Array.from(response.headers.entries()));
        
        // Check for non-JSON responses that might be returning HTML error pages
        const contentType = response.headers.get('content-type');
        console.log('Response content type:', contentType);
        
        if (contentType && contentType.indexOf('application/json') !== -1) {
            // This is a JSON response, proceed normally
            if (!response.ok) {
                return response.json().then(data => {
                    throw new Error(data.error || 'Failed to sync tag to local database');
                });
            }
            return response.json();
        } else {
            // This is likely an HTML error page or unexpected response
            console.error('Received non-JSON response:', contentType);
            return response.text().then(text => {
                console.error('Response content (first 500 chars):', text.substring(0, 500) + '...');
                throw new Error('Server returned an unexpected response format. See console for details.');
            });
        }
    })
    .then(data => {
        // Show success notification
        console.log('Sync success response data:', data);
        showNotification('success', 'Tag successfully synced to local database');
        
        // Refresh tag list if needed
        if (data.refresh_needed) {
            loadTagsData();
        }
    })
    .catch(error => {
        console.error('Error syncing tag to local:', error);
        showNotification('error', `Error syncing tag: ${error.message}`);
    })
    .finally(() => {
        hideActionLoading('sync');
    });
}

/**
 * Format a tag ID as a UUID with dashes if needed
 * @param {string} id - The tag ID to format
 * @returns {string} - The formatted UUID
 */
function formatTagId(id) {
    if (!id) return id;
    
    // If the ID already has dashes, return it as is
    if (id.includes('-')) return id;
    
    // If it's a 32-character hex string, format it as UUID
    if (id.length === 32 && /^[0-9a-f]+$/i.test(id)) {
        return `${id.substring(0, 8)}-${id.substring(8, 12)}-${id.substring(12, 16)}-${id.substring(16, 20)}-${id.substring(20)}`;
    }
    
    // Otherwise return as is
    return id;
}

/**
 * Add tag to staged changes
 * @param {Object} tag - The tag object
 */
function addTagToStagedChanges(tag) {
    console.log('addTagToStagedChanges called with:', tag);
    
    // Show loading spinner
    showActionLoading('stage');
    
    // Get tag data and environment
    const tagData = tag.combined || tag;
    const tagName = tagData.name || 'Unknown Tag';
    
    // Check if this is a remote-only tag that needs to be staged directly
    if (tagData.sync_status === 'REMOTE_ONLY' || !getDatabaseId(tagData)) {
        console.log(`Tag "${tagName}" is remote-only, staging directly...`);
        
        // Show loading notification
        showNotification('info', `Adding remote tag "${tagName}" to staged changes...`);
        
        // Get current environment and mutation from global state or settings
        const currentEnvironment = window.currentEnvironment || { name: 'dev' };
        const mutationName = currentEnvironment.mutation_name || null;
        
        // Use the remote staging endpoint
        fetch('/metadata/tags/remote/stage_changes/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCsrfToken()
            },
            body: JSON.stringify({
                tag_data: tagData,
                environment: currentEnvironment.name,
                mutation_name: mutationName
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
                        throw new Error(data.error || 'Failed to add remote tag to staged changes');
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
            console.log('Remote staged changes success response data:', data);
            if (data.status === 'success') {
                showNotification('success', data.message || `Remote tag added to staged changes successfully`);
                if (data.files_created && data.files_created.length > 0) {
                    console.log('Created files:', data.files_created);
                }
            } else {
                throw new Error(data.error || 'Failed to add remote tag to staged changes');
            }
        })
        .catch(error => {
            console.error('Error adding remote tag to staged changes:', error);
            showNotification('error', `Error adding remote tag to staged changes: ${error.message}`);
        })
        .finally(() => {
            hideActionLoading('stage');
        });
        return;
    }
    
    // For local/synced tags, use the regular staging endpoint
    const tagId = getDatabaseId(tagData);
    
    if (!tagId) {
        console.error('Cannot add tag to staged changes without a database ID:', tagData);
        showNotification('error', 'Error adding tag to staged changes: Missing tag database ID.');
        hideActionLoading('stage');
        return;
    }
    
    console.log('Using database ID for staged changes:', tagId);
    
    // Format the tag ID as a UUID with dashes if needed
    const formattedTagId = formatTagId(tagId);
    console.log(`Formatted tag ID for staged changes: ${formattedTagId}`);
    
    // Get current environment and mutation from global state or settings
    const currentEnvironment = window.currentEnvironment || { name: 'dev' };
    const mutationName = currentEnvironment.mutation_name || null;
    
    console.log('Making API call to:', `/metadata/api/tags/${formattedTagId}/stage_changes/`);
    
    // Make API call to create MCP files
    fetch(`/metadata/api/tags/${formattedTagId}/stage_changes/`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCsrfToken()
        },
        body: JSON.stringify({
            environment: currentEnvironment.name,
            mutation_name: mutationName,
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
                    throw new Error(data.error || 'Failed to add tag to staged changes');
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
        if (data.success || data.files_created) {
            // Handle both success formats for backward compatibility
            const message = data.files_created ? 
                `Tag successfully added to staged changes: ${data.files_created.join(', ')}` :
                'Tag successfully added to staged changes';
            showNotification('success', message);
            
            // Optionally refresh the data to show updated status
            if (typeof loadTagsData === 'function') {
                loadTagsData();
            }
        } else {
            throw new Error(data.error || 'Failed to add tag to staged changes');
        }
    })
    .catch(error => {
        console.error('Error adding tag to staged changes:', error);
        showNotification('error', `Error adding tag to staged changes: ${error.message}`);
    })
    .finally(() => {
        hideActionLoading('stage');
    });
}

// Helper functions

/**
 * Get CSRF token from cookies
 */
function getCsrfToken() {
    return document.querySelector('[name=csrfmiddlewaretoken]')?.value || 
           document.cookie.split('; ')
               .find(row => row.startsWith('csrftoken='))
               ?.split('=')[1];
}

/**
 * Show loading state for an action
 */
function showActionLoading(actionType) {
    const buttons = document.querySelectorAll(`[data-action="${actionType}"]`);
    buttons.forEach(button => {
        button.disabled = true;
        button.dataset.originalHtml = button.innerHTML;
        button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>';
    });
}

/**
 * Hide loading state for an action
 */
function hideActionLoading(actionType) {
    const buttons = document.querySelectorAll(`[data-action="${actionType}"]`);
    buttons.forEach(button => {
        button.disabled = false;
        if (button.dataset.originalHtml) {
            button.innerHTML = button.dataset.originalHtml;
            delete button.dataset.originalHtml;
        }
    });
}

/**
 * Show notification toast
 */
function showNotification(type, message) {
    // Check if we have notifications container
    let container = document.getElementById('notifications-container');
    
    // Create it if it doesn't exist
    if (!container) {
        container = document.createElement('div');
        container.id = 'notifications-container';
        container.className = 'position-fixed bottom-0 end-0 p-3';
        container.style.zIndex = '1050';
        document.body.appendChild(container);
    }
    
    // Create unique ID
    const id = 'toast-' + Date.now();
    
    // Create toast HTML
    let bgClass, icon, title;
    
    if (type === 'success') {
        bgClass = 'bg-success';
        icon = 'fa-check-circle';
        title = 'Success';
    } else if (type === 'info') {
        bgClass = 'bg-info';
        icon = 'fa-info-circle';
        title = 'Info';
    } else {
        bgClass = 'bg-danger';
        icon = 'fa-exclamation-circle';
        title = 'Error';
    }
    
    const toast = document.createElement('div');
    toast.className = `toast ${bgClass} text-white`;
    toast.id = id;
    toast.setAttribute('role', 'alert');
    toast.setAttribute('aria-live', 'assertive');
    toast.setAttribute('aria-atomic', 'true');
    toast.innerHTML = `
        <div class="toast-header ${bgClass} text-white">
            <i class="fas ${icon} me-2"></i>
            <strong class="me-auto">${title}</strong>
            <button type="button" class="btn-close btn-close-white" data-bs-dismiss="toast" aria-label="Close"></button>
        </div>
        <div class="toast-body">${message}</div>
    `;
    
    container.appendChild(toast);
    
    // Initialize and show the toast
    const toastInstance = new bootstrap.Toast(toast, {
        delay: 5000
    });
    toastInstance.show();
    
    // Remove toast from DOM after it's hidden
    toast.addEventListener('hidden.bs.toast', function () {
        toast.remove();
    });
}

/**
 * Sync tag to DataHub
 * @param {Object} tag - The tag object
 */
function syncTagToDataHub(tag) {
    console.log('syncTagToDataHub called with:', tag);
    
    // Get tag data
    const tagData = tag.combined || tag;
    const tagId = getDatabaseId(tagData);
    
    if (!tagId) {
        console.error('Cannot sync tag without a database ID:', tagData);
        showNotification('error', 'Error syncing tag: Missing tag database ID.');
        return;
    }
    
    // Show loading notification
    showNotification('success', `Syncing tag "${tagData.name}" to DataHub...`);
    
    // Make the API call to sync this tag to DataHub
    fetch(`/metadata/api/tags/${tagId}/sync_to_datahub/`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCsrfToken()
        }
    })
    .then(response => {
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        return response.json();
    })
    .then(data => {
        console.log('Sync to DataHub response:', data);
        if (data.success) {
            showNotification('success', data.message);
            // Refresh the page to show updated sync status
            setTimeout(() => {
                window.location.reload();
            }, 1500);
        } else {
            throw new Error(data.error || 'Unknown error occurred');
        }
    })
    .catch(error => {
        console.error('Error syncing tag to DataHub:', error);
        showNotification('error', `Error syncing tag: ${error.message}`);
    });
}