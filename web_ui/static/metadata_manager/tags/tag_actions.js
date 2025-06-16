/**
 * Tag Actions - Additional functionality for tag management
 * Contains functions for downloading tag JSON, syncing to local, and adding to staged changes
 */

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
    const tagId = tagData.id;
    
    // Special handling for remote-only tags
    if (tagData.sync_status === 'REMOTE_ONLY') {
        console.log('Remote-only tag detected:', tagData);
        
        // For remote-only tags, we need to create them locally first
        // This is done by pulling the tag from the remote server
        fetch('/metadata/tags/pull/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCsrfToken()
            },
            body: JSON.stringify({
                urn: tagData.urn,
                pull_single: true
            })
        })
        .then(response => {
            if (!response.ok) {
                throw new Error('Failed to pull tag from DataHub');
            }
            return response.json();
        })
        .then(data => {
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
        console.error('Cannot sync tag without an ID:', tagData);
        showNotification('error', 'Error syncing tag: Missing tag ID.');
        hideActionLoading('sync');
        return;
    }
    
    // Make API call to sync tag to local
    fetch(`/api/metadata_manager/tags/${tagId}/sync_to_local/`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCsrfToken()
        }
    })
    .then(response => {
        if (!response.ok) {
            throw new Error('Failed to sync tag to local database');
        }
        return response.json();
    })
    .then(data => {
        // Show success notification
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
 * Add tag to staged changes
 * @param {Object} tag - The tag object
 */
function addTagToStagedChanges(tag) {
    console.log('addTagToStagedChanges called with:', tag);
    
    // Show loading spinner
    showActionLoading('stage');
    
    // Get tag data and environment
    const tagData = tag.combined || tag;
    // IMPORTANT: We must use the UUID (id) for the API endpoint, not the URN
    const tagId = tagData.id;
    
    // If we don't have an ID, we need to create the tag first
    if (!tagId) {
        console.error('Cannot add tag to staged changes without an ID:', tagData);
        showNotification('error', 'Error adding tag to staged changes: Missing tag ID. This tag needs to be created locally first.');
        hideActionLoading('stage');
        return;
    }
    
    // Get current environment and mutation from global state or settings
    const currentEnvironment = window.currentEnvironment || { name: 'dev' };
    const mutationName = currentEnvironment.mutation_name || null;
    
    console.log('Making API call to:', `/api/metadata_manager/tags/${tagId}/stage_changes/`);
    
    // Make API call to create MCP files
    fetch(`/api/metadata_manager/tags/${tagId}/stage_changes/`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCsrfToken()
        },
        body: JSON.stringify({
            environment: currentEnvironment.name,
            mutation_name: mutationName
        })
    })
    .then(response => {
        if (!response.ok) {
            throw new Error('Failed to add tag to staged changes');
        }
        return response.json();
    })
    .then(data => {
        // Show success notification
        showNotification('success', `Tag successfully added to staged changes: ${data.files_created.join(', ')}`);
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
    const bgClass = type === 'success' ? 'bg-success' : 'bg-danger';
    const icon = type === 'success' ? 'fa-check-circle' : 'fa-exclamation-circle';
    
    const toast = document.createElement('div');
    toast.className = `toast ${bgClass} text-white`;
    toast.id = id;
    toast.setAttribute('role', 'alert');
    toast.setAttribute('aria-live', 'assertive');
    toast.setAttribute('aria-atomic', 'true');
    toast.innerHTML = `
        <div class="toast-header ${bgClass} text-white">
            <i class="fas ${icon} me-2"></i>
            <strong class="me-auto">${type === 'success' ? 'Success' : 'Error'}</strong>
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