/**
 * Tag Actions - Additional functionality for tag management
 * Contains functions for downloading tag JSON, syncing to local, and adding to staged changes
 */

function getDatabaseId(tagData) {
    if (tagData.database_id) {
        return tagData.database_id;
    }
    
    if (tagData.local) {
        if (tagData.local.database_id) {
            return tagData.local.database_id;
        }
        if (tagData.local.id) {
            return tagData.local.id;
        }
    }
    
    if (tagData.id) {
        return tagData.id;
    }
    
    console.warn('Could not find database ID for tag:', tagData);
    return null;
}

function downloadTagJson(tag) {
    const tagData = tag.combined || tag;
    const jsonData = JSON.stringify(tagData, null, 2);
    const blob = new Blob([jsonData], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    
    const link = document.createElement('a');
    link.href = url;
    link.download = `tag-${tagData.name || 'data'}.json`;
    document.body.appendChild(link);
    link.click();
    
    setTimeout(() => {
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }, 100);
}

function syncTagToLocal(tag) {
    showActionLoading('sync');
    
    const tagData = tag.combined || tag;
    const tagId = getDatabaseId(tagData);
    
    if (tagData.sync_status === 'REMOTE_ONLY') {
        fetch('/metadata/tags/pull/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCsrfToken()
            },
            body: JSON.stringify({
                urns: [tagData.urn],
                pull_specific: true,
                debug_info: true
            })
        })
        .then(response => {
            const contentType = response.headers.get('content-type');
            
            if (contentType && contentType.indexOf('application/json') !== -1) {
                if (!response.ok) {
                    return response.json().then(data => {
                        throw new Error(data.error || 'Failed to pull tag from DataHub');
                    });
                }
                return response.json();
            } else {
                console.error('Received non-JSON response from pull API:', contentType);
                return response.text().then(text => {
                    throw new Error('Server returned an unexpected response format. Check server logs.');
                });
            }
        })
        .then(data => {
            if (data.success) {
                MetadataNotifications.show('sync', 'pull_success', 'tag', { name: tagData.name });
                if (typeof loadTagsData === 'function') {
                    loadTagsData();
                } else {
                    window.location.reload();
                }
            } else {
                throw new Error(data.error || 'Failed to pull tag');
            }
        })
        .catch(error => {
            console.error('Error pulling remote tag:', error);
            MetadataNotifications.show('sync', 'pull_error', 'tag', { error: error.message });
        })
        .finally(() => {
            hideActionLoading('sync');
        });
        
        return;
    }
    
    if (!tagId) {
        console.error('Cannot sync tag without a database ID:', tagData);
        MetadataNotifications.show('sync', 'sync_to_local_missing_id', 'tag');
        hideActionLoading('sync');
        return;
    }
    
    const formattedTagId = formatTagId(tagId);
    
    fetch(`/metadata/api/tags/${formattedTagId}/sync_to_local/`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCsrfToken()
        },
        body: JSON.stringify({
            debug_info: true
        })
    })
    .then(response => {
        const contentType = response.headers.get('content-type');
        
        if (contentType && contentType.indexOf('application/json') !== -1) {
            if (!response.ok) {
                return response.json().then(data => {
                    throw new Error(data.error || 'Failed to sync tag to local database');
                });
            }
            return response.json();
        } else {
            console.error('Received non-JSON response from sync API:', contentType);
            return response.text().then(text => {
                throw new Error('Server returned an unexpected response format. Check server logs.');
            });
        }
    })
    .then(data => {
        MetadataNotifications.show('sync', 'sync_to_local_success', 'tag', { name: tagData.name });
        
        if (data.refresh_needed) {
            loadTagsData();
        }
    })
    .catch(error => {
        console.error('Error syncing tag to local:', error);
        MetadataNotifications.show('sync', 'sync_to_local_error', 'tag', { error: error.message });
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

function addTagToStagedChanges(tag) {
    showActionLoading('stage');
    
    const tagData = tag.combined || tag;
    const tagName = tagData.name || 'Unknown Tag';
    
    if (tagData.sync_status === 'REMOTE_ONLY' || !getDatabaseId(tagData)) {
        MetadataNotifications.show('staged_changes', 'add_to_staged_start', 'tag', { name: tagName });
        
        const currentEnvironment = window.currentEnvironment || { name: 'dev' };
        const mutationName = currentEnvironment.mutation_name || null;
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
            const contentType = response.headers.get('content-type');
            
            if (contentType && contentType.indexOf('application/json') !== -1) {
                if (!response.ok) {
                    return response.json().then(data => {
                        throw new Error(data.error || 'Failed to add remote tag to staged changes');
                    });
                }
                return response.json();
            } else {
                console.error('Received non-JSON response from staging API:', contentType);
                return response.text().then(text => {
                    throw new Error('Server returned an unexpected response format. Check server logs.');
                });
            }
        })
        .then(data => {
            if (data.status === 'success') {
                MetadataNotifications.show('staged_changes', 'add_to_staged_success', 'tag', { name: tagName });
            } else {
                throw new Error(data.error || 'Failed to add remote tag to staged changes');
            }
        })
        .catch(error => {
            console.error('Error adding remote tag to staged changes:', error);
            MetadataNotifications.show('staged_changes', 'add_to_staged_error', 'tag', { error: error.message });
        })
        .finally(() => {
            hideActionLoading('stage');
        });
        return;
    }
    
    const tagId = getDatabaseId(tagData);
    
    if (!tagId) {
        console.error('Cannot add tag to staged changes without a database ID:', tagData);
        MetadataNotifications.show('staged_changes', 'add_to_staged_missing_id', 'tag');
        hideActionLoading('stage');
        return;
    }
    
    const formattedTagId = formatTagId(tagId);
    const currentEnvironment = window.currentEnvironment || { name: 'dev' };
    const mutationName = currentEnvironment.mutation_name || null;
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
        const contentType = response.headers.get('content-type');
        
        if (contentType && contentType.indexOf('application/json') !== -1) {
            if (!response.ok) {
                return response.json().then(data => {
                    throw new Error(data.error || 'Failed to add tag to staged changes');
                });
            }
            return response.json();
        } else {
            console.error('Received non-JSON response from staging API:', contentType);
            return response.text().then(text => {
                throw new Error('Server returned an unexpected response format. Check server logs.');
            });
        }
    })
    .then(data => {
        if (data.success || data.files_created) {
            const files = data.files_created || [];
            MetadataNotifications.show('staged_changes', 'add_to_staged_success', 'tag', { 
                name: tagData.name, 
                files: files 
            });
            
            if (typeof loadTagsData === 'function') {
                loadTagsData();
            }
        } else {
            throw new Error(data.error || 'Failed to add tag to staged changes');
        }
    })
    .catch(error => {
        console.error('Error adding tag to staged changes:', error);
        MetadataNotifications.show('staged_changes', 'add_to_staged_error', 'tag', { error: error.message });
    })
    .finally(() => {
        hideActionLoading('stage');
    });
}

function getCsrfToken() {
    return document.querySelector('[name=csrfmiddlewaretoken]')?.value || 
           document.cookie.split('; ')
               .find(row => row.startsWith('csrftoken='))
               ?.split('=')[1];
}

function showActionLoading(actionType) {
    const buttons = document.querySelectorAll(`[data-action="${actionType}"]`);
    buttons.forEach(button => {
        button.disabled = true;
        button.dataset.originalHtml = button.innerHTML;
        button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>';
    });
}

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

function syncTagToDataHub(tag) {
    const tagData = tag.combined || tag;
    const tagId = getDatabaseId(tagData);
    
    if (!tagId) {
        console.error('Cannot sync tag without a database ID:', tagData);
        MetadataNotifications.show('sync', 'sync_to_datahub_missing_id', 'tag');
        return;
    }
    
    // Show loading notification
    MetadataNotifications.show('sync', 'sync_to_datahub_start', 'tag', { name: tagData.name });
    
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
        if (data.success) {
            MetadataNotifications.show('sync', 'sync_to_datahub_success', 'tag', { name: tagData.name });
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
        MetadataNotifications.show('sync', 'sync_to_datahub_error', 'tag', { error: error.message });
    });
}