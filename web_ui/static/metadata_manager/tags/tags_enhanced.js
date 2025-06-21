// Global variables
let tagsData = {
    synced_tags: [],
    local_only_tags: [],
    remote_only_tags: [],
    datahub_url: ''
};
let currentSearch = {
    synced: '',
    local: '',
    remote: ''
};
let currentFilters = new Set();
let currentOverviewFilter = null;

// Pagination variables
let currentPagination = {
    synced: { page: 1, itemsPerPage: 25 },
    local: { page: 1, itemsPerPage: 25 },
    remote: { page: 1, itemsPerPage: 25 }
};

// Sorting variables
let currentSort = {
    column: null,
    direction: 'asc',
    tabType: null
};

// Connection-specific cache for users, groups, and ownership types
let usersAndGroupsCacheByConnection = {};
let currentConnectionId = null;

// Proxy object to maintain backward compatibility
let usersAndGroupsCache = new Proxy({}, {
    get(target, prop) {
        const connectionCache = getCurrentConnectionCache();
        return connectionCache[prop];
    },
    set(target, prop, value) {
        const connectionCache = getCurrentConnectionCache();
        connectionCache[prop] = value;
        return true;
    }
});

// Get or create cache for current connection
function getCurrentConnectionCache() {
    if (!currentConnectionId) {
        // Try to get connection ID from the page
        const connectionElement = document.getElementById('current-connection-name');
        if (connectionElement && connectionElement.dataset.connectionId) {
            currentConnectionId = connectionElement.dataset.connectionId;
        } else {
            // Fallback to default connection
            currentConnectionId = 'default';
        }
    }
    
    if (!usersAndGroupsCacheByConnection[currentConnectionId]) {
        usersAndGroupsCacheByConnection[currentConnectionId] = {
            users: [],
            groups: [],
            ownership_types: [],
            lastFetched: null,
            cacheExpiry: 5 * 60 * 1000 // 5 minutes
        };
    }
    
    return usersAndGroupsCacheByConnection[currentConnectionId];
}

// Function to switch to a specific connection cache
function switchConnectionCache(connectionId) {
    currentConnectionId = connectionId;
    console.log(`Switched to cache for connection: ${connectionId}`);
}

// Function to clear cache for a specific connection
function clearConnectionCache(connectionId) {
    if (usersAndGroupsCacheByConnection[connectionId]) {
        delete usersAndGroupsCacheByConnection[connectionId];
        console.log(`Cleared cache for connection: ${connectionId}`);
    }
}

// Make cache globally accessible for connection switching
window.usersAndGroupsCache = usersAndGroupsCache;
window.usersAndGroupsCacheByConnection = usersAndGroupsCacheByConnection;
window.switchConnectionCache = switchConnectionCache;
window.clearConnectionCache = clearConnectionCache;

// DataUtils object for safe data handling
const DataUtils = {
    createDisplaySafeItem: function(item, options = {}) {
        const { descriptionLength = 200, nameLength = 100, urnLength = 500 } = options;
        return {
            ...item,
            name: this.safeTruncateText(item.name || '', nameLength),
            description: this.safeTruncateText(item.description || '', descriptionLength),
            urn: this.safeTruncateText(item.urn || '', urnLength)
        };
    },
    
    safeJsonStringify: function(obj, maxLength = 5000) {
        try {
            // Handle circular references and special characters
            const seen = new WeakSet();
            const jsonString = JSON.stringify(obj, (key, value) => {
                if (typeof value === 'object' && value !== null) {
                    if (seen.has(value)) {
                        return '[Circular Reference]';
                    }
                    seen.add(value);
                }
                // Convert undefined values to null for valid JSON
                return value === undefined ? null : value;
            }, 2);
            return jsonString.length > maxLength ? jsonString.substring(0, maxLength) + '...' : jsonString;
        } catch (error) {
            console.error('Error stringifying object:', error);
            return '{}';
        }
    },
    
    safeJsonParse: function(jsonString) {
        try {
            if (!jsonString || typeof jsonString !== 'string') {
                console.error('Invalid JSON string:', jsonString);
                return {};
            }
            
            // Fix common JSON string issues
            // 1. Replace unescaped quotes within strings
            let fixedString = jsonString;
            
            // Log detailed info for debugging
            if (jsonString.length > 900 && jsonString.length < 1100) {
                console.log('Parsing potentially problematic JSON string around position 1000:', 
                    jsonString.substring(990, 1010));
            }
            
            return JSON.parse(fixedString);
        } catch (error) {
            console.error('Error parsing JSON:', error);
            console.error('JSON string (first 100 chars):', jsonString ? jsonString.substring(0, 100) : 'undefined');
            
            if (error instanceof SyntaxError && error.message.includes('position')) {
                const match = error.message.match(/position (\d+)/);
                if (match && match[1]) {
                    const position = parseInt(match[1]);
                    const start = Math.max(0, position - 20);
                    const end = Math.min(jsonString.length, position + 20);
                    console.error(`JSON error near position ${position}:`, 
                        jsonString.substring(start, position) + '→HERE→' + jsonString.substring(position, end));
                }
            }
            
            return {};
        }
    },
    
    safeEscapeHtml: function(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    },
    
    safeTruncateText: function(text, maxLength) {
        if (!text || typeof text !== 'string') return '';
        return text.length > maxLength ? text.substring(0, maxLength) + '...' : text;
    },
    
    sanitizeApiResponse: function(data) {
        // Simply return the data as-is for tags, no special sanitization needed
        return data;
    },
    
    formatDisplayText: function(text, maxLength, originalText = null) {
        if (!text) return '';
        const truncated = this.safeTruncateText(text, maxLength);
        return this.safeEscapeHtml(truncated);
    }
};

document.addEventListener('DOMContentLoaded', function() {
    // Initialize connection cache and load users and groups immediately on page load
    getCurrentConnectionCache(); // This initializes the connection ID and cache
    loadUsersAndGroups();
    
    loadTagsData();
    setupFilterListeners();
    setupSearchHandlers();
    setupBulkActions();
    setupActionButtonListeners();
    
    // Search functionality for each tab
    ['synced', 'local', 'remote'].forEach(tab => {
        const searchInput = document.getElementById(`${tab}-search`);
        const clearButton = document.getElementById(`${tab}-clear`);
        
        searchInput.addEventListener('input', function() {
            currentSearch[tab] = this.value.toLowerCase();
            displayTabContent(tab);
        });
        
        clearButton.addEventListener('click', function() {
            searchInput.value = '';
            currentSearch[tab] = '';
            displayTabContent(tab);
        });
    });
    
    // Refresh button
    document.getElementById('refreshTags').addEventListener('click', function() {
        loadTagsData();
    });
    
    // Setup form submission handler
    const tagForm = document.getElementById('tag-form');
    if (tagForm) {
        tagForm.addEventListener('submit', handleTagFormSubmission);
    }
    
    // Load users and groups when create modal is opened
    document.getElementById('createTagModal').addEventListener('show.bs.modal', async function() {
        // Ensure users and groups are loaded before setting up ownership interface
        if (usersAndGroupsCache.users.length === 0 && usersAndGroupsCache.groups.length === 0) {
            try {
                await loadUsersAndGroups();
                console.log('Tags modal: Users and groups loaded successfully');
            } catch (error) {
                console.error('Tags modal: Error loading users and groups:', error);
            }
        }
        
        // Only reset if not in edit mode (edit mode sets data attributes)
        const form = document.getElementById('tag-form');
        if (!form.dataset.editMode) {
            resetTagModal();
        }
        
        // Set up the ownership interface (Add Owner button functionality)
        setupOwnershipInterface();
        
        // Hide ownership section by default unless in edit mode with existing owners
        hideOwnershipSectionIfEmpty();
    });
    
    // Clean up when modal is closed
    document.getElementById('createTagModal').addEventListener('hidden.bs.modal', function() {
        resetTagModal();
    });
    

});

function setupFilterListeners() {
    // Overview filters (single select)
    document.querySelectorAll('[data-category="overview"]').forEach(stat => {
        stat.addEventListener('click', function() {
            const filter = this.dataset.filter;
            
            // Clear all overview filters first
            document.querySelectorAll('[data-category="overview"]').forEach(s => s.classList.remove('active'));
            
            if (filter === 'total') {
                // Clear all filters and show all data
                currentOverviewFilter = null;
                currentFilters.clear();
                document.querySelectorAll('[data-category="content"]').forEach(s => s.classList.remove('active'));
                applyFilters();
            } else if (currentOverviewFilter === filter) {
                // Deselect if clicking same filter
                currentOverviewFilter = null;
                applyFilters();
            } else {
                // Select new filter and switch tab
                currentOverviewFilter = filter;
                this.classList.add('active');
                
                // Clear content filters when switching overview
                currentFilters.clear();
                document.querySelectorAll('[data-category="content"]').forEach(s => s.classList.remove('active'));
                
                // Switch to appropriate tab based on filter
                switch(filter) {
                    case 'synced':
                        switchToTab('synced');
                        break;
                    case 'local-only':
                        switchToTab('local');
                        break;
                    case 'remote-only':
                        switchToTab('remote');
                        break;
                }
                
                applyFilters();
            }
        });
    });
    
    // Content filters (multi-select)
    document.querySelectorAll('[data-category="content"]').forEach(stat => {
        stat.addEventListener('click', function() {
            const filter = this.dataset.filter;
            
            if (currentFilters.has(filter)) {
                currentFilters.delete(filter);
                this.classList.remove('active');
            } else {
                currentFilters.add(filter);
                this.classList.add('active');
            }
            
            applyFilters();
        });
    });
}

function setupSearchHandlers() {
    ['synced', 'local', 'remote'].forEach(tab => {
        const searchInput = document.getElementById(`${tab}-search`);
        const clearButton = document.getElementById(`${tab}-clear`);
        
        if (searchInput && clearButton) {
            searchInput.addEventListener('input', function() {
                currentSearch[tab] = this.value.toLowerCase();
                renderTab(`${tab}-content`);
            });
            
            clearButton.addEventListener('click', function() {
                searchInput.value = '';
                currentSearch[tab] = '';
                renderTab(`${tab}-content`);
            });
        }
    });
}

function setupBulkActions() {
    ['synced', 'local', 'remote'].forEach(tab => {
        const selectAllCheckbox = document.getElementById(`selectAll${tab.charAt(0).toUpperCase() + tab.slice(1)}`);
        if (selectAllCheckbox) {
            selectAllCheckbox.addEventListener('change', function() {
                const checkboxes = document.querySelectorAll(`#${tab}-content .item-checkbox`);
                checkboxes.forEach(checkbox => {
                    checkbox.checked = this.checked;
                });
                updateBulkActionVisibility(tab);
            });
        }
    });
}

function updateBulkActionVisibility(tab) {
    const checkboxes = document.querySelectorAll(`#${tab}-content .item-checkbox:checked`);
    const bulkActions = document.getElementById(`${tab}-bulk-actions`);
    const selectedCount = document.getElementById(`${tab}-selected-count`);
    
    if (checkboxes.length > 0) {
        bulkActions.classList.add('show');
        selectedCount.textContent = checkboxes.length;
    } else {
        bulkActions.classList.remove('show');
    }
}

function applyFilters() {
    renderAllTabs();
}

function filterTags(tags) {
    return tags.filter(tag => {
        // Get the actual tag data (combined for synced, direct for others)
        const tagData = tag.combined || tag;
        
        // Apply overview filter
        if (currentOverviewFilter) {
            switch (currentOverviewFilter) {
                case 'total':
                    break; // Show all
                case 'synced':
                    if (tagData.sync_status !== 'SYNCED') return false;
                    break;
                case 'local-only':
                    if (tagData.sync_status !== 'LOCAL_ONLY') return false;
                    break;
                case 'remote-only':
                    if (tagData.sync_status !== 'REMOTE_ONLY') return false;
                    break;
            }
        }
        
        // Apply multi-select filters
        if (currentFilters.size > 0) {
            let passesFilter = false;
            
            for (const filter of currentFilters) {
                switch (filter) {
                    case 'owned':
                        // Calculate owners count
                        let hasOwners = false;
                        if (tagData.ownership_data && tagData.ownership_data.owners && tagData.ownership_data.owners.length > 0) {
                            hasOwners = true;
                        } else if (tagData.owner_names && tagData.owner_names.length > 0) {
                            hasOwners = true;
                        }
                        if (hasOwners) passesFilter = true;
                        break;

                }
            }
            
            if (!passesFilter) return false;
        }
        
        return true;
    });
}

function renderAllTabs() {
    displayTabContent('synced');
    displayTabContent('local');
    displayTabContent('remote');
}

function loadTagsData() {
    showLoading(true);
    
    // Reset the tag data cache
    window.tagDataCache = {};
    
    // Clear any existing selections to prevent stale data issues
    clearAllSelections();
    
    fetch('/metadata/tags/remote-data/')
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            // Apply global sanitization to prevent issues with long descriptions and malformed data
            tagsData = DataUtils.sanitizeApiResponse(data.data);
            
            // Pre-populate the tag data cache with all tags
            const allTags = [
                ...(tagsData.synced_tags || []),
                ...(tagsData.local_only_tags || []),
                ...(tagsData.remote_only_tags || [])
            ];
            
            allTags.forEach(tag => {
                const tagData = tag.combined || tag;
                const cacheKey = tagData.urn || tagData.id;
                if (cacheKey) {
                    window.tagDataCache[cacheKey] = tagData;
                }
            });
            
            console.log(`Cached ${Object.keys(window.tagDataCache).length} tags for quick access`);
            
            updateStatistics(data.data.statistics);
            updateTabBadges();
            displayAllTabs();
        } else {
            showError('Failed to load tags: ' + (data.error || 'Unknown error'));
        }
    })
    .catch(error => {
        console.error('Error loading tags:', error);
        showError('Failed to load tags data');
    })
    .finally(() => {
        showLoading(false);
    });
}

// Function to clear all selections when data is refreshed
function clearAllSelections() {
    const checkboxes = document.querySelectorAll('.item-checkbox, .select-all-checkbox');
    checkboxes.forEach(checkbox => {
        checkbox.checked = false;
    });
    
    // Update bulk action visibility for all tabs
    ['synced', 'local', 'remote'].forEach(tab => {
        updateBulkActionVisibility(tab);
    });
    
    console.log('Cleared all tag selections due to data refresh');
}

function showError(message) {
    // Show error in a user-friendly way
    const contentAreas = ['synced-content', 'local-content', 'remote-content'];
    contentAreas.forEach(contentId => {
        const element = document.getElementById(contentId);
        if (element) {
            element.innerHTML = `
                <div class="alert alert-danger m-3">
                    <i class="fas fa-exclamation-triangle me-2"></i>
                    ${message}
                </div>
            `;
        }
    });
}

function showLoading(show) {
    document.getElementById('loading-indicator').style.display = show ? 'block' : 'none';
    document.getElementById('tags-content').style.display = show ? 'none' : 'block';
}

function updateStatistics(stats) {
    document.getElementById('total-tags').textContent = stats.total_tags || 0;
    document.getElementById('synced-count').textContent = stats.synced_count || 0;
    document.getElementById('local-only-count').textContent = stats.local_count || 0;
    document.getElementById('remote-only-count').textContent = stats.remote_count || 0;
    document.getElementById('owned-tags').textContent = stats.owned_tags || 0;

}

function updateTabBadges() {
    document.getElementById('synced-badge').textContent = tagsData.synced_tags.length;
    document.getElementById('local-badge').textContent = tagsData.local_only_tags.length;
    document.getElementById('remote-badge').textContent = tagsData.remote_only_tags.length;
}

function displayAllTabs() {
    displayTabContent('synced');
    displayTabContent('local');
    displayTabContent('remote');
}

function displayTabContent(tabType) {
    const contentId = `${tabType}-content`;
    const searchTerm = currentSearch[tabType] || '';
    
    try {
        let items = [];
        
        switch (tabType) {
            case 'synced':
                items = tagsData.synced_tags || [];
                break;
            case 'local':
                items = tagsData.local_only_tags || [];
                break;
            case 'remote':
                items = tagsData.remote_only_tags || [];
                break;
        }
        
        // Apply filters
        items = filterTags(items);

        // Apply search
        if (searchTerm) {
            items = items.filter(item => {
                const tagData = item.combined || item;
                const name = tagData.properties?.name || tagData.name || '';
                const description = tagData.properties?.description || tagData.description || '';
                const urn = tagData.urn || '';
                
                return name.toLowerCase().includes(searchTerm) ||
                       description.toLowerCase().includes(searchTerm) ||
                       urn.toLowerCase().includes(searchTerm);
            });
        }

        // Apply sorting if active for this tab
        if (currentSort.column && currentSort.tabType === tabType) {
            items = sortItems(items, currentSort.column, currentSort.direction);
        }

        // Generate table HTML
        const tableHTML = generateTableHTML(items, tabType);
        
        // Update content
        const content = document.getElementById(contentId);
        content.innerHTML = tableHTML;
        
        // Attach click handlers for view buttons
        content.querySelectorAll('.view-item').forEach(button => {
            button.addEventListener('click', function() {
                const row = this.closest('tr');
                try {
                    // Parse the minimal reference data
                    const rawData = row.dataset.item
                        .replace(/&quot;/g, '"')
                        .replace(/&apos;/g, "'");
                    const minimalData = JSON.parse(rawData);
                    
                    // Look up the full tag data from the cache
                    const cacheKey = minimalData.urn || minimalData.id;
                    const fullTagData = window.tagDataCache[cacheKey];
                    
                    if (fullTagData) {
                        // Ensure cache is loaded before showing details
                        if (usersAndGroupsCache.users.length === 0) {
                            loadUsersAndGroups().then(() => showTagDetails(fullTagData));
                        } else {
                            showTagDetails(fullTagData);
                        }
                    } else {
                        console.error('Tag data not found in cache:', minimalData);
                        alert('Error: Tag data not found. Please refresh the page and try again.');
                    }
                } catch (error) {
                    console.error('Failed to parse item data from row:', error);
                }
            });
        });
        
        // Attach bulk action handlers
        content.querySelectorAll('.item-checkbox').forEach(checkbox => {
            checkbox.addEventListener('change', function() {
                updateBulkActionVisibility(tabType);
            });
        });
        
        // Attach sorting handlers and restore sort state
        attachSortingHandlers(content, tabType);
        restoreSortState(content, tabType);
        
        // Attach pagination handlers
        attachPaginationHandlers(content, tabType);
        
        // Attach select-all checkbox handler
        const selectAllCheckbox = content.querySelector('.select-all-checkbox');
        if (selectAllCheckbox) {
            selectAllCheckbox.addEventListener('change', function() {
                const checkboxes = content.querySelectorAll('.item-checkbox');
                checkboxes.forEach(checkbox => {
                    checkbox.checked = this.checked;
                });
                updateBulkActionVisibility(tabType);
            });
        }

        console.log(`Successfully rendered ${tabType} tab with ${items.length} items`);
        
    } catch (error) {
        console.error(`Error displaying ${tabType} tab:`, error);
        const content = document.getElementById(contentId);
        if (content) {
            content.innerHTML = `<div class="alert alert-danger">Error loading ${tabType} tags: ${error.message}</div>`;
        }
    }
}

function generateTableHTML(items, tabType) {
    const pagination = currentPagination[tabType];
    const startIndex = (pagination.page - 1) * pagination.itemsPerPage;
    const endIndex = startIndex + pagination.itemsPerPage;
    const paginatedItems = items.slice(startIndex, endIndex);
    
    const totalPages = Math.ceil(items.length / pagination.itemsPerPage);
    
    // Determine if we should show the sync status column
    const showSyncStatus = tabType === 'synced';
    // Adjust column widths based on table type - give more room for actions
    const nameWidth = showSyncStatus ? '110' : '100';        // Slightly less room for name on synced
    const descriptionWidth = showSyncStatus ? '160' : '150'; // Slightly less room for description on synced
    const urnWidth = showSyncStatus ? '180' : '260';         // Less space for URN to make room for actions
    const actionsWidth = showSyncStatus ? '150' : '140';     // More space for actions, especially on synced
    
    return `
        <div class="table-responsive">
            <table class="table table-hover mb-0">
                <thead>
                    <tr>
                        <th width="30">
                            <input type="checkbox" class="form-check-input select-all-checkbox" id="selectAll${tabType.charAt(0).toUpperCase() + tabType.slice(1)}">
                        </th>
                        <th class="sortable-header" data-sort="name" width="${nameWidth}">Name</th>
                        <th width="${descriptionWidth}">Description</th>
                        <th class="sortable-header" data-sort="color" width="80">Color</th>
                        <th class="sortable-header" data-sort="owners" width="60">Owners</th>
                        <th width="${urnWidth}">URN</th>
                        ${showSyncStatus ? '<th class="sortable-header" data-sort="sync_status" width="80">Sync Status</th>' : ''}
                        <th width="${actionsWidth}">Actions</th>
                    </tr>
                </thead>
                <tbody>
                    ${paginatedItems.length === 0 ? getEmptyStateHTML(tabType, currentSearch[tabType]) : 
                      paginatedItems.map(item => renderTagRow(item, tabType)).join('')}
                </tbody>
            </table>
        </div>
        ${items.length > pagination.itemsPerPage ? generatePaginationHTML(items.length, tabType) : ''}
    `;
}

function renderTagRow(tag, tabType) {
    // Get the tag data (combined for synced, direct for others)
    const tagData = tag.combined || tag;
    
    // Sanitize tag data for the data-item attribute using global utilities
    const sanitizedTag = sanitizeDataForAttribute(tag);
    
    // Enhanced name extraction: first check tag properties, then direct name property,
    // then try to extract from URN before falling back to 'Unnamed Tag'
    let name = tagData.properties?.name;
    if (!name) name = tagData.name;
    if (!name && tagData.urn) {
        // Extract name from URN (e.g., "DBDATE" from "urn:li:tag:DBDATE")
        const urnParts = tagData.urn.split(':');
        if (urnParts.length > 0) {
            name = urnParts[urnParts.length - 1];
        }
    }
    if (!name) name = 'Unnamed Tag';
    
    const description = tagData.properties?.description || tagData.description || '';
    const originalColor = tagData.properties?.colorHex || tagData.color;
    const color = originalColor || '#6c757d'; // For display purposes (background)
    const displayColor = originalColor || 'null'; // For text display
    const urn = tagData.urn || '';
    
    // Get owners information
    const owners = tagData.owner_names || [];
    let ownersCount = 0;
    
    // Calculate owners count from ownership_data if available
    if (tagData.ownership_data && tagData.ownership_data.owners) {
        ownersCount = tagData.ownership_data.owners.length;
    } else if (owners.length) {
        ownersCount = owners.length;
    }
    
    const ownersTitle = owners.length > 0 ? owners.join(', ') : 'No owners';
    

    
    // Get sync status (only needed for synced tab)
    const syncStatus = tagData.sync_status || 'UNKNOWN';
    const syncStatusClass = getStatusBadgeClass(syncStatus);
    const syncStatusText = syncStatus.replace('_', ' ');
    
    // Determine if we should show the sync status column
    const showSyncStatus = tabType === 'synced';
    
    // Instead of storing the full JSON data in the HTML attribute, 
    // just store the minimal data needed to identify the tag
    const minimalTagData = {
        id: sanitizedTag.id || sanitizedTag.datahub_id,
        urn: sanitizedTag.urn,
        sync_status: sanitizedTag.sync_status
    };
    
    // Store the full tag data in a global object for lookup
    if (!window.tagDataCache) {
        window.tagDataCache = {};
    }
    
    // Use the URN as a unique key for the cache
    const cacheKey = sanitizedTag.urn || sanitizedTag.id;
    if (cacheKey) {
        window.tagDataCache[cacheKey] = sanitizedTag;
    }
    
    // Use safe JSON stringify for minimal data attributes
    const safeJsonData = JSON.stringify(minimalTagData)
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&apos;');
    
    // Get custom action buttons
    const customActionButtons = getActionButtons(tagData, tabType);
    
    // Get the proper database ID for this tag
    const databaseId = getDatabaseId(tagData);
    
    return `
        <tr data-item='${safeJsonData}'>
            <td>
                <input type="checkbox" class="form-check-input item-checkbox" value="${databaseId || tagData.urn}">
            </td>
            <td title="${escapeHtml(name)}">
                <div class="d-flex align-items-center">
                    <i class="fas fa-tag text-info me-2"></i>
                    <strong>${DataUtils.formatDisplayText(name, 50, tagData._original?.name || name)}</strong>
                </div>
            </td>
            <td title="${description ? escapeHtml(description) : 'No description'}">
                ${description ? DataUtils.formatDisplayText(description, 150, tagData._originalDescription || description) : '<span class="text-muted">No description</span>'}
            </td>
            <td>
                <div class="d-flex align-items-center">
                    <div class="color-swatch me-2" style="background-color: ${color}"></div>
                    <code class="small">${displayColor}</code>
                </div>
            </td>
            <td title="${ownersTitle}" class="text-center">
                ${ownersCount > 0 ? `<i class="fas fa-users text-info me-1"></i><span class="badge bg-info">${ownersCount}</span>` : '<span class="text-muted">None</span>'}
            </td>
            <td title="${escapeHtml(urn)}">
                <code class="small">${escapeHtml(urn)}</code>
            </td>
            ${showSyncStatus ? `
            <td>
                <span class="badge ${syncStatusClass}">${syncStatusText}</span>
            </td>
            ` : ''}
            <td>
                <div class="btn-group action-buttons" role="group">
                    <!-- View entity button -->
                    <button type="button" class="btn btn-sm btn-outline-primary view-item" 
                            title="View Details">
                        <i class="fas fa-eye"></i>
                    </button>
                    
                    <!-- View in DataHub button if applicable (only for synced/remote tags) -->
                    ${urn && !urn.includes('local:') && tagData.sync_status !== 'LOCAL_ONLY' ? `
                        <a href="${getDataHubUrl(urn, 'tag')}" 
                           class="btn btn-sm btn-outline-info" 
                           target="_blank" title="View in DataHub">
                            <i class="fas fa-external-link-alt"></i>
                        </a>
                    ` : ''}
                    
                    <!-- Custom action buttons -->
                    ${customActionButtons}
                </div>
            </td>
        </tr>
    `;
}

function getEmptyStateHTML(tabType, hasSearch) {
    // Determine the correct colspan based on the number of columns shown
    // Synced: checkbox + name + description + color + owners + urn + sync status + actions = 8
    // Local/Remote: checkbox + name + description + color + owners + urn + actions = 7
    const colspan = tabType === 'synced' ? '8' : '7';
    
    if (hasSearch) {
    return `
            <tr>
                <td colspan="${colspan}" class="text-center py-4 text-muted">
                    <i class="fas fa-search fa-2x mb-2"></i><br>
                    No tags found matching your search criteria.
                </td>
                    </tr>
        `;
    }
    
    const emptyStates = {
        synced: 'No synced tags found. Tags that exist both locally and in DataHub will appear here.',
        local: 'No local-only tags found. Tags that exist only in this application will appear here.',
        remote: 'No remote-only tags found. Tags that exist only in DataHub will appear here.'
    };
    
    return `
        <tr>
            <td colspan="${colspan}" class="text-center py-4 text-muted">
                <i class="fas fa-tag fa-2x mb-2"></i><br>
                ${emptyStates[tabType]}
            </td>
        </tr>
    `;
}

function sanitizeDataForAttribute(item, maxDescriptionLength = 200) {
    // First ensure we have a valid name property
    const tagData = item.combined || item;
    
    // Make a copy of the item to avoid modifying the original
    const sanitizedItem = {...tagData};
    
    // Ensure name is properly set
    if (!sanitizedItem.name) {
        if (sanitizedItem.properties?.name) {
            sanitizedItem.name = sanitizedItem.properties.name;
        } else if (sanitizedItem.urn) {
            // Extract name from URN as fallback
            const urnParts = sanitizedItem.urn.split(':');
            if (urnParts.length > 0) {
                sanitizedItem.name = urnParts[urnParts.length - 1];
            }
        }
        
        if (!sanitizedItem.name) {
            sanitizedItem.name = 'Unnamed Tag';
        }
    }
    
    // Use global utility for consistent data sanitization
    return DataUtils.createDisplaySafeItem(sanitizedItem, {
        descriptionLength: maxDescriptionLength,
        nameLength: 100,
        urnLength: 500
    });
}

function getStatusBadgeClass(status) {
    switch (status) {
        case 'SYNCED':
            return 'bg-success';
        case 'MODIFIED':
            return 'bg-warning';
        case 'LOCAL_ONLY':
            return 'bg-secondary';
        case 'REMOTE_ONLY':
            return 'bg-info';
        default:
            return 'bg-secondary';
    }
}

function getActionButtons(tag, tabType) {
    // Get tag data
    const tagData = tag.combined || tag;
    const urn = tagData.urn || '';
    
    // Get the proper database ID for this tag
    const databaseId = getDatabaseId(tagData);
    
    let actionButtons = '';
    
    // 1. Edit Tag - For local and synced tags
    if (tabType === 'local' || tabType === 'synced') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-warning edit-tag" 
                    title="Edit Tag">
                <i class="fas fa-edit"></i>
            </button>
        `;
    }

    // 2. Sync to DataHub - Only for local-only tags
    if (tabType === 'local' && tagData.sync_status === 'LOCAL_ONLY') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-success sync-to-datahub" 
                    title="Sync to DataHub">
                <i class="fas fa-upload"></i>
            </button>
        `;
    }
    
    // 2b. Resync - Only for synced tags that are modified or synced
    if (tabType === 'synced' && (tagData.sync_status === 'MODIFIED' || tagData.sync_status === 'SYNCED')) {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-info resync-tag" 
                    title="Resync from DataHub">
                <i class="fas fa-sync-alt"></i>
            </button>
        `;
    }
    
    // 2c. Push to DataHub - For synced tags that are modified or local tags
    if ((tabType === 'synced' && tagData.sync_status === 'MODIFIED') || tabType === 'local') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-success push-to-datahub" 
                    title="Push to DataHub">
                <i class="fas fa-upload"></i>
            </button>
        `;
    }
    
    // 3. Sync to Local - Only for remote/synced tags
    if (tabType === 'remote' || (tabType === 'synced' && tagData.is_remote)) {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-primary sync-to-local" 
                    title="Sync to Local">
                <i class="fas fa-download"></i>
            </button>
        `;
    }
    
        // 4. Download JSON - Available for all tags
    actionButtons += `
        <button type="button" class="btn btn-sm btn-outline-secondary download-json"
                title="Download JSON">
            <i class="fas fa-file-download"></i>
        </button>
    `;

    // 5. Add to Staged Changes - Available for all tags
    actionButtons += `
        <button type="button" class="btn btn-sm btn-outline-warning add-to-staged"
                title="Add to Staged Changes">
            <i class="fab fa-github"></i>
        </button>
    `;

    // 6. Delete Local Tag - Only for synced and local tags
    if (tabType === 'synced' || tabType === 'local') {
        // Use a simple form with a POST action for direct deletion
        // This avoids all the complex JavaScript ID handling
        actionButtons += `
            <form method="POST" action="/metadata/api/tags/${tagData.id}/delete/" 
                  style="display:inline;" 
                  onsubmit="return confirm('Are you sure you want to delete this tag? This action cannot be undone.');">
                <input type="hidden" name="csrfmiddlewaretoken" value="${getCsrfToken()}">
                <button type="submit" class="btn btn-sm btn-outline-danger"
                        title="Delete Local Tag">
                    <i class="fas fa-trash"></i>
                </button>
            </form>
        `;
    }
    
    // 7. Delete Remote Tag - Only for remote-only tags
    if (tabType === 'remote') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-danger delete-remote-tag" 
                    title="Delete from DataHub">
                <i class="fas fa-trash"></i>
            </button>
        `;
    }
    
    return actionButtons;
}

function getDataHubUrl(urn, type) {
    if (!tagsData.datahub_url || !urn) return '#';
    
    // Ensure no double slashes and don't encode URN colons
    const baseUrl = tagsData.datahub_url.replace(/\/+$/, ''); // Remove trailing slashes
    return `${baseUrl}/tag/${urn}`;
}

function truncateUrn(urn, maxLength) {
    if (!urn || urn.length <= maxLength) return urn;
    return urn.substring(0, maxLength - 3) + '...';
}

function escapeHtml(text) {
    // Use global utility for consistent HTML escaping
    return DataUtils.safeEscapeHtml(text);
}

function generatePaginationHTML(totalItems, tabType) {
    const pagination = currentPagination[tabType];
    const totalPages = Math.ceil(totalItems / pagination.itemsPerPage);
    const currentPage = pagination.page;
    
    if (totalPages <= 1) return '';
    
    const startItem = (currentPage - 1) * pagination.itemsPerPage + 1;
    const endItem = Math.min(currentPage * pagination.itemsPerPage, totalItems);
    
    let paginationHTML = `
        <div class="pagination-container">
            <div class="pagination-info">
                Showing ${startItem}-${endItem} of ${totalItems} tags
            </div>
            <nav aria-label="Table pagination">
                <ul class="pagination mb-0">
    `;
    
    // Previous button
    paginationHTML += `
        <li class="page-item ${currentPage === 1 ? 'disabled' : ''}">
            <a class="page-link" href="#" data-page="${currentPage - 1}" data-tab="${tabType}">Previous</a>
        </li>
    `;
    
    // Page numbers
    const startPage = Math.max(1, currentPage - 2);
    const endPage = Math.min(totalPages, currentPage + 2);
    
    if (startPage > 1) {
        paginationHTML += `<li class="page-item"><a class="page-link" href="#" data-page="1" data-tab="${tabType}">1</a></li>`;
        if (startPage > 2) {
            paginationHTML += `<li class="page-item disabled"><span class="page-link">...</span></li>`;
        }
    }
    
    for (let i = startPage; i <= endPage; i++) {
        paginationHTML += `
            <li class="page-item ${i === currentPage ? 'active' : ''}">
                <a class="page-link" href="#" data-page="${i}" data-tab="${tabType}">${i}</a>
            </li>
        `;
    }
    
    if (endPage < totalPages) {
        if (endPage < totalPages - 1) {
            paginationHTML += `<li class="page-item disabled"><span class="page-link">...</span></li>`;
        }
        paginationHTML += `<li class="page-item"><a class="page-link" href="#" data-page="${totalPages}" data-tab="${tabType}">${totalPages}</a></li>`;
    }
    
    // Next button
    paginationHTML += `
        <li class="page-item ${currentPage === totalPages ? 'disabled' : ''}">
            <a class="page-link" href="#" data-page="${currentPage + 1}" data-tab="${tabType}">Next</a>
        </li>
    `;
    
    paginationHTML += `
                </ul>
            </nav>
            <div class="d-flex align-items-center">
                <label for="itemsPerPage-${tabType}" class="form-label me-2 mb-0">Items per page:</label>
                <select class="form-select form-select-sm" id="itemsPerPage-${tabType}" style="width: auto;">
                    <option value="10" ${pagination.itemsPerPage === 10 ? 'selected' : ''}>10</option>
                    <option value="25" ${pagination.itemsPerPage === 25 ? 'selected' : ''}>25</option>
                    <option value="50" ${pagination.itemsPerPage === 50 ? 'selected' : ''}>50</option>
                    <option value="100" ${pagination.itemsPerPage === 100 ? 'selected' : ''}>100</option>
                </select>
            </div>
        </div>
    `;
    
    return paginationHTML;
}

function attachPaginationHandlers(content, tabType) {
    // Attach pagination click handlers
    content.querySelectorAll('.page-link[data-page]').forEach(link => {
        link.addEventListener('click', function(e) {
            e.preventDefault();
            const page = parseInt(this.dataset.page);
            const tab = this.dataset.tab;
            if (page && tab && currentPagination[tab]) {
                currentPagination[tab].page = page;
                displayTabContent(tab);
            }
        });
    });
    
    // Attach items per page change handler
    const itemsPerPageSelect = content.querySelector(`#itemsPerPage-${tabType}`);
    if (itemsPerPageSelect) {
        itemsPerPageSelect.addEventListener('change', function() {
            currentPagination[tabType].itemsPerPage = parseInt(this.value);
            currentPagination[tabType].page = 1; // Reset to first page
            displayTabContent(tabType);
        });
    }
}

// Attach sorting handlers to table headers
function attachSortingHandlers(content, tabType) {
    const sortableHeaders = content.querySelectorAll('.sortable-header');
    
    sortableHeaders.forEach(header => {
        header.addEventListener('click', function() {
            const sortColumn = this.dataset.sort;
            
            // Toggle sort direction
            if (currentSort.column === sortColumn && currentSort.tabType === tabType) {
                currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
            } else {
                currentSort.column = sortColumn;
                currentSort.direction = 'asc';
                currentSort.tabType = tabType;
            }
            
            // Re-render the table with sorting
            displayTabContent(tabType);
        });
    });
}

// Restore sort state visual indicators
function restoreSortState(content, tabType) {
    if (currentSort.column && currentSort.tabType === tabType) {
        const tableHeaders = content.querySelectorAll('.sortable-header');
        tableHeaders.forEach(h => {
            h.classList.remove('sort-asc', 'sort-desc');
            if (h.dataset.sort === currentSort.column) {
                h.classList.add(currentSort.direction === 'asc' ? 'sort-asc' : 'sort-desc');
            }
        });
    }
}

// Get sort value from tag data
function getSortValue(tag, column) {
    const tagData = tag.combined || tag;
    
    switch(column) {
        case 'name':
            return (tagData.properties?.name || tagData.name || '').toLowerCase();
        case 'description':
            return (tagData.properties?.description || tagData.description || '').toLowerCase();
        case 'color':
            const colorValue = tagData.properties?.colorHex || tagData.color;
            return (colorValue || 'null').toLowerCase();
        case 'owners':
            // Calculate owners count from ownership_data if available
            let ownersCount = 0;
            if (tagData.ownership_data && tagData.ownership_data.owners) {
                ownersCount = tagData.ownership_data.owners.length;
            } else if (tagData.owner_names) {
                ownersCount = tagData.owner_names.length;
            }
            return ownersCount;

        case 'sync_status':
            const syncStatus = tagData.sync_status || 'UNKNOWN';
            // Define sort order: SYNCED -> MODIFIED -> LOCAL_ONLY -> REMOTE_ONLY -> UNKNOWN
            const statusOrder = {
                'SYNCED': 1,
                'MODIFIED': 2,
                'LOCAL_ONLY': 3,
                'REMOTE_ONLY': 4,
                'UNKNOWN': 5
            };
            return statusOrder[syncStatus] || 5;
        case 'urn':
            return (tagData.urn || '').toLowerCase();
        default:
            return '';
    }
}

// Sort items array
function sortItems(items, column, direction) {
    return items.sort((a, b) => {
        const aVal = getSortValue(a, column);
        const bVal = getSortValue(b, column);
        
        if (typeof aVal === 'number' && typeof bVal === 'number') {
            return direction === 'asc' ? aVal - bVal : bVal - aVal;
        }
        
        if (aVal < bVal) return direction === 'asc' ? -1 : 1;
        if (aVal > bVal) return direction === 'asc' ? 1 : -1;
        return 0;
    });
}

function showTagDetails(tag) {
    // Reuse existing modal functionality
    const tagData = tag.combined || tag;
    
    // Enhanced name extraction: check in different places and finally extract from URN if needed
    let tagName = tagData._original?.name;
    if (!tagName) tagName = tagData.properties?.name;
    if (!tagName) tagName = tagData.name;
    if (!tagName && tagData.urn) {
        // Extract name from URN (e.g., "DBDATE" from "urn:li:tag:DBDATE")
        const urnParts = tagData.urn.split(':');
        if (urnParts.length > 0) {
            tagName = urnParts[urnParts.length - 1];
        }
    }
    if (!tagName) tagName = 'Unnamed Tag';
    
    console.log("Showing tag details for:", tagData);
    
    // Update modal with tag data - use original descriptions if available
    document.getElementById('modal-tag-name').textContent = tagName;
    document.getElementById('modal-tag-description').textContent = tagData._originalDescription || tagData._original?.description || tagData.properties?.description || tagData.description || 'No description';
    
    const colorSwatch = document.getElementById('modal-tag-color-swatch');
    const originalColor = tagData.properties?.colorHex || tagData.color;
    const color = originalColor || '#6c757d'; // For display purposes (background)
    const displayColor = originalColor || 'null'; // For text display
    colorSwatch.style.backgroundColor = color;
    document.getElementById('modal-tag-color').textContent = displayColor;
    

    
    document.getElementById('modal-tag-urn').textContent = tagData.urn || '';
    
    // Update owners information split by ownership type
    const ownersListElement = document.getElementById('modal-owners-list');
    
    // Load users/groups cache if needed
    if (usersAndGroupsCache.users.length === 0) {
        loadUsersAndGroups();
    }
    
    // Check for ownership data from GraphQL (remote tags) or local storage
    const ownershipData = tagData.ownership || tagData.ownership_data || tagData.relationships?.ownership;
    
    if (ownershipData && ownershipData.owners && ownershipData.owners.length > 0) {
        // Group owners by ownership type
        const ownersByType = {};
        
        ownershipData.owners.forEach(ownerInfo => {
            let ownerUrn, ownershipTypeUrn, ownershipTypeName;
            
            // Handle different data structures
            if (ownerInfo.owner_urn && ownerInfo.ownership_type_urn) {
                // Local storage format
                ownerUrn = ownerInfo.owner_urn;
                ownershipTypeUrn = ownerInfo.ownership_type_urn;
                
                // Find the ownership type name from cache
                ownershipTypeName = 'Unknown Type';
                const ownershipType = usersAndGroupsCache.ownership_types.find(ot => ot.urn === ownershipTypeUrn);
                if (ownershipType) {
                    ownershipTypeName = ownershipType.name;
                }
            } else if (ownerInfo.owner && ownerInfo.ownershipType) {
                // GraphQL format
                ownerUrn = ownerInfo.owner.urn;
                ownershipTypeUrn = ownerInfo.ownershipType.urn;
                ownershipTypeName = ownerInfo.ownershipType.info?.name || 'Unknown Type';
            } else if (ownerInfo.ownerUrn && ownerInfo.type) {
                // Remote-only format
                ownerUrn = ownerInfo.ownerUrn;
                ownershipTypeUrn = ownerInfo.type;
                ownershipTypeName = 'Unknown Type';
                
                // Find the ownership type name from cache
                const ownershipType = usersAndGroupsCache.ownership_types.find(ot => ot.urn === ownershipTypeUrn);
                if (ownershipType) {
                    ownershipTypeName = ownershipType.name;
                }
            } else {
                return; // Skip invalid entries
            }
            
            // Find the owner name
            let ownerName = ownerUrn;
            let isUser = false;
            
            if (ownerInfo.owner && (ownerInfo.owner.username || ownerInfo.owner.name)) {
                // GraphQL format - owner data is already included
                if (ownerInfo.owner.username) {
                    // CorpUser
                    isUser = true;
                    ownerName = ownerInfo.owner.properties?.displayName || ownerInfo.owner.username;
                } else if (ownerInfo.owner.name) {
                    // CorpGroup
                    isUser = false;
                    ownerName = ownerInfo.owner.properties?.displayName || ownerInfo.owner.name;
                }
            } else {
                // Local storage format - need to look up in cache
                const user = usersAndGroupsCache.users.find(u => u.urn === ownerUrn);
                const group = usersAndGroupsCache.groups.find(g => g.urn === ownerUrn);
                
                if (user) {
                    isUser = true;
                    ownerName = user.display_name || user.username || ownerUrn;
                } else if (group) {
                    isUser = false;
                    ownerName = group.display_name || ownerUrn;
                }
            }
            
            if (!ownersByType[ownershipTypeName]) {
                ownersByType[ownershipTypeName] = [];
            }
            ownersByType[ownershipTypeName].push({
                name: ownerName,
                urn: ownerUrn,
                isUser: isUser
            });
        });
        
        // Generate HTML for owners grouped by type
        let ownersHTML = '';
        Object.keys(ownersByType).forEach(ownershipType => {
            const owners = ownersByType[ownershipType];
            ownersHTML += `
                <div class="mb-3">
                    <h6 class="text-primary mb-2">
                        <i class="fas fa-crown me-1"></i>${escapeHtml(ownershipType)}
                    </h6>
                    <div class="ms-3">
                        ${owners.map(owner => `
                            <div class="d-flex align-items-center mb-1">
                                <i class="fas fa-${owner.isUser ? 'user' : 'users'} text-muted me-2"></i>
                                <span>${escapeHtml(owner.name)}</span>
                            </div>
                        `).join('')}
                    </div>
                </div>
            `;
        });
        
        ownersListElement.innerHTML = ownersHTML;
    } else {
        ownersListElement.innerHTML = '<p class="text-muted">No ownership information available</p>';
    }
    
    // Update DataHub link with proper URL from configuration
    const datahubLink = document.getElementById('modal-datahub-link');
    const urn = tagData.urn;
    if (tagsData.datahub_url && urn && !urn.includes('local:')) {
        // Ensure no double slashes and don't encode URN colons
        const baseUrl = tagsData.datahub_url.replace(/\/+$/, ''); // Remove trailing slashes
        datahubLink.href = `${baseUrl}/tag/${urn}`;
        datahubLink.style.display = 'inline-block';
    } else {
        datahubLink.style.display = 'none';
    }
    
    // Update raw JSON data - directly use the tag data object
    const rawJsonElement = document.getElementById('modal-raw-json');
    try {
        // Create a clean copy of the tag data to display
        const displayData = JSON.parse(JSON.stringify(tagData));
        rawJsonElement.innerHTML = `<code>${escapeHtml(JSON.stringify(displayData, null, 2))}</code>`;
    } catch (error) {
        console.error("Error preparing JSON for display:", error);
        rawJsonElement.innerHTML = `<code>Error preparing JSON for display: ${error.message}</code>`;
    }
    
    // Get modal element
    const modalElement = document.getElementById('tagViewModal');
    
    // Add event listener for when modal is hidden
    modalElement.addEventListener('hidden.bs.modal', function() {
        // Clean up any resources
        console.log('Modal hidden, cleaning up resources');
        // Remove the event listener to prevent memory leaks
        modalElement.removeEventListener('hidden.bs.modal', arguments.callee);
    }, { once: true });
    
    // Show modal
    const modal = new bootstrap.Modal(modalElement);
    modal.show();
}

// Bulk action functions
function bulkResyncTags(tabType) {
    const selectedTags = getSelectedTags(tabType);
    if (selectedTags.length === 0) {
        showNotification('error', 'Please select tags to resync.');
        return;
    }
    
    if (confirm(`Are you sure you want to resync ${selectedTags.length} tag(s) from DataHub? This will update local data with current remote data.`)) {
        console.log(`Bulk resync ${selectedTags.length} tags for ${tabType}:`, selectedTags);
        
        // Show loading indicator
        showNotification('success', `Starting resync of ${selectedTags.length} tags from DataHub...`);
        
        // Get tag IDs/URNs
        const tagIds = selectedTags.map(tag => getDatabaseId(tag) || tag.urn).filter(id => id);
        
        if (tagIds.length === 0) {
            showNotification('error', 'No valid tag IDs found for resync.');
            return;
        }
        
        // Make bulk resync API call
        fetch('/metadata/api/tags/bulk_resync/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCsrfToken()
            },
            body: JSON.stringify({
                tag_ids: tagIds
            })
        })
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            return response.json();
        })
        .then(data => {
            console.log('Bulk resync response:', data);
            if (data.success) {
                showNotification('success', data.message);
                // Add a small delay before refreshing to ensure backend processing is complete
                setTimeout(() => {
                    // Refresh the data to show updated sync status
                    if (typeof loadTagsData === 'function') {
                        loadTagsData();
                    } else {
                        window.location.reload();
                    }
                }, 1000); // 1 second delay
            } else {
                throw new Error(data.error || 'Unknown error occurred');
            }
        })
        .catch(error => {
            console.error('Error in bulk resync:', error);
            showNotification('error', `Error resyncing tags: ${error.message}`);
        });
    }
}

function bulkSyncToDataHub(tabType) {
    const selectedTags = getSelectedTags(tabType);
    if (selectedTags.length === 0) {
        showNotification('error', 'Please select tags to sync to DataHub.');
        return;
    }
    
    if (confirm(`Are you sure you want to sync ${selectedTags.length} tag(s) to DataHub?`)) {
        console.log(`Bulk sync ${selectedTags.length} tags to DataHub for ${tabType}:`, selectedTags);
        
        // Show loading indicator
        showNotification('success', `Starting sync of ${selectedTags.length} tags to DataHub...`);
        
        // Get tag IDs
        const tagIds = selectedTags.map(tag => getDatabaseId(tag)).filter(id => id);
        
        if (tagIds.length === 0) {
            showNotification('error', 'No valid tag IDs found for sync.');
            return;
        }
        
        // Make bulk sync API call
        fetch('/metadata/api/tags/bulk_sync_to_datahub/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCsrfToken()
            },
            body: JSON.stringify({
                tag_ids: tagIds
            })
        })
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            return response.json();
        })
        .then(data => {
            console.log('Bulk sync response:', data);
            if (data.success) {
                showNotification('success', data.message);
                // Add a small delay before refreshing to ensure backend processing is complete
                setTimeout(() => {
                    // Refresh the data to show updated sync status
                    if (typeof loadTagsData === 'function') {
                        loadTagsData();
                    } else {
                        window.location.reload();
                    }
                }, 1000); // 1 second delay
            } else {
                throw new Error(data.error || 'Unknown error occurred');
            }
        })
        .catch(error => {
            console.error('Error in bulk sync:', error);
            showNotification('error', `Error syncing tags: ${error.message}`);
        });
    }
}

function bulkPushTags(tabType) {
    // Legacy function - redirect to new sync function
    bulkSyncToDataHub(tabType);
}

function bulkAddToPR(tabType) {
    const selectedTags = getSelectedTags(tabType);
    if (selectedTags.length === 0) {
        showNotification('error', 'Please select tags to add to staged changes.');
        return;
    }
    
    // Validate all selected tags have valid IDs and exist in current data
    const validatedTags = [];
    const invalidTags = [];
    
    selectedTags.forEach(tag => {
        const tagId = getDatabaseId(tag);
        if (!tagId) {
            invalidTags.push(tag);
            return;
        }
        
        // Check if the tag exists in current cached data
        const cacheKey = tag.urn || tag.id;
        const cachedTag = window.tagDataCache[cacheKey];
        if (!cachedTag) {
            console.warn('Tag not found in current cache, may be stale:', tag);
            invalidTags.push(tag);
            return;
        }
        
        validatedTags.push(tag);
    });
    
    if (invalidTags.length > 0) {
        console.error('Found invalid or stale tags:', invalidTags);
        showNotification('error', `${invalidTags.length} selected tags are invalid or from stale data. Please refresh the page and try again.`);
        
        // Clear selections to prevent further issues
        clearAllSelections();
        return;
    }
    
    if (confirm(`Are you sure you want to add ${validatedTags.length} tag(s) to staged changes?`)) {
        console.log(`Bulk add ${validatedTags.length} validated tags to staged changes for ${tabType}:`, validatedTags);
        
        // Show loading indicator
        showNotification('success', `Starting to add ${validatedTags.length} tags to staged changes...`);
        
        // Get current environment and mutation from global state or settings
        const currentEnvironment = window.currentEnvironment || { name: 'dev' };
        const mutationName = currentEnvironment.mutation_name || null;
        
        // Process each tag sequentially
        let successCount = 0;
        let errorCount = 0;
        let processedCount = 0;
        let createdFiles = [];
        
        // Create a function to process tags one by one
        function processNextTag(index) {
            if (index >= validatedTags.length) {
                // All tags processed
                if (successCount > 0) {
                    showNotification('success', `Completed: ${successCount} tags added to staged changes, ${errorCount} failed.`);
                    if (createdFiles.length > 0) {
                        console.log('Created files:', createdFiles);
                    }
                } else if (errorCount > 0) {
                    showNotification('error', `Failed to add any tags to staged changes. ${errorCount} errors occurred.`);
                }
                return;
            }
            
            const tag = validatedTags[index];
            
            // We need the ID for the API call - use the validated database ID
            const tagId = getDatabaseId(tag);
            if (!tagId) {
                console.error('Cannot add tag to staged changes without an ID:', tag);
                errorCount++;
                processedCount++;
                processNextTag(index + 1);
                return;
            }
            
            // Make the API call to add this tag to staged changes
            fetch(`/metadata/api/tags/${tagId}/stage_changes/`, {
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
                    throw new Error(`Failed to add tag ${tag.name || tag.urn} to staged changes`);
                }
                return response.json();
            })
            .then(data => {
                console.log(`Successfully added tag to staged changes: ${tag.name || tag.urn}`);
                successCount++;
                processedCount++;
                
                // Track created files
                if (data.files_created && data.files_created.length > 0) {
                    createdFiles = [...createdFiles, ...data.files_created];
                }
                
                // Update progress
                if (processedCount % 5 === 0 || processedCount === validatedTags.length) {
                    showNotification('success', `Progress: ${processedCount}/${validatedTags.length} tags processed`);
                }
                
                // Process the next tag
                processNextTag(index + 1);
            })
            .catch(error => {
                console.error(`Error adding tag ${tag.name || tag.urn} to staged changes:`, error);
                errorCount++;
                processedCount++;
                
                // Process the next tag despite the error
                processNextTag(index + 1);
            });
        }
        
        // Start processing tags
        processNextTag(0);
    }
}

/**
 * Download multiple tags as a single JSON file
 * @param {string} tabType - The tab type (synced, local, remote)
 */
function bulkDownloadJson(tabType) {
    const selectedTags = getSelectedTags(tabType);
    if (selectedTags.length === 0) {
        showNotification('error', 'Please select tags to download.');
        return;
    }
    
    console.log(`Bulk download ${selectedTags.length} tags for ${tabType}:`, selectedTags);
    
    // Create a JSON object with the selected tags
    const tagsData = {
        tags: selectedTags,
        metadata: {
            exported_at: new Date().toISOString(),
            count: selectedTags.length,
            source: window.location.origin,
            tab: tabType
        }
    };
    
    // Convert to pretty JSON
    const jsonData = JSON.stringify(tagsData, null, 2);
    
    // Create a blob and initiate download
    const blob = new Blob([jsonData], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    
    // Create temporary link and trigger download
    const link = document.createElement('a');
    link.href = url;
    link.download = `tags-export-${selectedTags.length}-${new Date().toISOString().slice(0, 10)}.json`;
    document.body.appendChild(link);
    link.click();
    
    // Clean up
    setTimeout(() => {
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }, 100);
    
    showNotification('success', `${selectedTags.length} tags exported successfully.`);
}

function bulkDeleteLocal(tabType) {
    const selectedTags = getSelectedTags(tabType);
    if (selectedTags.length === 0) {
        showNotification('error', 'Please select tags to delete.');
        return;
    }
    
    if (confirm(`Are you sure you want to delete ${selectedTags.length} local tag(s)? This action cannot be undone.`)) {
        console.log(`Bulk delete ${selectedTags.length} local tags for ${tabType}:`, selectedTags);
        
        // Show loading indicator
        showNotification('success', `Starting deletion of ${selectedTags.length} local tags...`);
        
        // Process each tag sequentially
        let successCount = 0;
        let errorCount = 0;
        let processedCount = 0;
        
        // Create a function to process tags one by one
        function processNextTag(index) {
            if (index >= selectedTags.length) {
                // All tags processed
                showNotification('success', `Completed: ${successCount} tags deleted, ${errorCount} failed.`);
                if (successCount > 0) {
                    // Refresh the data if any tags were successfully deleted
                    loadTagsData();
                }
                return;
            }
            
            const tag = selectedTags[index];
            
            // We need the database ID for the API call
            const tagId = getDatabaseId(tag);
            if (!tagId) {
                console.error('Cannot delete tag without a database ID:', tag);
                errorCount++;
                processedCount++;
                processNextTag(index + 1);
                return;
            }
            
            console.log('Using database ID for deletion:', tagId);
            
            // Get a proper name for the tag
            let tagName = tag.name;
            if (!tagName) {
                if (tag.properties?.name) {
                    tagName = tag.properties.name;
                } else if (tag.urn) {
                    // Extract name from URN as fallback
                    const urnParts = tag.urn.split(':');
                    if (urnParts.length > 0) {
                        tagName = urnParts[urnParts.length - 1];
                    }
                }
                
                if (!tagName) {
                    tagName = 'Unnamed Tag';
                }
            }
            
            // Make the API call to delete this tag
            console.log(`Deleting tag with ID: ${tagId}, name: ${tagName}`);
            
            // Format the tag ID as a UUID with dashes if needed
            const formattedTagId = formatTagId(tagId);
            console.log(`Formatted tag ID for deletion: ${formattedTagId}`);
            
            const csrfToken = getCsrfToken();
            
            fetch(`/metadata/api/tags/${formattedTagId}/delete/`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                    'X-CSRFToken': csrfToken
                },
                // Include a body to ensure proper POST handling
                body: JSON.stringify({
                    confirm: true
                })
            })
            .then(response => {
                // Log response status for debugging
                console.log(`Delete response status for tag ${tagName}:`, response.status);
                
                // Check if the response is ok (status in 200-299 range)
                if (!response.ok) {
                    if (response.status === 404) {
                        // If tag not found, consider it a success (it's already gone)
                        console.log(`Tag ${tagName} not found (404), considering as successfully deleted`);
                        return { success: true, message: `Tag ${tagName} already deleted` };
                    } else {
                        // For other error statuses, throw an error
                        throw new Error(`Server returned status ${response.status}`);
                    }
                }
                
                // Check for non-JSON responses that might be returning HTML error pages
                const contentType = response.headers.get('content-type');
                console.log('Delete response content type:', contentType);
                
                if (contentType && contentType.indexOf('application/json') !== -1) {
                    // This is a JSON response, proceed normally
                    return response.json();
                } else {
                    // This is likely an HTML error page or unexpected response
                    console.error('Received non-JSON response:', contentType);
                    return response.text().then(text => {
                        console.error('Delete response content (first 500 chars):', text.substring(0, 500) + '...');
                        throw new Error('Server returned an unexpected response format. See console for details.');
                    });
                }
                          })
              .then(data => {
                  console.log(`Successfully deleted tag: ${tagName}`);
                  successCount++;
                  processedCount++;
                  
                  // Update progress
                  if (processedCount % 5 === 0 || processedCount === selectedTags.length) {
                      showNotification('success', `Progress: ${processedCount}/${selectedTags.length} tags processed`);
                  }
                  
                  // Process the next tag
                  processNextTag(index + 1);
              })
                            .catch(error => {
                console.error(`Error deleting tag ${tagName}:`, error);
                errorCount++;
                processedCount++;
                
                // Show error notification for significant errors
                if (processedCount % 5 === 0 || processedCount === 1) {
                    showNotification('error', `Error deleting tag: ${error.message || 'Unknown error'}`);
                }
                
                // Process the next tag despite the error
                setTimeout(() => processNextTag(index + 1), 100); // Small delay to prevent overwhelming the server
              });
        }
        
        // Start processing tags
        processNextTag(0);
    }
}

function bulkSyncToLocal(tabType) {
    const selectedTags = getSelectedTags(tabType);
    if (selectedTags.length === 0) {
        showNotification('error', 'Please select tags to sync to local.');
        return;
    }
    
    if (confirm(`Are you sure you want to sync ${selectedTags.length} tag(s) to local?`)) {
        console.log(`Bulk sync ${selectedTags.length} tags to local for ${tabType}:`, selectedTags);
        
        // Show loading indicator
        showNotification('success', `Starting sync of ${selectedTags.length} tags to local...`);
        
        // Process each tag sequentially
        let successCount = 0;
        let errorCount = 0;
        let processedCount = 0;
        
        // Create a function to process tags one by one
        function processNextTag(index) {
            if (index >= selectedTags.length) {
                // All tags processed
                showNotification('success', `Completed: ${successCount} tags synced successfully, ${errorCount} failed.`);
                if (successCount > 0) {
                    // Refresh the data if any tags were successfully synced
                    loadTagsData();
                }
                return;
            }
            
            const tag = selectedTags[index];
            
            // For remote-only tags, we need to handle them differently
            if (tag.sync_status === 'REMOTE_ONLY') {
                // For remote-only tags, we need to create them first
                // This would typically be handled by the pull functionality
                // For now, we'll skip these and count them as errors
                console.log(`Skipping remote-only tag: ${tag.name || tag.urn}`);
                errorCount++;
                processedCount++;
                processNextTag(index + 1);
                return;
            }
            
            // We need the database ID for the API call
            const tagId = getDatabaseId(tag);
            if (!tagId) {
                console.error('Cannot sync tag without a database ID:', tag);
                errorCount++;
                processedCount++;
                processNextTag(index + 1);
                return;
            }
            
            console.log('Using database ID for sync to local:', tagId);
            
            // Format the tag ID as a UUID with dashes if needed
            const formattedTagId = formatTagId(tagId);
            console.log(`Formatted tag ID for sync: ${formattedTagId}`);
            
            // Get a proper name for the tag
            let tagName = tag.name;
            if (!tagName) {
                if (tag.properties?.name) {
                    tagName = tag.properties.name;
                } else if (tag.urn) {
                    // Extract name from URN as fallback
                    const urnParts = tag.urn.split(':');
                    if (urnParts.length > 0) {
                        tagName = urnParts[urnParts.length - 1];
                    }
                }
                
                if (!tagName) {
                    tagName = 'Unnamed Tag';
                }
            }
            
            // Make the API call to sync this tag
            fetch(`/metadata/api/tags/${formattedTagId}/sync_to_local/`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': getCsrfToken()
                }
            })
            .then(response => {
                if (!response.ok) {
                    throw new Error(`Failed to sync tag ${tagName}`);
                }
                return response.json();
            })
            .then(data => {
                console.log(`Successfully synced tag: ${tagName}`);
                successCount++;
                processedCount++;
                
                // Update progress
                if (processedCount % 5 === 0 || processedCount === selectedTags.length) {
                    showNotification('success', `Progress: ${processedCount}/${selectedTags.length} tags processed`);
                }
                
                // Process the next tag
                processNextTag(index + 1);
            })
            .catch(error => {
                console.error(`Error syncing tag ${tagName}:`, error);
                errorCount++;
                processedCount++;
                
                // Process the next tag despite the error
                processNextTag(index + 1);
            });
        }
        
        // Start processing tags
        processNextTag(0);
    }
}

function bulkDeleteRemote(tabType) {
    const selectedTags = getSelectedTags(tabType);
    if (selectedTags.length === 0) {
        alert('Please select tags to delete from DataHub.');
        return;
    }
    
    if (confirm(`Are you sure you want to delete ${selectedTags.length} tag(s) from DataHub? This action cannot be undone.`)) {
        console.log(`Bulk delete ${selectedTags.length} remote tags for ${tabType}:`, selectedTags);
        // TODO: Implement bulk delete remote API call
        alert('Bulk delete remote functionality will be implemented soon.');
    }
}

function updateBulkActionVisibility(tabType) {
    const checkboxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    const bulkActions = document.getElementById(`${tabType}-bulk-actions`);
    const selectedCount = document.getElementById(`${tabType}-selected-count`);
    
    if (bulkActions && selectedCount) {
        if (checkboxes.length > 0) {
            bulkActions.classList.add('show');
            selectedCount.textContent = checkboxes.length;
        } else {
            bulkActions.classList.remove('show');
            selectedCount.textContent = '0';
        }
    }
}

function getSelectedTags(tabType) {
    const checkboxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    const selectedTags = [];
    
    checkboxes.forEach(checkbox => {
        const row = checkbox.closest('tr');
        if (row && row.dataset.item) {
            try {
                const tagData = JSON.parse(row.dataset.item);
                selectedTags.push(tagData);
            } catch (e) {
                console.error('Error parsing tag data:', e);
            }
        }
    });
    
    return selectedTags;
}

function switchToTab(tabType) {
    // Activate the correct tab
    document.querySelectorAll('#tagTabs .nav-link').forEach(tab => {
        tab.classList.remove('active');
        tab.setAttribute('aria-selected', 'false');
    });
    
    document.querySelectorAll('.tab-pane').forEach(pane => {
        pane.classList.remove('show', 'active');
    });
    
    // Map tabType to actual tab IDs
    const tabMapping = {
        'synced': 'synced-tab',
        'local': 'local-tab', 
        'remote': 'remote-tab'
    };
    
    const paneMapping = {
        'synced': 'synced-tags',
        'local': 'local-tags',
        'remote': 'remote-tags'
    };
    
    const tabElement = document.getElementById(tabMapping[tabType]);
    const paneElement = document.getElementById(paneMapping[tabType]);
    
    if (tabElement && paneElement) {
        tabElement.classList.add('active');
        tabElement.setAttribute('aria-selected', 'true');
        paneElement.classList.add('show', 'active');
        
        console.log(`Switched to ${tabType} tab`);
    }
}

// Load users and groups with connection-specific caching
async function loadUsersAndGroups() {
    const now = Date.now();
    const connectionCache = getCurrentConnectionCache();
    
    // Check if cache is still valid for current connection
    if (connectionCache.lastFetched && 
        (now - connectionCache.lastFetched) < connectionCache.cacheExpiry &&
        connectionCache.users.length > 0) {
        console.log(`Using cached users and groups for connection: ${currentConnectionId}`);
        return;
    }
    
    console.log(`Fetching fresh users and groups data for connection: ${currentConnectionId}`);
    
    try {
        // Fetch users, groups, and ownership types in a single request
        const csrfToken = getCsrfToken();
        console.log('Using CSRF token for users-groups:', csrfToken);
        
        const response = await fetch('/metadata/api/users-groups/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': csrfToken
            },
            body: JSON.stringify({ type: 'all' })
        });
        
        const data = await response.json();
        
        console.log('Users and groups API response:', data);
        
        if (data.success) {
            // Check if data.data exists before accessing its properties
            if (data.data) {
                connectionCache.users = data.data.users || [];
                connectionCache.groups = data.data.groups || [];
                connectionCache.ownership_types = data.data.ownership_types || [];
                connectionCache.lastFetched = now;
                
                console.log(`Loaded ${connectionCache.users.length} users, ${connectionCache.groups.length} groups, and ${connectionCache.ownership_types.length} ownership types for connection ${currentConnectionId}${data.cached ? ' (cached)' : ''}`);
                populateSimpleDropdowns();
            } else {
                console.error('API returned success but no data object');
                // Initialize with empty arrays to prevent errors
                connectionCache.users = [];
                connectionCache.groups = [];
                connectionCache.ownership_types = [];
                showOwnersError();
            }
        } else {
            console.error('Failed to load users, groups, and ownership types:', data.error);
            // Initialize with empty arrays to prevent errors
            connectionCache.users = [];
            connectionCache.groups = [];
            connectionCache.ownership_types = [];
            showOwnersError();
        }
    } catch (error) {
        console.error('Error loading users and groups:', error);
        showOwnersError();
    }
}

// Setup the ownership interface with dynamic owner entries
function setupOwnershipInterface() {
    const container = document.getElementById('ownership-sections-container');
    const addButton = document.getElementById('add-ownership-section');
    
    if (!container || !addButton) return;
    
    // Clear existing sections
    container.innerHTML = '';
    
    // Remove any existing event listeners to prevent multiple handlers
    const newAddButton = addButton.cloneNode(true);
    addButton.parentNode.replaceChild(newAddButton, addButton);
    
    // Setup add ownership section button with enhanced interaction
    newAddButton.addEventListener('click', (e) => {
        e.preventDefault();
        addOwnershipSection();
        // Show the ownership section when adding the first one
        showOwnershipSection();
        // Add a subtle animation to the button
        newAddButton.style.transform = 'scale(0.95)';
        setTimeout(() => {
            newAddButton.style.transform = 'scale(1)';
        }, 150);
    });
    
    // Don't add an initial ownership section - let it be hidden by default
    // Only show ownership section if there are existing owners (in edit mode)
}



// Populate owners select dropdown (for individual entries)
function populateOwnersSelect(select) {
    if (!select) return;
    
    select.innerHTML = '';
    
    // Add users
    if (usersAndGroupsCache.users.length > 0) {
        const usersGroup = document.createElement('optgroup');
        usersGroup.label = '👤 Users';
        
        usersAndGroupsCache.users.forEach(user => {
            const option = document.createElement('option');
            option.value = user.urn;
            
            // Show display name, username, or URN as fallback
            let displayText = user.display_name || user.username;
            if (!displayText) {
                // Extract username from URN as fallback
                const urnParts = user.urn.split(':');
                displayText = urnParts[urnParts.length - 1] || user.urn;
                option.className = 'text-warning'; // Indicate missing name
                option.title = `⚠️ Name not available in current DataHub. URN: ${user.urn}`;
            }
            
            option.textContent = displayText;
            option.dataset.type = 'user';
            option.dataset.urn = user.urn;
            usersGroup.appendChild(option);
        });
        
        select.appendChild(usersGroup);
    }
    
    // Add groups
    if (usersAndGroupsCache.groups.length > 0) {
        const groupsGroup = document.createElement('optgroup');
        groupsGroup.label = '👥 Groups';
        
        usersAndGroupsCache.groups.forEach(group => {
            const option = document.createElement('option');
            option.value = group.urn;
            
            // Show display name or URN as fallback
            let displayText = group.display_name;
            if (!displayText) {
                // Extract group name from URN as fallback
                const urnParts = group.urn.split(':');
                displayText = urnParts[urnParts.length - 1] || group.urn;
                option.className = 'text-warning'; // Indicate missing name
                option.title = `⚠️ Name not available in current DataHub. URN: ${group.urn}`;
            }
            
            option.textContent = displayText;
            option.dataset.type = 'group';
            option.dataset.urn = group.urn;
            groupsGroup.appendChild(option);
        });
        
        select.appendChild(groupsGroup);
    }
    
    if (usersAndGroupsCache.users.length === 0 && usersAndGroupsCache.groups.length === 0) {
        const option = document.createElement('option');
        option.value = '';
        option.textContent = '⚠️ No users or groups available from current DataHub connection';
        option.disabled = true;
        option.className = 'text-warning';
        select.appendChild(option);
    }
}

// Format owner option for Select2
function formatOwnerOption(option) {
    if (!option.id) {
        return option.text;
    }
    
    const type = option.element.dataset.type;
    const icon = type === 'user' ? '👤' : '👥';
    const hasWarning = option.element.className.includes('text-warning');
    const warningIcon = hasWarning ? '⚠️ ' : '';
    
    return $(`<span class="${hasWarning ? 'text-warning' : ''}" title="${option.element.title || ''}">${warningIcon}${icon} ${option.text}</span>`);
}

// Format owner selection for Select2
function formatOwnerSelection(option) {
    if (!option.id) {
        return option.text;
    }
    
    const type = option.element.dataset.type;
    const icon = type === 'user' ? '👤' : '👥';
    const hasWarning = option.element.className.includes('text-warning');
    const warningIcon = hasWarning ? '⚠️ ' : '';
    
    return `${warningIcon}${icon} ${option.text}`;
}



// Populate ownership type select dropdown
function populateOwnershipTypeSelect(select) {
    if (!select) return;
    
    // Get already selected ownership types from other sections
    const selectedOwnershipTypes = getSelectedOwnershipTypes(select);
    
    // Clear existing options
    select.innerHTML = '';
    
    // Add default option
    const defaultOption = document.createElement('option');
    defaultOption.value = '';
    defaultOption.textContent = 'Select ownership type...';
    select.appendChild(defaultOption);
    
    if (usersAndGroupsCache.ownership_types.length > 0) {
        // Add all available ownership types (excluding already selected ones)
        usersAndGroupsCache.ownership_types.forEach(ownershipType => {
            // Skip if this ownership type is already selected in another section
            if (selectedOwnershipTypes.includes(ownershipType.urn)) {
                return;
            }
            
            const option = document.createElement('option');
            option.value = ownershipType.urn;
            option.textContent = `👑 ${ownershipType.name || ownershipType.urn.split(':').pop()}`;
            
            // Pre-select technical owner if available and not already selected
            if (ownershipType.urn === 'urn:li:ownershipType:__system__technical_owner' && 
                !selectedOwnershipTypes.includes(ownershipType.urn)) {
                option.selected = true;
            }
            
            select.appendChild(option);
        });
    } else {
        // Fallback - add common ownership types manually if cache is empty
        const commonTypes = [
            { urn: 'urn:li:ownershipType:__system__technical_owner', name: 'Technical Owner' },
            { urn: 'urn:li:ownershipType:__system__business_owner', name: 'Business Owner' },
            { urn: 'urn:li:ownershipType:__system__data_steward', name: 'Data Steward' }
        ];
        
        commonTypes.forEach(ownershipType => {
            // Skip if this ownership type is already selected in another section
            if (selectedOwnershipTypes.includes(ownershipType.urn)) {
                return;
            }
            
            const option = document.createElement('option');
            option.value = ownershipType.urn;
            option.textContent = `👑 ${ownershipType.name}`;
            option.className = 'text-warning'; // Indicate these are fallback options
            
            // Pre-select technical owner if not already selected
            if (ownershipType.urn === 'urn:li:ownershipType:__system__technical_owner' && 
                !selectedOwnershipTypes.includes(ownershipType.urn)) {
                option.selected = true;
            }
            
            select.appendChild(option);
        });
        
        // Add warning message
        const warningOption = document.createElement('option');
        warningOption.disabled = true;
        warningOption.textContent = '⚠️ Ownership types not loaded from current DataHub';
        warningOption.className = 'text-warning';
        select.appendChild(warningOption);
    }
}

// Helper function to get already selected ownership types from other sections
function getSelectedOwnershipTypes(currentSelect) {
    const selectedTypes = [];
    const allOwnershipTypeSelects = document.querySelectorAll('#ownership-sections-container .ownership-type-select');
    
    allOwnershipTypeSelects.forEach(select => {
        // Skip the current select element
        if (select === currentSelect) return;
        
        // Add the selected value if it exists
        if (select.value) {
            selectedTypes.push(select.value);
        }
    });
    
    return selectedTypes;
}

// Refresh all ownership type dropdowns to update available options
function refreshAllOwnershipTypeDropdowns() {
    const allOwnershipTypeSelects = document.querySelectorAll('#ownership-sections-container .ownership-type-select');
    
    allOwnershipTypeSelects.forEach(select => {
        // Store the current value
        const currentValue = select.value;
        
        // Repopulate the dropdown
        populateOwnershipTypeSelect(select);
        
        // Restore the current value if it's still available
        if (currentValue && select.querySelector(`option[value="${currentValue}"]`)) {
            select.value = currentValue;
        }
    });
}

// Legacy function - kept for compatibility but redirects to new function
function populateOwnerSelect(select) {
    populateOwnersSelect(select);
}

// Setup filtering for select dropdowns
function setupSelectFilter(filterInput, selectElement) {
    if (!filterInput || !selectElement) return;
    
    // Store original options
    const originalOptions = Array.from(selectElement.options);
    
    filterInput.addEventListener('input', function() {
        const filterText = this.value.toLowerCase();
        
        // Clear current options
        selectElement.innerHTML = '';
        
        // Filter and add matching options
        originalOptions.forEach(option => {
            if (option.textContent.toLowerCase().includes(filterText) || option.value === '') {
                selectElement.appendChild(option.cloneNode(true));
            }
        });
        
        // If no matches found (except empty option), show a message
        if (selectElement.options.length <= 1 && filterText) {
            const noMatchOption = document.createElement('option');
            noMatchOption.value = '';
            noMatchOption.textContent = 'No matches found';
            noMatchOption.disabled = true;
            selectElement.appendChild(noMatchOption);
        }
    });
}

// Enhanced setup filtering for select dropdowns with better UX
function setupEnhancedSelectFilter(filterInput, selectElement, itemType) {
    if (!filterInput || !selectElement) return;
    
    // Store original options
    const originalOptions = Array.from(selectElement.options);
    
    // Add placeholder enhancement
    filterInput.placeholder = `Type to search ${itemType}...`;
    
    // Add focus/blur handlers for visual feedback
    filterInput.addEventListener('focus', function() {
        this.style.borderColor = '#007bff';
        this.style.boxShadow = '0 0 0 0.2rem rgba(0, 123, 255, 0.15)';
    });
    
    filterInput.addEventListener('blur', function() {
        this.style.borderColor = '#e9ecef';
        this.style.boxShadow = 'none';
    });
    
    filterInput.addEventListener('input', function() {
        const filterText = this.value.toLowerCase();
        
        // Visual feedback for typing
        if (filterText) {
            this.style.backgroundColor = '#f8f9fa';
        } else {
            this.style.backgroundColor = '#ffffff';
        }
        
        // Clear current options
        selectElement.innerHTML = '';
        
        let matchCount = 0;
        
        // Filter and add matching options
        originalOptions.forEach(option => {
            if (option.textContent.toLowerCase().includes(filterText) || option.value === '') {
                selectElement.appendChild(option.cloneNode(true));
                if (option.value !== '') matchCount++;
            }
        });
        
        // If no matches found (except empty option), show a message
        if (matchCount === 0 && filterText) {
            const noMatchOption = document.createElement('option');
            noMatchOption.value = '';
            noMatchOption.textContent = `No ${itemType} found matching "${filterText}"`;
            noMatchOption.disabled = true;
            noMatchOption.style.fontStyle = 'italic';
            selectElement.appendChild(noMatchOption);
        }
        
        // Update select visual state based on matches
        if (matchCount > 0) {
            selectElement.style.borderColor = '#28a745';
        } else if (filterText) {
            selectElement.style.borderColor = '#ffc107';
        } else {
            selectElement.style.borderColor = '#e9ecef';
        }
    });
}

// Show error in owners interface
function showOwnersError() {
    const ownersSelect = document.getElementById('tagOwners');
    const ownershipTypeSelect = document.getElementById('tagOwnershipType');
    
    if (ownersSelect) {
        ownersSelect.innerHTML = '<option value="">Error loading owners</option>';
    }
    if (ownershipTypeSelect) {
        ownershipTypeSelect.innerHTML = '<option value="">Error loading ownership types</option>';
    }
}

// Helper function to copy text to clipboard
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(() => {
        // Show a brief success message
        const toast = document.createElement('div');
        toast.className = 'toast align-items-center text-white bg-success border-0 position-fixed';
        toast.style.cssText = 'top: 20px; right: 20px; z-index: 9999;';
        toast.innerHTML = `
            <div class="d-flex">
                <div class="toast-body">
                    <i class="fas fa-check me-2"></i>URN copied to clipboard
                </div>
                <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
            </div>
        `;
        document.body.appendChild(toast);
        const bsToast = new bootstrap.Toast(toast);
        bsToast.show();
        
        // Remove toast after it's hidden
        toast.addEventListener('hidden.bs.toast', () => {
            document.body.removeChild(toast);
        });
    }).catch(err => {
        console.error('Failed to copy text: ', err);
        alert('Failed to copy URN to clipboard');
    });
}

// Helper function to filter by owner (placeholder for future implementation)
function filterByOwner(ownerUrn) {
    console.log('Filter by owner:', ownerUrn);
    // TODO: Implement filtering functionality
    alert('Filter by owner functionality will be implemented soon.');
}

// Simplified function to populate the dropdowns
function populateSimpleDropdowns() {
    populateOwnersDropdown();
    populateOwnershipTypeDropdown();
}

// Populate the owners multi-select dropdown
function populateOwnersDropdown() {
    const select = document.getElementById('tagOwners');
    if (!select) return;
    
    select.innerHTML = '';
    
    // Add users
    if (usersAndGroupsCache.users.length > 0) {
        const usersGroup = document.createElement('optgroup');
        usersGroup.label = 'Users';
        
        usersAndGroupsCache.users.forEach(user => {
            const option = document.createElement('option');
            option.value = user.urn;
            option.textContent = user.display_name || user.username || user.urn;
            option.dataset.type = 'user';
            usersGroup.appendChild(option);
        });
        
        select.appendChild(usersGroup);
    }
    
    // Add groups
    if (usersAndGroupsCache.groups.length > 0) {
        const groupsGroup = document.createElement('optgroup');
        groupsGroup.label = 'Groups';
        
        usersAndGroupsCache.groups.forEach(group => {
            const option = document.createElement('option');
            option.value = group.urn;
            option.textContent = group.display_name || group.urn;
            option.dataset.type = 'group';
            groupsGroup.appendChild(option);
        });
        
        select.appendChild(groupsGroup);
    }
    
    if (usersAndGroupsCache.users.length === 0 && usersAndGroupsCache.groups.length === 0) {
        const option = document.createElement('option');
        option.value = '';
        option.textContent = 'No users or groups available';
        option.disabled = true;
        select.appendChild(option);
    }
}

// Populate the ownership types multi-select dropdown
function populateOwnershipTypeDropdown() {
    const select = document.getElementById('tagOwnershipType');
    if (!select) return;
    
    select.innerHTML = '';
    
    if (usersAndGroupsCache.ownership_types.length > 0) {
        usersAndGroupsCache.ownership_types.forEach(ownershipType => {
            const option = document.createElement('option');
            option.value = ownershipType.urn;
            option.textContent = ownershipType.name || ownershipType.urn;
            select.appendChild(option);
        });
    } else {
        const option = document.createElement('option');
        option.value = '';
        option.textContent = 'No ownership types available';
        option.disabled = true;
        select.appendChild(option);
    }
}

// Initialize Select2 for the ownership dropdowns
function initializeSelect2() {
    // Initialize Select2 for owners dropdown
    $('#tagOwners').select2({
        theme: 'bootstrap-5',
        width: '100%',
        placeholder: 'Search and select owners...',
        allowClear: true,
        dropdownParent: $('#createTagModal'),
        templateResult: formatOwnerOption,
        templateSelection: formatOwnerSelection
    });
    
}

// Simple function to remove a section
function removeSection(sectionId) {
    const section = document.getElementById(sectionId);
    if (section) {
        // Clean up Select2 instances in this section
        const select2Elements = section.querySelectorAll('.select2-hidden-accessible');
        select2Elements.forEach(element => {
            $(element).select2('destroy');
        });
        
        section.remove();
        updateRemoveButtons();
        
        // Check if container is now empty and hide ownership section if so
        const container = document.getElementById('ownership-sections-container');
        if (container && container.children.length === 0) {
            hideOwnershipSectionIfEmpty();
        }
        
        // Refresh all ownership type dropdowns to make removed options available again
        refreshAllOwnershipTypeDropdowns();
    }
}

// Add a new ownership section
function addOwnershipSection() {
    const container = document.getElementById('ownership-sections-container');
    if (!container) return;
    
    // Show the ownership section when adding the first owner
    showOwnershipSection();
    
    const sectionId = 'section-' + Date.now();
    
    const sectionHTML = `
        <div class="card mb-3" id="${sectionId}">
            <div class="card-header bg-light d-flex justify-content-between align-items-center">
                <h6 class="mb-0">
                    <i class="fas fa-crown me-2 text-primary"></i>
                    Ownership Section
                </h6>
                <button type="button" class="btn btn-sm btn-outline-danger" onclick="removeSection('${sectionId}')">
                    <i class="fas fa-times"></i>
                </button>
            </div>
            <div class="card-body">
                <div class="mb-3">
                    <label class="form-label">Owners <span class="text-danger">*</span></label>
                    <select class="form-select owners-select" name="owners[]" multiple required>
                        <!-- Options will be populated by JavaScript -->
                    </select>
                    <div class="form-text">Search and select multiple owners</div>
                </div>
                <div class="mb-3">
                    <label class="form-label">Ownership Type <span class="text-danger">*</span></label>
                    <select class="form-select ownership-type-select" name="ownership_types[]" required>
                        <option value="">Select ownership type...</option>
                    </select>
                    <div class="form-text">Select the ownership type for these owners</div>
                </div>
            </div>
        </div>
    `;
    
    container.insertAdjacentHTML('beforeend', sectionHTML);
    
    // Get the new section elements
    const newSection = document.getElementById(sectionId);
    const ownersSelect = newSection.querySelector('.owners-select');
    const ownershipTypeSelect = newSection.querySelector('.ownership-type-select');
    
    // Populate the dropdowns
    populateOwnersSelect(ownersSelect);
    populateOwnershipTypeSelect(ownershipTypeSelect);
    
    // Initialize Select2 for the owners dropdown
    $(ownersSelect).select2({
        theme: 'bootstrap-5',
        width: '100%',
        placeholder: 'Search and select owners...',
        allowClear: true,
        dropdownParent: $(newSection),
        templateResult: formatOwnerOption,
        templateSelection: formatOwnerSelection
    });
    
    // Add change listener to ownership type select to refresh other dropdowns
    ownershipTypeSelect.addEventListener('change', function() {
        refreshAllOwnershipTypeDropdowns();
    });
    
    updateRemoveButtons();
}

// Update visibility of remove buttons (hide if only one section)
function updateRemoveButtons() {
    const container = document.getElementById('ownership-sections-container');
    if (!container) return;
    
    const sections = container.querySelectorAll('.card');
    
    sections.forEach(section => {
        const removeButton = section.querySelector('button[onclick*="removeSection"]');
        if (removeButton) {
            removeButton.style.display = sections.length > 1 ? 'block' : 'none';
        }
    });
}

/**
 * Hide the ownership section if it's empty, show it if it has content
 */
function hideOwnershipSectionIfEmpty() {
    const container = document.getElementById('ownership-sections-container');
    const addButton = document.getElementById('add-ownership-section');
    const label = document.getElementById('ownership-label');
    const helpText = document.getElementById('ownership-help-text');
    
    if (!container || !addButton) return;
    
    const hasSections = container.children.length > 0;
    
    if (hasSections) {
        // Show the full ownership section
        if (label) label.style.display = 'block';
        container.style.display = 'block';
        if (helpText) helpText.style.display = 'block';
        
        // Update button text and style to normal
        addButton.innerHTML = '<i class="fas fa-plus me-1"></i> Add Owner';
        addButton.className = 'btn btn-sm btn-outline-primary mt-2';
    } else {
        // Hide everything except the "Add Owner" button
        if (label) label.style.display = 'none';
        container.style.display = 'none';
        if (helpText) helpText.style.display = 'none';
        
        // Show only the "Add Owner" button with simplified style
        addButton.style.display = 'block';
        addButton.textContent = '+ Add Owner';
        addButton.className = 'btn btn-sm btn-outline-primary';
    }
}

/**
 * Show the ownership section when adding owners
 */
function showOwnershipSection() {
    const container = document.getElementById('ownership-sections-container');
    const addButton = document.getElementById('add-ownership-section');
    const label = document.getElementById('ownership-label');
    const helpText = document.getElementById('ownership-help-text');
    
    if (!container || !addButton) return;
    
    // Show all elements
    if (label) label.style.display = 'block';
    container.style.display = 'block';
    if (helpText) helpText.style.display = 'block';
    
    // Update button text and style
    addButton.innerHTML = '<i class="fas fa-plus me-1"></i> Add Owner';
    addButton.className = 'btn btn-sm btn-outline-primary mt-2';
}

// Show error in owners interface
function showOwnersError() {
    const container = document.getElementById('ownership-sections-container');
    if (!container) return;
    
    container.innerHTML = `
        <div class="alert alert-danger">
            <i class="fas fa-exclamation-triangle me-2"></i>
            Failed to load users and groups. Please try refreshing the page.
        </div>
    `;
}

/**
 * Setup event listeners for tag action buttons
 */
/**
 * Get CSRF token from cookies or from hidden input field
 */
function getCsrfToken() {
    return document.querySelector('[name=csrfmiddlewaretoken]')?.value || 
           document.cookie.split('; ')
               .find(row => row.startsWith('csrftoken='))
               ?.split('=')[1];
}

function setupActionButtonListeners() {
    // Use event delegation since rows are dynamically created
    document.addEventListener('click', function(e) {
        // Get the closest row to find tag data
        const row = e.target.closest('tr[data-item]');
        if (!row) return;
        
        // Parse the minimal tag data from the row
        // First unescape any HTML entities we used to protect the JSON
        const rawData = row.dataset.item
            .replace(/&quot;/g, '"')
            .replace(/&apos;/g, "'");
        const minimalTagData = JSON.parse(rawData);
        if (!minimalTagData) return;
        
        // Look up the full tag data from the cache
        const cacheKey = minimalTagData.urn || minimalTagData.id;
        const tagData = window.tagDataCache[cacheKey];
        if (!tagData) {
            console.error("Could not find tag data in cache for:", minimalTagData);
            return;
        }
        
        // Check which button was clicked
        const clickedElement = e.target.closest('button');
        if (!clickedElement) return;

        if (clickedElement.classList.contains('edit-tag') || clickedElement.closest('.edit-tag')) {
            // Edit Tag button clicked
            console.log('Edit Tag clicked for tag:', tagData);
            editTag(tagData);
            e.preventDefault();
            e.stopPropagation();
        } else if (clickedElement.classList.contains('sync-to-datahub') || clickedElement.closest('.sync-to-datahub')) {
            // Sync to DataHub button clicked
            console.log('Sync to DataHub clicked for tag:', tagData);
            
            // Call the sync function
            syncTagToDataHub(tagData);
            e.preventDefault();
            e.stopPropagation();
        } else if (clickedElement.classList.contains('sync-to-local') || clickedElement.closest('.sync-to-local')) {
            // Sync to Local button clicked
            console.log('Sync to Local clicked for tag:', tagData);
            
            // Call the sync function directly - it will handle remote-only tags internally
            syncTagToLocal(tagData);
            e.preventDefault();
            e.stopPropagation();
        } else if (clickedElement.classList.contains('download-json') || clickedElement.closest('.download-json')) {
            // Download JSON button clicked
            console.log('Download JSON clicked for tag:', tagData);
            downloadTagJson(tagData);
            e.preventDefault();
            e.stopPropagation();
        } else if (clickedElement.classList.contains('add-to-staged') || clickedElement.closest('.add-to-staged')) {
            // Add to Staged Changes button clicked
            console.log('Add to Staged Changes clicked for tag:', tagData);
            
            // For staged changes, we still need a local tag first
            if (tagData.sync_status === 'REMOTE_ONLY') {
                // For remote-only tags, sync to local first, then add to staged changes
                syncTagToLocal(tagData);
            } else {
                addTagToStagedChanges(tagData);
            }
            e.preventDefault();
            e.stopPropagation();
        } else if (clickedElement.classList.contains('view-item') || clickedElement.closest('.view-item')) {
            // View Details button clicked
            console.log('View Details clicked for tag:', tagData);
            showTagDetails(tagData);
            e.preventDefault();
            e.stopPropagation();
        } else if (clickedElement.classList.contains('resync-tag') || clickedElement.closest('.resync-tag')) {
            // Resync Tag button clicked
            console.log('Resync Tag clicked for tag:', tagData);
            resyncTag(tagData);
            e.preventDefault();
            e.stopPropagation();
        } else if (clickedElement.classList.contains('push-to-datahub') || clickedElement.closest('.push-to-datahub')) {
            // Push to DataHub button clicked
            console.log('Push to DataHub clicked for tag:', tagData);
            syncTagToDataHub(tagData);
            e.preventDefault();
            e.stopPropagation();
        } else if (clickedElement.classList.contains('delete-remote-tag') || clickedElement.closest('.delete-remote-tag')) {
            // Delete Remote Tag button clicked
            console.log('Delete Remote Tag clicked for tag:', tagData);
            deleteRemoteTag(tagData);
            e.preventDefault();
            e.stopPropagation();
        // Delete Local Tag button is now handled by the form's native submission
        // No need for JavaScript handling here
        }
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
 * Get the actual database ID from a tag object
 * This is needed because the tag data might contain datahub_id instead of the actual database id
 * @param {Object} tagData - The tag data object
 * @returns {string|null} - The database ID or null if not found
 */
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
            // Add a small delay before refreshing to ensure backend processing is complete
            setTimeout(() => {
                // Refresh the data to show updated sync status
                if (typeof loadTagsData === 'function') {
                    loadTagsData();
                } else {
                    window.location.reload();
                }
            }, 1000); // 1 second delay
        } else {
            throw new Error(data.error || 'Unknown error occurred');
        }
    })
    .catch(error => {
        console.error('Error syncing tag to DataHub:', error);
        showNotification('error', `Error syncing tag: ${error.message}`);
    });
}

/**
 * Resync a single tag from DataHub
 * @param {Object} tag - The tag object
 */
function resyncTag(tag) {
    console.log('resyncTag called with:', tag);
    
    // Get tag data
    const tagData = tag.combined || tag;
    const tagId = getDatabaseId(tagData);
    
    if (!tagId) {
        console.error('Cannot resync tag without a database ID:', tagData);
        showNotification('error', 'Error resyncing tag: Missing tag database ID.');
        return;
    }
    
    if (!confirm(`Are you sure you want to resync "${tagData.name}" from DataHub? This will overwrite any local changes.`)) {
        return;
    }
    
    // Show loading notification
    showNotification('info', `Resyncing tag "${tagData.name}" from DataHub...`);
    
    // Make the API call to resync this tag from DataHub
    fetch(`/metadata/api/tags/${tagId}/resync/`, {
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
        console.log('Resync response:', data);
        if (data.success) {
            showNotification('success', data.message);
            // Add a small delay before refreshing to ensure backend processing is complete
            setTimeout(() => {
                // Refresh the data to show updated sync status
                if (typeof loadTagsData === 'function') {
                    loadTagsData();
                } else {
                    window.location.reload();
                }
            }, 1000); // 1 second delay
        } else {
            throw new Error(data.error || 'Unknown error occurred');
        }
    })
    .catch(error => {
        console.error('Error resyncing tag:', error);
        showNotification('error', `Error resyncing tag: ${error.message}`);
    });
}

/**
 * Delete a remote-only tag from DataHub
 * @param {Object} tag - The tag object
 */
function deleteRemoteTag(tag) {
    console.log('deleteRemoteTag called with:', tag);
    
    // Get tag data
    const tagData = tag.combined || tag;
    const tagUrn = tagData.urn;
    
    if (!tagUrn) {
        console.error('Cannot delete tag without a URN:', tagData);
        showNotification('error', 'Error deleting tag: Missing tag URN.');
        return;
    }
    
    if (!confirm(`Are you sure you want to delete "${tagData.name}" from DataHub? This action cannot be undone.`)) {
        return;
    }
    
    // Show loading notification
    showNotification('info', `Deleting tag "${tagData.name}" from DataHub...`);
    
    // Make the API call to delete this tag from DataHub
    fetch(`/metadata/api/tags/delete_remote/`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCsrfToken()
        },
        body: JSON.stringify({
            urn: tagUrn
        })
    })
    .then(response => {
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        return response.json();
    })
    .then(data => {
        console.log('Delete remote tag response:', data);
        if (data.success) {
            showNotification('success', data.message);
            // Add a small delay before refreshing to ensure backend processing is complete
            setTimeout(() => {
                // Refresh the data to show updated list
                if (typeof loadTagsData === 'function') {
                    loadTagsData();
                } else {
                    window.location.reload();
                }
            }, 1000); // 1 second delay
        } else {
            throw new Error(data.error || 'Unknown error occurred');
        }
    })
    .catch(error => {
        console.error('Error deleting remote tag:', error);
        showNotification('error', `Error deleting tag: ${error.message}`);
    });
}

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
 * Edit an existing tag
 * @param {Object} tag - The tag object to edit
 */
function editTag(tag) {
    console.log('editTag called with:', tag);
    
    // Get tag data
    const tagData = tag.combined || tag;
    const tagId = getDatabaseId(tagData);
    
    if (!tagId) {
        console.error('Cannot edit tag without a database ID:', tagData);
        showNotification('error', 'Error editing tag: Missing tag database ID.');
        return;
    }
    
    // Ensure users and groups are loaded
    if (usersAndGroupsCache.users.length === 0 && usersAndGroupsCache.groups.length === 0) {
        loadUsersAndGroups();
    }
    
    // Populate the form with existing data
    document.getElementById('tag-name').value = tagData.name || '';
    document.getElementById('tag-description').value = tagData.description || '';
    document.getElementById('tag-color').value = tagData.color || '#0d6efd';
    

    
    // Clear existing ownership sections and setup interface
    setupOwnershipInterface();
    
    // Wait for users/groups to load if not already loaded, then populate ownership data
    const populateOwnership = () => {
        const ownershipData = tagData.ownership_data || tagData.ownership;
        console.log('Populating ownership for edit mode:', ownershipData);
        
        // Clear existing ownership sections
        const ownershipContainer = document.getElementById('ownership-sections-container');
        if (ownershipContainer) {
            ownershipContainer.innerHTML = '';
        }
        
        if (ownershipData && ownershipData.owners && ownershipData.owners.length > 0) {
            // Group owners by ownership type to create separate sections
            const ownersByType = {};
            
            ownershipData.owners.forEach(ownerInfo => {
                let ownerUrn, ownershipTypeUrn;
                
                // Handle different data structures
                if (ownerInfo.owner_urn && ownerInfo.ownership_type_urn) {
                    // Local storage format
                    ownerUrn = ownerInfo.owner_urn;
                    ownershipTypeUrn = ownerInfo.ownership_type_urn;
                } else if (ownerInfo.owner && ownerInfo.ownershipType) {
                    // GraphQL format
                    ownerUrn = ownerInfo.owner.urn;
                    ownershipTypeUrn = ownerInfo.ownershipType.urn;
                } else if (ownerInfo.ownerUrn && ownerInfo.type) {
                    // Remote-only format
                    ownerUrn = ownerInfo.ownerUrn;
                    ownershipTypeUrn = ownerInfo.type;
                } else {
                    console.warn('Unrecognized owner info format:', ownerInfo);
                    return;
                }
                
                if (!ownersByType[ownershipTypeUrn]) {
                    ownersByType[ownershipTypeUrn] = [];
                }
                ownersByType[ownershipTypeUrn].push(ownerUrn);
            });
            
            // Create ownership sections for each type
            Object.entries(ownersByType).forEach(([ownershipTypeUrn, ownerUrns]) => {
                console.log(`Creating ownership section for type ${ownershipTypeUrn} with owners:`, ownerUrns);
                
                // Add a new ownership section
                addOwnershipSection();
                
                // Get the last added section (the one we just created)
                const sections = document.querySelectorAll('#ownership-sections-container .card');
                const section = sections[sections.length - 1];
                
                if (section) {
                    // Set the ownership type
                    const ownershipTypeSelect = section.querySelector('.ownership-type-select');
                    if (ownershipTypeSelect) {
                        ownershipTypeSelect.value = ownershipTypeUrn;
                        
                        // If the ownership type is not in the current list, add it with a warning
                        if (!ownershipTypeSelect.querySelector(`option[value="${ownershipTypeUrn}"]`)) {
                            const missingOption = document.createElement('option');
                            missingOption.value = ownershipTypeUrn;
                            const typeName = ownershipTypeUrn.split(':').pop() || 'Unknown Type';
                            missingOption.textContent = `⚠️ ${typeName} (not in current DataHub)`;
                            missingOption.className = 'text-warning';
                            missingOption.selected = true;
                            ownershipTypeSelect.appendChild(missingOption);
                            console.log('Added missing ownership type:', ownershipTypeUrn);
                        }
                    }
                    
                    // Set the owners using Select2
                    const ownersSelect = section.querySelector('.owners-select');
                    if (ownersSelect && ownerUrns.length > 0) {
                        console.log('Setting owners in Select2:', ownerUrns);
                        
                        // Check for missing owners and add them to the dropdown
                        ownerUrns.forEach(ownerUrn => {
                            const existingOption = ownersSelect.querySelector(`option[value="${ownerUrn}"]`);
                            if (!existingOption) {
                                // Add missing owner with warning
                                const missingOption = document.createElement('option');
                                missingOption.value = ownerUrn;
                                const ownerName = ownerUrn.split(':').pop() || ownerUrn;
                                const isUser = ownerUrn.includes(':corpuser:');
                                const icon = isUser ? '👤' : '👥';
                                missingOption.textContent = `⚠️ ${icon} ${ownerName} (not in current DataHub)`;
                                missingOption.className = 'text-warning';
                                missingOption.dataset.type = isUser ? 'user' : 'group';
                                missingOption.dataset.urn = ownerUrn;
                                missingOption.title = `⚠️ This ${isUser ? 'user' : 'group'} is not available in the current DataHub connection. URN: ${ownerUrn}`;
                                ownersSelect.appendChild(missingOption);
                                console.log('Added missing owner:', ownerUrn);
                            }
                        });
                        
                        // Ensure Select2 is initialized first
                        if (!$(ownersSelect).hasClass('select2-hidden-accessible')) {
                            console.log('Select2 not initialized yet, waiting...');
                            // Select2 not initialized yet, wait a bit
                            setTimeout(() => {
                                console.log('Setting owners after delay');
                                $(ownersSelect).val(ownerUrns).trigger('change');
                            }, 100);
                        } else {
                            console.log('Select2 already initialized, setting owners immediately');
                            $(ownersSelect).val(ownerUrns).trigger('change');
                        }
                    }
                }
            });
        } else {
            // No ownership data, hide the ownership section
            hideOwnershipSectionIfEmpty();
        }
    };
    
    // If users/groups are already loaded, populate immediately
    if (usersAndGroupsCache.users.length > 0 || usersAndGroupsCache.groups.length > 0) {
        setTimeout(populateOwnership, 100); // Small delay to ensure Select2 is initialized
    } else {
        // Wait for users/groups to load
        const checkAndPopulate = () => {
            if (usersAndGroupsCache.users.length > 0 || usersAndGroupsCache.groups.length > 0) {
                setTimeout(populateOwnership, 100);
            } else {
                setTimeout(checkAndPopulate, 100);
            }
        };
        checkAndPopulate();
    }
    
    // Update the form to be in edit mode
    const form = document.getElementById('tag-form');
    form.dataset.editMode = 'true';
    form.dataset.tagId = tagId;
    
    // Update modal title and button text
    document.querySelector('#createTagModal .modal-title').textContent = 'Edit Tag';
    document.querySelector('#createTagModal .btn-primary').textContent = 'Update Tag';
    
    // Show the modal
    const modal = new bootstrap.Modal(document.getElementById('createTagModal'));
    modal.show();
}

/**
 * Reset the create/edit tag modal to default state
 */
function resetTagModal() {
    // Reset form
    const form = document.getElementById('tag-form');
    form.reset();
    form.removeAttribute('data-edit-mode');
    form.removeAttribute('data-tag-id');
    

    
    // Reset modal title and button text
    document.querySelector('#createTagModal .modal-title').textContent = 'Create New Tag';
    document.querySelector('#createTagModal .btn-primary').textContent = 'Create Tag';
    
    // Clear ownership sections container
    const ownershipContainer = document.getElementById('ownership-sections-container');
    if (ownershipContainer) {
        ownershipContainer.innerHTML = '';
    }
    
    // Hide ownership section by default
    hideOwnershipSectionIfEmpty();
}

/**
 * Handle form submission for both create and edit modes
 */
function handleTagFormSubmission(event) {
    event.preventDefault();
    
    const form = event.target;
    const isEditMode = form.dataset.editMode === 'true';
    const tagId = form.dataset.tagId;
    
    // Collect form data
    const formData = new FormData(form);
    
    // Collect ownership data from multiple sections
    const ownershipSections = document.querySelectorAll('#ownership-sections-container .card');
    const owners = [];
    
    console.log('Found ownership sections:', ownershipSections.length);
    
    ownershipSections.forEach((section, sectionIndex) => {
        const ownersSelect = section.querySelector('.owners-select');
        const ownershipTypeSelect = section.querySelector('.ownership-type-select');
        
        console.log(`Section ${sectionIndex}:`, {
            ownersSelect: ownersSelect ? 'found' : 'not found',
            ownershipTypeSelect: ownershipTypeSelect ? 'found' : 'not found',
            ownershipTypeValue: ownershipTypeSelect ? ownershipTypeSelect.value : 'N/A'
        });
        
        if (ownersSelect && ownershipTypeSelect && ownershipTypeSelect.value) {
            // Get selected owners from Select2
            const selectedOwners = $(ownersSelect).val() || [];
            console.log(`Section ${sectionIndex} selected owners:`, selectedOwners);
            
            selectedOwners.forEach(ownerUrn => {
                if (ownerUrn) {
                    owners.push({
                        owner_urn: ownerUrn,
                        ownership_type_urn: ownershipTypeSelect.value
                    });
                    console.log(`Added owner: ${ownerUrn} with type: ${ownershipTypeSelect.value}`);
                }
            });
        }
    });
    
    console.log('Total owners collected:', owners);
    
    // Add ownership data to form
    owners.forEach((owner, index) => {
        formData.append(`owners[${index}]`, owner.owner_urn);
        formData.append(`ownership_types[${index}]`, owner.ownership_type_urn);
        console.log(`Added to form: owners[${index}]=${owner.owner_urn}, ownership_types[${index}]=${owner.ownership_type_urn}`);
    });
    
    // Determine the URL based on mode
    const url = isEditMode ? `/metadata/tags/${tagId}/` : '/metadata/tags/';
    const method = isEditMode ? 'POST' : 'POST'; // Both use POST, but edit will be handled differently
    
    if (isEditMode) {
        // For edit mode, add a hidden field to indicate this is an update
        formData.append('_method', 'PUT');
        
        // If this is a synced tag being edited, update sync status to MODIFIED
        const originalTagData = window.tagDataCache[Object.keys(window.tagDataCache).find(key => 
            window.tagDataCache[key].id == tagId || window.tagDataCache[key].datahub_id == tagId
        )];
        
        if (originalTagData && originalTagData.sync_status === 'SYNCED') {
            formData.append('sync_status', 'MODIFIED');
            console.log('Updating sync status to MODIFIED for synced tag');
        }
    }
    
    // Show loading state
    const submitButton = form.querySelector('.btn-primary');
    const originalText = submitButton.textContent;
    submitButton.disabled = true;
    submitButton.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i>Saving...';
    
    // Debug: Log form data
    console.log('Form submission details:');
    console.log('URL:', url);
    console.log('Method:', method);
    console.log('Form data:');
    for (let [key, value] of formData.entries()) {
        console.log(`  ${key}: ${value}`);
    }
    
    // Submit the form
    fetch(url, {
        method: method,
        body: formData,
        headers: {
            'X-CSRFToken': getCsrfToken(),
            'X-Requested-With': 'XMLHttpRequest'
        }
    })
    .then(response => {
        // Check if response is JSON (for AJAX responses) or HTML (for form redirects)
        const contentType = response.headers.get('content-type');
        
        if (!response.ok) {
            // For error responses, try to get the error message
            if (contentType && contentType.includes('application/json')) {
                return response.json().then(errorData => {
                    throw new Error(errorData.error || errorData.message || `HTTP ${response.status}: ${response.statusText}`);
                });
            } else {
                // Try to parse as text to get any error message
                return response.text().then(errorText => {
                    throw new Error(errorText || `HTTP ${response.status}: ${response.statusText}`);
                });
            }
        }
        
        // For successful responses, handle JSON or HTML
        if (contentType && contentType.includes('application/json')) {
            return response.json();
        } else {
            return response.text();
        }
    })
    .then(data => {
        // Handle the response data
        if (typeof data === 'object' && data.success !== undefined) {
            // JSON response
            if (data.success) {
                // Success - close modal and refresh data
                const modal = bootstrap.Modal.getInstance(document.getElementById('createTagModal'));
                modal.hide();
                
                showNotification('success', data.message || `Tag ${isEditMode ? 'updated' : 'created'} successfully!`);
                
                // Refresh the data
                if (typeof loadTagsData === 'function') {
                    loadTagsData();
                } else {
                    window.location.reload();
                }
            } else {
                throw new Error(data.error || 'Unknown error occurred');
            }
        } else {
            // HTML response (redirect) - assume success
            const modal = bootstrap.Modal.getInstance(document.getElementById('createTagModal'));
            modal.hide();
            
            showNotification('success', `Tag ${isEditMode ? 'updated' : 'created'} successfully!`);
            
            // Refresh the data
            if (typeof loadTagsData === 'function') {
                loadTagsData();
            } else {
                window.location.reload();
            }
        }
    })
    .catch(error => {
        console.error('Error saving tag:', error);
        showNotification('error', `Error ${isEditMode ? 'updating' : 'creating'} tag: ${error.message}`);
    })
    .finally(() => {
        // Reset button state
        submitButton.disabled = false;
        submitButton.textContent = originalText;
    });
}
// Global action functions
function resyncAll() {
    if (confirm('Are you sure you want to resync ALL tags from DataHub? This will update all local data with current remote data.')) {
        showNotification('success', 'Starting resync of all tags from DataHub...');
        
        fetch('/metadata/api/tags/resync_all/', {
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
            console.log('Resync all response:', data);
            if (data.success) {
                showNotification('success', data.message);
                setTimeout(() => {
                    if (typeof loadTagsData === 'function') {
                        loadTagsData();
                    } else {
                        window.location.reload();
                    }
                }, 2000); // 2 second delay for all tags
            } else {
                throw new Error(data.error || 'Unknown error occurred');
            }
        })
        .catch(error => {
            console.error('Error in resync all:', error);
            showNotification('error', `Error resyncing all tags: ${error.message}`);
        });
    }
}

function exportAll() {
    showNotification('success', 'Starting export of all tags...');
    
    fetch('/metadata/api/tags/export_all/', {
        method: 'GET',
        headers: {
            'X-CSRFToken': getCsrfToken()
        }
    })
    .then(response => {
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        // Get filename from Content-Disposition header or use default
        const contentDisposition = response.headers.get('Content-Disposition');
        let filename = 'all_tags_export.json';
        if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename="?([^"]+)"?/);
            if (filenameMatch) {
                filename = filenameMatch[1];
            }
        }
        
        return response.blob().then(blob => ({ blob, filename }));
    })
    .then(({ blob, filename }) => {
        // Create download link
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
        
        showNotification('success', 'All tags exported successfully!');
    })
    .catch(error => {
        console.error('Error exporting all tags:', error);
        showNotification('error', `Error exporting tags: ${error.message}`);
    });
}

function addAllToStagedChanges() {
    if (confirm('Are you sure you want to add ALL tags to staged changes? This will create MCP files for all tags in the current environment.')) {
        showNotification('success', 'Starting to add all tags to staged changes...');
        
        // Get current environment and mutation from global state or settings
        const currentEnvironment = window.currentEnvironment || { name: 'dev' };
        const mutationName = currentEnvironment.mutation_name || null;
        
        fetch('/metadata/api/tags/add_all_to_staged_changes/', {
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
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            return response.json();
        })
        .then(data => {
            console.log('Add all to staged changes response:', data);
            if (data.success) {
                showNotification('success', data.message);
                if (data.files_created && data.files_created.length > 0) {
                    console.log('Created files:', data.files_created);
                }
            } else {
                throw new Error(data.error || 'Unknown error occurred');
            }
        })
        .catch(error => {
            console.error('Error adding all tags to staged changes:', error);
            showNotification('error', `Error adding tags to staged changes: ${error.message}`);
        });
    }
}

function showImportModal() {
    const modal = new bootstrap.Modal(document.getElementById('importJsonModal'));
    modal.show();
}

function handleImportFormSubmission(event) {
    event.preventDefault();
    
    const form = event.target;
    const formData = new FormData(form);
    
    const submitButton = form.querySelector('button[type="submit"]');
    const originalText = submitButton.textContent;
    submitButton.disabled = true;
    submitButton.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i>Importing...';
    
    fetch('/metadata/api/tags/import_json/', {
        method: 'POST',
        body: formData,
        headers: {
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
        console.log('Import response:', data);
        if (data.success) {
            showNotification('success', data.message);
            
            // Close modal
            const modal = bootstrap.Modal.getInstance(document.getElementById('importJsonModal'));
            modal.hide();
            
            // Reset form
            form.reset();
            
            // Refresh data
            setTimeout(() => {
                if (typeof loadTagsData === 'function') {
                    loadTagsData();
                } else {
                    window.location.reload();
                }
            }, 1000);
        } else {
            throw new Error(data.error || 'Unknown error occurred');
        }
    })
    .catch(error => {
        console.error('Error importing tags:', error);
        showNotification('error', `Error importing tags: ${error.message}`);
    })
    .finally(() => {
        submitButton.disabled = false;
        submitButton.textContent = originalText;
    });
}

// Setup event listeners for import form
document.addEventListener('DOMContentLoaded', function() {
    const importForm = document.getElementById('import-json-form');
    if (importForm) {
        importForm.addEventListener('submit', handleImportFormSubmission);
    }
}); 