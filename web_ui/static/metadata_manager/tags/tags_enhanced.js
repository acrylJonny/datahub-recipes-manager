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

// User, group, and ownership type cache
let usersAndGroupsCache = {
    users: [],
    groups: [],
    ownership_types: [],
    lastFetched: null,
    cacheExpiry: 5 * 60 * 1000 // 5 minutes
};

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
    
    safeJsonStringify: function(obj, maxLength = 1000) {
        try {
            const jsonString = JSON.stringify(obj);
            return jsonString.length > maxLength ? jsonString.substring(0, maxLength) + '...' : jsonString;
        } catch (error) {
            console.error('Error stringifying object:', error);
            return '{}';
        }
    },
    
    safeJsonParse: function(jsonString) {
        try {
            return JSON.parse(jsonString);
        } catch (error) {
            console.error('Error parsing JSON:', error);
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
    
    // Load users and groups when create modal is opened
    document.getElementById('createTagModal').addEventListener('show.bs.modal', function() {
        loadUsersAndGroups().then(() => {
            // Clear container - don't add any initial sections
            const container = document.getElementById('ownership-sections-container');
            container.innerHTML = '';
        });
    });
    
    // Clean up Select2 when modal is closed
    document.getElementById('createTagModal').addEventListener('hidden.bs.modal', function() {
        const container = document.getElementById('ownership-sections-container');
        const select2Elements = container.querySelectorAll('.select2-hidden-accessible');
        select2Elements.forEach(element => {
            $(element).select2('destroy');
        });
    });
    
    // Setup add button listener
    document.addEventListener('click', function(e) {
        if (e.target && (e.target.id === 'add-ownership-section' || e.target.closest('#add-ownership-section'))) {
            e.preventDefault();
            addOwnershipSection();
        }
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
    // This will be called once on page load to set up initial handlers
    // The actual checkbox handlers are attached in displayTabContent after the table is rendered
    
    // Setup select-all checkboxes
    ['synced', 'local', 'remote'].forEach(tab => {
        // We'll attach these handlers after the table is rendered
        document.addEventListener('click', function(e) {
            // Check if a checkbox was clicked
            if (e.target && e.target.classList.contains('item-checkbox')) {
                updateBulkActionVisibility(tab);
            }
            
            // Check if select-all checkbox was clicked
            if (e.target && e.target.classList.contains('select-all-checkbox')) {
                const tabType = e.target.closest('.tab-pane').id.split('-')[0];
                const checkboxes = document.querySelectorAll(`#${tabType}-content .item-checkbox`);
                checkboxes.forEach(checkbox => {
                    checkbox.checked = e.target.checked;
                });
                updateBulkActionVisibility(tabType);
        }
        });
    });
}

function updateBulkActionVisibility(tab) {
    console.log(`Updating bulk action visibility for ${tab} tab`);
    
    const checkboxes = document.querySelectorAll(`#${tab}-content .item-checkbox:checked`);
    console.log(`Found ${checkboxes.length} checked checkboxes`);
    
    const bulkActions = document.getElementById(`${tab}-bulk-actions`);
    const selectedCount = document.getElementById(`${tab}-selected-count`);
    
    if (!bulkActions || !selectedCount) {
        console.error(`Could not find bulk actions or selected count elements for ${tab} tab`);
        return;
    }
    
    if (checkboxes.length > 0) {
        bulkActions.classList.add('show');
        selectedCount.textContent = checkboxes.length;
        console.log(`Showing bulk actions with ${checkboxes.length} selected items`);
    } else {
        bulkActions.classList.remove('show');
        selectedCount.textContent = '0';
        console.log('Hiding bulk actions - no items selected');
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
                        if ((tagData.owners_count || 0) > 0) passesFilter = true;
                        break;
                    case 'deprecated':
                        if (tagData.deprecated || tagData.properties?.deprecated) passesFilter = true;
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
    
    fetch('/metadata/tags/remote-data/')
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            // Apply global sanitization to prevent issues with long descriptions and malformed data
            tagsData = DataUtils.sanitizeApiResponse(data.data);
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
    document.getElementById('deprecated-tags').textContent = stats.deprecated_tags || 0;
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
                // Use safe JSON parsing
                const itemData = DataUtils.safeJsonParse(row.dataset.item);
                if (itemData) {
                    // Ensure cache is loaded before showing details
                    if (usersAndGroupsCache.users.length === 0) {
                        loadUsersAndGroups().then(() => showTagDetails(itemData));
                    } else {
                        showTagDetails(itemData);
                    }
                } else {
                    console.error('Failed to parse item data from row');
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
    
    return `
        <div class="table-responsive">
            <table class="table table-hover mb-0">
                <thead>
                    <tr>
                        <th width="40">
                                                         <input type="checkbox" class="form-check-input select-all-checkbox" id="selectAll${tabType.charAt(0).toUpperCase() + tabType.slice(1)}">
                        </th>
                        <th class="sortable-header" data-sort="name">Name</th>
                        <th>Description</th>
                        <th class="sortable-header" data-sort="color">Color</th>
                        <th class="sortable-header" data-sort="owners">Owners</th>
                        <th class="sortable-header" data-sort="deprecated">Deprecated</th>
                                                        <th>URN</th>
                        <th>Actions</th>
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
    
    const name = tagData.properties?.name || tagData.name || 'Unnamed Tag';
    const description = tagData.properties?.description || tagData.description || '';
    const color = tagData.properties?.colorHex || tagData.color || '#6c757d';
    const urn = tagData.urn || '';
    
    // Get owners information
    const owners = tagData.owner_names || [];
    const ownersCount = tagData.owners_count || owners.length || 0;
    const ownersTitle = owners.length > 0 ? owners.join(', ') : 'No owners';
    
    // Check if deprecated (this would come from DataHub deprecation info)
    const isDeprecated = tagData.deprecated || tagData.properties?.deprecated || false;
    
    // Use safe JSON stringify for data attributes
    const safeJsonData = DataUtils.safeJsonStringify(sanitizedTag);
    
    // Get custom action buttons
    const customActionButtons = getActionButtons(tagData, tabType);
    
    // Make sure we have an ID for the checkbox value
    const checkboxValue = tagData.id || tagData.urn || '';
    
    return `
        <tr data-item='${safeJsonData}'>
            <td>
                <input type="checkbox" class="form-check-input item-checkbox" value="${checkboxValue}" id="checkbox-${tabType}-${checkboxValue}">
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
                    <code class="small">${color}</code>
                </div>
            </td>
            <td title="${ownersTitle}" class="text-center">
                ${ownersCount > 0 ? `<i class="fas fa-users text-info me-1"></i><span class="badge bg-info">${ownersCount}</span>` : '<span class="text-muted">None</span>'}
            </td>
            <td>
                ${isDeprecated ? '<span class="badge bg-warning">Yes</span>' : '<span class="badge bg-success">No</span>'}
            </td>
            <td title="${escapeHtml(urn)}">
                <code class="small">${escapeHtml(urn)}</code>
            </td>
            <td>
                <div class="btn-group action-buttons" role="group">
                    <!-- View entity button -->
                    <button type="button" class="btn btn-sm btn-outline-primary view-item" 
                            title="View Details">
                        <i class="fas fa-eye"></i>
                    </button>
                    
                    <!-- View in DataHub button if applicable -->
                    ${urn && !urn.includes('local:') ? `
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
    if (hasSearch) {
    return `
            <tr>
                <td colspan="8" class="text-center py-4 text-muted">
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
            <td colspan="8" class="text-center py-4 text-muted">
                <i class="fas fa-tag fa-2x mb-2"></i><br>
                ${emptyStates[tabType]}
            </td>
        </tr>
    `;
}

function sanitizeDataForAttribute(item, maxDescriptionLength = 200) {
    // Use global utility for consistent data sanitization
    return DataUtils.createDisplaySafeItem(item, {
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
    
    let actionButtons = '';
    
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
        </div>
    `;
    
    return paginationHTML;
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
            return (tagData.properties?.colorHex || tagData.color || '').toLowerCase();
        case 'owners':
            return tagData.owners_count || (tagData.owner_names || []).length || 0;
        case 'deprecated':
            return tagData.deprecated || tagData.properties?.deprecated ? 1 : 0;
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
    console.log('Showing tag details:', tag);
    
    // For synced tags, we need to check both local and remote data
    const tagData = tag.combined || tag;
    
    // For remote-only tags, use the remote data directly
    const isRemoteOnly = tagData.sync_status === 'REMOTE_ONLY';
    
    // Log the data structure to help with debugging
    console.log('Tag data structure:', {
        name: tagData.name,
        properties: tagData.properties,
        sync_status: tagData.sync_status,
        urn: tagData.urn,
        hasOwnership: !!(tagData.ownership || tagData.ownership_data)
    });
    
    // Update modal with tag data - use original descriptions if available
    // For remote tags, prioritize properties.name over name
    let displayName = '';
    if (isRemoteOnly || tagData.sync_status === 'SYNCED') {
        // Remote tags store name in properties.name
        displayName = tagData.properties?.name || tagData.name || 'Unnamed Tag';
    } else {
        // Local tags store name directly
        displayName = tagData.name || tagData.properties?.name || 'Unnamed Tag';
    }
    document.getElementById('modal-tag-name').textContent = displayName;
    
    // Handle description similarly
    let displayDescription = '';
    if (isRemoteOnly || tagData.sync_status === 'SYNCED') {
        // Remote tags store description in properties.description
        displayDescription = tagData.properties?.description || tagData.description || 'No description';
    } else {
        // Local tags store description directly
        displayDescription = tagData.description || tagData.properties?.description || 'No description';
    }
    document.getElementById('modal-tag-description').textContent = displayDescription;
    
    // Handle color
    const colorSwatch = document.getElementById('modal-tag-color-swatch');
    let displayColor = '';
    if (isRemoteOnly || tagData.sync_status === 'SYNCED') {
        // Remote tags store color in properties.colorHex
        displayColor = tagData.properties?.colorHex || tagData.color || '#6c757d';
    } else {
        // Local tags store color directly
        displayColor = tagData.color || tagData.properties?.colorHex || '#6c757d';
    }
    colorSwatch.style.backgroundColor = displayColor;
    document.getElementById('modal-tag-color').textContent = displayColor;
    
    // Update deprecation status
    const isDeprecated = tagData.deprecated || tagData.properties?.deprecated || false;
    const deprecatedElement = document.getElementById('modal-tag-deprecated');
    deprecatedElement.textContent = isDeprecated ? 'Yes' : 'No';
    deprecatedElement.className = `badge ${isDeprecated ? 'bg-warning' : 'bg-success'}`;
    
    // Update URN
    document.getElementById('modal-tag-urn').textContent = tagData.urn || '';
    
    // Update owners information split by ownership type
    const ownersListElement = document.getElementById('modal-owners-list');
    
    // Load users/groups cache if needed
    if (usersAndGroupsCache.users.length === 0) {
        loadUsersAndGroups();
    }
    
    // Check for ownership data from GraphQL (remote tags) or local storage
    const ownershipData = tagData.ownership || tagData.ownership_data;
    console.log('Ownership data:', ownershipData);
    
    if (ownershipData && ownershipData.owners && ownershipData.owners.length > 0) {
        // Group owners by ownership type
        const ownersByType = {};
        
        ownershipData.owners.forEach(ownerInfo => {
            console.log('Processing owner info:', ownerInfo);
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
            } else {
                console.log('Skipping invalid owner entry:', ownerInfo);
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
    
    // Update last modified info if available
    const lastModifiedElement = document.getElementById('modal-last-modified');
    if (lastModifiedElement) {
        if (tagData.last_synced || tagData.last_modified) {
            const lastModified = tagData.last_modified || tagData.last_synced;
            const formattedDate = new Date(lastModified).toLocaleString();
            lastModifiedElement.innerHTML = `
                <div class="text-muted small">
                    <i class="fas fa-clock me-1"></i>
                    Last modified: ${formattedDate}
                </div>
            `;
        } else {
            lastModifiedElement.innerHTML = '';
        }
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
    
    // Update raw JSON data using safe JSON handling
    const rawJsonElement = document.getElementById('modal-raw-json');
    const safeJsonString = DataUtils.safeJsonStringify(tagData, 1000); // Allow longer strings for raw display
    rawJsonElement.innerHTML = `<code>${escapeHtml(JSON.stringify(DataUtils.safeJsonParse(safeJsonString), null, 2))}</code>`;
    
    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('tagViewModal'));
    modal.show();
}

// Bulk action functions
function bulkResyncTags(tabType) {
    const selectedTags = getSelectedTags(tabType);
    if (selectedTags.length === 0) {
        alert('Please select tags to resync.');
        return;
    }
    
    if (confirm(`Are you sure you want to resync ${selectedTags.length} tag(s)?`)) {
        console.log(`Bulk resync ${selectedTags.length} tags for ${tabType}:`, selectedTags);
        // TODO: Implement bulk resync API call
        alert('Bulk resync functionality will be implemented soon.');
    }
}

function bulkPushTags(tabType) {
    const selectedTags = getSelectedTags(tabType);
    if (selectedTags.length === 0) {
        alert('Please select tags to push.');
        return;
    }
    
    if (confirm(`Are you sure you want to push ${selectedTags.length} tag(s) to DataHub?`)) {
        console.log(`Bulk push ${selectedTags.length} tags for ${tabType}:`, selectedTags);
        // TODO: Implement bulk push API call
        alert('Bulk push functionality will be implemented soon.');
    }
}

function bulkAddToPR(tabType) {
    const selectedTags = getSelectedTags(tabType);
    if (selectedTags.length === 0) {
        showNotification('error', 'Please select tags to add to staged changes.');
        return;
    }
    
    if (confirm(`Are you sure you want to add ${selectedTags.length} tag(s) to staged changes?`)) {
        console.log(`Bulk add ${selectedTags.length} tags to staged changes for ${tabType}:`, selectedTags);
        
        // Show loading indicator
        showNotification('success', `Starting to add ${selectedTags.length} tags to staged changes...`);
        
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
            if (index >= selectedTags.length) {
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
            
            const tag = selectedTags[index];
            
            // For synced tags, get ID from the combined or local property
            let tagId = null;
            if (tabType === 'synced') {
                if (tag.combined && tag.combined.id) {
                    tagId = tag.combined.id;
                } else if (tag.local && tag.local.id) {
                    tagId = tag.local.id;
                } else {
                    tagId = tag.id;
                }
            } else {
                // For other tabs, use the direct ID
                tagId = tag.id;
            }
            
            console.log(`Processing tag with ID: ${tagId}, name: ${tag.name || tag.combined?.name || 'Unknown'}`);
            
            if (!tagId) {
                console.error('Cannot add tag to staged changes without an ID:', tag);
                errorCount++;
                processedCount++;
                processNextTag(index + 1);
                return;
            }
            
            // Make the API call to add this tag to staged changes
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
                    throw new Error(`Failed to add tag ${tag.name || tag.combined?.name || tag.urn} to staged changes`);
                }
                return response.json();
            })
            .then(data => {
                const tagName = tag.name || tag.combined?.name || tag.urn;
                console.log(`Successfully added tag to staged changes: ${tagName}`);
                successCount++;
                processedCount++;
                
                // Track created files
                if (data.files_created && data.files_created.length > 0) {
                    createdFiles = [...createdFiles, ...data.files_created];
                }
                
                // Update progress
                if (processedCount % 5 === 0 || processedCount === selectedTags.length) {
                    showNotification('success', `Progress: ${processedCount}/${selectedTags.length} tags processed`);
                }
                
                // Process the next tag
                processNextTag(index + 1);
            })
            .catch(error => {
                const tagName = tag.name || tag.combined?.name || tag.urn;
                console.error(`Error adding tag ${tagName} to staged changes:`, error);
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
            
            // Extract the ID from the appropriate location based on tag type
            let tagId = null;
            
            // For synced tags, check multiple locations
            if (tabType === 'synced') {
                if (tag.combined && tag.combined.id) {
                    tagId = tag.combined.id;
                } else if (tag.local && tag.local.id) {
                    tagId = tag.local.id;
                } else {
                    tagId = tag.id;
                }
            } else {
                // For other tags, use the direct ID
                tagId = tag.id;
            }
            
            console.log(`Deleting tag with ID: ${tagId}, name: ${tag.name || tag.combined?.name || 'Unknown'}`);
            
            if (!tagId) {
                console.error('Cannot delete tag without an ID:', tag);
                errorCount++;
                processedCount++;
                processNextTag(index + 1);
                return;
            }
            
            // Make the API call to delete this tag using DELETE method
            fetch(`/metadata/tags/${tagId}/delete/`, {
                method: 'DELETE',
                headers: {
                    'X-CSRFToken': getCsrfToken(),
                    'Content-Type': 'application/json'
                }
            })
            .then(response => {
                if (!response.ok) {
                    throw new Error(`Failed to delete tag ${tag.name || tag.urn}`);
                }
                return response.json();
            })
            .then(data => {
                console.log(`Successfully deleted tag: ${tag.name || tag.urn}`);
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
                console.error(`Error deleting tag ${tag.name || tag.urn}:`, error);
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
            
            // We need the ID for the API call
            const tagId = tag.id;
            if (!tagId) {
                console.error('Cannot sync tag without an ID:', tag);
                errorCount++;
                processedCount++;
                processNextTag(index + 1);
                return;
            }
            
            // Make the API call to sync this tag
            fetch(`/api/metadata_manager/tags/${tagId}/sync_to_local/`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': getCsrfToken()
                }
            })
            .then(response => {
                if (!response.ok) {
                    throw new Error(`Failed to sync tag ${tag.name || tag.urn}`);
                }
                return response.json();
            })
            .then(data => {
                console.log(`Successfully synced tag: ${tag.name || tag.urn}`);
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
                console.error(`Error syncing tag ${tag.name || tag.urn}:`, error);
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
    // First check if there are any checked checkboxes
    const checkboxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    console.log(`Found ${checkboxes.length} checked checkboxes for ${tabType}`);
    
    const selectedTags = [];
    
    checkboxes.forEach(checkbox => {
        const row = checkbox.closest('tr');
        if (row && row.dataset.item) {
            try {
                const tagData = JSON.parse(row.dataset.item);
                console.log(`Selected tag:`, tagData);
                
                // For synced tags, ensure we have the correct ID
                if (tabType === 'synced' && tagData.combined) {
                    // Make sure the ID is properly extracted from the combined data
                    if (!tagData.id && tagData.combined && tagData.combined.id) {
                        tagData.id = tagData.combined.id;
                    }
                    // Also ensure the local data has the ID
                    if (tagData.local && !tagData.local.id && tagData.combined && tagData.combined.id) {
                        tagData.local.id = tagData.combined.id;
                    }
                }
                
                selectedTags.push(tagData);
            } catch (e) {
                console.error('Error parsing tag data:', e);
            }
        } else {
            console.error('Could not find row or row.dataset.item for checkbox', checkbox);
        }
    });
    
    console.log(`Total selected tags: ${selectedTags.length}`);
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

// Load users and groups with caching
async function loadUsersAndGroups() {
    const now = Date.now();
    
    // Check if cache is still valid
    if (usersAndGroupsCache.lastFetched && 
        (now - usersAndGroupsCache.lastFetched) < usersAndGroupsCache.cacheExpiry &&
        usersAndGroupsCache.users.length > 0) {
        console.log('Using cached users and groups');
        populateOwnersSelect();
        return;
    }
    
    console.log('Fetching fresh users and groups data');
    
    try {
        // Fetch users, groups, and ownership types in a single request
        const response = await fetch('/metadata/tags/users-groups/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': document.querySelector('[name=csrfmiddlewaretoken]').value
            },
            body: JSON.stringify({ type: 'all' })
        });
        
        const data = await response.json();
        
        if (data.success) {
            usersAndGroupsCache.users = data.data.users || [];
            usersAndGroupsCache.groups = data.data.groups || [];
            usersAndGroupsCache.ownership_types = data.data.ownership_types || [];
            usersAndGroupsCache.lastFetched = now;
            
            console.log(`Loaded ${usersAndGroupsCache.users.length} users, ${usersAndGroupsCache.groups.length} groups, and ${usersAndGroupsCache.ownership_types.length} ownership types${data.cached ? ' (cached)' : ''}`);
            populateSimpleDropdowns();
        } else {
            console.error('Failed to load users, groups, and ownership types:', data.error);
            showOwnersError();
        }
    } catch (error) {
        console.error('Error loading users and groups:', error);
        showOwnersError();
    }
}

// Setup the ownership interface with dynamic owner entries
function setupOwnershipInterface() {
    const container = document.getElementById('owners-container');
    const addButton = document.getElementById('add-owner');
    
    if (!container || !addButton) return;
    
    // Clear existing entries (except template)
    const existingEntries = container.querySelectorAll('.owner-entry:not(#owner-template)');
    existingEntries.forEach(entry => entry.remove());
    
    // Setup add owner button with enhanced interaction
    addButton.onclick = () => {
        addOwnerEntry();
        // Add a subtle animation to the button
        addButton.style.transform = 'scale(0.95)';
        setTimeout(() => {
            addButton.style.transform = 'scale(1)';
        }, 150);
    };
    
    // Add one initial entry
    addOwnerEntry();
}

// Add a new owner entry
function addOwnerEntry() {
    const container = document.getElementById('owners-container');
    const template = document.getElementById('owner-template');
    
    if (!container || !template) return;
    
    // Clone the template
    const newEntry = template.cloneNode(true);
    newEntry.id = `owner-entry-${Date.now()}`;
    newEntry.style.display = 'block';
    
    // Add entry animation
    newEntry.style.opacity = '0';
    newEntry.style.transform = 'translateY(-20px)';
    
    // Populate ownership types dropdown
    const ownershipTypeSelectElement = newEntry.querySelector('.ownership-type-select');
    populateOwnershipTypeSelect(ownershipTypeSelectElement);
    
    // Populate owners dropdown
    const ownerSelectElement = newEntry.querySelector('.owner-select');
    populateOwnerSelect(ownerSelectElement);
    
    // Setup remove button with enhanced interaction
    const removeButton = newEntry.querySelector('.remove-owner');
    removeButton.onclick = () => {
        // Add removal animation
        newEntry.style.transform = 'translateX(-100%)';
        newEntry.style.opacity = '0';
        setTimeout(() => {
            removeOwnerEntry(newEntry);
        }, 300);
    };
    
    // Setup filtering for ownership types with enhanced UX
    const ownershipTypeFilter = newEntry.querySelector('.ownership-type-filter');
    setupEnhancedSelectFilter(ownershipTypeFilter, ownershipTypeSelectElement, 'ownership types');
    
    // Setup filtering for owners with enhanced UX
    const ownerFilter = newEntry.querySelector('.owner-filter');
    setupEnhancedSelectFilter(ownerFilter, ownerSelectElement, 'users and groups');
    
    // Add selection change handlers for visual feedback
    ownerSelectElement.addEventListener('change', function() {
        if (this.value) {
            this.style.borderColor = '#28a745';
            this.style.backgroundColor = '#f8fff9';
        } else {
            this.style.borderColor = '#e9ecef';
            this.style.backgroundColor = '#ffffff';
        }
    });
    
    ownershipTypeSelectElement.addEventListener('change', function() {
        if (this.value) {
            this.style.borderColor = '#28a745';
            this.style.backgroundColor = '#f8fff9';
        } else {
            this.style.borderColor = '#e9ecef';
            this.style.backgroundColor = '#ffffff';
        }
    });
    
    // Add to container
    container.appendChild(newEntry);
    
    // Animate in
    setTimeout(() => {
        newEntry.style.transition = 'all 0.3s ease';
        newEntry.style.opacity = '1';
        newEntry.style.transform = 'translateY(0)';
    }, 50);
    
    // Update remove button visibility
    updateRemoveButtonsVisibility();
}

// Remove an owner entry
function removeOwnerEntry(entry) {
    entry.remove();
    updateRemoveButtonsVisibility();
}

// Update visibility of remove buttons (hide if only one entry)
function updateRemoveButtonsVisibility() {
    const container = document.getElementById('owners-container');
    const entries = container.querySelectorAll('.owner-entry:not(#owner-template)');
    
    entries.forEach((entry, index) => {
        const removeButton = entry.querySelector('.remove-owner');
        removeButton.style.display = entries.length > 1 ? 'block' : 'none';
    });
}

// Populate ownership type select dropdown
function populateOwnershipTypeSelect(select) {
    if (!select) return;
    
    select.innerHTML = '<option value="">Choose ownership type...</option>';
    
    if (usersAndGroupsCache.ownership_types.length > 0) {
        usersAndGroupsCache.ownership_types.forEach(ownershipType => {
            const option = document.createElement('option');
            option.value = ownershipType.urn;
            option.textContent = `👑 ${ownershipType.name || ownershipType.urn}`;
            select.appendChild(option);
        });
    } else {
        const option = document.createElement('option');
        option.value = '';
        option.textContent = '⚠️ No ownership types available';
        option.disabled = true;
        select.appendChild(option);
    }
}

// Populate owner select dropdown
function populateOwnerSelect(select) {
    if (!select) return;
    
    select.innerHTML = '<option value="">Choose an owner...</option>';
    
    // Add users
    if (usersAndGroupsCache.users.length > 0) {
        const usersGroup = document.createElement('optgroup');
        usersGroup.label = '👤 Users';
        
        usersAndGroupsCache.users.forEach(user => {
            const option = document.createElement('option');
            option.value = user.urn;
            option.textContent = `👤 ${user.display_name || user.username || user.urn}`;
            option.dataset.type = 'user';
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
            option.textContent = `👥 ${group.display_name || group.urn}`;
            option.dataset.type = 'group';
            groupsGroup.appendChild(option);
        });
        
        select.appendChild(groupsGroup);
    }
    
    if (usersAndGroupsCache.users.length === 0 && usersAndGroupsCache.groups.length === 0) {
        const option = document.createElement('option');
        option.value = '';
        option.textContent = '⚠️ No users or groups available';
        option.disabled = true;
        select.appendChild(option);
    }
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
    }
}

// Add a new ownership section
function addOwnershipSection() {
    const container = document.getElementById('ownership-sections-container');
    if (!container) return;
    
    const sectionId = 'section-' + Date.now();
    
    // Create ownership type options
    let ownershipTypeOptions = '<option value="">Select ownership type...</option>';
    if (usersAndGroupsCache.ownership_types) {
        usersAndGroupsCache.ownership_types.forEach(type => {
            ownershipTypeOptions += `<option value="${type.urn}">${type.name || type.urn}</option>`;
        });
    }
    
    // Create owners options with icons
    let ownersOptions = '';
    if (usersAndGroupsCache.users) {
        usersAndGroupsCache.users.forEach(user => {
            ownersOptions += `<option value="${user.urn}" data-type="user">👤 ${user.display_name || user.username || user.urn}</option>`;
        });
    }
    if (usersAndGroupsCache.groups) {
        usersAndGroupsCache.groups.forEach(group => {
            ownersOptions += `<option value="${group.urn}" data-type="group">👥 ${group.display_name || group.urn}</option>`;
        });
    }
    
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
                         ${ownersOptions}
                     </select>
                     <div class="form-text">Search and select multiple owners</div>
                 </div>
                 <div class="mb-3">
                     <label class="form-label">Ownership Type <span class="text-danger">*</span></label>
                     <select class="form-select ownership-type-select" name="ownership_types[]" required>
                         ${ownershipTypeOptions}
                     </select>
                 </div>
             </div>
        </div>
    `;
    
    container.insertAdjacentHTML('beforeend', sectionHTML);
    
    // Initialize Select2 for the owners dropdown in the new section
    const newSection = document.getElementById(sectionId);
    const ownersSelect = newSection.querySelector('.owners-select');
    
    $(ownersSelect).select2({
        theme: 'bootstrap-5',
        width: '100%',
        placeholder: 'Search and select owners...',
        allowClear: true,
        dropdownParent: $('#createTagModal')
    });
    
    updateRemoveButtons();
}

// Update visibility of remove buttons (hide if only one section)
function updateRemoveButtons() {
    const container = document.getElementById('ownership-sections-container');
    const sections = container.querySelectorAll('.card');
    
    sections.forEach(section => {
        const removeButton = section.querySelector('button[onclick*="removeSection"]');
        if (removeButton) {
            removeButton.style.display = sections.length > 1 ? 'block' : 'none';
        }
    });
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
function setupActionButtonListeners() {
    // Use event delegation since rows are dynamically created
    document.addEventListener('click', function(e) {
        // Get the closest row to find tag data
        const row = e.target.closest('tr[data-item]');
        if (!row) return;
        
        // Parse the tag data from the row
        const tagData = DataUtils.safeJsonParse(row.dataset.item);
        if (!tagData) return;
        
        // Check which button was clicked
        const clickedElement = e.target.closest('button');
        if (!clickedElement) return;

        if (clickedElement.classList.contains('sync-to-local') || clickedElement.closest('.sync-to-local')) {
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
        }
    });
}
