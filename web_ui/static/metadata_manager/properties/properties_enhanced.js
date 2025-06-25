// Global variables
let propertiesData = [];
let filterData = {
    value_types: {},
    entity_types: {}
};
let currentFilters = new Set();
let currentOverviewFilter = null;
let currentSearch = {
    synced: '',
    local: '',
    remote: ''
};

// Pagination variables - enhanced to match tags page
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

// Pagination settings (deprecated - now using currentPagination object)
const ITEMS_PER_PAGE = 25;

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    // Initial load
    loadPropertiesData(true); // Skip sync validation on initial load

    // Setup tab switching
    setupTabSwitchHandlers();
    
    // Setup other event listeners
    setupEventListeners();
    setupSearchListeners();
    setupFilterListeners();
    setupBulkActions();
    setupImportFormHandler();
    setupEditFormHandler();
    setupOverviewClickHandlers();
    
});

function setupEventListeners() {
    // Refresh button
    const refreshBtn = document.getElementById('refreshProperties');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', function() {
            loadPropertiesData(); // Don't skip sync validation on manual refresh
        });
    }

    // Search functionality
    setupSearchListeners();
    
    // Edit form submission
    setupEditFormHandler();
    
    // Overview clickable stats
    setupOverviewClickHandlers();
    
    // Tab switching handlers
    setupTabSwitchHandlers();
    
    // Filter listeners
    setupFilterListeners();
    
    // Bulk actions
    setupBulkActions();
    
    // Import form handler
    setupImportFormHandler();
}

function setupSearchListeners() {
    ['synced', 'local', 'remote'].forEach(tabType => {
        const searchInput = document.getElementById(`${tabType}-search`);
        const clearButton = document.getElementById(`${tabType}-clear`);
        
        if (searchInput) {
            searchInput.addEventListener('input', function() {
                currentSearch[tabType] = this.value.toLowerCase();
                filterAndRenderProperties(tabType);
            });
        }
        
        if (clearButton) {
            clearButton.addEventListener('click', function() {
                searchInput.value = '';
                currentSearch[tabType] = '';
                filterAndRenderProperties(tabType);
            });
        }
    });
}

function setupFilterListeners() {
    // Overview filters - switch tabs instead of filtering
    document.querySelectorAll('.clickable-stat[data-category="overview"]').forEach(stat => {
        stat.addEventListener('click', function() {
            const filter = this.getAttribute('data-filter');
            
            // Switch to appropriate tab
            let targetTab = null;
            switch (filter) {
                case 'synced':
                    targetTab = 'synced-tab';
                    break;
                case 'local-only':
                    targetTab = 'local-tab';
                    break;
                case 'remote-only':
                    targetTab = 'remote-tab';
                    break;
                default:
                    return; // Don't switch for total
            }
            
            if (targetTab) {
                // Clear overview active states
                document.querySelectorAll('.clickable-stat[data-category="overview"]').forEach(s => {
                    s.classList.remove('active');
                });
                
                // Set this one as active
                this.classList.add('active');
                
                // Switch to the tab
                const tabButton = document.getElementById(targetTab);
                if (tabButton) {
                    tabButton.click();
                }
            }
        });
    });
    
    // Value type and entity type filters - multi-select functionality
    document.addEventListener('click', function(e) {
        if (e.target.closest('#value-type-filters .filter-stat') || e.target.closest('#entity-type-filters .filter-stat')) {
            const stat = e.target.closest('.filter-stat');
            const filterType = stat.closest('#value-type-filters') ? 'valueType' : 'entityType';
            const filterValue = stat.getAttribute('data-filter');
            
            // Toggle the filter
            toggleFilter(filterType, filterValue);
            
            // Toggle active state
            stat.classList.toggle('active');
            
            // Refresh current tab
            refreshCurrentTab();
        }
    });
}

function setupBulkActions() {
    // Setup bulk selection checkboxes
    ['synced', 'local', 'remote'].forEach(tabType => {
        setupBulkSelectionForTab(tabType);
    });
}

function setupImportFormHandler() {
    const importForm = document.getElementById('import-json-form');
    if (importForm) {
        importForm.addEventListener('submit', function(e) {
            e.preventDefault();
            
            const formData = new FormData(this);
            
            // Show loading state
            const submitButton = this.querySelector('button[type="submit"]');
            const originalText = submitButton.innerHTML;
            submitButton.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i> Importing...';
            submitButton.disabled = true;
            
            fetch('/metadata/properties/import/', {
                method: 'POST',
                body: formData,
                headers: {
                    'X-CSRFToken': getCSRFToken(),
                    'X-Requested-With': 'XMLHttpRequest',
                }
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    showNotification('success', 'Properties imported successfully');
                    
                    // Close modal
                    const modal = bootstrap.Modal.getInstance(document.getElementById('importJsonModal'));
                    modal.hide();
                    
                    // Refresh properties data
                    loadPropertiesData(true); // Skip sync validation to prevent mass status updates
                } else {
                    showNotification('error', data.error || 'Failed to import properties');
                }
            })
            .catch(error => {
                console.error('Error:', error);
                showNotification('error', 'Error importing properties');
            })
            .finally(() => {
                // Restore button state
                submitButton.innerHTML = originalText;
                submitButton.disabled = false;
            });
        });
    }
}

function setupEditFormHandler() {
    const editForm = document.getElementById('editPropertyForm');
    if (editForm) {
        editForm.addEventListener('submit', function(e) {
            e.preventDefault();
            
            const formData = new FormData(this);
            const propertyId = formData.get('property_id');
            
            // Show loading state
            const submitButton = this.querySelector('button[type="submit"]');
            const originalText = submitButton.innerHTML;
            submitButton.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i> Saving...';
            submitButton.disabled = true;
            
            fetch(this.action, {
                method: 'POST',
                body: formData,
                headers: {
                    'X-CSRFToken': getCSRFToken(),
                    'X-Requested-With': 'XMLHttpRequest',
                }
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    showNotification('success', 'Property updated successfully');
                    
                    // Close modal
                    const modal = bootstrap.Modal.getInstance(document.getElementById('editPropertyModal'));
                    modal.hide();
                    
                    // Refresh properties data
                    loadPropertiesData(true); // Skip sync validation to prevent mass status updates
                } else {
                    showNotification('error', data.error || 'Failed to update property');
                }
            })
            .catch(error => {
                console.error('Error:', error);
                showNotification('error', 'Error updating property');
            })
            .finally(() => {
                // Restore button state
                submitButton.innerHTML = originalText;
                submitButton.disabled = false;
            });
        });
    }
}

function setupOverviewClickHandlers() {
    // Overview filters - switch tabs instead of filtering
    document.querySelectorAll('.clickable-stat[data-category="overview"]').forEach(stat => {
        stat.addEventListener('click', function() {
            const filter = this.getAttribute('data-filter');
            
            // Switch to appropriate tab
            let targetTab = null;
            switch (filter) {
                case 'synced':
                    targetTab = 'synced-tab';
                    break;
                case 'local-only':
                    targetTab = 'local-tab';
                    break;
                case 'remote-only':
                    targetTab = 'remote-tab';
                    break;
                default:
                    return; // Don't switch for total
            }
            
            if (targetTab) {
                // Clear overview active states
                document.querySelectorAll('.clickable-stat[data-category="overview"]').forEach(s => {
                    s.classList.remove('active');
                });
                
                // Set this one as active
                this.classList.add('active');
                
                // Switch to the tab
                const tabButton = document.getElementById(targetTab);
                if (tabButton) {
                    tabButton.click();
                }
            }
        });
    });
    
    // Content filters - toggle selection for filtering
    document.querySelectorAll('.clickable-stat[data-category="content"]').forEach(stat => {
        stat.addEventListener('click', function() {
            this.classList.toggle('active');
            const filter = this.getAttribute('data-filter');
            console.log('Content filter toggled:', filter, this.classList.contains('active'));
            
            // Add future filtering logic here if needed
        });
    });
}

function setupTabSwitchHandlers() {
    // Tab switching handlers - just update overview state, content is already rendered
    document.querySelectorAll('[data-bs-toggle="tab"]').forEach(tab => {
        tab.addEventListener('shown.bs.tab', function (e) {
            // Get target from data-bs-target or href
            let targetId = e.target.getAttribute('data-bs-target') || e.target.getAttribute('href');
            if (targetId && targetId.startsWith('#')) {
                targetId = targetId.substring(1);
            }
            const tabType = targetId.replace('-items', '');
            console.log(`Tab switched to: ${tabType} (target: ${targetId})`);
            
            // Update overview active states based on current tab
            updateOverviewActiveState(tabType);
        });
    });
}

function updateOverviewActiveState(tabType) {
    // Clear all overview active states
    document.querySelectorAll('.clickable-stat[data-category="overview"]').forEach(stat => {
        stat.classList.remove('active');
    });
    
    // Set the appropriate overview stat as active based on current tab
    let filterValue = null;
    switch (tabType) {
        case 'synced':
            filterValue = 'synced';
            break;
        case 'local':
            filterValue = 'local-only';
            break;
        case 'remote':
            filterValue = 'remote-only';
            break;
    }
    
    if (filterValue) {
        const activeStat = document.querySelector(`.clickable-stat[data-filter="${filterValue}"]`);
        if (activeStat) {
            activeStat.classList.add('active');
        }
    }
}

function loadPropertiesData(skipSyncValidation = false) {
    // Show loading indicator
    const loadingIndicator = document.getElementById('loading-indicator');
    const propertiesContent = document.getElementById('properties-content');
    
    if (loadingIndicator) loadingIndicator.style.display = 'block';
    if (propertiesContent) propertiesContent.style.display = 'none';
    
    // Build URL with optional skip_sync_validation parameter
    let url = '/metadata/properties/data/';
    if (skipSyncValidation) {
        url += '?skip_sync_validation=true';
    }
    
    fetch(url, {
        method: 'GET',
        headers: {
            'X-Requested-With': 'XMLHttpRequest',
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            // Store data just like domains - the backend provides flat data with status field
            propertiesData = {
                synced_items: data.data.filter(p => p.status === 'synced'),
                local_only_items: data.data.filter(p => p.status === 'local_only'),
                remote_only_items: data.data.filter(p => p.status === 'remote_only'),
                statistics: data.statistics,
                filters: data.filters || { value_types: {}, entity_types: {} }
            };
            
            console.log('Raw data from server:', data.data.length, 'properties');
            console.log('Raw data sample:', data.data.slice(0, 3));
            console.log('Synced items:', propertiesData.synced_items.length, propertiesData.synced_items);
            console.log('Local only items:', propertiesData.local_only_items.length, propertiesData.local_only_items);
            console.log('Remote only items:', propertiesData.remote_only_items.length, propertiesData.remote_only_items);
            
            updateStatistics(data.statistics);
            updateFilterRows();
            
            // Render all tabs like other pages do
            renderAllTabs();
            
            // Don't override the default active tab from the template
            // The template sets synced as active by default, which is correct
        
            // Hide loading and show content
            if (loadingIndicator) loadingIndicator.style.display = 'none';
            if (propertiesContent) propertiesContent.style.display = 'block';
        } else {
            showNotification('error', data.error || 'Failed to load properties data');
        }
    })
    .catch(error => {
        console.error('Error loading properties:', error);
        showNotification('error', 'Failed to load properties data');
        if (loadingIndicator) loadingIndicator.style.display = 'none';
    });
}

function updateStatistics(statistics) {
    // Use statistics from backend if provided, otherwise calculate from data
    let syncedCount, localCount, remoteCount, totalCount;
    
    if (statistics) {
        syncedCount = statistics.synced_count || 0;
        localCount = statistics.local_only_count || 0;
        remoteCount = statistics.remote_only_count || 0;
        totalCount = statistics.total_count || 0;
    } else {
        syncedCount = propertiesData.filter(p => p.status === 'synced').length;
        localCount = propertiesData.filter(p => p.status === 'local_only').length;
        remoteCount = propertiesData.filter(p => p.status === 'remote_only').length;
        totalCount = propertiesData.length;
    }
    
    // Update overview statistics (both top filter bar and tab stats)
    const totalPropertiesEl = document.getElementById('total-properties');
    const totalEl = document.getElementById('total-items');
    const syncedCountEl = document.getElementById('synced-count');
    const localOnlyCountEl = document.getElementById('local-only-count');
    const remoteOnlyCountEl = document.getElementById('remote-only-count');
    
    if (totalPropertiesEl) totalPropertiesEl.textContent = totalCount;
    if (totalEl) totalEl.textContent = totalCount;
    if (syncedCountEl) syncedCountEl.textContent = syncedCount;
    if (localOnlyCountEl) localOnlyCountEl.textContent = localCount;
    if (remoteOnlyCountEl) remoteOnlyCountEl.textContent = remoteCount;
    
    // Update tab badges
    const syncedBadge = document.getElementById('synced-badge');
    const localBadge = document.getElementById('local-badge');
    const remoteBadge = document.getElementById('remote-badge');
    
    if (syncedBadge) syncedBadge.textContent = syncedCount;
    if (localBadge) localBadge.textContent = localCount;
    if (remoteBadge) remoteBadge.textContent = remoteCount;
    
    // Update content filter statistics
    updateContentFilterStats();
}

function updateContentFilterStats() {
    if (!propertiesData) return;
    
    // Combine all property types to get full data set
    const allProperties = [
        ...(propertiesData.synced_items || []),
        ...(propertiesData.local_only_items || []), 
        ...(propertiesData.remote_only_items || [])
    ];
    
    // Calculate content filter statistics
    const hasAllowedValues = allProperties.filter(p => p.allowedValues && p.allowedValues.length > 0).length;
    const hasEntityTypes = allProperties.filter(p => p.entity_types && p.entity_types.length > 0).length;
    const isSearchable = allProperties.filter(p => p.show_in_search_filters === true).length;
    const isImmutable = allProperties.filter(p => p.immutable === true).length;
    
    // Update elements
    const hasAllowedValuesEl = document.getElementById('has-allowedValues-count');
    const hasEntityTypesEl = document.getElementById('has-entityTypes-count');
    const isSearchableEl = document.getElementById('is-searchable-count');
    const isImmutableEl = document.getElementById('is-immutable-count');
    
    if (hasAllowedValuesEl) hasAllowedValuesEl.textContent = hasAllowedValues;
    if (hasEntityTypesEl) hasEntityTypesEl.textContent = hasEntityTypes;
    if (isSearchableEl) isSearchableEl.textContent = isSearchable;
    if (isImmutableEl) isImmutableEl.textContent = isImmutable;
}

function updateFilterRows() {
    console.log('propertiesData.filters:', propertiesData.filters);
    
    // Update value type filters
    const valueTypeContainer = document.getElementById('value-type-filters');
    console.log('valueTypeContainer found:', !!valueTypeContainer);
    
    if (valueTypeContainer && propertiesData.filters && propertiesData.filters.value_types) {
        console.log('Creating value type filters:', propertiesData.filters.value_types);
        valueTypeContainer.innerHTML = '';
        
        Object.entries(propertiesData.filters.value_types).forEach(([valueType, count]) => {
            const filterDiv = document.createElement('div');
            filterDiv.className = 'filter-stat text-center clickable-stat multi-select';
            filterDiv.setAttribute('data-filter', valueType);
            filterDiv.setAttribute('data-category', 'value-type');
            filterDiv.style.flex = '1';
            
            // Convert to title case
            const displayValueType = valueType.toLowerCase().replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
            
            filterDiv.innerHTML = `
                <div class="h5 mb-0">${count}</div>
                <div class="text-muted">${displayValueType}</div>
            `;
            
            valueTypeContainer.appendChild(filterDiv);
        });
    }
    
    // Update entity type filters
    const entityTypeContainer = document.getElementById('entity-type-filters');
    console.log('entityTypeContainer found:', !!entityTypeContainer);
    
    if (entityTypeContainer && propertiesData.filters && propertiesData.filters.entity_types) {
        console.log('Creating entity type filters:', propertiesData.filters.entity_types);
        entityTypeContainer.innerHTML = '';
        
        Object.entries(propertiesData.filters.entity_types).forEach(([entityType, count]) => {
            const filterDiv = document.createElement('div');
            filterDiv.className = 'filter-stat text-center clickable-stat multi-select';
            filterDiv.setAttribute('data-filter', entityType);
            filterDiv.setAttribute('data-category', 'entity-type');
            filterDiv.style.flex = '1';
            
            // Convert to title case and replace underscores with spaces
            const displayEntityType = entityType.toLowerCase().replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
            
            filterDiv.innerHTML = `
                <div class="h5 mb-0">${count}</div>
                <div class="text-muted">${displayEntityType}</div>
            `;
            
            entityTypeContainer.appendChild(filterDiv);
        });
    }
}

function toggleFilter(filterType, value) {
    const filterKey = `${filterType}:${value}`;
    if (currentFilters.has(filterKey)) {
        currentFilters.delete(filterKey);
    } else {
        currentFilters.add(filterKey);
    }
}

function applyFilters(items) {
    if (currentFilters.size === 0) {
        return items;
    }
    
    return items.filter(property => {
        // Check value type filters
        const valueTypeFilters = Array.from(currentFilters).filter(f => f.startsWith('valueType:'));
        if (valueTypeFilters.length > 0) {
            const valueTypeMatch = valueTypeFilters.some(filter => {
                const filterValue = filter.split(':')[1];
                return property.value_type && property.value_type === filterValue;
            });
            if (!valueTypeMatch) return false;
        }
        
        // Check entity type filters
        const entityTypeFilters = Array.from(currentFilters).filter(f => f.startsWith('entityType:'));
        if (entityTypeFilters.length > 0) {
            const entityTypeMatch = entityTypeFilters.some(filter => {
                const filterValue = filter.split(':')[1];
                return property.entity_types && property.entity_types.some(type => 
                    type === filterValue
                );
            });
            if (!entityTypeMatch) return false;
        }
        
        return true;
    });
}

function determineDefaultTab() {
    // Automatically switch to the first tab with data
    const syncedCount = propertiesData.synced_items ? propertiesData.synced_items.length : 0;
    const localCount = propertiesData.local_only_items ? propertiesData.local_only_items.length : 0;
    const remoteCount = propertiesData.remote_only_items ? propertiesData.remote_only_items.length : 0;
    
    console.log(`Tab counts - Synced: ${syncedCount}, Local: ${localCount}, Remote: ${remoteCount}`);
    
    if (syncedCount > 0) {
        return 'synced';
    } else if (localCount > 0) {
        return 'local';
    } else if (remoteCount > 0) {
        return 'remote';
    } else {
        // Default to synced if all are empty
        return 'synced';
    }
}

function renderAllTabs() {

    renderTab('synced-items');
    renderTab('local-items');
    renderTab('remote-items');
}

function setActiveTab(tabType) {
    // Remove active class from all tabs
    document.querySelectorAll('#properties-tabs .nav-link').forEach(tab => {
        tab.classList.remove('active');
    });
    document.querySelectorAll('.tab-pane').forEach(pane => {
        pane.classList.remove('active', 'show');
    });
    
    // Set the specified tab as active
    const tabButton = document.getElementById(`${tabType}-tab`);
    const tabPane = document.getElementById(`${tabType}-items`);
    
    if (tabButton && tabPane) {
        tabButton.classList.add('active');
        tabPane.classList.add('active', 'show');
    }
    
    updateOverviewActiveState(tabType);
    console.log(`Active tab set to: ${tabType}`);
}

function refreshCurrentTab() {
    // Get the currently active tab
    const activeTab = document.querySelector('.nav-link.active');
    if (activeTab) {
        const tabType = activeTab.id.replace('-tab', '');
        console.log('Refreshing current tab:', tabType);
        renderTab(`${tabType}-items`);
        updateFilterDisplay();
    }
}

function filterAndRenderProperties(tabType) {
    // Update current search term
    const searchInput = document.getElementById(`${tabType}-search`);
    if (searchInput) {
        currentSearch[tabType] = searchInput.value.toLowerCase();
    }
    
    // Re-render the tab with current filters and search
    displayTabContent(tabType);
}

function renderTab(tabId) {
    const tabType = tabId.replace('-items', '');
    displayTabContent(tabType);
}

function displayTabContent(tabType) {
    const contentElement = document.getElementById(`${tabType}-content`);
    const searchTerm = currentSearch[tabType] || '';
    

    
    if (!contentElement) {
        console.error(`Content element not found for ${tabType}-content`);
        return;
    }
    
    if (!propertiesData) {
        console.error('No propertiesData available');
        return;
    }
    
    let items = [];
    
    switch (tabType) {
        case 'synced':
            items = propertiesData.synced_items || [];
            break;
        case 'local':
            items = propertiesData.local_only_items || [];
            break;
        case 'remote':
            items = propertiesData.remote_only_items || [];
            break;
        default:
            console.error(`Unknown tab type: ${tabType}`);
            return;
    }
    
    console.log(`Tab ${tabType} should show ${items.length} items`);
    
    // Apply search
    if (searchTerm) {
        items = items.filter(property => 
            (property.name || '').toLowerCase().includes(searchTerm) ||
            (property.description || '').toLowerCase().includes(searchTerm) ||
            (property.qualified_name || '').toLowerCase().includes(searchTerm) ||
            (property.urn || '').toLowerCase().includes(searchTerm)
        );
        console.log(`After search filter: ${items.length} items`);
    }
    
    // Apply filters
    items = applyFilters(items);
    console.log(`After applying filters: ${items.length} items`);
    
    // Apply sorting if active for this tab
    if (currentSort.column && currentSort.tabType === tabType) {
        items = sortItems(items, currentSort.column, currentSort.direction);
    }
    
    // Generate table HTML with pagination
    const tableHTML = generateTableHTML(items, tabType);
    
    // Update content
    contentElement.innerHTML = tableHTML;
    
    // Setup event handlers
    setupBulkSelectionForTab(tabType);
    attachSortingHandlers(contentElement, tabType);
    restoreSortState(contentElement, tabType);
    attachPaginationHandlers(contentElement, tabType);
    attachViewButtonHandlers(contentElement);
    
    console.log(`Tab ${tabType} rendered successfully with ${items.length} items`);
}

function generateTableHTML(items, tabType) {
    const pagination = currentPagination[tabType];
    const startIndex = (pagination.page - 1) * pagination.itemsPerPage;
    const endIndex = startIndex + pagination.itemsPerPage;
    const paginatedItems = items.slice(startIndex, endIndex);
    
    // Determine if we should show the sync status column
    const showSyncStatus = tabType === 'synced';
    
    // Adjust column widths based on table type
    const nameWidth = showSyncStatus ? '140' : '180';
    const descriptionWidth = showSyncStatus ? '200' : '250';
    const urnWidth = showSyncStatus ? '120' : '150';
    const actionsWidth = showSyncStatus ? '150' : '180';
    
    return `
        <div class="table-responsive">
            <table class="table table-hover mb-0">
                <thead>
                    <tr>
                        <th width="40">
                            <input type="checkbox" class="form-check-input select-all-checkbox" id="selectAll${tabType.charAt(0).toUpperCase() + tabType.slice(1)}">
                        </th>
                        <th class="sortable-header" data-sort="name" width="${nameWidth}">Name</th>
                        <th width="${descriptionWidth}">Description</th>
                        <th class="sortable-header" data-sort="entity_types" width="200">Entity Types</th>
                        <th class="sortable-header" data-sort="value_type" width="120">Value Type</th>
                        <th class="sortable-header" data-sort="cardinality" width="100">Cardinality</th>
                        <th class="sortable-header" data-sort="allowedValues" width="100">Allowed Values</th>
                        <th width="${urnWidth}">URN</th>
                        ${showSyncStatus ? '<th class="sortable-header" data-sort="sync_status" width="100">Sync Status</th>' : ''}
                        <th width="${actionsWidth}">Actions</th>
                    </tr>
                </thead>
                <tbody>
                    ${paginatedItems.length === 0 ? getEmptyStateHTML(tabType, currentSearch[tabType]) : 
                      paginatedItems.map(item => renderPropertyRow(item, tabType)).join('')}
                </tbody>
            </table>
        </div>
        ${items.length > pagination.itemsPerPage ? generatePaginationHTML(items.length, tabType) : ''}
    `;
}

function getEmptyStateHTML(tabType, hasSearch) {
    const colSpan = tabType === 'synced' ? '10' : '9';
    return `
        <tr>
            <td colspan="${colSpan}" class="text-center py-4 text-muted">
                <i class="fas fa-inbox fa-2x mb-2"></i><br>
                ${hasSearch ? `No ${tabType} properties found matching your search` : `No ${tabType} properties found`}
            </td>
        </tr>
    `;
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
                Showing ${startItem}-${endItem} of ${totalItems} properties
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

function getSortValue(property, column) {
    switch(column) {
        case 'name':
            return (property.name || '').toLowerCase();
        case 'entity_types':
            return (property.entity_types || []).join(',').toLowerCase();
        case 'value_type':
            return (property.value_type || '').toLowerCase();
        case 'cardinality':
            return (property.cardinality || '').toLowerCase();
        case 'allowedValues':
            return (property.allowed_values || []).length;
        case 'sync_status':
            const syncStatus = property.sync_status || 'UNKNOWN';
            const statusOrder = {
                'SYNCED': 1,
                'MODIFIED': 2,
                'LOCAL_ONLY': 3,
                'REMOTE_ONLY': 4,
                'UNKNOWN': 5
            };
            return statusOrder[syncStatus] || 5;
        default:
            return '';
    }
}

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

// Tab content is rendered by renderAllTabs(), no need for individual loading

function renderPropertiesTable(properties, tabType, container) {
    console.log(`renderPropertiesTable: ${tabType} with ${properties.length} properties`);
    console.log('Properties data:', properties);
    console.log('Container:', container);
    
    // Get current page and items per page for this tab
    const pagination = currentPagination[tabType];
    const currentPage = pagination.page;
    const itemsPerPage = pagination.itemsPerPage;
    
    // Calculate pagination
    const totalItems = properties.length;
    const totalPages = Math.ceil(totalItems / itemsPerPage);
    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = Math.min(startIndex + itemsPerPage, totalItems);
    const paginatedProperties = properties.slice(startIndex, endIndex);
    
    if (totalItems === 0) {
        console.log('No properties to render');
        container.innerHTML = `
            <div class="text-center py-5">
                <i class="fas fa-inbox fa-3x text-muted mb-3"></i>
                <h5 class="text-muted">No ${tabType} properties found</h5>
                <p class="text-muted">There are no properties in this category.</p>
            </div>
        `;
        return;
    }
    
    // Determine if we should show the sync status column (only for synced tab)
    const showSyncStatus = tabType === 'synced';
    
    // Adjust column widths based on whether sync status is shown
    const nameWidth = showSyncStatus ? '140' : '180';
    const descriptionWidth = showSyncStatus ? '200' : '250';
    const urnWidth = showSyncStatus ? '120' : '150';
    const actionsWidth = showSyncStatus ? '150' : '180';
    
    console.log('Generating table HTML...');
    const tableHtml = `
        <div class="table-responsive">
            <table class="table table-hover mb-0">
                <thead>
                    <tr>
                        <th width="40px">
                            <input type="checkbox" class="form-check-input select-all-checkbox" id="selectAll${tabType.charAt(0).toUpperCase() + tabType.slice(1)}">
                        </th>
                        <th width="${nameWidth}px">Name</th>
                        <th width="${descriptionWidth}px">Description</th>
                        <th width="200px">Entity Types</th>
                        <th width="120px">Value Type</th>
                        <th width="100px">Cardinality</th>
                        <th width="100px">Allowed Values</th>
                        <th width="${urnWidth}px">URN</th>
                        ${showSyncStatus ? '<th class="sortable-header" data-sort="sync_status" width="100px">Sync Status</th>' : ''}
                        <th width="${actionsWidth}px">Actions</th>
                    </tr>
                </thead>
                <tbody>
                    ${paginatedProperties.map(property => renderPropertyRow(property, tabType)).join('')}
                </tbody>
            </table>
        </div>
        ${renderPagination(currentPage, totalPages, totalItems, startIndex + 1, endIndex, tabType)}
    `;
    
    console.log('Setting innerHTML...');
    container.innerHTML = tableHtml;
    console.log('Table rendered successfully');
    
    // Setup bulk selection
    setupBulkSelectionForTab(tabType);
    
    // Setup pagination handlers
    attachPaginationHandlers(container, tabType);
    
    // Setup sorting handlers
    attachSortingHandlers(container, tabType);
}

function renderPagination(currentPage, totalPages, totalItems, startItem, endItem, tabType) {
    if (totalPages <= 1) return '';
    
    const pagination = currentPagination[tabType];
    
    let paginationHTML = `
        <div class="pagination-container">
            <div class="pagination-info">
                Showing ${startItem}-${endItem} of ${totalItems} properties
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

function changePage(tabType, page) {
    if (currentPagination[tabType]) {
        currentPagination[tabType].page = page;
    } else {
        // Fallback for old structure
        switch (tabType) {
            case 'synced': currentSyncedPage = page; break;
            case 'local': currentLocalPage = page; break;
            case 'remote': currentRemotePage = page; break;
        }
    }
    renderTab(`${tabType}-items`);
}

function renderPropertyRow(property, tabType) {
    // Debug logging
    console.log('renderPropertyRow called with:', property.name, property.urn);
    console.log('Entity types:', property.entity_types);
    console.log('Value type:', property.value_type);
    
    // Get property data
    const propertyData = property.combined || property;
    const customActionButtons = getActionButtons(property, tabType);
    
    // Handle missing name gracefully
    const propertyName = property.name || property.qualified_name || 'Unnamed Property';
    const propertyUrn = property.urn || '';
    
    console.log('URN for truncation:', propertyUrn, 'truncated:', truncateUrn(propertyUrn, 30));
    
    // Escape the property data for the data attribute
    const propertyDataAttr = JSON.stringify(property).replace(/"/g, '&quot;');
    
    // Get the proper database ID for this property
    const databaseId = getDatabaseId(propertyData);
    
    // Get sync status information (for synced tab)
    const showSyncStatus = tabType === 'synced';
    const syncStatus = propertyData.sync_status || 'UNKNOWN';
    const syncStatusClass = getStatusBadgeClass(syncStatus);
    const syncStatusText = syncStatus.replace('_', ' ');
    
    return `
        <tr>
            <td>
                <input type="checkbox" class="form-check-input item-checkbox" value="${databaseId || property.urn}" data-property="${propertyDataAttr}">
            </td>
            <td>
                <strong>${escapeHtml(propertyName)}</strong>
            </td>
            <td>
                <div class="description-preview">
                    ${escapeHtml(property.description || 'No description')}
                </div>
            </td>
            <td>
                <div class="entity-types">
                    ${renderEntityTypes(property.entity_types, property.value_type)}
                </div>
            </td>
            <td>
                <span class="badge bg-info">${escapeHtml(getValueTypeDisplayName(property.value_type))}</span>
            </td>
            <td>
                <span class="badge bg-secondary">${escapeHtml(property.cardinality || 'SINGLE')}</span>
            </td>
            <td>
                <span class="badge bg-secondary">${property.allowedValuesCount || 0}</span>
            </td>
            <td title="${escapeHtml(propertyUrn)}">
                <code class="small">${escapeHtml(truncateUrn(propertyUrn, 30))}</code>
            </td>
            ${showSyncStatus ? `
            <td>
                <span class="badge ${syncStatusClass}">${syncStatusText}</span>
            </td>
            ` : ''}
            <td>
                <div class="btn-group action-buttons" role="group">
                    <!-- View entity button -->
                    <button type="button" class="btn btn-sm btn-outline-primary view-property" 
                            data-property-urn="${propertyUrn || databaseId}" title="View Details">
                        <i class="fas fa-eye"></i>
                    </button>
                    
                    <!-- View in DataHub button if property exists in DataHub for current connection -->
                    ${shouldShowDataHubViewButton(propertyData, tabType) ? `
                        <button type="button" class="btn btn-sm btn-outline-info view-in-datahub" 
                                onclick="viewInDataHub('${propertyUrn}')" title="View in DataHub">
                            <i class="fas fa-external-link-alt"></i>
                        </button>
                    ` : ''}
                    
                    <!-- Custom action buttons -->
                    ${customActionButtons}
                </div>
            </td>
        </tr>
    `;
}

function getActionButtons(property, tabType) {
    // Get property data
    const propertyData = property.combined || property;
    const urn = propertyData.urn || '';
    
    // Get connection context information
    const connectionContext = propertyData.connection_context || 'none'; // "current", "different", "none"
    const hasRemoteMatch = propertyData.has_remote_match || false;
    
    // Get the proper database ID for this property
    const databaseId = getDatabaseId(propertyData);
    
    let actionButtons = '';
    
    // 1. Edit Property - For local and synced properties
    if (tabType === 'local' || tabType === 'synced') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-warning edit-property" 
                    onclick="editProperty('${urn || databaseId}')" title="Edit Property">
                <i class="fas fa-edit"></i>
            </button>
        `;
    }

    // 2. Sync to DataHub - For properties in local tab
    // Show for ALL properties in local tab regardless of their connection or sync status
    if (tabType === 'local') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-success push-property" 
                    onclick="pushPropertyToDataHub('${databaseId}')" title="Push to DataHub">
                <i class="fas fa-upload"></i>
            </button>
        `;
    }
    
    // 2b. Resync - Only for synced properties (properties that belong to current connection and have remote match)
    if (tabType === 'synced' && connectionContext === 'current' && hasRemoteMatch) {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-info resync-property" 
                    onclick="resyncProperty('${databaseId}')" title="Resync from DataHub">
                <i class="fas fa-sync-alt"></i>
            </button>
        `;
    }
    
    // 2c. Push to DataHub - Only for synced properties that are modified
    if (tabType === 'synced' && connectionContext === 'current' && propertyData.sync_status === 'MODIFIED') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-success push-property" 
                    onclick="pushPropertyToDataHub('${databaseId}')" title="Push to DataHub">
                <i class="fas fa-upload"></i>
            </button>
        `;
    }
    
    // 3. Sync to Local - Only for remote-only properties
    if (tabType === 'remote') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-primary sync-property-to-local" 
                    onclick="syncPropertyToLocal(${JSON.stringify(property).replace(/"/g, '&quot;')})" title="Sync to Local">
                <i class="fas fa-download"></i>
            </button>
        `;
    }
    
    // 4. Download JSON - Available for all properties
    actionButtons += `
        <button type="button" class="btn btn-sm btn-outline-secondary download-json"
                onclick="downloadPropertyJson('${databaseId}')" title="Download JSON">
            <i class="fas fa-file-download"></i>
        </button>
    `;

    // 5. Add to Staged Changes - Available for all properties
    actionButtons += `
        <button type="button" class="btn btn-sm btn-outline-warning add-to-staged"
                onclick="addPropertyToStagedChanges(${JSON.stringify(property).replace(/"/g, '&quot;')})" title="Add to Staged Changes">
            <i class="fab fa-github"></i>
        </button>
    `;

    // 6. Delete Local Property - Only for synced and local properties
    if (tabType === 'synced' || tabType === 'local') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-danger delete-local-property" 
                    onclick="deleteLocalProperty('${databaseId}')" title="Delete Local Property">
                <i class="fas fa-trash"></i>
            </button>
        `;
    }
    
    // 7. Delete Remote Property - Only for remote-only properties
    if (tabType === 'remote') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-danger delete-remote-property" 
                    onclick="deleteRemoteProperty('${urn}')" title="Delete from DataHub">
                <i class="fas fa-trash"></i>
            </button>
        `;
    }
    
    return actionButtons;
}

function shouldShowDataHubViewButton(propertyData, tabType) {
    const urn = propertyData.urn || '';
    const connectionContext = propertyData.connection_context || 'none';
    const hasRemoteMatch = propertyData.has_remote_match || false;
    
    // Don't show for local URNs or empty URNs
    if (!urn || urn.includes('local:')) {
        return false;
    }
    
    // Show for remote-only properties (they definitely exist in DataHub)
    if (tabType === 'remote') {
        return true;
    }
    
    // For synced tab: show only if property belongs to current connection AND has remote match
    if (tabType === 'synced') {
        return connectionContext === 'current' && hasRemoteMatch;
    }
    
    // For local tab: don't show View in DataHub button
    // These properties either don't exist in DataHub or belong to different connections
    if (tabType === 'local') {
        return false;
    }
    
    return false;
}

/**
 * Setup bulk selection for a specific tab
 */
function setupBulkSelectionForTab(tabType) {
    const container = document.getElementById(`${tabType}-content`);
    if (!container) return;
    
    // Handle select all checkbox
    container.addEventListener('change', function(e) {
        if (e.target.classList.contains('select-all-checkbox')) {
            const checkboxes = container.querySelectorAll('input[type="checkbox"]:not(.select-all-checkbox)');
            checkboxes.forEach(checkbox => {
                checkbox.checked = e.target.checked;
            });
            updateBulkActionsVisibility(tabType);
        } else if (e.target.type === 'checkbox') {
            updateBulkActionsVisibility(tabType);
            
            // Update select all checkbox state
            const checkboxes = container.querySelectorAll('input[type="checkbox"]:not(.select-all-checkbox)');
            const checkedCheckboxes = container.querySelectorAll('input[type="checkbox"]:checked:not(.select-all-checkbox)');
            const selectAllCheckbox = container.querySelector('.select-all-checkbox');
            
            if (selectAllCheckbox) {
                selectAllCheckbox.checked = checkedCheckboxes.length === checkboxes.length;
                selectAllCheckbox.indeterminate = checkedCheckboxes.length > 0 && checkedCheckboxes.length < checkboxes.length;
            }
        }
    });
}

/**
 * Add property to staged changes (MCP)
 * @param {Object} property - The property object
 */
function addPropertyToStagedChanges(property) {
    
    // Get property data
    const propertyData = property.combined || property;
    const databaseId = getDatabaseId(propertyData);
    
    // Check if this is already a local/synced property or a remote-only property
    if (databaseId) {
        // Property already exists locally (synced or local-only), add directly to staged changes
        showNotification('info', 'Adding property to staged changes...');
        addPropertyToStagedChangesInternal(propertyData);
    } else {
        // Remote-only property - add directly to staged changes using remote endpoint
        showNotification('info', 'Adding remote property to staged changes...');
        addRemotePropertyToStagedChanges(property);
    }
}

function addPropertyToStagedChangesInternal(propertyData) {
    const propertyId = getDatabaseId(propertyData);
    
    if (!propertyId) {
        showNotification('error', 'Cannot add property to staged changes: No database ID found');
        return;
    }
    
    // Get current environment and mutation name
    const currentEnvironment = window.currentEnvironment || { name: 'dev' };
    const mutationName = window.mutationName || null;
    
    // Find the button to show loading state
    const button = event?.target || document.querySelector(`[onclick*="addPropertyToStagedChanges"]`);
    if (button) {
        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
    }
    
    // Make API call to add property to staged changes
    const url = `/metadata/properties/${propertyId}/stage_changes/`;
    console.log('🔗 Making request to URL:', url);
    fetch(url, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCSRFToken(),
            'X-Requested-With': 'XMLHttpRequest'
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
        if (data.status === 'success') {
            showNotification('success', data.message || 'Property added to staged changes successfully');
            // Refresh the data
            if (typeof loadPropertiesData === 'function') {
                loadPropertiesData(true); // Skip sync validation to prevent mass status updates
            }
        } else {
            throw new Error(data.error || 'Failed to add property to staged changes');
        }
    })
    .catch(error => {
        console.error('Error adding property to staged changes:', error);
        showNotification('error', `Error adding property to staged changes: ${error.message}`);
    })
    .finally(() => {
        if (button) {
            button.disabled = false;
            button.innerHTML = '<i class="fab fa-github"></i>';
        }
    });
}

/**
 * Get the actual database ID from a property object
 * This is needed because the property data might contain datahub_id instead of the actual database id
 * @param {Object} propertyData - The property data object
 * @returns {string|null} - The database ID or null if not found
 */
function getDatabaseId(propertyData) {

    
    // First check if this is a remote-only property
    if (propertyData.status === 'remote_only' || propertyData.sync_status === 'REMOTE_ONLY') {

        return null;
    }
    
    // For combined objects (synced properties), check local first
    if (propertyData.local && propertyData.local.id) {

        return propertyData.local.id;
    }
    
    // For local-only or synced properties, use the id directly
    if (propertyData.id) {

        return propertyData.id;
    }
    
    // Check for database_id field (explicitly added by backend)
    if (propertyData.database_id) {

        return propertyData.database_id;
    }
    
    // Log warning if we couldn't find an ID
    console.warn('🔍 Could not find database ID for property:', propertyData.name);
    return null;
}

/**
 * View property in DataHub
 * @param {string} propertyUrn - The property URN
 */
function viewInDataHub(propertyUrn) {
    // Get DataHub URL from the page data
    const datahubUrl = window.propertiesData?.datahub_url;
    if (!datahubUrl) {
        showNotification('error', 'DataHub URL not available');
        return;
    }
    
    // Construct the URL for structured properties
    const baseUrl = datahubUrl.replace(/\/+$/, ''); // Remove trailing slashes
    const propertyUrl = `${baseUrl}/structured-properties/${propertyUrn}`;
    
    // Open in new tab
    window.open(propertyUrl, '_blank');
}

/**
 * Download property JSON
 * @param {string} propertyId - The property ID
 */
function downloadPropertyJson(propertyId) {
    console.log(`Downloading property JSON for ID: ${propertyId}`);
    
    // Create a temporary link and trigger download
    const link = document.createElement('a');
    link.href = `/metadata/properties/${propertyId}/download/`;
    link.download = `property_${propertyId}.json`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    
    showNotification('success', 'Property JSON download started');
}

// Attach sorting handlers to table headers - matching tags implementation
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

// Sort the current table without re-rendering - matching assertions approach
function sortCurrentTable(content, tabType) {
    const tbody = content.querySelector('tbody');
    if (!tbody) return;
    
    const rows = Array.from(tbody.querySelectorAll('tr'));
    
    // Sort rows based on current sort settings
    rows.sort((a, b) => {
        const aVal = getSortValueFromRow(a, currentSort.column);
        const bVal = getSortValueFromRow(b, currentSort.column);
        
        if (aVal < bVal) return currentSort.direction === 'asc' ? -1 : 1;
        if (aVal > bVal) return currentSort.direction === 'asc' ? 1 : -1;
        return 0;
    });
    
    // Re-append sorted rows
    rows.forEach(row => tbody.appendChild(row));
    

}

// Get sort value from a table row for properties
function getSortValueFromRow(row, column) {
    const cells = row.querySelectorAll('td');
    
    // Check if this is a synced table (has sync status column)
    const hasSyncStatus = cells.length > 9; // More than 9 columns means sync status is present
    
    switch(column) {
        case 'name':
            return cells[1]?.textContent?.trim().toLowerCase() || ''; // Skip checkbox column
        case 'description':
            return cells[2]?.textContent?.trim().toLowerCase() || '';
        case 'entity_types':
            return cells[3]?.textContent?.trim().toLowerCase() || '';
        case 'value_type':
            return cells[4]?.textContent?.trim().toLowerCase() || '';
        case 'cardinality':
            return cells[5]?.textContent?.trim().toLowerCase() || '';
        case 'allowedValues':
            return cells[6]?.textContent?.trim().toLowerCase() || '';
        case 'sync_status':
            // Sync status column is at index 8 (after URN)
            return hasSyncStatus ? (cells[8]?.textContent?.trim().toLowerCase() || '') : '';
        default:
            return '';
    }
}

function attachViewButtonHandlers(container) {
    container.querySelectorAll('.view-property').forEach(button => {
        button.addEventListener('click', function() {
            const propertyUrn = this.dataset.propertyUrn;
            viewProperty(propertyUrn);
        });
    });
}

// Individual Action Functions
function viewProperty(propertyUrn) {
    // Find the property data across all categories
    let property = null;
    const allProperties = [
        ...(propertiesData.synced_items || []),
        ...(propertiesData.local_only_items || []),
        ...(propertiesData.remote_only_items || [])
    ];
    
    property = allProperties.find(p => p.urn === propertyUrn || p.id === propertyUrn);
    
    if (!property) {
        showNotification('error', 'Property not found');
        return;
    }
    
    // Show the property details modal
    showPropertyDetails(property);
}

function showPropertyDetails(property) {
    // Basic information
    document.getElementById('modal-property-name').textContent = property.name || 'Unnamed Property';
    document.getElementById('modal-property-id').textContent = property.id || property.qualified_name || 'No ID';
    document.getElementById('modal-property-urn').textContent = property.urn || 'No URN available';
    document.getElementById('modal-property-description').textContent = property.description || 'No description available';
    
    // Property details
    document.getElementById('modal-property-entity-types').innerHTML = property.entity_types && property.entity_types.length > 0 
        ? property.entity_types.map(type => `<span class="badge bg-light text-dark me-1">${escapeHtml(type)}</span>`).join('')
        : '<span class="text-muted">None specified</span>';
    
    document.getElementById('modal-property-value-type').innerHTML = `<span class="badge bg-info">${escapeHtml(getValueTypeDisplayName(property.value_type))}</span>`;
    document.getElementById('modal-property-cardinality').innerHTML = `<span class="badge bg-secondary">${escapeHtml(property.cardinality || 'SINGLE')}</span>`;
    
    // Raw JSON data
    document.getElementById('modal-property-raw-json').innerHTML = `<pre><code>${escapeHtml(JSON.stringify(property, null, 2))}</code></pre>`;
    
    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('propertyViewModal'));
    modal.show();
}

function editProperty(propertyUrn) {
    // Find the property data across all categories
    let property = null;
    const allProperties = [
        ...(propertiesData.synced_items || []),
        ...(propertiesData.local_only_items || []),
        ...(propertiesData.remote_only_items || [])
    ];
    
    property = allProperties.find(p => p.urn === propertyUrn || p.id === propertyUrn);
    
    if (!property) {
        showNotification('error', 'Property not found');
        return;
    }
    
    // Populate edit modal with property data
    populateEditPropertyModal(property);
    
    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('editPropertyModal'));
    modal.show();
}

function populateEditPropertyModal(property) {
    // Set form action URL
    const form = document.getElementById('editPropertyForm');
    form.action = `/metadata/properties/${property.id}/`;
    
    // Use the new comprehensive modal population function
    if (window.propertyModal && window.propertyModal.populateEditModal) {
        window.propertyModal.populateEditModal(property);
    } else {
        // Fallback to basic population if new modal system is not available
        document.getElementById('editPropertyId').value = property.id;
        document.getElementById('editPropertyName').value = property.name || '';
        document.getElementById('editPropertyDescription').value = property.description || '';
        document.getElementById('editPropertyQualifiedName').value = property.qualified_name || '';
        document.getElementById('editPropertyValueType').value = property.value_type || 'string';
        document.getElementById('editPropertyCardinality').value = property.cardinality || 'SINGLE';
        
        // Handle entity types
        const entityTypeCheckboxes = document.querySelectorAll('input[name="entity_types"]');
        entityTypeCheckboxes.forEach(checkbox => {
            checkbox.checked = (property.entity_types || []).includes(checkbox.value);
        });
        
        // Handle display settings
        document.getElementById('editShowInSearchFilters').checked = property.show_in_search_filters || false;
        document.getElementById('editShowAsAssetBadge').checked = property.show_as_asset_badge || false;
        document.getElementById('editShowInAssetSummary').checked = property.show_in_asset_summary || false;
        document.getElementById('editShowInColumnsTable').checked = property.show_in_columns_table || false;
        document.getElementById('editIsHidden').checked = property.is_hidden || false;
        document.getElementById('editPropertyImmutable').checked = property.immutable || false;
    }
}

function populatePropertyViewModal(property) {
    // Populate modal with property data
    document.getElementById('modal-property-name').textContent = property.name || '-';
    document.getElementById('modal-property-qualified-name').textContent = property.qualified_name || '-';
    document.getElementById('modal-property-urn').textContent = property.urn || '-';
    document.getElementById('modal-property-description').textContent = property.description || 'No description';
    document.getElementById('modal-property-value-type').textContent = property.value_type || 'STRING';
    document.getElementById('modal-property-cardinality').textContent = property.cardinality || 'SINGLE';
    
    // Entity types
    const entityTypesContainer = document.getElementById('modal-property-entity-types');
    if (property.entity_types && property.entity_types.length > 0) {
        entityTypesContainer.innerHTML = property.entity_types.map(type => 
            `<span class="badge bg-light text-dark me-1">${escapeHtml(type)}</span>`
        ).join('');
    } else {
        entityTypesContainer.textContent = 'None specified';
    }
    
    // Display settings
    document.getElementById('modal-property-search-filters').textContent = property.show_in_search_filters ? 'Yes' : 'No';
    document.getElementById('modal-property-asset-badge').textContent = property.show_as_asset_badge ? 'Yes' : 'No';
    document.getElementById('modal-property-asset-summary').textContent = property.show_in_asset_summary ? 'Yes' : 'No';
    document.getElementById('modal-property-columns-table').textContent = property.show_in_columns_table ? 'Yes' : 'No';
    document.getElementById('modal-property-hidden').textContent = property.is_hidden ? 'Yes' : 'No';
    document.getElementById('modal-property-immutable').textContent = property.immutable ? 'Yes' : 'No';
}

// Property Action Functions
function addPropertyToPR(propertyId) {
    console.log(`Adding property ${propertyId} to PR`);
    
    const button = document.querySelector(`button[onclick="addPropertyToPR('${propertyId}')"]`);
    if (button) {
        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
    }
    
    fetch(`/metadata/properties/${propertyId}/add-to-pr/`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCSRFToken(),
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showNotification('success', data.message);
            // Optionally reload the data to reflect any status changes
            loadPropertiesData(true); // Skip sync validation to prevent mass status updates
        } else {
            showNotification('error', data.error || 'Failed to add property to PR');
        }
    })
    .catch(error => {
        console.error('Error adding property to PR:', error);
        showNotification('error', 'Failed to add property to PR');
    })
    .finally(() => {
        if (button) {
            button.disabled = false;
            button.innerHTML = '<i class="fab fa-github"></i>';
        }
    });
}

function addRemotePropertyToPR(propertyUrn) {
    console.log('Adding remote property to staged changes:', propertyUrn);
    
    fetch('/metadata/properties/add-remote-to-pr/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-CSRFToken': getCSRFToken(),
        },
        body: `property_urn=${encodeURIComponent(propertyUrn)}`
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showNotification('success', data.message);
        } else {
            showNotification('error', data.error);
        }
    })
    .catch(error => {
        console.error('Error adding property to staged changes:', error);
        showNotification('error', 'Failed to add property to staged changes');
    });
}

function addRemotePropertyToStagedChanges(property) {
    console.log('Adding remote property to staged changes:', property);
    
    const currentEnvironment = getCurrentEnvironment();
    const mutationName = getCurrentMutationName();
    
    const payload = {
        item_data: property,
        environment: currentEnvironment,
        mutation_name: mutationName
    };
    
    fetch('/metadata/properties/remote/stage_changes/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCSRFToken(),
        },
        body: JSON.stringify(payload)
    })
    .then(response => response.json())
    .then(data => {
        if (data.status === 'success') {
            showNotification('success', data.message);
        } else {
            showNotification('error', data.error || 'Failed to add remote property to staged changes');
        }
    })
    .catch(error => {
        console.error('Error adding remote property to staged changes:', error);
        showNotification('error', 'Failed to add remote property to staged changes');
    });
}

function resyncProperty(propertyId) {
    console.log(`Resyncing property ${propertyId}`);
    
    const button = document.querySelector(`button[onclick="resyncProperty('${propertyId}')"]`);
    if (button) {
        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
    }
    
    fetch(`/metadata/properties/${propertyId}/resync/`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-CSRFToken': getCSRFToken(),
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showNotification('success', data.message);
            // Reload data to refresh the view
            loadPropertiesData(true); // Skip sync validation to prevent mass status updates
        } else {
            showNotification('error', data.error);
        }
    })
    .catch(error => {
        console.error('Error resyncing property:', error);
        showNotification('error', 'Failed to resync property');
    })
    .finally(() => {
        if (button) {
            button.disabled = false;
            button.innerHTML = '<i class="fas fa-sync-alt"></i>';
        }
    });
}

function pushPropertyToDataHub(propertyId) {
    console.log(`Pushing property ${propertyId} to DataHub`);
    
    const button = document.querySelector(`button[onclick="pushPropertyToDataHub('${propertyId}')"]`);
    if (button) {
        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
    }
    
    fetch(`/metadata/properties/${propertyId}/deploy/`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-CSRFToken': getCSRFToken(),
            'X-Requested-With': 'XMLHttpRequest'
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showNotification('success', data.message);
            // Reload data to refresh the view
            loadPropertiesData(true); // Skip sync validation to prevent mass status updates
        } else {
            showNotification('error', data.error);
        }
    })
    .catch(error => {
        console.error('Error pushing property to DataHub:', error);
        showNotification('error', 'Failed to push property to DataHub');
    })
    .finally(() => {
        if (button) {
            button.disabled = false;
            button.innerHTML = '<i class="fas fa-upload"></i>';
        }
    });
}

function deleteLocalProperty(propertyId) {
    console.log(`Deleting local property ${propertyId}`);
    
    if (!confirm('Are you sure you want to delete this property? This action cannot be undone.')) {
        return;
    }
    
    const button = document.querySelector(`button[onclick="deleteLocalProperty('${propertyId}')"]`);
    if (button) {
        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
    }
    
    fetch(`/metadata/properties/${propertyId}/delete/`, {
        method: 'DELETE',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCSRFToken(),
            'X-Requested-With': 'XMLHttpRequest'
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showNotification('success', data.message || 'Property deleted successfully');
            // Reload data to refresh the view
            loadPropertiesData(true); // Skip sync validation to prevent mass status updates
        } else {
            showNotification('error', data.error);
        }
    })
    .catch(error => {
        console.error('Error deleting property:', error);
        showNotification('error', 'Failed to delete property');
    })
    .finally(() => {
        if (button) {
            button.disabled = false;
            button.innerHTML = '<i class="fas fa-trash"></i>';
        }
    });
}

function syncPropertyToLocal(property, callback = null) {
    // Handle both property object and URN string for backward compatibility
    const propertyUrn = typeof property === 'string' ? property : property.urn;
    const propertyData = typeof property === 'object' ? property : null;
    
    console.log(`Syncing property to local: ${propertyUrn}`);
    
    // For property objects, extract the URN
    const urnToSync = propertyUrn || (propertyData && propertyData.urn) || (propertyData && propertyData.deterministic_urn);
    
    if (!urnToSync) {
        console.error('No URN found for property:', property);
        showNotification('error', 'Error: No URN found for property');
        if (callback) callback(null);
        return;
    }
    
    const button = document.querySelector(`button[onclick*="syncPropertyToLocal"]`);
    if (button) {
        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
    }
    
    fetch('/metadata/properties/sync/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-CSRFToken': getCSRFToken(),
            'X-Requested-With': 'XMLHttpRequest'
        },
        body: `property_urn=${encodeURIComponent(urnToSync)}`
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showNotification('success', data.message);
            // Reload data to refresh the view
            loadPropertiesData(true); // Skip sync validation to prevent mass status updates
            
            // If callback provided, call it with the synced property data
            if (callback && data.property) {
                callback(data.property);
            } else if (callback) {
                // If no property data returned, try to find it in the updated data
                // This is a fallback - ideally the backend should return the synced property
                setTimeout(() => {
                    // Try to find the synced property by URN
                    const allProperties = [
                        ...(window.propertiesData?.synced_items || []),
                        ...(window.propertiesData?.local_only_items || [])
                    ];
                    const syncedProperty = allProperties.find(p => p.urn === urnToSync);
                    callback(syncedProperty);
                }, 1000);
            }
        } else {
            showNotification('error', data.error);
            if (callback) callback(null);
        }
    })
    .catch(error => {
        console.error('Error syncing property:', error);
        showNotification('error', 'Failed to sync property to local');
        if (callback) callback(null);
    })
    .finally(() => {
        if (button) {
            button.disabled = false;
            button.innerHTML = '<i class="fas fa-download"></i>';
        }
    });
}

function deleteRemoteProperty(propertyUrn) {
    console.log(`Deleting remote property ${propertyUrn}`);
    
    if (!confirm('Are you sure you want to delete this property from DataHub? This action cannot be undone.')) {
        return;
    }
    
    const button = document.querySelector(`button[onclick="deleteRemoteProperty('${propertyUrn}')"]`);
    if (button) {
        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
    }
    
    fetch('/metadata/properties/delete-remote/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-CSRFToken': getCSRFToken(),
            'X-Requested-With': 'XMLHttpRequest'
        },
        body: `property_urn=${encodeURIComponent(propertyUrn)}`
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showNotification('success', data.message);
            // Reload data to refresh the view
            loadPropertiesData(true); // Skip sync validation to prevent mass status updates
        } else {
            showNotification('error', data.error);
        }
    })
    .catch(error => {
        console.error('Error deleting remote property:', error);
        showNotification('error', 'Failed to delete remote property');
    })
    .finally(() => {
        if (button) {
            button.disabled = false;
            button.innerHTML = '<i class="fas fa-trash"></i>';
        }
    });
}

function getCSRFToken() {
    return document.querySelector('[name=csrfmiddlewaretoken]').value;
}

function getCurrentEnvironment() {
    // Try to get from dropdown first
    const environmentSelect = document.getElementById('environment-select');
    if (environmentSelect) {
        return environmentSelect.value || 'dev';
    }
    
    // Fallback to data attribute or default
    const container = document.querySelector('[data-environment]');
    return container ? container.dataset.environment : 'dev';
}

function getCurrentMutationName() {
    // Try to get from input first
    const mutationInput = document.getElementById('mutation-name');
    if (mutationInput) {
        return mutationInput.value;
    }
    
    // Fallback to data attribute
    const container = document.querySelector('[data-mutation-name]');
    return container ? container.dataset.mutationName : null;
}

// Note: Duplicate showNotification function removed - using MetadataNotifications.show() instead
// This ensures consistent, standardized notification messages across all metadata types

function showNotification(type, message) {
    // Use global notification system
    if (typeof showToast === 'function') {
        showToast(type, message);
    } else {
        console.log(`${type.toUpperCase()}: ${message}`);
    }
}

function escapeHtml(text) {
    const map = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#039;'
    };
    
    // Handle null, undefined, or non-string values
    if (text === null || text === undefined) {
        return '';
    }
    
    // Convert to string if not already
    const str = String(text);
    
    return str.replace(/[&<>"']/g, function(m) { return map[m]; });
}

function renderEntityTypes(entityTypes, valueType) {
    if (!entityTypes || entityTypes.length === 0) {
        return '<span class="text-muted">-</span>';
    }
    
    // Show entity types for all properties with relative font size
    const validTypes = entityTypes.filter(type => {
        return typeof type === 'string' && type.trim() !== '';
    });
    
    if (validTypes.length === 0) {
        return '<span class="text-muted">-</span>';
    }

    return validTypes.map(type => {
        // Parse URN format: urn:li:entityType:datahub.dataset -> Dataset
        let displayType = type;
        
        if (type.startsWith('urn:li:entityType:')) {
            // Extract the entity type from URN
            const entityType = type.replace('urn:li:entityType:', '');
            
            // Handle datahub.* format
            if (entityType.startsWith('datahub.')) {
                displayType = entityType.replace('datahub.', '');
            } else {
                displayType = entityType;
            }
            
            // Convert to proper case
            displayType = displayType.toLowerCase()
                .replace(/_/g, ' ')
                .replace(/\b\w/g, l => l.toUpperCase());
        } else {
            // For non-URN formats, just clean up the display
            displayType = type.toLowerCase()
                .replace(/_/g, ' ')
                .replace(/\b\w/g, l => l.toUpperCase());
        }
        
        return `<span class="badge bg-light text-dark entity-type-badge">${escapeHtml(displayType)}</span>`;
    }).join('');
}

function truncateUrn(urn, maxLength) {
    if (!urn || urn.length <= maxLength) return urn;
    
    // Keep the beginning of the URN and truncate from the end
    return urn.substring(0, maxLength - 3) + '...';
}

function updateBulkActionsVisibility() {
    ['synced', 'local', 'remote'].forEach(tabType => {
        const bulkActions = document.getElementById(`${tabType}-bulk-actions`);
        const checkedBoxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
        const selectedCount = document.getElementById(`${tabType}-selected-count`);
        
        if (selectedCount) {
            selectedCount.textContent = checkedBoxes.length;
        }
        
        if (bulkActions) {
            if (checkedBoxes.length > 0) {
                bulkActions.classList.add('show');
            } else {
                bulkActions.classList.remove('show');
            }
        }
    });
}

// Bulk action functions for properties
function bulkResyncProperties(tabType) {
    const checkedBoxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    if (checkedBoxes.length === 0) return;
    
    if (confirm(`Are you sure you want to resync ${checkedBoxes.length} properties?`)) {
        checkedBoxes.forEach(checkbox => {
            const propertyId = checkbox.value;
            resyncProperty(propertyId);
        });
    }
}

function bulkPushProperties(tabType) {
    const checkedBoxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    if (checkedBoxes.length === 0) return;
    
    if (confirm(`Are you sure you want to push ${checkedBoxes.length} properties to DataHub?`)) {
        // Disable all checkboxes and show progress
        const allCheckboxes = document.querySelectorAll(`#${tabType}-content .item-checkbox`);
        allCheckboxes.forEach(cb => cb.disabled = true);
        
        // Show initial progress notification
        showNotification('info', `Starting bulk push of ${checkedBoxes.length} properties...`);
        
        let successCount = 0;
        let errorCount = 0;
        const errors = [];
        
        // Process properties sequentially
        async function processNext(index) {
            if (index >= checkedBoxes.length) {
                // All done - show final result
                allCheckboxes.forEach(cb => cb.disabled = false);
                
                if (errorCount === 0) {
                    showNotification('success', `Successfully pushed ${successCount} properties to DataHub`);
                } else if (successCount === 0) {
                    showNotification('error', `Failed to push all ${errorCount} properties. First few errors: ${errors.slice(0, 2).join('; ')}`);
                } else {
                    showNotification('info', `Bulk push completed: ${successCount} successful, ${errorCount} failed. First few errors: ${errors.slice(0, 2).join('; ')}`);
                }
                
                // Reload data to refresh the view
                loadPropertiesData(true); // Skip sync validation to prevent mass status updates
                return;
            }
            
            const checkbox = checkedBoxes[index];
            const propertyId = checkbox.value;
            
            // Show progress for every 5th property or the last one
            if (index % 5 === 0 || index === checkedBoxes.length - 1) {
                showNotification('info', `Pushing property ${index + 1} of ${checkedBoxes.length}...`);
            }
            
            try {
                const response = await fetch(`/metadata/properties/${propertyId}/deploy/`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCSRFToken(),
                        'X-Requested-With': 'XMLHttpRequest'
                    }
                });
                
                const data = await response.json();
                
                if (data.success) {
                    successCount++;
                    console.log(`Successfully pushed property ${propertyId}`);
                } else {
                    errorCount++;
                    const errorMsg = data.error || 'Unknown error';
                    errors.push(`Property ${propertyId}: ${errorMsg}`);
                    console.error(`Failed to push property ${propertyId}:`, errorMsg);
                }
            } catch (error) {
                errorCount++;
                const errorMsg = error.message || 'Network error';
                errors.push(`Property ${propertyId}: ${errorMsg}`);
                console.error(`Error pushing property ${propertyId}:`, error);
            }
            
            // Wait a bit before processing next property to avoid overwhelming the server
            setTimeout(() => processNext(index + 1), 500);
        }
        
        // Start processing
        processNext(0);
    }
}

function bulkSyncToLocal(tabType) {
    const checkedBoxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    if (checkedBoxes.length === 0) return;
    
    if (confirm(`Are you sure you want to sync ${checkedBoxes.length} properties to local?`)) {
        checkedBoxes.forEach(checkbox => {
            const propertyData = JSON.parse(checkbox.getAttribute('data-property') || '{}');
            syncPropertyToLocal(propertyData);
        });
    }
}

function bulkDeleteLocalProperties(tabType) {
    const checkedBoxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    if (checkedBoxes.length === 0) {
        showNotification('error', 'Please select properties to delete.');
        return;
    }
    
    if (confirm(`Are you sure you want to delete ${checkedBoxes.length} local properties? This action cannot be undone.`)) {
        console.log(`Bulk delete ${checkedBoxes.length} local properties for ${tabType}`);
        
        // Show loading indicator
        showNotification('success', `Starting deletion of ${checkedBoxes.length} local properties...`);
        
        // Process each property sequentially
        let successCount = 0;
        let errorCount = 0;
        let processedCount = 0;
        
        // Create a function to process properties one by one
        function processNextProperty(index) {
            if (index >= checkedBoxes.length) {
                // All properties processed
                showNotification('success', `Completed: ${successCount} properties deleted, ${errorCount} failed.`);
                if (successCount > 0) {
                    // Refresh the data if any properties were successfully deleted
                    loadPropertiesData();
                }
                return;
            }
            
            const checkbox = checkedBoxes[index];
            const propertyId = checkbox.value;
            
            // Make the API call to delete this property
            fetch(`/metadata/properties/${propertyId}/delete/`, {
                method: 'DELETE',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': getCSRFToken(),
                    'X-Requested-With': 'XMLHttpRequest',
                }
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    successCount++;
                } else {
                    errorCount++;
                }
                processedCount++;
                
                // Update progress
                if (processedCount % 5 === 0 || processedCount === checkedBoxes.length) {
                    showNotification('success', `Progress: ${processedCount}/${checkedBoxes.length} properties processed`);
                }
                
                // Process the next property
                processNextProperty(index + 1);
            })
            .catch(error => {
                console.error(`Error deleting property ${propertyId}:`, error);
                errorCount++;
                processedCount++;
                
                // Process the next property despite the error
                processNextProperty(index + 1);
            });
        }
        
        // Start processing properties
        processNextProperty(0);
    }
}

function bulkDeleteRemoteProperties(tabType) {
    const checkedBoxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    if (checkedBoxes.length === 0) {
        showNotification('error', 'Please select properties to delete.');
        return;
    }
    
    if (confirm(`Are you sure you want to delete ${checkedBoxes.length} remote properties? This action cannot be undone.`)) {
        console.log(`Bulk delete ${checkedBoxes.length} remote properties for ${tabType}`);
        
        // Show loading indicator
        showNotification('success', `Starting deletion of ${checkedBoxes.length} remote properties...`);
        
        // Process each property sequentially
        let successCount = 0;
        let errorCount = 0;
        let processedCount = 0;
        
        // Create a function to process properties one by one
        function processNextProperty(index) {
            if (index >= checkedBoxes.length) {
                // All properties processed
                showNotification('success', `Completed: ${successCount} properties deleted, ${errorCount} failed.`);
                if (successCount > 0) {
                    // Refresh the data if any properties were successfully deleted
                    loadPropertiesData(true); // Skip sync validation to prevent mass status updates
                }
                return;
            }
            
            const checkbox = checkedBoxes[index];
            const propertyUrn = checkbox.value;
            
            // Make the API call to delete this remote property
            fetch('/metadata/properties/delete_remote/', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': getCSRFToken(),
                    'X-Requested-With': 'XMLHttpRequest',
                },
                body: JSON.stringify({
                    property_urn: propertyUrn
                })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    successCount++;
                } else {
                    errorCount++;
                }
                processedCount++;
                
                // Update progress
                if (processedCount % 5 === 0 || processedCount === checkedBoxes.length) {
                    showNotification('success', `Progress: ${processedCount}/${checkedBoxes.length} properties processed`);
                }
                
                // Process the next property
                processNextProperty(index + 1);
            })
            .catch(error => {
                console.error(`Error deleting remote property ${propertyUrn}:`, error);
                errorCount++;
                processedCount++;
                
                // Process the next property despite the error
                processNextProperty(index + 1);
            });
        }
        
        // Start processing properties
        processNextProperty(0);
    }
}

function bulkAddToPR(tabType) {
    const checkedBoxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    if (checkedBoxes.length === 0) return;
    
    if (confirm(`Are you sure you want to add ${checkedBoxes.length} properties to staged changes?`)) {
        const properties = Array.from(checkedBoxes).map(checkbox => 
            JSON.parse(checkbox.getAttribute('data-property') || '{}')
        );
        
        let processedCount = 0;
        const totalCount = properties.length;
        
        // Function to update progress
        const updateProgress = () => {
            processedCount++;
            if (processedCount === totalCount) {
                showNotification('success', `Successfully processed ${totalCount} properties for staged changes`);
                loadPropertiesData(true); // Refresh data
            }
        };
        
        // Process properties based on their type
        showNotification('info', `Processing ${totalCount} properties for staged changes...`);
        properties.forEach(property => {
            const propertyData = property.combined || property;
            const databaseId = getDatabaseId(propertyData);
            
            if (databaseId) {
                // Property already exists locally (synced or local-only), add directly to staged changes
                addPropertyToStagedChangesInternal(propertyData);
                updateProgress();
            } else {
                // Remote-only property - add directly to staged changes using remote endpoint
                addRemotePropertyToStagedChanges(property);
                updateProgress();
            }
        });
    }
}

/**
 * Global Actions Functions
 */

/**
 * Resync all properties
 */
function resyncAll() {
    if (!confirm('Are you sure you want to resync all properties? This will update all properties from DataHub.')) {
        return;
    }
    
    MetadataNotifications.show('sync', 'resync_all_start', 'property');
    
    fetch('/metadata/properties/resync_all/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCSRFToken(),
            'X-Requested-With': 'XMLHttpRequest',
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            MetadataNotifications.show('sync', 'resync_success', 'property', { count: data.count || 0 });
            loadPropertiesData(true); // Skip sync validation to prevent mass status updates
        } else {
            MetadataNotifications.show('sync', 'resync_error', 'property', { error: data.error || 'Failed to resync properties' });
        }
    })
    .catch(error => {
        console.error('Error:', error);
        MetadataNotifications.show('sync', 'resync_error', 'property', { error: 'Error resyncing properties' });
    });
}

/**
 * Export all properties to JSON
 */
function exportAll() {
    showNotification('info', 'Preparing export...');
    
    // Collect all properties from all tabs
    const allProperties = [
        ...(propertiesData.synced_items || []),
        ...(propertiesData.local_only_items || []),
        ...(propertiesData.remote_only_items || [])
    ];
    
    if (allProperties.length === 0) {
        showNotification('warning', 'No properties to export');
        return;
    }
    
    // Create a JSON object with all properties
    const exportData = {
        properties: allProperties,
        metadata: {
            exported_at: new Date().toISOString(),
            total_count: allProperties.length,
            synced_count: propertiesData.synced_items?.length || 0,
            local_only_count: propertiesData.local_only_items?.length || 0,
            remote_only_count: propertiesData.remote_only_items?.length || 0,
            source: window.location.origin,
            export_type: 'all_properties'
        }
    };
    
    // Convert to pretty JSON
    const jsonData = JSON.stringify(exportData, null, 2);
    
    // Create a blob and initiate download
    const blob = new Blob([jsonData], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    
    // Create temporary link and trigger download
    const link = document.createElement('a');
    link.href = url;
    link.download = `properties-export-all-${allProperties.length}-${new Date().toISOString().slice(0, 10)}.json`;
    document.body.appendChild(link);
    link.click();
    
    // Clean up
    setTimeout(() => {
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }, 100);
    
    showNotification('success', `${allProperties.length} properties exported successfully`);
}

/**
 * Add all properties to staged changes
 */
function addAllToStagedChanges() {
    if (!confirm('Are you sure you want to add ALL properties to staged changes? This will create MCP files for all properties in the current environment.')) {
        return;
    }
    
    // Get current environment and mutation name
    const currentEnvironment = window.currentEnvironment || { name: 'dev' };
    const mutationName = window.mutationName || null;
    
    showNotification('info', 'Starting to add all properties to staged changes...');
    
    fetch('/metadata/properties/add_all_to_staged_changes/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCSRFToken(),
            'X-Requested-With': 'XMLHttpRequest'
        },
        body: JSON.stringify({
            environment: currentEnvironment.name,
            mutation_name: mutationName
        })
    })
    .then(response => response.json())
    .then(data => {
        console.log('Add all to staged changes response:', data);
        if (data.success) {
            showNotification('success', data.message || `Successfully added ${data.success_count || 0} properties to staged changes`);
            if (data.success_count > 0) {
                loadPropertiesData(true); // Skip sync validation to prevent mass status updates
            }
        } else {
            showNotification('error', data.error || 'Failed to add properties to staged changes');
        }
    })
    .catch(error => {
        console.error('Error adding all properties to staged changes:', error);
        showNotification('error', `Error adding properties to staged changes: ${error.message}`);
    });
}

/**
 * Show import modal
 */
function showImportModal() {
    const modal = new bootstrap.Modal(document.getElementById('importJsonModal'));
    modal.show();
}

/**
 * Bulk download JSON for a specific tab
 */
function bulkDownloadJson(tabType) {
    const selectedProperties = getSelectedProperties(tabType);
    
    if (selectedProperties.length === 0) {
        showNotification('warning', 'Please select properties to download');
        return;
    }
    
    // Create JSON data
    const jsonData = JSON.stringify(selectedProperties, null, 2);
    const blob = new Blob([jsonData], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    
    // Create download link
    const a = document.createElement('a');
    a.style.display = 'none';
    a.href = url;
    a.download = `properties_${tabType}_${new Date().toISOString().split('T')[0]}.json`;
    document.body.appendChild(a);
    a.click();
    window.URL.revokeObjectURL(url);
    document.body.removeChild(a);
    
    showNotification('success', `Downloaded ${selectedProperties.length} properties`);
}

/**
 * Get selected properties for a specific tab
 */
function getSelectedProperties(tabType) {
    const checkboxes = document.querySelectorAll(`#${tabType}-content input[type="checkbox"]:checked:not(.select-all-checkbox)`);
    const selectedProperties = [];
    
    checkboxes.forEach(checkbox => {
        const propertyData = JSON.parse(checkbox.getAttribute('data-property') || '{}');
        if (propertyData.id) {
            selectedProperties.push(propertyData);
        }
    });
    
    return selectedProperties;
}

/**
 * Update bulk actions visibility
 */
function updateBulkActionsVisibility(tabType) {
    const selectedCount = document.querySelectorAll(`#${tabType}-content input[type="checkbox"]:checked:not(.select-all-checkbox)`).length;
    const bulkActions = document.getElementById(`${tabType}-bulk-actions`);
    const selectedCountElement = document.getElementById(`${tabType}-selected-count`);
    
    if (bulkActions) {
        if (selectedCount > 0) {
            bulkActions.classList.add('show');
        } else {
            bulkActions.classList.remove('show');
        }
    }
    
    if (selectedCountElement) {
        selectedCountElement.textContent = selectedCount;
    }
}

function clearAllFilters() {
    currentFilters.clear();
    
    // Remove active states from all filter stats
    document.querySelectorAll('.clickable-stat.multi-select').forEach(stat => {
        stat.classList.remove('active');
    });
    
    // Refresh current tab
    refreshCurrentTab();
}

function updateFilterDisplay() {
    // Update active states based on current filters
    document.querySelectorAll('.clickable-stat.multi-select').forEach(stat => {
        const filterType = stat.closest('#value-type-filters') ? 'valueType' : 'entityType';
        const filterValue = stat.getAttribute('data-filter');
        const filterKey = `${filterType}:${filterValue}`;
        
        if (currentFilters.has(filterKey)) {
            stat.classList.add('active');
        } else {
            stat.classList.remove('active');
        }
    });
}

function getValueTypeDisplayName(valueType) {
    if (valueType && typeof valueType === 'object') {
        if (valueType.info && valueType.info.displayName) {
            return valueType.info.displayName;
        }
        if (valueType.displayName) {
            return valueType.displayName;
        }
        if (valueType.type) {
            return valueType.type.charAt(0).toUpperCase() + valueType.type.slice(1).toLowerCase();
        }
        if (valueType.urn) {
            // Try to extract from URN
            const match = valueType.urn.match(/datahub\\.(\\w+)/);
            if (match) return match[1].charAt(0).toUpperCase() + match[1].slice(1).toLowerCase();
        }
    }
    if (typeof valueType === 'string') {
        return valueType.charAt(0).toUpperCase() + valueType.slice(1).toLowerCase();
    }
    return 'String';
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