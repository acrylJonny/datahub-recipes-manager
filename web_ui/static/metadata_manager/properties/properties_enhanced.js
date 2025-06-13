// Global variables
let propertiesData = [];
let filterData = {
    value_types: {},
    entity_types: {}
};
let currentFilters = new Set();

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    setupEventListeners();
    loadPropertiesData();
});

function setupEventListeners() {
    // Refresh button
    const refreshBtn = document.getElementById('refreshProperties');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', function() {
            loadPropertiesData();
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
}

function setupSearchListeners() {
    ['synced', 'local', 'remote'].forEach(tabType => {
        const searchInput = document.getElementById(`${tabType}-search`);
        const clearButton = document.getElementById(`${tabType}-clear`);
        
        if (searchInput) {
            searchInput.addEventListener('input', function() {
                filterAndRenderProperties(tabType);
            });
        }
        
        if (clearButton) {
            clearButton.addEventListener('click', function() {
                searchInput.value = '';
                filterAndRenderProperties(tabType);
            });
        }
    });
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
                    showNotification('Property updated successfully', 'success');
                    
                    // Close modal
                    const modal = bootstrap.Modal.getInstance(document.getElementById('editPropertyModal'));
                    modal.hide();
                    
                    // Refresh properties data
                    loadPropertiesData();
                } else {
                    showNotification(data.error || 'Failed to update property', 'error');
                }
            })
            .catch(error => {
                console.error('Error:', error);
                showNotification('Error updating property', 'error');
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

function loadPropertiesData() {
    // Show loading indicator
    const loadingIndicator = document.getElementById('loading-indicator');
    const propertiesContent = document.getElementById('properties-content');
    
    if (loadingIndicator) loadingIndicator.style.display = 'block';
    if (propertiesContent) propertiesContent.style.display = 'none';
    
    fetch('/metadata/properties/data/', {
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
            
            console.log('=== PROPERTIES DATA CATEGORIZATION ===');
            console.log('Raw data from server:', data.data.length, 'properties');
            console.log('Raw data sample:', data.data.slice(0, 3));
            console.log('Synced items:', propertiesData.synced_items.length, propertiesData.synced_items);
            console.log('Local only items:', propertiesData.local_only_items.length, propertiesData.local_only_items);
            console.log('Remote only items:', propertiesData.remote_only_items.length, propertiesData.remote_only_items);
            console.log('=== END CATEGORIZATION ===');
            
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
            showNotification(data.error || 'Failed to load properties data', 'error');
        }
    })
    .catch(error => {
        console.error('Error loading properties:', error);
        showNotification('Failed to load properties data', 'error');
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
    
    // Update overview statistics
    const totalEl = document.getElementById('total-items');
    const syncedEl = document.getElementById('synced-count');
    const localEl = document.getElementById('local-only-count');
    const remoteEl = document.getElementById('remote-only-count');
    
    if (totalEl) totalEl.textContent = totalCount;
    if (syncedEl) syncedEl.textContent = syncedCount;
    if (localEl) localEl.textContent = localCount;
    if (remoteEl) remoteEl.textContent = remoteCount;
    
    // Update tab badges
    const syncedBadge = document.getElementById('synced-badge');
    const localBadge = document.getElementById('local-badge');
    const remoteBadge = document.getElementById('remote-badge');
    
    if (syncedBadge) syncedBadge.textContent = syncedCount;
    if (localBadge) localBadge.textContent = localCount;
    if (remoteBadge) remoteBadge.textContent = remoteCount;
}

function updateFilterRows() {
    console.log('=== UPDATING FILTER ROWS ===');
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
            filterDiv.innerHTML = `
                <div class="h5 mb-0">${count}</div>
                <div class="text-muted">${valueType}</div>
            `;
            
            filterDiv.addEventListener('click', function() {
                toggleFilter('valueType', valueType);
                this.classList.toggle('active');
                refreshCurrentTab();
            });
            
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
            filterDiv.innerHTML = `
                <div class="h5 mb-0">${count}</div>
                <div class="text-muted">${entityType}</div>
            `;
            
            filterDiv.addEventListener('click', function() {
                toggleFilter('entityType', entityType);
                this.classList.toggle('active');
                refreshCurrentTab();
            });
            
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
    console.log('=== Rendering all tabs ===');
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
    }
}

function renderTab(tabId) {
    const tabType = tabId.replace('-items', '');
    const contentElement = document.getElementById(`${tabType}-content`);
    const searchInput = document.getElementById(`${tabType}-search`);
    const searchTerm = searchInput ? searchInput.value.toLowerCase() : '';
    
    console.log(`=== Rendering tab: ${tabId} (type: ${tabType}) ===`);
    
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
    console.log(`PropertiesData structure:`, {
        synced: propertiesData.synced_items?.length || 0,
        local: propertiesData.local_only_items?.length || 0,
        remote: propertiesData.remote_only_items?.length || 0
    });
    
    console.log(`Raw items for ${tabType}:`, items.length, items);
    
    // Apply search
    if (searchTerm) {
        items = items.filter(property => 
            (property.name || '').toLowerCase().includes(searchTerm) ||
            (property.description || '').toLowerCase().includes(searchTerm) ||
            (property.qualified_name || '').toLowerCase().includes(searchTerm)
        );
        console.log(`After search filter: ${items.length} items`);
    }
    
    // Apply filters
    items = applyFilters(items);
    console.log(`After applying filters: ${items.length} items`);
    
    // Render table directly like domains does
    let html = `
        <div class="table-responsive">
            <table class="table table-hover mb-0">
                <thead>
                    <tr>
                        <th width="40px"><input type="checkbox" class="form-check-input select-all-checkbox" id="selectAll${tabType.charAt(0).toUpperCase() + tabType.slice(1)}"></th>
                        <th class="sortable-header" data-sort="name">Name</th>
                        <th width="200px" class="sortable-header" data-sort="urn">URN</th>
                        <th class="sortable-header" data-sort="value_type">Value Type</th>
                        <th class="sortable-header" data-sort="cardinality">Cardinality</th>
                        <th class="sortable-header" data-sort="entity_types">Entity Types</th>
                        <th>Description</th>
                        <th width="200px">Actions</th>
                    </tr>
                </thead>
                <tbody>
    `;
    
    if (items.length === 0) {
        console.log(`No items to render for ${tabType}`);
        html += `
                    <tr>
                        <td colspan="8" class="text-center py-4 text-muted">
                            <i class="fas fa-inbox fa-2x mb-2"></i><br>
                            No ${tabType} properties found
                        </td>
                    </tr>
        `;
    } else {
        console.log(`Rendering ${items.length} rows for ${tabType}`);
        items.forEach((property, index) => {
            console.log(`Rendering property ${index}:`, property.name);
            html += renderPropertyRow(property, tabType);
        });
    }
    
    html += `
                </tbody>
            </table>
        </div>
    `;
    
    console.log(`Setting innerHTML for ${tabType}-content`);
    contentElement.innerHTML = html;
    console.log(`Tab ${tabType} rendered successfully with HTML length:`, html.length);
    
    // Setup bulk selection and sorting after rendering
    setupBulkSelectionForTab(tabType);
    attachSortingHandlers(contentElement, tabType);
}

// Tab content is rendered by renderAllTabs(), no need for individual loading

function renderPropertiesTable(properties, tabType, container) {
    console.log(`renderPropertiesTable: ${tabType} with ${properties.length} properties`);
    console.log('Properties data:', properties);
    console.log('Container:', container);
    
    if (properties.length === 0) {
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
    
    console.log('Generating table HTML...');
    const tableHtml = `
        <div class="table-responsive">
            <table class="table table-hover mb-0">
                <thead>
                    <tr>
                        <th width="40px">
                            <input type="checkbox" class="form-check-input select-all-checkbox" id="selectAll${tabType.charAt(0).toUpperCase() + tabType.slice(1)}">
                        </th>
                        <th>Name</th>
                        <th width="200px">URN</th>
                        <th>Value Type</th>
                        <th>Cardinality</th>
                        <th>Entity Types</th>
                        <th>Description</th>
                        <th width="200px">Actions</th>
                    </tr>
                </thead>
                <tbody>
                    ${properties.map(property => renderPropertyRow(property, tabType)).join('')}
                </tbody>
            </table>
        </div>
    `;
    
    console.log('Setting innerHTML...');
    container.innerHTML = tableHtml;
    console.log('Table rendered successfully');
    
    // Setup bulk selection
    setupBulkSelection(tabType);
}

function renderPropertyRow(property, tabType) {
    // Debug logging
    console.log('renderPropertyRow called with:', property.name, property.urn);
    console.log('Entity types:', property.entity_types);
    console.log('Value type:', property.value_type);
    
    const statusBadge = getStatusBadge(property.status);
    const actionButtons = getActionButtons(property, tabType);
    
    // Handle missing name gracefully
    const propertyName = property.name || property.qualified_name || 'Unnamed Property';
    const propertyUrn = property.urn || '';
    
    console.log('URN for truncation:', propertyUrn, 'truncated:', truncateUrn(propertyUrn, 40));
    
    return `
        <tr>
            <td>
                <input type="checkbox" class="form-check-input item-checkbox" value="${property.id || property.urn}">
            </td>
            <td>
                <div class="d-flex align-items-center">
                    <strong>${escapeHtml(propertyName)}</strong>
                    ${statusBadge}
                </div>
                <small class="text-muted">${escapeHtml(property.qualified_name || '')}</small>
            </td>
            <td class="urn-cell">
                <code class="text-muted small urn-truncate" title="${escapeHtml(propertyUrn)}">${escapeHtml(truncateUrn(propertyUrn, 40))}</code>
            </td>
            <td>
                <span class="badge bg-info">${escapeHtml(property.value_type || 'STRING')}</span>
            </td>
            <td>
                <span class="badge bg-secondary">${escapeHtml(property.cardinality || 'SINGLE')}</span>
            </td>
            <td>
                <div class="entity-types">
                    ${renderEntityTypes(property.entity_types, property.value_type)}
                </div>
            </td>
            <td>
                <div class="description-preview">
                    ${escapeHtml(property.description || 'No description')}
                </div>
            </td>
            <td>
                <div class="btn-group" role="group">
                    ${actionButtons}
                </div>
            </td>
        </tr>
    `;
}

function getStatusBadge(status) {
    switch (status) {
        case 'synced':
            return '<span class="badge bg-success ms-2">Synced</span>';
        case 'local_only':
            return '<span class="badge bg-secondary ms-2">Local</span>';
        case 'remote_only':
            return '<span class="badge bg-info ms-2">Remote</span>';
        default:
            return '';
    }
}

function getActionButtons(property, tabType) {
    const buttons = [];
    const propertyId = property.id;
    const propertyUrn = property.urn;
    const status = property.status;
    
    console.log(`Getting action buttons for property: ${property.name}, id: ${propertyId}, status: ${status}`);
    
    // View button - always available
    buttons.push(`<button type="button" class="btn btn-sm btn-outline-primary view-property" onclick="viewProperty('${propertyUrn || propertyId}')" title="View Details">
        <i class="fas fa-eye"></i>
    </button>`);
    
    if (tabType === 'synced') {
        // Edit button
        buttons.push(`<button type="button" class="btn btn-sm btn-outline-secondary edit-property" onclick="editProperty('${propertyUrn || propertyId}')" title="Edit">
            <i class="fas fa-edit"></i>
        </button>`);
        // Add to PR button
        buttons.push(`<button type="button" class="btn btn-sm btn-outline-warning add-property-to-pr" onclick="addPropertyToPR('${propertyId}')" title="Add to PR">
            <i class="fab fa-github"></i>
        </button>`);
        // Sync/Resync button
        buttons.push(`<button type="button" class="btn btn-sm btn-outline-info resync-property" onclick="resyncProperty('${propertyId}')" title="Resync">
            <i class="fas fa-sync-alt"></i>
        </button>`);
        // Push to DataHub button
        buttons.push(`<button type="button" class="btn btn-sm btn-outline-success push-property" onclick="pushPropertyToDataHub('${propertyId}')" title="Push to DataHub">
            <i class="fas fa-upload"></i>
        </button>`);
        // Delete local button
        buttons.push(`<button type="button" class="btn btn-sm btn-outline-danger delete-local-property" onclick="deleteLocalProperty('${propertyId}')" title="Delete Local">
            <i class="fas fa-trash"></i>
        </button>`);
    } else if (tabType === 'local') {
        // Edit button
        buttons.push(`<button type="button" class="btn btn-sm btn-outline-secondary edit-property" onclick="editProperty('${propertyUrn || propertyId}')" title="Edit">
            <i class="fas fa-edit"></i>
        </button>`);
        // Add to PR button
        buttons.push(`<button type="button" class="btn btn-sm btn-outline-warning add-property-to-pr" onclick="addPropertyToPR('${propertyId}')" title="Add to PR">
            <i class="fab fa-github"></i>
        </button>`);
        // Push to DataHub button
        buttons.push(`<button type="button" class="btn btn-sm btn-outline-success push-property" onclick="pushPropertyToDataHub('${propertyId}')" title="Push to DataHub">
            <i class="fas fa-upload"></i>
        </button>`);
        // Delete button
        buttons.push(`<button type="button" class="btn btn-sm btn-outline-danger delete-local-property" onclick="deleteLocalProperty('${propertyId}')" title="Delete">
            <i class="fas fa-trash"></i>
        </button>`);
    } else if (tabType === 'remote') {
        // Sync to Local button
        buttons.push(`<button type="button" class="btn btn-sm btn-outline-primary sync-property-to-local" onclick="syncPropertyToLocal('${propertyUrn}')" title="Sync to Local">
            <i class="fas fa-download"></i>
        </button>`);
        // Add to PR button
        buttons.push(`<button type="button" class="btn btn-sm btn-outline-warning add-remote-property-to-pr" onclick="addRemotePropertyToPR('${propertyUrn}')" title="Add to PR">
            <i class="fab fa-github"></i>
        </button>`);
        // Delete remote button
        buttons.push(`<button type="button" class="btn btn-sm btn-outline-danger delete-remote-property" onclick="deleteRemoteProperty('${propertyUrn}')" title="Delete Remote">
            <i class="fas fa-trash"></i>
        </button>`);
    }
    
    return buttons.join(' ');
}

function setupBulkSelectionForTab(tabType) {
    const selectAllCheckbox = document.getElementById(`selectAll${tabType.charAt(0).toUpperCase() + tabType.slice(1)}`);
    const tabContent = document.getElementById(`${tabType}-content`);
    
    if (selectAllCheckbox && tabContent) {
        // Remove existing listeners to avoid duplicates
        selectAllCheckbox.replaceWith(selectAllCheckbox.cloneNode(true));
        const newSelectAllCheckbox = document.getElementById(`selectAll${tabType.charAt(0).toUpperCase() + tabType.slice(1)}`);
        
        newSelectAllCheckbox.addEventListener('change', function() {
            const itemCheckboxes = tabContent.querySelectorAll('.item-checkbox');
            itemCheckboxes.forEach(cb => {
                cb.checked = this.checked;
            });
            updateBulkActionsVisibility();
        });
        
        // Setup individual checkbox listeners
        const itemCheckboxes = tabContent.querySelectorAll('.item-checkbox');
        itemCheckboxes.forEach(checkbox => {
            checkbox.addEventListener('change', function() {
                updateBulkActionsVisibility();
                
                // Update select all checkbox state
                const allCheckboxes = tabContent.querySelectorAll('.item-checkbox');
                const checkedCheckboxes = tabContent.querySelectorAll('.item-checkbox:checked');
                
                if (newSelectAllCheckbox) {
                    newSelectAllCheckbox.checked = allCheckboxes.length > 0 && allCheckboxes.length === checkedCheckboxes.length;
                    newSelectAllCheckbox.indeterminate = checkedCheckboxes.length > 0 && checkedCheckboxes.length < allCheckboxes.length;
                }
            });
        });
    }
}

// Global sorting state for properties
let currentSort = { column: null, direction: null, tabType: null };

// Attach sorting handlers to table headers - matching assertions implementation
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
            
            // Update header classes in this table only
            const tableHeaders = content.querySelectorAll('.sortable-header');
            tableHeaders.forEach(h => {
                h.classList.remove('sort-asc', 'sort-desc');
            });
            this.classList.add(currentSort.direction === 'asc' ? 'sort-asc' : 'sort-desc');
            
            // Sort the current table
            sortCurrentTable(content, tabType);
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
    
    console.log(`✅ Sorted ${rows.length} rows by ${currentSort.column} (${currentSort.direction})`);
}

// Get sort value from a table row for properties
function getSortValueFromRow(row, column) {
    const cells = row.querySelectorAll('td');
    
    switch(column) {
        case 'name':
            return cells[1]?.textContent?.trim().toLowerCase() || ''; // Skip checkbox column
        case 'urn':
            return cells[2]?.textContent?.trim().toLowerCase() || '';
        case 'value_type':
            return cells[3]?.textContent?.trim().toLowerCase() || '';
        case 'cardinality':
            return cells[4]?.textContent?.trim().toLowerCase() || '';
        case 'entity_types':
            return cells[5]?.textContent?.trim().toLowerCase() || '';
        default:
            return '';
    }
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
        showNotification('Property not found', 'error');
        return;
    }
    
    // Populate modal with property data
    populatePropertyViewModal(property);
    
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
        showNotification('Property not found', 'error');
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
    
    // Populate form fields
    document.getElementById('editPropertyId').value = property.id;
    document.getElementById('editPropertyName').value = property.name || '';
    document.getElementById('editPropertyDescription').value = property.description || '';
    document.getElementById('editPropertyQualifiedName').value = property.qualified_name || '';
    document.getElementById('editPropertyValueType').value = property.value_type || 'STRING';
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
    document.getElementById('editImmutable').checked = property.immutable || false;
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
            showNotification(data.message, 'success');
            // Optionally reload the data to reflect any status changes
            loadPropertiesData();
        } else {
            showNotification(data.error || 'Failed to add property to PR', 'error');
        }
    })
    .catch(error => {
        console.error('Error adding property to PR:', error);
        showNotification('Failed to add property to PR', 'error');
    })
    .finally(() => {
        if (button) {
            button.disabled = false;
            button.innerHTML = '<i class="fab fa-github"></i>';
        }
    });
}

function addRemotePropertyToPR(propertyUrn) {
    console.log('Adding remote property to PR:', propertyUrn);
    
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
            showNotification(data.message, 'success');
        } else {
            showNotification(data.error, 'error');
        }
    })
    .catch(error => {
        console.error('Error adding property to PR:', error);
        showNotification('Failed to add property to PR', 'error');
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
            showNotification(data.message, 'success');
            // Reload data to refresh the view
            loadPropertiesData();
        } else {
            showNotification(data.error, 'error');
        }
    })
    .catch(error => {
        console.error('Error resyncing property:', error);
        showNotification('Failed to resync property', 'error');
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
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showNotification(data.message, 'success');
            // Reload data to refresh the view
            loadPropertiesData();
        } else {
            showNotification(data.error, 'error');
        }
    })
    .catch(error => {
        console.error('Error pushing property to DataHub:', error);
        showNotification('Failed to push property to DataHub', 'error');
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
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showNotification(data.message || 'Property deleted successfully', 'success');
            // Reload data to refresh the view
            loadPropertiesData();
        } else {
            showNotification(data.error, 'error');
        }
    })
    .catch(error => {
        console.error('Error deleting property:', error);
        showNotification('Failed to delete property', 'error');
    })
    .finally(() => {
        if (button) {
            button.disabled = false;
            button.innerHTML = '<i class="fas fa-trash"></i>';
        }
    });
}

function syncPropertyToLocal(propertyUrn) {
    console.log(`Syncing property to local: ${propertyUrn}`);
    
    const button = document.querySelector(`button[onclick="syncPropertyToLocal('${propertyUrn}')"]`);
    if (button) {
        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
    }
    
    fetch('/metadata/properties/sync/', {
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
            showNotification(data.message, 'success');
            // Reload data to refresh the view
            loadPropertiesData();
        } else {
            showNotification(data.error, 'error');
        }
    })
    .catch(error => {
        console.error('Error syncing property:', error);
        showNotification('Failed to sync property to local', 'error');
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
        },
        body: `property_urn=${encodeURIComponent(propertyUrn)}`
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showNotification(data.message, 'success');
            // Reload data to refresh the view
            loadPropertiesData();
        } else {
            showNotification(data.error, 'error');
        }
    })
    .catch(error => {
        console.error('Error deleting remote property:', error);
        showNotification('Failed to delete remote property', 'error');
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

function showNotification(message, type = 'info') {
    // Create notification element
    const notification = document.createElement('div');
    notification.className = `alert alert-${type === 'error' ? 'danger' : type} alert-dismissible fade show notification`;
    notification.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    // Add to page
    document.body.appendChild(notification);
    
    // Auto-remove after 5 seconds
    setTimeout(() => {
        if (notification.parentNode) {
            notification.remove();
        }
    }, 5000);
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
    
    return validTypes.map(type => 
        `<span class="badge bg-light text-dark entity-type-badge">${escapeHtml(type)}</span>`
    ).join('');
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
        checkedBoxes.forEach(checkbox => {
            const propertyId = checkbox.value;
            pushPropertyToDataHub(propertyId);
        });
    }
}

function bulkSyncToLocal(tabType) {
    const checkedBoxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    if (checkedBoxes.length === 0) return;
    
    if (confirm(`Are you sure you want to sync ${checkedBoxes.length} properties to local?`)) {
        checkedBoxes.forEach(checkbox => {
            const propertyUrn = checkbox.value;
            syncPropertyToLocal(propertyUrn);
        });
    }
}

function bulkDeleteLocalProperties(tabType) {
    const checkedBoxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    if (checkedBoxes.length === 0) return;
    
    if (confirm(`Are you sure you want to delete ${checkedBoxes.length} local properties? This action cannot be undone.`)) {
        checkedBoxes.forEach(checkbox => {
            const propertyId = checkbox.value;
            deleteLocalProperty(propertyId);
        });
    }
}

function bulkDeleteRemoteProperties(tabType) {
    const checkedBoxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    if (checkedBoxes.length === 0) return;
    
    if (confirm(`Are you sure you want to delete ${checkedBoxes.length} remote properties? This action cannot be undone.`)) {
        checkedBoxes.forEach(checkbox => {
            const propertyUrn = checkbox.value;
            deleteRemoteProperty(propertyUrn);
        });
    }
}

function bulkAddToPR(tabType) {
    const checkedBoxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    if (checkedBoxes.length === 0) return;
    
    if (confirm(`Are you sure you want to add ${checkedBoxes.length} properties to PR?`)) {
        checkedBoxes.forEach(checkbox => {
            const propertyId = checkbox.value;
            const propertyUrn = checkbox.value;
            
            if (tabType === 'remote') {
                addRemotePropertyToPR(propertyUrn);
            } else {
                addPropertyToPR(propertyId);
            }
        });
    }
}