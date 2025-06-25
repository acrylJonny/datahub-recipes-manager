let domainsData = {};
let currentFilters = new Set();
let currentOverviewFilter = null;

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

// DOM ready
document.addEventListener('DOMContentLoaded', function() {
    loadDomainsData();
    setupEventListeners();
    setupFilterListeners();
    setupBulkActions();
    setupSearchHandlers();
    setupModalHandlers();
});

function setupEventListeners() {
    // Refresh button
    document.getElementById('refreshDomains').addEventListener('click', function() {
        loadDomainsData();
    });
    
    // Tab switching
    document.querySelectorAll('#domainTabs button[data-bs-toggle="tab"]').forEach(tab => {
        tab.addEventListener('shown.bs.tab', function(e) {
            const tabId = e.target.getAttribute('aria-controls');
            renderTab(tabId);
        });
    });
    
    // Hierarchy expand/collapse buttons (using event delegation)
    document.addEventListener('click', function(e) {
        if (e.target.closest('.expand-button')) {
            e.preventDefault();
            e.stopPropagation();
            
            const button = e.target.closest('.expand-button');
            const row = button.closest('tr');
            const domainUrn = row.getAttribute('data-urn');
            
            if (domainUrn) {
                toggleDomainExpansion(domainUrn);
            }
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
    
    // Multi-select filters
    document.querySelectorAll('.clickable-stat.multi-select').forEach(stat => {
        stat.addEventListener('click', function() {
            const filter = this.getAttribute('data-filter');
            
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

function setupBulkActions() {
    // Select all checkboxes
    document.querySelectorAll('.select-all-checkbox').forEach(checkbox => {
        checkbox.addEventListener('change', function() {
            const tabType = this.id.replace('selectAll', '').toLowerCase();
            const tabContent = document.getElementById(`${tabType}-content`);
            const itemCheckboxes = tabContent.querySelectorAll('.item-checkbox');
            
            itemCheckboxes.forEach(cb => {
                cb.checked = this.checked;
            });
            
            updateBulkActionsVisibility();
        });
    });
    
    // Individual item checkboxes
    document.addEventListener('change', function(e) {
        if (e.target.classList.contains('item-checkbox')) {
            updateBulkActionsVisibility();
            
            // Update select all checkbox state
            const tabContent = e.target.closest('[id$="-content"]');
            const tabType = tabContent.id.replace('-content', '');
            const selectAllCheckbox = document.getElementById(`selectAll${tabType.charAt(0).toUpperCase() + tabType.slice(1)}`);
            const allCheckboxes = tabContent.querySelectorAll('.item-checkbox');
            const checkedCheckboxes = tabContent.querySelectorAll('.item-checkbox:checked');
            
            if (selectAllCheckbox) {
                selectAllCheckbox.checked = allCheckboxes.length > 0 && allCheckboxes.length === checkedCheckboxes.length;
                selectAllCheckbox.indeterminate = checkedCheckboxes.length > 0 && checkedCheckboxes.length < allCheckboxes.length;
            }
        }
    });
}

function setupSearchHandlers() {
    ['synced', 'local', 'remote'].forEach(tabType => {
        const searchInput = document.getElementById(`${tabType}-search`);
        const clearButton = document.getElementById(`${tabType}-clear`);
        
        if (searchInput) {
            searchInput.addEventListener('input', function() {
                renderTab(`${tabType}-items`);
            });
        }
        
        if (clearButton) {
            clearButton.addEventListener('click', function() {
                searchInput.value = '';
                renderTab(`${tabType}-items`);
            });
        }
    });
}

function setupModalHandlers() {
    // Create domain modal handler
    const createForm = document.getElementById('createDomainForm');
    if (createForm) {
        createForm.onsubmit = function(e) {
            e.preventDefault();
            
            const formData = new FormData(createForm);
            
            fetch('/metadata/domains/create/', {
                method: 'POST',
                body: formData,
                headers: {
                    'X-CSRFToken': getCsrfToken(),
                }
            })
            .then(response => {
                if (response.redirected) {
                    // Success - reload the domains data
                    loadDomainsData();
                    showSuccess(`Domain '${formData.get('name')}' created successfully`);
                    // Close the modal and reset form
                    bootstrap.Modal.getInstance(document.getElementById('createDomainModal')).hide();
                    createForm.reset();
                } else {
                    throw new Error('Failed to create domain');
                }
            })
            .catch(error => {
                console.error('Error creating domain:', error);
                showError('Failed to create domain: ' + error.message);
            });
        };
    }
}

function loadDomainsData() {
    const loadingIndicator = document.getElementById('loading-indicator');
    const domainsContent = document.getElementById('domains-content');
    
    loadingIndicator.style.display = 'block';
    domainsContent.style.display = 'none';
    
    fetch('/metadata/domains/data/', {
        method: 'GET',
        headers: {
            'X-Requested-With': 'XMLHttpRequest',
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            domainsData = data.data;
            updateStatistics(data);
            renderAllTabs();
            
            loadingIndicator.style.display = 'none';
            domainsContent.style.display = 'block';
        } else {
            throw new Error(data.error || 'Failed to load domains data');
        }
    })
    .catch(error => {
        console.error('Error loading domains:', error);
        showError('Failed to load domains data: ' + error.message);
        loadingIndicator.style.display = 'none';
    });
}

function updateStatistics(data) {
    // Add null checks for data and statistics
    if (!data || !data.data) {
        console.warn('Invalid data structure passed to updateStatistics:', data);
        return;
    }
    
    const actualData = data.data || data;
    const stats = actualData.statistics || {};
    
    // Update overview statistics
    document.getElementById('total-items').textContent = stats.total_items || 0;
    document.getElementById('synced-count').textContent = stats.synced_count || 0;
    document.getElementById('local-only-count').textContent = (stats.total_items || 0) - (stats.synced_count || 0) - (actualData.remote_only_items?.length || 0);
    document.getElementById('remote-only-count').textContent = actualData.remote_only_items?.length || 0;
    
    // Update filter statistics - use correct element IDs
    document.getElementById('domains-with-owners').textContent = stats.owned_items || 0;
    document.getElementById('domains-with-entities').textContent = stats.items_with_entities || 0;
    document.getElementById('domains-with-structured-properties').textContent = stats.items_with_structured_properties || 0;
    
    // Update tab badges
    document.getElementById('synced-badge').textContent = stats.synced_count || 0;
    document.getElementById('local-badge').textContent = (stats.total_items || 0) - (stats.synced_count || 0) - (actualData.remote_only_items?.length || 0);
    document.getElementById('remote-badge').textContent = actualData.remote_only_items?.length || 0;
}

function applyFilters() {
    renderAllTabs();
}

function filterDomains(domains) {
    return domains.filter(domain => {
        // Get the actual domain data (combined for synced, direct for others)
        const domainData = domain.combined || domain;
        
        // Apply overview filter
        if (currentOverviewFilter) {
            switch (currentOverviewFilter) {
                case 'total':
                    break; // Show all
                case 'synced':
                    if (domainData.sync_status !== 'SYNCED') return false;
                    break;
                case 'local-only':
                    if (domainData.sync_status !== 'LOCAL_ONLY') return false;
                    break;
                case 'remote-only':
                    if (domainData.sync_status !== 'REMOTE_ONLY') return false;
                    break;
                case 'modified':
                    if (domainData.sync_status !== 'MODIFIED') return false;
                    break;
            }
        }
        
        // Apply multi-select filters
        if (currentFilters.size > 0) {
            let passesFilter = false;
            
            for (const filter of currentFilters) {
                switch (filter) {
                    case 'owned':
                        if ((domainData.owners_count || 0) > 0) passesFilter = true;
                        break;
                    case 'unowned':
                        if ((domainData.owners_count || 0) === 0) passesFilter = true;
                        break;

                    case 'with-structured-properties':
                        if ((domainData.structured_properties_count || 0) > 0) passesFilter = true;
                        break;
                    case 'with-entities':
                        if ((domainData.entities_count || 0) > 0) passesFilter = true;
                        break;
                    case 'with-relationships':
                        // Remove this filter - domains don't have relationships
                        break;
                    case 'synced':
                        if (domainData.sync_status === 'SYNCED') passesFilter = true;
                        break;
                    case 'local-only':
                        if (domainData.sync_status === 'LOCAL_ONLY') passesFilter = true;
                        break;
                    case 'remote-only':
                        if (domainData.sync_status === 'REMOTE_ONLY') passesFilter = true;
                        break;
                    case 'modified':
                        if (domainData.sync_status === 'MODIFIED') passesFilter = true;
                        break;
                }
            }
            
            if (!passesFilter) return false;
        }
        
        return true;
    });
}

function renderAllTabs() {
    // Add sample hierarchy to domains for demonstration
    addSampleHierarchy();
    
    renderTab('synced-items');
    renderTab('local-items');
    renderTab('remote-items');
}

function addSampleHierarchy() {
    // Calculate real hierarchy based on parent_urn relationships
    calculateHierarchy();
}

function calculateHierarchy() {
    // Process each tab's domains to calculate hierarchy
    processTabHierarchy(domainsData.synced_items, 'combined');
    processTabHierarchy(domainsData.local_only_items, 'direct');
    processTabHierarchy(domainsData.remote_only_items, 'direct');
}

function processTabHierarchy(items, dataAccessType) {
    if (!items || items.length === 0) return;
    
    console.log(`[DEBUG] Processing hierarchy for ${items.length} items with dataAccessType: ${dataAccessType}`);
    
    // Create lookup maps and extract parent URNs
    const domainLookup = new Map();
    const childrenMap = new Map();
    const rootDomains = [];
    
    // Build a URN mapping for parent resolution (DataHub ID URN -> Deterministic URN)
    const urnMapping = new Map();
    items.forEach(item => {
        const domain = dataAccessType === 'combined' ? (item.combined || item) : item;
        // Map both ways: deterministic URN and any DataHub ID URN if available
        urnMapping.set(domain.urn, domain.urn);
        
        // If this is a combined domain, check if we have a DataHub ID we can map
        if (dataAccessType === 'combined' && item.local && item.local.datahub_id) {
            const datahubUrn = `urn:li:domain:${item.local.datahub_id}`;
            urnMapping.set(datahubUrn, domain.urn);
        }
        // Also check remote data for DataHub URN
        if (dataAccessType === 'combined' && item.remote && item.remote.urn) {
            urnMapping.set(item.remote.urn, domain.urn);
        }
    });
    
    console.log(`[DEBUG] Built URN mapping with ${urnMapping.size} entries`);
    
    // First pass: build lookup map and extract parent URNs
    items.forEach(item => {
        const domain = dataAccessType === 'combined' ? (item.combined || item) : item;
        domainLookup.set(domain.urn, domain);
        
        // Extract parent URN from parentDomains structure
        let parentUrn = null;
        if (domain.parentDomains && domain.parentDomains.domains && domain.parentDomains.domains.length > 0) {
            parentUrn = domain.parentDomains.domains[0].urn;
        } else if (domain.parent_urn) {
            parentUrn = domain.parent_urn;
        }
        
        // Resolve parent URN using our mapping (DataHub ID URN -> Deterministic URN)
        if (parentUrn && urnMapping.has(parentUrn)) {
            parentUrn = urnMapping.get(parentUrn);
        }
        
        // Store the resolved parent URN back on the domain
        domain.parent_urn = parentUrn;
        
        if (dataAccessType === 'combined') {
            console.log(`[DEBUG] ${dataAccessType} - Domain: ${domain.name}, URN: ${domain.urn}, Parent URN: ${parentUrn} ${parentUrn !== domain.parent_urn ? `(resolved from ${domain.parent_urn})` : ''}`);
        }
        
        // Build children map and identify roots
        if (parentUrn) {
            if (!childrenMap.has(parentUrn)) {
                childrenMap.set(parentUrn, []);
            }
            childrenMap.get(parentUrn).push(domain);
        } else {
            // This is a root domain (no parent)
            rootDomains.push(domain);
        }
    });
    
    console.log(`[DEBUG] ${dataAccessType} - Found ${rootDomains.length} root domains and ${childrenMap.size} parent-child relationships`);
    
    // Build hierarchy starting from root domains
    const hierarchyOrder = [];
    
    function addDomainToHierarchy(domain, level, isLastChild = false) {
        // Set hierarchy properties
        domain.hierarchy_level = level;
        domain.has_children = childrenMap.has(domain.urn);
        domain.is_expanded = false; // Default collapsed
        domain.is_last_child = isLastChild;
        
        if (dataAccessType === 'combined' && level > 0) {
            console.log(`[DEBUG] ${dataAccessType} - Setting hierarchy for ${domain.name}: level=${level}, has_children=${domain.has_children}, parent_urn=${domain.parent_urn}`);
        }
        
        // Add to hierarchy order
        hierarchyOrder.push(domain);
        
        // Add children recursively
        const children = childrenMap.get(domain.urn) || [];
        children.forEach((child, index) => {
            const isLastChildInGroup = index === children.length - 1;
            addDomainToHierarchy(child, level + 1, isLastChildInGroup);
        });
    }
    
    // Start with root domains
    rootDomains.forEach((rootDomain, index) => {
        const isLastRoot = index === rootDomains.length - 1;
        addDomainToHierarchy(rootDomain, 0, isLastRoot);
    });
    
    console.log(`[DEBUG] ${dataAccessType} - Built hierarchy with ${hierarchyOrder.length} domains`);
    
    // Rebuild items array in hierarchy order
    if (hierarchyOrder.length > 0) {
        // Store reference to original items before clearing
        const originalItems = [...items];
        
        // Clear the original items and replace with hierarchy order
        items.length = 0;
        hierarchyOrder.forEach(domain => {
            if (dataAccessType === 'combined') {
                // Find the original item that contains this domain
                const originalItem = originalItems.find(item => {
                    const itemDomain = item.combined || item;
                    return itemDomain.urn === domain.urn;
                });
                if (originalItem) {
                    // Transfer hierarchy properties to the combined data
                    if (originalItem.combined) {
                        originalItem.combined.hierarchy_level = domain.hierarchy_level;
                        originalItem.combined.has_children = domain.has_children;
                        originalItem.combined.is_expanded = domain.is_expanded;
                        originalItem.combined.is_last_child = domain.is_last_child;
                        originalItem.combined.parent_urn = domain.parent_urn;
                        
                        if (domain.hierarchy_level > 0) {
                            console.log(`[DEBUG] Transferred hierarchy to combined data for ${originalItem.combined.name}: level=${originalItem.combined.hierarchy_level}, parent_urn=${originalItem.combined.parent_urn}`);
                        }
                    }
                    items.push(originalItem);
                } else {
                    // Create wrapper for combined access
                    items.push({ combined: domain });
                }
            } else {
                items.push(domain);
            }
        });
    }
}

function renderTab(tabId) {
    const tabType = tabId.replace('-items', '');
    const contentElement = document.getElementById(`${tabType}-content`);
    const searchInput = document.getElementById(`${tabType}-search`);
    const searchTerm = searchInput ? searchInput.value.toLowerCase() : '';
    
    if (!contentElement) return;
    
    let items = [];
    
    switch (tabType) {
        case 'synced':
            items = domainsData.synced_items || [];
            break;
        case 'local':
            items = domainsData.local_only_items || [];
            break;
        case 'remote':
            items = domainsData.remote_only_items || [];
            break;
    }
    
    // Apply filters
    items = filterDomains(items);
    
    // Apply search
    if (searchTerm) {
        items = items.filter(item => {
            const domainData = item.combined || item;
            return (domainData.name || '').toLowerCase().includes(searchTerm) ||
                   (domainData.description || '').toLowerCase().includes(searchTerm) ||
                   (domainData.urn || '').toLowerCase().includes(searchTerm);
        });
    }
    
    // Apply sorting
    if (currentSort.column && currentSort.tabType === tabType) {
        items = sortDomains(items, currentSort.column, currentSort.direction);
    }
    
    // Use new pagination system
    const pagination = currentPagination[tabType];
    const currentPage = pagination.page;
    const itemsPerPage = pagination.itemsPerPage;
    
    // Separate root nodes from all items for pagination
    const rootNodes = items.filter(item => {
        const domainData = item.combined || item;
        return !domainData.parent_urn || domainData.hierarchy_level === 0;
    });
    
    const totalRootNodes = rootNodes.length;
    const totalPages = Math.ceil(totalRootNodes / itemsPerPage);
    const rootStartIndex = (currentPage - 1) * itemsPerPage;
    const rootEndIndex = Math.min(rootStartIndex + itemsPerPage, totalRootNodes);
    const pageRootNodes = rootNodes.slice(rootStartIndex, rootEndIndex);
    
    // Get all items that should be displayed (root nodes + their descendants)
    const pageItems = [];
    const rootUrns = new Set(pageRootNodes.map(item => {
        const domainData = item.combined || item;
        return domainData.urn;
    }));
    
    function includeDescendants(parentUrn) {
        return items.filter(item => {
            const domainData = item.combined || item;
            return domainData.parent_urn === parentUrn;
        });
    }
    
    function addItemAndDescendants(item) {
        pageItems.push(item);
        const domainData = item.combined || item;
        
        // Always add descendants to HTML, but control visibility through CSS/JS
        const descendants = includeDescendants(domainData.urn);
        descendants.forEach(descendant => {
            addItemAndDescendants(descendant);
        });
    }
    
    // Add root nodes and their expanded descendants only
    pageRootNodes.forEach(rootItem => {
        addItemAndDescendants(rootItem);
    });
    
    const totalItems = items.length;
    const startItem = rootStartIndex + 1;
    const endItem = rootEndIndex;
    
    // Render table
    let html = `
        <div class="table-responsive">
            <table class="table table-hover mb-0">
                <thead>
                    <tr>
                        <th width="40">
                            <input type="checkbox" class="form-check-input select-all-checkbox" id="selectAll${tabType.charAt(0).toUpperCase() + tabType.slice(1)}">
                        </th>
                        <th class="sortable-header" data-sort="name" data-tab="${tabType}">Name</th>
                        <th>Description</th>
                        <th class="sortable-header text-center" data-sort="owners" data-tab="${tabType}">Owners</th>
                        <th class="sortable-header text-center" data-sort="structured_properties" data-tab="${tabType}">Structured<br/>Properties</th>
                        <th class="sortable-header text-center" data-sort="entities" data-tab="${tabType}">Entities</th>
                        <th>URN</th>
                        ${tabType === 'synced' ? '<th class="sortable-header text-center" data-sort="sync_status">Sync Status</th>' : ''}
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
    `;
    
    if (pageItems.length === 0) {
        // Determine the correct colspan based on the number of columns shown
        // checkbox + name + description + owners + structured props + entities + urn + (sync status for synced) + actions
        const colspan = tabType === 'synced' ? '9' : '8'; 
        html += `
                    <tr>
                        <td colspan="${colspan}" class="text-center py-4 text-muted">
                            <i class="fas fa-inbox fa-2x mb-2"></i><br>
                            No domains found
                        </td>
                    </tr>
        `;
    } else {
        pageItems.forEach(item => {
            html += renderDomainRow(item, tabType);
        });
    }
    
    html += `
                </tbody>
            </table>
        </div>
    `;
    
    // Add pagination - using tags-style pagination
    if (totalPages > 1) {
        html += generatePaginationHTML(totalRootNodes, tabType);
    }
    
    contentElement.innerHTML = html;
    attachActionButtonHandlers();
    
    // Attach checkbox handlers for parent-child selection (includes select-all)
    attachCheckboxHandlers(contentElement, tabType);
    
    // Attach sorting handlers
    attachSortingHandlers(contentElement, tabType);
    
    // Restore sort state visual indicators
    restoreSortState(contentElement, tabType);
    
    // Attach pagination handlers
    attachPaginationHandlers(contentElement, tabType);
    
    // Attach expand/collapse handlers
    attachExpandCollapseHandlers(contentElement);
    
    // Apply current expansion state to the rendered rows
    applyCurrentExpansionState();
}

function renderDomainRow(domain, tabType) {
    const domainData = domain.combined || domain;
    const urn = domainData.urn || '';
    const name = escapeHtml(domainData.name || 'Unnamed Domain');
    const description = escapeHtml(domainData.description || '');
    const status = domainData.sync_status || 'UNKNOWN';
    const statusDisplay = domainData.sync_status_display || status;
    const ownersCount = domainData.owners_count || 0;
    const relationshipsCount = domainData.relationships_count || 0;
    const entitiesCount = domainData.entities_count || 0;
    
    // Domain icon and color
    const iconName = domainData.icon_name || 'folder';
    const iconStyle = domainData.icon_style || 'solid';
    const iconLibrary = domainData.icon_library || 'MATERIAL';
    const colorHex = domainData.color_hex || '#6c757d';
    
    // Hierarchy support - use calculated values from hierarchy processing
    const level = domainData.hierarchy_level || 0;
    const hasChildren = domainData.has_children || false;
    const isExpanded = domainData.is_expanded || false;
    const parentUrn = domainData.parent_urn;
    
    // Debug logging for synced tab
    if (tabType === 'synced' && (level > 0 || hasChildren)) {
        console.log(`[DEBUG] Rendering ${tabType} domain: ${name}, level=${level}, hasChildren=${hasChildren}, parentUrn=${parentUrn}`);
    }
    
    const statusBadgeClass = getStatusBadgeClass(status);
    const actionButtons = getActionButtons(domainData, tabType);
    
    // Build hierarchy display
    let hierarchyDisplay = '';
    if (level > 0) {
        hierarchyDisplay = `<div class="tree-connector${domainData.is_last_child ? ' last-child' : ''}"></div>`;
    }
    
    // Build expand/collapse button - always show container for consistent alignment
    let expandButton = '';
    if (hasChildren) {
        expandButton = `
            <div class="expand-button-container">
                <button class="expand-button" type="button">
                    <i class="expand-icon fas fa-${isExpanded ? 'chevron-down' : 'chevron-right'}"></i>
                </button>
            </div>
        `;
    } else {
        // Always show empty container for consistent alignment
        expandButton = '<div class="expand-button-container"></div>';
    }
    
    // Build domain icon
    const iconClass = iconLibrary === 'MATERIAL' ? `fas fa-${iconName}` : `fas fa-${iconName}`;
    const domainIcon = `
        <div class="domain-icon" style="background-color: ${colorHex}; color: white;">
            <i class="${iconClass}"></i>
        </div>
    `;
    
    return `
        <tr data-item='${JSON.stringify(domainData)}' data-urn="${urn}" data-domain-urn="${urn}" data-level="${level}" ${parentUrn ? `data-parent="${parentUrn}"` : ''}>
            <td>
                <input type="checkbox" class="form-check-input item-checkbox" value="${domainData.id || urn}">
            </td>
            <td class="name-cell">
                <div class="hierarchy-container">
                    ${hierarchyDisplay}
                    ${expandButton}
                    ${domainIcon}
                    <div class="item-name">${name}</div>
                </div>
            </td>
            <td>
                <div class="description-preview">${truncateText(description, 100)}</div>
            </td>
            <td class="text-center">
                ${ownersCount > 0 ? `<i class="fas fa-users text-info me-1"></i><span class="badge bg-info">${ownersCount}</span>` : '<span class="text-muted">None</span>'}
            </td>
            <td class="text-center">
                <span class="badge bg-success">${domainData.structured_properties_count || 0}</span>
            </td>
            <td class="text-center">
                <span class="badge bg-primary">${entitiesCount}</span>
            </td>
            <td title="${escapeHtml(urn)}">
                <code class="small">${escapeHtml(truncateUrn(urn, 40))}</code>
            </td>
            ${tabType === 'synced' ? `
                <td class="text-center">
                    <span class="badge ${getStatusBadgeClass(status)}">${statusDisplay.replace('_', ' ')}</span>
                </td>
            ` : ''}
            <td>
                <div class="btn-group" role="group">
                    ${actionButtons}
                </div>
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
                Showing ${startItem}-${endItem} of ${totalItems} domains
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
                renderTab(`${tab}-items`);
            }
        });
    });
    
    // Attach items per page change handler
    const itemsPerPageSelect = content.querySelector(`#itemsPerPage-${tabType}`);
    if (itemsPerPageSelect) {
        itemsPerPageSelect.addEventListener('change', function() {
            currentPagination[tabType].itemsPerPage = parseInt(this.value);
            currentPagination[tabType].page = 1; // Reset to first page
            renderTab(`${tabType}-items`);
        });
    }
}

function updateBulkActionsVisibility(tabType = null) {
    const tabTypes = tabType ? [tabType] : ['synced', 'local', 'remote'];
    
    tabTypes.forEach(currentTabType => {
        const tabContent = document.getElementById(`${currentTabType}-content`);
        const bulkActions = document.getElementById(`${currentTabType}-bulk-actions`);
        const selectedCount = document.getElementById(`${currentTabType}-selected-count`);
        
        if (tabContent && bulkActions && selectedCount) {
            const checkedBoxes = tabContent.querySelectorAll('.item-checkbox:checked');
            const count = checkedBoxes.length;
            
            selectedCount.textContent = count;
            
            if (count > 0) {
                bulkActions.classList.add('show');
            } else {
                bulkActions.classList.remove('show');
            }
        }
    });
}

function getActionButtons(domain, tabType) {
    const buttons = [];
    const domainId = domain.id;
    const domainUrn = domain.urn;
    const status = domain.sync_status;
    
    console.log(`Getting action buttons for domain: ${domain.name}, id: ${domainId}, status: ${status}`);
    
    // View button - always available
    buttons.push(`<button type="button" class="btn btn-sm btn-outline-primary view-domain" data-domain='${JSON.stringify(domain)}' title="View Details">
        <i class="fas fa-eye"></i>
    </button>`);
    
    // View in DataHub button - only for non-local domains (not LOCAL_ONLY status)
    if (domainUrn && !domainUrn.includes('local:') && status !== 'LOCAL_ONLY') {
        buttons.push(`<a href="${getDataHubUrl(domainUrn, 'domain')}" class="btn btn-sm btn-outline-info" target="_blank" title="View in DataHub">
            <i class="fas fa-external-link-alt"></i>
        </a>`);
    }
    
    if (domainId) {
        // Local domain actions
        switch (status) {
            case 'SYNCED':
                buttons.push(`<button type="button" class="btn btn-sm btn-outline-secondary edit-domain" onclick="editDomain('${domainId}')" title="Edit">
                    <i class="fas fa-edit"></i>
                </button>`);
                buttons.push(`<button type="button" class="btn btn-sm btn-outline-warning add-to-staged-changes" onclick="addDomainToStagedChanges(${JSON.stringify(domain).replace(/"/g, '&quot;')})" title="Add to Staged Changes">
                    <i class="fab fa-github"></i>
                </button>`);
                buttons.push(`<button type="button" class="btn btn-sm btn-outline-info resync-domain" onclick="resyncDomain('${domainId}')" title="Resync">
                    <i class="fas fa-sync-alt"></i>
                </button>`);
                buttons.push(`<button type="button" class="btn btn-sm btn-outline-success push-domain" onclick="pushDomainToDataHub('${domainId}')" title="Push to DataHub">
                    <i class="fas fa-upload"></i>
                </button>`);
                buttons.push(`<button type="button" class="btn btn-sm btn-outline-danger delete-local-domain" onclick="deleteLocalDomain('${domainId}')" title="Delete Local">
                    <i class="fas fa-trash"></i>
                </button>`);
                break;
                
            case 'LOCAL_ONLY':
                buttons.push(`<button type="button" class="btn btn-sm btn-outline-secondary edit-domain" onclick="editDomain('${domainId}')" title="Edit">
                    <i class="fas fa-edit"></i>
                </button>`);
                buttons.push(`<button type="button" class="btn btn-sm btn-outline-warning add-to-staged-changes" onclick="addDomainToStagedChanges(${JSON.stringify(domain).replace(/"/g, '&quot;')})" title="Add to Staged Changes">
                    <i class="fab fa-github"></i>
                </button>`);
                buttons.push(`<button type="button" class="btn btn-sm btn-outline-success push-domain" onclick="pushDomainToDataHub('${domainId}')" title="Push to DataHub">
                    <i class="fas fa-upload"></i>
                </button>`);
                buttons.push(`<button type="button" class="btn btn-sm btn-outline-danger delete-local-domain" onclick="deleteLocalDomain('${domainId}')" title="Delete">
                    <i class="fas fa-trash"></i>
                </button>`);
                break;
                
            case 'MODIFIED':
                buttons.push(`<button type="button" class="btn btn-sm btn-outline-secondary edit-domain" onclick="editDomain('${domainId}')" title="Edit">
                    <i class="fas fa-edit"></i>
                </button>`);
                buttons.push(`<button type="button" class="btn btn-sm btn-outline-warning add-to-staged-changes" onclick="addDomainToStagedChanges(${JSON.stringify(domain).replace(/"/g, '&quot;')})" title="Add to Staged Changes">
                    <i class="fab fa-github"></i>
                </button>`);
                buttons.push(`<button type="button" class="btn btn-sm btn-outline-info resync-domain" onclick="resyncDomain('${domainId}')" title="Resync">
                    <i class="fas fa-sync-alt"></i>
                </button>`);
                buttons.push(`<button type="button" class="btn btn-sm btn-outline-success push-domain" onclick="pushDomainToDataHub('${domainId}')" title="Push to DataHub">
                    <i class="fas fa-upload"></i>
                </button>`);
                buttons.push(`<button type="button" class="btn btn-sm btn-outline-danger delete-local-domain" onclick="deleteLocalDomain('${domainId}')" title="Delete Local">
                    <i class="fas fa-trash"></i>
                </button>`);
                break;
        }
    } else {
        // Remote-only domain actions (no edit button for remote-only domains)
        buttons.push(`<button type="button" class="btn btn-sm btn-outline-warning add-to-staged-changes" onclick="addDomainToStagedChanges(${JSON.stringify(domain).replace(/"/g, '&quot;')})" title="Add to Staged Changes">
            <i class="fab fa-github"></i>
        </button>`);
        buttons.push(`<button type="button" class="btn btn-sm btn-outline-primary sync-domain-to-local" onclick="syncDomainToLocal('${domainUrn}')" title="Sync to Local">
            <i class="fas fa-download"></i>
        </button>`);
        buttons.push(`<button type="button" class="btn btn-sm btn-outline-danger delete-remote-domain" onclick="deleteRemoteDomain('${domainUrn}')" title="Delete Remote">
            <i class="fas fa-trash"></i>
        </button>`);
    }
    
    console.log(`Generated ${buttons.length} action buttons for domain ${domain.name}`);
    return buttons.join(' ');
}

function attachActionButtonHandlers() {
    console.log('Attaching action button handlers');
    
    // View domain buttons
    const viewButtons = document.querySelectorAll('.view-domain');
    console.log(`Found ${viewButtons.length} view domain buttons`);
    
    viewButtons.forEach(button => {
        button.addEventListener('click', function() {
            console.log('View domain button clicked');
            const domainData = JSON.parse(this.getAttribute('data-domain'));
            showDomainDetails(domainData);
        });
    });
}

function reattachSelectAllHandlers() {
    // Re-attach select-all checkbox handlers for the current tab content
    document.querySelectorAll('.select-all-checkbox').forEach(checkbox => {
        // Remove existing handlers to avoid duplicates
        checkbox.removeEventListener('change', selectAllHandler);
        checkbox.addEventListener('change', selectAllHandler);
    });
}

function selectAllHandler(e) {
    const tabType = e.target.id.replace('selectAll', '').toLowerCase();
    const tabContent = document.getElementById(`${tabType}-content`);
    const itemCheckboxes = tabContent.querySelectorAll('.item-checkbox');
    
    itemCheckboxes.forEach(cb => {
        cb.checked = e.target.checked;
    });
    
    updateBulkActionsVisibility();
}

// Domain action functions

function attachSortingHandlers(contentDiv, tabType) {
    const sortableHeaders = contentDiv.querySelectorAll('.sortable-header');
    
    sortableHeaders.forEach(header => {
        header.addEventListener('click', function() {
            const column = this.getAttribute('data-sort');
            
            // Remove existing sort classes from all headers
            contentDiv.querySelectorAll('.sortable-header').forEach(h => {
                h.classList.remove('sort-asc', 'sort-desc');
            });
            
            // Toggle direction if same column, otherwise default to asc
            if (currentSort.column === column && currentSort.tabType === tabType) {
                currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
            } else {
                currentSort.column = column;
                currentSort.direction = 'asc';
                currentSort.tabType = tabType;
            }
            
            // Add appropriate sort class to current header
            this.classList.add(currentSort.direction === 'asc' ? 'sort-asc' : 'sort-desc');
            
            // Re-render the tab with new sorting
            renderTab(`${tabType}-items`);
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

function sortDomains(domains, column, direction) {
    return domains.sort((a, b) => {
        let aValue = getDomainSortValue(a, column);
        let bValue = getDomainSortValue(b, column);
        
        // Handle null/undefined values
        if (aValue === null || aValue === undefined) aValue = '';
        if (bValue === null || bValue === undefined) bValue = '';
        
        // Convert to strings for comparison if needed
        if (typeof aValue === 'string') aValue = aValue.toLowerCase();
        if (typeof bValue === 'string') bValue = bValue.toLowerCase();
        
        let comparison = 0;
        if (aValue < bValue) comparison = -1;
        if (aValue > bValue) comparison = 1;
        
        return direction === 'desc' ? -comparison : comparison;
    });
}

function getDomainSortValue(domain, column) {
    const domainData = domain.combined || domain;
    
    switch (column) {
        case 'name':
            return (domainData.name || '').toLowerCase();
        case 'owners':
            return domainData.owners_count || 0;
        case 'structured_properties':
            return domainData.structured_properties_count || 0;
        case 'entities':
            return domainData.entities_count || 0;
        case 'sync_status':
            const syncStatus = domainData.sync_status || 'UNKNOWN';
            // Define sort order: SYNCED -> MODIFIED -> LOCAL_ONLY -> REMOTE_ONLY -> UNKNOWN
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

function showDomainDetails(domain) {
    console.log('Showing domain details for:', domain.name);
    
    // Check if the modal element exists
    const modalElement = document.getElementById('domainViewModal');
    if (!modalElement) {
        console.error('domainViewModal element not found in DOM');
        showError('Modal not found. Please refresh the page.');
        return;
    }
    
    // Basic information - enhanced like data products and assertions
    const name = domain.properties?.name || domain.name || 'Unnamed Domain';
    const description = domain.properties?.description || domain.description || 'No description available';
    
    // Check multiple possible parent URN fields
    let parentDomain = 'No parent domain';
    let parentUrn = null;
    
    if (domain.parentDomains?.domains?.[0]?.properties?.name) {
        parentDomain = domain.parentDomains.domains[0].properties.name;
        parentUrn = domain.parentDomains.domains[0].urn;
    } else if (domain.parent_urn) {
        parentUrn = domain.parent_urn;
        // Try to find the parent domain name from the data
        const allDomains = [...(domainsData.synced_items || []), ...(domainsData.local_only_items || []), ...(domainsData.remote_only_items || [])];
        const parentDomainData = allDomains.find(item => {
            const d = item.combined || item;
            return d.urn === parentUrn;
        });
        if (parentDomainData) {
            const pd = parentDomainData.combined || parentDomainData;
            parentDomain = pd.properties?.name || pd.name || parentUrn;
        } else {
            parentDomain = parentUrn;
        }
    } else if (domain.parentDomain) {
        parentDomain = domain.parentDomain.properties?.name || domain.parentDomain.name || domain.parentDomain;
        parentUrn = domain.parentDomain.urn || domain.parentDomain;
    }
    
    // Populate basic information
    const nameElement = document.getElementById('modal-domain-name');
    const parentElement = document.getElementById('modal-domain-parent');
    const descriptionElement = document.getElementById('modal-domain-description');
    const urnElement = document.getElementById('modal-domain-urn');
    const statusElement = document.getElementById('modal-domain-status');
    
    if (nameElement) nameElement.textContent = name;
    if (parentElement) parentElement.innerHTML = parentDomain !== 'No parent domain' ? 
        `<span class="badge bg-info">${escapeHtml(parentDomain)}</span>${parentUrn ? `<br><small class="text-muted">${escapeHtml(parentUrn)}</small>` : ''}` : parentDomain;
    if (descriptionElement) descriptionElement.textContent = description;
    if (urnElement) urnElement.textContent = domain.urn || 'No URN available';
    
    // Status
    if (statusElement) {
        statusElement.textContent = domain.sync_status_display || domain.sync_status;
        statusElement.className = `badge ${getStatusBadgeClass(domain.sync_status)}`;
    }
    
    // Metrics & Ownership
    const ownersElement = document.getElementById('modal-domain-owners');
    const entitiesElement = document.getElementById('modal-domain-entities');
    const childrenElement = document.getElementById('modal-domain-children');
    
    if (ownersElement) ownersElement.textContent = domain.owners_count || 0;
    if (entitiesElement) entitiesElement.textContent = domain.entities_count || 0;
    
    // Calculate sub-domains count
    const subDomainsCount = calculateSubDomainsCount(domain.urn);
    if (childrenElement) childrenElement.textContent = subDomainsCount;
    
    // Owner details - format like tags and glossary
    const ownersListElement = document.getElementById('modal-domain-owners-list');
    
    if (ownersListElement) {
        // Check for ownership data from GraphQL (remote domains) or local storage
        const ownershipData = domain.ownership || domain.ownership_data;
        
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
                    
                    // Find the ownership type name from cache (if available)
                    ownershipTypeName = 'Unknown Type';
                    // Note: We'd need to load ownership types cache for domains too
                } else if (ownerInfo.owner && ownerInfo.ownershipType) {
                    // GraphQL format
                    ownerUrn = ownerInfo.owner.urn;
                    ownershipTypeUrn = ownerInfo.ownershipType.urn;
                    ownershipTypeName = ownerInfo.ownershipType.info?.name || 'Unknown Type';
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
                    // Local storage format or simple name format
                    if (ownerUrn.includes('corpuser:')) {
                        isUser = true;
                        ownerName = ownerUrn.replace('urn:li:corpuser:', '');
                    } else if (ownerUrn.includes('corpGroup:')) {
                        isUser = false;
                        ownerName = ownerUrn.replace('urn:li:corpGroup:', '');
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
        } else if (domain.owner_names && domain.owner_names.length > 0) {
            // Fallback to simple owner names format
            let ownersHTML = `
                <div class="mb-3">
                    <h6 class="text-primary mb-2">
                        <i class="material-icons me-1">account_tree</i>Domain Owners
                    </h6>
                    <div class="ms-3">
                        ${domain.owner_names.map(owner => `
                            <div class="d-flex align-items-center mb-1">
                                <i class="material-icons text-muted me-2">person</i>
                                <span>${escapeHtml(owner)}</span>
                            </div>
                        `).join('')}
                    </div>
                </div>
            `;
            ownersListElement.innerHTML = ownersHTML;
        } else {
            ownersListElement.innerHTML = '<p class="text-muted">No ownership information available</p>';
        }
    }
    
    // Domain Properties
    const iconElement = document.getElementById('modal-domain-icon');
    const colorElement = document.getElementById('modal-domain-color');
    const createdElement = document.getElementById('modal-domain-created');
    
    if (iconElement || colorElement || createdElement) {
        const iconName = domain.icon_name || 'folder';
        const iconClass = domain.icon_library === 'MATERIAL' ? `${iconName}` : iconName;
        const colorHex = domain.color_hex || '#6c757d';
        
        if (iconElement) iconElement.innerHTML = `<i class="${iconClass}"></i> ${iconName}`;
        if (colorElement) colorElement.innerHTML = `<span class="badge" style="background-color: ${colorHex};">${colorHex}</span>`;
        
        // Created date
        const createdDate = domain.created_at ? formatDate(domain.created_at) : 'Unknown';
        if (createdElement) createdElement.textContent = createdDate;
    }
    
    // Raw JSON Data
    const rawJsonElement = document.getElementById('modal-raw-json');
    if (rawJsonElement) {
        rawJsonElement.innerHTML = `<code>${escapeHtml(JSON.stringify(domain, null, 2))}</code>`;
    }
    
    // DataHub link
    const datahubLink = document.getElementById('modal-datahub-link');
    if (datahubLink) {
        if (domain.urn && !domain.urn.includes('local:') && domainsData.datahub_url) {
            datahubLink.href = getDataHubUrl(domain.urn, 'domain');
            datahubLink.style.display = 'inline-block';
        } else {
            datahubLink.style.display = 'none';
        }
    }
    
    // Show modal
    new bootstrap.Modal(modalElement).show();
}

function calculateSubDomainsCount(parentUrn) {
    let count = 0;
    
    // Check all tabs for child domains
    [domainsData.synced_items, domainsData.local_only_items, domainsData.remote_only_items].forEach(items => {
        if (items) {
            items.forEach(item => {
                const domain = item.combined || item;
                if (domain.parent_urn === parentUrn) {
                    count++;
                }
            });
        }
    });
    
    return count;
}

function editDomain(domainId) {
    console.log('Editing domain:', domainId);
    
    // Find the domain data from the current loaded data
    let domainData = null;
    
    // Search in all tabs for the domain
    if (domainsData.synced_items) {
        for (let item of domainsData.synced_items) {
            const data = item.combined || item.local || item;
            if (data.id == domainId) {
                domainData = data;
                break;
            }
        }
    }
    
    if (!domainData && domainsData.local_only_items) {
        for (let item of domainsData.local_only_items) {
            if (item.id == domainId) {
                domainData = item;
                break;
            }
        }
    }
    
    if (domainData) {
        // Populate the enhanced edit modal with null checks
        const nameField = document.getElementById('editDomainName');
        if (nameField) nameField.value = domainData.name || '';
        
        const descField = document.getElementById('editDomainDescription');
        if (descField) descField.value = domainData.description || '';
        
        // Populate appearance fields
        const iconField = document.getElementById('editDomainIcon');
        if (iconField) iconField.value = domainData.icon_name || 'folder';
        
        const colorField = document.getElementById('editDomainColor');
        if (colorField) colorField.value = domainData.color_hex || '#6c757d';
        
        const colorTextField = document.getElementById('editDomainColorText');
        if (colorTextField) colorTextField.value = domainData.color_hex || '#6c757d';
        
        // Update icon preview
        const iconPreview = document.getElementById('editDomainIconPreview');
        if (iconPreview) {
            iconPreview.className = `fas fa-${domainData.icon_name || 'folder'}`;
        }
        
        // Clear and populate ownership sections
        const ownershipContainer = document.getElementById('edit-ownership-sections-container');
        if (ownershipContainer) {
            ownershipContainer.innerHTML = '';
            
            // Load users and groups first, then populate ownership sections
            loadUsersAndGroups().then(() => {
                // Add ownership sections if there are owners
                if (domainData.ownership_data && domainData.ownership_data.owners && domainData.ownership_data.owners.length > 0) {
                    // Populate form with existing ownership data
                    domainData.ownership_data.owners.forEach((owner, index) => {
                        addOwnershipSection('edit-ownership-sections-container');
                        
                        // Wait for the section to be fully created and initialized
                        setTimeout(() => {
                            const sections = ownershipContainer.querySelectorAll('.card');
                            const currentSection = sections[sections.length - 1];
                            
                            if (currentSection) {
                                const ownerSelect = currentSection.querySelector('select[name="owners"]');
                                const typeSelect = currentSection.querySelector('select[name="ownership_types"]');
                                
                                if (ownerSelect && owner.owner_urn) {
                                    ownerSelect.value = owner.owner_urn;
                                    $(ownerSelect).trigger('change'); // Trigger Select2 update
                                }
                                
                                if (typeSelect && owner.ownership_type_urn) {
                                    typeSelect.value = owner.ownership_type_urn;
                                }
                            }
                        }, 200 + (index * 50)); // Stagger the updates
                    });
                    showOwnershipSection('edit-ownership-sections-container', 'edit-ownership-sections-label', 'edit-ownership-sections-help');
                } else {
                    // Add one empty ownership section
                    addOwnershipSection('edit-ownership-sections-container');
                    hideOwnershipSectionIfEmpty('edit-ownership-sections-container', 'edit-ownership-sections-label', 'edit-ownership-sections-help');
                }
            }).catch(error => {
                console.error('Error loading users and groups:', error);
                // Still add an empty section even if loading fails
                addOwnershipSection('edit-ownership-sections-container');
                hideOwnershipSectionIfEmpty('edit-ownership-sections-container', 'edit-ownership-sections-label', 'edit-ownership-sections-help');
            });
        }
        
        // Populate parent domain dropdown for edit modal
        populateEditParentDomainDropdown(domainData.parent_urn);
        
        // Setup form handlers
        setupEditFormHandlers();
        
        // Add form submission handler
        const form = document.getElementById('editDomainForm');
        form.onsubmit = function(e) {
            e.preventDefault();
            
            const formData = new FormData(form);
            
            fetch(`/metadata/domains/${domainId}/`, {
                method: 'POST',
                body: formData,
                headers: {
                    'X-CSRFToken': getCsrfToken(),
                }
            })
            .then(response => {
                if (response.redirected) {
                    // Success - reload the domains data
                    loadDomainsData();
                    showSuccess(`Domain '${formData.get('name')}' updated successfully`);
                    // Close the modal
                    bootstrap.Modal.getInstance(document.getElementById('editDomainModal')).hide();
                } else {
                    throw new Error('Failed to update domain');
                }
            })
            .catch(error => {
                console.error('Error updating domain:', error);
                showError('Failed to update domain: ' + error.message);
            });
        };
        
        // Show the modal
        const modal = new bootstrap.Modal(document.getElementById('editDomainModal'));
        modal.show();
        
        // Wait for modal to be fully shown before loading ownership
        document.getElementById('editDomainModal').addEventListener('shown.bs.modal', function() {
            // Ensure ownership interface is set up after modal is fully displayed  
            setTimeout(() => {
                setupOwnershipInterface('edit-ownership-sections-container', 'add-edit-ownership-section', 'edit-ownership-label', 'edit-ownership-help-text');
            }, 100);
        }, { once: true });
    } else {
        showError('Domain data not found');
    }
}

function populateEditParentDomainDropdown(currentParentUrn) {
    const dropdown = document.getElementById('editDomainParent');
    if (!dropdown) {
        console.warn('Edit parent dropdown not found');
        return;
    }
    
    dropdown.innerHTML = '<option value="">No parent (root domain)</option>';
    
    // Get all domains to populate parent options
    [domainsData.synced_items, domainsData.local_only_items, domainsData.remote_only_items].forEach(items => {
        if (items) {
            items.forEach(item => {
                const domain = item.combined || item;
                if (domain.urn && domain.name) {
                    const option = document.createElement('option');
                    option.value = domain.urn;
                    option.textContent = domain.name;
                    if (domain.urn === currentParentUrn) {
                        option.selected = true;
                    }
                    dropdown.appendChild(option);
                }
            });
        }
    });
    
    console.log(`Populated edit parent dropdown with ${dropdown.options.length} options, current parent: ${currentParentUrn}`);
}

function setupEditFormHandlers() {
    // Icon preview update
    const iconInput = document.getElementById('editDomainIcon');
    const iconPreview = document.getElementById('editDomainIconPreview');
    
    iconInput.addEventListener('input', function() {
        iconPreview.className = `fas fa-${this.value || 'folder'}`;
    });
    
    // Color synchronization
    const colorPicker = document.getElementById('editDomainColor');
    const colorText = document.getElementById('editDomainColorText');
    
    colorPicker.addEventListener('change', function() {
        colorText.value = this.value;
    });
    
    colorText.addEventListener('input', function() {
        if (/^#[0-9A-Fa-f]{6}$/.test(this.value)) {
            colorPicker.value = this.value;
        }
    });
}

function editRemoteDomain(domainUrn) {
    console.log('Editing remote domain:', domainUrn);
    // Show modal with remote domain info and option to sync locally first
    const modal = new bootstrap.Modal(document.getElementById('editRemoteDomainModal'));
    modal.show();
}

function addDomainToPR(domainId) {
    console.log(`Adding domain ${domainId} to PR`);
    
    const button = document.querySelector(`button[onclick="addDomainToPR('${domainId}')"]`);
    if (button) {
        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
    }
    
    fetch(`/metadata/domains/${domainId}/push-github/`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCsrfToken(),
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showSuccess(data.message);
            // Optionally reload the data to reflect any status changes
            loadDomainsData();
        } else {
            showError(data.error || 'Failed to add domain to PR');
        }
    })
    .catch(error => {
        console.error('Error adding domain to PR:', error);
        showError('Failed to add domain to PR');
    })
    .finally(() => {
        if (button) {
            button.disabled = false;
            button.innerHTML = '<i class="fab fa-github"></i>';
        }
    });
}

function addRemoteDomainToPR(domainUrn) {
    console.log('Adding remote domain to PR:', domainUrn);
    
    fetch('/metadata/domains/add-to-pr/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-CSRFToken': getCsrfToken(),
        },
        body: `domain_urn=${encodeURIComponent(domainUrn)}`
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showSuccess(data.message);
        } else {
            showError(data.error);
        }
    })
    .catch(error => {
        console.error('Error adding domain to PR:', error);
        showError('Failed to add domain to PR');
    });
}

function syncDomainToLocal(domainUrn) {
    console.log(`Syncing domain to local: ${domainUrn}`);
    
    const button = document.querySelector(`button[onclick="syncDomainToLocal('${domainUrn}')"]`);
    if (button) {
        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
    }
    
    fetch('/metadata/domains/sync/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-CSRFToken': getCsrfToken(),
        },
        body: `domain_urn=${encodeURIComponent(domainUrn)}`
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showSuccess(data.message);
            
            // Move domain to synced tab immediately
            moveDomainToSynced(domainUrn, data.domain_id, data.domain_name);
        } else {
            showError(data.error);
        }
    })
    .catch(error => {
        console.error('Error syncing domain:', error);
        showError('Failed to sync domain to local');
    })
    .finally(() => {
        if (button) {
            button.disabled = false;
            button.innerHTML = '<i class="fas fa-download"></i>';
        }
    });
}

function moveDomainToSynced(domainUrn, domainId, domainName) {
    console.log(`Moving domain to synced tab: ${domainUrn} -> ${domainId}`);
    
    // Find and remove from remote-only
    const remoteIndex = domainsData.remote_only_items.findIndex(item => item.urn === domainUrn);
    if (remoteIndex > -1) {
        const remoteDomain = domainsData.remote_only_items.splice(remoteIndex, 1)[0];
        
        // Create synced domain entry
        const syncedDomain = {
            local: {
                id: domainId,
                name: domainName,
                description: remoteDomain.description || '',
                urn: remoteDomain.urn,
                sync_status: 'SYNCED',
                sync_status_display: 'Synced',
                owners_count: 0,
                relationships_count: 0
            },
            remote: remoteDomain,
            combined: {
                id: domainId,
                name: domainName,
                description: remoteDomain.description || '',
                urn: remoteDomain.urn,
                sync_status: 'SYNCED',
                sync_status_display: 'Synced',
                owners_count: remoteDomain.owners_count || 0,
                relationships_count: remoteDomain.relationships_count || 0,
                owner_names: remoteDomain.owner_names || []
            }
        };
        
        // Add to synced items
        domainsData.synced_items.push(syncedDomain);
        
        // Update statistics
        domainsData.statistics.synced_count = (domainsData.statistics.synced_count || 0) + 1;
        
        // Re-render tabs and update stats
        updateStatistics();
        renderAllTabs();
        
        console.log('Domain successfully moved to synced tab');
    }
}

function resyncDomain(domainId) {
    console.log(`Resyncing domain: ${domainId}`);
    
    const button = document.querySelector(`button[onclick="resyncDomain('${domainId}')"]`);
    if (button) {
        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
    }
    
    fetch(`/metadata/domains/${domainId}/resync/`, {
        method: 'POST',
        headers: {
            'X-CSRFToken': getCsrfToken(),
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showSuccess(data.message);
            loadDomainsData(); // Reload to get updated data
        } else {
            showError(data.error);
        }
    })
    .catch(error => {
        console.error('Error resyncing domain:', error);
        showError('Failed to resync domain');
    })
    .finally(() => {
        if (button) {
            button.disabled = false;
            button.innerHTML = '<i class="fas fa-sync-alt"></i>';
        }
    });
}

function pushDomainToDataHub(domainId) {
    console.log(`Pushing domain to DataHub: ${domainId}`);
    
    const button = document.querySelector(`button[onclick="pushDomainToDataHub('${domainId}')"]`);
    if (button) {
        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
    }
    
    fetch(`/metadata/domains/${domainId}/push/`, {
        method: 'POST',
        headers: {
            'X-CSRFToken': getCsrfToken(),
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showSuccess(data.message);
            loadDomainsData(); // Reload to get updated sync status
        } else {
            showError(data.error);
        }
    })
    .catch(error => {
        console.error('Error pushing domain to DataHub:', error);
        showError('Failed to push domain to DataHub');
    })
    .finally(() => {
        if (button) {
            button.disabled = false;
            button.innerHTML = '<i class="fas fa-upload"></i>';
        }
    });
}

function deleteLocalDomain(domainId) {
    if (!confirm('Are you sure you want to delete this local domain? This action cannot be undone.')) {
        return;
    }
    
    console.log(`Deleting local domain: ${domainId}`);
    
    fetch(`/metadata/domains/${domainId}/delete/`, {
        method: 'DELETE',
        headers: {
            'X-CSRFToken': getCsrfToken(),
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showSuccess('Domain deleted successfully');
            loadDomainsData(); // Reload to refresh the list
        } else {
            showError(data.error || 'Failed to delete domain');
        }
    })
    .catch(error => {
        console.error('Error deleting domain:', error);
        showError('Failed to delete domain');
    });
}

function deleteRemoteDomain(domainUrn) {
    if (!confirm('Are you sure you want to delete this remote domain? This action cannot be undone.')) {
        return;
    }
    
    console.log(`Deleting remote domain: ${domainUrn}`);
    
    fetch(`/metadata/domains/remote/delete-remote/`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-CSRFToken': getCsrfToken(),
        },
        body: `domain_urn=${encodeURIComponent(domainUrn)}`
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showSuccess(data.message);
            loadDomainsData(); // Reload to refresh the list
        } else {
            showError(data.error);
        }
    })
    .catch(error => {
        console.error('Error deleting remote domain:', error);
        showError('Failed to delete remote domain');
    });
}

// Bulk action functions
function bulkResyncDomains(tabType) {
    const checkedBoxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    const domainIds = Array.from(checkedBoxes).map(cb => cb.value);
    
    if (domainIds.length === 0) {
        showError('No domains selected');
        return;
    }
    
    console.log(`Bulk resyncing ${domainIds.length} domains`);
    showSuccess(`Bulk resync started for ${domainIds.length} domain(s)`);
    
    // Process each domain individually
    domainIds.forEach(id => resyncDomain(id));
}

function bulkPushDomains(tabType) {
    const checkedBoxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    const domainIds = Array.from(checkedBoxes).map(cb => cb.value);
    
    if (domainIds.length === 0) {
        showError('No domains selected');
        return;
    }
    
    console.log(`Bulk pushing ${domainIds.length} domains`);
    showSuccess(`Bulk push started for ${domainIds.length} domain(s)`);
    
    // Process each domain individually
    domainIds.forEach(id => pushDomainToDataHub(id));
}

function bulkAddToPR(tabType) {
    const selectedDomains = getSelectedDomains(tabType);
    if (selectedDomains.length === 0) {
        showNotification('warning', 'No domains selected for adding to staged changes');
        return;
    }
    
    if (!confirm(`Are you sure you want to add ${selectedDomains.length} selected domains to staged changes?`)) {
        return;
    }
    
    // Process each domain sequentially
    let successCount = 0;
    let errorCount = 0;
    
    selectedDomains.forEach(domain => {
        try {
            addDomainToStagedChanges(domain);
            successCount++;
        } catch (error) {
            console.error(`Error adding domain ${domain.name} to staged changes:`, error);
            errorCount++;
        }
    });
    
    if (errorCount > 0) {
        showNotification('warning', `Added ${successCount} domains to staged changes, ${errorCount} errors`);
    } else {
        showNotification('success', `Added ${successCount} domains to staged changes`);
    }
}

function bulkDeleteLocal(tabType) {
    const checkedBoxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    const domainIds = Array.from(checkedBoxes).map(cb => cb.value);
    
    if (domainIds.length === 0) {
        showError('No domains selected');
        return;
    }
    
    if (!confirm(`Are you sure you want to delete ${domainIds.length} local domain(s)? This action cannot be undone.`)) {
        return;
    }
    
    console.log(`Bulk deleting ${domainIds.length} local domains`);
    
    // Process each domain individually but without individual confirmations
    domainIds.forEach(id => {
        fetch(`/metadata/domains/${id}/delete/`, {
            method: 'DELETE',
            headers: {
                'X-CSRFToken': getCsrfToken(),
            }
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                console.log(`Domain ${id} deleted successfully`);
            } else {
                showError(`Failed to delete domain ${id}: ${data.error}`);
            }
        })
        .catch(error => {
            console.error(`Error deleting domain ${id}:`, error);
            showError(`Failed to delete domain ${id}`);
        });
    });
    
    showSuccess(`Bulk delete started for ${domainIds.length} domain(s)`);
    // Reload data after a short delay to allow deletions to complete
    setTimeout(() => {
        loadDomainsData();
    }, 2000);
}

function bulkSyncToLocal(tabType) {
    const checkedBoxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    const domainUrns = Array.from(checkedBoxes).map(cb => cb.value);
    
    if (domainUrns.length === 0) {
        showError('No domains selected');
        return;
    }
    
    if (confirm(`Are you sure you want to sync ${domainUrns.length} selected domain(s) to local database?`)) {
        console.log(`Bulk syncing ${domainUrns.length} domains to local`);
        
        // Show loading state
        const button = document.querySelector(`button[onclick="bulkSyncToLocal('${tabType}')"]`);
        if (button) {
            const originalText = button.textContent;
            button.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Syncing...';
            button.disabled = true;
            
            // Use the new bulk sync endpoint
            fetch('/metadata/domains/bulk/sync-to-local/', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': getCsrfToken()
                },
                body: JSON.stringify({
                    domain_urns: domainUrns
                })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    showNotification('success', data.message);
                    // Refresh the current tab data
                    loadDomainsData();
                } else {
                    showNotification('error', data.error || 'Failed to sync domains to local');
                }
            })
            .catch(error => {
                console.error('Error syncing domains to local:', error);
                showNotification('error', 'An error occurred while syncing domains to local');
            })
            .finally(() => {
                // Restore button
                button.innerHTML = originalText;
                button.disabled = false;
            });
        } else {
            // Fallback to individual processing if button not found
            showSuccess(`Bulk sync to local started for ${domainUrns.length} domain(s)`);
            domainUrns.forEach(urn => syncDomainToLocal(urn));
        }
    }
}

function bulkDeleteRemote(tabType) {
    const checkedBoxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    const domainUrns = Array.from(checkedBoxes).map(cb => cb.value);
    
    if (domainUrns.length === 0) {
        showError('No domains selected');
        return;
    }
    
    if (!confirm(`Are you sure you want to delete ${domainUrns.length} remote domain(s)? This action cannot be undone.`)) {
        return;
    }
    
    console.log(`Bulk deleting ${domainUrns.length} remote domains`);
    
    // Process each domain individually but without individual confirmations
    domainUrns.forEach(urn => {
        fetch(`/metadata/domains/remote/delete-remote/`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                'X-CSRFToken': getCsrfToken(),
            },
            body: `domain_urn=${encodeURIComponent(urn)}`
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                console.log(`Remote domain ${urn} deleted successfully`);
            } else {
                showError(`Failed to delete remote domain ${urn}: ${data.error}`);
            }
        })
        .catch(error => {
            console.error(`Error deleting remote domain ${urn}:`, error);
            showError(`Failed to delete remote domain ${urn}`);
        });
    });
    
    showSuccess(`Bulk delete started for ${domainUrns.length} remote domain(s)`);
    // Reload data after a short delay to allow deletions to complete
    setTimeout(() => {
        loadDomainsData();
    }, 2000);
}

function bulkDownloadJson(tabType) {
    const selectedDomains = getSelectedDomains(tabType);
    if (selectedDomains.length === 0) {
        showError('Please select domains to download.');
        return;
    }
    
    console.log(`Bulk download ${selectedDomains.length} domains for ${tabType}:`, selectedDomains);
    
    // Create a JSON object with the selected domains
    const domainsData = {
        domains: selectedDomains,
        metadata: {
            exported_at: new Date().toISOString(),
            count: selectedDomains.length,
            source: window.location.origin,
            tab: tabType
        }
    };
    
    // Convert to pretty JSON
    const jsonData = JSON.stringify(domainsData, null, 2);
    
    // Create a blob and initiate download
    const blob = new Blob([jsonData], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    
    // Create temporary link and trigger download
    const link = document.createElement('a');
    link.href = url;
    link.download = `domains-export-${selectedDomains.length}-${new Date().toISOString().slice(0, 10)}.json`;
    document.body.appendChild(link);
    link.click();
    
    // Clean up
    setTimeout(() => {
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }, 100);
    
    showSuccess(`${selectedDomains.length} domains exported successfully.`);
}

function getSelectedDomains(tabType) {
    const checkboxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    const selectedDomains = [];
    
    checkboxes.forEach(checkbox => {
        const row = checkbox.closest('tr');
        const itemData = row.getAttribute('data-item');
        if (itemData) {
            try {
                const domain = JSON.parse(itemData);
                selectedDomains.push(domain);
            } catch (e) {
                console.error('Error parsing domain data:', e);
            }
        }
    });
    
    return selectedDomains;
}

// Global action functions
function resyncAll() {
    if (confirm('Are you sure you want to resync all domains? This may take a while.')) {
        showSuccess('Starting resync of all domains...');
        // Implementation would go here
        console.log('Resync all domains');
    }
}

function exportAll() {
    const allDomains = [
        ...(domainsData.synced_items || []),
        ...(domainsData.local_only_items || []),
        ...(domainsData.remote_only_items || [])
    ];
    
    if (allDomains.length === 0) {
        showError('No domains to export.');
        return;
    }
    
    // Create a JSON object with all domains
    const exportData = {
        domains: allDomains,
        metadata: {
            exported_at: new Date().toISOString(),
            count: allDomains.length,
            source: window.location.origin,
            export_type: 'all'
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
    link.download = `all-domains-export-${allDomains.length}-${new Date().toISOString().slice(0, 10)}.json`;
    document.body.appendChild(link);
    link.click();
    
    // Clean up
    setTimeout(() => {
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }, 100);
    
    showSuccess(`${allDomains.length} domains exported successfully.`);
}

function addDomainToStagedChanges(domain) {
    console.log('addDomainToStagedChanges called with:', domain);
    
    const domainName = domain.name || 'Unknown';
    
    // Check if this is a remote-only domain that needs to be staged directly
    if (domain.sync_status === 'REMOTE_ONLY' || !domain.id) {
        console.log(`Domain "${domainName}" is remote-only, staging directly...`);
        
        // Show loading notification
        showNotification('info', `Adding remote domain "${domainName}" to staged changes...`);
        
        // Get current environment and mutation from global state or settings
        const currentEnvironment = window.currentEnvironment || { name: 'dev' };
        const mutationName = currentEnvironment.mutation_name || null;
        
        // Use the remote staging endpoint
        fetch('/metadata/domains/remote/stage_changes/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCsrfToken()
            },
            body: JSON.stringify({
                domain_data: domain,
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
            console.log('Add remote domain to staged changes response:', data);
            if (data.status === 'success') {
                showNotification('success', data.message || `Remote domain added to staged changes successfully`);
                if (data.files_created && data.files_created.length > 0) {
                    console.log('Created files:', data.files_created);
                }
            } else {
                throw new Error(data.error || 'Unknown error occurred');
            }
        })
        .catch(error => {
            console.error('Error adding remote domain to staged changes:', error);
            showNotification('error', `Error adding remote domain to staged changes: ${error.message}`);
        });
        return;
    }
    
    if (!domain.id) {
        console.error('Cannot add domain to staged changes without an ID:', domain);
        showNotification('error', 'Error adding to staged changes: Missing domain ID.');
        return;
    }
    
    // Show loading notification
    showNotification('info', `Adding domain "${domainName}" to staged changes...`);
    
    // Get current environment and mutation from global state or settings
    const currentEnvironment = window.currentEnvironment || { name: 'dev' };
    const mutationName = currentEnvironment.mutation_name || null;
    
    // Use the staged changes endpoint
    fetch(`/metadata/domains/${domain.id}/stage_changes/`, {
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
        console.log('Add domain to staged changes response:', data);
        if (data.status === 'success') {
            showNotification('success', data.message || `Domain added to staged changes successfully`);
            if (data.files_created && data.files_created.length > 0) {
                console.log('Created files:', data.files_created);
            }
        } else {
            throw new Error(data.error || 'Unknown error occurred');
        }
    })
    .catch(error => {
        console.error('Error adding domain to staged changes:', error);
        showNotification('error', `Error adding domain to staged changes: ${error.message}`);
    });
}

function addAllToStagedChanges() {
    if (confirm('Are you sure you want to add all domains to staged changes?')) {
        // Show loading spinner
        const button = document.querySelector('[onclick="addAllToStagedChanges()"]');
        const originalText = button.textContent;
        button.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Processing...';
        button.disabled = true;
        
        fetch('/metadata/domains/bulk/add-to-staged-changes/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCsrfToken()
            },
            body: JSON.stringify({
                environment: 'dev',
                mutation_name: null
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                showNotification('success', data.message);
            } else {
                showNotification('error', data.error || 'Failed to add domains to staged changes');
            }
        })
        .catch(error => {
            console.error('Error adding domains to staged changes:', error);
            showNotification('error', 'An error occurred while adding domains to staged changes');
        })
        .finally(() => {
            // Restore button
            button.innerHTML = originalText;
            button.disabled = false;
        });
    }
}

function showImportModal() {
    // Implementation would go here
    console.log('Show import modal');
    showError('Import functionality not yet implemented.');
}

// Utility functions
function escapeHtml(text) {
    const map = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#039;'
    };
    return text.replace(/[&<>"']/g, function(m) { return map[m]; });
}

function truncateText(text, maxLength) {
    if (text.length <= maxLength) return text;
    return text.substr(0, maxLength) + '...';
}

function truncateUrn(urn, maxLength) {
    if (urn.length <= maxLength) return urn;
    return urn.substr(0, maxLength - 3) + '...';
}

function formatDate(timestamp) {
    return new Date(timestamp).toLocaleString();
}

function getStatusBadgeClass(status) {
    switch (status) {
        case 'SYNCED': return 'bg-success';
        case 'LOCAL_ONLY': return 'bg-secondary';  
        case 'REMOTE_ONLY': return 'bg-info';
        case 'MODIFIED': return 'bg-warning';
        case 'PENDING_PUSH': return 'bg-primary';
        default: return 'bg-light text-dark';
    }
}

function getDataHubUrl(urn, type) {
    if (domainsData.datahub_url) {
        return `${domainsData.datahub_url}/domain/${encodeURIComponent(urn)}`;
    }
    return '#';
}

function getCsrfToken() {
    return document.querySelector('[name=csrfmiddlewaretoken]').value;
}

function extractNameFromUrn(urn) {
    if (!urn) return 'Unknown';
    // Extract the last part of the URN after the last colon
    const parts = urn.split(':');
    return parts[parts.length - 1] || 'Unknown';
}

function getDatabaseId(domainData) {
    // Try to get the database ID from various possible sources
    if (domainData.id) {
        return domainData.id;
    }
    
    // If we have combined data (from cache), try to get ID from there
    if (domainData.combined && domainData.combined.id) {
        return domainData.combined.id;
    }
    
    // For local domains, try to extract from the domain data
    if (domainData.database_id) {
        return domainData.database_id;
    }
    
    // If no direct ID, we can't process this domain
    console.warn('Cannot determine database ID for domain:', domainData);
    return null;
}

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

function showError(message) {
    showNotification('error', message);
}

function showSuccess(message) {
    showNotification('success', message);
}

// Hierarchy functions
function toggleDomainExpansion(domainUrn) {
    console.log('Toggling domain expansion:', domainUrn);
    
    // Find the domain row
    const row = document.querySelector(`tr[data-urn="${domainUrn}"]`);
    if (!row) return;
    
    const expandButton = row.querySelector('.expand-button');
    const expandIcon = row.querySelector('.expand-icon');
    
    if (!expandButton || !expandIcon) return;
    
    const isExpanded = expandIcon.classList.contains('fa-chevron-down');
    
    if (isExpanded) {
        // Collapse - hide children
        expandIcon.classList.remove('fa-chevron-down');
        expandIcon.classList.add('fa-chevron-right');
        hideChildrenDomains(domainUrn);
    } else {
        // Expand - show children
        expandIcon.classList.remove('fa-chevron-right');
        expandIcon.classList.add('fa-chevron-down');
        showChildrenDomains(domainUrn);
    }
}

function hideChildrenDomains(parentUrn) {
    const childRows = document.querySelectorAll(`tr[data-parent="${parentUrn}"]`);
    childRows.forEach(row => {
        row.style.display = 'none';
        // Also hide any grandchildren
        const childUrn = row.getAttribute('data-urn');
        if (childUrn) {
            hideChildrenDomains(childUrn);
        }
    });
}

function showChildrenDomains(parentUrn) {
    const childRows = document.querySelectorAll(`tr[data-parent="${parentUrn}"]`);
    childRows.forEach(row => {
        row.style.display = '';
        // Show grandchildren only if their parent is expanded
        const childUrn = row.getAttribute('data-urn');
        const expandIcon = row.querySelector('.expand-icon');
        if (childUrn && expandIcon && expandIcon.classList.contains('fa-chevron-down')) {
            showChildrenDomains(childUrn);
        }
    });
}

function expandAllDomains() {
    console.log('Expanding all domains');
    const expandButtons = document.querySelectorAll('.expand-button');
    expandButtons.forEach(button => {
        const icon = button.querySelector('.expand-icon');
        if (icon && icon.classList.contains('fa-chevron-right')) {
            button.click();
        }
    });
}

function collapseAllDomains() {
    console.log('Collapsing all domains');
    const expandButtons = document.querySelectorAll('.expand-button');
    expandButtons.forEach(button => {
        const icon = button.querySelector('.expand-icon');
        if (icon && icon.classList.contains('fa-chevron-down')) {
            button.click();
        }
    });
}

function applyCurrentExpansionState() {
    console.log('[DEBUG] Applying current expansion state...');
    
    // Hide all child domains initially (they should be collapsed by default)
    const childRows = document.querySelectorAll('tr[data-level]:not([data-level="0"])');
    console.log(`[DEBUG] Found ${childRows.length} child rows to hide`);
    
    childRows.forEach(row => {
        const level = row.getAttribute('data-level');
        const urn = row.getAttribute('data-urn');
        const parentUrn = row.getAttribute('data-parent');
        console.log(`[DEBUG] Hiding child row: level=${level}, urn=${urn}, parent=${parentUrn}`);
        row.style.display = 'none';
    });
    
    // Then show children of expanded domains
    const expandedButtons = document.querySelectorAll('.expand-button .fa-chevron-down');
    console.log(`[DEBUG] Found ${expandedButtons.length} expanded buttons`);
    
    expandedButtons.forEach(icon => {
        const button = icon.closest('.expand-button');
        const row = button.closest('tr');
        const domainUrn = row.getAttribute('data-urn');
        console.log(`[DEBUG] Showing children for expanded domain: ${domainUrn}`);
        if (domainUrn) {
            showChildrenDomains(domainUrn);
        }
    });
}

function showDomainViewModal(domain) {
    // Populate basic information
    document.getElementById('modal-domain-name').textContent = domain.name || 'N/A';
    document.getElementById('modal-domain-description').textContent = domain.description || 'No description provided';
    document.getElementById('modal-domain-urn').textContent = domain.urn || 'N/A';
    
    // Handle parent domain
    const parentElement = document.getElementById('modal-domain-parent');
    if (domain.parent_urn || domain.parentDomain) {
        const parentUrn = domain.parent_urn || domain.parentDomain;
        const parentName = extractNameFromUrn(parentUrn);
        parentElement.innerHTML = `<code class="small">${parentName}</code>`;
    } else {
        parentElement.textContent = 'Root Domain';
    }
    
    // Update status badge
    const statusElement = document.getElementById('modal-domain-status');
    statusElement.textContent = domain.sync_status_display || domain.sync_status || 'Unknown';
    statusElement.className = `badge ${getStatusBadgeClass(domain.sync_status)}`;
    
    // Update metrics
    document.getElementById('modal-domain-owners').textContent = domain.owners_count || 0;
    document.getElementById('modal-domain-entities').textContent = domain.entities_count || 0;
    document.getElementById('modal-domain-children').textContent = domain.children_count || 0;
    
    // Update ownership information
    const ownersList = document.getElementById('modal-domain-owners-list');
    if (domain.owner_names && domain.owner_names.length > 0) {
        ownersList.innerHTML = `
            <h6 class="text-muted">Domain Owners</h6>
            <div class="d-flex flex-wrap gap-1">
                ${domain.owner_names.map(owner => 
                    `<span class="badge bg-secondary">${owner}</span>`
                ).join('')}
            </div>
        `;
    } else {
        ownersList.innerHTML = `
            <h6 class="text-muted">Domain Owners</h6>
            <p class="text-muted">No ownership information available</p>
        `;
    }
    
    // Update domain properties
    const iconElement = document.getElementById('modal-domain-icon');
    const colorElement = document.getElementById('modal-domain-color');
    const createdElement = document.getElementById('modal-domain-created');
    
    // Icon
    if (domain.icon_name) {
        const iconLibrary = domain.icon_library === 'MATERIAL' ? 'MATERIAL' : 'MATERIAL';
        const iconStyle = domain.icon_style || 'solid';
        iconElement.innerHTML = `<i class="${iconLibrary} fa-${domain.icon_name}"></i> ${domain.icon_name}`;
    } else {
        iconElement.innerHTML = '<i class="fas fa-folder"></i> Default';
    }
    
    // Color
    if (domain.color_hex) {
        colorElement.innerHTML = `<span class="badge" style="background-color: ${domain.color_hex};">${domain.color_hex}</span>`;
    } else {
        colorElement.innerHTML = '<span class="badge" style="background-color: #6c757d;">#6c757d (default)</span>';
    }
    
    // Created date
    if (domain.created_at) {
        const date = new Date(domain.created_at);
        createdElement.textContent = date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
    } else {
        createdElement.textContent = 'Unknown';
    }
    
    // Display structured properties
    displayStructuredProperties(domain);
    
    // Update DataHub link
    const datahubLink = document.getElementById('modal-datahub-link');
    if (domain.sync_status !== 'LOCAL_ONLY' && window.datahubUrl) {
        const domainId = domain.urn.split(':').pop();
        datahubLink.href = `${window.datahubUrl}/domain/${domainId}`;
        datahubLink.style.display = 'inline-block';
    } else {
        datahubLink.style.display = 'none';
    }
    
    // Update raw JSON
    const rawJsonElement = document.getElementById('modal-raw-json');
    rawJsonElement.innerHTML = `<code class="language-json">${JSON.stringify(domain, null, 2)}</code>`;
    
    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('domainViewModal'));
    modal.show();
}

// Helper function to display structured properties - matching glossary pattern
function displayStructuredProperties(domain) {
    const structuredPropsContainer = document.getElementById('modal-domain-structured-properties');
    
    // Get structured properties from multiple possible locations
    let structuredProperties = domain.structuredProperties || domain.structured_properties || 
                              (domain.raw_data && domain.raw_data.structuredProperties) ||
                              [];
    
    // If we have a nested structure, extract the properties array
    if (structuredProperties && structuredProperties.properties) {
        structuredProperties = structuredProperties.properties;
    }
    
    let propertiesHTML = '';
    
    // Structured Properties Section
    if (structuredProperties && structuredProperties.length > 0) {
        propertiesHTML = `
            <div class="card mb-3">
                <div class="card-header">
                    <h6 class="card-title mb-0"><i class="fas fa-cogs me-2"></i>Structured Properties (${structuredProperties.length})</h6>
                </div>
                <div class="card-body">
                    <div class="row">
                        ${structuredProperties.map(prop => {
                            // Handle new backend format with URN, displayName, and values
                            const propertyName = prop.urn || prop.propertyUrn || (prop.structuredProperty && prop.structuredProperty.urn) || 'Unknown';
                            const displayName = prop.displayName || prop.qualifiedName || propertyName.split(':').pop() || propertyName;
                            let propertyValue = '';
                            
                            if (prop.values && Array.isArray(prop.values)) {
                                // New format: values is an array of objects with stringValue/numberValue
                                propertyValue = prop.values.map(v => {
                                    if (v.stringValue) return v.stringValue;
                                    if (v.numberValue !== undefined) return v.numberValue.toString();
                                    return v;
                                }).filter(v => v !== null && v !== undefined).join(', ');
                            } else if (prop.value) {
                                // Old format: single value
                                propertyValue = typeof prop.value === 'object' ? JSON.stringify(prop.value) : prop.value;
                            }
                            
                            return `
                                <div class="col-md-6 mb-2">
                                    <dl class="row mb-0">
                                        <dt class="col-sm-5 text-truncate" title="${escapeHtml(propertyName)}">${escapeHtml(displayName)}:</dt>
                                        <dd class="col-sm-7 mb-0">
                                            <span class="text-break">${escapeHtml(propertyValue)}</span>
                                            ${propertyName !== displayName ? `<br><small class="text-muted" title="${escapeHtml(propertyName)}">URN: ${escapeHtml(propertyName)}</small>` : ''}
                                        </dd>
                                    </dl>
                                </div>
                            `;
                        }).join('')}
                    </div>
                </div>
            </div>
        `;
    }
    
    if (propertiesHTML) {
        structuredPropsContainer.innerHTML = propertiesHTML;
    } else {
        // Show empty card for structured properties
        structuredPropsContainer.innerHTML = `
            <div class="card mb-3">
                <div class="card-header">
                    <h6 class="card-title mb-0"><i class="fas fa-cogs me-2"></i>Structured Properties (0)</h6>
                </div>
                <div class="card-body">
                    <div class="text-center py-3 text-muted">
                        <i class="fas fa-cogs fa-2x mb-2"></i><br>
                        No structured properties available
                    </div>
                </div>
            </div>
        `;
    }
}

// Add expand/collapse handlers for hierarchy
function attachExpandCollapseHandlers(content) {
    // Attach expand/collapse button handlers
    const expandButtons = content.querySelectorAll('.expand-button');
    expandButtons.forEach(button => {
        button.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            
            const row = this.closest('tr');
            const domainUrn = row.getAttribute('data-urn');
            if (domainUrn) {
                toggleDomainExpansion(domainUrn);
            }
        });
    });
}

// Add checkbox handlers with hierarchical selection logic
function attachCheckboxHandlers(content, tabType) {
    // Attach individual checkbox handlers
    content.querySelectorAll('.item-checkbox').forEach(checkbox => {
        checkbox.addEventListener('change', function() {
            if (this.checked) {
                // When checking a checkbox, select all its descendants
                selectAllDescendants(this, content);
                // Also select all parent checkboxes (so parents are selected when any child is selected)
                selectParentCheckboxes(this, content);
            } else {
                // When unchecking a checkbox, unselect all its descendants
                unselectAllDescendants(this, content);
                // Also check if any parent should be unselected (if no siblings are selected)
                updateParentCheckboxStates(this, content);
            }
            updateBulkActionsVisibility(tabType);
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
            updateBulkActionsVisibility(tabType);
        });
    }
}

// Helper function to select all descendants when a checkbox is selected
function selectAllDescendants(checkbox, content) {
    const row = checkbox.closest('tr');
    const domainUrn = row.getAttribute('data-urn');
    
    if (domainUrn) {
        // Find all direct children
        const childRows = content.querySelectorAll(`tr[data-parent="${domainUrn}"]`);
        childRows.forEach(childRow => {
            const childCheckbox = childRow.querySelector('.item-checkbox');
            if (childCheckbox && !childCheckbox.checked) {
                childCheckbox.checked = true;
                // Recursively select all descendants of this child
                selectAllDescendants(childCheckbox, content);
            }
        });
    }
}

// Helper function to unselect all descendants when a checkbox is unselected
function unselectAllDescendants(checkbox, content) {
    const row = checkbox.closest('tr');
    const domainUrn = row.getAttribute('data-urn');
    
    if (domainUrn) {
        // Find all direct children
        const childRows = content.querySelectorAll(`tr[data-parent="${domainUrn}"]`);
        childRows.forEach(childRow => {
            const childCheckbox = childRow.querySelector('.item-checkbox');
            if (childCheckbox && childCheckbox.checked) {
                childCheckbox.checked = false;
                // Recursively unselect all descendants of this child
                unselectAllDescendants(childCheckbox, content);
            }
        });
    }
}

// Helper function to select parent checkboxes when child is selected
function selectParentCheckboxes(checkbox, content) {
    const row = checkbox.closest('tr');
    const parentUrn = row.getAttribute('data-parent');
    
    if (parentUrn) {
        const parentRow = content.querySelector(`tr[data-urn="${parentUrn}"]`);
        if (parentRow) {
            const parentCheckbox = parentRow.querySelector('.item-checkbox');
            if (parentCheckbox && !parentCheckbox.checked) {
                parentCheckbox.checked = true;
                // Recursively select parent's parents
                selectParentCheckboxes(parentCheckbox, content);
            }
        }
    }
}

// Helper function to update parent checkbox states based on children
function updateParentCheckboxStates(checkbox, content) {
    const row = checkbox.closest('tr');
    const parentUrn = row.getAttribute('data-parent');
    
    if (parentUrn) {
        const parentRow = content.querySelector(`tr[data-urn="${parentUrn}"]`);
        if (parentRow) {
            const parentCheckbox = parentRow.querySelector('.item-checkbox');
            if (parentCheckbox) {
                // Check if any siblings are still selected
                const siblingRows = content.querySelectorAll(`tr[data-parent="${parentUrn}"]`);
                const hasSelectedSiblings = Array.from(siblingRows).some(siblingRow => {
                    const siblingCheckbox = siblingRow.querySelector('.item-checkbox');
                    return siblingCheckbox && siblingCheckbox.checked;
                });
                
                // If no siblings are selected, unselect the parent
                if (!hasSelectedSiblings && parentCheckbox.checked) {
                    parentCheckbox.checked = false;
                    // Recursively update parent's parents
                    updateParentCheckboxStates(parentCheckbox, content);
                }
            }
        }
    }
}

// ========================
// Select2 Ownership Functions (from tags page)
// ========================

// Users and groups cache for ownership
let usersAndGroupsCache = {
    users: [],
    groups: [],
    ownership_types: [],
    lastFetched: null
};

// Load users and groups for ownership
async function loadUsersAndGroups() {
    // Check if data is already cached and recent
    const now = Date.now();
    const cacheExpiry = 5 * 60 * 1000; // 5 minutes
    
    if (usersAndGroupsCache.lastFetched && (now - usersAndGroupsCache.lastFetched) < cacheExpiry) {
        console.log('Using cached users and groups data');
        return;
    }

    console.log('Loading users and groups from server...');
    
    try {
        const response = await fetch('/metadata/api/users-groups/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCsrfToken()
            },
            body: JSON.stringify({ type: 'all' })
        });

        if (response.ok) {
            const result = await response.json();
            
            if (result.success && result.data) {
                usersAndGroupsCache.users = result.data.users || [];
                usersAndGroupsCache.groups = result.data.groups || [];
                usersAndGroupsCache.ownership_types = result.data.ownership_types || [];
                usersAndGroupsCache.lastFetched = now;
                
                console.log(`Loaded ${usersAndGroupsCache.users.length} users, ${usersAndGroupsCache.groups.length} groups, ${usersAndGroupsCache.ownership_types.length} ownership types`);
            } else {
                console.error('API returned error:', result.error);
            }
        } else {
            console.error('Failed to load users/groups data:', response.status);
        }
    } catch (error) {
        console.error('Error loading users and groups:', error);
    }
}

// Setup the ownership interface with dynamic owner entries
function setupOwnershipInterface(containerId, addButtonId, labelId, helpTextId) {
    console.log('Setting up ownership interface for:', containerId, addButtonId);
    
    const container = document.getElementById(containerId);
    const addButton = document.getElementById(addButtonId);
    
    if (!container) {
        console.error('Container not found:', containerId);
        return;
    }
    
    if (!addButton) {
        console.error('Add button not found:', addButtonId);
        return;
    }
    
    console.log('Found container and button, setting up interface...');
    
    // Clear existing sections
    container.innerHTML = '';
    
    // Remove any existing event listeners to prevent multiple handlers
    const newAddButton = addButton.cloneNode(true);
    addButton.parentNode.replaceChild(newAddButton, addButton);
    
    // Setup add ownership section button with enhanced interaction
    newAddButton.addEventListener('click', (e) => {
        e.preventDefault();
        console.log('Add ownership section clicked for container:', containerId);
        addOwnershipSection(containerId);
        // Show the ownership section when adding the first one
        showOwnershipSection(containerId, labelId, helpTextId);
        // Add a subtle animation to the button
        newAddButton.style.transform = 'scale(0.95)';
        setTimeout(() => {
            newAddButton.style.transform = 'scale(1)';
        }, 150);
    });
    
    console.log('Ownership interface setup complete for:', containerId);
    
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
    }
}

function getSelectedOwnershipTypes(currentSelect) {
    const container = currentSelect.closest('#ownership-sections-container, #edit-ownership-sections-container');
    if (!container) return [];
    
    const ownershipTypeSelects = container.querySelectorAll('.ownership-type-select');
    const selectedTypes = [];
    
    ownershipTypeSelects.forEach(select => {
        if (select !== currentSelect && select.value) {
            selectedTypes.push(select.value);
        }
    });
    
    return selectedTypes;
}

function refreshAllOwnershipTypeDropdowns() {
    // Refresh all ownership type dropdowns to update available options
    document.querySelectorAll('.ownership-type-select').forEach(select => {
        const currentValue = select.value;
        populateOwnershipTypeSelect(select);
        // Restore the current value if it's still valid
        if (currentValue && Array.from(select.options).some(option => option.value === currentValue)) {
            select.value = currentValue;
        }
    });
}

function removeSection(sectionId) {
    const section = document.getElementById(sectionId);
    if (section) {
        const container = section.parentElement;
        section.remove();
        updateRemoveButtons(container);
        refreshAllOwnershipTypeDropdowns();
        
        // Hide ownership section if no sections remain
        if (container.children.length === 0) {
            hideOwnershipSectionIfEmpty(container.id);
        }
    }
}

function addOwnershipSection(containerId) {
    const container = document.getElementById(containerId);
    if (!container) {
        console.error('Container not found:', containerId);
        return;
    }
    
    // Show the ownership section when adding the first owner
    const isFirstSection = container.children.length === 0;
    console.log('Adding ownership section - isFirstSection:', isFirstSection, 'container children:', container.children.length);
    
    if (isFirstSection) {
        // Get label and help text IDs based on container ID
        let labelId, helpTextId;
        if (containerId === 'ownership-sections-container') {
            labelId = 'ownership-label';
            helpTextId = 'ownership-help-text';
        } else if (containerId === 'edit-ownership-sections-container') {
            labelId = 'edit-ownership-label';
            helpTextId = 'edit-ownership-help-text';
        }
        console.log('Calling showOwnershipSection with:', { containerId, labelId, helpTextId });
        showOwnershipSection(containerId, labelId, helpTextId);
    }
    
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
    
    console.log('Inserting HTML for section ID:', sectionId);
    container.insertAdjacentHTML('beforeend', sectionHTML);
    console.log('HTML inserted, container children count:', container.children.length);
    
    // Use lastElementChild directly instead of getElementById - more reliable
    const newSection = container.lastElementChild;
    if (newSection && newSection.classList.contains('card')) {
        console.log('Found section using lastElementChild, ID:', newSection.id);
        // Verify the ID matches what we expect, if not, update the remove button
        if (newSection.id !== sectionId) {
            console.log('ID mismatch - expected:', sectionId, 'actual:', newSection.id);
            const removeButton = newSection.querySelector('button[onclick*="removeSection"]');
            if (removeButton) {
                removeButton.setAttribute('onclick', `removeSection('${newSection.id}')`);
            }
        }
        addOwnershipSectionElements(newSection, container);
        return;
    }
    
    console.error('Could not find newly created section using lastElementChild');
    
    // Final fallback - try getElementById with a delay
    setTimeout(() => {
        const delayedSection = document.getElementById(sectionId);
        if (delayedSection) {
            console.log('Found section with delay using getElementById');
            addOwnershipSectionElements(delayedSection, container);
        } else {
            console.error('Complete failure to find section even with delay');
        }
    }, 50);
}

// Helper function to setup ownership section elements
function addOwnershipSectionElements(newSection, container) {
    const ownersSelect = newSection.querySelector('.owners-select');
    const ownershipTypeSelect = newSection.querySelector('.ownership-type-select');
    
    if (!ownersSelect || !ownershipTypeSelect) {
        console.error('Failed to find select elements in section');
        return;
    }
    
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
    
    updateRemoveButtons(container);
}



// Update visibility of remove buttons (hide if only one section)
function updateRemoveButtons(container) {
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
function hideOwnershipSectionIfEmpty(containerId, labelId, helpTextId) {
    const container = document.getElementById(containerId);
    let addButtonId;
    
    // Get the correct button ID based on container
    if (containerId === 'ownership-sections-container') {
        addButtonId = 'add-ownership-section';
    } else if (containerId === 'edit-ownership-sections-container') {
        addButtonId = 'add-edit-ownership-section';
    } else {
        addButtonId = containerId.replace('-container', '').replace('sections', 'section');
    }
    
    const addButton = document.getElementById(addButtonId);
    const label = document.getElementById(labelId);
    const helpText = document.getElementById(helpTextId);
    
    if (!container) return;
    
    const hasSections = container.children.length > 0;
    
    if (hasSections) {
        // Show the full ownership section
        if (label) label.style.display = 'block';
        container.style.display = 'block';
        if (helpText) helpText.style.display = 'block';
        
        // Update button text and style to normal
        if (addButton) {
            addButton.innerHTML = '<i class="fas fa-plus me-1"></i> Add Owner';
            addButton.className = 'btn btn-sm btn-outline-primary mt-2';
        }
    } else {
        // Hide everything except the "Add Owner" button
        if (label) label.style.display = 'none';
        container.style.display = 'none';
        if (helpText) helpText.style.display = 'none';
        
        // Show only the "Add Owner" button with simplified style
        if (addButton) {
            addButton.style.display = 'block';
            addButton.textContent = '+ Add Owner';
            addButton.className = 'btn btn-sm btn-outline-primary';
        }
    }
}

/**
 * Show the ownership section when adding owners
 */
function showOwnershipSection(containerId, labelId, helpTextId) {
    const container = document.getElementById(containerId);
    let addButtonId;
    
    // Get the correct button ID based on container
    if (containerId === 'ownership-sections-container') {
        addButtonId = 'add-ownership-section';
    } else if (containerId === 'edit-ownership-sections-container') {
        addButtonId = 'add-edit-ownership-section';
    } else {
        addButtonId = containerId.replace('-container', '').replace('sections', 'section');
    }
    
    const addButton = document.getElementById(addButtonId);
    const label = document.getElementById(labelId);
    const helpText = document.getElementById(helpTextId);
    
    console.log('showOwnershipSection called with:', { containerId, labelId, helpTextId, addButtonId });
    console.log('Elements found:', { 
        container: !!container, 
        addButton: !!addButton, 
        label: !!label, 
        helpText: !!helpText 
    });
    
    if (!container) return;
    
    // Show all elements
    if (label) label.style.display = 'block';
    container.style.display = 'block';
    if (helpText) helpText.style.display = 'block';
    
    // Update button text and style
    if (addButton) {
        addButton.innerHTML = '<i class="fas fa-plus me-1"></i> Add Owner';
        addButton.className = 'btn btn-sm btn-outline-primary mt-2';
    }
}

