let domainsData = {};
let currentFilters = new Set();
let currentOverviewFilter = null;

// Pagination settings
const ITEMS_PER_PAGE = 25;
let currentSyncedPage = 1;
let currentLocalPage = 1;
let currentRemotePage = 1;

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
            updateStatistics();
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

function updateStatistics() {
    const stats = domainsData.statistics || {};
    
    // Overview statistics
    document.getElementById('total-items').textContent = stats.total_items || 0;
    document.getElementById('synced-count').textContent = stats.synced_count || 0;
    document.getElementById('local-only-count').textContent = (stats.total_items || 0) - (stats.synced_count || 0) - (domainsData.remote_only_items?.length || 0);
    document.getElementById('remote-only-count').textContent = domainsData.remote_only_items?.length || 0;
    
    // Content statistics
    document.getElementById('owned-domains').textContent = stats.owned_items || 0;
    document.getElementById('domains-with-entities').textContent = stats.items_with_entities || 0;
    
    // Tab badges
    document.getElementById('synced-badge').textContent = stats.synced_count || 0;
    document.getElementById('local-badge').textContent = (stats.total_items || 0) - (stats.synced_count || 0) - (domainsData.remote_only_items?.length || 0);
    document.getElementById('remote-badge').textContent = domainsData.remote_only_items?.length || 0;
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

                    case 'with-entities':
                        if ((domainData.entities_count || 0) > 0) passesFilter = true;
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
    
    // Create lookup maps and extract parent URNs
    const domainLookup = new Map();
    const childrenMap = new Map();
    const rootDomains = [];
    
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
        
        // Store the extracted parent URN back on the domain
        domain.parent_urn = parentUrn;
        
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
    
    // Build hierarchy starting from root domains
    const hierarchyOrder = [];
    
    function addDomainToHierarchy(domain, level, isLastChild = false) {
        // Set hierarchy properties
        domain.hierarchy_level = level;
        domain.has_children = childrenMap.has(domain.urn);
        domain.is_expanded = false; // Default collapsed
        domain.is_last_child = isLastChild;
        
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
    
    // Rebuild items array in hierarchy order
    if (hierarchyOrder.length > 0) {
        // Clear the original items and replace with hierarchy order
        items.length = 0;
        hierarchyOrder.forEach(domain => {
            if (dataAccessType === 'combined') {
                // Find the original item that contains this domain
                const originalItem = items.find(item => {
                    const itemDomain = item.combined || item;
                    return itemDomain.urn === domain.urn;
                });
                if (originalItem) {
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
    
    // Pagination based on root nodes
    const currentPage = tabType === 'synced' ? currentSyncedPage : 
                       tabType === 'local' ? currentLocalPage : currentRemotePage;
    
    // Separate root nodes from all items for pagination
    const rootNodes = items.filter(item => {
        const domainData = item.combined || item;
        return !domainData.parent_urn || domainData.hierarchy_level === 0;
    });
    
    const totalRootNodes = rootNodes.length;
    const totalPages = Math.ceil(totalRootNodes / ITEMS_PER_PAGE);
    const rootStartIndex = (currentPage - 1) * ITEMS_PER_PAGE;
    const rootEndIndex = Math.min(rootStartIndex + ITEMS_PER_PAGE, totalRootNodes);
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
                        <th class="sortable" data-column="name" data-tab="${tabType}" style="cursor: pointer;">
                            Name ${getSortIcon('name', tabType)}
                        </th>
                        <th>Description</th>
                        <th class="sortable" data-column="owners" data-tab="${tabType}" style="cursor: pointer;">
                            Owners ${getSortIcon('owners', tabType)}
                        </th>
                        <th class="sortable" data-column="entities" data-tab="${tabType}" style="cursor: pointer;">
                            Entities ${getSortIcon('entities', tabType)}
                        </th>
                        <th>URN</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
    `;
    
    if (pageItems.length === 0) {
        html += `
                    <tr>
                        <td colspan="7" class="text-center py-4 text-muted">
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
    
    // Add pagination
    if (totalPages > 1) {
        html += renderPagination(currentPage, totalPages, totalRootNodes, startItem, endItem, tabType);
    }
    
    contentElement.innerHTML = html;
    attachActionButtonHandlers();
    reattachSelectAllHandlers();
    
    // Attach sorting handlers
    attachSortingHandlers(contentElement, tabType);
    
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
    const iconLibrary = domainData.icon_library || 'font-awesome';
    const colorHex = domainData.color_hex || '#6c757d';
    
    // Hierarchy support
    const level = domainData.hierarchy_level || 0;
    const hasChildren = domainData.has_children || false;
    const isExpanded = domainData.is_expanded || false;
            // Extract parent URN from parentDomains structure
            let parentUrn = null;
            if (domainData.parentDomains && domainData.parentDomains.domains && domainData.parentDomains.domains.length > 0) {
                parentUrn = domainData.parentDomains.domains[0].urn;
            } else if (domainData.parent_urn) {
                parentUrn = domainData.parent_urn;
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
    const iconClass = iconLibrary === 'font-awesome' ? `fas fa-${iconName}` : iconName;
    const domainIcon = `
        <div class="domain-icon" style="background-color: ${colorHex}; color: white;">
            <i class="${iconClass}"></i>
        </div>
    `;
    
    return `
        <tr data-item='${JSON.stringify(domainData)}' data-urn="${urn}" data-level="${level}" ${parentUrn ? `data-parent="${parentUrn}"` : ''}>
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
            <td>
                <span class="badge bg-secondary">${ownersCount}</span>
            </td>
            <td>
                <span class="badge bg-primary">${entitiesCount}</span>
            </td>
            <td>
                <code class="small text-muted">${escapeHtml(urn)}</code>
            </td>
            <td>
                <div class="btn-group" role="group">
                    ${actionButtons}
                </div>
            </td>
        </tr>
    `;
}

function renderPagination(currentPage, totalPages, totalItems, startItem, endItem, tabType) {
    return `
        <div class="pagination-container">
            <div class="pagination-info">
                Showing ${startItem}-${endItem} of ${totalItems} domains
            </div>
            <nav aria-label="Pagination">
                <ul class="pagination pagination-sm mb-0">
                    <li class="page-item ${currentPage === 1 ? 'disabled' : ''}">
                        <a class="page-link" href="javascript:void(0)" onclick="changePage('${tabType}', ${currentPage - 1})">Previous</a>
                    </li>
                    ${renderPageNumbers(currentPage, totalPages, tabType)}
                    <li class="page-item ${currentPage === totalPages ? 'disabled' : ''}">
                        <a class="page-link" href="javascript:void(0)" onclick="changePage('${tabType}', ${currentPage + 1})">Next</a>
                    </li>
                </ul>
            </nav>
        </div>
    `;
}

function renderPageNumbers(currentPage, totalPages, tabType) {
    let html = '';
    const maxVisible = 5;
    let startPage = Math.max(1, currentPage - Math.floor(maxVisible / 2));
    let endPage = Math.min(totalPages, startPage + maxVisible - 1);
    
    // Adjust start page if we're near the end
    if (endPage - startPage + 1 < maxVisible) {
        startPage = Math.max(1, endPage - maxVisible + 1);
    }
    
    for (let i = startPage; i <= endPage; i++) {
        html += `
            <li class="page-item ${i === currentPage ? 'active' : ''}">
                <a class="page-link" href="javascript:void(0)" onclick="changePage('${tabType}', ${i})">${i}</a>
            </li>
        `;
    }
    
    return html;
}

function changePage(tabType, page) {
    if (tabType === 'synced') {
        currentSyncedPage = page;
    } else if (tabType === 'local') {
        currentLocalPage = page;
    } else if (tabType === 'remote') {
        currentRemotePage = page;
    }
    renderTab(`${tabType}-items`);
}

function updateBulkActionsVisibility() {
    ['synced', 'local', 'remote'].forEach(tabType => {
        const tabContent = document.getElementById(`${tabType}-content`);
        const bulkActions = document.getElementById(`${tabType}-bulk-actions`);
        const selectedCount = document.getElementById(`${tabType}-selected-count`);
        
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
    
    // View in DataHub button - only for non-local domains
    if (domainUrn && !domainUrn.includes('local:')) {
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
                buttons.push(`<button type="button" class="btn btn-sm btn-outline-warning add-domain-to-pr" onclick="addDomainToPR('${domainId}')" title="Add to PR">
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
                buttons.push(`<button type="button" class="btn btn-sm btn-outline-warning add-domain-to-pr" onclick="addDomainToPR('${domainId}')" title="Add to PR">
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
                buttons.push(`<button type="button" class="btn btn-sm btn-outline-warning add-domain-to-pr" onclick="addDomainToPR('${domainId}')" title="Add to PR">
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
        // Remote-only domain actions
        buttons.push(`<button type="button" class="btn btn-sm btn-outline-secondary edit-remote-domain" onclick="editRemoteDomain('${domainUrn}')" title="Edit (Remote)">
            <i class="fas fa-edit"></i>
        </button>`);
        buttons.push(`<button type="button" class="btn btn-sm btn-outline-warning add-remote-domain-to-pr" onclick="addRemoteDomainToPR('${domainUrn}')" title="Add to PR">
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
// Sorting functions
function getSortIcon(column, tabType) {
    if (currentSort.column !== column || currentSort.tabType !== tabType) {
        return '<i class="fas fa-sort text-muted ms-1"></i>';
    }
    
    if (currentSort.direction === 'asc') {
        return '<i class="fas fa-sort-up text-primary ms-1"></i>';
    } else {
        return '<i class="fas fa-sort-down text-primary ms-1"></i>';
    }
}

function attachSortingHandlers(contentDiv, tabType) {
    const sortableHeaders = contentDiv.querySelectorAll('.sortable');
    
    sortableHeaders.forEach(header => {
        header.addEventListener('click', function() {
            const column = this.getAttribute('data-column');
            const tab = this.getAttribute('data-tab');
            
            // Toggle direction if same column, otherwise default to asc
            if (currentSort.column === column && currentSort.tabType === tab) {
                currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
            } else {
                currentSort.column = column;
                currentSort.direction = 'asc';
                currentSort.tabType = tab;
            }
            
            // Re-render the tab with new sorting
            renderTab(`${tabType}-items`);
        });
    });
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
            return domainData.name || '';
        case 'owners':
            return domainData.owners_count || 0;
        case 'entities':
            return domainData.entities_count || 0;
        default:
            return '';
    }
}

function showDomainDetails(domain) {
    console.log('Showing domain details for:', domain.name);
    
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
    
    document.getElementById('modal-domain-name').textContent = name;
    document.getElementById('modal-domain-parent').innerHTML = parentDomain !== 'No parent domain' ? 
        `<span class="badge bg-info">${escapeHtml(parentDomain)}</span>${parentUrn ? `<br><small class="text-muted">${escapeHtml(parentUrn)}</small>` : ''}` : parentDomain;
    document.getElementById('modal-domain-description').textContent = description;
    document.getElementById('modal-domain-urn').textContent = domain.urn || 'No URN available';
    
    // Status
    const statusBadge = document.getElementById('modal-domain-status');
    statusBadge.textContent = domain.sync_status_display || domain.sync_status;
    statusBadge.className = `badge ${getStatusBadgeClass(domain.sync_status)}`;
    
    // Metrics & Ownership
    document.getElementById('modal-domain-owners').textContent = domain.owners_count || 0;
    document.getElementById('modal-domain-entities').textContent = domain.entities_count || 0;
    
    // Calculate sub-domains count
    const subDomainsCount = calculateSubDomainsCount(domain.urn);
    document.getElementById('modal-domain-children').textContent = subDomainsCount;
    
    // Owner details - format like tags and glossary
    const ownersListElement = document.getElementById('modal-domain-owners-list');
    
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
                    <i class="fas fa-crown me-1"></i>Domain Owners
                </h6>
                <div class="ms-3">
                    ${domain.owner_names.map(owner => `
                        <div class="d-flex align-items-center mb-1">
                            <i class="fas fa-user text-muted me-2"></i>
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
    
    // Domain Properties
    const iconName = domain.icon_name || 'folder';
    const iconClass = domain.icon_library === 'font-awesome' ? `fas fa-${iconName}` : iconName;
    const colorHex = domain.color_hex || '#6c757d';
    
    document.getElementById('modal-domain-icon').innerHTML = `<i class="${iconClass}"></i> ${iconName}`;
    document.getElementById('modal-domain-color').innerHTML = `<span class="badge" style="background-color: ${colorHex};">${colorHex}</span>`;
    
    // Created date
    const createdDate = domain.created_at ? formatDate(domain.created_at) : 'Unknown';
    document.getElementById('modal-domain-created').textContent = createdDate;
    
    // Raw JSON Data
    document.getElementById('modal-raw-json').innerHTML = `<code>${escapeHtml(JSON.stringify(domain, null, 2))}</code>`;
    
    // DataHub link
    const datahubLink = document.getElementById('modal-datahub-link');
    if (domain.urn && !domain.urn.includes('local:') && domainsData.datahub_url) {
        datahubLink.href = getDataHubUrl(domain.urn, 'domain');
        datahubLink.style.display = 'inline-block';
    } else {
        datahubLink.style.display = 'none';
    }
    
    // Show modal
    new bootstrap.Modal(document.getElementById('domainViewModal')).show();
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
        // Populate the enhanced edit modal
        document.getElementById('editDomainName').value = domainData.name || '';
        document.getElementById('editDomainDescription').value = domainData.description || '';
        
        // Populate appearance fields
        document.getElementById('editDomainIcon').value = domainData.icon_name || 'folder';
        document.getElementById('editDomainColor').value = domainData.color_hex || '#6c757d';
        document.getElementById('editDomainColorText').value = domainData.color_hex || '#6c757d';
        
        // Update icon preview
        const iconPreview = document.getElementById('editDomainIconPreview');
        iconPreview.className = `fas fa-${domainData.icon_name || 'folder'}`;
        
        // Populate owners field
        if (domainData.owner_names && domainData.owner_names.length > 0) {
            document.getElementById('editDomainOwners').value = domainData.owner_names.join(', ');
        } else {
            document.getElementById('editDomainOwners').value = '';
        }
        
        // Populate parent domain dropdown
        populateParentDomainDropdown(domainData.parent_urn);
        
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
    } else {
        showError('Domain data not found');
    }
}

function populateParentDomainDropdown(currentParentUrn) {
    const dropdown = document.getElementById('editDomainParent');
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
    const checkedBoxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    const items = Array.from(checkedBoxes).map(cb => cb.value);
    
    if (items.length === 0) {
        showError('No domains selected');
        return;
    }
    
    console.log(`Bulk adding ${items.length} domains to PR`);
    showSuccess(`Bulk add to PR for ${items.length} domain(s) (feature to be implemented)`);
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
    
    console.log(`Bulk syncing ${domainUrns.length} domains to local`);
    showSuccess(`Bulk sync to local started for ${domainUrns.length} domain(s)`);
    
    // Process each domain individually
    domainUrns.forEach(urn => syncDomainToLocal(urn));
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
    return '...' + urn.substr(urn.length - maxLength + 3);
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

function showError(message) {
    const notification = document.createElement('div');
    notification.className = 'alert alert-danger alert-dismissible fade show notification';
    notification.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    document.body.appendChild(notification);
    
    setTimeout(() => {
        if (notification.parentNode) {
            notification.parentNode.removeChild(notification);
        }
    }, 5000);
}

function showSuccess(message) {
    const notification = document.createElement('div');
    notification.className = 'alert alert-success alert-dismissible fade show notification';
    notification.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    document.body.appendChild(notification);
    
    setTimeout(() => {
        if (notification.parentNode) {
            notification.parentNode.removeChild(notification);
        }
    }, 5000);
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
    // Hide all child domains initially (they should be collapsed by default)
    const childRows = document.querySelectorAll('tr[data-level]:not([data-level="0"])');
    childRows.forEach(row => {
        row.style.display = 'none';
    });
    
    // Then show children of expanded domains
    const expandedButtons = document.querySelectorAll('.expand-button .fa-chevron-down');
    expandedButtons.forEach(icon => {
        const button = icon.closest('.expand-button');
        const row = button.closest('tr');
        const domainUrn = row.getAttribute('data-urn');
        if (domainUrn) {
            showChildrenDomains(domainUrn);
        }
    });
} 