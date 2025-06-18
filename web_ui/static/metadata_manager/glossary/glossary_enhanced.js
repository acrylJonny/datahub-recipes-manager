console.log('Glossary script loading...');
// Global variables
let glossaryData = {
    synced_items: [],
    local_only_items: [],
    remote_only_items: [],
    datahub_url: ''
};
let currentSearch = {
    synced: '',
    local: '',
    remote: ''
};

// Pagination variables - only paginate at root level
let currentPagination = {
    synced: { page: 1, itemsPerPage: 25 },
    local: { page: 1, itemsPerPage: 25 },
    remote: { page: 1, itemsPerPage: 25 }
};

// Filter state variables
let currentOverviewFilter = 'synced'; // Default to synced tab
let currentFilters = new Set();

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

// Make cache globally accessible for connection switching
if (!window.usersAndGroupsCache) {
    window.usersAndGroupsCache = usersAndGroupsCache;
    window.usersAndGroupsCacheByConnection = usersAndGroupsCacheByConnection;
}

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
    }
};

document.addEventListener('DOMContentLoaded', function() {
    console.log('DOM loaded, starting glossary initialization...');
    // Initialize connection cache
    getCurrentConnectionCache();
    loadGlossaryData();
    loadUsersAndGroups(); // Load users and groups cache for owner lookup
    
    // Set up filter listeners
    setupFilterListeners();
    
    // Search functionality for each tab
    ['synced', 'local', 'remote'].forEach(tab => {
        const searchInput = document.getElementById(`${tab}-search`);
        const clearButton = document.getElementById(`${tab}-clear`);
        
        searchInput.addEventListener('input', function() {
            currentSearch[tab] = this.value.toLowerCase();
            // Reset pagination when searching
            currentPagination[tab].page = 1;
            displayTabContent(tab);
        });
        
        clearButton.addEventListener('click', function() {
            searchInput.value = '';
            currentSearch[tab] = '';
            // Reset pagination when clearing search
            currentPagination[tab].page = 1;
            displayTabContent(tab);
        });
    });
    
    // Global search functionality
    const globalSearchInput = document.getElementById('globalSearchInput');
    if (globalSearchInput) {
        globalSearchInput.addEventListener('input', function() {
            const searchTerm = this.value.toLowerCase();
            // Apply search to all tabs
            ['synced', 'local', 'remote'].forEach(tab => {
                currentSearch[tab] = searchTerm;
                // Reset pagination when searching
                currentPagination[tab].page = 1;
                const tabSearchInput = document.getElementById(`${tab}-search`);
                if (tabSearchInput) {
                    tabSearchInput.value = this.value;
                }
            });
            // Refresh current active tab
            const activeTab = document.querySelector('.nav-link.active');
            if (activeTab) {
                const tabType = activeTab.id.replace('-tab', '');
                displayTabContent(tabType);
            }
        });
    }
    

    
    // Refresh button
    document.getElementById('refreshGlossary').addEventListener('click', function() {
        loadGlossaryData();
    });
});

function loadGlossaryData() {
    console.log('Loading glossary data...');
    showLoading(true);
    
    fetch('/metadata/glossary/data/')
        .then(response => {
            console.log('Received response:', response.status);
            return response.json();
        })
        .then(data => {
            console.log('Parsed data:', data.success, 'Items:', data.data?.local_only_items?.length || 0);
            if (data.success) {
                glossaryData = data.data;
                console.log('DataHub URL:', glossaryData.datahub_url);
                
                // Safely update statistics
                if (data.data && data.data.statistics) {
                    updateStatistics(data.data.statistics);
                } else {
                    console.warn('No statistics data available');
                    updateStatistics({});
                }
                
                updateTabBadges();
                displayAllTabs();
            } else {
                console.error('Data loading failed:', data.error);
                showError('Failed to load glossary: ' + (data.error || 'Unknown error'));
            }
        })
        .catch(error => {
            console.error('Error loading glossary:', error);
            showError('Failed to load glossary data: ' + error.message);
        })
        .finally(() => {
            showLoading(false);
        });
}

function showLoading(show) {
    document.getElementById('loading-indicator').style.display = show ? 'block' : 'none';
    document.getElementById('glossary-content').style.display = show ? 'none' : 'block';
}

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

function switchToTab(tabType) {
    // Activate the correct tab
    document.querySelectorAll('#glossaryTabs .nav-link').forEach(tab => {
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
        'synced': 'synced-items',
        'local': 'local-items',
        'remote': 'remote-items'
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

function applyFilters() {
    // Apply filters to all tabs, not just the active one
    displayAllTabs();
}



function updateStatistics(stats) {
    console.log('Updating statistics with:', stats);
    
    // Safely update each statistic with error handling
    try {
        document.getElementById('total-items').textContent = stats?.total_items || 0;
        document.getElementById('synced-count').textContent = stats?.synced_count || 0;
        document.getElementById('local-only-count').textContent = stats?.local_count || 0;
        document.getElementById('remote-only-count').textContent = stats?.remote_count || 0;
        document.getElementById('owned-items').textContent = stats?.owned_items || 0;
        document.getElementById('items-with-relationships').textContent = stats?.items_with_relationships || 0;
        document.getElementById('items-with-properties').textContent = stats?.items_with_custom_properties || 0;
        document.getElementById('items-with-structured-properties').textContent = stats?.items_with_structured_properties || 0;
    } catch (error) {
        console.error('Error updating statistics:', error);
        // Set all to 0 if there's an error
        ['total-items', 'synced-count', 'local-only-count', 'remote-only-count', 
         'owned-items', 'items-with-relationships', 'items-with-properties', 'items-with-structured-properties'].forEach(id => {
            const element = document.getElementById(id);
            if (element) element.textContent = '0';
        });
    }
}

function updateTabBadges() {
    document.getElementById('synced-badge').textContent = glossaryData.synced_items.length;
    document.getElementById('local-badge').textContent = glossaryData.local_only_items.length;
    document.getElementById('remote-badge').textContent = glossaryData.remote_only_items.length;
}

function displayAllTabs() {
    displayTabContent('synced');
    displayTabContent('local');
    displayTabContent('remote');
    
    // Set default active state for synced overview filter if no filter is currently active
    if (!currentOverviewFilter) {
        currentOverviewFilter = 'synced';
        const syncedFilter = document.querySelector('[data-filter="synced"][data-category="overview"]');
        if (syncedFilter) {
            syncedFilter.classList.add('active');
        }
    }
}

function displayTabContent(tabType, contentFilters = []) {
    console.log('Displaying tab content for:', tabType, 'with filters:', Array.from(currentFilters));
    
    let items, contentId;
    
    try {
        switch(tabType) {
            case 'synced':
                items = glossaryData.synced_items.map(item => item.combined);
                contentId = 'synced-content';
                break;
            case 'local':
                items = glossaryData.local_only_items;
                contentId = 'local-content';
                break;
            case 'remote':
                items = glossaryData.remote_only_items;
                contentId = 'remote-content';
                break;
        }
        
        console.log(`Tab ${tabType}: ${items?.length || 0} items`);
        
        // Debug: Log first item to understand data structure
        if (items && items.length > 0) {
            console.log('Sample item data structure:', {
                name: items[0].name,
                entity_type: items[0].entity_type,
                __typename: items[0].__typename,
                type: items[0].type,
                urn: items[0].urn,
                keys: Object.keys(items[0])
            });
        }
        
        // Sort items alphabetically by name (A-Z)
        if (items && items.length > 0) {
            items.sort((a, b) => (a.name || '').toLowerCase().localeCompare((b.name || '').toLowerCase()));
        }
        
        // Apply overview filter only when it matches the current tab context
        // This allows each tab to show its natural data while still allowing overview filters to switch tabs
        if (currentOverviewFilter && currentOverviewFilter !== 'total') {
            // Only apply the filter if it matches the current tab's context
            const shouldApplyFilter = 
                (currentOverviewFilter === 'synced' && tabType === 'synced') ||
                (currentOverviewFilter === 'local-only' && tabType === 'local') ||
                (currentOverviewFilter === 'remote-only' && tabType === 'remote');
                
            if (shouldApplyFilter) {
                items = items.filter(item => {
                    switch (currentOverviewFilter) {
                        case 'synced':
                            return item.sync_status === 'SYNCED';
                        case 'local-only':
                            return item.sync_status === 'LOCAL_ONLY' || item.sync_status === 'MODIFIED';
                        case 'remote-only':
                            return item.sync_status === 'REMOTE_ONLY';
                        default:
                            return true;
                    }
                });
            }
        }
        
        // Filter items based on search
        const searchTerm = currentSearch[tabType];
        if (searchTerm) {
            items = items.filter(item => {
                const entityType = determineEntityType(item);
                return (item.name || '').toLowerCase().includes(searchTerm) ||
                    (item.description || '').toLowerCase().includes(searchTerm) ||
                    (entityType || '').toLowerCase().includes(searchTerm) ||
                    (item.urn || '').toLowerCase().includes(searchTerm) ||
                    (item.parent_name || '').toLowerCase().includes(searchTerm);
            });
        }
        
        // Apply content filters (multi-select)
        if (currentFilters && currentFilters.size > 0) {
            items = items.filter(item => {
                let passesFilter = false;
                
                for (const filter of currentFilters) {
                    switch(filter) {
                        case 'with-owners':
                            if ((item.owners_count || 0) > 0) passesFilter = true;
                            break;
                        case 'with-relationships':
                            if ((item.relationships_count || 0) > 0) passesFilter = true;
                            break;
                        case 'with-properties':
                            if ((item.custom_properties_count || 0) > 0) passesFilter = true;
                            break;
                        case 'with-structured-properties':
                            if ((item.structured_properties_count || 0) > 0) passesFilter = true;
                            break;
                    }
                }
                
                return passesFilter;
            });
        }
        
        const content = document.getElementById(contentId);
        if (!content) {
            console.error('Content element not found:', contentId);
            return;
        }
        
        if (items.length === 0) {
            console.log(`No items for ${tabType}, showing empty state`);
            content.innerHTML = getEmptyStateHTML(tabType, searchTerm);
            return;
        }
        
        // Organize items into hierarchy first
        const hierarchy = organizeHierarchy(items);
        
        // Apply pagination only to root-level items
        const pagination = currentPagination[tabType];
        const totalRootItems = hierarchy.length;
        const totalPages = Math.ceil(totalRootItems / pagination.itemsPerPage);
        const startIndex = (pagination.page - 1) * pagination.itemsPerPage;
        const endIndex = startIndex + pagination.itemsPerPage;
        const paginatedRootItems = hierarchy.slice(startIndex, endIndex);
        
        console.log(`Generating table for ${tabType} with ${totalRootItems} root items (showing ${paginatedRootItems.length} on page ${pagination.page})`);
        
        // Generate table with pagination
        const tableHTML = generateTableHTMLWithPagination(paginatedRootItems, tabType, totalRootItems, pagination.page, totalPages);
        content.innerHTML = tableHTML;
        
        // Attach pagination handlers
        attachPaginationHandlers(content, tabType);
        
        // Attach click handlers for view buttons
        content.querySelectorAll('.view-item').forEach(button => {
            button.addEventListener('click', function() {
                const row = this.closest('tr');
                const itemData = DataUtils.safeJsonParse(row.dataset.item);
                if (itemData) {
                    showItemDetails(itemData);
                } else {
                    console.error('Failed to parse item data from row');
                }
            });
        });
        
        // Attach click handlers for expand/collapse buttons
        content.querySelectorAll('.expand-button').forEach(button => {
            button.addEventListener('click', function(e) {
                e.stopPropagation();
                const icon = this.querySelector('.expand-icon');
                const nodeUrn = icon.id.replace('icon-', '');
                toggleNodeChildren(nodeUrn);
            });
        });
        
        console.log(`Successfully rendered ${tabType} tab`);
        
    } catch (error) {
        console.error(`Error displaying ${tabType} tab:`, error);
        const content = document.getElementById(contentId);
        if (content) {
            content.innerHTML = `<div class="alert alert-danger">Error loading ${tabType} glossary items: ${error.message}</div>`;
        }
    }
}

function generateTableHTMLWithPagination(rootItems, tabType, totalItems, currentPage, totalPages) {
    const showingStart = totalItems > 0 ? ((currentPage - 1) * currentPagination[tabType].itemsPerPage) + 1 : 0;
    const showingEnd = Math.min(currentPage * currentPagination[tabType].itemsPerPage, totalItems);
    
    return `
        <div class="table-responsive">
            <table class="table table-hover table-striped mb-0">
                <thead>
                    <tr>
                        <th class="sortable-header" data-sort="name" width="20%">Name</th>
                        <th width="20%">Description</th>
                        <th class="sortable-header text-center" data-sort="owners_count" width="8%">Owners</th>
                        <th class="sortable-header text-center" data-sort="relationships_count" width="8%">Relationships</th>
                        <th class="sortable-header text-center" data-sort="custom_properties_count" width="8%">Custom<br/>Properties</th>
                        <th class="sortable-header text-center" data-sort="structured_properties_count" width="8%">Structured<br/>Properties</th>
                        <th width="13%">URN</th>
                        <th width="15%">Actions</th>
                    </tr>
                </thead>
                <tbody>
                    ${renderHierarchicalItems(rootItems, tabType, 0)}
                </tbody>
            </table>
        </div>
        ${totalItems > currentPagination[tabType].itemsPerPage ? generatePaginationHTML(currentPage, totalPages, totalItems, showingStart, showingEnd, tabType) : ''}
    `;
}

function generatePaginationHTML(currentPage, totalPages, totalItems, showingStart, showingEnd, tabType) {
    if (totalPages <= 1) return '';
    
    let paginationHTML = `
    <div class="pagination-container d-flex justify-content-between align-items-center">
        <div class="pagination-info">
            Showing ${showingStart} to ${showingEnd} of ${totalItems} root items
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
        const activeClass = i === currentPage ? 'active' : '';
        paginationHTML += `<li class="page-item ${activeClass}"><a class="page-link" href="#" data-page="${i}" data-tab="${tabType}">${i}</a></li>`;
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
                <option value="10" ${currentPagination[tabType].itemsPerPage === 10 ? 'selected' : ''}>10</option>
                <option value="25" ${currentPagination[tabType].itemsPerPage === 25 ? 'selected' : ''}>25</option>
                <option value="50" ${currentPagination[tabType].itemsPerPage === 50 ? 'selected' : ''}>50</option>
                <option value="100" ${currentPagination[tabType].itemsPerPage === 100 ? 'selected' : ''}>100</option>
            </select>
        </div>
    </div>
    `;
    
    return paginationHTML;
}

function attachPaginationHandlers(content, tabType) {
    // Attach pagination click handlers
    content.querySelectorAll('.pagination .page-link').forEach(link => {
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

function organizeHierarchy(items) {
    // Create a map for quick lookup
    const itemMap = new Map();
    const rootItems = [];
    
    // Validate input
    if (!items || !Array.isArray(items)) {
        console.warn('organizeHierarchy: Invalid items input:', items);
        return [];
    }
    
    // First pass: create map of all items
    items.forEach(item => {
        if (item && item.urn) {
            itemMap.set(item.urn, { ...item, children: [] });
        } else {
            console.warn('organizeHierarchy: Item missing URN:', item);
        }
    });
    
    // Second pass: organize into hierarchy
    items.forEach(item => {
        if (!item || !item.urn) return;
        
        try {
            const entityType = determineEntityType(item);
            let parentUrn = null;
            
            // For glossary terms, use parent_node_urn to link to their parent glossary node
            if (entityType === 'glossaryTerm' && item.parent_node_urn) {
                parentUrn = item.parent_node_urn;
            }
            // For glossary nodes, use parent_urn to link to their parent glossary node
            else if (entityType === 'glossaryNode' && item.parent_urn) {
                parentUrn = item.parent_urn;
            }
            
            if (parentUrn && itemMap.has(parentUrn)) {
                // Add to parent's children
                itemMap.get(parentUrn).children.push(itemMap.get(item.urn));
            } else {
                // Root level item (no parent or parent not found)
                rootItems.push(itemMap.get(item.urn));
            }
        } catch (error) {
            console.error('Error organizing item in hierarchy:', item, error);
            // Add to root items as fallback
            if (itemMap.has(item.urn)) {
                rootItems.push(itemMap.get(item.urn));
            }
        }
    });
    
    return rootItems;
}

function renderHierarchicalItems(items, tabType, level) {
    return items.map(item => {
        const hasChildren = item.children && item.children.length > 0;
        let html = renderGlossaryRow(item, tabType, level, hasChildren);
        
        if (hasChildren) {
            // Add children rows (initially hidden for collapsed state)
            html += renderHierarchicalItems(item.children, tabType, level + 1);
        }
        
        return html;
    }).join('');
}

function determineEntityType(item) {
    // First, check if entity_type is already set correctly
    if (item.entity_type === 'glossaryTerm' || item.entity_type === 'glossaryNode') {
        return item.entity_type;
    }
    
    // Check __typename from GraphQL
    if (item.__typename) {
        if (item.__typename === 'GlossaryTerm') return 'glossaryTerm';
        if (item.__typename === 'GlossaryNode') return 'glossaryNode';
    }
    
    // Check URN pattern
    if (item.urn) {
        if (item.urn.includes(':glossaryTerm:')) return 'glossaryTerm';
        if (item.urn.includes(':glossaryNode:')) return 'glossaryNode';
    }
    
    // Check type field
    if (item.type) {
        if (item.type.toLowerCase().includes('term')) return 'glossaryTerm';
        if (item.type.toLowerCase().includes('node')) return 'glossaryNode';
    }
    
    // Check if it has definition (terms usually have definitions, nodes don't)
    if (item.definition || item.termSource) return 'glossaryTerm';
    
    // Check if it has children (nodes usually have children, terms don't)
    if (item.children && item.children.length > 0) return 'glossaryNode';
    
    // Default fallback
    console.warn('Could not determine entity type for item:', item);
    return 'unknown';
}

function renderGlossaryRow(item, tabType, level = 0, hasChildren = false) {
    const statusClass = getStatusBadgeClass(item.sync_status);
    
    // Determine the actual entity type using multiple methods
    const entityType = determineEntityType(item);
    
    // Determine icon and type display based on determined entity type
    let typeIcon, typeDisplay, typeBadgeClass;
    if (entityType === 'glossaryTerm') {
        typeIcon = 'fas fa-bookmark text-success';  // Bookmark icon for terms
        typeDisplay = 'Term';
        typeBadgeClass = 'success';
    } else if (entityType === 'glossaryNode') {
        // Use folder icon for nodes - closed if has children, open if expanded
        typeIcon = hasChildren ? 'fas fa-folder text-primary' : 'fas fa-folder-open text-primary';
        typeDisplay = 'Node';
        typeBadgeClass = 'primary';
    } else {
        typeIcon = 'fas fa-question-circle text-warning';
        typeDisplay = 'Unknown';
        typeBadgeClass = 'warning';
        console.warn('Unknown entity type for item:', item);
    }
    
    // Create indentation for hierarchy
    const indent = level * 20;
    
    // Determine the correct parent URN for data attributes
    let parentUrn = null;
    if (entityType === 'glossaryTerm' && item.parent_node_urn) {
        parentUrn = item.parent_node_urn;
    } else if (entityType === 'glossaryNode' && item.parent_urn) {
        parentUrn = item.parent_urn;
    }
    
    // Create a safe version of the item for JSON.stringify using global utilities
    const safeItem = DataUtils.createDisplaySafeItem(item, {
        descriptionLength: 200,
        nameLength: 100,
        urnLength: 500
    });
    
    return `
        <tr data-item='${DataUtils.safeJsonStringify(safeItem)}' data-level="${level}" data-node-urn="${item.urn}" ${parentUrn ? `data-parent-node="${parentUrn}"` : ''} ${level > 0 ? 'style="display: none;"' : ''}>
            <td>
                <div class="hierarchy-container" style="margin-left: ${indent}px;">
                    ${level > 0 ? '<div class="tree-connector"></div>' : ''}
                    <div class="expand-button-container">
                        ${hasChildren ? `
                            <button class="expand-button" type="button" title="Expand/Collapse">
                                <i class="fas fa-chevron-right expand-icon" id="icon-${item.urn}"></i>
                            </button>
                        ` : ''}
                    </div>
                    <i class="${typeIcon} me-2" style="font-size: 1.1em;"></i>
                    <span class="item-name">${escapeHtml(item.name || 'Unnamed')}</span>
                </div>
            </td>
            <td class="description-cell">
                <div class="description-preview" title="${escapeHtml(item.description || '')}">${escapeHtml(truncateText(item.description || '', 150))}</div>
            </td>
            <td class="text-center">
                ${(item.owners_count || 0) > 0 ? `<i class="fas fa-users text-info me-1"></i><span class="badge bg-info">${item.owners_count}</span>` : '<span class="text-muted">None</span>'}
            </td>
            <td class="text-center">
                ${entityType === 'glossaryTerm' ? 
                    ((item.relationships_count || 0) > 0 ? `<i class="fas fa-link text-success me-1"></i><span class="badge bg-success">${item.relationships_count}</span>` : '<span class="text-muted">None</span>') :
                    '<span class="text-muted">N/A</span>'
                }
            </td>
            <td class="text-center">
                <span class="badge bg-secondary">${item.custom_properties_count || 0}</span>
            </td>
            <td class="text-center">
                <span class="badge bg-success">${item.structured_properties_count || 0}</span>
            </td>
            <td>
                <code class="small" title="${escapeHtml(item.urn || '')}">${escapeHtml(truncateUrnFromEnd(item.urn || '', 30))}</code>
            </td>
            <td>
                <div class="btn-group btn-group-sm" role="group">
                    <button type="button" class="btn btn-outline-primary view-item" 
                            title="View Details">
                        <i class="fas fa-eye"></i>
                    </button>
                    <button type="button" class="btn btn-outline-secondary" 
                            title="Edit">
                        <i class="fas fa-edit"></i>
                    </button>
                    ${item.urn && !item.urn.includes('local:') ? `
                        <a href="${getDataHubUrl(item.urn, entityType)}" 
                           class="btn btn-outline-info" 
                           target="_blank" title="View in DataHub">
                            <i class="fas fa-external-link-alt"></i>
                        </a>
                    ` : ''}
                </div>
            </td>
        </tr>
    `;
}

function getActionButtons(item, tabType) {
    const hasDataHubConnection = true; // This should come from the template context
    let buttons = '';
    
    if (tabType === 'synced' && item.sync_status === 'MODIFIED' && hasDataHubConnection) {
        const deployUrl = item.type === 'node' ? 
            `/metadata/glossary/nodes/${item.id}/deploy/` : 
            `/metadata/glossary/terms/${item.id}/deploy/`;
        buttons += `
            <form action="${deployUrl}" method="post" class="d-inline">
                <input type="hidden" name="csrfmiddlewaretoken" value="${getCsrfToken()}">
                <button type="submit" class="btn btn-sm btn-outline-warning" title="Update in DataHub">
                    <i class="fas fa-sync-alt"></i>
                </button>
            </form>
        `;
    }
    
    if (tabType === 'local' && hasDataHubConnection) {
        const deployUrl = item.type === 'node' ? 
            `/metadata/glossary/nodes/${item.id}/deploy/` : 
            `/metadata/glossary/terms/${item.id}/deploy/`;
        buttons += `
            <form action="${deployUrl}" method="post" class="d-inline">
                <input type="hidden" name="csrfmiddlewaretoken" value="${getCsrfToken()}">
                <button type="submit" class="btn btn-sm btn-outline-success" title="Deploy to DataHub">
                    <i class="fas fa-cloud-upload-alt"></i>
                </button>
            </form>
        `;
    }
    
    if (tabType === 'remote') {
        buttons += `
            <form action="/metadata/glossary/pull/" method="post" class="d-inline">
                <input type="hidden" name="csrfmiddlewaretoken" value="${getCsrfToken()}">
                <input type="hidden" name="${item.type}_urns" value="${item.urn}">
                <button type="submit" class="btn btn-sm btn-outline-info" title="Import to Local">
                    <i class="fas fa-download"></i>
                </button>
            </form>
        `;
    }
    
    return buttons;
}

function getEmptyStateHTML(tabType, hasSearch) {
    if (hasSearch) {
        return `
            <div class="py-5 text-center">
                <div class="mb-3">
                    <i class="fas fa-search fa-4x text-muted"></i>
                </div>
                <h4>No items found</h4>
                <p class="text-muted">No items match your search criteria.</p>
            </div>
        `;
    }
    
    const emptyStates = {
        synced: {
            icon: 'fas fa-sync-alt',
            title: 'No synced items',
            description: 'Items that exist both locally and in DataHub will appear here.'
        },
        local: {
            icon: 'fas fa-laptop',
            title: 'No local-only items',
            description: 'Items that exist only in this application will appear here.',
            action: `<div class="mt-2">
                        <a href="/metadata/glossary/nodes/create/" class="btn btn-primary me-2">
                            <i class="fas fa-folder-plus me-1"></i> Create Node
                        </a>
                        <a href="/metadata/glossary/terms/create/" class="btn btn-success">
                            <i class="fas fa-tag me-1"></i> Create Term
                        </a>
                     </div>`
        },
        remote: {
            icon: 'fas fa-server',
            title: 'No remote-only items',
            description: 'Items that exist only in DataHub will appear here.'
        }
    };
    
    const state = emptyStates[tabType];
    return `
        <div class="py-5 text-center">
            <div class="mb-3">
                <i class="${state.icon} fa-4x text-muted"></i>
            </div>
            <h4>${state.title}</h4>
            <p class="text-muted">${state.description}</p>
            ${state.action || ''}
        </div>
    `;
}



function showItemDetails(item) {
    // Determine the actual entity type first
    const entityType = determineEntityType(item);
    
    // Extract the correct data based on entity type
    let itemData = item;
    
    // If this is a combined item (synced), get the appropriate data
    if (item.combined) {
        itemData = item.combined;
    } else if (item.remote && item.local) {
        // For synced items, prefer remote data for comprehensive info
        itemData = item.remote;
    }
    
    // For remote-only items, check if we have the raw GraphQL data
    if (itemData.raw_data) {
        // Use the raw GraphQL data which has the proper structure
        const rawData = itemData.raw_data;
        if (entityType === 'glossaryTerm' && rawData.glossaryTerm) {
            itemData = { ...itemData, ...rawData.glossaryTerm };
        } else if (entityType === 'glossaryNode' && rawData.glossaryNode) {
            itemData = { ...itemData, ...rawData.glossaryNode };
        }
    }
    
    // Basic information
    document.getElementById('modal-item-name').textContent = itemData.name || itemData.properties?.name || 'Unnamed';
    
    // Set type with icon
    const typeElement = document.getElementById('modal-item-type');
    if (entityType === 'glossaryTerm') {
        typeElement.innerHTML = '<i class="fas fa-bookmark text-success me-1"></i>Glossary Term';
    } else if (entityType === 'glossaryNode') {
        typeElement.innerHTML = '<i class="fas fa-folder-open text-primary me-1"></i>Glossary Node';
    } else {
        typeElement.innerHTML = `<i class="fas fa-question-circle text-warning me-1"></i>Unknown Type`;
    }
    
    document.getElementById('modal-item-description').textContent = itemData.description || itemData.properties?.description || 'No description available';
    document.getElementById('modal-item-urn').textContent = itemData.urn || 'No URN available';
    
    // Parent information (for terms and child nodes)
    const parentLabel = document.getElementById('modal-parent-label');
    const parentValue = document.getElementById('modal-parent-value');
    const parentElement = document.getElementById('modal-item-parent');
    
    let parentUrn = null;
    if (entityType === 'glossaryTerm') {
        parentUrn = itemData.parent_node_urn || itemData.parentNodes?.nodes?.[0]?.urn;
    } else if (entityType === 'glossaryNode') {
        parentUrn = itemData.parent_urn || itemData.parentNodes?.nodes?.[0]?.urn;
    }
    
    if (parentUrn) {
        parentElement.textContent = parentUrn;
        parentLabel.style.display = 'block';
        parentValue.style.display = 'block';
    } else {
        parentLabel.style.display = 'none';
        parentValue.style.display = 'none';
    }
    
    // Status
    const statusBadge = document.getElementById('modal-item-status');
    statusBadge.textContent = itemData.sync_status_display || itemData.sync_status || item.sync_status_display || item.sync_status;
    statusBadge.className = `badge ${getStatusBadgeClass(itemData.sync_status || item.sync_status)}`;
    
    // DataHub link
    const datahubLink = document.getElementById('modal-datahub-link');
    if (itemData.urn && !itemData.urn.includes('local:') && glossaryData.datahub_url) {
        datahubLink.href = getDataHubUrl(itemData.urn, entityType);
        datahubLink.style.display = 'inline-block';
    } else {
        datahubLink.style.display = 'none';
    }
    
    // Ownership information - similar to tags page
    const ownersList = document.getElementById('modal-owners-list');
    
    // Load users/groups cache if needed
    if (usersAndGroupsCache.users.length === 0) {
        loadUsersAndGroups();
    }
    
    // Check for ownership data from the comprehensive GraphQL query
    const ownershipData = itemData.ownership || item.ownership;
    
    if (ownershipData && ownershipData.length > 0) {
        // Group owners by ownership type - using the same format as tags view
        const ownersByType = {};
        
        ownershipData.forEach(ownerInfo => {
            let ownerUrn, ownershipTypeUrn, ownershipTypeName;
            
            // Handle the processed ownership data structure from backend
            if (ownerInfo.urn && ownerInfo.ownershipType) {
                // Backend processed format
                ownerUrn = ownerInfo.urn;
                ownershipTypeUrn = ownerInfo.ownershipType.urn;
                ownershipTypeName = ownerInfo.ownershipType.name || 'Unknown Type';
            } else if (ownerInfo.owner && ownerInfo.ownershipType) {
                // GraphQL format
                ownerUrn = ownerInfo.owner.urn;
                ownershipTypeUrn = ownerInfo.ownershipType.urn;
                ownershipTypeName = ownerInfo.ownershipType.info?.name || 'Unknown Type';
            } else {
                return; // Skip invalid entries
            }
            
            // Find the owner name using the same logic as tags view
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
                // Backend processed format or need to look up in cache
                if (ownerInfo.displayName || ownerInfo.name) {
                    ownerName = ownerInfo.displayName || ownerInfo.name;
                    isUser = ownerUrn?.includes('corpuser');
                } else {
                    // Look up in cache
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
        
        // Generate HTML for owners grouped by type - using the same format as tags view
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
        
        ownersList.innerHTML = ownersHTML;
    } else {
        ownersList.innerHTML = '<p class="text-muted">No ownership information available</p>';
    }
    
    // Relationships - Show existing relationships or load on demand for glossary terms
    const relationshipsList = document.getElementById('modal-relationships-list');
    
    // Check if we already have relationships data from the backend
    if ((itemData.relationships && itemData.relationships.length > 0) || (item.relationships && item.relationships.length > 0)) {
        const relationshipsData = itemData.relationships || item.relationships;
        // Display existing relationships data
        let relationshipsHTML = '';
        
        // Group relationships by type
        const relationshipsByType = {};
        relationshipsData.forEach(rel => {
            const relType = rel.type || 'Related';
            if (!relationshipsByType[relType]) {
                relationshipsByType[relType] = [];
            }
            relationshipsByType[relType].push(rel);
        });
        
        // Display each relationship type
        Object.keys(relationshipsByType).forEach(relType => {
            const rels = relationshipsByType[relType];
            const typeIcon = relType === 'isA' ? 'fas fa-arrow-up' : 
                           relType === 'hasA' ? 'fas fa-arrow-down' : 
                           'fas fa-link';
            const typeColor = relType === 'isA' ? 'text-primary' : 
                            relType === 'hasA' ? 'text-success' : 
                            'text-info';
            
            relationshipsHTML += `
                <div class="mb-3">
                    <h6 class="${typeColor}"><i class="${typeIcon} me-1"></i>${relType} (${rels.length})</h6>
                    ${rels.map(rel => `
                        <div class="border rounded p-2 mb-2">
                            <div class="d-flex justify-content-between align-items-start">
                                <div>
                                    <strong>${escapeHtml(rel.entity?.name || 'Unknown Entity')}</strong>
                                    ${rel.entity?.type ? `<span class="badge bg-secondary ms-2">${rel.entity.type}</span>` : ''}
                                </div>
                            </div>
                            <div class="mt-1">
                                <code class="small">${escapeHtml(rel.entity?.urn || 'No URN')}</code>
                            </div>
                        </div>
                    `).join('')}
                </div>
            `;
        });
        
        relationshipsList.innerHTML = relationshipsHTML;
    } else {
        // No relationships data available from backend
        relationshipsList.innerHTML = '<p class="text-muted">No relationships available</p>';
    }
    
    // Properties
    const propertiesDiv = document.getElementById('modal-glossary-properties');
    let propertiesHTML = '';
    
    // Custom Properties
    if ((itemData.custom_properties && itemData.custom_properties.length > 0) || (item.custom_properties && item.custom_properties.length > 0)) {
        const customProperties = itemData.custom_properties || item.custom_properties;
        propertiesHTML += `
            <div class="border rounded p-2 mb-2">
                <strong>Custom Properties:</strong>
                <div class="mt-2">
                    ${customProperties.map(prop => `
                        <div class="mb-1">
                            <strong>${escapeHtml(prop.key || 'Unknown')}:</strong> ${escapeHtml(prop.value || '')}
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }
    
    // Structured Properties
    if ((itemData.structured_properties && itemData.structured_properties.length > 0) || (item.structured_properties && item.structured_properties.length > 0)) {
        const structuredProperties = itemData.structured_properties || item.structured_properties;
        propertiesHTML += `
            <div class="border rounded p-2 mb-2">
                <strong>Structured Properties:</strong>
                <div class="mt-2">
                    ${structuredProperties.map(prop => `
                        <div class="mb-1">
                            <strong>${escapeHtml(prop.propertyUrn || 'Unknown')}:</strong> ${escapeHtml(JSON.stringify(prop.value) || '')}
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }
    
    if (propertiesHTML) {
        propertiesDiv.innerHTML = propertiesHTML;
    } else {
        propertiesDiv.innerHTML = '<p class="text-muted">No additional properties available</p>';
    }
    
    // Raw JSON
    const rawData = itemData;
    document.getElementById('modal-raw-json').innerHTML = `<code>${escapeHtml(JSON.stringify(rawData, null, 2))}</code>`;
    
    // Show modal
    new bootstrap.Modal(document.getElementById('itemViewModal')).show();
}

// Utility functions - using global utilities for consistency
function escapeHtml(text) {
    return DataUtils.safeEscapeHtml(text);
}

function truncateText(text, maxLength) {
    return DataUtils.safeTruncateText(text, maxLength);
}

function truncateUrnFromEnd(urn, maxLength) {
    if (!urn || urn.length <= maxLength) return urn;
    
    // Keep the beginning of the URN and truncate from the end
    return urn.substring(0, maxLength - 3) + '...';
}

function formatDate(timestamp) {
    try {
        const date = new Date(parseInt(timestamp));
        return date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
    } catch (e) {
        return timestamp;
    }
}

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
            connectionCache.users = data.data.users || [];
            connectionCache.groups = data.data.groups || [];
            connectionCache.ownership_types = data.data.ownership_types || [];
            connectionCache.lastFetched = now;
            
            console.log(`Loaded ${connectionCache.users.length} users, ${connectionCache.groups.length} groups, and ${connectionCache.ownership_types.length} ownership types for connection ${currentConnectionId}${data.cached ? ' (cached)' : ''}`);
        } else {
            console.error('Failed to load users, groups, and ownership types:', data.error);
        }
    } catch (error) {
        console.error('Error loading users and groups:', error);
    }
}

function getDataHubUrl(urn, type) {
    if (glossaryData.datahub_url && urn && !urn.includes('local:')) {
        // Ensure no double slashes and use correct DataHub URL format
        const baseUrl = glossaryData.datahub_url.replace(/\/+$/, ''); // Remove trailing slashes
        
        // Determine the correct entity type for the URL
        let entityType;
        if (type === 'glossaryTerm' || type === 'term') {
            entityType = 'glossaryTerm';
        } else if (type === 'glossaryNode' || type === 'node') {
            entityType = 'glossaryNode';
        } else {
            // Default based on URN pattern if type is unclear
            entityType = urn.includes('glossaryTerm') ? 'glossaryTerm' : 'glossaryNode';
        }
        
        // DataHub expects the full URN including the urn:li: prefix
        return `${baseUrl}/${entityType}/${urn}`;
    }
    return '#';
}

function getStatusBadgeClass(status) {
    switch(status) {
        case 'SYNCED': return 'bg-success';
        case 'MODIFIED': return 'bg-warning';
        case 'LOCAL_ONLY': return 'bg-secondary';
        case 'REMOTE_ONLY': return 'bg-info';
        default: return 'bg-primary';
    }
}

function getCsrfToken() {
    const csrfElement = document.querySelector('[name=csrfmiddlewaretoken]');
    return csrfElement ? csrfElement.value : '';
}

function showError(message) {
    // Create and show error alert
    const alert = document.createElement('div');
    alert.className = 'alert alert-danger alert-dismissible fade show';
    alert.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    document.querySelector('.container-fluid').insertBefore(alert, document.querySelector('.row'));
}

// Expand/collapse functionality
function toggleNodeChildren(nodeUrn) {
    console.log('Toggling children for node:', nodeUrn);
    
    const expandIcon = document.getElementById(`icon-${nodeUrn}`);
    const childRows = document.querySelectorAll(`[data-parent-node="${nodeUrn}"]`);
    
    if (!expandIcon) {
        console.error('Expand icon not found for node:', nodeUrn);
        return;
    }
    
    const isCollapsed = expandIcon.classList.contains('fa-chevron-right');
    
    if (isCollapsed) {
        // Expand: show direct children only
        expandIcon.classList.remove('fa-chevron-right');
        expandIcon.classList.add('fa-chevron-down');
        childRows.forEach(row => {
            row.style.display = 'table-row';
        });
    } else {
        // Collapse: hide all descendants recursively
        expandIcon.classList.remove('fa-chevron-down');
        expandIcon.classList.add('fa-chevron-right');
        
        // Recursively collapse all descendants
        collapseAllDescendants(nodeUrn);
    }
}

function collapseAllDescendants(parentUrn) {
    // Find all direct children
    const childRows = document.querySelectorAll(`[data-parent-node="${parentUrn}"]`);
    
    childRows.forEach(row => {
        // Hide the row
        row.style.display = 'none';
        
        // Get the URN of this child
        const childUrn = row.dataset.nodeUrn;
        
        // Collapse the expand icon if it has one
        const childExpandIcon = document.getElementById(`icon-${childUrn}`);
        if (childExpandIcon) {
            childExpandIcon.classList.remove('fa-chevron-down');
            childExpandIcon.classList.add('fa-chevron-right');
        }
        
        // Recursively collapse this child's descendants
        if (childUrn) {
            collapseAllDescendants(childUrn);
        }
    });
}

// Expand all nodes
function expandAllNodes() {
    const allExpandIcons = document.querySelectorAll('.expand-icon');
    allExpandIcons.forEach(icon => {
        if (icon.classList.contains('fa-chevron-right')) {
            const nodeUrn = icon.id.replace('icon-', '');
            // Only expand, don't toggle
            icon.classList.remove('fa-chevron-right');
            icon.classList.add('fa-chevron-down');
            const childRows = document.querySelectorAll(`[data-parent-node="${nodeUrn}"]`);
            childRows.forEach(row => {
                row.style.display = 'table-row';
            });
        }
    });
}

// Collapse all nodes
function collapseAllNodes() {
    const allExpandIcons = document.querySelectorAll('.expand-icon');
    allExpandIcons.forEach(icon => {
        if (icon.classList.contains('fa-chevron-down')) {
            const nodeUrn = icon.id.replace('icon-', '');
            icon.classList.remove('fa-chevron-down');
            icon.classList.add('fa-chevron-right');
            // Use the recursive collapse function
            collapseAllDescendants(nodeUrn);
        }
    });
}

// CSV Download/Upload Functions
function downloadCSV() {
    // Get current glossary data
    const allItems = [
        ...(glossaryData.synced_items || []).map(item => item.combined || item),
        ...(glossaryData.local_only_items || []),
        ...(glossaryData.remote_only_items || [])
    ];
    
    if (allItems.length === 0) {
        alert('No glossary items to export');
        return;
    }
    
    // Convert to CSV format
    const csvData = convertToCSV(allItems);
    
    // Create and download file
    const blob = new Blob([csvData], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', `glossary_export_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

function downloadCSVTemplate() {
    // Create a template CSV with example data
    const templateData = [
        {
            name: 'Customer',
            type: 'term',
            description: 'A person or organization that purchases goods or services',
            parent: 'Business Entities',
            owner_email: 'data-owner@company.com',
            owner_group: '',
            relationships: JSON.stringify([
                { type: 'isA', entity: 'Person' },
                { type: 'hasA', entity: 'Account' }
            ]),
            custom_properties: JSON.stringify({
                'data_classification': 'PII',
                'retention_period': '7 years'
            })
        },
        {
            name: 'Business Entities',
            type: 'node',
            description: 'Top-level category for business-related terms',
            parent: '',
            owner_email: '',
            owner_group: 'Data Governance Team',
            relationships: '[]',
            custom_properties: '{}'
        }
    ];
    
    const csvData = convertToCSV(templateData);
    
    // Create and download file
    const blob = new Blob([csvData], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', 'glossary_template.csv');
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

function convertToCSV(items) {
    const headers = [
        'name',
        'type',
        'description',
        'parent',
        'owner_email',
        'owner_group',
        'relationships',
        'custom_properties'
    ];
    
    const csvRows = [headers.join(',')];
    
    items.forEach(item => {
        // Determine type
        let type = 'term';
        if (item.entity_type === 'glossaryNode' || item.urn?.includes('glossaryNode')) {
            type = 'node';
        }
        
        // Get parent information
        let parent = '';
        if (item.parent_node_urn || item.parent_urn) {
            parent = item.parent_node_urn || item.parent_urn;
        } else if (item.parentNodes?.nodes?.[0]?.urn) {
            parent = item.parentNodes.nodes[0].urn;
        }
        
        // Get owner information
        let ownerEmail = '';
        let ownerGroup = '';
        if (item.ownership && item.ownership.length > 0) {
            const owners = item.ownership;
            const userOwner = owners.find(o => o.urn?.includes('corpuser') || o.owner?.urn?.includes('corpuser'));
            const groupOwner = owners.find(o => o.urn?.includes('corpgroup') || o.owner?.urn?.includes('corpgroup'));
            
            if (userOwner) {
                ownerEmail = userOwner.displayName || userOwner.name || userOwner.urn || '';
            }
            if (groupOwner) {
                ownerGroup = groupOwner.displayName || groupOwner.name || groupOwner.urn || '';
            }
        }
        
        // Get relationships
        let relationships = '[]';
        if (item.relationships && item.relationships.length > 0) {
            relationships = JSON.stringify(item.relationships.map(rel => ({
                type: rel.type,
                entity: rel.entity?.name || rel.entity?.urn || 'Unknown'
            })));
        }
        
        // Get custom properties
        let customProperties = '{}';
        if (item.custom_properties && item.custom_properties.length > 0) {
            const props = {};
            item.custom_properties.forEach(prop => {
                props[prop.key] = prop.value;
            });
            customProperties = JSON.stringify(props);
        }
        
        const row = [
            escapeCSVField(item.name || ''),
            escapeCSVField(type),
            escapeCSVField(item.description || ''),
            escapeCSVField(parent),
            escapeCSVField(ownerEmail),
            escapeCSVField(ownerGroup),
            escapeCSVField(relationships),
            escapeCSVField(customProperties)
        ];
        
        csvRows.push(row.join(','));
    });
    
    return csvRows.join('\n');
}

function escapeCSVField(field) {
    if (field === null || field === undefined) {
        return '';
    }
    
    const stringField = String(field);
    
    // If field contains comma, newline, or quote, wrap in quotes and escape internal quotes
    if (stringField.includes(',') || stringField.includes('\n') || stringField.includes('"')) {
        return '"' + stringField.replace(/"/g, '""') + '"';
    }
    
    return stringField;
}

function showUploadCSVModal() {
    const modal = new bootstrap.Modal(document.getElementById('csvUploadModal'));
    modal.show();
    
    // Reset form
    document.getElementById('csvUploadForm').reset();
    document.getElementById('uploadProgress').classList.add('d-none');
    document.getElementById('uploadResults').classList.add('d-none');
    document.getElementById('uploadCSVBtn').disabled = false;
}

async function uploadCSV() {
    const fileInput = document.getElementById('csvFile');
    const updateExisting = document.getElementById('updateExisting').checked;
    const dryRun = document.getElementById('dryRun').checked;
    
    if (!fileInput.files[0]) {
        alert('Please select a CSV file');
        return;
    }
    
    const file = fileInput.files[0];
    
    // Validate file size (10MB limit)
    if (file.size > 10 * 1024 * 1024) {
        alert('File size must be less than 10MB');
        return;
    }
    
    // Validate file type
    if (!file.name.toLowerCase().endsWith('.csv')) {
        alert('Please select a CSV file');
        return;
    }
    
    // Show progress
    document.getElementById('uploadProgress').classList.remove('d-none');
    document.getElementById('uploadResults').classList.add('d-none');
    document.getElementById('uploadCSVBtn').disabled = true;
    
    const progressBar = document.querySelector('#uploadProgress .progress-bar');
    const statusDiv = document.getElementById('uploadStatus');
    
    try {
        // Create FormData
        const formData = new FormData();
        formData.append('csv_file', file);
        formData.append('update_existing', updateExisting);
        formData.append('dry_run', dryRun);
        formData.append('csrfmiddlewaretoken', getCsrfToken());
        
        statusDiv.textContent = 'Uploading file...';
        progressBar.style.width = '25%';
        
        // Upload file
        const response = await fetch('/metadata/glossary/csv-upload/', {
            method: 'POST',
            body: formData
        });
        
        statusDiv.textContent = 'Processing CSV...';
        progressBar.style.width = '75%';
        
        const result = await response.json();
        
        progressBar.style.width = '100%';
        statusDiv.textContent = 'Complete!';
        
        // Show results
        setTimeout(() => {
            document.getElementById('uploadProgress').classList.add('d-none');
            document.getElementById('uploadResults').classList.remove('d-none');
            
            const resultsContent = document.getElementById('uploadResultsContent');
            
            if (result.success) {
                let html = '<div class="alert alert-success">';
                html += `<h6><i class="fas fa-check-circle me-1"></i>Upload ${dryRun ? 'Preview' : 'Completed'} Successfully</h6>`;
                html += `<p><strong>Processed:</strong> ${result.data.processed_count} items</p>`;
                
                if (result.data.created_count > 0) {
                    html += `<p><strong>Created:</strong> ${result.data.created_count} items</p>`;
                }
                if (result.data.updated_count > 0) {
                    html += `<p><strong>Updated:</strong> ${result.data.updated_count} items</p>`;
                }
                if (result.data.skipped_count > 0) {
                    html += `<p><strong>Skipped:</strong> ${result.data.skipped_count} items</p>`;
                }
                
                html += '</div>';
                
                if (result.data.errors && result.data.errors.length > 0) {
                    html += '<div class="alert alert-warning">';
                    html += '<h6><i class="fas fa-exclamation-triangle me-1"></i>Warnings</h6>';
                    html += '<ul class="mb-0">';
                    result.data.errors.forEach(error => {
                        html += `<li>${escapeHtml(error)}</li>`;
                    });
                    html += '</ul></div>';
                }
                
                if (!dryRun) {
                    html += '<div class="mt-3">';
                    html += '<button type="button" class="btn btn-primary" onclick="loadGlossaryData(); bootstrap.Modal.getInstance(document.getElementById(\'csvUploadModal\')).hide();">';
                    html += '<i class="fas fa-sync-alt me-1"></i>Refresh Data</button>';
                    html += '</div>';
                }
                
                resultsContent.innerHTML = html;
            } else {
                resultsContent.innerHTML = `
                    <div class="alert alert-danger">
                        <h6><i class="fas fa-exclamation-circle me-1"></i>Upload Failed</h6>
                        <p>${escapeHtml(result.error || 'Unknown error occurred')}</p>
                    </div>
                `;
            }
        }, 500);
        
    } catch (error) {
        console.error('Upload error:', error);
        document.getElementById('uploadProgress').classList.add('d-none');
        document.getElementById('uploadResults').classList.remove('d-none');
        document.getElementById('uploadResultsContent').innerHTML = `
            <div class="alert alert-danger">
                <h6><i class="fas fa-exclamation-circle me-1"></i>Upload Error</h6>
                <p>An error occurred while uploading the file: ${escapeHtml(error.message)}</p>
            </div>
        `;
    } finally {
        document.getElementById('uploadCSVBtn').disabled = false;
    }
} 