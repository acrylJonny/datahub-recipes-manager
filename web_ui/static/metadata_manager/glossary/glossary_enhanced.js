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
    
    safeJsonStringify: function(obj, maxLength = 5000) {
        try {
            // Handle circular references and special characters
            const seen = new WeakSet();
            let jsonString = JSON.stringify(obj, (key, value) => {
                if (typeof value === 'object' && value !== null) {
                    if (seen.has(value)) {
                        return '[Circular Reference]';
                    }
                    seen.add(value);
                }
                // Convert undefined values to null for valid JSON
                return value === undefined ? null : value;
            }, 2);
            
            // If the string is too long, we need to truncate safely
            if (jsonString.length > maxLength) {
                // Try to create a simplified version by removing large arrays/objects
                const simplified = this.simplifyObjectForJson(obj, maxLength);
                jsonString = JSON.stringify(simplified, null, 2);
                
                // If still too long, truncate at the last complete JSON structure
                if (jsonString.length > maxLength) {
                    const truncated = jsonString.substring(0, maxLength);
                    // Find the last complete JSON structure by looking for the last closing brace/bracket
                    let lastValidIndex = truncated.lastIndexOf('}');
                    const lastBracket = truncated.lastIndexOf(']');
                    if (lastBracket > lastValidIndex) {
                        lastValidIndex = lastBracket;
                    }
                    
                    if (lastValidIndex > 0) {
                        jsonString = truncated.substring(0, lastValidIndex + 1);
                    } else {
                        // Fallback to a minimal valid JSON object
                        jsonString = '{"truncated": true, "original_size": ' + jsonString.length + '}';
                    }
                }
            }
            
            return jsonString;
        } catch (error) {
            console.error('Error stringifying object:', error);
            return '{"error": "Failed to stringify object"}';
        }
    },
    
    // Helper function to simplify objects for JSON storage
    simplifyObjectForJson: function(obj, maxLength) {
        if (typeof obj !== 'object' || obj === null) {
            return obj;
        }
        
        const simplified = {};
        
        // Keep essential fields
        const essentialFields = ['id', 'name', 'urn', 'type', 'sync_status', 'description'];
        essentialFields.forEach(field => {
            if (obj.hasOwnProperty(field)) {
                simplified[field] = obj[field];
            }
        });
        
        // Add other fields but truncate large arrays/objects
        Object.keys(obj).forEach(key => {
            if (!essentialFields.includes(key)) {
                const value = obj[key];
                if (Array.isArray(value)) {
                    // Limit arrays to first few items
                    simplified[key] = value.length > 3 ? value.slice(0, 3).concat([`... ${value.length - 3} more items`]) : value;
                } else if (typeof value === 'object' && value !== null) {
                    // Simplify nested objects
                    simplified[key] = this.simplifyObjectForJson(value, 100);
                } else if (typeof value === 'string' && value.length > 200) {
                    // Truncate long strings
                    simplified[key] = value.substring(0, 200) + '...';
                } else {
                    simplified[key] = value;
                }
            }
        });
        
        return simplified;
    },
    
    safeJsonParse: function(jsonString) {
        try {
            if (!jsonString || typeof jsonString !== 'string') {
                console.error('Invalid JSON string:', jsonString);
                return {};
            }
            
            // Try to parse directly first
            return JSON.parse(jsonString);
            
        } catch (error) {
            console.error('Error parsing JSON:', error);
            console.error('JSON string length:', jsonString ? jsonString.length : 'undefined');
            
            if (error instanceof SyntaxError && error.message.includes('position')) {
                const match = error.message.match(/position (\d+)/);
                if (match && match[1]) {
                    const position = parseInt(match[1]);
                    const start = Math.max(0, position - 50);
                    const end = Math.min(jsonString.length, position + 50);
                    console.error(`JSON error near position ${position}:`, 
                        jsonString.substring(start, position) + '→HERE→' + jsonString.substring(position, end));
                }
            }
            
            // Try to fix common JSON issues and re-parse
            try {
                let fixedString = jsonString;
                
                // Remove any trailing incomplete strings or objects
                const lastValidBrace = fixedString.lastIndexOf('}');
                const lastValidBracket = fixedString.lastIndexOf(']');
                const lastValidIndex = Math.max(lastValidBrace, lastValidBracket);
                
                if (lastValidIndex > 0 && lastValidIndex < fixedString.length - 10) {
                    console.log('Attempting to fix truncated JSON by cutting at last valid structure');
                    fixedString = fixedString.substring(0, lastValidIndex + 1);
                    return JSON.parse(fixedString);
                }
                
                // If that doesn't work, try to extract just the essential data
                console.log('Attempting to extract essential data from malformed JSON');
                const nameMatch = jsonString.match(/"name"\s*:\s*"([^"]+)"/);
                const urnMatch = jsonString.match(/"urn"\s*:\s*"([^"]+)"/);
                const typeMatch = jsonString.match(/"type"\s*:\s*"([^"]+)"/);
                const idMatch = jsonString.match(/"id"\s*:\s*"?([^",}]+)"?/);
                
                if (nameMatch || urnMatch || idMatch) {
                    return {
                        name: nameMatch ? nameMatch[1] : 'Unknown',
                        urn: urnMatch ? urnMatch[1] : '',
                        type: typeMatch ? typeMatch[1] : 'unknown',
                        id: idMatch ? idMatch[1] : '',
                        _parsed_from_malformed: true
                    };
                }
                
            } catch (fixError) {
                console.error('Failed to fix JSON:', fixError);
            }
            
            // Final fallback
            console.error('Returning empty object due to unparseable JSON');
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
    // Initialize connection cache and load users and groups immediately on page load
    getCurrentConnectionCache(); // This initializes the connection ID and cache
    
    loadGlossaryData();
    setupFilterListeners();
    setupBulkActions();
    setupActionButtonListeners();
    
    // Initialize create modals
    initializeCreateModals();
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
        
        // Attach checkbox handlers
        attachCheckboxHandlers(content, tabType);
        
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
            <table class="table table-hover mb-0">
                <thead>
                    <tr>
                        <th width="40">
                            <input type="checkbox" class="form-check-input select-all-checkbox" id="selectAll${tabType.charAt(0).toUpperCase() + tabType.slice(1)}">
                        </th>
                        <th class="sortable-header" data-sort="name" width="200">Name</th>
                        <th width="180">Description</th>
                        <th class="sortable-header text-center" data-sort="owners_count" width="70">Owners</th>
                        <th class="sortable-header text-center" data-sort="relationships_count" width="90">Relationships</th>
                        <th class="sortable-header text-center" data-sort="custom_properties_count" width="80">Custom<br/>Properties</th>
                        <th class="sortable-header text-center" data-sort="structured_properties_count" width="80">Structured<br/>Properties</th>
                        <th width="180">URN</th>
                        <th width="160">Actions</th>
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
    
    console.log(`organizeHierarchy: Processing ${items.length} items`);
    
    // First pass: create map of all items
    items.forEach((item, index) => {
        if (!item) {
            console.warn(`organizeHierarchy: Null/undefined item at index ${index}:`, item);
            return;
        }
        
        // For hierarchy organization, use URN if available, otherwise use ID for local items
        const itemKey = item.urn || item.id;
        
        if (itemKey) {
            itemMap.set(itemKey, { ...item, children: [] });
        } else {
            console.warn('organizeHierarchy: Item missing both URN and ID:', item);
        }
    });
    
    // Second pass: organize into hierarchy
    items.forEach(item => {
        if (!item) return;
        
        const itemKey = item.urn || item.id;
        if (!itemKey) return;
        
        try {
            const entityType = determineEntityType(item);
            let parentKey = null;
            
            // For glossary terms, find parent using different methods based on whether it's local or remote
            if (entityType === 'glossaryTerm') {
                // Try different parent reference methods
                parentKey = item.parent_node_urn || item.parent_node_id || item.parent_urn || item.parent_id;
            }
            // For glossary nodes, use parent reference
            else if (entityType === 'glossaryNode') {
                parentKey = item.parent_urn || item.parent_id;
            }
            
            if (parentKey && itemMap.has(parentKey)) {
                // Add to parent's children
                itemMap.get(parentKey).children.push(itemMap.get(itemKey));
            } else {
                // Root level item (no parent or parent not found)
                rootItems.push(itemMap.get(itemKey));
            }
        } catch (error) {
            console.error('Error organizing item in hierarchy:', item, error);
            // Add to root items as fallback
            if (itemMap.has(itemKey)) {
                rootItems.push(itemMap.get(itemKey));
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
    // First, validate that we have a valid item
    if (!item || typeof item !== 'object' || Object.keys(item).length === 0) {
        console.warn('Could not determine entity type for item:', item);
        return 'unknown';
    }
    
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
    
    // Determine the correct parent key for data attributes (URN or ID)
    let parentKey = null;
    if (entityType === 'glossaryTerm') {
        parentKey = item.parent_node_urn || item.parent_node_id || item.parent_urn || item.parent_id;
    } else if (entityType === 'glossaryNode') {
        parentKey = item.parent_urn || item.parent_id;
    }
    
    // Use URN or ID as the primary key for the item
    const itemKey = item.urn || item.id;
    
    // Store only essential data in the data-item attribute to avoid JSON size issues
    const minimalItemData = {
        id: item.id,
        name: item.name,
        urn: item.urn,
        type: item.type,
        sync_status: item.sync_status,
        description: item.description ? item.description.substring(0, 200) : ''
    };
    
    // Store the full item data in a global cache for lookup
    if (!window.glossaryDataCache) {
        window.glossaryDataCache = {};
    }
    
    // Use the URN as a unique key for the cache, fallback to ID
    const cacheKey = item.urn || item.id;
    if (cacheKey) {
        window.glossaryDataCache[cacheKey] = item;
    }
    
    // Use safe JSON stringify for minimal data attributes
    const safeJsonData = JSON.stringify(minimalItemData)
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&apos;');
    
    return `
        <tr data-item='${safeJsonData}' data-level="${level}" data-node-urn="${itemKey}" ${parentKey ? `data-parent-node="${parentKey}"` : ''} ${level > 0 ? 'style="display: none;"' : ''}>
            <td>
                <input type="checkbox" class="form-check-input item-checkbox" value="${item.id || item.urn}">
            </td>
            <td>
                <div class="hierarchy-container" style="margin-left: ${indent}px;">
                    ${level > 0 ? '<div class="tree-connector"></div>' : ''}
                    <div class="expand-button-container">
                        ${hasChildren ? `
                            <button class="expand-button" type="button" title="Expand/Collapse">
                                <i class="fas fa-chevron-right expand-icon" id="icon-${itemKey}"></i>
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
                    (() => {
                        let relationshipsCount = 0;
                        if (item.relationships_count !== undefined && item.relationships_count !== null) {
                            relationshipsCount = item.relationships_count;
                        } else if (item.relationships) {
                            relationshipsCount = item.relationships.length;
                        }
                        return relationshipsCount > 0 ? 
                            `<i class="fas fa-link text-success me-1"></i><span class="badge bg-success">${relationshipsCount}</span>` : 
                            '<span class="text-muted">None</span>';
                    })() :
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
                <div class="btn-group action-buttons" role="group">
                    ${getActionButtons(item, tabType)}
                </div>
            </td>
        </tr>
    `;
}

function getActionButtons(item, tabType) {
    const hasDataHubConnection = true; // This should come from the template context
    let actionButtons = '';
    
    // 1. View details button (always available) - FIRST like in tags
    actionButtons += `
        <button type="button" class="btn btn-sm btn-outline-primary view-item" 
                title="View Details">
            <i class="fas fa-eye"></i>
        </button>
    `;
    
    // 2. DataHub link (if item has URN and is not local-only) - SECOND like in tags
    if (item.urn && !item.urn.includes('local:') && glossaryData.datahub_url) {
        const entityType = determineEntityType(item);
        const datahubUrl = getDataHubUrl(item.urn, entityType);
        actionButtons += `
            <a href="${datahubUrl}" target="_blank" class="btn btn-sm btn-outline-info" title="View in DataHub">
                <i class="fas fa-external-link-alt"></i>
            </a>
        `;
    }
    
    // 3. Edit button - For local and synced items
    if (tabType === 'local' || tabType === 'synced') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-warning edit-item" 
                    title="Edit ${item.type === 'node' ? 'Node' : 'Term'}">
                <i class="fas fa-edit"></i>
            </button>
        `;
    }
    
    // 4. Deploy to DataHub - Only for local-only items
    if (tabType === 'local' && hasDataHubConnection) {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-success deploy-to-datahub" 
                    title="Deploy to DataHub">
                <i class="fas fa-cloud-upload-alt"></i>
            </button>
        `;
    }
    
    // 5. Resync - Only for synced items that are modified or synced
    if (tabType === 'synced' && (item.sync_status === 'MODIFIED' || item.sync_status === 'SYNCED') && hasDataHubConnection) {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-info resync-item" 
                    title="Resync from DataHub">
                <i class="fas fa-sync-alt"></i>
            </button>
        `;
    }
    
    // 6. Sync to Local - Only for remote items
    if (tabType === 'remote') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-primary sync-to-local" 
                    title="Import to Local">
                <i class="fas fa-download"></i>
            </button>
        `;
    }
    
    // 7. Download JSON - Available for all items
    actionButtons += `
        <button type="button" class="btn btn-sm btn-outline-secondary download-json"
                title="Download JSON">
            <i class="fas fa-file-download"></i>
        </button>
    `;

    // 8. Add to Staged Changes - Available for all items
    actionButtons += `
        <button type="button" class="btn btn-sm btn-outline-warning add-to-staged"
                title="Add to Staged Changes">
            <i class="fab fa-github"></i>
        </button>
    `;

    // 9. Delete Local - Only for synced and local items (LAST like in tags)
    if (tabType === 'synced' || tabType === 'local') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-danger delete-local" 
                    title="Delete Local ${item.type === 'node' ? 'Node' : 'Term'}">
                <i class="fas fa-trash"></i>
            </button>
        `;
    }
    
    return actionButtons;
}

function getEmptyStateHTML(tabType, hasSearch) {
    // Calculate colspan based on number of columns: checkbox + name + description + owners + relationships + custom props + structured props + urn + actions = 9
    const colspan = '9';
    
    if (hasSearch) {
        return `
            <tr>
                <td colspan="${colspan}" class="text-center py-4 text-muted">
                    <i class="fas fa-search fa-2x mb-2"></i><br>
                    No glossary items found matching your search criteria.
                </td>
            </tr>
        `;
    }
    
    const emptyStates = {
        synced: 'No synced glossary items found. Items that exist both locally and in DataHub will appear here.',
        local: 'No local-only glossary items found. Items that exist only in this application will appear here.',
        remote: 'No remote-only glossary items found. Items that exist only in DataHub will appear here.'
    };
    
    return `
        <tr>
            <td colspan="${colspan}" class="text-center py-4 text-muted">
                <i class="fas fa-folder-open fa-2x mb-2"></i><br>
                ${emptyStates[tabType]}
            </td>
        </tr>
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
    
    // Basic information - handle both old and new data structures
    const name = itemData.name || itemData.properties?.name || 'Unnamed';
    const description = itemData.description || itemData.properties?.description || 'No description available';
    const urn = itemData.urn || 'No URN available';
    
    document.getElementById('modal-item-name').textContent = name;
    document.getElementById('modal-item-description').textContent = description;
    document.getElementById('modal-item-urn').textContent = urn;
    
    // Set type with icon
    const typeElement = document.getElementById('modal-item-type');
    if (entityType === 'glossaryTerm') {
        typeElement.innerHTML = '<i class="fas fa-bookmark text-success me-1"></i>Glossary Term';
    } else if (entityType === 'glossaryNode') {
        typeElement.innerHTML = '<i class="fas fa-folder-open text-primary me-1"></i>Glossary Node';
    } else {
        typeElement.innerHTML = `<i class="fas fa-question-circle text-warning me-1"></i>Unknown Type`;
    }
    
    // Parent information (for terms and child nodes)
    const parentLabel = document.getElementById('modal-parent-label');
    const parentValue = document.getElementById('modal-parent-value');
    const parentElement = document.getElementById('modal-item-parent');
    
    let parentUrn = null;
    if (entityType === 'glossaryTerm') {
        // Check multiple possible locations for parent URN
        parentUrn = itemData.parent_node_urn || 
                   itemData.parentNodes?.nodes?.[0]?.urn ||
                   (itemData.parentNodes && itemData.parentNodes.length > 0 ? itemData.parentNodes[0].urn : null);
    } else if (entityType === 'glossaryNode') {
        parentUrn = itemData.parent_urn || 
                   itemData.parentNodes?.nodes?.[0]?.urn ||
                   (itemData.parentNodes && itemData.parentNodes.length > 0 ? itemData.parentNodes[0].urn : null);
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
    const syncStatus = itemData.sync_status_display || itemData.sync_status || item.sync_status_display || item.sync_status || 'Unknown';
    statusBadge.textContent = syncStatus;
    statusBadge.className = `badge ${getStatusBadgeClass(itemData.sync_status || item.sync_status)}`;
    
    // DataHub link
    const datahubLink = document.getElementById('modal-datahub-link');
    if (urn && !urn.includes('local:') && glossaryData.datahub_url) {
        datahubLink.href = getDataHubUrl(urn, entityType);
        datahubLink.style.display = 'inline-block';
    } else {
        datahubLink.style.display = 'none';
    }
    
    // Ownership information - handle both old and new structures
    const ownersList = document.getElementById('modal-owners-list');
    
    // Check for ownership data from the comprehensive GraphQL query or processed data
    let ownershipData = null;
    if (itemData.ownership?.owners) {
        ownershipData = itemData.ownership.owners;
    } else if (itemData.owners) {
        ownershipData = itemData.owners;
    } else if (item.ownership?.owners) {
        ownershipData = item.ownership.owners;
    } else if (item.owners) {
        ownershipData = item.owners;
    }
    
    if (ownersList && ownershipData && ownershipData.length > 0) {
        // Display ownership info using processed data
        let ownersHTML = '';
        ownershipData.forEach(owner => {
            const ownerName = owner.name || owner.displayName || owner.urn || 'Unknown';
            const ownerType = owner.type || (owner.urn && owner.urn.includes(':corpGroup:') ? 'group' : 'user');
            const ownershipTypeName = owner.ownershipType?.name || 'Unknown';
            const icon = ownerType === 'group' ? 'fas fa-users' : 'fas fa-user';
            
            ownersHTML += `
                <div class="owner-item mb-2">
                    <div class="d-flex align-items-center">
                        <i class="${icon} me-2"></i>
                        <div>
                            <strong>${escapeHtml(ownerName)}</strong>
                            <br>
                            <small class="text-muted">${escapeHtml(ownershipTypeName)}</small>
                        </div>
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
    
    // Only process relationships for glossary terms, not nodes
    // (entityType is already declared at the top of the function)
    
    if (entityType === 'glossaryTerm') {
        // Check if we already have relationships data from the backend
        let relationshipsData = itemData.relationships || item.relationships;
        
        if (relationshipsData && relationshipsData.length > 0) {
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
            // No relationships data available from backend for this term
            relationshipsList.innerHTML = '<p class="text-muted">No relationships available for this term</p>';
        }
    } else if (entityType === 'glossaryNode') {
        // Glossary nodes don't have relationships
        relationshipsList.innerHTML = '<p class="text-muted">Glossary nodes do not have relationships</p>';
    } else {
        // Unknown entity type
        relationshipsList.innerHTML = '<p class="text-muted">Relationships not applicable for this item type</p>';
    }
    
    // Properties - handle both old and new structures
    const propertiesDiv = document.getElementById('modal-glossary-properties');
    let propertiesHTML = '';
    
    // Custom Properties - check multiple possible locations
    let customProperties = itemData.custom_properties || itemData.customProperties || 
                          itemData.properties?.customProperties || item.custom_properties || 
                          item.customProperties;
    
    if (customProperties && customProperties.length > 0) {
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
    
    // Structured Properties - check multiple possible locations
    let structuredProperties = itemData.structured_properties || itemData.structuredProperties || 
                              item.structured_properties || item.structuredProperties;
    
    if (structuredProperties && structuredProperties.length > 0) {
        propertiesHTML += `
            <div class="border rounded p-2 mb-2">
                <strong>Structured Properties:</strong>
                <div class="mt-2">
                    ${structuredProperties.map(prop => `
                        <div class="mb-1">
                            <strong>${escapeHtml(prop.urn || prop.propertyUrn || 'Unknown')}:</strong> 
                            ${escapeHtml(prop.values ? JSON.stringify(prop.values) : JSON.stringify(prop.value) || '')}
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
    
    // Raw JSON - show the processed data
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
    if (!timestamp) return 'N/A';
    const date = new Date(timestamp);
    return date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
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

function setupActionButtonListeners() {
    // Use event delegation since rows are dynamically created
    document.addEventListener('click', function(e) {
        // Get the closest row to find item data
        const row = e.target.closest('tr[data-item]');
        if (!row) return;
        
        // Parse the minimal item data from the row
        const rawData = row.dataset.item
            .replace(/&quot;/g, '"')
            .replace(/&apos;/g, "'");
        
        let minimalItemData;
        try {
            minimalItemData = JSON.parse(rawData);
        } catch (error) {
            console.error('Failed to parse minimal item data:', error, rawData);
            return;
        }
        
        if (!minimalItemData) return;
        
        // Look up the full item data from the cache
        const cacheKey = minimalItemData.urn || minimalItemData.id;
        const itemData = window.glossaryDataCache && window.glossaryDataCache[cacheKey] ? 
                         window.glossaryDataCache[cacheKey] : minimalItemData;
        
        // Check which button was clicked
        const clickedElement = e.target.closest('button');
        if (!clickedElement) return;

        if (clickedElement.classList.contains('view-item') || clickedElement.closest('.view-item')) {
            // View Details button clicked
            console.log('View Details clicked for item:', itemData);
            showItemDetails(itemData);
            e.preventDefault();
            e.stopPropagation();
        } else if (clickedElement.classList.contains('edit-item') || clickedElement.closest('.edit-item')) {
            // Edit Item button clicked
            console.log('Edit Item clicked for item:', itemData);
            editItem(itemData);
            e.preventDefault();
            e.stopPropagation();
        } else if (clickedElement.classList.contains('deploy-to-datahub') || clickedElement.closest('.deploy-to-datahub')) {
            // Deploy to DataHub button clicked
            console.log('Deploy to DataHub clicked for item:', itemData);
            deployToDataHub(itemData);
            e.preventDefault();
            e.stopPropagation();
        } else if (clickedElement.classList.contains('resync-item') || clickedElement.closest('.resync-item')) {
            // Resync Item button clicked
            console.log('Resync Item clicked for item:', itemData);
            resyncItem(itemData);
            e.preventDefault();
            e.stopPropagation();
        } else if (clickedElement.classList.contains('sync-to-local') || clickedElement.closest('.sync-to-local')) {
            // Sync to Local button clicked
            console.log('Sync to Local clicked for item:', itemData);
            syncToLocal(itemData);
            e.preventDefault();
            e.stopPropagation();
        } else if (clickedElement.classList.contains('download-json') || clickedElement.closest('.download-json')) {
            // Download JSON button clicked
            console.log('Download JSON clicked for item:', itemData);
            downloadItemJson(itemData);
            e.preventDefault();
            e.stopPropagation();
        } else if (clickedElement.classList.contains('add-to-staged') || clickedElement.closest('.add-to-staged')) {
            // Add to Staged Changes button clicked
            console.log('Add to Staged Changes clicked for item:', itemData);
            addToStagedChanges(itemData);
            e.preventDefault();
            e.stopPropagation();
        } else if (clickedElement.classList.contains('delete-local') || clickedElement.closest('.delete-local')) {
            // Delete Local button clicked
            console.log('Delete Local clicked for item:', itemData);
            deleteLocalItem(itemData);
            e.preventDefault();
            e.stopPropagation();
        }
    });
}

/**
 * Edit an item by navigating to its edit page
 * @param {Object} item - The item object
 */
function editItem(item) {
    const itemType = item.type === 'node' ? 'nodes' : 'terms';
    const editUrl = `/metadata/glossary/${itemType}/${item.id}/`;
    window.location.href = editUrl;
}

/**
 * Deploy item to DataHub
 * @param {Object} item - The item object
 */
function deployToDataHub(item) {
    console.log('deployToDataHub called with:', item);
    
    if (!item.id) {
        console.error('Cannot deploy item without an ID:', item);
        showError('Error deploying item: Missing item ID.');
        return;
    }
    
    const itemType = item.type === 'node' ? 'Node' : 'Term';
    const itemName = item.name || 'Unknown';
    
    // Show loading notification
    showNotification('info', `Deploying ${itemType.toLowerCase()} "${itemName}" to DataHub...`);
    
    // Make the API call to deploy this item to DataHub
    const deployUrl = item.type === 'node' ? 
        `/metadata/glossary/nodes/${item.id}/deploy/` : 
        `/metadata/glossary/terms/${item.id}/deploy/`;
    
    fetch(deployUrl, {
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
        console.log('Deploy to DataHub response:', data);
        if (data.success) {
            showNotification('success', data.message || `${itemType} deployed successfully`);
            // Add a small delay before refreshing to ensure backend processing is complete
            setTimeout(() => {
                // Refresh the data to show updated sync status
                if (typeof loadGlossaryData === 'function') {
                    loadGlossaryData();
                } else {
                    window.location.reload();
                }
            }, 1000); // 1 second delay
        } else {
            throw new Error(data.error || 'Unknown error occurred');
        }
    })
    .catch(error => {
        console.error('Error deploying item to DataHub:', error);
        showNotification('error', `Error deploying ${itemType.toLowerCase()}: ${error.message}`);
    });
}

/**
 * Resync an item from DataHub
 * @param {Object} item - The item object
 */
function resyncItem(item) {
    console.log('resyncItem called with:', item);
    
    if (!item.id) {
        console.error('Cannot resync item without an ID:', item);
        showError('Error resyncing item: Missing item ID.');
        return;
    }
    
    const itemType = item.type === 'node' ? 'Node' : 'Term';
    const itemName = item.name || 'Unknown';
    
    if (!confirm(`Are you sure you want to resync "${itemName}" from DataHub? This will overwrite any local changes.`)) {
        return;
    }
    
    // Show loading notification
    showNotification('info', `Resyncing ${itemType.toLowerCase()} "${itemName}" from DataHub...`);
    
    // For now, we'll use the same deploy endpoint but could add a specific resync endpoint later
    const deployUrl = item.type === 'node' ? 
        `/metadata/glossary/nodes/${item.id}/deploy/` : 
        `/metadata/glossary/terms/${item.id}/deploy/`;
    
    fetch(deployUrl, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCsrfToken()
        },
        body: JSON.stringify({ action: 'resync' })
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
            showNotification('success', data.message || `${itemType} resynced successfully`);
            setTimeout(() => {
                if (typeof loadGlossaryData === 'function') {
                    loadGlossaryData();
                } else {
                    window.location.reload();
                }
            }, 1000);
        } else {
            throw new Error(data.error || 'Unknown error occurred');
        }
    })
    .catch(error => {
        console.error('Error resyncing item:', error);
        showNotification('error', `Error resyncing ${itemType.toLowerCase()}: ${error.message}`);
    });
}

/**
 * Sync item to local
 * @param {Object} item - The item object
 */
function syncToLocal(item) {
    console.log('syncToLocal called with:', item);
    
    if (!item.urn) {
        console.error('Cannot sync item without URN:', item);
        showError('Error syncing item: Missing URN.');
        return;
    }
    
    const itemType = item.type === 'node' ? 'Node' : 'Term';
    const itemName = item.name || 'Unknown';
    
    // Show loading notification
    showNotification('info', `Importing ${itemType.toLowerCase()} "${itemName}" to local...`);
    
    // Use the existing pull endpoint
    const formData = new FormData();
    formData.append('csrfmiddlewaretoken', getCsrfToken());
    formData.append(`${item.type}_urns`, item.urn);
    
    fetch('/metadata/glossary/pull/', {
        method: 'POST',
        body: formData
    })
    .then(response => {
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        return response.json();
    })
    .then(data => {
        console.log('Sync to local response:', data);
        if (data.success) {
            showNotification('success', data.message || `${itemType} imported successfully`);
            setTimeout(() => {
                if (typeof loadGlossaryData === 'function') {
                    loadGlossaryData();
                } else {
                    window.location.reload();
                }
            }, 1000);
        } else {
            throw new Error(data.error || 'Unknown error occurred');
        }
    })
    .catch(error => {
        console.error('Error syncing item to local:', error);
        showNotification('error', `Error importing ${itemType.toLowerCase()}: ${error.message}`);
    });
}

/**
 * Download item as JSON
 * @param {Object} item - The item object
 */
function downloadItemJson(item) {
    console.log('downloadItemJson called with:', item);
    
    const itemType = item.type === 'node' ? 'Node' : 'Term';
    const itemName = item.name || 'Unknown';
    
    // Create a clean JSON representation
    const jsonData = {
        urn: item.urn,
        name: item.name,
        description: item.description,
        type: item.type,
        sync_status: item.sync_status,
        properties: item.properties || {},
        ownership: item.ownership || [],
        custom_properties: item.custom_properties || {},
        structured_properties: item.structured_properties || {},
        relationships: item.relationships || [],
        institutional_memory: item.institutional_memory || {}
    };
    
    // Create and download the file
    const blob = new Blob([JSON.stringify(jsonData, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${itemType.toLowerCase()}_${itemName.replace(/[^a-zA-Z0-9]/g, '_')}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    showNotification('success', `${itemType} JSON downloaded successfully`);
}

/**
 * Add item to staged changes
 * @param {Object} item - The item object
 */
function addToStagedChanges(item) {
    console.log('addToStagedChanges called with:', item);
    
    if (!item.id) {
        console.error('Cannot add item to staged changes without an ID:', item);
        showError('Error adding to staged changes: Missing item ID.');
        return;
    }
    
    const itemType = item.type === 'node' ? 'Node' : 'Term';
    const itemName = item.name || 'Unknown';
    
    // Show loading notification
    showNotification('info', `Adding ${itemType.toLowerCase()} "${itemName}" to staged changes...`);
    
    // Use the git push endpoint
    const pushUrl = item.type === 'node' ? 
        `/metadata/glossary/nodes/${item.id}/git_push/` : 
        `/metadata/glossary/terms/${item.id}/git_push/`;
    
    fetch(pushUrl, {
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
        console.log('Add to staged changes response:', data);
        if (data.success) {
            showNotification('success', data.message || `${itemType} added to staged changes successfully`);
        } else {
            throw new Error(data.error || 'Unknown error occurred');
        }
    })
    .catch(error => {
        console.error('Error adding item to staged changes:', error);
        showNotification('error', `Error adding ${itemType.toLowerCase()} to staged changes: ${error.message}`);
    });
}

/**
 * Delete local item
 * @param {Object} item - The item object
 */
function deleteLocalItem(item) {
    console.log('deleteLocalItem called with:', item);
    
    if (!item.id) {
        console.error('Cannot delete item without an ID:', item);
        showError('Error deleting item: Missing item ID.');
        return;
    }
    
    const itemType = item.type === 'node' ? 'Node' : 'Term';
    const itemName = item.name || 'Unknown';
    
    if (!confirm(`Are you sure you want to delete the local ${itemType.toLowerCase()} "${itemName}"? This action cannot be undone.`)) {
        return;
    }
    
    // Show loading notification
    showNotification('info', `Deleting local ${itemType.toLowerCase()} "${itemName}"...`);
    
    // Use the delete endpoint
    const deleteUrl = item.type === 'node' ? 
        `/metadata/glossary/nodes/${item.id}/` : 
        `/metadata/glossary/terms/${item.id}/`;
    
    fetch(deleteUrl, {
        method: 'DELETE',
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
        console.log('Delete local item response:', data);
        if (data.success) {
            showNotification('success', data.message || `${itemType} deleted successfully`);
            setTimeout(() => {
                if (typeof loadGlossaryData === 'function') {
                    loadGlossaryData();
                } else {
                    window.location.reload();
                }
            }, 1000);
        } else {
            throw new Error(data.error || 'Unknown error occurred');
        }
    })
    .catch(error => {
        console.error('Error deleting local item:', error);
        showNotification('error', `Error deleting ${itemType.toLowerCase()}: ${error.message}`);
    });
}

/**
 * Show notification to user
 * @param {string} type - The notification type (success, error, info, warning)
 * @param {string} message - The message to display
 */
function showNotification(type, message) {
    // Create notification element
    const notification = document.createElement('div');
    notification.className = `alert alert-${type} alert-dismissible fade show position-fixed`;
    notification.style.cssText = 'top: 20px; right: 20px; z-index: 1050; min-width: 300px;';
    notification.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    // Add to page
    document.body.appendChild(notification);
    
    // Auto-remove after 5 seconds
    setTimeout(() => {
        if (notification.parentNode) {
            notification.parentNode.removeChild(notification);
        }
    }, 5000);
}

// Checkbox and bulk action handlers
function attachCheckboxHandlers(content, tabType) {
    // Attach individual checkbox handlers
    content.querySelectorAll('.item-checkbox').forEach(checkbox => {
        checkbox.addEventListener('change', function() {
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

function updateBulkActionsVisibility(tabType) {
    const checkboxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    const bulkActions = document.getElementById(`${tabType}-bulk-actions`);
    const selectedCount = document.getElementById(`${tabType}-selected-count`);
    
    if (checkboxes.length > 0) {
        bulkActions.classList.add('show');
        selectedCount.textContent = checkboxes.length;
    } else {
        bulkActions.classList.remove('show');
        selectedCount.textContent = '0';
    }
}

function getSelectedItems(tabType) {
    const checkboxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    const selectedItems = [];
    
    checkboxes.forEach(checkbox => {
        const row = checkbox.closest('tr');
        if (row && row.dataset.item) {
            // Parse the minimal item data from the row
            const rawData = row.dataset.item
                .replace(/&quot;/g, '"')
                .replace(/&apos;/g, "'");
            
            let minimalItemData;
            try {
                minimalItemData = JSON.parse(rawData);
            } catch (error) {
                console.error('Failed to parse minimal item data in bulk selection:', error);
                return;
            }
            
            if (minimalItemData) {
                // Look up the full item data from the cache
                const cacheKey = minimalItemData.urn || minimalItemData.id;
                const fullItemData = window.glossaryDataCache && window.glossaryDataCache[cacheKey] ? 
                                   window.glossaryDataCache[cacheKey] : minimalItemData;
                selectedItems.push(fullItemData);
            }
        }
    });
    
    return selectedItems;
}

// Bulk action functions
function bulkResyncItems(tabType) {
    const selectedItems = getSelectedItems(tabType);
    if (selectedItems.length === 0) {
        showNotification('warning', 'No items selected for resync');
        return;
    }
    
    if (!confirm(`Are you sure you want to resync ${selectedItems.length} selected items?`)) {
        return;
    }
    
    // Process each item
    selectedItems.forEach(item => {
        resyncItem(item);
    });
    
    showNotification('success', `Started resync for ${selectedItems.length} items`);
}

function bulkDeployItems(tabType) {
    const selectedItems = getSelectedItems(tabType);
    if (selectedItems.length === 0) {
        showNotification('warning', 'No items selected for deployment');
        return;
    }
    
    if (!confirm(`Are you sure you want to deploy ${selectedItems.length} selected items to DataHub?`)) {
        return;
    }
    
    // Process each item
    selectedItems.forEach(item => {
        deployToDataHub(item);
    });
    
    showNotification('success', `Started deployment for ${selectedItems.length} items`);
}

function bulkSyncToLocal(tabType) {
    const selectedItems = getSelectedItems(tabType);
    if (selectedItems.length === 0) {
        showNotification('warning', 'No items selected for sync to local');
        return;
    }
    
    if (!confirm(`Are you sure you want to sync ${selectedItems.length} selected items to local?`)) {
        return;
    }
    
    // Process each item
    selectedItems.forEach(item => {
        syncToLocal(item);
    });
    
    showNotification('success', `Started sync to local for ${selectedItems.length} items`);
}

function bulkDownloadJson(tabType) {
    const selectedItems = getSelectedItems(tabType);
    if (selectedItems.length === 0) {
        showNotification('warning', 'No items selected for download');
        return;
    }
    
    // Create a combined JSON file
    const combinedData = {
        items: selectedItems,
        exported_at: new Date().toISOString(),
        count: selectedItems.length,
        type: 'glossary_bulk_export'
    };
    
    const blob = new Blob([JSON.stringify(combinedData, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `glossary_bulk_export_${selectedItems.length}_items_${new Date().toISOString().split('T')[0]}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    showNotification('success', `Downloaded ${selectedItems.length} items as JSON`);
}

function bulkAddToPR(tabType) {
    const selectedItems = getSelectedItems(tabType);
    if (selectedItems.length === 0) {
        showNotification('warning', 'No items selected for adding to staged changes');
        return;
    }
    
    if (!confirm(`Are you sure you want to add ${selectedItems.length} selected items to staged changes?`)) {
        return;
    }
    
    // Process each item
    selectedItems.forEach(item => {
        addToStagedChanges(item);
    });
    
    showNotification('success', `Added ${selectedItems.length} items to staged changes`);
}

function bulkDeleteLocal(tabType) {
    const selectedItems = getSelectedItems(tabType);
    if (selectedItems.length === 0) {
        showNotification('warning', 'No items selected for deletion');
        return;
    }
    
    if (!confirm(`Are you sure you want to delete ${selectedItems.length} selected local items? This action cannot be undone.`)) {
        return;
    }
    
    // Process each item
    selectedItems.forEach(item => {
        deleteLocalItem(item);
    });
    
    showNotification('success', `Deleted ${selectedItems.length} local items`);
}

// Modal initialization functions
async function initializeCreateModals() {
    console.log('Initializing create modals...');
    
    // Load users and groups data first
    try {
        await loadUsersAndGroups();
        console.log('Users and groups loaded successfully');
    } catch (error) {
        console.error('Error loading users and groups:', error);
    }
    
    // Initialize create node modal
    const createNodeModal = document.getElementById('createNodeModal');
    console.log('createNodeModal found:', !!createNodeModal);
    if (createNodeModal) {
        createNodeModal.addEventListener('show.bs.modal', function() {
            console.log('Node modal opening...');
            // Load parent node options
            populateParentNodeOptions('node-parent', false); // false = optional parent
            
            // Set up the ownership interface
            setupNodeOwnershipInterface();
            
            // Hide ownership section by default
            hideNodeOwnershipSectionIfEmpty();
        });
        
        // Clean up when modal is closed
        createNodeModal.addEventListener('hidden.bs.modal', function() {
            resetNodeModal();
        });
    }
    
    // Initialize create term modal
    const createTermModal = document.getElementById('createTermModal');
    console.log('createTermModal found:', !!createTermModal);
    if (createTermModal) {
        createTermModal.addEventListener('show.bs.modal', function() {
            console.log('Term modal opening...');
            // Load parent node options
            populateParentNodeOptions('term-parent-node', true); // true = required parent
            
            // Load relationship options
            populateTermRelationshipOptions();
            
            // Set up the ownership interface
            setupTermOwnershipInterface();
            
            // Hide ownership section by default
            hideTermOwnershipSectionIfEmpty();
        });
        
        // Clean up when modal is closed
        createTermModal.addEventListener('hidden.bs.modal', function() {
            resetTermModal();
        });
    }
    
    console.log('Create modals initialization complete');
}

function populateParentNodeOptions(selectId, required = false) {
    const select = document.getElementById(selectId);
    if (!select) return;
    
    // Clear existing options (except the first placeholder)
    while (select.children.length > 1) {
        select.removeChild(select.lastChild);
    }
    
    // Get all available nodes from the current data
    const allNodes = [];
    
    // Collect nodes from all tabs
    ['synced', 'local', 'remote'].forEach(tabType => {
        const tabData = glossaryData[tabType] || [];
        tabData.forEach(item => {
            if (determineEntityType(item) === 'glossaryNode') {
                allNodes.push({
                    id: item.id,
                    name: item.name,
                    urn: item.urn,
                    tabType: tabType
                });
            }
        });
    });
    
    // Sort nodes by name
    allNodes.sort((a, b) => a.name.localeCompare(b.name));
    
    // Add nodes to select
    allNodes.forEach(node => {
        const option = document.createElement('option');
        option.value = node.id;
        option.textContent = `${node.name} (${node.tabType})`;
        select.appendChild(option);
    });
    
    // Update required attribute
    select.required = required;
}

function populateTermRelationshipOptions() {
    const isASelect = document.getElementById('term-is-a');
    const hasASelect = document.getElementById('term-has-a');
    
    if (!isASelect || !hasASelect) return;
    
    // Clear existing options
    isASelect.innerHTML = '';
    hasASelect.innerHTML = '';
    
    // Get all available terms from the current data
    const allTerms = [];
    
    // Collect terms from all tabs
    ['synced', 'local', 'remote'].forEach(tabType => {
        const tabData = glossaryData[tabType] || [];
        tabData.forEach(item => {
            if (determineEntityType(item) === 'glossaryTerm') {
                allTerms.push({
                    id: item.id,
                    name: item.name,
                    urn: item.urn,
                    tabType: tabType
                });
            }
        });
    });
    
    // Sort terms by name
    allTerms.sort((a, b) => a.name.localeCompare(b.name));
    
    // Add terms to both selects
    allTerms.forEach(term => {
        const isAOption = document.createElement('option');
        isAOption.value = term.urn;
        isAOption.textContent = `${term.name} (${term.tabType})`;
        isASelect.appendChild(isAOption);
        
        const hasAOption = document.createElement('option');
        hasAOption.value = term.urn;
        hasAOption.textContent = `${term.name} (${term.tabType})`;
        hasASelect.appendChild(hasAOption);
    });
}

// Glossary Ownership Management Functions (similar to tags)

// Load users and groups from the API
async function loadUsersAndGroups() {
    console.log('Loading users and groups for glossary...');
    try {
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
            // Use the existing proxy cache
            const cache = getCurrentConnectionCache();
            cache.users = data.data.users || [];
            cache.groups = data.data.groups || [];
            cache.ownership_types = data.data.ownership_types || [];
            console.log('Loaded users and groups:', cache);
        } else {
            throw new Error(data.error || 'Failed to load users and groups');
        }
    } catch (error) {
        console.error('Error loading users and groups:', error);
        throw error;
    }
}

// Setup the ownership interface for nodes
function setupNodeOwnershipInterface() {
    const container = document.getElementById('node-ownership-sections-container');
    const addButton = document.getElementById('add-node-ownership-section');
    
    if (!container || !addButton) return;
    
    // Clear existing sections
    container.innerHTML = '';
    
    // Remove any existing event listeners to prevent multiple handlers
    const newAddButton = addButton.cloneNode(true);
    addButton.parentNode.replaceChild(newAddButton, addButton);
    
    // Setup add ownership section button
    newAddButton.addEventListener('click', (e) => {
        e.preventDefault();
        addNodeOwnershipSection();
        showNodeOwnershipSection();
        // Add a subtle animation to the button
        newAddButton.style.transform = 'scale(0.95)';
        setTimeout(() => {
            newAddButton.style.transform = 'scale(1)';
        }, 150);
    });
}

// Setup the ownership interface for terms
function setupTermOwnershipInterface() {
    const container = document.getElementById('term-ownership-sections-container');
    const addButton = document.getElementById('add-term-ownership-section');
    
    if (!container || !addButton) return;
    
    // Clear existing sections
    container.innerHTML = '';
    
    // Remove any existing event listeners to prevent multiple handlers
    const newAddButton = addButton.cloneNode(true);
    addButton.parentNode.replaceChild(newAddButton, addButton);
    
    // Setup add ownership section button
    newAddButton.addEventListener('click', (e) => {
        e.preventDefault();
        addTermOwnershipSection();
        showTermOwnershipSection();
        // Add a subtle animation to the button
        newAddButton.style.transform = 'scale(0.95)';
        setTimeout(() => {
            newAddButton.style.transform = 'scale(1)';
        }, 150);
    });
}

// Add a new ownership section for nodes
function addNodeOwnershipSection() {
    const container = document.getElementById('node-ownership-sections-container');
    if (!container) return;
    
    showNodeOwnershipSection();
    
    const sectionId = 'node-section-' + Date.now();
    
    const sectionHTML = `
        <div class="card mb-3" id="${sectionId}">
            <div class="card-header bg-light d-flex justify-content-between align-items-center">
                <h6 class="mb-0">
                    <i class="fas fa-crown me-2 text-primary"></i>
                    Ownership Section
                </h6>
                <button type="button" class="btn btn-sm btn-outline-danger" onclick="removeNodeSection('${sectionId}')">
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
    
    updateNodeRemoveButtons();
}

// Add a new ownership section for terms
function addTermOwnershipSection() {
    const container = document.getElementById('term-ownership-sections-container');
    if (!container) return;
    
    showTermOwnershipSection();
    
    const sectionId = 'term-section-' + Date.now();
    
    const sectionHTML = `
        <div class="card mb-3" id="${sectionId}">
            <div class="card-header bg-light d-flex justify-content-between align-items-center">
                <h6 class="mb-0">
                    <i class="fas fa-crown me-2 text-primary"></i>
                    Ownership Section
                </h6>
                <button type="button" class="btn btn-sm btn-outline-danger" onclick="removeTermSection('${sectionId}')">
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
    
    updateTermRemoveButtons();
}

// Populate owners select dropdown
function populateOwnersSelect(selectElement) {
    if (!selectElement) return;
    
    const cache = getCurrentConnectionCache();
    selectElement.innerHTML = '';
    
    // Add users
    if (cache.users.length > 0) {
        const userGroup = document.createElement('optgroup');
        userGroup.label = 'Users';
        cache.users.forEach(user => {
            const option = document.createElement('option');
            option.value = user.urn;
            option.textContent = `${user.display_name || user.username} (${user.username})`;
            option.dataset.type = 'user';
            userGroup.appendChild(option);
        });
        selectElement.appendChild(userGroup);
    }
    
    // Add groups
    if (cache.groups.length > 0) {
        const groupGroup = document.createElement('optgroup');
        groupGroup.label = 'Groups';
        cache.groups.forEach(group => {
            const option = document.createElement('option');
            option.value = group.urn;
            option.textContent = group.display_name;
            option.dataset.type = 'group';
            groupGroup.appendChild(option);
        });
        selectElement.appendChild(groupGroup);
    }
}

// Populate ownership type select dropdown
function populateOwnershipTypeSelect(selectElement) {
    if (!selectElement) return;
    
    const cache = getCurrentConnectionCache();
    
    // Clear existing options except the first one
    selectElement.innerHTML = '<option value="">Select ownership type...</option>';
    
    cache.ownership_types.forEach(type => {
        const option = document.createElement('option');
        option.value = type.urn;
        option.textContent = type.name;
        selectElement.appendChild(option);
    });
}

// Remove section functions
function removeNodeSection(sectionId) {
    const section = document.getElementById(sectionId);
    if (section) {
        section.remove();
        updateNodeRemoveButtons();
        hideNodeOwnershipSectionIfEmpty();
    }
}

function removeTermSection(sectionId) {
    const section = document.getElementById(sectionId);
    if (section) {
        section.remove();
        updateTermRemoveButtons();
        hideTermOwnershipSectionIfEmpty();
    }
}

// Update remove button visibility for nodes
function updateNodeRemoveButtons() {
    const container = document.getElementById('node-ownership-sections-container');
    if (!container) return;
    
    const sections = container.querySelectorAll('.card');
    const removeButtons = container.querySelectorAll('.btn-outline-danger');
    
    removeButtons.forEach(button => {
        button.style.display = sections.length > 1 ? 'block' : 'none';
    });
}

// Update remove button visibility for terms
function updateTermRemoveButtons() {
    const container = document.getElementById('term-ownership-sections-container');
    if (!container) return;
    
    const sections = container.querySelectorAll('.card');
    const removeButtons = container.querySelectorAll('.btn-outline-danger');
    
    removeButtons.forEach(button => {
        button.style.display = sections.length > 1 ? 'block' : 'none';
    });
}

// Show/hide ownership sections for nodes
function hideNodeOwnershipSectionIfEmpty() {
    const container = document.getElementById('node-ownership-sections-container');
    const addButton = document.getElementById('add-node-ownership-section');
    const label = document.getElementById('node-ownership-label');
    const helpText = document.getElementById('node-ownership-help-text');
    
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

function showNodeOwnershipSection() {
    const container = document.getElementById('node-ownership-sections-container');
    const addButton = document.getElementById('add-node-ownership-section');
    const label = document.getElementById('node-ownership-label');
    const helpText = document.getElementById('node-ownership-help-text');
    
    if (!container || !addButton) return;
    
    // Show all elements
    if (label) label.style.display = 'block';
    container.style.display = 'block';
    if (helpText) helpText.style.display = 'block';
    
    // Update button text and style
    addButton.innerHTML = '<i class="fas fa-plus me-1"></i> Add Owner';
    addButton.className = 'btn btn-sm btn-outline-primary mt-2';
}

// Show/hide ownership sections for terms
function hideTermOwnershipSectionIfEmpty() {
    const container = document.getElementById('term-ownership-sections-container');
    const addButton = document.getElementById('add-term-ownership-section');
    const label = document.getElementById('term-ownership-label');
    const helpText = document.getElementById('term-ownership-help-text');
    
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

function showTermOwnershipSection() {
    const container = document.getElementById('term-ownership-sections-container');
    const addButton = document.getElementById('add-term-ownership-section');
    const label = document.getElementById('term-ownership-label');
    const helpText = document.getElementById('term-ownership-help-text');
    
    if (!container || !addButton) return;
    
    // Show all elements
    if (label) label.style.display = 'block';
    container.style.display = 'block';
    if (helpText) helpText.style.display = 'block';
    
    // Update button text and style
    addButton.innerHTML = '<i class="fas fa-plus me-1"></i> Add Owner';
    addButton.className = 'btn btn-sm btn-outline-primary mt-2';
}

// Reset modal functions
function resetNodeModal() {
    // Reset form
    const form = document.getElementById('createNodeForm');
    if (form) {
        form.reset();
    }
    
    // Clear ownership sections container
    const ownershipContainer = document.getElementById('node-ownership-sections-container');
    if (ownershipContainer) {
        ownershipContainer.innerHTML = '';
    }
    
    // Hide ownership section by default
    hideNodeOwnershipSectionIfEmpty();
}

function resetTermModal() {
    // Reset form
    const form = document.getElementById('createTermForm');
    if (form) {
        form.reset();
    }
    
    // Clear ownership sections container
    const ownershipContainer = document.getElementById('term-ownership-sections-container');
    if (ownershipContainer) {
        ownershipContainer.innerHTML = '';
    }
    
    // Hide ownership section by default
    hideTermOwnershipSectionIfEmpty();
}

// Bulk Actions Setup
function setupBulkActions() {
    ['synced', 'local', 'remote'].forEach(tab => {
        const selectAllCheckbox = document.getElementById(`selectAll${tab.charAt(0).toUpperCase() + tab.slice(1)}`);
        if (selectAllCheckbox) {
            selectAllCheckbox.addEventListener('change', function() {
                const checkboxes = document.querySelectorAll(`#${tab}-content .item-checkbox`);
                checkboxes.forEach(checkbox => {
                    checkbox.checked = this.checked;
                });
                updateBulkActionsVisibility(tab);
            });
        }
    });
}