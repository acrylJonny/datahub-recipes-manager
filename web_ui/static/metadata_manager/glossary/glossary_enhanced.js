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

// Connection-specific cache for users, groups, ownership types, and domains
let usersAndGroupsCacheByConnection = {};
let domainsCacheByConnection = {};
let currentConnectionId = null;

// Proxy objects to maintain backward compatibility
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

let domainsCache = new Proxy({}, {
    get(target, prop) {
        const connectionCache = getCurrentDomainCache();
        return connectionCache[prop];
    },
    set(target, prop, value) {
        const connectionCache = getCurrentDomainCache();
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

// Get or create domain cache for current connection
function getCurrentDomainCache() {
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
    
    if (!domainsCacheByConnection[currentConnectionId]) {
        domainsCacheByConnection[currentConnectionId] = {
            domains: [],
            lastFetched: null,
            cacheExpiry: 5 * 60 * 1000 // 5 minutes
        };
    }
    
    return domainsCacheByConnection[currentConnectionId];
}

// Make cache globally accessible for connection switching
if (!window.usersAndGroupsCache) {
    window.usersAndGroupsCache = usersAndGroupsCache;
    window.usersAndGroupsCacheByConnection = usersAndGroupsCacheByConnection;
    window.domainsCache = domainsCache;
    window.domainsCacheByConnection = domainsCacheByConnection;
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
    loadUsersAndGroups();
    loadDomains();
    loadGlossaryNodes(); // Load nodes for parent selection dropdowns
    
    loadGlossaryData();
    setupFilterListeners();
    setupBulkActions();
    setupActionButtonListeners();
    
    // Refresh button
    document.getElementById('refreshGlossary').addEventListener('click', function() {
        loadGlossaryData();
        loadGlossaryNodes(); // Reload nodes when refreshing
    });
    
    // Load users and groups when create modals are opened
    document.getElementById('createNodeModal').addEventListener('show.bs.modal', async function() {
        // Ensure users and groups are loaded before setting up ownership interface
        if (usersAndGroupsCache.users.length === 0 && usersAndGroupsCache.groups.length === 0) {
            try {
                await loadUsersAndGroups();
                console.log('Node modal: Users and groups loaded successfully');
            } catch (error) {
                console.error('Node modal: Error loading users and groups:', error);
            }
        }
        
        // Load glossary nodes for parent selection
        if (!window.glossaryNodesCache || window.glossaryNodesCache.length === 0) {
            try {
                await loadGlossaryNodes();
                console.log('Node modal: Glossary nodes loaded successfully');
            } catch (error) {
                console.error('Node modal: Error loading glossary nodes:', error);
            }
        } else {
            // Populate dropdowns with cached nodes
            populateParentNodeDropdowns();
        }
        
        // Only reset if not in edit mode (edit mode sets data attributes)
        const form = document.getElementById('createNodeForm');
        if (!form.dataset.editMode) {
            resetNodeModal();
        }
        
        // Set up the ownership interface (Add Owner button functionality)
        setupNodeOwnershipInterface();
        
        // Hide ownership section by default unless in edit mode with existing owners
        hideNodeOwnershipSectionIfEmpty();
    });
    
    // Clean up when modal is closed
    document.getElementById('createNodeModal').addEventListener('hidden.bs.modal', function() {
        resetNodeModal();
    });
    
    // Load users and groups when create term modal is opened
    document.getElementById('createTermModal').addEventListener('show.bs.modal', async function() {
        // Ensure users and groups are loaded before setting up ownership interface
        if (usersAndGroupsCache.users.length === 0 && usersAndGroupsCache.groups.length === 0) {
            try {
                await loadUsersAndGroups();
                console.log('Term modal: Users and groups loaded successfully');
            } catch (error) {
                console.error('Term modal: Error loading users and groups:', error);
            }
        }
        
        // Load domains for domain selection
        const domainCache = getCurrentDomainCache();
        if (!domainCache.domains || domainCache.domains.length === 0) {
            try {
                await loadDomains();
                console.log('Term modal: Domains loaded successfully');
            } catch (error) {
                console.error('Term modal: Error loading domains:', error);
            }
        }
        
        // Load glossary nodes for parent selection
        if (!window.glossaryNodesCache || window.glossaryNodesCache.length === 0) {
            try {
                await loadGlossaryNodes();
                console.log('Term modal: Glossary nodes loaded successfully');
            } catch (error) {
                console.error('Term modal: Error loading glossary nodes:', error);
            }
        } else {
            // Populate dropdowns with cached nodes
            populateParentNodeDropdowns();
        }
        
        // Populate domain dropdown
        populateDomainDropdown();
        
        // Only reset if not in edit mode (edit mode sets data attributes)
        const form = document.getElementById('createTermForm');
        if (!form.dataset.editMode) {
            resetTermModal();
        }
        
        // Set up the ownership interface (Add Owner button functionality)
        setupTermOwnershipInterface();
        
        // Hide ownership section by default unless in edit mode with existing owners
        hideTermOwnershipSectionIfEmpty();
    });
    
    // Clean up when modal is closed
    document.getElementById('createTermModal').addEventListener('hidden.bs.modal', function() {
        resetTermModal();
    });
});

function loadGlossaryData() {
    console.log('Loading glossary data...');
    showLoading(true);
    
    fetch('/metadata/glossary/data/')
        .then(response => {
            console.log('Received response:', response.status, response.statusText);
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            // Check if the response is actually JSON
            const contentType = response.headers.get('content-type');
            if (!contentType || !contentType.includes('application/json')) {
                throw new Error(`Expected JSON response but got: ${contentType}`);
            }
            
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
        
        // Apply sorting if specified
        if (currentSort.column && currentSort.tabType === tabType) {
            items = sortItems(items, currentSort.column, currentSort.direction);
        } else {
            // Default sort by name (A-Z)
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
                            // Synced tab should show both SYNCED and MODIFIED items (both exist locally and remotely)
                            return item.sync_status === 'SYNCED' || item.sync_status === 'MODIFIED';
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
        
        // Attach sorting handlers
        attachSortingHandlers(content, tabType);
        
        // Restore sort state visual indicators
        restoreSortState(content, tabType);
        
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
                        <th width="30">
                            <input type="checkbox" class="form-check-input select-all-checkbox" id="selectAll${tabType.charAt(0).toUpperCase() + tabType.slice(1)}">
                        </th>
                        <th class="sortable-header" data-sort="name" width="200">Name</th>
                        <th class="text-start" width="160">Description</th>
                        <th class="sortable-header text-center" data-sort="domain" width="120">Domain</th>
                        <th class="sortable-header text-center" data-sort="owners_count" width="70">Owners</th>
                        <th class="sortable-header text-center" data-sort="custom_properties_count" width="120">Custom<br/>Properties</th>
                        <th class="sortable-header text-center" data-sort="structured_properties_count" width="80">Structured<br/>Properties</th>
                        <th class="sortable-header text-center" data-sort="deprecated" width="80">Deprecated</th>
                        <th width="160">URN</th>
                        ${tabType === 'synced' ? '<th class="sortable-header text-center" data-sort="sync_status" width="100">Sync Status</th>' : ''}
                        <th width="180">Actions</th>
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
        // Use folder icon for nodes - open folder for nodes without children or when expanded
        // For nodes with children, check if they're expanded (initially all are collapsed)
        const isExpanded = false; // Initially all nodes are collapsed
        typeIcon = hasChildren && !isExpanded ? 'fas fa-folder text-primary' : 'fas fa-folder-open text-primary';
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
                <div class="description-preview" title="${escapeHtml(item.description || '')}">${escapeHtml(truncateText(item.description || '', 120))}</div>
            </td>
            <td class="text-center">
                ${entityType === 'glossaryTerm' ? 
                    (() => {
                        // Calculate domain count dynamically from available data
                        const domains = item.domains;
                        let domainCount = 0;
                        let domainName = '';
                        
                        if (domains && domains.domain) {
                            domainCount = 1;
                            domainName = domains.domain.name || domains.domain.urn;
                        } else if (item.domain_urn) {
                            // Fallback: check for direct domain_urn field
                            domainCount = 1;
                            domainName = item.domain_name || item.domain_urn;
                        } else if (item.raw_data && item.raw_data.domain) {
                            // Check raw GraphQL data for domain information
                            domainCount = 1;
                            domainName = item.raw_data.domain.domain?.properties?.name || 
                                        item.raw_data.domain.domain?.urn || 'Unknown Domain';
                        }
                        
                        if (domainCount > 0) {
                            return `<i class="fas fa-sitemap text-primary me-1"></i><span class="badge bg-primary" title="Domain: ${escapeHtml(domainName)}">${domainCount}</span>`;
                        }
                        return '<span class="text-muted">None</span>';
                    })() :
                    '<span class="text-muted">N/A</span>'
                }
            </td>
            <td class="text-center">
                ${(item.owners_count || 0) > 0 ? `<i class="fas fa-users text-info me-1"></i><span class="badge bg-info">${item.owners_count}</span>` : '<span class="text-muted">None</span>'}
            </td>
            <td class="text-center">
                <span class="badge bg-secondary">${item.custom_properties_count || 0}</span>
            </td>
            <td class="text-center">
                <span class="badge bg-success">${item.structured_properties_count || 0}</span>
            </td>
            <td class="text-center">
                ${item.deprecated ? '<span class="badge bg-warning">Yes</span>' : '<span class="badge bg-success">No</span>'}
            </td>
            <td>
                <code class="small" title="${escapeHtml(item.urn || '')}">${escapeHtml(truncateUrnFromEnd(item.urn || '', 30))}</code>
            </td>
            ${tabType === 'synced' ? `
                <td class="text-center">
                    <span class="badge ${getStatusBadgeClass(item.sync_status)}">${(item.sync_status || 'UNKNOWN').replace('_', ' ')}</span>
                </td>
            ` : ''}
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

    // 9. Delete button - For remote-only items
    if (tabType === 'remote') {
        const entityType = determineEntityType(item);
        let canDelete = false;
        let disabledReason = '';
        
        if (entityType === 'glossaryTerm') {
            // Terms can always be deleted
            canDelete = true;
        } else if (entityType === 'glossaryNode') {
            // Nodes can only be deleted if they have no children
            canDelete = !item.children || item.children.length === 0;
            if (!canDelete) {
                disabledReason = 'Glossary nodes with children cannot be deleted. Please delete all child items first.';
            }
        }
        
        if (canDelete) {
            actionButtons += `
                <button type="button" class="btn btn-sm btn-outline-danger delete-remote-item" 
                        title="Delete ${item.type === 'node' ? 'Node' : 'Term'} from DataHub">
                    <i class="fas fa-trash"></i>
                </button>
            `;
        } else {
            // Show disabled button with tooltip explanation
            actionButtons += `
                <button type="button" class="btn btn-sm btn-outline-secondary delete-remote-disabled" 
                        disabled 
                        title="${disabledReason}"
                        style="opacity: 0.5; cursor: not-allowed;">
                    <i class="fas fa-trash"></i>
                </button>
            `;
        }
    }
    
    // 10. Delete Local - Only for synced and local items (using same trash icon)
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
    // Determine the correct colspan based on the number of columns shown
    const colspan = tabType === 'synced' ? '10' : '9'; // checkbox + name + description + domain + owners + custom props + structured props + deprecated + urn + (sync status for synced) + actions
    
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
        local: 'No local-only glossary items found. Create new items or import from DataHub to get started.',
        remote: 'No remote-only glossary items found. All remote items have been imported locally.'
    };
    
    return `
        <tr>
            <td colspan="${colspan}" class="text-center py-4 text-muted">
                <i class="fas fa-inbox fa-2x mb-2"></i><br>
                ${emptyStates[tabType] || 'No items found.'}
            </td>
        </tr>
    `;
}



function showItemDetails(item) {
    // Load domains cache if needed for domain lookup
    if (domainsCache.domains.length === 0) {
        loadDomains();
    }
    
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
    
    // Domain information (only for terms)
    const domainLabel = document.getElementById('modal-domain-label');
    const domainValue = document.getElementById('modal-domain-value');
    const domainElement = document.getElementById('modal-item-domain');
    
    if (entityType === 'glossaryTerm') {
        // Use only what the backend provides for domains
        const domains = itemData.domains;
        if (domains && domains.domain) {
            const domainName = domains.domain.name || domains.domain.urn;
            domainElement.textContent = domainName;
            domainLabel.style.display = 'block';
            domainValue.style.display = 'block';
        } else {
            domainLabel.style.display = 'none';
            domainValue.style.display = 'none';
        }
    } else {
        domainLabel.style.display = 'none';
        domainValue.style.display = 'none';
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
    
    // Ownership information
    const ownersList = document.getElementById('modal-owners-list');
    
    // Use only what the backend provides in the ownership section
    const ownershipData = itemData.ownership;
    
    if (ownershipData && Array.isArray(ownershipData) && ownershipData.length > 0) {
        // Group owners by ownership type using the data as provided by backend
        const ownersByType = {};
        
        ownershipData.forEach(ownerInfo => {
            // Use the data structure as provided by the backend
            const ownershipTypeName = ownerInfo.ownershipType?.name || 'Unknown Type';
            const ownerName = ownerInfo.displayName || ownerInfo.name || 'Unknown Owner';
            const isUser = ownerInfo.urn?.includes(':corpuser:'); // Check URN to determine user vs group
            
            if (!ownersByType[ownershipTypeName]) {
                ownersByType[ownershipTypeName] = [];
            }
            ownersByType[ownershipTypeName].push({
                name: ownerName,
                urn: ownerInfo.urn,
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
    let customProperties = itemData.customProperties || itemData.custom_properties || 
                          itemData.properties?.customProperties || item.customProperties || 
                          item.custom_properties;
    
    // Structured Properties - check multiple possible locations  
    let structuredProperties = itemData.structuredProperties || itemData.structured_properties || 
                              item.structuredProperties || item.structured_properties;
    
    // Custom Properties Section
    if (customProperties && customProperties.length > 0) {
        propertiesHTML += `
            <div class="card mb-3">
                <div class="card-header">
                    <h6 class="card-title mb-0"><i class="fas fa-tags me-2"></i>Custom Properties (${customProperties.length})</h6>
                </div>
                <div class="card-body">
                    <div class="row">
                        ${customProperties.map(prop => `
                            <div class="col-md-6 mb-2">
                                <dl class="row mb-0">
                                    <dt class="col-sm-5 text-truncate" title="${escapeHtml(prop.key || 'Unknown')}">${escapeHtml(prop.key || 'Unknown')}:</dt>
                                    <dd class="col-sm-7 mb-0">
                                        <span class="text-break">${escapeHtml(prop.value || '')}</span>
                                    </dd>
                                </dl>
                            </div>
                        `).join('')}
                    </div>
                </div>
            </div>
        `;
    }
    
    // Structured Properties Section
    if (structuredProperties && structuredProperties.length > 0) {
        propertiesHTML += `
            <div class="card mb-3">
                <div class="card-header">
                    <h6 class="card-title mb-0"><i class="fas fa-cogs me-2"></i>Structured Properties (${structuredProperties.length})</h6>
                </div>
                <div class="card-body">
                    <div class="row">
                        ${structuredProperties.map(prop => {
                            // Handle new backend format with URN, displayName, and values
                            const propertyName = prop.urn || prop.propertyUrn || 'Unknown';
                            const displayName = prop.displayName || prop.qualifiedName || propertyName.split(':').pop() || propertyName;
                            let propertyValue = '';
                            
                            if (prop.values && Array.isArray(prop.values)) {
                                // New format: values is an array of strings/numbers
                                propertyValue = prop.values.filter(v => v !== null && v !== undefined).join(', ');
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
        propertiesDiv.innerHTML = propertiesHTML;
    } else {
        // Show empty cards for both custom and structured properties
        propertiesDiv.innerHTML = `
            <div class="card mb-3">
                <div class="card-header">
                    <h6 class="card-title mb-0"><i class="fas fa-tags me-2"></i>Custom Properties (0)</h6>
                </div>
                <div class="card-body">
                    <div class="text-center py-3 text-muted">
                        <i class="fas fa-tags fa-2x mb-2"></i><br>
                        No custom properties available
                    </div>
                </div>
            </div>
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
    
    // Raw JSON - show the processed data with enhanced domain information
    let rawData = { ...itemData };
    
    // If we have the original GraphQL raw data, include the original domain structure
    if (itemData.raw_data) {
        rawData.original_graphql_data = itemData.raw_data;
    }
    
    // Enhance domain information in the raw data if available
    if (entityType === 'glossaryTerm' && itemData.domains) {
        rawData.domain_information = {
            processed_domains: itemData.domains,
            domain_count: itemData.domains && itemData.domains.domain ? 1 : 0,
            domain_urn: itemData.domains && itemData.domains.domain ? itemData.domains.domain.urn : null,
            domain_name: itemData.domains && itemData.domains.domain ? itemData.domains.domain.name : null
        };
    }
    
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
    
    // Find the folder icon for this node
    const nodeRow = document.querySelector(`[data-node-urn="${nodeUrn}"]`);
    const folderIcon = nodeRow ? nodeRow.querySelector('.fas.fa-folder, .fas.fa-folder-open') : null;
    
    if (isCollapsed) {
        // Expand: show direct children only
        expandIcon.classList.remove('fa-chevron-right');
        expandIcon.classList.add('fa-chevron-down');
        childRows.forEach(row => {
            row.style.display = 'table-row';
        });
        
        // Change folder icon to open when expanded
        if (folderIcon) {
            folderIcon.classList.remove('fa-folder');
            folderIcon.classList.add('fa-folder-open');
        }
    } else {
        // Collapse: hide all descendants recursively
        expandIcon.classList.remove('fa-chevron-down');
        expandIcon.classList.add('fa-chevron-right');
        
        // Recursively collapse all descendants
        collapseAllDescendants(nodeUrn);
        
        // Change folder icon to closed when collapsed
        if (folderIcon) {
            folderIcon.classList.remove('fa-folder-open');
            folderIcon.classList.add('fa-folder');
        }
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
        
        // Update folder icon to closed when collapsed
        const folderIcon = row.querySelector('.fas.fa-folder, .fas.fa-folder-open');
        if (folderIcon) {
            folderIcon.classList.remove('fa-folder-open');
            folderIcon.classList.add('fa-folder');
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
            
            // Update folder icon to open when expanded
            const nodeRow = document.querySelector(`[data-node-urn="${nodeUrn}"]`);
            const folderIcon = nodeRow ? nodeRow.querySelector('.fas.fa-folder, .fas.fa-folder-open') : null;
            if (folderIcon) {
                folderIcon.classList.remove('fa-folder');
                folderIcon.classList.add('fa-folder-open');
            }
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
            
            // Update folder icon to closed when collapsed
            const nodeRow = document.querySelector(`[data-node-urn="${nodeUrn}"]`);
            const folderIcon = nodeRow ? nodeRow.querySelector('.fas.fa-folder, .fas.fa-folder-open') : null;
            if (folderIcon) {
                folderIcon.classList.remove('fa-folder-open');
                folderIcon.classList.add('fa-folder');
            }
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
    // Create a template CSV with example data using new format
    const templateData = [
        {
            name: 'Customer',
            entity_type: 'glossaryTerm',
            description: 'A person or organization that purchases goods or services',
            parent_node_name: 'Business Entities',
            ownership: [
                {
                    owner: { urn: 'urn:li:corpuser:data-owner@company.com', displayName: 'data-owner@company.com' },
                    type: 'urn:li:ownershipType:__system__technical_owner'
                }
            ],
            relationships: [
                { type: 'isA', entity: { name: 'Person' } },
                { type: 'hasA', entity: { name: 'Account' } }
            ],
            custom_properties: [
                { key: 'data_classification', value: 'PII' },
                { key: 'retention_period', value: '7 years' }
            ],
            domain: { name: 'Customer Data' }
        },
        {
            name: 'Business Entities',
            entity_type: 'glossaryNode',
            description: 'Top-level category for business-related terms',
            parent_node_name: '',
            ownership: [
                {
                    owner: { urn: 'urn:li:corpgroup:data-governance-team', displayName: 'Data Governance Team' },
                    type: 'urn:li:ownershipType:__system__data_steward'
                }
            ],
            relationships: [],
            custom_properties: [],
            domain: { name: 'Governance' }
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
        'parent_name',
        'owner_emails',
        'owner_groups',
        'owner_types',
        'hasA_relationships',
        'isA_relationships',
        'custom_properties',
        'domain_name'
    ];
    
    const csvRows = ['"' + headers.join('","') + '"'];
    
    items.forEach(item => {
        // Determine type
        let type = 'term';
        if (item.entity_type === 'glossaryNode' || item.urn?.includes('glossaryNode')) {
            type = 'node';
        }
        
        // Get parent information by name
        let parentName = '';
        if (item.parent_node_name) {
            parentName = item.parent_node_name;
        } else if (item.parentNodes?.nodes?.[0]?.name) {
            parentName = item.parentNodes.nodes[0].name;
        } else if (item.parent_name) {
            parentName = item.parent_name;
        }
        
        // Get owner information with emails and groups separated
        let ownerEmails = [];
        let ownerGroups = [];
        let ownerTypes = [];
        
        if (item.ownership && item.ownership.length > 0) {
            item.ownership.forEach(owner => {
                const ownerData = owner.owner || owner;
                const ownerUrn = ownerData.urn || owner.urn;
                const ownerType = owner.type || owner.ownershipType || 'urn:li:ownershipType:__system__technical_owner';
                
                if (ownerUrn?.includes('corpuser')) {
                    // Extract email from URN or use display name
                    let email = '';
                    if (ownerUrn.includes('@')) {
                        // Extract email from URN like urn:li:corpuser:purnima.garg@apptware.com
                        email = ownerUrn.split(':').pop();
                    } else {
                        // Use display name if available
                        email = ownerData.displayName || ownerData.name || ownerUrn;
                    }
                    ownerEmails.push(email);
                    ownerTypes.push(ownerType);
                } else if (ownerUrn?.includes('corpgroup')) {
                    // Get group name
                    const groupName = ownerData.displayName || ownerData.name || ownerUrn.split(':').pop();
                    ownerGroups.push(groupName);
                    ownerTypes.push(ownerType);
                }
            });
        }
        
        // Get relationships separated by type
        let hasARelationships = [];
        let isARelationships = [];
        
        if (item.relationships && item.relationships.length > 0) {
            item.relationships.forEach(rel => {
                const entityName = rel.entity?.name || rel.entity?.displayName || rel.name || 'Unknown';
                if (rel.type === 'hasA') {
                    hasARelationships.push(entityName);
                } else if (rel.type === 'isA') {
                    isARelationships.push(entityName);
                }
            });
        }
        
        // Get custom properties (keep same as they are now)
        let customProperties = {};
        if (item.custom_properties && item.custom_properties.length > 0) {
            item.custom_properties.forEach(prop => {
                customProperties[prop.key] = prop.value;
            });
        }
        
        // Get domain name
        let domainName = '';
        if (item.domain?.name) {
            domainName = item.domain.name;
        } else if (item.domain_name) {
            domainName = item.domain_name;
        }
        
        const row = [
            escapeCSVField(item.name || ''),
            escapeCSVField(type),
            escapeCSVField(item.description || ''),
            escapeCSVField(parentName),
            escapeCSVField(ownerEmails.join(', ')),
            escapeCSVField(ownerGroups.join(', ')),
            escapeCSVField(ownerTypes.join(', ')),
            escapeCSVField(hasARelationships.join(', ')),
            escapeCSVField(isARelationships.join(', ')),
            escapeCSVField(JSON.stringify(customProperties)),
            escapeCSVField(domainName)
        ];
        
        csvRows.push('"' + row.join('","') + '"');
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
        
        console.log('CSV upload response status:', response.status, response.statusText);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        // Check if the response is actually JSON
        const contentType = response.headers.get('content-type');
        if (!contentType || !contentType.includes('application/json')) {
            throw new Error(`Expected JSON response but got: ${contentType}`);
        }
        
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
        } else if (clickedElement.classList.contains('delete-remote-item') || clickedElement.closest('.delete-remote-item')) {
            // Delete Remote Item button clicked
            console.log('Delete Remote Item clicked for item:', itemData);
            deleteRemoteItem(itemData);
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
    const itemType = determineEntityType(item);
    if (itemType === 'glossaryNode') {
        editNode(item);
    } else if (itemType === 'glossaryTerm') {
        editTerm(item);
                } else {
        showNotification('error', 'Unknown item type for editing');
    }
}

function editNode(node) {
    console.log('editNode called with:', node);
    
    // Get node data
    const nodeData = node.combined || node;
    const nodeId = nodeData.id;
    
    if (!nodeId) {
        console.error('Cannot edit node without a database ID:', nodeData);
        showNotification('error', 'Error editing node: Missing node database ID.');
        return;
    }
    
    // Ensure users and groups are loaded
    if (usersAndGroupsCache.users.length === 0 && usersAndGroupsCache.groups.length === 0) {
        loadUsersAndGroups();
    }
    
    // Populate the form with existing data
    document.getElementById('node-name').value = nodeData.name || '';
    document.getElementById('node-description').value = nodeData.description || '';
    
    // Load and populate parent node dropdown
    if (!window.glossaryNodesCache || window.glossaryNodesCache.length === 0) {
        loadGlossaryNodes().then(() => {
            populateParentNodeDropdowns();
            // Set the current parent if it exists
            if (nodeData.parent_id) {
                document.getElementById('node-parent').value = nodeData.parent_id;
            }
        });
        } else {
        populateParentNodeDropdowns();
        // Set the current parent if it exists
        if (nodeData.parent_id) {
            document.getElementById('node-parent').value = nodeData.parent_id;
        }
    }
    
    // Clear existing ownership sections and setup interface
    setupNodeOwnershipInterface();
    
    // Wait for users/groups to load if not already loaded, then populate ownership data
    const populateOwnership = () => {
        const ownershipData = nodeData.ownership_data || nodeData.ownership;
        console.log('Populating ownership for edit mode:', ownershipData);
        
        // Clear existing ownership sections
        const ownershipContainer = document.getElementById('node-ownership-sections-container');
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
                addNodeOwnershipSection();
                
                // Get the last added section (the one we just created)
                const sections = document.querySelectorAll('#node-ownership-sections-container .card');
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
            hideNodeOwnershipSectionIfEmpty();
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
    const form = document.getElementById('createNodeForm');
    form.dataset.editMode = 'true';
    form.dataset.nodeId = nodeId;
    
    // Change form action to edit endpoint
    form.action = `/metadata/glossary/nodes/${nodeId}/edit/`;
    
    // Update modal title and button text
    document.querySelector('#createNodeModal .modal-title').textContent = 'Edit Glossary Node';
    document.querySelector('#createNodeModal .btn-primary').textContent = 'Update Node';
    
    // Show the modal
    const modal = new bootstrap.Modal(document.getElementById('createNodeModal'));
    modal.show();
}

function editTerm(term) {
    console.log('editTerm called with:', term);
    
    // Get term data
    const termData = term.combined || term;
    const termId = termData.id;
    
    if (!termId) {
        console.error('Cannot edit term without a database ID:', termData);
        showNotification('error', 'Error editing term: Missing term database ID.');
        return;
    }
    
    // Ensure users and groups are loaded
    if (usersAndGroupsCache.users.length === 0 && usersAndGroupsCache.groups.length === 0) {
        loadUsersAndGroups();
    }
    
    // Populate the form with existing data
    document.getElementById('term-name').value = termData.name || '';
    document.getElementById('term-description').value = termData.description || '';
    
    // Load and populate parent node dropdown
    if (!window.glossaryNodesCache || window.glossaryNodesCache.length === 0) {
        loadGlossaryNodes().then(() => {
            populateParentNodeDropdowns();
            // Set the current parent if it exists
            if (termData.parent_id) {
                document.getElementById('term-parent-node').value = termData.parent_id;
            }
        });
    } else {
        populateParentNodeDropdowns();
        // Set the current parent if it exists
        if (termData.parent_id) {
            document.getElementById('term-parent-node').value = termData.parent_id;
        }
    }
    
    // Load and populate domain dropdown
    const domainCache = getCurrentDomainCache();
    if (!domainCache.domains || domainCache.domains.length === 0) {
        loadDomains().then(() => {
            populateDomainDropdown();
            // Set the current domain if it exists
            if (termData.domain_urn) {
                document.getElementById('term-domain').value = termData.domain_urn;
            }
        });
    } else {
        populateDomainDropdown();
        // Set the current domain if it exists
        if (termData.domain_urn) {
            document.getElementById('term-domain').value = termData.domain_urn;
        }
    }
    
    // Clear existing ownership sections and setup interface
    setupTermOwnershipInterface();
    
    // Wait for users/groups to load if not already loaded, then populate ownership data
    const populateOwnership = () => {
        const ownershipData = termData.ownership_data || termData.ownership;
        console.log('Populating ownership for edit mode:', ownershipData);
        
        // Clear existing ownership sections
        const ownershipContainer = document.getElementById('term-ownership-sections-container');
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
                addTermOwnershipSection();
                
                // Get the last added section (the one we just created)
                const sections = document.querySelectorAll('#term-ownership-sections-container .card');
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
            hideTermOwnershipSectionIfEmpty();
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
    const form = document.getElementById('createTermForm');
    form.dataset.editMode = 'true';
    form.dataset.termId = termId;
    
    // Change form action to edit endpoint
    form.action = `/metadata/glossary/terms/${termId}/edit/`;
    
    // Update modal title and button text
    document.querySelector('#createTermModal .modal-title').textContent = 'Edit Glossary Term';
    document.querySelector('#createTermModal .btn-primary').textContent = 'Update Term';
    
    // Show the modal
    const modal = new bootstrap.Modal(document.getElementById('createTermModal'));
    modal.show();
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
        
        console.log('Users-groups response status:', response.status, response.statusText);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        // Check if the response is actually JSON
        const contentType = response.headers.get('content-type');
        if (!contentType || !contentType.includes('application/json')) {
            throw new Error(`Expected JSON response but got: ${contentType}`);
        }
        
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

// Load domains from both remote DataHub and local database
async function loadDomains() {
    console.log('Loading domains for glossary...');
    try {
        const csrfToken = getCsrfToken();
        console.log('Using CSRF token for domains:', csrfToken);
        
        const response = await fetch('/metadata/api/search-domains/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': csrfToken
            },
            body: JSON.stringify({
                input: {
                    start: 0,
                    count: 1000, // Get a large number of domains
                    query: "*",
                    types: ["DOMAIN"]
                }
            })
        });
        
        console.log('Domains response status:', response.status, response.statusText);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const contentType = response.headers.get('content-type');
        if (!contentType || !contentType.includes('application/json')) {
            throw new Error(`Expected JSON response but got: ${contentType}`);
        }
        
        const data = await response.json();
        console.log('Domains API response:', data);
        
        if (data.success) {
            // Use the domain cache
            const cache = getCurrentDomainCache();
            
            // Process remote domains from GraphQL response
            const remoteDomains = [];
            if (data.data && data.data.searchAcrossEntities && data.data.searchAcrossEntities.searchResults) {
                data.data.searchAcrossEntities.searchResults.forEach(result => {
                    if (result.entity && result.entity.type === 'DOMAIN') {
                        remoteDomains.push({
                            urn: result.entity.urn,
                            name: result.entity.properties?.name || 'Unknown Domain',
                            description: result.entity.properties?.description || '',
                            type: 'remote'
                        });
                    }
                });
            }
            
            // Get local domains
            const localDomains = data.local_domains || [];
            
            // Combine and deduplicate domains
            const allDomains = [];
            const seenUrns = new Set();
            
            // Add remote domains first
            remoteDomains.forEach(domain => {
                if (!seenUrns.has(domain.urn)) {
                    allDomains.push(domain);
                    seenUrns.add(domain.urn);
                }
            });
            
            // Add local domains that aren't already in remote
            localDomains.forEach(domain => {
                if (!seenUrns.has(domain.urn)) {
                    allDomains.push({
                        ...domain,
                        type: 'local'
                    });
                    seenUrns.add(domain.urn);
                }
            });
            
            cache.domains = allDomains;
            cache.lastFetched = Date.now();
            console.log('Loaded domains:', cache.domains);
        } else {
            throw new Error(data.error || 'Failed to load domains');
        }
    } catch (error) {
        console.error('Error loading domains:', error);
        // Don't throw error - domains are not critical for basic functionality
        const cache = getCurrentDomainCache();
        cache.domains = [];
        cache.lastFetched = Date.now();
    }
}

// Load glossary nodes for parent selection dropdowns
async function loadGlossaryNodes() {
    console.log('Loading glossary nodes for parent selection...');
    try {
        const response = await fetch('/metadata/glossary/data/');
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        
        if (data.success && data.data) {
            // Extract all nodes from the data
            const allNodes = [];
            
            // Add synced nodes
            if (data.data.synced_items) {
                data.data.synced_items.forEach(item => {
                    if (item.local && item.local.type === 'node') {
                        allNodes.push({
                            id: item.local.id,
                            name: item.local.name,
                            urn: item.local.urn,
                            type: 'synced'
                        });
                    }
                });
            }
            
            // Add local-only nodes
            if (data.data.local_only_items) {
                data.data.local_only_items.forEach(item => {
                    if (item.type === 'node') {
                        allNodes.push({
                            id: item.id,
                            name: item.name,
                            urn: item.urn,
                            type: 'local'
                        });
                    }
                });
            }
            
            // Add remote-only nodes
            if (data.data.remote_only_items) {
                data.data.remote_only_items.forEach(item => {
                    if (item.type === 'node') {
                        allNodes.push({
                            id: item.id || 'remote-' + Date.now(),
                            name: item.name,
                            urn: item.urn,
                            type: 'remote'
                        });
                    }
                });
            }
            
            // Store in a global variable for easy access
            window.glossaryNodesCache = allNodes;
            console.log('Loaded glossary nodes for parent selection:', allNodes);
            
            // Populate existing dropdowns
            populateParentNodeDropdowns();
            
            return allNodes;
        } else {
            throw new Error(data.error || 'Failed to load glossary data');
        }
    } catch (error) {
        console.error('Error loading glossary nodes:', error);
        window.glossaryNodesCache = [];
        return [];
    }
}

// Populate parent node dropdowns with available nodes
function populateParentNodeDropdowns() {
    const nodes = window.glossaryNodesCache || [];
    
    // Populate node parent dropdown
    const nodeParentSelect = document.getElementById('node-parent');
    if (nodeParentSelect) {
        // Clear existing options except the first one
        while (nodeParentSelect.children.length > 1) {
            nodeParentSelect.removeChild(nodeParentSelect.lastChild);
        }
        
        // Add node options
        nodes.forEach(node => {
            const option = document.createElement('option');
            option.value = node.id;
            option.textContent = node.name;
            nodeParentSelect.appendChild(option);
        });
    }
    
    // Populate term parent dropdown
    const termParentSelect = document.getElementById('term-parent-node');
    if (termParentSelect) {
        // Clear existing options except the first one
        while (termParentSelect.children.length > 1) {
            termParentSelect.removeChild(termParentSelect.lastChild);
        }
        
        // Add node options
        nodes.forEach(node => {
            const option = document.createElement('option');
            option.value = node.id;
            option.textContent = node.name;
            termParentSelect.appendChild(option);
        });
    }
}

// Populate domain dropdown with available domains
function populateDomainDropdown() {
    const cache = getCurrentDomainCache();
    const domains = cache.domains || [];
    
    const domainSelect = document.getElementById('term-domain');
    if (domainSelect) {
        // Clear existing options except the first one
        while (domainSelect.children.length > 1) {
            domainSelect.removeChild(domainSelect.lastChild);
        }
        
        // Add domain options
        domains.forEach(domain => {
            const option = document.createElement('option');
            option.value = domain.urn;
            option.textContent = domain.name;
            domainSelect.appendChild(option);
        });
        
        console.log('Populated domain dropdown with', domains.length, 'domains');
    }
}

// Setup the ownership interface for nodes
function setupNodeOwnershipInterface() {
    const container = document.getElementById('node-ownership-sections-container');
    const addButton = document.getElementById('add-node-ownership-section');
    
    if (!container || !addButton) {
        console.warn('Node ownership interface elements not found');
        console.log('Container:', container);
        console.log('Add button:', addButton);
        return;
    }
    
    console.log('Setting up node ownership interface...');
    console.log('Container found:', container);
    console.log('Add button found:', addButton);
    
    // Clear existing sections
    container.innerHTML = '';
    
    // Remove any existing event listeners by cloning and replacing
    const newAddButton = addButton.cloneNode(true);
    addButton.parentNode.replaceChild(newAddButton, addButton);
    
    console.log('Button replaced with new button:', newAddButton);
    
    // Setup add ownership section button with a single event listener
    newAddButton.addEventListener('click', function(e) {
        e.preventDefault();
        e.stopPropagation();
        console.log('Add node ownership section clicked');
        addNodeOwnershipSection();
        showNodeOwnershipSection();
        // Add a subtle animation to the button
        this.style.transform = 'scale(0.95)';
        setTimeout(() => {
            this.style.transform = 'scale(1)';
        }, 150);
    });
    
    console.log('Node ownership interface setup complete');
}

// Setup the ownership interface for terms
function setupTermOwnershipInterface() {
    const container = document.getElementById('term-ownership-sections-container');
    const addButton = document.getElementById('add-term-ownership-section');
    
    if (!container || !addButton) {
        console.warn('Term ownership interface elements not found');
        console.log('Container:', container);
        console.log('Add button:', addButton);
        return;
    }
    
    console.log('Setting up term ownership interface...');
    console.log('Container found:', container);
    console.log('Add button found:', addButton);
    
    // Clear existing sections
    container.innerHTML = '';
    
    // Remove any existing event listeners by cloning and replacing
    const newAddButton = addButton.cloneNode(true);
    addButton.parentNode.replaceChild(newAddButton, addButton);
    
    console.log('Button replaced with new button:', newAddButton);
    
    // Setup add ownership section button with a single event listener
    newAddButton.addEventListener('click', function(e) {
        e.preventDefault();
        e.stopPropagation();
        console.log('Add term ownership section clicked');
        addTermOwnershipSection();
        showTermOwnershipSection();
        // Add a subtle animation to the button
        this.style.transform = 'scale(0.95)';
        setTimeout(() => {
            this.style.transform = 'scale(1)';
        }, 150);
    });
    
    console.log('Term ownership interface setup complete');
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
        refreshAllNodeOwnershipTypeDropdowns();
    });
    
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
        refreshAllTermOwnershipTypeDropdowns();
    });
    
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
        // Clean up Select2 instances in this section
        const select2Elements = section.querySelectorAll('.select2-hidden-accessible');
        select2Elements.forEach(element => {
            $(element).select2('destroy');
        });
        
        section.remove();
        updateNodeRemoveButtons();
        
        // Check if container is now empty and hide ownership section if so
        const container = document.getElementById('node-ownership-sections-container');
        if (container && container.children.length === 0) {
            hideNodeOwnershipSectionIfEmpty();
        }
        
        // Refresh all ownership type dropdowns to make removed options available again
        refreshAllNodeOwnershipTypeDropdowns();
    }
}

function removeTermSection(sectionId) {
    const section = document.getElementById(sectionId);
    if (section) {
        // Clean up Select2 instances in this section
        const select2Elements = section.querySelectorAll('.select2-hidden-accessible');
        select2Elements.forEach(element => {
            $(element).select2('destroy');
        });
        
        section.remove();
        updateTermRemoveButtons();
        
        // Check if container is now empty and hide ownership section if so
        const container = document.getElementById('term-ownership-sections-container');
        if (container && container.children.length === 0) {
            hideTermOwnershipSectionIfEmpty();
        }
        
        // Refresh all ownership type dropdowns to make removed options available again
        refreshAllTermOwnershipTypeDropdowns();
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



/**
 * Delete remote item from DataHub
 * @param {Object} item - The item object
 */
function deleteRemoteItem(item) {
    console.log('deleteRemoteItem called with:', item);
    
    if (!item.urn) {
        console.error('Cannot delete remote item without URN:', item);
        showError('Error deleting item: Missing URN.');
        return;
    }
    
    const itemType = item.type === 'node' ? 'Node' : 'Term';
    const itemName = item.name || 'Unknown';
    
    // Check if item can be deleted (nodes must have no children)
    const entityType = determineEntityType(item);
    if (entityType === 'glossaryNode' && item.children && item.children.length > 0) {
        showNotification('error', `Cannot delete node "${itemName}" because it has child items. Please delete all child items first.`);
        return;
    }
    
    if (!confirm(`Are you sure you want to permanently delete the ${itemType.toLowerCase()} "${itemName}" from DataHub? This action cannot be undone.`)) {
        return;
    }
    
    // Show loading notification
    showNotification('info', `Deleting ${itemType.toLowerCase()} "${itemName}" from DataHub...`);
    
    // Use the DataHub REST client to delete the item
    // This would need to be implemented via an endpoint that calls the DataHub API
    const deleteUrl = `/metadata/glossary/delete-remote/`;
    
    fetch(deleteUrl, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCsrfToken()
        },
        body: JSON.stringify({
            urn: item.urn,
            type: itemType.toLowerCase()
        })
    })
    .then(response => {
        console.log('Delete remote response status:', response.status, response.statusText);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        // Check if the response is actually JSON
        const contentType = response.headers.get('content-type');
        if (!contentType || !contentType.includes('application/json')) {
            throw new Error(`Expected JSON response but got: ${contentType}`);
        }
        
        return response.json();
    })
    .then(data => {
        console.log('Delete remote item response:', data);
        if (data.success) {
            showNotification('success', data.message || `${itemType} deleted from DataHub successfully`);
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
        console.error('Error deleting remote item:', error);
        showNotification('error', `Error deleting ${itemType.toLowerCase()}: ${error.message}`);
    });
}

// Format owner option for Select2
function formatOwnerOption(option) {
    if (!option.id) return option.text;
    
    const isGroup = option.id.includes(':corpGroup:');
    const icon = isGroup ? 'fas fa-users' : 'fas fa-user';
    const type = isGroup ? 'Group' : 'User';
    
    return $(`<span><i class="${icon} me-2"></i>${option.text} <small class="text-muted">(${type})</small></span>`);
}

// Format owner selection for Select2
function formatOwnerSelection(option) {
    if (!option.id) return option.text;
    
    const isGroup = option.id.includes(':corpGroup:');
    const icon = isGroup ? 'fas fa-users' : 'fas fa-user';
    
    return $(`<span><i class="${icon} me-1"></i>${option.text}</span>`);
}

// Refresh ownership type dropdowns for nodes
function refreshAllNodeOwnershipTypeDropdowns() {
    const container = document.getElementById('node-ownership-sections-container');
    if (!container) return;
    
    const ownershipTypeSelects = container.querySelectorAll('.ownership-type-select');
    ownershipTypeSelects.forEach(select => {
        const currentValue = select.value;
        populateOwnershipTypeSelect(select);
        select.value = currentValue; // Restore the current selection
    });
}

// Refresh ownership type dropdowns for terms
function refreshAllTermOwnershipTypeDropdowns() {
    const container = document.getElementById('term-ownership-sections-container');
    if (!container) return;
    
    const ownershipTypeSelects = container.querySelectorAll('.ownership-type-select');
    ownershipTypeSelects.forEach(select => {
        const currentValue = select.value;
        populateOwnershipTypeSelect(select);
        select.value = currentValue; // Restore the current selection
    });
}

// Global sort state
let currentSort = {
    column: null,
    direction: 'asc',
    tabType: null
};

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

// Get sort value from glossary item data
function getSortValue(item, column) {
    const itemData = item.combined || item;
    
    switch(column) {
        case 'name':
            return (itemData.name || '').toLowerCase();
        case 'description':
            return (itemData.description || '').toLowerCase();
        case 'domain':
            return (itemData.domain_name || itemData.domain || '').toLowerCase();
        case 'owners_count':
            return itemData.owners_count || 0;
        case 'custom_properties_count':
            return itemData.custom_properties_count || 0;
        case 'structured_properties_count':
            return itemData.structured_properties_count || 0;
        case 'deprecated':
            return itemData.deprecated ? 1 : 0;
        case 'sync_status':
            const syncStatus = itemData.sync_status || 'UNKNOWN';
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
            return (itemData.urn || '').toLowerCase();
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

/**
 * Deploy item to DataHub
 * @param {Object} item - The item object
 */
async function deployToDataHub(item, suppressReload = false) {
    console.log('deployToDataHub called with:', item);
    
    if (!item.id) {
        console.error('Cannot deploy item without an ID:', item);
        showNotification('error', 'Error deploying item: Missing item ID.');
        throw new Error('Missing item ID');
    }
    
    const itemType = item.type === 'node' ? 'Node' : 'Term';
    const itemName = item.name || 'Unknown';
    
    // Show loading notification
    showNotification('info', `Deploying ${itemType.toLowerCase()} "${itemName}" to DataHub...`);
    
    // Make the API call to deploy this item to DataHub
    const deployUrl = item.type === 'node' ? 
        `/metadata/glossary/nodes/${item.id}/deploy/` : 
        `/metadata/glossary/terms/${item.id}/deploy/`;
    
    try {
        const response = await fetch(deployUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCsrfToken(),
            },
        });
        
        const data = await response.json();
        
        if (response.ok && data.success) {
            showNotification('success', data.message || `${itemType} "${itemName}" deployed successfully!`);
            
            if (!suppressReload) {
                setTimeout(() => {
                    if (typeof loadGlossaryData === 'function') {
                        loadGlossaryData();
                    } else {
                        window.location.reload();
                    }
                }, 1000);
            }
        } else {
            const errorMessage = data.error || data.message || 'Unknown error occurred';
            showNotification('error', `Error deploying ${itemType.toLowerCase()}: ${errorMessage}`);
            throw new Error(errorMessage);
        }
    } catch (error) {
        console.error(`Error deploying ${itemType.toLowerCase()}:`, error);
        showNotification('error', `Error deploying ${itemType.toLowerCase()}: ${error.message}`);
        throw error;
    }
}

/**
 * Resync an item from DataHub
 * @param {Object} item - The item object
 */
async function resyncItem(item, suppressReload = false) {
    console.log('resyncItem called with:', item);
    
    if (!item.id) {
        console.error('Cannot resync item without an ID:', item);
        showNotification('error', 'Error resyncing item: Missing item ID.');
        throw new Error('Missing item ID');
    }
    
    const itemType = item.type === 'node' ? 'Node' : 'Term';
    const itemName = item.name || 'Unknown';
    
    // Show loading notification
    showNotification('info', `Resyncing ${itemType.toLowerCase()} "${itemName}" from DataHub...`);
    
    // Make the API call to resync this item from DataHub
    const resyncUrl = item.type === 'node' ? 
        `/metadata/glossary/nodes/${item.id}/resync/` : 
        `/metadata/glossary/terms/${item.id}/resync/`;
    
    try {
        const response = await fetch(resyncUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCsrfToken(),
            },
        });
        
        const data = await response.json();
        
        if (response.ok && data.success) {
            showNotification('success', data.message || `${itemType} "${itemName}" resynced successfully!`);
            
            if (!suppressReload) {
                setTimeout(() => {
                    if (typeof loadGlossaryData === 'function') {
                        loadGlossaryData();
                    } else {
                        window.location.reload();
                    }
                }, 1000);
            }
        } else {
            const errorMessage = data.error || data.message || 'Unknown error occurred';
            showNotification('error', `Error resyncing ${itemType.toLowerCase()}: ${errorMessage}`);
            throw new Error(errorMessage);
        }
    } catch (error) {
        console.error(`Error resyncing ${itemType.toLowerCase()}:`, error);
        showNotification('error', `Error resyncing ${itemType.toLowerCase()}: ${error.message}`);
        throw error;
    }
}

/**
 * Sync item to local
 * @param {Object} item - The item object
 */
async function syncToLocal(item, suppressReload = false) {
    console.log('syncToLocal called with:', item);
    
    if (!item.urn) {
        console.error('Cannot sync item without a URN:', item);
        showNotification('error', 'Error syncing item to local: Missing item URN.');
        throw new Error('Missing item URN');
    }
    
    const itemType = item.type === 'node' ? 'Node' : 'Term';
    const itemName = item.name || 'Unknown';
    
    // Show loading notification
    showNotification('info', `Syncing ${itemType.toLowerCase()} "${itemName}" to local...`);
    
    // Make the API call to sync this item to local
    const syncUrl = '/metadata/glossary/pull/';
    
    try {
        const formData = new FormData();
        if (item.type === 'node') {
            formData.append('node_urns', item.urn);
        } else {
            formData.append('term_urns', item.urn);
        }
        
        const response = await fetch(syncUrl, {
            method: 'POST',
            headers: {
                'X-CSRFToken': getCsrfToken(),
                'X-Requested-With': 'XMLHttpRequest',
                'Accept': 'application/json',
            },
            body: formData,
        });
        
        const data = await response.json();
        
        if (response.ok && data.success) {
            showNotification('success', data.message || `${itemType} "${itemName}" synced to local successfully!`);
            
            if (!suppressReload) {
                setTimeout(() => {
                    if (typeof loadGlossaryData === 'function') {
                        loadGlossaryData();
                    } else {
                        window.location.reload();
                    }
                }, 1000);
            }
        } else {
            const errorMessage = data.error || data.message || 'Unknown error occurred';
            showNotification('error', `Error syncing ${itemType.toLowerCase()} to local: ${errorMessage}`);
            throw new Error(errorMessage);
        }
    } catch (error) {
        console.error(`Error syncing ${itemType.toLowerCase()} to local:`, error);
        showNotification('error', `Error syncing ${itemType.toLowerCase()} to local: ${error.message}`);
        throw error;
    }
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
    
    const itemType = item.type === 'node' ? 'Node' : 'Term';
    const itemName = item.name || 'Unknown';
    
    // Check if this is a remote-only item that needs to be staged directly
    if (item.sync_status === 'REMOTE_ONLY' || !item.id) {
        console.log(`Item "${itemName}" is remote-only, staging directly...`);
        
        // Show loading notification
        showNotification('info', `Adding remote ${itemType.toLowerCase()} "${itemName}" to staged changes...`);
        
        // Get current environment and mutation from global state or settings
        const currentEnvironment = window.currentEnvironment || { name: 'dev' };
        const mutationName = currentEnvironment.mutation_name || null;
        
        // Use the remote staging endpoint
        fetch('/metadata/glossary/remote/stage_changes/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCsrfToken()
            },
            body: JSON.stringify({
                item_data: item,
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
            console.log('Add remote to staged changes response:', data);
            if (data.status === 'success') {
                showNotification('success', data.message || `Remote ${itemType} added to staged changes successfully`);
                if (data.files_created && data.files_created.length > 0) {
                    console.log('Created files:', data.files_created);
                }
            } else {
                throw new Error(data.error || 'Unknown error occurred');
            }
        })
        .catch(error => {
            console.error('Error adding remote item to staged changes:', error);
            showNotification('error', `Error adding remote ${itemType.toLowerCase()} to staged changes: ${error.message}`);
        });
        return;
    }
    
    if (!item.id) {
        console.error('Cannot add item to staged changes without an ID:', item);
        showNotification('error', 'Error adding to staged changes: Missing item ID.');
        return;
    }
    
    // Show loading notification
    showNotification('info', `Adding ${itemType.toLowerCase()} "${itemName}" to staged changes...`);
    
    // Get current environment and mutation from global state or settings
    const currentEnvironment = window.currentEnvironment || { name: 'dev' };
    const mutationName = currentEnvironment.mutation_name || null;
    
    // Use the staged changes endpoint
    const stageUrl = item.type === 'node' ? 
        `/metadata/glossary/nodes/${item.id}/stage_changes/` : 
        `/metadata/glossary/terms/${item.id}/stage_changes/`;
    
    fetch(stageUrl, {
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
        console.log('Add to staged changes response:', data);
        if (data.status === 'success') {
            showNotification('success', data.message || `${itemType} added to staged changes successfully`);
            if (data.files_created && data.files_created.length > 0) {
                console.log('Created files:', data.files_created);
            }
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
function deleteLocalItem(item, suppressNotification = false) {
    console.log('deleteLocalItem called with:', item);
    
    if (!item.id) {
        console.error('Cannot delete item without an ID:', item);
        if (!suppressNotification) showNotification('error', 'Error deleting item: Missing item ID.');
        return Promise.reject(new Error('Missing item ID'));
    }
    
    const itemType = item.type === 'node' ? 'Node' : 'Term';
    const itemName = item.name || 'Unknown';
    
    if (!suppressNotification && !confirm(`Are you sure you want to delete the local ${itemType.toLowerCase()} "${itemName}"? This action cannot be undone.`)) {
        return Promise.reject(new Error('User cancelled deletion'));
    }
    
    // Show loading notification
    if (!suppressNotification) showNotification('info', `Deleting local ${itemType.toLowerCase()} "${itemName}"...`);
    
    // Use the correct delete endpoint
    const deleteUrl = item.type === 'node' ? 
        `/metadata/glossary/nodes/${item.id}/delete/` : 
        `/metadata/glossary/terms/${item.id}/delete/`;
    
    return fetch(deleteUrl, {
        method: 'DELETE',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCsrfToken()
        }
    })
    .then(response => {
        console.log('Delete response status:', response.status, response.statusText);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        const contentType = response.headers.get('content-type');
        if (!contentType || !contentType.includes('application/json')) {
            throw new Error(`Expected JSON response but got: ${contentType}`);
        }
        return response.json();
    })
    .then(data => {
        console.log('Delete local item response:', data);
        if (data.success) {
            if (!suppressNotification) {
                showNotification('success', data.message || `${itemType} deleted successfully`);
                setTimeout(() => {
                    if (typeof loadGlossaryData === 'function') {
                        loadGlossaryData();
                    } else {
                        window.location.reload();
                    }
                }, 1000);
            }
            return true;
        } else {
            throw new Error(data.error || 'Unknown error occurred');
        }
    })
    .catch(error => {
        console.error('Error deleting local item:', error);
        if (!suppressNotification) showNotification('error', `Error deleting ${itemType.toLowerCase()}: ${error.message}`);
        return Promise.reject(error);
    });
}

/**
 * Show notification to user
 * @param {string} type - The notification type (success, error, info, warning)
 * @param {string} message - The message to display
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
    let bgClass, icon, title;
    
    if (type === 'success') {
        bgClass = 'bg-success';
        icon = 'fa-check-circle';
        title = 'Success';
    } else if (type === 'info') {
        bgClass = 'bg-info';
        icon = 'fa-info-circle';
        title = 'Info';
    } else if (type === 'warning') {
        bgClass = 'bg-warning';
        icon = 'fa-exclamation-triangle';
        title = 'Warning';
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

// Checkbox and bulk action handlers with hierarchical selection
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

/**
 * Recursively select all descendants when a checkbox is selected
 * @param {HTMLElement} checkbox - The checkbox that was selected
 * @param {HTMLElement} content - The content container
 */
function selectAllDescendants(checkbox, content) {
    const row = checkbox.closest('tr');
    if (!row) return;
    
    // Get the current item's URN/ID
    const itemUrn = row.dataset.nodeUrn || row.dataset.termUrn;
    if (!itemUrn) return;
    
    // Find all direct children (items that have this item as their parent)
    const childRows = content.querySelectorAll(`[data-parent-node="${itemUrn}"]`);
    childRows.forEach(childRow => {
        const childCheckbox = childRow.querySelector('.item-checkbox');
        if (childCheckbox && !childCheckbox.checked) {
            childCheckbox.checked = true;
            // Recursively select all descendants of this child
            selectAllDescendants(childCheckbox, content);
        }
    });
}

/**
 * Recursively unselect all descendants when a checkbox is unselected
 * @param {HTMLElement} checkbox - The checkbox that was unselected
 * @param {HTMLElement} content - The content container
 */
function unselectAllDescendants(checkbox, content) {
    const row = checkbox.closest('tr');
    if (!row) return;
    
    // Get the current item's URN/ID
    const itemUrn = row.dataset.nodeUrn || row.dataset.termUrn;
    if (!itemUrn) return;
    
    // Find all direct children (items that have this item as their parent)
    const childRows = content.querySelectorAll(`[data-parent-node="${itemUrn}"]`);
    childRows.forEach(childRow => {
        const childCheckbox = childRow.querySelector('.item-checkbox');
        if (childCheckbox && childCheckbox.checked) {
            childCheckbox.checked = false;
            // Recursively unselect all descendants of this child
            unselectAllDescendants(childCheckbox, content);
        }
    });
}

/**
 * Recursively select parent checkboxes when a child is selected
 * @param {HTMLElement} checkbox - The checkbox that was selected
 * @param {HTMLElement} content - The content container
 */
function selectParentCheckboxes(checkbox, content) {
    const row = checkbox.closest('tr');
    if (!row) return;
    
    // Get the parent node URN/ID from data attributes
    const parentNodeUrn = row.dataset.parentNode;
    if (!parentNodeUrn) return; // This is a root item
    
    // Find the parent row
    const parentRow = content.querySelector(`[data-node-urn="${parentNodeUrn}"]`);
    if (!parentRow) return; // Parent not found in current view
    
    // Get the parent's checkbox
    const parentCheckbox = parentRow.querySelector('.item-checkbox');
    if (!parentCheckbox) return;
    
    // Check the parent checkbox if it's not already checked
    if (!parentCheckbox.checked) {
        parentCheckbox.checked = true;
        // Recursively check the parent's parents
        selectParentCheckboxes(parentCheckbox, content);
    }
}

/**
 * Update parent checkbox states based on children selection
 * @param {HTMLElement} checkbox - The checkbox that was unselected
 * @param {HTMLElement} content - The content container
 */
function updateParentCheckboxStates(checkbox, content) {
    const row = checkbox.closest('tr');
    if (!row) return;
    
    // Get the parent node URN/ID from data attributes
    const parentNodeUrn = row.dataset.parentNode;
    if (!parentNodeUrn) return; // This is a root item
    
    // Find the parent row
    const parentRow = content.querySelector(`[data-node-urn="${parentNodeUrn}"]`);
    if (!parentRow) return; // Parent not found in current view
    
    // Get the parent's checkbox
    const parentCheckbox = parentRow.querySelector('.item-checkbox');
    if (!parentCheckbox) return;
    
    // Check if any siblings are still selected
    const siblingRows = content.querySelectorAll(`[data-parent-node="${parentNodeUrn}"]`);
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

function getAllSelectedItems() {
    // Get selected items from all tabs that support local deletion (synced and local)
    const allSelectedItems = [];
    const tabs = ['synced', 'local'];
    
    tabs.forEach(tabType => {
        const tabItems = getSelectedItems(tabType);
        allSelectedItems.push(...tabItems);
    });
    
    return allSelectedItems;
}

// Bulk action functions
async function bulkResyncItems(tabType) {
    const selectedItems = getSelectedItems(tabType);
    if (selectedItems.length === 0) {
        showNotification('warning', 'No items selected for resync');
        return;
    }
    
    if (!confirm(`Are you sure you want to resync ${selectedItems.length} selected items?`)) {
        return;
    }
    
    // Process items sequentially to avoid database locking
    let successCount = 0;
    let errorCount = 0;
    
    showNotification('info', `Starting resync of ${selectedItems.length} items from DataHub...`);
    
    for (const item of selectedItems) {
        try {
            await resyncItem(item, true); // Suppress reload
            successCount++;
        } catch (error) {
            console.error(`Error resyncing item ${item.name}:`, error);
            errorCount++;
        }
    }
    
    // Reload data once at the end
    await loadGlossaryData();
    
    if (errorCount === 0) {
        showNotification('success', `Successfully resynced ${successCount} items from DataHub`);
    } else {
        showNotification('warning', `Completed: ${successCount} items resynced successfully, ${errorCount} failed`);
    }
}

async function bulkDeployItems(tabType) {
    const selectedItems = getSelectedItems(tabType);
    if (selectedItems.length === 0) {
        showNotification('warning', 'No items selected for deployment');
        return;
    }
    
    if (!confirm(`Are you sure you want to deploy ${selectedItems.length} selected items to DataHub?`)) {
        return;
    }
    
    // Process items sequentially to avoid database locking
    let successCount = 0;
    let errorCount = 0;
    
    showNotification('info', `Starting deployment of ${selectedItems.length} items to DataHub...`);
    
    for (const item of selectedItems) {
        try {
            await deployToDataHub(item, true); // Suppress reload
            successCount++;
        } catch (error) {
            console.error(`Error deploying item ${item.name}:`, error);
            errorCount++;
        }
    }
    
    // Reload data once at the end
    await loadGlossaryData();
    
    if (errorCount === 0) {
        showNotification('success', `Successfully deployed ${successCount} items to DataHub`);
    } else {
        showNotification('warning', `Completed: ${successCount} items deployed successfully, ${errorCount} failed`);
    }
}

async function bulkSyncToLocal(tabType) {
    const selectedItems = getSelectedItems(tabType);
    if (selectedItems.length === 0) {
        showNotification('warning', 'No items selected for sync to local');
        return;
    }
    
    if (!confirm(`Are you sure you want to sync ${selectedItems.length} selected items to local?`)) {
        return;
    }
    
    // Process items sequentially to avoid database locking
    let successCount = 0;
    let errorCount = 0;
    
    showNotification('info', `Starting sync of ${selectedItems.length} items to local...`);
    
    for (const item of selectedItems) {
        try {
            await syncToLocal(item, true); // Suppress reload
            successCount++;
        } catch (error) {
            console.error(`Error syncing item ${item.name}:`, error);
            errorCount++;
        }
    }
    
    // Reload data once at the end
    await loadGlossaryData();
    
    if (errorCount === 0) {
        showNotification('success', `Successfully synced ${successCount} items to local`);
    } else {
        showNotification('warning', `Completed: ${successCount} items synced successfully, ${errorCount} failed`);
    }
}

function bulkDownloadJson(tabType) {
    const selectedItems = tabType === 'global' ? getAllSelectedItems() : getSelectedItems(tabType);
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

async function bulkAddToPR(tabType) {
    const selectedItems = tabType === 'global' ? getAllSelectedItems() : getSelectedItems(tabType);
    if (selectedItems.length === 0) {
        showNotification('warning', 'No items selected for adding to staged changes');
        return;
    }
    
    if (!confirm(`Are you sure you want to add ${selectedItems.length} selected items to staged changes?`)) {
        return;
    }
    
    // Process each item sequentially to avoid race conditions on the same MCP file
    let successCount = 0;
    let errorCount = 0;
    
    showNotification('info', `Starting to add ${selectedItems.length} items to staged changes...`);
    
    for (const item of selectedItems) {
        try {
            // Convert the synchronous-looking addToStagedChanges to return a promise
            await new Promise((resolve, reject) => {
                const itemType = item.type === 'node' ? 'Node' : 'Term';
                const itemName = item.name || 'Unknown';
                
                // Check if this is a remote-only item that needs to be staged directly
                if (item.sync_status === 'REMOTE_ONLY' || !item.id) {
                    console.log(`Item "${itemName}" is remote-only, staging directly...`);
                    
                    // Get current environment and mutation from global state or settings
                    const currentEnvironment = window.currentEnvironment || { name: 'dev' };
                    const mutationName = currentEnvironment.mutation_name || null;
                    
                    // Use the remote staging endpoint
                    fetch('/metadata/glossary/remote/stage_changes/', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'X-CSRFToken': getCsrfToken()
                        },
                        body: JSON.stringify({
                            item_data: item,
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
                        console.log('Add remote to staged changes response:', data);
                        if (data.status === 'success') {
                            resolve(data);
                        } else {
                            reject(new Error(data.error || 'Unknown error occurred'));
                        }
                    })
                    .catch(error => {
                        console.error('Error adding remote item to staged changes:', error);
                        reject(error);
                    });
                    return;
                }
                
                if (!item.id) {
                    console.error('Cannot add item to staged changes without an ID:', item);
                    reject(new Error('Missing item ID'));
                    return;
                }
                
                // Get current environment and mutation from global state or settings
                const currentEnvironment = window.currentEnvironment || { name: 'dev' };
                const mutationName = currentEnvironment.mutation_name || null;
                
                // Use the staged changes endpoint
                const stageUrl = item.type === 'node' ? 
                    `/metadata/glossary/nodes/${item.id}/stage_changes/` : 
                    `/metadata/glossary/terms/${item.id}/stage_changes/`;
                
                fetch(stageUrl, {
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
                    console.log('Add to staged changes response:', data);
                    if (data.status === 'success') {
                        resolve(data);
                    } else {
                        reject(new Error(data.error || 'Unknown error occurred'));
                    }
                })
                .catch(error => {
                    console.error('Error adding item to staged changes:', error);
                    reject(error);
                });
            });
            
            successCount++;
            console.log(`Successfully added ${item.name} to staged changes (${successCount}/${selectedItems.length})`);
        } catch (error) {
            console.error(`Error adding item ${item.name} to staged changes:`, error);
            errorCount++;
        }
    }
    
    if (errorCount > 0) {
        showNotification('warning', `Added ${successCount} items to staged changes, ${errorCount} errors`);
    } else {
        showNotification('success', `Successfully added all ${successCount} items to staged changes`);
    }
}

function bulkDeleteLocal(tabType) {
    const selectedItems = getSelectedItems(tabType);
    if (selectedItems.length === 0) {
        showNotification('warning', 'No items selected for deletion');
        return;
    }
    
    if (!confirm(`Are you sure you want to delete ${selectedItems.length} selected items? This action cannot be undone.`)) {
        return;
    }
    
    // Get deletion order (children before parents)
    const deletionOrder = getDeletionOrder(selectedItems);
    
    showNotification('info', `Starting deletion of ${deletionOrder.length} items in dependency order...`);
    
    // Process items in dependency order
    let successCount = 0;
    let errorCount = 0;
    
    function processNextItem(index) {
        if (index >= deletionOrder.length) {
            // All items processed
            if (errorCount === 0) {
                showNotification('success', `Successfully deleted ${successCount} items`);
            } else {
                showNotification('warning', `Completed: ${successCount} items deleted successfully, ${errorCount} failed`);
            }
            
            // Reload data once at the end
            setTimeout(() => {
                if (typeof loadGlossaryData === 'function') {
                    loadGlossaryData();
                } else {
                    window.location.reload();
                }
            }, 1000);
            return;
        }
        
        const item = deletionOrder[index];
        deleteLocalItem(item, true) // Suppress individual notifications
            .then(() => {
                successCount++;
                processNextItem(index + 1);
            })
            .catch((error) => {
                console.error(`Error deleting item ${item.name}:`, error);
                errorCount++;
                processNextItem(index + 1);
            });
    }
    
    processNextItem(0);
}

/**
 * Get the proper deletion order for glossary items (children before parents)
 * @param {Array} items - Array of selected glossary items
 * @returns {Array} Items in deletion order (children first, then parents)
 */
function getDeletionOrder(items) {
    if (!items || items.length === 0) return [];
    
    // Create a map of URNs to items for quick lookup
    const urnToItem = {};
    items.forEach(item => {
        if (item.urn) {
            urnToItem[item.urn] = item;
        }
    });
    
    // Build child-to-parent and parent-to-children maps
    const childrenMap = {};
    const parentMap = {};
    
    items.forEach(item => {
        const parentUrn = item.parent_urn || item.parent_node_urn;
        if (parentUrn && urnToItem[parentUrn]) {
            // This item has a parent in the selection
            if (!childrenMap[parentUrn]) {
                childrenMap[parentUrn] = [];
            }
            childrenMap[parentUrn].push(item.urn);
            parentMap[item.urn] = parentUrn;
        }
    });
    
    // Use topological sort (post-order traversal) to get deletion order
    const visited = new Set();
    const order = [];
    
    function visit(urn) {
        if (visited.has(urn)) return;
        visited.add(urn);
        
        // Visit all children first
        if (childrenMap[urn]) {
            childrenMap[urn].forEach(childUrn => {
                if (urnToItem[childUrn]) {
                    visit(childUrn);
                }
            });
        }
        
        // Add this item to the order (children will be deleted before parents)
        if (urnToItem[urn]) {
            order.push(urnToItem[urn]);
        }
    }
    
    // Start with items that have no parents in the selection (root items)
    items.forEach(item => {
        const parentUrn = item.parent_urn || item.parent_node_urn;
        if (!parentUrn || !urnToItem[parentUrn]) {
            // This is a root item (no parent in selection)
            visit(item.urn);
        }
    });
    
    // Also visit any remaining items (in case of cycles or isolated items)
    items.forEach(item => {
        if (item.urn && !visited.has(item.urn)) {
            visit(item.urn);
        }
    });
    
    console.log('Deletion order:', order.map(item => `${item.name} (${item.type})`));
    return order;
}

// Global bulk action functions for the dropdown
function resyncAll() {
    const allItems = getAllSelectedItems();
    if (allItems.length === 0) {
        showNotification('warning', 'No items selected for resync');
        return;
    }
    
    if (!confirm(`Are you sure you want to resync all ${allItems.length} selected items?`)) {
        return;
    }
    
    bulkResyncItems('global');
}

function exportAll() {
    const allItems = getAllSelectedItems();
    if (allItems.length === 0) {
        // If no items selected, export all data
        const allData = [
            ...(glossaryData.synced_items || []).map(item => item.combined || item),
            ...(glossaryData.local_only_items || []),
            ...(glossaryData.remote_only_items || [])
        ];
        
        if (allData.length === 0) {
            showNotification('warning', 'No glossary data to export');
            return;
        }
        
        bulkDownloadJson('all');
        return;
    }
    
    bulkDownloadJson('global');
}

function addAllToStagedChanges() {
    const allItems = getAllSelectedItems();
    if (allItems.length === 0) {
        showNotification('warning', 'No items selected for adding to staged changes');
        return;
    }
    
    if (!confirm(`Are you sure you want to add all ${allItems.length} selected items to staged changes?`)) {
        return;
    }
    
    bulkAddToPR('global');
}

function showImportModal() {
    // Show the CSV upload modal for now - can be enhanced later for JSON import
    showUploadCSVModal();
}