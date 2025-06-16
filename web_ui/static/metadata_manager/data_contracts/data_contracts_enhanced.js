console.log('Data Contracts script loading...');

// Global variables
let contractsData = {
    synced_items: [],
    local_only_items: [],
    remote_only_items: [],
    datahub_url: '',
    datahub_token: ''
};

let currentSearch = {
    synced: '',
    local: '',
    remote: ''
};

let currentOverviewFilter = null;
let currentSort = { column: null, direction: 'asc', tabType: null };
let currentPagination = {
    synced: { page: 1, itemsPerPage: 25 },
    local: { page: 1, itemsPerPage: 25 },
    remote: { page: 1, itemsPerPage: 25 }
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

    sanitizeApiResponse: function(data) {
        if (!data || typeof data !== 'object') return data;
        
        if (Array.isArray(data)) {
            return data.map(item => this.createDisplaySafeItem(item));
        }
        
        return this.createDisplaySafeItem(data);
    },

    safeTruncateText: function(text, maxLength) {
        if (!text || typeof text !== 'string') return text || '';
        if (text.length <= maxLength) return text;
        return text.substring(0, maxLength - 3) + '...';
    },

    safeJsonParse: function(jsonString, fallback = null) {
        try {
            return JSON.parse(jsonString);
        } catch (e) {
            console.warn('Failed to parse JSON:', e);
            return fallback;
        }
    },

    safeJsonStringify: function(obj, fallback = '{}') {
        try {
            return JSON.stringify(obj, null, 2);
        } catch (e) {
            console.warn('Failed to stringify object:', e);
            return fallback;
        }
    },

    formatDisplayText: function(text, maxLength = 50) {
        if (!text) return '';
        return this.safeTruncateText(text, maxLength);
    },

    safeEscapeHtml: function(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
};

document.addEventListener('DOMContentLoaded', function() {
    console.log('Data Contracts page loaded, initializing...');
    
    // Load users and groups cache first
    loadUsersAndGroups().then(() => {
        // Then load contracts data
        loadContractsData();
    });
    
    // Search functionality for each tab
    ['synced', 'local', 'remote'].forEach(tab => {
        const searchInput = document.getElementById(`${tab}-search`);
        const clearButton = document.getElementById(`${tab}-clear`);
        
        if (searchInput) {
            searchInput.addEventListener('input', function() {
                currentSearch[tab] = this.value.toLowerCase();
                displayTabContent(tab);
            });
        }
        
        if (clearButton) {
            clearButton.addEventListener('click', function() {
                if (searchInput) {
                    searchInput.value = '';
                    currentSearch[tab] = '';
                    displayTabContent(tab);
                }
            });
        }
    });
    
    // Refresh button
    const refreshButton = document.getElementById('refreshContracts');
    if (refreshButton) {
        refreshButton.addEventListener('click', function() {
            loadContractsData();
        });
    }

    // Filter functionality
    setupFilterHandlers();
});

function loadUsersAndGroups() {
    return new Promise((resolve) => {
        // Check if cache is still valid
        if (usersAndGroupsCache.lastFetched && 
            (Date.now() - usersAndGroupsCache.lastFetched) < usersAndGroupsCache.cacheExpiry) {
            console.log('Using cached users and groups data');
            resolve();
            return;
        }

        console.log('Loading users and groups...');
        fetch('/metadata/tags/users-groups/')
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    usersAndGroupsCache.users = data.data.users || [];
                    usersAndGroupsCache.groups = data.data.groups || [];
                    usersAndGroupsCache.ownership_types = data.data.ownership_types || [];
                    usersAndGroupsCache.lastFetched = Date.now();
                    console.log(`Loaded ${usersAndGroupsCache.users.length} users, ${usersAndGroupsCache.groups.length} groups, ${usersAndGroupsCache.ownership_types.length} ownership types`);
                } else {
                    console.error('Failed to load users and groups:', data.error);
                }
            })
            .catch(error => {
                console.error('Error loading users and groups:', error);
            })
            .finally(() => {
                resolve();
            });
    });
}

function loadContractsData() {
    console.log('Loading contracts data...');
    showLoading(true);
    
    fetch('/metadata/data-contracts/data/')
        .then(response => {
            console.log('Received response:', response.status);
            return response.json();
        })
        .then(data => {
            console.log('Parsed data:', data.success);
            if (data.success) {
                // Process the data to match expected structure
                const processedData = processContractsData(data.data);
                contractsData = processedData;
                updateStatistics();
                updateTabBadges();
                displayAllTabs();
            } else {
                console.error('Data loading failed:', data.error);
                showError('Failed to load contracts: ' + (data.error || 'Unknown error'));
            }
        })
        .catch(error => {
            console.error('Error loading contracts:', error);
            showError('Failed to load contracts data: ' + error.message);
        })
        .finally(() => {
            showLoading(false);
        });
}

function processContractsData(rawData) {
    // Process the raw data to create synced, local, and remote arrays
    const processed = {
        synced_items: [],
        local_only_items: [],
        remote_only_items: [],
        datahub_url: rawData.datahub_url || '',
        datahub_token: rawData.datahub_token || ''
    };

    // For now, put all contracts in remote_only_items since data contracts are typically remote
    if (rawData.remote_data_contracts) {
        processed.remote_only_items = rawData.remote_data_contracts.map(contract => ({
            ...contract,
            sync_status: 'REMOTE_ONLY',
            sync_status_display: 'Remote Only'
        }));
    }

    return processed;
}

function showLoading(show) {
    const loadingIndicator = document.getElementById('loading-indicator');
    const contractsContent = document.getElementById('contracts-content');
    
    if (loadingIndicator) {
        loadingIndicator.style.display = show ? 'block' : 'none';
    }
    if (contractsContent) {
        contractsContent.style.display = show ? 'none' : 'block';
    }
}

function updateStatistics() {
    const stats = calculateStatistics();
    
    // Update main statistics with null checks
    const totalElement = document.getElementById('total-items');
    if (totalElement) totalElement.textContent = stats.total_items || 0;
    
    const syncedElement = document.getElementById('synced-count');
    if (syncedElement) syncedElement.textContent = stats.synced_count || 0;
    
    const localOnlyElement = document.getElementById('local-only-count');
    if (localOnlyElement) localOnlyElement.textContent = stats.local_only_count || 0;
    
    const remoteOnlyElement = document.getElementById('remote-only-count');
    if (remoteOnlyElement) remoteOnlyElement.textContent = stats.remote_only_count || 0;
}

function calculateStatistics() {
    const stats = {
        total_items: contractsData.synced_items.length + contractsData.local_only_items.length + contractsData.remote_only_items.length,
        synced_count: contractsData.synced_items.length,
        local_only_count: contractsData.local_only_items.length,
        remote_only_count: contractsData.remote_only_items.length
    };

    return stats;
}

function updateTabBadges() {
    document.getElementById('synced-badge').textContent = contractsData.synced_items.length;
    document.getElementById('local-badge').textContent = contractsData.local_only_items.length;
    document.getElementById('remote-badge').textContent = contractsData.remote_only_items.length;
}

function displayAllTabs() {
    displayTabContent('synced');
    displayTabContent('local');
    displayTabContent('remote');
}

function displayTabContent(tabType) {
    console.log('Displaying tab content for:', tabType);
    
    let items, contentId;
    
    try {
        switch(tabType) {
            case 'synced':
                items = contractsData.synced_items.map(item => item.combined || item);
                contentId = 'synced-content';
                break;
            case 'local':
                items = contractsData.local_only_items;
                contentId = 'local-content';
                break;
            case 'remote':
                items = contractsData.remote_only_items;
                contentId = 'remote-content';
                break;
        }
        
        console.log(`Tab ${tabType}: ${items?.length || 0} items`);
        
        // Sort items alphabetically by URN (A-Z)
        if (items && items.length > 0) {
            items.sort((a, b) => (a.urn || '').toLowerCase().localeCompare((b.urn || '').toLowerCase()));
        }
        
        // Apply filters
        items = applyFilters(items);
        
        // Filter items based on search
        const searchTerm = currentSearch[tabType];
        if (searchTerm) {
            items = items.filter(item => 
                (item.urn || '').toLowerCase().includes(searchTerm) ||
                (item.properties?.entityUrn || '').toLowerCase().includes(searchTerm) ||
                (item.status?.state || '').toLowerCase().includes(searchTerm)
            );
        }
        
        // Apply sorting if active for this tab
        if (currentSort.column && currentSort.tabType === tabType) {
            items = sortItems(items, currentSort.column, currentSort.direction);
        }
        
        const content = document.getElementById(contentId);
        if (!content) {
            console.error('Content element not found:', contentId);
            return;
        }
        
        if (items.length === 0) {
            console.log(`No items for ${tabType}, showing empty state`);
            content.innerHTML = `
                <div class="table-responsive">
                    <table class="table table-hover table-striped mb-0">
                        <thead>
                            <tr>
                                <th class="sortable-header" data-sort="urn">URN</th>
                                <th class="sortable-header" data-sort="entityUrn">Entity URN</th>
                                <th class="sortable-header" data-sort="state">State</th>
                                <th class="sortable-header" data-sort="assertions">Assertions</th>
                                <th width="15%">Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${getEmptyStateHTML(tabType, searchTerm)}
                        </tbody>
                    </table>
                </div>
            `;
            return;
        }
        
        console.log(`Generating table for ${tabType} with ${items.length} items`);
        const tableHTML = generateTableHTML(items, tabType);
        content.innerHTML = tableHTML;
        
        // Attach click handlers for view buttons
        content.querySelectorAll('.view-item').forEach(button => {
            button.addEventListener('click', function() {
                const row = this.closest('tr');
                const itemData = JSON.parse(row.dataset.item);
                showContractDetails(itemData);
            });
        });
        
        // Attach sorting handlers
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
        
        console.log(`Successfully rendered ${tabType} tab`);
        
    } catch (error) {
        console.error(`Error displaying ${tabType} tab:`, error);
        const content = document.getElementById(contentId);
        if (content) {
            content.innerHTML = `<div class="alert alert-danger">Error loading ${tabType} contracts: ${error.message}</div>`;
        }
    }
}

function applyFilters(items) {
    if (!items) return [];
    
    let filteredItems = [...items];
    
    // Apply overview filter (single select)
    if (currentOverviewFilter) {
        filteredItems = filteredItems.filter(item => {
            switch(currentOverviewFilter) {
                case 'synced':
                    return item.sync_status === 'SYNCED';
                case 'local-only':
                    return item.sync_status === 'LOCAL_ONLY';
                case 'remote-only':
                    return item.sync_status === 'REMOTE_ONLY';
                case 'total':
                default:
                    return true; // Show all
            }
        });
    }
    
    return filteredItems;
}

function setupFilterHandlers() {
    // Overview filters (single select)
    document.querySelectorAll('[data-category="overview"]').forEach(stat => {
        stat.addEventListener('click', function() {
            const filter = this.dataset.filter;
            
            // Clear all overview filters first
            document.querySelectorAll('[data-category="overview"]').forEach(s => s.classList.remove('active'));
            
            if (filter === 'total') {
                // Clear all filters and show all data
                currentOverviewFilter = null;
                displayAllTabs();
            } else if (currentOverviewFilter === filter) {
                // Deselect if clicking same filter
                currentOverviewFilter = null;
                displayAllTabs();
            } else {
                // Select new filter and switch tab
                currentOverviewFilter = filter;
                this.classList.add('active');
                
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
                
                displayAllTabs();
            }
        });
    });
}

function switchToTab(tabType) {
    // Remove active class from all tab buttons
    document.querySelectorAll('.nav-link').forEach(tab => {
        tab.classList.remove('active');
    });
    
    // Remove active class from all tab panes
    document.querySelectorAll('.tab-pane').forEach(pane => {
        pane.classList.remove('active', 'show');
    });
    
    // Activate the selected tab
    const tabButton = document.querySelector(`[data-bs-target="#${tabType}"]`);
    if (tabButton) {
        tabButton.classList.add('active');
    }
    
    const tabPane = document.getElementById(tabType);
    if (tabPane) {
        tabPane.classList.add('active', 'show');
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
            <table class="table table-hover table-striped mb-0">
                <thead>
                    <tr>
                        <th class="sortable-header" data-sort="urn">URN</th>
                        <th class="sortable-header" data-sort="entityUrn">Entity URN</th>
                        <th class="sortable-header" data-sort="state">State</th>
                        <th class="sortable-header" data-sort="assertions">Assertions</th>
                        <th width="15%">Actions</th>
                    </tr>
                </thead>
                <tbody>
                    ${paginatedItems.length === 0 ? getEmptyStateHTML(tabType, currentSearch[tabType]) : 
                      paginatedItems.map(item => renderContractRow(item, tabType)).join('')}
                </tbody>
            </table>
        </div>
        ${items.length > pagination.itemsPerPage ? generatePaginationHTML(items.length, tabType) : ''}
    `;
}

function renderContractRow(contract, tabType) {
    const statusClass = getStatusBadgeClass(contract.sync_status);
    const typeIcon = 'fas fa-file-contract text-warning';
    
    // Extract contract data
    const urn = contract.urn || '';
    const properties = contract.properties || {};
    const entityUrn = properties.entityUrn || '';
    const status = contract.status || {};
    const state = status.state || 'Unknown';
    
    // Count assertions - handle different data structures
    let assertionCount = 0;
    if (properties.freshness) {
        if (Array.isArray(properties.freshness)) {
            assertionCount += properties.freshness.length;
        } else if (properties.freshness.assertion) {
            assertionCount++;
        }
    }
    if (properties.schema) {
        if (Array.isArray(properties.schema)) {
            assertionCount += properties.schema.length;
        } else if (properties.schema.assertion) {
            assertionCount++;
        }
    }
    if (properties.dataQuality) {
        if (Array.isArray(properties.dataQuality)) {
            assertionCount += properties.dataQuality.length;
        } else if (properties.dataQuality.assertion) {
            assertionCount++;
        }
    }
    
    // Sanitize contract data for the data-item attribute
    const sanitizedContract = DataUtils.createDisplaySafeItem(contract);
    
    return `
        <tr data-item='${DataUtils.safeJsonStringify(sanitizedContract)}'>
            <td>
                <div class="d-flex align-items-center">
                    <i class="${typeIcon} me-2"></i>
                    <code class="small text-truncate d-block" style="max-width: 300px;" title="${DataUtils.safeEscapeHtml(urn)}">${DataUtils.safeEscapeHtml(DataUtils.formatDisplayText(urn, 60))}</code>
                </div>
            </td>
            <td>
                <code class="small text-truncate d-block" style="max-width: 400px;" title="${DataUtils.safeEscapeHtml(entityUrn)}">${DataUtils.safeEscapeHtml(DataUtils.formatDisplayText(entityUrn, 80))}</code>
            </td>
            <td>
                <span class="badge bg-secondary">${DataUtils.safeEscapeHtml(state)}</span>
            </td>
            <td>
                <span class="badge bg-info">${assertionCount}</span>
            </td>
            <td>
                <div class="btn-group" role="group">
                    <button type="button" class="btn btn-sm btn-outline-primary view-item" title="View Details">
                        <i class="fas fa-eye"></i>
                    </button>
                    ${contractsData.datahub_url && entityUrn ? `
                        <a href="${contractsData.datahub_url}/dataset/${encodeURIComponent(entityUrn)}/Quality/Data%20Contract" 
                           class="btn btn-sm btn-outline-info" 
                           target="_blank" title="View in DataHub">
                            <i class="fas fa-external-link-alt"></i>
                        </a>
                    ` : ''}
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
                Showing ${startItem}-${endItem} of ${totalItems} contracts
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

function getEmptyStateHTML(tabType, hasSearch) {
    if (hasSearch) {
        return `
            <tr>
                <td colspan="5" class="text-center py-4 text-muted">
                    <i class="fas fa-search fa-2x mb-2"></i><br>
                    No contracts found matching your search criteria.
                </td>
            </tr>
        `;
    }
    
    const emptyStates = {
        synced: 'No synced contracts found. Contracts that exist both locally and in DataHub will appear here.',
        local: 'No local-only contracts found. Contracts that exist only in this application will appear here.',
        remote: 'No remote-only contracts found. Contracts that exist only in DataHub will appear here.'
    };
    
    return `
        <tr>
            <td colspan="5" class="text-center py-4 text-muted">
                <i class="fas fa-file-contract fa-2x mb-2"></i><br>
                ${emptyStates[tabType]}
            </td>
        </tr>
    `;
}

function attachSortingHandlers(content, tabType) {
    content.querySelectorAll('.sortable-header').forEach(header => {
        header.addEventListener('click', function() {
            const column = this.dataset.sort;
            
            // Toggle sort direction if same column, otherwise default to asc
            if (currentSort.column === column && currentSort.tabType === tabType) {
                currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
            } else {
                currentSort.column = column;
                currentSort.direction = 'asc';
                currentSort.tabType = tabType;
            }
            
            // Reset pagination to first page when sorting
            currentPagination[tabType].page = 1;
            
            displayTabContent(tabType);
        });
    });
}

function restoreSortState(content, tabType) {
    if (currentSort.column && currentSort.tabType === tabType) {
        const header = content.querySelector(`[data-sort="${currentSort.column}"]`);
        if (header) {
            header.classList.add('sorted');
            header.classList.add(currentSort.direction);
            
            // Add sort indicator
            const indicator = currentSort.direction === 'asc' ? '↑' : '↓';
            if (!header.querySelector('.sort-indicator')) {
                header.innerHTML += ` <span class="sort-indicator">${indicator}</span>`;
            }
        }
    }
}

function getSortValue(contract, column) {
    switch (column) {
        case 'urn':
            return contract.urn || '';
        case 'entityUrn':
            return contract.properties?.entityUrn || '';
        case 'state':
            return contract.status?.state || '';
        case 'assertions':
            // Count assertions for sorting
            let count = 0;
            const props = contract.properties || {};
            if (props.freshness) count += Array.isArray(props.freshness) ? props.freshness.length : 1;
            if (props.schema) count += Array.isArray(props.schema) ? props.schema.length : 1;
            if (props.dataQuality) count += Array.isArray(props.dataQuality) ? props.dataQuality.length : 1;
            return count;
        default:
            return '';
    }
}

function sortItems(items, column, direction) {
    return [...items].sort((a, b) => {
        const aVal = getSortValue(a, column);
        const bVal = getSortValue(b, column);
        
        // Handle numeric sorting for assertions
        if (column === 'assertions') {
            return direction === 'asc' ? aVal - bVal : bVal - aVal;
        }
        
        // Handle string sorting
        const aStr = String(aVal).toLowerCase();
        const bStr = String(bVal).toLowerCase();
        
        if (direction === 'asc') {
            return aStr.localeCompare(bStr);
        } else {
            return bStr.localeCompare(aStr);
        }
    });
}

function showContractDetails(contract) {
    // Basic information
    document.getElementById('modal-contract-urn').textContent = contract.urn || 'No URN available';
    document.getElementById('modal-contract-entityurn').textContent = contract.properties?.entityUrn || 'No entity URN available';
    document.getElementById('modal-contract-state').textContent = contract.status?.state || 'Unknown';
    
    // Status
    const statusBadge = document.getElementById('modal-contract-status');
    statusBadge.textContent = contract.sync_status_display || contract.sync_status;
    statusBadge.className = `badge ${getStatusBadgeClass(contract.sync_status)}`;
    
    // DataHub link - point to entity URN's quality page
    const datahubLink = document.getElementById('modal-datahub-link');
    const entityUrn = contract.properties?.entityUrn;
    if (entityUrn && contractsData.datahub_url) {
        datahubLink.href = `${contractsData.datahub_url}/dataset/${encodeURIComponent(entityUrn)}/Quality/Data%20Contract`;
        datahubLink.style.display = 'inline-block';
    } else {
        datahubLink.style.display = 'none';
    }
    
    // Assertions information - look for urn:li:assertion:... items
    const assertionsList = document.getElementById('modal-assertions-list');
    const properties = contract.properties || {};
    let assertionsHTML = '';
    
    // Function to extract assertions from any object
    function findAssertions(obj, path = '') {
        const assertions = [];
        
        if (obj && typeof obj === 'object') {
            for (const [key, value] of Object.entries(obj)) {
                const currentPath = path ? `${path}.${key}` : key;
                
                if (typeof value === 'string' && value.startsWith('urn:li:assertion:')) {
                    assertions.push({
                        type: key,
                        urn: value,
                        path: currentPath
                    });
                } else if (Array.isArray(value)) {
                    value.forEach((item, index) => {
                        if (typeof item === 'string' && item.startsWith('urn:li:assertion:')) {
                            assertions.push({
                                type: key,
                                urn: item,
                                path: `${currentPath}[${index}]`
                            });
                        } else if (item && typeof item === 'object') {
                            assertions.push(...findAssertions(item, `${currentPath}[${index}]`));
                        }
                    });
                } else if (value && typeof value === 'object') {
                    assertions.push(...findAssertions(value, currentPath));
                }
            }
        }
        
        return assertions;
    }
    
    // Find all assertions in the contract
    const allAssertions = findAssertions(contract);
    
    if (allAssertions.length > 0) {
        assertionsHTML = '<ul class="list-group list-group-flush">';
        allAssertions.forEach((assertion, index) => {
            assertionsHTML += `
                <li class="list-group-item d-flex justify-content-between align-items-start">
                    <div class="ms-2 me-auto">
                        <div class="fw-bold">${DataUtils.safeEscapeHtml(assertion.type)}</div>
                        <small class="text-muted">Path: ${DataUtils.safeEscapeHtml(assertion.path)}</small>
                        <div class="mt-1">
                            <code class="small">${DataUtils.safeEscapeHtml(assertion.urn)}</code>
                        </div>
                    </div>
                    <span class="badge bg-primary rounded-pill">${index + 1}</span>
                </li>
            `;
        });
        assertionsHTML += '</ul>';
        assertionsList.innerHTML = assertionsHTML;
    } else {
        assertionsList.innerHTML = '<p class="text-muted">No assertions found</p>';
    }
    
    // Contract Properties
    const propertiesDiv = document.getElementById('modal-contract-properties');
    if (contract.properties || contract.structuredProperties) {
        let propertiesHTML = '';
        
        if (contract.structuredProperties && contract.structuredProperties.properties) {
            propertiesHTML += `
                <div class="border rounded p-2 mb-2">
                    <strong>Structured Properties:</strong>
                    <div class="mt-2">
                        ${contract.structuredProperties.properties.map(prop => `
                            <div class="mb-2">
                                <strong>${DataUtils.safeEscapeHtml(prop.structuredPropertyKey || 'Unknown')}</strong>: 
                                ${prop.value ? DataUtils.safeEscapeHtml(DataUtils.safeJsonStringify(prop.value)) : 'No value'}
                            </div>
                        `).join('')}
                    </div>
                </div>
            `;
        }
        
        if (contract.properties) {
            const contractProps = { ...contract.properties };
            // Remove assertions as they're shown separately
            delete contractProps.freshness;
            delete contractProps.schema;
            delete contractProps.dataQuality;
            
            if (Object.keys(contractProps).length > 0) {
                propertiesHTML += `
                    <div class="border rounded p-2 mb-2">
                        <strong>Contract Properties:</strong>
                        <pre class="mt-2 mb-0"><code>${DataUtils.safeEscapeHtml(DataUtils.safeJsonStringify(contractProps))}</code></pre>
                    </div>
                `;
            }
        }
        
        if (propertiesHTML) {
            propertiesDiv.innerHTML = propertiesHTML;
        } else {
            propertiesDiv.innerHTML = '<p class="text-muted">No additional properties available</p>';
        }
    } else {
        propertiesDiv.innerHTML = '<p class="text-muted">No additional properties available</p>';
    }
    
    // Raw JSON - Show comprehensive data
    const rawData = contract.raw_data || contract;
    document.getElementById('modal-raw-json').innerHTML = `<code>${DataUtils.safeEscapeHtml(DataUtils.safeJsonStringify(rawData))}</code>`;
    
    // Show modal
    new bootstrap.Modal(document.getElementById('contractViewModal')).show();
}

function getDataHubUrl(urn, type) {
    if (contractsData.datahub_url) {
        return `${contractsData.datahub_url}/dataContract/${encodeURIComponent(urn)}`;
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
    const alert = document.createElement('div');
    alert.className = 'alert alert-danger alert-dismissible fade show';
    alert.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    const container = document.querySelector('.container-fluid');
    if (container) {
        container.insertBefore(alert, container.querySelector('.row'));
    }
}

console.log('Data Contracts script loaded successfully'); 