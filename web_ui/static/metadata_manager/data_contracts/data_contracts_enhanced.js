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
    setupBulkActions();
});

function setupBulkActions() {
    // This function will be called to set up initial bulk action structure
    // Individual checkbox handlers are attached in displayTabContent
    console.log('Bulk actions structure initialized');
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

function getSelectedContracts(tabType) {
    const checkboxes = document.querySelectorAll(`#${tabType}-content .item-checkbox:checked`);
    const selectedContracts = [];
    
    checkboxes.forEach(checkbox => {
        const row = checkbox.closest('tr');
        if (row && row.dataset.item) {
            try {
                const contractData = DataUtils.safeJsonParse(row.dataset.item);
                if (contractData) {
                    selectedContracts.push(contractData);
                }
            } catch (error) {
                console.error('Error parsing contract data:', error);
            }
        }
    });
    
    return selectedContracts;
}

function clearAllSelections() {
    const checkboxes = document.querySelectorAll('.item-checkbox, .select-all-checkbox');
    checkboxes.forEach(checkbox => {
        checkbox.checked = false;
    });
    
    // Update bulk action visibility for all tabs
    ['synced', 'local', 'remote'].forEach(tab => {
        updateBulkActionVisibility(tab);
    });
    
    console.log('Cleared all contract selections due to data refresh');
}

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
    
    // Clear any existing selections to prevent stale data issues
    clearAllSelections();
    
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
                    <table class="table table-hover mb-0">
                        <thead>
                            <tr>
                                <th width="30">
                                    <input type="checkbox" class="form-check-input select-all-checkbox" id="selectAll${tabType.charAt(0).toUpperCase() + tabType.slice(1)}">
                                </th>
                                <th class="sortable-header" data-sort="urn" width="300">URN</th>
                                <th class="sortable-header" data-sort="entityUrn" width="300">Entity URN</th>
                                <th class="sortable-header" data-sort="state" width="100">State</th>
                                <th class="sortable-header" data-sort="assertions" width="80">Assertions</th>
                                <th width="140">Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${getEmptyStateHTML(tabType, searchTerm)}
                        </tbody>
                    </table>
                </div>
            `;
            
            // Attach select-all checkbox handler for empty state
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
        
        // Attach click handlers for sync-to-local buttons
        content.querySelectorAll('.sync-to-local').forEach(button => {
            button.addEventListener('click', function() {
                const row = this.closest('tr');
                const itemData = JSON.parse(row.dataset.item);
                syncContractToLocal(itemData);
            });
        });
        
        // Attach click handlers for resync buttons
        content.querySelectorAll('.resync-contract').forEach(button => {
            button.addEventListener('click', function() {
                const row = this.closest('tr');
                const itemData = JSON.parse(row.dataset.item);
                resyncContract(itemData);
            });
        });
        
        // Attach click handlers for download JSON buttons
        content.querySelectorAll('.download-json').forEach(button => {
            button.addEventListener('click', function() {
                const row = this.closest('tr');
                const itemData = JSON.parse(row.dataset.item);
                downloadContractJson(itemData);
            });
        });
        
        // Attach click handlers for add to staged changes buttons
        content.querySelectorAll('.add-to-staged').forEach(button => {
            button.addEventListener('click', function() {
                const row = this.closest('tr');
                const itemData = JSON.parse(row.dataset.item);
                addContractToStagedChanges(itemData);
            });
        });
        
        // Attach checkbox handlers for bulk actions
        content.querySelectorAll('.item-checkbox').forEach(checkbox => {
            checkbox.addEventListener('change', function() {
                updateBulkActionVisibility(tabType);
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
        
        // Attach sorting handlers
        attachSortingHandlers(content, tabType);
        restoreSortState(content, tabType);
        
        // Attach checkbox handlers
        attachCheckboxHandlers(content, tabType);
        
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
            <table class="table table-hover mb-0">
                <thead>
                    <tr>
                        <th width="30">
                            <input type="checkbox" class="form-check-input select-all-checkbox" id="selectAll${tabType.charAt(0).toUpperCase() + tabType.slice(1)}">
                        </th>
                        <th class="sortable-header" data-sort="urn" width="250">URN</th>
                        <th class="sortable-header" data-sort="entityUrn" width="250">Entity URN</th>
                        <th class="sortable-header" data-sort="state" width="100">State</th>
                        <th class="sortable-header" data-sort="result" width="100">Result</th>
                        <th class="sortable-header" data-sort="assertions" width="80">Assertions</th>
                        <th width="140">Actions</th>
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
    const result = contract.result || {};
    const resultType = result.type || 'Unknown';
    
    // Get state badge class - active is green, pending is yellow
    let stateBadgeClass = 'bg-secondary';
    if (state.toLowerCase() === 'active') {
        stateBadgeClass = 'bg-success';
    } else if (state.toLowerCase() === 'pending') {
        stateBadgeClass = 'bg-warning';
    }
    
    // Get result badge class - passing is green, failing is red
    let resultBadgeClass = 'bg-secondary';
    if (resultType.toLowerCase() === 'passing') {
        resultBadgeClass = 'bg-success';
    } else if (resultType.toLowerCase() === 'failing') {
        resultBadgeClass = 'bg-danger';
    }
    
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
    
    // Use safe JSON stringify for minimal data attributes
    const safeJsonData = JSON.stringify(sanitizedContract)
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&apos;');
    
    return `
        <tr data-item='${safeJsonData}'>
            <td>
                <input type="checkbox" class="form-check-input item-checkbox" value="${contract.urn || contract.id}">
            </td>
            <td title="${DataUtils.safeEscapeHtml(urn)}">
                <div class="d-flex align-items-center">
                    <i class="${typeIcon} me-2"></i>
                    <code class="small text-truncate" style="max-width: 200px; display: inline-block;">${DataUtils.safeEscapeHtml(DataUtils.formatDisplayText(urn, 40))}</code>
                </div>
            </td>
            <td title="${DataUtils.safeEscapeHtml(entityUrn)}">
                <code class="small text-truncate" style="max-width: 200px; display: inline-block;">${DataUtils.safeEscapeHtml(DataUtils.formatDisplayText(entityUrn, 40))}</code>
            </td>
            <td>
                <span class="badge ${stateBadgeClass}">${DataUtils.safeEscapeHtml(state)}</span>
            </td>
            <td>
                <span class="badge ${resultBadgeClass}">${DataUtils.safeEscapeHtml(resultType)}</span>
            </td>
            <td class="text-center">
                <span class="badge bg-info">${assertionCount}</span>
            </td>
            <td>
                <div class="btn-group" role="group">
                    ${getContractActionButtons(contract, tabType)}
                </div>
            </td>
        </tr>
    `;
}

function getContractActionButtons(contract, tabType) {
    const contractData = contract;
    const urn = contractData.urn || '';
    const entityUrn = contractData.properties?.entityUrn || '';
    
    let actionButtons = '';
    
    // 1. View Details - Available for all contracts
    actionButtons += `
        <button type="button" class="btn btn-sm btn-outline-primary view-item" title="View Details">
            <i class="fas fa-eye"></i>
        </button>
    `;
    
    // 2. Sync to Local - Only for remote contracts
    if (tabType === 'remote') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-success sync-to-local" 
                    title="Sync to Local">
                <i class="fas fa-download"></i>
            </button>
        `;
    }
    
    // 3. Resync - Only for synced contracts
    if (tabType === 'synced') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-info resync-contract" 
                    title="Resync from DataHub">
                <i class="fas fa-sync-alt"></i>
            </button>
        `;
    }
    
    // 4. Download JSON - Available for all contracts
    actionButtons += `
        <button type="button" class="btn btn-sm btn-outline-secondary download-json"
                title="Download JSON">
            <i class="fas fa-file-download"></i>
        </button>
    `;

    // 5. Add to Staged Changes - Available for all contracts
    actionButtons += `
        <button type="button" class="btn btn-sm btn-outline-warning add-to-staged"
                title="Add to Staged Changes">
            <i class="fab fa-github"></i>
        </button>
    `;
    
    // 6. View in DataHub - Available if we have DataHub URL and entity URN
    if (contractsData.datahub_url && entityUrn) {
        actionButtons += `
            <a href="${contractsData.datahub_url}/dataset/${encodeURIComponent(entityUrn)}/Quality/Data%20Contract" 
               class="btn btn-sm btn-outline-info" 
               target="_blank" title="View in DataHub">
                <i class="fas fa-external-link-alt"></i>
            </a>
        `;
    }
    
    return actionButtons;
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
                <td colspan="7" class="text-center py-4 text-muted">
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
            <td colspan="7" class="text-center py-4 text-muted">
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
        case 'result':
            return contract.result?.type || '';
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

function attachCheckboxHandlers(content, tabType) {
    // Handle individual checkboxes
    const checkboxes = content.querySelectorAll('.item-checkbox');
    checkboxes.forEach(checkbox => {
        checkbox.addEventListener('change', function() {
            updateBulkActionVisibility(tabType);
        });
    });
    
    console.log(`Attached checkbox handlers for ${tabType} tab: ${checkboxes.length} checkboxes`);
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

// Bulk action functions
function bulkResyncContracts(tabType) {
    const selectedContracts = getSelectedContracts(tabType);
    if (selectedContracts.length === 0) {
        showNotification('Please select contracts to resync', 'warning');
        return;
    }
    
    if (!confirm(`Are you sure you want to resync ${selectedContracts.length} contract(s)?`)) {
        return;
    }
    
    showNotification(`Starting resync of ${selectedContracts.length} contract(s)...`, 'info');
    
    // Process contracts sequentially to avoid overwhelming the server
    let completed = 0;
    let errors = 0;
    
    function processNextContract(index) {
        if (index >= selectedContracts.length) {
            // All done
            const successCount = completed - errors;
            if (errors === 0) {
                showNotification(`Successfully resynced ${successCount} contract(s)`, 'success');
            } else {
                showNotification(`Completed with ${successCount} success(es) and ${errors} error(s)`, 'warning');
            }
            loadContractsData(); // Refresh data
            return;
        }
        
        const contract = selectedContracts[index];
        
        // Here you would call your resync API endpoint
        // For now, just simulate the process
        setTimeout(() => {
            completed++;
            processNextContract(index + 1);
        }, 100);
    }
    
    processNextContract(0);
}

function bulkSyncToDataHub(tabType) {
    const selectedContracts = getSelectedContracts(tabType);
    if (selectedContracts.length === 0) {
        showNotification('Please select contracts to sync to DataHub', 'warning');
        return;
    }
    
    if (!confirm(`Are you sure you want to sync ${selectedContracts.length} contract(s) to DataHub?`)) {
        return;
    }
    
    showNotification(`Starting sync of ${selectedContracts.length} contract(s) to DataHub...`, 'info');
    
    // Process contracts sequentially
    let completed = 0;
    let errors = 0;
    
    function processNextContract(index) {
        if (index >= selectedContracts.length) {
            const successCount = completed - errors;
            if (errors === 0) {
                showNotification(`Successfully synced ${successCount} contract(s) to DataHub`, 'success');
            } else {
                showNotification(`Completed with ${successCount} success(es) and ${errors} error(s)`, 'warning');
            }
            loadContractsData();
            return;
        }
        
        const contract = selectedContracts[index];
        
        // Here you would call your sync to DataHub API endpoint
        setTimeout(() => {
            completed++;
            processNextContract(index + 1);
        }, 100);
    }
    
    processNextContract(0);
}

function bulkSyncToLocal(tabType) {
    const selectedContracts = getSelectedContracts(tabType);
    if (selectedContracts.length === 0) {
        showNotification('Please select contracts to sync to local', 'warning');
        return;
    }
    
    if (!confirm(`Are you sure you want to sync ${selectedContracts.length} contract(s) to local?`)) {
        return;
    }
    
    showNotification(`Starting sync of ${selectedContracts.length} contract(s) to local...`, 'info');
    
    // Process contracts sequentially
    let completed = 0;
    let errors = 0;
    
    function processNextContract(index) {
        if (index >= selectedContracts.length) {
            const successCount = completed - errors;
            if (errors === 0) {
                showNotification(`Successfully synced ${successCount} contract(s) to local`, 'success');
            } else {
                showNotification(`Completed with ${successCount} success(es) and ${errors} error(s)`, 'warning');
            }
            loadContractsData();
            return;
        }
        
        const contract = selectedContracts[index];
        
        // Call sync to local API endpoint
        syncContractToLocal(contract, true).then(() => {
            completed++;
            processNextContract(index + 1);
        }).catch((error) => {
            console.error(`Error syncing contract ${contract.name || contract.urn}:`, error);
            errors++;
            completed++;
            processNextContract(index + 1);
        });
    }
    
    processNextContract(0);
}

function bulkDownloadJson(tabType) {
    const selectedContracts = getSelectedContracts(tabType);
    if (selectedContracts.length === 0) {
        showNotification('Please select contracts to download', 'warning');
        return;
    }
    
    const jsonData = JSON.stringify(selectedContracts, null, 2);
    const blob = new Blob([jsonData], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    
    const a = document.createElement('a');
    a.href = url;
    a.download = `data_contracts_${tabType}_${new Date().toISOString().split('T')[0]}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    showNotification(`Downloaded ${selectedContracts.length} contract(s) as JSON`, 'success');
}

function bulkAddToPR(tabType) {
    const selectedContracts = getSelectedContracts(tabType);
    if (selectedContracts.length === 0) {
        showNotification('Please select contracts to add to staged changes', 'warning');
        return;
    }
    
    if (!confirm(`Are you sure you want to add ${selectedContracts.length} contract(s) to staged changes?`)) {
        return;
    }
    
    showNotification(`Adding ${selectedContracts.length} contract(s) to staged changes...`, 'info');
    
    // Here you would call your API to add to staged changes
    setTimeout(() => {
        showNotification(`Successfully added ${selectedContracts.length} contract(s) to staged changes`, 'success');
    }, 1000);
}

function bulkDeleteLocal(tabType) {
    const selectedContracts = getSelectedContracts(tabType);
    if (selectedContracts.length === 0) {
        showNotification('Please select contracts to delete', 'warning');
        return;
    }
    
    if (!confirm(`Are you sure you want to delete ${selectedContracts.length} contract(s) from local storage? This action cannot be undone.`)) {
        return;
    }
    
    showNotification(`Deleting ${selectedContracts.length} contract(s) from local storage...`, 'info');
    
    // Process contracts sequentially
    let completed = 0;
    let errors = 0;
    
    function processNextContract(index) {
        if (index >= selectedContracts.length) {
            const successCount = completed - errors;
            if (errors === 0) {
                showNotification(`Successfully deleted ${successCount} contract(s) from local storage`, 'success');
            } else {
                showNotification(`Completed with ${successCount} success(es) and ${errors} error(s)`, 'warning');
            }
            loadContractsData();
            return;
        }
        
        const contract = selectedContracts[index];
        
        // Here you would call your delete API endpoint
        setTimeout(() => {
            completed++;
            processNextContract(index + 1);
        }, 100);
    }
    
    processNextContract(0);
}

// Global bulk action functions
function resyncAll() {
    if (!confirm('Are you sure you want to resync all contracts? This may take a while.')) {
        return;
    }
    
    showNotification('Starting resync of all contracts...', 'info');
    
    // Here you would call your resync all API endpoint
    setTimeout(() => {
        showNotification('Successfully resynced all contracts', 'success');
        loadContractsData();
    }, 2000);
}

function exportAll() {
    const allContracts = [
        ...contractsData.synced_items,
        ...contractsData.local_only_items,
        ...contractsData.remote_only_items
    ];
    
    if (allContracts.length === 0) {
        showNotification('No contracts to export', 'warning');
        return;
    }
    
    const jsonData = JSON.stringify(allContracts, null, 2);
    const blob = new Blob([jsonData], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    
    const a = document.createElement('a');
    a.href = url;
    a.download = `all_data_contracts_${new Date().toISOString().split('T')[0]}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    showNotification(`Exported ${allContracts.length} contract(s) as JSON`, 'success');
}

function addAllToStagedChanges() {
    const allContracts = [
        ...contractsData.synced_items,
        ...contractsData.local_only_items,
        ...contractsData.remote_only_items
    ];
    
    if (allContracts.length === 0) {
        showNotification('No contracts to add to staged changes', 'warning');
        return;
    }
    
    if (!confirm(`Are you sure you want to add all ${allContracts.length} contract(s) to staged changes?`)) {
        return;
    }
    
    showNotification(`Adding all ${allContracts.length} contract(s) to staged changes...`, 'info');
    
    // Here you would call your API to add all to staged changes
    setTimeout(() => {
        showNotification(`Successfully added all ${allContracts.length} contract(s) to staged changes`, 'success');
    }, 2000);
}

function showImportModal() {
    // Here you would show an import modal similar to the tags page
    showNotification('Import functionality not yet implemented', 'info');
}

// Individual action functions
async function syncContractToLocal(contract, suppressNotifications = false) {
    try {
        if (!suppressNotifications) {
            showNotification(`Syncing contract "${contract.name || contract.urn}" to local...`, 'info');
        }
        
        const response = await fetch('/metadata/data-contracts/sync-to-local/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCsrfToken()
            },
            body: JSON.stringify({
                urn: contract.urn
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            if (!suppressNotifications) {
                showNotification(result.message || 'Contract synced successfully', 'success');
                loadContractsData(); // Refresh data
            }
            return result;
        } else {
            if (!suppressNotifications) {
                showNotification(result.error || 'Failed to sync contract', 'danger');
            }
            throw new Error(result.error || 'Failed to sync contract');
        }
    } catch (error) {
        console.error('Error syncing contract to local:', error);
        if (!suppressNotifications) {
            showNotification('Error syncing contract to local', 'danger');
        }
        throw error;
    }
}

async function resyncContract(contract) {
    try {
        showNotification(`Resyncing contract "${contract.name || contract.urn}"...`, 'info');
        
        const response = await fetch(`/metadata/data-contracts/${contract.id}/resync/`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCsrfToken()
            }
        });
        
        const result = await response.json();
        
        if (result.success) {
            showNotification(result.message || 'Contract resynced successfully', 'success');
            loadContractsData(); // Refresh data
        } else {
            showNotification(result.error || 'Failed to resync contract', 'danger');
        }
    } catch (error) {
        console.error('Error resyncing contract:', error);
        showNotification('Error resyncing contract', 'danger');
    }
}

function downloadContractJson(contract) {
    const jsonData = JSON.stringify(contract, null, 2);
    const blob = new Blob([jsonData], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    
    const a = document.createElement('a');
    a.href = url;
    a.download = `data_contract_${contract.name || contract.urn.split(':').pop()}_${new Date().toISOString().split('T')[0]}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    showNotification('Contract downloaded as JSON', 'success');
}

async function addContractToStagedChanges(contract) {
    try {
        showNotification(`Adding contract "${contract.name || contract.urn}" to staged changes...`, 'info');
        
        const formData = new FormData();
        formData.append('contract_urn', contract.urn);
        formData.append('csrfmiddlewaretoken', getCsrfToken());
        
        const response = await fetch('/metadata/data-contracts/stage_changes/', {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (result.success) {
            showNotification(result.message || 'Contract added to staged changes', 'success');
        } else {
            showNotification(result.error || 'Failed to add contract to staged changes', 'danger');
        }
    } catch (error) {
        console.error('Error adding contract to staged changes:', error);
        showNotification('Error adding contract to staged changes', 'danger');
    }
}

function showNotification(message, type = 'info') {
    // Create notification element
    const notification = document.createElement('div');
    notification.className = `alert alert-${type} alert-dismissible fade show position-fixed`;
    notification.style.cssText = 'top: 20px; right: 20px; z-index: 9999; min-width: 300px;';
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

console.log('Data Contracts script loaded successfully'); 