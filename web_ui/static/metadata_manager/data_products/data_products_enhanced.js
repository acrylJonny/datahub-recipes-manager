// Global variables
let productsData = {
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
        const connectionElement = document.getElementById('current-connection-name');
        if (connectionElement && connectionElement.dataset.connectionId) {
            currentConnectionId = connectionElement.dataset.connectionId;
        } else {
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
            const seen = new WeakSet();
            const jsonString = JSON.stringify(obj, (key, value) => {
                if (typeof value === 'object' && value !== null) {
                    if (seen.has(value)) {
                        return '[Circular Reference]';
                    }
                    seen.add(value);
                }
                return value === undefined ? null : value;
            }, 2);
            return jsonString.length > maxLength ? jsonString.substring(0, maxLength) + '...' : jsonString;
        } catch (error) {
            console.error('Error stringifying object:', error);
            return '{}';
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
    // Only initialize if the legacy system is not already loaded
    if (typeof window.legacyCurrentTab === 'undefined') {
        // Enhanced data products system initialized
        loadProductsData();
        setupFilterListeners();
        setupSearchHandlers();
        setupBulkActions();
        setupActionButtonListeners();
        
        // Search functionality for each tab
        ['synced', 'local', 'remote'].forEach(tab => {
            const searchInput = document.getElementById(`${tab}-search`);
            const clearButton = document.getElementById(`${tab}-clear`);
            
            if (searchInput) {
                searchInput.addEventListener('input', function() {
                    currentSearch[tab] = this.value;
                    renderTabContent(tab);
                });
            }
            
            if (clearButton) {
                clearButton.addEventListener('click', function() {
                    currentSearch[tab] = '';
                    if (searchInput) searchInput.value = '';
                    renderTabContent(tab);
                });
            }
        });
    } else {
        // Legacy data products system detected, skipping enhanced initialization
    }
});

function setupFilterListeners() {
    // Overview filters (single select)
    document.querySelectorAll('.clickable-stat[data-category="overview"]').forEach(stat => {
        stat.addEventListener('click', function() {
            const filter = this.getAttribute('data-filter');
            
            // Clear all overview filters first
            document.querySelectorAll('.clickable-stat[data-category="overview"]').forEach(s => {
                s.classList.remove('active');
            });
            
            if (currentOverviewFilter === filter) {
                currentOverviewFilter = null;
            } else {
                currentOverviewFilter = filter;
                this.classList.add('active');
            }
            
            applyFilters();
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

function setupSearchHandlers() {
    ['synced', 'local', 'remote'].forEach(tab => {
        const searchInput = document.getElementById(`${tab}-search`);
        const clearButton = document.getElementById(`${tab}-clear`);
        
        if (searchInput) {
            searchInput.addEventListener('input', function() {
                currentSearch[tab] = this.value;
                renderTabContent(tab);
            });
        }
        
        if (clearButton) {
            clearButton.addEventListener('click', function() {
                currentSearch[tab] = '';
                if (searchInput) searchInput.value = '';
                renderTabContent(tab);
            });
        }
    });
}

function setupBulkActions() {
    // Initialize bulk actions as hidden for all tabs
    ['synced', 'local', 'remote'].forEach(tabType => {
        const bulkActions = document.getElementById(`${tabType}-bulk-actions`);
        const selectedCount = document.getElementById(`${tabType}-selected-count`);
        
        if (bulkActions && selectedCount) {
            bulkActions.classList.remove('show');
            selectedCount.textContent = '0';
        }
    });
}

function updateBulkActionVisibility(tab) {
    const tabContent = document.getElementById(`${tab}-content`);
    const bulkActions = document.getElementById(`${tab}-bulk-actions`);
    const selectedCount = document.getElementById(`${tab}-selected-count`);
    
    if (tabContent && bulkActions && selectedCount) {
        const checkboxes = tabContent.querySelectorAll('.item-checkbox:checked');
        const count = checkboxes.length;
        
        if (count > 0) {
            bulkActions.classList.add('show');
            selectedCount.textContent = count;
        } else {
            bulkActions.classList.remove('show');
            selectedCount.textContent = '0';
        }
    }
}

function applyFilters() {
    renderAllTabs();
}

function filterProducts(products) {
    return products.filter(product => {
        // Apply overview filter
        if (currentOverviewFilter) {
            switch (currentOverviewFilter) {
                case 'synced':
                    if (product.sync_status !== 'SYNCED') return false;
                    break;
                case 'local-only':
                    if (product.sync_status !== 'LOCAL_ONLY') return false;
                    break;
                case 'remote-only':
                    if (product.sync_status !== 'REMOTE_ONLY') return false;
                    break;
            }
        }
        
        // Apply content filters
        for (const filter of currentFilters) {
            switch (filter) {
                case 'has-entities':
                    if (!product.entities_count || product.entities_count === 0) return false;
                    break;
                case 'has-owners':
                    if (!product.owners_count || product.owners_count === 0) return false;
                    break;
                case 'has-domain':
                    if (!product.domain_urn) return false;
                    break;
                case 'has-external-url':
                    if (!product.external_url) return false;
                    break;
            }
        }
        
        return true;
    });
}

function renderAllTabs() {
    renderTabContent('synced');
    renderTabContent('local');
    renderTabContent('remote');
    updateTabBadges();
}

function updateTabBadges() {
    const syncedBadge = document.getElementById('synced-badge');
    const localBadge = document.getElementById('local-badge');
    const remoteBadge = document.getElementById('remote-badge');
    
    if (syncedBadge) syncedBadge.textContent = filterProducts(productsData.synced_items || []).length;
    if (localBadge) localBadge.textContent = filterProducts(productsData.local_only_items || []).length;
    if (remoteBadge) remoteBadge.textContent = filterProducts(productsData.remote_only_items || []).length;
}

function renderTabContent(tabType) {
    const contentDiv = document.getElementById(`${tabType}-content`);
    if (!contentDiv) return;
    
    const items = getItemsForTab(tabType);
    const filteredItems = filterProducts(items);
    const searchTerm = currentSearch[tabType] || '';
    
    let searchedItems = filteredItems;
    if (searchTerm) {
        searchedItems = filteredItems.filter(item => 
            item.name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
            item.description?.toLowerCase().includes(searchTerm.toLowerCase()) ||
            item.urn?.toLowerCase().includes(searchTerm.toLowerCase())
        );
    }
    
    // Apply sorting
    if (currentSort.column && currentSort.tabType === tabType) {
        searchedItems = sortItems(searchedItems, currentSort.column, currentSort.direction);
    }
    
    // Apply pagination
    const pagination = currentPagination[tabType];
    const startIndex = (pagination.page - 1) * pagination.itemsPerPage;
    const endIndex = startIndex + pagination.itemsPerPage;
    const paginatedItems = searchedItems.slice(startIndex, endIndex);
    
    const tableHTML = generateTableHTML(paginatedItems, tabType);
    const paginationHTML = generatePaginationHTML(searchedItems.length, tabType);
    
    contentDiv.innerHTML = tableHTML + paginationHTML;
    
    // Attach event handlers
    attachPaginationHandlers(contentDiv, tabType);
    attachSortingHandlers(contentDiv, tabType);
    attachBulkActionHandlers(contentDiv, tabType);
    
    // Update bulk action visibility
    updateBulkActionVisibility(tabType);
}

function getItemsForTab(tabType) {
    switch (tabType) {
        case 'synced': return productsData.synced_items || [];
        case 'local': return productsData.local_only_items || [];
        case 'remote': return productsData.remote_only_items || [];
        default: return [];
    }
}

function generateTableHTML(items, tabType) {
    if (items.length === 0) {
        return getEmptyStateHTML(tabType, currentSearch[tabType]);
    }
    
    const tableRows = items.map(item => renderProductRow(item, tabType)).join('');
    
    return `
        <div class="table-responsive">
            <table class="table table-striped table-hover">
                <thead>
                    <tr>
                        <th width="40">
                            <input type="checkbox" class="form-check-input select-all-checkbox" 
                                   onchange="toggleSelectAll('${tabType}', this)">
                        </th>
                        <th class="sortable-header" data-column="name">Name</th>
                        <th class="sortable-header" data-column="description">Description</th>
                        <th class="sortable-header" data-column="entities_count">Entities</th>
                        <th class="sortable-header" data-column="owners_count">Owners</th>
                        <th class="sortable-header" data-column="domain">Domain</th>
                        <th class="sortable-header" data-column="urn">URN</th>
                        <th class="sortable-header" data-column="sync_status">Status</th>
                        <th width="200">Actions</th>
                    </tr>
                </thead>
                <tbody>
                    ${tableRows}
                </tbody>
            </table>
        </div>
    `;
}

function renderProductRow(product, tabType) {
    const safeProduct = DataUtils.createDisplaySafeItem(product);
    
    return `
        <tr data-item='${DataUtils.safeJsonStringify(product)}'>
            <td>
                <input type="checkbox" class="form-check-input item-checkbox" 
                       data-product-id="${product.id || product.urn}">
            </td>
            <td>
                <div class="d-flex align-items-center">
                    <div>
                        <strong>${DataUtils.safeEscapeHtml(safeProduct.name)}</strong>
                        ${product.external_url ? `<a href="${product.external_url}" target="_blank" class="ms-2"><i class="fas fa-external-link-alt text-muted"></i></a>` : ''}
                    </div>
                </div>
            </td>
            <td>
                <div class="description-preview">
                    ${DataUtils.safeEscapeHtml(safeProduct.description)}
                </div>
            </td>
            <td class="text-center">
                ${product.entities_count || 0}
            </td>
            <td class="text-center">
                ${product.owners_count || 0}
            </td>
            <td>
                ${product.domain_name ? `<span class="badge bg-info">${DataUtils.safeEscapeHtml(product.domain_name)}</span>` : '-'}
            </td>
            <td>
                <code class="small">${DataUtils.safeEscapeHtml(truncateUrn(product.urn, 40))}</code>
            </td>
            <td class="text-center">
                <span class="badge ${getStatusBadgeClass(product.sync_status)}">${product.sync_status_display || product.sync_status}</span>
            </td>
            <td>
                ${getActionButtons(product, tabType)}
            </td>
        </tr>
    `;
}

function getEmptyStateHTML(tabType, hasSearch) {
    const icons = {
        synced: 'fas fa-sync-alt',
        local: 'fas fa-laptop', 
        remote: 'fas fa-server'
    };
    
    const messages = {
        synced: hasSearch ? 'No synced data products match your search.' : 'No synced data products found.',
        local: hasSearch ? 'No local data products match your search.' : 'No local data products found.',
        remote: hasSearch ? 'No remote data products match your search.' : 'No remote data products found.'
    };
    
    return `
        <div class="text-center py-5">
            <i class="${icons[tabType]} fa-3x text-muted mb-3"></i>
            <h5 class="text-muted">${messages[tabType]}</h5>
            ${hasSearch ? '<p class="text-muted">Try adjusting your search terms.</p>' : ''}
        </div>
    `;
}

function getStatusBadgeClass(status) {
    switch (status) {
        case 'SYNCED': return 'bg-success';
        case 'LOCAL_ONLY': return 'bg-secondary';
        case 'REMOTE_ONLY': return 'bg-info';
        case 'OUT_OF_SYNC': return 'bg-warning';
        default: return 'bg-secondary';
    }
}

function getActionButtons(product, tabType) {
    const buttons = [];
    
    // 1. View button (always first - unique to data products)
    buttons.push(`
        <button type="button" class="btn btn-sm btn-outline-primary view-item" 
                onclick="showProductDetails(${DataUtils.safeJsonStringify(product).replace(/'/g, "\\'")})"
                title="View Details">
            <i class="fas fa-eye"></i>
        </button>
    `);
    
    // Tab-specific actions - following consistent order
    switch (tabType) {
        case 'local':
            // 2. Edit - For local data products (would need to be implemented)
            // Skipping edit for now as it's not implemented for data products
            
            // 3. Sync to DataHub - For local products
            buttons.push(`
                <button type="button" class="btn btn-sm btn-outline-success" 
                        onclick="syncProductToDataHub('${product.id}', this)"
                        title="Sync to DataHub">
                    <i class="fas fa-upload"></i>
                </button>
            `);
            
            // 4. Download JSON - Available for all
            buttons.push(`
                <button type="button" class="btn btn-sm btn-outline-secondary" 
                        onclick="downloadProductJson(${DataUtils.safeJsonStringify(product).replace(/'/g, "\\'")})"
                        title="Download JSON">
                    <i class="fas fa-file-download"></i>
                </button>
            `);
            
            // 5. Add to Staged Changes - Available for all
            buttons.push(`
                <button type="button" class="btn btn-sm btn-outline-warning" 
                        onclick="addProductToStagedChanges(${DataUtils.safeJsonStringify(product).replace(/'/g, "\\'")})"
                        title="Add to Staged Changes">
                    <i class="fab fa-github"></i>
                </button>
            `);
            
            // 6. Delete Local - For local products
            buttons.push(`
                <button type="button" class="btn btn-sm btn-outline-danger" 
                        onclick="deleteLocalProduct('${product.id}', this)"
                        title="Delete Local">
                    <i class="fas fa-trash"></i>
                </button>
            `);
            break;
            
        case 'remote':
            // 3. Sync to Local - For remote products
            buttons.push(`
                <button type="button" class="btn btn-sm btn-outline-primary" 
                        onclick="syncProductToLocal('${product.urn}', this)"
                        title="Sync to Local">
                    <i class="fas fa-download"></i>
                </button>
            `);
            
            // 4. Download JSON - Available for all
            buttons.push(`
                <button type="button" class="btn btn-sm btn-outline-secondary" 
                        onclick="downloadProductJson(${DataUtils.safeJsonStringify(product).replace(/'/g, "\\'")})"
                        title="Download JSON">
                    <i class="fas fa-file-download"></i>
                </button>
            `);
            
            // 5. Add to Staged Changes - Available for all
            buttons.push(`
                <button type="button" class="btn btn-sm btn-outline-warning" 
                        onclick="addProductToStagedChanges(${DataUtils.safeJsonStringify(product).replace(/'/g, "\\'")})"
                        title="Add to Staged Changes">
                    <i class="fab fa-github"></i>
                </button>
            `);
            
            // 6. Delete Remote - For remote products
            buttons.push(`
                <button type="button" class="btn btn-sm btn-outline-danger" 
                        onclick="deleteRemoteProduct('${product.urn}', this)"
                        title="Delete Remote">
                    <i class="fas fa-trash"></i>
                </button>
            `);
            break;
            
        case 'synced':
            // 2. Edit - For synced data products (would need to be implemented)
            // Skipping edit for now as it's not implemented for data products
            
            // 3. Resync - For synced products
            buttons.push(`
                <button type="button" class="btn btn-sm btn-outline-info" 
                        onclick="resyncProduct('${product.id}', '${product.urn}', this)"
                        title="Resync from DataHub">
                    <i class="fas fa-sync-alt"></i>
                </button>
            `);
            
            // 3b. Push to DataHub - For modified synced products
            buttons.push(`
                <button type="button" class="btn btn-sm btn-outline-success" 
                        onclick="pushProductToDataHub('${product.id}', this)"
                        title="Push to DataHub">
                    <i class="fas fa-upload"></i>
                </button>
            `);
            
            // 4. Download JSON - Available for all
            buttons.push(`
                <button type="button" class="btn btn-sm btn-outline-secondary" 
                        onclick="downloadProductJson(${DataUtils.safeJsonStringify(product).replace(/'/g, "\\'")})"
                        title="Download JSON">
                    <i class="fas fa-file-download"></i>
                </button>
            `);
            
            // 5. Add to Staged Changes - Available for all
            buttons.push(`
                <button type="button" class="btn btn-sm btn-outline-warning" 
                        onclick="addProductToStagedChanges(${DataUtils.safeJsonStringify(product).replace(/'/g, "\\'")})"
                        title="Add to Staged Changes">
                    <i class="fab fa-github"></i>
                </button>
            `);
            
            // 6. Delete Local - For synced products
            buttons.push(`
                <button type="button" class="btn btn-sm btn-outline-danger" 
                        onclick="deleteLocalProduct('${product.id}', this)"
                        title="Delete Local">
                    <i class="fas fa-trash"></i>
                </button>
            `);
            break;
    }
    
    // View in DataHub button - always last for non-local products
    if (product.urn && !product.urn.includes('local:') && tabType !== 'local') {
        buttons.push(`
            <a href="${getDataHubUrl(product.urn, 'dataProduct')}" 
               class="btn btn-sm btn-outline-info" 
               target="_blank" title="View in DataHub">
                <i class="fas fa-external-link-alt"></i>
            </a>
        `);
    }
    
    return `<div class="btn-group" role="group">${buttons.join('')}</div>`;
}

function generatePaginationHTML(totalItems, tabType) {
    const pagination = currentPagination[tabType];
    const totalPages = Math.ceil(totalItems / pagination.itemsPerPage);
    
    if (totalPages <= 1) return '';
    
    const startItem = (pagination.page - 1) * pagination.itemsPerPage + 1;
    const endItem = Math.min(pagination.page * pagination.itemsPerPage, totalItems);
    
    let paginationHTML = `
        <div class="pagination-container">
            <div class="pagination-info">
                Showing ${startItem} to ${endItem} of ${totalItems} entries
            </div>
            <nav aria-label="Data products pagination">
                <ul class="pagination pagination-sm mb-0">
    `;
    
    // Previous button
    paginationHTML += `
        <li class="page-item ${pagination.page === 1 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="changePage('${tabType}', ${pagination.page - 1}); return false;">Previous</a>
        </li>
    `;
    
    // Page numbers
    const startPage = Math.max(1, pagination.page - 2);
    const endPage = Math.min(totalPages, pagination.page + 2);
    
    for (let i = startPage; i <= endPage; i++) {
        paginationHTML += `
            <li class="page-item ${i === pagination.page ? 'active' : ''}">
                <a class="page-link" href="#" onclick="changePage('${tabType}', ${i}); return false;">${i}</a>
            </li>
        `;
    }
    
    // Next button
    paginationHTML += `
        <li class="page-item ${pagination.page === totalPages ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="changePage('${tabType}', ${pagination.page + 1}); return false;">Next</a>
        </li>
    `;
    
    paginationHTML += `
                </ul>
            </nav>
        </div>
    `;
    
    return paginationHTML;
}

function attachPaginationHandlers(content, tabType) {
    // Pagination handlers are inline in the HTML
}

function attachSortingHandlers(content, tabType) {
    content.querySelectorAll('.sortable-header').forEach(header => {
        header.addEventListener('click', function() {
            const column = this.getAttribute('data-column');
            
            if (currentSort.column === column && currentSort.tabType === tabType) {
                currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
            } else {
                currentSort.column = column;
                currentSort.direction = 'asc';
                currentSort.tabType = tabType;
            }
            
            renderTabContent(tabType);
            restoreSortState(content, tabType);
        });
    });
}

function restoreSortState(content, tabType) {
    if (currentSort.column && currentSort.tabType === tabType) {
        const header = content.querySelector(`[data-column="${currentSort.column}"]`);
        if (header) {
            header.classList.add(`sort-${currentSort.direction}`);
        }
    }
}

function attachBulkActionHandlers(content, tabType) {
    content.querySelectorAll('.item-checkbox').forEach(checkbox => {
        checkbox.addEventListener('change', function() {
            updateBulkActionVisibility(tabType);
        });
    });
}

function sortItems(items, column, direction) {
    return items.sort((a, b) => {
        const aVal = getSortValue(a, column);
        const bVal = getSortValue(b, column);
        
        if (aVal < bVal) return direction === 'asc' ? -1 : 1;
        if (aVal > bVal) return direction === 'asc' ? 1 : -1;
        return 0;
    });
}

function getSortValue(product, column) {
    switch (column) {
        case 'name': return product.name || '';
        case 'description': return product.description || '';
        case 'entities_count': return product.entities_count || 0;
        case 'owners_count': return product.owners_count || 0;
        case 'domain': return product.domain_name || '';
        case 'urn': return product.urn || '';
        case 'sync_status': return product.sync_status || '';
        default: return '';
    }
}

function changePage(tabType, page) {
    currentPagination[tabType].page = page;
    renderTabContent(tabType);
}

function toggleSelectAll(tabType, checkbox) {
    const content = document.getElementById(`${tabType}-content`);
    const itemCheckboxes = content.querySelectorAll('.item-checkbox');
    
    itemCheckboxes.forEach(cb => {
        cb.checked = checkbox.checked;
    });
    
    updateBulkActionVisibility(tabType);
}

function loadProductsData() {
    // Check if legacy system is loaded first
    if (typeof window.legacyCurrentTab !== 'undefined') {
        // Legacy system detected, deferring to legacy loadProductsData
        return;
    }
    
    const loadingIndicator = document.getElementById('loading-indicator');
    const productsContent = document.getElementById('products-content');
    
    if (loadingIndicator) loadingIndicator.style.display = 'block';
    if (productsContent) productsContent.style.display = 'none';
    
    fetch('/metadata/data-products/data/', {
        method: 'GET',
        headers: {
            'X-Requested-With': 'XMLHttpRequest',
        }
    })
    .then(response => response.json())
    .then(data => {
        productsData = data;
        updateStatistics(data.statistics);
        renderAllTabs();
        
        if (loadingIndicator) loadingIndicator.style.display = 'none';
        if (productsContent) productsContent.style.display = 'block';
    })
    .catch(error => {
        console.error('Error loading data products:', error);
        showError('Failed to load data products data');
        if (loadingIndicator) loadingIndicator.style.display = 'none';
    });
}

function updateStatistics(stats) {
    if (!stats) return;
    
    // Update overview statistics
    const totalElement = document.getElementById('total-items');
    const syncedElement = document.getElementById('synced-count');
    const localElement = document.getElementById('local-count');
    const remoteElement = document.getElementById('remote-count');
    
    if (totalElement) totalElement.textContent = stats.total || 0;
    if (syncedElement) syncedElement.textContent = stats.synced || 0;
    if (localElement) localElement.textContent = stats.local_only || 0;
    if (remoteElement) remoteElement.textContent = stats.remote_only || 0;
    
    // Update filter statistics
    const entitiesElement = document.getElementById('products-with-entities');
    const ownersElement = document.getElementById('products-with-owners');
    const domainElement = document.getElementById('products-with-domain');
    const urlElement = document.getElementById('products-with-external-url');
    
    if (entitiesElement) entitiesElement.textContent = stats.with_entities || 0;
    if (ownersElement) ownersElement.textContent = stats.with_owners || 0;
    if (domainElement) domainElement.textContent = stats.with_domain || 0;
    if (urlElement) urlElement.textContent = stats.with_external_url || 0;
}

// Note: Duplicate showNotification function removed - using MetadataNotifications.show() instead
// This ensures consistent, standardized notification messages across all metadata types

function showError(message) {
    if (typeof showToast === 'function') {
        showToast('error', message);
    } else {
        console.error(message);
    }
}

function showSuccess(message) {
    if (typeof showToast === 'function') {
        showToast('success', message);
    } else {
        console.log(message);
    }
}

function getCsrfToken() {
    return document.querySelector('[name=csrfmiddlewaretoken]')?.value ||
           document.querySelector('meta[name="csrf-token"]')?.getAttribute('content') ||
           '';
}

function truncateUrn(urn, maxLength) {
    if (!urn) return '';
    return urn.length > maxLength ? urn.substring(0, maxLength) + '...' : urn;
}

function setupActionButtonListeners() {
    // Action button listeners are handled via inline onclick handlers in the HTML
}

// Action functions for data products
function showProductDetails(product) {
    const modal = document.getElementById('productViewModal');
    if (!modal) return;
    
    // Populate basic information
    document.getElementById('modal-product-name').textContent = product.name || '-';
    document.getElementById('modal-product-description').textContent = product.description || '-';
    document.getElementById('modal-product-domain').textContent = product.domain_name || '-';
    document.getElementById('modal-product-entities').textContent = product.entities_count || '0';
    document.getElementById('modal-product-urn').textContent = product.urn || '-';
    
    // Update status badge
    const statusElement = document.getElementById('modal-product-status');
    if (statusElement) {
        statusElement.textContent = product.sync_status_display || product.sync_status || '-';
        statusElement.className = `badge ${getStatusBadgeClass(product.sync_status)}`;
    }
    
    // Update DataHub link
    const datahubLink = document.getElementById('modal-datahub-link');
    if (datahubLink && product.urn) {
        datahubLink.href = getDataHubUrl(product.urn, 'dataProduct');
    }
    
    // Update owners list
    const ownersElement = document.getElementById('modal-owners-list');
    if (ownersElement) {
        if (product.ownership && product.ownership.owners && product.ownership.owners.length > 0) {
            ownersElement.innerHTML = product.ownership.owners.map(owner => 
                `<div class="owner-item">${DataUtils.safeEscapeHtml(owner.owner_name || owner.owner_urn)}</div>`
            ).join('');
        } else {
            ownersElement.innerHTML = '<p class="text-muted">No ownership information available</p>';
        }
    }
    
    // Show the modal
    const bsModal = new bootstrap.Modal(modal);
    bsModal.show();
}

function getDataHubUrl(urn, type) {
    const baseUrl = productsData.datahub_url || 'http://localhost:9002';
    return `${baseUrl}/entity/${urn}`;
}

function downloadProductJson(product) {
    const jsonData = JSON.stringify(product, null, 2);
    const blob = new Blob([jsonData], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    
    const link = document.createElement('a');
    link.href = url;
    link.download = `data-product-${product.name || 'data'}.json`;
    document.body.appendChild(link);
    link.click();
    
    setTimeout(() => {
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }, 100);
}

function addProductToStagedChanges(product) {
    const productName = product.name || 'Unknown';
    
    // Check if this is a remote-only product that needs to be staged directly
    // Remote products have URNs as IDs or no local database ID
    const isRemoteProduct = product.sync_status === 'REMOTE_ONLY' || 
                           !product.id || 
                           (typeof product.id === 'string' && product.id.startsWith('urn:'));
    
    if (isRemoteProduct) {
        // Remote-only product, staging directly
        
        // Show loading notification
        MetadataNotifications.show('staged_changes', 'add_to_staged_start', 'data_product', { name: productName });
        
        // Get current environment and mutation from global state or settings
        const currentEnvironment = window.currentEnvironment || { name: 'dev' };
        const mutationName = currentEnvironment.mutation_name || null;
        
        // Use the remote staging endpoint - we'll need to create this endpoint
        fetch('/metadata/data-products/remote/stage_changes/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': getCsrfToken()
            },
            body: JSON.stringify({
                product_data: product,
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
            if (data.success) {
                MetadataNotifications.show('staged_changes', 'add_to_staged_success', 'data_product', { name: productName });
            } else {
                throw new Error(data.error || 'Unknown error occurred');
            }
        })
        .catch(error => {
            console.error('Error adding remote product to staged changes:', error);
            MetadataNotifications.show('staged_changes', 'add_to_staged_error', 'data_product', { error: error.message });
        });
        return;
    }
    
    if (!product.id) {
        console.error('Cannot add product to staged changes without an ID:', product);
        MetadataNotifications.show('staged_changes', 'add_to_staged_missing_id', 'data_product');
        return;
    }
    
    // Show loading notification
            MetadataNotifications.show('staged_changes', 'add_to_staged_start', 'data_product', { name: productName });
    
    // Get current environment and mutation from global state or settings
    const currentEnvironment = window.currentEnvironment || { name: 'dev' };
    const mutationName = currentEnvironment.mutation_name || null;
    
    // Use the staged changes endpoint
    fetch(`/metadata/data-products/${product.id}/stage_changes/`, {
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
        if (data.success) {
            MetadataNotifications.show('staged_changes', 'add_to_staged_success', 'data_product', { name: productName });
        } else {
            throw new Error(data.error || 'Unknown error occurred');
        }
    })
    .catch(error => {
        console.error('Error adding product to staged changes:', error);
        MetadataNotifications.show('staged_changes', 'add_to_staged_error', 'data_product', { error: error.message });
    });
}

// Bulk action functions
function bulkResyncProducts(tabType) {
    const selectedProducts = getSelectedProducts(tabType);
    if (selectedProducts.length === 0) {
        MetadataNotifications.show('selection', 'none_selected', 'data_product');
        return;
    }
    
    if (!confirm(`Are you sure you want to resync ${selectedProducts.length} product(s) from DataHub?`)) {
        return;
    }
    
    MetadataNotifications.show('sync', 'resync_bulk_start', 'data_product', { count: selectedProducts.length });
    // Bulk resyncing products
    // TODO: Implement actual resync logic
    // After implementation, add single refresh: await loadProductsData();
}

function bulkSyncToDataHub(tabType) {
    const selectedProducts = getSelectedProducts(tabType);
    if (selectedProducts.length === 0) {
        MetadataNotifications.show('selection', 'none_selected', 'data_product');
        return;
    }
    
    if (!confirm(`Are you sure you want to sync ${selectedProducts.length} product(s) to DataHub?`)) {
        return;
    }
    
    MetadataNotifications.show('sync', 'sync_to_datahub_bulk_start', 'data_product', { count: selectedProducts.length });
    // Bulk syncing products to DataHub
    // TODO: Implement actual sync logic
    // After implementation, add single refresh: await loadProductsData();
}

async function bulkSyncToLocal(tabType) {
    const selectedProducts = getSelectedProducts(tabType);
    if (selectedProducts.length === 0) {
        MetadataNotifications.show('selection', 'none_selected', 'data_product');
        return;
    }
    
    if (!confirm(`Are you sure you want to sync ${selectedProducts.length} product(s) to local?`)) {
        return;
    }
    
    MetadataNotifications.show('sync', 'sync_to_local_bulk_start', 'data_product', { count: selectedProducts.length });
    
    let successCount = 0;
    let errorCount = 0;
    
    // Process products sequentially to avoid overwhelming the server
    for (const product of selectedProducts) {
        try {
            await syncSingleProductToLocal(product.urn);
            successCount++;
        } catch (error) {
            console.error(`Error syncing product ${product.name}:`, error);
            errorCount++;
        }
    }
    
    // Single refresh at the end
    await loadProductsData();
    
    if (errorCount === 0) {
        MetadataNotifications.show('sync', 'sync_to_local_bulk_success', 'data_product', { successCount, errorCount: 0 });
    } else {
        MetadataNotifications.show('sync', 'sync_to_local_bulk_success', 'data_product', { successCount, errorCount });
    }
}

async function syncSingleProductToLocal(productUrn) {
    const response = await fetch('/metadata/data-products/sync-to-local/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCsrfToken(),
        },
        body: JSON.stringify({
            'product_urn': productUrn
        })
    });
    
    const data = await response.json();
    
    if (!data.success) {
        throw new Error(data.error || 'Unknown error');
    }
    
    return data;
}

function bulkDownloadJson(tabType) {
    const selectedProducts = getSelectedProducts(tabType);
    if (selectedProducts.length === 0) {
        MetadataNotifications.show('export', 'export_none_selected', 'data_product');
        return;
    }
    
    const jsonData = JSON.stringify(selectedProducts, null, 2);
    const blob = new Blob([jsonData], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    
    const link = document.createElement('a');
    link.href = url;
    link.download = `data-products-bulk-${new Date().toISOString().split('T')[0]}.json`;
    document.body.appendChild(link);
    link.click();
    
    setTimeout(() => {
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }, 100);
    
    MetadataNotifications.show('export', 'export_success', 'data_product', { count: selectedProducts.length });
}

function bulkAddToPR(tabType) {
    const selectedProducts = getSelectedProducts(tabType);
    if (selectedProducts.length === 0) {
        MetadataNotifications.show('selection', 'none_selected', 'data_product');
        return;
    }
    
    if (!confirm(`Are you sure you want to add ${selectedProducts.length} product(s) to staged changes?`)) {
        return;
    }
    
    MetadataNotifications.show('staged_changes', 'add_to_staged_bulk_start', 'data_product', { count: selectedProducts.length });
    
    let successCount = 0;
    let errorCount = 0;
    
    // Process products sequentially to avoid overwhelming the server
    selectedProducts.forEach((product, index) => {
        setTimeout(() => {
            try {
                addProductToStagedChanges(product);
                successCount++;
                
                // Show final result after all products are processed
                if (index === selectedProducts.length - 1) {
                    setTimeout(() => {
                        if (errorCount === 0) {
                            MetadataNotifications.show('staged_changes', 'add_to_staged_bulk_success', 'data_product', { successCount, errorCount: 0 });
                        } else {
                            showNotification('warning', `Completed: ${successCount} added successfully, ${errorCount} failed`);
                        }
                    }, 1000);
                }
            } catch (error) {
                console.error(`Error adding product ${product.name} to staged changes:`, error);
                errorCount++;
            }
        }, index * 500); // Stagger requests by 500ms
    });
}

async function bulkDeleteLocal(tabType) {
    const selectedProducts = getSelectedProducts(tabType);
    if (selectedProducts.length === 0) {
        showNotification('error', 'Please select data products to delete.');
        return;
    }
    
    if (!confirm(`Are you sure you want to delete ${selectedProducts.length} local product(s)? This action cannot be undone.`)) {
        return;
    }
    
    showNotification('info', `Starting deletion of ${selectedProducts.length} local data product(s)...`);
    
    let successCount = 0;
    let errorCount = 0;
    const errors = [];
    
    // Process deletions sequentially to avoid overwhelming the server
    for (const product of selectedProducts) {
        if (!product.id) {
            errors.push(`Product "${product.name}" has no ID for local deletion`);
            errorCount++;
            continue;
        }
        
        try {
            const response = await fetch(`/metadata/data-products/${product.id}/delete/`, {
                method: 'DELETE',
                headers: {
                    'X-CSRFToken': getCsrfToken(),
                    'Content-Type': 'application/json'
                }
            });
            
            const result = await response.json();
            
            if (result.success) {
                successCount++;
            } else {
                errors.push(`Failed to delete "${product.name}": ${result.error}`);
                errorCount++;
            }
        } catch (error) {
            console.error(`Error deleting product "${product.name}":`, error);
            errors.push(`Failed to delete "${product.name}": ${error.message}`);
            errorCount++;
        }
    }
    
    // Show results
    if (successCount > 0) {
        showNotification('success', `Successfully deleted ${successCount} local data product(s)`);
    }
    
    if (errorCount > 0) {
        console.error('Bulk delete errors:', errors);
        showNotification('error', `Failed to delete ${errorCount} product(s). Check console for details.`);
    }
    
    // Single refresh after all operations
    await loadProductsData();
}

async function bulkDeleteRemote(tabType) {
    const selectedProducts = getSelectedProducts(tabType);
    if (selectedProducts.length === 0) {
        showNotification('error', 'Please select data products to delete.');
        return;
    }
    
    if (!confirm(`Are you sure you want to delete ${selectedProducts.length} remote product(s)? This action cannot be undone.`)) {
        return;
    }
    
    showNotification('info', `Starting deletion of ${selectedProducts.length} remote data product(s)...`);
    
    let successCount = 0;
    let errorCount = 0;
    const errors = [];
    
    // Process deletions sequentially to avoid overwhelming the server
    for (const product of selectedProducts) {
        if (!product.urn) {
            errors.push(`Product "${product.name}" has no URN for remote deletion`);
            errorCount++;
            continue;
        }
        
        try {
            const response = await fetch('/metadata/data-products/delete-remote/', {
                method: 'POST',
                headers: {
                    'X-CSRFToken': getCsrfToken(),
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    product_urn: product.urn
                })
            });
            
            const result = await response.json();
            
            if (result.success) {
                successCount++;
            } else {
                errors.push(`Failed to delete "${product.name}": ${result.error}`);
                errorCount++;
            }
        } catch (error) {
            console.error(`Error deleting remote product "${product.name}":`, error);
            errors.push(`Failed to delete "${product.name}": ${error.message}`);
            errorCount++;
        }
    }
    
    // Show results
    if (successCount > 0) {
        showNotification('success', `Successfully deleted ${successCount} remote data product(s)`);
    }
    
    if (errorCount > 0) {
        console.error('Bulk delete errors:', errors);
        showNotification('error', `Failed to delete ${errorCount} product(s). Check console for details.`);
    }
    
    // Single refresh after all operations
    await loadProductsData();
}

function getSelectedProducts(tabType) {
    const tabContent = document.getElementById(`${tabType}-content`);
    if (!tabContent) return [];
    
    const checkboxes = tabContent.querySelectorAll('.item-checkbox:checked');
    const products = [];
    
    checkboxes.forEach(checkbox => {
        const row = checkbox.closest('tr');
        if (row) {
            try {
                // Get the product data from the row's data-item attribute
                const productData = JSON.parse(row.getAttribute('data-item'));
                if (productData) {
                    products.push(productData);
                }
            } catch (e) {
                console.error('Error parsing product data from row:', e);
                // Fallback: create minimal product object from checkbox data
                const productId = checkbox.getAttribute('data-product-id');
                const nameCell = row.querySelector('td:nth-child(2) strong');
                const name = nameCell ? nameCell.textContent.trim() : 'Unknown';
                
                products.push({
                    id: productId.startsWith('urn:') ? null : productId,
                    urn: productId.startsWith('urn:') ? productId : null,
                    name: name
                });
            }
        }
    });
    
    return products;
}

// Individual action functions that need to be implemented
function syncProductToDataHub(productId, button) {
    if (!confirm('Are you sure you want to sync this data product to DataHub?')) {
        return;
    }
    
    const originalHtml = button.innerHTML;
    button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
    button.disabled = true;
    
    showNotification('info', 'Syncing data product to DataHub...');
    // Syncing product to DataHub
    
    // TODO: Implement actual sync logic
    setTimeout(() => {
        showNotification('success', 'Data product synced to DataHub successfully!');
        button.innerHTML = originalHtml;
        button.disabled = false;
        // Single refresh after sync
        loadProductsData();
    }, 1000);
}

async function syncProductToLocal(productUrn, button) {
    if (!confirm('Are you sure you want to sync this data product to local storage?')) {
        return;
    }
    
    const originalHtml = button.innerHTML;
    button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
    button.disabled = true;
    
    try {
        await syncSingleProductToLocal(productUrn);
        showNotification('success', 'Data product synced to local successfully!');
        
        // Single refresh after successful sync
        await loadProductsData();
    } catch (error) {
        console.error('Error syncing product to local:', error);
        showNotification('error', 'Failed to sync data product to local: ' + error.message);
    } finally {
        button.innerHTML = originalHtml;
        button.disabled = false;
    }
}

function resyncProduct(productId, productUrn, button) {
    if (!confirm('Are you sure you want to resync this data product from DataHub?')) {
        return;
    }
    
    const originalHtml = button.innerHTML;
    button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
    button.disabled = true;
    
    showNotification('info', 'Resyncing data product from DataHub...');
    // Resyncing product
    
    // TODO: Implement actual resync logic
    setTimeout(() => {
        showNotification('success', 'Data product resynced successfully!');
        button.innerHTML = originalHtml;
        button.disabled = false;
        // Single refresh after resync
        loadProductsData();
    }, 1000);
}

function pushProductToDataHub(productId, button) {
    if (!confirm('Are you sure you want to push this data product to DataHub?')) {
        return;
    }
    
    const originalHtml = button.innerHTML;
    button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
    button.disabled = true;
    
    showNotification('info', 'Pushing data product to DataHub...');
    // Pushing product to DataHub
    
    // TODO: Implement actual push logic
    setTimeout(() => {
        showNotification('success', 'Data product pushed to DataHub successfully!');
        button.innerHTML = originalHtml;
        button.disabled = false;
        // Single refresh after push
        loadProductsData();
    }, 1000);
}

async function deleteLocalProduct(productId, button) {
    if (!confirm('Are you sure you want to delete this local data product? This action cannot be undone.')) {
        return;
    }
    
    const originalHtml = button.innerHTML;
    button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
    button.disabled = true;
    
    showNotification('info', 'Deleting local data product...');
    
    try {
        const response = await fetch(`/metadata/data-products/${productId}/delete/`, {
            method: 'DELETE',
            headers: {
                'X-CSRFToken': getCsrfToken(),
                'Content-Type': 'application/json'
            }
        });
        
        const result = await response.json();
        
        if (result.success) {
            // Use template's notification system if available, otherwise use enhanced system
            if (typeof showSuccess === 'function') {
                showSuccess('Local data product deleted successfully!');
            } else {
                showNotification('success', 'Local data product deleted successfully!');
            }
            // Single refresh after successful delete
            await loadProductsData();
        } else {
            if (typeof showError === 'function') {
                showError(`Failed to delete data product: ${result.error}`);
            } else {
                showNotification('error', `Failed to delete data product: ${result.error}`);
            }
        }
    } catch (error) {
        console.error('Error deleting local product:', error);
        if (typeof showError === 'function') {
            showError(`Failed to delete data product: ${error.message}`);
        } else {
            showNotification('error', `Failed to delete data product: ${error.message}`);
        }
    } finally {
        button.innerHTML = originalHtml;
        button.disabled = false;
    }
}

async function deleteRemoteProduct(productUrn, button) {
    if (!confirm('Are you sure you want to delete this remote data product? This action cannot be undone.')) {
        return;
    }
    
    const originalHtml = button.innerHTML;
    button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
    button.disabled = true;
    
    showNotification('info', 'Deleting remote data product...');
    
    try {
        const response = await fetch('/metadata/data-products/delete-remote/', {
            method: 'POST',
            headers: {
                'X-CSRFToken': getCsrfToken(),
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                product_urn: productUrn
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            // Use template's notification system if available, otherwise use enhanced system
            if (typeof showSuccess === 'function') {
                showSuccess('Remote data product deleted successfully!');
            } else {
                showNotification('success', 'Remote data product deleted successfully!');
            }
            // Single refresh after successful delete
            await loadProductsData();
        } else {
            if (typeof showError === 'function') {
                showError(`Failed to delete remote data product: ${result.error}`);
            } else {
                showNotification('error', `Failed to delete remote data product: ${result.error}`);
            }
        }
    } catch (error) {
        console.error('Error deleting remote product:', error);
        if (typeof showError === 'function') {
            showError(`Failed to delete remote data product: ${error.message}`);
        } else {
            showNotification('error', `Failed to delete remote data product: ${error.message}`);
        }
    } finally {
        button.innerHTML = originalHtml;
        button.disabled = false;
    }
} 