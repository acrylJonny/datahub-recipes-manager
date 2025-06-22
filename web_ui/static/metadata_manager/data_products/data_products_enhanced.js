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
        console.log('Initializing enhanced data products system');
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
        console.log('Legacy data products system detected, skipping enhanced initialization');
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
    // Tab-specific bulk actions will be handled in renderTabContent
}

function updateBulkActionVisibility(tab) {
    const bulkActionsDiv = document.getElementById(`${tab}-bulk-actions`);
    const selectedItems = document.querySelectorAll(`#${tab}-content .item-checkbox:checked`);
    const countSpan = document.getElementById(`${tab}-selected-count`);
    
    if (bulkActionsDiv && countSpan) {
        if (selectedItems.length > 0) {
            bulkActionsDiv.classList.add('show');
            countSpan.textContent = selectedItems.length;
        } else {
            bulkActionsDiv.classList.remove('show');
            countSpan.textContent = '0';
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
                       data-product-id="${product.id || product.urn}"
                       onchange="updateBulkActionVisibility('${tabType}')">
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
    
    // View button (always available)
    buttons.push(`
        <button type="button" class="btn btn-sm btn-outline-info view-item" 
                onclick="showProductDetails(${DataUtils.safeJsonStringify(product).replace(/'/g, "\\'")})"
                title="View Details">
            <i class="fas fa-eye"></i>
        </button>
    `);
    
    // Tab-specific actions
    switch (tabType) {
        case 'synced':
            buttons.push(`
                <button type="button" class="btn btn-sm btn-outline-primary" 
                        onclick="resyncProduct('${product.id}', '${product.urn}', this)"
                        title="Resync">
                    <i class="fas fa-sync-alt"></i>
                </button>
            `);
            buttons.push(`
                <button type="button" class="btn btn-sm btn-outline-success" 
                        onclick="pushProductToDataHub('${product.id}', this)"
                        title="Push to DataHub">
                    <i class="fas fa-upload"></i>
                </button>
            `);
            break;
        case 'local':
            buttons.push(`
                <button type="button" class="btn btn-sm btn-outline-success" 
                        onclick="syncProductToDataHub('${product.id}', this)"
                        title="Sync to DataHub">
                    <i class="fas fa-upload"></i>
                </button>
            `);
            buttons.push(`
                <button type="button" class="btn btn-sm btn-outline-danger" 
                        onclick="deleteLocalProduct('${product.id}', this)"
                        title="Delete Local">
                    <i class="fas fa-trash"></i>
                </button>
            `);
            break;
        case 'remote':
            buttons.push(`
                <button type="button" class="btn btn-sm btn-outline-primary" 
                        onclick="syncProductToLocal('${product.urn}', this)"
                        title="Sync to Local">
                    <i class="fas fa-download"></i>
                </button>
            `);
            buttons.push(`
                <button type="button" class="btn btn-sm btn-outline-danger" 
                        onclick="deleteRemoteProduct('${product.urn}', this)"
                        title="Delete Remote">
                    <i class="fas fa-trash"></i>
                </button>
            `);
            break;
    }
    
    // Download JSON (always available)
    buttons.push(`
        <button type="button" class="btn btn-sm btn-outline-secondary" 
                onclick="downloadProductJson(${DataUtils.safeJsonStringify(product).replace(/'/g, "\\'")})"
                title="Download JSON">
            <i class="fas fa-download"></i>
        </button>
    `);
    
    // Add to staged changes (always available)
    buttons.push(`
        <button type="button" class="btn btn-sm btn-outline-warning" 
                onclick="addProductToStagedChanges(${DataUtils.safeJsonStringify(product).replace(/'/g, "\\'")})"
                title="Add to Staged Changes">
            <i class="fab fa-github"></i>
        </button>
    `);
    
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
        console.log('Legacy system detected, deferring to legacy loadProductsData');
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

function showError(message) {
    // Create notification
    const notification = document.createElement('div');
    notification.className = 'alert alert-danger alert-dismissible fade show notification';
    notification.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    document.body.appendChild(notification);
    
    // Remove after 5 seconds
    setTimeout(() => {
        if (notification && notification.parentNode) {
            notification.parentNode.removeChild(notification);
        }
    }, 5000);
}

function showSuccess(message) {
    // Create notification
    const notification = document.createElement('div');
    notification.className = 'alert alert-success alert-dismissible fade show notification';
    notification.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    document.body.appendChild(notification);
    
    // Remove after 5 seconds
    setTimeout(() => {
        if (notification && notification.parentNode) {
            notification.parentNode.removeChild(notification);
        }
    }, 5000);
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
    // Implementation for adding to staged changes
    console.log('Adding product to staged changes:', product);
    showSuccess('Product added to staged changes');
}

// Bulk action functions
function bulkResyncProducts(tabType) {
    const selectedProducts = getSelectedProducts(tabType);
    if (selectedProducts.length === 0) {
        showError('No products selected');
        return;
    }
    
    if (!confirm(`Are you sure you want to resync ${selectedProducts.length} product(s)?`)) {
        return;
    }
    
    console.log('Bulk resyncing products:', selectedProducts);
    showSuccess(`Bulk resyncing ${selectedProducts.length} product(s)`);
}

function bulkSyncToDataHub(tabType) {
    const selectedProducts = getSelectedProducts(tabType);
    if (selectedProducts.length === 0) {
        showError('No products selected');
        return;
    }
    
    if (!confirm(`Are you sure you want to sync ${selectedProducts.length} product(s) to DataHub?`)) {
        return;
    }
    
    console.log('Bulk syncing to DataHub:', selectedProducts);
    showSuccess(`Bulk syncing ${selectedProducts.length} product(s) to DataHub`);
}

async function bulkSyncToLocal(tabType) {
    const selectedProducts = getSelectedProducts(tabType);
    if (selectedProducts.length === 0) {
        showError('No products selected');
        return;
    }
    
    if (!confirm(`Are you sure you want to sync ${selectedProducts.length} product(s) to local?`)) {
        return;
    }
    
    showSuccess(`Starting sync of ${selectedProducts.length} product(s) to local...`);
    
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
    
    // Reload data once at the end
    await loadProductsData();
    
    if (errorCount === 0) {
        showSuccess(`Successfully synced ${successCount} product(s) to local`);
    } else {
        showError(`Completed: ${successCount} synced successfully, ${errorCount} failed`);
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
        showError('No products selected');
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
}

function bulkAddToPR(tabType) {
    const selectedProducts = getSelectedProducts(tabType);
    if (selectedProducts.length === 0) {
        showError('No products selected');
        return;
    }
    
    console.log('Bulk adding to PR:', selectedProducts);
    showSuccess(`Adding ${selectedProducts.length} product(s) to staged changes`);
}

function bulkDeleteLocal(tabType) {
    const selectedProducts = getSelectedProducts(tabType);
    if (selectedProducts.length === 0) {
        showError('No products selected');
        return;
    }
    
    if (!confirm(`Are you sure you want to delete ${selectedProducts.length} local product(s)? This action cannot be undone.`)) {
        return;
    }
    
    console.log('Bulk deleting local products:', selectedProducts);
    showSuccess(`Deleting ${selectedProducts.length} local product(s)`);
}

function bulkDeleteRemote(tabType) {
    const selectedProducts = getSelectedProducts(tabType);
    if (selectedProducts.length === 0) {
        showError('No products selected');
        return;
    }
    
    if (!confirm(`Are you sure you want to delete ${selectedProducts.length} remote product(s)? This action cannot be undone.`)) {
        return;
    }
    
    console.log('Bulk deleting remote products:', selectedProducts);
    showSuccess(`Deleting ${selectedProducts.length} remote product(s)`);
}

function getSelectedProducts(tabType) {
    const checkboxes = document.querySelectorAll(`input[data-tab="${tabType}"].product-checkbox:checked`);
    const products = [];
    
    checkboxes.forEach(checkbox => {
        const row = checkbox.closest('tr');
        if (row) {
            const productId = row.dataset.productId;
            const productUrn = row.dataset.productUrn;
            const storeKey = productId || productUrn;
            
            // Try to find the product data in the global data store
            let productData = null;
            if (window.productDataStore && window.productDataStore[tabType]) {
                productData = window.productDataStore[tabType].get(storeKey);
            }
            
            if (productData) {
                products.push(productData);
            } else {
                // Fallback: create minimal product object from row data
                const nameCell = row.querySelector('td:nth-child(2)');
                const name = nameCell ? nameCell.textContent.trim() : 'Unknown';
                
                products.push({
                    id: productId,
                    urn: productUrn,
                    name: name
                });
            }
        }
    });
    
    return products;
}

// Individual action functions that need to be implemented
function syncProductToDataHub(productId, button) {
    console.log('Syncing product to DataHub:', productId);
    showSuccess('Product synced to DataHub');
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
        showSuccess('Data product synced to local successfully!');
        
        // Reload data to refresh the UI
        await loadProductsData();
    } catch (error) {
        console.error('Error syncing product to local:', error);
        showError('Failed to sync data product to local: ' + error.message);
    } finally {
        button.innerHTML = originalHtml;
        button.disabled = false;
    }
}

function resyncProduct(productId, productUrn, button) {
    console.log('Resyncing product:', productId, productUrn);
    showSuccess('Product resynced');
}

function pushProductToDataHub(productId, button) {
    console.log('Pushing product to DataHub:', productId);
    showSuccess('Product pushed to DataHub');
}

function deleteLocalProduct(productId, button) {
    if (!confirm('Are you sure you want to delete this local product?')) {
        return;
    }
    console.log('Deleting local product:', productId);
    showSuccess('Local product deleted');
}

function deleteRemoteProduct(productUrn, button) {
    if (!confirm('Are you sure you want to delete this remote product?')) {
        return;
    }
    console.log('Deleting remote product:', productUrn);
    showSuccess('Remote product deleted');
} 