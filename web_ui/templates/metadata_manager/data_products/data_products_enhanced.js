let productsData = {};
let currentFilters = new Set();
let currentOverviewFilter = null;

// Pagination settings
const ITEMS_PER_PAGE = 25;
let currentSyncedPage = 1;
let currentLocalPage = 1;
let currentRemotePage = 1;

// DOM ready
document.addEventListener('DOMContentLoaded', function() {
    loadDataProductsData();
    setupEventListeners();
    setupFilterListeners();
    setupBulkActions();
    setupSearchHandlers();
});

function setupEventListeners() {
    // Refresh button
    document.getElementById('refreshProducts').addEventListener('click', function() {
        loadDataProductsData();
    });
    
    // Tab switching
    document.querySelectorAll('#productTabs button[data-bs-toggle="tab"]').forEach(tab => {
        tab.addEventListener('shown.bs.tab', function(e) {
            const tabId = e.target.getAttribute('aria-controls');
            renderTab(tabId);
        });
    });
    
    // Modal view handlers
    document.addEventListener('click', function(e) {
        if (e.target.classList.contains('view-item') || e.target.closest('.view-item')) {
            const button = e.target.classList.contains('view-item') ? e.target : e.target.closest('.view-item');
            const row = button.closest('tr');
            const productData = JSON.parse(row.getAttribute('data-item'));
            showProductDetails(productData);
        }
    });
}

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
                // Deselect if clicking same filter
                currentOverviewFilter = null;
            } else {
                // Select new filter
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
            
            selectAllCheckbox.checked = allCheckboxes.length > 0 && allCheckboxes.length === checkedCheckboxes.length;
            selectAllCheckbox.indeterminate = checkedCheckboxes.length > 0 && checkedCheckboxes.length < allCheckboxes.length;
        }
    });
    
    // Bulk action buttons
    document.getElementById('bulkSync')?.addEventListener('click', handleBulkSync);
    document.getElementById('bulkAddToPR')?.addEventListener('click', handleBulkAddToPR);
    document.getElementById('bulkDelete')?.addEventListener('click', handleBulkDelete);
    document.getElementById('bulkDownload')?.addEventListener('click', handleBulkDownload);
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

function loadDataProductsData() {
    const loadingIndicator = document.getElementById('loading-indicator');
    const productsContent = document.getElementById('products-content');
    
    loadingIndicator.style.display = 'block';
    productsContent.style.display = 'none';
    
    fetch('{% url "metadata_manager:data_products_data" %}', {
        method: 'GET',
        headers: {
            'X-Requested-With': 'XMLHttpRequest',
        }
    })
    .then(response => response.json())
    .then(data => {
        productsData = data;
        updateStatistics();
        renderAllTabs();
        
        loadingIndicator.style.display = 'none';
        productsContent.style.display = 'block';
    })
    .catch(error => {
        console.error('Error loading data products:', error);
        showError('Failed to load data products data');
        loadingIndicator.style.display = 'none';
    });
}

function updateStatistics() {
    const stats = productsData.statistics || {};
    
    // Overview statistics
    document.getElementById('total-items').textContent = stats.total || 0;
    document.getElementById('synced-count').textContent = stats.synced || 0;
    document.getElementById('local-count').textContent = stats.local_only || 0;
    document.getElementById('remote-count').textContent = stats.remote_only || 0;
    
    // Content statistics
    document.getElementById('products-with-entities').textContent = stats.with_entities || 0;
    document.getElementById('products-with-owners').textContent = stats.with_owners || 0;
    document.getElementById('products-with-relationships').textContent = stats.with_relationships || 0;
    document.getElementById('products-with-domain').textContent = stats.with_domain || 0;
    document.getElementById('products-with-external-url').textContent = stats.with_external_url || 0;
    
    // Status statistics
    document.getElementById('status-synced-count').textContent = stats.status_synced || 0;
    document.getElementById('status-local-only-count').textContent = stats.status_local_only || 0;
    document.getElementById('status-remote-only-count').textContent = stats.status_remote_only || 0;
    document.getElementById('status-modified-count').textContent = stats.status_modified || 0;
    
    // Tab badges
    document.getElementById('synced-badge').textContent = stats.synced || 0;
    document.getElementById('local-badge').textContent = stats.local_only || 0;
    document.getElementById('remote-badge').textContent = stats.remote_only || 0;
}

function applyFilters() {
    renderAllTabs();
}

function filterProducts(products) {
    if (!products) return [];
    
    return products.filter(product => {
        // Apply overview filter (single select)
        if (currentOverviewFilter) {
            switch (currentOverviewFilter) {
                case 'total':
                    // Show all - no additional filtering
                    break;
                case 'synced':
                    if (product.sync_status !== 'SYNCED') return false;
                    break;
                case 'local':
                    if (product.sync_status !== 'LOCAL_ONLY') return false;
                    break;
                case 'remote':
                    if (product.sync_status !== 'REMOTE_ONLY') return false;
                    break;
            }
        }
        
        // Apply multi-select filters
        for (const filter of currentFilters) {
            switch (filter) {
                case 'has-entities':
                    if (!product.entities_count || product.entities_count === 0) return false;
                    break;
                case 'has-owners':
                    if (!product.owners_count || product.owners_count === 0) return false;
                    break;
                case 'has-relationships':
                    if (!product.relationships_count || product.relationships_count === 0) return false;
                    break;
                case 'has-domain':
                    if (!product.domain_urn) return false;
                    break;
                case 'has-external-url':
                    if (!product.external_url) return false;
                    break;
                case 'status-synced':
                    if (product.sync_status !== 'SYNCED') return false;
                    break;
                case 'status-local-only':
                    if (product.sync_status !== 'LOCAL_ONLY') return false;
                    break;
                case 'status-remote-only':
                    if (product.sync_status !== 'REMOTE_ONLY') return false;
                    break;
                case 'status-modified':
                    if (product.sync_status !== 'MODIFIED') return false;
                    break;
            }
        }
        
        return true;
    });
}

function renderAllTabs() {
    renderTab('synced-items');
    renderTab('local-items');
    renderTab('remote-items');
}

function renderTab(tabId) {
    const tabType = tabId.replace('-items', '');
    let products = [];
    
    switch (tabType) {
        case 'synced':
            products = productsData.synced_products || [];
            break;
        case 'local':
            products = productsData.local_products || [];
            break;
        case 'remote':
            products = productsData.remote_products || [];
            break;
    }
    
    // Apply filters
    products = filterProducts(products);
    
    // Apply search
    const searchTerm = document.getElementById(`${tabType}-search`)?.value?.toLowerCase();
    if (searchTerm) {
        products = products.filter(product => {
            const name = (product.properties?.name || product.name || '').toLowerCase();
            const description = (product.properties?.description || product.description || '').toLowerCase();
            const domain = (product.domain?.domain?.properties?.name || '').toLowerCase();
            const urn = (product.urn || '').toLowerCase();
            
            return name.includes(searchTerm) || 
                   description.includes(searchTerm) || 
                   domain.includes(searchTerm) || 
                   urn.includes(searchTerm);
        });
    }
    
    // Get current page for this tab
    let currentPage;
    switch (tabType) {
        case 'synced': currentPage = currentSyncedPage; break;
        case 'local': currentPage = currentLocalPage; break;
        case 'remote': currentPage = currentRemotePage; break;
    }
    
    // Calculate pagination
    const totalItems = products.length;
    const totalPages = Math.ceil(totalItems / ITEMS_PER_PAGE);
    const startIndex = (currentPage - 1) * ITEMS_PER_PAGE;
    const endIndex = Math.min(startIndex + ITEMS_PER_PAGE, totalItems);
    const paginatedProducts = products.slice(startIndex, endIndex);
    
    // Render table
    const contentDiv = document.getElementById(`${tabType}-content`);
    if (paginatedProducts.length === 0) {
        contentDiv.innerHTML = `
            <div class="text-center py-5">
                <i class="fas fa-cube fa-3x text-muted mb-3"></i>
                <h5>No data products found</h5>
                <p class="text-muted">Try adjusting your search or filters</p>
            </div>
        `;
        return;
    }
    
    const tableHTML = `
        <div class="table-responsive">
            <table class="table table-hover mb-0">
                <thead>
                    <tr>
                        <th class="checkbox-column">
                            <input type="checkbox" class="form-check-input" onclick="toggleAllInTab('${tabType}')">
                        </th>
                        <th>Name</th>
                        <th>Domain</th>
                        <th>Description</th>
                        <th>Entities</th>
                        <th>URN</th>
                        <th>Status</th>
                        <th>Owners</th>
                        <th>Relationships</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    ${paginatedProducts.map(product => renderProductRow(product, tabType)).join('')}
                </tbody>
            </table>
        </div>
        ${renderPagination(currentPage, totalPages, totalItems, startIndex + 1, endIndex, tabType)}
    `;
    
    contentDiv.innerHTML = tableHTML;
}

function renderProductRow(product, tabType) {
    const statusClass = getStatusBadgeClass(product.sync_status);
    const typeIcon = 'fas fa-cube text-primary';
    
    // Extract product data
    const name = product.properties?.name || product.name || 'Unnamed';
    const description = product.properties?.description || product.description || '';
    const domain = product.domain?.domain?.properties?.name || '';
    const entityCount = product.entities_count || 0;
    const urn = product.urn || '';
    
    return `
        <tr data-item='${JSON.stringify(product)}'>
            <td class="checkbox-column">
                <input type="checkbox" class="form-check-input item-checkbox" data-product-id="${product.id || product.urn}">
            </td>
            <td>
                <div class="d-flex align-items-center">
                    <i class="${typeIcon} me-2"></i>
                    <strong>${escapeHtml(name)}</strong>
                </div>
            </td>
            <td>
                ${domain ? `<span class="badge bg-info">${escapeHtml(domain)}</span>` : '<span class="text-muted">None</span>'}
            </td>
            <td class="description-cell">
                <div class="description-preview" title="${escapeHtml(description)}">
                    ${escapeHtml(description || 'No description')}
                </div>
            </td>
            <td>
                <span class="badge bg-primary">${entityCount}</span>
            </td>
            <td>
                <code class="small text-truncate d-block" style="max-width: 200px;" title="${escapeHtml(urn)}">${escapeHtml(truncateUrn(urn, 30))}</code>
            </td>
            <td>
                <span class="badge ${statusClass}">${product.sync_status_display || product.sync_status}</span>
            </td>
            <td>
                ${product.owners_count ? `
                    <div class="d-flex align-items-center">
                        <i class="fas fa-users me-1"></i>
                        <span class="badge bg-info">${product.owners_count}</span>
                        ${product.owner_names && product.owner_names.length > 0 ? `
                            <small class="ms-2 text-muted">${product.owner_names.slice(0, 2).join(', ')}${product.owner_names.length > 2 ? '...' : ''}</small>
                        ` : ''}
                    </div>
                ` : `<span class="text-muted">None</span>`}
            </td>
            <td>
                ${product.relationships_count ? `
                    <div class="d-flex align-items-center">
                        <i class="fas fa-project-diagram me-1"></i>
                        <span class="badge bg-secondary">${product.relationships_count}</span>
                    </div>
                ` : `<span class="text-muted">None</span>`}
            </td>
            <td>
                <div class="btn-group" role="group">
                    <button type="button" class="btn btn-sm btn-outline-primary view-item" 
                            title="View Details">
                        <i class="fas fa-eye"></i>
                    </button>
                    ${product.urn && !product.urn.includes('local:') ? `
                        <a href="${getDataHubUrl(product.urn, 'product')}" 
                           class="btn btn-sm btn-outline-info" 
                           target="_blank" title="View in DataHub">
                            <i class="fas fa-external-link-alt"></i>
                        </a>
                    ` : ''}
                    ${getActionButtons(product, tabType)}
                </div>
            </td>
        </tr>
    `;
}

function renderPagination(currentPage, totalPages, totalItems, startItem, endItem, tabType) {
    if (totalPages <= 1) return '';
    
    const prevDisabled = currentPage <= 1 ? 'disabled' : '';
    const nextDisabled = currentPage >= totalPages ? 'disabled' : '';
    
    let paginationHTML = `
        <div class="pagination-container">
            <div class="pagination-info">
                Showing ${startItem}-${endItem} of ${totalItems} data products
            </div>
            <nav aria-label="Data products pagination">
                <ul class="pagination mb-0">
                    <li class="page-item ${prevDisabled}">
                        <a class="page-link" href="#" onclick="changePage('${tabType}', ${currentPage - 1})">
                            <i class="fas fa-chevron-left"></i>
                        </a>
                    </li>
    `;
    
    // Page numbers
    const maxVisiblePages = 5;
    let startPage = Math.max(1, currentPage - Math.floor(maxVisiblePages / 2));
    let endPage = Math.min(totalPages, startPage + maxVisiblePages - 1);
    
    if (endPage - startPage < maxVisiblePages - 1) {
        startPage = Math.max(1, endPage - maxVisiblePages + 1);
    }
    
    for (let i = startPage; i <= endPage; i++) {
        const activeClass = i === currentPage ? 'active' : '';
        paginationHTML += `
            <li class="page-item ${activeClass}">
                <a class="page-link" href="#" onclick="changePage('${tabType}', ${i})">${i}</a>
            </li>
        `;
    }
    
    paginationHTML += `
                    <li class="page-item ${nextDisabled}">
                        <a class="page-link" href="#" onclick="changePage('${tabType}', ${currentPage + 1})">
                            <i class="fas fa-chevron-right"></i>
                        </a>
                    </li>
                </ul>
            </nav>
        </div>
    `;
    
    return paginationHTML;
}

function changePage(tabType, page) {
    switch (tabType) {
        case 'synced': currentSyncedPage = page; break;
        case 'local': currentLocalPage = page; break;
        case 'remote': currentRemotePage = page; break;
    }
    renderTab(`${tabType}-items`);
}

function toggleAllInTab(tabType) {
    const table = document.querySelector(`#${tabType}-content table`);
    const headerCheckbox = table.querySelector('thead input[type="checkbox"]');
    const itemCheckboxes = table.querySelectorAll('tbody .item-checkbox');
    
    itemCheckboxes.forEach(cb => {
        cb.checked = headerCheckbox.checked;
    });
    
    updateBulkActionsVisibility();
}

function updateBulkActionsVisibility() {
    const checkedBoxes = document.querySelectorAll('.item-checkbox:checked');
    const bulkActionsBar = document.getElementById('bulkActionsBar');
    const selectedCount = document.getElementById('selectedCount');
    
    if (checkedBoxes.length > 0) {
        selectedCount.textContent = checkedBoxes.length;
        bulkActionsBar.classList.add('show');
    } else {
        bulkActionsBar.classList.remove('show');
    }
}

function getActionButtons(product, tabType) {
    let buttons = '';
    
    if (tabType === 'local' && product.sync_status === 'LOCAL_ONLY') {
        buttons += `
            <button type="button" class="btn btn-sm btn-outline-success" 
                    onclick="syncProduct('${product.id}')" title="Sync to DataHub">
                <i class="fas fa-upload"></i>
            </button>
        `;
    }
    
    if (tabType === 'synced' && product.sync_status === 'MODIFIED') {
        buttons += `
            <button type="button" class="btn btn-sm btn-outline-warning" 
                    onclick="syncProduct('${product.id}')" title="Update in DataHub">
                <i class="fas fa-sync"></i>
            </button>
        `;
    }
    
    return buttons;
}

// Bulk action handlers
function handleBulkSync() {
    const checkedBoxes = document.querySelectorAll('.item-checkbox:checked');
    const productIds = Array.from(checkedBoxes).map(cb => cb.getAttribute('data-product-id'));
    
    if (productIds.length === 0) return;
    
    if (confirm(`Are you sure you want to sync ${productIds.length} data product(s)?`)) {
        // Implement bulk sync logic
        console.log('Bulk syncing products:', productIds);
    }
}

function handleBulkAddToPR() {
    const checkedBoxes = document.querySelectorAll('.item-checkbox:checked');
    const productIds = Array.from(checkedBoxes).map(cb => cb.getAttribute('data-product-id'));
    
    if (productIds.length === 0) return;
    
    // Implement bulk add to PR logic
    console.log('Adding products to PR:', productIds);
}

function handleBulkDelete() {
    const checkedBoxes = document.querySelectorAll('.item-checkbox:checked');
    const productIds = Array.from(checkedBoxes).map(cb => cb.getAttribute('data-product-id'));
    
    if (productIds.length === 0) return;
    
    if (confirm(`Are you sure you want to delete ${productIds.length} data product(s)? This action cannot be undone.`)) {
        // Implement bulk delete logic
        console.log('Bulk deleting products:', productIds);
    }
}

function handleBulkDownload() {
    const checkedBoxes = document.querySelectorAll('.item-checkbox:checked');
    const productIds = Array.from(checkedBoxes).map(cb => cb.getAttribute('data-product-id'));
    
    if (productIds.length === 0) return;
    
    // Implement bulk download logic
    console.log('Bulk downloading products:', productIds);
}

// Individual product actions
function syncProduct(productId) {
    if (confirm('Are you sure you want to sync this data product?')) {
        // Implement sync logic
        console.log('Syncing product:', productId);
    }
}

// Utility functions (keeping existing ones and adding any missing)
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function truncateText(text, maxLength) {
    if (!text) return '';
    if (text.length <= maxLength) return text;
    return text.substring(0, maxLength) + '...';
}

function truncateUrn(urn, maxLength) {
    if (!urn || urn.length <= maxLength) return urn;
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

function getDataHubUrl(urn, type) {
    if (productsData.datahub_url) {
        return `${productsData.datahub_url}/dataProduct/${encodeURIComponent(urn)}`;
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
    document.querySelector('.container-fluid').insertBefore(alert, document.querySelector('.row'));
}

function showProductDetails(product) {
    // Basic information
    const name = product.properties?.name || product.name || 'Unnamed';
    const description = product.properties?.description || product.description || 'No description available';
    const domain = product.domain?.domain?.properties?.name || 'No domain assigned';
    
    document.getElementById('modal-product-name').textContent = name;
    document.getElementById('modal-product-domain').innerHTML = domain !== 'No domain assigned' ? 
        `<span class="badge bg-info">${escapeHtml(domain)}</span>` : domain;
    document.getElementById('modal-product-description').textContent = description;
    document.getElementById('modal-product-urn').textContent = product.urn || 'No URN available';
    
    // Status
    const statusBadge = document.getElementById('modal-product-status');
    statusBadge.textContent = product.sync_status_display || product.sync_status;
    statusBadge.className = `badge ${getStatusBadgeClass(product.sync_status)}`;
    
    // DataHub link
    const datahubLink = document.getElementById('modal-datahub-link');
    if (product.urn && !product.urn.includes('local:') && productsData.datahub_url) {
        datahubLink.href = getDataHubUrl(product.urn, 'product');
        datahubLink.style.display = 'inline-block';
    } else {
        datahubLink.style.display = 'none';
    }
    
    // Show modal
    new bootstrap.Modal(document.getElementById('productViewModal')).show();
} 