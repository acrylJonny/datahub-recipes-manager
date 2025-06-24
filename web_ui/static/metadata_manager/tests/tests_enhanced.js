// Tests Enhanced JavaScript
// Enhanced functionality for tests page

document.addEventListener('DOMContentLoaded', function() {
    // Initialize connection cache
    getCurrentConnectionCache();
    
    // Show loading indicator
    document.getElementById('loading-indicator').style.display = 'block';
    document.getElementById('tests-content').style.display = 'none';
    
    // Load tests data
    loadTestsData();
    
    // Set up event listeners
    setupEventListeners();
});

let testsData = [];
let filteredTests = [];
let currentFilters = {
    overview: null,
    content: []
};

// Sorting variables
let currentSort = {
    column: null,
    direction: 'asc',
    tabType: null
};

// Connection-specific cache management (following tags pattern)
let testsCacheByConnection = {};
let currentConnectionId = null;

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
    
    if (!testsCacheByConnection[currentConnectionId]) {
        testsCacheByConnection[currentConnectionId] = {
            testsData: [],
            lastFetched: null,
            cacheExpiry: 5 * 60 * 1000 // 5 minutes
        };
    }
    
    return testsCacheByConnection[currentConnectionId];
}

// Function to switch to a specific connection cache
function switchConnectionCache(connectionId) {
    console.log(`Switching tests cache from ${currentConnectionId} to ${connectionId}`);
    currentConnectionId = connectionId;
    
    // Reload tests data for the new connection
    loadTestsData();
}

// Function to clear cache for a specific connection
function clearConnectionCache(connectionId) {
    if (testsCacheByConnection[connectionId]) {
        delete testsCacheByConnection[connectionId];
        console.log(`Cleared tests cache for connection: ${connectionId}`);
    }
}

// Make cache globally accessible for connection switching
window.testsCacheByConnection = testsCacheByConnection;
window.switchConnectionCache = switchConnectionCache;
window.clearConnectionCache = clearConnectionCache;

function setupEventListeners() {
    // Refresh button
    document.getElementById('refreshTests').addEventListener('click', function() {
        loadTestsData();
    });
    
    // Filter statistics
    document.querySelectorAll('.filter-stat.clickable-stat').forEach(stat => {
        stat.addEventListener('click', function() {
            const filter = this.dataset.filter;
            const category = this.dataset.category;
            handleFilterClick(filter, category, this);
        });
    });
    
    // Tab switching
    document.querySelectorAll('#testTabs button').forEach(tab => {
        tab.addEventListener('click', function() {
            const targetTab = this.getAttribute('data-bs-target');
            loadTabContent(targetTab.replace('#', '').replace('-items', ''));
        });
    });
    
    // Search functionality
    setupSearchListeners();
    
    // Edit form submission
    setupEditFormHandler();
    
    // Overview clickable stats
    setupOverviewClickHandlers();
    
    // Tab switching handlers
    setupTabSwitchHandlers();
    
    // Action button listeners
    setupActionButtonListeners();
}

function setupSearchListeners() {
    ['synced', 'local', 'remote'].forEach(tabType => {
        const searchInput = document.getElementById(`${tabType}-search`);
        const clearButton = document.getElementById(`${tabType}-clear`);
        
        if (searchInput) {
            searchInput.addEventListener('input', function() {
                filterAndRenderTests(tabType);
            });
        }
        
        if (clearButton) {
            clearButton.addEventListener('click', function() {
                searchInput.value = '';
                filterAndRenderTests(tabType);
            });
        }
    });
}

function setupEditFormHandler() {
    const editForm = document.getElementById('editTestForm');
    if (editForm) {
        editForm.addEventListener('submit', function(e) {
            e.preventDefault();
            
            const formData = new FormData(this);
            const testUrn = formData.get('test_urn');
            
            // Show loading state
            const submitButton = this.querySelector('button[type="submit"]');
            const originalText = submitButton.innerHTML;
            submitButton.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i> Saving...';
            submitButton.disabled = true;
            
            fetch(this.action, {
                method: 'POST',
                body: formData,
                headers: {
                    'X-CSRFToken': getCSRFToken(),
                    'X-Requested-With': 'XMLHttpRequest',
                }
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    showNotification('success', 'Test updated successfully');
                    
                    // Close modal
                    const modal = bootstrap.Modal.getInstance(document.getElementById('editTestModal'));
                    modal.hide();
                    
                    // Refresh tests data
                    loadTestsData();
                } else {
                    showNotification('error', data.error || 'Failed to update test');
                }
            })
            .catch(error => {
                console.error('Error:', error);
                showNotification('error', 'Error updating test');
            })
            .finally(() => {
                // Restore button state
                submitButton.innerHTML = originalText;
                submitButton.disabled = false;
            });
        });
    }
}

function setupOverviewClickHandlers() {
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
}

function setupTabSwitchHandlers() {
    // Tab switching handlers
    document.querySelectorAll('[data-bs-toggle="tab"]').forEach(tab => {
        tab.addEventListener('shown.bs.tab', function (e) {
            const targetId = e.target.getAttribute('href').substring(1);
            const tabType = targetId.replace('-items', '');
            loadTabContent(tabType);
            
            // Update overview active states based on current tab
            updateOverviewActiveState(tabType);
        });
    });
}

function updateOverviewActiveState(tabType) {
    // Clear all overview active states
    document.querySelectorAll('.clickable-stat[data-category="overview"]').forEach(stat => {
        stat.classList.remove('active');
    });
    
    // Set the appropriate overview stat as active based on current tab
    let filterValue = null;
    switch (tabType) {
        case 'synced':
            filterValue = 'synced';
            break;
        case 'local':
            filterValue = 'local-only';
            break;
        case 'remote':
            filterValue = 'remote-only';
            break;
    }
    
    if (filterValue) {
        const activeStat = document.querySelector(`.clickable-stat[data-filter="${filterValue}"]`);
        if (activeStat) {
            activeStat.classList.add('active');
        }
    }
}

function setupActionButtonListeners() {
    // Use event delegation for dynamically added buttons
    document.addEventListener('click', function(e) {
        const target = e.target.closest('button');
        if (!target) return;
        
        // Get the test data from the row
        const row = target.closest('tr');
        if (!row) return;
        
        const itemData = row.getAttribute('data-item');
        let testData = null;
        
        if (itemData) {
            try {
                const parsedData = JSON.parse(itemData.replace(/&quot;/g, '"').replace(/&apos;/g, "'"));
                const cacheKey = parsedData.urn || parsedData.id;
                testData = window.testDataCache[cacheKey] || parsedData;
            } catch (e) {
                console.error('Error parsing test data:', e);
                return;
            }
        }
        
        if (!testData) return;
        
        // Handle different action buttons
        if (target.classList.contains('view-item')) {
            e.preventDefault();
            viewTest(testData.urn);
        } else if (target.classList.contains('edit-test')) {
            e.preventDefault();
            editTest(testData.urn);
        } else if (target.classList.contains('add-to-staged')) {
            e.preventDefault();
            addTestToStagedChanges(testData);
        } else if (target.classList.contains('download-json')) {
            e.preventDefault();
            downloadTestJson(testData);
        } else if (target.classList.contains('sync-to-datahub')) {
            e.preventDefault();
            syncTestToDataHub(testData);
        } else if (target.classList.contains('sync-to-local')) {
            e.preventDefault();
            syncTestToLocal(testData.urn);
        } else if (target.classList.contains('resync-test')) {
            e.preventDefault();
            resyncTest(testData);
        } else if (target.classList.contains('push-to-datahub')) {
            e.preventDefault();
            pushTestToDataHub(testData);
        } else if (target.classList.contains('delete-remote-test')) {
            e.preventDefault();
            deleteRemoteTest(testData.urn);
        }
    });
}

function loadTestsData() {
    console.log('Loading tests data...');
    
    // Hide error states and show loading
    document.getElementById('loading-indicator').style.display = 'block';
    document.getElementById('tests-content').style.display = 'none';
    
    fetch('/metadata/tests/remote-data/', {
        method: 'GET',
        headers: {
            'X-Requested-With': 'XMLHttpRequest',
            'Content-Type': 'application/json',
        },
    })
    .then(response => {
        console.log('Tests data response status:', response.status);
        return response.json();
    })
    .then(data => {
        console.log('Tests data received:', data);
        testsData = data.tests || [];
        filteredTests = [...testsData];
        
        console.log(`Loaded ${testsData.length} tests`);
        
        // Update statistics
        updateStatistics();
        
        // Load initial tab content
        loadTabContent('synced');
        
        // Set initial overview active state
        updateOverviewActiveState('synced');
        
        // Hide loading and show content
        document.getElementById('loading-indicator').style.display = 'none';
        document.getElementById('tests-content').style.display = 'block';
    })
    .catch(error => {
        console.error('Error loading tests data:', error);
        document.getElementById('loading-indicator').style.display = 'none';
        showNotification('error', 'Error loading tests data');
    });
}

function updateStatistics() {
    const synced = testsData.filter(test => test.status === 'synced').length;
    const localOnly = testsData.filter(test => test.status === 'local_only').length;
    const remoteOnly = testsData.filter(test => test.status === 'remote_only').length;
    const withResults = testsData.filter(test => test.has_results).length;
    const failing = testsData.filter(test => test.is_failing).length;
    
    // Update overview statistics
    document.getElementById('total-items').textContent = testsData.length;
    document.getElementById('synced-count').textContent = synced;
    document.getElementById('local-only-count').textContent = localOnly;
    document.getElementById('remote-only-count').textContent = remoteOnly;
    
    // Update content filters
    document.getElementById('tests-with-results').textContent = withResults;
    document.getElementById('failing-tests').textContent = failing;
    
    // Update tab badges
    document.getElementById('synced-badge').textContent = synced;
    document.getElementById('local-badge').textContent = localOnly;
    document.getElementById('remote-badge').textContent = remoteOnly;
}

function handleFilterClick(filter, category, element) {
    if (category === 'overview') {
        // Single select for overview
        document.querySelectorAll('.filter-stat[data-category="overview"]').forEach(stat => {
            stat.classList.remove('active');
        });
        
        if (currentFilters.overview === filter) {
            currentFilters.overview = null;
        } else {
            currentFilters.overview = filter;
            element.classList.add('active');
        }
    } else if (category === 'content') {
        // Multi-select for content filters
        const index = currentFilters.content.indexOf(filter);
        if (index > -1) {
            currentFilters.content.splice(index, 1);
            element.classList.remove('active');
        } else {
            currentFilters.content.push(filter);
            element.classList.add('active');
        }
    }
    
    // Apply filters and refresh current tab
    applyFilters();
    const activeTab = document.querySelector('#testTabs .nav-link.active');
    if (activeTab) {
        const tabType = activeTab.getAttribute('data-bs-target').replace('#', '').replace('-items', '');
        loadTabContent(tabType);
    }
}

function applyFilters() {
    filteredTests = testsData.filter(test => {
        // Apply overview filter
        if (currentFilters.overview) {
            if (currentFilters.overview === 'synced' && test.status !== 'synced') return false;
            if (currentFilters.overview === 'local-only' && test.status !== 'local_only') return false;
            if (currentFilters.overview === 'remote-only' && test.status !== 'remote_only') return false;
        }
        
        // Apply content filters
        for (const contentFilter of currentFilters.content) {
            if (contentFilter === 'with-results' && !test.has_results) return false;
            if (contentFilter === 'failing' && !test.is_failing) return false;
        }
        
        return true;
    });
}

function loadTabContent(tabType) {
    const contentDiv = document.getElementById(`${tabType}-content`);
    if (!contentDiv) return;
    
    let tabTests = filteredTests.filter(test => {
        switch(tabType) {
            case 'synced': return test.status === 'synced';
            case 'local': return test.status === 'local_only';
            case 'remote': return test.status === 'remote_only';
            default: return true;
        }
    });
    
    // Apply search filter
    const searchInput = document.getElementById(`${tabType}-search`);
    if (searchInput && searchInput.value) {
        const searchTerm = searchInput.value.toLowerCase();
        tabTests = tabTests.filter(test => 
            test.name.toLowerCase().includes(searchTerm) ||
            (test.description && test.description.toLowerCase().includes(searchTerm))
        );
    }
    
    renderTestsTable(tabTests, tabType, contentDiv);
}

function renderTestsTable(tests, tabType, container) {
    if (tests.length === 0) {
        container.innerHTML = `
            <div class="text-center py-5">
                <i class="fas fa-flask fa-3x text-muted mb-3"></i>
                <h5 class="text-muted">No tests found</h5>
                <p class="text-muted">No tests match the current filters.</p>
            </div>
        `;
        return;
    }
    
    // Apply sorting
    if (currentSort.column && currentSort.tabType === tabType) {
        tests = sortTests(tests, currentSort.column, currentSort.direction);
    }
    
    const tableHTML = `
        <div class="table-responsive">
            <table class="table table-hover mb-0">
                <thead class="table-light">
                    <tr>
                        <th width="50">
                            <input type="checkbox" class="form-check-input" id="${tabType}-select-all">
                        </th>
                        <th class="sortable" data-column="name" data-tab="${tabType}" style="cursor: pointer;">
                            Test Name ${getSortIcon('name', tabType)}
                        </th>
                        <th>Description</th>
                        <th class="sortable" data-column="category" data-tab="${tabType}" style="cursor: pointer;">
                            Category ${getSortIcon('category', tabType)}
                        </th>
                        <th class="sortable" data-column="results" data-tab="${tabType}" style="cursor: pointer;">
                            Results ${getSortIcon('results', tabType)}
                        </th>
                        <th class="sortable" data-column="last_run" data-tab="${tabType}" style="cursor: pointer;">
                            Last Run ${getSortIcon('last_run', tabType)}
                        </th>
                        <th>URN</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    ${tests.map(test => renderTestRow(test, tabType)).join('')}
                </tbody>
            </table>
        </div>
    `;
    
    container.innerHTML = tableHTML;
    
    // Set up bulk selection
    setupBulkSelection(tabType);
    
    // Attach sorting handlers
    attachSortingHandlers(container, tabType);
}

function renderTestRow(test, tabType) {
    const actions = getActionButtons(test, tabType);
    
    // Format test results
    let resultsDisplay = '<span class="text-muted">No results</span>';
    if (test.has_results) {
        const passCount = test.passing_count || 0;
        const failCount = test.failing_count || 0;
        resultsDisplay = `<span class="badge bg-success">${passCount} Pass</span> <span class="badge bg-danger">${failCount} Fail</span>`;
    }
    
    // Format last run time
    let lastRunDisplay = '<span class="text-muted">Never</span>';
    if (test.last_run) {
        const lastRunDate = new Date(test.last_run);
        lastRunDisplay = `<small class="text-muted">${lastRunDate.toLocaleDateString()}</small>`;
    }
    
    return `
        <tr data-test-id="${test.id}" data-test-urn="${test.urn || ''}">
            <td>
                <input type="checkbox" class="form-check-input test-checkbox" 
                       data-test-id="${test.id}" data-tab="${tabType}">
            </td>
            <td>
                <div class="d-flex align-items-center">
                    <i class="fas fa-flask me-2 text-primary"></i>
                    <div class="fw-bold">${test.name}</div>
                </div>
            </td>
            <td>
                <div class="text-muted">${test.description || 'No description'}</div>
            </td>
            <td>
                <span class="badge bg-secondary">${test.category || 'UNCATEGORIZED'}</span>
            </td>
            <td>${resultsDisplay}</td>
            <td>${lastRunDisplay}</td>
            <td>
                <code class="small text-muted" style="font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;">${test.urn || 'N/A'}</code>
            </td>
            <td>${actions}</td>
        </tr>
    `;
}

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

function attachSortingHandlers(container, tabType) {
    const sortableHeaders = container.querySelectorAll('.sortable');
    
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
            loadTabContent(tabType);
        });
    });
}

function sortTests(tests, column, direction) {
    return tests.sort((a, b) => {
        let aValue = getTestSortValue(a, column);
        let bValue = getTestSortValue(b, column);
        
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

function getTestSortValue(test, column) {
    switch (column) {
        case 'name':
            return test.name || '';
        case 'category':
            return test.category || '';
        case 'results':
            // Sort by total results count (pass + fail)
            const passCount = test.passing_count || 0;
            const failCount = test.failing_count || 0;
            return passCount + failCount;
        case 'last_run':
            return test.last_run ? new Date(test.last_run).getTime() : 0;
        default:
            return '';
    }
}

function getActionButtons(test, tabType) {
    // Get test data
    const testData = test.combined || test;
    const urn = testData.urn || '';
    
    // Get connection context information
    const connectionContext = testData.connection_context || 'none'; // "current", "different", "none"
    const hasRemoteMatch = testData.has_remote_match || false;
    
    // Get the proper database ID for this test
    const databaseId = getDatabaseId(testData);
    
    let actionButtons = '';
    
    // 1. Edit Test - For local and synced tests
    if (tabType === 'local' || tabType === 'synced') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-warning edit-test" 
                    title="Edit Test">
                <i class="fas fa-edit"></i>
            </button>
        `;
    }

    // 2. Sync to DataHub - For tests in local tab
    // Show for ALL tests in local tab regardless of their connection or sync status
    if (tabType === 'local') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-success sync-to-datahub" 
                    title="Sync to DataHub">
                <i class="fas fa-upload"></i>
            </button>
        `;
    }
    
    // 2b. Resync - Only for synced tests (tests that belong to current connection and have remote match)
    if (tabType === 'synced' && connectionContext === 'current' && hasRemoteMatch) {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-info resync-test" 
                    title="Resync from DataHub">
                <i class="fas fa-sync-alt"></i>
            </button>
        `;
    }
    
    // 2c. Push to DataHub - Only for synced tests that are modified
    if (tabType === 'synced' && connectionContext === 'current' && testData.sync_status === 'MODIFIED') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-success push-to-datahub" 
                    title="Push to DataHub">
                <i class="fas fa-upload"></i>
            </button>
        `;
    }
    
    // 3. Sync to Local - Only for remote-only tests
    if (tabType === 'remote') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-primary sync-to-local" 
                    title="Sync to Local">
                <i class="fas fa-download"></i>
            </button>
        `;
    }
    
    // 4. Download JSON - Available for all tests
    actionButtons += `
        <button type="button" class="btn btn-sm btn-outline-secondary download-json"
                title="Download JSON">
            <i class="fas fa-file-download"></i>
        </button>
    `;

    // 5. Add to Staged Changes - Available for all tests
    actionButtons += `
        <button type="button" class="btn btn-sm btn-outline-warning add-to-staged"
                title="Add to Staged Changes">
            <i class="fab fa-github"></i>
        </button>
    `;

    // 6. Delete Local Test - Only for synced and local tests
    if (tabType === 'synced' || tabType === 'local') {
        // Use a simple form with a POST action for direct deletion
        // This avoids all the complex JavaScript ID handling
        actionButtons += `
            <form method="POST" action="/metadata/tests/${testData.id}/delete/" 
                  style="display:inline;" 
                  onsubmit="return confirm('Are you sure you want to delete this test? This action cannot be undone.');">
                <input type="hidden" name="csrfmiddlewaretoken" value="${getCsrfToken()}">
                <button type="submit" class="btn btn-sm btn-outline-danger"
                        title="Delete Local Test">
                    <i class="fas fa-trash"></i>
                </button>
            </form>
        `;
    }
    
    // 7. Delete Remote Test - Only for remote-only tests
    if (tabType === 'remote') {
        actionButtons += `
            <button type="button" class="btn btn-sm btn-outline-danger delete-remote-test" 
                    title="Delete from DataHub">
                <i class="fas fa-trash"></i>
            </button>
        `;
    }
    
    return actionButtons;
}

function setupBulkSelection(tabType) {
    const selectAllCheckbox = document.getElementById(`${tabType}-select-all`);
    const checkboxes = document.querySelectorAll(`.test-checkbox[data-tab="${tabType}"]`);
    const bulkActions = document.getElementById(`${tabType}-bulk-actions`);
    const selectedCount = document.getElementById(`${tabType}-selected-count`);
    
    if (selectAllCheckbox) {
        selectAllCheckbox.addEventListener('change', function() {
            checkboxes.forEach(checkbox => {
                checkbox.checked = this.checked;
            });
            updateBulkActionVisibility(tabType);
        });
    }
    
    checkboxes.forEach(checkbox => {
        checkbox.addEventListener('change', function() {
            updateBulkActionVisibility(tabType);
        });
    });
}

function updateBulkActionVisibility(tabType) {
    const checkboxes = document.querySelectorAll(`.test-checkbox[data-tab="${tabType}"]:checked`);
    const bulkActions = document.getElementById(`${tabType}-bulk-actions`);
    const selectedCount = document.getElementById(`${tabType}-selected-count`);
    
    if (selectedCount) {
        selectedCount.textContent = checkboxes.length;
    }
    
    if (bulkActions) {
        if (checkboxes.length > 0) {
            bulkActions.classList.add('show');
        } else {
            bulkActions.classList.remove('show');
        }
    }
}

// Individual Action Functions
function viewTest(testUrn) {
    // Find the test data
    const test = testsData.find(t => t.urn === testUrn);
    if (!test) {
        showNotification('error', 'Test not found');
        return;
    }
    
    // Populate modal with test data
    populateTestViewModal(test);
    
    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('testViewModal'));
    modal.show();
}

function editTest(testUrn) {
    // Find the test data
    const test = testsData.find(t => t.urn === testUrn);
    if (!test) {
        showNotification('error', 'Test not found');
        return;
    }
    
    // Populate edit modal with test data
    populateEditTestModal(test);
    
    // Show modal
    const modal = new bootstrap.Modal(document.getElementById('editTestModal'));
    modal.show();
}

function populateEditTestModal(test) {
    // Set form action URL
    const form = document.getElementById('editTestForm');
    form.action = `/metadata/tests/${test.urn}/`;
    
    // Populate form fields
    document.getElementById('editTestUrn').value = test.urn;
    document.getElementById('editTestName').value = test.name || '';
    document.getElementById('editTestDescription').value = test.description || '';
    document.getElementById('editTestCategory').value = test.category || '';
    
    // Handle definition JSON - format for editing
    let definitionText = '';
    if (test.definition_json) {
        try {
            if (typeof test.definition_json === 'string') {
                // Try to parse and reformat for better editing
                try {
                    const parsed = JSON.parse(test.definition_json);
                    definitionText = JSON.stringify(parsed, null, 2);
                } catch (e) {
                    // If parsing fails, use the string as-is
                    definitionText = test.definition_json;
                }
            } else {
                // If it's an object, stringify it
                definitionText = JSON.stringify(test.definition_json, null, 2);
            }
        } catch (e) {
            definitionText = test.definition_json || '';
        }
    }
    
    const editDefinitionElement = document.getElementById('editTestDefinition');
    editDefinitionElement.value = definitionText;
    
    // Add JSON validation on input
    editDefinitionElement.addEventListener('input', function() {
        try {
            if (this.value.trim()) {
                JSON.parse(this.value);
                this.classList.remove('is-invalid');
                this.classList.add('is-valid');
            } else {
                this.classList.remove('is-invalid', 'is-valid');
            }
        } catch (e) {
            this.classList.remove('is-valid');
            this.classList.add('is-invalid');
        }
    });
}

function addTestToPR(testId) {
    showNotification('info', 'Add to PR functionality will be implemented');
}

function addRemoteTestToPR(testUrn) {
    showNotification('info', 'Add remote test to PR functionality will be implemented');
}

function addTestToStagedChanges(testData) {
    console.log('Adding test to staged changes:', testData);
    
    // Get the proper database ID for this test
    const databaseId = getDatabaseId(testData);
    
    if (!databaseId) {
        // This is a remote-only test, use the remote endpoint
        addRemoteTestToStagedChanges(testData);
        return;
    }
    
    console.log('Using database ID for staged changes:', databaseId);
    
    // Get current environment and mutation from global state or settings
    const currentEnvironment = window.currentEnvironment || { name: 'dev' };
    const mutationName = currentEnvironment.mutation_name || null;
    
    console.log('Making API call to:', `/metadata/tests/${databaseId}/stage_changes/`);
    
    // Make API call to create test files
    fetch(`/metadata/tests/${databaseId}/stage_changes/`, {
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
            throw new Error('Failed to add test to staged changes');
        }
        return response.json();
    })
    .then(data => {
        // Show success notification
        showNotification('success', `Test successfully added to staged changes: ${data.files_created.join(', ')}`);
    })
    .catch(error => {
        console.error('Error adding test to staged changes:', error);
        showNotification('error', `Error adding test to staged changes: ${error.message}`);
    });
}

function addRemoteTestToStagedChanges(testData) {
    console.log('Adding remote test to staged changes:', testData);
    
    // Get current environment and mutation from global state or settings
    const currentEnvironment = window.currentEnvironment || { name: 'dev' };
    const mutationName = currentEnvironment.mutation_name || null;
    
    // Make API call to create test files for remote test
    fetch('/metadata/tests/remote/stage_changes/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCsrfToken()
        },
        body: JSON.stringify({
            test_urn: testData.urn,
            environment: currentEnvironment.name,
            mutation_name: mutationName
        })
    })
    .then(response => {
        if (!response.ok) {
            throw new Error('Failed to add remote test to staged changes');
        }
        return response.json();
    })
    .then(data => {
        // Show success notification
        showNotification('success', `Remote test successfully added to staged changes: ${data.files_created.join(', ')}`);
    })
    .catch(error => {
        console.error('Error adding remote test to staged changes:', error);
        showNotification('error', `Error adding remote test to staged changes: ${error.message}`);
    });
}

function downloadTestJson(testData) {
    console.log('Downloading test JSON:', testData);
    
    // Create a JSON object with the test data
    const testDataForDownload = {
        test: testData,
        metadata: {
            exported_at: new Date().toISOString(),
            source: window.location.origin
        }
    };
    
    // Convert to pretty JSON
    const jsonData = JSON.stringify(testDataForDownload, null, 2);
    
    // Create a blob and initiate download
    const blob = new Blob([jsonData], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    
    // Create temporary link and trigger download
    const link = document.createElement('a');
    link.href = url;
    link.download = `test-${testData.name || 'unknown'}-${new Date().toISOString().slice(0, 10)}.json`;
    document.body.appendChild(link);
    link.click();
    
    // Clean up
    setTimeout(() => {
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }, 100);
    
    showNotification('success', 'Test exported successfully.');
}

function syncTestToDataHub(testData) {
    console.log('syncTestToDataHub called with:', testData);
    
    const testId = getDatabaseId(testData);
    
    if (!testId) {
        console.error('Cannot sync test without a database ID:', testData);
        showNotification('error', 'Error syncing test: Missing test database ID.');
        return;
    }
    
    // Show loading notification
    showNotification('success', `Syncing test "${testData.name}" to DataHub...`);
    
    // Make the API call to sync this test to DataHub
    fetch(`/metadata/tests/${testId}/sync_to_datahub/`, {
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
        console.log('Sync to DataHub response:', data);
        if (data.success) {
            showNotification('success', data.message);
            // Refresh the page to show updated sync status
            setTimeout(() => {
                window.location.reload();
            }, 1500);
        } else {
            throw new Error(data.error || 'Unknown error occurred');
        }
    })
    .catch(error => {
        console.error('Error syncing test to DataHub:', error);
        showNotification('error', `Error syncing test: ${error.message}`);
    });
}

function resyncTest(testData) {
    console.log('resyncTest called with:', testData);
    
    const testId = getDatabaseId(testData);
    
    if (!testId) {
        console.error('Cannot resync test without a database ID:', testData);
        showNotification('error', 'Error resyncing test: Missing test database ID.');
        return;
    }
    
    // Show loading notification
    showNotification('success', `Resyncing test "${testData.name}" from DataHub...`);
    
    // Make the API call to resync this test from DataHub
    fetch(`/metadata/tests/${testId}/resync/`, {
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
        console.log('Resync response:', data);
        if (data.success) {
            showNotification('success', data.message);
            // Refresh the page to show updated data
            setTimeout(() => {
                window.location.reload();
            }, 1500);
        } else {
            throw new Error(data.error || 'Unknown error occurred');
        }
    })
    .catch(error => {
        console.error('Error resyncing test:', error);
        showNotification('error', `Error resyncing test: ${error.message}`);
    });
}

function pushTestToDataHub(testData) {
    console.log('pushTestToDataHub called with:', testData);
    
    const testId = getDatabaseId(testData);
    
    if (!testId) {
        console.error('Cannot push test without a database ID:', testData);
        showNotification('error', 'Error pushing test: Missing test database ID.');
        return;
    }
    
    // Show loading notification
    showNotification('success', `Pushing test "${testData.name}" to DataHub...`);
    
    // Make the API call to push this test to DataHub
    fetch(`/metadata/tests/${testId}/push_to_datahub/`, {
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
        console.log('Push to DataHub response:', data);
        if (data.success) {
            showNotification('success', data.message);
            // Refresh the page to show updated sync status
            setTimeout(() => {
                window.location.reload();
            }, 1500);
        } else {
            throw new Error(data.error || 'Unknown error occurred');
        }
    })
    .catch(error => {
        console.error('Error pushing test to DataHub:', error);
        showNotification('error', `Error pushing test: ${error.message}`);
    });
}

function syncTestToLocal(testUrn) {
    showNotification('info', 'Sync to local functionality will be implemented');
}

function pushTestToDataHub(testId) {
    showNotification('info', 'Push to DataHub functionality will be implemented');
}

function deleteLocalTest(testId) {
    // Find the test to determine its status
    const test = testsData.find(t => t.id === testId || t.urn === testId);
    const testStatus = test ? test.status : 'unknown';
    
    let confirmMessage = 'Are you sure you want to delete this test?';
    let deleteType = 'both'; // both local and remote
    
    if (testStatus === 'synced') {
        confirmMessage = 'Are you sure you want to delete this test locally? (It will remain on DataHub)';
        deleteType = 'local_only';
    } else if (testStatus === 'local') {
        confirmMessage = 'Are you sure you want to delete this local test?';
        deleteType = 'local_only';
    } else if (testStatus === 'remote') {
        confirmMessage = 'Are you sure you want to delete this test from DataHub?';
        deleteType = 'remote_only';
    }
    
    if (confirm(confirmMessage)) {
        // Call backend to delete test
        fetch(`/metadata/tests/${testId}/delete/`, {
            method: 'POST',
            headers: {
                'X-CSRFToken': getCSRFToken(),
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                delete_type: deleteType
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                showNotification(data.message || 'Test deleted successfully', 'success');
                loadTestsData(); // Refresh data
            } else {
                showNotification(data.error || 'Failed to delete test', 'error');
            }
        })
        .catch(error => {
            showNotification('Error deleting test', 'error');
            console.error('Error:', error);
        });
    }
}

function deleteRemoteTest(testUrn) {
    if (confirm('Are you sure you want to delete this remote test?')) {
        showNotification('Delete remote functionality will be implemented', 'info');
    }
}

// Bulk Action Functions
function bulkPushTests(tabType) {
    const selectedTests = getSelectedTests(tabType);
    if (selectedTests.length === 0) {
        showNotification('No tests selected', 'warning');
        return;
    }
    
    showNotification(`Bulk push functionality will be implemented for ${selectedTests.length} tests`, 'info');
}

function bulkAddToPR(tabType) {
    const selectedTests = getSelectedTests(tabType);
    if (selectedTests.length === 0) {
        showNotification('error', 'Please select tests to add to staged changes.');
        return;
    }
    
    // Validate all selected tests have valid IDs and exist in current data
    const validatedTests = [];
    const invalidTests = [];
    
    selectedTests.forEach(test => {
        const testId = getDatabaseId(test);
        if (!testId) {
            invalidTests.push(test);
            return;
        }
        
        // Check if the test exists in current cached data
        const cacheKey = test.urn || test.id;
        const cachedTest = window.testDataCache[cacheKey];
        if (!cachedTest) {
            console.warn('Test not found in current cache, may be stale:', test);
            invalidTests.push(test);
            return;
        }
        
        validatedTests.push(test);
    });
    
    if (invalidTests.length > 0) {
        console.error('Found invalid or stale tests:', invalidTests);
        showNotification('error', `${invalidTests.length} selected tests are invalid or from stale data. Please refresh the page and try again.`);
        
        // Clear selections to prevent further issues
        clearAllSelections();
        return;
    }
    
    if (confirm(`Are you sure you want to add ${validatedTests.length} test(s) to staged changes?`)) {
        console.log(`Bulk add ${validatedTests.length} validated tests to staged changes for ${tabType}:`, validatedTests);
        
        // Show loading indicator
        showNotification('success', `Starting to add ${validatedTests.length} tests to staged changes...`);
        
        // Get current environment and mutation from global state or settings
        const currentEnvironment = window.currentEnvironment || { name: 'dev' };
        const mutationName = currentEnvironment.mutation_name || null;
        
        // Process each test sequentially
        let successCount = 0;
        let errorCount = 0;
        let processedCount = 0;
        let createdFiles = [];
        
        // Create a function to process tests one by one
        function processNextTest(index) {
            if (index >= validatedTests.length) {
                // All tests processed
                if (successCount > 0) {
                    showNotification('success', `Completed: ${successCount} tests added to staged changes, ${errorCount} failed.`);
                    if (createdFiles.length > 0) {
                        console.log('Created files:', createdFiles);
                    }
                } else if (errorCount > 0) {
                    showNotification('error', `Failed to add any tests to staged changes. ${errorCount} errors occurred.`);
                }
                return;
            }
            
            const test = validatedTests[index];
            
            // We need the ID for the API call - use the validated database ID
            const testId = getDatabaseId(test);
            if (!testId) {
                console.error('Cannot add test to staged changes without an ID:', test);
                errorCount++;
                processedCount++;
                processNextTest(index + 1);
                return;
            }
            
            // Make the API call to add this test to staged changes
            fetch(`/metadata/tests/${testId}/stage_changes/`, {
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
                    throw new Error(`Failed to add test ${test.name || test.urn} to staged changes`);
                }
                return response.json();
            })
            .then(data => {
                console.log(`Successfully added test to staged changes: ${test.name || test.urn}`);
                successCount++;
                processedCount++;
                
                // Track created files
                if (data.files_created && data.files_created.length > 0) {
                    createdFiles = [...createdFiles, ...data.files_created];
                }
                
                // Update progress
                if (processedCount % 5 === 0 || processedCount === validatedTests.length) {
                    showNotification('success', `Progress: ${processedCount}/${validatedTests.length} tests processed`);
                }
                
                // Process the next test
                processNextTest(index + 1);
            })
            .catch(error => {
                console.error(`Error adding test ${test.name || test.urn} to staged changes:`, error);
                errorCount++;
                processedCount++;
                
                // Process the next test despite the error
                processNextTest(index + 1);
            });
        }
        
        // Start processing tests
        processNextTest(0);
    }
}

function bulkDeleteLocal(tabType) {
    const selectedTests = getSelectedTests(tabType);
    if (selectedTests.length === 0) {
        showNotification('warning', 'No tests selected');
        return;
    }
    
    // Determine delete type and message based on tab
    let confirmMessage = `Are you sure you want to delete ${selectedTests.length} test(s)?`;
    let deleteType = 'both';
    
    if (tabType === 'synced') {
        confirmMessage = `Are you sure you want to delete ${selectedTests.length} test(s) locally? (They will remain on DataHub)`;
        deleteType = 'local_only';
    } else if (tabType === 'local') {
        confirmMessage = `Are you sure you want to delete ${selectedTests.length} local test(s)?`;
        deleteType = 'local_only';
    } else if (tabType === 'remote') {
        confirmMessage = `Are you sure you want to delete ${selectedTests.length} test(s) from DataHub?`;
        deleteType = 'remote_only';
    }
    
    if (confirm(confirmMessage)) {
        // Create array of delete promises
        const deletePromises = selectedTests.map(testId => 
            fetch(`/metadata/tests/${testId}/delete/`, {
                method: 'POST',
                headers: {
                    'X-CSRFToken': getCSRFToken(),
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    delete_type: deleteType
                })
            }).then(response => response.json())
        );
        
        Promise.all(deletePromises)
        .then(results => {
            const successful = results.filter(r => r.success).length;
            const failed = results.length - successful;
            
            if (failed === 0) {
                showNotification('success', `Successfully deleted ${successful} test(s)`);
            } else {
                showNotification('warning', `Deleted ${successful} test(s), ${failed} failed`);
            }
            
            loadTestsData(); // Refresh data
        })
        .catch(error => {
            showNotification('error', 'Error during bulk delete');
            console.error('Error:', error);
        });
    }
}

function bulkSyncToLocal(tabType) {
    const selectedTests = getSelectedTests(tabType);
    if (selectedTests.length === 0) {
        showNotification('warning', 'No tests selected');
        return;
    }
    
    showNotification('info', `Bulk sync to local functionality will be implemented for ${selectedTests.length} tests`);
}

function bulkDeleteRemote(tabType) {
    const selectedTests = getSelectedTests(tabType);
    if (selectedTests.length === 0) {
        showNotification('warning', 'No tests selected');
        return;
    }
    
    showNotification('info', `Bulk delete remote functionality will be implemented for ${selectedTests.length} tests`);
}

function getSelectedTests(tabType) {
    const checkboxes = document.querySelectorAll(`#${tabType}-items .item-checkbox:checked`);
    return Array.from(checkboxes).map(checkbox => {
        const row = checkbox.closest('tr');
        const itemData = row.getAttribute('data-item');
        if (itemData) {
            try {
                const parsedData = JSON.parse(itemData.replace(/&quot;/g, '"').replace(/&apos;/g, "'"));
                const cacheKey = parsedData.urn || parsedData.id;
                return window.testDataCache[cacheKey] || parsedData;
            } catch (e) {
                console.error('Error parsing item data:', e);
                return null;
            }
        }
        return null;
    }).filter(item => item !== null);
}

function clearAllSelections() {
    const checkboxes = document.querySelectorAll('.item-checkbox');
    checkboxes.forEach(checkbox => {
        checkbox.checked = false;
    });
    
    // Update bulk action visibility for all tabs
    ['synced', 'local', 'remote'].forEach(tabType => {
        updateBulkActionVisibility(tabType);
    });
}

function getCsrfToken() {
    return document.querySelector('[name=csrfmiddlewaretoken]')?.value || 
           document.cookie.split('; ')
               .find(row => row.startsWith('csrftoken='))
               ?.split('=')[1];
}

function getCSRFToken() {
    const token = document.querySelector('[name=csrfmiddlewaretoken]');
    return token ? token.value : document.querySelector('meta[name="csrf-token"]')?.getAttribute('content') || '';
}

function populateTestViewModal(test) {
    document.getElementById('modal-test-name').textContent = test.name || 'Unnamed Test';
    document.getElementById('modal-test-urn').textContent = test.urn || 'N/A';
    document.getElementById('modal-test-category').textContent = test.category || 'UNCATEGORIZED';
    document.getElementById('modal-test-description').textContent = test.description || 'No description available';
    
    // Format results
    if (test.has_results) {
        document.getElementById('modal-test-passing').textContent = test.passing_count || 0;
        document.getElementById('modal-test-failing').textContent = test.failing_count || 0;
        
        if (test.last_run) {
            const lastRunDate = new Date(test.last_run);
            document.getElementById('modal-test-last-run').textContent = lastRunDate.toLocaleString();
        } else {
            document.getElementById('modal-test-last-run').textContent = 'Never';
        }
    } else {
        document.getElementById('modal-test-passing').textContent = 'N/A';
        document.getElementById('modal-test-failing').textContent = 'N/A';
        document.getElementById('modal-test-last-run').textContent = 'No results';
    }
    
    // Show definition if available with syntax highlighting
    const definitionElement = document.getElementById('modal-test-definition');
    if (test.definition_json && test.definition_json.trim()) {
        try {
            let definitionData;
            if (typeof test.definition_json === 'string') {
                definitionData = JSON.parse(test.definition_json);
            } else {
                definitionData = test.definition_json;
            }
            
            // Create a formatted JSON with syntax highlighting
            const formatted = JSON.stringify(definitionData, null, 2);
            
            // Clear the element and create a code block
            definitionElement.innerHTML = '';
            const codeBlock = document.createElement('pre');
            codeBlock.className = 'language-json';
            const codeElement = document.createElement('code');
            codeElement.className = 'language-json';
            codeElement.textContent = formatted;
            codeBlock.appendChild(codeElement);
            definitionElement.appendChild(codeBlock);
            
            // Apply syntax highlighting if Prism.js is available
            if (typeof Prism !== 'undefined') {
                Prism.highlightElement(codeElement);
            }
        } catch (e) {
            // If JSON parsing fails, show as plain text
            definitionElement.innerHTML = '';
            const preElement = document.createElement('pre');
            preElement.textContent = test.definition_json;
            definitionElement.appendChild(preElement);
        }
    } else {
        definitionElement.innerHTML = '<span class="text-muted">No definition available</span>';
    }
}

// Handle create test form submission
document.addEventListener('DOMContentLoaded', function() {
    const createTestForm = document.getElementById('createTestForm');
    if (createTestForm) {
        createTestForm.addEventListener('submit', function(e) {
            e.preventDefault();
            
            const formData = new FormData(this);
            
            fetch(this.action, {
                method: 'POST',
                body: formData,
                headers: {
                    'X-CSRFToken': getCSRFToken(),
                },
            })
            .then(response => {
                if (response.redirected) {
                    // If redirected, it means success - reload the page
                    window.location.reload();
                } else {
                    return response.text();
                }
            })
            .then(html => {
                if (html) {
                    // If we get HTML back, there were form errors
                    // For now, just show a generic error
                    showNotification('Please check your form data and try again', 'error');
                }
            })
            .catch(error => {
                console.error('Error:', error);
                showNotification('Error creating test', 'error');
            });
        });
    }
});

// Utility Functions
function filterAndRenderTests(tabType) {
    loadTabContent(tabType);
}

function getCSRFToken() {
    return document.querySelector('[name=csrfmiddlewaretoken]').value;
}

// Helper functions (consistent with tags page)

function getStatusBadgeClass(status) {
    switch (status) {
        case 'SYNCED':
            return 'bg-success';
        case 'MODIFIED':
            return 'bg-warning';
        case 'LOCAL_ONLY':
            return 'bg-info';
        case 'REMOTE_ONLY':
            return 'bg-secondary';
        case 'NOT_SYNCED':
            return 'bg-light text-dark';
        default:
            return 'bg-secondary';
    }
}

function shouldShowDataHubViewButton(testData, tabType) {
    const urn = testData.urn || '';
    const connectionContext = testData.connection_context || 'none';
    const hasRemoteMatch = testData.has_remote_match || false;
    
    // Don't show for local URNs or empty URNs
    if (!urn || urn.includes('local:')) {
        return false;
    }
    
    // Show for remote-only tests (they definitely exist in DataHub)
    if (tabType === 'remote') {
        return true;
    }
    
    // For synced tab: show only if test belongs to current connection AND has remote match
    if (tabType === 'synced') {
        return connectionContext === 'current' && hasRemoteMatch;
    }
    
    // For local tab: don't show View in DataHub button
    // These tests either don't exist in DataHub or belong to different connections
    if (tabType === 'local') {
        return false;
    }
    
    return false;
}

function getDataHubUrl(urn, type) {
    if (!testsData.datahub_url || !urn) return '#';
    
    // Ensure no double slashes and don't encode URN colons
    const baseUrl = testsData.datahub_url.replace(/\/+$/, ''); // Remove trailing slashes
    return `${baseUrl}/test/${urn}`;
}

function getDatabaseId(testData) {
    // Check if test has a valid database ID and is not remote-only
    if (testData.sync_status === 'REMOTE_ONLY' || testData.status === 'remote_only') {
        return null; // Remote-only tests don't have valid database IDs
    }
    
    // Return the database ID if available and test is not remote-only
    return testData.id || testData.datahub_id || null;
}

function escapeHtml(text) {
    // Use global utility for consistent HTML escaping
    if (typeof DataUtils !== 'undefined' && DataUtils.safeEscapeHtml) {
        return DataUtils.safeEscapeHtml(text);
    }
    
    // Fallback implementation
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function sanitizeDataForAttribute(item, maxDescriptionLength = 200) {
    // First ensure we have a valid name property
    const testData = item.combined || item;
    
    // Make a copy of the item to avoid modifying the original
    const sanitizedItem = {...testData};
    
    // Ensure name is properly set
    if (!sanitizedItem.name) {
        if (sanitizedItem.properties?.name) {
            sanitizedItem.name = sanitizedItem.properties.name;
        } else if (sanitizedItem.urn) {
            // Extract name from URN as fallback
            const urnParts = sanitizedItem.urn.split(':');
            if (urnParts.length > 0) {
                sanitizedItem.name = urnParts[urnParts.length - 1];
            }
        }
        
        if (!sanitizedItem.name) {
            sanitizedItem.name = 'Unnamed Test';
        }
    }
    
    // Use global utility for consistent data sanitization if available
    if (typeof DataUtils !== 'undefined' && DataUtils.createDisplaySafeItem) {
        return DataUtils.createDisplaySafeItem(sanitizedItem, {
            descriptionLength: maxDescriptionLength,
            nameLength: 100,
            urnLength: 500
        });
    }
    
    // Fallback implementation
    return sanitizedItem;
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