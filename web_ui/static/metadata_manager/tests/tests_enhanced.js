// Tests Enhanced JavaScript
// Enhanced functionality for tests page

document.addEventListener('DOMContentLoaded', function() {
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
                    showNotification('Test updated successfully', 'success');
                    
                    // Close modal
                    const modal = bootstrap.Modal.getInstance(document.getElementById('editTestModal'));
                    modal.hide();
                    
                    // Refresh tests data
                    loadTestsData();
                } else {
                    showNotification(data.error || 'Failed to update test', 'error');
                }
            })
            .catch(error => {
                console.error('Error:', error);
                showNotification('Error updating test', 'error');
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

function loadTestsData() {
    // Hide error states and show loading
    document.getElementById('loading-indicator').style.display = 'block';
    document.getElementById('tests-content').style.display = 'none';
    
    fetch('/metadata/tests/data/', {
        method: 'GET',
        headers: {
            'X-Requested-With': 'XMLHttpRequest',
            'Content-Type': 'application/json',
        },
    })
    .then(response => response.json())
    .then(data => {
        testsData = data.tests || [];
        filteredTests = [...testsData];
        
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
        showNotification('Error loading tests data', 'error');
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
    
    const tableHTML = `
        <div class="table-responsive">
            <table class="table table-hover mb-0">
                <thead class="table-light">
                    <tr>
                        <th width="50">
                            <input type="checkbox" class="form-check-input" id="${tabType}-select-all">
                        </th>
                        <th>Test Name</th>
                        <th>Category</th>
                        <th>URN</th>
                        <th>Results</th>
                        <th>Last Run</th>
                        <th>Status</th>
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
}

function renderTestRow(test, tabType) {
    const statusBadge = getStatusBadge(test.status);
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
                    <div>
                        <div class="fw-bold">${test.name}</div>
                        ${test.description ? `<small class="text-muted">${test.description}</small>` : ''}
                    </div>
                </div>
            </td>
            <td>
                <span class="badge bg-secondary">${test.category || 'UNCATEGORIZED'}</span>
            </td>
            <td>
                <code class="small">${test.urn || 'N/A'}</code>
            </td>
            <td>${resultsDisplay}</td>
            <td>${lastRunDisplay}</td>
            <td>${statusBadge}</td>
            <td>${actions}</td>
        </tr>
    `;
}

function getStatusBadge(status) {
    switch(status) {
        case 'synced':
            return '<span class="badge bg-success">Synced</span>';
        case 'local_only':
            return '<span class="badge bg-secondary">Local Only</span>';
        case 'remote_only':
            return '<span class="badge bg-info">Remote Only</span>';
        default:
            return '<span class="badge bg-warning">Unknown</span>';
    }
}

function getActionButtons(test, tabType) {
    const buttons = [];
    
    // View button for all tabs
    buttons.push(`<button class="btn btn-sm btn-outline-info" onclick="viewTest('${test.urn}')" title="View Details">
        <i class="fas fa-eye"></i>
    </button>`);
    
    // Edit button for synced and local tests (not remote-only)
    if (tabType === 'synced' || tabType === 'local') {
        buttons.push(`<button class="btn btn-sm btn-outline-primary" onclick="editTest('${test.urn}')" title="Edit Test">
            <i class="fas fa-edit"></i>
        </button>`);
    }
    
    if (tabType === 'synced') {
        buttons.push(`<button class="btn btn-sm btn-outline-success" onclick="pushTestToDataHub('${test.id}')" title="Push to DataHub">
            <i class="fas fa-upload"></i>
        </button>`);
        buttons.push(`<button class="btn btn-sm btn-outline-warning" onclick="addTestToPR('${test.id}')" title="Add to PR">
            <i class="fab fa-github"></i>
        </button>`);
        buttons.push(`<button class="btn btn-sm btn-outline-danger" onclick="deleteLocalTest('${test.id}')" title="Remove from Local (keeps on DataHub)">
            <i class="fas fa-trash"></i>
        </button>`);
    } else if (tabType === 'local') {
        buttons.push(`<button class="btn btn-sm btn-outline-success" onclick="pushTestToDataHub('${test.id}')" title="Push to DataHub">
            <i class="fas fa-upload"></i>
        </button>`);
        buttons.push(`<button class="btn btn-sm btn-outline-warning" onclick="addTestToPR('${test.id}')" title="Add to PR">
            <i class="fab fa-github"></i>
        </button>`);
        buttons.push(`<button class="btn btn-sm btn-outline-danger" onclick="deleteLocalTest('${test.id}')" title="Delete Local Test">
            <i class="fas fa-trash"></i>
        </button>`);
    } else if (tabType === 'remote') {
        buttons.push(`<button class="btn btn-sm btn-outline-primary" onclick="syncTestToLocal('${test.urn}')" title="Sync to Local">
            <i class="fas fa-download"></i>
        </button>`);
        buttons.push(`<button class="btn btn-sm btn-outline-warning" onclick="addRemoteTestToPR('${test.urn}')" title="Add to PR">
            <i class="fab fa-github"></i>
        </button>`);
        buttons.push(`<button class="btn btn-sm btn-outline-danger" onclick="deleteRemoteTest('${test.urn}')" title="Delete from DataHub">
            <i class="fas fa-trash"></i>
        </button>`);
    }
    
    return `<div class="btn-group" role="group">${buttons.join('')}</div>`;
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
        showNotification('Test not found', 'error');
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
        showNotification('Test not found', 'error');
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
    
    // Handle definition JSON - convert to YAML if needed
    let definitionText = '';
    if (test.definition_json) {
        try {
            if (typeof test.definition_json === 'string') {
                // If it's already a string, use it directly
                definitionText = test.definition_json;
            } else {
                // If it's an object, stringify it
                definitionText = JSON.stringify(test.definition_json, null, 2);
            }
        } catch (e) {
            definitionText = test.definition_json || '';
        }
    }
    document.getElementById('editTestDefinition').value = definitionText;
}

function addTestToPR(testId) {
    showNotification('Add to PR functionality will be implemented', 'info');
}

function addRemoteTestToPR(testUrn) {
    showNotification('Add remote test to PR functionality will be implemented', 'info');
}

function syncTestToLocal(testUrn) {
    showNotification('Sync to local functionality will be implemented', 'info');
}

function pushTestToDataHub(testId) {
    showNotification('Push to DataHub functionality will be implemented', 'info');
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
        showNotification('No tests selected', 'warning');
        return;
    }
    
    showNotification(`Bulk add to PR functionality will be implemented for ${selectedTests.length} tests`, 'info');
}

function bulkDeleteLocal(tabType) {
    const selectedTests = getSelectedTests(tabType);
    if (selectedTests.length === 0) {
        showNotification('No tests selected', 'warning');
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
                showNotification(`Successfully deleted ${successful} test(s)`, 'success');
            } else {
                showNotification(`Deleted ${successful} test(s), ${failed} failed`, 'warning');
            }
            
            loadTestsData(); // Refresh data
        })
        .catch(error => {
            showNotification('Error during bulk delete', 'error');
            console.error('Error:', error);
        });
    }
}

function bulkSyncToLocal(tabType) {
    const selectedTests = getSelectedTests(tabType);
    if (selectedTests.length === 0) {
        showNotification('No tests selected', 'warning');
        return;
    }
    
    showNotification(`Bulk sync to local functionality will be implemented for ${selectedTests.length} tests`, 'info');
}

function bulkDeleteRemote(tabType) {
    const selectedTests = getSelectedTests(tabType);
    if (selectedTests.length === 0) {
        showNotification('No tests selected', 'warning');
        return;
    }
    
    showNotification(`Bulk delete remote functionality will be implemented for ${selectedTests.length} tests`, 'info');
}

function getSelectedTests(tabType) {
    const checkboxes = document.querySelectorAll(`.test-checkbox[data-tab="${tabType}"]:checked`);
    return Array.from(checkboxes).map(cb => cb.getAttribute('data-test-id'));
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
    
    // Show definition if available
    const definitionElement = document.getElementById('modal-test-definition');
    if (test.definition_json && test.definition_json.trim()) {
        try {
            const formatted = JSON.stringify(JSON.parse(test.definition_json), null, 2);
            definitionElement.textContent = formatted;
        } catch (e) {
            definitionElement.textContent = test.definition_json;
        }
    } else {
        definitionElement.textContent = 'No definition available';
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

function showNotification(message, type = 'info') {
    // Create notification element
    const notification = document.createElement('div');
    notification.className = `alert alert-${type === 'error' ? 'danger' : type} alert-dismissible fade show notification`;
    notification.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    // Add to page
    document.body.appendChild(notification);
    
    // Auto remove after 5 seconds
    setTimeout(() => {
        if (notification.parentNode) {
            notification.parentNode.removeChild(notification);
        }
    }, 5000);
} 