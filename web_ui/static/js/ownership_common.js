/**
 * Common ownership functionality for tags and glossary items
 */

// Global ownership cache and utilities
window.OwnershipManager = {
    // Cache for users, groups, and ownership types by connection
    usersAndGroupsCacheByConnection: {},
    currentConnectionId: null,

    // API endpoints - use common endpoints that work across all pages
    API_ENDPOINTS: {
        users: '/metadata/api/users-groups/',
        groups: '/metadata/api/users-groups/', 
        ownershipTypes: '/metadata/api/users-groups/'
    },

    // Get or create cache for current connection
    getCurrentConnectionCache: function() {
        if (!this.currentConnectionId) {
            // Try to get connection ID from the page
            const connectionElement = document.getElementById('current-connection-name');
            if (connectionElement && connectionElement.dataset.connectionId) {
                this.currentConnectionId = connectionElement.dataset.connectionId;
            } else {
                // Fallback to default connection
                this.currentConnectionId = 'default';
            }
        }
        
        if (!this.usersAndGroupsCacheByConnection[this.currentConnectionId]) {
            this.usersAndGroupsCacheByConnection[this.currentConnectionId] = {
                users: [],
                groups: [],
                ownership_types: [],
                lastFetched: null,
                cacheExpiry: 5 * 60 * 1000 // 5 minutes
            };
        }
        
        return this.usersAndGroupsCacheByConnection[this.currentConnectionId];
    },

    // Load users and groups for the current connection
    loadUsersAndGroups: async function() {
        const cache = this.getCurrentConnectionCache();
        const now = Date.now();
        
        // Check if cache is still valid
        if (cache.lastFetched && (now - cache.lastFetched) < cache.cacheExpiry) {
            console.log('Using cached users and groups data');
            return cache;
        }

        console.log('Loading users and groups from server...');
        
        try {
            // Get CSRF token
            const csrfToken = document.querySelector('[name=csrfmiddlewaretoken]')?.value || '';
            
            // Use the common API endpoint that returns all data in one call
            const response = await fetch(this.API_ENDPOINTS.users, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': csrfToken
                },
                body: JSON.stringify({ type: 'all' })
            });

            if (response.ok) {
                const result = await response.json();
                
                if (result.success && result.data) {
                    cache.users = result.data.users || [];
                    cache.groups = result.data.groups || [];
                    cache.ownership_types = result.data.ownership_types || [];
                    cache.lastFetched = now;
                    
                    console.log(`Loaded ${cache.users.length} users, ${cache.groups.length} groups, ${cache.ownership_types.length} ownership types`);
                    return cache;
                } else {
                    console.error('API returned error:', result.error);
                    return cache;
                }
            } else {
                console.error('Failed to load users/groups data:', response.status);
                return cache;
            }
        } catch (error) {
            console.error('Error loading users and groups:', error);
            return cache;
        }
    },

    // Format owner option for dropdowns
    formatOwnerOption: function(option) {
        if (!option.id) return option.text;
        
        const isGroup = option.id.includes(':corpGroup:');
        const icon = isGroup ? 'fas fa-users' : 'fas fa-user';
        const type = isGroup ? 'Group' : 'User';
        
        return `<i class="${icon} me-2"></i>${option.text} <small class="text-muted">(${type})</small>`;
    },

    // Format owner selection for display
    formatOwnerSelection: function(option) {
        if (!option.id) return option.text;
        
        const isGroup = option.id.includes(':corpGroup:');
        const icon = isGroup ? 'fas fa-users' : 'fas fa-user';
        
        return `<i class="${icon} me-1"></i>${option.text}`;
    },

    // Populate owners dropdown
    populateOwnersSelect: function(select) {
        const cache = this.getCurrentConnectionCache();
        
        // Clear existing options except the first one (placeholder)
        while (select.children.length > 1) {
            select.removeChild(select.lastChild);
        }
        
        // Add users
        cache.users.forEach(user => {
            const option = document.createElement('option');
            option.value = user.urn;
            // Handle different possible property names from the API
            option.textContent = user.display_name || user.displayName || user.username || user.urn;
            option.dataset.type = 'user';
            select.appendChild(option);
        });
        
        // Add groups
        cache.groups.forEach(group => {
            const option = document.createElement('option');
            option.value = group.urn;
            // Handle different possible property names from the API
            option.textContent = group.display_name || group.displayName || group.name || group.urn;
            option.dataset.type = 'group';
            select.appendChild(option);
        });
    },

    // Populate ownership type dropdown
    populateOwnershipTypeSelect: function(select) {
        const cache = this.getCurrentConnectionCache();
        
        // Clear existing options except the first one (placeholder)
        while (select.children.length > 1) {
            select.removeChild(select.lastChild);
        }
        
        // Add ownership types
        cache.ownership_types.forEach(ownershipType => {
            const option = document.createElement('option');
            option.value = ownershipType.urn;
            // Handle different possible property names from the API
            option.textContent = ownershipType.name || ownershipType.display_name || ownershipType.displayName || ownershipType.urn;
            select.appendChild(option);
        });
    },

    // Add ownership section to a form
    addOwnershipSection: function(containerId, sectionIndex = 1) {
        const container = document.getElementById(containerId);
        if (!container) {
            console.error(`Container ${containerId} not found`);
            return;
        }

        const cache = this.getCurrentConnectionCache();
        
        const sectionHtml = `
            <div class="ownership-section mb-3" id="ownership-section-${sectionIndex}">
                <div class="row">
                    <div class="col-md-6">
                        <label for="owner-${sectionIndex}" class="form-label">Owner</label>
                        <select class="form-select owner-select" id="owner-${sectionIndex}" name="owners">
                            <option value="">Select an owner...</option>
                        </select>
                    </div>
                    <div class="col-md-4">
                        <label for="ownership-type-${sectionIndex}" class="form-label">Ownership Type</label>
                        <select class="form-select ownership-type-select" id="ownership-type-${sectionIndex}" name="ownership_types">
                            <option value="">Select type...</option>
                        </select>
                    </div>
                    <div class="col-md-2 d-flex align-items-end">
                        <button type="button" class="btn btn-outline-danger remove-ownership-btn" onclick="OwnershipManager.removeSection('ownership-section-${sectionIndex}')">
                            <i class="fas fa-trash"></i>
                        </button>
                    </div>
                </div>
            </div>
        `;

        container.insertAdjacentHTML('beforeend', sectionHtml);

        // Populate the dropdowns
        const ownerSelect = document.getElementById(`owner-${sectionIndex}`);
        const ownershipTypeSelect = document.getElementById(`ownership-type-${sectionIndex}`);
        
        this.populateOwnersSelect(ownerSelect);
        this.populateOwnershipTypeSelect(ownershipTypeSelect);

        this.updateRemoveButtons(containerId);
    },

    // Remove ownership section
    removeSection: function(sectionId) {
        const section = document.getElementById(sectionId);
        if (section) {
            const container = section.parentElement;
            section.remove();
            this.updateRemoveButtons(container.id);
        }
    },

    // Update remove button visibility
    updateRemoveButtons: function(containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;

        const sections = container.querySelectorAll('.ownership-section');
        const removeButtons = container.querySelectorAll('.remove-ownership-btn');
        
        // Show remove buttons only if there's more than one section
        removeButtons.forEach(button => {
            button.style.display = sections.length > 1 ? 'block' : 'none';
        });
    },

    // Show ownership section if hidden
    showOwnershipSection: function(containerId) {
        const container = document.getElementById(containerId);
        if (container) {
            container.style.display = 'block';
        }
    },

    // Hide ownership section if empty
    hideOwnershipSectionIfEmpty: function(containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;

        const sections = container.querySelectorAll('.ownership-section');
        if (sections.length === 0) {
            container.style.display = 'none';
        }
    },

    // Get selected ownership data from form
    getSelectedOwnershipData: function(containerId) {
        const container = document.getElementById(containerId);
        if (!container) return [];

        const sections = container.querySelectorAll('.ownership-section');
        const ownershipData = [];

        sections.forEach(section => {
            const ownerSelect = section.querySelector('.owner-select');
            const ownershipTypeSelect = section.querySelector('.ownership-type-select');
            
            if (ownerSelect.value && ownershipTypeSelect.value) {
                ownershipData.push({
                    owner: ownerSelect.value,
                    ownershipType: ownershipTypeSelect.value
                });
            }
        });

        return ownershipData;
    },

    // Populate form with existing ownership data
    populateOwnershipForm: function(containerId, ownershipData) {
        const container = document.getElementById(containerId);
        if (!container) return;

        // Clear existing sections
        container.innerHTML = '';

        if (!ownershipData || ownershipData.length === 0) {
            // Add one empty section
            this.addOwnershipSection(containerId, 1);
            this.hideOwnershipSectionIfEmpty(containerId);
            return;
        }

        // Add sections for each ownership entry
        ownershipData.forEach((ownership, index) => {
            this.addOwnershipSection(containerId, index + 1);
            
            // Set the values
            const ownerSelect = document.getElementById(`owner-${index + 1}`);
            const ownershipTypeSelect = document.getElementById(`ownership-type-${index + 1}`);
            
            if (ownerSelect && ownership.owner) {
                ownerSelect.value = ownership.owner;
            }
            if (ownershipTypeSelect && ownership.ownershipType) {
                ownershipTypeSelect.value = ownership.ownershipType;
            }
        });

        this.showOwnershipSection(containerId);
    },

    // Display ownership information in a modal or details view
    displayOwnershipInfo: function(ownershipData, containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;

        if (!ownershipData || ownershipData.length === 0) {
            container.innerHTML = '<span class="text-muted">No owners assigned</span>';
            return;
        }

        const cache = this.getCurrentConnectionCache();
        
        // Group owners by ownership type
        const ownersByType = {};
        
        ownershipData.forEach(ownerInfo => {
            let ownerUrn, ownershipTypeUrn, ownershipTypeName;
            
            // Handle different data formats
            if (typeof ownerInfo === 'object' && ownerInfo.owner) {
                // New format from GraphQL
                ownerUrn = ownerInfo.owner.urn || ownerInfo.owner;
                ownershipTypeUrn = ownerInfo.ownershipType?.urn || ownerInfo.ownershipType;
            } else if (typeof ownerInfo === 'string') {
                // Simple URN format
                ownerUrn = ownerInfo;
                ownershipTypeUrn = 'urn:li:ownershipType:__system__business_owner'; // Default
            } else {
                // Fallback
                ownerUrn = ownerInfo.owner || ownerInfo;
                ownershipTypeUrn = ownerInfo.ownershipType || 'urn:li:ownershipType:__system__business_owner';
            }
            
            // Find ownership type name
            const ownershipType = cache.ownership_types.find(ot => ot.urn === ownershipTypeUrn);
            ownershipTypeName = ownershipType ? ownershipType.name : 'Business Owner';
            
            if (!ownersByType[ownershipTypeName]) {
                ownersByType[ownershipTypeName] = [];
            }
            
            // Find owner name
            let ownerName = 'Unknown';
            const user = cache.users.find(u => u.urn === ownerUrn);
            const group = cache.groups.find(g => g.urn === ownerUrn);
            
            if (user) {
                ownerName = user.displayName || user.username || user.urn;
            } else if (group) {
                ownerName = group.displayName || group.name || group.urn;
            }
            
            ownersByType[ownershipTypeName].push({
                name: ownerName,
                urn: ownerUrn,
                isGroup: ownerUrn.includes(':corpGroup:')
            });
        });
        
        // Generate HTML
        let html = '';
        Object.entries(ownersByType).forEach(([typeName, owners]) => {
            html += `<div class="mb-2">`;
            html += `<strong>${typeName}:</strong><br>`;
            owners.forEach(owner => {
                const icon = owner.isGroup ? 'fas fa-users text-info' : 'fas fa-user text-primary';
                html += `<span class="me-2"><i class="${icon} me-1"></i>${owner.name}</span>`;
            });
            html += `</div>`;
        });
        
        container.innerHTML = html;
    }
};

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    // Load users and groups cache
    OwnershipManager.loadUsersAndGroups();
}); 