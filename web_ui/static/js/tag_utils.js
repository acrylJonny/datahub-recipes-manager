/**
 * Tag utility functions for DataHub
 */

/**
 * Apply a tag to an entity
 * 
 * @param {string} entityUrn - Entity URN
 * @param {string} tagUrn - Tag URN
 * @param {string|null} colorHex - Optional color hex code (e.g. "#FF5733")
 * @returns {Promise} - Promise resolving to the API response
 */
function applyTagToEntity(entityUrn, tagUrn, colorHex = null) {
    // Get CSRF token
    const csrfToken = document.querySelector('[name=csrfmiddlewaretoken]')?.value;
    
    if (!csrfToken) {
        console.error('CSRF token not found');
        return Promise.reject(new Error('CSRF token not found'));
    }
    
    // Create request data
    const data = {
        entity_urn: entityUrn,
        tag_urn: tagUrn
    };
    
    // Add color if provided
    if (colorHex) {
        data.color_hex = colorHex;
    }
    
    // Send API request
    return fetch('/metadata/tags/entity/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': csrfToken
        },
        body: JSON.stringify(data)
    })
    .then(response => {
        if (!response.ok) {
            throw new Error(`Error applying tag: ${response.statusText}`);
        }
        return response.json();
    });
}

/**
 * Remove a tag from an entity
 * 
 * @param {string} entityUrn - Entity URN
 * @param {string} tagUrn - Tag URN
 * @returns {Promise} - Promise resolving to the API response
 */
function removeTagFromEntity(entityUrn, tagUrn) {
    // Get CSRF token
    const csrfToken = document.querySelector('[name=csrfmiddlewaretoken]')?.value;
    
    if (!csrfToken) {
        console.error('CSRF token not found');
        return Promise.reject(new Error('CSRF token not found'));
    }
    
    // Create request data
    const data = {
        entity_urn: entityUrn,
        tag_urn: tagUrn
    };
    
    // Send API request
    return fetch('/metadata/tags/entity/', {
        method: 'DELETE',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': csrfToken
        },
        body: JSON.stringify(data)
    })
    .then(response => {
        if (!response.ok) {
            throw new Error(`Error removing tag: ${response.statusText}`);
        }
        return response.json();
    });
}

/**
 * Show notification after tag operation
 * 
 * @param {boolean} success - Whether the operation was successful
 * @param {string} message - Message to display
 */
function showTagNotification(success, message) {
    // Create toast element
    const toastEl = document.createElement('div');
    toastEl.className = `toast align-items-center text-white bg-${success ? 'success' : 'danger'} border-0 position-fixed bottom-0 end-0 m-3`;
    toastEl.setAttribute('role', 'alert');
    toastEl.setAttribute('aria-live', 'assertive');
    toastEl.setAttribute('aria-atomic', 'true');
    
    // Add toast content
    toastEl.innerHTML = `
        <div class="d-flex">
            <div class="toast-body">
                <i class="fas fa-${success ? 'check-circle' : 'exclamation-circle'} me-2"></i>
                ${message}
            </div>
            <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button>
        </div>
    `;
    
    // Add to document
    document.body.appendChild(toastEl);
    
    // Initialize and show toast
    const toast = new bootstrap.Toast(toastEl, {
        delay: 5000
    });
    toast.show();
    
    // Remove from DOM after it's hidden
    toastEl.addEventListener('hidden.bs.toast', () => {
        toastEl.remove();
    });
} 