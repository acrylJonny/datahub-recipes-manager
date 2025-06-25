/**
 * Common notification utilities for the DataHub Recipes Manager
 * Provides toast notifications with consistent styling across the application
 */

/**
 * Show a toast notification with consistent styling
 * @param {string} type - The type of notification (success, error, warning, info)
 * @param {string} message - The message to display
 */
function showToast(type, message) {
    // Check if we have notifications container
    let container = document.getElementById('notifications-container');
    
    // Create it if it doesn't exist
    if (!container) {
        container = document.createElement('div');
        container.id = 'notifications-container';
        container.className = 'position-fixed bottom-0 end-0 p-3';
        container.style.zIndex = '9999';
        document.body.appendChild(container);
    }
    
    // Create unique ID
    const id = 'toast-' + Date.now() + '-' + Math.random().toString(36).substr(2, 9);
    
    // Determine title and icon based on type
    let title, icon, bgClass;
    switch (type) {
        case 'success':
            title = 'Success';
            icon = 'fas fa-check-circle';
            bgClass = 'bg-success';
            break;
        case 'error':
            title = 'Error';
            icon = 'fas fa-exclamation-circle';
            bgClass = 'bg-danger';
            break;
        case 'warning':
            title = 'Warning';
            icon = 'fas fa-exclamation-triangle';
            bgClass = 'bg-warning';
            break;
        case 'info':
            title = 'Info';
            icon = 'fas fa-info-circle';
            bgClass = 'bg-info';
            break;
        default:
            title = 'Notification';
            icon = 'fas fa-bell';
            bgClass = 'bg-primary';
    }
    
    // Create toast HTML (matching tags page style)
    const toastHtml = `
        <div id="${id}" class="toast align-items-center text-white ${bgClass} border-0 mb-3" role="alert" aria-live="assertive" aria-atomic="true">
            <div class="d-flex">
                <div class="toast-body">
                    <i class="${icon} me-2"></i>${message}
                </div>
                <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button>
            </div>
        </div>
    `;
    
    // Add to container
    container.insertAdjacentHTML('beforeend', toastHtml);
    
    // Initialize and show the toast
    const toastElement = document.getElementById(id);
    const toast = new bootstrap.Toast(toastElement, {
        autohide: true,
        delay: 5000
    });
    
    // Show the toast
    toast.show();
    
    // Remove from DOM after hiding
    toastElement.addEventListener('hidden.bs.toast', function() {
        toastElement.remove();
    });
}

/**
 * Handle Django messages by converting them to toast notifications
 * This function should be called on page load to convert server-side messages
 * @param {Array} messages - Array of Django messages {type: string, text: string}
 */
function handleDjangoMessages(messages) {
    if (!messages || messages.length === 0) return;
    
    messages.forEach(function(msg) {
        showToast(msg.type, msg.text);
    });
}

/**
 * Initialize Django messages handling on page load
 * This function looks for Django messages in the global scope and converts them to toasts
 */
function initializeDjangoMessages() {
    // This will be populated by Django template in each page
    if (typeof window.djangoMessages !== 'undefined' && window.djangoMessages.length > 0) {
        handleDjangoMessages(window.djangoMessages);
    }
}

// Auto-initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', initializeDjangoMessages);

// Export functions for use in other scripts
window.Notifications = {
    showToast: showToast,
    handleDjangoMessages: handleDjangoMessages,
    initializeDjangoMessages: initializeDjangoMessages
}; 