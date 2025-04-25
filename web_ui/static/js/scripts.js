/**
 * DataHub Recipes Manager - Common JavaScript functions
 */

document.addEventListener('DOMContentLoaded', function() {
    // Initialize tooltips
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // Initialize popovers
    var popoverTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="popover"]'));
    popoverTriggerList.map(function (popoverTriggerEl) {
        return new bootstrap.Popover(popoverTriggerEl);
    });

    // Add confirm dialog to delete buttons
    document.querySelectorAll('.confirm-delete').forEach(function(button) {
        button.addEventListener('click', function(event) {
            if (!confirm('Are you sure you want to delete this item? This action cannot be undone.')) {
                event.preventDefault();
            }
        });
    });

    // Format JSON displays if highlight.js is available
    if (typeof hljs !== 'undefined') {
        document.querySelectorAll('.json-content').forEach(function(element) {
            if (element.textContent) {
                try {
                    const jsonObj = JSON.parse(element.textContent);
                    const formatted = JSON.stringify(jsonObj, null, 2);
                    element.textContent = formatted;
                    hljs.highlightElement(element);
                } catch(e) {
                    console.error('Error formatting JSON:', e);
                }
            }
        });
    }

    // Copy to clipboard functionality
    document.querySelectorAll('.copy-button').forEach(function(button) {
        button.addEventListener('click', function() {
            const target = document.querySelector(button.dataset.copyTarget);
            if (target) {
                navigator.clipboard.writeText(target.textContent).then(function() {
                    // Show success feedback
                    const originalText = button.innerHTML;
                    button.innerHTML = '<i class="fas fa-check me-1"></i> Copied!';
                    setTimeout(function() {
                        button.innerHTML = originalText;
                    }, 2000);
                }).catch(function(err) {
                    console.error('Failed to copy text: ', err);
                });
            }
        });
    });
}); 