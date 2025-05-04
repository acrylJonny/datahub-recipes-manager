// Add to GitHub PR button handler for policy detail page
document.addEventListener('DOMContentLoaded', function() {
    const addToGitPrBtn = document.getElementById('addToGitPrBtn');
    if (addToGitPrBtn) {
        addToGitPrBtn.addEventListener('click', function() {
            // Get policy ID from URL path
            const pathParts = window.location.pathname.split('/');
            const policyId = pathParts[pathParts.length - 2];
            
            // Show loading state
            const originalText = this.innerHTML;
            this.innerHTML = '<i class="fas fa-spinner fa-spin me-1"></i> Adding...';
            this.disabled = true;
            
            // Send POST request to add policy to GitHub PR
            fetch(`/policies/${policyId}/push-github/`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': document.querySelector('[name=csrfmiddlewaretoken]').value
                }
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    // Create success notification
                    const alert = document.createElement('div');
                    alert.className = 'alert alert-success alert-dismissible fade show';
                    alert.innerHTML = `
                        <i class="fas fa-check-circle me-2"></i>
                        Policy added to GitHub PR successfully!
                        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
                    `;
                    
                    // Insert at the top of the container
                    const container = document.querySelector('.container-fluid');
                    container.insertBefore(alert, container.firstChild);
                    
                    // Auto dismiss after 5 seconds
                    setTimeout(() => {
                        alert.classList.remove('show');
                        setTimeout(() => alert.remove(), 150);
                    }, 5000);
                } else {
                    alert(`Error: ${data.error || 'Failed to add policy to GitHub PR'}`);
                }
            })
            .catch(error => {
                alert(`Error: ${error.message || 'An error occurred'}`);
            })
            .finally(() => {
                // Restore button state
                this.innerHTML = originalText;
                this.disabled = false;
            });
        });
    }
}); 