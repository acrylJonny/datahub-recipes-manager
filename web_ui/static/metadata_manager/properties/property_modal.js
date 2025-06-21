/**
 * Property Modal JavaScript
 * Handles create and edit property modal functionality
 */

document.addEventListener('DOMContentLoaded', function() {
    // Initialize modals
    initializeCreateModal();
    initializeEditModal();
});

/**
 * Initialize Create Property Modal
 */
function initializeCreateModal() {
    const createModal = document.getElementById('createPropertyModal');
    if (!createModal) return;

    const valueTypeSelect = document.getElementById('createPropertyValueType');
    const allowedEntityTypesSection = document.getElementById('allowedEntityTypesSection');
    const allowedValuesSection = document.getElementById('allowedValuesSection');
    const showAsAssetBadgeCheckbox = document.getElementById('createShowAsAssetBadge');
    const showInColumnsTableCheckbox = document.getElementById('createShowInColumnsTable');
    const showInColumnsTableContainer = document.getElementById('showInColumnsTableContainer');
    const addAllowedValueBtn = document.getElementById('addAllowedValue');
    const allowedValuesContainer = document.getElementById('allowedValuesContainer');

    // Value type change handler
    if (valueTypeSelect) {
        valueTypeSelect.addEventListener('change', function() {
            const selectedValue = this.value;
            
            // Show/hide allowed entity types section for URN type
            if (selectedValue === 'urn') {
                allowedEntityTypesSection.style.display = 'block';
                document.getElementById('createPropertyAllowedEntityTypes').required = true;
            } else {
                allowedEntityTypesSection.style.display = 'none';
                document.getElementById('createPropertyAllowedEntityTypes').required = false;
            }
            
            // Show/hide allowed values section for non-URN types
            if (selectedValue === 'urn') {
                allowedValuesSection.style.display = 'none';
            } else {
                allowedValuesSection.style.display = 'block';
            }
        });
    }

    // Show as Asset Badge change handler
    if (showAsAssetBadgeCheckbox) {
        showAsAssetBadgeCheckbox.addEventListener('change', function() {
            if (this.checked) {
                showInColumnsTableCheckbox.disabled = false;
                showInColumnsTableContainer.classList.remove('text-muted');
            } else {
                showInColumnsTableCheckbox.checked = false;
                showInColumnsTableCheckbox.disabled = true;
                showInColumnsTableContainer.classList.add('text-muted');
            }
        });
    }

    // Add allowed value button handler
    if (addAllowedValueBtn) {
        addAllowedValueBtn.addEventListener('click', function() {
            addAllowedValueRow(allowedValuesContainer, 'create');
        });
    }

    // Form submission handler
    const createForm = document.getElementById('createPropertyForm');
    if (createForm) {
        createForm.addEventListener('submit', function(e) {
            if (!validateCreateForm()) {
                e.preventDefault();
            }
        });
    }

    // Initialize Select2 for allowed entity types
    const allowedEntityTypesSelect = document.getElementById('createPropertyAllowedEntityTypes');
    if (allowedEntityTypesSelect) {
        $(allowedEntityTypesSelect).select2({
            theme: 'bootstrap-5',
            width: '100%',
            placeholder: 'Select allowed entity types...',
            allowClear: true,
            dropdownParent: createModal
        });
    }
}

/**
 * Initialize Edit Property Modal
 */
function initializeEditModal() {
    const editModal = document.getElementById('editPropertyModal');
    if (!editModal) return;

    const valueTypeSelect = document.getElementById('editPropertyValueType');
    const allowedEntityTypesSection = document.getElementById('editAllowedEntityTypesSection');
    const allowedValuesSection = document.getElementById('editAllowedValuesSection');
    const showAsAssetBadgeCheckbox = document.getElementById('editShowAsAssetBadge');
    const showInColumnsTableCheckbox = document.getElementById('editShowInColumnsTable');
    const showInColumnsTableContainer = document.getElementById('editShowInColumnsTableContainer');
    const addEditAllowedValueBtn = document.getElementById('addEditAllowedValue');
    const editAllowedValuesContainer = document.getElementById('editAllowedValuesContainer');

    // Value type change handler
    if (valueTypeSelect) {
        valueTypeSelect.addEventListener('change', function() {
            const selectedValue = this.value;
            
            // Show/hide allowed entity types section for URN type
            if (selectedValue === 'urn') {
                allowedEntityTypesSection.style.display = 'block';
                document.getElementById('editPropertyAllowedEntityTypes').required = true;
            } else {
                allowedEntityTypesSection.style.display = 'none';
                document.getElementById('editPropertyAllowedEntityTypes').required = false;
            }
            
            // Show/hide allowed values section for non-URN types
            if (selectedValue === 'urn') {
                allowedValuesSection.style.display = 'none';
            } else {
                allowedValuesSection.style.display = 'block';
            }
        });
    }

    // Show as Asset Badge change handler
    if (showAsAssetBadgeCheckbox) {
        showAsAssetBadgeCheckbox.addEventListener('change', function() {
            if (this.checked) {
                showInColumnsTableCheckbox.disabled = false;
                showInColumnsTableContainer.classList.remove('text-muted');
            } else {
                showInColumnsTableCheckbox.checked = false;
                showInColumnsTableCheckbox.disabled = true;
                showInColumnsTableContainer.classList.add('text-muted');
            }
        });
    }

    // Add allowed value button handler
    if (addEditAllowedValueBtn) {
        addEditAllowedValueBtn.addEventListener('click', function() {
            addAllowedValueRow(editAllowedValuesContainer, 'edit');
        });
    }

    // Form submission handler
    const editForm = document.getElementById('editPropertyForm');
    if (editForm) {
        editForm.addEventListener('submit', function(e) {
            if (!validateEditForm()) {
                e.preventDefault();
            }
        });
    }

    // Initialize Select2 for allowed entity types
    const allowedEntityTypesSelect = document.getElementById('editPropertyAllowedEntityTypes');
    if (allowedEntityTypesSelect) {
        $(allowedEntityTypesSelect).select2({
            theme: 'bootstrap-5',
            width: '100%',
            placeholder: 'Select allowed entity types...',
            allowClear: true,
            dropdownParent: editModal
        });
    }
}

/**
 * Add a new allowed value row
 */
function addAllowedValueRow(container, modalType) {
    const row = document.createElement('div');
    row.className = `row mb-2 ${modalType}-allowed-value-row`;
    
    row.innerHTML = `
        <div class="col-md-6">
            <input type="text" class="form-control" name="allowed_values[]" placeholder="Value">
        </div>
        <div class="col-md-5">
            <input type="text" class="form-control" name="allowed_value_descriptions[]" placeholder="Description (optional)">
        </div>
        <div class="col-md-1">
            <button type="button" class="btn btn-sm btn-outline-danger remove-${modalType}-allowed-value">
                <i class="fas fa-times"></i>
            </button>
        </div>
    `;
    
    container.appendChild(row);
    
    // Add remove handler
    const removeBtn = row.querySelector(`.remove-${modalType}-allowed-value`);
    removeBtn.addEventListener('click', function() {
        container.removeChild(row);
        updateRemoveButtons(container, modalType);
    });
    
    updateRemoveButtons(container, modalType);
}

/**
 * Update remove buttons visibility
 */
function updateRemoveButtons(container, modalType) {
    const rows = container.querySelectorAll(`.${modalType}-allowed-value-row`);
    const removeButtons = container.querySelectorAll(`.remove-${modalType}-allowed-value`);
    
    if (rows.length === 1) {
        removeButtons.forEach(btn => btn.style.display = 'none');
    } else {
        removeButtons.forEach(btn => btn.style.display = 'block');
    }
}

/**
 * Validate Create Form
 */
function validateCreateForm() {
    const form = document.getElementById('createPropertyForm');
    const name = form.querySelector('#createPropertyName').value.trim();
    const valueType = form.querySelector('#createPropertyValueType').value;
    const cardinality = form.querySelector('#createPropertyCardinality').value;
    const entityTypes = form.querySelectorAll('input[name="entity_types"]:checked');
    const allowedEntityTypes = form.querySelector('#createPropertyAllowedEntityTypes');
    
    // Clear previous errors
    clearFormErrors(form);
    
    let isValid = true;
    
    // Validate required fields
    if (!name) {
        showFieldError('#createPropertyName', 'Property name is required');
        isValid = false;
    }
    
    if (!valueType) {
        showFieldError('#createPropertyValueType', 'Value type is required');
        isValid = false;
    }
    
    if (!cardinality) {
        showFieldError('#createPropertyCardinality', 'Cardinality is required');
        isValid = false;
    }
    
    if (entityTypes.length === 0) {
        showFieldError('.form-check', 'At least one entity type must be selected');
        isValid = false;
    }
    
    // Validate URN-specific requirements
    if (valueType === 'urn' && allowedEntityTypes) {
        const selectedAllowedTypes = $(allowedEntityTypes).val();
        if (!selectedAllowedTypes || selectedAllowedTypes.length === 0) {
            showFieldError('#createPropertyAllowedEntityTypes', 'Allowed entity types are required for URN properties');
            isValid = false;
        }
    }
    
    // Validate allowed values
    if (valueType !== 'urn') {
        const allowedValueInputs = form.querySelectorAll('input[name="allowed_values[]"]');
        const values = [];
        let hasEmptyValue = false;
        
        allowedValueInputs.forEach(input => {
            const value = input.value.trim();
            if (value) {
                if (values.includes(value)) {
                    showFieldError(input, 'Duplicate values are not allowed');
                    isValid = false;
                } else {
                    values.push(value);
                }
            } else {
                hasEmptyValue = true;
            }
        });
        
        if (hasEmptyValue && values.length > 0) {
            showFieldError('#allowedValuesContainer', 'All allowed values must be filled or empty');
            isValid = false;
        }
    }
    
    return isValid;
}

/**
 * Validate Edit Form
 */
function validateEditForm() {
    const form = document.getElementById('editPropertyForm');
    const name = form.querySelector('#editPropertyName').value.trim();
    const valueType = form.querySelector('#editPropertyValueType').value;
    const cardinality = form.querySelector('#editPropertyCardinality').value;
    const entityTypes = form.querySelectorAll('input[name="entity_types"]:checked');
    const allowedEntityTypes = form.querySelector('#editPropertyAllowedEntityTypes');
    
    // Clear previous errors
    clearFormErrors(form);
    
    let isValid = true;
    
    // Validate required fields
    if (!name) {
        showFieldError('#editPropertyName', 'Property name is required');
        isValid = false;
    }
    
    if (!valueType) {
        showFieldError('#editPropertyValueType', 'Value type is required');
        isValid = false;
    }
    
    if (!cardinality) {
        showFieldError('#editPropertyCardinality', 'Cardinality is required');
        isValid = false;
    }
    
    if (entityTypes.length === 0) {
        showFieldError('.form-check', 'At least one entity type must be selected');
        isValid = false;
    }
    
    // Validate URN-specific requirements
    if (valueType === 'urn' && allowedEntityTypes) {
        const selectedAllowedTypes = $(allowedEntityTypes).val();
        if (!selectedAllowedTypes || selectedAllowedTypes.length === 0) {
            showFieldError('#editPropertyAllowedEntityTypes', 'Allowed entity types are required for URN properties');
            isValid = false;
        }
    }
    
    // Validate allowed values
    if (valueType !== 'urn') {
        const allowedValueInputs = form.querySelectorAll('input[name="allowed_values[]"]');
        const values = [];
        let hasEmptyValue = false;
        
        allowedValueInputs.forEach(input => {
            const value = input.value.trim();
            if (value) {
                if (values.includes(value)) {
                    showFieldError(input, 'Duplicate values are not allowed');
                    isValid = false;
                } else {
                    values.push(value);
                }
            } else {
                hasEmptyValue = true;
            }
        });
        
        if (hasEmptyValue && values.length > 0) {
            showFieldError('#editAllowedValuesContainer', 'All allowed values must be filled or empty');
            isValid = false;
        }
    }
    
    return isValid;
}

/**
 * Show field error
 */
function showFieldError(selector, message) {
    const field = document.querySelector(selector);
    if (field) {
        field.classList.add('is-invalid');
        
        // Remove existing error message
        const existingError = field.parentNode.querySelector('.invalid-feedback');
        if (existingError) {
            existingError.remove();
        }
        
        // Add new error message
        const errorDiv = document.createElement('div');
        errorDiv.className = 'invalid-feedback';
        errorDiv.textContent = message;
        field.parentNode.appendChild(errorDiv);
    }
}

/**
 * Clear form errors
 */
function clearFormErrors(form) {
    const invalidFields = form.querySelectorAll('.is-invalid');
    invalidFields.forEach(field => {
        field.classList.remove('is-invalid');
    });
    
    const errorMessages = form.querySelectorAll('.invalid-feedback');
    errorMessages.forEach(error => {
        error.remove();
    });
}

/**
 * Populate edit modal with property data
 */
function populateEditModal(property) {
    const modal = document.getElementById('editPropertyModal');
    if (!modal) return;
    
    // Basic information
    document.getElementById('editPropertyId').value = property.id;
    document.getElementById('editPropertyName').value = property.name || '';
    document.getElementById('editPropertyQualifiedName').value = property.qualified_name || '';
    document.getElementById('editPropertyDescription').value = property.description || '';
    
    // Configuration
    document.getElementById('editPropertyValueType').value = property.value_type || '';
    document.getElementById('editPropertyCardinality').value = property.cardinality || '';
    document.getElementById('editPropertyImmutable').checked = property.immutable || false;
    
    // Entity types
    const entityTypes = property.entity_types || [];
    document.querySelectorAll('input[name="entity_types"]').forEach(checkbox => {
        checkbox.checked = entityTypes.includes(checkbox.value);
    });
    
    // Display settings
    document.getElementById('editShowInSearchFilters').checked = property.show_in_search_filters || false;
    document.getElementById('editShowAsAssetBadge').checked = property.show_as_asset_badge || false;
    document.getElementById('editShowInAssetSummary').checked = property.show_in_asset_summary || false;
    document.getElementById('editShowInColumnsTable').checked = property.show_in_columns_table || false;
    document.getElementById('editIsHidden').checked = property.is_hidden || false;
    
    // Handle conditional display
    const valueType = property.value_type;
    if (valueType === 'urn') {
        document.getElementById('editAllowedEntityTypesSection').style.display = 'block';
        document.getElementById('editAllowedValuesSection').style.display = 'none';
        
        // Populate allowed entity types if available
        if (property.allowed_entity_types) {
            const allowedEntityTypesSelect = document.getElementById('editPropertyAllowedEntityTypes');
            $(allowedEntityTypesSelect).val(property.allowed_entity_types).trigger('change');
        }
    } else {
        document.getElementById('editAllowedEntityTypesSection').style.display = 'none';
        document.getElementById('editAllowedValuesSection').style.display = 'block';
        
        // Populate allowed values if available
        if (property.allowedValues && property.allowedValues.length > 0) {
            populateAllowedValues('edit', property.allowedValues);
        }
    }
    
    // Handle show in columns table dependency
    const showAsAssetBadge = property.show_as_asset_badge;
    if (!showAsAssetBadge) {
        document.getElementById('editShowInColumnsTable').disabled = true;
        document.getElementById('editShowInColumnsTableContainer').classList.add('text-muted');
    }
    
    // Trigger change events to update UI
    document.getElementById('editPropertyValueType').dispatchEvent(new Event('change'));
    document.getElementById('editShowAsAssetBadge').dispatchEvent(new Event('change'));
}

/**
 * Populate allowed values
 */
function populateAllowedValues(modalType, allowedValues) {
    const container = document.getElementById(`${modalType}AllowedValuesContainer`);
    if (!container) return;
    
    // Clear existing rows
    container.innerHTML = '';
    
    allowedValues.forEach((value, index) => {
        const row = document.createElement('div');
        row.className = `row mb-2 ${modalType}-allowed-value-row`;
        
        row.innerHTML = `
            <div class="col-md-6">
                <input type="text" class="form-control" name="allowed_values[]" value="${value.value || value}" placeholder="Value">
            </div>
            <div class="col-md-5">
                <input type="text" class="form-control" name="allowed_value_descriptions[]" value="${value.description || ''}" placeholder="Description (optional)">
            </div>
            <div class="col-md-1">
                <button type="button" class="btn btn-sm btn-outline-danger remove-${modalType}-allowed-value">
                    <i class="fas fa-times"></i>
                </button>
            </div>
        `;
        
        container.appendChild(row);
        
        // Add remove handler
        const removeBtn = row.querySelector(`.remove-${modalType}-allowed-value`);
        removeBtn.addEventListener('click', function() {
            container.removeChild(row);
            updateRemoveButtons(container, modalType);
        });
    });
    
    updateRemoveButtons(container, modalType);
}

/**
 * Reset create modal
 */
function resetCreateModal() {
    const modal = document.getElementById('createPropertyModal');
    if (!modal) return;
    
    const form = document.getElementById('createPropertyForm');
    form.reset();
    
    // Clear Select2
    const allowedEntityTypesSelect = document.getElementById('createPropertyAllowedEntityTypes');
    if (allowedEntityTypesSelect) {
        $(allowedEntityTypesSelect).val(null).trigger('change');
    }
    
    // Reset allowed values
    const allowedValuesContainer = document.getElementById('allowedValuesContainer');
    allowedValuesContainer.innerHTML = `
        <div class="row mb-2 allowed-value-row">
            <div class="col-md-6">
                <input type="text" class="form-control" name="allowed_values[]" placeholder="Value">
            </div>
            <div class="col-md-5">
                <input type="text" class="form-control" name="allowed_value_descriptions[]" placeholder="Description (optional)">
            </div>
            <div class="col-md-1">
                <button type="button" class="btn btn-sm btn-outline-danger remove-allowed-value" style="display: none;">
                    <i class="fas fa-times"></i>
                </button>
            </div>
        </div>
    `;
    
    // Reset conditional displays
    document.getElementById('allowedEntityTypesSection').style.display = 'none';
    document.getElementById('allowedValuesSection').style.display = 'block';
    document.getElementById('createShowInColumnsTable').disabled = false;
    document.getElementById('showInColumnsTableContainer').classList.remove('text-muted');
    
    // Clear errors
    clearFormErrors(form);
}

// Export functions for global access
window.propertyModal = {
    populateEditModal,
    resetCreateModal,
    validateCreateForm,
    validateEditForm
}; 