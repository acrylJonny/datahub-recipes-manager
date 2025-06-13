/**
 * Global utility functions for safe data handling across the application
 */

/**
 * Safely truncate text to prevent performance issues and JSON parsing problems
 * @param {string} text - The text to truncate
 * @param {number} maxLength - Maximum length allowed
 * @param {boolean} wordBoundary - Whether to break at word boundaries
 * @returns {string} - Safely truncated text
 */
function safeTruncateText(text, maxLength = 500, wordBoundary = true) {
    if (!text || typeof text !== 'string') return '';
    if (text.length <= maxLength) return text;
    
    if (wordBoundary) {
        // Find the last space before the max length to avoid cutting words
        const truncated = text.substring(0, maxLength);
        const lastSpace = truncated.lastIndexOf(' ');
        
        if (lastSpace > maxLength * 0.8) { // Only use word boundary if it's not too far back
            return text.substring(0, lastSpace) + '...';
        }
    }
    
    return text.substring(0, maxLength) + '...';
}

/**
 * Safely escape HTML to prevent XSS and handle very long strings
 * @param {string} text - The text to escape
 * @param {number} maxLength - Maximum length before truncation
 * @returns {string} - Safely escaped HTML
 */
function safeEscapeHtml(text, maxLength = 10000) {
    if (!text) return '';
    
    // Convert to string if not already
    text = String(text);
    
    // Limit extremely long strings to prevent performance issues
    if (text.length > maxLength) {
        text = text.substring(0, maxLength) + '... [truncated]';
    }
    
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Safely prepare an object for JSON.stringify by truncating long strings and handling problematic content
 * @param {object} obj - The object to sanitize
 * @param {number} maxStringLength - Maximum length for string values
 * @returns {object} - Sanitized object safe for JSON.stringify
 */
function sanitizeForJson(obj, maxStringLength = 200) {
    if (obj === null || obj === undefined) return obj;
    
    if (typeof obj === 'string') {
        // Remove or escape problematic characters that could break JSON
        let cleaned = obj
            .replace(/[\u0000-\u001F\u007F-\u009F]/g, '') // Remove control characters
            .replace(/["\\\n\r\t]/g, match => {
                switch (match) {
                    case '"': return '\\"';
                    case '\\': return '\\\\';
                    case '\n': return '\\n';
                    case '\r': return '\\r';
                    case '\t': return '\\t';
                    default: return '';
                }
            });
        
        return safeTruncateText(cleaned, maxStringLength);
    }
    
    if (typeof obj === 'number' || typeof obj === 'boolean') {
        return obj;
    }
    
    if (Array.isArray(obj)) {
        return obj.map(item => sanitizeForJson(item, maxStringLength));
    }
    
    if (typeof obj === 'object') {
        const sanitized = {};
        for (const [key, value] of Object.entries(obj)) {
            // Skip functions and undefined values
            if (typeof value === 'function' || value === undefined) continue;
            
            sanitized[key] = sanitizeForJson(value, maxStringLength);
        }
        return sanitized;
    }
    
    return obj;
}

/**
 * Safely stringify JSON with error handling and data sanitization
 * @param {any} data - The data to stringify
 * @param {number} maxStringLength - Maximum length for string values
 * @returns {string} - Safe JSON string
 */
function safeJsonStringify(data, maxStringLength = 200) {
    try {
        const sanitized = sanitizeForJson(data, maxStringLength);
        return JSON.stringify(sanitized);
    } catch (error) {
        console.error('Error stringifying JSON:', error);
        return JSON.stringify({ error: 'Failed to serialize data', type: typeof data });
    }
}

/**
 * Safely parse JSON with error handling
 * @param {string} jsonString - The JSON string to parse
 * @returns {any} - Parsed object or null if parsing fails
 */
function safeJsonParse(jsonString) {
    try {
        return JSON.parse(jsonString);
    } catch (error) {
        console.error('Error parsing JSON:', error);
        return null;
    }
}

/**
 * Create a display-safe version of an item for table rows and data attributes
 * @param {object} item - The original item
 * @param {object} options - Options for sanitization
 * @returns {object} - Display-safe item
 */
function createDisplaySafeItem(item, options = {}) {
    const {
        descriptionLength = 200,
        nameLength = 100,
        urnLength = 500
    } = options;
    
    if (!item || typeof item !== 'object') return item;
    
    return {
        ...item,
        name: safeTruncateText(item.name || '', nameLength),
        description: safeTruncateText(item.description || '', descriptionLength),
        urn: safeTruncateText(item.urn || '', urnLength),
        // Keep original values for tooltips and modals
        _original: {
            name: item.name,
            description: item.description,
            urn: item.urn
        }
    };
}

/**
 * Format text for display in table cells with proper truncation and escaping
 * @param {string} text - The text to format
 * @param {number} maxLength - Maximum display length
 * @param {string} originalText - Original text for tooltip
 * @returns {string} - Formatted HTML
 */
function formatDisplayText(text, maxLength = 150, originalText = null) {
    const displayText = safeTruncateText(text || '', maxLength);
    const tooltipText = originalText || text || '';
    
    return `<span title="${safeEscapeHtml(tooltipText)}">${safeEscapeHtml(displayText)}</span>`;
}

/**
 * Handle API response data safely, sanitizing long descriptions and problematic content
 * @param {object} response - The API response
 * @returns {object} - Sanitized response
 */
function sanitizeApiResponse(response) {
    if (!response || typeof response !== 'object') return response;
    
    // Handle different response structures
    if (response.data) {
        response.data = sanitizeDataItems(response.data);
    }
    
    if (response.items) {
        response.items = sanitizeDataItems(response.items);
    }
    
    return response;
}

/**
 * Sanitize data items (arrays or objects containing items)
 * @param {any} data - The data to sanitize
 * @returns {any} - Sanitized data
 */
function sanitizeDataItems(data) {
    if (Array.isArray(data)) {
        return data.map(item => sanitizeDataItem(item));
    }
    
    if (data && typeof data === 'object') {
        const sanitized = { ...data };
        
        // Handle common data structure patterns
        ['synced_items', 'local_only_items', 'remote_only_items', 'items'].forEach(key => {
            if (Array.isArray(sanitized[key])) {
                sanitized[key] = sanitized[key].map(item => sanitizeDataItem(item));
            }
        });
        
        return sanitized;
    }
    
    return data;
}

/**
 * Sanitize individual data item
 * @param {object} item - The item to sanitize
 * @returns {object} - Sanitized item
 */
function sanitizeDataItem(item) {
    if (!item || typeof item !== 'object') return item;
    
    const sanitized = { ...item };
    
    // Sanitize common problematic fields
    if (sanitized.description && typeof sanitized.description === 'string') {
        // Keep original for modals, create truncated for display
        sanitized._originalDescription = sanitized.description;
        if (sanitized.description.length > 1000) {
            sanitized.description = safeTruncateText(sanitized.description, 500);
        }
    }
    
    // Handle nested objects
    if (sanitized.combined && typeof sanitized.combined === 'object') {
        sanitized.combined = sanitizeDataItem(sanitized.combined);
    }
    
    if (sanitized.local && typeof sanitized.local === 'object') {
        sanitized.local = sanitizeDataItem(sanitized.local);
    }
    
    if (sanitized.remote && typeof sanitized.remote === 'object') {
        sanitized.remote = sanitizeDataItem(sanitized.remote);
    }
    
    return sanitized;
}

// Export functions for use in other scripts
window.DataUtils = {
    safeTruncateText,
    safeEscapeHtml,
    sanitizeForJson,
    safeJsonStringify,
    safeJsonParse,
    createDisplaySafeItem,
    formatDisplayText,
    sanitizeApiResponse,
    sanitizeDataItems,
    sanitizeDataItem
}; 