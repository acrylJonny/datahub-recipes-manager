"""
Data sanitization utilities for handling potentially problematic content
that could break JSON parsing or cause performance issues.
"""

import json
import re
from typing import Any, Dict, List, Union, Optional


class DataSanitizer:
    """Utility class for sanitizing data to prevent JSON parsing issues and performance problems."""
    
    # Control characters that can break JSON
    CONTROL_CHARS_PATTERN = re.compile(r'[\x00-\x1f\x7f-\x9f]')
    
    # Default limits for different types of content
    DEFAULT_LIMITS = {
        'description': 1000,  # Longer limit for backend processing
        'name': 200,
        'urn': 1000,
        'general_string': 500,
        'display_description': 200,  # Shorter limit for display
        'display_name': 100,
        'display_urn': 500,
    }
    
    @classmethod
    def sanitize_string(cls, text: str, max_length: int = None, remove_control_chars: bool = True) -> str:
        """
        Sanitize a string by removing control characters and truncating if necessary.
        
        Args:
            text: The string to sanitize
            max_length: Maximum length allowed (None for no limit)
            remove_control_chars: Whether to remove control characters
            
        Returns:
            Sanitized string
        """
        if not isinstance(text, str):
            return str(text) if text is not None else ''
        
        # Remove control characters that can break JSON
        if remove_control_chars:
            text = cls.CONTROL_CHARS_PATTERN.sub('', text)
        
        # Truncate if necessary
        if max_length and len(text) > max_length:
            # Try to break at word boundary if possible
            if max_length > 20:
                truncated = text[:max_length - 3]
                last_space = truncated.rfind(' ')
                if last_space > max_length * 0.8:  # Only use word boundary if not too far back
                    text = text[:last_space] + '...'
                else:
                    text = text[:max_length - 3] + '...'
            else:
                text = text[:max_length - 3] + '...'
        
        return text
    
    @classmethod
    def sanitize_for_json(cls, obj: Any, max_string_length: int = None) -> Any:
        """
        Recursively sanitize an object for safe JSON serialization.
        
        Args:
            obj: The object to sanitize
            max_string_length: Maximum length for string values
            
        Returns:
            Sanitized object safe for JSON serialization
        """
        if max_string_length is None:
            max_string_length = cls.DEFAULT_LIMITS['general_string']
        
        if obj is None:
            return None
        
        if isinstance(obj, str):
            return cls.sanitize_string(obj, max_string_length)
        
        if isinstance(obj, (int, float, bool)):
            return obj
        
        if isinstance(obj, list):
            return [cls.sanitize_for_json(item, max_string_length) for item in obj]
        
        if isinstance(obj, dict):
            sanitized = {}
            for key, value in obj.items():
                # Skip functions and other non-serializable types
                if callable(value):
                    continue
                
                # Sanitize key if it's a string
                safe_key = cls.sanitize_string(str(key), 100) if not isinstance(key, str) else cls.sanitize_string(key, 100)
                sanitized[safe_key] = cls.sanitize_for_json(value, max_string_length)
            
            return sanitized
        
        # For other types, try to convert to string
        try:
            return cls.sanitize_string(str(obj), max_string_length)
        except Exception:
            return f"<{type(obj).__name__} object>"
    
    @classmethod
    def sanitize_api_response(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sanitize API response data with specific handling for common data structures.
        
        Args:
            data: The API response data
            
        Returns:
            Sanitized data
        """
        if not isinstance(data, dict):
            return cls.sanitize_for_json(data)
        
        sanitized = {}
        
        for key, value in data.items():
            if key in ['synced_items', 'local_only_items', 'remote_only_items', 'items']:
                # Handle arrays of items
                if isinstance(value, list):
                    sanitized[key] = [cls.sanitize_item(item) for item in value]
                else:
                    sanitized[key] = value
            elif key == 'statistics':
                # Statistics should be safe as they're typically numbers
                sanitized[key] = value
            else:
                sanitized[key] = cls.sanitize_for_json(value)
        
        return sanitized
    
    @classmethod
    def sanitize_item(cls, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sanitize individual data items with special handling for common fields.
        
        Args:
            item: The item to sanitize
            
        Returns:
            Sanitized item
        """
        if not isinstance(item, dict):
            return cls.sanitize_for_json(item)
        
        sanitized = {}
        
        for key, value in item.items():
            if key == 'description':
                # Handle descriptions specially - keep original and create truncated version
                if isinstance(value, str) and len(value) > cls.DEFAULT_LIMITS['description']:
                    sanitized['_originalDescription'] = value
                    sanitized[key] = cls.sanitize_string(value, cls.DEFAULT_LIMITS['display_description'])
                else:
                    sanitized[key] = cls.sanitize_string(value, cls.DEFAULT_LIMITS['description']) if isinstance(value, str) else value
            
            elif key == 'name':
                sanitized[key] = cls.sanitize_string(value, cls.DEFAULT_LIMITS['name']) if isinstance(value, str) else value
            
            elif key == 'urn':
                sanitized[key] = cls.sanitize_string(value, cls.DEFAULT_LIMITS['urn']) if isinstance(value, str) else value
            
            elif key == 'properties' and isinstance(value, dict):
                # Handle nested properties
                sanitized[key] = cls.sanitize_properties(value)
            
            elif key in ['combined', 'local', 'remote'] and isinstance(value, dict):
                # Recursively handle nested objects
                sanitized[key] = cls.sanitize_item(value)
            
            elif key == 'raw_data':
                # Skip raw data to reduce payload size
                continue
            
            else:
                sanitized[key] = cls.sanitize_for_json(value)
        
        return sanitized
    
    @classmethod
    def sanitize_properties(cls, properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sanitize properties object with special handling for common property fields.
        
        Args:
            properties: The properties object to sanitize
            
        Returns:
            Sanitized properties
        """
        sanitized = {}
        
        for key, value in properties.items():
            if key == 'description':
                # Handle description in properties
                if isinstance(value, str) and len(value) > cls.DEFAULT_LIMITS['description']:
                    sanitized['_originalDescription'] = value
                    sanitized[key] = cls.sanitize_string(value, cls.DEFAULT_LIMITS['display_description'])
                else:
                    sanitized[key] = cls.sanitize_string(value, cls.DEFAULT_LIMITS['description']) if isinstance(value, str) else value
            
            elif key in ['name', 'displayName']:
                sanitized[key] = cls.sanitize_string(value, cls.DEFAULT_LIMITS['name']) if isinstance(value, str) else value
            
            else:
                sanitized[key] = cls.sanitize_for_json(value)
        
        return sanitized
    
    @classmethod
    def create_display_safe_item(cls, item: Dict[str, Any], limits: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
        """
        Create a display-safe version of an item for frontend use.
        
        Args:
            item: The original item
            limits: Custom limits for different fields
            
        Returns:
            Display-safe item with original values preserved
        """
        if not isinstance(item, dict):
            return item
        
        if limits is None:
            limits = {
                'description': cls.DEFAULT_LIMITS['display_description'],
                'name': cls.DEFAULT_LIMITS['display_name'],
                'urn': cls.DEFAULT_LIMITS['display_urn'],
            }
        
        sanitized = cls.sanitize_item(item.copy())
        
        # Ensure original values are preserved for tooltips and modals
        if '_original' not in sanitized:
            sanitized['_original'] = {
                'name': item.get('name'),
                'description': item.get('description'),
                'urn': item.get('urn'),
            }
        
        return sanitized
    
    @classmethod
    def safe_json_dumps(cls, obj: Any, max_string_length: int = None, **kwargs) -> str:
        """
        Safely serialize object to JSON string with error handling.
        
        Args:
            obj: Object to serialize
            max_string_length: Maximum length for string values
            **kwargs: Additional arguments for json.dumps
            
        Returns:
            JSON string or error representation
        """
        try:
            sanitized = cls.sanitize_for_json(obj, max_string_length)
            return json.dumps(sanitized, **kwargs)
        except Exception as e:
            return json.dumps({
                'error': 'Failed to serialize data',
                'error_type': type(e).__name__,
                'error_message': str(e),
                'original_type': type(obj).__name__
            })
    
    @classmethod
    def safe_json_loads(cls, json_str: str) -> Any:
        """
        Safely parse JSON string with error handling.
        
        Args:
            json_str: JSON string to parse
            
        Returns:
            Parsed object or None if parsing fails
        """
        try:
            return json.loads(json_str)
        except Exception as e:
            print(f"Error parsing JSON: {e}")
            return None


# Convenience functions for common use cases
def sanitize_api_response(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convenience function to sanitize API response data."""
    return DataSanitizer.sanitize_api_response(data)


def sanitize_for_display(item: Dict[str, Any], limits: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
    """Convenience function to create display-safe items."""
    return DataSanitizer.create_display_safe_item(item, limits)


def safe_json_response(obj: Any, max_string_length: int = None) -> str:
    """Convenience function for safe JSON serialization in API responses."""
    return DataSanitizer.safe_json_dumps(obj, max_string_length) 