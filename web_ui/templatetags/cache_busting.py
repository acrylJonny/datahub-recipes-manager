"""
Template tags for cache busting functionality.
"""

import os
import time
from django import template
from django.conf import settings

register = template.Library()

def get_cache_version():
    """Get the current cache version, creating one if it doesn't exist."""
    cache_version_file = os.path.join(settings.BASE_DIR, '.cache_version')
    
    try:
        with open(cache_version_file, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        # Generate new version if file doesn't exist
        version = str(int(time.time()))
        try:
            with open(cache_version_file, 'w') as f:
                f.write(version)
        except Exception:
            # If we can't write the file, use current timestamp
            pass
        return version

@register.simple_tag
def cache_bust(static_path):
    """
    Add cache busting version parameter to a static file path.
    
    Usage in templates:
    {% load cache_busting %}
    <script src="{% cache_bust 'js/my-script.js' %}"></script>
    """
    from django.templatetags.static import static
    
    # Get the static URL
    static_url = static(static_path)
    
    # Get cache version
    version = get_cache_version()
    
    # Add version parameter
    separator = '&' if '?' in static_url else '?'
    return f"{static_url}{separator}v={version}"

@register.simple_tag
def cache_version():
    """
    Get the current cache version.
    
    Usage in templates:
    {% load cache_busting %}
    <script src="{% static 'js/my-script.js' %}?v={% cache_version %}"></script>
    """
    return get_cache_version() 