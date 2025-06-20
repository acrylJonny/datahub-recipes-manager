# Frontend Cache Busting

This document explains the cache busting system implemented to ensure users get fresh static files when the server restarts.

## Overview

The cache busting system automatically invalidates browser caches for static files (CSS, JavaScript, images) when the Django server restarts, ensuring users always get the latest version of frontend assets.

## How It Works

1. **Cache Version Generation**: When the server starts, a unique timestamp-based version is generated and stored in `web_ui/.cache_version`
2. **Template Context**: The cache version is available in all templates via the `cache_version` context variable
3. **URL Parameters**: Static file URLs include the cache version as a query parameter (e.g., `?v=1703123456`)
4. **Automatic Invalidation**: Each server restart generates a new version, forcing browsers to fetch fresh files

## Implementation Details

### Context Processor
- `web_ui/web_ui/context_processors.py` - `cache_busting()` function
- Automatically adds `cache_version` to all template contexts
- Reads from `.cache_version` file or generates new version

### App Configuration
- `web_ui/web_ui/apps.py` - `WebUiConfig.ready()` method
- Generates new cache version on Django startup
- Called automatically when the server starts

### Management Command
- `web_ui/web_ui/management/commands/generate_cache_version.py`
- Manually generate new cache version: `python manage.py generate_cache_version`
- Also called automatically by the startup script

### Startup Script Integration
- `run_web_ui.sh` calls the management command during setup
- Ensures cache version is generated before server starts

### Template Usage
Static files in templates are automatically cache-busted:

```html
<!-- CSS Files -->
<link rel="stylesheet" href="{% static 'css/style.css' %}?v={{ cache_version }}">

<!-- JavaScript Files -->
<script src="{% static 'js/scripts.js' %}?v={{ cache_version }}"></script>

<!-- Images -->
<img src="{% static 'images/logo.png' %}?v={{ cache_version }}" alt="Logo">
```

### Template Tags (Optional)
For more control, use the custom template tags:

```html
{% load cache_busting %}

<!-- Using cache_bust tag -->
<script src="{% cache_bust 'js/my-script.js' %}"></script>

<!-- Using cache_version tag -->
<script src="{% static 'js/my-script.js' %}?v={% cache_version %}"></script>
```

## Files Affected

### Base Template
- `web_ui/templates/base.html` - All core static files use cache busting

### Specific Templates
- `web_ui/templates/metadata_manager/glossary/list.html` - Glossary enhanced JavaScript
- Any template extending base.html inherits cache busting for core files

## Configuration

### Settings
- `web_ui/web_ui/settings.py` - Context processor added to `TEMPLATES` configuration

### Git Ignore
- `.gitignore` - Excludes `web_ui/.cache_version` from version control

## Benefits

1. **Automatic Cache Invalidation**: No manual cache clearing needed
2. **Development Friendly**: New version generated on every server restart
3. **Production Ready**: Works with both development and production servers
4. **Transparent**: No changes needed to existing templates
5. **Flexible**: Custom template tags available for special cases

## Troubleshooting

### Cache Version Not Updating
- Ensure the startup script is used: `./run_web_ui.sh`
- Manually run: `python manage.py generate_cache_version`
- Check file permissions on the web_ui directory

### Template Context Missing
- Verify context processor is in settings: `web_ui.context_processors.cache_busting`
- Check for template rendering errors in Django logs

### Static Files Not Loading
- Run `python manage.py collectstatic`
- Check `STATIC_URL` and `STATIC_ROOT` settings
- Verify file paths in templates 