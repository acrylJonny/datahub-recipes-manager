from django import template

register = template.Library()

@register.filter
def get_item(dictionary, key):
    """Gets a value from a dictionary by key.
    
    Usage in template:
    {{ my_dict|get_item:key_var }}
    """
    if dictionary is None:
        return None
    return dictionary.get(key)

@register.filter
def selectattr(items, attr_name):
    """Select items from a list that have a specific attribute value as truthy.
    
    Usage in template:
    {{ my_list|selectattr:"is_secret" }}
    """
    if items is None:
        return []
    result = []
    for item in items:
        if isinstance(item, dict) and item.get(attr_name):
            result.append(item)
    return result

@register.filter
def sub(value, arg):
    """Subtract the arg from the value.
    
    Usage in template:
    {{ 5|sub:2 }}  # Returns 3
    {{ total|sub:count }}
    """
    try:
        return int(value) - int(arg)
    except (ValueError, TypeError):
        return value 