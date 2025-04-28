from django import template

register = template.Library()

@register.filter
def dictvalues(dictionary):
    """Return the values of a dictionary."""
    return dictionary.values()

@register.filter
def selectattr(sequence, attr):
    """Filter a sequence of objects by an attribute."""
    return [item for item in sequence if item.get(attr, False)]

@register.filter
def sub(value, arg):
    """Subtract the arg from the value."""
    try:
        return int(value) - int(arg)
    except (ValueError, TypeError):
        return 0 