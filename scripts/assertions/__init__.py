#!/usr/bin/env python3
"""
Assertion utilities for DataHub metadata testing.

This package provides tools for making assertions about metadata entities
and their relationships in DataHub.
"""

from importlib import import_module
from pathlib import Path

# Auto-import all module files
__all__ = []

# Get the directory of this package
package_dir = Path(__file__).parent

# Import all Python files in this directory except __init__.py
for py_file in package_dir.glob("*.py"):
    if py_file.name != "__init__.py":
        module_name = py_file.stem
        __all__.append(module_name)

__version__ = "1.0.0" 