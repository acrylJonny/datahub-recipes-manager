#!/usr/bin/env python3
"""
Metadata testing utilities.

This package provides tools for testing the integrity and quality of DataHub metadata.
Tests can be defined to verify certain constraints, structures, and relationships between entities.
"""

from importlib import import_module
from pathlib import Path

# Auto-import all test modules
__all__ = []

# Get the directory of this package
package_dir = Path(__file__).parent

# Import all Python files in this directory except __init__.py
for py_file in package_dir.glob("*.py"):
    if py_file.name != "__init__.py":
        module_name = py_file.stem
        __all__.append(module_name)
