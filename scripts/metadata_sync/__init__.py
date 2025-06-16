#!/usr/bin/env python3
"""
Metadata synchronization utilities.

This package provides tools for syncing metadata between different DataHub environments,
suitable for CICD workflows.
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
