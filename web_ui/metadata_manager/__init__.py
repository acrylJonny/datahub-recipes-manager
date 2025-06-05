"""
Metadata Manager for DataHub entities with deterministic URNs.

This app provides a web UI and API for managing DataHub metadata entities
(tags, glossary terms, domains, etc.) with deterministic URNs for consistent
synchronization across environments.
"""

__version__ = "1.0.0"
__author__ = "DataHub CI/CD Manager Team"

# Metadata Manager app
default_app_config = "metadata_manager.apps.MetadataManagerConfig"
