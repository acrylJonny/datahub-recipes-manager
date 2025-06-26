"""
DataHub CI/CD Client Package.

A comprehensive Python package for DataHub operations in CI/CD environments.
Provides services, MCP builders, emitters, and utilities for metadata management.
"""

# Core components
from .core.base_client import BaseDataHubClient
from .core.connection import DataHubConnection

# Emitters
from .emitters import (
    DataHubEmitter,
    FileEmitter,
)

# MCP Builders
from .mcp_builders import (
    DataProductMCPBuilder,
    TagMCPBuilder,
    # Other builders will be added as they're implemented
)

# Services
from .services import (
    AnalyticsService,
    AssertionService,
    DataContractService,
    DataProductService,
    DomainService,
    EditedDataService,
    GlossaryService,
    GroupService,
    IngestionService,
    MetadataTestService,
    OwnershipTypeService,
    SchemaService,
    StructuredPropertiesService,
    TagService,
    UserService,
)

__version__ = "0.1.0"

__all__ = [
    # Core
    "DataHubConnection",
    "BaseDataHubClient",

    # Services
    "TagService",
    "DomainService",
    "StructuredPropertiesService",
    "GlossaryService",
    "DataProductService",
    "UserService",
    "GroupService",
    "OwnershipTypeService",
    "IngestionService",
    "EditedDataService",
    "AssertionService",
    "MetadataTestService",
    "DataContractService",
    "SchemaService",
    "AnalyticsService",

    # MCP Builders
    "DataProductMCPBuilder",
    "TagMCPBuilder",

    # Emitters
    "FileEmitter",
    "DataHubEmitter",
]
