"""
DataHub CI/CD Client Services Package.

This package contains all service classes for interacting with DataHub.
"""

from .analytics import AnalyticsService
from .assertions import AssertionService
from .data_contracts import DataContractService
from .data_products import DataProductService
from .domains import DomainService
from .edited_data import EditedDataService
from .glossary import GlossaryService
from .groups import GroupService
from .ingestion import IngestionService
from .ownership_types import OwnershipTypeService
from .properties import StructuredPropertiesService
from .schema import SchemaService
from .tags import TagService
from .tests import MetadataTestService
from .users import UserService

__all__ = [
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
]
