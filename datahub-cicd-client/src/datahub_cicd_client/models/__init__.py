"""
Pydantic models for the DataHub CI/CD client.
"""

from .base import (
    BaseDataHubEntity,
    DataHubConnection,
    EntityType,
    GraphQLResponse,
    OperationResult,
    OperationType,
    OwnerType,
    ValidationResult,
)
from .entities import (
    Assertion,
    DataContract,
    DataProduct,
    Domain,
    GlossaryNode,
    GlossaryTerm,
    IngestionExecution,
    IngestionSource,
    StructuredPropertyDefinition,
    Tag,
    Test,
)
from .metadata import EntityMetadata, GlobalTag, Owner, Relationship, StructuredProperty

__all__ = [
    # Base models
    "BaseDataHubEntity",
    "DataHubConnection",
    "GraphQLResponse",
    "EntityType",
    "OwnerType",
    "OperationType",
    "ValidationResult",
    "OperationResult",
    # Entity models
    "Tag",
    "Domain",
    "GlossaryNode",
    "GlossaryTerm",
    "DataProduct",
    "DataContract",
    "Assertion",
    "Test",
    "StructuredPropertyDefinition",
    "IngestionSource",
    "IngestionExecution",
    # Metadata models
    "Owner",
    "StructuredProperty",
    "GlobalTag",
    "EntityMetadata",
    "Relationship",
]
