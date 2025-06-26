"""
Pydantic models for the metadata manager.
"""
from .base import (
    EntityType,
    OwnerType,
    OperationType,
    BaseEntity,
    Owner,
    StructuredProperty,
    GlobalTag,
    GlossaryTerm,
    EntityMetadata,
    SyncOperation,
    StagingOperation,
    GitOperation,
    ValidationResult,
    OperationResult
)

from .entities import (
    Tag,
    Domain,
    GlossaryNode,
    GlossaryTerm as GlossaryTermEntity,
    DataProduct,
    DataContract,
    Assertion,
    Test,
    StructuredPropertyDefinition
)

__all__ = [
    # Base models
    'EntityType',
    'OwnerType', 
    'OperationType',
    'BaseEntity',
    'Owner',
    'StructuredProperty',
    'GlobalTag',
    'GlossaryTerm',
    'EntityMetadata',
    'SyncOperation',
    'StagingOperation',
    'GitOperation',
    'ValidationResult',
    'OperationResult',
    
    # Entity models
    'Tag',
    'Domain',
    'GlossaryNode',
    'GlossaryTermEntity',
    'DataProduct',
    'DataContract',
    'Assertion',
    'Test',
    'StructuredPropertyDefinition'
] 