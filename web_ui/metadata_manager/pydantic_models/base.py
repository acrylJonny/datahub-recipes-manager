"""
Base Pydantic models for the metadata manager.
"""
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum


class EntityType(str, Enum):
    """Supported entity types."""
    TAG = "tag"
    DOMAIN = "domain"
    GLOSSARY_NODE = "glossary_node"
    GLOSSARY_TERM = "glossary_term"
    DATA_PRODUCT = "data_product"
    DATA_CONTRACT = "data_contract"
    ASSERTION = "assertion"
    TEST = "test"
    STRUCTURED_PROPERTY = "structured_property"


class OwnerType(str, Enum):
    """Owner types in DataHub."""
    USER = "user"
    GROUP = "group"


class OperationType(str, Enum):
    """Types of operations."""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    SYNC = "sync"
    STAGE = "stage"


class BaseEntity(BaseModel):
    """Base model for all DataHub entities."""
    urn: Optional[str] = None
    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = Field(None, max_length=5000)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    class Config:
        """Pydantic configuration."""
        validate_assignment = True
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class Owner(BaseModel):
    """Owner information."""
    urn: str
    type: OwnerType
    name: Optional[str] = None
    email: Optional[str] = None
    
    @validator('urn')
    def validate_urn(cls, v):
        """Validate URN format."""
        if not v.startswith('urn:li:'):
            raise ValueError('URN must start with "urn:li:"')
        return v


class StructuredProperty(BaseModel):
    """Structured property definition."""
    qualified_name: str
    value: Union[str, int, float, bool, List[Any], Dict[str, Any]]
    type: Optional[str] = None
    
    @validator('qualified_name')
    def validate_qualified_name(cls, v):
        """Validate qualified name format."""
        if not v or '.' not in v:
            raise ValueError('Qualified name must contain at least one dot')
        return v


class GlobalTag(BaseModel):
    """Global tag information."""
    urn: str
    name: str
    color: Optional[str] = None
    
    @validator('color')
    def validate_color(cls, v):
        """Validate color format."""
        if v and not v.startswith('#'):
            raise ValueError('Color must be a hex color starting with #')
        return v


class GlossaryTerm(BaseModel):
    """Glossary term information."""
    urn: str
    name: str
    definition: Optional[str] = None


class EntityMetadata(BaseModel):
    """Common metadata for entities."""
    owners: List[Owner] = Field(default_factory=list)
    structured_properties: Dict[str, StructuredProperty] = Field(default_factory=dict)
    global_tags: List[GlobalTag] = Field(default_factory=list)
    glossary_terms: List[GlossaryTerm] = Field(default_factory=list)
    domain: Optional[str] = None


class SyncOperation(BaseModel):
    """Sync operation details."""
    entity_type: EntityType
    operation_type: OperationType
    entity_data: Dict[str, Any]
    environment: str
    user: str
    timestamp: datetime = Field(default_factory=datetime.now)
    success: Optional[bool] = None
    error_message: Optional[str] = None
    
    class Config:
        """Pydantic configuration."""
        use_enum_values = True


class StagingOperation(BaseModel):
    """Staging operation details."""
    entity_type: EntityType
    operation_type: OperationType
    staging_data: Dict[str, Any]
    environment: str
    user: str
    timestamp: datetime = Field(default_factory=datetime.now)
    mcp_file_path: Optional[str] = None
    validated: bool = False
    
    class Config:
        """Pydantic configuration."""
        use_enum_values = True


class GitOperation(BaseModel):
    """Git operation details."""
    entity_type: EntityType
    operation_type: OperationType
    file_path: str
    commit_message: str
    pr_title: str
    pr_body: str
    branch_name: str
    user: str
    timestamp: datetime = Field(default_factory=datetime.now)
    pr_url: Optional[str] = None
    
    class Config:
        """Pydantic configuration."""
        use_enum_values = True


class ValidationResult(BaseModel):
    """Validation result."""
    valid: bool
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    entity_type: Optional[EntityType] = None
    
    class Config:
        """Pydantic configuration."""
        use_enum_values = True


class OperationResult(BaseModel):
    """Operation result."""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    errors: List[str] = Field(default_factory=list)
    operation_type: Optional[OperationType] = None
    entity_type: Optional[EntityType] = None
    
    class Config:
        """Pydantic configuration."""
        use_enum_values = True 