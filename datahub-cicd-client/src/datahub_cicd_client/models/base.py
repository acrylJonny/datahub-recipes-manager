"""
Base Pydantic models for the DataHub CI/CD client.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl, validator


class EntityType(str, Enum):
    """Supported DataHub entity types."""

    TAG = "tag"
    DOMAIN = "domain"
    GLOSSARY_NODE = "glossaryNode"
    GLOSSARY_TERM = "glossaryTerm"
    DATA_PRODUCT = "dataProduct"
    DATA_CONTRACT = "dataContract"
    ASSERTION = "assertion"
    TEST = "test"
    STRUCTURED_PROPERTY = "structuredProperty"
    DATASET = "dataset"
    DATA_JOB = "dataJob"
    DATA_FLOW = "dataFlow"
    DASHBOARD = "dashboard"
    CHART = "chart"
    CONTAINER = "container"
    INGESTION_SOURCE = "ingestionSource"


class OwnerType(str, Enum):
    """Owner types in DataHub."""

    USER = "corpuser"
    GROUP = "corpGroup"


class OperationType(str, Enum):
    """Types of operations."""

    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    UPSERT = "UPSERT"
    PATCH = "PATCH"


class GraphQLResponse(BaseModel):
    """GraphQL response model."""

    data: Optional[Dict[str, Any]] = None
    errors: Optional[List[Dict[str, Any]]] = None
    extensions: Optional[Dict[str, Any]] = None

    @property
    def has_errors(self) -> bool:
        """Check if response has errors."""
        return self.errors is not None and len(self.errors) > 0

    @property
    def error_messages(self) -> List[str]:
        """Get list of error messages."""
        if not self.has_errors:
            return []
        return [error.get("message", "Unknown error") for error in self.errors]


class DataHubConnection(BaseModel):
    """DataHub connection configuration."""

    server_url: HttpUrl = Field(..., description="DataHub GMS server URL")
    token: Optional[str] = Field(None, description="DataHub access token")
    verify_ssl: bool = Field(True, description="Verify SSL certificates")
    timeout: int = Field(30, ge=1, le=300, description="Request timeout in seconds")
    retry_count: int = Field(3, ge=0, le=10, description="Number of retries")

    class Config:
        """Pydantic configuration."""

        validate_assignment = True

    @validator("server_url")
    def validate_server_url(cls, v):
        """Validate server URL format."""
        url_str = str(v)
        if not url_str.endswith("/"):
            return f"{url_str}/"
        return url_str


class BaseDataHubEntity(BaseModel):
    """Base model for all DataHub entities."""

    urn: Optional[str] = Field(None, description="Entity URN")
    name: str = Field(..., min_length=1, max_length=500, description="Entity name")
    description: Optional[str] = Field(None, max_length=10000, description="Entity description")
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")

    class Config:
        """Pydantic configuration."""

        validate_assignment = True
        use_enum_values = True
        json_encoders = {datetime: lambda v: v.isoformat() if v else None}

    @validator("urn")
    def validate_urn(cls, v):
        """Validate URN format."""
        if v and not v.startswith("urn:li:"):
            raise ValueError('URN must start with "urn:li:"')
        return v


class ValidationResult(BaseModel):
    """Validation result model."""

    valid: bool = Field(..., description="Whether validation passed")
    errors: List[str] = Field(default_factory=list, description="Validation errors")
    warnings: List[str] = Field(default_factory=list, description="Validation warnings")
    entity_type: Optional[EntityType] = Field(None, description="Entity type validated")
    entity_name: Optional[str] = Field(None, description="Entity name validated")

    class Config:
        """Pydantic configuration."""

        use_enum_values = True

    @property
    def has_errors(self) -> bool:
        """Check if validation has errors."""
        return len(self.errors) > 0

    @property
    def has_warnings(self) -> bool:
        """Check if validation has warnings."""
        return len(self.warnings) > 0


class OperationResult(BaseModel):
    """Operation result model."""

    success: bool = Field(..., description="Whether operation succeeded")
    message: str = Field(..., description="Operation result message")
    data: Optional[Dict[str, Any]] = Field(None, description="Operation result data")
    errors: List[str] = Field(default_factory=list, description="Operation errors")
    warnings: List[str] = Field(default_factory=list, description="Operation warnings")
    operation_type: Optional[OperationType] = Field(None, description="Type of operation")
    entity_type: Optional[EntityType] = Field(None, description="Entity type operated on")
    entity_urn: Optional[str] = Field(None, description="Entity URN operated on")
    duration_ms: Optional[int] = Field(None, ge=0, description="Operation duration in milliseconds")

    class Config:
        """Pydantic configuration."""

        use_enum_values = True

    @property
    def has_errors(self) -> bool:
        """Check if operation has errors."""
        return len(self.errors) > 0

    @property
    def has_warnings(self) -> bool:
        """Check if operation has warnings."""
        return len(self.warnings) > 0


class BatchOperationResult(BaseModel):
    """Batch operation result model."""

    total_operations: int = Field(..., ge=0, description="Total number of operations")
    successful_operations: int = Field(..., ge=0, description="Number of successful operations")
    failed_operations: int = Field(..., ge=0, description="Number of failed operations")
    results: List[OperationResult] = Field(
        default_factory=list, description="Individual operation results"
    )
    duration_ms: Optional[int] = Field(
        None, ge=0, description="Total batch duration in milliseconds"
    )

    @validator("successful_operations", "failed_operations")
    def validate_operation_counts(cls, v, values):
        """Validate operation counts."""
        total = values.get("total_operations", 0)
        if v > total:
            raise ValueError("Operation count cannot exceed total operations")
        return v

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_operations == 0:
            return 0.0
        return self.successful_operations / self.total_operations

    @property
    def has_failures(self) -> bool:
        """Check if batch has any failures."""
        return self.failed_operations > 0


class PaginationInfo(BaseModel):
    """Pagination information model."""

    page: int = Field(1, ge=1, description="Current page number")
    page_size: int = Field(20, ge=1, le=1000, description="Number of items per page")
    total_items: Optional[int] = Field(None, ge=0, description="Total number of items")
    total_pages: Optional[int] = Field(None, ge=0, description="Total number of pages")
    has_next: bool = Field(False, description="Whether there is a next page")
    has_previous: bool = Field(False, description="Whether there is a previous page")

    @validator("total_pages")
    def calculate_total_pages(cls, v, values):
        """Calculate total pages if not provided."""
        if v is not None:
            return v

        total_items = values.get("total_items")
        page_size = values.get("page_size", 20)

        if total_items is not None:
            return (total_items + page_size - 1) // page_size

        return None


class SearchResult(BaseModel):
    """Search result model."""

    entities: List[Dict[str, Any]] = Field(
        default_factory=list, description="Search result entities"
    )
    pagination: PaginationInfo = Field(..., description="Pagination information")
    facets: Optional[Dict[str, List[Dict[str, Any]]]] = Field(None, description="Search facets")
    query: str = Field(..., description="Search query")
    filters: Optional[Dict[str, Any]] = Field(None, description="Applied filters")
    duration_ms: Optional[int] = Field(None, ge=0, description="Search duration in milliseconds")

    @property
    def total_results(self) -> int:
        """Get total number of results."""
        return self.pagination.total_items or 0

    @property
    def result_count(self) -> int:
        """Get current page result count."""
        return len(self.entities)
