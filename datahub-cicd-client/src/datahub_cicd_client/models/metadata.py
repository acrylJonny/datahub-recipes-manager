"""
Metadata-specific Pydantic models for the DataHub CI/CD client.
"""
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, validator

from .base import OwnerType


class Owner(BaseModel):
    """Owner information model."""
    urn: str = Field(..., description="Owner URN")
    type: OwnerType = Field(..., description="Owner type")
    name: Optional[str] = Field(None, description="Owner display name")
    email: Optional[str] = Field(None, description="Owner email address")

    @validator('urn')
    def validate_urn(cls, v):
        """Validate URN format."""
        if not v.startswith('urn:li:'):
            raise ValueError('URN must start with "urn:li:"')
        return v

    @validator('email')
    def validate_email(cls, v):
        """Validate email format."""
        if v and '@' not in v:
            raise ValueError('Invalid email format')
        return v

    class Config:
        """Pydantic configuration."""
        use_enum_values = True
        json_schema_extra = {
            "example": {
                "urn": "urn:li:corpuser:john.doe",
                "type": "corpuser",
                "name": "John Doe",
                "email": "john.doe@company.com"
            }
        }


class StructuredProperty(BaseModel):
    """Structured property model."""
    qualified_name: str = Field(..., description="Fully qualified property name")
    value: Union[str, int, float, bool, List[Any], Dict[str, Any]] = Field(..., description="Property value")
    type: Optional[str] = Field(None, description="Property type")

    @validator('qualified_name')
    def validate_qualified_name(cls, v):
        """Validate qualified name format."""
        if not v or '.' not in v:
            raise ValueError('Qualified name must contain at least one dot')
        return v

    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "qualified_name": "company.data.classification",
                "value": "CONFIDENTIAL",
                "type": "STRING"
            }
        }


class GlobalTag(BaseModel):
    """Global tag model."""
    urn: str = Field(..., description="Tag URN")
    name: str = Field(..., description="Tag name")
    color: Optional[str] = Field(None, regex=r'^#[0-9A-Fa-f]{6}$', description="Tag color")

    @validator('urn')
    def validate_urn(cls, v):
        """Validate tag URN."""
        if not v.startswith('urn:li:tag:'):
            raise ValueError('Tag URN must start with "urn:li:tag:"')
        return v

    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "urn": "urn:li:tag:PII",
                "name": "PII",
                "color": "#FF5733"
            }
        }


class GlossaryTerm(BaseModel):
    """Glossary term model."""
    urn: str = Field(..., description="Glossary term URN")
    name: str = Field(..., description="Term name")
    definition: Optional[str] = Field(None, description="Term definition")

    @validator('urn')
    def validate_urn(cls, v):
        """Validate glossary term URN."""
        if not v.startswith('urn:li:glossaryTerm:'):
            raise ValueError('Glossary term URN must start with "urn:li:glossaryTerm:"')
        return v

    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "urn": "urn:li:glossaryTerm:data_quality",
                "name": "Data Quality",
                "definition": "Measure of data accuracy and completeness"
            }
        }


class EntityMetadata(BaseModel):
    """Common metadata for entities."""
    owners: List[Owner] = Field(default_factory=list, description="Entity owners")
    structured_properties: Dict[str, StructuredProperty] = Field(
        default_factory=dict,
        description="Structured properties"
    )
    global_tags: List[GlobalTag] = Field(default_factory=list, description="Global tags")
    glossary_terms: List[GlossaryTerm] = Field(default_factory=list, description="Glossary terms")
    domain: Optional[str] = Field(None, description="Domain URN")

    @validator('domain')
    def validate_domain(cls, v):
        """Validate domain URN."""
        if v and not v.startswith('urn:li:domain:'):
            raise ValueError('Domain must be a valid domain URN')
        return v

    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "owners": [
                    {
                        "urn": "urn:li:corpuser:john.doe",
                        "type": "corpuser",
                        "name": "John Doe"
                    }
                ],
                "structured_properties": {
                    "company.data.classification": {
                        "qualified_name": "company.data.classification",
                        "value": "CONFIDENTIAL",
                        "type": "STRING"
                    }
                },
                "global_tags": [
                    {
                        "urn": "urn:li:tag:PII",
                        "name": "PII",
                        "color": "#FF5733"
                    }
                ],
                "domain": "urn:li:domain:finance"
            }
        }


class Relationship(BaseModel):
    """Entity relationship model."""
    source_urn: str = Field(..., description="Source entity URN")
    target_urn: str = Field(..., description="Target entity URN")
    relationship_type: str = Field(..., description="Relationship type")
    direction: str = Field(default="OUTGOING", regex=r'^(OUTGOING|INCOMING|UNDIRECTED)$', description="Relationship direction")
    created_at: Optional[datetime] = Field(None, description="Relationship creation time")

    @validator('source_urn', 'target_urn')
    def validate_urns(cls, v):
        """Validate URN format."""
        if not v.startswith('urn:li:'):
            raise ValueError('URN must start with "urn:li:"')
        return v

    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }
        json_schema_extra = {
            "example": {
                "source_urn": "urn:li:dataset:(urn:li:dataPlatform:postgres,customers,PROD)",
                "target_urn": "urn:li:domain:customer_data",
                "relationship_type": "BelongsTo",
                "direction": "OUTGOING"
            }
        }


class SchemaField(BaseModel):
    """Schema field model."""
    field_path: str = Field(..., description="Field path")
    native_data_type: str = Field(..., description="Native data type")
    type: str = Field(..., description="DataHub field type")
    nullable: bool = Field(True, description="Whether field is nullable")
    description: Optional[str] = Field(None, description="Field description")
    tags: List[GlobalTag] = Field(default_factory=list, description="Field-level tags")
    glossary_terms: List[GlossaryTerm] = Field(default_factory=list, description="Field-level glossary terms")

    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "field_path": "customer_id",
                "native_data_type": "INTEGER",
                "type": "NUMBER",
                "nullable": False,
                "description": "Unique customer identifier",
                "tags": [
                    {
                        "urn": "urn:li:tag:PII",
                        "name": "PII"
                    }
                ]
            }
        }


class SchemaMetadata(BaseModel):
    """Schema metadata model."""
    dataset_urn: str = Field(..., description="Dataset URN")
    platform: str = Field(..., description="Platform name")
    schema_name: str = Field(..., description="Schema name")
    version: int = Field(0, ge=0, description="Schema version")
    hash: Optional[str] = Field(None, description="Schema hash")
    fields: List[SchemaField] = Field(default_factory=list, description="Schema fields")
    primary_keys: List[str] = Field(default_factory=list, description="Primary key field paths")
    foreign_keys: List[Dict[str, Any]] = Field(default_factory=list, description="Foreign key definitions")

    @validator('dataset_urn')
    def validate_dataset_urn(cls, v):
        """Validate dataset URN."""
        if not v.startswith('urn:li:dataset:'):
            raise ValueError('Dataset URN must start with "urn:li:dataset:"')
        return v

    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:postgres,customers,PROD)",
                "platform": "postgres",
                "schema_name": "customers",
                "version": 1,
                "fields": [
                    {
                        "field_path": "customer_id",
                        "native_data_type": "INTEGER",
                        "type": "NUMBER",
                        "nullable": False
                    }
                ],
                "primary_keys": ["customer_id"]
            }
        }


class LineageEdge(BaseModel):
    """Lineage edge model."""
    upstream_urn: str = Field(..., description="Upstream entity URN")
    downstream_urn: str = Field(..., description="Downstream entity URN")
    type: str = Field(default="TRANSFORMED", description="Lineage type")
    created_at: Optional[datetime] = Field(None, description="Lineage creation time")

    @validator('upstream_urn', 'downstream_urn')
    def validate_urns(cls, v):
        """Validate URN format."""
        if not v.startswith('urn:li:'):
            raise ValueError('URN must start with "urn:li:"')
        return v

    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }
        json_schema_extra = {
            "example": {
                "upstream_urn": "urn:li:dataset:(urn:li:dataPlatform:postgres,raw_customers,PROD)",
                "downstream_urn": "urn:li:dataset:(urn:li:dataPlatform:postgres,customers,PROD)",
                "type": "TRANSFORMED"
            }
        }


class UsageStatistics(BaseModel):
    """Usage statistics model."""
    entity_urn: str = Field(..., description="Entity URN")
    total_sql_queries: Optional[int] = Field(None, ge=0, description="Total SQL queries")
    unique_user_count: Optional[int] = Field(None, ge=0, description="Unique user count")
    top_sql_queries: List[str] = Field(default_factory=list, description="Top SQL queries")
    user_counts: List[Dict[str, Any]] = Field(default_factory=list, description="User activity counts")
    window_start: Optional[datetime] = Field(None, description="Statistics window start")
    window_end: Optional[datetime] = Field(None, description="Statistics window end")

    @validator('entity_urn')
    def validate_entity_urn(cls, v):
        """Validate entity URN."""
        if not v.startswith('urn:li:'):
            raise ValueError('Entity URN must start with "urn:li:"')
        return v

    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }
        json_schema_extra = {
            "example": {
                "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:postgres,customers,PROD)",
                "total_sql_queries": 1250,
                "unique_user_count": 15,
                "top_sql_queries": [
                    "SELECT * FROM customers WHERE status = 'active'"
                ]
            }
        }
