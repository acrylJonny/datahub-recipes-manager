"""
Specific entity models for the metadata manager.
"""
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from pydantic import Field, validator
from .base import BaseEntity, EntityMetadata, EntityType


class Tag(BaseEntity):
    """Tag entity model."""
    color: Optional[str] = Field(None, pattern=r'^#[0-9A-Fa-f]{6}$')
    entity_count: Optional[int] = Field(0, ge=0)
    
    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "name": "PII",
                "description": "Personally Identifiable Information",
                "color": "#FF5733",
                "entity_count": 42
            }
        }


class Domain(BaseEntity):
    """Domain entity model."""
    entity_count: Optional[int] = Field(0, ge=0)
    parent_domain: Optional[str] = None
    sub_domains: List[str] = Field(default_factory=list)
    
    @validator('parent_domain')
    def validate_parent_domain(cls, v):
        """Validate parent domain URN."""
        if v and not v.startswith('urn:li:domain:'):
            raise ValueError('Parent domain must be a valid domain URN')
        return v
    
    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "name": "Finance",
                "description": "Financial data and analytics",
                "entity_count": 156,
                "parent_domain": "urn:li:domain:enterprise"
            }
        }


class GlossaryNode(BaseEntity):
    """Glossary node (category) entity model."""
    parent_node: Optional[str] = None
    child_nodes: List[str] = Field(default_factory=list)
    terms: List[str] = Field(default_factory=list)
    
    @validator('parent_node')
    def validate_parent_node(cls, v):
        """Validate parent node URN."""
        if v and not v.startswith('urn:li:glossaryNode:'):
            raise ValueError('Parent node must be a valid glossary node URN')
        return v
    
    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "name": "Data Quality",
                "description": "Terms related to data quality metrics",
                "parent_node": "urn:li:glossaryNode:root"
            }
        }


class GlossaryTerm(BaseEntity):
    """Glossary term entity model."""
    definition: Optional[str] = Field(None, max_length=10000)
    parent_node: Optional[str] = None
    related_terms: List[str] = Field(default_factory=list)
    entity_count: Optional[int] = Field(0, ge=0)
    
    @validator('parent_node')
    def validate_parent_node(cls, v):
        """Validate parent node URN."""
        if v and not v.startswith('urn:li:glossaryNode:'):
            raise ValueError('Parent node must be a valid glossary node URN')
        return v
    
    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "name": "Data Completeness",
                "description": "Measure of data completeness",
                "definition": "The percentage of non-null values in a dataset",
                "parent_node": "urn:li:glossaryNode:data_quality"
            }
        }


class DataProduct(BaseEntity):
    """Data product entity model."""
    domain: Optional[str] = None
    assets: List[str] = Field(default_factory=list)
    platform: Optional[str] = None
    external_url: Optional[str] = None
    
    @validator('domain')
    def validate_domain(cls, v):
        """Validate domain URN."""
        if v and not v.startswith('urn:li:domain:'):
            raise ValueError('Domain must be a valid domain URN')
        return v
    
    @validator('external_url')
    def validate_external_url(cls, v):
        """Validate external URL."""
        if v and not (v.startswith('http://') or v.startswith('https://')):
            raise ValueError('External URL must start with http:// or https://')
        return v
    
    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "name": "Customer Analytics Dashboard",
                "description": "Analytics dashboard for customer insights",
                "domain": "urn:li:domain:analytics",
                "platform": "looker",
                "external_url": "https://company.looker.com/dashboards/123"
            }
        }


class DataContract(BaseEntity):
    """Data contract entity model."""
    entity: str = Field(..., description="URN of the entity this contract applies to")
    schema_version: Optional[str] = None
    contract_type: Optional[str] = None
    status: Optional[str] = Field(None, pattern=r'^(ACTIVE|INACTIVE|DRAFT)$')
    
    @validator('entity')
    def validate_entity(cls, v):
        """Validate entity URN."""
        if not v.startswith('urn:li:'):
            raise ValueError('Entity must be a valid URN')
        return v
    
    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "name": "Customer Table Contract",
                "description": "Data contract for customer table",
                "entity": "urn:li:dataset:(urn:li:dataPlatform:postgres,customers,PROD)",
                "schema_version": "1.0",
                "status": "ACTIVE"
            }
        }


class Assertion(BaseEntity):
    """Assertion entity model."""
    entity: str = Field(..., description="URN of the entity this assertion applies to")
    assertion_type: str = Field(..., pattern=r'^(FIELD|DATASET|CUSTOM)$')
    field_path: Optional[str] = None
    operator: Optional[str] = None
    expected_value: Optional[Union[str, int, float, bool]] = None
    
    @validator('entity')
    def validate_entity(cls, v):
        """Validate entity URN."""
        if not v.startswith('urn:li:'):
            raise ValueError('Entity must be a valid URN')
        return v
    
    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "name": "Customer Email Not Null",
                "description": "Email field should not be null",
                "entity": "urn:li:dataset:(urn:li:dataPlatform:postgres,customers,PROD)",
                "assertion_type": "FIELD",
                "field_path": "email",
                "operator": "IS_NOT_NULL"
            }
        }


class Test(BaseEntity):
    """Test entity model."""
    test_type: str = Field(..., pattern=r'^(UNIT|INTEGRATION|CUSTOM)$')
    entity: Optional[str] = None
    test_suite: Optional[str] = None
    status: Optional[str] = Field(None, pattern=r'^(PASSED|FAILED|SKIPPED)$')
    last_run: Optional[datetime] = None
    
    @validator('entity')
    def validate_entity(cls, v):
        """Validate entity URN."""
        if v and not v.startswith('urn:li:'):
            raise ValueError('Entity must be a valid URN')
        return v
    
    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "name": "Customer Data Quality Test",
                "description": "Test for customer data quality",
                "test_type": "INTEGRATION",
                "entity": "urn:li:dataset:(urn:li:dataPlatform:postgres,customers,PROD)",
                "status": "PASSED"
            }
        }


class StructuredPropertyDefinition(BaseEntity):
    """Structured property definition model."""
    qualified_name: str = Field(..., description="Fully qualified name of the property")
    value_type: str = Field(..., pattern=r'^(STRING|NUMBER|BOOLEAN|DATE|URN)$')
    cardinality: str = Field(default="SINGLE", pattern=r'^(SINGLE|MULTIPLE)$')
    allowed_values: Optional[List[str]] = None
    entity_types: List[str] = Field(default_factory=list)
    
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
                "name": "Data Classification",
                "description": "Classification level of the data",
                "qualified_name": "company.data.classification",
                "value_type": "STRING",
                "cardinality": "SINGLE",
                "allowed_values": ["PUBLIC", "INTERNAL", "CONFIDENTIAL", "RESTRICTED"],
                "entity_types": ["dataset", "dataJob"]
            }
        } 