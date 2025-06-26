"""
Entity-specific Pydantic models for the DataHub CI/CD client.
"""
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import Field, HttpUrl, validator

from .base import BaseDataHubEntity


class Tag(BaseDataHubEntity):
    """Tag entity model."""
    color: Optional[str] = Field(None, regex=r'^#[0-9A-Fa-f]{6}$', description="Hex color code")
    entity_count: Optional[int] = Field(0, ge=0, description="Number of entities with this tag")

    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "name": "PII",
                "description": "Personally Identifiable Information",
                "color": "#FF5733",
                "entity_count": 42,
                "urn": "urn:li:tag:PII"
            }
        }


class Domain(BaseDataHubEntity):
    """Domain entity model."""
    entity_count: Optional[int] = Field(0, ge=0, description="Number of entities in this domain")
    parent_domain: Optional[str] = Field(None, description="Parent domain URN")
    sub_domains: List[str] = Field(default_factory=list, description="Child domain URNs")

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
                "description": "Financial data and analytics domain",
                "entity_count": 156,
                "parent_domain": "urn:li:domain:enterprise",
                "urn": "urn:li:domain:finance"
            }
        }


class GlossaryNode(BaseDataHubEntity):
    """Glossary node (category) entity model."""
    parent_node: Optional[str] = Field(None, description="Parent glossary node URN")
    child_nodes: List[str] = Field(default_factory=list, description="Child glossary node URNs")
    terms: List[str] = Field(default_factory=list, description="Glossary term URNs in this node")

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
                "parent_node": "urn:li:glossaryNode:root",
                "urn": "urn:li:glossaryNode:data_quality"
            }
        }


class GlossaryTerm(BaseDataHubEntity):
    """Glossary term entity model."""
    definition: Optional[str] = Field(None, max_length=10000, description="Term definition")
    parent_node: Optional[str] = Field(None, description="Parent glossary node URN")
    related_terms: List[str] = Field(default_factory=list, description="Related glossary term URNs")
    entity_count: Optional[int] = Field(0, ge=0, description="Number of entities using this term")

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
                "parent_node": "urn:li:glossaryNode:data_quality",
                "urn": "urn:li:glossaryTerm:data_completeness"
            }
        }


class DataProduct(BaseDataHubEntity):
    """Data product entity model."""
    domain: Optional[str] = Field(None, description="Domain URN")
    assets: List[str] = Field(default_factory=list, description="Asset URNs in this data product")
    platform: Optional[str] = Field(None, description="Platform name")
    external_url: Optional[HttpUrl] = Field(None, description="External URL")

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
                "name": "Customer Analytics Dashboard",
                "description": "Analytics dashboard for customer insights",
                "domain": "urn:li:domain:analytics",
                "platform": "looker",
                "external_url": "https://company.looker.com/dashboards/123",
                "urn": "urn:li:dataProduct:customer_analytics"
            }
        }


class DataContract(BaseDataHubEntity):
    """Data contract entity model."""
    entity: str = Field(..., description="URN of the entity this contract applies to")
    schema_version: Optional[str] = Field(None, description="Schema version")
    contract_type: Optional[str] = Field(None, description="Contract type")
    status: Optional[str] = Field(None, regex=r'^(ACTIVE|INACTIVE|DRAFT)$', description="Contract status")

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
                "status": "ACTIVE",
                "urn": "urn:li:dataContract:customer_table"
            }
        }


class Assertion(BaseDataHubEntity):
    """Assertion entity model."""
    entity: str = Field(..., description="URN of the entity this assertion applies to")
    assertion_type: str = Field(..., regex=r'^(FIELD|DATASET|CUSTOM)$', description="Assertion type")
    field_path: Optional[str] = Field(None, description="Field path for field assertions")
    operator: Optional[str] = Field(None, description="Assertion operator")
    expected_value: Optional[Union[str, int, float, bool]] = Field(None, description="Expected value")

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
                "operator": "IS_NOT_NULL",
                "urn": "urn:li:assertion:customer_email_not_null"
            }
        }


class Test(BaseDataHubEntity):
    """Test entity model."""
    test_type: str = Field(..., regex=r'^(UNIT|INTEGRATION|CUSTOM)$', description="Test type")
    entity: Optional[str] = Field(None, description="URN of the entity this test applies to")
    test_suite: Optional[str] = Field(None, description="Test suite URN")
    status: Optional[str] = Field(None, regex=r'^(PASSED|FAILED|SKIPPED)$', description="Test status")
    last_run: Optional[datetime] = Field(None, description="Last run timestamp")

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
                "status": "PASSED",
                "urn": "urn:li:test:customer_data_quality"
            }
        }


class StructuredPropertyDefinition(BaseDataHubEntity):
    """Structured property definition model."""
    qualified_name: str = Field(..., description="Fully qualified name of the property")
    value_type: str = Field(..., regex=r'^(STRING|NUMBER|BOOLEAN|DATE|URN)$', description="Value type")
    cardinality: str = Field(default="SINGLE", regex=r'^(SINGLE|MULTIPLE)$', description="Cardinality")
    allowed_values: Optional[List[str]] = Field(None, description="Allowed values for the property")
    entity_types: List[str] = Field(default_factory=list, description="Entity types this property applies to")

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
                "entity_types": ["dataset", "dataJob"],
                "urn": "urn:li:structuredProperty:company.data.classification"
            }
        }


class IngestionSource(BaseDataHubEntity):
    """Ingestion source entity model."""
    platform: str = Field(..., description="Platform name")
    type: str = Field(..., description="Source type")
    config: Dict[str, Any] = Field(default_factory=dict, description="Source configuration")
    schedule: Optional[str] = Field(None, description="Cron schedule")
    enabled: bool = Field(True, description="Whether source is enabled")
    last_run: Optional[datetime] = Field(None, description="Last execution timestamp")
    next_run: Optional[datetime] = Field(None, description="Next scheduled execution")

    class Config:
        """Pydantic configuration."""
        json_schema_extra = {
            "example": {
                "name": "Production PostgreSQL",
                "description": "Production database ingestion",
                "platform": "postgres",
                "type": "postgres",
                "config": {
                    "host": "prod-db.company.com",
                    "database": "production"
                },
                "schedule": "0 2 * * *",
                "enabled": True,
                "urn": "urn:li:dataHubIngestionSource:prod_postgres"
            }
        }


class IngestionExecution(BaseModel):
    """Ingestion execution model."""
    execution_id: str = Field(..., description="Execution ID")
    source_urn: str = Field(..., description="Source URN")
    status: str = Field(..., regex=r'^(RUNNING|SUCCESS|FAILURE|CANCELLED)$', description="Execution status")
    start_time: datetime = Field(..., description="Execution start time")
    end_time: Optional[datetime] = Field(None, description="Execution end time")
    duration_ms: Optional[int] = Field(None, ge=0, description="Execution duration in milliseconds")
    entities_produced: Optional[int] = Field(None, ge=0, description="Number of entities produced")
    entities_updated: Optional[int] = Field(None, ge=0, description="Number of entities updated")
    error_message: Optional[str] = Field(None, description="Error message if failed")

    class Config:
        """Pydantic configuration."""
        validate_assignment = True
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }
        json_schema_extra = {
            "example": {
                "execution_id": "exec_123456",
                "source_urn": "urn:li:dataHubIngestionSource:prod_postgres",
                "status": "SUCCESS",
                "start_time": "2024-01-15T10:00:00Z",
                "end_time": "2024-01-15T10:30:00Z",
                "duration_ms": 1800000,
                "entities_produced": 150,
                "entities_updated": 25
            }
        }
