from django.db import models
from django.utils import timezone
import uuid
from django.contrib.sessions.models import Session
import json


class BaseMetadataModel(models.Model):
    """Base model for all metadata entities"""

    SYNC_STATUS_CHOICES = [
        ("NOT_SYNCED", "Not Synced"),
        ("SYNCED", "Synced"),
        ("LOCAL_ONLY", "Local Only"),
        ("REMOTE_ONLY", "Remote Only"),
        ("MODIFIED", "Modified"),
        ("PENDING_PUSH", "Pending Push"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)

    # URN handling
    deterministic_urn = models.CharField(max_length=255, unique=True)
    original_urn = models.CharField(max_length=255, blank=True, null=True)

    # Status tracking
    datahub_id = models.CharField(
        max_length=255, blank=True, null=True, help_text="ID in DataHub"
    )
    sync_status = models.CharField(
        max_length=20, choices=SYNC_STATUS_CHOICES, default="NOT_SYNCED"
    )
    last_synced = models.DateTimeField(null=True, blank=True)

    # Environmental context
    environment = models.ForeignKey(
        "Environment", on_delete=models.SET_NULL, null=True, blank=True
    )

    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True

    def __str__(self):
        return self.name

    @property
    def is_synced(self):
        """Check if this entity is synced between local and remote"""
        return self.sync_status == "SYNCED"

    def mark_as_synced(self):
        """Mark this entity as synced"""
        self.sync_status = "SYNCED"
        self.last_synced = timezone.now()
        self.save(update_fields=["sync_status", "last_synced"])

    def mark_as_not_synced(self):
        """Mark this entity as not synced"""
        self.sync_status = "NOT_SYNCED"
        self.save(update_fields=["sync_status"])


class Tag(BaseMetadataModel):
    """Model representing a DataHub tag"""

    color = models.CharField(max_length=20, default="#0d6efd", null=True, blank=True)
    
    # Store ownership data
    ownership_data = models.JSONField(blank=True, null=True)
    owners_count = models.IntegerField(default=0)

    class Meta:
        ordering = ["name"]
        verbose_name = "Tag"
        verbose_name_plural = "Tags"

    def to_dict(self):
        """Convert tag to dictionary for export/syncing purposes"""
        data = {
            "name": self.name,
            "description": self.description,
            "color": self.color,
            "urn": self.deterministic_urn,
            "original_urn": self.original_urn if self.original_urn else None,
        }
        
        # Add ownership information if available
        if self.ownership_data:
            data["ownership_data"] = self.ownership_data
            data["owners_count"] = self.owners_count
        
        return data


class Environment(models.Model):
    """Model representing an environment to facilitate CI/CD processes"""

    name = models.CharField(max_length=100)
    description = models.TextField(blank=True, null=True)
    is_default = models.BooleanField(default=False)

    # Config
    datahub_url = models.URLField(blank=True)
    datahub_token = models.CharField(max_length=255, blank=True)

    # Git config
    git_branch = models.CharField(max_length=100, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    class Meta:
        ordering = ["-is_default", "name"]


class GlossaryNode(BaseMetadataModel):
    """Represents a node in the DataHub Glossary"""

    parent = models.ForeignKey(
        "self", on_delete=models.CASCADE, null=True, blank=True, related_name="children"
    )
    color_hex = models.CharField(
        max_length=20,
        blank=True,
        null=True,
        help_text="Color in hex format (e.g. #FF5733)",
    )

    class Meta:
        verbose_name = "Glossary Node"
        verbose_name_plural = "Glossary Nodes"

    def to_dict(self):
        """Convert glossary node to dictionary for export/syncing purposes"""
        data = {
            "name": self.name,
            "description": self.description,
            "urn": self.deterministic_urn,
            "original_urn": self.original_urn if self.original_urn else None,
        }

        # Add color_hex if it exists
        try:
            if hasattr(self, "color_hex") and self.color_hex:
                data["color_hex"] = self.color_hex
        except:
            pass

        if self.parent:
            data["parent_urn"] = self.parent.deterministic_urn

        return data

    @property
    def can_deploy(self):
        """Check if this node can be deployed to DataHub"""
        return self.sync_status in ["LOCAL_ONLY", "MODIFIED"]


class GlossaryTerm(BaseMetadataModel):
    """Represents a term in the DataHub Glossary"""

    parent_node = models.ForeignKey(
        GlossaryNode,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="terms",
    )
    domain_urn = models.CharField(max_length=255, blank=True, null=True)
    term_source = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        verbose_name = "Glossary Term"
        verbose_name_plural = "Glossary Terms"

    def to_dict(self):
        """Convert glossary term to dictionary for export/syncing purposes"""
        data = {
            "name": self.name,
            "description": self.description,
            "urn": self.deterministic_urn,
            "original_urn": self.original_urn if self.original_urn else None,
            "term_source": self.term_source,
        }

        if self.parent_node:
            data["parent_node_urn"] = self.parent_node.deterministic_urn

        if self.domain_urn:
            data["domain_urn"] = self.domain_urn

        return data


class Domain(BaseMetadataModel):
    """Represents a DataHub Domain"""

    # Parent domain relationship
    parent_domain_urn = models.CharField(max_length=255, blank=True, null=True)
    
    # Display properties from DataHub
    color_hex = models.CharField(
        max_length=20,
        blank=True,
        null=True,
        help_text="Color in hex format (e.g. #FF5733)",
    )
    icon_name = models.CharField(max_length=100, blank=True, null=True)
    icon_style = models.CharField(max_length=50, blank=True, null=True, default="solid")
    icon_library = models.CharField(max_length=50, blank=True, null=True, default="font-awesome")
    
    # Ownership and relationship counts for quick access
    owners_count = models.IntegerField(default=0)
    relationships_count = models.IntegerField(default=0)
    entities_count = models.IntegerField(default=0)
    
    # Store raw GraphQL data for comprehensive details
    raw_data = models.JSONField(blank=True, null=True)

    class Meta:
        verbose_name = "Domain"
        verbose_name_plural = "Domains"

    def to_dict(self):
        """Convert domain to dictionary for export/syncing purposes"""
        data = {
            "id": str(self.id),  # Include the ID for frontend action buttons
            "name": self.name,
            "description": self.description,
            "urn": self.deterministic_urn,
            "original_urn": self.original_urn if self.original_urn else None,
        }
        
        # Add parent domain if exists
        if self.parent_domain_urn:
            data["parentDomains"] = {"domains": [{"urn": self.parent_domain_urn}]}
        
        # Add display properties if they exist
        if self.color_hex or self.icon_name:
            display_props = {}
            if self.color_hex:
                display_props["colorHex"] = self.color_hex
            if self.icon_name:
                display_props["icon"] = {
                    "name": self.icon_name,
                    "style": self.icon_style or "solid",
                    "iconLibrary": self.icon_library or "font-awesome",
                }
            data["displayProperties"] = display_props
        
        return data

    @property
    def can_deploy(self):
        """Check if this domain can be deployed to DataHub"""
        return self.sync_status in ["LOCAL_ONLY", "MODIFIED"]

    @property
    def display_icon_html(self):
        """Get HTML for displaying the icon"""
        if self.icon_name:
            library_prefix = "fas" if self.icon_library == "font-awesome" else ""
            return f'<i class="{library_prefix} fa-{self.icon_name}"></i>'
        return ""

    @property
    def display_color_style(self):
        """Get CSS style for the color"""
        if self.color_hex:
            return f"color: {self.color_hex};"
        return ""


class Assertion(BaseMetadataModel):
    """Represents a metadata assertion from DataHub"""

    # Assertion type - field, sql, volume, freshness, dataset, schema, custom
    assertion_type = models.CharField(max_length=50, default="UNKNOWN")
    
    # Entity that this assertion is attached to
    entity_urn = models.CharField(max_length=500, blank=True, null=True)
    
    # Platform information
    platform_name = models.CharField(max_length=100, blank=True, null=True)
    
    # External URL for assertion
    external_url = models.URLField(blank=True, null=True)
    
    # Status information
    removed = models.BooleanField(default=False)
    
    # Store comprehensive assertion data structure from DataHub
    info_data = models.JSONField(blank=True, null=True, help_text="Complete info structure from DataHub")
    
    # Store ownership data
    ownership_data = models.JSONField(blank=True, null=True)
    owners_count = models.IntegerField(default=0)
    
    # Store relationships data
    relationships_data = models.JSONField(blank=True, null=True)
    relationships_count = models.IntegerField(default=0)
    
    # Store run events data
    run_events_data = models.JSONField(blank=True, null=True)
    
    # Store tags data
    tags_data = models.JSONField(blank=True, null=True)
    
    # Store monitor data
    monitor_data = models.JSONField(blank=True, null=True)
    
    # Legacy fields for backward compatibility
    type = models.CharField(max_length=50, blank=True, null=True)  # Keep for backward compatibility
    config = models.JSONField(blank=True, null=True)  # Keep for backward compatibility
    last_run = models.DateTimeField(null=True, blank=True)
    last_status = models.CharField(max_length=20, null=True, blank=True)

    class Meta:
        verbose_name = "Assertion"
        verbose_name_plural = "Assertions"
        ordering = ["name"]

    def to_dict(self):
        """Convert assertion to dictionary for export/syncing purposes"""
        data = {
            "id": str(self.id),  # Include the ID for frontend action buttons
            "name": self.name,
            "description": self.description,
            "urn": self.deterministic_urn,
            "original_urn": self.original_urn if self.original_urn else None,
            "assertion_type": self.assertion_type,
            "entity_urn": self.entity_urn,
            "platform_name": self.platform_name,
            "external_url": self.external_url,
            "removed": self.removed,
            "sync_status": self.sync_status,
            "sync_status_display": self.get_sync_status_display(),
        }
        
        # Add comprehensive data structures if they exist
        if self.info_data:
            data["info"] = self.info_data
        if self.ownership_data:
            data["ownership"] = self.ownership_data
        if self.relationships_data:
            data["relationships"] = self.relationships_data
        if self.run_events_data:
            data["runEvents"] = self.run_events_data
        if self.tags_data:
            data["tags"] = self.tags_data
        if self.monitor_data:
            data["monitor"] = self.monitor_data
            
        return data

    @property
    def can_deploy(self):
        """Check if this assertion can be deployed to DataHub"""
        return self.sync_status in ["LOCAL_ONLY", "MODIFIED"]
    
    @property 
    def display_type(self):
        """Get user-friendly display type"""
        return self.assertion_type.replace("_", " ").title() if self.assertion_type else "Unknown"
    
    @property
    def display_entity(self):
        """Get user-friendly entity display"""
        if self.entity_urn:
            # Extract readable part from URN
            parts = self.entity_urn.split(":")
            if len(parts) >= 3:
                return parts[-1]  # Get the last part which is usually the table/dataset name
        return "Unknown Entity"


class AssertionResult(models.Model):
    """Represents a result of running an assertion"""

    assertion = models.ForeignKey(
        Assertion, on_delete=models.CASCADE, related_name="results"
    )
    run_at = models.DateTimeField(default=timezone.now)
    status = models.CharField(max_length=20)
    details = models.JSONField(null=True, blank=True)

    def __str__(self):
        return f"{self.assertion.name} - {self.run_at}"

    class Meta:
        verbose_name = "Assertion Result"
        verbose_name_plural = "Assertion Results"


class SyncConfig(models.Model):
    """Configuration for metadata synchronization between environments"""

    name = models.CharField(max_length=100)
    description = models.TextField(blank=True, null=True)
    source_environment = models.CharField(max_length=50)
    target_environment = models.CharField(max_length=50)
    entity_types = models.JSONField()
    last_run = models.DateTimeField(null=True, blank=True)
    last_status = models.CharField(max_length=20, null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    class Meta:
        verbose_name = "Sync Configuration"
        verbose_name_plural = "Sync Configurations"


class StructuredProperty(BaseMetadataModel):
    """Represents a DataHub Structured Property"""

    qualified_name = models.CharField(max_length=255, unique=True)
    value_type = models.CharField(max_length=50, default="STRING")
    cardinality = models.CharField(max_length=50, default="SINGLE")
    immutable = models.BooleanField(default=False)
    entity_types = models.JSONField(default=list)
    allowed_values = models.JSONField(default=list, blank=True, null=True)

    # Display settings
    show_in_search_filters = models.BooleanField(default=True)
    show_as_asset_badge = models.BooleanField(default=True)
    show_in_asset_summary = models.BooleanField(default=True)
    show_in_columns_table = models.BooleanField(default=False)
    is_hidden = models.BooleanField(default=False)

    class Meta:
        verbose_name = "Structured Property"
        verbose_name_plural = "Structured Properties"
        ordering = ["name"]

    def to_dict(self):
        """Convert structured property to dictionary for export/syncing purposes"""
        return {
            "name": self.name,
            "description": self.description,
            "urn": self.deterministic_urn,
            "original_urn": self.original_urn if self.original_urn else None,
            "qualified_name": self.qualified_name,
            "value_type": self.value_type,
            "cardinality": self.cardinality,
            "immutable": self.immutable,
            "entity_types": self.entity_types,
            "allowed_values": self.allowed_values,
            "settings": {
                "show_in_search_filters": self.show_in_search_filters,
                "show_as_asset_badge": self.show_as_asset_badge,
                "show_in_asset_summary": self.show_in_asset_summary,
                "show_in_columns_table": self.show_in_columns_table,
                "is_hidden": self.is_hidden,
            },
        }

    @property
    def can_deploy(self):
        """Check if this property can be deployed to DataHub"""
        return self.sync_status in ["LOCAL_ONLY", "MODIFIED"]


class DataProduct(BaseMetadataModel):
    """Represents a DataHub data product"""

    # Data product specific fields
    external_url = models.URLField(blank=True, null=True)
    
    # Entity relationships - for data products this would be assets/datasets
    entity_urns = models.JSONField(default=list, blank=True, help_text="List of entity URNs that belong to this data product")
    
    # Domain information
    domain_urn = models.CharField(max_length=500, blank=True, null=True)
    
    # Platform information
    platform_name = models.CharField(max_length=100, blank=True, null=True)
    
    # Status information
    removed = models.BooleanField(default=False)
    
    # Store comprehensive data product data structure from DataHub
    properties_data = models.JSONField(blank=True, null=True, help_text="Complete properties structure from DataHub")
    
    # Store ownership data
    ownership_data = models.JSONField(blank=True, null=True)
    owners_count = models.IntegerField(default=0)
    
    # Store relationships data
    relationships_data = models.JSONField(blank=True, null=True)
    relationships_count = models.IntegerField(default=0)
    
    # Store entities data
    entities_data = models.JSONField(blank=True, null=True)
    entities_count = models.IntegerField(default=0)
    
    # Store tags data
    tags_data = models.JSONField(blank=True, null=True)
    
    # Store glossary terms data
    glossary_terms_data = models.JSONField(blank=True, null=True)
    
    # Store structured properties data
    structured_properties_data = models.JSONField(blank=True, null=True)
    
    # Store institutional memory data
    institutional_memory_data = models.JSONField(blank=True, null=True)

    class Meta:
        verbose_name = "Data Product"
        verbose_name_plural = "Data Products"
        ordering = ["name"]

    def to_dict(self):
        """Convert data product to dictionary for export/syncing purposes"""
        base_dict = {
            "id": str(self.id),  # Include the ID for frontend action buttons
            "name": self.name,
            "description": self.description,
            "urn": self.deterministic_urn,
            "original_urn": self.original_urn if self.original_urn else None,
            "external_url": self.external_url,
            "entity_urns": self.entity_urns,
            "domain_urn": self.domain_urn,
            "platform_name": self.platform_name,
            "properties_data": self.properties_data,
            "ownership_data": self.ownership_data,
            "relationships_data": self.relationships_data,
            "entities_data": self.entities_data,
            "tags_data": self.tags_data,
            "glossary_terms_data": self.glossary_terms_data,
            "structured_properties_data": self.structured_properties_data,
            "institutional_memory_data": self.institutional_memory_data,
            "sync_status": self.sync_status,
            "last_synced": self.last_synced.isoformat() if self.last_synced else None,
        }
        
        return base_dict

    @property
    def can_deploy(self):
        """Check if this data product can be deployed to DataHub"""
        return self.sync_status in ["LOCAL_ONLY", "MODIFIED"]

    @property 
    def display_domain(self):
        """Get display name for domain"""
        if self.domain_urn and self.properties_data:
            # Try to extract domain name from stored data
            return self.properties_data.get('domain', {}).get('domain', {}).get('properties', {}).get('name', self.domain_urn)
        return self.domain_urn or "No Domain"

    @property
    def display_entities_count(self):
        """Get formatted entities count"""
        return self.entities_count if self.entities_count > 0 else 0


class DataProductResult(models.Model):
    """Represents a result of deploying/syncing a data product"""

    data_product = models.ForeignKey(
        DataProduct, on_delete=models.CASCADE, related_name="deployment_results"
    )
    deployed_at = models.DateTimeField(default=timezone.now)
    status = models.CharField(max_length=20)
    details = models.JSONField(null=True, blank=True)

    def __str__(self):
        return f"{self.data_product.name} - {self.status} at {self.deployed_at}"

    class Meta:
        verbose_name = "Data Product Result"
        verbose_name_plural = "Data Product Results"
        ordering = ["-deployed_at"]


class DataHubUser(models.Model):
    """Cached DataHub user information for owner selection"""
    
    urn = models.CharField(max_length=500, unique=True)
    username = models.CharField(max_length=255)
    display_name = models.CharField(max_length=255, blank=True, null=True)
    email = models.EmailField(blank=True, null=True)
    
    # Cache metadata
    last_updated = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        verbose_name = "DataHub User"
        verbose_name_plural = "DataHub Users"
        ordering = ["username"]
    
    def __str__(self):
        return self.display_name or self.username


class DataHubGroup(models.Model):
    """Cached DataHub group information for owner selection"""
    
    urn = models.CharField(max_length=500, unique=True)
    display_name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    
    # Cache metadata
    last_updated = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        verbose_name = "DataHub Group"
        verbose_name_plural = "DataHub Groups"
        ordering = ["display_name"]
    
    def __str__(self):
        return self.display_name


class DataHubOwnershipType(models.Model):
    """Cached DataHub ownership type information"""
    
    urn = models.CharField(max_length=500, unique=True)
    name = models.CharField(max_length=255)
    
    # Cache metadata
    last_updated = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        verbose_name = "DataHub Ownership Type"
        verbose_name_plural = "DataHub Ownership Types"
        ordering = ["name"]
    
    def __str__(self):
        return self.name


class Test(BaseMetadataModel):
    """Represents a DataHub metadata test"""

    # Test specific fields
    category = models.CharField(max_length=100, blank=True, null=True)
    
    # Test definition
    definition_json = models.JSONField(blank=True, null=True, help_text="Test definition in JSON format")
    yaml_definition = models.TextField(blank=True, null=True, help_text="Test definition in YAML format")
    
    # Test results data
    results_data = models.JSONField(blank=True, null=True, help_text="Test execution results")
    passing_count = models.IntegerField(default=0)
    failing_count = models.IntegerField(default=0)
    last_run_timestamp = models.BigIntegerField(blank=True, null=True, help_text="Last run timestamp in milliseconds")
    
    # Status information
    removed = models.BooleanField(default=False)
    
    class Meta:
        verbose_name = "Test"
        verbose_name_plural = "Tests"
        ordering = ["name"]

    def to_dict(self):
        """Convert test to dictionary for export/syncing purposes"""
        return {
            "id": str(self.id),  # Include the ID for frontend action buttons
            "name": self.name,
            "description": self.description,
            "category": self.category,
            "urn": self.deterministic_urn,
            "original_urn": self.original_urn if self.original_urn else None,
            "definition_json": self.definition_json,
            "yaml_definition": self.yaml_definition,
            "results": {
                "passingCount": self.passing_count,
                "failingCount": self.failing_count,
                "lastRunTimestampMillis": self.last_run_timestamp,
            } if self.passing_count > 0 or self.failing_count > 0 else None,
            "sync_status": self.sync_status,
            "last_synced": self.last_synced.isoformat() if self.last_synced else None,
        }

    @property
    def can_deploy(self):
        """Check if this test can be deployed to DataHub"""
        return self.sync_status in ["LOCAL_ONLY", "MODIFIED"]
    
    @property
    def has_results(self):
        """Check if this test has execution results"""
        return self.passing_count > 0 or self.failing_count > 0
    
    @property
    def is_failing(self):
        """Check if this test has any failing results"""
        return self.failing_count > 0
    
    @property
    def last_run(self):
        """Get last run time as datetime if available"""
        if self.last_run_timestamp:
            from datetime import datetime
            return datetime.fromtimestamp(self.last_run_timestamp / 1000)
        return None


class SearchResultCache(models.Model):
    """Cache for search results tied to user sessions"""
    session_key = models.CharField(max_length=40, db_index=True)
    cache_key = models.CharField(max_length=255, db_index=True)
    entity_urn = models.CharField(max_length=500, db_index=True)
    entity_data = models.JSONField()
    search_params = models.JSONField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        unique_together = ['session_key', 'cache_key', 'entity_urn']
        indexes = [
            models.Index(fields=['session_key', 'cache_key']),
            models.Index(fields=['created_at']),
        ]
    
    @classmethod
    def cleanup_old_entries(cls, hours=24):
        """Remove cache entries older than specified hours"""
        from django.utils import timezone
        from datetime import timedelta
        
        cutoff_time = timezone.now() - timedelta(hours=hours)
        deleted_count = cls.objects.filter(created_at__lt=cutoff_time).delete()[0]
        return deleted_count
    
    @classmethod
    def get_cached_results(cls, session_key, cache_key, start=0, count=20):
        """Get paginated cached results"""
        results = cls.objects.filter(
            session_key=session_key,
            cache_key=cache_key
        ).order_by('id')[start:start + count]
        
        return [result.entity_data for result in results]
    
    @classmethod
    def get_total_count(cls, session_key, cache_key):
        """Get total count of cached results"""
        return cls.objects.filter(
            session_key=session_key,
            cache_key=cache_key
        ).count()
    
    @classmethod
    def clear_cache(cls, session_key, cache_key=None):
        """Clear cache for session, optionally for specific cache_key"""
        query = cls.objects.filter(session_key=session_key)
        if cache_key:
            query = query.filter(cache_key=cache_key)
        return query.delete()[0]


class SearchProgress(models.Model):
    """Track search progress for real-time updates"""
    session_key = models.CharField(max_length=40, db_index=True)
    cache_key = models.CharField(max_length=255, db_index=True)
    current_step = models.CharField(max_length=200)
    current_entity_type = models.CharField(max_length=50, blank=True)
    current_platform = models.CharField(max_length=100, blank=True)
    total_combinations = models.IntegerField(default=0)
    completed_combinations = models.IntegerField(default=0)
    total_results_found = models.IntegerField(default=0)
    is_complete = models.BooleanField(default=False)
    error_message = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        unique_together = ['session_key', 'cache_key']
        indexes = [
            models.Index(fields=['session_key', 'cache_key']),
            models.Index(fields=['updated_at']),
        ]
    
    @classmethod
    def update_progress(cls, session_key, cache_key, **kwargs):
        """Update search progress"""
        progress, created = cls.objects.get_or_create(
            session_key=session_key,
            cache_key=cache_key,
            defaults=kwargs
        )
        if not created:
            for key, value in kwargs.items():
                setattr(progress, key, value)
            progress.save()
        return progress
    
    @classmethod
    def get_progress(cls, session_key, cache_key):
        """Get current search progress"""
        try:
            return cls.objects.get(session_key=session_key, cache_key=cache_key)
        except cls.DoesNotExist:
            return None
