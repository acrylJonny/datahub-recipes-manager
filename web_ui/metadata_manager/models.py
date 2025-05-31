from django.db import models
from django.utils import timezone
import uuid

class BaseMetadataModel(models.Model):
    """Base model for all metadata entities"""
    
    SYNC_STATUS_CHOICES = [
        ('SYNCED', 'Synced'),
        ('LOCAL_ONLY', 'Local Only'),
        ('REMOTE_ONLY', 'Remote Only'),
        ('MODIFIED', 'Modified'),
        ('PENDING_PUSH', 'Pending Push'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    
    # URN handling
    deterministic_urn = models.CharField(max_length=255, unique=True)
    original_urn = models.CharField(max_length=255, blank=True)
    
    # Status tracking
    datahub_id = models.CharField(max_length=255, blank=True, null=True, help_text="ID in DataHub")
    sync_status = models.CharField(max_length=20, choices=SYNC_STATUS_CHOICES, default='NOT_SYNCED')
    last_synced = models.DateTimeField(null=True, blank=True)
    
    # Environmental context
    environment = models.ForeignKey('Environment', on_delete=models.SET_NULL, null=True, blank=True)
    
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
        return self.sync_status == 'SYNCED'
        
    def mark_as_synced(self):
        """Mark this entity as synced"""
        self.sync_status = 'SYNCED'
        self.last_synced = timezone.now()
        self.save(update_fields=['sync_status', 'last_synced'])
        
    def mark_as_not_synced(self):
        """Mark this entity as not synced"""
        self.sync_status = 'NOT_SYNCED'
        self.save(update_fields=['sync_status'])

class Tag(BaseMetadataModel):
    """Model representing a DataHub tag"""
    color = models.CharField(max_length=20, default='#0d6efd', null=True, blank=True)
    
    class Meta:
        ordering = ['name']
        verbose_name = 'Tag'
        verbose_name_plural = 'Tags'
        
    def to_dict(self):
        """Convert tag to dictionary for export/syncing purposes"""
        return {
            'name': self.name,
            'description': self.description,
            'color': self.color,
            'urn': self.deterministic_urn,
            'original_urn': self.original_urn if self.original_urn else None
        }

class Environment(models.Model):
    """Model representing an environment to facilitate CI/CD processes"""
    name = models.CharField(max_length=100)
    description = models.TextField(blank=True)
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
        ordering = ['-is_default', 'name']

class GlossaryNode(BaseMetadataModel):
    """Represents a node in the DataHub Glossary"""
    parent = models.ForeignKey('self', on_delete=models.CASCADE, null=True, blank=True, related_name='children')
    color_hex = models.CharField(max_length=20, blank=True, null=True, help_text="Color in hex format (e.g. #FF5733)")
    
    class Meta:
        verbose_name = "Glossary Node"
        verbose_name_plural = "Glossary Nodes"
        
    def to_dict(self):
        """Convert glossary node to dictionary for export/syncing purposes"""
        data = {
            'name': self.name,
            'description': self.description,
            'urn': self.deterministic_urn,
            'original_urn': self.original_urn if self.original_urn else None
        }
        
        # Add color_hex if it exists
        try:
            if hasattr(self, 'color_hex') and self.color_hex:
                data['color_hex'] = self.color_hex
        except:
            pass
        
        if self.parent:
            data['parent_urn'] = self.parent.deterministic_urn
            
        return data
        
    @property
    def can_deploy(self):
        """Check if this node can be deployed to DataHub"""
        return self.sync_status in ['LOCAL_ONLY', 'MODIFIED']

class GlossaryTerm(BaseMetadataModel):
    """Represents a term in the DataHub Glossary"""
    parent_node = models.ForeignKey(GlossaryNode, on_delete=models.CASCADE, null=True, blank=True, related_name='terms')
    domain_urn = models.CharField(max_length=255, blank=True, null=True)
    term_source = models.CharField(max_length=255, blank=True, null=True)
    
    class Meta:
        verbose_name = "Glossary Term"
        verbose_name_plural = "Glossary Terms"
        
    def to_dict(self):
        """Convert glossary term to dictionary for export/syncing purposes"""
        data = {
            'name': self.name,
            'description': self.description,
            'urn': self.deterministic_urn,
            'original_urn': self.original_urn if self.original_urn else None,
            'term_source': self.term_source
        }
        
        if self.parent_node:
            data['parent_node_urn'] = self.parent_node.deterministic_urn
        
        if self.domain_urn:
            data['domain_urn'] = self.domain_urn
            
        return data

class Domain(BaseMetadataModel):
    """Represents a DataHub Domain"""
    class Meta:
        verbose_name = "Domain"
        verbose_name_plural = "Domains"
        
    def to_dict(self):
        """Convert domain to dictionary for export/syncing purposes"""
        return {
            'name': self.name,
            'description': self.description,
            'urn': self.deterministic_urn,
            'original_urn': self.original_urn if self.original_urn else None
        }
        
    @property
    def can_deploy(self):
        """Check if this domain can be deployed to DataHub"""
        return self.sync_status in ['LOCAL_ONLY', 'MODIFIED']

class Assertion(models.Model):
    """Represents a metadata assertion"""
    name = models.CharField(max_length=100)
    description = models.TextField(blank=True, null=True)
    type = models.CharField(max_length=50)
    config = models.JSONField()
    last_run = models.DateTimeField(null=True, blank=True)
    last_status = models.CharField(max_length=20, null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return self.name
    
    class Meta:
        verbose_name = "Assertion"
        verbose_name_plural = "Assertions"

class AssertionResult(models.Model):
    """Represents a result of running an assertion"""
    assertion = models.ForeignKey(Assertion, on_delete=models.CASCADE, related_name='results')
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