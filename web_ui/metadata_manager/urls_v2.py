"""
New URL patterns using the refactored architecture.
This file will gradually replace the existing urls.py as we migrate each entity type.
"""
from django.urls import path
from . import views  # Keep existing views for non-migrated entities

# Import new entity views
from .entities.tags import views as tag_views
from .entities.domains import views as domain_views
from .entities.glossary import views as glossary_views

app_name = "metadata_manager"

urlpatterns = [
    # Main metadata manager dashboard (keep existing)
    path("", views.MetadataIndexView.as_view(), name="metadata_index"),
    
    # ========================================
    # MIGRATED ENTITIES (New Architecture)
    # ========================================
    
    # Tags - New Architecture
    path("tags/", tag_views.tags_index, name="tag_list"),
    path("tags/sync/", tag_views.sync_tags, name="sync_tags"),
    path("tags/remote-data/", tag_views.get_remote_tags_data, name="get_remote_tags_data"),
    path("tags/stage-changes/", tag_views.stage_tag_changes, name="stage_tag_changes"),
    path("tags/confirm-staging/", tag_views.confirm_tag_staging, name="confirm_tag_staging"),
    path("tags/create-pr/", tag_views.create_tag_pr, name="create_tag_pr"),
    path("tags/validate/", tag_views.validate_tag_data, name="validate_tag_data"),
    path("tags/<str:tag_name>/detail/", tag_views.tag_detail, name="tag_detail"),
    
    # Domains - New Architecture  
    path("domains/", domain_views.domains_index, name="domain_list"),
    path("domains/sync/", domain_views.sync_domains, name="sync_domains"),
    path("domains/remote-data/", domain_views.get_remote_domains_data, name="get_remote_domains_data"),
    path("domains/stage-changes/", domain_views.stage_domain_changes, name="stage_domain_changes"),
    path("domains/confirm-staging/", domain_views.confirm_domain_staging, name="confirm_domain_staging"),
    path("domains/create-pr/", domain_views.create_domain_pr, name="create_domain_pr"),
    path("domains/validate/", domain_views.validate_domain_data, name="validate_domain_data"),
    path("domains/<str:domain_name>/detail/", domain_views.domain_detail, name="domain_detail"),
    
    # Glossary - New Architecture (placeholder for future implementation)
    # path("glossary/", glossary_views.glossary_index, name="glossary_list"),
    # path("glossary/sync/", glossary_views.sync_glossary, name="sync_glossary"),
    
    # ========================================
    # LEGACY ENTITIES (To be migrated)
    # ========================================
    
    # Structured Properties (legacy)
    path("properties/", views.PropertyListView.as_view(), name="property_list"),
    # ... other legacy property URLs
    
    # Data Products (legacy)
    path("data-products/", views.DataProductListView.as_view(), name="data_product_list"),
    # ... other legacy data product URLs
    
    # Data Contracts (legacy)  
    path("data-contracts/", views.DataContractListView.as_view(), name="data_contract_list"),
    # ... other legacy data contract URLs
    
    # Assertions (legacy)
    path("assertions/", views.AssertionListView.as_view(), name="assertion_list"),
    # ... other legacy assertion URLs
    
    # Tests (legacy)
    path("tests/", views.TestListView.as_view(), name="test_list"),
    # ... other legacy test URLs
    
    # ========================================
    # COMMON API ENDPOINTS
    # ========================================
    
    # Common endpoints used across multiple entity types
    path("api/users-groups/", views.get_users_and_groups, name="api_users_and_groups"),
    path("api/ownership-types/", views.get_ownership_types, name="api_ownership_types"),
    path("api/platforms/", views.get_platforms, name="get_platforms"),
    path("api/structured-properties/", views.get_structured_properties, name="get_structured_properties"),
    
    # Entity details (generic)
    path("entities/<str:urn>/", views.get_entity_details, name="get_entity_details"),
    path("entities/<str:urn>/schema/", views.get_entity_schema, name="get_entity_schema"),
    
    # Global sync endpoint
    path("sync/", views.sync_metadata, name="sync_metadata"),
    
    # Configuration endpoints
    path("config/datahub-url/", views.get_datahub_url_config, name="get_datahub_url_config"),
    
    # Export/Import endpoints
    path("entities/editable/export-with-mutations/", views.export_entities_with_mutations, name="export_entities_with_mutations"),
    
    # ========================================
    # MIGRATION UTILITIES
    # ========================================
    
    # Health check endpoint to verify new architecture
    path("health/", views.health_check, name="health_check"),
    
    # Migration status endpoint
    path("migration/status/", views.migration_status, name="migration_status"),
]

# URL patterns for backward compatibility during migration
# These will redirect to new URLs with deprecation warnings
legacy_redirects = [
    # Legacy tag URLs -> New tag URLs
    path("tags/list/", tag_views.tags_index, name="tag_list_legacy"),
    path("tags/sync-tags/", tag_views.sync_tags, name="sync_tags_legacy"),
    
    # Legacy domain URLs -> New domain URLs  
    path("domains/list/", domain_views.domains_index, name="domain_list_legacy"),
    path("domains/sync-domains/", domain_views.sync_domains, name="sync_domains_legacy"),
]

# Add legacy redirects to main URL patterns
urlpatterns.extend(legacy_redirects) 