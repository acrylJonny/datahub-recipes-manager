from django.urls import path
from . import views
from . import views_tags
from . import views_glossary
from . import views_domains
from . import views_assertions
from . import views_sync
from . import views_tests
from . import views_properties
from . import views_data_contracts
from . import views_data_products

app_name = "metadata_manager"

urlpatterns = [
    # Main metadata manager dashboard
    path("", views.MetadataIndexView.as_view(), name="metadata_index"),
    # Editable Properties
    path(
        "entities/editable/", views.editable_properties_view, name="editable_properties"
    ),
    path(
        "entities/editable/list/",
        views.get_editable_entities,
        name="get_editable_entities",
    ),
    path(
        "entities/editable/progress/",
        views.get_search_progress,
        name="get_search_progress",
    ),
    path(
        "entities/editable/update/",
        views.update_entity_properties,
        name="update_entity_properties",
    ),
    path(
        "entities/editable/cache/clear/",
        views.clear_editable_entities_cache,
        name="clear_editable_entities_cache",
    ),
    path(
        "platforms/",
        views.get_platforms,
        name="get_platforms",
    ),
    path("entities/<str:urn>/", views.get_entity_details, name="get_entity_details"),
    path(
        "entities/<str:urn>/schema/", views.get_entity_schema, name="get_entity_schema"
    ),
    path("sync/", views.sync_metadata, name="sync_metadata"),
    path("config/datahub-url/", views.get_datahub_url_config, name="get_datahub_url_config"),
    path("structured-properties/", views.get_structured_properties, name="get_structured_properties"),
    # Structured Properties
    path(
        "properties/", views_properties.PropertyListView.as_view(), name="property_list"
    ),
    path(
        "properties/data/", views_properties.PropertyListView.as_view(), name="properties_data"
    ),
    path(
        "properties/<uuid:property_id>/",
        views_properties.PropertyDetailView.as_view(),
        name="property_detail",
    ),
    path(
        "properties/<uuid:property_id>/delete/",
        views_properties.PropertyDetailView.as_view(),
        name="property_delete",
    ),
    path(
        "properties/<uuid:property_id>/deploy/",
        views_properties.PropertyDeployView.as_view(),
        name="property_deploy",
    ),
    path(
        "properties/pull/",
        views_properties.PropertyPullView.as_view(),
        name="property_pull",
    ),
    path(
        "properties/values/",
        views_properties.PropertyValuesView.as_view(),
        name="property_values",
    ),
    # Additional property action endpoints  
    path(
        "properties/<uuid:property_id>/add-to-pr/",
        views_properties.add_property_to_pr,
        name="property_add_to_pr",
    ),
    path(
        "properties/<uuid:property_id>/resync/",
        views_properties.resync_property,
        name="property_resync",
    ),
    path(
        "properties/sync/",
        views_properties.sync_property_to_local,
        name="property_sync_to_local",
    ),
    path(
        "properties/add-remote-to-pr/",
        views_properties.add_remote_property_to_pr,
        name="property_add_remote_to_pr",
    ),
    path(
        "properties/delete-remote/",
        views_properties.delete_remote_property,
        name="property_delete_remote",
    ),
    path(
        "properties/<uuid:property_id>/stage_changes/",
        views_properties.PropertyAddToStagedChangesView.as_view(),
        name="property_add_to_staged_changes",
    ),
    path(
        "properties/remote/stage_changes/",
        views_properties.PropertyRemoteAddToStagedChangesView.as_view(),
        name="property_remote_add_to_staged_changes",
    ),
    path(
        "properties/<uuid:property_id>/download/",
        views_properties.PropertyDownloadJsonView.as_view(),
        name="property_download_json",
    ),
    # Global property action endpoints
    path(
        "properties/resync_all/",
        views_properties.resync_all_properties,
        name="property_resync_all",
    ),
    path(
        "properties/export_all/",
        views_properties.export_all_properties,
        name="property_export_all",
    ),
    path(
        "properties/add_all_to_staged_changes/",
        views_properties.PropertyAddAllToStagedChangesView.as_view(),
        name="property_add_all_to_staged_changes",
    ),
    path(
        "properties/import/",
        views_properties.import_properties,
        name="property_import",
    ),
    # Tags
    path("tags/", views_tags.TagListView.as_view(), name="tag_list"),
    path("tags/remote-data/", views_tags.get_remote_tags_data, name="get_remote_tags_data"),
    path("tags/users-groups/", views_tags.get_users_and_groups, name="get_users_and_groups"),
    
    # Common API endpoints for users, groups, and ownership types (used across multiple pages)
    path("api/users/", views_tags.get_users_and_groups, name="api_users"),
    path("api/groups/", views_tags.get_users_and_groups, name="api_groups"), 
    path("api/ownership-types/", views_tags.get_users_and_groups, name="api_ownership_types"),
    path("api/users-groups/", views_tags.get_users_and_groups, name="api_users_and_groups"),
    
    path("tags/<uuid:tag_id>/", views_tags.TagDetailView.as_view(), name="tag_detail"),
    path(
        "tags/<uuid:tag_id>/edit/", views_tags.TagDetailView.as_view(), name="tag_edit"
    ),
    path(
        "tags/<uuid:tag_id>/delete/",
        views_tags.TagDetailView.as_view(),
        name="tag_delete",
    ),
    path(
        "tags/<uuid:tag_id>/deploy/",
        views_tags.TagDeployView.as_view(),
        name="tag_deploy",
    ),
    path(
        "tags/<uuid:tag_id>/push-github/",
        views_tags.TagGitPushView.as_view(),
        name="tag_push_github",
    ),
    path(
        "tags/<uuid:tag_id>/sync_to_local/",
        views_tags.TagSyncToLocalView.as_view(),
        name="tag_sync_to_local_direct"
    ),

    path(
        "tags/pull/", views_tags.TagPullView.as_view(), name="tag_pull"
    ),  # Support both GET and POST for pulling tags
    path(
        "tags/entity/", views_tags.TagEntityView.as_view(), name="tag_entity"
    ),  # New endpoint for applying tags to entities
    # API endpoints for tag actions
    path(
        "api/tags/<str:tag_id>/sync_to_local/",
        views_tags.TagSyncToLocalView.as_view(),
        name="tag_sync_to_local_api"
    ),
    path(
        "api/tags/<str:tag_id>/download/",
        views_tags.TagDownloadJsonView.as_view(),
        name="tag_download_json_api"
    ),
    path(
        "api/tags/<str:tag_id>/stage_changes/",
        views_tags.TagAddToStagedChangesView.as_view(),
        name="tag_add_to_staged_changes_api"
    ),
    path(
        "api/tags/<str:tag_id>/delete/",
        views_tags.TagDeleteView.as_view(),
        name="tag_delete_api"
    ),
    path(
        "api/tags/<str:tag_id>/sync_to_datahub/",
        views_tags.TagSyncToDataHubView.as_view(),
        name="tag_sync_to_datahub_api"
    ),
    path(
        "api/tags/<str:tag_id>/push_to_datahub/",
        views_tags.TagPushToDataHubView.as_view(),
        name="tag_push_to_datahub_api"
    ),
    path(
        "api/tags/<str:tag_id>/resync/",
        views_tags.TagResyncView.as_view(),
        name="tag_resync_api"
    ),
    path(
        "api/tags/bulk_sync_to_datahub/",
        views_tags.TagBulkSyncToDataHubView.as_view(),
        name="tag_bulk_sync_to_datahub_api"
    ),
    # New bulk operation endpoints
    path(
        "api/tags/bulk_resync/",
        views_tags.TagBulkResyncView.as_view(),
        name="tag_bulk_resync_api"
    ),
    path(
        "api/tags/resync_all/",
        views_tags.TagResyncAllView.as_view(),
        name="tag_resync_all_api"
    ),
    path(
        "api/tags/export_all/",
        views_tags.TagExportAllView.as_view(),
        name="tag_export_all_api"
    ),
    path(
        "api/tags/import_json/",
        views_tags.TagImportJsonView.as_view(),
        name="tag_import_json_api"
    ),
    path(
        "api/tags/add_all_to_staged_changes/",
        views_tags.TagAddAllToStagedChangesView.as_view(),
        name="tag_add_all_to_staged_changes_api"
    ),
    path(
        "api/tags/delete_remote/",
        views_tags.TagDeleteRemoteView.as_view(),
        name="tag_delete_remote_api"
    ),
    # Glossary
    path("glossary/", views_glossary.GlossaryListView.as_view(), name="glossary_list"),
    path(
        "glossary/data/",
        views_glossary.get_remote_glossary_data,
        name="get_remote_glossary_data",
    ),
    path(
        "glossary/csv-upload/",
        views_glossary.glossary_csv_upload,
        name="glossary_csv_upload",
    ),
    # API endpoint for domain search
    path(
        "api/search-domains/",
        views_glossary.search_domains,
        name="search_domains",
    ),
    path(
        "glossary/pull/",
        views_glossary.GlossaryPullView.as_view(),
        name="glossary_pull",
    ),

    # Glossary Nodes
    path(
        "glossary/nodes/create/",
        views_glossary.GlossaryNodeCreateView.as_view(),
        name="glossary_node_create",
    ),
    path(
        "glossary/nodes/<uuid:node_id>/",
        views_glossary.GlossaryNodeDetailView.as_view(),
        name="glossary_node_detail",
    ),
    path(
        "glossary/nodes/<uuid:node_id>/edit/",
        views_glossary.GlossaryNodeDetailView.as_view(),
        name="glossary_node_edit",
    ),
    path(
        "glossary/nodes/<uuid:node_id>/delete/",
        views_glossary.GlossaryNodeDetailView.as_view(),
        name="glossary_node_delete",
    ),
    path(
        "glossary/nodes/<uuid:node_id>/deploy/",
        views_glossary.GlossaryNodeDeployView.as_view(),
        name="glossary_node_deploy",
    ),
    path(
        "glossary/nodes/<uuid:node_id>/push-github/",
        views_glossary.GlossaryNodeGitPushView.as_view(),
        name="glossary_node_push_github",
    ),
    path(
        "glossary/nodes/<uuid:node_id>/resync/",
        views_glossary.GlossaryNodeResyncView.as_view(),
        name="glossary_node_resync",
    ),
    # Glossary Terms
    path(
        "glossary/terms/create/",
        views_glossary.GlossaryTermCreateView.as_view(),
        name="glossary_term_create",
    ),
    path(
        "glossary/terms/<uuid:term_id>/",
        views_glossary.GlossaryTermDetailView.as_view(),
        name="glossary_term_detail",
    ),
    path(
        "glossary/terms/<uuid:term_id>/edit/",
        views_glossary.GlossaryTermDetailView.as_view(),
        name="glossary_term_edit",
    ),
    path(
        "glossary/terms/<uuid:term_id>/delete/",
        views_glossary.GlossaryTermDetailView.as_view(),
        name="glossary_term_delete",
    ),
    path(
        "glossary/terms/<uuid:term_id>/deploy/",
        views_glossary.GlossaryTermDeployView.as_view(),
        name="glossary_term_deploy",
    ),
    path(
        "glossary/terms/<uuid:term_id>/push-github/",
        views_glossary.GlossaryTermGitPushView.as_view(),
        name="glossary_term_push_github",
    ),
    path(
        "glossary/terms/<uuid:term_id>/resync/",
        views_glossary.GlossaryTermResyncView.as_view(),
        name="glossary_term_resync",
    ),
    
    # Glossary Staged Changes API endpoints
    path(
        "glossary/nodes/<uuid:node_id>/stage_changes/",
        views_glossary.GlossaryNodeAddToStagedChangesView.as_view(),
        name="glossary_node_add_to_staged_changes",
    ),
    path(
        "glossary/terms/<uuid:term_id>/stage_changes/",
        views_glossary.GlossaryTermAddToStagedChangesView.as_view(),
        name="glossary_term_add_to_staged_changes",
    ),
    path(
        "glossary/remote/stage_changes/",
        views_glossary.GlossaryRemoteAddToStagedChangesView.as_view(),
        name="glossary_remote_add_to_staged_changes",
    ),

    # Domains
    path("domains/", views_domains.DomainListView.as_view(), name="domain_list"),
    path(
        "domains/data/",
        views_domains.get_remote_domains_data,
        name="get_remote_domains_data",
    ),
    path(
        "domains/create/", views_domains.DomainListView.as_view(), name="domain_create"
    ),
    path(
        "domains/<uuid:domain_id>/",
        views_domains.DomainDetailView.as_view(),
        name="domain_detail",
    ),
    path(
        "domains/<uuid:domain_id>/edit/",
        views_domains.DomainDetailView.as_view(),
        name="domain_edit",
    ),
    path(
        "domains/<uuid:domain_id>/delete/",
        views_domains.DomainDetailView.as_view(),
        name="domain_delete",
    ),
    path(
        "domains/<uuid:domain_id>/deploy/",
        views_domains.DomainDeployView.as_view(),
        name="domain_deploy",
    ),
    path(
        "domains/<uuid:domain_id>/push-github/",
        views_domains.DomainGitPushView.as_view(),
        name="domain_push_github",
    ),
    path(
        "domains/import-export/",
        views_domains.DomainImportExportView.as_view(),
        name="domain_import_export",
    ),
    path("domains/pull/", views_domains.DomainPullView.as_view(), name="domain_pull"),
    # Additional domain action endpoints
    path(
        "domains/sync/",
        views_domains.sync_domain_to_local,
        name="domain_sync_to_local",
    ),
    path(
        "domains/<uuid:domain_id>/sync/",
        views_domains.sync_domain_to_local,
        name="domain_sync",
    ),
    path(
        "domains/<uuid:domain_id>/resync/",
        views_domains.resync_domain,
        name="domain_resync",
    ),
    path(
        "domains/<uuid:domain_id>/push/",
        views_domains.push_domain_to_datahub,
        name="domain_push_to_datahub",
    ),
    path(
        "domains/<uuid:domain_id>/delete-remote/",
        views_domains.delete_remote_domain,
        name="domain_delete_remote",
    ),
    path(
        "domains/add-to-pr/",
        views_domains.add_remote_domain_to_pr,
        name="domain_add_remote_to_pr",
    ),
    path(
        "domains/<uuid:domain_id>/stage_changes/",
        views_domains.add_domain_to_staged_changes,
        name="domain_add_to_staged_changes",
    ),
    path(
        "domains/remote/stage_changes/",
        views_domains.DomainRemoteAddToStagedChangesView.as_view(),
        name="domain_remote_add_to_staged_changes",
    ),
    # Bulk domain operations
    path(
        "domains/bulk/sync-to-local/",
        views_domains.bulk_sync_domains_to_local,
        name="domain_bulk_sync_to_local",
    ),
    path(
        "domains/bulk/add-to-staged-changes/",
        views_domains.bulk_add_domains_to_staged_changes,
        name="domain_bulk_add_to_staged_changes",
    ),
    # Data Contracts
    path(
        "data-contracts/",
        views_data_contracts.DataContractListView.as_view(),
        name="data_contract_list",
    ),
    path(
        "data-contracts/data/",
        views_data_contracts.get_remote_data_contracts_data,
        name="get_remote_data_contracts_data",
    ),
    path(
        "data-contracts/sync-to-local/",
        views_data_contracts.sync_data_contract_to_local,
        name="sync_data_contract_to_local",
    ),
    path(
        "data-contracts/<uuid:contract_id>/resync/",
        views_data_contracts.resync_data_contract,
        name="resync_data_contract",
    ),
    path(
        "data-contracts/stage_changes/",
        views_data_contracts.add_data_contract_to_staged_changes,
        name="data_contract_add_to_staged_changes",
    ),
    # Data Products
    path(
        "data-products/",
        views_data_products.DataProductListView.as_view(),
        name="data_product_list",
    ),
    path(
        "data-products/create/",
        views_data_products.DataProductListView.as_view(),
        name="data_product_create",
    ),
    path(
        "data-products/data/",
        views_data_products.get_remote_data_products_data,
        name="get_remote_data_products_data",
    ),
    path(
        "data-products/<uuid:data_product_id>/",
        views_data_products.DataProductDetailView.as_view(),
        name="data_product_detail",
    ),
    path(
        "data-products/<uuid:data_product_id>/edit/",
        views_data_products.DataProductDetailView.as_view(),
        name="data_product_edit",
    ),
    path(
        "data-products/<uuid:data_product_id>/delete/",
        views_data_products.DataProductDetailView.as_view(),
        name="data_product_delete",
    ),
    path(
        "data-products/<uuid:data_product_id>/add-to-pr/",
        views_data_products.add_data_product_to_pr,
        name="add_data_product_to_pr",
    ),
    # Additional data product action endpoints
    path(
        "data-products/sync-to-local/",
        views_data_products.sync_data_product_to_local,
        name="sync_data_product_to_local",
    ),
    path(
        "data-products/<uuid:data_product_id>/sync/",
        views_data_products.sync_data_product_to_local,
        name="data_product_sync",
    ),
    path(
        "data-products/<uuid:data_product_id>/push-to-datahub/",
        views_data_products.push_data_product_to_datahub,
        name="push_data_product_to_datahub",
    ),
    path(
        "data-products/<uuid:data_product_id>/resync/",
        views_data_products.resync_data_product,
        name="resync_data_product",
    ),
    path(
        "data-products/delete-remote/",
        views_data_products.delete_remote_data_product,
        name="delete_remote_data_product",
    ),
    path(
        "data-products/add-remote-to-pr/",
        views_data_products.add_remote_data_product_to_pr,
        name="add_remote_data_product_to_pr",
    ),
    path(
        "data-products/<uuid:data_product_id>/stage_changes/",
        views_data_products.add_data_product_to_staged_changes,
        name="data_product_add_to_staged_changes",
    ),
    path(
        "data-products/remote/stage_changes/",
        views_data_products.DataProductRemoteAddToStagedChangesView.as_view(),
        name="data_product_remote_add_to_staged_changes",
    ),
    # Assertions
    path(
        "assertions/",
        views_assertions.AssertionListView.as_view(),
        name="assertion_list",
    ),
    path(
        "assertions/datahub/",
        views_assertions.get_datahub_assertions,
        name="get_datahub_assertions",
    ),
    path(
        "assertions/data/",
        views_assertions.get_remote_assertions_data,
        name="get_remote_assertions_data",
    ),
    path(
        "assertions/create/",
        views_assertions.create_datahub_assertion,
        name="assertion_create",
    ),
    path(
        "assertions/create-local/",
        views_assertions.create_local_assertion,
        name="assertion_create_local",
    ),
    path(
        "assertions/<uuid:assertion_id>/",
        views_assertions.AssertionDetailView.as_view(),
        name="assertion_detail",
    ),
    path(
        "assertions/<uuid:assertion_id>/run/",
        views_assertions.AssertionRunView.as_view(),
        name="assertion_run",
    ),
    path(
        "assertions/<uuid:assertion_id>/delete/",
        views_assertions.AssertionDeleteView.as_view(),
        name="assertion_delete",
    ),
    # New assertion action endpoints
    path(
        "assertions/run-remote/",
        views_assertions.run_remote_assertion,
        name="run_remote_assertion",
    ),
    path(
        "assertions/sync-to-local/",
        views_assertions.sync_assertion_to_local,
        name="sync_assertion_to_local",
    ),
    path(
        "assertions/<uuid:assertion_id>/push/",
        views_assertions.push_assertion_to_datahub,
        name="push_assertion_to_datahub",
    ),
    path(
        "assertions/<uuid:assertion_id>/resync/",
        views_assertions.resync_assertion,
        name="resync_assertion",
    ),
    path(
        "assertions/<uuid:assertion_id>/delete-local/",
        views_assertions.delete_local_assertion,
        name="delete_local_assertion",
    ),
    path(
        "assertions/<uuid:assertion_id>/add-to-pr/",
        views_assertions.add_assertion_to_pr,
        name="add_assertion_to_pr",
    ),
    path(
        "assertions/<uuid:assertion_id>/edit/",
        views_assertions.edit_assertion,
        name="edit_assertion",
    ),
    # Metadata Tests
    path("tests/", views_tests.TestListView.as_view(), name="tests_list"),
    path("tests/data/", views_tests.TestListView.as_view(), name="tests_data"),
    path("tests/remote-data/", views_tests.get_remote_tests_data, name="get_remote_tests_data"),
    path("tests/create/", views_tests.TestDetailView.as_view(), name="test_create"),
    path(
        "tests/<str:test_urn>/",
        views_tests.TestDetailView.as_view(),
        name="test_detail",
    ),
    path(
        "tests/<str:test_id>/delete/",
        views_tests.TestDeleteView.as_view(),
        name="test_delete",
    ),
    path(
        "tests/<str:test_urn>/export/",
        views_tests.TestExportView.as_view(),
        name="test_export",
    ),
    path(
        "tests/<str:test_urn>/push-github/",
        views_tests.TestGitPushView.as_view(),
        name="test_push_github",
    ),
    path("tests/import/", views_tests.TestImportView.as_view(), name="test_import"),
    path(
        "tests/<str:test_id>/stage_changes/",
        views_tests.TestStageChangesView.as_view(),
        name="test_stage_changes",
    ),
    path(
        "tests/remote/stage_changes/",
        views_tests.TestRemoteStageChangesView.as_view(),
        name="test_remote_stage_changes",
    ),
    # Sync
    path("sync/", views_sync.SyncConfigListView.as_view(), name="sync_config_list"),
    path(
        "sync/create/",
        views_sync.SyncConfigListView.as_view(),
        name="sync_config_create",
    ),
]

