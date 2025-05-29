from django.urls import path
from . import views
from . import views_tags
from . import views_glossary
from . import views_domains
from . import views_assertions
from . import views_sync
from . import views_tests

app_name = 'metadata_manager'

urlpatterns = [
    # Main metadata manager dashboard
    path('', views.MetadataIndexView.as_view(), name='metadata_index'),

    # Editable Properties
    path('entities/editable/', views.editable_properties_view, name='editable_properties'),
    path('entities/editable/list/', views.get_editable_entities, name='get_editable_entities'),
    path('entities/<str:urn>/', views.get_entity_details, name='get_entity_details'),
    path('entities/<str:urn>/schema/', views.get_entity_schema, name='get_entity_schema'),
    path('entities/update/', views.update_entity_properties, name='update_entity_properties'),
    path('sync/', views.sync_metadata, name='sync_metadata'),

    # Tags
    path('tags/', views_tags.TagListView.as_view(), name='tag_list'),
    path('tags/<uuid:tag_id>/', views_tags.TagDetailView.as_view(), name='tag_detail'),
    path('tags/<uuid:tag_id>/edit/', views_tags.TagDetailView.as_view(), name='tag_edit'),
    path('tags/<uuid:tag_id>/delete/', views_tags.TagDetailView.as_view(), name='tag_delete'),
    path('tags/<uuid:tag_id>/deploy/', views_tags.TagDeployView.as_view(), name='tag_deploy'),
    path('tags/<uuid:tag_id>/push-github/', views_tags.TagGitPushView.as_view(), name='tag_push_github'),
    path('tags/import-export/', views_tags.TagImportExportView.as_view(), name='tag_import_export'),
    path('tags/pull/', views_tags.TagPullView.as_view(), name='tag_pull'),  # Support both GET and POST for pulling tags
    path('tags/entity/', views_tags.TagEntityView.as_view(), name='tag_entity'),  # New endpoint for applying tags to entities
    
    # Glossary
    path('glossary/', views_glossary.GlossaryListView.as_view(), name='glossary_list'),
    path('glossary/pull/', views_glossary.GlossaryPullView.as_view(), name='glossary_pull'),
    path('glossary/import-export/', views_glossary.GlossaryImportExportView.as_view(), name='glossary_import_export'),

    # Glossary Nodes
    path('glossary/nodes/create/', views_glossary.GlossaryNodeCreateView.as_view(), name='glossary_node_create'),
    path('glossary/nodes/<uuid:node_id>/', views_glossary.GlossaryNodeDetailView.as_view(), name='glossary_node_detail'),
    path('glossary/nodes/<uuid:node_id>/edit/', views_glossary.GlossaryNodeDetailView.as_view(), name='glossary_node_edit'),
    path('glossary/nodes/<uuid:node_id>/delete/', views_glossary.GlossaryNodeDetailView.as_view(), name='glossary_node_delete'),
    path('glossary/nodes/<uuid:node_id>/deploy/', views_glossary.GlossaryNodeDeployView.as_view(), name='glossary_node_deploy'),
    path('glossary/nodes/<uuid:node_id>/push-github/', views_glossary.GlossaryNodeGitPushView.as_view(), name='glossary_node_push_github'),

    # Glossary Terms
    path('glossary/terms/create/', views_glossary.GlossaryTermCreateView.as_view(), name='glossary_term_create'),
    path('glossary/terms/<uuid:term_id>/', views_glossary.GlossaryTermDetailView.as_view(), name='glossary_term_detail'),
    path('glossary/terms/<uuid:term_id>/edit/', views_glossary.GlossaryTermDetailView.as_view(), name='glossary_term_edit'),
    path('glossary/terms/<uuid:term_id>/delete/', views_glossary.GlossaryTermDetailView.as_view(), name='glossary_term_delete'),
    path('glossary/terms/<uuid:term_id>/deploy/', views_glossary.GlossaryTermDeployView.as_view(), name='glossary_term_deploy'),
    path('glossary/terms/<uuid:term_id>/push-github/', views_glossary.GlossaryTermGitPushView.as_view(), name='glossary_term_push_github'),
    
    # Domains
    path('domains/', views_domains.DomainListView.as_view(), name='domain_list'),
    path('domains/create/', views_domains.DomainListView.as_view(), name='domain_create'),
    path('domains/<uuid:domain_id>/', views_domains.DomainDetailView.as_view(), name='domain_detail'),
    path('domains/<uuid:domain_id>/edit/', views_domains.DomainDetailView.as_view(), name='domain_edit'),
    path('domains/<uuid:domain_id>/delete/', views_domains.DomainDetailView.as_view(), name='domain_delete'),
    path('domains/<uuid:domain_id>/deploy/', views_domains.DomainDeployView.as_view(), name='domain_deploy'),
    path('domains/<uuid:domain_id>/push-github/', views_domains.DomainGitPushView.as_view(), name='domain_push_github'),
    path('domains/import-export/', views_domains.DomainImportExportView.as_view(), name='domain_import_export'),
    path('domains/pull/', views_domains.DomainPullView.as_view(), name='domain_pull'),
    
    # Assertions
    path('assertions/', views_assertions.AssertionListView.as_view(), name='assertion_list'),
    path('assertions/create/', views_assertions.AssertionListView.as_view(), name='assertion_create'),
    path('assertions/<int:assertion_id>/', views_assertions.AssertionDetailView.as_view(), name='assertion_detail'),
    path('assertions/<int:assertion_id>/run/', views_assertions.AssertionRunView.as_view(), name='assertion_run'),
    path('assertions/<int:assertion_id>/delete/', views_assertions.AssertionDeleteView.as_view(), name='assertion_delete'),
    
    # Metadata Tests
    path('tests/', views_tests.TestListView.as_view(), name='tests_list'),
    path('tests/create/', views_tests.TestDetailView.as_view(), name='test_create'),
    path('tests/<str:test_urn>/', views_tests.TestDetailView.as_view(), name='test_detail'),
    path('tests/<str:test_urn>/delete/', views_tests.TestDeleteView.as_view(), name='test_delete'),
    path('tests/<str:test_urn>/export/', views_tests.TestExportView.as_view(), name='test_export'),
    path('tests/<str:test_urn>/push-github/', views_tests.TestGitPushView.as_view(), name='test_push_github'),
    path('tests/import/', views_tests.TestImportView.as_view(), name='test_import'),
    
    # Sync
    path('sync/', views_sync.SyncConfigListView.as_view(), name='sync_config_list'),
    path('sync/create/', views_sync.SyncConfigListView.as_view(), name='sync_config_create'),
] 