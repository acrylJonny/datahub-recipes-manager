from django.contrib import admin
from django.urls import path, include
from django.shortcuts import redirect
from . import views

def home_redirect(request):
    return redirect('list_tests')

urlpatterns = [
    path('admin/', admin.site.urls),
    path('tests/', include('test_runner.urls')),
    path('', home_redirect, name='home'),
    path('', views.dashboard, name='dashboard'),
    path('scripts/', views.script_list, name='script_list'),
    path('scripts/history/', views.script_history, name='script_history'),
    path('scripts/run/<str:script_name>/', views.run_script, name='run_script'),
    path('scripts/result/<int:result_id>/', views.script_result, name='script_result'),
    path('scripts/export-result/<int:result_id>/', views.export_result, name='export_result'),
    path('artifacts/<int:artifact_id>/', views.view_artifact, name='view_artifact'),
    path('artifacts/<int:artifact_id>/download/', views.download_artifact, name='download_artifact'),
    path('settings/', views.connection_settings, name='settings'),
    path('templates/', views.list_templates, name='list_templates'),
    path('templates/<str:template_name>/', views.edit_template, name='edit_template'),
    path('policies/', views.list_policies, name='list_policies'),
    path('policies/<str:policy_id>/', views.edit_policy, name='edit_policy'),
    path('tests/', views.list_tests, name='list_tests'),
    path('tests/run/<str:test_name>/', views.run_test, name='run_test'),
    path('metadata/', include('metadata_manager.urls')),
]
