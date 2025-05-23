from django.urls import path
from . import views

app_name = 'policies'

urlpatterns = [
    path('', views.policies, name='policies'),
    path('detail/<str:policy_id>/', views.policy_view, name='policy_view'),
    path('create/', views.policy_create, name='policy_create'),
    path('import/', views.policy_import, name='policy_import'),
    path('edit/<str:policy_id>/', views.policy_edit, name='policy_edit'),
    path('delete/<str:policy_id>/', views.policy_delete, name='policy_delete'),
    path('download/<str:policy_id>/', views.policy_download, name='policy_download'),
    path('export-all/', views.export_all_policies, name='export_all_policies'),
    path('<str:policy_id>/push-github/', views.policy_push_github, name='policy_push_github'),
    path('<str:policy_id>/deploy/', views.policy_deploy, name='policy_deploy'),
] 