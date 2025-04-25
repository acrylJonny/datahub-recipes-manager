from django.urls import path
from . import views

urlpatterns = [
    path('', views.list_tests, name='list_tests'),
    path('run/', views.run_test, name='run_test'),
]
