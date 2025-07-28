"""
Minimal URL configuration for testing.
"""

from django.contrib import admin
from django.urls import path, include
from django.http import HttpResponse


def simple_view(request):
    """Simple test view."""
    return HttpResponse("Hello World")


urlpatterns = [
    path("admin/", admin.site.urls),
    path("", simple_view, name="home"),
    path("test/", simple_view, name="test"),
    # Include application URLs for testing (only for apps that have urls.py)
    path("metadata/", include("metadata_manager.urls", namespace="metadata_manager")),
    path("templates/", include("template_manager.urls", namespace="template_manager")),
] 