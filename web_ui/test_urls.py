"""
Minimal URL configuration for testing.
"""

from django.contrib import admin
from django.urls import path
from django.http import HttpResponse


def simple_view(request):
    """Simple test view."""
    return HttpResponse("Hello World")


urlpatterns = [
    path("admin/", admin.site.urls),
    path("", simple_view, name="home"),
    path("test/", simple_view, name="test"),
] 