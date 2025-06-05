from django.shortcuts import render, redirect
from django.views import View
from django.contrib import messages
import json
import logging
import os
import sys

# Add project root to sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

# Import the deterministic URN utilities
from .models import SyncConfig

logger = logging.getLogger(__name__)


class SyncConfigListView(View):
    """View to list and create sync configurations"""

    def get(self, request):
        """Display list of sync configurations"""
        try:
            configs = SyncConfig.objects.all().order_by("name")
            return render(
                request,
                "metadata_manager/sync/list.html",
                {"configs": configs, "page_title": "Metadata Sync Configurations"},
            )
        except Exception as e:
            logger.error(f"Error in sync config list view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return render(
                request,
                "metadata_manager/sync/list.html",
                {"error": str(e), "page_title": "Metadata Sync Configurations"},
            )

    def post(self, request):
        """Create a new sync configuration"""
        try:
            name = request.POST.get("name")
            description = request.POST.get("description", "")
            source_environment = request.POST.get("source_environment")
            target_environment = request.POST.get("target_environment")
            entity_types = request.POST.getlist("entity_types", [])

            # Validate required fields
            if not name:
                messages.error(request, "Name is required")
                return redirect("sync_config_list")

            if not source_environment or not target_environment:
                messages.error(request, "Source and target environments are required")
                return redirect("sync_config_list")

            if not entity_types:
                messages.error(request, "At least one entity type must be selected")
                return redirect("sync_config_list")

            # Format entity types as JSON for storage
            entity_types_json = json.dumps(entity_types)

            # Create the sync configuration
            SyncConfig.objects.create(
                name=name,
                description=description,
                source_environment=source_environment,
                target_environment=target_environment,
                entity_types=entity_types_json,
            )

            messages.success(
                request, f"Sync configuration '{name}' created successfully"
            )
            return redirect("sync_config_list")
        except Exception as e:
            logger.error(f"Error creating sync config: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("sync_config_list")
