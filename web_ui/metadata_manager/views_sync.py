from django.shortcuts import render, redirect, get_object_or_404
from django.views import View
from django.contrib import messages
from django.http import JsonResponse, HttpResponse
import json
import logging
import os
import sys

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Import the deterministic URN utilities
from utils.urn_utils import generate_deterministic_urn, get_full_urn_from_name
from .models import SyncConfig

logger = logging.getLogger(__name__)

class SyncConfigListView(View):
    """View to list and create sync configurations"""
    
    def get(self, request):
        """Display list of sync configurations"""
        try:
            configs = SyncConfig.objects.all().order_by('name')
            return render(request, 'metadata_manager/sync/list.html', {
                'configs': configs,
                'page_title': 'Metadata Sync Configurations'
            })
        except Exception as e:
            logger.error(f"Error in sync config list view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return render(request, 'metadata_manager/sync/list.html', {
                'error': str(e),
                'page_title': 'Metadata Sync Configurations'
            })
    
    def post(self, request):
        """Create a new sync configuration"""
        try:
            # Implementation will be added in the future
            return redirect('sync_config_list')
        except Exception as e:
            logger.error(f"Error creating sync config: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect('sync_config_list') 