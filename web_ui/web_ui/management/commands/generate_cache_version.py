"""
Django management command to generate a new cache version.
This command should be run on server startup to invalidate frontend caches.
"""

import os
import time
from django.core.management.base import BaseCommand
from django.conf import settings


class Command(BaseCommand):
    help = 'Generate a new cache version to bust frontend caches'

    def handle(self, *args, **options):
        cache_version_file = os.path.join(settings.BASE_DIR, '.cache_version')
        
        # Generate new version based on current timestamp
        version = str(int(time.time()))
        
        try:
            with open(cache_version_file, 'w') as f:
                f.write(version)
            
            self.stdout.write(
                self.style.SUCCESS(f'Successfully generated new cache version: {version}')
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error generating cache version: {e}')
            ) 