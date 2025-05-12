from django.core.management.base import BaseCommand
import os
from pathlib import Path
import uuid
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Generate a default .env file for the application'

    def add_arguments(self, parser):
        parser.add_argument(
            '--force',
            action='store_true',
            help='Overwrite existing .env file if it exists',
        )

    def handle(self, *args, **options):
        # Get base project directory
        base_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
        env_file = base_dir / '.env'
        
        # Check if file exists and force option is not provided
        if env_file.exists() and not options['force']:
            self.stdout.write(self.style.WARNING(f'.env file already exists at {env_file}'))
            self.stdout.write('Use --force to overwrite it')
            return
        
        # Generate a random Django secret key
        secret_key = str(uuid.uuid4()).replace('-', '')
        
        env_content = f"""# DataHub Recipe Manager Environment Configuration

# Application Settings
DEBUG=true
SECRET_KEY={secret_key}
ALLOWED_HOSTS=localhost,127.0.0.1
LOAD_REPOSITORY_DATA=true

# DataHub Connection
DATAHUB_GMS_URL=http://localhost:8080
DATAHUB_TOKEN=your_datahub_token_here

# Git Integration
# GITHUB_TOKEN=your_github_token_here
# GITHUB_USERNAME=your_username_or_org
# GITHUB_REPOSITORY=datahub-recipes-manager

# Database Settings (uncomment to use PostgreSQL instead of SQLite)
# DB_ENGINE=django.db.backends.postgresql
# DB_NAME=datahub_recipes
# DB_USER=postgres
# DB_PASSWORD=postgres
# DB_HOST=localhost
# DB_PORT=5432

# Logging
LOG_LEVEL=INFO
"""
        
        # Write the file
        with open(env_file, 'w') as f:
            f.write(env_content)
        
        self.stdout.write(self.style.SUCCESS(f'Generated .env file at {env_file}'))
        self.stdout.write('Remember to update the values with your actual configuration') 