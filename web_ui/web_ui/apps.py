from django.apps import AppConfig
import os
import logging

logger = logging.getLogger(__name__)

class WebUiConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'web_ui'
    
    def ready(self):
        """
        Called when Django starts. Initialize settings from environment variables.
        """
        # Import here to avoid circular imports
        from .models import AppSettings, Environment
        
        # Check for .env file
        env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), '.env')
        if os.path.exists(env_path):
            try:
                # Parse .env file
                with open(env_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        
                        if '=' in line:
                            key, value = line.split('=', 1)
                            key = key.strip()
                            value = value.strip()
                            
                            # Handle quotes in values
                            if value.startswith('"') and value.endswith('"'):
                                value = value[1:-1]
                            elif value.startswith("'") and value.endswith("'"):
                                value = value[1:-1]
                            
                            # Set settings only if they don't already exist
                            if key == 'DATAHUB_GMS_URL' and not AppSettings.get('datahub_url'):
                                AppSettings.set('datahub_url', value)
                            elif key == 'DATAHUB_TOKEN' and not AppSettings.get('datahub_token'):
                                AppSettings.set('datahub_token', value)
                            elif key == 'LOAD_REPOSITORY_DATA':
                                AppSettings.set('load_repository_data', value)
                
                logger.info("Loaded settings from .env file")
            except Exception as e:
                logger.error(f"Error loading settings from .env file: {str(e)}")
        
        # Set default settings if not already set
        if not AppSettings.get('default_policy_type'):
            AppSettings.set('default_policy_type', 'METADATA')
        
        if not AppSettings.get('default_schedule'):
            AppSettings.set('default_schedule', '0 0 * * *')
        
        if not AppSettings.get('log_level'):
            AppSettings.set('log_level', 'INFO')
        
        if not AppSettings.get('timeout'):
            AppSettings.set('timeout', '30')
        
        # Initialize boolean settings with defaults if not set
        for key, default in [
            ('verify_ssl', True),
            ('validate_on_import', True),
            ('auto_backup_policies', True),
            ('auto_enable_recipes', False),
            ('debug_mode', False)
        ]:
            if AppSettings.get(key) is None:
                AppSettings.set(key, 'true' if default else 'false')
        
        # Load data from repository if requested or if there are no environments
        # This block should only run once in the initialization process
        # Skip this in management commands like migrate, shell, etc.
        import sys
        if not any(arg in sys.argv for arg in ['migrate', 'shell', 'collectstatic', 'makemigrations']):
            try:
                # Check if we should load data
                should_load = AppSettings.get_bool('load_repository_data', False)
                
                # If auto load isn't enabled, check if we have any environments
                if not should_load:
                    # Import inside try block to handle case where tables don't exist yet
                    from django.db import connection
                    env_table_exists = 'web_ui_environment' in connection.introspection.table_names()
                    if env_table_exists:
                        # Only auto-load if there are no environments
                        should_load = Environment.objects.count() == 0
                
                # Load data if needed
                if should_load:
                    logger.info("Starting repository data loader")
                    # Lazy import to avoid circular import issues
                    from .services.repo_loader import RepositoryLoader
                    results = RepositoryLoader.load_all()
                    logger.info(f"Repository data loaded: {results}")
            except Exception as e:
                logger.error(f"Error loading repository data: {str(e)}", exc_info=True) 