from django.apps import AppConfig

class MetadataManagerConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'metadata_manager'
    verbose_name = 'DataHub Metadata Manager'
    
    def ready(self):
        # Import signal handlers or perform other app initialization
        pass 