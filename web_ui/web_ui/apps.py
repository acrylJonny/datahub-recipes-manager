from django.apps import AppConfig
import os
import logging
import time
from django.db import connection
from asgiref.sync import sync_to_async
import asyncio

logger = logging.getLogger(__name__)


class WebUiConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "web_ui"

    def ready(self):
        """
        Called when Django starts. Initialize settings from environment variables.
        """
        # Import here to avoid circular imports
        import sys

        # Skip all database operations during migrations
        if any(
            arg in sys.argv
            for arg in ["migrate", "shell", "collectstatic", "makemigrations"]
        ):
            return

        # For ASGI servers, run database operations in a thread
        try:
            # Check if we're in an event loop (ASGI context)
            loop = asyncio.get_running_loop()
            if loop.is_running():
                # We're in an async context, defer initialization
                import threading
                threading.Thread(target=self._initialize_settings, daemon=True).start()
                return
        except RuntimeError:
            # No event loop running, safe to run synchronously
            pass

        # Run initialization synchronously
        self._initialize_settings()
        
        # Generate new cache version on startup
        self._generate_cache_version()

    def _initialize_settings(self):
        """Initialize settings - can be called from thread or sync context"""
        # Check if tables exist before attempting to use them
        try:
            tables = connection.introspection.table_names()
            settings_table_exists = "web_ui_settings" in tables

            if not settings_table_exists:
                logger.info(
                    "Settings table does not exist yet, skipping initialization"
                )
                return

            # Now it's safe to import models
            from .models import AppSettings, Environment

            # Check for .env file
            env_path = os.path.join(
                os.path.dirname(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                ),
                ".env",
            )
            if os.path.exists(env_path):
                try:
                    # Parse .env file
                    with open(env_path, "r") as f:
                        for line in f:
                            line = line.strip()
                            if not line or line.startswith("#"):
                                continue

                            if "=" in line:
                                key, value = line.split("=", 1)
                                key = key.strip()
                                value = value.strip()

                                # Handle quotes in values
                                if value.startswith('"') and value.endswith('"'):
                                    value = value[1:-1]
                                elif value.startswith("'") and value.endswith("'"):
                                    value = value[1:-1]

                                # Set settings only if they don't already exist
                                try:
                                    if key == "DATAHUB_GMS_URL" and not AppSettings.get(
                                        "datahub_url"
                                    ):
                                        AppSettings.set("datahub_url", value)
                                    elif key == "DATAHUB_TOKEN" and not AppSettings.get(
                                        "datahub_token"
                                    ):
                                        AppSettings.set("datahub_token", value)
                                    elif key == "LOAD_REPOSITORY_DATA":
                                        AppSettings.set("load_repository_data", value)
                                except Exception as e:
                                    logger.warning(
                                        f"Could not set setting {key}: {str(e)}"
                                    )

                    logger.info("Loaded settings from .env file")
                except Exception as e:
                    logger.warning(f"Error loading settings from .env file: {str(e)}")

            # Set default settings if not already set
            try:
                if not AppSettings.get("default_policy_type"):
                    AppSettings.set("default_policy_type", "METADATA")

                if not AppSettings.get("default_schedule"):
                    AppSettings.set("default_schedule", "0 0 * * *")

                if not AppSettings.get("log_level"):
                    AppSettings.set("log_level", "INFO")

                if not AppSettings.get("timeout"):
                    AppSettings.set("timeout", "30")

                # Initialize boolean settings with defaults if not set
                for key, default in [
                    ("verify_ssl", True),
                    ("validate_on_import", True),
                    ("auto_backup_policies", True),
                    ("auto_enable_recipes", False),
                    ("debug_mode", False),
                ]:
                    try:
                        if AppSettings.get(key) is None:
                            AppSettings.set(key, "true" if default else "false")
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"Error setting default settings: {str(e)}")

            # Load data from repository if requested or if there are no environments
            # This block should only run once in the initialization process
            try:
                # Check if we should load data
                should_load = False
                try:
                    should_load = AppSettings.get_bool("load_repository_data", False)
                except Exception:
                    pass

                # If auto load isn't enabled, check if we have any environments
                if not should_load and "web_ui_environment" in tables:
                    try:
                        # Only auto-load if there are no environments
                        should_load = Environment.objects.count() == 0
                    except Exception:
                        pass

                # Load data if needed
                if should_load:
                    logger.info("Starting repository data loader")
                    # Lazy import to avoid circular import issues
                    from .services.repo_loader import RepositoryLoader

                    results = RepositoryLoader.load_all()
                    logger.info(f"Repository data loaded: {results}")
            except Exception as e:
                logger.warning(f"Error loading repository data: {str(e)}")
        except Exception as e:
            logger.warning(f"Error during initialization: {str(e)}")

    def _generate_cache_version(self):
        """Generate a new cache version to bust frontend caches on server restart"""
        try:
            from django.conf import settings
            cache_version_file = os.path.join(settings.BASE_DIR, '.cache_version')
            
            # Generate new version based on current timestamp
            version = str(int(time.time()))
            
            with open(cache_version_file, 'w') as f:
                f.write(version)
            
            logger.info(f"Generated new cache version: {version}")
        except Exception as e:
            logger.warning(f"Error generating cache version: {str(e)}")
