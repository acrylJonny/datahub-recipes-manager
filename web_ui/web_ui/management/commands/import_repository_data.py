from django.core.management.base import BaseCommand
from web_ui.services.repo_loader import RepositoryLoader
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Import data from the repository directory structure into the database"

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting repository data import..."))

        try:
            # Run the repository data loader
            results = RepositoryLoader.load_all()

            # Print results
            self.stdout.write(self.style.SUCCESS("Successfully loaded:"))
            self.stdout.write(f"- {results['environments']} environments")
            self.stdout.write(
                f"- {results['template_vars']} environment variable templates"
            )
            self.stdout.write(f"- {results['recipe_templates']} recipe templates")
            self.stdout.write(
                f"- {results['env_vars_instances']} environment variable instances"
            )
            self.stdout.write(f"- {results['recipe_instances']} recipe instances")

            total_loaded = (
                results["environments"]
                + results["template_vars"]
                + results["recipe_templates"]
                + results["env_vars_instances"]
                + results["recipe_instances"]
            )

            self.stdout.write(
                self.style.SUCCESS(
                    f"Import complete. Loaded {total_loaded} items in total."
                )
            )

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error during import: {str(e)}"))
            logger.error(f"Error during repository import: {str(e)}", exc_info=True)
