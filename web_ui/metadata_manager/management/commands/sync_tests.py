from django.core.management.base import BaseCommand
from django.utils import timezone
import logging
import sys
import os

# Add project root to sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
)

from utils.datahub_client_adapter import test_datahub_connection
from metadata_manager.models import Test

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Sync tests from DataHub to local database'

    def add_arguments(self, parser):
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force sync even if tests already exist locally'
        )

    def handle(self, *args, **options):
        self.stdout.write("Starting test sync from DataHub...")
        
        try:
            # Check DataHub connection
            connected, client = test_datahub_connection()
            if not connected or not client:
                self.stdout.write(
                    self.style.ERROR("Not connected to DataHub. Please check your connection settings.")
                )
                return

            # Fetch tests from DataHub
            self.stdout.write("Fetching tests from DataHub...")
            tests_response = client.list_tests()
            if not tests_response:
                self.stdout.write(self.style.WARNING("No tests found on DataHub."))
                return

            synced_count = 0
            updated_count = 0
            error_count = 0

            for remote_test in tests_response:
                try:
                    test_urn = remote_test.get('urn', '')
                    test_name = remote_test.get('name', '')
                    
                    if not test_urn:
                        self.stdout.write(f"Skipping test without URN: {test_name}")
                        error_count += 1
                        continue

                    # Check if test already exists locally
                    local_test, created = Test.objects.get_or_create(
                        urn=test_urn,
                        defaults={
                            'name': test_name,
                            'description': remote_test.get('description', ''),
                            'category': remote_test.get('category', ''),
                            'definition_json': remote_test.get('definition_json', {}),
                            'sync_status': 'SYNCED',
                            'last_synced': timezone.now(),
                        }
                    )

                    if created:
                        synced_count += 1
                        self.stdout.write(f"Synced new test: {test_name}")
                    elif options['force'] or local_test.sync_status != 'SYNCED':
                        # Update existing test
                        local_test.name = test_name
                        local_test.description = remote_test.get('description', '')
                        local_test.category = remote_test.get('category', '')
                        local_test.definition_json = remote_test.get('definition_json', {})
                        
                        # Update results if available
                        results = remote_test.get('results', {})
                        if results:
                            local_test.passing_count = results.get('passingCount', 0)
                            local_test.failing_count = results.get('failingCount', 0)
                            local_test.last_run_timestamp = results.get('lastRunTimestampMillis')
                            local_test.results_data = results

                        local_test.sync_status = 'SYNCED'
                        local_test.last_synced = timezone.now()
                        local_test.save()
                        
                        updated_count += 1
                        self.stdout.write(f"Updated test: {test_name}")
                    else:
                        self.stdout.write(f"Test already synced: {test_name}")

                except Exception as e:
                    error_count += 1
                    logger.error(f"Error syncing test {remote_test.get('name', 'unknown')}: {str(e)}")
                    self.stdout.write(
                        self.style.ERROR(f"Error syncing test {remote_test.get('name', 'unknown')}: {str(e)}")
                    )

            # Summary
            self.stdout.write(
                self.style.SUCCESS(
                    f"Sync completed! Synced: {synced_count}, Updated: {updated_count}, Errors: {error_count}"
                )
            )

        except Exception as e:
            logger.error(f"Error during test sync: {str(e)}")
            self.stdout.write(self.style.ERROR(f"Error during test sync: {str(e)}")) 