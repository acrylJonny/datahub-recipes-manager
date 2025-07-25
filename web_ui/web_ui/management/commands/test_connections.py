from django.core.management.base import BaseCommand
from django.utils import timezone
from web_ui.models import Connection
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Test all active DataHub connections and update their status'

    def add_arguments(self, parser):
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force test all connections regardless of last test time',
        )
        parser.add_argument(
            '--connection-id',
            type=str,
            help='Test only a specific connection by ID',
        )

    def handle(self, *args, **options):
        """Test all active connections and update their status."""
        force = options.get('force', False)
        connection_id = options.get('connection_id')
        
        if connection_id:
            # Test specific connection
            try:
                connection = Connection.objects.get(id=connection_id, is_active=True)
                connections = [connection]
            except Connection.DoesNotExist:
                self.stdout.write(
                    self.style.ERROR(f'Connection with ID {connection_id} not found or inactive')
                )
                return
        else:
            # Test all active connections
            connections = Connection.objects.filter(is_active=True)

        self.stdout.write(f'Testing {connections.count()} connection(s)...')
        
        success_count = 0
        failure_count = 0
        skipped_count = 0
        
        for connection in connections:
            # Skip if recently tested (unless forced)
            if not force and connection.last_tested:
                time_since_test = timezone.now() - connection.last_tested
                if time_since_test.total_seconds() < 3600:  # 1 hour
                    self.stdout.write(f'Skipping {connection.name} (tested {time_since_test} ago)')
                    skipped_count += 1
                    continue
            
            self.stdout.write(f'Testing connection: {connection.name}')
            
            try:
                if connection.test_connection():
                    self.stdout.write(
                        self.style.SUCCESS(f'✓ {connection.name} - Connection successful')
                    )
                    success_count += 1
                else:
                    self.stdout.write(
                        self.style.ERROR(f'✗ {connection.name} - Connection failed: {connection.error_message}')
                    )
                    failure_count += 1
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'✗ {connection.name} - Error testing connection: {str(e)}')
                )
                failure_count += 1
                
                # Update connection status manually if test_connection() failed with exception
                connection.connection_status = 'failed'
                connection.error_message = f"Error testing connection: {str(e)}"
                connection.last_tested = timezone.now()
                connection.save(update_fields=['connection_status', 'error_message', 'last_tested'])
        
        # Summary
        self.stdout.write(
            self.style.SUCCESS(
                f'\nTesting complete: {success_count} successful, {failure_count} failed, {skipped_count} skipped'
            )
        )
        
        if failure_count > 0:
            self.stdout.write(
                self.style.WARNING(
                    f'Warning: {failure_count} connection(s) failed. Check connection settings.'
                )
            ) 