"""
Django management command to refresh DataHub client information for all environments.
"""

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from web_ui.models import Environment, DataHubClientInfo
from web_ui.datahub_utils import get_datahub_client_info, refresh_all_client_info


class Command(BaseCommand):
    help = 'Refresh DataHub client information for environments'

    def add_arguments(self, parser):
        parser.add_argument(
            '--environment',
            type=str,
            help='Refresh client info for a specific environment (by name)',
        )
        parser.add_argument(
            '--all',
            action='store_true',
            help='Refresh client info for all environments',
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Show detailed output',
        )

    def handle(self, *args, **options):
        verbose = options['verbose']
        
        if options['environment']:
            # Refresh specific environment
            try:
                environment = Environment.objects.get(name=options['environment'])
                self.stdout.write(f"Refreshing client info for environment: {environment.name}")
                
                result = get_datahub_client_info(environment)
                
                if result['success']:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"✓ Successfully refreshed client info for {environment.name}"
                        )
                    )
                    if verbose:
                        self.stdout.write(f"  Client ID: {result['client_id']}")
                        self.stdout.write(f"  Server ID: {result['server_id']}")
                        if result['frontend_base_url']:
                            self.stdout.write(f"  Frontend URL: {result['frontend_base_url']}")
                else:
                    self.stdout.write(
                        self.style.ERROR(
                            f"✗ Failed to refresh client info for {environment.name}: {result['error']}"
                        )
                    )
                    
            except Environment.DoesNotExist:
                raise CommandError(f"Environment '{options['environment']}' does not exist")
                
        elif options['all']:
            # Refresh all environments
            self.stdout.write("Refreshing client info for all environments...")
            
            results = refresh_all_client_info()
            
            success_count = 0
            failed_count = 0
            
            for result in results:
                if result['success']:
                    success_count += 1
                    self.stdout.write(
                        self.style.SUCCESS(f"✓ {result['environment']}")
                    )
                    if verbose and result['client_id']:
                        self.stdout.write(f"  Client ID: {result['client_id']}")
                else:
                    failed_count += 1
                    self.stdout.write(
                        self.style.ERROR(f"✗ {result['environment']}: {result['error']}")
                    )
            
            self.stdout.write(f"\nSummary:")
            self.stdout.write(f"  Successful: {success_count}")
            self.stdout.write(f"  Failed: {failed_count}")
            self.stdout.write(f"  Total: {len(results)}")
            
        else:
            # Show current status
            self.stdout.write("Current DataHub client info status:")
            self.stdout.write("-" * 50)
            
            environments = Environment.objects.all().prefetch_related('datahub_client_info')
            
            for env in environments:
                client_info = env.datahub_client_info.first()
                
                if client_info:
                    status_color = self.style.SUCCESS if client_info.connection_status == 'connected' else self.style.ERROR
                    status_text = client_info.connection_status.upper()
                    
                    self.stdout.write(f"{env.name}: {status_color(status_text)}")
                    
                    if verbose:
                        if client_info.client_id:
                            self.stdout.write(f"  Client ID: {client_info.client_id}")
                        if client_info.last_connection_test:
                            self.stdout.write(f"  Last tested: {client_info.last_connection_test}")
                        if client_info.error_message:
                            self.stdout.write(f"  Error: {client_info.error_message}")
                else:
                    self.stdout.write(f"{env.name}: {self.style.WARNING('NO DATA')}")
            
            self.stdout.write(f"\nUse --all to refresh all environments or --environment <name> for specific environment") 