from django.core.management.base import BaseCommand
from django.core.management import call_command
from django.utils import timezone
import time
import logging
import signal
import sys

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Start continuous monitoring of DataHub connections (runs every hour)'
    
    def __init__(self):
        super().__init__()
        self.running = True

    def add_arguments(self, parser):
        parser.add_argument(
            '--interval',
            type=int,
            default=3600,  # 1 hour
            help='Testing interval in seconds (default: 3600 = 1 hour)',
        )
        parser.add_argument(
            '--daemon',
            action='store_true',
            help='Run as daemon (suppress output after startup)',
        )

    def handle_signal(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.stdout.write('\nReceived shutdown signal. Stopping connection monitor...')
        self.running = False

    def handle(self, *args, **options):
        """Start continuous connection monitoring."""
        interval = options.get('interval', 3600)
        daemon_mode = options.get('daemon', False)
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
        
        self.stdout.write(
            self.style.SUCCESS(f'Starting connection monitor (testing every {interval} seconds)...')
        )
        
        if daemon_mode:
            self.stdout.write('Running in daemon mode. Use SIGTERM or SIGINT to stop.')
        
        iteration = 0
        
        try:
            while self.running:
                iteration += 1
                
                if not daemon_mode:
                    self.stdout.write(f'\n--- Connection Test Iteration {iteration} at {timezone.now()} ---')
                
                try:
                    # Run the connection test command
                    call_command('test_connections', verbosity=0 if daemon_mode else 1)
                    
                    if not daemon_mode:
                        self.stdout.write(f'Next test in {interval} seconds...')
                    
                except Exception as e:
                    error_msg = f'Error during connection testing: {str(e)}'
                    logger.error(error_msg)
                    if not daemon_mode:
                        self.stdout.write(self.style.ERROR(error_msg))
                
                # Wait for the specified interval (or until shutdown signal)
                for _ in range(interval):
                    if not self.running:
                        break
                    time.sleep(1)
                        
        except KeyboardInterrupt:
            self.stdout.write('\nKeyboard interrupt received. Stopping...')
        
        self.stdout.write(self.style.SUCCESS('Connection monitor stopped.')) 