from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta
from metadata_manager.models import SearchResultCache, SearchProgress


class Command(BaseCommand):
    help = 'Clean up old search cache entries and progress records'

    def add_arguments(self, parser):
        parser.add_argument(
            '--hours',
            type=int,
            default=1,
            help='Remove cache entries older than this many hours (default: 1)'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be deleted without actually deleting'
        )

    def handle(self, *args, **options):
        hours = options['hours']
        dry_run = options['dry_run']
        
        cutoff_time = timezone.now() - timedelta(hours=hours)
        
        # Count entries to be deleted
        cache_count = SearchResultCache.objects.filter(created_at__lt=cutoff_time).count()
        progress_count = SearchProgress.objects.filter(created_at__lt=cutoff_time).count()
        
        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f'DRY RUN: Would delete {cache_count} cache entries and {progress_count} progress records older than {hours} hours'
                )
            )
            return
        
        # Delete old entries
        cache_deleted = SearchResultCache.objects.filter(created_at__lt=cutoff_time).delete()[0]
        progress_deleted = SearchProgress.objects.filter(created_at__lt=cutoff_time).delete()[0]
        
        self.stdout.write(
            self.style.SUCCESS(
                f'Successfully deleted {cache_deleted} cache entries and {progress_deleted} progress records older than {hours} hours'
            )
        )
        
        # Show current counts
        remaining_cache = SearchResultCache.objects.count()
        remaining_progress = SearchProgress.objects.count()
        
        self.stdout.write(
            f'Remaining: {remaining_cache} cache entries, {remaining_progress} progress records'
        ) 