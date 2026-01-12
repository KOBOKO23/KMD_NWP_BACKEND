"""
Django Management Command to Clean Up Old Data
File: wrf_data/management/commands/cleanup_old_data.py

Usage:
    python manage.py cleanup_old_data                    # Use default retention (7 days)
    python manage.py cleanup_old_data --days 14          # Keep last 14 days
    python manage.py cleanup_old_data --dry-run          # Preview without deleting
"""

from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils import timezone
from datetime import datetime, timedelta
from pathlib import Path
import shutil
import logging

from wrf_data.models import ForecastRun, ForecastData

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Clean up old GRIB files and database records based on retention policy'

    def add_arguments(self, parser):
        parser.add_argument(
            '--days',
            type=int,
            help='Number of days to keep (default: from settings)',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be deleted without actually deleting',
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Skip confirmation prompt',
        )

    def handle(self, *args, **options):
        """Main command handler"""
        
        # Get retention period
        keep_days = options.get('days') or settings.WRF_CONFIG.get('KEEP_RAW_FILES_DAYS', 7)
        cutoff_date = timezone.now() - timedelta(days=keep_days)
        
        self.stdout.write(self.style.SUCCESS('\n' + '=' * 70))
        self.stdout.write(self.style.SUCCESS('🧹 KMD WRF Data Cleanup'))
        self.stdout.write(self.style.SUCCESS('=' * 70))
        self.stdout.write(f'Retention Period: {keep_days} days')
        self.stdout.write(f'Cutoff Date:      {cutoff_date.strftime("%Y-%m-%d %H:%M")}')
        self.stdout.write(f'Dry Run:          {options["dry_run"]}')
        self.stdout.write(self.style.SUCCESS('=' * 70 + '\n'))
        
        # Clean GRIB files
        files_stats = self.cleanup_grib_files(
            cutoff_date=cutoff_date,
            dry_run=options['dry_run'],
            force=options['force']
        )
        
        # Clean database records
        db_stats = self.cleanup_database_records(
            cutoff_date=cutoff_date,
            dry_run=options['dry_run'],
            force=options['force']
        )
        
        # Summary
        self.stdout.write(self.style.SUCCESS('\n' + '=' * 70))
        self.stdout.write(self.style.SUCCESS('📊 Cleanup Summary'))
        self.stdout.write(self.style.SUCCESS('=' * 70))
        
        # Files
        self.stdout.write('\n📁 GRIB Files:')
        self.stdout.write(f'   Folders deleted:  {files_stats["folders_deleted"]}')
        self.stdout.write(f'   Files deleted:    {files_stats["files_deleted"]}')
        self.stdout.write(f'   Space freed:      {files_stats["space_freed_mb"]:.2f} MB')
        
        # Database
        self.stdout.write('\n💾 Database Records:')
        self.stdout.write(f'   Forecast runs:    {db_stats["forecast_runs_deleted"]}')
        self.stdout.write(f'   Forecast data:    {db_stats["forecast_data_deleted"]}')
        self.stdout.write(f'   Fetch logs:       {db_stats["fetch_logs_deleted"]}')
        
        self.stdout.write(self.style.SUCCESS('\n' + '=' * 70))
        
        if options['dry_run']:
            self.stdout.write(self.style.WARNING('\n⚠️  DRY RUN - No files were actually deleted\n'))
        else:
            self.stdout.write(self.style.SUCCESS('\n✅ Cleanup completed successfully\n'))
    
    def cleanup_grib_files(self, cutoff_date, dry_run=False, force=False):
        """
        Clean up old GRIB files from disk
        
        Returns:
            dict: Statistics about deleted files
        """
        raw_path = Path(settings.WRF_CONFIG['LOCAL_DATA_PATH'])
        
        if not raw_path.exists():
            self.stdout.write(self.style.WARNING(f'\n⚠️  Data directory not found: {raw_path}\n'))
            return {
                'folders_deleted': 0,
                'files_deleted': 0,
                'space_freed_mb': 0
            }
        
        self.stdout.write('\n🔍 Scanning GRIB files...\n')
        
        folders_to_delete = []
        total_size = 0
        total_files = 0
        
        # Scan for old folders
        for folder in raw_path.iterdir():
            if not folder.is_dir():
                continue
            
            folder_name = folder.name
            
            # Check if folder name is a valid date (YYYYMMDDHH)
            if len(folder_name) == 10 and folder_name.isdigit():
                try:
                    folder_date = datetime.strptime(folder_name, '%Y%m%d%H')
                    folder_date = timezone.make_aware(folder_date)
                    
                    if folder_date < cutoff_date:
                        # Calculate folder size
                        folder_size = sum(
                            f.stat().st_size for f in folder.rglob('*') if f.is_file()
                        )
                        file_count = sum(1 for f in folder.rglob('*') if f.is_file())
                        
                        folders_to_delete.append({
                            'path': folder,
                            'name': folder_name,
                            'date': folder_date,
                            'size': folder_size,
                            'files': file_count
                        })
                        
                        total_size += folder_size
                        total_files += file_count
                        
                except ValueError:
                    logger.warning(f"Invalid date folder: {folder_name}")
                    continue
        
        if not folders_to_delete:
            self.stdout.write(self.style.SUCCESS('✓ No old files to delete\n'))
            return {
                'folders_deleted': 0,
                'files_deleted': 0,
                'space_freed_mb': 0
            }
        
        # Show folders to be deleted
        self.stdout.write(f'Found {len(folders_to_delete)} old folders:\n')
        for folder_info in folders_to_delete:
            self.stdout.write(
                f'  • {folder_info["name"]} → '
                f'{folder_info["date"].strftime("%Y-%m-%d %H:%M")} → '
                f'{folder_info["size"] / (1024*1024):.1f} MB ({folder_info["files"]} files)'
            )
        
        self.stdout.write(f'\nTotal: {total_size / (1024*1024):.1f} MB in {total_files} files\n')
        
        # Confirm deletion
        if not dry_run and not force:
            confirm = input('Delete these folders? [y/N]: ')
            if confirm.lower() != 'y':
                self.stdout.write(self.style.WARNING('Aborted by user\n'))
                return {
                    'folders_deleted': 0,
                    'files_deleted': 0,
                    'space_freed_mb': 0
                }
        
        # Delete folders
        folders_deleted = 0
        if not dry_run:
            for folder_info in folders_to_delete:
                try:
                    shutil.rmtree(folder_info['path'])
                    folders_deleted += 1
                    logger.info(f"Deleted folder: {folder_info['name']}")
                except Exception as e:
                    logger.error(f"Failed to delete {folder_info['name']}: {e}")
                    self.stdout.write(self.style.ERROR(
                        f'  ✗ Failed to delete {folder_info["name"]}: {e}'
                    ))
        else:
            folders_deleted = len(folders_to_delete)
        
        return {
            'folders_deleted': folders_deleted,
            'files_deleted': total_files,
            'space_freed_mb': total_size / (1024 * 1024)
        }
    
    def cleanup_database_records(self, cutoff_date, dry_run=False, force=False):
        """
        Clean up old database records
        
        Returns:
            dict: Statistics about deleted records
        """
        self.stdout.write('\n🔍 Scanning database records...\n')
        
        # Find old forecast runs
        old_runs = ForecastRun.objects.filter(
            run_date__lt=cutoff_date.date()
        )
        
        runs_count = old_runs.count()
        
        if runs_count == 0:
            self.stdout.write(self.style.SUCCESS('✓ No old database records to delete\n'))
            return {
                'forecast_runs_deleted': 0,
                'forecast_data_deleted': 0,
                'fetch_logs_deleted': 0
            }
        
        # Count related records (will be cascade deleted)
        data_count = ForecastData.objects.filter(
            forecast_run__in=old_runs
        ).count()
        
        from wrf_data.models import DataFetchLog
        logs_count = DataFetchLog.objects.filter(
            forecast_run__in=old_runs
        ).count()
        
        self.stdout.write(f'Found old database records:')
        self.stdout.write(f'  • Forecast runs:  {runs_count}')
        self.stdout.write(f'  • Forecast data:  {data_count}')
        self.stdout.write(f'  • Fetch logs:     {logs_count}\n')
        
        # Show sample of runs to be deleted
        sample_runs = old_runs.order_by('-run_date')[:5]
        self.stdout.write('Sample runs (showing 5 most recent):')
        for run in sample_runs:
            self.stdout.write(
                f'  • {run.run_date} {run.run_time} → {run.status}'
            )
        if runs_count > 5:
            self.stdout.write(f'  ... and {runs_count - 5} more\n')
        else:
            self.stdout.write('')
        
        # Confirm deletion
        if not dry_run and not force:
            confirm = input('Delete these database records? [y/N]: ')
            if confirm.lower() != 'y':
                self.stdout.write(self.style.WARNING('Aborted by user\n'))
                return {
                    'forecast_runs_deleted': 0,
                    'forecast_data_deleted': 0,
                    'fetch_logs_deleted': 0
                }
        
        # Delete records
        if not dry_run:
            try:
                # Django will cascade delete related records
                deleted_count = old_runs.delete()
                logger.info(f"Deleted {runs_count} old forecast runs from database")
            except Exception as e:
                logger.error(f"Failed to delete database records: {e}")
                self.stdout.write(self.style.ERROR(f'✗ Database deletion failed: {e}\n'))
                return {
                    'forecast_runs_deleted': 0,
                    'forecast_data_deleted': 0,
                    'fetch_logs_deleted': 0
                }
        
        return {
            'forecast_runs_deleted': runs_count,
            'forecast_data_deleted': data_count,
            'fetch_logs_deleted': logs_count
        }