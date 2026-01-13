"""
Django Management Command to Fetch WRF Data
Production version with SSH key authentication
File: wrf_data/management/commands/fetch_wrf_data.py

Usage:
    python manage.py fetch_wrf_data --list                    # List available runs
    python manage.py fetch_wrf_data --latest                  # Fetch latest run
    python manage.py fetch_wrf_data --date 2025011219         # Fetch specific run (YYYYMMDDHH)
    python manage.py fetch_wrf_data --domain kenya            # Fetch specific domain
"""

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django.utils import timezone
from datetime import datetime, timedelta
import logging
from pathlib import Path

from wrf_data.models import Domain, Parameter, ForecastRun, ForecastData, DataFetchLog
from wrf_data.utils.ssh_fetcher import create_fetcher_from_config
from wrf_data.utils.grib_processor import GRIBProcessor

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Fetch and process WRF GRIB files from remote server via SSH'

    def add_arguments(self, parser):
        parser.add_argument(
            '--date',
            type=str,
            help='Forecast run date in YYYYMMDDHH format (e.g., 2025011219). Default: yesterday at 19:00',
        )
        parser.add_argument(
            '--domain',
            type=str,
            choices=['kenya', 'east-africa', 'both'],
            default='both',
            help='Domain to fetch. Default: both',
        )
        parser.add_argument(
            '--max-hours',
            type=int,
            default=72,
            help='Maximum forecast hours to download (default: 72)',
        )
        parser.add_argument(
            '--latest',
            action='store_true',
            help='Fetch the latest available run from server',
        )
        parser.add_argument(
            '--list',
            action='store_true',
            help='List available runs on server without downloading',
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force re-fetch even if data already exists',
        )

    def handle(self, *args, **options):
        """Main command handler"""
        
        # List mode
        if options['list']:
            self.list_available_runs()
            return
        
        # Determine run date
        if options['latest']:
            run_date = self.get_latest_run_date()
        elif options['date']:
            try:
                run_date = datetime.strptime(options['date'], '%Y%m%d%H')
            except ValueError:
                raise CommandError('Invalid date format. Use YYYYMMDDHH (e.g., 2025011219)')
        else:
            # Default: yesterday at 19:00 (7pm EAT)
            yesterday = timezone.now().date() - timedelta(days=1)
            run_date = datetime.combine(yesterday, datetime.strptime('19:00', '%H:%M').time())
        
        # Make timezone aware
        run_date = timezone.make_aware(run_date)
        
        # Display header
        self.stdout.write(self.style.SUCCESS('\n' + '=' * 70))
        self.stdout.write(self.style.SUCCESS('🌤️  KMD WRF Data Fetcher - Production'))
        self.stdout.write(self.style.SUCCESS('=' * 70))
        self.stdout.write(f'Run Date:     {run_date.strftime("%Y-%m-%d %H:%M")}')
        self.stdout.write(f'Domain:       {options["domain"]}')
        self.stdout.write(f'Max Hours:    {options["max_hours"]}')
        self.stdout.write(f'Force Fetch:  {options["force"]}')
        self.stdout.write(self.style.SUCCESS('=' * 70 + '\n'))
        
        # Check if already exists
        if not options['force']:
            existing = ForecastRun.objects.filter(
                run_date=run_date.date(),
                run_time=run_date.time(),
                status='completed'
            ).exists()
            
            if existing:
                self.stdout.write(self.style.WARNING(
                    f'\n⚠️  Data already exists for {run_date.strftime("%Y%m%d%H")}.'
                ))
                self.stdout.write(self.style.WARNING(
                    '   Use --force to re-fetch.\n'
                ))
                return
        
        # Create or update ForecastRun
        forecast_run, created = ForecastRun.objects.get_or_create(
            run_date=run_date.date(),
            run_time=run_date.time(),
            defaults={
                'initialization_time': run_date,
                'forecast_hours': options['max_hours'],
                'status': 'fetching',
                'progress': 0
            }
        )
        
        if not created:
            forecast_run.status = 'fetching'
            forecast_run.progress = 0
            forecast_run.error_message = ''
            forecast_run.save()
            self.stdout.write(f'Updating existing forecast run (ID: {forecast_run.id})\n')
        else:
            self.stdout.write(f'Created new forecast run (ID: {forecast_run.id})\n')
        
        # Create fetch log
        fetch_log = DataFetchLog.objects.create(
            forecast_run=forecast_run,
            status='partial',
            ssh_host=settings.WRF_CONFIG['SSH_HOST'],
            ssh_user=settings.WRF_CONFIG['SSH_USERNAME']
        )
        
        try:
            # Initialize SSH fetcher
            self.stdout.write('📡 Connecting to WRF server...')
            fetcher = create_fetcher_from_config(settings.WRF_CONFIG)
            
            with fetcher:
                self.stdout.write(self.style.SUCCESS('✓ Connected successfully\n'))
                
                # Check if run exists on server
                if not fetcher.check_run_exists(run_date):
                    error_msg = f"Run {run_date.strftime('%Y%m%d%H')} not found on server"
                    self.stdout.write(self.style.ERROR(f'\n❌ {error_msg}\n'))
                    
                    # Show available runs
                    self.stdout.write('📋 Available runs on server (last 10):')
                    runs = fetcher.list_available_runs()
                    for i, run in enumerate(runs[:10], 1):
                        dt = datetime.strptime(run, '%Y%m%d%H')
                        self.stdout.write(f'  {i:2d}. {run} → {dt.strftime("%Y-%m-%d %H:%M")}')
                    
                    fetch_log.status = 'failed'
                    fetch_log.error_message = error_msg
                    fetch_log.completed_at = timezone.now()
                    fetch_log.save()
                    
                    forecast_run.status = 'failed'
                    forecast_run.error_message = error_msg
                    forecast_run.save()
                    
                    raise CommandError(error_msg)
                
                # Download GRIB files
                self.stdout.write('📥 Downloading GRIB files...\n')
                results = fetcher.download_forecast_run(
                    run_date=run_date,
                    domain=options['domain'],
                    max_hours=options['max_hours']
                )
                
                # Update fetch log
                fetch_log.files_requested = results['total']
                fetch_log.files_downloaded = len(results['success'])
                fetch_log.completed_at = timezone.now()
                
                # Calculate total bytes
                total_bytes = sum(
                    Path(f).stat().st_size for f in results['success'] 
                    if Path(f).exists()
                )
                fetch_log.total_bytes = total_bytes
                
                if results['failed']:
                    fetch_log.status = 'partial'
                    fetch_log.error_message = f"{len(results['failed'])} files failed"
                    self.stdout.write(self.style.WARNING(
                        f'\n⚠️  Downloaded {len(results["success"])}/{results["total"]} files'
                    ))
                    for failed_file in results['failed']:
                        self.stdout.write(self.style.ERROR(f'   ✗ {failed_file}'))
                else:
                    fetch_log.status = 'success'
                    self.stdout.write(self.style.SUCCESS(
                        f'\n✓ Successfully downloaded all {results["total"]} files '
                        f'({total_bytes / (1024*1024):.1f} MB)\n'
                    ))
                
                fetch_log.add_log(f"Downloaded {len(results['success'])}/{results['total']} files", 'info')
                fetch_log.save()
                
                # Update forecast run
                forecast_run.progress = 50  # Files downloaded
                forecast_run.files_downloaded = {
                    'run_folder': results['run_folder'],
                    'success': [str(Path(f).name) for f in results['success']],
                    'failed': results['failed'],
                    'total_bytes': total_bytes
                }
                forecast_run.save()
                
                # Process GRIB files
                if results['success']:
                    self.stdout.write('\n🔬 Processing GRIB files...\n')
                    self.process_downloaded_files(
                        forecast_run=forecast_run,
                        file_list=results['success'],
                        domain_filter=options['domain']
                    )
                
                # Mark as completed
                forecast_run.status = 'completed'
                forecast_run.progress = 100
                forecast_run.completed_at = timezone.now()
                forecast_run.save()
                
                self.stdout.write(self.style.SUCCESS('\n' + '=' * 70))
                self.stdout.write(self.style.SUCCESS(
                    f'✅ Fetch complete! Files saved to: data/raw/{results["run_folder"]}'
                ))
                self.stdout.write(self.style.SUCCESS('=' * 70 + '\n'))
                
        except Exception as e:
            error_msg = str(e)
            self.stdout.write(self.style.ERROR(f'\n❌ Error: {error_msg}\n'))
            logger.exception("Fetch failed")
            
            fetch_log.status = 'failed'
            fetch_log.error_message = error_msg
            fetch_log.completed_at = timezone.now()
            fetch_log.save()
            
            forecast_run.status = 'failed'
            forecast_run.error_message = error_msg
            forecast_run.save()
            
            raise CommandError(f'Fetch failed: {error_msg}')
    
    def list_available_runs(self):
        """List available runs on the server"""
        self.stdout.write(self.style.SUCCESS('\n📋 Listing available runs on server...\n'))
        
        try:
            fetcher = create_fetcher_from_config(settings.WRF_CONFIG)
            
            with fetcher:
                runs = fetcher.list_available_runs()
                
                if not runs:
                    self.stdout.write(self.style.WARNING('No runs found on server'))
                    return
                
                self.stdout.write(f'Found {len(runs)} runs:\n')
                
                for i, run in enumerate(runs[:20], 1):
                    try:
                        dt = datetime.strptime(run, '%Y%m%d%H')
                        formatted = dt.strftime('%Y-%m-%d %H:%M')
                        
                        # Check if already fetched
                        exists = ForecastRun.objects.filter(
                            run_date=dt.date(),
                            run_time=dt.time(),
                            status='completed'
                        ).exists()
                        
                        status_icon = '✓' if exists else ' '
                        self.stdout.write(f'  [{status_icon}] {i:2d}. {run} → {formatted}')
                    except:
                        self.stdout.write(f'      {i:2d}. {run}')
                
                if len(runs) > 20:
                    self.stdout.write(f'\n... and {len(runs) - 20} more')
                
                self.stdout.write('\n')
                
        except Exception as e:
            raise CommandError(f'Failed to list runs: {e}')
    
    def get_latest_run_date(self) -> datetime:
        """Get the latest run date from server"""
        self.stdout.write('🔍 Finding latest run on server...')
        
        try:
            fetcher = create_fetcher_from_config(settings.WRF_CONFIG)
            
            with fetcher:
                latest_folder = fetcher.get_latest_run_folder()
                
                if not latest_folder:
                    raise CommandError('No runs found on server')
                
                run_date = datetime.strptime(latest_folder, '%Y%m%d%H')
                self.stdout.write(self.style.SUCCESS(
                    f'✓ Latest run: {latest_folder} ({run_date.strftime("%Y-%m-%d %H:%M")})\n'
                ))
                
                return run_date
                
        except Exception as e:
            raise CommandError(f'Failed to get latest run: {e}')
    
    def process_downloaded_files(self, forecast_run, file_list, domain_filter):
        """
        Process all downloaded GRIB files
        
        Args:
            forecast_run: ForecastRun instance
            file_list: List of local file paths
            domain_filter: 'kenya', 'east-africa', or 'both'
        """
        total_files = len(file_list)
        processed_count = 0
        failed_count = 0
        
        # Get active parameters from database
        parameters = Parameter.objects.filter(is_active=True)
        
        for i, grib_file in enumerate(file_list, 1):
            file_path = Path(grib_file)
            filename = file_path.name
            
            # Parse filename: WRFPRS_d01.00 or WRFPRS_d02.15
            try:
                parts = filename.split('.')
                if len(parts) != 2 or not parts[0].startswith('WRFPRS_d'):
                    logger.warning(f"Skipping invalid filename: {filename}")
                    continue
                
                domain_suffix = parts[0][-2:]  # '01' or '02'
                hour = int(parts[1])           # 0, 1, 2, ..., 72
                
                # Determine domain
                domain_code = 'kenya' if domain_suffix == '01' else 'east-africa'
                
                # Skip if not in domain filter
                if domain_filter != 'both' and domain_filter != domain_code:
                    continue
                
                # Get domain from database
                try:
                    domain = Domain.objects.get(code=domain_code, is_active=True)
                except Domain.DoesNotExist:
                    logger.error(f"Domain {domain_code} not found in database")
                    failed_count += 1
                    continue
                
                # Calculate time step (0-24 for 0-72 hours at 3-hour intervals)
                time_step = hour // 3
                
                # Calculate valid time
                valid_time = forecast_run.initialization_time + timedelta(hours=hour)
                
                self.stdout.write(
                    f'  [{i:3d}/{total_files}] {filename} → {domain_code} T+{hour}h'
                )
                
                # Process GRIB file
                success = self.process_single_grib(
                    forecast_run=forecast_run,
                    domain=domain,
                    grib_path=str(file_path),
                    time_step=time_step,
                    valid_time=valid_time,
                    parameters=parameters
                )
                
                if success:
                    processed_count += 1
                    self.stdout.write(self.style.SUCCESS('    ✓ Processed'))
                else:
                    failed_count += 1
                    self.stdout.write(self.style.ERROR('    ✗ Failed'))
                
                # Update progress
                progress = 50 + int((i / total_files) * 50)  # 50-100%
                forecast_run.progress = progress
                forecast_run.save()
                
            except Exception as e:
                logger.error(f"Error processing {filename}: {e}")
                failed_count += 1
                self.stdout.write(self.style.ERROR(f'    ✗ Error: {e}'))
        
        self.stdout.write(self.style.SUCCESS(
            f'\n✓ Processed {processed_count}/{total_files} files'
        ))
        if failed_count > 0:
            self.stdout.write(self.style.WARNING(
                f'⚠️  {failed_count} files failed processing'
            ))
    
    def process_single_grib(self, forecast_run, domain, grib_path, time_step, valid_time, parameters):
        """
        Process a single GRIB file and extract all parameters
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with GRIBProcessor(grib_path) as processor:
                for parameter in parameters:
                    try:
                        # Extract parameter from GRIB
                        data = processor.extract_parameter(
                            parameter_code=parameter.code,
                            apply_color_mapping=True
                        )
                        
                        if not data:
                            logger.warning(
                                f'No data extracted for {parameter.code} from {grib_path}'
                            )
                            continue
                        
                        # Save to database
                        forecast_data, created = ForecastData.objects.update_or_create(
                            forecast_run=forecast_run,
                            domain=domain,
                            parameter=parameter,
                            time_step=time_step,
                            defaults={
                                'valid_time': valid_time,
                                'grid_lats': data['lats'],
                                'grid_lons': data['lons'],
                                'values': data['values'],
                                'color_data': data.get('color_data', []),
                                'min_value': data['metadata'].get('min'),
                                'max_value': data['metadata'].get('max'),
                                'mean_value': data['metadata'].get('mean'),
                                'source_file': grib_path,
                            }
                        )
                        
                        action = 'Created' if created else 'Updated'
                        logger.debug(f'{action} {parameter.code} data for {domain.code} T+{time_step*3}h')
                        
                    except Exception as e:
                        logger.error(f'Error extracting {parameter.code}: {e}')
                        continue
                
                return True
                
        except Exception as e:
            logger.error(f'Error processing GRIB file {grib_path}: {e}')
            return False