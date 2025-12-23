"""
Django Management Command to Fetch WRF Data
File: wrf_data/management/commands/fetch_wrf_data.py

Usage:
    python manage.py fetch_wrf_data                    # Fetch today's data
    python manage.py fetch_wrf_data --date 2024-12-15  # Fetch specific date
    python manage.py fetch_wrf_data --force            # Force re-fetch existing data
"""

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django.utils import timezone
from datetime import datetime
import logging
from pathlib import Path

from wrf_data.models import Domain, Parameter, ForecastRun, ForecastData, DataFetchLog
from wrf_data.utils.ssh_fetcher import WRFDataFetcher
from wrf_data.utils.grib_processor import GRIBProcessor

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Fetch and process WRF model data from remote server'

    def add_arguments(self, parser):
        parser.add_argument(
            '--date',
            type=str,
            help='Date to fetch (YYYY-MM-DD format). Default: today',
        )
        parser.add_argument(
            '--domain',
            type=str,
            choices=['kenya', 'east-africa', 'both'],
            default='both',
            help='Domain to fetch. Default: both',
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force re-fetch even if data already exists',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Dry run - test connection and list files without downloading',
        )

    def handle(self, *args, **options):
        """Main command handler"""
        # Parse date
        if options['date']:
            try:
                fetch_date = datetime.strptime(options['date'], '%Y-%m-%d').date()
            except ValueError:
                raise CommandError('Invalid date format. Use YYYY-MM-DD')
        else:
            fetch_date = timezone.now().date()

        # Base time
        base_time_str = settings.WRF_CONFIG.get('BASE_TIME', '09:00')
        hour, minute = map(int, base_time_str.split(':'))
        base_datetime = timezone.make_aware(
            datetime.combine(fetch_date, datetime.min.time().replace(hour=hour, minute=minute))
        )

        self.stdout.write(self.style.SUCCESS(f'\n🌤️  KMD WRF Data Fetcher'))
        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(f'Fetch Date: {fetch_date}')
        self.stdout.write(f'Base Time: {base_datetime}')
        self.stdout.write(f'Domain: {options["domain"]}')
        self.stdout.write(f'Dry Run: {options["dry_run"]}')
        self.stdout.write(self.style.SUCCESS('=' * 60 + '\n'))

        # Check existing
        if not options['force'] and ForecastRun.objects.filter(run_date=fetch_date, status='completed').exists():
            self.stdout.write(self.style.WARNING(
                f'⚠️  Data already exists for {fetch_date}. Use --force to re-fetch.'
            ))
            return

        # Validate config
        if not self._validate_config():
            return

        # Determine domains
        domains = ['kenya', 'east-africa'] if options['domain'] == 'both' else [options['domain']]

        # Forecast run
        forecast_run, created = ForecastRun.objects.get_or_create(
            run_date=fetch_date,
            run_time=base_datetime.time(),
            defaults={
                'initialization_time': base_datetime,
                'forecast_hours': settings.WRF_CONFIG.get('FORECAST_HOURS', 72),
                'status': 'pending',
            }
        )

        if not created and not options['force']:
            self.stdout.write(self.style.WARNING(f'Forecast run already exists (ID: {forecast_run.id})'))

        forecast_run.status = 'fetching' if not options['dry_run'] else 'pending'
        forecast_run.save()

        # Process domains
        for domain_code in domains:
            self.stdout.write(f'\n📡 Processing domain: {domain_code.upper()}')
            self.stdout.write('-' * 60)
            try:
                self._process_domain(
                    forecast_run=forecast_run,
                    domain_code=domain_code,
                    base_datetime=base_datetime,
                    dry_run=options['dry_run']
                )
            except Exception as e:
                self.stderr.write(self.style.ERROR(f'❌ Error processing {domain_code}: {e}'))
                logger.exception(f"Error processing domain {domain_code}")
                forecast_run.status = 'failed'
                forecast_run.error_message = str(e)
                forecast_run.save()
                raise

        if not options['dry_run']:
            forecast_run.status = 'completed'
            forecast_run.completed_at = timezone.now()
            forecast_run.progress = 100
            forecast_run.save()
            self.stdout.write(self.style.SUCCESS(f'\n✅ Data fetch completed successfully!'))
            self.stdout.write(f'Forecast Run ID: {forecast_run.id}')
        else:
            self.stdout.write(self.style.SUCCESS(f'\n✅ Dry run completed!'))

    def _validate_config(self):
        """Validate WRF configuration"""
        config = settings.WRF_CONFIG
        required_fields = ['WRF_TARGET_HOST', 'WRF_TARGET_USERNAME']
        missing = [f for f in required_fields if not config.get(f)]
        if missing:
            self.stderr.write(self.style.ERROR(f'❌ Missing required config: {", ".join(missing)}'))
            return False

        if not (config.get('WRF_TARGET_PASSWORD') or config.get('WRF_KEY_PATH')):
            self.stderr.write(self.style.ERROR('❌ No authentication method configured (password or SSH key)'))
            return False

        self.stdout.write(self.style.SUCCESS('✓ Configuration validated'))
        return True

    def _process_domain(self, forecast_run, domain_code, base_datetime, dry_run=False):
        """Process a single domain via jump host"""
        try:
            domain = Domain.objects.get(code=domain_code, is_active=True)
        except Domain.DoesNotExist:
            raise CommandError(f'Domain {domain_code} not found or inactive')

        config = settings.WRF_CONFIG
        remote_path = config['KENYA_PATH'] if domain_code == 'kenya' else config['EAST_AFRICA_PATH']
        domain_suffix = config.get('KENYA_FILE_SUFFIX' if domain_code == 'kenya' else 'EAST_AFRICA_FILE_SUFFIX', '01')
        local_path = Path(config['LOCAL_DATA_PATH']) / forecast_run.run_date.strftime('%Y%m%d')
        local_path.mkdir(parents=True, exist_ok=True)

        fetch_log = DataFetchLog.objects.create(
            forecast_run=forecast_run,
            ssh_host=config['WRF_TARGET_HOST'],
            ssh_user=config['WRF_TARGET_USERNAME'],
            status='success' if dry_run else 'pending',
        )

        fetcher = WRFDataFetcher(
            host=config['WRF_TARGET_HOST'],
            port=config.get('WRF_TARGET_PORT', 22),
            username=config['WRF_TARGET_USERNAME'],
            password=config.get('WRF_TARGET_PASSWORD'),
            jump_host=config.get('WRF_JUMP_HOST'),
            jump_port=config.get('WRF_JUMP_PORT', 22),
            jump_username=config.get('WRF_JUMP_USERNAME'),
            jump_password=config.get('WRF_JUMP_PASSWORD'),
        )

        self.stdout.write('Connecting to WRF server via jump host...')
        try:
            with fetcher:
                self.stdout.write(self.style.SUCCESS('✓ Connected to target server'))

                file_list = fetcher.get_wrf_files_for_date(
                    date=base_datetime,
                    domain_suffix=domain_suffix,
                    remote_base_path=remote_path,
                    local_base_path=str(local_path),
                    hours=config.get('FORECAST_HOURS', 72),
                )

                self.stdout.write(f'Found {len(file_list)} files to process')
                fetch_log.files_requested = len(file_list)

                if dry_run:
                    self.stdout.write(self.style.WARNING('DRY RUN - Files that would be downloaded:'))
                    for file_info in file_list[:5]:
                        self.stdout.write(f"  - {file_info['remote']}")
                    if len(file_list) > 5:
                        self.stdout.write(f"  ... and {len(file_list) - 5} more")
                    fetch_log.save()
                    return

                downloaded_count = 0
                processed_count = 0
                for i, file_info in enumerate(file_list):
                    forecast_run.progress = int((i / len(file_list)) * 100)
                    forecast_run.save()
                    self.stdout.write(f'\n[{i+1}/{len(file_list)}] Processing T+{file_info["hour"]}h...')

                    if fetcher.download_file(file_info['remote'], file_info['local']):
                        downloaded_count += 1
                        fetch_log.files_downloaded = downloaded_count
                        fetch_log.save()
                        try:
                            self._process_grib_file(
                                forecast_run, domain, file_info['local'], file_info['hour'] // 3, file_info['valid_time']
                            )
                            processed_count += 1
                            self.stdout.write(self.style.SUCCESS('  ✓ Processed'))
                        except Exception as e:
                            self.stderr.write(self.style.ERROR(f'  ❌ Processing failed: {e}'))
                            fetch_log.add_log(f'Processing failed for {file_info["local"]}: {e}', 'error')
                    else:
                        self.stderr.write(self.style.ERROR('  ❌ Download failed'))
                        fetch_log.add_log(f'Download failed: {file_info["remote"]}', 'error')

                fetch_log.completed_at = timezone.now()
                fetch_log.status = 'success' if downloaded_count == len(file_list) else 'partial'
                fetch_log.save()
                self.stdout.write(self.style.SUCCESS(f'\n✅ Downloaded: {downloaded_count}/{len(file_list)} files'))
                self.stdout.write(self.style.SUCCESS(f'✅ Processed: {processed_count} files'))

        except Exception as e:
            fetch_log.status = 'failed'
            fetch_log.error_message = str(e)
            fetch_log.completed_at = timezone.now()
            fetch_log.save()
            raise

    def _process_grib_file(self, forecast_run, domain, grib_path, time_step, valid_time):
        """Process a single GRIB file and extract all parameters"""
        parameters = Parameter.objects.filter(is_active=True)
        with GRIBProcessor(grib_path) as processor:
            for parameter in parameters:
                try:
                    data = processor.extract_parameter(parameter_code=parameter.code, apply_color_mapping=True)
                    if not data:
                        logger.warning(f'No data extracted for {parameter.code}')
                        continue

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
                    self.stdout.write(f'    {action}: {parameter.code}')

                except Exception as e:
                    logger.error(f'Error processing {parameter.code}: {e}')
                    self.stderr.write(self.style.ERROR(f'    ❌ {parameter.code}: {e}'))
