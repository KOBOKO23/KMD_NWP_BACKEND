"""
Celery tasks for WRF data fetching and processing
File: wrf_data/tasks.py
PRODUCTION VERSION
"""

from celery import shared_task
from django.utils import timezone
from django.conf import settings
from datetime import datetime, timedelta
import logging
import os

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=3)
def fetch_wrf_data_task(self, date_str=None):
    """
    Celery task to fetch and process WRF data from SSH server
    
    Args:
        date_str: Date string in ISO format (YYYY-MM-DD). If None, uses today.
    
    Returns:
        dict: Status information about the fetch operation
    """
    from .models import ForecastRun, ForecastData, Domain, Parameter, DataFetchLog
    from .utils.ssh_fetcher import create_fetcher_from_config
    from .utils.grib_processor import GRIBProcessor
    from .utils.color_mapper import get_mapper_for_parameter
    import numpy as np
    
    try:
        # Parse date
        if date_str:
            fetch_date = datetime.fromisoformat(date_str).date()
        else:
            fetch_date = timezone.now().date()
        
        # Set run time to 19:00 (7 PM EAT - when WRF runs)
        run_time = datetime.strptime(settings.WRF_CONFIG['BASE_TIME'], '%H:%M').time()
        run_datetime = datetime.combine(fetch_date, run_time)
        
        logger.info(f"🚀 Starting WRF data fetch for: {fetch_date} at {run_time}")
        
        # Create or get ForecastRun
        forecast_run, created = ForecastRun.objects.get_or_create(
            run_date=fetch_date,
            run_time=run_time,
            defaults={
                'status': 'fetching',
                'progress': 0,
                'initialization_time': timezone.make_aware(run_datetime),
                'forecast_hours': settings.WRF_CONFIG['FORECAST_HOURS'],
            }
        )
        
        if not created:
            forecast_run.status = 'fetching'
            forecast_run.progress = 0
            forecast_run.error_message = ''
            forecast_run.save()
        
        # Create fetch log
        fetch_log = DataFetchLog.objects.create(
            forecast_run=forecast_run,
            status='success',
            ssh_host=settings.WRF_CONFIG['SSH_HOST'],
            ssh_user=settings.WRF_CONFIG['SSH_USERNAME'],
        )
        
        # Initialize SSH fetcher
        fetcher = create_fetcher_from_config(settings.WRF_CONFIG)
        
        # Download GRIB files
        logger.info("📥 Connecting to SSH server...")
        download_results = fetcher.download_forecast_run(
            run_date=run_datetime,
            domain='both',
            max_hours=settings.WRF_CONFIG['FORECAST_HOURS']
        )
        
        # Update fetch log
        fetch_log.files_requested = download_results['total']
        fetch_log.files_downloaded = len(download_results['success'])
        fetch_log.completed_at = timezone.now()
        
        if len(download_results['failed']) > 0:
            fetch_log.status = 'partial'
            fetch_log.error_message = f"Failed to download {len(download_results['failed'])} files"
        
        fetch_log.save()
        
        # Update forecast run status
        forecast_run.status = 'processing'
        forecast_run.progress = 30
        forecast_run.save()
        
        # Process downloaded GRIB files
        logger.info("⚙️  Processing GRIB files...")
        
        domains = Domain.objects.filter(is_active=True)
        parameters = Parameter.objects.filter(is_active=True)
        
        local_folder = settings.WRF_CONFIG['LOCAL_DATA_PATH'] / download_results['run_folder']
        
        processed_count = 0
        total_steps = len(domains) * len(parameters) * 25  # 25 timesteps
        
        for domain in domains:
            logger.info(f"  Processing domain: {domain.name}")
            
            for parameter in parameters:
                logger.info(f"    Processing parameter: {parameter.name}")
                
                # Track cumulative values for parameters that need it
                previous_values = None
                
                for timestep in range(25):  # 0-72 hours at 3-hour intervals
                    hour = timestep * 3
                    
                    try:
                        # Construct GRIB file path
                        domain_suffix = domain.file_suffix
                        grib_filename = f'WRFPRS_d{domain_suffix}.{hour:02d}'
                        grib_file = local_folder / grib_filename
                        
                        if not grib_file.exists():
                            logger.warning(f"      ⚠️  File not found: {grib_filename}")
                            continue
                        
                        # Extract data from GRIB
                        with GRIBProcessor(str(grib_file)) as processor:
                            data = processor.extract_parameter(
                                parameter.code,
                                apply_color_mapping=False
                            )
                        
                        if not data:
                            logger.warning(f"      ⚠️  No data extracted for {parameter.code}")
                            continue
                        
                        # Get arrays
                        lats = np.array(data['lats'])
                        lons = np.array(data['lons'])
                        values = np.array(data['values'])
                        
                        # Apply cumulative/running aggregation
                        if parameter.code == 'rainfall':
                            # Rainfall: cumulative sum
                            if previous_values is None:
                                previous_values = np.zeros_like(values)
                            values = previous_values + values
                            previous_values = values.copy()
                        
                        elif parameter.code == 'temp-max':
                            # Max temperature: running maximum
                            if previous_values is None:
                                previous_values = values.copy()
                            else:
                                values = np.maximum(previous_values, values)
                                previous_values = values.copy()
                        
                        elif parameter.code == 'temp-min':
                            # Min temperature: running minimum
                            if previous_values is None:
                                previous_values = values.copy()
                            else:
                                values = np.minimum(previous_values, values)
                                previous_values = values.copy()
                        
                        # Apply color mapping
                        mapper = get_mapper_for_parameter(parameter.code)
                        color_data = mapper.map_grid(values)
                        
                        # Calculate statistics
                        valid_values = values[~np.isnan(values)]
                        min_val = float(np.min(valid_values)) if len(valid_values) > 0 else None
                        max_val = float(np.max(valid_values)) if len(valid_values) > 0 else None
                        mean_val = float(np.mean(valid_values)) if len(valid_values) > 0 else None
                        
                        # Calculate valid time
                        valid_time = timezone.make_aware(
                            run_datetime + timedelta(hours=hour)
                        )
                        
                        # Save to database
                        ForecastData.objects.update_or_create(
                            forecast_run=forecast_run,
                            domain=domain,
                            parameter=parameter,
                            time_step=timestep,
                            defaults={
                                'valid_time': valid_time,
                                'grid_lats': lats.tolist(),
                                'grid_lons': lons.tolist(),
                                'values': values.tolist(),
                                'color_data': color_data,
                                'min_value': min_val,
                                'max_value': max_val,
                                'mean_value': mean_val,
                                'source_file': grib_filename,
                            }
                        )
                        
                        processed_count += 1
                        
                        # Update progress
                        progress = 30 + int((processed_count / total_steps) * 70)
                        forecast_run.progress = progress
                        forecast_run.save()
                        
                        logger.info(f"      ✓ Processed T+{hour}h")
                    
                    except Exception as e:
                        logger.error(f"      ❌ Error processing timestep {hour}h: {e}")
                        continue
        
        # Mark as completed
        forecast_run.status = 'completed'
        forecast_run.progress = 100
        forecast_run.completed_at = timezone.now()
        forecast_run.save()
        
        logger.info(f"✅ WRF data fetch completed for {fetch_date}")
        logger.info(f"   Processed {processed_count} data points")
        
        return {
            'status': 'success',
            'date': fetch_date.isoformat(),
            'forecast_run_id': forecast_run.id,
            'processed_count': processed_count,
            'message': f'Successfully fetched and processed WRF data for {fetch_date}'
        }
    
    except Exception as e:
        logger.error(f"❌ Error in fetch_wrf_data_task: {e}", exc_info=True)
        
        # Update forecast run status
        if 'forecast_run' in locals():
            forecast_run.status = 'failed'
            forecast_run.error_message = str(e)
            forecast_run.save()
        
        # Update fetch log
        if 'fetch_log' in locals():
            fetch_log.status = 'failed'
            fetch_log.error_message = str(e)
            fetch_log.completed_at = timezone.now()
            fetch_log.save()
        
        # Retry task
        raise self.retry(exc=e, countdown=300)  # Retry after 5 minutes


@shared_task
def cleanup_old_data(days_to_keep=7):
    """
    Clean up old forecast data and GRIB files
    
    Args:
        days_to_keep: Number of days of data to retain
    """
    from .models import ForecastRun
    
    try:
        cutoff_date = timezone.now().date() - timedelta(days=days_to_keep)
        
        # Delete old forecast runs (cascade deletes ForecastData)
        old_runs = ForecastRun.objects.filter(run_date__lt=cutoff_date)
        count = old_runs.count()
        old_runs.delete()
        
        # Delete old GRIB files
        data_path = settings.WRF_CONFIG['LOCAL_DATA_PATH']
        deleted_folders = 0
        
        if data_path.exists():
            for folder in data_path.iterdir():
                if folder.is_dir() and len(folder.name) == 10:  # YYYYMMDDHH format
                    try:
                        folder_date = datetime.strptime(folder.name[:8], '%Y%m%d').date()
                        if folder_date < cutoff_date:
                            import shutil
                            shutil.rmtree(folder)
                            deleted_folders += 1
                            logger.info(f"Deleted old GRIB folder: {folder.name}")
                    except Exception as e:
                        logger.warning(f"Error deleting folder {folder.name}: {e}")
        
        logger.info(f"✅ Cleanup complete: {count} forecast runs, {deleted_folders} GRIB folders")
        
        return {
            'status': 'success',
            'deleted_runs': count,
            'deleted_folders': deleted_folders,
            'cutoff_date': cutoff_date.isoformat()
        }
    
    except Exception as e:
        logger.error(f"❌ Error in cleanup_old_data: {e}", exc_info=True)
        raise


@shared_task
def daily_forecast_fetch():
    """
    Daily scheduled task to fetch latest WRF data
    Run this task every day at 10:00 PM EAT (3 hours after model run at 7 PM)
    """
    today = timezone.now().date()
    return fetch_wrf_data_task.delay(today.isoformat())