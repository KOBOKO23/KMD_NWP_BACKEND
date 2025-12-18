"""
Celery tasks for WRF data fetching and processing
File: wrf_data/tasks.py

This file should be placed in your Django backend at:
kmd-weather-backend/wrf_data/tasks.py
"""

from celery import shared_task
from django.utils import timezone
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=3)
def fetch_and_process_wrf_data(self, date_str=None):
    """
    Celery task to fetch and process WRF data from SSH server
    
    Args:
        date_str: Date string in ISO format (YYYY-MM-DD). If None, uses today.
    
    Returns:
        dict: Status information about the fetch operation
    """
    from .models import ForecastRun, ForecastData, Domain, Parameter
    from .utils.ssh_fetcher import SSHFetcher
    from .utils.grib_processor import GRIBProcessor
    from .utils.color_mapper import ColorMapper
    
    try:
        # Parse date
        if date_str:
            fetch_date = datetime.fromisoformat(date_str).date()
        else:
            fetch_date = timezone.now().date()
        
        logger.info(f"Starting WRF data fetch for date: {fetch_date}")
        
        # Create or get ForecastRun
        forecast_run, created = ForecastRun.objects.get_or_create(
            run_date=fetch_date,
            defaults={
                'status': 'processing',
                'progress': 0
            }
        )
        
        if not created:
            forecast_run.status = 'processing'
            forecast_run.progress = 0
            forecast_run.save()
        
        # Initialize utilities
        ssh_fetcher = SSHFetcher()
        grib_processor = GRIBProcessor()
        color_mapper = ColorMapper()
        
        # Get all domains and parameters
        domains = Domain.objects.all()
        parameters = Parameter.objects.all()
        
        total_steps = len(domains) * len(parameters) * 25  # 25 timesteps (0-72h at 3h intervals)
        current_step = 0
        
        # Fetch and process data for each domain and parameter
        for domain in domains:
            logger.info(f"Processing domain: {domain.name}")
            
            for parameter in parameters:
                logger.info(f"Processing parameter: {parameter.name}")
                
                try:
                    # Construct remote file path
                    # Example: /data/wrf/2024/01/15/kenya/rainfall/rainfall_000.grb2
                    remote_base_path = f"/data/wrf/{fetch_date.year:04d}/{fetch_date.month:02d}/{fetch_date.day:02d}/{domain.code}/{parameter.code}"
                    
                    # Process each timestep (0 to 72 hours at 3-hour intervals)
                    for timestep in range(25):  # 0, 3, 6, ..., 72
                        hour = timestep * 3
                        
                        try:
                            # Remote file path
                            remote_file = f"{remote_base_path}/{parameter.code}_{hour:03d}.grb2"
                            
                            # Fetch GRIB file from SSH server
                            local_file = ssh_fetcher.fetch_file(
                                remote_path=remote_file,
                                local_dir=f"data/raw/{fetch_date}/{domain.code}/{parameter.code}"
                            )
                            
                            if local_file:
                                # Process GRIB to GeoJSON
                                geojson_data = grib_processor.process_grib_to_geojson(
                                    grib_file=local_file,
                                    parameter=parameter.code
                                )
                                
                                # Apply color mapping
                                colored_data = color_mapper.apply_colors(
                                    geojson_data,
                                    parameter=parameter.code
                                )
                                
                                # Save to database
                                ForecastData.objects.update_or_create(
                                    forecast_run=forecast_run,
                                    domain=domain,
                                    parameter=parameter,
                                    timestep=timestep,
                                    defaults={
                                        'data': colored_data,
                                        'valid_time': forecast_run.run_date + timedelta(hours=hour)
                                    }
                                )
                                
                                logger.info(f"Successfully processed {domain.code}/{parameter.code} timestep {hour}h")
                            else:
                                logger.warning(f"Failed to fetch {remote_file}")
                        
                        except Exception as e:
                            logger.error(f"Error processing timestep {hour}h for {domain.code}/{parameter.code}: {str(e)}")
                            # Continue with next timestep
                        
                        # Update progress
                        current_step += 1
                        progress = int((current_step / total_steps) * 100)
                        forecast_run.progress = progress
                        forecast_run.save()
                
                except Exception as e:
                    logger.error(f"Error processing parameter {parameter.code}: {str(e)}")
                    # Continue with next parameter
        
        # Mark as completed
        forecast_run.status = 'completed'
        forecast_run.progress = 100
        forecast_run.completed_at = timezone.now()
        forecast_run.save()
        
        logger.info(f"WRF data fetch completed for {fetch_date}")
        
        return {
            'status': 'success',
            'date': fetch_date.isoformat(),
            'forecast_run_id': forecast_run.id,
            'message': f'Successfully fetched and processed WRF data for {fetch_date}'
        }
    
    except Exception as e:
        logger.error(f"Error in fetch_and_process_wrf_data: {str(e)}")
        
        # Update forecast run status
        if 'forecast_run' in locals():
            forecast_run.status = 'failed'
            forecast_run.error_message = str(e)
            forecast_run.save()
        
        # Retry task
        raise self.retry(exc=e, countdown=300)  # Retry after 5 minutes


@shared_task
def cleanup_old_forecasts(days_to_keep=7):
    """
    Clean up old forecast data to save disk space
    
    Args:
        days_to_keep: Number of days of data to retain
    """
    from .models import ForecastRun
    
    try:
        cutoff_date = timezone.now().date() - timedelta(days=days_to_keep)
        
        old_runs = ForecastRun.objects.filter(run_date__lt=cutoff_date)
        count = old_runs.count()
        old_runs.delete()
        
        logger.info(f"Cleaned up {count} old forecast runs")
        
        return {
            'status': 'success',
            'deleted_count': count,
            'cutoff_date': cutoff_date.isoformat()
        }
    
    except Exception as e:
        logger.error(f"Error in cleanup_old_forecasts: {str(e)}")
        raise


@shared_task
def daily_forecast_fetch():
    """
    Daily scheduled task to fetch latest WRF data
    Run this task every day at 10:00 AM EAT (after model run completes)
    """
    from django.utils import timezone
    
    today = timezone.now().date()
    return fetch_and_process_wrf_data.delay(today.isoformat())


# Alternative: Synchronous version (if not using Celery)
def fetch_and_process_wrf_data_sync(date_str=None):
    """
    Synchronous version of fetch_and_process_wrf_data
    Use this if you don't have Celery set up
    
    Call from views.py like:
        from .tasks import fetch_and_process_wrf_data_sync
        result = fetch_and_process_wrf_data_sync(fetch_date.isoformat())
    """
    from .models import ForecastRun, ForecastData, Domain, Parameter
    from .utils.ssh_fetcher import SSHFetcher
    from .utils.grib_processor import GRIBProcessor
    from .utils.color_mapper import ColorMapper
    
    try:
        # Parse date
        if date_str:
            fetch_date = datetime.fromisoformat(date_str).date()
        else:
            fetch_date = timezone.now().date()
        
        logger.info(f"Starting WRF data fetch for date: {fetch_date}")
        
        # Create or get ForecastRun
        forecast_run, created = ForecastRun.objects.get_or_create(
            run_date=fetch_date,
            defaults={
                'status': 'processing',
                'progress': 0
            }
        )
        
        if not created:
            forecast_run.status = 'processing'
            forecast_run.progress = 0
            forecast_run.save()
        
        # Initialize utilities
        ssh_fetcher = SSHFetcher()
        grib_processor = GRIBProcessor()
        color_mapper = ColorMapper()
        
        # Get all domains and parameters
        domains = Domain.objects.all()
        parameters = Parameter.objects.all()
        
        total_steps = len(domains) * len(parameters) * 25
        current_step = 0
        
        # Process data
        for domain in domains:
            for parameter in parameters:
                remote_base_path = f"/data/wrf/{fetch_date.year:04d}/{fetch_date.month:02d}/{fetch_date.day:02d}/{domain.code}/{parameter.code}"
                
                for timestep in range(25):
                    hour = timestep * 3
                    
                    try:
                        remote_file = f"{remote_base_path}/{parameter.code}_{hour:03d}.grb2"
                        
                        local_file = ssh_fetcher.fetch_file(
                            remote_path=remote_file,
                            local_dir=f"data/raw/{fetch_date}/{domain.code}/{parameter.code}"
                        )
                        
                        if local_file:
                            geojson_data = grib_processor.process_grib_to_geojson(
                                grib_file=local_file,
                                parameter=parameter.code
                            )
                            
                            colored_data = color_mapper.apply_colors(
                                geojson_data,
                                parameter=parameter.code
                            )
                            
                            ForecastData.objects.update_or_create(
                                forecast_run=forecast_run,
                                domain=domain,
                                parameter=parameter,
                                timestep=timestep,
                                defaults={
                                    'data': colored_data,
                                    'valid_time': forecast_run.run_date + timedelta(hours=hour)
                                }
                            )
                    
                    except Exception as e:
                        logger.error(f"Error processing timestep {hour}h: {str(e)}")
                    
                    current_step += 1
                    forecast_run.progress = int((current_step / total_steps) * 100)
                    forecast_run.save()
        
        # Mark as completed
        forecast_run.status = 'completed'
        forecast_run.progress = 100
        forecast_run.completed_at = timezone.now()
        forecast_run.save()
        
        return {
            'status': 'success',
            'date': fetch_date.isoformat(),
            'forecast_run_id': forecast_run.id
        }
    
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        
        if 'forecast_run' in locals():
            forecast_run.status = 'failed'
            forecast_run.error_message = str(e)
            forecast_run.save()
        
        raise
