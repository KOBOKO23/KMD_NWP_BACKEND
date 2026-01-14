
"""
Celery configuration for KMD Weather Backend
File: config/celery.py

Place this file in: kmd-weather-backend/config/celery.py
"""

import os
from celery import Celery
from celery.schedules import crontab

# Set default Django settings module
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

# Create Celery app
app = Celery('kmd_weather')

# Load configuration from Django settings with CELERY_ prefix
app.config_from_object('django.conf:settings', namespace='CELERY')

# Auto-discover tasks in all installed apps
app.autodiscover_tasks()


# Celery Beat Schedule (for periodic tasks)
app.conf.beat_schedule = {
    # Fetch WRF data daily at 10:00 AM EAT (after model run completes at 09:00)
    'fetch-daily-wrf-data': {
        'task': 'wrf_data.tasks.daily_forecast_fetch',
        'schedule': crontab(hour=10, minute=0),  # 10:00 AM daily
        'options': {
            'expires': 3600,  # Task expires after 1 hour
        }
    },
    
    # Clean up old forecasts weekly (every Sunday at 2:00 AM)
    'cleanup-old-forecasts': {
        'task': 'wrf_data.tasks.cleanup_old_forecasts',
        'schedule': crontab(hour=2, minute=0, day_of_week=0),  # Sunday 2:00 AM
        'kwargs': {'days_to_keep': 7},
    },
}

# Celery Configuration Options
app.conf.update(
    # Task settings
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Africa/Nairobi',  # EAT timezone
    enable_utc=False,
    
    # Result backend (optional - stores task results)
    result_expires=3600,  # Results expire after 1 hour
    
    # Task execution settings
    task_track_started=True,
    task_time_limit=3600,  # 1 hour hard limit
    task_soft_time_limit=3000,  # 50 minutes soft limit
    
    # Worker settings
    worker_prefetch_multiplier=1,  # One task at a time
    worker_max_tasks_per_child=50,  # Restart worker after 50 tasks
    
    # Retry settings
    task_default_retry_delay=300,  # 5 minutes
    task_max_retries=3,
)


@app.task(bind=True)
def debug_task(self):
    """Debug task to test Celery setup"""
    print(f'Request: {self.request!r}')