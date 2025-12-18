"""
Django Models for WRF Data Storage
File: wrf_data/models.py
"""

from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils import timezone
import json


class Domain(models.Model):
    """
    Represents a WRF model domain (Kenya or East Africa)
    """
    name = models.CharField(max_length=50, unique=True)
    code = models.CharField(max_length=20, unique=True)  # 'kenya' or 'east-africa'
    resolution_km = models.FloatField()
    grid_points_x = models.IntegerField()
    grid_points_y = models.IntegerField()
    center_lat = models.FloatField()
    center_lon = models.FloatField()
    min_lat = models.FloatField()
    max_lat = models.FloatField()
    min_lon = models.FloatField()
    max_lon = models.FloatField()
    file_suffix = models.CharField(max_length=10)  # '01' for Kenya, '02' for East Africa
    description = models.TextField(blank=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['name']
        
    def __str__(self):
        return f"{self.name} ({self.resolution_km}km)"


class Parameter(models.Model):
    """
    Represents a weather parameter (rainfall, temperature, etc.)
    """
    name = models.CharField(max_length=100)
    code = models.CharField(max_length=50, unique=True)  # 'rainfall', 'temp-max', etc.
    unit = models.CharField(max_length=20)
    grib_variable_name = models.CharField(max_length=100)  # Variable name in GRIB file
    description = models.TextField(blank=True)
    min_value = models.FloatField(null=True, blank=True)
    max_value = models.FloatField(null=True, blank=True)
    color_scale = models.JSONField(default=list)  # Color scale configuration
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['name']
        
    def __str__(self):
        return f"{self.name} ({self.unit})"


class ForecastRun(models.Model):
    """
    Represents a single WRF model forecast run
    """
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('fetching', 'Fetching Data'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    run_date = models.DateField()  # Date of the model run
    run_time = models.TimeField()  # Time of the model run (usually 09:00 local)
    initialization_time = models.DateTimeField()  # Full datetime of initialization
    forecast_hours = models.IntegerField(default=72)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    progress = models.IntegerField(default=0, validators=[MinValueValidator(0), MaxValueValidator(100)])
    error_message = models.TextField(blank=True)
    
    # Metadata
    model_version = models.CharField(max_length=50, default='WRF-ARW v4.5')
    physics_scheme = models.CharField(max_length=100, default='Thompson/RRTMG')
    
    # File tracking
    files_downloaded = models.JSONField(default=dict)  # Track which files were downloaded
    processing_log = models.JSONField(default=list)  # Log of processing steps
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    
    class Meta:
        ordering = ['-run_date', '-run_time']
        unique_together = ['run_date', 'run_time']
        indexes = [
            models.Index(fields=['-run_date', '-run_time']),
            models.Index(fields=['status']),
        ]
        
    def __str__(self):
        return f"Forecast Run {self.run_date} {self.run_time} - {self.status}"
    
    def is_latest(self):
        """Check if this is the latest forecast run"""
        latest = ForecastRun.objects.filter(status='completed').order_by('-run_date', '-run_time').first()
        return latest and latest.id == self.id


class ForecastData(models.Model):
    """
    Stores processed forecast data for a specific domain, parameter, and time step
    """
    forecast_run = models.ForeignKey(ForecastRun, on_delete=models.CASCADE, related_name='data')
    domain = models.ForeignKey(Domain, on_delete=models.CASCADE)
    parameter = models.ForeignKey(Parameter, on_delete=models.CASCADE)
    
    time_step = models.IntegerField(validators=[MinValueValidator(0), MaxValueValidator(24)])  # 0-24 (0-72 hours in 3hr steps)
    valid_time = models.DateTimeField()  # The actual forecast valid time
    
    # Grid data
    grid_lats = models.JSONField()  # 2D array of latitudes
    grid_lons = models.JSONField()  # 2D array of longitudes
    values = models.JSONField()  # 2D array of parameter values
    
    # Color-mapped data (ready for frontend)
    color_data = models.JSONField()  # 2D array of color values based on color scale
    
    # Statistics
    min_value = models.FloatField(null=True, blank=True)
    max_value = models.FloatField(null=True, blank=True)
    mean_value = models.FloatField(null=True, blank=True)
    
    # File reference
    source_file = models.CharField(max_length=255, blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        ordering = ['forecast_run', 'domain', 'parameter', 'time_step']
        unique_together = ['forecast_run', 'domain', 'parameter', 'time_step']
        indexes = [
            models.Index(fields=['forecast_run', 'domain', 'parameter', 'time_step']),
            models.Index(fields=['valid_time']),
        ]
        
    def __str__(self):
        return f"{self.forecast_run} - {self.domain.code} - {self.parameter.code} - T+{self.time_step*3}h"


class DataFetchLog(models.Model):
    """
    Logs all data fetch attempts from the WRF server
    """
    STATUS_CHOICES = [
        ('success', 'Success'),
        ('partial', 'Partial Success'),
        ('failed', 'Failed'),
    ]
    
    forecast_run = models.ForeignKey(ForecastRun, on_delete=models.CASCADE, related_name='fetch_logs', null=True, blank=True)
    started_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES)
    
    # Connection details
    ssh_host = models.CharField(max_length=255)
    ssh_user = models.CharField(max_length=100)
    
    # Transfer statistics
    files_requested = models.IntegerField(default=0)
    files_downloaded = models.IntegerField(default=0)
    total_bytes = models.BigIntegerField(default=0)
    
    # Logs
    log_messages = models.JSONField(default=list)
    error_message = models.TextField(blank=True)
    
    class Meta:
        ordering = ['-started_at']
        
    def __str__(self):
        return f"Fetch Log {self.started_at} - {self.status}"
    
    def add_log(self, message, level='info'):
        """Add a log message"""
        self.log_messages.append({
            'timestamp': timezone.now().isoformat(),
            'level': level,
            'message': message
        })
        self.save()


# Signal to create default domains and parameters
from django.db.models.signals import post_migrate
from django.dispatch import receiver

@receiver(post_migrate)
def create_default_data(sender, **kwargs):
    """
    Create default domains and parameters after migration
    """
    if sender.name == 'wrf_data':
        # Create domains if they don't exist
        Domain.objects.get_or_create(
            code='kenya',
            defaults={
                'name': 'Kenya',
                'resolution_km': 3.0,
                'grid_points_x': 200,
                'grid_points_y': 200,
                'center_lat': 0.0,
                'center_lon': 37.5,
                'min_lat': -5.0,
                'max_lat': 5.0,
                'min_lon': 33.0,
                'max_lon': 42.0,
                'file_suffix': '01',
                'description': 'Kenya High Resolution Domain (3km)'
            }
        )
        
        Domain.objects.get_or_create(
            code='east-africa',
            defaults={
                'name': 'East Africa',
                'resolution_km': 9.0,
                'grid_points_x': 300,
                'grid_points_y': 250,
                'center_lat': -3.0,
                'center_lon': 40.0,
                'min_lat': -12.0,
                'max_lat': 6.0,
                'min_lon': 28.0,
                'max_lon': 52.0,
                'file_suffix': '02',
                'description': 'East Africa Regional Domain (9km)'
            }
        )
        
        # Create parameters if they don't exist
        # Rainfall color scale (from your frontend)
        rainfall_colors = [
            {'min': 0, 'max': 1, 'color': '#ffffff'},
            {'min': 1, 'max': 2, 'color': '#d3ffbe'},
            {'min': 2, 'max': 11, 'color': '#55ff00'},
            {'min': 11, 'max': 21, 'color': '#73dfff'},
            {'min': 21, 'max': 51, 'color': '#00a9e6'},
            {'min': 51, 'max': 71, 'color': '#ffaa00'},
            {'min': 71, 'max': 101, 'color': '#ff5a00'},
            {'min': 101, 'max': 999, 'color': '#ff0000'},
        ]
        
        Parameter.objects.get_or_create(
            code='rainfall',
            defaults={
                'name': 'Accumulated Rainfall',
                'unit': 'mm',
                'grib_variable_name': 'APCP',  # Accumulated precipitation
                'description': 'Total accumulated rainfall',
                'min_value': 0,
                'max_value': 500,
                'color_scale': rainfall_colors
            }
        )
        
        # Max Temperature color scale
        temp_max_colors = [
            {'min': 0, 'max': 15, 'color': '#70a800'},
            {'min': 15, 'max': 16, 'color': '#98e600'},
            {'min': 16, 'max': 21, 'color': '#e6e600'},
            {'min': 21, 'max': 26, 'color': '#ffaa00'},
            {'min': 26, 'max': 31, 'color': '#ff5a00'},
            {'min': 31, 'max': 36, 'color': '#c00000'},
            {'min': 36, 'max': 50, 'color': '#800000'},
        ]
        
        Parameter.objects.get_or_create(
            code='temp-max',
            defaults={
                'name': 'Maximum Temperature',
                'unit': '°C',
                'grib_variable_name': 'T2MAX',  # 2m max temperature
                'description': 'Maximum temperature at 2 meters',
                'min_value': -10,
                'max_value': 50,
                'color_scale': temp_max_colors
            }
        )
        
        # Min Temperature color scale
        temp_min_colors = [
            {'min': 0, 'max': 5, 'color': '#08306b'},
            {'min': 5, 'max': 6, 'color': '#0066ff'},
            {'min': 6, 'max': 11, 'color': '#00a884'},
            {'min': 11, 'max': 16, 'color': '#70a800'},
            {'min': 16, 'max': 21, 'color': '#98e600'},
            {'min': 21, 'max': 26, 'color': '#e6e600'},
            {'min': 26, 'max': 40, 'color': '#ffaa00'},
        ]
        
        Parameter.objects.get_or_create(
            code='temp-min',
            defaults={
                'name': 'Minimum Temperature',
                'unit': '°C',
                'grib_variable_name': 'T2MIN',  # 2m min temperature
                'description': 'Minimum temperature at 2 meters',
                'min_value': -10,
                'max_value': 40,
                'color_scale': temp_min_colors
            }
        )
        
        # Relative Humidity (generic color scale)
        rh_colors = [
            {'min': 0, 'max': 20, 'color': '#8B4513'},
            {'min': 20, 'max': 40, 'color': '#D2691E'},
            {'min': 40, 'max': 60, 'color': '#F0E68C'},
            {'min': 60, 'max': 80, 'color': '#90EE90'},
            {'min': 80, 'max': 100, 'color': '#00CED1'},
        ]
        
        Parameter.objects.get_or_create(
            code='rh',
            defaults={
                'name': 'Relative Humidity',
                'unit': '%',
                'grib_variable_name': 'RH2',  # 2m relative humidity
                'description': 'Relative humidity at 2 meters',
                'min_value': 0,
                'max_value': 100,
                'color_scale': rh_colors
            }
        )
        
        # CAPE (generic color scale)
        cape_colors = [
            {'min': 0, 'max': 500, 'color': '#E0E0E0'},
            {'min': 500, 'max': 1000, 'color': '#FFFF99'},
            {'min': 1000, 'max': 2000, 'color': '#FFCC66'},
            {'min': 2000, 'max': 3000, 'color': '#FF9933'},
            {'min': 3000, 'max': 5000, 'color': '#FF3333'},
            {'min': 5000, 'max': 10000, 'color': '#CC0000'},
        ]
        
        Parameter.objects.get_or_create(
            code='cape',
            defaults={
                'name': 'CAPE',
                'unit': 'J/kg',
                'grib_variable_name': 'CAPE',  # Convective Available Potential Energy
                'description': 'Convective Available Potential Energy',
                'min_value': 0,
                'max_value': 10000,
                'color_scale': cape_colors
            }
        )
