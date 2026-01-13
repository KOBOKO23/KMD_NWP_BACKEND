"""
Django REST Framework Serializers for WRF Data
File: wrf_data/serializers.py
"""

from rest_framework import serializers
from .models import Domain, Parameter, ForecastRun, ForecastData, DataFetchLog


class DomainSerializer(serializers.ModelSerializer):
    """
    Serializer for Domain model
    """
    class Meta:
        model = Domain
        fields = [
            'id', 'name', 'code', 'resolution_km', 
            'grid_points_x', 'grid_points_y',
            'center_lat', 'center_lon',
            'min_lat', 'max_lat', 'min_lon', 'max_lon',
            'description', 'is_active'
        ]
        read_only_fields = ['id']


class ParameterSerializer(serializers.ModelSerializer):
    """
    Serializer for Parameter model
    """
    class Meta:
        model = Parameter
        fields = [
            'id', 'name', 'code', 'unit', 'description',
            'min_value', 'max_value', 'color_scale', 'is_active'
        ]
        read_only_fields = ['id']


class ForecastRunListSerializer(serializers.ModelSerializer):
    """
    Serializer for listing forecast runs (minimal data)
    """
    class Meta:
        model = ForecastRun
        fields = [
            'id', 'run_date', 'run_time', 'initialization_time',
            'forecast_hours', 'status', 'progress',
            'model_version', 'physics_scheme',
            'created_at', 'updated_at', 'completed_at'
        ]
        read_only_fields = fields


class ForecastRunDetailSerializer(serializers.ModelSerializer):
    """
    Serializer for detailed forecast run information
    """
    class Meta:
        model = ForecastRun
        fields = '__all__'
        read_only_fields = ['id', 'created_at', 'updated_at']


class ForecastDataSerializer(serializers.ModelSerializer):
    """
    Serializer for forecast data (includes grid data and color mapping)
    """
    domain_info = DomainSerializer(source='domain', read_only=True)
    parameter_info = ParameterSerializer(source='parameter', read_only=True)
    
    class Meta:
        model = ForecastData
        fields = [
            'id', 'forecast_run', 'domain', 'parameter',
            'domain_info', 'parameter_info',
            'time_step', 'valid_time',
            'grid_lats', 'grid_lons', 'values', 'color_data',
            'min_value', 'max_value', 'mean_value',
            'source_file', 'created_at'
        ]
        read_only_fields = ['id', 'created_at']


class ForecastDataMinimalSerializer(serializers.ModelSerializer):
    """
    Minimal serializer for forecast data (without full grid data)
    Used for listing available data
    """
    domain_code = serializers.CharField(source='domain.code', read_only=True)
    parameter_code = serializers.CharField(source='parameter.code', read_only=True)
    parameter_name = serializers.CharField(source='parameter.name', read_only=True)
    parameter_unit = serializers.CharField(source='parameter.unit', read_only=True)
    
    class Meta:
        model = ForecastData
        fields = [
            'id', 'time_step', 'valid_time',
            'domain_code', 'parameter_code', 'parameter_name', 'parameter_unit',
            'min_value', 'max_value', 'mean_value'
        ]


class ForecastDataGridSerializer(serializers.ModelSerializer):
    """
    Serializer that returns ONLY the color-mapped grid data
    Optimized for frontend consumption
    """
    domain = serializers.CharField(source='domain.code')
    parameter = serializers.CharField(source='parameter.code')
    parameter_name = serializers.CharField(source='parameter.name')
    unit = serializers.CharField(source='parameter.unit')
    color_scale = serializers.JSONField(source='parameter.color_scale')
    
    class Meta:
        model = ForecastData
        fields = [
            'domain', 'parameter', 'parameter_name', 'unit',
            'time_step', 'valid_time',
            'grid_lats', 'grid_lons', 'color_data',
            'min_value', 'max_value', 'color_scale'
        ]


class DataFetchLogSerializer(serializers.ModelSerializer):
    """
    Serializer for data fetch logs
    """
    duration_seconds = serializers.SerializerMethodField()
    
    class Meta:
        model = DataFetchLog
        fields = [
            'id', 'forecast_run', 'started_at', 'completed_at',
            'duration_seconds', 'status', 'ssh_host', 'ssh_user',
            'files_requested', 'files_downloaded', 'total_bytes',
            'log_messages', 'error_message'
        ]
        read_only_fields = fields
    
    def get_duration_seconds(self, obj):
        """Calculate duration in seconds"""
        if obj.completed_at and obj.started_at:
            delta = obj.completed_at - obj.started_at
            return delta.total_seconds()
        return None


class LatestForecastSerializer(serializers.Serializer):
    """
    Serializer for the latest forecast response
    Returns forecast run info and all available data
    """
    forecast_run = ForecastRunListSerializer()
    domains = DomainSerializer(many=True)
    parameters = ParameterSerializer(many=True)
    available_timesteps = serializers.ListField(child=serializers.IntegerField())
    total_data_count = serializers.IntegerField()


class ForecastDataRequestSerializer(serializers.Serializer):
    """
    Serializer for validating forecast data requests
    """
    domain = serializers.CharField(required=True)
    parameter = serializers.CharField(required=True)
    timestep = serializers.IntegerField(required=True, min_value=0, max_value=24)
    
    def validate_domain(self, value):
        """Validate domain exists"""
        if not Domain.objects.filter(code=value, is_active=True).exists():
            raise serializers.ValidationError(f"Domain '{value}' not found or inactive")
        return value
    
    def validate_parameter(self, value):
        """Validate parameter exists"""
        if not Parameter.objects.filter(code=value, is_active=True).exists():
            raise serializers.ValidationError(f"Parameter '{value}' not found or inactive")
        return value


class FetchTriggerSerializer(serializers.Serializer):
    """
    Serializer for triggering a new data fetch
    """
    force = serializers.BooleanField(default=False, required=False)
    date = serializers.DateField(required=False)
    
    def validate_date(self, value):
        """Validate date is not in the future"""
        from django.utils import timezone
        if value and value > timezone.now().date():
            raise serializers.ValidationError("Cannot fetch data for future dates")
        return value
