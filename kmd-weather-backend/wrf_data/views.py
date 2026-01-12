"""
Django REST Framework Views for WRF Data API
File: wrf_data/views.py
"""

from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from django.shortcuts import get_object_or_404
from django.utils import timezone
from django.db.models import Q, Max
from django.core.cache import cache
from datetime import datetime, timedelta
import logging
from rest_framework.decorators import api_view
import numpy as np
import random


from .models import Domain, Parameter, ForecastRun, ForecastData, DataFetchLog
from .serializers import (
    DomainSerializer,
    ParameterSerializer,
    ForecastRunListSerializer,
    ForecastRunDetailSerializer,
    ForecastDataSerializer,
    ForecastDataMinimalSerializer,
    ForecastDataGridSerializer,
    DataFetchLogSerializer,
    LatestForecastSerializer,
    ForecastDataRequestSerializer,
    FetchTriggerSerializer,
)

logger = logging.getLogger(__name__)



class DomainViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for Domain model
    
    list: Get all active domains
    retrieve: Get a specific domain by ID
    """
    queryset = Domain.objects.filter(is_active=True)
    serializer_class = DomainSerializer
    permission_classes = [AllowAny]


class ParameterViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for Parameter model
    
    list: Get all active parameters
    retrieve: Get a specific parameter by ID
    """
    queryset = Parameter.objects.filter(is_active=True)
    serializer_class = ParameterSerializer
    permission_classes = [AllowAny]


class ForecastRunViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for ForecastRun model
    
    list: Get all forecast runs
    retrieve: Get details of a specific forecast run
    latest: Get the latest completed forecast run
    """
    queryset = ForecastRun.objects.all().order_by('-run_date', '-run_time')
    permission_classes = [AllowAny]
    
    def get_serializer_class(self):
        """Use different serializers for list vs detail"""
        if self.action == 'retrieve':
            return ForecastRunDetailSerializer
        return ForecastRunListSerializer
    
    def get_queryset(self):
        """Filter by status if provided"""
        queryset = super().get_queryset()
        
        status = self.request.query_params.get('status', None)
        if status:
            queryset = queryset.filter(status=status)
        
        return queryset
    
    @action(detail=False, methods=['get'])
    def latest(self, request):
        """
        Get the latest completed forecast run with all available data
        
        GET /api/forecasts/latest/
        
        Returns:
            {
                "forecast_run": {...},
                "domains": [...],
                "parameters": [...],
                "available_timesteps": [0, 1, 2, ...],
                "total_data_count": 150
            }
        """
        # Get latest completed forecast run
        latest_run = ForecastRun.objects.filter(
            status='completed'
        ).order_by('-run_date', '-run_time').first()
        
        if not latest_run:
            return Response(
                {"detail": "No completed forecast runs available"},
                status=status.HTTP_404_NOT_FOUND
            )
        
        # Get available domains and parameters
        domains = Domain.objects.filter(is_active=True)
        parameters = Parameter.objects.filter(is_active=True)
        
        # Get available timesteps
        timesteps = ForecastData.objects.filter(
            forecast_run=latest_run
        ).values_list('time_step', flat=True).distinct().order_by('time_step')
        
        # Count total data points
        total_count = ForecastData.objects.filter(forecast_run=latest_run).count()
        
        # Serialize response
        response_data = {
            'forecast_run': ForecastRunListSerializer(latest_run).data,
            'domains': DomainSerializer(domains, many=True).data,
            'parameters': ParameterSerializer(parameters, many=True).data,
            'available_timesteps': list(timesteps),
            'total_data_count': total_count,
        }
        
        return Response(response_data)
    
    @action(detail=True, methods=['get'])
    def data(self, request, pk=None):
        """
        Get forecast data for a specific run, domain, parameter, and timestep
        
        GET /api/forecasts/{id}/data/?domain=kenya&parameter=rainfall&timestep=0
        
        Query Parameters:
            - domain: Domain code ('kenya' or 'east-africa')
            - parameter: Parameter code ('rainfall', 'temp-max', etc.)
            - timestep: Time step (0-24)
        
        Returns:
            Forecast data with color-mapped grid
        """
        forecast_run = self.get_object()
        
        # Validate request parameters
        serializer = ForecastDataRequestSerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)
        
        domain_code = serializer.validated_data['domain']
        parameter_code = serializer.validated_data['parameter']
        timestep = serializer.validated_data['timestep']
        
        # Get domain and parameter objects
        domain = get_object_or_404(Domain, code=domain_code, is_active=True)
        parameter = get_object_or_404(Parameter, code=parameter_code, is_active=True)
        
        # Get forecast data
        forecast_data = get_object_or_404(
            ForecastData,
            forecast_run=forecast_run,
            domain=domain,
            parameter=parameter,
            time_step=timestep
        )
        
        # Serialize and return
        data = ForecastDataGridSerializer(forecast_data).data
        
        return Response(data)
    
    @action(detail=False, methods=['post'])
    def fetch(self, request):
        """
        Trigger a new data fetch from WRF server
        
        POST /api/forecasts/fetch/
        
        Body:
            {
                "force": false,  // Optional: force re-fetch even if data exists
                "date": "2024-12-15"  // Optional: specific date (default: today)
            }
        
        Returns:
            Status of fetch operation
        """
        serializer = FetchTriggerSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        force = serializer.validated_data.get('force', False)
        fetch_date = serializer.validated_data.get('date', timezone.now().date())
        
        # Import here to avoid circular imports
        from .tasks import fetch_and_process_wrf_data
        
        # Check if data already exists for this date
        if not force:
            existing_run = ForecastRun.objects.filter(
                run_date=fetch_date,
                status='completed'
            ).exists()
            
            if existing_run:
                return Response({
                    "status": "skipped",
                    "message": f"Data already exists for {fetch_date}. Use force=true to re-fetch."
                })
        
        # Trigger async task (if using Celery) or run synchronously
        try:
            # If you have Celery set up:
            # task = fetch_and_process_wrf_data.delay(fetch_date.isoformat())
            # return Response({
            #     "status": "queued",
            #     "task_id": task.id,
            #     "message": f"Data fetch queued for {fetch_date}"
            # })
            
            # For now, run synchronously (will block)
            logger.info(f"Triggering data fetch for {fetch_date}")
            
            # You'll implement this function in tasks.py
            result = fetch_and_process_wrf_data(fetch_date.isoformat())
            
            return Response({
                "status": "success" if result else "failed",
                "message": f"Data fetch {'completed' if result else 'failed'} for {fetch_date}"
            })
            
        except Exception as e:
            logger.error(f"Error triggering fetch: {e}")
            return Response({
                "status": "error",
                "message": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class ForecastDataViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for ForecastData model
    
    list: Get all forecast data (with filtering)
    retrieve: Get specific forecast data point
    """
    queryset = ForecastData.objects.all().select_related(
        'forecast_run', 'domain', 'parameter'
    )
    permission_classes = [AllowAny]
    
    def get_serializer_class(self):
        """Use minimal serializer for list, full for detail"""
        if self.action == 'list':
            return ForecastDataMinimalSerializer
        return ForecastDataSerializer
    
    def get_queryset(self):
        """
        Filter forecast data by query parameters
        
        Query params:
            - forecast_run: Forecast run ID
            - domain: Domain code
            - parameter: Parameter code
            - timestep: Time step
            - latest: If 'true', only return data from latest run
        """
        queryset = super().get_queryset()
        
        # Filter by latest run
        if self.request.query_params.get('latest', '').lower() == 'true':
            latest_run = ForecastRun.objects.filter(
                status='completed'
            ).order_by('-run_date', '-run_time').first()
            
            if latest_run:
                queryset = queryset.filter(forecast_run=latest_run)
        
        # Filter by forecast run
        forecast_run_id = self.request.query_params.get('forecast_run')
        if forecast_run_id:
            queryset = queryset.filter(forecast_run_id=forecast_run_id)
        
        # Filter by domain
        domain_code = self.request.query_params.get('domain')
        if domain_code:
            queryset = queryset.filter(domain__code=domain_code)
        
        # Filter by parameter
        parameter_code = self.request.query_params.get('parameter')
        if parameter_code:
            queryset = queryset.filter(parameter__code=parameter_code)
        
        # Filter by timestep
        timestep = self.request.query_params.get('timestep')
        if timestep is not None:
            queryset = queryset.filter(time_step=int(timestep))
        
        return queryset.order_by('forecast_run', 'domain', 'parameter', 'time_step')


class DataFetchLogViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for DataFetchLog model
    
    list: Get all fetch logs
    retrieve: Get specific fetch log
    """
    queryset = DataFetchLog.objects.all().order_by('-started_at')
    serializer_class = DataFetchLogSerializer
    permission_classes = [AllowAny]
    
    def get_queryset(self):
        """Filter by status if provided"""
        queryset = super().get_queryset()
        
        status = self.request.query_params.get('status')
        if status:
            queryset = queryset.filter(status=status)
        
        return queryset


@api_view(["GET"])
def ping(request):
    return Response({
        "status": "ok",
        "message": "backend is alive"
    })


@api_view(['GET'])
def get_raw_grib_data(request):
    """
    GRIB data extraction endpoint with stratified random sampling
    Eliminates grid line artifacts by breaking regular sampling patterns
    
    GET /api/test-grib/?parameter=rainfall&timestep=0&domain=kenya
    """
    from .utils.grib_processor import GRIBProcessor
    from .utils.color_mapper import get_mapper_for_parameter
    import os
    from django.conf import settings
    import time
    
    start_time = time.time()
    
    parameter = request.GET.get('parameter', 'rainfall')
    timestep = int(request.GET.get('timestep', 0))
    domain = request.GET.get('domain', 'kenya')
    
    # Create cache key
    cache_key = f'grib_data_{domain}_{parameter}_{timestep}_v2'  # v2 for new sampling
    
    # Check cache first (1 hour TTL)
    cached_data = cache.get(cache_key)
    if cached_data:
        cache_time = time.time() - start_time
        logger.info(f'✓ Cache HIT: {cache_key} ({cache_time*1000:.0f}ms)')
        return Response(cached_data)
    
    logger.info(f'✗ Cache MISS: {cache_key} - Processing GRIB file')
    
    # Determine domain suffix
    domain_suffix = '01' if domain == 'kenya' else '02'
    
    # Find latest data
    data_dir = os.path.join(settings.BASE_DIR, 'data', 'raw')
    if not os.path.exists(data_dir):
        return Response({'error': 'No data directory found'}, status=status.HTTP_404_NOT_FOUND)
        
    dates = sorted([d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))])
    if not dates:
        return Response({'error': 'No data available'}, status=status.HTTP_404_NOT_FOUND)
        
    latest_date = dates[-1]
    grib_file = os.path.join(data_dir, latest_date, f'WRFPRS_d{domain_suffix}.{timestep:02d}')
    
    if not os.path.exists(grib_file):
        available_files = []
        date_dir = os.path.join(data_dir, latest_date)
        if os.path.exists(date_dir):
            available_files = sorted([f for f in os.listdir(date_dir) if f.startswith('WRFPRS')])
        
        return Response({
            'error': f'GRIB file not found: WRFPRS_d{domain_suffix}.{timestep:02d}',
            'available_files': available_files,
            'help': f'Only {len(available_files)} timesteps available for {latest_date}'
        }, status=status.HTTP_404_NOT_FOUND)
    
    try:
        # Get color mapper first (before opening GRIB)
        mapper = get_mapper_for_parameter(parameter)
        
        # Base downsampling factor
        base_step = 10 if domain == 'kenya' else 6
        
        grib_start = time.time()
        
        with GRIBProcessor(grib_file) as processor:
            data = processor.extract_parameter(parameter, apply_color_mapping=False)
            
            if not data:
                return Response({
                    'error': f'Parameter {parameter} not found in GRIB file'
                }, status=status.HTTP_404_NOT_FOUND)
        
        grib_time = time.time() - grib_start
        logger.info(f'  GRIB extraction: {grib_time*1000:.0f}ms')
        
        process_start = time.time()
        
        lats = np.array(data['lats'])
        lons = np.array(data['lons'])
        values = np.array(data['values'])
        
        # STRATIFIED RANDOM SAMPLING - Eliminates grid line artifacts!
        points = []
        rows, cols = lats.shape
        
        logger.info(f'  Using stratified random sampling (base_step={base_step})')
        
        # Divide into grid cells, sample randomly within each cell
        for i in range(0, rows - base_step, base_step):
            for j in range(0, cols - base_step, base_step):
                # Randomly pick one point within this grid cell
                sample_i = i + random.randint(0, base_step - 1)
                sample_j = j + random.randint(0, base_step - 1)
                
                # Ensure we don't go out of bounds
                sample_i = min(sample_i, rows - 1)
                sample_j = min(sample_j, cols - 1)
                
                lat = float(lats[sample_i, sample_j])
                lon = float(lons[sample_i, sample_j])
                value = float(values[sample_i, sample_j])
                
                if not np.isnan(value):
                    color = mapper.map_value(value)
                    points.append({
                        'lat': lat,
                        'lon': lon,
                        'value': value,
                        'color': color
                    })
        
        # Add extra random points for better coverage (10% more)
        extra_samples = max(50, int(len(points) * 0.1))
        logger.info(f'  Adding {extra_samples} extra random points')
        
        for _ in range(extra_samples):
            rand_i = random.randint(0, rows - 1)
            rand_j = random.randint(0, cols - 1)
            
            lat = float(lats[rand_i, rand_j])
            lon = float(lons[rand_i, rand_j])
            value = float(values[rand_i, rand_j])
            
            if not np.isnan(value):
                color = mapper.map_value(value)
                points.append({
                    'lat': lat,
                    'lon': lon,
                    'value': value,
                    'color': color
                })
        
        process_time = time.time() - process_start
        logger.info(f'  Data processing: {process_time*1000:.0f}ms')
        
        response_data = {
            'success': True,
            'file': os.path.basename(grib_file),
            'parameter': parameter,
            'timestep': timestep,
            'domain': domain,
            'points': points,
            'metadata': {
                'name': processor.PARAMETER_MAPPING[parameter]['name'],
                'unit': data['metadata'].get('units', 'unknown'),
                'min_value': float(data['metadata']['min']),
                'max_value': float(data['metadata']['max']),
                'mean_value': float(data['metadata']['mean']),
                'original_shape': [int(lats.shape[0]), int(lats.shape[1])],
                'total_points': len(points),
                'downsample_factor': base_step,
                'sampling_method': 'stratified_random'
            },
            'legend': mapper.get_legend_items()
        }
        
        # Cache for 1 hour
        cache.set(cache_key, response_data, 3600)
        
        total_time = time.time() - start_time
        logger.info(f'✓ Complete: {cache_key} - {len(points)} points in {total_time*1000:.0f}ms (cached)')
        
        return Response(response_data)
        
    except Exception as e:
        logger.error(f"✗ Error processing GRIB: {e}", exc_info=True)
        return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)