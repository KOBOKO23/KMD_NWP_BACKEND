"""
Django REST Framework Views for WRF Data API
File: wrf_data/views.py
PRODUCTION VERSION - All dummy data removed
"""

from rest_framework import viewsets, status
from rest_framework.decorators import action, api_view
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from django.shortcuts import get_object_or_404
from django.utils import timezone
from django.core.cache import cache
from django.conf import settings
from datetime import datetime, timedelta
import logging

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
    ForecastDataRequestSerializer,
    FetchTriggerSerializer,
)

logger = logging.getLogger(__name__)


@api_view(["GET"])
def ping(request):
    """Health check endpoint"""
    return Response({
        "status": "ok",
        "message": "KMD Weather Backend is running",
        "timestamp": timezone.now().isoformat(),
        "version": "1.0.0"
    })


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
        
        status_filter = self.request.query_params.get('status', None)
        if status_filter:
            queryset = queryset.filter(status=status_filter)
        
        return queryset
    
    @action(detail=False, methods=['get'])
    def latest(self, request):
        """
        Get the latest completed forecast run with all available data
        
        GET /api/forecasts/latest/
        """
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
                "force": false,
                "date": "2025-01-13"  // Optional: specific date (default: today)
            }
        
        Requires: CRON_SECRET_TOKEN in Authorization header for security
        """
        # Security check
        token = request.headers.get('Authorization', '').replace('Bearer ', '')
        expected_token = settings.CRON_SECRET_TOKEN
        
        if token != expected_token:
            return Response({
                "error": "Unauthorized - Invalid token"
            }, status=status.HTTP_401_UNAUTHORIZED)
        
        serializer = FetchTriggerSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        force = serializer.validated_data.get('force', False)
        fetch_date = serializer.validated_data.get('date', timezone.now().date())
        
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
        
        # Queue the fetch task
        try:
            from .tasks import fetch_wrf_data_task
            
            # If Celery is available, use async task
            if settings.CELERY_BROKER_URL:
                task = fetch_wrf_data_task.delay(fetch_date.isoformat())
                return Response({
                    "status": "queued",
                    "task_id": str(task.id),
                    "date": fetch_date.isoformat(),
                    "message": f"Data fetch queued for {fetch_date}"
                })
            else:
                # Fallback to synchronous execution (not recommended for production)
                logger.warning("Celery not configured - running fetch synchronously")
                result = fetch_wrf_data_task(fetch_date.isoformat())
                
                return Response({
                    "status": "completed" if result else "failed",
                    "date": fetch_date.isoformat(),
                    "message": f"Data fetch {'completed' if result else 'failed'} for {fetch_date}"
                })
            
        except Exception as e:
            logger.error(f"Error triggering fetch: {e}", exc_info=True)
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
        
        status_filter = self.request.query_params.get('status')
        if status_filter:
            queryset = queryset.filter(status=status_filter)
        
        return queryset