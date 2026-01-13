"""
Django REST Framework Views for WRF Data API
ON-DEMAND PROCESSING VERSION - No persistent storage required
File: wrf_data/views.py
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
import os
import tempfile
import shutil
import numpy as np

from .models import Domain, Parameter, ForecastRun, ForecastData, DataFetchLog
from .serializers import (
    DomainSerializer,
    ParameterSerializer,
    ForecastRunListSerializer,
    ForecastDataGridSerializer,
)

logger = logging.getLogger(__name__)


@api_view(["GET"])
def ping(request):
    """Health check endpoint"""
    return Response({
        "status": "ok",
        "message": "KMD Weather Backend is running (On-Demand Mode)",
        "timestamp": timezone.now().isoformat(),
        "version": "2.0.0-on-demand"
    })


class DomainViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet for Domain model"""
    queryset = Domain.objects.filter(is_active=True)
    serializer_class = DomainSerializer
    permission_classes = [AllowAny]


class ParameterViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet for Parameter model"""
    queryset = Parameter.objects.filter(is_active=True)
    serializer_class = ParameterSerializer
    permission_classes = [AllowAny]


class ForecastRunViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet for ForecastRun model"""
    queryset = ForecastRun.objects.all().order_by('-run_date', '-run_time')
    serializer_class = ForecastRunListSerializer
    permission_classes = [AllowAny]
    
    @action(detail=False, methods=['get'])
    def latest(self, request):
        """Get metadata about the latest available forecast"""
        # Get today's date and expected run time
        today = timezone.now().date()
        run_time = datetime.strptime(settings.WRF_CONFIG['BASE_TIME'], '%H:%M').time()
        
        # Return available metadata
        domains = Domain.objects.filter(is_active=True)
        parameters = Parameter.objects.filter(is_active=True)
        
        response_data = {
            'run_date': today.isoformat(),
            'run_time': run_time.isoformat(),
            'domains': DomainSerializer(domains, many=True).data,
            'parameters': ParameterSerializer(parameters, many=True).data,
            'available_timesteps': list(range(25)),  # 0-72 hours at 3-hour intervals
            'mode': 'on-demand',
            'note': 'Data is fetched and processed on-demand for each request'
        }
        
        return Response(response_data)
    
    @action(detail=False, methods=['get'])
    def get_data(self, request):
        """
        **ON-DEMAND DATA ENDPOINT**
        Fetches, processes, and returns data in a single request
        
        GET /api/forecasts/get_data/?date=2025-01-13&domain=kenya&parameter=rainfall&timestep=0
        
        Query params:
            - date: YYYY-MM-DD (default: today)
            - domain: kenya or east-africa
            - parameter: rainfall, temp-max, temp-min, rh, cape
            - timestep: 0-24 (0-72 hours in 3-hour intervals)
        
        Returns:
            Color-mapped grid data ready for visualization
        """
        # Parse and validate parameters
        date_str = request.query_params.get('date', timezone.now().date().isoformat())
        domain_code = request.query_params.get('domain')
        parameter_code = request.query_params.get('parameter')
        timestep = request.query_params.get('timestep')
        
        if not all([domain_code, parameter_code, timestep is not None]):
            return Response({
                "error": "Missing required parameters",
                "required": ["domain", "parameter", "timestep"],
                "optional": ["date"]
            }, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            fetch_date = datetime.fromisoformat(date_str).date()
            timestep = int(timestep)
            
            if timestep < 0 or timestep > 24:
                raise ValueError("Timestep must be between 0 and 24")
                
        except ValueError as e:
            return Response({
                "error": f"Invalid parameter: {str(e)}"
            }, status=status.HTTP_400_BAD_REQUEST)
        
        # Validate domain and parameter exist
        try:
            domain = Domain.objects.get(code=domain_code, is_active=True)
            parameter = Parameter.objects.get(code=parameter_code, is_active=True)
        except Domain.DoesNotExist:
            return Response({
                "error": f"Domain '{domain_code}' not found"
            }, status=status.HTTP_404_NOT_FOUND)
        except Parameter.DoesNotExist:
            return Response({
                "error": f"Parameter '{parameter_code}' not found"
            }, status=status.HTTP_404_NOT_FOUND)
        
        # Generate cache key
        cache_key = f"wrf_data_{date_str}_{domain_code}_{parameter_code}_{timestep}"
        
        # Check cache first (15 minute TTL)
        cached_data = cache.get(cache_key)
        if cached_data:
            logger.info(f"✓ Cache hit: {cache_key}")
            return Response(cached_data)
        
        logger.info(f"📊 Processing request: {fetch_date} | {domain_code} | {parameter_code} | T+{timestep*3}h")
        
        # Process data on-demand
        try:
            data = self._fetch_and_process(
                fetch_date=fetch_date,
                domain=domain,
                parameter=parameter,
                timestep=timestep
            )
            
            if not data:
                return Response({
                    "error": "Failed to process data",
                    "details": "Check server logs for more information"
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            # Cache the result (15 minutes)
            cache.set(cache_key, data, 900)
            
            logger.info(f"✓ Successfully processed and cached: {cache_key}")
            return Response(data)
            
        except Exception as e:
            logger.error(f"❌ Error processing data: {e}", exc_info=True)
            return Response({
                "error": "Processing failed",
                "details": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _fetch_and_process(self, fetch_date, domain, parameter, timestep):
        """
        Fetch GRIB file, process it, and return data
        Uses temporary storage - files are deleted after processing
        """
        from .utils.ssh_fetcher import create_fetcher_from_config
        from .utils.grib_processor import GRIBProcessor
        from .utils.color_mapper import get_mapper_for_parameter
        
        temp_dir = None
        
        try:
            # Create temporary directory
            temp_dir = tempfile.mkdtemp(prefix='wrf_temp_')
            logger.info(f"📁 Created temp directory: {temp_dir}")
            
            # Calculate run datetime
            run_time = datetime.strptime(settings.WRF_CONFIG['BASE_TIME'], '%H:%M').time()
            run_datetime = datetime.combine(fetch_date, run_time)
            
            # Calculate which GRIB file we need
            forecast_hour = timestep * 3
            grib_filename = f'WRFPRS_d{domain.file_suffix}.{forecast_hour:02d}'
            
            # Connect and download specific file
            logger.info(f"📥 Fetching: {grib_filename}")
            fetcher = create_fetcher_from_config(settings.WRF_CONFIG)
            
            if not fetcher.connect():
                raise ConnectionError("Failed to connect to WRF server")
            
            try:
                # Get remote path
                folder_name = fetcher.get_forecast_folder_name(run_datetime)
                remote_path = f"{fetcher.remote_archive_path}/{folder_name}/{grib_filename}"
                local_path = os.path.join(temp_dir, grib_filename)
                
                # Download file
                success = fetcher.download_file(remote_path, local_path)
                
                if not success:
                    raise FileNotFoundError(f"Failed to download {grib_filename}")
                
                logger.info(f"✓ Downloaded to {local_path}")
                
            finally:
                fetcher.disconnect()
            
            # Process GRIB file
            logger.info(f"⚙️  Processing {grib_filename}")
            
            with GRIBProcessor(local_path) as processor:
                extracted_data = processor.extract_parameter(
                    parameter.code,
                    apply_color_mapping=False
                )
            
            if not extracted_data:
                raise ValueError(f"No data extracted for {parameter.code}")
            
            # Get arrays
            lats = np.array(extracted_data['lats'])
            lons = np.array(extracted_data['lons'])
            values = np.array(extracted_data['values'])
            
            # For cumulative parameters, we'd need to process all previous timesteps
            # For demo purposes, we'll just return the current timestep
            # TODO: Implement proper cumulative calculation if needed
            
            # Apply color mapping
            mapper = get_mapper_for_parameter(parameter.code)
            color_data = mapper.map_grid(values)
            
            # Calculate statistics
            valid_values = values[~np.isnan(values)]
            min_val = float(np.min(valid_values)) if len(valid_values) > 0 else None
            max_val = float(np.max(valid_values)) if len(valid_values) > 0 else None
            
            # Calculate valid time
            valid_time = timezone.make_aware(run_datetime + timedelta(hours=forecast_hour))
            
            # Prepare response
            result = {
                'domain': domain.code,
                'parameter': parameter.code,
                'parameter_name': parameter.name,
                'unit': parameter.unit,
                'time_step': timestep,
                'valid_time': valid_time.isoformat(),
                'grid_lats': lats.tolist(),
                'grid_lons': lons.tolist(),
                'color_data': color_data,
                'min_value': min_val,
                'max_value': max_val,
                'color_scale': parameter.color_scale,
            }
            
            return result
            
        finally:
            # CRITICAL: Clean up temporary files
            if temp_dir and os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                    logger.info(f"🗑️  Cleaned up temp directory: {temp_dir}")
                except Exception as e:
                    logger.warning(f"Failed to cleanup temp dir: {e}")


class ForecastDataViewSet(viewsets.ReadOnlyModelViewSet):
    """
    DEPRECATED - Use ForecastRunViewSet.get_data() instead
    This viewset is kept for backwards compatibility
    """
    queryset = ForecastData.objects.none()
    serializer_class = ForecastDataGridSerializer
    permission_classes = [AllowAny]
    
    def list(self, request):
        return Response({
            "message": "This endpoint is deprecated. Use /api/forecasts/get_data/ instead",
            "example": "/api/forecasts/get_data/?date=2025-01-13&domain=kenya&parameter=rainfall&timestep=0"
        }, status=status.HTTP_410_GONE)


class DataFetchLogViewSet(viewsets.ReadOnlyModelViewSet):
    """Minimal logging - not critical for on-demand mode"""
    queryset = DataFetchLog.objects.all().order_by('-started_at')[:10]
    permission_classes = [AllowAny]
    
    def list(self, request):
        return Response({
            "message": "Fetch logs are not maintained in on-demand mode",
            "note": "Data is fetched and processed in real-time for each request"
        })