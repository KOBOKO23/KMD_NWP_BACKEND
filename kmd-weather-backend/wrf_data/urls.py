"""
URL Configuration for WRF Data API
File: wrf_data/urls.py
"""

from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (
    DomainViewSet,
    ParameterViewSet,
    ForecastRunViewSet,
    ForecastDataViewSet,
    DataFetchLogViewSet,
)

# Create router and register viewsets
router = DefaultRouter()
router.register(r'domains', DomainViewSet, basename='domain')
router.register(r'parameters', ParameterViewSet, basename='parameter')
router.register(r'forecasts', ForecastRunViewSet, basename='forecast')
router.register(r'forecast-data', ForecastDataViewSet, basename='forecast-data')
router.register(r'fetch-logs', DataFetchLogViewSet, basename='fetch-log')

app_name = 'wrf_data'

urlpatterns = [
    path('', include(router.urls)),
]
