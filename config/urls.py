"""
Main URL Configuration
File: config/urls.py
"""

from django.contrib import admin
from django.urls import path, include
from django.http import JsonResponse
from django.conf import settings

def api_root(request):
    """API root endpoint"""
    return JsonResponse({
        'message': 'KMD NWP System API',
        'version': '1.0',
        'endpoints': {
            'admin': '/admin/',
            'api': '/api/',
            'domains': '/api/domains/',
            'parameters': '/api/parameters/',
            'forecasts': '/api/forecasts/',
            'latest_forecast': '/api/forecasts/latest/',
            'fetch_data': '/api/forecasts/fetch/',
            'forecast_data': '/api/forecast-data/',
            'fetch_logs': '/api/fetch-logs/',
        }
    })

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', include('wrf_data.urls')),
    path('', api_root),
]

if settings.DEBUG:
    from django.conf import settings
    from django.conf.urls.static import static

    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
