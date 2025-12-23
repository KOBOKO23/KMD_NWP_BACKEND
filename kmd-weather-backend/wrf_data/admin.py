"""
Django Admin Configuration for WRF Data
File: wrf_data/admin.py
"""

from django.contrib import admin
from django.utils.html import format_html
from django.http import HttpResponse
from .models import Domain, Parameter, ForecastRun, ForecastData, DataFetchLog
from .tasks import fetch_and_process_wrf_data # for retry action
from django.utils.safestring import mark_safe


# ----------------------
# DOMAIN ADMIN
# ----------------------
@admin.register(Domain)
class DomainAdmin(admin.ModelAdmin):
    list_display = ['name', 'code', 'resolution_km', 'grid_size', 'bounds', 'is_active_badge', 'updated_at']
    list_filter = ['is_active', 'resolution_km']
    search_fields = ['name', 'code', 'description']
    readonly_fields = ['created_at', 'updated_at']

    fieldsets = (
        ('Basic Information', {
            'fields': ('name', 'code', 'description', 'is_active')
        }),
        ('Grid Configuration', {
            'fields': ('resolution_km', 'grid_points_x', 'grid_points_y', 'file_suffix')
        }),
        ('Geographic Bounds', {
            'fields': (
                'center_lat', 'center_lon',
                ('min_lat', 'max_lat'),
                ('min_lon', 'max_lon')
            )
        }),
        ('Metadata', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )

    def grid_size(self, obj):
        return f"{obj.grid_points_x} × {obj.grid_points_y}"
    grid_size.short_description = 'Grid Size'

    def bounds(self, obj):
        return f"Lat: {obj.min_lat}°-{obj.max_lat}°, Lon: {obj.min_lon}°-{obj.max_lon}°"
    bounds.short_description = 'Bounds'

    def is_active_badge(self, obj):
        if obj.is_active:
            return mark_safe('<span style="color: green;">✓ Active</span>')
        return format_html('<span style="color: red;">✗ Inactive</span>')



# ----------------------
# PARAMETER ADMIN
# ----------------------
@admin.register(Parameter)
class ParameterAdmin(admin.ModelAdmin):
    list_display = ['name', 'code', 'unit', 'value_range', 'is_active_badge', 'updated_at']
    list_filter = ['is_active', 'unit']
    search_fields = ['name', 'code', 'grib_variable_name', 'description']
    readonly_fields = ['created_at', 'updated_at']

    fieldsets = (
        ('Basic Information', {
            'fields': ('name', 'code', 'unit', 'description', 'is_active')
        }),
        ('GRIB Configuration', {
            'fields': ('grib_variable_name',)
        }),
        ('Value Range', {
            'fields': ('min_value', 'max_value')
        }),
        ('Color Scale', {
            'fields': ('color_scale',),
            'description': 'JSON array defining color mapping for visualization'
        }),
        ('Metadata', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )

    def value_range(self, obj):
        if obj.min_value is not None and obj.max_value is not None:
            return f"{obj.min_value} - {obj.max_value} {obj.unit}"
        return "N/A"
    value_range.short_description = 'Range'

    def is_active_badge(self, obj):
        if obj.is_active:
            return format_html('{}', '<span style="color: green;">✓ Active</span>')
        return format_html('{}', '<span style="color: red;">✗ Inactive</span>')
    is_active_badge.short_description = 'Status'


# ----------------------
# FORECAST DATA INLINE
# ----------------------
class ForecastDataInline(admin.TabularInline):
    model = ForecastData
    extra = 0
    readonly_fields = ('domain', 'parameter', 'valid_time', 'min_value', 'max_value', 'mean_value')
    can_delete = False
    show_change_link = True


# ----------------------
# FORECAST RUN ADMIN
# ----------------------
@admin.register(ForecastRun)
class ForecastRunAdmin(admin.ModelAdmin):
    list_display = [
        'id', 'run_date', 'run_time', 'status_badge', 
        'progress_bar', 'forecast_hours', 'data_count', 'updated_at'
    ]
    list_filter = ['status', 'run_date', 'model_version']
    search_fields = ['run_date', 'error_message']
    readonly_fields = ['created_at', 'updated_at', 'completed_at', 'data_count']
    date_hierarchy = 'run_date'
    inlines = [ForecastDataInline]

    fieldsets = (
        ('Run Information', {
            'fields': ('run_date', 'run_time', 'initialization_time', 'forecast_hours')
        }),
        ('Status', {
            'fields': ('status', 'progress', 'error_message')
        }),
        ('Model Configuration', {
            'fields': ('model_version', 'physics_scheme')
        }),
        ('Tracking', {
            'fields': ('files_downloaded', 'processing_log'),
            'classes': ('collapse',)
        }),
        ('Metadata', {
            'fields': ('data_count', 'created_at', 'updated_at', 'completed_at'),
            'classes': ('collapse',)
        }),
    )

    def status_badge(self, obj):
        colors = {
            'pending': '#FFA500',
            'fetching': '#1E90FF',
            'processing': '#9370DB',
            'completed': '#32CD32',
            'failed': '#DC143C',
        }
        color = colors.get(obj.status, '#808080')
        return format_html(
            '<span style="background-color: {}; color: white; padding: 3px 10px; border-radius: 3px;">{}</span>',
            color, obj.get_status_display()
        )
    status_badge.short_description = 'Status'

    def progress_bar(self, obj):
        color = '#32CD32' if obj.status == 'completed' else '#DC143C' if obj.status == 'failed' else '#1E90FF'
        return format_html(
            '<div style="width: 100px; background-color: #f0f0f0; border-radius: 3px;">'
            '<div style="width: {}px; background-color: {}; height: 20px; border-radius: 3px; text-align: center; color: white; line-height: 20px;">{}</div>'
            '</div>',
            obj.progress, color, f"{obj.progress}%"
        )
    progress_bar.short_description = 'Progress'

    def data_count(self, obj):
        return obj.data.count()
    data_count.short_description = 'Data Points'

    actions = ['mark_as_failed', 'mark_as_pending']

    def mark_as_failed(self, request, queryset):
        queryset.update(status='failed')
        self.message_user(request, f"{queryset.count()} runs marked as failed")
    mark_as_failed.short_description = "Mark as Failed"

    def mark_as_pending(self, request, queryset):
        queryset.update(status='pending', progress=0)
        self.message_user(request, f"{queryset.count()} runs marked as pending")
    mark_as_pending.short_description = "Mark as Pending"


# ----------------------
# FORECAST DATA ADMIN
# ----------------------
@admin.register(ForecastData)
class ForecastDataAdmin(admin.ModelAdmin):
    list_display = [
        'id', 'forecast_run', 'domain', 'parameter', 
        'time_step_display', 'valid_time', 'value_stats_colored', 'created_at'
    ]
    list_filter = ['domain', 'parameter', 'time_step', 'forecast_run__run_date']
    search_fields = ['forecast_run__run_date', 'source_file']
    readonly_fields = ['created_at', 'grid_size']
    date_hierarchy = 'valid_time'
    actions = ['export_as_csv']

    fieldsets = (
        ('Reference', {
            'fields': ('forecast_run', 'domain', 'parameter')
        }),
        ('Time Information', {
            'fields': ('time_step', 'valid_time')
        }),
        ('Grid Data', {
            'fields': ('grid_size', 'source_file'),
            'description': 'Grid data stored as JSON (not displayed for performance)'
        }),
        ('Statistics', {
            'fields': ('min_value', 'max_value', 'mean_value')
        }),
        ('Metadata', {
            'fields': ('created_at',),
            'classes': ('collapse',)
        }),
    )

    def time_step_display(self, obj):
        hours = obj.time_step * 3
        return f"T+{hours:02d}h (step {obj.time_step})"
    time_step_display.short_description = 'Time Step'

    def value_stats_colored(self, obj):
        if obj.min_value is not None and obj.max_value is not None:
            return format_html(
                '<span style="color:blue;">Min: {:.2f}</span>, '
                '<span style="color:red;">Max: {:.2f}</span>, '
                '<span style="color:green;">Avg: {:.2f}</span>',
                obj.min_value, obj.max_value, obj.mean_value
            )
        return "N/A"
    value_stats_colored.short_description = "Statistics"

    def grid_size(self, obj):
        if obj.grid_lats and obj.grid_lons:
            rows = len(obj.grid_lats)
            cols = len(obj.grid_lats[0]) if rows > 0 else 0
            return f"{rows} × {cols}"
        return "N/A"
    grid_size.short_description = 'Grid Size'

    def export_as_csv(self, request, queryset):
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="forecast_data.csv"'
        import csv
        writer = csv.writer(response)
        writer.writerow(['Forecast Run', 'Domain', 'Parameter', 'Valid Time', 'Min', 'Max', 'Avg'])
        for obj in queryset:
            writer.writerow([obj.forecast_run.id, obj.domain, obj.parameter, obj.valid_time, obj.min_value, obj.max_value, obj.mean_value])
        return response
    export_as_csv.short_description = "Export Selected to CSV"


# ----------------------
# DATA FETCH LOG ADMIN
# ----------------------
@admin.register(DataFetchLog)
class DataFetchLogAdmin(admin.ModelAdmin):
    list_display = [
        'id', 'forecast_run', 'started_at', 'duration', 
        'status_badge', 'ssh_host', 'files_stats'
    ]
    list_filter = ['status', 'started_at', 'ssh_host']
    search_fields = ['ssh_host', 'ssh_user', 'error_message']
    readonly_fields = ['started_at', 'completed_at', 'duration', 'data_size']
    date_hierarchy = 'started_at'
    actions = ['retry_fetch', 'mark_failed']

    fieldsets = (
        ('Reference', {
            'fields': ('forecast_run',)
        }),
        ('Connection', {
            'fields': ('ssh_host', 'ssh_user')
        }),
        ('Timing', {
            'fields': ('started_at', 'completed_at', 'duration')
        }),
        ('Transfer Statistics', {
            'fields': ('files_requested', 'files_downloaded', 'total_bytes', 'data_size')
        }),
        ('Status', {
            'fields': ('status', 'error_message')
        }),
        ('Logs', {
            'fields': ('log_messages',),
            'classes': ('collapse',)
        }),
    )

    def status_badge(self, obj):
        colors = {
            'success': '#32CD32',
            'partial': '#FFA500',
            'failed': '#DC143C',
        }
        color = colors.get(obj.status, '#808080')
        return format_html(
            '<span style="background-color: {}; color: white; padding: 3px 10px; border-radius: 3px;">{}</span>',
            color, obj.get_status_display()
        )
    status_badge.short_description = 'Status'

    def files_stats(self, obj):
        if obj.files_requested > 0:
            percent = (obj.files_downloaded / obj.files_requested) * 100
            return f"{obj.files_downloaded}/{obj.files_requested} ({percent:.0f}%)"
        return "0/0"
    files_stats.short_description = 'Files Downloaded'

    def duration(self, obj):
        if obj.completed_at and obj.started_at:
            delta = obj.completed_at - obj.started_at
            minutes = delta.total_seconds() / 60
            return f"{minutes:.1f} min"
        return "In progress..."
    duration.short_description = 'Duration'

    def data_size(self, obj):
        if obj.total_bytes:
            mb = obj.total_bytes / (1024 * 1024)
            return f"{mb:.2f} MB"
        return "0 MB"
    data_size.short_description = 'Data Size'

    # Actions
    def retry_fetch(self, request, queryset):
        for log in queryset:
            fetch_and_process_wrf_data.delay(log.forecast_run.id)
        self.message_user(request, f"Retry task enqueued for {queryset.count()} logs")
    retry_fetch.short_description = "Retry Data Fetch"

    def mark_failed(self, request, queryset):
        queryset.update(status='failed')
        self.message_user(request, f"{queryset.count()} logs marked as failed")
    mark_failed.short_description = "Mark as Failed"


# ----------------------
# ADMIN SITE CUSTOMIZATION
# ----------------------
admin.site.site_header = "KMD NWP System Administration"
admin.site.site_title = "KMD NWP Admin"
admin.site.index_title = "Welcome to KMD Numerical Weather Prediction System"
