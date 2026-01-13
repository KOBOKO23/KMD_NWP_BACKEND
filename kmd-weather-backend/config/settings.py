"""
Django settings for KMD WRF Backend
ON-DEMAND PROCESSING VERSION - Optimized for Render free tier
"""

import os
from pathlib import Path
import environ
import dj_database_url
import tempfile

# Build paths inside the project
BASE_DIR = Path(__file__).resolve().parent.parent

# Initialize environment variables
env = environ.Env(DEBUG=(bool, False))

# Read .env file if it exists
env_file = os.path.join(BASE_DIR, '.env')
if os.path.exists(env_file):
    environ.Env.read_env(env_file)

# Security Settings
SECRET_KEY = env('SECRET_KEY', default='django-insecure-change-this-in-production')
DEBUG = env('DEBUG')
ALLOWED_HOSTS = env.list('ALLOWED_HOSTS', default=['localhost', '127.0.0.1'])

# Application definition
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    
    # Third-party apps
    'rest_framework',
    'corsheaders',
    'django_extensions',
    
    # Local apps
    'wrf_data',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware',
    'corsheaders.middleware.CorsMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'config.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'config.wsgi.application'

# ============================================
# Database Configuration
# ============================================
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}

# Use sqlite3 in production if DATABASE_URL is provided
if os.environ.get('DATABASE_URL'):
    DATABASES['default'] = dj_database_url.config(
        default=env('DATABASE_URL'),
        conn_max_age=600,
        conn_health_checks=True,
    )

# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]

# Internationalization
LANGUAGE_CODE = 'en-us'
TIME_ZONE = env('WRF_TIMEZONE', default='Africa/Nairobi')
USE_I18N = True
USE_TZ = True

# ============================================
# Static Files
# ============================================
STATIC_URL = '/static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'
STATICFILES_STORAGE = 'whitenoise.storage.CompressedManifestStaticFilesStorage'

# Media files
MEDIA_URL = '/media/'
MEDIA_ROOT = BASE_DIR / 'media'

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# ============================================
# CORS Configuration
# ============================================
CORS_ALLOWED_ORIGINS = env.list('CORS_ALLOWED_ORIGINS', default=[
    'http://localhost:3000',
    'http://localhost:5173',
])
CORS_ALLOW_CREDENTIALS = True

# ============================================
# Django REST Framework
# ============================================
REST_FRAMEWORK = {
    'DEFAULT_RENDERER_CLASSES': [
        'rest_framework.renderers.JSONRenderer',
    ],
    'DEFAULT_PARSER_CLASSES': [
        'rest_framework.parsers.JSONParser',
    ],
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.PageNumberPagination',
    'PAGE_SIZE': 100,
    'DATETIME_FORMAT': '%Y-%m-%d %H:%M:%S',
    'DEFAULT_THROTTLE_CLASSES': [
        'rest_framework.throttling.AnonRateThrottle',
    ],
    'DEFAULT_THROTTLE_RATES': {
        'anon': '100/hour',  # Generous for demo
    }
}

# ============================================
# Temporary Storage (Ephemeral)
# ============================================
# On Render, use system temp directory (ephemeral, auto-cleaned)
TEMP_ROOT = Path(tempfile.gettempdir()) / 'wrf_temp'
TEMP_ROOT.mkdir(parents=True, exist_ok=True)

# ============================================
# WRF Data Configuration
# ============================================
WRF_CONFIG = {
    # Proxy Server (Jump Host)
    'JUMP_HOST': env('WRF_JUMP_HOST', default=''),
    'JUMP_PORT': env.int('WRF_JUMP_PORT', default=22),
    'JUMP_USERNAME': env('WRF_JUMP_USERNAME', default=''),
    'JUMP_PASSWORD': env('WRF_JUMP_PASSWORD', default=''),
    'JUMP_SSH_KEY': env('WRF_JUMP_SSH_KEY_BASE64', default=''),

    # Target SSH Server
    'SSH_HOST': env('WRF_TARGET_HOST', default=''),
    'SSH_PORT': env.int('WRF_TARGET_PORT', default=22),
    'SSH_USERNAME': env('WRF_TARGET_USERNAME', default=''),
    'SSH_PASSWORD': env('WRF_TARGET_PASSWORD', default=''),
    'SSH_PRIVATE_KEY': env('WRF_SSH_PRIVATE_KEY_BASE64', default=''),

    # Remote Path
    'REMOTE_BASE_PATH': env('WRF_REMOTE_GRIB_PATH', default='/home/nwp/DA/SEVERE'),

    # Local paths - use temp directory (auto-cleaned)
    'LOCAL_DATA_PATH': TEMP_ROOT,
    'PROCESSED_DATA_PATH': TEMP_ROOT / 'processed',

    # Model Configuration
    'BASE_TIME': env('WRF_BASE_TIME', default='19:00'),
    'TIMEZONE': env('WRF_TIMEZONE', default='Africa/Nairobi'),
    'FORECAST_HOURS': env.int('WRF_FORECAST_HOURS', default=72),
    'TIME_STEP_HOURS': env.int('WRF_TIME_STEP_HOURS', default=3),
}

# ============================================
# Cache Configuration (In-Memory)
# ============================================
CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'wrf-data-cache',
        'TIMEOUT': 900,  # 15 minutes
        'OPTIONS': {
            'MAX_ENTRIES': 100  # Store up to 100 timesteps in memory
        }
    }
}

# ============================================
# Celery Configuration (DISABLED)
# ============================================
# Celery is not needed for on-demand processing
CELERY_BROKER_URL = env('CELERY_BROKER_URL', default='')
CELERY_RESULT_BACKEND = env('CELERY_RESULT_BACKEND', default='')

# ============================================
# Logging Configuration
# ============================================
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module} {message}',
            'style': '{',
        },
        'simple': {
            'format': '{levelname} {message}',
            'style': '{',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
    },
    'root': {
        'handlers': ['console'],
        'level': env('LOG_LEVEL', default='INFO'),
    },
    'loggers': {
        'wrf_data': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False,
        },
        'django': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False,
        },
    },
}

# ============================================
# Security Settings (Production)
# ============================================
if not DEBUG:
    SECURE_SSL_REDIRECT = True
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
    SECURE_BROWSER_XSS_FILTER = True
    SECURE_CONTENT_TYPE_NOSNIFF = True
    X_FRAME_OPTIONS = 'DENY'
    SECURE_HSTS_SECONDS = 31536000
    SECURE_HSTS_INCLUDE_SUBDOMAINS = True
    SECURE_HSTS_PRELOAD = True