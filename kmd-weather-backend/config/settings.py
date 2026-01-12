"""
Django settings for KMD WRF Backend
"""

import os
from pathlib import Path
import environ
import dj_database_url


# Build paths inside the project
BASE_DIR = Path(__file__).resolve().parent.parent

# Initialize environment variables
env = environ.Env(
    DEBUG=(bool, False)
)

# Read .env file
environ.Env.read_env(os.path.join(BASE_DIR, '.env'))

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
    'corsheaders.middleware.CorsMiddleware',  # Must be before CommonMiddleware
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

# Database Configuration
DATABASES = {
    'default': {
        'ENGINE': env('DB_ENGINE', default='django.db.backends.sqlite3'),
        'NAME': env('DB_NAME', default=BASE_DIR / 'db.sqlite3'),
        'USER': env('DB_USER', default=''),
        'PASSWORD': env('DB_PASSWORD', default=''),
        'HOST': env('DB_HOST', default=''),
        'PORT': env('DB_PORT', default=''),
    }
}

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

# Static files (CSS, JavaScript, Images)
STATIC_URL = '/static/'
STATIC_ROOT = BASE_DIR / 'staticfiles'
# STATICFILES_DIRS = [
 #   BASE_DIR / 'static'
 #   ]
 
# Media files
MEDIA_URL = 'media/'
MEDIA_ROOT = BASE_DIR / 'media'

# Default primary key field type
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# CORS Configuration
CORS_ALLOWED_ORIGINS = env.list('CORS_ALLOWED_ORIGINS', default=[
    'http://localhost:3000',
    'http://localhost:5173',
    'http://127.0.0.1:3000',
])

CORS_ALLOW_CREDENTIALS = True

# Django REST Framework Configuration
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
        'rest_framework.throttling.UserRateThrottle'
    ],
    'DEFAULT_THROTTLE_RATES': {
        'anon': '100/hour',
        'user': '1000/hour'
    }
}

# ===============================
# WRF Data Configuration
# ===============================
WRF_CONFIG = {
    # Target SSH Server (GRIB)
    'SSH_HOST': env('WRF_TARGET_HOST', default=''),
    'SSH_PORT': env.int('WRF_TARGET_PORT', default=22),
    'SSH_USERNAME': env('WRF_TARGET_USERNAME', default=''),
    'SSH_PASSWORD': env('WRF_TARGET_PASSWORD', default=''),
    'SSH_KEY_PATH': env('WRF_KEY_PATH', default=''),
    'SSH_KEY_PASSWORD': env('WRF_KEY_PASSWORD', default=''),

    # Jump Host (Gateway)
    'JUMP_HOST': env('WRF_JUMP_HOST', default=''),
    'JUMP_PORT': env.int('WRF_JUMP_PORT', default=22),
    'JUMP_USERNAME': env('WRF_JUMP_USERNAME', default=''),
    'JUMP_PASSWORD': env('WRF_JUMP_PASSWORD', default=''),

    # Remote Paths
    'REMOTE_BASE_PATH': env('WRF_REMOTE_GRIB_PATH', default='/data/wrf'),
    'KENYA_PATH': env('WRF_KENYA_PATH', default='/data/wrf/kenya'),
    'EAST_AFRICA_PATH': env('WRF_EAST_AFRICA_PATH', default='/data/wrf/east_africa'),

    # Local Paths
    'LOCAL_DATA_PATH': Path(BASE_DIR / env('LOCAL_DATA_PATH', default='data/raw')),
    'PROCESSED_DATA_PATH': Path(BASE_DIR / env('PROCESSED_DATA_PATH', default='data/processed')),

    # Model Configuration
    'BASE_TIME': env('WRF_BASE_TIME', default='09:00'),
    'TIMEZONE': env('WRF_TIMEZONE', default='Africa/Nairobi'),
    'FORECAST_HOURS': env.int('WRF_FORECAST_HOURS', default=72),
    'TIME_STEP_HOURS': env.int('WRF_TIME_STEP_HOURS', default=3),

    # Data Retention
    'KEEP_RAW_FILES_DAYS': env.int('KEEP_RAW_FILES_DAYS', default=7),
    'KEEP_PROCESSED_FILES_DAYS': env.int('KEEP_PROCESSED_FILES_DAYS', default=30),

    # File Naming Convention
    'KENYA_FILE_PREFIX': 'wrfout_',
    'KENYA_FILE_SUFFIX': '01',
    'EAST_AFRICA_FILE_PREFIX': 'wrfout_',
    'EAST_AFRICA_FILE_SUFFIX': '02',
}

# Ensure local directories exist
for path in [WRF_CONFIG['LOCAL_DATA_PATH'], WRF_CONFIG['PROCESSED_DATA_PATH']]:
    path.mkdir(parents=True, exist_ok=True)

# Logging Configuration
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module} {message}',
            'style': '{',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
        'file': {
            'class': 'logging.FileHandler',
            'filename': env('LOG_FILE', default='logs/kmd_backend.log'),
            'formatter': 'verbose',
        },
    },
    'root': {
        'handlers': ['console', 'file'],
        'level': env('LOG_LEVEL', default='INFO'),
    },
    'loggers': {
        'wrf_data': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': False,
        },
    },
}

# Cache configuration for GRIB data
CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'wrf-grib-cache',
        'OPTIONS': {
            'MAX_ENTRIES': 200  # Cache up to 200 different requests
        }
    }
}

# Celery Configuration (for automated data fetching)
CELERY_BROKER_URL = env('CELERY_BROKER_URL', default='redis://localhost:6379/0')
CELERY_RESULT_BACKEND = env('CELERY_RESULT_BACKEND', default='redis://localhost:6379/0')
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = 'Africa/Nairobi'

# Add at the top with other imports
# ============================================
# Persistent Storage Configuration
# ============================================
IS_RAILWAY = os.environ.get('RAILWAY_ENVIRONMENT') is not None

if IS_RAILWAY:
    # Use persistent volume on Railway
    DATA_ROOT = Path('/app/data')
else:
    # Use local directory for development
    DATA_ROOT = BASE_DIR / 'data'

# ============================================
# WRF Configuration (UPDATE THIS SECTION)
# ============================================
WRF_CONFIG = {
    # Proxy Server (First Hop)
    'JUMP_HOST': env('WRF_JUMP_HOST', default=''),
    'JUMP_PORT': env.int('WRF_JUMP_PORT', default=22),
    'JUMP_USERNAME': env('WRF_JUMP_USERNAME', default=''),
    'JUMP_PASSWORD': env('WRF_JUMP_PASSWORD', default=''),  # Optional, use key instead
    'JUMP_SSH_KEY': env('WRF_JUMP_SSH_KEY_BASE64', default=''),  # Base64 encoded

    # Target SSH Server (Second Hop)
    'SSH_HOST': env('WRF_TARGET_HOST', default=''),
    'SSH_PORT': env.int('WRF_TARGET_PORT', default=22),
    'SSH_USERNAME': env('WRF_TARGET_USERNAME', default=''),
    'SSH_PASSWORD': env('WRF_TARGET_PASSWORD', default=''),  # Optional, use key instead
    'SSH_PRIVATE_KEY': env('WRF_SSH_PRIVATE_KEY_BASE64', default=''),  # Base64 encoded

    # Remote Paths
    'REMOTE_BASE_PATH': env('WRF_REMOTE_GRIB_PATH', default='/home/nwp/DA/SEVERE'),

    # Local Paths (Use persistent volume)
    'LOCAL_DATA_PATH': DATA_ROOT / env('LOCAL_DATA_PATH', default='raw'),
    'PROCESSED_DATA_PATH': DATA_ROOT / env('PROCESSED_DATA_PATH', default='processed'),

    # Model Configuration
    'BASE_TIME': env('WRF_BASE_TIME', default='19:00'),
    'TIMEZONE': env('WRF_TIMEZONE', default='Africa/Nairobi'),
    'FORECAST_HOURS': env.int('WRF_FORECAST_HOURS', default=72),
    'TIME_STEP_HOURS': env.int('WRF_TIME_STEP_HOURS', default=3),

    # Data Retention
    'KEEP_RAW_FILES_DAYS': env.int('KEEP_RAW_FILES_DAYS', default=7),
    'KEEP_PROCESSED_FILES_DAYS': env.int('KEEP_PROCESSED_FILES_DAYS', default=30),
}

# Ensure local directories exist
for path in [WRF_CONFIG['LOCAL_DATA_PATH'], WRF_CONFIG['PROCESSED_DATA_PATH']]:
    path.mkdir(parents=True, exist_ok=True)

# Cache directory
CACHE_DIR = DATA_ROOT / 'cache'
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Logging directory
LOG_DIR = DATA_ROOT / 'logs'
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ============================================
# Production Database Configuration
# ============================================
if not DEBUG:
    DATABASES['default'] = dj_database_url.config(
        default=env('DATABASE_URL'),
        conn_max_age=600,
        conn_health_checks=True,
    )

# ============================================
# Static Files (WhiteNoise)
# ============================================
MIDDLEWARE.insert(1, 'whitenoise.middleware.WhiteNoiseMiddleware')
STATICFILES_STORAGE = 'whitenoise.storage.CompressedManifestStaticFilesStorage'

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

# ============================================
# Logging Configuration (UPDATE)
# ============================================
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module} {process:d} {message}',
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
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': LOG_DIR / 'kmd_backend.log',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5,
            'formatter': 'verbose',
        },
    },
    'root': {
        'handlers': ['console', 'file'],
        'level': env('LOG_LEVEL', default='INFO'),
    },
    'loggers': {
        'wrf_data': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': False,
        },
        'django': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': False,
        },
    },
}

# ============================================
# Cron Security Token
# ============================================
CRON_SECRET_TOKEN = env('CRON_SECRET_TOKEN', default='change-this-in-production')