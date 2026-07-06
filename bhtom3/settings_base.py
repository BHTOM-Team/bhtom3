"""
Shared Django settings for the bhtom3 project.
"""
import logging.config
import os
import pkgutil
import ast
import tempfile
import importlib.util

from dotenv import dotenv_values

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

secret = dotenv_values(os.path.join(BASE_DIR, 'env/.bhtom.env'))

BHTOM2_UPLOAD_SERVICE_URL = secret.get('BHTOM2_UPLOAD_SERVICE_URL', secret.get('UPLOAD_SERVICE_URL', ''))
BHTOM2_API_BASE_URL = secret.get('BHTOM2_API_BASE_URL', '')
BHTOM2_API_TOKEN = secret.get('BHTOM2_API_TOKEN', '')
BHTOM2_API_TIMEOUT = int(secret.get('BHTOM2_API_TIMEOUT', '30'))
PUBLIC_UPLOAD_PASSWORD = secret.get('PUBLIC_UPLOAD_PASSWORD', '')

DATA_SERVICE_CONNECT_TIMEOUT = int(secret.get('DATA_SERVICE_CONNECT_TIMEOUT', os.environ.get('DATA_SERVICE_CONNECT_TIMEOUT', '10')))
DATA_SERVICE_READ_TIMEOUT = int(secret.get('DATA_SERVICE_READ_TIMEOUT', os.environ.get('DATA_SERVICE_READ_TIMEOUT', '60')))
DATA_SERVICE_JOB_TIMEOUT = int(secret.get('DATA_SERVICE_JOB_TIMEOUT', os.environ.get('DATA_SERVICE_JOB_TIMEOUT', '300')))
DB_WORKER_HEARTBEAT_INTERVAL = int(secret.get('DB_WORKER_HEARTBEAT_INTERVAL', os.environ.get('DB_WORKER_HEARTBEAT_INTERVAL', '300')))
DB_WORKER_STALE_RUNNING_AFTER = int(secret.get('DB_WORKER_STALE_RUNNING_AFTER', os.environ.get('DB_WORKER_STALE_RUNNING_AFTER', '7200')))
OBSERVATION_STATUS_FACILITY_TIMEOUT = int(secret.get('OBSERVATION_STATUS_FACILITY_TIMEOUT', os.environ.get('OBSERVATION_STATUS_FACILITY_TIMEOUT', '300')))


def env_bool(name, default=False):
    value = secret.get(name, os.environ.get(name, default))
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on')


ORCID_ENABLED = env_bool('ORCID_ENABLED', True)
ORCID_CLIENT_ID = secret.get('ORCID_CLIENT_ID', os.environ.get('ORCID_CLIENT_ID', ''))
ORCID_CLIENT_SECRET = secret.get('ORCID_CLIENT_SECRET', os.environ.get('ORCID_CLIENT_SECRET', ''))
ORCID_USE_SANDBOX = env_bool('ORCID_USE_SANDBOX', False)
ORCID_BASE_DOMAIN = secret.get(
    'ORCID_BASE_DOMAIN',
    os.environ.get('ORCID_BASE_DOMAIN', 'sandbox.orcid.org' if ORCID_USE_SANDBOX else 'orcid.org'),
)
ORCID_SEND_ADMIN_NOTIFICATION = env_bool('ORCID_SEND_ADMIN_NOTIFICATION', True)
ORCID_ADMIN_NOTIFY_EMAILS = secret.get('ORCID_ADMIN_NOTIFY_EMAILS', os.environ.get('ORCID_ADMIN_NOTIFY_EMAILS', ''))
ORCID_PUBLIC_API_TIMEOUT = int(secret.get('ORCID_PUBLIC_API_TIMEOUT', os.environ.get('ORCID_PUBLIC_API_TIMEOUT', '6')))
ORCID_ALLAUTH_AVAILABLE = importlib.util.find_spec('allauth') is not None

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = secret.get("SECRET_KEY", '')

TOM_NAME = 'bhtom3'

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django.contrib.sites',
    'django_extensions',
    'django_tables2',
    'guardian',
    'tom_common',
    'django_comments',
    'bootstrap4',
    'crispy_bootstrap4',
    'crispy_forms',
    'rest_framework',
    'rest_framework.authtoken',
    'django_filters',
    'django_gravatar',
    'django_htmx',
    'tom_targets',
    'tom_alerts',
    'tom_catalogs',
    'tom_dataservices',
    'tom_observations',
    'tom_dataproducts',
    'django_tasks',
    'custom_code',
    'django_tasks.backends.database',
    'tom_swift',
]

if ORCID_ENABLED and ORCID_ALLAUTH_AVAILABLE:
    INSTALLED_APPS += [
        'allauth',
        'allauth.account',
        'allauth.socialaccount',
        'allauth.socialaccount.providers.orcid',
    ]

SITE_ID = 1

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'django_htmx.middleware.HtmxMiddleware',
    'tom_common.middleware.Raise403Middleware',
    'tom_common.middleware.ExternalServiceMiddleware',
    'tom_common.middleware.AuthStrategyMiddleware',
]

if ORCID_ENABLED and ORCID_ALLAUTH_AVAILABLE:
    MIDDLEWARE += ['allauth.account.middleware.AccountMiddleware']

ROOT_URLCONF = 'bhtom3.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [os.path.join(BASE_DIR, 'templates')],
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

CRISPY_TEMPLATE_PACK = 'bootstrap4'

WSGI_APPLICATION = 'bhtom3.wsgi.application'

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
        'OPTIONS': {
            # Required by django_tasks.backends.database on SQLite.
            'transaction_mode': 'EXCLUSIVE',
            'timeout': 30,
        },
    }
}

DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

LOGIN_URL = '/accounts/login/'
LOGIN_REDIRECT_URL = '/'
LOGOUT_REDIRECT_URL = '/'

AUTHENTICATION_BACKENDS = (
    'django.contrib.auth.backends.ModelBackend',
    'guardian.backends.ObjectPermissionBackend',
)

if ORCID_ENABLED and ORCID_ALLAUTH_AVAILABLE:
    AUTHENTICATION_BACKENDS += ('allauth.account.auth_backends.AuthenticationBackend',)

ACCOUNT_EMAIL_VERIFICATION = 'none'
ACCOUNT_USERNAME_REQUIRED = True
SOCIALACCOUNT_ADAPTER = 'custom_code.socialaccount_adapter.BhtomOrcidSocialAccountAdapter'
SOCIALACCOUNT_AUTO_SIGNUP = True
SOCIALACCOUNT_LOGIN_ON_GET = True
SOCIALACCOUNT_QUERY_EMAIL = True
SOCIALACCOUNT_PROVIDERS = {
    'orcid': {
        'SCOPE': ['/authenticate'],
        'BASE_DOMAIN': ORCID_BASE_DOMAIN,
        'APP': {
            'client_id': ORCID_CLIENT_ID,
            'secret': ORCID_CLIENT_SECRET,
            'key': '',
        },
    }
}

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = False

USE_TZ = True

DATETIME_FORMAT = 'Y-m-d H:i:s'
DATE_FORMAT = 'Y-m-d'

STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, '_static')
STATICFILES_DIRS = [os.path.join(BASE_DIR, 'static')]
MEDIA_ROOT = os.path.join(BASE_DIR, 'data')
MEDIA_URL = '/data/'

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'timestamped': {
            'format': '%(asctime)s %(levelname)s %(name)s %(message)s',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'timestamped',
        }
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': 'INFO'
        }
    }
}

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.filebased.FileBasedCache',
        'LOCATION': tempfile.gettempdir()
    }
}

TARGET_TYPE = 'SIDEREAL'

TARGET_MODEL_CLASS = 'custom_code.models.BhtomTarget'

MATCH_MANAGERS = {}

FACILITIES = {
    'LCO': {
        'portal_url': 'https://observe.lco.global',
        'api_key': secret.get('LCO_API_KEY', ''),
    },
    'GEM': {
        'portal_url': {
            'GS': 'https://139.229.34.15:8443',
            'GN': 'https://128.171.88.221:8443',
        },
        'api_key': {
            'GS': '',
            'GN': '',
        },
        'user_email': '',
        'programs': {
            'GS-YYYYS-T-NNN': {
                'MM': 'Std: Some descriptive text',
                'NN': 'Rap: Some descriptive text'
            },
            'GN-YYYYS-T-NNN': {
                'QQ': 'Std: Some descriptive text',
                'PP': 'Rap: Some descriptive text',
            },
        },
    },
    'SWIFT': {
            'SWIFT_USERNAME': secret.get("SWIFT_USERNAME"),
            'SWIFT_SHARED_SECRET': secret.get("SWIFT_SHARED_SECRET"),
        },
    'LT': {
           'proposalIDs': ((secret.get("LT_PROPOSAL_ID"), secret.get("LT_PROPOSAL_TITLE")), ),
           'username': secret.get("LT_PROPOSAL_USER"),
           'password': secret.get("LT_PROPOSAL_PASS"),
           'LT_HOST': '161.72.57.3',
           'LT_PORT': '8080',
           'DEBUG': False,
    },
    'REM': {
    },
    'SUHORA': {
    },
    'BOLECINA': {
    },
    'LESEDI': {
    },
}

DATA_PRODUCT_TYPES = {
    'photometry': ('photometry', ' Photometry - SExtractor format'),
    'photometry_csv': ('photometry_csv', 'Photometry - CSV'),
    'fits_file': ('fits_file', 'FITS File'),
    'spectroscopy': ('spectroscopy', 'Spectroscopy'),
    'highenergy': ('highenergy', 'High-Energy Light Curves'),
    # 'image_file': ('image_file', 'Image File')
}
CLASSIFICATION_TYPES = [
    ("Unknown", "Unknown"), ('Be-star outburst', 'Be-star outburst'),
    ('AGN', "Active Galactic Nucleus(AGN)"), ("BL Lac", "BL Lac"),
    ("CV", "Cataclysmic Variable(CV)"), ("CEPH", "Cepheid Variable(CEPH)"),
    ("EB", "Eclipsing Binary(EB)"),
    ("Galaxy", "Galaxy"), ("LPV", "Long Period Variable(LPV)"),
    ("LBV", "Luminous Blue Variable(LBV)"),
    ("M-dwarf flare", "M-dwarf flare"), ("Microlensing Event", "Microlensing Event"), ("Nova", "Nova"),
    ("Peculiar Supernova", "Peculiar Supernova"),
    ("Planetary Transit", "Planetary Transit"),
    ("QSO", "Quasar(QSO)"), ("RCrB", "R CrB Variable"), ("RR Lyrae Variable", "RR Lyrae Variable"),
    ("SSO", "Solar System Object(SSO)"),
    ("Star", "Star"), ("SN", "Supernova(SN)"), ("Supernova imposter", "Supernova imposter"),
    ("Symbiotic star", "Symbiotic star"),
    ("TDE", "Tidal Disruption Event(TDE)"), ("Variable star-other", "Variable star-other"),
    ("XRB", "X-Ray Binary(XRB)"),
    ("YSO", "Young Stellar Object(YSO)")]

DATA_PROCESSORS = {
    'photometry': 'tom_dataproducts.processors.photometry_processor.PhotometryProcessor',
    'spectroscopy': 'tom_dataproducts.processors.spectroscopy_processor.SpectroscopyProcessor',
}

TOM_FACILITY_CLASSES = [
    'bhtom3.bhtom_observations.facilities.lco.LCOFacility',
    'tom_observations.facilities.gemini.GEMFacility',
    'tom_observations.facilities.soar.SOARFacility',
    'tom_swift.swift.SwiftFacility',
#    'tom_lt.lt.LTFacility',
    'bhtom3.bhtom_observations.facilities.lt.LTFacility',
    'bhtom3.bhtom_observations.facilities.rem.REM',
    'bhtom3.bhtom_observations.facilities.suhora.SUHORA',
    'bhtom3.bhtom_observations.facilities.bolecina.BOLECINA',
    'bhtom3.bhtom_observations.facilities.lesedi.LESEDI',
]

TOM_ALERT_CLASSES = [
    'tom_alerts.brokers.alerce.ALeRCEBroker',
    #  'tom_alerts.brokers.antares.ANTARESBroker',
    'tom_alerts.brokers.gaia.GaiaBroker',
    'tom_alerts.brokers.lasair.LasairBroker',
    'tom_alerts.brokers.tns.TNSBroker',
    #  'tom_alerts.brokers.fink.FinkBroker',
]


def _discover_custom_harvesters():
    discovered = []
    package_name = 'custom_code.bhtom_catalogs.harvesters'
    package_path = os.path.join(BASE_DIR, 'custom_code', 'bhtom_catalogs', 'harvesters')
    if not os.path.isdir(package_path):
        return discovered

    for _, mod_short, _ in pkgutil.iter_modules([package_path]):
        if mod_short.startswith('_'):
            continue
        module_name = f'{package_name}.{mod_short}'
        file_path = os.path.join(package_path, f'{mod_short}.py')
        if not os.path.isfile(file_path):
            continue
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                tree = ast.parse(f.read(), filename=file_path)
        except Exception:
            continue
        for node in tree.body:
            if not isinstance(node, ast.ClassDef):
                continue
            base_names = []
            for base in node.bases:
                if isinstance(base, ast.Name):
                    base_names.append(base.id)
                elif isinstance(base, ast.Attribute):
                    base_names.append(base.attr)
            if 'AbstractHarvester' in base_names:
                discovered.append(f'{module_name}.{node.name}')
    return discovered


BROKERS = {
    'TNS': {
        # BHTOM_Bot TNS API
        'api_key': secret.get('TNS_API_KEY', ''),
        'user_agent': 'tns_marker{"tns_id":99624,"type": "bot", "name":"BHTOM_Bot"}',
        'bot_id':99624,
        'bot_name':"BHTOM_Bot"
    },
    'Lasair': {
        'api_key': '',
    }
}

_BASE_HARVESTER_CLASSES = [
    'custom_code.bhtom_catalogs.harvesters.simbad.SimbadHarvester',
    'tom_catalogs.harvesters.ned.NEDHarvester',
    'tom_catalogs.harvesters.jplhorizons.JPLHorizonsHarvester',
    'tom_catalogs.harvesters.tns.TNSHarvester',
]
_CORE_CUSTOM_HARVESTER_CLASSES = [
    'custom_code.bhtom_catalogs.harvesters.gaia_dr3.GaiaDR3Harvester',
    'custom_code.bhtom_catalogs.harvesters.lsst.LSSTHarvester',
    'custom_code.bhtom_catalogs.harvesters.gaia_alerts.GaiaAlertsHarvester',
    'custom_code.bhtom_catalogs.harvesters.exoclock.ExoClockHarvester',
]
_BOTTOM_CUSTOM_HARVESTER_CLASSES = [
    'custom_code.bhtom_catalogs.harvesters.crts.CRTSHarvester',
]
_extra_custom_harvesters = [
    h for h in _discover_custom_harvesters()
    if h not in _CORE_CUSTOM_HARVESTER_CLASSES and h not in _BOTTOM_CUSTOM_HARVESTER_CLASSES
]
TOM_HARVESTER_CLASSES = (
    _CORE_CUSTOM_HARVESTER_CLASSES
    + _extra_custom_harvesters
    + _BASE_HARVESTER_CLASSES
    + _BOTTOM_CUSTOM_HARVESTER_CLASSES
)

HARVESTERS = {
    'TNS': {
        # BHTOM_Bot TNS API
        'api_key': secret.get('TNS_API_KEY', ''),
        'user_agent': 'tns_marker{"tns_id":99624,"type": "bot", "name":"BHTOM_Bot"}',
        'bot_id':99624,
        'bot_name':"BHTOM_Bot"
    }
}

EXTRA_FIELDS = []

AUTH_STRATEGY = 'READ_ONLY'

TARGET_PERMISSIONS_ONLY = True

OPEN_URLS = []

HOOKS = {
    'target_post_save': 'custom_code.hooks.target_post_save',
    'observation_change_state': 'tom_common.hooks.observation_change_state',
    'data_product_post_upload': 'tom_dataproducts.hooks.data_product_post_upload',
    'data_product_post_save': 'tom_dataproducts.hooks.data_product_post_save',
    'multiple_data_products_post_save': 'tom_dataproducts.hooks.multiple_data_products_post_save',
}

AUTO_QUERY_DATA_SERVICES_ON_TARGET_CREATE = True
DATA_SERVICES_UPDATE_INTERVAL_SECONDS = 86400
DATA_SERVICES_UPDATE_IMPORTANCE_GT = 0.0

AUTO_THUMBNAILS = False

THUMBNAIL_MAX_SIZE = (0, 0)

THUMBNAIL_DEFAULT_SIZE = (200, 200)

HINTS_ENABLED = True
HINT_LEVEL = 20

REST_FRAMEWORK = {
    'DEFAULT_PERMISSION_CLASSES': [
    ],
    'TEST_REQUEST_DEFAULT_FORMAT': 'json',
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.LimitOffsetPagination',
    'PAGE_SIZE': 1000
}

TASKS = {
    "default": {
        "BACKEND": "django_tasks.backends.database.DatabaseBackend"
    }
}

OBSERVATION_STATUS_UPDATE_INTERVAL_SECONDS = 180
DB_WORKER_THREADS = 4
LCO_INSTRUMENTS_TIMEOUT_SECONDS = 8
LCO_INSTRUMENTS_CACHE_SECONDS = 86400
LCO_ARCHIVE_API_URL = 'https://archive-api.lco.global'
LCO_ARCHIVE_TIMEOUT_SECONDS = 30
FRAM_ARCHIVE_USERNAME = secret.get('FRAM_ARCHIVE_USERNAME', os.environ.get('FRAM_ARCHIVE_USERNAME', 'guest'))
FRAM_ARCHIVE_PASSWORD = secret.get('FRAM_ARCHIVE_PASSWORD', os.environ.get('FRAM_ARCHIVE_PASSWORD', 'framarchive'))
FRAM_ARCHIVE_TIMEOUT = (30, 300)

PLOTLY_THEME = 'plotly_white'

try:
    from local_settings import *  # noqa
except ImportError:
    pass

TOMEMAIL: str = secret.get('TOMEMAIL')
TOMEMAILPASSWORD: str = secret.get('TOMEMAILPASSWORD')

EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
EMAIL_HOST = secret.get('EMAIL_HOST')
EMAIL_PORT = secret.get('EMAIL_PORT')
EMAIL_USE_TLS = secret.get('EMAIL_USE_TLS', True)
EMAIL_HOST_USER = TOMEMAIL
EMAIL_HOST_PASSWORD = TOMEMAILPASSWORD
DEFAULT_FROM_EMAIL = secret.get('DEFAULT_FROM_EMAIL', EMAIL_HOST_USER)
SERVER_EMAIL = secret.get('SERVER_EMAIL', EMAIL_HOST_USER)
