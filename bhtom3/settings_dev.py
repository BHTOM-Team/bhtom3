"""
Local development settings for bhtom3.
"""
from .settings_base import *  # noqa

DEBUG = True

ALLOWED_HOSTS = ['visata.astrouw.edu.pl', 'localhost', '127.0.0.1', '*']
CORS_REPLACE_HTTPS_REFERER = False
HOST_SCHEME = "http://"
SECURE_PROXY_SSL_HEADER = None
SECURE_SSL_REDIRECT = False
SESSION_COOKIE_SECURE = False
CSRF_COOKIE_SECURE = False
SECURE_HSTS_SECONDS = None
SECURE_HSTS_INCLUDE_SUBDOMAINS = False
SECURE_FRAME_DENY = False
