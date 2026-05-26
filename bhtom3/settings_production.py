"""
Production settings for bhtom3.
"""
from .settings_base import *  # noqa

DEBUG = False

ALLOWED_HOSTS = ['bhtom3.bhtom.space', '127.0.0.1', 'localhost', '193.0.88.218']
HOST_SCHEME = "https://"
SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')
SESSION_COOKIE_SECURE = True
CSRF_COOKIE_SECURE = True
CSRF_TRUSTED_ORIGINS = ['https://bhtom3.bhtom.space']
SECURE_SSL_REDIRECT = False
